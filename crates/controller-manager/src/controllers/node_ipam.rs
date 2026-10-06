//! Node IPAM: allocate a per-node pod CIDR out of the cluster CIDR.
//!
//! A faithful port of upstream kube-controller-manager's `nodeipam` range
//! allocator:
//!
//! * [`CidrSet`] is `pkg/controller/nodeipam/ipam/cidrset/cidr_set.go` — an
//!   in-memory bitmap with `AllocateNext` / `Occupy` / `Release`.
//! * [`RangeAllocator`] is `pkg/controller/nodeipam/ipam/range_allocator.go` —
//!   `occupyCIDRs` over the existing nodes at construction, a rate-limited
//!   queue drained by [`CIDR_UPDATE_WORKERS`] dedicated workers
//!   (`cidrUpdateWorkers = 30`, `cidr_allocator.go:62`), `AllocateOrOccupyCIDR`,
//!   `ReleaseCIDR` on node deletion, `cidrUpdateRetries = 3` on the write, and
//!   `CIDRNotAvailable` / `CIDRAssignmentFailed` events
//!   (`controllerutil.RecordNodeStatusChange`).
//!
//! Before this, node IPAM lived inside the node controller's single shared
//! queue worker and rebuilt its used-set with a full LIST of every node on each
//! allocation, so under load the assignment slipped past a client's
//! Get -> Update window (#1887).
//!
//! Gated by `--allocate-node-cidrs` (which upstream requires be paired with
//! `--cluster-cidr`); IPv4 single-stack only for now.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use ipnet::Ipv4Net;
use rusternetes_common::resources::{EventSource, EventType, Node, NodeSpec, ObjectReference};
use rusternetes_common::Error;
use rusternetes_storage::{
    build_key, build_prefix, extract_key, EventRecorder, Storage, WatchEvent, WorkQueue,
};
use tracing::{debug, error, info, warn};

/// The no. of NodeSpec updates the allocator can process concurrently.
/// Upstream `cidrUpdateWorkers` (`pkg/controller/nodeipam/ipam/cidr_allocator.go:62`).
pub const CIDR_UPDATE_WORKERS: usize = 30;

/// The no. of times a NodeSpec update is retried before it is dropped.
/// Upstream `cidrUpdateRetries` (`pkg/controller/nodeipam/ipam/cidr_allocator.go:65`).
pub const CIDR_UPDATE_RETRIES: usize = 3;

/// Static configuration for pod-CIDR allocation, parsed from the
/// `--cluster-cidr` / `--node-cidr-mask-size` flags.
#[derive(Debug, Clone)]
pub struct NodeIpamConfig {
    /// The whole cluster pod network (e.g. `10.244.0.0/16`).
    pub cluster_cidr: Ipv4Net,
    /// Prefix length of each per-node subnet (e.g. `24`).
    pub node_mask: u8,
    /// Service CIDR to keep out of the allocatable range when it overlaps the
    /// cluster CIDR (upstream `CIDRAllocatorParams.ServiceCIDR`).
    pub service_cidr: Option<Ipv4Net>,
}

impl NodeIpamConfig {
    /// Parse `--cluster-cidr` and validate `--node-cidr-mask-size` against it.
    /// The node mask must be no shorter than the cluster prefix and at most 32.
    pub fn new(cluster_cidr: &str, node_mask: u8) -> Result<Self, String> {
        let cluster: Ipv4Net = cluster_cidr
            .parse()
            .map_err(|e| format!("invalid --cluster-cidr {cluster_cidr:?}: {e}"))?;
        let cluster = cluster.trunc();
        if node_mask > 32 {
            return Err(format!("--node-cidr-mask-size {node_mask} exceeds 32"));
        }
        if node_mask < cluster.prefix_len() {
            return Err(format!(
                "--node-cidr-mask-size {node_mask} is shorter than the cluster CIDR prefix /{}",
                cluster.prefix_len()
            ));
        }
        Ok(Self {
            cluster_cidr: cluster,
            node_mask,
            service_cidr: None,
        })
    }

    /// Like [`Self::new`], additionally parsing the optional
    /// `--service-cluster-ip-range`.
    pub fn from_flags(
        cluster_cidr: &str,
        node_mask: u8,
        service_cidr: Option<&str>,
    ) -> Result<Self, String> {
        let mut cfg = Self::new(cluster_cidr, node_mask)?;
        if let Some(svc) = service_cidr {
            let svc: Ipv4Net = svc
                .parse()
                .map_err(|e| format!("invalid --service-cluster-ip-range {svc:?}: {e}"))?;
            cfg = cfg.with_service_cidr(svc);
        }
        Ok(cfg)
    }

    /// Keep `service_cidr` out of the allocatable range
    /// (upstream `rangeAllocator.filterOutServiceRange`).
    #[must_use]
    pub fn with_service_cidr(mut self, service_cidr: Ipv4Net) -> Self {
        self.service_cidr = Some(service_cidr.trunc());
        self
    }
}

/// Errors from [`CidrSet`]; the wording follows upstream's `cidr_set.go`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CidrSetError {
    /// `ErrCIDRRangeNoCIDRsRemaining`.
    NoCidrsRemaining,
    /// The node mask does not fit the cluster CIDR.
    InvalidMask(String),
    /// The CIDR lies outside the cluster CIDR.
    OutOfRange(String),
}

impl fmt::Display for CidrSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoCidrsRemaining => write!(
                f,
                "CIDR allocation failed; there are no remaining CIDRs left to allocate in the accepted range"
            ),
            Self::InvalidMask(m) | Self::OutOfRange(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for CidrSetError {}

struct CidrSetInner {
    /// Bitmap of allocated blocks, grown lazily (upstream `used big.Int`).
    used: Vec<u64>,
    /// Number of CIDRs allocated (upstream `allocatedCIDRs`).
    allocated: u64,
    /// Next CIDR index that should be free (upstream `nextCandidate`).
    next_candidate: u64,
}

impl CidrSetInner {
    fn bit(&self, i: u64) -> bool {
        self.used
            .get((i / 64) as usize)
            .is_some_and(|w| w & (1 << (i % 64)) != 0)
    }

    fn set_bit(&mut self, i: u64, on: bool) {
        let word = (i / 64) as usize;
        if word >= self.used.len() {
            if !on {
                return;
            }
            self.used.resize(word + 1, 0);
        }
        if on {
            self.used[word] |= 1 << (i % 64);
        } else {
            self.used[word] &= !(1 << (i % 64));
        }
    }
}

/// Manages a set of CIDR ranges from which blocks of IPs can be allocated.
/// Port of upstream `cidrset.CidrSet` (`cidr_set.go`), IPv4 only.
pub struct CidrSet {
    cluster: Ipv4Net,
    node_mask: u8,
    max_cidrs: u64,
    inner: Mutex<CidrSetInner>,
}

impl CidrSet {
    /// Upstream `NewCIDRSet`. `cluster` must be network-aligned.
    pub fn new(cluster: Ipv4Net, node_mask: u8) -> Result<Self, CidrSetError> {
        if node_mask > 32 || node_mask < cluster.prefix_len() {
            return Err(CidrSetError::InvalidMask(format!(
                "node CIDR mask /{node_mask} does not fit cluster CIDR {cluster}"
            )));
        }
        // getMaxCIDRs
        let max_cidrs = 1u64 << (node_mask - cluster.prefix_len());
        Ok(Self {
            cluster,
            node_mask,
            max_cidrs,
            inner: Mutex::new(CidrSetInner {
                used: Vec::new(),
                allocated: 0,
                next_candidate: 0,
            }),
        })
    }

    /// Maximum number of CIDRs that can be allocated.
    #[cfg(test)]
    pub fn max_cidrs(&self) -> u64 {
        self.max_cidrs
    }

    /// Number of CIDRs currently marked used.
    #[cfg(test)]
    pub fn allocated(&self) -> u64 {
        self.inner.lock().expect("cidr set poisoned").allocated
    }

    fn node_netmask(&self) -> u32 {
        Ipv4Net::new(std::net::Ipv4Addr::UNSPECIFIED, self.node_mask)
            .expect("mask validated in new")
            .netmask()
            .into()
    }

    /// Upstream `indexToCIDRBlock`.
    fn index_to_cidr_block(&self, index: u64) -> Ipv4Net {
        let shifted = (index << (32 - u32::from(self.node_mask))) as u32;
        let ip = u32::from(self.cluster.network()) | shifted;
        Ipv4Net::new(ip.into(), self.node_mask).expect("mask validated in new")
    }

    /// Allocates the next free CIDR range, marking it occupied (upstream
    /// `AllocateNext`).
    pub fn allocate_next(&self) -> Result<Ipv4Net, CidrSetError> {
        let mut s = self.inner.lock().expect("cidr set poisoned");
        if s.allocated == self.max_cidrs {
            return Err(CidrSetError::NoCidrsRemaining);
        }
        let mut candidate = s.next_candidate;
        for _ in 0..self.max_cidrs {
            if !s.bit(candidate) {
                break;
            }
            candidate = (candidate + 1) % self.max_cidrs;
        }
        s.next_candidate = (candidate + 1) % self.max_cidrs;
        s.set_bit(candidate, true);
        s.allocated += 1;
        Ok(self.index_to_cidr_block(candidate))
    }

    /// Upstream `getIndexForIP`.
    fn index_for_ip(&self, ip: u32) -> Result<u64, CidrSetError> {
        let idx =
            u64::from(u32::from(self.cluster.network()) ^ ip) >> (32 - u32::from(self.node_mask));
        if idx >= self.max_cidrs {
            return Err(CidrSetError::OutOfRange(format!(
                "CIDR: {}/{} is out of the range of CIDR allocator",
                std::net::Ipv4Addr::from(ip),
                self.node_mask
            )));
        }
        Ok(idx)
    }

    /// Upstream `getBeginningAndEndIndices`: the inclusive range of node-sized
    /// blocks that `cidr` covers.
    fn get_indices(&self, cidr: Ipv4Net) -> Result<(u64, u64), CidrSetError> {
        let cluster_ip = u32::from(self.cluster.network());
        let cluster_mask = u32::from(self.cluster.netmask());
        let cidr_ip = u32::from(cidr.network());
        let cidr_mask = u32::from(cidr.netmask());
        let cluster_contains = cidr_ip & cluster_mask == cluster_ip;
        let cidr_contains = cluster_ip & cidr_mask == cidr_ip;
        if !cluster_contains && !cidr_contains {
            return Err(CidrSetError::OutOfRange(format!(
                "cidr {cidr} is out the range of cluster cidr {}",
                self.cluster
            )));
        }
        let (mut begin, mut end) = (0, self.max_cidrs - 1);
        if self.cluster.prefix_len() < cidr.prefix_len() {
            let node_mask = self.node_netmask();
            begin = self.index_for_ip(cidr_ip & node_mask)?;
            end = self.index_for_ip((cidr_ip | !cidr_mask) & node_mask)?;
        }
        Ok((begin, end))
    }

    /// Releases the given CIDR range (upstream `Release`).
    pub fn release(&self, cidr: Ipv4Net) -> Result<(), CidrSetError> {
        let (begin, end) = self.get_indices(cidr)?;
        let mut s = self.inner.lock().expect("cidr set poisoned");
        for i in begin..=end {
            // Only change the counter if we change the bit, to prevent double
            // counting.
            if s.bit(i) {
                s.set_bit(i, false);
                s.allocated -= 1;
            }
        }
        Ok(())
    }

    /// Marks the given CIDR range as used. Succeeds even if it was previously
    /// used (upstream `Occupy`).
    pub fn occupy(&self, cidr: Ipv4Net) -> Result<(), CidrSetError> {
        let (begin, end) = self.get_indices(cidr)?;
        let mut s = self.inner.lock().expect("cidr set poisoned");
        for i in begin..=end {
            if !s.bit(i) {
                s.set_bit(i, true);
                s.allocated += 1;
            }
        }
        Ok(())
    }
}

/// A node's pod CIDRs: `spec.podCIDRs`, falling back to the legacy singular
/// `spec.podCIDR` when the list is absent.
fn node_pod_cidrs(node: &Node) -> Vec<String> {
    let Some(spec) = node.spec.as_ref() else {
        return Vec::new();
    };
    match spec.pod_cidrs.as_ref() {
        Some(c) if !c.is_empty() => c.clone(),
        _ => spec.pod_cidr.iter().cloned().collect(),
    }
}

/// Allocates and tracks per-node pod CIDRs. Port of upstream `rangeAllocator`.
pub struct RangeAllocator<S: Storage> {
    storage: Arc<S>,
    cidr_set: CidrSet,
    recorder: EventRecorder<S>,
    /// Where incoming work is placed to de-dup and to allow rate limited
    /// requeues on errors (upstream `queue`).
    queue: WorkQueue,
}

impl<S: Storage + 'static> RangeAllocator<S> {
    /// Upstream `NewCIDRRangeAllocator`: builds the CIDR set, filters out the
    /// service range, and occupies the CIDRs of every node in `nodes`. An error
    /// (garbage in `podCIDRs`, or a CIDR outside the cluster range) is fatal,
    /// as upstream ("This error will keep crashing controller-manager").
    pub fn new(storage: Arc<S>, cfg: NodeIpamConfig, nodes: &[Node]) -> Result<Self, String> {
        let cidr_set = CidrSet::new(cfg.cluster_cidr, cfg.node_mask).map_err(|e| e.to_string())?;
        let ra = Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            storage,
            cidr_set,
            queue: WorkQueue::new(),
        };
        if let Some(svc) = cfg.service_cidr {
            ra.filter_out_service_range(svc);
        }
        for node in nodes {
            if node_pod_cidrs(node).is_empty() {
                debug!("Node {} has no CIDR, ignoring", node.metadata.name);
                continue;
            }
            ra.occupy_cidrs(node).map_err(|e| e.to_string())?;
        }
        Ok(ra)
    }

    /// Marks every CIDR of the service range that overlaps the cluster CIDR as
    /// used so it is never assignable (upstream `filterOutServiceRange`).
    fn filter_out_service_range(&self, service_cidr: Ipv4Net) {
        let cluster = self.cidr_set.cluster;
        let overlaps =
            cluster.contains(&service_cidr.network()) || service_cidr.contains(&cluster.network());
        if !overlaps {
            return;
        }
        if let Err(e) = self.cidr_set.occupy(service_cidr) {
            error!(
                "Error filtering out service cidr {service_cidr} out cluster cidr {cluster}: {e}"
            );
        }
    }

    /// Marks `node.spec.podCIDRs` as used (upstream `occupyCIDRs`).
    fn occupy_cidrs(&self, node: &Node) -> Result<()> {
        for (idx, cidr) in node_pod_cidrs(node).iter().enumerate() {
            let pod_cidr: Ipv4Net = cidr.parse().map_err(|_| {
                anyhow::anyhow!("failed to parse node {}, CIDR {}", node.metadata.name, cidr)
            })?;
            // Upstream: an index beyond the configured cluster CIDRs cannot be
            // locked (cluster went from dual-stack to single-stack).
            if idx >= 1 {
                anyhow::bail!(
                    "node:{} has an allocated cidr: {} at index:{} that does not exist in cluster cidrs configuration",
                    node.metadata.name,
                    cidr,
                    idx
                );
            }
            self.cidr_set.occupy(pod_cidr).map_err(|e| {
                anyhow::anyhow!(
                    "failed to mark cidr[{pod_cidr}] at idx [{idx}] as occupied for node: {}: {e}",
                    node.metadata.name
                )
            })?;
        }
        Ok(())
    }

    /// Looks at `node`: assigns it a valid CIDR if it has none, or marks its
    /// CIDR as used if it has one (upstream `AllocateOrOccupyCIDR`).
    pub async fn allocate_or_occupy_cidr(&self, node: &Node) -> Result<()> {
        if !node_pod_cidrs(node).is_empty() {
            return self.occupy_cidrs(node);
        }
        let allocated = match self.cidr_set.allocate_next() {
            Ok(c) => c,
            Err(e) => {
                self.record_node_status_change(node, "CIDRNotAvailable")
                    .await;
                anyhow::bail!("failed to allocate cidr from cluster cidr at idx:0: {e}");
            }
        };
        debug!(
            "Putting node {} with CIDR {} into the work queue",
            node.metadata.name, allocated
        );
        self.update_cidrs_allocation(&node.metadata.name, allocated)
            .await
    }

    /// Marks `node.spec.podCIDRs` as unused (upstream `ReleaseCIDR`).
    pub fn release_cidr(&self, node: &Node) -> Result<()> {
        for (idx, cidr) in node_pod_cidrs(node).iter().enumerate() {
            let pod_cidr: Ipv4Net = cidr.parse().map_err(|_| {
                anyhow::anyhow!(
                    "failed to parse CIDR {} on Node {}",
                    cidr,
                    node.metadata.name
                )
            })?;
            if idx >= 1 {
                anyhow::bail!(
                    "node:{} has an allocated cidr: {} at index:{} that does not exist in cluster cidrs configuration",
                    node.metadata.name,
                    cidr,
                    idx
                );
            }
            debug!("Release CIDR {} for node {}", cidr, node.metadata.name);
            self.cidr_set
                .release(pod_cidr)
                .map_err(|e| anyhow::anyhow!("error when releasing CIDR {cidr}: {e}"))?;
        }
        Ok(())
    }

    fn release_allocated(&self, allocated: Ipv4Net) {
        if let Err(e) = self.cidr_set.release(allocated) {
            error!("Error releasing allocated CIDR {allocated}: {e}");
        }
    }

    /// Assigns `allocated` to the node and writes it (upstream
    /// `updateCIDRsAllocation`).
    async fn update_cidrs_allocation(&self, node_name: &str, allocated: Ipv4Net) -> Result<()> {
        let key = build_key("nodes", None, node_name);
        let cidrs = vec![allocated.to_string()];
        let node: Node = match self.storage.get(&key).await {
            Ok(n) => n,
            Err(e) => {
                // Deviation from upstream, which leaks the CIDR here: nothing
                // was written, so handing it back is always safe.
                error!(
                    "Failed while getting node {node_name} for updating Node.Spec.PodCIDRs: {e}"
                );
                self.release_allocated(allocated);
                if matches!(e, Error::NotFound(_)) {
                    return Ok(());
                }
                return Err(e.into());
            }
        };

        let existing = node_pod_cidrs(&node);
        // The CIDR list matches the proposed one: we possibly updated this node
        // and just failed to ack the success.
        if existing == cidrs {
            debug!("Node {node_name} already has allocated CIDR {allocated}. It matches the proposed one");
            return Ok(());
        }
        // The node has CIDRs: release the reserved one.
        if !existing.is_empty() {
            error!(
                "Node {node_name} already has a CIDR allocated ({existing:?}). Releasing the new one {allocated}"
            );
            self.release_allocated(allocated);
            return Ok(());
        }

        // The node has no CIDR currently assigned, so we set it.
        let mut last_err = None;
        for _ in 0..CIDR_UPDATE_RETRIES {
            match self.patch_node_cidrs(node_name, &cidrs).await {
                Ok(()) => {
                    info!("Set node {node_name} PodCIDR to {cidrs:?}");
                    return Ok(());
                }
                Err(e) => last_err = Some(e),
            }
        }
        let err = last_err.expect("CIDR_UPDATE_RETRIES > 0");
        // Failed: release back to the pool.
        error!(
            "Failed to update node {node_name} PodCIDR {cidrs:?} after multiple attempts: {err}"
        );
        self.record_node_status_change(&node, "CIDRAssignmentFailed")
            .await;
        // We accept the fact that we may leak CIDRs when we do not know whether
        // the request went through (upstream: `IsServerTimeout`; our equivalent
        // is a transport failure). Safer than releasing; a controller restart
        // returns all falsely allocated CIDRs to the pool.
        if !matches!(err, Error::Network(_)) {
            error!("CIDR assignment for node {node_name} failed. Releasing allocated CIDR");
            self.release_allocated(allocated);
        }
        Err(err.into())
    }

    /// Sets `spec.podCIDR`/`podCIDRs` on the node (upstream
    /// `nodeutil.PatchNodeCIDRs`, `component-helpers/node/util/cidr.go:40`).
    /// Rusternetes storage has no merge-patch verb, so this is a fresh get
    /// followed by an update of just those two spec fields.
    async fn patch_node_cidrs(&self, node_name: &str, cidrs: &[String]) -> Result<(), Error> {
        let key = build_key("nodes", None, node_name);
        let mut node: Node = self.storage.get(&key).await?;
        let spec = node.spec.get_or_insert(NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: None,
            taints: None,
        });
        spec.pod_cidr = Some(cidrs[0].clone());
        spec.pod_cidrs = Some(cidrs.to_vec());
        self.storage.update(&key, &node).await?;
        Ok(())
    }

    /// Upstream `controllerutil.RecordNodeStatusChange`
    /// (`pkg/controller/util/node/controller_utils.go:178`).
    async fn record_node_status_change(&self, node: &Node, new_status: &str) {
        let involved = ObjectReference {
            kind: Some("Node".to_string()),
            api_version: Some("v1".to_string()),
            name: Some(node.metadata.name.clone()),
            uid: Some(node.metadata.uid.clone()),
            namespace: None,
            ..Default::default()
        };
        let source = EventSource {
            component: "cidrAllocator".to_string(),
            host: None,
        };
        let msg = format!("Node {} status is now: {}", node.metadata.name, new_status);
        if let Err(e) = self
            .recorder
            .event(&involved, &source, EventType::Normal, new_status, &msg)
            .await
        {
            warn!("failed to record {new_status} event: {e}");
        }
    }

    /// Upstream `syncNode`.
    async fn sync_node(&self, key: &str) -> Result<()> {
        let name = key.strip_prefix("nodes/").unwrap_or(key);
        let node: Node = match self.storage.get(&build_key("nodes", None, name)).await {
            Ok(n) => n,
            Err(Error::NotFound(_)) => {
                debug!("node {name} has been deleted");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        if node.metadata.deletion_timestamp.is_some() {
            debug!("node {name} is being deleted");
            return Ok(());
        }
        self.allocate_or_occupy_cidr(&node).await
    }

    /// Upstream `runWorker` / `processNextNodeWorkItem`.
    async fn worker(self: Arc<Self>) {
        while let Some(key) = self.queue.get().await {
            match self.sync_node(&key).await {
                Ok(()) => self.queue.forget(&key).await,
                Err(e) => {
                    error!("error syncing '{key}': {e}, requeuing");
                    self.queue.requeue_rate_limited(key.clone()).await;
                }
            }
            self.queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, known: &HashMap<String, Node>) {
        for name in known.keys() {
            self.queue.add(format!("nodes/{name}")).await;
        }
    }

    /// Upstream `Run`: start [`CIDR_UPDATE_WORKERS`] workers, then feed the
    /// queue from node watch events. A deleted node's CIDRs are released from
    /// the last-seen copy (the informer's `DeleteFunc` / tombstone), including
    /// nodes that vanished while the watch was down.
    pub async fn run(self: Arc<Self>, initial: Vec<Node>) -> Result<()> {
        info!("Starting range CIDR allocator ({CIDR_UPDATE_WORKERS} workers)");
        for _ in 0..CIDR_UPDATE_WORKERS {
            tokio::spawn(Arc::clone(&self).worker());
        }

        let by_name = |nodes: Vec<Node>| -> HashMap<String, Node> {
            nodes
                .into_iter()
                .map(|n| (n.metadata.name.clone(), n))
                .collect()
        };
        let mut known = by_name(initial);
        let prefix = build_prefix("nodes", None);

        loop {
            // Watch first, then list, so no event falls in the gap.
            let mut watch = match self.storage.watch(&prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish node watch: {e}, retrying");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            match self.storage.list::<Node>("/registry/nodes/").await {
                Ok(items) => {
                    let fresh = by_name(items);
                    for (name, old) in &known {
                        if !fresh.contains_key(name) {
                            if let Err(e) = self.release_cidr(old) {
                                error!("Error while processing CIDR Release: {e}");
                            }
                        }
                    }
                    known = fresh;
                    self.enqueue_all(&known).await;
                }
                Err(e) => {
                    error!("Failed to list nodes: {e}, retrying");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }

            let mut resync = tokio::time::interval(Duration::from_secs(30));
            resync.tick().await;
            loop {
                tokio::select! {
                    event = watch.next() => match event {
                        Some(Ok(ev)) => {
                            let key = extract_key(&ev);
                            let name = key.strip_prefix("nodes/").unwrap_or(&key).to_string();
                            match ev {
                                WatchEvent::Deleted(_, prev) => {
                                    // Release only a node we still track, so a
                                    // delete already seen by a relist cannot
                                    // free a CIDR since handed to another node.
                                    if let Some(cached) = known.remove(&name) {
                                        let gone = serde_json::from_str::<Node>(&prev).unwrap_or(cached);
                                        if let Err(e) = self.release_cidr(&gone) {
                                            error!("Error while processing CIDR Release: {e}");
                                        }
                                    }
                                }
                                WatchEvent::Added(_, v) | WatchEvent::Modified(_, v) => {
                                    if let Ok(n) = serde_json::from_str::<Node>(&v) {
                                        known.insert(name, n);
                                    }
                                    self.queue.add(key).await;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!("Node watch error: {e}, reconnecting");
                            break;
                        }
                        None => {
                            warn!("Node watch stream ended, reconnecting");
                            break;
                        }
                    },
                    _ = resync.tick() => self.enqueue_all(&known).await,
                }
            }
        }
    }
}

/// Run node IPAM against `storage`: list the existing nodes (retrying until the
/// list succeeds), build the allocator from them, then run it. Upstream's
/// `NewNodeIpamController` passes the initial node list into
/// `NewCIDRRangeAllocator` the same way.
pub async fn run_node_ipam<S: Storage + 'static>(
    storage: Arc<S>,
    cfg: NodeIpamConfig,
) -> Result<()> {
    let nodes = loop {
        match storage.list::<Node>("/registry/nodes/").await {
            Ok(n) => break n,
            Err(e) => {
                warn!("node IPAM: failed to list nodes: {e}, retrying");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };
    let ra = Arc::new(RangeAllocator::new(storage, cfg, &nodes).map_err(|e| anyhow::anyhow!(e))?);
    ra.run(nodes).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::Event;
    use rusternetes_storage::memory::MemoryStorage;
    use rusternetes_storage::{build_key, build_prefix};
    use std::collections::HashSet;

    fn net(s: &str) -> Ipv4Net {
        s.parse().unwrap()
    }

    fn set(cluster: &str, mask: u8) -> CidrSet {
        CidrSet::new(net(cluster).trunc(), mask).unwrap()
    }

    // ---- CidrSet: ported from ipam/cidrset/cidr_set_test.go ----

    #[test]
    fn config_rejects_node_mask_shorter_than_cluster() {
        assert!(NodeIpamConfig::new("10.244.0.0/16", 8).is_err());
        assert!(NodeIpamConfig::new("10.244.0.0/16", 33).is_err());
        assert!(NodeIpamConfig::new("not-a-cidr", 24).is_err());
        assert!(NodeIpamConfig::new("10.244.0.0/16", 24).is_ok());
    }

    // TestCIDRSetFullyAllocated
    #[test]
    fn cidr_set_fully_allocated() {
        let a = set("127.123.234.0/30", 30);
        assert_eq!(a.allocate_next().unwrap(), net("127.123.234.0/30"));
        assert_eq!(a.allocate_next(), Err(CidrSetError::NoCidrsRemaining));
        a.release(net("127.123.234.0/30")).unwrap();
        assert_eq!(a.allocate_next().unwrap(), net("127.123.234.0/30"));
        assert!(a.allocate_next().is_err());
    }

    // TestIndexToCIDRBlock (IPv4 rows)
    #[test]
    fn cidr_set_index_to_cidr_block() {
        let a = set("127.123.3.0/16", 24);
        assert_eq!(a.index_to_cidr_block(0), net("127.123.0.0/24"));
        assert_eq!(a.index_to_cidr_block(15), net("127.123.15.0/24"));
        assert_eq!(a.index_to_cidr_block(255), net("127.123.255.0/24"));
        let b = set("10.0.0.0/8", 26);
        assert_eq!(b.index_to_cidr_block(1), net("10.0.0.64/26"));
    }

    // TestCIDRSet_RandomishAllocation
    #[test]
    fn cidr_set_randomish_allocation() {
        let a = set("127.123.234.0/16", 24);
        let first: Vec<_> = (0..256).map(|_| a.allocate_next().unwrap()).collect();
        assert!(a.allocate_next().is_err());
        for c in &first {
            a.release(*c).unwrap();
        }
        let again: Vec<_> = (0..256).map(|_| a.allocate_next().unwrap()).collect();
        assert!(a.allocate_next().is_err());
        assert_eq!(first, again);
    }

    // TestCIDRSet_AllocationOccupied
    #[test]
    fn cidr_set_allocation_occupied() {
        let a = set("127.123.234.0/16", 24);
        let cidrs: Vec<_> = (0..256).map(|_| a.allocate_next().unwrap()).collect();
        for c in &cidrs {
            a.release(*c).unwrap();
        }
        for c in &cidrs[128..] {
            a.occupy(*c).unwrap();
        }
        a.occupy(cidrs[128]).unwrap();
        let mut got: Vec<_> = (0..128).map(|_| a.allocate_next().unwrap()).collect();
        assert!(a.allocate_next().is_err());
        got.extend_from_slice(&cidrs[128..]);
        assert_eq!(got, cidrs);
    }

    // TestDoubleOccupyRelease
    #[test]
    fn cidr_set_double_occupy_release() {
        let a = set("10.42.0.0/16", 24);
        let ops: [(&str, bool, u64); 7] = [
            ("10.42.5.0/24", true, 1),
            ("10.42.9.0/24", true, 2),
            ("10.42.8.0/22", true, 5),
            ("10.42.9.0/24", true, 5),
            ("10.42.9.0/24", false, 4),
            ("10.42.9.0/24", false, 4),
            ("10.42.4.0/22", false, 3),
        ];
        for (cidr, occupy, want) in ops {
            if occupy {
                a.occupy(net(cidr)).unwrap();
            } else {
                a.release(net(cidr)).unwrap();
            }
            assert_eq!(a.allocated(), want, "after {cidr} occupy={occupy}");
        }
        for i in 0..(256 - 3) {
            a.allocate_next()
                .unwrap_or_else(|_| panic!("failed after {i}"));
        }
        assert!(a.allocate_next().is_err());
    }

    // TestGetBitforCIDR / TestOccupy (IPv4 rows)
    #[test]
    fn cidr_set_occupy_ranges_and_out_of_range() {
        let a = set("127.0.0.0/8", 16);
        // Whole cluster, and a shorter prefix that contains it, occupy every bit.
        a.occupy(net("127.0.0.0/8")).unwrap();
        assert_eq!(a.allocated(), 256);
        let b = set("127.0.0.0/8", 16);
        b.occupy(net("127.0.0.0/2")).unwrap();
        assert_eq!(b.allocated(), 256);
        // A /16 at index 123.
        let c = set("127.0.0.0/8", 16);
        c.occupy(net("127.123.0.0/16")).unwrap();
        assert_eq!(c.allocated(), 1);
        assert_eq!(c.get_indices(net("127.123.0.0/16")).unwrap(), (123, 123));
        // A /12 spans 16 /16s.
        assert_eq!(c.get_indices(net("127.16.0.0/12")).unwrap(), (16, 31));
        // Out of the cluster range.
        assert!(c.occupy(net("128.0.0.0/16")).is_err());
        assert!(c.release(net("10.0.0.0/16")).is_err());
    }

    // Test_getMaxCIDRs
    #[test]
    fn cidr_set_max_cidrs() {
        assert_eq!(set("10.0.0.0/16", 24).max_cidrs(), 256);
        assert!(CidrSet::new(net("10.0.0.0/16"), 8).is_err());
        assert!(CidrSet::new(net("10.0.0.0/16"), 33).is_err());
    }

    // ---- RangeAllocator: ported from ipam/range_allocator_test.go ----

    fn node_with_cidr(name: &str, cidr: Option<&str>) -> Node {
        let mut n = Node::new(name);
        if let Some(c) = cidr {
            n.spec = Some(rusternetes_common::resources::NodeSpec {
                pod_cidr: Some(c.to_string()),
                pod_cidrs: Some(vec![c.to_string()]),
                provider_id: None,
                unschedulable: None,
                taints: None,
            });
        }
        n
    }

    async fn put(storage: &Arc<MemoryStorage>, n: &Node) {
        storage
            .create(&build_key("nodes", None, &n.metadata.name), n)
            .await
            .unwrap();
    }

    async fn cidr_of(storage: &Arc<MemoryStorage>, name: &str) -> Option<String> {
        let n: Node = storage.get(&build_key("nodes", None, name)).await.unwrap();
        n.spec.and_then(|s| s.pod_cidr)
    }

    fn allocator(
        storage: &Arc<MemoryStorage>,
        cluster: &str,
        mask: u8,
        nodes: &[Node],
    ) -> Result<Arc<RangeAllocator<MemoryStorage>>, String> {
        let cfg = NodeIpamConfig::new(cluster, mask).unwrap();
        RangeAllocator::new(storage.clone(), cfg, nodes).map(Arc::new)
    }

    // TestOccupyPreExistingCIDR
    #[tokio::test]
    async fn new_occupies_preexisting_cidrs() {
        let storage = Arc::new(MemoryStorage::new());
        // A non-network-aligned stored value still occupies its /24.
        let nodes = [
            node_with_cidr("a", Some("10.244.0.1/24")),
            node_with_cidr("b", None),
        ];
        let ra = allocator(&storage, "10.244.0.0/16", 24, &nodes).unwrap();
        assert_eq!(ra.cidr_set.allocate_next().unwrap(), net("10.244.1.0/24"));
    }

    #[tokio::test]
    async fn new_fails_on_garbage_or_out_of_range_cidr() {
        let storage = Arc::new(MemoryStorage::new());
        let bad = [node_with_cidr("a", Some("not-a-cidr"))];
        assert!(allocator(&storage, "10.244.0.0/16", 24, &bad).is_err());
        let out = [node_with_cidr("a", Some("192.168.0.0/24"))];
        assert!(allocator(&storage, "10.244.0.0/16", 24, &out).is_err());
    }

    // TestAllocateOrOccupyCIDRSuccess
    #[tokio::test]
    async fn allocate_or_occupy_assigns_lowest_free_and_keeps_existing() {
        let storage = Arc::new(MemoryStorage::new());
        let n1 = node_with_cidr("node-1", None);
        let n2 = node_with_cidr("node-2", None);
        put(&storage, &n1).await;
        put(&storage, &n2).await;
        let ra = allocator(&storage, "10.244.0.0/16", 24, &[]).unwrap();

        ra.allocate_or_occupy_cidr(&n1).await.unwrap();
        ra.allocate_or_occupy_cidr(&n2).await.unwrap();
        assert_eq!(
            cidr_of(&storage, "node-1").await.as_deref(),
            Some("10.244.0.0/24")
        );
        assert_eq!(
            cidr_of(&storage, "node-2").await.as_deref(),
            Some("10.244.1.0/24")
        );
        let stored: Node = storage
            .get(&build_key("nodes", None, "node-1"))
            .await
            .unwrap();
        assert_eq!(
            stored.spec.unwrap().pod_cidrs,
            Some(vec!["10.244.0.0/24".to_string()])
        );

        // Re-running on a node that has a CIDR occupies, never reallocates.
        let stored: Node = storage
            .get(&build_key("nodes", None, "node-1"))
            .await
            .unwrap();
        ra.allocate_or_occupy_cidr(&stored).await.unwrap();
        assert_eq!(
            cidr_of(&storage, "node-1").await.as_deref(),
            Some("10.244.0.0/24")
        );
    }

    // TestAllocateOrOccupyCIDRFailure: exhausted range, no update, event.
    #[tokio::test]
    async fn exhausted_range_errors_without_update_and_records_event() {
        let storage = Arc::new(MemoryStorage::new());
        let n = node_with_cidr("node0", None);
        put(&storage, &n).await;
        let ra = allocator(&storage, "127.123.234.0/28", 30, &[]).unwrap();
        for _ in 0..4 {
            ra.cidr_set.allocate_next().unwrap();
        }
        assert!(ra.allocate_or_occupy_cidr(&n).await.is_err());
        assert_eq!(cidr_of(&storage, "node0").await, None);
        let events: Vec<Event> = storage.list(&build_prefix("events", None)).await.unwrap();
        assert!(
            events.iter().any(|e| e.reason == "CIDRNotAvailable"),
            "got {events:?}"
        );
    }

    // TestReleaseCIDRSuccess
    #[tokio::test]
    async fn release_cidr_returns_it_to_the_pool() {
        let storage = Arc::new(MemoryStorage::new());
        let n = node_with_cidr("node0", Some("10.10.0.0/24"));
        let ra = allocator(&storage, "10.10.0.0/16", 24, std::slice::from_ref(&n)).unwrap();
        ra.release_cidr(&n).unwrap();
        assert_eq!(ra.cidr_set.allocate_next().unwrap(), net("10.10.0.0/24"));
        // A node without CIDRs releases nothing.
        ra.release_cidr(&node_with_cidr("x", None)).unwrap();
    }

    // TestNodeDeletionReleaseCIDR (syncNode rows)
    #[tokio::test]
    async fn sync_node_releases_nothing_for_live_or_terminating_nodes() {
        let storage = Arc::new(MemoryStorage::new());
        let mut n = node_with_cidr("node0", Some("10.10.0.0/24"));
        put(&storage, &n).await;
        let ra = allocator(&storage, "10.10.0.0/16", 24, std::slice::from_ref(&n)).unwrap();
        ra.sync_node("nodes/node0").await.unwrap();
        assert_eq!(ra.cidr_set.allocate_next().unwrap(), net("10.10.1.0/24"));

        n.metadata.deletion_timestamp = Some(chrono::Utc::now());
        storage
            .update(&build_key("nodes", None, "node0"), &n)
            .await
            .unwrap();
        ra.sync_node("nodes/node0").await.unwrap();
        assert_eq!(ra.cidr_set.allocate_next().unwrap(), net("10.10.2.0/24"));
        // A key for a node that no longer exists is a no-op, not an error.
        ra.sync_node("nodes/gone").await.unwrap();
    }

    // updateCIDRsAllocation: node vanished between allocate and write.
    #[tokio::test]
    async fn allocation_for_vanished_node_is_released() {
        let storage = Arc::new(MemoryStorage::new());
        let ra = allocator(&storage, "10.10.0.0/16", 24, &[]).unwrap();
        ra.allocate_or_occupy_cidr(&node_with_cidr("ghost", None))
            .await
            .unwrap();
        assert_eq!(ra.cidr_set.allocated(), 0);
    }

    // updateCIDRsAllocation: "Node already has a CIDR allocated. Releasing the new one".
    #[tokio::test]
    async fn allocation_losing_race_releases_the_new_cidr() {
        let storage = Arc::new(MemoryStorage::new());
        let stored = node_with_cidr("node0", Some("10.10.7.0/24"));
        put(&storage, &stored).await;
        let ra = allocator(&storage, "10.10.0.0/16", 24, &[]).unwrap();
        // The caller's snapshot is stale: it still shows no CIDR.
        ra.allocate_or_occupy_cidr(&node_with_cidr("node0", None))
            .await
            .unwrap();
        assert_eq!(
            cidr_of(&storage, "node0").await.as_deref(),
            Some("10.10.7.0/24")
        );
        assert_eq!(ra.cidr_set.allocated(), 0);
    }

    // filterOutServiceRange
    #[tokio::test]
    async fn service_cidr_is_filtered_out() {
        let storage = Arc::new(MemoryStorage::new());
        let cfg = NodeIpamConfig::new("10.0.0.0/16", 24)
            .unwrap()
            .with_service_cidr(net("10.0.0.0/23"));
        let ra = RangeAllocator::new(storage, cfg, &[]).unwrap();
        assert_eq!(ra.cidr_set.allocate_next().unwrap(), net("10.0.2.0/24"));
    }

    // The issue #1887 race: assignment must not serialise behind one worker.
    #[tokio::test]
    async fn thirty_concurrent_syncs_get_distinct_cidrs() {
        let storage = Arc::new(MemoryStorage::new());
        for i in 0..30 {
            put(&storage, &node_with_cidr(&format!("n{i}"), None)).await;
        }
        let ra = allocator(&storage, "10.244.0.0/16", 24, &[]).unwrap();
        let mut set = tokio::task::JoinSet::new();
        for i in 0..30 {
            let ra = ra.clone();
            set.spawn(async move { ra.sync_node(&format!("nodes/n{i}")).await });
        }
        while let Some(r) = set.join_next().await {
            r.unwrap().unwrap();
        }
        let mut seen = HashSet::new();
        for i in 0..30 {
            let c = cidr_of(&storage, &format!("n{i}")).await.expect("assigned");
            assert!(seen.insert(c), "duplicate CIDR");
        }
    }

    async fn wait_for<F, Fut>(mut f: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        for _ in 0..100 {
            if f().await {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("condition not reached in 5s");
    }

    #[test]
    fn from_flags_parses_service_range() {
        let cfg = NodeIpamConfig::from_flags("10.0.0.0/16", 24, Some("10.0.0.0/23")).unwrap();
        assert_eq!(cfg.service_cidr, Some(net("10.0.0.0/23")));
        assert!(NodeIpamConfig::from_flags("10.0.0.0/16", 24, Some("nope")).is_err());
        assert_eq!(
            NodeIpamConfig::from_flags("10.0.0.0/16", 24, None)
                .unwrap()
                .service_cidr,
            None
        );
    }

    #[test]
    fn worker_count_matches_upstream() {
        // cidrUpdateWorkers = 30, cidrUpdateRetries = 3
        // (pkg/controller/nodeipam/ipam/cidr_allocator.go).
        assert_eq!(CIDR_UPDATE_WORKERS, 30);
        assert_eq!(CIDR_UPDATE_RETRIES, 3);
    }

    // End to end: startup occupy, watch-driven allocate, release on delete.
    #[tokio::test]
    async fn run_occupies_existing_allocates_new_and_releases_deleted() {
        let storage = Arc::new(MemoryStorage::new());
        put(&storage, &node_with_cidr("old", Some("10.244.0.0/24"))).await;
        // A /23 holds exactly two /24s, so the range is full until "old" goes.
        let cfg = NodeIpamConfig::new("10.244.0.0/23", 24).unwrap();
        let s = storage.clone();
        tokio::spawn(async move { run_node_ipam(s, cfg).await });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        put(&storage, &node_with_cidr("new", None)).await;
        let s = storage.clone();
        wait_for(|| {
            let s = s.clone();
            async move { cidr_of(&s, "new").await.is_some() }
        })
        .await;
        // "old" was occupied at startup, so "new" cannot have 10.244.0.0/24.
        assert_eq!(
            cidr_of(&storage, "new").await.as_deref(),
            Some("10.244.1.0/24")
        );

        // Deleting "old" frees 10.244.0.0/24 for the next node.
        storage
            .delete(&build_key("nodes", None, "old"))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        put(&storage, &node_with_cidr("third", None)).await;
        let s = storage.clone();
        wait_for(|| {
            let s = s.clone();
            async move { cidr_of(&s, "third").await.is_some() }
        })
        .await;
        assert_eq!(
            cidr_of(&storage, "third").await.as_deref(),
            Some("10.244.0.0/24")
        );
    }
}
