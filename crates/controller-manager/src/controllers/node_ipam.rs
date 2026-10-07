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
//! `--cluster-cidr`). [`CidrSet`] is IPv4+IPv6; the [`RangeAllocator`]
//! holds one [`CidrSet`] per `--cluster-cidr` entry (IPv4, IPv6 or both).

use std::collections::HashMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use ipnet::IpNet;
use rusternetes_common::resources::{EventSource, EventType, Node, ObjectReference};
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

/// Default IPv4 node CIDR mask size (upstream `defaultNodeMaskCIDRIPv4`,
/// `cmd/kube-controller-manager/app/core.go:79`).
pub const DEFAULT_NODE_MASK_CIDR_IPV4: u8 = 24;
/// Default IPv6 node CIDR mask size (upstream `defaultNodeMaskCIDRIPv6`,
/// `cmd/kube-controller-manager/app/core.go:81`).
pub const DEFAULT_NODE_MASK_CIDR_IPV6: u8 = 64;

/// The `--node-cidr-mask-size`, `--node-cidr-mask-size-ipv4` and
/// `--node-cidr-mask-size-ipv6` flags; `0` means "not set", as upstream's
/// `NodeIPAMControllerConfiguration` (`cmd/kube-controller-manager/app/options/nodeipamcontroller.go:39-41`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeCidrMaskSizes {
    pub general: u8,
    pub ipv4: u8,
    pub ipv6: u8,
}

/// Static configuration for pod-CIDR allocation, parsed from the
/// `--cluster-cidr` / `--node-cidr-mask-size*` flags.
#[derive(Debug, Clone)]
pub struct NodeIpamConfig {
    /// The cluster pod networks, one per IP family at most two (upstream
    /// `CIDRAllocatorParams.ClusterCIDRs`); `cidr_sets[idx]` and
    /// `node_masks[idx]` are mapped to it by index.
    pub cluster_cidrs: Vec<IpNet>,
    /// Prefix length of each per-node subnet, one per entry of
    /// `cluster_cidrs` (upstream `NodeCIDRMaskSizes`).
    pub node_masks: Vec<u8>,
    /// Service CIDRs to keep out of the allocatable range when they overlap a
    /// cluster CIDR (upstream `ServiceCIDR` then `SecondaryServiceCIDR`).
    pub service_cidrs: Vec<IpNet>,
}

fn parse_cidr_list(flag: &str, list: &str) -> Result<Vec<IpNet>, String> {
    list.trim()
        .split(',')
        .map(|c| {
            c.trim()
                .parse::<IpNet>()
                .map(|n| n.trunc())
                .map_err(|e| format!("invalid {flag} entry {c:?}: {e}"))
        })
        .collect()
}

/// Upstream `netutils.IsDualStackCIDRs`: both IP families present.
fn is_dual_stack(cidrs: &[IpNet]) -> bool {
    cidrs.iter().any(|c| matches!(c, IpNet::V4(_)))
        && cidrs.iter().any(|c| matches!(c, IpNet::V6(_)))
}

impl NodeIpamConfig {
    /// Single-stack convenience: `--cluster-cidr` plus `--node-cidr-mask-size`.
    #[cfg(test)]
    pub fn new(cluster_cidr: &str, node_mask: u8) -> Result<Self, String> {
        Self::from_flags(
            cluster_cidr,
            NodeCidrMaskSizes {
                general: node_mask,
                ..Default::default()
            },
            None,
        )
    }

    /// Parse `--cluster-cidr` (comma-separated, one or two entries),
    /// `--node-cidr-mask-size*` and the optional `--service-cluster-ip-range`
    /// (one or two entries).
    ///
    /// Port of `validateCIDRs` / `setNodeCIDRMaskSizes` /
    /// `newNodeIpamController` (`cmd/kube-controller-manager/app/core.go`,
    /// `:967`, `:1009`, `:114-147`) and `NodeIPAMControllerOptions.Validate`
    /// (`options/nodeipamcontroller.go:79`). Deviation: an unparsable service
    /// CIDR is an error here (upstream only logs a warning).
    pub fn from_flags(
        cluster_cidr: &str,
        masks: NodeCidrMaskSizes,
        service_cidr: Option<&str>,
    ) -> Result<Self, String> {
        let cluster_cidrs = parse_cidr_list("--cluster-cidr", cluster_cidr)?;
        // validateCIDRs
        if cluster_cidrs.len() > 1 && !is_dual_stack(&cluster_cidrs) {
            return Err(format!(
                "len of ClusterCIDRs=={} and they are not configured as dual stack (at least one from each IPFamily",
                cluster_cidrs.len()
            ));
        }
        if cluster_cidrs.len() > 2 {
            return Err(format!(
                "length of clusterCIDRs is:{} more than max allowed of 2",
                cluster_cidrs.len()
            ));
        }

        let service_cidrs = match service_cidr.map(str::trim).filter(|s| !s.is_empty()) {
            None => Vec::new(),
            Some(list) => {
                let svc = parse_cidr_list("--service-cluster-ip-range", list)?;
                if svc.len() > 2 {
                    return Err(
                        "--service-cluster-ip-range can not contain more than two entries".into(),
                    );
                }
                if svc.len() == 2 && !is_dual_stack(&svc) {
                    return Err(
                        "serviceCIDR and secondaryServiceCIDR are not dualstack (from different IPfamiles)"
                            .into(),
                    );
                }
                svc
            }
        };

        let node_masks = Self::node_cidr_mask_sizes(masks, &cluster_cidrs)?;
        for (cidr, mask) in cluster_cidrs.iter().zip(&node_masks) {
            let width = if matches!(cidr, IpNet::V4(_)) {
                32
            } else {
                128
            };
            if *mask > width {
                return Err(format!(
                    "--node-cidr-mask-size {mask} exceeds {width} for {cidr}"
                ));
            }
            if *mask < cidr.prefix_len() {
                return Err(format!(
                    "--node-cidr-mask-size {mask} is shorter than the cluster CIDR prefix /{} of {cidr}",
                    cidr.prefix_len()
                ));
            }
        }
        Ok(Self {
            cluster_cidrs,
            node_masks,
            service_cidrs,
        })
    }

    /// Upstream `setNodeCIDRMaskSizes` (`core.go:1009-1078`): one mask per
    /// cluster CIDR, in cluster-CIDR order (`sortedSizes`).
    fn node_cidr_mask_sizes(m: NodeCidrMaskSizes, clusters: &[IpNet]) -> Result<Vec<u8>, String> {
        let sorted = |v4: u8, v6: u8| -> Vec<u8> {
            clusters
                .iter()
                .map(|c| if matches!(c, IpNet::V6(_)) { v6 } else { v4 })
                .collect()
        };
        let (mut v4, mut v6) = (DEFAULT_NODE_MASK_CIDR_IPV4, DEFAULT_NODE_MASK_CIDR_IPV6);
        // case one: cluster is dualstack
        if clusters.len() > 1 {
            if m.general != 0 {
                return Err(
                    "usage of --node-cidr-mask-size is not allowed with dual-stack clusters".into(),
                );
            }
            if m.ipv4 != 0 {
                v4 = m.ipv4;
            }
            if m.ipv6 != 0 {
                v6 = m.ipv6;
            }
            return Ok(sorted(v4, v6));
        }
        let single_stack_v6 = matches!(clusters[0], IpNet::V6(_));
        if m.general != 0 {
            if m.ipv4 != 0 || m.ipv6 != 0 {
                return Err("usage of --node-cidr-mask-size-ipv4 and --node-cidr-mask-size-ipv6 is not allowed if --node-cidr-mask-size is set. For dual-stack clusters please unset it and use IPFamily specific flags".into());
            }
            return Ok(sorted(m.general, m.general));
        }
        if m.ipv4 != 0 {
            if single_stack_v6 {
                return Err(
                    "usage of --node-cidr-mask-size-ipv4 is not allowed for a single-stack IPv6 cluster"
                        .into(),
                );
            }
            v4 = m.ipv4;
        }
        if m.ipv6 != 0 {
            if !single_stack_v6 {
                return Err(
                    "usage of --node-cidr-mask-size-ipv6 is not allowed for a single-stack IPv4 cluster"
                        .into(),
                );
            }
            v6 = m.ipv6;
        }
        Ok(sorted(v4, v6))
    }

    /// Keep `service_cidr` out of the allocatable range
    /// (upstream `rangeAllocator.filterOutServiceRange`).
    #[cfg(test)]
    #[must_use]
    pub fn with_service_cidr(mut self, service_cidr: IpNet) -> Self {
        self.service_cidrs.push(service_cidr.trunc());
        self
    }
}

/// Errors from [`CidrSet`]; the wording follows upstream's `cidr_set.go`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CidrSetError {
    /// `ErrCIDRRangeNoCIDRsRemaining`.
    NoCidrsRemaining,
    /// `ErrCIDRSetSubNetTooBig`.
    SubnetTooBig,
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
            Self::SubnetTooBig => {
                f.write_str("New CIDR set failed; the node CIDR size is too big")
            }
            Self::InvalidMask(m) | Self::OutOfRange(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for CidrSetError {}

/// The subnet mask size cannot be greater than 16 more than the cluster mask
/// size for an IPv6 cluster CIDR. Upstream `clusterSubnetMaxDiff`
/// (`cidrset/cidr_set.go:57`; limited by the uncompressed bitmap).
const CLUSTER_SUBNET_MAX_DIFF: u8 = 16;

/// A point-in-time read of one set's `node_ipam_controller_cidrset_*` series
/// (upstream `cidrset/metrics.go`: `cidrset_cidrs_allocations_total`,
/// `cidrset_cidrs_releases_total`, `cirdset_max_cidrs`, `cidrset_usage_cidrs`,
/// `cidrset_allocation_tries_per_request`), keyed by the `clusterCIDR` label.
#[allow(dead_code)] // exposition on /metrics: #2410 (the series are tracked and tested)
#[derive(Debug, Clone, PartialEq)]
pub struct CidrSetMetrics {
    /// The `clusterCIDR` label.
    pub label: String,
    pub allocations: u64,
    pub releases: u64,
    pub max_cidrs: u64,
    /// Fraction of blocks in use, `allocatedCIDRs / maxCIDRs`.
    pub usage: f64,
    /// Sum and count of the `allocation_tries_per_request` histogram.
    pub allocation_tries_sum: f64,
    pub allocation_tries_count: u64,
}

struct CidrSetInner {
    /// Bitmap of allocated blocks, grown lazily (upstream `used big.Int`).
    used: Vec<u64>,
    /// Number of CIDRs allocated (upstream `allocatedCIDRs`).
    allocated: u64,
    /// Next CIDR index that should be free (upstream `nextCandidate`).
    next_candidate: u64,
    allocations: u64,
    releases: u64,
    tries_sum: f64,
    tries_count: u64,
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

/// An address as a big-endian integer; an IPv4 address occupies the low 32
/// bits (the arithmetic below is generic over the 32/128-bit width).
fn addr_bits(a: IpAddr) -> u128 {
    match a {
        IpAddr::V4(v4) => u128::from(u32::from(v4)),
        IpAddr::V6(v6) => u128::from(v6),
    }
}

/// Manages a set of CIDR ranges from which blocks of IPs can be allocated.
/// Port of upstream `cidrset.CidrSet` (`cidr_set.go`), IPv4 and IPv6.
pub struct CidrSet {
    cluster: IpNet,
    /// Address width in bits: 32 or 128.
    width: u32,
    node_mask: u8,
    max_cidrs: u64,
    /// Upstream `label` (`clusterCIDR.String()`), identifies the metrics.
    #[allow(dead_code)] // read by `metrics()`, see above
    label: String,
    inner: Mutex<CidrSetInner>,
}

impl CidrSet {
    /// Upstream `NewCIDRSet`. `cluster` must be network-aligned.
    pub fn new(cluster: IpNet, node_mask: u8) -> Result<Self, CidrSetError> {
        let width: u32 = if cluster.addr().is_ipv4() { 32 } else { 128 };
        // `!cluster.IP.To4() && subNetMaskSize-clusterMaskSize > clusterSubnetMaxDiff`
        // (`cidr_set.go:74`); written without underflow.
        if width == 128 && node_mask > cluster.prefix_len().saturating_add(CLUSTER_SUBNET_MAX_DIFF)
        {
            return Err(CidrSetError::SubnetTooBig);
        }
        if u32::from(node_mask) > width || node_mask < cluster.prefix_len() {
            return Err(CidrSetError::InvalidMask(format!(
                "node CIDR mask /{node_mask} does not fit cluster CIDR {cluster}"
            )));
        }
        // getMaxCIDRs
        let max_cidrs = 1u64 << (node_mask - cluster.prefix_len());
        Ok(Self {
            width,
            node_mask,
            max_cidrs,
            label: cluster.to_string(),
            cluster,
            inner: Mutex::new(CidrSetInner {
                used: Vec::new(),
                allocated: 0,
                next_candidate: 0,
                allocations: 0,
                releases: 0,
                tries_sum: 0.0,
                tries_count: 0,
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

    /// The set's metric series (upstream `cidrset/metrics.go`).
    #[allow(dead_code)] // exposition on /metrics is a follow-up
    pub fn metrics(&self) -> CidrSetMetrics {
        let s = self.inner.lock().expect("cidr set poisoned");
        CidrSetMetrics {
            label: self.label.clone(),
            allocations: s.allocations,
            releases: s.releases,
            max_cidrs: self.max_cidrs,
            usage: s.allocated as f64 / self.max_cidrs as f64,
            allocation_tries_sum: s.tries_sum,
            allocation_tries_count: s.tries_count,
        }
    }

    /// All-ones of the address width.
    fn full(&self) -> u128 {
        if self.width == 128 {
            u128::MAX
        } else {
            (1u128 << self.width) - 1
        }
    }

    /// The host bits of a `prefix`-length mask.
    fn host_mask(&self, prefix: u8) -> u128 {
        match 1u128.checked_shl(self.width - u32::from(prefix)) {
            Some(v) => v - 1,
            None => u128::MAX,
        }
    }

    fn net_mask(&self, prefix: u8) -> u128 {
        self.full() & !self.host_mask(prefix)
    }

    /// Upstream `indexToCIDRBlock`.
    fn index_to_cidr_block(&self, index: u64) -> IpNet {
        let shifted = u128::from(index)
            .checked_shl(self.width - u32::from(self.node_mask))
            .unwrap_or(0);
        let ip = addr_bits(self.cluster.network()) | shifted;
        let addr = if self.width == 32 {
            IpAddr::V4(Ipv4Addr::from(ip as u32))
        } else {
            IpAddr::V6(Ipv6Addr::from(ip))
        };
        IpNet::new(addr, self.node_mask).expect("mask validated in new")
    }

    /// Allocates the next free CIDR range, marking it occupied (upstream
    /// `AllocateNext`).
    pub fn allocate_next(&self) -> Result<IpNet, CidrSetError> {
        let mut s = self.inner.lock().expect("cidr set poisoned");
        if s.allocated == self.max_cidrs {
            return Err(CidrSetError::NoCidrsRemaining);
        }
        let mut candidate = s.next_candidate;
        let mut tries = 0u64;
        while tries < self.max_cidrs {
            if !s.bit(candidate) {
                break;
            }
            candidate = (candidate + 1) % self.max_cidrs;
            tries += 1;
        }
        s.next_candidate = (candidate + 1) % self.max_cidrs;
        s.set_bit(candidate, true);
        s.allocated += 1;
        // Update metrics
        s.allocations += 1;
        s.tries_sum += tries as f64;
        s.tries_count += 1;
        Ok(self.index_to_cidr_block(candidate))
    }

    /// Upstream `getIndexForIP`.
    fn index_for_ip(&self, ip: u128) -> Result<u64, CidrSetError> {
        let x = addr_bits(self.cluster.network()) ^ ip;
        let idx = x
            .checked_shr(self.width - u32::from(self.node_mask))
            .unwrap_or(0);
        if idx >= u128::from(self.max_cidrs) {
            let addr = if self.width == 32 {
                IpAddr::V4(Ipv4Addr::from(ip as u32))
            } else {
                IpAddr::V6(Ipv6Addr::from(ip))
            };
            return Err(CidrSetError::OutOfRange(format!(
                "CIDR: {addr}/{} is out of the range of CIDR allocator",
                self.node_mask
            )));
        }
        Ok(idx as u64)
    }

    /// Upstream `getBeginningAndEndIndices`: the inclusive range of node-sized
    /// blocks that `cidr` covers.
    fn get_indices(&self, cidr: IpNet) -> Result<(u64, u64), CidrSetError> {
        // A CIDR of the other address family cannot lie in this cluster CIDR
        // (Go's `IPNet.Contains` is false across families).
        if cidr.addr().is_ipv4() != self.cluster.addr().is_ipv4() {
            return Err(CidrSetError::OutOfRange(format!(
                "cidr {cidr} is out the range of cluster cidr {}",
                self.cluster
            )));
        }
        let cluster_ip = addr_bits(self.cluster.network());
        let cluster_mask = self.net_mask(self.cluster.prefix_len());
        let cidr_ip = addr_bits(cidr.network());
        let cidr_mask = self.net_mask(cidr.prefix_len());
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
            let node_mask = self.net_mask(self.node_mask);
            begin = self.index_for_ip(cidr_ip & node_mask)?;
            end = self.index_for_ip((cidr_ip | !cidr_mask & self.full()) & node_mask)?;
        }
        Ok((begin, end))
    }

    /// Releases the given CIDR range (upstream `Release`).
    pub fn release(&self, cidr: IpNet) -> Result<(), CidrSetError> {
        let (begin, end) = self.get_indices(cidr)?;
        let mut s = self.inner.lock().expect("cidr set poisoned");
        for i in begin..=end {
            // Only change the counter if we change the bit, to prevent double
            // counting.
            if s.bit(i) {
                s.set_bit(i, false);
                s.allocated -= 1;
                s.releases += 1;
            }
        }
        Ok(())
    }

    /// Marks the given CIDR range as used. Succeeds even if it was previously
    /// used (upstream `Occupy`).
    pub fn occupy(&self, cidr: IpNet) -> Result<(), CidrSetError> {
        let (begin, end) = self.get_indices(cidr)?;
        let mut s = self.inner.lock().expect("cidr set poisoned");
        for i in begin..=end {
            if !s.bit(i) {
                s.set_bit(i, true);
                s.allocated += 1;
                s.allocations += 1;
            }
        }
        Ok(())
    }
}

/// Upstream `nodeForCIDRMergePatch` (`component-helpers/node/util/cidr.go:29`):
/// `podCIDR` is always sent, `podCIDRs` is `omitempty`.
fn node_cidr_merge_patch(cidrs: &[String]) -> serde_json::Value {
    let mut spec = serde_json::Map::new();
    spec.insert("podCIDR".into(), cidrs[0].clone().into());
    if !cidrs.is_empty() {
        spec.insert("podCIDRs".into(), cidrs.to_vec().into());
    }
    serde_json::json!({ "spec": spec })
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
    /// One set per cluster CIDR, mapped by index (upstream `cidrSets`).
    cidr_sets: Vec<CidrSet>,
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
        // create a cidrSet for each cidr we operate on
        let cidr_sets = cfg
            .cluster_cidrs
            .iter()
            .zip(&cfg.node_masks)
            .map(|(cidr, mask)| CidrSet::new(*cidr, *mask).map_err(|e| e.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let ra = Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            storage,
            cidr_sets,
            queue: WorkQueue::new(),
        };
        // ServiceCIDR, then SecondaryServiceCIDR.
        for svc in &cfg.service_cidrs {
            ra.filter_out_service_range(*svc);
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
    fn filter_out_service_range(&self, service_cidr: IpNet) {
        for (idx, set) in self.cidr_sets.iter().enumerate() {
            let cluster = set.cluster;
            // if they don't overlap then ignore the filtering
            let overlaps = cluster.contains(&service_cidr.network())
                || service_cidr.contains(&cluster.network());
            if !overlaps {
                continue;
            }
            if let Err(e) = set.occupy(service_cidr) {
                error!(
                    "Error filtering out service cidr {service_cidr} out cluster cidr {cluster} (index {idx}): {e}"
                );
            }
        }
    }

    /// Marks `node.spec.podCIDRs` as used (upstream `occupyCIDRs`).
    fn occupy_cidrs(&self, node: &Node) -> Result<()> {
        for (idx, cidr) in node_pod_cidrs(node).iter().enumerate() {
            let pod_cidr: IpNet = cidr.parse().map_err(|_| {
                anyhow::anyhow!("failed to parse node {}, CIDR {}", node.metadata.name, cidr)
            })?;
            // Upstream: an index beyond the configured cluster CIDRs cannot be
            // locked (cluster went from dual-stack to single-stack).
            if idx >= self.cidr_sets.len() {
                anyhow::bail!(
                    "node:{} has an allocated cidr: {} at index:{} that does not exist in cluster cidrs configuration",
                    node.metadata.name,
                    cidr,
                    idx
                );
            }
            self.cidr_sets[idx].occupy(pod_cidr).map_err(|e| {
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
        let mut allocated: Vec<IpNet> = Vec::with_capacity(self.cidr_sets.len());
        for (idx, set) in self.cidr_sets.iter().enumerate() {
            match set.allocate_next() {
                Ok(c) => allocated.push(c),
                Err(e) => {
                    // Deviation from upstream, which leaks the CIDRs already
                    // taken from earlier sets (and again on every requeue):
                    // nothing was written, so handing them back is safe.
                    self.release_allocated(&allocated);
                    self.record_node_status_change(node, "CIDRNotAvailable")
                        .await;
                    anyhow::bail!("failed to allocate cidr from cluster cidr at idx:{idx}: {e}");
                }
            }
        }
        debug!(
            "Putting node {} with CIDRs {:?} into the work queue",
            node.metadata.name, allocated
        );
        self.update_cidrs_allocation(&node.metadata.name, allocated)
            .await
    }

    /// Marks `node.spec.podCIDRs` as unused (upstream `ReleaseCIDR`).
    pub fn release_cidr(&self, node: &Node) -> Result<()> {
        for (idx, cidr) in node_pod_cidrs(node).iter().enumerate() {
            let pod_cidr: IpNet = cidr.parse().map_err(|_| {
                anyhow::anyhow!(
                    "failed to parse CIDR {} on Node {}",
                    cidr,
                    node.metadata.name
                )
            })?;
            if idx >= self.cidr_sets.len() {
                anyhow::bail!(
                    "node:{} has an allocated cidr: {} at index:{} that does not exist in cluster cidrs configuration",
                    node.metadata.name,
                    cidr,
                    idx
                );
            }
            debug!("Release CIDR {} for node {}", cidr, node.metadata.name);
            self.cidr_sets[idx]
                .release(pod_cidr)
                .map_err(|e| anyhow::anyhow!("error when releasing CIDR {cidr}: {e}"))?;
        }
        Ok(())
    }

    /// Releases `allocated[idx]` back to `cidr_sets[idx]`.
    fn release_allocated(&self, allocated: &[IpNet]) {
        for (idx, cidr) in allocated.iter().enumerate() {
            if let Err(e) = self.cidr_sets[idx].release(*cidr) {
                error!("Error releasing allocated CIDR {cidr} (index {idx}): {e}");
            }
        }
    }

    /// Assigns `allocated` to the node and writes it (upstream
    /// `updateCIDRsAllocation`).
    async fn update_cidrs_allocation(&self, node_name: &str, allocated: Vec<IpNet>) -> Result<()> {
        let key = build_key("nodes", None, node_name);
        let cidrs: Vec<String> = allocated.iter().map(IpNet::to_string).collect();
        let node: Node = match self.storage.get(&key).await {
            Ok(n) => n,
            Err(e) => {
                // Deviation from upstream, which leaks the CIDR here: nothing
                // was written, so handing it back is always safe.
                error!(
                    "Failed while getting node {node_name} for updating Node.Spec.PodCIDRs: {e}"
                );
                self.release_allocated(&allocated);
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
            debug!("Node {node_name} already has allocated CIDRs {allocated:?}. It matches the proposed one");
            return Ok(());
        }
        // The node has CIDRs: release the reserved one.
        if !existing.is_empty() {
            error!(
                "Node {node_name} already has a CIDR allocated ({existing:?}). Releasing the new one {allocated:?}"
            );
            self.release_allocated(&allocated);
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
            self.release_allocated(&allocated);
        }
        Err(err.into())
    }

    /// Sets `spec.podCIDR`/`podCIDRs` on the node (upstream
    /// `nodeutil.PatchNodeCIDRs`, `component-helpers/node/util/cidr.go:40`).
    /// Sends the same strategic-merge patch body upstream builds
    /// (`nodeForCIDRMergePatch`, `cidr.go:29-38`: `{"spec":{"podCIDR":c[0],
    /// "podCIDRs":c}}`, `podCIDRs` omitempty) through
    /// [`Storage::patch_strategic_merge`] (a real PATCH in API mode).
    async fn patch_node_cidrs(&self, node_name: &str, cidrs: &[String]) -> Result<(), Error> {
        let key = build_key("nodes", None, node_name);
        let patch = node_cidr_merge_patch(cidrs);
        let _: Node = self.storage.patch_strategic_merge(&key, &patch).await?;
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

    /// The patch body is upstream's `nodeForCIDRMergePatch`
    /// (`cidr.go:29-38`), and applying it leaves unrelated spec/metadata alone.
    #[tokio::test]
    async fn patch_node_cidrs_sends_only_the_cidr_delta() {
        assert_eq!(
            node_cidr_merge_patch(&["10.0.0.0/24".to_string()]),
            serde_json::json!({"spec": {"podCIDR": "10.0.0.0/24", "podCIDRs": ["10.0.0.0/24"]}})
        );
        let storage = Arc::new(MemoryStorage::new());
        let key = build_key("nodes", None, "n1");
        let _: serde_json::Value = storage
            .create(
                &key,
                &serde_json::json!({"apiVersion": "v1", "kind": "Node",
                    "metadata": {"name": "n1", "labels": {"x": "y"}},
                    "spec": {"unschedulable": true}}),
            )
            .await
            .unwrap();
        let patched: Node = storage
            .patch_strategic_merge(&key, &node_cidr_merge_patch(&["10.0.0.0/24".to_string()]))
            .await
            .unwrap();
        let spec = patched.spec.unwrap();
        assert_eq!(spec.pod_cidr.as_deref(), Some("10.0.0.0/24"));
        assert_eq!(spec.unschedulable, Some(true));
        assert_eq!(patched.metadata.labels.unwrap()["x"], "y");
    }

    fn ipn(s: &str) -> IpNet {
        s.parse::<IpNet>().unwrap().trunc()
    }

    fn set(cluster: &str, mask: u8) -> CidrSet {
        CidrSet::new(ipn(cluster), mask).unwrap()
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
        assert_eq!(a.allocate_next().unwrap(), ipn("127.123.234.0/30"));
        assert_eq!(a.allocate_next(), Err(CidrSetError::NoCidrsRemaining));
        a.release(ipn("127.123.234.0/30")).unwrap();
        assert_eq!(a.allocate_next().unwrap(), ipn("127.123.234.0/30"));
        assert!(a.allocate_next().is_err());
    }

    // TestIndexToCIDRBlock (IPv4 rows)
    #[test]
    fn cidr_set_index_to_cidr_block() {
        let a = set("127.123.3.0/16", 24);
        assert_eq!(a.index_to_cidr_block(0), ipn("127.123.0.0/24"));
        assert_eq!(a.index_to_cidr_block(15), ipn("127.123.15.0/24"));
        assert_eq!(a.index_to_cidr_block(255), ipn("127.123.255.0/24"));
        let b = set("10.0.0.0/8", 26);
        assert_eq!(b.index_to_cidr_block(1), ipn("10.0.0.64/26"));
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
                a.occupy(ipn(cidr)).unwrap();
            } else {
                a.release(ipn(cidr)).unwrap();
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
        a.occupy(ipn("127.0.0.0/8")).unwrap();
        assert_eq!(a.allocated(), 256);
        let b = set("127.0.0.0/8", 16);
        b.occupy(ipn("127.0.0.0/2")).unwrap();
        assert_eq!(b.allocated(), 256);
        // A /16 at index 123.
        let c = set("127.0.0.0/8", 16);
        c.occupy(ipn("127.123.0.0/16")).unwrap();
        assert_eq!(c.allocated(), 1);
        assert_eq!(c.get_indices(ipn("127.123.0.0/16")).unwrap(), (123, 123));
        // A /12 spans 16 /16s.
        assert_eq!(c.get_indices(ipn("127.16.0.0/12")).unwrap(), (16, 31));
        // Out of the cluster range.
        assert!(c.occupy(ipn("128.0.0.0/16")).is_err());
        assert!(c.release(ipn("10.0.0.0/16")).is_err());
    }

    // Test_getMaxCIDRs
    #[test]
    fn cidr_set_max_cidrs() {
        assert_eq!(set("10.0.0.0/16", 24).max_cidrs(), 256);
        assert!(CidrSet::new(ipn("10.0.0.0/16"), 8).is_err());
        assert!(CidrSet::new(ipn("10.0.0.0/16"), 33).is_err());
    }

    // TestIndexToCIDRBlock (IPv4 /32 and every IPv6 row)
    #[test]
    fn cidr_set_index_to_cidr_block_v6() {
        // (cluster, mask, index, want) verbatim from cidr_set_test.go:86-196.
        let rows = [
            ("192.168.5.219/28", 32, 5, "192.168.5.213/32"),
            ("2001:0db8:1234:3::/48", 64, 0, "2001:db8:1234::/64"),
            ("2001:0db8:1234::/48", 64, 15, "2001:db8:1234:f::/64"),
            (
                "2001:0db8:85a3::8a2e:0370:7334/50",
                63,
                6425,
                "2001:db8:85a3:3232::/63",
            ),
            ("2001:0db8::/32", 48, 0, "2001:db8::/48"),
            ("2001:0db8::/32", 48, 15, "2001:db8:f::/48"),
            (
                "2001:0db8:85a3::8a2e:0370:7334/32",
                48,
                6425,
                "2001:db8:1919::/48",
            ),
            ("2001:0db8:1234:ff00::/56", 72, 0, "2001:db8:1234:ff00::/72"),
            (
                "2001:0db8:1234:ff00::/56",
                72,
                15,
                "2001:db8:1234:ff00:f00::/72",
            ),
            (
                "2001:0db8:1234:ff00::0370:7334/56",
                72,
                6425,
                "2001:db8:1234:ff19:1900::/72",
            ),
            (
                "2001:0db8:1234:0:1234::/80",
                96,
                0,
                "2001:db8:1234:0:1234::/96",
            ),
            (
                "2001:0db8:1234:0:1234::/80",
                96,
                15,
                "2001:db8:1234:0:1234:f::/96",
            ),
            (
                "2001:0db8:1234:ff00::0370:7334/80",
                96,
                6425,
                "2001:db8:1234:ff00:0:1919::/96",
            ),
        ];
        for (cluster, mask, index, want) in rows {
            let a = set(cluster, mask);
            assert_eq!(
                a.index_to_cidr_block(index).to_string(),
                want,
                "{cluster} /{mask} #{index}"
            );
        }
    }

    // TestCIDRSetv6
    #[test]
    fn cidr_set_v6_allocation_and_subnet_too_big() {
        let a = set("127.0.0.0/8", 32);
        assert_eq!(a.allocate_next().unwrap(), ipn("127.0.0.0/32"));
        assert_eq!(a.allocate_next().unwrap(), ipn("127.0.0.1/32"));
        // "Max cluster subnet size with IPv6": 49 - 32 > clusterSubnetMaxDiff (16).
        let err = CidrSet::new(ipn("beef:1234::/32"), 49).err().unwrap();
        assert_eq!(
            err.to_string(),
            "New CIDR set failed; the node CIDR size is too big"
        );
        let b = set("2001:beef:1234:369b::/60", 64);
        assert_eq!(b.allocate_next().unwrap(), ipn("2001:beef:1234:3690::/64"));
        assert_eq!(b.allocate_next().unwrap(), ipn("2001:beef:1234:3691::/64"));
    }

    // TestGetBitforCIDR / TestOccupy (IPv6 rows)
    #[test]
    fn cidr_set_v6_indices_and_occupy() {
        let a = set("be00::/8", 16);
        assert_eq!(a.get_indices(ipn("be00::/16")).unwrap(), (0, 0));
        let b = set("2001:beef:1200::/40", 48);
        b.occupy(ipn("2001:beef:1200::/40")).unwrap();
        assert_eq!(b.allocated(), 256);
        let c = set("2001:beef:1200::/40", 48);
        c.occupy(ipn("2001:beef:1234::/34")).unwrap();
        assert_eq!(c.allocated(), 256);
        // A v4 CIDR is out of range of a v6 cluster, and vice versa.
        assert!(c.occupy(ipn("10.0.0.0/16")).is_err());
        assert!(set("10.0.0.0/8", 16).occupy(ipn("be00::/16")).is_err());
        // Outside the cluster.
        assert!(c.release(ipn("2001:beef:3400::/48")).is_err());
    }

    // Test_getMaxCIDRs (IPv6 row)
    #[test]
    fn cidr_set_max_cidrs_v6() {
        assert_eq!(set("2001:db8::/48", 64).max_cidrs(), 65536);
    }

    fn expect_metrics(a: &CidrSet, usage: f64, allocs: u64, releases: u64, tries: f64, max: u64) {
        let m = a.metrics();
        assert_eq!(
            (
                m.usage,
                m.allocations,
                m.releases,
                m.allocation_tries_sum,
                m.max_cidrs
            ),
            (usage, allocs, releases, tries, max)
        );
    }

    // TestCidrSetMetrics
    #[test]
    fn cidr_set_metrics() {
        let a = set("10.0.0.0/16", 24);
        assert_eq!(a.metrics().label, "10.0.0.0/16");
        expect_metrics(&a, 0.0, 0, 0, 0.0, 256);
        for i in 1..=256u64 {
            a.allocate_next().unwrap();
            expect_metrics(&a, i as f64 / 256.0, i, 0, 0.0, 256);
        }
        a.release(ipn("10.0.0.0/16")).unwrap();
        expect_metrics(&a, 0.0, 256, 256, 0.0, 256);
        a.occupy(ipn("10.0.0.0/16")).unwrap();
        expect_metrics(&a, 1.0, 512, 256, 0.0, 256);
    }

    // TestCidrSetMetricsHistogram
    #[test]
    fn cidr_set_metrics_histogram() {
        let a = set("10.0.0.0/16", 24);
        a.occupy(ipn("10.0.0.0/17")).unwrap();
        expect_metrics(&a, 0.5, 128, 0, 0.0, 256);
        // Occupy does not move nextCandidate, so AllocateNext walks 128 bits.
        a.allocate_next().unwrap();
        expect_metrics(&a, 129.0 / 256.0, 129, 0, 128.0, 256);
    }

    // TestCidrSetMetricsDual
    #[test]
    fn cidr_set_metrics_dual() {
        let a = set("10.0.0.0/16", 24);
        let b = set("2001:db8::/48", 64);
        expect_metrics(&a, 0.0, 0, 0, 0.0, 256);
        expect_metrics(&b, 0.0, 0, 0, 0.0, 65536);
        a.occupy(ipn("10.0.0.0/16")).unwrap();
        expect_metrics(&a, 1.0, 256, 0, 0.0, 256);
        b.occupy(ipn("2001:db8::/48")).unwrap();
        expect_metrics(&b, 1.0, 65536, 0, 0.0, 65536);
        a.release(ipn("10.0.0.0/16")).unwrap();
        expect_metrics(&a, 0.0, 256, 256, 0.0, 256);
        b.release(ipn("2001:db8::/48")).unwrap();
        expect_metrics(&b, 0.0, 65536, 65536, 0.0, 65536);
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
        assert_eq!(
            ra.cidr_sets[0].allocate_next().unwrap(),
            ipn("10.244.1.0/24")
        );
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
            ra.cidr_sets[0].allocate_next().unwrap();
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
        assert_eq!(
            ra.cidr_sets[0].allocate_next().unwrap(),
            ipn("10.10.0.0/24")
        );
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
        assert_eq!(
            ra.cidr_sets[0].allocate_next().unwrap(),
            ipn("10.10.1.0/24")
        );

        n.metadata.deletion_timestamp = Some(chrono::Utc::now());
        storage
            .update(&build_key("nodes", None, "node0"), &n)
            .await
            .unwrap();
        ra.sync_node("nodes/node0").await.unwrap();
        assert_eq!(
            ra.cidr_sets[0].allocate_next().unwrap(),
            ipn("10.10.2.0/24")
        );
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
        assert_eq!(ra.cidr_sets[0].allocated(), 0);
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
        assert_eq!(ra.cidr_sets[0].allocated(), 0);
    }

    // filterOutServiceRange
    #[tokio::test]
    async fn service_cidr_is_filtered_out() {
        let storage = Arc::new(MemoryStorage::new());
        let cfg = NodeIpamConfig::new("10.0.0.0/16", 24)
            .unwrap()
            .with_service_cidr(ipn("10.0.0.0/23"));
        let ra = RangeAllocator::new(storage, cfg, &[]).unwrap();
        assert_eq!(ra.cidr_sets[0].allocate_next().unwrap(), ipn("10.0.2.0/24"));
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
        let m = masks(24, 0, 0);
        let cfg = NodeIpamConfig::from_flags("10.0.0.0/16", m, Some("10.0.0.0/23")).unwrap();
        assert_eq!(cfg.service_cidrs, [ipn("10.0.0.0/23")]);
        assert!(NodeIpamConfig::from_flags("10.0.0.0/16", m, Some("nope")).is_err());
        assert!(NodeIpamConfig::from_flags("10.0.0.0/16", m, None)
            .unwrap()
            .service_cidrs
            .is_empty());
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

    // ---- dual-stack: multi-set RangeAllocator (#2409) ----

    fn node_with_cidrs(name: &str, cidrs: &[&str]) -> Node {
        let mut n = Node::new(name);
        if !cidrs.is_empty() {
            n.spec = Some(rusternetes_common::resources::NodeSpec {
                pod_cidr: Some(cidrs[0].to_string()),
                pod_cidrs: Some(cidrs.iter().map(|c| c.to_string()).collect()),
                provider_id: None,
                unschedulable: None,
                taints: None,
            });
        }
        n
    }

    async fn cidrs_of(storage: &Arc<MemoryStorage>, name: &str) -> Vec<String> {
        let n: Node = storage.get(&build_key("nodes", None, name)).await.unwrap();
        n.spec.and_then(|s| s.pod_cidrs).unwrap_or_default()
    }

    /// Builds a config from explicit per-cluster-CIDR masks, as
    /// `CIDRAllocatorParams{ClusterCIDRs, NodeCIDRMaskSizes}`.
    fn dual_cfg(clusters: &[&str], masks: &[u8]) -> NodeIpamConfig {
        NodeIpamConfig {
            cluster_cidrs: clusters.iter().map(|c| ipn(c)).collect(),
            node_masks: masks.to_vec(),
            service_cidrs: Vec::new(),
        }
    }

    // TestOccupyPreExistingCIDR (range_allocator_test.go:45-290), every row.
    #[test]
    fn occupy_pre_existing_cidr_rows() {
        // (description, existing podCIDRs, cluster cidrs, masks, ctrlCreateFail)
        type Row<'a> = (&'a str, &'a [&'a str], &'a [&'a str], &'a [u8], bool);
        let rows: &[Row<'_>] = &[
            (
                "single stack no node allocation",
                &[],
                &["10.10.0.0/16"],
                &[24],
                false,
            ),
            (
                "dual stack no node allocation",
                &[],
                &["10.10.0.0/16", "ace:cab:deca::/8"],
                &[24, 24],
                false,
            ),
            (
                "single stack correct node allocation",
                &["10.10.0.1/24"],
                &["10.10.0.0/16"],
                &[24],
                false,
            ),
            (
                "dual stack both allocated correctly",
                &["10.10.0.1/24", "a00::/86"],
                &["10.10.0.0/16", "ace:cab:deca::/8"],
                &[24, 24],
                false,
            ),
            (
                "fail, single stack incorrect node allocation",
                &["172.10.0.1/24"],
                &["10.10.0.0/16"],
                &[24],
                true,
            ),
            (
                "fail, dualstack node allocating from non existing cidr",
                &["10.10.0.1/24", "a00::/86"],
                &["10.10.0.0/16"],
                &[24],
                true,
            ),
            (
                "fail, dualstack node allocating bad v4",
                &["172.10.0.1/24", "a00::/86"],
                &["10.10.0.0/16", "ace:cab:deca::/8"],
                &[24, 24],
                true,
            ),
            (
                "fail, dualstack node allocating bad v6",
                &["10.10.0.1/24", "cdd::/86"],
                &["10.10.0.0/16", "ace:cab:deca::/8"],
                &[24, 24],
                true,
            ),
        ];
        for (desc, pod_cidrs, clusters, masks, fail) in rows {
            let storage = Arc::new(MemoryStorage::new());
            let node = node_with_cidrs("node0", pod_cidrs);
            let r = RangeAllocator::new(storage, dual_cfg(clusters, masks), &[node]);
            assert_eq!(r.is_err(), *fail, "{desc}: {:?}", r.err());
        }
    }

    // TestAllocateOrOccupyCIDRSuccess "Dualstack CIDRs v4,v6" / "v6,v4": one
    // podCIDR per cluster CIDR, in cluster-CIDR order.
    #[tokio::test]
    async fn dual_stack_allocates_one_cidr_per_family_in_config_order() {
        for (clusters, masks, want) in [
            (
                ["127.123.234.0/8", "ace:cab:deca::/84"],
                [24u8, 98],
                ["127.0.0.0/24", "ace:cab:deca::/98"],
            ),
            (
                ["ace:cab:deca::/84", "127.123.234.0/8"],
                [98u8, 24],
                ["ace:cab:deca::/98", "127.0.0.0/24"],
            ),
        ] {
            let storage = Arc::new(MemoryStorage::new());
            put(&storage, &node_with_cidrs("node0", &[])).await;
            let ra =
                RangeAllocator::new(storage.clone(), dual_cfg(&clusters, &masks), &[]).unwrap();
            ra.allocate_or_occupy_cidr(&node_with_cidrs("node0", &[]))
                .await
                .unwrap();
            assert_eq!(cidrs_of(&storage, "node0").await, want);
            // spec.podCIDR is the primary (first) one.
            assert_eq!(cidr_of(&storage, "node0").await.as_deref(), Some(want[0]));
        }
    }

    // `filterOutServiceRange` loops over every cluster CIDR; a service range
    // only touches the set of the family it overlaps, and the secondary
    // service CIDR is filtered the same way (`SecondaryServiceCIDR`).
    #[tokio::test]
    async fn dual_stack_service_cidrs_filter_only_overlapping_family() {
        let storage = Arc::new(MemoryStorage::new());
        let mut cfg = dual_cfg(&["10.0.0.0/16", "fd00::/48"], &[24, 64]);
        cfg.service_cidrs = vec![ipn("10.0.0.0/23"), ipn("fd00::/62")];
        let ra = RangeAllocator::new(storage, cfg, &[]).unwrap();
        assert_eq!(ra.cidr_sets.len(), 2);
        assert_eq!(ra.cidr_sets[0].allocate_next().unwrap(), ipn("10.0.2.0/24"));
        assert_eq!(
            ra.cidr_sets[1].allocate_next().unwrap(),
            ipn("fd00:0:0:4::/64")
        );
    }

    // TestReleaseCIDRSuccess dual-stack: both CIDRs go back to their own sets.
    #[tokio::test]
    async fn dual_stack_release_returns_each_cidr_to_its_set() {
        let storage = Arc::new(MemoryStorage::new());
        let n = node_with_cidrs("node0", &["10.10.0.0/24", "fd00::/64"]);
        let ra = RangeAllocator::new(
            storage,
            dual_cfg(&["10.10.0.0/16", "fd00::/48"], &[24, 64]),
            std::slice::from_ref(&n),
        )
        .unwrap();
        assert_eq!(ra.cidr_sets[0].allocated(), 1);
        assert_eq!(ra.cidr_sets[1].allocated(), 1);
        ra.release_cidr(&n).unwrap();
        assert_eq!(ra.cidr_sets[0].allocated(), 0);
        assert_eq!(ra.cidr_sets[1].allocated(), 0);
        // `idx >= len(r.cidrSets)`: a CIDR index with no set cannot be released.
        let single = RangeAllocator::new(
            Arc::new(MemoryStorage::new()),
            dual_cfg(&["10.10.0.0/16"], &[24]),
            &[],
        )
        .unwrap();
        assert!(single.release_cidr(&n).is_err());
    }

    // updateCIDRsAllocation "node has cidrs, release the reserved": every
    // allocated CIDR is released, one per set.
    #[tokio::test]
    async fn dual_stack_losing_race_releases_every_new_cidr() {
        let storage = Arc::new(MemoryStorage::new());
        put(
            &storage,
            &node_with_cidrs("node0", &["10.10.7.0/24", "fd00:0:0:7::/64"]),
        )
        .await;
        let ra = RangeAllocator::new(
            storage.clone(),
            dual_cfg(&["10.10.0.0/16", "fd00::/48"], &[24, 64]),
            &[],
        )
        .unwrap();
        ra.allocate_or_occupy_cidr(&node_with_cidrs("node0", &[]))
            .await
            .unwrap();
        assert_eq!(ra.cidr_sets[0].allocated(), 0);
        assert_eq!(ra.cidr_sets[1].allocated(), 0);
        assert_eq!(
            cidrs_of(&storage, "node0").await,
            ["10.10.7.0/24", "fd00:0:0:7::/64"]
        );
    }

    // Deviation from upstream (which leaks the set-0 CIDR when set 1 is
    // exhausted, then again on every rate-limited retry): nothing was
    // written, so the earlier allocations are handed back.
    #[tokio::test]
    async fn dual_stack_partial_allocation_failure_releases_earlier_sets() {
        let storage = Arc::new(MemoryStorage::new());
        let n = node_with_cidrs("node0", &[]);
        put(&storage, &n).await;
        // v6 /126 with /127 nodes -> 2 CIDRs; exhaust them.
        let ra = RangeAllocator::new(
            storage.clone(),
            dual_cfg(&["10.10.0.0/16", "fd00::/126"], &[24, 127]),
            &[],
        )
        .unwrap();
        ra.cidr_sets[1].allocate_next().unwrap();
        ra.cidr_sets[1].allocate_next().unwrap();
        assert!(ra.allocate_or_occupy_cidr(&n).await.is_err());
        assert_eq!(ra.cidr_sets[0].allocated(), 0);
        assert!(cidrs_of(&storage, "node0").await.is_empty());
        let events: Vec<Event> = storage.list(&build_prefix("events", None)).await.unwrap();
        assert!(events.iter().any(|e| e.reason == "CIDRNotAvailable"));
    }

    // ---- --cluster-cidr list / --node-cidr-mask-size-ipv4/-ipv6 ----
    // cmd/kube-controller-manager/app/core.go validateCIDRs + setNodeCIDRMaskSizes

    fn masks(general: u8, ipv4: u8, ipv6: u8) -> NodeCidrMaskSizes {
        NodeCidrMaskSizes {
            general,
            ipv4,
            ipv6,
        }
    }

    #[test]
    fn from_flags_dual_stack_uses_per_family_masks_and_defaults() {
        let cfg =
            NodeIpamConfig::from_flags("10.244.0.0/16,fd00::/48", masks(0, 0, 0), None).unwrap();
        assert_eq!(cfg.cluster_cidrs, [ipn("10.244.0.0/16"), ipn("fd00::/48")]);
        // defaultNodeMaskCIDRIPv4 = 24, defaultNodeMaskCIDRIPv6 = 64
        assert_eq!(cfg.node_masks, [24, 64]);

        // sortedSizes follows cluster-CIDR order, not v4-first.
        let cfg =
            NodeIpamConfig::from_flags("fd00::/48, 10.244.0.0/16", masks(0, 26, 56), None).unwrap();
        assert_eq!(cfg.cluster_cidrs, [ipn("fd00::/48"), ipn("10.244.0.0/16")]);
        assert_eq!(cfg.node_masks, [56, 26]);
    }

    #[test]
    fn from_flags_single_stack_mask_rules() {
        // Single-stack IPv6 defaults to /64.
        let cfg = NodeIpamConfig::from_flags("fd00::/48", masks(0, 0, 0), None).unwrap();
        assert_eq!(cfg.node_masks, [64]);
        // --node-cidr-mask-size is the reference for a single family.
        let cfg = NodeIpamConfig::from_flags("fd00::/48", masks(60, 0, 0), None).unwrap();
        assert_eq!(cfg.node_masks, [60]);
        // Family flags are accepted only for their own family.
        let cfg = NodeIpamConfig::from_flags("10.0.0.0/16", masks(0, 26, 0), None).unwrap();
        assert_eq!(cfg.node_masks, [26]);
        assert!(NodeIpamConfig::from_flags("10.0.0.0/16", masks(0, 0, 64), None).is_err());
        assert!(NodeIpamConfig::from_flags("fd00::/48", masks(0, 24, 0), None).is_err());
        // Mixing the general flag with a family flag is an error.
        assert!(NodeIpamConfig::from_flags("10.0.0.0/16", masks(24, 24, 0), None).is_err());
    }

    #[test]
    fn from_flags_dual_stack_rejects_general_mask_and_bad_lists() {
        let e =
            NodeIpamConfig::from_flags("10.0.0.0/16,fd00::/48", masks(24, 0, 0), None).unwrap_err();
        assert!(
            e.contains("--node-cidr-mask-size is not allowed with dual-stack"),
            "{e}"
        );
        // two cidrs of the same family
        let e = NodeIpamConfig::from_flags("10.0.0.0/16,10.1.0.0/16", masks(0, 0, 0), None)
            .unwrap_err();
        assert!(e.contains("not configured as dual stack"), "{e}");
        // more than two
        let e =
            NodeIpamConfig::from_flags("10.0.0.0/16,fd00::/48,10.1.0.0/16", masks(0, 0, 0), None)
                .unwrap_err();
        assert!(e.contains("more than max allowed of 2"), "{e}");
        assert!(NodeIpamConfig::from_flags("10.0.0.0/16,nope", masks(0, 0, 0), None).is_err());
        // A mask that does not fit its own cluster CIDR is still rejected.
        assert!(NodeIpamConfig::from_flags("10.0.0.0/16,fd00::/48", masks(0, 8, 0), None).is_err());
    }

    #[test]
    fn from_flags_service_cidr_list() {
        let cfg = NodeIpamConfig::from_flags(
            "10.0.0.0/16,fd00::/48",
            masks(0, 0, 0),
            Some("10.0.0.0/23,fd00::/62"),
        )
        .unwrap();
        assert_eq!(cfg.service_cidrs, [ipn("10.0.0.0/23"), ipn("fd00::/62")]);
        // `--service-cluster-ip-range can not contain more than two entries`
        assert!(NodeIpamConfig::from_flags(
            "10.0.0.0/16",
            masks(0, 0, 0),
            Some("10.0.0.0/23,fd00::/62,10.9.0.0/24")
        )
        .is_err());
        // serviceCIDR and secondaryServiceCIDR must be from different families.
        let e = NodeIpamConfig::from_flags(
            "10.0.0.0/16",
            masks(0, 0, 0),
            Some("10.0.0.0/23,10.9.0.0/24"),
        )
        .unwrap_err();
        assert!(e.contains("not dualstack"), "{e}");
    }
}
