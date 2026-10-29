//! CSIStorageCapacity publisher (#3214, overlaps #3201), a port of
//! kubernetes-csi/external-provisioner v6.4.0 `pkg/capacity/capacity.go`
//! (`Controller`) and `pkg/capacity/topology/{topology,nodes}.go`
//! (`Segment`, `nodeTopology.sync`).
//!
//! The Go controller is event/queue driven and so is this port (#3548): the
//! StorageClass, CSIStorageCapacity and topology (CSINode/Node) watches over
//! `Storage` call `onSCAddOrUpdate`/`onSCDelete`/`onCAddOrUpdate`/`onCDelete`/
//! `onTopologyChanges`, which maintain `capacities` and the work queue that
//! the workers drain (`run_capacity_loop`, started from `main.rs`, #3219).
//! Deviation: the `Storage` seam has no label-selector watch, so the
//! CSIStorageCapacity informer sees every object of the namespace and relies
//! on the `isManaged` safeguard that upstream keeps for exactly this.

use super::csi_capacity_metrics::CapacityStats;
use super::csi_capacity_refresh::{ItemKey, QueueKey, State};
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::csi::{CSINode, CSIStorageCapacity};
use rusternetes_common::resources::volume::LabelSelector;
use rusternetes_common::resources::volume::{NodeSelectorTerm, StorageClass, VolumeBindingMode};
use rusternetes_common::resources::Node;
use rusternetes_common::types::{ObjectMeta, OwnerReference, TypeMeta};
use rusternetes_common::{Error, Result};
use rusternetes_csi::controller_client::CsiControllerClient;
use rusternetes_csi::proto::{GetCapacityRequest, GetCapacityResponse, Topology};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::cmp::Ordering;
use std::collections::HashMap;

/// `DriverNameLabel` (capacity.go:51).
pub const DRIVER_NAME_LABEL: &str = "csi.storage.k8s.io/drivername";
/// `ManagedByLabel` (capacity.go:52).
pub const MANAGED_BY_LABEL: &str = "csi.storage.k8s.io/managed-by";

/// `topology.SegmentEntry`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SegmentEntry {
    pub key: String,
    pub value: String,
}

/// `topology.Segment`: entries sorted by key, keys unique.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Segment(Vec<SegmentEntry>);

impl Segment {
    /// `sort.Sort(&segment)` after building from entries.
    pub fn from_entries(mut entries: Vec<SegmentEntry>) -> Self {
        entries.sort_by(|a, b| (&a.key, &a.value).cmp(&(&b.key, &b.value)));
        Segment(entries)
    }

    pub fn entries(&self) -> &[SegmentEntry] {
        &self.0
    }

    /// `Segment.Compare` (topology.go:57-71): fewer entries is smaller, then
    /// entry by entry (key, then value).
    ///
    /// ```text
    /// if len(s) < len(other) { return -1 }
    /// if len(s) > len(other) { return 1 }
    /// for i := range s { cmp := s[i].Compare(other[i]); if cmp != 0 { return cmp } }
    /// ```
    pub fn compare(&self, other: &Segment) -> Ordering {
        self.0.len().cmp(&other.0.len()).then_with(|| {
            self.0
                .iter()
                .zip(&other.0)
                .map(|(a, b)| (&a.key, &a.value).cmp(&(&b.key, &b.value)))
                .find(|o| *o != Ordering::Equal)
                .unwrap_or(Ordering::Equal)
        })
    }

    /// `Segment.GetLabelMap` (topology.go:98-109). A Rust `Segment` is never
    /// the Go nil segment, so this is always the (possibly empty) map.
    pub fn label_map(&self) -> HashMap<String, String> {
        self.0
            .iter()
            .map(|e| (e.key.clone(), e.value.clone()))
            .collect()
    }

    /// `Segment.GetLabelSelector` (topology.go:91-96): `MatchLabels` only.
    pub fn label_selector(&self) -> LabelSelector {
        LabelSelector {
            match_labels: Some(self.label_map()),
            match_expressions: None,
        }
    }
}

/// `termToSegment` (capacity.go:458-478).
pub fn term_to_segment(term: &NodeSelectorTerm) -> std::result::Result<Segment, String> {
    if term.match_fields.as_ref().is_some_and(|f| !f.is_empty()) {
        return Err(format!("MatchFields not empty: {:?}", term.match_fields));
    }
    let mut entries = Vec::new();
    for m in term.match_expressions.iter().flatten() {
        if m.operator != "In" {
            return Err(format!("unexpected operator: {}", m.operator));
        }
        let values = m.values.as_deref().unwrap_or_default();
        if values.len() != 1 {
            return Err(format!("need exactly one label value, got: {values:?}"));
        }
        entries.push(SegmentEntry {
            key: m.key.clone(),
            value: values[0].clone(),
        });
    }
    Ok(Segment::from_entries(entries))
}

/// `nodeTopology.sync` (nodes.go:268-341), minus the callback diffing: one
/// segment per distinct label tuple over the nodes the driver runs on.
/// `driverTopologyKeys` (nodes.go:197-208) is `None` only when the driver is
/// absent from the CSINode; present without keys it is the empty segment.
pub fn discover_segments(csi_nodes: &[CSINode], nodes: &[Node], driver_name: &str) -> Vec<Segment> {
    let mut segments: Vec<Segment> = Vec::new();
    'node: for csi_node in csi_nodes {
        let Some(driver) = csi_node.spec.drivers.iter().find(|d| d.name == driver_name) else {
            // Driver not running on node, ignore it.
            continue;
        };
        let Some(node) = nodes
            .iter()
            .find(|n| n.metadata.name == csi_node.metadata.name)
        else {
            // Obsolete CSINode object? Ignore it.
            continue;
        };
        let mut keys = driver.topology_keys.clone().unwrap_or_default();
        keys.sort();
        let mut entries = Vec::new();
        for key in keys {
            let Some(value) = node.metadata.labels.as_ref().and_then(|l| l.get(&key)) else {
                // Kubelet recorded the key in CSINode but the Node label is
                // not there yet: ignore the node until the node update.
                continue 'node;
            };
            entries.push(SegmentEntry {
                key,
                value: value.clone(),
            });
        }
        let new_segment = Segment::from_entries(entries);
        if !segments
            .iter()
            .any(|s| s.compare(&new_segment) == Ordering::Equal)
        {
            segments.push(new_segment);
        }
    }
    segments
}

/// `CSICapacityClient` (capacity.go:136-138).
#[allow(async_fn_in_trait)]
pub trait CapacityClient {
    async fn get_capacity(
        &self,
        req: GetCapacityRequest,
    ) -> std::result::Result<GetCapacityResponse, String>;
}

impl CapacityClient for CsiControllerClient {
    async fn get_capacity(
        &self,
        req: GetCapacityRequest,
    ) -> std::result::Result<GetCapacityResponse, String> {
        self.get_capacity_response(req)
            .await
            .map_err(|e| e.to_string())
    }
}

/// `getNameWithMaxLength` (cmd/csi-provisioner/util.go:30-60) with
/// `hash` = FNV-1a 32 as 8 hex chars (util.go:63-70).
pub fn name_with_max_length(base: &str, suffix: &str, max_length: usize) -> String {
    fn hash(s: &str) -> String {
        let mut h: u32 = 0x811c9dc5;
        for b in s.bytes() {
            h ^= u32::from(b);
            h = h.wrapping_mul(0x01000193);
        }
        format!("{h:08x}")
    }
    let name = if base.is_empty() {
        suffix.to_string()
    } else {
        format!("{base}-{suffix}")
    };
    if name.len() <= max_length {
        return name;
    }
    let base_length = max_length as isize - 10 - suffix.len() as isize;
    if base_length <= 0 {
        let mut short = hash(&name);
        let prefix_len = base.len().min(max_length.saturating_sub(9));
        let prefix = &base[..prefix_len];
        if !prefix.is_empty() {
            short = format!("{prefix}-{}", hash(&name));
        }
        short.truncate(max_length);
        return short;
    }
    format!("{}-{}-{suffix}", &base[..base_length as usize], hash(base))
}

/// `managedByID` (csi-provisioner.go:507-510); `DNS1035LabelMaxLength` is 63.
pub fn managed_by_id(node_name: Option<&str>) -> String {
    match node_name {
        Some(n) => name_with_max_length("external-provisioner", n, 63),
        None => "external-provisioner".to_string(),
    }
}

/// `topology.NewFixedNodeTopology` input (csi-provisioner.go:495-504): the
/// single segment of the node's `AccessibleTopology` (possibly empty).
pub fn fixed_segment(accessible_topology: Option<&HashMap<String, String>>) -> Segment {
    Segment::from_entries(
        accessible_topology
            .into_iter()
            .flatten()
            .map(|(k, v)| SegmentEntry {
                key: k.clone(),
                value: v.clone(),
            })
            .collect(),
    )
}

/// `owner.Lookup` (pkg/owner/owner.go:37-100): walk `levels` controlling
/// owners up from the starting object; `levels == 0` returns the object
/// itself. Only apiVersion, kind, name and uid are set; controller is true.
/// `limiter` is the token bucket of the client `owner.Lookup` builds.
pub async fn lookup_owner<S: Storage>(
    storage: &S,
    namespace: &str,
    name: &str,
    api_version: &str,
    kind: &str,
    levels: i32,
    limiter: &rusternetes_client::ratelimit::RateLimiter,
) -> Result<OwnerReference> {
    let (mut api_version, mut kind, mut name, mut levels) = (
        api_version.to_string(),
        kind.to_string(),
        name.to_string(),
        levels,
    );
    loop {
        let resource = match kind.as_str() {
            "Pod" => "pods",
            "ReplicaSet" => "replicasets",
            "ReplicationController" => "replicationcontrollers",
            "StatefulSet" => "statefulsets",
            "DaemonSet" => "daemonsets",
            "Deployment" => "deployments",
            "Job" => "jobs",
            "CronJob" => "cronjobs",
            other => {
                return Err(Error::Internal(format!(
                    "look up owner: unsupported kind {other}"
                )))
            }
        };
        // `owner.Lookup(config, ...)` builds its own client from the config whose
        // QPS/Burst were just set to the capacity values (csi-provisioner.go:
        // 456-457, :474; owner.go:37-44): every GET takes a token of that
        // client's own bucket.
        limiter.acquire().await;
        let obj: serde_json::Value = storage
            .get(&build_key(resource, Some(namespace), &name))
            .await
            .map_err(|e| Error::Internal(format!("get object: {e}")))?;
        let meta = obj.get("metadata");
        let str_of = |v: Option<&serde_json::Value>, k: &str| {
            v.and_then(|m| m.get(k))
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_string()
        };
        if levels == 0 {
            let mut o = OwnerReference::new(&api_version, &kind, &name, str_of(meta, "uid"));
            o.controller = Some(true);
            return Ok(o);
        }
        let controller = meta
            .and_then(|m| m.get("ownerReferences"))
            .and_then(|r| r.as_array())
            .into_iter()
            .flatten()
            .find(|o| o.get("controller").and_then(|c| c.as_bool()) == Some(true));
        let Some(owner) = controller else {
            return Err(Error::Internal(format!(
                "{api_version}/{kind} {name:?} in namespace {namespace:?} has no controlling owner, cannot unwind the ownership further"
            )));
        };
        let o_api = str_of(Some(owner), "apiVersion");
        let o_kind = str_of(Some(owner), "kind");
        let o_name = str_of(Some(owner), "name");
        if levels == 1 {
            let mut o = OwnerReference::new(&o_api, &o_kind, &o_name, str_of(Some(owner), "uid"));
            o.controller = Some(true);
            return Ok(o);
        }
        (api_version, kind, name, levels) = (o_api, o_kind, o_name, levels - 1);
    }
}

/// How the topology is obtained (`topology.Informer`): discovered from
/// CSINode/Node, or the fixed segment of node-deployment mode
/// (`NewFixedNodeTopology`).
#[derive(Clone)]
pub enum TopologySource {
    Discovered,
    Fixed(Segment),
}

/// `nodeTopology.driverTopologyKeys` (nodes.go:175-185): `None` if the driver
/// is not running on the node, otherwise at least an empty slice of keys.
pub fn driver_topology_keys(csi_node: &CSINode, driver_name: &str) -> Option<Vec<String>> {
    csi_node
        .spec
        .drivers
        .iter()
        .find(|d| d.name == driver_name)
        .map(|d| d.topology_keys.clone().unwrap_or_default())
}

/// `isManaged` (capacity.go:833-836).
fn is_managed_by(c: &CSIStorageCapacity, driver_name: &str, managed_by_id: &str) -> bool {
    c.metadata.labels.as_ref().is_some_and(|l| {
        l.get(DRIVER_NAME_LABEL).map(String::as_str) == Some(driver_name)
            && l.get(MANAGED_BY_LABEL).map(String::as_str) == Some(managed_by_id)
    })
}

/// `Controller.Run` (capacity.go:232-260): `prepare`, a worker (`--capacity-threads`
/// is #3547), `pollCapacities` every `poll_interval`
/// (`wait.UntilWithContext`) and the informer event handlers that feed the
/// queue: StorageClass, CSIStorageCapacity and the topology (CSINode / Node)
/// watches. The queue, including the `refreshTopology`/`refreshSC` triggers of
/// provision.go, is worked off with per-item exponential backoff.
pub async fn run_capacity_loop<S: Storage, C: CapacityClient>(
    ctl: &CapacityController<'_, S, C>,
    topology: &TopologySource,
    poll_interval: std::time::Duration,
    retry_interval_start: std::time::Duration,
    retry_interval_max: std::time::Duration,
    threads: u32,
) {
    ctl.register_metrics();
    // Subscribe first, list second: nothing that happens in between is lost.
    // The informers deliver such events again, which the handlers tolerate.
    let sc_prefix = build_prefix("storageclasses", None);
    let c_prefix = build_prefix("csistoragecapacities", Some(&ctl.owner_namespace));
    let node_prefix = build_prefix("nodes", None);
    let csinode_prefix = build_prefix("csinodes", None);
    let sc_watch = ctl.storage.watch(&sc_prefix).await.ok();
    let c_watch = ctl.storage.watch(&c_prefix).await.ok();
    let node_watch = ctl.storage.watch(&node_prefix).await.ok();
    let csinode_watch = ctl.storage.watch(&csinode_prefix).await.ok();
    loop {
        match ctl.prepare(topology).await {
            Ok(()) => break,
            Err(e) => {
                tracing::error!("CSIStorageCapacity publisher ({}): {e}", ctl.driver_name);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
    tokio::join!(
        ctl.run_workers(threads, retry_interval_start, retry_interval_max),
        ctl.run_poll(poll_interval),
        ctl.run_informer(
            sc_prefix,
            sc_watch,
            false,
            |l: Vec<StorageClass>| ctl.apply_sc_list(l),
            |o: &StorageClass| ctl.informer_sc_upsert(o),
            |o: &StorageClass| ctl.informer_sc_delete(o),
        ),
        ctl.run_informer(
            c_prefix,
            c_watch,
            true,
            |l: Vec<CSIStorageCapacity>| ctl.apply_c_list(l),
            |o: &CSIStorageCapacity| ctl.informer_c_upsert(o),
            |o: &CSIStorageCapacity| ctl.informer_c_delete(o),
        ),
        ctl.run_topology(topology, node_watch, csinode_watch),
    );
}

/// The `Controller` fields that decide what gets published.
pub struct CapacityController<'a, S: Storage, C: CapacityClient> {
    pub storage: &'a S,
    pub client: &'a C,
    pub driver_name: String,
    pub owner: Option<OwnerReference>,
    pub managed_by_id: String,
    pub owner_namespace: String,
    pub immediate_binding: bool,
    /// `capacities` + `queue`, shared with the provisioner's refresh hooks.
    pub refresher: std::sync::Arc<super::csi_capacity_refresh::CapacityRefresher>,
    /// The token bucket of the dedicated capacity clientset
    /// (`--kube-api-capacity-qps/-burst`, csi-provisioner.go:456-458): every
    /// non-watch request of that clientset (CSIStorageCapacity create, update,
    /// delete and the informer's list) takes a token, see [`capacity_limiter`].
    pub limiter: std::sync::Arc<rusternetes_client::ratelimit::RateLimiter>,
}

/// The rate limiter `kubernetes.NewForConfig` builds for `config.QPS` /
/// `config.Burst` (client-go `rest/config.go:370-381`): zero means
/// `DefaultQPS` / `DefaultBurst`, a QPS that is not positive means no limiter.
pub fn capacity_limiter(
    qps: f64,
    burst: i32,
) -> std::sync::Arc<rusternetes_client::ratelimit::RateLimiter> {
    use rusternetes_client::ratelimit::{RateLimiter, DEFAULT_BURST, DEFAULT_QPS};
    let qps = if qps == 0.0 { DEFAULT_QPS } else { qps };
    let burst = if burst == 0 {
        DEFAULT_BURST
    } else {
        f64::from(burst)
    };
    std::sync::Arc::new(if qps > 0.0 {
        RateLimiter::new(qps, burst)
    } else {
        RateLimiter::unlimited()
    })
}

/// The `csitopology` workqueue of `NewNodeTopology` (nodes.go:49, built in
/// csi-provisioner.go:488-494), whose only key is `""`. The Node and CSINode
/// handlers just `queue.Add("")` (nodes.go:71, :89, :102, :114, :134, :147);
/// `RunWorker` (:212-228) runs one `sync` per `queue.Get`. A workqueue holds a
/// key once however often it is added, and an `Add` while the key is being
/// processed queues it once more when `Done` runs, which is exactly the one
/// stored permit of a `Notify`: a burst of events is one `sync`, so n node
/// registrations do not cost n recomputations.
///
/// The queue is never used rate-limited: `sync` has no failure path that
/// re-adds the key (nodes.go:259-262 and :277-281 log and return), so the
/// `NewTypedItemExponentialFailureRateLimiter(retryIntervalStart,
/// retryIntervalMax)` upstream passes in is dead weight and there is no retry
/// to port; the next event (or the initial `Add`) is what triggers a sync.
#[derive(Default)]
pub struct TopologyQueue {
    notify: tokio::sync::Notify,
}

impl TopologyQueue {
    /// `queue.Add("")`.
    pub fn add(&self) {
        self.notify.notify_one();
    }

    /// `queue.Get()`: resolves once something was added since the last `get`.
    pub async fn get(&self) {
        self.notify.notified().await;
    }
}

/// `WorkItem` (capacity.go:105-108) with the StorageClass resolved.
struct WorkItem<'s> {
    segment: &'s Segment,
    sc: &'s StorageClass,
}

/// `Quantity.Value()` of the stored string; `None` when unparseable.
fn quantity_value(q: &str) -> Option<i128> {
    Quantity::parse(q).ok().map(|q| q.value())
}

/// `sizesAreEqual` (capacity.go:838-849).
fn sizes_are_equal(expected: Option<&str>, actual: Option<&str>) -> bool {
    match (expected, actual) {
        (None, None) => true,
        (Some(a), Some(b)) => quantity_value(a).is_some() && quantity_value(a) == quantity_value(b),
        _ => false,
    }
}

/// `resource.NewQuantity(v, resource.BinarySI)` rendered for the wire.
fn binary_si(v: i64) -> String {
    Quantity::from_value(v, Format::BinarySI).canonical_string()
}

/// Event handlers of capacity.go (`onTopologyChanges`, `onSCAddOrUpdate`,
/// `onSCDelete`, `onCAddOrUpdate`, `onCDelete`, `pollCapacities`; #3548).
/// Like upstream they run under `capacitiesLock` ([`CapacityRefresher::lock`])
/// and read the StorageClass list / topology segments from the caches that the
/// `informer_*` / `apply_*` functions below keep up to date, which is what the
/// listers of the Go informers are.
impl<S: Storage, C: CapacityClient> CapacityController<'_, S, C> {
    /// `isManaged` (capacity.go:833-836).
    fn is_managed(&self, c: &CSIStorageCapacity) -> bool {
        is_managed_by(c, &self.driver_name, &self.managed_by_id)
    }

    /// `isOwnedByUs` (capacity.go:821-828): a *controller* reference with our UID.
    fn is_owned_by_us(&self, c: &CSIStorageCapacity) -> bool {
        let Some(owner) = &self.owner else {
            return false;
        };
        c.metadata
            .owner_references
            .iter()
            .flatten()
            .any(|o| o.controller == Some(true) && o.uid == owner.uid)
    }

    /// `sc.Provisioner != c.driverName` and the immediate-binding check shared
    /// by `onTopologyChanges` (capacity.go:353-358) and `onSCAddOrUpdate`
    /// (:371-379).
    fn sc_is_wanted(&self, sc: &StorageClass) -> bool {
        sc.provisioner == self.driver_name
            && (self.immediate_binding
                || sc.volume_binding_mode != Some(VolumeBindingMode::Immediate))
    }

    /// `addWorkItem` (capacity.go:469-484). Must hold the lock.
    fn add_work_item(&self, st: &mut State, segment: &Segment, sc: &StorageClass) {
        let item = ItemKey {
            segment: segment.clone(),
            storage_class_name: sc.metadata.name.clone(),
        };
        // Ensure that we have an entry for it...
        st.capacities.entry(item.clone()).or_insert(None);
        // ... and then tell our workers to update or create that capacity object.
        tracing::trace!("Capacity Controller: enqueuing {item:?}");
        st.add_item(item);
    }

    /// `removeWorkItem` (capacity.go:488-512). Must hold the lock.
    fn remove_work_item(&self, st: &mut State, segment: &Segment, sc: &StorageClass) {
        let item = ItemKey {
            segment: segment.clone(),
            storage_class_name: sc.metadata.name.clone(),
        };
        let Some(capacity) = st.capacities.remove(&item) else {
            // Already gone or in the queue to be removed.
            tracing::trace!("Capacity Controller: {item:?} already removed");
            return;
        };
        // Deleting the item prevents further updates to it, in case that it is
        // already in the queue.
        let Some(capacity) = capacity else {
            tracing::trace!("Capacity Controller: {item:?} removed, no object");
            return;
        };
        // Any capacity object in the queue will be deleted.
        tracing::trace!(
            "Capacity Controller: enqueuing CSIStorageCapacity {} for removal",
            capacity.metadata.name
        );
        st.add_capacity(
            capacity
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| self.owner_namespace.clone()),
            capacity.metadata.name.clone(),
        );
    }

    /// `onTopologyChanges` (capacity.go:339-366), called by the topology informer.
    pub fn on_topology_changes(&self, added: &[Segment], removed: &[Segment]) {
        tracing::trace!(
            "Capacity Controller: topology changed: added {added:?}, removed {removed:?}"
        );
        let mut st = self.refresher.lock();
        let storageclasses: Vec<StorageClass> = st.classes.values().cloned().collect();
        for sc in &storageclasses {
            if !self.sc_is_wanted(sc) {
                continue;
            }
            for segment in added {
                self.add_work_item(&mut st, segment, sc);
            }
            for segment in removed {
                self.remove_work_item(&mut st, segment, sc);
            }
        }
    }

    /// `onSCAddOrUpdate` (capacity.go:370-387): add or update events of the
    /// StorageClass informer.
    pub fn on_sc_add_or_update(&self, sc: &StorageClass) {
        if sc.provisioner != self.driver_name {
            return;
        }
        tracing::trace!(
            "Capacity Controller: storage class {} was updated or added",
            sc.metadata.name
        );
        if !self.sc_is_wanted(sc) {
            tracing::trace!(
                "Capacity Controller: ignoring storage class {} because it uses immediate binding",
                sc.metadata.name
            );
            return;
        }
        let mut st = self.refresher.lock();
        let segments = st.segments.clone();
        for segment in &segments {
            self.add_work_item(&mut st, segment, sc);
        }
    }

    /// `onSCDelete` (capacity.go:390-403): delete events of the StorageClass
    /// informer.
    pub fn on_sc_delete(&self, sc: &StorageClass) {
        if sc.provisioner != self.driver_name {
            return;
        }
        tracing::trace!(
            "Capacity Controller: storage class {} was removed",
            sc.metadata.name
        );
        let mut st = self.refresher.lock();
        let segments = st.segments.clone();
        for segment in &segments {
            self.remove_work_item(&mut st, segment, sc);
        }
    }

    /// `pollCapacities` (capacity.go:515-523): must be called periodically to
    /// detect when the underlying storage capacity has changed.
    pub fn poll_capacities(&self) {
        let mut st = self.refresher.lock();
        let items: Vec<ItemKey> = st.capacities.keys().cloned().collect();
        for item in items {
            tracing::trace!("Capacity Controller: enqueuing {item:?} for periodic update");
            st.add_item(item);
        }
    }

    /// `onCAddOrUpdate` (capacity.go:687-726): takes a read-only
    /// CSIStorageCapacity object and either remembers it for future updates or
    /// ensures that it gets deleted if no longer needed. Foreign objects are
    /// ignored.
    pub fn on_c_add_or_update(&self, capacity: &CSIStorageCapacity) {
        let mut st = self.refresher.lock();
        if !self.is_managed(capacity) {
            // Not ours (anymore?). For the unlikely case that someone removed
            // our owner reference, we also must remove our reference to the
            // object.
            let items: Vec<ItemKey> = st
                .capacities
                .iter()
                .filter(|(_, c2)| {
                    c2.as_ref()
                        .is_some_and(|c2| c2.metadata.uid == capacity.metadata.uid)
                })
                .map(|(item, _)| item.clone())
                .collect();
            for item in items {
                st.capacities.insert(item.clone(), None);
                tracing::trace!(
                    "Capacity Controller: CSIStorageCapacity {} owner was modified by someone, enqueueing {item:?} for re-creation",
                    capacity.metadata.name
                );
                st.add_item(item);
            }
            return;
        }

        let mut matched = None;
        for (item, capacity2) in st.capacities.iter() {
            match capacity2 {
                // We already have matched the object. Either way, remember the
                // new object revision to avoid the "conflict" error when we
                // try to update the old object.
                Some(c2) if c2.metadata.uid == capacity.metadata.uid => {
                    matched = Some(item.clone());
                    break;
                }
                // This is the capacity object for this particular combination
                // of parameters. Reuse it.
                None if item.equals(capacity) => {
                    matched = Some(item.clone());
                    break;
                }
                _ => {}
            }
        }
        match matched {
            Some(item) => {
                st.capacities.insert(item, Some(capacity.clone()));
            }
            None => {
                // The CSIStorageCapacity object is obsolete, delete it.
                tracing::trace!(
                    "Capacity Controller: CSIStorageCapacity {} with resource version {:?} is obsolete, enqueue for removal",
                    capacity.metadata.name,
                    capacity.metadata.resource_version
                );
                st.add_capacity(
                    capacity
                        .metadata
                        .namespace
                        .clone()
                        .unwrap_or_else(|| self.owner_namespace.clone()),
                    capacity.metadata.name.clone(),
                );
            }
        }
    }

    /// `onCDelete` (capacity.go:728-741).
    pub fn on_c_delete(&self, capacity: &CSIStorageCapacity) {
        let mut st = self.refresher.lock();
        let item = st
            .capacities
            .iter()
            .find(|(_, c2)| {
                c2.as_ref()
                    .is_some_and(|c2| c2.metadata.uid == capacity.metadata.uid)
            })
            .map(|(item, _)| item.clone());
        if let Some(item) = item {
            // The object is still needed. Someone else must have removed it.
            // Re-create it...
            tracing::trace!(
                "Capacity Controller: CSIStorageCapacity {} was removed by someone, enqueue {item:?} for re-creation",
                capacity.metadata.name
            );
            st.capacities.insert(item.clone(), None);
            st.add_item(item);
        }
    }

    /// `CollectWithStability` (capacity.go:751-767), evaluated now.
    pub fn stats(&self) -> CapacityStats {
        self.refresher.stats(&|c| self.is_managed(c))
    }

    /// `legacyregistry.CustomMustRegister(capacityController)`
    /// (csi-provisioner.go:545): the gauges are computed at scrape time.
    pub fn register_metrics(&self) {
        let weak = std::sync::Arc::downgrade(&self.refresher);
        let (driver, managed_by) = (self.driver_name.clone(), self.managed_by_id.clone());
        super::csi_capacity_metrics::register(&self.driver_name, move || {
            let refresher = weak.upgrade()?;
            Some(refresher.stats(&|c| is_managed_by(c, &driver, &managed_by)))
        });
    }

    // ---- informer caches ----

    /// StorageClass informer add/update: update the cache, then call the
    /// handler registered in `NewCentralCapacityController` (:188-204).
    pub fn informer_sc_upsert(&self, sc: &StorageClass) {
        self.refresher
            .lock()
            .classes
            .insert(sc.metadata.name.clone(), sc.clone());
        self.on_sc_add_or_update(sc);
    }

    /// StorageClass informer delete (:205-216).
    pub fn informer_sc_delete(&self, sc: &StorageClass) {
        self.refresher.lock().classes.remove(&sc.metadata.name);
        self.on_sc_delete(sc);
    }

    /// CSIStorageCapacity informer add/update (`prepare`, :293-309).
    pub fn informer_c_upsert(&self, c: &CSIStorageCapacity) {
        self.refresher
            .lock()
            .cache
            .insert(c.metadata.name.clone(), c.clone());
        self.on_c_add_or_update(c);
    }

    /// CSIStorageCapacity informer delete (`prepare`, :310-321).
    pub fn informer_c_delete(&self, c: &CSIStorageCapacity) {
        self.refresher.lock().cache.remove(&c.metadata.name);
        self.on_c_delete(c);
    }

    /// The reflector's `Replace` of a StorageClass list: every listed object is
    /// an add/update, cached objects that are gone were deleted.
    pub fn apply_sc_list(&self, list: Vec<StorageClass>) {
        let names: std::collections::HashSet<&str> =
            list.iter().map(|o| o.metadata.name.as_str()).collect();
        let gone: Vec<StorageClass> = self
            .refresher
            .lock()
            .classes
            .values()
            .filter(|o| !names.contains(o.metadata.name.as_str()))
            .cloned()
            .collect();
        for sc in &list {
            self.informer_sc_upsert(sc);
        }
        for sc in &gone {
            self.informer_sc_delete(sc);
        }
    }

    /// The reflector's `Replace` of a CSIStorageCapacity list.
    pub fn apply_c_list(&self, list: Vec<CSIStorageCapacity>) {
        let names: std::collections::HashSet<&str> =
            list.iter().map(|o| o.metadata.name.as_str()).collect();
        let gone: Vec<CSIStorageCapacity> = self
            .refresher
            .lock()
            .cache
            .values()
            .filter(|o| !names.contains(o.metadata.name.as_str()))
            .cloned()
            .collect();
        for c in &list {
            self.informer_c_upsert(c);
        }
        for c in &gone {
            self.informer_c_delete(c);
        }
    }

    /// `nodeTopology.sync` (nodes.go:245-341) bookkeeping: remember the new
    /// segment list and call the callback (`onTopologyChanges`) with what was
    /// added and removed.
    pub fn apply_topology(&self, segments: Vec<Segment>) {
        let (added, removed) = {
            let mut st = self.refresher.lock();
            let added: Vec<Segment> = segments
                .iter()
                .filter(|s| !st.segments.contains(s))
                .cloned()
                .collect();
            let removed: Vec<Segment> = st
                .segments
                .iter()
                .filter(|s| !segments.contains(s))
                .cloned()
                .collect();
            st.segments = segments;
            (added, removed)
        };
        if !added.is_empty() || !removed.is_empty() {
            tracing::trace!("topology changed: added {added:?}, removed {removed:?}");
            self.on_topology_changes(&added, &removed);
        }
    }

    /// The segments the topology informer would report right now.
    pub async fn current_segments(&self, topology: &TopologySource) -> Result<Vec<Segment>> {
        Ok(match topology {
            TopologySource::Fixed(s) => vec![s.clone()],
            TopologySource::Discovered => {
                let csi_nodes: Vec<CSINode> =
                    self.storage.list(&build_prefix("csinodes", None)).await?;
                let nodes: Vec<Node> = self.storage.list(&build_prefix("nodes", None)).await?;
                discover_segments(&csi_nodes, &nodes, &self.driver_name)
            }
        })
    }

    /// `prepare` (capacity.go:262-337): learn the StorageClasses and topology
    /// segments, so that the work items are known, then feed the existing
    /// CSIStorageCapacity objects to `onCAddOrUpdate`.
    pub async fn prepare(&self, topology: &TopologySource) -> Result<()> {
        let classes: Vec<StorageClass> = self
            .storage
            .list(&build_prefix("storageclasses", None))
            .await?;
        self.apply_sc_list(classes);
        let segments = self.current_segments(topology).await?;
        self.apply_topology(segments);
        // :273 "onTopologyChanges lists the classes."
        let segments = self.refresher.lock().segments.clone();
        self.on_topology_changes(&segments, &[]);
        // The CSIStorageCapacity informer lists through the capacity clientset.
        self.limiter.acquire().await;
        let capacities: Vec<CSIStorageCapacity> = self
            .storage
            .list(&build_prefix(
                "csistoragecapacities",
                Some(&self.owner_namespace),
            ))
            .await?;
        self.apply_c_list(capacities);
        Ok(())
    }

    // ---- workers ----

    /// `for range threadiness { go wait.UntilWithContext(runWorker, time.Second) }`
    /// (capacity.go:238-253): `--capacity-threads` workers on the one queue.
    /// The queue hands an item out to one worker at a time (`Done` re-queues
    /// what was added meanwhile), which `syncCapacity` relies on (:568-571).
    /// As upstream, zero threads means no worker.
    pub async fn run_workers(
        &self,
        threads: u32,
        start: std::time::Duration,
        max: std::time::Duration,
    ) {
        futures::future::join_all((0..threads).map(|_| self.run_worker(start, max))).await;
    }

    /// `wait.UntilWithContext(pollCapacities, pollPeriod)` (capacity.go:255).
    async fn run_poll(&self, period: std::time::Duration) {
        let mut ticker = tokio::time::interval(period);
        loop {
            ticker.tick().await;
            self.poll_capacities();
        }
    }

    /// `runWorker` (capacity.go:525-528): one of `--capacity-threads` workers.
    async fn run_worker(&self, start: std::time::Duration, max: std::time::Duration) {
        loop {
            if self.process_next(start, max).await {
                continue;
            }
            let wake = self.refresher.next_delayed();
            tokio::select! {
                _ = async {
                    match wake {
                        Some(t) => tokio::time::sleep_until(tokio::time::Instant::from_std(t)).await,
                        None => std::future::pending().await,
                    }
                } => {}
                _ = self.refresher.notified() => {}
            }
        }
    }

    /// `processNextWorkItem` (capacity.go:531-561) without blocking: `false`
    /// when nothing is queued. Success is `Forget`, failure is `AddRateLimited`
    /// (`NewTypedItemExponentialFailureRateLimiter(retryIntervalStart,
    /// retryIntervalMax)`, csi-provisioner.go:528).
    pub async fn process_next(&self, start: std::time::Duration, max: std::time::Duration) -> bool {
        let Some(key) = self.refresher.pop() else {
            return false;
        };
        let r = match &key {
            QueueKey::Item { item, .. } => self.sync_item(item).await,
            QueueKey::Capacity {
                namespace, name, ..
            } => self.delete_capacity(namespace, name).await,
        };
        // `defer c.queue.Done(obj)` runs before the error handling.
        self.refresher.done(&key);
        match r {
            Ok(()) => self.refresher.forget(&key),
            Err(e) => {
                tracing::error!("CSIStorageCapacity publisher ({}): {e}", self.driver_name);
                let d = self.refresher.add_rate_limited(key, start, max);
                tracing::warn!("Retrying after {d:?}");
            }
        }
        true
    }

    /// Process what is queued right now (and what is rescheduled by it).
    pub async fn sync_queued(&self, start: std::time::Duration, max: std::time::Duration) {
        while self.process_next(start, max).await {}
    }

    /// `syncCapacity(ctx, item)` (capacity.go:564-671): reads the object from
    /// `capacities`, which only the informer event handlers update.
    async fn sync_item(&self, item: &ItemKey) -> Result<()> {
        let (current, sc) = {
            let st = self.refresher.lock();
            (
                st.capacities.get(item).cloned(),
                st.classes.get(&item.storage_class_name).cloned(),
            )
        };
        // :577-582: "The item was removed in the meantime. This can happen when
        // the storage class or the topology segment are gone."
        let Some(current) = current else {
            tracing::trace!("Capacity Controller: {item:?} became obsolete");
            return Ok(());
        };
        // :584-592: "Another indication that the value is no longer needed."
        let Some(sc) = sc else {
            return Ok(());
        };
        self.sync_capacity(
            &WorkItem {
                segment: &item.segment,
                sc: &sc,
            },
            current,
        )
        .await
    }

    /// `syncCapacity` (:594-670).
    async fn sync_capacity(
        &self,
        item: &WorkItem<'_>,
        current: Option<CSIStorageCapacity>,
    ) -> Result<()> {
        // :594-603 — the assumption is that the capacity is independent of the
        // capabilities, hence no VolumeCapabilities.
        let req = GetCapacityRequest {
            parameters: item.sc.parameters.clone().unwrap_or_default(),
            volume_capabilities: Vec::new(),
            accessible_topology: Some(Topology {
                segments: item.segment.label_map(),
            }),
        };
        let resp = self.client.get_capacity(req).await.map_err(|e| {
            Error::Internal(format!(
                "CSI GetCapacity for {}/{:?}: {e}",
                item.sc.metadata.name,
                item.segment.entries()
            ))
        })?;
        let quantity = binary_si(resp.available_capacity);
        let maximum_volume_size = resp.maximum_volume_size.map(binary_si);

        match current {
            None => {
                let obj = self.new_capacity(item, quantity, maximum_volume_size);
                let name = obj.metadata.name.clone();
                // :642-645: the created object is intentionally not stored in
                // `capacities`; it arrives through the informer.
                self.limiter.acquire().await;
                self.storage
                    .create(
                        &build_key("csistoragecapacities", Some(&self.owner_namespace), &name),
                        &obj,
                    )
                    .await?;
            }
            Some(c)
                if sizes_are_equal(c.capacity.as_deref(), Some(&quantity))
                    && sizes_are_equal(
                        c.maximum_volume_size.as_deref(),
                        maximum_volume_size.as_deref(),
                    )
                    && (self.owner.is_none() || self.is_owned_by_us(&c)) => {}
            Some(mut c) => {
                // `capacity.DeepCopy()` (:653): `c` is already a copy of the
                // cached object.
                c.capacity = Some(quantity);
                c.maximum_volume_size = maximum_volume_size;
                if let Some(owner) = &self.owner {
                    if !self.is_owned_by_us(&c) {
                        c.metadata
                            .owner_references
                            .get_or_insert_with(Vec::new)
                            .push(owner.clone());
                    }
                }
                self.limiter.acquire().await;
                self.storage
                    .update(
                        &build_key(
                            "csistoragecapacities",
                            Some(&self.owner_namespace),
                            &c.metadata.name,
                        ),
                        &c,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    /// The create path of `syncCapacity` (:617-640): a brand-new object. Kept
    /// apart from the update path, which must write a clone of the stored
    /// object (upstream's `capacity.DeepCopy()`, :653) so the PUT carries the
    /// stored uid / resourceVersion.
    fn new_capacity(
        &self,
        item: &WorkItem<'_>,
        quantity: String,
        maximum_volume_size: Option<String>,
    ) -> CSIStorageCapacity {
        let mut labels = HashMap::new();
        labels.insert(DRIVER_NAME_LABEL.to_string(), self.driver_name.clone());
        labels.insert(MANAGED_BY_LABEL.to_string(), self.managed_by_id.clone());
        // `GenerateName: "csisc-"`; the API server fills the suffix on
        // the HTTP path, direct storage must do it itself.
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let name = format!("csisc-{}", &suffix[..5]);
        let mut metadata = ObjectMeta::new(name.clone())
            .with_namespace(&self.owner_namespace)
            .with_labels(labels);
        metadata.generate_name = Some("csisc-".to_string());
        if let Some(owner) = &self.owner {
            metadata.owner_references = Some(vec![owner.clone()]);
        }
        CSIStorageCapacity {
            type_meta: TypeMeta {
                kind: "CSIStorageCapacity".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata,
            storage_class_name: item.sc.metadata.name.clone(),
            capacity: Some(quantity),
            maximum_volume_size,
            node_topology: Some(item.segment.label_selector()),
        }
    }

    /// `deleteCapacity` (:673-681): NotFound is success.
    async fn delete_capacity(&self, namespace: &str, name: &str) -> Result<()> {
        tracing::trace!("Capacity Controller: removing CSIStorageCapacity {name}");
        self.limiter.acquire().await;
        match self
            .storage
            .delete(&build_key("csistoragecapacities", Some(namespace), name))
            .await
        {
            Err(Error::NotFound(_)) => Ok(()),
            r => r,
        }
    }

    // ---- informers over `Storage` ----

    /// A list+watch informer for one resource: `first` is a stream subscribed
    /// before the initial list (`prepare` did that list); after a broken watch
    /// it is re-subscribed and the resource re-listed (`Replace`).
    async fn run_informer<T>(
        &self,
        prefix: String,
        mut first: Option<rusternetes_storage::WatchStream>,
        throttled: bool,
        relist: impl Fn(Vec<T>),
        upsert: impl Fn(&T),
        delete: impl Fn(&T),
    ) where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        use futures::StreamExt;
        use rusternetes_storage::WatchEvent;
        loop {
            let mut stream = match first.take() {
                Some(s) => s,
                None => {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    let s = match self.storage.watch(&prefix).await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!("watch {prefix}: {e}");
                            continue;
                        }
                    };
                    // Only the capacity informer lists through the throttled
                    // capacity clientset; watches are never throttled
                    // (client-go rest/request.go:763-764).
                    if throttled {
                        self.limiter.acquire().await;
                    }
                    match self.storage.list::<T>(&prefix).await {
                        Ok(l) => relist(l),
                        Err(e) => {
                            tracing::error!("list {prefix}: {e}");
                            continue;
                        }
                    }
                    s
                }
            };
            while let Some(ev) = stream.next().await {
                match ev {
                    Ok(WatchEvent::Added(_, v)) | Ok(WatchEvent::Modified(_, v)) => {
                        match serde_json::from_str::<T>(&v) {
                            Ok(o) => upsert(&o),
                            Err(e) => tracing::error!("decode {prefix} event: {e}"),
                        }
                    }
                    Ok(WatchEvent::Deleted(_, v)) => match serde_json::from_str::<T>(&v) {
                        Ok(o) => delete(&o),
                        Err(e) => tracing::error!("decode {prefix} event: {e}"),
                    },
                    Err(e) => {
                        tracing::error!("watch {prefix}: {e}");
                        break;
                    }
                }
            }
        }
    }

    /// `NewNodeTopology`'s Node and CSINode handlers (nodes.go:63-150) and its
    /// `RunWorker`: whenever something relevant changed, queue a recompute of
    /// the segments (coalesced by [`TopologyQueue`]) that calls
    /// `onTopologyChanges`. A fixed topology never changes.
    async fn run_topology(
        &self,
        topology: &TopologySource,
        nodes: Option<rusternetes_storage::WatchStream>,
        csinodes: Option<rusternetes_storage::WatchStream>,
    ) {
        if matches!(topology, TopologySource::Fixed(_)) {
            return std::future::pending().await;
        }
        let queue = TopologyQueue::default();
        // `nt.queue.Add("") // Initial sync` (nodes.go:225).
        queue.add();
        tokio::join!(
            // `RunWorker` (nodes.go:226-243): the one worker.
            async {
                loop {
                    queue.get().await;
                    self.sync_topology(topology).await;
                }
            },
            self.watch_topology(nodes, csinodes, &queue),
        );
    }

    /// The Node and CSINode event handlers of `NewNodeTopology`
    /// (nodes.go:63-150): they only `queue.Add("")`.
    async fn watch_topology(
        &self,
        mut nodes: Option<rusternetes_storage::WatchStream>,
        mut csinodes: Option<rusternetes_storage::WatchStream>,
        queue: &TopologyQueue,
    ) {
        use futures::StreamExt;
        use rusternetes_storage::WatchEvent;
        let node_prefix = build_prefix("nodes", None);
        let csinode_prefix = build_prefix("csinodes", None);
        // What the handlers' `oldObj` provides in Go: node labels and the
        // driver's topology keys of the previous revision.
        let mut node_labels: HashMap<String, Option<HashMap<String, String>>> = HashMap::new();
        let mut csinode_keys: HashMap<String, Option<Vec<String>>> = HashMap::new();
        loop {
            let (mut ns, mut cs) = match (nodes.take(), csinodes.take()) {
                (Some(n), Some(c)) => (n, c),
                _ => {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    let (Ok(n), Ok(c)) = (
                        self.storage.watch(&node_prefix).await,
                        self.storage.watch(&csinode_prefix).await,
                    ) else {
                        tracing::error!("topology watch failed");
                        continue;
                    };
                    queue.add();
                    (n, c)
                }
            };
            if let Ok(l) = self.storage.list::<Node>(&node_prefix).await {
                node_labels = l
                    .into_iter()
                    .map(|n| (n.metadata.name.clone(), n.metadata.labels.clone()))
                    .collect();
            }
            if let Ok(l) = self.storage.list::<CSINode>(&csinode_prefix).await {
                csinode_keys = l
                    .into_iter()
                    .map(|n| {
                        let k = driver_topology_keys(&n, &self.driver_name);
                        (n.metadata.name, k)
                    })
                    .collect();
            }
            loop {
                let changed = tokio::select! {
                    ev = ns.next() => match ev {
                        None | Some(Err(_)) => break,
                        Some(Ok(WatchEvent::Added(_, v))) => {
                            serde_json::from_str::<Node>(&v).is_ok_and(|n| {
                                node_labels.insert(n.metadata.name, n.metadata.labels);
                                true
                            })
                        }
                        Some(Ok(WatchEvent::Modified(_, v))) => {
                            // Shortcut: labels haven't changed, no need to sync.
                            serde_json::from_str::<Node>(&v).is_ok_and(|n| {
                                let old = node_labels
                                    .insert(n.metadata.name, n.metadata.labels.clone());
                                old.as_ref() != Some(&n.metadata.labels)
                            })
                        }
                        Some(Ok(WatchEvent::Deleted(_, v))) => {
                            serde_json::from_str::<Node>(&v).is_ok_and(|n| {
                                node_labels.remove(&n.metadata.name);
                                true
                            })
                        }
                    },
                    ev = cs.next() => match ev {
                        None | Some(Err(_)) => break,
                        Some(Ok(WatchEvent::Added(_, v))) => {
                            serde_json::from_str::<CSINode>(&v).is_ok_and(|n| {
                                let k = driver_topology_keys(&n, &self.driver_name);
                                csinode_keys.insert(n.metadata.name, k);
                                true
                            })
                        }
                        Some(Ok(WatchEvent::Modified(_, v))) => {
                            // Shortcut: keys haven't changed, no need to sync.
                            serde_json::from_str::<CSINode>(&v).is_ok_and(|n| {
                                let k = driver_topology_keys(&n, &self.driver_name);
                                let old = csinode_keys.insert(n.metadata.name, k.clone());
                                old.as_ref() != Some(&k)
                            })
                        }
                        Some(Ok(WatchEvent::Deleted(_, v))) => {
                            serde_json::from_str::<CSINode>(&v).is_ok_and(|n| {
                                csinode_keys.remove(&n.metadata.name);
                                true
                            })
                        }
                    },
                };
                if changed {
                    queue.add();
                }
            }
        }
    }

    /// `nodeTopology.sync`.
    async fn sync_topology(&self, topology: &TopologySource) {
        match self.current_segments(topology).await {
            Ok(segments) => self.apply_topology(segments),
            Err(e) => tracing::error!("CSIStorageCapacity publisher ({}): {e}", self.driver_name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::csi_capacity_refresh::{CapacityRefresher, ItemKey, QueueKey};
    use super::*;
    use rusternetes_client::ratelimit::RateLimiter;
    use rusternetes_common::resources::csi::{CSINodeDriver, CSINodeSpec};
    use rusternetes_common::resources::volume::VolumeNodeAffinity;
    use rusternetes_common::resources::volume::{
        NodeSelectorRequirement, StorageClass, VolumeBindingMode,
    };
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_storage::build_key;
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::time::Duration;

    const DRIVER: &str = "my-csi-driver";
    const NS: &str = "kube-system";
    const MANAGED: &str = "instance-1";

    fn seg(pairs: &[(&str, &str)]) -> Segment {
        Segment::from_entries(
            pairs
                .iter()
                .map(|(k, v)| SegmentEntry {
                    key: k.to_string(),
                    value: v.to_string(),
                })
                .collect(),
        )
    }

    fn req(key: &str, op: &str, vals: &[&str]) -> NodeSelectorRequirement {
        NodeSelectorRequirement {
            key: key.into(),
            operator: op.into(),
            values: Some(vals.iter().map(|v| v.to_string()).collect()),
        }
    }

    // ---- TestRefresh (capacity_test.go:1799-1957) ----

    fn vna(pairs: &[(&str, &str)]) -> VolumeNodeAffinity {
        VolumeNodeAffinity {
            required: Some(rusternetes_common::resources::volume::NodeSelector {
                node_selector_terms: vec![NodeSelectorTerm {
                    match_expressions: Some(
                        pairs.iter().map(|(k, v)| req(k, "In", &[v])).collect(),
                    ),
                    match_fields: None,
                }],
            }),
        }
    }

    /// `itemsAsSortedStringSlice`.
    fn queued_strings(c: &CapacityRefresher) -> Vec<String> {
        let mut v: Vec<String> = c
            .queued()
            .iter()
            .map(|i| {
                let e: Vec<String> = i
                    .segment
                    .entries()
                    .iter()
                    .map(|e| format!("{}: {}", e.key, e.value))
                    .collect();
                format!("{}, [{}]", i.storage_class_name, e.join(" "))
            })
            .collect();
        v.sort();
        v
    }

    const DEEP: [(&str, &str); 3] = [("layer0", "foo"), ("layer1", "X"), ("layer2", "A")];
    const DEEP_OTHER: [(&str, &str); 3] = [("layer0", "foo"), ("layer1", "X"), ("layer2", "B")];

    async fn refresh_fixture(
        segments: &[&[(&str, &str)]],
    ) -> (MemoryStorage, FakeClient, std::sync::Arc<CapacityRefresher>) {
        let st = MemoryStorage::new();
        put_sc(&st, sc("direct-sc", DRIVER, None)).await;
        put_sc(&st, sc("triple-sc", DRIVER, None)).await;
        let cl = FakeClient::default();
        let c = ctl(&st, &cl, false);
        let segs: Vec<Segment> = segments.iter().map(|s| seg(s)).collect();
        settle(&c, &st, &segs).await;
        // "Clear queue so that below we only get to see items scheduled for refresh."
        c.refresher.clear_queue();
        let r = c.refresher.clone();
        (st, cl, r)
    }

    #[tokio::test]
    async fn refresh_storageclass() {
        let (_s, _c, r) = refresh_fixture(&[&[("layer0", "foo")], &[("layer0", "bar")]]).await;
        r.refresh_sc("direct-sc");
        assert_eq!(
            queued_strings(&r),
            vec!["direct-sc, [layer0: bar]", "direct-sc, [layer0: foo]"]
        );
    }

    #[tokio::test]
    async fn refresh_topology() {
        let (_s, _c, r) = refresh_fixture(&[&[("layer0", "foo")], &[("layer0", "bar")]]).await;
        r.refresh_topology(&vna(&[("layer0", "bar")]));
        assert_eq!(
            queued_strings(&r),
            vec!["direct-sc, [layer0: bar]", "triple-sc, [layer0: bar]"]
        );
    }

    #[tokio::test]
    async fn refresh_deep_topology() {
        let (_s, _c, r) = refresh_fixture(&[&DEEP, &DEEP_OTHER]).await;
        r.refresh_topology(&vna(&DEEP));
        assert_eq!(
            queued_strings(&r),
            vec![
                "direct-sc, [layer0: foo layer1: X layer2: A]",
                "triple-sc, [layer0: foo layer1: X layer2: A]"
            ]
        );
    }

    #[tokio::test]
    async fn refresh_unknown_or_truncated_topology_and_class_queue_nothing() {
        let (_s, _c, r) = refresh_fixture(&[&DEEP, &DEEP_OTHER]).await;
        r.refresh_topology(&vna(&[
            ("layer0", "foo"),
            ("layer1", "X"),
            ("layer2", "BBBBBBBBBBB"),
        ]));
        r.refresh_sc("no-such-sc");
        r.refresh_topology(&vna(&[("layer0", "foo")]));
        assert!(queued_strings(&r).is_empty());
    }

    #[tokio::test]
    async fn refresh_topology_skips_unusable_terms_only() {
        let (_s, _c, r) = refresh_fixture(&[&DEEP]).await;
        r.refresh_topology(&VolumeNodeAffinity { required: None });
        assert!(queued_strings(&r).is_empty());
        // An unusable term (`MatchFields`) is skipped, a later usable one is not.
        let mut a = vna(&DEEP);
        let bad = NodeSelectorTerm {
            match_expressions: None,
            match_fields: Some(vec![req("metadata.name", "In", &["n"])]),
        };
        a.required
            .as_mut()
            .unwrap()
            .node_selector_terms
            .insert(0, bad);
        r.refresh_topology(&a);
        assert_eq!(queued_strings(&r).len(), 2);
    }

    #[tokio::test]
    async fn queue_does_not_deduplicate_fresh_adds() {
        // capacity.go:115-120: `QueueKey` holds pointers and every
        // `queue.Add(QueueKey{item: &item})` uses a fresh one, so the workqueue
        // keeps both entries.
        let (_s, _c, r) = refresh_fixture(&[&[("layer0", "foo")]]).await;
        r.refresh_sc("direct-sc");
        r.refresh_sc("direct-sc");
        assert_eq!(queued_strings(&r).len(), 2);
    }

    #[tokio::test]
    async fn sync_queued_refreshes_only_the_queued_items() {
        let (st, cl, r) = refresh_fixture(&[&[("layer0", "foo")], &[("layer0", "bar")]]).await;
        *cl.capacity.lock().unwrap() = 99;
        cl.requests.lock().unwrap().clear();
        r.refresh_topology(&vna(&[("layer0", "bar")]));
        let c = ctl_with(&st, &cl, r.clone());
        c.sync_queued(Duration::from_secs(1), Duration::from_secs(300))
            .await;
        assert_eq!(cl.requests.lock().unwrap().len(), 2);
        let caps = list_caps(&st).await;
        let updated = caps
            .iter()
            .filter(|c| c.capacity.as_deref() == Some("99"))
            .count();
        assert_eq!(updated, 2);
        assert!(r.queued().is_empty());
    }

    #[tokio::test]
    async fn sync_queued_skips_items_that_are_no_longer_wanted() {
        let (st, cl, r) = refresh_fixture(&[&[("layer0", "foo")]]).await;
        r.refresh_sc("direct-sc");
        // The StorageClass is gone from the lister while the item is still
        // queued: "Another indication that the value is no longer needed."
        r.lock().classes.remove("direct-sc");
        cl.requests.lock().unwrap().clear();
        let c = ctl_with(&st, &cl, r.clone());
        c.sync_queued(Duration::from_secs(1), Duration::from_secs(300))
            .await;
        assert!(cl.requests.lock().unwrap().is_empty());
        // The item left `capacities`: it "became obsolete".
        r.refresh_sc("triple-sc");
        r.set_capacities(HashSet::new());
        c.sync_queued(Duration::from_secs(1), Duration::from_secs(300))
            .await;
        assert!(cl.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_item_is_requeued_with_exponential_backoff_until_it_succeeds() {
        let (st, cl, r) = refresh_fixture(&[&[("layer0", "foo")]]).await;
        *cl.fail.lock().unwrap() = true;
        r.refresh_sc("direct-sc");
        let c = ctl_with(&st, &cl, r.clone());
        c.sync_queued(Duration::from_millis(10), Duration::from_secs(1))
            .await;
        // `AddRateLimited`: not queued yet, but scheduled.
        assert!(r.queued().is_empty());
        assert!(r.next_delayed().is_some());
        *cl.fail.lock().unwrap() = false;
        tokio::time::sleep(Duration::from_millis(30)).await;
        c.sync_queued(Duration::from_millis(10), Duration::from_secs(1))
            .await;
        // `Forget` after success: nothing pending.
        assert!(r.next_delayed().is_none());
        assert!(r.queued().is_empty());
    }

    #[test]
    fn rate_limiter_doubles_per_failure_and_caps_at_max() {
        let r = CapacityRefresher::new();
        let k = QueueKey::Item {
            id: 1,
            item: ItemKey {
                segment: seg(&[]),
                storage_class_name: "sc".into(),
            },
        };
        let (s, m) = (Duration::from_secs(1), Duration::from_secs(5));
        let d: Vec<_> = (0..5)
            .map(|_| r.add_rate_limited(k.clone(), s, m))
            .collect();
        assert_eq!(
            d,
            [1, 2, 4, 5, 5].map(Duration::from_secs).to_vec(),
            "ItemExponentialFailureRateLimiter.When"
        );
        r.forget(&k);
        assert_eq!(r.add_rate_limited(k, s, m), s);
    }

    // ---- TestTermToSegment (capacity_test.go:1686-1797) ----

    #[test]
    fn term_to_segment_rejects_match_fields() {
        let t = NodeSelectorTerm {
            match_expressions: None,
            match_fields: Some(vec![req("name", "In", &["worker-1"])]),
        };
        assert!(term_to_segment(&t).is_err());
    }

    #[test]
    fn term_to_segment_rejects_non_in_operator_and_multi_values() {
        let t = |r| NodeSelectorTerm {
            match_expressions: Some(vec![r]),
            match_fields: None,
        };
        assert!(term_to_segment(&t(req("segment", "NotIn", &["a"]))).is_err());
        assert!(term_to_segment(&t(req("segment", "In", &["a", "b"]))).is_err());
    }

    #[test]
    fn term_to_segment_sorts_entries() {
        let t = NodeSelectorTerm {
            match_expressions: Some(vec![
                req("zone", "In", &["X"]),
                req("segment", "In", &["a"]),
            ]),
            match_fields: None,
        };
        assert_eq!(
            term_to_segment(&t).unwrap(),
            seg(&[("segment", "a"), ("zone", "X")])
        );
    }

    // ---- Segment ----

    #[test]
    fn segment_compare_is_length_first_then_entrywise() {
        // topology.go:57-71: fewer keys is smaller before any value is looked at.
        let short = seg(&[("z", "9")]);
        let long = seg(&[("a", "1"), ("b", "1")]);
        assert_eq!(short.compare(&long), Ordering::Less);
        assert_eq!(long.compare(&short), Ordering::Greater);
        assert_eq!(long.compare(&long.clone()), Ordering::Equal);
        assert_eq!(
            seg(&[("a", "1")]).compare(&seg(&[("a", "2")])),
            Ordering::Less
        );
    }

    #[test]
    fn segment_label_selector_is_match_labels_only() {
        let s = seg(&[("a", "1"), ("b", "2")]);
        let sel = s.label_selector();
        assert_eq!(sel.match_labels.unwrap(), s.label_map());
        assert!(sel.match_expressions.is_none());
        assert_eq!(s.label_map().len(), 2);
    }

    // ---- nodeTopology.sync (nodes_test.go TestNodeTopology) ----

    fn csi_node(name: &str, driver: &str, keys: Option<Vec<&str>>) -> CSINode {
        CSINode {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta::new(name),
            spec: CSINodeSpec {
                drivers: vec![CSINodeDriver {
                    name: driver.into(),
                    node_id: name.into(),
                    topology_keys: keys.map(|k| k.into_iter().map(String::from).collect()),
                    allocatable: None,
                }],
            },
        }
    }

    fn node(name: &str, labels: &[(&str, &str)]) -> Node {
        let mut n = Node::new(name);
        n.metadata.labels = Some(
            labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        n
    }

    #[test]
    fn discover_segments_dedups_and_skips_other_drivers_and_missing_labels() {
        let csi_nodes = vec![
            csi_node("n1", DRIVER, Some(vec!["B", "A"])),
            csi_node("n2", DRIVER, Some(vec!["A", "B"])),
            csi_node("n3", "other-driver", Some(vec!["A", "B"])),
            // label B not on the Node yet: ignored until the node update.
            csi_node("n4", DRIVER, Some(vec!["A", "B"])),
            // CSINode without a Node: obsolete, ignored.
            csi_node("n5", DRIVER, Some(vec!["A", "B"])),
            // driver without topology keys: the empty segment (matches all nodes).
            csi_node("n6", DRIVER, None),
        ];
        let nodes = vec![
            node("n1", &[("A", "x"), ("B", "y")]),
            node("n2", &[("A", "x"), ("B", "y")]),
            node("n3", &[("A", "x"), ("B", "y")]),
            node("n4", &[("A", "x")]),
            node("n6", &[]),
        ];
        let mut got = discover_segments(&csi_nodes, &nodes, DRIVER);
        got.sort_by(|a, b| a.compare(b));
        assert_eq!(
            got,
            vec![Segment::default(), seg(&[("A", "x"), ("B", "y")])]
        );
    }

    // ---- capacity reconcile (capacity_test.go TestCapacityController) ----

    #[derive(Default)]
    struct FakeClient {
        capacity: Mutex<i64>,
        max: Mutex<Option<i64>>,
        requests: Mutex<Vec<GetCapacityRequest>>,
        fail: Mutex<bool>,
        /// How long a `GetCapacity` call takes.
        delay: Mutex<Duration>,
    }

    impl CapacityClient for FakeClient {
        async fn get_capacity(
            &self,
            req: GetCapacityRequest,
        ) -> std::result::Result<GetCapacityResponse, String> {
            self.requests.lock().unwrap().push(req);
            let delay = *self.delay.lock().unwrap();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            if *self.fail.lock().unwrap() {
                return Err("boom".into());
            }
            Ok(GetCapacityResponse {
                available_capacity: *self.capacity.lock().unwrap(),
                maximum_volume_size: *self.max.lock().unwrap(),
                ..Default::default()
            })
        }
    }

    fn sc(name: &str, provisioner: &str, mode: Option<VolumeBindingMode>) -> StorageClass {
        StorageClass {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta::new(name),
            provisioner: provisioner.into(),
            parameters: Some([("foo".to_string(), "bar".to_string())].into()),
            reclaim_policy: None,
            volume_binding_mode: mode,
            allowed_topologies: None,
            allow_volume_expansion: None,
            mount_options: None,
        }
    }

    fn owner() -> OwnerReference {
        let mut o = OwnerReference::new("apps/v1", "Deployment", "csi-ctrl", "owner-uid");
        o.controller = Some(true);
        o
    }

    async fn put_sc(st: &MemoryStorage, s: StorageClass) {
        st.create(&build_key("storageclasses", None, &s.metadata.name), &s)
            .await
            .unwrap();
    }

    async fn list_caps(st: &MemoryStorage) -> Vec<CSIStorageCapacity> {
        st.list::<CSIStorageCapacity>(&format!("/registry/csistoragecapacities/{NS}/"))
            .await
            .unwrap()
    }

    fn ctl<'a>(
        st: &'a MemoryStorage,
        cl: &'a FakeClient,
        immediate: bool,
    ) -> CapacityController<'a, MemoryStorage, FakeClient> {
        CapacityController {
            storage: st,
            client: cl,
            driver_name: DRIVER.into(),
            owner: Some(owner()),
            managed_by_id: MANAGED.into(),
            owner_namespace: NS.into(),
            immediate_binding: immediate,
            refresher: Default::default(),
            limiter: std::sync::Arc::new(rusternetes_client::ratelimit::RateLimiter::unlimited()),
        }
    }

    fn ctl_with<'a>(
        st: &'a MemoryStorage,
        cl: &'a FakeClient,
        refresher: std::sync::Arc<CapacityRefresher>,
    ) -> CapacityController<'a, MemoryStorage, FakeClient> {
        CapacityController {
            refresher,
            ..ctl(st, cl, false)
        }
    }

    #[tokio::test]
    async fn publishes_one_object_per_segment_and_class_with_labels_owner_and_sizes() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 1 << 30;
        *cl.max.lock().unwrap() = Some(1 << 20);
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        put_sc(&st, sc("foreign-sc", "other-driver", None)).await;
        let segs = vec![seg(&[("zone", "a")]), seg(&[("zone", "b")])];
        settle(&ctl(&st, &cl, false), &st, &segs).await;

        let caps = list_caps(&st).await;
        assert_eq!(caps.len(), 2);
        for c in &caps {
            assert!(c.metadata.name.starts_with("csisc-"));
            assert_eq!(c.metadata.namespace.as_deref(), Some(NS));
            assert_eq!(c.storage_class_name, "late-sc");
            assert_eq!(c.capacity.as_deref(), Some("1Gi"));
            assert_eq!(c.maximum_volume_size.as_deref(), Some("1Mi"));
            let l = c.metadata.labels.as_ref().unwrap();
            assert_eq!(l[DRIVER_NAME_LABEL], DRIVER);
            assert_eq!(l[MANAGED_BY_LABEL], MANAGED);
            let o = &c.metadata.owner_references.as_ref().unwrap()[0];
            assert_eq!(o.uid, "owner-uid");
        }
        // capacity.go:599-604: the request carries the class parameters and
        // the segment as AccessibleTopology, and no VolumeCapabilities.
        let reqs = cl.requests.lock().unwrap();
        assert_eq!(reqs.len(), 2);
        assert!(reqs.iter().all(|r| r.parameters["foo"] == "bar"
            && r.volume_capabilities.is_empty()
            && r.accessible_topology.is_some()));
    }

    #[tokio::test]
    async fn stats_count_goal_current_and_obsolete() {
        // capacity.go:769-807: goal = work items, current = items with an
        // object, obsolete = managed objects matching no item.
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let c = ctl(&st, &cl, false);
        settle(&c, &st, &[seg(&[("zone", "a")]), seg(&[("zone", "b")])]).await;
        // Every object is created and has been seen, so each item has one.
        assert_eq!(
            c.stats(),
            CapacityStats {
                goal: 2,
                current: 2,
                obsolete: 0
            }
        );

        // Zone b disappears: its object is deleted, so nothing obsolete remains.
        settle(&c, &st, &[seg(&[("zone", "a")])]).await;
        assert_eq!(
            c.stats(),
            CapacityStats {
                goal: 1,
                current: 1,
                obsolete: 0
            }
        );
        assert_eq!(list_caps(&st).await.len(), 1);
    }

    #[tokio::test]
    async fn the_gauges_are_exported_per_driver_at_scrape_time() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let c = ctl(&st, &cl, false);
        c.register_metrics();
        settle(&c, &st, &[seg(&[("zone", "a")])]).await;
        let out = super::super::csi_capacity_metrics::gather();
        for (n, v) in [
            ("csistoragecapacities_desired_goal", 1),
            ("csistoragecapacities_desired_current", 1),
            ("csistoragecapacities_obsolete", 0),
        ] {
            let want = format!("{n}{{driver_name=\"{DRIVER}\"}} {v}");
            assert!(out.contains(&want), "{want} missing in:\n{out}");
        }
    }

    #[tokio::test]
    async fn unchanged_object_is_not_rewritten_and_changed_capacity_is_updated() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 100;
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let segs = vec![seg(&[("zone", "a")])];
        let c = ctl(&st, &cl, false);
        settle(&c, &st, &segs).await;
        let first = list_caps(&st).await.remove(0);

        settle(&c, &st, &segs).await;
        let same = list_caps(&st).await.remove(0);
        assert_eq!(
            first.metadata.resource_version,
            same.metadata.resource_version
        );

        *cl.capacity.lock().unwrap() = 200;
        settle(&c, &st, &segs).await;
        let caps = list_caps(&st).await;
        assert_eq!(caps.len(), 1, "update, not a second object");
        assert_eq!(caps[0].metadata.name, first.metadata.name);
        assert_eq!(caps[0].capacity.as_deref(), Some("200"));
    }

    #[tokio::test]
    async fn update_keeps_the_stored_identity() {
        // The API server's Store turns a non-empty metadata.uid on a PUT into
        // a UID precondition (rest/update.go:188-203): the update must carry
        // the stored uid and creationTimestamp, as upstream's DeepCopy does.
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let segs = vec![seg(&[("zone", "a")])];
        let c = ctl(&st, &cl, false);
        settle(&c, &st, &segs).await;
        let before = list_caps(&st).await.remove(0);
        *cl.capacity.lock().unwrap() = 7;
        settle(&c, &st, &segs).await;
        let after = list_caps(&st).await.remove(0);
        assert_eq!(after.capacity.as_deref(), Some("7"));
        assert_eq!(after.metadata.uid, before.metadata.uid);
        assert_eq!(
            after.metadata.creation_timestamp,
            before.metadata.creation_timestamp
        );
    }

    #[tokio::test]
    async fn max_volume_size_change_and_missing_owner_trigger_update() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 100;
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let segs = vec![seg(&[])];
        let c = ctl(&st, &cl, false);
        settle(&c, &st, &segs).await;

        // capacity.go:643-650: MaximumVolumeSize differs -> update.
        *cl.max.lock().unwrap() = Some(10);
        settle(&c, &st, &segs).await;
        assert_eq!(
            list_caps(&st).await[0].maximum_volume_size.as_deref(),
            Some("10")
        );

        // capacity.go:641 / 660-662: owner reference stripped -> re-added.
        let mut obj = list_caps(&st).await.remove(0);
        obj.metadata.owner_references = None;
        st.update(
            &build_key("csistoragecapacities", Some(NS), &obj.metadata.name),
            &obj,
        )
        .await
        .unwrap();
        settle(&c, &st, &segs).await;
        let o = list_caps(&st)
            .await
            .remove(0)
            .metadata
            .owner_references
            .unwrap();
        assert_eq!(o.len(), 1);
        assert_eq!(o[0].uid, "owner-uid");
    }

    #[tokio::test]
    async fn obsolete_duplicate_and_unmanaged_objects() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 5;
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let c = ctl(&st, &cl, false);
        let both = vec![seg(&[("zone", "a")]), seg(&[("zone", "b")])];
        settle(&c, &st, &both).await;
        assert_eq!(list_caps(&st).await.len(), 2);

        // A foreign object (other managed-by) must survive (capacity.go:782-786).
        let mut foreign = list_caps(&st).await.remove(0);
        foreign.metadata.name = "foreign".into();
        foreign.metadata.uid = "foreign-uid".into();
        foreign
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .insert(MANAGED_BY_LABEL.into(), "someone-else".into());
        st.create(
            &build_key("csistoragecapacities", Some(NS), "foreign"),
            &foreign,
        )
        .await
        .unwrap();
        // A duplicate of a managed one: "should never happen, but ... clean that up".
        let mut dup = list_caps(&st)
            .await
            .into_iter()
            .find(|x| x.metadata.name != "foreign")
            .unwrap();
        dup.metadata.name = "dup".into();
        dup.metadata.uid = "dup-uid".into();
        st.create(&build_key("csistoragecapacities", Some(NS), "dup"), &dup)
            .await
            .unwrap();

        // Segment b disappears -> its object goes; the duplicate goes too.
        settle(&c, &st, &both[..1]).await;
        let names: Vec<_> = list_caps(&st)
            .await
            .into_iter()
            .map(|c| c.metadata.name)
            .collect();
        assert!(names.contains(&"foreign".to_string()));
        assert_eq!(
            names.len(),
            2,
            "one managed object + the foreign one: {names:?}"
        );

        // Storage class gone -> everything managed goes.
        st.delete(&build_key("storageclasses", None, "late-sc"))
            .await
            .unwrap();
        settle(&c, &st, &both[..1]).await;
        let names: Vec<_> = list_caps(&st)
            .await
            .into_iter()
            .map(|c| c.metadata.name)
            .collect();
        assert_eq!(names, vec!["foreign".to_string()]);
    }

    #[tokio::test]
    async fn immediate_binding_classes_are_skipped_unless_enabled() {
        // capacity.go:379-381 (onTopologyChanges) and :412-415 (onSCAddOrUpdate).
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("imm", DRIVER, Some(VolumeBindingMode::Immediate))).await;
        put_sc(
            &st,
            sc(
                "late",
                DRIVER,
                Some(VolumeBindingMode::WaitForFirstConsumer),
            ),
        )
        .await;
        let segs = vec![seg(&[("zone", "a")])];
        settle(&ctl(&st, &cl, false), &st, &segs).await;
        let classes: Vec<_> = list_caps(&st)
            .await
            .into_iter()
            .map(|c| c.storage_class_name)
            .collect();
        assert_eq!(classes, vec!["late".to_string()]);
        settle(&ctl(&st, &cl, true), &st, &segs).await;
        assert_eq!(list_caps(&st).await.len(), 2);
    }

    #[test]
    fn managed_by_id_matches_upstream_naming() {
        assert_eq!(managed_by_id(None), "external-provisioner");
        assert_eq!(managed_by_id(Some("node-1")), "external-provisioner-node-1");
        let long = "n".repeat(80);
        let id = managed_by_id(Some(&long));
        assert!(id.len() <= 63, "{id}");
    }

    #[tokio::test]
    async fn lookup_owner_walks_controller_chain() {
        use serde_json::json;
        let st = MemoryStorage::new();
        st.create(
            &build_key("pods", Some(NS), "p"),
            &json!({"metadata":{"name":"p","uid":"pu","ownerReferences":[
                {"apiVersion":"apps/v1","kind":"ReplicaSet","name":"rs","uid":"rsu","controller":true}]}}),
        )
        .await
        .unwrap();
        st.create(
            &build_key("replicasets", Some(NS), "rs"),
            &json!({"metadata":{"name":"rs","uid":"rsu","ownerReferences":[
                {"apiVersion":"apps/v1","kind":"Deployment","name":"d","uid":"du","controller":true}]}}),
        )
        .await
        .unwrap();
        let ul = RateLimiter::unlimited();
        let o0 = lookup_owner(&st, NS, "p", "v1", "Pod", 0, &ul)
            .await
            .unwrap();
        assert_eq!((o0.kind.as_str(), o0.uid.as_str()), ("Pod", "pu"));
        let o1 = lookup_owner(&st, NS, "p", "v1", "Pod", 1, &ul)
            .await
            .unwrap();
        assert_eq!((o1.kind.as_str(), o1.uid.as_str()), ("ReplicaSet", "rsu"));
        let o2 = lookup_owner(&st, NS, "p", "v1", "Pod", 2, &ul)
            .await
            .unwrap();
        assert_eq!((o2.kind.as_str(), o2.uid.as_str()), ("Deployment", "du"));
        assert_eq!(o2.controller, Some(true));
        assert!(lookup_owner(&st, NS, "p", "v1", "Pod", 3, &ul)
            .await
            .is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn lookup_owner_gets_go_through_the_capacity_rate_limiter() {
        // csi-provisioner.go:456-474: `config.QPS/Burst` are set to the capacity
        // values BEFORE `owner.Lookup(config, ...)`, which builds its own client
        // (pkg/owner/owner.go:37-44) with a token bucket of QPS 1 / burst 1
        // here: the 2nd GET of a two-level walk waits 1s.
        use serde_json::json;
        let st = MemoryStorage::new();
        st.create(
            &build_key("pods", Some(NS), "p"),
            &json!({"metadata":{"name":"p","uid":"pu","ownerReferences":[
                {"apiVersion":"apps/v1","kind":"ReplicaSet","name":"rs","uid":"rsu","controller":true}]}}),
        )
        .await
        .unwrap();
        st.create(
            &build_key("replicasets", Some(NS), "rs"),
            &json!({"metadata":{"name":"rs","uid":"rsu","ownerReferences":[
                {"apiVersion":"apps/v1","kind":"Deployment","name":"d","uid":"du","controller":true}]}}),
        )
        .await
        .unwrap();
        let l = RateLimiter::new(1.0, 1.0);
        let start = tokio::time::Instant::now();
        lookup_owner(&st, NS, "p", "v1", "Pod", 2, &l)
            .await
            .unwrap();
        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test]
    async fn topology_queue_coalesces_a_burst_into_one_sync() {
        // nodes.go:63-150 handlers only `queue.Add("")`; RunWorker (:226-243)
        // syncs once per Get, so n adds before the worker runs are one sync.
        let q = TopologyQueue::default();
        for _ in 0..1000 {
            q.add();
        }
        tokio::time::timeout(Duration::from_millis(100), q.get())
            .await
            .expect("one pending sync");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), q.get())
                .await
                .is_err(),
            "the burst must have been coalesced into a single sync"
        );
    }

    #[tokio::test]
    async fn topology_queue_add_while_syncing_queues_one_more_sync() {
        // workqueue Add during processing marks the item dirty; Done queues it
        // again exactly once.
        let q = TopologyQueue::default();
        q.add();
        q.get().await;
        q.add();
        q.add();
        tokio::time::timeout(Duration::from_millis(100), q.get())
            .await
            .expect("one re-queued sync");
        assert!(tokio::time::timeout(Duration::from_millis(100), q.get())
            .await
            .is_err());
    }

    #[test]
    fn fixed_segment_sorts_entries() {
        let m: HashMap<String, String> = [
            ("b".to_string(), "2".to_string()),
            ("a".to_string(), "1".to_string()),
        ]
        .into();
        assert_eq!(fixed_segment(Some(&m)), seg(&[("a", "1"), ("b", "2")]));
        assert_eq!(fixed_segment(None), Segment::default());
    }

    #[tokio::test]
    async fn capacity_tick_discovers_segments_from_csinodes() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let c = ctl(&st, &cl, false);
        // No CSINodes: no segments, nothing published.
        c.prepare(&TopologySource::Discovered).await.unwrap();
        pump(&c, &st).await;
        assert!(list_caps(&st).await.is_empty());
        // Fixed topology publishes exactly one object.
        c.prepare(&TopologySource::Fixed(seg(&[("zone", "a")])))
            .await
            .unwrap();
        pump(&c, &st).await;
        assert_eq!(list_caps(&st).await.len(), 1);
    }

    #[test]
    fn driver_topology_keys_is_none_without_the_driver_and_empty_without_keys() {
        // nodes.go:175-185
        let csi = |driver: &str, keys: Option<Vec<&str>>| CSINode {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta::new("n"),
            spec: CSINodeSpec {
                drivers: vec![CSINodeDriver {
                    name: driver.into(),
                    node_id: "n".into(),
                    topology_keys: keys.map(|k| k.into_iter().map(String::from).collect()),
                    allocatable: None,
                }],
            },
        };
        assert_eq!(driver_topology_keys(&csi("other", None), DRIVER), None);
        assert_eq!(
            driver_topology_keys(&csi(DRIVER, None), DRIVER),
            Some(vec![])
        );
        assert_eq!(
            driver_topology_keys(&csi(DRIVER, Some(vec!["a"])), DRIVER),
            Some(vec!["a".to_string()])
        );
    }

    // ---- event-driven triggers (#3548): capacity.go onSCAddOrUpdate etc. ----

    async fn list_scs(st: &MemoryStorage) -> Vec<StorageClass> {
        st.list::<StorageClass>(&build_prefix("storageclasses", None))
            .await
            .unwrap()
    }

    /// `prepare` (capacity.go:262-337): classes and topology are known, then
    /// the existing objects are fed to `onCAddOrUpdate`.
    async fn boot<'a>(
        st: &'a MemoryStorage,
        cl: &'a FakeClient,
        immediate: bool,
        segs: &[Segment],
    ) -> CapacityController<'a, MemoryStorage, FakeClient> {
        let c = ctl(st, cl, immediate);
        c.apply_sc_list(list_scs(st).await);
        c.apply_topology(segs.to_vec());
        c.apply_c_list(list_caps(st).await);
        c
    }

    /// A full pass of the controller's loops over `segs`: `prepare`, one
    /// `pollCapacities` tick and the workers, with the informer feeding their
    /// writes back.
    async fn settle(
        c: &CapacityController<'_, MemoryStorage, FakeClient>,
        st: &MemoryStorage,
        segs: &[Segment],
    ) {
        c.apply_sc_list(list_scs(st).await);
        c.apply_topology(segs.to_vec());
        c.apply_c_list(list_caps(st).await);
        // The queue keeps every add (fresh `QueueKey` pointers, capacity.go:
        // 115-120), so the entries of `apply_topology` would be synced as well
        // as those of the poll. upstream's test queue (capacity_test.go:
        // 1383-1403) collapses them; do the same here to count one sync per item.
        c.refresher.clear_queue();
        c.poll_capacities();
        pump(c, st).await;
    }

    /// The informer delivering the effects of the workers' writes back to the
    /// handlers, until nothing is left to do.
    async fn pump(c: &CapacityController<'_, MemoryStorage, FakeClient>, st: &MemoryStorage) {
        for _ in 0..6 {
            c.sync_queued(Duration::from_millis(1), Duration::from_secs(1))
                .await;
            c.apply_c_list(list_caps(st).await);
        }
    }

    fn queued_classes(c: &CapacityRefresher) -> Vec<String> {
        let mut v: Vec<String> = c
            .queued()
            .iter()
            .map(|i| i.storage_class_name.clone())
            .collect();
        v.sort();
        v
    }

    #[tokio::test]
    async fn sc_added_enqueues_one_item_per_known_segment() {
        // onSCAddOrUpdate (capacity.go:370-387)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        let c = boot(
            &st,
            &cl,
            false,
            &[seg(&[("zone", "a")]), seg(&[("zone", "b")])],
        )
        .await;
        assert!(queued_classes(&c.refresher).is_empty());
        put_sc(&st, sc("late", DRIVER, None)).await;
        c.apply_sc_list(list_scs(&st).await);
        assert_eq!(queued_classes(&c.refresher), vec!["late", "late"]);
        pump(&c, &st).await;
        assert_eq!(list_caps(&st).await.len(), 2);
    }

    #[tokio::test]
    async fn sc_of_other_driver_or_immediate_binding_is_ignored_on_add() {
        // capacity.go:371-379
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        put_sc(&st, sc("foreign", "other-driver", None)).await;
        put_sc(&st, sc("imm", DRIVER, Some(VolumeBindingMode::Immediate))).await;
        c.apply_sc_list(list_scs(&st).await);
        assert!(queued_classes(&c.refresher).is_empty());
        let c = boot(&st, &cl, true, &[seg(&[("zone", "a")])]).await;
        assert_eq!(queued_classes(&c.refresher), vec!["imm"]);
    }

    #[tokio::test]
    async fn sc_deleted_removes_items_and_enqueues_object_removal() {
        // onSCDelete (capacity.go:390-403) + removeWorkItem (:488-512)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        pump(&c, &st).await;
        assert_eq!(list_caps(&st).await.len(), 1);
        st.delete(&build_key("storageclasses", None, "late"))
            .await
            .unwrap();
        c.apply_sc_list(list_scs(&st).await);
        assert_eq!(c.stats().goal, 0);
        // The object itself is queued for removal, not left for a poll tick.
        assert_eq!(list_caps(&st).await.len(), 1);
        pump(&c, &st).await;
        assert!(list_caps(&st).await.is_empty());
    }

    #[tokio::test]
    async fn topology_added_creates_objects_for_all_matching_classes() {
        // "add storage topology segment" (capacity_test.go:927-1001)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 1 << 30;
        put_sc(&st, sc("imm", DRIVER, Some(VolumeBindingMode::Immediate))).await;
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[]).await;
        c.apply_topology(vec![seg(&[("zone", "a")])]);
        pump(&c, &st).await;
        let caps = list_caps(&st).await;
        assert_eq!(caps.len(), 1);
        assert_eq!(caps[0].storage_class_name, "late");
        assert_eq!(c.stats().goal, 1);
        assert_eq!(c.stats().current, 1);
    }

    #[tokio::test]
    async fn topology_removed_deletes_objects_and_added_creates_new_ones() {
        // "add and remove storage topology segment" (capacity_test.go:1039-1085)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        pump(&c, &st).await;
        c.apply_topology(vec![seg(&[("zone", "b")])]);
        pump(&c, &st).await;
        let caps = list_caps(&st).await;
        assert_eq!(caps.len(), 1);
        assert_eq!(
            caps[0].node_topology.as_ref().unwrap().match_labels,
            Some([("zone".to_string(), "b".to_string())].into())
        );
        c.apply_topology(vec![]);
        pump(&c, &st).await;
        assert!(list_caps(&st).await.is_empty());
    }

    #[tokio::test]
    async fn capacity_deleted_by_someone_is_recreated() {
        // "re-create capacity" (capacity_test.go:744-783); onCDelete (:728-741)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        pump(&c, &st).await;
        let before = list_caps(&st).await.remove(0);
        st.delete(&build_key(
            "csistoragecapacities",
            Some(NS),
            &before.metadata.name,
        ))
        .await
        .unwrap();
        c.informer_c_delete(&before);
        pump(&c, &st).await;
        let after = list_caps(&st).await;
        assert_eq!(after.len(), 1);
        assert_ne!(after[0].metadata.uid, before.metadata.uid);
    }

    #[tokio::test]
    async fn capacity_that_lost_its_labels_is_left_alone_and_recreated() {
        // "ignore capacity after owner change" (capacity_test.go:793-844);
        // onCAddOrUpdate !isManaged branch (:688-701)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        pump(&c, &st).await;
        let mut obj = list_caps(&st).await.remove(0);
        obj.metadata.labels = None;
        st.update(
            &build_key("csistoragecapacities", Some(NS), &obj.metadata.name),
            &obj,
        )
        .await
        .unwrap();
        c.apply_c_list(list_caps(&st).await);
        pump(&c, &st).await;
        let caps = list_caps(&st).await;
        assert_eq!(caps.len(), 2, "foreign object kept, new one created");
        assert_eq!(
            caps.iter().filter(|c| c.metadata.labels.is_some()).count(),
            1
        );
    }

    #[tokio::test]
    async fn delete_and_recreate_by_someone_with_wrong_capacity_is_corrected() {
        // "delete and recreate by someone" (capacity_test.go:845-892)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 1 << 30;
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        pump(&c, &st).await;
        let old = list_caps(&st).await.remove(0);
        st.delete(&build_key(
            "csistoragecapacities",
            Some(NS),
            &old.metadata.name,
        ))
        .await
        .unwrap();
        let mut new = old.clone();
        new.metadata.uid = "uid-2".into();
        new.capacity = Some("1Mi".into());
        st.create(
            &build_key("csistoragecapacities", Some(NS), &new.metadata.name),
            &new,
        )
        .await
        .unwrap();
        // Watch order: Deleted(old), then Added(new).
        c.informer_c_delete(&old);
        let stored = list_caps(&st).await.remove(0);
        c.informer_c_upsert(&stored);
        pump(&c, &st).await;
        let caps = list_caps(&st).await;
        assert_eq!(caps.len(), 1);
        assert_eq!(caps[0].capacity.as_deref(), Some("1Gi"));
    }

    #[tokio::test]
    async fn redundant_managed_capacity_is_deleted_on_add() {
        // "delete redundant capacity" (capacity_test.go:784-792); :723-725
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        let c = boot(&st, &cl, false, &[]).await;
        let mut dup = CSIStorageCapacity {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta::new("csisc-dup").with_namespace(NS),
            storage_class_name: "late".into(),
            capacity: Some("1Gi".into()),
            maximum_volume_size: None,
            node_topology: Some(seg(&[("zone", "a")]).label_selector()),
        };
        dup.metadata.uid = "dup-uid".into();
        dup.metadata.labels = Some(
            [
                (DRIVER_NAME_LABEL.to_string(), DRIVER.to_string()),
                (MANAGED_BY_LABEL.to_string(), MANAGED.to_string()),
            ]
            .into(),
        );
        st.create(
            &build_key("csistoragecapacities", Some(NS), "csisc-dup"),
            &dup,
        )
        .await
        .unwrap();
        c.informer_c_upsert(&dup);
        pump(&c, &st).await;
        assert!(list_caps(&st).await.is_empty());
    }

    #[tokio::test]
    async fn gauges_are_computed_lazily_from_current_state() {
        // CollectWithStability (capacity.go:751-767): no tick stores them; an
        // object created by a worker is not counted until its event arrives
        // (:642-645).
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        assert_eq!(
            c.stats(),
            CapacityStats {
                goal: 1,
                current: 0,
                obsolete: 0
            }
        );
        c.sync_queued(Duration::from_millis(1), Duration::from_secs(1))
            .await;
        assert_eq!(c.stats().current, 0, "created, event not seen yet");
        c.apply_c_list(list_caps(&st).await);
        assert_eq!(
            c.stats(),
            CapacityStats {
                goal: 1,
                current: 1,
                obsolete: 0
            }
        );
        // "obsolete object, missing SC" (capacity_test.go:440-459)
        st.delete(&build_key("storageclasses", None, "late"))
            .await
            .unwrap();
        c.apply_sc_list(list_scs(&st).await);
        assert_eq!(
            c.stats(),
            CapacityStats {
                goal: 0,
                current: 0,
                obsolete: 1
            }
        );
    }

    /// Poll `f` until it holds, for at most 5s.
    async fn eventually(what: &str, mut f: impl AsyncFnMut() -> bool) {
        for _ in 0..250 {
            if f().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for: {what}");
    }

    #[tokio::test]
    async fn run_loop_reacts_to_watch_events_long_before_the_poll_tick() {
        // The StorageClass, Node and CSINode informers drive the queue
        // (capacity.go:188-219 + nodes.go:63-150); the poll period is an hour.
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 1 << 30;
        let c = ctl(&st, &cl, false);
        let run = run_capacity_loop(
            &c,
            &TopologySource::Discovered,
            Duration::from_secs(3600),
            Duration::from_millis(10),
            Duration::from_secs(1),
            2,
        );
        let scenario = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            put_sc(&st, sc("late-sc", DRIVER, None)).await;
            let n = node("n1", &[("zone", "a")]);
            st.create(&build_key("nodes", None, "n1"), &n)
                .await
                .unwrap();
            let csi = csi_node("n1", DRIVER, Some(vec!["zone"]));
            st.create(&build_key("csinodes", None, "n1"), &csi)
                .await
                .unwrap();
            eventually("object for the new segment", async || {
                list_caps(&st).await.len() == 1
            })
            .await;
            // A new label on the node changes the segment: new object, old gone.
            let mut n = node("n1", &[("zone", "b")]);
            n.metadata = st
                .get::<Node>(&build_key("nodes", None, "n1"))
                .await
                .unwrap()
                .metadata;
            n.metadata.labels = Some([("zone".to_string(), "b".to_string())].into());
            st.update(&build_key("nodes", None, "n1"), &n)
                .await
                .unwrap();
            eventually("object moved to zone b", async || {
                let caps = list_caps(&st).await;
                caps.len() == 1
                    && caps[0].node_topology.as_ref().unwrap().match_labels
                        == Some([("zone".to_string(), "b".to_string())].into())
            })
            .await;
            // The StorageClass goes: its object is removed by the delete event.
            st.delete(&build_key("storageclasses", None, "late-sc"))
                .await
                .unwrap();
            eventually("object removed with its class", async || {
                list_caps(&st).await.is_empty()
            })
            .await;
        };
        tokio::select! {
            _ = run => unreachable!("the loop never ends"),
            _ = scenario => {}
        }
    }

    // ---- #3547: capacity clientset rate limit and --capacity-threads ----

    fn zones(n: usize) -> Vec<Segment> {
        (0..n).map(|i| seg(&[("zone", &format!("z{i}"))])).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_creates_and_deletes_go_through_the_capacity_rate_limiter() {
        // csi-provisioner.go:456-458 builds the clientset that creates, updates
        // and deletes CSIStorageCapacity objects with its own QPS/Burst
        // (token bucket, rest/config.go:370-381): burst 2 at 1 QPS lets two
        // creates through at once, the 3rd and 4th wait 1s and 2s.
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late", DRIVER, None)).await;
        let limited = |qps: f64, burst: f64| CapacityController {
            limiter: std::sync::Arc::new(rusternetes_client::ratelimit::RateLimiter::new(
                qps, burst,
            )),
            ..ctl(&st, &cl, false)
        };
        let c = limited(1.0, 2.0);
        let start = tokio::time::Instant::now();
        settle(&c, &st, &zones(4)).await;
        assert_eq!(list_caps(&st).await.len(), 4);
        assert_eq!(start.elapsed(), Duration::from_secs(2));

        // Deletes of obsolete objects are throttled by the same bucket kind:
        // burst 1 at 1 QPS, four deletes take 3s.
        let c = limited(1.0, 1.0);
        let start = tokio::time::Instant::now();
        settle(&c, &st, &[]).await;
        assert!(list_caps(&st).await.is_empty());
        assert_eq!(start.elapsed(), Duration::from_secs(3));
    }

    #[test]
    fn capacity_limiter_follows_client_go_defaulting() {
        // rest/config.go:370-381: 0 -> DefaultQPS/DefaultBurst, qps <= 0 -> none.
        let l = capacity_limiter(1.0, 5);
        assert_eq!((l.qps(), l.burst()), (1.0, 5.0));
        let l = capacity_limiter(0.0, 0);
        assert_eq!((l.qps(), l.burst()), (5.0, 10.0));
        assert!(capacity_limiter(-1.0, 5).qps() <= 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_threads_workers_process_the_queue_concurrently() {
        // Controller.Run (capacity.go:238-253) starts `threadiness` workers on
        // the one queue; four 100ms GetCapacity calls take 100ms with four.
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.delay.lock().unwrap() = Duration::from_millis(100);
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &zones(4)).await;
        let start = tokio::time::Instant::now();
        let run = c.run_workers(4, Duration::from_millis(1), Duration::from_secs(1));
        tokio::select! {
            _ = run => unreachable!("workers never end"),
            _ = eventually("four objects", async || list_caps(&st).await.len() == 4) => {}
        }
        assert!(
            start.elapsed() < Duration::from_millis(200),
            "took {:?}",
            start.elapsed()
        );
        // The same item is never processed by two workers at once, so no
        // duplicate objects appear (checked by the count above plus a tick).
        c.apply_c_list(list_caps(&st).await);
        c.poll_capacities();
        tokio::select! {
            _ = c.run_workers(4, Duration::from_millis(1), Duration::from_secs(1)) => unreachable!(),
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
        assert_eq!(list_caps(&st).await.len(), 4);
    }

    #[test]
    fn workqueue_does_not_hand_out_a_key_that_is_being_processed() {
        // client-go workqueue Add/Get/Done: re-adding the same key in flight
        // marks it dirty; it is queued again by Done, never processed twice at
        // once. (Fresh adds of an item are fresh keys, see
        // `queue_does_not_deduplicate_fresh_adds`; the same key is added again
        // by `AddRateLimited` and by `Done`.)
        let r = CapacityRefresher::new();
        let k = ItemKey {
            segment: seg(&[("zone", "a")]),
            storage_class_name: "sc".into(),
        };
        r.set_capacities([k.clone()].into());
        r.refresh_sc("sc");
        let key = r.pop().expect("queued");
        r.lock().add(key.clone());
        r.lock().add(key.clone());
        assert_eq!(r.pop(), None, "in flight: must not be handed out again");
        r.done(&key);
        assert_eq!(r.pop(), Some(key.clone()));
        assert_eq!(r.pop(), None, "dirty twice, queued once");
    }
}
