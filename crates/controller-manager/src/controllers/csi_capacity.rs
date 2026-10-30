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
