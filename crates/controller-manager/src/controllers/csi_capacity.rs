//! CSIStorageCapacity publisher (#3214, overlaps #3201), a port of
//! kubernetes-csi/external-provisioner v6.4.0 `pkg/capacity/capacity.go`
//! (`Controller`) and `pkg/capacity/topology/{topology,nodes}.go`
//! (`Segment`, `nodeTopology.sync`).
//!
//! The Go controller is event/queue driven. This port keeps its *decisions*
//! (which `(topology segment, StorageClass)` pairs get an object, when an
//! object is created / updated / deleted, ownership and labels) as one
//! level-triggered `reconcile` pass over `Storage`; wiring it into a run loop
//! is `run_capacity_loop`, started from `main.rs` (#3219).

use super::csi_capacity_metrics::CapacityStats;
use super::csi_capacity_refresh::ItemKey;
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
pub async fn lookup_owner<S: Storage>(
    storage: &S,
    namespace: &str,
    name: &str,
    api_version: &str,
    kind: &str,
    levels: i32,
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
/// CSINode/Node, or the fixed segment of node-deployment mode.
#[derive(Clone)]
pub enum TopologySource {
    Discovered,
    Fixed(Segment),
}

/// One tick of the publisher: `onTopologyChanges` input, then `reconcile`.
pub async fn capacity_tick<S: Storage, C: CapacityClient>(
    ctl: &CapacityController<'_, S, C>,
    topology: &TopologySource,
) -> Result<()> {
    let segments = match topology {
        TopologySource::Fixed(s) => vec![s.clone()],
        TopologySource::Discovered => {
            let csi_nodes: Vec<CSINode> = ctl.storage.list(&build_prefix("csinodes", None)).await?;
            let nodes: Vec<Node> = ctl.storage.list(&build_prefix("nodes", None)).await?;
            discover_segments(&csi_nodes, &nodes, &ctl.driver_name)
        }
    };
    ctl.reconcile(&segments).await
}

/// `Controller.Run` (capacity.go:232-259): `pollCapacities` re-syncs every
/// object each `poll_interval` (`wait.UntilWithContext`), and the queue fed by
/// `refreshTopology`/`refreshSC` (provision.go) is worked off in between with
/// per-item exponential backoff. Deviation: topology and StorageClass changes
/// are picked up by the poll tick rather than from informer events, and there
/// is one worker (`--csi-capacity-threads` is accepted for flag parity only).
pub async fn run_capacity_loop<S: Storage, C: CapacityClient>(
    ctl: &CapacityController<'_, S, C>,
    topology: &TopologySource,
    poll_interval: std::time::Duration,
    retry_interval_start: std::time::Duration,
    retry_interval_max: std::time::Duration,
) {
    let mut next_poll = tokio::time::Instant::now();
    loop {
        if tokio::time::Instant::now() >= next_poll {
            if let Err(e) = capacity_tick(ctl, topology).await {
                tracing::error!("CSIStorageCapacity publisher ({}): {e}", ctl.driver_name);
            }
            next_poll = tokio::time::Instant::now() + poll_interval;
        }
        ctl.sync_queued(retry_interval_start, retry_interval_max)
            .await;
        let mut wake = next_poll;
        if let Some(d) = ctl.refresher.next_delayed() {
            wake = wake.min(tokio::time::Instant::from_std(d));
        }
        tokio::select! {
            _ = tokio::time::sleep_until(wake) => {}
            _ = ctl.refresher.notified() => {}
        }
    }
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
}

/// `WorkItem` (capacity.go:96-99) with the StorageClass resolved.
struct WorkItem<'s> {
    segment: &'s Segment,
    sc: &'s StorageClass,
}

impl WorkItem<'_> {
    /// `WorkItem.equals` (capacity.go:101-104).
    fn equals(&self, c: &CSIStorageCapacity) -> bool {
        c.storage_class_name == self.sc.metadata.name
            && c.node_topology.as_ref().is_some_and(|t| {
                t.match_labels.as_ref() == Some(&self.segment.label_map())
                    && t.match_expressions.as_ref().is_none_or(|e| e.is_empty())
            })
    }
}

/// `Quantity.Value()` of the stored string; `None` when unparseable.
fn quantity_value(q: &str) -> Option<i128> {
    Quantity::parse(q).ok().map(|q| q.value())
}

/// `sizesAreEqual` (capacity.go:841-849).
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

impl<S: Storage, C: CapacityClient> CapacityController<'_, S, C> {
    /// `isManaged` (capacity.go:828-831).
    fn is_managed(&self, c: &CSIStorageCapacity) -> bool {
        c.metadata.labels.as_ref().is_some_and(|l| {
            l.get(DRIVER_NAME_LABEL) == Some(&self.driver_name)
                && l.get(MANAGED_BY_LABEL) == Some(&self.managed_by_id)
        })
    }

    /// `isOwnedByUs` (capacity.go:817-824): a *controller* reference with our UID.
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

    /// One pass of `onTopologyChanges` (:365-391) + `onCAddOrUpdate`
    /// (:539-587) + `syncCapacity` (:586-715): publish, update and delete
    /// objects until they match `segments` x the driver's StorageClasses.
    pub async fn reconcile(&self, segments: &[Segment]) -> Result<()> {
        let (stats, r) = self.reconcile_with_stats(segments).await;
        super::csi_capacity_metrics::publish(&self.driver_name, stats);
        r
    }

    /// `reconcile` plus the gauge values of the pass, as
    /// `getObjectsGoal/Current/Obsolete` (capacity.go:769-807) would report
    /// them right after it: `obsolete` is what is left after the deletions.
    async fn reconcile_with_stats(&self, segments: &[Segment]) -> (CapacityStats, Result<()>) {
        let mut stats = CapacityStats::default();
        let r = self.reconcile_inner(segments, &mut stats).await;
        (stats, r)
    }

    async fn reconcile_inner(&self, segments: &[Segment], stats: &mut CapacityStats) -> Result<()> {
        let classes: Vec<StorageClass> = self
            .storage
            .list(&build_prefix("storageclasses", None))
            .await?;
        let mut items = Vec::new();
        for sc in &classes {
            if sc.provisioner != self.driver_name {
                continue;
            }
            // :379-381
            if !self.immediate_binding
                && sc.volume_binding_mode == Some(VolumeBindingMode::Immediate)
            {
                continue;
            }
            for segment in segments {
                items.push(WorkItem { segment, sc });
            }
        }

        // `addWorkItem` / `removeWorkItem` (:469-513): the refresh triggers
        // look items up in this set.
        self.refresher.set_capacities(
            items
                .iter()
                .map(|i| ItemKey {
                    segment: i.segment.clone(),
                    storage_class_name: i.sc.metadata.name.clone(),
                })
                .collect(),
        );

        let existing: Vec<CSIStorageCapacity> = self
            .storage
            .list(&build_prefix(
                "csistoragecapacities",
                Some(&self.owner_namespace),
            ))
            .await?;
        // Match each managed object to the first item it fits; foreign objects
        // are ignored (:547-562) and everything else is obsolete or a
        // duplicate and is deleted (:582-585).
        let mut matched: Vec<Option<CSIStorageCapacity>> = vec![None; items.len()];
        let mut first_err = None;
        for c in existing {
            if !self.is_managed(&c) {
                continue;
            }
            match items
                .iter()
                .enumerate()
                .find(|(i, it)| matched[*i].is_none() && it.equals(&c))
            {
                Some((i, _)) => matched[i] = Some(c),
                // A failed delete leaves the object obsolete (:792-807).
                None => {
                    if let Err(e) = self.delete_capacity(&c).await {
                        stats.obsolete += 1;
                        first_err.get_or_insert(e);
                    }
                }
            }
        }

        stats.goal = items.len() as i64;
        for (item, current) in items.iter().zip(matched) {
            let existed = current.is_some();
            match self.sync_capacity(item, current).await {
                Ok(created) => stats.current += i64::from(existed || created),
                Err(e) => {
                    stats.current += i64::from(existed);
                    first_err.get_or_insert(e);
                }
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// `runWorker` / `processNextWorkItem` (capacity.go:525-560) for the items
    /// currently queued: success is `Forget`, failure is `AddRateLimited`
    /// (`NewTypedItemExponentialFailureRateLimiter(retryIntervalStart,
    /// retryIntervalMax)`, csi-provisioner.go:528).
    pub async fn sync_queued(&self, start: std::time::Duration, max: std::time::Duration) {
        while let Some(key) = self.refresher.pop() {
            match self.sync_item(&key).await {
                Ok(()) => self.refresher.forget(&key),
                Err(e) => {
                    tracing::error!("CSIStorageCapacity publisher ({}): {e}", self.driver_name);
                    let d = self.refresher.add_rate_limited(key, start, max);
                    tracing::warn!("Retrying after {d:?}");
                }
            }
        }
    }

    /// `syncCapacity(ctx, item)` (:564-672) for one queued item.
    async fn sync_item(&self, key: &ItemKey) -> Result<()> {
        // :572-583: "The item was removed in the meantime."
        if !self.refresher.is_capacity(key) {
            return Ok(());
        }
        // :585-593: a missing StorageClass is "another indication that the
        // value is no longer needed".
        let sc: StorageClass = match self
            .storage
            .get(&build_key("storageclasses", None, &key.storage_class_name))
            .await
        {
            Ok(sc) => sc,
            Err(Error::NotFound(_)) => return Ok(()),
            Err(e) => {
                return Err(Error::Internal(format!(
                    "retrieve storage class for {key:?}: {e}"
                )))
            }
        };
        let item = WorkItem {
            segment: &key.segment,
            sc: &sc,
        };
        let existing: Vec<CSIStorageCapacity> = self
            .storage
            .list(&build_prefix(
                "csistoragecapacities",
                Some(&self.owner_namespace),
            ))
            .await?;
        let current = existing
            .into_iter()
            .find(|c| self.is_managed(c) && item.equals(c));
        self.sync_capacity(&item, current).await.map(|_| ())
    }

    /// `syncCapacity` (:586-715).
    async fn sync_capacity(
        &self,
        item: &WorkItem<'_>,
        current: Option<CSIStorageCapacity>,
    ) -> Result<bool> {
        // :599-604 — the assumption is that the capacity is independent of the
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
                self.storage
                    .create(
                        &build_key("csistoragecapacities", Some(&self.owner_namespace), &name),
                        &obj,
                    )
                    .await?;
                return Ok(true);
            }
            Some(c)
                if sizes_are_equal(c.capacity.as_deref(), Some(&quantity))
                    && sizes_are_equal(
                        c.maximum_volume_size.as_deref(),
                        maximum_volume_size.as_deref(),
                    )
                    && (self.owner.is_none() || self.is_owned_by_us(&c)) => {}
            Some(mut c) => {
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
        Ok(false)
    }

    /// The create path of `syncCapacity` (:621-640): a brand-new object. Kept
    /// apart from the update path, which must write a clone of the stored
    /// object (upstream's `capacity.DeepCopy()`, :661) so the PUT carries the
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

    /// `deleteCapacity` (:717-725): NotFound is success.
    async fn delete_capacity(&self, c: &CSIStorageCapacity) -> Result<()> {
        match self
            .storage
            .delete(&build_key(
                "csistoragecapacities",
                Some(&self.owner_namespace),
                &c.metadata.name,
            ))
            .await
        {
            Err(Error::NotFound(_)) => Ok(()),
            r => r,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::csi_capacity_refresh::{CapacityRefresher, ItemKey};
    use super::*;
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
        c.reconcile(&segs).await.unwrap();
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
    async fn queue_deduplicates() {
        let (_s, _c, r) = refresh_fixture(&[&[("layer0", "foo")]]).await;
        r.refresh_sc("direct-sc");
        r.refresh_sc("direct-sc");
        assert_eq!(queued_strings(&r).len(), 1);
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
        // The StorageClass is gone: "Another indication that the value is no longer needed."
        st.delete(&build_key("storageclasses", None, "direct-sc"))
            .await
            .unwrap();
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
        let k = ItemKey {
            segment: seg(&[]),
            storage_class_name: "sc".into(),
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
    }

    impl CapacityClient for FakeClient {
        async fn get_capacity(
            &self,
            req: GetCapacityRequest,
        ) -> std::result::Result<GetCapacityResponse, String> {
            self.requests.lock().unwrap().push(req);
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
        ctl(&st, &cl, false).reconcile(&segs).await.unwrap();

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
        let (s, r) = c
            .reconcile_with_stats(&[seg(&[("zone", "a")]), seg(&[("zone", "b")])])
            .await;
        r.unwrap();
        // Every object is created in this pass, so each item has one now.
        assert_eq!(
            s,
            CapacityStats {
                goal: 2,
                current: 2,
                obsolete: 0
            }
        );

        // Zone b disappears: its object is deleted in this pass, so nothing
        // obsolete remains afterwards.
        let (s, r) = c.reconcile_with_stats(&[seg(&[("zone", "a")])]).await;
        r.unwrap();
        assert_eq!(
            s,
            CapacityStats {
                goal: 1,
                current: 1,
                obsolete: 0
            }
        );
        assert_eq!(list_caps(&st).await.len(), 1);
    }

    #[tokio::test]
    async fn reconcile_publishes_the_gauges_per_driver() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        ctl(&st, &cl, false)
            .reconcile(&[seg(&[("zone", "a")])])
            .await
            .unwrap();
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
        c.reconcile(&segs).await.unwrap();
        let first = list_caps(&st).await.remove(0);

        c.reconcile(&segs).await.unwrap();
        let same = list_caps(&st).await.remove(0);
        assert_eq!(
            first.metadata.resource_version,
            same.metadata.resource_version
        );

        *cl.capacity.lock().unwrap() = 200;
        c.reconcile(&segs).await.unwrap();
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
        c.reconcile(&segs).await.unwrap();
        let before = list_caps(&st).await.remove(0);
        *cl.capacity.lock().unwrap() = 7;
        c.reconcile(&segs).await.unwrap();
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
        c.reconcile(&segs).await.unwrap();

        // capacity.go:643-650: MaximumVolumeSize differs -> update.
        *cl.max.lock().unwrap() = Some(10);
        c.reconcile(&segs).await.unwrap();
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
        c.reconcile(&segs).await.unwrap();
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
        c.reconcile(&both).await.unwrap();
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
        c.reconcile(&both[..1]).await.unwrap();
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
        c.reconcile(&both[..1]).await.unwrap();
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
        ctl(&st, &cl, false).reconcile(&segs).await.unwrap();
        let classes: Vec<_> = list_caps(&st)
            .await
            .into_iter()
            .map(|c| c.storage_class_name)
            .collect();
        assert_eq!(classes, vec!["late".to_string()]);
        ctl(&st, &cl, true).reconcile(&segs).await.unwrap();
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
        let o0 = lookup_owner(&st, NS, "p", "v1", "Pod", 0).await.unwrap();
        assert_eq!((o0.kind.as_str(), o0.uid.as_str()), ("Pod", "pu"));
        let o1 = lookup_owner(&st, NS, "p", "v1", "Pod", 1).await.unwrap();
        assert_eq!((o1.kind.as_str(), o1.uid.as_str()), ("ReplicaSet", "rsu"));
        let o2 = lookup_owner(&st, NS, "p", "v1", "Pod", 2).await.unwrap();
        assert_eq!((o2.kind.as_str(), o2.uid.as_str()), ("Deployment", "du"));
        assert_eq!(o2.controller, Some(true));
        assert!(lookup_owner(&st, NS, "p", "v1", "Pod", 3).await.is_err());
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
        capacity_tick(&c, &TopologySource::Discovered)
            .await
            .unwrap();
        assert!(list_caps(&st).await.is_empty());
        // Fixed topology publishes exactly one object.
        capacity_tick(&c, &TopologySource::Fixed(seg(&[("zone", "a")])))
            .await
            .unwrap();
        assert_eq!(list_caps(&st).await.len(), 1);
    }
}
