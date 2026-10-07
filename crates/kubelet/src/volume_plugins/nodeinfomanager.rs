//! Port of `pkg/volume/csi/nodeinfomanager/nodeinfomanager.go`: records what a
//! CSI driver's `NodeGetInfo` reported on the `Node` (the
//! `csi.volume.kubernetes.io/nodeid` annotation and the topology labels) and on
//! the `CSINode` object (`spec.drivers[]`: node id, topology keys, allocatable
//! count).
//!
//! NOT PORTED (deliberate, tracked in the follow-up issues): the CSI-migration
//! annotation (`setMigrationAnnotation`, `migratedPlugins`) and
//! `UpdateCSIDriver`.

use async_trait::async_trait;
use rusternetes_common::resources::{
    CSINode, CSINodeDriver, CSINodeSpec, Node, VolumeNodeResources,
};
use rusternetes_common::types::{ObjectMeta, OwnerReference, TypeMeta};
use rusternetes_storage::{build_key, Storage};
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// `annotationKeyNodeID` (`nodeinfomanager.go:49`).
pub const ANNOTATION_KEY_NODE_ID: &str = "csi.volume.kubernetes.io/nodeid";

/// `updateBackoff` (`nodeinfomanager.go:54-59`): 4 steps, 10ms, factor 5.
/// (Upstream also adds jitter 0.1; irrelevant to the contract.)
const UPDATE_BACKOFF_STEPS: u32 = 4;
const UPDATE_BACKOFF_DURATION: std::time::Duration = std::time::Duration::from_millis(10);
const UPDATE_BACKOFF_FACTOR: u32 = 5;

/// `initBackoff` (`csi_plugin.go:388-393`): 6 steps, 30ms, factor 8.
const INIT_BACKOFF_STEPS: u32 = 6;
const INIT_BACKOFF_DURATION: std::time::Duration = std::time::Duration::from_millis(30);
const INIT_BACKOFF_FACTOR: u32 = 8;

/// The `Interface` subset the registration handler needs
/// (`nodeinfomanager.go:75-91`): record / remove a driver's node info.
#[async_trait]
pub trait NodeInfoInstaller: Send + Sync {
    /// `InstallCSIDriver`.
    async fn install_csi_driver(
        &self,
        driver_name: &str,
        driver_node_id: &str,
        max_attach_limit: i64,
        topology: &HashMap<String, String>,
    ) -> Result<(), String>;

    /// `UninstallCSIDriver`.
    async fn uninstall_csi_driver(&self, driver_name: &str) -> Result<(), String>;
}

/// `nodeInfoManager` (`nodeinfomanager.go:63-70`).
pub struct NodeInfoManager<S: Storage> {
    node_name: String,
    /// `nim.nodeID`: the UID of this kubelet's Node, set by
    /// [`NodeInfoManager::initialize_csi_node`].
    node_uid: std::sync::Mutex<String>,
    storage: Arc<S>,
    /// `nim.lock`.
    lock: tokio::sync::Mutex<()>,
}

impl<S: Storage> NodeInfoManager<S> {
    /// `NewNodeInfoManager` (`nodeinfomanager.go:98`).
    pub fn new(node_name: impl Into<String>, storage: Arc<S>) -> Self {
        Self {
            node_name: node_name.into(),
            node_uid: std::sync::Mutex::new(String::new()),
            storage,
            lock: tokio::sync::Mutex::new(()),
        }
    }

    fn node_key(&self) -> String {
        build_key("nodes", None, &self.node_name)
    }

    fn csinode_key(&self) -> String {
        build_key("csinodes", None, &self.node_name)
    }

    fn node_uid(&self) -> String {
        self.node_uid.lock().unwrap().clone()
    }

    /// `wait.ExponentialBackoff(updateBackoff, ...)`
    /// (`nodeinfomanager.go:54-59`): up to `Steps` attempts, sleeping
    /// `Duration * Factor^n` between them. `op` returning `Err` means "not done
    /// yet"; every attempt's error is reported when the steps run out.
    async fn exponential_backoff<F, Fut>(&self, what: &str, mut op: F) -> Result<(), String>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let mut errs = Vec::new();
        let mut delay = UPDATE_BACKOFF_DURATION;
        for step in 0..UPDATE_BACKOFF_STEPS {
            match op().await {
                Ok(()) => return Ok(()),
                Err(e) => errs.push(e),
            }
            if step + 1 < UPDATE_BACKOFF_STEPS {
                tokio::time::sleep(delay).await;
                delay *= UPDATE_BACKOFF_FACTOR;
            }
        }
        Err(format!(
            "error updating {what}: timed out waiting for the condition; caused by: [{}]",
            errs.join(", ")
        ))
    }

    /// `InitializeCSINodeWithAnnotation` / `tryInitializeCSINodeWithAnnotation`
    /// (`nodeinfomanager.go:413-467`), minus the CSI-migration annotation: read
    /// the Node's UID into `nim.nodeID`, then create the CSINode if missing or
    /// make sure it is owned by this Node.
    pub async fn initialize_csi_node(&self) -> Result<(), String> {
        self.exponential_backoff("CSINode annotation", || async {
            let _g = self.lock.lock().await;
            let node: Node = self
                .storage
                .get(&self.node_key())
                .await
                .map_err(|e| e.to_string())?;
            *self.node_uid.lock().unwrap() = node.metadata.uid.clone();
            match self.storage.get::<CSINode>(&self.csinode_key()).await {
                Err(rusternetes_common::Error::NotFound(_)) => {
                    self.create_csi_node().await.map(|_| ())
                }
                Err(e) => Err(e.to_string()),
                Ok(info) => self.ensure_node_owns_csi_node(&info).await,
            }
        })
        .await
    }

    /// `initializeCSINode`'s goroutine (`csi_plugin.go:374-415`): keep the
    /// kubelet NotReady (`SetKubeletError`) until the CSINode exists and is
    /// owned by the Node. First waits forever for the API server
    /// (`waitForAPIServerForever`, `csi_plugin.go:972-1000`), then retries
    /// `InitializeCSINodeWithAnnotation` over `initBackoff` (6 steps, 30ms,
    /// factor 8 -- ~140s). `Err` after the steps run out is upstream's
    /// `klog.Fatalf` (the kubelet restarts to retry).
    pub async fn initialize_csi_node_gating_ready(
        &self,
        set_kubelet_error: impl Fn(Option<String>),
    ) -> Result<(), String> {
        self.initialize_csi_node_gating_ready_with(
            set_kubelet_error,
            INIT_BACKOFF_STEPS,
            INIT_BACKOFF_DURATION,
        )
        .await
    }

    async fn initialize_csi_node_gating_ready_with(
        &self,
        set_kubelet_error: impl Fn(Option<String>),
        steps: u32,
        duration: std::time::Duration,
    ) -> Result<(), String> {
        // `kvh.SetKubeletError(errors.New("CSINode is not yet initialized"))`
        // (csi_plugin.go:374).
        set_kubelet_error(Some("CSINode is not yet initialized".into()));
        // `wait.PollImmediateInfinite(time.Second, ...)`: any answer but a
        // transport/permission error (success or NotFound) proves the API
        // server is reachable.
        loop {
            match self.storage.get::<CSINode>(&self.csinode_key()).await {
                Ok(_) | Err(rusternetes_common::Error::NotFound(_)) => break,
                Err(e) => {
                    tracing::debug!(
                        "Failed to contact API server when waiting for CSINode publishing: {e}"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
        let mut delay = duration;
        let mut last = String::new();
        for step in 0..steps {
            match self.initialize_csi_node().await {
                Ok(()) => {
                    // Allow the kubelet to post Ready (csi_plugin.go:404).
                    set_kubelet_error(None);
                    return Ok(());
                }
                Err(e) => {
                    set_kubelet_error(Some(format!("failed to initialize CSINode: {e}")));
                    tracing::error!("Failed to initialize CSINode: {e}");
                    last = e;
                }
            }
            if step + 1 < steps {
                tokio::time::sleep(delay).await;
                delay *= INIT_BACKOFF_FACTOR;
            }
        }
        Err(format!(
            "Failed to initialize CSINode after retrying: {last}"
        ))
    }

    /// `CreateCSINode` (`nodeinfomanager.go:505-532`), minus the migration
    /// annotation: a CSINode named after the node, owned by it. Like upstream
    /// it takes no lock (its callers hold `nim.lock`).
    pub async fn create_csi_node(&self) -> Result<CSINode, String> {
        let info = CSINode {
            type_meta: TypeMeta {
                kind: "CSINode".into(),
                api_version: "storage.k8s.io/v1".into(),
            },
            metadata: ObjectMeta {
                name: self.node_name.clone(),
                owner_references: Some(vec![OwnerReference {
                    // `nodeKind.Version`: `v1.SchemeGroupVersion` = "v1".
                    api_version: "v1".into(),
                    kind: "Node".into(),
                    name: self.node_name.clone(),
                    uid: self.node_uid(),
                    block_owner_deletion: None,
                    controller: None,
                }]),
                ..Default::default()
            },
            spec: CSINodeSpec { drivers: vec![] },
        };
        self.storage
            .create(&self.csinode_key(), &info)
            .await
            .map_err(|e| e.to_string())
    }

    /// `nodeOwnsCSINode` (`nodeinfomanager.go:484-503`).
    fn node_owns_csi_node(&self, info: &CSINode) -> (bool, String) {
        let node_uid = self.node_uid();
        let mut owner_id = String::new();
        let mut found = false;
        for r in info.metadata.owner_references.iter().flatten() {
            if r.kind != "Node" {
                continue;
            }
            owner_id = r.uid.clone();
            if r.name != self.node_name {
                continue;
            }
            if owner_id == node_uid {
                found = true;
                break;
            }
        }
        (found, owner_id)
    }

    /// `ensureNodeOwnsCSINode` (`nodeinfomanager.go:469-482`): a CSINode left
    /// by a previous incarnation of the node is deleted and an error returned
    /// so the next attempt creates a fresh one.
    async fn ensure_node_owns_csi_node(&self, info: &CSINode) -> Result<(), String> {
        let (ok, owner_id) = self.node_owns_csi_node(info);
        if ok {
            return Ok(());
        }
        let new_id = self.node_uid();
        tracing::info!(
            "existing CSINode {:?} is owned by different node (oldNodeID={owner_id:?}, newNodeID={new_id:?}), cleaning up...",
            info.metadata.name
        );
        self.storage
            .delete(&self.csinode_key())
            .await
            .map_err(|e| {
                format!(
                    "error deleting existing CSINode {:?}: {e}",
                    info.metadata.name
                )
            })?;
        Err(format!(
            "CSINode {:?} was owned by different node (oldNodeID={owner_id:?}, newNodeID={new_id:?}), deleted it",
            info.metadata.name
        ))
    }

    /// `updateNode` / `tryUpdateNode` (`nodeinfomanager.go:166-224`): re-read
    /// the Node on every attempt so existing changes are not overwritten, apply
    /// the update functions, and write back only what changed. Upstream sends
    /// one `PatchNodeStatus`; here metadata goes through `update` (resource
    /// version guarded, so a concurrent writer surfaces as a conflict and the
    /// backoff re-reads) and `.status` through `update_status`, the way this
    /// kubelet writes node status elsewhere.
    async fn update_node(
        &self,
        apply: impl Fn(&mut Node) -> Result<(), String>,
    ) -> Result<(), String> {
        self.exponential_backoff("node", || async {
            let _g = self.lock.lock().await;
            let original: Node = self
                .storage
                .get(&self.node_key())
                .await
                .map_err(|e| e.to_string())?;
            let mut node = original.clone();
            apply(&mut node)?;
            let meta = |n: &Node| serde_json::json!([n.metadata.labels, n.metadata.annotations]);
            let status = |n: &Node| serde_json::to_value(&n.status).unwrap_or_default();
            let meta_changed = meta(&original) != meta(&node);
            let status_changed = status(&original) != status(&node);
            if meta_changed {
                let updated: Node = self
                    .storage
                    .update(&self.node_key(), &node)
                    .await
                    .map_err(|e| e.to_string())?;
                node.metadata.resource_version = updated.metadata.resource_version;
            }
            if status_changed {
                self.storage
                    .update_status(&self.node_key(), &node)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Ok(())
        })
        .await
    }

    /// `updateCSINode` / `tryUpdateCSINode` (`nodeinfomanager.go:363-411`).
    async fn update_csi_node(
        &self,
        driver_name: &str,
        driver_node_id: &str,
        max_attach_limit: i64,
        topology: &HashMap<String, String>,
    ) -> Result<(), String> {
        self.exponential_backoff("CSINode", || async {
            let _g = self.lock.lock().await;
            let info = match self.storage.get::<CSINode>(&self.csinode_key()).await {
                Ok(i) => i,
                Err(rusternetes_common::Error::NotFound(_)) => self.create_csi_node().await?,
                Err(e) => return Err(e.to_string()),
            };
            self.ensure_node_owns_csi_node(&info).await?;
            self.install_driver_to_csi_node(
                info,
                driver_name,
                driver_node_id,
                max_attach_limit,
                topology,
            )
            .await
        })
        .await
    }

    /// `installDriverToCSINode` (`nodeinfomanager.go:594-653`).
    async fn install_driver_to_csi_node(
        &self,
        mut info: CSINode,
        driver_name: &str,
        driver_node_id: &str,
        mut max_attach_limit: i64,
        topology: &HashMap<String, String>,
    ) -> Result<(), String> {
        let topology_keys: BTreeSet<String> = topology.keys().cloned().collect();
        let mut spec_modified = true;
        // Clone the driver list, omitting the driver that matches `driver_name`.
        let mut new_specs: Vec<CSINodeDriver> = Vec::new();
        for d in &info.spec.drivers {
            if d.name == driver_name {
                let existing_keys: BTreeSet<String> =
                    d.topology_keys.iter().flatten().cloned().collect();
                if d.node_id == driver_node_id
                    && existing_keys == topology_keys
                    && keep_allocatable_count(d, max_attach_limit)
                {
                    spec_modified = false;
                }
            } else {
                new_specs.push(d.clone());
            }
        }
        if !spec_modified {
            return Ok(());
        }

        let mut driver_spec = CSINodeDriver {
            name: driver_name.to_string(),
            node_id: driver_node_id.to_string(),
            // `sets.List` is sorted; a nil set serializes as no keys.
            topology_keys: if topology_keys.is_empty() {
                None
            } else {
                Some(topology_keys.into_iter().collect())
            },
            allocatable: None,
        };
        if max_attach_limit > 0 {
            if max_attach_limit > i32::MAX as i64 {
                tracing::warn!(
                    "Exceeded max supported attach limit value, truncating it to {}",
                    i32::MAX
                );
                max_attach_limit = i32::MAX as i64;
            }
            driver_spec.allocatable = Some(VolumeNodeResources {
                count: Some(max_attach_limit as i32),
            });
        } else if max_attach_limit != 0 {
            tracing::error!(
                "Invalid attach limit value {max_attach_limit} cannot be added to CSINode object for {driver_name:?}"
            );
        }
        new_specs.push(driver_spec);
        info.spec.drivers = new_specs;
        self.storage
            .update(&self.csinode_key(), &info)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    /// `uninstallDriverFromCSINode` / `tryUninstallDriverFromCSINode`
    /// (`nodeinfomanager.go:655-715`): NotFound is not an error; the object is
    /// only written when the driver was present.
    async fn uninstall_driver_from_csi_node(&self, driver_name: &str) -> Result<(), String> {
        self.exponential_backoff("CSINode", || async {
            let _g = self.lock.lock().await;
            let mut info = match self.storage.get::<CSINode>(&self.csinode_key()).await {
                Ok(i) => i,
                Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
                Err(e) => return Err(e.to_string()),
            };
            let before = info.spec.drivers.len();
            info.spec.drivers.retain(|d| d.name != driver_name);
            if info.spec.drivers.len() == before {
                return Ok(());
            }
            self.storage
                .update(&self.csinode_key(), &info)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
        .await
    }
}

/// `keepAllocatableCount` (`nodeinfomanager.go:586-592`): true iff the new
/// limit does not require a CSINode update.
fn keep_allocatable_count(d: &CSINodeDriver, max_attach_limit: i64) -> bool {
    let count = d.allocatable.as_ref().and_then(|a| a.count);
    if max_attach_limit == 0 {
        return count.is_none();
    }
    count.is_some_and(|c| c as i64 == max_attach_limit)
}

/// `buildNodeIDMapFromAnnotation` (`nodeinfomanager.go:226-251`).
fn build_node_id_map_from_annotation(node: &Node) -> Result<BTreeMap<String, String>, String> {
    let prev = node
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANNOTATION_KEY_NODE_ID))
        .cloned()
        .unwrap_or_default();
    if prev.is_empty() {
        return Ok(BTreeMap::new());
    }
    serde_json::from_str::<Option<BTreeMap<String, String>>>(&prev)
        .map(|m| m.unwrap_or_default())
        .map_err(|e| {
            format!(
                "failed to parse node's {ANNOTATION_KEY_NODE_ID:?} annotation value ({prev:?}) err={e}"
            )
        })
}

/// `updateNodeIDInNode` (`nodeinfomanager.go:253-289`).
fn update_node_id_in_node(
    node: &mut Node,
    driver_name: &str,
    driver_node_id: &str,
) -> Result<(), String> {
    let mut map = build_node_id_map_from_annotation(node)?;
    if map.get(driver_name).is_some_and(|v| v == driver_node_id) {
        return Ok(());
    }
    map.insert(driver_name.to_string(), driver_node_id.to_string());
    let json = serde_json::to_string(&map).map_err(|e| {
        format!("error while marshalling node ID map updated with driverName={driver_name:?}, nodeID={driver_node_id:?}: {e}")
    })?;
    node.metadata
        .annotations
        .get_or_insert_with(HashMap::new)
        .insert(ANNOTATION_KEY_NODE_ID.to_string(), json);
    Ok(())
}

/// `removeNodeIDFromNode` (`nodeinfomanager.go:291-339`).
fn remove_node_id_from_node(node: &mut Node, driver_name: &str) -> Result<(), String> {
    let has = node
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANNOTATION_KEY_NODE_ID))
        .is_some_and(|v| !v.is_empty());
    if !has {
        return Ok(());
    }
    let mut map = build_node_id_map_from_annotation(node)?;
    if map.remove(driver_name).is_none() {
        return Ok(());
    }
    let annotations = node.metadata.annotations.get_or_insert_with(HashMap::new);
    if map.is_empty() {
        annotations.remove(ANNOTATION_KEY_NODE_ID);
    } else {
        annotations.insert(
            ANNOTATION_KEY_NODE_ID.to_string(),
            serde_json::to_string(&map).map_err(|e| e.to_string())?,
        );
    }
    Ok(())
}

/// `updateTopologyLabels` (`nodeinfomanager.go:341-361`): a label already
/// present with a different value is a collision and fails the install.
fn update_topology_labels(
    node: &mut Node,
    topology: &HashMap<String, String>,
) -> Result<(), String> {
    if topology.is_empty() {
        return Ok(());
    }
    for (k, v) in topology {
        if let Some(cur) = node.metadata.labels.as_ref().and_then(|l| l.get(k)) {
            if cur != v {
                return Err(format!(
                    "detected topology value collision: driver reported {k:?}:{v:?} but existing label is {k:?}:{cur:?}"
                ));
            }
        }
    }
    let labels = node.metadata.labels.get_or_insert_with(HashMap::new);
    for (k, v) in topology {
        labels.insert(k.clone(), v.clone());
    }
    Ok(())
}

/// `removeMaxAttachLimit` (`nodeinfomanager.go:717-747`).
fn remove_max_attach_limit(node: &mut Node, driver_name: &str) {
    let key = csi_attach_limit_key(driver_name);
    if let Some(status) = node.status.as_mut() {
        for map in [&mut status.capacity, &mut status.allocatable] {
            if let Some(m) = map {
                m.remove(&key);
                if m.is_empty() {
                    *map = None;
                }
            }
        }
    }
}

/// `CSIAttachLimitPrefix` / `ResourceNameLengthLimit`
/// (`pkg/volume/util/attach_limit.go:27-32`).
const CSI_ATTACH_LIMIT_PREFIX: &str = "attachable-volumes-csi-";
const RESOURCE_NAME_LENGTH_LIMIT: usize = 63;

/// `GetCSIAttachLimitKey` (`pkg/volume/util/attach_limit.go:36-48`).
pub fn csi_attach_limit_key(driver_name: &str) -> String {
    if CSI_ATTACH_LIMIT_PREFIX.len() + driver_name.len() >= RESOURCE_NAME_LENGTH_LIMIT {
        let chars_from_driver_name = &driver_name[..23];
        let digest = Sha1::digest(driver_name.as_bytes());
        let hashed: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        return format!(
            "{CSI_ATTACH_LIMIT_PREFIX}{chars_from_driver_name}{}",
            &hashed[..16]
        );
    }
    format!("{CSI_ATTACH_LIMIT_PREFIX}{driver_name}")
}

#[async_trait]
impl<S: Storage + 'static> NodeInfoInstaller for NodeInfoManager<S> {
    /// `InstallCSIDriver` (`nodeinfomanager.go:113-136`).
    async fn install_csi_driver(
        &self,
        driver_name: &str,
        driver_node_id: &str,
        max_attach_limit: i64,
        topology: &HashMap<String, String>,
    ) -> Result<(), String> {
        if driver_node_id.is_empty() {
            return Err(
                "error adding CSI driver node info: driverNodeID must not be empty".to_string(),
            );
        }
        self.update_node(|node| {
            remove_max_attach_limit(node, driver_name);
            update_node_id_in_node(node, driver_name, driver_node_id)?;
            update_topology_labels(node, topology)
        })
        .await
        .map_err(|e| format!("error updating Node object with CSI driver node info: {e}"))?;

        self.update_csi_node(driver_name, driver_node_id, max_attach_limit, topology)
            .await
            .map_err(|e| format!("error updating CSINode object with CSI driver node info: {e}"))
    }

    /// `UninstallCSIDriver` (`nodeinfomanager.go:150-164`).
    async fn uninstall_csi_driver(&self, driver_name: &str) -> Result<(), String> {
        self.uninstall_driver_from_csi_node(driver_name)
            .await
            .map_err(|e| format!("error uninstalling CSI driver from CSINode object {e}"))?;
        self.update_node(|node| {
            remove_max_attach_limit(node, driver_name);
            remove_node_id_from_node(node, driver_name)
        })
        .await
        .map_err(|e| format!("error removing CSI driver node info from Node object {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::MemoryStorage;

    const DRIVER1: &str = "com.example.csi.driver1";

    type Annotation = Vec<(&'static str, &'static str)>;

    fn node(node_ids: Annotation, labels: Vec<(&str, &str)>, capacity: Vec<(&str, &str)>) -> Node {
        let mut n = Node::new("node1");
        if !node_ids.is_empty() {
            let m: std::collections::BTreeMap<_, _> = node_ids.into_iter().collect();
            n.metadata.annotations = Some(HashMap::from([(
                ANNOTATION_KEY_NODE_ID.to_string(),
                serde_json::to_string(&m).unwrap(),
            )]));
        }
        if !labels.is_empty() {
            n.metadata.labels = Some(
                labels
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
        }
        if !capacity.is_empty() {
            let c: HashMap<String, String> = capacity
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            n.status = Some(rusternetes_common::resources::NodeStatus {
                capacity: Some(c.clone()),
                allocatable: Some(c),
                ..Default::default()
            });
        }
        n
    }

    fn csi_node(drivers: Vec<(&str, &str, Option<i32>, Vec<&str>)>, owner_uid: &str) -> CSINode {
        CSINode {
            type_meta: TypeMeta {
                kind: "CSINode".into(),
                api_version: "storage.k8s.io/v1".into(),
            },
            metadata: ObjectMeta {
                name: "node1".into(),
                owner_references: Some(vec![OwnerReference {
                    api_version: "v1".into(),
                    kind: "Node".into(),
                    name: "node1".into(),
                    uid: owner_uid.into(),
                    block_owner_deletion: None,
                    controller: None,
                }]),
                ..Default::default()
            },
            spec: CSINodeSpec {
                drivers: drivers
                    .into_iter()
                    .map(|(name, id, count, keys)| CSINodeDriver {
                        name: name.into(),
                        node_id: id.into(),
                        topology_keys: if keys.is_empty() {
                            None
                        } else {
                            Some(keys.into_iter().map(String::from).collect())
                        },
                        allocatable: count.map(|c| VolumeNodeResources { count: Some(c) }),
                    })
                    .collect(),
            },
        }
    }

    async fn setup(
        n: Node,
        c: Option<CSINode>,
    ) -> (Arc<MemoryStorage>, NodeInfoManager<MemoryStorage>) {
        let st = Arc::new(MemoryStorage::new());
        st.create(&build_key("nodes", None, "node1"), &n)
            .await
            .unwrap();
        if let Some(c) = c {
            st.create(&build_key("csinodes", None, "node1"), &c)
                .await
                .unwrap();
        }
        let nim = NodeInfoManager::new("node1", st.clone());
        (st, nim)
    }

    type ErrLog = Arc<std::sync::Mutex<Vec<Option<String>>>>;

    fn recorder() -> (ErrLog, impl Fn(Option<String>)) {
        let log: ErrLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        let l = log.clone();
        (log, move |e| l.lock().unwrap().push(e))
    }

    /// csi_plugin.go:374,404: NotReady first, cleared only once the CSINode
    /// is installed and owned by the Node.
    #[tokio::test]
    async fn gate_sets_error_then_clears_once_csinode_initialized() {
        let (st, nim) = setup(node(vec![], vec![], vec![]), None).await;
        let (log, set) = recorder();
        nim.initialize_csi_node_gating_ready(set).await.unwrap();
        assert_eq!(
            *log.lock().unwrap(),
            vec![Some("CSINode is not yet initialized".to_string()), None]
        );
        assert!(got(&st).await.1.is_some());
    }

    /// csi_plugin.go:398,413: failures keep the kubelet NotReady with the
    /// cause and give up (upstream `klog.Fatalf`) after the backoff.
    #[tokio::test]
    async fn gate_stays_not_ready_and_errors_after_backoff() {
        let st = Arc::new(MemoryStorage::new()); // no Node: init always fails
        let nim = NodeInfoManager::new("node1", st);
        let (log, set) = recorder();
        let err = nim
            .initialize_csi_node_gating_ready_with(set, 2, std::time::Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(
            err.starts_with("Failed to initialize CSINode after retrying"),
            "{err}"
        );
        let log = log.lock().unwrap();
        assert_eq!(log[0].as_deref(), Some("CSINode is not yet initialized"));
        assert!(log[1]
            .as_ref()
            .unwrap()
            .starts_with("failed to initialize CSINode: "));
        assert!(log.last().unwrap().is_some(), "never cleared");
    }

    async fn got(st: &MemoryStorage) -> (Node, Option<CSINode>) {
        let n: Node = st.get(&build_key("nodes", None, "node1")).await.unwrap();
        let c: Option<CSINode> = st.get(&build_key("csinodes", None, "node1")).await.ok();
        (n, c)
    }

    fn nodeid_map(n: &Node) -> std::collections::BTreeMap<String, String> {
        n.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(ANNOTATION_KEY_NODE_ID))
            .map(|s| serde_json::from_str(s).unwrap())
            .unwrap_or_default()
    }

    fn topo(kv: &[(&str, &str)]) -> HashMap<String, String> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn no_nodeid_annotation(n: &Node) -> bool {
        n.metadata
            .annotations
            .as_ref()
            .is_none_or(|a| !a.contains_key(ANNOTATION_KEY_NODE_ID))
    }

    /// `TestInstallCSIDriver` "empty node": annotation, topology label, and a
    /// created CSINode owned by the node.
    #[tokio::test]
    async fn install_on_empty_node_creates_csinode_and_annotates_node() {
        let (st, nim) = setup(node(vec![], vec![], vec![]), None).await;
        nim.install_csi_driver(
            DRIVER1,
            "com.example.csi/csi-node1",
            0,
            &topo(&[("com.example.csi/zone", "zoneA")]),
        )
        .await
        .unwrap();
        let (n, c) = got(&st).await;
        assert_eq!(nodeid_map(&n)[DRIVER1], "com.example.csi/csi-node1");
        assert_eq!(n.metadata.labels.unwrap()["com.example.csi/zone"], "zoneA");
        let c = c.expect("CSINode created");
        let o = &c.metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(
            (o.kind.as_str(), o.name.as_str(), o.api_version.as_str()),
            ("Node", "node1", "v1")
        );
        assert_eq!(c.spec.drivers.len(), 1);
        let d = &c.spec.drivers[0];
        assert_eq!(d.name, DRIVER1);
        assert_eq!(d.node_id, "com.example.csi/csi-node1");
        assert_eq!(
            d.topology_keys,
            Some(vec!["com.example.csi/zone".to_string()])
        );
        assert!(d.allocatable.is_none());
    }

    /// "pre-existing node info from different driver": the other driver's
    /// annotation entry, label and CSINode driver entry all survive; the new
    /// driver is appended.
    #[tokio::test]
    async fn install_keeps_other_drivers() {
        let (st, nim) = setup(
            node(
                vec![(
                    "net.example.storage.other-driver",
                    "net.example.storage/test-node",
                )],
                vec![("net.example.storage/rack", "rack1")],
                vec![],
            ),
            Some(csi_node(
                vec![(
                    "net.example.storage.other-driver",
                    "net.example.storage/test-node",
                    None,
                    vec!["net.example.storage/rack"],
                )],
                "",
            )),
        )
        .await;
        nim.install_csi_driver(
            DRIVER1,
            "com.example.csi/csi-node1",
            0,
            &topo(&[("com.example.csi/zone", "zoneA")]),
        )
        .await
        .unwrap();
        let (n, c) = got(&st).await;
        let ids = nodeid_map(&n);
        assert_eq!(ids.len(), 2);
        assert_eq!(
            ids["net.example.storage.other-driver"],
            "net.example.storage/test-node"
        );
        let labels = n.metadata.labels.unwrap();
        assert_eq!(labels["net.example.storage/rack"], "rack1");
        assert_eq!(labels["com.example.csi/zone"], "zoneA");
        let names: Vec<_> = c
            .unwrap()
            .spec
            .drivers
            .iter()
            .map(|d| d.name.clone())
            .collect();
        assert_eq!(names, vec!["net.example.storage.other-driver", DRIVER1]);
    }

    /// "...different node ID and topology values; labels should conflict".
    #[tokio::test]
    async fn install_fails_on_topology_label_collision() {
        let (_st, nim) = setup(
            node(
                vec![(DRIVER1, "com.example.csi/csi-node1")],
                vec![("com.example.csi/zone", "zoneA")],
                vec![],
            ),
            Some(csi_node(
                vec![(
                    DRIVER1,
                    "com.example.csi/csi-node1",
                    None,
                    vec!["com.example.csi/zone"],
                )],
                "",
            )),
        )
        .await;
        let err = nim
            .install_csi_driver(
                DRIVER1,
                "com.example.csi/csi-node1",
                0,
                &topo(&[("com.example.csi/zone", "other-zone")]),
            )
            .await
            .unwrap_err();
        assert!(err.contains("topology value collision"), "{err}");
    }

    /// "...different node ID and topology keys; new labels should be added":
    /// the driver's entry is replaced, old labels kept.
    #[tokio::test]
    async fn install_replaces_same_driver_entry_and_adds_labels() {
        let (st, nim) = setup(
            node(
                vec![(DRIVER1, "com.example.csi/csi-node1")],
                vec![("com.example.csi/zone", "zoneA")],
                vec![],
            ),
            Some(csi_node(
                vec![(
                    DRIVER1,
                    "com.example.csi/csi-node1",
                    None,
                    vec!["com.example.csi/zone"],
                )],
                "",
            )),
        )
        .await;
        nim.install_csi_driver(
            DRIVER1,
            "com.example.csi/other-node",
            0,
            &topo(&[("com.example.csi/rack", "rack1")]),
        )
        .await
        .unwrap();
        let (n, c) = got(&st).await;
        assert_eq!(nodeid_map(&n)[DRIVER1], "com.example.csi/other-node");
        let labels = n.metadata.labels.unwrap();
        assert_eq!(labels["com.example.csi/zone"], "zoneA");
        assert_eq!(labels["com.example.csi/rack"], "rack1");
        let c = c.unwrap();
        assert_eq!(c.spec.drivers.len(), 1);
        assert_eq!(c.spec.drivers[0].node_id, "com.example.csi/other-node");
        assert_eq!(
            c.spec.drivers[0].topology_keys,
            Some(vec!["com.example.csi/rack".to_string()])
        );
    }

    /// "empty node ID".
    #[tokio::test]
    async fn install_with_empty_node_id_fails() {
        let (_st, nim) = setup(node(vec![], vec![], vec![]), None).await;
        let err = nim
            .install_csi_driver(DRIVER1, "", 0, &HashMap::new())
            .await
            .unwrap_err();
        assert!(err.contains("driverNodeID must not be empty"), "{err}");
    }

    /// "pre-existing node info with driver, but owned by previous node": the
    /// stale CSINode (another node's UID) is deleted and recreated, so only the
    /// new driver remains.
    #[tokio::test]
    async fn install_replaces_a_csinode_owned_by_a_previous_node() {
        let mut n = node(vec![], vec![], vec![]);
        n.metadata.uid = "node1-uid".into();
        let (st, nim) = setup(
            n,
            Some(csi_node(
                vec![(
                    "com.example.csi.old-driver",
                    "com.example.csi/csi-node2",
                    None,
                    vec![],
                )],
                "node2-uid",
            )),
        )
        .await;
        nim.initialize_csi_node().await.unwrap();
        nim.install_csi_driver(DRIVER1, "com.example.csi/csi-node1", 0, &HashMap::new())
            .await
            .unwrap();
        let (_n, c) = got(&st).await;
        let c = c.unwrap();
        assert_eq!(
            c.spec.drivers.len(),
            1,
            "only the new driver: the old CSINode represented a previous node"
        );
        assert_eq!(c.spec.drivers[0].name, DRIVER1);
        assert_eq!(c.metadata.owner_references.unwrap()[0].uid, "node1-uid");
    }

    /// "new node with valid/max/overflown max limit of volumes" and "without".
    #[tokio::test]
    async fn install_maps_max_volumes_to_allocatable_count() {
        for (limit, want) in [
            (10_i64, Some(10_i32)),
            (i32::MAX as i64, Some(i32::MAX)),
            (i32::MAX as i64 + 1, Some(i32::MAX)),
            (0, None),
        ] {
            let (st, nim) = setup(node(vec![], vec![], vec![]), None).await;
            nim.install_csi_driver(DRIVER1, "com.example.csi/csi-node1", limit, &HashMap::new())
                .await
                .unwrap();
            let (_n, c) = got(&st).await;
            let a = c.unwrap().spec.drivers[0].allocatable.clone();
            assert_eq!(a.and_then(|a| a.count), want, "limit {limit}");
        }
    }

    /// "node with existing valid max limit of volumes": a changed limit updates
    /// the CSINode driver entry; unrelated Node capacity is untouched.
    #[tokio::test]
    async fn install_updates_a_changed_limit() {
        let (st, nim) = setup(
            node(vec![], vec![], vec![("cpu", "4m")]),
            Some(csi_node(
                vec![(DRIVER1, "com.example.csi/csi-node1", Some(10), vec![])],
                "",
            )),
        )
        .await;
        nim.install_csi_driver(DRIVER1, "com.example.csi/csi-node1", 20, &HashMap::new())
            .await
            .unwrap();
        let (n, c) = got(&st).await;
        assert_eq!(
            c.unwrap().spec.drivers[0]
                .allocatable
                .as_ref()
                .unwrap()
                .count,
            Some(20)
        );
        assert_eq!(n.status.unwrap().capacity.unwrap()["cpu"], "4m");
    }

    /// `removeMaxAttachLimit` (kept upstream for version skew): installing
    /// drops a legacy `attachable-volumes-csi-<driver>` capacity entry.
    #[tokio::test]
    async fn install_removes_the_legacy_attach_limit_capacity() {
        let key = "attachable-volumes-csi-com.example.csi.driver1";
        let (st, nim) = setup(node(vec![], vec![], vec![("cpu", "4m"), (key, "10")]), None).await;
        nim.install_csi_driver(DRIVER1, "com.example.csi/csi-node1", 0, &HashMap::new())
            .await
            .unwrap();
        let (n, _c) = got(&st).await;
        let status = n.status.unwrap();
        assert!(!status.capacity.as_ref().unwrap().contains_key(key));
        assert!(!status.allocatable.as_ref().unwrap().contains_key(key));
        assert_eq!(status.capacity.unwrap()["cpu"], "4m");
    }

    /// A conflicting write (here injected) is retried, as `updateNode` /
    /// `updateCSINode` do under `wait.ExponentialBackoff`.
    #[tokio::test]
    async fn install_retries_update_conflicts() {
        let (st, nim) = setup(node(vec![], vec![], vec![]), None).await;
        st.inject_conflicts(2);
        nim.install_csi_driver(DRIVER1, "com.example.csi/csi-node1", 0, &HashMap::new())
            .await
            .unwrap();
        let (n, c) = got(&st).await;
        assert_eq!(nodeid_map(&n)[DRIVER1], "com.example.csi/csi-node1");
        assert_eq!(c.unwrap().spec.drivers.len(), 1);
    }

    /// `TestInstallCSIDriverExistingAnnotation`: a CSINode created up front and
    /// a Node already carrying (other) annotation entries.
    #[tokio::test]
    async fn install_after_create_csinode_with_existing_annotation() {
        let (st, nim) = setup(
            node(
                vec![("net.example.storage/other-driver", "x")],
                vec![],
                vec![],
            ),
            None,
        )
        .await;
        nim.create_csi_node().await.unwrap();
        nim.install_csi_driver(
            "com.example.csi/driver1",
            "com.example.csi/some-node",
            0,
            &HashMap::new(),
        )
        .await
        .unwrap();
        let (_n, c) = got(&st).await;
        let c = c.unwrap();
        let d = &c.spec.drivers[0];
        assert_eq!(
            (d.name.as_str(), d.node_id.as_str()),
            ("com.example.csi/driver1", "com.example.csi/some-node")
        );
    }

    /// `TestUninstallCSIDriver` "pre-existing node info from the same driver":
    /// annotation removed (and with it the annotation key), the CSINode driver
    /// removed, topology labels left alone.
    #[tokio::test]
    async fn uninstall_removes_driver_but_keeps_labels() {
        let (st, nim) = setup(
            node(
                vec![(DRIVER1, "com.example.csi/csi-node1")],
                vec![("com.example.csi/zone", "zoneA")],
                vec![],
            ),
            Some(csi_node(
                vec![(
                    DRIVER1,
                    "com.example.csi/csi-node1",
                    None,
                    vec!["com.example.csi/zone"],
                )],
                "",
            )),
        )
        .await;
        nim.uninstall_csi_driver(DRIVER1).await.unwrap();
        let (n, c) = got(&st).await;
        assert!(no_nodeid_annotation(&n));
        assert_eq!(n.metadata.labels.unwrap()["com.example.csi/zone"], "zoneA");
        assert!(c.unwrap().spec.drivers.is_empty());
    }

    /// "pre-existing node info from different driver": nothing changes.
    #[tokio::test]
    async fn uninstall_leaves_other_drivers() {
        let (st, nim) = setup(
            node(
                vec![(
                    "net.example.storage.other-driver",
                    "net.example.storage/csi-node1",
                )],
                vec![],
                vec![],
            ),
            Some(csi_node(
                vec![(
                    "net.example.storage.other-driver",
                    "net.example.storage/csi-node1",
                    None,
                    vec![],
                )],
                "",
            )),
        )
        .await;
        nim.uninstall_csi_driver(DRIVER1).await.unwrap();
        let (n, c) = got(&st).await;
        assert_eq!(
            nodeid_map(&n)["net.example.storage.other-driver"],
            "net.example.storage/csi-node1"
        );
        assert_eq!(c.unwrap().spec.drivers.len(), 1);
    }

    /// "pre-existing info about the same driver in node, but empty CSINode",
    /// and no CSINode at all (NotFound is not an error).
    #[tokio::test]
    async fn uninstall_cleans_the_annotation_without_a_csinode_entry() {
        let (st, nim) = setup(
            node(vec![(DRIVER1, "com.example.csi/csi-node1")], vec![], vec![]),
            None,
        )
        .await;
        nim.uninstall_csi_driver(DRIVER1).await.unwrap();
        let (n, c) = got(&st).await;
        assert!(no_nodeid_annotation(&n));
        assert!(c.is_none());
    }

    /// "new node with valid max limit" (uninstall): an unrelated capacity entry
    /// stays; the driver's legacy attach-limit key is dropped.
    #[tokio::test]
    async fn uninstall_removes_the_legacy_attach_limit_capacity() {
        let key = "attachable-volumes-csi-com.example.csi.driver1";
        let (st, nim) = setup(node(vec![], vec![], vec![("cpu", "4m"), (key, "10")]), None).await;
        nim.uninstall_csi_driver(DRIVER1).await.unwrap();
        let (n, _c) = got(&st).await;
        let capacity = n.status.unwrap().capacity.unwrap();
        assert!(!capacity.contains_key(key));
        assert_eq!(capacity["cpu"], "4m");
    }

    /// `GetCSIAttachLimitKey` (`pkg/volume/util/attach_limit.go:36-48`): a name
    /// that would reach 63 chars is truncated to 23 chars plus a 16-hex sha1.
    #[test]
    fn attach_limit_key_truncates_long_driver_names() {
        assert_eq!(csi_attach_limit_key("a.b"), "attachable-volumes-csi-a.b");
        let long = "very-long-csi-driver-name.storage.example.com.xyz";
        let k = csi_attach_limit_key(long);
        assert_eq!(k.len(), "attachable-volumes-csi-".len() + 23 + 16, "{k}");
        assert!(k.starts_with("attachable-volumes-csi-very-long-csi-driver-"));
    }
}
