//! Port of `pkg/volume/csi/nodeinfomanager/nodeinfomanager.go`: records what a
//! CSI driver's `NodeGetInfo` reported on the `Node` (the
//! `csi.volume.kubernetes.io/nodeid` annotation and the topology labels) and on
//! the `CSINode` object (`spec.drivers[]`: node id, topology keys, allocatable
//! count).
//!
//! Also ported: the CSI-migration annotation (`setMigrationAnnotation`,
//! `migratedPlugins`) and `UpdateCSIDriver`.

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

/// `v1.MigratedPluginsAnnotationKey`
/// (`staging/src/k8s.io/api/core/v1/annotation_key_constants.go:132`).
pub const MIGRATED_PLUGINS_ANNOTATION_KEY: &str = "storage.alpha.kubernetes.io/migrated-plugins";

/// `nodeInfoManager.migratedPlugins` (`nodeinfomanager.go:68`): in-tree plugin
/// name -> "is it served by CSI on this node".
pub type MigratedPlugins = BTreeMap<String, fn() -> bool>;

/// The map `csiPlugin.Init` builds (`csi_plugin.go:325-347`). The in-tree
/// plugins all have a CSI translation in 1.35; `CSIMigrationPortworx` is GA and
/// `LockToDefault: true` (`pkg/features/kube_features.go:1178`), so its closure
/// is constant `true`.
pub fn default_migrated_plugins() -> MigratedPlugins {
    // csitranslationplugins.*PluginName
    // (`staging/src/k8s.io/csi-translation-lib/plugins/`: gce_pd.go:35,
    // aws_ebs.go:37, openstack_cinder.go:35, azure_disk.go:36,
    // azure_file.go:34, vsphere_volume.go:33, portworx.go:30).
    [
        "kubernetes.io/gce-pd",
        "kubernetes.io/aws-ebs",
        "kubernetes.io/cinder",
        "kubernetes.io/azure-disk",
        "kubernetes.io/azure-file",
        "kubernetes.io/vsphere-volume",
        "kubernetes.io/portworx-volume",
    ]
    .into_iter()
    .map(|n| (n.to_string(), (|| true) as fn() -> bool))
    .collect()
}

/// `setMigrationAnnotation` (`nodeinfomanager.go:544-583`): make the CSINode's
/// migrated-plugins annotation list exactly the plugins whose func reports
/// true. Returns whether the annotation changed. A `None` map is a no-op; an
/// emptied list deletes the key but leaves the (possibly empty) map behind.
pub fn set_migration_annotation(
    migrated_plugins: Option<&MigratedPlugins>,
    node_info: &mut CSINode,
) -> bool {
    let Some(migrated_plugins) = migrated_plugins else {
        return false;
    };
    let mut annotations = node_info.metadata.annotations.clone().unwrap_or_default();
    let old: BTreeSet<&str> = match annotations.get(MIGRATED_PLUGINS_ANNOTATION_KEY) {
        Some(v) if !v.is_empty() => v.split(',').collect(),
        _ => BTreeSet::new(),
    };
    let new: BTreeSet<&str> = migrated_plugins
        .iter()
        .filter(|(_, f)| f())
        .map(|(n, _)| n.as_str())
        .collect();
    if old == new {
        return false;
    }
    // `sets.List` is sorted.
    let nas = new.into_iter().collect::<Vec<_>>().join(",");
    if nas.is_empty() {
        annotations.remove(MIGRATED_PLUGINS_ANNOTATION_KEY);
    } else {
        annotations.insert(MIGRATED_PLUGINS_ANNOTATION_KEY.to_string(), nas);
    }
    node_info.metadata.annotations = Some(annotations);
    true
}

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

    /// `UpdateCSIDriver` (`nodeinfomanager.go:138-144`): refresh the CSINode
    /// entry only.
    async fn update_csi_driver(
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
    /// `nim.migratedPlugins`; `None` is upstream's nil map.
    migrated_plugins: Option<MigratedPlugins>,
}

impl<S: Storage> NodeInfoManager<S> {
    /// `NewNodeInfoManager` (`nodeinfomanager.go:98`).
    pub fn new(node_name: impl Into<String>, storage: Arc<S>) -> Self {
        Self {
            node_name: node_name.into(),
            node_uid: std::sync::Mutex::new(String::new()),
            storage,
            lock: tokio::sync::Mutex::new(()),
            migrated_plugins: None,
        }
    }

    /// The `migratedPlugins` argument of `NewNodeInfoManager`.
    pub fn with_migrated_plugins(mut self, migrated_plugins: MigratedPlugins) -> Self {
        self.migrated_plugins = Some(migrated_plugins);
        self
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
    /// (`nodeinfomanager.go:413-467`): read the Node's UID into `nim.nodeID`,
    /// then create the CSINode if missing, or make sure it is owned by this
    /// Node and carries the current migrated-plugins annotation.
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
                Ok(mut info) => {
                    self.ensure_node_owns_csi_node(&info).await?;
                    // `nodeinfomanager.go:457-462`
                    if set_migration_annotation(self.migrated_plugins.as_ref(), &mut info) {
                        self.storage
                            .update(&self.csinode_key(), &info)
                            .await
                            .map(|_| ())
                            .map_err(|e| e.to_string())
                    } else {
                        Ok(())
                    }
                }
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

    pub async fn initialize_csi_node_gating_ready_with(
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

    /// `CreateCSINode` (`nodeinfomanager.go:505-532`): a CSINode named after the node, owned by it. Like upstream
    /// it takes no lock (its callers hold `nim.lock`).
    pub async fn create_csi_node(&self) -> Result<CSINode, String> {
        let mut info = CSINode {
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
        set_migration_annotation(self.migrated_plugins.as_ref(), &mut info);
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
        // `nodeinfomanager.go:624-628`: the annotation is set before deciding
        // whether anything needs writing.
        let annotation_modified =
            set_migration_annotation(self.migrated_plugins.as_ref(), &mut info);
        if !spec_modified && !annotation_modified {
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

    /// `UpdateCSIDriver` (`nodeinfomanager.go:138-144`).
    async fn update_csi_driver(
        &self,
        driver_name: &str,
        driver_node_id: &str,
        max_attach_limit: i64,
        topology: &HashMap<String, String>,
    ) -> Result<(), String> {
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
