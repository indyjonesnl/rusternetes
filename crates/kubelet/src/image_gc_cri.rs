//! CRI-backed implementations of the [`crate::image_gc`] traits.
//!
//! * [`CriImageGcRuntime`] is `kubeGenericRuntimeManager`'s `ListImages`
//!   (`pkg/kubelet/kuberuntime/kuberuntime_image.go:102`), `RemoveImage`
//!   (`:138`), `GetPods(all=true)` (`kuberuntime_manager.go:438`) and
//!   `GetContainerStatus` over the CRI `ImageService` / `RuntimeService`.
//! * [`CriStatsProvider`] is `criStatsProvider.ImageFsStats`
//!   (`pkg/kubelet/stats/cri_stats_provider.go:503`): CRI `ImageFsInfo` for the
//!   image filesystem's mountpoint, then a `statfs` on it for capacity and
//!   availability (upstream asks cAdvisor's `GetDirFsInfo`).
//! * [`NodeEvents`] is `im.recorder` + `im.nodeRef` (`kubelet.go:897`): events
//!   against the Node object.

use std::collections::HashMap;

use async_trait::async_trait;
use rusternetes_common::resources::{EventSource, EventType, ObjectReference};
use rusternetes_cri::{v1, CriClient};
use rusternetes_storage::{EventRecorder, StorageBackend};

use crate::image_gc::{
    FsStats, GcContainer, GcPod, Image, ImageGcRuntime, ImageSpec, NodeEventSink, StatsProvider,
};
use crate::labels::{POD_NAMESPACE_LABEL, POD_NAME_LABEL, POD_UID_LABEL};

/// `ImageGcRuntime` over a CRI client.
pub struct CriImageGcRuntime {
    cri: CriClient,
}

impl CriImageGcRuntime {
    pub fn new(cri: CriClient) -> Self {
        Self { cri }
    }
}

/// `toKubeContainer` (`kuberuntime/helpers.go:101-123`): a container with no id
/// or no image is not convertible and is skipped by `GetPods`.
pub fn to_gc_container(c: &v1::Container) -> Option<GcContainer> {
    let image = c.image.as_ref()?;
    if c.id.is_empty() {
        return None;
    }
    // Keep backwards compatibility to older runtimes, c.ImageId has been added in v1.30.
    let image_id = if c.image_id.is_empty() {
        c.image_ref.clone()
    } else {
        c.image_id.clone()
    };
    Some(GcContainer {
        id: c.id.clone(),
        name: c
            .metadata
            .as_ref()
            .map(|m| m.name.clone())
            .unwrap_or_default(),
        image: image.image.clone(),
        image_id,
        image_ref: c.image_ref.clone(),
        image_runtime_handler: image.runtime_handler.clone(),
    })
}

#[async_trait]
impl ImageGcRuntime for CriImageGcRuntime {
    async fn list_images(&self) -> anyhow::Result<Vec<Image>> {
        let mut cri = self.cri.clone();
        Ok(cri
            .list_images()
            .await?
            .into_iter()
            .map(|img| Image {
                id: img.id,
                size: img.size as i64,
                runtime_handler: img.spec.map(|s| s.runtime_handler).unwrap_or_default(),
                pinned: img.pinned,
                repo_tags: img.repo_tags,
                repo_digests: img.repo_digests,
            })
            .collect())
    }

    /// `GetPods(ctx, all=true)`: containers in every state, grouped by the pod
    /// UID label.
    async fn get_pods(&self) -> anyhow::Result<Vec<GcPod>> {
        let mut cri = self.cri.clone();
        let containers = cri.list_containers(None).await?;
        let mut pods: HashMap<String, GcPod> = HashMap::new();
        let mut order: Vec<String> = Vec::new();
        for c in &containers {
            if c.metadata.is_none() {
                continue;
            }
            let Some(converted) = to_gc_container(c) else {
                continue;
            };
            let uid = c.labels.get(POD_UID_LABEL).cloned().unwrap_or_default();
            let pod = pods.entry(uid.clone()).or_insert_with(|| {
                order.push(uid.clone());
                GcPod {
                    id: uid.clone(),
                    name: c.labels.get(POD_NAME_LABEL).cloned().unwrap_or_default(),
                    namespace: c
                        .labels
                        .get(POD_NAMESPACE_LABEL)
                        .cloned()
                        .unwrap_or_default(),
                    containers: Vec::new(),
                }
            });
            pod.containers.push(converted);
        }
        Ok(order
            .into_iter()
            .filter_map(|uid| pods.remove(&uid))
            .collect())
    }

    async fn container_image_mounts(
        &self,
        _pod_id: &str,
        container_id: &str,
    ) -> anyhow::Result<Vec<String>> {
        let mut cri = self.cri.clone();
        let resp = cri.container_status(container_id, false).await?;
        Ok(resp
            .status
            .map(|s| {
                s.mounts
                    .into_iter()
                    .filter_map(|m| m.image.map(|i| i.image))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// `kubeGenericRuntimeManager.RemoveImage` sends only `ImageSpec.Image`
    /// (`kuberuntime_image.go:140`), not the runtime handler.
    async fn remove_image(&self, image: &ImageSpec) -> anyhow::Result<()> {
        let mut cri = self.cri.clone();
        cri.remove_image(&image.image).await?;
        Ok(())
    }
}

/// `StatsProvider` over CRI `ImageFsInfo` + `statfs`.
pub struct CriStatsProvider {
    cri: CriClient,
}

impl CriStatsProvider {
    pub fn new(cri: CriClient) -> Self {
        Self { cri }
    }
}

#[async_trait]
impl StatsProvider for CriStatsProvider {
    async fn image_fs_stats(&self) -> anyhow::Result<FsStats> {
        let mut cri = self.cri.clone();
        let resp = cri.image_fs_info().await?;

        // CRI may return the stats of multiple image filesystems but we only
        // return the first one.
        let Some(fs) = resp.image_filesystems.first() else {
            anyhow::bail!("imageFs information is unavailable");
        };
        // `getFsInfo`: a nil fsID, or a filesystem unknown to this node, yields
        // no capacity/availability (best-effort partial result).
        let mountpoint = fs.fs_id.as_ref().map(|id| id.mountpoint.as_str());
        let Some(mountpoint) = mountpoint else {
            tracing::debug!("Failed to get filesystem info: fsID is nil");
            return Ok(FsStats::default());
        };
        match crate::volume_plugins::metrics_statfs::fs_info(mountpoint) {
            Ok((available, capacity, _, inodes, inodes_free, _)) => Ok(FsStats {
                capacity_bytes: Some(capacity as u64),
                available_bytes: Some(available as u64),
                inodes: Some(inodes as u64),
                inodes_free: Some(inodes_free as u64),
                device: std::fs::metadata(mountpoint).ok().map(|m| {
                    use std::os::unix::fs::MetadataExt;
                    m.dev()
                }),
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!(mountpoint, %err, "Failed to get the info of the filesystem with mountpoint");
                Ok(FsStats::default())
            }
            Err(err) => Err(anyhow::anyhow!(
                "get filesystem info: Failed to get the info of the filesystem with mountpoint: {err}"
            )),
        }
    }
}

/// `record.EventRecorder` scoped to the node (`nodeRef`, `kubelet.go:880-886`:
/// `Kind: Node, Name/UID: nodeName, Namespace: ""`).
pub struct NodeEvents {
    recorder: EventRecorder<StorageBackend>,
    node_name: String,
}

impl NodeEvents {
    pub fn new(recorder: EventRecorder<StorageBackend>, node_name: &str) -> Self {
        Self {
            recorder,
            node_name: node_name.to_string(),
        }
    }

    fn node_ref(&self) -> ObjectReference {
        ObjectReference {
            kind: Some("Node".to_string()),
            namespace: Some(String::new()),
            name: Some(self.node_name.clone()),
            uid: Some(self.node_name.clone()),
            api_version: None,
            resource_version: None,
            field_path: None,
        }
    }
}

impl NodeEventSink for NodeEvents {
    fn warning(&self, reason: &str, message: &str) {
        let recorder = self.recorder.clone();
        let involved = self.node_ref();
        let source = EventSource {
            component: crate::events::KUBELET_COMPONENT.to_string(),
            host: Some(self.node_name.clone()),
        };
        let (reason, message) = (reason.to_string(), message.to_string());
        tokio::spawn(async move {
            let _ = recorder
                .event(&involved, &source, EventType::Warning, &reason, &message)
                .await;
        });
    }
}
