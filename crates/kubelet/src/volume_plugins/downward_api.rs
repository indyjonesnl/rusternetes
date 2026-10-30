use crate::volume_plugins::{Attributes, Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{Pod, Volume};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Port of `pkg/volume/downwardapi/downwardapi.go`.
pub struct DownwardApiPlugin {
    host: Arc<dyn VolumeHost>,
}

impl DownwardApiPlugin {
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }
}

#[async_trait]
impl VolumePlugin for DownwardApiPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::DOWNWARD_API
    }

    /// `GetVolumeName` (`downwardapi.go:69-77`): the user-defined volume name,
    /// because this is an ephemeral volume type.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        if spec.volume.downward_api.is_none() {
            return Err(anyhow!("Spec does not reference a DownwardAPI volume type"));
        }
        Ok(spec.name().to_string())
    }

    /// `RequiresRemount` (`downwardapi.go:83-85`): `true`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        true
    }

    /// `SupportsSELinuxContextMount` (`downwardapi.go:91-93`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }

    /// `CanSupport` (`downwardapi.go:79-81`), verbatim:
    ///
    /// ```go
    /// return spec.Volume != nil && spec.Volume.DownwardAPI != nil
    /// ```
    ///
    /// downwardAPI has no PersistentVolume form — upstream's `CanSupport` has
    /// no `spec.PersistentVolume` arm either — so only the inline volume is
    /// checked, same as emptyDir/configMap/secret.
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.volume.downward_api.is_some()
    }

    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        let path = self
            .host
            .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name);
        Ok(Box::new(DownwardApiMounter {
            host: self.host.clone(),
            metrics: crate::volume_plugins::metrics_cached::cached_du(&path),
            path,
            // The whole Volume is cloned (not just its `downward_api` source)
            // because the final log line names `volume.name`, and the whole
            // Pod is cloned because a `fieldRef` can name any of its fields —
            // unlike Task 5/6's mounters, which read only namespace and name.
            volume: spec.volume.clone(),
            pod: pod.clone(),
            // `GetNodeAllocatable` (`pkg/volume/plugins.go:397`) — this is the
            // only plugin that calls it, since only a `resourceFieldRef` needs
            // the node's allocatable to default an unset limit.
            node_allocatable: self.host.get_node_allocatable().clone(),
        }))
    }

    /// `NewUnmounter` (`downwardapi.go:110-131`): the wrapper plugins share one unmounter that
    /// delegates `TearDownAt` to emptyDir (`volumeutil.UnmountViaEmptyDir`).
    fn new_unmounter(
        &self,
        vol_name: &str,
        pod_uid: &str,
    ) -> Result<Box<dyn crate::volume_plugins::Unmounter>> {
        Ok(Box::new(
            crate::volume_plugins::util::WrappedEmptyDirUnmounter::new(
                self.host.clone(),
                self.name(),
                vol_name,
                pod_uid,
            ),
        ))
    }

    /// `ConstructVolumeSpec` (`downwardapi.go:110-131`): a bare `DownwardAPIVolumeSource{}`
    /// named after the volume.
    fn construct_volume_spec(
        &self,
        vol_name: &str,
        _mount_path: &str,
    ) -> Result<crate::volume_plugins::ReconstructedVolume> {
        crate::volume_plugins::util::reconstructed_volume(
            vol_name,
            serde_json::json!({"downwardAPI": {}}),
        )
    }
}

pub struct DownwardApiMounter {
    /// `plugin.host`, for the wrapped emptyDir's metadata dir.
    pub host: Arc<dyn VolumeHost>,
    pub path: String,
    pub volume: Volume,
    pub pod: Pod,
    pub node_allocatable: HashMap<String, String>,
    /// Embedded `volume.MetricsProvider` (`downwardapi.go:102`).
    pub metrics: crate::volume_plugins::metrics_cached::CachedMetrics,
}

impl crate::volume_plugins::plugin::MetricsProvider for DownwardApiMounter {
    /// `volume.NewCachedMetrics(volume.NewMetricsDu(getPath(..)))` (`downwardapi.go:102`).
    fn get_metrics(&self) -> anyhow::Result<crate::volume_plugins::plugin::Metrics> {
        self.metrics.get_metrics()
    }
}

#[async_trait]
impl Mounter for DownwardApiMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    /// `downwardAPIVolume.GetAttributes` (`pkg/volume/downwardapi/downwardapi.go:154-160`).
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: true,
            managed: true,
            selinux_relabel: true,
        }
    }

    async fn set_up_at(&self, dir: &str, args: &crate::volume_plugins::MounterArgs) -> Result<()> {
        let downward_api = self
            .volume
            .downward_api
            .as_ref()
            .expect("checked by can_support");

        let volume_dir = dir;

        // `CollectData` runs BEFORE any write (`SetUpAt`, downwardapi.go),
        // so a bad item leaves the volume untouched.
        let items = downward_api.items.as_deref().unwrap_or(&[]);
        let payload = collect_data(
            items,
            &self.pod,
            &self.node_allocatable,
            downward_api.default_mode,
        )?;

        // `volumeutil.NewAtomicWriter(dir, ctx).Write(data, setPerms)`
        // (downwardapi.go SetUpAt): `..data` symlink swap, no-op when the
        // content is unchanged, so periodic re-SetUp is inert.
        //
        // `wrapped.SetUpAt` (downwardapi.go:186): the wrapped emptyDir's
        // `setupDir` creates the root at 0777. Its fsGroup ownership
        // runs in the AtomicWriter's `setPerms` (`:217-222`) below.
        // `wrappedVolumeSpec()` is a `Medium: Memory` emptyDir
        // (downwardapi.go:54-58): a tmpfs on the volume dir, then
        // `SetReady(getMetaDir())` (empty_dir.go:284).
        crate::volume_plugins::util::new_wrapper_mounter(
            &self.host,
            &self.volume.name,
            Some("Memory"),
            &self.pod,
        )
        .await?
        .set_up_at(volume_dir, args)
        .await
        .context("Failed to create DownwardAPI volume directory")?;
        // `volumeutil.MakeNestedMountpoints(b.volName, dir, *b.pod)`
        // (downwardapi.go:191), before the failure-teardown `defer` is armed.
        crate::volume_plugins::util::nested_volumes::make_nested_mountpoints(
            &self.volume.name,
            std::path::Path::new(volume_dir),
            &self.pod,
        )?;

        // `defer` at downwardapi.go:195-208: when the AtomicWriter fails after
        // the wrapped SetUpAt, `unmounter.TearDown()` runs and emptyDir
        // `TearDownAt` removes the volume directory (same as configMap).
        if let Err(e) = crate::volume_ownership::write_payload_with_ownership(
            std::path::Path::new(volume_dir),
            &payload,
            args.fs_group,
            true,
        ) {
            // `defer` teardown: `unmounter.TearDown()` -> `UnmountViaEmptyDir`
            // (ready dir, umount of the tmpfs, volume dir).
            if let Err(td) = crate::volume_plugins::util::unmount_via_empty_dir(
                &self.host,
                volume_dir,
                &self.volume.name,
                &self.pod.metadata.uid,
            )
            .await
            {
                tracing::error!("Error tearing down volume {}: {}", self.volume.name, td);
            }
            return Err(anyhow::Error::new(e).context(format!(
                "failed to project DownwardAPI volume {} for pod {}/{}",
                self.volume.name,
                self.pod.metadata.namespace.as_deref().unwrap_or(""),
                self.pod.metadata.name
            )));
        }

        info!(
            "Created DownwardAPI volume {} at {}",
            self.volume.name, volume_dir
        );
        Ok(())
    }
}

/// Port of `CollectData` (`pkg/volume/downwardapi/downwardapi.go:197-236`).
///
/// Every item failure is appended to an error list and the whole list is
/// returned as one aggregate (`utilerrors.NewAggregate(errlist)`), instead of
/// stopping at the first. The path is `filepath.Clean`ed
/// (`fPath := filepath.Clean(fileInfo.Path)`), the mode is the item's
/// `Mode` else the volume default, and an item with neither `fieldRef` nor
/// `resourceFieldRef` still yields an (empty) entry: upstream never errors
/// there (the API validation layer rejects it before it reaches the kubelet).
///
/// Like upstream, a nil `defaultMode` is an error ("no defaultMode used, not
/// even the default value for it"): the API server
/// (`SetDefaults_DownwardAPIVolumeSource`) and the static-pod decoder
/// (`rusternetes_common::defaults::default_volume_source_modes`) default it.
pub fn collect_data(
    items: &[rusternetes_common::resources::DownwardAPIVolumeFile],
    pod: &Pod,
    node_allocatable: &HashMap<String, String>,
    default_mode: Option<i32>,
) -> Result<std::collections::BTreeMap<String, crate::atomic_writer::FileProjection>> {
    let Some(default_mode) = default_mode else {
        bail!("no defaultMode used, not even the default value for it");
    };
    let default_mode = default_mode as u32;
    let mut errlist: Vec<String> = Vec::new();
    let mut data = std::collections::BTreeMap::new();
    for item in items {
        let mode = item.mode.map(|m| m as u32).unwrap_or(default_mode);
        let mut bytes = Vec::new();
        if let Some(field_ref) = &item.field_ref {
            match crate::downward_api::resolve_pod_field(pod, &field_ref.field_path) {
                Ok(v) => bytes = v.into_bytes(),
                Err(e) => errlist.push(e.to_string()),
            }
        } else if let Some(resource_ref) = &item.resource_field_ref {
            match crate::downward_api::resolve_container_resource(
                pod,
                resource_ref,
                Some(node_allocatable),
            ) {
                Ok(v) => bytes = v.into_bytes(),
                Err(e) => errlist.push(e.to_string()),
            }
        }
        data.insert(
            clean_path(&item.path),
            crate::atomic_writer::FileProjection {
                fs_user: None,
                data: bytes,
                mode,
            },
        );
    }
    if errlist.is_empty() {
        Ok(data)
    } else {
        // `utilerrors.NewAggregate` renders `[e1, e2]` for several errors and
        // the bare message for one.
        Err(anyhow!(if errlist.len() == 1 {
            errlist.remove(0)
        } else {
            format!("[{}]", errlist.join(", "))
        }))
    }
}

/// Lexical `filepath.Clean` for a relative path: drops empty and `.`
/// segments and collapses `..` against preceding segments.
fn clean_path(p: &str) -> String {
    let mut out: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." if out.last().is_some_and(|l| *l != "..") => {
                out.pop();
            }
            s => out.push(s),
        }
    }
    if out.is_empty() {
        ".".to_string()
    } else {
        out.join("/")
    }
}
