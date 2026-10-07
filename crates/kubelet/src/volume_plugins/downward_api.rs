use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{anyhow, Context, Result};
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
        Ok(Box::new(DownwardApiMounter {
            path: self
                .host
                .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name),
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
}

struct DownwardApiMounter {
    path: String,
    volume: Volume,
    pod: Pod,
    node_allocatable: HashMap<String, String>,
}

#[async_trait]
impl Mounter for DownwardApiMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    async fn set_up(&self) -> Result<()> {
        let downward_api = self
            .volume
            .downward_api
            .as_ref()
            .expect("checked by can_support");

        let volume_dir = &self.path;
        std::fs::create_dir_all(volume_dir)
            .context("Failed to create DownwardAPI volume directory")?;

        // Spec defaultMode, or 0644 (the API default; see `collect_data`).
        let da_default_mode = downward_api.default_mode.unwrap_or(0o644);

        // `CollectData` runs BEFORE any write (`SetUpAt`, downwardapi.go),
        // so a bad item leaves the volume untouched.
        let items = downward_api.items.as_deref().unwrap_or(&[]);
        let payload = collect_data(
            items,
            &self.pod,
            &self.node_allocatable,
            da_default_mode as u32,
        )?;

        // `volumeutil.NewAtomicWriter(dir, ctx).Write(data, setPerms)`
        // (downwardapi.go SetUpAt): `..data` symlink swap, no-op when the
        // content is unchanged, so periodic re-SetUp is inert.
        crate::atomic_writer::write_projected_payload(std::path::Path::new(volume_dir), &payload)
            .with_context(|| {
            format!(
                "failed to project DownwardAPI volume {} for pod {}/{}",
                self.volume.name,
                self.pod.metadata.namespace.as_deref().unwrap_or(""),
                self.pod.metadata.name
            )
        })?;

        // Directory permissions: defaultMode plus traverse bits, applied after
        // the payload is written so restrictive modes cannot block the write.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let da_dir_mode = da_default_mode as u32 | 0o111;
            std::fs::set_permissions(volume_dir, std::fs::Permissions::from_mode(da_dir_mode))?;
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
/// Deliberate deviation: upstream errors with "no defaultMode used, not even
/// the default value for it" when `defaultMode` is nil, because the API server
/// has defaulted it. The caller here passes 0644 for that case.
pub(crate) fn collect_data(
    items: &[rusternetes_common::resources::DownwardAPIVolumeFile],
    pod: &Pod,
    node_allocatable: &HashMap<String, String>,
    default_mode: u32,
) -> Result<std::collections::BTreeMap<String, crate::atomic_writer::FileProjection>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::Volume;
    use serde_json::json;

    fn plugin() -> DownwardApiPlugin {
        DownwardApiPlugin::new(std::sync::Arc::new(
            crate::volume_plugins::KubeletVolumeHost::new(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::from([("memory".to_string(), "2Gi".to_string())]),
            ),
        ))
    }

    fn inline_downward_api() -> Volume {
        serde_json::from_value(json!({"name": "podinfo", "downwardAPI": {}})).unwrap()
    }

    fn empty_dir_volume() -> Volume {
        serde_json::from_value(json!({"name": "scratch", "emptyDir": {}})).unwrap()
    }

    #[test]
    fn supports_an_inline_downward_api() {
        let v = inline_downward_api();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_other_kinds() {
        let v = empty_dir_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(!plugin().can_support(&spec));
    }

    #[test]
    fn plugin_name_is_the_upstream_name() {
        assert_eq!(plugin().name(), crate::pod_dirs::plugin::DOWNWARD_API);
    }

    fn test_pod() -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "u1",
                         "labels": {"key1": "value1"}},
            "spec": {"containers": [{"name": "c", "image": "i"}]}
        }))
        .unwrap()
    }

    fn mounter_for(dir: &std::path::Path, items: serde_json::Value) -> DownwardApiMounter {
        DownwardApiMounter {
            path: dir.to_string_lossy().into_owned(),
            volume: serde_json::from_value(
                json!({"name": "podinfo", "downwardAPI": {"items": items}}),
            )
            .unwrap(),
            pod: test_pod(),
            node_allocatable: HashMap::new(),
        }
    }

    // downwardapi_test.go: test_write_with_two_consecutive_slashes_in_the_path
    // (CollectData: `fPath := filepath.Clean(fileInfo.Path)`).
    #[test]
    fn collect_data_cleans_the_path() {
        let items: Vec<rusternetes_common::resources::DownwardAPIVolumeFile> =
            serde_json::from_value(json!([
                {"path": "this//labels", "fieldRef": {"fieldPath": "metadata.labels"}}
            ]))
            .unwrap();
        let data = collect_data(&items, &test_pod(), &HashMap::new(), 0o644).unwrap();
        assert!(data.contains_key("this/labels"), "{:?}", data.keys());
    }

    // CollectData appends every failure to `errlist` and returns
    // `utilerrors.NewAggregate(errlist)` rather than stopping at the first.
    #[test]
    fn collect_data_aggregates_all_errors() {
        let items: Vec<rusternetes_common::resources::DownwardAPIVolumeFile> =
            serde_json::from_value(json!([
                {"path": "a", "fieldRef": {"fieldPath": "metadata.bogusA"}},
                {"path": "b", "fieldRef": {"fieldPath": "metadata.bogusB"}}
            ]))
            .unwrap();
        let err = collect_data(&items, &test_pod(), &HashMap::new(), 0o644)
            .unwrap_err()
            .to_string();
        assert!(err.contains("bogusA") && err.contains("bogusB"), "{err}");
    }

    // An item with neither ref still gets `data[fPath] = fileProjection`
    // (empty data, resolved mode) upstream; it is not an error.
    #[test]
    fn collect_data_item_without_ref_is_an_empty_file() {
        let items: Vec<rusternetes_common::resources::DownwardAPIVolumeFile> =
            serde_json::from_value(json!([{"path": "empty", "mode": 256}])).unwrap();
        let data = collect_data(&items, &test_pod(), &HashMap::new(), 0o644).unwrap();
        assert_eq!(data["empty"].data, Vec::<u8>::new());
        assert_eq!(data["empty"].mode, 0o400);
    }

    // SetUpAt projects through volumeutil.NewAtomicWriter: user-visible
    // entries are symlinks through `..data`, and re-SetUp of unchanged data
    // makes no filesystem change.
    #[tokio::test]
    async fn set_up_uses_the_atomic_writer_and_is_inert_on_resetup() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_for(
            tmp.path(),
            json!([{"path": "name_file", "fieldRef": {"fieldPath": "metadata.name"}}]),
        );
        m.set_up().await.unwrap();
        let link = tmp.path().join("..data");
        assert!(std::fs::symlink_metadata(&link).is_ok(), "no ..data link");
        assert!(std::fs::symlink_metadata(tmp.path().join("name_file"))
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            std::fs::read_to_string(tmp.path().join("name_file")).unwrap(),
            "p"
        );
        let before = std::fs::read_link(&link).unwrap();
        m.set_up().await.unwrap();
        assert_eq!(before, std::fs::read_link(&link).unwrap());
    }

    #[tokio::test]
    async fn set_up_applies_item_mode_over_default() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_for(
            tmp.path(),
            json!([{"path": "f", "mode": 256, "fieldRef": {"fieldPath": "metadata.name"}}]),
        );
        m.set_up().await.unwrap();
        let mode = std::fs::metadata(tmp.path().join("f"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o400);
    }
}
