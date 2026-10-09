use crate::volume_plugins::{
    Attributes, Mounter, ReconstructedVolume, Spec, Unmounter, VolumeHost, VolumePlugin,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusternetes_common::resources::Pod;
use std::sync::Arc;

/// Port of `pkg/volume/local/local.go` (the `localVolumePlugin`), Filesystem
/// mode with a directory source only. Block-device local PVs (global
/// bind-mount staging, `generateBlockDeviceBaseGlobalPath`) are not ported.
///
/// **Deviation:** upstream's `SetUpAt` bind-mounts `globalPath` onto the pod
/// volume dir; like [`super::host_path::HostPathPlugin`] we hand the container
/// runtime the host directory itself, so there is no mount to count refs of
/// (`local.go:552-577`, `:619`).
pub struct LocalPlugin {}

impl LocalPlugin {
    pub fn new(_host: Arc<dyn VolumeHost>) -> Self {
        Self {}
    }
}

/// `getVolumeSource` (`local.go:171-179`): only the PV arm exists.
fn local_path<'a>(spec: &'a Spec<'_>) -> Option<&'a str> {
    spec.persistent_volume
        .and_then(|pv| pv.spec.local.as_ref())
        .map(|l| l.path.as_str())
}

#[async_trait]
impl VolumePlugin for LocalPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::LOCAL
    }

    /// `GetVolumeName` (`local.go:116-124`): the PV name.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        spec.persistent_volume
            .and_then(|pv| pv.spec.local.as_ref().map(|_| pv.metadata.name.clone()))
            .ok_or_else(|| anyhow!("spec does not reference a Local volume type"))
    }

    /// `CanSupport` (`local.go:126-128`):
    /// `spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Local != nil`.
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        local_path(spec).is_some()
    }

    /// `RequiresRemount` (`local.go:130-132`): `false`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// `SupportsSELinuxContextMount` (`local.go:134-136`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }

    /// `NewMounter` (`local.go:138-169`).
    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        let path = local_path(spec)
            .ok_or_else(|| anyhow!("local plugin got a spec with no local source"))?;
        Ok(Box::new(LocalMounter {
            path: path.to_string(),
            read_only: spec.read_only,
            fs_group: crate::volume_plugins::util::fs_group_from(pod),
            fs_group_change_policy: pod
                .spec
                .as_ref()
                .and_then(|s| s.security_context.as_ref())
                .and_then(|sc| sc.fs_group_change_policy.clone()),
        }))
    }

    fn new_unmounter(&self, _vol_name: &str, _pod_uid: &str) -> Result<Box<dyn Unmounter>> {
        Ok(Box::new(LocalUnmounter))
    }

    /// `ConstructVolumeSpec` (`local.go:196-`): a PV named after the volume
    /// with an empty-path Filesystem local source.
    fn construct_volume_spec(
        &self,
        vol_name: &str,
        _mount_path: &str,
    ) -> Result<ReconstructedVolume> {
        let pv = serde_json::from_value(serde_json::json!({
            "metadata": {"name": vol_name},
            "spec": {"local": {"path": ""}, "volumeMode": "Filesystem"}
        }))?;
        let mut rv =
            crate::volume_plugins::util::reconstructed_volume(vol_name, serde_json::json!({}))?;
        rv.persistent_volume = Some(pv);
        Ok(rv)
    }
}

struct LocalMounter {
    path: String,
    read_only: bool,
    fs_group: Option<i64>,
    fs_group_change_policy: Option<String>,
}

#[async_trait]
impl Mounter for LocalMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    /// `localVolumeMounter.GetAttributes` (`local.go:482-488`).
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: self.read_only,
            managed: !self.read_only,
            selinux_relabel: true,
        }
    }

    /// `SetUpAt` (`local.go:529-626`).
    async fn set_up_at(&self, dir: &str, _args: &crate::volume_plugins::MounterArgs) -> Result<()> {
        if dir.is_empty() {
            return Err(anyhow!("LocalVolume volume path is empty"));
        }
        if dir.split('/').any(|i| i == "..") {
            return Err(anyhow!("invalid path: {} must not contain '..'", dir));
        }
        // `local.go:616-624`: "Volume owner will be written only once on the
        // first volume mount" -- `NewVolumeOwnership(..).ChangePermissions()`
        // with `mounterArgs.FsGroup` / `FSGroupChangePolicy`, skipped when
        // read-only.
        if !self.read_only {
            let (root, fs_group, policy) = (
                dir.to_string(),
                self.fs_group,
                self.fs_group_change_policy.clone(),
            );
            tokio::task::spawn_blocking(move || {
                crate::volume_ownership::set_volume_ownership_with_policy(
                    std::path::Path::new(&root),
                    fs_group,
                    policy.as_deref(),
                    false,
                )
            })
            .await
            .map_err(|e| anyhow!("fsGroup ownership task panicked: {e}"))??;
        }
        Ok(())
    }
}

struct LocalUnmounter;

#[async_trait]
impl Unmounter for LocalUnmounter {
    fn get_path(&self) -> String {
        String::new()
    }
    async fn tear_down_at(&self, _dir: &str) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{PersistentVolume, Volume};
    use serde_json::json;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    fn plugin() -> LocalPlugin {
        LocalPlugin::new(Arc::new(crate::volume_plugins::KubeletVolumeHost::new(
            "/var/lib/rusternetes".to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            std::collections::HashMap::new(),
        )))
    }

    fn pv_local(path: &str) -> PersistentVolume {
        serde_json::from_value(json!({
            "metadata": {"name": "pv-local"},
            "spec": {"local": {"path": path}}
        }))
        .unwrap()
    }

    fn claimed() -> Volume {
        serde_json::from_value(json!({"name": "c", "persistentVolumeClaim": {"claimName": "c"}}))
            .unwrap()
    }

    fn pod(gid: i64, policy: Option<&str>) -> Pod {
        let mut sc = json!({"fsGroup": gid});
        if let Some(p) = policy {
            sc["fsGroupChangePolicy"] = json!(p);
        }
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "u"},
            "spec": {"containers": [], "securityContext": sc}
        }))
        .unwrap()
    }

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("local-plugin-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn mode(p: &std::path::Path) -> u32 {
        std::fs::metadata(p).unwrap().permissions().mode() & 0o7777
    }

    /// `CanSupport` (`local.go:126-128`): PV arm only.
    #[test]
    fn supports_only_a_local_pv() {
        let v = claimed();
        let pv = pv_local("/mnt/disks/a");
        let s = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: false,
        };
        assert!(plugin().can_support(&s));
        let s = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
        };
        assert!(!plugin().can_support(&s));
    }

    /// `SetUpAt` (`local.go:620-624`): a read-write local volume gets
    /// `SetVolumeOwnership` with the pod's fsGroup.
    #[tokio::test]
    async fn set_up_applies_fs_group() {
        let d = tmp("fsgroup");
        let f = d.join("f");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o400)).unwrap();
        let gid = std::fs::metadata(&d).unwrap().gid() as i64;
        let v = claimed();
        let pv = pv_local(d.to_str().unwrap());
        let s = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: false,
        };
        let m = plugin().new_mounter(&s, &pod(gid, None)).await.unwrap();
        m.set_up().await.unwrap();
        assert_eq!(mode(&f), 0o660);
        assert_eq!(mode(&d) & 0o2000, 0o2000);
    }

    /// `local.go:616`: `if !m.readOnly` guards the ownership change.
    #[tokio::test]
    async fn read_only_volume_is_not_chowned() {
        let d = tmp("ro");
        let f = d.join("f");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o400)).unwrap();
        let gid = std::fs::metadata(&d).unwrap().gid() as i64;
        let v = claimed();
        let pv = pv_local(d.to_str().unwrap());
        let s = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: true,
        };
        let m = plugin().new_mounter(&s, &pod(gid, None)).await.unwrap();
        m.set_up().await.unwrap();
        assert_eq!(mode(&f), 0o400);
    }

    /// `mounterArgs.FSGroupChangePolicy` is threaded through
    /// (`local.go:621`): OnRootMismatch with a matching root must not walk,
    /// while Always must.
    #[tokio::test]
    async fn fs_group_change_policy_is_honoured() {
        let d = tmp("orm");
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o2770)).unwrap();
        let f = d.join("f");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
        let gid = std::fs::metadata(&d).unwrap().gid() as i64;
        let v = claimed();
        let pv = pv_local(d.to_str().unwrap());
        let s = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: false,
        };
        let m = plugin()
            .new_mounter(&s, &pod(gid, Some("OnRootMismatch")))
            .await
            .unwrap();
        m.set_up().await.unwrap();
        assert_eq!(mode(&f), 0o600, "OnRootMismatch + matching root: no walk");
        let m = plugin()
            .new_mounter(&s, &pod(gid, Some("Always")))
            .await
            .unwrap();
        m.set_up().await.unwrap();
        assert_eq!(mode(&f), 0o660, "Always walks");
    }
}
