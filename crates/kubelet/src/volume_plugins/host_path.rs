use crate::runtime::{check_host_path_type, HostPathCheck};
use crate::volume_plugins::{Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::Result;
use async_trait::async_trait;
use rusternetes_common::resources::volume::HostPathType;
use rusternetes_common::resources::Pod;
use std::sync::Arc;
use tracing::info;

/// Port of `pkg/volume/hostpath/host_path.go`.
pub struct HostPathPlugin {}

impl HostPathPlugin {
    /// Takes the host uniformly with every other plugin (Task 11's registry
    /// constructs plugins generically), but hostPath's path comes straight
    /// from the volume source, not from the pod directory layout the host
    /// provides — so nothing here reads it.
    pub fn new(_host: Arc<dyn VolumeHost>) -> Self {
        Self {}
    }
}

/// `HostPathType` (`common/src/resources/volume.rs:103-111`) has no
/// `#[serde(rename_all)]`, so its variant names already are the upstream
/// wire strings `check_host_path_type` matches on
/// (`v1.HostPathDirectoryOrCreate` = `"DirectoryOrCreate"`, etc. —
/// `staging/src/k8s.io/api/core/v1/types.go:797-825`). An explicit match is
/// used rather than a `serde_json::to_value` round trip: it is exhaustive
/// (a new variant fails to compile here instead of silently serialising to
/// something unexpected) and has no fallible/serialisation path to paper
/// over with `unwrap_or_default`.
fn host_path_type_as_str(t: &HostPathType) -> &'static str {
    match t {
        HostPathType::DirectoryOrCreate => "DirectoryOrCreate",
        HostPathType::Directory => "Directory",
        HostPathType::FileOrCreate => "FileOrCreate",
        HostPathType::File => "File",
        HostPathType::Socket => "Socket",
        HostPathType::CharDevice => "CharDevice",
        HostPathType::BlockDevice => "BlockDevice",
    }
}

#[async_trait]
impl VolumePlugin for HostPathPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::HOST_PATH
    }

    /// `CanSupport` (`host_path.go:98-101`), both arms verbatim:
    ///
    /// ```go
    /// return (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.HostPath != nil) ||
    ///     (spec.Volume != nil && spec.Volume.HostPath != nil)
    /// ```
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.persistent_volume
            .map(|pv| pv.spec.host_path.is_some())
            .unwrap_or(false)
            || spec.volume.host_path.is_some()
    }

    /// `NewMounter` (`host_path.go:121-145`) — `getVolumeSource`
    /// (`host_path.go:362-371`) reads the inline arm first and falls back to
    /// the PV arm; the two are never both set in practice (a PVC-backed
    /// `Volume` carries `persistentVolumeClaim`, not `hostPath`), so the
    /// order only matters for fidelity to upstream, not for behaviour here.
    async fn new_mounter(&self, spec: &Spec<'_>, _pod: &Pod) -> Result<Box<dyn Mounter>> {
        let (path, path_type) = if let Some(hp) = spec.volume.host_path.as_ref() {
            // Pre-move behaviour expanded only the inline arm's path
            // (`991a503d:volumes.rs:961`, `expand_env_vars(&host_path.path)`).
            // The PV arm was always a plain clone (`:1361`) and so was the
            // ephemeral arm (`:1531`) — deliberately asymmetric, kept here.
            // `expand_env_vars` (`crate::runtime`) resolves an unset name to
            // the empty string via `unwrap_or_default()`, so expanding the PV
            // arm too would silently turn a `spec.hostPath.path` typo into a
            // different, existing directory with no error.
            (crate::runtime::expand_env_vars(&hp.path), hp.type_.clone())
        } else if let Some(hp) = spec
            .persistent_volume
            .and_then(|pv| pv.spec.host_path.as_ref())
        {
            (
                hp.path.clone(),
                hp.r#type
                    .as_ref()
                    .map(host_path_type_as_str)
                    .map(String::from),
            )
        } else {
            return Err(anyhow::anyhow!(
                "hostPath plugin got a spec with no hostPath"
            ));
        };
        Ok(Box::new(HostPathMounter {
            path,
            path_type,
            volume_name: spec.volume.name.clone(),
        }))
    }
}

struct HostPathMounter {
    path: String,
    path_type: Option<String>,
    volume_name: String,
}

#[async_trait]
impl Mounter for HostPathMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    /// `hostPathMounter.SetUp` (`host_path.go:241-255`) — the type check runs
    /// for both arms, which is why a PV-backed hostPath is checked here where
    /// `create_volume`'s PVC branch did not check it. Sanctioned delta, see
    /// the plan.
    async fn set_up(&self) -> Result<()> {
        // ---- moved verbatim from create_volume's hostPath branch (d401f4ea) ----
        let path = &self.path;
        let host_path_type = self.path_type.as_deref();
        match check_host_path_type(path, host_path_type) {
            HostPathCheck::Ok => {}
            HostPathCheck::Missing => {
                return Err(anyhow::anyhow!(
                    "hostPath {} does not exist (type={:?})",
                    path,
                    host_path_type
                ));
            }
            HostPathCheck::WrongKind => {
                return Err(anyhow::anyhow!(
                    "hostPath {} exists but does not match type={:?}",
                    path,
                    host_path_type
                ));
            }
            HostPathCheck::UnsupportedType => {
                return Err(anyhow::anyhow!(
                    "hostPath {} declared unknown type {:?}",
                    path,
                    host_path_type
                ));
            }
        }
        info!("Using hostPath volume {} at {}", self.volume_name, path);
        // ---- end moved body ----
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{PersistentVolume, Volume};
    use serde_json::json;

    fn plugin() -> HostPathPlugin {
        HostPathPlugin::new(std::sync::Arc::new(
            crate::volume_plugins::KubeletVolumeHost::new(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    fn inline_host_path(path: &str) -> Volume {
        serde_json::from_value(json!({"name": "hp", "hostPath": {"path": path}})).unwrap()
    }

    fn claimed_volume() -> Volume {
        serde_json::from_value(
            json!({"name": "claimed", "persistentVolumeClaim": {"claimName": "c"}}),
        )
        .unwrap()
    }

    fn pv_host_path(path: &str) -> PersistentVolume {
        serde_json::from_value(json!({
            "metadata": {"name": "pv-1"},
            "spec": {"hostPath": {"path": path}}
        }))
        .unwrap()
    }

    fn test_pod() -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": []}
        }))
        .unwrap()
    }

    /// Upstream's CanSupport checks BOTH arms — a hostPath PV and an inline
    /// hostPath volume are the same plugin's work
    /// (`pkg/volume/hostpath/host_path.go:98-101`).
    #[test]
    fn supports_an_inline_host_path() {
        let v = inline_host_path("/tmp/x");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn supports_a_host_path_persistent_volume() {
        let v = claimed_volume();
        let pv = pv_host_path("/mnt/data");
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_a_volume_with_neither_arm() {
        let v: Volume = serde_json::from_value(json!({"name": "scratch", "emptyDir": {}})).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        assert!(!plugin().can_support(&spec));
    }

    /// The path comes from the source, not from the pod directory.
    #[tokio::test]
    async fn get_path_is_the_host_path_for_a_pv() {
        let v = claimed_volume();
        let pv = pv_host_path("/mnt/data");
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
        };
        let pod = test_pod();
        let m = plugin().new_mounter(&spec, &pod).await.unwrap();
        assert_eq!(m.get_path(), "/mnt/data");
    }

    /// Pre-move, only the inline hostPath arm was environment-expanded
    /// (`991a503d:volumes.rs:961`); the PV arm was a plain clone
    /// (`:1361`). An unset var expands to "" via `expand_env_vars`'s
    /// `unwrap_or_default()`, so this also proves the split does not
    /// silently turn a typo'd env-var reference into a different,
    /// existing directory.
    #[tokio::test]
    async fn only_the_inline_arm_expands_environment_variables() {
        let v = inline_host_path("/data/$RUSTERNETES_HOSTPATH_TEST_UNSET_VAR");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
        };
        let pod = test_pod();
        let m = plugin().new_mounter(&spec, &pod).await.unwrap();
        assert_eq!(m.get_path(), "/data/");
    }

    /// The PV arm's counterpart to the test above: same unset var, but
    /// sourced from `pv.spec.host_path` instead of the inline volume, must
    /// come through unexpanded.
    #[tokio::test]
    async fn the_pv_arm_does_not_expand_environment_variables() {
        let v = claimed_volume();
        let pv = pv_host_path("/data/$RUSTERNETES_HOSTPATH_TEST_UNSET_VAR");
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
        };
        let pod = test_pod();
        let m = plugin().new_mounter(&spec, &pod).await.unwrap();
        assert_eq!(m.get_path(), "/data/$RUSTERNETES_HOSTPATH_TEST_UNSET_VAR");
    }
}
