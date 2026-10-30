use crate::runtime::check_host_path_type_msg;
use crate::volume_plugins::{Attributes, Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{anyhow, Result};
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
fn host_path_type_as_str(t: &HostPathType) -> &str {
    match t {
        HostPathType::DirectoryOrCreate => "DirectoryOrCreate",
        HostPathType::Directory => "Directory",
        HostPathType::FileOrCreate => "FileOrCreate",
        HostPathType::File => "File",
        HostPathType::Socket => "Socket",
        HostPathType::CharDevice => "CharDevice",
        HostPathType::BlockDevice => "BlockDevice",
        HostPathType::Unknown(v) => v,
    }
}

#[async_trait]
impl VolumePlugin for HostPathPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::HOST_PATH
    }

    /// `GetVolumeName` (`host_path.go:89-96`): the host path itself, via
    /// `getVolumeSource` (`host_path.go:362-371`), which prefers the inline
    /// source and falls back to the PV's.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        if let Some(source) = &spec.volume.host_path {
            return Ok(source.path.clone());
        }
        if let Some(source) = spec
            .persistent_volume
            .and_then(|pv| pv.spec.host_path.as_ref())
        {
            return Ok(source.path.clone());
        }
        Err(anyhow!("spec does not reference an HostPath volume type"))
    }

    /// `RequiresRemount` (`host_path.go:103-105`): `false`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// `SupportsSELinuxContextMount` (`host_path.go:111-113`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
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
            // The path is used verbatim, as upstream does
            // (`host_path.go:139`: `&hostPath{path: path, ...}`
            // from `getVolumeSource`; no `os.ExpandEnv` anywhere in
            // `pkg/volume/` or `pkg/kubelet/`). The only upstream expansion
            // is `subPathExpr`, in `makeMounts`
            // (`pkg/kubelet/kubelet_pods.go:309-310`), with `$(VAR)`
            // syntax against the container env. The earlier kubelet-env
            // `$VAR` expansion defaulted unset names to "" (#1984).
            (hp.path.clone(), hp.type_.clone())
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
            // `getVolumeSource` returns `spec.ReadOnly` for both arms
            // (`host_path.go:364`, `:367`).
            read_only: spec.read_only,
        }))
    }

    /// `NewUnmounter` (`host_path.go:147-151`): `&hostPathUnmounter{&hostPath{path: ""}}`.
    fn new_unmounter(
        &self,
        _vol_name: &str,
        _pod_uid: &str,
    ) -> Result<Box<dyn crate::volume_plugins::Unmounter>> {
        Ok(Box::new(HostPathUnmounter))
    }

    /// `ConstructVolumeSpec` (`host_path.go:184-196`): a hostPath volume named
    /// after, and pointing at, `volumeName`.
    fn construct_volume_spec(
        &self,
        vol_name: &str,
        _mount_path: &str,
    ) -> Result<crate::volume_plugins::ReconstructedVolume> {
        crate::volume_plugins::util::reconstructed_volume(
            vol_name,
            serde_json::json!({"hostPath": {"path": vol_name}}),
        )
    }
}

/// Port of `ValidatePathNoBacksteps`
/// (`pkg/volume/validation/pv_validation.go:62-71`): rejects any `/`-separated
/// element equal to `..`.
pub fn validate_path_no_backsteps(target_path: &str) -> std::result::Result<(), &'static str> {
    if target_path.split('/').any(|item| item == "..") {
        return Err("must not contain '..'");
    }
    Ok(())
}

struct HostPathMounter {
    path: String,
    path_type: Option<String>,
    volume_name: String,
    read_only: bool,
}

#[async_trait]
impl Mounter for HostPathMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    /// `hostPathMounter.GetAttributes` (`host_path.go:232-238`):
    /// `{ReadOnly: b.readOnly, Managed: false, SELinuxRelabel: false}`.
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: self.read_only,
            managed: false,
            selinux_relabel: false,
        }
    }

    /// `hostPathMounter.SetUp` (`host_path.go:241-255`) — the type check runs
    /// for both arms, which is why a PV-backed hostPath is checked here where
    /// `create_volume`'s PVC branch did not check it. Sanctioned delta, see
    /// the plan.
    async fn set_up_with(&self, _args: &crate::volume_plugins::MounterArgs) -> Result<()> {
        // `validation.ValidatePathNoBacksteps(b.GetPath())` runs BEFORE the
        // type check (`host_path.go:242-245`), so a path with a backstep is
        // rejected with this error, never a type error. #1973.
        if let Err(e) = validate_path_no_backsteps(&self.path) {
            return Err(anyhow!("invalid HostPath `{}`: {}", self.path, e));
        }
        // ---- moved verbatim from create_volume's hostPath branch
        //      (991a503d:crates/kubelet/src/volumes.rs:959-988) ----
        let path = &self.path;
        let host_path_type = self.path_type.as_deref();
        // `checkType` (`host_path.go:251-253`): upstream's exact error text.
        check_host_path_type_msg(path, host_path_type).map_err(|(_, msg)| anyhow!(msg))?;
        info!("Using hostPath volume {} at {}", self.volume_name, path);
        // ---- end moved body ----
        Ok(())
    }

    /// `hostPathMounter.SetUpAt` (`host_path.go:257-260`): "SetUpAt does not
    /// make sense for host paths - probably programmer error."
    async fn set_up_at(
        &self,
        _dir: &str,
        _args: &crate::volume_plugins::MounterArgs,
    ) -> Result<()> {
        Err(anyhow!("SetUpAt() does not make sense for host paths"))
    }
}

/// `hostPathUnmounter` (`host_path.go:266-280`). A hostPath volume is the host's
/// own directory: there is nothing under the pod directory to unmount.
pub struct HostPathUnmounter;

#[async_trait]
impl crate::volume_plugins::Unmounter for HostPathUnmounter {
    /// `hostPath.path` is `""` for an unmounter (`host_path.go:149`).
    fn get_path(&self) -> String {
        String::new()
    }

    /// `TearDown` (`host_path.go:272-274`): "TearDown does nothing."
    async fn tear_down(&self) -> Result<()> {
        Ok(())
    }

    /// `TearDownAt` (`host_path.go:277-280`): "does not make sense for host
    /// paths - probably programmer error."
    async fn tear_down_at(&self, _dir: &str) -> Result<()> {
        Err(anyhow!("TearDownAt() does not make sense for host paths"))
    }
}

impl crate::volume_plugins::plugin::MetricsProvider for HostPathMounter {
    /// `volume.MetricsNil` (`host_path.go:215`, `:344`).
    fn get_metrics(&self) -> anyhow::Result<crate::volume_plugins::plugin::Metrics> {
        crate::volume_plugins::plugin::MetricsProvider::get_metrics(
            &crate::volume_plugins::util::metrics::MetricsNil,
        )
    }
}

impl crate::volume_plugins::plugin::MetricsProvider for HostPathUnmounter {
    /// `volume.MetricsNil` (`host_path.go:215`, `:344`).
    fn get_metrics(&self) -> anyhow::Result<crate::volume_plugins::plugin::Metrics> {
        crate::volume_plugins::plugin::MetricsProvider::get_metrics(
            &crate::volume_plugins::util::metrics::MetricsNil,
        )
    }
}
