use anyhow::Result;
use async_trait::async_trait;
use rusternetes_common::resources::{PersistentVolume, Pod, Volume};

/// Port of `volume.Spec` (`pkg/volume/plugins.go:434`).
///
/// A volume as the plugin sees it: the inline `v1.Volume` always, plus the
/// bound `PersistentVolume` when the volume reached us through a claim. Both
/// arms exist because a plugin answers `can_support` for either — a hostPath
/// PV and an inline hostPath volume are both the hostPath plugin's work
/// (`pkg/volume/hostpath/host_path.go:98`).
pub struct Spec<'a> {
    pub volume: &'a Volume,
    pub persistent_volume: Option<&'a PersistentVolume>,
}

/// Port of `volume.VolumePlugin` (`pkg/volume/plugins.go:128`).
///
/// Only the methods with a consumer in this sub-project are ported.
/// `GetVolumeName`, `RequiresRemount`, `ConstructVolumeSpec` and
/// `NewUnmounter` arrive with the sub-project that calls them (#1970).
#[async_trait]
pub trait VolumePlugin: Send + Sync {
    /// `GetPluginName` (`plugins.go:138`). Namespaced, exactly one `/`.
    fn name(&self) -> &'static str;

    /// `CanSupport` (`plugins.go:151`). The spec is read-only.
    fn can_support(&self, spec: &Spec<'_>) -> bool;

    /// `NewMounter` (`plugins.go:162`).
    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>>;
}

/// Port of `volume.Mounter` (`pkg/volume/volume.go:162`).
///
/// `set_up` is async where upstream's `SetUp` is synchronous: our bodies await
/// storage reads, and Go blocks where Rust awaits. That is an idiom
/// difference, not a mechanism change.
///
/// `get_path` returns a `String` rather than upstream's `string` path type
/// because every caller in this crate already threads volume paths as
/// `String`. It is not necessarily under the pod directory — the hostPath
/// plugin's path is wherever the host path points.
#[async_trait]
pub trait Mounter: Send {
    /// `Volume::GetPath` (`volume.go:36`).
    fn get_path(&self) -> String;

    /// `Mounter::SetUp` (`volume.go:175`). Upstream takes `MounterArgs`
    /// (fsGroup, SELinux label); no moved body reads any of it, so the
    /// argument is not ported until a consumer needs it.
    async fn set_up(&self) -> Result<()>;
}
