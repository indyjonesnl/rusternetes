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

impl Spec<'_> {
    /// Port of `Spec.Name` (`pkg/volume/plugins.go:444-453`):
    ///
    /// ```go
    /// switch {
    /// case spec.Volume != nil:       return spec.Volume.Name
    /// case spec.PersistentVolume != nil: return spec.PersistentVolume.Name
    /// default:                       return ""
    /// }
    /// ```
    ///
    /// **Deviation, inherited:** upstream's `Spec.Volume` is nilable, so the
    /// second arm is reachable for a PVC-backed volume whose spec is built
    /// from the dereferenced PV alone. Our [`Spec`] (ported in `ec0bbd72`)
    /// makes `volume` mandatory, so only the first arm can ever run. Widening
    /// `Spec` is a change to every plugin's `can_support` and is out of scope
    /// here; the arm is written out so the divergence is visible at the point
    /// it matters.
    pub fn name(&self) -> &str {
        &self.volume.name
    }

    /// Clone this borrowed spec into an owned [`OwnedSpec`].
    pub fn to_owned_spec(&self) -> OwnedSpec {
        OwnedSpec {
            volume: self.volume.clone(),
            persistent_volume: self.persistent_volume.cloned(),
        }
    }
}

/// Owned counterpart of [`Spec`].
///
/// **Rust-idiom deviation, no upstream equivalent.** Upstream has exactly one
/// `volume.Spec` type, held by pointer, so the volume manager's caches can
/// store the same `*volume.Spec` the plugin lookup used. A borrowed `Spec<'a>`
/// cannot be stored next to the `Pod` it borrows from (that is a
/// self-referential struct), so the caches hold an `OwnedSpec` and hand out a
/// borrowed [`Spec`] via [`OwnedSpec::as_spec`] whenever a plugin call needs
/// one. The mechanism — one spec value shared by every reader — is preserved
/// by wrapping it in an `Arc`; only the expression changes.
#[derive(Clone)]
pub struct OwnedSpec {
    pub volume: Volume,
    pub persistent_volume: Option<PersistentVolume>,
}

impl OwnedSpec {
    /// Borrow as the `Spec` the [`VolumePlugin`] methods take.
    pub fn as_spec(&self) -> Spec<'_> {
        Spec {
            volume: &self.volume,
            persistent_volume: self.persistent_volume.as_ref(),
        }
    }

    /// `Spec.Name` (`pkg/volume/plugins.go:444`). See [`Spec::name`].
    pub fn name(&self) -> &str {
        &self.volume.name
    }
}

/// Port of `volume.VolumePlugin` (`pkg/volume/plugins.go:128`).
///
/// Only the methods with a consumer are ported. `ConstructVolumeSpec` and
/// `NewUnmounter` arrive with the sub-project that calls them (#1970);
/// `GetVolumeName`, `RequiresRemount` and `SupportsSELinuxContextMount`
/// arrived here with `DesiredStateOfWorld`, which calls all three.
#[async_trait]
pub trait VolumePlugin: Send + Sync {
    /// `GetPluginName` (`plugins.go:138`). Namespaced, exactly one `/`.
    fn name(&self) -> &'static str;

    /// `GetVolumeName` (`plugins.go:146`).
    ///
    /// A name/ID uniquely identifying the actual backing device, directory or
    /// path — NOT `spec.Name()` in general. `util::get_unique_volume_name`
    /// prefixes it with the plugin name to form the cache key for attachable
    /// and device-mountable volumes.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String>;

    /// `CanSupport` (`plugins.go:151`). The spec is read-only.
    fn can_support(&self, spec: &Spec<'_>) -> bool;

    /// `RequiresRemount` (`plugins.go:156`).
    ///
    /// True for volumes whose contents track the API object and must be
    /// re-mounted when the pod is updated. `DesiredStateOfWorld` reads this to
    /// decide whether a re-add keeps the original `mountRequestTime` or takes
    /// a fresh one (`desired_state_of_world.go:362-364`).
    fn requires_remount(&self, spec: &Spec<'_>) -> bool;

    /// `SupportsSELinuxContextMount` (`plugins.go:180`).
    fn supports_selinux_context_mount(&self, spec: &Spec<'_>) -> Result<bool>;

    /// True when upstream's `FindAttachablePluginBySpec` would return this
    /// plugin for `spec` (`plugins.go:805-818`).
    ///
    /// **Deliberate collapse.** Upstream type-asserts the plugin to
    /// `volume.AttachableVolumePlugin`, then calls `CanAttach(spec)`, and
    /// `util.IsAttachableVolume` additionally requires `NewAttacher()` to
    /// succeed (`pkg/volume/util/util.go:635-645`). We have neither the
    /// sub-interface nor an `Attacher` type, and inventing them to hold one
    /// boolean would be worse than stating the predicate directly. Every
    /// plugin this crate registers answers `false`, which is the same answer
    /// upstream gives for all seven of them. When an attachable plugin lands,
    /// this becomes `self.as_attachable().is_some_and(|a| a.can_attach(spec))`
    /// and the branch that reads it does not move.
    fn can_attach(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// True when upstream's `FindDeviceMountablePluginBySpec` would return
    /// this plugin for `spec` (`plugins.go:836-849`). Same collapse as
    /// [`VolumePlugin::can_attach`], for `DeviceMountableVolumePlugin` /
    /// `CanDeviceMount` / `NewDeviceMounter`.
    fn can_device_mount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

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
