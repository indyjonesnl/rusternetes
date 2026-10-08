use anyhow::Result;
use async_trait::async_trait;
use rusternetes_common::quantity::Quantity;
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
    /// `Spec.ReadOnly` (`pkg/volume/plugins.go:437`): set by
    /// `NewSpecFromPersistentVolume(pv, readOnly)` (`plugins.go:556-561`) from
    /// the pod's `persistentVolumeClaim.readOnly`
    /// (`desired_state_of_world_populator.go:461`, `:588`); false for
    /// `NewSpecFromVolume` (`plugins.go:549-553`).
    pub read_only: bool,
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
            read_only: self.read_only,
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
    /// See [`Spec::read_only`].
    pub read_only: bool,
}

impl OwnedSpec {
    /// Borrow as the `Spec` the [`VolumePlugin`] methods take.
    pub fn as_spec(&self) -> Spec<'_> {
        Spec {
            volume: &self.volume,
            persistent_volume: self.persistent_volume.as_ref(),
            read_only: self.read_only,
        }
    }

    /// `Spec.Name` (`pkg/volume/plugins.go:444`). See [`Spec::name`].
    pub fn name(&self) -> &str {
        &self.volume.name
    }
}

/// Port of `volume.VolumePlugin` (`pkg/volume/plugins.go:128`).
///
/// Only the methods with a consumer are ported. `NewUnmounter` and
/// `ConstructVolumeSpec` arrived with the orphaned-volume teardown
/// ([`crate::volumes::VolumeManager::unmount_orphaned_volumes`]) and the CSI
/// held-device scan; `GetVolumeName`, `RequiresRemount` and
/// `SupportsSELinuxContextMount` arrived with `DesiredStateOfWorld`, which
/// calls all three.
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

    /// Rust spelling of the type assertion
    /// `volumePlugin.(volume.DeviceMountableVolumePlugin)`
    /// (`plugins.go:836-849`, `FindDeviceMountablePluginBySpec`). `None` is a
    /// failed assertion — every plugin but CSI, as upstream.
    fn as_device_mountable_plugin(&self) -> Option<&dyn DeviceMountableVolumePlugin> {
        None
    }

    /// Rust spelling of the type assertion
    /// `volumePlugin.(volume.NodeExpandableVolumePlugin)`
    /// (`plugins.go:931`, `:944`), which `FindNodeExpandablePlugin{BySpec,
    /// ByName}` perform. `None` is a failed assertion — every plugin
    /// registered today, as upstream's seven.
    fn as_node_expandable_plugin(&self) -> Option<&dyn NodeExpandableVolumePlugin> {
        None
    }

    /// Rust spelling of the type assertion
    /// `volumePlugin.(volume.BlockVolumePlugin)` (`plugins.go:906`, `:919`),
    /// which `FindMapperPluginBy{Spec,Name}` perform. `None` is a failed
    /// assertion — every plugin registered today, as upstream's seven.
    fn as_block_volume_plugin(&self) -> Option<&dyn BlockVolumePlugin> {
        None
    }

    /// `NewMounter` (`plugins.go:162`).
    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>>;

    /// `NewUnmounter` (`plugins.go:167`): create a [`Unmounter`] from
    /// recoverable state — the volume's name as per the `v1.Volume` spec and
    /// the UID of the pod it belonged to, which is all that survives on disk
    /// (`<pod>/volumes/<plugin>/<volName>`). Not async: the CSI plugin reads
    /// its volume info file here, everything else just builds a path.
    fn new_unmounter(&self, vol_name: &str, pod_uid: &str) -> Result<Box<dyn Unmounter>>;

    /// `ConstructVolumeSpec` (`plugins.go:173`): rebuild a spec from the
    /// volume name and the volume's path on disk, for a volume the kubelet
    /// found there rather than in the API. The spec may be incomplete.
    fn construct_volume_spec(
        &self,
        vol_name: &str,
        mount_path: &str,
    ) -> Result<ReconstructedVolume>;
}

/// `volume.ReconstructedVolume` (`pkg/volume/plugins.go`): what
/// `ConstructVolumeSpec` rebuilds from a mounted volume's on-disk state.
///
/// **Deviation:** upstream returns a `*volume.Spec` whose `Volume` is nil for a
/// PV-backed spec. Our [`Spec`] borrows a mandatory `Volume`, so the owned parts
/// are returned and the caller builds the `Spec`; for the PV arm `volume` is a
/// placeholder named after the PV (see `Spec::name`'s inherited deviation).
pub struct ReconstructedVolume {
    pub volume: Volume,
    pub persistent_volume: Option<PersistentVolume>,
}

/// Port of `volume.DeviceMounterArgs` (`pkg/volume/volume.go:290-294`).
///
/// `SELinuxLabel` is not ported (SELinuxMountReadWriteOncePod is not modelled
/// for CSI; see #2312).
///
/// **Deviation:** `node_name` is not upstream. Upstream's attacher reads it
/// from `host.GetNodeName()` (`csi_attacher.go:297`); our `VolumeHost` has no
/// node-name accessor, so the caller passes it here.
#[derive(Clone, Debug, Default)]
pub struct DeviceMounterArgs {
    pub fs_group: Option<i64>,
    pub node_name: String,
}

/// Port of `volume.DeviceMounter` (`pkg/volume/volume.go:296-311`): mounts a
/// device to a global path that individual pods then bind mount. For CSI this
/// is `NodeStageVolume`.
///
/// Errors follow upstream's contract: `TransientOperationFailure`,
/// `UncertainProgressError`, anything else is final.
#[async_trait]
pub trait DeviceMounter: Send + Sync {
    /// `GetDeviceMountPath` (`volume.go:301`).
    fn get_device_mount_path(&self, spec: &Spec<'_>) -> Result<String>;

    /// `MountDevice` (`volume.go:310`). `device_path` may be empty when the
    /// plugin has no attach step.
    async fn mount_device(
        &self,
        spec: &Spec<'_>,
        device_path: &str,
        device_mount_path: &str,
        args: &DeviceMounterArgs,
    ) -> Result<()>;
}

/// Port of `volume.DeviceMountableVolumePlugin` (`pkg/volume/plugins.go`):
/// `NewDeviceMounter` (`CanDeviceMount` stays on [`VolumePlugin`], see
/// [`VolumePlugin::can_device_mount`]).
pub trait DeviceMountableVolumePlugin: VolumePlugin {
    /// `NewDeviceMounter`.
    fn new_device_mounter(&self) -> Result<Box<dyn DeviceMounter>>;
}

/// Port of `volume.Unmounter` (`pkg/volume/volume.go:189-198`).
///
/// `async` where upstream is synchronous: the CSI implementation awaits a gRPC
/// `NodeUnpublishVolume`. An idiom difference, not a mechanism change.
/// `Send + Sync` for the reason given on [`Mounter`].
///
/// Upstream's `Unmounter` embeds `Volume`, i.e. `GetPath` plus a
/// `MetricsProvider`; only `GetPath` is ported, as the metrics half has no
/// consumer on the teardown path.
#[async_trait]
pub trait Unmounter: Send + Sync {
    /// `Volume::GetPath` (`volume.go:36`).
    fn get_path(&self) -> String;

    /// `TearDown` (`volume.go:194`): unmount the volume from a
    /// self-determined directory and remove traces of the SetUp procedure.
    /// Every upstream implementation is `return x.TearDownAt(x.GetPath())`,
    /// so that is the default.
    async fn tear_down(&self) -> Result<()> {
        self.tear_down_at(&self.get_path()).await
    }

    /// `TearDownAt` (`volume.go:197`): unmount the volume from the specified
    /// directory and remove traces of the SetUp procedure.
    async fn tear_down_at(&self, dir: &str) -> Result<()>;
}

/// Port of `volume.Attributes` (`pkg/volume/volume.go:119-124`): the
/// attributes of a [`Mounter`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Attributes {
    pub read_only: bool,
    pub managed: bool,
    pub selinux_relabel: bool,
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
///
/// **`Sync` added.** Upstream's `volume.Mounter` carries no such bound, but the
/// value is stored in `actualStateOfWorld.attachedVolumes` — a map behind an
/// `RWMutex` that every kubelet goroutine reads (`actual_state_of_world.go:256`,
/// `:359`). `ActualStateOfWorld` holding an `Arc<dyn Mounter>` is only `Sync`
/// if the mounter is, so Rust makes explicit the sharing Go leaves implicit.
#[async_trait]
pub trait Mounter: Send + Sync {
    /// `Volume::GetPath` (`volume.go:36`).
    fn get_path(&self) -> String;

    /// `Mounter::SetUp` (`volume.go:175`). Upstream takes `MounterArgs`
    /// (fsGroup, SELinux label); no moved body reads any of it, so the
    /// argument is not ported until a consumer needs it.
    async fn set_up(&self) -> Result<()>;

    /// `Mounter::GetAttributes` (`volume.go:187`): the attributes of the
    /// mounter, called after `SetUp`. The kubelet's `makeMounts` reads
    /// `Managed && SELinuxRelabel` to decide the CRI mount's `selinux_relabel`
    /// and `ReadOnly` to force a read-only mount
    /// (`pkg/kubelet/kubelet_pods.go:296`, `:392`).
    fn get_attributes(&self) -> Attributes;
}

/// Port of `volume.BlockVolumeMapper` (`pkg/volume/volume.go:200-203`), which
/// is `volume.BlockVolume` (`volume.go:45-62`) under another name.
///
/// The whole of `BlockVolume` is ported: `GetGlobalMapPath`,
/// `GetPodDeviceMapPath`, `SupportsMetrics` and the embedded
/// [`MetricsProvider`]. The raw device a mapper maps is handed to the runtime
/// as a CRI `Device` in `ContainerConfig.devices` (CRI v1), never as a mount.
///
/// `Send + Sync` for the same reason as [`Mounter`].
pub trait BlockVolumeMapper: Send + Sync + MetricsProvider {
    /// `BlockVolume::GetGlobalMapPath` (`volume.go:49`). Global map path
    /// containing the bind mount associated with a block device, e.g.
    /// `plugins/kubernetes.io/{PluginName}/{DefaultKubeletVolumeDevicesDirName}/{volumePluginDependentPath}/{pod uuid}`.
    fn get_global_map_path(&self, spec: &Spec<'_>) -> Result<String>;

    /// `BlockVolume::GetPodDeviceMapPath` (`volume.go:53`). Returns the pod
    /// device map path and the name of the symlink to the block device, e.g.
    /// `pods/{podUid}/{DefaultKubeletVolumeDevicesDirName}/{escapeQualifiedPluginName}/`, `{volumeName}`.
    fn get_pod_device_map_path(&self) -> (String, String);

    /// `BlockVolume::SupportsMetrics` (`volume.go:57`): true if the
    /// [`MetricsProvider`] is initialized.
    fn supports_metrics(&self) -> bool;

    /// Rust spelling of the type assertion
    /// `mapper.(volume.CustomBlockVolumeMapper)` (`volume.go:205`), which the
    /// operation generator performs to decide whether the plugin needs a
    /// plugin-specific `SetUpDevice`/`MapPodDevice`. `None` is a failed
    /// assertion.
    fn as_custom_block_volume_mapper(&self) -> Option<&dyn CustomBlockVolumeMapper> {
        None
    }
}

/// Port of `volume.BlockVolumeUnmapper` (`pkg/volume/volume.go:228-230`),
/// which is `BlockVolume` under another name.
pub trait BlockVolumeUnmapper: Send + Sync + MetricsProvider {
    /// `BlockVolume::GetGlobalMapPath` (`volume.go:49`).
    fn get_global_map_path(&self, spec: &Spec<'_>) -> Result<String>;

    /// `BlockVolume::GetPodDeviceMapPath` (`volume.go:53`).
    fn get_pod_device_map_path(&self) -> (String, String);

    /// `BlockVolume::SupportsMetrics` (`volume.go:57`).
    fn supports_metrics(&self) -> bool;

    /// Rust spelling of the type assertion
    /// `unmapper.(volume.CustomBlockVolumeUnmapper)` (`volume.go:233`).
    fn as_custom_block_volume_unmapper(&self) -> Option<&dyn CustomBlockVolumeUnmapper> {
        None
    }
}

/// Port of `volume.CustomBlockVolumeMapper` (`pkg/volume/volume.go:205-225`):
/// the plugin-specific set-up/map steps. Idempotent, like every upstream
/// volume method.
pub trait CustomBlockVolumeMapper: BlockVolumeMapper {
    /// `SetUpDevice` (`volume.go:211`). Prepares the volume on the node the
    /// plugin-specific way; may be called more than once. Returns the staging
    /// path if device setup succeeded.
    fn set_up_device(&self) -> Result<String>;

    /// `MapPodDevice` (`volume.go:218`). Maps the block device to a path and
    /// returns it. A unique device path across a kubelet node reboot is
    /// required to avoid unexpected block-volume destruction. An empty string
    /// means "use the path the attacher returned".
    fn map_pod_device(&self) -> Result<String>;

    /// `GetStagingPath` (`volume.go:224`): the path used for staging the
    /// volume, mainly used by CSI plugins.
    fn get_staging_path(&self) -> String;
}

/// Port of `volume.CustomBlockVolumeUnmapper` (`pkg/volume/volume.go:233-243`).
pub trait CustomBlockVolumeUnmapper: BlockVolumeUnmapper {
    /// `TearDownDevice` (`volume.go:238`). Removes traces of `SetUpDevice`;
    /// for a non-attachable plugin this detaches the volume from the node.
    fn tear_down_device(&self, map_path: &str, device_path: &str) -> Result<()>;

    /// `UnmapPodDevice` (`volume.go:242`). Removes traces of `MapPodDevice`.
    fn unmap_pod_device(&self) -> Result<()>;
}

/// Port of `volume.MetricsProvider` (`pkg/volume/volume.go:64-68`).
pub trait MetricsProvider {
    /// `GetMetrics`. May be expensive for some implementations.
    fn get_metrics(&self) -> Result<Metrics>;
}

/// Port of `volume.Metrics` (`pkg/volume/volume.go:73-110`). Each quantity is
/// `Option` because upstream's are nilable pointers (nil = not reported).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Metrics {
    /// The time at which these stats were updated.
    pub time: Option<chrono::DateTime<chrono::Utc>>,
    /// Total bytes used by the volume. For block devices this may exceed the
    /// total size of the files.
    pub used: Option<Quantity>,
    /// Total capacity (bytes) of the volume's underlying storage.
    pub capacity: Option<Quantity>,
    /// Storage space available (bytes) for the volume.
    pub available: Option<Quantity>,
    /// Total inodes used by the volume.
    pub inodes_used: Option<Quantity>,
    /// Total inodes available in the volume.
    pub inodes: Option<Quantity>,
    /// Free inodes in the volume.
    pub inodes_free: Option<Quantity>,
    /// Non-empty when the stats could not be collected.
    pub error: String,
}

/// Port of `volume.NodeResizeOptions` (`pkg/volume/plugins.go:99-116`).
///
/// `Clone` because upstream passes it by value and `nodeExpander` reuses one
/// copy for the pre-check and the plugin call.
#[derive(Clone)]
pub struct NodeResizeOptions<'a> {
    pub volume_spec: &'a Spec<'a>,
    /// Location of the actual device on the node. For CSI this may just be
    /// the volume ID.
    pub device_path: String,
    /// Where the device is mounted on the node: the global mount path if the
    /// volume type is attachable, otherwise where it was mounted for the pod.
    pub device_mount_path: String,
    /// Where the volume is staged (`DeviceStagePath`).
    pub device_stage_path: String,
    pub new_size: Quantity,
    pub old_size: Quantity,
}

/// Port of `volume.NodeExpandableVolumePlugin` (`pkg/volume/plugins.go:256-263`),
/// the extension of [`VolumePlugin`] for volumes that require expansion on
/// the node via a `NodeExpand` call. A plugin opts in by returning itself
/// from [`VolumePlugin::as_node_expandable_plugin`].
///
/// Replaces the `requires_fs_resize` predicate that previously sat on
/// [`VolumePlugin`] and collapsed this sub-interface (#2328). Upstream's
/// `RequiresFSResize()` takes no spec, and neither does this.
#[async_trait]
pub trait NodeExpandableVolumePlugin: VolumePlugin {
    /// `RequiresFSResize` (`plugins.go:260`).
    fn requires_fs_resize(&self) -> bool;

    /// `NodeExpand` (`plugins.go:262`): expand the volume on
    /// `device_mount_path` and report whether the resize happened. Async
    /// where upstream is synchronous because the CSI implementation awaits
    /// a gRPC `NodeExpandVolume`; an idiom difference, not a mechanism one.
    async fn node_expand(&self, resize_options: NodeResizeOptions<'_>) -> Result<bool>;
}

/// Port of `volume.BlockVolumePlugin` (`pkg/volume/plugins.go:265-283`), the
/// extension of [`VolumePlugin`] for block-volume support. A plugin opts in by
/// returning itself from [`VolumePlugin::as_block_volume_plugin`].
pub trait BlockVolumePlugin: VolumePlugin {
    /// `NewBlockVolumeMapper` (`plugins.go:272`): create a mapper from an API
    /// specification. Ownership of the spec is not transferred.
    fn new_block_volume_mapper(
        &self,
        spec: &Spec<'_>,
        pod: &Pod,
    ) -> Result<Box<dyn BlockVolumeMapper>>;

    /// `NewBlockVolumeUnmapper` (`plugins.go:276`): create an unmapper from
    /// recoverable state. `name` is the volume name as per the `v1.Volume`
    /// spec, `pod_uid` the UID of the enclosing pod.
    fn new_block_volume_unmapper(
        &self,
        name: &str,
        pod_uid: &str,
    ) -> Result<Box<dyn BlockVolumeUnmapper>>;

    /// `ConstructBlockVolumeSpec` (`plugins.go:282`): reconstruct a spec from
    /// the pod UID, volume name and pod device map path read off disk. The
    /// spec may be incomplete. Returns an [`OwnedSpec`] because a borrowed
    /// [`Spec`] cannot outlive the call.
    fn construct_block_volume_spec(
        &self,
        pod_uid: &str,
        volume_name: &str,
        volume_path: &str,
    ) -> Result<OwnedSpec>;
}

#[cfg(test)]
mod block_tests {
    use super::*;

    struct Plain;
    impl MetricsProvider for Plain {
        fn get_metrics(&self) -> Result<Metrics> {
            Ok(Metrics::default())
        }
    }
    impl BlockVolumeMapper for Plain {
        fn get_global_map_path(&self, _s: &Spec<'_>) -> Result<String> {
            Ok("g".into())
        }
        fn get_pod_device_map_path(&self) -> (String, String) {
            ("p".into(), "n".into())
        }
        fn supports_metrics(&self) -> bool {
            false
        }
    }

    struct Custom;
    impl MetricsProvider for Custom {
        fn get_metrics(&self) -> Result<Metrics> {
            Ok(Metrics::default())
        }
    }
    impl BlockVolumeMapper for Custom {
        fn get_global_map_path(&self, _s: &Spec<'_>) -> Result<String> {
            Ok("g".into())
        }
        fn get_pod_device_map_path(&self) -> (String, String) {
            ("p".into(), "n".into())
        }
        fn supports_metrics(&self) -> bool {
            true
        }
        fn as_custom_block_volume_mapper(&self) -> Option<&dyn CustomBlockVolumeMapper> {
            Some(self)
        }
    }
    impl CustomBlockVolumeMapper for Custom {
        fn set_up_device(&self) -> Result<String> {
            Ok("/staging".into())
        }
        fn map_pod_device(&self) -> Result<String> {
            Ok(String::new())
        }
        fn get_staging_path(&self) -> String {
            "/staging".into()
        }
    }

    /// The operation generator type-asserts a mapper to
    /// `CustomBlockVolumeMapper`; a plain mapper must fail that assertion.
    #[test]
    fn custom_mapper_assertion() {
        assert!(Plain.as_custom_block_volume_mapper().is_none());
        let c = Custom;
        let custom = c.as_custom_block_volume_mapper().unwrap();
        assert_eq!(custom.set_up_device().unwrap(), "/staging");
        assert_eq!(custom.map_pod_device().unwrap(), "");
        assert_eq!(custom.get_staging_path(), "/staging");
        assert!(c.supports_metrics());
    }
}
