use crate::volume_plugins::metrics_statfs::MetricsStatFs;
use crate::volume_plugins::plugin::{Metrics, MetricsProvider};
use crate::volume_plugins::util::mount_ops::{cleanup_mount_with_force, Mounter as MountUtil};
use crate::volume_plugins::util::mount_utils::is_not_mount_point;
use crate::volume_plugins::util::{join_mount_options, mount_option_from_spec};
use crate::volume_plugins::{
    Attributes, Mounter, ReconstructedVolume, Spec, Unmounter, VolumeHost, VolumePlugin,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusternetes_common::resources::volume::{NFSVolumeSource, PersistentVolumeAccessMode};
use rusternetes_common::resources::Pod;
use std::io;
use std::os::unix::fs::DirBuilderExt;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error};

/// `unMountTimeout` (`pkg/volume/nfs/nfs.go:67`).
const UNMOUNT_TIMEOUT: Duration = Duration::from_secs(60);

/// Port of `pkg/volume/nfs/nfs.go` (`nfsPlugin`), registered by
/// `ProbeVolumePlugins` (`cmd/kubelet/app/plugins.go`).
///
/// Not ported: `Recycle` (`nfs.go:156-173`, the PV controller's recycler
/// pod, not a kubelet concern). The `MetricsProvider` embedded in `nfs`
/// (`volume.NewMetricsStatFS`, `nfs.go:131`, `:150`) is on the concrete
/// mounter/unmounter, which the `Mounter`/`Unmounter` supertrait requires.
pub struct NfsPlugin {
    host: Arc<dyn VolumeHost>,
}

impl NfsPlugin {
    /// `Init` (`nfs.go:70-73`).
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }

    /// `newMounterInternal` (`nfs.go:116-138`), returning the concrete mounter
    /// so its embedded `MetricsProvider` stays reachable.
    pub fn new_nfs_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<NfsMounter> {
        let (source, read_only) = get_volume_source(spec)?;
        Ok(NfsMounter {
            nfs: Nfs::new(spec.name(), &pod.metadata.uid, self.host.clone()),
            server: get_server_from_source(source),
            export_path: source.path.clone(),
            read_only,
            mount_options: mount_option_from_spec(spec, &[]),
        })
    }

    /// `newUnmounterInternal` (`nfs.go:144-152`).
    pub fn new_nfs_unmounter(&self, vol_name: &str, pod_uid: &str) -> NfsUnmounter {
        NfsUnmounter(Nfs::new(vol_name, pod_uid, self.host.clone()))
    }

    /// `GetAccessModes` (`nfs.go:108-114`): `PersistentVolumePlugin`.
    pub fn get_access_modes(&self) -> Vec<PersistentVolumeAccessMode> {
        vec![
            PersistentVolumeAccessMode::ReadWriteOnce,
            PersistentVolumeAccessMode::ReadOnlyMany,
            PersistentVolumeAccessMode::ReadWriteMany,
        ]
    }

    /// `SupportsMountOption` (`nfs.go:100-102`): `true`.
    pub fn supports_mount_option(&self) -> bool {
        true
    }
}

/// `getVolumeSource` (`nfs.go:295-304`): the inline source (with its own
/// `readOnly`) first, else the PV's (with `spec.ReadOnly`, i.e. the claim's).
pub fn get_volume_source<'a>(spec: &'a Spec<'_>) -> Result<(&'a NFSVolumeSource, bool)> {
    if let Some(nfs) = spec.volume.nfs.as_ref() {
        return Ok((nfs, nfs.read_only.unwrap_or(false)));
    }
    if let Some(nfs) = spec.persistent_volume.and_then(|pv| pv.spec.nfs.as_ref()) {
        return Ok((nfs, spec.read_only));
    }
    Err(anyhow!("Spec does not reference a NFS volume type"))
}

/// `getServerFromSource` (`nfs.go:306-311`): an IPv6 server is bracketed.
pub fn get_server_from_source(source: &NFSVolumeSource) -> String {
    if source.server.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{}]", source.server)
    } else {
        source.server.clone()
    }
}

#[async_trait]
impl VolumePlugin for NfsPlugin {
    /// `GetPluginName` (`nfs.go:75-77`).
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::NFS
    }

    /// `GetVolumeName` (`nfs.go:79-89`): `<server>/<path>`.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        let (source, _) = get_volume_source(spec)?;
        Ok(format!("{}/{}", source.server, source.path))
    }

    /// `CanSupport` (`nfs.go:91-94`).
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.persistent_volume
            .is_some_and(|pv| pv.spec.nfs.is_some())
            || spec.volume.nfs.is_some()
    }

    /// `RequiresRemount` (`nfs.go:96-98`): `false`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }

    /// `SupportsSELinuxContextMount` (`nfs.go:104-106`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }

    /// `NewMounter` / `newMounterInternal` (`nfs.go:116-138`).
    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        Ok(Box::new(self.new_nfs_mounter(spec, pod)?))
    }

    /// `NewUnmounter` / `newUnmounterInternal` (`nfs.go:140-152`).
    fn new_unmounter(&self, vol_name: &str, pod_uid: &str) -> Result<Box<dyn Unmounter>> {
        Ok(Box::new(self.new_nfs_unmounter(vol_name, pod_uid)))
    }

    /// `ConstructVolumeSpec` (`nfs.go:175-187`): an NFS volume named, and
    /// pathed, after the volume.
    fn construct_volume_spec(
        &self,
        vol_name: &str,
        _mount_path: &str,
    ) -> Result<ReconstructedVolume> {
        crate::volume_plugins::util::reconstructed_volume(
            vol_name,
            serde_json::json!({"nfs": {"server": "", "path": vol_name}}),
        )
    }
}

/// `nfs` (`nfs.go:190-196`).
#[derive(Clone)]
struct Nfs {
    vol_name: String,
    pod_uid: String,
    host: Arc<dyn VolumeHost>,
    /// Embedded `volume.MetricsProvider` (`nfs.go:195`), set to
    /// `volume.NewMetricsStatFS(getPath(..))` (`nfs.go:131`, `:150`).
    metrics: MetricsStatFs,
}

impl Nfs {
    fn new(vol_name: &str, pod_uid: &str, host: Arc<dyn VolumeHost>) -> Self {
        let mut nfs = Nfs {
            vol_name: vol_name.to_string(),
            pod_uid: pod_uid.to_string(),
            host,
            metrics: MetricsStatFs::new(""),
        };
        nfs.metrics = MetricsStatFs::new(&nfs.path());
        nfs
    }

    /// `GetPath` (`nfs.go:198-201`).
    fn path(&self) -> String {
        self.host
            .get_pod_volume_dir(&self.pod_uid, crate::pod_dirs::plugin::NFS, &self.vol_name)
    }
}

/// `nfsMounter` (`nfs.go:203-209`).
#[derive(Clone)]
pub struct NfsMounter {
    nfs: Nfs,
    server: String,
    export_path: String,
    read_only: bool,
    mount_options: Vec<String>,
}

impl NfsMounter {
    /// The blocking body of `SetUpAt` (`nfs.go:226-271`).
    fn set_up_at_blocking(&self, mounter: &MountUtil, dir: &str) -> Result<()> {
        // `mount.IsNotMountPoint` returns `true` with the error; ErrNotExist
        // is tolerated (`nfs.go:227-231`).
        let not_mnt = match is_not_mount_point(dir) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => true,
            Err(e) => return Err(e.into()),
        };
        debug!("NFS mount set up: {dir} {}", !not_mnt);
        if !not_mnt {
            return Ok(());
        }
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o750)
            .create(dir)?;
        let source = format!("{}:{}", self.server, self.export_path);
        let mut options: Vec<String> = Vec::new();
        if self.read_only {
            options.push("ro".to_string());
        }
        let mount_options = join_mount_options(&self.mount_options, &options);
        let Err(mount_err) =
            mounter.mount_sensitive_without_systemd(&source, dir, "nfs", &mount_options, &[], &[])
        else {
            return Ok(());
        };
        match is_not_mount_point(dir) {
            Err(e) => {
                error!("IsNotMountPoint check failed: {e}");
                return Err(mount_err.into());
            }
            Ok(false) => {
                if let Err(e) = mounter.unmount(dir) {
                    error!("Failed to unmount: {e}");
                    return Err(mount_err.into());
                }
                match is_not_mount_point(dir) {
                    Err(e) => {
                        error!("IsNotMountPoint check failed: {e}");
                        return Err(mount_err.into());
                    }
                    Ok(false) => {
                        // This is very odd, we don't expect it.  We'll try again next sync loop.
                        error!(
                            "{dir} is still mounted, despite call to unmount().  Will try again next sync loop."
                        );
                        return Err(mount_err.into());
                    }
                    Ok(true) => {}
                }
            }
            Ok(true) => {}
        }
        let _ = std::fs::remove_dir(dir);
        Err(mount_err.into())
    }
}

#[async_trait]
impl Mounter for NfsMounter {
    fn get_path(&self) -> String {
        self.nfs.path()
    }

    /// `GetAttributes` (`nfs.go:213-219`).
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: self.read_only,
            managed: false,
            selinux_relabel: false,
        }
    }

    /// `SetUpAt` (`nfs.go:226-271`). Mount(8) and the mount-table reads block,
    /// so they run off the async runtime.
    async fn set_up_at(&self, dir: &str, _args: &crate::volume_plugins::MounterArgs) -> Result<()> {
        let this = self.clone();
        let dir = dir.to_string();
        tokio::task::spawn_blocking(move || {
            let host = this.nfs.host.clone();
            this.set_up_at_blocking(host.get_mounter(), &dir)
        })
        .await
        .map_err(|e| anyhow!("NFS SetUpAt task failed: {e}"))?
    }
}

/// `nfsUnmounter` (`nfs.go:275-277`).
pub struct NfsUnmounter(Nfs);

impl MetricsProvider for NfsMounter {
    fn get_metrics(&self) -> Result<Metrics> {
        self.nfs.metrics.get_metrics()
    }
}

impl MetricsProvider for NfsUnmounter {
    fn get_metrics(&self) -> Result<Metrics> {
        self.0.metrics.get_metrics()
    }
}

#[async_trait]
impl Unmounter for NfsUnmounter {
    fn get_path(&self) -> String {
        self.0.path()
    }

    /// `TearDownAt` (`nfs.go:283-293`). Upstream asserts the mounter to
    /// `mount.MounterForceUnmounter`; the Linux `*Mounter` the kubelet hands
    /// the plugin implements it, so the `CleanupMountWithForce` arm is the one
    /// taken, with `extensiveMountPointCheck` true: it "consult[s] /proc/mounts.
    /// We can't use faster IsLikelyNotMountPoint (lstat()), since there may be
    /// root_squash on the NFS server and kubelet may not be able to do
    /// lstat/stat() there."
    async fn tear_down_at(&self, dir: &str) -> Result<()> {
        let host = self.0.host.clone();
        let dir = dir.to_string();
        tokio::task::spawn_blocking(move || {
            cleanup_mount_with_force(&dir, host.get_mounter(), true, UNMOUNT_TIMEOUT)
        })
        .await
        .map_err(|e| anyhow!("NFS TearDownAt task failed: {e}"))??;
        Ok(())
    }
}
