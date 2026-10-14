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
/// mounter/unmounter; the `Mounter`/`Unmounter` traits do not yet require it.
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
    fn new_nfs_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<NfsMounter> {
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
    fn new_nfs_unmounter(&self, vol_name: &str, pod_uid: &str) -> NfsUnmounter {
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
fn get_volume_source<'a>(spec: &'a Spec<'_>) -> Result<(&'a NFSVolumeSource, bool)> {
    if let Some(nfs) = spec.volume.nfs.as_ref() {
        return Ok((nfs, nfs.read_only.unwrap_or(false)));
    }
    if let Some(nfs) = spec.persistent_volume.and_then(|pv| pv.spec.nfs.as_ref()) {
        return Ok((nfs, spec.read_only));
    }
    Err(anyhow!("Spec does not reference a NFS volume type"))
}

/// `getServerFromSource` (`nfs.go:306-311`): an IPv6 server is bracketed.
fn get_server_from_source(source: &NFSVolumeSource) -> String {
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
struct NfsMounter {
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
struct NfsUnmounter(Nfs);

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

#[cfg(test)]
mod tests {
    //! Ports of `pkg/volume/nfs/nfs_test.go` (`TestCanSupport`,
    //! `TestGetAccessModes`, `TestPluginVolume`, `TestIPV6VolumeSource`,
    //! `TestIPV4VolumeSource`, `TestPluginPersistentVolume`,
    //! `TestPersistentClaimReadOnlyFlag`). `doTestPlugin` drives the mounter
    //! through a `FakeMounter`; our `mount.Interface` is the concrete
    //! `Mounter` shelling out to mount(8), so the mount call itself is not
    //! faked here: the source string it receives is asserted from the
    //! mounter's inputs, and the cleanup path is exercised on a real directory.
    use super::*;
    use crate::volume_plugins::{KubeletVolumeHost, VolumePluginMgr};
    use rusternetes_common::resources::{PersistentVolume, Volume};
    use serde_json::json;

    fn host(tmp: &std::path::Path) -> Arc<dyn VolumeHost> {
        Arc::new(KubeletVolumeHost::new(
            tmp.to_string_lossy().to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            std::collections::HashMap::new(),
        ))
    }

    fn plugin(tmp: &std::path::Path) -> NfsPlugin {
        NfsPlugin::new(host(tmp))
    }

    fn vol(source: serde_json::Value) -> Volume {
        serde_json::from_value(json!({"name": "vol1", "nfs": source})).unwrap()
    }

    fn bare(name: &str) -> Volume {
        serde_json::from_value(json!({"name": name})).unwrap()
    }

    fn pv(v: serde_json::Value) -> PersistentVolume {
        serde_json::from_value(v).unwrap()
    }

    fn pod() -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "uid": "poduid"},
            "spec": {"containers": []}
        }))
        .unwrap()
    }

    fn spec<'a>(volume: &'a Volume, pv: Option<&'a PersistentVolume>, ro: bool) -> Spec<'a> {
        Spec {
            volume,
            persistent_volume: pv,
            read_only: ro,
        }
    }

    /// `TestCanSupport`.
    #[test]
    fn can_support() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = VolumePluginMgr::new(vec![Box::new(plugin(tmp.path()))]);
        let plug = mgr.find_plugin_by_name("kubernetes.io/nfs").unwrap();
        assert_eq!(plug.name(), "kubernetes.io/nfs");

        let v = vol(json!({"server": "", "path": ""}));
        assert!(plug.can_support(&spec(&v, None, false)));
        let p =
            pv(json!({"metadata": {"name": "pv"}, "spec": {"nfs": {"server": "", "path": ""}}}));
        let empty = bare("x");
        assert!(plug.can_support(&spec(&empty, Some(&p), false)));
        assert!(!plug.can_support(&spec(&empty, None, false)));
    }

    /// `TestGetAccessModes`.
    #[test]
    fn get_access_modes() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(
            plugin(tmp.path()).get_access_modes(),
            vec![
                PersistentVolumeAccessMode::ReadWriteOnce,
                PersistentVolumeAccessMode::ReadOnlyMany,
                PersistentVolumeAccessMode::ReadWriteMany
            ]
        );
    }

    /// `doTestPlugin` (`nfs_test.go:99-190`) as far as it does not touch the
    /// fake mounter: the volume path under `kubernetes.io~nfs`, the
    /// `<server>:<path>` source, and `GetAttributes`.
    /// `nfs` embeds `volume.NewMetricsStatFS(getPath(..))` in both the mounter
    /// and the unmounter (`nfs.go:131`, `:150`): metrics are the statfs of the
    /// pod volume dir.
    #[tokio::test]
    async fn mounter_and_unmounter_provide_statfs_metrics() {
        let tmp = tempfile::tempdir().unwrap();
        let plug = plugin(tmp.path());
        let v = vol(json!({"server": "127.0.0.1", "path": "/somepath"}));
        let m = plug
            .new_nfs_mounter(&spec(&v, None, false), &pod())
            .unwrap();
        let u = plug.new_nfs_unmounter("vol1", "poduid");

        // Directory absent: FsInfoFailed, like upstream's statfs error.
        let err = m.get_metrics().unwrap_err();
        assert!(err.to_string().starts_with("failed to get FsInfo"), "{err}");

        std::fs::create_dir_all(m.get_path()).unwrap();
        for got in [m.get_metrics().unwrap(), u.get_metrics().unwrap()] {
            assert!(got.capacity.unwrap().value() > 0);
            assert!(got.available.unwrap().value() > 0);
        }
    }

    async fn do_test_plugin(spec: &Spec<'_>, want_source: &str) {
        let tmp = tempfile::tempdir().unwrap();
        let plug = plugin(tmp.path());
        let m = plug.new_mounter(spec, &pod()).await.unwrap();
        let want = tmp
            .path()
            .join("pods/poduid/volumes/kubernetes.io~nfs/vol1")
            .to_string_lossy()
            .to_string();
        assert_eq!(m.get_path(), want);
        let (source, _) = get_volume_source(spec).unwrap();
        assert_eq!(
            format!("{}:{}", get_server_from_source(source), source.path),
            want_source
        );
        let a = m.get_attributes();
        assert!(!a.managed && !a.selinux_relabel);
        assert_eq!(
            plug.new_unmounter("vol1", "poduid").unwrap().get_path(),
            want
        );
    }

    #[tokio::test]
    async fn plugin_volume() {
        let v = vol(json!({"server": "localhost", "path": "/somepath", "readOnly": false}));
        do_test_plugin(&spec(&v, None, false), "localhost:/somepath").await;
    }

    #[tokio::test]
    async fn ipv6_volume_source() {
        let v = vol(json!({"server": "0:0:0:0:0:0:0:1", "path": "/somepath"}));
        do_test_plugin(&spec(&v, None, false), "[0:0:0:0:0:0:0:1]:/somepath").await;
    }

    #[tokio::test]
    async fn ipv4_volume_source() {
        let v = vol(json!({"server": "127.0.0.1", "path": "/somepath"}));
        do_test_plugin(&spec(&v, None, false), "127.0.0.1:/somepath").await;
    }

    /// `TestPluginPersistentVolume`.
    #[tokio::test]
    async fn plugin_persistent_volume() {
        let p = pv(json!({"metadata": {"name": "vol1"},
            "spec": {"nfs": {"server": "localhost", "path": "/somepath"}}}));
        let v = bare("vol1");
        do_test_plugin(&spec(&v, Some(&p), false), "localhost:/somepath").await;
    }

    /// `TestPersistentClaimReadOnlyFlag`: the claim's readOnly reaches the
    /// mounter through `Spec.ReadOnly`.
    #[tokio::test]
    async fn persistent_claim_read_only_flag() {
        let tmp = tempfile::tempdir().unwrap();
        let p =
            pv(json!({"metadata": {"name": "pvA"}, "spec": {"nfs": {"server": "", "path": ""}}}));
        let v = bare("pvA");
        let m = plugin(tmp.path())
            .new_mounter(&spec(&v, Some(&p), true), &pod())
            .await
            .unwrap();
        assert!(m.get_attributes().read_only);
    }

    /// `GetVolumeName` (`nfs.go:79-89`) and `getVolumeSource`'s error.
    #[test]
    fn volume_name_and_missing_source() {
        let tmp = tempfile::tempdir().unwrap();
        let plug = plugin(tmp.path());
        let v = vol(json!({"server": "s", "path": "/p"}));
        assert_eq!(
            plug.get_volume_name(&spec(&v, None, false)).unwrap(),
            "s//p"
        );
        let empty = bare("x");
        assert_eq!(
            plug.get_volume_name(&spec(&empty, None, false))
                .unwrap_err()
                .to_string(),
            "Spec does not reference a NFS volume type"
        );
    }

    /// `MountOptionFromSpec` reads `spec.mountOptions`; `SetUpAt` joins `ro`
    /// (`nfs.go:238-243`) through `JoinMountOptions` (sorted, de-duplicated).
    #[test]
    fn mount_options_come_from_the_pv() {
        let p = pv(json!({"metadata": {"name": "pv"},
            "spec": {"nfs": {"server": "s", "path": "/p"}, "mountOptions": ["vers=4.1", "hard"]}}));
        let v = bare("pv");
        assert_eq!(
            mount_option_from_spec(&spec(&v, Some(&p), false), &[]),
            vec!["hard", "vers=4.1"]
        );
        assert_eq!(
            join_mount_options(
                &["vers=4.1".into(), "hard".into(), "".into()],
                &["ro".into()]
            ),
            vec!["hard", "ro", "vers=4.1"]
        );
        assert!(mount_option_from_spec(&spec(&v, None, false), &[]).is_empty());
    }

    /// `ConstructVolumeSpec` (`nfs.go:175-187`).
    #[test]
    fn construct_volume_spec_uses_the_volume_name_as_path() {
        let tmp = tempfile::tempdir().unwrap();
        let rv = plugin(tmp.path())
            .construct_volume_spec("vol1", "/m")
            .unwrap();
        assert_eq!(rv.volume.name, "vol1");
        assert_eq!(rv.volume.nfs.unwrap().path, "vol1");
    }

    /// `TearDownAt` on a directory that is not a mount point removes it
    /// (`CleanupMountWithForce` -> `removePathIfNotMountPoint`), and skips a
    /// path that does not exist.
    #[tokio::test]
    async fn tear_down_removes_an_unmounted_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let u = plugin(tmp.path()).new_unmounter("vol1", "poduid").unwrap();
        let dir = u.get_path();
        std::fs::create_dir_all(&dir).unwrap();
        u.tear_down().await.unwrap();
        assert!(!std::path::Path::new(&dir).exists());
        u.tear_down().await.unwrap();
    }
}
