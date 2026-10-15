use crate::volume_plugins::csi_driver_lister::CsiDriverLister;
use crate::volume_plugins::util::mount_ops::Mounter;
use rusternetes_common::auth::TokenManager;
use rusternetes_storage::StorageBackend;
use std::collections::HashMap;
use std::sync::Arc;

/// Port of `volume.VolumeHost` (`pkg/volume/plugins.go:341-422`).
///
/// The context object every plugin receives at `Init`. It exists because
/// upstream's plugins are standalone types with no back-reference to the
/// kubelet: everything shared — the client, the token minter, the directory
/// layout — reaches them through this one interface. Splitting
/// `create_volume`'s blocks into plugins needs exactly that, so the object is
/// a port, not a parameter bag invented for the refactor.
///
/// Only the methods with a consumer are ported. `GetMounter`, `GetHostUtil`,
/// `GetPluginDir`, `GetPodPluginDir`, `GetPodsDir` and the block-device
/// methods have no caller in epic #1970.
pub trait VolumeHost: Send + Sync {
    /// `GetPodVolumeDir` (`plugins.go:361`).
    fn get_pod_volume_dir(&self, pod_uid: &str, plugin_name: &str, volume_name: &str) -> String;

    /// `GetPodPluginDir` (`plugins.go:364`, `kubelet_getters.go:208-210`).
    /// `escaped_plugin_name` is already escaped, as at upstream's callers.
    fn get_pod_plugin_dir(&self, pod_uid: &str, escaped_plugin_name: &str) -> String;

    /// `GetPluginDir` (`plugins.go:346`, `kubelet_getters.go:83-85`):
    /// `<root>/plugins/<pluginName>`, the per-plugin global directory (the
    /// device mount paths of attachable plugins live under it). `plugin_name`
    /// is the unescaped plugin name, as upstream's callers pass it.
    fn get_plugin_dir(&self, plugin_name: &str) -> String;

    /// `GetVolumeDevicePluginDir` (`plugins.go:351`,
    /// `kubelet_getters.go:104-106`): `<root>/plugins/<pluginName>/volumeDevices`,
    /// the global map path root of a block-volume plugin.
    fn get_volume_device_plugin_dir(&self, plugin_name: &str) -> String;

    /// `GetPodVolumeDeviceDir` (`plugins.go:374`, `kubelet_getters.go:194-196`):
    /// `<root>/pods/<podUID>/volumeDevices/<escapedPluginName>`.
    fn get_pod_volume_device_dir(&self, pod_uid: &str, escaped_plugin_name: &str) -> String;

    /// `GetKubeClient` (`plugins.go:377`). Our equivalent of the API client is
    /// the storage backend the kubelet already reads objects through.
    fn get_kube_client(&self) -> Option<&Arc<StorageBackend>>;

    /// `GetServiceAccountTokenFunc` (`plugins.go:405`).
    fn get_service_account_token_func(&self) -> &TokenManager;

    /// `GetNodeAllocatable` (`plugins.go:397`). The kubelet implementation
    /// (`pkg/kubelet/volume_host.go:226-232`) looks the node up through a
    /// lister and returns `node.Status.Allocatable`.
    ///
    /// Expression difference, not a mechanism change: upstream is fallible
    /// because it fetches the node on every call; ours is constructed with
    /// the map already in hand (the kubelet already tracks it), so there is
    /// no lookup and therefore no error path — `Result<_, Error>` would have
    /// nothing to return but `Ok`.
    fn get_node_allocatable(&self) -> &HashMap<String, String>;

    /// Not a port — upstream's `VolumeHost` has no root-directory accessor
    /// (`pkg/volume/plugins.go` and `pkg/kubelet/volume_host.go` expose only
    /// derived dirs: `GetPluginDir`, `GetPodsDir`, `GetPodVolumeDir`,
    /// `GetPodPluginDir`, `GetVolumeDevicePluginDir`,
    /// `GetPodVolumeDeviceDir` — none return the bare root). This exists
    /// because the secret plugin's CA-cert injection is a Rusternetes-only
    /// mechanism with no upstream counterpart, and it needs the root to build
    /// a `_certs` path alongside `pods`. It returns the host's *stored* root
    /// rather than inverting `crate::pod_dirs::get_pods_dir()` (i.e.
    /// re-deriving it via `.parent()`), so the pod-directory layout keeps
    /// exactly one definition — [`crate::pod_dirs`] — instead of a second
    /// implicit one that silently breaks if the layout ever changes. That was #1967: two
    /// path builders disagreeing.
    fn get_volumes_base_path(&self) -> &str;

    /// `GetPodCertificateCredentialBundle` (`plugins.go` `KubeletVolumeHost`,
    /// implemented by `pkg/kubelet/volume_host.go`): the kubelet's pod
    /// certificate manager. `None` is upstream's `NoOpManager` situation
    /// (static / detached kubelet), whose bundle call fails "unimplemented".
    /// `VolumeHost.GetMounter` (`pkg/volume/plugins.go`): the kubelet's
    /// `mount.Interface`, `mount.New(s.ExperimentalMounterPath)`
    /// (`cmd/kubelet/app/server.go:487`).
    fn get_mounter(&self) -> &Mounter;

    fn pod_certificate_manager(&self) -> Option<&Arc<dyn crate::podcertificate::Manager>> {
        None
    }

    /// `KubeletVolumeHost.CSIDriverLister` (`plugins.go`,
    /// `pkg/kubelet/volume_host.go:166-168`). `None` is upstream's nil
    /// lister, the state `csiPlugin.Init` logs "CSIDriverLister not found"
    /// for (`csi_plugin.go:305-308`).
    fn csi_driver_lister(&self) -> Option<&CsiDriverLister> {
        None
    }
}

/// The kubelet's `VolumeHost`. Owns clones of the three values
/// `VolumeManager` already carries, plus the node allocatable map.
pub struct KubeletVolumeHost {
    volumes_base_path: String,
    storage: Option<Arc<StorageBackend>>,
    token_manager: TokenManager,
    node_allocatable: HashMap<String, String>,
    pod_certificate_manager: Option<Arc<dyn crate::podcertificate::Manager>>,
    csi_driver_lister: Option<CsiDriverLister>,
    mounter: Mounter,
}

impl KubeletVolumeHost {
    pub fn new(
        volumes_base_path: String,
        storage: Option<Arc<StorageBackend>>,
        token_manager: TokenManager,
        node_allocatable: HashMap<String, String>,
    ) -> Self {
        Self {
            volumes_base_path,
            storage,
            token_manager,
            node_allocatable,
            pod_certificate_manager: None,
            csi_driver_lister: None,
            // `mount.New("")`: systemd tried, safe-not-mounted detected
            // (`mount_linux.go:79-85`).
            mounter: Mounter::new(
                "",
                true,
                crate::volume_plugins::util::mount_ops::detect_safe_not_mounted_behavior(),
            ),
        }
    }

    /// Attach the `CSIDriver` lister (`kubeletVolumeHost.csiDriverLister`,
    /// `volume_host.go:166`).
    pub fn with_csi_driver_lister(mut self, lister: CsiDriverLister) -> Self {
        self.csi_driver_lister = Some(lister);
        self
    }

    /// Attach the kubelet's pod certificate manager
    /// (`kubeletVolumeHost.podCertificateManager`, `volume_host.go`).
    pub fn with_pod_certificate_manager(
        mut self,
        manager: Arc<dyn crate::podcertificate::Manager>,
    ) -> Self {
        self.pod_certificate_manager = Some(manager);
        self
    }
}

impl VolumeHost for KubeletVolumeHost {
    fn csi_driver_lister(&self) -> Option<&CsiDriverLister> {
        self.csi_driver_lister.as_ref()
    }

    fn get_pod_volume_dir(&self, pod_uid: &str, plugin_name: &str, volume_name: &str) -> String {
        crate::pod_dirs::get_pod_volume_dir(
            &self.volumes_base_path,
            pod_uid,
            plugin_name,
            volume_name,
        )
        .to_string_lossy()
        .into_owned()
    }

    fn get_pod_plugin_dir(&self, pod_uid: &str, escaped_plugin_name: &str) -> String {
        crate::pod_dirs::get_pod_plugin_dir(&self.volumes_base_path, pod_uid, escaped_plugin_name)
            .to_string_lossy()
            .into_owned()
    }

    fn get_plugin_dir(&self, plugin_name: &str) -> String {
        std::path::Path::new(&self.volumes_base_path)
            .join("plugins")
            .join(plugin_name)
            .to_string_lossy()
            .into_owned()
    }

    fn get_volume_device_plugin_dir(&self, plugin_name: &str) -> String {
        std::path::Path::new(&self.get_plugin_dir(plugin_name))
            .join("volumeDevices")
            .to_string_lossy()
            .into_owned()
    }

    fn get_pod_volume_device_dir(&self, pod_uid: &str, escaped_plugin_name: &str) -> String {
        crate::pod_dirs::get_pod_dir(&self.volumes_base_path, pod_uid)
            .join("volumeDevices")
            .join(escaped_plugin_name)
            .to_string_lossy()
            .into_owned()
    }

    fn get_kube_client(&self) -> Option<&Arc<StorageBackend>> {
        self.storage.as_ref()
    }

    fn get_service_account_token_func(&self) -> &TokenManager {
        &self.token_manager
    }

    fn get_node_allocatable(&self) -> &HashMap<String, String> {
        &self.node_allocatable
    }

    fn get_volumes_base_path(&self) -> &str {
        &self.volumes_base_path
    }

    fn get_mounter(&self) -> &Mounter {
        &self.mounter
    }

    fn pod_certificate_manager(&self) -> Option<&Arc<dyn crate::podcertificate::Manager>> {
        self.pod_certificate_manager.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host() -> KubeletVolumeHost {
        KubeletVolumeHost::new(
            "/var/lib/rusternetes".to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            std::collections::HashMap::new(),
        )
    }

    /// The host is the only way a plugin learns where its directory goes, so
    /// it must agree with `pod_dirs` exactly — the layout bug #1967 was two
    /// path builders disagreeing.
    #[test]
    fn pod_volume_dir_matches_pod_dirs() {
        let got = host().get_pod_volume_dir("uid-1", "kubernetes.io/empty-dir", "data");
        assert_eq!(
            got,
            "/var/lib/rusternetes/pods/uid-1/volumes/kubernetes.io~empty-dir/data"
        );
    }

    /// The accessor must return exactly the root the host was constructed
    /// with — no re-derivation from `crate::pod_dirs::get_pods_dir()` or any
    /// other getter. This is the invariant a `.parent()`-based derivation
    /// would silently assume instead of stating.
    #[test]
    fn volumes_base_path_matches_constructor_argument() {
        assert_eq!(host().get_volumes_base_path(), "/var/lib/rusternetes");
    }
}
