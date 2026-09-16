use rusternetes_common::auth::TokenManager;
use rusternetes_storage::StorageBackend;
use std::collections::HashMap;
use std::path::PathBuf;
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
/// `GetPluginDir`, `GetPodPluginDir` and the block-device methods have no
/// caller in epic #1970.
pub trait VolumeHost: Send + Sync {
    /// `GetPodsDir` (`plugins.go:355`).
    fn get_pods_dir(&self) -> PathBuf;

    /// `GetPodVolumeDir` (`plugins.go:361`).
    fn get_pod_volume_dir(&self, pod_uid: &str, plugin_name: &str, volume_name: &str) -> String;

    /// `GetKubeClient` (`plugins.go:377`). Our equivalent of the API client is
    /// the storage backend the kubelet already reads objects through.
    fn get_kube_client(&self) -> Option<&Arc<StorageBackend>>;

    /// `GetServiceAccountTokenFunc` (`plugins.go:405`).
    fn get_service_account_token_func(&self) -> &TokenManager;

    /// The node's `status.allocatable`.
    ///
    /// DEVIATION (CLAUDE.md rule 8): upstream has no `VolumeHost` method for
    /// this. It defaults an unset downwardAPI `limits.*` in the kubelet before
    /// the plugin runs — `defaultPodLimitsForDownwardAPI`
    /// (`pkg/kubelet/kubelet_resources.go:43-47`) mutates the pod. Our
    /// downwardAPI block does it inline instead, so the value has to reach the
    /// plugin somehow. Moving the defaulting to the kubelet is a behaviour
    /// change and belongs in the per-plugin fidelity follow-up, not here.
    fn node_allocatable(&self) -> &HashMap<String, String>;
}

/// The kubelet's `VolumeHost`. Owns clones of the three values
/// `VolumeManager` already carries, plus the node allocatable map.
pub struct KubeletVolumeHost {
    volumes_base_path: String,
    storage: Option<Arc<StorageBackend>>,
    token_manager: TokenManager,
    node_allocatable: HashMap<String, String>,
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
        }
    }
}

impl VolumeHost for KubeletVolumeHost {
    fn get_pods_dir(&self) -> PathBuf {
        crate::pod_dirs::get_pods_dir(&self.volumes_base_path)
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

    fn get_kube_client(&self) -> Option<&Arc<StorageBackend>> {
        self.storage.as_ref()
    }

    fn get_service_account_token_func(&self) -> &TokenManager {
        &self.token_manager
    }

    fn node_allocatable(&self) -> &HashMap<String, String> {
        &self.node_allocatable
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

    #[test]
    fn pods_dir_matches_pod_dirs() {
        assert_eq!(
            host().get_pods_dir(),
            std::path::PathBuf::from("/var/lib/rusternetes/pods")
        );
    }
}
