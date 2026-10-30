//! Unit tests moved from `src/volume_plugins/host.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::auth::TokenManager;
    pub use rusternetes_kubelet::volume_plugins::csi_driver_lister::CsiDriverLister;
    pub use rusternetes_kubelet::volume_plugins::util::hostutil::{HostUtil, HostUtils};
    pub use rusternetes_kubelet::volume_plugins::util::mount_ops::Mounter;
    pub use rusternetes_storage::StorageBackend;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::host::*;

    fn host() -> KubeletVolumeHost {
        KubeletVolumeHost::new_for_tests(
            "/var/lib/rusternetes".to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            std::collections::HashMap::new(),
        )
    }

    #[test]
    fn pods_dir_and_host_util() {
        let h = host();
        assert_eq!(h.get_pods_dir(), "/var/lib/rusternetes/pods");
        assert!(h.get_host_util().get_file_type("/tmp").is_ok());
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
    /// with — no re-derivation from `rusternetes_kubelet::pod_dirs::get_pods_dir()` or any
    /// other getter. This is the invariant a `.parent()`-based derivation
    /// would silently assume instead of stating.
    #[test]
    fn volumes_base_path_matches_constructor_argument() {
        assert_eq!(host().get_volumes_base_path(), "/var/lib/rusternetes");
    }
}
