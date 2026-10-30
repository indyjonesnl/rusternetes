//! Unit tests moved from `src/pod_dirs.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::path::{Path, PathBuf};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::pod_dirs::*;

    #[test]
    fn a_volume_dir_follows_the_upstream_layout() {
        assert_eq!(
            get_pod_volume_dir("/var/lib/kubelet", "uid-1", plugin::CONFIG_MAP, "cfg"),
            Path::new("/var/lib/kubelet/pods/uid-1/volumes/kubernetes.io~configmap/cfg"),
        );
    }

    #[test]
    fn a_plugin_name_is_escaped_on_disk() {
        // EscapeQualifiedName replaces every "/" with "~".
        assert_eq!(
            escape_qualified_name("kubernetes.io/empty-dir"),
            "kubernetes.io~empty-dir"
        );
        assert_eq!(escape_qualified_name("a/b/c"), "a~b~c");
        assert_eq!(escape_qualified_name("no-slash"), "no-slash");
    }

    /// The reason upstream keys on UID rather than name: a pod recreated under
    /// the same name is a different pod, and must not inherit the previous
    /// incarnation's files.
    #[test]
    fn a_recreated_pod_with_the_same_name_gets_a_distinct_dir() {
        let first = get_pod_volume_dir("/root", "uid-aaa", plugin::EMPTY_DIR, "scratch");
        let second = get_pod_volume_dir("/root", "uid-bbb", plugin::EMPTY_DIR, "scratch");
        assert_ne!(first, second);
    }

    /// Two volumes of different kinds may share a name; the plugin segment is
    /// what keeps them apart on disk.
    #[test]
    fn same_volume_name_under_different_plugins_does_not_collide() {
        let cm = get_pod_volume_dir("/root", "uid-1", plugin::CONFIG_MAP, "shared");
        let secret = get_pod_volume_dir("/root", "uid-1", plugin::SECRET, "shared");
        assert_ne!(cm, secret);
    }

    #[test]
    fn the_per_pod_dirs_sit_under_the_pods_dir() {
        assert_eq!(get_pods_dir("/root"), Path::new("/root/pods"));
        assert_eq!(get_pod_dir("/root", "u"), Path::new("/root/pods/u"));
        assert_eq!(
            get_pod_volumes_dir("/root", "u"),
            Path::new("/root/pods/u/volumes")
        );
    }
}

mod volumes_from_pod_dir_tests {
    use super::__parent::*;
    use rusternetes_kubelet::pod_dirs::*;

    /// `getVolumesFromPodDir` (`reconstruct_common.go:191-240`): one entry per
    /// `<pod>/volumes/<escaped plugin>/<volume>`, plugin name unescaped; a pod
    /// without a volumes dir has none.
    #[test]
    fn lists_every_volume_with_its_unescaped_plugin() {
        let root = std::env::temp_dir().join(format!("vfpd-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let root_s = root.to_string_lossy().into_owned();
        for (plugin, vol) in [
            (plugin::EMPTY_DIR, "a"),
            (plugin::EMPTY_DIR, "b"),
            (plugin::CSI, "pvc-1"),
        ] {
            std::fs::create_dir_all(get_pod_volume_dir(&root_s, "u1", plugin, vol)).unwrap();
        }
        std::fs::create_dir_all(get_pod_dir(&root_s, "u2")).unwrap();

        let mut got = get_volumes_from_pod_dir(&root_s, "u1").unwrap();
        got.sort_by(|a, b| a.volume_path.cmp(&b.volume_path));

        let names: Vec<_> = got
            .iter()
            .map(|v| (v.plugin_name.as_str(), v.volume_spec_name.as_str()))
            .collect();
        assert_eq!(
            names,
            vec![
                (plugin::CSI, "pvc-1"),
                (plugin::EMPTY_DIR, "a"),
                (plugin::EMPTY_DIR, "b")
            ]
        );
        assert!(got.iter().all(|v| v.pod_uid == "u1"));
        assert_eq!(
            got[1].volume_path,
            get_pod_volume_dir(&root_s, "u1", plugin::EMPTY_DIR, "a")
        );
        assert!(get_volumes_from_pod_dir(&root_s, "u2").unwrap().is_empty());
        assert!(get_volumes_from_pod_dir(&root_s, "nope")
            .unwrap()
            .is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }
}
