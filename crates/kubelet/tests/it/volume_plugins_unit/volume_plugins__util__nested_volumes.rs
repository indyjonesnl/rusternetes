//! Unit tests moved from `src/volume_plugins/util/nested_volumes.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::pod::VolumeMount;
    pub use rusternetes_common::resources::Pod;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::nested_volumes::*;
    use serde_json::json;

    fn pod(mounts: serde_json::Value) -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "u"},
            "spec": {"containers": [{"name": "c", "image": "i", "volumeMounts": mounts}]}
        }))
        .unwrap()
    }

    #[test]
    fn nests_only_first_level_and_handles_sort_order() {
        let p = pod(json!([
            {"name": "v", "mountPath": "/dir"},
            {"name": "a", "mountPath": "/dir/nested"},
            {"name": "b", "mountPath": "/dir/nested-vol"},
            {"name": "c", "mountPath": "/dir/nested/double"},
            {"name": "d", "mountPath": "/dir2"},
        ]));
        let mut got = get_nested_mountpoints("v", &p).unwrap();
        got.sort();
        assert_eq!(got, vec!["nested", "nested-vol"]);
    }

    #[test]
    fn rejects_a_mount_path_escaping_the_rootfs() {
        let p = pod(json!([{"name": "v", "mountPath": "../../etc"}]));
        assert!(get_nested_mountpoints("v", &p).is_err());
    }

    #[test]
    fn creates_the_mountpoints_0755() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let p = pod(json!([
            {"name": "v", "mountPath": "/dir"},
            {"name": "a", "mountPath": "/dir/x/y"},
        ]));
        make_nested_mountpoints("v", tmp.path(), &p).unwrap();
        let m = std::fs::metadata(tmp.path().join("x/y")).unwrap();
        assert!(m.is_dir());
        assert_eq!(m.permissions().mode() & 0o777, 0o755);
    }

    #[test]
    fn go_clean_matches_filepath_clean() {
        assert_eq!(go_clean("/a//b/./c/../d/"), "/a/b/d");
        assert_eq!(go_clean(""), ".");
        assert_eq!(go_clean("../x"), "../x");
        assert_eq!(go_clean("/.."), "/");
    }
}
