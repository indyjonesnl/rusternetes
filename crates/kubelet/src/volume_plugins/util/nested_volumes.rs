//! Port of `pkg/volume/util/nested_volumes.go`.

use rusternetes_common::resources::pod::VolumeMount;
use rusternetes_common::resources::Pod;

/// Lexical `filepath.Clean` for slash-separated paths (Go `path/filepath`,
/// Unix): collapses `//`, drops `.`, resolves `..` against earlier elements,
/// keeps a leading `..` on a relative path, and returns `.` for empty.
fn go_clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let rooted = path.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if out.last().is_some_and(|l| *l != "..") {
                    out.pop();
                } else if !rooted {
                    out.push("..");
                }
            }
            p => out.push(p),
        }
    }
    let joined = out.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".to_string(),
        (false, false) => joined,
    }
}

/// Port of `getNestedMountpoints` (`nested_volumes.go:35-99`): the mountpoint
/// directories, relative to the mount of volume `name`, that other volumes are
/// mounted beneath, across every container (init, regular and ephemeral —
/// `podutil.AllFeatureEnabledContainers()`).
fn get_nested_mountpoints(name: &str, pod: &Pod) -> anyhow::Result<Vec<String>> {
    let mut retval = Vec::new();
    let Some(spec) = &pod.spec else {
        return Ok(retval);
    };
    let mut mount_lists: Vec<&[VolumeMount]> = Vec::new();
    for c in spec.init_containers.iter().flatten() {
        mount_lists.push(c.volume_mounts.as_deref().unwrap_or_default());
    }
    for c in &spec.containers {
        mount_lists.push(c.volume_mounts.as_deref().unwrap_or_default());
    }
    for c in spec.ephemeral_containers.iter().flatten() {
        mount_lists.push(c.volume_mounts.as_deref().unwrap_or_default());
    }
    for mounts in mount_lists {
        let mut all: Vec<String> = Vec::new();
        let mut mine: Vec<String> = Vec::new();
        for vol in mounts {
            let cleaned = go_clean(&vol.mount_path);
            all.push(cleaned.clone());
            if vol.name == name {
                mine.push(cleaned);
            }
        }
        all.sort();
        for my in &mine {
            // "Don't let a container trick us into creating directories
            // outside of its rootfs"
            if my.starts_with("../") {
                anyhow::bail!("invalid container mount point {my}");
            }
            let my_slash = format!("{my}/");
            // Nested mountpoints can sort apart (`/dir/nested`,
            // `/dir/nested-vol`, `/dir/nested/double`), so keep every one
            // already taken, not just the last (nested_volumes.go:55-60).
            let mut prev: Vec<String> = Vec::new();
            for mp in &all {
                if !mp.starts_with(&my_slash) {
                    continue;
                }
                if prev.iter().any(|p| mp.starts_with(p.as_str())) {
                    continue;
                }
                prev.push(format!("{mp}/"));
                retval.push(mp[my_slash.len()..].to_string());
            }
        }
    }
    Ok(retval)
}

/// Port of `MakeNestedMountpoints` (`nested_volumes.go:102-114`): creates the
/// mountpoint directories (0755) in `base_dir` for volumes mounted beneath the
/// volume `name`, so the runtime can mount over them in a read-only volume.
pub fn make_nested_mountpoints(
    name: &str,
    base_dir: &std::path::Path,
    pod: &Pod,
) -> anyhow::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    for dir in get_nested_mountpoints(name, pod)? {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o755)
            .create(base_dir.join(&dir))
            .map_err(|e| anyhow::anyhow!("unable to create nested volume mountpoints: {e}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
