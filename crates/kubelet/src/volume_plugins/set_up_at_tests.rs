//! `Mounter::SetUpAt(dir, MounterArgs)` (#2781, split from #2687).
//!
//! Upstream: `pkg/volume/volume.go:175` (`SetUp(MounterArgs)`), `:184`
//! (`SetUpAt(dir, MounterArgs)`); every plugin's `SetUp` is
//! `SetUpAt(GetPath(), args)` (e.g. `pkg/volume/emptydir/empty_dir.go`
//! `SetUp`); hostPath's `SetUpAt` is `"SetUpAt() does not make sense for host
//! paths"` (`pkg/volume/hostpath/host_path.go:257-260`).

use super::empty_dir::EmptyDirPlugin;
use super::host_path::HostPathPlugin;
use super::plugin::{Mounter, MounterArgs, Spec, VolumePlugin};
use rusternetes_common::resources::{Pod, Volume};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

fn host() -> Arc<crate::volume_plugins::KubeletVolumeHost> {
    Arc::new(crate::volume_plugins::KubeletVolumeHost::new(
        "/var/lib/rusternetes".to_string(),
        None,
        rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        HashMap::new(),
    ))
}

fn pod() -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
        "spec": {"containers": []}
    }))
    .unwrap()
}

async fn mounter_for(plugin: &dyn VolumePlugin, v: &Volume) -> Box<dyn Mounter> {
    let spec = Spec {
        volume: v,
        persistent_volume: None,
        read_only: false,
    };
    plugin.new_mounter(&spec, &pod()).await.unwrap()
}

/// `SetUpAt` sets up the volume at the GIVEN directory, not at `GetPath()`.
#[tokio::test]
async fn set_up_at_sets_up_the_given_dir_not_get_path() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("elsewhere");
    let v: Volume = serde_json::from_value(json!({"name": "ed", "emptyDir": {}})).unwrap();
    let m = mounter_for(&EmptyDirPlugin::new(host()), &v).await;
    m.set_up_at(dir.to_str().unwrap(), &MounterArgs::default())
        .await
        .unwrap();
    assert!(dir.is_dir(), "SetUpAt(dir) must create dir");
    assert!(
        !std::path::Path::new(&m.get_path()).exists(),
        "SetUpAt(dir) must not touch GetPath()"
    );
}

/// `MounterArgs.FsGroup` reaches the mounter per call (`volume.go:132`), not
/// from a field captured at construction: the pod here has no fsGroup.
#[tokio::test]
async fn set_up_at_applies_the_fs_group_from_mounter_args() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("vol");
    std::fs::create_dir_all(&dir).unwrap();
    let f = dir.join("f");
    std::fs::write(&f, b"x").unwrap();
    std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o400)).unwrap();
    let gid = std::fs::metadata(&dir).unwrap().gid() as i64;
    let v: Volume = serde_json::from_value(json!({"name": "ed", "emptyDir": {}})).unwrap();
    let m = mounter_for(&EmptyDirPlugin::new(host()), &v).await;
    let args = MounterArgs {
        fs_group: Some(gid),
        ..Default::default()
    };
    m.set_up_at(dir.to_str().unwrap(), &args).await.unwrap();
    assert_eq!(
        std::fs::metadata(&f).unwrap().permissions().mode() & 0o7777,
        0o660
    );
}

/// `host_path.go:257-260`.
#[tokio::test]
async fn host_path_set_up_at_does_not_make_sense() {
    let v: Volume =
        serde_json::from_value(json!({"name": "hp", "hostPath": {"path": "/tmp"}})).unwrap();
    let m = mounter_for(&HostPathPlugin::new(host()), &v).await;
    let err = m
        .set_up_at("/tmp/x", &MounterArgs::default())
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "SetUpAt() does not make sense for host paths"
    );
}

/// `SetUp(args)` is `SetUpAt(GetPath(), args)` (`volume.go:175`).
#[tokio::test]
async fn set_up_with_args_delegates_to_set_up_at_get_path() {
    use std::sync::Mutex;
    struct Rec(Mutex<Vec<(String, Option<i64>)>>);
    #[async_trait::async_trait]
    impl Mounter for Rec {
        fn get_path(&self) -> String {
            "/the/path".into()
        }
        async fn set_up_at(&self, dir: &str, args: &MounterArgs) -> anyhow::Result<()> {
            self.0.lock().unwrap().push((dir.into(), args.fs_group));
            Ok(())
        }
        fn get_attributes(&self) -> super::plugin::Attributes {
            Default::default()
        }
    }
    let r = Rec(Mutex::new(vec![]));
    let args = MounterArgs {
        fs_group: Some(7),
        ..Default::default()
    };
    r.set_up_with(&args).await.unwrap();
    r.set_up().await.unwrap();
    assert_eq!(
        *r.0.lock().unwrap(),
        vec![("/the/path".into(), Some(7)), ("/the/path".into(), None)]
    );
}
