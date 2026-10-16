//! Tests for the CSI block-volume plugin, ported in shape from
//! `pkg/volume/csi/csi_block_test.go` (`TestBlockMapperGetGlobalMapPath`,
//! `TestBlockMapperGetStagingPath`, `TestBlockMapperGetPublishPath`,
//! `TestBlockMapperGetPodDeviceMapPath`, `TestBlockMapperSetupDevice`,
//! `TestBlockMapperMapPodDevice`, `TestBlockMapperTearDownDevice`) and
//! `csi_plugin_test.go` (`TestPluginNewBlockMapper*`, `TestPluginConstructBlockVolumeSpec`).
//!
//! Upstream drives a fake in-process `csiClient`; here the driver is a real
//! gRPC `csi.v1.Node` server on a unix socket (`csi_client::fake`).

use super::*;
use crate::volume_plugins::csi_client::fake::{serve, FakeDriver};
use crate::volume_plugins::csi_client::proto::node_service_capability::rpc::Type as Cap;
use crate::volume_plugins::csi_client::proto::volume_capability::AccessType;
use crate::volume_plugins::csi_client::CsiError;
use crate::volume_plugins::csi_driver_lister::CsiDriverLister;
use crate::volume_plugins::csi_drivers_store::{csi_drivers, Driver};
use crate::volume_plugins::{KubeletVolumeHost, VolumePlugin};
use rusternetes_common::resources::{PersistentVolume, Secret, Volume};
use rusternetes_storage::MemoryStorage;
use serde_json::json;

struct Fx {
    _dir: tempfile::TempDir,
    driver: String,
    fake: FakeDriver,
    storage: Arc<StorageBackend>,
    plugin: CsiPlugin,
    _srv: tokio::task::JoinHandle<()>,
}

/// A kubelet root in a tempdir, an in-memory API, a CSIDriver lister holding
/// a (synced) CSIDriver with `csi_driver_spec` and a fake driver registered
/// under a test-unique name.
async fn fx(name: &str, caps: &[Cap], csi_driver_spec: serde_json::Value) -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("kubelet").to_string_lossy().to_string();
    let sock = dir.path().join("csi.sock");
    let driver = format!("{name}.csi.example.com");
    let fake = FakeDriver::with_capabilities(caps);
    let srv = serve(fake.clone(), &sock);
    csi_drivers().set(
        &driver,
        Driver {
            endpoint: sock.to_string_lossy().to_string(),
            highest_supported_version: "1.0.0".into(),
        },
    );
    let storage = Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new())));
    let lister = CsiDriverLister::new();
    lister.insert(
        serde_json::from_value(
            json!({"apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
            "metadata": {"name": driver}, "spec": csi_driver_spec}),
        )
        .unwrap(),
    );
    lister.mark_synced();
    let host = Arc::new(
        KubeletVolumeHost::new(
            root,
            Some(storage.clone()),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        )
        .with_csi_driver_lister(lister),
    );
    Fx {
        _dir: dir,
        driver,
        fake,
        storage,
        plugin: CsiPlugin::new(host),
        _srv: srv,
    }
}

fn pod() -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": "p", "namespace": "ns1", "uid": "pod-uid"},
        "spec": {"containers": [], "nodeName": "node-1", "serviceAccountName": "sa-1"}
    }))
    .unwrap()
}

fn block_pv(driver: &str, extra_csi: serde_json::Value) -> PersistentVolume {
    let mut csi = json!({"driver": driver, "volumeHandle": "vol-1"});
    for (k, v) in extra_csi.as_object().unwrap() {
        csi[k] = v.clone();
    }
    serde_json::from_value(json!({
        "metadata": {"name": "pv1"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteMany"],
            "volumeMode": "Block",
            "csi": csi
        }
    }))
    .unwrap()
}

fn claim_volume() -> Volume {
    serde_json::from_value(json!({"name": "data", "persistentVolumeClaim": {"claimName": "c"}}))
        .unwrap()
}

async fn put_attachment(storage: &Arc<StorageBackend>, driver: &str, metadata: serde_json::Value) {
    let name = get_attachment_name("vol-1", driver, "node-1");
    let va = json!({"apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
        "metadata": {"name": name},
        "spec": {"attacher": driver, "nodeName": "node-1", "source": {"persistentVolumeName": "pv1"}},
        "status": {"attached": true, "attachmentMetadata": metadata}});
    storage
        .create(&build_key("volumeattachments", None, &name), &va)
        .await
        .unwrap();
}

fn device_root(f: &Fx) -> String {
    f.plugin
        .host
        .get_volume_device_plugin_dir("kubernetes.io/csi")
}

/// `volume.BlockVolumePlugin` is an optional interface of the plugin
/// (`plugins.go:906`, `:919`): the CSI plugin must opt in, as upstream's
/// `var _ volume.BlockVolumePlugin = &csiPlugin{}` (`csi_plugin.go:727`).
#[tokio::test]
async fn csi_plugin_is_a_block_volume_plugin() {
    let f = fx("blockplugin", &[], json!({"attachRequired": false})).await;
    assert!(f.plugin.as_block_volume_plugin().is_some());
}

/// `TestPluginNewBlockMapper` / `TestBlockMapperGetGlobalMapPath` /
/// `GetStagingPath` / `getPublishPath` / `GetPodDeviceMapPath`: the path
/// layout of `csi_block.go:17-70`, and the volume info the mapper persists
/// for `NewBlockVolumeUnmapper` (`csi_plugin.go:784-803`).
#[tokio::test]
async fn mapper_paths_and_persisted_volume_data() {
    let f = fx("blockpaths", &[], json!({"attachRequired": false})).await;
    let (v, pv, pod) = (claim_volume(), block_pv(&f.driver, json!({})), pod());
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&pv),
        read_only: false,
    };
    let bp = f.plugin.as_block_volume_plugin().unwrap();
    let mapper = bp.new_block_volume_mapper(&spec, &pod).unwrap();
    let root = device_root(&f);

    assert_eq!(
        mapper.get_global_map_path(&spec).unwrap(),
        format!("{root}/pv1/dev")
    );
    let (dir, name) = mapper.get_pod_device_map_path();
    assert!(
        dir.ends_with("/pods/pod-uid/volumeDevices/kubernetes.io~csi"),
        "{dir}"
    );
    assert_eq!(name, "pv1");
    assert!(mapper.supports_metrics());
    let custom = mapper.as_custom_block_volume_mapper().unwrap();
    assert_eq!(custom.get_staging_path(), format!("{root}/staging/pv1"));

    // `saveVolumeData(dataDir, ..)`: <specName>/data/vol_data.json.
    let data = load_volume_data(Path::new(&format!("{root}/pv1/data"))).unwrap();
    assert_eq!(data[vol_data_key::SPEC_VOL_ID], "pv1");
    assert_eq!(data[vol_data_key::VOL_HANDLE], "vol-1");
    assert_eq!(data[vol_data_key::DRIVER_NAME], f.driver);
    assert_eq!(data[vol_data_key::NODE_NAME], "node-1");
    assert_eq!(
        data[vol_data_key::ATTACHMENT_ID],
        get_attachment_name("vol-1", &f.driver, "node-1")
    );

    // `NewBlockVolumeUnmapper` reloads driver and handle from it.
    let unmapper = bp.new_block_volume_unmapper("pv1", "pod-uid").unwrap();
    assert!(unmapper.as_custom_block_volume_unmapper().is_some());
    assert_eq!(
        unmapper.get_global_map_path(&spec).unwrap(),
        format!("{root}/pv1/dev")
    );
}

/// `NewBlockVolumeMapper` needs a CSI PV source (`getCSISourceFromSpec`).
#[tokio::test]
async fn new_block_volume_mapper_rejects_a_non_csi_spec() {
    let f = fx("blockreject", &[], json!({})).await;
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: None,
        read_only: false,
    };
    let bp = f.plugin.as_block_volume_plugin().unwrap();
    assert!(bp.new_block_volume_mapper(&spec, &pod()).is_err());
}

/// `TestPluginConstructBlockVolumeSpec` (`csi_plugin_test.go`).
#[tokio::test]
async fn construct_block_volume_spec_rebuilds_a_block_pv() {
    let f = fx("blockconstruct", &[], json!({"attachRequired": false})).await;
    let (v, pv, pod) = (claim_volume(), block_pv(&f.driver, json!({})), pod());
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&pv),
        read_only: false,
    };
    let bp = f.plugin.as_block_volume_plugin().unwrap();
    bp.new_block_volume_mapper(&spec, &pod).unwrap();

    let owned = bp
        .construct_block_volume_spec("pod-uid", "pv1", "/ignored")
        .unwrap();
    let pv = owned.persistent_volume.unwrap();
    assert_eq!(pv.metadata.name, "pv1");
    let csi = pv.spec.csi.unwrap();
    assert_eq!(csi.driver, f.driver);
    assert_eq!(csi.volume_handle.as_deref(), Some("vol-1"));
    assert_eq!(
        serde_json::to_value(&pv.spec.volume_mode).unwrap(),
        json!("Block")
    );

    // Nothing persisted for an unknown volume.
    assert!(bp
        .construct_block_volume_spec("pod-uid", "nope", "/ignored")
        .is_err());
}

/// `TestBlockMapperSetupDevice`: `NodeStageVolume` with a block capability
/// on the staging path, carrying the attachment's metadata as publish context
/// and the node-stage secret (`csi_block.go:140-197`).
#[tokio::test]
async fn set_up_device_stages_a_block_volume() {
    let f = fx("blockstage", &[Cap::StageUnstageVolume], json!({})).await;
    put_attachment(&f.storage, &f.driver, json!({"devicePath": "/dev/x"})).await;
    let sec = Secret::new("stage-secret", "sec-ns")
        .with_data(HashMap::from([("k".to_string(), b"v".to_vec())]));
    f.storage
        .create(&build_key("secrets", Some("sec-ns"), "stage-secret"), &sec)
        .await
        .unwrap();
    let (v, pod) = (claim_volume(), pod());
    let pv = block_pv(
        &f.driver,
        json!({"volumeAttributes": {"a": "b"},
               "nodeStageSecretRef": {"name": "stage-secret", "namespace": "sec-ns"}}),
    );
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&pv),
        read_only: false,
    };
    let bp = f.plugin.as_block_volume_plugin().unwrap();
    let mapper = bp.new_block_volume_mapper(&spec, &pod).unwrap();
    let custom = mapper.as_custom_block_volume_mapper().unwrap();

    let staging = custom.set_up_device().await.unwrap();
    assert_eq!(staging, custom.get_staging_path());
    assert!(Path::new(&staging).is_dir(), "stagingPath is created first");

    let calls = f.fake.calls.lock().unwrap();
    assert_eq!(calls.stage.len(), 1);
    let req = &calls.stage[0];
    assert_eq!(req.volume_id, "vol-1");
    assert_eq!(req.staging_target_path, staging);
    assert_eq!(req.publish_context["devicePath"], "/dev/x");
    assert_eq!(req.volume_context["a"], "b");
    assert_eq!(req.secrets["k"], "v");
    assert!(matches!(
        req.volume_capability.as_ref().unwrap().access_type,
        Some(AccessType::Block(_))
    ));
    assert!(calls.publish.is_empty());
}

/// Without `STAGE_UNSTAGE_VOLUME` nothing is staged and the staging path is
/// empty (`csi_block.go:155-159`).
#[tokio::test]
async fn set_up_device_without_stage_unstage_skips_staging() {
    let f = fx("blocknostage", &[], json!({"attachRequired": false})).await;
    let (v, pv, pod) = (claim_volume(), block_pv(&f.driver, json!({})), pod());
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&pv),
        read_only: false,
    };
    let mapper = f
        .plugin
        .as_block_volume_plugin()
        .unwrap()
        .new_block_volume_mapper(&spec, &pod)
        .unwrap();
    let staging = mapper
        .as_custom_block_volume_mapper()
        .unwrap()
        .set_up_device()
        .await
        .unwrap();
    assert_eq!(staging, "");
    assert!(f.fake.calls.lock().unwrap().stage.is_empty());
}

/// A missing VolumeAttachment is a transient failure (`csi_block.go:279`).
#[tokio::test]
async fn set_up_device_missing_attachment_is_transient() {
    let f = fx("blocknoattach", &[Cap::StageUnstageVolume], json!({})).await;
    let (v, pv, pod) = (claim_volume(), block_pv(&f.driver, json!({})), pod());
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&pv),
        read_only: false,
    };
    let mapper = f
        .plugin
        .as_block_volume_plugin()
        .unwrap()
        .new_block_volume_mapper(&spec, &pod)
        .unwrap();
    let err = mapper
        .as_custom_block_volume_mapper()
        .unwrap()
        .set_up_device()
        .await
        .unwrap_err();
    assert!(
        !err.downcast_ref::<CsiError>()
            .unwrap()
            .is_operation_finished(),
        "{err}"
    );
    assert!(
        err.to_string().contains("failed to get volume attachment"),
        "{err}"
    );
    assert!(f.fake.calls.lock().unwrap().stage.is_empty());
}

/// `TestBlockMapperMapPodDevice`: `NodePublishVolume` to
/// `publish/{specName}/{podUID}` with the staging path, a block capability and
/// the pod-info attributes the CSIDriver asks for (`csi_block.go:200-269`).
#[tokio::test]
async fn map_pod_device_publishes_a_block_volume() {
    let f = fx(
        "blockpublish",
        &[Cap::StageUnstageVolume],
        json!({"podInfoOnMount": true}),
    )
    .await;
    put_attachment(&f.storage, &f.driver, json!({"devicePath": "/dev/x"})).await;
    let (v, pod) = (claim_volume(), pod());
    let pv = block_pv(&f.driver, json!({"volumeAttributes": {"a": "b"}}));
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&pv),
        read_only: true,
    };
    let mapper = f
        .plugin
        .as_block_volume_plugin()
        .unwrap()
        .new_block_volume_mapper(&spec, &pod)
        .unwrap();
    let custom = mapper.as_custom_block_volume_mapper().unwrap();

    let publish = custom.map_pod_device().await.unwrap();
    assert_eq!(publish, format!("{}/publish/pv1/pod-uid", device_root(&f)));

    let calls = f.fake.calls.lock().unwrap();
    let req = &calls.publish[0];
    assert_eq!(req.volume_id, "vol-1");
    assert_eq!(req.target_path, publish);
    assert_eq!(req.staging_target_path, custom.get_staging_path());
    assert!(req.readonly);
    assert_eq!(req.publish_context["devicePath"], "/dev/x");
    assert_eq!(req.volume_context["a"], "b");
    assert_eq!(req.volume_context["csi.storage.k8s.io/pod.name"], "p");
    assert_eq!(
        req.volume_context["csi.storage.k8s.io/pod.namespace"],
        "ns1"
    );
    assert_eq!(req.volume_context["csi.storage.k8s.io/pod.uid"], "pod-uid");
    assert_eq!(req.volume_context["csi.storage.k8s.io/ephemeral"], "false");
    assert!(matches!(
        req.volume_capability.as_ref().unwrap().access_type,
        Some(AccessType::Block(_))
    ));
}

/// `TestBlockMapperTearDownDevice` / `UnmapPodDevice`: `NodeUnpublishVolume`
/// on the publish path, then `NodeUnstageVolume` on the staging path, then the
/// orphan files and the per-volume directory are removed
/// (`csi_block.go:365-496`), all through the unmapper rebuilt from disk.
#[tokio::test]
async fn unmapper_unpublishes_unstages_and_cleans_up() {
    let f = fx("blockteardown", &[Cap::StageUnstageVolume], json!({})).await;
    put_attachment(&f.storage, &f.driver, json!({})).await;
    let (v, pv, pod) = (claim_volume(), block_pv(&f.driver, json!({})), pod());
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&pv),
        read_only: false,
    };
    let bp = f.plugin.as_block_volume_plugin().unwrap();
    let mapper = bp.new_block_volume_mapper(&spec, &pod).unwrap();
    let custom = mapper.as_custom_block_volume_mapper().unwrap();
    let staging = custom.set_up_device().await.unwrap();
    let publish = custom.map_pod_device().await.unwrap();

    let unmapper = bp.new_block_volume_unmapper("pv1", "pod-uid").unwrap();
    let custom = unmapper.as_custom_block_volume_unmapper().unwrap();
    custom.unmap_pod_device().await.unwrap();
    {
        let calls = f.fake.calls.lock().unwrap();
        assert_eq!(calls.unpublish.len(), 1);
        assert_eq!(calls.unpublish[0].volume_id, "vol-1");
        assert_eq!(calls.unpublish[0].target_path, publish);
    }

    // The driver owns publishPath; the fake leaves the dir it created, so
    // remove it as a conformant driver's NodeUnpublishVolume would.
    std::fs::remove_dir_all(&publish).unwrap();
    custom.tear_down_device("", "").await.unwrap();
    let calls = f.fake.calls.lock().unwrap();
    assert_eq!(calls.unstage.len(), 1);
    assert_eq!(calls.unstage[0].staging_target_path, staging);
    assert!(!Path::new(&staging).exists());
    let root = device_root(&f);
    assert!(!Path::new(&format!("{root}/publish/pv1")).exists());
    assert!(
        !Path::new(&format!("{root}/pv1")).exists(),
        "cleanupOrphanDeviceFiles removes everything under <volume name>"
    );
}

/// `TearDownDevice` of an already-deleted staging path skips
/// `NodeUnstageVolume` (`csi_block.go:428-431`).
#[tokio::test]
async fn tear_down_device_skips_unstage_when_staging_path_is_gone() {
    let f = fx(
        "blockgone",
        &[Cap::StageUnstageVolume],
        json!({"attachRequired": false}),
    )
    .await;
    let (v, pv, pod) = (claim_volume(), block_pv(&f.driver, json!({})), pod());
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&pv),
        read_only: false,
    };
    let bp = f.plugin.as_block_volume_plugin().unwrap();
    bp.new_block_volume_mapper(&spec, &pod).unwrap();
    let unmapper = bp.new_block_volume_unmapper("pv1", "pod-uid").unwrap();
    unmapper
        .as_custom_block_volume_unmapper()
        .unwrap()
        .tear_down_device("", "")
        .await
        .unwrap();
    assert!(f.fake.calls.lock().unwrap().unstage.is_empty());
}
