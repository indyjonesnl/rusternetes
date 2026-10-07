//! Tests for the CSI volume plugin, ported in shape from
//! `pkg/volume/csi/csi_mounter_test.go` (`TestMounterGetPath`,
//! `TestMounterSetUp`, `TestMounterSetupJsonFileHandling`,
//! `TestMounterSetUpSimple`, `TestMounterSetUpWithInline`,
//! `TestUnmounterTeardown*`) and `csi_attacher_test.go` (`TestAttacherMountDevice`).
//!
//! Upstream drives a fake in-process `csiClient`; here the driver is a real
//! gRPC `csi.v1.Node` server on a unix socket (`csi_client::fake`) registered in
//! the driver store, so the whole request path is exercised.

use super::*;
use crate::volume_plugins::csi_client::fake::{serve, FakeDriver};
use crate::volume_plugins::csi_client::proto::node_service_capability::rpc::Type as Cap;
use crate::volume_plugins::csi_client::proto::volume_capability::AccessType;
use crate::volume_plugins::csi_client::CsiError;
use crate::volume_plugins::csi_drivers_store::{csi_drivers, Driver};
use rusternetes_common::resources::{Pod, Secret, Volume};
use rusternetes_storage::{build_key, MemoryStorage, Storage, StorageBackend};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

fn host(
    base: &str,
    storage: Option<Arc<StorageBackend>>,
) -> Arc<crate::volume_plugins::KubeletVolumeHost> {
    Arc::new(crate::volume_plugins::KubeletVolumeHost::new(
        base.to_string(),
        storage,
        rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        HashMap::new(),
    ))
}

fn plugin() -> CsiPlugin {
    CsiPlugin::new(host("/var/lib/rusternetes", None))
}

fn pod() -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": "p", "namespace": "ns1", "uid": "uid-1"},
        "spec": {"containers": [], "nodeName": "node-1", "serviceAccountName": "sa-1"}
    }))
    .unwrap()
}

fn claim_volume() -> Volume {
    serde_json::from_value(json!({"name": "data", "persistentVolumeClaim": {"claimName": "c"}}))
        .unwrap()
}

fn pv(
    driver: &str,
    extra_csi: serde_json::Value,
) -> rusternetes_common::resources::PersistentVolume {
    let mut csi = json!({"driver": driver, "volumeHandle": "vol-1"});
    for (k, v) in extra_csi.as_object().unwrap() {
        csi[k] = v.clone();
    }
    serde_json::from_value(json!({
        "metadata": {"name": "pv1"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteMany"],
            "mountOptions": ["foo=bar"],
            "csi": csi
        }
    }))
    .unwrap()
}

/// Everything a mount test needs: a tempdir kubelet root, an in-memory API, a
/// fake driver registered under a test-unique name.
struct Fx {
    _dir: tempfile::TempDir,
    root: String,
    driver: String,
    fake: FakeDriver,
    storage: Arc<StorageBackend>,
    plugin: CsiPlugin,
    _srv: tokio::task::JoinHandle<()>,
}

async fn fx(name: &str, caps: &[Cap], csi_driver: Option<serde_json::Value>) -> Fx {
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
    if let Some(spec) = csi_driver {
        let obj = json!({"apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
            "metadata": {"name": driver}, "spec": spec});
        storage
            .create(&build_key("csidrivers", None, &driver), &obj)
            .await
            .unwrap();
    }
    let plugin = CsiPlugin::new(host(&root, Some(storage.clone())));
    Fx {
        _dir: dir,
        root,
        driver,
        fake,
        storage,
        plugin,
        _srv: srv,
    }
}

async fn put_secret(storage: &Arc<StorageBackend>, ns: &str, name: &str, k: &str, v: &str) {
    let s =
        Secret::new(name, ns).with_data(HashMap::from([(k.to_string(), v.as_bytes().to_vec())]));
    storage
        .create(&build_key("secrets", Some(ns), name), &s)
        .await
        .unwrap();
}

// ---- can_support / names -------------------------------------------------

#[test]
fn supports_an_inline_csi_volume() {
    let v: Volume =
        serde_json::from_value(json!({"name": "csi-inline", "csi": {"driver": "example.com/csi"}}))
            .unwrap();
    let spec = Spec {
        volume: &v,
        persistent_volume: None,
    };
    assert!(plugin().can_support(&spec));
}

#[test]
fn rejects_other_kinds() {
    let v: Volume = serde_json::from_value(json!({"name": "scratch", "emptyDir": {}})).unwrap();
    let spec = Spec {
        volume: &v,
        persistent_volume: None,
    };
    assert!(!plugin().can_support(&spec));
}

/// The PV arm of `CanSupport` (`csi_plugin.go:447-455`): a claim-backed volume
/// whose bound PV has a CSI source is this plugin's.
#[test]
fn supports_a_persistent_volume_with_a_csi_source() {
    let v = claim_volume();
    let p = pv("example.com/csi", json!({}));
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    assert!(plugin().can_support(&spec));
}

#[test]
fn rejects_a_persistent_volume_without_a_csi_source() {
    let v = claim_volume();
    let p: rusternetes_common::resources::PersistentVolume = serde_json::from_value(json!({
        "metadata": {"name": "pv1"},
        "spec": {"hostPath": {"path": "/mnt/data"}}
    }))
    .unwrap();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    assert!(!plugin().can_support(&spec));
}

#[test]
fn plugin_name_is_the_upstream_name() {
    assert_eq!(plugin().name(), crate::pod_dirs::plugin::CSI);
}

/// `GetVolumeName` for a PV spec: `<driver>^<volumeHandle>`.
#[test]
fn get_volume_name_for_a_pv() {
    let v = claim_volume();
    let p = pv("example.com/csi", json!({}));
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    assert_eq!(
        plugin().get_volume_name(&spec).unwrap(),
        "example.com/csi^vol-1"
    );
}

// ---- GetPath -------------------------------------------------------------

/// `TestMounterGetPath` (`csi_mounter_test.go:84`): the mount path is
/// `pods/<uid>/volumes/kubernetes.io~csi/<PV name>/mount` — the PV's name, not
/// the pod volume's, because `Spec.Name()` is the PV's name for a PV-resolved
/// spec (`plugins.go:444-453`).
#[tokio::test]
async fn mounter_get_path_uses_the_pv_name_and_mount_suffix() {
    let v = claim_volume();
    let p = pv("example.com/csi", json!({}));
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = plugin().new_mounter(&spec, &pod()).await.unwrap();
    assert_eq!(
        m.get_path(),
        "/var/lib/rusternetes/pods/uid-1/volumes/kubernetes.io~csi/pv1/mount"
    );
}

// ---- SetUp: persistent ---------------------------------------------------

/// `TestMounterSetUp` / `TestMounterSetUpSimple` "setup with persistent
/// source": NodePublishVolume carries volume id, target path, volume context,
/// node-publish secrets, fs type, mount flags and the access mode; the volume
/// info file is persisted for teardown (`csi_mounter.go:284-300`).
#[tokio::test]
async fn set_up_publishes_a_persistent_volume() {
    let f = fx("pub", &[], Some(json!({"attachRequired": false}))).await;
    put_secret(&f.storage, "sec-ns", "pub-secret", "token", "s3cret").await;
    let p = pv(
        &f.driver,
        json!({
            "fsType": "ext4",
            "volumeAttributes": {"a": "b"},
            "nodePublishSecretRef": {"name": "pub-secret", "namespace": "sec-ns"}
        }),
    );
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    m.set_up().await.unwrap();

    let calls = f.fake.calls.lock().unwrap();
    assert_eq!(calls.publish.len(), 1);
    let req = &calls.publish[0];
    assert_eq!(req.volume_id, "vol-1");
    assert_eq!(req.target_path, m.get_path());
    assert_eq!(req.volume_context["a"], "b");
    assert_eq!(req.secrets["token"], "s3cret");
    assert!(req.staging_target_path.is_empty(), "no STAGE_UNSTAGE");
    assert!(!req.readonly);
    match req
        .volume_capability
        .as_ref()
        .unwrap()
        .access_type
        .as_ref()
        .unwrap()
    {
        AccessType::Mount(mnt) => {
            assert_eq!(mnt.fs_type, "ext4");
            assert_eq!(mnt.mount_flags, vec!["foo=bar"]);
        }
        other => panic!("expected mount, got {other:?}"),
    }
    drop(calls);

    // volume info persisted next to the mount dir
    let data_file = std::path::Path::new(&m.get_path())
        .parent()
        .unwrap()
        .join("vol_data.json");
    let data: HashMap<String, String> =
        serde_json::from_str(&std::fs::read_to_string(data_file).unwrap()).unwrap();
    assert_eq!(data["specVolID"], "pv1");
    assert_eq!(data["volumeHandle"], "vol-1");
    assert_eq!(data["driverName"], f.driver);
    assert_eq!(data["nodeName"], "node-1");
    assert_eq!(data["volumeLifecycleMode"], "Persistent");
    assert!(data["attachmentID"].starts_with("csi-"));
}

/// The claim's `readOnly` reaches NodePublishVolume (`Spec.ReadOnly`,
/// `csi_plugin.go:513`).
#[tokio::test]
async fn set_up_passes_the_claims_read_only_flag() {
    let f = fx("ro", &[], Some(json!({"attachRequired": false}))).await;
    let p = pv(&f.driver, json!({}));
    let v: Volume = serde_json::from_value(
        json!({"name": "data", "persistentVolumeClaim": {"claimName": "c", "readOnly": true}}),
    )
    .unwrap();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    m.set_up().await.unwrap();
    assert!(f.fake.calls.lock().unwrap().publish[0].readonly);
}

/// With STAGE_UNSTAGE_VOLUME the volume is staged first (`MountDevice`,
/// `csi_attacher.go:264-411`), then published with the staging path
/// (`csi_mounter.go:164-178`). Stage gets the node-STAGE secret.
#[tokio::test]
async fn set_up_stages_then_publishes_when_the_driver_supports_it() {
    let f = fx(
        "stage",
        &[Cap::StageUnstageVolume],
        Some(json!({"attachRequired": false})),
    )
    .await;
    put_secret(&f.storage, "sec-ns", "stage-secret", "k", "stage-v").await;
    let p = pv(
        &f.driver,
        json!({
            "fsType": "xfs",
            "nodeStageSecretRef": {"name": "stage-secret", "namespace": "sec-ns"}
        }),
    );
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    m.set_up().await.unwrap();

    let calls = f.fake.calls.lock().unwrap();
    assert_eq!(calls.stage.len(), 1);
    let stage = &calls.stage[0];
    assert_eq!(stage.volume_id, "vol-1");
    assert_eq!(stage.secrets["k"], "stage-v");
    // <root>/plugins/kubernetes.io/csi/<driver>/<sha256(handle)>/globalmount
    let want_prefix = format!("{}/plugins/kubernetes.io/csi/{}/", f.root, f.driver);
    assert!(
        stage.staging_target_path.starts_with(&want_prefix)
            && stage.staging_target_path.ends_with("/globalmount"),
        "{}",
        stage.staging_target_path
    );
    assert!(std::path::Path::new(&stage.staging_target_path).is_dir());
    assert_eq!(calls.publish.len(), 1);
    assert_eq!(
        calls.publish[0].staging_target_path,
        stage.staging_target_path
    );
}

/// Pod information is injected into the volume context when the CSIDriver sets
/// `podInfoOnMount` (`getPodInfoAttrs`, `csi_util.go:208`).
#[tokio::test]
async fn set_up_injects_pod_info_when_the_driver_asks_for_it() {
    let f = fx(
        "podinfo",
        &[],
        Some(json!({"attachRequired": false, "podInfoOnMount": true})),
    )
    .await;
    let p = pv(&f.driver, json!({"volumeAttributes": {"a": "b"}}));
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    m.set_up().await.unwrap();
    let calls = f.fake.calls.lock().unwrap();
    let vc = &calls.publish[0].volume_context;
    assert_eq!(vc["a"], "b");
    assert_eq!(vc["csi.storage.k8s.io/pod.name"], "p");
    assert_eq!(vc["csi.storage.k8s.io/pod.namespace"], "ns1");
    assert_eq!(vc["csi.storage.k8s.io/pod.uid"], "uid-1");
    assert_eq!(vc["csi.storage.k8s.io/serviceAccount.name"], "sa-1");
    assert_eq!(vc["csi.storage.k8s.io/ephemeral"], "false");
}

/// Without `podInfoOnMount` the context is left alone.
#[tokio::test]
async fn set_up_omits_pod_info_by_default() {
    let f = fx("nopodinfo", &[], Some(json!({"attachRequired": false}))).await;
    let p = pv(&f.driver, json!({}));
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    m.set_up().await.unwrap();
    assert!(f.fake.calls.lock().unwrap().publish[0]
        .volume_context
        .is_empty());
}

/// `TestPodServiceAccountTokenAttrs` (`csi_mounter_test.go:1286`): a CSIDriver
/// with `tokenRequests` gets `csi.storage.k8s.io/serviceAccount.tokens` in the
/// volume context, a JSON map audience -> `{token, expirationTimestamp}`
/// (`podServiceAccountTokenAttrs`, `csi_mounter.go:358-416`).
#[tokio::test]
async fn set_up_injects_service_account_tokens_into_the_volume_context() {
    let f = fx(
        "satoken",
        &[],
        Some(json!({"attachRequired": false,
            "tokenRequests": [{"audience": "gcp", "expirationSeconds": 3600}, {"audience": ""}]})),
    )
    .await;
    f.storage
        .create(
            &build_key("serviceaccounts", Some("ns1"), "sa-1"),
            &json!({"apiVersion": "v1", "kind": "ServiceAccount",
                "metadata": {"name": "sa-1", "namespace": "ns1", "uid": "sa-uid-1"}}),
        )
        .await
        .unwrap();
    let p = pv(&f.driver, json!({}));
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    m.set_up().await.unwrap();
    let calls = f.fake.calls.lock().unwrap();
    let req = &calls.publish[0];
    let raw = req
        .volume_context
        .get("csi.storage.k8s.io/serviceAccount.tokens")
        .expect("tokens attribute missing from volume_context");
    let tokens: serde_json::Value = serde_json::from_str(raw).unwrap();
    // One entry per tokenRequest, keyed by the requested audience ("" kept).
    assert_eq!(tokens.as_object().unwrap().len(), 2, "{raw}");
    let gcp = tokens["gcp"]["token"].as_str().expect("gcp token");
    assert!(tokens["gcp"]["expirationTimestamp"].as_str().is_some());
    assert!(tokens[""]["token"].as_str().is_some());
    // The token is for the pod's service account, bound to the requested audience.
    let claims = rusternetes_common::auth::TokenManager::new_auto(b"test-secret")
        .validate_token_with_audiences(gcp, &["gcp".to_string()])
        .unwrap();
    assert_eq!(claims.sub, "system:serviceaccount:ns1:sa-1");
    // Not in the secrets unless the driver asks for it.
    assert!(!req
        .secrets
        .contains_key("csi.storage.k8s.io/serviceAccount.tokens"));
}

/// `serviceAccountTokenInSecrets`: the tokens move from `volume_context` to
/// `node_publish_secrets` (`csi_mounter.go:243-249`), so they stay out of the
/// PV-visible context.
#[tokio::test]
async fn set_up_puts_service_account_tokens_in_secrets_when_asked() {
    let f = fx(
        "satokensecrets",
        &[],
        Some(
            json!({"attachRequired": false, "serviceAccountTokenInSecrets": true,
            "tokenRequests": [{"audience": "gcp"}]}),
        ),
    )
    .await;
    put_secret(&f.storage, "sec-ns", "pub-secret", "token", "s3cret").await;
    let p = pv(
        &f.driver,
        json!({"nodePublishSecretRef": {"name": "pub-secret", "namespace": "sec-ns"}}),
    );
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    m.set_up().await.unwrap();
    let calls = f.fake.calls.lock().unwrap();
    let req = &calls.publish[0];
    assert!(!req
        .volume_context
        .contains_key("csi.storage.k8s.io/serviceAccount.tokens"));
    assert_eq!(req.secrets["token"], "s3cret", "merged, not replaced");
    assert!(req.secrets["csi.storage.k8s.io/serviceAccount.tokens"].contains("\"gcp\""));
}

/// A CSIDriver without `tokenRequests` adds nothing.
#[tokio::test]
async fn set_up_without_token_requests_adds_no_token_attribute() {
    let f = fx("notokens", &[], Some(json!({"attachRequired": false}))).await;
    let p = pv(&f.driver, json!({}));
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    m.set_up().await.unwrap();
    assert!(f.fake.calls.lock().unwrap().publish[0]
        .volume_context
        .is_empty());
}

/// The publish context comes from the node's VolumeAttachment
/// (`getPublishContext`, `csi_plugin.go:906-928`) when attach is required.
#[tokio::test]
async fn set_up_reads_the_publish_context_from_the_volume_attachment() {
    let f = fx("va", &[], None).await; // no CSIDriver object: attach NOT skipped
    let p = pv(&f.driver, json!({}));
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();

    // No VolumeAttachment yet: a transient failure, nothing published.
    let err = m.set_up().await.unwrap_err();
    assert!(
        matches!(err.downcast_ref::<CsiError>(), Some(CsiError::Transient(_))),
        "{err:#}"
    );
    assert!(err.to_string().contains("failed to fetch publishContext"));
    assert!(f.fake.calls.lock().unwrap().publish.is_empty());

    let attach_id = get_attachment_name("vol-1", &f.driver, "node-1");
    let va = json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
        "metadata": {"name": attach_id},
        "spec": {"attacher": f.driver, "nodeName": "node-1", "source": {"persistentVolumeName": "pv1"}},
        "status": {"attached": true, "attachmentMetadata": {"devicePath": "/dev/x"}}
    });
    f.storage
        .create(&build_key("volumeattachments", None, &attach_id), &va)
        .await
        .unwrap();
    m.set_up().await.unwrap();
    assert_eq!(
        f.fake.calls.lock().unwrap().publish[0].publish_context["devicePath"],
        "/dev/x"
    );
}

/// `TestMounterSetUpSimple` "setup with unknown CSI driver": no registered
/// driver is a TRANSIENT failure (`csi_mounter.go:105-109`) and nothing is
/// silently mounted.
#[tokio::test]
async fn set_up_with_an_unregistered_driver_is_transient() {
    let f = fx("unreg", &[], Some(json!({"attachRequired": false}))).await;
    let p = pv("never-registered.csi.example.com", json!({}));
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    let err = m.set_up().await.unwrap_err();
    assert!(
        matches!(err.downcast_ref::<CsiError>(), Some(CsiError::Transient(_))),
        "{err:#}"
    );
    assert!(
        err.to_string().contains("failed to get CSI client"),
        "{err:#}"
    );
    assert!(!std::path::Path::new(&m.get_path()).exists());
}

/// `TestMounterSetupJsonFileHandling`: a FINAL publish error removes the
/// volume info file and mount dir; a non-final (uncertain) one keeps them so
/// teardown can still find the volume (`csi_mounter.go:300-310`).
#[tokio::test]
async fn final_publish_error_cleans_up_but_uncertain_keeps_the_data_file() {
    for (code, keeps) in [
        (tonic::Code::InvalidArgument, false),
        (tonic::Code::Aborted, true),
        (tonic::Code::DeadlineExceeded, true),
    ] {
        let f = fx("jsonfile", &[], Some(json!({"attachRequired": false}))).await;
        *f.fake.publish_error.lock().unwrap() = Some(code);
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        let err = m.set_up().await.unwrap_err();
        let parent = std::path::PathBuf::from(m.get_path())
            .parent()
            .unwrap()
            .to_path_buf();
        assert_eq!(
            parent.join("vol_data.json").exists(),
            keeps,
            "{code:?}: {err:#}"
        );
        assert_eq!(
            matches!(
                err.downcast_ref::<CsiError>(),
                Some(CsiError::UncertainProgress(_))
            ),
            keeps,
            "{code:?}"
        );
    }
}

/// A PV resolved from a claim whose driver does not list `Persistent` is
/// rejected (`supportsVolumeLifecycleMode`, `csi_mounter.go:531-560`).
#[tokio::test]
async fn set_up_rejects_a_driver_that_does_not_support_the_mode() {
    let f = fx(
        "modes",
        &[],
        Some(json!({"attachRequired": false, "volumeLifecycleModes": ["Ephemeral"]})),
    )
    .await;
    let p = pv(&f.driver, json!({}));
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    let err = m.set_up().await.unwrap_err();
    assert!(
        err.to_string()
            .contains("volume mode \"Persistent\" not supported by driver"),
        "{err:#}"
    );
    assert!(f.fake.calls.lock().unwrap().publish.is_empty());
}

// ---- SetUp: inline (ephemeral) -------------------------------------------

/// `TestMounterSetUpWithInline`: an inline volume on a registered driver with
/// an Ephemeral-capable CSIDriver is published under a generated handle
/// `csi-<sha256(podUID + volumeName)>` (`makeVolumeHandle`, `csi_mounter.go:612`).
#[tokio::test]
async fn set_up_publishes_an_inline_volume_on_a_registered_driver() {
    let f = fx(
        "inline",
        &[],
        Some(json!({"volumeLifecycleModes": ["Ephemeral"]})),
    )
    .await;
    put_secret(&f.storage, "ns1", "inline-secret", "k", "v").await;
    let v: Volume = serde_json::from_value(json!({
        "name": "eph",
        "csi": {
            "driver": f.driver, "fsType": "ext4",
            "volumeAttributes": {"x": "y"},
            "nodePublishSecretRef": {"name": "inline-secret"}
        }
    }))
    .unwrap();
    let spec = Spec {
        volume: &v,
        persistent_volume: None,
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    assert_eq!(
        m.get_path(),
        format!("{}/pods/uid-1/volumes/kubernetes.io~csi/eph/mount", f.root)
    );
    m.set_up().await.unwrap();
    let calls = f.fake.calls.lock().unwrap();
    let req = &calls.publish[0];
    assert!(req.volume_id.starts_with("csi-") && req.volume_id.len() == 4 + 64);
    assert_eq!(req.volume_context["x"], "y");
    assert_eq!(
        req.secrets["k"], "v",
        "inline secret ref uses the pod's namespace"
    );
    assert!(calls.stage.is_empty(), "inline volumes are never staged");
}

/// Deviation preserved from the pre-plugin `create_volume`: an inline volume
/// whose driver is NOT registered keeps the placeholder directory instead of
/// failing the pod (upstream would fail with "no CSIDriver object").
#[tokio::test]
async fn inline_volume_on_an_unregistered_driver_keeps_the_placeholder_dir() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_string_lossy().to_string();
    let p = CsiPlugin::new(host(&root, None));
    let v: Volume = serde_json::from_value(
        json!({"name": "eph", "csi": {"driver": "not-registered-inline.example.com"}}),
    )
    .unwrap();
    let spec = Spec {
        volume: &v,
        persistent_volume: None,
    };
    let m = p.new_mounter(&spec, &pod()).await.unwrap();
    m.set_up().await.unwrap();
    assert!(std::path::Path::new(&m.get_path()).is_dir());
}

// ---- TearDown --------------------------------------------------------------

/// `TestUnmounterTeardown` (`csi_mounter_test.go:1172`): the unmounter reloads
/// the volume info file, calls NodeUnpublishVolume with the saved handle and the
/// mount path, then removes the mount dir, the info file and the volume dir.
#[tokio::test]
async fn tear_down_unpublishes_and_cleans_up() {
    let f = fx("teardown", &[], Some(json!({"attachRequired": false}))).await;
    let p = pv(&f.driver, json!({}));
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    m.set_up().await.unwrap();
    let mount = std::path::PathBuf::from(m.get_path());
    assert!(mount.is_dir());

    let um = f.plugin.new_unmounter("pv1", "uid-1").unwrap();
    assert_eq!(um.get_path(), m.get_path());
    um.tear_down().await.unwrap();

    let calls = f.fake.calls.lock().unwrap();
    assert_eq!(calls.unpublish.len(), 1);
    assert_eq!(calls.unpublish[0].volume_id, "vol-1");
    assert_eq!(calls.unpublish[0].target_path, m.get_path());
    assert!(!mount.exists());
    assert!(!mount.parent().unwrap().exists(), "volume dir removed");
}

/// `TestUnmounterTeardownNoClientError` (`csi_mounter_test.go:1228`): a driver
/// that is no longer registered is a transient failure, not a silent success.
#[tokio::test]
async fn tear_down_with_an_unregistered_driver_is_transient() {
    let f = fx(
        "teardown-noclient",
        &[],
        Some(json!({"attachRequired": false})),
    )
    .await;
    let p = pv(&f.driver, json!({}));
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    m.set_up().await.unwrap();
    let um = f.plugin.new_unmounter("pv1", "uid-1").unwrap();

    csi_drivers().delete(&f.driver);
    let err = um.tear_down().await.unwrap_err();
    assert!(
        matches!(err.downcast_ref::<CsiError>(), Some(CsiError::Transient(_))),
        "{err:#}"
    );
    assert!(f.fake.calls.lock().unwrap().unpublish.is_empty());
}

/// `NewUnmounter` fails when no volume info file exists
/// (`csi_plugin.go:560-563`).
#[test]
fn new_unmounter_without_a_data_file_fails() {
    let err = plugin().new_unmounter("missing", "uid-x").err().unwrap();
    assert!(
        err.to_string()
            .contains("unmounter failed to load volume data file"),
        "{err:#}"
    );
}

/// An orphaned pod's published CSI volume is NodeUnpublished by the kubelet's
/// cleanup: upstream's reconciler `unmountVolumes` -> `UnmountVolume` ->
/// `NewUnmounter` -> `TearDownAt` (`reconciler.go`, `operation_generator.go`
/// `GenerateUnmountVolumeFunc`, `csi_plugin.go:540-567`, `csi_mounter.go:432-466`).
/// A live pod's volume must be left alone.
#[tokio::test]
async fn orphaned_pod_csi_volume_is_unpublished() {
    let f = fx("orphan", &[], Some(json!({"attachRequired": false}))).await;
    let p = pv(&f.driver, json!({}));
    let v = claim_volume();
    let spec = Spec {
        volume: &v,
        persistent_volume: Some(&p),
    };
    let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
    m.set_up().await.unwrap();
    let mount = std::path::PathBuf::from(m.get_path());
    let vm = crate::volumes::VolumeManager::new(
        f.root.clone(),
        None,
        rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
    );

    // Live pod: untouched.
    let live: std::collections::HashSet<String> = ["uid-1".to_string()].into();
    vm.unmount_orphaned_csi_volumes(&live).await;
    assert!(f.fake.calls.lock().unwrap().unpublish.is_empty());
    assert!(mount.is_dir());

    // Orphaned pod: NodeUnpublishVolume with the saved handle + mount path.
    vm.unmount_orphaned_csi_volumes(&std::collections::HashSet::new())
        .await;
    let calls = f.fake.calls.lock().unwrap();
    assert_eq!(calls.unpublish.len(), 1);
    assert_eq!(calls.unpublish[0].volume_id, "vol-1");
    assert_eq!(calls.unpublish[0].target_path, m.get_path());
    assert!(!mount.exists());
}
