//! The CSI external-attacher (#1460), driven against a fake CSI Controller
//! service on a unix socket.
//!
//! Upstream: kubernetes-csi/external-attacher `pkg/controller/csi_handler.go`
//! (`syncAttach`, `syncDetach`, `csiAttach`, `csiDetach`, `getNodeID`),
//! `trivial_handler.go`, `util.go` (`markAsAttached`, `markAsDetached`,
//! `GetFinalizerName`) and `pkg/attacher/attacher.go`; the cases mirror
//! `csi_handler_test.go`.

use rusternetes_common::resources::csi::VolumeAttachment;
use rusternetes_common::resources::PersistentVolume;
use rusternetes_controller_manager::controllers::csi_attacher::CsiAttacher;
use rusternetes_csi::controller_client::CsiControllerClient;
use rusternetes_csi::proto::controller_server::{Controller, ControllerServer};
use rusternetes_csi::proto::identity_server::{Identity, IdentityServer};
use rusternetes_csi::proto::*;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

const DRIVER: &str = "test.csi.example.com";
const FINALIZER: &str = "external-attacher/test-csi-example-com";
const NODE_ID_ANN: &str = "csi.alpha.kubernetes.io/node-id";

#[derive(Default)]
struct Recorded {
    publish: Vec<ControllerPublishVolumeRequest>,
    unpublish: Vec<ControllerUnpublishVolumeRequest>,
}

#[derive(Clone)]
struct Fake {
    rec: Arc<Mutex<Recorded>>,
    attach: bool,
    publish_readonly: bool,
    publish_code: Option<tonic::Code>,
    unpublish_code: Option<tonic::Code>,
}

impl Fake {
    fn new() -> Self {
        Self {
            rec: Default::default(),
            attach: true,
            publish_readonly: false,
            publish_code: None,
            unpublish_code: None,
        }
    }
}

#[tonic::async_trait]
impl Identity for Fake {
    async fn get_plugin_capabilities(
        &self,
        _r: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: vec![PluginCapability {
                r#type: Some(plugin_capability::Type::Service(
                    plugin_capability::Service {
                        r#type: plugin_capability::service::Type::ControllerService as i32,
                    },
                )),
            }],
        }))
    }
}

#[tonic::async_trait]
impl Controller for Fake {
    async fn controller_get_capabilities(
        &self,
        _r: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        use controller_service_capability::rpc::Type as T;
        let mut types = vec![];
        if self.attach {
            types.push(T::PublishUnpublishVolume);
        }
        if self.publish_readonly {
            types.push(T::PublishReadonly);
        }
        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities: types
                .into_iter()
                .map(|t| ControllerServiceCapability {
                    r#type: Some(controller_service_capability::Type::Rpc(
                        controller_service_capability::Rpc { r#type: t as i32 },
                    )),
                })
                .collect(),
        }))
    }
    async fn controller_publish_volume(
        &self,
        r: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        self.rec.lock().unwrap().publish.push(r.into_inner());
        if let Some(c) = self.publish_code {
            return Err(Status::new(c, "fake publish failure"));
        }
        Ok(Response::new(ControllerPublishVolumeResponse {
            publish_context: [("devicePath".to_string(), "/dev/fake".to_string())].into(),
        }))
    }
    async fn controller_unpublish_volume(
        &self,
        r: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        self.rec.lock().unwrap().unpublish.push(r.into_inner());
        if let Some(c) = self.unpublish_code {
            return Err(Status::new(c, "fake unpublish failure"));
        }
        Ok(Response::new(ControllerUnpublishVolumeResponse {}))
    }
    async fn create_volume(
        &self,
        _r: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        Err(Status::unimplemented("attacher test"))
    }
    async fn delete_volume(
        &self,
        _r: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        Err(Status::unimplemented("attacher test"))
    }
    async fn create_snapshot(
        &self,
        _r: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented("attacher test"))
    }
    async fn delete_snapshot(
        &self,
        _r: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented("attacher test"))
    }
}

struct Env {
    storage: Arc<MemoryStorage>,
    attacher: CsiAttacher<MemoryStorage>,
    rec: Arc<Mutex<Recorded>>,
    _dir: tempfile::TempDir,
}

fn start(fake: Fake) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("csi.sock");
    let rec = fake.rec.clone();
    let listener = tokio::net::UnixListener::bind(&sock).unwrap();
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(IdentityServer::new(fake.clone()))
            .add_service(ControllerServer::new(fake))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let storage = Arc::new(MemoryStorage::new());
    let attacher = CsiAttacher::new(
        storage.clone(),
        DRIVER,
        CsiControllerClient::with_endpoint(sock),
    );
    Env {
        storage,
        attacher,
        rec,
        _dir: dir,
    }
}

async fn put(
    s: &MemoryStorage,
    resource: &str,
    ns: Option<&str>,
    name: &str,
    v: serde_json::Value,
) {
    s.create(&build_key(resource, ns, name), &v).await.unwrap();
}

fn pv_json(deleting: bool) -> serde_json::Value {
    let mut meta = json!({"name": "pv1", "uid": "pv-uid"});
    if deleting {
        meta["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
        meta["finalizers"] = json!([FINALIZER]);
    }
    json!({
        "apiVersion": "v1", "kind": "PersistentVolume", "metadata": meta,
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "mountOptions": ["noatime"],
            "csi": {
                "driver": DRIVER, "volumeHandle": "vol-1", "fsType": "xfs",
                "volumeAttributes": {"shape": "round"},
                "controllerPublishSecretRef": {"name": "pub", "namespace": "ns1"}
            }
        }
    })
}

fn csinode_json() -> serde_json::Value {
    json!({"apiVersion": "storage.k8s.io/v1", "kind": "CSINode",
           "metadata": {"name": "node-1"},
           "spec": {"drivers": [{"name": DRIVER, "nodeID": "csi-node-1"}]}})
}

fn va_json(name: &str, attacher: &str) -> serde_json::Value {
    json!({"apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
           "metadata": {"name": name, "uid": format!("{name}-uid")},
           "spec": {"attacher": attacher, "nodeName": "node-1",
                    "source": {"persistentVolumeName": "pv1"}}})
}

/// pv1 + secret + CSINode + one VolumeAttachment named `va1`.
async fn seed(e: &Env, deleting_pv: bool) {
    put(
        &e.storage,
        "persistentvolumes",
        None,
        "pv1",
        pv_json(deleting_pv),
    )
    .await;
    put(
        &e.storage,
        "secrets",
        Some("ns1"),
        "pub",
        json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": "pub", "namespace": "ns1"},
            "data": {"token": "czNjcmV0"}
        }),
    )
    .await;
    put(&e.storage, "csinodes", None, "node-1", csinode_json()).await;
    put(
        &e.storage,
        "volumeattachments",
        None,
        "va1",
        va_json("va1", DRIVER),
    )
    .await;
}

async fn va(e: &Env) -> VolumeAttachment {
    e.storage
        .get(&build_key("volumeattachments", None, "va1"))
        .await
        .unwrap()
}

async fn mark_deleting(e: &Env) {
    let key = build_key("volumeattachments", None, "va1");
    let mut v = va(e).await;
    v.metadata.deletion_timestamp = Some(chrono::Utc::now());
    e.storage.update(&key, &v).await.unwrap();
}

/// `TestAttach`: finalizer + node-id annotation first, then
/// ControllerPublishVolume with the PV's CSI source, then `markAsAttached`.
#[tokio::test]
async fn attach_publishes_and_marks_attached() {
    let e = start(Fake::new());
    seed(&e, false).await;
    e.attacher.reconcile_all().await.unwrap();

    let got = va(&e).await;
    let st = got.status.expect("status set");
    assert!(st.attached);
    assert!(st.attach_error.is_none());
    assert_eq!(st.attachment_metadata.unwrap()["devicePath"], "/dev/fake");
    assert_eq!(
        got.metadata.finalizers.unwrap(),
        vec![FINALIZER.to_string()]
    );
    assert_eq!(got.metadata.annotations.unwrap()[NODE_ID_ANN], "csi-node-1");

    {
        let rec = e.rec.lock().unwrap();
        assert_eq!(rec.publish.len(), 1);
        let req = &rec.publish[0];
        assert_eq!(req.volume_id, "vol-1");
        assert_eq!(req.node_id, "csi-node-1");
        assert!(!req.readonly);
        assert_eq!(req.volume_context["shape"], "round");
        assert_eq!(req.secrets["token"], "s3cret");
        let cap = req.volume_capability.as_ref().unwrap();
        match cap.access_type.as_ref().unwrap() {
            volume_capability::AccessType::Mount(m) => {
                assert_eq!(m.fs_type, "xfs");
                assert_eq!(m.mount_flags, vec!["noatime".to_string()]);
            }
            other => panic!("expected mount, got {other:?}"),
        }
        assert_eq!(
            cap.access_mode.as_ref().unwrap().mode,
            volume_capability::access_mode::Mode::SingleNodeWriter as i32
        );
    }

    // The PV is protected from deletion while attached
    // (`addPVFinalizer`, csi_handler.go:342-363).
    let pv: PersistentVolume = e
        .storage
        .get(&build_key("persistentvolumes", None, "pv1"))
        .await
        .unwrap();
    assert!(pv
        .metadata
        .finalizers
        .unwrap()
        .contains(&FINALIZER.to_string()));

    // An attached VolumeAttachment is left alone (csi_handler.go:257-261).
    e.attacher.reconcile_all().await.unwrap();
    assert_eq!(e.rec.lock().unwrap().publish.len(), 1);
}

/// `TestAttach` error cases: the error lands in status.attachError (message
/// and gRPC code), `attached` stays false, the finalizer stays, and the retry
/// is rate limited rather than immediate.
#[tokio::test]
async fn attach_error_is_saved_and_retry_is_backed_off() {
    let mut f = Fake::new();
    f.publish_code = Some(tonic::Code::Internal);
    let e = start(f);
    seed(&e, false).await;
    e.attacher.reconcile_all().await.unwrap();

    let got = va(&e).await;
    let st = got.status.expect("status set even on failure");
    assert!(!st.attached);
    let err = st.attach_error.expect("attachError");
    assert!(err.message.unwrap().contains("fake publish failure"));
    assert_eq!(err.error_code, Some(tonic::Code::Internal as i32));
    assert!(err.time.is_some());
    assert_eq!(
        got.metadata.finalizers.unwrap(),
        vec![FINALIZER.to_string()]
    );

    e.attacher.reconcile_all().await.unwrap();
    assert_eq!(
        e.rec.lock().unwrap().publish.len(),
        1,
        "retry must wait for backoff"
    );
}

/// `csiAttach`: "Refuse to attach volumes that are marked for deletion", and
/// that check happens before the VA finalizer is added.
#[tokio::test]
async fn attach_refuses_pv_marked_for_deletion() {
    let e = start(Fake::new());
    seed(&e, true).await;
    e.attacher.reconcile_all().await.unwrap();
    assert!(e.rec.lock().unwrap().publish.is_empty());
    let got = va(&e).await;
    let msg = got.status.unwrap().attach_error.unwrap().message.unwrap();
    assert!(msg.contains("is marked for deletion"), "{msg}");
    assert!(got.metadata.finalizers.is_none());
}

/// `getNodeID` for attach consults only the CSINode (va == nil).
#[tokio::test]
async fn attach_without_csinode_driver_errors() {
    let e = start(Fake::new());
    seed(&e, false).await;
    e.storage
        .delete(&build_key("csinodes", None, "node-1"))
        .await
        .unwrap();
    e.attacher.reconcile_all().await.unwrap();
    assert!(e.rec.lock().unwrap().publish.is_empty());
    assert!(va(&e).await.status.unwrap().attach_error.is_some());
}

/// `TestDetach`: ControllerUnpublishVolume, then the finalizer goes and the
/// deleted VolumeAttachment is gone.
#[tokio::test]
async fn detach_unpublishes_and_releases_the_object() {
    let e = start(Fake::new());
    seed(&e, false).await;
    e.attacher.reconcile_all().await.unwrap();
    assert!(va(&e).await.status.unwrap().attached);

    mark_deleting(&e).await;
    e.attacher.reconcile_all().await.unwrap();

    {
        let rec = e.rec.lock().unwrap();
        assert_eq!(rec.unpublish.len(), 1);
        assert_eq!(rec.unpublish[0].volume_id, "vol-1");
        assert_eq!(rec.unpublish[0].node_id, "csi-node-1");
        assert_eq!(rec.unpublish[0].secrets["token"], "s3cret");
    }
    let key = build_key("volumeattachments", None, "va1");
    assert!(
        e.storage.get::<VolumeAttachment>(&key).await.is_err(),
        "VA removed"
    );
}

/// `getNodeID` for detach falls back to the VA's node-id annotation when the
/// CSINode no longer lists the driver (csi_handler.go:755-771).
#[tokio::test]
async fn detach_falls_back_to_node_id_annotation() {
    let e = start(Fake::new());
    seed(&e, false).await;
    e.attacher.reconcile_all().await.unwrap();
    e.storage
        .delete(&build_key("csinodes", None, "node-1"))
        .await
        .unwrap();
    mark_deleting(&e).await;
    e.attacher.reconcile_all().await.unwrap();
    assert_eq!(e.rec.lock().unwrap().unpublish[0].node_id, "csi-node-1");
}

/// A failed detach lands in status.detachError and the object (and its
/// finalizer) stays.
#[tokio::test]
async fn detach_error_is_saved_and_object_kept() {
    let mut f = Fake::new();
    f.unpublish_code = Some(tonic::Code::Internal);
    let e = start(f);
    seed(&e, false).await;
    e.attacher.reconcile_all().await.unwrap();
    mark_deleting(&e).await;
    e.attacher.reconcile_all().await.unwrap();

    let got = va(&e).await;
    let st = got.status.unwrap();
    assert!(st.attached, "still attached until the detach succeeds");
    assert!(st
        .detach_error
        .unwrap()
        .message
        .unwrap()
        .contains("fake unpublish failure"));
    assert_eq!(
        got.metadata.finalizers.unwrap(),
        vec![FINALIZER.to_string()]
    );
}

/// `trivialHandler`: a driver without PUBLISH_UNPUBLISH_VOLUME gets every
/// VolumeAttachment marked attached, no finalizer, no RPC.
#[tokio::test]
async fn driver_without_publish_capability_is_marked_attached() {
    let mut f = Fake::new();
    f.attach = false;
    let e = start(f);
    seed(&e, false).await;
    e.attacher.reconcile_all().await.unwrap();
    let got = va(&e).await;
    assert!(got.status.unwrap().attached);
    assert!(got.metadata.finalizers.is_none());
    assert!(e.rec.lock().unwrap().publish.is_empty());
}

/// `PUBLISH_READONLY`: the PV's readOnly only reaches the driver when it
/// reports the capability (csi_handler.go:492-496).
#[tokio::test]
async fn read_only_is_sent_only_with_publish_readonly_capability() {
    for (cap, want) in [(false, false), (true, true)] {
        let mut f = Fake::new();
        f.publish_readonly = cap;
        let e = start(f);
        let mut pv = pv_json(false);
        pv["spec"]["csi"]["readOnly"] = json!(true);
        put(&e.storage, "persistentvolumes", None, "pv1", pv).await;
        put(&e.storage, "csinodes", None, "node-1", csinode_json()).await;
        put(
            &e.storage,
            "secrets",
            Some("ns1"),
            "pub",
            json!({"apiVersion": "v1", "kind": "Secret",
                   "metadata": {"name": "pub", "namespace": "ns1"}}),
        )
        .await;
        put(
            &e.storage,
            "volumeattachments",
            None,
            "va1",
            va_json("va1", DRIVER),
        )
        .await;
        e.attacher.reconcile_all().await.unwrap();
        assert_eq!(e.rec.lock().unwrap().publish[0].readonly, want);
    }
}

/// `syncNewOrUpdatedVolumeAttachment` skips other drivers' attachments.
#[tokio::test]
async fn other_drivers_attachments_are_ignored() {
    let e = start(Fake::new());
    seed(&e, false).await;
    put(
        &e.storage,
        "volumeattachments",
        None,
        "other",
        va_json("other", "other.csi.io"),
    )
    .await;
    e.attacher.reconcile_all().await.unwrap();
    let other: VolumeAttachment = e
        .storage
        .get(&build_key("volumeattachments", None, "other"))
        .await
        .unwrap();
    assert!(other.status.is_none() && other.metadata.finalizers.is_none());
}

/// `SyncNewOrUpdatedPersistentVolume`: a deleted PV carrying the finalizer is
/// released once no VolumeAttachment references it.
#[tokio::test]
async fn pv_finalizer_is_removed_when_no_attachment_needs_it() {
    let e = start(Fake::new());
    put(&e.storage, "persistentvolumes", None, "pv1", pv_json(true)).await;
    e.attacher.reconcile_all().await.unwrap();
    let r = e
        .storage
        .get::<PersistentVolume>(&build_key("persistentvolumes", None, "pv1"))
        .await;
    assert!(
        r.is_err(),
        "deleting PV with no finalizer left is reaped: {r:?}"
    );
}

/// ...and kept while a VolumeAttachment still references it.
#[tokio::test]
async fn pv_finalizer_is_kept_while_an_attachment_references_it() {
    let e = start(Fake::new());
    put(&e.storage, "persistentvolumes", None, "pv1", pv_json(true)).await;
    put(
        &e.storage,
        "volumeattachments",
        None,
        "va1",
        va_json("va1", "other.csi.io"),
    )
    .await;
    e.attacher.reconcile_all().await.unwrap();
    let pv: PersistentVolume = e
        .storage
        .get(&build_key("persistentvolumes", None, "pv1"))
        .await
        .unwrap();
    assert_eq!(pv.metadata.finalizers.unwrap(), vec![FINALIZER.to_string()]);
}
