//! The CSI external-provisioner loop (#2882), driven against a fake CSI
//! Controller service on a unix socket.
//!
//! Upstream: kubernetes-csi/external-provisioner `pkg/controller/controller.go`
//! (`Provision`, `prepareProvision`, `ShouldProvision`) over
//! sig-storage-lib-external-provisioner `controller/controller.go`
//! (`syncClaim`, `provisionClaimOperation`) and `volume_store.go`.

use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::*;
use rusternetes_common::resources::{Event, PersistentVolume, PersistentVolumeClaim};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::csi_provisioner::CsiProvisioner;
use rusternetes_controller_manager::controllers::dynamic_provisioner::DynamicProvisionerController;
use rusternetes_csi::controller_client::CsiControllerClient;
use rusternetes_csi::proto::controller_server::{Controller, ControllerServer};
use rusternetes_csi::proto::identity_server::{Identity, IdentityServer};
use rusternetes_csi::proto::*;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

const DRIVER: &str = "csi.example.com";
const PVC_UID: &str = "5f4e3d2c-1111-2222-3333-444455556666";

#[derive(Default)]
struct Recorded {
    create_volume: Vec<CreateVolumeRequest>,
    delete_volume: Vec<DeleteVolumeRequest>,
}

#[derive(Clone)]
struct Fake {
    rec: Arc<Mutex<Recorded>>,
    /// `None` echoes the requested size; `Some(n)` reports `n` bytes.
    capacity: Option<i64>,
    create_code: Option<tonic::Code>,
}

impl Fake {
    fn new() -> Self {
        Self {
            rec: Default::default(),
            capacity: None,
            create_code: None,
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
        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities: vec![ControllerServiceCapability {
                r#type: Some(controller_service_capability::Type::Rpc(
                    controller_service_capability::Rpc {
                        r#type: controller_service_capability::rpc::Type::CreateDeleteVolume as i32,
                    },
                )),
            }],
        }))
    }
    async fn create_volume(
        &self,
        r: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = r.into_inner();
        self.rec.lock().unwrap().create_volume.push(req.clone());
        if let Some(c) = self.create_code {
            return Err(Status::new(c, "fake create failure"));
        }
        let required = req.capacity_range.as_ref().map_or(0, |c| c.required_bytes);
        Ok(Response::new(CreateVolumeResponse {
            volume: Some(Volume {
                capacity_bytes: self.capacity.unwrap_or(required),
                volume_id: "vol-1".into(),
                volume_context: [("shape".to_string(), "round".to_string())].into(),
                ..Default::default()
            }),
        }))
    }
    async fn delete_volume(
        &self,
        r: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        self.rec.lock().unwrap().delete_volume.push(r.into_inner());
        Ok(Response::new(DeleteVolumeResponse {}))
    }
    async fn create_snapshot(
        &self,
        _r: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented("not part of slice 1"))
    }
    async fn delete_snapshot(
        &self,
        _r: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented("not part of slice 1"))
    }
}

fn start(fake: Fake) -> (tempfile::TempDir, CsiControllerClient, Arc<Mutex<Recorded>>) {
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
    (dir, CsiControllerClient::with_endpoint(sock), rec)
}

fn storage_class(binding: Option<VolumeBindingMode>) -> StorageClass {
    StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".into(),
            api_version: "storage.k8s.io/v1".into(),
        },
        metadata: ObjectMeta::new("fast"),
        provisioner: DRIVER.into(),
        parameters: Some(
            [
                ("type".to_string(), "ssd".to_string()),
                ("csi.storage.k8s.io/fstype".to_string(), "xfs".to_string()),
            ]
            .into(),
        ),
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Retain),
        volume_binding_mode: binding,
        allowed_topologies: None,
        allow_volume_expansion: None,
        mount_options: Some(vec!["noatime".into()]),
    }
}

fn claim(annotations: &[(&str, &str)]) -> PersistentVolumeClaim {
    let mut meta = ObjectMeta::new("data").with_namespace("ns1");
    meta.uid = PVC_UID.into();
    meta.annotations = Some(
        annotations
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    );
    PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".into(),
            api_version: "v1".into(),
        },
        metadata: meta,
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            resources: ResourceRequirements {
                requests: Some([("storage".to_string(), "1Gi".to_string())].into()),
                ..Default::default()
            },
            storage_class_name: Some("fast".into()),
            ..Default::default()
        },
        status: None,
    }
}

const ANN: &str = "volume.kubernetes.io/storage-provisioner";

async fn env(
    fake: Fake,
    sc: StorageClass,
) -> (
    Arc<MemoryStorage>,
    CsiProvisioner<MemoryStorage>,
    Arc<Mutex<Recorded>>,
    tempfile::TempDir,
) {
    let storage = Arc::new(MemoryStorage::new());
    storage
        .create(&build_key("storageclasses", None, "fast"), &sc)
        .await
        .unwrap();
    let (dir, client, rec) = start(fake);
    let p = CsiProvisioner::new(storage.clone(), DRIVER, client);
    (storage, p, rec, dir)
}

async fn pv(storage: &MemoryStorage) -> Option<PersistentVolume> {
    storage
        .get(&build_key(
            "persistentvolumes",
            None,
            &format!("pvc-{PVC_UID}"),
        ))
        .await
        .ok()
}

async fn events(storage: &MemoryStorage) -> Vec<Event> {
    storage.list("/registry/events/").await.unwrap_or_default()
}

#[tokio::test]
async fn provisions_a_pv_for_an_annotated_claim() {
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    p.sync_claim(&claim(&[(ANN, DRIVER)])).await.unwrap();

    // CreateVolume: `pvc-<uid>`, required bytes, prefixed params stripped,
    // mount capability with the SC fsType + mountOptions, SINGLE_NODE_WRITER.
    let reqs = rec.lock().unwrap().create_volume.clone();
    assert_eq!(reqs.len(), 1);
    let r = &reqs[0];
    assert_eq!(r.name, format!("pvc-{PVC_UID}"));
    assert_eq!(r.capacity_range.as_ref().unwrap().required_bytes, 1 << 30);
    assert_eq!(r.parameters.get("type").map(String::as_str), Some("ssd"));
    assert!(!r.parameters.contains_key("csi.storage.k8s.io/fstype"));
    assert_eq!(r.volume_capabilities.len(), 1);
    let vc = &r.volume_capabilities[0];
    match vc.access_type.as_ref().unwrap() {
        volume_capability::AccessType::Mount(m) => {
            assert_eq!(m.fs_type, "xfs");
            assert_eq!(m.mount_flags, vec!["noatime".to_string()]);
        }
        other => panic!("expected mount, got {other:?}"),
    }
    assert_eq!(
        vc.access_mode.as_ref().unwrap().mode,
        volume_capability::access_mode::Mode::SingleNodeWriter as i32
    );

    // The PV (`Provision` + `provisionClaimOperation`).
    let pv = pv(&storage).await.expect("PV created");
    let csi = pv.spec.csi.as_ref().unwrap();
    assert_eq!(csi.driver, DRIVER);
    assert_eq!(csi.volume_handle.as_deref(), Some("vol-1"));
    assert_eq!(csi.fs_type.as_deref(), Some("xfs"));
    let attrs = csi.volume_attributes.as_ref().unwrap();
    assert_eq!(attrs.get("shape").map(String::as_str), Some("round"));
    assert_eq!(
        pv.spec.capacity.get("storage").map(String::as_str),
        Some("1Gi")
    );
    assert_eq!(pv.spec.storage_class_name.as_deref(), Some("fast"));
    assert_eq!(
        pv.spec.persistent_volume_reclaim_policy,
        Some(PersistentVolumeReclaimPolicy::Retain)
    );
    assert_eq!(pv.spec.mount_options, Some(vec!["noatime".to_string()]));
    let claim_ref: &ObjectReference = pv.spec.claim_ref.as_ref().unwrap();
    assert_eq!(claim_ref.name.as_deref(), Some("data"));
    assert_eq!(claim_ref.uid.as_deref(), Some(PVC_UID));
    assert_eq!(
        pv.metadata
            .annotations
            .as_ref()
            .unwrap()
            .get("pv.kubernetes.io/provisioned-by")
            .map(String::as_str),
        Some(DRIVER)
    );
    assert!(pv
        .metadata
        .finalizers
        .as_ref()
        .unwrap()
        .contains(&"external-provisioner.volume.kubernetes.io/finalizer".to_string()));
    assert!(events(&storage)
        .await
        .iter()
        .any(|e| e.reason == "ProvisioningSucceeded"));
}

#[tokio::test]
async fn the_beta_annotation_is_honoured_too() {
    let (storage, p, _rec, _d) = env(Fake::new(), storage_class(None)).await;
    p.sync_claim(&claim(&[(
        "volume.beta.kubernetes.io/storage-provisioner",
        DRIVER,
    )]))
    .await
    .unwrap();
    assert!(pv(&storage).await.is_some());
}

#[tokio::test]
async fn claims_for_other_provisioners_or_already_bound_are_ignored() {
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    p.sync_claim(&claim(&[])).await.unwrap();
    p.sync_claim(&claim(&[(ANN, "other.csi.io")]))
        .await
        .unwrap();
    let mut bound = claim(&[(ANN, DRIVER)]);
    bound.spec.volume_name = Some("pv-x".into());
    p.sync_claim(&bound).await.unwrap();
    assert!(rec.lock().unwrap().create_volume.is_empty());
    assert!(pv(&storage).await.is_none());
}

#[tokio::test]
async fn an_existing_pv_stops_provisioning() {
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    let c = claim(&[(ANN, DRIVER)]);
    p.sync_claim(&c).await.unwrap();
    p.sync_claim(&c).await.unwrap();
    assert_eq!(rec.lock().unwrap().create_volume.len(), 1);
    assert!(pv(&storage).await.is_some());
}

#[tokio::test]
async fn wait_for_first_consumer_needs_a_selected_node() {
    let (storage, p, rec, _d) = env(
        Fake::new(),
        storage_class(Some(VolumeBindingMode::WaitForFirstConsumer)),
    )
    .await;
    p.sync_claim(&claim(&[(ANN, DRIVER)])).await.unwrap();
    assert!(rec.lock().unwrap().create_volume.is_empty());
    p.sync_claim(&claim(&[
        (ANN, DRIVER),
        ("volume.kubernetes.io/selected-node", "node-1"),
    ]))
    .await
    .unwrap();
    assert_eq!(rec.lock().unwrap().create_volume.len(), 1);
    assert!(pv(&storage).await.is_some());
}

#[tokio::test]
async fn a_driver_volume_smaller_than_requested_is_deleted_and_retried() {
    let mut fake = Fake::new();
    fake.capacity = Some(1 << 20);
    let (storage, p, rec, _d) = env(fake, storage_class(None)).await;
    let err = p
        .sync_claim(&claim(&[(ANN, DRIVER)]))
        .await
        .expect_err("capacity shortfall must error (ProvisioningInBackground)");
    assert!(err.to_string().contains("less than requested capacity"));
    let del = rec.lock().unwrap().delete_volume.clone();
    assert_eq!(del.len(), 1);
    assert_eq!(del[0].volume_id, "vol-1");
    assert!(pv(&storage).await.is_none());
}

#[tokio::test]
async fn a_failed_create_volume_emits_provisioning_failed_and_no_pv() {
    let mut fake = Fake::new();
    fake.create_code = Some(tonic::Code::InvalidArgument);
    let (storage, p, _rec, _d) = env(fake, storage_class(None)).await;
    p.sync_claim(&claim(&[(ANN, DRIVER)])).await.unwrap_err();
    assert!(pv(&storage).await.is_none());
    assert!(events(&storage)
        .await
        .iter()
        .any(|e| e.reason == "ProvisioningFailed"));
}

#[tokio::test]
async fn block_mode_requests_a_block_capability_without_fstype() {
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    let mut c = claim(&[(ANN, DRIVER)]);
    c.spec.volume_mode = Some(PersistentVolumeMode::Block);
    p.sync_claim(&c).await.unwrap();
    let reqs = rec.lock().unwrap().create_volume.clone();
    assert!(matches!(
        reqs[0].volume_capabilities[0].access_type,
        Some(volume_capability::AccessType::Block(_))
    ));
    let pv = pv(&storage).await.unwrap();
    assert_eq!(pv.spec.csi.unwrap().fs_type, None);
    assert_eq!(pv.spec.volume_mode, Some(PersistentVolumeMode::Block));
}

#[tokio::test]
async fn a_claim_selector_is_rejected_without_an_rpc() {
    let (_storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    let mut c = claim(&[(ANN, DRIVER)]);
    c.spec.selector = Some(LabelSelector {
        match_labels: Some([("a".to_string(), "b".to_string())].into()),
        match_expressions: None,
    });
    let err = p.sync_claim(&c).await.unwrap_err();
    assert!(err.to_string().contains("claim Selector is not supported"));
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

#[tokio::test]
async fn an_unhandled_data_source_kind_is_left_to_a_populator() {
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    let mut c = claim(&[(ANN, DRIVER)]);
    c.spec.data_source = Some(TypedLocalObjectReference {
        api_group: Some("populator.example.com".into()),
        kind: "Populator".into(),
        name: "p".into(),
    });
    // IgnoredError: no RPC, no PV, not an error.
    p.sync_claim(&c).await.unwrap();
    assert!(rec.lock().unwrap().create_volume.is_empty());
    assert!(pv(&storage).await.is_none());
}

/// KCM half (`provisionClaimOperationExternal`, pv_controller.go:1822-1861):
/// for a StorageClass no in-tree plugin handles, the PV controller sets the
/// storage-provisioner annotations (both) and emits `ExternalProvisioning`.
#[tokio::test]
async fn the_pv_controller_annotates_claims_for_an_external_provisioner() {
    let storage = Arc::new(MemoryStorage::new());
    storage
        .create(
            &build_key("storageclasses", None, "fast"),
            &storage_class(None),
        )
        .await
        .unwrap();
    let c = claim(&[]);
    storage
        .create(
            &build_key("persistentvolumeclaims", Some("ns1"), "data"),
            &c,
        )
        .await
        .unwrap();
    DynamicProvisionerController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    let got: PersistentVolumeClaim = storage
        .get(&build_key("persistentvolumeclaims", Some("ns1"), "data"))
        .await
        .unwrap();
    let ann: &HashMap<String, String> = got.metadata.annotations.as_ref().unwrap();
    assert_eq!(ann.get(ANN).map(String::as_str), Some(DRIVER));
    assert_eq!(
        ann.get("volume.beta.kubernetes.io/storage-provisioner")
            .map(String::as_str),
        Some(DRIVER)
    );
    assert!(events(&storage)
        .await
        .iter()
        .any(|e| e.reason == "ExternalProvisioning"));
    // ... and the in-tree path does not create a PV for it.
    assert!(pv(&storage).await.is_none());
}
