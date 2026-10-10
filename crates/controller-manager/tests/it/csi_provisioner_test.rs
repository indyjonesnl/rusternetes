//! The CSI external-provisioner loop (#2882), driven against a fake CSI
//! Controller service on a unix socket.
//!
//! Upstream: kubernetes-csi/external-provisioner `pkg/controller/controller.go`
//! (`Provision`, `prepareProvision`, `ShouldProvision`) over
//! sig-storage-lib-external-provisioner `controller/controller.go`
//! (`syncClaim`, `provisionClaimOperation`) and `volume_store.go`.

use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::*;
use rusternetes_common::resources::{Event, PersistentVolume, PersistentVolumeClaim, Secret};
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
    delete_code: Option<tonic::Code>,
    /// Controller capabilities beyond CREATE_DELETE_VOLUME.
    extra_caps: Vec<controller_service_capability::rpc::Type>,
    /// A driver that ignores `volume_content_source` (it must echo it).
    drop_content_source: bool,
    /// Report `VOLUME_ACCESSIBILITY_CONSTRAINTS`.
    topology: bool,
    /// `Volume.accessible_topology` of the CreateVolume response.
    accessible_topology: Vec<Topology>,
}

impl Fake {
    fn new() -> Self {
        Self {
            rec: Default::default(),
            capacity: None,
            create_code: None,
            delete_code: None,
            extra_caps: vec![],
            drop_content_source: false,
            topology: false,
            accessible_topology: vec![],
        }
    }

    /// A driver that opts in to `VOLUME_ACCESSIBILITY_CONSTRAINTS`.
    fn with_topology() -> Self {
        let mut f = Self::new();
        f.topology = true;
        f
    }

    /// A driver that can restore snapshots and clone volumes.
    fn with_content_sources() -> Self {
        let mut f = Self::new();
        f.extra_caps = vec![
            controller_service_capability::rpc::Type::CreateDeleteSnapshot,
            controller_service_capability::rpc::Type::CloneVolume,
        ];
        f
    }
}

#[tonic::async_trait]
impl Identity for Fake {
    async fn get_plugin_capabilities(
        &self,
        _r: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        let mut types = vec![plugin_capability::service::Type::ControllerService];
        if self.topology {
            types.push(plugin_capability::service::Type::VolumeAccessibilityConstraints);
        }
        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: types
                .into_iter()
                .map(|t| PluginCapability {
                    r#type: Some(plugin_capability::Type::Service(
                        plugin_capability::Service { r#type: t as i32 },
                    )),
                })
                .collect(),
        }))
    }
}

#[tonic::async_trait]
impl Controller for Fake {
    async fn controller_get_capabilities(
        &self,
        _r: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        let mut types = vec![controller_service_capability::rpc::Type::CreateDeleteVolume];
        types.extend(self.extra_caps.iter().copied());
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
                accessible_topology: self.accessible_topology.clone(),
                content_source: if self.drop_content_source {
                    None
                } else {
                    req.volume_content_source.clone()
                },
            }),
        }))
    }
    async fn delete_volume(
        &self,
        r: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        self.rec.lock().unwrap().delete_volume.push(r.into_inner());
        if let Some(c) = self.delete_code {
            return Err(Status::new(c, "fake delete failure"));
        }
        Ok(Response::new(DeleteVolumeResponse {}))
    }
    async fn controller_publish_volume(
        &self,
        _r: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        Err(Status::unimplemented("not part of the provisioner"))
    }
    async fn controller_unpublish_volume(
        &self,
        _r: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        Err(Status::unimplemented("not part of the provisioner"))
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

// ---- data sources (#2963): getVolumeContentSource / getPVCSource /
// getSnapshotSource (external-provisioner controller.go:1184 / :1198 / :1289),
// the clone / snapshot-protection finalizers (:1054 / :1070 / :1097) and
// Provision's "volume content source missing" cleanup (:931-:945).

const SNAP_GROUP: &str = "snapshot.storage.k8s.io";
const SNAP_UID: &str = "snap-uid-1";
const CLONE_FINALIZER: &str = "provisioner.storage.kubernetes.io/cloning-protection";
const SNAP_FINALIZER: &str =
    "provisioner.storage.kubernetes.io/volumesnapshot-as-source-protection";

fn snapshot_claim() -> PersistentVolumeClaim {
    let mut c = claim(&[(ANN, DRIVER)]);
    c.spec.data_source = Some(TypedLocalObjectReference {
        api_group: Some(SNAP_GROUP.into()),
        kind: "VolumeSnapshot".into(),
        name: "snap".into(),
    });
    c
}

fn clone_claim() -> PersistentVolumeClaim {
    let mut c = claim(&[(ANN, DRIVER)]);
    c.spec.data_source = Some(TypedLocalObjectReference {
        api_group: None,
        kind: "PersistentVolumeClaim".into(),
        name: "src".into(),
    });
    c
}

fn snapshot(ready: bool, restore_size: Option<&str>) -> VolumeSnapshot {
    let mut meta = ObjectMeta::new("snap").with_namespace("ns1");
    meta.uid = SNAP_UID.into();
    VolumeSnapshot {
        type_meta: TypeMeta {
            kind: "VolumeSnapshot".into(),
            api_version: "snapshot.storage.k8s.io/v1".into(),
        },
        metadata: meta,
        spec: Default::default(),
        status: Some(VolumeSnapshotStatus {
            bound_volume_snapshot_content_name: Some("snapcontent-1".into()),
            creation_time: None,
            ready_to_use: Some(ready),
            restore_size: restore_size.map(String::from),
            error: None,
        }),
    }
}

fn snapshot_content(driver: &str, handle: Option<&str>) -> VolumeSnapshotContent {
    VolumeSnapshotContent {
        type_meta: TypeMeta {
            kind: "VolumeSnapshotContent".into(),
            api_version: "snapshot.storage.k8s.io/v1".into(),
        },
        metadata: ObjectMeta::new("snapcontent-1"),
        spec: VolumeSnapshotContentSpec {
            driver: driver.into(),
            volume_snapshot_ref: ObjectReference {
                name: Some("snap".into()),
                namespace: Some("ns1".into()),
                uid: Some(SNAP_UID.into()),
                ..Default::default()
            },
            ..Default::default()
        },
        status: Some(VolumeSnapshotContentStatus {
            snapshot_handle: handle.map(String::from),
            creation_time: None,
            ready_to_use: Some(true),
            restore_size: None,
            error: None,
        }),
    }
}

async fn put_snapshot(storage: &MemoryStorage, s: &VolumeSnapshot, c: &VolumeSnapshotContent) {
    storage
        .create(&build_key("volumesnapshots", Some("ns1"), "snap"), s)
        .await
        .unwrap();
    storage
        .create(
            &build_key("volumesnapshotcontents", None, "snapcontent-1"),
            c,
        )
        .await
        .unwrap();
}

async fn snapshot_finalizers(storage: &MemoryStorage) -> Vec<String> {
    let s: VolumeSnapshot = storage
        .get(&build_key("volumesnapshots", Some("ns1"), "snap"))
        .await
        .unwrap();
    s.metadata.finalizers.unwrap_or_default()
}

/// A bound CSI source PVC `src` and its Bound PV `pv-src`.
async fn put_source_pvc(storage: &MemoryStorage, driver: &str, size: &str) {
    let mut meta = ObjectMeta::new("src").with_namespace("ns1");
    meta.uid = "src-uid".into();
    let mut src = claim(&[]);
    src.metadata = meta;
    src.spec.volume_name = Some("pv-src".into());
    src.spec.resources.requests = Some([("storage".to_string(), size.to_string())].into());
    src.status = Some(PersistentVolumeClaimStatus {
        phase: PersistentVolumeClaimPhase::Bound,
        ..Default::default()
    });
    storage
        .create(
            &build_key("persistentvolumeclaims", Some("ns1"), "src"),
            &src,
        )
        .await
        .unwrap();
    let pv = PersistentVolume {
        type_meta: TypeMeta {
            kind: "PersistentVolume".into(),
            api_version: "v1".into(),
        },
        metadata: ObjectMeta::new("pv-src"),
        spec: PersistentVolumeSpec {
            csi: Some(CSIVolumeSource {
                driver: driver.into(),
                volume_handle: Some("vol-src".into()),
                ..Default::default()
            }),
            claim_ref: Some(ObjectReference {
                kind: Some("PersistentVolumeClaim".into()),
                namespace: Some("ns1".into()),
                name: Some("src".into()),
                uid: Some("src-uid".into()),
                ..Default::default()
            }),
            ..Default::default()
        },
        status: Some(PersistentVolumeStatus {
            phase: PersistentVolumePhase::Bound,
            message: None,
            reason: None,
            last_phase_transition_time: None,
        }),
    };
    storage
        .create(&build_key("persistentvolumes", None, "pv-src"), &pv)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_snapshot_data_source_becomes_a_snapshot_content_source() {
    let (storage, p, rec, _d) = env(Fake::with_content_sources(), storage_class(None)).await;
    put_snapshot(
        &storage,
        &snapshot(true, Some("1Gi")),
        &snapshot_content(DRIVER, Some("snap-handle-1")),
    )
    .await;
    p.sync_claim(&snapshot_claim()).await.unwrap();

    let reqs = rec.lock().unwrap().create_volume.clone();
    assert_eq!(reqs.len(), 1);
    match reqs[0]
        .volume_content_source
        .as_ref()
        .and_then(|s| s.r#type.as_ref())
    {
        Some(volume_content_source::Type::Snapshot(s)) => {
            assert_eq!(s.snapshot_id, "snap-handle-1")
        }
        other => panic!("expected a snapshot content source, got {other:?}"),
    }
    assert!(pv(&storage).await.is_some());
    // setSnapshotFinalizer ran before CreateVolume; removeSnapshotFinalizer
    // after the PV was built (Provision, :1043-:1049).
    assert!(!snapshot_finalizers(&storage)
        .await
        .contains(&SNAP_FINALIZER.to_string()));
}

#[tokio::test]
async fn a_snapshot_is_protected_while_its_volume_is_not_yet_provisioned() {
    let mut fake = Fake::with_content_sources();
    fake.create_code = Some(tonic::Code::Unavailable);
    let (storage, p, _rec, _d) = env(fake, storage_class(None)).await;
    put_snapshot(
        &storage,
        &snapshot(true, None),
        &snapshot_content(DRIVER, Some("h")),
    )
    .await;
    p.sync_claim(&snapshot_claim()).await.unwrap_err();
    assert!(snapshot_finalizers(&storage)
        .await
        .contains(&SNAP_FINALIZER.to_string()));
}

/// `--prevent-volume-mode-conversion` (default on): a Block claim over a
/// Filesystem snapshot needs the allow-volume-mode-change annotation.
#[tokio::test]
async fn a_volume_mode_conversion_needs_the_snapshot_content_annotation() {
    let (storage, p, rec, _d) = env(Fake::with_content_sources(), storage_class(None)).await;
    let mut content = snapshot_content(DRIVER, Some("h"));
    content.spec.source_volume_mode = Some("Filesystem".into());
    put_snapshot(&storage, &snapshot(true, None), &content).await;
    let mut c = snapshot_claim();
    c.spec.volume_mode = Some(PersistentVolumeMode::Block);
    let err = p.sync_claim(&c).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("modifies the mode of the source volume but does not have permission"),
        "{err}"
    );
    assert!(rec.lock().unwrap().create_volume.is_empty());

    content
        .metadata
        .annotations
        .get_or_insert_with(Default::default)
        .insert(
            "snapshot.storage.kubernetes.io/allow-volume-mode-change".into(),
            "true".into(),
        );
    storage
        .update(
            &build_key("volumesnapshotcontents", None, "snapcontent-1"),
            &content,
        )
        .await
        .unwrap();
    p.sync_claim(&c).await.unwrap();
    assert_eq!(rec.lock().unwrap().create_volume.len(), 1);
}

#[tokio::test]
async fn an_unready_snapshot_is_not_restored() {
    let (storage, p, rec, _d) = env(Fake::with_content_sources(), storage_class(None)).await;
    put_snapshot(
        &storage,
        &snapshot(false, None),
        &snapshot_content(DRIVER, Some("h")),
    )
    .await;
    let err = p.sync_claim(&snapshot_claim()).await.unwrap_err();
    assert!(
        err.to_string().contains("snapshot snap is not Ready"),
        "{err}"
    );
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

#[tokio::test]
async fn a_snapshot_of_another_driver_is_refused() {
    let (storage, p, rec, _d) = env(Fake::with_content_sources(), storage_class(None)).await;
    put_snapshot(
        &storage,
        &snapshot(true, None),
        &snapshot_content("other.csi.io", Some("h")),
    )
    .await;
    let err = p.sync_claim(&snapshot_claim()).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("is not handled by CSI driver of StorageClass fast"),
        "{err}"
    );
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

#[tokio::test]
async fn a_snapshot_without_a_handle_is_refused() {
    let (storage, p, rec, _d) = env(Fake::with_content_sources(), storage_class(None)).await;
    put_snapshot(
        &storage,
        &snapshot(true, None),
        &snapshot_content(DRIVER, None),
    )
    .await;
    let err = p.sync_claim(&snapshot_claim()).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("snapshot handle snap is not available"),
        "{err}"
    );
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

#[tokio::test]
async fn a_claim_smaller_than_the_snapshot_restore_size_is_refused() {
    let (storage, p, rec, _d) = env(Fake::with_content_sources(), storage_class(None)).await;
    put_snapshot(
        &storage,
        &snapshot(true, Some("2Gi")),
        &snapshot_content(DRIVER, Some("h")),
    )
    .await;
    let err = p.sync_claim(&snapshot_claim()).await.unwrap_err();
    assert!(err.to_string().contains("is less than the size"), "{err}");
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

#[tokio::test]
async fn a_snapshot_data_source_of_the_wrong_api_group_is_rejected() {
    let (_storage, p, rec, _d) = env(Fake::with_content_sources(), storage_class(None)).await;
    let mut c = snapshot_claim();
    c.spec.data_source.as_mut().unwrap().api_group = Some("example.com".into());
    let err = p.sync_claim(&c).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("the PVC source does not belong to the right APIGroup"),
        "{err}"
    );
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

#[tokio::test]
async fn a_snapshot_source_needs_create_delete_snapshot() {
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    put_snapshot(
        &storage,
        &snapshot(true, None),
        &snapshot_content(DRIVER, Some("h")),
    )
    .await;
    let err = p.sync_claim(&snapshot_claim()).await.unwrap_err();
    assert!(err.to_string().contains("CREATE_DELETE_SNAPSHOT"), "{err}");
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

#[tokio::test]
async fn a_pvc_data_source_becomes_a_volume_content_source_and_is_protected() {
    let (storage, p, rec, _d) = env(Fake::with_content_sources(), storage_class(None)).await;
    put_source_pvc(&storage, DRIVER, "1Gi").await;
    p.sync_claim(&clone_claim()).await.unwrap();

    let reqs = rec.lock().unwrap().create_volume.clone();
    assert_eq!(reqs.len(), 1);
    match reqs[0]
        .volume_content_source
        .as_ref()
        .and_then(|s| s.r#type.as_ref())
    {
        Some(volume_content_source::Type::Volume(v)) => assert_eq!(v.volume_id, "vol-src"),
        other => panic!("expected a volume content source, got {other:?}"),
    }
    assert!(pv(&storage).await.is_some());
    // setCloneFinalizer (:1054): the source stays until the clone is bound.
    let src: PersistentVolumeClaim = storage
        .get(&build_key("persistentvolumeclaims", Some("ns1"), "src"))
        .await
        .unwrap();
    assert!(src
        .metadata
        .finalizers
        .unwrap_or_default()
        .contains(&CLONE_FINALIZER.to_string()));
}

#[tokio::test]
async fn a_clone_smaller_than_its_source_is_refused() {
    let (storage, p, rec, _d) = env(Fake::with_content_sources(), storage_class(None)).await;
    put_source_pvc(&storage, DRIVER, "2Gi").await;
    let err = p.sync_claim(&clone_claim()).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("must be greater than or equal in size to the specified PVC data source"),
        "{err}"
    );
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

#[tokio::test]
async fn a_clone_of_another_drivers_volume_is_refused() {
    let (storage, p, rec, _d) = env(Fake::with_content_sources(), storage_class(None)).await;
    put_source_pvc(&storage, "other.csi.io", "1Gi").await;
    let err = p.sync_claim(&clone_claim()).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("claim in dataSource not bound or invalid"),
        "{err}"
    );
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

#[tokio::test]
async fn a_clone_needs_clone_volume() {
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    put_source_pvc(&storage, DRIVER, "1Gi").await;
    let err = p.sync_claim(&clone_claim()).await.unwrap_err();
    assert!(err.to_string().contains("CLONE_VOLUME"), "{err}");
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

/// Provision :931-:945: a driver that ignores the content source has made a
/// blank volume; delete it and retry in the background.
#[tokio::test]
async fn a_volume_without_the_requested_content_source_is_deleted() {
    let mut fake = Fake::with_content_sources();
    fake.drop_content_source = true;
    let (storage, p, rec, _d) = env(fake, storage_class(None)).await;
    put_source_pvc(&storage, DRIVER, "1Gi").await;
    let err = p.sync_claim(&clone_claim()).await.unwrap_err();
    assert!(
        err.to_string().contains("volume content source missing"),
        "{err}"
    );
    let del = rec.lock().unwrap().delete_volume.clone();
    assert_eq!(del.len(), 1);
    assert_eq!(del[0].volume_id, "vol-1");
    assert!(pv(&storage).await.is_none());
}

/// `dataSource` (:2096): the cross-namespace form needs the
/// CrossNamespaceVolumeDataSource gate, off here.
#[tokio::test]
async fn a_data_source_ref_namespace_needs_the_cross_namespace_gate() {
    let (_storage, p, rec, _d) = env(Fake::with_content_sources(), storage_class(None)).await;
    let mut c = clone_claim();
    c.spec.data_source = None;
    c.spec.data_source_ref = Some(TypedObjectReference {
        api_group: None,
        kind: "PersistentVolumeClaim".into(),
        name: "src".into(),
        namespace: Some("other".into()),
    });
    let err = p.sync_claim(&c).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("CrossNamespaceVolumeDataSource feature is disabled"),
        "{err}"
    );
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

// ---- delete path (#2965) ----------------------------------------------------
//
// sig-storage-lib-external-provisioner `controller/controller.go`:
// `syncVolume` (:1149), `isProvisionerForVolume` (:1175),
// `handleProtectionFinalizer` (:1203), `shouldDelete` (:1285),
// `deleteVolumeOperation` (:1636); external-provisioner
// `pkg/controller/controller.go` `Delete` (:1390), `handleSecretsForDeletion`,
// `canDeleteVolume` (:1526).

const FINALIZER: &str = "external-provisioner.volume.kubernetes.io/finalizer";

fn csi_pv(
    name: &str,
    phase: PersistentVolumePhase,
    policy: PersistentVolumeReclaimPolicy,
) -> PersistentVolume {
    let mut meta = ObjectMeta::new(name);
    meta.annotations = Some(
        [(
            "pv.kubernetes.io/provisioned-by".to_string(),
            DRIVER.to_string(),
        )]
        .into(),
    );
    meta.finalizers = Some(vec![FINALIZER.to_string()]);
    PersistentVolume {
        type_meta: TypeMeta {
            kind: "PersistentVolume".into(),
            api_version: "v1".into(),
        },
        metadata: meta,
        spec: PersistentVolumeSpec {
            persistent_volume_reclaim_policy: Some(policy),
            csi: Some(CSIVolumeSource {
                driver: DRIVER.into(),
                volume_handle: Some("vol-1".into()),
                ..Default::default()
            }),
            ..Default::default()
        },
        status: Some(PersistentVolumeStatus {
            phase,
            ..Default::default()
        }),
    }
}

async fn put_pv(storage: &MemoryStorage, pv: &PersistentVolume) {
    storage
        .create(&build_key("persistentvolumes", None, &pv.metadata.name), pv)
        .await
        .unwrap();
}

async fn get_pv(storage: &MemoryStorage, name: &str) -> Option<PersistentVolume> {
    storage
        .get(&build_key("persistentvolumes", None, name))
        .await
        .ok()
}

fn deleted(rec: &Arc<Mutex<Recorded>>) -> Vec<DeleteVolumeRequest> {
    rec.lock().unwrap().delete_volume.clone()
}

#[tokio::test]
async fn a_released_delete_pv_is_deleted_and_its_finalizer_released() {
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    let pv = csi_pv(
        "pv1",
        PersistentVolumePhase::Released,
        PersistentVolumeReclaimPolicy::Delete,
    );
    put_pv(&storage, &pv).await;
    p.sync_volume(&pv).await.unwrap();
    let reqs = deleted(&rec);
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].volume_id, "vol-1");
    assert!(get_pv(&storage, "pv1").await.is_none(), "PV removed");
}

#[tokio::test]
async fn a_pv_that_should_not_be_deleted_is_left_alone() {
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    let cases = [
        // `shouldDelete`: Retain reclaim policy.
        (
            "retain",
            PersistentVolumePhase::Released,
            PersistentVolumeReclaimPolicy::Retain,
        ),
        // `shouldDelete`: not Released.
        (
            "bound",
            PersistentVolumePhase::Bound,
            PersistentVolumeReclaimPolicy::Delete,
        ),
        (
            "failed",
            PersistentVolumePhase::Failed,
            PersistentVolumeReclaimPolicy::Delete,
        ),
    ];
    for (name, phase, policy) in cases {
        let pv = csi_pv(name, phase, policy);
        put_pv(&storage, &pv).await;
        p.sync_volume(&pv).await.unwrap();
        assert!(get_pv(&storage, name).await.is_some(), "{name} kept");
    }
    assert!(deleted(&rec).is_empty());

    // `shouldDelete`: "The finalizer was removed, i.e. the volume has been
    // already deleted."
    let mut pv = csi_pv(
        "gone",
        PersistentVolumePhase::Released,
        PersistentVolumeReclaimPolicy::Delete,
    );
    pv.metadata.finalizers = None;
    pv.metadata.deletion_timestamp = Some(chrono::Utc::now());
    put_pv(&storage, &pv).await;
    p.sync_volume(&pv).await.unwrap();
    assert!(deleted(&rec).is_empty());
}

#[tokio::test]
async fn another_drivers_pv_is_not_ours_to_delete() {
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    let mut pv = csi_pv(
        "other",
        PersistentVolumePhase::Released,
        PersistentVolumeReclaimPolicy::Delete,
    );
    pv.metadata
        .annotations
        .as_mut()
        .unwrap()
        .insert("pv.kubernetes.io/provisioned-by".into(), "other.csi".into());
    put_pv(&storage, &pv).await;
    p.sync_volume(&pv).await.unwrap();
    assert!(deleted(&rec).is_empty());
    assert!(get_pv(&storage, "other").await.is_some());

    // A statically provisioned CSI PV is ours iff `spec.csi.driver` is.
    let mut stat = csi_pv(
        "static",
        PersistentVolumePhase::Released,
        PersistentVolumeReclaimPolicy::Delete,
    );
    stat.metadata.annotations = None;
    put_pv(&storage, &stat).await;
    p.sync_volume(&stat).await.unwrap();
    assert_eq!(deleted(&rec).len(), 1);
}

#[tokio::test]
async fn the_protection_finalizer_follows_the_reclaim_policy() {
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;

    // Bound + Delete: the finalizer is added.
    let mut pv = csi_pv(
        "bound",
        PersistentVolumePhase::Bound,
        PersistentVolumeReclaimPolicy::Delete,
    );
    pv.metadata.finalizers = None;
    put_pv(&storage, &pv).await;
    p.sync_volume(&pv).await.unwrap();
    let got = get_pv(&storage, "bound").await.unwrap();
    assert_eq!(got.metadata.finalizers, Some(vec![FINALIZER.to_string()]));

    // Reclaim policy changed to Retain: the finalizer is removed.
    let pv = csi_pv(
        "retained",
        PersistentVolumePhase::Bound,
        PersistentVolumeReclaimPolicy::Retain,
    );
    put_pv(&storage, &pv).await;
    p.sync_volume(&pv).await.unwrap();
    let got = get_pv(&storage, "retained").await.unwrap();
    assert!(got.metadata.finalizers.unwrap_or_default().is_empty());
    assert!(deleted(&rec).is_empty());
}

fn with_secret_annotations(mut pv: PersistentVolume) -> PersistentVolume {
    let ann = pv.metadata.annotations.as_mut().unwrap();
    ann.insert(
        "volume.kubernetes.io/provisioner-deletion-secret-name".into(),
        "creds".into(),
    );
    ann.insert(
        "volume.kubernetes.io/provisioner-deletion-secret-namespace".into(),
        "sec-ns".into(),
    );
    pv
}

#[tokio::test]
async fn deletion_secrets_come_from_the_provisioner_annotations() {
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    let secret =
        Secret::new("creds", "sec-ns").with_data([("user".to_string(), b"admin".to_vec())].into());
    storage
        .create(&build_key("secrets", Some("sec-ns"), "creds"), &secret)
        .await
        .unwrap();
    let pv = with_secret_annotations(csi_pv(
        "pv1",
        PersistentVolumePhase::Released,
        PersistentVolumeReclaimPolicy::Delete,
    ));
    put_pv(&storage, &pv).await;
    p.sync_volume(&pv).await.unwrap();
    let reqs = deleted(&rec);
    assert_eq!(reqs.len(), 1);
    assert_eq!(
        reqs[0].secrets.get("user").map(String::as_str),
        Some("admin")
    );
}

#[tokio::test]
async fn a_missing_deletion_secret_does_not_block_deletion() {
    // "Continue with deletion, as the secret may have already been deleted."
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    let pv = with_secret_annotations(csi_pv(
        "pv1",
        PersistentVolumePhase::Released,
        PersistentVolumeReclaimPolicy::Delete,
    ));
    put_pv(&storage, &pv).await;
    p.sync_volume(&pv).await.unwrap();
    assert_eq!(deleted(&rec).len(), 1);
    assert!(deleted(&rec)[0].secrets.is_empty());
}

#[tokio::test]
async fn a_failed_delete_volume_keeps_the_pv_and_warns() {
    let mut fake = Fake::new();
    fake.delete_code = Some(tonic::Code::Internal);
    let (storage, p, _rec, _d) = env(fake, storage_class(None)).await;
    let pv = csi_pv(
        "pv1",
        PersistentVolumePhase::Released,
        PersistentVolumeReclaimPolicy::Delete,
    );
    put_pv(&storage, &pv).await;
    p.sync_volume(&pv).await.unwrap_err();
    let got = get_pv(&storage, "pv1")
        .await
        .expect("PV kept for the retry");
    assert_eq!(got.metadata.finalizers, Some(vec![FINALIZER.to_string()]));
    assert!(events(&storage)
        .await
        .iter()
        .any(|e| e.reason == "VolumeFailedDelete"));
}

#[tokio::test]
async fn an_attached_volume_postpones_deletion_without_failing_it() {
    // `canDeleteVolume` (controller.go:1526): only when the driver can
    // PUBLISH_UNPUBLISH_VOLUME (csi-provisioner.go:312).
    let mut fake = Fake::new();
    fake.extra_caps = vec![controller_service_capability::rpc::Type::PublishUnpublishVolume];
    let (storage, p, rec, _d) = env(fake, storage_class(None)).await;
    let va = rusternetes_common::resources::csi::VolumeAttachment {
        type_meta: TypeMeta {
            kind: "VolumeAttachment".into(),
            api_version: "storage.k8s.io/v1".into(),
        },
        metadata: ObjectMeta::new("va1"),
        spec: rusternetes_common::resources::csi::VolumeAttachmentSpec {
            attacher: DRIVER.into(),
            node_name: "node-1".into(),
            source: rusternetes_common::resources::csi::VolumeAttachmentSource {
                persistent_volume_name: Some("pv1".into()),
                inline_volume_spec: None,
            },
        },
        status: None,
    };
    storage
        .create(&build_key("volumeattachments", None, "va1"), &va)
        .await
        .unwrap();
    let pv = csi_pv(
        "pv1",
        PersistentVolumePhase::Released,
        PersistentVolumeReclaimPolicy::Delete,
    );
    put_pv(&storage, &pv).await;
    let err = p.sync_volume(&pv).await.unwrap_err();
    assert!(
        err.to_string().contains("still attached to node node-1"),
        "{err}"
    );
    assert!(deleted(&rec).is_empty());
    assert!(get_pv(&storage, "pv1").await.is_some());
    let evs = events(&storage).await;
    assert!(evs.iter().any(|e| e.reason == "VolumeDelete"));
    assert!(!evs.iter().any(|e| e.reason == "VolumeFailedDelete"));
}

// ---- infeasible slow retry (#2965) ---------------------------------------------
//
// lib `delayProvisioningIfRecentlyInfeasible` (:1545), `markForSlowRetry`
// (:1566), `slowset.SlowSet` (csi-lib-utils).

#[tokio::test]
async fn an_infeasible_claim_is_not_retried_until_the_slow_set_expires() {
    let mut fake = Fake::new();
    fake.create_code = Some(tonic::Code::InvalidArgument);
    let (storage, p, rec, _d) = env(fake, storage_class(None)).await;
    let c = claim(&[(ANN, DRIVER)]);
    p.sync_claim(&c).await.unwrap_err();
    assert_eq!(rec.lock().unwrap().create_volume.len(), 1);

    // Second sync: delayed, the driver is not asked again.
    let err = p.sync_claim(&c).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("skipping volume provisioning for pvc"),
        "{err}"
    );
    assert_eq!(rec.lock().unwrap().create_volume.len(), 1);

    // A replaced StorageClass (new UID) clears the delay (:1555-1558).
    let mut sc = storage_class(None);
    sc.metadata.uid = "new-class-uid".into();
    storage
        .update(&build_key("storageclasses", None, "fast"), &sc)
        .await
        .unwrap();
    p.sync_claim(&c).await.unwrap_err();
    assert_eq!(rec.lock().unwrap().create_volume.len(), 2);
}

#[tokio::test]
async fn the_slow_set_expires_after_the_retry_interval_max() {
    let mut fake = Fake::new();
    fake.create_code = Some(tonic::Code::InvalidArgument);
    let (_storage, p, rec, _d) = env(fake, storage_class(None)).await;
    let p = p.with_retry_interval_max(std::time::Duration::from_millis(150));
    let c = claim(&[(ANN, DRIVER)]);
    p.sync_claim(&c).await.unwrap_err();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    p.sync_claim(&c).await.unwrap_err();
    assert_eq!(rec.lock().unwrap().create_volume.len(), 2);
}

#[tokio::test]
async fn a_non_infeasible_failure_is_not_slow_retried() {
    let mut fake = Fake::new();
    fake.create_code = Some(tonic::Code::Internal);
    let (_storage, p, rec, _d) = env(fake, storage_class(None)).await;
    let c = claim(&[(ANN, DRIVER)]);
    p.sync_claim(&c).await.unwrap_err();
    p.sync_claim(&c).await.unwrap_err();
    assert_eq!(rec.lock().unwrap().create_volume.len(), 2);
}

// ---- secrets and VolumeAttributesClass (#2964) ------------------------------
//
// external-provisioner `pkg/controller/controller.go`: `prepareProvision`
// (:743-:826), `getSecretReference` (:1922), `getCredentials` (:2004),
// `getSecretsFromSC` (:1484); `Provision` (:985-:1018).

use rusternetes_common::resources::csi::VolumeAttributesClass;

fn with_params(mut sc: StorageClass, extra: &[(&str, &str)]) -> StorageClass {
    let p = sc.parameters.get_or_insert_with(Default::default);
    for (k, v) in extra {
        p.insert(k.to_string(), v.to_string());
    }
    sc
}

async fn put_secret(storage: &MemoryStorage, ns: &str, name: &str, k: &str, v: &str) {
    let secret = Secret::new(name, ns).with_data([(k.to_string(), v.as_bytes().to_vec())].into());
    storage
        .create(&build_key("secrets", Some(ns), name), &secret)
        .await
        .unwrap();
}

fn secret_params() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "csi.storage.k8s.io/provisioner-secret-name",
            "prov-${pvc.name}",
        ),
        (
            "csi.storage.k8s.io/provisioner-secret-namespace",
            "${pvc.namespace}",
        ),
        ("csi.storage.k8s.io/controller-publish-secret-name", "cp"),
        (
            "csi.storage.k8s.io/controller-publish-secret-namespace",
            "sec-ns",
        ),
        ("csi.storage.k8s.io/node-stage-secret-name", "ns-${pv.name}"),
        ("csi.storage.k8s.io/node-stage-secret-namespace", "sec-ns"),
        (
            "csi.storage.k8s.io/node-publish-secret-name",
            "${pvc.annotations['example.com/np']}",
        ),
        ("csi.storage.k8s.io/node-publish-secret-namespace", "sec-ns"),
        ("csi.storage.k8s.io/controller-expand-secret-name", "ce"),
        (
            "csi.storage.k8s.io/controller-expand-secret-namespace",
            "sec-ns",
        ),
        ("csi.storage.k8s.io/node-expand-secret-name", "ne"),
        ("csi.storage.k8s.io/node-expand-secret-namespace", "sec-ns"),
        ("csi.storage.k8s.io/controller-modify-secret-name", "cm"),
        (
            "csi.storage.k8s.io/controller-modify-secret-namespace",
            "sec-ns",
        ),
    ]
}

#[tokio::test]
async fn provisioner_secrets_are_resolved_and_recorded_on_the_pv() {
    let sc = with_params(storage_class(None), &secret_params());
    let (storage, p, rec, _d) = env(Fake::new(), sc).await;
    put_secret(&storage, "ns1", "prov-data", "token", "s3cret").await;
    p.sync_claim(&claim(&[(ANN, DRIVER), ("example.com/np", "np-secret")]))
        .await
        .unwrap();

    let reqs = rec.lock().unwrap().create_volume.clone();
    assert_eq!(reqs.len(), 1, "CreateVolume must be issued");
    assert_eq!(
        reqs[0].secrets.get("token").map(String::as_str),
        Some("s3cret")
    );
    // The secret parameters are stripped from the driver parameters.
    assert_eq!(reqs[0].parameters.len(), 1);

    let pv = pv(&storage).await.expect("PV created");
    let ann = pv.metadata.annotations.as_ref().unwrap();
    let a = |k: &str| ann.get(k).map(String::as_str);
    assert_eq!(
        a("volume.kubernetes.io/provisioner-deletion-secret-name"),
        Some("prov-data")
    );
    assert_eq!(
        a("volume.kubernetes.io/provisioner-deletion-secret-namespace"),
        Some("ns1")
    );
    assert_eq!(
        a("volume.kubernetes.io/controller-modify-secret-name"),
        Some("cm")
    );
    assert_eq!(
        a("volume.kubernetes.io/controller-modify-secret-namespace"),
        Some("sec-ns")
    );

    let csi = pv.spec.csi.as_ref().unwrap();
    let r = |s: &Option<SecretReference>| {
        s.as_ref()
            .map(|s| (s.name.clone().unwrap(), s.namespace.clone().unwrap()))
    };
    let want = |n: &str| Some((n.to_string(), "sec-ns".to_string()));
    assert_eq!(r(&csi.controller_publish_secret_ref), want("cp"));
    assert_eq!(
        r(&csi.node_stage_secret_ref),
        want(&format!("ns-pvc-{PVC_UID}"))
    );
    assert_eq!(r(&csi.node_publish_secret_ref), want("np-secret"));
    assert_eq!(r(&csi.controller_expand_secret_ref), want("ce"));
    assert_eq!(r(&csi.node_expand_secret_ref), want("ne"));
}

#[tokio::test]
async fn the_default_secret_parameters_cover_every_call() {
    // `defaultSecretParams` (:174): `csi.storage.k8s.io/secret-*`.
    let sc = with_params(
        storage_class(None),
        &[
            ("csi.storage.k8s.io/secret-name", "dflt"),
            ("csi.storage.k8s.io/secret-namespace", "sec-ns"),
        ],
    );
    let (storage, p, rec, _d) = env(Fake::new(), sc).await;
    put_secret(&storage, "sec-ns", "dflt", "k", "v").await;
    p.sync_claim(&claim(&[(ANN, DRIVER)])).await.unwrap();
    let reqs = rec.lock().unwrap().create_volume.clone();
    assert_eq!(reqs[0].secrets.get("k").map(String::as_str), Some("v"));
    let pv = pv(&storage).await.unwrap();
    assert_eq!(
        pv.spec
            .csi
            .unwrap()
            .node_stage_secret_ref
            .unwrap()
            .name
            .as_deref(),
        Some("dflt")
    );
}

#[tokio::test]
async fn a_name_without_a_namespace_secret_parameter_is_refused() {
    let sc = with_params(
        storage_class(None),
        &[("csi.storage.k8s.io/provisioner-secret-name", "only-name")],
    );
    let (_storage, p, rec, _d) = env(Fake::new(), sc).await;
    let err = p.sync_claim(&claim(&[(ANN, DRIVER)])).await.unwrap_err();
    assert!(
        err.to_string().contains("Both must be specified"),
        "got: {err}"
    );
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

#[tokio::test]
async fn an_unresolvable_secret_template_token_is_refused() {
    let sc = with_params(
        storage_class(None),
        &[
            ("csi.storage.k8s.io/provisioner-secret-name", "${pvc.bogus}"),
            ("csi.storage.k8s.io/provisioner-secret-namespace", "ns1"),
        ],
    );
    let (_storage, p, rec, _d) = env(Fake::new(), sc).await;
    let err = p.sync_claim(&claim(&[(ANN, DRIVER)])).await.unwrap_err();
    assert!(err.to_string().contains("invalid tokens"), "got: {err}");
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

#[tokio::test]
async fn a_missing_provisioner_secret_fails_before_create_volume() {
    // `getCredentials` error -> ProvisioningNoChange (:749-:752).
    let sc = with_params(
        storage_class(None),
        &[
            ("csi.storage.k8s.io/provisioner-secret-name", "absent"),
            ("csi.storage.k8s.io/provisioner-secret-namespace", "ns1"),
        ],
    );
    let (_storage, p, rec, _d) = env(Fake::new(), sc).await;
    let err = p.sync_claim(&claim(&[(ANN, DRIVER)])).await.unwrap_err();
    assert!(
        err.to_string().contains("error getting secret absent"),
        "got: {err}"
    );
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

#[tokio::test]
async fn class_derived_deletion_secrets_are_resolved_for_a_pv_without_annotations() {
    // `getSecretsFromSC` (:1484): the claim is rebuilt from the PV's claimRef.
    let sc = with_params(
        storage_class(None),
        &[
            (
                "csi.storage.k8s.io/provisioner-secret-name",
                "${pvc.name}-del",
            ),
            (
                "csi.storage.k8s.io/provisioner-secret-namespace",
                "${pvc.namespace}",
            ),
        ],
    );
    let (storage, p, rec, _d) = env(Fake::new(), sc).await;
    put_secret(&storage, "ns1", "data-del", "user", "admin").await;
    let mut pv = csi_pv(
        "pv1",
        PersistentVolumePhase::Released,
        PersistentVolumeReclaimPolicy::Delete,
    );
    pv.spec.storage_class_name = Some("fast".into());
    pv.spec.claim_ref = Some(ObjectReference {
        name: Some("data".into()),
        namespace: Some("ns1".into()),
        ..Default::default()
    });
    put_pv(&storage, &pv).await;
    p.sync_volume(&pv).await.unwrap();
    let reqs = deleted(&rec);
    assert_eq!(reqs.len(), 1);
    assert_eq!(
        reqs[0].secrets.get("user").map(String::as_str),
        Some("admin")
    );
}

fn vac(driver: &str, params: &[(&str, &str)]) -> VolumeAttributesClass {
    VolumeAttributesClass {
        type_meta: TypeMeta {
            kind: "VolumeAttributesClass".into(),
            api_version: "storage.k8s.io/v1".into(),
        },
        metadata: ObjectMeta::new("gold"),
        driver_name: driver.into(),
        parameters: Some(
            params
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        ),
    }
}

async fn put_vac(storage: &MemoryStorage, v: &VolumeAttributesClass) {
    storage
        .create(&build_key("volumeattributesclasses", None, "gold"), v)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_volume_attributes_class_becomes_mutable_parameters() {
    // `req.MutableParameters = vac.Parameters` (:814-:824) and
    // `pv.Spec.VolumeAttributesClassName` (:1016-:1018).
    let mut fake = Fake::new();
    fake.extra_caps = vec![controller_service_capability::rpc::Type::ModifyVolume];
    let (storage, p, rec, _d) = env(fake, storage_class(None)).await;
    put_vac(&storage, &vac(DRIVER, &[("iops", "3000")])).await;
    let mut c = claim(&[(ANN, DRIVER)]);
    c.spec.volume_attributes_class_name = Some("gold".into());
    p.sync_claim(&c).await.unwrap();
    let reqs = rec.lock().unwrap().create_volume.clone();
    assert_eq!(reqs.len(), 1);
    assert_eq!(
        reqs[0].mutable_parameters.get("iops").map(String::as_str),
        Some("3000")
    );
    let pv = pv(&storage).await.unwrap();
    assert_eq!(
        pv.spec.volume_attributes_class_name.as_deref(),
        Some("gold")
    );
}

#[tokio::test]
async fn a_volume_attributes_class_of_another_driver_is_refused() {
    let mut fake = Fake::new();
    fake.extra_caps = vec![controller_service_capability::rpc::Type::ModifyVolume];
    let (storage, p, rec, _d) = env(fake, storage_class(None)).await;
    put_vac(&storage, &vac("other.example.com", &[("iops", "3000")])).await;
    let mut c = claim(&[(ANN, DRIVER)]);
    c.spec.volume_attributes_class_name = Some("gold".into());
    let err = p.sync_claim(&c).await.unwrap_err();
    assert!(
        err.to_string().contains("does not match driver name"),
        "got: {err}"
    );
    assert!(rec.lock().unwrap().create_volume.is_empty());
}

// ---- topology (#3007): external-provisioner pkg/controller/topology.go -------

use rusternetes_common::resources::csi::{CSINode, CSINodeDriver, CSINodeSpec};
use rusternetes_common::resources::Node;

const SELECTED_NODE: &str = "volume.kubernetes.io/selected-node";

fn zone(v: &str) -> Topology {
    Topology {
        segments: [("topology.example.com/zone".to_string(), v.to_string())].into(),
    }
}

async fn put_node(storage: &MemoryStorage, name: &str, zone: Option<&str>, with_csi_node: bool) {
    let mut n = Node::new(name);
    if let Some(z) = zone {
        n.metadata.labels = Some([("topology.example.com/zone".to_string(), z.to_string())].into());
    }
    storage
        .create(&build_key("nodes", None, name), &n)
        .await
        .unwrap();
    if with_csi_node {
        let cn = CSINode {
            type_meta: TypeMeta {
                kind: "CSINode".into(),
                api_version: "storage.k8s.io/v1".into(),
            },
            metadata: ObjectMeta::new(name),
            spec: CSINodeSpec {
                drivers: vec![CSINodeDriver {
                    name: DRIVER.into(),
                    node_id: name.into(),
                    topology_keys: Some(vec!["topology.example.com/zone".into()]),
                    allocatable: None,
                }],
            },
        };
        storage
            .create(&build_key("csinodes", None, name), &cn)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn immediate_binding_passes_the_aggregated_cluster_topology() {
    let (storage, p, rec, _d) = env(Fake::with_topology(), storage_class(None)).await;
    put_node(&storage, "n1", Some("a"), true).await;
    put_node(&storage, "n2", Some("b"), true).await;
    // A node without the driver registered must not be reported.
    put_node(&storage, "n3", Some("c"), false).await;
    p.sync_claim(&claim(&[(ANN, DRIVER)])).await.unwrap();

    let reqs = rec.lock().unwrap().create_volume.clone();
    assert_eq!(reqs.len(), 1);
    let ar = reqs[0]
        .accessibility_requirements
        .as_ref()
        .expect("topology");
    assert_eq!(ar.requisite, vec![zone("a"), zone("b")]);
    // Immediate binding: statefulset-spreading rotation of the sorted terms.
    assert_eq!(ar.preferred.len(), 2);
}

#[tokio::test]
async fn a_driver_without_the_topology_capability_gets_no_requirements() {
    let (storage, p, rec, _d) = env(Fake::new(), storage_class(None)).await;
    put_node(&storage, "n1", Some("a"), true).await;
    p.sync_claim(&claim(&[(ANN, DRIVER)])).await.unwrap();
    let reqs = rec.lock().unwrap().create_volume.clone();
    assert!(reqs[0].accessibility_requirements.is_none());
    assert!(pv(&storage).await.unwrap().spec.node_affinity.is_none());
}

#[tokio::test]
async fn delayed_binding_prefers_the_selected_nodes_topology() {
    let (storage, p, rec, _d) = env(
        Fake::with_topology(),
        storage_class(Some(VolumeBindingMode::WaitForFirstConsumer)),
    )
    .await;
    put_node(&storage, "n1", Some("a"), true).await;
    put_node(&storage, "n2", Some("b"), true).await;
    p.sync_claim(&claim(&[(ANN, DRIVER), (SELECTED_NODE, "n2")]))
        .await
        .unwrap();
    let reqs = rec.lock().unwrap().create_volume.clone();
    let ar = reqs[0]
        .accessibility_requirements
        .as_ref()
        .expect("topology");
    assert_eq!(ar.requisite, vec![zone("a"), zone("b")]);
    assert_eq!(ar.preferred, vec![zone("b"), zone("a")]);
}

#[tokio::test]
async fn the_pv_gets_node_affinity_from_the_accessible_topology() {
    let mut fake = Fake::with_topology();
    fake.accessible_topology = vec![zone("a"), zone("b")];
    let (storage, p, _rec, _d) = env(fake, storage_class(None)).await;
    put_node(&storage, "n1", Some("a"), true).await;
    p.sync_claim(&claim(&[(ANN, DRIVER)])).await.unwrap();
    let terms = pv(&storage)
        .await
        .unwrap()
        .spec
        .node_affinity
        .expect("nodeAffinity")
        .required
        .unwrap()
        .node_selector_terms;
    assert_eq!(terms.len(), 2);
    let e = &terms[1].match_expressions.as_ref().unwrap()[0];
    assert_eq!(e.key, "topology.example.com/zone");
    assert_eq!(e.operator, "In");
    assert_eq!(e.values.as_deref(), Some(&["b".to_string()][..]));
}

#[tokio::test]
async fn a_selected_node_without_a_csinode_reschedules() {
    let (storage, p, rec, _d) = env(
        Fake::with_topology(),
        storage_class(Some(VolumeBindingMode::WaitForFirstConsumer)),
    )
    .await;
    let c = claim(&[(ANN, DRIVER), (SELECTED_NODE, "gone")]);
    storage
        .create(
            &build_key("persistentvolumeclaims", Some("ns1"), "data"),
            &c,
        )
        .await
        .unwrap();
    let _ = p.sync_claim(&c).await;
    // `ProvisioningReschedule`: no RPC, selectedNode removed, no PV.
    assert!(rec.lock().unwrap().create_volume.is_empty());
    let after: PersistentVolumeClaim = storage
        .get(&build_key("persistentvolumeclaims", Some("ns1"), "data"))
        .await
        .unwrap();
    assert!(!after
        .metadata
        .annotations
        .unwrap_or_default()
        .contains_key(SELECTED_NODE));
    assert!(pv(&storage).await.is_none());
}

#[tokio::test]
async fn resource_exhausted_with_a_selected_node_reschedules() {
    let mut fake = Fake::with_topology();
    fake.create_code = Some(tonic::Code::ResourceExhausted);
    let (storage, p, _rec, _d) = env(
        fake,
        storage_class(Some(VolumeBindingMode::WaitForFirstConsumer)),
    )
    .await;
    put_node(&storage, "n1", Some("a"), true).await;
    let c = claim(&[(ANN, DRIVER), (SELECTED_NODE, "n1")]);
    storage
        .create(
            &build_key("persistentvolumeclaims", Some("ns1"), "data"),
            &c,
        )
        .await
        .unwrap();
    let _ = p.sync_claim(&c).await;
    let after: PersistentVolumeClaim = storage
        .get(&build_key("persistentvolumeclaims", Some("ns1"), "data"))
        .await
        .unwrap();
    assert!(!after
        .metadata
        .annotations
        .unwrap_or_default()
        .contains_key(SELECTED_NODE));
}
