//! The CSI Controller-service client, in the role of the gRPC half of
//! kubernetes-csi/external-provisioner (`pkg/controller/controller.go`) and
//! external-snapshotter: `CreateVolume` (with `VolumeContentSource`),
//! `DeleteVolume`, `CreateSnapshot`, `DeleteSnapshot`, preceded by the
//! driver-capability gates upstream applies.
//!
//! Upstream citations (external-provisioner `master`, read verbatim):
//! - `checkDriverCapabilities` (`controller.go:449-483`): the capability gates.
//! - `checkError` (`controller.go:2036-2073`): gRPC code -> provisioning state.
//! - `GetDriverCapabilities` (`controller.go:331-348`): `GetPluginCapabilities`
//!   + `ControllerGetCapabilities`, each under its own timeout.
//! - `--timeout` default `10s` (`cmd/csi-provisioner/csi-provisioner.go:89`),
//!   applied per operation with `context.WithTimeout` (`controller.go:878`,
//!   `:1445`).
//!
//! Scope: the wire client only. The provisioning loop (PVC -> request,
//! capacity re-check, orphan cleanup) is separate work.

use super::csi_client::{normalize_endpoint, proto};
use hyper_util::rt::TokioIo;
use proto::controller_client::ControllerClient;
use proto::controller_service_capability::rpc::Type as ControllerRpc;
use proto::identity_client::IdentityClient;
use proto::plugin_capability::service::Type as PluginService;
use proto::{
    ControllerGetCapabilitiesRequest, CreateSnapshotRequest, CreateVolumeRequest,
    DeleteSnapshotRequest, DeleteVolumeRequest, GetPluginCapabilitiesRequest, Snapshot, Volume,
};
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

/// `--timeout` default of external-provisioner.
pub const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);

/// `controller.ProvisioningState` (sig-storage-lib-external-provisioner).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProvisioningState {
    /// The operation may still be running or partly done: retry, never clean up.
    InBackground,
    /// Give up on this node; the scheduler may pick another.
    Reschedule,
    /// The operation is known not to be in progress.
    Finished,
    /// Nothing was attempted (a local failure before the RPC).
    NoChange,
}

/// Port of `checkError` (`controller.go:2036-2073`).
pub fn provisioning_state(status: &tonic::Status, may_reschedule: bool) -> ProvisioningState {
    let _ = (status, may_reschedule);
    todo!()
}

#[derive(Debug, thiserror::Error)]
pub enum ControllerError {
    /// A `checkDriverCapabilities` failure; no RPC was sent.
    #[error("{0}")]
    UnsupportedCapability(String),
    /// A request rejected locally; no RPC was sent.
    #[error("{0}")]
    InvalidArgument(String),
    /// The driver answered with something the spec forbids.
    #[error("{0}")]
    InvalidResponse(String),
    #[error("{}", .0.message())]
    Grpc(Box<tonic::Status>),
}

impl ControllerError {
    pub fn status(&self) -> Option<&tonic::Status> {
        match self {
            ControllerError::Grpc(s) => Some(s),
            _ => None,
        }
    }

    pub fn provisioning_state(&self, may_reschedule: bool) -> ProvisioningState {
        let _ = may_reschedule;
        todo!()
    }
}

/// What the driver reports (`rpc.PluginCapabilitySet` / `ControllerCapabilitySet`).
#[derive(Clone, Debug, Default)]
pub struct DriverCapabilities {
    pub plugin: HashSet<PluginService>,
    pub controller: HashSet<ControllerRpc>,
}

impl DriverCapabilities {
    /// `SupportsTopology`: `VOLUME_ACCESSIBILITY_CONSTRAINTS`.
    pub fn supports_topology(&self) -> bool {
        todo!()
    }
}

/// `requiredCapabilities` (`controller.go`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RequiredCapabilities {
    pub snapshot: bool,
    pub clone: bool,
    pub modify_volume: bool,
}

impl RequiredCapabilities {
    pub fn for_request(req: &CreateVolumeRequest) -> Self {
        let _ = req;
        todo!()
    }
}

/// Port of `checkDriverCapabilities` (`controller.go:449-483`).
pub fn check_driver_capabilities(
    caps: &DriverCapabilities,
    rc: &RequiredCapabilities,
) -> Result<(), ControllerError> {
    let _ = (caps, rc);
    todo!()
}

pub struct CsiControllerClient {
    endpoint: PathBuf,
    timeout: Duration,
}

impl CsiControllerClient {
    pub fn with_endpoint(endpoint: impl Into<PathBuf>) -> Self {
        Self {
            endpoint: endpoint.into(),
            timeout: DEFAULT_OPERATION_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[allow(dead_code)]
    fn channel(&self) -> Result<Channel, ControllerError> {
        let path = normalize_endpoint(&self.endpoint);
        let endpoint = Endpoint::try_from("http://[::]:50051")
            .map_err(|e| ControllerError::InvalidArgument(format!("invalid CSI endpoint: {e}")))?;
        Ok(
            endpoint.connect_with_connector_lazy(service_fn(move |_: Uri| {
                let path = path.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(path).await?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            })),
        )
    }

    pub async fn get_driver_capabilities(&self) -> Result<DriverCapabilities, ControllerError> {
        let _: Option<(IdentityClient<Channel>, ControllerClient<Channel>)> = None;
        let _ = (
            GetPluginCapabilitiesRequest {},
            ControllerGetCapabilitiesRequest {},
        );
        todo!()
    }

    pub async fn create_volume(&self, req: CreateVolumeRequest) -> Result<Volume, ControllerError> {
        let _ = req;
        todo!()
    }

    pub async fn delete_volume(&self, req: DeleteVolumeRequest) -> Result<(), ControllerError> {
        let _ = req;
        todo!()
    }

    pub async fn create_snapshot(
        &self,
        req: CreateSnapshotRequest,
    ) -> Result<Snapshot, ControllerError> {
        let _ = req;
        todo!()
    }

    pub async fn delete_snapshot(&self, req: DeleteSnapshotRequest) -> Result<(), ControllerError> {
        let _ = req;
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::proto::controller_server::{Controller, ControllerServer};
    use super::proto::identity_server::{Identity, IdentityServer};
    use super::proto::volume_capability::access_mode::Mode;
    use super::proto::volume_capability::{AccessMode, AccessType, MountVolume};
    use super::proto::volume_content_source::{SnapshotSource, Type as SourceType, VolumeSource};
    use super::proto::*;
    use super::*;
    use std::sync::{Arc, Mutex};
    use tonic::{Request, Response, Status};

    #[derive(Default)]
    struct Recorded {
        create_volume: Vec<CreateVolumeRequest>,
        delete_volume: Vec<DeleteVolumeRequest>,
        create_snapshot: Vec<CreateSnapshotRequest>,
        delete_snapshot: Vec<DeleteSnapshotRequest>,
    }

    #[derive(Clone)]
    struct Fake {
        rec: Arc<Mutex<Recorded>>,
        plugin_caps: Vec<PluginService>,
        ctrl_caps: Vec<ControllerRpc>,
        create_code: Option<tonic::Code>,
        delay: Duration,
    }

    impl Fake {
        fn full() -> Self {
            Self {
                rec: Default::default(),
                plugin_caps: vec![PluginService::ControllerService],
                ctrl_caps: vec![
                    ControllerRpc::CreateDeleteVolume,
                    ControllerRpc::CreateDeleteSnapshot,
                    ControllerRpc::CloneVolume,
                    ControllerRpc::ModifyVolume,
                ],
                create_code: None,
                delay: Duration::ZERO,
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
                capabilities: self
                    .plugin_caps
                    .iter()
                    .map(|t| PluginCapability {
                        r#type: Some(plugin_capability::Type::Service(
                            plugin_capability::Service { r#type: *t as i32 },
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
            Ok(Response::new(ControllerGetCapabilitiesResponse {
                capabilities: self
                    .ctrl_caps
                    .iter()
                    .map(|t| ControllerServiceCapability {
                        r#type: Some(controller_service_capability::Type::Rpc(
                            controller_service_capability::Rpc { r#type: *t as i32 },
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
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            if let Some(c) = self.create_code {
                return Err(Status::new(c, "fake create failure"));
            }
            Ok(Response::new(CreateVolumeResponse {
                volume: Some(Volume {
                    capacity_bytes: req.capacity_range.map_or(0, |c| c.required_bytes),
                    volume_id: "vol-1".into(),
                    content_source: req.volume_content_source,
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
            r: Request<CreateSnapshotRequest>,
        ) -> Result<Response<CreateSnapshotResponse>, Status> {
            let req = r.into_inner();
            self.rec.lock().unwrap().create_snapshot.push(req.clone());
            Ok(Response::new(CreateSnapshotResponse {
                snapshot: Some(Snapshot {
                    snapshot_id: "snap-1".into(),
                    source_volume_id: req.source_volume_id,
                    ready_to_use: true,
                    ..Default::default()
                }),
            }))
        }
        async fn delete_snapshot(
            &self,
            r: Request<DeleteSnapshotRequest>,
        ) -> Result<Response<DeleteSnapshotResponse>, Status> {
            self.rec
                .lock()
                .unwrap()
                .delete_snapshot
                .push(r.into_inner());
            Ok(Response::new(DeleteSnapshotResponse {}))
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

    fn cap() -> VolumeCapability {
        VolumeCapability {
            access_type: Some(AccessType::Mount(MountVolume {
                fs_type: "ext4".into(),
                mount_flags: vec!["noatime".into()],
                volume_mount_group: String::new(),
            })),
            access_mode: Some(AccessMode {
                mode: Mode::SingleNodeWriter as i32,
            }),
        }
    }

    fn create_req() -> CreateVolumeRequest {
        CreateVolumeRequest {
            name: "pvc-1234".into(),
            capacity_range: Some(CapacityRange {
                required_bytes: 1 << 30,
                limit_bytes: 0,
            }),
            volume_capabilities: vec![cap()],
            parameters: [("type".to_string(), "fast".to_string())].into(),
            secrets: [("k".to_string(), "v".to_string())].into(),
            ..Default::default()
        }
    }

    fn snap_source() -> VolumeContentSource {
        VolumeContentSource {
            r#type: Some(SourceType::Snapshot(SnapshotSource {
                snapshot_id: "snap-9".into(),
            })),
        }
    }

    #[tokio::test]
    async fn create_volume_sends_the_whole_request_and_returns_the_volume() {
        let (_d, client, rec) = start(Fake::full());
        let mut req = create_req();
        req.volume_content_source = Some(snap_source());
        req.accessibility_requirements = Some(TopologyRequirement {
            requisite: vec![Topology {
                segments: [("zone".to_string(), "a".to_string())].into(),
            }],
            preferred: vec![],
        });
        let vol = client.create_volume(req.clone()).await.unwrap();
        assert_eq!(vol.volume_id, "vol-1");
        assert_eq!(vol.capacity_bytes, 1 << 30);
        assert_eq!(vol.content_source, req.volume_content_source);
        assert_eq!(rec.lock().unwrap().create_volume, vec![req]);
    }

    #[tokio::test]
    async fn snapshot_source_needs_create_delete_snapshot_and_sends_no_rpc() {
        let mut f = Fake::full();
        f.ctrl_caps = vec![ControllerRpc::CreateDeleteVolume];
        let (_d, client, rec) = start(f);
        let mut req = create_req();
        req.volume_content_source = Some(snap_source());
        let err = client.create_volume(req).await.unwrap_err();
        assert!(matches!(err, ControllerError::UnsupportedCapability(_)));
        assert_eq!(
            err.to_string(),
            "CSI driver does not support snapshot restore: controller CREATE_DELETE_SNAPSHOT capability is not reported"
        );
        assert!(rec.lock().unwrap().create_volume.is_empty());
    }

    #[tokio::test]
    async fn volume_source_needs_clone_volume() {
        let mut f = Fake::full();
        f.ctrl_caps = vec![ControllerRpc::CreateDeleteVolume];
        let (_d, client, rec) = start(f);
        let mut req = create_req();
        req.volume_content_source = Some(VolumeContentSource {
            r#type: Some(SourceType::Volume(VolumeSource {
                volume_id: "v0".into(),
            })),
        });
        let err = client.create_volume(req).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "CSI driver does not support clone operations: controller CLONE_VOLUME capability is not reported"
        );
        assert!(rec.lock().unwrap().create_volume.is_empty());
    }

    #[tokio::test]
    async fn mutable_parameters_need_modify_volume() {
        let mut f = Fake::full();
        f.ctrl_caps = vec![ControllerRpc::CreateDeleteVolume];
        let (_d, client, _rec) = start(f);
        let mut req = create_req();
        req.mutable_parameters = [("iops".to_string(), "3000".to_string())].into();
        let err = client.create_volume(req).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "CSI driver does not support VolumeAttributesClass: controller MODIFY_VOLUME capability is not reported"
        );
    }

    #[tokio::test]
    async fn driver_without_controller_service_or_create_delete_is_rejected() {
        let mut f = Fake::full();
        f.plugin_caps = vec![];
        let (_d, client, rec) = start(f);
        let err = client.create_volume(create_req()).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "CSI driver does not support dynamic provisioning: plugin CONTROLLER_SERVICE capability is not reported"
        );
        let mut f = Fake::full();
        f.ctrl_caps = vec![];
        let (_d2, client2, _) = start(f);
        let err = client2.create_volume(create_req()).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "CSI driver does not support dynamic provisioning: controller CREATE_DELETE_VOLUME capability is not reported"
        );
        assert!(rec.lock().unwrap().create_volume.is_empty());
    }

    #[tokio::test]
    async fn create_volume_rejects_empty_name_and_capabilities_locally() {
        let (_d, client, rec) = start(Fake::full());
        let mut req = create_req();
        req.name.clear();
        assert!(matches!(
            client.create_volume(req).await.unwrap_err(),
            ControllerError::InvalidArgument(_)
        ));
        let mut req = create_req();
        req.volume_capabilities.clear();
        assert!(matches!(
            client.create_volume(req).await.unwrap_err(),
            ControllerError::InvalidArgument(_)
        ));
        assert!(rec.lock().unwrap().create_volume.is_empty());
    }

    #[test]
    fn provisioning_state_matches_check_error() {
        use tonic::Code::*;
        let st = |c| tonic::Status::new(c, "x");
        assert_eq!(
            provisioning_state(&st(ResourceExhausted), true),
            ProvisioningState::Reschedule
        );
        assert_eq!(
            provisioning_state(&st(ResourceExhausted), false),
            ProvisioningState::Finished
        );
        for c in [Cancelled, DeadlineExceeded, Unavailable, Aborted] {
            assert_eq!(
                provisioning_state(&st(c), true),
                ProvisioningState::InBackground
            );
        }
        for c in [InvalidArgument, NotFound, AlreadyExists, Internal, Unknown] {
            assert_eq!(
                provisioning_state(&st(c), true),
                ProvisioningState::Finished
            );
        }
        let local = ControllerError::UnsupportedCapability("x".into());
        assert_eq!(local.provisioning_state(true), ProvisioningState::NoChange);
    }

    #[tokio::test]
    async fn grpc_failure_keeps_the_status_and_maps_to_a_state() {
        let mut f = Fake::full();
        f.create_code = Some(tonic::Code::ResourceExhausted);
        let (_d, client, _rec) = start(f);
        let err = client.create_volume(create_req()).await.unwrap_err();
        assert_eq!(err.status().unwrap().code(), tonic::Code::ResourceExhausted);
        assert_eq!(err.provisioning_state(true), ProvisioningState::Reschedule);
    }

    #[tokio::test]
    async fn operation_timeout_is_deadline_exceeded_and_in_background() {
        let mut f = Fake::full();
        f.delay = Duration::from_secs(5);
        let (_d, client, _rec) = start(f);
        let client = client.with_timeout(Duration::from_millis(200));
        let err = client.create_volume(create_req()).await.unwrap_err();
        assert_eq!(err.status().unwrap().code(), tonic::Code::DeadlineExceeded);
        assert_eq!(
            err.provisioning_state(false),
            ProvisioningState::InBackground
        );
    }

    #[tokio::test]
    async fn delete_volume_sends_id_and_secrets() {
        let (_d, client, rec) = start(Fake::full());
        let req = DeleteVolumeRequest {
            volume_id: "vol-1".into(),
            secrets: [("k".to_string(), "v".to_string())].into(),
        };
        client.delete_volume(req.clone()).await.unwrap();
        assert_eq!(rec.lock().unwrap().delete_volume, vec![req]);
        assert!(matches!(
            client
                .delete_volume(DeleteVolumeRequest::default())
                .await
                .unwrap_err(),
            ControllerError::InvalidArgument(_)
        ));
    }

    #[tokio::test]
    async fn snapshots_round_trip_and_are_capability_gated() {
        let (_d, client, rec) = start(Fake::full());
        let req = CreateSnapshotRequest {
            source_volume_id: "vol-1".into(),
            name: "snapshot-abc".into(),
            parameters: [("p".to_string(), "q".to_string())].into(),
            ..Default::default()
        };
        let snap = client.create_snapshot(req.clone()).await.unwrap();
        assert_eq!(snap.snapshot_id, "snap-1");
        assert!(snap.ready_to_use);
        client
            .delete_snapshot(DeleteSnapshotRequest {
                snapshot_id: "snap-1".into(),
                secrets: Default::default(),
            })
            .await
            .unwrap();
        assert_eq!(rec.lock().unwrap().create_snapshot, vec![req.clone()]);
        assert_eq!(rec.lock().unwrap().delete_snapshot.len(), 1);

        let mut f = Fake::full();
        f.ctrl_caps = vec![ControllerRpc::CreateDeleteVolume];
        let (_d2, client2, rec2) = start(f);
        let err = client2.create_snapshot(req).await.unwrap_err();
        assert!(matches!(err, ControllerError::UnsupportedCapability(_)));
        assert!(rec2.lock().unwrap().create_snapshot.is_empty());
    }

    #[tokio::test]
    async fn get_driver_capabilities_reports_both_sets() {
        let mut f = Fake::full();
        f.plugin_caps = vec![
            PluginService::ControllerService,
            PluginService::VolumeAccessibilityConstraints,
        ];
        let (_d, client, _rec) = start(f);
        let caps = client.get_driver_capabilities().await.unwrap();
        assert!(caps.supports_topology());
        assert!(caps.controller.contains(&ControllerRpc::CloneVolume));
        assert!(caps.plugin.contains(&PluginService::ControllerService));
    }
}
