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
///
/// Only a gRPC status reaches here; a non-gRPC error is handled by
/// [`ControllerError::provisioning_state`].
pub fn provisioning_state(status: &tonic::Status, may_reschedule: bool) -> ProvisioningState {
    use tonic::Code::*;
    match status.code() {
        // CSI: operation not pending, "Unable to provision in
        // `accessible_topology`". May succeed on another node.
        ResourceExhausted => {
            if may_reschedule {
                ProvisioningState::Reschedule
            } else {
                ProvisioningState::Finished
            }
        }
        // The previous call may still be in progress.
        Cancelled | DeadlineExceeded | Unavailable | Aborted => ProvisioningState::InBackground,
        // Provisioning either did not start or failed: not in progress.
        _ => ProvisioningState::Finished,
    }
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

    /// The provisioning state this error implies. Failures before any RPC
    /// leave nothing to clean up (`ProvisioningNoChange`); a malformed driver
    /// answer is treated like `checkError`'s non-gRPC branch
    /// (`controller.go:2041-2046`): be on the safe side, assume in progress.
    pub fn provisioning_state(&self, may_reschedule: bool) -> ProvisioningState {
        match self {
            ControllerError::Grpc(s) => provisioning_state(s, may_reschedule),
            ControllerError::InvalidResponse(_) => ProvisioningState::InBackground,
            ControllerError::UnsupportedCapability(_) | ControllerError::InvalidArgument(_) => {
                ProvisioningState::NoChange
            }
        }
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
        self.plugin
            .contains(&PluginService::VolumeAccessibilityConstraints)
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
    /// What a `CreateVolumeRequest` needs beyond plain create/delete: a
    /// snapshot source needs `CREATE_DELETE_SNAPSHOT`, a volume source needs
    /// `CLONE_VOLUME` (`getSnapshotSource` / `getPVCSource`), and mutable
    /// parameters (a VolumeAttributesClass) need `MODIFY_VOLUME`.
    pub fn for_request(req: &CreateVolumeRequest) -> Self {
        let mut rc = Self::default();
        match req
            .volume_content_source
            .as_ref()
            .and_then(|s| s.r#type.as_ref())
        {
            Some(proto::volume_content_source::Type::Snapshot(_)) => rc.snapshot = true,
            Some(proto::volume_content_source::Type::Volume(_)) => rc.clone = true,
            None => {}
        }
        rc.modify_volume = !req.mutable_parameters.is_empty();
        rc
    }
}

/// Port of `checkDriverCapabilities` (`controller.go:449-483`), messages verbatim.
pub fn check_driver_capabilities(
    caps: &DriverCapabilities,
    rc: &RequiredCapabilities,
) -> Result<(), ControllerError> {
    let unsupported = |m: &str| Err(ControllerError::UnsupportedCapability(m.to_string()));
    if !caps.plugin.contains(&PluginService::ControllerService) {
        return unsupported(
            "CSI driver does not support dynamic provisioning: plugin CONTROLLER_SERVICE capability is not reported",
        );
    }
    if !caps.controller.contains(&ControllerRpc::CreateDeleteVolume) {
        return unsupported(
            "CSI driver does not support dynamic provisioning: controller CREATE_DELETE_VOLUME capability is not reported",
        );
    }
    if rc.snapshot
        && !caps
            .controller
            .contains(&ControllerRpc::CreateDeleteSnapshot)
    {
        return unsupported(
            "CSI driver does not support snapshot restore: controller CREATE_DELETE_SNAPSHOT capability is not reported",
        );
    }
    if rc.clone && !caps.controller.contains(&ControllerRpc::CloneVolume) {
        return unsupported(
            "CSI driver does not support clone operations: controller CLONE_VOLUME capability is not reported",
        );
    }
    if rc.modify_volume && !caps.controller.contains(&ControllerRpc::ModifyVolume) {
        return unsupported(
            "CSI driver does not support VolumeAttributesClass: controller MODIFY_VOLUME capability is not reported",
        );
    }
    Ok(())
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

    /// Per-operation timeout (`--timeout`, default [`DEFAULT_OPERATION_TIMEOUT`]).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Lazily dial the driver's unix socket (same connector as the Node client).
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

    /// One operation under its own timeout (`context.WithTimeout(.., p.timeout)`),
    /// a timeout surfacing as the `DeadlineExceeded` status Go's context yields.
    async fn call<T>(
        &self,
        fut: impl std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    ) -> Result<T, ControllerError> {
        match tokio::time::timeout(self.timeout, fut).await {
            Ok(r) => r
                .map(|r| r.into_inner())
                .map_err(|s| ControllerError::Grpc(Box::new(s))),
            Err(_) => Err(ControllerError::Grpc(Box::new(
                tonic::Status::deadline_exceeded("context deadline exceeded"),
            ))),
        }
    }

    /// Port of `GetDriverCapabilities` (`controller.go:331-348`) with
    /// `rpc.GetPluginCapabilities` / `rpc.GetControllerCapabilities`
    /// (csi-lib-utils `rpc/common.go`): only supported capabilities are kept.
    pub async fn get_driver_capabilities(&self) -> Result<DriverCapabilities, ControllerError> {
        let ch = self.channel()?;
        let plugin = self
            .call(
                IdentityClient::new(ch.clone())
                    .get_plugin_capabilities(GetPluginCapabilitiesRequest {}),
            )
            .await?;
        let controller = self
            .call(
                ControllerClient::new(ch)
                    .controller_get_capabilities(ControllerGetCapabilitiesRequest {}),
            )
            .await?;
        let mut caps = DriverCapabilities::default();
        for c in plugin.capabilities {
            if let Some(proto::plugin_capability::Type::Service(s)) = c.r#type {
                if let Ok(t) = PluginService::try_from(s.r#type) {
                    caps.plugin.insert(t);
                }
            }
        }
        for c in controller.capabilities {
            if let Some(proto::controller_service_capability::Type::Rpc(r)) = c.r#type {
                if let Ok(t) = ControllerRpc::try_from(r.r#type) {
                    caps.controller.insert(t);
                }
            }
        }
        Ok(caps)
    }

    /// `CreateVolume` (`controller.go:878-880`), after `checkDriverCapabilities`.
    /// `VolumeContentSource` selects snapshot restore / clone.
    pub async fn create_volume(&self, req: CreateVolumeRequest) -> Result<Volume, ControllerError> {
        // CSI spec: `name` and `volume_capabilities` are REQUIRED.
        if req.name.is_empty() {
            return Err(ControllerError::InvalidArgument(
                "CreateVolume: name must be set".into(),
            ));
        }
        if req.volume_capabilities.is_empty() {
            return Err(ControllerError::InvalidArgument(
                "CreateVolume: volume_capabilities must be set".into(),
            ));
        }
        let caps = self.get_driver_capabilities().await?;
        check_driver_capabilities(&caps, &RequiredCapabilities::for_request(&req))?;
        let resp = self
            .call(ControllerClient::new(self.channel()?).create_volume(req))
            .await?;
        match resp.volume {
            Some(v) if !v.volume_id.is_empty() => Ok(v),
            _ => Err(ControllerError::InvalidResponse(
                "CreateVolume returned no volume or an empty volume_id".into(),
            )),
        }
    }

    /// `DeleteVolume` (`controller.go:1452`). Idempotent per the CSI spec.
    pub async fn delete_volume(&self, req: DeleteVolumeRequest) -> Result<(), ControllerError> {
        if req.volume_id.is_empty() {
            return Err(ControllerError::InvalidArgument(
                "DeleteVolume: volume_id must be set".into(),
            ));
        }
        self.call(ControllerClient::new(self.channel()?).delete_volume(req))
            .await
            .map(|_| ())
    }

    /// `CreateSnapshot`, gated on `CREATE_DELETE_SNAPSHOT` (external-snapshotter).
    pub async fn create_snapshot(
        &self,
        req: CreateSnapshotRequest,
    ) -> Result<Snapshot, ControllerError> {
        if req.source_volume_id.is_empty() || req.name.is_empty() {
            return Err(ControllerError::InvalidArgument(
                "CreateSnapshot: source_volume_id and name must be set".into(),
            ));
        }
        self.require_snapshot().await?;
        let resp = self
            .call(ControllerClient::new(self.channel()?).create_snapshot(req))
            .await?;
        match resp.snapshot {
            Some(s) if !s.snapshot_id.is_empty() => Ok(s),
            _ => Err(ControllerError::InvalidResponse(
                "CreateSnapshot returned no snapshot or an empty snapshot_id".into(),
            )),
        }
    }

    /// `DeleteSnapshot`, gated on `CREATE_DELETE_SNAPSHOT`. Idempotent.
    pub async fn delete_snapshot(&self, req: DeleteSnapshotRequest) -> Result<(), ControllerError> {
        if req.snapshot_id.is_empty() {
            return Err(ControllerError::InvalidArgument(
                "DeleteSnapshot: snapshot_id must be set".into(),
            ));
        }
        self.require_snapshot().await?;
        self.call(ControllerClient::new(self.channel()?).delete_snapshot(req))
            .await
            .map(|_| ())
    }

    async fn require_snapshot(&self) -> Result<(), ControllerError> {
        let caps = self.get_driver_capabilities().await?;
        if !caps.plugin.contains(&PluginService::ControllerService)
            || !caps
                .controller
                .contains(&ControllerRpc::CreateDeleteSnapshot)
        {
            return Err(ControllerError::UnsupportedCapability(
                "CSI driver does not support snapshots: controller CREATE_DELETE_SNAPSHOT capability is not reported".into(),
            ));
        }
        Ok(())
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
