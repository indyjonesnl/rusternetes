//! Port of `pkg/volume/csi/csi_client.go` — the kubelet's CSI Node-service
//! client.
//!
//! Every method opens a fresh connection to the driver's socket for the call
//! and drops it afterwards, exactly as upstream does (`nodeV1ClientCreator`
//! returns a new `grpc.ClientConn` and a `closer` per RPC,
//! `csi_client.go:142-151`). Our channel is lazy, so "open" is free and the
//! first RPC dials.

use super::csi_drivers_store::csi_drivers;
use hyper_util::rt::TokioIo;
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::PersistentVolumeAccessMode;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

/// Generated from `proto/csi/v1/csi.proto` (package `csi.v1`).
#[allow(clippy::result_large_err, clippy::large_enum_variant)]
pub mod proto {
    tonic::include_proto!("csi.v1");
}

use proto::node_client::NodeClient;
use proto::node_service_capability::rpc::Type as NodeRpcType;
use proto::volume_capability::access_mode::Mode as AccessModeKind;
use proto::volume_capability::{AccessMode, AccessType, BlockVolume, MountVolume};
use proto::{
    CapacityRange, NodeExpandVolumeRequest, NodeGetCapabilitiesRequest, NodeGetInfoRequest,
    NodePublishVolumeRequest,
    NodeStageVolumeRequest, NodeUnpublishVolumeRequest, NodeUnstageVolumeRequest, VolumeCapability,
};

/// `csiTimeout` (`pkg/volume/csi/csi_plugin.go:55`): the deadline every CSI
/// operation context carries (`createCSIOperationContext`, `csi_util.go:198`).
pub const CSI_TIMEOUT: Duration = Duration::from_secs(120);

/// `fsTypeBlockName` (`csi_client.go:53`): the `fsType` that selects the block
/// access type instead of the mount one.
pub const FS_TYPE_BLOCK_NAME: &str = "block";

/// The three outcomes `pkg/volume/util/types/types.go` distinguishes for a
/// volume operation, plus the plain error.
///
/// - `UncertainProgress` (`NewUncertainProgressError`): the RPC may still be
///   running or may have partly succeeded, so cleanup is UNSAFE.
/// - `Transient` (`NewTransientOperationFailure`): the operation did not start;
///   retry, but there is nothing to clean up.
/// - `Failed`: a final error — the operation is known not to be in progress.
#[derive(Debug, thiserror::Error)]
pub enum CsiError {
    #[error("{0}")]
    UncertainProgress(String),
    #[error("{0}")]
    Transient(String),
    #[error("{0}")]
    Failed(String),
}

impl CsiError {
    /// `IsOperationFinishedError` (`pkg/volume/util/types/types.go:171`): true
    /// unless the error is uncertain-progress or transient. Only a finished
    /// operation may have its mount directory removed (`csi_mounter.go:301`).
    pub fn is_operation_finished(&self) -> bool {
        matches!(self, CsiError::Failed(_))
    }
}

/// Port of `csiResizeOptions` (`csi_client.go:116-125`).
#[derive(Clone, Debug)]
pub struct CsiResizeOptions {
    pub volume_id: String,
    pub volume_path: String,
    pub staging_target_path: String,
    pub fs_type: String,
    pub access_mode: PersistentVolumeAccessMode,
    pub new_size: Quantity,
    pub mount_options: Vec<String>,
    pub secrets: HashMap<String, String>,
}

/// What `NodeExpandVolume` (`csi_client.go:289-358`) can return.
///
/// Unlike [`CsiError`] this keeps the gRPC status of a final failure: the
/// caller (`csiPlugin.nodeExpandWithClient`, `expander.go:116-128`) classifies
/// it with `inUseError` / `isInfeasibleError`, both of which read the code via
/// `status.FromError`.
#[derive(Debug, thiserror::Error)]
pub enum NodeExpandError {
    /// `volumetypes.NewUncertainProgressError` (`csi_client.go:349`): a
    /// non-final gRPC error. Upstream wraps only the message, so the code is
    /// not recoverable from it, and `status.FromError` on it is `ok == false`.
    #[error("{0}")]
    UncertainProgress(String),
    /// A final gRPC error, returned as-is (`csi_client.go:351`).
    #[error("{}", .0.message())]
    Grpc(Box<tonic::Status>),
    /// A non-gRPC error: argument validation, or the access-mode lookup.
    #[error("{0}")]
    Failed(String),
}

/// Port of `isFinalError` (`csi_client.go:711-733`).
///
/// A final error means the operation either never started or definitely
/// failed; anything else (cancelled, deadline, unavailable, exhausted,
/// aborted) may leave the driver mid-operation.
pub fn is_final_error(status: &tonic::Status) -> bool {
    use tonic::Code::*;
    !matches!(
        status.code(),
        Cancelled | DeadlineExceeded | Unavailable | ResourceExhausted | Aborted
    )
}

/// Port of `asCSIAccessModeV1` (`csi_client.go:501-516`).
pub fn as_csi_access_mode(am: &PersistentVolumeAccessMode) -> AccessModeKind {
    match am {
        PersistentVolumeAccessMode::ReadWriteOnce => AccessModeKind::SingleNodeWriter,
        PersistentVolumeAccessMode::ReadOnlyMany => AccessModeKind::MultiNodeReaderOnly,
        PersistentVolumeAccessMode::ReadWriteMany => AccessModeKind::MultiNodeMultiWriter,
        // Lets drivers that lack SINGLE_NODE_MULTI_WRITER serve ReadWriteOncePod.
        PersistentVolumeAccessMode::ReadWriteOncePod => AccessModeKind::SingleNodeWriter,
    }
}

/// Port of `asSingleNodeMultiWriterCapableCSIAccessModeV1`
/// (`csi_client.go:518-530`).
pub fn as_single_node_multi_writer_capable_csi_access_mode(
    am: &PersistentVolumeAccessMode,
) -> AccessModeKind {
    match am {
        PersistentVolumeAccessMode::ReadWriteOnce => AccessModeKind::SingleNodeMultiWriter,
        PersistentVolumeAccessMode::ReadOnlyMany => AccessModeKind::MultiNodeReaderOnly,
        PersistentVolumeAccessMode::ReadWriteMany => AccessModeKind::MultiNodeMultiWriter,
        PersistentVolumeAccessMode::ReadWriteOncePod => AccessModeKind::SingleNodeSingleWriter,
    }
}

/// The three values upstream's `NodeGetInfo` returns (`csi_client.go:43-47`:
/// `nodeID`, `maxVolumePerNode`, `accessibleTopology`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeInfo {
    pub node_id: String,
    pub max_volumes_per_node: i64,
    pub accessible_topology: HashMap<String, String>,
}

/// Port of `csiDriverClient` (`csi_client.go:106-112`).
pub struct CsiDriverClient {
    driver_name: String,
    endpoint: PathBuf,
}

impl CsiDriverClient {
    /// Port of `newCsiDriverClient` (`csi_client.go:153-170`): resolve the
    /// driver's endpoint from the registered-driver store.
    pub fn new(driver_name: &str) -> Result<Self, CsiError> {
        let driver = csi_drivers().get(driver_name).ok_or_else(|| {
            CsiError::Failed(format!(
                "driver name {driver_name} not found in the list of registered CSI drivers"
            ))
        })?;
        Ok(Self::with_endpoint(driver_name, driver.endpoint))
    }

    /// A client for an explicit endpoint, bypassing the store.
    pub fn with_endpoint(driver_name: &str, endpoint: impl Into<PathBuf>) -> Self {
        Self {
            driver_name: driver_name.to_string(),
            endpoint: endpoint.into(),
        }
    }

    pub fn driver_name(&self) -> &str {
        &self.driver_name
    }

    /// Port of `newV1NodeClient` + `newGrpcConn` (`csi_client.go:142-151`,
    /// `532-557`): dial the driver's unix socket. tonic 0.12 rides hyper 1.x, so
    /// the tokio `UnixStream` is wrapped in `TokioIo` (same as `rusternetes-cri`).
    fn node_client(&self) -> Result<NodeClient<Channel>, CsiError> {
        let path = normalize_endpoint(&self.endpoint);
        // The URI is a placeholder; the connector always dials `path`.
        let endpoint = Endpoint::try_from("http://[::]:50051")
            .map_err(|e| CsiError::Failed(format!("invalid CSI endpoint: {e}")))?;
        let channel = endpoint.connect_with_connector_lazy(service_fn(move |_: Uri| {
            let path = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }));
        Ok(NodeClient::new(channel))
    }

    /// Run `fut` under [`CSI_TIMEOUT`], mapping a timeout to the
    /// `DeadlineExceeded` status `context.WithTimeout` produces upstream.
    #[allow(clippy::result_large_err)] // tonic::Status is tonic's own error type
    async fn call<T>(
        &self,
        fut: impl std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    ) -> Result<T, tonic::Status> {
        match tokio::time::timeout(CSI_TIMEOUT, fut).await {
            Ok(r) => r.map(|r| r.into_inner()),
            Err(_) => Err(tonic::Status::deadline_exceeded(
                "context deadline exceeded",
            )),
        }
    }

    /// `nodeGetCapabilities` (`csi_client.go:692-709`).
    async fn node_get_capabilities(&self) -> Result<Vec<NodeRpcType>, CsiError> {
        let mut c = self.node_client()?;
        let resp = self
            .call(c.node_get_capabilities(NodeGetCapabilitiesRequest {}))
            .await
            .map_err(|s| CsiError::Failed(s.to_string()))?;
        Ok(resp
            .capabilities
            .into_iter()
            .filter_map(|cap| match cap.r#type {
                Some(proto::node_service_capability::Type::Rpc(rpc)) => {
                    NodeRpcType::try_from(rpc.r#type).ok()
                }
                _ => None,
            })
            .collect())
    }

    /// `nodeSupportsCapability` (`csi_client.go:674-690`). Like upstream, this
    /// calls `NodeGetCapabilities` every time — drivers may change.
    async fn node_supports_capability(&self, want: NodeRpcType) -> Result<bool, CsiError> {
        Ok(self.node_get_capabilities().await?.contains(&want))
    }

    /// `NodeGetInfo` / `nodeGetInfoV1` (`csi_client.go:172-209`): the driver's
    /// node id, its max volumes per node (0 = unlimited) and the topology
    /// segments (empty when the driver reports no `accessible_topology`).
    pub async fn node_get_info(&self) -> Result<NodeInfo, CsiError> {
        let mut c = self.node_client()?;
        let resp = self
            .call(c.node_get_info(NodeGetInfoRequest {}))
            .await
            .map_err(|s| CsiError::Failed(s.to_string()))?;
        Ok(NodeInfo {
            node_id: resp.node_id,
            max_volumes_per_node: resp.max_volumes_per_node,
            accessible_topology: resp
                .accessible_topology
                .map(|t| t.segments)
                .unwrap_or_default(),
        })
    }

    /// `NodeSupportsStageUnstage` (`csi_client.go:486`).
    pub async fn node_supports_stage_unstage(&self) -> Result<bool, CsiError> {
        self.node_supports_capability(NodeRpcType::StageUnstageVolume)
            .await
    }

    /// `NodeSupportsNodeExpand` (`csi_client.go:482-484`).
    pub async fn node_supports_node_expand(&self) -> Result<bool, CsiError> {
        self.node_supports_capability(NodeRpcType::ExpandVolume)
            .await
    }

    /// `NodeSupportsVolumeMountGroup` (`csi_client.go:670`).
    pub async fn node_supports_volume_mount_group(&self) -> Result<bool, CsiError> {
        self.node_supports_capability(NodeRpcType::VolumeMountGroup)
            .await
    }

    /// `NodeSupportsSingleNodeMultiWriterAccessMode` (`csi_client.go:584`).
    pub async fn node_supports_single_node_multi_writer_access_mode(
        &self,
    ) -> Result<bool, CsiError> {
        self.node_supports_capability(NodeRpcType::SingleNodeMultiWriter)
            .await
    }

    /// `getNodeV1AccessModeMapper` (`csi_client.go:490-499`).
    async fn access_mode_for(
        &self,
        am: &PersistentVolumeAccessMode,
    ) -> Result<AccessModeKind, CsiError> {
        Ok(
            if self
                .node_supports_single_node_multi_writer_access_mode()
                .await?
            {
                as_single_node_multi_writer_capable_csi_access_mode(am)
            } else {
                as_csi_access_mode(am)
            },
        )
    }

    /// The `VolumeCapability` both `NodePublishVolume` and `NodeStageVolume`
    /// build identically (`csi_client.go:242-267`, `408-432`): block access for
    /// `fsType == "block"`, otherwise a mount capability.
    fn volume_capability(
        mode: AccessModeKind,
        fs_type: &str,
        mount_options: &[String],
        fs_group: Option<i64>,
    ) -> VolumeCapability {
        let access_type = if fs_type == FS_TYPE_BLOCK_NAME {
            AccessType::Block(BlockVolume {})
        } else {
            AccessType::Mount(MountVolume {
                fs_type: fs_type.to_string(),
                mount_flags: mount_options.to_vec(),
                volume_mount_group: fs_group.map(|g| g.to_string()).unwrap_or_default(),
            })
        };
        VolumeCapability {
            access_mode: Some(AccessMode { mode: mode as i32 }),
            access_type: Some(access_type),
        }
    }

    /// Port of `NodePublishVolume` (`csi_client.go:211-287`).
    #[allow(clippy::too_many_arguments)]
    pub async fn node_publish_volume(
        &self,
        vol_id: &str,
        read_only: bool,
        staging_target_path: &str,
        target_path: &str,
        access_mode: &PersistentVolumeAccessMode,
        publish_context: HashMap<String, String>,
        volume_context: HashMap<String, String>,
        secrets: HashMap<String, String>,
        fs_type: &str,
        mount_options: &[String],
        fs_group: Option<i64>,
    ) -> Result<(), CsiError> {
        if vol_id.is_empty() {
            return Err(CsiError::Failed("missing volume id".into()));
        }
        if target_path.is_empty() {
            return Err(CsiError::Failed("missing target path".into()));
        }
        let mode = self.access_mode_for(access_mode).await?;
        let mut c = self.node_client()?;
        let req = NodePublishVolumeRequest {
            volume_id: vol_id.to_string(),
            target_path: target_path.to_string(),
            readonly: read_only,
            publish_context,
            volume_context,
            secrets,
            staging_target_path: staging_target_path.to_string(),
            volume_capability: Some(Self::volume_capability(
                mode,
                fs_type,
                mount_options,
                fs_group,
            )),
        };
        match self.call(c.node_publish_volume(req)).await {
            Ok(_) => Ok(()),
            // A non-final error means the publish may still be in flight.
            Err(s) if !is_final_error(&s) => Err(CsiError::UncertainProgress(s.to_string())),
            Err(s) => Err(CsiError::Failed(s.to_string())),
        }
    }

    /// Port of `NodeUnpublishVolume` (`csi_client.go:359-384`).
    pub async fn node_unpublish_volume(
        &self,
        vol_id: &str,
        target_path: &str,
    ) -> Result<(), CsiError> {
        if vol_id.is_empty() {
            return Err(CsiError::Failed("missing volume id".into()));
        }
        if target_path.is_empty() {
            return Err(CsiError::Failed("missing target path".into()));
        }
        let mut c = self.node_client()?;
        self.call(c.node_unpublish_volume(NodeUnpublishVolumeRequest {
            volume_id: vol_id.to_string(),
            target_path: target_path.to_string(),
        }))
        .await
        .map(|_| ())
        .map_err(|s| CsiError::Failed(s.to_string()))
    }

    /// Port of `NodeExpandVolume` (`csi_client.go:289-358`): returns the
    /// capacity the driver reports (`resp.CapacityBytes`).
    pub async fn node_expand_volume(
        &self,
        opts: &CsiResizeOptions,
    ) -> Result<Quantity, NodeExpandError> {
        if opts.volume_id.is_empty() {
            return Err(NodeExpandError::Failed("missing volume id".into()));
        }
        if opts.volume_path.is_empty() {
            return Err(NodeExpandError::Failed("missing volume path".into()));
        }
        // `opts.newSize.Value() < 0`
        if opts.new_size.is_negative() {
            return Err(NodeExpandError::Failed(
                "size can not be less than 0".into(),
            ));
        }
        let mode = self
            .access_mode_for(&opts.access_mode)
            .await
            .map_err(|e| NodeExpandError::Failed(e.to_string()))?;
        let mut c = self
            .node_client()
            .map_err(|e| NodeExpandError::Failed(e.to_string()))?;
        let req = NodeExpandVolumeRequest {
            volume_id: opts.volume_id.clone(),
            volume_path: opts.volume_path.clone(),
            capacity_range: Some(CapacityRange {
                // `opts.newSize.Value()`: int64, rounded up.
                required_bytes: i64::try_from(opts.new_size.value()).unwrap_or(i64::MAX),
                limit_bytes: 0,
            }),
            // Not all CSI drivers support NodeStageUnstage, so the
            // StagingTargetPath is only set when available (empty otherwise).
            staging_target_path: opts.staging_target_path.clone(),
            volume_capability: Some(Self::volume_capability(
                mode,
                &opts.fs_type,
                &opts.mount_options,
                None,
            )),
            secrets: opts.secrets.clone(),
        };
        match self.call(c.node_expand_volume(req)).await {
            Ok(resp) => Ok(Quantity::from_value(resp.capacity_bytes, Format::BinarySI)),
            Err(s) if !is_final_error(&s) => Err(NodeExpandError::UncertainProgress(s.to_string())),
            Err(s) => Err(NodeExpandError::Grpc(Box::new(s))),
        }
    }

    /// Port of `NodeStageVolume` (`csi_client.go:386-454`).
    #[allow(clippy::too_many_arguments)]
    pub async fn node_stage_volume(
        &self,
        vol_id: &str,
        publish_context: HashMap<String, String>,
        staging_target_path: &str,
        fs_type: &str,
        access_mode: &PersistentVolumeAccessMode,
        secrets: HashMap<String, String>,
        volume_context: HashMap<String, String>,
        mount_options: &[String],
        fs_group: Option<i64>,
    ) -> Result<(), CsiError> {
        if vol_id.is_empty() {
            return Err(CsiError::Failed("missing volume id".into()));
        }
        if staging_target_path.is_empty() {
            return Err(CsiError::Failed("missing staging target path".into()));
        }
        let mode = self.access_mode_for(access_mode).await?;
        let mut c = self.node_client()?;
        let req = NodeStageVolumeRequest {
            volume_id: vol_id.to_string(),
            publish_context,
            staging_target_path: staging_target_path.to_string(),
            volume_capability: Some(Self::volume_capability(
                mode,
                fs_type,
                mount_options,
                fs_group,
            )),
            secrets,
            volume_context,
        };
        match self.call(c.node_stage_volume(req)).await {
            Ok(_) => Ok(()),
            Err(s) if !is_final_error(&s) => Err(CsiError::UncertainProgress(s.to_string())),
            Err(s) => Err(CsiError::Failed(s.to_string())),
        }
    }

    /// Port of `NodeUnstageVolume` (`csi_client.go:456-480`).
    pub async fn node_unstage_volume(
        &self,
        vol_id: &str,
        staging_target_path: &str,
    ) -> Result<(), CsiError> {
        if vol_id.is_empty() {
            return Err(CsiError::Failed("missing volume id".into()));
        }
        if staging_target_path.is_empty() {
            return Err(CsiError::Failed("missing staging target path".into()));
        }
        let mut c = self.node_client()?;
        self.call(c.node_unstage_volume(NodeUnstageVolumeRequest {
            volume_id: vol_id.to_string(),
            staging_target_path: staging_target_path.to_string(),
        }))
        .await
        .map(|_| ())
        .map_err(|s| CsiError::Failed(s.to_string()))
    }
}

/// Strip a `unix://` (or `unix:`) scheme, leaving the socket path. The
/// registered endpoint is a path (`csi_plugin.go:118`) but plugin-registration
/// hands over `unix://` forms in some deployments.
fn normalize_endpoint(endpoint: &std::path::Path) -> PathBuf {
    let s = endpoint.to_string_lossy();
    let trimmed = s
        .strip_prefix("unix://")
        .or_else(|| s.strip_prefix("unix:"))
        .unwrap_or(&s);
    PathBuf::from(trimmed)
}

#[cfg(test)]
pub(crate) mod fake {
    //! A fake CSI driver: a real gRPC `csi.v1.Node` server on a unix socket, in
    //! the role upstream's `fake.NewIdentityClient`/`fake.NewNodeClient`
    //! (`pkg/volume/csi/fake/fake_client.go`) play — but over the wire, so the
    //! client's request construction and error mapping are exercised too.

    use super::proto::node_server::{Node, NodeServer};
    use super::proto::*;
    use std::sync::{Arc, Mutex};
    use tonic::{Request, Response, Status};

    #[derive(Default)]
    pub struct Calls {
        pub stage: Vec<NodeStageVolumeRequest>,
        pub unstage: Vec<NodeUnstageVolumeRequest>,
        pub publish: Vec<NodePublishVolumeRequest>,
        pub unpublish: Vec<NodeUnpublishVolumeRequest>,
        pub expand: Vec<NodeExpandVolumeRequest>,
        pub capability_calls: usize,
        pub node_get_info_calls: usize,
    }

    #[derive(Clone, Default)]
    pub struct FakeDriver {
        pub calls: Arc<Mutex<Calls>>,
        pub capabilities: Arc<Mutex<Vec<i32>>>,
        /// When set, `NodePublishVolume` fails with this code.
        pub publish_error: Arc<Mutex<Option<tonic::Code>>>,
        /// When set, `NodeExpandVolume` fails with this code.
        pub expand_error: Arc<Mutex<Option<tonic::Code>>>,
        /// `NodeGetInfo`'s answer: `Err` makes the RPC fail.
        pub node_info: Arc<Mutex<Option<Result<NodeGetInfoResponse, tonic::Code>>>>,
    }

    impl FakeDriver {
        pub fn with_capabilities(caps: &[node_service_capability::rpc::Type]) -> Self {
            let d = Self::default();
            *d.capabilities.lock().unwrap() = caps.iter().map(|c| *c as i32).collect();
            d
        }
    }

    #[tonic::async_trait]
    impl Node for FakeDriver {
        async fn node_stage_volume(
            &self,
            r: Request<NodeStageVolumeRequest>,
        ) -> Result<Response<NodeStageVolumeResponse>, Status> {
            self.calls.lock().unwrap().stage.push(r.into_inner());
            Ok(Response::new(NodeStageVolumeResponse {}))
        }
        async fn node_unstage_volume(
            &self,
            r: Request<NodeUnstageVolumeRequest>,
        ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
            self.calls.lock().unwrap().unstage.push(r.into_inner());
            Ok(Response::new(NodeUnstageVolumeResponse {}))
        }
        async fn node_publish_volume(
            &self,
            r: Request<NodePublishVolumeRequest>,
        ) -> Result<Response<NodePublishVolumeResponse>, Status> {
            let req = r.into_inner();
            // A conformant driver creates the target path; do the same so the
            // kubelet-side post-publish steps see a directory.
            let _ = std::fs::create_dir_all(&req.target_path);
            self.calls.lock().unwrap().publish.push(req);
            if let Some(code) = *self.publish_error.lock().unwrap() {
                return Err(Status::new(code, "fake publish failure"));
            }
            Ok(Response::new(NodePublishVolumeResponse {}))
        }
        async fn node_unpublish_volume(
            &self,
            r: Request<NodeUnpublishVolumeRequest>,
        ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
            self.calls.lock().unwrap().unpublish.push(r.into_inner());
            Ok(Response::new(NodeUnpublishVolumeResponse {}))
        }
        /// `fake.NodeClient.NodeExpandVolume` (`pkg/volume/csi/fake/fake_client.go:319-343`).
        async fn node_expand_volume(
            &self,
            r: Request<NodeExpandVolumeRequest>,
        ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
            let req = r.into_inner();
            if let Some(code) = *self.expand_error.lock().unwrap() {
                return Err(Status::new(code, "fake expand failure"));
            }
            let required = req.capacity_range.as_ref().map_or(0, |c| c.required_bytes);
            self.calls.lock().unwrap().expand.push(req);
            Ok(Response::new(NodeExpandVolumeResponse {
                capacity_bytes: required,
            }))
        }
        async fn node_get_info(
            &self,
            _r: Request<NodeGetInfoRequest>,
        ) -> Result<Response<NodeGetInfoResponse>, Status> {
            self.calls.lock().unwrap().node_get_info_calls += 1;
            match self.node_info.lock().unwrap().clone() {
                Some(Ok(r)) => Ok(Response::new(r)),
                Some(Err(code)) => Err(Status::new(code, "fake node info failure")),
                None => Ok(Response::new(NodeGetInfoResponse::default())),
            }
        }
        async fn node_get_capabilities(
            &self,
            _r: Request<NodeGetCapabilitiesRequest>,
        ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
            self.calls.lock().unwrap().capability_calls += 1;
            let capabilities = self
                .capabilities
                .lock()
                .unwrap()
                .iter()
                .map(|t| NodeServiceCapability {
                    r#type: Some(node_service_capability::Type::Rpc(
                        node_service_capability::Rpc { r#type: *t },
                    )),
                })
                .collect();
            Ok(Response::new(NodeGetCapabilitiesResponse { capabilities }))
        }
    }

    /// Serve `driver` on `socket` until the returned handle is dropped.
    pub fn serve(driver: FakeDriver, socket: &std::path::Path) -> tokio::task::JoinHandle<()> {
        let listener = tokio::net::UnixListener::bind(socket).unwrap();
        let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(NodeServer::new(driver))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        })
    }
}

#[cfg(test)]
mod tests {
    use super::fake::*;
    use super::proto::{NodeGetInfoResponse, Topology};
    use super::*;
    use rusternetes_common::quantity::Format;

    fn caps(c: &[NodeRpcType]) -> FakeDriver {
        FakeDriver::with_capabilities(c)
    }

    /// `TestClientNodeGetInfo` (`csi_client_test.go`): `NodeGetInfo` returns the
    /// driver's node id, its max volumes per node and the topology segments
    /// (`csi_client.go:172-209`, `nodeGetInfoV1`).
    #[tokio::test]
    async fn node_get_info_returns_id_limit_and_topology_segments() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        *d.node_info.lock().unwrap() = Some(Ok(NodeGetInfoResponse {
            node_id: "com.example.csi/csi-node1".into(),
            max_volumes_per_node: 10,
            accessible_topology: Some(Topology {
                segments: HashMap::from([("com.example.csi/zone".into(), "zoneA".into())]),
            }),
        }));
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);

        let info = c.node_get_info().await.unwrap();
        assert_eq!(info.node_id, "com.example.csi/csi-node1");
        assert_eq!(info.max_volumes_per_node, 10);
        assert_eq!(info.accessible_topology["com.example.csi/zone"], "zoneA");
        assert_eq!(d.calls.lock().unwrap().node_get_info_calls, 1);
    }

    /// No `accessible_topology` in the response: upstream returns a nil map
    /// (`csi_client.go:203-206`).
    #[tokio::test]
    async fn node_get_info_without_topology_is_empty_and_errors_propagate() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        *d.node_info.lock().unwrap() = Some(Ok(NodeGetInfoResponse {
            node_id: "n".into(),
            ..Default::default()
        }));
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);
        assert!(c
            .node_get_info()
            .await
            .unwrap()
            .accessible_topology
            .is_empty());

        *d.node_info.lock().unwrap() = Some(Err(tonic::Code::Unavailable));
        assert!(c.node_get_info().await.is_err());
    }

    /// `TestClientNodePublishVolume` (`csi_client_test.go`): the request carries
    /// volume id, target path, readonly, contexts, secrets, fs type and flags.
    #[tokio::test]
    async fn node_publish_volume_builds_the_request() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);

        let target = dir.path().join("target");
        c.node_publish_volume(
            "vol-1",
            true,
            "/stage",
            target.to_str().unwrap(),
            &PersistentVolumeAccessMode::ReadWriteOnce,
            HashMap::from([("pc".into(), "1".into())]),
            HashMap::from([("vc".into(), "2".into())]),
            HashMap::from([("s".into(), "3".into())]),
            "ext4",
            &["noatime".to_string()],
            Some(1000),
        )
        .await
        .unwrap();

        let calls = d.calls.lock().unwrap();
        let req = &calls.publish[0];
        assert_eq!(req.volume_id, "vol-1");
        assert_eq!(req.target_path, target.to_str().unwrap());
        assert!(req.readonly);
        assert_eq!(req.staging_target_path, "/stage");
        assert_eq!(req.publish_context["pc"], "1");
        assert_eq!(req.volume_context["vc"], "2");
        assert_eq!(req.secrets["s"], "3");
        let cap = req.volume_capability.as_ref().unwrap();
        assert_eq!(
            cap.access_mode.as_ref().unwrap().mode,
            AccessModeKind::SingleNodeWriter as i32
        );
        match cap.access_type.as_ref().unwrap() {
            AccessType::Mount(m) => {
                assert_eq!(m.fs_type, "ext4");
                assert_eq!(m.mount_flags, vec!["noatime"]);
                assert_eq!(m.volume_mount_group, "1000");
            }
            other => panic!("expected mount access type, got {other:?}"),
        }
    }

    /// `fsType == "block"` selects the block access type (`csi_client.go:259`).
    #[tokio::test]
    async fn node_publish_volume_block_fs_type_is_block_access() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);
        c.node_publish_volume(
            "v",
            false,
            "",
            dir.path().join("t").to_str().unwrap(),
            &PersistentVolumeAccessMode::ReadWriteOnce,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            "block",
            &[],
            None,
        )
        .await
        .unwrap();
        let calls = d.calls.lock().unwrap();
        assert!(matches!(
            calls.publish[0]
                .volume_capability
                .as_ref()
                .unwrap()
                .access_type,
            Some(AccessType::Block(_))
        ));
    }

    /// Missing volume id / target path are rejected before any RPC
    /// (`csi_client.go:224-229`).
    #[tokio::test]
    async fn node_publish_volume_validates_arguments() {
        let c = CsiDriverClient::with_endpoint("drv", "/nonexistent.sock");
        let e = c
            .node_publish_volume(
                "",
                false,
                "",
                "/t",
                &PersistentVolumeAccessMode::ReadWriteOnce,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                "",
                &[],
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "missing volume id");
        let e = c
            .node_publish_volume(
                "v",
                false,
                "",
                "",
                &PersistentVolumeAccessMode::ReadWriteOnce,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                "",
                &[],
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "missing target path");
    }

    /// A driver advertising SINGLE_NODE_MULTI_WRITER gets the capable mapper
    /// (`getNodeV1AccessModeMapper`, `csi_client.go:490`).
    #[tokio::test]
    async fn access_mode_mapper_follows_driver_capability() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[NodeRpcType::SingleNodeMultiWriter]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);
        c.node_publish_volume(
            "v",
            false,
            "",
            dir.path().join("t").to_str().unwrap(),
            &PersistentVolumeAccessMode::ReadWriteOncePod,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            "ext4",
            &[],
            None,
        )
        .await
        .unwrap();
        let calls = d.calls.lock().unwrap();
        assert_eq!(
            calls.publish[0]
                .volume_capability
                .as_ref()
                .unwrap()
                .access_mode
                .as_ref()
                .unwrap()
                .mode,
            AccessModeKind::SingleNodeSingleWriter as i32
        );
    }

    /// `isFinalError` table (`csi_client_test.go::TestIsFinalError`): a
    /// non-final publish error is uncertain-progress, a final one is not, and
    /// only a final error is an "operation finished" error.
    #[tokio::test]
    async fn publish_error_classification() {
        for (code, finished) in [
            (tonic::Code::InvalidArgument, true),
            (tonic::Code::NotFound, true),
            (tonic::Code::Internal, true),
            (tonic::Code::Aborted, false),
            (tonic::Code::Unavailable, false),
            (tonic::Code::DeadlineExceeded, false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let sock = dir.path().join("csi.sock");
            let d = caps(&[]);
            *d.publish_error.lock().unwrap() = Some(code);
            let _srv = serve(d, &sock);
            let c = CsiDriverClient::with_endpoint("drv", &sock);
            let e = c
                .node_publish_volume(
                    "v",
                    false,
                    "",
                    dir.path().join("t").to_str().unwrap(),
                    &PersistentVolumeAccessMode::ReadWriteOnce,
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                    "ext4",
                    &[],
                    None,
                )
                .await
                .unwrap_err();
            assert_eq!(e.is_operation_finished(), finished, "{code:?}");
            assert_eq!(
                matches!(e, CsiError::UncertainProgress(_)),
                !finished,
                "{code:?}"
            );
        }
    }

    #[tokio::test]
    async fn stage_unstage_unpublish_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[NodeRpcType::StageUnstageVolume]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);

        assert!(c.node_supports_stage_unstage().await.unwrap());
        assert!(!c.node_supports_volume_mount_group().await.unwrap());

        c.node_stage_volume(
            "v",
            HashMap::new(),
            "/stage",
            "xfs",
            &PersistentVolumeAccessMode::ReadWriteMany,
            HashMap::from([("k".into(), "v".into())]),
            HashMap::new(),
            &["ro".to_string()],
            None,
        )
        .await
        .unwrap();
        c.node_unstage_volume("v", "/stage").await.unwrap();
        c.node_unpublish_volume("v", "/target").await.unwrap();

        let calls = d.calls.lock().unwrap();
        assert_eq!(calls.stage[0].staging_target_path, "/stage");
        assert_eq!(calls.stage[0].secrets["k"], "v");
        assert_eq!(
            calls.stage[0]
                .volume_capability
                .as_ref()
                .unwrap()
                .access_mode
                .as_ref()
                .unwrap()
                .mode,
            AccessModeKind::MultiNodeMultiWriter as i32
        );
        assert_eq!(calls.unstage[0].staging_target_path, "/stage");
        assert_eq!(calls.unpublish[0].target_path, "/target");
    }

    fn resize_opts() -> CsiResizeOptions {
        CsiResizeOptions {
            volume_id: "vol-abcde".into(),
            volume_path: "/foo/bar".into(),
            staging_target_path: String::new(),
            fs_type: "ext4".into(),
            access_mode: PersistentVolumeAccessMode::ReadWriteOnce,
            new_size: Quantity::parse("10Gi").unwrap(),
            mount_options: vec!["noatime".into()],
            secrets: HashMap::new(),
        }
    }

    /// `TestClientNodeSupportsNodeExpand` (`csi_client_test.go:712`):
    /// `NodeSupportsNodeExpand` is the `EXPAND_VOLUME` capability.
    #[tokio::test]
    async fn node_supports_node_expand_follows_capability() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let _srv = serve(caps(&[NodeRpcType::StageUnstageVolume]), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);
        assert!(!c.node_supports_node_expand().await.unwrap());

        let sock2 = dir.path().join("csi2.sock");
        let _srv2 = serve(caps(&[NodeRpcType::ExpandVolume]), &sock2);
        let c2 = CsiDriverClient::with_endpoint("drv", &sock2);
        assert!(c2.node_supports_node_expand().await.unwrap());
    }

    /// `TestNodeExpandVolume` (`csi_client_test.go:776`) "with all correct
    /// values", plus the request shape `NodeExpandVolume` builds
    /// (`csi_client.go:316-345`).
    #[tokio::test]
    async fn node_expand_volume_builds_the_request() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);

        let mut o = resize_opts();
        o.staging_target_path = "/stage".into();
        o.secrets = HashMap::from([("k".into(), "v".into())]);
        let got = c.node_expand_volume(&o).await.unwrap();
        // The returned quantity is the driver's `CapacityBytes`.
        assert_eq!(got.value(), 10 * 1024 * 1024 * 1024);

        let calls = d.calls.lock().unwrap();
        let r = &calls.expand[0];
        assert_eq!(r.volume_id, "vol-abcde");
        assert_eq!(r.volume_path, "/foo/bar");
        assert_eq!(r.staging_target_path, "/stage");
        assert_eq!(r.capacity_range.as_ref().unwrap().required_bytes, 10 << 30);
        assert_eq!(r.secrets["k"], "v");
        let cap = r.volume_capability.as_ref().unwrap();
        assert_eq!(
            cap.access_mode.as_ref().unwrap().mode,
            AccessModeKind::SingleNodeWriter as i32
        );
        match cap.access_type.as_ref().unwrap() {
            AccessType::Mount(m) => {
                assert_eq!(m.fs_type, "ext4");
                assert_eq!(m.mount_flags, vec!["noatime".to_string()]);
            }
            other => panic!("expected mount access type, got {other:?}"),
        }
    }

    /// `fsType == "block"` selects the block access type
    /// (`csi_client.go:333-343`), and an unset staging path stays unset
    /// (`csi_client.go:326-330`).
    #[tokio::test]
    async fn node_expand_volume_block_access_type() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);
        let mut o = resize_opts();
        o.fs_type = FS_TYPE_BLOCK_NAME.into();
        c.node_expand_volume(&o).await.unwrap();
        let calls = d.calls.lock().unwrap();
        let r = &calls.expand[0];
        assert!(r.staging_target_path.is_empty());
        assert!(matches!(
            r.volume_capability.as_ref().unwrap().access_type,
            Some(AccessType::Block(_))
        ));
    }

    /// `TestNodeExpandVolume` failing rows: missing volume id, missing volume
    /// path, negative size. None reaches the driver.
    #[tokio::test]
    async fn node_expand_volume_validates_arguments() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);

        let mut o = resize_opts();
        o.volume_id.clear();
        assert!(matches!(c.node_expand_volume(&o).await,
            Err(NodeExpandError::Failed(m)) if m == "missing volume id"));
        let mut o = resize_opts();
        o.volume_path.clear();
        assert!(matches!(c.node_expand_volume(&o).await,
            Err(NodeExpandError::Failed(m)) if m == "missing volume path"));
        let mut o = resize_opts();
        o.new_size = Quantity::from_value(-10, Format::DecimalSI);
        assert!(matches!(c.node_expand_volume(&o).await,
            Err(NodeExpandError::Failed(m)) if m == "size can not be less than 0"));
        assert!(d.calls.lock().unwrap().expand.is_empty());
    }

    /// `NodeExpandVolume`'s error branch (`csi_client.go:347-353`): a final
    /// gRPC error is returned as-is (so `inUseError`/`isInfeasibleError` can
    /// read its code); a non-final one is uncertain progress.
    #[tokio::test]
    async fn node_expand_volume_error_classification() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("csi.sock");
        let d = caps(&[]);
        let _srv = serve(d.clone(), &sock);
        let c = CsiDriverClient::with_endpoint("drv", &sock);

        *d.expand_error.lock().unwrap() = Some(tonic::Code::InvalidArgument);
        match c.node_expand_volume(&resize_opts()).await {
            Err(NodeExpandError::Grpc(s)) => assert_eq!(s.code(), tonic::Code::InvalidArgument),
            other => panic!("expected a final gRPC error, got {other:?}"),
        }
        *d.expand_error.lock().unwrap() = Some(tonic::Code::Unavailable);
        assert!(matches!(
            c.node_expand_volume(&resize_opts()).await,
            Err(NodeExpandError::UncertainProgress(_))
        ));
    }

    /// `newCsiDriverClient` (`csi_client.go:153-170`) for an unregistered driver.
    #[test]
    fn unregistered_driver_is_an_error() {
        let e = CsiDriverClient::new("never.registered.example.com")
            .err()
            .unwrap();
        assert_eq!(
            e.to_string(),
            "driver name never.registered.example.com not found in the list of registered CSI drivers"
        );
    }
}
