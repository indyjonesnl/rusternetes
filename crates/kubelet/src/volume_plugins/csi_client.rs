//! Port of `pkg/volume/csi/csi_client.go` — the kubelet's CSI Node-service
//! client.
//!
//! Every method opens a fresh connection to the driver's socket for the call
//! and drops it afterwards, exactly as upstream does (`nodeV1ClientCreator`
//! returns a new `grpc.ClientConn` and a `closer` per RPC,
//! `csi_client.go:142-151`). Our channel is lazy, so "open" is free and the
//! first RPC dials.

use super::csi_drivers_store::csi_drivers;
use super::plugin::{Metrics, Spec};
use super::util::metrics::record_csi_operation_latency_metrics;
use hyper_util::rt::TokioIo;
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::PersistentVolumeAccessMode;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

pub(crate) use rusternetes_csi::normalize_endpoint;
/// The generated `csi.v1` proto lives in the shared `rusternetes-csi` crate
/// (the controller-manager's provisioner uses it too).
pub use rusternetes_csi::proto;

use proto::node_client::NodeClient;
use proto::node_service_capability::rpc::Type as NodeRpcType;
use proto::volume_capability::access_mode::Mode as AccessModeKind;
use proto::volume_capability::{AccessMode, AccessType, BlockVolume, MountVolume};
use proto::{
    CapacityRange, NodeExpandVolumeRequest, NodeGetCapabilitiesRequest, NodeGetInfoRequest,
    NodeGetVolumeStatsRequest, NodePublishVolumeRequest, NodeStageVolumeRequest,
    NodeUnpublishVolumeRequest, NodeUnstageVolumeRequest, VolumeCapability,
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
        // `asCSIAccessModeV1` falls through to `..._UNKNOWN` (csi_client.go:513).
        PersistentVolumeAccessMode::Unknown(_) => AccessModeKind::Unknown,
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
        PersistentVolumeAccessMode::Unknown(_) => AccessModeKind::Unknown,
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
    /// The `Migrated` field of the `additionalInfo` that
    /// `createCSIOperationContext` (`csi_util.go:198-204`) stores in the
    /// operation's `context.Context` and `RecordMetricsInterceptor`
    /// (`csi_metrics.go:112-136`) reads back. Rust has no `context.Value`; the
    /// operation's client carries it instead. False by default, like an
    /// operation whose context has no `additionalInfo` (`csi_metrics.go:126`).
    migrated: bool,
}

/// `createCSIOperationContext` (`csi_util.go:198-204`), the part that feeds
/// metrics: `migrated = volumeSpec.Migrated` for a non-nil spec, false for nil
/// (`csi_attacher.go:557`, `csi_mounter.go:447`: "could not get spec info on
/// whether this is a migrated operation because c.spec is nil").
pub fn csi_operation_migrated(volume_spec: Option<&Spec<'_>>) -> bool {
    volume_spec.is_some_and(|s| s.migrated)
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
            migrated: false,
        }
    }

    /// Mark this client's calls as belonging to an operation on `volume_spec`
    /// (`createCSIOperationContext(volumeSpec, ..)`, `csi_util.go:198`), so the
    /// `migrated` label of `csi_operations_seconds` follows `Spec.Migrated`.
    pub fn with_operation_spec(mut self, volume_spec: Option<&Spec<'_>>) -> Self {
        self.migrated = csi_operation_migrated(volume_spec);
        self
    }

    /// Like [`Self::with_operation_spec`] for a caller that already holds the
    /// spec's `Migrated` flag.
    pub fn with_migrated(mut self, migrated: bool) -> Self {
        self.migrated = migrated;
        self
    }

    /// The `migrated` value the metrics interceptor labels calls with.
    pub fn migrated(&self) -> bool {
        self.migrated
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
    ///
    /// Also the port of `MetricsManager.RecordMetricsInterceptor`
    /// (`csi_metrics.go:112-136`, installed by `newGrpcConn`,
    /// `csi_client.go:543`): the call's duration and gRPC status are recorded
    /// under `method` with the operation's `migrated` label.
    async fn call<T>(
        &self,
        method: &str,
        fut: impl std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    ) -> Result<T, tonic::Status> {
        let start = std::time::Instant::now();
        let result = match tokio::time::timeout(CSI_TIMEOUT, fut).await {
            Ok(r) => r.map(|r| r.into_inner()),
            Err(_) => Err(tonic::Status::deadline_exceeded(
                "context deadline exceeded",
            )),
        };
        record_csi_operation_latency_metrics(
            &self.driver_name,
            method,
            result.as_ref().err(),
            start.elapsed(),
            if self.migrated { "true" } else { "false" },
        );
        result
    }

    /// `nodeGetCapabilities` (`csi_client.go:692-709`).
    async fn node_get_capabilities(&self) -> Result<Vec<NodeRpcType>, CsiError> {
        let mut c = self.node_client()?;
        let resp = self
            .call(
                "/csi.v1.Node/NodeGetCapabilities",
                c.node_get_capabilities(NodeGetCapabilitiesRequest {}),
            )
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
            .call(
                "/csi.v1.Node/NodeGetInfo",
                c.node_get_info(NodeGetInfoRequest {}),
            )
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

    /// `NodeSupportsVolumeStats` (`csi_client.go:580-582`): the `GET_VOLUME_STATS`
    /// node capability.
    pub async fn node_supports_volume_stats(&self) -> Result<bool, CsiError> {
        self.node_supports_capability(NodeRpcType::GetVolumeStats)
            .await
    }

    /// `nodeSupportsVolumeCondition` (`csi_client.go:666-668`).
    async fn node_supports_volume_condition(&self) -> Result<bool, CsiError> {
        self.node_supports_capability(NodeRpcType::VolumeCondition)
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
        match self
            .call("/csi.v1.Node/NodePublishVolume", c.node_publish_volume(req))
            .await
        {
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
        self.call(
            "/csi.v1.Node/NodeUnpublishVolume",
            c.node_unpublish_volume(NodeUnpublishVolumeRequest {
                volume_id: vol_id.to_string(),
                target_path: target_path.to_string(),
            }),
        )
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
        match self
            .call("/csi.v1.Node/NodeExpandVolume", c.node_expand_volume(req))
            .await
        {
            Ok(resp) => Ok(Quantity::from_value(resp.capacity_bytes, Format::BinarySI)),
            Err(s) if !is_final_error(&s) => Err(NodeExpandError::UncertainProgress(s.to_string())),
            Err(s) => Err(NodeExpandError::Grpc(Box::new(s))),
        }
    }

    /// Port of `NodeGetVolumeStats` (`csi_client.go:588-664`): ask the driver
    /// for the usage of the volume published at `target_path` and map it to
    /// [`Metrics`]. `time` is left unset, as upstream leaves `metrics.Time`
    /// for `metricsCsi.GetMetrics` to set.
    pub async fn node_get_volume_stats(
        &self,
        vol_id: &str,
        target_path: &str,
    ) -> Result<Metrics, CsiError> {
        if vol_id.is_empty() {
            return Err(CsiError::Failed("missing volume id".into()));
        }
        if target_path.is_empty() {
            return Err(CsiError::Failed("missing target path".into()));
        }
        let mut c = self.node_client()?;
        let resp = self
            .call(
                "/csi.v1.Node/NodeGetVolumeStats",
                c.node_get_volume_stats(NodeGetVolumeStatsRequest {
                    volume_id: vol_id.to_string(),
                    volume_path: target_path.to_string(),
                    staging_target_path: String::new(),
                }),
            )
            .await
            .map_err(|s| CsiError::Failed(s.to_string()))?;

        let zero = || Some(Quantity::from_value(0, Format::BinarySI));
        let mut metrics = Metrics {
            used: zero(),
            capacity: zero(),
            available: zero(),
            inodes_used: zero(),
            inodes: zero(),
            inodes_free: zero(),
            ..Default::default()
        };

        let mut is_support_node_volume_condition = false;
        // `utilfeature.DefaultFeatureGate.Enabled(features.CSIVolumeHealth)`
        if rusternetes_common::feature_gates::gate_enabled("CSIVolumeHealth").unwrap_or(false) {
            is_support_node_volume_condition = self.node_supports_volume_condition().await?;
            if is_support_node_volume_condition {
                // `resp.VolumeCondition.GetAbnormal()` / `GetMessage()`: a nil
                // condition reads as false / "".
                let cond = resp.volume_condition.clone().unwrap_or_default();
                metrics.abnormal = Some(cond.abnormal);
                metrics.message = Some(cond.message);
            }
        }

        // If the driver does not support volume condition and usages is nil,
        // return an error. (proto3 `repeated` has no nil; empty is nil.)
        if !is_support_node_volume_condition && resp.usage.is_empty() {
            return Err(CsiError::Failed(
                "failed to get usage from response. usage is nil".into(),
            ));
        }

        for usage in &resp.usage {
            match proto::volume_usage::Unit::try_from(usage.unit) {
                Ok(proto::volume_usage::Unit::Bytes) => {
                    metrics.available =
                        Some(Quantity::from_value(usage.available, Format::BinarySI));
                    metrics.capacity = Some(Quantity::from_value(usage.total, Format::BinarySI));
                    metrics.used = Some(Quantity::from_value(usage.used, Format::BinarySI));
                }
                Ok(proto::volume_usage::Unit::Inodes) => {
                    metrics.inodes_free =
                        Some(Quantity::from_value(usage.available, Format::BinarySI));
                    metrics.inodes = Some(Quantity::from_value(usage.total, Format::BinarySI));
                    metrics.inodes_used = Some(Quantity::from_value(usage.used, Format::BinarySI));
                }
                _ => tracing::error!(unit = usage.unit, "unknown unit in VolumeUsage"),
            }
        }
        Ok(metrics)
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
        match self
            .call("/csi.v1.Node/NodeStageVolume", c.node_stage_volume(req))
            .await
        {
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
        self.call(
            "/csi.v1.Node/NodeUnstageVolume",
            c.node_unstage_volume(NodeUnstageVolumeRequest {
                volume_id: vol_id.to_string(),
                staging_target_path: staging_target_path.to_string(),
            }),
        )
        .await
        .map(|_| ())
        .map_err(|s| CsiError::Failed(s.to_string()))
    }
}
