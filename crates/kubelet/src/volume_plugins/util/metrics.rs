//! Port of `pkg/volume/metrics_block.go` (`metricsBlock`, `NewMetricsBlock`),
//! `metrics_nil.go` (`MetricsNil`) and the two `metrics_errors.go`
//! constructors they use (`NewNoPathDefinedError`, `NewNotSupportedError`).
//!
//! Also `pkg/volume/util/metrics.go`: `StorageOperationMetric`,
//! `OperationCompleteHook`, `GetFullQualifiedPluginNameForVolume` and
//! `csiOperationsLatencyMetric`.
//!
//! `servermetrics.CollectVolumeStatCalDuration("block", ..)`
//! (`metrics_block.go:55`) is ported in [`crate::server_metrics`].

use crate::server_metrics::collect_volume_stat_cal_duration;
use crate::volume_plugins::plugin::{Metrics, MetricsProvider};
use anyhow::{anyhow, Result};
use prometheus::{HistogramOpts, HistogramVec};
use rusternetes_common::quantity::{Format, Quantity};
use std::io::{Seek, SeekFrom};
use std::sync::{LazyLock, Once};
use std::time::{Duration, Instant};

/// `NewNoPathDefinedError` (`metrics_errors.go:60-65`).
pub const NO_PATH_DEFINED: &str = "no path defined for disk usage metrics.";
/// `NewNotSupportedError` (`metrics_errors.go:34-40`).
pub const NOT_SUPPORTED: &str = "metrics are not supported for MetricsNil Volumes";

/// `MetricsError` with code `ErrCodeNotSupported` (`metrics_errors.go:25`).
/// Kept as a typed error so `IsNotSupported` (`metrics_errors.go:86-88`) can
/// be answered by [`is_not_supported`].
#[derive(Debug, thiserror::Error)]
#[error("{msg}")]
pub struct NotSupportedError {
    msg: String,
}

/// `NewNotSupportedErrorWithDriverName` (`metrics_errors.go:50-57`): the
/// driver name is added to the message.
pub fn new_not_supported_error_with_driver_name(name: &str) -> anyhow::Error {
    anyhow::Error::new(NotSupportedError {
        msg: format!("metrics are not supported for {name} volumes"),
    })
}

/// `IsNotSupported` (`metrics_errors.go:86-88`).
pub fn is_not_supported(err: &anyhow::Error) -> bool {
    err.downcast_ref::<NotSupportedError>().is_some()
}

/// `metricsBlock` (`metrics_block.go:33-36`): detects the size of a
/// BlockMode volume from the device node it is attached to.
pub struct MetricsBlock {
    device: String,
}

/// `NewMetricsBlock` (`metrics_block.go:42-44`).
pub fn new_metrics_block(device: &str) -> MetricsBlock {
    MetricsBlock {
        device: device.to_string(),
    }
}

impl MetricsProvider for MetricsBlock {
    /// `GetMetrics` (`metrics_block.go:48-75`). Only the capacity of the
    /// device can be detected with standard tools.
    fn get_metrics(&self) -> Result<Metrics> {
        let start_time = Instant::now();
        let result = (|| {
            let mut metrics = Metrics {
                time: Some(chrono::Utc::now()),
                ..Default::default()
            };
            if self.device.is_empty() {
                return Err(anyhow!(NO_PATH_DEFINED));
            }
            self.get_block_info(&mut metrics)?;
            Ok(metrics)
        })();
        collect_volume_stat_cal_duration("block", start_time);
        result
    }
}

impl MetricsBlock {
    /// `getBlockInfo` (`metrics_block.go:77-91`): fetches `Capacity` by
    /// opening the device and seeking to the end.
    fn get_block_info(&self, metrics: &mut Metrics) -> Result<()> {
        let mut dev = std::fs::File::open(&self.device)
            .map_err(|e| anyhow!("unable to open device {:?}: {e}", self.device))?;
        let end = dev
            .seek(SeekFrom::End(0))
            .map_err(|e| anyhow!("failed to detect size of {:?}: {e}", self.device))?;
        metrics.capacity = Some(Quantity::from_value(end as i64, Format::BinarySI));
        Ok(())
    }
}

/// `MetricsNil` (`metrics_nil.go:19-34`): a [`MetricsProvider`] that does not
/// support returning metrics.
pub struct MetricsNil;

impl MetricsProvider for MetricsNil {
    /// `GetMetrics` (`metrics_nil.go:32-34`): an empty `Metrics` and an error.
    fn get_metrics(&self) -> Result<Metrics> {
        Err(anyhow!(NOT_SUPPORTED))
    }
}

/// `csiOperationsLatencyMetric` (`pkg/volume/util/metrics.go:66-75`):
/// subsystem `csi`, name `operations_seconds`, `StabilityLevel: ALPHA` (so the
/// served help carries the `[ALPHA] ` prefix, `component-base/metrics/opts.go`).
static CSI_OPERATIONS_LATENCY_METRIC: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "csi_operations_seconds",
            "[ALPHA] Container Storage Interface operation duration with gRPC error code status total",
        )
        .buckets(vec![
            0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 25.0, 50.0, 120.0, 300.0, 600.0,
        ]),
        &["driver_name", "method_name", "grpc_status_code", "migrated"],
    )
    .expect("valid csi operations histogram definition")
});

/// `StorageOperationMetric` (`pkg/volume/util/metrics.go:48-56`):
/// `storage_operation_duration_seconds`, `StabilityLevel: ALPHA`.
static STORAGE_OPERATION_METRIC: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "storage_operation_duration_seconds",
            "[ALPHA] Storage operation duration",
        )
        .buckets(vec![
            0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 25.0, 50.0, 120.0, 300.0, 600.0,
        ]),
        &["volume_plugin", "operation_name", "status", "migrated"],
    )
    .expect("valid storage operation histogram definition")
});

static REGISTER_METRICS: Once = Once::new();

/// `registerMetrics` (`metrics.go:81-87`), `registerMetrics.Do`; registered on
/// first use rather than from `init()` (see `crate::server_metrics`).
fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        prometheus::default_registry()
            .register(Box::new(CSI_OPERATIONS_LATENCY_METRIC.clone()))
            .expect("csi operations histogram registered once");
        prometheus::default_registry()
            .register(Box::new(STORAGE_OPERATION_METRIC.clone()))
            .expect("storage operation histogram registered once");
    });
}

/// `getErrorCode` (`metrics.go:148-161`): `codes.OK.String()` for no error, the
/// gRPC code name for a gRPC status. `unknown-non-grpc` (an error that did not
/// come from a gRPC call) cannot occur here: every error this client's
/// interceptor sees is a `tonic::Status`.
pub fn get_error_code(err: Option<&tonic::Status>) -> &'static str {
    use tonic::Code;
    // `codes.Code.String()` (`google.golang.org/grpc/codes/code_string.go`).
    match err.map(|s| s.code()) {
        None | Some(Code::Ok) => "OK",
        Some(Code::Cancelled) => "Canceled",
        Some(Code::Unknown) => "Unknown",
        Some(Code::InvalidArgument) => "InvalidArgument",
        Some(Code::DeadlineExceeded) => "DeadlineExceeded",
        Some(Code::NotFound) => "NotFound",
        Some(Code::AlreadyExists) => "AlreadyExists",
        Some(Code::PermissionDenied) => "PermissionDenied",
        Some(Code::ResourceExhausted) => "ResourceExhausted",
        Some(Code::FailedPrecondition) => "FailedPrecondition",
        Some(Code::Aborted) => "Aborted",
        Some(Code::OutOfRange) => "OutOfRange",
        Some(Code::Unimplemented) => "Unimplemented",
        Some(Code::Internal) => "Internal",
        Some(Code::Unavailable) => "Unavailable",
        Some(Code::DataLoss) => "DataLoss",
        Some(Code::Unauthenticated) => "Unauthenticated",
    }
}

/// `statusSuccess` / `statusFailUnknown` (`pkg/volume/util/metrics.go:35-38`).
const STATUS_SUCCESS: &str = "success";
const STATUS_FAIL_UNKNOWN: &str = "fail-unknown";

/// `GetFullQualifiedPluginNameForVolume` (`pkg/volume/util/metrics.go:127-139`):
/// a CSI volume's plugin name gets the driver appended
/// (`kubernetes.io/csi:csi-hostpath`) so metrics of different CSI drivers stay
/// apart.
pub fn get_full_qualified_plugin_name_for_volume(
    plugin_name: &str,
    spec: Option<&crate::volume_plugins::plugin::Spec<'_>>,
) -> String {
    if let Some(spec) = spec {
        if let Some(csi) = &spec.volume.csi {
            return format!("{plugin_name}:{}", csi.driver);
        }
        if let Some(csi) = spec.persistent_volume.and_then(|pv| pv.spec.csi.as_ref()) {
            return format!("{plugin_name}:{}", csi.driver);
        }
    }
    plugin_name.to_string()
}

/// The hook `OperationCompleteHook` (`pkg/volume/util/metrics.go:90-108`)
/// returns: created when the operation is generated (`requestTime :=
/// time.Now()`), called when it completes.
#[derive(Debug)]
pub struct OperationCompleteHook {
    plugin: String,
    operation_name: &'static str,
    request_time: Instant,
}

/// `OperationCompleteHook(plugin, operationName)`.
pub fn operation_complete_hook(
    plugin: &str,
    operation_name: &'static str,
) -> OperationCompleteHook {
    OperationCompleteHook {
        plugin: plugin.to_string(),
        operation_name,
        request_time: Instant::now(),
    }
}

impl OperationCompleteHook {
    /// `opComplete(CompleteFuncParam{Err, Migrated})`: observes the time since
    /// the hook was created under `status=success` or, when the operation's
    /// detailed error is set, `fail-unknown`. An unset `migrated` is false.
    pub fn complete(&self, err: Option<&anyhow::Error>, migrated: Option<bool>) {
        register_metrics();
        let status = if err.is_some() {
            STATUS_FAIL_UNKNOWN
        } else {
            STATUS_SUCCESS
        };
        STORAGE_OPERATION_METRIC
            .with_label_values(&[
                self.plugin.as_str(),
                self.operation_name,
                status,
                if migrated.unwrap_or(false) {
                    "true"
                } else {
                    "false"
                },
            ])
            .observe(self.request_time.elapsed().as_secs_f64());
    }
}

/// `RecordCSIOperationLatencyMetrics` (`metrics.go:138-146`): records the CSI
/// operation latency and gRPC status into `csi_operations_seconds`.
/// `migrated` is the `"true"`/`"false"` string upstream passes.
pub fn record_csi_operation_latency_metrics(
    driver_name: &str,
    operation_name: &str,
    operation_err: Option<&tonic::Status>,
    operation_duration: Duration,
    migrated: &str,
) {
    register_metrics();
    CSI_OPERATIONS_LATENCY_METRIC
        .with_label_values(&[
            driver_name,
            operation_name,
            get_error_code(operation_err),
            migrated,
        ])
        .observe(operation_duration.as_secs_f64());
}
