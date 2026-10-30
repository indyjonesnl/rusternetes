//! Port of `pkg/volume/csi/csi_metrics.go` (`metricsCsi`, `NewMetricsCsi`):
//! the `volume.MetricsProvider` of a CSI mounter and block mapper, which asks
//! the driver for `NodeGetVolumeStats`.
//!
//! The Go `MetricsManager` / `RecordMetricsInterceptor` half of that file is
//! gRPC latency instrumentation of the client connection and is not part of
//! this provider.

use super::*;
use crate::server_metrics::collect_volume_stat_cal_duration;
use crate::volume_plugins::csi_client::CSI_TIMEOUT;
use crate::volume_plugins::plugin::{Metrics, MetricsProvider};
use crate::volume_plugins::util::metrics::new_not_supported_error_with_driver_name;
use std::sync::Mutex;
use std::time::Instant;

/// `metricsCsi` (`csi_metrics.go:37-44`).
pub struct MetricsCsi {
    /// `targetPath`: the directory path the volume is mounted to.
    target_path: String,
    /// `volumeID`: volume handle or id.
    volume_id: String,
    /// `csiClientGetter.driverName`.
    driver_name: String,
    /// `csiClientGetter.csiClient`: the cached client.
    client: Mutex<Option<Arc<CsiDriverClient>>>,
}

/// `NewMetricsCsi` (`csi_metrics.go:49-53`).
pub fn new_metrics_csi(volume_id: &str, target_path: &str, driver_name: &str) -> MetricsCsi {
    MetricsCsi {
        target_path: target_path.to_string(),
        volume_id: volume_id.to_string(),
        driver_name: driver_name.to_string(),
        client: Mutex::new(None),
    }
}

impl MetricsCsi {
    /// `csiClientGetter.Get`: the cached client, else a new one for the
    /// registered driver.
    fn get_client(&self) -> Result<Arc<CsiDriverClient>, CsiError> {
        let mut cached = self.client.lock().unwrap();
        if let Some(c) = cached.as_ref() {
            return Ok(c.clone());
        }
        let c = Arc::new(CsiDriverClient::new(&self.driver_name)?);
        *cached = Some(c.clone());
        Ok(c)
    }

    async fn get_metrics_async(&self) -> Result<Metrics> {
        let current_time = chrono::Utc::now();
        // Get CSI client. Treat the absence of the CSI driver as a transient
        // error (see kubernetes issue 120268, cited by `csi_metrics.go:65`).
        let client = self
            .get_client()
            .map_err(|e| CsiError::Transient(e.to_string()))?;
        // Check whether "GET_VOLUME_STATS" is set.
        let volume_stats_set =
            tokio::time::timeout(CSI_TIMEOUT, client.node_supports_volume_stats())
                .await
                .map_err(|_| anyhow!("context deadline exceeded"))??;
        // If the plugin does not support volume stats, return.
        if !volume_stats_set {
            return Err(new_not_supported_error_with_driver_name(&self.driver_name));
        }
        let mut metrics = tokio::time::timeout(
            CSI_TIMEOUT,
            client.node_get_volume_stats(&self.volume_id, &self.target_path),
        )
        .await
        .map_err(|_| anyhow!("context deadline exceeded"))??;
        // Set recorded time.
        metrics.time = Some(current_time);
        Ok(metrics)
    }
}

impl MetricsProvider for MetricsCsi {
    /// `GetMetrics` (`csi_metrics.go:55-91`). The trait is synchronous, as
    /// upstream's call is blocking, so the async client call is driven to
    /// completion here.
    fn get_metrics(&self) -> Result<Metrics> {
        let start = Instant::now();
        let result = match tokio::runtime::Handle::try_current() {
            Ok(h) => tokio::task::block_in_place(|| h.block_on(self.get_metrics_async())),
            Err(_) => tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(self.get_metrics_async()),
        };
        // `defer servermetrics.CollectVolumeStatCalDuration(driverName, startTime)`
        collect_volume_stat_cal_duration(&self.driver_name, start);
        result
    }
}

/// `csiMountMgr.MetricsProvider = NewMetricsCsi(volumeHandle, dir, driverName)`
/// (`csi_plugin.go:535`), `dir := mounter.GetPath()`.
impl MetricsProvider for CsiMounter {
    fn get_metrics(&self) -> Result<Metrics> {
        new_metrics_csi(&self.volume_id, &self.path, &self.driver_name).get_metrics()
    }
}
