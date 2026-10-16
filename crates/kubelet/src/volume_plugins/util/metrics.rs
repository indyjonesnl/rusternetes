//! Port of `pkg/volume/metrics_block.go` (`metricsBlock`, `NewMetricsBlock`),
//! `metrics_nil.go` (`MetricsNil`) and the two `metrics_errors.go`
//! constructors they use (`NewNoPathDefinedError`, `NewNotSupportedError`).
//!
//! `servermetrics.CollectVolumeStatCalDuration("block", ..)`
//! (`metrics_block.go:55`) is ported in [`crate::server_metrics`].

use crate::server_metrics::collect_volume_stat_cal_duration;
use crate::volume_plugins::plugin::{Metrics, MetricsProvider};
use anyhow::{anyhow, Result};
use rusternetes_common::quantity::{Format, Quantity};
use std::io::{Seek, SeekFrom};
use std::time::Instant;

/// `NewNoPathDefinedError` (`metrics_errors.go:60-65`).
pub const NO_PATH_DEFINED: &str = "no path defined for disk usage metrics.";
/// `NewNotSupportedError` (`metrics_errors.go:34-40`).
pub const NOT_SUPPORTED: &str = "metrics are not supported for MetricsNil Volumes";

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

#[cfg(test)]
mod tests {
    use super::*;

    /// `metrics_block.go:62-64`: an empty device is `NoPathDefined`.
    #[test]
    fn block_metrics_without_a_device_is_no_path_defined() {
        let e = new_metrics_block("").get_metrics().unwrap_err();
        assert_eq!(e.to_string(), NO_PATH_DEFINED);
    }

    /// `metrics_block.go:55`: GetMetrics defers
    /// `CollectVolumeStatCalDuration("block", ..)`.
    #[test]
    fn block_metrics_observes_volume_stat_duration() {
        let _ = new_metrics_block("").get_metrics();
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        assert!(body.contains(
            "kubelet_volume_metric_collection_duration_seconds_count{metric_source=\"block\"}"
        ));
    }

    /// `getBlockInfo` seeks to the end: the capacity is the file size.
    #[test]
    fn block_metrics_capacity_is_the_size_found_by_seeking_to_the_end() {
        let f = tempfile::NamedTempFile::new().unwrap();
        f.as_file().set_len(4096).unwrap();
        let m = new_metrics_block(f.path().to_str().unwrap())
            .get_metrics()
            .unwrap();
        assert_eq!(m.capacity.unwrap().value(), 4096);
    }

    #[test]
    fn block_metrics_of_a_missing_device_fail_to_open() {
        let e = new_metrics_block("/nonexistent/dev")
            .get_metrics()
            .unwrap_err();
        assert!(e.to_string().starts_with("unable to open device"), "{e}");
    }

    #[test]
    fn nil_metrics_are_not_supported() {
        assert_eq!(
            MetricsNil.get_metrics().unwrap_err().to_string(),
            NOT_SUPPORTED
        );
    }
}
