//! Port of `pkg/volume/metrics_statfs.go` (`metricsStatFS`),
//! `pkg/volume/metrics_errors.go` (`MetricsError`) and `fs.Info`
//! (`pkg/volume/util/fs/fs.go`, linux).

use crate::server_metrics::collect_volume_stat_cal_duration;
use crate::volume_plugins::plugin::{Metrics, MetricsProvider};
use anyhow::Result;
use rusternetes_common::quantity::{Format, Quantity};
use std::time::Instant;

/// `MetricsError` codes (`metrics_errors.go:23-31`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsErrorCode {
    NotSupported = 1,
    NoPathDefined = 2,
    FsInfoFailed = 3,
}

/// `MetricsError` (`metrics_errors.go:78-85`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsError {
    pub code: MetricsErrorCode,
    pub msg: String,
}

impl std::fmt::Display for MetricsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for MetricsError {}

/// `metricsStatFS` (`metrics_statfs.go:31-34`).
#[derive(Clone)]
pub struct MetricsStatFs {
    path: String,
}

impl MetricsStatFs {
    /// `NewMetricsStatFS` (`metrics_statfs.go:37-39`).
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }
}

/// `NewNoPathDefinedError` (`metrics_errors.go:56-61`).
fn no_path_defined_error() -> MetricsError {
    MetricsError {
        code: MetricsErrorCode::NoPathDefined,
        msg: "no path defined for disk usage metrics.".to_string(),
    }
}

/// `NewFsInfoFailedError` (`metrics_errors.go:64-69`).
fn fs_info_failed_error(err: &std::io::Error) -> MetricsError {
    MetricsError {
        code: MetricsErrorCode::FsInfoFailed,
        msg: format!("failed to get FsInfo due to error {err}"),
    }
}

/// `fs.Info` (linux, `fs.go:39-60`): (available bytes, byte capacity, byte
/// usage, total inodes, inodes free, inode usage).
fn fs_info(path: &str) -> std::io::Result<(i64, i64, i64, i64, i64, i64)> {
    let c = std::ffi::CString::new(path)
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    // SAFETY: `c` is NUL-terminated; `st` is a valid out-pointer.
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c.as_ptr(), &mut st) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let bsize = st.f_bsize as i64;
    let available = st.f_bavail as i64 * bsize;
    let capacity = st.f_blocks as i64 * bsize;
    let usage = (st.f_blocks as i64 - st.f_bfree as i64) * bsize;
    let inodes = st.f_files as i64;
    let inodes_free = st.f_ffree as i64;
    Ok((
        available,
        capacity,
        usage,
        inodes,
        inodes_free,
        inodes - inodes_free,
    ))
}

impl MetricsStatFs {
    /// `getFsInfo` (`metrics_statfs.go:63-75`): every quantity is
    /// `resource.BinarySI`.
    fn get_fs_info(&self, metrics: &mut Metrics) -> Result<()> {
        let (available, capacity, usage, inodes, inodes_free, inodes_used) =
            fs_info(&self.path).map_err(|e| fs_info_failed_error(&e))?;
        let q = |v| Some(Quantity::from_value(v, Format::BinarySI));
        metrics.available = q(available);
        metrics.capacity = q(capacity);
        metrics.used = q(usage);
        metrics.inodes = q(inodes);
        metrics.inodes_free = q(inodes_free);
        metrics.inodes_used = q(inodes_used);
        Ok(())
    }
}

impl MetricsProvider for MetricsStatFs {
    /// `GetMetrics` (`metrics_statfs.go:46-60`).
    ///
    /// `defer servermetrics.CollectVolumeStatCalDuration("statfs", startTime)`
    /// (`:47`) observes on every return path, including the early errors.
    /// Idiom difference: upstream returns the partial `*Metrics` alongside
    /// the error; the `Result` here carries only the error.
    fn get_metrics(&self) -> Result<Metrics> {
        let start_time = Instant::now();
        let result = (|| {
            if self.path.is_empty() {
                return Err(no_path_defined_error().into());
            }
            let mut metrics = Metrics {
                time: Some(chrono::Utc::now()),
                ..Default::default()
            };
            self.get_fs_info(&mut metrics)?;
            Ok(metrics)
        })();
        collect_volume_stat_cal_duration("statfs", start_time);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code(e: &anyhow::Error) -> Option<MetricsErrorCode> {
        e.downcast_ref::<MetricsError>().map(|m| m.code)
    }

    // TestGetMetricsStatFS (metrics_statfs_test.go:29)
    #[test]
    fn get_metrics_statfs() {
        let err = MetricsStatFs::new("").get_metrics().unwrap_err();
        assert_eq!(code(&err), Some(MetricsErrorCode::NoPathDefined));
        assert_eq!(err.to_string(), "no path defined for disk usage metrics.");

        let err = MetricsStatFs::new("/not/a/real/directory")
            .get_metrics()
            .unwrap_err();
        assert_eq!(code(&err), Some(MetricsErrorCode::FsInfoFailed));
        assert!(err
            .to_string()
            .starts_with("failed to get FsInfo due to error "));

        let tmp = tempfile::tempdir().unwrap();
        let m = MetricsStatFs::new(tmp.path().to_str().unwrap())
            .get_metrics()
            .unwrap();
        assert!(m.time.is_some());
        assert!(m.capacity.as_ref().unwrap().value() > 0);
        assert!(m.available.as_ref().unwrap().value() > 0);
        assert!(m.used.as_ref().unwrap().value() >= 0);
        assert_eq!(
            m.inodes_used.as_ref().unwrap().value(),
            m.inodes.as_ref().unwrap().value() - m.inodes_free.as_ref().unwrap().value()
        );
    }

    // metrics_statfs.go:47: GetMetrics defers CollectVolumeStatCalDuration
    // ("statfs", ..), so every call (even the no-path error) is observed.
    #[test]
    fn get_metrics_statfs_observes_volume_stat_duration() {
        let _ = MetricsStatFs::new("").get_metrics();
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        assert!(body.contains(
            "kubelet_volume_metric_collection_duration_seconds_count{metric_source=\"statfs\"}"
        ));
    }
}
