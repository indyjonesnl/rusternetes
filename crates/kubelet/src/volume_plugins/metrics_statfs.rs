//! Port of `pkg/volume/metrics_statfs.go` (`metricsStatFS`),
//! `pkg/volume/metrics_errors.go` (`MetricsError`) and `fs.Info`
//! (`pkg/volume/util/fs/fs.go`, linux).

use crate::volume_plugins::plugin::{Metrics, MetricsProvider};
use anyhow::Result;

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

impl MetricsProvider for MetricsStatFs {
    fn get_metrics(&self) -> Result<Metrics> {
        Err(anyhow::anyhow!("unimplemented"))
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
}
