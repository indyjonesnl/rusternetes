//! Port of `pkg/volume/metrics_du.go` (`metricsDu`) and `DiskUsage`
//! (`pkg/volume/util/fs/fs.go:65-142`, linux).
//!
//! `DiskUsage` observes `servermetrics.CollectVolumeStatCalDuration` under
//! `fsquota` when the quota system knows the path and `du` otherwise
//! (`fs.go:80-85`). The fsquota provider (`pkg/volume/util/fsquota`) is gated
//! by `LocalStorageCapacityIsolationFSQuotaMonitoring` and not ported, so
//! `GetConsumption`/`GetInodes` always return nil here, which is exactly
//! upstream's "path does not support quotas" case: the `du` source and the
//! walk are used.

use crate::server_metrics::collect_volume_stat_cal_duration;
use crate::volume_plugins::metrics_statfs::{fs_info, fs_info_failed_error, no_path_defined_error};
use crate::volume_plugins::plugin::{Metrics, MetricsProvider};
use anyhow::{anyhow, Result};
use rusternetes_common::quantity::{Format, Quantity};
use std::collections::HashSet;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::Instant;

/// `fs.UsageInfo` (`fs.go:35-38`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct UsageInfo {
    pub bytes: i64,
    pub inodes: i64,
}

/// `filepath.SkipDir` as returned by the `Walk` callback.
enum Step {
    Continue,
    SkipDir,
}

/// `DiskUsage` (`fs.go:65-142`).
pub fn disk_usage(path: &str) -> Result<UsageInfo> {
    let mut usage = UsageInfo::default();
    if path.is_empty() {
        return Err(anyhow!("invalid directory"));
    }

    // fs.go:73-85: fsquota.GetConsumption/GetInodes are nil (see module doc).
    let start_time = Instant::now();
    let result = (|| {
        let top_level_dev = std::fs::metadata(path)?.dev();
        let mut deduped_inodes: HashSet<u64> = HashSet::new();
        let md = std::fs::symlink_metadata(path)?;
        walk(
            Path::new(path),
            &md,
            top_level_dev,
            &mut deduped_inodes,
            &mut usage,
        )?;
        Ok(())
    })();
    // fs.go:84: `defer servermetrics.CollectVolumeStatCalDuration("du", ..)`.
    collect_volume_stat_cal_duration("du", start_time);
    result.map(|()| usage)
}

/// The `filepath.Walk` callback of `fs.go:104-139` for one entry.
fn visit(
    path: &Path,
    md: &std::fs::Metadata,
    top_level_dev: u64,
    deduped: &mut HashSet<u64>,
    usage: &mut UsageInfo,
) -> Step {
    // fs.go:122: don't descend into directories on other devices.
    if md.dev() != top_level_dev {
        return Step::SkipDir;
    }
    // fs.go:127-134: dedupe hardlinks.
    if md.nlink() > 1 && !deduped.insert(md.ino()) {
        return Step::Continue;
    }
    let _ = path;
    usage.bytes += md.blocks() as i64 * 512;
    usage.inodes += 1;
    Step::Continue
}

/// `filepath.Walk`'s `walk`: lstat-based, lexical order. A `SkipDir` from a
/// non-directory skips the rest of its parent directory.
fn walk(
    path: &Path,
    md: &std::fs::Metadata,
    top_level_dev: u64,
    deduped: &mut HashSet<u64>,
    usage: &mut UsageInfo,
) -> Result<Step> {
    let step = visit(path, md, top_level_dev, deduped, usage);
    if !md.is_dir() {
        return Ok(step);
    }
    if matches!(step, Step::SkipDir) {
        return Ok(Step::Continue);
    }
    let mut names: Vec<_> = match std::fs::read_dir(path) {
        Ok(rd) => rd.filter_map(|e| e.ok()).map(|e| e.file_name()).collect(),
        // fs.go:108-110: ignore files deleted after the directory was read.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Step::Continue),
        Err(e) => {
            return Err(anyhow!(
                "unable to count inodes for {}: {e}",
                path.display()
            ))
        }
    };
    names.sort();
    for name in names {
        let child = path.join(&name);
        let cmd = match std::fs::symlink_metadata(&child) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(anyhow!(
                    "unable to count inodes for {}: {e}",
                    child.display()
                ))
            }
        };
        match walk(&child, &cmd, top_level_dev, deduped, usage)? {
            Step::SkipDir if !cmd.is_dir() => return Ok(Step::Continue),
            _ => {}
        }
    }
    Ok(Step::Continue)
}

/// `metricsDu` (`metrics_du.go:30-33`).
#[derive(Clone)]
pub struct MetricsDu {
    path: String,
}

impl MetricsDu {
    /// `NewMetricsDu` (`metrics_du.go:36-38`).
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }
}

impl MetricsProvider for MetricsDu {
    /// `GetMetrics` (`metrics_du.go:43-59`).
    fn get_metrics(&self) -> Result<Metrics> {
        if self.path.is_empty() {
            return Err(no_path_defined_error().into());
        }
        let mut metrics = Metrics {
            time: Some(chrono::Utc::now()),
            ..Default::default()
        };
        let q = |v| Some(Quantity::from_value(v, Format::BinarySI));
        // getDiskUsage (`:62-70`)
        let usage = disk_usage(&self.path)?;
        metrics.used = q(usage.bytes);
        metrics.inodes_used = q(usage.inodes);
        // getFsInfo (`:74-84`)
        let (available, capacity, _, inodes, inodes_free, _) =
            fs_info(&self.path).map_err(|e| fs_info_failed_error(&e))?;
        metrics.available = q(available);
        metrics.capacity = q(capacity);
        metrics.inodes = q(inodes);
        metrics.inodes_free = q(inodes_free);
        Ok(metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_plugins::metrics_statfs::MetricsErrorCode;

    fn blocks_bytes(p: &Path) -> i64 {
        std::fs::symlink_metadata(p).unwrap().blocks() as i64 * 512
    }

    fn du_count() -> f64 {
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        body.lines()
            .find(|l| {
                l.starts_with(
                    "kubelet_volume_metric_collection_duration_seconds_count{metric_source=\"du\"}",
                )
            })
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0)
    }

    // TestMetricsDuGetCapacity (metrics_du_test.go:31)
    #[test]
    fn metrics_du_get_capacity() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let metrics = MetricsDu::new(dir.to_str().unwrap());
        let empty = blocks_bytes(dir);

        let actual = metrics.get_metrics().unwrap();
        assert_eq!(empty as i128, actual.used.as_ref().unwrap().value());
        assert!(actual.capacity.as_ref().unwrap().value() > 0);
        assert!(actual.available.as_ref().unwrap().value() > 0);

        let f1 = dir.join("f1");
        std::fs::write(&f1, "Hello World").unwrap();
        let actual = metrics.get_metrics().unwrap();
        assert_eq!(
            (empty + blocks_bytes(&f1)) as i128,
            actual.used.as_ref().unwrap().value()
        );

        // a hardlink leaves the inode count unchanged
        let previous = actual.inodes_used.as_ref().unwrap().value();
        std::fs::hard_link(&f1, dir.join("f2")).unwrap();
        let actual = metrics.get_metrics().unwrap();
        assert_eq!(previous, actual.inodes_used.as_ref().unwrap().value());
    }

    // TestMetricsDuRequirePath (metrics_du_test.go:84)
    #[test]
    fn metrics_du_require_path() {
        let err = MetricsDu::new("").get_metrics().unwrap_err();
        let me = err.downcast_ref::<crate::volume_plugins::metrics_statfs::MetricsError>();
        assert_eq!(me.map(|m| m.code), Some(MetricsErrorCode::NoPathDefined));
    }

    // TestMetricsDuRequireRealDirectory (metrics_du_test.go:98)
    #[test]
    fn metrics_du_require_real_directory() {
        assert!(MetricsDu::new("/not/a/real/directory")
            .get_metrics()
            .is_err());
    }

    // fs.go:84-85: DiskUsage defers CollectVolumeStatCalDuration("du", ..).
    #[test]
    fn disk_usage_observes_du_duration() {
        let tmp = tempfile::tempdir().unwrap();
        let before = du_count();
        disk_usage(tmp.path().to_str().unwrap()).unwrap();
        assert!(du_count() >= before + 1.0);
    }

    // fs.go:69-71
    #[test]
    fn disk_usage_rejects_empty_path() {
        assert_eq!(disk_usage("").unwrap_err().to_string(), "invalid directory");
    }
}
