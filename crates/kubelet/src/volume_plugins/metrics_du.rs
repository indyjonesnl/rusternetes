//! Port of `pkg/volume/metrics_du.go` (`metricsDu`) and `DiskUsage`
//! (`pkg/volume/util/fs/fs.go:65-142`, linux).
//!
//! `DiskUsage` asks the quota system first (`fsquota.GetConsumption` /
//! `GetInodes`, `fs.go:77,85`; see [`crate::fsquota`], gated by
//! `LocalStorageCapacityIsolationFSQuotaMonitoring`). It observes
//! `servermetrics.CollectVolumeStatCalDuration` under `fsquota` when the
//! quota system knows the path and `du` otherwise (`fs.go:80-85`); a
//! `None`/error answer (path does not support quotas, or the `xfs_quota` tool
//! is missing) means "use other mechanisms", i.e. the walk, which then only
//! counts the half the quota system did not answer (`fs.go:127-135`).

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

/// Which halves of the usage the walk counts: `consumption == nil` /
/// `inodes == nil` (`fs.go:127-135`).
#[derive(Clone, Copy)]
struct Counting {
    bytes: bool,
    inodes: bool,
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

    // fs.go:73-85: first check whether the quota system knows about this
    // directory. A nil quantity or error means that the path does not
    // support quotas or the xfs_quota tool is missing and we should use
    // other mechanisms.
    let start_time = Instant::now();
    #[cfg(target_os = "linux")]
    let (consumption, inodes) = (
        crate::fsquota::get_consumption(path).ok().flatten(),
        crate::fsquota::get_inodes(path).ok().flatten(),
    );
    #[cfg(not(target_os = "linux"))]
    let (consumption, inodes): (Option<i64>, Option<i64>) = (None, None);
    let source = if consumption.is_some() {
        "fsquota"
    } else {
        "du"
    };
    if let Some(c) = consumption {
        usage.bytes = c;
    }
    if let Some(i) = inodes {
        usage.inodes = i;
    }
    let counting = Counting {
        bytes: consumption.is_none(),
        inodes: inodes.is_none(),
    };
    let result = (|| {
        if inodes.is_some() && consumption.is_some() {
            return Ok(());
        }
        let top_level_dev = std::fs::metadata(path)?.dev();
        let mut deduped_inodes: HashSet<u64> = HashSet::new();
        let md = std::fs::symlink_metadata(path)?;
        walk(
            Path::new(path),
            &md,
            top_level_dev,
            &mut deduped_inodes,
            &mut usage,
            counting,
        )?;
        Ok(())
    })();
    // fs.go:80-84: `defer servermetrics.CollectVolumeStatCalDuration(..)`.
    collect_volume_stat_cal_duration(source, start_time);
    result.map(|()| usage)
}

/// The `filepath.Walk` callback of `fs.go:104-139` for one entry.
fn visit(
    path: &Path,
    md: &std::fs::Metadata,
    top_level_dev: u64,
    deduped: &mut HashSet<u64>,
    usage: &mut UsageInfo,
    counting: Counting,
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
    if counting.bytes {
        usage.bytes += md.blocks() as i64 * 512;
    }
    if counting.inodes {
        usage.inodes += 1;
    }
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
    counting: Counting,
) -> Result<Step> {
    let step = visit(path, md, top_level_dev, deduped, usage, counting);
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
        match walk(&child, &cmd, top_level_dev, deduped, usage, counting)? {
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
