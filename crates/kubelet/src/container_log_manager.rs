//! Container log rotation, ported from
//! `pkg/kubelet/logs/container_log_manager.go` (and
//! `container_log_manager_stub.go`).
//!
//! The manager lists the runtime's containers every `containerLogMonitorInterval`
//! (CRI `ListContainers`), queues every RUNNING container, and
//! `containerLogMaxWorkers` workers drain the queue: `ContainerStatus` for the
//! log path, `Stat` the file, and when it has reached `containerLogMaxSize`
//! clean up unused/excess rotated files, gzip the older ones, rename the latest
//! log to `<log>.<YYYYMMDD-HHMMSS>` and ask the runtime to reopen the log via
//! CRI `ReopenContainerLog`.
//!
//! Deviations from upstream (deliberate, Rust-idiomatic or scoped):
//! * `kubecontainer.OSInterface` is not modelled; `std::fs` is used directly.
//! * The workqueue is a small port of client-go's `Type` queue semantics
//!   (dirty/processing sets, FIFO, `Add`/`Get`/`Done`/`ShutDown`). Upstream
//!   also wraps it in a rate limiter, but only ever calls `Forget` on it
//!   (never `AddRateLimited`), so the limiter has no observable effect and is
//!   not ported.
//! * `filepath.Glob` is replaced by a directory scan matching the literal
//!   `<log>.*` / `<log>*` prefix; glob metacharacters in the log path are not
//!   interpreted.

use std::collections::{HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use k3rs_common::quantity::Quantity;
use k3rs_cri::{v1, CriClient};
use tokio::sync::{Mutex, Notify};
use tracing::{debug, error, info};

/// `timestampFormat`: Go layout `20060102-150405`.
pub const TIMESTAMP_FORMAT: &str = "%Y%m%d-%H%M%S";
/// `compressSuffix`: the suffix for compressed log.
pub const COMPRESS_SUFFIX: &str = ".gz";
/// `tmpSuffix`: the suffix for temporary file.
pub const TMP_SUFFIX: &str = ".tmp";

/// `ContainerLogManager`: manages the lifecycle of all container logs.
#[async_trait]
pub trait ContainerLogManager: Send + Sync {
    /// Start the container log manager.
    fn start(self: Arc<Self>);
    /// Clean removes all logs of the specified container.
    async fn clean(&self, container_id: &str) -> Result<()>;
}

/// The `KubeletConfiguration` fields the manager is built from
/// (`containerLogMaxSize`, `containerLogMaxFiles`, `containerLogMaxWorkers`,
/// `containerLogMonitorInterval`; `kubelet.go:714-721`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerLogConfig {
    pub max_size: String,
    pub max_files: i32,
    pub max_workers: i32,
    pub monitor_interval: Duration,
}

impl Default for ContainerLogConfig {
    /// The v1beta1 defaults (`defaults.go:257-267`): 10Mi, 5 files, 1 worker, 10s.
    fn default() -> Self {
        Self {
            max_size: "10Mi".to_string(),
            max_files: 5,
            max_workers: 1,
            monitor_interval: Duration::from_secs(10),
        }
    }
}

/// `LogRotatePolicy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogRotatePolicy {
    /// Size in bytes at which the log is rotated. Negative disables rotation.
    pub max_size: i64,
    /// Maximum number of log files that can be present. If rotating the logs
    /// creates excess files, the oldest file is removed.
    pub max_files: i64,
}

/// `GetAllLogs`: all in-use (rotated/compressed) logs for a specific container
/// log, sorted oldest to newest, followed by the latest `log` itself.
pub fn get_all_logs(log: &str) -> Result<Vec<String>> {
    let pattern = format!("{log}.*");
    let logs = glob_prefix(log, true)
        .with_context(|| format!("failed to list all log files with pattern {pattern:?}"))?;
    let (mut inuse, _) = filter_unused_logs(&logs);
    inuse.sort();
    inuse.push(log.to_string());
    Ok(inuse)
}

/// `parseMaxSize`: quantity string to an int64 byte count (`AsInt64`).
pub fn parse_max_size(size: &str) -> Result<i64> {
    let q = Quantity::parse(size).map_err(|e| anyhow!("{e}"))?;
    let v = q.value();
    if !q.is_integer() || v < i64::MIN as i128 || v > i64::MAX as i128 {
        bail!("invalid max log size");
    }
    Ok(v as i64)
}

/// `NewContainerLogManager`. A negative max size disables rotation and yields
/// the stub manager.
pub fn new_container_log_manager(
    cri: CriClient,
    cfg: &ContainerLogConfig,
) -> Result<Arc<dyn ContainerLogManager>> {
    if cfg.max_files <= 1 {
        bail!("invalid MaxFiles {}, must be > 1", cfg.max_files);
    }
    let parsed = parse_max_size(&cfg.max_size)
        .with_context(|| format!("failed to parse container log max size {:?}", cfg.max_size))?;
    // Negative number means to disable container log rotation.
    if parsed < 0 {
        return Ok(Arc::new(StubContainerLogManager));
    }
    Ok(Arc::new(RotatingContainerLogManager::new(
        cri,
        LogRotatePolicy {
            max_size: parsed,
            max_files: cfg.max_files as i64,
        },
        cfg.max_workers.max(0) as usize,
        cfg.monitor_interval,
    )))
}

/// `containerLogManagerStub` / `NewStubContainerLogManager`.
pub struct StubContainerLogManager;

#[async_trait]
impl ContainerLogManager for StubContainerLogManager {
    fn start(self: Arc<Self>) {}
    async fn clean(&self, _container_id: &str) -> Result<()> {
        Ok(())
    }
}

// ---- workqueue (client-go util/workqueue `Type`) ---------------------------

#[derive(Default)]
struct QueueState {
    queue: VecDeque<String>,
    dirty: HashSet<String>,
    processing: HashSet<String>,
    shutting_down: bool,
}

/// Port of client-go's `workqueue.Type`: an item added while already queued is
/// deduplicated, and one added while being processed is re-queued on `done`.
#[derive(Default)]
pub struct WorkQueue {
    state: StdMutex<QueueState>,
    notify: Notify,
}

impl WorkQueue {
    pub fn add(&self, item: &str) {
        let mut s = self.state.lock().unwrap();
        if s.shutting_down || s.dirty.contains(item) {
            return;
        }
        s.dirty.insert(item.to_string());
        if s.processing.contains(item) {
            return;
        }
        s.queue.push_back(item.to_string());
        drop(s);
        self.notify.notify_one();
    }

    pub fn len(&self) -> usize {
        self.state.lock().unwrap().queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Blocks until an item is available; `None` once shut down and drained.
    pub async fn get(&self) -> Option<String> {
        loop {
            let notified = self.notify.notified();
            {
                let mut s = self.state.lock().unwrap();
                if let Some(item) = s.queue.pop_front() {
                    s.processing.insert(item.clone());
                    s.dirty.remove(&item);
                    return Some(item);
                }
                if s.shutting_down {
                    return None;
                }
            }
            notified.await;
        }
    }

    pub fn done(&self, item: &str) {
        let mut s = self.state.lock().unwrap();
        s.processing.remove(item);
        if s.dirty.contains(item) {
            s.queue.push_back(item.to_string());
            drop(s);
            self.notify.notify_one();
        } else if s.processing.is_empty() {
            drop(s);
            self.notify.notify_waiters();
        }
    }

    pub fn shut_down(&self) {
        self.state.lock().unwrap().shutting_down = true;
        self.notify.notify_waiters();
        // `notify_waiters` does not store a permit; wake any worker that is
        // between its check and its await too.
        self.notify.notify_one();
    }
}

// ---- manager ---------------------------------------------------------------

/// `containerLogManager`.
pub struct RotatingContainerLogManager {
    cri: CriClient,
    policy: LogRotatePolicy,
    clock: Box<dyn Fn() -> chrono::DateTime<chrono::Local> + Send + Sync>,
    mutex: Mutex<()>,
    pub queue: WorkQueue,
    max_workers: usize,
    monitoring_period: Duration,
}

impl RotatingContainerLogManager {
    pub fn new(
        cri: CriClient,
        policy: LogRotatePolicy,
        max_workers: usize,
        monitoring_period: Duration,
    ) -> Self {
        Self::with_clock(
            cri,
            policy,
            max_workers,
            monitoring_period,
            Box::new(chrono::Local::now),
        )
    }

    /// As [`Self::new`] with an injectable clock (upstream's `clock.Clock`).
    pub fn with_clock(
        cri: CriClient,
        policy: LogRotatePolicy,
        max_workers: usize,
        monitoring_period: Duration,
        clock: Box<dyn Fn() -> chrono::DateTime<chrono::Local> + Send + Sync>,
    ) -> Self {
        Self {
            cri,
            policy,
            clock,
            mutex: Mutex::new(()),
            queue: WorkQueue::default(),
            max_workers,
            monitoring_period,
        }
    }

    pub fn policy(&self) -> LogRotatePolicy {
        self.policy
    }

    pub fn max_workers(&self) -> usize {
        self.max_workers
    }

    pub fn monitoring_period(&self) -> Duration {
        self.monitoring_period
    }

    /// `processQueueItems`: one worker, until the queue is shut down.
    pub async fn process_queue_items(&self, worker: usize) {
        debug!(worker, "Starting container log rotation worker");
        while self.process_container(worker).await {}
        debug!(worker, "Terminating container log rotation worker");
    }

    /// `rotateLogs`: queue every RUNNING container for rotation.
    pub async fn rotate_logs(&self) -> Result<()> {
        let _guard = self.mutex.lock().await;
        debug!("Starting container log rotation sequence");
        let containers = self
            .cri
            .clone()
            .list_containers(Some(v1::ContainerFilter::default()))
            .await
            .context("failed to list containers")?;
        for container in containers {
            // Only rotate logs for running containers. Non-running containers
            // won't generate new output, it doesn't make sense to keep an empty
            // latest log.
            if container.state != v1::ContainerState::ContainerRunning as i32 {
                continue;
            }
            debug!(id = %container.id, "Adding new entry to the queue for processing");
            self.queue.add(&container.id);
        }
        Ok(())
    }

    /// `processContainer`: returns false when the queue was shut down.
    async fn process_container(&self, worker: usize) -> bool {
        let Some(id) = self.queue.get().await else {
            return false;
        };
        self.process_one(worker, &id).await;
        // `defer { queue.Done(key); queue.Forget(key) }`
        self.queue.done(&id);
        true
    }

    async fn process_one(&self, worker: usize, id: &str) {
        let resp = match self.cri.clone().container_status(id, false).await {
            Ok(r) => r,
            Err(err) => {
                error!(worker, container_id = %id, %err, "Failed to get container status");
                return;
            }
        };
        let Some(status) = resp.status else {
            error!(worker, container_id = %id, "Container status is nil");
            return;
        };
        let path = status.log_path;
        let size = match fs::metadata(&path) {
            Ok(m) => m.len() as i64,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                if let Err(err) = self.cri.clone().reopen_container_log(id).await {
                    error!(worker, container_id = %id, %path, %err,
                        "Container log doesn't exist, reopen container log failed");
                    return;
                }
                match fs::metadata(&path) {
                    Ok(m) => m.len() as i64,
                    Err(err) => {
                        error!(worker, container_id = %id, %path, %err,
                            "Failed to stat container log after reopen");
                        return;
                    }
                }
            }
            Err(err) => {
                error!(worker, container_id = %id, %path, %err, "Failed to stat container log");
                return;
            }
        };
        if size < self.policy.max_size {
            debug!(worker, container_id = %id, %path, size, max_size = self.policy.max_size,
                "log file doesn't need to be rotated");
            return;
        }
        if let Err(err) = self.rotate_log(id, &path).await {
            error!(worker, container_id = %id, %path, size, max_size = self.policy.max_size,
                err = %format!("{err:#}"), "Failed to rotate log for container");
        }
    }

    /// `rotateLog`.
    pub async fn rotate_log(&self, id: &str, log: &str) -> Result<()> {
        let pattern = format!("{log}.*");
        let logs = glob_prefix(log, true)
            .with_context(|| format!("failed to list all log files with pattern {pattern:?}"))?;
        let logs = self
            .cleanup_unused_logs(logs)
            .context("failed to cleanup logs")?;
        let logs = self
            .remove_excess_logs(logs)
            .context("failed to remove excess logs")?;
        // Compress uncompressed log files.
        for l in logs {
            if l.ends_with(COMPRESS_SUFFIX) {
                continue;
            }
            let target = l.clone();
            tokio::task::spawn_blocking(move || compress_log(&target))
                .await
                .map_err(|e| anyhow!("compress task failed: {e}"))?
                .with_context(|| format!("failed to compress log {l:?}"))?;
        }
        self.rotate_latest_log(id, log)
            .await
            .with_context(|| format!("failed to rotate log {log:?}"))
    }

    /// `cleanupUnusedLogs`: remove temporary or unused log files left behind by
    /// a previous rotation failure; returns the in-use logs.
    pub fn cleanup_unused_logs(&self, logs: Vec<String>) -> Result<Vec<String>> {
        let (inuse, unused) = filter_unused_logs(&logs);
        for l in unused {
            fs::remove_file(&l).with_context(|| format!("failed to remove unused log {l:?}"))?;
        }
        Ok(inuse)
    }

    /// `removeExcessLogs`: remove old logs so at most `MaxFiles` remain.
    pub fn remove_excess_logs(&self, mut logs: Vec<String>) -> Result<Vec<String>> {
        // Sort log files in oldest to newest order.
        logs.sort();
        // Container will create a new log file, and we'll rotate the latest log
        // file. Other than those 2 files, we can have at most MaxFiles-2
        // rotated log files. Remove from oldest to newest, so as not to break
        // ongoing `kubectl logs`.
        let max_rotated = (self.policy.max_files - 2).max(0) as usize;
        let remove = logs.len().saturating_sub(max_rotated);
        for l in &logs[..remove] {
            fs::remove_file(l).with_context(|| format!("failed to remove old log {l:?}"))?;
        }
        Ok(logs.split_off(remove))
    }

    /// `rotateLatestLog`: rotate the latest log without compression, so that
    /// the container can still write and fluentd can finish reading.
    pub async fn rotate_latest_log(&self, id: &str, log: &str) -> Result<()> {
        let timestamp = (self.clock)().format(TIMESTAMP_FORMAT).to_string();
        let rotated = format!("{log}.{timestamp}");
        fs::rename(log, &rotated)
            .with_context(|| format!("failed to rotate log {log:?} to {rotated:?}"))?;
        if let Err(err) = self.cri.clone().reopen_container_log(id).await {
            // Rename the rotated log back, so that we can try rotating it again
            // next round. If kubelet gets restarted at this point, we'll lose
            // the original log.
            if let Err(rename_err) = fs::rename(&rotated, log) {
                // This shouldn't happen. Report an error if this happens,
                // because we will lose the original log.
                error!(rotated_log = %rotated, new_log = %log, container_id = %id,
                    err = %rename_err, "Failed to rename rotated log");
            }
            return Err(anyhow!(err))
                .with_context(|| format!("failed to reopen container log {id:?}"));
        }
        Ok(())
    }
}

#[async_trait]
impl ContainerLogManager for RotatingContainerLogManager {
    /// `Start`: spawn `maxWorkers` queue workers and the periodic rotation loop
    /// (`wait.Forever(rotateLogs, monitoringPeriod)`).
    fn start(self: Arc<Self>) {
        info!(workers = self.max_workers, monitor_period = ?self.monitoring_period,
            "Initializing container log rotate workers");
        for i in 0..self.max_workers {
            let m = Arc::clone(&self);
            tokio::spawn(async move { m.process_queue_items(i + 1).await });
        }
        let m = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                if let Err(err) = m.rotate_logs().await {
                    error!(err = %format!("{err:#}"), "Failed to rotate container logs");
                }
                tokio::time::sleep(m.monitoring_period).await;
            }
        });
    }

    /// `Clean`: remove all logs of the specified container (including rotated
    /// ones).
    async fn clean(&self, container_id: &str) -> Result<()> {
        let _guard = self.mutex.lock().await;
        let resp = self
            .cri
            .clone()
            .container_status(container_id, false)
            .await
            .with_context(|| format!("failed to get container status {container_id:?}"))?;
        let status = resp
            .status
            .ok_or_else(|| anyhow!("container status is nil for {container_id:?}"))?;
        // Deviation: upstream globs `*` for an empty LogPath, i.e. the kubelet's
        // working directory; a container without a log has nothing to clean.
        if status.log_path.is_empty() {
            return Ok(());
        }
        let pattern = format!("{}*", status.log_path);
        let logs = glob_prefix(&status.log_path, false)
            .with_context(|| format!("failed to list all log files with pattern {pattern:?}"))?;
        for l in logs {
            match fs::remove_file(&l) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(anyhow!(e)).with_context(|| {
                        format!("failed to remove container {container_id:?} log {l:?}")
                    })
                }
            }
        }
        Ok(())
    }
}

/// `filterUnusedLogs`: split logs into (in use, unused).
pub fn filter_unused_logs(logs: &[String]) -> (Vec<String>, Vec<String>) {
    let mut inuse = Vec::new();
    let mut unused = Vec::new();
    for l in logs {
        if is_in_use(l, logs) {
            inuse.push(l.clone());
        } else {
            unused.push(l.clone());
        }
    }
    (inuse, unused)
}

/// `isInUse`: whether a container log file is still in use.
pub fn is_in_use(l: &str, logs: &[String]) -> bool {
    // All temporary files are not in use.
    if l.ends_with(TMP_SUFFIX) {
        return false;
    }
    // All compressed logs are in use.
    if l.ends_with(COMPRESS_SUFFIX) {
        return true;
    }
    // Files that have already been compressed are not in use.
    let compressed = format!("{l}{COMPRESS_SUFFIX}");
    !logs.contains(&compressed)
}

/// `compressLog`: compress a log to `log.gz` with gzip.
pub fn compress_log(log: &str) -> Result<()> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::fs::PermissionsExt;

    let info = fs::metadata(log).context("failed to stat log file")?;
    let mut r = fs::File::open(log).with_context(|| format!("failed to open log {log:?}"))?;
    let tmp_log = format!("{log}{TMP_SUFFIX}");
    let f = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(info.permissions().mode())
        .open(&tmp_log)
        .with_context(|| format!("failed to create temporary log {tmp_log:?}"))?;
    let result = (|| -> Result<()> {
        let mut w = GzEncoder::new(f, Compression::default());
        io::copy(&mut r, &mut w)
            .with_context(|| format!("failed to compress {log:?} to {tmp_log:?}"))?;
        w.finish()
            .with_context(|| format!("failed to compress {log:?} to {tmp_log:?}"))?;
        let compressed = format!("{log}{COMPRESS_SUFFIX}");
        fs::rename(&tmp_log, &compressed)
            .with_context(|| format!("failed to rename {tmp_log:?} to {compressed:?}"))?;
        // Remove old log file.
        drop(r);
        fs::remove_file(log)
            .with_context(|| format!("failed to remove log {log:?} after compress"))?;
        Ok(())
    })();
    // Best effort cleanup of tmp_log.
    let _ = fs::remove_file(&tmp_log);
    result
}

/// The `filepath.Glob` of `<log>.*` (`dot = true`) or `<log>*` (`dot = false`):
/// entries of the log's directory whose name starts with `<base>.` / `<base>`,
/// sorted by name.
fn glob_prefix(log: &str, dot: bool) -> io::Result<Vec<String>> {
    let path = Path::new(log);
    let base = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let prefix = if dot { format!("{base}.") } else { base };
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty());
    let entries = match fs::read_dir(dir.unwrap_or_else(|| Path::new("."))) {
        Ok(e) => e,
        // Glob swallows I/O errors on a missing directory.
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out: Vec<String> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(&prefix))
        .map(|n| match dir {
            Some(d) => d.join(n).to_string_lossy().into_owned(),
            None => n,
        })
        .collect();
    out.sort();
    Ok(out)
}
