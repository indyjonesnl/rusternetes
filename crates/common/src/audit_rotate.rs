//! Size/age/count-based log rotation for `--audit-log-path`.
//!
//! Port of `lumberjack.Logger` (gopkg.in/natefinch/lumberjack.v2 v2.2.1,
//! lumberjack.go: `Write`, `rotate`, `openNew`, `backupName`,
//! `openExistingOrNew`, `millRunOnce`, `oldLogFiles`, `compressLogFile`),
//! the writer the log backend uses (`AuditLogOptions.getWriter` /
//! `ensureLogFile`, k8s.io/apiserver/pkg/server/options/audit.go). The mill
//! goroutine is a detached thread here; `mill_run_once` is exposed so tests
//! are deterministic.

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

/// `backupTimeFormat` (lumberjack.go): `2006-01-02T15-04-05.000`.
pub const BACKUP_TIME_FORMAT: &str = "%Y-%m-%dT%H-%M-%S%.3f";
const COMPRESS_SUFFIX: &str = ".gz";
/// `defaultMaxSize` (lumberjack.go), megabytes.
const DEFAULT_MAX_SIZE_MB: u64 = 100;
const MEGABYTE: u64 = 1024 * 1024;

/// The `--audit-log-max*` / `--audit-log-compress` options
/// (`AuditLogOptions`, options/audit.go). Zero means lumberjack's default.
#[derive(Debug, Clone, Default)]
pub struct RotationOptions {
    /// `--audit-log-maxsize`: megabytes before rotation (0 = 100).
    pub max_size_mb: u64,
    /// `--audit-log-maxage`: days to keep rotated files (0 = forever).
    pub max_age_days: u64,
    /// `--audit-log-maxbackup`: rotated files to keep (0 = all).
    pub max_backups: usize,
    /// `--audit-log-compress`: gzip rotated files.
    pub compress: bool,
}

/// `lumberjack.Logger`.
pub struct RotatingFile {
    pub path: PathBuf,
    pub opts: RotationOptions,
    /// Test seam for lumberjack's `megabyte` variable.
    pub megabyte: u64,
    pub size: u64,
    pub file: Option<File>,
}

fn dir_of(path: &Path) -> &Path {
    path.parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

impl RotatingFile {
    /// `ensureLogFile` (options/audit.go): `MkdirAll(dir, 0700)` and create
    /// the file 0600 if missing.
    pub fn open(path: impl Into<PathBuf>, opts: RotationOptions) -> io::Result<Self> {
        let path = path.into();
        let dir = dir_of(&path);
        if !dir.exists() {
            fs::create_dir_all(dir)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
            }
        }
        let mut oo = OpenOptions::new();
        oo.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            oo.mode(0o600);
        }
        oo.open(&path)?;
        Ok(Self {
            path,
            opts,
            megabyte: MEGABYTE,
            size: 0,
            file: None,
        })
    }

    fn max(&self) -> u64 {
        let mb = if self.opts.max_size_mb == 0 {
            DEFAULT_MAX_SIZE_MB
        } else {
            self.opts.max_size_mb
        };
        mb * self.megabyte
    }

    /// `Logger.Write`.
    pub fn write(&mut self, p: &[u8]) -> io::Result<()> {
        let write_len = p.len() as u64;
        if write_len > self.max() {
            return Err(io::Error::other(format!(
                "write length {} exceeds maximum file size {}",
                write_len,
                self.max()
            )));
        }
        if self.file.is_none() {
            self.open_existing_or_new(write_len)?;
        }
        if self.size + write_len > self.max() {
            self.rotate()?;
        }
        let f = self.file.as_mut().expect("opened above");
        f.write_all(p)?;
        self.size += write_len;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        match self.file.as_mut() {
            Some(f) => f.flush(),
            None => Ok(()),
        }
    }

    /// `Logger.rotate`.
    pub fn rotate(&mut self) -> io::Result<()> {
        self.file = None;
        self.open_new()?;
        self.mill();
        Ok(())
    }

    /// `Logger.openNew`: move any existing file aside, create a fresh one
    /// with the old file's mode.
    fn open_new(&mut self) -> io::Result<()> {
        fs::create_dir_all(dir_of(&self.path)).map_err(|e| {
            io::Error::other(format!("can't make directories for new logfile: {e}"))
        })?;
        let mut mode: u32 = 0o600;
        if let Ok(meta) = fs::metadata(&self.path) {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                mode = meta.permissions().mode() & 0o7777;
            }
            #[cfg(not(unix))]
            let _ = &meta;
            fs::rename(&self.path, backup_name(&self.path, Utc::now()))
                .map_err(|e| io::Error::other(format!("can't rename log file: {e}")))?;
        }
        let mut oo = OpenOptions::new();
        oo.create(true).write(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            oo.mode(mode);
        }
        #[cfg(not(unix))]
        let _ = mode;
        self.file = Some(
            oo.open(&self.path)
                .map_err(|e| io::Error::other(format!("can't open new logfile: {e}")))?,
        );
        self.size = 0;
        Ok(())
    }

    /// `Logger.openExistingOrNew`.
    fn open_existing_or_new(&mut self, write_len: u64) -> io::Result<()> {
        self.mill();
        let meta = match fs::metadata(&self.path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return self.open_new(),
            Err(e) => {
                return Err(io::Error::other(format!(
                    "error getting log file info: {e}"
                )))
            }
        };
        if meta.len() + write_len >= self.max() {
            return self.rotate();
        }
        match OpenOptions::new().append(true).open(&self.path) {
            Ok(f) => {
                self.file = Some(f);
                self.size = meta.len();
                Ok(())
            }
            // lumberjack: if the old file can't be opened, start a new one.
            Err(_) => self.open_new(),
        }
    }

    /// `Logger.mill`: post-rotation cleanup off the write path. Upstream
    /// coalesces runs through a size-1 channel; a detached thread per
    /// rotation is equivalent enough because rotations are rare.
    fn mill(&self) {
        if self.opts.max_backups == 0 && self.opts.max_age_days == 0 && !self.opts.compress {
            return;
        }
        let (path, opts) = (self.path.clone(), self.opts.clone());
        std::thread::spawn(move || {
            let _ = mill_run_once(&path, &opts, Utc::now());
        });
    }
}

/// `prefixAndExt`: prefix includes the trailing `-`.
fn prefix_and_ext(name: &Path) -> (String, String) {
    let filename = name
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let ext = match filename.rfind('.') {
        Some(i) => filename[i..].to_string(),
        None => String::new(),
    };
    let stem = &filename[..filename.len() - ext.len()];
    (format!("{stem}-"), ext)
}

/// `backupName`: `<name>-<UTC timestamp><ext>`.
pub fn backup_name(name: &Path, now: DateTime<Utc>) -> PathBuf {
    let (prefix, ext) = prefix_and_ext(name);
    dir_of(name).join(format!(
        "{}{}{}",
        prefix,
        now.format(BACKUP_TIME_FORMAT),
        ext
    ))
}

/// `Logger.timeFromName`.
fn time_from_name(filename: &str, prefix: &str, ext: &str) -> Option<DateTime<Utc>> {
    let rest = filename.strip_prefix(prefix)?.strip_suffix(ext)?;
    let nd = NaiveDateTime::parse_from_str(rest, BACKUP_TIME_FORMAT).ok()?;
    Some(Utc.from_utc_datetime(&nd))
}

/// `Logger.oldLogFiles`: backups sorted newest first by encoded timestamp.
fn old_log_files(path: &Path) -> io::Result<Vec<(DateTime<Utc>, String)>> {
    let (prefix, ext) = prefix_and_ext(path);
    let gz_ext = format!("{ext}{COMPRESS_SUFFIX}");
    let mut out = Vec::new();
    for entry in fs::read_dir(dir_of(path))
        .map_err(|e| io::Error::other(format!("can't read log file directory: {e}")))?
    {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(t) =
            time_from_name(&name, &prefix, &ext).or_else(|| time_from_name(&name, &prefix, &gz_ext))
        {
            out.push((t, name));
        }
    }
    out.sort_by_key(|a| std::cmp::Reverse(a.0));
    Ok(out)
}

/// `Logger.millRunOnce`: enforce MaxBackups, then MaxAge, then compress
/// what is left.
pub fn mill_run_once(path: &Path, opts: &RotationOptions, now: DateTime<Utc>) -> io::Result<()> {
    if opts.max_backups == 0 && opts.max_age_days == 0 && !opts.compress {
        return Ok(());
    }
    let mut files = old_log_files(path)?;
    let dir = dir_of(path);
    let mut remove: Vec<String> = Vec::new();

    if opts.max_backups > 0 && opts.max_backups < files.len() {
        let mut preserved = std::collections::HashSet::new();
        let mut remaining = Vec::new();
        for f in files {
            // Count the plain and the compressed copy of a backup once.
            let key =
                f.1.strip_suffix(COMPRESS_SUFFIX)
                    .unwrap_or(&f.1)
                    .to_string();
            preserved.insert(key);
            if preserved.len() > opts.max_backups {
                remove.push(f.1);
            } else {
                remaining.push(f);
            }
        }
        files = remaining;
    }
    if opts.max_age_days > 0 {
        let cutoff = now - chrono::Duration::hours(24 * opts.max_age_days as i64);
        let mut remaining = Vec::new();
        for f in files {
            if f.0 < cutoff {
                remove.push(f.1);
            } else {
                remaining.push(f);
            }
        }
        files = remaining;
    }
    let compress: Vec<String> = if opts.compress {
        files
            .iter()
            .filter(|f| !f.1.ends_with(COMPRESS_SUFFIX))
            .map(|f| f.1.clone())
            .collect()
    } else {
        Vec::new()
    };

    let mut err: Option<io::Error> = None;
    for f in remove {
        if let Err(e) = fs::remove_file(dir.join(f)) {
            err.get_or_insert(e);
        }
    }
    for f in compress {
        let src = dir.join(&f);
        let dst = dir.join(format!("{f}{COMPRESS_SUFFIX}"));
        if let Err(e) = compress_log_file(&src, &dst) {
            err.get_or_insert(e);
        }
    }
    err.map_or(Ok(()), Err)
}

/// `compressLogFile`: gzip `src` into `dst` keeping its mode, then remove
/// `src`; a failed attempt removes the partial `dst`.
fn compress_log_file(src: &Path, dst: &Path) -> io::Result<()> {
    let result = (|| {
        let mut input = File::open(src)?;
        let meta = fs::metadata(src)?;
        let mut oo = OpenOptions::new();
        oo.create(true).write(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
            oo.mode(meta.permissions().mode() & 0o7777);
        }
        #[cfg(not(unix))]
        let _ = &meta;
        let out = oo.open(dst)?;
        let mut gz = flate2::write::GzEncoder::new(out, flate2::Compression::default());
        io::copy(&mut input, &mut gz)?;
        gz.finish()?.sync_all()?;
        drop(input);
        fs::remove_file(src)
    })();
    if let Err(e) = result {
        let _ = fs::remove_file(dst);
        return Err(io::Error::other(format!(
            "failed to compress log file: {e}"
        )));
    }
    Ok(())
}

/// Audit backend writing one JSON event per line through a [`RotatingFile`]
/// (plugin/pkg/audit/log backend over the lumberjack writer).
pub struct RotatingFileAuditBackend {
    inner: Arc<Mutex<RotatingFile>>,
}

impl RotatingFileAuditBackend {
    pub fn new(path: impl Into<PathBuf>, opts: RotationOptions) -> io::Result<Self> {
        let path = path.into();
        let w = RotatingFile::open(&path, opts)?;
        info!("Audit logging enabled: writing to {}", path.display());
        Ok(Self {
            inner: Arc::new(Mutex::new(w)),
        })
    }
}

#[async_trait::async_trait]
impl crate::audit::AuditBackend for RotatingFileAuditBackend {
    async fn log(&self, event: crate::audit::AuditEvent) -> Result<(), String> {
        let mut line =
            serde_json::to_vec(&event).map_err(|e| format!("Serialization error: {e}"))?;
        line.push(b'\n');
        self.inner
            .lock()
            .await
            .write(&line)
            .map_err(|e| format!("Write error: {e}"))
    }

    async fn flush(&self) -> Result<(), String> {
        self.inner
            .lock()
            .await
            .flush()
            .map_err(|e| format!("Flush error: {e}"))
    }
}
