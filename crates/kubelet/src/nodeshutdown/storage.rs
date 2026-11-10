//! Port of `pkg/kubelet/nodeshutdown/storage.go`.

use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// `storage` interface (storage.go:28-31). Upstream's `Store`/`Load` take
/// `interface{}`; the only value ever passed is [`State`], so the Rust
/// interface is concrete over it.
pub trait Storage: Send + Sync {
    fn store(&self, data: &State) -> std::io::Result<()>;
    fn load(&self) -> std::io::Result<State>;
}

/// `localStorage` (storage.go:33-35).
pub struct LocalStorage {
    pub path: PathBuf,
}

impl Storage for LocalStorage {
    /// `localStorage.Store` (storage.go:37-43).
    fn store(&self, data: &State) -> std::io::Result<()> {
        let b = serde_json::to_vec(data).map_err(std::io::Error::other)?;
        atomic_write(&self.path, &b, 0o644)
    }

    /// `localStorage.Load` (storage.go:45-54): a missing file loads nothing
    /// and is not an error (the zero `state` is returned).
    fn load(&self) -> std::io::Result<State> {
        match std::fs::read(&self.path) {
            Ok(b) => serde_json::from_slice(&b).map_err(std::io::Error::other),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(State::default()),
            Err(e) => Err(e),
        }
    }
}

/// `timestamp` (storage.go:56-61): Unix seconds, 0 for the zero time.
pub fn timestamp(t: DateTime<Utc>) -> f64 {
    if is_zero(t) {
        return 0.0;
    }
    t.timestamp() as f64
}

/// `time.Time.IsZero`: January 1, year 1, 00:00:00 UTC.
pub fn is_zero(t: DateTime<Utc>) -> bool {
    t == zero_time()
}

/// Go's zero `time.Time`.
pub fn zero_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0).unwrap()
}

/// `state` (storage.go:63-66).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct State {
    #[serde(with = "go_time")]
    pub start_time: DateTime<Utc>,
    #[serde(with = "go_time")]
    pub end_time: DateTime<Utc>,
}

impl Default for State {
    fn default() -> Self {
        State {
            start_time: zero_time(),
            end_time: zero_time(),
        }
    }
}

/// `time.Time` JSON: `RFC3339Nano` (trailing fractional zeros trimmed).
mod go_time {
    use super::*;

    pub fn serialize<S: Serializer>(t: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        let mut out = format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            t.year(),
            t.month(),
            t.day(),
            t.hour(),
            t.minute(),
            t.second()
        );
        let nanos = t.nanosecond() % 1_000_000_000;
        if nanos != 0 {
            let frac = format!("{nanos:09}");
            out.push('.');
            out.push_str(frac.trim_end_matches('0'));
        }
        out.push('Z');
        s.serialize_str(&out)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        let s = String::deserialize(d)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|t| t.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }
}

/// `atomicWrite` (storage.go:69-95): write a temp file next to `filename`,
/// chmod, fsync, then rename over it.
fn atomic_write(filename: &Path, data: &[u8], perm: u32) -> std::io::Result<()> {
    let dir = filename.parent().unwrap_or_else(|| Path::new("."));
    let base = filename
        .file_name()
        .map(|b| b.to_string_lossy().into_owned())
        .unwrap_or_default();
    // os.CreateTemp(dir, ".tmp-"+base): pattern + random suffix, O_EXCL.
    let mut attempt = 0u32;
    let (tmp, mut f) = loop {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let tmp = dir.join(format!(".tmp-{base}{}{nanos}{attempt}", std::process::id()));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)
        {
            Ok(f) => break (tmp, f),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && attempt < 10_000 => {
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    };
    let res = (|| {
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(perm))?;
        f.write_all(data)?;
        f.sync_all()
    })();
    drop(f);
    match res.and_then(|_| std::fs::rename(&tmp, filename)) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}
