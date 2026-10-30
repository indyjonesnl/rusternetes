//! Unit tests moved from `src/audit_rotate.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
    pub use std::fs::{self, File, OpenOptions};
    pub use std::io::{self, Write};
    pub use std::path::{Path, PathBuf};
    pub use std::sync::Arc;
    pub use tokio::sync::Mutex;
    pub use tracing::info;
}

mod tests {
    use super::__parent::*;
    use chrono::Duration;
    use rusternetes_common::audit_rotate::*;

    fn touch(dir: &Path, name: &str) {
        fs::write(dir.join(name), b"old").unwrap();
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn backup_name_matches_lumberjack_format() {
        let t = Utc.with_ymd_and_hms(2016, 11, 4, 18, 30, 0).unwrap();
        // lumberjack.go type doc: server.log -> server-2016-11-04T18-30-00.000.log
        assert_eq!(
            backup_name(Path::new("/var/log/foo/server.log"), t),
            PathBuf::from("/var/log/foo/server-2016-11-04T18-30-00.000.log")
        );
    }

    #[test]
    fn rotates_when_write_would_exceed_max_size() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("audit.log");
        let mut w = RotatingFile::open(
            &p,
            RotationOptions {
                max_size_mb: 1,
                ..Default::default()
            },
        )
        .unwrap();
        w.megabyte = 10; // upstream tests mock `megabyte` the same way
        w.write(b"12345").unwrap();
        w.write(b"67890").unwrap(); // exactly full: no rotation
        assert_eq!(names(d.path()), vec!["audit.log"]);
        w.write(b"x").unwrap(); // would exceed: rotates
        let n = names(d.path());
        assert_eq!(n.len(), 2, "{n:?}");
        assert_eq!(fs::read(&p).unwrap(), b"x");
        let backup = n.iter().find(|s| *s != "audit.log").unwrap();
        assert_eq!(fs::read(d.path().join(backup)).unwrap(), b"1234567890");
    }

    #[test]
    fn write_longer_than_max_is_rejected() {
        let d = tempfile::tempdir().unwrap();
        let mut w = RotatingFile::open(
            d.path().join("a.log"),
            RotationOptions {
                max_size_mb: 1,
                ..Default::default()
            },
        )
        .unwrap();
        w.megabyte = 4;
        let e = w.write(b"12345").unwrap_err();
        assert!(e.to_string().contains("exceeds maximum file size 4"), "{e}");
    }

    #[test]
    fn existing_file_is_appended_to_until_full() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("a.log");
        fs::write(&p, b"abc").unwrap();
        let mut w = RotatingFile::open(&p, RotationOptions::default()).unwrap();
        w.write(b"def").unwrap();
        assert_eq!(fs::read(&p).unwrap(), b"abcdef");
    }

    #[test]
    fn max_backups_keeps_newest_and_counts_compressed_once() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("audit.log");
        touch(d.path(), "audit-2020-01-01T00-00-00.000.log");
        touch(d.path(), "audit-2020-01-02T00-00-00.000.log.gz");
        touch(d.path(), "audit-2020-01-03T00-00-00.000.log");
        touch(d.path(), "audit-2020-01-03T00-00-00.000.log.gz");
        touch(d.path(), "unrelated.log");
        let opts = RotationOptions {
            max_backups: 2,
            ..Default::default()
        };
        mill_run_once(&p, &opts, Utc::now()).unwrap();
        assert_eq!(
            names(d.path()),
            vec![
                "audit-2020-01-02T00-00-00.000.log.gz",
                "audit-2020-01-03T00-00-00.000.log",
                "audit-2020-01-03T00-00-00.000.log.gz",
                "unrelated.log"
            ]
        );
    }

    #[test]
    fn max_age_removes_old_backups() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("audit.log");
        let now = Utc.with_ymd_and_hms(2020, 1, 10, 0, 0, 0).unwrap();
        let fresh = format!(
            "audit-{}.log",
            (now - Duration::days(1)).format(BACKUP_TIME_FORMAT)
        );
        let stale = format!(
            "audit-{}.log",
            (now - Duration::days(5)).format(BACKUP_TIME_FORMAT)
        );
        touch(d.path(), &fresh);
        touch(d.path(), &stale);
        let opts = RotationOptions {
            max_age_days: 3,
            ..Default::default()
        };
        mill_run_once(&p, &opts, now).unwrap();
        assert_eq!(names(d.path()), vec![fresh]);
    }

    #[test]
    fn compress_gzips_backups_and_removes_originals() {
        use std::io::Read;
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("audit.log");
        touch(d.path(), "audit-2020-01-01T00-00-00.000.log");
        let opts = RotationOptions {
            compress: true,
            ..Default::default()
        };
        mill_run_once(&p, &opts, Utc::now()).unwrap();
        assert_eq!(
            names(d.path()),
            vec!["audit-2020-01-01T00-00-00.000.log.gz"]
        );
        let f = File::open(d.path().join("audit-2020-01-01T00-00-00.000.log.gz")).unwrap();
        let mut s = String::new();
        flate2::read::GzDecoder::new(f)
            .read_to_string(&mut s)
            .unwrap();
        assert_eq!(s, "old");
    }
}
