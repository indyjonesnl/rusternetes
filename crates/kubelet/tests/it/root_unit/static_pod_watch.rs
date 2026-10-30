//! Unit tests moved from `src/static_pod_watch.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use futures_util::StreamExt;
    pub use inotify::{EventMask, Inotify, WatchMask};
    pub use std::path::{Path, PathBuf};
    pub use std::time::{Duration, Instant};
    pub use tokio::sync::mpsc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::static_pod_watch::*;

    #[test]
    fn dot_files_ignored_and_ops_mapped() {
        let m = |n: &str, mask| produce_watch_event(Path::new(n), mask).map(|e| e.event_type);
        assert_eq!(m("/d/.hidden", EventMask::CREATE), None);
        assert_eq!(m("/d/a.yaml", EventMask::CREATE), Some(PodEventType::Add));
        assert_eq!(m("/d/a.yaml", EventMask::MOVED_TO), Some(PodEventType::Add));
        assert_eq!(
            m("/d/a.yaml", EventMask::MODIFY),
            Some(PodEventType::Modify)
        );
        assert_eq!(
            m("/d/a.yaml", EventMask::ATTRIB),
            Some(PodEventType::Modify)
        );
        assert_eq!(
            m("/d/a.yaml", EventMask::DELETE),
            Some(PodEventType::Delete)
        );
        assert_eq!(
            m("/d/a.yaml", EventMask::MOVED_FROM),
            Some(PodEventType::Delete)
        );
        assert_eq!(m("/d/a.yaml", EventMask::OPEN), None);
        // Create wins over Write when both bits are set (switch order).
        assert_eq!(
            m("/d/a.yaml", EventMask::CREATE | EventMask::MODIFY),
            Some(PodEventType::Add)
        );
    }

    #[test]
    fn backoff_doubles_to_cap_and_expires() {
        let mut b = BackOff::new(Duration::from_secs(1), Duration::from_secs(20));
        let t0 = Instant::now();
        assert!(!b.is_in_backoff_since_update(t0));
        b.next(t0);
        assert!(b.is_in_backoff_since_update(t0));
        assert!(!b.is_in_backoff_since_update(t0 + Duration::from_secs(1)));
        for _ in 0..10 {
            b.next(t0);
        }
        assert_eq!(b.entry.unwrap().0, Duration::from_secs(20));
        assert!(!b.is_in_backoff_since_update(t0 + Duration::from_secs(41)));
    }

    #[tokio::test]
    async fn watch_delivers_file_events() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, mut rx) = mpsc::channel(EVENT_BUFFER_LEN);
        let h = tokio::spawn(start_watch(dir.path().to_path_buf(), tx));
        tokio::time::sleep(Duration::from_millis(300)).await;
        let f = dir.path().join("a.yaml");
        std::fs::write(&f, "x").unwrap();
        let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("event within 5s")
            .unwrap();
        assert_eq!(ev.file_name, f);
        assert_eq!(ev.event_type, PodEventType::Add);
        h.abort();
    }
}
