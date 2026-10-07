//! Port of `pkg/kubelet/config/file_linux.go`: the inotify half of the static
//! pod file source. `sourceFile.run` (`file.go:92-117`) lists the manifest
//! directory every `fileCheckFrequency` AND consumes `watchEvents` produced by
//! `startWatch`, so a manifest change is seen immediately instead of up to a
//! poll period later. The poll stays as the safety net (and is the only path
//! when inotify is unavailable, e.g. the watch cannot be created).
//!
//! Deliberate deviation: upstream's `consumeWatchEvent` applies one file to
//! the `UndeltaStore` (`store.Add`/`store.Delete`, keyed through
//! `fileKeyMapping`) and republishes the whole set. We have no per-file store;
//! `Kubelet::poll_static_pods` re-reads the directory (`listConfig`), which
//! reaches the same final set, so an event simply triggers that pass.

use futures_util::StreamExt;
use inotify::{EventMask, Inotify, WatchMask};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// `eventBufferLen` (`file.go:44`).
pub const EVENT_BUFFER_LEN: usize = 10;
/// `retryPeriod` / `maxRetryPeriod` (`file_linux.go:36-37`).
const RETRY_PERIOD: Duration = Duration::from_secs(1);
const MAX_RETRY_PERIOD: Duration = Duration::from_secs(20);

/// `podEventType` (`file.go:46-52`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodEventType {
    Add,
    Modify,
    Delete,
}

/// `watchEvent` (`file.go:54-57`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchEvent {
    pub file_name: PathBuf,
    pub event_type: PodEventType,
}

/// `produceWatchEvent` (`file_linux.go:84-112`) with fsnotify's inotify mask
/// to Op mapping (`backend_inotify.go:520-545` `newEvent`): `IN_CREATE |
/// IN_MOVED_TO` Create, `IN_MODIFY` Write, `IN_ATTRIB` Chmod, `IN_DELETE |
/// IN_DELETE_SELF` Remove, `IN_MOVE_SELF | IN_MOVED_FROM` Rename. The switch
/// order is Create, Write, Chmod, Remove, Rename; names starting with '.' and
/// any other event are ignored.
pub fn produce_watch_event(name: &Path, mask: EventMask) -> Option<WatchEvent> {
    // "Ignore file start with dots"
    if name
        .file_name()
        .is_some_and(|n| n.to_string_lossy().starts_with('.'))
    {
        return None;
    }
    let event_type = if mask.intersects(EventMask::CREATE | EventMask::MOVED_TO) {
        PodEventType::Add
    } else if mask.contains(EventMask::MODIFY) || mask.contains(EventMask::ATTRIB) {
        PodEventType::Modify
    } else if mask.intersects(
        // Remove, then Rename: both are podDelete.
        EventMask::DELETE | EventMask::DELETE_SELF | EventMask::MOVE_SELF | EventMask::MOVED_FROM,
    ) {
        PodEventType::Delete
    } else {
        return None;
    };
    Some(WatchEvent {
        file_name: name.to_path_buf(),
        event_type,
    })
}

/// `flowcontrol.NewBackOff(retryPeriod, maxRetryPeriod)` restricted to the
/// calls `startWatch` makes (`Next`, `IsInBackOffSinceUpdate`;
/// client-go/util/flowcontrol/backoff.go:96-108,133-144). No jitter, as
/// `NewBackOff` uses none.
struct BackOff {
    initial: Duration,
    max: Duration,
    entry: Option<(Duration, Instant)>,
}

impl BackOff {
    fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            entry: None,
        }
    }
    fn has_expired(&self, now: Instant, last: Instant) -> bool {
        now.saturating_duration_since(last) > self.max * 2
    }
    fn next(&mut self, now: Instant) {
        self.entry = Some(match self.entry {
            Some((b, last)) if !self.has_expired(now, last) => ((b * 2).min(self.max), now),
            _ => (self.initial, now),
        });
    }
    fn is_in_backoff_since_update(&self, now: Instant) -> bool {
        match self.entry {
            Some((b, last)) if !self.has_expired(now, last) => {
                now.saturating_duration_since(last) < b
            }
            _ => false,
        }
    }
}

/// A `retryableError` (`file_linux.go:39-45`) is not backed off.
enum WatchError {
    Retryable(String),
    Other(String),
}

/// `startWatch` (`file_linux.go:47-65`): `wait.Forever(doWatch, retryPeriod)`
/// with a back-off on non-retryable errors. Returns only once the receiver
/// is gone.
pub async fn start_watch(path: PathBuf, tx: mpsc::Sender<WatchEvent>) {
    let mut back_off = BackOff::new(RETRY_PERIOD, MAX_RETRY_PERIOD);
    loop {
        if !back_off.is_in_backoff_since_update(Instant::now()) {
            match do_watch(&path, &tx).await {
                Ok(()) => {}
                Err(WatchError::Retryable(e)) => {
                    tracing::error!(path = %path.display(), "Unable to read config path: {e}");
                }
                Err(WatchError::Other(e)) => {
                    tracing::error!(path = %path.display(), "Unable to read config path: {e}");
                    back_off.next(Instant::now());
                }
            }
        }
        if tx.is_closed() {
            return;
        }
        tokio::time::sleep(RETRY_PERIOD).await;
    }
}

/// `doWatch` (`file_linux.go:67-96`).
async fn do_watch(path: &Path, tx: &mpsc::Sender<WatchEvent>) -> Result<(), WatchError> {
    if let Err(e) = std::fs::metadata(path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            return Err(WatchError::Other(e.to_string()));
        }
        return Err(WatchError::Retryable(
            "path does not exist, ignoring".into(),
        ));
    }
    let inotify =
        Inotify::init().map_err(|e| WatchError::Other(format!("unable to create inotify: {e}")))?;
    // fsnotify's mask for a path (`backend_inotify.go:197-209`).
    inotify
        .watches()
        .add(
            path,
            WatchMask::MOVED_TO
                | WatchMask::MOVED_FROM
                | WatchMask::MOVE_SELF
                | WatchMask::CREATE
                | WatchMask::ATTRIB
                | WatchMask::MODIFY
                | WatchMask::DELETE
                | WatchMask::DELETE_SELF,
        )
        .map_err(|e| {
            WatchError::Other(format!(
                "unable to create inotify for path {:?}: {e}",
                path.display()
            ))
        })?;
    let mut events = inotify
        .into_event_stream(vec![0u8; 4096])
        .map_err(|e| WatchError::Other(format!("unable to create inotify: {e}")))?;
    while let Some(ev) = events.next().await {
        let ev = ev.map_err(|e| {
            WatchError::Other(format!("error while watching {:?}: {e}", path.display()))
        })?;
        if ev.mask.contains(EventMask::Q_OVERFLOW) {
            return Err(WatchError::Other(format!(
                "error while watching {:?}: inotify queue overflow",
                path.display()
            )));
        }
        if ev.mask.contains(EventMask::IGNORED) {
            // The watch is gone (directory removed); re-establish via retry.
            return Err(WatchError::Retryable("watch removed".into()));
        }
        let name = match &ev.name {
            Some(n) => path.join(n),
            None => path.to_path_buf(),
        };
        if let Some(we) = produce_watch_event(&name, ev.mask) {
            if tx.send(we).await.is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
