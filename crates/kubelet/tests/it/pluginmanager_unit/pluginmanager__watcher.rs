//! Unit tests moved from `src/pluginmanager/watcher.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use futures_util::StreamExt;
    pub use inotify::{Inotify, WatchDescriptor, WatchMask, Watches};
    pub use rusternetes_kubelet::pluginmanager::cache::DesiredStateOfWorld;
    pub use std::collections::HashMap;
    pub use std::os::unix::fs::FileTypeExt;
    pub use std::path::{Path, PathBuf};
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::pluginmanager::watcher::*;
    use std::os::unix::net::UnixListener;
    use std::time::Duration;

    /// `waitForRegistration` / `waitForUnregistration`
    /// (`plugin_watcher_test.go`): poll the desired state until `cond` holds.
    async fn wait_for(what: &str, cond: impl Fn() -> bool) {
        for _ in 0..200 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for {what}");
    }

    /// `newWatcher` (`plugin_watcher_test.go`): start a watcher on `dir`.
    fn start(dir: &Path) -> (Arc<DesiredStateOfWorld>, tokio::sync::watch::Sender<bool>) {
        let dsw = Arc::new(DesiredStateOfWorld::new());
        let (tx, rx) = tokio::sync::watch::channel(false);
        let w = Arc::new(Watcher::new(dir, dsw.clone()));
        w.start(rx).unwrap();
        (dsw, tx)
    }

    /// `TestPluginRegistration`: a socket appearing lands in the desired state;
    /// removing it takes it out again.
    #[tokio::test]
    async fn socket_create_and_remove() {
        let dir = tempfile::tempdir().unwrap();
        let (dsw, _stop) = start(dir.path());
        for i in 0..10 {
            let sock = dir.path().join(format!("plugin-{i}.sock"));
            let key = sock.to_string_lossy().to_string();
            let l = UnixListener::bind(&sock).unwrap();
            wait_for("registration", || dsw.plugin_exists(&key)).await;
            assert_eq!(dsw.get_plugins_to_register().len(), 1);
            drop(l);
            std::fs::remove_file(&sock).unwrap();
            wait_for("unregistration", || !dsw.plugin_exists(&key)).await;
            assert!(dsw.get_plugins_to_register().is_empty());
        }
    }

    /// `TestPluginRegistrationSameName`: sockets with distinct paths all stay.
    #[tokio::test]
    async fn many_sockets_all_registered() {
        let dir = tempfile::tempdir().unwrap();
        let (dsw, _stop) = start(dir.path());
        let mut keep = vec![];
        for i in 0..10 {
            let sock = dir.path().join(format!("plugin-{i}.sock"));
            keep.push(UnixListener::bind(&sock).unwrap());
            let key = sock.to_string_lossy().to_string();
            wait_for("registration", || dsw.plugin_exists(&key)).await;
            assert_eq!(dsw.get_plugins_to_register().len(), i + 1);
        }
    }

    /// `handleCreateEvent`: dot-files and non-sockets are ignored.
    #[tokio::test]
    async fn ignores_dotfiles_and_regular_files() {
        let dir = tempfile::tempdir().unwrap();
        let (dsw, _stop) = start(dir.path());
        let _l = UnixListener::bind(dir.path().join(".hidden.sock")).unwrap();
        std::fs::write(dir.path().join("regular"), b"x").unwrap();
        // A real socket afterwards proves earlier events were processed
        // (inotify delivers in order) without a fixed sleep.
        let ok = dir.path().join("ok.sock");
        let _l2 = UnixListener::bind(&ok).unwrap();
        wait_for("ok.sock", || dsw.plugin_exists(&ok.to_string_lossy())).await;
        assert_eq!(dsw.get_plugins_to_register().len(), 1);
    }

    /// `TestPluginRegistrationAtKubeletStart`: sockets already present when the
    /// watcher starts (including in sub-directories) are discovered.
    #[tokio::test]
    async fn existing_sockets_found_at_start() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        let a = dir.path().join("a.sock");
        let b = dir.path().join("sub/b.sock");
        let _la = UnixListener::bind(&a).unwrap();
        let _lb = UnixListener::bind(&b).unwrap();
        let (dsw, _stop) = start(dir.path());
        assert!(dsw.plugin_exists(&a.to_string_lossy()));
        assert!(dsw.plugin_exists(&b.to_string_lossy()));
    }

    /// `handleCreateEvent` on a directory -> `traversePluginDir`: a
    /// sub-directory made after start is watched and a socket made in it is
    /// registered.
    #[tokio::test]
    async fn socket_in_new_subdirectory_is_registered() {
        let dir = tempfile::tempdir().unwrap();
        let (dsw, _stop) = start(dir.path());
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        let sock = sub.join("p.sock");
        let _l = UnixListener::bind(&sock).unwrap();
        wait_for("sub socket", || dsw.plugin_exists(&sock.to_string_lossy())).await;
    }

    /// `TestPluginReRegistration`: a socket re-created at the same path with
    /// NO delay between remove and bind is seen as Remove+Create events, so
    /// the desired state's UUID and timestamp are swapped. A stat-identity
    /// poll cannot see this (freed inode reused, same ctime tick: #2429).
    #[tokio::test]
    async fn recreated_socket_changes_uuid() {
        let dir = tempfile::tempdir().unwrap();
        let (dsw, _stop) = start(dir.path());
        let sock = dir.path().join("p.sock");
        let key = sock.to_string_lossy().to_string();
        let mut l = UnixListener::bind(&sock).unwrap();
        wait_for("registration", || dsw.plugin_exists(&key)).await;
        let mut last = dsw.get_plugins_to_register()[0].clone();
        for _ in 0..10 {
            drop(l);
            std::fs::remove_file(&sock).unwrap();
            l = UnixListener::bind(&sock).unwrap();
            let prev = last.uuid.clone();
            wait_for("re-registration", || {
                let p = dsw.get_plugins_to_register();
                p.len() == 1 && p[0].uuid != prev
            })
            .await;
            let now = dsw.get_plugins_to_register()[0].clone();
            assert!(now.timestamp > last.timestamp);
            last = now;
        }
    }

    /// fsnotify delivers Create/Remove immediately; a poll cannot (#2429).
    /// Registration and removal must land well inside the old 200ms poll tick.
    #[tokio::test]
    async fn events_are_delivered_without_polling_delay() {
        let dir = tempfile::tempdir().unwrap();
        let (dsw, _stop) = start(dir.path());
        let sock = dir.path().join("p.sock");
        let key = sock.to_string_lossy().to_string();
        let l = UnixListener::bind(&sock).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(dsw.plugin_exists(&key), "Create not seen within 100ms");
        drop(l);
        std::fs::remove_file(&sock).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!dsw.plugin_exists(&key), "Remove not seen within 100ms");
    }

    /// A stable socket must not churn the UUID (no events, no re-register).
    #[tokio::test]
    async fn stable_socket_keeps_uuid() {
        let dir = tempfile::tempdir().unwrap();
        let (dsw, _stop) = start(dir.path());
        let sock = dir.path().join("p.sock");
        let _l = UnixListener::bind(&sock).unwrap();
        wait_for("registration", || {
            dsw.plugin_exists(&sock.to_string_lossy())
        })
        .await;
        let first = dsw.get_plugins_to_register()[0].uuid.clone();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(dsw.get_plugins_to_register()[0].uuid, first);
    }
}
