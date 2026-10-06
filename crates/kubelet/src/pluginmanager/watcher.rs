//! Port of `pkg/kubelet/pluginmanager/pluginwatcher/plugin_watcher.go`: watch
//! the plugin registration directory (`<root>/plugins_registry`) for unix
//! sockets and mirror them into the desired state of world.
//!
//! DEVIATION (deliberate): upstream subscribes to inotify through `fsnotify`.
//! No inotify binding is in this workspace's dependency set, so the directory
//! is polled instead and the same events are derived by diffing successive
//! scans: a socket that appears is a Create (`handleCreateEvent`), one that
//! disappears is a Remove (`handleDeleteEvent`), and one whose inode/ctime
//! changed between scans is a delete+create that raced the poll, which upstream
//! sees as two events and we collapse to a Create (`AddOrUpdatePlugin` swaps the
//! UUID, so the reconciler still re-registers). Tracked in the follow-up issue.

use super::cache::DesiredStateOfWorld;
use std::collections::HashMap;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How often the directory is rescanned (inotify is immediate upstream).
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Identity of a socket file: a re-created socket has a new inode and/or ctime.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct SocketId {
    ino: u64,
    ctime: i64,
    ctime_nsec: i64,
}

/// `Watcher` (`plugin_watcher.go:36`).
pub struct Watcher {
    path: PathBuf,
    desired_state_of_world: Arc<DesiredStateOfWorld>,
    known: Mutex<HashMap<PathBuf, SocketId>>,
}

impl Watcher {
    /// `NewWatcher` (`:44`).
    pub fn new(sock_dir: impl Into<PathBuf>, dsw: Arc<DesiredStateOfWorld>) -> Self {
        Self {
            path: sock_dir.into(),
            desired_state_of_world: dsw,
            known: Mutex::new(HashMap::new()),
        }
    }

    /// `Start` (`:53-100`): create the directory if needed (`init`), pick up
    /// plugins already present (`traversePluginDir` -> registration at kubelet
    /// start), then keep watching until `stop` fires.
    pub fn start(
        self: &Arc<Self>,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), String> {
        std::fs::create_dir_all(&self.path)
            .map_err(|e| format!("error (re-)creating root {}: {e}", self.path.display()))?;
        self.poll_once();
        let w = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(POLL_INTERVAL) => w.poll_once(),
                    _ = stop.changed() => return,
                }
            }
        });
        Ok(())
    }

    /// One scan of the registration directory, applying the create / delete
    /// events it implies to the desired state of world.
    pub fn poll_once(&self) {
        let mut found = HashMap::new();
        self.scan(&self.path, &mut found);

        let mut known = self.known.lock().unwrap();
        // handleDeleteEvent (`:206-212`): socket gone.
        let gone: Vec<PathBuf> = known
            .keys()
            .filter(|p| !found.contains_key(*p))
            .cloned()
            .collect();
        for p in gone {
            tracing::info!(path = %p.display(), "Removing socket path from desired state cache");
            self.desired_state_of_world
                .remove_plugin(&p.to_string_lossy());
            known.remove(&p);
        }
        // handleCreateEvent -> handlePluginRegistration (`:151-205`).
        for (p, id) in found {
            if known.get(&p) == Some(&id) {
                continue;
            }
            tracing::info!(path = %p.display(), "Adding socket path or updating timestamp to desired state cache");
            match self
                .desired_state_of_world
                .add_or_update_plugin(&p.to_string_lossy())
            {
                Ok(()) => {
                    known.insert(p, id);
                }
                Err(e) => {
                    tracing::error!(path = %p.display(), "Error when handling create event: {e}")
                }
            }
        }
    }

    /// `traversePluginDir` (`:116-155`): recurse into sub-directories; ignore
    /// names starting with '.' and anything that is not a unix socket; errors
    /// below the root are ignored.
    fn scan(&self, dir: &Path, out: &mut HashMap<PathBuf, SocketId>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            let path = entry.path();
            let Ok(md) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            if md.is_dir() {
                self.scan(&path, out);
            } else if md.file_type().is_socket() {
                out.insert(
                    path,
                    SocketId {
                        ino: md.ino(),
                        ctime: md.ctime(),
                        ctime_nsec: md.ctime_nsec(),
                    },
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    fn setup() -> (tempfile::TempDir, Arc<DesiredStateOfWorld>, Watcher) {
        let dir = tempfile::tempdir().unwrap();
        let dsw = Arc::new(DesiredStateOfWorld::new());
        let w = Watcher::new(dir.path(), dsw.clone());
        (dir, dsw, w)
    }

    /// `TestPluginRegistration`: a socket appearing lands in the desired state;
    /// removing it takes it out again.
    #[test]
    fn socket_create_and_remove() {
        let (dir, dsw, w) = setup();
        let sock = dir.path().join("plugin-0.sock");
        let l = UnixListener::bind(&sock).unwrap();
        w.poll_once();
        assert!(dsw.plugin_exists(&sock.to_string_lossy()));
        drop(l);
        std::fs::remove_file(&sock).unwrap();
        w.poll_once();
        assert!(!dsw.plugin_exists(&sock.to_string_lossy()));
    }

    /// `handleCreateEvent`: dot-files and non-sockets are ignored.
    #[test]
    fn ignores_dotfiles_and_regular_files() {
        let (dir, dsw, w) = setup();
        let _l = UnixListener::bind(dir.path().join(".hidden.sock")).unwrap();
        std::fs::write(dir.path().join("regular"), b"x").unwrap();
        w.poll_once();
        assert!(dsw.get_plugins_to_register().is_empty());
    }

    /// `TestPluginRegistrationAtKubeletStart`: sockets already present when the
    /// watcher starts (including in sub-directories) are discovered.
    #[test]
    fn existing_sockets_found_at_start() {
        let (dir, dsw, w) = setup();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        let a = dir.path().join("a.sock");
        let b = dir.path().join("sub/b.sock");
        let _la = UnixListener::bind(&a).unwrap();
        let _lb = UnixListener::bind(&b).unwrap();
        w.poll_once();
        assert!(dsw.plugin_exists(&a.to_string_lossy()));
        assert!(dsw.plugin_exists(&b.to_string_lossy()));
    }

    /// `TestPluginReRegistration`: a socket re-created at the same path between
    /// two scans swaps the desired state's UUID, which is what makes the
    /// reconciler unregister and re-register it.
    #[test]
    fn recreated_socket_changes_uuid() {
        let (dir, dsw, w) = setup();
        let sock = dir.path().join("p.sock");
        let l = UnixListener::bind(&sock).unwrap();
        w.poll_once();
        let first = dsw.get_plugins_to_register()[0].uuid.clone();
        drop(l);
        std::fs::remove_file(&sock).unwrap();
        let _l2 = UnixListener::bind(&sock).unwrap();
        w.poll_once();
        let second = dsw.get_plugins_to_register();
        assert_eq!(second.len(), 1);
        assert_ne!(second[0].uuid, first);
    }

    /// A stable socket must not churn the UUID between scans (else the plugin
    /// would be re-registered every poll).
    #[test]
    fn stable_socket_keeps_uuid() {
        let (dir, dsw, w) = setup();
        let _l = UnixListener::bind(dir.path().join("p.sock")).unwrap();
        w.poll_once();
        let first = dsw.get_plugins_to_register()[0].uuid.clone();
        w.poll_once();
        assert_eq!(dsw.get_plugins_to_register()[0].uuid, first);
    }
}
