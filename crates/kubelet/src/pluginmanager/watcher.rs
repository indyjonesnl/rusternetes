//! Port of `pkg/kubelet/pluginmanager/pluginwatcher/plugin_watcher.go`: watch
//! the plugin registration directory (`<root>/plugins_registry`) for unix
//! sockets and mirror them into the desired state of world.
//!
//! Like upstream this is event driven: inotify (via the `inotify` crate, the
//! binding `fsnotify` wraps upstream) delivers Create and Remove events, a
//! Create of a directory is traversed and watched recursively, names starting
//! with '.' and non-sockets are ignored, and the registration directory is
//! traversed once at start to pick up plugins that are already present. A
//! socket re-created at the same path is therefore a Remove followed by a
//! Create (`AddOrUpdatePlugin` swaps the UUID so the reconciler
//! re-registers), never inferred from stat identity.
//!
//! `fsnotify` event mapping (`vendor/github.com/fsnotify/fsnotify/
//! backend_inotify.go:520-545` `newEvent`): `IN_CREATE | IN_MOVED_TO` is
//! Create and `IN_DELETE | IN_DELETE_SELF` is Remove. `IN_MOVED_FROM` is
//! Rename, which `Start` (`plugin_watcher.go:75-84`) handles as neither.

use super::cache::DesiredStateOfWorld;
use futures_util::StreamExt;
use inotify::{Inotify, WatchDescriptor, WatchMask, Watches};
use std::collections::HashMap;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// `Watcher` (`plugin_watcher.go:36`).
pub struct Watcher {
    path: PathBuf,
    desired_state_of_world: Arc<DesiredStateOfWorld>,
}

/// The `fsnotify.Watcher`: the inotify watch set plus the directory each
/// watch descriptor belongs to (fsnotify keeps this in `w.watches`), needed
/// to turn an event's name back into a full path.
struct FsWatcher {
    watches: Watches,
    dirs: HashMap<WatchDescriptor, PathBuf>,
}

impl FsWatcher {
    /// `fsWatcher.Add(dir)`. Watches exactly the events `fsnotify` registers
    /// for Create and Remove (`backend_inotify.go:197,203`).
    fn add(&mut self, dir: &Path) -> std::io::Result<()> {
        let wd = self.watches.add(
            dir,
            WatchMask::CREATE | WatchMask::MOVED_TO | WatchMask::DELETE | WatchMask::DELETE_SELF,
        )?;
        self.dirs.insert(wd, dir.to_path_buf());
        Ok(())
    }
}

impl Watcher {
    /// `NewWatcher` (`:44`).
    pub fn new(sock_dir: impl Into<PathBuf>, dsw: Arc<DesiredStateOfWorld>) -> Self {
        Self {
            path: sock_dir.into(),
            desired_state_of_world: dsw,
        }
    }

    /// `Start` (`:53-100`): create the directory if needed (`init`), start the
    /// inotify watcher, traverse the directory (watching sub-directories and
    /// registering plugins already present), then process events until `stop`
    /// fires. Must be called from within a tokio runtime.
    pub fn start(
        self: &Arc<Self>,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), String> {
        // init (`:102-110`)
        std::fs::create_dir_all(&self.path)
            .map_err(|e| format!("error (re-)creating root {}: {e}", self.path.display()))?;

        let inotify =
            Inotify::init().map_err(|e| format!("failed to start plugin fsWatcher, err: {e}"))?;
        let mut fs = FsWatcher {
            watches: inotify.watches(),
            dirs: HashMap::new(),
        };
        let mut events = inotify
            .into_event_stream(vec![0u8; 4096])
            .map_err(|e| format!("failed to start plugin fsWatcher, err: {e}"))?;

        // Traverse plugin dir and add filesystem watchers before starting the
        // plugin processing goroutine (`:66-70`).
        if let Err(e) = self.traverse_plugin_dir(&mut fs, &self.path.clone()) {
            tracing::error!(path = %self.path.display(), "Failed to traverse plugin socket path: {e}");
        }

        let w = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    ev = events.next() => match ev {
                        Some(Ok(event)) => w.handle_event(&mut fs, event),
                        Some(Err(e)) => tracing::error!("FsWatcher received error: {e}"),
                        None => return,
                    },
                    _ = stop.changed() => return,
                }
            }
        });
        Ok(())
    }

    /// The body of the `select` in `Start` (`:73-86`).
    fn handle_event(&self, fs: &mut FsWatcher, event: inotify::Event<std::ffi::OsString>) {
        use inotify::EventMask;
        if event.mask.contains(EventMask::Q_OVERFLOW) {
            // fsnotify: ErrEventOverflow on the Errors channel (`:93-94`).
            tracing::error!("FsWatcher received error: inotify queue overflow");
            return;
        }
        if event.mask.contains(EventMask::IGNORED) {
            // The kernel dropped the watch (directory deleted/unmounted).
            fs.dirs.remove(&event.wd);
            return;
        }
        let Some(dir) = fs.dirs.get(&event.wd).cloned() else {
            return;
        };
        let name = match &event.name {
            Some(n) => dir.join(n),
            None => dir, // IN_DELETE_SELF names the watched directory itself.
        };
        if event
            .mask
            .intersects(EventMask::CREATE | EventMask::MOVED_TO)
        {
            if let Err(e) = self.handle_create_event(fs, &name) {
                tracing::error!(event = %name.display(), "Error when handling create event: {e}");
            }
        } else if event
            .mask
            .intersects(EventMask::DELETE | EventMask::DELETE_SELF)
        {
            self.handle_delete_event(&name);
        }
    }

    /// `traversePluginDir` (`:116-155`): watch `dir`, then walk its children,
    /// watching sub-directories and handling a Create for every unix socket.
    /// Only an error on the directory itself is returned; below it errors are
    /// logged and skipped.
    fn traverse_plugin_dir(&self, fs: &mut FsWatcher, dir: &Path) -> Result<(), String> {
        // watch the new dir
        fs.add(dir)
            .map_err(|e| format!("failed to watch {}, err: {e}", dir.display()))?;
        // traverse existing children in the dir (`w.fs.Walk`)
        let entries = std::fs::read_dir(dir)
            .map_err(|e| format!("error accessing path: {} error: {e}", dir.display()))?;
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(md) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            if md.is_dir() {
                self.traverse_plugin_dir(fs, &path)?;
            } else if is_unix_domain_socket(&path).unwrap_or(false) {
                if let Err(e) = self.handle_plugin_registration(&path) {
                    tracing::error!(event = %path.display(), "Error when handling create: {e}");
                }
            } else {
                tracing::trace!(path = %path.display(), "Ignoring file");
            }
        }
        Ok(())
    }

    /// `handleCreateEvent` (`:157-185`): names starting with '.' are ignored,
    /// a directory is traversed, a unix socket is registered, anything else is
    /// ignored.
    fn handle_create_event(&self, fs: &mut FsWatcher, path: &Path) -> Result<(), String> {
        // `getStat` is `os.Stat` (`plugin_watcher_others.go`).
        let md = std::fs::metadata(path)
            .map_err(|e| format!("stat file {} failed: {e}", path.display()))?;
        let starts_with_dot = path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with('.'));
        if starts_with_dot {
            tracing::trace!(path = %path.display(), "Ignoring file (starts with '.')");
            return Ok(());
        }
        if !md.is_dir() {
            if !md.file_type().is_socket() {
                tracing::trace!(path = %path.display(), "Ignoring non socket file");
                return Ok(());
            }
            return self.handle_plugin_registration(path);
        }
        self.traverse_plugin_dir(fs, path)
    }

    /// `handlePluginRegistration` (`:187-203`): the socket may have been
    /// deleted and re-created before it left the desired state, so
    /// `AddOrUpdatePlugin` is called regardless to refresh the timestamp.
    fn handle_plugin_registration(&self, socket_path: &Path) -> Result<(), String> {
        let p = socket_path.to_string_lossy();
        tracing::info!(path = %p, "Adding socket path or updating timestamp to desired state cache");
        self.desired_state_of_world
            .add_or_update_plugin(&p)
            .map_err(|e| {
                format!("error adding socket path {p} or updating timestamp to desired state cache: {e}")
            })
    }

    /// `handleDeleteEvent` (`:205-212`).
    fn handle_delete_event(&self, socket_path: &Path) {
        let p = socket_path.to_string_lossy();
        tracing::info!(path = %p, "Removing socket path from desired state cache");
        self.desired_state_of_world.remove_plugin(&p);
    }
}

/// `util.IsUnixDomainSocket` (`pkg/util/filesystem/util_unix.go:29-38`):
/// `os.Stat` then `ModeSocket`.
fn is_unix_domain_socket(path: &Path) -> std::io::Result<bool> {
    Ok(std::fs::metadata(path)?.file_type().is_socket())
}

#[cfg(test)]
mod tests {
    use super::*;
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
