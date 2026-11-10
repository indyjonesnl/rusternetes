//! Port of `pkg/kubelet/nodeshutdown/systemd/inhibit_linux.go` (and
//! `inhibit_others.go`).
//!
//! Only the parts that do not need a D-Bus client are ported: the logind
//! constants, [`InhibitLock`], `ReleaseInhibitLock` (a `close(2)` on the lock
//! fd) and `OverrideInhibitDelay` (a logind drop-in file). Upstream's
//! `DBusCon` talks to logind/systemd over the system bus with
//! `github.com/godbus/dbus/v5`; this workspace has no D-Bus client crate, so
//! `NewDBusCon`, `CurrentInhibitDelay`, `InhibitShutdown`, `ReloadLogindConf`
//! and `MonitorShutdown` are NOT ported. The shutdown manager runs against
//! the `DbusInhibiter` trait exactly as upstream does and the default
//! `systemDbus` factory reports that no D-Bus connection is available.

use std::path::{Path, PathBuf};
use std::time::Duration;

/// `logindService` (inhibit_linux.go:34).
pub const LOGIND_SERVICE: &str = "org.freedesktop.login1";
/// `logindObject` (inhibit_linux.go:35).
pub const LOGIND_OBJECT: &str = "/org/freedesktop/login1";
/// `logindInterface` (inhibit_linux.go:36).
pub const LOGIND_INTERFACE: &str = "org.freedesktop.login1.Manager";
/// `logindConfigDirectory` (inhibit_linux.go:176).
pub const LOGIND_CONFIG_DIRECTORY: &str = "/etc/systemd/logind.conf.d/";
/// `kubeletLogindConf` (inhibit_linux.go:177).
pub const KUBELET_LOGIND_CONF: &str = "99-kubelet.conf";

/// `InhibitLock` (inhibit_linux.go:55): the file descriptor logind returns
/// from `Inhibit()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct InhibitLock(pub u32);

/// `DBusCon.ReleaseInhibitLock` (inhibit_linux.go:92-100): close the lock fd,
/// which lets logind start the shutdown.
pub fn release_inhibit_lock(lock: InhibitLock) -> Result<(), String> {
    // SAFETY: close(2) on an integer fd has no memory-safety preconditions.
    let rc = unsafe { libc::close(lock.0 as libc::c_int) };
    if rc != 0 {
        return Err(format!(
            "unable to close systemd inhibitor lock: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

/// `DBusCon.OverrideInhibitDelay` (inhibit_linux.go:179-200).
pub fn override_inhibit_delay(inhibit_delay_max: Duration) -> Result<(), String> {
    override_inhibit_delay_in(Path::new(LOGIND_CONFIG_DIRECTORY), inhibit_delay_max)
}

/// [`override_inhibit_delay`] with the config directory as a parameter (the
/// directory is a package constant upstream; the parameter only lets tests
/// avoid writing under `/etc`).
pub fn override_inhibit_delay_in(dir: &Path, inhibit_delay_max: Duration) -> Result<(), String> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o755)
        .create(dir)
        .map_err(|e| format!("failed creating {} directory: {e}", dir.display()))?;

    // `%.0f` of `inhibitDelayMax.Seconds()`.
    let secs = format!("{:.0}", inhibit_delay_max.as_secs_f64());
    let inhibit_override =
        format!("# Kubelet logind override\n[Login]\nInhibitDelayMaxSec={secs}\n");

    let path: PathBuf = dir.join(KUBELET_LOGIND_CONF);
    std::fs::write(&path, inhibit_override.as_bytes()).map_err(|e| {
        format!(
            "failed writing logind shutdown inhibit override file {}: {e}",
            path.display()
        )
    })?;
    // os.WriteFile(…, 0644) applies the mode on create (subject to umask).
    let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644));
    Ok(())
}
