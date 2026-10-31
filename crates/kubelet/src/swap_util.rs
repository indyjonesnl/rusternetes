//! Port of `pkg/kubelet/util/swap/swap_util.go`: whether the kernel accepts
//! the tmpfs `noswap` mount option (`IsTmpfsNoswapOptionSupported`).
//!
//! Deviation: `utilkernel.GetVersion` / `version.ParseGeneric` and
//! `inuserns.RunningInUserNS` are small local helpers over the same files
//! (`/proc/sys/kernel/osrelease`, `/proc/self/uid_map`).

use std::sync::OnceLock;
use tracing::{error, info};

use crate::volume_plugins::empty_dir::Mount;

/// `TmpfsNoswapOption`.
pub const TMPFS_NOSWAP_OPTION: &str = "noswap";
/// `utilkernel.TmpfsNoswapSupportKernelVersion` (`pkg/util/kernel/constants.go:47`).
pub const TMPFS_NOSWAP_SUPPORT_KERNEL_VERSION: &str = "6.4";

/// `version.ParseGeneric(s)`: the leading dotted run of numbers
/// (`^\s*v?([0-9]+(?:\.[0-9]+)*)(.*)*$`); anything after it is ignored.
pub fn parse_generic_version(s: &str) -> Option<Vec<u64>> {
    let s = s.trim_start();
    let s = s.strip_prefix('v').unwrap_or(s);
    let end = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let mut parts = Vec::new();
    for p in s[..end].split('.') {
        if p.is_empty() {
            // "6." is not `[0-9]+(\.[0-9]+)*` followed by anything: the run
            // stops before the dangling dot.
            break;
        }
        parts.push(p.parse::<u64>().ok()?);
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts)
    }
}

/// `Version.AtLeast`: component-wise comparison, missing components are 0.
pub fn version_at_least(v: &[u64], min: &[u64]) -> bool {
    let n = v.len().max(min.len());
    for i in 0..n {
        let (a, b) = (
            v.get(i).copied().unwrap_or(0),
            min.get(i).copied().unwrap_or(0),
        );
        if a != b {
            return a > b;
        }
    }
    true
}

/// `utilkernel.GetVersion`: `/proc/sys/kernel/osrelease`.
fn kernel_version() -> Result<Vec<u64>, String> {
    let raw = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map_err(|e| format!("failed to read os-release file: {e}"))?;
    parse_generic_version(raw.trim()).ok_or_else(|| "failed to parse kernel version".to_string())
}

/// `inuserns.RunningInUserNS` over the content of `/proc/self/uid_map`: true
/// unless the map is the single identity range `0 0 4294967295`. An unreadable
/// map means "not in a user namespace".
pub fn running_in_user_ns_from(uid_map: Option<&str>) -> bool {
    let Some(content) = uid_map else {
        return false;
    };
    let lines: Vec<Vec<&str>> = content
        .lines()
        .map(|l| l.split_whitespace().collect::<Vec<_>>())
        .filter(|f| !f.is_empty())
        .collect();
    !(lines.len() == 1 && lines[0] == ["0", "0", "4294967295"])
}

/// The helper closure of `IsTmpfsNoswapOptionSupported`, uncached.
fn is_tmpfs_noswap_option_supported_helper(mounter: &dyn Mount, mount_path: &str) -> bool {
    if running_in_user_ns_from(
        std::fs::read_to_string("/proc/self/uid_map")
            .ok()
            .as_deref(),
    ) {
        // Turning off swap in unprivileged tmpfs mounts unsupported
        // https://github.com/torvalds/linux/blob/v6.8/mm/shmem.c#L4004-L4011
        // https://github.com/kubernetes/kubernetes/issues/125137
        info!("Running under a user namespace - tmpfs noswap is not supported");
        return false;
    }

    let kernel = match kernel_version() {
        Ok(v) => v,
        Err(e) => {
            error!("cannot determine kernel version, unable to determine is tmpfs noswap is supported: {e}");
            return false;
        }
    };
    let min = parse_generic_version(TMPFS_NOSWAP_SUPPORT_KERNEL_VERSION).expect("static version");
    if version_at_least(&kernel, &min) {
        return true;
    }

    if mount_path.is_empty() {
        error!("mount path is empty, falling back to /tmp");
    }
    let base = if mount_path.is_empty() {
        std::env::temp_dir()
    } else {
        std::path::PathBuf::from(mount_path)
    };
    // os.MkdirTemp(mountPath, "tmpfs-noswap-test-")
    let dir = match tempfile_in(&base) {
        Ok(d) => d,
        Err(e) => {
            info!(
                "error creating dir to test if tmpfs noswap is enabled. Assuming not supported: mount path {}: {e}",
                base.display()
            );
            return false;
        }
    };
    let target = dir.to_string_lossy().into_owned();
    let supported = match mounter.mount_sensitive_without_systemd(
        "tmpfs",
        &target,
        "tmpfs",
        &[TMPFS_NOSWAP_OPTION.to_string()],
    ) {
        Err(e) => {
            info!("error mounting tmpfs with the noswap option. Assuming not supported: {e}");
            false
        }
        Ok(()) => {
            if let Err(e) = mounter.unmount(&target) {
                error!("error unmounting test tmpfs dir {target}: {e}");
            }
            true
        }
    };
    if let Err(e) = std::fs::remove_dir_all(&dir) {
        error!("error removing test tmpfs dir {target}: {e}");
    }
    supported
}

/// `os.MkdirTemp(dir, "tmpfs-noswap-test-")`.
fn tempfile_in(base: &std::path::Path) -> std::io::Result<std::path::PathBuf> {
    std::fs::create_dir_all(base)?;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    for i in 0..100u32 {
        let p = base.join(format!(
            "tmpfs-noswap-test-{}{}",
            nanos.wrapping_add(i),
            std::process::id()
        ));
        match std::fs::create_dir(&p) {
            Ok(()) => return Ok(p),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists))
}

static TMPFS_NOSWAP_OPTION_SUPPORTED: OnceLock<bool> = OnceLock::new();

/// `IsTmpfsNoswapOptionSupported`: probed once per process
/// (`tmpfsNoswapOptionAvailabilityOnce`).
pub fn is_tmpfs_noswap_option_supported(mounter: &dyn Mount, mount_path: &str) -> bool {
    *TMPFS_NOSWAP_OPTION_SUPPORTED
        .get_or_init(|| is_tmpfs_noswap_option_supported_helper(mounter, mount_path))
}
