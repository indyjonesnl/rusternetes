//! Host-derived inputs to the CRI security context / mount policy.
//!
//! Ports two upstream mechanisms that read node state:
//! - `securitycontext.defaultMaskedPaths` + `possibleCPUs`
//!   (`pkg/securitycontext/util.go:190-220`, `util_linux.go:27-80`): the
//!   per-CPU `thermal_throttle` masked paths, appended after a host `os.Stat`.
//! - `Manager.LookupRuntimeHandler`
//!   (`pkg/kubelet/runtimeclass/runtimeclass_manager.go:58-78`), used by
//!   `runtimeClassSupportsRecursiveReadOnlyMounts`
//!   (`pkg/kubelet/kubelet_pods.go:2800-2811`).

use rusternetes_common::resources::RuntimeClass;
use rusternetes_storage::{build_key, Storage};
use std::path::Path;
use std::sync::OnceLock;

/// Upstream `possibleCPUsParsed` range parsing (`util_linux.go:42-77`): comma
/// separated `N` / `A-B` items; any malformed item yields `None`.
pub fn parse_possible_cpus(data: &str) -> Option<Vec<u32>> {
    let mut cpus = Vec::new();
    for r in data.trim().split(',') {
        match r.split_once('-') {
            None => cpus.push(r.parse().ok()?),
            Some((start, end)) => {
                let (start, end): (u32, u32) = (start.parse().ok()?, end.parse().ok()?);
                cpus.extend(start..=end);
            }
        }
    }
    Some(cpus)
}

/// Upstream `possibleCPUs` (`util_linux.go:28-38`): the parsed
/// `<sysfs>/devices/system/cpu/possible`, else `0..num_cpu`.
pub fn possible_cpus(sysfs: &Path, num_cpu: usize) -> Vec<u32> {
    std::fs::read_to_string(sysfs.join("devices/system/cpu/possible"))
        .ok()
        .and_then(|d| parse_possible_cpus(&d))
        .unwrap_or_else(|| (0..num_cpu as u32).collect())
}

/// The per-CPU `thermal_throttle` paths of upstream `defaultMaskedPaths`
/// (`util.go:212-217`): each one that exists on the host under `sysfs`.
/// Returned paths are the container-visible `/sys/...` form.
pub fn thermal_throttle_paths(sysfs: &Path, num_cpu: usize) -> Vec<String> {
    possible_cpus(sysfs, num_cpu)
        .into_iter()
        .map(|cpu| format!("/sys/devices/system/cpu/cpu{cpu}/thermal_throttle"))
        .filter(|p| sysfs.join(p.trim_start_matches("/sys/")).exists())
        .collect()
}

/// Upstream `defaultMaskedPaths()` (a `sync.OnceValue`, `util.go:190`): the
/// static base plus the host's `thermal_throttle` paths.
pub fn default_masked_paths() -> &'static [String] {
    static PATHS: OnceLock<Vec<String>> = OnceLock::new();
    PATHS.get_or_init(|| {
        let mut v: Vec<String> = super::translate::DEFAULT_MASKED_PATHS
            .iter()
            .map(|s| s.to_string())
            .collect();
        let ncpu = std::thread::available_parallelism().map_or(1, |n| n.get());
        v.extend(thermal_throttle_paths(Path::new("/sys"), ncpu));
        v
    })
}

/// Upstream `Manager.LookupRuntimeHandler`: nil/empty RuntimeClass resolves to
/// the empty handler; a missing RuntimeClass is an error.
pub async fn lookup_runtime_handler<S: Storage>(
    storage: &S,
    runtime_class_name: Option<&str>,
) -> Result<String, String> {
    let name = match runtime_class_name {
        None | Some("") => return Ok(String::new()),
        Some(n) => n,
    };
    storage
        .get::<RuntimeClass>(&build_key("runtimeclasses", None, name))
        .await
        .map(|rc| rc.handler)
        .map_err(|e| format!("failed to lookup RuntimeClass {name}: {e}"))
}
