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
    let _ = data;
    None
}

/// Upstream `possibleCPUs` (`util_linux.go:28-38`): the parsed
/// `<sysfs>/devices/system/cpu/possible`, else `0..num_cpu`.
pub fn possible_cpus(sysfs: &Path, num_cpu: usize) -> Vec<u32> {
    let _ = (sysfs, num_cpu);
    Vec::new()
}

/// The per-CPU `thermal_throttle` paths of upstream `defaultMaskedPaths`
/// (`util.go:212-217`): each one that exists on the host under `sysfs`.
/// Returned paths are the container-visible `/sys/...` form.
pub fn thermal_throttle_paths(sysfs: &Path, num_cpu: usize) -> Vec<String> {
    let _ = (sysfs, num_cpu);
    Vec::new()
}

/// Upstream `defaultMaskedPaths()` (a `sync.OnceValue`): the static base plus
/// the host's `thermal_throttle` paths.
pub fn default_masked_paths() -> &'static [String] {
    static PATHS: OnceLock<Vec<String>> = OnceLock::new();
    PATHS.get_or_init(Vec::new)
}

/// Upstream `Manager.LookupRuntimeHandler`: nil/empty RuntimeClass resolves to
/// the empty handler; a missing RuntimeClass is an error.
pub async fn lookup_runtime_handler<S: Storage>(
    storage: &S,
    runtime_class_name: Option<&str>,
) -> Result<String, String> {
    let _ = (storage, runtime_class_name, build_key("runtimeclasses", None, ""));
    Err("unimplemented".into())
}

#[allow(dead_code)]
fn _uses(_: &RuntimeClass) {}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::MemoryStorage;

    #[test]
    fn parses_cpu_ranges_like_upstream() {
        assert_eq!(parse_possible_cpus("0-3\n"), Some(vec![0, 1, 2, 3]));
        assert_eq!(parse_possible_cpus("0,2-3"), Some(vec![0, 2, 3]));
        assert_eq!(parse_possible_cpus("5"), Some(vec![5]));
        assert_eq!(parse_possible_cpus("0-x"), None);
        assert_eq!(parse_possible_cpus("a"), None);
    }

    fn fake_sysfs(possible: Option<&str>, with_throttle: &[u32]) -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        let cpu = d.path().join("devices/system/cpu");
        std::fs::create_dir_all(&cpu).unwrap();
        if let Some(p) = possible {
            std::fs::write(cpu.join("possible"), p).unwrap();
        }
        for n in with_throttle {
            std::fs::create_dir_all(cpu.join(format!("cpu{n}/thermal_throttle"))).unwrap();
        }
        d
    }

    #[test]
    fn thermal_paths_only_for_cpus_that_exist_on_host() {
        let d = fake_sysfs(Some("0-3"), &[0, 2]);
        assert_eq!(
            thermal_throttle_paths(d.path(), 99),
            vec![
                "/sys/devices/system/cpu/cpu0/thermal_throttle".to_string(),
                "/sys/devices/system/cpu/cpu2/thermal_throttle".to_string(),
            ]
        );
    }

    #[test]
    fn possible_file_missing_falls_back_to_num_cpu() {
        let d = fake_sysfs(None, &[1]);
        assert_eq!(possible_cpus(d.path(), 2), vec![0, 1]);
        assert_eq!(
            thermal_throttle_paths(d.path(), 2),
            vec!["/sys/devices/system/cpu/cpu1/thermal_throttle".to_string()]
        );
    }

    #[test]
    fn default_masked_paths_keep_static_base_first() {
        let p = default_masked_paths();
        assert_eq!(p[0], "/proc/asound");
        assert_eq!(p[11], "/sys/devices/virtual/powercap");
    }

    #[tokio::test]
    async fn lookup_handler_matches_upstream() {
        let s = MemoryStorage::new();
        assert_eq!(lookup_runtime_handler(&s, None).await, Ok(String::new()));
        assert_eq!(
            lookup_runtime_handler(&s, Some("")).await,
            Ok(String::new())
        );
        assert!(lookup_runtime_handler(&s, Some("gvisor")).await.is_err());
        s.create(
            &build_key("runtimeclasses", None, "gvisor"),
            &RuntimeClass::new("gvisor", "runsc"),
        )
        .await
        .unwrap();
        assert_eq!(
            lookup_runtime_handler(&s, Some("gvisor")).await,
            Ok("runsc".to_string())
        );
    }
}
