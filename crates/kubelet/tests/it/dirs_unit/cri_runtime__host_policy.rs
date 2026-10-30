//! Unit tests moved from `src/cri_runtime/host_policy.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::RuntimeClass;
    pub use rusternetes_storage::{build_key, Storage};
    pub use std::path::Path;
    pub use std::sync::OnceLock;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::cri_runtime::host_policy::*;
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
