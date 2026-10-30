//! Unit tests moved from `src/sysctl.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::pod::Pod;
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::pod::{PodSecurityContext, PodSpec, Sysctl};
    use rusternetes_kubelet::sysctl::*;

    fn pod_with(sysctls: Vec<(&str, &str)>, host_net: bool, host_ipc: bool) -> Pod {
        let spec = PodSpec {
            host_network: Some(host_net),
            host_ipc: Some(host_ipc),
            security_context: Some(PodSecurityContext {
                sysctls: Some(
                    sysctls
                        .into_iter()
                        .map(|(n, v)| Sysctl {
                            name: n.to_string(),
                            value: v.to_string(),
                        })
                        .collect(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        };
        Pod::new("p", spec)
    }

    #[test]
    fn no_sysctls_admitted() {
        assert!(Allowlist::new(&[])
            .admit(&Pod::new("p", PodSpec::default()))
            .is_ok());
    }

    #[test]
    fn safe_sysctls_admitted() {
        let w = Allowlist::new(&[]);
        let pod = pod_with(
            vec![
                ("kernel.shm_rmid_forced", "1"),
                ("net.ipv4.ip_local_port_range", "1024 65000"),
            ],
            false,
            false,
        );
        assert!(w.admit(&pod).is_ok());
    }

    #[test]
    fn slash_separated_safe_name_normalized_and_admitted() {
        let w = Allowlist::new(&[]);
        let pod = pod_with(vec![("kernel/shm_rmid_forced", "1")], false, false);
        assert!(w.admit(&pod).is_ok());
    }

    #[test]
    fn unsafe_sysctl_rejected_by_default() {
        let w = Allowlist::new(&[]);
        let pod = pod_with(vec![("kernel.msgmax", "8192")], false, false);
        let err = w.admit(&pod).unwrap_err();
        assert!(err.contains("not allowlisted"), "got: {err}");
        assert!(err.starts_with("forbidden sysctl:"), "got: {err}");
    }

    #[test]
    fn allowed_unsafe_exact_admitted() {
        let w = Allowlist::new(&["kernel.msgmax".to_string()]);
        let pod = pod_with(vec![("kernel.msgmax", "8192")], false, false);
        assert!(w.admit(&pod).is_ok());
    }

    #[test]
    fn allowed_unsafe_prefix_admitted() {
        let w = Allowlist::new(&["kernel.msg*".to_string()]);
        let pod = pod_with(vec![("kernel.msgmnb", "16384")], false, false);
        assert!(w.admit(&pod).is_ok());
    }

    #[test]
    fn net_sysctl_forbidden_with_host_network() {
        let w = Allowlist::new(&[]);
        let pod = pod_with(vec![("net.ipv4.tcp_syncookies", "1")], true, false);
        let err = w.admit(&pod).unwrap_err();
        assert!(
            err.contains("not allowed with host Net enabled"),
            "got: {err}"
        );
    }

    #[test]
    fn ipc_sysctl_forbidden_with_host_ipc() {
        let w = Allowlist::new(&["kernel.msgmax".to_string()]);
        let pod = pod_with(vec![("kernel.msgmax", "8192")], false, true);
        let err = w.admit(&pod).unwrap_err();
        assert!(
            err.contains("not allowed with host IPC enabled"),
            "got: {err}"
        );
    }

    #[test]
    fn unknown_namespace_unsafe_pattern_skipped() {
        // `vm.swappiness` is not a namespaced sysctl; the operator entry is
        // dropped, so a pod requesting it is still rejected.
        let w = Allowlist::new(&["vm.swappiness".to_string()]);
        let pod = pod_with(vec![("vm.swappiness", "1")], false, false);
        assert!(w.admit(&pod).is_err());
    }
}
