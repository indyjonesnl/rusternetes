//! The all-in-one binary takes the standalone controller-manager's
//! `--kube-api-qps` / `--kube-api-burst` (upstream defaults 20 / 30,
//! `pkg/controller/apis/config/v1alpha1/defaults.go:59,62`), so its embedded
//! controllers are rate limited per controller like upstream's (#2308 item 3).

use std::process::Command;

fn help() -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_rusternetes"))
        .arg("--help")
        .output()
        .expect("spawn rusternetes binary");
    assert!(out.status.success());
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn help_lists_the_flags_with_upstream_defaults() {
    let h = help();
    assert!(h.contains("--kube-api-qps"), "{h}");
    assert!(h.contains("--kube-api-burst"), "{h}");
    let after = |flag: &str| {
        h[h.find(flag).unwrap()..]
            .lines()
            .take(8)
            .collect::<String>()
    };
    assert!(after("--kube-api-qps").contains("[default: 20]"));
    assert!(after("--kube-api-burst").contains("[default: 30]"));
}

#[test]
fn rejects_a_non_numeric_qps() {
    let out = Command::new(env!("CARGO_BIN_EXE_rusternetes"))
        .arg("--kube-api-qps=fast")
        .output()
        .expect("spawn rusternetes binary");
    assert!(!out.status.success());
}
