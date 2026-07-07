//! Regression guard for the netstack removal (spec 001-remove-netlink, US3).
//!
//! The all-in-one binary must NOT accept the removed in-process networking
//! flags. Per clarification Q1 (contracts/cli-flags.md), they are deleted
//! outright — not kept as no-op aliases — so clap must reject them with an
//! "unexpected argument" error and a non-zero exit.

use std::process::Command;

fn rejects_flag(flag: &str) {
    let out = Command::new(env!("CARGO_BIN_EXE_rusternetes"))
        .arg(flag)
        .output()
        .expect("spawn rusternetes binary");

    assert!(
        !out.status.success(),
        "binary unexpectedly accepted removed flag {flag:?}"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("unexpected argument") || stderr.contains(flag),
        "expected clap to reject {flag:?}; stderr was:\n{stderr}"
    );
}

#[test]
fn rejects_pod_network_mode() {
    rejects_flag("--pod-network-mode=cni");
}

#[test]
fn rejects_netstack_pod_cidr() {
    rejects_flag("--netstack-pod-cidr=10.244.0.0/16");
}

#[test]
fn rejects_netstack_service_cidr() {
    rejects_flag("--netstack-service-cidr=10.96.0.0/12");
}
