//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-node] Probes + Init containers.
//!
//! Source of truth: Ginkgo descriptors at
//!   https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/common/node/
//! Specifically:
//!   - test/e2e/common/node/init_container.go (RestartNever / RestartAlways
//!     init container restart semantics + ordering)
//!   - test/e2e/common/node/container_probe.go (liveness/readiness/startup
//!     probes with exec/httpGet/tcpSocket/grpc actions + threshold knobs)
//!
//!
//! See docs/conformance/node-probes-init-containers.md for the
//! test-by-test status table and cross-reference into
//! docs/CONFORMANCE.md "Init containers" failure bucket (~2 failures).
//!
//! No HTTP harness: the kubelet doesn't host the REST surface. These
//! tests exercise the pure helpers
//!   - `rusternetes_kubelet::runtime::decide_next_init_action`
//!   - `rusternetes_common::resources::resolve_probe_port`
//!
//! directly, mirroring the prior-art pattern from
//! `crates/kubelet/tests/runtime_prestop_exit_test.rs` and
//! `crates/kubelet/tests/init_container_restart_test.rs`.
//!
//! Not covered here (#1762 item 2): the probe action / threshold / timing
//! specs of `container_probe.go`. The probe worker lives inline in
//! `runtime.rs` with no callable seam, and the specs previously here only
//! built a `Probe` and asserted the field they had just set, so they could
//! not fail when the kubelet regressed. They belong in a test that drives the
//! real probe loop once it is extracted (upstream `pkg/kubelet/prober/worker.go`).

use rusternetes_common::resources::{
    resolve_probe_port, Container, ContainerPort, IntOrString, Pod, PodSpec,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_kubelet::runtime::{decide_next_init_action, InitAction, InitContainerObserved};

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn make_container(name: &str) -> Container {
    Container {
        name: name.to_string(),
        image: "registry.k8s.io/e2e-test-images/agnhost:2.55".to_string(),
        image_pull_policy: Some("IfNotPresent".to_string()),
        ..Default::default()
    }
}

fn make_init(name: &str, restart_policy: Option<&str>) -> Container {
    let mut c = make_container(name);
    c.image = "registry.k8s.io/e2e-test-images/busybox:1.37.0-1".to_string();
    c.restart_policy = restart_policy.map(|s| s.to_string());
    c
}

fn make_pod_with_inits(name: &str, restart_policy: &str, inits: Vec<Container>) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace("default"),
        spec: Some(PodSpec {
            containers: vec![make_container("run1")],
            init_containers: Some(inits),
            restart_policy: Some(restart_policy.to_string()),
            ..Default::default()
        }),
        status: None,
    }
}

// ===========================================================================
// Section 1 — Init container ordering and restart semantics
//
// Mirrors test/e2e/common/node/init_container.go:
//   - L218 ConformanceIt "should invoke init containers on a RestartNever pod"
//   - L275 ConformanceIt "should invoke init containers on a RestartAlways pod"
//   - L330 ConformanceIt "should not start app containers if init containers
//          fail on a RestartAlways pod"        (Sonobuoy R160: FAIL)
//   - L430 ConformanceIt "should not start app containers and fail the pod
//          if init containers fail on a RestartNever pod"
//
// The kubelet helper under test, `decide_next_init_action`, encodes the
// entire state machine: declaration-order iteration, sidecar carve-out,
// running-blocks-advancement rule, and the pod-restart-policy branch.
// ===========================================================================

/// [sig-node] InitContainer should invoke init containers on a RestartNever pod [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/init_container.go:218
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn init_containers_should_invoke_on_restart_never_pod() {
    let pod = make_pod_with_inits(
        "invoke-never",
        "Never",
        vec![make_init("init1", None), make_init("init2", None)],
    );
    let observed = vec![
        InitContainerObserved::Exited(0),
        InitContainerObserved::Exited(0),
    ];
    let action = decide_next_init_action(&pod, &observed);
    assert_eq!(
        action,
        InitAction {
            all_init_done: true,
            next_index: None,
            should_retry: false,
        },
        "two successful init containers on a RestartNever pod must surface \
         all_init_done=true so app containers can start (init_container.go:218)"
    );
}

/// [sig-node] InitContainer should invoke init containers on a RestartAlways pod [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/init_container.go:275
/// Sonobuoy (Round 160, 2026-04-26): PASS
#[test]
fn init_containers_should_invoke_on_restart_always_pod() {
    let pod = make_pod_with_inits(
        "invoke-always",
        "Always",
        vec![make_init("init1", None), make_init("init2", None)],
    );
    let observed = vec![
        InitContainerObserved::Exited(0),
        InitContainerObserved::Exited(0),
    ];
    let action = decide_next_init_action(&pod, &observed);
    assert!(
        action.all_init_done,
        "two successful init containers on a RestartAlways pod must complete \
         (init_container.go:275)"
    );
}

/// [sig-node] InitContainer should not start app containers if init containers fail on a RestartAlways pod [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/init_container.go:330
/// Sonobuoy (Round 160): FAIL — "Expected <*v1.PodCondition>: nil not to be nil".
/// This unit-level mirror covers the pure helper
/// `decide_next_init_action`, which already returns
/// `InitAction { all_init_done: false, next_index: Some(0),
/// should_retry: true }` for a RestartAlways pod with a failed init.
/// The accompanying production-side publishing of
/// `PodCondition{Type: "Initialized", Status: "False", Reason:
/// "ContainersNotInitialized"}` is provided by
/// `Self::init_failed_pod_conditions` on the kubelet status-sync path
/// in `crates/kubelet/src/kubelet.rs` (around the init-container
/// failure branch, ~L2225) — those conditions are what the upstream
/// e2e assertion at init_container.go:446 checks.
#[test]
fn init_containers_should_not_start_app_on_restart_always_failure() {
    let pod = make_pod_with_inits(
        "fail-always",
        "Always",
        vec![make_init("init1", None), make_init("init2", None)],
    );
    // init1 has crashed (non-zero exit) on a RestartAlways pod: kubelet must
    // mark it for retry and MUST NOT advance into init2 or any app container.
    let observed = vec![
        InitContainerObserved::Exited(1),
        InitContainerObserved::NotStarted,
    ];
    let action = decide_next_init_action(&pod, &observed);
    assert_eq!(
        action,
        InitAction {
            all_init_done: false,
            next_index: Some(0),
            should_retry: true,
        },
        "RestartAlways pod with a failed init container must retry init[0] \
         and never advance to app containers (init_container.go:330)"
    );
}

/// [sig-node] InitContainer should not start app containers and fail the pod if init containers fail on a RestartNever pod [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/common/node/init_container.go:430
/// Sonobuoy (Round 160): FAIL — same PodCondition-not-published symptom as
/// the RestartAlways failure case. The pure helper `decide_next_init_action`
/// returns `InitAction { all_init_done: false, next_index: None,
/// should_retry: false }` for a RestartNever pod with a failed init, and
/// the kubelet status-sync path in `crates/kubelet/src/kubelet.rs`
/// marks the pod `Phase::Failed` with the
/// `Initialized=False / Reason=ContainersNotInitialized` condition via
/// `Self::init_failed_pod_conditions`.
#[test]
fn init_containers_should_not_start_app_and_fail_pod_on_restart_never_failure() {
    let pod = make_pod_with_inits(
        "fail-never",
        "Never",
        vec![make_init("init1", None), make_init("init2", None)],
    );
    let observed = vec![
        InitContainerObserved::Exited(1),
        InitContainerObserved::NotStarted,
    ];
    let action = decide_next_init_action(&pod, &observed);
    assert_eq!(
        action,
        InitAction {
            all_init_done: false,
            next_index: None,
            should_retry: false,
        },
        "RestartNever pod with a failed init container is terminal — the \
         kubelet must not retry and must not start app containers \
         (init_container.go:430)"
    );
}

#[test]
fn init_containers_run_in_declaration_order() {
    // The first not-yet-completed regular init container wins, even if a
    // later one is already running (which can't really happen in practice
    // but exercises the ordering guarantee).
    let pod = make_pod_with_inits(
        "ordering",
        "Always",
        vec![
            make_init("init1", None),
            make_init("init2", None),
            make_init("init3", None),
        ],
    );
    let observed = vec![
        InitContainerObserved::Exited(0),
        InitContainerObserved::NotStarted,
        InitContainerObserved::NotStarted,
    ];
    let action = decide_next_init_action(&pod, &observed);
    assert_eq!(
        action.next_index,
        Some(1),
        "init containers MUST be started in declaration order \
         (init_container.go ordering invariant)"
    );
    assert!(!action.should_retry);
}

#[test]
fn init_containers_running_blocks_app_container_start() {
    // While init[0] is still running, the kubelet must NOT advance — no
    // next_index, all_init_done=false. Mirrors the RestartAlways e2e where
    // app container `run1` stays `Waiting{PodInitializing}` while init[0]
    // is running.
    let pod = make_pod_with_inits(
        "running",
        "Always",
        vec![make_init("init1", None), make_init("init2", None)],
    );
    let observed = vec![
        InitContainerObserved::Running,
        InitContainerObserved::NotStarted,
    ];
    let action = decide_next_init_action(&pod, &observed);
    assert_eq!(action.next_index, None);
    assert!(!action.all_init_done);
}

#[test]
fn restart_on_failure_retries_failed_init() {
    // RestartPolicy=OnFailure behaves like Always for failed init containers
    // (upstream `kubelet/kuberuntime_container.go::shouldContainerBeRestarted`).
    let pod = make_pod_with_inits("on-failure", "OnFailure", vec![make_init("init1", None)]);
    let observed = vec![InitContainerObserved::Exited(1)];
    let action = decide_next_init_action(&pod, &observed);
    assert!(action.should_retry, "OnFailure must retry failed inits");
}

#[test]
fn sidecar_init_does_not_gate_app_when_still_running() {
    // KEP-753 sidecar (per-container restartPolicy=Always on an init
    // container) is not part of the upstream conformance "Init containers"
    // tests in init_container.go but is covered indirectly by the
    // sidecar_containers.go tests. The pure helper must still respect the
    // carve-out: a Running sidecar must not block app containers once the
    // regular inits are done.
    let pod = make_pod_with_inits(
        "with-sidecar",
        "Always",
        vec![
            make_init("init1", None),
            make_init("sidecar0", Some("Always")),
        ],
    );
    let observed = vec![
        InitContainerObserved::Exited(0),
        InitContainerObserved::Running,
    ];
    let action = decide_next_init_action(&pod, &observed);
    assert!(action.all_init_done);
}

#[test]
fn pod_without_init_containers_is_trivially_done() {
    let pod = make_pod_with_inits("no-inits", "Always", vec![]);
    let action = decide_next_init_action(&pod, &[]);
    assert!(action.all_init_done);
    assert_eq!(action.next_index, None);
}

#[test]
fn first_unstarted_init_container_is_selected_without_retry() {
    let pod = make_pod_with_inits("fresh", "Always", vec![make_init("init1", None)]);
    let observed = vec![InitContainerObserved::NotStarted];
    let action = decide_next_init_action(&pod, &observed);
    assert_eq!(
        action,
        InitAction {
            all_init_done: false,
            next_index: Some(0),
            should_retry: false,
        }
    );
}

// ===========================================================================
// Section 3 — Probe port resolution (Int vs named String)
//
// Mirrors the IntOrString port handling exercised by container_probe.go
// L168/L291/L310 — the e2e tests use both integer and named ports. The
// `resolve_probe_port` helper is the single source of truth used by the
// kubelet to look up the port for HTTP/TCP/gRPC probes.
// ===========================================================================

#[test]
fn probe_port_int_passes_through() {
    let c = make_container("app");
    assert_eq!(resolve_probe_port(&IntOrString::Int(80), &c), Some(80));
}

#[test]
fn probe_port_named_resolves_via_container_ports() {
    let mut c = make_container("app");
    c.ports = Some(vec![
        ContainerPort {
            name: Some("http".to_string()),
            container_port: 8080,
            host_port: None,
            host_ip: None,
            protocol: "TCP".to_string(),
        },
        ContainerPort {
            name: Some("metrics".to_string()),
            container_port: 9090,
            host_port: None,
            host_ip: None,
            protocol: "TCP".to_string(),
        },
    ]);
    assert_eq!(
        resolve_probe_port(&IntOrString::String("http".to_string()), &c),
        Some(8080),
        "named port `http` must resolve to its containerPort"
    );
    assert_eq!(
        resolve_probe_port(&IntOrString::String("metrics".to_string()), &c),
        Some(9090)
    );
}

#[test]
fn probe_port_out_of_range_returns_none() {
    let c = make_container("app");
    assert_eq!(resolve_probe_port(&IntOrString::Int(-1), &c), None);
    assert_eq!(resolve_probe_port(&IntOrString::Int(70_000), &c), None);
}
