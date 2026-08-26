//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-node] Variable Expansion — the `subPathExpr` cluster.
//!
//! Upstream references (k8s v1.35):
//!   - `test/e2e/common/node/expansion.go` — the five [Conformance] specs
//!   - `pkg/kubelet/kubelet_pods.go:308-325` — `makeMounts`: expand, then
//!     reject an absolute result, then reject one containing `..`
//!   - `pkg/kubelet/container/helpers.go:242-256` —
//!     `ExpandContainerVolumeMounts`: a missing *or empty* key is an error
//!   - `pkg/volume/validation/pv_validation.go:62-71` —
//!     `ValidatePathNoBacksteps` ("must not contain '..'")
//!
//! These tests drive the **production** path: `translate::container_config`,
//! the same function `ContainerRuntime::create_and_start_container` calls. The
//! previous version of this file defined its own private copy of an
//! `expand_subpath_expr` helper and asserted against that, so it passed while
//! the product had no expansion at all and all five upstream specs were red.
//! If the fix in `translate.rs` is reverted, every test here must fail.
//!
//! None of these require Docker or a running runtime.

use std::collections::HashMap;

use rusternetes_common::resources::pod::{EnvVar, VolumeMount};
use rusternetes_common::resources::{Container, Pod, PodSpec};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_kubelet::cri_runtime::translate;

const VOLUME: &str = "workdir1";
const HOST_ROOT: &str = "/var/lib/rusternetes/volumes/pod-uid/workdir1";

fn env(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value: Some(value.to_string()),
        value_from: None,
    }
}

/// A pod with one container mounting `workdir1` at /logscontainer via
/// `subPathExpr`, shaped like upstream's `newPod` in expansion.go.
fn pod_with_sub_path_expr(expr: &str, envs: Vec<EnvVar>) -> Pod {
    let container = Container {
        name: "dapi-container".to_string(),
        image: "agnhost:2.59".to_string(),
        env: Some(envs),
        volume_mounts: Some(vec![VolumeMount {
            name: VOLUME.to_string(),
            mount_path: "/logscontainer".to_string(),
            read_only: None,
            sub_path: None,
            sub_path_expr: Some(expr.to_string()),
            mount_propagation: None,
            recursive_read_only: None,
        }]),
        ..Default::default()
    };
    Pod {
        type_meta: TypeMeta {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
        },
        metadata: ObjectMeta {
            name: "var-expansion".to_string(),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![container],
            ..Default::default()
        }),
        status: None,
    }
}

fn host_paths() -> HashMap<String, String> {
    HashMap::from([(VOLUME.to_string(), HOST_ROOT.to_string())])
}

fn build(pod: &Pod) -> Result<rusternetes_cri::v1::ContainerConfig, String> {
    let container = &pod.spec.as_ref().unwrap().containers[0];
    translate::container_config(
        pod,
        container,
        &container.image,
        &host_paths(),
        &HashMap::new(),
        &HashMap::new(),
    )
}

/// "should allow substituting values in a volume subpath" [Conformance]
///
/// The mount must land in the *expanded* subdirectory of the volume, not at
/// the volume root.
#[test]
fn subpath_expr_is_expanded_against_container_env() {
    let pod = pod_with_sub_path_expr("$(POD_NAME)", vec![env("POD_NAME", "foo")]);
    let cfg = build(&pod).expect("expansion must succeed");

    assert_eq!(cfg.mounts.len(), 1);
    assert_eq!(cfg.mounts[0].host_path, format!("{HOST_ROOT}/foo"));
    assert_eq!(cfg.mounts[0].container_path, "/logscontainer");
}

/// "should succeed in writing subpaths in container" [Conformance] — the
/// multi-segment shape that spec relies on.
#[test]
fn subpath_expr_expands_multiple_references() {
    let pod = pod_with_sub_path_expr(
        "$(NAMESPACE)/$(POD_NAME)",
        vec![env("NAMESPACE", "ns1"), env("POD_NAME", "foo")],
    );
    let cfg = build(&pod).expect("expansion must succeed");

    assert_eq!(cfg.mounts[0].host_path, format!("{HOST_ROOT}/ns1/foo"));
}

/// A literal prefix around the reference survives expansion.
#[test]
fn subpath_expr_keeps_literal_segments() {
    let pod = pod_with_sub_path_expr("logs/$(POD_NAME)", vec![env("POD_NAME", "foo")]);
    let cfg = build(&pod).expect("expansion must succeed");

    assert_eq!(cfg.mounts[0].host_path, format!("{HOST_ROOT}/logs/foo"));
}

/// "should fail substituting values in a volume subpath with absolute path"
/// [Conformance]. Upstream's exact wording, because the spec asserts the
/// container fails and this string is what the operator sees.
#[test]
fn absolute_subpath_expr_result_is_rejected() {
    let pod = pod_with_sub_path_expr("$(POD_NAME)", vec![env("POD_NAME", "/tmp")]);
    let err = build(&pod).expect_err("an absolute expansion result must fail");

    assert!(
        err.contains("error SubPath `/tmp` must not be an absolute path"),
        "unexpected error: {err}"
    );
}

/// "should fail substituting values in a volume subpath with backticks"
/// [Conformance].
///
/// Despite the name, the upstream spec sets `POD_NAME: ".."`
/// (`test/e2e/common/node/expansion.go:155-181`) — it is the backstep check
/// that must fire, not any backtick rule. A backtick-specific filter passes a
/// test named after backticks and leaves this spec red.
#[test]
fn backstep_subpath_expr_result_is_rejected() {
    let pod = pod_with_sub_path_expr("$(POD_NAME)", vec![env("POD_NAME", "..")]);
    let err = build(&pod).expect_err("a `..` expansion result must fail");

    assert!(
        err.contains("unable to provision SubPath `..`: must not contain '..'"),
        "unexpected error: {err}"
    );
}

/// A backstep hidden mid-path is rejected too.
#[test]
fn nested_backstep_subpath_expr_result_is_rejected() {
    let pod = pod_with_sub_path_expr("logs/$(POD_NAME)/out", vec![env("POD_NAME", "..")]);
    let err = build(&pod).expect_err("a nested `..` must fail");

    assert!(
        err.contains("must not contain '..'"),
        "unexpected error: {err}"
    );
}

/// `ExpandContainerVolumeMounts` treats a missing key as an error rather than
/// leaving `$(VAR)` verbatim — otherwise the mount would silently land in a
/// directory literally named `$(VAR)`.
#[test]
fn missing_env_key_fails_expansion() {
    let pod = pod_with_sub_path_expr("$(POD_NAME)", vec![]);
    let err = build(&pod).expect_err("a missing key must fail");

    assert!(
        err.contains("missing value for POD_NAME"),
        "unexpected error: {err}"
    );
}

/// An *empty* value counts as missing upstream (`!ok || len(value) == 0`).
#[test]
fn empty_env_value_fails_expansion() {
    let pod = pod_with_sub_path_expr("$(POD_NAME)", vec![env("POD_NAME", "")]);
    let err = build(&pod).expect_err("an empty value must fail");

    assert!(
        err.contains("missing value for POD_NAME"),
        "unexpected error: {err}"
    );
}

/// Several missing keys are reported together, sorted — upstream joins
/// `sets.List(missingKeys)`, which is sorted.
#[test]
fn multiple_missing_keys_are_reported_sorted() {
    let pod = pod_with_sub_path_expr("$(ZED)/$(ALPHA)", vec![]);
    let err = build(&pod).expect_err("missing keys must fail");

    assert!(
        err.contains("missing value for ALPHA, ZED"),
        "unexpected error: {err}"
    );
}

/// A plain `subPath` (no expression) keeps working, and is still guarded.
#[test]
fn plain_sub_path_still_joins_and_is_guarded() {
    let mut pod = pod_with_sub_path_expr("unused", vec![]);
    {
        let vm = &mut pod.spec.as_mut().unwrap().containers[0]
            .volume_mounts
            .as_mut()
            .unwrap()[0];
        vm.sub_path_expr = None;
        vm.sub_path = Some("nested/sub".to_string());
    }
    let cfg = build(&pod).expect("a plain subPath must still work");
    assert_eq!(cfg.mounts[0].host_path, format!("{HOST_ROOT}/nested/sub"));
}

/// A mount with no subPath at all mounts the volume root, unchanged behaviour.
#[test]
fn no_sub_path_mounts_volume_root() {
    let mut pod = pod_with_sub_path_expr("unused", vec![]);
    pod.spec.as_mut().unwrap().containers[0]
        .volume_mounts
        .as_mut()
        .unwrap()[0]
        .sub_path_expr = None;

    let cfg = build(&pod).expect("no subPath must succeed");
    assert_eq!(cfg.mounts[0].host_path, HOST_ROOT);
}
