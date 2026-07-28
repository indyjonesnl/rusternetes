//! Sandbox lookups must be keyed on the pod's identity, not on its bare name.
//!
//! Two pods in different namespaces routinely share a name — the upstream
//! conformance suite creates `netserver-0` / `netserver-1` in every
//! `pod-network-test-*` namespace, and hydrophone runs those namespaces
//! concurrently. A name-only `PodSandboxFilter` makes the second pod adopt the
//! first pod's sandbox, so both land in one network namespace: they report the
//! same podIP and the second container dies with
//! `listen tcp :8083: bind: address already in use`.
//!
//! Upstream keys the lookup on the pod UID —
//! k8s.io/kubernetes/pkg/kubelet/kuberuntime/kuberuntime_sandbox.go:339
//! (`getSandboxIDByPodUID`):
//!
//! ```go
//! filter := &runtimeapi.PodSandboxFilter{
//!     LabelSelector: map[string]string{types.KubernetesPodUIDLabel: string(podUID)},
//! }
//! ```

use rusternetes_kubelet::cri_runtime::runtime::{
    sandbox_filter_by_namespaced_name, sandbox_filter_by_uid,
};

const POD_NAME_LABEL: &str = "io.kubernetes.pod.name";
const POD_NAMESPACE_LABEL: &str = "io.kubernetes.pod.namespace";
const POD_UID_LABEL: &str = "io.kubernetes.pod.uid";

/// A UID-keyed filter selects exactly one pod, and must not fall back to the
/// name (which would re-open the cross-namespace collision).
#[test]
fn uid_filter_selects_on_uid_only() {
    let filter = sandbox_filter_by_uid("7eb75fc9-b604-402b-b943-35f568fe0b4b", None);

    assert_eq!(
        filter.label_selector.get(POD_UID_LABEL).map(String::as_str),
        Some("7eb75fc9-b604-402b-b943-35f568fe0b4b"),
        "filter must select on the pod UID label: {:?}",
        filter.label_selector
    );
    assert!(
        !filter.label_selector.contains_key(POD_NAME_LABEL),
        "UID is unique — adding the name label would only narrow it wrongly on rename: {:?}",
        filter.label_selector
    );
    assert!(
        filter.state.is_none(),
        "no state filter unless one was asked for"
    );
}

/// The ready-only variant is what the start path uses to decide whether it can
/// reuse a running sandbox instead of creating one.
#[test]
fn uid_filter_can_narrow_to_ready_sandboxes() {
    let ready = rusternetes_cri::v1::PodSandboxState::SandboxReady as i32;
    let filter = sandbox_filter_by_uid("uid-1", Some(ready));

    assert_eq!(
        filter.state.map(|s| s.state),
        Some(ready),
        "ready-state narrowing must be honoured"
    );
}

/// Callers that only know `(namespace, name)` — orphan cleanup, which lists
/// what the runtime reports rather than what the apiserver holds — must still
/// be namespace-scoped, or cleaning up `srv` in one namespace tears down `srv`
/// in every other namespace.
#[test]
fn namespaced_name_filter_includes_namespace() {
    let filter = sandbox_filter_by_namespaced_name("netns-probe-2", "srv", None);

    assert_eq!(
        filter
            .label_selector
            .get(POD_NAME_LABEL)
            .map(String::as_str),
        Some("srv"),
        "name label required: {:?}",
        filter.label_selector
    );
    assert_eq!(
        filter
            .label_selector
            .get(POD_NAMESPACE_LABEL)
            .map(String::as_str),
        Some("netns-probe-2"),
        "namespace label required, else same-named pods in other namespaces match: {:?}",
        filter.label_selector
    );
}

/// Regression guard for the exact conformance shape: `netserver-0` exists in
/// two concurrent test namespaces, so a lookup for one must not produce a
/// filter that would also match the other.
#[test]
fn same_name_in_two_namespaces_yields_distinct_filters() {
    let a = sandbox_filter_by_namespaced_name("pod-network-test-3155", "netserver-0", None);
    let b = sandbox_filter_by_namespaced_name("pod-network-test-8315", "netserver-0", None);

    assert_ne!(
        a.label_selector, b.label_selector,
        "identically-named pods in different namespaces must not share a selector"
    );
}
