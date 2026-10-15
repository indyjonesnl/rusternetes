//! `GetValidationOptionsFromPodSpecAndMeta` per-option old-spec gating (#3539).
//!
//! Sources (release-1.35):
//! - `pkg/api/pod/util.go:411` `GetValidationOptionsFromPodSpecAndMeta`
//! - `pkg/api/pod/util_test.go` `TestOldPodViolatesMatchLabelKeysValidationOption`,
//!   `TestOldPodViolatesLegacyMatchLabelKeysValidationOption`,
//!   `TestValidateAllowNonLocalProjectedTokenPathOption`,
//!   `TestValidateInvalidLabelValueInNodeSelectorOption`
//! - `pkg/apis/core/validation/validation.go` `validateNodeAffinity` (:5209),
//!   `validatePodAffinity` (:4269), `validateTopologySpreadConstraints`
//!   (:8742), `validateSysctls` (:5479), the projected token path (:1228).
//!
//! An option is relaxed on update when the OLD spec already violates the rule,
//! so an object that is already stored stays updatable; a create (no old spec)
//! is strict.

use rusternetes_common::resources::pod::{Pod, PodSpec};
use rusternetes_common::validation::pod::{validate_pod_create, validate_pod_update};
use serde_json::{json, Value};

fn pod(spec: Value) -> Pod {
    let mut s = json!({
        "containers": [{"name": "c", "image": "img"}],
        "serviceAccountName": "default",
    });
    for (k, v) in spec.as_object().unwrap() {
        s[k] = v.clone();
    }
    let spec: PodSpec = serde_json::from_value(s).unwrap();
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default", "resourceVersion": "1"},
        "spec": spec,
    }))
    .unwrap()
}

fn expr(key: &str, value: &str) -> Value {
    json!({"key": key, "operator": "NotIn", "values": [value]})
}

fn spread(match_label_keys: &[&str], exprs: Vec<Value>) -> Value {
    json!({"topologySpreadConstraints": [{
        "maxSkew": 1,
        "topologyKey": "zone",
        "whenUnsatisfiable": "DoNotSchedule",
        "matchLabelKeys": match_label_keys,
        "labelSelector": {"matchExpressions": exprs},
    }]})
}

/// Create is strict; an update whose old spec already carries the same
/// violation is accepted.
fn assert_create_rejects_update_allows(spec: Value, what: &str) {
    let create = validate_pod_create(&pod(spec.clone()), true);
    assert!(!create.is_empty(), "{what}: create must be rejected");
    let update = validate_pod_update(&pod(spec.clone()), &pod(spec), true, false);
    assert!(
        update.is_empty(),
        "{what}: update must keep an already-stored violation, got {update:?}"
    );
}

#[test]
fn invalid_label_value_in_pod_affinity_selector() {
    let spec = json!({"affinity": {"podAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": [{
            "labelSelector": {"matchExpressions":
                [{"key": "foo", "operator": "In", "values": ["-1"]}]},
            "topologyKey": "zone",
        }]
    }}});
    assert_create_rejects_update_allows(spec, "AllowInvalidLabelValueInSelector");
}

#[test]
fn invalid_label_value_in_required_node_affinity() {
    let spec = json!({"affinity": {"nodeAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{
            "matchExpressions": [{"key": "foo", "operator": "In", "values": ["-1"]}]
        }]}
    }}});
    assert_create_rejects_update_allows(spec, "AllowInvalidLabelValueInRequiredNodeAffinity");
}

#[test]
fn invalid_topology_spread_constraint_label_selector() {
    let spec = json!({"topologySpreadConstraints": [{
        "maxSkew": 1,
        "topologyKey": "zone",
        "whenUnsatisfiable": "DoNotSchedule",
        "labelSelector": {"matchExpressions":
            [{"key": "foo", "operator": "In", "values": ["-1"]}]},
    }]});
    assert_create_rejects_update_allows(spec, "AllowInvalidTopologySpreadConstraintLabelSelector");
}

#[test]
fn non_local_projected_token_path() {
    let spec = json!({"volumes": [{
        "name": "foo",
        "projected": {"sources": [{"serviceAccountToken":
            {"path": "../foo", "expirationSeconds": 3600}}]},
    }]});
    assert_create_rejects_update_allows(spec, "AllowNonLocalProjectedTokenPath");
}

#[test]
fn namespaced_sysctls_with_host_network() {
    // validateSysctls (validation.go:5479-5484) -> ValidateHostSysctl (:5491).
    let spec = json!({
        "hostNetwork": true,
        "securityContext": {"sysctls":
            [{"name": "net.ipv4.ip_local_port_range", "value": "1024 65535"}]},
    });
    let errs = validate_pod_create(&pod(spec.clone()), true);
    assert!(
        errs.iter()
            .any(|e| e.field == "spec.securityContext.sysctls[0].name"
                && e.detail == "may not be specified when 'hostNetwork' is true"),
        "{errs:?}"
    );
    let update = validate_pod_update(&pod(spec.clone()), &pod(spec), true, false);
    assert!(update.is_empty(), "{update:?}");
}

#[test]
fn namespaced_sysctls_with_host_ipc() {
    let spec = json!({
        "hostIPC": true,
        "securityContext": {"sysctls": [{"name": "kernel.shmmax", "value": "1"}]},
    });
    let errs = validate_pod_create(&pod(spec), true);
    assert!(
        errs.iter()
            .any(|e| e.detail == "may not be specified when 'hostIPC' is true"),
        "{errs:?}"
    );
}

/// MatchLabelKeysInPodTopologySpread + ...SelectorMerge are both Beta and on
/// in 1.35, so a create takes `validateTopologySpreadConstraints` case
/// `:8777-8779` -> `ValidateMatchLabelKeysAndMismatchLabelKeys`, which only
/// flags a key repeated inside the selector. One `NotIn foo` expression with
/// `matchLabelKeys: [foo]` is valid (util_test.go `UpdateValidMatchLabelKeys`).
#[test]
fn match_label_keys_selector_merge_allows_single_expression() {
    let errs = validate_pod_create(&pod(spread(&["foo"], vec![expr("foo", "value1")])), true);
    assert!(errs.is_empty(), "{errs:?}");
}

/// util_test.go `UpdateInvalidMatchLabelKeys`: two `foo` expressions plus
/// matchLabelKeys `foo` fails the merged-selector rule.
#[test]
fn match_label_keys_selector_merge_rejects_repeated_key() {
    let spec = spread(&["foo"], vec![expr("foo", "value1"), expr("foo", "value2")]);
    let errs = validate_pod_create(&pod(spec), true);
    assert!(
        errs.iter().any(
            |e| e.field == "spec.topologySpreadConstraints[0].matchLabelKeys[0]"
                && e.detail == "exists in both matchLabelKeys and labelSelector"
        ),
        "{errs:?}"
    );
}

/// `OldPodViolatesMatchLabelKeysValidation` (util.go:457): the old pod fails
/// the merged-selector rule, so the update validates with the legacy rule
/// (`:8773-8776`). With a lone `foo` expression the legacy rule rejects
/// (key already in the selector) while the merged rule accepts - so a create
/// is valid but an update whose old pod is that same spec is... also valid,
/// because the old pod does NOT violate the merged rule. Pinning the second
/// half: an old pod that violates it keeps being validated by the legacy rule,
/// which rejects a lone expression.
#[test]
fn old_pod_violating_match_label_keys_switches_to_legacy_rule() {
    let old = pod(spread(&["foo"], vec![expr("foo", "a"), expr("foo", "b")]));
    let new = pod(spread(&["foo"], vec![expr("foo", "a")]));
    let update = validate_pod_update(&new, &old, true, false);
    assert!(
        update.iter().any(
            |e| e.field == "spec.topologySpreadConstraints[0].matchLabelKeys[0]"
                && e.detail == "exists in both matchLabelKeys and labelSelector"
        ),
        "{update:?}"
    );
}
