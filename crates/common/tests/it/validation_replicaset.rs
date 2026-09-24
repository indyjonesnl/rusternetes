//! Tests for ReplicaSet field validation.
//!
//! Mirrors upstream `ValidateReplicaSetSpec`
//! (`pkg/apis/apps/validation/validation.go`, release-1.35).

use rusternetes_common::resources::workloads::{ReplicaSet, ReplicaSetStatus};
use rusternetes_common::validation::apps::{
    validate_replicaset, validate_replicaset_status_update, validate_replicaset_update,
};
use rusternetes_common::validation::field::ErrorType;
use serde_json::json;

/// Build a ReplicaSet from a spec JSON fragment (metadata is fixed).
fn rs(spec: serde_json::Value) -> ReplicaSet {
    serde_json::from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "rs", "namespace": "default", "resourceVersion": "1"},
        "spec": spec
    }))
    .unwrap()
}

fn matching_spec() -> serde_json::Value {
    json!({
        "replicas": 3,
        "selector": {"matchLabels": {"app": "web"}},
        "template": {
            "metadata": {"labels": {"app": "web"}},
            "spec": {"containers": [{"name": "c", "image": "nginx"}]}
        }
    })
}

#[test]
fn valid_replicaset_passes() {
    let errs = validate_replicaset(&rs(matching_spec()));
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
}

#[test]
fn negative_replicas_rejected() {
    let mut spec = matching_spec();
    spec["replicas"] = json!(-1);
    let errs = validate_replicaset(&rs(spec));
    assert!(
        errs.iter()
            .any(|e| e.field == "spec.replicas" && e.error_type == ErrorType::Invalid),
        "expected spec.replicas Invalid, got: {errs:?}"
    );
}

#[test]
fn negative_min_ready_seconds_rejected() {
    let mut spec = matching_spec();
    spec["minReadySeconds"] = json!(-5);
    let errs = validate_replicaset(&rs(spec));
    assert!(
        errs.iter()
            .any(|e| e.field == "spec.minReadySeconds" && e.error_type == ErrorType::Invalid),
        "expected spec.minReadySeconds Invalid, got: {errs:?}"
    );
}

#[test]
fn empty_selector_rejected() {
    let spec = json!({
        "replicas": 1,
        "selector": {},
        "template": {"metadata": {"labels": {"app": "web"}}, "spec": {"containers": [{"name": "c", "image": "nginx"}]}}
    });
    let errs = validate_replicaset(&rs(spec));
    // Upstream emits Invalid("empty selector is invalid for deployment") — ReplicaSet
    // reuses the "deployment" string verbatim (validation.go line 819).
    assert!(
        errs.iter().any(|e| e.field == "spec.selector"
            && e.error_type == ErrorType::Invalid
            && e.detail
                .contains("empty selector is invalid for deployment")),
        "expected spec.selector Invalid empty-selector, got: {errs:?}"
    );
}

#[test]
fn template_labels_must_match_selector() {
    let spec = json!({
        "replicas": 1,
        "selector": {"matchLabels": {"app": "web"}},
        // template labels do not satisfy the selector
        "template": {"metadata": {"labels": {"app": "other"}}, "spec": {"containers": [{"name": "c", "image": "nginx"}]}}
    });
    let errs = validate_replicaset(&rs(spec));
    assert!(
        errs.iter().any(|e| e.field.starts_with("spec.template")),
        "expected a template-labels mismatch error, got: {errs:?}"
    );
}

fn aggregate(errs: &[rusternetes_common::validation::field::Error]) -> String {
    errs.iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

/// `ValidateReplicaSet` starts with `ValidateObjectMeta` (validation.go:757).
#[test]
fn test_validate_replicaset_validates_metadata() {
    let mut r = rs(matching_spec());
    r.metadata.name = "Not_A_Name".to_string();
    let agg = aggregate(&validate_replicaset(&r));
    assert!(agg.contains("metadata.name"), "got: {agg}");
}

/// `ValidateReplicaSetUpdate` (validation.go:763-769): the selector is
/// immutable and the metadata update is validated.
#[test]
fn test_validate_replicaset_update() {
    let old = rs(matching_spec());
    assert!(validate_replicaset_update(&old, &old).is_empty());

    let mut spec = matching_spec();
    spec["selector"] = json!({"matchLabels": {"app": "web", "tier": "x"}});
    spec["template"]["metadata"]["labels"] = json!({"app": "web", "tier": "x"});
    let agg = aggregate(&validate_replicaset_update(&rs(spec), &old));
    assert!(agg.contains("spec.selector"), "got: {agg}");
    assert!(agg.contains("field is immutable"), "got: {agg}");

    let mut renamed = old.clone();
    renamed.metadata.name = "other".to_string();
    let agg = aggregate(&validate_replicaset_update(&renamed, &old));
    assert!(agg.contains("metadata.name"), "got: {agg}");
}

/// `ValidateReplicaSetStatus` (validation.go:780-804).
#[test]
fn test_validate_replicaset_status_update() {
    let old = rs(matching_spec());
    let with = |status: ReplicaSetStatus| {
        let mut r = old.clone();
        r.status = Some(status);
        r
    };

    let ok = with(ReplicaSetStatus {
        replicas: 3,
        fully_labeled_replicas: Some(3),
        ready_replicas: 2,
        available_replicas: 2,
        observed_generation: Some(1),
        ..ReplicaSetStatus::default()
    });
    let errs = validate_replicaset_status_update(&ok, &old);
    assert!(errs.is_empty(), "got: {}", aggregate(&errs));

    let bad = with(ReplicaSetStatus {
        replicas: 1,
        fully_labeled_replicas: Some(2),
        ready_replicas: 1,
        available_replicas: -1,
        observed_generation: Some(-1),
        terminating_replicas: Some(-2),
        ..ReplicaSetStatus::default()
    });
    let agg = aggregate(&validate_replicaset_status_update(&bad, &old));
    for field in [
        "status.fullyLabeledReplicas",
        "status.availableReplicas",
        "status.observedGeneration",
        "status.terminatingReplicas",
    ] {
        assert!(agg.contains(field), "{field} missing, got: {agg}");
    }
    assert!(
        agg.contains("cannot be greater than status.replicas"),
        "got: {agg}"
    );

    let over_ready = with(ReplicaSetStatus {
        replicas: 3,
        ready_replicas: 1,
        available_replicas: 2,
        ..ReplicaSetStatus::default()
    });
    let agg = aggregate(&validate_replicaset_status_update(&over_ready, &old));
    assert!(
        agg.contains("cannot be greater than readyReplicas"),
        "got: {agg}"
    );
}
