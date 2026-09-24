use rusternetes_common::resources::workloads::{StatefulSet, StatefulSetStatus};
use rusternetes_common::validation::apps::{
    validate_statefulset, validate_statefulset_status_update, validate_statefulset_update,
};

fn base() -> StatefulSet {
    serde_json::from_value(serde_json::json!({
        "metadata": {"name": "web", "namespace": "default", "resourceVersion": "1"},
        "spec": {
            "serviceName": "svc",
            "podManagementPolicy": "OrderedReady",
            "updateStrategy": {"type": "RollingUpdate"},
            "selector": {"matchLabels": {"app": "web"}},
            "template": {
                "metadata": {"labels": {"app": "web"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    }))
    .unwrap()
}

#[test]
fn service_name_immutable() {
    let old = base();
    let mut new = base();
    new.spec.service_name = "other".to_string();
    assert_eq!(validate_statefulset_update(&new, &old).len(), 1);
}

#[test]
fn pod_management_policy_immutable() {
    let mut old = base();
    old.spec.pod_management_policy = Some("OrderedReady".to_string());
    let mut new = base();
    new.spec.pod_management_policy = Some("Parallel".to_string());
    assert_eq!(validate_statefulset_update(&new, &old).len(), 1);
}

#[test]
fn volume_claim_templates_immutable() {
    let old = base();
    let mut new = base();
    new.spec.volume_claim_templates = Some(vec![serde_json::from_value(
        serde_json::json!({"metadata": {"name": "data"}, "spec": {}}),
    )
    .unwrap()]);
    assert_eq!(validate_statefulset_update(&new, &old).len(), 1);
    new.spec.volume_claim_templates = None;
    assert!(validate_statefulset_update(&new, &old).is_empty());
}

#[test]
fn full_spec_revalidated_on_update() {
    // Upstream ValidateStatefulSetUpdate re-runs ValidateStatefulSetSpec on the
    // new object, so an update that introduces an invalid spec (here a negative
    // replica count) must be rejected even though no immutable field changed.
    let old = base();
    let mut new = base();
    new.spec.replicas = Some(-1);
    assert!(!validate_statefulset_update(&new, &old).is_empty());
}

#[test]
fn mutable_fields_allowed() {
    let old = base();
    let mut new = base();
    new.spec.replicas = Some(5);
    new.spec.min_ready_seconds = Some(10);
    new.spec.revision_history_limit = Some(3);
    assert!(validate_statefulset_update(&new, &old).is_empty());
}

fn aggregate(errs: &[rusternetes_common::validation::field::Error]) -> String {
    errs.iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

/// `ValidateStatefulSetName` is `NameIsDNSLabel` (validation.go:50-55): a
/// dotted name is a valid subdomain but not a valid StatefulSet name.
#[test]
fn create_requires_a_dns_label_name() {
    let mut ss = base();
    assert!(
        validate_statefulset(&ss).is_empty(),
        "{}",
        aggregate(&validate_statefulset(&ss))
    );
    ss.metadata.name = "a.b".to_string();
    let agg = aggregate(&validate_statefulset(&ss));
    assert!(agg.contains("metadata.name"), "got: {agg}");
}

/// The selector is not in the mutable list, so the clone-and-compare of
/// `ValidateStatefulSetUpdate` (validation.go:255-269) forbids changing it.
#[test]
fn selector_change_is_forbidden() {
    let old = base();
    let mut new = base();
    new.spec.selector =
        serde_json::from_value(serde_json::json!({"matchLabels": {"app": "web", "x": "y"}}))
            .unwrap();
    new.spec.template.metadata.as_mut().unwrap().labels = Some(
        [
            ("app".to_string(), "web".to_string()),
            ("x".to_string(), "y".to_string()),
        ]
        .into(),
    );
    let agg = aggregate(&validate_statefulset_update(&new, &old));
    assert!(
        agg.contains("updates to statefulset spec for fields other than"),
        "got: {agg}"
    );
}

/// `AllowInvalidServiceName: true` on update (validation.go:246-248): a
/// stored invalid serviceName does not block other updates.
#[test]
fn update_tolerates_a_stored_invalid_service_name() {
    let mut old = base();
    old.spec.service_name = "Not_A_Label".to_string();
    let mut new = old.clone();
    new.spec.replicas = Some(3);
    let errs = validate_statefulset_update(&new, &old);
    assert!(errs.is_empty(), "got: {}", aggregate(&errs));
}

/// A stored spec that fails validation skips the pod template check on
/// update (validation.go:250-253).
#[test]
fn update_skips_the_template_when_the_stored_spec_is_invalid() {
    let mut old = base();
    old.spec.template.spec.containers.clear();
    let mut new = old.clone();
    new.spec.replicas = Some(3);
    let errs = validate_statefulset_update(&new, &old);
    assert!(errs.is_empty(), "got: {}", aggregate(&errs));

    // A valid stored spec keeps the template check.
    let old = base();
    let mut new = base();
    new.spec.template.spec.containers.clear();
    let agg = aggregate(&validate_statefulset_update(&new, &old));
    assert!(agg.contains("spec.template.spec.containers"), "got: {agg}");
}

/// `ValidateStatefulSetStatus` / `ValidateStatefulSetStatusUpdate`
/// (validation.go:275-322).
#[test]
fn status_update_is_validated() {
    let mut old = base();
    old.status = Some(StatefulSetStatus {
        collision_count: Some(2),
        ..StatefulSetStatus::default()
    });
    let with = |status: StatefulSetStatus| {
        let mut ss = old.clone();
        ss.status = Some(status);
        ss
    };

    let ok = with(StatefulSetStatus {
        replicas: 3,
        ready_replicas: Some(2),
        available_replicas: Some(2),
        current_replicas: Some(3),
        updated_replicas: Some(3),
        collision_count: Some(2),
        ..StatefulSetStatus::default()
    });
    let errs = validate_statefulset_status_update(&ok, &old);
    assert!(errs.is_empty(), "got: {}", aggregate(&errs));

    let bad = with(StatefulSetStatus {
        replicas: 1,
        ready_replicas: Some(1),
        available_replicas: Some(2),
        current_replicas: Some(-1),
        observed_generation: Some(-1),
        collision_count: Some(1),
        ..StatefulSetStatus::default()
    });
    let agg = aggregate(&validate_statefulset_status_update(&bad, &old));
    for want in [
        "status.currentReplicas",
        "status.observedGeneration",
        "cannot be greater than status.replicas",
        "cannot be greater than status.readyReplicas",
        "cannot be decremented",
    ] {
        assert!(agg.contains(want), "{want} missing, got: {agg}");
    }
}
