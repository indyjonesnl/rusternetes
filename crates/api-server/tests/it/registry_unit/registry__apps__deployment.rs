//! Unit tests moved from `src/registry/apps/deployment.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
        RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_api_server::registry::scale::{Scalable, ScaleRest};
    pub use rusternetes_api_server::ssa::ResetFields;
    pub use rusternetes_common::equality::semantic_equal;
    pub use rusternetes_common::pod_warnings::get_warnings_for_pod_template;
    pub use rusternetes_common::resources::{
        Deployment, DeploymentStatus, Scale, ScaleSpec, ScaleStatus,
    };
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use rusternetes_common::validation::apps::{
        validate_deployment, validate_deployment_status_update, validate_deployment_update,
    };
    pub use rusternetes_common::validation::field::{ErrorList, Path};
    pub use rusternetes_common::validation::metav1::is_dns1123_label;
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::apps::deployment::*;
    use std::collections::HashMap;

    fn deployment() -> Deployment {
        let mut d: Deployment = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "d", "namespace": "default", "generation": 4},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "d"}},
                "template": {
                    "metadata": {"labels": {"app": "d"}},
                    "spec": {"containers": [{"name": "c", "image": "i"}]}
                }
            },
            "status": {"replicas": 1}
        }))
        .unwrap();
        convert_to_internal(&mut d);
        d
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert_eq!(
            Strategy.default_garbage_collection_policy(&ctx()),
            Some(GarbageCollectionPolicy::DeleteDependents)
        );
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(StatusStrategy.allow_unconditional_update());
    }

    /// strategy.go:72-79.
    #[test]
    fn prepare_for_create_resets_status_and_generation() {
        let mut d = deployment();
        Strategy.prepare_for_create(&ctx(), &mut d);
        assert_eq!(d.status, Some(DeploymentStatus::default()));
        assert_eq!(d.metadata.generation, Some(1));
    }

    /// strategy.go:108-123, the cases of upstream `TestStatusUpdates` and
    /// the generation rules.
    #[test]
    fn prepare_for_update_keeps_status_and_bumps_generation_on_spec_or_annotations() {
        let old = deployment();

        let mut same = old.clone();
        same.status = None;
        same.metadata.labels = Some(HashMap::from([("l".into(), "1".into())]));
        Strategy.prepare_for_update(&ctx(), &mut same, &old);
        assert_eq!(same.status, old.status);
        assert_eq!(same.metadata.generation, Some(4), "labels do not bump");

        let mut scaled = old.clone();
        scaled.spec.replicas = Some(3);
        Strategy.prepare_for_update(&ctx(), &mut scaled, &old);
        assert_eq!(scaled.metadata.generation, Some(5));

        let mut annotated = old.clone();
        annotated.metadata.annotations = Some(HashMap::from([("a".into(), "1".into())]));
        Strategy.prepare_for_update(&ctx(), &mut annotated, &old);
        assert_eq!(annotated.metadata.generation, Some(5));

        // Semantic equality: an empty map is no change.
        let mut empty = old.clone();
        empty.metadata.annotations = Some(HashMap::new());
        Strategy.prepare_for_update(&ctx(), &mut empty, &old);
        assert_eq!(empty.metadata.generation, Some(4));
    }

    /// strategy.go:162-169.
    #[test]
    fn status_prepare_for_update_keeps_spec_and_labels() {
        let mut old = deployment();
        old.metadata.labels = Some(HashMap::from([("l".into(), "old".into())]));
        let mut new = old.clone();
        new.spec.replicas = Some(9);
        new.metadata.labels = Some(HashMap::from([("l".into(), "new".into())]));
        new.metadata.annotations = Some(HashMap::from([("a".into(), "kept".into())]));
        new.status = Some(DeploymentStatus {
            replicas: Some(2),
            ..DeploymentStatus::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec.replicas, Some(1));
        assert_eq!(new.metadata.labels, old.metadata.labels);
        assert_eq!(new.metadata.annotations.unwrap()["a"], "kept");
        assert_eq!(new.status.unwrap().replicas, Some(2));
    }

    #[test]
    fn a_name_that_is_not_a_dns_label_warns() {
        let mut d = deployment();
        assert!(Strategy.warnings_on_create(&ctx(), &d).is_empty());
        d.metadata.name = "a.b".to_string();
        let warnings = Strategy.warnings_on_create(&ctx(), &d);
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].starts_with("metadata.name: this is used in Pod names and hostnames"),
            "{warnings:?}"
        );
    }

    fn with_template_node_selector(d: &mut Deployment) {
        d.spec.template.spec.node_selector = Some(HashMap::from([(
            "beta.kubernetes.io/os".to_string(),
            "linux".to_string(),
        )]));
    }

    /// strategy.go:93: `GetWarningsForPodTemplate` at `spec.template`.
    #[test]
    fn create_warns_about_the_pod_template_under_spec_template() {
        let mut d = deployment();
        with_template_node_selector(&mut d);
        let warnings = Strategy.warnings_on_create(&ctx(), &d);
        assert_eq!(
            warnings,
            vec![
                r#"spec.template.spec.nodeSelector[beta.kubernetes.io/os]: deprecated since v1.14; use "kubernetes.io/os" instead"#
            ]
        );
    }

    /// pkg/api/pod/warnings.go:243-255: a template (not a Pod) warns about
    /// the deprecated container AppArmor annotation.
    #[test]
    fn create_warns_about_the_template_apparmor_annotation() {
        let mut d = deployment();
        d.spec
            .template
            .metadata
            .as_mut()
            .unwrap()
            .annotations
            .get_or_insert_with(Default::default)
            .insert(
                "container.apparmor.security.beta.kubernetes.io/c".to_string(),
                "runtime/default".to_string(),
            );
        let warnings = Strategy.warnings_on_create(&ctx(), &d);
        assert_eq!(
            warnings,
            vec![
                r#"spec.template.metadata.annotations[container.apparmor.security.beta.kubernetes.io/c]: deprecated since v1.30; use the "appArmorProfile" field instead"#
            ]
        );
    }

    /// strategy.go:133-140: only a generation change (spec or annotations)
    /// re-evaluates the template.
    #[test]
    fn update_warns_only_when_the_generation_changed() {
        let old = deployment();
        let mut new = old.clone();
        with_template_node_selector(&mut new);
        assert!(Strategy.warnings_on_update(&ctx(), &new, &old).is_empty());
        new.metadata.generation = Some(old.metadata.generation.unwrap() + 1);
        let warnings = Strategy.warnings_on_update(&ctx(), &new, &old);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].starts_with("spec.template.spec.nodeSelector["),
            "{warnings:?}"
        );
    }

    /// Upstream `dropDisabledStatusFields` (strategy.go): gate off and the old
    /// status lacks the field -> dropped; old has it, or gate on -> kept.
    #[test]
    #[serial_test::serial]
    fn status_drops_terminating_replicas_when_gate_off() {
        use rusternetes_common::feature_gates::{self, Feature};
        let _gate =
            feature_gates::with_feature(Feature::DeploymentReplicaSetTerminatingReplicas, false);
        let old = deployment();
        let mut new = old.clone();
        new.status = Some(DeploymentStatus {
            terminating_replicas: Some(3),
            ..Default::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.status.unwrap().terminating_replicas, None);

        let mut old2 = deployment();
        old2.status = Some(DeploymentStatus {
            terminating_replicas: Some(1),
            ..Default::default()
        });
        let mut new2 = old2.clone();
        new2.status = Some(DeploymentStatus {
            terminating_replicas: Some(3),
            ..Default::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new2, &old2);
        assert_eq!(new2.status.unwrap().terminating_replicas, Some(3));
    }

    #[test]
    #[serial_test::serial]
    fn status_keeps_terminating_replicas_when_gate_on() {
        let old = deployment();
        let mut new = old.clone();
        new.status = Some(DeploymentStatus {
            terminating_replicas: Some(3),
            ..Default::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.status.unwrap().terminating_replicas, Some(3));
    }
}
