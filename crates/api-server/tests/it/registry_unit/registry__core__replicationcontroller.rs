//! Unit tests moved from `src/registry/core/replicationcontroller.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
        RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_api_server::registry::scale::{Scalable, ScaleRest};
    pub use rusternetes_common::equality::semantic_equal;
    pub use rusternetes_common::resources::{
        ReplicationController, ReplicationControllerStatus, Scale, ScaleSpec, ScaleStatus,
    };
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::is_dns1123_label;
    pub use rusternetes_common::validation::replicationcontroller::{
        validate_replication_controller, validate_replication_controller_status_update,
        validate_replication_controller_update,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::replicationcontroller::*;
    use std::collections::HashMap;

    fn rc() -> ReplicationController {
        let mut rc: ReplicationController = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": {"name": "r", "namespace": "default", "generation": 4,
                         "resourceVersion": "1"},
            "spec": {
                "template": {
                    "metadata": {"labels": {"app": "r", "tier": "web"}},
                    "spec": {"containers": [{"name": "c", "image": "i"}]}
                }
            },
            "status": {"replicas": 1}
        }))
        .unwrap();
        convert_to_internal(&mut rc);
        rc
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
            Some(GarbageCollectionPolicy::OrphanDependents)
        );
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(StatusStrategy.allow_unconditional_update());
    }

    /// `SetDefaults_ReplicationController`: replicas, and the selector and
    /// labels from the template.
    #[test]
    fn defaults_come_from_the_template() {
        let rc = rc();
        assert_eq!(rc.spec.replicas, Some(1));
        let labels = HashMap::from([
            ("app".to_string(), "r".to_string()),
            ("tier".to_string(), "web".to_string()),
        ]);
        assert_eq!(rc.spec.selector.as_ref(), Some(&labels));
        assert_eq!(rc.metadata.labels.as_ref(), Some(&labels));
    }

    /// `SetDefaults_ReplicationController` (pkg/apis/core/v1/defaults.go:50-64):
    /// a set selector and labels are kept, and so are set replicas.
    #[test]
    fn explicit_values_are_not_defaulted() {
        let mut rc: ReplicationController = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "r", "labels": {"team": "x"}},
            "spec": {
                "replicas": 3,
                "selector": {"app": "explicit"},
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                }
            }
        }))
        .unwrap();
        convert_to_internal(&mut rc);
        assert_eq!(rc.spec.replicas, Some(3));
        let selector = HashMap::from([("app".to_string(), "explicit".to_string())]);
        assert_eq!(rc.spec.selector.as_ref(), Some(&selector));
        let labels = HashMap::from([("team".to_string(), "x".to_string())]);
        assert_eq!(rc.metadata.labels.as_ref(), Some(&labels));
    }

    #[test]
    fn prepare_for_create_resets_status_and_generation() {
        let mut rc = rc();
        Strategy.prepare_for_create(&ctx(), &mut rc);
        assert_eq!(rc.status, Some(ReplicationControllerStatus::default()));
        assert_eq!(rc.metadata.generation, Some(1));
        let errs = Strategy.validate(&ctx(), &rc);
        assert!(errs.is_empty(), "{errs:?}");
    }

    #[test]
    fn prepare_for_update_keeps_status_and_bumps_generation_on_spec() {
        let old = rc();
        let mut touched = old.clone();
        touched.status = None;
        Strategy.prepare_for_update(&ctx(), &mut touched, &old);
        assert_eq!(touched.status, old.status);
        assert_eq!(touched.metadata.generation, Some(4));

        let mut wider = old.clone();
        wider.spec.replicas = Some(3);
        Strategy.prepare_for_update(&ctx(), &mut wider, &old);
        assert_eq!(wider.metadata.generation, Some(5));
        let errs = Strategy.validate_update(&ctx(), &wider, &old);
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// strategy.go:162-179.
    #[test]
    fn a_non_convertible_selector_cannot_change() {
        let mut old = rc();
        old.metadata.annotations = Some(HashMap::from([(
            format!("{NON_CONVERTIBLE_ANNOTATION_PREFIX}/spec.selector"),
            "x".to_string(),
        )]));
        let mut new = old.clone();
        new.spec.selector = Some(HashMap::from([("app".to_string(), "r".to_string())]));
        let errs = Strategy.validate_update(&ctx(), &new, &old);
        assert!(
            errs.iter()
                .any(|e| e.detail == "cannot update non-convertible selector"),
            "{errs:?}"
        );
    }

    /// `scaleFromRC`: the selector string is sorted by key.
    #[test]
    fn scale_from_rc_sorts_the_selector() {
        let scale = scale_from_rc(&rc()).unwrap();
        assert_eq!(scale.status.selector, "app=r,tier=web");
        assert_eq!(scale.spec.replicas, 1);
        assert_eq!(scale.status.replicas, 1);
    }

    #[test]
    fn status_prepare_for_update_keeps_spec() {
        let old = rc();
        let mut new = old.clone();
        new.spec.replicas = Some(9);
        new.status = Some(ReplicationControllerStatus {
            replicas: 2,
            ..Default::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec.replicas, old.spec.replicas);
        assert_eq!(new.status.unwrap().replicas, 2);
    }
}
