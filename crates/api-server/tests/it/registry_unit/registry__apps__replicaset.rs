//! Unit tests moved from `src/registry/apps/replicaset.rs`.
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
    pub use rusternetes_common::resources::{
        ReplicaSet, ReplicaSetStatus, Scale, ScaleSpec, ScaleStatus,
    };
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use rusternetes_common::validation::apps::{
        validate_replicaset, validate_replicaset_status_update, validate_replicaset_update,
    };
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::metav1::is_dns1123_label;
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::apps::replicaset::*;
    use std::collections::HashMap;

    fn replica_set() -> ReplicaSet {
        let mut rs: ReplicaSet = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1", "kind": "ReplicaSet",
            "metadata": {"name": "r", "namespace": "default", "generation": 4},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "r"}},
                "template": {
                    "metadata": {"labels": {"app": "r"}},
                    "spec": {"containers": [{"name": "c", "image": "i"}]}
                }
            },
            "status": {"replicas": 1}
        }))
        .unwrap();
        convert_to_internal(&mut rs);
        rs
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

    /// strategy.go:80-86.
    #[test]
    fn prepare_for_create_resets_status_and_generation() {
        let mut rs = replica_set();
        Strategy.prepare_for_create(&ctx(), &mut rs);
        assert_eq!(rs.status, Some(ReplicaSetStatus::default()));
        assert_eq!(rs.metadata.generation, Some(1));
    }

    /// strategy.go:90-109: only a spec change bumps the generation.
    #[test]
    fn prepare_for_update_keeps_status_and_bumps_generation_on_spec() {
        let old = replica_set();

        let mut annotated = old.clone();
        annotated.status = None;
        annotated.metadata.annotations = Some(HashMap::from([("a".into(), "1".into())]));
        Strategy.prepare_for_update(&ctx(), &mut annotated, &old);
        assert_eq!(annotated.status, old.status);
        assert_eq!(
            annotated.metadata.generation,
            Some(4),
            "annotations do not bump"
        );

        let mut scaled = old.clone();
        scaled.spec.replicas = 3;
        Strategy.prepare_for_update(&ctx(), &mut scaled, &old);
        assert_eq!(scaled.metadata.generation, Some(5));
    }

    /// strategy.go:209-215: spec is kept, labels are not reset.
    #[test]
    fn status_prepare_for_update_keeps_spec_only() {
        let old = replica_set();
        let mut new = old.clone();
        new.spec.replicas = 9;
        new.metadata.labels = Some(HashMap::from([("l".into(), "new".into())]));
        new.status = Some(ReplicaSetStatus {
            replicas: 2,
            ..ReplicaSetStatus::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec.replicas, 1);
        assert_eq!(new.metadata.labels.unwrap()["l"], "new");
        assert_eq!(new.status.unwrap().replicas, 2);
    }

    /// storage.go:266-288.
    #[test]
    fn scale_from_replica_set_maps_replicas_and_selector() {
        let rs = replica_set();
        let scale = scale_from_replica_set(&rs).unwrap();
        assert_eq!(scale.spec.replicas, 1);
        assert_eq!(scale.status.replicas, 1);
        assert_eq!(scale.status.selector, "app=r");
        assert_eq!(scale.metadata.name, "r");
    }

    /// Upstream `dropDisabledStatusFields` (strategy.go): gate off and the old
    /// status lacks the field -> dropped; old has it, or gate on -> kept.
    #[test]
    #[serial_test::serial]
    fn status_drops_terminating_replicas_when_gate_off() {
        use rusternetes_common::feature_gates::{self, Feature};
        let _gate =
            feature_gates::with_feature(Feature::DeploymentReplicaSetTerminatingReplicas, false);
        let old = replica_set();
        let mut new = old.clone();
        new.status = Some(ReplicaSetStatus {
            terminating_replicas: Some(3),
            ..Default::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.status.unwrap().terminating_replicas, None);

        let mut old2 = replica_set();
        old2.status = Some(ReplicaSetStatus {
            terminating_replicas: Some(1),
            ..Default::default()
        });
        let mut new2 = old2.clone();
        new2.status = Some(ReplicaSetStatus {
            terminating_replicas: Some(3),
            ..Default::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new2, &old2);
        assert_eq!(new2.status.unwrap().terminating_replicas, Some(3));
    }

    #[test]
    #[serial_test::serial]
    fn status_keeps_terminating_replicas_when_gate_on() {
        let old = replica_set();
        let mut new = old.clone();
        new.status = Some(ReplicaSetStatus {
            terminating_replicas: Some(3),
            ..Default::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.status.unwrap().terminating_replicas, Some(3));
    }
}
