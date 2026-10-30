//! Unit tests moved from `src/registry/apps/statefulset.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::core::persistentvolumeclaim::get_warnings_for_persistent_volume_claim_spec;
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
        RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_api_server::registry::scale::{Scalable, ScaleRest};
    pub use rusternetes_api_server::ssa::ResetFields;
    pub use rusternetes_common::equality::semantic_equal;
    pub use rusternetes_common::feature_gates::{self, Feature};
    pub use rusternetes_common::pod_warnings::get_warnings_for_pod_template;
    pub use rusternetes_common::resources::{
        Scale, ScaleSpec, ScaleStatus, StatefulSet, StatefulSetStatus,
    };
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use rusternetes_common::validation::apps::{
        validate_statefulset, validate_statefulset_status_update, validate_statefulset_update,
    };
    pub use rusternetes_common::validation::field::{ErrorList, Path};
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::apps::statefulset::*;
    use rusternetes_common::resources::policy::IntOrString;
    use std::collections::HashMap;

    fn stateful_set() -> StatefulSet {
        let mut ss: StatefulSet = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1", "kind": "StatefulSet",
            "metadata": {"name": "s", "namespace": "default", "generation": 4},
            "spec": {
                "replicas": 1,
                "serviceName": "svc",
                "selector": {"matchLabels": {"app": "s"}},
                "template": {
                    "metadata": {"labels": {"app": "s"}},
                    "spec": {"containers": [{"name": "c", "image": "i"}]}
                }
            },
            "status": {"replicas": 1}
        }))
        .unwrap();
        convert_to_internal(&mut ss);
        ss
    }

    fn with_max_unavailable(mut ss: StatefulSet) -> StatefulSet {
        ss.spec
            .update_strategy
            .as_mut()
            .unwrap()
            .rolling_update
            .as_mut()
            .unwrap()
            .max_unavailable = Some(IntOrString::Int(2));
        ss
    }

    fn max_unavailable(ss: &StatefulSet) -> Option<IntOrString> {
        ss.spec
            .update_strategy
            .as_ref()?
            .rolling_update
            .as_ref()?
            .max_unavailable
            .clone()
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

    #[test]
    fn prepare_for_create_resets_status_and_generation() {
        let mut ss = stateful_set();
        Strategy.prepare_for_create(&ctx(), &mut ss);
        assert_eq!(ss.status, Some(StatefulSetStatus::default()));
        assert_eq!(ss.metadata.generation, Some(1));
    }

    /// `dropStatefulSetDisabledFields` with the gate at its 1.35 default
    /// (off): dropped on create and on an update that introduces it, kept
    /// when the stored object already used it.
    #[test]
    #[serial_test::serial]
    fn max_unavailable_is_dropped_while_the_gate_is_off() {
        let _gate = feature_gates::with_feature(Feature::MaxUnavailableStatefulSet, false);

        let mut created = with_max_unavailable(stateful_set());
        Strategy.prepare_for_create(&ctx(), &mut created);
        assert_eq!(max_unavailable(&created), None);

        let old = stateful_set();
        let mut introduced = with_max_unavailable(old.clone());
        Strategy.prepare_for_update(&ctx(), &mut introduced, &old);
        assert_eq!(max_unavailable(&introduced), None);

        let old = with_max_unavailable(stateful_set());
        let mut kept = old.clone();
        Strategy.prepare_for_update(&ctx(), &mut kept, &old);
        assert_eq!(max_unavailable(&kept), Some(IntOrString::Int(2)));
    }

    #[test]
    #[serial_test::serial]
    fn max_unavailable_is_kept_with_the_gate_on() {
        let _gate = feature_gates::with_feature(Feature::MaxUnavailableStatefulSet, true);
        let mut created = with_max_unavailable(stateful_set());
        Strategy.prepare_for_create(&ctx(), &mut created);
        assert_eq!(max_unavailable(&created), Some(IntOrString::Int(2)));
    }

    #[test]
    fn prepare_for_update_keeps_status_and_bumps_generation_on_spec() {
        let old = stateful_set();

        let mut annotated = old.clone();
        annotated.status = None;
        annotated.metadata.annotations = Some(HashMap::from([("a".into(), "1".into())]));
        Strategy.prepare_for_update(&ctx(), &mut annotated, &old);
        assert_eq!(annotated.status, old.status);
        assert_eq!(annotated.metadata.generation, Some(4));

        let mut scaled = old.clone();
        scaled.spec.replicas = Some(3);
        Strategy.prepare_for_update(&ctx(), &mut scaled, &old);
        assert_eq!(scaled.metadata.generation, Some(5));
    }

    #[test]
    fn status_prepare_for_update_keeps_spec_only() {
        let old = stateful_set();
        let mut new = old.clone();
        new.spec.replicas = Some(9);
        new.metadata.labels = Some(HashMap::from([("l".into(), "new".into())]));
        new.status = Some(StatefulSetStatus {
            replicas: 2,
            ..StatefulSetStatus::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec.replicas, Some(1));
        assert_eq!(new.metadata.labels.unwrap()["l"], "new");
        assert_eq!(new.status.unwrap().replicas, 2);
    }

    #[test]
    fn a_negative_revision_history_limit_warns() {
        let mut ss = stateful_set();
        ss.spec.revision_history_limit = Some(10);
        assert!(Strategy.warnings_on_create(&ctx(), &ss).is_empty());
        ss.spec.revision_history_limit = Some(-1);
        let warnings = Strategy.warnings_on_create(&ctx(), &ss);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].starts_with("spec.revisionHistoryLimit:"));
        assert_eq!(Strategy.warnings_on_update(&ctx(), &ss, &ss).len(), 1);
    }

    /// `SetDefaults_StatefulSet` (pkg/apis/apps/v1/defaults.go:127-135): an absent
    /// retention policy and its empty fields default to Retain, so validation
    /// never sees an unset whenDeleted/whenScaled (#2210 item 3).
    #[test]
    fn absent_retention_policy_defaults_to_retain() {
        let ss = stateful_set();
        let p = ss.spec.persistent_volume_claim_retention_policy.unwrap();
        assert_eq!(p.when_deleted.as_deref(), Some("Retain"));
        assert_eq!(p.when_scaled.as_deref(), Some("Retain"));
    }

    #[test]
    fn volume_claim_templates_and_the_template_warn() {
        let mut ss = stateful_set();
        ss.spec.template.metadata.as_mut().unwrap().annotations = Some(HashMap::from([(
            "container.apparmor.security.beta.kubernetes.io/c".into(),
            "runtime/default".into(),
        )]));
        ss.spec.volume_claim_templates = Some(vec![serde_json::from_value(serde_json::json!({
            "metadata": {"name": "data"},
            "spec": {"resources": {"requests": {"storage": "1500m"}}}
        }))
        .unwrap()]);
        let create = Strategy.warnings_on_create(&ctx(), &ss);
        assert!(
            create.iter().any(|w| w.starts_with(
                "spec.volumeClaimTemplates[0].resources.requests[storage]: fractional byte value"
            )),
            "{create:?}"
        );
        // Update: unchanged generation skips the template, keeps the PVC
        // warning under the miscapitalised `Spec` segment (strategy.go:176).
        let update = Strategy.warnings_on_update(&ctx(), &ss, &ss);
        assert_eq!(update.len(), 1, "{update:?}");
        assert!(
            update[0].starts_with("spec.volumeClaimTemplates[0].Spec.resources.requests[storage]:")
        );
        let mut newer = ss.clone();
        newer.metadata.generation = Some(5);
        assert!(Strategy.warnings_on_update(&ctx(), &newer, &ss).len() > 1);
    }

    #[test]
    fn scale_from_stateful_set_maps_replicas_and_selector() {
        let ss = stateful_set();
        let scale = scale_from_stateful_set(&ss).unwrap();
        assert_eq!(scale.spec.replicas, 1);
        assert_eq!(scale.status.replicas, 1);
        assert_eq!(scale.status.selector, "app=s");
    }
}
