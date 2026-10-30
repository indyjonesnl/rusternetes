//! Unit tests moved from `src/registry/core/podtemplate.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::equality::semantic_equal;
    pub use rusternetes_common::resources::PodTemplate;
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::podtemplate::{
        validate_pod_template, validate_pod_template_update,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::podtemplate::*;

    fn pod_template() -> PodTemplate {
        let mut pt: PodTemplate = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "PodTemplate",
            "metadata": {"name": "t", "namespace": "default", "generation": 3,
                         "resourceVersion": "1"},
            "template": {
                "metadata": {"labels": {"app": "t"}},
                "spec": {"containers": [{"name": "c", "image": "i"}]}
            }
        }))
        .unwrap();
        convert_to_internal(&mut pt);
        pt
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert!(Strategy.default_garbage_collection_policy(&ctx()).is_none());
    }

    #[test]
    fn prepare_for_create_starts_the_generation_at_one() {
        let mut pt = pod_template();
        Strategy.prepare_for_create(&ctx(), &mut pt);
        assert_eq!(pt.metadata.generation, Some(1));
        let errs = Strategy.validate(&ctx(), &pt);
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// `TestStrategy` (strategy_test.go): only a template change bumps.
    #[test]
    fn prepare_for_update_bumps_generation_on_template_change() {
        let old = pod_template();
        let mut labelled = old.clone();
        labelled.metadata.labels = Some([("l".to_string(), "v".to_string())].into());
        Strategy.prepare_for_update(&ctx(), &mut labelled, &old);
        assert_eq!(labelled.metadata.generation, Some(3));

        let mut reimaged = old.clone();
        reimaged.template.spec.containers[0].image = "j".into();
        Strategy.prepare_for_update(&ctx(), &mut reimaged, &old);
        assert_eq!(reimaged.metadata.generation, Some(4));
        let errs = Strategy.validate_update(&ctx(), &reimaged, &old);
        assert!(errs.is_empty(), "{errs:?}");
    }
}
