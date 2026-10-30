//! Unit tests moved from `src/registry/apiregistration/apiservice.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::resources::apiregistration::{
        new_local_available_condition, set_api_service_condition, set_defaults_api_service,
    };
    pub use rusternetes_common::resources::APIService;
    pub use rusternetes_common::validation::apiservice::{
        validate_api_service, validate_api_service_status_update, validate_api_service_update,
    };
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::apiregistration::apiservice::*;

    fn svc(remote: bool) -> APIService {
        let mut a = serde_json::from_value::<APIService>(serde_json::json!({
            "apiVersion": "apiregistration.k8s.io/v1", "kind": "APIService",
            "metadata": {"name": "v1alpha1.wardle.example.com"},
            "spec": {
                "group": "wardle.example.com", "version": "v1alpha1",
                "groupPriorityMinimum": 2000, "versionPriority": 200,
            }
        }))
        .unwrap();
        if remote {
            a.spec.service = Some(Default::default());
            a.spec.service.as_mut().unwrap().namespace = "ns".into();
            a.spec.service.as_mut().unwrap().name = "svc".into();
        }
        convert_to_internal(&mut a);
        a
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(!Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(!Strategy.allow_unconditional_update());
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(!StatusStrategy.allow_unconditional_update());
    }

    /// strategy.go:68-76: a local APIService is available at once.
    #[test]
    fn a_local_apiservice_is_available_on_create() {
        let mut a = svc(false);
        a.status.conditions.push(Default::default());
        Strategy.prepare_for_create(&ctx(), &mut a);
        assert_eq!(a.status.conditions.len(), 1);
        let c = &a.status.conditions[0];
        assert_eq!(
            (c.type_.as_str(), c.status.as_str(), c.reason.as_str()),
            ("Available", "True", "Local")
        );
        assert_eq!(c.message, "Local APIServices are always available");
    }

    /// strategy.go:68-76: a remote one starts with an empty status.
    #[test]
    fn a_remote_apiservice_has_an_empty_status_on_create() {
        let mut a = svc(true);
        a.status.conditions.push(Default::default());
        Strategy.prepare_for_create(&ctx(), &mut a);
        assert!(a.status.conditions.is_empty());
    }

    /// v1/defaults.go: the service port defaults to 443.
    #[test]
    fn the_service_port_defaults_to_443() {
        assert_eq!(svc(true).spec.service.unwrap().port, Some(443));
    }

    /// strategy.go:78-83 and 143-149.
    #[test]
    fn spec_updates_keep_the_status_and_status_updates_keep_the_spec() {
        let mut old = svc(true);
        old.status
            .conditions
            .push(rusternetes_common::resources::APIServiceCondition {
                type_: "Available".into(),
                status: "True".into(),
                ..Default::default()
            });

        let mut new = svc(true);
        new.spec.group_priority_minimum = 5;
        Strategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.status, old.status);
        assert_eq!(new.spec.group_priority_minimum, 5);

        let mut new = svc(true);
        new.spec.group_priority_minimum = 5;
        new.metadata.labels = Some([("a".to_string(), "b".to_string())].into());
        new.status = Default::default();
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec, old.spec);
        assert_eq!(new.metadata.labels, old.metadata.labels);
        assert!(new.status.conditions.is_empty());
    }
}
