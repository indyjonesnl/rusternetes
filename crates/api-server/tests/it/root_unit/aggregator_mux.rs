//! Unit tests moved from `src/aggregator_mux.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::apiservice_registration::AggregatorHandlerManager;
    pub use rusternetes_common::resources::apiregistration::AVAILABLE;
    pub use rusternetes_common::resources::APIService;
    pub use rusternetes_discovery::kube_version::compare_kube_aware_version_strings;
    pub use serde_json::{json, Value};
    pub use std::cmp::Ordering;
    pub use std::sync::{Arc, Mutex, RwLock};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::aggregator_mux::*;
    use rusternetes_common::resources::apiregistration::APIServiceCondition;
    use rusternetes_common::resources::APIServiceReference;

    fn svc(name: &str, group_prio: i32, version_prio: i32, available: bool) -> APIService {
        let (version, group) = name.split_once('.').unwrap();
        let mut s = APIService::default();
        s.metadata.name = name.to_string();
        s.spec.group = group.to_string();
        s.spec.version = version.to_string();
        s.spec.group_priority_minimum = group_prio;
        s.spec.version_priority = version_prio;
        s.spec.service = Some(APIServiceReference {
            namespace: "ns".into(),
            name: "api".into(),
            port: None,
        });
        s.status.conditions.push(APIServiceCondition {
            type_: AVAILABLE.into(),
            status: if available { "True" } else { "False" }.into(),
            ..Default::default()
        });
        s
    }

    /// handler_apis_test.go `TestAPIGroup` "sorting".
    #[test]
    fn api_group_sorting() {
        let services = vec![
            svc("v1.foo", 20, 10, true),
            svc("v2.bar", 11, 0, true),
            svc("v2.foo", 1, 15, true),
            svc("v1.bar", 11, 0, true),
        ];
        let ApiGroupResponse::Group(g) = serve_api_group("foo", &services) else {
            panic!("expected a group");
        };
        assert_eq!(
            g,
            json!({
                "kind": "APIGroup", "apiVersion": "v1", "name": "foo",
                "versions": [
                    {"groupVersion": "foo/v2", "version": "v2"},
                    {"groupVersion": "foo/v1", "version": "v1"},
                ],
                "preferredVersion": {"groupVersion": "foo/v2", "version": "v2"},
            })
        );
    }

    /// handler_apis_test.go `TestAPIGroupMissing`: no APIService for the
    /// group (never added, or deleted) delegates.
    #[test]
    fn api_group_missing_delegates() {
        assert_eq!(
            serve_api_group("groupName", &[]),
            ApiGroupResponse::Delegate
        );
        assert_eq!(
            serve_api_group("missing", &[svc("v1.foo", 0, 0, true)]),
            ApiGroupResponse::Delegate
        );
    }

    /// `TestAPIs` "unavailable service": an unavailable APIService is still
    /// listed in its group's discovery.
    #[test]
    fn api_group_includes_unavailable_service() {
        let ApiGroupResponse::Group(g) = serve_api_group("foo", &[svc("v1.foo", 11, 0, false)])
        else {
            panic!("expected a group");
        };
        assert_eq!(g["versions"].as_array().unwrap().len(), 1);
        assert_eq!(g["preferredVersion"]["groupVersion"], "foo/v1");
    }

    #[test]
    fn sorted_by_group_and_version_orders_groups_by_priority_then_name() {
        let services = vec![
            svc("v1.zeta", 5, 0, true),
            svc("v1.beta", 11, 0, true),
            svc("v1.alpha", 11, 0, true),
            svc("v1beta1.alpha", 11, 0, true),
        ];
        let sorted = sorted_by_group_and_version(&services);
        let groups: Vec<&str> = sorted.iter().map(|g| g[0].spec.group.as_str()).collect();
        assert_eq!(groups, ["alpha", "beta", "zeta"]);
        // GA beats beta within a group when version priorities tie.
        assert_eq!(sorted[0][0].spec.version, "v1");
    }

    /// `proxyHandler.ServeHTTP` before any `updateAPIService`: handlingInfo
    /// is nil, so the local delegate serves.
    #[test]
    fn proxy_handler_without_info_delegates() {
        assert_eq!(ProxyHandler::new().decide(), ProxyDecision::Delegate);
    }

    /// handler_proxy_test.go `TestProxyHandler` "no target"/local cases: an
    /// APIService with no `spec.service` is served locally.
    #[test]
    fn proxy_handler_local_apiservice_is_delegated() {
        let h = ProxyHandler::new();
        let mut a = svc("v1.local", 0, 0, true);
        a.spec.service = None;
        h.update_api_service(&a);
        assert_eq!(h.decide(), ProxyDecision::Local);
    }

    /// `TestProxyHandler` "service unavailable": 503 until Available=True,
    /// then `updateAPIService` flips it.
    #[test]
    fn proxy_handler_gates_on_available_and_follows_updates() {
        let h = ProxyHandler::new();
        h.update_api_service(&svc("v1.foo", 0, 0, false));
        assert_eq!(h.decide(), ProxyDecision::ServiceUnavailable);

        h.update_api_service(&svc("v1.foo", 0, 0, true));
        let ProxyDecision::Proxy(info) = h.decide() else {
            panic!("expected proxy");
        };
        assert_eq!(info.service_namespace, "ns");
        assert_eq!(info.service_name, "api");
        assert_eq!(info.service_port, 443);

        // Back to unavailable, and `setServiceAvailable` forces it on.
        h.update_api_service(&svc("v1.foo", 0, 0, false));
        h.set_service_available();
        assert!(matches!(h.decide(), ProxyDecision::Proxy(_)));
    }

    #[test]
    fn proxy_handler_transport_error_is_500_after_availability_gate() {
        let h = ProxyHandler::new();
        h.update_api_service(&svc("v1.foo", 0, 0, false));
        h.handling_info
            .write()
            .unwrap()
            .as_mut()
            .unwrap()
            .transport_building_error = Some("boom".into());
        // The 503 gate comes first.
        assert_eq!(h.decide(), ProxyDecision::ServiceUnavailable);
        h.set_service_available();
        assert_eq!(h.decide(), ProxyDecision::Error("boom".into()));
    }
}
