//! Unit tests moved from `src/registry/core/service/storage.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::registry::core::service::alloc::{
        self, ClusterIpAllocators, ClusterIpTxn,
    };
    pub use rusternetes_api_server::registry::core::service::portallocator::operation::PortAllocationOperation;
    pub use rusternetes_api_server::registry::core::service::portallocator::PortAllocator;
    pub use rusternetes_api_server::registry::core::service::strategy::{StatusStrategy, Strategy};
    pub use rusternetes_api_server::registry::generic::store::{
        AfterDelete, BeginCreate, BeginUpdate, CreateOptions, Finish, UpdateOptions,
    };
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{GroupResource, RequestContext};
    pub use rusternetes_common::deletion::DeleteOptions;
    pub use rusternetes_common::resources::{
        Endpoints, IPFamily, IPFamilyPolicy, Service, ServiceExternalTrafficPolicy, ServiceType,
    };
    pub use rusternetes_common::validation::service::parse_ip_sloppy;
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::StorageBackend;
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::service::storage::*;

    fn svc(cluster_ip: &str, cluster_ips: Option<&[&str]>) -> Service {
        let mut s: Service = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "s"}, "spec": {}
        }))
        .unwrap();
        s.spec.cluster_ip = Some(cluster_ip.to_string());
        s.spec.cluster_ips = cluster_ips.map(|ips| ips.iter().map(|ip| ip.to_string()).collect());
        s
    }

    /// `TestNormalizeClusterIPs` (storage/storage_test.go:287-425).
    #[test]
    fn normalize_cluster_ips_matches_upstream() {
        type Case<'a> = (&'a str, Option<Service>, Service, &'a str, &'a [&'a str]);
        let v4 = &["10.0.0.10"][..];
        let cases: Vec<Case> = vec![
            ("new - only clusterip used", None, svc("10.0.0.10", None), "10.0.0.10", v4),
            ("new - only clusterips used", None, svc("", Some(v4)), "", v4),
            ("new - both used", None, svc("10.0.0.10", Some(v4)), "10.0.0.10", v4),
            (
                "update - no change",
                Some(svc("10.0.0.10", Some(v4))),
                svc("10.0.0.10", Some(v4)),
                "10.0.0.10",
                v4,
            ),
            (
                "update - malformed change",
                Some(svc("10.0.0.10", Some(v4))),
                svc("10.0.0.11", Some(&["10.0.0.11"])),
                "10.0.0.11",
                &["10.0.0.11"],
            ),
            (
                "update - malformed change on secondary ip",
                Some(svc("10.0.0.10", Some(&["10.0.0.10", "2000::1"]))),
                svc("10.0.0.11", Some(&["10.0.0.11", "3000::1"])),
                "10.0.0.11",
                &["10.0.0.11", "3000::1"],
            ),
            (
                "update - upgrade",
                Some(svc("10.0.0.10", Some(v4))),
                svc("10.0.0.10", Some(&["10.0.0.10", "2000::1"])),
                "10.0.0.10",
                &["10.0.0.10", "2000::1"],
            ),
            (
                "update - downgrade",
                Some(svc("10.0.0.10", Some(&["10.0.0.10", "2000::1"]))),
                svc("10.0.0.10", Some(v4)),
                "10.0.0.10",
                v4,
            ),
            (
                "update - user cleared cluster IP",
                Some(svc("10.0.0.10", Some(v4))),
                svc("", Some(v4)),
                "",
                &[],
            ),
            (
                "update - user cleared clusterIPs",
                Some(svc("10.0.0.10", Some(v4))),
                svc("10.0.0.10", None),
                "10.0.0.10",
                v4,
            ),
            (
                "update - user cleared both",
                Some(svc("10.0.0.10", Some(v4))),
                svc("", None),
                "",
                &[],
            ),
            (
                "update - user cleared ClusterIP but changed clusterIPs",
                Some(svc("10.0.0.10", Some(v4))),
                svc("", Some(&["10.0.0.11"])),
                "",
                &["10.0.0.11"],
            ),
            (
                "update - user cleared ClusterIPs but changed ClusterIP",
                Some(svc("10.0.0.10", Some(&["10.0.0.10", "2000::1"]))),
                svc("10.0.0.11", None),
                "10.0.0.11",
                &[],
            ),
            (
                "update - user changed from None to ClusterIP",
                Some(svc("None", Some(&["None"]))),
                svc("10.0.0.10", Some(&["None"])),
                "10.0.0.10",
                v4,
            ),
            (
                "update - user changed from ClusterIP to None",
                Some(svc("10.0.0.10", Some(v4))),
                svc("None", Some(v4)),
                "None",
                &["None"],
            ),
            (
                "update - user changed from ClusterIP to None and changed ClusterIPs in a dual stack",
                Some(svc("10.0.0.10", Some(&["10.0.0.10", "2000::1"]))),
                svc("None", Some(&["10.0.0.11", "2000::1"])),
                "None",
                &["10.0.0.11", "2000::1"],
            ),
        ];
        for (name, old, mut new, want_ip, want_ips) in cases {
            normalize_cluster_ips(&mut new, old.as_ref());
            assert_eq!(cluster_ip(&new), want_ip, "{name}");
            assert_eq!(cluster_ips(&new), want_ips, "{name}");
        }
    }

    fn lb_local(ips: &[&str], node_port: Option<u16>, hcnp: Option<i32>) -> Service {
        let mut s: Service = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "foo"},
            "spec": {"type": "LoadBalancer", "externalTrafficPolicy": "Local",
                     "ports": [{"name": "p", "port": 80, "protocol": "TCP"}]}
        }))
        .unwrap();
        if let Some(first) = ips.first() {
            s.spec.cluster_ip = Some(first.to_string());
            s.spec.cluster_ips = Some(ips.iter().map(|ip| ip.to_string()).collect());
        }
        s.spec.ports[0].node_port = node_port;
        s.spec.health_check_node_port = hcnp;
        s
    }

    /// `TestPatchAllocatedValues` "all_patched" (storage/storage_test.go:436-446).
    #[test]
    fn patch_allocated_values_fills_what_the_client_omitted() {
        let before = lb_local(&["10.0.0.93", "2000::76"], Some(30093), Some(31234));
        let mut update = lb_local(&[], None, None);
        patch_allocated_values(&mut update, &before);
        assert_eq!(update.spec.cluster_ip, before.spec.cluster_ip);
        assert_eq!(update.spec.cluster_ips, before.spec.cluster_ips);
        assert_eq!(update.spec.ports[0].node_port, Some(30093));
        assert_eq!(update.spec.health_check_node_port, Some(31234));
    }

    /// `defaultOnReadIPFamilies`: a headful Service stored without families
    /// reads with them inferred from its ClusterIPs.
    #[test]
    fn default_on_read_infers_families() {
        let mut s = svc("10.0.0.10", None);
        default_on_read(&mut s, &IPFamily::IPv4);
        assert_eq!(cluster_ips(&s), &["10.0.0.10"]);
        assert_eq!(s.spec.ip_families, Some(vec![IPFamily::IPv4]));
        assert_eq!(s.spec.ip_family_policy, Some(IPFamilyPolicy::SingleStack));
    }

    /// `defaultOnReadIPFamilies` on an IPv6-primary cluster
    /// (storage.go:281-330 uses `r.primaryIPFamily`, not a fixed IPv4).
    #[test]
    fn default_on_read_headless_uses_the_primary_family() {
        let mut selectorless = svc("None", None);
        default_on_read(&mut selectorless, &IPFamily::IPv6);
        assert_eq!(
            selectorless.spec.ip_families,
            Some(vec![IPFamily::IPv6, IPFamily::IPv4])
        );
        assert_eq!(
            selectorless.spec.ip_family_policy,
            Some(IPFamilyPolicy::RequireDualStack)
        );

        let mut with_selector = svc("None", None);
        with_selector.spec.selector = Some([("a".to_string(), "b".to_string())].into());
        default_on_read(&mut with_selector, &IPFamily::IPv6);
        assert_eq!(with_selector.spec.ip_families, Some(vec![IPFamily::IPv6]));
        assert_eq!(
            with_selector.spec.ip_family_policy,
            Some(IPFamilyPolicy::SingleStack)
        );
    }
}
