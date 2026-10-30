//! Unit tests moved from `src/controllers/servicecidr.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use chrono::Utc;
    pub use futures::StreamExt;
    pub use ipnet::IpNet;
    pub use rusternetes_common::resources::{
        IPAddress, ServiceCIDR, ServiceCIDRCondition, ServiceCIDRStatus,
    };
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, build_prefix, Storage, WatchEvent, WorkQueue};
    pub use std::net::IpAddr;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::servicecidr::*;

    #[test]
    fn network_and_broadcast_addresses_are_not_contained() {
        let prefix: IpNet = "10.96.0.0/24".parse().unwrap();
        assert!(!prefix_contains_ip(&prefix, &"10.96.0.0".parse().unwrap()));
        assert!(!prefix_contains_ip(
            &prefix,
            &"10.96.0.255".parse().unwrap()
        ));
        assert!(prefix_contains_ip(&prefix, &"10.96.0.1".parse().unwrap()));
        assert!(!prefix_contains_ip(&prefix, &"10.97.0.1".parse().unwrap()));
    }

    #[test]
    fn ipv6_broadcast_rule_does_not_apply() {
        let prefix: IpNet = "2001:db8::/64".parse().unwrap();
        // Network address still excluded ...
        assert!(!prefix_contains_ip(&prefix, &"2001:db8::".parse().unwrap()));
        // ... but the all-ones address is a normal IPv6 host address.
        assert!(prefix_contains_ip(
            &prefix,
            &"2001:db8::ffff:ffff:ffff:ffff".parse().unwrap()
        ));
    }

    fn sc(name: &str, cidrs: &[&str]) -> ServiceCIDR {
        ServiceCIDR::new(name, cidrs.iter().map(|c| c.to_string()).collect())
    }

    fn ip(addr: &str, managed_by: Option<&str>) -> IPAddress {
        let mut ip = IPAddress::new(
            addr,
            rusternetes_common::resources::ParentReference {
                group: None,
                resource: "services".to_string(),
                namespace: Some("default".to_string()),
                name: "svc".to_string(),
                uid: None,
            },
        );
        if let Some(m) = managed_by {
            ip.metadata
                .labels
                .get_or_insert_with(Default::default)
                .insert(LABEL_MANAGED_BY.to_string(), m.to_string());
        }
        ip
    }

    /// Upstream `TestController_cidrToCidrs`: overlapping = contains or is
    /// contained, unrelated ranges excluded.
    #[test]
    fn overlapping_includes_containing_and_contained_only() {
        let all = vec![
            sc("a", &["192.168.0.0/24"]),
            sc("wide", &["192.168.0.0/16"]),
            sc("narrow", &["192.168.0.128/25"]),
            sc("unrelated", &["10.0.0.0/24"]),
        ];
        assert_eq!(
            overlapping_service_cidrs(&all, &all[0]),
            vec!["a", "narrow", "wide"]
        );
    }

    /// Upstream `TestController_ipToCidrs`: only allocator-managed IPs count.
    #[test]
    fn containing_requires_allocator_label() {
        let all = vec![sc("a", &["192.168.0.0/24"]), sc("b", &["10.0.0.0/24"])];
        let managed = ip("192.168.0.23", Some(IP_ALLOCATOR_CONTROLLER_NAME));
        assert_eq!(containing_service_cidrs(&all, &managed), vec!["a"]);
        assert!(containing_service_cidrs(&all, &ip("192.168.0.23", None)).is_empty());
        assert!(
            containing_service_cidrs(&all, &ip("192.168.0.23", Some("someone-else"))).is_empty()
        );
    }

    #[test]
    fn ip_family_of_cidr() {
        assert_eq!(cidr_ip_family("10.96.0.0/12"), Some("IPv4"));
        assert_eq!(cidr_ip_family("2001:db8::/64"), Some("IPv6"));
        assert_eq!(cidr_ip_family("not-a-cidr"), None);
    }
}
