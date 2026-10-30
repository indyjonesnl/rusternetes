//! Unit tests moved from `src/registry/core/service/ipallocator/repair.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use futures::StreamExt;
    pub use ipnet::IpNet;
    pub use rusternetes_api_server::registry::core::service::ipallocator::{
        family_of, ip_address_key, new_ip_address, prefix_contains_ip, IpAddressClient,
        CONTROLLER_NAME, IP_ADDRESS_PREFIX, LABEL_IP_ADDRESS_FAMILY, LABEL_MANAGED_BY,
    };
    pub use rusternetes_common::resources::{
        EventSource, EventType, IPAddress, ObjectReference, Service, ServiceCIDR,
    };
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::workqueue::{extract_key, WorkQueue};
    pub use rusternetes_storage::{build_key, EventRecorder, Storage};
    pub use std::net::IpAddr;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{error, warn};
}

mod tests {
    //! Ports of controller/repairip_test.go.
    use super::__parent::*;
    use chrono::TimeZone;
    use rusternetes_api_server::registry::core::service::ipallocator::repair::*;
    use rusternetes_common::resources::Event;
    use rusternetes_storage::MemoryStorage;
    use serde_json::json;

    /// `fake.NewSimpleClientset()` with the create/delete reactors of
    /// `newFakeRepair` (repairip_test.go:54-90): writes land in storage
    /// (the informer indexers) and each is recorded as a `(verb,
    /// resource)` action for `expectAction`.
    struct FakeClient {
        storage: Arc<MemoryStorage>,
        actions: std::sync::Mutex<Vec<[&'static str; 2]>>,
    }

    impl FakeClient {
        fn new(storage: Arc<MemoryStorage>) -> Arc<Self> {
            Arc::new(Self {
                storage,
                actions: Default::default(),
            })
        }

        fn actions(&self) -> Vec<[&'static str; 2]> {
            self.actions.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl IpAddressClient for FakeClient {
        async fn create(&self, ip: IPAddress) -> Result<()> {
            self.actions.lock().unwrap().push(["create", "ipaddresses"]);
            self.storage
                .create(&ip_address_key(&ip.metadata.name), &ip)
                .await
                .map(|_| ())
        }

        async fn delete(&self, name: &str) -> Result<()> {
            self.actions.lock().unwrap().push(["delete", "ipaddresses"]);
            self.storage.delete(&ip_address_key(name)).await
        }
    }

    const V4: &str = "10.0.0.0/16";
    const V6: &str = "2001:db8::/64";

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2012, 1, 1, 0, 0, 0).unwrap()
    }

    fn secs(n: i64) -> DateTime<Utc> {
        t0() + chrono::Duration::seconds(n)
    }

    /// `newServiceWithCreationTimestamp` (repairip_test.go).
    fn svc(name: &str, ips: &[&str], created: DateTime<Utc>) -> Service {
        let mut s: Service = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": name, "namespace": "bar"},
            "spec": {"clusterIP": ips[0], "clusterIPs": ips},
        }))
        .unwrap();
        s.metadata.creation_timestamp = Some(created);
        s
    }

    /// `newIPAddressWithCreationTimestamp`.
    fn ip_address(name: &str, svc_name: &str, created: DateTime<Utc>) -> IPAddress {
        let mut ip = new_ip_address(name, Some(&svc(svc_name, &[name], t0())));
        ip.metadata.creation_timestamp = Some(created);
        ip
    }

    fn cidr(name: &str, cidrs: &[&str]) -> ServiceCIDR {
        ServiceCIDR::new(
            name,
            cidrs
                .iter()
                .filter(|c| !c.is_empty())
                .map(|c| c.to_string())
                .collect(),
        )
    }

    struct Case {
        name: &'static str,
        svcs: Vec<Service>,
        ip_addresses: Vec<IPAddress>,
        cidrs: Vec<ServiceCIDR>,
        test_time: DateTime<Utc>,
        expected_ips: &'static [&'static str],
        /// IPAddresses that must be gone afterwards.
        deleted_ips: &'static [&'static str],
        /// `actions [][]string // verb and resource`.
        actions: &'static [[&'static str; 2]],
        events: &'static [&'static str],
    }

    async fn run(tc: Case) {
        let storage = Arc::new(MemoryStorage::new());
        for c in &tc.cidrs {
            storage
                .create(&build_key("servicecidrs", None, &c.metadata.name), c)
                .await
                .unwrap();
        }
        for s in &tc.svcs {
            storage
                .create(&svc_key("bar", &s.metadata.name), s)
                .await
                .unwrap();
        }
        for ip in &tc.ip_addresses {
            storage
                .create(&ip_address_key(&ip.metadata.name), ip)
                .await
                .unwrap();
        }
        let client = FakeClient::new(storage.clone());
        let r = RepairIpAddress::new(storage.clone(), client.clone()).with_clock(tc.test_time);
        r.run_once()
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", tc.name));

        for name in tc.expected_ips {
            assert!(
                storage
                    .get::<IPAddress>(&ip_address_key(name))
                    .await
                    .is_ok(),
                "{}: IPAddress {name} missing",
                tc.name
            );
        }
        for name in tc.deleted_ips {
            assert!(
                storage
                    .get::<IPAddress>(&ip_address_key(name))
                    .await
                    .is_err(),
                "{}: IPAddress {name} should be gone",
                tc.name
            );
        }
        // `expectAction(t, c.Actions(), test.actions)`.
        assert_eq!(client.actions(), tc.actions, "{}: actions", tc.name);
        // One Event per (object, reason): repeated reasons aggregate.
        let events: Vec<Event> = storage.list("/registry/events/").await.unwrap();
        let got: HashSet<String> = events
            .iter()
            .map(|e| format!("Warning {} {}", e.reason, e.message))
            .collect();
        let want_reasons: HashSet<&str> = tc
            .events
            .iter()
            .map(|e| e.split(' ').nth(1).unwrap())
            .collect();
        let got_reasons: HashSet<&str> = got.iter().map(|e| e.split(' ').nth(1).unwrap()).collect();
        assert_eq!(got_reasons, want_reasons, "{}: events {got:?}", tc.name);
        for e in &got {
            assert!(
                tc.events.contains(&e.as_str()),
                "{}: unexpected event {e}",
                tc.name
            );
        }
    }

    use std::collections::HashSet;

    #[tokio::test]
    async fn repair_service_ip() {
        let k8s = || vec![cidr("kubernetes", &[V4, V6])];
        let cases = vec![
            Case {
                name: "no changes needed single stack",
                svcs: vec![svc("test-svc", &["10.0.1.1"], secs(1))],
                ip_addresses: vec![ip_address("10.0.1.1", "test-svc", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &[],
                actions: &[],
                events: &[],
            },
            Case {
                name: "no changes needed dual stack",
                svcs: vec![svc("test-svc", &["10.0.1.1", "2001:db8::10"], secs(1))],
                ip_addresses: vec![
                    ip_address("10.0.1.1", "test-svc", t0()),
                    ip_address("2001:db8::10", "test-svc", t0()),
                ],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1", "2001:db8::10"],
                deleted_ips: &[],
                actions: &[],
                events: &[],
            },
            Case {
                name: "no changes needed dual stack multiple cidrs",
                svcs: vec![svc("test-svc", &["192.168.0.1", "2001:db8:a:b::10"], secs(1))],
                ip_addresses: vec![
                    ip_address("192.168.0.1", "test-svc", t0()),
                    ip_address("2001:db8:a:b::10", "test-svc", t0()),
                ],
                cidrs: vec![
                    cidr("kubernetes", &[V4, V6]),
                    cidr("custom", &["192.168.0.0/24", "2001:db8:a:b::/64"]),
                ],
                test_time: t0(),
                expected_ips: &["192.168.0.1", "2001:db8:a:b::10"],
                deleted_ips: &[],
                actions: &[],
                events: &[],
            },
            Case {
                name: "create IPAddress single stack",
                svcs: vec![svc("test-svc", &["10.0.1.1"], secs(1))],
                ip_addresses: vec![],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &[],
                actions: &[["create", "ipaddresses"]],
                events: &["Warning ClusterIPNotAllocated Cluster IP [IPv4]: 10.0.1.1 is not allocated; repairing"],
            },
            Case {
                name: "create IPAddresses dual stack",
                svcs: vec![svc("test-svc", &["10.0.1.1", "2001:db8::10"], secs(1))],
                ip_addresses: vec![],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1", "2001:db8::10"],
                deleted_ips: &[],
                actions: &[["create", "ipaddresses"], ["create", "ipaddresses"]],
                events: &[
                    "Warning ClusterIPNotAllocated Cluster IP [IPv4]: 10.0.1.1 is not allocated; repairing",
                    "Warning ClusterIPNotAllocated Cluster IP [IPv6]: 2001:db8::10 is not allocated; repairing",
                ],
            },
            Case {
                name: "create IPAddress single stack from secondary",
                svcs: vec![svc("test-svc", &["192.168.1.1"], secs(1))],
                ip_addresses: vec![],
                cidrs: vec![cidr("kubernetes", &[V4, V6]), cidr("custom", &["192.168.1.0/24", ""])],
                test_time: t0(),
                expected_ips: &["192.168.1.1"],
                deleted_ips: &[],
                actions: &[["create", "ipaddresses"]],
                events: &["Warning ClusterIPNotAllocated Cluster IP [IPv4]: 192.168.1.1 is not allocated; repairing"],
            },
            Case {
                name: "reconcile IPAddress single stack wrong reference",
                svcs: vec![svc("test-svc", &["10.0.1.1"], secs(1))],
                ip_addresses: vec![ip_address("10.0.1.1", "test-svc2", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &[],
                actions: &[["delete", "ipaddresses"], ["create", "ipaddresses"]],
                events: &["Warning ClusterIPNotAllocated the ClusterIP [IPv4]: 10.0.1.1 for Service bar/test-svc has a wrong reference; repairing"],
            },
            Case {
                name: "reconcile IPAddresses dual stack",
                svcs: vec![svc("test-svc", &["10.0.1.1", "2001:db8::10"], t0() - chrono::Duration::seconds(1))],
                ip_addresses: vec![
                    ip_address("10.0.1.1", "test-svc2", t0()),
                    ip_address("2001:db8::10", "test-svc2", t0()),
                ],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1", "2001:db8::10"],
                deleted_ips: &[],
                actions: &[["delete", "ipaddresses"], ["create", "ipaddresses"], ["delete", "ipaddresses"], ["create", "ipaddresses"]],
                events: &[
                    "Warning ClusterIPNotAllocated the ClusterIP [IPv4]: 10.0.1.1 for Service bar/test-svc has a wrong reference; repairing",
                    "Warning ClusterIPNotAllocated the ClusterIP [IPv6]: 2001:db8::10 for Service bar/test-svc has a wrong reference; repairing",
                ],
            },
            Case {
                name: "one IP out of range",
                svcs: vec![svc("test-svc", &["192.168.1.1", "2001:db8::10"], secs(1))],
                ip_addresses: vec![
                    ip_address("192.168.1.1", "test-svc", t0()),
                    ip_address("2001:db8::10", "test-svc", t0()),
                ],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["2001:db8::10"],
                deleted_ips: &[],
                actions: &[],
                events: &["Warning ClusterIPOutOfRange Cluster IP [IPv4]: 192.168.1.1 is not within any configured Service CIDR; please recreate service"],
            },
            Case {
                name: "one IP orphan within grace period",
                svcs: vec![],
                ip_addresses: vec![ip_address("10.0.1.1", "test-svc", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &[],
                actions: &[],
                events: &[],
            },
            Case {
                name: "one IP orphan after grace period",
                svcs: vec![],
                ip_addresses: vec![ip_address("10.0.1.1", "test-svc", t0())],
                cidrs: k8s(),
                test_time: secs(70),
                expected_ips: &[],
                deleted_ips: &["10.0.1.1"],
                actions: &[["delete", "ipaddresses"]],
                events: &["Warning IPAddressNotAllocated IPAddress: 10.0.1.1 for Service bar/test-svc appears to have leaked: cleaning up"],
            },
            Case {
                name: "one IP out of range matching the network address",
                svcs: vec![svc("test-svc", &["10.0.0.0"], secs(1))],
                ip_addresses: vec![ip_address("10.0.0.0", "test-svc", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.0.0"],
                deleted_ips: &[],
                actions: &[],
                events: &["Warning ClusterIPOutOfRange Cluster IP [IPv4]: 10.0.0.0 is not within any configured Service CIDR; please recreate service"],
            },
            Case {
                name: "one IP out of range matching the broadcast address",
                svcs: vec![svc("test-svc", &["10.0.255.255"], secs(1))],
                ip_addresses: vec![ip_address("10.0.255.255", "test-svc", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.255.255"],
                deleted_ips: &[],
                actions: &[],
                events: &["Warning ClusterIPOutOfRange Cluster IP [IPv4]: 10.0.255.255 is not within any configured Service CIDR; please recreate service"],
            },
            Case {
                name: "one IPv6 out of range matching the subnet address",
                svcs: vec![svc("test-svc", &["2001:db8::"], secs(1))],
                ip_addresses: vec![ip_address("2001:db8::", "test-svc", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["2001:db8::"],
                deleted_ips: &[],
                actions: &[],
                events: &["Warning ClusterIPOutOfRange Cluster IP [IPv6]: 2001:db8:: is not within any configured Service CIDR; please recreate service"],
            },
            Case {
                name: "one IPv6 matching the broadcast address",
                svcs: vec![svc("test-svc", &["2001:db8::ffff:ffff:ffff:ffff"], secs(1))],
                ip_addresses: vec![ip_address("2001:db8::ffff:ffff:ffff:ffff", "test-svc", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["2001:db8::ffff:ffff:ffff:ffff"],
                deleted_ips: &[],
                actions: &[],
                events: &[],
            },
            Case {
                name: "one IP orphan matching the broadcast address within grace period",
                svcs: vec![],
                ip_addresses: vec![ip_address("10.0.255.255", "test-svc", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.255.255"],
                deleted_ips: &[],
                actions: &[],
                events: &[],
            },
            Case {
                name: "one IP orphan matching the broadcast address after grace period",
                svcs: vec![],
                ip_addresses: vec![ip_address("10.0.255.255", "test-svc", t0())],
                cidrs: k8s(),
                test_time: secs(90),
                expected_ips: &[],
                deleted_ips: &["10.0.255.255"],
                actions: &[["delete", "ipaddresses"]],
                events: &["Warning IPAddressNotAllocated IPAddress: 10.0.255.255 for Service bar/test-svc appears to have leaked: cleaning up"],
            },
            Case {
                name: "Two IPAddresses referencing the same service",
                svcs: vec![svc("test-svc", &["10.0.1.1"], secs(1))],
                ip_addresses: vec![
                    ip_address("10.0.1.1", "test-svc", t0()),
                    ip_address("10.0.1.2", "test-svc", t0()),
                ],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &["10.0.1.2"],
                actions: &[["delete", "ipaddresses"]],
                events: &["Warning IPAddressWrongReference IPAddress: 10.0.1.2 for Service bar/test-svc has a wrong reference; cleaning up"],
            },
            Case {
                name: "Two IPAddresses referencing the same service, the second in its grace period",
                svcs: vec![svc("test-svc", &["10.0.1.1"], secs(1))],
                ip_addresses: vec![
                    ip_address("10.0.1.1", "test-svc", t0()),
                    ip_address("10.0.1.2", "test-svc", secs(2)),
                ],
                cidrs: k8s(),
                test_time: secs(3),
                expected_ips: &["10.0.1.1", "10.0.1.2"],
                deleted_ips: &[],
                actions: &[],
                events: &[],
            },
            Case {
                name: "Two IPAddresses referencing the same service, the second past its grace period",
                svcs: vec![svc("test-svc", &["10.0.1.1"], secs(1))],
                ip_addresses: vec![
                    ip_address("10.0.1.1", "test-svc", t0()),
                    ip_address("10.0.1.2", "test-svc", secs(2)),
                ],
                cidrs: k8s(),
                test_time: secs(10),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &["10.0.1.2"],
                actions: &[["delete", "ipaddresses"]],
                events: &["Warning IPAddressWrongReference IPAddress: 10.0.1.2 for Service bar/test-svc has a wrong reference; cleaning up"],
            },
            Case {
                name: "Two Services with same ClusterIP",
                svcs: vec![
                    svc("test-svc", &["10.0.1.1"], secs(1)),
                    svc("test-svc2", &["10.0.1.1"], secs(1)),
                ],
                ip_addresses: vec![ip_address("10.0.1.1", "test-svc2", t0())],
                cidrs: k8s(),
                test_time: t0(),
                expected_ips: &["10.0.1.1"],
                deleted_ips: &[],
                actions: &[],
                events: &["Warning ClusterIPAlreadyAllocated Cluster IP [IPv4]:10.0.1.1 was assigned to multiple services; please recreate service"],
            },
        ];
        for tc in cases {
            run(tc).await;
        }
    }

    /// An IPAddress another controller manages is left alone, whatever it
    /// references (`TestRepairIPAddress_syncIPAddress`, "not managed by this
    /// controller").
    #[tokio::test]
    async fn an_ip_address_managed_elsewhere_is_ignored() {
        let storage = Arc::new(MemoryStorage::new());
        let mut ip = ip_address("2001:db8::11", "foo", t0());
        ip.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert(LABEL_MANAGED_BY.to_string(), "controller-foo".to_string());
        storage
            .create(&ip_address_key("2001:db8::11"), &ip)
            .await
            .unwrap();
        let r = RepairIpAddress::new(storage.clone(), FakeClient::new(storage.clone()))
            .with_clock(secs(3600));
        r.sync_ip_address(&ip).await.unwrap();
        assert!(storage
            .get::<IPAddress>(&ip_address_key("2001:db8::11"))
            .await
            .is_ok());
    }

    /// A leaked IPAddress still inside its grace period is requeued for
    /// when the grace ends (`ipQueue.AddAfter(key, gracePeriod-ipLifetime)`,
    /// repairip.go:553).
    #[tokio::test]
    async fn a_young_leaked_ip_address_is_requeued_after_the_grace_period() {
        let storage = Arc::new(MemoryStorage::new());
        let ip = ip_address("2001:db8::12", "gone", t0());
        storage
            .create(&ip_address_key("2001:db8::12"), &ip)
            .await
            .unwrap();
        let r = RepairIpAddress::new(storage.clone(), FakeClient::new(storage.clone()))
            .with_clock(secs(20));
        assert_eq!(
            r.sync_ip_address(&ip).await.unwrap(),
            Some(Duration::from_secs(40))
        );
        assert!(storage
            .get::<IPAddress>(&ip_address_key("2001:db8::12"))
            .await
            .is_ok());
    }

    async fn eventually<F, Fut>(what: &str, mut cond: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        for _ in 0..100 {
            if cond().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for: {what}");
    }

    /// The workers react to an IPAddress event without a `run_once`
    /// (repairip.go:208-216): a leaked IPAddress is deleted.
    #[tokio::test]
    async fn workers_delete_a_leaked_ip_address_on_its_event() {
        let storage = Arc::new(MemoryStorage::new());
        let r = Arc::new(
            RepairIpAddress::new(storage.clone(), FakeClient::new(storage.clone()))
                .with_clock(secs(3600)),
        );
        let handles = r.spawn_workers();
        tokio::time::sleep(Duration::from_millis(100)).await;
        storage
            .create(
                &ip_address_key("2001:db8::13"),
                &ip_address("2001:db8::13", "gone", t0()),
            )
            .await
            .unwrap();
        eventually("leaked IPAddress deleted", || async {
            storage
                .get::<IPAddress>(&ip_address_key("2001:db8::13"))
                .await
                .is_err()
        })
        .await;
        handles.iter().for_each(|h| h.abort());
    }

    /// ...and a Service event creates its missing IPAddress.
    #[tokio::test]
    async fn workers_create_a_missing_ip_address_on_the_service_event() {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create(
                &build_key("servicecidrs", None, DEFAULT_SERVICE_CIDR_NAME),
                &cidr(DEFAULT_SERVICE_CIDR_NAME, &[V4]),
            )
            .await
            .unwrap();
        let r = Arc::new(RepairIpAddress::new(
            storage.clone(),
            FakeClient::new(storage.clone()),
        ));
        let handles = r.spawn_workers();
        tokio::time::sleep(Duration::from_millis(100)).await;
        storage
            .create(&svc_key("bar", "foo"), &svc("foo", &["10.0.0.7"], t0()))
            .await
            .unwrap();
        eventually("IPAddress created", || async {
            storage
                .get::<IPAddress>(&ip_address_key("10.0.0.7"))
                .await
                .is_ok()
        })
        .await;
        handles.iter().for_each(|h| h.abort());
    }
}
