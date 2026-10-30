//! Unit tests moved from `src/registry/core/service/alloc.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::core::service::ipallocator::cidr::MetaAllocator;
    pub use rusternetes_api_server::registry::core::service::ipallocator::IpError;
    pub use rusternetes_api_server::registry::core::service::portallocator::operation::PortAllocationOperation;
    pub use rusternetes_api_server::registry::core::service::portallocator::{
        PortAllocator, PortError,
    };
    pub use rusternetes_api_server::registry::rest::internal_error;
    pub use rusternetes_common::resources::{
        IPFamily, IPFamilyPolicy, Service, ServiceExternalTrafficPolicy, ServiceType,
    };
    pub use rusternetes_common::validation::field;
    pub use rusternetes_common::validation::service::{
        parse_ip_sloppy, validate_service_cluster_ips_related_fields,
    };
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::Storage;
    pub use std::net::IpAddr;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::service::alloc::*;
    use rusternetes_api_server::registry::core::service::portallocator::PortRange;
    use serde_json::json;

    fn pa() -> Arc<PortAllocator> {
        Arc::new(PortAllocator::new_in_memory(
            PortRange::parse("30000-32767").unwrap(),
        ))
    }

    fn svc(spec: serde_json::Value) -> Service {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "foo", "namespace": "default"},
            "spec": spec,
        }))
        .unwrap()
    }

    fn ports(s: &Service) -> Vec<usize> {
        s.spec
            .ports
            .iter()
            .map(|p| node_port(p.node_port))
            .collect()
    }

    async fn create(pa: &Arc<PortAllocator>, s: &mut Service) -> Result<()> {
        let op = txn_alloc_node_ports(pa, s, false).await?;
        settle(op, &Ok(())).await;
        Ok(())
    }

    /// The allocation-side cases of `TestCreateInitNodePorts`
    /// (storage_test.go:6176). The remaining cases fail validation first.
    #[tokio::test]
    async fn create_init_node_ports() {
        struct Case {
            name: &'static str,
            spec: serde_json::Value,
            expect_error: bool,
            expect_node_ports: bool,
        }
        let tcp = |name: &str, port: u16| json!({"name": name, "port": port, "protocol": "TCP"});
        let udp = |name: &str, port: u16| json!({"name": name, "port": port, "protocol": "UDP"});
        let cases = [
            Case {
                name: "type:ClusterIP",
                spec: json!({"type": "ClusterIP", "ports": [tcp("p", 80)]}),
                expect_error: false,
                expect_node_ports: false,
            },
            Case {
                name: "type:NodePort_single_port_unspecified",
                spec: json!({"type": "NodePort", "ports": [tcp("p", 80)]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:NodePort_single_port_specified",
                spec: json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30001}]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:NodePort_multiport_unspecified",
                spec: json!({"type": "NodePort", "ports": [tcp("p", 80), tcp("q", 443)]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:NodePort_multiport_multiproto_unspecified",
                spec: json!({"type": "NodePort", "ports": [tcp("p", 53), udp("q", 53)]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:NodePort_multiport_multiproto_same",
                spec: json!({"type": "NodePort", "ports": [
                    {"name": "p", "port": 53, "protocol": "TCP", "nodePort": 30053},
                    {"name": "q", "port": 53, "protocol": "UDP", "nodePort": 30053}]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:NodePort_multiport_multiproto_conflict",
                spec: json!({"type": "NodePort", "ports": [
                    {"name": "p", "port": 93, "protocol": "TCP", "nodePort": 30093},
                    {"name": "q", "port": 76, "protocol": "UDP", "nodePort": 30093}]}),
                expect_error: true,
                expect_node_ports: false,
            },
            Case {
                name: "type:LoadBalancer_single_port_unspecified:on_alloc:false",
                spec: json!({"type": "LoadBalancer", "allocateLoadBalancerNodePorts": false, "ports": [tcp("p", 80)]}),
                expect_error: false,
                expect_node_ports: false,
            },
            Case {
                name: "type:LoadBalancer_single_port_unspecified:on_alloc:true",
                spec: json!({"type": "LoadBalancer", "allocateLoadBalancerNodePorts": true, "ports": [tcp("p", 80)]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:LoadBalancer_single_port_specified:on_alloc:false",
                spec: json!({"type": "LoadBalancer", "allocateLoadBalancerNodePorts": false,
                    "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30002}]}),
                expect_error: false,
                expect_node_ports: true,
            },
            Case {
                name: "type:LoadBalancer_multiport_multiproto_same",
                spec: json!({"type": "LoadBalancer", "ports": [
                    {"name": "p", "port": 53, "protocol": "TCP", "nodePort": 30054},
                    {"name": "q", "port": 53, "protocol": "UDP", "nodePort": 30054}]}),
                expect_error: false,
                expect_node_ports: true,
            },
        ];

        for tc in cases {
            let pa = pa();
            let requested = svc(tc.spec.clone());
            let mut s = requested.clone();
            let res = create(&pa, &mut s).await;
            if tc.expect_error {
                assert!(res.is_err(), "{}: expected an error", tc.name);
                assert!(pa.allocated().await.is_empty(), "{}: rolled back", tc.name);
                continue;
            }
            res.unwrap_or_else(|e| panic!("{}: {e}", tc.name));
            let got = ports(&s);
            let allocated: Vec<_> = got.iter().filter(|p| **p != 0).collect();
            if !tc.expect_node_ports {
                assert!(allocated.is_empty(), "{}: got {got:?}", tc.name);
                continue;
            }
            assert_eq!(allocated.len(), got.len(), "{}: {got:?}", tc.name);
            for p in &allocated {
                assert!(pa.has(**p).await, "{}: {p} not allocated", tc.name);
            }
            for (i, want) in ports(&requested).into_iter().enumerate() {
                if want != 0 {
                    assert_eq!(got[i], want, "{}: Ports[{i}]", tc.name);
                }
            }
            // Unique, except the same Port shared across protocols.
            let mut seen = std::collections::HashMap::new();
            for (i, p) in s.spec.ports.iter().enumerate() {
                if let Some(prev) = seen.insert(got[i], p.port) {
                    assert_eq!(prev, p.port, "{}: non-unique Ports[{i}]", tc.name);
                }
            }
        }
    }

    #[tokio::test]
    async fn a_node_port_in_use_is_rejected() {
        let pa = pa();
        let spec = json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30100}]});
        create(&pa, &mut svc(spec.clone())).await.unwrap();
        let err = create(&pa, &mut svc(spec)).await.unwrap_err();
        // `NewInvalid(api.Kind("Service"), service.Name, el)` (alloc.go:536).
        let Error::Status(st) = err else {
            panic!("want a NewInvalid Status, got {err:?}")
        };
        let d = st.details.as_ref().unwrap();
        assert_eq!(d.kind.as_deref(), Some("Service"));
        assert_eq!(d.name.as_deref(), Some("foo"));
        let c = &d.causes.as_ref().unwrap()[0];
        assert_eq!(c.field.as_deref(), Some("spec.ports[0].nodePort"));
        let msg = st.message.as_deref().unwrap();
        assert!(
            msg.starts_with(
                "Service \"foo\" is invalid: spec.ports[0].nodePort: Invalid value: 30100: "
            ),
            "{msg}"
        );
        assert!(msg.contains("provided port is already allocated"), "{msg}");
    }

    #[tokio::test]
    async fn a_failed_write_rolls_the_allocation_back() {
        let pa = pa();
        let mut s = svc(json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP"}]}));
        let op = txn_alloc_node_ports(&pa, &mut s, false).await.unwrap();
        assert_eq!(pa.allocated().await.len(), 1);
        settle(op, &Err::<(), _>(Error::Internal("boom".into()))).await;
        assert!(pa.allocated().await.is_empty());
    }

    #[tokio::test]
    async fn a_dry_run_claims_nothing() {
        let pa = pa();
        let mut s = svc(json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP"}]}));
        let op = txn_alloc_node_ports(&pa, &mut s, true).await.unwrap();
        settle(op, &Ok(())).await;
        assert_ne!(ports(&s)[0], 0);
        assert!(pa.allocated().await.is_empty());
    }

    #[tokio::test]
    async fn a_local_load_balancer_gets_a_health_check_node_port() {
        let pa = pa();
        let mut s = svc(
            json!({"type": "LoadBalancer", "externalTrafficPolicy": "Local",
            "ports": [{"port": 80, "protocol": "TCP"}]}),
        );
        create(&pa, &mut s).await.unwrap();
        let hc = s.spec.health_check_node_port.unwrap() as usize;
        assert!(pa.has(hc).await);
        release_node_ports(&pa, &s).await;
        assert!(pa.allocated().await.is_empty());
    }

    /// `txnUpdateNodePorts` (alloc.go:754-795).
    #[tokio::test]
    async fn update_moves_and_frees_node_ports() {
        let pa = pa();
        let mut old = svc(
            json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30200}]}),
        );
        create(&pa, &mut old).await.unwrap();

        // Changing the requested port claims the new one and frees the old
        // one — on commit only.
        let mut new = svc(
            json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30201}]}),
        );
        let op = txn_update_node_ports(&pa, &mut new, &old, false)
            .await
            .unwrap();
        assert!(pa.has(30200).await, "the release is deferred to commit");
        settle(op, &Ok(())).await;
        assert_eq!(pa.allocated().await, vec![30201]);

        // Keeping a port does not re-allocate it.
        let mut same = new.clone();
        let op = txn_update_node_ports(&pa, &mut same, &new, false)
            .await
            .unwrap();
        settle(op, &Ok(())).await;
        assert_eq!(pa.allocated().await, vec![30201]);

        // NodePort -> ClusterIP frees it.
        let mut cip = svc(json!({"type": "ClusterIP", "ports": [{"port": 80, "protocol": "TCP"}]}));
        let op = txn_update_node_ports(&pa, &mut cip, &same, false)
            .await
            .unwrap();
        settle(op, &Ok(())).await;
        assert!(pa.allocated().await.is_empty());

        // ClusterIP -> NodePort allocates.
        let mut np = svc(json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP"}]}));
        let op = txn_update_node_ports(&pa, &mut np, &cip, false)
            .await
            .unwrap();
        settle(op, &Ok(())).await;
        assert_eq!(pa.allocated().await, ports(&np));
    }

    #[tokio::test]
    async fn update_to_a_port_in_use_is_rejected_and_keeps_the_old_one() {
        let pa = pa();
        create(
            &pa,
            &mut svc(json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30300}]})),
        )
        .await
        .unwrap();
        let mut old = svc(
            json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30301}]}),
        );
        create(&pa, &mut old).await.unwrap();

        let mut new = svc(
            json!({"type": "NodePort", "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30300}]}),
        );
        let err = txn_update_node_ports(&pa, &mut new, &old, false)
            .await
            .err()
            .expect("the port is taken");
        assert!(
            matches!(&err, Error::Status(st) if st.message.as_deref().is_some_and(|m| m.starts_with("Service \"foo\" is invalid: "))),
            "{err:?}"
        );
        assert_eq!(pa.allocated().await, vec![30300, 30301]);
    }

    #[tokio::test]
    async fn update_to_local_traffic_policy_allocates_and_back_frees() {
        let pa = pa();
        let mut old = svc(
            json!({"type": "LoadBalancer", "externalTrafficPolicy": "Cluster",
            "ports": [{"port": 80, "protocol": "TCP", "nodePort": 30400}]}),
        );
        create(&pa, &mut old).await.unwrap();

        let mut local = old.clone();
        local.spec.external_traffic_policy = Some(ServiceExternalTrafficPolicy::Local);
        let op = txn_update_node_ports(&pa, &mut local, &old, false)
            .await
            .unwrap();
        settle(op, &Ok(())).await;
        let hc = local.spec.health_check_node_port.expect("allocated") as usize;
        assert!(pa.has(hc).await);

        let mut cluster = local.clone();
        cluster.spec.external_traffic_policy = Some(ServiceExternalTrafficPolicy::Cluster);
        let op = txn_update_node_ports(&pa, &mut cluster, &local, false)
            .await
            .unwrap();
        settle(op, &Ok(())).await;
        assert!(!pa.has(hc).await);
    }

    /// `txnAllocClusterIPs` (alloc.go:305-312) returns `nil, err` when
    /// `allocClusterIPs` fails and never releases what `allocIPs`
    /// (alloc.go:395-451) already took for an earlier family: the repair loop
    /// reclaims it. A dual-stack create whose IPv6 family cannot be served
    /// therefore leaves the IPv4 address allocated.
    #[tokio::test]
    async fn failed_cluster_ip_alloc_leaves_partial_allocation_to_repair() {
        use rusternetes_common::resources::ServiceCIDR;
        use rusternetes_storage::{build_key, MemoryStorage};

        let storage = Arc::new(MemoryStorage::new());
        // Only the IPv4 family has a ServiceCIDR, so the IPv6 allocation
        // fails after the IPv4 one succeeded.
        let sc = ServiceCIDR::new("v4", vec!["10.0.0.0/24".to_string()]);
        storage
            .create(&build_key("servicecidrs", None, "v4"), &sc)
            .await
            .unwrap();
        let pa = Arc::new(ClusterIpAllocators::new(
            storage,
            &[IPFamily::IPv4, IPFamily::IPv6],
        ));
        let mut s = svc(json!({
            "type": "ClusterIP",
            "ipFamilies": ["IPv4", "IPv6"],
            "ports": [{"port": 80, "protocol": "TCP"}],
        }));
        assert!(txn_alloc_cluster_ips(&pa, &mut s, false).await.is_err());
        let v4 = pa.get(&IPFamily::IPv4).unwrap();
        assert_eq!(v4.used().await, 1, "the partial allocation is not released");
    }

    /// `apierrors.NewInvalid(api.Kind("Service"), service.Name, el)`
    /// (alloc.go:156, 229, 281, 420, 444, 536, 561, 824): every Invalid the
    /// allocator returns names the Service.
    #[test]
    fn invalid_helpers_name_the_service() {
        let el = vec![field::Error::required(&field::Path::new("spec"), "")];
        let Error::Status(st) = invalid_service(&svc(json!({})), el) else {
            panic!("want a NewInvalid Status")
        };
        assert_eq!(
            st.message.as_deref(),
            Some("Service \"foo\" is invalid: spec: Required value")
        );
        assert_eq!(st.details.unwrap().kind.as_deref(), Some("Service"));

        let s = svc(json!({"clusterIP": "10.0.0.1"}));
        let Error::Status(st) = invalid_cluster_ips(&s, "boom".into()) else {
            panic!("want a NewInvalid Status")
        };
        assert_eq!(
            st.message.as_deref(),
            Some(
                "Service \"foo\" is invalid: spec.clusterIPs: Invalid value: [\"10.0.0.1\"]: boom"
            )
        );
    }
}
