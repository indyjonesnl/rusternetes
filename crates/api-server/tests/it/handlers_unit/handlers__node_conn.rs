//! Unit tests moved from `src/handlers/node_conn.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::Node;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::node_conn::*;
    use rusternetes_common::resources::{
        DaemonEndpoint, Node, NodeAddress, NodeDaemonEndpoints, NodeStatus,
    };

    fn make_node(addresses: Vec<NodeAddress>, kubelet_port: Option<i32>) -> Node {
        let mut node = Node::new("test-node");
        node.status = Some(NodeStatus {
            addresses: Some(addresses),
            daemon_endpoints: kubelet_port.map(|p| NodeDaemonEndpoints {
                kubelet_endpoint: Some(DaemonEndpoint { port: p }),
            }),
            ..Default::default()
        });
        node
    }

    #[test]
    fn prefers_internal_ip_and_uses_advertised_port() {
        let node = make_node(
            vec![
                NodeAddress {
                    address_type: "ExternalIP".to_string(),
                    address: "1.2.3.4".to_string(),
                },
                NodeAddress {
                    address_type: "InternalIP".to_string(),
                    address: "192.168.1.10".to_string(),
                },
            ],
            Some(10255),
        );

        let conn = node_conn(&node, None).expect("should resolve");
        assert_eq!(conn.host, "192.168.1.10");
        assert_eq!(conn.port, 10255);
        assert_eq!(conn.scheme, "https");
    }

    #[test]
    fn falls_back_to_external_ip_when_no_internal() {
        let node = make_node(
            vec![NodeAddress {
                address_type: "ExternalIP".to_string(),
                address: "5.6.7.8".to_string(),
            }],
            None,
        );

        let conn = node_conn(&node, None).expect("should resolve");
        assert_eq!(conn.host, "5.6.7.8");
        assert_eq!(conn.port, 10250); // default
    }

    #[test]
    fn port_override_wins_over_advertised_port() {
        let node = make_node(
            vec![NodeAddress {
                address_type: "InternalIP".to_string(),
                address: "10.0.0.1".to_string(),
            }],
            Some(10255),
        );

        let conn = node_conn(&node, Some(9090)).expect("should resolve");
        assert_eq!(conn.port, 9090);
    }

    #[test]
    fn no_address_returns_not_found() {
        let node = make_node(vec![], None);
        let err = node_conn(&node, None).unwrap_err();
        assert!(
            matches!(err, rusternetes_common::Error::NotFound(_)),
            "expected NotFound, got: {err:?}"
        );
    }

    #[test]
    fn no_status_returns_not_found() {
        let node = Node::new("bare-node");
        let err = node_conn(&node, None).unwrap_err();
        assert!(matches!(err, rusternetes_common::Error::NotFound(_)));
    }

    #[test]
    fn scheme_is_always_https() {
        let node = make_node(
            vec![NodeAddress {
                address_type: "InternalIP".to_string(),
                address: "10.0.0.2".to_string(),
            }],
            None,
        );
        let conn = node_conn(&node, None).expect("should resolve");
        assert_eq!(conn.scheme, "https");
    }
}
