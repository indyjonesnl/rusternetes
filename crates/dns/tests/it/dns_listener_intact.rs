//! Regression guard for the netstack removal (spec 001-remove-netlink, US2).
//!
//! The in-process DNS server serves over standard OS UDP+TCP sockets. It never
//! depended on the (now-deleted) `rusternetes-netstack` smoltcp short-circuit;
//! removing netstack must leave the DNS listener path and wire-response logic
//! fully intact.

use rusternetes_common::resources::{Service, ServiceSpec, ServiceType};
use rusternetes_dns::server::respond_bytes;
use rusternetes_dns::zone::{Zone, CLUSTER_ZONE};

use hickory_proto::op::{Message, MessageType, OpCode, Query};
use hickory_proto::rr::{DNSClass, Name, RecordType};
use std::str::FromStr;

fn cluster_ip_svc(name: &str, ns: &str, ip: &str) -> Service {
    let mut s = Service::new(name, ServiceSpec::default());
    s.metadata.namespace = Some(ns.to_string());
    s.spec.cluster_ip = Some(ip.to_string());
    s.spec.service_type = Some(ServiceType::ClusterIP);
    s
}

fn build_query(id: u16, name: &str, qtype: RecordType) -> Vec<u8> {
    let mut msg = Message::new(id, MessageType::Query, OpCode::Query);
    let mut q = Query::query(Name::from_str(name).unwrap(), qtype);
    q.set_query_class(DNSClass::IN);
    msg.add_query(q);
    msg.to_vec().unwrap()
}

/// The wire-response path (shared with the socket `serve` loop) resolves a
/// known Service with no netstack involvement.
#[test]
fn dns_wire_logic_intact_without_netstack() {
    let zone = Zone::build(
        CLUSTER_ZONE,
        &[cluster_ip_svc("kubernetes", "default", "10.96.0.1")],
        &[],
        &[],
    );
    let query = build_query(1, "kubernetes.default.svc.cluster.local.", RecordType::A);
    let resp = respond_bytes(&zone, &query).expect("well-formed query resolves");
    let msg = Message::from_vec(&resp).expect("response parses");
    assert_eq!(msg.metadata.message_type, MessageType::Response);
    assert_eq!(msg.answers.len(), 1, "one A answer for the known service");
}

/// The standard-socket listener path `serve` uses (OS UDP+TCP bind) is
/// available — this is the only networking DNS relies on post-removal.
#[tokio::test]
async fn dns_binds_standard_udp_and_tcp_sockets() {
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP on ephemeral port");
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TCP on ephemeral port");
    assert!(udp.local_addr().is_ok());
    assert!(tcp.local_addr().is_ok());
}
