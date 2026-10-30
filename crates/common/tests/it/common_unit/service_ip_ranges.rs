//! Unit tests moved from `src/service_ip_ranges.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use ipnet::IpNet;
    pub use rusternetes_common::resources::IPFamily;
    pub use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::service_ip_ranges::*;

    #[test]
    fn empty_is_the_default_single_stack_range() {
        let r = ServiceIpRanges::parse("").unwrap();
        assert_eq!(r.cidrs(), vec!["10.96.0.0/12"]);
        assert_eq!(r.families(), vec![IPFamily::IPv4]);
        assert_eq!(r, ServiceIpRanges::default());
    }

    #[test]
    fn a_dual_stack_pair_keeps_its_order() {
        let r = ServiceIpRanges::parse("10.96.0.0/12,2001:db8:1::/112").unwrap();
        assert_eq!(r.cidrs(), vec!["10.96.0.0/12", "2001:db8:1::/112"]);
        assert_eq!(r.families(), vec![IPFamily::IPv4, IPFamily::IPv6]);
    }

    #[test]
    fn rejects_what_upstream_rejects() {
        let err = |s: &str| ServiceIpRanges::parse(s).unwrap_err();
        assert!(err("10.96.0.0/12,fd00::/112,10.0.0.0/16").contains("more than two entries"));
        assert!(err("nope").contains("[0] is not a valid cidr"));
        assert!(err("10.96.0.0/12,nope").contains("[1] is not an ip net"));
        assert!(err("10.96.0.0/12,10.0.0.0/16").contains("must be of different IP family"));
        assert!(err("10.96.0.0/30").contains("at least 8 IP addresses"));
    }

    /// `TestGetServiceIPAndRanges` (cmd/kube-apiserver/app/options/
    /// completion_test.go:23-67): (flag, apiServerServiceIP, primary, secondary).
    #[test]
    fn upstream_get_service_ip_and_ranges_table() {
        let r = ServiceIpRanges::parse("192.0.2.1/24").unwrap();
        assert_eq!(r.api_server_service_ip().to_string(), "192.0.2.1");
        assert_eq!(r.cidrs(), vec!["192.0.2.0/24"]);
        // (Upstream's IPv4+IPv4 row is omitted: validation.go:67-76 rejects it
        // and the table does not exercise validation.)
        let r = ServiceIpRanges::parse("192.0.2.1/24,2001:db2:1:3:4::1/112").unwrap();
        assert_eq!(r.api_server_service_ip().to_string(), "192.0.2.1");
        assert_eq!(r.cidrs(), vec!["192.0.2.0/24", "2001:db2:1:3:4::/112"]);
        // IPv6-primary dual-stack.
        let r = ServiceIpRanges::parse("2001:db2:1:3:4::1/112,192.0.2.1/24").unwrap();
        assert_eq!(r.api_server_service_ip().to_string(), "2001:db2:1:3:4::1");
        assert_eq!(r.cidrs(), vec!["2001:db2:1:3:4::/112", "192.0.2.0/24"]);
        assert_eq!(r.families(), vec![IPFamily::IPv6, IPFamily::IPv4]);
        assert_eq!(
            ServiceIpRanges::parse("fd00::/112").unwrap().families()[0],
            IPFamily::IPv6
        );
        for bad in [
            "192.0.2.1/30,192.168.128.0/17",
            "192.0.2.1/33,192.168.128.0/17",
            "192.0.2.1/24,192.168.128.0/33",
            "2001:db2:1:3:4::1/129,192.0.2.1/24",
            "192.0.2.1/24,2001:db2:1:3:4::1/129",
            "192.0.2.1,192.168.128.0/17",
            "192.0.2.1/24,192.168.128.1",
            "2001:db2:1:3:4::1,192.0.2.1/24",
            "192.0.2.1/24,2001:db2:1:3:4::1",
            "bad.ip.range,192.168.0.2/24",
            "192.168.0.2/24,bad.ip.range",
        ] {
            assert!(ServiceIpRanges::parse(bad).is_err(), "{bad}");
        }
    }
}
