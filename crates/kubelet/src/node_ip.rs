//! kubelet `--node-ip` handling.
//!
//! Ported from:
//! * `staging/src/k8s.io/component-helpers/node/util/ips.go`
//!   (`parseNodeIP` / `ParseNodeIPArgument`, tests in `ips_test.go`)
//! * `pkg/kubelet/nodestatus/setters.go` `NodeAddress` (non-cloud-provider
//!   branch; tests in `setters_test.go` `TestNodeAddress`)
//! * `pkg/kubelet/kubelet_node_status.go` `validateNodeIP` (static checks only;
//!   the "address belongs to a local interface" check is not ported).

use std::net::IpAddr;

/// `parseNodeIP(nodeIP, allowDual, sloppy=true)` via `ParseNodeIPArgument`.
/// Returns `(ips, invalid_tokens)`; invalid tokens are returned for the caller
/// to log and ignore. `cloud_provider` "" or "external" allows dual-stack.
pub fn parse_node_ip_argument(
    _node_ip: &str,
    _cloud_provider: &str,
) -> Result<(Vec<IpAddr>, Vec<String>), String> {
    unimplemented!()
}

/// `validateNodeIP` static checks (kubelet_node_status.go:706-722).
pub fn validate_node_ip(_ip: IpAddr) -> Result<(), String> {
    unimplemented!()
}

/// `NodeAddress` setter, no external cloud provider
/// (setters.go:141-189). Returns `(type, address)` pairs.
/// `lookup` is `net.LookupIP(node.Name)`; `resolve` is
/// `resolveAddressFunc(nodeIP)` (default-route address of the family).
pub fn node_addresses(
    _node_ips: &[IpAddr],
    _hostname: &str,
    _lookup: &dyn Fn(&str) -> Vec<IpAddr>,
    _resolve: &dyn Fn(Option<IpAddr>) -> Option<IpAddr>,
) -> Result<Vec<(String, String)>, String> {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    // ips_test.go TestParseNodeIPArgument
    #[test]
    fn parse_argument_cases() {
        let p = |s: &str| parse_node_ip_argument(s, "");
        assert_eq!(p("").unwrap(), (vec![], vec![]));
        assert_eq!(p(" ").unwrap(), (vec![], vec!["".to_string()]));
        assert_eq!(p("blah").unwrap(), (vec![], vec!["blah".to_string()]));
        assert_eq!(p(" 1.2.3.4   ").unwrap().0, vec![ip("1.2.3.4")]);
        assert_eq!(p("01.2.3.004").unwrap().0, vec![ip("1.2.3.4")]);
        assert_eq!(p("1.2.3").unwrap().1, vec!["1.2.3".to_string()]);
        assert_eq!(p("1.2.3.0/24").unwrap().1, vec!["1.2.3.0/24".to_string()]);
        assert_eq!(p("0.0.0.0").unwrap().0, vec![ip("0.0.0.0")]);
        let r = p("1.2.3.4,not-an-IPv6-address").unwrap();
        assert_eq!(r.0, vec![ip("1.2.3.4")]);
        assert_eq!(r.1, vec!["not-an-IPv6-address".to_string()]);
        assert_eq!(
            p("abcd:0abc:00ab:0000:0000::1").unwrap().0,
            vec![ip("abcd:abc:ab::1")]
        );
        assert_eq!(
            p("1.2.3.4,abcd::ef01").unwrap().0,
            vec![ip("1.2.3.4"), ip("abcd::ef01")]
        );
        assert_eq!(
            p("abcd::ef01 , 1.2.3.4").unwrap().0,
            vec![ip("abcd::ef01"), ip("1.2.3.4")]
        );
        let r = p("abcd::ef01 , 1.2.3.4, something else").unwrap();
        assert_eq!(r.0, vec![ip("abcd::ef01"), ip("1.2.3.4")]);
        assert_eq!(r.1, vec!["something else".to_string()]);
        for bad in [
            "1.2.3.4,5.6.7.8",
            "abcd::1,abcd::2",
            "1.2.3.4,abcd::1,5.6.7.8",
        ] {
            assert!(
                p(bad)
                    .unwrap_err()
                    .contains("either a single IP or a dual-stack pair of IPs"),
                "{bad}"
            );
        }
        for bad in ["1.2.3.4,::", "0.0.0.0,abcd::1"] {
            assert!(
                p(bad)
                    .unwrap_err()
                    .contains("cannot include '0.0.0.0' or '::'"),
                "{bad}"
            );
        }
    }

    #[test]
    fn parse_argument_dual_stack_cloud_provider() {
        assert!(parse_node_ip_argument("1.2.3.4,abcd::ef01", "external").is_ok());
        let e = parse_node_ip_argument("1.2.3.4,abcd::ef01", "aws").unwrap_err();
        assert!(e.contains("not supported in this configuration"));
        assert!(parse_node_ip_argument("1.2.3.4", "aws").is_ok());
    }

    // kubelet_node_status.go validateNodeIP
    #[test]
    fn validate_rejects_special_addresses() {
        for bad in [
            "127.0.0.1",
            "::1",
            "224.0.0.1",
            "ff02::1",
            "169.254.1.1",
            "fe80::1",
            "0.0.0.0",
            "::",
        ] {
            assert!(validate_node_ip(ip(bad)).is_err(), "{bad}");
        }
        assert!(validate_node_ip(ip("10.1.1.1")).is_ok());
        assert!(validate_node_ip(ip("fd00::1")).is_ok());
    }

    fn none(_: &str) -> Vec<IpAddr> {
        vec![]
    }

    // setters_test.go TestNodeAddress: single --node-ip
    #[test]
    fn address_single_node_ip() {
        let r = node_addresses(&[ip("fd00::5")], "h", &none, &|_| None).unwrap();
        assert_eq!(
            r,
            vec![
                ("InternalIP".into(), "fd00::5".into()),
                ("Hostname".into(), "h".into())
            ]
        );
    }

    // setters_test.go: dual --node-ips => two InternalIPs then Hostname
    #[test]
    fn address_dual_stack() {
        let r = node_addresses(&[ip("10.0.0.2"), ip("fd00::2")], "h", &none, &|_| None).unwrap();
        assert_eq!(
            r,
            vec![
                ("InternalIP".into(), "10.0.0.2".into()),
                ("InternalIP".into(), "fd00::2".into()),
                ("Hostname".into(), "h".into())
            ]
        );
    }

    // setters_test.go: invalid (loopback / multicast) node IP errors
    #[test]
    fn address_invalid_node_ip_errors() {
        assert!(node_addresses(&[ip("127.0.0.1")], "h", &none, &|_| None).is_err());
        assert!(node_addresses(&[ip("10.0.0.2"), ip("ff02::1")], "h", &none, &|_| None).is_err());
    }

    // setters_test.go: no nodeIP prefers IPv4 from the DNS lookup
    #[test]
    fn address_no_node_ip_prefers_ipv4_lookup() {
        let lk = |_: &str| vec![ip("fd00::9"), ip("10.0.0.9")];
        let r = node_addresses(&[], "h", &lk, &|_| None).unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "10.0.0.9".into()));
    }

    // setters_test.go: nodeIP "::" prefers IPv6; resolve fallback when lookup empty
    #[test]
    fn address_unspecified_ipv6_prefers_ipv6_and_resolves() {
        let lk = |_: &str| vec![ip("10.0.0.9"), ip("fd00::9")];
        let r = node_addresses(&[ip("::")], "h", &lk, &|_| None).unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "fd00::9".into()));
        let r = node_addresses(&[ip("0.0.0.0")], "h", &none, &|_| Some(ip("10.9.9.9"))).unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "10.9.9.9".into()));
    }

    // setters.go:160: a hostname that is itself an IP is used
    #[test]
    fn address_hostname_ip_literal_used() {
        let r = node_addresses(&[], "10.2.2.2", &none, &|_| None).unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "10.2.2.2".into()));
    }

    // setters.go:181: nothing found => error
    #[test]
    fn address_nothing_found_errors() {
        assert!(node_addresses(&[], "h", &none, &|_| None).is_err());
    }
}
