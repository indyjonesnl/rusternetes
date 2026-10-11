//! kubelet `--node-ip` handling.
//!
//! Ported from:
//! * `staging/src/k8s.io/component-helpers/node/util/ips.go`
//!   (`parseNodeIP` / `ParseNodeIPArgument`, tests in `ips_test.go`)
//! * `pkg/kubelet/nodestatus/setters.go` `NodeAddress` (non-cloud-provider
//!   branch; tests in `setters_test.go` `TestNodeAddress`)
//! * `pkg/kubelet/kubelet_node_status.go` `validateNodeIP` (static checks only;
//!   the "address belongs to a local interface" check is not ported).

use std::net::{IpAddr, Ipv4Addr};

static CONFIGURED_NODE_IPS: std::sync::OnceLock<Vec<IpAddr>> = std::sync::OnceLock::new();

/// Record the parsed `--node-ip` (upstream threads `nodeIPs` through
/// `createAndInitKubelet`; we keep it process-wide so the many
/// `Kubelet::new*` constructors do not change). First call wins.
pub fn set_configured_node_ips(ips: Vec<IpAddr>) {
    let _ = CONFIGURED_NODE_IPS.set(ips);
}

/// The parsed `--node-ip` values (empty when the flag is unset).
pub fn configured_node_ips() -> &'static [IpAddr] {
    CONFIGURED_NODE_IPS.get().map(Vec::as_slice).unwrap_or(&[])
}

/// `netutils.ParseIPSloppy`: like `ParseIP` but tolerates leading zeros in
/// IPv4 octets ("01.2.3.004").
fn parse_ip_sloppy(s: &str) -> Option<IpAddr> {
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Some(ip);
    }
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 4 {
        return None;
    }
    let mut o = [0u8; 4];
    for (i, p) in parts.iter().enumerate() {
        if p.is_empty() || !p.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        o[i] = p.parse::<u8>().ok()?;
    }
    Some(IpAddr::V4(Ipv4Addr::from(o)))
}

/// `parseNodeIP(nodeIP, allowDual, sloppy=true)` via `ParseNodeIPArgument`
/// (ips.go:33-75). Returns `(ips, invalid_tokens)`; invalid tokens are
/// returned for the caller to log and ignore. `cloud_provider` "" or
/// "external" allows dual-stack.
pub fn parse_node_ip_argument(
    node_ip: &str,
    cloud_provider: &str,
) -> Result<(Vec<IpAddr>, Vec<String>), String> {
    let allow_dual = cloud_provider.is_empty() || cloud_provider == "external";
    let mut ips: Vec<IpAddr> = Vec::new();
    let mut invalid: Vec<String> = Vec::new();
    if !node_ip.is_empty() {
        for tok in node_ip.split(',') {
            let tok = tok.trim();
            match parse_ip_sloppy(tok) {
                Some(ip) => ips.push(ip),
                None => invalid.push(tok.to_string()),
            }
        }
    }
    if ips.len() > 2 || (ips.len() == 2 && ips[0].is_ipv6() == ips[1].is_ipv6()) {
        return Err("must contain either a single IP or a dual-stack pair of IPs".into());
    } else if ips.len() == 2 && !allow_dual {
        return Err("dual-stack not supported in this configuration".into());
    } else if ips.len() == 2 && (ips[0].is_unspecified() || ips[1].is_unspecified()) {
        return Err("dual-stack node IP cannot include '0.0.0.0' or '::'".into());
    }
    Ok((ips, invalid))
}

/// `validateNodeIP` static checks (kubelet_node_status.go:706-722).
pub fn validate_node_ip(ip: IpAddr) -> Result<(), String> {
    if ip.is_loopback() {
        return Err("nodeIP can't be loopback address".into());
    }
    if ip.is_multicast() {
        return Err("nodeIP can't be a multicast address".into());
    }
    let link_local = match ip {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) == 0xfe80,
    };
    if link_local {
        return Err("nodeIP can't be a link-local unicast address".into());
    }
    if ip.is_unspecified() {
        return Err("nodeIP can't be an all zeros address".into());
    }
    Ok(())
}

/// `Kubelet.nodeIPValidator` = `validateNodeIP` (kubelet_node_status.go:706-740):
/// the static checks plus "the IP belongs to a host interface".
pub fn validate_node_ip_on_host(ip: IpAddr) -> Result<(), String> {
    validate_node_ip(ip)?;
    // `net.InterfaceAddrs()` + `ip.Equal(nodeIP)`.
    if host_interface_ips()?.contains(&ip) {
        return Ok(());
    }
    Err(format!(
        "node IP: \"{ip}\" not found in the host's network interfaces"
    ))
}

/// `net.InterfaceAddrs()`: every unicast address of every host interface
/// (getifaddrs(3), as Go's netlink/`interfaceAddrTable` does on Linux).
fn host_interface_ips() -> Result<Vec<IpAddr>, String> {
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: getifaddrs fills `head` with a list that freeifaddrs releases.
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    let mut out = Vec::new();
    let mut cur = head;
    while !cur.is_null() {
        // SAFETY: `cur` is a live node of the list returned above.
        let ifa = unsafe { &*cur };
        if !ifa.ifa_addr.is_null() {
            // SAFETY: ifa_addr points at a sockaddr whose family tags its
            // concrete type (sockaddr_in / sockaddr_in6).
            match i32::from(unsafe { (*ifa.ifa_addr).sa_family }) {
                libc::AF_INET => {
                    let sa = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in) };
                    out.push(IpAddr::V4(std::net::Ipv4Addr::from(u32::from_be(
                        sa.sin_addr.s_addr,
                    ))));
                }
                libc::AF_INET6 => {
                    let sa = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in6) };
                    out.push(IpAddr::V6(std::net::Ipv6Addr::from(sa.sin6_addr.s6_addr)));
                }
                _ => {}
            }
        }
        cur = ifa.ifa_next;
    }
    // SAFETY: `head` came from getifaddrs and is freed exactly once.
    unsafe { libc::freeifaddrs(head) };
    Ok(out)
}

/// `NodeAddress` setter, no external cloud provider
/// (setters.go:70-96, 141-189). Returns `(type, address)` pairs.
/// `lookup` is `net.LookupIP(node.Name)`; `resolve` is
/// `resolveAddressFunc(nodeIP)` (default-route address of the family).
pub fn node_addresses(
    node_ips: &[IpAddr],
    validate: &dyn Fn(IpAddr) -> Result<(), String>,
    hostname: &str,
    lookup: &dyn Fn(&str) -> Vec<IpAddr>,
    resolve: &dyn Fn(Option<IpAddr>) -> Option<IpAddr>,
) -> Result<Vec<(String, String)>, String> {
    let node_ip = node_ips.first().copied();
    let prefer_ipv4 = node_ip.is_none_or(|ip| ip.is_ipv4());
    let node_ip_specified = node_ip.is_some_and(|ip| !ip.is_unspecified());
    let secondary = node_ips.get(1).copied();
    let secondary_specified = secondary.is_some_and(|ip| !ip.is_unspecified());

    if let (true, Some(ip)) = (node_ip_specified, node_ip) {
        validate(ip).map_err(|e| format!("failed to validate nodeIP: {e}"))?;
    }
    if let (true, Some(ip)) = (secondary_specified, secondary) {
        validate(ip).map_err(|e| format!("failed to validate secondaryNodeIP: {e}"))?;
    }

    let internal = |a: IpAddr| ("InternalIP".to_string(), a.to_string());
    let host = ("Hostname".to_string(), hostname.to_string());
    if let (true, true, Some(a), Some(b)) =
        (node_ip_specified, secondary_specified, node_ip, secondary)
    {
        return Ok(vec![internal(a), internal(b), host]);
    }

    // 1) nodeIP if set (and not 0.0.0.0/::)  2) hostname that is an IP
    // 3) DNS lookup of the node name (preferred family first)
    // 4) default-route address
    let mut ip_addr: Option<IpAddr> = None;
    if node_ip_specified {
        ip_addr = node_ip;
    } else if let Some(a) = parse_ip_sloppy(hostname) {
        ip_addr = Some(a);
    } else {
        for a in lookup(hostname) {
            if validate(a).is_ok() {
                if a.is_ipv4() == prefer_ipv4 {
                    ip_addr = Some(a);
                    break;
                } else if ip_addr.is_none() {
                    ip_addr = Some(a);
                }
            }
        }
        if ip_addr.is_none() {
            ip_addr = resolve(node_ip);
        }
    }
    let ip_addr = ip_addr.ok_or_else(|| format!("can't get ip address of node {hostname}"))?;
    Ok(vec![internal(ip_addr), host])
}

/// The InternalIP list the kubelet advertises: the `NodeAddress` setter
/// result, falling back to `detect` (the legacy single-IPv4 path) when the
/// setter errors or yields no InternalIP. `Never empty`.
pub fn effective_internal_ips(
    configured: &[IpAddr],
    validate: &dyn Fn(IpAddr) -> Result<(), String>,
    hostname: &str,
    lookup: &dyn Fn(&str) -> Vec<IpAddr>,
    resolve: &dyn Fn(Option<IpAddr>) -> Option<IpAddr>,
    detect: &dyn Fn() -> String,
) -> Vec<String> {
    // Upstream runs the setter whether or not --node-ip is set
    // (pkg/kubelet/nodestatus/setters.go:70-96, 141-189): with no node IP it
    // looks up the node name, preferring IPv4 but accepting a valid IPv6.
    if let Ok(addrs) = node_addresses(configured, validate, hostname, lookup, resolve) {
        let ips: Vec<String> = addrs
            .into_iter()
            .filter(|(t, _)| t == "InternalIP")
            .map(|(_, a)| a)
            .collect();
        if !ips.is_empty() {
            return ips;
        }
    }
    vec![detect()]
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

    // kubelet_node_status_test.go TestValidateNodeIPParam: an address that
    // is not on any host interface is rejected (cases "IPv4 address that
    // doesn't belong to host").
    #[test]
    fn host_validator_rejects_ip_not_on_host_interfaces() {
        let e = validate_node_ip_on_host(ip("1.2.3.4")).unwrap_err();
        assert!(
            e.contains("not found in the host's network interfaces"),
            "{e}"
        );
        assert!(validate_node_ip_on_host(ip("127.0.0.1")).is_err());
    }

    // Same test: every real interface address validates.
    #[test]
    fn host_validator_accepts_interface_addresses() {
        for a in host_interface_ips().unwrap() {
            if validate_node_ip(a).is_ok() {
                assert!(validate_node_ip_on_host(a).is_ok(), "{a}");
            }
        }
    }

    fn none(_: &str) -> Vec<IpAddr> {
        vec![]
    }

    // setters_test.go TestNodeAddress: single --node-ip
    #[test]
    fn address_single_node_ip() {
        let r = node_addresses(&[ip("fd00::5")], &validate_node_ip, "h", &none, &|_| None).unwrap();
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
        let r = node_addresses(
            &[ip("10.0.0.2"), ip("fd00::2")],
            &validate_node_ip,
            "h",
            &none,
            &|_| None,
        )
        .unwrap();
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
        assert!(
            node_addresses(&[ip("127.0.0.1")], &validate_node_ip, "h", &none, &|_| None).is_err()
        );
        assert!(node_addresses(
            &[ip("10.0.0.2"), ip("ff02::1")],
            &validate_node_ip,
            "h",
            &none,
            &|_| None
        )
        .is_err());
    }

    // setters_test.go: no nodeIP prefers IPv4 from the DNS lookup
    #[test]
    fn address_no_node_ip_prefers_ipv4_lookup() {
        let lk = |_: &str| vec![ip("fd00::9"), ip("10.0.0.9")];
        let r = node_addresses(&[], &validate_node_ip, "h", &lk, &|_| None).unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "10.0.0.9".into()));
    }

    // setters_test.go: nodeIP "::" prefers IPv6; resolve fallback when lookup empty
    #[test]
    fn address_unspecified_ipv6_prefers_ipv6_and_resolves() {
        let lk = |_: &str| vec![ip("10.0.0.9"), ip("fd00::9")];
        let r = node_addresses(&[ip("::")], &validate_node_ip, "h", &lk, &|_| None).unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "fd00::9".into()));
        let r = node_addresses(&[ip("0.0.0.0")], &validate_node_ip, "h", &none, &|_| {
            Some(ip("10.9.9.9"))
        })
        .unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "10.9.9.9".into()));
    }

    // setters.go:160: a hostname that is itself an IP is used
    #[test]
    fn address_hostname_ip_literal_used() {
        let r = node_addresses(&[], &validate_node_ip, "10.2.2.2", &none, &|_| None).unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "10.2.2.2".into()));
    }

    // setters.go:181: nothing found => error
    #[test]
    fn address_nothing_found_errors() {
        assert!(node_addresses(&[], &validate_node_ip, "h", &none, &|_| None).is_err());
    }

    // setters.go:163-179 runs with NO --node-ip too: an IPv6-only host (DNS
    // yields only a valid v6 address) advertises it instead of 127.0.0.1 (#3186).
    #[test]
    fn effective_ips_without_node_ip_uses_ipv6_only_lookup() {
        let lk = |_: &str| vec![ip("fd00::9")];
        let got = effective_internal_ips(&[], &validate_node_ip, "h", &lk, &|_| None, &|| {
            "127.0.0.1".to_string()
        });
        assert_eq!(got, vec!["fd00::9".to_string()]);
    }

    #[test]
    fn effective_ips_without_node_ip_prefers_ipv4_and_falls_back_to_detect() {
        let lk = |_: &str| vec![ip("fd00::9"), ip("10.0.0.9")];
        let d = || "127.0.0.1".to_string();
        assert_eq!(
            effective_internal_ips(&[], &validate_node_ip, "h", &lk, &|_| None, &d),
            vec!["10.0.0.9".to_string()]
        );
        assert_eq!(
            effective_internal_ips(&[], &validate_node_ip, "h", &none, &|_| None, &d),
            vec!["127.0.0.1".to_string()]
        );
    }
}
