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

pub mod net_interface;

/// `netutils.ParseIPSloppy`: like `ParseIP` but tolerates leading zeros in
/// IPv4 octets ("01.2.3.004").
pub(crate) fn parse_ip_sloppy(s: &str) -> Option<IpAddr> {
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

/// `cloudproviderapi.AnnotationAlphaProvidedIPAddr`
/// (staging/src/k8s.io/cloud-provider/api/well_known_annotations.go:25).
pub const ANNOTATION_ALPHA_PROVIDED_IP_ADDR: &str = "alpha.kubernetes.io/provided-node-ip";

/// `cloudproviderapi.TaintExternalCloudProvider`
/// (staging/src/k8s.io/cloud-provider/api/well_known_taints.go:24).
pub const TAINT_EXTERNAL_CLOUD_PROVIDER: &str = "node.cloudprovider.kubernetes.io/uninitialized";

/// `cloudprovider.IsExternal` (staging/src/k8s.io/cloud-provider/plugins.go:79).
pub fn is_external_cloud_provider(name: &str) -> bool {
    name == "external"
}

/// `NewMainKubelet` cloud-provider gate (kubelet.go:461-464): only "" and
/// "external" are accepted; any other name is an
/// `ErrorForDisabledProvider` (cloud-provider/plugins.go:94-96). STUB.
pub fn validate_cloud_provider(_name: &str) -> Result<(), String> {
    Ok(())
}

/// `nodestatus.NodeAddress` Setter (setters.go:62-135): mutates `node`'s
/// annotations and `status.addresses`, or returns an error after possibly
/// partially mutating the node (the Go Setter contract).
/// `lookup` is `net.LookupIP(node.Name)`; `resolve` is
/// `resolveAddressFunc(nodeIP)` (default-route address of the family).
pub fn set_node_address(
    node: &mut rusternetes_common::resources::Node,
    node_ips: &[IpAddr],
    validate: &dyn Fn(IpAddr) -> Result<(), String>,
    hostname: &str,
    external_cloud_provider: bool,
    lookup: &dyn Fn(&str) -> Vec<IpAddr>,
    resolve: &dyn Fn(Option<IpAddr>) -> Result<IpAddr, String>,
) -> Result<(), String> {
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

    if let (true, true, Some(nip)) = (external_cloud_provider, node_ip_specified, node_ip) {
        // Annotate the Node object with nodeIP for external cloud provider.
        let mut annotation = nip.to_string();
        if let (true, Some(sec)) = (secondary_specified, secondary) {
            annotation.push(',');
            annotation.push_str(&sec.to_string());
        }
        node.metadata
            .annotations
            .get_or_insert_with(Default::default)
            .insert(ANNOTATION_ALPHA_PROVIDED_IP_ADDR.to_string(), annotation);
    } else if let Some(annotations) = node.metadata.annotations.as_mut() {
        // Clean up stale annotations if no longer using a cloud provider or
        // no longer overriding node IP.
        annotations.remove(ANNOTATION_ALPHA_PROVIDED_IP_ADDR);
    }

    let status = node.status.get_or_insert_with(Default::default);
    let internal = |a: IpAddr| rusternetes_common::resources::NodeAddress {
        address_type: "InternalIP".to_string(),
        address: a.to_string(),
    };
    let host = rusternetes_common::resources::NodeAddress {
        address_type: "Hostname".to_string(),
        address: hostname.to_string(),
    };

    if external_cloud_provider {
        // If --cloud-provider=external and node address is already set, then
        // we return early because provider set addresses should take precedence.
        if status.addresses.as_ref().is_some_and(|a| !a.is_empty()) {
            return Ok(());
        }
        // If nodeIPs are not set wait for the external cloud-provider to set
        // the node addresses.
        if node_ip.is_none() {
            status.addresses = Some(vec![host]);
            return Ok(());
        }
    }
    if let (true, true, Some(a), Some(b)) =
        (node_ip_specified, secondary_specified, node_ip, secondary)
    {
        status.addresses = Some(vec![internal(a), internal(b), host]);
        return Ok(());
    }
    let node_name = node.metadata.name.clone();

    // 1) nodeIP if set (and not 0.0.0.0/::)  2) hostname that is an IP
    // 3) DNS lookup of the node name (preferred family first)
    // 4) default-route address
    let mut ip_addr: Option<IpAddr> = None;
    // Go's shared `err` variable: set by validateNodeIPFunc in the lookup
    // loop, overwritten by resolveAddressFunc (setters.go:164-179).
    let mut err: Option<String> = None;
    if node_ip_specified {
        ip_addr = node_ip;
    } else if let Some(a) = parse_ip_sloppy(hostname) {
        ip_addr = Some(a);
    } else {
        for a in lookup(&node_name) {
            err = validate(a).err();
            if err.is_none() {
                if a.is_ipv4() == prefer_ipv4 {
                    ip_addr = Some(a);
                    break;
                } else if ip_addr.is_none() {
                    ip_addr = Some(a);
                }
            }
        }
        if ip_addr.is_none() {
            match resolve(node_ip) {
                Ok(a) => {
                    ip_addr = Some(a);
                    err = None;
                }
                Err(e) => err = Some(e),
            }
        }
    }
    let ip_addr = ip_addr.ok_or_else(|| {
        format!(
            "can't get ip address of node {node_name}. error: {}",
            err.as_deref().unwrap_or("<nil>")
        )
    })?;
    status.addresses = Some(vec![internal(ip_addr), host]);
    Ok(())
}

/// The setter without an external cloud provider, on a scratch node named
/// `hostname`; returns `(type, address)` pairs.
#[cfg(test)]
pub fn node_addresses(
    node_ips: &[IpAddr],
    validate: &dyn Fn(IpAddr) -> Result<(), String>,
    hostname: &str,
    lookup: &dyn Fn(&str) -> Vec<IpAddr>,
    resolve: &dyn Fn(Option<IpAddr>) -> Result<IpAddr, String>,
) -> Result<Vec<(String, String)>, String> {
    let mut node = rusternetes_common::resources::Node::new(hostname);
    set_node_address(
        &mut node, node_ips, validate, hostname, false, lookup, resolve,
    )?;
    Ok(node
        .status
        .and_then(|s| s.addresses)
        .unwrap_or_default()
        .into_iter()
        .map(|a| (a.address_type, a.address))
        .collect())
}

/// The InternalIP list the kubelet advertises: the `NodeAddress` setter
/// result, falling back to `detect` (the legacy single-IPv4 path) when the
/// setter errors or yields no InternalIP. `Never empty`.
#[allow(clippy::too_many_arguments)]
pub fn effective_internal_ips(
    configured: &[IpAddr],
    validate: &dyn Fn(IpAddr) -> Result<(), String>,
    hostname: &str,
    external_cloud_provider: bool,
    lookup: &dyn Fn(&str) -> Vec<IpAddr>,
    resolve: &dyn Fn(Option<IpAddr>) -> Result<IpAddr, String>,
    detect: &dyn Fn() -> String,
) -> Vec<String> {
    // Upstream runs the setter whether or not --node-ip is set
    // (pkg/kubelet/nodestatus/setters.go:70-96, 141-189): with no node IP it
    // looks up the node name, preferring IPv4 but accepting a valid IPv6.
    let mut node = rusternetes_common::resources::Node::new(hostname);
    if set_node_address(
        &mut node,
        configured,
        validate,
        hostname,
        external_cloud_provider,
        lookup,
        resolve,
    )
    .is_ok()
    {
        let ips: Vec<String> = node
            .status
            .and_then(|s| s.addresses)
            .unwrap_or_default()
            .into_iter()
            .filter(|a| a.address_type == "InternalIP")
            .map(|a| a.address)
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
        let r = node_addresses(&[ip("fd00::5")], &validate_node_ip, "h", &none, &|_| {
            Err("no default route".to_string())
        })
        .unwrap();
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
            &|_| Err("no default route".to_string()),
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
            node_addresses(&[ip("127.0.0.1")], &validate_node_ip, "h", &none, &|_| Err(
                "no default route".to_string()
            ))
            .is_err()
        );
        assert!(node_addresses(
            &[ip("10.0.0.2"), ip("ff02::1")],
            &validate_node_ip,
            "h",
            &none,
            &|_| Err("no default route".to_string())
        )
        .is_err());
    }

    // setters_test.go: no nodeIP prefers IPv4 from the DNS lookup
    #[test]
    fn address_no_node_ip_prefers_ipv4_lookup() {
        let lk = |_: &str| vec![ip("fd00::9"), ip("10.0.0.9")];
        let r = node_addresses(&[], &validate_node_ip, "h", &lk, &|_| {
            Err("no default route".to_string())
        })
        .unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "10.0.0.9".into()));
    }

    // setters_test.go: nodeIP "::" prefers IPv6; resolve fallback when lookup empty
    #[test]
    fn address_unspecified_ipv6_prefers_ipv6_and_resolves() {
        let lk = |_: &str| vec![ip("10.0.0.9"), ip("fd00::9")];
        let r = node_addresses(&[ip("::")], &validate_node_ip, "h", &lk, &|_| {
            Err("no default route".to_string())
        })
        .unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "fd00::9".into()));
        let r = node_addresses(&[ip("0.0.0.0")], &validate_node_ip, "h", &none, &|_| {
            Ok(ip("10.9.9.9"))
        })
        .unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "10.9.9.9".into()));
    }

    // setters.go:160: a hostname that is itself an IP is used
    #[test]
    fn address_hostname_ip_literal_used() {
        let r = node_addresses(&[], &validate_node_ip, "10.2.2.2", &none, &|_| {
            Err("no default route".to_string())
        })
        .unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "10.2.2.2".into()));
    }

    // setters.go:181: nothing found => error
    #[test]
    fn address_nothing_found_errors() {
        assert!(node_addresses(&[], &validate_node_ip, "h", &none, &|_| Err(
            "no default route".to_string()
        ))
        .is_err());
    }

    // setters.go:183 `can't get ip address of node %s. error: %v`
    #[test]
    fn address_nothing_found_error_text_carries_resolver_error() {
        let e = node_addresses(&[], &validate_node_ip, "h", &none, &|_| {
            Err("no default route".to_string())
        })
        .unwrap_err();
        assert_eq!(e, "can't get ip address of node h. error: no default route");
    }

    // setters.go:163-179 runs with NO --node-ip too: an IPv6-only host (DNS
    // yields only a valid v6 address) advertises it instead of 127.0.0.1 (#3186).
    #[test]
    fn effective_ips_without_node_ip_uses_ipv6_only_lookup() {
        let lk = |_: &str| vec![ip("fd00::9")];
        let got = effective_internal_ips(
            &[],
            &validate_node_ip,
            "h",
            false,
            &lk,
            &|_| Err("no default route".to_string()),
            &|| "127.0.0.1".to_string(),
        );
        assert_eq!(got, vec!["fd00::9".to_string()]);
    }

    #[test]
    fn effective_ips_without_node_ip_prefers_ipv4_and_falls_back_to_detect() {
        let lk = |_: &str| vec![ip("fd00::9"), ip("10.0.0.9")];
        let d = || "127.0.0.1".to_string();
        assert_eq!(
            effective_internal_ips(
                &[],
                &validate_node_ip,
                "h",
                false,
                &lk,
                &|_| Err("no default route".to_string()),
                &d
            ),
            vec!["10.0.0.9".to_string()]
        );
        assert_eq!(
            effective_internal_ips(
                &[],
                &validate_node_ip,
                "h",
                false,
                &none,
                &|_| Err("no default route".to_string()),
                &d
            ),
            vec!["127.0.0.1".to_string()]
        );
    }

    // kubelet.go:461-464 + plugins.go:94-96
    #[test]
    fn cloud_provider_must_be_empty_or_external() {
        assert!(validate_cloud_provider("").is_ok());
        assert!(validate_cloud_provider("external").is_ok());
        assert_eq!(
            validate_cloud_provider("aws").unwrap_err(),
            "cloud provider \"aws\" was specified, but built-in cloud providers are disabled. Please set --cloud-provider=external and migrate to an external cloud provider"
        );
        assert!(is_external_cloud_provider("external"));
        assert!(!is_external_cloud_provider(""));
    }

    // ---- setters_test.go TestNodeAddress (cloud-provider cases) ----

    type TestNode = rusternetes_common::resources::Node;

    fn setter_node(annotations: Option<&[(&str, &str)]>, existing: Option<&str>) -> TestNode {
        let mut n = TestNode::new("test-node");
        n.metadata.annotations = annotations.map(|a| {
            a.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        });
        let status = n.status.get_or_insert_with(Default::default);
        status.addresses = Some(match existing {
            Some(a) => vec![rusternetes_common::resources::NodeAddress {
                address_type: String::new(),
                address: a.to_string(),
            }],
            None => vec![],
        });
        n
    }

    fn addrs_of(n: &TestNode) -> Vec<(String, String)> {
        n.status
            .as_ref()
            .and_then(|s| s.addresses.clone())
            .unwrap_or_default()
            .into_iter()
            .map(|a| (a.address_type, a.address))
            .collect()
    }

    fn ann_of(n: &TestNode) -> std::collections::HashMap<String, String> {
        n.metadata.annotations.clone().unwrap_or_default()
    }

    /// Run the setter the way TestNodeAddress does: validator always ok, DNS
    /// lookup yielding nothing, `resolve` returning `resolved`.
    fn run_setter(
        node: &mut TestNode,
        ips: &[&str],
        cloud: bool,
        resolved: Option<&str>,
    ) -> Result<(), String> {
        let ips: Vec<IpAddr> = ips.iter().map(|s| ip(s)).collect();
        let r = resolved.map(ip);
        set_node_address(
            node,
            &ips,
            &|_| Ok(()),
            "test-hostname",
            cloud,
            &none,
            &|_| r.ok_or_else(|| "no resolved ip".to_string()),
        )
    }

    fn pair(t: &str, a: &str) -> (String, String) {
        (t.to_string(), a.to_string())
    }

    #[test]
    fn setter_cloud_provider_and_node_ip_specified() {
        let mut n = setter_node(None, None);
        run_setter(&mut n, &["10.0.0.1"], true, None).unwrap();
        assert_eq!(
            addrs_of(&n),
            vec![
                pair("InternalIP", "10.0.0.1"),
                pair("Hostname", "test-hostname")
            ]
        );
    }

    #[test]
    fn setter_unspecified_node_ip_with_cloud_provider_resolves() {
        for (nip, res) in [("0.0.0.0", "10.0.0.2"), ("::", "2001:db2::2")] {
            for cloud in [false, true] {
                let mut n = setter_node(None, None);
                run_setter(&mut n, &[nip], cloud, Some(res)).unwrap();
                assert_eq!(
                    addrs_of(&n),
                    vec![pair("InternalIP", res), pair("Hostname", "test-hostname")],
                    "{nip} cloud={cloud}"
                );
            }
        }
    }

    #[test]
    fn setter_no_node_ip_resolve() {
        for res in ["10.0.0.2", "2001:db2::2"] {
            let mut n = setter_node(None, None);
            run_setter(&mut n, &[], false, Some(res)).unwrap();
            assert_eq!(
                addrs_of(&n),
                vec![pair("InternalIP", res), pair("Hostname", "test-hostname")]
            );
            // "using cloud provider and no nodeIP resolve": hostname only.
            let mut n = setter_node(None, None);
            run_setter(&mut n, &[], true, Some(res)).unwrap();
            assert_eq!(addrs_of(&n), vec![pair("Hostname", "test-hostname")]);
        }
    }

    #[test]
    fn setter_cloud_provider_gets_node_ip_annotation() {
        let mut n = setter_node(None, None);
        run_setter(&mut n, &["10.1.1.1"], true, None).unwrap();
        assert_eq!(
            ann_of(&n),
            [(
                ANNOTATION_ALPHA_PROVIDED_IP_ADDR.to_string(),
                "10.1.1.1".to_string()
            )]
            .into()
        );
    }

    #[test]
    fn setter_cloud_provider_and_address_already_set_returns_early() {
        let mut n = setter_node(None, Some("10.1.1.2"));
        run_setter(&mut n, &["10.1.1.1"], true, None).unwrap();
        assert_eq!(addrs_of(&n), vec![pair("", "10.1.1.2")]);
    }

    #[test]
    fn setter_no_cloud_provider_no_annotation() {
        let mut n = setter_node(Some(&[]), None);
        run_setter(&mut n, &["10.1.1.1"], false, None).unwrap();
        assert!(ann_of(&n).is_empty());
        assert_eq!(
            addrs_of(&n),
            vec![
                pair("InternalIP", "10.1.1.1"),
                pair("Hostname", "test-hostname")
            ]
        );
    }

    #[test]
    fn setter_stale_annotation_removed_without_cloud_provider() {
        let mut n = setter_node(
            Some(&[(ANNOTATION_ALPHA_PROVIDED_IP_ADDR, "10.1.1.3")]),
            None,
        );
        run_setter(&mut n, &["10.1.1.1"], false, None).unwrap();
        assert!(ann_of(&n).is_empty());
    }

    #[test]
    fn setter_incorrect_annotation_fixed() {
        let mut n = setter_node(
            Some(&[(ANNOTATION_ALPHA_PROVIDED_IP_ADDR, "10.1.1.3")]),
            None,
        );
        run_setter(&mut n, &["10.1.1.1"], true, None).unwrap();
        assert_eq!(ann_of(&n)[ANNOTATION_ALPHA_PROVIDED_IP_ADDR], "10.1.1.1");
    }

    #[test]
    fn setter_dual_stack_cloud_annotation() {
        let mut n = setter_node(None, None);
        run_setter(
            &mut n,
            &["2600:1f14:1d4:d101::ba3d", "10.1.1.2"],
            true,
            None,
        )
        .unwrap();
        assert_eq!(
            addrs_of(&n),
            vec![
                pair("InternalIP", "2600:1f14:1d4:d101::ba3d"),
                pair("InternalIP", "10.1.1.2"),
                pair("Hostname", "test-hostname")
            ]
        );
        assert_eq!(
            ann_of(&n)[ANNOTATION_ALPHA_PROVIDED_IP_ADDR],
            "2600:1f14:1d4:d101::ba3d,10.1.1.2"
        );
    }

    #[test]
    fn setter_upgrade_and_downgrade_cloud_dual_stack() {
        let mut n = setter_node(
            Some(&[(ANNOTATION_ALPHA_PROVIDED_IP_ADDR, "10.1.1.1")]),
            None,
        );
        run_setter(
            &mut n,
            &["10.1.1.1", "2600:1f14:1d4:d101::ba3d"],
            true,
            None,
        )
        .unwrap();
        assert_eq!(
            ann_of(&n)[ANNOTATION_ALPHA_PROVIDED_IP_ADDR],
            "10.1.1.1,2600:1f14:1d4:d101::ba3d"
        );
        let mut n = setter_node(
            Some(&[(
                ANNOTATION_ALPHA_PROVIDED_IP_ADDR,
                "10.1.1.1,2600:1f14:1d4:d101::ba3d",
            )]),
            None,
        );
        run_setter(&mut n, &["10.1.1.1"], true, None).unwrap();
        assert_eq!(ann_of(&n)[ANNOTATION_ALPHA_PROVIDED_IP_ADDR], "10.1.1.1");
    }

    // TestNodeAddress_NoCloudProvider: invalid IPs error, addresses stay unset.
    #[test]
    fn setter_invalid_node_ips_error() {
        let mut n = setter_node(None, None);
        let e = set_node_address(
            &mut n,
            &[ip("127.0.0.1")],
            &validate_node_ip,
            "h",
            false,
            &none,
            &|_| Err("not reached".into()),
        )
        .unwrap_err();
        assert!(e.starts_with("failed to validate nodeIP:"), "{e}");
        let e = set_node_address(
            &mut n,
            &[ip("10.1.1.1"), ip("ff02::1")],
            &validate_node_ip,
            "h",
            false,
            &none,
            &|_| Err("not reached".into()),
        )
        .unwrap_err();
        assert!(e.starts_with("failed to validate secondaryNodeIP:"), "{e}");
        assert!(addrs_of(&n).is_empty());
    }
}
