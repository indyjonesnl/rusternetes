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
/// `ErrorForDisabledProvider` (cloud-provider/plugins.go:94-96).
pub fn validate_cloud_provider(name: &str) -> Result<(), String> {
    if !is_external_cloud_provider(name) && !name.is_empty() {
        return Err(format!(
            "cloud provider {name:?} was specified, but built-in cloud providers are disabled. Please set --cloud-provider=external and migrate to an external cloud provider"
        ));
    }
    Ok(())
}

/// `RunKubelet`'s `ParseNodeIPArgument(kubeServer.NodeIP,
/// kubeServer.CloudProvider)` (cmd/kubelet/app/server.go:1216-1222) followed
/// by `NewMainKubelet`'s cloud-provider gate and
/// `externalCloudProvider = cloudprovider.IsExternal(cloudProvider)`
/// (kubelet.go:461-464, 619). Returns `(nodeIPs, externalCloudProvider)`;
/// unparsable tokens are logged and ignored.
pub fn node_ip_config(node_ip: &str, cloud_provider: &str) -> Result<(Vec<IpAddr>, bool), String> {
    let (ips, invalid) = parse_node_ip_argument(node_ip, cloud_provider)
        .map_err(|e| format!("bad --node-ip {node_ip:?}: {e}"))?;
    if !invalid.is_empty() {
        tracing::warn!("Could not parse some node IP(s), ignoring them: {invalid:?}");
    }
    validate_cloud_provider(cloud_provider)?;
    Ok((ips, is_external_cloud_provider(cloud_provider)))
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

/// `nodeutil.GetHostname` (staging/src/k8s.io/component-helpers/node/util/
/// hostname.go:28-46). `os_hostname` is `os.Hostname`.
pub fn get_hostname(
    hostname_override: &str,
    os_hostname: &dyn Fn() -> std::io::Result<String>,
) -> Result<String, String> {
    let host_name = if hostname_override.is_empty() {
        os_hostname().map_err(|e| format!("couldn't determine hostname: {e}"))?
    } else {
        hostname_override.to_string()
    };
    // Trim whitespaces first to avoid getting an empty hostname.
    let host_name = host_name.trim();
    if host_name.is_empty() {
        return Err("empty hostname is invalid".into());
    }
    Ok(host_name.to_lowercase())
}

/// `utilnode.GetNodeHostIPs` (pkg/util/node/node.go:65-98).
pub fn get_node_host_ips(
    addresses: &[rusternetes_common::resources::NodeAddress],
) -> Result<Vec<IpAddr>, String> {
    // Re-sort the addresses with InternalIPs first and then ExternalIPs.
    let mut all_ips: Vec<IpAddr> = Vec::with_capacity(addresses.len());
    for kind in ["InternalIP", "ExternalIP"] {
        all_ips.extend(
            addresses
                .iter()
                .filter(|a| a.address_type == kind)
                .filter_map(|a| parse_ip_sloppy(&a.address)),
        );
    }
    let Some(&first) = all_ips.first() else {
        let known: Vec<String> = addresses
            .iter()
            .map(|a| format!("{{{} {}}}", a.address_type, a.address))
            .collect();
        return Err(format!(
            "host IP unknown; known addresses: [{}]",
            known.join(" ")
        ));
    };
    let mut node_ips = vec![first];
    if let Some(&other) = all_ips.iter().find(|ip| ip.is_ipv6() != first.is_ipv6()) {
        node_ips.push(other);
    }
    Ok(node_ips)
}

/// The host-IP block of `generateAPIPodStatus`
/// (pkg/kubelet/kubelet_pods.go:2005-2040), applied to `s` once
/// `getHostIPsAnyWay` succeeded with `host_ips`. Returns the
/// `HostIPsIPFamilyMismatch` event message when upstream would record one.
pub fn apply_host_ips(
    host_network: bool,
    s: &mut rusternetes_common::resources::pod::PodStatus,
    host_ips: &[IpAddr],
) -> Option<String> {
    use rusternetes_common::resources::pod::{HostIP, PodIP};
    // utilnet.IPFamilyOfString: "4" / "6", or "" when unparseable.
    let family_of_str = |ip: &str| match parse_ip_sloppy(ip) {
        Some(ip) if ip.is_ipv4() => "4",
        Some(_) => "6",
        None => "",
    };
    let family = |ip: &IpAddr| if ip.is_ipv4() { "4" } else { "6" };
    let primary = *host_ips.first()?;
    let mut mismatch = None;
    if let Some(old) = s.host_ip.as_deref().filter(|h| !h.is_empty()) {
        if family_of_str(old) != family(&primary) {
            mismatch = Some(format!(
                "Kubelet detected an IPv{} node IP ({}), but the cloud provider selected an IPv{} node IP ({}); pass an explicit `--node-ip` to kubelet to fix this.",
                family_of_str(old),
                old,
                family(&primary),
                primary
            ));
        }
    }
    s.host_ip = Some(primary.to_string());
    let mut hips = vec![HostIP {
        ip: primary.to_string(),
    }];
    if host_ips.len() == 2 {
        hips.push(HostIP {
            ip: host_ips[1].to_string(),
        });
    }
    s.host_i_ps = Some(hips);

    // HostNetwork Pods inherit the node IPs as PodIPs. They are immutable once
    // set, other than that if the node becomes dual-stack, we add the secondary IP.
    if host_network {
        if s.pod_ip.as_deref().unwrap_or("").is_empty() {
            s.pod_ip = Some(primary.to_string());
            s.pod_i_ps = Some(vec![PodIP {
                ip: primary.to_string(),
            }]);
        }
        // Secondary IP is not set #105320
        if host_ips.len() == 2 {
            if let Some(pod_ips) = s.pod_i_ps.as_mut().filter(|p| p.len() == 1) {
                if family_of_str(&pod_ips[0].ip) != family(&host_ips[1]) {
                    pod_ips.push(PodIP {
                        ip: host_ips[1].to_string(),
                    });
                }
            }
        }
    }
    mismatch
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    // hostname_test.go TestGetHostname
    #[test]
    fn get_hostname_cases() {
        let os = || Ok("OsHost".to_string());
        assert_eq!(get_hostname("overridden", &os).unwrap(), "overridden");
        assert_eq!(get_hostname("OVERRIDDEN", &os).unwrap(), "overridden");
        assert_eq!(get_hostname(" OVERRIDDEN ", &os).unwrap(), "overridden");
        assert_eq!(
            get_hostname(" ", &os).unwrap_err(),
            "empty hostname is invalid"
        );
        // empty parameter: os.Hostname(), lowercased.
        assert_eq!(get_hostname("", &os).unwrap(), "oshost");
        // os.Hostname failure.
        let bad = || Err(std::io::Error::other("boom"));
        assert!(get_hostname("", &bad)
            .unwrap_err()
            .starts_with("couldn't determine hostname: "));
        // os hostname that is only whitespace.
        let blank = || Ok("  ".to_string());
        assert_eq!(
            get_hostname("", &blank).unwrap_err(),
            "empty hostname is invalid"
        );
    }

    // pkg/util/node/node_test.go TestGetNodeHostIPs
    #[test]
    fn get_node_host_ips_cases() {
        let a = |t: &str, v: &str| rusternetes_common::resources::NodeAddress {
            address_type: t.to_string(),
            address: v.to_string(),
        };
        let ips = |l: &[&str]| l.iter().map(|s| ip(s)).collect::<Vec<_>>();
        assert!(get_node_host_ips(&[]).is_err());
        assert!(get_node_host_ips(&[a("Hostname", "example.com")]).is_err());
        let err = get_node_host_ips(&[a("Hostname", "example.com")]).unwrap_err();
        assert!(
            err.starts_with("host IP unknown; known addresses: "),
            "{err}"
        );
        // IPv4-only, simple
        assert_eq!(
            get_node_host_ips(&[
                a("InternalIP", "1.2.3.4"),
                a("ExternalIP", "4.3.2.1"),
                a("ExternalIP", "4.3.2.2")
            ])
            .unwrap(),
            ips(&["1.2.3.4"])
        );
        // external-first
        assert_eq!(
            get_node_host_ips(&[
                a("ExternalIP", "4.3.2.1"),
                a("ExternalIP", "4.3.2.2"),
                a("InternalIP", "1.2.3.4")
            ])
            .unwrap(),
            ips(&["1.2.3.4"])
        );
        // no internal
        assert_eq!(
            get_node_host_ips(&[a("ExternalIP", "4.3.2.1"), a("ExternalIP", "4.3.2.2")]).unwrap(),
            ips(&["4.3.2.1"])
        );
        // dual-stack node
        assert_eq!(
            get_node_host_ips(&[
                a("InternalIP", "1.2.3.4"),
                a("ExternalIP", "4.3.2.1"),
                a("ExternalIP", "4.3.2.2"),
                a("InternalIP", "a:b::c:d"),
                a("ExternalIP", "d:c::b:a")
            ])
            .unwrap(),
            ips(&["1.2.3.4", "a:b::c:d"])
        );
        // dual-stack node, IPv6-first, no internal IPv4
        assert_eq!(
            get_node_host_ips(&[
                a("InternalIP", "a:b::c:d"),
                a("ExternalIP", "d:c::b:a"),
                a("ExternalIP", "4.3.2.1"),
                a("ExternalIP", "4.3.2.2")
            ])
            .unwrap(),
            ips(&["a:b::c:d", "4.3.2.1"])
        );
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
    fn addresses_without_node_ip_use_ipv6_only_lookup() {
        let lk = |_: &str| vec![ip("fd00::9")];
        let got = node_addresses(&[], &validate_node_ip, "h", &lk, &|_| {
            Err("no default route".to_string())
        })
        .unwrap();
        assert_eq!(got[0], ("InternalIP".to_string(), "fd00::9".to_string()));
    }

    #[test]
    fn addresses_without_node_ip_prefer_ipv4() {
        let lk = |_: &str| vec![ip("fd00::9"), ip("10.0.0.9")];
        let got = node_addresses(&[], &validate_node_ip, "h", &lk, &|_| {
            Err("no default route".to_string())
        })
        .unwrap();
        assert_eq!(got[0], ("InternalIP".to_string(), "10.0.0.9".to_string()));
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

    // server.go:1216-1222 + kubelet.go:461-464, 619
    #[test]
    fn node_ip_config_parses_and_gates_cloud_provider() {
        assert_eq!(node_ip_config("", "").unwrap(), (vec![], false));
        assert_eq!(
            node_ip_config("10.1.1.1,fd00::1", "external").unwrap(),
            (vec![ip("10.1.1.1"), ip("fd00::1")], true)
        );
        assert!(node_ip_config("10.1.1.1", "aws")
            .unwrap_err()
            .contains("built-in cloud providers are disabled"));
        // ParseNodeIPArgument rejects dual-stack for a legacy provider first.
        assert!(node_ip_config("10.1.1.1,fd00::1", "aws")
            .unwrap_err()
            .contains("dual-stack not supported in this configuration"));
        assert!(node_ip_config("10.1.1.1,10.1.1.2", "")
            .unwrap_err()
            .starts_with("bad --node-ip"));
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

    fn status() -> rusternetes_common::resources::pod::PodStatus {
        Default::default()
    }

    fn pod_ips(s: &rusternetes_common::resources::pod::PodStatus) -> Vec<String> {
        s.pod_i_ps
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|p| p.ip)
            .collect()
    }

    // kubelet_pods_test.go TestGenerateAPIPodStatusHostNetworkPodIPs
    #[test]
    fn host_network_pod_ips_inherit_node_ips() {
        use rusternetes_common::resources::pod::PodIP;
        // (node host IPs, CRI pod IPs, expected PodIPs)
        let cases: Vec<(Vec<&str>, Vec<&str>, Vec<&str>)> = vec![
            (vec!["10.0.0.1"], vec![], vec!["10.0.0.1"]),
            (
                vec!["10.0.0.1", "fd01::1234"],
                vec![],
                vec!["10.0.0.1", "fd01::1234"],
            ),
            // CRI PodIPs override NodeAddresses; the secondary is appended
            (
                vec!["10.0.0.1", "fd01::1234"],
                vec!["192.168.0.1"],
                vec!["192.168.0.1", "fd01::1234"],
            ),
            (
                vec!["10.0.0.1", "fd01::1234"],
                vec!["192.168.0.1", "2001:db8::2"],
                vec!["192.168.0.1", "2001:db8::2"],
            ),
        ];
        for (host, cri, want) in cases {
            let host: Vec<IpAddr> = host.into_iter().map(ip).collect();
            let mut s = status();
            if !cri.is_empty() {
                s.pod_ip = Some(cri[0].to_string());
                s.pod_i_ps = Some(cri.iter().map(|i| PodIP { ip: i.to_string() }).collect());
            }
            apply_host_ips(true, &mut s, &host);
            assert_eq!(pod_ips(&s), want, "{host:?} {cri:?}");
            assert_eq!(s.pod_ip.as_deref(), Some(want[0]));
            assert_eq!(s.host_ip, Some(host[0].to_string()));
            assert_eq!(s.host_i_ps.unwrap().len(), host.len());
        }
    }

    #[test]
    fn non_host_network_pod_does_not_inherit_node_ips() {
        let mut s = status();
        apply_host_ips(false, &mut s, &[ip("10.0.0.1")]);
        assert_eq!(s.host_ip.as_deref(), Some("10.0.0.1"));
        assert!(s.pod_ip.is_none() && s.pod_i_ps.is_none());
    }

    // pod IPs are immutable once set; only the secondary is added (#105320)
    #[test]
    fn host_network_pod_ips_immutable_but_gain_secondary() {
        use rusternetes_common::resources::pod::PodIP;
        let mut s = status();
        s.pod_ip = Some("10.0.0.1".into());
        s.pod_i_ps = Some(vec![PodIP {
            ip: "10.0.0.1".into(),
        }]);
        apply_host_ips(true, &mut s, &[ip("1.1.1.1")]);
        assert_eq!(pod_ips(&s), vec!["10.0.0.1"]);
        apply_host_ips(true, &mut s, &[ip("1.1.1.1"), ip("2001:db8::2")]);
        assert_eq!(pod_ips(&s), vec!["10.0.0.1", "2001:db8::2"]);
    }

    // kubelet_pods.go:2011-2017 HostIPsIPFamilyMismatch
    #[test]
    fn host_ip_family_mismatch_message() {
        let mut s = status();
        s.host_ip = Some("10.0.0.1".into());
        let msg = apply_host_ips(false, &mut s, &[ip("fd00::1")]).unwrap();
        assert_eq!(
            msg,
            "Kubelet detected an IPv4 node IP (10.0.0.1), but the cloud provider selected an IPv6 node IP (fd00::1); pass an explicit `--node-ip` to kubelet to fix this."
        );
        assert_eq!(s.host_ip.as_deref(), Some("fd00::1"));
        let mut s = status();
        s.host_ip = Some("10.0.0.1".into());
        assert!(apply_host_ips(false, &mut s, &[ip("10.0.0.2")]).is_none());
        assert!(apply_host_ips(false, &mut status(), &[ip("10.0.0.2")]).is_none());
    }
}
