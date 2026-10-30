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
pub fn host_interface_ips() -> Result<Vec<IpAddr>, String> {
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

/// RunKubelet's identity derivation (cmd/kubelet/app/server.go:1208-1211):
/// `hostname, err := nodeutil.GetHostname(kubeServer.HostnameOverride)`, then
/// `nodeName := types.NodeName(hostname)`. Returns `(node_name, hostname)`,
/// always equal: upstream has no separate node-name input (no cloud provider
/// is consulted here, as built-in providers are removed).
pub fn resolve_node_identity(
    hostname_override: Option<&str>,
    os_hostname: &dyn Fn() -> std::io::Result<String>,
) -> Result<(String, String), String> {
    let hostname = get_hostname(hostname_override.unwrap_or(""), os_hostname)?;
    Ok((hostname.clone(), hostname))
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
