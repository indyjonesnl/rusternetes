//! Port of `staging/src/k8s.io/apimachinery/pkg/util/net/interface.go`
//! (release-1.35): `ChooseHostInterface`, `ResolveBindAddress`,
//! `ChooseBindAddressForInterface` and the default-route parsing they rest on.
//! Tests are ported from `interface_test.go` in the same directory.
//!
//! This is the `resolveAddressFunc` of the kubelet `NodeAddress` setter
//! (`pkg/kubelet/nodestatus/setters.go:62-67`, wired to
//! `netutils.ResolveBindAddress` in `pkg/kubelet/kubelet.go`).

// The whole upstream helper surface is ported; the kubelet binary uses only
// `resolve_bind_address`.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use super::parse_ip_sloppy;

/// `AddressFamily` (interface.go:35-38); the value is the IP version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AddressFamily {
    V4 = 4,
    V6 = 6,
}

/// `AddressFamilyPreference` (interface.go:41).
pub type AddressFamilyPreference = [AddressFamily; 2];

/// interface.go:43-46 `preferIPv4` / `preferIPv6`.
pub const PREFER_IPV4: AddressFamilyPreference = [AddressFamily::V4, AddressFamily::V6];
pub const PREFER_IPV6: AddressFamilyPreference = [AddressFamily::V6, AddressFamily::V4];

/// interface.go:48-51.
pub const LOOPBACK_INTERFACE_NAME: &str = "lo";

/// interface.go:53-56.
const IPV4_ROUTE_FILE: &str = "/proc/net/route";
const IPV6_ROUTE_FILE: &str = "/proc/net/ipv6_route";

/// `net.Flags` subset used by interface.go.
pub const FLAG_UP: u32 = 1 << 0;
pub const FLAG_BROADCAST: u32 = 1 << 1;
pub const FLAG_LOOPBACK: u32 = 1 << 2;
pub const FLAG_POINT_TO_POINT: u32 = 1 << 3;
pub const FLAG_MULTICAST: u32 = 1 << 4;

/// `net.Interface` (name + flags are all interface.go reads).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interface {
    pub name: String,
    pub flags: u32,
}

/// `Route` (interface.go:58-63).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Route {
    pub interface: String,
    pub destination: IpAddr,
    pub gateway: IpAddr,
    pub family: AddressFamily,
}

/// Error type; `no_routes` mirrors `noRoutesError` / `IsNoRoutesError`
/// (interface.go:70-90).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetError {
    message: String,
    no_routes: bool,
}

impl NetError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            no_routes: false,
        }
    }

    /// `IsNoRoutesError`.
    pub fn is_no_routes_error(&self) -> bool {
        self.no_routes
    }
}

impl fmt::Display for NetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for NetError {}

type Result<T> = std::result::Result<T, NetError>;

/// `networkInterfacer` (interface.go:344-348). `addrs` returns
/// `net.Addr.String()` values ("ip/prefixlen").
pub trait NetworkInterfacer {
    fn interface_by_name(&self, name: &str) -> Result<Interface>;
    fn addrs(&self, intf: &Interface) -> Result<Vec<String>>;
    fn interfaces(&self) -> Result<Vec<Interface>>;
}

/// `ParseCIDRSloppy` reduced to the address part (the only part interface.go
/// uses), with the same `invalid CIDR address: <s>` error text.
fn parse_cidr_sloppy(s: &str) -> Result<IpAddr> {
    let bad = || NetError::new(format!("invalid CIDR address: {s}"));
    let (addr, prefix) = s.split_once('/').ok_or_else(bad)?;
    let ip = parse_ip_sloppy(addr).ok_or_else(bad)?;
    let max = if ip.is_ipv4() { 32 } else { 128 };
    match prefix.parse::<u32>() {
        Ok(p) if p <= max && prefix.bytes().all(|c| c.is_ascii_digit()) => Ok(ip),
        _ => Err(bad()),
    }
}

/// `net.IP.IsGlobalUnicast`.
fn is_global_unicast(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(v4) => {
            !(v4 == Ipv4Addr::BROADCAST
                || v4.is_unspecified()
                || v4.is_loopback()
                || v4.is_multicast()
                || v4.is_link_local())
        }
        IpAddr::V6(v6) => {
            !(v6.is_unspecified()
                || v6.is_loopback()
                || v6.is_multicast()
                || (v6.segments()[0] & 0xffc0) == 0xfe80)
        }
    }
}

/// `hex.DecodeString` with Go's error strings.
fn hex_decode(s: &str) -> Result<Vec<u8>> {
    fn nibble(c: u8) -> Option<u8> {
        (c as char).to_digit(16).map(|d| d as u8)
    }
    let invalid = |c: u8| {
        NetError::new(format!(
            "encoding/hex: invalid byte: U+{:04X} {:?}",
            c as u32, c as char
        ))
    };
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len() / 2);
    for pair in b.as_chunks::<2>().0 {
        let hi = nibble(pair[0]).ok_or_else(|| invalid(pair[0]))?;
        let lo = nibble(pair[1]).ok_or_else(|| invalid(pair[1]))?;
        out.push(hi << 4 | lo);
    }
    if b.len() % 2 == 1 {
        let last = b[b.len() - 1];
        if nibble(last).is_none() {
            return Err(invalid(last));
        }
        return Err(NetError::new("encoding/hex: odd length hex string"));
    }
    Ok(out)
}

/// The `bufio.Reader.ReadString('\n')` loop of the route parsers: an
/// unterminated final line comes back with `io.EOF` and is NOT parsed
/// (interface.go:114-117, 153-156).
fn terminated_lines(input: &str) -> impl Iterator<Item = &str> {
    input
        .split_inclusive('\n')
        .take_while(|l| l.ends_with('\n'))
}

fn route_field<'a>(fields: &[&'a str], i: usize) -> Result<&'a str> {
    // Go indexes `fields[i]` and would panic; a Rust error is the closest
    // safe equivalent for a malformed /proc line.
    fields
        .get(i)
        .copied()
        .ok_or_else(|| NetError::new("route file line has too few fields"))
}

/// `getIPv4DefaultRoutes` (interface.go:111-148).
pub fn get_ipv4_default_routes(input: &str) -> Result<Vec<Route>> {
    let mut routes = Vec::new();
    for line in terminated_lines(input) {
        // ignore the headers in the route info
        if line.starts_with("Iface") {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        let dest = parse_ip(route_field(&fields, 1)?, AddressFamily::V4)?;
        let gw = parse_ip(route_field(&fields, 2)?, AddressFamily::V4)?;
        if dest != IpAddr::V4(Ipv4Addr::UNSPECIFIED) {
            continue;
        }
        routes.push(Route {
            interface: route_field(&fields, 0)?.to_string(),
            destination: dest,
            gateway: gw,
            family: AddressFamily::V4,
        });
    }
    Ok(routes)
}

/// `getIPv6DefaultRoutes` (interface.go:150-182).
pub fn get_ipv6_default_routes(input: &str) -> Result<Vec<Route>> {
    let mut routes = Vec::new();
    for line in terminated_lines(input) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let dest = parse_ip(route_field(&fields, 0)?, AddressFamily::V6)?;
        let gw = parse_ip(route_field(&fields, 4)?, AddressFamily::V6)?;
        if dest != IpAddr::V6(Ipv6Addr::UNSPECIFIED) {
            continue;
        }
        if gw == IpAddr::V6(Ipv6Addr::UNSPECIFIED) {
            continue; // loopback
        }
        routes.push(Route {
            interface: route_field(&fields, 9)?.to_string(),
            destination: dest,
            gateway: gw,
            family: AddressFamily::V6,
        });
    }
    Ok(routes)
}

/// `parseIP` (interface.go:184-206): the hex address of a route file; IPv4
/// is stored little-endian.
pub fn parse_ip(s: &str, family: AddressFamily) -> Result<IpAddr> {
    if s.is_empty() {
        return Err(NetError::new("input is nil"));
    }
    let bytes = hex_decode(s)?;
    if family == AddressFamily::V4 {
        if bytes.len() != 4 {
            return Err(NetError::new("invalid IPv4 address in route"));
        }
        return Ok(IpAddr::V4(Ipv4Addr::new(
            bytes[3], bytes[2], bytes[1], bytes[0],
        )));
    }
    // Must be IPv6
    let arr: [u8; 16] = bytes
        .try_into()
        .map_err(|_| NetError::new("invalid IPv6 address in route"))?;
    Ok(IpAddr::V6(Ipv6Addr::from(arr)))
}

/// `isInterfaceUp` (interface.go:208-218).
pub fn is_interface_up(intf: Option<&Interface>) -> bool {
    intf.is_some_and(|i| i.flags & FLAG_UP != 0)
}

/// `isLoopbackOrPointToPoint` (interface.go:220-222).
fn is_loopback_or_point_to_point(intf: &Interface) -> bool {
    intf.flags & (FLAG_LOOPBACK | FLAG_POINT_TO_POINT) != 0
}

/// `getMatchingGlobalIP` (interface.go:225-247).
pub fn get_matching_global_ip(addrs: &[String], family: AddressFamily) -> Result<Option<IpAddr>> {
    for a in addrs {
        let ip = parse_cidr_sloppy(a)?;
        if member_of(ip, family) && is_global_unicast(ip) {
            return Ok(Some(ip));
        }
    }
    Ok(None)
}

/// `getIPFromInterface` (interface.go:251-272).
pub fn get_ip_from_interface(
    intf_name: &str,
    for_family: AddressFamily,
    nw: &dyn NetworkInterfacer,
) -> Result<Option<IpAddr>> {
    let intf = nw.interface_by_name(intf_name)?;
    if is_interface_up(Some(&intf)) {
        let addrs = nw.addrs(&intf)?;
        if let Some(ip) = get_matching_global_ip(&addrs, for_family)? {
            return Ok(Some(ip));
        }
    }
    Ok(None)
}

/// `getIPFromLoopbackInterface` (interface.go:276-302).
pub fn get_ip_from_loopback_interface(
    for_family: AddressFamily,
    nw: &dyn NetworkInterfacer,
) -> Result<Option<IpAddr>> {
    for intf in nw.interfaces()? {
        if !is_interface_up(Some(&intf)) {
            continue;
        }
        if intf.flags & FLAG_LOOPBACK != 0 {
            let addrs = nw.addrs(&intf)?;
            if let Some(ip) = get_matching_global_ip(&addrs, for_family)? {
                return Ok(Some(ip));
            }
        }
    }
    Ok(None)
}

/// `memberOf` (interface.go:305-312): `ip.To4() != nil` selects IPv4.
pub fn member_of(ip: IpAddr, family: AddressFamily) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(_) => family == AddressFamily::V4,
        IpAddr::V6(_) => family == AddressFamily::V6,
    }
}

/// `chooseIPFromHostInterfaces` (interface.go:317-357).
pub fn choose_ip_from_host_interfaces(
    nw: &dyn NetworkInterfacer,
    families: &AddressFamilyPreference,
) -> Result<IpAddr> {
    let intfs = nw.interfaces()?;
    if intfs.is_empty() {
        return Err(NetError::new("no interfaces found on host."));
    }
    for family in families {
        for intf in &intfs {
            if !is_interface_up(Some(intf)) {
                continue;
            }
            if is_loopback_or_point_to_point(intf) {
                continue;
            }
            let addrs = nw.addrs(intf)?;
            if addrs.is_empty() {
                continue;
            }
            for addr in &addrs {
                let ip = parse_cidr_sloppy(addr).map_err(|e| {
                    NetError::new(format!(
                        "unable to parse CIDR for interface {:?}: {e}",
                        intf.name
                    ))
                })?;
                if !member_of(ip, *family) {
                    continue;
                }
                // TODO (upstream): decide if should open up to allow IPv6 LLAs.
                if !is_global_unicast(ip) {
                    continue;
                }
                return Ok(ip);
            }
        }
    }
    Err(NetError::new(
        "no acceptable interface with global unicast address found on host",
    ))
}

/// `RouteFile.extract` (interface.go:100-108).
fn extract_routes(name: &str, parse: fn(&str) -> Result<Vec<Route>>) -> Result<Vec<Route>> {
    let content = std::fs::read_to_string(name).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            NetError::new(format!("open {name}: no such file or directory"))
        } else {
            NetError::new(format!("open {name}: {e}"))
        }
    })?;
    parse(&content)
}

/// `getAllDefaultRoutes` (interface.go:411-424) over explicit route files.
pub fn get_all_default_routes_from(v4_file: &str, v6_file: &str) -> Result<Vec<Route>> {
    let mut routes = extract_routes(v4_file, get_ipv4_default_routes)?;
    // The IPv6 route file is optional: its error is ignored.
    if let Ok(v6) = extract_routes(v6_file, get_ipv6_default_routes) {
        routes.extend(v6);
    }
    if routes.is_empty() {
        return Err(NetError {
            message: format!("no default routes found in {v4_file:?} or {v6_file:?}"),
            no_routes: true,
        });
    }
    Ok(routes)
}

/// `chooseHostInterfaceFromRoute` (interface.go:430-466).
pub fn choose_host_interface_from_route(
    routes: &[Route],
    nw: &dyn NetworkInterfacer,
    families: &AddressFamilyPreference,
) -> Result<IpAddr> {
    for family in families {
        for route in routes {
            if route.family != *family {
                continue;
            }
            if let Some(ip) = get_ip_from_interface(&route.interface, *family, nw)? {
                return Ok(ip);
            }
            // Default routes present but interfaces carry only link-local
            // addresses (RFC5549): the global IP may sit on the loopback.
            if let Some(ip) = get_ip_from_loopback_interface(*family, nw)? {
                return Ok(ip);
            }
        }
    }
    Err(NetError::new("unable to select an IP from default routes."))
}

/// `networkInterface` (interface.go:359-379), backed by getifaddrs(3) as
/// Go's `net.Interfaces()` / `Interface.Addrs()` are on Linux.
struct SystemNetworkInterface;

/// One getifaddrs entry: (name, flags, "ip/prefixlen" when it carries an
/// IPv4/IPv6 address).
fn system_entries() -> Result<Vec<(String, u32, Option<String>)>> {
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: getifaddrs fills `head` with a list that freeifaddrs releases.
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return Err(NetError::new(std::io::Error::last_os_error().to_string()));
    }
    let mut out = Vec::new();
    let mut cur = head;
    while !cur.is_null() {
        // SAFETY: `cur` is a live node of the list returned above.
        let ifa = unsafe { &*cur };
        let name = unsafe { std::ffi::CStr::from_ptr(ifa.ifa_name) }
            .to_string_lossy()
            .into_owned();
        let f = ifa.ifa_flags as i32;
        let mut flags = 0;
        for (bit, flag) in [
            (libc::IFF_UP, FLAG_UP),
            (libc::IFF_BROADCAST, FLAG_BROADCAST),
            (libc::IFF_LOOPBACK, FLAG_LOOPBACK),
            (libc::IFF_POINTOPOINT, FLAG_POINT_TO_POINT),
            (libc::IFF_MULTICAST, FLAG_MULTICAST),
        ] {
            if f & bit != 0 {
                flags |= flag;
            }
        }
        let mut addr = None;
        if !ifa.ifa_addr.is_null() {
            // SAFETY: the sockaddr family tags its concrete type.
            match i32::from(unsafe { (*ifa.ifa_addr).sa_family }) {
                libc::AF_INET => {
                    let sa = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in) };
                    let ip = Ipv4Addr::from(u32::from_be(sa.sin_addr.s_addr));
                    let prefix = if ifa.ifa_netmask.is_null() {
                        32
                    } else {
                        let m = unsafe { &*(ifa.ifa_netmask as *const libc::sockaddr_in) };
                        m.sin_addr.s_addr.count_ones()
                    };
                    addr = Some(format!("{ip}/{prefix}"));
                }
                libc::AF_INET6 => {
                    let sa = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in6) };
                    let ip = Ipv6Addr::from(sa.sin6_addr.s6_addr);
                    let prefix = if ifa.ifa_netmask.is_null() {
                        128
                    } else {
                        let m = unsafe { &*(ifa.ifa_netmask as *const libc::sockaddr_in6) };
                        m.sin6_addr
                            .s6_addr
                            .iter()
                            .map(|b| b.count_ones())
                            .sum::<u32>()
                    };
                    addr = Some(format!("{ip}/{prefix}"));
                }
                _ => {}
            }
        }
        out.push((name, flags, addr));
        cur = ifa.ifa_next;
    }
    // SAFETY: `head` came from getifaddrs and is freed exactly once.
    unsafe { libc::freeifaddrs(head) };
    Ok(out)
}

impl NetworkInterfacer for SystemNetworkInterface {
    fn interface_by_name(&self, name: &str) -> Result<Interface> {
        self.interfaces()?
            .into_iter()
            .find(|i| i.name == name)
            .ok_or_else(|| NetError::new("route ip+net: no such network interface"))
    }

    fn addrs(&self, intf: &Interface) -> Result<Vec<String>> {
        Ok(system_entries()?
            .into_iter()
            .filter(|(n, _, _)| *n == intf.name)
            .filter_map(|(_, _, a)| a)
            .collect())
    }

    fn interfaces(&self) -> Result<Vec<Interface>> {
        let mut out: Vec<Interface> = Vec::new();
        for (name, flags, _) in system_entries()? {
            if !out.iter().any(|i| i.name == name) {
                out.push(Interface { name, flags });
            }
        }
        Ok(out)
    }
}

/// `chooseHostInterface` (interface.go:370-382).
fn choose_host_interface_with(families: &AddressFamilyPreference) -> Result<IpAddr> {
    let nw = SystemNetworkInterface;
    let no_v4_route_file = matches!(
        std::fs::metadata(IPV4_ROUTE_FILE),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound
    );
    if no_v4_route_file {
        return choose_ip_from_host_interfaces(&nw, families);
    }
    let routes = get_all_default_routes_from(IPV4_ROUTE_FILE, IPV6_ROUTE_FILE)?;
    choose_host_interface_from_route(&routes, &nw, families)
}

/// `ChooseHostInterface` (interface.go:366-368).
pub fn choose_host_interface() -> Result<IpAddr> {
    choose_host_interface_with(&PREFER_IPV4)
}

/// `ResolveBindAddress` (interface.go:468-483).
pub fn resolve_bind_address(bind_address: Option<IpAddr>) -> Result<IpAddr> {
    let families = match bind_address {
        Some(ip) if member_of(ip, AddressFamily::V6) => PREFER_IPV6,
        _ => PREFER_IPV4,
    };
    match bind_address {
        Some(ip) if !(ip.is_unspecified() || ip.is_loopback()) => Ok(ip),
        _ => choose_host_interface_with(&families),
    }
}

/// `ChooseBindAddressForInterface` (interface.go:488-500).
pub fn choose_bind_address_for_interface(intf_name: &str) -> Result<IpAddr> {
    let nw = SystemNetworkInterface;
    for family in PREFER_IPV4 {
        if let Some(ip) = get_ip_from_interface(intf_name, family, &nw)? {
            return Ok(ip);
        }
    }
    Err(NetError::new(format!(
        "unable to select an IP from {intf_name} network interface"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn ip(s: &str) -> IpAddr {
        parse_ip_sloppy(s).unwrap()
    }

    const GATEWAYFIRST: &str =
        "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n\
eth3\t00000000\t0100FE0A\t0003\t0\t0\t1024\t00000000\t0\t0\t0\n\
eth3\t0000FE0A\t00000000\t0001\t0\t0\t0\t0080FFFF\t0\t0\t0\n\
docker0\t000011AC\t00000000\t0001\t0\t0\t0\t0000FFFF\t0\t0\t0\n\
virbr0\t007AA8C0\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0\n";
    const GATEWAYLAST: &str =
        "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n\
docker0\t000011AC\t00000000\t0001\t0\t0\t0\t0000FFFF\t0\t0\t0\n\
virbr0\t007AA8C0\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0\n\
eth3\t0000FE0A\t00000000\t0001\t0\t0\t0\t0080FFFF\t0\t0\t0\n\
eth3\t00000000\t0100FE0A\t0003\t0\t0\t1024\t00000000\t0\t0\t0\n";
    const GATEWAYMIDDLE: &str =
        "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n\
eth3\t0000FE0A\t00000000\t0001\t0\t0\t0\t0080FFFF\t0\t0\t0\n\
docker0\t000011AC\t00000000\t0001\t0\t0\t0\t0000FFFF\t0\t0\t0\n\
eth3\t00000000\t0100FE0A\t0003\t0\t0\t1024\t00000000\t0\t0\t0\n\
virbr0\t007AA8C0\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0\n";
    const NO_INTERNET: &str =
        "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n\
docker0\t000011AC\t00000000\t0001\t0\t0\t0\t0000FFFF\t0\t0\t0\n\
virbr0\t007AA8C0\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0\n";
    const NOTHING: &str =
        "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n";
    const BAD_DESTINATION: &str =
        "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n\
eth3\t00000000\t0100FE0A\t0003\t0\t0\t1024\t00000000\t0\t0\t0\n\
eth3\t0000FE0AA1\t00000000\t0001\t0\t0\t0\t0080FFFF\t0\t0\t0\n\
docker0\t000011AC\t00000000\t0001\t0\t0\t0\t0000FFFF\t0\t0\t0\n";
    const BAD_GATEWAY: &str =
        "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n\
eth3\t00000000\t0100FE0AA1\t0003\t0\t0\t1024\t00000000\t0\t0\t0\n\
eth3\t0000FE0A\t00000000\t0001\t0\t0\t0\t0080FFFF\t0\t0\t0\n";
    const ROUTE_INVALIDHEX: &str =
        "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n\
eth3\t00000000\t0100FE0AA\t0003\t0\t0\t1024\t00000000\t0\t0\t0\n\
eth3\t0000FE0A\t00000000\t0001\t0\t0\t0\t0080FFFF\t0\t0\t0\n";

    const V6_GATEWAYFIRST: &str = "00000000000000000000000000000000 00 00000000000000000000000000000000 00 20010001000000000000000000000001 00000064 00000000 00000000 00000003 eth3\n\
20010002000000000000000000000000 40 00000000000000000000000000000000 00 00000000000000000000000000000000 00000100 00000000 00000000 00000001 eth3\n\
00000000000000000000000000000000 60 00000000000000000000000000000000 00 00000000000000000000000000000000 00000400 00000000 00000000 00200200       lo\n";
    const V6_GATEWAYLAST: &str = "20010002000000000000000000000000 40 00000000000000000000000000000000 00 00000000000000000000000000000000 00000100 00000000 00000000 00000001 eth3\n\
00000000000000000000000000000000 60 00000000000000000000000000000000 00 00000000000000000000000000000000 00000400 00000000 00000000 00200200       lo\n\
00000000000000000000000000000000 00 00000000000000000000000000000000 00 20010001000000000000000000000001 00000064 00000000 00000000 00000003 eth3\n";
    const V6_GATEWAYMIDDLE: &str = "20010002000000000000000000000000 40 00000000000000000000000000000000 00 00000000000000000000000000000000 00000100 00000000 00000000 00000001 eth3\n\
00000000000000000000000000000000 00 00000000000000000000000000000000 00 20010001000000000000000000000001 00000064 00000000 00000000 00000003 eth3\n\
00000000000000000000000000000000 60 00000000000000000000000000000000 00 00000000000000000000000000000000 00000400 00000000 00000000 00200200       lo\n";
    const V6_NO_DEFAULT_ROUTES: &str = "00000000000000000000000000000000 60 00000000000000000000000000000000 00 00000000000000000000000000000000 00000400 00000000 00000000 00200200       lo\n\
20010001000000000000000000000000 40 00000000000000000000000000000000 00 00000000000000000000000000000000 00000400 00000000 00000000 00000001  docker0\n\
20010002000000000000000000000000 40 00000000000000000000000000000000 00 00000000000000000000000000000000 00000100 00000000 00000000 00000001   eth3\n\
fe800000000000000000000000000000 40 00000000000000000000000000000000 00 00000000000000000000000000000000 00000100 00000000 00000000 00000001   eth3\n";
    const V6_BAD_DESTINATION: &str = "2001000200000000 7a 00000000000000000000000000000000 00 00000000000000000000000000000000 00000400 00000000 00000000 00200200       lo\n";
    const V6_BAD_GATEWAY: &str = "00000000000000000000000000000000 00 00000000000000000000000000000000 00 200100010000000000000000000000000012 00000064 00000000 00000000 00000003 eth3\n";
    const V6_ROUTE_INVALIDHEX: &str = "000000000000000000000000000000000 00 00000000000000000000000000000000 00 fe80000000000000021fcafffea0ec00 00000064 00000000 00000000 00000003 enp1s0f0\n\n";

    fn ipv4_route() -> Route {
        Route {
            interface: "eth3".into(),
            destination: ip("0.0.0.0"),
            gateway: ip("10.254.0.1"),
            family: AddressFamily::V4,
        }
    }
    fn ipv6_route() -> Route {
        Route {
            interface: "eth3".into(),
            destination: ip("::"),
            gateway: ip("2001:1::1"),
            family: AddressFamily::V6,
        }
    }

    const FLAG_UP_SET: u32 = FLAG_UP | FLAG_BROADCAST | FLAG_MULTICAST;
    const FLAG_DOWN_SET: u32 = FLAG_BROADCAST | FLAG_MULTICAST;
    const FLAG_LOOPBACK_SET: u32 = FLAG_UP | FLAG_LOOPBACK;
    const FLAG_P2P_SET: u32 = FLAG_UP | FLAG_POINT_TO_POINT;

    fn intf(name: &str, flags: u32) -> Interface {
        Interface {
            name: name.into(),
            flags,
        }
    }
    fn up_intf() -> Interface {
        intf("eth3", FLAG_UP_SET)
    }
    fn down_intf() -> Interface {
        intf("eth3", FLAG_DOWN_SET)
    }
    fn loopback_intf() -> Interface {
        intf("lo", FLAG_LOOPBACK_SET)
    }
    fn p2p_intf() -> Interface {
        intf("lo", FLAG_P2P_SET)
    }

    /// The mock `networkInterfacer` implementations of interface_test.go.
    #[derive(Clone, Copy)]
    enum Fixture {
        Valid,
        V4V6,
        Ipv6,
        OnlyLinkLocals,
        FailGettingNetwork,
        NoNetwork,
        Down,
        Loopback,
        P2p,
        LinkLocalLoopback,
        Globals,
        FailGetAddrs,
        NoAddrs,
        InvalidAddr,
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    impl NetworkInterfacer for Fixture {
        fn interface_by_name(&self, name: &str) -> Result<Interface> {
            use Fixture::*;
            match self {
                FailGettingNetwork => Err(NetError::new("unable get Interface")),
                NoNetwork => Err(NetError::new("no such network interface")),
                Down => Ok(down_intf()),
                Loopback => Ok(loopback_intf()),
                P2p => Ok(p2p_intf()),
                LinkLocalLoopback | Globals if name == LOOPBACK_INTERFACE_NAME => {
                    Ok(loopback_intf())
                }
                _ => Ok(up_intf()),
            }
        }
        fn addrs(&self, i: &Interface) -> Result<Vec<String>> {
            use Fixture::*;
            Ok(match self {
                Valid | Down => s(&["fe80::2f7:6fff:fe6e:2956/64", "10.254.71.145/17"]),
                V4V6 => s(&["2001::10/64", "10.254.71.145/17"]),
                Ipv6 => s(&["2001::200/64"]),
                OnlyLinkLocals => s(&["169.254.162.166/16", "fe80::200/10"]),
                FailGettingNetwork | NoNetwork => vec![],
                Loopback | P2p => s(&["::1/128", "127.0.0.1/8"]),
                LinkLocalLoopback => {
                    if i.name == LOOPBACK_INTERFACE_NAME {
                        s(&["::1/128", "127.0.0.1/8", "10.1.1.1/32", "fd00:1:1::1/128"])
                    } else {
                        s(&["169.254.162.166/16", "fe80::200/10"])
                    }
                }
                Globals => {
                    if i.name == LOOPBACK_INTERFACE_NAME {
                        s(&["::1/128", "127.0.0.1/8", "10.1.1.1/32", "fd00:1:1::1/128"])
                    } else {
                        s(&[
                            "169.254.162.166/16",
                            "fe80::200/10",
                            "192.168.1.1/31",
                            "fd00::200/127",
                        ])
                    }
                }
                FailGetAddrs => return Err(NetError::new("unable to get Addrs")),
                NoAddrs => vec![],
                InvalidAddr => s(&["10.20.30.40.50/24"]),
            })
        }
        fn interfaces(&self) -> Result<Vec<Interface>> {
            use Fixture::*;
            match self {
                FailGettingNetwork => Err(NetError::new("mock failed getting all interfaces")),
                NoNetwork => Ok(vec![]),
                Down => Ok(vec![down_intf()]),
                Loopback => Ok(vec![loopback_intf()]),
                P2p => Ok(vec![p2p_intf()]),
                LinkLocalLoopback | Globals => Ok(vec![up_intf(), loopback_intf()]),
                _ => Ok(vec![up_intf()]),
            }
        }
    }

    // TestGetIPv4Routes
    #[test]
    fn get_ipv4_routes() {
        // (name, input, count, expected, err fragment)
        let cases: Vec<(&str, &str, usize, Option<Route>, &str)> = vec![
            ("gatewayfirst", GATEWAYFIRST, 1, Some(ipv4_route()), ""),
            ("gatewaymiddle", GATEWAYMIDDLE, 1, Some(ipv4_route()), ""),
            ("gatewaylast", GATEWAYLAST, 1, Some(ipv4_route()), ""),
            ("no routes", NOTHING, 0, None, ""),
            ("badDestination", BAD_DESTINATION, 0, None, "invalid IPv4"),
            ("badGateway", BAD_GATEWAY, 0, None, "invalid IPv4"),
            (
                "route_Invalidhex",
                ROUTE_INVALIDHEX,
                0,
                None,
                "odd length hex string",
            ),
            ("no default routes", NO_INTERNET, 0, None, ""),
        ];
        for (name, input, count, expected, frag) in cases {
            match get_ipv4_default_routes(input) {
                Err(e) => assert!(e.to_string().contains(frag), "{name}: {e}"),
                Ok(routes) => {
                    assert!(frag.is_empty(), "{name}: error {frag:?} expected");
                    assert_eq!(routes.len(), count, "{name}");
                    if count == 1 {
                        assert_eq!(routes[0].gateway, expected.unwrap().gateway, "{name}");
                        assert_eq!(routes[0].destination, ip("0.0.0.0"), "{name}");
                    }
                }
            }
        }
    }

    // TestGetIPv6Routes
    #[test]
    fn get_ipv6_routes() {
        let cases: Vec<(&str, &str, usize, &str)> = vec![
            ("v6 gatewayfirst", V6_GATEWAYFIRST, 1, ""),
            ("v6 gatewaymiddle", V6_GATEWAYMIDDLE, 1, ""),
            ("v6 gatewaylast", V6_GATEWAYLAST, 1, ""),
            ("v6 no routes", "", 0, ""),
            ("v6 badDestination", V6_BAD_DESTINATION, 0, "invalid IPv6"),
            ("v6 badGateway", V6_BAD_GATEWAY, 0, "invalid IPv6"),
            (
                "v6 route_Invalidhex",
                V6_ROUTE_INVALIDHEX,
                0,
                "odd length hex string",
            ),
            ("v6 no default routes", V6_NO_DEFAULT_ROUTES, 0, ""),
        ];
        for (name, input, count, frag) in cases {
            match get_ipv6_default_routes(input) {
                Err(e) => assert!(e.to_string().contains(frag), "{name}: {e}"),
                Ok(routes) => {
                    assert!(frag.is_empty(), "{name}: error {frag:?} expected");
                    assert_eq!(routes.len(), count, "{name}");
                    if count == 1 {
                        assert_eq!(routes[0].gateway, ipv6_route().gateway, "{name}");
                        assert_eq!(routes[0].destination, ip("::"), "{name}");
                        assert_eq!(routes[0].interface, "eth3", "{name}");
                    }
                }
            }
        }
    }

    // ReadString('\n') hits EOF on an unterminated last line and the loop
    // breaks BEFORE parsing it (interface.go:114-117).
    #[test]
    fn unterminated_last_line_is_ignored() {
        let input = "eth3\t00000000\t0100FE0A\t0003\t0\t0\t1024\t00000000\t0\t0\t0";
        assert_eq!(get_ipv4_default_routes(input).unwrap().len(), 0);
    }

    // TestParseIP
    #[test]
    fn parse_ip_cases() {
        let b = |a: [u8; 4]| Some(IpAddr::V4(Ipv4Addr::from(a)));
        let cases: Vec<(&str, &str, AddressFamily, Option<IpAddr>)> = vec![
            ("empty", "", AddressFamily::V4, None),
            ("too short", "AA", AddressFamily::V4, None),
            ("too long", "0011223344", AddressFamily::V4, None),
            ("invalid", "invalid!", AddressFamily::V4, None),
            ("zero", "00000000", AddressFamily::V4, b([0, 0, 0, 0])),
            (
                "ffff",
                "FFFFFFFF",
                AddressFamily::V4,
                b([255, 255, 255, 255]),
            ),
            (
                "valid v4",
                "12345678",
                AddressFamily::V4,
                b([120, 86, 52, 18]),
            ),
            (
                "valid v6",
                "fe800000000000000000000000000000",
                AddressFamily::V6,
                Some(ip("fe80::")),
            ),
            (
                "v6 too short",
                "fe80000000000000021fcafffea0ec0",
                AddressFamily::V6,
                None,
            ),
            (
                "v6 too long",
                "fe80000000000000021fcafffea0ec002",
                AddressFamily::V6,
                None,
            ),
        ];
        for (name, input, fam, expected) in cases {
            assert_eq!(parse_ip(input, fam).ok(), expected, "{name}");
        }
        assert_eq!(
            parse_ip("", AddressFamily::V4).unwrap_err().to_string(),
            "input is nil"
        );
    }

    // TestIsInterfaceUp
    #[test]
    fn is_interface_up_cases() {
        assert!(is_interface_up(Some(&intf("eth3", FLAG_UP))));
        assert!(!is_interface_up(Some(&intf("eth3", 0))));
        assert!(!is_interface_up(None));
    }

    // TestFinalIP
    #[test]
    fn final_ip() {
        let cases: Vec<(&str, &str, AddressFamily, Option<&str>)> = vec![
            ("no ipv4", "2001::5/64", AddressFamily::V4, None),
            ("no ipv6", "10.128.0.4/32", AddressFamily::V6, None),
            (
                "invalidV4CIDR",
                "10.20.30.40.50/24",
                AddressFamily::V4,
                None,
            ),
            (
                "invalidV6CIDR",
                "fe80::2f7:67fff:fe6e:2956/64",
                AddressFamily::V6,
                None,
            ),
            ("loopback", "127.0.0.1/24", AddressFamily::V4, None),
            ("loopbackv6", "::1/128", AddressFamily::V6, None),
            ("link local v4", "169.254.1.10/16", AddressFamily::V4, None),
            (
                "link local v6",
                "fe80::2f7:6fff:fe6e:2956/64",
                AddressFamily::V6,
                None,
            ),
            (
                "ip4",
                "10.254.12.132/17",
                AddressFamily::V4,
                Some("10.254.12.132"),
            ),
            ("ip6", "2001::5/64", AddressFamily::V6, Some("2001::5")),
        ];
        for (name, addr, fam, expected) in cases {
            let got = get_matching_global_ip(&[addr.to_string()], fam)
                .ok()
                .flatten();
            assert_eq!(got, expected.map(ip), "{name}");
        }
        assert_eq!(
            get_matching_global_ip(&[], AddressFamily::V4).unwrap(),
            None
        );
        assert!(
            get_matching_global_ip(&["10.20.30.40.50/24".into()], AddressFamily::V4)
                .unwrap_err()
                .to_string()
                .contains("invalid CIDR")
        );
    }

    // TestAddrs
    #[test]
    fn addrs_fixture() {
        assert_eq!(Fixture::Valid.addrs(&intf("eth3", 0)).unwrap().len(), 2);
    }

    // TestGetIPFromInterface
    #[test]
    fn get_ip_from_interface_cases() {
        use Fixture::*;
        let cases: Vec<(&str, AddressFamily, Fixture, Option<&str>, &str)> = vec![
            ("ipv4", AddressFamily::V4, Valid, Some("10.254.71.145"), ""),
            ("ipv6", AddressFamily::V6, Ipv6, Some("2001::200"), ""),
            ("no ipv4", AddressFamily::V4, Ipv6, None, ""),
            ("no ipv6", AddressFamily::V6, Valid, None, ""),
            ("I/F down", AddressFamily::V4, Down, None, ""),
            (
                "I/F get fail",
                AddressFamily::V4,
                NoNetwork,
                None,
                "no such network interface",
            ),
            (
                "fail get addr",
                AddressFamily::V4,
                FailGetAddrs,
                None,
                "unable to get Addrs",
            ),
            (
                "bad addr",
                AddressFamily::V4,
                InvalidAddr,
                None,
                "invalid CIDR",
            ),
        ];
        for (name, fam, nw, expected, frag) in cases {
            match get_ip_from_interface("eth3", fam, &nw) {
                Err(e) => assert!(e.to_string().contains(frag), "{name}: {e}"),
                Ok(got) => {
                    assert!(frag.is_empty(), "{name}: error {frag:?} expected");
                    assert_eq!(got, expected.map(ip), "{name}");
                }
            }
        }
    }

    // TestGetIPFromLoopbackInterface
    #[test]
    fn get_ip_from_loopback_interface_cases() {
        use Fixture::*;
        let cases = [
            (AddressFamily::V4, LinkLocalLoopback, Some("10.1.1.1")),
            (AddressFamily::V6, LinkLocalLoopback, Some("fd00:1:1::1")),
            (AddressFamily::V4, Loopback, None),
            (AddressFamily::V6, Loopback, None),
        ];
        for (fam, nw, expected) in cases {
            assert_eq!(
                get_ip_from_loopback_interface(fam, &nw).unwrap(),
                expected.map(ip)
            );
        }
    }

    type RouteCase<'a> = (
        &'a str,
        &'a Vec<Route>,
        Fixture,
        AddressFamilyPreference,
        Option<&'a str>,
    );

    // TestChooseHostInterfaceFromRoute
    #[test]
    fn choose_host_interface_from_route_cases() {
        use Fixture::*;
        let v4 = vec![ipv4_route()];
        let v6 = vec![ipv6_route()];
        let both = vec![ipv4_route(), ipv6_route()];
        let none: Vec<Route> = vec![];
        let cases: Vec<RouteCase> = vec![
            (
                "single-stack ipv4",
                &v4,
                Valid,
                PREFER_IPV4,
                Some("10.254.71.145"),
            ),
            (
                "single-stack ipv4, prefer v6",
                &v4,
                Valid,
                PREFER_IPV6,
                Some("10.254.71.145"),
            ),
            (
                "single-stack ipv6",
                &v6,
                Ipv6,
                PREFER_IPV4,
                Some("2001::200"),
            ),
            (
                "single-stack ipv6, prefer v6",
                &v6,
                Ipv6,
                PREFER_IPV6,
                Some("2001::200"),
            ),
            (
                "dual stack",
                &both,
                V4V6,
                PREFER_IPV4,
                Some("10.254.71.145"),
            ),
            (
                "dual stack, prefer v6",
                &both,
                V4V6,
                PREFER_IPV6,
                Some("2001::10"),
            ),
            (
                "LLA and loopback with global, IPv4",
                &v4,
                LinkLocalLoopback,
                PREFER_IPV4,
                Some("10.1.1.1"),
            ),
            (
                "LLA and loopback with global, IPv6",
                &v6,
                LinkLocalLoopback,
                PREFER_IPV6,
                Some("fd00:1:1::1"),
            ),
            (
                "LLA and loopback with global, dual stack prefer IPv4",
                &both,
                LinkLocalLoopback,
                PREFER_IPV4,
                Some("10.1.1.1"),
            ),
            (
                "LLA and loopback with global, dual stack prefer IPv6",
                &both,
                LinkLocalLoopback,
                PREFER_IPV6,
                Some("fd00:1:1::1"),
            ),
            (
                "LLA and loopback with global, no routes",
                &none,
                LinkLocalLoopback,
                PREFER_IPV6,
                None,
            ),
            (
                "interface and loopback with global, IPv4",
                &v4,
                Globals,
                PREFER_IPV4,
                Some("192.168.1.1"),
            ),
            (
                "interface and loopback with global, IPv6",
                &v6,
                Globals,
                PREFER_IPV6,
                Some("fd00::200"),
            ),
            (
                "interface and loopback with global, dual stack prefer IPv4",
                &both,
                Globals,
                PREFER_IPV4,
                Some("192.168.1.1"),
            ),
            (
                "interface and loopback with global, dual stack prefer IPv6",
                &both,
                Globals,
                PREFER_IPV6,
                Some("fd00::200"),
            ),
            (
                "interface and loopback with global, no routes",
                &none,
                Globals,
                PREFER_IPV6,
                None,
            ),
            ("all LLA", &v4, OnlyLinkLocals, PREFER_IPV4, None),
            ("no routes", &none, Valid, PREFER_IPV4, None),
            ("fail get IP", &v4, FailGetAddrs, PREFER_IPV4, None),
        ];
        for (name, routes, nw, order, expected) in cases {
            let got = choose_host_interface_from_route(routes, &nw, &order).ok();
            assert_eq!(got, expected.map(ip), "{name}");
        }
        assert_eq!(
            choose_host_interface_from_route(&[], &Valid, &PREFER_IPV4)
                .unwrap_err()
                .to_string(),
            "unable to select an IP from default routes."
        );
    }

    // TestMemberOf
    #[test]
    fn member_of_cases() {
        assert!(member_of(ip("10.20.30.40"), AddressFamily::V4));
        assert!(!member_of(ip("10.10.10.10"), AddressFamily::V6));
        assert!(!member_of(ip("2001::100"), AddressFamily::V4));
        assert!(member_of(ip("2001::100"), AddressFamily::V6));
    }

    type HostCase<'a> = (
        &'a str,
        Fixture,
        AddressFamilyPreference,
        Option<&'a str>,
        &'a str,
    );

    // TestGetIPFromHostInterfaces
    #[test]
    fn choose_ip_from_host_interfaces_cases() {
        use Fixture::*;
        let cases: Vec<HostCase> = vec![
            (
                "fail get I/Fs",
                FailGettingNetwork,
                PREFER_IPV4,
                None,
                "failed getting all interfaces",
            ),
            (
                "no interfaces",
                NoNetwork,
                PREFER_IPV4,
                None,
                "no interfaces",
            ),
            ("I/F not up", Down, PREFER_IPV4, None, "no acceptable"),
            (
                "loopback only",
                Loopback,
                PREFER_IPV4,
                None,
                "no acceptable",
            ),
            ("P2P I/F only", P2p, PREFER_IPV4, None, "no acceptable"),
            (
                "fail get addrs",
                FailGetAddrs,
                PREFER_IPV4,
                None,
                "unable to get Addrs",
            ),
            ("no addresses", NoAddrs, PREFER_IPV4, None, "no acceptable"),
            (
                "invalid addr",
                InvalidAddr,
                PREFER_IPV4,
                None,
                "invalid CIDR",
            ),
            (
                "no matches",
                OnlyLinkLocals,
                PREFER_IPV4,
                None,
                "no acceptable",
            ),
            (
                "single-stack ipv4",
                Valid,
                PREFER_IPV4,
                Some("10.254.71.145"),
                "",
            ),
            (
                "single-stack ipv4, prefer ipv6",
                Valid,
                PREFER_IPV6,
                Some("10.254.71.145"),
                "",
            ),
            (
                "single-stack ipv6",
                Ipv6,
                PREFER_IPV4,
                Some("2001::200"),
                "",
            ),
            (
                "single-stack ipv6, prefer ipv6",
                Ipv6,
                PREFER_IPV6,
                Some("2001::200"),
                "",
            ),
            ("dual stack", V4V6, PREFER_IPV4, Some("10.254.71.145"), ""),
            (
                "dual stack, prefer ipv6",
                V4V6,
                PREFER_IPV6,
                Some("2001::10"),
                "",
            ),
        ];
        for (name, nw, order, expected, frag) in cases {
            match choose_ip_from_host_interfaces(&nw, &order) {
                Ok(got) => assert_eq!(Some(got), expected.map(ip), "{name}"),
                Err(e) => {
                    assert!(expected.is_none(), "{name}: {e}");
                    assert!(e.to_string().contains(frag), "{name}: {e}");
                }
            }
        }
    }

    fn write_tmp(content: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    // TestFailGettingIPv4Routes / TestGetAllDefaultRoutesFailNoV4RouteFile
    #[test]
    fn get_all_default_routes_missing_v4_file() {
        let e = get_all_default_routes_from("no-such-file", "no-such-file").unwrap_err();
        assert!(e.to_string().contains("no such file"), "{e}");
    }

    type AllRoutesCase<'a> = (&'a str, &'a str, &'a str, Vec<Route>, &'a str);

    // TestGetAllDefaultRoutes
    #[test]
    fn get_all_default_routes_cases() {
        let cases: Vec<AllRoutesCase> = vec![
            (
                "no routes",
                NO_INTERNET,
                V6_NO_DEFAULT_ROUTES,
                vec![],
                "no default routes",
            ),
            (
                "only v4 route",
                GATEWAYFIRST,
                V6_NO_DEFAULT_ROUTES,
                vec![ipv4_route()],
                "",
            ),
            (
                "only v6 route",
                NO_INTERNET,
                V6_GATEWAYFIRST,
                vec![ipv6_route()],
                "",
            ),
            (
                "v4 and v6 routes",
                GATEWAYFIRST,
                V6_GATEWAYFIRST,
                vec![ipv4_route(), ipv6_route()],
                "",
            ),
        ];
        for (name, v4, v6, expected, frag) in cases {
            let f4 = write_tmp(v4);
            let f6 = write_tmp(v6);
            match get_all_default_routes_from(
                f4.path().to_str().unwrap(),
                f6.path().to_str().unwrap(),
            ) {
                Err(e) => {
                    assert!(e.to_string().contains(frag), "{name}: {e}");
                    assert!(e.is_no_routes_error(), "{name}");
                }
                Ok(routes) => {
                    assert!(frag.is_empty(), "{name}");
                    assert_eq!(routes.len(), expected.len(), "{name}");
                    for (r, e) in routes.iter().zip(&expected) {
                        assert_eq!(r.gateway, e.gateway, "{name}");
                        assert_eq!(r.family, e.family, "{name}");
                    }
                }
            }
        }
    }

    // The getifaddrs-backed `networkInterface` (interface.go:359-379) sees the
    // loopback interface with its prefix length, like Go's `net.Interface.Addrs`.
    #[test]
    fn system_network_interface_lists_loopback() {
        let nw = SystemNetworkInterface;
        let lo = nw.interface_by_name(LOOPBACK_INTERFACE_NAME).unwrap();
        assert!(is_interface_up(Some(&lo)));
        assert!(lo.flags & FLAG_LOOPBACK != 0);
        assert!(nw.addrs(&lo).unwrap().contains(&"127.0.0.1/8".to_string()));
        let e = nw.interface_by_name("no-such-if0").unwrap_err();
        assert_eq!(e.to_string(), "route ip+net: no such network interface");
    }

    // ResolveBindAddress (interface.go:468-483): a specific non-loopback
    // address is returned verbatim; no host lookup happens.
    #[test]
    fn resolve_bind_address_passthrough() {
        assert_eq!(
            resolve_bind_address(Some(ip("10.1.2.3"))).unwrap(),
            ip("10.1.2.3")
        );
        assert_eq!(
            resolve_bind_address(Some(ip("2001:db8::5"))).unwrap(),
            ip("2001:db8::5")
        );
    }
}
