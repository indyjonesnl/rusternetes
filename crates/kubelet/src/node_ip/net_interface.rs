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
#![allow(dead_code)]

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
    pub fn new(message: impl Into<String>) -> Self {
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

pub type Result<T> = std::result::Result<T, NetError>;

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
pub struct SystemNetworkInterface;

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
