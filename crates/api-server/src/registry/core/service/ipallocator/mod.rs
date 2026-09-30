//! Port of `pkg/registry/core/service/ipallocator`: ClusterIP allocation
//! backed by `IPAddress` objects (KEP-1880, MultiCIDRServiceAllocator, GA and
//! locked on in 1.35). An address is allocated by creating the IPAddress
//! named after it; the create's uniqueness is the allocation lock.
//!
//! Upstream reads IPAddresses through an informer's lister and writes them
//! through a loopback client. Rusternetes runs in-process, so both go to
//! storage directly — the same objects under the same keys, with the
//! lister's view replaced by a fresh read.

pub mod cidr;
pub mod repair;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ipnet::IpNet;
use rand::Rng;
use rusternetes_common::resources::{IPAddress, IPAddressSpec, ParentReference, Service};
use rusternetes_common::Error;
use rusternetes_storage::{build_key, Storage};

/// `ControllerName` (ipallocator.go:44), the `managed-by` label value.
pub const CONTROLLER_NAME: &str = "ipallocator.k8s.io";
/// `networkingv1.LabelIPAddressFamily` (well_known_labels.go:26).
pub const LABEL_IP_ADDRESS_FAMILY: &str = "ipaddress.kubernetes.io/ip-family";
/// `networkingv1.LabelManagedBy` (well_known_labels.go:32).
pub const LABEL_MANAGED_BY: &str = "ipaddress.kubernetes.io/managed-by";

pub(crate) const IP_ADDRESS_PREFIX: &str = "/registry/ipaddresses/";

/// The errors of interfaces.go:43-58.
#[derive(Debug)]
pub enum IpError {
    Full,
    Allocated,
    MismatchedNetwork,
    NotReady,
    NotInRange { ip: IpAddr, valid_range: String },
    Other(Error),
}

impl fmt::Display for IpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IpError::Full => write!(f, "range is full"),
            IpError::Allocated => write!(f, "provided IP is already allocated"),
            IpError::MismatchedNetwork => {
                write!(f, "the provided network does not match the current range")
            }
            IpError::NotReady => write!(f, "allocator not ready"),
            IpError::NotInRange { ip, valid_range } => write!(
                f,
                "the provided IP ({ip}) is not in the valid range. The range of valid IPs is {valid_range}"
            ),
            IpError::Other(e) => write!(f, "{e}"),
        }
    }
}

pub type IpResult<T> = std::result::Result<T, IpError>;

/// `"IPv4"` / `"IPv6"`, the `api.IPFamily` strings.
pub fn family_of(ip: &IpAddr) -> &'static str {
    if ip.is_ipv6() {
        "IPv6"
    } else {
        "IPv4"
    }
}

fn to_u128(ip: IpAddr) -> u128 {
    match ip {
        IpAddr::V4(v4) => u32::from(v4) as u128,
        IpAddr::V6(v6) => u128::from(v6),
    }
}

fn from_u128(v: u128, is6: bool) -> IpAddr {
    if is6 {
        IpAddr::V6(Ipv6Addr::from(v))
    } else {
        IpAddr::V4(Ipv4Addr::from(v as u32))
    }
}

/// `broadcastAddress` (ipallocator.go:582-605): every host bit set.
fn broadcast_address(prefix: &IpNet) -> u128 {
    to_u128(prefix.broadcast())
}

/// `servicecidr.PrefixContainsIP` (pkg/api/servicecidr/servicecidr.go:
/// 100-113): neither the network address nor, for IPv4, the broadcast
/// address counts as contained.
pub fn prefix_contains_ip(prefix: &IpNet, ip: &IpAddr) -> bool {
    if prefix.network() == *ip {
        return false;
    }
    if ip.is_ipv4() && to_u128(*ip) == broadcast_address(prefix) {
        return false;
    }
    prefix.contains(ip)
}

/// `hostsPerNetwork` (ipallocator.go:553-577), capped at `u64::MAX`.
fn hosts_per_network(prefix: &IpNet) -> u64 {
    let host_bits = prefix.max_prefix_len() - prefix.prefix_len();
    if host_bits >= 64 {
        return u64::MAX;
    }
    let mut max = 1u64 << host_bits;
    // Don't use the network's ".0" address.
    max -= 1;
    if prefix.addr().is_ipv4() {
        // Don't use the IPv4 network's broadcast address.
        if max == 0 {
            return 0;
        }
        max -= 1;
    }
    max
}

/// `calculateRangeOffset` (ipallocator/bitmap.go:356-380): the size of the
/// lower band kept for static allocation, `min(max(16, size/16), 256)`.
pub fn calculate_range_offset(prefix: &IpNet) -> u64 {
    const MIN: u128 = 16;
    const MAX: u128 = 256;
    const STEP: u128 = 16;
    let host_bits = u32::from(prefix.max_prefix_len() - prefix.prefix_len());
    // `netutils.RangeSize`, saturating as upstream's does at 64 bits.
    let cidr_size: u128 = if host_bits >= 64 {
        u64::MAX as u128
    } else {
        1u128 << host_bits
    };
    if cidr_size <= MIN {
        return 0;
    }
    (cidr_size / STEP).clamp(MIN, MAX) as u64
}

/// `serviceToRef` (ipallocator.go:607-619).
pub fn service_to_ref(svc: &Service) -> ParentReference {
    ParentReference {
        group: Some(String::new()),
        resource: "services".to_string(),
        namespace: svc.metadata.namespace.clone(),
        name: svc.metadata.name.clone(),
        uid: None,
    }
}

/// The IPAddress the allocator creates for `name` (ipallocator.go:
/// 148-162, and repairip.go `newIPAddress`).
pub fn new_ip_address(name: &str, svc: Option<&Service>) -> IPAddress {
    let family = name
        .parse::<IpAddr>()
        .map(|ip| family_of(&ip))
        .unwrap_or("IPv4");
    // An allocation made without a Service (upstream's test-only
    // `Allocate`) has no parent reference.
    let mut ip = IPAddress {
        type_meta: rusternetes_common::types::TypeMeta {
            kind: "IPAddress".to_string(),
            api_version: "networking.k8s.io/v1".to_string(),
        },
        metadata: rusternetes_common::types::ObjectMeta::new(name),
        spec: Some(IPAddressSpec {
            parent_ref: svc.map(service_to_ref),
        }),
    };
    ip.metadata.labels = Some(HashMap::from([
        (LABEL_IP_ADDRESS_FAMILY.to_string(), family.to_string()),
        (LABEL_MANAGED_BY.to_string(), CONTROLLER_NAME.to_string()),
    ]));
    ip.metadata.ensure_uid();
    ip.metadata.ensure_creation_timestamp();
    ip
}

pub(crate) fn ip_address_key(name: &str) -> String {
    build_key("ipaddresses", None, name)
}

/// Every IPAddress this allocator family manages
/// (`ForEach`/`Used`'s label selector, ipallocator.go:391-405).
#[allow(dead_code)] // upstream API; exercised by the tests
async fn list_managed<S: Storage>(storage: &S, family: &str) -> Vec<IPAddress> {
    storage
        .list::<IPAddress>(IP_ADDRESS_PREFIX)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|ip| {
            let labels = ip.metadata.labels.as_ref();
            labels
                .and_then(|l| l.get(LABEL_IP_ADDRESS_FAMILY))
                .map(String::as_str)
                == Some(family)
                && labels
                    .and_then(|l| l.get(LABEL_MANAGED_BY))
                    .map(String::as_str)
                    == Some(CONTROLLER_NAME)
        })
        .collect()
}

/// `Allocator` (ipallocator.go:46-70): the addresses of one CIDR.
pub struct Allocator<S: Storage> {
    storage: Arc<S>,
    prefix: IpNet,
    first: u128,
    offset_address: u128,
    last: u128,
    family: &'static str,
    range_offset: u64,
    size: u64,
    /// Whether this allocator may hand out new addresses; it depends on
    /// its ServiceCIDR being ready.
    pub(crate) ready: AtomicBool,
}

impl<S: Storage> Allocator<S> {
    /// `NewIPAllocator` (ipallocator.go:76-133).
    pub fn new(prefix: IpNet, storage: Arc<S>) -> std::result::Result<Self, String> {
        let prefix = prefix.trunc();
        if prefix.addr().is_ipv6() && prefix.prefix_len() < 64 {
            return Err(format!(
                "shortest allowed prefix length for service CIDR is 64, got {}",
                prefix.prefix_len()
            ));
        }
        let size = hosts_per_network(&prefix);
        let family = family_of(&prefix.addr());
        // Don't allocate the network's ".0" address.
        let first = to_u128(prefix.network()) + 1;
        // IPv6 uses the broadcast address; IPv4 does not.
        let mut last = broadcast_address(&prefix);
        if prefix.addr().is_ipv4() {
            last -= 1;
        }
        // KEP-3070: reserve the lower band for static allocation.
        let range_offset = calculate_range_offset(&prefix);
        let offset_address = first + range_offset as u128;
        Ok(Self {
            storage,
            prefix,
            first,
            offset_address,
            last,
            family,
            range_offset,
            size,
            ready: AtomicBool::new(true),
        })
    }

    pub fn ip_family(&self) -> &'static str {
        self.family
    }

    fn is6(&self) -> bool {
        self.family == "IPv6"
    }

    /// `createIPAddress` (ipallocator.go:147-176).
    async fn create_ip_address(&self, name: &str, svc: Option<&Service>) -> IpResult<()> {
        let ip = new_ip_address(name, svc);
        match self.storage.create(&ip_address_key(name), &ip).await {
            Ok(_) => Ok(()),
            Err(Error::AlreadyExists(_)) => Err(IpError::Allocated),
            Err(e) => Err(IpError::Other(e)),
        }
    }

    /// `allocateService` (ipallocator.go:198-225).
    pub async fn allocate_service(
        &self,
        svc: Option<&Service>,
        ip: IpAddr,
        dry_run: bool,
    ) -> IpResult<()> {
        if !self.ready.load(Ordering::SeqCst) {
            return Err(IpError::NotReady);
        }
        let addr = to_u128(ip);
        if ip.is_ipv6() != self.is6() || addr < self.first || self.last < addr {
            return Err(IpError::NotInRange {
                ip,
                valid_range: self.prefix.to_string(),
            });
        }
        if dry_run {
            return Ok(());
        }
        self.create_ip_address(&ip.to_string(), svc).await
    }

    /// `allocateNextService` (ipallocator.go:239-296): a random start in the
    /// upper band, then the lower band once the upper one is full.
    pub async fn allocate_next_service(
        &self,
        svc: Option<&Service>,
        dry_run: bool,
    ) -> IpResult<IpAddr> {
        if !self.ready.load(Ordering::SeqCst) {
            return Err(IpError::NotReady);
        }
        if dry_run {
            // Don't bother finding a free value: racy and not worth it.
            return Ok(self.prefix.network());
        }
        let range_size = self.size - self.range_offset;
        if range_size == 0 {
            return Err(IpError::Full);
        }
        let offset = rand::rng().random_range(0..range_size);
        let result = self
            .allocate_from_range(self.offset_address, self.last, offset, svc)
            .await;
        match result {
            Err(IpError::Full) if self.range_offset != 0 => {
                let offset = rand::rng().random_range(0..self.range_offset);
                self.allocate_from_range(self.first, self.offset_address - 1, offset, svc)
                    .await
            }
            other => other,
        }
    }

    /// `ipIterator` + `allocateFromRange` (ipallocator.go:298-366): walk
    /// `[first, last]` from `first + offset`, wrapping once, and create the
    /// first address not already taken.
    async fn allocate_from_range(
        &self,
        first: u128,
        last: u128,
        offset: u64,
        svc: Option<&Service>,
    ) -> IpResult<IpAddr> {
        let taken: HashSet<String> = self
            .storage
            .list::<IPAddress>(IP_ADDRESS_PREFIX)
            .await
            .map_err(IpError::Other)?
            .into_iter()
            .map(|ip| ip.metadata.name)
            .collect();
        let count = last - first + 1;
        let start = offset as u128 % count;
        for i in 0..count {
            let ip = from_u128(first + (start + i) % count, self.is6());
            let name = ip.to_string();
            if taken.contains(&name) {
                continue;
            }
            match self.create_ip_address(&name, svc).await {
                Ok(()) => return Ok(ip),
                // A mid-air collision with another allocator: try the next
                // address (https://issues.k8s.io/135333).
                Err(IpError::Allocated) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(IpError::Full)
    }

    /// `release` (ipallocator.go:374-391): delete the IPAddress whatever the
    /// cache says; errors are ignored for compatibility.
    pub async fn release(&self, ip: IpAddr, dry_run: bool) -> IpResult<()> {
        if dry_run {
            return Ok(());
        }
        let name = ip.to_string();
        if let Err(e) = self.storage.delete(&ip_address_key(&name)).await {
            tracing::info!("error releasing ip {name} : {e}");
        }
        Ok(())
    }

    /// `Has` (ipallocator.go:411-419).
    #[allow(dead_code)] // upstream API; exercised by the tests
    pub async fn has(&self, ip: IpAddr) -> bool {
        self.storage
            .get::<IPAddress>(&ip_address_key(&ip.to_string()))
            .await
            .is_ok()
    }

    /// `Used` (ipallocator.go:425-448): the managed addresses in this CIDR.
    #[allow(dead_code)] // upstream API; exercised by the tests
    pub async fn used(&self) -> usize {
        list_managed(&*self.storage, self.family)
            .await
            .iter()
            .filter_map(|ip| ip.metadata.name.parse::<IpAddr>().ok())
            .filter(|ip| self.prefix.contains(ip))
            .count()
    }

    /// `Free` (ipallocator.go:451-474).
    #[allow(dead_code)] // upstream API; exercised by the tests
    pub async fn free(&self) -> u64 {
        let used = self.used().await as u64;
        self.size.saturating_sub(used)
    }

    #[allow(dead_code)] // upstream API; exercised by the tests
    pub(crate) fn size(&self) -> u64 {
        self.size
    }
}

#[cfg(test)]
mod tests;
