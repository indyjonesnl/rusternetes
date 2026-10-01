//! Validation for `networking.k8s.io` IPAddress, ported from upstream
//! `ValidateIPAddress` / `validateIPAddressParentReference`
//! (`pkg/apis/networking/validation/validation.go`).
//!
//! The `metadata.name` must be a canonical IP (upstream `ValidateIPAddressName`).

use crate::resources::ipaddress::{IPAddress, ParentReference};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;
use crate::validation::objectmeta::{
    name_is_ip, name_is_path_segment, validate_immutable_field, validate_object_meta,
    validate_object_meta_update,
};

/// Upstream `ValidateIPAddress` (pkg/apis/networking/validation/validation.go:759-765):
/// `ValidateObjectMeta` with `ValidateIPAddressName`, then the parent
/// reference.
pub fn validate_ip_address(ip: &IPAddress) -> ErrorList {
    let mut errs = validate_object_meta(&ip.metadata, false, name_is_ip, &Path::new("metadata"));
    let spec_path = Path::new("spec");
    match ip.spec.as_ref().and_then(|spec| spec.parent_ref.as_ref()) {
        // A missing spec, or a spec with no parentRef, is a missing parentRef —
        // which upstream requires (`validation.go:771-773`).
        None => errs.push(Error::required(&spec_path.child("parentRef"), "")),
        Some(parent_ref) => errs.extend(validate_parent_reference(parent_ref, &spec_path)),
    }
    errs
}

/// Upstream `ValidateIPAddressUpdate` (validation.go:812-817):
/// `ValidateObjectMetaUpdate`, then `spec.parentRef` is immutable. The
/// strategy's `ValidateUpdate` (ipaddress/strategy.go:84-89) runs
/// `ValidateIPAddress` first; [`validate_ip_address`] is that half.
pub fn validate_ip_address_update(update: &IPAddress, old: &IPAddress) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&update.metadata, &old.metadata, &Path::new("metadata"));
    let new_ref = update.spec.as_ref().and_then(|s| s.parent_ref.as_ref());
    let old_ref = old.spec.as_ref().and_then(|s| s.parent_ref.as_ref());
    errs.extend(validate_immutable_field(
        &new_ref,
        &old_ref,
        &Path::new("spec").child("parentRef"),
    ));
    errs
}

/// Upstream `validateIPAddressParentReference`.
fn validate_parent_reference(pr: &ParentReference, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    let p = fld_path.child("parentRef");

    // group is required, but the core group (used by Services) is the empty
    // value and so cannot be enforced; only validate it when present.
    if let Some(group) = &pr.group {
        if !group.is_empty() {
            for msg in is_dns1123_subdomain(group) {
                errs.push(Error::invalid(&p.child("group"), group.clone(), msg));
            }
        }
    }

    // resource is required.
    if pr.resource.is_empty() {
        errs.push(Error::required(&p.child("resource"), ""));
    } else {
        for msg in name_is_path_segment(&pr.resource, false) {
            errs.push(Error::invalid(
                &p.child("resource"),
                pr.resource.clone(),
                msg,
            ));
        }
    }

    // name is required.
    if pr.name.is_empty() {
        errs.push(Error::required(&p.child("name"), ""));
    } else {
        for msg in name_is_path_segment(&pr.name, false) {
            errs.push(Error::invalid(&p.child("name"), pr.name.clone(), msg));
        }
    }

    // namespace is optional.
    if let Some(ns) = &pr.namespace {
        if !ns.is_empty() {
            for msg in name_is_path_segment(ns, false) {
                errs.push(Error::invalid(&p.child("namespace"), ns.clone(), msg));
            }
        }
    }

    errs
}

/// Go's `net.IPNet.String()` for a parsed prefix: an IPv4-mapped IPv6
/// network prints in IPv4 form, its mask cut to the last 32 bits
/// (`networkNumberAndMask`, net/ip.go).
fn go_ipnet_string(net: std::net::IpAddr, prefix: u8) -> String {
    use std::net::IpAddr;
    match net {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => format!("{v4}/{}", prefix.saturating_sub(96)),
            None => format!("{v6}/{prefix}"),
        },
        IpAddr::V4(v4) => format!("{v4}/{prefix}"),
    }
}

/// Go's `net.IP.String()`: an IPv4-mapped IPv6 address prints as IPv4.
fn go_ip_string(ip: std::net::IpAddr) -> String {
    use std::net::IpAddr;
    match ip {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map_or_else(|| v6.to_string(), |v4| v4.to_string()),
        IpAddr::V4(v4) => v4.to_string(),
    }
}

/// Port of `GetWarningsForCIDR`
/// (apimachinery/pkg/util/validation/ip.go:211-250), for a value validation
/// has already accepted: host bits set after the prefix, an IPv4-mapped IPv6
/// value, or an IPv6 value not in RFC 5952 canonical form.
///
/// Includes the leading-zero forms accepted by `ParseCIDRSloppy`.
pub fn get_warnings_for_cidr(fld_path: &Path, value: &str) -> Vec<String> {
    use std::net::IpAddr;
    let Some((ip_str, prefix_str)) = value.split_once('/') else {
        return Vec::new();
    };
    let (Some(ip), Ok(prefix)) = (
        super::ingress::parse_ip_sloppy(ip_str),
        prefix_str.parse::<u8>(),
    ) else {
        return Vec::new();
    };
    let (network, addr_len) = match ip {
        IpAddr::V4(v4) if prefix <= 32 => {
            let mask = u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0);
            (IpAddr::V4((u32::from(v4) & mask).into()), 32)
        }
        IpAddr::V6(v6) if prefix <= 128 => {
            let mask = u128::MAX.checked_shl(128 - u32::from(prefix)).unwrap_or(0);
            (IpAddr::V6((u128::from(v6) & mask).into()), 128)
        }
        _ => return Vec::new(),
    };
    let ipnet = go_ipnet_string(network, prefix);
    let mut warnings = Vec::new();
    if ip != network {
        warnings.push(format!(
            "{fld_path}: CIDR value {value:?} is ambiguous in this context (should be {ipnet:?} or {:?}?)",
            format!("{}/{addr_len}", go_ip_string(ip)),
        ));
    }
    // `netip.ParsePrefix` rejects what `ParseCIDRSloppy` let through: a
    // prefix length with leading zeros, or an IPv4-mapped IPv6 address.
    let mapped = matches!(ip, IpAddr::V6(v6) if v6.to_ipv4_mapped().is_some());
    if ip_str.parse::<IpAddr>().is_err() || mapped || prefix_str != prefix.to_string() {
        warnings.push(format!(
            "{fld_path}: non-standard CIDR value {value:?} will be considered invalid in a future Kubernetes release: use {ipnet:?}"
        ));
    }
    if let IpAddr::V6(v6) = ip {
        let canonical = format!("{v6}/{prefix}");
        if warnings.is_empty() && canonical != value {
            warnings.push(format!(
                "{fld_path}: IPv6 CIDR value {value:?} should be in RFC 5952 canonical format ({canonical:?})"
            ));
        }
    }
    warnings
}

#[cfg(test)]
mod cidr_warning_tests {
    use super::*;

    /// `TestGetWarningsForCIDR` (apimachinery/pkg/util/validation/ip_test.go:530-605),
    /// less the leading-zero IPv4 cases the Rust parser rejects outright.
    #[test]
    fn warnings_match_upstream() {
        let path = Path::new("spec").child("loadBalancerSourceRanges").index(0);
        let cases: [(&str, &[&str]); 6] = [
            ("192.12.2.0/24", &[]),
            ("2001:db8::/64", &[]),
            (
                "192.12.2.0/024",
                &[
                    r#"spec.loadBalancerSourceRanges[0]: non-standard CIDR value "192.12.2.0/024" will be considered invalid in a future Kubernetes release: use "192.12.2.0/24""#,
                ],
            ),
            (
                "::ffff:192.12.2.0/120",
                &[
                    r#"spec.loadBalancerSourceRanges[0]: non-standard CIDR value "::ffff:192.12.2.0/120" will be considered invalid in a future Kubernetes release: use "192.12.2.0/24""#,
                ],
            ),
            (
                "192.12.2.8/24",
                &[
                    r#"spec.loadBalancerSourceRanges[0]: CIDR value "192.12.2.8/24" is ambiguous in this context (should be "192.12.2.0/24" or "192.12.2.8/32"?)"#,
                ],
            ),
            (
                "2001:db8:0:0::/64",
                &[
                    r#"spec.loadBalancerSourceRanges[0]: IPv6 CIDR value "2001:db8:0:0::/64" should be in RFC 5952 canonical format ("2001:db8::/64")"#,
                ],
            ),
        ];
        for (cidr, want) in cases {
            assert_eq!(get_warnings_for_cidr(&path, cidr), want, "{cidr}");
        }
    }
}
