//! Endpoints (core/v1) validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::validateEndpointSubsets`
//! (release-1.35).
//!
//! `ValidateEndpointsCreate` / `ValidateEndpointsUpdate`
//! (`validation.go:8229-8261`): ObjectMeta, then the subsets. Each subset must
//! carry addresses (the endpoint IPs must be valid and non-special) and
//! well-formed ports.

use std::net::IpAddr;
use std::str::FromStr;

use crate::equality::semantic_equal;
use crate::resources::endpoints::{EndpointAddress, EndpointPort, Endpoints};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_label;
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};

/// Mirrors upstream `ValidateEndpointIP`: a valid IP that is not unspecified,
/// loopback, or link-local.
fn validate_endpoint_ip(ip: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Ok(parsed) = IpAddr::from_str(ip) else {
        errs.push(Error::invalid(
            fld_path,
            ip.to_string(),
            "must be a valid IP address",
        ));
        return errs;
    };
    if parsed.is_unspecified() {
        errs.push(Error::invalid(
            fld_path,
            ip.to_string(),
            format!("may not be unspecified ({ip})"),
        ));
    }
    if parsed.is_loopback() {
        errs.push(Error::invalid(
            fld_path,
            ip.to_string(),
            "may not be in the loopback range (127.0.0.0/8, ::1/128)",
        ));
    }
    let link_local = match parsed {
        IpAddr::V4(v4) => v4.is_link_local(),
        // fe80::/10
        IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) == 0xfe80,
    };
    if link_local {
        errs.push(Error::invalid(
            fld_path,
            ip.to_string(),
            "may not be in the link-local range (169.254.0.0/16, fe80::/10)",
        ));
    }
    errs
}

/// Upstream `validateEndpointAddress` (validation.go:8288-8302): the IP, a
/// DNS-label hostname, and a DNS-subdomain nodeName (`ValidateNodeName` is
/// `NameIsDNSSubdomain`).
fn validate_endpoint_address(address: &EndpointAddress, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(hostname) = address.hostname.as_deref().filter(|h| !h.is_empty()) {
        for msg in is_dns1123_label(hostname) {
            errs.push(Error::invalid(
                &fld_path.child("hostname"),
                hostname.to_string(),
                msg,
            ));
        }
    }
    if let Some(node_name) = &address.node_name {
        for msg in name_is_dns_subdomain(node_name, false) {
            errs.push(Error::invalid(
                &fld_path.child("nodeName"),
                node_name.clone(),
                msg,
            ));
        }
    }
    errs.extend(validate_endpoint_ip(&address.ip, &fld_path.child("ip")));
    errs
}

fn validate_port(port: &EndpointPort, require_name: bool, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    // name: required when multi-port; when present it must be a DNS-1123 label.
    // Upstream `validateEndpointPort` (validation.go:8338-8342) validates the
    // port name with `ValidateDNS1123Label` — NOT the 15-char IANA_SVC_NAME
    // `IsValidPortName` rule (that one is reserved for ContainerPort names and
    // string targetPorts).
    match &port.name {
        Some(n) if !n.is_empty() => {
            for msg in is_dns1123_label(n) {
                errs.push(Error::invalid(&fld_path.child("name"), n.clone(), msg));
            }
        }
        _ => {
            if require_name {
                errs.push(Error::required(&fld_path.child("name"), ""));
            }
        }
    }
    if !(1..=65535).contains(&(port.port as i32)) {
        errs.push(Error::invalid(
            &fld_path.child("port"),
            port.port as i32,
            "must be between 1 and 65535, inclusive",
        ));
    }
    // protocol: required, then must be TCP/UDP/SCTP. Upstream
    // `validateEndpointPort` (validation.go:8346-8350) emits Required when
    // empty, NotSupported otherwise.
    match port.protocol.as_str() {
        "" => {
            errs.push(Error::required(&fld_path.child("protocol"), ""));
        }
        "TCP" | "UDP" | "SCTP" => {}
        other => {
            errs.push(Error::not_supported(
                &fld_path.child("protocol"),
                other.to_string(),
                &["TCP", "UDP", "SCTP"],
            ));
        }
    }
    errs
}

/// Upstream `validateEndpointSubsets` (validation.go:8263-8286).
fn validate_endpoint_subsets(endpoints: &Endpoints, subsets_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for (i, ss) in endpoints.subsets.iter().enumerate() {
        let idx = subsets_path.index(i);
        let addrs = ss.addresses.as_deref().unwrap_or(&[]);
        let not_ready = ss.not_ready_addresses.as_deref().unwrap_or(&[]);
        if addrs.is_empty() && not_ready.is_empty() {
            errs.push(Error::required(
                &idx,
                "must specify `addresses` or `notReadyAddresses`",
            ));
        }
        for (j, a) in addrs.iter().enumerate() {
            errs.extend(validate_endpoint_address(
                a,
                &idx.child("addresses").index(j),
            ));
        }
        for (j, a) in not_ready.iter().enumerate() {
            errs.extend(validate_endpoint_address(
                a,
                &idx.child("notReadyAddresses").index(j),
            ));
        }
        if let Some(ports) = &ss.ports {
            let require_name = ports.len() > 1;
            for (j, p) in ports.iter().enumerate() {
                errs.extend(validate_port(p, require_name, &idx.child("ports").index(j)));
            }
        }
    }
    errs
}

/// Upstream `ValidateEndpoints` (validation.go:8229-8246). `ValidateEndpointsName`
/// is `NameIsDNSSubdomain`, and `ValidateEndpointsSpecificAnnotations` checks
/// nothing. On update, subset errors are dropped when the subsets did not
/// change, "since apparently older versions of Kubernetes considered the data
/// valid".
pub fn validate_endpoints(endpoints: &Endpoints, old: Option<&Endpoints>) -> ErrorList {
    let mut errs = validate_object_meta(
        &endpoints.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    let subset_errs = validate_endpoint_subsets(endpoints, &Path::new("subsets"));
    let unchanged = old.is_some_and(|old| semantic_equal(&old.subsets, &endpoints.subsets));
    if !unchanged {
        errs.extend(subset_errs);
    }
    errs
}

/// Upstream `ValidateEndpointsCreate` (validation.go:8249-8251).
pub fn validate_endpoints_create(endpoints: &Endpoints) -> ErrorList {
    validate_endpoints(endpoints, None)
}

/// Upstream `ValidateEndpointsUpdate` (validation.go:8257-8261).
pub fn validate_endpoints_update(endpoints: &Endpoints, old: &Endpoints) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&endpoints.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_endpoints(endpoints, Some(old)));
    errs
}
