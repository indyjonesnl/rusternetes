//! NetworkPolicy validation — port of upstream Kubernetes
//! `pkg/apis/networking/validation/validation.go::ValidateNetworkPolicySpec`
//! (release-1.35).
//!
//! ipBlock is fully validated: the cidr and each `except` are syntactically
//! valid CIDRs, and each `except` is a strict subset of the cidr (contained in
//! it, with a longer prefix) — mirroring upstream `ValidateIPBlock`.

use std::net::IpAddr;

use crate::resources::networking::{
    IPBlock, NetworkPolicy, NetworkPolicyPeer, NetworkPolicyPort, NetworkPolicySpec,
};
use crate::resources::policy::IntOrString;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::ingress::parse_ip_sloppy;
use crate::validation::metav1::{validate_label_selector, LabelSelectorValidationOptions};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};

const MIN_PORT: i64 = 1;
const MAX_PORT: i64 = 65535;

/// Upstream pkg/apis/networking/validation/validation.go:59. The strict CIDR
/// feature gate defaults false in release-1.35 (pkg/features/kube_features.go:1844).
#[derive(Default)]
struct NetworkPolicyValidationOptions<'a> {
    allow_invalid_label_value_in_selector: bool,
    allow_cidrs_even_if_invalid: Vec<&'a str>,
}

impl NetworkPolicyValidationOptions<'_> {
    fn selector_options(&self) -> LabelSelectorValidationOptions {
        LabelSelectorValidationOptions {
            allow_invalid_label_value_in_selector: self.allow_invalid_label_value_in_selector,
            ..Default::default()
        }
    }
}

/// Mask `bits` to its leading `prefix` bits within a `width`-bit address.
fn mask_bits(bits: u128, prefix: u32, width: u32) -> u128 {
    if prefix == 0 {
        return 0;
    }
    if prefix >= width {
        return bits;
    }
    let host = width - prefix;
    (bits >> host) << host
}

/// ParseCIDRSloppy: vendor/k8s.io/utils/internal/third_party/forked/golang/net/
/// ip.go:206. Preserve the original address family's mask width even for an
/// IPv4-mapped IPv6 address; ValidateIPBlock compares the original mask sizes.
fn parse_cidr_network(s: &str) -> Option<(u128, u8, bool)> {
    let (ip, prefix) = s.split_once('/')?;
    if prefix.is_empty() || !prefix.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let prefix: u8 = prefix.parse().ok()?;
    match parse_ip_sloppy(ip)? {
        IpAddr::V4(v4) if prefix <= 32 => {
            let bits = u32::from(v4) as u128;
            Some((mask_bits(bits, prefix as u32, 32), prefix, false))
        }
        IpAddr::V6(v6) if prefix <= 128 => {
            let bits = u128::from(v6);
            Some((mask_bits(bits, prefix as u32, 128), prefix, true))
        }
        _ => None,
    }
}

/// Upstream `IsValidCIDRForLegacyField` with the default strict-validation
/// feature gate disabled (apimachinery/pkg/util/validation/ip.go:132-191).
fn is_valid_cidr(s: &str) -> bool {
    parse_cidr_network(s).is_some()
}

/// Upstream ValidateIPBlock's `!cidr.Contains(except.IP) || cidrMask >= exceptMask`.
/// Go's net.IPNet.Contains converts mapped IPv6 network addresses to IPv4 for
/// containment, while Mask.Size still reports their original IPv6 prefix.
fn is_strict_subset(except: &str, cidr: &str) -> bool {
    let Some((ex_net, ex_prefix, ex_v6)) = parse_cidr_network(except) else {
        return false;
    };
    let Some((cidr_net, cidr_prefix, cidr_v6)) = parse_cidr_network(cidr) else {
        return false;
    };
    if ex_prefix <= cidr_prefix {
        return false;
    }
    let (ex_net, ex_v6) = if ex_v6 && ex_net >> 32 == 0xffff {
        (ex_net & u128::from(u32::MAX), false)
    } else {
        (ex_net, ex_v6)
    };
    let (cidr_net, cidr_prefix, cidr_v6) = if cidr_v6 && cidr_net >> 32 == 0xffff {
        (cidr_net & u128::from(u32::MAX), cidr_prefix - 96, false)
    } else {
        (cidr_net, cidr_prefix, cidr_v6)
    };
    if ex_v6 != cidr_v6 {
        return false;
    }
    let width = if cidr_v6 { 128 } else { 32 };
    mask_bits(ex_net, cidr_prefix as u32, width) == cidr_net
}

/// Upstream staging/src/k8s.io/apimachinery/pkg/util/validation/validation.go:321,
/// `IsValidPortName`. Keep the individual errors and their order.
fn port_name_errors(port: &str) -> Vec<&'static str> {
    let mut errs = Vec::new();
    if port.len() > 15 {
        errs.push("must be no more than 15 characters");
    }
    if port.is_empty()
        || !port
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        errs.push("must contain only alpha-numeric characters (a-z, 0-9), and hyphens (-)");
    }
    if !port.bytes().any(|b| b.is_ascii_lowercase()) {
        errs.push("must contain at least one letter (a-z)");
    }
    if port.contains("--") {
        errs.push("must not contain consecutive hyphens");
    }
    if port.starts_with('-') || port.ends_with('-') {
        errs.push("must not begin or end with a hyphen");
    }
    errs
}

/// Port-level validation. Mirrors upstream `ValidateNetworkPolicyPort`.
fn validate_port(port: &NetworkPolicyPort, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if !matches!(port.protocol.as_str(), "TCP" | "UDP" | "SCTP") {
        errs.push(Error::not_supported(
            &fld_path.child("protocol"),
            port.protocol.clone(),
            &["TCP", "UDP", "SCTP"],
        ));
    }

    match &port.port {
        None => {
            if let Some(ep) = port.end_port {
                errs.push(Error::invalid(
                    &fld_path.child("endPort"),
                    ep,
                    "may not be specified when `port` is not specified",
                ));
            }
        }
        Some(IntOrString::Int(n)) => {
            let p = *n as i64;
            if !(MIN_PORT..=MAX_PORT).contains(&p) {
                errs.push(Error::invalid(
                    &fld_path.child("port"),
                    p,
                    "must be between 1 and 65535, inclusive",
                ));
            }
            if let Some(ep) = port.end_port {
                if (ep as i64) < p {
                    errs.push(Error::invalid(
                        &fld_path.child("endPort"),
                        *n,
                        "must be greater than or equal to `port`",
                    ));
                }
                if !(MIN_PORT..=MAX_PORT).contains(&(ep as i64)) {
                    errs.push(Error::invalid(
                        &fld_path.child("endPort"),
                        ep,
                        "must be between 1 and 65535, inclusive",
                    ));
                }
            }
        }
        Some(IntOrString::String(s)) => {
            if let Some(ep) = port.end_port {
                errs.push(Error::invalid(
                    &fld_path.child("endPort"),
                    ep,
                    "may not be specified when `port` is non-numeric",
                ));
            }
            for message in port_name_errors(s) {
                errs.push(Error::invalid(&fld_path.child("port"), s.clone(), message));
            }
        } // No catch-all arm: `IntOrString` has exactly the two variants
          // upstream's `intstr.IntOrString` can hold, so "must be an integer or
          // string" is now unrepresentable rather than a runtime check. The old
          // `serde_json::Value` type could carry `true`, `[]`, `{}` or a float
          // this far.
    }

    errs
}

/// Upstream pkg/apis/networking/validation/validation.go:246, `ValidateIPBlock`.
fn validate_ip_block(
    ipb: &IPBlock,
    fld_path: &Path,
    opts: &NetworkPolicyValidationOptions<'_>,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if ipb.cidr.is_empty() {
        errs.push(Error::required(&fld_path.child("cidr"), ""));
        return errs;
    }
    if !is_valid_cidr(&ipb.cidr)
        && !opts
            .allow_cidrs_even_if_invalid
            .contains(&ipb.cidr.as_str())
    {
        errs.push(Error::invalid(
            &fld_path.child("cidr"),
            ipb.cidr.clone(),
            "must be a valid CIDR value, (e.g. 10.9.8.0/24 or 2001:db8::/64)",
        ));
    }
    if parse_cidr_network(&ipb.cidr).is_none() {
        return errs;
    }
    if let Some(except) = &ipb.except {
        for (i, ex) in except.iter().enumerate() {
            if !is_valid_cidr(ex) {
                if !opts.allow_cidrs_even_if_invalid.contains(&ex.as_str()) {
                    errs.push(Error::invalid(
                        &fld_path.child("except").index(i),
                        ex.clone(),
                        "must be a valid CIDR value, (e.g. 10.9.8.0/24 or 2001:db8::/64)",
                    ));
                }
            } else if !is_strict_subset(ex, &ipb.cidr) {
                errs.push(Error::invalid(
                    &fld_path.child("except").index(i),
                    ex.clone(),
                    "must be a strict subset of `cidr`",
                ));
            }
        }
    }
    errs
}

/// Peer validation. Mirrors upstream `ValidateNetworkPolicyPeer`.
fn validate_peer(
    peer: &NetworkPolicyPeer,
    fld_path: &Path,
    opts: &NetworkPolicyValidationOptions<'_>,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut num_peers = 0;

    if let Some(ps) = &peer.pod_selector {
        num_peers += 1;
        errs.extend(validate_label_selector(
            ps,
            opts.selector_options(),
            &fld_path.child("podSelector"),
        ));
    }
    if let Some(ns) = &peer.namespace_selector {
        num_peers += 1;
        errs.extend(validate_label_selector(
            ns,
            opts.selector_options(),
            &fld_path.child("namespaceSelector"),
        ));
    }
    if let Some(ipb) = &peer.ip_block {
        num_peers += 1;
        errs.extend(validate_ip_block(ipb, &fld_path.child("ipBlock"), opts));
    }

    if num_peers == 0 {
        errs.push(Error::required(fld_path, "must specify a peer"));
    } else if num_peers > 1 && peer.ip_block.is_some() {
        errs.push(Error::forbidden(
            fld_path,
            "may not specify both ipBlock and another peer",
        ));
    }

    errs
}

/// Validate a `NetworkPolicySpec`. Mirrors upstream `ValidateNetworkPolicySpec`.
pub fn validate_network_policy_spec(spec: &NetworkPolicySpec, fld_path: &Path) -> ErrorList {
    validate_network_policy_spec_with_options(
        spec,
        fld_path,
        &NetworkPolicyValidationOptions::default(),
    )
}

fn validate_network_policy_spec_with_options(
    spec: &NetworkPolicySpec,
    fld_path: &Path,
    opts: &NetworkPolicyValidationOptions<'_>,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    errs.extend(validate_label_selector(
        &spec.pod_selector,
        opts.selector_options(),
        &fld_path.child("podSelector"),
    ));

    if let Some(ingress) = &spec.ingress {
        for (i, rule) in ingress.iter().enumerate() {
            let rule_path = fld_path.child("ingress").index(i);
            if let Some(ports) = &rule.ports {
                for (j, p) in ports.iter().enumerate() {
                    errs.extend(validate_port(p, &rule_path.child("ports").index(j)));
                }
            }
            if let Some(from) = &rule.from {
                for (j, peer) in from.iter().enumerate() {
                    errs.extend(validate_peer(peer, &rule_path.child("from").index(j), opts));
                }
            }
        }
    }
    if let Some(egress) = &spec.egress {
        for (i, rule) in egress.iter().enumerate() {
            let rule_path = fld_path.child("egress").index(i);
            if let Some(ports) = &rule.ports {
                for (j, p) in ports.iter().enumerate() {
                    errs.extend(validate_port(p, &rule_path.child("ports").index(j)));
                }
            }
            if let Some(to) = &rule.to {
                for (j, peer) in to.iter().enumerate() {
                    errs.extend(validate_peer(peer, &rule_path.child("to").index(j), opts));
                }
            }
        }
    }

    // policyTypes: at most two, each Ingress or Egress.
    if let Some(types) = &spec.policy_types {
        if types.len() > 2 {
            errs.push(Error::invalid(
                &fld_path.child("policyTypes"),
                serde_json::json!(types),
                "may not specify more than two policyTypes",
            ));
            return errs;
        }
        for (i, t) in types.iter().enumerate() {
            if t != "Ingress" && t != "Egress" {
                errs.push(Error::not_supported(
                    &fld_path.child("policyTypes").index(i),
                    t.clone(),
                    &["Ingress", "Egress"],
                ));
            }
        }
    }

    errs
}

/// Validate a new `NetworkPolicy`. Mirrors upstream `ValidateNetworkPolicy`.
pub fn validate_network_policy(np: &NetworkPolicy) -> ErrorList {
    validate_network_policy_with_options(np, &NetworkPolicyValidationOptions::default())
}

fn validate_network_policy_with_options(
    np: &NetworkPolicy,
    opts: &NetworkPolicyValidationOptions<'_>,
) -> ErrorList {
    let mut errs = validate_object_meta(
        &np.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_network_policy_spec_with_options(
        &np.spec,
        &Path::new("spec"),
        opts,
    ));
    errs
}

/// Full strategy update validation: upstream pkg/registry/networking/networkpolicy/
/// strategy.go:86-90 calls BOTH ValidateNetworkPolicy and ValidateNetworkPolicyUpdate
/// (pkg/apis/networking/validation/validation.go:188-238), preserving their order.
pub fn validate_network_policy_update(new: &NetworkPolicy, old: &NetworkPolicy) -> ErrorList {
    // Only the OLD TOP-LEVEL selector enables expression-value compatibility.
    // Match-label values, label keys and selector operators remain validated.
    let mut opts = NetworkPolicyValidationOptions {
        allow_invalid_label_value_in_selector: !validate_label_selector(
            &old.spec.pod_selector,
            LabelSelectorValidationOptions::default(),
            &Path::new("spec").child("podSelector"),
        )
        .is_empty(),
        ..Default::default()
    };
    let mut errs = validate_network_policy_with_options(new, &opts);
    for peer in old
        .spec
        .ingress
        .iter()
        .flatten()
        .flat_map(|r| r.from.iter().flatten())
        .chain(
            old.spec
                .egress
                .iter()
                .flatten()
                .flat_map(|r| r.to.iter().flatten()),
        )
    {
        if let Some(block) = &peer.ip_block {
            opts.allow_cidrs_even_if_invalid.push(&block.cidr);
            opts.allow_cidrs_even_if_invalid
                .extend(block.except.iter().flatten().map(String::as_str));
        }
    }
    errs.extend(validate_object_meta_update(
        &new.metadata,
        &old.metadata,
        &Path::new("metadata"),
    ));
    errs.extend(validate_network_policy_spec_with_options(
        &new.spec,
        &Path::new("spec"),
        &opts,
    ));
    errs
}

#[cfg(test)]
mod ip_block_tests {
    use super::*;

    fn ipb_errs(json: serde_json::Value) -> Vec<String> {
        let ipb: IPBlock = serde_json::from_value(json).unwrap();
        validate_ip_block(
            &ipb,
            &Path::new("ipBlock"),
            &NetworkPolicyValidationOptions::default(),
        )
        .into_iter()
        .map(|e| e.to_string())
        .collect()
    }

    #[test]
    fn valid_strict_subset_passes() {
        assert!(ipb_errs(serde_json::json!({
            "cidr": "10.0.0.0/8", "except": ["10.1.0.0/16", "10.2.3.0/24"]
        }))
        .is_empty());
    }

    #[test]
    fn except_not_contained_rejected() {
        let e = ipb_errs(serde_json::json!({
            "cidr": "10.0.0.0/8", "except": ["192.168.0.0/16"]
        }));
        assert!(e.iter().any(|m| m.contains("strict subset")), "{e:?}");
    }

    #[test]
    fn except_equal_or_shorter_prefix_rejected() {
        // same prefix as cidr -> not strict (prefix must be longer)
        let e = ipb_errs(serde_json::json!({
            "cidr": "10.0.0.0/8", "except": ["10.0.0.0/8"]
        }));
        assert!(e.iter().any(|m| m.contains("strict subset")), "{e:?}");
    }

    #[test]
    fn ipv6_strict_subset_passes() {
        assert!(ipb_errs(serde_json::json!({
            "cidr": "2001:db8::/32", "except": ["2001:db8:1::/48"]
        }))
        .is_empty());
    }

    #[test]
    fn mixed_family_except_rejected() {
        let e = ipb_errs(serde_json::json!({
            "cidr": "10.0.0.0/8", "except": ["2001:db8::/64"]
        }));
        assert!(e.iter().any(|m| m.contains("strict subset")), "{e:?}");
    }

    #[test]
    fn invalid_cidr_still_rejected() {
        let e = ipb_errs(serde_json::json!({"cidr": "10.0.0.0/8", "except": ["notacidr"]}));
        assert!(e.iter().any(|m| m.contains("valid CIDR")), "{e:?}");
    }
}
