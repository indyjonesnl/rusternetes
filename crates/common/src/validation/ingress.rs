//! Ingress validators ported from Kubernetes release-1.35
//! `pkg/apis/networking/validation/validation.go:293-553,592-622,670-744`.
//! Feature gates follow the upstream defaults: relaxed service names and strict
//! legacy IP validation are disabled (`pkg/features/kube_features.go:1708,1844`).

use std::net::{IpAddr, Ipv4Addr};

use crate::resources::ingress::{
    HTTPIngressPath, Ingress, IngressBackend, IngressLoadBalancerStatus, IngressRule, IngressSpec,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1035_label, is_dns1123_label, is_dns1123_subdomain};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, name_is_path_segment, validate_object_meta, validate_object_meta_update,
};
use once_cell::sync::Lazy;
use regex::Regex;

#[derive(Clone, Copy, Default)]
struct IngressValidationOptions {
    allow_invalid_secret_name: bool,
    allow_invalid_wildcard_host_rule: bool,
    allow_relaxed_service_name_validation: bool,
}

const DNS1123_SUBDOMAIN_MAX_LENGTH: usize = 253;

/// Upstream `invalidPathSequences` — substrings forbidden anywhere in an
/// Exact/Prefix path.
const INVALID_PATH_SEQUENCES: &[&str] = &["//", "/./", "/../", "%2f", "%2F"];

/// Upstream `invalidPathSuffixes` — suffixes forbidden on an Exact/Prefix path.
const INVALID_PATH_SUFFIXES: &[&str] = &["/..", "/."];

/// Upstream `wildcardDNS1123SubdomainFmt` = `\*\.` + `dns1123SubdomainFmt`.
static WILDCARD_DNS1123_SUBDOMAIN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^\*\.[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")
        .expect("wildcard dns1123 subdomain regex")
});

/// Port of upstream `validation.IsWildcardDNS1123Subdomain`: `*.` followed by a
/// valid DNS-1123 subdomain.
fn is_wildcard_dns1123_subdomain(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if value.len() > DNS1123_SUBDOMAIN_MAX_LENGTH {
        errs.push(format!(
            "must be no more than {DNS1123_SUBDOMAIN_MAX_LENGTH} characters"
        ));
    }
    if !WILDCARD_DNS1123_SUBDOMAIN_RE.is_match(value) {
        errs.push("a wildcard DNS-1123 subdomain must start with '*.', followed by a valid DNS subdomain, which must consist of lower case alphanumeric characters, '-' or '.' and end with an alphanumeric character (e.g. '*.example.com', regex used for validation is '\\*\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')".to_string());
    }
    errs
}

/// `netutils.ParseIPSloppy`, including leading zeros in embedded IPv4 tails.
/// Upstream vendor/k8s.io/utils/internal/third_party/forked/golang/net/ip.go:42.
pub(crate) fn parse_ip_sloppy(value: &str) -> Option<IpAddr> {
    if let Ok(ip) = value.parse() {
        return Some(ip);
    }
    let (prefix, dotted) = value
        .rsplit_once(':')
        .map_or((None, value), |(prefix, tail)| (Some(prefix), tail));
    let mut parts = dotted.split('.');
    let mut octets = [0; 4];
    for octet in &mut octets {
        let part = parts.next()?;
        if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        *octet = part
            .bytes()
            .try_fold(0u8, |n, b| n.checked_mul(10)?.checked_add(b - b'0'))?;
    }
    if parts.next().is_some() {
        return None;
    }
    let ipv4 = Ipv4Addr::from(octets);
    match prefix {
        Some(prefix) => format!("{prefix}:{ipv4}").parse().ok(),
        None => Some(IpAddr::V4(ipv4)),
    }
}

fn validate_ingress_tls_with_options(
    spec: &IngressSpec,
    fld_path: &Path,
    opts: IngressValidationOptions,
) -> ErrorList {
    let mut errs = Vec::new();
    for (ti, tls) in spec.tls.iter().flatten().enumerate() {
        let tls_path = fld_path.index(ti);
        for (hi, host) in tls.hosts.iter().flatten().enumerate() {
            let messages = if host.contains('*') {
                is_wildcard_dns1123_subdomain(host)
            } else {
                is_dns1123_subdomain(host)
            };
            for message in messages {
                errs.push(Error::invalid(
                    &tls_path.child("hosts").index(hi),
                    host.clone(),
                    message,
                ));
            }
        }
        if !opts.allow_invalid_secret_name {
            if let Some(secret) = tls.secret_name.as_deref().filter(|s| !s.is_empty()) {
                for message in is_dns1123_subdomain(secret) {
                    errs.push(Error::invalid(
                        &tls_path.child("secretName"),
                        secret.to_string(),
                        message,
                    ));
                }
            }
        }
    }
    errs
}

#[cfg(test)]
fn validate_ingress_tls(spec: &IngressSpec, fld_path: &Path) -> ErrorList {
    validate_ingress_tls_with_options(spec, fld_path, IngressValidationOptions::default())
}

/// Upstream staging/src/k8s.io/apimachinery/pkg/util/validation/validation.go:321.
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

fn validate_backend(
    backend: &IngressBackend,
    fld_path: &Path,
    opts: IngressValidationOptions,
) -> ErrorList {
    let mut errs = Vec::new();
    match (&backend.service, &backend.resource) {
        (Some(_), Some(_)) => errs.push(Error::invalid(
            fld_path,
            "",
            "cannot set both resource and service backends",
        )),
        (None, None) => errs.push(Error::invalid(
            fld_path,
            "",
            "resource or service backend is required",
        )),
        (Some(service), None) => {
            if service.name.is_empty() {
                errs.push(Error::required(
                    &fld_path.child("service").child("name"),
                    "",
                ));
            } else {
                let messages = if opts.allow_relaxed_service_name_validation {
                    is_dns1123_label(&service.name)
                } else {
                    is_dns1035_label(&service.name)
                };
                for message in messages {
                    errs.push(Error::invalid(
                        &fld_path.child("service").child("name"),
                        service.name.clone(),
                        message,
                    ));
                }
            }
            let name = service
                .port
                .as_ref()
                .and_then(|port| port.name.as_deref())
                .unwrap_or("");
            let number = service
                .port
                .as_ref()
                .and_then(|port| port.number)
                .unwrap_or(0);
            if !name.is_empty() && number != 0 {
                errs.push(Error::invalid(
                    fld_path,
                    "",
                    "cannot set both port name & port number",
                ));
            } else if !name.is_empty() {
                for message in port_name_errors(name) {
                    errs.push(Error::invalid(
                        &fld_path.child("service").child("port").child("name"),
                        name.to_string(),
                        message,
                    ));
                }
            } else if number != 0 {
                if !(1..=65535).contains(&number) {
                    errs.push(Error::invalid(
                        &fld_path.child("service").child("port").child("number"),
                        number,
                        "must be between 1 and 65535, inclusive",
                    ));
                }
            } else {
                errs.push(Error::required(fld_path, "port name or number is required"));
            }
        }
        (None, Some(resource)) => {
            let path = fld_path.child("resource");
            if let Some(group) = &resource.api_group {
                for message in is_dns1123_subdomain(group) {
                    errs.push(Error::invalid(
                        &path.child("apiGroup"),
                        group.clone(),
                        message,
                    ));
                }
            }
            for (field, value) in [("kind", &resource.kind), ("name", &resource.name)] {
                if value.is_empty() {
                    errs.push(Error::required(&path.child(field), ""));
                } else {
                    for message in name_is_path_segment(value, false) {
                        errs.push(Error::invalid(&path.child(field), value.clone(), message));
                    }
                }
            }
        }
    }
    errs
}

fn validate_http_path(
    path: &HTTPIngressPath,
    fld_path: &Path,
    opts: IngressValidationOptions,
) -> ErrorList {
    let Some(path_type) = path.path_type.as_deref() else {
        return vec![Error::required(
            &fld_path.child("pathType"),
            "pathType must be specified",
        )];
    };
    let mut errs = Vec::new();
    let value = path.path.as_deref().unwrap_or("");
    match path_type {
        "Exact" | "Prefix" => {
            if !value.starts_with('/') {
                errs.push(Error::invalid(
                    &fld_path.child("path"),
                    value.to_string(),
                    "must be an absolute path",
                ));
            }
            if !value.is_empty() {
                for sequence in INVALID_PATH_SEQUENCES {
                    if value.contains(sequence) {
                        errs.push(Error::invalid(
                            &fld_path.child("path"),
                            value.to_string(),
                            format!("must not contain '{sequence}'"),
                        ));
                    }
                }
                for suffix in INVALID_PATH_SUFFIXES {
                    if value.ends_with(suffix) {
                        errs.push(Error::invalid(
                            &fld_path.child("path"),
                            value.to_string(),
                            format!("cannot end with '{suffix}'"),
                        ));
                    }
                }
            }
        }
        "ImplementationSpecific" => {
            if !value.is_empty() && !value.starts_with('/') {
                errs.push(Error::invalid(
                    &fld_path.child("path"),
                    value.to_string(),
                    "must be an absolute path",
                ));
            }
        }
        other => errs.push(Error::not_supported(
            &fld_path.child("pathType"),
            other.to_string(),
            &["Exact", "ImplementationSpecific", "Prefix"],
        )),
    }
    errs.extend(validate_backend(
        &path.backend,
        &fld_path.child("backend"),
        opts,
    ));
    errs
}

fn validate_rule_value(
    rule: &IngressRule,
    fld_path: &Path,
    opts: IngressValidationOptions,
) -> ErrorList {
    let mut errs = Vec::new();
    if let Some(http) = &rule.http {
        let paths = fld_path.child("http").child("paths");
        if http.paths.is_empty() {
            errs.push(Error::required(&paths, ""));
        }
        for (i, path) in http.paths.iter().enumerate() {
            errs.extend(validate_http_path(path, &paths.index(i), opts));
        }
    }
    errs
}

fn validate_ingress_spec_with_options(
    spec: &IngressSpec,
    fld_path: &Path,
    opts: IngressValidationOptions,
) -> ErrorList {
    let mut errs = Vec::new();
    let rules = spec.rules.as_deref().unwrap_or(&[]);
    if rules.is_empty() && spec.default_backend.is_none() {
        errs.push(Error::invalid(
            fld_path,
            serde_json::json!(spec.rules),
            "either `defaultBackend` or `rules` must be specified",
        ));
    }
    if let Some(backend) = &spec.default_backend {
        errs.extend(validate_backend(
            backend,
            &fld_path.child("defaultBackend"),
            opts,
        ));
    }
    for (i, rule) in rules.iter().enumerate() {
        let rule_path = fld_path.child("rules").index(i);
        let host = rule.host.as_deref().unwrap_or("");
        let wildcard = host.contains('*');
        if !host.is_empty() {
            if parse_ip_sloppy(host).is_some() {
                errs.push(Error::invalid(
                    &rule_path.child("host"),
                    host.to_string(),
                    "must be a DNS name, not an IP address",
                ));
            }
            let messages = if wildcard {
                is_wildcard_dns1123_subdomain(host)
            } else {
                is_dns1123_subdomain(host)
            };
            for message in messages {
                errs.push(Error::invalid(
                    &rule_path.child("host"),
                    host.to_string(),
                    message,
                ));
            }
        }
        if !wildcard || !opts.allow_invalid_wildcard_host_rule {
            errs.extend(validate_rule_value(rule, &rule_path, opts));
        }
    }
    errs.extend(validate_ingress_tls_with_options(
        spec,
        &fld_path.child("tls"),
        opts,
    ));
    if let Some(class) = &spec.ingress_class_name {
        for message in is_dns1123_subdomain(class) {
            errs.push(Error::invalid(
                &fld_path.child("ingressClassName"),
                class.clone(),
                message,
            ));
        }
    }
    errs
}

/// Validate the spec with upstream default feature gates.
pub fn validate_ingress_spec(spec: &IngressSpec, fld_path: &Path) -> ErrorList {
    validate_ingress_spec_with_options(spec, fld_path, IngressValidationOptions::default())
}

fn validate_ingress_with_options(ingress: &Ingress, opts: IngressValidationOptions) -> ErrorList {
    let mut errs = validate_object_meta(
        &ingress.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    if let Some(spec) = &ingress.spec {
        errs.extend(validate_ingress_spec_with_options(
            spec,
            &Path::new("spec"),
            opts,
        ));
    } else {
        // Upstream Spec is a value, so an omitted spec has no rules or backend.
        errs.push(Error::invalid(
            &Path::new("spec"),
            serde_json::Value::Null,
            "either `defaultBackend` or `rules` must be specified",
        ));
    }
    errs
}

/// Upstream ValidateIngressCreate, including the create-only class check.
pub fn validate_ingress_create(ingress: &Ingress) -> ErrorList {
    let mut errs = validate_ingress_with_options(ingress, IngressValidationOptions::default());
    let annotation = ingress
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("kubernetes.io/ingress.class"));
    let class = ingress
        .spec
        .as_ref()
        .and_then(|s| s.ingress_class_name.as_ref());
    if let (Some(annotation), Some(class)) = (annotation, class) {
        if annotation != class {
            errs.push(Error::invalid(
                &Path::new("annotations").child("kubernetes.io/ingress.class"),
                annotation.clone(),
                "must match `ingressClassName` when both are specified",
            ));
        }
    }
    errs
}

/// Compatibility entry point for validating a newly created Ingress.
pub fn validate_ingress(ingress: &Ingress) -> ErrorList {
    validate_ingress_create(ingress)
}

/// Upstream ValidateIngressUpdate and the old-object compatibility gates
/// (`validation.go:318-327,670-744`). Each gate applies to the entire new spec.
pub fn validate_ingress_update(ingress: &Ingress, old_ingress: &Ingress) -> ErrorList {
    let mut errs = validate_object_meta_update(
        &ingress.metadata,
        &old_ingress.metadata,
        &Path::new("metadata"),
    );
    let mut opts = IngressValidationOptions::default();
    if let Some(old) = &old_ingress.spec {
        opts.allow_invalid_secret_name = old.tls.iter().flatten().any(|tls| {
            tls.secret_name
                .as_deref()
                .is_some_and(|name| !name.is_empty() && !is_dns1123_subdomain(name).is_empty())
        });
        opts.allow_invalid_wildcard_host_rule = old.rules.iter().flatten().any(|rule| {
            rule.host.as_deref().is_some_and(|host| host.contains('*'))
                && !validate_rule_value(rule, &Path::new(""), IngressValidationOptions::default())
                    .is_empty()
        });
        let backends = old.default_backend.iter().chain(
            old.rules
                .iter()
                .flatten()
                .filter_map(|r| r.http.as_ref())
                .flat_map(|http| http.paths.iter().map(|p| &p.backend)),
        );
        opts.allow_relaxed_service_name_validation =
            backends.filter_map(|b| b.service.as_ref()).any(|service| {
                is_dns1123_label(&service.name).is_empty()
                    && !is_dns1035_label(&service.name).is_empty()
            });
    }
    errs.extend(validate_ingress_with_options(ingress, opts));
    errs
}

/// Upstream ValidateIngressLoadBalancerStatus (`validation.go:389-416`).
/// Ports are deliberately unconstrained upstream. Legacy IP validation follows
/// the disabled StrictIPCIDRValidation default and accepts exact old values.
pub fn validate_ingress_load_balancer_status(
    status: &IngressLoadBalancerStatus,
    old_status: Option<&IngressLoadBalancerStatus>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();
    for (i, ingress) in status.ingress.iter().flatten().enumerate() {
        let path = fld_path.child("ingress").index(i);
        if let Some(ip) = ingress.ip.as_deref().filter(|ip| !ip.is_empty()) {
            let unchanged = old_status
                .into_iter()
                .flat_map(|s| s.ingress.iter().flatten())
                .any(|old| old.ip.as_deref() == Some(ip));
            if !unchanged && parse_ip_sloppy(ip).is_none() {
                let mut error = Error::invalid(
                    &path.child("ip"),
                    ip.to_string(),
                    "must be a valid IP address, (e.g. 10.9.8.7 or 2001:db8::ffff)",
                );
                error.origin = "format=ip-sloppy".to_string();
                errs.push(error);
            }
        }
        if let Some(host) = ingress.hostname.as_deref().filter(|host| !host.is_empty()) {
            for message in is_dns1123_subdomain(host) {
                errs.push(Error::invalid(
                    &path.child("hostname"),
                    host.to_string(),
                    message,
                ));
            }
            if parse_ip_sloppy(host).is_some() {
                errs.push(Error::invalid(
                    &path.child("hostname"),
                    host.to_string(),
                    "must be a DNS name, not an IP address",
                ));
            }
        }
    }
    errs
}

/// Upstream ValidateIngressStatusUpdate (`validation.go:381-385`): metadata
/// update checks and load-balancer validation, without spec validation.
pub fn validate_ingress_status_update(ingress: &Ingress, old_ingress: &Ingress) -> ErrorList {
    let mut errs = validate_object_meta_update(
        &ingress.metadata,
        &old_ingress.metadata,
        &Path::new("metadata"),
    );
    if let Some(status) = ingress
        .status
        .as_ref()
        .and_then(|s| s.load_balancer.as_ref())
    {
        let old = old_ingress
            .status
            .as_ref()
            .and_then(|s| s.load_balancer.as_ref());
        errs.extend(validate_ingress_load_balancer_status(
            status,
            old,
            &Path::new("status").child("loadBalancer"),
        ));
    }
    errs
}

#[cfg(test)]
mod tls_tests {
    use super::*;

    fn tls_errs(json: serde_json::Value) -> Vec<String> {
        let spec: IngressSpec = serde_json::from_value(json).unwrap();
        validate_ingress_tls(&spec, &Path::new("spec").child("tls"))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    fn base(tls: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "defaultBackend": {"service": {"name": "s", "port": {"number": 80}}},
            "tls": tls
        })
    }

    #[test]
    fn valid_tls_passes() {
        assert!(tls_errs(base(serde_json::json!([
            {"hosts": ["example.com", "*.example.com"], "secretName": "my-tls"}
        ])))
        .is_empty());
    }

    #[test]
    fn invalid_host_rejected() {
        let e = tls_errs(base(serde_json::json!([{"hosts": ["NotADNSName"]}])));
        assert!(e.iter().any(|m| m.contains("hosts")), "{e:?}");
    }

    #[test]
    fn invalid_wildcard_host_rejected() {
        // bare "*" is not a valid wildcard subdomain (needs "*.")
        let e = tls_errs(base(serde_json::json!([{"hosts": ["*"]}])));
        assert!(e.iter().any(|m| m.contains("wildcard")), "{e:?}");
    }

    #[test]
    fn invalid_secret_name_rejected() {
        let e = tls_errs(base(
            serde_json::json!([{"hosts": ["example.com"], "secretName": "Bad_Name"}]),
        ));
        assert!(e.iter().any(|m| m.contains("secretName")), "{e:?}");
    }
}

#[cfg(test)]
mod spec_tests {
    use super::*;

    fn spec_errs(json: serde_json::Value) -> Vec<String> {
        let spec: IngressSpec = serde_json::from_value(json).unwrap();
        validate_ingress_spec(&spec, &Path::new("spec"))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    /// A spec with a single rule whose HTTP path uses the given pathType/path.
    fn rule_with_path(path_type: &str, path: &str) -> serde_json::Value {
        serde_json::json!({
            "rules": [{
                "http": {
                    "paths": [{
                        "path": path,
                        "pathType": path_type,
                        "backend": {"service": {"name": "s", "port": {"number": 80}}}
                    }]
                }
            }]
        })
    }

    #[test]
    fn valid_prefix_path_passes() {
        assert!(
            spec_errs(rule_with_path("Prefix", "/foo/bar")).is_empty(),
            "{:?}",
            spec_errs(rule_with_path("Prefix", "/foo/bar"))
        );
    }

    #[test]
    fn double_slash_sequence_rejected() {
        let e = spec_errs(rule_with_path("Prefix", "/foo//bar"));
        assert!(
            e.iter().any(|m| m.contains("must not contain '//'")),
            "{e:?}"
        );
    }

    #[test]
    fn dot_dot_sequence_rejected() {
        let e = spec_errs(rule_with_path("Exact", "/foo/../bar"));
        assert!(
            e.iter().any(|m| m.contains("must not contain '/../'")),
            "{e:?}"
        );
    }

    #[test]
    fn percent_encoded_slash_rejected() {
        let e = spec_errs(rule_with_path("Prefix", "/foo%2Fbar"));
        assert!(
            e.iter().any(|m| m.contains("must not contain '%2F'")),
            "{e:?}"
        );
        let e2 = spec_errs(rule_with_path("Prefix", "/foo%2fbar"));
        assert!(
            e2.iter().any(|m| m.contains("must not contain '%2f'")),
            "{e2:?}"
        );
    }

    #[test]
    fn dot_dot_suffix_rejected() {
        let e = spec_errs(rule_with_path("Prefix", "/foo/.."));
        assert!(
            e.iter().any(|m| m.contains("cannot end with '/..'")),
            "{e:?}"
        );
    }

    #[test]
    fn dot_suffix_rejected() {
        let e = spec_errs(rule_with_path("Prefix", "/foo/."));
        assert!(
            e.iter().any(|m| m.contains("cannot end with '/.'")),
            "{e:?}"
        );
    }

    #[test]
    fn implementation_specific_skips_sequence_checks() {
        // ImplementationSpecific only checks the absolute-path prefix, not the
        // invalid-sequence/suffix rules.
        assert!(
            spec_errs(rule_with_path("ImplementationSpecific", "/foo//bar")).is_empty(),
            "{:?}",
            spec_errs(rule_with_path("ImplementationSpecific", "/foo//bar"))
        );
    }

    fn rule_with_host(host: &str) -> serde_json::Value {
        serde_json::json!({
            "rules": [{
                "host": host,
                "http": {
                    "paths": [{
                        "path": "/",
                        "pathType": "Prefix",
                        "backend": {"service": {"name": "s", "port": {"number": 80}}}
                    }]
                }
            }]
        })
    }

    #[test]
    fn wildcard_host_accepted() {
        assert!(
            spec_errs(rule_with_host("*.example.com")).is_empty(),
            "{:?}",
            spec_errs(rule_with_host("*.example.com"))
        );
    }

    #[test]
    fn bad_wildcard_host_rejected() {
        // bare "*" is not a valid wildcard subdomain (needs "*.")
        let e = spec_errs(rule_with_host("*"));
        assert!(e.iter().any(|m| m.contains("wildcard")), "{e:?}");
    }

    #[test]
    fn wildcard_not_at_start_rejected() {
        // "foo.*.example.com" does not match the wildcard format.
        let e = spec_errs(rule_with_host("foo.*.example.com"));
        assert!(e.iter().any(|m| m.contains("wildcard")), "{e:?}");
    }

    #[test]
    fn plain_host_still_validated() {
        let e = spec_errs(rule_with_host("Not_A_DNS_Name"));
        assert!(e.iter().any(|m| m.contains("host")), "{e:?}");
    }

    fn spec_with_class(class_name: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "ingressClassName": class_name,
            "defaultBackend": {"service": {"name": "s", "port": {"number": 80}}}
        })
    }

    #[test]
    fn valid_ingress_class_name_passes() {
        assert!(
            spec_errs(spec_with_class(serde_json::json!("nginx"))).is_empty(),
            "{:?}",
            spec_errs(spec_with_class(serde_json::json!("nginx")))
        );
    }

    #[test]
    fn invalid_ingress_class_name_rejected() {
        let e = spec_errs(spec_with_class(serde_json::json!("Bad_Class")));
        assert!(e.iter().any(|m| m.contains("ingressClassName")), "{e:?}");
    }
}
