//! Service validation — port of `pkg/apis/core/validation/validation.go`
//! (release-1.35): `ValidateServiceCreate` (:6924-6936),
//! `ValidateServiceUpdate` (:6939-6959), `ValidateServiceStatusUpdate`
//! (:6962-6966), `validateService` (:6570-6778) and the helpers they call.
//!
//! Validation runs on the object after v1 defaulting (`SetDefaults_Service`)
//! and the Service REST's `beginCreate` / `beginUpdate`, which is why fields
//! such as `sessionAffinity`, `type` and `internalTrafficPolicy` are
//! `Required` here.
//!
//! Feature gates at their 1.35 defaults: `RelaxedServiceNameValidation` off
//! (names are DNS-1035 labels), `StrictIPCIDRValidation` off (IPs and CIDRs
//! parse "sloppily"), `PreferSameTrafficDistribution` GA.
//!
//! `type`, `ipFamilies`, `ipFamilyPolicy` and the traffic policies are closed
//! enums in [`ServiceSpec`], so a value outside the supported set fails to
//! decode rather than reaching the `NotSupported` checks upstream has for them.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};

use crate::resources::policy::IntOrString;
use crate::resources::service::{
    IPFamily, IPFamilyPolicy, LoadBalancerStatus, Service, ServiceExternalTrafficPolicy,
    ServiceInternalTrafficPolicy, ServicePort, ServiceType, SessionAffinityConfig,
};
use crate::validation::field::{BadValue, Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1035_label, is_dns1123_label, is_dns1123_subdomain, is_qualified_name, validate_labels,
};
use crate::validation::objectmeta::{validate_object_meta, validate_object_meta_update};

/// `core.MaxClientIPServiceAffinitySeconds` (pkg/apis/core/types.go).
const MAX_CLIENT_IP_SERVICE_AFFINITY_SECONDS: i32 = 86400;

/// `core.DeprecatedAnnotationTopologyAwareHints`.
const DEPRECATED_ANNOTATION_TOPOLOGY_AWARE_HINTS: &str =
    "service.kubernetes.io/topology-aware-hints";
/// `core.AnnotationTopologyMode`.
const ANNOTATION_TOPOLOGY_MODE: &str = "service.kubernetes.io/topology-mode";
/// `core.AnnotationLoadBalancerSourceRangesKey`.
const ANNOTATION_LOAD_BALANCER_SOURCE_RANGES: &str =
    "service.beta.kubernetes.io/load-balancer-source-ranges";

/// `supportedPortProtocols` (validation.go:2717-2721).
const SUPPORTED_PORT_PROTOCOLS: &[&str] = &["SCTP", "TCP", "UDP"];
/// `supportedSessionAffinityType` (validation.go:6557).
const SUPPORTED_SESSION_AFFINITY_TYPE: &[&str] = &["ClientIP", "None"];
/// `supportedLoadBalancerIPMode` (validation.go:8649).
const SUPPORTED_LOAD_BALANCER_IP_MODE: &[&str] = &["Proxy", "VIP"];
/// `supportedTrafficDistribution` with `PreferSameTrafficDistribution` on
/// (validation.go:6904-6913).
const SUPPORTED_TRAFFIC_DISTRIBUTION: &[&str] =
    &["PreferClose", "PreferSameZone", "PreferSameNode"];

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// `netutils.ParseIPSloppy` (k8s.io/utils/net/parse.go): like
/// `net.ParseIP`, which also accepts IPv4 octets with leading zeros (read
/// as decimal).
pub fn parse_ip_sloppy(value: &str) -> Option<IpAddr> {
    if let Ok(ip) = value.parse::<IpAddr>() {
        return Some(ip);
    }
    let parts: Vec<&str> = value.split('.').collect();
    if parts.len() != 4 {
        return None;
    }
    let mut octets = [0u8; 4];
    for (i, p) in parts.iter().enumerate() {
        if p.is_empty() || p.len() > 3 || !p.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        octets[i] = p.parse::<u16>().ok().filter(|n| *n <= 255)? as u8;
    }
    Some(IpAddr::V4(Ipv4Addr::from(octets)))
}

/// `netutils.ParseCIDRSloppy`: `ip/prefix` with a sloppy IP and a decimal
/// prefix no longer than the address.
pub(crate) fn parse_cidr_sloppy(value: &str) -> Option<(IpAddr, u8)> {
    let (ip, prefix) = value.split_once('/')?;
    let ip = parse_ip_sloppy(ip)?;
    if prefix.is_empty() || !prefix.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let prefix: u32 = prefix.parse().ok()?;
    let bits = if ip.is_ipv4() { 32 } else { 128 };
    (prefix <= bits).then_some((ip, prefix as u8))
}

fn is_ipv6_string(value: &str) -> bool {
    matches!(parse_ip_sloppy(value), Some(IpAddr::V6(_)))
}

/// `IsValidIPForLegacyField` (validation.go:9556-9558, apimachinery
/// util/validation/ip.go:81-87) with `StrictIPCIDRValidation` off: a
/// value already valid in the old object passes as-is.
pub fn is_valid_ip_for_legacy_field(fld: &Path, value: &str, valid_old: &[String]) -> ErrorList {
    if valid_old.iter().any(|v| v == value) || parse_ip_sloppy(value).is_some() {
        return Vec::new();
    }
    vec![Error::invalid(
        fld,
        value,
        "must be a valid IP address, (e.g. 10.9.8.7 or 2001:db8::ffff)",
    )
    .with_origin("format=ip-sloppy")]
}

/// `IsValidCIDRForLegacyField` (validation.go:9563-9565, ip.go:184-191).
pub fn is_valid_cidr_for_legacy_field(fld: &Path, value: &str, valid_old: &[String]) -> ErrorList {
    if valid_old.iter().any(|v| v == value) || parse_cidr_sloppy(value).is_some() {
        return Vec::new();
    }
    vec![Error::invalid(
        fld,
        value,
        "must be a valid CIDR value, (e.g. 10.9.8.0/24 or 2001:db8::/64)",
    )]
}

/// `ValidateEndpointIP` (validation.go:8314-8334).
fn validate_endpoint_ip(ip_address: &str, fld: &Path) -> ErrorList {
    let mut errs = Vec::new();
    let Some(ip) = parse_ip_sloppy(ip_address) else {
        errs.push(
            Error::invalid(fld, ip_address, "must be a valid IP address")
                .with_origin("format=ip-sloppy"),
        );
        return errs;
    };
    if ip.is_unspecified() {
        errs.push(Error::invalid(
            fld,
            ip_address,
            format!("may not be unspecified ({ip_address})"),
        ));
    }
    if ip.is_loopback() {
        errs.push(Error::invalid(
            fld,
            ip_address,
            "may not be in the loopback range (127.0.0.0/8, ::1/128)",
        ));
    }
    let (link_local_unicast, link_local_multicast) = match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            (v4.is_link_local(), o[0] == 224 && o[1] == 0 && o[2] == 0)
        }
        IpAddr::V6(v6) => {
            let s = v6.segments();
            (s[0] & 0xffc0 == 0xfe80, s[0] & 0xff0f == 0xff02)
        }
    };
    if link_local_unicast {
        errs.push(Error::invalid(
            fld,
            ip_address,
            "may not be in the link-local range (169.254.0.0/16, fe80::/10)",
        ));
    }
    if link_local_multicast {
        errs.push(Error::invalid(
            fld,
            ip_address,
            "may not be in the link-local multicast range (224.0.0.0/24, ff02::/10)",
        ));
    }
    errs
}

/// `validation.IsValidPortNum`.
fn is_valid_port_num(port: i64) -> Option<&'static str> {
    (!(1..=65535).contains(&port)).then_some("must be between 1 and 65535, inclusive")
}

/// `validation.IsValidPortName` (apimachinery util/validation/validation.go).
fn is_valid_port_name(port: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if port.len() > 15 {
        errs.push("must be no more than 15 characters".to_string());
    }
    if !port
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        errs.push(
            "must contain only alpha-numeric characters (a-z, 0-9), and hyphens (-)".to_string(),
        );
    }
    if !port.bytes().any(|b| b.is_ascii_lowercase()) {
        errs.push("must contain at least one letter (a-z)".to_string());
    }
    if port.contains("--") {
        errs.push("must not contain consecutive hyphens".to_string());
    }
    if !port.is_empty() && (port.starts_with('-') || port.ends_with('-')) {
        errs.push("must not begin or end with a hyphen".to_string());
    }
    errs
}

/// `ValidatePortNumOrName` (validation.go).
fn validate_port_num_or_name(port: &IntOrString, fld: &Path) -> ErrorList {
    let mut errs = Vec::new();
    match port {
        IntOrString::Int(n) => {
            if let Some(msg) = is_valid_port_num(i64::from(*n)) {
                errs.push(Error::invalid(fld, *n, msg));
            }
        }
        IntOrString::String(s) => {
            if s.is_empty() {
                errs.push(Error::required(fld, ""));
            } else {
                for msg in is_valid_port_name(s) {
                    errs.push(Error::invalid(fld, s.clone(), msg));
                }
            }
        }
    }
    errs
}

/// `ValidateDNS1123Label`.
fn validate_dns1123_label(value: &str, fld: &Path) -> ErrorList {
    is_dns1123_label(value)
        .into_iter()
        .map(|msg| Error::invalid(fld, value, msg))
        .collect()
}

/// `ValidateQualifiedName`.
fn validate_qualified_name(value: &str, fld: &Path) -> ErrorList {
    is_qualified_name(value)
        .into_iter()
        .map(|msg| Error::invalid(fld, value, msg))
        .collect()
}

/// `ValidateServiceName` = `NameIsDNS1035Label` (validation.go:290).
fn name_is_dns1035_label(name: &str, prefix: bool) -> Vec<String> {
    let name = if prefix && name.len() > 1 && name.ends_with('-') {
        // `maskTrailingDash`.
        format!("{}a", &name[..name.len() - 2])
    } else {
        name.to_string()
    };
    is_dns1035_label(&name)
}

fn opt_str(v: Option<&str>) -> BadValue {
    match v {
        Some(s) => BadValue::from(s),
        None => BadValue::Json(serde_json::Value::Null),
    }
}

fn policy_str(p: &IPFamilyPolicy) -> &'static str {
    match p {
        IPFamilyPolicy::SingleStack => "SingleStack",
        IPFamilyPolicy::PreferDualStack => "PreferDualStack",
        IPFamilyPolicy::RequireDualStack => "RequireDualStack",
    }
}

fn family_str(f: &IPFamily) -> &'static str {
    match f {
        IPFamily::IPv4 => "IPv4",
        IPFamily::IPv6 => "IPv6",
    }
}

fn families_value(svc: &Service) -> BadValue {
    BadValue::Json(serde_json::Value::Array(
        ip_families(svc)
            .iter()
            .map(|f| serde_json::Value::String(family_str(f).to_string()))
            .collect(),
    ))
}

fn policy_value(svc: &Service) -> BadValue {
    opt_str(svc.spec.ip_family_policy.as_ref().map(policy_str))
}

fn cluster_ips(svc: &Service) -> &[String] {
    svc.spec.cluster_ips.as_deref().unwrap_or(&[])
}

fn ip_families(svc: &Service) -> &[IPFamily] {
    svc.spec.ip_families.as_deref().unwrap_or(&[])
}

fn cluster_ip(svc: &Service) -> &str {
    svc.spec.cluster_ip.as_deref().unwrap_or("")
}

fn service_type(svc: &Service) -> Option<&ServiceType> {
    svc.spec.service_type.as_ref()
}

fn is_type(svc: &Service, t: ServiceType) -> bool {
    service_type(svc) == Some(&t)
}

/// `isHeadlessService` (validation.go:9212-9216).
fn is_headless_service(svc: &Service) -> bool {
    cluster_ips(svc).len() == 1 && cluster_ips(svc)[0] == "None"
}

/// `apiservice.ExternallyAccessible` (pkg/api/service/util.go:71-75).
pub fn externally_accessible(svc: &Service) -> bool {
    is_type(svc, ServiceType::LoadBalancer)
        || is_type(svc, ServiceType::NodePort)
        || (is_type(svc, ServiceType::ClusterIP)
            && svc
                .spec
                .external_ips
                .as_ref()
                .is_some_and(|v| !v.is_empty()))
}

/// `apiservice.NeedsHealthCheck` (util.go:78-93).
pub fn needs_health_check(svc: &Service) -> bool {
    is_type(svc, ServiceType::LoadBalancer)
        && svc.spec.external_traffic_policy == Some(ServiceExternalTrafficPolicy::Local)
}

fn health_check_node_port(svc: &Service) -> i32 {
    svc.spec.health_check_node_port.unwrap_or(0)
}

fn node_port(p: &ServicePort) -> i32 {
    p.node_port.map(i32::from).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// validateService
// ---------------------------------------------------------------------------

/// `validateClientIPAffinityConfig` (validation.go:3395-3412) and
/// `validateAffinityTimeout` (:3414-3420).
fn validate_client_ip_affinity_config(
    config: Option<&SessionAffinityConfig>,
    fld: &Path,
) -> ErrorList {
    let detail = "when session affinity type is ClientIP";
    let Some(config) = config else {
        return vec![Error::required(fld, detail)];
    };
    let Some(client_ip) = &config.client_ip else {
        return vec![Error::required(&fld.child("clientIP"), detail)];
    };
    let timeout_path = fld.child("clientIP").child("timeoutSeconds");
    let Some(timeout) = client_ip.timeout_seconds else {
        return vec![Error::required(&timeout_path, detail)];
    };
    if timeout <= 0 || timeout > MAX_CLIENT_IP_SERVICE_AFFINITY_SECONDS {
        return vec![Error::invalid(
            &timeout_path,
            timeout,
            format!(
                "must be greater than 0 and less than {MAX_CLIENT_IP_SERVICE_AFFINITY_SECONDS}"
            ),
        )];
    }
    Vec::new()
}

/// `validateServicePort` (validation.go:6780-6822).
fn validate_service_port(
    sp: &ServicePort,
    require_name: bool,
    all_names: &mut HashSet<String>,
    fld: &Path,
) -> ErrorList {
    let mut errs = Vec::new();
    let name = sp.name.as_deref().unwrap_or("");
    if require_name && name.is_empty() {
        errs.push(Error::required(&fld.child("name"), ""));
    } else if !name.is_empty() {
        errs.extend(validate_dns1123_label(name, &fld.child("name")));
        if !all_names.insert(name.to_string()) {
            errs.push(Error::duplicate(&fld.child("name"), name));
        }
    }

    if let Some(msg) = is_valid_port_num(i64::from(sp.port)) {
        errs.push(Error::invalid(&fld.child("port"), i32::from(sp.port), msg));
    }

    if sp.protocol.is_empty() {
        errs.push(Error::required(&fld.child("protocol"), ""));
    } else if !SUPPORTED_PORT_PROTOCOLS.contains(&sp.protocol.as_str()) {
        errs.push(Error::not_supported(
            &fld.child("protocol"),
            sp.protocol.clone(),
            SUPPORTED_PORT_PROTOCOLS,
        ));
    }

    // Defaulting sets targetPort from port, so an unset one is the zero
    // IntOrString.
    let target = sp.target_port.clone().unwrap_or(IntOrString::Int(0));
    errs.extend(validate_port_num_or_name(&target, &fld.child("targetPort")));

    if let Some(app_protocol) = &sp.app_protocol {
        errs.extend(validate_qualified_name(
            app_protocol,
            &fld.child("appProtocol"),
        ));
    }
    errs
}

/// `validateServiceExternalTrafficPolicy` (validation.go:6824-6859).
fn validate_service_external_traffic_policy(svc: &Service) -> ErrorList {
    let mut errs = Vec::new();
    let fld = Path::new("spec");
    let etp = svc.spec.external_traffic_policy.as_ref().map(|p| match p {
        ServiceExternalTrafficPolicy::Cluster => "Cluster",
        ServiceExternalTrafficPolicy::Local => "Local",
    });
    if !externally_accessible(svc) {
        if let Some(etp) = etp {
            errs.push(Error::invalid(
                &fld.child("externalTrafficPolicy"),
                etp,
                "may only be set for externally-accessible services",
            ));
        }
    } else if etp.is_none() {
        errs.push(Error::required(&fld.child("externalTrafficPolicy"), ""));
    }

    let hcnp = health_check_node_port(svc);
    if !needs_health_check(svc) {
        if hcnp != 0 {
            errs.push(Error::invalid(
                &fld.child("healthCheckNodePort"),
                hcnp,
                "may only be set when `type` is 'LoadBalancer' and `externalTrafficPolicy` is 'Local'",
            ));
        }
    } else if hcnp == 0 {
        errs.push(Error::required(&fld.child("healthCheckNodePort"), ""));
    } else if let Some(msg) = is_valid_port_num(i64::from(hcnp)) {
        errs.push(Error::invalid(&fld.child("healthCheckNodePort"), hcnp, msg));
    }
    errs
}

/// `validateServiceExternalTrafficFieldsUpdate` (validation.go:6861-6871).
fn validate_service_external_traffic_fields_update(before: &Service, after: &Service) -> ErrorList {
    if needs_health_check(before)
        && needs_health_check(after)
        && health_check_node_port(after) != health_check_node_port(before)
    {
        return vec![Error::forbidden(
            &Path::new("spec").child("healthCheckNodePort"),
            "field is immutable",
        )];
    }
    Vec::new()
}

/// `validateServiceInternalTrafficFieldsValue` (validation.go:6875-6892).
/// The enum admits only the supported values.
fn validate_service_internal_traffic_fields_value(svc: &Service) -> ErrorList {
    let itp: Option<&ServiceInternalTrafficPolicy> = svc.spec.internal_traffic_policy.as_ref();
    if itp.is_none()
        && matches!(
            service_type(svc),
            Some(ServiceType::NodePort | ServiceType::LoadBalancer | ServiceType::ClusterIP)
        )
    {
        return vec![Error::required(
            &Path::new("spec").child("internalTrafficPolicy"),
            "",
        )];
    }
    Vec::new()
}

/// `validateServiceTrafficDistribution` (validation.go:6896-6921).
fn validate_service_traffic_distribution(svc: &Service) -> ErrorList {
    match &svc.spec.traffic_distribution {
        Some(td) if !SUPPORTED_TRAFFIC_DISTRIBUTION.contains(&td.as_str()) => {
            vec![Error::not_supported(
                &Path::new("spec").child("trafficDistribution"),
                td.clone(),
                SUPPORTED_TRAFFIC_DISTRIBUTION,
            )]
        }
        _ => Vec::new(),
    }
}

/// `sameLoadBalancerClass` (validation.go).
fn same_load_balancer_class(old: &Service, new: &Service) -> bool {
    old.spec.load_balancer_class == new.spec.load_balancer_class
}

/// `validateLoadBalancerClassField` (validation.go:9219-9240).
fn validate_load_balancer_class_field(old: Option<&Service>, svc: &Service) -> ErrorList {
    let mut errs = Vec::new();
    let fld = Path::new("spec").child("loadBalancerClass");
    if let Some(old) = old {
        if is_type(old, ServiceType::LoadBalancer)
            && is_type(svc, ServiceType::LoadBalancer)
            && !same_load_balancer_class(old, svc)
        {
            errs.push(Error::invalid(
                &fld,
                opt_str(svc.spec.load_balancer_class.as_deref()),
                "may not change once set",
            ));
        }
    }
    if is_type(svc, ServiceType::LoadBalancer) {
        if let Some(class) = &svc.spec.load_balancer_class {
            errs.extend(validate_qualified_name(class, &fld));
        }
    } else if svc.spec.load_balancer_class.is_some() {
        errs.push(Error::forbidden(
            &fld,
            "may only be used when `type` is 'LoadBalancer'",
        ));
    }
    errs
}

/// `validateService` (validation.go:6570-6778).
fn validate_service(svc: &Service, old: Option<&Service>) -> ErrorList {
    let mut errs = Vec::new();
    let meta_path = Path::new("metadata");
    let empty = HashMap::new();
    let annotations = svc.metadata.annotations.as_ref().unwrap_or(&empty);

    if let (Some(mode), Some(hints)) = (
        annotations.get(ANNOTATION_TOPOLOGY_MODE),
        annotations.get(DEPRECATED_ANNOTATION_TOPOLOGY_AWARE_HINTS),
    ) {
        if mode != hints {
            errs.push(Error::invalid(
                &meta_path.child("annotations").key(ANNOTATION_TOPOLOGY_MODE),
                mode.clone(),
                format!(
                    "must match annotations[{DEPRECATED_ANNOTATION_TOPOLOGY_AWARE_HINTS}] when both are specified"
                ),
            ));
        }
    }

    let spec_path = Path::new("spec");
    let headless = is_headless_service(svc);

    if svc.spec.ports.is_empty() && !headless && !is_type(svc, ServiceType::ExternalName) {
        errs.push(Error::required(&spec_path.child("ports"), ""));
    }
    match service_type(svc) {
        Some(ServiceType::LoadBalancer) if headless => errs.push(Error::invalid(
            &spec_path.child("clusterIPs").index(0),
            cluster_ips(svc)[0].clone(),
            "may not be set to 'None' for LoadBalancer services",
        )),
        Some(ServiceType::NodePort) if headless => errs.push(Error::invalid(
            &spec_path.child("clusterIPs").index(0),
            cluster_ips(svc)[0].clone(),
            "may not be set to 'None' for NodePort services",
        )),
        Some(ServiceType::ExternalName) => {
            if !cluster_ips(svc).is_empty() {
                errs.push(Error::forbidden(
                    &spec_path.child("clusterIPs"),
                    "may not be set for ExternalName services",
                ));
            }
            if !ip_families(svc).is_empty() {
                errs.push(Error::forbidden(
                    &spec_path.child("ipFamilies"),
                    "may not be set for ExternalName services",
                ));
            }
            if svc.spec.ip_family_policy.is_some() {
                errs.push(Error::forbidden(
                    &spec_path.child("ipFamilyPolicy"),
                    "may not be set for ExternalName services",
                ));
            }
            // The CNAME may have a trailing dot to mark it fully qualified.
            let external_name = svc.spec.external_name.as_deref().unwrap_or("");
            let cname = external_name.strip_suffix('.').unwrap_or(external_name);
            if !cname.is_empty() {
                for msg in is_dns1123_subdomain(cname) {
                    errs.push(Error::invalid(&spec_path.child("externalName"), cname, msg));
                }
            } else {
                errs.push(Error::required(&spec_path.child("externalName"), ""));
            }
        }
        _ => {}
    }

    let mut all_port_names = HashSet::new();
    let ports_path = spec_path.child("ports");
    let require_name = svc.spec.ports.len() > 1;
    for (i, port) in svc.spec.ports.iter().enumerate() {
        errs.extend(validate_service_port(
            port,
            require_name,
            &mut all_port_names,
            &ports_path.index(i),
        ));
    }

    if let Some(selector) = &svc.spec.selector {
        errs.extend(validate_labels(selector, &spec_path.child("selector")));
    }

    match svc.spec.session_affinity.as_deref() {
        None | Some("") => errs.push(Error::required(&spec_path.child("sessionAffinity"), "")),
        Some(sa) if !SUPPORTED_SESSION_AFFINITY_TYPE.contains(&sa) => {
            errs.push(Error::not_supported(
                &spec_path.child("sessionAffinity"),
                sa,
                SUPPORTED_SESSION_AFFINITY_TYPE,
            ))
        }
        _ => {}
    }
    match svc.spec.session_affinity.as_deref() {
        Some("ClientIP") => errs.extend(validate_client_ip_affinity_config(
            svc.spec.session_affinity_config.as_ref(),
            &spec_path.child("sessionAffinityConfig"),
        )),
        Some("None") if svc.spec.session_affinity_config.is_some() => errs.push(Error::forbidden(
            &spec_path.child("sessionAffinityConfig"),
            "must not be set when session affinity is None",
        )),
        _ => {}
    }

    errs.extend(validate_service_cluster_ips_related_fields(svc, old));

    // New external IPs must be valid and non-special; old ones stay.
    let ip_path = spec_path.child("externalIPs");
    let existing_external_ips: &[String] = old
        .and_then(|o| o.spec.external_ips.as_deref())
        .unwrap_or(&[]);
    for (i, ip) in svc.spec.external_ips.iter().flatten().enumerate() {
        let idx_path = ip_path.index(i);
        let ip_errs = is_valid_ip_for_legacy_field(&idx_path, ip, existing_external_ips);
        if !ip_errs.is_empty() {
            errs.extend(ip_errs);
        } else {
            errs.extend(validate_endpoint_ip(ip, &idx_path));
        }
    }

    if svc.spec.service_type.is_none() {
        errs.push(Error::required(&spec_path.child("type"), ""));
    }

    if is_type(svc, ServiceType::ClusterIP) {
        for (i, port) in svc.spec.ports.iter().enumerate() {
            if node_port(port) != 0 {
                errs.push(Error::forbidden(
                    &ports_path.index(i).child("nodePort"),
                    "may not be used when `type` is 'ClusterIP'",
                ));
            }
        }
    }

    // Duplicate node ports and ports, by (protocol, port).
    let mut node_ports: HashSet<(String, i32)> = HashSet::new();
    for (i, port) in svc.spec.ports.iter().enumerate() {
        if node_port(port) == 0 {
            continue;
        }
        if !node_ports.insert((port.protocol.clone(), node_port(port))) {
            errs.push(Error::duplicate(
                &ports_path.index(i).child("nodePort"),
                node_port(port),
            ));
        }
    }
    let mut ports: HashSet<(String, u16)> = HashSet::new();
    for (i, port) in svc.spec.ports.iter().enumerate() {
        if !ports.insert((port.protocol.clone(), port.port)) {
            // `field.Duplicate(portPath, key)` renders the internal
            // `core.ServicePort` key as JSON, whose fields carry no json tags.
            // serde_json sorts the keys, where Go keeps field order.
            errs.push(Error::duplicate(
                &ports_path.index(i),
                serde_json::json!({
                    "Name": "", "Protocol": port.protocol, "AppProtocol": null,
                    "Port": port.port, "TargetPort": 0, "NodePort": 0,
                }),
            ));
        }
    }

    // Source ranges, from the field or the legacy annotation.
    let source_ranges = svc
        .spec
        .load_balancer_source_ranges
        .as_deref()
        .unwrap_or(&[]);
    if !source_ranges.is_empty() {
        let fld = spec_path.child("LoadBalancerSourceRanges");
        if !is_type(svc, ServiceType::LoadBalancer) {
            errs.push(Error::forbidden(
                &fld,
                "may only be used when `type` is 'LoadBalancer'",
            ));
        }
        let existing: Vec<String> = old
            .and_then(|o| o.spec.load_balancer_source_ranges.as_ref())
            .map(|r| r.iter().map(|v| v.trim().to_string()).collect())
            .unwrap_or_default();
        for (idx, value) in source_ranges.iter().enumerate() {
            errs.extend(is_valid_cidr_for_legacy_field(
                &fld.index(idx),
                value.trim(),
                &existing,
            ));
        }
    } else if let Some(val) = annotations.get(ANNOTATION_LOAD_BALANCER_SOURCE_RANGES) {
        let fld = Path::new("metadata")
            .child("annotations")
            .key(ANNOTATION_LOAD_BALANCER_SOURCE_RANGES);
        if !is_type(svc, ServiceType::LoadBalancer) {
            errs.push(Error::forbidden(
                &fld,
                "may only be used when `type` is 'LoadBalancer'",
            ));
        }
        let old_val = old.and_then(|o| {
            o.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(ANNOTATION_LOAD_BALANCER_SOURCE_RANGES))
        });
        if old_val != Some(val) {
            let val = val.trim();
            if !val.is_empty() {
                for value in val.split(',') {
                    errs.extend(is_valid_cidr_for_legacy_field(&fld, value.trim(), &[]));
                }
            }
        }
    }

    if svc.spec.allocate_load_balancer_node_ports.is_some()
        && !is_type(svc, ServiceType::LoadBalancer)
    {
        errs.push(Error::forbidden(
            &spec_path.child("allocateLoadBalancerNodePorts"),
            "may only be used when `type` is 'LoadBalancer'",
        ));
    }
    if is_type(svc, ServiceType::LoadBalancer)
        && svc.spec.allocate_load_balancer_node_ports.is_none()
    {
        // Upstream's path really is the root `allocateLoadBalancerNodePorts`.
        errs.push(Error::required(
            &Path::new("allocateLoadBalancerNodePorts"),
            "",
        ));
    }

    errs.extend(validate_load_balancer_class_field(None, svc));
    errs.extend(validate_service_external_traffic_policy(svc));
    errs.extend(validate_service_internal_traffic_fields_value(svc));
    errs.extend(validate_service_traffic_distribution(svc));
    errs
}

// ---------------------------------------------------------------------------
// ClusterIPs, IP families and their upgrade/downgrade rules
// ---------------------------------------------------------------------------

/// `ValidateServiceClusterIPsRelatedFields` (validation.go:8964-9092).
pub fn validate_service_cluster_ips_related_fields(
    svc: &Service,
    old: Option<&Service>,
) -> ErrorList {
    // Validated (all unset) for ExternalName in `validateService`.
    if is_type(svc, ServiceType::ExternalName) {
        return Vec::new();
    }
    let mut errs = Vec::new();
    let mut has_invalid_ips = false;
    let spec_path = Path::new("spec");
    let cluster_ips_field = spec_path.child("clusterIPs");
    let ip_families_field = spec_path.child("ipFamilies");
    let ips = cluster_ips(svc);

    if !cluster_ip(svc).is_empty() {
        if ips.is_empty() {
            errs.push(Error::required(&cluster_ips_field, ""));
        } else if ips[0] != cluster_ip(svc) {
            errs.push(Error::invalid(
                &cluster_ips_field,
                ips.to_vec(),
                "first value must match `clusterIP`",
            ));
        }
    } else if !ips.is_empty() {
        errs.push(Error::invalid(
            &cluster_ips_field,
            ips.to_vec(),
            "must be empty when `clusterIP` is not specified",
        ));
    }

    // Families are a closed enum; only duplicates remain to check.
    let mut seen = HashSet::new();
    for (i, family) in ip_families(svc).iter().enumerate() {
        if !seen.insert(family_str(family)) {
            errs.push(Error::duplicate(
                &ip_families_field.index(i),
                family_str(family),
            ));
        }
    }

    let existing: &[String] = old.map(cluster_ips).unwrap_or(&[]);
    for (i, ip) in ips.iter().enumerate() {
        if i == 0 && ip == "None" {
            if ips.len() > 1 {
                has_invalid_ips = true;
                errs.push(Error::invalid(
                    &cluster_ips_field,
                    ips.to_vec(),
                    "'None' must be the first and only value",
                ));
            }
            continue;
        }
        let ip_errs = is_valid_ip_for_legacy_field(&cluster_ips_field.index(i), ip, existing);
        has_invalid_ips = has_invalid_ips || !ip_errs.is_empty();
        errs.extend(ip_errs);
    }

    if ips.len() > 2 {
        errs.push(Error::invalid(
            &cluster_ips_field,
            ips.to_vec(),
            "may only hold up to 2 values",
        ));
    }

    // Further checks would only restate a bad IP.
    if has_invalid_ips {
        return errs;
    }

    // `netutils.IsDualStackIPStrings` (k8s.io/utils/net/ipfamily.go:58-68):
    // at least one IP of each family.
    if ips.len() > 1 {
        let v6 = ips.iter().filter(|ip| is_ipv6_string(ip)).count();
        if v6 == 0 || v6 == ips.len() {
            errs.push(Error::invalid(
                &cluster_ips_field,
                ips.to_vec(),
                "may specify no more than one IP for each IP family",
            ));
        }
    }

    if !is_headless_service(svc) && !ips.is_empty() && !ip_families(svc).is_empty() {
        for (i, ip) in ips.iter().enumerate() {
            let Some(family) = ip_families(svc).get(i) else {
                break;
            };
            if *family == IPFamily::IPv4 && is_ipv6_string(ip) {
                errs.push(Error::invalid(
                    &cluster_ips_field.index(i),
                    ip.clone(),
                    format!("expected an IPv4 value as indicated by `ipFamilies[{i}]`"),
                ));
            }
            if *family == IPFamily::IPv6 && !is_ipv6_string(ip) {
                errs.push(Error::invalid(
                    &cluster_ips_field.index(i),
                    ip.clone(),
                    format!("expected an IPv6 value as indicated by `ipFamilies[{i}]`"),
                ));
            }
        }
    }
    errs
}

fn is_single_stack(svc: &Service) -> bool {
    svc.spec.ip_family_policy == Some(IPFamilyPolicy::SingleStack)
}

/// `validateUpgradeDowngradeClusterIPs` (validation.go:9095-9148).
fn validate_upgrade_downgrade_cluster_ips(old: &Service, svc: &Service) -> ErrorList {
    let mut errs = Vec::new();
    if is_type(svc, ServiceType::ExternalName) || is_type(old, ServiceType::ExternalName) {
        return errs;
    }
    if is_headless_service(old) && is_headless_service(svc) {
        return errs;
    }
    let (old_ips, new_ips) = (cluster_ips(old), cluster_ips(svc));
    let fld = Path::new("spec").child("clusterIPs");
    match old_ips.len().cmp(&new_ips.len()) {
        std::cmp::Ordering::Equal => {
            for (i, ip) in old_ips.iter().enumerate() {
                if *ip != new_ips[i] {
                    errs.push(Error::invalid(
                        &fld.index(i),
                        new_ips.to_vec(),
                        "may not change once set",
                    ));
                }
            }
        }
        std::cmp::Ordering::Greater => {
            if new_ips.is_empty() {
                errs.push(Error::invalid(
                    &fld.index(0),
                    new_ips.to_vec(),
                    "primary clusterIP can not be unset",
                ));
            }
            if !old_ips.is_empty() && !new_ips.is_empty() && new_ips[0] != old_ips[0] {
                errs.push(Error::invalid(
                    &fld.index(0),
                    new_ips.to_vec(),
                    "may not change once set",
                ));
            }
            if new_ips.len() == 1 && !is_single_stack(svc) {
                errs.push(Error::invalid(
                    &Path::new("spec").child("ipFamilyPolicy"),
                    policy_value(svc),
                    "must be set to 'SingleStack' when releasing the secondary clusterIP",
                ));
            }
        }
        std::cmp::Ordering::Less => {
            if !old_ips.is_empty() && new_ips[0] != old_ips[0] {
                errs.push(Error::invalid(
                    &fld.index(0),
                    new_ips.to_vec(),
                    "may not change once set",
                ));
            }
        }
    }
    errs
}

/// `validateUpgradeDowngradeIPFamilies` (validation.go:9151-9210).
fn validate_upgrade_downgrade_ip_families(old: &Service, svc: &Service) -> ErrorList {
    let mut errs = Vec::new();
    if is_type(svc, ServiceType::ExternalName) || is_type(old, ServiceType::ExternalName) {
        return errs;
    }
    let (old_headless, new_headless) = (is_headless_service(old), is_headless_service(svc));
    if old_headless != new_headless || new_headless {
        return errs;
    }
    let (old_f, new_f) = (ip_families(old), ip_families(svc));
    let fld = Path::new("spec").child("ipFamilies").index(0);
    match old_f.len().cmp(&new_f.len()) {
        std::cmp::Ordering::Equal => {
            for (i, f) in old_f.iter().enumerate() {
                if *f != new_f[i] {
                    errs.push(Error::invalid(
                        &fld,
                        families_value(svc),
                        "may not change once set",
                    ));
                }
            }
        }
        std::cmp::Ordering::Greater => {
            if cluster_ips(svc).is_empty() {
                errs.push(Error::invalid(
                    &fld,
                    families_value(svc),
                    "primary ipFamily can not be unset",
                ));
            }
            if !new_f.is_empty() && new_f[0] != old_f[0] {
                errs.push(Error::invalid(
                    &fld,
                    cluster_ips(svc).to_vec(),
                    "may not change once set",
                ));
            }
            if new_f.len() == 1 && !is_single_stack(svc) {
                errs.push(Error::invalid(
                    &Path::new("spec").child("ipFamilyPolicy"),
                    policy_value(svc),
                    "must be set to 'SingleStack' when releasing the secondary ipFamily",
                ));
            }
        }
        std::cmp::Ordering::Less => {
            if !old_f.is_empty() && !new_f.is_empty() && new_f[0] != old_f[0] {
                errs.push(Error::invalid(
                    &fld,
                    cluster_ips(svc).to_vec(),
                    "may not change once set",
                ));
            }
        }
    }
    errs
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// `ValidateLoadBalancerStatus` (validation.go:8653-8696).
pub fn validate_load_balancer_status(
    status: Option<&LoadBalancerStatus>,
    old_status: Option<&LoadBalancerStatus>,
    fld: &Path,
    svc: &Service,
) -> ErrorList {
    let mut errs = Vec::new();
    let ingress = status.map(|s| s.ingress.as_slice()).unwrap_or(&[]);
    let ingr_path = fld.child("ingress");
    if !is_type(svc, ServiceType::LoadBalancer) && !ingress.is_empty() {
        errs.push(Error::forbidden(
            &ingr_path,
            "may only be used when `spec.type` is 'LoadBalancer'",
        ));
        return errs;
    }
    let existing: Vec<String> = old_status
        .map(|s| {
            s.ingress
                .iter()
                .filter_map(|i| i.ip.clone().filter(|ip| !ip.is_empty()))
                .collect()
        })
        .unwrap_or_default();
    for (i, ing) in ingress.iter().enumerate() {
        let idx_path = ingr_path.index(i);
        let ip = ing.ip.as_deref().unwrap_or("");
        if !ip.is_empty() {
            errs.extend(is_valid_ip_for_legacy_field(
                &idx_path.child("ip"),
                ip,
                &existing,
            ));
        }
        match ing.ip_mode.as_deref() {
            None => {
                if !ip.is_empty() {
                    errs.push(Error::required(
                        &idx_path.child("ipMode"),
                        "must be specified when `ip` is set",
                    ));
                }
            }
            Some(_) if ip.is_empty() => errs.push(Error::forbidden(
                &idx_path.child("ipMode"),
                "may not be specified when `ip` is not set",
            )),
            Some(mode) if !SUPPORTED_LOAD_BALANCER_IP_MODE.contains(&mode) => {
                errs.push(Error::not_supported(
                    &idx_path.child("ipMode"),
                    mode,
                    SUPPORTED_LOAD_BALANCER_IP_MODE,
                ))
            }
            _ => {}
        }
        let hostname = ing.hostname.as_deref().unwrap_or("");
        if !hostname.is_empty() {
            for msg in is_dns1123_subdomain(hostname) {
                errs.push(Error::invalid(&idx_path.child("hostname"), hostname, msg));
            }
            if parse_ip_sloppy(hostname).is_some() {
                errs.push(Error::invalid(
                    &idx_path.child("hostname"),
                    hostname,
                    "must be a DNS name, not an IP address",
                ));
            }
        }
    }
    errs
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

/// `ValidateServiceCreate` (validation.go:6924-6936).
pub fn validate_service_create(svc: &Service) -> ErrorList {
    let mut errs = validate_object_meta(
        &svc.metadata,
        true,
        name_is_dns1035_label,
        &Path::new("metadata"),
    );
    errs.extend(validate_service(svc, None));
    errs
}

/// `ValidateServiceUpdate` (validation.go:6939-6959).
pub fn validate_service_update(svc: &Service, old: &Service) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&svc.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_upgrade_downgrade_cluster_ips(old, svc));
    errs.extend(validate_upgrade_downgrade_ip_families(old, svc));
    errs.extend(validate_load_balancer_class_field(Some(old), svc));
    errs.extend(validate_service_external_traffic_fields_update(old, svc));
    errs.extend(validate_service(svc, Some(old)));
    errs
}

/// `ValidateServiceStatusUpdate` (validation.go:6962-6966).
pub fn validate_service_status_update(svc: &Service, old: &Service) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&svc.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_load_balancer_status(
        svc.status.as_ref().and_then(|s| s.load_balancer.as_ref()),
        old.status.as_ref().and_then(|s| s.load_balancer.as_ref()),
        &Path::new("status").child("loadBalancer"),
        svc,
    ));
    errs
}
