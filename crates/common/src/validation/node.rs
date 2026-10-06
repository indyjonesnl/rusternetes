//! Node validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go` (release-1.35): `ValidateNode`
//! (:7178-7216) and `ValidateNodeUpdate` (:7237-7303), with the helpers they
//! call.
//!
//! `spec.configSource` and `spec.externalID` are not modelled on
//! [`crate::resources::node::NodeSpec`] — the first is a removed feature, the
//! second deprecated and unused — so decoding drops them and the rules that
//! guard them (`validateNodeConfigSourceSpec`, externalID immutability) have
//! nothing to check.

use crate::resources::node::{
    ConfigMapNodeConfigSource, Node, NodeConfigSource, NodeConfigStatus, NodeSwapStatus, Taint,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_label, is_dns1123_subdomain, is_valid_label_value, validate_label_name,
};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use crate::validation::resourcequota::validate_resource_quantity_value;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

const TAINT_EFFECTS: [&str; 3] = ["NoSchedule", "PreferNoSchedule", "NoExecute"];

/// `core.TaintsAnnotationKey` (pkg/apis/core/annotation_key_constants.go:35).
pub const TAINTS_ANNOTATION_KEY: &str = "scheduler.alpha.kubernetes.io/taints";

/// Upstream `validation.DNS1123SubdomainMaxLength`.
const DNS1123_SUBDOMAIN_MAX_LENGTH: usize = 253;

/// Upstream `isNotPositiveErrorMsg`.
const IS_NOT_POSITIVE_ERROR_MSG: &str = "must be greater than zero";

/// Upstream `nodeDeclaredFeatureRegexp`: UpperCamelCase segments separated by '/'.
static NODE_DECLARED_FEATURE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[A-Z][a-zA-Z0-9]*(/[a-zA-Z][a-zA-Z0-9]*)*$").expect("feature regex")
});

#[derive(PartialEq, Eq, Clone, Copy)]
enum IpFamily {
    V4,
    V6,
}

/// `netutils.ParseCIDRSloppy` (k8s.io/utils/net), which both
/// `IsValidCIDRForLegacyField` (validation.go:7200) and
/// `IsDualStackCIDRStrings` -> `ParseCIDRs` (validation.go:7205) use, so a
/// leading-zero IPv4 CIDR parses; `nodeWarnings` warns about it instead.
fn parse_cidr(cidr: &str) -> Option<IpFamily> {
    match crate::validation::service::parse_cidr_sloppy(cidr)?.0 {
        IpAddr::V4(_) => Some(IpFamily::V4),
        IpAddr::V6(_) => Some(IpFamily::V6),
    }
}

/// Port of `validateNodeTaints` (validation.go:7100-7131): each key a label
/// name, each value a label value, each effect known, and taints unique by
/// (key, effect).
fn validate_node_taints(taints: &[Taint], fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut seen: HashSet<(&str, &str)> = HashSet::new();
    for (i, taint) in taints.iter().enumerate() {
        let tp = fld_path.index(i);
        errs.extend(validate_label_name(&taint.key, &tp.child("key")));
        let value = taint.value.as_deref().unwrap_or("");
        let msgs = is_valid_label_value(value);
        if !msgs.is_empty() {
            errs.push(Error::invalid(
                &tp.child("value"),
                value.to_string(),
                msgs.join(";"),
            ));
        }
        if !TAINT_EFFECTS.contains(&taint.effect.as_str()) {
            errs.push(Error::not_supported(
                &tp.child("effect"),
                taint.effect.clone(),
                &TAINT_EFFECTS,
            ));
        }
        if !seen.insert((taint.key.as_str(), taint.effect.as_str())) {
            let mut e = Error::duplicate(&tp, taint.key.clone());
            e.detail = "taints must be unique by key and effect pair".to_string();
            errs.push(e);
        }
    }
    errs
}

/// Port of `ValidateNodeSpecificAnnotations` (validation.go:7133-7144) for
/// the taints annotation: `ValidateTaintsInNodeAnnotations` (:7083-7097)
/// decodes it as a taint list (`helper.GetTaintsFromNodeAnnotations`) and
/// validates each entry.
///
/// The `preferAvoidPods` half is `ValidateAvoidPodsInNodeAnnotations`
/// (:5096-5118), see [`validate_avoid_pods_in_node_annotations`].
fn validate_node_specific_annotations(
    annotations: Option<&HashMap<String, String>>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(raw) = annotations
        .and_then(|a| a.get(TAINTS_ANNOTATION_KEY))
        .filter(|v| !v.is_empty())
    {
        match serde_json::from_str::<Vec<Taint>>(raw) {
            Ok(taints) => {
                errs.extend(validate_node_taints(
                    &taints,
                    &fld_path.child(TAINTS_ANNOTATION_KEY),
                ));
            }
            Err(e) => errs.push(Error::invalid(
                fld_path,
                TAINTS_ANNOTATION_KEY.to_string(),
                e.to_string(),
            )),
        }
    }
    if let Some(raw) = annotations
        .and_then(|a| a.get(PREFER_AVOID_PODS_ANNOTATION_KEY))
        .filter(|v| !v.is_empty())
    {
        errs.extend(validate_avoid_pods_in_node_annotations(raw, fld_path));
    }
    errs
}

/// `core.PreferAvoidPodsAnnotationKey` (pkg/apis/core/annotation_key_constants.go).
pub const PREFER_AVOID_PODS_ANNOTATION_KEY: &str = "scheduler.alpha.kubernetes.io/preferAvoidPods";

/// `v1.AvoidPods` (staging/src/k8s.io/api/core/v1/types.go).
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct AvoidPods {
    prefer_avoid_pods: Vec<PreferAvoidPodsEntry>,
}

/// `v1.PreferAvoidPodsEntry`.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct PreferAvoidPodsEntry {
    pod_signature: Option<PodSignature>,
}

/// `v1.PodSignature`: the controller the pod belongs to.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct PodSignature {
    pod_controller: Option<PodController>,
}

/// The `metav1.OwnerReference` of a `PodSignature`; only `controller` is
/// validated, and Go decodes the other fields leniently, so they are not
/// modelled (a missing `uid` etc. must not fail decoding).
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct PodController {
    controller: Option<bool>,
}

/// Port of `ValidateAvoidPodsInNodeAnnotations` (validation.go:5096-5118) and
/// `validatePreferAvoidPodsEntry` (:5121-5135). `raw` is the non-empty
/// annotation value; decoding mirrors
/// `GetAvoidPodsFromNodeAnnotations` (component-helpers/scheduling/corev1/helpers.go:52-61).
///
/// Deviation: upstream dereferences `PodController.Controller` unchecked and
/// would panic when it is unset; here an unset value is treated as `false`.
fn validate_avoid_pods_in_node_annotations(raw: &str, fld_path: &Path) -> ErrorList {
    let avoids = match serde_json::from_str::<AvoidPods>(raw) {
        Ok(a) => a,
        Err(e) => {
            return vec![Error::invalid(
                &fld_path.child("AvoidPods"),
                PREFER_AVOID_PODS_ANNOTATION_KEY.to_string(),
                e.to_string(),
            )]
        }
    };
    let mut errs: ErrorList = Vec::new();
    for (i, entry) in avoids.prefer_avoid_pods.iter().enumerate() {
        let idx_path = fld_path.child(PREFER_AVOID_PODS_ANNOTATION_KEY).index(i);
        match entry
            .pod_signature
            .as_ref()
            .and_then(|s| s.pod_controller.as_ref())
        {
            None => errs.push(Error::required(&idx_path.child("PodSignature"), "")),
            Some(controller) => {
                if controller.controller != Some(true) {
                    errs.push(Error::invalid(
                        &idx_path
                            .child("PodSignature")
                            .child("PodController")
                            .child("Controller"),
                        false,
                        "must point to a controller",
                    ));
                }
            }
        }
    }
    errs
}

/// Port of `ValidateNodeResources` (validation.go:7219-7234):
/// `ValidateResourceQuantityValue` over `status.capacity` and
/// `status.allocatable`.
fn validate_node_resources(node: &Node) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(status) = node.status.as_ref() else {
        return errs;
    };
    let status_path = Path::new("status");
    for (field, map) in [
        ("capacity", status.capacity.as_ref()),
        ("allocatable", status.allocatable.as_ref()),
    ] {
        for (name, value) in map.into_iter().flatten() {
            errs.extend(validate_resource_quantity_value(
                name,
                value,
                &status_path.child(field).child(name),
            ));
        }
    }
    errs
}

/// Port of `validateNodeSwapStatus` (validation.go:9567-9583).
fn validate_node_swap_status(swap: Option<&NodeSwapStatus>, fld_path: &Path) -> ErrorList {
    match swap.and_then(|s| s.capacity) {
        Some(capacity) if capacity <= 0 => vec![Error::invalid(
            &fld_path.child("capacity"),
            capacity.to_string(),
            IS_NOT_POSITIVE_ERROR_MSG,
        )],
        _ => Vec::new(),
    }
}

fn declared_features(node: &Node) -> &[String] {
    node.status
        .as_ref()
        .and_then(|s| s.declared_features.as_deref())
        .unwrap_or_default()
}

fn taints(node: &Node) -> &[Taint] {
    node.spec
        .as_ref()
        .and_then(|s| s.taints.as_deref())
        .unwrap_or_default()
}

fn pod_cidrs(node: &Node) -> &[String] {
    node.spec
        .as_ref()
        .and_then(|s| s.pod_cidrs.as_deref())
        .unwrap_or_default()
}

fn provider_id(node: &Node) -> &str {
    node.spec
        .as_ref()
        .and_then(|s| s.provider_id.as_deref())
        .unwrap_or("")
}

/// Port of `ValidateNode` (validation.go:7178-7216).
///
/// Upstream roots the taint path at `metadata` (`fldPath.Child("taints")`
/// with `fldPath := field.NewPath("metadata")`), so taint errors read
/// `metadata.taints[i]`; that is kept.
pub fn validate_node(node: &Node) -> ErrorList {
    let meta_path = Path::new("metadata");
    let mut errs = validate_object_meta(&node.metadata, false, name_is_dns_subdomain, &meta_path);
    errs.extend(validate_node_specific_annotations(
        node.metadata.annotations.as_ref(),
        &meta_path.child("annotations"),
    ));
    let taints = taints(node);
    if !taints.is_empty() {
        errs.extend(validate_node_taints(taints, &meta_path.child("taints")));
    }

    errs.extend(validate_node_resources(node));
    errs.extend(validate_node_swap_status(
        node.status
            .as_ref()
            .and_then(|s| s.node_info.as_ref())
            .and_then(|i| i.swap.as_ref()),
        &meta_path.child("nodeSwapStatus"),
    ));
    errs.extend(validate_node_declared_features(
        declared_features(node),
        &Path::new("status").child("declaredFeatures"),
    ));

    let cidrs = pod_cidrs(node);
    if !cidrs.is_empty() {
        let cidrs_path = Path::new("spec").child("podCIDRs");
        let mut families: Vec<Option<IpFamily>> = Vec::with_capacity(cidrs.len());
        for (i, c) in cidrs.iter().enumerate() {
            let fam = parse_cidr(c);
            if fam.is_none() {
                errs.push(Error::invalid(
                    &cidrs_path.index(i),
                    c.clone(),
                    "must be a valid CIDR value, (e.g. 10.9.8.0/24 or 2001:db8::/64)",
                ));
            }
            families.push(fam);
        }
        if cidrs.len() > 1 {
            // validation.go:7205-7208: a ParseCIDRs failure (netutils.ParseCIDRs,
            // k8s.io/utils/net/net.go:30-40, first bad index) is an InternalError
            // on top of the Invalid below (dualStack is false then).
            if let Some(i) = families.iter().position(Option::is_none) {
                errs.push(Error::internal(
                    &cidrs_path,
                    format!(
                        "invalid PodCIDRs. failed to check with dual stack with error:invalid CIDR[{i}]: <nil> (invalid CIDR address: {})",
                        cidrs[i]
                    ),
                ));
            }
            let dual_stack = cidrs.len() == 2
                && matches!((families[0], families[1]), (Some(a), Some(b)) if a != b);
            if !dual_stack {
                errs.push(Error::invalid(
                    &cidrs_path,
                    cidrs.join(","),
                    "may specify no more than one CIDR for each IP family",
                ));
            }
        }
    }

    errs
}

/// Port of `ValidateNodeUpdate` (validation.go:7237-7303). The Node strategy
/// runs it after [`validate_node`]; the status strategy runs it alone.
pub fn validate_node_update(new_node: &Node, old_node: &Node) -> ErrorList {
    let meta_path = Path::new("metadata");
    let mut errs = validate_object_meta_update(&new_node.metadata, &old_node.metadata, &meta_path);
    errs.extend(validate_node_specific_annotations(
        new_node.metadata.annotations.as_ref(),
        &meta_path.child("annotations"),
    ));
    errs.extend(validate_node_resources(new_node));

    let status_path = Path::new("status");
    errs.extend(validate_node_declared_features(
        declared_features(new_node),
        &status_path.child("declaredFeatures"),
    ));

    if let Some(addresses) = new_node.status.as_ref().and_then(|s| s.addresses.as_ref()) {
        let addr_path = status_path.child("addresses");
        let mut seen: HashSet<(&str, &str)> = HashSet::new();
        for (i, addr) in addresses.iter().enumerate() {
            if !seen.insert((addr.address_type.as_str(), addr.address.as_str())) {
                errs.push(Error::duplicate(
                    &addr_path.index(i),
                    format!("{}/{}", addr.address_type, addr.address),
                ));
            }
        }
    }

    // The controller manager may assign a CIDR to a node that has none.
    let spec = Path::new("spec");
    let old_cidrs = pod_cidrs(old_node);
    if !old_cidrs.is_empty() && old_cidrs != pod_cidrs(new_node) {
        errs.push(Error::forbidden(
            &spec.child("podCIDRs"),
            "node updates may not change podCIDR except from \"\" to valid",
        ));
    }

    let old_pid = provider_id(old_node);
    if !old_pid.is_empty() && old_pid != provider_id(new_node) {
        errs.push(Error::forbidden(
            &spec.child("providerID"),
            "node updates may not change providerID except from \"\" to valid",
        ));
    }

    if let Some(config) = new_node.status.as_ref().and_then(|s| s.config.as_ref()) {
        errs.extend(validate_node_config_status(
            config,
            &status_path.child("config"),
        ));
    }

    let taints = taints(new_node);
    if !taints.is_empty() {
        errs.extend(validate_node_taints(taints, &meta_path.child("taints")));
    }

    errs
}

/// Port of upstream `validateNodeDeclaredFeatureName`.
fn validate_node_declared_feature_name(name: &str) -> Option<String> {
    if name.len() > DNS1123_SUBDOMAIN_MAX_LENGTH {
        return Some(format!(
            "invalid feature name {name:?}: must be no more than {DNS1123_SUBDOMAIN_MAX_LENGTH} characters"
        ));
    }
    if !NODE_DECLARED_FEATURE_REGEX.is_match(name) {
        return Some(format!(
            "invalid feature name {name:?}: must start with an UpperCamelCase segment, with subsequent segments separated by '/' (e.g., MyFeature or MyFeature/mySubFeature), and contain only alphanumeric characters and slashes"
        ));
    }
    None
}

/// Port of upstream `validateNodeDeclaredFeatures`: each name valid, list
/// sorted alphabetically with no adjacent duplicates.
fn validate_node_declared_features(features: &[String], fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    for (i, feature) in features.iter().enumerate() {
        if let Some(msg) = validate_node_declared_feature_name(feature) {
            errs.push(Error::invalid(&fld_path.index(i), feature.clone(), msg));
        }
        if i + 1 < features.len() {
            let next = &features[i + 1];
            if feature == next {
                errs.push(Error::duplicate(&fld_path.index(i + 1), next.clone()));
            } else if feature.as_str() > next.as_str() {
                errs.push(Error::invalid(
                    &fld_path.index(i + 1),
                    next.clone(),
                    "list must be sorted alphabetically".to_string(),
                ));
            }
        }
    }
    errs
}

/// Port of upstream `validateConfigMapNodeConfigSource`: target ConfigMap
/// namespace (DNS-1123 label), name (DNS-1123 subdomain), and `kubeletConfigKey`
/// (a valid ConfigMap key) are all required and well-formed.
fn validate_config_map_node_config_source(
    source: &ConfigMapNodeConfigSource,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if source.namespace.is_empty() {
        errs.push(Error::required(&fld_path.child("namespace"), ""));
    } else {
        for msg in is_dns1123_label(&source.namespace) {
            errs.push(Error::invalid(
                &fld_path.child("namespace"),
                source.namespace.clone(),
                msg,
            ));
        }
    }
    if source.name.is_empty() {
        errs.push(Error::required(&fld_path.child("name"), ""));
    } else {
        for msg in is_dns1123_subdomain(&source.name) {
            errs.push(Error::invalid(
                &fld_path.child("name"),
                source.name.clone(),
                msg,
            ));
        }
    }
    let key = source.kubelet_config_key.as_deref().unwrap_or("");
    if key.is_empty() {
        errs.push(Error::required(&fld_path.child("kubeletConfigKey"), ""));
    } else {
        for msg in crate::validation::configmap::config_map_key_errors(key) {
            errs.push(Error::invalid(
                &fld_path.child("kubeletConfigKey"),
                key.to_string(),
                msg,
            ));
        }
    }
    errs
}

/// Port of upstream `validateConfigMapNodeConfigSourceStatus`: a status source
/// additionally requires `uid` and `resourceVersion`.
fn validate_config_map_node_config_source_status(
    source: &ConfigMapNodeConfigSource,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if source.uid.as_deref().unwrap_or("").is_empty() {
        errs.push(Error::required(&fld_path.child("uid"), ""));
    }
    if source.resource_version.as_deref().unwrap_or("").is_empty() {
        errs.push(Error::required(&fld_path.child("resourceVersion"), ""));
    }
    errs.extend(validate_config_map_node_config_source(source, fld_path));
    errs
}

/// Port of upstream `validateNodeConfigSourceStatus`: exactly one reference
/// subfield (currently only `configMap`) must be set.
fn validate_node_config_source_status(source: &NodeConfigSource, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut count = 0;
    if let Some(cm) = &source.config_map {
        count += 1;
        errs.extend(validate_config_map_node_config_source_status(
            cm,
            &fld_path.child("configMap"),
        ));
    }
    if count != 1 {
        errs.push(Error::invalid(
            fld_path,
            "<configSource>".to_string(),
            "exactly one reference subfield must be non-nil",
        ));
    }
    errs
}

/// Port of upstream `validateNodeConfigStatus`: validate the assigned / active /
/// lastKnownGood config sources when set.
fn validate_node_config_status(config: &NodeConfigStatus, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(assigned) = &config.assigned {
        errs.extend(validate_node_config_source_status(
            assigned,
            &fld_path.child("assigned"),
        ));
    }
    if let Some(active) = &config.active {
        errs.extend(validate_node_config_source_status(
            active,
            &fld_path.child("active"),
        ));
    }
    if let Some(lkg) = &config.last_known_good {
        errs.extend(validate_node_config_source_status(
            lkg,
            &fld_path.child("lastKnownGood"),
        ));
    }
    errs
}

#[cfg(test)]
mod status_update_tests {
    use super::*;

    fn node_with_status(status: serde_json::Value) -> Node {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "node-1", "resourceVersion": "1"},
            "status": status,
        }))
        .expect("node decodes")
    }

    #[test]
    fn clean_status_passes() {
        let node = node_with_status(serde_json::json!({
            "addresses": [
                {"type": "InternalIP", "address": "10.0.0.1"},
                {"type": "Hostname", "address": "node-1"}
            ],
            "capacity": {"cpu": "4", "memory": "8Gi"},
            "allocatable": {"cpu": "4", "memory": "8Gi"},
            "declaredFeatures": ["GuaranteedQoSPodCPUResize"]
        }));
        assert!(validate_node_update(&node, &node).is_empty());
    }

    #[test]
    fn duplicate_address_is_rejected() {
        let node = node_with_status(serde_json::json!({
            "addresses": [
                {"type": "InternalIP", "address": "10.0.0.1"},
                {"type": "InternalIP", "address": "10.0.0.1"}
            ]
        }));
        let errs = validate_node_update(&node, &node);
        assert!(
            errs.iter().any(|e| e.field.contains("addresses")),
            "{errs:?}"
        );
    }

    #[test]
    fn negative_capacity_is_rejected() {
        let node = node_with_status(serde_json::json!({
            "capacity": {"cpu": "-1"}
        }));
        let errs = validate_node_update(&node, &node);
        assert!(
            errs.iter()
                .any(|e| e.field.contains("capacity") && e.detail.contains("greater than or equal")),
            "{errs:?}"
        );
    }

    #[test]
    fn unsorted_declared_features_rejected() {
        let node = node_with_status(serde_json::json!({
            "declaredFeatures": ["ZebraFeature", "AlphaFeature"]
        }));
        let errs = validate_node_update(&node, &node);
        assert!(
            errs.iter()
                .any(|e| e.detail.contains("sorted alphabetically")),
            "{errs:?}"
        );
    }

    #[test]
    fn bad_declared_feature_name_rejected() {
        let node = node_with_status(serde_json::json!({
            "declaredFeatures": ["lowercaseStart"]
        }));
        let errs = validate_node_update(&node, &node);
        assert!(
            errs.iter().any(|e| e.detail.contains("UpperCamelCase")),
            "{errs:?}"
        );
    }

    #[test]
    fn empty_status_passes() {
        let node = node_with_status(serde_json::json!({}));
        assert!(validate_node_update(&node, &node).is_empty());
    }

    #[test]
    fn config_status_valid_assigned_passes() {
        let node = node_with_status(serde_json::json!({
            "config": {"assigned": {"configMap": {
                "namespace": "kube-system", "name": "my-config",
                "uid": "abc-123", "resourceVersion": "42", "kubeletConfigKey": "kubelet"
            }}}
        }));
        assert!(
            validate_node_update(&node, &node).is_empty(),
            "{:?}",
            validate_node_update(&node, &node)
        );
    }

    #[test]
    fn config_status_requires_uid_and_resource_version() {
        let node = node_with_status(serde_json::json!({
            "config": {"active": {"configMap": {
                "namespace": "kube-system", "name": "my-config", "kubeletConfigKey": "kubelet"
            }}}
        }));
        let errs = validate_node_update(&node, &node);
        assert!(errs.iter().any(|e| e.field.ends_with("uid")), "{errs:?}");
        assert!(
            errs.iter().any(|e| e.field.ends_with("resourceVersion")),
            "{errs:?}"
        );
    }

    #[test]
    fn config_status_empty_source_rejected() {
        // a NodeConfigSource with no subfield set -> "exactly one reference subfield"
        let node = node_with_status(serde_json::json!({
            "config": {"assigned": {}}
        }));
        let errs = validate_node_update(&node, &node);
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("exactly one reference subfield")),
            "{errs:?}"
        );
    }

    #[test]
    fn config_status_bad_namespace_and_key_rejected() {
        let node = node_with_status(serde_json::json!({
            "config": {"assigned": {"configMap": {
                "namespace": "Bad_NS", "name": "cfg", "uid": "u", "resourceVersion": "1",
                "kubeletConfigKey": "bad key!"
            }}}
        }));
        let errs = validate_node_update(&node, &node);
        assert!(
            errs.iter().any(|e| e.field.ends_with("namespace")),
            "{errs:?}"
        );
        assert!(
            errs.iter().any(|e| e.field.ends_with("kubeletConfigKey")),
            "{errs:?}"
        );
    }
}

#[cfg(test)]
mod avoid_pods_tests {
    use super::*;

    const KEY: &str = "scheduler.alpha.kubernetes.io/preferAvoidPods";

    fn node_with_avoid(raw: &str) -> Node {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "abc-123", "annotations": {KEY: raw}},
        }))
        .expect("node decodes")
    }

    fn fields(raw: &str) -> Vec<String> {
        validate_node(&node_with_avoid(raw))
            .into_iter()
            .map(|e| e.field)
            .collect()
    }

    // validation_test.go:18415-18435 (valid entry).
    #[test]
    fn valid_entry_passes() {
        let raw = r#"{"preferAvoidPods":[{"podSignature":{"podController":{
            "apiVersion":"v1","kind":"ReplicationController","name":"foo",
            "uid":"abcdef123456","controller":true}},
            "reason":"some reason","message":"some message"}]}"#;
        assert!(fields(raw).is_empty());
    }

    // validation_test.go:18608-18628 ("missing-podSignature").
    #[test]
    fn missing_pod_signature_is_required() {
        let f = fields(r#"{"preferAvoidPods":[{"reason":"r","message":"m"}]}"#);
        assert_eq!(
            f,
            vec![format!("metadata.annotations.{KEY}[0].PodSignature")]
        );
    }

    // validation_test.go:18630-18660 ("invalid-podController").
    #[test]
    fn non_controller_pod_controller_is_invalid() {
        let raw = r#"{"preferAvoidPods":[{"podSignature":{"podController":{
            "apiVersion":"v1","kind":"ReplicationController","name":"foo",
            "uid":"abcdef123456","controller":false}}}]}"#;
        let errs = validate_node(&node_with_avoid(raw));
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(
            errs[0].field,
            format!("metadata.annotations.{KEY}[0].PodSignature.PodController.Controller")
        );
        assert!(errs[0].detail.contains("must point to a controller"));
    }

    #[test]
    fn undecodable_annotation_is_invalid_at_avoid_pods() {
        let f = fields("not json");
        assert_eq!(f, vec!["metadata.annotations.AvoidPods".to_string()]);
    }
}

#[cfg(test)]
mod pod_cidr_tests {
    use super::*;
    use crate::validation::field::ErrorType;

    fn node_with_cidrs(cidrs: &[&str]) -> Node {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "abc-123"},
            "spec": {"podCIDRs": cidrs},
        }))
        .expect("node decodes")
    }

    // validation.go:7200 validates each podCIDR with IsValidCIDRForLegacyField
    // (ParseCIDRSloppy), so a leading-zero IPv4 CIDR is accepted (and only
    // warned about by nodeWarnings -> GetWarningsForCIDR).
    #[test]
    fn leading_zero_ipv4_cidr_is_accepted() {
        assert!(validate_node(&node_with_cidrs(&["010.009.008.0/24"])).is_empty());
    }

    // validation.go:7205: IsDualStackCIDRStrings -> ParseCIDRs -> ParseCIDRSloppy.
    #[test]
    fn leading_zero_ipv4_cidr_dual_stack_is_accepted() {
        assert!(validate_node(&node_with_cidrs(&["010.009.008.0/24", "2001:db8::/64"])).is_empty());
    }

    #[test]
    fn garbage_cidr_is_still_invalid() {
        let errs = validate_node(&node_with_cidrs(&["10.9.8.0/33"]));
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(errs[0].field, "spec.podCIDRs[0]");
    }

    // validation.go:7205-7208: when ParseCIDRs fails on a multi-CIDR list,
    // upstream emits field.InternalError(podCIDRsField, ...) and then, since
    // dualStack is false, the Invalid "no more than one CIDR" error too.
    #[test]
    fn unparseable_multi_cidr_list_emits_internal_error() {
        let errs = validate_node(&node_with_cidrs(&["10.9.8.0/24", "bogus"]));
        let internal: Vec<_> = errs
            .iter()
            .filter(|e| e.error_type == ErrorType::Internal)
            .collect();
        assert_eq!(internal.len(), 1, "{errs:?}");
        assert_eq!(internal[0].field, "spec.podCIDRs");
        assert_eq!(
            internal[0].detail,
            "invalid PodCIDRs. failed to check with dual stack with error:invalid CIDR[1]: <nil> (invalid CIDR address: bogus)"
        );
        // per-element Invalid + the dual-stack Invalid remain.
        assert!(errs.iter().any(|e| e.field == "spec.podCIDRs[1]"));
        assert!(errs.iter().any(|e| e.field == "spec.podCIDRs"
            && e.error_type == ErrorType::Invalid
            && e.detail == "may specify no more than one CIDR for each IP family"));
    }

    #[test]
    fn single_bad_cidr_has_no_internal_error() {
        let errs = validate_node(&node_with_cidrs(&["bogus"]));
        assert!(errs.iter().all(|e| e.error_type != ErrorType::Internal));
    }
}
