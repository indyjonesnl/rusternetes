//! ResourceClaim (resource.k8s.io / DRA) validation — port of upstream
//! Kubernetes `pkg/apis/resource/validation/validation.go::ValidateResourceClaim`
//! (release-1.35).
//!
//! Scope: the CEL-free structural validation of `spec.devices.requests` —
//! request count cap + unique names, per-request `exactly`/`firstAvailable`
//! mutual exclusivity, `deviceClassName` (required + DNS subdomain),
//! `allocationMode` enum + `count` coupling, the `firstAvailable` sub-request
//! set, `selectors` (`validateSelectorSlice`, `:298`), `tolerations`
//! (`validateDeviceToleration`, `:1411`) and `config`
//! (`validateDeviceClaimConfiguration`, `:372`, which cross-checks each
//! entry's `requests` against `gatherRequestNames`, `:186`).
//!
//! Compiling a CEL expression against the DRA CEL environment is still tracked
//! in #1442 — the length/non-empty half of `validateCELSelector` is ported, the
//! compile is not. The status-side allocation results are tracked in #2016.
//! ObjectMeta is validated separately.

use crate::resources::{
    DeviceAllocationMode, DeviceClaim, DeviceClaimConfiguration, DeviceSelector, DeviceToleration,
    ExactDeviceRequest, OpaqueDeviceConfiguration, ResourceClaim, ResourceClaimSpec,
    TolerationOperator,
};
use crate::validation::csinode::validate_csi_driver_name;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_label, is_dns1123_subdomain, is_valid_label_value, validate_label_name,
};
use std::collections::{HashMap, HashSet};

const DEVICE_REQUESTS_MAX_SIZE: usize = 32;
const FIRST_AVAILABLE_MAX_SIZE: usize = 8;
/// `DeviceSelectorsMaxSize` (`staging/src/k8s.io/api/resource/v1/types.go:1081`).
pub(crate) const DEVICE_SELECTORS_MAX_SIZE: usize = 32;
/// `CELSelectorExpressionMaxLength` (`types.go:1188`).
pub(crate) const CEL_SELECTOR_EXPRESSION_MAX_LENGTH: usize = 10 * 1024;
/// `DeviceTolerationsMaxLength` (`types.go:1083`).
const DEVICE_TOLERATIONS_MAX_LENGTH: usize = 16;
/// `DeviceConfigMaxSize` (`types.go:769`).
const DEVICE_CONFIG_MAX_SIZE: usize = 32;
/// `OpaqueParametersMaxLength` (`types.go:1305`).
const OPAQUE_PARAMETERS_MAX_LENGTH: usize = 10 * 1024;

/// Validate a `ResourceClaim` on create. Mirrors the structural portion of
/// upstream `ValidateResourceClaim` (minus CEL/constraints/config — see #1442).
pub fn validate_resource_claim(claim: &ResourceClaim) -> ErrorList {
    validate_resource_claim_spec(&claim.spec, &Path::new("spec"))
}

/// Validate a `ResourceClaim` on update. Mirrors `ValidateResourceClaimUpdate`:
/// `spec` is immutable after creation, plus the create validation.
pub fn validate_resource_claim_update(new: &ResourceClaim, old: &ResourceClaim) -> ErrorList {
    let mut errs = validate_resource_claim(new);
    if serde_json::to_value(&new.spec).ok() != serde_json::to_value(&old.spec).ok() {
        errs.push(Error::invalid(
            &Path::new("spec"),
            "<spec>".to_string(),
            "field is immutable",
        ));
    }
    errs
}

/// Validate a `ResourceClaimSpec` at the given path. Shared by `ResourceClaim`
/// and `ResourceClaimTemplate` (upstream `validateResourceClaimSpec`).
pub fn validate_resource_claim_spec(spec: &ResourceClaimSpec, fld_path: &Path) -> ErrorList {
    validate_device_claim(&spec.devices, &fld_path.child("devices"))
}

/// Validate a `ResourceClaimTemplate` on create. Mirrors upstream
/// `ValidateResourceClaimTemplate`: validates the embedded `spec.spec`
/// (a `ResourceClaimSpec`). ObjectMeta is validated separately.
pub fn validate_resource_claim_template(
    template: &crate::resources::ResourceClaimTemplate,
) -> ErrorList {
    validate_resource_claim_spec(&template.spec.spec, &Path::new("spec").child("spec"))
}

/// The request names a `config` or `constraint` entry may reference: request
/// name -> sub-request names (empty when the request has no `firstAvailable`).
/// Port of upstream's `requestNames` map and `gatherRequestNames`
/// (`pkg/apis/resource/validation/validation.go:163-200`).
type RequestNames = HashMap<String, HashSet<String>>;

fn gather_request_names(claim: &DeviceClaim) -> RequestNames {
    let mut names: RequestNames = HashMap::new();
    for request in &claim.requests {
        let subs = request
            .first_available
            .iter()
            .map(|sub| sub.name.clone())
            .collect();
        names.insert(request.name.clone(), subs);
    }
    names
}

/// `requestNames.Has` (`validation.go:165-185`): `a` matches a request, `a/b` a
/// request and one of its sub-requests, anything with two `/` matches nothing.
fn request_names_has(names: &RequestNames, name: &str) -> bool {
    let segments: Vec<&str> = name.split('/').collect();
    if segments.len() > 2 {
        return false;
    }
    let Some(sub_names) = names.get(segments[0]) else {
        return false;
    };
    segments.len() == 1 || sub_names.contains(segments[1])
}

fn validate_device_claim(claim: &DeviceClaim, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let request_names = gather_request_names(claim);
    let requests_path = fld_path.child("requests");

    if claim.requests.len() > DEVICE_REQUESTS_MAX_SIZE {
        errs.push(Error::too_many(&requests_path, DEVICE_REQUESTS_MAX_SIZE));
    }

    let mut seen: HashSet<&str> = HashSet::new();
    for (i, request) in claim.requests.iter().enumerate() {
        let rp = requests_path.index(i);

        // name — required, DNS-1123 label, unique across requests.
        if request.name.is_empty() {
            errs.push(Error::required(&rp.child("name"), ""));
        } else {
            for msg in is_dns1123_label(&request.name) {
                errs.push(Error::invalid(&rp.child("name"), request.name.clone(), msg));
            }
            if !seen.insert(request.name.as_str()) {
                errs.push(Error::duplicate(&rp.child("name"), request.name.clone()));
            }
        }

        // exactly one of `exactly` / `firstAvailable`.
        let has_exactly = request.exactly.is_some();
        let has_first = !request.first_available.is_empty();
        match (has_exactly, has_first) {
            (false, false) => errs.push(Error::required(
                &rp,
                "exactly one of `exactly` or `firstAvailable` is required",
            )),
            (true, true) => errs.push(Error::invalid(
                &rp,
                String::new(),
                "exactly one of `exactly` or `firstAvailable` is required, but multiple fields are set",
            )),
            (true, false) => {
                errs.extend(validate_exact_device_request(
                    request.exactly.as_ref().unwrap(),
                    &rp.child("exactly"),
                ));
            }
            (false, true) => {
                let fa_path = rp.child("firstAvailable");
                if request.first_available.len() > FIRST_AVAILABLE_MAX_SIZE {
                    errs.push(Error::too_many(&fa_path, FIRST_AVAILABLE_MAX_SIZE));
                }
                let mut sub_seen: HashSet<&str> = HashSet::new();
                for (j, sub) in request.first_available.iter().enumerate() {
                    let sp = fa_path.index(j);
                    if sub.name.is_empty() {
                        errs.push(Error::required(&sp.child("name"), ""));
                    } else {
                        for msg in is_dns1123_label(&sub.name) {
                            errs.push(Error::invalid(&sp.child("name"), sub.name.clone(), msg));
                        }
                        if !sub_seen.insert(sub.name.as_str()) {
                            errs.push(Error::duplicate(&sp.child("name"), sub.name.clone()));
                        }
                    }
                    errs.extend(validate_device_class_name(
                        &sub.device_class_name,
                        &sp.child("deviceClassName"),
                    ));
                    errs.extend(validate_selector_slice(&sub.selectors, &sp.child("selectors")));
                    errs.extend(validate_allocation_mode(
                        sub.allocation_mode.as_ref(),
                        sub.count,
                        &sp,
                    ));
                    errs.extend(validate_tolerations(&sub.tolerations, &sp));
                }
            }
        }
    }

    // `config` (`validateDeviceClaim`, `validation.go:155-158`).
    let config_path = fld_path.child("config");
    if claim.config.len() > DEVICE_CONFIG_MAX_SIZE {
        errs.push(Error::too_many(&config_path, DEVICE_CONFIG_MAX_SIZE));
    }
    for (i, config) in claim.config.iter().enumerate() {
        errs.extend(validate_device_claim_configuration(
            config,
            &config_path.index(i),
            &request_names,
        ));
    }

    // `constraints[].requests` reference the same request names
    // (`validateDeviceConstraint`, `validation.go:361-371`). The
    // `matchAttribute`/`distinctAttribute` half needs the fully-qualified-name
    // rule and stays on #2016.
    let constraints_path = fld_path.child("constraints");
    for (i, constraint) in claim.constraints.iter().enumerate() {
        let requests_path = constraints_path.index(i).child("requests");
        for (j, name) in constraint.requests.iter().enumerate() {
            errs.extend(validate_request_name_ref(
                name,
                &requests_path.index(j),
                &request_names,
            ));
        }
    }

    errs
}

/// `validateSelectorSlice` + `validateSelector` + the CEL-free half of
/// `validateCELSelector` (`validation.go:298-330`). Shared with `DeviceClass`,
/// which validates the identical shape.
pub(crate) fn validate_selector_slice(selectors: &[DeviceSelector], fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if selectors.len() > DEVICE_SELECTORS_MAX_SIZE {
        errs.push(Error::too_many(fld_path, DEVICE_SELECTORS_MAX_SIZE));
    }
    for (i, selector) in selectors.iter().enumerate() {
        let sp = fld_path.index(i);
        match &selector.cel {
            None => errs.push(Error::required(&sp.child("cel"), "")),
            Some(cel) => {
                let expr_path = sp.child("cel").child("expression");
                if cel.expression.is_empty() {
                    errs.push(Error::required(&expr_path, ""));
                } else if cel.expression.len() > CEL_SELECTOR_EXPRESSION_MAX_LENGTH {
                    errs.push(Error::too_long(
                        &expr_path,
                        CEL_SELECTOR_EXPRESSION_MAX_LENGTH,
                    ));
                }
            }
        }
    }
    errs
}

/// `validateDeviceToleration` (`validation.go:1411-1437`) over a request's
/// `tolerations`, plus the `DeviceTolerationsMaxLength` cap the caller applies.
fn validate_tolerations(tolerations: &[DeviceToleration], fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let path = fld_path.child("tolerations");
    if tolerations.len() > DEVICE_TOLERATIONS_MAX_LENGTH {
        errs.push(Error::too_many(&path, DEVICE_TOLERATIONS_MAX_LENGTH));
    }
    for (i, toleration) in tolerations.iter().enumerate() {
        errs.extend(validate_device_toleration(toleration, &path.index(i)));
    }
    errs
}

fn validate_device_toleration(toleration: &DeviceToleration, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // An empty key matches every taint key, so it is checked only when set.
    if !toleration.key.is_empty() {
        errs.extend(validate_label_name(&toleration.key, &fld_path.child("key")));
    }

    let value = toleration.value.clone().unwrap_or_default();
    match &toleration.operator {
        Some(TolerationOperator::Exists) => {
            if !value.is_empty() {
                errs.push(Error::invalid(
                    &fld_path.child("value"),
                    value,
                    "must be empty for operator `Exists`",
                ));
            }
        }
        Some(TolerationOperator::Equal) => {
            for msg in is_valid_label_value(&value) {
                errs.push(Error::invalid(&fld_path.child("value"), value.clone(), msg));
            }
        }
        // Upstream's `case "":`. An operator that is neither `Equal` nor
        // `Exists` is a `NotSupported` upstream; the closed Rust enum rejects
        // it in the decoder instead.
        None => errs.push(Error::required(&fld_path.child("operator"), "")),
    }

    // `effect` is explicitly optional in a toleration (`validation.go:1428`),
    // and an unsupported one cannot reach here through the closed enum.
    errs
}

/// `validateDeviceClaimConfiguration` (`validation.go:372-380`).
fn validate_device_claim_configuration(
    config: &DeviceClaimConfiguration,
    fld_path: &Path,
    request_names: &RequestNames,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let requests_path = fld_path.child("requests");
    if config.requests.len() > DEVICE_REQUESTS_MAX_SIZE {
        errs.push(Error::too_many(&requests_path, DEVICE_REQUESTS_MAX_SIZE));
    }
    let mut seen: HashSet<&str> = HashSet::new();
    for (i, name) in config.requests.iter().enumerate() {
        let rp = requests_path.index(i);
        if !seen.insert(name.as_str()) {
            errs.push(Error::duplicate(&rp, name.clone()));
        }
        errs.extend(validate_request_name_ref(name, &rp, request_names));
    }

    // `validateDeviceConfiguration` (`validation.go:417-424`).
    match &config.opaque {
        None => errs.push(Error::required(&fld_path.child("opaque"), "")),
        Some(opaque) => {
            errs.extend(validate_opaque_configuration(
                opaque,
                &fld_path.child("opaque"),
            ));
        }
    }
    errs
}

/// `validateRequestNameRef` (`validation.go:382-400`).
fn validate_request_name_ref(
    name: &str,
    fld_path: &Path,
    request_names: &RequestNames,
) -> ErrorList {
    const DETAIL: &str = "must be the name of a request in the claim or the name of a request and \
                          a subrequest separated by '/'";
    let mut errs: ErrorList = Vec::new();
    let segments: Vec<&str> = name.split('/').collect();
    if segments.len() > 2 {
        errs.push(Error::invalid(fld_path, name.to_string(), DETAIL));
        return errs;
    }
    for segment in &segments {
        for msg in is_dns1123_label(segment) {
            errs.push(Error::invalid(fld_path, name.to_string(), msg));
        }
    }
    if !request_names_has(request_names, name) {
        errs.push(Error::invalid(fld_path, name.to_string(), DETAIL));
    }
    errs
}

/// `validateOpaqueConfiguration` (`validation.go:425-430`).
fn validate_opaque_configuration(opaque: &OpaqueDeviceConfiguration, fld_path: &Path) -> ErrorList {
    let mut errs = validate_csi_driver_name(&opaque.driver, &fld_path.child("driver"));
    errs.extend(validate_raw_extension(
        &opaque.parameters,
        &fld_path.child("parameters"),
        OPAQUE_PARAMETERS_MAX_LENGTH,
    ));
    errs
}

/// `validateRawExtension` (`validation.go:1297-1316`). Our decoder has already
/// parsed the JSON, so the "error parsing data as JSON" arm is unreachable —
/// what remains is the absent/null `Required`, the byte-length cap and the
/// "must be a valid JSON object" shape check, all keyed on `<value omitted>`
/// exactly as upstream reports them.
fn validate_raw_extension(
    parameters: &serde_json::Value,
    fld_path: &Path,
    max_length: usize,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if parameters.is_null() {
        errs.push(Error::required(fld_path, ""));
    } else if serde_json::to_string(parameters).map_or(0, |s| s.len()) > max_length {
        errs.push(Error::too_long(fld_path, max_length));
    } else if !parameters.is_object() {
        errs.push(Error::invalid(
            fld_path,
            "<value omitted>".to_string(),
            "must be a valid JSON object",
        ));
    }
    errs
}

fn validate_exact_device_request(req: &ExactDeviceRequest, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    errs.extend(validate_device_class_name(
        &req.device_class_name,
        &fld_path.child("deviceClassName"),
    ));
    errs.extend(validate_selector_slice(
        &req.selectors,
        &fld_path.child("selectors"),
    ));
    errs.extend(validate_allocation_mode(
        req.allocation_mode.as_ref(),
        req.count,
        fld_path,
    ));
    errs.extend(validate_tolerations(&req.tolerations, fld_path));
    errs
}

/// `validateDeviceAllocationMode` (`validation.go:267-286`), shared by an exact
/// request and a sub-request.
fn validate_allocation_mode(
    allocation_mode: Option<&DeviceAllocationMode>,
    count: Option<i64>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    // allocationMode + count coupling (only when allocationMode is set).
    if let Some(mode) = allocation_mode {
        let count = count.unwrap_or(0);
        match mode {
            DeviceAllocationMode::All => {
                if count != 0 {
                    errs.push(Error::invalid(
                        &fld_path.child("count"),
                        count,
                        "must not be specified when allocationMode is 'All'",
                    ));
                }
            }
            DeviceAllocationMode::ExactCount => {
                if count <= 0 {
                    errs.push(Error::invalid(
                        &fld_path.child("count"),
                        count,
                        "must be greater than zero",
                    ));
                }
            }
        }
    }
    errs
}

/// `deviceClassName` is required and a DNS-1123 subdomain (upstream
/// `validateDeviceClass`).
fn validate_device_class_name(name: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if name.is_empty() {
        errs.push(Error::required(fld_path, ""));
    } else {
        for msg in is_dns1123_subdomain(name) {
            errs.push(Error::invalid(fld_path, name.to_string(), msg));
        }
    }
    errs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim(devices: serde_json::Value) -> ResourceClaim {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "claim-1"},
            "spec": {"devices": devices},
        }))
        .unwrap()
    }

    fn errs(devices: serde_json::Value) -> Vec<String> {
        validate_resource_claim(&claim(devices))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn valid_exact_request_passes() {
        assert!(errs(serde_json::json!({
            "requests": [{
                "name": "gpu",
                "exactly": {"deviceClassName": "gpu.example.com", "allocationMode": "ExactCount", "count": 2}
            }]
        }))
        .is_empty());
    }

    #[test]
    fn request_needs_exactly_one_of_exactly_or_first_available() {
        // neither
        assert!(errs(serde_json::json!({"requests": [{"name": "r"}]}))
            .iter()
            .any(|m| m.contains("exactly one of")));
        // both
        assert!(errs(serde_json::json!({
            "requests": [{"name": "r",
                "exactly": {"deviceClassName": "c.example.com"},
                "firstAvailable": [{"name": "s", "deviceClassName": "c.example.com"}]}]
        }))
        .iter()
        .any(|m| m.contains("multiple fields are set")));
    }

    #[test]
    fn duplicate_request_names_rejected() {
        let e = errs(serde_json::json!({
            "requests": [
                {"name": "r", "exactly": {"deviceClassName": "c.example.com"}},
                {"name": "r", "exactly": {"deviceClassName": "c.example.com"}}
            ]
        }));
        assert!(
            e.iter().any(|m| m.to_lowercase().contains("duplicate")),
            "{e:?}"
        );
    }

    #[test]
    fn allocation_mode_count_coupling() {
        // ExactCount with count 0 -> error
        assert!(errs(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 0}}]
        }))
        .iter()
        .any(|m| m.contains("greater than zero")));
        // All with a count -> error
        assert!(errs(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "All", "count": 3}}]
        }))
        .iter()
        .any(|m| m.contains("must not be specified when allocationMode is 'All'")));
    }

    #[test]
    fn bad_device_class_name_rejected() {
        let e = errs(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": ""}}]
        }));
        assert!(e.iter().any(|m| m.contains("deviceClassName")), "{e:?}");
    }

    #[test]
    fn spec_immutable_on_update() {
        let old = claim(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 1}}]
        }));
        let new = claim(serde_json::json!({
            "requests": [{"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 2}}]
        }));
        let e = validate_resource_claim_update(&new, &old);
        assert!(
            e.iter()
                .any(|x| x.field == "spec" && x.detail == "field is immutable"),
            "{e:?}"
        );
    }

    #[test]
    fn template_validates_embedded_spec() {
        use crate::resources::ResourceClaimTemplate;
        let tmpl: ResourceClaimTemplate = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "t"},
            "spec": {"spec": {"devices": {"requests": [
                {"name": "r", "exactly": {"deviceClassName": "c.example.com", "allocationMode": "ExactCount", "count": 0}}
            ]}}}
        })).unwrap();
        let errs = validate_resource_claim_template(&tmpl);
        // count 0 with ExactCount -> error, attached under spec.spec.devices...
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("greater than zero")),
            "{errs:?}"
        );
        assert!(
            errs.iter()
                .any(|e| e.field.starts_with("spec.spec.devices")),
            "{errs:?}"
        );
    }
}
