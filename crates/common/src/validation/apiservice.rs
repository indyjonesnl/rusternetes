//! APIService validation — port of
//! `staging/src/k8s.io/kube-aggregator/pkg/apis/apiregistration/validation/validation.go`
//! (release-1.35): `ValidateAPIService`, `ValidateAPIServiceUpdate`,
//! `ValidateAPIServiceStatus` and `ValidateAPIServiceStatusUpdate`.

use base64::Engine;

use crate::resources::{APIService, APIServiceStatus};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1035_label, is_dns1123_subdomain};
use crate::validation::objectmeta::{
    name_is_path_segment, validate_object_meta, validate_object_meta_update,
};

/// The decoded length of `spec.caBundle` (`len(apiService.Spec.CABundle)`).
/// A value that is not base64 never reaches the strategy upstream -- the
/// decoder rejects it -- so an undecodable one counts as its wire length.
fn ca_bundle_len(api_service: &APIService) -> usize {
    match api_service.spec.ca_bundle.as_deref() {
        None => 0,
        Some(s) => base64::engine::general_purpose::STANDARD
            .decode(s)
            .map(|b| b.len())
            .unwrap_or(s.len()),
    }
}

/// `ValidateAPIService` (validation.go:33-92).
pub fn validate_api_service(api_service: &APIService) -> ErrorList {
    let required_name = format!("{}.{}", api_service.spec.version, api_service.spec.group);
    let meta_path = Path::new("metadata");

    // The name func (validation.go:36-47): `path.IsValidPathSegmentName`, then
    // the name must be `version.group`. `validate_object_meta` takes a plain
    // function, so the second half is applied here, for `name` and for
    // `generateName` (which the func is also called with, `prefix=true`).
    let mismatch = format!("must be `spec.version+\".\"+spec.group`: \"{required_name}\"");
    let mut errs = validate_object_meta(
        &api_service.metadata,
        false,
        name_is_path_segment,
        &meta_path,
    );
    if let Some(gn) = api_service
        .metadata
        .generate_name
        .as_deref()
        .filter(|g| !g.is_empty())
    {
        if name_is_path_segment(gn, true).is_empty() && gn != required_name {
            errs.push(Error::invalid(
                &meta_path.child("generateName"),
                gn.to_string(),
                mismatch.clone(),
            ));
        }
    }
    if !api_service.metadata.name.is_empty()
        && name_is_path_segment(&api_service.metadata.name, false).is_empty()
        && api_service.metadata.name != required_name
    {
        errs.push(Error::invalid(
            &meta_path.child("name"),
            api_service.metadata.name.clone(),
            mismatch,
        ));
    }

    let spec = &api_service.spec;
    let spec_path = Path::new("spec");

    // in this case we allow empty group
    if spec.group.is_empty() && spec.version != "v1" {
        errs.push(Error::required(
            &spec_path.child("group"),
            "only v1 may have an empty group and it better be legacy kube",
        ));
    }
    if !spec.group.is_empty() {
        for msg in is_dns1123_subdomain(&spec.group) {
            errs.push(Error::invalid(
                &spec_path.child("group"),
                spec.group.clone(),
                msg,
            ));
        }
    }

    for msg in is_dns1035_label(&spec.version) {
        errs.push(Error::invalid(
            &spec_path.child("version"),
            spec.version.clone(),
            msg,
        ));
    }

    if spec.group_priority_minimum <= 0 || spec.group_priority_minimum > 20000 {
        errs.push(Error::invalid(
            &spec_path.child("groupPriorityMinimum"),
            spec.group_priority_minimum,
            "must be positive and less than 20000",
        ));
    }
    if spec.version_priority <= 0 || spec.version_priority > 1000 {
        errs.push(Error::invalid(
            &spec_path.child("versionPriority"),
            spec.version_priority,
            "must be positive and less than 1000",
        ));
    }

    let ca_len = ca_bundle_len(api_service);
    let Some(service) = &spec.service else {
        if ca_len != 0 {
            errs.push(Error::invalid(
                &spec_path.child("caBundle"),
                format!("{ca_len} bytes"),
                "local APIServices may not have a caBundle",
            ));
        }
        if spec.insecure_skip_tls_verify {
            errs.push(Error::invalid(
                &spec_path.child("insecureSkipTLSVerify"),
                spec.insecure_skip_tls_verify,
                "local APIServices may not have insecureSkipTLSVerify",
            ));
        }
        return errs;
    };

    let service_path = spec_path.child("service");
    if service.namespace.is_empty() {
        errs.push(Error::required(&service_path.child("namespace"), ""));
    }
    if service.name.is_empty() {
        errs.push(Error::required(&service_path.child("name"), ""));
    }
    let port = service.port.unwrap_or(0);
    if !(1..=65535).contains(&port) {
        errs.push(Error::invalid(
            &service_path.child("port"),
            port,
            "port is not valid: must be between 1 and 65535, inclusive",
        ));
    }
    if spec.insecure_skip_tls_verify && ca_len > 0 {
        errs.push(Error::invalid(
            &spec_path.child("insecureSkipTLSVerify"),
            spec.insecure_skip_tls_verify,
            "may not be true if caBundle is present",
        ));
    }

    errs
}

/// `ValidateAPIServiceUpdate` (validation.go:94-100).
pub fn validate_api_service_update(new: &APIService, old: &APIService) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_api_service(new));
    errs
}

/// `ValidateAPIServiceStatus` (validation.go:102-116): a condition status is
/// one of `True`, `False` or `Unknown`.
pub fn validate_api_service_status(status: &APIServiceStatus, fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    for (i, condition) in status.conditions.iter().enumerate() {
        if !matches!(condition.status.as_str(), "True" | "False" | "Unknown") {
            errs.push(Error::not_supported(
                &fld_path.child("conditions").index(i).child("status"),
                condition.status.clone(),
                &["True", "False", "Unknown"],
            ));
        }
    }
    errs
}

/// `ValidateAPIServiceStatusUpdate` (validation.go:118-124).
pub fn validate_api_service_status_update(new: &APIService, old: &APIService) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_api_service_status(
        &new.status,
        &Path::new("status"),
    ));
    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::APIServiceReference;

    fn valid(name: &str) -> APIService {
        let mut a = APIService::default();
        a.metadata.name = name.to_string();
        a.spec.group = "wardle.example.com".to_string();
        a.spec.version = "v1alpha1".to_string();
        a.spec.group_priority_minimum = 2000;
        a.spec.version_priority = 200;
        a
    }

    fn remote() -> APIService {
        let mut a = valid("v1alpha1.wardle.example.com");
        a.spec.service = Some(APIServiceReference {
            namespace: "ns".into(),
            name: "svc".into(),
            port: Some(443),
        });
        a
    }

    fn fields(errs: &ErrorList) -> Vec<String> {
        errs.iter().map(|e| e.field.clone()).collect()
    }

    #[test]
    fn a_well_formed_apiservice_is_valid() {
        assert!(validate_api_service(&remote()).is_empty());
        assert!(validate_api_service(&valid("v1alpha1.wardle.example.com")).is_empty());
    }

    #[test]
    fn the_name_must_be_version_dot_group() {
        let errs = validate_api_service(&valid("wrong"));
        assert_eq!(fields(&errs), ["metadata.name"]);
        assert_eq!(
            errs[0].detail,
            "must be `spec.version+\".\"+spec.group`: \"v1alpha1.wardle.example.com\""
        );
    }

    #[test]
    fn a_name_that_is_not_a_path_segment_reports_only_that() {
        let errs = validate_api_service(&valid("a/b"));
        assert_eq!(fields(&errs), ["metadata.name"]);
        assert_eq!(errs[0].detail, "may not contain '/'");
    }

    #[test]
    fn only_v1_may_have_an_empty_group() {
        let mut a = valid("v1alpha1.");
        a.spec.group.clear();
        assert!(fields(&validate_api_service(&a)).contains(&"spec.group".to_string()));
        let mut a = valid("v1.");
        a.spec.group.clear();
        a.spec.version = "v1".into();
        assert!(validate_api_service(&a).is_empty());
    }

    #[test]
    fn priorities_are_bounded() {
        let mut a = valid("v1alpha1.wardle.example.com");
        a.spec.group_priority_minimum = 0;
        a.spec.version_priority = 1001;
        assert_eq!(
            fields(&validate_api_service(&a)),
            ["spec.groupPriorityMinimum", "spec.versionPriority"]
        );
        a.spec.group_priority_minimum = 20001;
        a.spec.version_priority = 0;
        assert_eq!(
            fields(&validate_api_service(&a)),
            ["spec.groupPriorityMinimum", "spec.versionPriority"]
        );
    }

    #[test]
    fn a_local_apiservice_may_not_carry_tls_settings() {
        let mut a = valid("v1alpha1.wardle.example.com");
        a.spec.ca_bundle = Some(base64::engine::general_purpose::STANDARD.encode(b"pem"));
        a.spec.insecure_skip_tls_verify = true;
        let errs = validate_api_service(&a);
        assert_eq!(
            fields(&errs),
            ["spec.caBundle", "spec.insecureSkipTLSVerify"]
        );
        assert_eq!(errs[0].detail, "local APIServices may not have a caBundle");
    }

    #[test]
    fn a_service_reference_needs_namespace_name_and_a_valid_port() {
        let mut a = remote();
        a.spec.service = Some(APIServiceReference {
            namespace: String::new(),
            name: String::new(),
            port: Some(70000),
        });
        assert_eq!(
            fields(&validate_api_service(&a)),
            [
                "spec.service.namespace",
                "spec.service.name",
                "spec.service.port"
            ]
        );
    }

    #[test]
    fn insecure_skip_tls_verify_conflicts_with_a_ca_bundle() {
        let mut a = remote();
        a.spec.ca_bundle = Some(base64::engine::general_purpose::STANDARD.encode(b"pem"));
        a.spec.insecure_skip_tls_verify = true;
        let errs = validate_api_service(&a);
        assert_eq!(fields(&errs), ["spec.insecureSkipTLSVerify"]);
        assert_eq!(errs[0].detail, "may not be true if caBundle is present");
    }

    #[test]
    fn a_status_condition_must_be_true_false_or_unknown() {
        let mut a = remote();
        a.status
            .conditions
            .push(crate::resources::APIServiceCondition {
                type_: "Available".into(),
                status: "Maybe".into(),
                ..Default::default()
            });
        let errs = validate_api_service_status(&a.status, &Path::new("status"));
        assert_eq!(fields(&errs), ["status.conditions[0].status"]);
    }
}
