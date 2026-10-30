//! Unit tests moved from `src/validation/apiservice.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use base64::Engine;
    pub use rusternetes_common::resources::{APIService, APIServiceStatus};
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{is_dns1035_label, is_dns1123_subdomain};
    pub use rusternetes_common::validation::objectmeta::{
        name_is_path_segment, validate_object_meta, validate_object_meta_update,
    };
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::APIServiceReference;
    use rusternetes_common::validation::apiservice::*;

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
            .push(rusternetes_common::resources::APIServiceCondition {
                type_: "Available".into(),
                status: "Maybe".into(),
                ..Default::default()
            });
        let errs = validate_api_service_status(&a.status, &Path::new("status"));
        assert_eq!(fields(&errs), ["status.conditions[0].status"]);
    }
}
