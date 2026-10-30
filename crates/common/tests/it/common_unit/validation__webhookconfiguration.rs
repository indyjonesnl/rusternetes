//! Unit tests moved from `src/validation/webhookconfiguration.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::admission_webhook::{
        FailurePolicy, LabelSelector as WebhookLabelSelector, MatchCondition, MatchPolicy,
        MutatingWebhook, MutatingWebhookConfiguration, ReinvocationPolicy, RuleWithOperations,
        SideEffectClass, ValidatingWebhook, ValidatingWebhookConfiguration,
    };
    pub use rusternetes_common::resources::WebhookClientConfig;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        is_dns1035_label, is_dns1123_subdomain, validate_label_selector,
        LabelSelectorValidationOptions,
    };
    pub use rusternetes_common::validation::validating_admission_policy::validate_match_conditions;
    pub use rusternetes_common::validation::validating_admission_policy::PreexistingExpressions;
    pub use std::collections::HashSet;
}

mod deep_validation_tests {
    use super::__parent::*;
    use rusternetes_common::validation::webhookconfiguration::*;

    fn p() -> Path {
        Path::new("clientConfig")
    }

    #[test]
    fn valid_https_url_passes() {
        assert!(validate_webhook_url("https://example.com/hook", &p().child("url")).is_empty());
    }

    #[test]
    fn http_scheme_rejected() {
        let errs = validate_webhook_url("http://example.com", &p().child("url"));
        assert!(errs.iter().any(|e| e.detail.contains("https")), "{errs:?}");
    }

    #[test]
    fn url_with_query_and_fragment_rejected() {
        let errs = validate_webhook_url("https://example.com/h?a=1#frag", &p().child("url"));
        assert!(errs.iter().any(|e| e.detail.contains("query")), "{errs:?}");
        assert!(
            errs.iter().any(|e| e.detail.contains("fragment")),
            "{errs:?}"
        );
    }

    #[test]
    fn service_requires_name_and_namespace() {
        let errs = validate_webhook_service("", "", None, None, &p().child("service"));
        assert!(errs.iter().any(|e| e.field.ends_with("name")), "{errs:?}");
        assert!(
            errs.iter().any(|e| e.field.ends_with("namespace")),
            "{errs:?}"
        );
    }

    #[test]
    fn service_port_out_of_range_rejected() {
        let errs = validate_webhook_service("svc", "ns", None, Some(0), &p().child("service"));
        assert!(
            errs.iter().any(|e| e.detail.contains("port is not valid")),
            "{errs:?}"
        );
        assert!(
            validate_webhook_service("svc", "ns", None, Some(443), &p().child("service"))
                .is_empty()
        );
    }

    #[test]
    fn service_path_empty_segment_rejected() {
        let errs =
            validate_webhook_service("svc", "ns", Some("/a//b"), None, &p().child("service"));
        assert!(
            errs.iter().any(|e| e.detail.contains("may not be empty")),
            "{errs:?}"
        );
        assert!(validate_webhook_service(
            "svc",
            "ns",
            Some("/mutate"),
            None,
            &p().child("service")
        )
        .is_empty());
    }

    #[test]
    fn admission_review_version_must_be_dns1035() {
        // Leading digit is valid DNS1123 but not DNS1035.
        let errs = validate_admission_review_versions(
            &["1v".to_string(), "v1".to_string()],
            &Path::new("admissionReviewVersions"),
        );
        assert!(
            errs.iter().any(|e| e.detail.contains("DNS-1035")),
            "{errs:?}"
        );
    }
}

mod match_condition_update_tests {
    use super::__parent::*;
    use rusternetes_common::validation::webhookconfiguration::*;
    use serde_json::json;

    fn config(conditions: serde_json::Value) -> ValidatingWebhookConfiguration {
        serde_json::from_value(json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingWebhookConfiguration",
            "metadata": { "name": "hook.example.com" },
            "webhooks": [{
                "name": "hook.example.com",
                "clientConfig": { "url": "https://example.com/hook" },
                "sideEffects": "None",
                "admissionReviewVersions": ["v1"],
                "matchConditions": conditions,
            }],
        }))
        .expect("fixture decodes")
    }

    /// The create path always validates: a duplicate name is an error.
    #[test]
    fn a_create_validates_match_conditions() {
        let bad = config(json!([
            { "name": "same", "expression": "true" },
            { "name": "same", "expression": "false" }
        ]));
        let errs = validate_validating_webhook_configuration(&bad);
        assert!(
            errs.iter()
                .any(|e| e.field == "webhooks[0].matchConditions[1].name"),
            "{errs:?}"
        );
    }

    /// Upstream `ignoreValidatingWebhookMatchConditions` (`validation.go:605`):
    /// an unchanged list is left alone, so an object stored before the rule
    /// existed stays updatable.
    #[test]
    fn an_update_that_keeps_the_list_is_spared() {
        let bad = config(json!([
            { "name": "same", "expression": "true" },
            { "name": "same", "expression": "false" }
        ]));
        let errs = validate_validating_webhook_configuration_update(&bad, &bad);
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// Changing any entry re-validates the whole list.
    #[test]
    fn an_update_that_changes_the_list_is_validated() {
        let old = config(json!([
            { "name": "same", "expression": "true" },
            { "name": "same", "expression": "false" }
        ]));
        let new = config(json!([
            { "name": "same", "expression": "true" },
            { "name": "same", "expression": "changed" }
        ]));
        let errs = validate_validating_webhook_configuration_update(&new, &old);
        assert!(
            errs.iter()
                .any(|e| e.field == "webhooks[0].matchConditions[1].name"),
            "{errs:?}"
        );
    }

    /// A list that grows is a change even when every kept entry matches.
    #[test]
    fn a_longer_list_is_validated() {
        let old = config(json!([{ "name": "one", "expression": "true" }]));
        let new = config(json!([
            { "name": "one", "expression": "true" },
            { "name": "one", "expression": "false" }
        ]));
        let errs = validate_validating_webhook_configuration_update(&new, &old);
        assert!(
            errs.iter()
                .any(|e| e.field == "webhooks[0].matchConditions[1].name"),
            "{errs:?}"
        );
    }
}
