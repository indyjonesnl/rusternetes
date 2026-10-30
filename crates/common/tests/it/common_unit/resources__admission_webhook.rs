//! Unit tests moved from `src/resources/admission_webhook.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::WebhookClientConfig;
    pub use rusternetes_common::types::ObjectMeta;
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::admission_webhook::*;

    /// Go-parity decode: a webhook `rule` that omits `apiGroups` (and other
    /// list fields) must deserialize. Upstream `admissionregistration.Rule`
    /// has `apiGroups`/`apiVersions`/`resources` as `omitempty`, and the
    /// sig-api-machinery AdmissionWebhook conformance e2e sends rules without
    /// `apiGroups` — our required `Vec` field made MutatingWebhookConfiguration
    /// create 422 ("webhooks[0].rules[0]: missing field `apiGroups`"), blocking
    /// every core-resource webhook spec at registration.
    #[test]
    fn rule_with_operations_decodes_without_apigroups() {
        let v = serde_json::json!({
            "operations": ["CREATE"],
            "apiVersions": ["v1"],
            "resources": ["configmaps"]
            // apiGroups intentionally omitted
        });
        let r: RuleWithOperations =
            serde_json::from_value(v).expect("rule without apiGroups must decode (Go-parity)");
        assert!(r.rule.api_groups.is_empty());
        assert_eq!(r.rule.resources, vec!["configmaps".to_string()]);
    }

    /// And a full webhook config whose rule omits apiGroups must decode too.
    #[test]
    fn webhook_config_with_rule_missing_apigroups_decodes() {
        let v = serde_json::json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "MutatingWebhookConfiguration",
            "metadata": {"name": "webhook-x"},
            "webhooks": [{
                "name": "m.example.com",
                "clientConfig": {"service": {"namespace": "ns", "name": "svc", "path": "/mutate"}},
                "rules": [{"operations": ["CREATE"], "apiVersions": ["v1"], "resources": ["configmaps"]}],
                "admissionReviewVersions": ["v1"],
                "sideEffects": "None"
            }]
        });
        let cfg: MutatingWebhookConfiguration = serde_json::from_value(v)
            .expect("webhook config with rule missing apiGroups must decode");
        assert_eq!(cfg.metadata.name, "webhook-x");
    }

    /// Reproduces the conformance webhook failure: a ValidatingWebhookConfiguration's
    /// rule fields (apiGroups/resources, which are `#[serde(flatten)]`-ed from Rule)
    /// must survive the api-server storage round-trip (struct -> serde_json::Value ->
    /// struct). If they don't, every webhook rule matches nothing, the webhook is
    /// never invoked, and the e2e readiness markers are never denied.
    #[test]
    fn rule_fields_survive_value_round_trip() {
        let incoming = serde_json::json!({
            "apiVersion": "admissionregistration.k8s.io/v1",
            "kind": "ValidatingWebhookConfiguration",
            "metadata": {"name": "deny-cms"},
            "webhooks": [{
                "name": "deny.example.com",
                "clientConfig": {"service": {"namespace": "ns", "name": "svc", "path": "/c", "port": 8443}},
                "rules": [{"operations": ["CREATE"], "apiGroups": [""], "apiVersions": ["v1"], "resources": ["configmaps"]}],
                "admissionReviewVersions": ["v1"],
                "sideEffects": "None"
            }]
        });

        // 1. Deserialize the incoming request (flatten on the way in).
        let cfg: ValidatingWebhookConfiguration =
            serde_json::from_value(incoming).expect("decode incoming");
        let rule0 = &cfg.webhooks.as_ref().unwrap()[0].rules[0];
        assert_eq!(
            rule0.rule.resources,
            vec!["configmaps".to_string()],
            "deser dropped resources"
        );
        assert_eq!(
            rule0.rule.api_groups,
            vec!["".to_string()],
            "deser dropped apiGroups"
        );
        assert_eq!(
            cfg.webhooks.as_ref().unwrap()[0]
                .client_config
                .service
                .as_ref()
                .unwrap()
                .port,
            Some(8443),
            "deser dropped/garbled service port"
        );

        // 2. Storage round-trip: struct -> Value -> struct (what the api-server does).
        let stored = serde_json::to_value(&cfg).expect("to_value");
        let back: ValidatingWebhookConfiguration =
            serde_json::from_value(stored.clone()).expect("from_value");
        let rb = &back.webhooks.as_ref().unwrap()[0].rules[0];
        assert_eq!(
            rb.rule.resources,
            vec!["configmaps".to_string()],
            "storage round-trip dropped rule.resources; serialized form was: {}",
            serde_json::to_string(&stored.pointer("/webhooks/0/rules/0").unwrap()).unwrap()
        );
        assert_eq!(
            rb.rule.api_groups,
            vec!["".to_string()],
            "round-trip dropped apiGroups"
        );
    }

    #[test]
    fn test_validating_webhook_config_creation() {
        let config = ValidatingWebhookConfiguration::new("test-webhook");
        assert_eq!(config.metadata.name, "test-webhook");
        assert_eq!(config.api_version, "admissionregistration.k8s.io/v1");
        assert_eq!(config.kind, "ValidatingWebhookConfiguration");
    }

    #[test]
    fn test_mutating_webhook_config_creation() {
        let config = MutatingWebhookConfiguration::new("test-webhook");
        assert_eq!(config.metadata.name, "test-webhook");
        assert_eq!(config.api_version, "admissionregistration.k8s.io/v1");
        assert_eq!(config.kind, "MutatingWebhookConfiguration");
    }

    #[test]
    fn test_operation_type_serialization() {
        let create = serde_json::to_string(&OperationType::Create).unwrap();
        assert_eq!(create, r#""CREATE""#);

        let all = serde_json::to_string(&OperationType::All).unwrap();
        assert_eq!(all, r#""*""#);
    }

    #[test]
    fn test_failure_policy() {
        assert_eq!(
            serde_json::to_string(&FailurePolicy::Ignore).unwrap(),
            r#""Ignore""#
        );
        assert_eq!(
            serde_json::to_string(&FailurePolicy::Fail).unwrap(),
            r#""Fail""#
        );
    }

    #[test]
    fn test_side_effect_class() {
        assert_eq!(
            serde_json::to_string(&SideEffectClass::None).unwrap(),
            r#""None""#
        );
        assert_eq!(
            serde_json::to_string(&SideEffectClass::NoneOnDryRun).unwrap(),
            r#""NoneOnDryRun""#
        );
    }
}
