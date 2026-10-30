//! Unit tests moved from `src/authorization_config.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::authorization_match_conditions::{
        CelMatcher, WebhookMatchCondition,
    };
    pub use rusternetes_api_server::authorizer::{AUTHORIZATION_MODE_CHOICES, MODE_WEBHOOK};
    pub use rusternetes_common::go_duration::{format_go_duration, parse_go_duration};
    pub use rusternetes_common::validation::field::{
        BadValue, Error as FieldError, ErrorList, Path,
    };
    pub use rusternetes_common::validation::metav1::is_dns1123_subdomain;
    pub use std::collections::BTreeSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::authorization_config::*;
    use rusternetes_common::validation::field::{ErrorType, Path};

    const SEC: i64 = 1_000_000_000;

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    // ---- load_test.go TestLoadFromData ----------------------------------

    fn default_webhook() -> WebhookConfiguration {
        WebhookConfiguration {
            authorized_ttl: 5 * 60 * SEC,
            cache_authorized_requests: true,
            unauthorized_ttl: 30 * SEC,
            cache_unauthorized_requests: true,
            ..Default::default()
        }
    }

    fn bare_webhook_type() -> AuthorizationConfiguration {
        AuthorizationConfiguration {
            authorizers: vec![AuthorizerConfiguration {
                type_: "Webhook".into(),
                ..Default::default()
            }],
        }
    }

    fn defaulted_webhook() -> AuthorizationConfiguration {
        AuthorizationConfiguration {
            authorizers: vec![AuthorizerConfiguration {
                type_: "Webhook".into(),
                name: "default".into(),
                webhook: Some(default_webhook()),
            }],
        }
    }

    #[test]
    fn load_nil_and_empty_give_the_default_config() {
        assert_eq!(load_from_data(b"").unwrap(), Default::default());
    }

    #[test]
    fn load_every_version_json_defaults_and_yaml() {
        for v in ["v1alpha1", "v1beta1", "v1"] {
            let json = format!(
                r#"{{"apiVersion":"apiserver.config.k8s.io/{v}","kind":"AuthorizationConfiguration","authorizers":[{{"type":"Webhook"}}]}}"#
            );
            assert_eq!(
                load_from_data(json.as_bytes()).unwrap(),
                bare_webhook_type(),
                "{v} json"
            );
            let defaults = format!(
                r#"{{"apiVersion":"apiserver.config.k8s.io/{v}","kind":"AuthorizationConfiguration","authorizers":[{{"type":"Webhook","name":"default","webhook":{{}}}}]}}"#
            );
            assert_eq!(
                load_from_data(defaults.as_bytes()).unwrap(),
                defaulted_webhook(),
                "{v} defaults"
            );
            let yaml = format!(
                "\napiVersion: apiserver.config.k8s.io/{v}\nkind: AuthorizationConfiguration\nauthorizers:\n- type: Webhook\n"
            );
            assert_eq!(
                load_from_data(yaml.as_bytes()).unwrap(),
                bare_webhook_type(),
                "{v} yaml"
            );
        }
    }

    #[test]
    fn load_decodes_every_field() {
        let data = br#"
apiVersion: apiserver.config.k8s.io/v1
kind: AuthorizationConfiguration
authorizers:
- type: Webhook
  name: w1
  webhook:
    authorizedTTL: 1m
    cacheAuthorizedRequests: false
    unauthorizedTTL: 2s
    cacheUnauthorizedRequests: false
    timeout: 3s
    subjectAccessReviewVersion: v1
    matchConditionSubjectAccessReviewVersion: v1
    failurePolicy: Deny
    connectionInfo:
      type: KubeConfigFile
      kubeConfigFile: /etc/kubeconfig
    matchConditions:
    - expression: "request.user == 'admin'"
- type: RBAC
  name: rbac
"#;
        let c = load_from_data(data).unwrap();
        assert_eq!(c.authorizers.len(), 2);
        let w = c.authorizers[0].webhook.as_ref().unwrap();
        assert_eq!(w.authorized_ttl, 60 * SEC);
        assert!(!w.cache_authorized_requests);
        assert_eq!(w.unauthorized_ttl, 2 * SEC);
        assert!(!w.cache_unauthorized_requests);
        assert_eq!(w.timeout, 3 * SEC);
        assert_eq!(w.subject_access_review_version, "v1");
        assert_eq!(w.match_condition_subject_access_review_version, "v1");
        assert_eq!(w.failure_policy, "Deny");
        assert_eq!(w.connection_info.type_, "KubeConfigFile");
        assert_eq!(
            w.connection_info.kube_config_file.as_deref(),
            Some("/etc/kubeconfig")
        );
        assert_eq!(w.match_conditions[0].expression, "request.user == 'admin'");
        assert_eq!(c.authorizers[1].type_, "RBAC");
        assert!(c.authorizers[1].webhook.is_none());
    }

    #[test]
    fn load_errors() {
        let cases: [(&str, &str, &str); 6] = [
            (
                "missing apiVersion",
                r#"{"kind":"AuthorizationConfiguration"}"#,
                "'apiVersion' is missing",
            ),
            (
                "missing kind",
                r#"{"apiVersion":"apiserver.config.k8s.io/v1alpha1"}"#,
                "'Kind' is missing",
            ),
            (
                "unknown group",
                r#"{"apiVersion":"apps/v1alpha1","kind":"AuthorizationConfiguration"}"#,
                "apps/v1alpha1",
            ),
            (
                "unknown version",
                r#"{"apiVersion":"apiserver.config.k8s.io/v99","kind":"AuthorizationConfiguration"}"#,
                "apiserver.config.k8s.io/v99",
            ),
            (
                "unknown kind",
                r#"{"apiVersion":"apiserver.config.k8s.io/v1alpha1","kind":"SomeConfiguration"}"#,
                "SomeConfiguration",
            ),
            (
                "unknown field",
                r#"{"apiVersion":"apiserver.config.k8s.io/v1alpha1","kind":"AuthorizationConfiguration","authorzers":[{"type":"Webhook"}]}"#,
                r#"unknown field "authorzers""#,
            ),
        ];
        for (name, data, want) in cases {
            let err = load_from_data(data.as_bytes()).expect_err(name);
            assert!(err.contains(want), "{name}: {err}");
        }
    }

    #[test]
    fn load_exact_error_strings() {
        assert_eq!(
            load_from_data(br#"{"kind":"AuthorizationConfiguration"}"#).unwrap_err(),
            r#"Object 'apiVersion' is missing in '{"kind":"AuthorizationConfiguration"}'"#
        );
        assert_eq!(
            load_from_data(br#"{"apiVersion":"apiserver.config.k8s.io/v99","kind":"AuthorizationConfiguration"}"#)
                .unwrap_err(),
            r#"no kind "AuthorizationConfiguration" is registered for version "apiserver.config.k8s.io/v99" in scheme "k8s.io/apiserver/pkg/apis/apiserver/load/load.go:32""#
        );
        assert_eq!(
            load_from_data(br#"{"apiVersion":"apiserver.config.k8s.io/v1","kind":"AuthorizationConfiguration","authorizers":[{"type":"Webhook","webhok":{}}],"x":1}"#)
                .unwrap_err(),
            r#"strict decoding error: unknown field "authorizers[0].webhok", unknown field "x""#
        );
    }

    // ---- validation_test.go TestValidateAuthorizationConfiguration -------

    fn good_webhook() -> WebhookConfiguration {
        WebhookConfiguration {
            timeout: 5 * SEC,
            authorized_ttl: 5 * 60 * SEC,
            unauthorized_ttl: 30 * SEC,
            failure_policy: "NoOpinion".into(),
            subject_access_review_version: "v1".into(),
            match_condition_subject_access_review_version: "v1".into(),
            connection_info: WebhookConnectionInfo {
                type_: "InClusterConfig".into(),
                kube_config_file: None,
            },
            ..Default::default()
        }
    }

    fn one(
        type_: &str,
        name: &str,
        webhook: Option<WebhookConfiguration>,
    ) -> AuthorizationConfiguration {
        AuthorizationConfiguration {
            authorizers: vec![AuthorizerConfiguration {
                type_: type_.into(),
                name: name.into(),
                webhook,
            }],
        }
    }

    fn validate(
        c: &AuthorizationConfiguration,
        known: &[&str],
        repeatable: &[&str],
    ) -> Vec<String> {
        validate_authorization_configuration(&Path::default(), c, &set(known), &set(repeatable))
            .iter()
            .map(|e| e.to_string())
            .collect()
    }

    fn webhook_errs(w: WebhookConfiguration) -> Vec<String> {
        validate(
            &one("Webhook", "default", Some(w)),
            &["Webhook"],
            &["Webhook"],
        )
    }

    #[test]
    fn at_least_one_authorizer() {
        assert_eq!(
            validate(&Default::default(), &[], &[]),
            vec!["authorizers: Required value: at least one authorization mode must be defined"]
        );
    }

    #[test]
    fn type_and_name_are_required() {
        assert_eq!(
            validate(&one("", "", None), &["Webhook"], &["Webhook"]),
            vec!["authorizers[0].type: Required value"]
        );
        assert_eq!(
            validate(&one("Foo", "", None), &["Foo"], &["Webhook"]),
            vec!["authorizers[0].name: Required value"]
        );
    }

    #[test]
    fn names_unique_and_dns1123() {
        let dup = AuthorizationConfiguration {
            authorizers: vec![
                AuthorizerConfiguration {
                    type_: "Foo".into(),
                    name: "foo".into(),
                    webhook: None,
                },
                AuthorizerConfiguration {
                    type_: "Bar".into(),
                    name: "foo".into(),
                    webhook: None,
                },
            ],
        };
        assert_eq!(
            validate(&dup, &["Foo", "Bar"], &["Webhook"]),
            vec![r#"authorizers[1].name: Duplicate value: "foo""#]
        );
        assert!(validate(&one("Foo", "myauthorizer", None), &["Foo"], &["Webhook"]).is_empty());
        assert!(validate(
            &one("Foo", "foo.example.domain", None),
            &["Foo"],
            &["Webhook"]
        )
        .is_empty());
        let bad = validate(
            &one("Foo", "FOO.example.domain", None),
            &["Foo"],
            &["Webhook"],
        );
        assert_eq!(bad.len(), 1);
        assert!(
            bad[0].starts_with(
                r#"authorizers[0].name: Invalid value: "FOO.example.domain": authorizer name is invalid: a lowercase RFC 1123 subdomain must consist of"#
            ),
            "{bad:?}"
        );
    }

    #[test]
    fn webhook_configurations_valid() {
        assert!(webhook_errs(good_webhook()).is_empty());
        let mut w = good_webhook();
        w.match_conditions = vec![
            WebhookMatchCondition {
                expression:
                    "has(request.resourceAttributes) && request.resourceAttributes.namespace == 'kube-system'"
                        .into(),
            },
            WebhookMatchCondition {
                expression: "request.user == 'admin'".into(),
            },
        ];
        assert!(webhook_errs(w).is_empty());
        let two = AuthorizationConfiguration {
            authorizers: vec![
                AuthorizerConfiguration {
                    type_: "Webhook".into(),
                    name: "default".into(),
                    webhook: Some(good_webhook()),
                },
                AuthorizerConfiguration {
                    type_: "Webhook".into(),
                    name: "second-webhook".into(),
                    webhook: Some(good_webhook()),
                },
            ],
        };
        assert!(validate(&two, &["Webhook"], &["Webhook"]).is_empty());
    }

    #[test]
    fn unknown_and_duplicate_types() {
        assert_eq!(
            validate(&one("Foo", "", None), &["Webhook"], &["Webhook"]),
            vec![r#"authorizers[0].type: Unsupported value: "Foo": supported values: "Webhook""#]
        );
        let twice = AuthorizationConfiguration {
            authorizers: vec![
                AuthorizerConfiguration {
                    type_: "Foo".into(),
                    name: "foo-1".into(),
                    webhook: None,
                },
                AuthorizerConfiguration {
                    type_: "Foo".into(),
                    name: "foo-2".into(),
                    webhook: None,
                },
            ],
        };
        assert_eq!(
            validate(&twice, &["Foo"], &["Webhook"]),
            vec![r#"authorizers[1].type: Duplicate value: "Foo""#]
        );
    }

    #[test]
    fn webhook_field_presence_by_type() {
        assert_eq!(
            validate(&one("Webhook", "default", None), &["Webhook"], &["Webhook"]),
            vec!["authorizers[0].webhook: Required value: required when type=Webhook"]
        );
        assert_eq!(
            validate(
                &one("Foo", "foo", Some(WebhookConfiguration::default())),
                &["Foo"],
                &["Webhook"]
            ),
            vec![
                r#"authorizers[0].webhook: Invalid value: "non-null": may only be specified when type=Webhook"#
            ]
        );
    }

    #[test]
    fn webhook_timeout() {
        let mut w = good_webhook();
        w.timeout = 0;
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].timeout: Required value"]
        );
        for (d, shown) in [(-30 * SEC, "-30s"), (60 * SEC, "1m0s")] {
            let mut w = good_webhook();
            w.timeout = d;
            assert_eq!(
                webhook_errs(w),
                vec![format!(
                    r#"authorizers[0].timeout: Invalid value: "{shown}": must be > 0s and <= 30s"#
                )]
            );
        }
    }

    #[test]
    fn webhook_ttls() {
        let mut w = good_webhook();
        w.authorized_ttl = 0;
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].authorizedTTL: Required value"]
        );
        let mut w = good_webhook();
        w.authorized_ttl = -30 * SEC;
        assert_eq!(
            webhook_errs(w),
            vec![r#"authorizers[0].authorizedTTL: Invalid value: "-30s": must be > 0s"#]
        );
        let mut w = good_webhook();
        w.unauthorized_ttl = 0;
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].unauthorizedTTL: Required value"]
        );
        let mut w = good_webhook();
        w.unauthorized_ttl = -30 * SEC;
        assert_eq!(
            webhook_errs(w),
            vec![r#"authorizers[0].unauthorizedTTL: Invalid value: "-30s": must be > 0s"#]
        );
    }

    #[test]
    fn webhook_sar_versions() {
        let mut w = good_webhook();
        w.subject_access_review_version = String::new();
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].subjectAccessReviewVersion: Required value"]
        );
        let mut w = good_webhook();
        w.subject_access_review_version = "v2beta1".into();
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].subjectAccessReviewVersion: Unsupported value: "v2beta1": supported values: "v1", "v1beta1""#
            ]
        );
        // v1beta1 is a valid subjectAccessReviewVersion.
        let mut w = good_webhook();
        w.subject_access_review_version = "v1beta1".into();
        assert!(webhook_errs(w).is_empty());

        let mut w = good_webhook();
        w.match_condition_subject_access_review_version = String::new();
        w.match_conditions = vec![WebhookMatchCondition {
            expression: "true".into(),
        }];
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].matchConditionSubjectAccessReviewVersion: Required value: required if match conditions are specified"]
        );
        // Empty is fine without match conditions.
        let mut w = good_webhook();
        w.match_condition_subject_access_review_version = String::new();
        assert!(webhook_errs(w).is_empty());
        let mut w = good_webhook();
        w.match_condition_subject_access_review_version = "v1beta1".into();
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].matchConditionSubjectAccessReviewVersion: Unsupported value: "v1beta1": supported values: "v1""#
            ]
        );
    }

    #[test]
    fn webhook_failure_policy() {
        let mut w = good_webhook();
        w.failure_policy = String::new();
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].failurePolicy: Required value"]
        );
        let mut w = good_webhook();
        w.failure_policy = "AlwaysAllow".into();
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].failurePolicy: Unsupported value: "AlwaysAllow": supported values: "NoOpinion", "Deny""#
            ]
        );
        let mut w = good_webhook();
        w.failure_policy = "Deny".into();
        assert!(webhook_errs(w).is_empty());
    }

    #[test]
    fn webhook_connection_info() {
        let mut w = good_webhook();
        w.connection_info = WebhookConnectionInfo::default();
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].connectionInfo.type: Required value"]
        );
        // validation.go:763 passes the whole struct as the bad value.
        let mut w = good_webhook();
        w.connection_info.type_ = "ExternalClusterConfig".into();
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].connectionInfo.type: Unsupported value: {"Type":"ExternalClusterConfig","KubeConfigFile":null}: supported values: "InClusterConfig", "KubeConfigFile""#
            ]
        );
        let mut w = good_webhook();
        w.connection_info.kube_config_file = Some(String::new());
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].connectionInfo.kubeConfigFile: Invalid value: "": can only be set when type=KubeConfigFile"#
            ]
        );
        let mut w = good_webhook();
        w.connection_info.type_ = "KubeConfigFile".into();
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].connectionInfo.kubeConfigFile: Required value"]
        );
        let mut w = good_webhook();
        w.connection_info.type_ = "KubeConfigFile".into();
        w.connection_info.kube_config_file = Some("../some/relative/path/kubeconfig".into());
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].connectionInfo.kubeConfigFile: Invalid value: "../some/relative/path/kubeconfig": must be an absolute path"#
            ]
        );
        let f = tempfile_path("kc");
        std::fs::write(&f, "x").unwrap();
        let mut w = good_webhook();
        w.connection_info.type_ = "KubeConfigFile".into();
        w.connection_info.kube_config_file = Some(f.clone());
        assert!(webhook_errs(w).is_empty());
        let mut w = good_webhook();
        w.connection_info.type_ = "KubeConfigFile".into();
        w.connection_info.kube_config_file = Some("/nonexistent/kc".into());
        let e = webhook_errs(w);
        assert!(
            e[0].starts_with(
                r#"authorizers[0].connectionInfo.kubeConfigFile: Invalid value: "/nonexistent/kc": error loading file: "#
            ),
            "{e:?}"
        );
        let mut w = good_webhook();
        w.connection_info.type_ = "KubeConfigFile".into();
        w.connection_info.kube_config_file = Some("/tmp".into());
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].connectionInfo.kubeConfigFile: Invalid value: "/tmp": must be a regular file"#
            ]
        );
        let _ = std::fs::remove_file(f);
    }

    fn tempfile_path(tag: &str) -> String {
        std::env::temp_dir()
            .join(format!("authzcfg-{tag}-{}", std::process::id()))
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn webhook_match_condition_paths_carry_the_authorizer_index() {
        let mut w = good_webhook();
        w.match_conditions = vec![WebhookMatchCondition {
            expression: "  ".into(),
        }];
        assert_eq!(
            webhook_errs(w),
            vec!["authorizers[0].matchConditions[0].expression: Required value"]
        );
        let mut w = good_webhook();
        let c = WebhookMatchCondition {
            expression: "request.user == 'admin'".into(),
        };
        w.match_conditions = vec![c.clone(), c];
        assert_eq!(
            webhook_errs(w),
            vec![
                r#"authorizers[0].matchConditions[1].expression: Duplicate value: "request.user == 'admin'""#
            ]
        );
    }

    #[test]
    fn error_types_are_field_error_types() {
        let errs = validate_authorization_configuration(
            &Path::default(),
            &Default::default(),
            &set(&[]),
            &set(&[]),
        );
        assert_eq!(errs[0].error_type, ErrorType::Required);
    }

    // ---- LoadAndValidateData (authorizer/config.go) ----------------------

    const HDR: &str = "apiVersion: apiserver.config.k8s.io/v1\nkind: AuthorizationConfiguration\n";

    #[test]
    fn load_and_validate_wraps_load_errors() {
        let e = load_and_validate_data(b"{\"kind\":\"AuthorizationConfiguration\"}", &set(&[]))
            .unwrap_err();
        assert!(
            e.starts_with("failed to load AuthorizationConfiguration from file: Object 'apiVersion' is missing"),
            "{e}"
        );
    }

    #[test]
    fn load_and_validate_empty_requires_an_authorizer() {
        assert_eq!(
            load_and_validate_data(b"", &set(&[])).unwrap_err(),
            "authorizers: Required value: at least one authorization mode must be defined"
        );
    }

    #[test]
    fn load_and_validate_builtin_authorizer_names_must_be_lowercased_type() {
        let ok =
            format!("{HDR}authorizers:\n- type: Node\n  name: node\n- type: RBAC\n  name: rbac\n");
        let c = load_and_validate_data(ok.as_bytes(), &set(&[])).unwrap();
        assert_eq!(c.authorizers.len(), 2);

        let bad = format!("{HDR}authorizers:\n- type: RBAC\n  name: foo\n");
        assert_eq!(
            load_and_validate_data(bad.as_bytes(), &set(&[])).unwrap_err(),
            "expected name rbac for authorizer RBAC instead of foo"
        );
        // Webhook names are free.
        let wh = format!(
            "{HDR}authorizers:\n- type: Webhook\n  name: whatever\n  webhook:\n    timeout: 5s\n    subjectAccessReviewVersion: v1\n    failurePolicy: Deny\n    connectionInfo:\n      type: InClusterConfig\n"
        );
        assert!(load_and_validate_data(wh.as_bytes(), &set(&[])).is_ok());
    }

    #[test]
    fn load_and_validate_unknown_type_and_missing_required_types() {
        let unknown = format!("{HDR}authorizers:\n- type: Bogus\n  name: bogus\n");
        let e = load_and_validate_data(unknown.as_bytes(), &set(&[])).unwrap_err();
        assert_eq!(
            e,
            r#"authorizers[0].type: Unsupported value: "Bogus": supported values: "ABAC", "AlwaysAllow", "AlwaysDeny", "Node", "RBAC", "Webhook""#
        );
        let only_rbac = format!("{HDR}authorizers:\n- type: RBAC\n  name: rbac\n");
        assert_eq!(
            load_and_validate_data(only_rbac.as_bytes(), &set(&["Node", "RBAC"])).unwrap_err(),
            "missing required types: [Node]"
        );
    }

    #[test]
    fn load_and_validate_aggregates_multiple_errors() {
        let two = format!("{HDR}authorizers:\n- type: RBAC\n  name: a\n- type: Node\n  name: b\n");
        assert_eq!(
            load_and_validate_data(two.as_bytes(), &set(&[])).unwrap_err(),
            "[expected name rbac for authorizer RBAC instead of a, expected name node for authorizer Node instead of b]"
        );
    }
}
