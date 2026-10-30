//! Unit tests moved from `src/authorizer.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::abac::PolicyList;
    pub use rusternetes_api_server::authorization_config::{
        name_for_authorizer_mode, AuthorizationConfiguration, AuthorizerConfiguration,
    };
    pub use rusternetes_api_server::authorization_reload::ChainBuilder;
    pub use rusternetes_common::authz::{
        node_graph::NodeGraph, superuser_then, AlwaysAllowAuthorizer, AlwaysDenyAuthorizer,
        Authorizer, AuthzStorage, NodeAuthorizer, NodeAuthorizerFeatures, RBACAuthorizer,
    };
    pub use rusternetes_storage::Storage;
    pub use std::path::Path;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::authorizer::*;
    use rusternetes_common::auth::UserInfo;
    use rusternetes_common::authz::{Decision, RequestAttributes};
    use rusternetes_storage::MemoryStorage;

    fn user(name: &str, groups: &[&str]) -> UserInfo {
        UserInfo {
            username: name.to_string(),
            uid: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            extra: Default::default(),
        }
    }

    async fn allowed(a: &Arc<dyn Authorizer>, attrs: RequestAttributes) -> bool {
        matches!(a.authorize(&attrs).await.unwrap(), Decision::Allow)
    }

    fn modes(m: &[&str]) -> Vec<String> {
        m.iter().map(|s| s.to_string()).collect()
    }

    fn pods(u: UserInfo) -> RequestAttributes {
        RequestAttributes::new(u, "get", "pods").with_namespace("default")
    }

    fn build(m: &[&str]) -> anyhow::Result<Arc<dyn Authorizer>> {
        build_authorizer(Arc::new(MemoryStorage::new()), false, &modes(m), "")
    }

    /// A kubelet reads its own Node through the Node authorizer even with an
    /// empty RBAC store (#1664; the all-in-one binary runs one in-process).
    #[tokio::test]
    async fn kubelet_is_authorized_by_node_authorizer_with_empty_rbac() {
        let a = build(&["Node", "RBAC"]).unwrap();
        let attrs =
            RequestAttributes::new(user("system:node:n1", &["system:nodes"]), "get", "nodes")
                .with_name("n1");
        assert!(allowed(&a, attrs).await);
    }

    #[tokio::test]
    async fn masters_allowed_and_unknown_user_denied() {
        let a = build(&["Node", "RBAC"]).unwrap();
        assert!(allowed(&a, pods(user("admin", &["system:masters"]))).await);
        assert!(!allowed(&a, pods(user("alice", &[]))).await);
    }

    #[tokio::test]
    async fn skip_auth_allows_everything() {
        let a = build_authorizer(Arc::new(MemoryStorage::new()), true, &[], "").unwrap();
        let attrs = RequestAttributes::new(user("alice", &[]), "delete", "nodes");
        assert!(allowed(&a, attrs).await);
    }

    // TestAuthzValidate (pkg/kubeapiserver/options/authorization_test.go:34-).
    fn errs(m: &[&str]) -> String {
        validate_authorization_modes(&modes(m), "").join("; ")
    }

    #[test]
    fn validate_unknown_mode() {
        assert!(errs(&["DoesNotExist"]).contains("is not a valid mode"));
    }

    #[test]
    fn validate_at_least_one_mode() {
        assert!(errs(&[]).contains("at least one authorization-mode must be passed"));
    }

    #[test]
    fn validate_duplicate_mode() {
        assert!(errs(&["AlwaysAllow", "AlwaysAllow"]).contains("has mode specified more than once"));
    }

    #[test]
    fn validate_allow_and_deny_ok() {
        assert!(
            validate_authorization_modes(&modes(&["AlwaysAllow", "AlwaysDeny"]), "").is_empty()
        );
        assert!(validate_authorization_modes(&modes(&["Node", "RBAC"]), "").is_empty());
    }

    #[test]
    fn complete_defaults_to_always_allow_like_upstream() {
        // authorization.go:92-94: no modes (and no --authorization-config) => AlwaysAllow.
        assert_eq!(
            complete_authorization_modes("", &[]),
            modes(&["AlwaysAllow"])
        );
        assert_eq!(
            complete_authorization_modes("", &modes(&["RBAC"])),
            modes(&["RBAC"])
        );
    }

    /// With no `--authorization-mode` the built chain allows everything
    /// (upstream default AlwaysAllow); Node,RBAC must be passed explicitly.
    #[tokio::test]
    async fn no_modes_builds_always_allow_chain() {
        let a = build(&[]).unwrap();
        assert!(allowed(&a, pods(user("nobody", &[]))).await);
    }

    #[test]
    fn valid_modes_are_the_upstream_choices() {
        for m in [
            "AlwaysAllow",
            "AlwaysDeny",
            "ABAC",
            "Webhook",
            "RBAC",
            "Node",
        ] {
            assert!(is_valid_authorization_mode(m), "{m}");
        }
        assert!(!is_valid_authorization_mode("rbac"));
    }

    #[tokio::test]
    async fn always_allow_mode_allows_everyone() {
        let a = build(&["AlwaysAllow"]).unwrap();
        assert!(allowed(&a, pods(user("alice", &[]))).await);
    }

    #[tokio::test]
    async fn always_deny_mode_denies_all_but_masters() {
        let a = build(&["AlwaysDeny"]).unwrap();
        assert!(!allowed(&a, pods(user("alice", &[]))).await);
        // reload.go:97-99: the privileged-groups authorizer is always first.
        assert!(allowed(&a, pods(user("admin", &["system:masters"]))).await);
    }

    /// Order matters: AlwaysDeny gives no opinion, so a later AlwaysAllow wins.
    #[tokio::test]
    async fn modes_are_consulted_in_order() {
        let a = build(&["AlwaysDeny", "AlwaysAllow"]).unwrap();
        assert!(allowed(&a, pods(user("alice", &[]))).await);
    }

    /// RBAC alone has no Node authorizer, so a kubelet is not let in.
    #[tokio::test]
    async fn rbac_only_has_no_node_authorizer() {
        let a = build(&["RBAC"]).unwrap();
        let attrs =
            RequestAttributes::new(user("system:node:n1", &["system:nodes"]), "get", "nodes")
                .with_name("n1");
        assert!(!allowed(&a, attrs).await);
    }

    #[test]
    fn invalid_modes_fail_to_build() {
        assert!(build(&["Bogus"]).is_err());
        assert!(build(&["RBAC", "RBAC"]).is_err());
    }

    // authorization.go:139-141, :147-149 (verbatim messages).
    #[test]
    fn validate_abac_requires_policy_file() {
        assert!(errs(&["ABAC"])
            .contains("authorization-mode ABAC's authorization policy file not passed"));
        assert!(validate_authorization_modes(&modes(&["ABAC"]), "/p").is_empty());
    }

    #[test]
    fn validate_policy_file_requires_abac() {
        let e = validate_authorization_modes(&modes(&["RBAC"]), "/p").join("; ");
        assert!(e.contains("cannot specify --authorization-policy-file without mode ABAC"));
    }

    fn policy_file(contents: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static N: AtomicUsize = AtomicUsize::new(0);
        let p = std::env::temp_dir().join(format!(
            "abac_authorizer_{}_{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::write(&p, contents).unwrap();
        p
    }

    #[tokio::test]
    async fn abac_mode_authorizes_from_policy_file() {
        let p = policy_file(
            "{\"apiVersion\":\"abac.authorization.kubernetes.io/v1beta1\",\"kind\":\"Policy\",\"spec\":{\"user\":\"alice\",\"namespace\":\"*\",\"resource\":\"pods\",\"apiGroup\":\"*\",\"readonly\":true}}\n",
        );
        let a = build_authorizer(
            Arc::new(MemoryStorage::new()),
            false,
            &modes(&["ABAC"]),
            p.to_str().unwrap(),
        )
        .unwrap();
        let _ = std::fs::remove_file(&p);
        assert!(allowed(&a, pods(user("alice", &[]))).await);
        assert!(!allowed(&a, pods(user("bob", &[]))).await);
        let write =
            RequestAttributes::new(user("alice", &[]), "create", "pods").with_namespace("default");
        assert!(!allowed(&a, write).await);
    }

    #[test]
    fn abac_mode_fails_on_missing_or_bad_policy_file() {
        let b =
            |f: &str| build_authorizer(Arc::new(MemoryStorage::new()), false, &modes(&["ABAC"]), f);
        assert!(b("/nonexistent/policy.jsonl").is_err());
        let p = policy_file("not json\n");
        let e = b(p.to_str().unwrap()).err().unwrap().to_string();
        let _ = std::fs::remove_file(&p);
        assert!(e.contains("line 1: not json"), "{e}");
        // Validate runs first: ABAC without a file never reaches the loader.
        assert!(b("")
            .err()
            .unwrap()
            .to_string()
            .contains("policy file not passed"));
    }

    // TestAuthzValidate: Webhook without its config file is refused at build.
    #[test]
    fn webhook_mode_requires_its_config_file() {
        let err = build(&["Webhook"]).err().unwrap().to_string();
        assert!(
            err.contains("authorization-mode Webhook's authorization config file not passed"),
            "{err}"
        );
    }

    // ---- --authorization-config (#3294) --------------------------------

    const CFG_HDR: &str =
        "apiVersion: apiserver.config.k8s.io/v1\nkind: AuthorizationConfiguration\nauthorizers:\n";

    fn config_file(authorizers: &str) -> String {
        policy_file(&format!("{CFG_HDR}{authorizers}"))
            .to_string_lossy()
            .into_owned()
    }

    fn build_cfg(cfg: &str, modes_: &[&str], legacy: bool) -> anyhow::Result<Arc<dyn Authorizer>> {
        build_authorizer_with_config(
            Arc::new(MemoryStorage::new()),
            false,
            &modes(modes_),
            "",
            &Default::default(),
            cfg,
            legacy,
        )
    }

    // authorization.go:92: the AlwaysAllow default only applies without a file.
    #[test]
    fn complete_does_not_default_when_a_config_file_is_given() {
        assert_eq!(
            complete_authorization_modes("/some/file", &[]),
            Vec::<String>::new()
        );
        assert_eq!(
            complete_authorization_modes("", &[]),
            modes(&["AlwaysAllow"])
        );
    }

    // authorization.go:117-119.
    #[test]
    fn config_file_is_mutually_exclusive_with_the_legacy_flags() {
        let f = config_file("- type: RBAC\n  name: rbac\n");
        assert_eq!(
            load_authorization_config(&f, true).unwrap_err(),
            "--authorization-config can not be specified when --authorization-mode or --authorization-webhook-* flags are defined"
        );
        let e = build_cfg(&f, &["RBAC"], true).err().unwrap().to_string();
        assert!(
            e.contains("--authorization-config can not be specified"),
            "{e}"
        );
        let _ = std::fs::remove_file(&f);
    }

    // authorization.go:122-125: LoadAndValidateFile errors surface verbatim.
    #[test]
    fn config_file_errors_surface() {
        assert_eq!(
            load_authorization_config("/nonexistent/authz.yaml", false).unwrap_err(),
            "open /nonexistent/authz.yaml: no such file or directory"
        );
        let f = config_file("- type: Webhook\n  name: w\n");
        let e = load_authorization_config(&f, false).unwrap_err();
        assert!(
            e.starts_with("authorizers[0].webhook: Required value: required when type=Webhook"),
            "{e}"
        );
        let _ = std::fs::remove_file(&f);
    }

    #[test]
    fn config_file_loads_and_returns_its_data() {
        let f = config_file("- type: Node\n  name: node\n- type: RBAC\n  name: rbac\n");
        let (c, data) = load_authorization_config(&f, false).unwrap();
        assert_eq!(c.authorizers.len(), 2);
        assert!(data.starts_with("apiVersion: apiserver.config.k8s.io/v1"));
        let _ = std::fs::remove_file(&f);
    }

    // The configured authorizers replace the default: no --authorization-mode
    // and a config of AlwaysDeny must NOT fall back to AlwaysAllow.
    #[tokio::test]
    async fn config_file_drives_the_chain() {
        let f = config_file("- type: AlwaysDeny\n  name: alwaysdeny\n");
        let a = build_cfg(&f, &[], false).unwrap();
        assert!(!allowed(&a, pods(user("alice", &[]))).await);
        assert!(allowed(&a, pods(user("admin", &["system:masters"]))).await);
        let _ = std::fs::remove_file(&f);

        let f = config_file(
            "- type: AlwaysDeny\n  name: alwaysdeny\n- type: AlwaysAllow\n  name: alwaysallow\n",
        );
        let a = build_cfg(&f, &[], false).unwrap();
        assert!(allowed(&a, pods(user("alice", &[]))).await);
        let _ = std::fs::remove_file(&f);

        let f = config_file("- type: Node\n  name: node\n- type: RBAC\n  name: rbac\n");
        let a = build_cfg(&f, &[], false).unwrap();
        let kubelet =
            RequestAttributes::new(user("system:node:n1", &["system:nodes"]), "get", "nodes")
                .with_name("n1");
        assert!(allowed(&a, kubelet).await);
        assert!(!allowed(&a, pods(user("alice", &[]))).await);
        let _ = std::fs::remove_file(&f);
    }

    // config.go:97-99 / reload.go ABAC: the policy file flag still applies.
    #[tokio::test]
    async fn config_file_abac_uses_the_policy_file_flag() {
        let policy = policy_file(
            "{\"apiVersion\":\"abac.authorization.kubernetes.io/v1beta1\",\"kind\":\"Policy\",\"spec\":{\"user\":\"alice\",\"namespace\":\"*\",\"resource\":\"*\",\"apiGroup\":\"*\",\"readonly\":true}}\n",
        );
        let f = config_file("- type: ABAC\n  name: abac\n");
        let a = build_authorizer_with_config(
            Arc::new(MemoryStorage::new()),
            false,
            &[],
            policy.to_str().unwrap(),
            &Default::default(),
            &f,
            false,
        )
        .unwrap();
        assert!(allowed(&a, pods(user("alice", &[]))).await);
        assert!(!allowed(&a, pods(user("bob", &[]))).await);
        let _ = std::fs::remove_file(&f);
        let _ = std::fs::remove_file(&policy);
    }

    #[test]
    fn invalid_config_file_fails_the_build() {
        let f = config_file("- type: RBAC\n  name: wrong\n");
        let e = build_cfg(&f, &[], false).err().unwrap().to_string();
        assert!(
            e.contains("expected name rbac for authorizer RBAC instead of wrong"),
            "{e}"
        );
        let _ = std::fs::remove_file(&f);
    }

    mod args {
        use clap::{CommandFactory, FromArgMatches, Parser};
        use rusternetes_api_server::authorizer::AuthorizationArgs;

        #[derive(Parser, Debug)]
        struct Wrap {
            #[command(flatten)]
            a: AuthorizationArgs,
        }

        fn parsed(argv: &[&str]) -> AuthorizationArgs {
            let m = Wrap::command().get_matches_from(argv);
            let mut w = Wrap::from_arg_matches(&m).unwrap();
            w.a.mark_legacy_flags_set(&m);
            w.a
        }

        // authorization.go:200-211: pflag `Changed`, so even a flag set to its
        // default value counts; the policy file and --authorization-config don't.
        #[test]
        fn legacy_flags_set_follows_the_command_line() {
            assert!(!parsed(&["x"]).legacy_flags_set);
            assert!(!parsed(&["x", "--authorization-config=/c"]).legacy_flags_set);
            assert!(!parsed(&["x", "--authorization-policy-file=/p"]).legacy_flags_set);
            for flag in [
                "--authorization-mode=RBAC",
                "--authorization-webhook-config-file=/w",
                "--authorization-webhook-version=v1beta1",
                "--authorization-webhook-cache-authorized-ttl=5m",
                "--authorization-webhook-cache-unauthorized-ttl=30s",
            ] {
                let a = parsed(&["x", "--authorization-config=/c", flag]);
                assert!(a.legacy_flags_set, "{flag}");
                assert_eq!(a.authorization_config, "/c");
            }
        }

        #[test]
        fn flag_parses_comma_list() {
            let w =
                Wrap::try_parse_from(["x", "--authorization-mode=Node,RBAC,AlwaysDeny"]).unwrap();
            assert_eq!(w.a.authorization_mode, ["Node", "RBAC", "AlwaysDeny"]);
            let w = Wrap::try_parse_from(["x"]).unwrap();
            assert!(w.a.authorization_mode.is_empty());
            assert!(w.a.authorization_policy_file.is_empty());
            let w = Wrap::try_parse_from(["x", "--authorization-policy-file=/p"]).unwrap();
            assert_eq!(w.a.authorization_policy_file, "/p");
        }
    }
}
