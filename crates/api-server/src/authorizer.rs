//! The one place the api-server's authorizer chain is built.
//!
//! Ported from `pkg/kubeapiserver/authorizer/reload.go` `newForConfig`
//! (:97-99 privileged-groups authorizer first, then the configured modes in
//! order) and `pkg/kubeapiserver/options/authorization.go`
//! (`BuiltInAuthorizationOptions` Complete/Validate/AddFlags).
//! Both entry points (`main.rs` and `lib.rs::run`, the latter used by the
//! all-in-one binary) call [`build_authorizer`] so they cannot diverge (#2679).

use rusternetes_common::authz::{
    superuser_then, AlwaysAllowAuthorizer, Authorizer, AuthzStorage, NodeAuthorizer, RBACAuthorizer,
};
use std::sync::Arc;

/// `--authorization-mode` values (`pkg/kubeapiserver/authorizer/modes/modes.go:22-35`).
pub const MODE_ALWAYS_ALLOW: &str = "AlwaysAllow";
pub const MODE_ALWAYS_DENY: &str = "AlwaysDeny";
pub const MODE_ABAC: &str = "ABAC";
pub const MODE_WEBHOOK: &str = "Webhook";
pub const MODE_RBAC: &str = "RBAC";
pub const MODE_NODE: &str = "Node";
/// `AuthorizationModeChoices` (modes.go:38).
pub const AUTHORIZATION_MODE_CHOICES: [&str; 6] = [
    MODE_ALWAYS_ALLOW,
    MODE_ALWAYS_DENY,
    MODE_ABAC,
    MODE_WEBHOOK,
    MODE_RBAC,
    MODE_NODE,
];

/// The `--authorization-mode` flag, shared by the `api-server` and all-in-one
/// binaries (#2854).
#[derive(clap::Args, Debug, Clone, Default)]
pub struct AuthorizationArgs {
    #[arg(long = "authorization-mode", value_delimiter = ',')]
    pub authorization_mode: Vec<String>,
}

/// `IsValidAuthorizationMode` (modes.go:41).
pub fn is_valid_authorization_mode(_mode: &str) -> bool {
    todo!()
}

/// `BuiltInAuthorizationOptions.Complete`.
pub fn complete_authorization_modes(_modes: &[String]) -> Vec<String> {
    todo!()
}

/// `BuiltInAuthorizationOptions.Validate`.
pub fn validate_authorization_modes(_modes: &[String]) -> Vec<String> {
    todo!()
}

/// Build the authorizer chain.
pub fn build_authorizer<S: AuthzStorage + 'static>(
    storage: Arc<S>,
    skip_auth: bool,
    modes: &[String],
) -> anyhow::Result<Arc<dyn Authorizer>> {
    let _ = modes;
    if skip_auth {
        return Ok(Arc::new(AlwaysAllowAuthorizer));
    }
    let node: Arc<dyn Authorizer> = Arc::new(NodeAuthorizer);
    let rbac: Arc<dyn Authorizer> = Arc::new(RBACAuthorizer::new(storage));
    Ok(Arc::new(superuser_then(vec![node, rbac])))
}

#[cfg(test)]
mod tests {
    use super::*;
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
        build_authorizer(Arc::new(MemoryStorage::new()), false, &modes(m))
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
        let a = build_authorizer(Arc::new(MemoryStorage::new()), true, &[]).unwrap();
        let attrs = RequestAttributes::new(user("alice", &[]), "delete", "nodes");
        assert!(allowed(&a, attrs).await);
    }

    // TestAuthzValidate (pkg/kubeapiserver/options/authorization_test.go:34-).
    fn errs(m: &[&str]) -> String {
        validate_authorization_modes(&modes(m)).join("; ")
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
        assert!(validate_authorization_modes(&modes(&["AlwaysAllow", "AlwaysDeny"])).is_empty());
        assert!(validate_authorization_modes(&modes(&["Node", "RBAC"])).is_empty());
    }

    #[test]
    fn complete_defaults_to_node_rbac() {
        assert_eq!(complete_authorization_modes(&[]), modes(&["Node", "RBAC"]));
        assert_eq!(
            complete_authorization_modes(&modes(&["RBAC"])),
            modes(&["RBAC"])
        );
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

    mod args {
        use super::super::AuthorizationArgs;
        use clap::Parser;

        #[derive(Parser, Debug)]
        struct Wrap {
            #[command(flatten)]
            a: AuthorizationArgs,
        }

        #[test]
        fn flag_parses_comma_list() {
            let w =
                Wrap::try_parse_from(["x", "--authorization-mode=Node,RBAC,AlwaysDeny"]).unwrap();
            assert_eq!(w.a.authorization_mode, ["Node", "RBAC", "AlwaysDeny"]);
            let w = Wrap::try_parse_from(["x"]).unwrap();
            assert!(w.a.authorization_mode.is_empty());
        }
    }
}
