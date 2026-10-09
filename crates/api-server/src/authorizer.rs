//! The one place the api-server's authorizer chain is built.
//!
//! Ported from `pkg/kubeapiserver/authorizer/reload.go` `newForConfig`
//! (:97-99 privileged-groups authorizer first, then the configured modes in
//! order) and `pkg/kubeapiserver/options/authorization.go`
//! (`BuiltInAuthorizationOptions` Complete/Validate/AddFlags).
//! Both entry points (`main.rs` and `lib.rs::run`, the latter used by the
//! all-in-one binary) call [`build_authorizer`] so they cannot diverge (#2679).

use crate::abac::PolicyList;
use rusternetes_common::authz::{
    superuser_then, AlwaysAllowAuthorizer, AlwaysDenyAuthorizer, Authorizer, AuthzStorage,
    NodeAuthorizer, RBACAuthorizer,
};
use std::path::Path;
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
    /// Ordered list of plug-ins to do authorization on secure port.
    /// Comma-delimited list of: AlwaysAllow,AlwaysDeny,ABAC,Webhook,RBAC,Node
    /// (`AddFlags`, authorization.go:166-168). Defaults to Node,RBAC.
    #[arg(long = "authorization-mode", value_delimiter = ',')]
    pub authorization_mode: Vec<String>,

    /// File with authorization policy in json line by line format, used with
    /// --authorization-mode=ABAC, on the secure port (`AddFlags`,
    /// authorization.go:176-177).
    #[arg(long = "authorization-policy-file", default_value = "")]
    pub authorization_policy_file: String,
    /// `--authorization-webhook-*` (#2895).
    #[command(flatten)]
    pub webhook: crate::authorization_webhook::AuthorizationWebhookArgs,
}

/// `IsValidAuthorizationMode` (modes.go:41).
pub fn is_valid_authorization_mode(mode: &str) -> bool {
    AUTHORIZATION_MODE_CHOICES.contains(&mode)
}

/// `BuiltInAuthorizationOptions.Complete`
/// (pkg/kubeapiserver/options/authorization.go:88-96).
///
/// Deliberate deviation: upstream defaults an empty list to `AlwaysAllow`;
/// rusternetes keeps its established `Node,RBAC` default (what kubeadm sets
/// for a real cluster, and what #1664/#2679 rely on).
pub fn complete_authorization_modes(modes: &[String]) -> Vec<String> {
    if modes.is_empty() {
        return vec![MODE_NODE.to_string(), MODE_RBAC.to_string()];
    }
    modes.to_vec()
}

/// `BuiltInAuthorizationOptions.Validate`, legacy-flag branch
/// (authorization.go:123-152). The Webhook-specific checks (webhook config
/// file) arrive with that mode's flags.
pub fn validate_authorization_modes(modes: &[String], policy_file: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if modes.is_empty() {
        errs.push("at least one authorization-mode must be passed".to_string());
    }
    let mut seen = std::collections::HashSet::new();
    for mode in modes {
        if !is_valid_authorization_mode(mode) {
            errs.push(format!("authorization-mode {mode:?} is not a valid mode"));
        }
        if mode == MODE_ABAC && policy_file.is_empty() {
            errs.push("authorization-mode ABAC's authorization policy file not passed".to_string());
        }
        seen.insert(mode.as_str());
    }
    if seen.len() != modes.len() {
        // Go's %q of a []string prints `["a" "b"]`.
        let quoted: Vec<String> = modes.iter().map(|m| format!("{m:?}")).collect();
        errs.push(format!(
            "authorization-mode [{}] has mode specified more than once",
            quoted.join(" ")
        ));
    }
    if !policy_file.is_empty() && !seen.contains(MODE_ABAC) {
        errs.push("cannot specify --authorization-policy-file without mode ABAC".to_string());
    }
    errs
}

/// Build the authorizer chain: `AlwaysAllow` when `skip_auth`, otherwise the
/// `system:masters` superuser authorizer first (reload.go:97-99) followed by
/// the configured `modes` in order (reload.go:101-176), after Complete and
/// Validate. ABAC reads `policy_file` once here (reload.go:118-123 reuses the
/// list loaded at startup; no hot reload). Webhook: see [`build_authorizer_with_webhook`].
pub fn build_authorizer<S: AuthzStorage + 'static>(
    storage: Arc<S>,
    skip_auth: bool,
    modes: &[String],
    policy_file: &str,
) -> anyhow::Result<Arc<dyn Authorizer>> {
    build_authorizer_with_webhook(storage, skip_auth, modes, policy_file, &Default::default())
}

/// [`build_authorizer`] with the `--authorization-webhook-*` options, for the
/// `Webhook` mode (reload.go:124-167).
pub fn build_authorizer_with_webhook<S: AuthzStorage + 'static>(
    storage: Arc<S>,
    skip_auth: bool,
    modes: &[String],
    policy_file: &str,
    webhook: &crate::authorization_webhook::AuthorizationWebhookArgs,
) -> anyhow::Result<Arc<dyn Authorizer>> {
    let modes = complete_authorization_modes(modes);
    let mut errs = validate_authorization_modes(&modes, policy_file);
    errs.extend(crate::authorization_webhook::validate_webhook_args(
        &modes, webhook,
    ));
    if !errs.is_empty() {
        anyhow::bail!("invalid authorization options: {}", errs.join("; "));
    }
    if skip_auth {
        return Ok(Arc::new(AlwaysAllowAuthorizer));
    }
    let mut chain: Vec<Arc<dyn Authorizer>> = Vec::new();
    for mode in &modes {
        match mode.as_str() {
            MODE_NODE => chain.push(Arc::new(NodeAuthorizer)),
            MODE_ALWAYS_ALLOW => chain.push(Arc::new(AlwaysAllowAuthorizer)),
            MODE_ALWAYS_DENY => chain.push(Arc::new(AlwaysDenyAuthorizer)),
            // config.go:126-130 `abac.NewFromFile(config.PolicyFile)`.
            MODE_ABAC => chain.push(Arc::new(PolicyList::new_from_file(Path::new(policy_file))?)),
            MODE_RBAC => chain.push(Arc::new(RBACAuthorizer::new(storage.clone()))),
            MODE_WEBHOOK => chain.push(Arc::new(
                crate::authorization_webhook::WebhookAuthorizer::from_args(webhook)?,
            )),
            other => anyhow::bail!("authorization-mode {other} is not supported yet"),
        }
    }
    Ok(Arc::new(superuser_then(chain)))
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
            assert!(w.a.authorization_policy_file.is_empty());
            let w = Wrap::try_parse_from(["x", "--authorization-policy-file=/p"]).unwrap();
            assert_eq!(w.a.authorization_policy_file, "/p");
        }
    }
}
