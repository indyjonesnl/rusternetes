//! The one place the api-server's authorizer chain is built.
//!
//! Ported from `pkg/kubeapiserver/authorizer/reload.go` `newForConfig`
//! (:97-99 privileged-groups authorizer first, then the configured modes in
//! order) and `pkg/kubeapiserver/options/authorization.go`
//! (`BuiltInAuthorizationOptions` Complete/Validate/AddFlags).
//! Both entry points (`main.rs` and `lib.rs::run`, the latter used by the
//! all-in-one binary) call [`build_authorizer`] so they cannot diverge (#2679).

use crate::abac::PolicyList;
use crate::authorization_config::{
    name_for_authorizer_mode, AuthorizationConfiguration, AuthorizerConfiguration,
};
use crate::authorization_reload::ChainBuilder;
use rusternetes_common::authz::{
    superuser_then, AlwaysAllowAuthorizer, AlwaysDenyAuthorizer, Authorizer, AuthzStorage,
    NodeAuthorizer, NodeAuthorizerFeatures, RBACAuthorizer,
};
use rusternetes_storage::Storage;
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
    /// Defaults to AlwaysAllow. Comma-delimited list of:
    /// AlwaysAllow,AlwaysDeny,ABAC,Webhook,RBAC,Node (`AddFlags`,
    /// authorization.go:173-175).
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

    /// File with Authorization Configuration to configure the authorizer
    /// chain. This flag is mutually exclusive with the other
    /// --authorization-mode and --authorization-webhook-* flags (`AddFlags`,
    /// authorization.go:194-197; `AuthorizationConfigurationFile`, #3294).
    #[arg(long = "authorization-config", default_value = "")]
    pub authorization_config: String,

    /// `AreLegacyFlagsSet` (authorization.go:200-211): whether any of
    /// `--authorization-mode` / `--authorization-webhook-config-file` /
    /// `-version` / `-cache-authorized-ttl` / `-cache-unauthorized-ttl` was
    /// given on the command line (pflag `fs.Changed`; NOT
    /// `--authorization-policy-file`). Set by [`Self::mark_legacy_flags_set`].
    #[arg(skip)]
    pub legacy_flags_set: bool,
}

impl AuthorizationArgs {
    /// Fill [`Self::legacy_flags_set`] from the parsed command line.
    pub fn mark_legacy_flags_set(&mut self, matches: &clap::ArgMatches) {
        use clap::parser::ValueSource;
        self.legacy_flags_set = [
            "authorization_mode",
            "authorization_webhook_config_file",
            "authorization_webhook_version",
            "authorization_webhook_cache_authorized_ttl",
            "authorization_webhook_cache_unauthorized_ttl",
        ]
        .iter()
        .any(|id| matches.value_source(id) == Some(ValueSource::CommandLine));
    }
}

/// `IsValidAuthorizationMode` (modes.go:41).
pub fn is_valid_authorization_mode(mode: &str) -> bool {
    AUTHORIZATION_MODE_CHOICES.contains(&mode)
}

/// `BuiltInAuthorizationOptions.Complete`
/// (pkg/kubeapiserver/options/authorization.go:88-96).
///
/// No deviation (#3266): an empty list defaults to `AlwaysAllow`, exactly like
/// upstream, but only when no `--authorization-config` file is given
/// (`len(AuthorizationConfigurationFile) == 0 && len(Modes) == 0`, #3294).
/// Deployments that want RBAC must pass `--authorization-mode=Node,RBAC`
/// explicitly, as kubeadm does.
pub fn complete_authorization_modes(config_file: &str, modes: &[String]) -> Vec<String> {
    if config_file.is_empty() && modes.is_empty() {
        return vec![MODE_ALWAYS_ALLOW.to_string()];
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
pub fn build_authorizer<S: AuthzStorage + Storage + 'static>(
    storage: Arc<S>,
    skip_auth: bool,
    modes: &[String],
    policy_file: &str,
) -> anyhow::Result<Arc<dyn Authorizer>> {
    build_authorizer_with_webhook(storage, skip_auth, modes, policy_file, &Default::default())
}

/// [`build_authorizer`] with the `--authorization-webhook-*` options, for the
/// `Webhook` mode (reload.go:124-167).
pub fn build_authorizer_with_webhook<S: AuthzStorage + Storage + 'static>(
    storage: Arc<S>,
    skip_auth: bool,
    modes: &[String],
    policy_file: &str,
    webhook: &crate::authorization_webhook::AuthorizationWebhookArgs,
) -> anyhow::Result<Arc<dyn Authorizer>> {
    build_authorizer_with_config(storage, skip_auth, modes, policy_file, webhook, "", false)
}

/// The `--authorization-config` branch of `BuiltInAuthorizationOptions.Validate`
/// / `ToAuthorizationConfig` (authorization.go:110-127, :232-247): the legacy
/// flags must be unset, then the file is loaded and validated with no opinion
/// about required modes (`LoadAndValidateFile(file, compiler, nil)`). Returns
/// the configuration and the file's data.
///
/// `StructuredAuthorizationConfiguration` (the gate checked at :105 / :236) is
/// GA and locked on in 1.35, so that check cannot fire.
pub fn load_authorization_config(
    config_file: &str,
    legacy_flags_set: bool,
) -> Result<
    (
        crate::authorization_config::AuthorizationConfiguration,
        String,
    ),
    String,
> {
    if legacy_flags_set {
        return Err("--authorization-config can not be specified when --authorization-mode or --authorization-webhook-* flags are defined".to_string());
    }
    crate::authorization_config::load_and_validate_file(config_file, &Default::default())
}

/// [`build_authorizer_with_webhook`] with `--authorization-config`: `Complete`
/// (authorization.go:88-96, the AlwaysAllow default only without a file),
/// `Validate` (:99-163, the file branch returning early), then the chain
/// (reload.go:97-176) from the file's authorizers instead of the modes.
///
/// ABAC still reads `--authorization-policy-file` (`config.PolicyFile`,
/// config.go:126-130). With a file, the result is a
/// [`ReloadableAuthorizer`](crate::authorization_reload::ReloadableAuthorizer)
/// that watches it (`Config.New`, config.go:157-159; must be called inside a
/// tokio runtime).
pub fn build_authorizer_with_config<S: AuthzStorage + Storage + 'static>(
    storage: Arc<S>,
    skip_auth: bool,
    modes: &[String],
    policy_file: &str,
    webhook: &crate::authorization_webhook::AuthorizationWebhookArgs,
    config_file: &str,
    legacy_flags_set: bool,
) -> anyhow::Result<Arc<dyn Authorizer>> {
    let modes = complete_authorization_modes(config_file, modes);
    let (configuration, initial_data) = if config_file.is_empty() {
        let mut errs = validate_authorization_modes(&modes, policy_file);
        errs.extend(crate::authorization_webhook::validate_webhook_args(
            &modes, webhook,
        ));
        if !errs.is_empty() {
            anyhow::bail!("invalid authorization options: {}", errs.join("; "));
        }
        // `buildAuthorizationConfiguration` (authorization.go:250-290): the
        // legacy flags are the same chain as a file of the same shape.
        (
            AuthorizationConfiguration {
                authorizers: modes
                    .iter()
                    .map(|mode| AuthorizerConfiguration {
                        type_: mode.clone(),
                        name: if mode == MODE_WEBHOOK {
                            DEFAULT_WEBHOOK_NAME.to_string()
                        } else {
                            name_for_authorizer_mode(mode)
                        },
                        webhook: None,
                    })
                    .collect(),
            },
            String::new(),
        )
    } else {
        load_authorization_config(config_file, legacy_flags_set)
            .map_err(|e| anyhow::anyhow!("invalid authorization options: {e}"))?
    };
    if skip_auth {
        return Ok(Arc::new(AlwaysAllowAuthorizer));
    }

    // Config.New (config.go:84-145): authorizers that persist across reloads.
    let mut factory = ChainFactory {
        node: None,
        rbac: None,
        abac: None,
        legacy_webhook: webhook.clone(),
    };
    for a in &configuration.authorizers {
        match a.type_.as_str() {
            MODE_NODE => factory.node = Some(Arc::new(new_node_authorizer(storage.clone()))),
            // config.go:126-130 `abac.NewFromFile(config.PolicyFile)`.
            MODE_ABAC => {
                factory.abac = Some(Arc::new(PolicyList::new_from_file(Path::new(policy_file))?))
            }
            MODE_RBAC => factory.rbac = Some(Arc::new(RBACAuthorizer::new(storage.clone()))),
            _ => {}
        }
    }
    let factory = Arc::new(factory);
    let initial = factory.new_for_config(&configuration)?;
    if config_file.is_empty() {
        return Ok(initial);
    }
    let reloadable = crate::authorization_reload::ReloadableAuthorizer::new(
        config_file,
        crate::apiserver_identity::apiserver_id(&crate::apiserver_identity::hostname()),
        configuration,
        initial_data.into_bytes(),
        initial,
        factory,
    );
    reloadable.spawn_reload();
    Ok(reloadable)
}

/// `node.NewAuthorizer(graph, identifier, nodeRules)` plus
/// `node.AddGraphEventHandlers` (pkg/kubeapiserver/authorizer/config.go:84-100,
/// `Config.New`): the graph is created with the authorizer and kept current by
/// informers on pods, PVs, volume attachments and resource slices.
///
/// `PodCertificateRequest` is Beta and OFF by default in 1.35, in which case
/// upstream passes a nil PCR informer and `NodeAuthorizer` answers no opinion
/// for that resource; both follow `NodeAuthorizerFeatures::default()`.
///
/// Must run inside a tokio runtime, like the reloadable chain; without one the
/// graph stays empty and the authorizer denies every graph-gated request.
fn new_node_authorizer<S: Storage + 'static>(storage: Arc<S>) -> NodeAuthorizer {
    let authorizer = NodeAuthorizer::default();
    if tokio::runtime::Handle::try_current().is_ok() {
        crate::node_graph_populator::spawn_graph_populator(
            storage,
            authorizer.graph().clone(),
            NodeAuthorizerFeatures::default().pod_certificate_request,
        );
    } else {
        tracing::warn!(
            "Node authorizer built outside a tokio runtime: its graph will not be populated"
        );
    }
    authorizer
}

/// `defaultWebhookName` (authorization.go).
const DEFAULT_WEBHOOK_NAME: &str = "default";

/// `newForConfig` (reload.go:97-176) with the authorizers that
/// `Config.New` builds once and reuses across reloads.
struct ChainFactory {
    node: Option<Arc<dyn Authorizer>>,
    rbac: Option<Arc<dyn Authorizer>>,
    abac: Option<Arc<dyn Authorizer>>,
    /// `--authorization-webhook-*`, for a Webhook entry without its own
    /// configuration (the legacy-flags path).
    legacy_webhook: crate::authorization_webhook::AuthorizationWebhookArgs,
}

impl crate::authorization_reload::ChainBuilder for ChainFactory {
    fn new_for_config(
        &self,
        config: &AuthorizationConfiguration,
    ) -> anyhow::Result<Arc<dyn Authorizer>> {
        use crate::authorization_metrics::instrumented_authorizer as instrument;
        use crate::authorization_webhook::WebhookAuthorizer;
        if config.authorizers.is_empty() {
            anyhow::bail!("at least one authorization mode must be passed");
        }
        // The privileged-groups authorizer is not instrumented upstream either.
        let mut chain: Vec<Arc<dyn Authorizer>> = Vec::new();
        for a in &config.authorizers {
            let mode = a.type_.as_str();
            let delegate: Arc<dyn Authorizer> = match mode {
                MODE_NODE => self.node.clone().ok_or_else(|| {
                    anyhow::anyhow!(
                        "authorizer type Node is not allowed if it was not enabled at initial server startup"
                    )
                })?,
                MODE_ALWAYS_ALLOW => Arc::new(AlwaysAllowAuthorizer),
                MODE_ALWAYS_DENY => Arc::new(AlwaysDenyAuthorizer),
                MODE_ABAC => self.abac.clone().ok_or_else(|| {
                    anyhow::anyhow!(
                        "authorizer type ABAC is not allowed if it was not enabled at initial server startup"
                    )
                })?,
                MODE_WEBHOOK => Arc::new(match &a.webhook {
                    Some(w) => WebhookAuthorizer::from_configuration(&a.name, w)?,
                    None => WebhookAuthorizer::from_args(&self.legacy_webhook)?,
                }),
                MODE_RBAC => self.rbac.clone().ok_or_else(|| {
                    anyhow::anyhow!(
                        "authorizer type RBAC is not allowed if it was not enabled at initial server startup"
                    )
                })?,
                other => anyhow::bail!("unknown authorization mode {other} specified"),
            };
            chain.push(instrument(mode, &a.name, delegate));
        }
        Ok(Arc::new(superuser_then(chain)))
    }
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
        use super::super::AuthorizationArgs;
        use clap::{CommandFactory, FromArgMatches, Parser};

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
