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
    node_graph::NodeGraph, superuser_then, AlwaysAllowAuthorizer, AlwaysDenyAuthorizer, Authorizer,
    AuthzStorage, NodeAuthorizer, NodeAuthorizerFeatures, RBACAuthorizer,
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
            MODE_NODE => factory.node = Some(new_node_authorizer(storage.clone())),
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

/// The Node authorizer with its graph and the populator that keeps it current
/// (`pkg/kubeapiserver/authorizer/config.go`: `node.NewGraph()`,
/// `node.AddGraphEventHandlers`, `node.NewAuthorizer(graph, nodeidentifier,
/// bootstrappolicy.NodeRules())`).
///
/// DEVIATION: `AuthorizeNodeWithSelectors` is pinned off here although it is GA
/// and locked on upstream. With it on, upstream authorizes a node's pod and
/// node reads by `spec.nodeName` / `metadata.name` field selectors, and this
/// repo's kubelet reads pods and nodes through `ApiStorage` without them; the
/// stricter decisions would lock every kubelet out of its own pods. Off is the
/// mode upstream runs with the gate disabled (node_authorizer.go:131-153):
/// pods and nodes fall to the static node rules. Flip it once the kubelet
/// sends the selectors.
fn new_node_authorizer<S: Storage + 'static>(storage: Arc<S>) -> Arc<dyn Authorizer> {
    let graph = Arc::new(NodeGraph::new());
    crate::node_graph_populator::spawn(storage, graph.clone());
    let features = NodeAuthorizerFeatures {
        authorize_node_with_selectors: false,
        ..NodeAuthorizerFeatures::current()
    };
    Arc::new(NodeAuthorizer::new(graph).with_features(features))
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
