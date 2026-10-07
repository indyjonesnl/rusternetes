//! The `paramKind` / `paramRef` read-access authorization — port of
//! `pkg/registry/admissionregistration/validatingadmissionpolicy/authz.go` and
//! `.../validatingadmissionpolicybinding/authz.go`.
//!
//! Upstream runs it inside the strategy's `Validate` / `ValidateUpdate`, once
//! the object is well-formed (`validatingadmissionpolicy/strategy.go:79-87`,
//! `:104-112`; the binding's likewise). A strategy's `validate` is synchronous
//! here and the authorizer is async, so the check runs in the Store's
//! `BeginCreate` / `BeginUpdate` hook instead — the same seam the RBAC
//! policybased storages use. The hook first runs the strategy's own validation
//! and, like upstream's `if len(errs) == 0`, authorizes only a well-formed
//! object (the malformed one is then rejected by the Store's validation with
//! the ordinary errors).
//!
//! Where this differs from upstream: `resolver.ResourceResolver`
//! (`resolver/resolver.go:30-61`) asks the discovery client for the resource
//! serving a kind. [`resolve_resource`] asks the same discovery documents the
//! api-server serves (`rusternetes_discovery::resolve_kind_to_resource`) for
//! the built-in kinds and reads the CustomResourceDefinitions in storage for
//! the rest. A kind it cannot resolve is authorized on resource `*`, which is
//! what upstream does when `Resolve` fails.

use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, RequestAttributes};
use rusternetes_common::resources::validating_admission_policy::{ParamKind, ParamRef};
use rusternetes_common::resources::{
    CustomResourceDefinition, ValidatingAdmissionPolicy, ValidatingAdmissionPolicyBinding,
};
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
use rusternetes_common::Result;
use rusternetes_storage::{build_key, build_prefix, Storage, StorageBackend};

use crate::registry::rbac::escalation_check::escalation_allowed;
use crate::registry::rest::RequestContext;

/// `EscalationAllowed(ctx)` and `UserFrom(ctx)`: `Ok(None)` for a superuser
/// (skip all checks), the requester otherwise.
fn requester(ctx: &RequestContext, what: &str) -> std::result::Result<Option<UserInfo>, String> {
    // for superuser, skip all checks
    if escalation_allowed(ctx) {
        return Ok(None);
    }
    match &ctx.user {
        Some(user) => Ok(Some(user.clone())),
        None => Err(format!(
            "cannot identify user to authorize read access to {what}"
        )),
    }
}

/// Go's `%v` of a `*user.DefaultInfo`: `&{name uid [groups] map[extra]}`.
fn format_user(user: &UserInfo) -> String {
    let mut extra: Vec<_> = user.extra.iter().collect();
    extra.sort_by(|a, b| a.0.cmp(b.0));
    let extra = extra
        .iter()
        .map(|(k, v)| format!("{k}:[{}]", v.join(" ")))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "&{{{} {} [{}] map[{}]}}",
        user.username,
        user.uid,
        user.groups.join(" "),
        extra
    )
}

/// `schema.ParseGroupVersion` (apimachinery/pkg/runtime/schema/group_version.go):
/// `""` and `"v1"` are the core group; `"g/v"` splits; more than one `/` is an
/// error.
fn parse_group_version(api_version: &str) -> std::result::Result<(String, String), ()> {
    if api_version.is_empty() || api_version == "/" {
        return Ok((String::new(), String::new()));
    }
    match api_version.matches('/').count() {
        0 => Ok((String::new(), api_version.to_string())),
        1 => {
            let (g, v) = api_version.split_once('/').unwrap_or_default();
            Ok((g.to_string(), v.to_string()))
        }
        _ => Err(()),
    }
}

/// `ResourceResolver.Resolve(gv.WithKind(kind))` (resolver/resolver.go:36-60);
/// see the module docs for how it differs.
async fn resolve_resource(
    storage: &StorageBackend,
    group: &str,
    version: &str,
    kind: &str,
) -> Option<String> {
    if let Some(resource) = rusternetes_discovery::resolve_kind_to_resource(group, version, kind) {
        return Some(resource);
    }
    let prefix = build_prefix("customresourcedefinitions", None);
    let crds: Vec<CustomResourceDefinition> = storage.list(&prefix).await.ok()?;
    crds.into_iter()
        .find(|crd| {
            crd.spec.group == group
                && crd.spec.names.kind == kind
                && crd.spec.versions.iter().any(|v| v.name == version)
        })
        .map(|crd| crd.spec.names.plural)
}

/// `validatingAdmissionPolicyStrategy.authorizeCreate` / `authorizeUpdate` /
/// `authorize` (validatingadmissionpolicy/authz.go:30-96): the requester needs
/// `get` on all objects of `spec.paramKind`. The error is wrapped in a
/// `field.Forbidden` at `spec.paramKind` (strategy.go:83-85, :108-110).
///
/// `old` is `None` on create; on update the check is skipped when the
/// paramKind is unchanged (authz.go:47-50).
pub async fn authorize_param_kind(
    ctx: &RequestContext,
    authorizer: &dyn Authorizer,
    storage: &StorageBackend,
    policy: &ValidatingAdmissionPolicy,
    old: Option<&ValidatingAdmissionPolicy>,
) -> ErrorList {
    let Some(param_kind) = policy.spec.as_ref().and_then(|s| s.param_kind.as_ref()) else {
        // no paramKind in new object
        return Vec::new();
    };
    if let Some(old_kind) = old
        .and_then(|o| o.spec.as_ref())
        .and_then(|s| s.param_kind.as_ref())
    {
        if old_kind == param_kind {
            // identical paramKind to old object
            return Vec::new();
        }
    }
    match authorize_kind(ctx, authorizer, storage, param_kind).await {
        Ok(()) => Vec::new(),
        Err(msg) => vec![FieldError::forbidden(
            &Path::new("spec").child("paramKind"),
            msg,
        )],
    }
}

async fn authorize_kind(
    ctx: &RequestContext,
    authorizer: &dyn Authorizer,
    storage: &StorageBackend,
    param_kind: &ParamKind,
) -> std::result::Result<(), String> {
    let Some(user) = requester(ctx, "paramKind resources")? else {
        return Ok(());
    };
    let api_version = param_kind.api_version.clone().unwrap_or_default();
    // default to requiring permissions on all group/version/resources
    let (mut resource, mut api_group) = ("*".to_string(), "*".to_string());
    if let Ok((group, version)) = parse_group_version(&api_version) {
        // we only need to authorize the parsed group/version
        api_group = group.clone();
        if let Some(r) = resolve_resource(storage, &group, &version, &param_kind.kind).await {
            // we only need to authorize the resolved resource
            resource = r;
        }
    }
    // require that the user can read (verb "get") the referred kind.
    let attrs = RequestAttributes::new(user.clone(), "get", resource)
        .with_api_group(api_group)
        .with_name("*")
        .with_namespace("*");
    match authorizer.authorize(&attrs).await {
        Err(e) => Err(e.to_string()),
        Ok(Decision::Allow) => Ok(()),
        Ok(Decision::Deny(_)) => Err(format!(
            "user {} must have \"get\" permission on all objects of the referenced paramKind (kind={}, apiVersion={})",
            format_user(&user),
            param_kind.kind,
            api_version
        )),
    }
}

/// `validatingAdmissionPolicyBindingStrategy.authorizeCreate` /
/// `authorizeUpdate` / `authorize` (validatingadmissionpolicybinding/authz.go:
/// 30-117): the requester needs `get` (named paramRef) or `list` (selector) on
/// the param resource, found through the policy named by `spec.policyName`.
/// The error is wrapped in a `field.Forbidden` at `spec.paramRef`.
///
/// `old` is `None` on create; on update the check is skipped when paramRef and
/// policyName are both unchanged (authz.go:47-50).
pub async fn authorize_param_ref(
    ctx: &RequestContext,
    authorizer: &dyn Authorizer,
    storage: &StorageBackend,
    binding: &ValidatingAdmissionPolicyBinding,
    old: Option<&ValidatingAdmissionPolicyBinding>,
) -> Result<ErrorList> {
    let Some(param_ref) = binding.spec.as_ref().and_then(|s| s.param_ref.as_ref()) else {
        // no paramRef in new object
        return Ok(Vec::new());
    };
    if let Some(old_spec) = old.and_then(|o| o.spec.as_ref()) {
        if old_spec.param_ref.as_ref() == Some(param_ref)
            && old_spec.policy_name == binding.spec.as_ref().and_then(|s| s.policy_name.clone())
        {
            // identical paramRef and policy to old object
            return Ok(Vec::new());
        }
    }
    let policy_name = binding
        .spec
        .as_ref()
        .and_then(|s| s.policy_name.clone())
        .unwrap_or_default();
    Ok(
        match authorize_ref(ctx, authorizer, storage, &policy_name, param_ref).await {
            Ok(()) => Vec::new(),
            Err(msg) => vec![FieldError::forbidden(
                &Path::new("spec").child("paramRef"),
                msg,
            )],
        },
    )
}

async fn authorize_ref(
    ctx: &RequestContext,
    authorizer: &dyn Authorizer,
    storage: &StorageBackend,
    policy_name: &str,
    param_ref: &ParamRef,
) -> std::result::Result<(), String> {
    let Some(user) = requester(ctx, "paramRef object")? else {
        return Ok(());
    };
    // default to requiring permissions on all group/version/resources
    let (mut resource, mut api_group) = ("*".to_string(), "*".to_string());

    // `policyGetter.GetValidatingAdmissionPolicy`
    let policy: std::result::Result<ValidatingAdmissionPolicy, _> = storage
        .get(&build_key("validatingadmissionpolicies", None, policy_name))
        .await;
    let mut gv_parse_failed = false;
    let mut gvr_resolve_failed = false;
    let policy_param_kind = policy
        .as_ref()
        .ok()
        .and_then(|p| p.spec.as_ref())
        .and_then(|s| s.param_kind.clone());
    if let Some(param_kind) = &policy_param_kind {
        match parse_group_version(param_kind.api_version.as_deref().unwrap_or_default()) {
            Err(()) => gv_parse_failed = true,
            Ok((group, version)) => {
                // we only need to authorize the parsed group/version
                api_group = group.clone();
                match resolve_resource(storage, &group, &version, &param_kind.kind).await {
                    // we only need to authorize the resolved resource
                    Some(r) => resource = r,
                    None => gvr_resolve_failed = true,
                }
            }
        }
    }

    let name = param_ref.name.clone().unwrap_or_default();
    let verb = if name.is_empty() { "list" } else { "get" };
    let mut attrs = RequestAttributes::new(user.clone(), verb, resource).with_api_group(api_group);
    if !name.is_empty() {
        attrs = attrs.with_name(name);
    }
    // if empty, no namespace indicates get across all namespaces
    if let Some(ns) = param_ref.namespace.as_deref().filter(|n| !n.is_empty()) {
        attrs = attrs.with_namespace(ns);
    }

    match authorizer.authorize(&attrs).await {
        Err(e) => Err(format!("failed to authorize request: {e}")),
        Ok(Decision::Allow) => Ok(()),
        Ok(Decision::Deny(_)) => {
            let who = format_user(&user);
            if policy.is_err() {
                return Err(format!(
                    "unable to get policy {policy_name} to determine minimum required permissions and user {who} does not have \"{verb}\" permission for all groups, versions and resources"
                ));
            }
            let kind = policy_param_kind
                .as_ref()
                .map(|k| {
                    format!(
                        "&{{{} {}}}",
                        k.api_version.clone().unwrap_or_default(),
                        k.kind
                    )
                })
                .unwrap_or_default();
            if gv_parse_failed {
                return Err(format!(
                    "unable to parse paramKind {kind} to determine minimum required permissions and user {who} does not have \"{verb}\" permission for all groups, versions and resources"
                ));
            }
            if gvr_resolve_failed {
                return Err(format!(
                    "unable to resolve paramKind {kind} to determine minimum required permissions and user {who} does not have \"{verb}\" permission for all groups, versions and resources"
                ));
            }
            Err(format!(
                "user {who} does not have \"{verb}\" permission on the object referenced by paramRef"
            ))
        }
    }
}
