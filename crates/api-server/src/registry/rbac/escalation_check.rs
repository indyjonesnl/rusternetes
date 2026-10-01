//! Port of `pkg/registry/rbac/escalation_check.go` and `helpers.go`: the
//! checks the policybased storages run before the privilege-escalation rule.

use rusternetes_common::authz::{Authorizer, Decision, RequestAttributes};
use rusternetes_common::resources::RoleRef;
use serde::Serialize;

use crate::registry::rest::RequestContext;

/// `user.SystemPrivilegedGroup`.
const SYSTEM_PRIVILEGED_GROUP: &str = "system:masters";

/// `rbac.GroupName`.
const RBAC_GROUP: &str = "rbac.authorization.k8s.io";

/// `EscalationAllowed` (escalation_check.go:33-47): whether the requester is a
/// superuser. `system:masters` is special because the API server uses it for
/// privileged loopback connections, so a member can always do anything.
pub fn escalation_allowed(ctx: &RequestContext) -> bool {
    ctx.user
        .as_ref()
        .is_some_and(|u| u.groups.iter().any(|g| g == SYSTEM_PRIVILEGED_GROUP))
}

/// `RoleEscalationAuthorized` (escalation_check.go:55-100): whether the
/// requester is explicitly authorized to `escalate` the role resource named by
/// the request. `resource` is `roles` or `clusterroles` (the `roleResources`
/// gate), `name` the request name (empty on a POST).
pub async fn role_escalation_authorized(
    ctx: &RequestContext,
    authorizer: &dyn Authorizer,
    resource: &str,
) -> bool {
    let Some(user) = ctx.user.clone() else {
        return false;
    };
    let mut attrs = RequestAttributes::new(user, "escalate", resource).with_api_group(RBAC_GROUP);
    if let Some(name) = ctx.name.as_deref().filter(|n| !n.is_empty()) {
        attrs = attrs.with_name(name);
    }
    if let Some(ns) = ctx.namespace.as_deref().filter(|n| !n.is_empty()) {
        attrs = attrs.with_namespace(ns);
    }
    authorize_allowed(authorizer, &attrs).await
}

/// `BindingAuthorized` (escalation_check.go:103-146): whether the requester is
/// explicitly authorized to `bind` the specified roleRef.
///
/// Authorized against the namespace where the binding is being created (the
/// empty namespace for ClusterRoleBindings). This allows delegating the right
/// to bind particular ClusterRoles in RoleBindings within particular
/// namespaces, and to bind a ClusterRole across all namespaces in a
/// ClusterRoleBinding.
pub async fn binding_authorized(
    ctx: &RequestContext,
    role_ref: &RoleRef,
    binding_namespace: &str,
    authorizer: &dyn Authorizer,
) -> bool {
    let Some(user) = ctx.user.clone() else {
        return false;
    };
    // This occurs after defaulting and conversion, so values pulled from the
    // roleRef won't change. Invalid APIGroup or Name values will fail
    // validation.
    let resource = match role_ref.kind.as_str() {
        "ClusterRole" => "clusterroles",
        "Role" => "roles",
        _ => return false,
    };
    let mut attrs =
        RequestAttributes::new(user, "bind", resource).with_api_group(role_ref.api_group.clone());
    if !role_ref.name.is_empty() {
        attrs = attrs.with_name(role_ref.name.clone());
    }
    if !binding_namespace.is_empty() {
        attrs = attrs.with_namespace(binding_namespace);
    }
    authorize_allowed(authorizer, &attrs).await
}

/// `decision == authorizer.DecisionAllow`; an error is logged by upstream and
/// is not an allow.
async fn authorize_allowed(authorizer: &dyn Authorizer, attrs: &RequestAttributes) -> bool {
    match authorizer.authorize(attrs).await {
        Ok(Decision::Allow) => true,
        Ok(Decision::Deny(_)) => false,
        Err(e) => {
            tracing::error!(
                "error authorizing user {:?} to {} {} named {:?} in namespace {:?}: {e}",
                attrs.user.username,
                attrs.verb,
                attrs.resource,
                attrs.name,
                attrs.namespace,
            );
            false
        }
    }
}

/// `IsOnlyMutatingGCFields` (helpers.go:29-50): whether `obj` differs from
/// `old` only in the fields the garbage collector manipulates — ownerReferences
/// and finalizers — plus the fields upstream also stomps (selfLink,
/// managedFields).
pub fn is_only_mutating_gc_fields<T: Serialize>(obj: &T, old: &T) -> bool {
    let (Some(mut obj), Some(mut old)) = (
        serde_json::to_value(obj).ok(),
        serde_json::to_value(old).ok(),
    ) else {
        return false;
    };
    for value in [&mut obj, &mut old] {
        if let Some(meta) = value.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            for field in ["ownerReferences", "finalizers", "selfLink", "managedFields"] {
                meta.remove(field);
            }
        }
    }
    obj == old
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::Role;
    use rusternetes_common::types::OwnerReference;

    fn role() -> Role {
        Role::new("r", "ns")
    }

    /// TestIsOnlyMutatingGCFields (helpers_test.go:43).
    #[test]
    fn only_gc_fields_may_differ() {
        let base = role();
        assert!(is_only_mutating_gc_fields(&base, &base.clone()));

        let mut owner = base.clone();
        owner.metadata.owner_references = Some(vec![OwnerReference {
            api_version: "v1".into(),
            kind: "Pod".into(),
            name: "foo".into(),
            uid: "u".into(),
            controller: None,
            block_owner_deletion: None,
        }]);
        assert!(is_only_mutating_gc_fields(&owner, &base));

        let mut both = owner.clone();
        both.metadata.finalizers = Some(vec!["final".into()]);
        assert!(is_only_mutating_gc_fields(&both, &base));

        let mut annotated = both.clone();
        annotated.metadata.annotations = Some([("foo".to_string(), "bar".to_string())].into());
        assert!(!is_only_mutating_gc_fields(&annotated, &base));

        let mut other = both;
        other.rules = vec![rusternetes_common::resources::PolicyRule {
            verbs: vec!["get".into()],
            api_groups: None,
            resources: None,
            resource_names: None,
            non_resource_urls: None,
        }];
        assert!(!is_only_mutating_gc_fields(&other, &base));
    }

    #[test]
    fn superusers_are_system_masters_members() {
        let mut ctx = RequestContext::new(None);
        assert!(!escalation_allowed(&ctx));
        let user = |groups: &[&str]| rusternetes_common::auth::UserInfo {
            username: "u".into(),
            uid: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            extra: Default::default(),
        };
        ctx = ctx.with_user(&user(&["devs"]));
        assert!(!escalation_allowed(&ctx));
        let ctx = RequestContext::new(None).with_user(&user(&["devs", "system:masters"]));
        assert!(escalation_allowed(&ctx));
    }
}
