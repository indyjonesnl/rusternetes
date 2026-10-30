//! Port of `pkg/registry/rbac/validation/rule.go`: the rule resolver the
//! policybased storages consult, and `ConfirmNoEscalation`.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::resources::{
    ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, RoleRef, Subject,
};
use rusternetes_common::{Error, Result};
use rusternetes_storage::{build_key, build_prefix, Storage};

use super::policy_compact::{compact_rules, compact_string, quote_strings};
use super::policy_comparator::covers;

/// `AuthorizationRuleResolver` (rule.go:36-50).
#[async_trait]
pub trait AuthorizationRuleResolver: Send + Sync {
    /// `GetRoleReferenceRules`: resolve the role reference of a RoleBinding or
    /// ClusterRoleBinding. `namespace` is the namespace of the binding, the
    /// empty string for a ClusterRoleBinding.
    async fn get_role_reference_rules(
        &self,
        role_ref: &RoleRef,
        namespace: &str,
    ) -> Result<Vec<PolicyRule>>;

    /// `RulesFor`: the rules that apply to a user in a namespace, and the
    /// errors met resolving them. As upstream's contract says, the rules may be
    /// incomplete when there are errors: policy rules are purely additive, so a
    /// determination can be made from those found.
    async fn rules_for(&self, user: &UserInfo, namespace: &str) -> (Vec<PolicyRule>, Vec<String>);
}

/// `ConfirmNoEscalation` (rule.go:53-80): whether the roles of the requester
/// in the request namespace encompass the provided rules. The `Err` is the
/// message upstream wraps in `NewForbidden`.
pub async fn confirm_no_escalation(
    user: Option<&UserInfo>,
    namespace: Option<&str>,
    resolver: &dyn AuthorizationRuleResolver,
    rules: &[PolicyRule],
) -> std::result::Result<(), String> {
    let Some(user) = user else {
        return Err("no user on context".to_string());
    };
    let namespace = namespace.unwrap_or("");

    let (owner_rules, resolution_errors) = resolver.rules_for(user, namespace).await;
    let (owner_rights_cover, missing_rights) = covers(&owner_rules, rules);
    if owner_rights_cover {
        return Ok(());
    }

    let missing_descriptions: BTreeSet<String> = compact_rules(&missing_rights)
        .iter()
        .map(compact_string)
        .collect();
    let mut msg = format!(
        "user {:?} (groups={}) is attempting to grant RBAC permissions not currently held:\n{}",
        user.username,
        quote_strings(&user.groups),
        missing_descriptions
            .into_iter()
            .collect::<Vec<_>>()
            .join("\n")
    );
    if !resolution_errors.is_empty() {
        msg.push_str(&format!(
            "; resolution errors: [{}]",
            resolution_errors.join(", ")
        ));
    }
    Err(msg)
}

/// `DefaultRuleResolver` (rule.go:82-100), reading Roles, RoleBindings,
/// ClusterRoles and ClusterRoleBindings from storage where upstream reads them
/// through listers.
pub struct DefaultRuleResolver<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage> DefaultRuleResolver<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

/// `serviceaccount.MatchesUsername`: whether `username` is the service
/// account `namespace/name`.
fn matches_service_account_username(namespace: &str, name: &str, username: &str) -> bool {
    username
        .strip_prefix("system:serviceaccount:")
        .and_then(|rest| rest.strip_prefix(namespace))
        .and_then(|rest| rest.strip_prefix(':'))
        .is_some_and(|rest| rest == name)
}

/// `appliesToUser` (rule.go:281-304).
pub fn applies_to_user(user: &UserInfo, subject: &Subject, namespace: &str) -> bool {
    match subject.kind.as_str() {
        "User" => user.username == subject.name,
        "Group" => user.groups.contains(&subject.name),
        "ServiceAccount" => {
            // Default the namespace to the namespace we are working in, so
            // bindings that reference service accounts in the local namespace
            // need not qualify them.
            let sa_namespace = match subject.namespace.as_deref() {
                Some(ns) if !ns.is_empty() => ns,
                _ => namespace,
            };
            if sa_namespace.is_empty() {
                return false;
            }
            matches_service_account_username(sa_namespace, &subject.name, &user.username)
        }
        _ => false,
    }
}

/// `appliesTo` (rule.go:264-271).
fn applies_to(user: &UserInfo, subjects: &[Subject], namespace: &str) -> bool {
    subjects
        .iter()
        .any(|subject| applies_to_user(user, subject, namespace))
}

#[async_trait]
impl<S: Storage> AuthorizationRuleResolver for DefaultRuleResolver<S> {
    /// `GetRoleReferenceRules` (rule.go:236-262).
    async fn get_role_reference_rules(
        &self,
        role_ref: &RoleRef,
        binding_namespace: &str,
    ) -> Result<Vec<PolicyRule>> {
        match role_ref.kind.as_str() {
            "Role" => {
                let key = build_key("roles", Some(binding_namespace), &role_ref.name);
                self.storage
                    .get::<Role>(&key)
                    .await
                    .map(|role| role.rules)
                    .map_err(|e| match e {
                        Error::NotFound(_) => Error::NotFound(format!(
                            "role.rbac.authorization.k8s.io \"{}\" not found",
                            role_ref.name
                        )),
                        e => e,
                    })
            }
            "ClusterRole" => {
                let key = build_key("clusterroles", None, &role_ref.name);
                self.storage
                    .get::<ClusterRole>(&key)
                    .await
                    .map(|role| role.rules)
                    .map_err(|e| match e {
                        Error::NotFound(_) => Error::NotFound(format!(
                            "clusterrole.rbac.authorization.k8s.io \"{}\" not found",
                            role_ref.name
                        )),
                        e => e,
                    })
            }
            kind => Err(Error::Internal(format!(
                "unsupported role reference kind: {kind:?}"
            ))),
        }
    }

    /// `RulesFor` → `VisitRulesFor` (rule.go:102-111, 163-234): every
    /// ClusterRoleBinding that applies, then, for a namespaced request, every
    /// RoleBinding of that namespace that applies.
    async fn rules_for(&self, user: &UserInfo, namespace: &str) -> (Vec<PolicyRule>, Vec<String>) {
        let mut rules = Vec::new();
        let mut errors = Vec::new();

        match self
            .storage
            .list::<ClusterRoleBinding>(&build_prefix("clusterrolebindings", None))
            .await
        {
            Err(e) => errors.push(e.to_string()),
            Ok(bindings) => {
                for binding in bindings {
                    if !applies_to(user, &binding.subjects, "") {
                        continue;
                    }
                    match self.get_role_reference_rules(&binding.role_ref, "").await {
                        Ok(r) => rules.extend(r),
                        Err(e) => errors.push(e.to_string()),
                    }
                }
            }
        }

        if !namespace.is_empty() {
            match self
                .storage
                .list::<RoleBinding>(&build_prefix("rolebindings", Some(namespace)))
                .await
            {
                Err(e) => errors.push(e.to_string()),
                Ok(bindings) => {
                    for binding in bindings {
                        if !applies_to(user, &binding.subjects, namespace) {
                            continue;
                        }
                        match self
                            .get_role_reference_rules(&binding.role_ref, namespace)
                            .await
                        {
                            Ok(r) => rules.extend(r),
                            Err(e) => errors.push(e.to_string()),
                        }
                    }
                }
            }
        }
        (rules, errors)
    }
}
