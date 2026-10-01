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
fn applies_to_user(user: &UserInfo, subject: &Subject, namespace: &str) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::memory::MemoryStorage;

    fn user(name: &str, groups: &[&str]) -> UserInfo {
        UserInfo {
            username: name.to_string(),
            uid: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            extra: Default::default(),
        }
    }

    fn pods(verbs: &[&str]) -> PolicyRule {
        PolicyRule {
            verbs: verbs.iter().map(|v| v.to_string()).collect(),
            api_groups: Some(vec![String::new()]),
            resources: Some(vec!["pods".into()]),
            resource_names: None,
            non_resource_urls: None,
        }
    }

    /// TestAppliesTo (rule_test.go:166).
    #[test]
    fn subjects_apply_by_kind() {
        let u = user("alice", &["devs"]);
        let s = |kind: &str, name: &str, ns: Option<&str>| Subject {
            kind: kind.into(),
            name: name.into(),
            namespace: ns.map(str::to_string),
            api_group: None,
        };
        assert!(applies_to_user(&u, &s("User", "alice", None), ""));
        assert!(!applies_to_user(&u, &s("User", "bob", None), ""));
        assert!(applies_to_user(&u, &s("Group", "devs", None), ""));
        assert!(!applies_to_user(&u, &s("Group", "ops", None), ""));

        let sa = user("system:serviceaccount:ns1:builder", &[]);
        assert!(applies_to_user(
            &sa,
            &s("ServiceAccount", "builder", Some("ns1")),
            ""
        ));
        // A subject without a namespace takes the binding's.
        assert!(applies_to_user(
            &sa,
            &s("ServiceAccount", "builder", None),
            "ns1"
        ));
        assert!(!applies_to_user(
            &sa,
            &s("ServiceAccount", "builder", None),
            "ns2"
        ));
        // ... and with neither there is nothing to match.
        assert!(!applies_to_user(
            &sa,
            &s("ServiceAccount", "builder", None),
            ""
        ));
        assert!(!applies_to_user(
            &sa,
            &s("ServiceAccount", "build", Some("ns1")),
            ""
        ));
        assert!(!applies_to_user(&sa, &s("Robot", "builder", None), ""));
    }

    /// TestDefaultRuleResolver (rule_test.go:54): cluster bindings apply
    /// everywhere, role bindings only in their namespace.
    #[tokio::test]
    async fn rules_come_from_cluster_bindings_and_the_namespaces_bindings() {
        let storage = Arc::new(MemoryStorage::new());
        let mk_cr = |name: &str, verbs: &[&str]| ClusterRole {
            metadata: ObjectMeta::new(name),
            rules: vec![pods(verbs)],
            ..ClusterRole::new(name)
        };
        storage
            .create(
                &build_key("clusterroles", None, "cr"),
                &mk_cr("cr", &["get"]),
            )
            .await
            .unwrap();
        let alice = vec![Subject::user("alice")];
        let crb = ClusterRoleBinding::new("crb")
            .with_role_ref(RoleRef::cluster_role("cr"))
            .with_subjects(alice.clone());
        storage
            .create(&build_key("clusterrolebindings", None, "crb"), &crb)
            .await
            .unwrap();
        let rb = RoleBinding::new("rb", "ns1")
            .with_role_ref(RoleRef::cluster_role("cr"))
            .with_subjects(alice);
        storage
            .create(&build_key("rolebindings", Some("ns1"), "rb"), &rb)
            .await
            .unwrap();

        let resolver = DefaultRuleResolver::new(storage);
        let alice = user("alice", &[]);
        assert_eq!(resolver.rules_for(&alice, "").await.0.len(), 1);
        assert_eq!(resolver.rules_for(&alice, "ns1").await.0.len(), 2);
        assert_eq!(resolver.rules_for(&alice, "ns2").await.0.len(), 1);
        assert!(resolver
            .rules_for(&user("bob", &[]), "ns1")
            .await
            .0
            .is_empty());
    }

    #[tokio::test]
    async fn a_missing_role_is_an_error_not_an_empty_rule_set() {
        let resolver = DefaultRuleResolver::new(Arc::new(MemoryStorage::new()));
        let err = resolver
            .get_role_reference_rules(&RoleRef::role("nope"), "ns1")
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::NotFound(m) if m.contains("role.rbac.authorization.k8s.io \"nope\" not found"))
        );
        let err = resolver
            .get_role_reference_rules(
                &RoleRef {
                    kind: "Pod".into(),
                    ..RoleRef::role("x")
                },
                "ns1",
            )
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("unsupported role reference kind: \"Pod\""));
    }

    #[tokio::test]
    async fn confirm_no_escalation_names_the_missing_rules() {
        let resolver = DefaultRuleResolver::new(Arc::new(MemoryStorage::new()));
        let alice = user("alice", &["g1"]);
        let msg = confirm_no_escalation(
            Some(&alice),
            Some("ns1"),
            &resolver,
            &[pods(&["get", "list"])],
        )
        .await
        .unwrap_err();
        assert_eq!(
            msg,
            "user \"alice\" (groups=[\"g1\"]) is attempting to grant RBAC permissions not currently held:\n\
             {APIGroups:[\"\"], Resources:[\"pods\"], Verbs:[\"get\" \"list\"]}"
        );
        assert!(
            confirm_no_escalation(Some(&alice), Some("ns1"), &resolver, &[])
                .await
                .is_ok()
        );
        assert_eq!(
            confirm_no_escalation(None, None, &resolver, &[])
                .await
                .unwrap_err(),
            "no user on context"
        );
    }
}
