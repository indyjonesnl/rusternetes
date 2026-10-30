//! Unit tests moved from `src/registry/rbac/rule.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::registry::rbac::policy_compact::{
        compact_rules, compact_string, quote_strings,
    };
    pub use rusternetes_api_server::registry::rbac::policy_comparator::covers;
    pub use rusternetes_common::auth::UserInfo;
    pub use rusternetes_common::resources::{
        ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, RoleRef, Subject,
    };
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::{build_key, build_prefix, Storage};
    pub use std::collections::BTreeSet;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::rbac::rule::*;
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
