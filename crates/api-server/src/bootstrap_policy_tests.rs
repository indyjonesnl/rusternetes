//! Ported from `plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy_test.go`
//! (release-1.35): the policy-invariant tests that do not compare against a
//! vendored fixture -- `TestEditViewRelationship` (:86-118),
//! `TestClusterRoleLabel` (:287-312), `TestNodeRuleVerbsConsistency` (:314-322),
//! `TestClusterRoleVerbsConsistency` (:324-334) and
//! `TestNamespaceRoleVerbsConsistency` (:336-348).
//!
//! The fixture-equality tests (`TestBootstrap*Roles*`, `testObjects`) are
//! ported in `bootstrap.rs` (`*_match_upstream_testdata`).

use super::*;
use crate::registry::rbac::policy_comparator::covers;
use rusternetes_common::resources::PolicyRule;

/// `ClusterRoles()`: every ClusterRole except `system:controller:*`
/// (`ControllerRoles()`).
fn cluster_roles() -> Vec<serde_json::Value> {
    bootstrap_policy_items()
        .unwrap()
        .into_iter()
        .filter(|i| {
            i["kind"] == "ClusterRole"
                && !i["metadata"]["name"]
                    .as_str()
                    .unwrap()
                    .starts_with("system:controller:")
        })
        .collect()
}

fn rules_of(role: &serde_json::Value) -> Vec<PolicyRule> {
    serde_json::from_value(role["rules"].clone()).unwrap_or_default()
}

fn rule(groups: &[&str], resources: &[&str]) -> PolicyRule {
    PolicyRule {
        // `bootstrappolicy.Read` (policy.go:39).
        verbs: vec!["get".into(), "list".into(), "watch".into()],
        api_groups: Some(groups.iter().map(|s| s.to_string()).collect()),
        resources: Some(resources.iter().map(|s| s.to_string()).collect()),
        resource_names: None,
        non_resource_urls: None,
    }
}

/// `list` without `watch` in any rule of `rules` (the three
/// `Test*VerbsConsistency` assertions).
fn list_without_watch(rules: &[PolicyRule]) -> bool {
    rules
        .iter()
        .any(|r| r.verbs.iter().any(|v| v == "list") && !r.verbs.iter().any(|v| v == "watch"))
}

/// `TestEditViewRelationship`.
#[test]
#[serial_test::serial]
fn view_role_has_no_escalating_or_ungettable_powers() {
    rusternetes_common::feature_gates::reset_to_defaults();
    let roles = cluster_roles();
    let view = roles
        .iter()
        .find(|r| r["metadata"]["name"] == "system:aggregate-to-view")
        .expect("system:aggregate-to-view");
    let view_rules = rules_of(view);

    // viewEscalatingNamespaceResources (policy_test.go:68-75): rules that
    // would allow privilege escalation based on the ability to view (GET) them.
    let escalating = [
        rule(&[""], &["pods/attach"]),
        rule(&[""], &["pods/proxy"]),
        rule(&[""], &["pods/exec"]),
        rule(&[""], &["pods/portforward"]),
        rule(&[""], &["secrets"]),
        rule(&[""], &["services/proxy"]),
    ];
    for r in &escalating {
        assert!(
            !covers(&view_rules, std::slice::from_ref(r)).0,
            "view has extra powers: {r:?}"
        );
    }

    // ungettableResources (policy_test.go:79-81).
    let ungettable = [rule(&["apps", "extensions"], &["deployments/rollback"])];
    for r in &ungettable {
        assert!(
            !covers(&view_rules, std::slice::from_ref(r)).0,
            "view has ungettable resource: {r:?}"
        );
    }
}

/// `TestClusterRoleLabel`.
#[test]
#[serial_test::serial]
fn cluster_roles_and_bindings_carry_the_bootstrapping_label() {
    rusternetes_common::feature_gates::reset_to_defaults();
    for role in cluster_roles() {
        assert_eq!(
            role["metadata"]["labels"]["kubernetes.io/bootstrapping"], "rbac-defaults",
            "ClusterRole {}",
            role["metadata"]["name"]
        );
    }

    // `ClusterRoleBindings()` excludes `ControllerRoleBindings()`; the labels
    // must be exactly the one entry (reflect.DeepEqual, policy_test.go:305).
    let want = serde_json::json!({"kubernetes.io/bootstrapping": "rbac-defaults"});
    for b in bootstrap_policy_items().unwrap().into_iter().filter(|i| {
        i["kind"] == "ClusterRoleBinding"
            && !i["metadata"]["name"]
                .as_str()
                .unwrap()
                .starts_with("system:controller:")
    }) {
        assert_eq!(
            b["metadata"]["labels"], want,
            "ClusterRoleBinding {}",
            b["metadata"]["name"]
        );
    }
}

/// `TestNodeRuleVerbsConsistency`: `NodeRules()` are the rules of the
/// `system:node` ClusterRole (policy.go `ClusterRoles()`).
#[test]
#[serial_test::serial]
fn node_rules_list_implies_watch() {
    rusternetes_common::feature_gates::reset_to_defaults();
    let roles = cluster_roles();
    let node = roles
        .iter()
        .find(|r| r["metadata"]["name"] == "system:node")
        .expect("system:node");
    assert!(!list_without_watch(&rules_of(node)));
}

/// `TestClusterRoleVerbsConsistency`.
#[test]
#[serial_test::serial]
fn cluster_role_rules_list_implies_watch() {
    rusternetes_common::feature_gates::reset_to_defaults();
    for role in cluster_roles() {
        assert!(
            !list_without_watch(&rules_of(&role)),
            "The ClusterRole {} has Verb `List` but does not have Verb `Watch`.",
            role["metadata"]["name"]
        );
    }
}

/// `TestNamespaceRoleVerbsConsistency`.
#[test]
#[serial_test::serial]
fn namespace_role_rules_list_implies_watch() {
    rusternetes_common::feature_gates::reset_to_defaults();
    let roles: Vec<_> = bootstrap_policy_items()
        .unwrap()
        .into_iter()
        .filter(|i| i["kind"] == "Role")
        .collect();
    assert!(!roles.is_empty(), "namespace roles must be seeded");
    for role in roles {
        assert!(
            !list_without_watch(&rules_of(&role)),
            "The Role {}/{} has Verb `List` but does not have Verb `Watch`.",
            role["metadata"]["namespace"],
            role["metadata"]["name"]
        );
    }
}

/// Mutation guard: the helper really flags a list-without-watch rule.
#[test]
fn list_without_watch_detects_the_violation() {
    let mut r = rule(&[""], &["pods"]);
    r.verbs = vec!["list".into()];
    assert!(list_without_watch(&[r]));
}

/// `TestBootstrapClusterRoleBindings`, `TestBootstrapNamespaceRoles` and
/// `TestBootstrapNamespaceRoleBindings` (`policy_test.go:120-216`): with the
/// default gates the generated objects equal the vendored fixtures.
#[test]
#[serial_test::serial]
fn bindings_and_namespace_objects_match_upstream_testdata() {
    rusternetes_common::feature_gates::reset_to_defaults();
    let is_controller = |i: &serde_json::Value| {
        i["metadata"]["name"]
            .as_str()
            .unwrap()
            .starts_with("system:controller:")
    };
    for (kind, yaml) in [
        (
            "ClusterRoleBinding",
            include_str!("bootstrap_policy/cluster-role-bindings.yaml"),
        ),
        (
            "Role",
            include_str!("bootstrap_policy/namespace-roles.yaml"),
        ),
        (
            "RoleBinding",
            include_str!("bootstrap_policy/namespace-role-bindings.yaml"),
        ),
    ] {
        let key = |i: &serde_json::Value| {
            (
                i["metadata"]["namespace"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                i["metadata"]["name"].as_str().unwrap().to_string(),
            )
        };
        let mut got: Vec<_> = bootstrap_policy_items()
            .unwrap()
            .into_iter()
            .filter(|i| i["kind"] == kind && !(kind == "ClusterRoleBinding" && is_controller(i)))
            .collect();
        got.sort_by_key(key);
        let mut want = load_policy_items(yaml).unwrap();
        want.sort_by_key(key);
        assert_eq!(got, want, "{kind}");
    }
}
