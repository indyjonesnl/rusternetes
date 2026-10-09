//! Match a request against a policy's `matchConstraints` and a binding's
//! `matchResources`, shared by MutatingAdmissionPolicy (#2731) and, later,
//! ValidatingAdmissionPolicy.
//!
//! Ported from, in `staging/src/k8s.io/apiserver/pkg/admission/plugin/`:
//! - `webhook/predicates/rules/rules.go` (`rules.Matcher`)
//! - `webhook/predicates/namespace/matcher.go` (`MatchNamespaceSelector`)
//! - `webhook/predicates/object/matcher.go` (`MatchObjectSelector`)
//! - `policy/matching/matching.go` (`Matcher.Matches`, `matchesResourceRules`)
//! - `policy/generic/policy_matcher.go` (`DefinitionMatches`, `BindingMatches`)
//!
//! Tests are ported from `policy/matching/matching_test.go` (`TestMatcher`)
//! and `rules/rules_test.go`.

use std::collections::HashMap;

use async_trait::async_trait;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
use rusternetes_common::resources::validating_admission_policy::{
    MatchPolicyType, MatchResources, NamedRuleWithOperations, OperationType, RuleWithOperations,
};
use rusternetes_common::types::label_selector_as_selector;
use rusternetes_common::{Error, Result};
use serde_json::Value;

/// The part of `admission.Attributes` the matchers read.
#[derive(Debug, Clone)]
pub struct Attributes {
    pub kind: GroupVersionKind,
    pub resource: GroupVersionResource,
    pub subresource: String,
    /// `""` for a cluster-scoped object. For a request on a Namespace itself
    /// this is the namespace's own name.
    pub namespace: String,
    pub name: String,
    pub operation: Operation,
    pub object: Option<Value>,
    pub old_object: Option<Value>,
}

/// `runtime.EquivalentResourceMapper`.
pub trait EquivalentResourceMapper: Send + Sync {
    /// `EquivalentResourcesFor`: every resource that shares storage with
    /// `resource` for `subresource`, in registration order (the resource
    /// itself included).
    fn equivalent_resources_for(
        &self,
        resource: &GroupVersionResource,
        subresource: &str,
    ) -> Vec<GroupVersionResource>;

    /// `KindFor`; an all-empty kind is upstream's "unknown".
    fn kind_for(&self, resource: &GroupVersionResource, subresource: &str) -> GroupVersionKind;
}

/// `NamespaceLister.Get`; a missing namespace is `Error::NotFound`.
#[async_trait]
pub trait NamespaceLister: Send + Sync {
    async fn namespace_labels(&self, name: &str) -> Result<HashMap<String, String>>;
}

/// What `Matcher::matches` reports for a request that matches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    pub resource: GroupVersionResource,
    pub kind: GroupVersionKind,
}

fn rule_matches(_rule: &RuleWithOperations, _attr: &Attributes) -> bool {
    unimplemented!()
}

pub struct Matcher<'a> {
    pub namespaces: &'a dyn NamespaceLister,
    pub mapper: &'a dyn EquivalentResourceMapper,
}

impl Matcher<'_> {
    /// `matching.Matcher.Matches`.
    pub async fn matches(
        &self,
        _attr: &Attributes,
        _criteria: &MatchResources,
    ) -> Result<Option<Match>> {
        unimplemented!()
    }

    /// `generic.matcher.DefinitionMatches`.
    pub async fn definition_matches(
        &self,
        _attr: &Attributes,
        _constraints: Option<&MatchResources>,
    ) -> Result<Option<Match>> {
        unimplemented!()
    }

    /// `generic.matcher.BindingMatches`.
    pub async fn binding_matches(
        &self,
        _attr: &Attributes,
        _match_resources: Option<&MatchResources>,
    ) -> Result<bool> {
        unimplemented!()
    }
}

#[allow(dead_code)]
fn _unused(_: &NamedRuleWithOperations, _: &MatchPolicyType, _: &OperationType, _: Error) {
    let _ = label_selector_as_selector;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        LabelSelector, LabelSelectorOperator, LabelSelectorRequirement,
    };

    fn gvr(g: &str, v: &str, r: &str) -> GroupVersionResource {
        GroupVersionResource {
            group: g.into(),
            version: v.into(),
            resource: r.into(),
        }
    }
    fn gvk(g: &str, v: &str, k: &str) -> GroupVersionKind {
        GroupVersionKind {
            group: g.into(),
            version: v.into(),
            kind: k.into(),
        }
    }

    /// `runtime.NewEquivalentResourceRegistryWithIdentity` as `TestMatcher`
    /// builds it: deployments are co-located across API groups.
    #[derive(Default)]
    struct Mapper {
        entries: Vec<(GroupVersionResource, String, GroupVersionKind)>,
    }
    impl Mapper {
        fn identity(r: &GroupVersionResource) -> String {
            if r.resource == "deployments" {
                "/deployments".into()
            } else {
                format!("{}/{}", r.group, r.resource)
            }
        }
        fn register(&mut self, r: GroupVersionResource, sub: &str, k: GroupVersionKind) {
            self.entries.push((r, sub.into(), k));
        }
    }
    impl EquivalentResourceMapper for Mapper {
        fn equivalent_resources_for(
            &self,
            resource: &GroupVersionResource,
            subresource: &str,
        ) -> Vec<GroupVersionResource> {
            let id = Self::identity(resource);
            let mut out: Vec<GroupVersionResource> = Vec::new();
            for (r, sub, _) in &self.entries {
                if sub == subresource && Self::identity(r) == id && !out.contains(r) {
                    out.push(r.clone());
                }
            }
            out
        }
        fn kind_for(&self, resource: &GroupVersionResource, subresource: &str) -> GroupVersionKind {
            self.entries
                .iter()
                .find(|(r, s, _)| r == resource && s == subresource)
                .map(|(_, _, k)| k.clone())
                .unwrap_or_else(|| gvk("", "", ""))
        }
    }

    struct NoNamespaces;
    #[async_trait]
    impl NamespaceLister for NoNamespaces {
        async fn namespace_labels(&self, name: &str) -> Result<HashMap<String, String>> {
            Err(Error::NotFound(format!("namespaces \"{name}\" not found")))
        }
    }

    fn mapper() -> Mapper {
        let mut m = Mapper::default();
        for (g, v) in [
            ("extensions", "v1beta1"),
            ("apps", "v1"),
            ("apps", "v1beta1"),
            ("apps", "v1alpha1"),
        ] {
            m.register(gvr(g, v, "deployments"), "", gvk(g, v, "Deployment"));
        }
        m.register(
            gvr("extensions", "v1beta1", "deployments"),
            "scale",
            gvk("extensions", "v1beta1", "Scale"),
        );
        m.register(
            gvr("apps", "v1", "deployments"),
            "scale",
            gvk("autoscaling", "v1", "Scale"),
        );
        m.register(
            gvr("apps", "v1beta1", "deployments"),
            "scale",
            gvk("apps", "v1beta1", "Scale"),
        );
        m.register(
            gvr("apps", "v1alpha1", "deployments"),
            "scale",
            gvk("apps", "v1alpha1", "Scale"),
        );
        m
    }

    fn attrs(
        kind: GroupVersionKind,
        ns: &str,
        name: &str,
        resource: GroupVersionResource,
        sub: &str,
        op: Operation,
    ) -> Attributes {
        Attributes {
            kind,
            resource,
            subresource: sub.into(),
            namespace: ns.into(),
            name: name.into(),
            operation: op,
            object: None,
            old_object: None,
        }
    }

    fn strs(v: &[&str]) -> Option<Vec<String>> {
        Some(v.iter().map(|s| s.to_string()).collect())
    }

    fn rule(groups: &[&str], versions: &[&str], resources: &[&str]) -> NamedRuleWithOperations {
        NamedRuleWithOperations {
            resource_names: None,
            rule: RuleWithOperations {
                operations: Some(vec![OperationType::All]),
                api_groups: strs(groups),
                api_versions: strs(versions),
                resources: strs(resources),
                scope: Some("*".into()),
            },
        }
    }
    fn named(mut r: NamedRuleWithOperations, names: &[&str]) -> NamedRuleWithOperations {
        r.resource_names = strs(names);
        r
    }

    fn mr(policy: Option<MatchPolicyType>, rules: Vec<NamedRuleWithOperations>) -> MatchResources {
        MatchResources {
            namespace_selector: Some(LabelSelector::default()),
            object_selector: Some(LabelSelector::default()),
            resource_rules: Some(rules),
            exclude_resource_rules: None,
            match_policy: policy,
        }
    }

    struct Case {
        name: &'static str,
        criteria: MatchResources,
        attrs: Attributes,
        expect_matches: bool,
        expect_kind: Option<GroupVersionKind>,
        expect_resource: Option<GroupVersionResource>,
        expect_err: &'static str,
    }

    fn case(
        name: &'static str,
        criteria: MatchResources,
        attrs: Attributes,
        expect_matches: bool,
    ) -> Case {
        Case {
            name,
            criteria,
            attrs,
            expect_matches,
            expect_kind: None,
            expect_resource: None,
            expect_err: "",
        }
    }
    impl Case {
        fn kind(mut self, k: GroupVersionKind) -> Self {
            self.expect_kind = Some(k);
            self
        }
        fn resource(mut self, r: GroupVersionResource) -> Self {
            self.expect_resource = Some(r);
            self
        }
        fn err(mut self, e: &'static str) -> Self {
            self.expect_err = e;
            self
        }
    }

    fn deploy_attrs() -> Attributes {
        attrs(
            gvk("apps", "v1", "Deployment"),
            "ns",
            "name",
            gvr("apps", "v1", "deployments"),
            "",
            Operation::Create,
        )
    }
    fn scale_attrs(res: GroupVersionResource, sub: &str) -> Attributes {
        attrs(
            gvk("autoscaling", "v1", "Scale"),
            "ns",
            "name",
            res,
            sub,
            Operation::Create,
        )
    }
    fn bad_selector(key: &str) -> LabelSelector {
        LabelSelector {
            match_labels: None,
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: key.into(),
                operator: LabelSelectorOperator::In,
                values: Some(vec!["bad value".into()]),
            }]),
        }
    }
    fn pod_attrs() -> Attributes {
        let mut a = attrs(
            gvk("example.apiserver.k8s.io", "v1", "Pod"),
            "ns",
            "name",
            gvr("example.apiserver.k8s.io", "v1", "pods"),
            "",
            Operation::Create,
        );
        a.object = Some(serde_json::json!({"metadata": {}}));
        a
    }

    /// `TestMatcher` (matching_test.go:75-560).
    fn cases() -> Vec<Case> {
        use MatchPolicyType::{Equivalent, Exact};
        let dep = |g, v| rule(&[g], &[v], &["deployments"]);
        let dep_scale = |g, v| rule(&[g], &[v], &["deployments", "deployments/scale"]);
        let mut out = vec![];

        let mut no_rules = mr(None, vec![]);
        no_rules.object_selector = None;
        out.push(case(
            "no rules (just write)",
            no_rules,
            deploy_attrs(),
            false,
        ));
        out.push(
            case(
                "wildcard rule, match as requested",
                mr(None, vec![rule(&["*"], &["*"], &["*"])]),
                deploy_attrs(),
                true,
            )
            .kind(gvk("apps", "v1", "Deployment")),
        );
        out.push(
            case(
                "specific rules, prefer exact match",
                mr(
                    None,
                    vec![
                        dep("extensions", "v1"),
                        dep("apps", "v1beta1"),
                        dep("apps", "v1"),
                    ],
                ),
                deploy_attrs(),
                true,
            )
            .kind(gvk("apps", "v1", "Deployment")),
        );
        out.push(case(
            "specific rules, match miss",
            mr(
                None,
                vec![dep("extensions", "v1beta1"), dep("apps", "v1beta1")],
            ),
            deploy_attrs(),
            false,
        ));
        out.push(case(
            "specific rules, exact match miss",
            mr(
                Some(Exact),
                vec![dep("extensions", "v1beta1"), dep("apps", "v1beta1")],
            ),
            deploy_attrs(),
            false,
        ));
        out.push(
            case(
                "specific rules, equivalent match, prefer extensions",
                mr(
                    Some(Equivalent),
                    vec![dep("extensions", "v1beta1"), dep("apps", "v1beta1")],
                ),
                deploy_attrs(),
                true,
            )
            .resource(gvr("extensions", "v1beta1", "deployments"))
            .kind(gvk("extensions", "v1beta1", "Deployment")),
        );
        out.push(
            case(
                "specific rules, equivalent match, prefer apps",
                mr(
                    Some(Equivalent),
                    vec![dep("apps", "v1beta1"), dep("extensions", "v1beta1")],
                ),
                deploy_attrs(),
                true,
            )
            .resource(gvr("apps", "v1beta1", "deployments"))
            .kind(gvk("apps", "v1beta1", "Deployment")),
        );
        out.push(
            case(
                "specific rules, subresource prefer exact match",
                mr(
                    None,
                    vec![
                        dep_scale("extensions", "v1beta1"),
                        dep_scale("apps", "v1beta1"),
                        dep_scale("apps", "v1"),
                    ],
                ),
                scale_attrs(gvr("apps", "v1", "deployments"), "scale"),
                true,
            )
            .kind(gvk("autoscaling", "v1", "Scale")),
        );
        out.push(case(
            "specific rules, subresource match miss",
            mr(
                None,
                vec![
                    dep_scale("extensions", "v1beta1"),
                    dep_scale("apps", "v1beta1"),
                ],
            ),
            scale_attrs(gvr("apps", "v1", "deployments"), "scale"),
            false,
        ));
        out.push(case(
            "specific rules, subresource exact match miss",
            mr(
                Some(Exact),
                vec![
                    dep_scale("extensions", "v1beta1"),
                    dep_scale("apps", "v1beta1"),
                ],
            ),
            scale_attrs(gvr("apps", "v1", "deployments"), "scale"),
            false,
        ));
        out.push(
            case(
                "specific rules, subresource equivalent match, prefer extensions",
                mr(
                    Some(Equivalent),
                    vec![
                        dep_scale("extensions", "v1beta1"),
                        dep_scale("apps", "v1beta1"),
                    ],
                ),
                scale_attrs(gvr("apps", "v1", "deployments"), "scale"),
                true,
            )
            .resource(gvr("extensions", "v1beta1", "deployments"))
            .kind(gvk("extensions", "v1beta1", "Scale")),
        );
        out.push(
            case(
                "specific rules, subresource equivalent match, prefer apps",
                mr(
                    Some(Equivalent),
                    vec![
                        dep_scale("apps", "v1beta1"),
                        dep_scale("extensions", "v1beta1"),
                    ],
                ),
                scale_attrs(gvr("apps", "v1", "deployments"), "scale"),
                true,
            )
            .resource(gvr("apps", "v1beta1", "deployments"))
            .kind(gvk("apps", "v1beta1", "Scale")),
        );
        out.push(
            case(
                "specific rules, prefer exact match and name match",
                mr(None, vec![named(dep("apps", "v1"), &["name"])]),
                scale_attrs(gvr("apps", "v1", "deployments"), ""),
                true,
            )
            .kind(gvk("autoscaling", "v1", "Scale")),
        );
        out.push(case(
            "specific rules, prefer exact match and name match miss",
            mr(None, vec![named(dep("apps", "v1"), &["wrong-name"])]),
            scale_attrs(gvr("apps", "v1", "deployments"), ""),
            false,
        ));
        out.push(
            case(
                "specific rules, subresource equivalent match, prefer extensions and name match",
                mr(
                    Some(Equivalent),
                    vec![named(dep_scale("apps", "v1"), &["name"])],
                ),
                scale_attrs(gvr("extensions", "v1beta1", "deployments"), "scale"),
                true,
            )
            .resource(gvr("apps", "v1", "deployments"))
            .kind(gvk("autoscaling", "v1", "Scale")),
        );
        out.push(case(
            "specific rules, subresource equivalent match, prefer extensions and name match miss",
            mr(
                Some(Equivalent),
                vec![named(dep_scale("apps", "v1"), &["wrong-name"])],
            ),
            scale_attrs(gvr("extensions", "v1beta1", "deployments"), "scale"),
            false,
        ));

        let mut c = mr(None, vec![rule(&["*"], &["*"], &["*"])]);
        c.exclude_resource_rules = Some(vec![dep("extensions", "v1beta1")]);
        out.push(
            case(
                "exclude resource match on miss",
                c.clone(),
                scale_attrs(gvr("apps", "v1", "deployments"), ""),
                true,
            )
            .kind(gvk("autoscaling", "v1", "Scale")),
        );
        out.push(case(
            "exclude resource miss on match",
            c,
            scale_attrs(gvr("extensions", "v1beta1", "deployments"), ""),
            false,
        ));

        let mut c = mr(None, vec![]);
        c.resource_rules = None;
        c.exclude_resource_rules = Some(vec![dep("extensions", "v1beta1")]);
        out.push(case(
            "treat empty ResourceRules as match",
            c,
            scale_attrs(gvr("apps", "v1", "deployments"), ""),
            true,
        ));
        out.push(case(
            "treat non-empty ResourceRules as no match",
            mr(
                None,
                vec![NamedRuleWithOperations {
                    resource_names: None,
                    rule: RuleWithOperations {
                        operations: None,
                        api_groups: None,
                        api_versions: None,
                        resources: None,
                        scope: None,
                    },
                }],
            ),
            scale_attrs(gvr("apps", "v1", "deployments"), ""),
            false,
        ));

        let wild = |res: &str| {
            let mut r = rule(&["*"], &["*"], &[res]);
            r.rule.scope = None;
            r
        };
        let mut c = mr(None, vec![wild("deployments")]);
        c.namespace_selector = Some(bad_selector("key "));
        out.push(case(
            "erroring namespace selector on otherwise non-matching rule doesn't error",
            c,
            pod_attrs(),
            false,
        ));
        let mut c = mr(None, vec![wild("pods")]);
        c.namespace_selector = Some(bad_selector("key"));
        out.push(
            case(
                "erroring namespace selector on otherwise matching rule errors",
                c,
                pod_attrs(),
                false,
            )
            .err("bad value"),
        );
        let mut c = mr(None, vec![wild("deployments")]);
        c.object_selector = Some(bad_selector("key"));
        out.push(case(
            "erroring object selector on otherwise non-matching rule doesn't error",
            c,
            pod_attrs(),
            false,
        ));
        let mut c = mr(None, vec![wild("pods")]);
        c.object_selector = Some(bad_selector("key"));
        out.push(
            case(
                "erroring object selector on otherwise matching rule errors",
                c,
                pod_attrs(),
                false,
            )
            .err("bad value"),
        );
        out
    }

    #[tokio::test]
    async fn upstream_test_matcher() {
        let mapper = mapper();
        let m = Matcher {
            namespaces: &NoNamespaces,
            mapper: &mapper,
        };
        for tc in cases() {
            match m.matches(&tc.attrs, &tc.criteria).await {
                Err(e) => {
                    assert!(
                        !tc.expect_err.is_empty(),
                        "{}: unexpected error {e}",
                        tc.name
                    );
                    assert!(
                        e.to_string().contains(tc.expect_err),
                        "{}: expected error containing {:?}, got {e}",
                        tc.name,
                        tc.expect_err
                    );
                }
                Ok(got) => {
                    assert!(
                        tc.expect_err.is_empty(),
                        "{}: expected error {:?}, got none",
                        tc.name,
                        tc.expect_err
                    );
                    assert_eq!(got.is_some(), tc.expect_matches, "{}: matches", tc.name);
                    if let Some(got) = got {
                        if let Some(k) = &tc.expect_kind {
                            assert_eq!(&got.kind, k, "{}: kind", tc.name);
                        }
                        // Exact match by default; an equivalent one only when
                        // the case says so (matching_test.go:753-762).
                        let want = tc
                            .expect_resource
                            .clone()
                            .unwrap_or_else(|| tc.attrs.resource.clone());
                        assert_eq!(got.resource, want, "{}: resource", tc.name);
                    }
                }
            }
        }
    }

    // ---- rules_test.go --------------------------------------------------

    fn ra(g: &str, v: &str, r: &str, sub: &str, op: Operation, ns: &str) -> Attributes {
        attrs(
            gvk(g, v, &format!("k{r}")),
            ns,
            "name",
            gvr(g, v, r),
            sub,
            op,
        )
    }
    fn r_with(f: impl FnOnce(&mut RuleWithOperations)) -> RuleWithOperations {
        let mut r = RuleWithOperations {
            operations: Some(vec![OperationType::All]),
            api_groups: strs(&["*"]),
            api_versions: strs(&["*"]),
            resources: strs(&["*"]),
            scope: None,
        };
        f(&mut r);
        r
    }

    #[test]
    fn upstream_test_group() {
        let a = |g: &str| ra(g, "v", "r", "", Operation::Create, "ns");
        let wild = r_with(|_| {});
        assert!(rule_matches(&wild, &a("g")));
        let exact = r_with(|r| r.api_groups = strs(&["g1", "g2"]));
        assert!(rule_matches(&exact, &a("g1")));
        assert!(rule_matches(&exact, &a("g2")));
        assert!(!rule_matches(&exact, &a("g3")));
        assert!(!rule_matches(&exact, &a("g4")));
    }

    #[test]
    fn upstream_test_version() {
        let a = |g: &str, v: &str| ra(g, v, "r", "", Operation::Create, "ns");
        let exact = r_with(|r| r.api_versions = strs(&["v1", "v2"]));
        assert!(rule_matches(&exact, &a("g1", "v1")));
        assert!(rule_matches(&exact, &a("g2", "v2")));
        assert!(!rule_matches(&exact, &a("g1", "v3")));
        assert!(!rule_matches(&exact, &a("g2", "v4")));
        assert!(rule_matches(&r_with(|_| {}), &a("g", "v")));
    }

    #[test]
    fn upstream_test_operation() {
        use Operation::*;
        let all = [Create, Update, Delete, Connect];
        let run = |ops: Vec<OperationType>, want: &[Operation]| {
            let rule = r_with(|r| r.operations = Some(ops));
            for op in all.clone() {
                assert_eq!(
                    rule_matches(&rule, &ra("g", "v", "r", "", op.clone(), "ns")),
                    want.contains(&op),
                    "{op:?}"
                );
            }
        };
        run(vec![OperationType::All], &all);
        run(vec![OperationType::Create], &[Create]);
        run(vec![OperationType::Update], &[Update]);
        run(vec![OperationType::Delete], &[Delete]);
        run(vec![OperationType::Connect], &[Connect]);
        run(
            vec![OperationType::Update, OperationType::Delete],
            &[Update, Delete],
        );
    }

    #[test]
    fn upstream_test_resource() {
        let with = |res: &[&str]| r_with(|r| r.resources = strs(res));
        let a = |g: &str, r: &str, sub: &str| ra(g, "v", r, sub, Operation::Create, "ns");
        let rule = with(&["*"]);
        assert!(rule_matches(&rule, &a("g", "r", "")));
        assert!(rule_matches(&rule, &a("2", "r2", "")));
        assert!(!rule_matches(&rule, &a("g", "r", "exec")));
        assert!(!rule_matches(&rule, &a("2", "r2", "proxy")));
        let rule = with(&["r/*"]);
        assert!(rule_matches(&rule, &a("g", "r", "")));
        assert!(rule_matches(&rule, &a("g", "r", "exec")));
        assert!(!rule_matches(&rule, &a("2", "r2", "")));
        assert!(!rule_matches(&rule, &a("2", "r2", "proxy")));
        let rule = with(&["r/*", "r2"]);
        assert!(rule_matches(&rule, &a("g", "r", "")));
        assert!(rule_matches(&rule, &a("g", "r", "exec")));
        assert!(rule_matches(&rule, &a("2", "r2", "")));
        assert!(!rule_matches(&rule, &a("2", "r2", "proxy")));
        let rule = with(&["*/proxy", "*/exec"]);
        assert!(rule_matches(&rule, &a("g", "r", "exec")));
        assert!(rule_matches(&rule, &a("2", "r2", "proxy")));
        assert!(rule_matches(&rule, &a("2", "r3", "proxy")));
        assert!(!rule_matches(&rule, &a("g", "r", "")));
        assert!(!rule_matches(&rule, &a("2", "r2", "")));
        assert!(!rule_matches(&rule, &a("2", "r4", "scale")));
    }

    #[test]
    fn upstream_test_scope() {
        let with = |scope: &str| r_with(|r| r.scope = Some(scope.into()));
        let ns = |g, v, r, sub| ra(g, v, r, sub, Operation::Create, "ns");
        let cl = |g, v, r, sub| ra(g, v, r, sub, Operation::Create, "");
        let cluster = with("Cluster");
        for a in [
            cl("g", "v", "r", ""),
            cl("g", "v", "r", "exec"),
            cl("", "v1", "namespaces", ""),
            cl("", "v1", "namespaces", "finalize"),
            ns("", "v1", "namespaces", ""),
            ns("", "v1", "namespaces", "finalize"),
        ] {
            assert!(rule_matches(&cluster, &a), "{a:?}");
        }
        for a in [ns("g", "v", "r", ""), ns("g", "v", "r", "exec")] {
            assert!(!rule_matches(&cluster, &a), "{a:?}");
        }
        let namespaced = with("Namespaced");
        for a in [ns("g", "v", "r", ""), ns("g", "v", "r", "exec")] {
            assert!(rule_matches(&namespaced, &a), "{a:?}");
        }
        for a in [
            cl("", "v1", "namespaces", ""),
            cl("", "v1", "namespaces", "finalize"),
            ns("", "v1", "namespaces", ""),
            ns("", "v1", "namespaces", "finalize"),
            cl("g", "v", "r", ""),
            cl("g", "v", "r", "exec"),
        ] {
            assert!(!rule_matches(&namespaced, &a), "{a:?}");
        }
        let all = with("*");
        for a in [
            ns("g", "v", "r", ""),
            cl("g", "v", "r", "exec"),
            cl("", "v1", "namespaces", ""),
            ns("", "v1", "namespaces", "finalize"),
        ] {
            assert!(rule_matches(&all, &a), "{a:?}");
        }
    }

    // ---- policy_matcher.go ---------------------------------------------

    #[tokio::test]
    async fn definition_without_constraints_or_selectors_is_an_error() {
        let mapper = mapper();
        let m = Matcher {
            namespaces: &NoNamespaces,
            mapper: &mapper,
        };
        let e = m
            .definition_matches(&deploy_attrs(), None)
            .await
            .unwrap_err();
        assert!(
            e.to_string()
                .contains("policy contained no match constraints, a required field"),
            "{e}"
        );
        let mut c = mr(None, vec![rule(&["*"], &["*"], &["*"])]);
        c.namespace_selector = None;
        let e = m
            .definition_matches(&deploy_attrs(), Some(&c))
            .await
            .unwrap_err();
        assert!(
            e.to_string().contains(
                "a nil namespace selector was passed, please ensure selectors are initialized properly"
            ),
            "{e}"
        );
        let mut c = mr(None, vec![rule(&["*"], &["*"], &["*"])]);
        c.object_selector = None;
        let e = m
            .definition_matches(&deploy_attrs(), Some(&c))
            .await
            .unwrap_err();
        assert!(
            e.to_string().contains(
                "a nil object selector was passed, please ensure selectors are initialized properly"
            ),
            "{e}"
        );
    }

    #[tokio::test]
    async fn binding_without_match_resources_matches_everything() {
        let mapper = mapper();
        let m = Matcher {
            namespaces: &NoNamespaces,
            mapper: &mapper,
        };
        assert!(m.binding_matches(&deploy_attrs(), None).await.unwrap());
        let mut c = mr(None, vec![rule(&["*"], &["*"], &["*"])]);
        c.object_selector = None;
        assert!(m.binding_matches(&deploy_attrs(), Some(&c)).await.is_err());
        let c = mr(None, vec![rule(&["batch"], &["*"], &["*"])]);
        assert!(!m.binding_matches(&deploy_attrs(), Some(&c)).await.unwrap());
    }

    // ---- namespace / object selectors (namespace/matcher.go, object/matcher.go)

    struct OneNamespace;
    #[async_trait]
    impl NamespaceLister for OneNamespace {
        async fn namespace_labels(&self, name: &str) -> Result<HashMap<String, String>> {
            if name == "ns" {
                Ok(HashMap::from([("env".to_string(), "prod".to_string())]))
            } else {
                Err(Error::NotFound(format!("namespaces \"{name}\" not found")))
            }
        }
    }

    fn label_sel(k: &str, v: &str) -> LabelSelector {
        LabelSelector {
            match_labels: Some(HashMap::from([(k.to_string(), v.to_string())])),
            match_expressions: None,
        }
    }

    #[tokio::test]
    async fn namespace_selector_reads_namespace_labels() {
        let mapper = mapper();
        let m = Matcher {
            namespaces: &OneNamespace,
            mapper: &mapper,
        };
        let mut c = mr(None, vec![rule(&["*"], &["*"], &["*"])]);
        c.namespace_selector = Some(label_sel("env", "prod"));
        assert!(m.matches(&deploy_attrs(), &c).await.unwrap().is_some());
        c.namespace_selector = Some(label_sel("env", "dev"));
        assert!(m.matches(&deploy_attrs(), &c).await.unwrap().is_none());
        // A missing namespace is a 404, namespace/matcher.go:116-124.
        let mut a = deploy_attrs();
        a.namespace = "gone".into();
        assert!(matches!(m.matches(&a, &c).await, Err(Error::NotFound(_))));
        // A cluster-scoped non-namespace is never excluded (matcher.go:97-101).
        let mut a = deploy_attrs();
        a.namespace = String::new();
        assert!(m.matches(&a, &c).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn namespace_create_reads_labels_from_the_object() {
        // namespace/matcher.go:62-73: on create/update of a Namespace the
        // lister does not have the new labels yet.
        let mapper = mapper();
        let m = Matcher {
            namespaces: &NoNamespaces,
            mapper: &mapper,
        };
        let mut c = mr(None, vec![rule(&["*"], &["*"], &["*"])]);
        c.namespace_selector = Some(label_sel("env", "prod"));
        let mut a = attrs(
            gvk("", "v1", "Namespace"),
            "newns",
            "newns",
            gvr("", "v1", "namespaces"),
            "",
            Operation::Create,
        );
        a.object =
            Some(serde_json::json!({"metadata": {"name": "newns", "labels": {"env": "prod"}}}));
        assert!(m.matches(&a, &c).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn object_selector_matches_the_object_or_the_old_object() {
        // object/matcher.go:56-58.
        let mapper = mapper();
        let m = Matcher {
            namespaces: &NoNamespaces,
            mapper: &mapper,
        };
        let mut c = mr(None, vec![rule(&["*"], &["*"], &["*"])]);
        c.object_selector = Some(label_sel("a", "b"));
        let mut a = deploy_attrs();
        assert!(m.matches(&a, &c).await.unwrap().is_none());
        a.object = Some(serde_json::json!({"metadata": {"labels": {"a": "x"}}}));
        assert!(m.matches(&a, &c).await.unwrap().is_none());
        a.old_object = Some(serde_json::json!({"metadata": {"labels": {"a": "b"}}}));
        assert!(m.matches(&a, &c).await.unwrap().is_some());
    }
}
