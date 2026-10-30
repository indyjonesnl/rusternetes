//! Unit tests moved from `src/authz.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_common::auth::UserInfo;
    pub use rusternetes_common::authz::node_authorizer::{NodeAuthorizer, NodeAuthorizerFeatures};
    pub use rusternetes_common::authz::reasons::{
        aggregate_errors, applicable_subject, describe_subject, quoted,
    };
    pub use rusternetes_common::error::Result;
    pub use rusternetes_common::field_selector::{FieldRequirement, FieldSelector};
    pub use rusternetes_common::label_selector::{LabelRequirement, LabelSelector};
    pub use rusternetes_common::resources::rbac::{
        ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, Subject,
    };
    pub use serde::de::DeserializeOwned;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::authz::*;

    fn selector_attrs(field: &str, label: &str) -> RequestAttributes {
        let mut a = RequestAttributes::new(
            UserInfo {
                username: "alice".into(),
                uid: String::new(),
                groups: vec![],
                extra: Default::default(),
            },
            "list",
            "jobs",
        );
        a.set_selectors(field, label);
        a
    }

    // authorization_test.go TestGetAuthorizerAttributes "enabled, good field selector".
    #[test]
    fn set_selectors_parses_good_field_selector() {
        let a = selector_attrs("foo==bar", "");
        assert_eq!(a.field_selector_requirements.len(), 1);
        assert_eq!(a.field_selector_requirements[0].field(), "foo");
        assert_eq!(a.field_selector_requirements[0].value(), "bar");
        assert!(a.field_selector_parsing_err.is_none());
        assert!(a.label_selector_requirements.is_empty());
    }

    // "enabled, bad field selector": requirements nil, error recorded.
    #[test]
    fn set_selectors_records_field_selector_parse_error() {
        let a = selector_attrs("*bar", "");
        assert!(a.field_selector_requirements.is_empty());
        assert!(a.field_selector_parsing_err.is_some());
    }

    // "enabled, good label selector".
    #[test]
    fn set_selectors_parses_good_label_selector() {
        let a = selector_attrs("", "foo==bar");
        assert_eq!(a.label_selector_requirements.len(), 1);
        assert_eq!(a.label_selector_requirements[0].key(), "foo");
        assert!(a.label_selector_parsing_err.is_none());
    }

    // "enabled, bad label selector". Upstream's text differs only in the empty
    // field path, which Go renders `<nil>` and the existing parser renders empty.
    #[test]
    fn set_selectors_records_label_selector_parse_error() {
        let a = selector_attrs("", "*bar");
        assert!(a.label_selector_requirements.is_empty());
        assert_eq!(
            a.label_selector_parsing_err.as_deref(),
            Some("unable to parse requirement: : Invalid value: \"*bar\": name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')")
        );
    }

    #[test]
    fn test_policy_rule_matching() {
        let rule = PolicyRule {
            verbs: vec!["get".to_string(), "list".to_string()],
            api_groups: Some(vec!["".to_string()]),
            resources: Some(vec!["pods".to_string()]),
            resource_names: None,
            non_resource_urls: None,
        };

        let user = UserInfo {
            username: "test-user".to_string(),
            uid: "test-uid".to_string(),
            groups: vec![],
            extra: std::collections::HashMap::new(),
        };

        let attrs = RequestAttributes::new(user.clone(), "get", "pods")
            .with_namespace("default")
            .with_api_group("");

        // This would need a mock storage implementation to fully test
        // Just testing the rule matching logic
        assert!(rule.verbs.contains(&attrs.verb));
    }

    struct NoStore;
    #[async_trait]
    impl AuthzStorage for NoStore {
        async fn get<T>(&self, key: &str, _ns: Option<&str>) -> Result<T>
        where
            T: DeserializeOwned + Send + Sync,
        {
            Err(rusternetes_common::error::Error::NotFound(key.to_string()))
        }
        async fn list<T>(&self, _ns: Option<&str>) -> Result<Vec<T>>
        where
            T: serde::Serialize + DeserializeOwned + Send + Sync,
        {
            Ok(Vec::new())
        }
    }

    /// A kubelet's own pod watch, verbatim: cluster-scoped LIST of pods with a
    /// `spec.nodeName` field selector, as `system:node:<name>`.
    ///
    /// A vanilla kind kubelet against a swapped rusternetes api-server was denied
    /// this and therefore never started a single pod:
    ///
    ///   Failed to watch: failed to list *v1.Pod: Node vs-apisrv-dbg-worker is not
    ///   authorized to list pods in namespace None
    ///
    /// Upstream allows it through the static node rules
    /// (plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go NodeRules():
    /// `NewRule(Read...).Groups(legacyGroup).Resources("pods")`), which the Node
    /// authorizer falls through to when pods are not subdivided by selector
    /// (node_authorizer.go: "Access to other resources is not subdivided, so just
    /// evaluate against the statically defined node rules").
    /// A node authorizer over an empty graph with the selector-gated pod/node
    /// decisions off, so pods fall through to the static rules (the mode the
    /// api-server runs the authorizer in until the kubelet sends selectors).
    fn node_authorizer() -> NodeAuthorizer {
        NodeAuthorizer::new(Arc::new(node_graph::NodeGraph::new())).with_features(
            NodeAuthorizerFeatures {
                authorize_node_with_selectors: false,
                kubelet_service_account_token_for_credential_providers: true,
                pod_certificate_request: false,
            },
        )
    }

    fn node_user() -> UserInfo {
        UserInfo {
            username: "system:node:worker-1".to_string(),
            uid: "node-uid".to_string(),
            groups: vec!["system:nodes".to_string()],
            extra: std::collections::HashMap::new(),
        }
    }

    #[tokio::test]
    async fn node_may_list_pods_cluster_wide() {
        let attrs = RequestAttributes::new(node_user(), "list", "pods").with_api_group("");
        assert!(
            matches!(
                node_authorizer().authorize(&attrs).await.unwrap(),
                Decision::Allow
            ),
            "the kubelet's own cluster-scoped pod list must be allowed"
        );
    }

    #[tokio::test]
    async fn node_may_watch_pods_cluster_wide() {
        let attrs = RequestAttributes::new(node_user(), "watch", "pods").with_api_group("");
        assert!(matches!(
            node_authorizer().authorize(&attrs).await.unwrap(),
            Decision::Allow
        ));
    }

    /// The second denial the same kubelet logged, also a NodeRules entry:
    /// `NewRule("get","list","watch").Groups("node.k8s.io").Resources("runtimeclasses")`.
    #[tokio::test]
    async fn node_may_list_runtimeclasses() {
        let attrs = RequestAttributes::new(node_user(), "list", "runtimeclasses")
            .with_api_group("node.k8s.io");
        assert!(matches!(
            node_authorizer().authorize(&attrs).await.unwrap(),
            Decision::Allow
        ));
    }

    /// Upstream NodeRules grants pods create/delete outright and constrains them
    /// in NodeRestriction admission instead (policy.go:215-217). This asserted
    /// the opposite while rusternetes had no such plugin (#1721); it does now
    /// (api-server `handlers::node_restriction`), which is what lets a kubelet
    /// delete the mirror pod of a static pod (#1906).
    #[tokio::test]
    async fn node_may_create_and_delete_pods() {
        for verb in ["create", "delete"] {
            let attrs = RequestAttributes::new(node_user(), verb, "pods")
                .with_namespace("kube-system")
                .with_api_group("");
            assert_eq!(
                node_authorizer().authorize(&attrs).await.unwrap(),
                Decision::Allow,
                "node should be authorized to {verb} pods; NodeRestriction narrows it"
            );
        }
    }

    /// `pods/eviction` (policy.go:223) stays denied: there is no NodeRestriction
    /// equivalent for the eviction path yet, so the grant would let a node evict
    /// any pod in the cluster.
    #[tokio::test]
    async fn node_may_not_create_pod_evictions() {
        let attrs = RequestAttributes::new(node_user(), "create", "pods")
            .with_namespace("kube-system")
            .with_api_group("")
            .with_subresource("eviction");
        assert!(matches!(
            node_authorizer().authorize(&attrs).await.unwrap(),
            Decision::Deny(_)
        ));
    }

    /// Services are read-only for nodes (`Read...`), so writes stay denied — the
    /// rule table must not become a blanket allow.
    #[tokio::test]
    async fn node_may_not_delete_services() {
        let attrs = RequestAttributes::new(node_user(), "delete", "services")
            .with_namespace("default")
            .with_api_group("");
        assert!(
            matches!(
                node_authorizer().authorize(&attrs).await.unwrap(),
                Decision::Deny(_)
            ),
            "nodes have read-only access to services"
        );
    }

    /// And a non-node user is still not a node.
    #[tokio::test]
    async fn non_node_user_is_denied() {
        let user = UserInfo {
            username: "alice".to_string(),
            uid: "u".to_string(),
            groups: vec![],
            extra: std::collections::HashMap::new(),
        };
        let attrs = RequestAttributes::new(user, "list", "pods").with_api_group("");
        assert!(matches!(
            node_authorizer().authorize(&attrs).await.unwrap(),
            Decision::Deny(_)
        ));
    }

    fn rule(verbs: &[&str], groups: &[&str], resources: &[&str]) -> PolicyRule {
        PolicyRule {
            verbs: verbs.iter().map(|s| s.to_string()).collect(),
            api_groups: Some(groups.iter().map(|s| s.to_string()).collect()),
            resources: Some(resources.iter().map(|s| s.to_string()).collect()),
            resource_names: None,
            non_resource_urls: None,
        }
    }

    /// #1667: a rule on the subresource `pods/binding` must authorize a request
    /// for resource=pods subresource=binding (the scheduler's Bind), and a rule
    /// on the bare `pods` must NOT — subresources are granted explicitly. Also
    /// `*/binding` matches. Mirrors upstream `resourceMatches`.
    #[test]
    fn rbac_matches_subresource_combined() {
        let authz = RBACAuthorizer::new(Arc::new(NoStore));
        let bind = RequestAttributes::new(mk_user("system:kube-scheduler"), "create", "pods")
            .with_api_group("")
            .with_subresource("binding");

        assert!(
            authz.rule_allows(&rule(&["create"], &[""], &["pods/binding"]), &bind),
            "a rule on pods/binding must authorize the Bind"
        );
        assert!(
            !authz.rule_allows(&rule(&["create"], &[""], &["pods"]), &bind),
            "a rule on the bare `pods` must NOT grant pods/binding"
        );
        assert!(
            authz.rule_allows(&rule(&["create"], &[""], &["*/binding"]), &bind),
            "`*/binding` must match the binding subresource"
        );
        assert!(
            authz.rule_allows(&rule(&["*"], &["*"], &["*"]), &bind),
            "`*` resources must match everything incl. subresources"
        );
        // A bare-resource request still matches a bare-resource rule.
        let get_pod = RequestAttributes::new(mk_user("x"), "get", "pods").with_api_group("");
        assert!(authz.rule_allows(&rule(&["get"], &[""], &["pods"]), &get_pod));
    }

    fn mk_user(username: &str) -> UserInfo {
        UserInfo {
            username: username.to_string(),
            uid: String::new(),
            groups: vec![],
            extra: std::collections::HashMap::new(),
        }
    }

    /// `mockAuthzHandler` from `authorization/union/union_test.go:30-38`:
    /// a fixed decision, empty reason, optional error.
    struct MockAuthz {
        opinion: Opinion,
        error: Option<&'static str>,
    }

    fn mock(opinion: Opinion, error: Option<&'static str>) -> Arc<dyn Authorizer> {
        Arc::new(MockAuthz { opinion, error })
    }

    fn no_op() -> Opinion {
        Opinion::NoOpinion {
            reason: String::new(),
            error: None,
        }
    }

    #[async_trait]
    impl Authorizer for MockAuthz {
        async fn authorize(&self, a: &RequestAttributes) -> Result<Decision> {
            self.authorize_outcome(a).await?.into_decision()
        }
        async fn authorize_outcome(&self, _: &RequestAttributes) -> Result<AuthorizationOutcome> {
            Ok(AuthorizationOutcome {
                opinion: self.opinion.clone(),
                error: self.error.map(str::to_string),
            })
        }
        async fn get_user_rules(
            &self,
            _: &UserInfo,
            _: &str,
        ) -> Result<(
            Vec<rusternetes_common::resources::ResourceRule>,
            Vec<rusternetes_common::resources::NonResourceRule>,
        )> {
            Ok((vec![], vec![]))
        }
    }

    /// Ports of `TestAuthorizationSecondPasses`, `TestAuthorizationFirstPasses`,
    /// `TestAuthorizationNonePasses`, `TestAuthorizationError` and
    /// `TestAuthorizationUnequivocalDeny`
    /// (`authorization/union/union_test.go:40-82,224-268`), plus the reason
    /// join of `union.go:45-68` (`strings.Join(reasonlist, "\n")`, empty
    /// reasons skipped, errors aggregated).
    #[tokio::test]
    async fn union_test_go_authorize_ports() {
        let attrs = RequestAttributes::new(mk_user("a"), "get", "pods");
        let allow = || mock(Opinion::Allow(String::new()), None);
        let deny = |e| mock(Opinion::Deny(String::new()), e);

        // SecondPasses / FirstPasses.
        let u = UnionAuthorizer::new(vec![mock(no_op(), None), allow()]);
        assert_eq!(u.authorize(&attrs).await.unwrap(), Decision::Allow);
        let u = UnionAuthorizer::new(vec![allow(), mock(no_op(), None)]);
        assert_eq!(u.authorize(&attrs).await.unwrap(), Decision::Allow);

        // NonePasses: not Allow.
        let u = UnionAuthorizer::new(vec![mock(no_op(), None), mock(no_op(), None)]);
        assert!(!matches!(u.authorize(&attrs).await, Ok(Decision::Allow)));

        // Error: two NoOpinion+error delegates -> an error, aggregated with
        // dedup of identical messages (utilerrors.NewAggregate).
        let u = UnionAuthorizer::new(vec![mock(no_op(), Some("foo")), mock(no_op(), Some("foo"))]);
        let out = u.authorize_outcome(&attrs).await.unwrap();
        assert_eq!(out.error.as_deref(), Some("foo"));
        assert!(u.authorize(&attrs).await.is_err());
        let u = UnionAuthorizer::new(vec![mock(no_op(), Some("a")), mock(no_op(), Some("b"))]);
        let out = u.authorize_outcome(&attrs).await.unwrap();
        assert_eq!(out.error.as_deref(), Some("[a, b]"));

        // UnequivocalDeny cases 0-3.
        let u = UnionAuthorizer::new(vec![]);
        let out = u.authorize_outcome(&attrs).await.unwrap();
        assert!(matches!(out.opinion, Opinion::NoOpinion { .. }));
        let u = UnionAuthorizer::new(vec![mock(no_op(), None), allow(), deny(None)]);
        assert!(matches!(
            u.authorize_outcome(&attrs).await.unwrap().opinion,
            Opinion::Allow(_)
        ));
        let u = UnionAuthorizer::new(vec![mock(no_op(), None), deny(None), allow()]);
        assert!(matches!(
            u.authorize_outcome(&attrs).await.unwrap().opinion,
            Opinion::Deny(_)
        ));
        let u = UnionAuthorizer::new(vec![
            mock(no_op(), None),
            deny(Some("webhook failed closed")),
            allow(),
        ]);
        let out = u.authorize_outcome(&attrs).await.unwrap();
        assert!(matches!(out.opinion, Opinion::Deny(_)));
        assert_eq!(out.error.as_deref(), Some("webhook failed closed"));

        // Reason join: "\n" separator, empty reasons skipped.
        let r = |s: &str| {
            mock(
                Opinion::NoOpinion {
                    reason: s.into(),
                    error: None,
                },
                None,
            )
        };
        let u = UnionAuthorizer::new(vec![r("one"), r(""), r("two")]);
        match u.authorize_outcome(&attrs).await.unwrap().opinion {
            Opinion::NoOpinion { reason, .. } => assert_eq!(reason, "one\ntwo"),
            other => panic!("unexpected {other:?}"),
        }
    }

    struct RulesMock(
        Vec<rusternetes_common::resources::ResourceRule>,
        Vec<rusternetes_common::resources::NonResourceRule>,
    );

    #[async_trait]
    impl Authorizer for RulesMock {
        async fn authorize(&self, _: &RequestAttributes) -> Result<Decision> {
            Ok(Decision::Deny(String::new()))
        }
        async fn get_user_rules(
            &self,
            _: &UserInfo,
            _: &str,
        ) -> Result<(
            Vec<rusternetes_common::resources::ResourceRule>,
            Vec<rusternetes_common::resources::NonResourceRule>,
        )> {
            Ok((self.0.clone(), self.1.clone()))
        }
    }

    /// Ports of `TestAuthorizationResourceRules` and
    /// `TestAuthorizationNonResourceRules` (union_test.go:97-198):
    /// `unionAuthzRulesHandler.RulesFor` concatenates rules in order.
    #[tokio::test]
    async fn union_rules_concatenate_in_order() {
        use rusternetes_common::resources::{NonResourceRule, ResourceRule};
        let s = |v: &[&str]| Some(v.iter().map(|x| x.to_string()).collect::<Vec<_>>());
        let rr = |verbs: &[&str], res: &[&str], names: Option<&[&str]>| ResourceRule {
            verbs: verbs.iter().map(|x| x.to_string()).collect(),
            api_groups: s(&["*"]),
            resources: s(res),
            resource_names: names.and_then(s),
        };
        let nr = |url: &str| NonResourceRule {
            verbs: vec!["get".into()],
            non_resource_urls: s(&[url]),
        };
        let u = UnionAuthorizer::new(vec![
            Arc::new(RulesMock(
                vec![
                    rr(&["*"], &["bindings"], None),
                    rr(&["get", "list", "watch"], &["*"], None),
                ],
                vec![nr("/api")],
            )),
            Arc::new(RulesMock(
                vec![
                    rr(&["*"], &["events"], None),
                    rr(&["get"], &["*"], Some(&["foo"])),
                ],
                vec![nr("/api/*")],
            )),
        ]);
        let (res, non) = u.get_user_rules(&mk_user("a"), "").await.unwrap();
        let want_res = vec![
            rr(&["*"], &["bindings"], None),
            rr(&["get", "list", "watch"], &["*"], None),
            rr(&["*"], &["events"], None),
            rr(&["get"], &["*"], Some(&["foo"])),
        ];
        assert_eq!(
            serde_json::to_value(&res).unwrap(),
            serde_json::to_value(&want_res).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&non).unwrap(),
            serde_json::to_value(vec![nr("/api"), nr("/api/*")]).unwrap()
        );
    }

    /// #1664: the Node,RBAC union is allow-override — a `Deny` from one
    /// authorizer falls through to the next, any `Allow` wins, and only an
    /// all-deny chain denies. In particular a kubelet (`system:node:<n>`) is
    /// authorized for its own Node via the Node authorizer even when the other
    /// authorizer denies (as an empty RBAC store would).
    #[tokio::test]
    async fn union_is_allow_override() {
        let u = mk_user("alice");
        let attrs = RequestAttributes::new(u.clone(), "get", "pods").with_namespace("default");

        // Deny then Allow -> Allow (fall-through).
        let union = UnionAuthorizer::new(vec![
            Arc::new(AlwaysDenyAuthorizer),
            Arc::new(AlwaysAllowAuthorizer),
        ]);
        assert!(matches!(
            union.authorize(&attrs).await.unwrap(),
            Decision::Allow
        ));

        // All deny -> Deny.
        let union_deny = UnionAuthorizer::new(vec![Arc::new(AlwaysDenyAuthorizer)]);
        assert!(matches!(
            union_deny.authorize(&attrs).await.unwrap(),
            Decision::Deny(_)
        ));

        // Node authorizer grants a kubelet its own Node, even chained with a
        // denying authorizer (models Node + empty-RBAC).
        let node_union = UnionAuthorizer::new(vec![
            Arc::new(node_authorizer()),
            Arc::new(AlwaysDenyAuthorizer),
        ]);
        // NodeIdentity requires the system:nodes group (default.go:42-68).
        let mut node_user = mk_user("system:node:n1");
        node_user.groups.push("system:nodes".into());
        let node_attrs = RequestAttributes::new(node_user, "get", "nodes").with_name("n1");
        assert!(matches!(
            node_union.authorize(&node_attrs).await.unwrap(),
            Decision::Allow
        ));
        // A non-node user gets no Node grant; with a denying peer the union denies.
        let denied = RequestAttributes::new(mk_user("mallory"), "get", "nodes").with_name("n1");
        assert!(matches!(
            node_union.authorize(&denied).await.unwrap(),
            Decision::Deny(_)
        ));
    }

    /// Port of `TestPrivilegedGroupAuthorizer`
    /// (`staging/src/k8s.io/apiserver/pkg/authorization/authorizerfactory/builtin_test.go:41-55`):
    /// a user in a privileged group is allowed, one outside it is not, and the
    /// authorizer holds regardless of what any RBAC object says (#1576).
    #[tokio::test]
    async fn privileged_group_authorizer() {
        let auth = PrivilegedGroupAuthorizer::new(&["allow-01", "allow-01"]);
        let mut yes = mk_user("alice");
        yes.groups = vec!["no".into(), "allow-01".into()];
        let mut no = mk_user("bob");
        no.groups = vec!["no".into(), "deny-01".into()];
        let a = RequestAttributes::new(yes, "delete", "nodes");
        assert!(matches!(auth.authorize(&a).await.unwrap(), Decision::Allow));
        let b = RequestAttributes::new(no, "delete", "nodes");
        assert!(matches!(
            auth.authorize(&b).await.unwrap(),
            Decision::Deny(_)
        ));
    }

    /// `system:masters` is the superuser group (`user.SystemPrivilegedGroup`,
    /// `k8s.io/apiserver/pkg/authentication/user/user.go:71`) and the union
    /// built by `superuser_then` puts it first, as `newForConfig` does
    /// (`pkg/kubeapiserver/authorizer/reload.go:97-99`): it is allowed against an
    /// empty RBAC store, a non-member is not.
    #[tokio::test]
    async fn system_masters_is_superuser_over_empty_rbac() {
        let union = superuser_then(vec![Arc::new(AlwaysDenyAuthorizer)]);
        let mut admin = mk_user("kubernetes-admin");
        admin.groups = vec![SYSTEM_PRIVILEGED_GROUP.to_string()];
        let a = RequestAttributes::new(admin, "create", "clusterroles")
            .with_api_group("rbac.authorization.k8s.io");
        assert!(matches!(
            union.authorize(&a).await.unwrap(),
            Decision::Allow
        ));
        let m = RequestAttributes::new(mk_user("mallory"), "create", "clusterroles");
        assert!(matches!(
            union.authorize(&m).await.unwrap(),
            Decision::Deny(_)
        ));
    }
}

#[path = "authz/allow_reason_tests.rs"]
mod allow_reason_tests {
    //! Cases from rbac/rbac_test.go:147-269, validation/rule_test.go (appliesTo),
    //! authorization/union/union_test.go, with reason text asserted from the sources.
    use super::__parent::*;
    use rusternetes_common::authz::*;
    use rusternetes_common::resources::rbac::RoleRef;
    use serde::Serialize;

    fn node_authorizer() -> NodeAuthorizer {
        NodeAuthorizer::new(Arc::new(
            rusternetes_common::authz::node_graph::NodeGraph::new(),
        ))
    }

    #[derive(Default)]
    struct Store {
        roles: Vec<Role>,
        cluster_roles: Vec<ClusterRole>,
        bindings: Vec<RoleBinding>,
        cluster_bindings: Vec<ClusterRoleBinding>,
        cluster_list_error: Option<String>,
        role_list_error: Option<String>,
    }

    fn convert<T: DeserializeOwned>(value: impl Serialize) -> Result<T> {
        Ok(serde_json::from_value(serde_json::to_value(value).unwrap()).unwrap())
    }

    #[async_trait]
    impl AuthzStorage for Store {
        async fn get<T: DeserializeOwned + Send + Sync>(
            &self,
            key: &str,
            namespace: Option<&str>,
        ) -> Result<T> {
            let kind = std::any::type_name::<T>().rsplit("::").next().unwrap();
            match kind {
                "Role" => {
                    if let Some(role) = self.roles.iter().find(|r| {
                        r.metadata.name == key && r.metadata.namespace.as_deref() == namespace
                    }) {
                        return convert(role);
                    }
                }
                "ClusterRole" => {
                    if let Some(role) = self.cluster_roles.iter().find(|r| r.metadata.name == key) {
                        return convert(role);
                    }
                }
                _ => panic!("unexpected kind {kind}"),
            }
            Err(rusternetes_common::Error::NotFound(format!(
                "missing {kind} {key}"
            )))
        }
        async fn list<T: Serialize + DeserializeOwned + Send + Sync>(
            &self,
            namespace: Option<&str>,
        ) -> Result<Vec<T>> {
            match std::any::type_name::<T>().rsplit("::").next().unwrap() {
                "RoleBinding" => {
                    if let Some(error) = &self.role_list_error {
                        return Err(rusternetes_common::Error::Authorization(error.clone()));
                    }
                    convert(
                        self.bindings
                            .iter()
                            .filter(|b| b.metadata.namespace.as_deref() == namespace)
                            .collect::<Vec<_>>(),
                    )
                }
                "ClusterRoleBinding" => {
                    if let Some(error) = &self.cluster_list_error {
                        return Err(rusternetes_common::Error::Authorization(error.clone()));
                    }
                    convert(&self.cluster_bindings)
                }
                kind => panic!("unexpected kind {kind}"),
            }
        }
    }

    fn attrs(name: &str) -> RequestAttributes {
        RequestAttributes::new(
            UserInfo {
                username: name.into(),
                uid: String::new(),
                groups: if name.starts_with("system:node:") {
                    vec!["system:nodes".into()]
                } else {
                    vec!["readers".into()]
                },
                extra: Default::default(),
            },
            "get",
            "pods",
        )
        .with_namespace("ns")
    }
    fn policy() -> PolicyRule {
        PolicyRule {
            verbs: vec!["get".into()],
            api_groups: Some(vec!["".into()]),
            resources: Some(vec!["pods".into()]),
            resource_names: None,
            non_resource_urls: None,
        }
    }
    fn store(subjects: Vec<Subject>, cluster: bool, reference: RoleRef) -> Store {
        let mut role = Role::new("reader", "ns");
        role.rules = vec![policy()];
        let mut cluster_role = ClusterRole::new("reader");
        cluster_role.rules = vec![policy()];
        Store {
            roles: vec![role],
            cluster_roles: vec![cluster_role],
            bindings: if cluster {
                vec![]
            } else {
                vec![RoleBinding::new("binding", "ns")
                    .with_subjects(subjects.clone())
                    .with_role_ref(reference.clone())]
            },
            cluster_bindings: if cluster {
                vec![ClusterRoleBinding::new("binding")
                    .with_subjects(subjects)
                    .with_role_ref(reference)]
            } else {
                vec![]
            },
            ..Store::default()
        }
    }

    // rbac.go:63-80; validation/rule.go:139-176,187-200,215-228.
    #[tokio::test]
    async fn rbac_binding_subject_and_reference_reasons() {
        for (subject, username, cluster, reference, expected) in [
            (Subject::user("alice"), "alice", true, RoleRef::cluster_role("reader"), "RBAC: allowed by ClusterRoleBinding \"binding\" of ClusterRole \"reader\" to User \"alice\""),
            (Subject::group("readers"), "alice", true, RoleRef::cluster_role("reader"), "RBAC: allowed by ClusterRoleBinding \"binding\" of ClusterRole \"reader\" to Group \"readers\""),
            (Subject::user("alice"), "alice", false, RoleRef::role("reader"), "RBAC: allowed by RoleBinding \"binding/ns\" of Role \"reader\" to User \"alice\""),
            (Subject::user("alice"), "alice", false, RoleRef::cluster_role("reader"), "RBAC: allowed by RoleBinding \"binding/ns\" of ClusterRole \"reader\" to User \"alice\""),
            (Subject::service_account("sa", "ns"), "system:serviceaccount:ns:sa", false, RoleRef::role("reader"), "RBAC: allowed by RoleBinding \"binding/ns\" of Role \"reader\" to ServiceAccount \"sa/ns\""),
        ] {
            let authorizer = RBACAuthorizer::new(Arc::new(store(vec![subject], cluster, reference)));
            assert_eq!(authorizer.authorize_opinion(&attrs(username)).await.unwrap(), Opinion::Allow(expected.into()));
        }
        // First applicable subject wins; an omitted SA namespace defaults to the binding namespace.
        let mut sa = Subject::service_account("sa", "");
        sa.namespace = None;
        let authorizer =
            RBACAuthorizer::new(Arc::new(store(vec![sa], false, RoleRef::role("reader"))));
        assert_eq!(authorizer.authorize_opinion(&attrs("system:serviceaccount:ns:sa")).await.unwrap(), Opinion::Allow("RBAC: allowed by RoleBinding \"binding/ns\" of Role \"reader\" to ServiceAccount \"sa/ns\"".into()));
    }

    // VisitRulesFor visits cluster bindings before namespaced bindings (rule.go:179-235).
    #[tokio::test]
    async fn rbac_cluster_binding_precedes_namespaced_binding() {
        let mut store = store(vec![Subject::user("alice")], false, RoleRef::role("reader"));
        store.cluster_bindings.push(
            ClusterRoleBinding::new("cluster-first")
                .with_subjects(vec![Subject::user("alice")])
                .with_role_ref(RoleRef::cluster_role("reader")),
        );
        let opinion = RBACAuthorizer::new(Arc::new(store))
            .authorize_opinion(&attrs("alice"))
            .await
            .unwrap();
        assert_eq!(opinion, Opinion::Allow("RBAC: allowed by ClusterRoleBinding \"cluster-first\" of ClusterRole \"reader\" to User \"alice\"".into()));
    }

    // rbac.go:122-126: no match is silent, errors appear in the reason (not error slot).
    #[tokio::test]
    async fn rbac_denial_and_resolution_errors() {
        let empty = RBACAuthorizer::new(Arc::new(Store::default()));
        assert_eq!(
            empty.authorize_opinion(&attrs("alice")).await.unwrap(),
            Opinion::NoOpinion {
                reason: String::new(),
                error: None
            }
        );
        assert_eq!(
            empty
                .authorize_opinion(&attrs("system:admin"))
                .await
                .unwrap(),
            Opinion::NoOpinion {
                reason: String::new(),
                error: None
            }
        );
        let broken = Store {
            cluster_list_error: Some("cluster list failed".into()),
            ..Store::default()
        };
        assert_eq!(
            RBACAuthorizer::new(Arc::new(broken))
                .authorize_opinion(&attrs("alice"))
                .await
                .unwrap(),
            Opinion::NoOpinion {
                reason: "RBAC: Authorization error: cluster list failed".into(),
                error: None
            }
        );
        let mut partial = store(vec![Subject::user("alice")], false, RoleRef::role("reader"));
        partial.cluster_list_error = Some("cluster list failed".into());
        assert!(matches!(
            RBACAuthorizer::new(Arc::new(partial))
                .authorize_opinion(&attrs("alice"))
                .await
                .unwrap(),
            Opinion::Allow(_)
        ));
    }

    struct Fixed(Opinion);
    #[async_trait]
    impl Authorizer for Fixed {
        async fn authorize(&self, _: &RequestAttributes) -> Result<Decision> {
            unreachable!()
        }
        async fn authorize_opinion(&self, _: &RequestAttributes) -> Result<Opinion> {
            Ok(self.0.clone())
        }
        async fn get_user_rules(
            &self,
            _: &UserInfo,
            _: &str,
        ) -> Result<(
            Vec<rusternetes_common::resources::ResourceRule>,
            Vec<rusternetes_common::resources::NonResourceRule>,
        )> {
            Ok((vec![], vec![]))
        }
    }

    // union.go:54-69: skip empty reasons, join with newline; first decision owns its reason.
    #[tokio::test]
    async fn union_reason_join_and_short_circuit() {
        let union = UnionAuthorizer::new(vec![
            Arc::new(Fixed(Opinion::NoOpinion {
                reason: "first".into(),
                error: None,
            })),
            Arc::new(Fixed(Opinion::NoOpinion {
                reason: String::new(),
                error: None,
            })),
            Arc::new(Fixed(Opinion::NoOpinion {
                reason: "last".into(),
                error: None,
            })),
        ]);
        assert_eq!(
            union.authorize_opinion(&attrs("alice")).await.unwrap(),
            Opinion::NoOpinion {
                reason: "first\nlast".into(),
                error: None
            }
        );
        let union = UnionAuthorizer::new(vec![
            Arc::new(Fixed(Opinion::NoOpinion {
                reason: "earlier".into(),
                error: Some("ignored".into()),
            })),
            Arc::new(Fixed(Opinion::Allow("winner".into()))),
        ]);
        assert_eq!(
            union.authorize_opinion(&attrs("alice")).await.unwrap(),
            Opinion::Allow("winner".into())
        );
    }

    // builtin.go:32-33,60-61,82-96; node_authorizer.go:109-118,164-168.
    #[tokio::test]
    async fn builtin_and_node_reasons_match_upstream() {
        assert_eq!(
            AlwaysAllowAuthorizer
                .authorize_opinion(&attrs("alice"))
                .await
                .unwrap(),
            Opinion::Allow(String::new())
        );
        assert_eq!(
            AlwaysDenyAuthorizer
                .authorize_opinion(&attrs("alice"))
                .await
                .unwrap(),
            Opinion::NoOpinion {
                reason: "Everything is forbidden.".into(),
                error: None
            }
        );
        let privileged = PrivilegedGroupAuthorizer::new(&["readers"]);
        assert_eq!(
            privileged.authorize_opinion(&attrs("alice")).await.unwrap(),
            Opinion::Allow(String::new())
        );
        assert_eq!(
            node_authorizer()
                .authorize_opinion(&attrs("alice"))
                .await
                .unwrap(),
            Opinion::NoOpinion {
                reason: String::new(),
                error: None
            }
        );
        assert_eq!(
            node_authorizer()
                .authorize_opinion(&attrs("system:node:"))
                .await
                .unwrap(),
            Opinion::NoOpinion {
                reason: "unknown node for user \"system:node:\"".into(),
                error: None
            }
        );
    }

    // rbac.go:123-124 uses utilerrors.NewAggregate; util/errors/errors.go:70-97
    // wraps multiple unique errors, but never a single error or duplicate.
    #[tokio::test]
    async fn rbac_multiple_resolution_errors_and_quoted_subjects() {
        let broken = Store {
            cluster_list_error: Some("cluster failed".into()),
            role_list_error: Some("roles failed".into()),
            ..Store::default()
        };
        assert_eq!(
            RBACAuthorizer::new(Arc::new(broken))
                .authorize_opinion(&attrs("alice"))
                .await
                .unwrap(),
            Opinion::NoOpinion {
                reason:
                    "RBAC: [Authorization error: cluster failed, Authorization error: roles failed]"
                        .into(),
                error: None
            }
        );
        let username = "alice\u{7}\u{0}\n\u{200b}";
        let authorizer = RBACAuthorizer::new(Arc::new(store(
            vec![Subject::user(username)],
            true,
            RoleRef::cluster_role("reader"),
        )));
        assert_eq!(authorizer.authorize_opinion(&attrs(username)).await.unwrap(), Opinion::Allow("RBAC: allowed by ClusterRoleBinding \"binding\" of ClusterRole \"reader\" to User \"alice\\a\\x00\\n\\u200b\"".into()));
    }

    #[tokio::test]
    async fn node_default_and_success_reasons_are_empty() {
        let own = attrs("system:node:n").with_name("n");
        let mut own_node = own.clone();
        own_node.resource = "nodes".into();
        assert_eq!(
            node_authorizer()
                .authorize_opinion(&own_node)
                .await
                .unwrap(),
            Opinion::Allow(String::new())
        );
        let mut unknown = own.clone();
        unknown.resource = "not-a-node-resource".into();
        assert_eq!(
            node_authorizer().authorize_opinion(&unknown).await.unwrap(),
            Opinion::NoOpinion {
                reason: String::new(),
                error: None
            }
        );
        let mut no_group = own_node;
        no_group.user.groups.clear();
        assert_eq!(
            node_authorizer()
                .authorize_opinion(&no_group)
                .await
                .unwrap(),
            Opinion::NoOpinion {
                reason: String::new(),
                error: None
            }
        );
    }

    // authorizer/interfaces.go:80-85: reason and error are independent fields.
    // Legacy adapters must also accept the older embedded NoOpinion error slot.
    #[test]
    fn outcome_adapters_preserve_embedded_errors() {
        let outcome = AuthorizationOutcome {
            opinion: Opinion::NoOpinion {
                reason: "abstained".into(),
                error: Some("evaluation failed".into()),
            },
            error: None,
        };
        assert_eq!(outcome.clone().into_opinion(), outcome.opinion);
        assert!(outcome.into_decision().is_err());
        let denied = AuthorizationOutcome {
            opinion: Opinion::Deny("denied".into()),
            error: Some("evaluation failed".into()),
        };
        assert!(denied.into_decision().is_err());
    }
}
