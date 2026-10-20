//! Cases from rbac/rbac_test.go:147-269, validation/rule_test.go (appliesTo),
//! authorization/union/union_test.go, with reason text asserted from the sources.
use super::*;
use crate::resources::rbac::RoleRef;
use serde::Serialize;

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
        Err(crate::Error::NotFound(format!("missing {kind} {key}")))
    }
    async fn list<T: Serialize + DeserializeOwned + Send + Sync>(
        &self,
        namespace: Option<&str>,
    ) -> Result<Vec<T>> {
        match std::any::type_name::<T>().rsplit("::").next().unwrap() {
            "RoleBinding" => {
                if let Some(error) = &self.role_list_error {
                    return Err(crate::Error::Authorization(error.clone()));
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
                    return Err(crate::Error::Authorization(error.clone()));
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
    let authorizer = RBACAuthorizer::new(Arc::new(store(vec![sa], false, RoleRef::role("reader"))));
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
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
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
        NodeAuthorizer
            .authorize_opinion(&attrs("alice"))
            .await
            .unwrap(),
        Opinion::NoOpinion {
            reason: String::new(),
            error: None
        }
    );
    assert_eq!(
        NodeAuthorizer
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
        NodeAuthorizer.authorize_opinion(&own_node).await.unwrap(),
        Opinion::Allow(String::new())
    );
    let mut unknown = own.clone();
    unknown.resource = "not-a-node-resource".into();
    assert_eq!(
        NodeAuthorizer.authorize_opinion(&unknown).await.unwrap(),
        Opinion::NoOpinion {
            reason: String::new(),
            error: None
        }
    );
    let mut no_group = own_node;
    no_group.user.groups.clear();
    assert_eq!(
        NodeAuthorizer.authorize_opinion(&no_group).await.unwrap(),
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
