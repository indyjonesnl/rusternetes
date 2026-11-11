//! #3450: the CEL `authorizer` and `authorizer.requestResource` variables on
//! the MutatingAdmissionPolicy mutation path (`ForInput`'s activation,
//! `plugin/cel/activation.go:44-72`, reached by both
//! `patch/json_patch.go` `jsonPatcher.Patch` and `patch/smd.go`
//! `applyConfigPatcher.Patch`). The matcher side is covered by
//! `admission_unit/admission__policy_match_conditions.rs`.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use k3rs_api_server::admission::policy_apply_config::ApplyConfigPatcher;
use k3rs_api_server::admission::policy_json_patch::JsonPatcher;
use k3rs_api_server::admission::policy_matching::Attributes;
use k3rs_api_server::admission::policy_mutating::{PatchRequest, Patcher, VersionedAttributes};
use k3rs_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
use k3rs_common::auth::UserInfo;
use k3rs_common::authz::{
    AlwaysAllowAuthorizer, AlwaysDenyAuthorizer, Authorizer, Decision, RequestAttributes,
    UserRulesOutcome,
};
use k3rs_common::cel_cost::RUNTIME_CEL_COST_BUDGET;
use serde_json::{json, Value};

/// Allows everything and records what it was asked.
#[derive(Default)]
struct Recording(Mutex<Vec<RequestAttributes>>);

#[async_trait]
impl Authorizer for Recording {
    async fn authorize(&self, attrs: &RequestAttributes) -> k3rs_common::Result<Decision> {
        self.0.lock().unwrap().push(attrs.clone());
        Ok(Decision::Allow)
    }

    async fn get_user_rules_outcome(
        &self,
        _user: &UserInfo,
        _namespace: &str,
    ) -> k3rs_common::Result<UserRulesOutcome> {
        Ok(UserRulesOutcome::default())
    }
}

fn gvr() -> GroupVersionResource {
    GroupVersionResource {
        group: "apps".into(),
        version: "v1".into(),
        resource: "deployments".into(),
    }
}

fn gvk() -> GroupVersionKind {
    GroupVersionKind {
        group: "apps".into(),
        version: "v1".into(),
        kind: "Deployment".into(),
    }
}

fn user() -> UserInfo {
    UserInfo {
        username: "alice".into(),
        uid: "u1".into(),
        groups: vec!["devs".into()],
        extra: Default::default(),
    }
}

/// Runs `patcher` over a Deployment with `authorizer` bound (or not).
fn run(
    patcher: Arc<dyn Patcher>,
    authorizer: Option<Arc<dyn Authorizer>>,
) -> Result<Value, String> {
    let object = json!({"spec": {"replicas": 1}});
    let versioned = VersionedAttributes {
        versioned_kind: gvk(),
        versioned_object: Some(object.clone()),
        versioned_old_object: None,
        dirty: false,
    };
    let attributes = Attributes {
        kind: gvk(),
        resource: gvr(),
        subresource: "status".into(),
        namespace: "team-a".into(),
        name: "d1".into(),
        operation: Operation::Update,
        object: Some(object),
        old_object: None,
        user_info: Some(user()),
        dry_run: false,
        operation_options: None,
    };
    let g = gvr();
    let req = PatchRequest {
        matched_resource: &g,
        versioned_attributes: &versioned,
        attributes: &attributes,
        param: None,
        namespace: None,
        authorizer,
    };
    // A current-thread runtime cannot drive the authorizer's IO from inside a
    // CEL function; the library hands it to a helper thread (#3450).
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(patcher.patch(&req, RUNTIME_CEL_COST_BUDGET))
        .map_err(|e| format!("{e:?}"))
}

const JSON_PATCH_ON_ALLOWED: &str = r#"authorizer.group('apps').resource('deployments').check('get').allowed()
    ? [JSONPatch{op: "replace", path: "/spec/replicas", value: 3}]
    : [JSONPatch{op: "replace", path: "/spec/replicas", value: 0}]"#;

const APPLY_ON_ALLOWED: &str = r#"Object{spec: Object.spec{
    replicas: authorizer.group('apps').resource('deployments').check('get').allowed() ? 3 : 0
}}"#;

#[test]
fn json_patch_branches_on_the_authorizer_decision() {
    let allowed = run(
        JsonPatcher::new_patcher(JSON_PATCH_ON_ALLOWED),
        Some(Arc::new(AlwaysAllowAuthorizer)),
    )
    .unwrap();
    assert_eq!(allowed["spec"]["replicas"], 3, "{allowed}");

    let denied = run(
        JsonPatcher::new_patcher(JSON_PATCH_ON_ALLOWED),
        Some(Arc::new(AlwaysDenyAuthorizer)),
    )
    .unwrap();
    assert_eq!(denied["spec"]["replicas"], 0, "{denied}");
}

#[test]
fn apply_configuration_branches_on_the_authorizer_decision() {
    let allowed = run(
        ApplyConfigPatcher::new_patcher(APPLY_ON_ALLOWED),
        Some(Arc::new(AlwaysAllowAuthorizer)),
    )
    .unwrap();
    assert_eq!(allowed["spec"]["replicas"], 3, "{allowed}");

    let denied = run(
        ApplyConfigPatcher::new_patcher(APPLY_ON_ALLOWED),
        Some(Arc::new(AlwaysDenyAuthorizer)),
    )
    .unwrap();
    assert_eq!(denied["spec"]["replicas"], 0, "{denied}");
}

/// `authorizer.requestResource` is pre-bound to the request's own resource
/// (`NewResourceAuthorizerVal(userInfo, authz, versionedAttr)`), and the
/// authorizer sees the request user.
#[test]
fn request_resource_authorizer_carries_the_request_identity() {
    let rec = Arc::new(Recording::default());
    let out = run(
        JsonPatcher::new_patcher(
            r#"authorizer.requestResource.check('patch').allowed()
                ? [JSONPatch{op: "replace", path: "/spec/replicas", value: 5}]
                : []"#,
        ),
        Some(rec.clone()),
    )
    .unwrap();
    assert_eq!(out["spec"]["replicas"], 5, "{out}");
    let seen = rec.0.lock().unwrap();
    assert_eq!(seen.len(), 1, "{seen:?}");
    let a = &seen[0];
    assert_eq!(a.user.username, "alice");
    assert_eq!(a.verb, "patch");
    assert_eq!(a.api_group, "apps");
    assert_eq!(a.resource, "deployments");
    assert_eq!(a.subresource.as_deref(), Some("status"));
    assert_eq!(a.namespace.as_deref(), Some("team-a"));
    assert_eq!(a.name.as_deref(), Some("d1"));
}

/// activation.go:52-55: the variables exist only when an authorizer was
/// supplied, so using one without it is a compile/eval error, not a silent
/// `false`.
#[test]
fn authorizer_variable_is_absent_without_an_authorizer() {
    for patcher in [
        JsonPatcher::new_patcher(JSON_PATCH_ON_ALLOWED),
        ApplyConfigPatcher::new_patcher(APPLY_ON_ALLOWED),
    ] {
        let err = run(patcher, None).unwrap_err();
        assert!(err.contains("authorizer"), "{err}");
    }
}
