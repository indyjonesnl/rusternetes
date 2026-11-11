//! Tests for `src/admission/runtime_class.rs` and its wiring, ported from
//! `plugin/pkg/admission/runtimeclass/admission_test.go` (release-1.35).
#![allow(unused_imports, dead_code)]

use std::sync::Arc;

use k3rs_api_server::admission::plugins::EnabledForTest;
use k3rs_api_server::admission::runtime_class::{set_overhead, set_scheduling, validate_overhead};
use k3rs_api_server::endpoints::handlers::admission::Admission;
use k3rs_api_server::registry::rest::RequestContext;
use k3rs_api_server::state::ApiServerState;
use k3rs_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
use k3rs_common::auth::{TokenManager, UserInfo};
use k3rs_common::authz::{AlwaysAllowAuthorizer, Authorizer};
use k3rs_common::observability::MetricsRegistry;
use k3rs_common::resources::{Pod, RuntimeClass};
use k3rs_common::{Error, Result};
use k3rs_storage::{Storage, StorageBackend};
use serde_json::{json, Value};

fn rc(v: Value) -> RuntimeClass {
    let mut base = json!({"apiVersion":"node.k8s.io/v1","kind":"RuntimeClass",
        "metadata":{"name":"foo"},"handler":"bar"});
    for (k, val) in v.as_object().unwrap() {
        base[k] = val.clone();
    }
    serde_json::from_value(base).unwrap()
}

fn pod(spec: Value) -> Pod {
    let mut s = json!({"containers":[{"name":"c","image":"i"}]});
    for (k, val) in spec.as_object().unwrap() {
        s[k] = val.clone();
    }
    serde_json::from_value(json!({"apiVersion":"v1","kind":"Pod",
        "metadata":{"name":"p","namespace":"default"},"spec":s}))
    .unwrap()
}

fn overhead(p: &Pod) -> Option<std::collections::HashMap<String, String>> {
    p.spec.as_ref().unwrap().overhead.clone()
}

// admission_test.go TestSetOverhead.
#[test]
fn set_overhead_cases() {
    let class = rc(json!({"overhead":{"podFixed":{"cpu":"100m","memory":"1"}}}));
    let mut p = pod(json!({}));
    set_overhead(&mut p, Some(&class)).unwrap();
    assert_eq!(overhead(&p).unwrap()["cpu"], "100m");
    assert_eq!(overhead(&p).unwrap()["memory"], "1");

    let class = rc(json!({"overhead":{"podFixed":{"cpu":"10","memory":"10G"}}}));
    let mut p = pod(json!({"overhead":{"cpu":"100m","memory":"1"}}));
    assert_eq!(
        set_overhead(&mut p, Some(&class)).unwrap_err(),
        "pod rejected: Pod's Overhead doesn't match RuntimeClass's defined Overhead"
    );

    // Same overhead already set (quantities compare by value).
    let class = rc(json!({"overhead":{"podFixed":{"cpu":"100m","memory":"1"}}}));
    let mut p = pod(json!({"overhead":{"cpu":"0.1","memory":"1"}}));
    set_overhead(&mut p, Some(&class)).unwrap();

    // No class / no overhead: untouched.
    let mut p = pod(json!({}));
    set_overhead(&mut p, None).unwrap();
    set_overhead(&mut p, Some(&rc(json!({})))).unwrap();
    assert!(overhead(&p).is_none());
}

// admission_test.go TestSetScheduling.
#[test]
fn set_scheduling_cases() {
    let mut p = pod(json!({"nodeSelector":{"foo":"bar"}}));
    set_scheduling(&mut p, Some(&rc(json!({})))).unwrap();
    assert_eq!(
        p.spec
            .as_ref()
            .unwrap()
            .node_selector
            .as_ref()
            .unwrap()
            .len(),
        1
    );

    let class = rc(json!({"scheduling":{"nodeSelector":{"foo":"conflict"}}}));
    let mut p = pod(json!({"nodeSelector":{"foo":"bar"}}));
    assert_eq!(
        set_scheduling(&mut p, Some(&class)).unwrap_err(),
        "conflict: runtimeClass.scheduling.nodeSelector[foo] = conflict; pod.spec.nodeSelector[foo] = bar"
    );

    let class = rc(json!({"scheduling":{"nodeSelector":{"foo":"bar"}}}));
    let mut p = pod(json!({}));
    set_scheduling(&mut p, Some(&class)).unwrap();
    assert_eq!(
        p.spec.as_ref().unwrap().node_selector.as_ref().unwrap()["foo"],
        "bar"
    );

    let class = rc(json!({"scheduling":{"nodeSelector":{"foo":"bar","fizz":"buzz"}}}));
    let mut p = pod(json!({"nodeSelector":{"foo":"bar"}}));
    set_scheduling(&mut p, Some(&class)).unwrap();
    let sel = p.spec.as_ref().unwrap().node_selector.clone().unwrap();
    assert_eq!((sel.len(), sel["fizz"].as_str()), (2, "buzz"));

    // Multiple tolerations: the duplicate is dropped, the new one appended.
    let tol =
        |k: &str, v: &str| json!({"key":k,"operator":"Equal","value":v,"effect":"NoSchedule"});
    let class = rc(json!({"scheduling":{"tolerations":[tol("foo","bar"), tol("fizz","buzz")]}}));
    let mut p = pod(json!({"nodeSelector":{"foo":"bar"},"tolerations":[tol("foo","bar")]}));
    set_scheduling(&mut p, Some(&class)).unwrap();
    let t = p.spec.as_ref().unwrap().tolerations.clone().unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t[1].key.as_deref(), Some("fizz"));
}

// admission_test.go TestValidateOverhead.
#[test]
fn validate_overhead_cases() {
    let class = rc(json!({"overhead":{"podFixed":{"cpu":"100m","memory":"1"}}}));
    validate_overhead(
        &pod(json!({"overhead":{"cpu":"100m","memory":"1"}})),
        Some(&class),
    )
    .unwrap();
    assert_eq!(
        validate_overhead(&pod(json!({})), Some(&class)).unwrap_err(),
        "pod rejected: Pod's Overhead doesn't match RuntimeClass's defined Overhead"
    );
    assert_eq!(
        validate_overhead(&pod(json!({"overhead":{"cpu":"1"}})), None).unwrap_err(),
        "pod rejected: Pod Overhead set without corresponding RuntimeClass defined Overhead"
    );
    validate_overhead(&pod(json!({})), None).unwrap();
}

fn state() -> ApiServerState {
    ApiServerState::new(
        Arc::new(StorageBackend::new_memory()),
        Arc::new(TokenManager::new(b"test-secret")),
        Arc::new(AlwaysAllowAuthorizer) as Arc<dyn Authorizer>,
        Arc::new(MetricsRegistry::new()),
        true,
    )
}

async fn create_pod(state: &ApiServerState, p: Pod, sub: Option<&str>) -> Result<Pod> {
    let gvk = GroupVersionKind {
        group: String::new(),
        version: "v1".into(),
        kind: "Pod".into(),
    };
    let gvr = GroupVersionResource {
        group: String::new(),
        version: "v1".into(),
        resource: "pods".into(),
    };
    let user = UserInfo {
        username: "alice".into(),
        uid: String::new(),
        groups: vec![],
        extra: Default::default(),
    };
    let admission = Admission {
        state,
        kind: &gvk,
        resource: &gvr,
        subresource: sub,
        namespace: Some("default"),
        user: &user,
        dry_run: false,
        forbid_create: false,
    };
    let op = Operation::Create;
    let obj = admission.admit(op.clone(), p, None).await?;
    admission
        .validate(&RequestContext::new(Some("default")), op, Some(&obj), None)
        .await?;
    Ok(obj)
}

fn msg(e: Error) -> String {
    match e {
        Error::Forbidden(m) => m,
        other => other.to_string(),
    }
}

// TestAdmit / TestValidate: through the chain, the class is read from storage.
#[tokio::test]
async fn chain_admits_and_validates_through_storage() {
    let _g = EnabledForTest::new(&["RuntimeClass"]);
    let st = state();
    let class = rc(json!({
        "overhead":{"podFixed":{"cpu":"100m"}},
        "scheduling":{"nodeSelector":{"disk":"ssd"}}}));
    st.storage
        .create("/registry/runtimeclasses/foo", &class)
        .await
        .unwrap();

    // Overhead and scheduling are set from the class.
    let out = create_pod(&st, pod(json!({"runtimeClassName":"foo"})), None)
        .await
        .unwrap();
    assert_eq!(overhead(&out).unwrap()["cpu"], "100m");
    assert_eq!(
        out.spec.as_ref().unwrap().node_selector.as_ref().unwrap()["disk"],
        "ssd"
    );

    // A differing overhead is rejected.
    let e = create_pod(
        &st,
        pod(json!({"runtimeClassName":"foo","overhead":{"cpu":"1"}})),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(
        msg(e),
        "pods \"p\" is forbidden: pod rejected: Pod's Overhead doesn't match RuntimeClass's defined Overhead"
    );

    // An unknown class is rejected.
    let e = create_pod(&st, pod(json!({"runtimeClassName":"nope"})), None)
        .await
        .unwrap_err();
    assert_eq!(
        msg(e),
        "pods \"p\" is forbidden: pod rejected: RuntimeClass \"nope\" not found"
    );

    // Overhead without a class (Validate): rejected.
    let e = create_pod(&st, pod(json!({"overhead":{"cpu":"1"}})), None)
        .await
        .unwrap_err();
    assert_eq!(
        msg(e),
        "pods \"p\" is forbidden: pod rejected: Pod Overhead set without corresponding RuntimeClass defined Overhead"
    );

    // Subresources are ignored.
    create_pod(&st, pod(json!({"overhead":{"cpu":"1"}})), Some("status"))
        .await
        .unwrap();
}

#[tokio::test]
async fn disabled_plugin_does_nothing() {
    let _g = EnabledForTest::new(&["AlwaysAdmit"]);
    let st = state();
    create_pod(&st, pod(json!({"runtimeClassName":"nope"})), None)
        .await
        .unwrap();
}
