//! IntOrString must preserve the int-vs-string distinction on the wire.
//!
//! Upstream encodes `IntOrString` by its `Type` discriminator
//! (apimachinery/pkg/util/intstr/intstr.go `MarshalJSON`): an `Int` marshals as
//! a JSON **number**, a `String` as a JSON **string**. Clients rely on that
//! distinction — `intstr.UnmarshalJSON` assigns `Type = String` for anything
//! quoted, and `GetScaledValueFromIntOrPercent` then rejects a string that is
//! not a percentage with "invalid type: string is not a percentage".
//!
//! Serving `"maxUnavailable": "1"` where upstream serves `1` therefore wedges
//! the real (Go) DaemonSet controller, which is exactly what happened to
//! kube-proxy and kindnet in the vanilla-swap api-server leg:
//!
//!   kube-system/kube-proxy failed with : couldn't get unavailable numbers:
//!   invalid value for MaxUnavailable: invalid value for IntOrString:
//!   invalid type: string is not a percentage
//!
//! Defaults per pkg/apis/apps/v1/defaults.go: DaemonSet maxUnavailable
//! `intstr.FromInt32(1)`, maxSurge `intstr.FromInt32(0)` — both integers.
//! Deployment defaults are `intstr.FromString("25%")` — a string.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn template() -> Value {
    json!({
        "metadata": {"labels": {"app": "a"}},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
    })
}

fn ds(name: &str, strategy: Option<Value>) -> Value {
    let mut spec = json!({
        "selector": {"matchLabels": {"app": "a"}},
        "template": template(),
    });
    if let Some(s) = strategy {
        spec["updateStrategy"] = s;
    }
    json!({
        "apiVersion": "apps/v1", "kind": "DaemonSet",
        "metadata": {"name": name}, "spec": spec
    })
}

fn sts(name: &str, strategy: Option<Value>) -> Value {
    let mut spec = json!({
        "serviceName": "svc",
        "selector": {"matchLabels": {"app": "a"}},
        "template": template(),
    });
    if let Some(s) = strategy {
        spec["updateStrategy"] = s;
    }
    json!({
        "apiVersion": "apps/v1", "kind": "StatefulSet",
        "metadata": {"name": name}, "spec": spec
    })
}

/// A JSON number must survive the round trip as a number, never as a string.
/// This is the shape kubeadm ships for kube-proxy. `maxUnavailable` is 0 here
/// because ValidateRollingUpdateDaemonSet forbids a non-zero `maxSurge`
/// alongside a non-zero `maxUnavailable`; the 1/0 default pair is covered by
/// `daemonset_defaults_are_integers_not_strings` below.
#[tokio::test]
async fn daemonset_integer_max_unavailable_stays_an_integer() {
    let state = TestApiServer::new();
    let uri = format!("/apis/apps/v1/namespaces/{NS}/daemonsets");
    let (code, body) = state
        .post(
            &uri,
            &ds(
                "ds-int",
                Some(json!({
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 2}
                })),
            ),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    let ru = &body["spec"]["updateStrategy"]["rollingUpdate"];
    assert_eq!(ru["maxUnavailable"], json!(0), "must be a number: {body}");
    assert_eq!(ru["maxSurge"], json!(2), "must be a number: {body}");

    // and again on the read path, not just the create response
    let (code, got) = state.get(&format!("{uri}/ds-int")).await;
    assert_eq!(code, StatusCode::OK, "{got}");
    let ru = &got["spec"]["updateStrategy"]["rollingUpdate"];
    assert_eq!(ru["maxUnavailable"], json!(0), "must be a number: {got}");
    assert_eq!(ru["maxSurge"], json!(2), "must be a number: {got}");
}

/// A percentage must stay a string — the other half of the discriminator.
#[tokio::test]
async fn daemonset_percentage_max_unavailable_stays_a_string() {
    let state = TestApiServer::new();
    let uri = format!("/apis/apps/v1/namespaces/{NS}/daemonsets");
    let (code, body) = state
        .post(
            &uri,
            &ds(
                "ds-pct",
                Some(json!({
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxUnavailable": "10%", "maxSurge": "0%"}
                })),
            ),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    let ru = &body["spec"]["updateStrategy"]["rollingUpdate"];
    assert_eq!(ru["maxUnavailable"], json!("10%"), "{body}");
    assert_eq!(ru["maxSurge"], json!("0%"), "{body}");
}

/// SetDefaults_DaemonSet: `intstr.FromInt32(1)` / `intstr.FromInt32(0)` — both
/// integers. Defaulting must not re-introduce the string form.
#[tokio::test]
async fn daemonset_defaults_are_integers_not_strings() {
    let state = TestApiServer::new();
    let uri = format!("/apis/apps/v1/namespaces/{NS}/daemonsets");
    let (code, body) = state.post(&uri, &ds("ds-default", None)).await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    let ru = &body["spec"]["updateStrategy"]["rollingUpdate"];
    assert_eq!(ru["maxUnavailable"], json!(1), "{body}");
    assert_eq!(ru["maxSurge"], json!(0), "{body}");
}

/// Defaulting one half must not stringify the half the user supplied.
#[tokio::test]
async fn daemonset_partial_rolling_update_defaults_the_missing_half_as_an_integer() {
    let state = TestApiServer::new();
    let uri = format!("/apis/apps/v1/namespaces/{NS}/daemonsets");
    let (code, body) = state
        .post(
            &uri,
            &ds(
                "ds-partial",
                Some(json!({
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxUnavailable": "25%"}
                })),
            ),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    let ru = &body["spec"]["updateStrategy"]["rollingUpdate"];
    assert_eq!(ru["maxUnavailable"], json!("25%"), "{body}");
    assert_eq!(ru["maxSurge"], json!(0), "must be a number: {body}");
}

/// StatefulSet shares the same lossy helper, so it shares the same bug.
#[tokio::test]
async fn statefulset_integer_max_unavailable_stays_an_integer() {
    let state = TestApiServer::new();
    let uri = format!("/apis/apps/v1/namespaces/{NS}/statefulsets");
    let (code, body) = state
        .post(
            &uri,
            &sts(
                "sts-int",
                Some(json!({
                    "type": "RollingUpdate",
                    "rollingUpdate": {"partition": 0, "maxUnavailable": 1}
                })),
            ),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    let ru = &body["spec"]["updateStrategy"]["rollingUpdate"];
    assert_eq!(ru["maxUnavailable"], json!(1), "must be a number: {body}");
}

#[tokio::test]
async fn statefulset_percentage_max_unavailable_stays_a_string() {
    let state = TestApiServer::new();
    let uri = format!("/apis/apps/v1/namespaces/{NS}/statefulsets");
    let (code, body) = state
        .post(
            &uri,
            &sts(
                "sts-pct",
                Some(json!({
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxUnavailable": "50%"}
                })),
            ),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    let ru = &body["spec"]["updateStrategy"]["rollingUpdate"];
    assert_eq!(ru["maxUnavailable"], json!("50%"), "{body}");
}

/// Deployment already round-trips losslessly; pin it so the lateral cleanup
/// cannot regress the one site that was already correct.
#[tokio::test]
async fn deployment_int_and_percent_both_round_trip() {
    let state = TestApiServer::new();
    let uri = format!("/apis/apps/v1/namespaces/{NS}/deployments");
    let body = json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": "dep-mixed"},
        "spec": {
            "selector": {"matchLabels": {"app": "a"}},
            "template": template(),
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": 1, "maxSurge": "25%"}
            }
        }
    });
    let (code, got) = state.post(&uri, &body).await;
    assert_eq!(code, StatusCode::CREATED, "{got}");
    let ru = &got["spec"]["strategy"]["rollingUpdate"];
    assert_eq!(ru["maxUnavailable"], json!(1), "must be a number: {got}");
    assert_eq!(ru["maxSurge"], json!("25%"), "{got}");
}
