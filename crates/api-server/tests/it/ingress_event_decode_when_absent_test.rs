//! `ingress.rs`, `ingressclass.rs` and `event.rs` fields decode when absent.
//!
//! Small-module slice of #1939. Every field here already had a validator — the
//! only thing missing was reachability: a body upstream answers with 422
//! Invalid and a field path answered serde's 400 BadRequest instead, which
//! carries no `Status`, no `reason` and no `details.causes`.
//!
//! Upstream has no required JSON fields. `HTTPIngressPath.PathType` is a
//! pointer whose absence `validateHTTPIngressPath`
//! (`pkg/apis/networking/validation/validation.go`) reports as
//! `Required(pathType)`; `IngressClassSpec.Controller`,
//! `IngressServiceBackend.Name` and `EventSeries.Count` /
//! `.LastObservedTime` are plain fields whose zero value the validator
//! rejects (`ValidateIngressClassSpec`, `validateIngressBackend`,
//! `pkg/apis/core/validation/events.go`).

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn ingress(rule: Value) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "Ingress",
        "metadata": { "name": "ing", "namespace": "default" },
        "spec": { "rules": [rule] }
    })
}

/// `(label, path, body, substring the answer must contain)`.
fn cases() -> Vec<(&'static str, &'static str, Value, &'static str)> {
    let ing_path = "/apis/networking.k8s.io/v1/namespaces/default/ingresses";
    vec![
        (
            "an http rule with no paths",
            ing_path,
            ingress(json!({ "host": "a.example.com", "http": {} })),
            "spec.rules[0].http.paths: Required value",
        ),
        (
            "a path with no pathType",
            ing_path,
            ingress(json!({ "host": "a.example.com", "http": { "paths": [
                { "path": "/", "backend": { "service": { "name": "s", "port": { "number": 80 } } } }
            ] } })),
            "spec.rules[0].http.paths[0].pathType: Required value: pathType must be specified",
        ),
        (
            "a path with no backend",
            ing_path,
            ingress(json!({ "host": "a.example.com", "http": { "paths": [
                { "path": "/", "pathType": "Prefix" }
            ] } })),
            "spec.rules[0].http.paths[0].backend: Required value: must specify a service or resource",
        ),
        (
            "a service backend with no name",
            ing_path,
            ingress(json!({ "host": "a.example.com", "http": { "paths": [
                { "path": "/", "pathType": "Prefix", "backend": { "service": { "port": { "number": 80 } } } }
            ] } })),
            "spec.rules[0].http.paths[0].backend.service.name: Required value",
        ),
        (
            "a resource backend with no kind",
            ing_path,
            ingress(json!({ "host": "a.example.com", "http": { "paths": [
                { "path": "/", "pathType": "Prefix", "backend": { "resource": { "name": "r" } } }
            ] } })),
            "spec.rules[0].http.paths[0].backend.resource.kind: Required value",
        ),
        (
            "an ingressClass with no controller",
            "/apis/networking.k8s.io/v1/ingressclasses",
            json!({
                "apiVersion": "networking.k8s.io/v1", "kind": "IngressClass",
                "metadata": { "name": "ic" }, "spec": {}
            }),
            "spec.controller: Required value",
        ),
        (
            "ingressClass parameters with no kind",
            "/apis/networking.k8s.io/v1/ingressclasses",
            json!({
                "apiVersion": "networking.k8s.io/v1", "kind": "IngressClass",
                "metadata": { "name": "ic-params" },
                "spec": { "controller": "acme.io/ingress", "parameters": { "name": "p" } }
            }),
            "spec.parameters.kind: Required value",
        ),
        (
            "an event series with no count",
            "/apis/events.k8s.io/v1/namespaces/default/events",
            json!({
                "apiVersion": "events.k8s.io/v1", "kind": "Event",
                "metadata": { "name": "ev-count", "namespace": "default" },
                "eventTime": "2026-01-01T00:00:00.000000Z",
                "reportingController": "c", "reportingInstance": "i",
                "action": "a", "reason": "r", "type": "Normal",
                "series": { "lastObservedTime": "2026-01-01T00:00:00.000000Z" }
            }),
            "series.count",
        ),
        (
            "an event series with no lastObservedTime",
            "/apis/events.k8s.io/v1/namespaces/default/events",
            json!({
                "apiVersion": "events.k8s.io/v1", "kind": "Event",
                "metadata": { "name": "ev-time", "namespace": "default" },
                "eventTime": "2026-01-01T00:00:00.000000Z",
                "reportingController": "c", "reportingInstance": "i",
                "action": "a", "reason": "r", "type": "Normal",
                "series": { "count": 2 }
            }),
            "series.lastObservedTime: Required value",
        ),
    ]
}

#[tokio::test]
async fn a_body_upstream_rejects_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (label, path, body, expected) in cases() {
        let (status, resp) = api
            .send("POST", path, Some("application/json"), Some(&body))
            .await;

        assert_ne!(
            status.as_u16(),
            400,
            "{label} was rejected by the decoder, before any validation: {resp}"
        );
        assert_eq!(
            status.as_u16(),
            422,
            "{label} must be Invalid, not {status}: {resp}"
        );
        let message = resp["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected),
            "{label} must report `{expected}`, got: {message}"
        );
    }
}

/// The accept side. `ServiceAccount.imagePullSecrets[]` carries no upstream
/// obligation — `ValidateServiceAccount`
/// (`pkg/apis/core/validation/validation.go`) validates only its ObjectMeta —
/// so an entry with no `name` must be *written*, not rejected. The well-formed
/// Ingress and IngressClass keep a validator that rejects everything from
/// passing the table above.
#[tokio::test]
async fn a_body_upstream_accepts_is_written() {
    let api = TestApiServer::new();

    let accepted: Vec<(&str, &str, Value)> = vec![
        (
            "a serviceAccount with a nameless imagePullSecret",
            "/api/v1/namespaces/default/serviceaccounts",
            json!({
                "apiVersion": "v1", "kind": "ServiceAccount",
                "metadata": { "name": "sa", "namespace": "default" },
                "imagePullSecrets": [{}]
            }),
        ),
        (
            "a well-formed ingress",
            "/apis/networking.k8s.io/v1/namespaces/default/ingresses",
            ingress(json!({ "host": "a.example.com", "http": { "paths": [
                {
                    "path": "/", "pathType": "Prefix",
                    "backend": { "service": { "name": "s", "port": { "number": 80 } } }
                }
            ] } })),
        ),
        (
            "a well-formed ingressClass",
            "/apis/networking.k8s.io/v1/ingressclasses",
            json!({
                "apiVersion": "networking.k8s.io/v1", "kind": "IngressClass",
                "metadata": { "name": "ic-ok" },
                "spec": { "controller": "acme.io/ingress" }
            }),
        ),
        (
            "an event with no series at all",
            "/apis/events.k8s.io/v1/namespaces/default/events",
            json!({
                "apiVersion": "events.k8s.io/v1", "kind": "Event",
                "metadata": { "name": "ev-ok", "namespace": "default" },
                "eventTime": "2026-01-01T00:00:00.000000Z",
                "reportingController": "c", "reportingInstance": "i",
                "action": "a", "reason": "r", "type": "Normal"
            }),
        ),
    ];

    for (label, path, body) in accepted {
        let (status, resp) = api
            .send("POST", path, Some("application/json"), Some(&body))
            .await;
        assert!(
            status.is_success(),
            "{label} must be written: {status} {resp}"
        );
    }
}

/// A collection body with no `items` key decodes to an empty list — upstream's
/// `EventList.Items` is a plain slice, so an absent key is `nil`, not an error.
#[tokio::test]
async fn a_list_without_items_decodes() {
    use rusternetes_common::resources::event::{EventList, EventV1List};

    let list: EventList = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "EventList"
    }))
    .expect("an EventList with no items must decode");
    assert!(list.items.is_empty());

    let v1: EventV1List = serde_json::from_value(json!({
        "apiVersion": "events.k8s.io/v1", "kind": "EventList"
    }))
    .expect("an events.k8s.io/v1 EventList with no items must decode");
    assert!(v1.items.is_empty());
}
