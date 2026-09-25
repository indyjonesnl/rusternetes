//! The tail of #1939: the five small `resources/` modules.
//!
//! `authentication.rs`, `binding.rs`, `endpoints.rs`, `custom_metrics.rs` and
//! `external_metrics.rs` each held a handful of fields that were required at
//! decode time. Three carry a real upstream obligation and two carry the
//! opposite one — nothing upstream validates them, because they are the
//! *response* types of read-only aggregated APIs — so this file pins both
//! halves: the rejection where upstream rejects, and the accept where it does
//! not.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

/// `TokenReview` with no token. Upstream answers this from the registry, not
/// from a validator: `BindingREST`-style, `tokenreview/storage.go:80` returns
/// `apierrors.NewBadRequest("token is required for TokenReview in
/// authentication")` — a 400 whose body is a `Status`, which is what makes it
/// different from serde's decode failure (also a 400, but with a Go-shaped
/// deserializer message and no upstream wording).
#[tokio::test]
async fn a_token_review_with_no_token_reports_the_upstream_message() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            "/apis/authentication.k8s.io/v1/tokenreviews",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authentication.k8s.io/v1",
                "kind": "TokenReview",
                "spec": {}
            })),
        )
        .await;

    assert_eq!(status.as_u16(), 400, "must be BadRequest: {body}");
    assert_eq!(
        body["message"].as_str().unwrap_or_default(),
        "token is required for TokenReview in authentication",
        "must carry upstream's message, not the decoder's: {body}"
    );
    assert_eq!(body["reason"], "BadRequest");
}

/// `ValidatePodBinding` (`pkg/apis/core/validation/validation.go:6527-6539`):
/// an empty `target.name` is `Required` at that path.
#[tokio::test]
async fn a_binding_with_no_target_name_reports_a_field_path() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            "/api/v1/namespaces/default/pods",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": { "name": "bindee", "namespace": "default" },
                "spec": { "containers": [{ "name": "c", "image": "busybox" }] }
            })),
        )
        .await;
    assert!(status.is_success(), "pod must be created: {body}");

    let (status, body) = api
        .send(
            "POST",
            "/api/v1/namespaces/default/pods/bindee/binding",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "v1", "kind": "Binding",
                "metadata": { "name": "bindee", "namespace": "default" },
                "target": { "kind": "Node" }
            })),
        )
        .await;
    assert_eq!(status.as_u16(), 422, "must be Invalid: {body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("target.name: Required value"),
        "must report the field path: {body}"
    );
}

/// `validateEndpointPort` (`validation.go:8336-8355`): an absent port is `0`,
/// which `IsValidPortNum` rejects. The validator already said so — the field
/// simply could not reach it.
#[tokio::test]
async fn an_endpoint_port_that_is_absent_is_invalid_not_undecodable() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            "/api/v1/namespaces/default/endpoints",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "v1", "kind": "Endpoints",
                "metadata": { "name": "ep", "namespace": "default" },
                "subsets": [{
                    "addresses": [{ "ip": "10.0.0.1" }],
                    "ports": [{ "protocol": "TCP" }]
                }]
            })),
        )
        .await;

    assert_ne!(
        status.as_u16(),
        400,
        "must not be rejected by the decoder: {body}"
    );
    assert_eq!(status.as_u16(), 422, "must be Invalid: {body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("subsets[0].ports[0].port: Invalid value: 0"),
        "must report the port path: {body}"
    );
}

/// The negative half. `custom.metrics.k8s.io` and `external.metrics.k8s.io` are
/// read-only aggregated APIs: upstream has no create path and therefore no
/// validation for these types at all, so the obligation is that they *decode*,
/// not that they are rejected.
#[tokio::test]
async fn the_metrics_response_types_decode_when_fields_are_absent() {
    use rusternetes_common::resources::custom_metrics::{MetricValue, MetricValueList};
    use rusternetes_common::resources::external_metrics::{
        ExternalMetricValue, ExternalMetricValueList,
    };

    let value: MetricValue = serde_json::from_value(json!({})).expect("MetricValue");
    assert!(value.metric_name.is_empty());
    assert!(value.value.is_empty());
    assert!(value.described_object.kind.is_empty());

    let list: MetricValueList =
        serde_json::from_value(json!({ "apiVersion": "custom.metrics.k8s.io/v1beta2" }))
            .expect("MetricValueList");
    assert!(list.items.is_empty());

    let external: ExternalMetricValue = serde_json::from_value(json!({})).expect("value");
    assert!(external.metric_name.is_empty());

    let external_list: ExternalMetricValueList =
        serde_json::from_value(json!({ "apiVersion": "external.metrics.k8s.io/v1beta1" }))
            .expect("ExternalMetricValueList");
    assert!(external_list.items.is_empty());
}

/// The accept side of the three that do carry an obligation.
#[tokio::test]
async fn the_well_formed_bodies_are_still_accepted() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            "/apis/authentication.k8s.io/v1/tokenreviews",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "authentication.k8s.io/v1",
                "kind": "TokenReview",
                "spec": { "token": "not-a-real-token" }
            })),
        )
        .await;
    assert!(
        status.is_success(),
        "a TokenReview with a token is answered, not rejected: {status} {body}"
    );

    let (status, body) = api
        .send(
            "POST",
            "/api/v1/namespaces/default/endpoints",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "v1", "kind": "Endpoints",
                "metadata": { "name": "ep-ok", "namespace": "default" },
                "subsets": [{
                    "addresses": [{ "ip": "10.0.0.1" }],
                    "ports": [{ "port": 80, "protocol": "TCP" }]
                }]
            })),
        )
        .await;
    assert!(status.is_success(), "a valid Endpoints is written: {body}");
}
