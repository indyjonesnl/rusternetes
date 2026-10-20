//! `?fieldValidation=Warn` on an `events.k8s.io/v1` body must answer with one
//! `Warning: 299` header per strict-decoding violation (#3694).
//!
//! Upstream decodes the body into the *versioned* object and records strict
//! decoding errors as warnings on the request's recorder
//! (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/rest.go`
//! `addStrictDecodingWarnings`, :439-446), which `WithWarningRecorder` +
//! `transformResponseObject` write out as `Warning` headers. The events.k8s.io
//! handler converts the body onto the core object before the generic create
//! path sees it, so the versioned-decode warnings were being dropped.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn body(name: &str) -> Value {
    json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": { "name": name },
        "eventTime": "2026-09-08T10:05:00.123456Z",
        "reportingController": "probe",
        "reportingInstance": "probe-0",
        "action": "Probe",
        "reason": "Probe",
        "type": "Normal",
        "regarding": { "kind": "Pod", "name": "p", "namespace": NS },
        "bogusField": "x",
    })
}

async fn send(method: &str, uri: &str, value: &Value) -> (StatusCode, Vec<String>) {
    let api = TestApiServer::new();
    if method == "PUT" {
        let mut base = value.clone();
        base.as_object_mut().unwrap().remove("bogusField");
        let (code, _) = api
            .send(
                "POST",
                &format!("/apis/events.k8s.io/v1/namespaces/{NS}/events"),
                Some("application/json"),
                Some(&base),
            )
            .await;
        assert_eq!(code, StatusCode::CREATED);
    }
    let (status, headers, _bytes, _v) = api
        .send_full(
            method,
            uri,
            Some("application/json"),
            None,
            Some(serde_json::to_vec(value).unwrap()),
        )
        .await;
    let warnings = headers
        .get_all("warning")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect();
    (status, warnings)
}

#[tokio::test]
async fn create_warn_emits_unknown_field_warning() {
    let uri = format!("/apis/events.k8s.io/v1/namespaces/{NS}/events?fieldValidation=Warn");
    let (status, warnings) = send("POST", &uri, &body("w-create")).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(
        warnings
            .iter()
            .any(|w| w.starts_with("299 ") && w.contains("unknown field \\\"bogusField\\\"")),
        "{warnings:?}"
    );
}

#[tokio::test]
async fn update_warn_emits_unknown_field_warning() {
    let uri =
        format!("/apis/events.k8s.io/v1/namespaces/{NS}/events/w-update?fieldValidation=Warn");
    let (status, warnings) = send("PUT", &uri, &body("w-update")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        warnings.iter().any(|w| w.contains("bogusField")),
        "{warnings:?}"
    );
}

#[tokio::test]
async fn create_ignore_emits_no_warning() {
    let uri = format!("/apis/events.k8s.io/v1/namespaces/{NS}/events?fieldValidation=Ignore");
    let (status, warnings) = send("POST", &uri, &body("w-ignore")).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(warnings.is_empty(), "{warnings:?}");
}
