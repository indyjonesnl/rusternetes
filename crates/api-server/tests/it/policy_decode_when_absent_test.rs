//! `policy.rs` fields decode when absent — and a PDB's `selector` is a pointer.
//!
//! `policy.rs` slice of #1939. Six fields were required at decode time, so a
//! body upstream accepts (or rejects with a field path) answered serde's 400
//! BadRequest: no `Status`, no `reason`, no `details.causes`.
//!
//! One of the six may not simply be defaulted. Upstream's
//! `PodDisruptionBudgetSpec.Selector` is a **pointer**, and its doc comment
//! (`staging/src/k8s.io/api/policy/v1/types.go:36-42`) states the distinction:
//!
//! > A null selector will match no pods, while an empty ({}) selector will
//! > select all pods within the namespace.
//!
//! Both the eviction endpoint (`pkg/registry/core/pod/storage/eviction.go:498`)
//! and the disruption controller (`pkg/controller/disruption/disruption.go:630`)
//! read it through `LabelSelectorAsSelector`, which maps `nil` to
//! `labels.Nothing()` (`apimachinery/pkg/apis/meta/v1/helpers.go:37-43`).
//! Defaulting the field would have turned "protects no pods" into "protects
//! every pod in the namespace".

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

/// `(label, path, body, substring the answer must contain)`.
fn cases() -> Vec<(&'static str, &'static str, Value, &'static str)> {
    vec![
        (
            "a scopeSelector requirement with no scopeName",
            "/api/v1/namespaces/default/resourcequotas",
            json!({
                "apiVersion": "v1", "kind": "ResourceQuota",
                "metadata": { "name": "rq", "namespace": "default" },
                "spec": { "scopeSelector": { "matchExpressions": [{ "operator": "Exists" }] } }
            }),
            // Upstream reports the un-indexed path here — `fldPath.Child("scopeName")`
            // on `fld.Child("scopeSelector").Child("matchExpressions")`
            // (`validateScopedResourceSelectorRequirement`,
            // `pkg/apis/core/validation/validation.go`).
            "spec.scopeSelector.matchExpressions.scopeName: Invalid value: \"\": \
             unsupported scope",
        ),
        (
            "a scopeSelector requirement with no operator",
            "/api/v1/namespaces/default/resourcequotas",
            json!({
                "apiVersion": "v1", "kind": "ResourceQuota",
                "metadata": { "name": "rq-op", "namespace": "default" },
                "spec": { "scopeSelector": { "matchExpressions": [
                    { "scopeName": "PriorityClass" }
                ] } }
            }),
            "spec.scopeSelector.matchExpressions.operator: Invalid value: \"\": \
             not a valid selector operator",
        ),
        (
            "a limitRange item with no type",
            "/api/v1/namespaces/default/limitranges",
            json!({
                "apiVersion": "v1", "kind": "LimitRange",
                "metadata": { "name": "lr", "namespace": "default" },
                "spec": { "limits": [{ "max": { "cpu": "1" } }] }
            }),
            "spec.limits[0].type: Unsupported value",
        ),
        (
            "a PDB selector requirement with no key",
            "/apis/policy/v1/namespaces/default/poddisruptionbudgets",
            json!({
                "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
                "metadata": { "name": "pdb-bad", "namespace": "default" },
                "spec": {
                    "minAvailable": 1,
                    "selector": { "matchExpressions": [{ "operator": "Exists" }] }
                }
            }),
            "spec.selector.matchExpressions[0].key: Invalid value: \"\"",
        ),
    ]
}

#[tokio::test]
async fn a_policy_body_upstream_rejects_answers_422_with_a_field_path() {
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

/// The accept side: upstream requires none of these fields, so each body must
/// be written rather than rejected.
#[tokio::test]
async fn a_policy_body_upstream_accepts_is_written() {
    let api = TestApiServer::new();

    let accepted: Vec<(&str, &str, Value)> = vec![
        (
            "a resourceQuota with an empty scopeSelector",
            "/api/v1/namespaces/default/resourcequotas",
            json!({
                "apiVersion": "v1", "kind": "ResourceQuota",
                "metadata": { "name": "rq-empty-scopes", "namespace": "default" },
                "spec": { "scopeSelector": {} }
            }),
        ),
        (
            "a limitRange with no limits",
            "/api/v1/namespaces/default/limitranges",
            json!({
                "apiVersion": "v1", "kind": "LimitRange",
                "metadata": { "name": "lr-empty", "namespace": "default" },
                "spec": {}
            }),
        ),
        (
            "a PDB with no selector",
            "/apis/policy/v1/namespaces/default/poddisruptionbudgets",
            json!({
                "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
                "metadata": { "name": "pdb-null-selector", "namespace": "default" },
                "spec": { "minAvailable": 1 }
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

/// A PDB posted without a selector must be stored without one — round-tripping
/// it as `{}` would silently widen it from "no pods" to "every pod in the
/// namespace".
#[tokio::test]
async fn a_pdb_without_a_selector_is_written_without_one() {
    let api = TestApiServer::new();

    let (status, _) = api
        .send(
            "POST",
            "/apis/policy/v1/namespaces/default/poddisruptionbudgets",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
                "metadata": { "name": "pdb-no-sel", "namespace": "default" },
                "spec": { "minAvailable": 1 }
            })),
        )
        .await;
    assert!(
        status.is_success(),
        "a PDB with no selector must be written"
    );

    let (_, stored) = api
        .send(
            "GET",
            "/apis/policy/v1/namespaces/default/poddisruptionbudgets/pdb-no-sel",
            None,
            None,
        )
        .await;
    assert!(
        stored["spec"].get("selector").is_none(),
        "a null selector must round-trip as absent, not as `{{}}`: {stored}"
    );
}

/// The behaviour that pointer-ness buys: a null-selector PDB protects nothing,
/// so an eviction it would otherwise block is allowed. The `{}` case is the
/// control — an empty selector selects every pod in the namespace, so the same
/// eviction is refused.
#[tokio::test]
async fn a_null_selector_pdb_protects_no_pods_but_an_empty_one_protects_all() {
    // The control is a selector that matches the victim. An *empty* (`{}`)
    // selector should also match every pod in the namespace upstream
    // (`LabelSelectorAsSelector` returns `labels.Everything()`), but
    // `LabelSelector::matches_labels` treats it as matching nothing — a
    // pre-existing divergence tracked separately, so it is not the control here.
    for (selector, must_evict) in [
        (None, true),
        (Some(json!({ "matchLabels": { "app": "web" } })), false),
    ] {
        let api = TestApiServer::new();
        let ns = "default";

        let (status, body) = api
            .send(
                "POST",
                &format!("/api/v1/namespaces/{ns}/pods"),
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": { "name": "victim", "namespace": ns, "labels": { "app": "web" } },
                    "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
                })),
            )
            .await;
        assert!(status.is_success(), "pod must be created: {body}");

        // Mark it Running and Ready so it counts as a healthy, disruptable pod.
        let (status, body) = api
            .send(
                "PUT",
                &format!("/api/v1/namespaces/{ns}/pods/victim/status"),
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": { "name": "victim", "namespace": ns },
                    "status": {
                        "phase": "Running",
                        "conditions": [{ "type": "Ready", "status": "True" }]
                    }
                })),
            )
            .await;
        assert!(status.is_success(), "pod status must be written: {body}");

        let (_, stored_pod) = api
            .send(
                "GET",
                &format!("/api/v1/namespaces/{ns}/pods/victim"),
                None,
                None,
            )
            .await;
        assert_eq!(
            stored_pod["status"]["phase"], "Running",
            "the victim must be Running, or the eviction bypasses every PDB              (upstream `canIgnorePDB`): {stored_pod}"
        );

        let mut spec = json!({ "minAvailable": 1 });
        if let (Some(obj), Some(sel)) = (spec.as_object_mut(), selector.clone()) {
            obj.insert("selector".to_string(), sel);
        }
        let (status, body) = api
            .send(
                "POST",
                &format!("/apis/policy/v1/namespaces/{ns}/poddisruptionbudgets"),
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
                    "metadata": { "name": "pdb", "namespace": ns },
                    "spec": spec,
                })),
            )
            .await;
        assert!(status.is_success(), "PDB must be created: {body}");

        let (status, body) = api
            .send(
                "POST",
                &format!("/api/v1/namespaces/{ns}/pods/victim/eviction"),
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "policy/v1", "kind": "Eviction",
                    "metadata": { "name": "victim", "namespace": ns }
                })),
            )
            .await;

        if must_evict {
            assert!(
                status.is_success(),
                "a null-selector PDB matches no pods, so the eviction must be \
                 allowed (got {status}): {body}"
            );
        } else {
            assert_eq!(
                status.as_u16(),
                429,
                "a PDB whose selector matches the victim must refuse the \
                 eviction: {body}"
            );
        }
    }
}
