//! A pod's `affinity` is validated, and its fields decode when absent.
//!
//! Last `pod.rs` slice of #1939. Create-side affinity validation did not exist:
//! only the gated-pod *mutation* rules did. A node selector with no terms, a
//! `matchExpressions` entry with no operator, an `In` with no values, a weight
//! of 0 or 200, a pod affinity term with no `topologyKey` — all were written.
//!
//! Upstream runs `validateAffinity` from `validatePodSpec`
//! (`pkg/apis/core/validation/validation.go:4655`), which reaches
//! `ValidateNodeSelector` (`:5034`), `ValidateNodeSelectorRequirement`
//! (`:4956`), `ValidateNodeFieldSelectorRequirement` (`:4991`),
//! `ValidatePreferredSchedulingTerms` (`:5114`) and `validatePodAffinityTerm`
//! (`:5152`).

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn containers() -> Value {
    json!([{ "name": "c", "image": "nginx" }])
}

fn spec(affinity: Value) -> Value {
    json!({ "containers": containers(), "affinity": affinity })
}

/// `(label, pod spec, substring the answer must contain)`.
fn cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "a required nodeAffinity with no terms",
            spec(json!({ "nodeAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": {} } })),
            "nodeSelectorTerms: Required value: must have at least one node selector term",
        ),
        (
            "a matchExpressions entry with no operator",
            spec(json!({ "nodeAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": { "nodeSelectorTerms": [{ "matchExpressions": [{ "key": "k" }] }] } } })),
            "matchExpressions[0].operator: Invalid value: \"\": not a valid selector operator",
        ),
        (
            "an In requirement with no values",
            spec(json!({ "nodeAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": { "nodeSelectorTerms": [{ "matchExpressions": [{ "key": "k", "operator": "In" }] }] } } })),
            "matchExpressions[0].values: Required value: must be specified when `operator` is 'In' or 'NotIn'",
        ),
        (
            "an Exists requirement carrying values",
            spec(json!({ "nodeAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": { "nodeSelectorTerms": [{ "matchExpressions": [{ "key": "k", "operator": "Exists", "values": ["v"] }] }] } } })),
            "matchExpressions[0].values: Forbidden: may not be specified when `operator` is 'Exists' or 'DoesNotExist'",
        ),
        (
            "a Gt requirement with two values",
            spec(json!({ "nodeAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": { "nodeSelectorTerms": [{ "matchExpressions": [{ "key": "k", "operator": "Gt", "values": ["1", "2"] }] }] } } })),
            "must be specified single value when `operator` is 'Lt' or 'Gt'",
        ),
        (
            "a requirement with no key",
            spec(json!({ "nodeAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": { "nodeSelectorTerms": [{ "matchExpressions": [{ "operator": "Exists" }] }] } } })),
            "matchExpressions[0].key: Invalid value: \"\"",
        ),
        (
            "a matchFields entry selecting something other than metadata.name",
            spec(json!({ "nodeAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": { "nodeSelectorTerms": [{ "matchFields": [{ "key": "spec.nodeName", "operator": "In", "values": ["n1"] }] }] } } })),
            "matchFields[0].key: Invalid value: \"spec.nodeName\": not a valid field selector key",
        ),
        (
            "a preferred scheduling term with no weight",
            spec(json!({ "nodeAffinity": { "preferredDuringSchedulingIgnoredDuringExecution": [{ "preference": { "matchExpressions": [{ "key": "k", "operator": "Exists" }] } }] } })),
            "preferredDuringSchedulingIgnoredDuringExecution[0].weight: Invalid value: 0: must be in the range 1-100",
        ),
        (
            "a pod affinity term with no topologyKey",
            spec(json!({ "podAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": [{ "labelSelector": { "matchLabels": { "a": "b" } } }] } })),
            "requiredDuringSchedulingIgnoredDuringExecution[0].topologyKey: Required value: can not be empty",
        ),
        (
            "a pod affinity term with an unknown selector operator",
            spec(json!({ "podAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": [{ "topologyKey": "zone", "labelSelector": { "matchExpressions": [{ "key": "k", "operator": "Bogus" }] } }] } })),
            "labelSelector.matchExpressions[0].operator: Invalid value: \"Bogus\"",
        ),
        (
            "a weighted anti-affinity term with weight 200",
            spec(json!({ "podAntiAffinity": { "preferredDuringSchedulingIgnoredDuringExecution": [{ "weight": 200, "podAffinityTerm": { "topologyKey": "zone" } }] } })),
            "preferredDuringSchedulingIgnoredDuringExecution[0].weight: Invalid value: 200: must be in the range 1-100",
        ),
        (
            "a weighted term with no podAffinityTerm at all",
            spec(json!({ "podAntiAffinity": { "preferredDuringSchedulingIgnoredDuringExecution": [{ "weight": 50 }] } })),
            "preferredDuringSchedulingIgnoredDuringExecution[0].podAffinityTerm.topologyKey: Required value",
        ),
        (
            "a pod affinity term naming an invalid namespace",
            spec(json!({ "podAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": [{ "topologyKey": "zone", "namespaces": ["Bad_NS"] }] } })),
            "requiredDuringSchedulingIgnoredDuringExecution[0].namespace: Invalid value: \"Bad_NS\"",
        ),
    ]
}

#[tokio::test]
async fn every_bad_affinity_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (i, (label, body_spec, expected)) in cases().into_iter().enumerate() {
        let (status, body) = api
            .send(
                "POST",
                "/api/v1/namespaces/default/pods",
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": format!("aff-{i}"), "namespace": "default" },
                    "spec": body_spec,
                })),
            )
            .await;

        assert_ne!(
            status.as_u16(),
            400,
            "{label} was rejected by the decoder, before any validation: {body}"
        );
        assert_eq!(
            status.as_u16(),
            422,
            "{label} must be Invalid, not {status}: {body}"
        );
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected),
            "{label} must report `{expected}`, got: {message}"
        );
    }
}

/// A pod affinity term with **no** `labelSelector` is legal: upstream's
/// `validatePodAffinityTerm` passes `nil` straight to `ValidateLabelSelector`,
/// which returns no errors for it (`:5130-5137`). It is not a no-op, though —
/// `LabelSelectorAsSelector(nil)` is `labels.Nothing()`
/// (`apimachinery/pkg/apis/meta/v1/helpers.go:37-43`), so the term matches no
/// pods. That is why the field is `Option` rather than a defaulted value: an
/// absent selector and an empty one mean opposite things, and defaulting the
/// absent one to `{}` would silently turn "match nothing" into "match
/// everything".
#[tokio::test]
async fn a_pod_affinity_term_without_a_label_selector_is_written() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            "/api/v1/namespaces/default/pods",
            Some("application/json"),
            Some(&json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": "aff-nil-selector", "namespace": "default" },
                "spec": spec(json!({
                    "podAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": [
                            { "topologyKey": "kubernetes.io/hostname" }
                        ]
                    }
                })),
            })),
        )
        .await;

    assert!(status.is_success(), "{status} {body}");
    let term = &body["spec"]["affinity"]["podAffinity"]
        ["requiredDuringSchedulingIgnoredDuringExecution"][0];
    assert!(
        term.get("labelSelector").is_none(),
        "an absent labelSelector must not be materialised as an empty one, \
         which would select every pod instead of none: {term}"
    );
}

/// Well-formed affinity of each kind must still be written.
#[tokio::test]
async fn well_formed_affinity_is_written() {
    let api = TestApiServer::new();

    let affinities = [
        json!({ "nodeAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": { "nodeSelectorTerms": [{ "matchExpressions": [{ "key": "disk", "operator": "In", "values": ["ssd"] }] }] } } }),
        json!({ "nodeAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": { "nodeSelectorTerms": [{ "matchFields": [{ "key": "metadata.name", "operator": "In", "values": ["node-1"] }] }] } } }),
        json!({ "nodeAffinity": { "preferredDuringSchedulingIgnoredDuringExecution": [{ "weight": 100, "preference": { "matchExpressions": [{ "key": "zone", "operator": "Exists" }] } }] } }),
        json!({ "podAffinity": { "requiredDuringSchedulingIgnoredDuringExecution": [{ "topologyKey": "zone", "labelSelector": { "matchLabels": { "app": "web" } }, "namespaces": ["default"] }] } }),
        json!({ "podAntiAffinity": { "preferredDuringSchedulingIgnoredDuringExecution": [{ "weight": 1, "podAffinityTerm": { "topologyKey": "zone" } }] } }),
    ];

    for (i, affinity) in affinities.iter().enumerate() {
        let (status, body) = api
            .send(
                "POST",
                "/api/v1/namespaces/default/pods",
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": format!("good-aff-{i}"), "namespace": "default" },
                    "spec": spec(affinity.clone()),
                })),
            )
            .await;
        assert!(
            status.is_success(),
            "well-formed affinity must be written: {affinity} -> {status} {body}"
        );
    }
}
