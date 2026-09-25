//! A CustomResourceDefinition's spec is validated, not rejected by the decoder.
//!
//! `crd.rs` slice of #1939. Sixteen fields of `resources/crd.rs` were required
//! at decode time — `spec.group`, `spec.names`, `spec.versions`, a version's
//! `name`/`served`/`storage`, a printer column's `jsonPath`, a scale
//! subresource's two replica paths, a selectable field's `jsonPath`, the
//! conversion `strategy`, a webhook conversion's `clientConfig` and
//! `conversionReviewVersions`, and a service reference's `namespace`/`name` —
//! so a body upstream answers with a 422 and a field path got serde's 400
//! BadRequest instead.
//!
//! Making them decode is only half of it: `validate_crd` checked a handful of
//! emptiness rules with bare strings and no field path, so most of what
//! `validateCustomResourceDefinitionSpec`
//! (`staging/src/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation/validation.go:353`)
//! rejects was accepted. Both halves land together: defaulting a field with no
//! validator behind it turns a 400 into a silent accept.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";

/// A minimal valid version block.
fn version(name: &str) -> Value {
    json!({
        "name": name,
        "served": true,
        "storage": true,
        "schema": { "openAPIV3Schema": { "type": "object" } }
    })
}

/// A CRD body whose `spec` is `spec`, named after `spec.names.plural` +
/// `spec.group` so the name rule never fires first.
fn crd(spec: Value) -> Value {
    let plural = spec["names"]["plural"].as_str().unwrap_or("widgets");
    let group = spec["group"].as_str().unwrap_or("example.com");
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("{plural}.{group}") },
        "spec": spec,
    })
}

/// A valid spec with `patch` merged into it.
fn spec_with(patch: Value) -> Value {
    let mut spec = json!({
        "group": "example.com",
        "scope": "Namespaced",
        "names": { "plural": "widgets", "kind": "Widget" },
        "versions": [version("v1")]
    });
    for (k, v) in patch.as_object().unwrap() {
        spec[k] = v.clone();
    }
    spec
}

fn cases() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "a spec with no group",
            json!({
                "scope": "Namespaced",
                "names": { "plural": "widgets", "kind": "Widget" },
                "versions": [version("v1")]
            }),
            "spec.group: Required value",
        ),
        (
            "a group that is not a domain",
            spec_with(json!({ "group": "example" })),
            "spec.group: Invalid value: \"example\": should be a domain with at least one dot",
        ),
        (
            "a spec with no versions",
            spec_with(json!({ "versions": [] })),
            "spec.versions: Invalid value: \"\": must have exactly one version marked as storage version",
        ),
        (
            "a version with no name",
            spec_with(json!({ "versions": [{
                "served": true, "storage": true,
                "schema": { "openAPIV3Schema": { "type": "object" } }
            }] })),
            "spec.versions[0].name: Invalid value: \"\"",
        ),
        (
            "two versions with the same name",
            spec_with(json!({ "versions": [version("v1"), {
                "name": "v1", "served": true, "storage": false,
                "schema": { "openAPIV3Schema": { "type": "object" } }
            }] })),
            "must contain unique version names",
        ),
        (
            "two storage versions",
            spec_with(json!({ "versions": [version("v1"), version("v2")] })),
            "must have exactly one version marked as storage version",
        ),
        (
            "a printer column with no jsonPath",
            spec_with(json!({ "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": { "openAPIV3Schema": { "type": "object" } },
                "additionalPrinterColumns": [{ "name": "Age", "type": "date" }]
            }] })),
            "spec.versions[0].additionalPrinterColumns[0].JSONPath: Required value",
        ),
        (
            "a printer column with an unsupported type",
            spec_with(json!({ "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": { "openAPIV3Schema": { "type": "object" } },
                "additionalPrinterColumns": [
                    { "name": "Age", "type": "timestamp", "jsonPath": ".metadata.creationTimestamp" }
                ]
            }] })),
            "spec.versions[0].additionalPrinterColumns[0].type: Invalid value: \"timestamp\"",
        ),
        (
            "a scale subresource with no specReplicasPath",
            spec_with(json!({ "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": { "openAPIV3Schema": { "type": "object" } },
                "subresources": { "scale": { "statusReplicasPath": ".status.replicas" } }
            }] })),
            "spec.versions[0].subresources.scale.specReplicasPath: Required value",
        ),
        (
            "a scale specReplicasPath outside .spec",
            spec_with(json!({ "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": { "openAPIV3Schema": { "type": "object" } },
                "subresources": { "scale": {
                    "specReplicasPath": ".status.replicas",
                    "statusReplicasPath": ".status.replicas"
                } }
            }] })),
            "should be a json path under .spec",
        ),
        (
            "a selectable field with no jsonPath",
            spec_with(json!({ "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": { "openAPIV3Schema": { "type": "object" } },
                "selectableFields": [{}]
            }] })),
            "spec.versions[0].selectableFields[0].jsonPath: Required value",
        ),
        (
            "a conversion with no strategy",
            spec_with(json!({ "conversion": {} })),
            "spec.conversion.strategy: Required value",
        ),
        (
            "a Webhook conversion with no webhook",
            spec_with(json!({ "conversion": { "strategy": "Webhook" } })),
            "spec.conversion.webhookClientConfig: Required value",
        ),
        (
            "a Webhook conversion whose webhook has neither url nor service",
            spec_with(json!({ "conversion": {
                "strategy": "Webhook",
                "webhook": { "clientConfig": {}, "conversionReviewVersions": ["v1"] }
            } })),
            "exactly one of url or service is required",
        ),
        (
            "a None conversion that still carries a webhook",
            spec_with(json!({ "conversion": {
                "strategy": "None",
                "webhook": {
                    "clientConfig": { "url": "https://example.com/convert" },
                    "conversionReviewVersions": ["v1"]
                }
            } })),
            "spec.conversion.webhookClientConfig: Forbidden",
        ),
        (
            "a service reference with no name",
            spec_with(json!({ "conversion": {
                "strategy": "Webhook",
                "webhook": {
                    "clientConfig": { "service": { "namespace": "default" } },
                    "conversionReviewVersions": ["v1"]
                }
            } })),
            "spec.conversion.webhookClientConfig.service.name: Required value",
        ),
        (
            "a Webhook conversion with no conversionReviewVersions",
            spec_with(json!({ "conversion": {
                "strategy": "Webhook",
                "webhook": { "clientConfig": { "url": "https://example.com/convert" } }
            } })),
            "spec.conversion.conversionReviewVersions: Required value",
        ),
        (
            "names whose kind and listKind are the same",
            spec_with(json!({
                "names": { "plural": "widgets", "kind": "Widget", "listKind": "Widget" }
            })),
            "kind and listKind may not be the same",
        ),
        (
            "a short name that is not a DNS label",
            spec_with(json!({
                "names": { "plural": "widgets", "kind": "Widget", "shortNames": ["Widget"] }
            })),
            "spec.names.shortNames[0]: Invalid value: \"Widget\"",
        ),
    ]
}

#[tokio::test]
async fn every_bad_crd_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (label, spec, expected) in cases() {
        let (status, body) = api
            .send("POST", CRDS, Some("application/json"), Some(&crd(spec)))
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

/// The accept side, plus the defaulting that has to happen before the
/// `names.singular` / `names.listKind` / `conversion` rules can be satisfied:
/// `SetDefaults_CustomResourceDefinitionSpec`
/// (`apiextensions-apiserver/pkg/apis/apiextensions/v1/defaults.go:41-53`).
#[tokio::test]
async fn a_valid_crd_is_written_with_its_names_and_conversion_defaulted() {
    let api = TestApiServer::new();

    let (status, body) = api
        .send(
            "POST",
            CRDS,
            Some("application/json"),
            Some(&crd(spec_with(json!({})))),
        )
        .await;
    assert!(
        status.is_success(),
        "a valid CRD must be written: {status} {body}"
    );
    assert_eq!(body["spec"]["names"]["singular"], "widget");
    assert_eq!(body["spec"]["names"]["listKind"], "WidgetList");
    assert_eq!(body["spec"]["conversion"]["strategy"], "None");

    // `status.storedVersions` is seeded from the storage version
    // (`defaults.go:29-38`).
    assert_eq!(body["status"]["storedVersions"][0], "v1");
}

/// A CRD carrying every construct this slice validates must still be written.
#[tokio::test]
async fn a_fully_populated_crd_is_written() {
    let api = TestApiServer::new();

    let spec = spec_with(json!({
        "names": {
            "plural": "gadgets", "kind": "Gadget", "singular": "gadget",
            "listKind": "GadgetList", "shortNames": ["gdg"], "categories": ["all"]
        },
        "versions": [{
            "name": "v1", "served": true, "storage": true,
            "schema": { "openAPIV3Schema": { "type": "object" } },
            "subresources": {
                "status": {},
                "scale": {
                    "specReplicasPath": ".spec.replicas",
                    "statusReplicasPath": ".status.replicas",
                    "labelSelectorPath": ".status.selector"
                }
            },
            "additionalPrinterColumns": [
                { "name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp" }
            ],
            "selectableFields": [{ "jsonPath": ".spec.color" }]
        }],
        "conversion": {
            "strategy": "Webhook",
            "webhook": {
                "clientConfig": {
                    "service": { "namespace": "default", "name": "converter", "port": 443 }
                },
                "conversionReviewVersions": ["v1", "v1beta1"]
            }
        }
    }));

    let (status, body) = api
        .send("POST", CRDS, Some("application/json"), Some(&crd(spec)))
        .await;
    assert!(
        status.is_success(),
        "a fully populated CRD must be written: {status} {body}"
    );
}
