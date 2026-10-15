//! Mirrors of the server-side field validation conformance cases (#3758).
//!
//! Upstream (`release-1.35`), `test/e2e/apimachinery/field_validation.go`:
//!
//! | case | line |
//! |---|---|
//! | typed object, unknown and duplicate fields | `:62` |
//! | typed object, unknown metadata field | `:115` |
//! | valid CR, CRD with validation schema | `:166` |
//! | CR with unknown fields, CRD with no schema | `:287` |
//! | invalid CR with extra properties, CRD with schema | `:348` |
//! | unknown metadata in root and embedded object of a CR | `:472` |
//! | duplicates in a CR when preserving unknown fields | `:620` |
//!
//! Each body is sent over HTTP exactly as the upstream raw REST client sends
//! it (`fieldValidation=Strict`, `fieldManager=field_validation_mgr`), and the
//! upstream substring assertion is checked on the response body.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";

async fn raw(api: &TestApiServer, method: &str, uri: &str, ct: &str, body: &str) -> (u16, String) {
    let (s, _h, bytes, _v) = api
        .send_with_headers(
            method,
            uri,
            &[("content-type", ct)],
            Some(body.as_bytes().to_vec()),
        )
        .await;
    (s.as_u16(), String::from_utf8_lossy(&bytes).into_owned())
}

/// `err.Error()` of a client-go `StatusError`: the Status `message`.
fn status_message(body: &str) -> String {
    serde_json::from_str::<Value>(body).unwrap()["message"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn namespace(api: &TestApiServer, name: &str) {
    let (s, v) = api
        .post(
            "/api/v1/namespaces",
            &json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":name}}),
        )
        .await;
    assert_eq!(s, 201, "{v}");
}

/// `fixtures.NewRandomNameMultipleVersionCustomResourceDefinition(ClusterScoped)`
/// (apiextensions-apiserver/test/integration/fixtures/resources.go:83-119) with
/// an optional shared schema on every version, then
/// `CreateNewV1CustomResourceDefinition` (`:410`), which waits for discovery.
async fn create_crd(api: &TestApiServer, schema: Option<&Value>) -> (String, String, String) {
    let g = "fooa";
    let allow_all =
        json!({"openAPIV3Schema": {"type":"object","x-kubernetes-preserve-unknown-fields": true}});
    let schema = schema.cloned().unwrap_or(allow_all);
    let version = |n: &str, storage: bool| {
        json!({"name": n, "served": true, "storage": storage,
               "subresources": {"status": {}}, "schema": schema})
    };
    let name = format!("{g}s.mygroup.example.com");
    let (s, v) = api
        .post(
            CRDS,
            &json!({
                "apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
                "metadata": {"name": name},
                "spec": {
                    "group": "mygroup.example.com",
                    "versions": [version("v1beta1", false), version("v1", true)],
                    "names": {"plural": format!("{g}s"), "singular": g, "kind": g, "listKind": format!("{g}List")},
                    "scope": "Cluster",
                },
            }),
        )
        .await;
    assert_eq!(s, 201, "cannot create crd {v}");
    api.wait_crd_established(&name).await;
    // Upstream's `Versions[0]` is v1beta1.
    (
        format!("/apis/mygroup.example.com/v1beta1/{g}s"),
        "mygroup.example.com/v1beta1".to_string(),
        g.to_string(),
    )
}

const SCHEMA_WITH_PORTS: &str = r#"{
  "openAPIV3Schema": {
    "type": "object",
    "properties": {
      "spec": {
        "type": "object",
        "properties": {
          "foo": {"type": "string"},
          "cronSpec": {"type": "string", "pattern": "^(\\d+|\\*)(/\\d+)?(\\s+(\\d+|\\*)(/\\d+)?){4}$"},
          "ports": {
            "type": "array",
            "x-kubernetes-list-map-keys": ["containerPort", "protocol"],
            "x-kubernetes-list-type": "map",
            "items": {
              "properties": {
                "containerPort": {"format": "int32", "type": "integer"},
                "hostIP": {"type": "string"},
                "hostPort": {"format": "int32", "type": "integer"},
                "name": {"type": "string"},
                "protocol": {"type": "string"}
              },
              "required": ["containerPort", "protocol"],
              "type": "object"
            }
          }
        }
      }
    }
  }
}"#;

fn schema_with_ports() -> Value {
    serde_json::from_str(SCHEMA_WITH_PORTS).unwrap()
}

/// The `rest.Patch(types.ApplyPatchType)...Param(fieldValidation, Strict)`
/// call (field_validation.go:268-275 and siblings).
async fn strict_apply(api: &TestApiServer, base: &str, name: &str, yaml: &str) -> (u16, String) {
    raw(
        api,
        "PATCH",
        &format!("{base}/{name}?fieldManager=field_validation_mgr&fieldValidation=Strict"),
        "application/apply-patch+yaml",
        yaml,
    )
    .await
}

const DEPLOY_TAIL: &str = r#""selector": {"matchLabels": {"app": "nginx"}},
    "template": {
      "metadata": {"labels": {"app": "nginx"}},
      "spec": {"containers": [{"name": "nginx", "image": "nginx:latest"}]}
    }"#;

/// field_validation.go:62 "should detect unknown and duplicate fields of a
/// typed object".
#[tokio::test]
async fn conformance_field_validation_typed_unknown_and_duplicate() {
    let api = TestApiServer::builder().strict_namespaces().build();
    namespace(&api, "fv").await;
    // :66-91 the body, verbatim: `unknownField` and a duplicated `replicas`.
    let body = format!(
        r#"{{
  "apiVersion": "apps/v1", "kind": "Deployment",
  "metadata": {{"name": "my-dep", "labels": {{"app": "nginx"}}}},
  "spec": {{
    "unknownField": "foo",
    "replicas": 2,
    "replicas": 3,
    {DEPLOY_TAIL}
  }}
}}"#
    );
    // :92-99 POST with fieldValidation=Strict.
    let (s, text) = raw(
        &api,
        "POST",
        "/apis/apps/v1/namespaces/fv/deployments?fieldManager=field_validation_mgr&fieldValidation=Strict",
        "application/json",
        &body,
    )
    .await;
    assert_ne!(s, 201, "strict create must be rejected: {text}");
    // :100-102 `err.Error()` is the Status message, not the JSON body.
    let text = status_message(&text);
    assert!(
        text.contains(
            r#"strict decoding error: unknown field "spec.unknownField", duplicate field "spec.replicas""#
        ),
        "error missing unknown/duplicate field field, got: {text}"
    );
}

/// field_validation.go:115 "should detect unknown metadata fields of a typed
/// object".
#[tokio::test]
async fn conformance_field_validation_typed_unknown_metadata() {
    let api = TestApiServer::builder().strict_namespaces().build();
    namespace(&api, "fv").await;
    // :119-146 the body, verbatim.
    let body = format!(
        r#"{{
  "apiVersion": "apps/v1", "kind": "Deployment",
  "metadata": {{"name": "my-dep", "unknownMeta": "foo", "labels": {{"app": "nginx"}}}},
  "spec": {{ {DEPLOY_TAIL} }}
}}"#
    );
    let (s, text) = raw(
        &api,
        "POST",
        "/apis/apps/v1/namespaces/fv/deployments?fieldManager=field_validation_mgr&fieldValidation=Strict",
        "application/json",
        &body,
    )
    .await;
    assert_ne!(s, 201, "strict create must be rejected: {text}");
    // :154-156 `err.Error()` is the Status message, not the JSON body.
    let text = status_message(&text);
    assert!(
        text.contains(r#"strict decoding error: unknown field "metadata.unknownMeta""#),
        "error missing unknown metadata field, got: {text}"
    );
}

/// field_validation.go:166 "should create/apply a valid CR for CRD with
/// validation schema".
#[tokio::test]
async fn conformance_field_validation_valid_cr_with_schema() {
    let api = TestApiServer::builder().strict_namespaces().build();
    // :178-226 CRD with the ports/cronSpec schema on every version.
    let (base, api_version, kind) = create_crd(&api, Some(&schema_with_ports())).await;
    // :237-259
    let yaml = format!(
        "apiVersion: {api_version}\nkind: {kind}\nmetadata:\n  name: mytest\nspec:\n  foo: foo1\n  cronSpec: \"* * * * */5\"\n  ports:\n  - name: x\n    containerPort: 80\n    protocol: TCP"
    );
    let (s, text) = strict_apply(&api, &base, "mytest", &yaml).await;
    assert!(s == 200 || s == 201, "valid CR must apply: {s} {text}");
}

/// field_validation.go:287 "should create/apply a CR with unknown fields for
/// CRD with no validation schema".
#[tokio::test]
async fn conformance_field_validation_unknown_fields_no_schema() {
    let api = TestApiServer::builder().strict_namespaces().build();
    // :298-313 default fixture: AllowAllSchema().
    let (base, api_version, kind) = create_crd(&api, None).await;
    // :323-338
    let yaml = format!(
        "apiVersion: {api_version}\nkind: {kind}\nmetadata:\n  name: mytest\nspec:\n  unknown: uk1\n  cronSpec: \"* * * * */5\"\n  ports:\n  - name: x\n    containerPort: 80\n    protocol: TCP"
    );
    let (s, text) = strict_apply(&api, &base, "mytest", &yaml).await;
    assert!(
        s == 200 || s == 201,
        "unknown fields must be kept: {s} {text}"
    );
}

/// field_validation.go:348 "should create/apply an invalid CR with extra
/// properties for CRD with validation schema".
#[tokio::test]
async fn conformance_field_validation_invalid_cr_extra_properties() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let (base, api_version, kind) = create_crd(&api, Some(&schema_with_ports())).await;
    // :425-440 `unknownField` at the root.
    let yaml = format!(
        "apiVersion: {api_version}\nkind: {kind}\nmetadata:\n  name: mytest\nunknownField: unknown\nspec:\n  foo: foo1\n  cronSpec: \"* * * * */5\"\n  ports:\n  - name: x\n    containerPort: 80\n    protocol: TCP"
    );
    let (_s, text) = strict_apply(&api, &base, "mytest", &yaml).await;
    // :448-450
    assert!(
        text.contains(".unknownField: field not declared in schema"),
        "error missing unknown field: {text}"
    );
}

/// field_validation.go:472 "should detect unknown metadata fields in both the
/// root and embedded object of a CR".
#[tokio::test]
async fn conformance_field_validation_cr_unknown_metadata_root_and_embedded() {
    let api = TestApiServer::builder().strict_namespaces().build();
    namespace(&api, "fv").await;
    // :483-546 spec preserves unknown fields and embeds a resource at
    // spec.template.
    let mut schema = schema_with_ports();
    let spec = &mut schema["openAPIV3Schema"]["properties"]["spec"];
    spec["x-kubernetes-preserve-unknown-fields"] = json!(true);
    spec["properties"]["template"] = json!({
        "type": "object",
        "x-kubernetes-embedded-resource": true,
        "properties": {
            "metadata": {"type":"object","properties":{"name":{"type":"string"}}},
            "spec": {"type": "object"}
        }
    });
    let (base, api_version, kind) = create_crd(&api, Some(&schema)).await;
    // :578-597 (the YAML body, with ns = the framework namespace).
    let yaml = format!(
        "apiVersion: {api_version}\nkind: {kind}\nmetadata:\n  name: mytest\n  unknownMeta: unknown\nspec:\n  template:\n    apiversion: foo/v1\n    kind: Sub\n    metadata:\n        unknownSubMeta: unknown\n        name: subobject\n        namespace: fv\n  foo: foo1\n  cronSpec: \"* * * * */5\"\n  ports:\n  - name: x\n    containerPort: 80\n    protocol: TCP"
    );
    let (_s, text) = strict_apply(&api, &base, "mytest", &yaml).await;
    // :605-607 either message satisfies upstream.
    assert!(
        text.contains(".spec.template.metadata.unknownSubMeta: field not declared in schema")
            || text.contains(".metadata.unknownMeta: field not declared in schema"),
        "error missing unknown metadata field: {text}"
    );
}

/// field_validation.go:620 "should detect duplicates in a CR when preserving
/// unknown fields".
#[tokio::test]
async fn conformance_field_validation_cr_duplicates_preserving_unknown() {
    let api = TestApiServer::builder().strict_namespaces().build();
    // :631-690 spec preserves unknown fields.
    let mut schema = schema_with_ports();
    schema["openAPIV3Schema"]["properties"]["spec"]["x-kubernetes-preserve-unknown-fields"] =
        json!(true);
    let (base, api_version, kind) = create_crd(&api, Some(&schema)).await;
    // :709-722 duplicated `foo` on line 9.
    let yaml = format!(
        "\napiVersion: {api_version}\nkind: {kind}\nmetadata:\n  name: mytest\nspec:\n  unknown: uk1\n  foo: foo1\n  foo: foo2\n  cronSpec: \"* * * * */5\"\n  ports:\n  - name: x\n    containerPort: 80\n    protocol: TCP"
    );
    let (_s, text) = strict_apply(&api, &base, "mytest", &yaml).await;
    // :730-732 (the Go raw result is JSON, so its quotes are escaped).
    let plain = text.replace("\\\"", "\"");
    assert!(
        plain.contains(r#"line 9: key "foo" already set in map"#),
        "error missing duplicate field: {text}"
    );
}
