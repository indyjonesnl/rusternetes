//! The MutatingAdmissionPolicy `jsonPatch` patcher.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/
//! mutating/patch/json_patch.go` (`NewJSONPatcher`, `jsonPatcher.Patch`,
//! `evaluatePatchExpression`) and its tests (`json_patch_test.go`
//! `TestJSONPatch`).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusternetes_common::cel::CELEvaluator;
use serde_json::Value;

use super::policy_mutating::{PatchError, PatchRequest, Patcher};

/// `NewJSONPatcher` over a compiled `JSONPatchCondition`.
pub struct JsonPatcher {
    expression: String,
    evaluator: Mutex<CELEvaluator>,
}

impl JsonPatcher {
    pub fn new(expression: &str) -> Arc<dyn Patcher> {
        Arc::new(Self {
            expression: expression.to_string(),
            evaluator: Mutex::new(CELEvaluator::new()),
        })
    }
}

#[async_trait]
impl Patcher for JsonPatcher {
    async fn patch(&self, _request: &PatchRequest<'_>) -> Result<Value, PatchError> {
        let _ = (&self.expression, &self.evaluator);
        Err(PatchError::Other("jsonPatch patcher not implemented".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::policy_mutating::VersionedAttributes;
    use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
    use serde_json::json;

    fn run(expression: &str, object: Value) -> Result<Value, String> {
        let gvr = GroupVersionResource {
            group: "apps".into(),
            version: "v1".into(),
            resource: "deployments".into(),
        };
        let versioned = VersionedAttributes {
            versioned_kind: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            versioned_object: Some(object),
            versioned_old_object: None,
            dirty: false,
        };
        let patcher = JsonPatcher::new(expression);
        let req = PatchRequest {
            matched_resource: &gvr,
            versioned_attributes: &versioned,
            param: None,
            namespace: None,
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(patcher.patch(&req)).map_err(|e| match e {
            PatchError::Status(e) => e.to_string(),
            PatchError::Other(m) => m,
        })
    }

    fn ok(expression: &str, object: Value, expected: Value) {
        assert_eq!(run(expression, object).unwrap(), expected, "{expression}");
    }

    fn containers(names: &[&str]) -> Value {
        let c: Vec<Value> = names.iter().map(|n| json!({"name": n})).collect();
        json!({"spec": {"template": {"spec": {"containers": c}}}})
    }

    #[test]
    fn false_test_operation_leaves_object_unchanged() {
        let o = json!({"spec": {"replicas": 1}});
        ok(
            r#"[JSONPatch{op: "test", path: "/spec/replicas", value: 100},
                JSONPatch{op: "replace", path: "/spec/replicas", value: 3}]"#,
            o.clone(),
            o,
        );
    }

    #[test]
    fn true_test_operation_applies() {
        ok(
            r#"[JSONPatch{op: "test", path: "/spec/replicas", value: 1},
                JSONPatch{op: "replace", path: "/spec/replicas", value: 3}]"#,
            json!({"spec": {"replicas": 1}}),
            json!({"spec": {"replicas": 3}}),
        );
    }

    #[test]
    fn remove_ops() {
        ok(
            r#"[JSONPatch{op: "remove", path: "/spec/replicas"}]"#,
            json!({"spec": {"replicas": 1}}),
            json!({"spec": {}}),
        );
        ok(
            r#"[JSONPatch{op: "remove", path: "/metadata/labels/y"}]"#,
            json!({"metadata": {"labels": {"x": "1", "y": "1"}}}),
            json!({"metadata": {"labels": {"x": "1"}}}),
        );
        ok(
            r#"[JSONPatch{op: "remove", path: "/spec/template/spec/containers/1"}]"#,
            containers(&["a", "b", "c"]),
            containers(&["a", "c"]),
        );
    }

    #[test]
    fn copy_and_move_ops() {
        ok(
            r#"[JSONPatch{op: "copy", from: "/metadata/labels/x", path: "/metadata/labels/y"}]"#,
            json!({"metadata": {"labels": {"x": "1"}}}),
            json!({"metadata": {"labels": {"x": "1", "y": "1"}}}),
        );
        ok(
            r#"[JSONPatch{op: "copy", from: "/spec/template/spec/containers/0", path: "/spec/template/spec/containers/-"}]"#,
            containers(&["a", "b", "c"]),
            containers(&["a", "b", "c", "a"]),
        );
        ok(
            r#"[JSONPatch{op: "move", from: "/metadata/labels/x", path: "/metadata/labels/y"}]"#,
            json!({"metadata": {"labels": {"x": "1"}}}),
            json!({"metadata": {"labels": {"y": "1"}}}),
        );
        ok(
            r#"[JSONPatch{op: "move", from: "/spec/template/spec/containers/0", path: "/spec/template/spec/containers/-"}]"#,
            containers(&["a", "b", "c"]),
            containers(&["b", "c", "a"]),
        );
    }

    #[test]
    fn add_ops() {
        ok(
            r#"[JSONPatch{op: "add", path: "/metadata/labels/x", value: "2"}]"#,
            json!({"metadata": {"labels": {"y": "1"}}}),
            json!({"metadata": {"labels": {"y": "1", "x": "2"}}}),
        );
        ok(
            r#"[JSONPatch{op: "add", path: "/metadata/labels", value: {"y": "2"}}]"#,
            json!({"spec": {}}),
            json!({"spec": {}, "metadata": {"labels": {"y": "2"}}}),
        );
        // performs a replacement
        ok(
            r#"[JSONPatch{op: "add", path: "/metadata/labels", value: {"y": "2"}}]"#,
            json!({"metadata": {"labels": {"x": "1"}}}),
            json!({"metadata": {"labels": {"y": "2"}}}),
        );
        ok(
            r#"[JSONPatch{op: "add", path: "/spec/template/spec/containers/0", value: {"name": "x"}}]"#,
            containers(&["a"]),
            containers(&["x", "a"]),
        );
        ok(
            r#"[JSONPatch{op: "add", path: "/spec/template/spec/containers/-", value: {"name": "x"}}]"#,
            containers(&["a"]),
            containers(&["a", "x"]),
        );
    }

    #[test]
    fn replace_ops() {
        // an unset map key is added
        ok(
            r#"[JSONPatch{op: "replace", path: "/metadata/labels/x", value: "2"}]"#,
            json!({"metadata": {"labels": {"y": "1"}}}),
            json!({"metadata": {"labels": {"y": "1", "x": "2"}}}),
        );
        ok(
            r#"[JSONPatch{op: "replace", path: "/metadata/labels", value: {"y": "2"}}]"#,
            json!({"spec": {}}),
            json!({"spec": {}, "metadata": {"labels": {"y": "2"}}}),
        );
        ok(
            r#"[JSONPatch{op: "replace", path: "/metadata/labels", value: {"y": "2"}}]"#,
            json!({"metadata": {"labels": {"x": "1"}}}),
            json!({"metadata": {"labels": {"y": "2"}}}),
        );
        ok(
            r#"[JSONPatch{op: "replace", path: "/spec/template/spec/containers/0", value: {"name": "x"}}]"#,
            containers(&["a"]),
            containers(&["x"]),
        );
    }

    #[test]
    fn replace_end_of_list_with_dash_is_not_allowed() {
        let err = run(
            r#"[JSONPatch{op: "replace", path: "/spec/template/spec/containers/-", value: {"name": "x"}}]"#,
            containers(&["a"]),
        )
        .unwrap_err();
        assert!(
            err.contains("JSON Patch: replace operation does not apply: doc is missing key: /spec/template/spec/containers/-: missing value"),
            "{err}"
        );
    }

    #[test]
    fn patch_type_field_access_testing_and_equality() {
        ok(
            r#"[JSONPatch{op: "add", path: "/metadata/labels", value: {
                  "op": JSONPatch{op: "opValue"}.op,
                  "path": JSONPatch{path: "pathValue"}.path,
                  "from": JSONPatch{from: "fromValue"}.from,
                  "value": string(JSONPatch{value: "valueValue"}.value)}}]"#,
            json!({"metadata": {"labels": {}}}),
            json!({"metadata": {"labels": {
                "op": "opValue", "path": "pathValue", "from": "fromValue", "value": "valueValue"}}}),
        );
        ok(
            r#"[JSONPatch{op: "add", path: "/metadata/labels", value: {
                  "op": string(has(JSONPatch{op: "opValue"}.op)),
                  "op-unset": string(has(JSONPatch{}.op)),
                  "value": string(has(JSONPatch{value: "valueValue"}.value)),
                  "value-unset": string(has(JSONPatch{}.value))}}]"#,
            json!({"metadata": {"labels": {}}}),
            json!({"metadata": {"labels": {
                "op": "true", "op-unset": "false", "value": "true", "value-unset": "false"}}}),
        );
        ok(
            r#"[JSONPatch{op: "add", path: "/metadata/labels", value: {
                  "empty": string(JSONPatch{} == JSONPatch{}),
                  "same-all": string(JSONPatch{op: "add", path: "path", from: "from", value: 1} == JSONPatch{op: "add", path: "path", from: "from", value: 1}),
                  "different-op": string(JSONPatch{op: "add"} == JSONPatch{op: "remove"}),
                  "different-value": string(JSONPatch{op: "add", path: "path", from: "from", value: "1"} == JSONPatch{op: "add", path: "path", from: "from", value: 1})}}]"#,
            json!({"metadata": {"labels": {}}}),
            json!({"metadata": {"labels": {
                "empty": "true", "same-all": "true", "different-op": "false", "different-value": "false"}}}),
        );
    }

    #[test]
    fn cel_object_initializers() {
        ok(
            r#"[JSONPatch{op: "add", path: "/spec", value: Object.spec{selector: Object.spec.selector{}, replicas: 10}}]"#,
            json!({"spec": {}}),
            json!({"spec": {"selector": {}, "replicas": 10}}),
        );
        ok(
            r#"[JSONPatch{op: "add", path: "/spec/template/spec/containers/-",
                  value: Object.spec.template.spec.containers{
                    name: "x",
                    ports: [Object.spec.template.spec.containers.ports{containerPort: 8080}]}}]"#,
            containers(&["a"]),
            json!({"spec": {"template": {"spec": {"containers": [
                {"name": "a"}, {"name": "x", "ports": [{"containerPort": 8080}]}]}}}}),
        );
    }

    #[test]
    fn invalid_cel_initializer_type_name_is_rejected() {
        let err = run(
            r#"[JSONPatch{op: "add", path: "/spec/template/spec/containers/-",
                  value: Object.spec.template.spec.containers{
                    name: "x",
                    ports: [Object.spec.template.spec.containers.portsZ{containerPort: 8080}]}}]"#,
            containers(&["a"]),
        )
        .unwrap_err();
        assert!(
            err.contains("mismatch: unexpected type name \"Object.spec.template.spec.containers.portsZ\", expected \"Object.spec.template.spec.containers.ports\""),
            "{err}"
        );
    }

    #[test]
    fn compile_error_is_reported() {
        let err = run("[JSONPatch{op: ", json!({})).unwrap_err();
        assert!(!err.is_empty());
    }

    #[test]
    fn original_object_is_not_modified() {
        // patch.Patch: "The original object in the request MUST NOT be modified in-place."
        let o = json!({"spec": {"replicas": 1}});
        let _ = run(
            r#"[JSONPatch{op: "replace", path: "/spec/replicas", value: 3}]"#,
            o.clone(),
        );
        assert_eq!(o, json!({"spec": {"replicas": 1}}));
    }
}
