//! The MutatingAdmissionPolicy `jsonPatch` patcher.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/
//! mutating/patch/json_patch.go` (`NewJSONPatcher`, `jsonPatcher.Patch`,
//! `evaluatePatchExpression`) and its tests (`json_patch_test.go`
//! `TestJSONPatch`).

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusternetes_common::cel::{CELContext, CELEvaluator};
use rusternetes_common::cel_struct;
use serde_json::{json, Value};

use super::policy_json_patch_ops::{apply, ApplyError, Operation};
use super::policy_mutating::{PatchError, PatchRequest, Patcher};

/// `NewJSONPatcher` (json_patch.go:63-66) over a `JSONPatchCondition`
/// (json_patch.go:45-59: expression returning `list(JSONPatch)`).
pub struct JsonPatcher {
    expression: String,
    /// `PatchEvaluator.CompilationErrors()`, found when the patcher is built.
    compile_error: Option<String>,
    evaluator: Mutex<CELEvaluator>,
}

impl JsonPatcher {
    pub fn new_patcher(expression: &str) -> Arc<dyn Patcher> {
        let compile_error = cel_struct::compile(expression).err();
        Arc::new(Self {
            expression: expression.to_string(),
            compile_error,
            evaluator: Mutex::new(CELEvaluator::new()),
        })
    }

    /// `evaluatePatchExpression` (json_patch.go:118-192): evaluate the
    /// expression and turn each `JSONPatch` element into an `Operation`.
    fn evaluate_patch_expression(
        &self,
        r: &PatchRequest<'_>,
    ) -> Result<Vec<Operation>, PatchError> {
        let json = evaluate_expression(&self.evaluator, &self.expression, r)?;
        Self::operations_from(json)
    }

    fn operations_from(json: Value) -> Result<Vec<Operation>, PatchError> {
        let Value::Array(items) = json else {
            return Err(PatchError::Other(
                "type mismatch: JSONPatchType.expression should evaluate to array".into(),
            ));
        };
        items
            .into_iter()
            .map(|item| {
                let Value::Object(m) = item else {
                    return Err(PatchError::Other(
                        "type mismatch: JSONPatchType.expression should evaluate to array of JSONPatch"
                            .into(),
                    ));
                };
                // resultOp["op"/"path"] are always set (the Go zero value is
                // ""); "from" only when non-empty; "value" only when set.
                let s = |k: &str| m.get(k).and_then(Value::as_str).unwrap_or("").to_string();
                let from = s("from");
                Ok(Operation {
                    op: s("op"),
                    path: Some(s("path")),
                    from: (!from.is_empty()).then_some(from),
                    value: m.get("value").cloned(),
                })
            })
            .collect()
    }
}

/// The shared half of `jsonPatcher.evaluatePatchExpression` and
/// `applyConfigPatcher.Patch`: bind `object`/`oldObject`/`params`/
/// `namespaceObject`/`request`, evaluate, and marshal the result to JSON.
pub(super) fn evaluate_expression(
    evaluator: &Mutex<CELEvaluator>,
    expression: &str,
    r: &PatchRequest<'_>,
) -> Result<Value, PatchError> {
    {
        let mut ctx = CELContext::new();
        let attrs = r.versioned_attributes;
        // `CreateAdmissionRequest` (cel/compile.go) needs the request's
        // attributes, which `PatchRequest` does not carry yet: only the kind
        // and resource are known here.
        let request = json!({
            "kind": {
                "group": attrs.versioned_kind.group,
                "version": attrs.versioned_kind.version,
                "kind": attrs.versioned_kind.kind,
            },
            "resource": {
                "group": r.matched_resource.group,
                "version": r.matched_resource.version,
                "resource": r.matched_resource.resource,
            },
        });
        let vars: [(&str, &Value); 5] = [
            (
                "object",
                attrs.versioned_object.as_ref().unwrap_or(&Value::Null),
            ),
            (
                "oldObject",
                attrs.versioned_old_object.as_ref().unwrap_or(&Value::Null),
            ),
            ("params", r.param.unwrap_or(&Value::Null)),
            ("namespaceObject", r.namespace.unwrap_or(&Value::Null)),
            ("request", &request),
        ];
        for (name, v) in vars {
            ctx.add_json_variable(name, v)
                .map_err(|e| PatchError::Other(e.to_string()))?;
        }

        let value = evaluator
            .lock()
            .unwrap()
            .evaluate_to_value(expression, &ctx)
            .map_err(|e| PatchError::Other(e.to_string()))?;
        value.json().map_err(|e| {
            PatchError::Other(format!(
                "JSONPath valueExpression evaluated to a type that could not marshal to JSON: {e}"
            ))
        })
    }
}

#[async_trait]
impl Patcher for JsonPatcher {
    /// `jsonPatcher.Patch` (json_patch.go:72-116), over the unstructured
    /// representation (the `*unstructured.Unstructured` branch at :102-103).
    async fn patch(&self, request: &PatchRequest<'_>) -> Result<Value, PatchError> {
        if let Some(e) = &self.compile_error {
            // cel_struct checks `Object.<path>{}` type names statically
            // (CheckTypeNamesMatchFieldPathNames), which upstream does at
            // runtime and wraps as "type mismatch: %w" (json_patch.go:170-173).
            return Err(PatchError::Other(
                if e.starts_with("unexpected type name") {
                    format!("type mismatch: {e}")
                } else {
                    e.clone()
                },
            ));
        }
        let ops = self.evaluate_patch_expression(request)?;
        let object = request
            .versioned_attributes
            .versioned_object
            .clone()
            .unwrap_or(Value::Null);
        match apply(&object, &ops) {
            Ok(patched) => Ok(patched),
            // If a json patch fails a test operation, the patch must not be
            // applied (json_patch.go:94-97).
            Err(ApplyError::TestFailed) => Ok(object),
            Err(e) => Err(PatchError::Other(format!("JSON Patch: {e}"))),
        }
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
        let patcher = JsonPatcher::new_patcher(expression);
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
            json!({"metadata": {}, "spec": {}}),
            json!({"metadata": {"labels": {"y": "2"}}, "spec": {}}),
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
            json!({"metadata": {}, "spec": {}}),
            json!({"metadata": {"labels": {"y": "2"}}, "spec": {}}),
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
                  "op": has(JSONPatch{op: "opValue"}.op) ? "true" : "false",
                  "op-unset": has(JSONPatch{}.op) ? "true" : "false",
                  "value": has(JSONPatch{value: "valueValue"}.value) ? "true" : "false",
                  "value-unset": has(JSONPatch{}.value) ? "true" : "false"}}]"#,
            json!({"metadata": {"labels": {}}}),
            json!({"metadata": {"labels": {
                "op": "true", "op-unset": "false", "value": "true", "value-unset": "false"}}}),
        );
        ok(
            r#"[JSONPatch{op: "add", path: "/metadata/labels", value: {
                  "empty": (JSONPatch{} == JSONPatch{}) ? "true" : "false",
                  "same-all": (JSONPatch{op: "add", path: "path", from: "from", value: 1} == JSONPatch{op: "add", path: "path", from: "from", value: 1}) ? "true" : "false",
                  "different-op": (JSONPatch{op: "add"} == JSONPatch{op: "remove"}) ? "true" : "false",
                  "different-value": (JSONPatch{op: "add", path: "path", from: "from", value: "1"} == JSONPatch{op: "add", path: "path", from: "from", value: 1}) ? "true" : "false"}}]"#,
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
