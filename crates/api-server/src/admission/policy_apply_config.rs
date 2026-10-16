//! The MutatingAdmissionPolicy `applyConfiguration` patcher.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/
//! mutating/patch/smd.go` (`NewApplyConfigurationPatcher`,
//! `applyConfigPatcher.Patch`, `ApplyStructuredMergeDiff`, `validatePatch`,
//! `findAtomics`) and its tests (`smd_test.go` `TestApplyConfiguration`).

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusternetes_common::cel::CELEvaluator;
use rusternetes_common::cel_struct::{self, TypeCheck};
use serde_json::{Map, Value};

use super::policy_json_patch::evaluate_expression_budgeted;
use super::policy_mutating::{PatchError, PatchRequest, Patcher};
use crate::fieldmanager::apply::merge_values;
use crate::fieldmanager::fieldset::{field_kind, FieldKind};

/// `NewApplyConfigurationPatcher` (smd.go:59-61).
pub struct ApplyConfigPatcher {
    expression: String,
    /// `expressionEvaluator.CompilationErrors()`, found when built.
    compile_error: Option<String>,
    /// `spec.variables`, `(name, expression)` in declaration order.
    variables: Vec<(String, String)>,
    evaluator: Mutex<CELEvaluator>,
}

impl ApplyConfigPatcher {
    pub fn new_patcher(expression: &str) -> Arc<dyn Patcher> {
        Self::new_patcher_with_variables(expression, &[])
    }

    /// With the policy's composited `variables` (cel/composition.go).
    pub fn new_patcher_with_variables(
        expression: &str,
        variables: &[(String, String)],
    ) -> Arc<dyn Patcher> {
        Arc::new(Self {
            expression: expression.to_string(),
            compile_error: cel_struct::compile(expression).err(),
            variables: variables.to_vec(),
            evaluator: Mutex::new(CELEvaluator::new()),
        })
    }
}

#[async_trait]
impl Patcher for ApplyConfigPatcher {
    /// `applyConfigPatcher.Patch` (smd.go:67-116).
    async fn patch(
        &self,
        r: &PatchRequest<'_>,
        runtime_cel_cost_budget: i64,
    ) -> Result<Value, PatchError> {
        if let Some(e) = &self.compile_error {
            // `objVal.CheckTypeNamesMatchFieldPathNames` (smd.go:99-102);
            // cel_struct checks the names statically.
            return Err(PatchError::Other(
                if e.starts_with("unexpected type name") {
                    format!("type mismatch: {e}")
                } else {
                    e.clone()
                },
            ));
        }
        // smd.go:77 `ForInput(..., runtimeCELCostBudget)`; the remaining budget
        // is not read again (`eval, _, err :=`).
        let (value, _) = evaluate_expression_budgeted(
            &self.evaluator,
            &self.expression,
            &self.variables,
            r,
            TypeCheck::Root,
            runtime_cel_cost_budget,
        )?;
        // smd.go:87-90, :104-107: the compiler ensures an `Object`.
        let Value::Object(mut patch) = value else {
            return Err(PatchError::Other(format!(
                "unsupported return type from ApplyConfiguration expression: {value}"
            )));
        };
        // `patchObject.SetGroupVersionKind(...VersionedObject GVK)` (:110).
        let kind = &r.versioned_attributes.versioned_kind;
        let api_version = if kind.group.is_empty() {
            kind.version.clone()
        } else {
            format!("{}/{}", kind.group, kind.version)
        };
        patch.insert("apiVersion".into(), Value::String(api_version));
        patch.insert("kind".into(), Value::String(kind.kind.clone()));
        let original = r
            .versioned_attributes
            .versioned_object
            .clone()
            .unwrap_or_else(|| Value::Object(Map::new()));
        apply_structured_merge_diff(&original, &Value::Object(patch))
            .map_err(|e| PatchError::Other(format!("error applying patch: {e}")))
    }
}

/// `ApplyStructuredMergeDiff` (smd.go:120-166) over the unstructured form:
/// validate the patch, merge it into the original and return the copy. No
/// ownership is tracked (smd.go:151-158).
///
/// DEVIATION: upstream's typed conversion and merge are driven by the OpenAPI
/// schema (`typeConverter.ObjectToTyped`, `liveObjTyped.Merge`), including the
/// `failed to convert patch object to typed object: .spec.replicasx: field not
/// declared in schema` check. This tree has no per-type schema; like
/// `fieldmanager::fieldset` the shapes come from the field-name table in
/// `field_kind`; undeclared fields are rejected only for built-in kinds with a
/// Rust type (`undeclared_field`; rest tracked in #3129).
pub fn apply_structured_merge_diff(original: &Value, patch: &Value) -> Result<Value, String> {
    let merged = merge_values(original, patch, "");
    // `typeConverter.ObjectToTyped(patch)` (smd.go:128-131) precedes
    // `validatePatch`.
    if let Some(path) = undeclared_field(original, &merged, patch) {
        return Err(format!(
            "failed to convert patch object to typed object: .{path}: field not declared in schema"
        ));
    }
    validate_patch(patch).map_err(|e| format!("invalid ApplyConfiguration: {e}"))?;
    Ok(merged)
}

/// First field the patch introduces that the object's type does not declare:
/// typed conversion's `field not declared in schema` (structured-merge-diff
/// `typed/helpers.go` `ValidationError`). Only built-in kinds with a Rust type
/// are checked; the type's declared fields stand in for the OpenAPI schema
/// (rest tracked in #3129). The patch alone cannot be deserialized (required
/// fields), so the merged object is checked and fields already undeclared in
/// the original are ignored.
fn undeclared_field(original: &Value, merged: &Value, patch: &Value) -> Option<String> {
    let unknown = |v: &Value| -> Option<Vec<String>> {
        use crate::handlers::validation::find_unknown_fields_via_schema as f;
        use rusternetes_common::resources as r;
        let kind = patch.get("kind")?.as_str()?;
        let api = patch.get("apiVersion")?.as_str()?;
        Some(match (api, kind) {
            ("apps/v1", "Deployment") => f::<r::Deployment>(v),
            ("apps/v1", "ReplicaSet") => f::<r::ReplicaSet>(v),
            ("apps/v1", "StatefulSet") => f::<r::StatefulSet>(v),
            ("apps/v1", "DaemonSet") => f::<r::DaemonSet>(v),
            ("batch/v1", "Job") => f::<r::Job>(v),
            ("batch/v1", "CronJob") => f::<r::CronJob>(v),
            ("v1", "Pod") => f::<r::Pod>(v),
            ("v1", "Service") => f::<r::Service>(v),
            ("v1", "ConfigMap") => f::<r::ConfigMap>(v),
            _ => return None,
        })
    };
    let before = unknown(original)?;
    unknown(merged)?
        .into_iter()
        .find(|p| !p.is_empty() && !before.contains(p))
}

/// `validatePatch` (smd.go:173-179).
fn validate_patch(v: &Value) -> Result<(), String> {
    let mut atomics = Vec::new();
    find_atomics(&mut String::new(), "", v, &mut atomics);
    if atomics.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "may not mutate atomic arrays, maps or structs: {}",
            atomics.join(", ")
        ))
    }
}

/// `findAtomics` (smd.go:182-209): field paths of every atomic map found in
/// the value. `name` is the field the value sits under (the schema lookup).
fn find_atomics(path: &mut String, name: &str, v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Object(m) => {
            if field_kind(name) == FieldKind::Atomic {
                out.push(path.clone());
            }
            for (k, child) in m {
                let len = path.len();
                path.push('.');
                path.push_str(k);
                find_atomics(path, k, child, out);
                path.truncate(len);
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                let len = path.len();
                path.push_str(&format!("[{i}]"));
                // List elements resolve through the list's element type, not
                // a field of their own.
                find_atomics(path, "", item, out);
                path.truncate(len);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::policy_matching::Attributes;
    use crate::admission::policy_mutating::VersionedAttributes;
    use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
    use rusternetes_common::cel_cost::RUNTIME_CEL_COST_BUDGET;
    use serde_json::json;

    /// `TestApplyConfiguration` harness: a Deployment `Create` with the
    /// given object / old object.
    fn run(expression: &str, object: Value, old: Option<Value>) -> Result<Value, String> {
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
            versioned_old_object: old,
            dirty: false,
        };
        run_with(ApplyConfigPatcher::new_patcher(expression), versioned, gvr)
    }

    fn run_with(
        patcher: Arc<dyn Patcher>,
        versioned: VersionedAttributes,
        gvr: GroupVersionResource,
    ) -> Result<Value, String> {
        let attributes = Attributes {
            kind: versioned.versioned_kind.clone(),
            resource: gvr.clone(),
            subresource: "".into(),
            namespace: "default".into(),
            name: "d1".into(),
            operation: Operation::Update,
            object: versioned.versioned_object.clone(),
            old_object: versioned.versioned_old_object.clone(),
            user_info: None,
            dry_run: false,
            operation_options: None,
        };
        let req = PatchRequest {
            matched_resource: &gvr,
            versioned_attributes: &versioned,
            attributes: &attributes,
            param: None,
            namespace: None,
            authorizer: None,
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(patcher.patch(&req, RUNTIME_CEL_COST_BUDGET))
            .map_err(|e| match e {
                PatchError::Status(e) => e.to_string(),
                PatchError::Other(m) => m,
            })
    }

    /// The expected result carries the patch's TypeMeta (smd.go:110).
    fn typed(mut v: Value) -> Value {
        v["apiVersion"] = json!("apps/v1");
        v["kind"] = json!("Deployment");
        v
    }

    fn volumes(names: &[&str]) -> Value {
        let v: Vec<Value> = names.iter().map(|n| json!({"name": n})).collect();
        typed(json!({"spec": {"template": {"spec": {"volumes": v}}}}))
    }

    #[test]
    fn add_to_list_type_map() {
        let got = run(
            r#"Object{spec: Object.spec{template: Object.spec.template{
                spec: Object.spec.template.spec{
                  volumes: [Object.spec.template.spec.volumes{name: "y"}]}}}}"#,
            json!({"spec": {"template": {"spec": {"volumes": [{"name": "x"}]}}}}),
            None,
        )
        .unwrap();
        assert_eq!(got, volumes(&["x", "y"]));
    }

    #[test]
    fn update_list_type_map_entry() {
        let got = run(
            r#"Object{spec: Object.spec{template: Object.spec.template{
                spec: Object.spec.template.spec{
                  volumes: [Object.spec.template.spec.volumes{name: "y",
                    hostPath: Object.spec.template.spec.volumes.hostPath{path: "a"}}]}}}}"#,
            json!({"spec": {"template": {"spec": {"volumes": [{"name": "x"}, {"name": "y"}]}}}}),
            None,
        )
        .unwrap();
        assert_eq!(
            got,
            typed(json!({"spec": {"template": {"spec": {"volumes": [
                {"name": "x"}, {"name": "y", "hostPath": {"path": "a"}}]}}}}))
        );
    }

    #[test]
    fn conditionals_and_old_object() {
        let got = run(
            "Object{spec: Object.spec{replicas: object.spec.replicas % 2 == 0 ? object.spec.replicas + 1 : object.spec.replicas}}",
            json!({"spec": {"replicas": 2}}),
            None,
        )
        .unwrap();
        assert_eq!(got["spec"]["replicas"], json!(3));
        let got = run(
            "Object{spec: Object.spec{replicas: oldObject.spec.replicas % 2 == 0 ? oldObject.spec.replicas + 1 : oldObject.spec.replicas}}",
            json!({"spec": {"replicas": 1}}),
            Some(json!({"spec": {"replicas": 2}})),
        )
        .unwrap();
        assert_eq!(got["spec"]["replicas"], json!(3));
    }

    #[test]
    fn complex_initialization() {
        let got = run(
            r#"Object{spec: Object.spec{replicas: 1,
                template: Object.spec.template{
                  metadata: Object.spec.template.metadata{labels: {"app": "nginx"}},
                  spec: Object.spec.template.spec{containers: [Object.spec.template.spec.containers{
                    name: "nginx", image: "nginx:1.14.2",
                    ports: [Object.spec.template.spec.containers.ports{containerPort: 80}],
                    resources: Object.spec.template.spec.containers.resources{
                      limits: {"cpu": "128M"}}}]}}}}"#,
            json!({"spec": {}}),
            None,
        )
        .unwrap();
        assert_eq!(
            got,
            typed(json!({"spec": {"replicas": 1, "template": {
                "metadata": {"labels": {"app": "nginx"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx:1.14.2",
                    "ports": [{"containerPort": 80}],
                    "resources": {"limits": {"cpu": "128M"}}}]}}}}))
        );
    }

    #[test]
    fn change_to_atomic_is_rejected() {
        let err = run(
            r#"Object{spec: Object.spec{selector: Object.spec.selector{matchLabels: {"l": "v"}}}}"#,
            json!({"spec": {"replicas": 1}}),
            None,
        )
        .unwrap_err();
        assert!(
            err.contains("error applying patch: invalid ApplyConfiguration: may not mutate atomic arrays, maps or structs: .spec.selector"),
            "{err}"
        );
    }

    // smd_test.go "apply configuration with invalid field name": the typed
    // conversion of the patch rejects undeclared fields.
    #[test]
    fn invalid_field_name_is_rejected() {
        let err = run(
            "Object{spec: Object.spec{replicasx: 1}}",
            json!({"spec": {"replicas": 1,
                "selector": {"matchLabels": {"a": "b"}},
                "template": {"metadata": {"labels": {"a": "b"}},
                    "spec": {"containers": [{"name": "c", "image": "i"}]}}}}),
            None,
        )
        .unwrap_err();
        assert_eq!(
            err,
            "error applying patch: failed to convert patch object to typed object: .spec.replicasx: field not declared in schema"
        );
    }

    #[test]
    fn declared_fields_still_apply() {
        let out = run(
            "Object{spec: Object.spec{replicas: 3}}",
            json!({"spec": {"replicas": 1,
                "selector": {"matchLabels": {"a": "b"}},
                "template": {"metadata": {"labels": {"a": "b"}},
                    "spec": {"containers": [{"name": "c", "image": "i"}]}}}}),
            None,
        )
        .unwrap();
        assert_eq!(out["spec"]["replicas"], json!(3));
    }

    #[test]
    fn invalid_type_name_is_rejected() {
        let err = run(
            "Object{spec: Object.specx{replicas: 1}}",
            json!({"spec": {"replicas": 1}}),
            None,
        )
        .unwrap_err();
        assert!(
            err.contains("type mismatch: unexpected type name \"Object.specx\", expected \"Object.spec\", which matches field name path from root Object type"),
            "{err}"
        );
    }

    // `CheckTypeNamesMatchFieldPathNames` runs on the EVALUATED ObjectVal
    // (smd.go:99-102), so a struct reaching a field through a composited
    // variable is checked too (#2944).
    #[test]
    fn type_name_mismatch_through_a_variable_is_rejected() {
        let versioned = VersionedAttributes {
            versioned_kind: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            versioned_object: Some(json!({"spec": {"replicas": 1}})),
            versioned_old_object: None,
            dirty: false,
        };
        let gvr = GroupVersionResource {
            group: "apps".into(),
            version: "v1".into(),
            resource: "deployments".into(),
        };
        let p = ApplyConfigPatcher::new_patcher_with_variables(
            "Object{spec: variables.s}",
            &[("s".to_string(), "Object.wrong{replicas: 2}".to_string())],
        );
        let err = run_with(p, versioned, gvr).unwrap_err();
        assert!(
            err.contains("type mismatch: unexpected type name \"Object.wrong\", expected \"Object.spec\", which matches field name path from root Object type"),
            "{err}"
        );
    }

    #[test]
    fn non_object_result_is_rejected() {
        let err = run(r#""I'm a teapot!""#, json!({}), None).unwrap_err();
        assert!(err.contains("ApplyConfiguration"), "{err}");
    }

    fn env_object() -> Value {
        json!({"spec": {"replicas": 1, "template": {"spec": {"containers": [{
            "name": "nginx",
            "env": [{"name": "test", "value": "a"}, {"name": "test", "value": "b"},
                    {"name": "foo", "value": "bar"}]}]}}}})
    }

    #[test]
    fn duplicate_env_vars_untouched_by_patch_are_preserved() {
        let got = run("Object{spec: Object.spec{replicas: 3}}", env_object(), None).unwrap();
        assert_eq!(got["spec"]["replicas"], json!(3));
        assert_eq!(
            got["spec"]["template"]["spec"]["containers"][0]["env"],
            env_object()["spec"]["template"]["spec"]["containers"][0]["env"]
        );
    }

    #[test]
    fn modifying_a_duplicated_env_var_consolidates_it() {
        let got = run(
            r#"Object{spec: Object.spec{template: Object.spec.template{
                spec: Object.spec.template.spec{
                  containers: [Object.spec.template.spec.containers{name: "nginx",
                    env: [Object.spec.template.spec.containers.env{name: "test", value: "c"}]}]}}}}"#,
            env_object(),
            None,
        )
        .unwrap();
        assert_eq!(
            got["spec"]["template"]["spec"]["containers"][0]["env"],
            json!([{"name": "test", "value": "c"}, {"name": "foo", "value": "bar"}])
        );
    }

    // The request's attributes and composited variables reach the apply
    // configuration expression too (smd.go:67-116 evaluates through the same
    // `ForInput` as json_patch.go).
    #[test]
    fn request_and_variables_are_bound() {
        let versioned = VersionedAttributes {
            versioned_kind: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            versioned_object: Some(json!({"metadata": {"name": "d1"}})),
            versioned_old_object: None,
            dirty: false,
        };
        let gvr = GroupVersionResource {
            group: "apps".into(),
            version: "v1".into(),
            resource: "deployments".into(),
        };
        let p = ApplyConfigPatcher::new_patcher_with_variables(
            r#"Object{metadata: Object.metadata{
                 labels: {"op": request.operation, "ns": request.namespace, "v": variables.n}}}"#,
            &[("n".to_string(), "object.metadata.name + '-x'".to_string())],
        );
        let got = run_with(p, versioned, gvr).unwrap();
        assert_eq!(
            got["metadata"]["labels"],
            json!({"op": "UPDATE", "ns": "default", "v": "d1-x"})
        );
    }
}
