//! The MutatingAdmissionPolicy `applyConfiguration` patcher.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/
//! mutating/patch/smd.go` (`NewApplyConfigurationPatcher`,
//! `applyConfigPatcher.Patch`, `ApplyStructuredMergeDiff`, `validatePatch`,
//! `findAtomics`) and its tests (`smd_test.go` `TestApplyConfiguration`).

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
