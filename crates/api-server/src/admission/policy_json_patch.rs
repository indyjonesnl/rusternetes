//! The MutatingAdmissionPolicy `jsonPatch` patcher.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/
//! mutating/patch/json_patch.go` (`NewJSONPatcher`, `jsonPatcher.Patch`,
//! `evaluatePatchExpression`) and its tests (`json_patch_test.go`
//! `TestJSONPatch`).

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusternetes_common::admission::GroupVersionKind;
use rusternetes_common::admission::Operation as AdmissionOperation;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::cel::{CELContext, CELEvaluator};
use rusternetes_common::cel_authz_library;
use rusternetes_common::cel_cost::CostOptions;
use rusternetes_common::cel_struct::{self, TypeCheck};
use serde_json::{json, Value};

use super::policy_json_patch_ops::{apply, ApplyError, Operation};
use super::policy_mutating::{PatchError, PatchRequest, Patcher};

/// `NewJSONPatcher` (json_patch.go:63-66) over a `JSONPatchCondition`
/// (json_patch.go:45-59: expression returning `list(JSONPatch)`).
pub struct JsonPatcher {
    expression: String,
    /// `PatchEvaluator.CompilationErrors()`, found when the patcher is built.
    compile_error: Option<String>,
    /// `spec.variables`, `(name, expression)` in declaration order
    /// (`CompositionEnv.CompiledVariables`, cel/composition.go).
    variables: Vec<(String, String)>,
    evaluator: Mutex<CELEvaluator>,
}

impl JsonPatcher {
    pub fn new_patcher(expression: &str) -> Arc<dyn Patcher> {
        Self::new_patcher_with_variables(expression, &[])
    }

    /// `CompositedCompiler.CompileMutatingEvaluator` (cel/composition.go):
    /// `variables` are the policy's `spec.variables`, `(name, expression)` in
    /// declaration order.
    pub fn new_patcher_with_variables(
        expression: &str,
        variables: &[(String, String)],
    ) -> Arc<dyn Patcher> {
        let compile_error = cel_struct::compile(expression).err();
        Arc::new(Self {
            expression: expression.to_string(),
            compile_error,
            variables: variables.to_vec(),
            evaluator: Mutex::new(CELEvaluator::new()),
        })
    }

    /// `evaluatePatchExpression` (json_patch.go:118-192): evaluate the
    /// expression and turn each `JSONPatch` element into an `Operation`.
    fn evaluate_patch_expression(
        &self,
        r: &PatchRequest<'_>,
    ) -> Result<Vec<Operation>, PatchError> {
        let json = evaluate_expression(
            &self.evaluator,
            &self.expression,
            &self.variables,
            r,
            TypeCheck::JsonPatchValues,
        )?;
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
    variables: &[(String, String)],
    r: &PatchRequest<'_>,
    check: TypeCheck,
) -> Result<Value, PatchError> {
    evaluate_expression_costed(evaluator, expression, variables, r, check, None)
}

/// What a costed evaluation spent (`EvalDetails.ActualCost()` and the
/// `compositionContext` cost, activation.go:132-189).
#[derive(Debug, Default)]
pub(super) struct CostReport {
    /// The expression's own actual cost.
    pub cost: u64,
    /// The composited variables this expression read (transitively), with the
    /// cost of evaluating each. Upstream evaluates a variable lazily, once,
    /// when it is first read (composition.go:191-225).
    pub variable_costs: Vec<(String, u64)>,
}

/// [`evaluate_expression`], reporting the runtime cost when `report` is set
/// (cel-go `cel.CostTracking`, #3323): the variables and the expression are
/// each evaluated under the per-call limit (`environment/base.go:83`).
pub(super) fn evaluate_expression_costed(
    evaluator: &Mutex<CELEvaluator>,
    expression: &str,
    variables: &[(String, String)],
    r: &PatchRequest<'_>,
    check: TypeCheck,
    report: Option<&mut CostReport>,
) -> Result<Value, PatchError> {
    {
        let mut variable_cost: std::collections::HashMap<&str, u64> =
            std::collections::HashMap::new();
        let mut ctx = CELContext::new();
        let attrs = r.versioned_attributes;
        let request = create_admission_request(r);
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
        // activation.go:52-55: `authorizer` and `authorizer.requestResource`
        // exist only when `inputs.Authorizer != nil`.
        if let Some(authz) = &r.authorizer {
            let user = r.attributes.user_info.clone().unwrap_or(UserInfo {
                username: String::new(),
                uid: String::new(),
                groups: Vec::new(),
                extra: Default::default(),
            });
            ctx.add_variable(
                cel_authz_library::AUTHORIZER_VAR.to_string(),
                cel_authz_library::new_authorizer_val(&user, authz.clone()),
            );
            ctx.add_variable(
                cel_authz_library::REQUEST_RESOURCE_AUTHORIZER_VAR.to_string(),
                cel_authz_library::new_resource_authorizer_val(
                    &user,
                    authz.clone(),
                    &r.attributes.resource.group,
                    &r.attributes.resource.resource,
                    &r.attributes.subresource,
                    &r.attributes.namespace,
                    &r.attributes.name,
                ),
            );
        }

        let mut evaluator = evaluator.lock().unwrap();
        // `compositionContext.Variables` binds `variables` as a lazy map: a
        // variable is evaluated when read and may read the others
        // (variableAccessor.Callback, composition.go). Here they are
        // evaluated up front in declaration order, so a variable can read the
        // ones declared before it, and a failure is only reported when the
        // expression actually reads that variable.
        // The values are bound with their `Object{}` type markers, so
        // `CheckTypeNamesMatchFieldPathNames` still sees a struct reaching a
        // field through a variable (as the lazily evaluated ObjectVal does).
        let mut bound: std::collections::HashMap<cel::objects::Key, cel::Value> =
            std::collections::HashMap::new();
        let bind =
            |ctx: &mut CELContext,
             bound: &std::collections::HashMap<cel::objects::Key, cel::Value>| {
                ctx.add_variable(
                    "variables".to_string(),
                    cel::Value::Map(cel::objects::Map {
                        map: Arc::new(bound.clone()),
                    }),
                );
            };
        let mut failed: Vec<(&str, String)> = Vec::new();
        bind(&mut ctx, &bound);
        for (name, expr) in variables {
            let evaluated = if report.is_some() {
                let (r, c) = evaluator.evaluate_with_cost_raw(expr, &ctx, CostOptions::default());
                variable_cost.insert(name.as_str(), c);
                r.map_err(|e| anyhow::anyhow!("Failed to execute CEL expression '{expr}': {e}"))
            } else {
                evaluator.evaluate_to_value_raw(expr, &ctx)
            };
            let result = evaluated.map_err(|e| e.to_string()).and_then(|v| {
                // Fails a value that cannot marshal to JSON.
                cel_struct::strip_type_markers(&v)
                    .json()
                    .map_err(|e| e.to_string())?;
                Ok(v)
            });
            match result {
                Ok(v) => {
                    bound.insert(cel::objects::Key::from(name.as_str()), v);
                    bind(&mut ctx, &bound);
                }
                Err(e) => failed.push((name, composited_failure(&failed, expr, name, e))),
            }
        }

        let evaluated = match report {
            None => evaluator.evaluate_to_value_checked(expression, &ctx, check),
            Some(rep) => {
                let (r, c) =
                    evaluator.evaluate_with_cost_raw(expression, &ctx, CostOptions::default());
                rep.cost = c;
                rep.variable_costs = read_variable_costs(expression, variables, &variable_cost);
                // the error wording of `evaluate_to_value_checked`
                r.map_err(|e| {
                    anyhow::anyhow!("Failed to execute CEL expression '{expression}': {e}")
                })
                .and_then(|v| {
                    cel_struct::finish_checked(v, check).map_err(|e| {
                        if e.starts_with("unexpected type name") {
                            anyhow::anyhow!("type mismatch: {e}")
                        } else {
                            anyhow::anyhow!("Failed to execute CEL expression '{expression}': {e}")
                        }
                    })
                })
            }
        };
        let value = evaluated.map_err(|e| {
            PatchError::Other(
                match failed.iter().find(|(n, _)| reads_variable(expression, n)) {
                    Some((_, msg)) => msg.clone(),
                    None => e.to_string(),
                },
            )
        })?;
        value.json().map_err(|e| {
            PatchError::Other(format!(
                "JSONPath valueExpression evaluated to a type that could not marshal to JSON: {e}"
            ))
        })
    }
}

/// The variables `expression` reads, transitively (a variable may read the
/// ones declared before it), with their evaluation costs: the lazily
/// evaluated composited variables of composition.go:191-225.
fn read_variable_costs(
    expression: &str,
    variables: &[(String, String)],
    costs: &std::collections::HashMap<&str, u64>,
) -> Vec<(String, u64)> {
    let mut read: Vec<&str> = Vec::new();
    let mut work: Vec<&str> = vec![expression];
    while let Some(text) = work.pop() {
        for (name, expr) in variables {
            if reads_variable(text, name) && !read.contains(&name.as_str()) {
                read.push(name);
                work.push(expr);
            }
        }
    }
    variables
        .iter()
        .filter(|(n, _)| read.contains(&n.as_str()))
        .map(|(n, _)| (n.clone(), costs.get(n.as_str()).copied().unwrap_or(0)))
        .collect()
}

/// Whether `expression` reads `variables.<name>` / `variables["name"]`.
fn reads_variable(expression: &str, name: &str) -> bool {
    expression.contains(&format!("variables.{name}"))
        || expression.contains(&format!("variables[\"{name}\"]"))
        || expression.contains(&format!("variables['{name}']"))
}

/// `types.NewErr("composited variable %q fails to evaluate: %v", ...)`
/// (composition.go `variableAccessor.Callback`). When the variable failed
/// because it read another failed variable, that variable's failure is
/// reported instead, as the lazy map would surface it.
fn composited_failure(failed: &[(&str, String)], expr: &str, name: &str, err: String) -> String {
    if let Some((_, msg)) = failed.iter().find(|(n, _)| reads_variable(expr, n)) {
        return msg.clone();
    }
    format!("composited variable {name:?} fails to evaluate: {err}")
}

/// `CreateAdmissionRequest` (admission/plugin/cel/condition.go:115-175):
/// `Object`/`OldObject` are left unset, the kind/resource are the matched
/// equivalents and `requestKind`/`requestResource` the request's own.
/// `userInfo` is `authenticationv1.UserInfo` (every field `omitempty`, so a
/// nil user is `{}`), `dryRun` is `attr.IsDryRun()` and `options` is
/// `attr.GetOperationOptions()` (omitted when there are none). Unlike the
/// unstructured conversion, empty `subResource`/`name`/`namespace` are kept
/// as `""`.
fn create_admission_request(r: &PatchRequest<'_>) -> Value {
    let attr = r.attributes;
    let k = &r.versioned_attributes.versioned_kind;
    let res = r.matched_resource;
    let mut user_info = serde_json::Map::new();
    if let Some(u) = &attr.user_info {
        if !u.username.is_empty() {
            user_info.insert("username".into(), json!(u.username));
        }
        if !u.uid.is_empty() {
            user_info.insert("uid".into(), json!(u.uid));
        }
        if !u.groups.is_empty() {
            user_info.insert("groups".into(), json!(u.groups));
        }
        if !u.extra.is_empty() {
            user_info.insert("extra".into(), json!(u.extra));
        }
    }
    let mut request = json!({
        "kind": {"group": k.group, "version": k.version, "kind": k.kind},
        "resource": {"group": res.group, "version": res.version, "resource": res.resource},
        "subResource": attr.subresource,
        "requestKind": {
            "group": attr.kind.group, "version": attr.kind.version, "kind": attr.kind.kind,
        },
        "requestResource": {
            "group": attr.resource.group,
            "version": attr.resource.version,
            "resource": attr.resource.resource,
        },
        "requestSubResource": attr.subresource,
        "name": attr.name,
        "namespace": attr.namespace,
        "operation": match attr.operation {
            AdmissionOperation::Create => "CREATE",
            AdmissionOperation::Update => "UPDATE",
            AdmissionOperation::Delete => "DELETE",
            AdmissionOperation::Connect => "CONNECT",
        },
        "userInfo": user_info,
        "dryRun": attr.dry_run,
    });
    if let Some(options) = &attr.operation_options {
        request["options"] = options.clone();
    }
    request
}

/// Unknown field paths of `object` when decoded as the typed `kind`; `None`
/// for a kind with no typed model here (the `*unstructured.Unstructured`
/// branch, json_patch.go:102-103, which has nothing to decode strictly).
fn unknown_fields_of(kind: &GroupVersionKind, object: &Value) -> Option<Vec<String>> {
    use crate::handlers::validation::find_unknown_fields_via_schema as f;
    use rusternetes_common::resources as r;
    Some(match (kind.group.as_str(), kind.kind.as_str()) {
        ("", "Pod") => f::<r::Pod>(object),
        ("", "ReplicationController") => f::<r::ReplicationController>(object),
        ("", "Service") => f::<r::Service>(object),
        ("", "LimitRange") => f::<r::LimitRange>(object),
        ("", "PersistentVolumeClaim") => f::<r::volume::PersistentVolumeClaim>(object),
        ("apps", "Deployment") => f::<r::Deployment>(object),
        ("apps", "DaemonSet") => f::<r::DaemonSet>(object),
        ("apps", "StatefulSet") => f::<r::StatefulSet>(object),
        ("apps", "ReplicaSet") => f::<r::ReplicaSet>(object),
        ("batch", "Job") => f::<r::Job>(object),
        ("batch", "CronJob") => f::<r::CronJob>(object),
        _ => return None,
    })
}

/// The strict typed decode of the patched object (json_patch.go:87 encodes
/// with `Strict: true`, :111 `jsonSerializer.Decode(patchedJS, ...)` and wraps
/// a failure in `apierrors.NewInternalError`), e.g.
/// `strict decoding error: unknown field "spec.bogus"`.
///
/// Deliberate deviation: upstream's input is the typed object re-encoded, so
/// it never carries a field the type lacks. Ours is the stored JSON, which may
/// carry fields our structs do not model, so only unknown fields the patch
/// itself introduced are reported.
fn strict_decode_check(
    kind: &GroupVersionKind,
    before: &Value,
    patched: &Value,
) -> Result<(), PatchError> {
    let Some(after) = unknown_fields_of(kind, patched) else {
        return Ok(());
    };
    let existing = unknown_fields_of(kind, before).unwrap_or_default();
    let introduced: Vec<String> = after
        .into_iter()
        .filter(|f| !existing.contains(f))
        .map(|f| format!("unknown field \"{f}\""))
        .collect();
    if introduced.is_empty() {
        return Ok(());
    }
    Err(PatchError::Status(rusternetes_common::Error::Internal(
        format!("strict decoding error: {}", introduced.join(", ")),
    )))
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
            Ok(patched) => {
                strict_decode_check(
                    &request.versioned_attributes.versioned_kind,
                    &object,
                    &patched,
                )?;
                Ok(patched)
            }
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
    use crate::admission::policy_matching::Attributes;
    use crate::admission::policy_mutating::VersionedAttributes;
    use rusternetes_common::admission::Operation as AdmissionOperation;
    use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
    use serde_json::json;

    fn attributes(object: &Value) -> Attributes {
        Attributes {
            kind: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            resource: GroupVersionResource {
                group: "apps".into(),
                version: "v1".into(),
                resource: "deployments".into(),
            },
            subresource: "".into(),
            namespace: "default".into(),
            name: "d1".into(),
            operation: AdmissionOperation::Create,
            object: Some(object.clone()),
            old_object: None,
            user_info: None,
            dry_run: false,
            operation_options: None,
        }
    }

    fn run_with(
        patcher: Arc<dyn Patcher>,
        object: Value,
        attributes: &Attributes,
    ) -> Result<Value, String> {
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
        let req = PatchRequest {
            matched_resource: &gvr,
            versioned_attributes: &versioned,
            attributes,
            param: None,
            namespace: None,
            authorizer: None,
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(patcher.patch(&req)).map_err(|e| match e {
            PatchError::Status(e) => e.to_string(),
            PatchError::Other(m) => m,
        })
    }

    fn run(expression: &str, object: Value) -> Result<Value, String> {
        let attrs = attributes(&object);
        run_with(JsonPatcher::new_patcher(expression), object, &attrs)
    }

    fn ok(expression: &str, object: Value, expected: Value) {
        assert_eq!(run(expression, object).unwrap(), expected, "{expression}");
    }

    fn containers(names: &[&str]) -> Value {
        let c: Vec<Value> = names.iter().map(|n| json!({"name": n})).collect();
        json!({"spec": {"template": {"spec": {"containers": c}}}})
    }

    #[test]
    fn patch_introducing_unknown_field_is_a_strict_decoding_error() {
        // json_patch.go:87,111: the patched JSON is strict-decoded into the
        // typed object; an unknown field is an InternalError.
        let o = json!({"spec": {"replicas": 1}});
        let err = run(
            r#"[JSONPatch{op: "add", path: "/spec/bogus", value: "x"}]"#,
            o,
        )
        .unwrap_err();
        assert!(
            err.contains("strict decoding error: unknown field \"spec.bogus\""),
            "{err}"
        );
    }

    #[test]
    fn patch_leaving_known_fields_decodes() {
        ok(
            r#"[JSONPatch{op: "replace", path: "/spec/replicas", value: 3}]"#,
            json!({"spec": {"replicas": 1}}),
            json!({"spec": {"replicas": 3}}),
        );
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

    // json_patch_test.go: "JSONPatch key escaping".
    #[test]
    fn json_patch_key_escaping() {
        ok(
            r#"[
                JSONPatch{op: "add", path: "/metadata/labels", value: {}},
                JSONPatch{op: "add", path: "/metadata/labels/" + jsonpatch.escapeKey("k8s.io/x~y"), value: "true"}
            ]"#,
            json!({"metadata": {"labels": {}}}),
            json!({"metadata": {"labels": {"k8s.io/x~y": "true"}}}),
        );
    }

    // `CreateAdmissionRequest` (admission/plugin/cel/condition.go:115-175)
    // fills the `request` variable from the request's attributes.
    #[test]
    fn request_variable_carries_attributes() {
        let object = json!({"metadata": {}});
        let mut attrs = attributes(&object);
        attrs.operation = AdmissionOperation::Update;
        attrs.subresource = "scale".into();
        let got = run_with(
            JsonPatcher::new_patcher(
                r#"[JSONPatch{op: "add", path: "/metadata/annotations", value: {
                    "operation": request.operation,
                    "name": request.name,
                    "namespace": request.namespace,
                    "subResource": request.subResource,
                    "requestSubResource": request.requestSubResource,
                    "requestKind": request.requestKind.kind,
                    "requestResource": request.requestResource.resource,
                    "dryRun": request.dryRun ? "true" : "false"
                }}]"#,
            ),
            object,
            &attrs,
        );
        assert_eq!(
            got.unwrap(),
            json!({"metadata": {"annotations": {
                "operation": "UPDATE", "name": "d1", "namespace": "default",
                "subResource": "scale", "requestSubResource": "scale",
                "requestKind": "Deployment", "requestResource": "deployments",
                "dryRun": "false"
            }}})
        );
    }

    // `CreateAdmissionRequest` (condition.go:128-153, :172-174): userInfo,
    // dryRun and options come from the request's attributes.
    #[test]
    fn request_variable_carries_user_dry_run_and_options() {
        let object = json!({"metadata": {}});
        let mut attrs = attributes(&object);
        attrs.user_info = Some(rusternetes_common::auth::UserInfo {
            username: "alice".into(),
            uid: "u1".into(),
            groups: vec!["g1".into()],
            extra: [("k".to_string(), vec!["v".to_string()])].into(),
        });
        attrs.dry_run = true;
        attrs.operation_options = Some(json!({"fieldManager": "fm"}));
        let got = run_with(
            JsonPatcher::new_patcher(
                r#"[JSONPatch{op: "add", path: "/metadata/annotations", value: {
                    "user": request.userInfo.username,
                    "uid": request.userInfo.uid,
                    "group": request.userInfo.groups[0],
                    "extra": request.userInfo.extra["k"][0],
                    "dryRun": request.dryRun ? "true" : "false",
                    "fm": request.options.fieldManager
                }}]"#,
            ),
            object,
            &attrs,
        );
        assert_eq!(
            got.unwrap(),
            json!({"metadata": {"annotations": {
                "user": "alice", "uid": "u1", "group": "g1", "extra": "v",
                "dryRun": "true", "fm": "fm"
            }}})
        );
    }

    fn vars(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect()
    }

    // composition.go: `variables.<name>` is bound, and a variable may
    // reference the ones declared before it.
    #[test]
    fn variables_are_bound() {
        let object = json!({"metadata": {"name": "d1"}});
        let attrs = attributes(&object);
        let p = JsonPatcher::new_patcher_with_variables(
            r#"[JSONPatch{op: "add", path: "/metadata/labels", value: {"x": variables.m}}]"#,
            &vars(&[
                ("n", "object.metadata.name + '-x'"),
                ("m", "variables.n + '!'"),
            ]),
        );
        assert_eq!(
            run_with(p, object, &attrs).unwrap(),
            json!({"metadata": {"name": "d1", "labels": {"x": "d1-x!"}}})
        );
    }

    // `variableAccessor.Callback` is lazy: a failing variable only fails the
    // evaluation when the expression reads it.
    #[test]
    fn failing_variable_only_fails_when_used() {
        let object = json!({"metadata": {"name": "d1"}});
        let attrs = attributes(&object);
        let decls = vars(&[("bad", "object.spec.nope.deeper")]);
        let unused = JsonPatcher::new_patcher_with_variables(
            r#"[JSONPatch{op: "add", path: "/metadata/labels", value: {}}]"#,
            &decls,
        );
        assert!(run_with(unused, object.clone(), &attrs).is_ok());
        let used = JsonPatcher::new_patcher_with_variables(
            r#"[JSONPatch{op: "add", path: "/metadata/labels", value: {"x": variables.bad}}]"#,
            &decls,
        );
        let err = run_with(used, object, &attrs).unwrap_err();
        assert!(
            err.contains(r#"composited variable "bad" fails to evaluate"#),
            "{err}"
        );
    }
}
