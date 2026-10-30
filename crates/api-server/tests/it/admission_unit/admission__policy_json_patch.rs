//! Unit tests moved from `src/admission/policy_json_patch.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::admission::policy_json_patch_ops::{
        apply, ApplyError, Operation,
    };
    pub use rusternetes_api_server::admission::policy_mutating::{
        PatchError, PatchRequest, Patcher,
    };
    pub use rusternetes_common::admission::GroupVersionKind;
    pub use rusternetes_common::admission::Operation as AdmissionOperation;
    pub use rusternetes_common::auth::UserInfo;
    pub use rusternetes_common::cel::{CELContext, CELEvaluator};
    pub use rusternetes_common::cel_authz_library;
    pub use rusternetes_common::cel_cost::{charge_budget, CostOptions};
    pub use rusternetes_common::cel_struct::{self, TypeCheck};
    pub use serde_json::{json, Value};
    pub use std::sync::{Arc, Mutex};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::policy_json_patch::*;
    use rusternetes_api_server::admission::policy_matching::Attributes;
    use rusternetes_api_server::admission::policy_mutating::VersionedAttributes;
    use rusternetes_common::admission::Operation as AdmissionOperation;
    use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
    use rusternetes_common::cel_cost::RUNTIME_CEL_COST_BUDGET;
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
        rt.block_on(patcher.patch(&req, RUNTIME_CEL_COST_BUDGET))
            .map_err(|e| match e {
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

#[path = "policy_patch_budget_tests.rs"]
mod budget_tests {
    //! `RuntimeCELCostBudget` enforcement of the MutatingAdmissionPolicy patchers
    //! (#3507): `dispatcher.go:259` passes `celconfig.RuntimeCELCostBudget` to
    //! `Patcher.Patch`, which hands it to `MutatingEvaluator.ForInput`
    //! (`json_patch.go:82`, `smd.go:77`); `activation.Evaluate`
    //! (`activation.go:132-189`) charges the composited variables and the
    //! expression against it and fails with `ErrOutOfBudget` when it runs out.

    use super::__parent::*;
    use rusternetes_api_server::admission::policy_apply_config::ApplyConfigPatcher;
    use rusternetes_api_server::admission::policy_json_patch::*;
    use rusternetes_api_server::admission::policy_matching::Attributes;
    use rusternetes_api_server::admission::policy_mutating::VersionedAttributes;
    use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
    use rusternetes_common::cel_cost::RUNTIME_CEL_COST_BUDGET;
    use serde_json::json;

    const OUT_OF_BUDGET: &str =
        "validation failed due to running out of cost budget, no further validation rules will be run";

    /// Each `object.xs.all(i, i >= 0)` over this object costs 500,003: under the
    /// 1M per-call limit, so every variable evaluates, but more than twenty of
    /// them are over the 10M `RuntimeCELCostBudget`.
    fn heavy_object() -> Value {
        let xs: Vec<u32> = (0..100_000).collect();
        json!({"xs": xs})
    }

    fn variables(n: usize) -> Vec<(String, String)> {
        (0..n)
            .map(|i| (format!("v{i}"), "object.xs.all(i, i >= 0)".to_string()))
            .collect()
    }

    fn reads(n: usize) -> String {
        (0..n)
            .map(|i| format!("variables.v{i}"))
            .collect::<Vec<_>>()
            .join(" && ")
    }

    fn run(patcher: Arc<dyn Patcher>) -> Result<Value, String> {
        let gvk = GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        };
        let gvr = GroupVersionResource {
            group: "apps".into(),
            version: "v1".into(),
            resource: "deployments".into(),
        };
        let object = heavy_object();
        let versioned = VersionedAttributes {
            versioned_kind: gvk.clone(),
            versioned_object: Some(object.clone()),
            versioned_old_object: None,
            dirty: false,
        };
        let attributes = Attributes {
            kind: gvk,
            resource: gvr.clone(),
            subresource: "".into(),
            namespace: "default".into(),
            name: "d1".into(),
            operation: Operation::Create,
            object: Some(object),
            old_object: None,
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

    fn apply_config(n: usize) -> Result<Value, String> {
        let expr = format!("{} ? Object{{}} : Object{{}}", reads(n));
        run(ApplyConfigPatcher::new_patcher_with_variables(
            &expr,
            &variables(n),
        ))
    }

    fn json_patch(n: usize) -> Result<Value, String> {
        let expr = format!(
            "{} ? [JSONPatch{{op: \"add\", path: \"/spec\", value: 1}}] : []",
            reads(n)
        );
        run(JsonPatcher::new_patcher_with_variables(
            &expr,
            &variables(n),
        ))
    }

    /// activation.go:162-168: the composited variables spend more than the
    /// budget, so the patch fails instead of applying.
    #[test]
    fn json_patch_variables_over_budget_fail_the_patch() {
        assert_eq!(json_patch(24).unwrap_err(), OUT_OF_BUDGET);
    }

    /// smd.go:77 `ForInput(..., runtimeCELCostBudget)`.
    #[test]
    fn apply_configuration_variables_over_budget_fail_the_patch() {
        assert_eq!(apply_config(24).unwrap_err(), OUT_OF_BUDGET);
    }

    /// Within the budget nothing changes.
    #[test]
    fn within_budget_still_patches() {
        json_patch(3).unwrap();
        apply_config(3).unwrap();
    }
}
