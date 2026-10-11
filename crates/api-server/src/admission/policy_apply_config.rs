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
use serde_json::Value;

use super::policy_mutating::{PatchError, PatchRequest, Patcher};

/// `NewApplyConfigurationPatcher` (smd.go:59-61).
pub struct ApplyConfigPatcher {
    expression: String,
    compile_error: Option<String>,
    evaluator: Mutex<CELEvaluator>,
}

impl ApplyConfigPatcher {
    pub fn new_patcher(expression: &str) -> Arc<dyn Patcher> {
        Arc::new(Self {
            expression: expression.to_string(),
            compile_error: None,
            evaluator: Mutex::new(CELEvaluator::new()),
        })
    }
}

#[async_trait]
impl Patcher for ApplyConfigPatcher {
    async fn patch(&self, _request: &PatchRequest<'_>) -> Result<Value, PatchError> {
        let _ = (&self.expression, &self.compile_error, &self.evaluator);
        Err(PatchError::Other("not implemented".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::policy_mutating::VersionedAttributes;
    use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
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
        let patcher = ApplyConfigPatcher::new_patcher(expression);
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
        let got = run(
            "Object{spec: Object.spec{replicas: 3}}",
            env_object(),
            None,
        )
        .unwrap();
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
}
