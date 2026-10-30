//! Unit tests moved from `src/admission/policy_apply_config.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::admission::policy_json_patch::evaluate_expression_budgeted;
    pub use rusternetes_api_server::admission::policy_mutating::{
        PatchError, PatchRequest, Patcher,
    };
    pub use rusternetes_api_server::fieldmanager::apply::merge_values;
    pub use rusternetes_api_server::fieldmanager::fieldset::{field_kind, FieldKind};
    pub use rusternetes_common::cel::CELEvaluator;
    pub use rusternetes_common::cel_struct::{self, TypeCheck};
    pub use serde_json::{Map, Value};
    pub use std::sync::{Arc, Mutex};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::policy_apply_config::*;
    use rusternetes_api_server::admission::policy_matching::Attributes;
    use rusternetes_api_server::admission::policy_mutating::VersionedAttributes;
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
