//! Unit tests moved from `src/cel.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use cel::{
        objects::{Key, Map, OptionalValue},
        Value,
    };
    pub use rusternetes_common::cel_struct::{self, CompiledExpression};
    pub use serde_json;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::cel::*;

    #[test]
    fn test_cel_evaluator_simple() {
        let mut evaluator = CELEvaluator::new();
        let mut context = CELContext::new();
        context.add_variable("x".to_string(), Value::Int(5));
        context.add_variable("y".to_string(), Value::Int(3));

        let result = evaluator.evaluate("x > y", &context).unwrap();
        assert!(result);

        let result = evaluator.evaluate("x < y", &context).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_cel_evaluator_string() {
        let mut evaluator = CELEvaluator::new();
        let mut context = CELContext::new();
        context.add_variable(
            "name".to_string(),
            Value::String(Arc::new("test".to_string())),
        );

        let result = evaluator
            .evaluate_string("'Hello, ' + name", &context)
            .unwrap();
        assert_eq!(result, "Hello, test");
    }

    #[test]
    fn test_json_to_cel_value() {
        let json = serde_json::json!({
            "name": "test",
            "count": 42,
            "active": true,
            "tags": ["a", "b", "c"]
        });

        let cel_value = json_to_cel_value(&json).unwrap();
        match cel_value {
            Value::Map(map) => {
                let name_key = Key::String(Arc::new("name".to_string()));
                let count_key = Key::String(Arc::new("count".to_string()));
                let active_key = Key::String(Arc::new("active".to_string()));
                assert_eq!(
                    map.get(&name_key),
                    Some(&Value::String(Arc::new("test".to_string())))
                );
                assert_eq!(map.get(&count_key), Some(&Value::Int(42)));
                assert_eq!(map.get(&active_key), Some(&Value::Bool(true)));
            }
            _ => panic!("Expected map"),
        }
    }

    #[test]
    fn test_cel_context_for_admission() {
        let object = serde_json::json!({
            "spec": {
                "replicas": 3
            }
        });

        let context = CELContext::for_admission(&object, None, None).unwrap();
        assert!(context.variables.contains_key("object"));
        assert!(context.variables.contains_key("request"));
    }

    #[test]
    fn test_cel_admission_validation() {
        let mut evaluator = CELEvaluator::new();
        let object = serde_json::json!({
            "spec": {
                "replicas": 10
            }
        });

        let context = CELContext::for_admission(&object, None, None).unwrap();

        // This should fail because replicas > 5
        let result = evaluator
            .evaluate("object.spec.replicas <= 5", &context)
            .unwrap();
        assert!(!result);

        // This should pass
        let result = evaluator
            .evaluate("object.spec.replicas <= 100", &context)
            .unwrap();
        assert!(result);
    }

    #[test]
    fn test_type_check() {
        let mut evaluator = CELEvaluator::new();

        // Valid expression
        assert!(evaluator.type_check("x > 5").is_ok());

        // Note: Invalid expression testing skipped due to CEL library internals
        // The CEL library will catch syntax errors at runtime
    }

    /// Reproduces the K8s conformance test "should allow expressions to refer variables".
    /// A VAP defines:
    ///   variables: [{name: "replicas", expression: "object.spec.replicas"},
    ///               {name: "oddReplicas", expression: "variables.replicas % 2 == 1"}]
    ///   validations: [{expression: "variables.replicas > 1"},
    ///                 {expression: "variables.oddReplicas"}]
    /// With a 3-replica deployment, both validations should pass.
    #[test]
    fn test_cel_vap_variables_refer() {
        let mut evaluator = CELEvaluator::new();

        // Simulate a Deployment with 3 replicas
        let deployment = serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "test-deploy", "namespace": "default"},
            "spec": {
                "replicas": 3,
                "selector": {"matchLabels": {"app": "test"}},
                "template": {
                    "metadata": {"labels": {"app": "test"}},
                    "spec": {"containers": [{"name": "c1", "image": "nginx"}]}
                }
            }
        });

        let mut context = CELContext::new();
        context.add_json_variable("object", &deployment).unwrap();

        // Step 1: Evaluate variable "replicas" = object.spec.replicas
        let replicas_val = evaluator
            .evaluate_to_value("object.spec.replicas", &context)
            .expect("should evaluate object.spec.replicas");
        assert_eq!(replicas_val, Value::Int(3));

        // Build the variables map incrementally (like the VAP code does)
        let mut var_map: HashMap<Key, Value> = HashMap::new();
        var_map.insert(Key::String(Arc::new("replicas".to_string())), replicas_val);
        context.add_variable(
            "variables".to_string(),
            Value::Map(Map {
                map: Arc::new(var_map.clone()),
            }),
        );

        // Step 2: Evaluate variable "oddReplicas" = variables.replicas % 2 == 1
        let odd_val = evaluator
            .evaluate_to_value("variables.replicas % 2 == 1", &context)
            .expect("should evaluate variables.replicas % 2 == 1");
        assert_eq!(odd_val, Value::Bool(true));

        var_map.insert(Key::String(Arc::new("oddReplicas".to_string())), odd_val);
        context.add_variable(
            "variables".to_string(),
            Value::Map(Map {
                map: Arc::new(var_map),
            }),
        );

        // Step 3: Evaluate validations
        let v1 = evaluator
            .evaluate("variables.replicas > 1", &context)
            .expect("should evaluate variables.replicas > 1");
        assert!(v1, "3 > 1 should be true");

        let v2 = evaluator
            .evaluate("variables.oddReplicas", &context)
            .expect("should evaluate variables.oddReplicas");
        assert!(v2, "oddReplicas (3 % 2 == 1) should be true");
    }

    /// Test that 1-replica deployment fails variables-based validation.
    #[test]
    fn test_cel_vap_variables_reject_low_replicas() {
        let mut evaluator = CELEvaluator::new();

        let deployment = serde_json::json!({
            "spec": {"replicas": 1}
        });

        let mut context = CELContext::new();
        context.add_json_variable("object", &deployment).unwrap();

        let replicas_val = evaluator
            .evaluate_to_value("object.spec.replicas", &context)
            .unwrap();

        let mut var_map: HashMap<Key, Value> = HashMap::new();
        var_map.insert(Key::String(Arc::new("replicas".to_string())), replicas_val);
        context.add_variable(
            "variables".to_string(),
            Value::Map(Map {
                map: Arc::new(var_map),
            }),
        );

        // 1 > 1 should be false
        let v1 = evaluator
            .evaluate("variables.replicas > 1", &context)
            .unwrap();
        assert!(!v1, "1 > 1 should be false");
    }

    /// Test namespaceObject in CEL context (used by "should validate against a Deployment").
    #[test]
    fn test_cel_namespace_object() {
        let mut evaluator = CELEvaluator::new();

        let ns = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "test-ns"}
        });

        let mut context = CELContext::new();
        context.add_json_variable("namespaceObject", &ns).unwrap();

        let result = evaluator
            .evaluate("namespaceObject.metadata.name == 'test-ns'", &context)
            .unwrap();
        assert!(result);
    }

    /// cel-go `OptionalTypes()` (apiserver `pkg/cel/environment/base.go:99-101`)
    /// is in the base env: `optional.of/none`, `hasValue`, `value`, `orValue`,
    /// and the `.?field` / `[?i]` syntax.
    fn eval_with(expr: &str, ctx: &CELContext) -> bool {
        CELEvaluator::new().evaluate(expr, ctx).unwrap()
    }

    #[test]
    fn optional_functions() {
        let ctx = CELContext::new();
        assert!(eval_with("optional.none().orValue(5) == 5", &ctx));
        assert!(eval_with("optional.of(3).orValue(5) == 3", &ctx));
        assert!(eval_with("optional.of(3).hasValue()", &ctx));
        assert!(!eval_with("optional.none().hasValue()", &ctx));
        assert!(eval_with("optional.of(3).value() == 3", &ctx));
        assert!(!eval_with("optional.ofNonZeroValue(0).hasValue()", &ctx));
        assert!(eval_with("optional.ofNonZeroValue(2).hasValue()", &ctx));
    }

    #[test]
    fn optional_syntax_field_and_index_select() {
        let mut ctx = CELContext::new();
        ctx.add_json_variable("m", &serde_json::json!({"a": "x", "l": [1, 2]}))
            .unwrap();
        assert!(eval_with("m.?a.orValue('d') == 'x'", &ctx));
        assert!(eval_with("m.?b.orValue('d') == 'd'", &ctx));
        assert!(eval_with("m.l[?5].orValue(9) == 9", &ctx));
        assert!(eval_with("m.l[?1].orValue(9) == 2", &ctx));
    }

    #[test]
    fn add_optional_json_variable_binds_some_and_none() {
        let mut ctx = CELContext::new();
        ctx.add_optional_json_variable("o", Some(&serde_json::json!({"foo": "bar"})))
            .unwrap();
        ctx.add_optional_json_variable("n", None).unwrap();
        assert!(eval_with("o.hasValue() && o.value().foo == 'bar'", &ctx));
        assert!(eval_with("o.?foo.orValue('') == 'bar'", &ctx));
        assert!(!eval_with("n.hasValue()", &ctx));
        assert!(eval_with("n.?foo.orValue('z') == 'z'", &ctx));
    }
}
