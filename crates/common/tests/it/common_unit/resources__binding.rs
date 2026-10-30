//! Unit tests moved from `src/resources/binding.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::binding::*;

    #[test]
    fn test_binding_creation() {
        let target = ObjectReference::new("node-1").with_kind("Node");

        let binding = Binding::new("test-pod", "default", target.clone());

        assert_eq!(binding.metadata.name, "test-pod");
        assert_eq!(binding.metadata.namespace, Some("default".to_string()));
        assert_eq!(binding.type_meta.kind, "Binding");
        assert_eq!(binding.type_meta.api_version, "v1");
        assert_eq!(binding.target.name, "node-1");
        assert_eq!(binding.target.kind, Some("Node".to_string()));
    }

    #[test]
    fn test_object_reference() {
        let obj_ref = ObjectReference::new("my-pod")
            .with_kind("Pod")
            .with_namespace("default");

        assert_eq!(obj_ref.name, "my-pod");
        assert_eq!(obj_ref.kind, Some("Pod".to_string()));
        assert_eq!(obj_ref.namespace, Some("default".to_string()));
    }
}
