//! Unit tests moved from `src/resources/config_and_secret.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::config_and_secret::*;

    #[test]
    fn test_configmap_creation() {
        let mut data = HashMap::new();
        data.insert("key1".to_string(), "value1".to_string());

        let cm = ConfigMap::new("test-config", "default").with_data(data);

        assert_eq!(cm.metadata.name, "test-config");
        assert_eq!(cm.metadata.namespace, Some("default".to_string()));
        assert!(cm.data.is_some());
    }

    #[test]
    fn test_secret_creation() {
        let mut data = HashMap::new();
        data.insert("password".to_string(), b"secret123".to_vec());

        let secret = Secret::new("test-secret", "default").with_data(data);

        assert_eq!(secret.metadata.name, "test-secret");
        assert_eq!(secret.metadata.namespace, Some("default".to_string()));
        assert!(secret.data.is_some());
        assert_eq!(secret.secret_type, Some("Opaque".to_string()));
    }
}
