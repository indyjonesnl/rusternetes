//! Unit tests moved from `src/handlers/volumeattributesclass.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Bytes,
        extract::{Path, Query, State},
        http::HeaderMap,
        response::Response,
        Extension,
    };
    pub use rusternetes_api_server::endpoints::handlers::{self as endpoints, RequestScope};
    pub use rusternetes_api_server::registry::storage::volumeattributesclass;
    pub use rusternetes_api_server::{middleware::AuthContext, state::ApiServerState};
    pub use rusternetes_common::{
        admission::{GroupVersionKind, GroupVersionResource},
        authz::{Decision, RequestAttributes},
        resources::csi::VolumeAttributesClass,
        List, Result,
    };
    pub use rusternetes_storage::build_prefix;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::debug;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::volumeattributesclass::*;
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use std::collections::HashMap;

    fn create_test_vac(name: &str) -> VolumeAttributesClass {
        let mut params = HashMap::new();
        params.insert("type".to_string(), "ssd".to_string());
        params.insert("iops".to_string(), "3000".to_string());

        VolumeAttributesClass {
            type_meta: TypeMeta {
                kind: "VolumeAttributesClass".to_string(),
                api_version: "storage.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            driver_name: "test-driver".to_string(),
            parameters: Some(params),
        }
    }

    #[tokio::test]
    async fn test_volumeattributesclass_serialization() {
        let vac = create_test_vac("fast-storage");
        let json = serde_json::to_string(&vac).unwrap();
        let deserialized: VolumeAttributesClass = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.metadata.name, "fast-storage");
        assert_eq!(deserialized.driver_name, "test-driver");
    }
}
