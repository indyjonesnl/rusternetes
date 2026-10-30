//! Unit tests moved from `src/handlers/poddisruptionbudget.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Bytes,
        extract::{Path, Query, State},
        http::HeaderMap,
        response::{IntoResponse, Response},
        Extension,
    };
    pub use rusternetes_api_server::endpoints::handlers::{self as endpoints, RequestScope};
    pub use rusternetes_api_server::registry::policy::poddisruptionbudget;
    pub use rusternetes_api_server::{middleware::AuthContext, state::ApiServerState};
    pub use rusternetes_common::{
        admission::{GroupVersionKind, GroupVersionResource},
        authz::{Decision, RequestAttributes},
        resources::PodDisruptionBudget,
        List, Result,
    };
    pub use rusternetes_storage::build_prefix;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::debug;
}

mod tests {
    use rusternetes_common::resources::PodDisruptionBudget;
    use rusternetes_common::resources::{IntOrString, PodDisruptionBudgetSpec};
    use rusternetes_common::types::LabelSelector;
    use std::collections::HashMap;

    #[test]
    fn test_pdb_handler_structure() {
        // Basic test to ensure handler structure is correct
        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(2)),
            max_unavailable: None,
            selector: Some(LabelSelector {
                match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                match_expressions: None,
            }),
            unhealthy_pod_eviction_policy: None,
        };

        let pdb = PodDisruptionBudget::new("test-pdb", "default", spec);
        assert_eq!(pdb.metadata.name, "test-pdb");
        assert_eq!(pdb.metadata.namespace, Some("default".to_string()));
    }
}
