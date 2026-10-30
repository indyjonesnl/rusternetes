//! Unit tests moved from `src/commands/explain.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::explain::*;

    #[test]
    fn test_get_resource_docs_has_pod() {
        let docs = get_resource_docs();
        assert!(docs.contains_key("pod"));
        let pod_doc = &docs["pod"];
        assert_eq!(pod_doc.kind, "Pod");
        assert_eq!(pod_doc.version, "v1");
    }

    #[test]
    fn test_get_resource_docs_has_service() {
        let docs = get_resource_docs();
        assert!(docs.contains_key("service"));
        assert_eq!(docs["service"].kind, "Service");
    }

    #[test]
    fn test_get_resource_docs_has_deployment() {
        let docs = get_resource_docs();
        assert!(docs.contains_key("deployment"));
        assert_eq!(docs["deployment"].version, "apps/v1");
    }

    #[test]
    fn test_pod_fields_contain_spec() {
        let docs = get_resource_docs();
        let pod_doc = &docs["pod"];
        assert!(pod_doc.fields.contains_key("spec"));
        assert_eq!(pod_doc.fields["spec"].type_info, "PodSpec");
    }

    #[tokio::test]
    async fn test_execute_unknown_resource_fails() {
        let result = execute("nonexistent", None, false).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_get_resource_docs_has_standard_fields() {
        let docs = get_resource_docs();
        for doc in docs.values() {
            assert!(doc.fields.contains_key("apiVersion"));
            assert!(doc.fields.contains_key("kind"));
            assert!(doc.fields.contains_key("metadata"));
        }
    }

    #[tokio::test]
    async fn test_execute_pod_succeeds() {
        let result = execute("pod", None, false).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_pod_field_path() {
        let result = execute("pod.spec", None, false).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_unknown_field_path_fails() {
        let result = execute("pod.nonexistentfield", None, false).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_resource_doc_fields_have_descriptions() {
        let docs = get_resource_docs();
        for doc in docs.values() {
            assert!(!doc.description.is_empty());
            for field_doc in doc.fields.values() {
                assert!(!field_doc.description.is_empty());
                assert!(!field_doc.type_info.is_empty());
            }
        }
    }
}
