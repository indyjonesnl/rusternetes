//! Unit tests moved from `src/audit.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use serde::{Deserialize, Serialize};
    pub use std::sync::Arc;
    pub use tokio::fs::OpenOptions;
    pub use tokio::io::AsyncWriteExt;
    pub use tokio::sync::Mutex;
    pub use tracing::{error, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::audit::*;

    #[test]
    fn test_audit_level() {
        let level = AuditLevel::Metadata;
        assert_eq!(level, AuditLevel::Metadata);
    }

    #[test]
    fn test_audit_stage() {
        let stage = AuditStage::RequestReceived;
        assert_eq!(stage, AuditStage::RequestReceived);
    }

    #[test]
    fn test_audit_policy_default() {
        let policy = AuditPolicy::default();
        assert_eq!(policy.level, AuditLevel::Metadata);
        assert!(policy.log_requests);
        assert!(!policy.log_responses);
        assert!(policy.log_metadata);
    }

    #[tokio::test]
    async fn test_audit_event_serialization() {
        let event = AuditEvent {
            api_version: "audit.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            level: AuditLevel::Metadata,
            audit_id: "test-id".to_string(),
            stage: AuditStage::RequestReceived,
            request_uri: "/api/v1/pods".to_string(),
            verb: "GET".to_string(),
            user: UserInfo {
                username: "test-user".to_string(),
                uid: "123".to_string(),
                groups: vec!["system:authenticated".to_string()],
                extra: None,
            },
            user_agent: None,
            source_ips: Vec::new(),
            object_ref: None,
            response_status: None,
            response_object: None,
            request_received_timestamp: Utc::now(),
            stage_timestamp: Utc::now(),
            annotations: None,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("test-user"));
        assert!(json.contains("GET"));
    }

    #[tokio::test]
    async fn test_file_audit_backend() {
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        let backend = FileAuditBackend::new(path.clone()).await.unwrap();

        let event = AuditEvent {
            api_version: "audit.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            level: AuditLevel::Metadata,
            audit_id: "test-123".to_string(),
            stage: AuditStage::RequestReceived,
            request_uri: "/api/v1/namespaces".to_string(),
            verb: "list".to_string(),
            user: UserInfo {
                username: "admin".to_string(),
                uid: "admin-uid".to_string(),
                groups: vec!["system:masters".to_string()],
                extra: None,
            },
            user_agent: None,
            source_ips: Vec::new(),
            object_ref: None,
            response_status: None,
            response_object: None,
            request_received_timestamp: Utc::now(),
            stage_timestamp: Utc::now(),
            annotations: None,
        };

        backend.log(event).await.unwrap();
        backend.flush().await.unwrap();

        // Read the file and verify content
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("admin"));
        assert!(content.contains("test-123"));
    }

    #[test]
    fn test_webhook_audit_backend_creation() {
        let backend = WebhookAuditBackend::new("http://localhost:8080/audit".to_string());
        assert_eq!(backend.url, "http://localhost:8080/audit");
    }

    #[tokio::test]
    async fn test_multi_backend() {
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        let file_backend = Arc::new(FileAuditBackend::new(path.clone()).await.unwrap());
        let multi_backend = MultiAuditBackend::new(vec![file_backend]);

        let event = AuditEvent {
            api_version: "audit.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            level: AuditLevel::Metadata,
            audit_id: "multi-123".to_string(),
            stage: AuditStage::ResponseComplete,
            request_uri: "/api/v1/pods".to_string(),
            verb: "create".to_string(),
            user: UserInfo {
                username: "developer".to_string(),
                uid: "dev-uid".to_string(),
                groups: vec!["developers".to_string()],
                extra: None,
            },
            user_agent: None,
            source_ips: Vec::new(),
            object_ref: Some(ObjectReference {
                resource: Some("pods".to_string()),
                subresource: None,
                api_group: None,
                namespace: Some("default".to_string()),
                name: Some("test-pod".to_string()),
                uid: Some("pod-123".to_string()),
                api_version: Some("v1".to_string()),
                resource_version: Some("1".to_string()),
            }),
            response_status: Some(ResponseStatus {
                code: 201,
                message: Some("Created".to_string()),
                ..Default::default()
            }),
            response_object: None,
            request_received_timestamp: Utc::now(),
            stage_timestamp: Utc::now(),
            annotations: None,
        };

        multi_backend.log(event).await.unwrap();
        multi_backend.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("developer"));
        assert!(content.contains("multi-123"));
    }

    #[tokio::test]
    async fn test_audit_logger() {
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        let backend = Arc::new(FileAuditBackend::new(path.clone()).await.unwrap());
        let policy = AuditPolicy::default();
        let logger = AuditLogger::new(backend, policy);

        let user = UserInfo {
            username: "test-user".to_string(),
            uid: "test-uid".to_string(),
            groups: vec!["users".to_string()],
            extra: None,
        };

        let audit_id = logger
            .log_request(
                "/api/v1/pods".to_string(),
                "list".to_string(),
                user.clone(),
                None,
            )
            .await;

        assert!(!audit_id.is_empty());

        logger
            .log_response(
                audit_id,
                "/api/v1/pods".to_string(),
                "list".to_string(),
                user,
                None,
                200,
                Some("OK".to_string()),
            )
            .await;

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("test-user"));
        assert!(content.contains("list"));
    }
}
