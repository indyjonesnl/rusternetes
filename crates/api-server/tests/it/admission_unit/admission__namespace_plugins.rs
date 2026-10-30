//! Unit tests moved from `src/admission/namespace_plugins.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::extract::{Extension, Request};
    pub use axum::middleware::Next;
    pub use axum::response::Response;
    pub use rusternetes_api_server::admission::builtin_plugins::new_forbidden;
    pub use rusternetes_api_server::admission::plugins::enabled;
    pub use rusternetes_api_server::registry::core::namespace;
    pub use rusternetes_api_server::registry::generic::CreateOptions;
    pub use rusternetes_api_server::registry::rest::{GroupResource, RequestContext, RestStorage};
    pub use rusternetes_common::admission::{GroupVersionKind, Operation};
    pub use rusternetes_common::resources::Namespace;
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::{build_key, Storage, StorageBackend};
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::namespace_plugins::*;

    #[test]
    fn lifecycle_operation_is_create_update_delete_not_connect() {
        use axum::http::Method;
        let op = |m: Method, p: &str| lifecycle_operation(&m, p);
        assert_eq!(
            op(Method::POST, "/api/v1/namespaces/a/pods"),
            Some(Operation::Create)
        );
        assert_eq!(
            op(Method::PATCH, "/api/v1/namespaces/a/pods/p"),
            Some(Operation::Update)
        );
        assert_eq!(
            op(Method::DELETE, "/api/v1/namespaces/a/pods/p"),
            Some(Operation::Delete)
        );
        assert_eq!(op(Method::POST, "/api/v1/namespaces/a/pods/p/exec"), None);
        assert_eq!(op(Method::GET, "/api/v1/namespaces/a/pods"), None);
    }

    fn pod_kind() -> GroupVersionKind {
        GroupVersionKind {
            group: String::new(),
            version: "version".into(),
            kind: "Pod".into(),
        }
    }

    fn namespace_kind() -> GroupVersionKind {
        GroupVersionKind {
            kind: "Namespace".into(),
            ..pod_kind()
        }
    }

    async fn storage_with(namespaces: &[&str]) -> Arc<StorageBackend> {
        let storage = Arc::new(StorageBackend::new_memory());
        for n in namespaces {
            let mut ns = Namespace::new(*n);
            namespace::convert_to_internal(&mut ns);
            storage
                .create(&build_key("namespaces", None, n), &ns)
                .await
                .unwrap();
        }
        storage
    }

    async fn provision(
        storage: &Arc<StorageBackend>,
        kind: &GroupVersionKind,
        ns: &str,
        dry_run: bool,
    ) -> Result<()> {
        auto_provision_admit(
            storage,
            kind,
            &GroupResource::new("", "pods"),
            Some(ns),
            "123",
            dry_run,
        )
        .await
    }

    /// `TestAdmission` (autoprovision/admission_test.go:94).
    #[tokio::test]
    async fn autoprovision_creates_the_missing_namespace() {
        let storage = storage_with(&[]).await;
        provision(&storage, &pod_kind(), "test", false)
            .await
            .unwrap();
        let ns: Namespace = storage
            .get(&build_key("namespaces", None, "test"))
            .await
            .unwrap();
        assert_eq!(ns.metadata.name, "test");
        assert_eq!(
            ns.status.and_then(|s| s.phase),
            Some(rusternetes_common::types::Phase::Active)
        );
    }

    /// `TestAdmissionNamespaceExists` (:114): the existing namespace is left
    /// alone.
    #[tokio::test]
    async fn autoprovision_leaves_an_existing_namespace() {
        let storage = storage_with(&["test"]).await;
        let before: Namespace = storage
            .get(&build_key("namespaces", None, "test"))
            .await
            .unwrap();
        provision(&storage, &pod_kind(), "test", false)
            .await
            .unwrap();
        let after: Namespace = storage
            .get(&build_key("namespaces", None, "test"))
            .await
            .unwrap();
        assert_eq!(before.metadata.uid, after.metadata.uid);
    }

    /// `TestAdmissionDryRun` (:134): nothing is created.
    #[tokio::test]
    async fn autoprovision_ignores_dry_run() {
        let storage = storage_with(&[]).await;
        provision(&storage, &pod_kind(), "test", true)
            .await
            .unwrap();
        assert!(!namespace_exists(&storage, "test").await.unwrap());
    }

    /// `TestIgnoreAdmission` (:154): a Namespace request provisions nothing.
    #[tokio::test]
    async fn autoprovision_ignores_namespace_kind() {
        let storage = storage_with(&[]).await;
        provision(&storage, &namespace_kind(), "test", false)
            .await
            .unwrap();
        assert!(!namespace_exists(&storage, "test").await.unwrap());
        // No namespace on the request (cluster-scoped) is ignored too.
        auto_provision_admit(
            &storage,
            &pod_kind(),
            &GroupResource::new("", "nodes"),
            None,
            "n",
            false,
        )
        .await
        .unwrap();
    }

    /// `TestAdmissionNamespaceExists` (exists/admission_test.go:82).
    #[tokio::test]
    async fn exists_admits_an_existing_namespace() {
        let storage = storage_with(&["test"]).await;
        exists_validate(&storage, &pod_kind(), Some("test"))
            .await
            .unwrap();
    }

    /// `TestAdmissionNamespaceDoesNotExist` (:99): NotFound.
    #[tokio::test]
    async fn exists_rejects_a_missing_namespace() {
        let storage = storage_with(&[]).await;
        let err = exists_validate(&storage, &pod_kind(), Some("test"))
            .await
            .unwrap_err();
        assert!(matches!(&err, Error::NotFound(m) if m == "namespaces \"test\" not found"));
        // A Namespace request and a cluster-scoped request pass.
        exists_validate(&storage, &namespace_kind(), Some("test"))
            .await
            .unwrap();
        exists_validate(&storage, &pod_kind(), None).await.unwrap();
    }
}
