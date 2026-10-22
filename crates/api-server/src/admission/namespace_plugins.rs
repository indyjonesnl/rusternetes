//! The `NamespaceAutoProvision` and `NamespaceExists` admission plugins and
//! the flag gate of `NamespaceLifecycle`.
//!
//! Ported from `plugin/pkg/admission/namespace/autoprovision/admission.go`
//! (`Provision.Admit`) and `plugin/pkg/admission/namespace/exists/admission.go`
//! (`Exists.Validate`), release-1.35. Upstream reads the namespace through an
//! informer lister and falls back to a live GET before answering NotFound
//! (exists) or creating (autoprovision); here the one read is the storage GET,
//! which is the live read, so the `WaitForReady` gate has nothing to wait on.

use std::sync::Arc;

use axum::extract::{Extension, Request};
use axum::middleware::Next;
use axum::response::Response;
use rusternetes_common::admission::{GroupVersionKind, Operation};
use rusternetes_common::resources::Namespace;
use rusternetes_common::{Error, Result};
use rusternetes_storage::{build_key, Storage, StorageBackend};

use super::builtin_plugins::new_forbidden;
use super::plugins::enabled;
use crate::registry::core::namespace;
use crate::registry::generic::CreateOptions;
use crate::registry::rest::{GroupResource, RequestContext, RestStorage};

/// `a.GetKind().GroupKind() == api.Kind("Namespace")`.
fn is_namespace_kind(kind: &GroupVersionKind) -> bool {
    kind.group.is_empty() && kind.kind == "Namespace"
}

/// Whether the namespace exists, read live from storage. Any read error other
/// than NotFound is returned as is for the caller to wrap.
async fn namespace_exists(storage: &StorageBackend, name: &str) -> Result<bool> {
    match storage
        .get::<serde_json::Value>(&build_key("namespaces", None, name))
        .await
    {
        Ok(_) => Ok(true),
        Err(Error::NotFound(_)) => Ok(false),
        Err(e) => Err(e),
    }
}

/// `Provision.Admit` (autoprovision/admission.go:55-95): on CREATE in a
/// namespace that does not exist, create it. A dry-run request, a request
/// without a namespace and a request for a Namespace itself are ignored.
/// `NewForbidden(a, err)` wraps a failed read or create, except
/// `AlreadyExists` (a racing provisioner), which is success.
pub async fn auto_provision_admit(
    storage: &Arc<StorageBackend>,
    kind: &GroupVersionKind,
    resource: &GroupResource,
    request_namespace: Option<&str>,
    name: &str,
    dry_run: bool,
) -> Result<()> {
    if dry_run {
        return Ok(());
    }
    let Some(ns_name) = request_namespace.filter(|n| !n.is_empty()) else {
        return Ok(());
    };
    if is_namespace_kind(kind) {
        return Ok(());
    }
    let forbidden = |err: Error| new_forbidden(&resource.to_string(), name, err);
    match namespace_exists(storage, ns_name).await {
        Ok(true) => return Ok(()),
        Ok(false) => {}
        Err(e) => return Err(forbidden(e)),
    }
    let mut ns = Namespace::new(ns_name);
    namespace::convert_to_internal(&mut ns);
    let ctx = RequestContext::new(None);
    match namespace::new_rest(storage.clone())
        .create(&ctx, ns, None, &CreateOptions { dry_run: false })
        .await
    {
        Ok(_) | Err(Error::AlreadyExists(_)) => Ok(()),
        Err(e) => Err(forbidden(e)),
    }
}

/// `Exists.Validate` (exists/admission.go:55-86): reject a request in a
/// namespace that does not exist with the NotFound the lookup returned.
pub async fn exists_validate(
    storage: &StorageBackend,
    kind: &GroupVersionKind,
    request_namespace: Option<&str>,
) -> Result<()> {
    let Some(ns_name) = request_namespace.filter(|n| !n.is_empty()) else {
        return Ok(());
    };
    if is_namespace_kind(kind) {
        return Ok(());
    }
    match namespace_exists(storage, ns_name).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(Error::NotFound(format!(
            "namespaces \"{ns_name}\" not found"
        ))),
        // `errors.NewInternalError(err)`.
        Err(e) => Err(Error::Internal(e.to_string())),
    }
}

/// Gates the `NamespaceLifecycle` middleware by `--enable-admission-plugins` /
/// `--disable-admission-plugins` (`plugins.enabled`), as upstream only runs the
/// plugin when it is in the enabled set.
///
/// With `NamespaceAutoProvision` on, a CREATE into a missing namespace skips
/// the middleware's NotFound: `AllOrderedPlugins` puts autoprovision ahead of
/// lifecycle (plugins.go:69-72), so the namespace is created by the chain
/// before lifecycle would look at it, and a fresh namespace is Active.
pub async fn namespace_lifecycle_gate(
    Extension(storage): Extension<Arc<StorageBackend>>,
    request: Request,
    next: Next,
) -> Response {
    if !enabled("NamespaceLifecycle") {
        return next.run(request).await;
    }
    let observed_op = lifecycle_operation(request.method(), request.uri().path());
    let started = std::time::Instant::now();
    let mut skipped = false;
    if enabled("NamespaceAutoProvision") && request.method() == axum::http::Method::POST {
        if let Some(ns) = rusternetes_middleware::create_target_namespace(request.uri().path()) {
            skipped = matches!(namespace_exists(&storage, ns).await, Ok(false));
        }
    }
    let rejection = if skipped {
        None
    } else {
        rusternetes_middleware::namespace_lifecycle_rejection(
            &storage,
            request.method(),
            request.uri().path(),
        )
        .await
    };
    // `WithControllerMetrics(NamespaceLifecycle)` (admission/metrics/
    // metrics.go:63-66, :89-100): the plugin's `Admit` is timed for every
    // CREATE/UPDATE/DELETE, whatever the resource (`NewHandler(Create,
    // Update, Delete)`, namespace/lifecycle/admission.go:196), and
    // `rejected` is `err != nil`.
    if let Some(op) = observed_op {
        rusternetes_admission_webhook::admission_metrics::observe_admission_controller(
            started.elapsed(),
            rejection.is_some(),
            &op,
            rusternetes_admission_webhook::admission_metrics::STEP_ADMIT,
            "NamespaceLifecycle",
        );
    }
    match rejection {
        Some(response) => response,
        None => next.run(request).await,
    }
}

/// The admission operation of a request as far as `NamespaceLifecycle` is
/// concerned: POST is CREATE, PUT/PATCH UPDATE and DELETE DELETE; a POST to a
/// connect subresource (`exec`, `attach`, `portforward`, `proxy`) is CONNECT,
/// which the plugin does not handle.
fn lifecycle_operation(method: &axum::http::Method, path: &str) -> Option<Operation> {
    use axum::http::Method;
    if *method == Method::DELETE {
        return Some(Operation::Delete);
    }
    if *method == Method::PUT || *method == Method::PATCH {
        return Some(Operation::Update);
    }
    if *method == Method::POST {
        let last = path.rsplit('/').next().unwrap_or_default();
        if matches!(last, "exec" | "attach" | "portforward") || path.contains("/proxy") {
            return None;
        }
        return Some(Operation::Create);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

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
