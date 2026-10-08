//! The one place the api-server's authorizer chain is built.
//!
//! Ported from `pkg/kubeapiserver/authorizer/reload.go` `newForConfig`
//! (:97-99 privileged-groups authorizer first, then the configured modes in
//! order) with the default `--authorization-mode=Node,RBAC`
//! (`pkg/kubeapiserver/options/authorization.go` `NewBuiltInAuthorizationOptions`).
//! Both entry points (`main.rs` and `lib.rs::run`, the latter used by the
//! all-in-one binary) call [`build_authorizer`] so they cannot diverge (#2679).

use rusternetes_common::authz::{
    superuser_then, AlwaysAllowAuthorizer, Authorizer, AuthzStorage, RBACAuthorizer,
};
use std::sync::Arc;

/// Build the authorizer chain: `AlwaysAllow` when `skip_auth`, otherwise
/// `system:masters` superuser, then Node, then RBAC.
pub fn build_authorizer<S: AuthzStorage + 'static>(
    storage: Arc<S>,
    skip_auth: bool,
) -> Arc<dyn Authorizer> {
    if skip_auth {
        return Arc::new(AlwaysAllowAuthorizer);
    }
    let rbac: Arc<dyn Authorizer> = Arc::new(RBACAuthorizer::new(storage));
    Arc::new(superuser_then(vec![rbac]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::authz::{Decision, RequestAttributes};
    use rusternetes_common::auth::UserInfo;
    use rusternetes_storage::MemoryStorage;

    fn user(name: &str, groups: &[&str]) -> UserInfo {
        UserInfo {
            username: name.to_string(),
            uid: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            extra: Default::default(),
        }
    }

    async fn allowed(a: &Arc<dyn Authorizer>, attrs: RequestAttributes) -> bool {
        matches!(a.authorize(&attrs).await.unwrap(), Decision::Allow)
    }

    /// A kubelet reads its own Node through the Node authorizer even with an
    /// empty RBAC store (#1664; the all-in-one binary runs one in-process).
    #[tokio::test]
    async fn kubelet_is_authorized_by_node_authorizer_with_empty_rbac() {
        let a = build_authorizer(Arc::new(MemoryStorage::new()), false);
        let attrs =
            RequestAttributes::new(user("system:node:n1", &["system:nodes"]), "get", "nodes")
                .with_name("n1");
        assert!(allowed(&a, attrs).await);
    }

    #[tokio::test]
    async fn masters_allowed_and_unknown_user_denied() {
        let a = build_authorizer(Arc::new(MemoryStorage::new()), false);
        let pods = |u| RequestAttributes::new(u, "get", "pods").with_namespace("default");
        assert!(allowed(&a, pods(user("admin", &["system:masters"]))).await);
        assert!(!allowed(&a, pods(user("alice", &[]))).await);
    }

    #[tokio::test]
    async fn skip_auth_allows_everything() {
        let a = build_authorizer(Arc::new(MemoryStorage::new()), true);
        let attrs = RequestAttributes::new(user("alice", &[]), "delete", "nodes");
        assert!(allowed(&a, attrs).await);
    }
}
