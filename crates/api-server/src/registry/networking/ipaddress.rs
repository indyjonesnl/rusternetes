//! Port of pkg/registry/networking/ipaddress/strategy.go and storage/storage.go.
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};
use rusternetes_common::resources::IPAddress;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::ipaddress::{validate_ip_address, validate_ip_address_update};
use rusternetes_storage::StorageBackend;
use std::sync::Arc;

pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:54-56: all IPAddresses are cluster scoped.
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<IPAddress> for Strategy {
    /// `noopNameGenerator` (strategy.go:38-42): IPAddress does not generate
    /// names, it returns the base.
    fn generate_name(&self, base: &str) -> String {
        base.to_string()
    }

    /// PrepareForCreate (strategy.go:59-62) changes nothing.
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut IPAddress) {}

    /// Validate (strategy.go:73-77).
    fn validate(&self, _ctx: &RequestContext, obj: &IPAddress) -> ErrorList {
        validate_ip_address(obj)
    }
}

impl RestUpdateStrategy<IPAddress> for Strategy {
    /// strategy.go:84-86: POST is needed to create one.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// PrepareForUpdate (strategy.go:65-70) changes nothing.
    fn prepare_for_update(&self, _ctx: &RequestContext, _obj: &mut IPAddress, _old: &IPAddress) {}

    /// ValidateUpdate (strategy.go:89-95): `ValidateIPAddress` then
    /// `ValidateIPAddressUpdate`.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &IPAddress,
        old: &IPAddress,
    ) -> ErrorList {
        let mut errs = validate_ip_address(obj);
        errs.extend(validate_ip_address_update(obj, old));
        errs
    }

    /// strategy.go:98-100.
    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<IPAddress> for Strategy {}

/// NewREST (storage/storage.go:37-55).
pub fn new_store(storage: Arc<StorageBackend>) -> Store<IPAddress, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("networking.k8s.io", "ipaddresses"),
        Arc::new(Strategy),
    )
}

/// The api-server's loopback client for IPAddresses
/// (`kubernetes.NewForConfig(c.LoopbackClientConfig)`,
/// pkg/registry/core/rest/storage_core.go:358, :430): a request as
/// `system:apiserver` that runs the Store (strategy, validation) and the
/// admission chain, as `create.go` / `delete.go` do, minus authorization,
/// which the privileged loopback user passes.
pub struct Loopback {
    state: std::sync::Weak<crate::state::ApiServerState>,
}

impl Loopback {
    pub fn new(state: &Arc<crate::state::ApiServerState>) -> Self {
        Self {
            state: Arc::downgrade(state),
        }
    }

    fn user() -> rusternetes_common::auth::UserInfo {
        rusternetes_common::auth::UserInfo {
            username: "system:apiserver".to_string(),
            uid: String::new(),
            groups: vec!["system:masters".to_string()],
            extra: Default::default(),
        }
    }
}

fn gvk() -> rusternetes_common::admission::GroupVersionKind {
    rusternetes_common::admission::GroupVersionKind {
        group: "networking.k8s.io".to_string(),
        version: "v1".to_string(),
        kind: "IPAddress".to_string(),
    }
}

fn gvr() -> rusternetes_common::admission::GroupVersionResource {
    rusternetes_common::admission::GroupVersionResource {
        group: "networking.k8s.io".to_string(),
        version: "v1".to_string(),
        resource: "ipaddresses".to_string(),
    }
}

#[async_trait::async_trait]
impl crate::registry::core::service::ipallocator::IpAddressClient for Loopback {
    async fn create(&self, ip: IPAddress) -> rusternetes_common::Result<()> {
        use crate::endpoints::handlers::admission::{Admission, CreateValidation};
        use rusternetes_common::admission::Operation;
        let state = self.state.upgrade().ok_or_else(|| {
            rusternetes_common::Error::Internal("api-server state is gone".to_string())
        })?;
        let (kind, resource, user) = (gvk(), gvr(), Self::user());
        let admission = Admission {
            state: &state,
            kind: &kind,
            resource: &resource,
            subresource: None,
            namespace: None,
            user: &user,
            dry_run: false,
        };
        let ctx = RequestContext::new(None)
            .with_user(&user)
            .with_group_version(&kind.group, &kind.version);
        let ip = admission.admit(Operation::Create, ip, None).await?;
        let validation = CreateValidation {
            admission: &admission,
            authorize_create: false,
        };
        new_store(state.storage.clone())
            .create(
                &ctx,
                ip,
                Some(&validation),
                &crate::registry::generic::CreateOptions::default(),
            )
            .await
            .map(|_| ())
    }

    async fn delete(&self, name: &str) -> rusternetes_common::Result<()> {
        use crate::endpoints::handlers::admission::{Admission, DeleteValidation};
        let state = self.state.upgrade().ok_or_else(|| {
            rusternetes_common::Error::Internal("api-server state is gone".to_string())
        })?;
        let (kind, resource, user) = (gvk(), gvr(), Self::user());
        let admission = Admission {
            state: &state,
            kind: &kind,
            resource: &resource,
            subresource: None,
            namespace: None,
            user: &user,
            dry_run: false,
        };
        let ctx = RequestContext::new(None).with_group_version(&kind.group, &kind.version);
        let validation = DeleteValidation {
            admission: &admission,
        };
        new_store(state.storage.clone())
            .delete(
                &ctx,
                name,
                Some(&validation),
                crate::registry::rest::zero_delete_options(),
            )
            .await
            .map(|_| ())
    }
}
