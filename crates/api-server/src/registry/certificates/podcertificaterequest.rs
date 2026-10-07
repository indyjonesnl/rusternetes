//! PodCertificateRequest strategy and storage — port of
//! `pkg/registry/certificates/podcertificaterequest/strategy.go` (`Strategy`
//! :43-101, `StatusStrategy` :104-175) and `storage/storage.go`.
//!
//! Not in the strategy: `StatusStrategy.ValidateUpdate`'s `"sign"` check on
//! `spec.signerName` (strategy.go:153-167). A [`RestUpdateStrategy`] is
//! synchronous and the authorizer is async, so the check lives in the
//! validating-admission step, next to the CSR `signing` plugin
//! ([`crate::admission::certificates::validate_pod_certificate_request_sign`]).

use std::sync::Arc;

use chrono::Utc;
use rusternetes_common::resources::podcertificaterequest::PodCertificateRequest;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::podcertificaterequest::{
    validate_pod_certificate_request_create, validate_pod_certificate_request_status_update,
    validate_pod_certificate_request_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};

/// `Strategy` (strategy.go:43-50), with `names.SimpleNameGenerator`.
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:59-61.
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<PodCertificateRequest> for Strategy {
    /// `PrepareForCreate` (strategy.go:63-66): a client cannot create a
    /// request with a status.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut PodCertificateRequest) {
        obj.status = Default::default();
    }

    /// `Validate` (strategy.go:68-71).
    fn validate(&self, _ctx: &RequestContext, obj: &PodCertificateRequest) -> ErrorList {
        validate_pod_certificate_request_create(obj)
    }
}

impl RestUpdateStrategy<PodCertificateRequest> for Strategy {
    /// strategy.go:79-81.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:83-87): status changes only through
    /// the status subresource.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut PodCertificateRequest,
        old: &PodCertificateRequest,
    ) {
        obj.status = old.status.clone();
    }

    /// `ValidateUpdate` (strategy.go:89-93).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &PodCertificateRequest,
        old: &PodCertificateRequest,
    ) -> ErrorList {
        validate_pod_certificate_request_update(obj, old)
    }

    /// strategy.go:99-101.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<PodCertificateRequest> for Strategy {}

/// `StatusStrategy` (strategy.go:104-118): embeds [`Strategy`].
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<PodCertificateRequest> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `StatusStrategy.PrepareForUpdate` (strategy.go:129-137): `/status`
    /// does not modify the spec, and only the status part of the metadata.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut PodCertificateRequest,
        old: &PodCertificateRequest,
    ) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
    }

    /// `StatusStrategy.ValidateUpdate` (strategy.go:139-151) up to the
    /// `"sign"` authorization, with the real clock (`clock.RealClock`).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &PodCertificateRequest,
        old: &PodCertificateRequest,
    ) -> ErrorList {
        validate_pod_certificate_request_status_update(obj, old, Utc::now())
    }

    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// `NewREST` (storage/storage.go): the main and status stores.
pub fn new_stores(
    storage: Arc<StorageBackend>,
) -> (
    Store<PodCertificateRequest, StorageBackend>,
    Store<PodCertificateRequest, StorageBackend>,
) {
    let store = Store::new(
        storage,
        GroupResource::new("certificates.k8s.io", "podcertificaterequests"),
        Arc::new(Strategy),
    );
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    (store, status)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// strategy_test.go: create wipes status; update keeps the old status;
    /// the status strategy keeps the old spec.
    #[test]
    fn prepare_for_create_and_update() {
        let ctx = RequestContext::new(Some("ns"));
        let mut obj = PodCertificateRequest::default();
        obj.status.certificate_chain = "x".into();
        RestCreateStrategy::prepare_for_create(&Strategy, &ctx, &mut obj);
        assert!(obj.status.certificate_chain.is_empty());

        let mut old = PodCertificateRequest::default();
        old.status.certificate_chain = "old".into();
        let mut new = PodCertificateRequest::default();
        new.status.certificate_chain = "new".into();
        new.spec.node_name = "n".into();
        RestUpdateStrategy::prepare_for_update(&Strategy, &ctx, &mut new, &old);
        assert_eq!(new.status.certificate_chain, "old");
        assert_eq!(new.spec.node_name, "n");

        let mut old = PodCertificateRequest::default();
        old.spec.node_name = "old".into();
        let mut new = PodCertificateRequest::default();
        new.spec.node_name = "new".into();
        RestUpdateStrategy::prepare_for_update(&StatusStrategy, &ctx, &mut new, &old);
        assert_eq!(new.spec.node_name, "old");
    }

    #[test]
    fn strategy_flags() {
        assert!(NamespaceScopedStrategy::namespace_scoped(&Strategy));
        assert!(!RestUpdateStrategy::<PodCertificateRequest>::allow_create_on_update(&Strategy));
        assert!(
            !RestUpdateStrategy::<PodCertificateRequest>::allow_unconditional_update(&Strategy)
        );
        assert!(
            !RestUpdateStrategy::<PodCertificateRequest>::allow_unconditional_update(
                &StatusStrategy
            )
        );
    }
}
