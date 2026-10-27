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
    /// `GetResetFields` (pkg/registry/certificates/podcertificaterequest/strategy.go:120-127):
    /// `spec`. Only the status strategy defines it; `Strategy` does not.
    fn get_reset_fields(&self) -> crate::ssa::ResetFields {
        crate::ssa::ResetFields::new().with("certificates.k8s.io/v1alpha1", &[&["spec"]])
    }

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
    fn reset_fields_only_on_status_strategy() {
        use crate::ssa::ResetFields;
        assert_eq!(
            RestUpdateStrategy::<PodCertificateRequest>::get_reset_fields(&StatusStrategy),
            ResetFields::new().with("certificates.k8s.io/v1alpha1", &[&["spec"]])
        );
        assert_eq!(
            RestUpdateStrategy::<PodCertificateRequest>::get_reset_fields(&Strategy),
            ResetFields::new()
        );
    }

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

    use chrono::TimeZone;
    use rusternetes_common::resources::podcertificaterequest::{
        PodCertificateRequestSpec, PodCertificateRequestStatus,
    };
    use rusternetes_common::types::Condition;

    fn cond(ty: &str, status: &str) -> Condition {
        Condition {
            condition_type: ty.into(),
            status: status.into(),
            observed_generation: None,
            last_transition_time: None,
            reason: Some("Foo".into()),
            message: Some("Foo message".into()),
        }
    }

    /// The spec every case in strategy_test.go builds (`foo/bar`, `pod-1`, an
    /// Ed25519 key); the key bytes are irrelevant to the strategy.
    fn base() -> PodCertificateRequest {
        let mut r = PodCertificateRequest::default();
        r.metadata.namespace = Some("foo".into());
        r.metadata.name = "bar".into();
        r.spec = PodCertificateRequestSpec {
            signer_name: "foo.com/abc".into(),
            pod_name: "pod-1".into(),
            pod_uid: "pod-1-uid".into(),
            service_account_name: "sa-1".into(),
            service_account_uid: "sa-uid-1".into(),
            node_name: "node-1".into(),
            node_uid: "node-uid-1".into(),
            max_expiration_seconds: Some(86400),
            pkix_public_key: vec![1, 2, 3],
            proof_of_possession: vec![4, 5, 6],
            unverified_user_annotations: None,
        };
        r
    }

    fn with_status(mut r: PodCertificateRequest, c: Condition) -> PodCertificateRequest {
        r.status = PodCertificateRequestStatus {
            conditions: vec![c],
            ..Default::default()
        };
        r
    }

    /// strategy_test.go `TestPrepareForCreate`: PrepareForCreate stomps any
    /// existing status, compared as whole objects.
    #[test]
    fn upstream_prepare_for_create_stomps_status() {
        let ctx = RequestContext::new(Some("foo"));
        let mut processed = with_status(base(), cond("Denied", "False"));
        RestCreateStrategy::prepare_for_create(&Strategy, &ctx, &mut processed);
        assert_eq!(processed, base());
    }

    /// `TestPrepareForUpdate`: PrepareForUpdate keeps the OLD status.
    #[test]
    fn upstream_prepare_for_update_keeps_old_status() {
        let ctx = RequestContext::new(Some("foo"));
        let old = with_status(base(), cond("Denied", "True"));
        let mut processed = with_status(base(), cond("Failed", "True"));
        RestUpdateStrategy::prepare_for_update(&Strategy, &ctx, &mut processed, &old);
        assert_eq!(processed, with_status(base(), cond("Denied", "True")));
    }

    /// `TestStatusPrepareForUpdate`: the status strategy resets every spec
    /// field, keeps the new status, and (`metav1.ResetObjectMetaForStatus`)
    /// takes labels/finalizers/etc. from the old object.
    #[test]
    fn upstream_status_prepare_for_update_resets_spec_and_metadata() {
        let ctx = RequestContext::new(Some("foo"));
        let mut old = with_status(base(), cond("Denied", "True"));
        old.metadata.labels = Some([("a".to_string(), "b".to_string())].into());
        old.metadata.finalizers = Some(vec!["f".into()]);

        let mut processed = with_status(base(), cond("Failed", "True"));
        processed.spec.signer_name = "foo.com/different-value".into();
        processed.metadata.labels = Some([("x".to_string(), "y".to_string())].into());
        processed.metadata.finalizers = None;

        RestUpdateStrategy::prepare_for_update(&StatusStrategy, &ctx, &mut processed, &old);

        let mut want = with_status(base(), cond("Failed", "True"));
        want.metadata.labels = old.metadata.labels.clone();
        want.metadata.finalizers = old.metadata.finalizers.clone();
        assert_eq!(processed, want);
    }

    /// `TestStatusValidateUpdate`, first case: with the status strategy's own
    /// validation the update of an unissued request to `Failed` has no
    /// errors. (The second case, the `"sign"` Forbidden for an unauthorized
    /// caller, is enforced in
    /// `admission::certificates::validate_pod_certificate_request_sign` and
    /// covered by the `csr_signer_authz_test` integration test.)
    #[test]
    fn upstream_status_validate_update_accepts_a_failed_condition() {
        let ctx = RequestContext::new(Some("foo"));
        let mut old = base();
        old.metadata.resource_version = Some("1".into());
        let mut failed = cond("Failed", "True");
        failed.last_transition_time = Some(
            chrono::Utc
                .timestamp_opt(Utc::now().timestamp(), 0)
                .unwrap(),
        );
        let mut new = with_status(old.clone(), failed);
        new.metadata.resource_version = Some("1".into());
        let errs = RestUpdateStrategy::validate_update(&StatusStrategy, &ctx, &new, &old);
        assert!(errs.is_empty(), "{errs:?}");
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
