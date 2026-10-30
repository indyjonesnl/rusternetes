//! APIService strategies and storage -- port of
//! `staging/src/k8s.io/kube-aggregator/pkg/registry/apiservice/strategy.go`
//! and `.../apiservice/etcd/etcd.go`, with the validation of
//! `pkg/apis/apiregistration/validation/validation.go` (in
//! `rusternetes_common::validation::apiservice`).
//!
//! Not modelled: `GetResetFields` (managed-fields reset sets), `WarningsOn*`
//! (both return nil upstream), and `MatchAPIService` /
//! `ToSelectableFields`, which are the generic `metadata.name` field selector
//! the list handler already serves.
//!
//! `REST.Categories` (etcd.go:75-78) is served by `rusternetes_discovery`
//! (`api-extensions`) and `REST.ConvertToTable` (etcd.go:82-120) by
//! `rusternetes_middleware::table::printer_columns` / `printer_row_cells`.

use std::sync::Arc;

use rusternetes_common::resources::apiregistration::{
    new_local_available_condition, set_api_service_condition, set_defaults_api_service,
};
use rusternetes_common::resources::APIService;
use rusternetes_common::validation::apiservice::{
    validate_api_service, validate_api_service_status_update, validate_api_service_update,
};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `SetDefaults_ServiceReference` (v1/defaults.go), run by the codec on every
/// decode.
pub fn convert_to_internal(api_service: &mut APIService) {
    set_defaults_api_service(api_service);
}

/// `apiServerStrategy` (strategy.go:38-47).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:49-51.
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<APIService> for Strategy {
    /// `PrepareForCreate` (strategy.go:68-76): the status is cleared, and a
    /// local APIService (no `spec.service`) is available immediately. A remote
    /// one gets its first condition from the availability controller.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut APIService) {
        obj.status = Default::default();
        if obj.spec.service.is_none() {
            set_api_service_condition(obj, new_local_available_condition());
        }
    }

    /// `Validate` (strategy.go:85-87).
    fn validate(&self, _ctx: &RequestContext, obj: &APIService) -> ErrorList {
        validate_api_service(obj)
    }
}

impl RestUpdateStrategy<APIService> for Strategy {
    /// `GetResetFields` (staging/src/k8s.io/kube-aggregator/pkg/registry/apiservice/strategy.go:55): status.
    fn get_reset_fields(&self) -> crate::ssa::ResetFields {
        crate::ssa::ResetFields::new()
            .with("apiregistration.k8s.io/v1", &[&["status"]])
            .with("apiregistration.k8s.io/v1beta1", &[&["status"]])
    }

    /// strategy.go:94-96.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:78-83): the status is the stored one.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut APIService, old: &APIService) {
        obj.status = old.status.clone();
    }

    /// `ValidateUpdate` (strategy.go:103-105).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &APIService,
        old: &APIService,
    ) -> ErrorList {
        validate_api_service_update(obj, old)
    }

    /// strategy.go:98-100.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// APIServices use the default delete strategy (`DeleteStrategy: strategy`
/// implements no `GarbageCollectionDeleteStrategy`).
impl RestDeleteStrategy<APIService> for Strategy {}

/// `apiServerStatusStrategy` (strategy.go:118-166): the update strategy of
/// `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<APIService> for StatusStrategy {
    /// `GetResetFields` (staging/src/k8s.io/kube-aggregator/pkg/registry/apiservice/strategy.go:127): spec, metadata.
    fn get_reset_fields(&self) -> crate::ssa::ResetFields {
        crate::ssa::ResetFields::new()
            .with("apiregistration.k8s.io/v1", &[&["spec"], &["metadata"]])
            .with(
                "apiregistration.k8s.io/v1beta1",
                &[&["spec"], &["metadata"]],
            )
    }

    /// strategy.go:151-153.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:143-149): only the status may change.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut APIService, old: &APIService) {
        obj.spec = old.spec.clone();
        obj.metadata.labels = old.metadata.labels.clone();
        obj.metadata.annotations = old.metadata.annotations.clone();
        obj.metadata.finalizers = old.metadata.finalizers.clone();
        obj.metadata.owner_references = old.metadata.owner_references.clone();
    }

    /// `ValidateUpdate` (strategy.go:161-163).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &APIService,
        old: &APIService,
    ) -> ErrorList {
        validate_api_service_status_update(obj, old)
    }

    /// strategy.go:155-157.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// `NewREST`'s store (etcd.go:41-62).
pub fn new_store(storage: Arc<StorageBackend>) -> Store<APIService, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("apiregistration.k8s.io", "apiservices"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}

/// The `/status` store (etcd.go:117-127): `UpdateStrategy` swapped, create and
/// delete strategies dropped.
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<APIService, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}
