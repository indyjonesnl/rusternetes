//! PriorityLevelConfiguration strategy and storage — port of
//! `pkg/registry/flowcontrol/prioritylevelconfiguration/strategy.go` and
//! `pkg/registry/flowcontrol/prioritylevelconfiguration/storage/storage.go`.
//!
//! Not modelled: the mandatory-object spec equality checks of
//! `pkg/apis/flowcontrol/validation` (`internalbootstrap.Mandatory*`).

use std::sync::Arc;

use rusternetes_common::resources::{PriorityLevelConfiguration, PriorityLevelConfigurationStatus};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::validation::prioritylevelconfiguration::{
    validate_priority_level_configuration, validate_priority_level_configuration_status_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};

/// The v1 defaulting a decoded PriorityLevelConfiguration goes through:
/// `SetDefaults_{Exempt,Limited}PriorityLevelConfiguration` and
/// `SetDefaults_QueuingConfiguration` (pkg/apis/flowcontrol/v1/defaults.go).
pub fn convert_to_internal(plc: &mut PriorityLevelConfiguration) {
    if let Some(exempt) = plc.spec.exempt.as_mut() {
        exempt.nominal_concurrency_shares.get_or_insert(0);
        exempt.lendable_percent.get_or_insert(0);
    }
    if let Some(limited) = plc.spec.limited.as_mut() {
        limited.nominal_concurrency_shares.get_or_insert(30);
        limited.lendable_percent.get_or_insert(0);
        if let Some(queuing) = limited
            .limit_response
            .as_mut()
            .and_then(|lr| lr.queuing.as_mut())
        {
            if queuing.hand_size == 0 {
                queuing.hand_size = 8;
            }
            if queuing.queues == 0 {
                queuing.queues = 64;
            }
            if queuing.queue_length_limit == 0 {
                queuing.queue_length_limit = 50;
            }
        }
    }
}

/// A Go `flowcontrol.PriorityLevelConfigurationStatus{}`: the status is not a pointer.
fn empty_status() -> PriorityLevelConfigurationStatus {
    PriorityLevelConfigurationStatus { conditions: None }
}

fn spec_json(obj: &PriorityLevelConfiguration) -> Option<serde_json::Value> {
    serde_json::to_value(&obj.spec).ok()
}

/// `ValidatePriorityLevelConfiguration` (validation.go): ObjectMeta with `NameIsDNSSubdomain`,
/// then spec and status.
fn validate(obj: &PriorityLevelConfiguration) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_priority_level_configuration(obj));
    errs
}

/// `priorityLevelConfigurationStrategy` (strategy.go).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<PriorityLevelConfiguration> for Strategy {
    /// Clears the status and starts the generation at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut PriorityLevelConfiguration) {
        obj.status = Some(empty_status());
        obj.metadata.generation = Some(1);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &PriorityLevelConfiguration) -> ErrorList {
        validate(obj)
    }
}

impl RestUpdateStrategy<PriorityLevelConfiguration> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Spec updates bump the generation; the status is not writable here.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut PriorityLevelConfiguration,
        old: &PriorityLevelConfiguration,
    ) {
        if spec_json(obj) != spec_json(old) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
        obj.status = old.status.clone();
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &PriorityLevelConfiguration,
        _old: &PriorityLevelConfiguration,
    ) -> ErrorList {
        validate(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<PriorityLevelConfiguration> for Strategy {}

/// `priorityLevelConfigurationStatusStrategy`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<PriorityLevelConfiguration> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Status changes cannot update the spec or the metadata.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut PriorityLevelConfiguration,
        old: &PriorityLevelConfiguration,
    ) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &PriorityLevelConfiguration,
        _old: &PriorityLevelConfiguration,
    ) -> ErrorList {
        validate_priority_level_configuration_status_update(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `NewREST` (storage/storage.go): the main store and the status store.
pub fn new_stores(
    storage: Arc<StorageBackend>,
) -> (
    Store<PriorityLevelConfiguration, StorageBackend>,
    Store<PriorityLevelConfiguration, StorageBackend>,
) {
    let store = Store::new(
        storage,
        GroupResource::new(
            "flowcontrol.apiserver.k8s.io",
            "prioritylevelconfigurations",
        ),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    (store, status)
}
