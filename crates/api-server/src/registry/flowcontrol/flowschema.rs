//! FlowSchema strategy and storage — port of
//! `pkg/registry/flowcontrol/flowschema/strategy.go` and
//! `pkg/registry/flowcontrol/flowschema/storage/storage.go`.
//!
//! Not modelled: the mandatory-object spec equality checks of
//! `pkg/apis/flowcontrol/validation` (`internalbootstrap.Mandatory*`).

use std::sync::Arc;

use rusternetes_common::resources::{FlowSchema, FlowSchemaStatus};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::flowschema::{
    validate_flow_schema, validate_flow_schema_status_update,
};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};

/// `SetDefaults_FlowSchemaSpec` (pkg/apis/flowcontrol/v1/defaults.go:31-35).
pub fn convert_to_internal(fs: &mut FlowSchema) {
    if fs.spec.matching_precedence == 0 {
        fs.spec.matching_precedence = 1000;
    }
}

/// A Go `flowcontrol.FlowSchemaStatus{}`: the status is not a pointer.
fn empty_status() -> FlowSchemaStatus {
    FlowSchemaStatus { conditions: None }
}

fn spec_json(obj: &FlowSchema) -> Option<serde_json::Value> {
    serde_json::to_value(&obj.spec).ok()
}

/// `ValidateFlowSchema` (validation.go): ObjectMeta with `NameIsDNSSubdomain`,
/// then spec and status.
fn validate(obj: &FlowSchema) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_flow_schema(obj));
    errs
}

/// `flowSchemaStrategy` (strategy.go).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<FlowSchema> for Strategy {
    /// Clears the status and starts the generation at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut FlowSchema) {
        obj.status = Some(empty_status());
        obj.metadata.generation = Some(1);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &FlowSchema) -> ErrorList {
        validate(obj)
    }
}

impl RestUpdateStrategy<FlowSchema> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Spec updates bump the generation; the status is not writable here.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut FlowSchema, old: &FlowSchema) {
        if spec_json(obj) != spec_json(old) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
        obj.status = old.status.clone();
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &FlowSchema,
        _old: &FlowSchema,
    ) -> ErrorList {
        validate(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<FlowSchema> for Strategy {}

/// `flowSchemaStatusStrategy`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<FlowSchema> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Status changes cannot update the spec or the metadata.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut FlowSchema, old: &FlowSchema) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
    }

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &FlowSchema,
        _old: &FlowSchema,
    ) -> ErrorList {
        validate_flow_schema_status_update(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `NewREST` (storage/storage.go): the main store and the status store.
pub fn new_stores(
    storage: Arc<StorageBackend>,
) -> (
    Store<FlowSchema, StorageBackend>,
    Store<FlowSchema, StorageBackend>,
) {
    let store = Store::new(
        storage,
        GroupResource::new("flowcontrol.apiserver.k8s.io", "flowschemas"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    (store, status)
}
