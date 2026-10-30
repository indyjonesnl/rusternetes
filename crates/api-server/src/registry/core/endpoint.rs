//! Endpoints strategy and storage — port of
//! `pkg/registry/core/endpoint/strategy.go` and
//! `pkg/registry/core/endpoint/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::resources::Endpoints;
use rusternetes_common::validation::endpoints::{
    validate_endpoints_create, validate_endpoints_update,
};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::metav1::get_warnings_for_ip;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `endpointscontroller.LabelManagedBy` and `ControllerName`
/// (pkg/controller/endpoint/endpoints_controller.go:73, :76).
pub const LABEL_MANAGED_BY: &str = "endpoints.kubernetes.io/managed-by";
pub const CONTROLLER_NAME: &str = "endpoint-controller";

/// `SetDefaults_Endpoints` (pkg/apis/core/v1/defaults.go:305-315): a port
/// with no protocol is TCP.
pub fn convert_to_internal(endpoints: &mut Endpoints) {
    for subset in &mut endpoints.subsets {
        for port in subset.ports.iter_mut().flatten() {
            if port.protocol.is_empty() {
                port.protocol = "TCP".to_string();
            }
        }
    }
}

/// `endpointsWarnings` (strategy.go:88-106): non-standard IPs draw a warning,
/// except on Endpoints the endpoints controller manages.
fn endpoints_warnings(endpoints: &Endpoints) -> Vec<String> {
    let managed = endpoints
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(LABEL_MANAGED_BY))
        .is_some_and(|v| v == CONTROLLER_NAME);
    if managed {
        return Vec::new();
    }
    let mut warnings = Vec::new();
    for (i, subset) in endpoints.subsets.iter().enumerate() {
        for (j, address) in subset.addresses.iter().flatten().enumerate() {
            let path = format!("subsets[{i}].addresses[{j}].ip");
            warnings.extend(get_warnings_for_ip(&path, &address.ip));
        }
        for (j, address) in subset.not_ready_addresses.iter().flatten().enumerate() {
            let path = format!("subsets[{i}].notReadyAddresses[{j}].ip");
            warnings.extend(get_warnings_for_ip(&path, &address.ip));
        }
    }
    warnings
}

/// `endpointsStrategy` (strategy.go:33-40).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<Endpoints> for Strategy {
    /// strategy.go:48-50: nothing to prepare.
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut Endpoints) {}

    fn validate(&self, _ctx: &RequestContext, obj: &Endpoints) -> ErrorList {
        validate_endpoints_create(obj)
    }

    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &Endpoints) -> Vec<String> {
        endpoints_warnings(obj)
    }
}

impl RestUpdateStrategy<Endpoints> for Strategy {
    /// strategy.go:70-73: a PUT may create Endpoints.
    fn allow_create_on_update(&self) -> bool {
        true
    }

    /// strategy.go:52-54: nothing to prepare.
    fn prepare_for_update(&self, _ctx: &RequestContext, _obj: &mut Endpoints, _old: &Endpoints) {}

    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &Endpoints,
        old: &Endpoints,
    ) -> ErrorList {
        validate_endpoints_update(obj, old)
    }

    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &Endpoints,
        _old: &Endpoints,
    ) -> Vec<String> {
        endpoints_warnings(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// Endpoints use the default delete strategy.
impl RestDeleteStrategy<Endpoints> for Strategy {}

/// `NewREST` (storage/storage.go:37-56).
pub fn new_store(storage: Arc<StorageBackend>) -> Store<Endpoints, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("", "endpoints"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}
