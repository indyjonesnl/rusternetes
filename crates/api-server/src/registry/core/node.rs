//! Node strategies and storage — port of `pkg/registry/core/node/strategy.go`
//! and `pkg/registry/core/node/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::resources::Node;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::ipaddress::get_warnings_for_cidr;
use rusternetes_common::validation::node::{validate_node, validate_node_update};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};
use crate::ssa::ResetFields;

/// `SetDefaults_NodeStatus` (pkg/apis/core/v1/defaults.go:346-354): with no
/// allocatable, allocatable is capacity. `NodeStatus` is a struct upstream,
/// never absent.
pub fn convert_to_internal(node: &mut Node) {
    let status = node.status.get_or_insert_with(Default::default);
    if status.allocatable.is_none() && status.capacity.is_some() {
        status.allocatable = status.capacity.clone();
    }
}

/// `dropDisabledFields` (strategy.go:94-118). `spec.configSource` is not
/// modelled, so decoding already dropped it; RuntimeHandlers and Features are
/// behind gates that are on (RecursiveReadOnlyMounts and
/// SupplementalGroupsPolicy are GA and locked). `status.declaredFeatures` is
/// behind the alpha `NodeDeclaredFeatures` gate.
fn drop_disabled_fields(node: &mut Node, old: Option<&Node>) {
    let status = node.status.get_or_insert_with(Default::default);
    if old.is_none() {
        status.config = None;
    }
    let declared_in_use = old
        .and_then(|o| o.status.as_ref())
        .is_some_and(|s| s.declared_features.is_some());
    if !feature_gates::enabled(Feature::NodeDeclaredFeatures) && !declared_in_use {
        status.declared_features = None;
    }
}

/// `nodeWarnings` (strategy.go:295-314). `spec.configSource` and
/// `spec.externalID` are not modelled, so only the podCIDRs warnings apply.
fn node_warnings(node: &Node) -> Vec<String> {
    let cidrs_path = Path::new("spec").child("podCIDRs");
    node.spec
        .as_ref()
        .and_then(|s| s.pod_cidrs.as_ref())
        .into_iter()
        .flatten()
        .enumerate()
        .flat_map(|(i, value)| get_warnings_for_cidr(&cidrs_path.index(i), value))
        .collect()
}

/// `nodeStrategy` (strategy.go:47-55).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<Node> for Strategy {
    /// `PrepareForCreate` (strategy.go:79-83): nodes may set status on
    /// create, so only disabled fields are dropped.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut Node) {
        drop_disabled_fields(obj, None);
    }

    /// strategy.go:131-135.
    fn validate(&self, _ctx: &RequestContext, obj: &Node) -> ErrorList {
        validate_node(obj)
    }

    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &Node) -> Vec<String> {
        node_warnings(obj)
    }
}

impl RestUpdateStrategy<Node> for Strategy {
    /// strategy.go:74-77.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:85-92): status only changes through
    /// `/status`.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Node, old: &Node) {
        obj.status = old.status.clone();
        drop_disabled_fields(obj, Some(old));
    }

    /// strategy.go:146-150.
    fn validate_update(&self, _ctx: &RequestContext, obj: &Node, old: &Node) -> ErrorList {
        let mut errs = validate_node(obj);
        errs.extend(validate_node_update(obj, old));
        errs
    }

    fn warnings_on_update(&self, _ctx: &RequestContext, obj: &Node, _old: &Node) -> Vec<String> {
        node_warnings(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }

    /// `GetResetFields` (core/node/strategy.go:64-72): `status`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with("v1", &[&["status"]])
    }
}

/// Nodes use the default delete strategy.
impl RestDeleteStrategy<Node> for Strategy {}

/// `nodeStatusStrategy` (strategy.go:161-211): the update strategy of
/// `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<Node> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:179-187: only status may change, and `status.config` only
    /// if it was already set.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Node, old: &Node) {
        obj.spec = old.spec.clone();
        let config_in_use = old.status.as_ref().is_some_and(|s| s.config.is_some());
        if !config_in_use {
            if let Some(status) = obj.status.as_mut() {
                status.config = None;
            }
        }
    }

    /// strategy.go:200-202.
    fn validate_update(&self, _ctx: &RequestContext, obj: &Node, old: &Node) -> ErrorList {
        validate_node_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }

    /// `GetResetFields` (core/node/strategy.go:169-177): `spec`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new().with("v1", &[&["spec"]])
    }
}

/// `NewStorage` (storage/storage.go:96-121): the Node store. It does not set
/// `ReturnDeletedObject`, so a DELETE that removes the node returns a Status.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<Node, StorageBackend> {
    Store::new(storage, GroupResource::new("", "nodes"), Arc::new(Strategy))
        .with_decode_defaulter(convert_to_internal)
}

/// The `/status` store: the Node store updating with [`StatusStrategy`]
/// (storage.go:119-121).
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<Node, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}
