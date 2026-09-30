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

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::NodeStatus;

    fn node(status: serde_json::Value) -> Node {
        let mut node: Node = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": "n", "resourceVersion": "1"},
            "spec": {"podCIDRs": ["10.0.0.0/24"]},
            "status": status
        }))
        .unwrap();
        convert_to_internal(&mut node);
        node
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(!Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert!(Strategy.default_garbage_collection_policy(&ctx()).is_none());
        assert!(!StatusStrategy.namespace_scoped());
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(StatusStrategy.allow_unconditional_update());
    }

    /// `SetDefaults_NodeStatus` (v1/defaults.go:346-354).
    #[test]
    fn allocatable_defaults_to_capacity() {
        let n = node(serde_json::json!({"capacity": {"cpu": "2"}}));
        assert_eq!(
            n.status.unwrap().allocatable,
            Some([("cpu".to_string(), "2".to_string())].into())
        );
    }

    /// `TestNodeStrategy` (strategy_test.go): create keeps status, but not
    /// `status.config` or, with its gate off, `status.declaredFeatures`.
    #[test]
    fn create_keeps_status_but_drops_disabled_fields() {
        let mut n = node(serde_json::json!({
            "capacity": {"cpu": "2"},
            "config": {"error": "x"},
            "declaredFeatures": ["A"]
        }));
        Strategy.prepare_for_create(&ctx(), &mut n);
        let status = n.status.as_ref().unwrap();
        assert_eq!(status.capacity.as_ref().unwrap()["cpu"], "2");
        assert!(status.config.is_none());
        assert!(status.declared_features.is_none());
        let errs = Strategy.validate(&ctx(), &n);
        assert!(errs.is_empty(), "{errs:?}");
    }

    #[test]
    fn update_keeps_status_and_status_update_keeps_spec() {
        let old = node(serde_json::json!({"capacity": {"cpu": "2"}}));
        let mut new = node(serde_json::json!({"capacity": {"cpu": "9"}}));
        new.spec.as_mut().unwrap().unschedulable = Some(true);
        Strategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(
            new.status.as_ref().unwrap().capacity,
            old.status.as_ref().unwrap().capacity
        );
        assert_eq!(new.spec.as_ref().unwrap().unschedulable, Some(true));

        let mut status =
            node(serde_json::json!({"capacity": {"cpu": "9"}, "config": {"error": "x"}}));
        status.spec.as_mut().unwrap().unschedulable = Some(true);
        StatusStrategy.prepare_for_update(&ctx(), &mut status, &old);
        assert_eq!(status.spec.as_ref().unwrap().unschedulable, None);
        let s: &NodeStatus = status.status.as_ref().unwrap();
        assert_eq!(s.capacity.as_ref().unwrap()["cpu"], "9");
        assert!(s.config.is_none(), "config was not in use");
    }

    /// `nodeWarnings` (strategy.go:306-311) with `GetWarningsForCIDR`.
    #[test]
    fn an_ambiguous_pod_cidr_warns() {
        let mut n = node(serde_json::json!({}));
        n.spec.as_mut().unwrap().pod_cidrs = Some(vec!["10.0.0.8/24".into()]);
        assert_eq!(
            Strategy.warnings_on_create(&ctx(), &n),
            vec![
                r#"spec.podCIDRs[0]: CIDR value "10.0.0.8/24" is ambiguous in this context (should be "10.0.0.0/24" or "10.0.0.8/32"?)"#
            ]
        );
    }
}
