//! EndpointSlice strategy and storage — port of
//! `pkg/registry/discovery/endpointslice/strategy.go` and
//! `pkg/registry/discovery/endpointslice/storage/storage.go`.
//!
//! The feature gates the strategy consults (`TopologyAwareHints` and
//! `PreferSameTrafficDistribution`) are GA and locked on in 1.35, so
//! `dropDisabledFieldsOnCreate` / `dropDisabledFieldsOnUpdate` never clear
//! anything and are not modelled. Only `discovery.k8s.io/v1` is served, so
//! `dropTopologyOnV1` always applies.

use std::collections::HashSet;
use std::sync::Arc;

use rusternetes_common::resources::endpointslice::EndpointSlice;
use rusternetes_common::validation::endpointslice::{
    validate_endpoint_slice, validate_endpoint_slice_update,
};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::metav1::get_warnings_for_ip;
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

const LABEL_MANAGED_BY: &str = "endpointslice.kubernetes.io/managed-by";
/// `endpointslicecontroller.ControllerName` and
/// `endpointslicemirroringcontroller.ControllerName`.
const CONTROLLER_NAMES: [&str; 2] = [
    "endpointslice-controller.k8s.io",
    "endpointslicemirroring-controller.k8s.io",
];
/// `corev1.LabelHostname`.
const LABEL_HOSTNAME: &str = "kubernetes.io/hostname";

/// The v1 defaulting a decoded EndpointSlice goes through:
/// `SetDefaults_EndpointPort` (pkg/apis/discovery/v1/defaults.go) — a port
/// without a protocol is TCP.
pub fn convert_to_internal(slice: &mut EndpointSlice) {
    for port in &mut slice.ports {
        if port.protocol.is_empty() {
            port.protocol = "TCP".to_string();
        }
    }
}

/// `ValidateEndpointSlice`'s ObjectMeta part: namespaced, `NameIsDNSSubdomain`.
fn validate_meta(slice: &EndpointSlice) -> ErrorList {
    validate_object_meta(
        &slice.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    )
}

fn deprecated_topology_node_names(slice: Option<&EndpointSlice>) -> HashSet<String> {
    slice
        .into_iter()
        .flat_map(|s| s.endpoints.iter())
        .filter_map(|ep| ep.deprecated_topology.as_ref()?.get(LABEL_HOSTNAME))
        .filter(|n| !n.is_empty())
        .cloned()
        .collect()
}

/// `dropTopologyOnV1` (strategy.go:182-218): writes to `deprecatedTopology`
/// are dropped. On an update that changed the endpoints, a node name that the
/// old slice carried there is kept by copying it into `nodeName`.
fn drop_topology_on_v1(old: Option<&EndpointSlice>, new: &mut EndpointSlice) {
    if old.is_some_and(|o| o.endpoints == new.endpoints) {
        return;
    }
    let prev_node_names = deprecated_topology_node_names(old);
    for ep in &mut new.endpoints {
        let topology_node = ep
            .deprecated_topology
            .as_ref()
            .and_then(|t| t.get(LABEL_HOSTNAME))
            .cloned();
        if let Some(node) = topology_node {
            if ep.node_name.is_none()
                && prev_node_names.contains(&node)
                && rusternetes_common::validation::objectmeta::name_is_dns_subdomain(&node, false)
                    .is_empty()
            {
                ep.node_name = Some(node);
            }
        }
        ep.deprecated_topology = None;
    }
}

/// `warnOnBadIPs` (strategy.go:262-282): skipped for the slices our own
/// controllers write, which fix up their IPs.
fn warn_on_bad_ips(slice: &EndpointSlice) -> Vec<String> {
    let managed_by = slice
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(LABEL_MANAGED_BY));
    if managed_by.is_some_and(|m| CONTROLLER_NAMES.contains(&m.as_str())) {
        return Vec::new();
    }
    let mut warnings = Vec::new();
    for (i, ep) in slice.endpoints.iter().enumerate() {
        for (j, addr) in ep.addresses.iter().enumerate() {
            let path = Path::new("endpoints").index(i).child("addresses").index(j);
            warnings.extend(get_warnings_for_ip(&path.to_string(), addr));
        }
    }
    warnings
}

/// `warnOnDeprecatedAddressType` (strategy.go:250-256).
fn warn_on_deprecated_address_type(slice: &EndpointSlice) -> Vec<String> {
    if slice.address_type == "FQDN" {
        return vec!["spec.addressType: FQDN endpoints are deprecated".to_string()];
    }
    Vec::new()
}

/// `endpointSliceStrategy` (strategy.go:46-60).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<EndpointSlice> for Strategy {
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut EndpointSlice) {
        obj.metadata.generation = Some(1);
        drop_topology_on_v1(None, obj);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &EndpointSlice) -> ErrorList {
        let mut errs = validate_meta(obj);
        errs.extend(validate_endpoint_slice(obj));
        errs
    }

    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &EndpointSlice) -> Vec<String> {
        let mut warnings = warn_on_deprecated_address_type(obj);
        warnings.extend(warn_on_bad_ips(obj));
        warnings
    }
}

/// Everything but the metadata, as JSON: what `PrepareForUpdate` compares
/// to decide whether the generation moves.
fn content(slice: &EndpointSlice) -> Option<serde_json::Value> {
    let mut value = serde_json::to_value(slice).ok()?;
    value.as_object_mut()?.remove("metadata");
    Some(value)
}

impl RestUpdateStrategy<EndpointSlice> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// The generation increments when anything other than the metadata
    /// changed, or the labels did (strategy.go:71-95).
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut EndpointSlice,
        old: &EndpointSlice,
    ) {
        if content(obj) != content(old) || obj.metadata.labels != old.metadata.labels {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
        drop_topology_on_v1(Some(old), obj);
    }

    /// `ValidateEndpointSliceUpdate`, with the ObjectMeta checks around it.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &EndpointSlice,
        old: &EndpointSlice,
    ) -> ErrorList {
        let mut errs = validate_meta(obj);
        errs.extend(validate_endpoint_slice_update(obj, old));
        errs
    }

    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &EndpointSlice,
        _old: &EndpointSlice,
    ) -> Vec<String> {
        warn_on_bad_ips(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<EndpointSlice> for Strategy {}

/// `NewREST` (storage/storage.go:36-55).
pub fn new_store(storage: Arc<StorageBackend>) -> Store<EndpointSlice, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("discovery.k8s.io", "endpointslices"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}
