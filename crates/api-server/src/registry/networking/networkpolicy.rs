//! Port of pkg/registry/networking/networkpolicy/strategy.go and storage/storage.go.
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};
use rusternetes_common::resources::IPBlock;
use rusternetes_common::resources::NetworkPolicy;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::networkpolicy::{
    validate_network_policy, validate_network_policy_update,
};
use rusternetes_common::validation::{field::Path, ipaddress::get_warnings_for_cidr};
use rusternetes_storage::StorageBackend;
use std::sync::Arc;
/// SetDefaults_NetworkPolicy (pkg/apis/networking/v1/defaults.go:38-45).
/// SetDefaults_NetworkPolicyPort (:30-35) defaults nil protocol pointers only.
pub fn convert_to_internal(obj: &mut NetworkPolicy) {
    for rule in obj.spec.ingress.iter_mut().flatten() {
        for port in rule.ports.iter_mut().flatten() {
            if port.protocol.is_none() {
                port.protocol = Some("TCP".to_string());
            }
        }
    }
    for rule in obj.spec.egress.iter_mut().flatten() {
        for port in rule.ports.iter_mut().flatten() {
            if port.protocol.is_none() {
                port.protocol = Some("TCP".to_string());
            }
        }
    }

    if obj
        .spec
        .policy_types
        .as_ref()
        .is_none_or(|types| types.is_empty())
    {
        let mut types = vec!["Ingress".to_string()];
        if obj
            .spec
            .egress
            .as_ref()
            .is_some_and(|rules| !rules.is_empty())
        {
            types.push("Egress".to_string());
        }
        obj.spec.policy_types = Some(types);
    }
}
pub struct Strategy;
impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}
impl RestCreateStrategy<NetworkPolicy> for Strategy {
    /// PrepareForCreate (pkg/registry/networking/networkpolicy/strategy.go:47-50).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut NetworkPolicy) {
        obj.metadata.generation = Some(1);
    }
    fn validate(&self, _ctx: &RequestContext, obj: &NetworkPolicy) -> ErrorList {
        validate_network_policy(obj)
    }
    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &NetworkPolicy) -> Vec<String> {
        warnings(obj)
    }
}
impl RestUpdateStrategy<NetworkPolicy> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }
    /// PrepareForUpdate (pkg/registry/networking/networkpolicy/strategy.go:53-63).
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut NetworkPolicy,
        old: &NetworkPolicy,
    ) {
        if obj.spec != old.spec {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &NetworkPolicy,
        old: &NetworkPolicy,
    ) -> ErrorList {
        validate_network_policy_update(obj, old)
    }
    fn allow_unconditional_update(&self) -> bool {
        true
    }
    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &NetworkPolicy,
        _old: &NetworkPolicy,
    ) -> Vec<String> {
        warnings(obj)
    }
}
impl RestDeleteStrategy<NetworkPolicy> for Strategy {}
/// NewREST (pkg/registry/networking/networkpolicy/storage/storage.go:36-55).
pub fn new_store(storage: Arc<StorageBackend>) -> Store<NetworkPolicy, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("networking.k8s.io", "networkpolicies"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}
/// networkPolicyWarnings (pkg/registry/networking/networkpolicy/strategy.go:103-132).
fn warnings(obj: &NetworkPolicy) -> Vec<String> {
    fn block_warnings(out: &mut Vec<String>, block: &IPBlock, path: &Path) {
        out.extend(get_warnings_for_cidr(&path.child("cidr"), &block.cidr));
        for (i, except) in block.except.iter().flatten().enumerate() {
            out.extend(get_warnings_for_cidr(
                &path.child("except").index(i),
                except,
            ));
        }
    }
    let mut out = Vec::new();
    for (i, rule) in obj.spec.ingress.iter().flatten().enumerate() {
        for (j, peer) in rule.from.iter().flatten().enumerate() {
            if let Some(block) = &peer.ip_block {
                block_warnings(
                    &mut out,
                    block,
                    &Path::new("spec")
                        .child("ingress")
                        .index(i)
                        .child("from")
                        .index(j)
                        .child("ipBlock"),
                );
            }
        }
    }
    for (i, rule) in obj.spec.egress.iter().flatten().enumerate() {
        for (j, peer) in rule.to.iter().flatten().enumerate() {
            if let Some(block) = &peer.ip_block {
                block_warnings(
                    &mut out,
                    block,
                    &Path::new("spec")
                        .child("egress")
                        .index(i)
                        .child("to")
                        .index(j)
                        .child("ipBlock"),
                );
            }
        }
    }
    out
}
