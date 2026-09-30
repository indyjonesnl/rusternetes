//! Quota evaluators: how much quota an object consumes.
//!
//! Ports of `staging/src/k8s.io/apiserver/pkg/quota/v1/generic/evaluator.go`
//! (`objectCountEvaluator`, `Matches`) and
//! `pkg/quota/v1/evaluator/core/services.go` (`serviceEvaluator`), with the
//! registry of `pkg/quota/v1/evaluator/core/registry.go`.
//!
//! The pod and PersistentVolumeClaim evaluators (`pods.go`,
//! `persistent_volume_claims.go`) are not ported here: pods are admitted by
//! the pod handler's own quota check (`crate::admission::check_resource_quota`),
//! and the quota controller does not yet compute the PVC evaluator's
//! `requests.storage` usage, so enforcing it would reject every claim with
//! `status unknown for quota` (#2081).

use rusternetes_common::admission::Operation;
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::quota::ResourceList;
use rusternetes_common::resources::{ResourceQuota, Service, ServiceType};
use serde_json::Value;

use crate::registry::rest::GroupResource;

/// `quota.Evaluator` (apiserver/pkg/quota/v1/interfaces.go), reduced to what
/// admission calls. Scope matching is always `MatchesNoScopeFunc` for the
/// evaluators ported here, so `MatchingScopes` / `UncoveredQuotaScopes`
/// return nothing and are left out.
pub trait Evaluator: Send + Sync {
    /// `Handles`: whether the operation can change quota usage.
    fn handles(&self, operation: &Operation, subresource: Option<&str>) -> bool;
    /// `MatchingResources`: the subset of `input` this evaluator tracks.
    fn matching_resources(&self, input: &[String]) -> Vec<String>;
    /// `Usage`: what `obj` consumes.
    fn usage(&self, obj: &Value) -> Result<ResourceList, String>;

    /// `Matches` via `generic.Matches` (evaluator.go:190-209) with
    /// `MatchesNoScopeFunc`: the quota tracks one of our resources, and has
    /// no scope (no scope matches an object this evaluator measures).
    fn matches(&self, quota: &ResourceQuota) -> bool {
        let match_resource = !self
            .matching_resources(&status_hard_names(quota))
            .is_empty();
        let has_scope = quota.spec.scopes.as_ref().is_some_and(|s| !s.is_empty())
            || quota
                .spec
                .scope_selector
                .as_ref()
                .is_some_and(|s| !s.match_expressions.is_empty());
        match_resource && !has_scope
    }
}

/// `quota.ResourceNames(resourceQuota.Status.Hard)`.
pub fn status_hard_names(quota: &ResourceQuota) -> Vec<String> {
    let mut names: Vec<String> = quota
        .status
        .as_ref()
        .and_then(|s| s.hard.as_ref())
        .map(|h| h.keys().cloned().collect())
        .unwrap_or_default();
    names.sort();
    names
}

/// `quota.Intersection` (apiserver/pkg/quota/v1/resources.go:170-186).
fn intersection(a: &[String], b: &[String]) -> Vec<String> {
    let mut out: Vec<String> = a.iter().filter(|n| b.contains(n)).cloned().collect();
    out.sort();
    out.dedup();
    out
}

fn one() -> Quantity {
    Quantity::from_value(1, Format::DecimalSI)
}

fn count(n: i64) -> Quantity {
    Quantity::from_value(n, Format::DecimalSI)
}

/// `ObjectCountQuotaResourceNameFor` (evaluator.go:167-173).
pub fn object_count_quota_resource_name_for(gr: &GroupResource) -> String {
    if gr.group.is_empty() {
        format!("count/{}", gr.resource)
    } else {
        format!("count/{}.{}", gr.resource, gr.group)
    }
}

/// `objectCountEvaluator` (evaluator.go:258-340): one of each resource name
/// per object, charged on CREATE only.
pub struct ObjectCountEvaluator {
    resource_names: Vec<String>,
}

impl ObjectCountEvaluator {
    /// `NewObjectCountEvaluator` (evaluator.go:325-340).
    pub fn new(gr: &GroupResource, alias: Option<&str>) -> Self {
        let mut resource_names = vec![object_count_quota_resource_name_for(gr)];
        if let Some(alias) = alias {
            resource_names.push(alias.to_string());
        }
        Self { resource_names }
    }
}

impl Evaluator for ObjectCountEvaluator {
    /// evaluator.go:275-282: count objects on create, never on a
    /// subresource.
    fn handles(&self, operation: &Operation, subresource: Option<&str>) -> bool {
        subresource.is_none() && *operation == Operation::Create
    }

    fn matching_resources(&self, input: &[String]) -> Vec<String> {
        intersection(input, &self.resource_names)
    }

    fn usage(&self, _obj: &Value) -> Result<ResourceList, String> {
        Ok(self
            .resource_names
            .iter()
            .map(|n| (n.clone(), one()))
            .collect())
    }
}

/// `serviceObjectCountName` (services.go:35).
const SERVICE_OBJECT_COUNT_NAME: &str = "count/services";
const SERVICES: &str = "services";
const SERVICES_NODE_PORTS: &str = "services.nodeports";
const SERVICES_LOAD_BALANCERS: &str = "services.loadbalancers";

/// `serviceEvaluator` (services.go:44-173).
pub struct ServiceEvaluator;

impl Evaluator for ServiceEvaluator {
    /// services.go:67-75: create and update, since a type change moves
    /// usage between node ports and load balancers.
    fn handles(&self, operation: &Operation, subresource: Option<&str>) -> bool {
        subresource.is_none() && matches!(operation, Operation::Create | Operation::Update)
    }

    /// `serviceResources` (services.go:37-42).
    fn matching_resources(&self, input: &[String]) -> Vec<String> {
        let tracked = [
            SERVICE_OBJECT_COUNT_NAME,
            SERVICES,
            SERVICES_NODE_PORTS,
            SERVICES_LOAD_BALANCERS,
        ]
        .map(String::from);
        intersection(input, &tracked)
    }

    /// `Usage` (services.go:117-147).
    fn usage(&self, obj: &Value) -> Result<ResourceList, String> {
        let svc: Service = serde_json::from_value(obj.clone())
            .map_err(|e| format!("expect *api.Service or *v1.Service, got {e}"))?;
        let ports = svc.spec.ports.len() as i64;
        let mut result = ResourceList::new();
        result.insert(SERVICE_OBJECT_COUNT_NAME.to_string(), one());
        result.insert(SERVICES.to_string(), one());
        result.insert(SERVICES_LOAD_BALANCERS.to_string(), count(0));
        result.insert(SERVICES_NODE_PORTS.to_string(), count(0));
        match svc.spec.service_type {
            Some(ServiceType::NodePort) => {
                result.insert(SERVICES_NODE_PORTS.to_string(), count(ports));
            }
            Some(ServiceType::LoadBalancer) => {
                // Without node-port allocation only explicit node ports count
                // (`portsWithNodePorts`, services.go:149-157).
                let node_ports = if svc.spec.allocate_load_balancer_node_ports == Some(false) {
                    svc.spec
                        .ports
                        .iter()
                        .filter(|p| p.node_port.is_some_and(|n| n != 0))
                        .count() as i64
                } else {
                    ports
                };
                result.insert(SERVICES_NODE_PORTS.to_string(), count(node_ports));
                result.insert(SERVICES_LOAD_BALANCERS.to_string(), one());
            }
            _ => {}
        }
        Ok(result)
    }
}

/// `legacyObjectCountAliases` (registry.go:33-38).
fn legacy_object_count_alias(gr: &GroupResource) -> Option<&'static str> {
    if !gr.group.is_empty() {
        return None;
    }
    match gr.resource.as_str() {
        "configmaps" => Some("configmaps"),
        "resourcequotas" => Some("resourcequotas"),
        "replicationcontrollers" => Some("replicationcontrollers"),
        "secrets" => Some("secrets"),
        _ => None,
    }
}

/// The evaluator for `gr`: the registry of `NewEvaluators`
/// (registry.go:41-70), falling back to an object-count evaluator as
/// `quotaEvaluator.Evaluate` does for an unregistered resource
/// (plugin/resourcequota/controller.go:667-674). `None` for the resources
/// whose evaluator is not ported (see the module docs).
pub fn evaluator_for(gr: &GroupResource) -> Option<Box<dyn Evaluator>> {
    if gr.group.is_empty() {
        match gr.resource.as_str() {
            "services" => return Some(Box::new(ServiceEvaluator)),
            "pods" | "persistentvolumeclaims" => return None,
            _ => {}
        }
    }
    Some(Box::new(ObjectCountEvaluator::new(
        gr,
        legacy_object_count_alias(gr),
    )))
}
