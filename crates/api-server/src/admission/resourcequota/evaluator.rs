//! Quota evaluators: how much quota an object consumes.
//!
//! Ports of `staging/src/k8s.io/apiserver/pkg/quota/v1/generic/evaluator.go`
//! (`objectCountEvaluator`, `Matches`) and
//! `pkg/quota/v1/evaluator/core/services.go` (`serviceEvaluator`), with the
//! registry of `pkg/quota/v1/evaluator/core/registry.go`.
//!
//! and `pkg/quota/v1/evaluator/core/persistent_volume_claims.go`
//! (`pvcEvaluator`; its `Usage` is `rusternetes_common::quota::pvc_usage`,
//! shared with the quota controller's `UsageStats`), including the
//! VolumeAttributesClass scope and `Handles` for the `status` subresource
//! (`RequiresQuotaReplenish`).
//!
//! and `pkg/quota/v1/evaluator/core/pods.go` (`podEvaluator`: `Handles`,
//! `Matches` through `podMatchesScopeFunc`, `MatchingResources`,
//! `MatchingScopes`, `Constraints`, `Usage`; the scope functions and `Usage`
//! are `rusternetes_common::quota`, shared with the quota controller).

use rusternetes_common::admission::Operation;
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::quota::{self, ResourceList};
use rusternetes_common::quota::{
    pvc_matches_resource_name, pvc_matches_scope, pvc_matching_scopes,
    pvc_requires_quota_replenish, pvc_usage, scope_selectors_from_quota,
};
use rusternetes_common::resources::{
    PersistentVolumeClaim, Pod, ResourceQuota, ScopedResourceSelectorRequirement, Service,
    ServiceType,
};
use serde_json::Value;

use super::Attributes;
use crate::registry::rest::GroupResource;

/// `quota.Evaluator` (apiserver/pkg/quota/v1/interfaces.go), reduced to what
/// admission calls. The evaluators without a scope function of their own use
/// `generic.MatchesNoScopeFunc`, which matches no scope, so their
/// `MatchingScopes` / `UncoveredQuotaScopes` return nothing.
pub trait Evaluator: Send + Sync {
    /// `Handles`: whether the request can change quota usage. It receives the
    /// whole request because the `status` subresource decides on the objects
    /// (persistent_volume_claims.go:96-111).
    fn handles(&self, a: &Attributes<'_>) -> bool;
    /// `GroupResource`: the resource this evaluator tracks, which
    /// `filterLimitedResourcesByGroupResource` compares against.
    fn group_resource(&self) -> GroupResource;
    /// `MatchingResources`: the subset of `input` this evaluator tracks.
    fn matching_resources(&self, input: &[String]) -> Vec<String>;
    /// `Usage`: what `obj` consumes.
    fn usage(&self, obj: &Value) -> Result<ResourceList, String>;
    /// `Constraints`: verify the required resources are present on `item`.
    /// A no-op for every evaluator ported here.
    fn constraints(&self, _required: &[String], _item: &Value) -> Result<(), String> {
        Ok(())
    }

    /// `MatchesScopeFunc` (generic/evaluator.go:63): whether `item` matches
    /// one scope selector. Default is `MatchesNoScopeFunc`
    /// (evaluator.go:50-52): never.
    fn matches_scope(
        &self,
        _selector: &ScopedResourceSelectorRequirement,
        _item: &Value,
    ) -> Result<bool, String> {
        Ok(false)
    }

    /// `Matches` via `generic.Matches` (evaluator.go:150-169): the quota
    /// tracks one of our resources, and `item` matches every scope the quota
    /// carries (`spec.scopes` and `spec.scopeSelector`).
    fn matches(&self, quota: &ResourceQuota, item: &Value) -> Result<bool, String> {
        let match_resource = !self
            .matching_resources(&status_hard_names(quota))
            .is_empty();
        let mut match_scope = true;
        for selector in scope_selectors_from_quota(&quota.spec) {
            let inner = self.matches_scope(&selector, item)?;
            match_scope = match_scope && inner;
        }
        Ok(match_resource && match_scope)
    }

    /// `MatchingScopes`: the selectors `item` matches.
    fn matching_scopes(
        &self,
        item: &Value,
        selectors: &[ScopedResourceSelectorRequirement],
    ) -> Result<Vec<ScopedResourceSelectorRequirement>, String> {
        let mut matched = Vec::new();
        for selector in selectors {
            let m = self
                .matches_scope(selector, item)
                .map_err(|e| format!("error on matching scope {selector:?}: {e}"))?;
            if m {
                matched.push(selector.clone());
            }
        }
        Ok(matched)
    }

    /// `UncoveredQuotaScopes`: the limited scopes with no matched quota scope
    /// of the same name. Evaluators with no scope function return none
    /// (`generic.UncoveredQuotaScopes`'s no-scope behaviour).
    fn uncovered_quota_scopes(
        &self,
        _limited: &[ScopedResourceSelectorRequirement],
        _matched: &[ScopedResourceSelectorRequirement],
    ) -> Result<Vec<ScopedResourceSelectorRequirement>, String> {
        Ok(Vec::new())
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
    group_resource: GroupResource,
    resource_names: Vec<String>,
}

impl ObjectCountEvaluator {
    /// `NewObjectCountEvaluator` (evaluator.go:325-340).
    pub fn new(gr: &GroupResource, alias: Option<&str>) -> Self {
        let mut resource_names = vec![object_count_quota_resource_name_for(gr)];
        if let Some(alias) = alias {
            resource_names.push(alias.to_string());
        }
        Self {
            group_resource: gr.clone(),
            resource_names,
        }
    }
}

impl Evaluator for ObjectCountEvaluator {
    fn group_resource(&self) -> GroupResource {
        self.group_resource.clone()
    }

    /// evaluator.go:275-282: count objects on create, never on a
    /// subresource.
    fn handles(&self, a: &Attributes<'_>) -> bool {
        a.subresource.is_none() && a.operation == Operation::Create
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
    fn group_resource(&self) -> GroupResource {
        GroupResource::new("", "services")
    }

    /// services.go:67-75: create and update, since a type change moves
    /// usage between node ports and load balancers.
    fn handles(&self, a: &Attributes<'_>) -> bool {
        a.subresource.is_none() && matches!(a.operation, Operation::Create | Operation::Update)
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

/// `pvcEvaluator` (persistent_volume_claims.go:73-160).
pub struct PersistentVolumeClaimEvaluator;

impl Evaluator for PersistentVolumeClaimEvaluator {
    fn group_resource(&self) -> GroupResource {
        GroupResource::new("", "persistentvolumeclaims")
    }

    /// persistent_volume_claims.go:96-111: create and update of the claim
    /// itself; on `status`, only an update that `RequiresQuotaReplenish`
    /// (an object that does not decode is not handled).
    fn handles(&self, a: &Attributes<'_>) -> bool {
        match a.subresource {
            None => matches!(a.operation, Operation::Create | Operation::Update),
            Some("status") => {
                let pvc = decode_pvc(a.object);
                let old = a.old_object.map(decode_pvc);
                match (pvc, old) {
                    (Ok(pvc), Some(Ok(old))) => pvc_requires_quota_replenish(&pvc, &old),
                    _ => false,
                }
            }
            Some(_) => false,
        }
    }

    /// `pvcMatchesScopeFunc` (persistent_volume_claims.go:293-309).
    fn matches_scope(
        &self,
        selector: &ScopedResourceSelectorRequirement,
        item: &Value,
    ) -> Result<bool, String> {
        pvc_matches_scope(selector, &decode_pvc(item)?)
    }

    /// `pvcEvaluator.MatchingScopes` (:122-139).
    fn matching_scopes(
        &self,
        item: &Value,
        selectors: &[ScopedResourceSelectorRequirement],
    ) -> Result<Vec<ScopedResourceSelectorRequirement>, String> {
        pvc_matching_scopes(&decode_pvc(item)?, selectors)
    }

    /// `pvcEvaluator.UncoveredQuotaScopes` (:141-161).
    fn uncovered_quota_scopes(
        &self,
        limited: &[ScopedResourceSelectorRequirement],
        matched: &[ScopedResourceSelectorRequirement],
    ) -> Result<Vec<ScopedResourceSelectorRequirement>, String> {
        Ok(limited
            .iter()
            .filter(|l| !matched.iter().any(|m| m.scope_name == l.scope_name))
            .cloned()
            .collect())
    }

    fn matching_resources(&self, input: &[String]) -> Vec<String> {
        let mut out: Vec<String> = input
            .iter()
            .filter(|n| pvc_matches_resource_name(n))
            .cloned()
            .collect();
        out.sort();
        out.dedup();
        out
    }

    fn usage(&self, obj: &Value) -> Result<ResourceList, String> {
        Ok(pvc_usage(&decode_pvc(obj)?))
    }
}

/// `toExternalPersistentVolumeClaimOrError` (persistent_volume_claims.go:
/// 260-274).
fn decode_pvc(obj: &Value) -> Result<PersistentVolumeClaim, String> {
    serde_json::from_value(obj.clone()).map_err(|e| {
        format!("expect *api.PersistentVolumeClaim or *v1.PersistentVolumeClaim, got {e}")
    })
}

/// `podResources` (pods.go:53-65).
const POD_RESOURCES: [&str; 11] = [
    "count/pods",
    "cpu",
    "memory",
    "ephemeral-storage",
    "requests.cpu",
    "requests.memory",
    "requests.ephemeral-storage",
    "limits.cpu",
    "limits.memory",
    "limits.ephemeral-storage",
    "pods",
];

/// `isExtendedResourceNameForQuota` (pods.go:86-97): a non-native name under
/// `requests.`, or the implicit `requests.deviceclass.resource.kubernetes.io/`
/// form.
fn is_extended_resource_name_for_quota(name: &str) -> bool {
    let implicit = name.starts_with("requests.deviceclass.resource.kubernetes.io/");
    (!quota::is_native_resource_name(name) || implicit) && name.starts_with(quota::REQUESTS_PREFIX)
}

/// `toExternalPodOrError` (pods.go:300-313).
fn decode_pod(obj: &Value) -> Result<Pod, String> {
    serde_json::from_value(obj.clone()).map_err(|e| format!("expect *api.Pod or *v1.Pod, got {e}"))
}

/// `podEvaluator` (pods.go:121-298).
pub struct PodEvaluator;

impl Evaluator for PodEvaluator {
    fn group_resource(&self) -> GroupResource {
        GroupResource::new("", "pods")
    }

    /// `Handles` (pods.go:179-199): a create, a `resize`, and an update only
    /// when the pod moves between the Terminating scopes. Anything it cannot
    /// decode is not handled.
    fn handles(&self, a: &Attributes<'_>) -> bool {
        match a.subresource {
            None => {
                if a.operation == Operation::Update {
                    let (Ok(pod), Some(Ok(old))) =
                        (decode_pod(a.object), a.old_object.map(decode_pod))
                    else {
                        return false;
                    };
                    // when scope changed
                    if quota::is_terminating(&old) != quota::is_terminating(&pod) {
                        return true;
                    }
                }
                a.operation == Operation::Create
            }
            Some("resize") => a.operation == Operation::Update,
            Some(_) => false,
        }
    }

    /// `MatchingResources` (pods.go:208-222).
    fn matching_resources(&self, input: &[String]) -> Vec<String> {
        let mut out: Vec<String> = input
            .iter()
            .filter(|name| {
                POD_RESOURCES.contains(&name.as_str())
                    || name.starts_with(quota::HUGEPAGES_PREFIX)
                    || name.starts_with("requests.hugepages-")
                    || is_extended_resource_name_for_quota(name)
            })
            .cloned()
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// `podMatchesScopeFunc` (pods.go:331-356).
    fn matches_scope(
        &self,
        selector: &ScopedResourceSelectorRequirement,
        item: &Value,
    ) -> Result<bool, String> {
        quota::pod_matches_scope(selector, &decode_pod(item)?)
    }

    /// `Constraints` (pods.go:124-168).
    fn constraints(&self, required: &[String], item: &Value) -> Result<(), String> {
        quota::pod_constraints(&decode_pod(item)?, required)
    }

    /// `PodUsageFunc` (pods.go:381-411).
    fn usage(&self, obj: &Value) -> Result<ResourceList, String> {
        Ok(quota::pod_usage(&decode_pod(obj)?, chrono::Utc::now()))
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
/// whose evaluator is not ported (none today).
pub fn evaluator_for(gr: &GroupResource) -> Option<Box<dyn Evaluator>> {
    if gr.group.is_empty() {
        match gr.resource.as_str() {
            "services" => return Some(Box::new(ServiceEvaluator)),
            "persistentvolumeclaims" => return Some(Box::new(PersistentVolumeClaimEvaluator)),
            "pods" => return Some(Box::new(PodEvaluator)),
            _ => {}
        }
    }
    Some(Box::new(ObjectCountEvaluator::new(
        gr,
        legacy_object_count_alias(gr),
    )))
}
