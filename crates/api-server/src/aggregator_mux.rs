//! The per-APIService handlers that kube-aggregator's
//! `APIAggregator.AddAPIService` mounts: the `proxyHandler` and the
//! `apiGroupHandler`.
//!
//! Ported from `staging/src/k8s.io/kube-aggregator/pkg/apiserver/`:
//! - `handler_proxy.go`: `proxyHandler`, `proxyHandlingInfo`,
//!   `updateAPIService`, `setServiceAvailable`, and the decision ladder at
//!   the top of `proxyHandler.ServeHTTP` ([`ProxyHandler::decide`]);
//! - `handler_apis.go`: `apiGroupHandler.ServeHTTP` and
//!   `convertToDiscoveryAPIGroup` ([`serve_api_group`]);
//! - `apis/apiregistration/v1/helper/helpers.go`: `SortedByGroupAndVersion`,
//!   `ByGroupPriorityMinimum`, `ByVersionPriority`,
//!   `IsAPIServiceConditionTrue`.
//!
//! Upstream mounts these on the generic apiserver's `NonGoRestfulMux` by
//! path; this server dispatches `/apis/<group>[/<version>...]` in
//! `router::custom_resource_fallback`, so the "mux" is the
//! [`crate::apiservice_registration::AggregatorHandlerManager`] lookup that
//! the fallback consults ([`active_handler_manager`]). `Unregister` is
//! removal from that table.
//!
//! The actual forward (`NewUpgradeAwareHandler` + `NewAuthProxyRoundTripper`)
//! is `handlers::aggregator::forward_to_aggregator`; the
//! `serviceResolver.ResolveEndpoint` call is
//! `handlers::aggregator::resolve_aggregator_backend_address`.

use std::cmp::Ordering;
use std::sync::{Arc, Mutex, RwLock};

use rusternetes_common::resources::apiregistration::AVAILABLE;
use rusternetes_common::resources::APIService;
use rusternetes_discovery::kube_version::compare_kube_aware_version_strings;
use serde_json::{json, Value};

use crate::apiservice_registration::AggregatorHandlerManager;

/// `IsAPIServiceConditionTrue(apiService, Available)` (helpers.go).
pub fn is_api_service_condition_true(api_service: &APIService, condition_type: &str) -> bool {
    api_service
        .status
        .conditions
        .iter()
        .find(|c| c.type_ == condition_type)
        .is_some_and(|c| c.status == "True")
}

/// `proxyHandlingInfo` (handler_proxy.go:60-82), minus the transport: the
/// roundtripper is built per request by `forward_to_aggregator` from
/// `insecure_skip_tls_verify` / `ca_bundle` / the service DNS name.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProxyHandlingInfo {
    /// `local`: this APIService is locally satisfied.
    pub local: bool,
    /// `name`: the name of the APIService.
    pub name: String,
    /// `transportBuildingError`: reported to clients as a 500.
    pub transport_building_error: Option<String>,
    pub service_name: String,
    pub service_namespace: String,
    /// `serviceAvailable`.
    pub service_available: bool,
    pub service_port: i32,
    /// `transportConfig.TLS.Insecure`.
    pub insecure_skip_tls_verify: bool,
    /// `transportConfig.TLS.CAData` (the wire, base64 form).
    pub ca_bundle: Option<String>,
}

/// What `proxyHandler.ServeHTTP` does with a request before it touches the
/// network (handler_proxy.go `ServeHTTP`).
#[derive(Debug, Clone, PartialEq)]
pub enum ProxyDecision {
    /// `handlingInfo` is nil: `r.localDelegate.ServeHTTP`.
    Delegate,
    /// `handlingInfo.local`: `r.localDelegate.ServeHTTP`.
    Local,
    /// `proxyError(w, req, "service unavailable", 503)`.
    ServiceUnavailable,
    /// `proxyError(w, req, transportBuildingError.Error(), 500)`.
    Error(String),
    /// Resolve the endpoint and forward.
    Proxy(ProxyHandlingInfo),
}

/// `proxyHandler` (handler_proxy.go:48-66). `handlingInfo` is an
/// `atomic.Value` upstream; an `RwLock` here.
#[derive(Default)]
pub struct ProxyHandler {
    pub handling_info: RwLock<Option<ProxyHandlingInfo>>,
}

impl ProxyHandler {
    pub fn new() -> Self {
        Self::default()
    }

    /// `proxyHandler.updateAPIService` (handler_proxy.go).
    pub fn update_api_service(&self, api_service: &APIService) {
        let info = match &api_service.spec.service {
            None => ProxyHandlingInfo {
                local: true,
                ..Default::default()
            },
            Some(svc) => ProxyHandlingInfo {
                local: false,
                name: api_service.metadata.name.clone(),
                transport_building_error: None,
                service_name: svc.name.clone(),
                service_namespace: svc.namespace.clone(),
                // `*apiService.Spec.Service.Port`: defaulted to 443
                // (`SetDefaults_ServiceReference`).
                service_port: svc.port.unwrap_or(443),
                service_available: is_api_service_condition_true(api_service, AVAILABLE),
                insecure_skip_tls_verify: api_service.spec.insecure_skip_tls_verify,
                ca_bundle: api_service.spec.ca_bundle.clone(),
            },
        };
        *self.handling_info.write().unwrap() = Some(info);
    }

    /// `proxyHandler.setServiceAvailable`.
    pub fn set_service_available(&self) {
        if let Some(info) = self.handling_info.write().unwrap().as_mut() {
            info.service_available = true;
        }
    }

    /// `handlingInfo.Load()`.
    pub fn handling_info(&self) -> Option<ProxyHandlingInfo> {
        self.handling_info.read().unwrap().clone()
    }

    /// The decision ladder at the top of `proxyHandler.ServeHTTP`, in
    /// upstream's order: nil -> delegate; local -> delegate; not available ->
    /// 503; transport error -> 500; otherwise proxy.
    pub fn decide(&self) -> ProxyDecision {
        let Some(info) = self.handling_info() else {
            return ProxyDecision::Delegate;
        };
        if info.local {
            return ProxyDecision::Local;
        }
        if !info.service_available {
            return ProxyDecision::ServiceUnavailable;
        }
        if let Some(e) = &info.transport_building_error {
            return ProxyDecision::Error(e.clone());
        }
        ProxyDecision::Proxy(info)
    }
}

// ---------------------------------------------------------------------------
// helpers.go: SortedByGroupAndVersion
// ---------------------------------------------------------------------------

/// `SortedByGroupAndVersion`: groups sorted by `ByGroupPriorityMinimum`
/// (highest priority first, then name), each group's members sorted by
/// `ByVersionPriority` (highest version priority first, then the highest
/// kube-aware version).
pub fn sorted_by_group_and_version(servers: &[APIService]) -> Vec<Vec<&APIService>> {
    let mut by_priority: Vec<&APIService> = servers.iter().collect();
    by_priority.sort_by(|a, b| {
        b.spec
            .group_priority_minimum
            .cmp(&a.spec.group_priority_minimum)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });
    let mut ret: Vec<Vec<&APIService>> = Vec::new();
    for curr in by_priority {
        match ret.iter_mut().find(|g| g[0].spec.group == curr.spec.group) {
            Some(g) => {
                g.push(curr);
                g.sort_by(|a, b| by_version_priority(a, b));
            }
            None => ret.push(vec![curr]),
        }
    }
    ret
}

/// `ByVersionPriority.Less` as an `Ordering`.
fn by_version_priority(a: &APIService, b: &APIService) -> Ordering {
    if a.spec.version_priority != b.spec.version_priority {
        return b.spec.version_priority.cmp(&a.spec.version_priority);
    }
    0.cmp(&compare_kube_aware_version_strings(
        &a.spec.version,
        &b.spec.version,
    ))
}

// ---------------------------------------------------------------------------
// handler_apis.go: apiGroupHandler
// ---------------------------------------------------------------------------

/// `convertToDiscoveryAPIGroup`: the discovery `APIGroup` for the
/// APIServices of one group, or `None` when there are none.
pub fn convert_to_discovery_api_group(api_services: &[APIService]) -> Option<Value> {
    let sorted = sorted_by_group_and_version(api_services);
    let first = sorted.into_iter().next()?;
    let mut preferred: Option<Value> = None;
    let mut name = String::new();
    let mut versions = Vec::new();
    for api_service in first {
        let gv = json!({
            "groupVersion": format!("{}/{}", api_service.spec.group, api_service.spec.version),
            "version": api_service.spec.version,
        });
        // "the first APIService which is valid becomes the default"
        if preferred.is_none() {
            name = api_service.spec.group.clone();
            preferred = Some(gv.clone());
        }
        versions.push(gv);
    }
    Some(json!({
        "kind": "APIGroup",
        "apiVersion": "v1",
        "name": name,
        "versions": versions,
        "preferredVersion": preferred?,
    }))
}

/// What `apiGroupHandler.ServeHTTP` does.
#[derive(Debug, Clone, PartialEq)]
pub enum ApiGroupResponse {
    /// No APIService for the group: `r.delegate.ServeHTTP(w, req)`.
    Delegate,
    /// `http.Error(w, "", http.StatusNotFound)`.
    NotFound,
    /// 200 with the negotiated `APIGroup`.
    Group(Value),
}

/// `apiGroupHandler.ServeHTTP` over `lister.List(labels.Everything())`.
pub fn serve_api_group(group_name: &str, api_services: &[APIService]) -> ApiGroupResponse {
    let for_group: Vec<APIService> = api_services
        .iter()
        .filter(|a| a.spec.group == group_name)
        .cloned()
        .collect();
    if for_group.is_empty() {
        return ApiGroupResponse::Delegate;
    }
    match convert_to_discovery_api_group(&for_group) {
        Some(g) => ApiGroupResponse::Group(g),
        None => ApiGroupResponse::NotFound,
    }
}

// ---------------------------------------------------------------------------
// The "mux": which manager the request path consults
// ---------------------------------------------------------------------------

static ACTIVE: Mutex<Option<Arc<AggregatorHandlerManager>>> = Mutex::new(None);

/// Install the `APIAggregator` whose handlers serve requests (the production
/// wiring, `spawn_apiservice_registration_hooks`). The last install wins.
pub fn install_handler_manager(manager: Arc<AggregatorHandlerManager>) {
    *ACTIVE.lock().unwrap() = Some(manager);
}

/// The installed aggregator, if any. When none is installed (a router built
/// without the registration hooks, e.g. unit tests) callers keep resolving the
/// APIService from storage.
pub fn active_handler_manager() -> Option<Arc<AggregatorHandlerManager>> {
    ACTIVE.lock().unwrap().clone()
}
