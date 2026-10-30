//! The HTTP half of the kube-aggregator `discoveryManager`: contacting an
//! aggregated API server for its discovery document.
//!
//! Ported from `staging/src/k8s.io/kube-aggregator/pkg/apiserver/handler_discovery.go`
//! (`fetchFreshDiscoveryForService`, the request it builds and the response
//! classification that follows), plus its helpers:
//! - `discovery.ContentTypeIsGVK`
//!   (`staging/src/k8s.io/client-go/discovery/discovery_client.go:377`);
//! - `endpoints.ConvertGroupVersionIntoToDiscovery`
//!   (`staging/src/k8s.io/apiserver/pkg/endpoints/installer.go:77`), the legacy
//!   `APIResourceList` -> aggregated-discovery conversion.
//!
//! `apidiscovery.k8s.io/v2` and `v2beta1` are field-for-field identical on the
//! wire (`apiserver/pkg/apis/apidiscovery/v2/conversion.go` is generated
//! `unsafe` casts), so the document is handled as JSON and the v2beta1 "skew"
//! needs no conversion beyond accepting its content type.
//!
//! Deviation: upstream hands `fetchFreshDiscoveryForService` the APIService's
//! `proxyHandler` (an `http.Handler`). Rusternetes has no per-APIService
//! handler (see `apiservice_registration` module docs), so the same role is the
//! [`AggregatorHandler`] trait, whose production implementation
//! ([`ProxyDiscoveryHandler`]) resolves the backend through the aggregator
//! proxy machinery (`handlers::aggregator`), including its "APIService is
//! Available" gate (handler_proxy.go `serviceAvailable` -> 503).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_storage::Storage;
use serde_json::{json, Map, Value};

use crate::apiservice_registration::{DiscoveryFreshness, VersionDiscovery};
use crate::handlers::aggregator::{
    build_aggregator_client, resolve_aggregator_target_with_storage,
};

/// `discovery.AcceptV2` (client-go discovery_client.go:67).
pub const ACCEPT_V2: &str = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList";
/// `discovery.AcceptV2Beta1` (client-go discovery_client.go:66).
pub const ACCEPT_V2_BETA1: &str =
    "application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList";

/// `http.TimeoutHandler(info.handler, 5*time.Second, "request timed out")`
/// (handler_discovery.go:227).
pub const DISCOVERY_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The user the aggregator applies to its own discovery requests
/// (handler_discovery.go:236-238 and :331-333).
pub const AGGREGATOR_USER: &str = "system:kube-aggregator";
pub const AGGREGATOR_GROUP: &str = "system:masters";

/// The `*http.Request` `fetchFreshDiscoveryForService` builds.
#[derive(Debug, Clone, Default)]
pub struct HandlerRequest {
    pub path: String,
    pub accept: String,
    pub if_none_match: Option<String>,
    pub user: String,
    pub groups: Vec<String>,
}

/// The `InMemoryResponseWriter` it records into.
#[derive(Debug, Clone, Default)]
pub struct HandlerResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub body: Vec<u8>,
}

/// An APIService's `proxyHandler`, as seen by the discovery manager.
#[async_trait]
pub trait AggregatorHandler: Send + Sync {
    /// Serve `req` as the aggregated server of APIService `<version>.<group>`.
    async fn serve(&self, group: &str, version: &str, req: HandlerRequest) -> HandlerResponse;
}

/// `http.TimeoutHandler`: "request timed out" as a 503.
pub async fn serve_with_timeout(
    handler: &dyn AggregatorHandler,
    group: &str,
    version: &str,
    req: HandlerRequest,
) -> HandlerResponse {
    match tokio::time::timeout(DISCOVERY_FETCH_TIMEOUT, handler.serve(group, version, req)).await {
        Ok(r) => r,
        Err(_) => HandlerResponse {
            status: 503,
            content_type: None,
            etag: None,
            body: b"request timed out".to_vec(),
        },
    }
}

/// `discovery.ContentTypeIsGVK`: `mime.ParseMediaType` then a match on the
/// `application/json` base and the `g`, `v` and `as` parameters.
pub fn content_type_is_gvk(content_type: &str, group: &str, version: &str, kind: &str) -> bool {
    let mut parts = content_type.split(';');
    let base = parts.next().unwrap_or("").trim().to_ascii_lowercase();
    if base != "application/json" {
        return false;
    }
    let mut params: BTreeMap<String, String> = BTreeMap::new();
    for p in parts {
        let Some((k, v)) = p.split_once('=') else {
            continue;
        };
        params.insert(
            k.trim().to_ascii_lowercase(),
            v.trim().trim_matches('"').to_string(),
        );
    }
    params.get("g").map(String::as_str) == Some(group)
        && params.get("v").map(String::as_str) == Some(version)
        && params.get("as").map(String::as_str) == Some(kind)
}

/// `isV2GVK || isV2Beta1GVK` (handler_discovery.go:256-257).
pub fn is_aggregated_discovery_content_type(content_type: &str) -> bool {
    content_type_is_gvk(
        content_type,
        "apidiscovery.k8s.io",
        "v2",
        "APIGroupDiscoveryList",
    ) || content_type_is_gvk(
        content_type,
        "apidiscovery.k8s.io",
        "v2beta1",
        "APIGroupDiscoveryList",
    )
}

pub fn gvk_value(group: &str, version: &str, kind: &str) -> Value {
    json!({"group": group, "version": version, "kind": kind})
}

fn str_field<'a>(v: &'a Value, k: &str) -> &'a str {
    v.get(k).and_then(Value::as_str).unwrap_or("")
}

/// `endpoints.ConvertGroupVersionIntoToDiscovery` (installer.go:77-168).
pub fn convert_group_version_into_to_discovery(list: &[Value]) -> Result<Vec<Value>, String> {
    let scope_of = |r: &Value| {
        if r.get("namespaced")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            "Namespaced"
        } else {
            "Cluster"
        }
    };
    let mut out: Vec<Map<String, Value>> = Vec::new();
    let mut parents: BTreeMap<String, usize> = BTreeMap::new();

    // "Loop through all top-level resources"
    for r in list {
        let name = str_field(r, "name");
        if name.contains('/') {
            // "Skip subresources for now so we can get the list of resources"
            continue;
        }
        let mut m = Map::new();
        m.insert("resource".into(), json!(name));
        m.insert("scope".into(), json!(scope_of(r)));
        m.insert(
            "responseKind".into(),
            gvk_value(
                str_field(r, "group"),
                str_field(r, "version"),
                str_field(r, "kind"),
            ),
        );
        m.insert(
            "verbs".into(),
            r.get("verbs").cloned().unwrap_or_else(|| json!([])),
        );
        for (src, dst) in [("shortNames", "shortNames"), ("categories", "categories")] {
            if let Some(v) = r
                .get(src)
                .filter(|v| v.as_array().is_some_and(|a| !a.is_empty()))
            {
                m.insert(dst.into(), v.clone());
            }
        }
        m.insert(
            "singularResource".into(),
            json!(str_field(r, "singularName")),
        );
        out.push(m);
        parents.insert(name.to_string(), out.len() - 1);
    }

    // "Loop through all subresources"
    for r in list {
        let name = str_field(r, "name");
        let Some((parent, sub)) = name.split_once('/') else {
            continue;
        };
        let scope = scope_of(r);
        let idx = match parents.get(parent) {
            Some(i) => *i,
            None => {
                // "If a subresource exists without a parent, create a parent"
                let mut m = Map::new();
                m.insert("resource".into(), json!(parent));
                m.insert("scope".into(), json!(scope));
                // "avoid nil panics in v0.26.0-v0.26.3 client-go clients"
                m.insert("responseKind".into(), gvk_value("", "", ""));
                m.insert("verbs".into(), json!([]));
                m.insert("singularResource".into(), json!(""));
                out.push(m);
                parents.insert(parent.to_string(), out.len() - 1);
                out.len() - 1
            }
        };
        let parent_scope = out[idx]
            .get("scope")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if parent_scope != scope {
            return Err(format!(
                "Error: Parent {parent} (scope: {parent_scope}) and subresource {sub} (scope: {scope}) scope do not match"
            ));
        }
        let kind = str_field(r, "kind");
        let response_kind = if kind.is_empty() {
            gvk_value("", "", "")
        } else {
            gvk_value(str_field(r, "group"), str_field(r, "version"), kind)
        };
        let subresource = json!({
            "subresource": sub,
            "responseKind": response_kind,
            "verbs": r.get("verbs").cloned().unwrap_or_else(|| json!([])),
        });
        out[idx]
            .entry("subresources")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .expect("subresources is an array")
            .push(subresource);
    }
    Ok(out.into_iter().map(Value::Object).collect())
}

/// Default a nil `responseKind` to `{}` "to avoid nil panics in
/// v0.26.0-v0.26.3 client-go clients" (handler_discovery.go:286-297).
fn default_response_kinds(resources: &mut [Value]) {
    for r in resources {
        let Some(obj) = r.as_object_mut() else {
            continue;
        };
        if obj.get("responseKind").is_none_or(Value::is_null) {
            obj.insert("responseKind".into(), json!({}));
        }
        if let Some(subs) = obj.get_mut("subresources").and_then(Value::as_array_mut) {
            for s in subs {
                if let Some(so) = s.as_object_mut() {
                    if so.get("responseKind").is_none_or(Value::is_null) {
                        so.insert("responseKind".into(), json!({}));
                    }
                }
            }
        }
    }
}

/// Decode an `APIGroupDiscoveryList` (v2 or v2beta1) into the
/// group+version -> entry map (handler_discovery.go:274-299).
pub fn parse_discovery_list(
    body: &[u8],
) -> Result<BTreeMap<(String, String), VersionDiscovery>, String> {
    let doc: Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let mut out = BTreeMap::new();
    let items = doc
        .get("items")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for g in items {
        let group = g
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        for v in g
            .get("versions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            let version = str_field(&v, "version").to_string();
            let mut resources = v
                .get("resources")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            default_response_kinds(&mut resources);
            out.insert(
                (group.clone(), version.clone()),
                VersionDiscovery {
                    version,
                    resources,
                    freshness: DiscoveryFreshness::Current,
                },
            );
        }
    }
    Ok(out)
}

/// Decode the legacy `APIResourceList` fallback response into a one-entry
/// discovery map (handler_discovery.go:349-367).
pub fn parse_legacy_resource_list(
    group: &str,
    version: &str,
    body: &[u8],
) -> Result<BTreeMap<(String, String), VersionDiscovery>, String> {
    let doc: Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let list = doc
        .get("resources")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let resources = convert_group_version_into_to_discovery(&list)?;
    Ok(BTreeMap::from([(
        (group.to_string(), version.to_string()),
        VersionDiscovery {
            version: version.to_string(),
            resources,
            freshness: DiscoveryFreshness::Current,
        },
    )]))
}

/// The legacy discovery path for a group version (handler_discovery.go:316-321).
pub fn legacy_path(group: &str, version: &str) -> String {
    if group.is_empty() {
        format!("/api/{version}")
    } else {
        format!("/apis/{group}/{version}")
    }
}

// ---------------------------------------------------------------------------
// Production handler
// ---------------------------------------------------------------------------

/// Reaches the aggregated server of an APIService the way the aggregator proxy
/// does: [`resolve_aggregator_target_with_storage`] (Available gate, endpoint
/// resolution, caBundle) then an HTTPS request pinned to the backend pod.
pub struct ProxyDiscoveryHandler<S: Storage + Send + Sync + 'static> {
    storage: Arc<S>,
}

impl<S: Storage + Send + Sync + 'static> ProxyDiscoveryHandler<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

fn unavailable(message: &str) -> HandlerResponse {
    HandlerResponse {
        status: 503,
        content_type: None,
        etag: None,
        body: message.as_bytes().to_vec(),
    }
}

#[async_trait]
impl<S: Storage + Send + Sync + 'static> AggregatorHandler for ProxyDiscoveryHandler<S> {
    async fn serve(&self, group: &str, version: &str, req: HandlerRequest) -> HandlerResponse {
        let target =
            match resolve_aggregator_target_with_storage(self.storage.as_ref(), group, version)
                .await
            {
                Ok(Some(t)) => t,
                Ok(None) => return unavailable("service unavailable"),
                Err(resp) => {
                    let status = resp.status().as_u16();
                    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                        .await
                        .unwrap_or_default();
                    return HandlerResponse {
                        status,
                        content_type: None,
                        etag: None,
                        body: body.to_vec(),
                    };
                }
            };
        let Some(client) = build_aggregator_client(&target) else {
            return unavailable("aggregator client error");
        };
        let url_host = target.server_name.as_deref().unwrap_or(&target.host);
        let url = format!(
            "{}://{}:{}{}",
            target.scheme, url_host, target.port, req.path
        );
        let mut b = client
            .get(&url)
            .header("Accept", &req.accept)
            .header("X-Remote-User", &req.user);
        for g in &req.groups {
            b = b.header("X-Remote-Group", g);
        }
        if let Some(etag) = &req.if_none_match {
            b = b.header("If-None-Match", etag);
        }
        match b.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let header = |n: &str| {
                    resp.headers()
                        .get(n)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string)
                };
                let content_type = header("content-type");
                let etag = header("etag");
                let body = resp.bytes().await.map(|b| b.to_vec()).unwrap_or_default();
                HandlerResponse {
                    status,
                    content_type,
                    etag,
                    body,
                }
            }
            Err(e) => unavailable(&format!("aggregated API server unavailable: {e}")),
        }
    }
}
