/// OpenAPI specification handler
use crate::openapi::generate_openapi_spec;
use crate::state::ApiServerState;
use tracing::info;

use crate::gnostic;
use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::Response,
};
use rusternetes_storage::{build_prefix, Storage};
use std::sync::Arc;

/// Encode a u64 as a protobuf varint
pub fn encode_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        } else {
            buf.push(byte | 0x80);
        }
    }
}

/// Content type emitted for the v3 protobuf response.
/// Client-go negotiates this via the matching Accept header when calling
/// `OpenAPIV3Client.Paths()`; we use a substring match on `proto-openapi.spec.v3`
/// so quality values / fallbacks like `, application/json` still resolve.
/// K8s ref: staging/src/k8s.io/client-go/openapi3/root.go
const V3_PROTO_CONTENT_TYPE: &str = "application/com.github.proto-openapi.spec.v3@v1.0+protobuf";

/// Does the Accept header request the gnostic v3 proto envelope?
/// Substring match keeps the check lenient w.r.t. quality values and the
/// `application/json` fallback that client-go always tacks on.
fn wants_v3_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.contains("proto-openapi.spec.v3"))
        .unwrap_or(false)
}

/// GET /openapi/v3
/// Get the OpenAPI v3 root document listing available paths.
///
/// Dynamically includes CRD group/version paths so kubectl can discover
/// CRD schemas via the OpenAPI v3 discovery mechanism.
///
/// The root group-list document is JSON-only — its shape is
/// `OpenAPIV3Discovery { paths: map<string, OpenAPIV3DiscoveryGroupVersion> }`,
/// which doesn't have a counterpart in the gnostic openapi.v3.Document proto
/// schema (Document is the per-sub-document spec). client-go's
/// `OpenAPIV3Root.Paths()` reads this as JSON; the proto Accept header only
/// kicks in on the per-group-version sub-documents.
/// K8s ref: staging/src/k8s.io/client-go/openapi3/root.go
pub async fn get_openapi_spec(State(state): State<Arc<ApiServerState>>) -> Response {
    // Return the root document that lists all available OpenAPI paths
    // In real K8s, this returns {"paths": {"/apis/apps/v1": {...}, ...}}
    let mut paths = serde_json::Map::new();
    let path_entry =
        |gv: &str| serde_json::json!({"serverRelativeURL": format!("/openapi/v3/{}", gv)});
    // Group-level documents (`api`, `apis`, `apis/<group>`): the discovery
    // endpoints' own definitions (kube-openapi handler3 publishes them).
    for key in upstream_group_docs().keys() {
        if openapi_path_served(key) {
            paths.insert(key.clone(), path_entry(key));
        }
    }
    paths.insert("api/v1".into(), path_entry("api/v1"));
    // MutatingAdmissionPolicy gate (storage_apiserver.go:187-205).
    if rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::MutatingAdmissionPolicy,
    ) {
        paths.insert(
            "apis/admissionregistration.k8s.io/v1beta1".into(),
            path_entry("apis/admissionregistration.k8s.io/v1beta1"),
        );
    }
    for (group, version) in &[
        ("apps", "v1"),
        ("batch", "v1"),
        ("networking.k8s.io", "v1"),
        ("rbac.authorization.k8s.io", "v1"),
        ("storage.k8s.io", "v1"),
        // StorageVersionMigration (storage_storagemigration.go:54-70),
        // dropped below while the StorageVersionMigrator gate is off.
        ("storagemigration.k8s.io", "v1beta1"),
        ("scheduling.k8s.io", "v1"),
        ("apiextensions.k8s.io", "v1"),
        ("apiregistration.k8s.io", "v1"),
        ("authentication.k8s.io", "v1"),
        ("authorization.k8s.io", "v1"),
        ("admissionregistration.k8s.io", "v1"),
        ("coordination.k8s.io", "v1"),
        ("flowcontrol.apiserver.k8s.io", "v1"),
        ("certificates.k8s.io", "v1"),
        // ClusterTrustBundle (storage_certificates.go:95-100)
        ("certificates.k8s.io", "v1beta1"),
        ("certificates.k8s.io", "v1alpha1"),
        ("discovery.k8s.io", "v1"),
        ("node.k8s.io", "v1"),
        ("autoscaling", "v1"),
        ("autoscaling", "v2"),
        ("policy", "v1"),
        ("resource.k8s.io", "v1"),
        ("events.k8s.io", "v1"),
    ] {
        let key = format!("apis/{}/{}", group, version);
        if openapi_path_served(&key) {
            paths.insert(key.clone(), path_entry(&key));
        }
    }

    // Dynamically add CRD group/version paths
    if let Ok(crds) = state
        .storage
        .list::<serde_json::Value>(&build_prefix("customresourcedefinitions", None))
        .await
    {
        for crd in &crds {
            let group = crd
                .pointer("/spec/group")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let versions = crd.pointer("/spec/versions").and_then(|v| v.as_array());
            for version in versions.into_iter().flatten() {
                let served = version
                    .get("served")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if !served {
                    continue;
                }
                let ver = version.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let gv_key = format!("apis/{}/{}", group, ver);
                if !paths.contains_key(&gv_key) {
                    paths.insert(gv_key.clone(), path_entry(&gv_key));
                }
            }
        }
    }

    let root = serde_json::json!({"paths": paths});
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&root).unwrap_or_default()))
        .unwrap()
}

/// GET /openapi/v3/*path
/// Returns the OpenAPI v3 spec for a specific group version.
///
/// Dynamically includes CRD schemas for the requested group/version.
/// When the client sends `Accept: application/com.github.proto-openapi.spec.v3@v1.0+protobuf`
/// (substring `proto-openapi.spec.v3`), the response body is the gnostic
/// `openapi.v3.Document` protobuf bytes; otherwise JSON.
/// K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/customresource_handler.go
/// K8s ref: staging/src/k8s.io/client-go/openapi3
pub async fn get_openapi_spec_path(
    State(state): State<Arc<ApiServerState>>,
    headers: HeaderMap,
    axum::extract::Path(gv_path): axum::extract::Path<String>,
) -> Response {
    // A group version whose storage was never installed has no document.
    if !openapi_path_served(gv_path.trim_matches('/')) {
        return axum::response::IntoResponse::into_response(StatusCode::NOT_FOUND);
    }

    // The group-level discovery documents are upstream's verbatim.
    if let Some(doc) = upstream_group_docs().get(gv_path.trim_matches('/')) {
        return openapi_v3_response(&headers, doc);
    }

    // Cache the static OpenAPI v3 spec — it doesn't change at runtime
    // (CRD definitions are in v2, not v3 for our implementation)
    use std::sync::OnceLock;
    static V3_SPEC: OnceLock<openapiv3::OpenAPI> = OnceLock::new();
    let spec = V3_SPEC.get_or_init(generate_openapi_spec).clone();
    let mut spec_json = serde_json::to_value(&spec).unwrap_or_default();

    // Parse the requested group/version from the path.
    // Paths are like "api/v1" or "apis/apps/v1" or "apis/example.com/v1"
    let (requested_group, requested_version) = parse_gv_path(&gv_path);

    // kube-openapi's handler3 serves one document per group-version whose
    // `components.schemas` hold the generated definitions of its kinds
    // (staging/src/k8s.io/kube-openapi/pkg/handler3/handler.go; the documents
    // are the kube-openapi builder output published at
    // api/openapi-spec/v3/apis__<group>__<version>_openapi.json).
    if let Some(upstream) = upstream_v3_schemas(&requested_group, &requested_version) {
        if let Some(schemas) = spec_json
            .pointer_mut("/components/schemas")
            .and_then(|v| v.as_object_mut())
        {
            for (name, schema) in upstream {
                schemas.insert(name.clone(), schema.clone());
            }
        }
        drop_superseded_stub_schemas(&mut spec_json, upstream);
    }

    // Query storage for CRDs matching this group/version and inject their schemas.
    if let Ok(crds) = state
        .storage
        .list::<serde_json::Value>(&build_prefix("customresourcedefinitions", None))
        .await
    {
        // Build a components/schemas map for CRD definitions
        let schemas = spec_json
            .pointer_mut("/components/schemas")
            .and_then(|v| v.as_object_mut());

        // If components/schemas doesn't exist, create it via the top-level object
        let needs_create = schemas.is_none();
        if needs_create {
            if let Some(obj) = spec_json.as_object_mut() {
                let components = obj
                    .entry("components")
                    .or_insert_with(|| serde_json::json!({}));
                if let Some(comp_obj) = components.as_object_mut() {
                    comp_obj
                        .entry("schemas")
                        .or_insert_with(|| serde_json::json!({}));
                }
            }
        }

        for crd in &crds {
            let group = crd
                .pointer("/spec/group")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let kind = crd
                .pointer("/spec/names/kind")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            // Only include CRDs matching the requested group/version
            if !requested_group.is_empty() && group != requested_group {
                continue;
            }

            let versions = crd.pointer("/spec/versions").and_then(|v| v.as_array());
            for version in versions.into_iter().flatten() {
                let served = version
                    .get("served")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if !served {
                    continue;
                }
                let ver = version.get("name").and_then(|v| v.as_str()).unwrap_or("");

                if !requested_version.is_empty() && ver != requested_version {
                    continue;
                }

                // Build definition key matching K8s ToRESTFriendlyName format:
                // group/version/kind -> reverse group domain parts, join with dots
                let group_parts: Vec<&str> = group.rsplitn(10, '.').collect();
                let def_key = format!("{}.{}.{}", group_parts.join("."), ver, kind);

                // Build the schema from CRD validation. Same publishing
                // semantics as the v2 endpoint — see
                // build_crd_schema_definition for the case analysis.
                let crd_preserves = crd
                    .pointer("/spec/preserveUnknownFields")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let schema_value =
                    build_crd_schema_definition(crd_preserves, version, group, kind, ver);

                // Insert into components/schemas
                if let Some(schemas) = spec_json
                    .pointer_mut("/components/schemas")
                    .and_then(|v| v.as_object_mut())
                {
                    schemas.insert(def_key.clone(), schema_value);
                }

                // Add OpenAPI paths for this CRD resource.
                // kubectl explain requires paths to map resource names to schemas.
                let plural = crd
                    .pointer("/spec/names/plural")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let scope = crd
                    .pointer("/spec/scope")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Namespaced");
                if !plural.is_empty() {
                    let schema_ref =
                        serde_json::json!({"$ref": format!("#/components/schemas/{}", def_key)});
                    let ok_response = serde_json::json!({
                        "description": "OK",
                        "content": {
                            "application/json": {
                                "schema": schema_ref
                            }
                        }
                    });
                    let get_op = serde_json::json!({
                        "operationId": format!("read{}{}{}",
                            if scope == "Namespaced" { "Namespaced" } else { "" }, kind, ver),
                        "responses": { "200": ok_response },
                        "tags": [format!("{}_v1", group.replace('.', "_"))],
                        "x-kubernetes-group-version-kind": {
                            "group": group, "version": ver, "kind": kind
                        }
                    });
                    if let Some(paths) = spec_json
                        .pointer_mut("/paths")
                        .and_then(|v| v.as_object_mut())
                    {
                        if scope == "Namespaced" {
                            let list_path = format!(
                                "/apis/{}/{}/namespaces/{{namespace}}/{}",
                                group, ver, plural
                            );
                            let item_path = format!(
                                "/apis/{}/{}/namespaces/{{namespace}}/{}/{{name}}",
                                group, ver, plural
                            );
                            paths
                                .entry(list_path)
                                .or_insert_with(|| serde_json::json!({"get": get_op}));
                            paths
                                .entry(item_path)
                                .or_insert_with(|| serde_json::json!({"get": get_op}));
                        } else {
                            let list_path = format!("/apis/{}/{}/{}", group, ver, plural);
                            let item_path = format!("/apis/{}/{}/{}/{{name}}", group, ver, plural);
                            paths
                                .entry(list_path)
                                .or_insert_with(|| serde_json::json!({"get": get_op}));
                            paths
                                .entry(item_path)
                                .or_insert_with(|| serde_json::json!({"get": get_op}));
                        }
                    }
                }
            }
        }

        // The injected CRD schemas reference ObjectMeta via the v2-style
        // `#/definitions/...` ref (build_crd_schema_definition is shared with the
        // v2 endpoint). In the v3 document refs must resolve under
        // `#/components/schemas/...`, and the referenced meta types must exist
        // there — otherwise `kubectl explain <crd>.metadata` shows the field as
        // <ObjectMeta> with no FIELDS (the ref dangles). Add the meta v1 schemas
        // (with their full property set) and rewrite the injected refs.
        if let Some(schemas) = spec_json
            .pointer_mut("/components/schemas")
            .and_then(|v| v.as_object_mut())
        {
            schemas
                .entry("io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta".to_string())
                .or_insert_with(meta_v1_object_meta_schema);
            schemas
                .entry("io.k8s.apimachinery.pkg.apis.meta.v1.OwnerReference".to_string())
                .or_insert_with(meta_v1_owner_reference_schema);
        }
        rewrite_definition_refs_to_components(&mut spec_json);
    }

    openapi_v3_response(&headers, &spec_json)
}

/// Serialize a v3 document as JSON, or as gnostic `openapi.v3.Document`
/// protobuf when the client asks for it.
fn openapi_v3_response(headers: &HeaderMap, spec_json: &serde_json::Value) -> Response {
    let json_bytes = serde_json::to_vec(spec_json).unwrap_or_default();

    if wants_v3_protobuf(headers) {
        match gnostic::openapi_v3_json_to_protobuf(&json_bytes) {
            Ok(proto_bytes) => {
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, V3_PROTO_CONTENT_TYPE)
                    .body(Body::from(proto_bytes))
                    .unwrap();
            }
            Err(e) => {
                info!(
                    "Failed to convert OpenAPI v3 spec to protobuf: {}, falling back to JSON",
                    e
                );
                // Fall through to JSON response
            }
        }
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json_bytes))
        .unwrap()
}

/// Upstream's generated `components.schemas` for a group-version, vendored
/// from release-1.35 `api/openapi-spec/v3` by
/// `scripts/sync-upstream-openapi.sh`. Every group-version the `/openapi/v3`
/// index publishes is served from upstream's definitions.
pub(crate) fn upstream_v3_schemas(
    group: &str,
    version: &str,
) -> Option<&'static serde_json::Map<String, serde_json::Value>> {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    let src: &'static str = match (group, version) {
        ("", "v1") => include_str!("../../openapi/upstream/v1.35/api__v1.schemas.json"),
        ("admissionregistration.k8s.io", "v1") => include_str!(
            "../../openapi/upstream/v1.35/admissionregistration.k8s.io__v1.schemas.json"
        ),
        ("admissionregistration.k8s.io", "v1beta1") => include_str!(
            "../../openapi/upstream/v1.35/admissionregistration.k8s.io__v1beta1.schemas.json"
        ),
        ("apiextensions.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/apiextensions.k8s.io__v1.schemas.json")
        }
        ("apiregistration.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/apiregistration.k8s.io__v1.schemas.json")
        }
        ("apps", "v1") => include_str!("../../openapi/upstream/v1.35/apps__v1.schemas.json"),
        ("authentication.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/authentication.k8s.io__v1.schemas.json")
        }
        ("authorization.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/authorization.k8s.io__v1.schemas.json")
        }
        ("autoscaling", "v1") => {
            include_str!("../../openapi/upstream/v1.35/autoscaling__v1.schemas.json")
        }
        ("autoscaling", "v2") => {
            include_str!("../../openapi/upstream/v1.35/autoscaling__v2.schemas.json")
        }
        ("batch", "v1") => include_str!("../../openapi/upstream/v1.35/batch__v1.schemas.json"),
        ("certificates.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/certificates.k8s.io__v1.schemas.json")
        }
        ("certificates.k8s.io", "v1alpha1") => {
            include_str!("../../openapi/upstream/v1.35/certificates.k8s.io__v1alpha1.schemas.json")
        }
        ("certificates.k8s.io", "v1beta1") => {
            include_str!("../../openapi/upstream/v1.35/certificates.k8s.io__v1beta1.schemas.json")
        }
        ("coordination.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/coordination.k8s.io__v1.schemas.json")
        }
        ("discovery.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/discovery.k8s.io__v1.schemas.json")
        }
        ("events.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/events.k8s.io__v1.schemas.json")
        }
        ("flowcontrol.apiserver.k8s.io", "v1") => include_str!(
            "../../openapi/upstream/v1.35/flowcontrol.apiserver.k8s.io__v1.schemas.json"
        ),
        ("networking.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/networking.k8s.io__v1.schemas.json")
        }
        ("node.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/node.k8s.io__v1.schemas.json")
        }
        ("policy", "v1") => include_str!("../../openapi/upstream/v1.35/policy__v1.schemas.json"),
        ("rbac.authorization.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/rbac.authorization.k8s.io__v1.schemas.json")
        }
        ("resource.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/resource.k8s.io__v1.schemas.json")
        }
        ("scheduling.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/scheduling.k8s.io__v1.schemas.json")
        }
        ("storagemigration.k8s.io", "v1beta1") => include_str!(
            "../../openapi/upstream/v1.35/storagemigration.k8s.io__v1beta1.schemas.json"
        ),
        ("storage.k8s.io", "v1") => {
            include_str!("../../openapi/upstream/v1.35/storage.k8s.io__v1.schemas.json")
        }
        _ => return None,
    };
    // One parsed copy per group-version, leaked so callers get `'static`.
    type Schemas = serde_json::Map<String, serde_json::Value>;
    static CACHE: OnceLock<Mutex<HashMap<(String, String), &'static Schemas>>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(Default::default).lock().ok()?;
    let key = (group.to_string(), version.to_string());
    if let Some(hit) = cache.get(&key) {
        return Some(hit);
    }
    let parsed: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(src).unwrap_or_default();
    let leaked: &'static _ = Box::leak(Box::new(parsed));
    cache.insert(key, leaked);
    Some(leaked)
}

/// Remove the hand-written stub schemas (`generate_openapi_spec`, keyed by the
/// bare kind name) whose group-version-kind the vendored upstream document
/// defines, and point every `$ref` to them at the upstream definition: the
/// published document must hold one definition per GVK, as kube-openapi's
/// builder produces.
fn drop_superseded_stub_schemas(
    spec: &mut serde_json::Value,
    upstream: &serde_json::Map<String, serde_json::Value>,
) {
    fn gvk_keys(schema: &serde_json::Value) -> Vec<String> {
        schema["x-kubernetes-group-version-kind"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|g| {
                let f = |k: &str| g[k].as_str().unwrap_or_default();
                format!("{}/{}/{}", f("group"), f("version"), f("kind"))
            })
            .collect()
    }
    let by_gvk: std::collections::HashMap<String, &str> = upstream
        .iter()
        .flat_map(|(name, schema)| {
            gvk_keys(schema)
                .into_iter()
                .map(move |k| (k, name.as_str()))
        })
        .collect();
    let Some(schemas) = spec
        .pointer_mut("/components/schemas")
        .and_then(|v| v.as_object_mut())
    else {
        return;
    };
    let mut renames: std::collections::HashMap<String, String> = Default::default();
    for (name, schema) in schemas.iter() {
        if upstream.contains_key(name) {
            continue;
        }
        if let Some(target) = gvk_keys(schema).iter().find_map(|k| by_gvk.get(k)) {
            renames.insert(
                format!("#/components/schemas/{name}"),
                format!("#/components/schemas/{target}"),
            );
        }
    }
    if renames.is_empty() {
        return;
    }
    for old in renames.keys() {
        schemas.remove(old.rsplit('/').next().unwrap_or(old));
    }
    fn walk(v: &mut serde_json::Value, renames: &std::collections::HashMap<String, String>) {
        match v {
            serde_json::Value::Object(m) => {
                if let Some(serde_json::Value::String(r)) = m.get_mut("$ref") {
                    if let Some(n) = renames.get(r.as_str()) {
                        *r = n.clone();
                    }
                }
                m.values_mut().for_each(|c| walk(c, renames));
            }
            serde_json::Value::Array(a) => a.iter_mut().for_each(|c| walk(c, renames)),
            _ => {}
        }
    }
    walk(spec, &renames);
}

/// Upstream's group-level v3 documents keyed by their index path (`api`,
/// `apis`, `apis/<group>`), vendored by `scripts/sync-upstream-openapi.sh`.
fn upstream_group_docs() -> &'static serde_json::Map<String, serde_json::Value> {
    use std::sync::OnceLock;
    static DOCS: OnceLock<serde_json::Map<String, serde_json::Value>> = OnceLock::new();
    DOCS.get_or_init(|| {
        serde_json::from_str(include_str!("../../openapi/upstream/v1.35/group_docs.json"))
            .unwrap_or_default()
    })
}

/// Upstream v2 (swagger) definitions for every group-version the server
/// publishes, with their transitive `$ref` closure (release-1.35
/// `api/openapi-spec/swagger.json`, vendored by
/// `scripts/sync-upstream-openapi.sh`).
fn upstream_v2_definitions() -> &'static serde_json::Map<String, serde_json::Value> {
    use std::sync::OnceLock;
    static DEFS: OnceLock<serde_json::Map<String, serde_json::Value>> = OnceLock::new();
    DEFS.get_or_init(|| {
        serde_json::from_str(include_str!(
            "../../openapi/upstream/v1.35/v2.definitions.json"
        ))
        .unwrap_or_default()
    })
}

/// Whether the `/openapi/v3` document at `path` (`apis/<group>[/<version>]`)
/// is published. `storagemigration.k8s.io/v1beta1` has a single resource,
/// installed only while the `StorageVersionMigrator` gate is on
/// (pkg/registry/storagemigration/rest/storage_storagemigration.go:41-43,
/// :54-70), and upstream drops a group version with no storage, so its
/// documents go with it (the same gate `discovery` applies).
fn openapi_path_served(path: &str) -> bool {
    if path == "apis/storagemigration.k8s.io" || path.starts_with("apis/storagemigration.k8s.io/") {
        return rusternetes_common::feature_gates::enabled(
            rusternetes_common::feature_gates::Feature::StorageVersionMigrator,
        );
    }
    true
}

/// Parse the group and version from an OpenAPI v3 path.
/// Examples:
///   "api/v1" -> ("", "v1")                    (core API)
///   "apis/apps/v1" -> ("apps", "v1")
///   "apis/example.com/v1" -> ("example.com", "v1")
fn parse_gv_path(path: &str) -> (String, String) {
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match parts.as_slice() {
        ["api", version] => (String::new(), version.to_string()),
        ["apis", group, version] => (group.to_string(), version.to_string()),
        _ => (String::new(), String::new()),
    }
}

/// Wrap JSON bytes in the Kubernetes protobuf wire format.
///
/// Uses the Go runtime.Unknown proto definition field numbering:
/// - 4 bytes magic: "k8s\0"
/// - Protobuf message with:
///   - field 1 (TypeMeta, nested): empty, omitted
///   - field 2 (raw, bytes): the raw data (JSON spec) -- tag 0x12
///   - field 3 (contentEncoding, string): empty, omitted
///   - field 4 (contentType, string): "application/json" -- tag 0x22
pub fn wrap_in_k8s_protobuf(_content_type: &str, data: &[u8]) -> Vec<u8> {
    let content_type_bytes = b"application/json";
    let mut msg = Vec::with_capacity(data.len() + 30);

    // Field 2: raw bytes (the JSON payload) -- tag = (2 << 3) | 2 = 0x12
    msg.push(0x12);
    encode_varint(&mut msg, data.len() as u64);
    msg.extend_from_slice(data);
    // Field 4: contentType -- tag = (4 << 3) | 2 = 0x22
    msg.push(0x22);
    encode_varint(&mut msg, content_type_bytes.len() as u64);
    msg.extend_from_slice(content_type_bytes);

    let mut buf = Vec::with_capacity(msg.len() + 4);
    buf.extend_from_slice(b"k8s\0");
    buf.extend_from_slice(&msg);
    buf
}

/// GET /openapi/v2 and /swagger.json
/// Returns an OpenAPI v2 (Swagger) specification.
///
/// Supports both protobuf and JSON Accept headers.
/// When protobuf is requested, wraps JSON in the K8s protobuf envelope and
/// responds with the MIME-safe content type (using '.' not '@').
/// See k8s.io/kube-openapi/pkg/handler for the canonical implementation.
///
/// Dynamically includes CRD validation schemas in the definitions section.
pub async fn get_swagger_spec(
    State(state): State<Arc<ApiServerState>>,
    headers: HeaderMap,
) -> Response {
    // Read CRDs from storage as raw JSON to preserve nested schemas.
    // Using typed deserialization (CustomResourceDefinition) loses nested
    // schemas in JSONSchemaPropsOrArray untagged enums. Raw JSON preserves
    // everything.
    //
    // We rebuild the spec on every request from a fresh storage list so that
    // CRD create/update/delete events are reflected immediately. This matches
    // upstream kube-apiserver's openapi controller behaviour where the
    // published spec is regenerated whenever a CRD is added, removed, or has
    // its served versions / schema changed.
    //
    // K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/customresource_handler.go
    let crds = state
        .storage
        .list::<serde_json::Value>(&build_prefix("customresourcedefinitions", None))
        .await
        .unwrap_or_default();

    let spec = build_swagger_spec_for_crds(&crds);
    let total_definitions = spec
        .get("definitions")
        .and_then(|d| d.as_object())
        .map(|m| m.len())
        .unwrap_or(0);
    // Subtract the baseline definitions (ObjectMeta + OwnerReference + built-in
    // core/v1 GVK stubs) that are always present to surface the CRD-derived
    // count. The exact count is owned by `core_v1_builtin_definitions` plus
    // the two apimachinery types above; recompute it from the same source so
    // this stays in sync if either list changes.
    let baseline = 2 + core_v1_builtin_definitions().len();
    if total_definitions > baseline {
        info!(
            "OpenAPI /v2: serving swagger spec with {} CRD definitions",
            total_definitions - baseline
        );
    }

    let json_bytes = serde_json::to_vec(&spec).unwrap_or_default();

    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Cache the protobuf conversion to avoid regenerating on every request.
    // The swagger spec changes when CRDs are created/deleted. We use a simple
    // hash of the JSON bytes to detect changes.
    use std::sync::Mutex;
    use std::sync::OnceLock;
    static PROTO_CACHE: OnceLock<Mutex<(u64, Vec<u8>)>> = OnceLock::new();
    let cache = PROTO_CACHE.get_or_init(|| Mutex::new((0, Vec::new())));

    let wants_protobuf = accept.contains("proto-openapi.spec.v2");
    if wants_protobuf {
        // Simple hash of JSON bytes for cache invalidation
        let hash = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            json_bytes.hash(&mut hasher);
            hasher.finish()
        };
        // Check cache
        {
            let cached = cache.lock().unwrap();
            if cached.0 == hash && !cached.1.is_empty() {
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(
                        header::CONTENT_TYPE,
                        "application/com.github.proto-openapi.spec.v2.v1.0+protobuf",
                    )
                    .body(Body::from(cached.1.clone()))
                    .unwrap();
            }
        }
        match gnostic::swagger_json_to_protobuf(&json_bytes) {
            Ok(proto_bytes) => {
                // Cache for future requests
                {
                    let mut cached = cache.lock().unwrap();
                    *cached = (hash, proto_bytes.clone());
                }
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(
                        header::CONTENT_TYPE,
                        "application/com.github.proto-openapi.spec.v2.v1.0+protobuf",
                    )
                    .body(Body::from(proto_bytes))
                    .unwrap();
            }
            Err(e) => {
                info!(
                    "Failed to convert swagger to protobuf: {}, falling back to JSON",
                    e
                );
                // Fall through to JSON response
            }
        }
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json_bytes))
        .unwrap()
}

/// Build a complete swagger v2 spec given a list of CRDs (as raw JSON).
///
/// Centralising this in a pure function means the openapi/v2 endpoint always
/// reflects the current storage state and the behaviour is independently
/// testable. Matches upstream kube-apiserver's openapi controller: rebuild
/// the spec from the latest set of registered CRDs on every refresh, dropping
/// schemas for versions that are no longer served or no longer present.
///
/// K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder/builder.go
pub fn build_swagger_spec_for_crds(crds: &[serde_json::Value]) -> serde_json::Value {
    let mut paths = serde_json::Map::new();
    let mut definitions = serde_json::Map::new();

    for crd in crds {
        let group = crd
            .pointer("/spec/group")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let plural = crd
            .pointer("/spec/names/plural")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let kind = crd
            .pointer("/spec/names/kind")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let scope = crd
            .pointer("/spec/scope")
            .and_then(|v| v.as_str())
            .unwrap_or("Namespaced");

        // CRD-level preserveUnknownFields applies to all versions unless
        // overridden at the schema level. K8s ref: builder.go:392-407
        let crd_preserves = crd
            .pointer("/spec/preserveUnknownFields")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let versions = crd.pointer("/spec/versions").and_then(|v| v.as_array());
        for version in versions.into_iter().flatten() {
            // Skip versions that are not served — they must NOT appear in the
            // published OpenAPI spec. Conformance test
            // "removes definition from spec when one version gets changed to
            // not be served" depends on this filter being applied at
            // publish-time so toggling served=false is reflected.
            let served = version
                .get("served")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !served {
                continue;
            }
            let ver = version.get("name").and_then(|v| v.as_str()).unwrap_or("");
            if ver.is_empty() || group.is_empty() || kind.is_empty() {
                continue;
            }

            // Build definition key like "io.example.stable.v1.CronTab"
            // (reverse-domain group + version + kind), matching K8s
            // ToRESTFriendlyName format.
            let group_parts: Vec<&str> = group.rsplitn(10, '.').collect();
            let def_key = format!("{}.{}.{}", group_parts.join("."), ver, kind);

            let schema_value =
                build_crd_schema_definition(crd_preserves, version, group, kind, ver);
            info!(
                "OpenAPI: publishing CRD definition {} (group={}, kind={}, version={})",
                def_key, group, kind, ver
            );
            definitions.insert(def_key.clone(), schema_value);

            // Add path entries for the CRD's API endpoints.
            let base_path = format!("/apis/{}/{}", group, ver);
            if scope == "Namespaced" {
                let ns_path = format!("{}/namespaces/{{namespace}}/{}", base_path, plural);
                let ns_item_path = format!("{}/{{name}}", ns_path);
                paths.insert(
                    ns_path,
                    serde_json::json!({
                        "get": {"description": format!("list {}", kind)},
                        "post": {"description": format!("create {}", kind)}
                    }),
                );
                paths.insert(
                    ns_item_path,
                    serde_json::json!({
                        "get": {"description": format!("get {}", kind)},
                        "put": {"description": format!("update {}", kind)},
                        "delete": {"description": format!("delete {}", kind)}
                    }),
                );
            } else {
                let cluster_path = format!("{}/{}", base_path, plural);
                let cluster_item_path = format!("{}/{{name}}", cluster_path);
                paths.insert(
                    cluster_path,
                    serde_json::json!({
                        "get": {"description": format!("list {}", kind)},
                        "post": {"description": format!("create {}", kind)}
                    }),
                );
                paths.insert(
                    cluster_item_path,
                    serde_json::json!({
                        "get": {"description": format!("get {}", kind)},
                        "put": {"description": format!("update {}", kind)},
                        "delete": {"description": format!("delete {}", kind)}
                    }),
                );
            }
        }
    }

    // Add standard K8s definitions referenced by CRD schemas.
    // CRD schemas use $ref to io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
    // so it must exist in the definitions section.
    // K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder/builder.go
    definitions.insert(
        "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta".to_string(),
        serde_json::json!({
            "description": "ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.",
            "type": "object",
            "properties": {
                "name": { "type": "string", "description": "Name must be unique within a namespace." },
                "namespace": { "type": "string", "description": "Namespace defines the space within which each name must be unique." },
                "labels": { "type": "object", "additionalProperties": { "type": "string" }, "description": "Map of string keys and values that can be used to organize and categorize objects." },
                "annotations": { "type": "object", "additionalProperties": { "type": "string" }, "description": "Annotations is an unstructured key value map stored with a resource." },
                "uid": { "type": "string", "description": "UID is the unique in time and space value for this object." },
                "resourceVersion": { "type": "string", "description": "An opaque value that represents the internal version of this object." },
                "generation": { "type": "integer", "format": "int64", "description": "A sequence number representing a specific generation of the desired state." },
                "creationTimestamp": { "type": "string", "format": "date-time", "description": "CreationTimestamp is a timestamp representing the server time when this object was created." },
                "deletionTimestamp": { "type": "string", "format": "date-time", "description": "DeletionTimestamp is RFC 3339 date and time at which this resource will be deleted." },
                "deletionGracePeriodSeconds": { "type": "integer", "format": "int64", "description": "Number of seconds allowed for this object to gracefully terminate." },
                "ownerReferences": { "type": "array", "items": { "$ref": "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.OwnerReference" } },
                "finalizers": { "type": "array", "items": { "type": "string" } },
                "managedFields": { "type": "array", "items": { "type": "object" } }
            }
        }),
    );
    definitions.insert(
        "io.k8s.apimachinery.pkg.apis.meta.v1.OwnerReference".to_string(),
        serde_json::json!({
            "description": "OwnerReference contains enough information to let you identify an owning object.",
            "type": "object",
            "required": ["apiVersion", "kind", "name", "uid"],
            "properties": {
                "apiVersion": { "type": "string" },
                "kind": { "type": "string" },
                "name": { "type": "string" },
                "uid": { "type": "string" },
                "controller": { "type": "boolean" },
                "blockOwnerDeletion": { "type": "boolean" }
            }
        }),
    );

    // Built-in core/v1 GVK schemas.
    //
    // Upstream kube-apiserver publishes one definition per built-in GVK via
    // kube-openapi's `pkg/builder`, sourced from `types_swagger_doc_generated.go`
    // that's codegen'd from the Go struct comments. Rusternetes does not have
    // codegen for this; we publish hand-written stubs so that:
    //   * kubectl explain / discovery sees the GVK + standard metadata layout
    //   * conformance assertions that check for `io.k8s.api.core.v1.Pod`
    //     (and other built-in keys) in the `definitions` map pass.
    //
    // K8s ref: staging/src/k8s.io/kube-openapi/pkg/builder/openapi.go
    //          staging/src/k8s.io/api/core/v1/types_swagger_doc_generated.go
    for (key, def) in core_v1_builtin_definitions() {
        definitions.insert(key, def);
    }
    // Upstream's generated definitions replace the stubs above (kube-openapi
    // `pkg/builder` output published as api/openapi-spec/swagger.json); the
    // stubs only remain for kinds upstream has no definition for.
    for (key, def) in upstream_v2_definitions() {
        // Definitions of an unserved group version are not published (see
        // [`openapi_path_served`]).
        // `DeleteOptions` / `WatchEvent` list every group version: they stay
        // while any of theirs is served.
        let gvks = def["x-kubernetes-group-version-kind"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default();
        let unserved = !gvks.is_empty()
            && gvks.iter().all(|g| {
                !openapi_path_served(&format!(
                    "apis/{}/{}",
                    g["group"].as_str().unwrap_or_default(),
                    g["version"].as_str().unwrap_or_default()
                ))
            });
        if !unserved {
            definitions.insert(key.clone(), def.clone());
        }
    }

    serde_json::json!({
        "swagger": "2.0",
        "info": {
            "title": "Rusternetes Kubernetes API",
            "version": "v1.35.0"
        },
        "paths": paths,
        "definitions": definitions
    })
}

/// Hand-written stub schemas for the most commonly referenced built-in
/// `io.k8s.api.core.v1` (and a few sibling) GVKs.
///
/// Upstream Go publishes per-GVK schemas generated from struct comments via
/// `kube-openapi/pkg/builder` + `types_swagger_doc_generated.go`. Rusternetes
/// doesn't have that codegen pipeline yet; we instead inline minimal stubs
/// here so discovery clients can resolve the canonical definition keys.
///
/// Each definition carries:
///   * the `x-kubernetes-group-version-kind` vendor extension (the marker the
///     Go conformance tests assert on for built-in resources), and
///   * the standard `apiVersion` / `kind` / `metadata` properties, with
///     `metadata` `$ref`'ing the existing ObjectMeta definition.
///
/// Returns an ordered list of `(definition_key, schema)` pairs. Keys must use
/// the dotted format (`io.k8s.api.core.v1.Pod`) matching upstream
/// `ToRESTFriendlyName`. `spec` / `status` are intentionally typed as
/// permissive `{type: object}` for now — fully fleshing them out requires
/// per-field codegen that's a follow-up.
fn core_v1_builtin_definitions() -> Vec<(String, serde_json::Value)> {
    /// One built-in resource: `(group, version, kind, description)`.
    /// For core types the group is the empty string, matching the Go GVK.
    const BUILT_INS: &[(&str, &str, &str, &str)] = &[
        ("", "v1", "Pod", "Pod is a collection of containers that can run on a host."),
        ("", "v1", "Service", "Service is a named abstraction of software service consisting of local port (for example 3306) that the proxy listens on, and the selector that determines which pods will answer requests sent through the proxy."),
        ("", "v1", "Node", "Node is a worker node in Kubernetes."),
        ("", "v1", "Namespace", "Namespace provides a scope for Names."),
        ("", "v1", "ConfigMap", "ConfigMap holds configuration data for pods to consume."),
        ("", "v1", "Secret", "Secret holds secret data of a certain type."),
        ("", "v1", "ServiceAccount", "ServiceAccount binds together: a name, understood by users, and perhaps by peripheral systems, for an identity; a principal that can be authenticated and authorized; a set of secrets."),
        ("", "v1", "PersistentVolume", "PersistentVolume (PV) is a storage resource provisioned by an administrator."),
        ("", "v1", "PersistentVolumeClaim", "PersistentVolumeClaim is a user's request for and claim to a persistent volume."),
        ("", "v1", "Event", "Event is a report of an event somewhere in the cluster."),
        ("", "v1", "Endpoints", "Endpoints is a collection of endpoints that implement the actual service."),
        ("", "v1", "ReplicationController", "ReplicationController represents the configuration of a replication controller."),
        ("apps", "v1", "Deployment", "Deployment enables declarative updates for Pods and ReplicaSets."),
        ("apps", "v1", "ReplicaSet", "ReplicaSet ensures that a specified number of pod replicas are running at any given time."),
        ("apps", "v1", "StatefulSet", "StatefulSet represents a set of pods with consistent identities."),
        ("apps", "v1", "DaemonSet", "DaemonSet represents the configuration of a daemon set."),
        ("batch", "v1", "Job", "Job represents the configuration of a single job."),
        ("batch", "v1", "CronJob", "CronJob represents the configuration of a single cron job."),
        // admissionregistration.k8s.io/v1 (k8s.io/api/admissionregistration/v1/types.go)
        ("admissionregistration.k8s.io", "v1", "MutatingWebhookConfiguration", "MutatingWebhookConfiguration describes the configuration of and admission webhook that accept or reject and may change the object."),
        ("admissionregistration.k8s.io", "v1", "ValidatingWebhookConfiguration", "ValidatingWebhookConfiguration describes the configuration of and admission webhook that accept or reject and object without changing it."),
        ("admissionregistration.k8s.io", "v1", "ValidatingAdmissionPolicy", "ValidatingAdmissionPolicy describes the definition of an admission validation policy that accepts or rejects an object without changing it."),
        ("admissionregistration.k8s.io", "v1", "ValidatingAdmissionPolicyBinding", "ValidatingAdmissionPolicyBinding binds the ValidatingAdmissionPolicy with paramerized resources."),
    ];

    BUILT_INS
        .iter()
        .map(|(group, version, kind, description)| {
            let api_group_dotted = if group.is_empty() {
                // core/v1 lives under io.k8s.api.core.v1
                "io.k8s.api.core".to_string()
            } else {
                // sibling groups live under io.k8s.api.<group>; non-domain
                // groups like "apps" and "batch" match upstream's flat layout.
                // Upstream ToRESTFriendlyName keeps only the leading label of a
                // domain-style group (`admissionregistration.k8s.io` ->
                // `io.k8s.api.admissionregistration`).
                format!("io.k8s.api.{}", group.split('.').next().unwrap_or(group))
            };
            let key = format!("{}.{}.{}", api_group_dotted, version, kind);

            let gvk_version = serde_json::json!([{
                "group": *group,
                "version": *version,
                "kind": *kind,
            }]);
            let mut def = serde_json::json!({
                "description": *description,
                "type": "object",
                "properties": {
                    "apiVersion": {
                        "type": "string",
                        "description": "APIVersion defines the versioned schema of this representation of an object. Servers should convert recognized schemas to the latest internal value, and may reject unrecognized values. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources"
                    },
                    "kind": {
                        "type": "string",
                        "description": "Kind is a string value representing the REST resource this object represents. Servers may infer this from the endpoint the client submits requests to. Cannot be updated. In CamelCase. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds"
                    },
                    "metadata": {
                        "$ref": "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
                        "description": "Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata"
                    },
                    "spec": { "type": "object", "description": "Specification of the desired behavior of the resource." },
                    "status": { "type": "object", "description": "Most recently observed status of the resource." }
                },
                "x-kubernetes-group-version-kind": gvk_version,
            });

            // Trim spec/status for resources that don't carry them upstream
            // (k8s.io/api/core/v1/types.go): ConfigMap/Secret/Event/Endpoints
            // are flat objects with no nested spec/status; ServiceAccount has
            // top-level secrets/imagePullSecrets but no spec or status.
            if matches!(
                *kind,
                "ConfigMap"
                    | "Secret"
                    | "Event"
                    | "Endpoints"
                    | "ServiceAccount"
                    | "MutatingWebhookConfiguration"
                    | "ValidatingWebhookConfiguration"
            ) {
                if let Some(props) = def
                    .get_mut("properties")
                    .and_then(|p| p.as_object_mut())
                {
                    props.remove("spec");
                    props.remove("status");
                }
            }

            (key, def)
        })
        .collect()
}

/// The meta/v1 ObjectMeta schema, with its full property set, for the
/// OpenAPI v3 `components/schemas` map. `kubectl explain <crd>.metadata`
/// resolves the CRD's `metadata` `$ref` to this and lists its FIELDS
/// (creationTimestamp, name, labels, …). Mirrors the v2 definition emitted by
/// `build_swagger_spec_for_crds`, but ref'd under `#/components/schemas/`.
fn meta_v1_object_meta_schema() -> serde_json::Value {
    serde_json::json!({
        "description": "ObjectMeta is metadata that all persisted resources must have, which includes all objects users must create.",
        "type": "object",
        "properties": {
            "name": { "type": "string", "description": "Name must be unique within a namespace." },
            "generateName": { "type": "string", "description": "GenerateName is an optional prefix used by the server to generate a unique name." },
            "namespace": { "type": "string", "description": "Namespace defines the space within which each name must be unique." },
            "selfLink": { "type": "string", "description": "Deprecated: selfLink is a legacy read-only field that is no longer populated by the system." },
            "labels": { "type": "object", "additionalProperties": { "type": "string" }, "description": "Map of string keys and values that can be used to organize and categorize objects." },
            "annotations": { "type": "object", "additionalProperties": { "type": "string" }, "description": "Annotations is an unstructured key value map stored with a resource." },
            "uid": { "type": "string", "description": "UID is the unique in time and space value for this object." },
            "resourceVersion": { "type": "string", "description": "An opaque value that represents the internal version of this object." },
            "generation": { "type": "integer", "format": "int64", "description": "A sequence number representing a specific generation of the desired state." },
            "creationTimestamp": { "type": "string", "format": "date-time", "description": "CreationTimestamp is a timestamp representing the server time when this object was created." },
            "deletionTimestamp": { "type": "string", "format": "date-time", "description": "DeletionTimestamp is RFC 3339 date and time at which this resource will be deleted." },
            "deletionGracePeriodSeconds": { "type": "integer", "format": "int64", "description": "Number of seconds allowed for this object to gracefully terminate." },
            "ownerReferences": { "type": "array", "items": { "$ref": "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.OwnerReference" }, "description": "List of objects depended by this object." },
            "finalizers": { "type": "array", "items": { "type": "string" }, "description": "Must be empty before the object is deleted from the registry." }
        }
    })
}

/// The meta/v1 OwnerReference schema for `components/schemas` (referenced by
/// [`meta_v1_object_meta_schema`]).
fn meta_v1_owner_reference_schema() -> serde_json::Value {
    serde_json::json!({
        "description": "OwnerReference contains enough information to let you identify an owning object.",
        "type": "object",
        "required": ["apiVersion", "kind", "name", "uid"],
        "properties": {
            "apiVersion": { "type": "string", "description": "API version of the referent." },
            "kind": { "type": "string", "description": "Kind of the referent." },
            "name": { "type": "string", "description": "Name of the referent." },
            "uid": { "type": "string", "description": "UID of the referent." },
            "controller": { "type": "boolean", "description": "If true, this reference points to the managing controller." },
            "blockOwnerDeletion": { "type": "boolean", "description": "If true, AND if the owner has the \"foregroundDeletion\" finalizer, then the owner cannot be deleted from the key-value store until this reference is removed." }
        }
    })
}

/// Rewrite every `$ref` of the form `#/definitions/<X>` to
/// `#/components/schemas/<X>` in-place. The CRD schema builder is shared with
/// the v2 (swagger) endpoint and emits v2-style `#/definitions/` refs; the v3
/// document resolves refs under `#/components/schemas/`. The base v3 spec never
/// uses `#/definitions/`, so this only touches the injected CRD schemas.
fn rewrite_definition_refs_to_components(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            // Recurse into children first (skip the `$ref` string itself).
            for (k, v) in map.iter_mut() {
                if k != "$ref" {
                    rewrite_definition_refs_to_components(v);
                }
            }
            // Rewrite the v2 `#/definitions/` prefix to v3 `#/components/schemas/`.
            if let Some(serde_json::Value::String(s)) = map.get_mut("$ref") {
                if let Some(rest) = s.strip_prefix("#/definitions/") {
                    *s = format!("#/components/schemas/{}", rest);
                }
            }
            // OpenAPI v3: a Schema Object with `$ref` set ignores all sibling
            // keywords (description, etc.) — unlike v2. CRD properties like
            // `metadata` carry both a `$ref` and a `description` ("Standard
            // object's metadata. ..."); without this, `kubectl explain
            // <crd>.metadata` resolves the ref and prints the ObjectMeta TYPE
            // description instead of the field one (conformance:
            // CustomResourcePublishOpenAPI "with validation schema"). Move the
            // `$ref` under `allOf` so the siblings are honored while the ref
            // still expands the fields.
            // (Only a string `$ref` is a reference: JSONSchemaProps has a
            // *property* named `$ref` whose value is a schema object.)
            if map.get("$ref").is_some_and(serde_json::Value::is_string) && map.len() > 1 {
                if let Some(r) = map.remove("$ref") {
                    map.insert("allOf".to_string(), serde_json::json!([{ "$ref": r }]));
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                rewrite_definition_refs_to_components(v);
            }
        }
        _ => {}
    }
}

/// Build the per-version CRD schema definition that's inserted into the spec's
/// definitions map. Handles the three cases that upstream kube-apiserver
/// distinguishes:
///   * preserveUnknownFields (CRD- or schema-level) — replace with a bare object
///   * has openAPIV3Schema — strip Go-omitempty defaults, inject the GVK extension
///   * no schema — emit a minimal object skeleton with the GVK extension
///
/// In all cases the standard metadata/apiVersion/kind properties are added,
/// matching upstream conformance expectations:
/// "works for CRD with validation schema" and
/// "works for CRD without validation schema".
fn build_crd_schema_definition(
    crd_preserves: bool,
    version: &serde_json::Value,
    group: &str,
    kind: &str,
    ver: &str,
) -> serde_json::Value {
    let gvk = serde_json::json!([{
        "group": group,
        "kind": kind,
        "version": ver,
    }]);

    if let Some(schema_val) = version.pointer("/schema/openAPIV3Schema") {
        let schema_preserves = schema_val
            .get("x-kubernetes-preserve-unknown-fields")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if crd_preserves || schema_preserves {
            // Replace entire schema with just {type: object}.
            // K8s ref: builder.go:393-395
            let mut def = serde_json::json!({
                "type": "object",
                "x-kubernetes-group-version-kind": gvk,
            });
            add_standard_crd_properties(&mut def);
            return def;
        }

        let mut cleaned = schema_val.clone();
        strip_false_extensions(&mut cleaned);
        if let Some(obj) = cleaned.as_object_mut() {
            obj.insert("x-kubernetes-group-version-kind".to_string(), gvk);
        }
        add_standard_crd_properties(&mut cleaned);
        return cleaned;
    }

    // No openAPIV3Schema declared. K8s still publishes a definition stub so
    // kubectl explain / discovery sees the GVK. Conformance test
    // "works for CRD without validation schema" depends on the definition
    // being present.
    let mut def = serde_json::json!({
        "type": "object",
        "x-kubernetes-group-version-kind": gvk,
    });
    add_standard_crd_properties(&mut def);
    def
}

/// Add standard K8s properties (metadata, apiVersion, kind) to a CRD schema definition.
/// K8s always adds these to CRD OpenAPI definitions.
/// K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder/builder.go
pub fn add_standard_crd_properties(schema: &mut serde_json::Value) {
    if let Some(obj) = schema.as_object_mut() {
        let properties = obj
            .entry("properties")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(props) = properties.as_object_mut() {
            // K8s CRD definitions reference the standard ObjectMeta definition
            // instead of inlining "type: object". This matches
            // staging/src/k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder/builder.go
            props.entry("metadata".to_string()).or_insert_with(|| {
                serde_json::json!({
                    "$ref": "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
                    "description": "Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata"
                })
            });
            props.entry("apiVersion".to_string()).or_insert_with(|| {
                serde_json::json!({
                    "description": "APIVersion defines the versioned schema of this representation of an object. Servers should convert recognized schemas to the latest internal value, and may reject unrecognized values. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources",
                    "type": "string"
                })
            });
            props.entry("kind".to_string()).or_insert_with(|| {
                serde_json::json!({
                    "description": "Kind is a string value representing the REST resource this object represents. Servers may infer this from the endpoint the client submits requests to. Cannot be updated. In CamelCase. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds",
                    "type": "string"
                })
            });
        }
    }
}

/// Recursively strip default/empty values from a CRD JSON schema to match
/// K8s Go's omitempty behavior. Go omitempty skips false booleans, empty
/// strings, nil pointers, and zero values. Our Rust serialization includes
/// these as explicit values in stored JSON.
///
/// K8s ref: JSONSchemaProps fields in apiextensions/v1/types.go all use
/// `json:",omitempty"` which omits zero values.
pub fn strip_false_extensions(value: &mut serde_json::Value) {
    if let Some(obj) = value.as_object_mut() {
        // K8s v2 conversion: when x-kubernetes-preserve-unknown-fields is true,
        // clear items and properties (kubectl can't handle them).
        // Also clear type if it was "object" with preserve-unknown-fields.
        // K8s ref: controller/openapi/v2/conversion.go:68-89
        if obj.get("x-kubernetes-preserve-unknown-fields") == Some(&serde_json::Value::Bool(true)) {
            obj.remove("items");
            obj.remove("properties");
            // If type is "object" with preserve-unknown-fields, clear it
            if obj.get("type") == Some(&serde_json::json!("object")) {
                obj.remove("type");
            }
        }

        // K8s v2: when nullable is true, clear type, items, properties
        // K8s ref: conversion.go:56-66
        if obj.get("nullable") == Some(&serde_json::Value::Bool(true)) {
            obj.remove("type");
            obj.remove("items");
            obj.remove("properties");
        }

        // Other boolean fields: strip only when false (Go omitempty)
        // x-kubernetes-* booleans are added by toKubeOpenAPI() only when true,
        // so they should be stripped when false but kept when true.
        let false_fields = [
            "exclusiveMaximum",
            "exclusiveMinimum",
            "uniqueItems",
            "nullable",
            "x-kubernetes-embedded-resource",
            "x-kubernetes-int-or-string",
            "x-kubernetes-preserve-unknown-fields",
        ];
        for key in &false_fields {
            if obj.get(*key) == Some(&serde_json::Value::Bool(false)) {
                obj.remove(*key);
            }
        }

        // Fields that should be omitted when empty string (Go omitempty on string)
        let empty_string_fields = [
            "id",
            "$schema",
            "$ref",
            "description",
            "type",
            "format",
            "title",
            "pattern",
            "discriminator",
        ];
        for key in &empty_string_fields {
            if let Some(serde_json::Value::String(s)) = obj.get(*key) {
                if s.is_empty() {
                    obj.remove(*key);
                }
            }
        }

        // NOTE: maximum/minimum/multipleOf/maxLength/minLength/maxItems/minItems/
        // maxProperties/minProperties are deliberately NOT stripped when 0. Upstream
        // they are pointers (`*float64`/`*int64`, `omitempty`), so only nil is
        // omitted and an explicit 0 is serialised.
        // K8s ref: apiextensions/v1/types_jsonschema.go:80-94. Our CRD model
        // (common/resources/crd.rs) is Option<_> and skips None, so an absent
        // field never reaches here as 0 (#2946).

        // Empty arrays should be omitted (Go omitempty on slices)
        let array_fields = ["required", "enum", "allOf", "oneOf", "anyOf"];
        for key in &array_fields {
            if let Some(serde_json::Value::Array(arr)) = obj.get(*key) {
                if arr.is_empty() {
                    obj.remove(*key);
                }
            }
        }

        // Empty objects should be omitted (Go omitempty on maps/structs)
        let empty_obj_fields = [
            "properties",
            "additionalProperties",
            "definitions",
            "patternProperties",
            "dependencies",
            "externalDocs",
        ];
        for key in &empty_obj_fields {
            if let Some(serde_json::Value::Object(m)) = obj.get(*key) {
                if m.is_empty() {
                    obj.remove(*key);
                }
            }
        }

        // Remove "additionalProperties": false — Go omitempty treats
        // the zero-value JSONSchemaPropsOrBool as omitted.
        // K8s only includes additionalProperties when it's explicitly
        // set to a non-default value.
        if obj.get("additionalProperties") == Some(&serde_json::Value::Bool(false)) {
            obj.remove("additionalProperties");
        }

        // Remove "default" when it's null — Go omits nil *JSON fields
        if obj.get("default") == Some(&serde_json::Value::Null) {
            obj.remove("default");
        }

        // Remove "example" when it's null
        if obj.get("example") == Some(&serde_json::Value::Null) {
            obj.remove("example");
        }

        // Remove "x-kubernetes-list-type" and "x-kubernetes-map-type" when empty
        for key in [
            "x-kubernetes-list-type",
            "x-kubernetes-map-type",
            "x-kubernetes-list-map-keys",
        ] {
            match obj.get(key) {
                Some(serde_json::Value::String(s)) if s.is_empty() => {
                    obj.remove(key);
                }
                Some(serde_json::Value::Array(a)) if a.is_empty() => {
                    obj.remove(key);
                }
                Some(serde_json::Value::Null) => {
                    obj.remove(key);
                }
                _ => {}
            }
        }

        // Unwrap JSONSchemaPropsOrArray: K8s CRD schemas store "items" as
        // {"schema": {...}} (Go's JSONSchemaPropsOrArray serialization).
        // OpenAPI v2 expects "items" to be a direct schema object.
        // K8s ref: vendor/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/types_jsonschema.go
        if let Some(items) = obj.get("items") {
            if let Some(items_obj) = items.as_object() {
                if items_obj.len() == 1 && items_obj.contains_key("schema") {
                    if let Some(inner_schema) = items_obj.get("schema") {
                        let unwrapped = inner_schema.clone();
                        obj.insert("items".to_string(), unwrapped);
                    }
                }
            }
        }

        // Recurse into all nested objects/arrays
        let keys: Vec<String> = obj.keys().cloned().collect();
        for key in keys {
            if let Some(v) = obj.get_mut(&key) {
                strip_false_extensions(v);
            }
        }
    } else if let Some(arr) = value.as_array_mut() {
        for item in arr.iter_mut() {
            strip_false_extensions(item);
        }
    }
}
