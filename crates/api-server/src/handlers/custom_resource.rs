//! Custom Resource (CR) API handlers for dynamically created CRDs
//!
//! Create, get, update, patch, delete, deletecollection and the `/status`
//! subresource go through the generic endpoint handlers and
//! [`crate::registry::generic::Store`] with the custom resource strategies
//! ([`crate::registry::apiextensions::customresource`]) — the
//! apiextensions-apiserver's `pkg/registry/customresource` wired into
//! `endpoints/handlers/{create,update,patch,delete}.go` by
//! `pkg/apiserver/customresource_handler.go`. Lists, watches and `/scale` are
//! still served here directly.

#![allow(dead_code)]

use crate::endpoints::handlers::{self as endpoints, RequestScope};
use crate::registry::apiextensions::customresource::{
    new_scale_rest, CustomResourceRest, StrictMode,
};
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, State},
    response::Response,
    Extension, Json,
};
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::{CustomResource, CustomResourceDefinition, Scale},
    schema_validation::SchemaValidator,
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// The CRD serving `plural.group` at `version`, or `NotFound`: upstream's
/// `crdHandler.ServeHTTP` answers a request for a CRD that is missing, whose
/// version is not served, or whose `/status` is not enabled with a 404
/// (customresource_handler.go:299-352).
async fn serving_crd(
    state: &ApiServerState,
    group: &str,
    version: &str,
    plural: &str,
    subresource: Option<&str>,
) -> Result<Arc<CustomResourceDefinition>> {
    let not_found = || {
        rusternetes_common::Error::NotFound(
            "the server could not find the requested resource".to_string(),
        )
    };
    let crd = get_crd_for_resource(state, &format!("{plural}.{group}"))
        .await
        .map_err(|_| not_found())?;
    let served = crd
        .spec
        .versions
        .iter()
        .find(|v| v.name == version)
        .ok_or_else(not_found)?;
    if !served.served {
        return Err(not_found());
    }
    if subresource == Some("status")
        && !served
            .subresources
            .as_ref()
            .is_some_and(|s| s.status.is_some())
    {
        return Err(not_found());
    }
    Ok(Arc::new(crd))
}

/// The `RequestScope` of a custom resource (or its `/status`) at one served
/// version, built from the CRD the way `getOrCreateServingInfoFor` builds the
/// per-version serving info (customresource_handler.go:700-1000).
fn scope(
    state: &ApiServerState,
    crd: &Arc<CustomResourceDefinition>,
    version: &str,
    subresource: Option<&'static str>,
    strict: StrictMode,
) -> RequestScope<CustomResource> {
    let group = crd.spec.group.clone();
    RequestScope {
        kind: GroupVersionKind {
            group: group.clone(),
            version: version.to_string(),
            kind: crd.spec.names.kind.clone(),
        },
        resource: GroupVersionResource {
            group,
            version: version.to_string(),
            resource: crd.spec.names.plural.clone(),
        },
        subresource,
        store: Box::new(CustomResourceRest::new(
            state.storage.clone(),
            crd.clone(),
            version,
            subresource.is_some(),
            strict,
        )),
        apply: Some(crate::ssa::apply_legacy::<CustomResource>),
        convert_to_internal: None,
        patch_conversion: None,
    }
}

fn strict_mode(params: &HashMap<String, String>, patch: bool) -> StrictMode {
    match params.get("fieldValidation").map(String::as_str) {
        Some("Strict") if patch => StrictMode::Patch,
        Some("Strict") => StrictMode::Write,
        _ => StrictMode::Off,
    }
}

/// The patch type of a PATCH. The content-type middleware moves a JSON patch
/// type to `x-original-content-type` so the body extracts as JSON.
fn patch_content_type(headers: &axum::http::HeaderMap) -> String {
    headers
        .get("x-original-content-type")
        .or_else(|| headers.get(axum::http::header::CONTENT_TYPE))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

fn query_params(uri: &axum::http::Uri) -> HashMap<String, String> {
    uri.query()
        .map(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_default()
}

/// Create a new custom resource instance
pub async fn create_custom_resource(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace)): Path<(String, String, String, Option<String>)>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    info!("Creating custom resource {group}/{version}/{plural}");
    let crd = serving_crd(&state, &group, &version, &plural, None).await?;
    endpoints::create_resource(
        &state,
        &scope(&state, &crd, &version, None, strict_mode(&params, false)),
        &auth_ctx.user,
        namespace.as_deref(),
        &params,
        &body,
    )
    .await
}

/// Get a specific custom resource instance
pub async fn get_custom_resource(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    info!("Getting custom resource {group}/{version}/{plural}: {name}");
    let crd = serving_crd(&state, &group, &version, &plural, None).await?;
    endpoints::get_resource(
        &state,
        &scope(&state, &crd, &version, None, StrictMode::Off),
        &auth_ctx.user,
        namespace.as_deref(),
        &name,
        &params,
    )
    .await
}

/// List custom resource instances
pub async fn list_custom_resources(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace)): Path<(String, String, String, Option<String>)>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Result<Json<List<CustomResource>>> {
    debug!("Listing custom resources {}/{}/{}", group, version, plural);

    // Find the CRD for this resource type
    let crd_name = format!("{}.{}", plural, group);
    let crd = get_crd_for_resource(&state, &crd_name).await?;

    // Check authorization
    let attrs = if let Some(ref ns) = namespace {
        RequestAttributes::new(auth_ctx.user.clone(), "list", &plural)
            .with_api_group(&group)
            .with_namespace(ns)
    } else {
        RequestAttributes::new(auth_ctx.user, "list", &plural).with_api_group(&group)
    };

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Reject field selectors that target paths the CRD version did not
    // expose via `x-kubernetes-selectable-fields`. Upstream K8s only allows
    // `metadata.name` / `metadata.namespace` plus the explicitly declared
    // selectable paths.
    if let Some(fs) = params.get("fieldSelector").filter(|s| !s.is_empty()) {
        validate_field_selector_paths(&crd, &version, fs)?;
    }

    // Build storage prefix
    let resource_type = format!("{}_{}", group.replace('.', "_"), plural);
    let prefix = if let Some(ref ns) = namespace {
        build_prefix(&resource_type, Some(ns))
    } else {
        build_prefix(&resource_type, None)
    };

    let mut crs: Vec<CustomResource> = state.storage.list(&prefix).await?;

    // Apply schema defaults on read (K8s "defaulting on read")
    for cr in &mut crs {
        apply_schema_defaults(&crd, &version, cr);
    }

    // Convert any CR whose stored version differs from the request version.
    // `convert_custom_resources` batches Webhook calls into a single
    // ConversionReview round-trip (mirrors upstream non-homogeneous list path).
    crs = crate::conversion::convert_custom_resources(&crd, crs, &version, &state.storage).await?;

    // Filter by field selector (e.g. spec.color=red, driven by the version's
    // x-kubernetes-selectable-fields) and label selector. The shared helper
    // serialises each CR exactly once. Runs AFTER conversion so the selector
    // sees the requested-version field layout.
    crate::handlers::filtering::apply_selectors(&mut crs, &params)?;

    // The list envelope MUST carry the typed list kind + the CRD's
    // group/version (e.g. CertificateList / cert-manager.io/v1), not the
    // generic ("List","v1"). A typed client (controller-runtime, cert-manager)
    // decodes a LIST against its registered scheme; "List"/"v1" isn't
    // registered there, so the list fails ("no kind \"List\" is registered for
    // version \"v1\"") and the informer never populates.
    let list_kind = crd
        .spec
        .names
        .list_kind
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}List", crd.spec.names.kind));
    let list_api_version = format!("{group}/{version}");
    let mut list = List::new(&list_kind, &list_api_version, crs);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list))
}

/// DELETE on a custom-resource collection (`deletecollection`).
///
/// The dynamic client's `DeleteCollection` (used by the
/// `CustomResourceFieldSelectors` conformance test) issues
/// `DELETE /apis/{group}/{version}/namespaces/{ns}/{plural}?fieldSelector=...`.
/// `CustomResourceRest::delete_collection` runs LIST's pipeline (selectable
/// paths -> default -> convert -> label/field selectors) so a field selector
/// on a non-storage version selects against the requested version's layout.
// Called directly by the CRD fallback rather than routed, so the request parts
// arrive as plain arguments instead of extractors.
#[allow(clippy::too_many_arguments)]
pub async fn deletecollection_custom_resources(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    group: String,
    version: String,
    plural: String,
    namespace: Option<String>,
    params: HashMap<String, String>,
    body: Bytes,
) -> Result<Response> {
    info!(
        "DeleteCollection custom resources {}/{}/{} (ns={:?}) params={:?}",
        group, version, plural, namespace, params
    );
    let crd = serving_crd(&state, &group, &version, &plural, None).await?;
    endpoints::delete_collection(
        &state,
        &scope(&state, &crd, &version, None, StrictMode::Off),
        &auth_ctx.user,
        namespace.as_deref(),
        &params,
        &body,
    )
    .await
}

/// Watch custom resources, converting every streamed object to the requested
/// API `version` before field-selector filtering and emission — the watch-side
/// counterpart to [`list_custom_resources`] (which converts at the
/// `convert_custom_resources` call above).
///
/// Without this, a watch on a non-storage version (e.g. `v2` of a CRD whose
/// storage version is `v1`) would field-select against the stored-version
/// layout and emit stored-version objects, breaking the
/// `CustomResourceFieldSelectors` conformance test.
pub async fn watch_custom_resources(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    group: &str,
    version: &str,
    plural: &str,
    namespace: Option<String>,
    watch_params: crate::handlers::watch::WatchParams,
) -> Result<axum::response::Response> {
    let crd_name = format!("{plural}.{group}");
    let crd = get_crd_for_resource(&state, &crd_name).await?;

    // Same selectable-path gate LIST applies: reject selectors on paths the
    // CRD version did not declare via `x-kubernetes-selectable-fields`.
    if let Some(fs) = watch_params
        .field_selector
        .as_deref()
        .filter(|s| !s.is_empty())
    {
        validate_field_selector_paths(&crd, version, fs)?;
    }

    let resource_type = format!("{}_{}", group.replace('.', "_"), plural);

    // Stamp watch bookmarks with the CRD's real Kind + apiVersion (captured
    // before `crd` moves into the converter). The generic resource_type
    // heuristic would mangle a CR's resource_type (e.g.
    // `cert-manager_io_certificates` → `Cert-manager_io_certificate`), and a
    // bad bookmark kind breaks typed-client watch decoding (controller-runtime
    // / cert-manager informers re-list in a hot loop).
    let bookmark_gvk = Some((crd.spec.names.kind.clone(), format!("{group}/{version}")));

    // Per-object converter: stored-version JSON -> requested-version JSON.
    // Best-effort — on any failure the original object passes through unchanged,
    // mirroring how LIST tolerates objects that won't convert.
    let crd = Arc::new(crd);
    let target_version = version.to_string();
    let storage = state.storage.clone();
    let converter: crate::handlers::watch::WatchObjectConverter =
        Arc::new(move |val: serde_json::Value| {
            let crd = crd.clone();
            let target_version = target_version.clone();
            let storage = storage.clone();
            Box::pin(async move {
                let cr: CustomResource = match serde_json::from_value(val.clone()) {
                    Ok(c) => c,
                    Err(_) => return val,
                };
                match crate::conversion::convert_custom_resource(
                    &crd,
                    cr,
                    &target_version,
                    &storage,
                )
                .await
                {
                    Ok(converted) => serde_json::to_value(converted).unwrap_or(val),
                    Err(_) => val,
                }
            })
        });

    match namespace {
        Some(ns) => {
            crate::handlers::watch::watch_namespaced_converted::<CustomResource>(
                state,
                auth_ctx,
                ns,
                &resource_type,
                group,
                watch_params,
                converter,
                bookmark_gvk,
            )
            .await
        }
        None => {
            crate::handlers::watch::watch_cluster_scoped_converted::<CustomResource>(
                state,
                auth_ctx,
                &resource_type,
                group,
                watch_params,
                converter,
                bookmark_gvk,
            )
            .await
        }
    }
}

/// Reject `fieldSelector=<path>=<val>` when `<path>` is neither
/// `metadata.name` / `metadata.namespace` nor declared in the CRD version's
/// `x-kubernetes-selectable-fields`. Mirrors the upstream apiextensions
/// validator that gates which CR paths the apiserver indexes.
pub(crate) fn validate_field_selector_paths(
    crd: &CustomResourceDefinition,
    version: &str,
    field_selector: &str,
) -> Result<()> {
    // Built-in fields always selectable on any resource.
    const BUILTIN: &[&str] = &["metadata.name", "metadata.namespace"];

    // Selectable JSONPaths declared by this CRD version. Strip the leading
    // `.` so `.spec.color` lines up with the dot-notation the FieldSelector
    // parser uses internally.
    let selectable: Vec<&str> = crd
        .spec
        .versions
        .iter()
        .find(|v| v.name == version)
        .and_then(|v| v.selectable_fields.as_ref())
        .map(|sf| {
            sf.iter()
                .map(|f| f.json_path.trim_start_matches('.'))
                .collect()
        })
        .unwrap_or_default();

    let parsed =
        rusternetes_common::field_selector::FieldSelector::parse(field_selector).map_err(|e| {
            rusternetes_common::Error::InvalidResource(format!("Invalid field selector: {e}"))
        })?;

    for requirement in parsed.requirements() {
        let path = requirement.field();
        if BUILTIN.contains(&path) || selectable.contains(&path) {
            continue;
        }
        return Err(rusternetes_common::Error::InvalidResource(format!(
            "field label not supported: {path}"
        )));
    }
    Ok(())
}

/// Update a custom resource instance
pub async fn update_custom_resource(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    info!("Updating custom resource {group}/{version}/{plural}: {name}");
    let crd = serving_crd(&state, &group, &version, &plural, None).await?;
    endpoints::update_resource(
        &state,
        &scope(&state, &crd, &version, None, strict_mode(&params, false)),
        &auth_ctx.user,
        namespace.as_deref(),
        &name,
        &params,
        &body,
    )
    .await
}

/// Read the parts of a PATCH request the endpoint handler needs.
async fn patch_request(
    req: axum::extract::Request,
) -> Result<(HashMap<String, String>, String, Bytes)> {
    let (parts, body) = req.into_parts();
    let body = axum::body::to_bytes(body, usize::MAX).await.map_err(|e| {
        rusternetes_common::Error::BadRequest(format!("Failed to read patch body: {e}"))
    })?;
    Ok((
        query_params(&parts.uri),
        patch_content_type(&parts.headers),
        body,
    ))
}

/// Patch a custom resource instance (JSON Patch, JSON Merge Patch, or
/// server-side apply)
pub async fn patch_custom_resource(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    req: axum::extract::Request,
) -> Result<Response> {
    info!("Patching custom resource {group}/{version}/{plural}: {name}");
    let (params, content_type, body) = patch_request(req).await?;
    let crd = serving_crd(&state, &group, &version, &plural, None).await?;
    endpoints::patch_resource(
        &state,
        &scope(&state, &crd, &version, None, strict_mode(&params, true)),
        &auth_ctx.user,
        namespace.as_deref(),
        &name,
        &params,
        &content_type,
        &body,
    )
    .await
}

/// Patch the status subresource of a custom resource
pub async fn patch_custom_resource_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    req: axum::extract::Request,
) -> Result<Response> {
    info!("Patching custom resource status {group}/{version}/{plural}: {name}");
    let (params, content_type, body) = patch_request(req).await?;
    let crd = serving_crd(&state, &group, &version, &plural, Some("status")).await?;
    endpoints::patch_resource(
        &state,
        &scope(
            &state,
            &crd,
            &version,
            Some("status"),
            strict_mode(&params, true),
        ),
        &auth_ctx.user,
        namespace.as_deref(),
        &name,
        &params,
        &content_type,
        &body,
    )
    .await
}

/// Delete a custom resource instance
pub async fn delete_custom_resource(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    info!("Deleting custom resource {group}/{version}/{plural}: {name}");
    let crd = serving_crd(&state, &group, &version, &plural, None).await?;
    endpoints::delete_resource(
        &state,
        &scope(&state, &crd, &version, None, StrictMode::Off),
        &auth_ctx.user,
        namespace.as_deref(),
        &name,
        &params,
        &body,
    )
    .await
}

/// Helper to get CRD from storage
async fn get_crd_for_resource(
    state: &ApiServerState,
    crd_name: &str,
) -> Result<CustomResourceDefinition> {
    let key = build_key("customresourcedefinitions", None, crd_name);
    state.storage.get(&key).await
}

/// Apply schema defaults from the CRD to a custom resource's spec.
/// Walks the CRD's JSONSchemaProps and for each property with a `default`
/// value, sets it on the CR if the field is missing.
pub(crate) fn apply_schema_defaults(
    crd: &CustomResourceDefinition,
    version: &str,
    cr: &mut CustomResource,
) {
    // Find the version in the CRD
    let crd_version = match crd.spec.versions.iter().find(|v| v.name == version) {
        Some(v) => v,
        None => return,
    };

    // Apply defaults from schema if present
    if let Some(ref validation) = crd_version.schema {
        // The CRD schema's top-level properties typically include "spec", "status", etc.
        // We need to find the "spec" property schema and apply defaults to cr.spec.
        if let Some(ref properties) = validation.open_apiv3_schema.properties {
            if let Some(spec_schema) = properties.get("spec") {
                let spec = cr.spec.get_or_insert_with(|| serde_json::json!({}));
                SchemaValidator::apply_defaults(spec_schema, spec);
            }
        }
        // Also apply defaults at the top level for the whole object
        // (handles cases where the schema defines defaults for top-level fields like "a")
        let mut cr_value = serde_json::to_value(&*cr).unwrap_or_default();
        SchemaValidator::apply_defaults(&validation.open_apiv3_schema, &mut cr_value);
        // Update spec from applied defaults
        if let Some(spec_val) = cr_value.get("spec") {
            cr.spec = Some(spec_val.clone());
        }
        // Update extra fields from applied defaults (top-level fields beyond
        // apiVersion/kind/metadata/spec/status)
        if let Some(obj) = cr_value.as_object() {
            for (k, v) in obj {
                if k != "apiVersion"
                    && k != "kind"
                    && k != "metadata"
                    && k != "spec"
                    && k != "status"
                {
                    cr.extra.insert(k.clone(), v.clone());
                }
            }
        }
    }
}

/// Validate a custom resource against its CRD schema.
///
/// On UPDATE/PATCH callers should use [`validate_custom_resource_with_old`]
/// so transition rules (`oldSelf`) are evaluated against the prior version.
fn validate_custom_resource(
    crd: &CustomResourceDefinition,
    version: &str,
    cr: &CustomResource,
) -> Result<()> {
    validate_custom_resource_with_old(crd, version, cr, None)
}

/// Like [`validate_custom_resource`] but also evaluates `x-kubernetes-validations`
/// transition rules using `old_cr` as the prior version for `oldSelf` bindings.
///
/// When `old_cr` is `Some` (UPDATE/PATCH), validation runs in *ratcheting*
/// mode (KEP-4008): schema and non-transition CEL rule failures whose path
/// resolves to a sub-tree that is unchanged from the prior object — and
/// every parent of which correlates between old and new — are dropped.
/// Transition rules (those that reference `oldSelf`) are never ratcheted.
///
/// Upstream: `apiextensions-apiserver/pkg/apiserver/validation/ratcheting.go`.
pub(crate) fn validate_custom_resource_with_old(
    crd: &CustomResourceDefinition,
    version: &str,
    cr: &CustomResource,
    old_cr: Option<&CustomResource>,
) -> Result<()> {
    match old_cr {
        None => {
            validate_custom_resource_schema(crd, version, cr)?;
            crate::handlers::cel_validation::validate_cr_against_crd(crd, version, cr, None)
        }
        Some(old) => validate_custom_resource_with_ratcheting(crd, version, cr, old),
    }
}

/// Ratcheting-aware validation used on UPDATE/PATCH. Walks each top-level
/// property schema (spec/status/extra) twice:
///
///   1. Schema-issue pass — collects every JSONSchema failure with its
///      structural path, then drops issues whose old/new sub-tree
///      correlates AND is equal.
///   2. CEL-rule pass — evaluates every `x-kubernetes-validations` rule at
///      every reachable schema/data instance, ratcheting the non-transition
///      failures with the same predicate.
///
/// If any unratcheted issue remains, returns the first one as an error so
/// the response carries the offending path & message.
fn validate_custom_resource_with_ratcheting(
    crd: &CustomResourceDefinition,
    version: &str,
    cr: &CustomResource,
    old_cr: &CustomResource,
) -> Result<()> {
    use crate::handlers::cel_validation::evaluate_cr_rules_with_paths;
    use crate::handlers::ratcheting::is_ratcheted;
    use rusternetes_common::schema_validation::ValidationIssue;

    let Some(crd_version) = crd.spec.versions.iter().find(|v| v.name == version) else {
        return Err(rusternetes_common::Error::InvalidResource(format!(
            "Version {} not found in CRD",
            version
        )));
    };
    if !crd_version.served {
        return Err(rusternetes_common::Error::InvalidResource(format!(
            "Version {} is not served",
            version
        )));
    }
    let Some(ref validation) = crd_version.schema else {
        return Ok(());
    };
    let crd_preserves = crd.spec.preserve_unknown_fields == Some(true);
    let schema_preserves = validation
        .open_apiv3_schema
        .x_kubernetes_preserve_unknown_fields
        == Some(true);
    if crd_preserves || schema_preserves {
        // K8s skips structural validation when preserve-unknown-fields is set.
        return Ok(());
    }

    let Some(ref properties) = validation.open_apiv3_schema.properties else {
        return Ok(());
    };

    // For each top-level property, collect schema + CEL failures, ratchet
    // those that resolve to an unchanged correlatable sub-tree.
    let new_extra_specs: HashMap<&str, &serde_json::Value> =
        cr.extra.iter().map(|(k, v)| (k.as_str(), v)).collect();
    let old_extra_specs: HashMap<&str, &serde_json::Value> =
        old_cr.extra.iter().map(|(k, v)| (k.as_str(), v)).collect();

    for (key, prop_schema) in properties {
        let (new_val, old_val) = match key.as_str() {
            "spec" => (cr.spec.as_ref(), old_cr.spec.as_ref()),
            "status" => (cr.status.as_ref(), old_cr.status.as_ref()),
            "metadata" | "apiVersion" | "kind" => continue,
            k => (
                new_extra_specs.get(k).copied(),
                old_extra_specs.get(k).copied(),
            ),
        };
        let Some(new_val) = new_val else { continue };
        let old_val = old_val.cloned();
        let old_val_ref = old_val.as_ref();

        // 1) Schema issues.
        let issues: Vec<ValidationIssue> = SchemaValidator::collect_issues(prop_schema, new_val);
        for issue in issues {
            let ratcheted = match old_val_ref {
                Some(old) => is_ratcheted(prop_schema, old, new_val, &issue.path),
                None => false,
            };
            if !ratcheted {
                return Err(rusternetes_common::Error::InvalidResource(
                    render_issue_message(&issue.path, &issue.message),
                ));
            }
        }

        // 2) CEL rule failures.
        let failures = evaluate_cr_rules_with_paths(prop_schema, new_val, old_val_ref)?;
        for f in failures {
            let ratcheted = if f.is_transition {
                false
            } else if let Some(old) = old_val_ref {
                is_ratcheted(prop_schema, old, new_val, &f.path)
            } else {
                false
            };
            if !ratcheted {
                let path_str = render_full_path(&f.path);
                let msg = if path_str.is_empty() {
                    f.message.clone()
                } else {
                    format!("{}: {}", path_str, f.message)
                };
                return Err(rusternetes_common::Error::InvalidResource(msg));
            }
        }
    }

    Ok(())
}

/// Render the K8s-style path for a sub-tree issue. Object segments under a
/// `additionalProperties` parent would appear as `[key]` in upstream's
/// messages; we render every key with dot notation since paths are
/// structural and the K8s test only matches substrings (`field`,
/// `list[0].field`, etc.).
fn render_full_path(path: &[rusternetes_common::schema_validation::PathSeg]) -> String {
    let mut out = String::new();
    for seg in path {
        match seg {
            rusternetes_common::schema_validation::PathSeg::Key(k) => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(k);
            }
            rusternetes_common::schema_validation::PathSeg::Index(i) => {
                out.push_str(&format!("[{}]", i));
            }
        }
    }
    out
}

fn render_issue_message(
    path: &[rusternetes_common::schema_validation::PathSeg],
    msg: &str,
) -> String {
    let p = render_full_path(path);
    if p.is_empty() || msg.starts_with(&p) {
        msg.to_string()
    } else {
        format!("{}: {}", p, msg)
    }
}

/// Schema-only portion of CR validation, kept as a separate helper so the
/// CEL pass can run after structural validation is known to succeed.
fn validate_custom_resource_schema(
    crd: &CustomResourceDefinition,
    version: &str,
    cr: &CustomResource,
) -> Result<()> {
    // Find the version in the CRD
    let crd_version = crd
        .spec
        .versions
        .iter()
        .find(|v| v.name == version)
        .ok_or_else(|| {
            rusternetes_common::Error::InvalidResource(format!(
                "Version {} not found in CRD",
                version
            ))
        })?;

    // Check if version is served
    if !crd_version.served {
        warn!("Version {} is not served", version);
        return Err(rusternetes_common::Error::InvalidResource(format!(
            "Version {} is not served",
            version
        )));
    }

    // Validate against schema if present.
    // K8s skips structural pruning/validation when CRD has preserveUnknownFields: true
    // (line 1432 in customresource_handler.go). Only metadata coercion runs.
    // Also skip if the schema root has x-kubernetes-preserve-unknown-fields: true.
    let crd_preserves = crd.spec.preserve_unknown_fields == Some(true);
    if let Some(ref validation) = crd_version.schema {
        let schema_preserves = validation
            .open_apiv3_schema
            .x_kubernetes_preserve_unknown_fields
            == Some(true);

        if !crd_preserves && !schema_preserves {
            // Validate ALL top-level properties against the schema, not just spec.
            // K8s validates the entire CR object. Some CRDs define enum fields
            // at the top level (e.g., cronies), not under spec.
            if let Some(ref properties) = validation.open_apiv3_schema.properties {
                // Validate spec if present. Root the error path at "spec" so a
                // missing nested required field reports the upstream-format path
                // (e.g. `spec.bars[0].name: Required value`).
                if let Some(ref spec) = cr.spec {
                    if let Some(spec_schema) = properties.get("spec") {
                        SchemaValidator::validate_no_unknown_check_with_root(
                            spec_schema,
                            spec,
                            "spec",
                        )?;
                    }
                }
                // Validate status if present
                if let Some(ref status) = cr.status {
                    if let Some(status_schema) = properties.get("status") {
                        SchemaValidator::validate_no_unknown_check_with_root(
                            status_schema,
                            status,
                            "status",
                        )?;
                    }
                }
                // Validate extra top-level fields (e.g., cronies, hostPort)
                for (key, value) in &cr.extra {
                    if key == "metadata" || key == "apiVersion" || key == "kind" {
                        continue;
                    }
                    if let Some(field_schema) = properties.get(key) {
                        SchemaValidator::validate_no_unknown_check_with_root(
                            field_schema,
                            value,
                            key,
                        )?;
                    }
                }
            }
        }
        // When preserveUnknownFields is true, skip structural validation.
        // K8s only runs metadata coercion (CoerceWithOptions) in this case.
    }

    Ok(())
}

/// Get the status subresource of a custom resource: `StatusREST.Get` is the
/// store's `Get`, so the whole object comes back (etcd.go:195-198).
pub async fn get_custom_resource_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    info!("Getting custom resource status {group}/{version}/{plural}: {name}");
    let crd = serving_crd(&state, &group, &version, &plural, Some("status")).await?;
    endpoints::get_resource(
        &state,
        &scope(&state, &crd, &version, Some("status"), StrictMode::Off),
        &auth_ctx.user,
        namespace.as_deref(),
        &name,
        &params,
    )
    .await
}

/// Update the status subresource of a custom resource. The request body is
/// the full object; the status strategy persists only its `.status`
/// (status_strategy.go:62-86).
pub async fn update_custom_resource_status(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    info!("Updating custom resource status {group}/{version}/{plural}: {name}");
    let crd = serving_crd(&state, &group, &version, &plural, Some("status")).await?;
    endpoints::update_resource(
        &state,
        &scope(
            &state,
            &crd,
            &version,
            Some("status"),
            strict_mode(&params, false),
        ),
        &auth_ctx.user,
        namespace.as_deref(),
        &name,
        &params,
        &body,
    )
    .await
}

/// The `/scale` `RequestScope`: `autoscaling/v1` `Scale` served as
/// `<plural>/scale` over `ScaleREST` (customresource_handler.go:1005-1040,
/// registry/customresource/etcd.go:129-256). `NotFound` when the version
/// does not enable `scale` (customresource_handler.go:349).
fn scale_scope(
    state: &ApiServerState,
    crd: &Arc<CustomResourceDefinition>,
    version: &str,
) -> Result<RequestScope<Scale>> {
    let store = new_scale_rest(state.storage.clone(), crd.clone(), version).ok_or_else(|| {
        rusternetes_common::Error::NotFound(
            "the server could not find the requested resource".to_string(),
        )
    })?;
    Ok(RequestScope {
        kind: GroupVersionKind {
            group: "autoscaling".to_string(),
            version: "v1".to_string(),
            kind: "Scale".to_string(),
        },
        resource: GroupVersionResource {
            group: crd.spec.group.clone(),
            version: version.to_string(),
            resource: crd.spec.names.plural.clone(),
        },
        subresource: Some("scale"),
        store: Box::new(store),
        apply: Some(crate::ssa::apply_legacy::<Scale>),
        convert_to_internal: None,
        patch_conversion: None,
    })
}

/// Get the scale subresource of a custom resource: `ScaleREST.Get`
/// (etcd.go:157-177).
pub async fn get_custom_resource_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Response> {
    info!("Getting custom resource scale {group}/{version}/{plural}: {name}");
    let crd = serving_crd(&state, &group, &version, &plural, None).await?;
    endpoints::get_resource(
        &state,
        &scale_scope(&state, &crd, &version)?,
        &auth_ctx.user,
        namespace.as_deref(),
        &name,
        &params,
    )
    .await
}

/// Update the scale subresource of a custom resource: `ScaleREST.Update`
/// (etcd.go:179-216).
pub async fn update_custom_resource_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    info!("Updating custom resource scale {group}/{version}/{plural}: {name}");
    let crd = serving_crd(&state, &group, &version, &plural, None).await?;
    endpoints::update_resource(
        &state,
        &scale_scope(&state, &crd, &version)?,
        &auth_ctx.user,
        namespace.as_deref(),
        &name,
        &params,
        &body,
    )
    .await
}

/// Patch the scale subresource of a custom resource: a patch of the Scale
/// into `ScaleREST.Update`.
pub async fn patch_custom_resource_scale(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((group, version, plural, namespace, name)): Path<(
        String,
        String,
        String,
        Option<String>,
        String,
    )>,
    req: axum::extract::Request,
) -> Result<Response> {
    info!("Patching custom resource scale {group}/{version}/{plural}: {name}");
    let (params, content_type, body) = patch_request(req).await?;
    let crd = serving_crd(&state, &group, &version, &plural, None).await?;
    endpoints::patch_resource(
        &state,
        &scale_scope(&state, &crd, &version)?,
        &auth_ctx.user,
        namespace.as_deref(),
        &name,
        &params,
        &content_type,
        &body,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        CustomResourceDefinitionNames, CustomResourceDefinitionSpec,
        CustomResourceDefinitionVersion, ResourceScope,
    };
    use rusternetes_common::types::ObjectMeta;
    use serde_json::json;

    fn create_test_crd() -> CustomResourceDefinition {
        CustomResourceDefinition {
            api_version: "apiextensions.k8s.io/v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
            metadata: ObjectMeta::new("crontabs.stable.example.com"),
            spec: CustomResourceDefinitionSpec {
                group: "stable.example.com".to_string(),
                names: CustomResourceDefinitionNames {
                    plural: "crontabs".to_string(),
                    singular: Some("crontab".to_string()),
                    kind: "CronTab".to_string(),
                    short_names: Some(vec!["ct".to_string()]),
                    categories: None,
                    list_kind: Some("CronTabList".to_string()),
                },
                scope: ResourceScope::Namespaced,
                versions: vec![CustomResourceDefinitionVersion {
                    name: "v1".to_string(),
                    served: true,
                    storage: true,
                    deprecated: None,
                    deprecation_warning: None,
                    schema: None,
                    subresources: None,
                    additional_printer_columns: None,
                    selectable_fields: None,
                }],
                conversion: None,
                preserve_unknown_fields: None,
            },
            status: None,
        }
    }

    fn create_test_custom_resource() -> CustomResource {
        CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: {
                let mut meta = ObjectMeta::new("my-crontab");
                meta.namespace = Some("default".to_string());
                meta
            },
            spec: Some(json!({
                "cronSpec": "* * * * */5",
                "image": "my-cron-image"
            })),
            status: None,
            extra: Default::default(),
        }
    }

    #[test]
    fn test_validate_custom_resource_success() {
        let crd = create_test_crd();
        let cr = create_test_custom_resource();

        assert!(validate_custom_resource(&crd, "v1", &cr).is_ok());
    }

    #[test]
    fn test_validate_custom_resource_invalid_version() {
        let crd = create_test_crd();
        let cr = create_test_custom_resource();

        let result = validate_custom_resource(&crd, "v2", &cr);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_custom_resource_not_served() {
        let mut crd = create_test_crd();
        crd.spec.versions[0].served = false;
        let cr = create_test_custom_resource();

        let result = validate_custom_resource(&crd, "v1", &cr);
        assert!(result.is_err());
    }

    /// Helper: create a CRD with a schema that has default values
    fn create_crd_with_defaults() -> CustomResourceDefinition {
        use rusternetes_common::resources::crd::JSONSchemaProps;
        use rusternetes_common::resources::CustomResourceValidation;

        let mut spec_properties = std::collections::HashMap::new();
        spec_properties.insert(
            "cronSpec".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                ..Default::default()
            },
        );
        spec_properties.insert(
            "image".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                default: Some(json!("default-image:latest")),
                ..Default::default()
            },
        );
        spec_properties.insert(
            "replicas".to_string(),
            JSONSchemaProps {
                type_: Some("integer".to_string()),
                default: Some(json!(1)),
                ..Default::default()
            },
        );
        // Nested object with defaults
        let mut nested_props = std::collections::HashMap::new();
        nested_props.insert(
            "enabled".to_string(),
            JSONSchemaProps {
                type_: Some("boolean".to_string()),
                default: Some(json!(true)),
                ..Default::default()
            },
        );
        spec_properties.insert(
            "logging".to_string(),
            JSONSchemaProps {
                type_: Some("object".to_string()),
                properties: Some(nested_props),
                ..Default::default()
            },
        );

        let spec_schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(spec_properties),
            ..Default::default()
        };

        let mut top_properties = std::collections::HashMap::new();
        top_properties.insert("spec".to_string(), spec_schema);

        let top_schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(top_properties),
            ..Default::default()
        };

        let mut crd = create_test_crd();
        crd.spec.versions[0].schema = Some(CustomResourceValidation {
            open_apiv3_schema: top_schema,
        });
        crd
    }

    #[test]
    fn test_schema_defaults_applied_to_missing_fields() {
        let crd = create_crd_with_defaults();
        // CR with only cronSpec set — image and replicas should get defaults
        let mut cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({
                "cronSpec": "* * * * */5"
            })),
            status: None,
            extra: Default::default(),
        };

        apply_schema_defaults(&crd, "v1", &mut cr);

        let spec = cr.spec.unwrap();
        assert_eq!(spec["cronSpec"], json!("* * * * */5"));
        assert_eq!(
            spec["image"],
            json!("default-image:latest"),
            "Default for 'image' should be applied"
        );
        assert_eq!(
            spec["replicas"],
            json!(1),
            "Default for 'replicas' should be applied"
        );
    }

    #[test]
    fn test_schema_defaults_do_not_overwrite_existing() {
        let crd = create_crd_with_defaults();
        let mut cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({
                "cronSpec": "0 0 * * *",
                "image": "my-custom-image:v2",
                "replicas": 3
            })),
            status: None,
            extra: Default::default(),
        };

        apply_schema_defaults(&crd, "v1", &mut cr);

        let spec = cr.spec.unwrap();
        assert_eq!(
            spec["image"],
            json!("my-custom-image:v2"),
            "Existing value should not be overwritten"
        );
        assert_eq!(
            spec["replicas"],
            json!(3),
            "Existing value should not be overwritten"
        );
    }

    #[test]
    fn test_schema_defaults_nested_objects() {
        let crd = create_crd_with_defaults();
        // CR has a logging object but without 'enabled' — default should be applied
        let mut cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({
                "cronSpec": "* * * * */5",
                "logging": {}
            })),
            status: None,
            extra: Default::default(),
        };

        apply_schema_defaults(&crd, "v1", &mut cr);

        let spec = cr.spec.unwrap();
        assert_eq!(
            spec["logging"]["enabled"],
            json!(true),
            "Nested default for 'logging.enabled' should be applied"
        );
    }

    #[test]
    fn test_strict_field_validation_rejects_unknown_cr_fields() {
        // Simulate strict field validation on a CR body with an unknown top-level field
        let cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({"cronSpec": "* * * * */5"})),
            status: None,
            extra: Default::default(),
        };

        // Body includes an unknown field "unknownTopLevel"
        let body = br#"{"apiVersion":"stable.example.com/v1","kind":"CronTab","metadata":{"name":"my-crontab"},"spec":{"cronSpec":"* * * * */5"},"unknownTopLevel":"bad"}"#;

        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = crate::handlers::validation::validate_strict_fields(&params, body, &cr);
        assert!(
            result.is_err(),
            "Strict validation should reject unknown fields"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("strict decoding error"),
            "Error should mention strict decoding: {}",
            err_msg
        );
        assert!(
            err_msg.contains("unknownTopLevel"),
            "Error should mention the unknown field name: {}",
            err_msg
        );
    }

    #[test]
    fn test_strict_field_validation_allows_valid_cr() {
        let cr = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(json!({"cronSpec": "* * * * */5"})),
            status: None,
            extra: Default::default(),
        };

        let body = br#"{"apiVersion":"stable.example.com/v1","kind":"CronTab","metadata":{"name":"my-crontab"},"spec":{"cronSpec":"* * * * */5"}}"#;

        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = crate::handlers::validation::validate_strict_fields(&params, body, &cr);
        assert!(
            result.is_ok(),
            "Strict validation should pass for valid CR: {:?}",
            result
        );
    }
}
