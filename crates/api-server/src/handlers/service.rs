use crate::registry::core::service::alloc;
use crate::{handlers::watch::WatchParams, middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::{LoadBalancerStatus, Service, ServiceStatus, ServiceType},
    List, Result,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Apply K8s session-affinity defaulting to a service spec.
///
/// - When sessionAffinity == "ClientIP" and timeoutSeconds is unset, default
///   it to 10800 (3 hours).
/// - When sessionAffinity != "ClientIP" (e.g. "None") the affinity config
///   must be cleared. Callers may opt into this clearing for the update path.
///
/// K8s ref: pkg/apis/core/v1/defaults.go SetDefaults_Service
fn apply_session_affinity_defaults(
    spec: &mut rusternetes_common::resources::ServiceSpec,
    clear_when_none: bool,
) {
    if spec.session_affinity.as_deref() == Some("ClientIP") {
        let cfg = spec.session_affinity_config.get_or_insert(
            rusternetes_common::resources::SessionAffinityConfig { client_ip: None },
        );
        let client_ip =
            cfg.client_ip
                .get_or_insert(rusternetes_common::resources::ClientIPConfig {
                    timeout_seconds: None,
                });
        if client_ip.timeout_seconds.is_none() {
            client_ip.timeout_seconds = Some(10800);
        }
    } else if clear_when_none {
        spec.session_affinity_config = None;
    }
}

/// Default each ServicePort's `targetPort` and `protocol`, mirroring upstream
/// `SetDefaults_Service`. A `targetPort` of `intstr.FromInt32(0)` or
/// `intstr.FromString("")` is treated as unset (Go's typed clients serialize an
/// omitted targetPort as the zero IntOrString `0`) and defaulted to `port`.
/// Protocol defaults to TCP. Must run before validation.
///
/// Returns `true` if any field was changed, so callers on the patch path can
/// force a re-save (the merge layer does no Service-specific defaulting).
fn default_service_ports(spec: &mut rusternetes_common::resources::ServiceSpec) -> bool {
    use rusternetes_common::resources::IntOrString;
    let mut changed = false;
    for port in &mut spec.ports {
        let target_port_unset = match &port.target_port {
            None => true,
            Some(IntOrString::Int(0)) => true,
            Some(IntOrString::String(s)) => s.is_empty(),
            _ => false,
        };
        if target_port_unset {
            port.target_port = Some(IntOrString::Int(port.port as i32));
            changed = true;
        }
        if port.protocol.is_empty() {
            port.protocol = "TCP".to_string();
            changed = true;
        }
    }
    changed
}

pub async fn create(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<(StatusCode, Json<Service>)> {
    // Parse the body manually so we can do strict field validation against the raw bytes.
    // Mirrors the deployment handler: on duplicate-field errors in strict mode, fall back
    // through Value -> from_value so validate_strict_fields can report all issues.
    let is_strict = params.get("fieldValidation").map(|v| v.as_str()) == Some("Strict");
    let mut service: Service = match serde_json::from_slice(&body) {
        Ok(s) => s,
        Err(e) => {
            let msg = e.to_string();
            if is_strict && msg.contains("duplicate field") {
                let value: serde_json::Value =
                    rusternetes_common::dump::decode_request_body(&body)?;
                serde_json::from_value(value).map_err(|e2| {
                    rusternetes_common::Error::BadRequest(format!("failed to decode: {}", e2))
                })?
            } else {
                // Upstream's transformDecodeError: a body that will not decode
                // is 400/BadRequest, never 422/Invalid (#1915).
                return Err(
                    rusternetes_common::dump::decode_request_body::<Service>(&body)
                        .expect_err("the decode that just failed cannot now succeed"),
                );
            }
        }
    };

    info!("Creating service: {}/{}", namespace, service.metadata.name);

    // Reject create with neither name nor generateName (#1065).
    crate::handlers::validation::validate_create_object_meta(
        &service.metadata,
        Some(&namespace),
        crate::handlers::validation::NameKind::DnsLabel,
    )?;

    // Default ServicePort.targetPort / protocol BEFORE validation.
    //
    // Upstream runs SetDefaults_Service before ValidateService, and treats a
    // targetPort of `intstr.FromInt32(0)` or `intstr.FromString("")` as unset,
    // not just an absent field. Go's typed e2e clients serialize an omitted
    // targetPort as the zero IntOrString `0`, so without this every such
    // Service would be rejected by validation with
    // `targetPort: Invalid value: 0` before defaulting could run.
    // K8s ref: pkg/apis/core/v1/defaults.go (SetDefaults_Service port loop).
    default_service_ports(&mut service.spec);

    // Field-level validation — accumulate every violation into a field::ErrorList
    // and return 422 Invalid if non-empty, mirroring upstream NewInvalid.
    // K8s ref: pkg/apis/core/validation/validation.go ValidateService.
    {
        let errs = rusternetes_common::validation::service::validate_service(&service);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Strict field validation: reject unknown / duplicate fields when requested.
    // Mirrors crates/api-server/src/handlers/pod.rs:38.
    crate::handlers::validation::validate_strict_fields(&params, &body, &service)?;

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "create", "services")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    service.metadata.namespace = Some(namespace.clone());

    // Enrich metadata with system fields
    service.metadata.ensure_uid();
    service.metadata.ensure_creation_timestamp();
    crate::handlers::lifecycle::set_initial_generation(&mut service.metadata);

    // Default service type to ClusterIP if not set
    if service.spec.service_type.is_none() {
        service.spec.service_type = Some(ServiceType::ClusterIP);
    }

    // Default sessionAffinity to "None" if not set
    // K8s: pkg/apis/core/v1/defaults.go:108
    if service.spec.session_affinity.is_none() {
        service.spec.session_affinity = Some("None".to_string());
    }

    apply_session_affinity_defaults(&mut service.spec, false);

    // Default internalTrafficPolicy to "Cluster" for ClusterIP/NodePort/LoadBalancer
    // K8s ref: pkg/apis/core/v1/defaults.go:141-146
    if service.spec.internal_traffic_policy.is_none()
        && matches!(
            service.spec.service_type,
            Some(ServiceType::ClusterIP)
                | Some(ServiceType::NodePort)
                | Some(ServiceType::LoadBalancer)
        )
    {
        service.spec.internal_traffic_policy =
            Some(rusternetes_common::resources::ServiceInternalTrafficPolicy::Cluster);
    }

    // Default ip_families and ip_family_policy for non-ExternalName services
    if !matches!(service.spec.service_type, Some(ServiceType::ExternalName)) {
        if service.spec.ip_families.is_none() {
            service.spec.ip_families = Some(vec![rusternetes_common::resources::IPFamily::IPv4]);
        }
        if service.spec.ip_family_policy.is_none() {
            service.spec.ip_family_policy =
                Some(rusternetes_common::resources::IPFamilyPolicy::SingleStack);
        }
    }

    // Allocate ClusterIP if needed
    let service_type = service
        .spec
        .service_type
        .as_ref()
        .unwrap_or(&ServiceType::ClusterIP);

    // Validate ExternalName services
    if matches!(service_type, ServiceType::ExternalName) {
        // ExternalName services must have an externalName field
        if service.spec.external_name.is_none() {
            return Err(rusternetes_common::Error::InvalidResource(
                "ExternalName service must have spec.externalName set".to_string(),
            ));
        }
        // ExternalName services cannot have a ClusterIP
        if service.spec.cluster_ip.is_some()
            && service.spec.cluster_ip.as_deref() != Some("None")
            && service.spec.cluster_ip.as_deref() != Some("")
        {
            return Err(rusternetes_common::Error::InvalidResource(
                "ExternalName service cannot have a ClusterIP".to_string(),
            ));
        }
        // ExternalName services don't need ClusterIP allocation - use empty string per K8s API convention
        service.spec.cluster_ip = Some("".to_string());
    } else {
        // Non-ExternalName types must not carry externalName — drop it (upstream
        // dropServiceDisabledFields) rather than rejecting a stray value.
        service.spec.external_name = None;
        // The ClusterIP itself is allocated just before the write, with the
        // node ports, so a request rejected before then claims nothing.
    }

    // Populate clusterIPs from clusterIP for consistency (K8s always returns both)
    if let Some(ref cip) = service.spec.cluster_ip {
        if cip != "None" && !cip.is_empty() && service.spec.cluster_ips.is_none() {
            service.spec.cluster_ips = Some(vec![cip.clone()]);
        }
    }

    // Initialize status — K8s always returns status.loadBalancer on Service objects
    if service.status.is_none() {
        service.status = Some(ServiceStatus {
            load_balancer: Some(LoadBalancerStatus { ingress: vec![] }),
            conditions: None,
        });
    }

    // Check ResourceQuota count limits for services
    crate::admission::check_count_quota(&state.storage, &namespace, "services").await?;

    let key = build_key("services", Some(&namespace), &service.metadata.name);

    // Check ResourceQuota for services
    {
        let quota_prefix = format!("/registry/resourcequotas/{}/", namespace);
        if let Ok(quotas) = state
            .storage
            .list::<rusternetes_common::resources::ResourceQuota>(&quota_prefix)
            .await
        {
            for quota in &quotas {
                if let Some(hard) = &quota.spec.hard {
                    // Count existing services
                    let svc_prefix = format!("/registry/services/{}/", namespace);
                    let existing_svcs: Vec<Service> =
                        state.storage.list(&svc_prefix).await.unwrap_or_default();

                    // Check "services" quota
                    if let Some(limit_str) = hard.get("services") {
                        if let Ok(limit) = limit_str.parse::<i64>() {
                            if existing_svcs.len() as i64 + 1 > limit {
                                return Err(rusternetes_common::Error::Forbidden(format!(
                                    "exceeded quota: services, requested: 1, used: {}, limited: {}",
                                    existing_svcs.len(),
                                    limit
                                )));
                            }
                        }
                    }

                    // Check "services.loadbalancers" quota
                    if matches!(service.spec.service_type, Some(ServiceType::LoadBalancer)) {
                        if let Some(limit_str) = hard.get("services.loadbalancers") {
                            if let Ok(limit) = limit_str.parse::<i64>() {
                                let current_lb = existing_svcs
                                    .iter()
                                    .filter(|s| {
                                        matches!(
                                            s.spec.service_type,
                                            Some(ServiceType::LoadBalancer)
                                        )
                                    })
                                    .count()
                                    as i64;
                                if current_lb + 1 > limit {
                                    return Err(rusternetes_common::Error::Forbidden(format!(
                                        "exceeded quota: services.loadbalancers, requested: 1, used: {}, limited: {}",
                                        current_lb, limit
                                    )));
                                }
                            }
                        }
                    }

                    // Check "services.nodeports" quota
                    if matches!(
                        service.spec.service_type,
                        Some(ServiceType::NodePort | ServiceType::LoadBalancer)
                    ) {
                        if let Some(limit_str) = hard.get("services.nodeports") {
                            if let Ok(limit) = limit_str.parse::<i64>() {
                                let current_np = existing_svcs
                                    .iter()
                                    .filter(|s| {
                                        matches!(
                                            s.spec.service_type,
                                            Some(ServiceType::NodePort | ServiceType::LoadBalancer)
                                        )
                                    })
                                    .count()
                                    as i64;
                                if current_np + 1 > limit {
                                    return Err(rusternetes_common::Error::Forbidden(format!(
                                        "exceeded quota: services.nodeports, requested: 1, used: {}, limited: {}",
                                        current_np, limit
                                    )));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Claim the ClusterIP, then the node ports (upstream beginCreate ->
    // allocateCreate, pkg/registry/core/service/storage/alloc.go:65-100). A
    // dry run checks the requested values without claiming them.
    let ip_txn =
        alloc::txn_alloc_cluster_ips(&state.cluster_ip_allocator, &mut service, is_dry_run).await?;
    let node_port_op =
        match alloc::txn_alloc_node_ports(&state.node_port_allocator, &mut service, is_dry_run)
            .await
        {
            Ok(op) => op,
            Err(e) => {
                ip_txn.revert().await;
                return Err(e);
            }
        };

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        alloc::settle_all(ip_txn, node_port_op, &Ok(())).await;
        info!(
            "Dry-run: Service {}/{} validated successfully (not created)",
            namespace, service.metadata.name
        );
        return Ok((StatusCode::CREATED, Json(service)));
    }

    let created = state.storage.create(&key, &service).await;
    alloc::settle_all(ip_txn, node_port_op, &created).await;

    Ok((StatusCode::CREATED, Json(created?)))
}

pub async fn get(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Service>> {
    debug!("Getting service: {}/{}", namespace, name);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "get", "services")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let key = build_key("services", Some(&namespace), &name);
    let service = state.storage.get(&key).await?;

    Ok(Json(service))
}

pub async fn update(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    DumpingJson(mut service): DumpingJson<Service>,
) -> Result<Json<Service>> {
    info!("Updating service: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "update", "services")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }
    // A PUT to an object that does not exist is a 404, not a create: upstream
    // consults the strategy's `AllowCreateOnUpdate()` in `Store.Update`
    // (registry/generic/registry/store.go:646-650) and only nine resources opt
    // in. The check sits ahead of every validator there, so it runs here before
    // validation too (#1905).
    crate::handlers::lifecycle::reject_create_on_update(
        &*state.storage,
        &build_key("services", Some(&namespace), &name),
        "",
        "services",
        &name,
    )
    .await?;

    service.metadata.name = name.clone();
    service.metadata.namespace = Some(namespace.clone());

    // Default sessionAffinity to "None" if not set (mirrors create handler).
    if service.spec.session_affinity.is_none() {
        service.spec.session_affinity = Some("None".to_string());
    }

    // Clear the affinity config when affinity is set to "None" on update so
    // the stored object matches the K8s API contract.
    apply_session_affinity_defaults(&mut service.spec, true);

    // Default ServicePort.targetPort / protocol before validation, same as the
    // create path — upstream runs SetDefaults_Service on update too, and Go's
    // typed clients send an unset targetPort as the zero IntOrString `0`.
    default_service_ports(&mut service.spec);

    let key = build_key("services", Some(&namespace), &name);

    // Get the old service for concurrency control, generation tracking, and
    // ClusterIP immutability checks.
    let old_service: Service = state.storage.get(&key).await?;

    // ClusterIP immutability — upstream pkg/apis/core/validation/validation.go
    // ValidateServiceUpdate. Once a Service has a ClusterIP assigned, it cannot
    // be changed (either to a different IP, cleared, or toggled to/from the
    // headless sentinel "None"). The sole legal exception is the transition to
    // ExternalName, which clears the ClusterIP below.
    let new_is_external_name = matches!(service.spec.service_type, Some(ServiceType::ExternalName));
    let old_was_external_name = matches!(
        old_service.spec.service_type,
        Some(ServiceType::ExternalName)
    );
    let old_cip = old_service.spec.cluster_ip.as_deref().unwrap_or("");
    if !old_cip.is_empty() && !new_is_external_name {
        let new_cip = service.spec.cluster_ip.as_deref().unwrap_or("");
        if new_cip != old_cip {
            return Err(rusternetes_common::Error::InvalidResource(format!(
                "spec.clusterIP: Invalid value: {:?}: field is immutable",
                new_cip
            )));
        }
        // Cross-check clusterIPs[0] when present — upstream rejects a mismatched
        // primary entry alongside the immutable clusterIP.
        if let Some(new_ips) = &service.spec.cluster_ips {
            if let Some(first) = new_ips.first() {
                if first != old_cip {
                    return Err(rusternetes_common::Error::InvalidResource(format!(
                        "spec.clusterIPs[0]: Invalid value: {:?}: field is immutable",
                        first
                    )));
                }
            }
        }
    }

    // When service type changes to ExternalName, clear ClusterIP and NodePorts
    if new_is_external_name {
        service.spec.cluster_ip = Some("".to_string());
        service.spec.cluster_ips = None;
        for port in &mut service.spec.ports {
            port.node_port = None;
        }
        service.spec.health_check_node_port = None;
    }
    // When changing FROM ExternalName TO ClusterIP/NodePort/LoadBalancer,
    // allocate a ClusterIP only on that genuine transition. Do NOT re-allocate
    // on a plain update — that would silently mask the immutability violation
    // we just rejected above.
    else {
        // Drop externalName whenever the type is not ExternalName, mirroring
        // upstream's dropServiceDisabledFields / type-dependent field clearing.
        // The DNS e2e flips an ExternalName Service to ClusterIP via a full PUT
        // that leaves spec.externalName populated; upstream silently clears it
        // rather than rejecting with "may not be set for non-ExternalName".
        service.spec.external_name = None;
        let needs_ip = service
            .spec
            .cluster_ip
            .as_ref()
            .is_none_or(|ip| ip.is_empty());
        // From ExternalName the ClusterIP is allocated just before the
        // write (txn_update_cluster_ips, case A).
        if needs_ip && !old_was_external_name {
            // Old service had a ClusterIP (handled by the immutability fence
            // above), or this is some other inconsistent state. Restore the
            // stored ClusterIP rather than allocating a new one. The
            // immutability check above will reject mismatches before we get
            // here; this branch covers the case where the new spec simply
            // omits clusterIP/clusterIPs (PATCH-style partials).
            service.spec.cluster_ip = old_service.spec.cluster_ip.clone();
            service.spec.cluster_ips = old_service.spec.cluster_ips.clone();
        }
    }

    // Field-level validation on update — same rules as create.
    // K8s ref: pkg/apis/core/validation/validation.go ValidateServiceUpdate.
    {
        let errs = rusternetes_common::validation::service::validate_service(&service);
        if !errs.is_empty() {
            return Err(rusternetes_common::Error::Invalid(errs));
        }
    }

    // Check resourceVersion for optimistic concurrency control
    crate::handlers::lifecycle::check_resource_version(
        old_service.metadata.resource_version.as_deref(),
        service.metadata.resource_version.as_deref(),
        &name,
    )?;

    // Reinstate the server-owned metadata a PUT body may omit (uid,
    // creationTimestamp, a pending deletion). A locally-built object — what the
    // dynamic client's Update() sends — carries none of them, and storing the
    // blanks orphans every child: the ownerReferences[].uid no longer matches a
    // live owner, so the garbage collector deletes the children (#1605).
    // Upstream: registry/rest/update.go::BeforeUpdate (lines 123-146).
    crate::handlers::lifecycle::inherit_server_owned_metadata(
        &mut service.metadata,
        &old_service.metadata,
    );

    // Increment generation if spec changed
    let old_value = serde_json::to_value(&old_service)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    let new_value = serde_json::to_value(&service)
        .map_err(|e| rusternetes_common::Error::Internal(e.to_string()))?;
    crate::handlers::lifecycle::maybe_increment_generation(
        &old_value,
        &new_value,
        &mut service.metadata,
    );

    // Claim and free the ClusterIP, then the node ports (upstream
    // beginUpdate -> allocateUpdate, alloc.go:591-626).
    let ip_txn = alloc::txn_update_cluster_ips(
        &state.cluster_ip_allocator,
        &mut service,
        &old_service,
        is_dry_run,
    )
    .await?;
    let node_port_op = match alloc::txn_update_node_ports(
        &state.node_port_allocator,
        &mut service,
        &old_service,
        is_dry_run,
    )
    .await
    {
        Ok(op) => op,
        Err(e) => {
            ip_txn.revert().await;
            return Err(e);
        }
    };

    // If dry-run, skip storage operation but return the validated resource
    if is_dry_run {
        alloc::settle_all(ip_txn, node_port_op, &Ok(())).await;
        info!(
            "Dry-run: Service {}/{} validated successfully (not updated)",
            namespace, name
        );
        return Ok(Json(service));
    }

    let updated = state.storage.update(&key, &service).await;
    alloc::settle_all(ip_txn, node_port_op, &updated).await;
    let updated = updated?;

    // Upstream ShouldDeleteDuringUpdate: an update that drains the last
    // finalizer off an object already pending deletion removes it as part of
    // that same request (store.go:565).
    crate::handlers::finalizers::finish_deletion_if_finalizers_drained(
        &*state.storage,
        &key,
        &updated,
    )
    .await?;

    Ok(Json(updated))
}

pub async fn delete_service(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Service>> {
    info!("Deleting service: {}/{}", namespace, name);

    // Check if this is a dry-run request
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "delete", "services")
        .with_namespace(&namespace)
        .with_api_group("")
        .with_name(&name);

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Get the service to validate it exists and potentially release its ClusterIP
    let key = build_key("services", Some(&namespace), &name);
    let service = state.storage.get::<Service>(&key).await?;

    // Run validating admission webhooks for DELETE (object=nil, oldObject=service).
    crate::handlers::admission_helper::run_delete_validating_webhooks(
        &state,
        "",
        "v1",
        "Service",
        "services",
        Some(&namespace),
        &name,
        &service,
        &user_for_webhook,
        is_dry_run,
    )
    .await?;

    // If dry-run, skip delete operation
    if is_dry_run {
        info!(
            "Dry-run: Service {}/{} validated successfully (not deleted)",
            namespace, name
        );
        return Ok(Json(service));
    }

    // Handle deletion with finalizers
    let deleted_immediately = !crate::handlers::finalizers::handle_delete_with_finalizers(
        &state.storage,
        &key,
        &service,
        &delete_opts,
    )
    .await?;

    if deleted_immediately {
        // Upstream afterDelete -> releaseAllocatedResources (alloc.go:886).
        alloc::release_node_ports(&state.node_port_allocator, &service).await;
        alloc::release_cluster_ips(&state.cluster_ip_allocator, &service).await;
        Ok(Json(service))
    } else {
        // Resource has finalizers, re-read to get updated version with deletionTimestamp
        let updated: Service = state.storage.get(&key).await?;
        Ok(Json(updated))
    }
}

pub async fn list(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    // Check if this is a watch request
    if params.watch.unwrap_or(false) {
        debug!("Watch request for services in namespace: {}", namespace);
        return crate::handlers::watch::watch_services(
            State(state),
            Extension(auth_ctx),
            Path(namespace),
            Query(params),
        )
        .await;
    }

    debug!("Listing services in namespace: {}", namespace);

    // Check authorization
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "services")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("services", Some(&namespace));
    let mut services = state.storage.list::<Service>(&prefix).await?;

    // Apply field and label selector filtering
    let mut params_map = HashMap::new();
    if let Some(fs) = params.field_selector {
        params_map.insert("fieldSelector".to_string(), fs);
    }
    if let Some(ls) = params.label_selector {
        params_map.insert("labelSelector".to_string(), ls);
    }
    crate::handlers::filtering::apply_selectors(&mut services, &params_map)?;

    // The list RV must never fall below an item this same list returns.
    // Upstream gets both from one etcd range response; here the store
    // revision and the items are read separately, so take the max (#1825).
    let resource_version =
        crate::handlers::list_collection_resource_version(&state.storage, &services).await;

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            services,
            Some(resource_version.to_string()),
            "Service",
        );
        return Ok(Json(table).into_response());
    }

    // Wrap in proper List object
    let mut list = List::new("ServiceList", "v1", services);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// List all services across all namespaces
pub async fn list_all_services(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    headers: HeaderMap,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    // Debug log the params
    debug!("list_all_services called with watch={:?}", params.watch);

    // Check if this is a watch request
    if params.watch.unwrap_or(false) {
        debug!("Watch request for all services");
        return crate::handlers::watch::watch_cluster_scoped::<Service>(
            state, auth_ctx, "services", "", params,
        )
        .await;
    }

    debug!("Listing all services");

    // Check authorization (cluster-wide list)
    let attrs = RequestAttributes::new(auth_ctx.user, "list", "services").with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    let prefix = build_prefix("services", None);
    let mut services = state.storage.list::<Service>(&prefix).await?;

    // Apply field and label selector filtering
    let mut params_map = HashMap::new();
    if let Some(fs) = params.field_selector {
        params_map.insert("fieldSelector".to_string(), fs);
    }
    if let Some(ls) = params.label_selector {
        params_map.insert("labelSelector".to_string(), ls);
    }
    crate::handlers::filtering::apply_selectors(&mut services, &params_map)?;

    // The list RV must never fall below an item this same list returns.
    // Upstream gets both from one etcd range response; here the store
    // revision and the items are read separately, so take the max (#1825).
    let resource_version =
        crate::handlers::list_collection_resource_version(&state.storage, &services).await;

    // Check if table format is requested
    let accept = headers.get("accept").and_then(|v| v.to_str().ok());
    if crate::handlers::table::wants_table(accept) {
        let table = crate::handlers::table::generic_table(
            services,
            Some(resource_version.to_string()),
            "Service",
        );
        return Ok(Json(table).into_response());
    }

    let mut list = List::new("ServiceList", "v1", services);
    list.metadata.resource_version =
        Some(crate::handlers::list_collection_resource_version(&state.storage, &list.items).await);
    Ok(Json(list).into_response())
}

/// Custom PATCH handler for services that applies ExternalName ClusterIP clearing
/// after the generic patch logic.
pub async fn patch(
    state: axum::extract::State<std::sync::Arc<crate::state::ApiServerState>>,
    auth_ctx: axum::Extension<crate::middleware::AuthContext>,
    path: axum::extract::Path<(String, String)>,
    query: axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> rusternetes_common::Result<Json<Service>> {
    let (namespace, name) = path.0.clone();
    let key = rusternetes_storage::build_key("services", Some(&namespace), &name);
    // The merge below writes before this handler sees the result, so the
    // ClusterIP and node ports are reconciled against the pre-patch object
    // afterwards.
    let old_service: Option<Service> = state.storage.get(&key).await.ok();
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&query.0);
    let result = crate::handlers::generic_patch::patch_namespaced_resource::<Service>(
        state.clone(),
        auth_ctx,
        axum::extract::Path((namespace.clone(), name.clone())),
        query,
        headers,
        body,
        "services",
        "",
    )
    .await?;

    // Post-patch: handle service type transitions
    let mut service = result.0;
    let mut needs_update = false;

    // Re-default ServicePort.targetPort / protocol after the merge. generic_patch
    // is type-generic and performs no Service-specific defaulting, so a patch
    // that sets targetPort to the zero IntOrString `0` (or clears protocol) would
    // otherwise persist an invalid `0`. Force a re-save when defaulting changed
    // anything. Mirrors the create/update paths.
    if default_service_ports(&mut service.spec) {
        needs_update = true;
    }

    if matches!(service.spec.service_type, Some(ServiceType::ExternalName)) {
        // Changing TO ExternalName — clear ClusterIP and NodePorts
        if service.spec.cluster_ip.as_deref() != Some("") && service.spec.cluster_ip.is_some() {
            service.spec.cluster_ip = Some("".to_string());
            service.spec.cluster_ips = None;
            for port in &mut service.spec.ports {
                port.node_port = None;
            }
            needs_update = true;
        }
    }

    // Claim and free the ClusterIP and node ports against the pre-patch
    // object (upstream beginUpdate -> allocateUpdate, alloc.go:591-626). A
    // dry-run patch wrote nothing and claims nothing.
    let Some(old_service) = old_service else {
        return Ok(Json(service));
    };
    let old_was_external_name = matches!(
        old_service.spec.service_type,
        Some(ServiceType::ExternalName)
    );
    let needs_ip = service
        .spec
        .cluster_ip
        .as_ref()
        .is_none_or(|ip| ip.is_empty());
    if needs_ip
        && !old_was_external_name
        && !matches!(service.spec.service_type, Some(ServiceType::ExternalName))
    {
        // A patch that drops the immutable ClusterIP keeps the stored one.
        service.spec.cluster_ip = old_service.spec.cluster_ip.clone();
        service.spec.cluster_ips = old_service.spec.cluster_ips.clone();
    }
    let before = serde_json::to_value(&service.spec).ok();
    let ip_txn = alloc::txn_update_cluster_ips(
        &state.cluster_ip_allocator,
        &mut service,
        &old_service,
        is_dry_run,
    )
    .await?;
    let node_port_op = match alloc::txn_update_node_ports(
        &state.node_port_allocator,
        &mut service,
        &old_service,
        is_dry_run,
    )
    .await
    {
        Ok(op) => op,
        Err(e) => {
            ip_txn.revert().await;
            return Err(e);
        }
    };
    if before != serde_json::to_value(&service.spec).ok() {
        needs_update = true;
    }

    if needs_update && !is_dry_run {
        let saved = state.storage.update(&key, &service).await;
        alloc::settle_all(ip_txn, node_port_op, &saved).await;
        return Ok(Json(saved?));
    }
    alloc::settle_all(ip_txn, node_port_op, &Ok(())).await;
    Ok(Json(service))
}

pub async fn deletecollection_services(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>,
    Path(namespace): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode> {
    info!(
        "DeleteCollection services in namespace: {} with params: {:?}",
        namespace, params
    );

    // Check authorization
    let user_for_webhook = auth_ctx.user.clone();
    let attrs = RequestAttributes::new(auth_ctx.user, "deletecollection", "services")
        .with_namespace(&namespace)
        .with_api_group("");

    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => {}
        Decision::Deny(reason) => {
            return Err(rusternetes_common::Error::Forbidden(reason));
        }
    }

    // Handle dry-run
    let is_dry_run = crate::handlers::dryrun::is_dry_run(&params);
    if is_dry_run {
        info!("Dry-run: Service collection would be deleted (not deleted)");
        return Ok(StatusCode::OK);
    }

    // Get all services in the namespace
    let prefix = build_prefix("services", Some(&namespace));
    let mut items = state.storage.list::<Service>(&prefix).await?;

    // Apply field and label selector filtering
    crate::handlers::filtering::apply_selectors(&mut items, &params)?;

    // Delete each matching resource
    let mut deleted_count = 0;
    for item in items {
        let key = build_key("services", Some(&namespace), &item.metadata.name);

        // Run validating admission webhooks for DELETE per item.
        crate::handlers::admission_helper::run_delete_validating_webhooks(
            &state,
            "",
            "v1",
            "Service",
            "services",
            Some(&namespace),
            &item.metadata.name,
            &item,
            &user_for_webhook,
            false,
        )
        .await?;

        // Handle deletion with finalizers
        let deleted_immediately = match crate::handlers::finalizers::delete_collection_item(
            &state.storage,
            &key,
            &item,
            &delete_opts,
        )
        .await?
        {
            Some(deleted) => deleted,
            // Already gone — a concurrent deleter won the race; upstream
            // DeleteCollection ignores NotFound rather than failing the request.
            None => continue,
        };

        if deleted_immediately {
            alloc::release_node_ports(&state.node_port_allocator, &item).await;
            alloc::release_cluster_ips(&state.cluster_ip_allocator, &item).await;
            deleted_count += 1;
        }
    }

    info!(
        "DeleteCollection completed: {} services deleted",
        deleted_count
    );
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use rusternetes_common::resources::{LoadBalancerStatus, Service, ServiceSpec, ServiceStatus};

    #[test]
    fn test_service_status_load_balancer_initialization() {
        // Simulate the create handler's status initialization logic.
        // When status is None, it should be populated with an empty loadBalancer.
        let mut service = Service::new("test-svc", ServiceSpec::default());

        // Apply the same logic as the create handler
        if service.status.is_none() {
            service.status = Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus { ingress: vec![] }),
                conditions: None,
            });
        }

        let status = service.status.as_ref().expect("status should be set");
        let lb = status
            .load_balancer
            .as_ref()
            .expect("loadBalancer should be set");
        assert!(lb.ingress.is_empty(), "ingress should be empty on create");
    }

    #[test]
    fn test_service_cluster_ips_populated_from_cluster_ip() {
        // When clusterIP is set but clusterIPs is None, the create handler
        // should populate clusterIPs from clusterIP.
        let mut service = Service::new(
            "test-svc",
            ServiceSpec {
                cluster_ip: Some("10.96.0.1".to_string()),
                cluster_ips: None,
                ..ServiceSpec::default()
            },
        );

        // Apply the same logic as the create handler
        if let Some(ref cip) = service.spec.cluster_ip {
            if cip != "None" && !cip.is_empty() && service.spec.cluster_ips.is_none() {
                service.spec.cluster_ips = Some(vec![cip.clone()]);
            }
        }

        let cluster_ips = service
            .spec
            .cluster_ips
            .as_ref()
            .expect("clusterIPs should be populated");
        assert_eq!(cluster_ips, &vec!["10.96.0.1".to_string()]);
    }

    #[test]
    fn test_service_cluster_ips_not_set_for_headless() {
        // When clusterIP is "None" (headless service), clusterIPs should NOT be populated.
        let mut service = Service::new(
            "headless-svc",
            ServiceSpec {
                cluster_ip: Some("None".to_string()),
                cluster_ips: None,
                ..ServiceSpec::default()
            },
        );

        if let Some(ref cip) = service.spec.cluster_ip {
            if cip != "None" && !cip.is_empty() && service.spec.cluster_ips.is_none() {
                service.spec.cluster_ips = Some(vec![cip.clone()]);
            }
        }

        assert!(
            service.spec.cluster_ips.is_none(),
            "clusterIPs should not be set for headless services"
        );
    }
}
