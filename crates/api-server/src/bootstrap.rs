use anyhow::{Context, Result};
use rusternetes_common::resources::rbac::ClusterRole;
use rusternetes_common::resources::{EndpointSlice, Endpoints};
use rusternetes_storage::Storage;
use rusternetes_storage::StorageBackend;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

#[cfg(test)]
const CLUSTER_ADMIN_ROLE_KEY: &str = "/registry/clusterroles/cluster-admin";
#[cfg(test)]
const CLUSTER_ADMIN_BINDING_KEY: &str = "/registry/clusterrolebindings/cluster-admin";

/// Upstream's bootstrap policy, vendored verbatim from
/// `plugin/pkg/auth/authorizer/rbac/bootstrappolicy/testdata/` (release-1.35).
/// Those files are the serialised output of `ClusterRoles()`,
/// `ClusterRoleBindings()`, `ControllerRoles()`, `ControllerRoleBindings()`,
/// `NamespaceRoles()` and `NamespaceRoleBindings()` in `policy.go` /
/// `controller_policy.go` / `namespace_policy.go`, so every role carries the
/// `kubernetes.io/bootstrapping=rbac-defaults` label and the
/// `rbac.authorization.kubernetes.io/autoupdate=true` annotation exactly as
/// `addClusterRoleLabel` (policy.go:48-60) stamps them. (The
/// `cluster-roles-featuregates.yaml` variant is not vendored: it only adds
/// alpha-gated roles.)
const BOOTSTRAP_POLICY: &[&str] = &[
    include_str!("bootstrap_policy/cluster-roles.yaml"),
    include_str!("bootstrap_policy/controller-roles.yaml"),
    include_str!("bootstrap_policy/namespace-roles.yaml"),
    include_str!("bootstrap_policy/cluster-role-bindings.yaml"),
    include_str!("bootstrap_policy/controller-role-bindings.yaml"),
    include_str!("bootstrap_policy/namespace-role-bindings.yaml"),
];

const AUTOUPDATE_ANNOTATION: &str = "rbac.authorization.kubernetes.io/autoupdate";

/// Decode one vendored `v1.List` into its items. Mirrors nothing upstream (Go
/// builds the objects in code); this is only the Rust-side loader for the
/// vendored testdata. A role's `rules: null` becomes `[]` so it decodes as a
/// typed `ClusterRole`.
fn load_policy_items(yaml: &str) -> Result<Vec<serde_json::Value>> {
    let list: serde_json::Value =
        serde_yaml::from_str(yaml).context("bootstrap policy YAML must parse")?;
    let mut items = list["items"].as_array().cloned().unwrap_or_default();
    for item in &mut items {
        if item["kind"].as_str().is_some_and(|k| k.ends_with("Role")) && item["rules"].is_null() {
            item["rules"] = serde_json::json!([]);
        }
    }
    Ok(items)
}

fn policy_key(item: &serde_json::Value) -> Option<(String, String)> {
    let plural = match item["kind"].as_str()? {
        "ClusterRole" => "clusterroles",
        "ClusterRoleBinding" => "clusterrolebindings",
        "Role" => "roles",
        "RoleBinding" => "rolebindings",
        _ => return None,
    };
    let name = item["metadata"]["name"].as_str()?;
    let ns = item["metadata"]["namespace"].as_str();
    Some((
        rusternetes_storage::build_key(plural, ns, name),
        item["kind"].as_str()?.to_string(),
    ))
}

/// Seed the full upstream RBAC bootstrap policy (#1659, #1753), mirroring
/// `EnsureRBACPolicy` (`pkg/registry/rbac/rest/storage_rbac.go:269-331`, run
/// from the `rbac/bootstrap-roles` post-start hook): every bootstrap
/// ClusterRole/ClusterRoleBinding/Role/RoleBinding is created if missing, and
/// an existing one is reconciled unless it opted out with
/// `rbac.authorization.kubernetes.io/autoupdate: "false"`
/// (`component-helpers/auth/rbac/reconciliation/reconcile_role.go`,
/// `reconcile_rolebindings.go`: add missing rules / subjects, never remove).
/// Namespaced objects get their namespace created first (`tryEnsureNamespace`,
/// `namespace.go:31-45`).
///
/// This includes `cluster-admin` bound to `system:masters`: without it a
/// freshly bootstrapped (empty) store denies the cluster admin — kubeadm's
/// `CN=kubernetes-admin, O=system:masters` client cert — every request (#1659).
/// The superuser effect comes from a real RBAC rule, so the
/// privilege-escalation check stays rule-based (NOT an authorizer
/// short-circuit). Idempotent.
///
/// Deviations: upstream's `Covers`-based rule reconciliation is approximated by
/// "append rules not already present verbatim", and `ClusterRole`s with an
/// `aggregationRule` are materialised once at seed time (no
/// clusterroleaggregation controller exists here; see
/// `registry/rbac/aggregation.rs`).
pub async fn bootstrap_default_rbac(storage: Arc<StorageBackend>) -> Result<()> {
    let mut namespaces = std::collections::BTreeSet::new();
    let mut aggregated: Vec<String> = Vec::new();

    for yaml in BOOTSTRAP_POLICY {
        for mut item in load_policy_items(yaml)? {
            let Some((key, kind)) = policy_key(&item) else {
                continue;
            };
            if let Some(ns) = item["metadata"]["namespace"].as_str() {
                if namespaces.insert(ns.to_string()) {
                    create_namespace_if_needed(storage.as_ref(), ns).await?;
                }
            }
            match storage.get::<serde_json::Value>(&key).await {
                Ok(existing) => {
                    if let Some(updated) = reconcile_policy_object(&existing, &item) {
                        storage
                            .update(&key, &updated)
                            .await
                            .with_context(|| format!("reconcile {key}"))?;
                        info!("Reconciled bootstrap RBAC object {key}");
                    }
                }
                Err(_) => {
                    item["metadata"]["uid"] = uuid::Uuid::new_v4().to_string().into();
                    item["metadata"]["creationTimestamp"] = chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                        .into();
                    match storage.create(&key, &item).await {
                        Ok(_) | Err(rusternetes_common::Error::AlreadyExists(_)) => {}
                        Err(e) => {
                            return Err(anyhow::Error::from(e))
                                .with_context(|| format!("create {key}"))
                        }
                    }
                    if kind == "ClusterRole" && !item["aggregationRule"].is_null() {
                        aggregated.push(key);
                    }
                }
            }
        }
    }

    // Aggregating roles (admin/edit/view) are filled from the leaf roles, which
    // now all exist.
    for key in aggregated {
        let mut role: ClusterRole = storage.get(&key).await?;
        crate::registry::rbac::aggregation::materialise_aggregated_rules(
            storage.as_ref(),
            &mut role,
        )
        .await;
        storage.update(&key, &role).await?;
    }
    Ok(())
}

/// `ReconcileRole` / `ReconcileRoleBinding`: for an object that has not opted
/// out of autoupdate, return the object with any missing rules (roles) or
/// subjects (bindings) added, or `None` when nothing needs to change.
fn reconcile_policy_object(
    existing: &serde_json::Value,
    desired: &serde_json::Value,
) -> Option<serde_json::Value> {
    if existing["metadata"]["annotations"][AUTOUPDATE_ANNOTATION].as_str() == Some("false") {
        return None;
    }
    let mut updated = existing.clone();
    let mut changed = false;
    // Rules of an aggregating role belong to the aggregation, not the policy.
    let field = if desired["kind"]
        .as_str()
        .is_some_and(|k| k.ends_with("Binding"))
    {
        "subjects"
    } else if desired["aggregationRule"].is_null() {
        "rules"
    } else {
        return None;
    };
    let have = updated[field].as_array().cloned().unwrap_or_default();
    let mut merged = have.clone();
    for want in desired[field].as_array().cloned().unwrap_or_default() {
        if !have.contains(&want) {
            merged.push(want);
            changed = true;
        }
    }
    if changed {
        updated[field] = serde_json::Value::Array(merged);
        Some(updated)
    } else {
        None
    }
}

/// How often the `kubernetes` Service endpoint is re-asserted to the live
/// api-server IP. Mirrors upstream's `DefaultEndpointReconcilerInterval`
/// (`pkg/controlplane/instance.go`, 10s).
pub const ENDPOINT_RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

const SERVICE_KEY: &str = "/registry/services/default/kubernetes";

/// ClusterIP of the `default/kubernetes` Service: the first address of the
/// service range this api-server bootstraps as the `kubernetes` ServiceCIDR
/// (10.96.0.0/12, see main.rs / lib.rs). Upstream derives it the same way, from
/// the first IP of `--service-cluster-ip-range`.
const KUBERNETES_SERVICE_IP: &str = "10.96.0.1";

const ENDPOINTS_KEY: &str = "/registry/endpoints/default/kubernetes";
const ENDPOINTSLICE_KEY: &str = "/registry/endpointslices/default/kubernetes";

/// Upstream `discovery.LabelSkipMirror`. Set on the `kubernetes` Endpoints so
/// the EndpointSlice mirroring controller leaves it alone — the apiserver owns
/// the slice directly (see `crates/controller-manager/.../endpointslice.rs`,
/// which honors this label). Matches `setSkipMirrorTrue`
/// (`pkg/controlplane/reconcilers/endpointsadapter.go`).
const SKIP_MIRROR_LABEL: &str = "endpointslice.kubernetes.io/skip-mirror";

/// Get the API server's IP address from the network interface.
/// This discovers the container's IP on the Docker/Podman network.
fn get_api_server_ip() -> Result<String> {
    // Try to get IP from network interfaces.
    // Look for non-loopback IPv4 addresses.
    let interfaces = match get_if_addrs::get_if_addrs() {
        Ok(addrs) => addrs,
        Err(e) => {
            warn!("Failed to get network interfaces: {}", e);
            return Err(anyhow::anyhow!("Failed to get network interfaces: {}", e));
        }
    };

    // Find the first non-loopback IPv4 address.
    for iface in interfaces {
        if !iface.is_loopback() {
            if let get_if_addrs::IfAddr::V4(addr) = iface.addr {
                let ip = addr.ip.to_string();
                info!(
                    "Discovered API server IP: {} (interface: {})",
                    ip, iface.name
                );
                return Ok(ip);
            }
        }
    }

    Err(anyhow::anyhow!("No non-loopback IPv4 address found"))
}

/// The single canonical `EndpointSubset` the `kubernetes` Endpoints must hold:
/// exactly the api-server's own `ip:port` over the `https`/TCP port. This is the
/// `masterCount == 1` shape of upstream's `masterCountEndpointReconciler`
/// (`pkg/controlplane/reconcilers/instancecount.go`) — we always force the
/// endpoint to our own address.
fn desired_subsets(ip: &str, port: u16) -> Vec<rusternetes_common::resources::EndpointSubset> {
    use rusternetes_common::resources::{EndpointAddress, EndpointPort, EndpointSubset};
    vec![EndpointSubset {
        addresses: Some(vec![EndpointAddress {
            ip: ip.to_string(),
            hostname: None,
            node_name: None,
            target_ref: None,
        }]),
        not_ready_addresses: None,
        ports: Some(vec![EndpointPort {
            name: Some("https".to_string()),
            port,
            protocol: "TCP".to_string(),
            app_protocol: None,
        }]),
    }]
}

/// Ensure the skip-mirror label is `"true"`. Returns whether the labels changed
/// (so the caller knows a write is needed).
fn ensure_skip_mirror(metadata: &mut rusternetes_common::types::ObjectMeta) -> bool {
    let labels = metadata.labels.get_or_insert_with(Default::default);
    if labels.get(SKIP_MIRROR_LABEL).map(String::as_str) == Some("true") {
        false
    } else {
        labels.insert(SKIP_MIRROR_LABEL.to_string(), "true".to_string());
        true
    }
}

/// Reconcile the `kubernetes` Endpoints to point at exactly `ip:port`. Idempotent:
/// only writes when the stored object diverges from the desired shape (so a
/// steady-state cluster does not churn resourceVersions every interval).
async fn reconcile_endpoints<S: Storage + ?Sized>(storage: &S, ip: &str, port: u16) -> Result<()> {
    let desired = desired_subsets(ip, port);

    match storage.get::<Endpoints>(ENDPOINTS_KEY).await {
        Ok(mut endpoints) => {
            let label_changed = ensure_skip_mirror(&mut endpoints.metadata);
            if endpoints.subsets != desired || label_changed {
                let old = endpoints
                    .subsets
                    .first()
                    .and_then(|s| s.addresses.as_ref())
                    .and_then(|a| a.first())
                    .map(|a| a.ip.clone())
                    .unwrap_or_default();
                endpoints.subsets = desired;
                storage
                    .update(ENDPOINTS_KEY, &endpoints)
                    .await
                    .context("Failed to update kubernetes Endpoints")?;
                info!("Reconciled kubernetes Endpoints: {} -> {}", old, ip);
            }
        }
        Err(_) => {
            use rusternetes_common::types::{ObjectMeta, TypeMeta};

            let mut metadata = ObjectMeta::new("kubernetes");
            metadata.namespace = Some("default".to_string());
            metadata.ensure_uid();
            metadata.ensure_creation_timestamp();
            ensure_skip_mirror(&mut metadata);

            let endpoints = Endpoints {
                type_meta: TypeMeta {
                    kind: "Endpoints".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata,
                subsets: desired,
            };

            storage
                .create(ENDPOINTS_KEY, &endpoints)
                .await
                .context("Failed to create kubernetes Endpoints")?;
            info!("Created kubernetes Endpoints with IP: {}", ip);
        }
    }
    Ok(())
}

/// Reconcile the `kubernetes` EndpointSlice to mirror the Endpoints. The
/// conformance test "should have Endpoints and EndpointSlices pointing to API
/// Server" (apiserver.go) expects BOTH to exist; the apiserver owns the slice
/// directly (the mirroring controller skips it via the skip-mirror label).
async fn reconcile_endpointslice<S: Storage + ?Sized>(
    storage: &S,
    ip: &str,
    port: u16,
) -> Result<()> {
    use rusternetes_common::resources::endpointslice::{
        Endpoint, EndpointConditions, EndpointPort,
    };

    let desired_endpoints = vec![Endpoint {
        addresses: vec![ip.to_string()],
        conditions: Some(EndpointConditions {
            ready: Some(true),
            serving: Some(true),
            terminating: Some(false),
        }),
        hostname: None,
        target_ref: None,
        node_name: None,
        zone: None,
        hints: None,
        deprecated_topology: None,
    }];
    let desired_ports = vec![EndpointPort {
        name: Some("https".to_string()),
        port: Some(port as i32),
        protocol: "TCP".to_string(),
        app_protocol: None,
    }];

    match storage.get::<EndpointSlice>(ENDPOINTSLICE_KEY).await {
        Ok(mut es) => {
            if es.endpoints != desired_endpoints || es.ports != desired_ports {
                es.endpoints = desired_endpoints;
                es.ports = desired_ports;
                storage
                    .update(ENDPOINTSLICE_KEY, &es)
                    .await
                    .context("Failed to update kubernetes EndpointSlice")?;
                info!("Reconciled kubernetes EndpointSlice to IP: {}", ip);
            }
        }
        Err(_) => {
            use rusternetes_common::types::{ObjectMeta, TypeMeta};

            let mut metadata = ObjectMeta::new("kubernetes");
            metadata.namespace = Some("default".to_string());
            let mut labels = std::collections::HashMap::new();
            labels.insert(
                "kubernetes.io/service-name".to_string(),
                "kubernetes".to_string(),
            );
            labels.insert(
                "endpointslice.kubernetes.io/managed-by".to_string(),
                "endpointslice-mirroring-controller.k8s.io".to_string(),
            );
            metadata.labels = Some(labels);
            metadata.ensure_uid();
            metadata.ensure_creation_timestamp();

            let es = EndpointSlice {
                type_meta: TypeMeta {
                    kind: "EndpointSlice".to_string(),
                    api_version: "discovery.k8s.io/v1".to_string(),
                },
                metadata,
                address_type: "IPv4".to_string(),
                endpoints: desired_endpoints,
                ports: desired_ports,
            };

            storage
                .create(ENDPOINTSLICE_KEY, &es)
                .await
                .context("Failed to create kubernetes EndpointSlice")?;
            info!("Created kubernetes EndpointSlice with IP: {}", ip);
        }
    }
    Ok(())
}

/// Reconcile both the `kubernetes` Endpoints and EndpointSlice to `ip:port`.
/// Idempotent and safe to call repeatedly (the interval reconciler does).
/// Create the `default/kubernetes` Service if it is missing.
///
/// Ports upstream's kubernetesservice controller
/// (`pkg/controlplane/instance.go:349` -> `kubernetesservice.New(...)`), which
/// creates and repairs this Service on every reconcile tick. The api-server owns
/// it because in-cluster clients depend on it existing: the kubelet derives
/// `KUBERNETES_SERVICE_HOST` / `KUBERNETES_SERVICE_PORT` from it, so without it
/// every client-go `InClusterConfig()` fails with
/// "unable to load in-cluster configuration".
///
/// Previously only the Endpoints were reconciled here and the Service came from
/// `bootstrap-cluster.yaml` — fine for the compose stack, but any cluster that
/// does not run `scripts/bootstrap-cluster.sh` had no Service at all. That is what
/// aborted the vanilla-swap api-server leg's conformance suite in 1ms (#1667).
///
/// Idempotent: an existing Service is left untouched (its ClusterIP is immutable
/// and may have been allocated by whoever created it first).
pub async fn reconcile_kubernetes_service<S: Storage + ?Sized>(
    storage: &S,
    api_server_port: u16,
) -> Result<()> {
    use rusternetes_common::resources::policy::IntOrString;
    use rusternetes_common::resources::{Service, ServicePort, ServiceSpec, ServiceType};
    use rusternetes_common::types::{ObjectMeta, TypeMeta};

    if storage.get::<Service>(SERVICE_KEY).await.is_ok() {
        return Ok(());
    }

    let mut metadata = ObjectMeta::new("kubernetes");
    metadata.namespace = Some("default".to_string());
    metadata.ensure_uid();
    metadata.ensure_creation_timestamp();
    // Upstream labels (`pkg/controlplane/controller/kubernetesservice`).
    let mut labels = std::collections::HashMap::new();
    labels.insert("component".to_string(), "apiserver".to_string());
    labels.insert("provider".to_string(), "kubernetes".to_string());
    metadata.labels = Some(labels);

    let service = Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata,
        spec: ServiceSpec {
            cluster_ip: Some(KUBERNETES_SERVICE_IP.to_string()),
            ports: vec![ServicePort {
                name: Some("https".to_string()),
                port: 443,
                target_port: Some(IntOrString::Int(api_server_port as i32)),
                protocol: "TCP".to_string(),
                node_port: None,
                app_protocol: None,
            }],
            service_type: Some(ServiceType::ClusterIP),
            // No selector: the api-server maintains the Endpoints itself.
            selector: None,
            // kube-proxy resolves a Service to its EndpointSlices BY IP FAMILY.
            // Without ipFamilies it matches no slice, decides the Service has
            // no endpoints, and installs a REJECT for the ClusterIP:
            //
            //   -A KUBE-SERVICES -d 10.96.0.1/32 -p tcp \
            //      --comment "default/kubernetes:https has no endpoints" -j REJECT
            //
            // so 10.96.0.1 is unroutable even with a present, ready
            // EndpointSlice. Observed live during the api-server swap:
            // kube-system/kube-dns had ipFamilies/ipFamilyPolicy/clusterIPs and
            // programmed 6 working DNAT rules; this Service had none of them
            // and got the REJECT above.
            //
            // Upstream sets ipFamilyPolicy and sessionAffinity explicitly when
            // it creates this Service
            // (pkg/controlplane/controller/kubernetesservice/controller.go:225-239)
            // and its registry allocator fills clusterIPs/ipFamilies on the way
            // in (pkg/registry/core/service/storage/alloc.go:239). We write
            // straight to storage, bypassing that allocator, so they have to be
            // set here or nothing sets them at all.
            cluster_ips: Some(vec![KUBERNETES_SERVICE_IP.to_string()]),
            ip_families: Some(vec![rusternetes_common::resources::service::IPFamily::IPv4]),
            ip_family_policy: Some(
                rusternetes_common::resources::service::IPFamilyPolicy::SingleStack,
            ),
            session_affinity: Some("None".to_string()),
            ..Default::default()
        },
        status: None,
    };

    storage
        .create(SERVICE_KEY, &service)
        .await
        .context("Failed to create kubernetes Service")?;
    info!(
        "Created default/kubernetes Service ({} :443 -> :{})",
        KUBERNETES_SERVICE_IP, api_server_port
    );
    Ok(())
}

pub async fn reconcile_kubernetes_endpoint<S: Storage + ?Sized>(
    storage: &S,
    ip: &str,
    port: u16,
) -> Result<()> {
    reconcile_endpoints(storage, ip, port).await?;
    reconcile_endpointslice(storage, ip, port).await?;
    Ok(())
}

/// Bootstrap the `kubernetes` Service Endpoints + EndpointSlice in the default
/// namespace, pointing them at this api-server's discovered IP. Run once at
/// startup so the endpoint is correct immediately; [`spawn_endpoint_reconciler`]
/// then keeps it correct across restarts / IP changes.
pub async fn bootstrap_kubernetes_service(
    storage: Arc<StorageBackend>,
    api_server_port: u16,
) -> Result<()> {
    info!("Bootstrapping kubernetes Service and Endpoints");
    let api_server_ip = get_api_server_ip().context("Failed to discover API server IP address")?;
    info!(
        "API server IP: {}, Port: {}",
        api_server_ip, api_server_port
    );
    reconcile_kubernetes_service(storage.as_ref(), api_server_port).await?;
    reconcile_kubernetes_endpoint(storage.as_ref(), &api_server_ip, api_server_port).await
}

/// Spawn the background reconciler that re-asserts the `kubernetes` endpoint to
/// the live api-server IP every [`ENDPOINT_RECONCILE_INTERVAL`]. Ports upstream's
/// `masterCountEndpointReconciler` run loop (`Controller.Run` ->
/// `wait.NonSlidingUntil(UpdateKubernetesService, EndpointInterval)`): the
/// endpoint self-heals after a container recreate (new bridge IP), a stale
/// write, or a clobber, without depending on the controller-manager.
pub fn spawn_endpoint_reconciler(
    storage: Arc<StorageBackend>,
    api_server_port: u16,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(ENDPOINT_RECONCILE_INTERVAL);
        // Skip the immediate first tick — startup bootstrap already ran.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let ip = match get_api_server_ip() {
                Ok(ip) => ip,
                Err(e) => {
                    warn!(
                        "endpoint reconciler: could not discover api-server IP: {}",
                        e
                    );
                    continue;
                }
            };
            if let Err(e) = reconcile_kubernetes_service(storage.as_ref(), api_server_port).await {
                warn!("endpoint reconciler: service reconcile failed: {}", e);
            }
            if let Err(e) =
                reconcile_kubernetes_endpoint(storage.as_ref(), &ip, api_server_port).await
            {
                warn!("endpoint reconciler: reconcile failed: {}", e);
            }
        }
    })
}

/// Spawn the APIService availability controller **inside the api-server**.
///
/// Upstream runs the aggregator's availability controller as part of
/// kube-apiserver (`kube-aggregator/pkg/controllers/status`), NOT in
/// kube-controller-manager. In the vanilla-module-swap the controller-manager
/// is the stock KCM, which does not run this controller, so a remote
/// `APIService` would never get an `Available` condition at all — the create
/// path deliberately writes none (upstream `PrepareForCreate`) — and every
/// aggregation client (`e2e Aggregator`, `kubectl get apiservices`) would hang
/// waiting for one. Running it here matches upstream placement and works
/// regardless of which controller-manager is deployed.
pub fn spawn_apiservice_availability_controller(
    storage: Arc<StorageBackend>,
) -> tokio::task::JoinHandle<()> {
    use rusternetes_controller_manager::controllers::apiservice::APIServiceAvailabilityController;
    tokio::spawn(async move {
        let controller = Arc::new(APIServiceAvailabilityController::new(storage));
        if let Err(e) = controller.run().await {
            warn!("APIService availability controller exited: {}", e);
        }
    })
}

// ---------------------------------------------------------------------------
// Default ServiceCIDR controller
// ---------------------------------------------------------------------------

/// Upstream `defaultservicecidr.DefaultServiceCIDRName`
/// (`pkg/controlplane/controller/defaultservicecidr/default_servicecidr_controller.go:47`).
pub const DEFAULT_SERVICE_CIDR_NAME: &str = "kubernetes";

/// Upstream `controllerName` (`default_servicecidr_controller.go:46`), used as
/// the event source component.
const DEFAULT_SERVICE_CIDR_CONTROLLER: &str = "kubernetes-service-cidr-controller";

/// The service range this api-server allocates ClusterIPs from — upstream's
/// `--service-cluster-ip-range`, which this api-server does not expose as a
/// flag. The `kubernetes` ServiceCIDR is seeded from it, and ClusterIPs are
/// allocated from the ServiceCIDRs. Must stay in step with
/// [`KUBERNETES_SERVICE_IP`] (the range's first address).
pub const DEFAULT_SERVICE_CIDRS: &[&str] = &["10.96.0.0/12"];

/// Upstream's controller interval (`default_servicecidr_controller.go:61`,
/// "same as DefaultEndpointReconcilerInterval", 10s).
pub const DEFAULT_SERVICE_CIDR_RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

/// Upstream `default_servicecidr_controller.go:236`. Applied with **no reason**
/// — the controller builds the condition without one, and ServiceCIDR status is
/// not condition-validated (`ValidateServiceCIDRStatusUpdate`,
/// `pkg/apis/networking/validation/validation.go:883-886`).
pub const DEFAULT_SERVICE_CIDR_READY_MESSAGE: &str = "Kubernetes default Service CIDR is ready";

/// Port of upstream's `kubernetes-service-cidr-controller`
/// (`pkg/controlplane/controller/defaultservicecidr`), which owns the
/// `kubernetes` ServiceCIDR and lives in the **apiserver**, not the KCM — it is
/// the only component that knows this process's service range.
///
/// Replaces the create-if-absent seed that used to be duplicated inline in
/// `main.rs` and `lib.rs`. Unlike that seed this reconciles: it upgrades
/// single-stack to dual-stack when the configured ranges grow, warns (once) via
/// an Event when the persisted CIDRs disagree with this api-server's
/// configuration, and applies `Ready=True` only when they agree.
///
/// The *deletion* half of ServiceCIDR lifecycle — the protection finalizer, the
/// `Ready=False`/`Terminating` condition, `canDeleteCIDR` — belongs to the
/// separate `service-cidr-controller` in the controller-manager
/// (`crates/controller-manager/src/controllers/servicecidr.rs`), exactly as
/// upstream splits it. This controller deliberately never touches the status of
/// a ServiceCIDR that is being deleted (`syncStatus`, `:188-193`).
pub struct DefaultServiceCIDRController<S: Storage + ?Sized> {
    storage: Arc<S>,
    /// Order matters: the first CIDR defines the default IP family.
    cidrs: Vec<String>,
    recorder: rusternetes_storage::EventRecorder<S>,
    reported_mismatched_cidrs: bool,
    reported_not_ready_condition: bool,
}

impl<S: Storage + ?Sized> DefaultServiceCIDRController<S> {
    pub fn new(storage: Arc<S>, cidrs: Vec<String>) -> Self {
        Self {
            recorder: rusternetes_storage::EventRecorder::new(Arc::clone(&storage)),
            storage,
            cidrs,
            reported_mismatched_cidrs: false,
            reported_not_ready_condition: false,
        }
    }

    fn key() -> String {
        rusternetes_storage::build_key("servicecidrs", None, DEFAULT_SERVICE_CIDR_NAME)
    }

    fn object_ref(
        sc: &rusternetes_common::resources::ServiceCIDR,
    ) -> rusternetes_common::resources::ObjectReference {
        rusternetes_common::resources::ObjectReference {
            kind: Some("ServiceCIDR".to_string()),
            namespace: None,
            name: Some(sc.metadata.name.clone()),
            uid: Some(sc.metadata.uid.clone()),
            api_version: Some("networking.k8s.io/v1".to_string()),
            resource_version: sc.metadata.resource_version.clone(),
            field_path: None,
        }
    }

    async fn warn_event(
        &self,
        sc: &rusternetes_common::resources::ServiceCIDR,
        reason: &str,
        message: &str,
    ) {
        let source = rusternetes_common::resources::EventSource {
            component: DEFAULT_SERVICE_CIDR_CONTROLLER.to_string(),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(
                &Self::object_ref(sc),
                &source,
                rusternetes_common::resources::EventType::Warning,
                reason,
                message,
            )
            .await
        {
            warn!(
                "default ServiceCIDR: could not record {} event: {}",
                reason, e
            );
        }
    }

    /// Upstream `sync` (`default_servicecidr_controller.go:142-185`).
    pub async fn sync(&mut self) -> Result<()> {
        use rusternetes_common::resources::{ServiceCIDR, ServiceCIDRSpec};
        use rusternetes_common::types::{ObjectMeta, TypeMeta};

        let key = Self::key();
        match self.storage.get::<ServiceCIDR>(&key).await {
            Ok(existing) => {
                let existing_cidrs = existing
                    .spec
                    .as_ref()
                    .map(|s| s.cidrs.clone())
                    .unwrap_or_default();
                // Single-stack -> dual-stack upgrade (`:148-156`).
                if self.cidrs.len() == 2
                    && existing_cidrs.len() == 1
                    && self.cidrs[0] == existing_cidrs[0]
                {
                    info!(
                        "Updating default ServiceCIDR from single-stack ({:?}) to dual-stack ({:?})",
                        existing_cidrs, self.cidrs
                    );
                    let mut updated = existing.clone();
                    updated.spec = Some(ServiceCIDRSpec {
                        cidrs: self.cidrs.clone(),
                    });
                    if let Err(e) = self.storage.update(&key, &updated).await {
                        warn!(
                            "The default ServiceCIDR can not be updated from {} to dual stack {:?}: {}",
                            self.cidrs[0], self.cidrs, e
                        );
                        self.warn_event(
                            &existing,
                            "KubernetesDefaultServiceCIDRError",
                            &format!(
                                "The default ServiceCIDR can not be upgraded from {} to dual stack {:?} : {}",
                                self.cidrs[0], self.cidrs, e
                            ),
                        )
                        .await;
                    }
                } else {
                    self.sync_status(&existing).await;
                }
                return Ok(());
            }
            Err(rusternetes_common::Error::NotFound(_)) => {}
            // Unknown error: retry on the next tick rather than racing a create
            // against a backend that is merely unreachable.
            Err(e) => return Err(e.into()),
        }

        // The default ServiceCIDR does not exist yet.
        info!("Creating default ServiceCIDR with CIDRs: {:?}", self.cidrs);
        let mut metadata = ObjectMeta::new(DEFAULT_SERVICE_CIDR_NAME);
        metadata.ensure_uid();
        metadata.ensure_creation_timestamp();
        let service_cidr = ServiceCIDR {
            type_meta: TypeMeta {
                kind: "ServiceCIDR".to_string(),
                api_version: "networking.k8s.io/v1".to_string(),
            },
            metadata,
            spec: Some(ServiceCIDRSpec {
                cidrs: self.cidrs.clone(),
            }),
            // No status on create — upstream's registry strategy clears it
            // (`pkg/registry/networking/servicecidr/strategy.go:67-71`) and
            // `syncStatus` below is what applies `Ready`.
            status: None,
        };
        let created = match self.storage.create(&key, &service_cidr).await {
            Ok(created) => created,
            // Another api-server replica won the race; fall through to status.
            Err(rusternetes_common::Error::AlreadyExists(_)) => {
                self.storage.get::<ServiceCIDR>(&key).await?
            }
            Err(e) => {
                self.warn_event(
                    &service_cidr,
                    "KubernetesDefaultServiceCIDRError",
                    "The default ServiceCIDR can not be created",
                )
                .await;
                return Err(e.into());
            }
        };
        self.sync_status(&created).await;
        Ok(())
    }

    /// Upstream `syncStatus` (`default_servicecidr_controller.go:187-245`).
    async fn sync_status(&mut self, sc: &rusternetes_common::resources::ServiceCIDR) {
        use rusternetes_common::resources::{ServiceCIDRCondition, ServiceCIDRStatus};

        // A ServiceCIDR being deleted belongs to the controller-manager's
        // service-cidr-controller; never fight it over the condition.
        if sc.metadata.deletion_timestamp.is_some() {
            return;
        }

        let spec_cidrs = sc
            .spec
            .as_ref()
            .map(|s| s.cidrs.clone())
            .unwrap_or_default();
        let same_config = spec_cidrs == self.cidrs;
        let ready = sc
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|c| c.iter().find(|c| c.condition_type == "Ready"))
            .cloned();

        if !same_config {
            if !self.reported_mismatched_cidrs {
                warn!(
                    "Inconsistent ServiceCIDR status for {}, controller configuration: {:?}, ServiceCIDR configuration: {:?}. Configure the flags to match current ServiceCIDR or manually delete it.",
                    sc.metadata.name, self.cidrs, spec_cidrs
                );
                self.warn_event(
                    sc,
                    "KubernetesDefaultServiceCIDRInconsistent",
                    &format!(
                        "The default ServiceCIDR {:?} does not match the controller flag configurations {:?}",
                        spec_cidrs, self.cidrs
                    ),
                )
                .await;
                self.reported_mismatched_cidrs = true;
            }
            // Inconsistent config is a problem regardless of the current Ready
            // condition; don't try to change it.
            return;
        }

        match ready {
            // Ready=False with matching config should not happen, and is not
            // ours to overwrite — the service-cidr-controller owns the
            // Terminating case. Report once and leave it for an operator.
            Some(c) if c.status == "False" => {
                if !self.reported_not_ready_condition {
                    warn!(
                        "Default ServiceCIDR {} condition Ready is False, but controller configuration matches. Please validate your cluster's network configuration. reason={} message={}",
                        sc.metadata.name, c.reason, c.message
                    );
                    let reason = if c.reason.is_empty() {
                        "KubernetesDefaultServiceCIDRError"
                    } else {
                        c.reason.as_str()
                    };
                    self.warn_event(
                        sc,
                        reason,
                        &format!("Configuration matches, but {}", c.message),
                    )
                    .await;
                    self.reported_not_ready_condition = true;
                }
            }
            // Already Ready=True and the config matches: nothing to do.
            Some(c) if c.status == "True" => {}
            // Missing or Unknown: this is ours to set.
            _ => {
                info!("Setting default ServiceCIDR condition Ready to True");
                let mut updated = sc.clone();
                updated.status = Some(ServiceCIDRStatus {
                    conditions: Some(vec![ServiceCIDRCondition {
                        condition_type: "Ready".to_string(),
                        status: "True".to_string(),
                        observed_generation: sc.metadata.generation,
                        last_transition_time: Some(
                            chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        ),
                        reason: String::new(),
                        message: DEFAULT_SERVICE_CIDR_READY_MESSAGE.to_string(),
                    }]),
                });
                if let Err(e) = self.storage.update_status(&Self::key(), &updated).await {
                    warn!("error updating default ServiceCIDR status: {}", e);
                    self.warn_event(
                        sc,
                        "KubernetesDefaultServiceCIDRError",
                        "The default ServiceCIDR Status can not be set to Ready=True",
                    )
                    .await;
                }
            }
        }
    }
}

/// Run the default-ServiceCIDR controller: one synchronous sync so the
/// `kubernetes` ServiceCIDR exists before this api-server starts serving, then
/// a background reconcile every [`DEFAULT_SERVICE_CIDR_RECONCILE_INTERVAL`].
/// Mirrors upstream `Controller.Start` (`default_servicecidr_controller.go:101-140`),
/// which likewise blocks on a first successful sync before returning.
pub async fn start_default_servicecidr_controller(
    storage: Arc<StorageBackend>,
) -> tokio::task::JoinHandle<()> {
    let cidrs: Vec<String> = DEFAULT_SERVICE_CIDRS
        .iter()
        .map(|c| c.to_string())
        .collect();
    let mut controller = DefaultServiceCIDRController::new(storage, cidrs);
    if let Err(e) = controller.sync().await {
        warn!("error initializing the default ServiceCIDR: {}", e);
    }
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(DEFAULT_SERVICE_CIDR_RECONCILE_INTERVAL);
        ticker.tick().await; // startup sync already ran
        loop {
            ticker.tick().await;
            if let Err(e) = controller.sync().await {
                warn!("error trying to sync the default ServiceCIDR: {}", e);
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Service NodePort repair
// ---------------------------------------------------------------------------

/// How long startup waits for the first successful repair pass before it
/// gives up (storage_core.go:535-539).
const INITIAL_REPAIR_TIMEOUT: Duration = Duration::from_secs(60);

/// One NodePort repair pass over `state`'s allocator — what the
/// `start-service-ip-repair-controllers` post-start hook waits for before
/// the api-server reports ready. Until it has run, the allocation snapshot
/// may not exist and no NodePort can be allocated
/// (allocator/storage/storage.go:154-157).
// Used by the test harness (test_support), not by the binary.
#[allow(dead_code)]
pub async fn repair_service_node_ports_once(
    state: &crate::state::ApiServerState,
) -> rusternetes_common::Result<()> {
    use crate::registry::core::service::portallocator::repair::Repair;
    Repair::new(
        state.storage.clone(),
        state.node_port_allocator.port_range(),
        state.node_port_registry.clone(),
    )
    .run_once()
    .await
}

/// One ClusterIP repair pass (`RepairIPAddress.runOnce`), for the test
/// harness: it creates the IPAddresses of Services stored without one.
// Used by the test harness (test_support), not by the binary.
#[allow(dead_code)]
pub async fn repair_service_cluster_ips_once(
    state: &crate::state::ApiServerState,
) -> rusternetes_common::Result<()> {
    use crate::registry::core::service::ipallocator::repair::RepairIpAddress;
    RepairIpAddress::new(state.storage.clone()).run_once().await
}

/// Run `pass` now and every `REPAIR_INTERVAL` after, signalling the first
/// success (the `onFirstSuccess` callback of both `RunUntil`s).
fn spawn_repair_loop<F, Fut>(
    name: &'static str,
    mut pass: F,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
)
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = rusternetes_common::Result<()>> + Send,
{
    use crate::registry::core::service::portallocator::repair::REPAIR_INTERVAL;
    let (first_success, first) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let mut first_success = Some(first_success);
        loop {
            match pass().await {
                Ok(()) => {
                    if let Some(tx) = first_success.take() {
                        let _ = tx.send(());
                    }
                }
                Err(e) => warn!("{name}: {e}"),
            }
            tokio::time::sleep(REPAIR_INTERVAL).await;
        }
    });
    (handle, first)
}

/// The `start-service-ip-repair-controllers` post-start hook
/// (pkg/registry/core/rest/storage_core.go:504-540): run the ClusterIP
/// repair (`RepairIPAddress`, storage_core.go:142-148) and the NodePort
/// repair (`portallocator/controller.Repair`, :128) every
/// `REPAIR_INTERVAL`, and fail startup unless both first passes succeed
/// within one minute.
pub async fn start_service_ip_repair_controllers(
    state: &crate::state::ApiServerState,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
    use crate::registry::core::service::ipallocator::repair::RepairIpAddress;
    use crate::registry::core::service::portallocator::repair::Repair;

    let ip_repair = Arc::new(RepairIpAddress::new(state.storage.clone()));
    let (ip_handle, ip_first) = spawn_repair_loop("service ClusterIP repair", move || {
        let ip_repair = ip_repair.clone();
        async move {
            // Wait for the default ServiceCIDR (repairip.go:196-206).
            while !ip_repair.default_service_cidr_exists().await {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            ip_repair.run_once().await
        }
    });

    let port_repair = Arc::new(tokio::sync::Mutex::new(Repair::new(
        state.storage.clone(),
        state.node_port_allocator.port_range(),
        state.node_port_registry.clone(),
    )));
    let (port_handle, port_first) = spawn_repair_loop("service NodePort repair", move || {
        let port_repair = port_repair.clone();
        async move { port_repair.lock().await.run_once().await }
    });

    let both = async {
        let (a, b) = tokio::join!(ip_first, port_first);
        a.is_ok() && b.is_ok()
    };
    match tokio::time::timeout(INITIAL_REPAIR_TIMEOUT, both).await {
        Ok(true) => Ok(vec![ip_handle, port_handle]),
        _ => {
            ip_handle.abort();
            port_handle.abort();
            Err(anyhow::anyhow!(
                "unable to perform initial IP and Port allocation check"
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// ClusterAuthenticationTrust controller
// ---------------------------------------------------------------------------

/// Upstream's resync: `wait.PollImmediateUntil(1*time.Minute, ...)`
/// (cluster_authentication_trust_controller.go:470-476).
const CLUSTER_AUTHENTICATION_TRUST_RESYNC: Duration = Duration::from_secs(60);

/// The CA bundle this api-server publishes, resolved the way the Namespace
/// create handler resolved it before this controller existed: the
/// well-known filesystem paths, then the serving CA.
fn resolve_cluster_ca(ca_cert_pem: Option<&str>) -> Option<String> {
    [
        "/etc/kubernetes/pki/ca.crt",
        "/etc/kubernetes/pki/api-server.crt",
        "/root/.rusternetes/certs/ca.crt",
    ]
    .iter()
    .find_map(|p| std::fs::read_to_string(p).ok())
    .or_else(|| ca_cert_pem.map(str::to_string))
    .filter(|ca| !ca.is_empty())
}

/// This api-server's `ClusterAuthenticationInfo`
/// (pkg/controlplane/apiserver/config.go:71). Upstream fills it from
/// `--client-ca-file` and the `--requestheader-*` flags, which this
/// api-server does not take yet (#1577). Until it does, the effective
/// configuration is published: the cluster CA for both client and front
/// proxy, and the identity headers the aggregator proxy sends
/// (`handlers::aggregator::build_proxy_headers`).
pub fn cluster_authentication_info(
    ca_cert_pem: Option<&str>,
) -> rusternetes_common::clusterauthenticationtrust::ClusterAuthenticationInfo {
    let ca = resolve_cluster_ca(ca_cert_pem);
    rusternetes_common::clusterauthenticationtrust::ClusterAuthenticationInfo {
        client_ca: ca.clone(),
        request_header_username_headers: Some(vec!["X-Remote-User".to_string()]),
        request_header_uid_headers: None,
        request_header_group_headers: Some(vec!["X-Remote-Group".to_string()]),
        request_header_extra_header_prefixes: Some(vec!["X-Remote-Extra-".to_string()]),
        request_header_allowed_names: Some(Vec::new()),
        request_header_ca: ca,
    }
}

/// `createNamespaceIfNeeded` (cluster_authentication_trust_controller.go:181-197).
/// Upstream creates through the API; this writes the object the Namespace
/// strategy would have stored (phase Active, the `kubernetes` finalizer, the
/// `kubernetes.io/metadata.name` label).
async fn create_namespace_if_needed(storage: &StorageBackend, ns: &str) -> Result<()> {
    use rusternetes_common::resources::Namespace;
    let key = rusternetes_storage::build_key("namespaces", None, ns);
    if storage.get::<Namespace>(&key).await.is_ok() {
        return Ok(());
    }
    let namespace = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": ns,
            "uid": uuid::Uuid::new_v4().to_string(),
            "creationTimestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "labels": {"kubernetes.io/metadata.name": ns}
        },
        "spec": {"finalizers": ["kubernetes"]},
        "status": {"phase": "Active"}
    });
    match storage.create(&key, &namespace).await {
        Ok(_) | Err(rusternetes_common::Error::AlreadyExists(_)) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// `syncConfigMap` (cluster_authentication_trust_controller.go:140-179) with
/// `writeConfigMap` (:199-220): update, or create when absent.
pub async fn sync_cluster_authentication_trust(
    storage: &StorageBackend,
    required: &rusternetes_common::clusterauthenticationtrust::ClusterAuthenticationInfo,
) -> Result<()> {
    use rusternetes_common::clusterauthenticationtrust::{
        sync_config_map_data, CONFIG_MAP_NAME, CONFIG_MAP_NAMESPACE,
    };
    use rusternetes_common::resources::ConfigMap;

    let key =
        rusternetes_storage::build_key("configmaps", Some(CONFIG_MAP_NAMESPACE), CONFIG_MAP_NAME);
    let existing = match storage.get::<ConfigMap>(&key).await {
        Ok(cm) => Some(cm),
        Err(rusternetes_common::Error::NotFound(_)) => None,
        Err(e) => return Err(e.into()),
    };
    let existing_data = existing
        .as_ref()
        .map(|cm| cm.data.clone().unwrap_or_default());
    // `RemoteRequestHeaderUID` is beta and on by default (kube_features.go:2059-2062).
    let Some(data) = sync_config_map_data(
        existing_data.as_ref(),
        required,
        true,
        chrono::Utc::now().timestamp(),
    )
    .map_err(|e| anyhow::anyhow!("{CONFIG_MAP_NAMESPACE}/{CONFIG_MAP_NAME}: {e}"))?
    else {
        return Ok(());
    };
    info!("writing updated authentication info to {CONFIG_MAP_NAMESPACE} configmaps/{CONFIG_MAP_NAME}");
    create_namespace_if_needed(storage, CONFIG_MAP_NAMESPACE).await?;
    match existing {
        Some(mut cm) => {
            cm.data = Some(data);
            storage.update(&key, &cm).await?;
        }
        None => {
            let mut cm = ConfigMap {
                type_meta: rusternetes_common::types::TypeMeta {
                    kind: "ConfigMap".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: rusternetes_common::types::ObjectMeta::new(CONFIG_MAP_NAME)
                    .with_namespace(CONFIG_MAP_NAMESPACE.to_string()),
                data: Some(data),
                binary_data: None,
                immutable: None,
            };
            cm.metadata.ensure_uid();
            cm.metadata.ensure_creation_timestamp();
            storage.create(&key, &cm).await?;
        }
    }
    Ok(())
}

/// The ClusterAuthenticationTrust controller, which kube-apiserver runs as
/// the `start-cluster-authentication-info-controller` post-start hook
/// (pkg/controlplane/apiserver/server.go:248-282): keep
/// `kube-system/extension-apiserver-authentication` holding how aggregated
/// api-servers should authenticate this one. It syncs at once, on every
/// add or delete of that ConfigMap (:117-135), and every minute (:470-476).
pub fn spawn_cluster_authentication_trust_controller(
    storage: Arc<StorageBackend>,
    required: rusternetes_common::clusterauthenticationtrust::ClusterAuthenticationInfo,
) -> tokio::task::JoinHandle<()> {
    use futures::StreamExt;
    use rusternetes_common::clusterauthenticationtrust::{CONFIG_MAP_NAME, CONFIG_MAP_NAMESPACE};
    use rusternetes_storage::WatchEvent;

    tokio::spawn(async move {
        let key = rusternetes_storage::build_key(
            "configmaps",
            Some(CONFIG_MAP_NAMESPACE),
            CONFIG_MAP_NAME,
        );
        let mut ticker = tokio::time::interval(CLUSTER_AUTHENTICATION_TRUST_RESYNC);
        let mut watch = None;
        loop {
            if watch.is_none() {
                watch = storage.watch(&key).await.ok();
            }
            let event = async {
                match watch.as_mut() {
                    Some(w) => w.next().await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                _ = ticker.tick() => {}
                ev = event => match ev {
                    Some(Ok(WatchEvent::Added(k, _) | WatchEvent::Deleted(k, _))) if k == key => {}
                    Some(Ok(_)) => continue,
                    // A broken watch is re-established on the next pass.
                    Some(Err(_)) | None => {
                        watch = None;
                        continue;
                    }
                },
            }
            if let Err(e) = sync_cluster_authentication_trust(storage.as_ref(), &required).await {
                warn!("cluster_authentication_trust_controller: {e}");
            }
        }
    })
}

/// The `kube-system` Role and RoleBinding of upstream's bootstrap namespace
/// policy that concern `extension-apiserver-authentication`
/// (plugin/pkg/auth/authorizer/rbac/bootstrappolicy/namespace_policy.go:75-82,
/// 124-126): read access to the ConfigMap, granted to the controller-manager
/// and the scheduler. `EnsureRBACPolicy` (pkg/registry/rbac/rest/
/// storage_rbac.go:269-331) creates each, creating the namespace first
/// (`tryEnsureNamespace`, component-helpers/auth/rbac/reconciliation/
/// namespace.go:31-45). Only the create half is ported, and only these two
/// objects. The rest of the bootstrap policy is `bootstrap_default_rbac`.
pub async fn bootstrap_extension_apiserver_authentication_rbac(
    storage: Arc<StorageBackend>,
) -> Result<()> {
    use rusternetes_common::clusterauthenticationtrust::{CONFIG_MAP_NAME, CONFIG_MAP_NAMESPACE};

    const ROLE: &str = "extension-apiserver-authentication-reader";
    const BINDING: &str = "system::extension-apiserver-authentication-reader";
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let objects = [
        (
            rusternetes_storage::build_key("roles", Some(CONFIG_MAP_NAMESPACE), ROLE),
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": {
                    "name": ROLE,
                    "namespace": CONFIG_MAP_NAMESPACE,
                    "uid": uuid::Uuid::new_v4().to_string(),
                    "creationTimestamp": now,
                    "labels": {"kubernetes.io/bootstrapping": "rbac-defaults"},
                    "annotations": {"rbac.authorization.kubernetes.io/autoupdate": "true"}
                },
                "rules": [{
                    "apiGroups": [""],
                    "resources": ["configmaps"],
                    "resourceNames": [CONFIG_MAP_NAME],
                    "verbs": ["get", "list", "watch"]
                }]
            }),
        ),
        (
            rusternetes_storage::build_key("rolebindings", Some(CONFIG_MAP_NAMESPACE), BINDING),
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "name": BINDING,
                    "namespace": CONFIG_MAP_NAMESPACE,
                    "uid": uuid::Uuid::new_v4().to_string(),
                    "creationTimestamp": now,
                    "labels": {"kubernetes.io/bootstrapping": "rbac-defaults"},
                    "annotations": {"rbac.authorization.kubernetes.io/autoupdate": "true"}
                },
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "Role",
                    "name": ROLE
                },
                "subjects": [
                    {"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": "system:kube-controller-manager"},
                    {"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": "system:kube-scheduler"}
                ]
            }),
        ),
    ];
    create_namespace_if_needed(storage.as_ref(), CONFIG_MAP_NAMESPACE).await?;
    for (key, obj) in objects {
        if storage.get::<serde_json::Value>(&key).await.is_ok() {
            continue;
        }
        match storage.create(&key, &obj).await {
            Ok(_) | Err(rusternetes_common::Error::AlreadyExists(_)) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::rbac::ClusterRoleBinding;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn creates_endpoints_and_slice_when_missing() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_endpoint(&storage, "10.89.0.5", 6443)
            .await
            .unwrap();

        let ep: Endpoints = storage.get(ENDPOINTS_KEY).await.unwrap();
        assert_eq!(ep.subsets[0].addresses.as_ref().unwrap()[0].ip, "10.89.0.5");
        assert_eq!(ep.subsets[0].ports.as_ref().unwrap()[0].port, 6443);
        // Mirroring is suppressed; the apiserver owns the slice.
        assert_eq!(
            ep.metadata
                .labels
                .as_ref()
                .unwrap()
                .get(SKIP_MIRROR_LABEL)
                .map(String::as_str),
            Some("true")
        );

        let es: EndpointSlice = storage.get(ENDPOINTSLICE_KEY).await.unwrap();
        assert_eq!(es.endpoints[0].addresses, vec!["10.89.0.5".to_string()]);
        assert_eq!(es.ports[0].port, Some(6443));
    }

    /// #1659: bootstrap seeds the cluster-admin ClusterRole + a binding to the
    /// system:masters group on an empty store, and is idempotent. This is what
    /// authorizes kubeadm's `O=system:masters` admin cert before any RBAC is
    /// applied (via a real rule, so the escalation check stays rule-based).
    #[tokio::test]
    async fn seeds_cluster_admin_for_system_masters() {
        let storage = Arc::new(StorageBackend::new_memory());

        bootstrap_default_rbac(storage.clone()).await.unwrap();
        // Idempotent: a second run must not error or duplicate.
        bootstrap_default_rbac(storage.clone()).await.unwrap();

        let role: ClusterRole = storage.get(CLUSTER_ADMIN_ROLE_KEY).await.unwrap();
        assert!(
            role.rules.iter().any(|r| r.verbs.contains(&"*".to_string())
                && r.resources
                    .as_ref()
                    .is_some_and(|x| x.contains(&"*".to_string()))),
            "cluster-admin must grant wildcard resource access"
        );

        let binding: ClusterRoleBinding = storage.get(CLUSTER_ADMIN_BINDING_KEY).await.unwrap();
        assert_eq!(binding.role_ref.name, "cluster-admin");
        assert!(
            binding
                .subjects
                .iter()
                .any(|s| s.kind == "Group" && s.name == "system:masters"),
            "binding must target the system:masters group"
        );
    }

    /// #1753: the whole upstream bootstrap policy is seeded, not only
    /// cluster-admin. Names are those of upstream's
    /// `bootstrappolicy/testdata/{cluster-roles,cluster-role-bindings,
    /// namespace-roles}.yaml` (release-1.35).
    #[tokio::test]
    async fn seeds_upstream_bootstrap_policy() {
        let storage = Arc::new(StorageBackend::new_memory());
        bootstrap_default_rbac(storage.clone()).await.unwrap();
        bootstrap_default_rbac(storage.clone()).await.unwrap();

        let role: ClusterRole = storage
            .get("/registry/clusterroles/system:service-account-issuer-discovery")
            .await
            .expect("issuer-discovery ClusterRole (policy.go:555-566)");
        assert_eq!(role.rules.len(), 1);
        let urls = role.rules[0].non_resource_urls.clone().unwrap();
        assert_eq!(
            urls,
            [
                "/.well-known/openid-configuration",
                "/.well-known/openid-configuration/",
                "/openid/v1/jwks",
                "/openid/v1/jwks/"
            ]
        );
        assert_eq!(role.rules[0].verbs, ["get"]);
        let labels = role.metadata.labels.clone().unwrap();
        assert_eq!(
            labels
                .get("kubernetes.io/bootstrapping")
                .map(String::as_str),
            Some("rbac-defaults")
        );
        assert_eq!(
            role.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("rbac.authorization.kubernetes.io/autoupdate"))
                .map(String::as_str),
            Some("true")
        );

        let binding: ClusterRoleBinding = storage
            .get("/registry/clusterrolebindings/system:service-account-issuer-discovery")
            .await
            .expect("issuer-discovery binding (policy.go:709)");
        assert_eq!(binding.subjects[0].name, "system:serviceaccounts");

        for name in [
            "admin",
            "edit",
            "view",
            "system:discovery",
            "system:basic-user",
            "system:public-info-viewer",
            "system:node",
            "system:node-proxier",
            "system:kube-scheduler",
            "system:kube-controller-manager",
            "system:controller:replicaset-controller",
            "system:controller:generic-garbage-collector",
        ] {
            storage
                .get::<ClusterRole>(&format!("/registry/clusterroles/{name}"))
                .await
                .unwrap_or_else(|e| panic!("ClusterRole {name} missing: {e}"));
        }
        for name in [
            "system:discovery",
            "system:basic-user",
            "system:public-info-viewer",
            "system:node-proxier",
            "system:kube-scheduler",
            "system:kube-controller-manager",
            "system:controller:replicaset-controller",
        ] {
            storage
                .get::<ClusterRoleBinding>(&format!("/registry/clusterrolebindings/{name}"))
                .await
                .unwrap_or_else(|e| panic!("ClusterRoleBinding {name} missing: {e}"));
        }
        // namespace-roles.yaml
        storage
            .get::<serde_json::Value>(
                "/registry/roles/kube-system/system::leader-locking-kube-scheduler",
            )
            .await
            .expect("kube-system leader-locking Role");
        // admin aggregates the aggregate-to-admin roles, so it must not be empty.
        let admin: ClusterRole = storage.get("/registry/clusterroles/admin").await.unwrap();
        assert!(
            !admin.rules.is_empty(),
            "aggregated admin rules materialised"
        );
    }

    /// The api-server must own the `default/kubernetes` Service, as upstream's
    /// kubernetesservice controller does (`pkg/controlplane/instance.go:349`,
    /// which creates AND repairs it on every reconcile tick).
    ///
    /// Ours only ever reconciled the Endpoints. In the compose stack the Service
    /// comes from bootstrap-cluster.yaml, so nothing was visibly broken — but on a
    /// cluster that does not run that script (the vanilla-swap api-server leg, and
    /// any real mixed deployment) there is no Service at all, so the kubelet
    /// injects no KUBERNETES_SERVICE_HOST/PORT and every in-cluster client dies:
    ///
    /// ```text
    /// Error loading client: error creating client: unable to load in-cluster
    /// configuration, KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be defined
    /// ```
    ///
    /// which aborted the whole conformance suite in 1ms (#1667).
    #[tokio::test]
    async fn creates_the_kubernetes_service_when_absent() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_service(&storage, 6443).await.unwrap();

        let svc: rusternetes_common::resources::Service =
            storage.get(SERVICE_KEY).await.expect("kubernetes Service");
        assert_eq!(svc.spec.cluster_ip.as_deref(), Some(KUBERNETES_SERVICE_IP));
        let ports = &svc.spec.ports;
        assert_eq!(ports[0].port, 443);
        assert_eq!(
            ports[0].target_port.as_ref().map(|t| format!("{t:?}")),
            Some(format!(
                "{:?}",
                rusternetes_common::resources::policy::IntOrString::Int(6443)
            ))
        );
        let labels = svc.metadata.labels.as_ref().expect("labels");
        assert_eq!(
            labels.get("component").map(String::as_str),
            Some("apiserver")
        );
        assert_eq!(
            labels.get("provider").map(String::as_str),
            Some("kubernetes")
        );
    }

    /// kube-proxy resolves a Service to its EndpointSlices BY IP FAMILY, so a
    /// `kubernetes` Service without `ipFamilies` yields no usable endpoints and
    /// kube-proxy installs a REJECT rule for 10.96.0.1 with the comment
    /// "default/kubernetes:https has no endpoints" — the ClusterIP is then
    /// unroutable even though the EndpointSlice is present and ready.
    ///
    /// Upstream sets `IPFamilyPolicy: SingleStack` and `SessionAffinity: None`
    /// explicitly when it creates this Service
    /// (`pkg/controlplane/controller/kubernetesservice/controller.go:225-239`),
    /// and the registry allocator fills `ClusterIPs`/`IPFamilies` on the way in
    /// (`pkg/registry/core/service/storage/alloc.go:239`). We write straight to
    /// storage and so bypass that allocator, which is why they must be set
    /// here. Observed live: `kube-dns` had all three and worked; this Service
    /// had none of them and did not.
    #[tokio::test]
    async fn kubernetes_service_carries_the_ip_family_fields_kube_proxy_needs() {
        use rusternetes_common::resources::service::{IPFamily, IPFamilyPolicy};

        let storage = MemoryStorage::new();
        reconcile_kubernetes_service(&storage, 6443).await.unwrap();
        let svc: rusternetes_common::resources::Service =
            storage.get(SERVICE_KEY).await.expect("kubernetes Service");

        assert_eq!(
            svc.spec.cluster_ips.as_deref(),
            Some([KUBERNETES_SERVICE_IP.to_string()].as_slice()),
            "clusterIPs must mirror clusterIP — the registry allocator would have \
             set it, and kube-proxy reads it"
        );
        assert_eq!(
            svc.spec.ip_families.as_deref(),
            Some([IPFamily::IPv4].as_slice()),
            "ipFamilies must be [IPv4]; without it kube-proxy matches no \
             EndpointSlice and REJECTs the ClusterIP"
        );
        assert_eq!(
            svc.spec.ip_family_policy,
            Some(IPFamilyPolicy::SingleStack),
            "upstream sets IPFamilyPolicy: SingleStack explicitly"
        );
        assert_eq!(
            svc.spec.session_affinity.as_deref(),
            Some("None"),
            "upstream sets SessionAffinity: None explicitly"
        );
    }

    /// Reconcile runs every tick, so it must be idempotent.
    #[tokio::test]
    async fn reconciling_the_service_twice_is_stable() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_service(&storage, 6443).await.unwrap();
        let first: rusternetes_common::resources::Service = storage.get(SERVICE_KEY).await.unwrap();
        reconcile_kubernetes_service(&storage, 6443).await.unwrap();
        let second: rusternetes_common::resources::Service =
            storage.get(SERVICE_KEY).await.unwrap();
        assert_eq!(first.metadata.uid, second.metadata.uid, "must not recreate");
        assert_eq!(
            second.spec.cluster_ip.as_deref(),
            Some(KUBERNETES_SERVICE_IP)
        );
    }

    /// And it must self-heal: upstream recreates the Service if it is deleted.
    #[tokio::test]
    async fn recreates_the_service_after_deletion() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_service(&storage, 6443).await.unwrap();
        storage.delete(SERVICE_KEY).await.unwrap();
        reconcile_kubernetes_service(&storage, 6443).await.unwrap();
        let svc: rusternetes_common::resources::Service = storage
            .get(SERVICE_KEY)
            .await
            .expect("Service must be recreated after deletion");
        assert_eq!(svc.spec.cluster_ip.as_deref(), Some(KUBERNETES_SERVICE_IP));
    }

    #[tokio::test]
    async fn updates_ip_on_recreate() {
        let storage = MemoryStorage::new();
        reconcile_kubernetes_endpoint(&storage, "172.18.0.2", 6443)
            .await
            .unwrap();
        // Simulate api-server container recreate with a new bridge IP.
        reconcile_kubernetes_endpoint(&storage, "172.18.0.9", 6443)
            .await
            .unwrap();

        let ep: Endpoints = storage.get(ENDPOINTS_KEY).await.unwrap();
        assert_eq!(
            ep.subsets[0].addresses.as_ref().unwrap()[0].ip,
            "172.18.0.9"
        );
        let es: EndpointSlice = storage.get(ENDPOINTSLICE_KEY).await.unwrap();
        assert_eq!(es.endpoints[0].addresses, vec!["172.18.0.9".to_string()]);
    }

    #[tokio::test]
    async fn repairs_malformed_empty_subsets() {
        let storage = MemoryStorage::new();
        // An endpoint stuck with no subsets — the failure mode the old one-shot
        // bootstrap could not repair (its update path only touched an existing
        // first address).
        use rusternetes_common::types::{ObjectMeta, TypeMeta};
        let mut metadata = ObjectMeta::new("kubernetes");
        metadata.namespace = Some("default".to_string());
        let broken = Endpoints {
            type_meta: TypeMeta {
                kind: "Endpoints".to_string(),
                api_version: "v1".to_string(),
            },
            metadata,
            subsets: vec![],
        };
        storage.create(ENDPOINTS_KEY, &broken).await.unwrap();

        reconcile_kubernetes_endpoint(&storage, "10.1.2.3", 6443)
            .await
            .unwrap();

        let ep: Endpoints = storage.get(ENDPOINTS_KEY).await.unwrap();
        assert_eq!(ep.subsets[0].addresses.as_ref().unwrap()[0].ip, "10.1.2.3");
        assert_eq!(
            ep.metadata
                .labels
                .as_ref()
                .unwrap()
                .get(SKIP_MIRROR_LABEL)
                .map(String::as_str),
            Some("true")
        );
    }

    // --- default ServiceCIDR controller -----------------------------------

    use rusternetes_common::resources::{ServiceCIDR, ServiceCIDRCondition, ServiceCIDRStatus};

    fn default_cidrs() -> Vec<String> {
        DEFAULT_SERVICE_CIDRS
            .iter()
            .map(|c| c.to_string())
            .collect()
    }

    fn sc_key() -> String {
        rusternetes_storage::build_key("servicecidrs", None, DEFAULT_SERVICE_CIDR_NAME)
    }

    fn controller(
        storage: &Arc<MemoryStorage>,
        cidrs: Vec<String>,
    ) -> DefaultServiceCIDRController<MemoryStorage> {
        DefaultServiceCIDRController::new(Arc::clone(storage), cidrs)
    }

    async fn stored(storage: &MemoryStorage) -> ServiceCIDR {
        storage
            .get::<ServiceCIDR>(&sc_key())
            .await
            .expect("default ServiceCIDR exists")
    }

    fn ready(sc: &ServiceCIDR) -> Option<ServiceCIDRCondition> {
        sc.status
            .as_ref()?
            .conditions
            .as_ref()?
            .iter()
            .find(|c| c.condition_type == "Ready")
            .cloned()
    }

    #[tokio::test]
    async fn creates_the_default_servicecidr_and_marks_it_ready() {
        let storage = Arc::new(MemoryStorage::new());
        controller(&storage, default_cidrs()).sync().await.unwrap();

        let sc = stored(&storage).await;
        assert_eq!(sc.spec.as_ref().unwrap().cidrs, default_cidrs());
        let cond = ready(&sc).expect("Ready condition applied by syncStatus");
        assert_eq!(cond.status, "True");
        assert_eq!(cond.message, DEFAULT_SERVICE_CIDR_READY_MESSAGE);
        assert_eq!(
            cond.reason, "",
            "upstream applies Ready=True with no reason"
        );
    }

    #[tokio::test]
    async fn sync_is_idempotent() {
        let storage = Arc::new(MemoryStorage::new());
        let mut c = controller(&storage, default_cidrs());
        c.sync().await.unwrap();
        let first = ready(&stored(&storage).await).unwrap();
        c.sync().await.unwrap();
        let second = ready(&stored(&storage).await).unwrap();
        assert_eq!(first.last_transition_time, second.last_transition_time);
    }

    #[tokio::test]
    async fn upgrades_single_stack_to_dual_stack() {
        let storage = Arc::new(MemoryStorage::new());
        controller(&storage, vec!["10.96.0.0/12".into()])
            .sync()
            .await
            .unwrap();

        let dual = vec!["10.96.0.0/12".to_string(), "2001:db8::/112".to_string()];
        controller(&storage, dual.clone()).sync().await.unwrap();

        assert_eq!(stored(&storage).await.spec.unwrap().cidrs, dual);
    }

    #[tokio::test]
    async fn mismatched_cidrs_leave_the_condition_alone() {
        let storage = Arc::new(MemoryStorage::new());
        // Persisted range disagrees with this api-server's configuration.
        controller(&storage, vec!["10.0.0.0/16".into()])
            .sync()
            .await
            .unwrap();
        let mut sc = stored(&storage).await;
        sc.status = None;
        storage.update(&sc_key(), &sc).await.unwrap();

        controller(&storage, vec!["10.96.0.0/12".into()])
            .sync()
            .await
            .unwrap();

        let sc = stored(&storage).await;
        assert_eq!(
            sc.spec.as_ref().unwrap().cidrs,
            vec!["10.0.0.0/16".to_string()],
            "a mismatch is reported, never silently rewritten"
        );
        assert!(
            ready(&sc).is_none(),
            "inconsistent config must not be marked Ready"
        );
    }

    #[tokio::test]
    async fn never_touches_the_status_of_a_terminating_servicecidr() {
        let storage = Arc::new(MemoryStorage::new());
        controller(&storage, default_cidrs()).sync().await.unwrap();

        // The controller-manager owns the terminating path; clear Ready and
        // mark the object deleting the way a DELETE + finalizer would.
        let mut sc = stored(&storage).await;
        sc.status = Some(ServiceCIDRStatus { conditions: None });
        sc.metadata.deletion_timestamp = Some(chrono::Utc::now());
        sc.metadata.finalizers = Some(vec!["networking.k8s.io/service-cidr-finalizer".into()]);
        storage.update(&sc_key(), &sc).await.unwrap();

        controller(&storage, default_cidrs()).sync().await.unwrap();

        assert!(
            ready(&stored(&storage).await).is_none(),
            "a deleting ServiceCIDR must not be marked Ready again"
        );
    }

    #[tokio::test]
    async fn does_not_overwrite_a_ready_false_condition() {
        let storage = Arc::new(MemoryStorage::new());
        controller(&storage, default_cidrs()).sync().await.unwrap();

        let mut sc = stored(&storage).await;
        sc.status = Some(ServiceCIDRStatus {
            conditions: Some(vec![ServiceCIDRCondition {
                condition_type: "Ready".to_string(),
                status: "False".to_string(),
                observed_generation: None,
                last_transition_time: None,
                reason: "Terminating".to_string(),
                message: "blocked".to_string(),
            }]),
        });
        storage.update(&sc_key(), &sc).await.unwrap();

        controller(&storage, default_cidrs()).sync().await.unwrap();

        let cond = ready(&stored(&storage).await).unwrap();
        assert_eq!(
            cond.status, "False",
            "Ready=False is another component's to clear, not ours"
        );
        assert_eq!(cond.reason, "Terminating");
    }
}
