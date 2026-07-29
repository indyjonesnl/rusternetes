use anyhow::{Context, Result};
use rusternetes_common::resources::rbac::{
    ClusterRole, ClusterRoleBinding, PolicyRule, RoleRef, Subject,
};
use rusternetes_common::resources::{EndpointSlice, Endpoints};
use rusternetes_storage::Storage;
use rusternetes_storage::StorageBackend;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const CLUSTER_ADMIN_ROLE_KEY: &str = "/registry/clusterroles/cluster-admin";
const CLUSTER_ADMIN_BINDING_KEY: &str = "/registry/clusterrolebindings/cluster-admin";

/// Seed the `cluster-admin` ClusterRole and a ClusterRoleBinding granting it to
/// the `system:masters` group, mirroring upstream bootstrap policy
/// (`plugin/pkg/auth/authorizer/rbac/bootstrappolicy`: the `cluster-admin`
/// ClusterRole bound to `SystemPrivilegedGroup`). Without this, a freshly
/// bootstrapped (empty) store denies the cluster admin — kubeadm's
/// `CN=kubernetes-admin, O=system:masters` client cert — every request, so
/// nothing (not even the admin re-seeding RBAC) can bring the cluster up
/// (#1659). The superuser effect thus comes from a real RBAC rule, so the
/// privilege-escalation check stays rule-based (it is NOT an authorizer
/// short-circuit). Idempotent: only creates what is missing.
pub async fn bootstrap_default_rbac(storage: Arc<StorageBackend>) -> Result<()> {
    use rusternetes_common::types::{ObjectMeta, TypeMeta};

    if storage
        .get::<ClusterRole>(CLUSTER_ADMIN_ROLE_KEY)
        .await
        .is_err()
    {
        let mut metadata = ObjectMeta::new("cluster-admin");
        metadata.ensure_uid();
        metadata.ensure_creation_timestamp();
        let role = ClusterRole {
            type_meta: TypeMeta {
                kind: "ClusterRole".to_string(),
                api_version: "rbac.authorization.k8s.io/v1".to_string(),
            },
            metadata,
            rules: vec![
                PolicyRule {
                    verbs: vec!["*".to_string()],
                    api_groups: Some(vec!["*".to_string()]),
                    resources: Some(vec!["*".to_string()]),
                    resource_names: None,
                    non_resource_urls: None,
                },
                PolicyRule {
                    verbs: vec!["*".to_string()],
                    api_groups: None,
                    resources: None,
                    resource_names: None,
                    non_resource_urls: Some(vec!["*".to_string()]),
                },
            ],
            aggregation_rule: None,
        };
        storage
            .create(CLUSTER_ADMIN_ROLE_KEY, &role)
            .await
            .context("Failed to create cluster-admin ClusterRole")?;
        info!("Bootstrapped cluster-admin ClusterRole");
    }

    if storage
        .get::<ClusterRoleBinding>(CLUSTER_ADMIN_BINDING_KEY)
        .await
        .is_err()
    {
        let mut metadata = ObjectMeta::new("cluster-admin");
        metadata.ensure_uid();
        metadata.ensure_creation_timestamp();
        let binding = ClusterRoleBinding {
            type_meta: TypeMeta {
                kind: "ClusterRoleBinding".to_string(),
                api_version: "rbac.authorization.k8s.io/v1".to_string(),
            },
            metadata,
            subjects: vec![Subject {
                kind: "Group".to_string(),
                name: "system:masters".to_string(),
                namespace: None,
                api_group: Some("rbac.authorization.k8s.io".to_string()),
            }],
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".to_string(),
                kind: "ClusterRole".to_string(),
                name: "cluster-admin".to_string(),
            },
        };
        storage
            .create(CLUSTER_ADMIN_BINDING_KEY, &binding)
            .await
            .context("Failed to create cluster-admin ClusterRoleBinding")?;
        info!("Bootstrapped cluster-admin ClusterRoleBinding -> system:masters");
    }

    Ok(())
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
/// is the stock KCM, which does not run this controller, so an aggregated
/// `APIService`'s `Available` condition would stay `Unknown` forever and every
/// aggregation client (`e2e Aggregator`, `kubectl get apiservices`) would hang
/// "waiting for APIService controller probe". Running it here matches upstream
/// placement and works regardless of which controller-manager is deployed.
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

#[cfg(test)]
mod tests {
    use super::*;
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
}
