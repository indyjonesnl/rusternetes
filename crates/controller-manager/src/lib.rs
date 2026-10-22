// Library interface for controller-manager
pub mod controllers;
pub use controllers::*;

use controllers::{
    apiservice::APIServiceAvailabilityController,
    certificate_signing_request::CertificateSigningRequestController,
    clusterrole_aggregation::ClusterRoleAggregationController,
    cronjob::CronJobController,
    daemonset::DaemonSetController,
    deployment::DeploymentController,
    dynamic_provisioner::DynamicProvisionerController,
    endpoints::EndpointsController,
    endpointslice::EndpointSliceController,
    ephemeral_volume::EphemeralVolumeController,
    events::EventsController,
    garbage_collector::GarbageCollector,
    hpa::HorizontalPodAutoscalerController,
    hpa_metrics_client::HttpMetricsConfig,
    ingress::IngressController,
    job::JobController,
    loadbalancer::LoadBalancerController,
    namespace::NamespaceController,
    network_policy::NetworkPolicyController,
    node::NodeController,
    pod_disruption_budget::{PodDisruptionBudgetController, StalePodDisruptionController},
    priorityclass::PriorityClassController,
    pv_binder::PVBinderController,
    pv_protection::PvProtectionController,
    pvc_protection::PvcProtectionController,
    replicaset::ReplicaSetController,
    replicationcontroller::ReplicationControllerController,
    resource_quota::ResourceQuotaController,
    service::ServiceController,
    serviceaccount::ServiceAccountController,
    servicecidr::ServiceCIDRController,
    statefulset::StatefulSetController,
    storage_class::StorageClassController,
    ttl_controller::TTLController,
    volume_expansion::VolumeExpansionController,
    volume_snapshot::VolumeSnapshotController,
    vpa::VerticalPodAutoscalerController,
};
use rusternetes_client::http::ApiClient;
use rusternetes_storage::api_storage::ApiStorage;
use rusternetes_storage::{Storage, StorageBackend};
use std::sync::Arc;
use tracing::{error, info};

/// Run a controller loop under crash supervision.
///
/// A controller that panics inside `tokio::spawn` dies **silently**: the panic
/// is captured in a `JoinHandle` nobody joins, the task disappears, and the
/// resource it reconciles is simply never reconciled again. That is exactly how
/// #1775 presented — quota `status.used` stopped being published under
/// conformance load, and restarting the controller-manager "fixed" it. The
/// panic seen live was:
///
/// ```text
/// thread 'tokio-rt-worker' panicked at futures-util/src/stream/unfold.rs:108:21:
/// Unfold must not be polled after it returned `Poll::Ready(None)`
/// ```
///
/// Upstream never lets a controller crash take reconciliation with it: every
/// controller loop runs under `runtime.HandleCrash` and is driven by
/// `wait.UntilWithContext`, so a crash is logged and the loop restarts
/// (`k8s.io/apimachinery/pkg/util/runtime/runtime.go`,
/// `k8s.io/apimachinery/pkg/util/wait/backoff.go`). This is that behaviour: log
/// loudly, then restart with capped exponential backoff.
///
/// `make_future` is called once per attempt, so each restart gets a fresh
/// future. `max_attempts` is `None` in production (restart forever) and `Some(n)`
/// in tests so they terminate; `backoff_base` is the first delay (doubling to a
/// 30s cap) and is `Duration::ZERO` in tests.
pub async fn supervise_controller<F, Fut>(
    name: &str,
    max_attempts: Option<u32>,
    backoff_base: std::time::Duration,
    mut make_future: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    use futures::FutureExt;

    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let outcome = std::panic::AssertUnwindSafe(make_future())
            .catch_unwind()
            .await;

        match outcome {
            Ok(()) => {
                // A controller's run() is an infinite loop, so returning at all
                // means it gave up (usually after logging its own error).
                error!("{name} returned unexpectedly (attempt {attempt}); restarting");
            }
            Err(panic) => {
                let detail = panic
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "<non-string panic payload>".to_string());
                error!("{name} PANICKED (attempt {attempt}): {detail}; restarting");
            }
        }

        if let Some(limit) = max_attempts {
            if attempt >= limit {
                error!("{name} exceeded {limit} attempts; giving up");
                return;
            }
        }

        // Capped exponential backoff, doubling per attempt up to 30s.
        if !backoff_base.is_zero() {
            let factor = 1u32 << std::cmp::min(attempt.saturating_sub(1), 5);
            let delay = std::cmp::min(backoff_base * factor, std::time::Duration::from_secs(30));
            tokio::time::sleep(delay).await;
        }
    }
}

/// Configuration for the controller-manager component.
pub struct ControllerManagerConfig {
    pub sync_interval: u64,
    /// Recycler plugin config of the PV binder, from the `--pv-recycler-*`
    /// flags (`ProbeProvisionableRecyclableVolumePlugins`, `plugins.go:100-120`).
    pub pv_recycler: controllers::pv_recycler::RecyclerConfig,
    /// Metrics client config for the HPA controller. When `None`,
    /// `HttpMetricsConfig::default()` is used (api-server:6443 + /etc/kubernetes/pki).
    pub metrics_config: Option<HttpMetricsConfig>,
    /// Cluster CA cert PEM, threaded to the namespace controller so it can
    /// (re)create `kube-root-ca.crt` in every namespace. `None` falls back to
    /// the legacy cert-file paths.
    pub ca_cert_pem: Option<String>,
    /// Node-IPAM config (pod-CIDR allocation). `None` disables it, matching
    /// upstream `--allocate-node-cidrs=false`.
    pub node_ipam: Option<crate::controllers::node_ipam::NodeIpamConfig>,
    /// `--legacy-service-account-token-clean-up-period`: how long a legacy
    /// service-account token must be unused before the cleaner removes it
    /// (`pkg/controller/serviceaccount/config/v1alpha1/defaults.go:44`:
    /// 365 days; see `DEFAULT_CLEAN_UP_PERIOD`).
    pub legacy_sa_token_clean_up_period: std::time::Duration,
    /// One CSI external-attacher per `(driver name, controller socket)`: the
    /// `--csi-attacher DRIVER=SOCKET` flag of the standalone binary (#1460).
    /// Upstream runs each external-attacher as its own sidecar per driver
    /// (kubernetes-csi/external-attacher `cmd/csi-attacher/main.go`,
    /// `--csi-address`), so there is one attacher per entry. Empty = none.
    pub csi_attachers: Vec<(String, String)>,
    /// Run the AttachDetachController (#3048), the `VolumeAttachment`-creating
    /// consumer of the attacher. Upstream registers it unconditionally
    /// (`cmd/kube-controller-manager/app/core.go:351-357`,
    /// `persistentvolume-attach-detach-controller`); here it stays opt-in
    /// (`--attach-detach-controller`), matching the standalone binary, because
    /// it is only useful together with a CSI attacher.
    pub attach_detach_controller: bool,
    /// Run the bootstrap signer. Upstream registers it
    /// `isDisabledByDefault: true` (`cmd/kube-controller-manager/app/bootstrap.go:35`),
    /// so it is opt-in (`--bootstrap-signer-controller`).
    pub bootstrap_signer_controller: bool,
    /// Run the SELinux warning controller (`selinux-warning-controller`,
    /// `isDisabledByDefault: true`, requires the `SELinuxChangePolicy` gate;
    /// `cmd/kube-controller-manager/app/core.go:1107`). Opt-in
    /// (`--selinux-warning-controller`).
    pub selinux_warning_controller: bool,
    /// `--terminated-pod-gc-threshold` (`PodGCControllerConfiguration.
    /// TerminatedPodGCThreshold`, default 12500): terminated pods tolerated
    /// before the pod GC deletes the oldest; `<= 0` disables that pass.
    pub terminated_pod_gc_threshold: i32,
}

/// Run the controller-manager against a storage backend directly (all-in-one
/// binary's storage mode, and the standalone binary).
pub async fn run(
    storage: Arc<StorageBackend>,
    config: ControllerManagerConfig,
) -> anyhow::Result<()> {
    run_controllers(move |_name| storage.clone(), config).await
}

/// Run the controller-manager as an api-server client: every controller's
/// `Storage` calls are proxied to the api-server over REST via [`ApiStorage`],
/// with no direct storage handle. This is the in-process counterpart of an
/// in-cluster controller-manager — the all-in-one binary calls this with an
/// [`ApiClient`] pointed at its embedded api-server over loopback, so
/// dns/scheduler/controller-manager all share the same trust boundary (only
/// the api-server touches storage).
pub async fn run_with_api(
    client: Arc<ApiClient>,
    config: ControllerManagerConfig,
) -> anyhow::Result<()> {
    let base = ApiStorage::new(Arc::clone(&client));
    run_controllers(
        move |name| Arc::new(base.with_client(controller_client(&client, name))),
        config,
    )
    .await
}

/// The api client one controller talks through: a clone of `skeleton` with its
/// OWN rate limiter, so a busy controller cannot starve the others. This is
/// `ClientBuilder.ClientOrDie(name)` upstream
/// (`staging/src/k8s.io/controller-manager/pkg/clientbuilder/client_builder.go:40-47`),
/// where QPS/Burst (`--kube-api-qps` / `--kube-api-burst`, default 20/30:
/// `pkg/controller/apis/config/v1alpha1/defaults.go:59,62`) apply per client.
fn controller_client(skeleton: &ApiClient, name: &str) -> Arc<ApiClient> {
    Arc::new(skeleton.for_controller(name))
}

/// Spawn all controllers as tokio tasks and wait for ctrl-c. Generic over the
/// storage seam so the SAME controllers run against either a real
/// `StorageBackend` or [`ApiStorage`].
///
/// `storage_for(name)` is called exactly once per controller, so a caller can
/// hand each controller its own api client and therefore its own rate limiter
/// (#1863). Upstream does the same: `ClientBuilder.ClientOrDie(name)` per
/// controller (`cmd/kube-controller-manager/app/controllermanager.go`), cloning
/// the skeleton config (`clientbuilder/client_builder.go:40-47`).
async fn run_controllers<S, F>(
    storage_for: F,
    config: ControllerManagerConfig,
) -> anyhow::Result<()>
where
    S: Storage + Send + Sync + 'static,
    F: Fn(&'static str) -> Arc<S>,
{
    info!("Starting Rusternetes Controller Manager");
    let _handles = spawn_controllers(storage_for, config);
    info!("All controllers started successfully");

    // Keep alive until shutdown
    tokio::signal::ctrl_c().await?;
    info!("Shutting down controller manager");

    Ok(())
}

/// Spawn every controller, one task each, returning the task handles.
fn spawn_controllers<S, F>(
    storage_for: F,
    config: ControllerManagerConfig,
) -> Vec<tokio::task::JoinHandle<()>>
where
    S: Storage + Send + Sync + 'static,
    F: Fn(&'static str) -> Arc<S>,
{
    let interval = config.sync_interval;
    let hpa_metrics_cfg = config.metrics_config.unwrap_or_default();
    let mut handles = Vec::new();

    // No leader election in all-in-one mode — single instance
    let cloud_provider: Option<Arc<dyn rusternetes_common::cloud_provider::CloudProvider>> = None;

    // Spawn all controllers
    let s = storage_for("LoadBalancer");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(LoadBalancerController::new(
            s,
            cloud_provider,
            "rusternetes".to_string(),
            interval,
        ));
        if let Err(e) = c.run().await {
            error!("LoadBalancer controller error: {}", e);
        }
    }));

    let s = storage_for("Deployment");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(DeploymentController::new(s, interval));
        if let Err(e) = c.run().await {
            error!("Deployment controller error: {}", e);
        }
    }));

    let s = storage_for("ReplicationController");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(ReplicationControllerController::new(s, interval));
        if let Err(e) = c.run().await {
            error!("ReplicationController controller error: {}", e);
        }
    }));

    let s = storage_for("ReplicaSet");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(ReplicaSetController::new(s, interval));
        if let Err(e) = c.run().await {
            error!("ReplicaSet controller error: {}", e);
        }
    }));

    let s = storage_for("StatefulSet");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(StatefulSetController::new(s));
        if let Err(e) = c.run().await {
            error!("StatefulSet controller error: {}", e);
        }
    }));

    let s = storage_for("DaemonSet");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(DaemonSetController::new(s));
        if let Err(e) = c.run().await {
            error!("DaemonSet controller error: {}", e);
        }
    }));

    let s = storage_for("Job");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(JobController::new(s));
        if let Err(e) = c.run().await {
            error!("Job controller error: {}", e);
        }
    }));

    let s = storage_for("CronJob");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(CronJobController::new(s));
        if let Err(e) = c.run().await {
            error!("CronJob controller error: {}", e);
        }
    }));

    let s = storage_for("PVBinder");
    let pv_recycler = config.pv_recycler.clone();
    handles.push(tokio::spawn(async move {
        let c = Arc::new(PVBinderController::new(s).with_recycler_config(pv_recycler));
        if let Err(e) = c.run().await {
            error!("PV/PVC Binder controller error: {}", e);
        }
    }));

    let s = storage_for("PvcProtection");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(PvcProtectionController::new(s));
        if let Err(e) = c.run().await {
            error!("PVC protection controller error: {}", e);
        }
    }));

    let s = storage_for("EphemeralVolume");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(EphemeralVolumeController::new(s));
        if let Err(e) = c.run().await {
            error!("Ephemeral volume controller error: {}", e);
        }
    }));

    let s = storage_for("PvProtection");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(PvProtectionController::new(s));
        if let Err(e) = c.run().await {
            error!("PV protection controller error: {}", e);
        }
    }));

    let s = storage_for("DynamicProvisioner");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(DynamicProvisionerController::new(s));
        if let Err(e) = c.run().await {
            error!("Dynamic Provisioner controller error: {}", e);
        }
    }));

    // One CSI external-attacher per driver (#3128), mirroring the standalone
    // binary's `--csi-attacher` loop.
    for (driver, socket) in &config.csi_attachers {
        let s = storage_for("CSIAttacher");
        let attacher = Arc::new(controllers::csi_attacher::CsiAttacher::new(
            s,
            driver,
            rusternetes_csi::controller_client::CsiControllerClient::with_endpoint(socket),
        ));
        handles.push(tokio::spawn(async move {
            if let Err(e) = attacher.run().await {
                error!("CSI attacher error: {e}");
            }
        }));
    }

    if config.attach_detach_controller {
        let s = storage_for("AttachDetach");
        let c = Arc::new(controllers::attach_detach::AttachDetachController::new(s));
        handles.push(tokio::spawn(async move {
            if let Err(e) = c.run().await {
                error!("AttachDetach controller error: {e}");
            }
        }));
    }

    // `newBootstrapSignerController` (bootstrap.go:30-53), disabled by default.
    if config.bootstrap_signer_controller {
        let s = storage_for("BootstrapSigner");
        let c = Arc::new(controllers::bootstrap_signer::Signer::new(s));
        handles.push(tokio::spawn(async move {
            if let Err(e) = c.run().await {
                error!("BootstrapSigner controller error: {e}");
            }
        }));
    }

    // `newSELinuxWarningControllerDescriptor` (core.go:1107), disabled by default.
    if config.selinux_warning_controller && controllers::selinux_warning::feature_gates_enabled() {
        let s = storage_for("SELinuxWarning");
        let c = Arc::new(controllers::selinux_warning::SELinuxWarningController::new(
            s,
        ));
        handles.push(tokio::spawn(async move {
            if let Err(e) = c.run().await {
                error!("SELinux warning controller error: {e}");
            }
        }));
    }

    let s = storage_for("VolumeSnapshot");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(VolumeSnapshotController::new(s));
        if let Err(e) = c.run().await {
            error!("Volume Snapshot controller error: {}", e);
        }
    }));

    let s = storage_for("VolumeExpansion");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(VolumeExpansionController::new(s));
        if let Err(e) = c.run().await {
            error!("Volume Expansion controller error: {}", e);
        }
    }));

    let s = storage_for("StorageClass");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(StorageClassController::new(s));
        if let Err(e) = c.run().await {
            error!("StorageClass controller error: {}", e);
        }
    }));

    let s = storage_for("Endpoints");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(EndpointsController::new(s));
        if let Err(e) = c.run().await {
            error!("Endpoints controller error: {}", e);
        }
    }));

    let s = storage_for("EndpointSlice");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(EndpointSliceController::new(s));
        if let Err(e) = c.run().await {
            error!("EndpointSlice controller error: {}", e);
        }
    }));

    let s = storage_for("Events");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(EventsController::new(s, interval));
        c.run().await;
    }));

    let s = storage_for("ResourceQuota");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(ResourceQuotaController::new(s));
        if let Err(e) = c.run().await {
            error!("ResourceQuota controller error: {}", e);
        }
    }));

    let s = storage_for("GarbageCollector");
    handles.push(tokio::spawn(async move {
        let c = GarbageCollector::new(s);
        c.run().await;
    }));

    let s = storage_for("HorizontalPodAutoscaler");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(HorizontalPodAutoscalerController::with_config(
            s,
            hpa_metrics_cfg,
        ));
        if let Err(e) = c.run().await {
            error!("HPA controller error: {}", e);
        }
    }));

    let s = storage_for("VerticalPodAutoscaler");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(VerticalPodAutoscalerController::new(s));
        c.run().await;
    }));

    let s = storage_for("TTL");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(TTLController::new(s));
        c.run().await;
    }));

    let s = storage_for("PodDisruptionBudget");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(PodDisruptionBudgetController::new(s));
        if let Err(e) = c.run().await {
            error!("PodDisruptionBudget controller error: {}", e);
        }
    }));

    let s = storage_for("StalePodDisruption");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(StalePodDisruptionController::new(s));
        if let Err(e) = c.run().await {
            error!("StalePodDisruption controller error: {}", e);
        }
    }));

    let s = storage_for("NetworkPolicy");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(NetworkPolicyController::new(s));
        if let Err(e) = c.run().await {
            error!("NetworkPolicy controller error: {}", e);
        }
    }));

    let s = storage_for("Ingress");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(IngressController::new(s));
        if let Err(e) = c.run().await {
            error!("Ingress controller error: {}", e);
        }
    }));

    let s = storage_for("CertificateSigningRequest");
    let csr_ca = controllers::cert_authority::load_cluster_ca_from_env();
    handles.push(tokio::spawn(async move {
        let mut controller = CertificateSigningRequestController::new(s);
        if let Some(ca) = csr_ca {
            controller = controller.with_certificate_authority(ca);
        }
        let c = Arc::new(controller);
        if let Err(e) = c.run().await {
            error!("CertificateSigningRequest controller error: {}", e);
        }
    }));

    // `newPodGarbageCollectorController`
    // (cmd/kube-controller-manager/app/core.go:564-576).
    let s = storage_for("PodGarbageCollector");
    let threshold = config.terminated_pod_gc_threshold;
    handles.push(tokio::spawn(async move {
        let c = Arc::new(controllers::podgc::PodGcController::new(s, threshold));
        if let Err(e) = c.run().await {
            error!("PodGC controller error: {}", e);
        }
    }));

    // `newCertificateSigningRequestCleanerController`
    // (cmd/kube-controller-manager/app/certificates.go:217-230).
    let s = storage_for("CertificateSigningRequestCleaner");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(controllers::csr_cleaner::CsrCleanerController::new(s));
        if let Err(e) = c.run().await {
            error!("CSR cleaner controller error: {}", e);
        }
    }));

    let s = storage_for("Namespace");
    let ns_ca = config.ca_cert_pem.clone();
    handles.push(tokio::spawn(async move {
        let c = Arc::new(NamespaceController::new(s).with_ca_cert(ns_ca));
        if let Err(e) = c.run().await {
            error!("Namespace controller error: {}", e);
        }
    }));

    let s = storage_for("TaintEviction");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(controllers::taint_eviction::TaintEvictionController::new(s));
        if let Err(e) = c.run().await {
            error!("TaintEviction controller error: {}", e);
        }
    }));

    let s = storage_for("ClusterRoleAggregation");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(ClusterRoleAggregationController::new(s));
        if let Err(e) = c.run().await {
            error!("ClusterRoleAggregator controller error: {}", e);
        }
    }));

    let s = storage_for("ServiceAccount");
    let sa_ca = config.ca_cert_pem.clone();
    handles.push(tokio::spawn(async move {
        let c = Arc::new(ServiceAccountController::new(s).with_ca_cert(sa_ca));
        if let Err(e) = c.run().await {
            error!("ServiceAccount controller error: {}", e);
        }
    }));

    // `newLegacyServiceAccountTokenCleanerController`
    // (cmd/kube-controller-manager/app/core.go:933-963).
    let s = storage_for("LegacyServiceAccountTokenCleaner");
    let clean_up_period = config.legacy_sa_token_clean_up_period;
    handles.push(tokio::spawn(async move {
        let options =
            controllers::legacy_serviceaccount_token_cleaner::LegacySATokenCleanerOptions {
                clean_up_period,
                sync_interval:
                    controllers::legacy_serviceaccount_token_cleaner::DEFAULT_CLEANER_SYNC_INTERVAL,
            };
        match controllers::legacy_serviceaccount_token_cleaner::LegacySATokenCleaner::new(
            s, options,
        ) {
            Ok(c) => {
                if let Err(e) = Arc::new(c).run().await {
                    error!("LegacyServiceAccountTokenCleaner controller error: {}", e);
                }
            }
            Err(e) => error!("failed to init the legacy service account token cleaner: {e}"),
        }
    }));

    let s = storage_for("Service");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(ServiceController::new(s));
        if let Err(e) = c.run().await {
            error!("Service controller error: {}", e);
        }
    }));

    let s = storage_for("Node");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(NodeController::new(s));
        if let Err(e) = c.run().await {
            error!("Node controller error: {}", e);
        }
    }));

    if let Some(ipam) = config.node_ipam.clone() {
        let s = storage_for("NodeIPAM");
        handles.push(tokio::spawn(async move {
            if let Err(e) = controllers::node_ipam::run_node_ipam(s, ipam).await {
                error!("Node IPAM controller error: {}", e);
            }
        }));
    }

    let s = storage_for("PriorityClass");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(PriorityClassController::new(s));
        if let Err(e) = c.run().await {
            error!("PriorityClass controller error: {}", e);
        }
    }));

    let s = storage_for("APIServiceAvailability");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(APIServiceAvailabilityController::new(s));
        if let Err(e) = c.run().await {
            error!("APIService availability controller error: {}", e);
        }
    }));

    let s = storage_for("ServiceCIDR");
    handles.push(tokio::spawn(async move {
        let c = Arc::new(ServiceCIDRController::new(s));
        if let Err(e) = c.run().await {
            error!("ServiceCIDR controller error: {}", e);
        }
    }));

    handles
}

#[cfg(test)]
mod supervisor_tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    /// #1775: a panicking controller must be restarted, not silently lost.
    ///
    /// The live failure was a `futures` Unfold panic inside the ResourceQuota
    /// controller; `tokio::spawn` swallowed it, the task vanished, and quota
    /// `status.used` was never published again until the process restarted.
    #[tokio::test]
    async fn a_panicking_controller_is_restarted() {
        let attempts = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&attempts);

        super::supervise_controller(
            "test controller",
            Some(3),
            std::time::Duration::ZERO,
            move || {
                let seen = Arc::clone(&seen);
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    panic!("simulated controller crash");
                }
            },
        )
        .await;

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "the supervisor must restart a panicking controller up to the attempt limit"
        );
    }

    /// A controller whose run() returns has given up — an infinite reconcile
    /// loop returning at all is a failure, so it is restarted too.
    #[tokio::test]
    async fn a_returning_controller_is_restarted() {
        let attempts = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&attempts);

        super::supervise_controller(
            "test controller",
            Some(2),
            std::time::Duration::ZERO,
            move || {
                let seen = Arc::clone(&seen);
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                }
            },
        )
        .await;

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    /// The panic payload must be recoverable for the log — a silent restart
    /// would hide the defect that caused it.
    #[test]
    fn the_panic_payload_is_recovered_for_logging() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(format!("boom {}", 1));
        let as_string = payload
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned());
        assert_eq!(as_string.as_deref(), Some("boom 1"));
    }
}

#[cfg(test)]
mod per_controller_client_tests {
    use super::*;
    use rusternetes_storage::MemoryStorage;
    use std::sync::Mutex;

    /// #1863: the number of controllers started equals the number of clients
    /// (hence limiters) built, one each, and no two share a bucket. Sharing one
    /// limiter across the 35 controllers is what #1856 did and #1862 reverted.
    #[tokio::test]
    async fn every_controller_gets_its_own_client_and_limiter() {
        let skeleton = ApiClient::new("http://127.0.0.1:1", true, None)
            .unwrap()
            .with_rate_limit(20.0, 30.0);
        let built: Mutex<Vec<(&'static str, Arc<ApiClient>)>> = Mutex::new(Vec::new());
        let storage = Arc::new(MemoryStorage::new());

        let handles = spawn_controllers(
            |name| {
                built
                    .lock()
                    .unwrap()
                    .push((name, controller_client(&skeleton, name)));
                storage.clone()
            },
            ControllerManagerConfig {
                sync_interval: 3600,
                pv_recycler: Default::default(),
                metrics_config: None,
                ca_cert_pem: None,
                node_ipam: None,
                legacy_sa_token_clean_up_period:
                    controllers::legacy_serviceaccount_token_cleaner::DEFAULT_CLEAN_UP_PERIOD,
                csi_attachers: Vec::new(),
                attach_detach_controller: false,
                bootstrap_signer_controller: false,
                selinux_warning_controller: false,
                terminated_pod_gc_threshold:
                    controllers::podgc::DEFAULT_TERMINATED_POD_GC_THRESHOLD,
            },
        );
        for h in &handles {
            h.abort();
        }

        let built = built.into_inner().unwrap();
        assert!(!handles.is_empty());
        assert_eq!(
            built.len(),
            handles.len(),
            "exactly one client per controller started"
        );

        let mut names: Vec<_> = built.iter().map(|(n, _)| *n).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), built.len(), "controller names are distinct");

        for (i, (na, a)) in built.iter().enumerate() {
            assert!(
                !a.shares_limiter_with(&skeleton),
                "{na} must not share the skeleton's limiter"
            );
            for (nb, b) in &built[i + 1..] {
                assert!(
                    !a.shares_limiter_with(b),
                    "{na} and {nb} must not share a limiter"
                );
            }
        }
    }
}

#[cfg(test)]
mod csi_attach_wiring_tests {
    use super::*;
    use rusternetes_storage::MemoryStorage;

    fn count(csi_attachers: Vec<(String, String)>, attach_detach_controller: bool) -> usize {
        let storage = Arc::new(MemoryStorage::new());
        let handles = spawn_controllers(
            |_| storage.clone(),
            ControllerManagerConfig {
                sync_interval: 3600,
                pv_recycler: Default::default(),
                metrics_config: None,
                ca_cert_pem: None,
                node_ipam: None,
                legacy_sa_token_clean_up_period:
                    controllers::legacy_serviceaccount_token_cleaner::DEFAULT_CLEAN_UP_PERIOD,
                csi_attachers,
                attach_detach_controller,
                bootstrap_signer_controller: false,
                selinux_warning_controller: false,
                terminated_pod_gc_threshold:
                    controllers::podgc::DEFAULT_TERMINATED_POD_GC_THRESHOLD,
            },
        );
        for h in &handles {
            h.abort();
        }
        handles.len()
    }

    /// #3128: the all-in-one / API-mode controller set starts the CSI
    /// external-attacher (one per driver) and the AttachDetachController only
    /// when asked, exactly like the standalone binary's `--csi-attacher` /
    /// `--attach-detach-controller` (both opt-in).
    #[tokio::test]
    async fn csi_attacher_and_attach_detach_are_opt_in() {
        let base = count(Vec::new(), false);
        assert_eq!(count(Vec::new(), true), base + 1);
        let one = vec![(
            "hostpath.csi.k8s.io".to_string(),
            "/tmp/csi.sock".to_string(),
        )];
        assert_eq!(count(one.clone(), false), base + 1);
        let two = vec![
            one[0].clone(),
            ("b.csi".to_string(), "/tmp/b.sock".to_string()),
        ];
        assert_eq!(count(two, true), base + 3);
    }
}
