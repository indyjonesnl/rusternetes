// mimalloc as the global allocator (off by default; `--features mimalloc`).
// Required for the musl static builds — musl's default allocator is ~10x
// slower under multi-threaded lock contention — and lowers idle RSS (#1041).
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod controllers;

use anyhow::Result;
use clap::Parser;
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
    node_ipam::{self, NodeCidrMaskSizes, NodeIpamConfig},
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
    storageversiongc::StorageVersionGCController,
    ttl_controller::TTLController,
    vac_protection::VacProtectionController,
    volume_data_source_validator::VolumeDataSourceValidator,
    volume_expansion::VolumeExpansionController,
    volume_snapshot::VolumeSnapshotController,
    vpa::VerticalPodAutoscalerController,
};
use rusternetes_common::cloud_provider::CloudProvider;
use rusternetes_storage::{StorageBackend, StorageConfig};
use std::sync::Arc;
use tracing::{error, info, warn};

/// The recycler plugin config the flags describe, built the way
/// `newPersistentVolumeBinderController` (`core.go:322-325`) probes the volume
/// plugins: a template file that fails to load/validate aborts startup.
/// A macro because the bin's own `mod controllers` and the library crate each
/// carry a distinct copy of `pv_recycler`.
macro_rules! recycler_config {
    ($args:expr, $module:path) => {{
        use $module::{PersistentVolumeRecyclerConfiguration, RecyclerConfig};
        let args: &Args = $args;
        RecyclerConfig::from_configuration(&PersistentVolumeRecyclerConfiguration {
            minimum_timeout_nfs: args.pv_recycler_minimum_timeout_nfs,
            pod_template_file_path_nfs: args.pv_recycler_pod_template_filepath_nfs.clone(),
            increment_timeout_nfs: args.pv_recycler_increment_timeout_nfs,
            minimum_timeout_host_path: args.pv_recycler_minimum_timeout_hostpath,
            pod_template_file_path_host_path: args
                .pv_recycler_pod_template_filepath_hostpath
                .clone(),
            increment_timeout_host_path: args.pv_recycler_timeout_increment_hostpath,
        })
        .map_err(|e| {
            anyhow::anyhow!(
                "failed to probe volume plugins when starting persistentvolume controller: {e}"
            )
        })
    }};
}

#[derive(Parser, Debug)]
#[command(name = "rusternetes-controller-manager")]
#[command(about = "Rusternetes Controller Manager - Runs controller loops")]
struct Args {
    /// Comma-separated `[component:]Gate=bool` pairs, e.g.
    /// `StorageCapacityScoring=true` or `kube:StorageCapacityScoring=true`;
    /// may be repeated (upstream `--feature-gates`,
    /// compatibility/registry.go:249-253). The component prefix defaults to
    /// `kube`; do not mix prefixed and unprefixed pairs.
    #[arg(long)]
    feature_gates: Vec<String>,

    /// The version the component emulates (APIs, features, ...), as
    /// `[component=]major.minor`, e.g. `--emulated-version=1.34`. Options are:
    /// kube=1.32..1.35(default:1.35) (upstream `--emulated-version`,
    /// compatibility/registry.go:231-236).
    #[arg(long, value_delimiter = ',')]
    emulated_version: Vec<String>,

    /// The min version of control plane components the server should be
    /// compatible with; must be <= `--emulated-version`. Options are:
    /// kube=1.32..1.35(default:1.34) (upstream `--min-compatibility-version`,
    /// compatibility/registry.go:243-247).
    #[arg(long, value_delimiter = ',')]
    min_compatibility_version: Vec<String>,

    /// Etcd endpoints (comma-separated)
    #[arg(long, default_value = "http://localhost:2379")]
    etcd_servers: String,

    /// Run a CSI external-provisioner for a driver: `<driver-name>=<controller
    /// socket path>` (the sidecar's `--csi-address`). Repeatable.
    #[arg(long = "csi-provisioner", value_name = "DRIVER=SOCKET")]
    csi_provisioners: Vec<String>,

    /// Run a CSI external-attacher for a driver: `<driver-name>=<controller
    /// socket path>` (the sidecar's `--csi-address` + `--attacher`).
    /// Repeatable.
    #[arg(long = "csi-attacher", value_name = "DRIVER=SOCKET")]
    csi_attachers: Vec<String>,

    /// Run the CSI half of the AttachDetachController (#3048): create and
    /// delete the `VolumeAttachment`s of scheduled pods' CSI volumes. Off by
    /// default; enable together with `--csi-attacher`.
    #[arg(long = "attach-detach-controller", default_value_t = false)]
    attach_detach_controller: bool,

    /// Run the bootstrap signer (`bootstrapsigner`). Upstream registers it
    /// `isDisabledByDefault: true` (bootstrap.go:35); opt-in here too.
    #[arg(long = "bootstrap-signer-controller", default_value_t = false)]
    bootstrap_signer_controller: bool,
    /// Run the bootstrap token cleaner (`tokencleaner`). Upstream registers
    /// it `isDisabledByDefault: true` (bootstrap.go:59); opt-in here too.
    #[arg(long = "token-cleaner-controller", default_value_t = false)]
    token_cleaner_controller: bool,

    /// Run the SELinux warning controller (`selinux-warning-controller`):
    /// events and a metric when pods sharing a volume use conflicting SELinux
    /// labels. Disabled by default upstream; requires the
    /// `SELinuxChangePolicy` feature gate.
    #[arg(long = "selinux-warning-controller", default_value_t = false)]
    selinux_warning_controller: bool,

    /// `--terminated-pod-gc-threshold`: number of terminated pods that can
    /// exist before the pod garbage collector starts deleting the oldest.
    /// `<= 0` disables that pass (default 12500, podgc config defaults.go:34).
    #[arg(long, default_value_t = controllers::podgc::DEFAULT_TERMINATED_POD_GC_THRESHOLD, allow_hyphen_values = true)]
    terminated_pod_gc_threshold: i32,

    /// `AttachDetachControllerConfiguration.ReconcilerMaxWaitForUnmountDuration`
    /// (default 6m): how long a volume still mounted on an unhealthy node is
    /// kept attached before it is force detached.
    #[arg(long, default_value = "360")]
    attach_detach_max_wait_for_unmount_seconds: u64,

    /// `AttachDetachControllerConfiguration.DisableForceDetachOnTimeout`.
    #[arg(long, default_value_t = false)]
    attach_detach_disable_force_detach_on_timeout: bool,

    /// `--attach-detach-reconcile-sync-period`
    /// (`ReconcilerSyncLoopPeriod`, default 60s; must be at least 1s).
    #[arg(long, default_value_t = 60)]
    attach_detach_reconcile_sync_period_seconds: u64,

    /// `--disable-attach-detach-reconcile-sync`.
    #[arg(long, default_value_t = false)]
    disable_attach_detach_reconcile_sync: bool,

    /// `--default-fstype` of external-provisioner, for every `--csi-provisioner`.
    #[arg(long, default_value = "")]
    csi_default_fstype: String,

    /// `--reconcile-sync` of external-attacher (`main.go:76`), in seconds.
    #[arg(long, default_value_t = 60)]
    csi_attacher_reconcile_sync_seconds: u64,

    /// `--max-entries` of external-attacher (`main.go:72`); 0 means no limit.
    #[arg(long, default_value_t = 0)]
    csi_attacher_max_entries: i32,

    /// `--extra-create-metadata` of external-provisioner.
    #[arg(long)]
    csi_extra_create_metadata: bool,

    /// `--controller-publish-readonly` of external-provisioner.
    #[arg(long)]
    csi_controller_publish_readonly: bool,

    /// `--prevent-volume-mode-conversion` of external-provisioner
    /// (csi-provisioner.go:112, default true).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    csi_prevent_volume_mode_conversion: bool,

    /// `--retry-interval-max` of external-provisioner
    /// (csi-provisioner.go:84, default 5m): the retry backoff cap and how long
    /// an infeasible claim is delayed.
    #[arg(long, default_value = "5m", value_parser = parse_clean_up_period)]
    csi_retry_interval_max: std::time::Duration,

    /// `--strict-topology` of external-provisioner (csi-provisioner.go:91):
    /// late binding passes only the selected node's topology to CreateVolume.
    #[arg(long)]
    csi_strict_topology: bool,

    /// Deprecated alias for `--feature-gates=CrossNamespaceVolumeDataSource=true`
    /// (alpha, default off): honour `dataSourceRef.namespace` when a
    /// ReferenceGrant allows it.
    #[arg(long)]
    csi_cross_namespace_volume_data_source: bool,

    /// `--immediate-topology` of external-provisioner (csi-provisioner.go:92,
    /// default true): immediate binding passes the aggregated cluster
    /// topology, or none when disabled.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    csi_immediate_topology: bool,

    /// `--node-deployment` of external-provisioner (csi-provisioner.go:106):
    /// deploy together with a CSI driver on a node to manage node-local
    /// volumes. Needs the `NODE_NAME` environment variable and a driver
    /// socket that serves the CSI Node service.
    #[arg(long)]
    csi_node_deployment: bool,

    /// `--node-deployment-immediate-binding` (csi-provisioner.go:107,
    /// default true).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    csi_node_deployment_immediate_binding: bool,

    /// `--node-deployment-base-delay` (csi-provisioner.go:108, default 20s).
    #[arg(long, default_value = "20s", value_parser = parse_clean_up_period)]
    csi_node_deployment_base_delay: std::time::Duration,

    /// `--node-deployment-max-delay` (csi-provisioner.go:109, default 60s).
    #[arg(long, default_value = "60s", value_parser = parse_clean_up_period)]
    csi_node_deployment_max_delay: std::time::Duration,

    /// `--enable-capacity` of external-provisioner (csi-provisioner.go:101):
    /// publish CSIStorageCapacity objects from the driver's GetCapacity for
    /// every `--csi-provisioner`. Needs the `NAMESPACE` environment variable
    /// (and `POD_NAME` unless the ownerref level is negative).
    #[arg(long)]
    csi_enable_capacity: bool,

    /// `--capacity-for-immediate-binding` (csi-provisioner.go:102).
    #[arg(long)]
    csi_capacity_for_immediate_binding: bool,

    /// `--capacity-poll-interval` (csi-provisioner.go:103, default 1m).
    #[arg(long, default_value = "1m", value_parser = parse_clean_up_period)]
    csi_capacity_poll_interval: std::time::Duration,

    /// `--capacity-ownerref-level` (csi-provisioner.go:104, default 1): -1 no
    /// owner, 0 the pod itself, 1 its StatefulSet/DaemonSet, 2 a Deployment.
    #[arg(long, default_value_t = 1, allow_hyphen_values = true)]
    csi_capacity_ownerref_level: i32,

    /// `--capacity-threads` (csi-provisioner.go:88, default 1): workers on the
    /// CSIStorageCapacity queue (`Controller.Run`, capacity.go:238-253).
    #[arg(long, default_value_t = 1)]
    csi_capacity_threads: u32,

    /// `--kube-api-capacity-qps` (csi-provisioner.go:98, default 1): QPS of the
    /// dedicated client that writes CSIStorageCapacity objects (:456). Zero
    /// means client-go's default of 5, a negative value no limit
    /// (`rest/config.go:370-381`).
    #[arg(long, default_value_t = 1.0, allow_hyphen_values = true)]
    csi_kube_api_capacity_qps: f64,

    /// `--kube-api-capacity-burst` (csi-provisioner.go:99, default 5): burst of
    /// that client (:457). Zero means client-go's default of 10.
    #[arg(long, default_value_t = 5, allow_hyphen_values = true)]
    csi_kube_api_capacity_burst: i32,

    /// Storage backend: "etcd" or "sqlite"
    #[arg(long, default_value = "etcd")]
    storage_backend: String,

    /// SQLite database path (only used when --storage-backend=sqlite)
    #[arg(long, default_value = "./data/rusternetes.db")]
    data_dir: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Controller sync interval in seconds
    #[arg(long, default_value = "5")]
    sync_interval: u64,

    /// The file path to a pod definition used as a template for NFS persistent
    /// volume recycling (`--pv-recycler-pod-template-filepath-nfs`,
    /// `options/persistentvolumebindercontroller.go:37`).
    #[arg(long, default_value = "")]
    pv_recycler_pod_template_filepath_nfs: String,

    /// The minimum ActiveDeadlineSeconds to use for an NFS Recycler pod.
    #[arg(long, default_value_t = 300)]
    pv_recycler_minimum_timeout_nfs: i32,

    /// the increment of time added per Gi to ActiveDeadlineSeconds for an NFS
    /// scrubber pod.
    #[arg(long, default_value_t = 30)]
    pv_recycler_increment_timeout_nfs: i32,

    /// The file path to a pod definition used as a template for HostPath
    /// persistent volume recycling. This is for development and testing only
    /// and will not work in a multi-node cluster.
    #[arg(long, default_value = "")]
    pv_recycler_pod_template_filepath_hostpath: String,

    /// The minimum ActiveDeadlineSeconds to use for a HostPath Recycler pod.
    /// This is for development and testing only and will not work in a
    /// multi-node cluster.
    #[arg(long, default_value_t = 60)]
    pv_recycler_minimum_timeout_hostpath: i32,

    /// the increment of time added per Gi to ActiveDeadlineSeconds for a
    /// HostPath scrubber pod. This is for development and testing only and
    /// will not work in a multi-node cluster. (Upstream registers this one as
    /// `--pv-recycler-timeout-increment-hostpath`, not `-increment-timeout-`:
    /// `persistentvolumebindercontroller.go:42`.)
    #[arg(long, default_value_t = 30)]
    pv_recycler_timeout_increment_hostpath: i32,

    /// Cloud provider (aws, gcp, azure, or none)
    #[arg(long)]
    cloud_provider: Option<String>,

    /// Cluster name for cloud provider resources
    #[arg(long, default_value = "rusternetes")]
    cluster_name: String,

    /// Cloud provider region
    #[arg(long)]
    cloud_region: Option<String>,

    /// Enable leader election (for HA)
    #[arg(long)]
    enable_leader_election: bool,

    /// Leader election identity (unique for each instance)
    #[arg(long)]
    leader_election_identity: Option<String>,

    /// Leader election lock key (etcd key of the retired etcd-lease elector;
    /// accepted and ignored, the lock is now a Lease -- see
    /// `--leader-election-resource-namespace` / `--leader-election-resource-name`).
    #[arg(long, hide = true)]
    leader_election_lock_key: Option<String>,

    /// Namespace of the leader-election Lease
    /// (`--leader-elect-resource-namespace`, kube-system by default).
    #[arg(
        long,
        alias = "leader-elect-resource-namespace",
        default_value = "kube-system"
    )]
    leader_election_resource_namespace: String,

    /// Name of the leader-election Lease (`--leader-elect-resource-name`).
    #[arg(
        long,
        alias = "leader-elect-resource-name",
        default_value = "kube-controller-manager"
    )]
    leader_election_resource_name: String,

    /// Leader election lease duration in seconds
    /// (`LeaseDuration`, 15s: component-base v1alpha1 defaults.go:37-39)
    #[arg(long, default_value = "15")]
    leader_election_lease_duration: u64,

    /// Seconds the acting leader retries refreshing leadership before giving up
    /// (`RenewDeadline`, 10s: defaults.go:40-42)
    #[arg(long, default_value = "10")]
    leader_election_renew_deadline: u64,

    /// Seconds between tries of leader-election actions
    /// (`RetryPeriod`, 2s: defaults.go:43-45)
    #[arg(long, default_value = "2")]
    leader_election_retry_period: u64,

    /// `SecureServing` (`--bind-address`, `--secure-port` default 10257,
    /// `--tls-cert-file`, ...): HTTPS for `/metrics`, `/statusz`, `/flagz`
    /// (cmd/kube-controller-manager/app/options/options.go:224-238).
    #[command(flatten)]
    secure_serving: rusternetes_middleware::secure_serving::options::SecureServingFlags,

    /// `Authentication` (`--authentication-kubeconfig`, `--client-ca-file`, ...).
    #[command(flatten)]
    authentication: rusternetes_middleware::secure_serving::options::AuthenticationFlags,

    /// `Authorization` (`--authorization-kubeconfig`, ...).
    #[command(flatten)]
    authorization: rusternetes_middleware::secure_serving::options::AuthorizationFlags,

    /// API server base URL for the HPA metrics client
    #[arg(long, default_value = "https://api-server:6443")]
    api_server_url: String,

    /// Directory holding the controller-manager client cert/key + CA for the metrics client
    #[arg(long, default_value = "/etc/kubernetes/pki")]
    pki_dir: String,

    /// Skip api-server cert verification for the HPA metrics client. Defaults to
    /// true because the cluster ships a self-signed api-server cert.
    #[arg(long, default_value_t = true)]
    metrics_insecure_skip_tls_verify: bool,

    /// Path to a kubeconfig for API mode (CA + server URL). When set, the
    /// controller-manager runs as an api-server client (every controller's
    /// `Storage` calls proxy to the api-server via `ApiStorage`) with no direct
    /// storage handle — the in-cluster static-pod path. When unset, the storage
    /// backend is used (all-in-one / legacy compose path).
    #[arg(long)]
    kubeconfig: Option<String>,

    /// Sustained requests/second EACH controller's api client may send
    /// (upstream `--kube-api-qps`; upstream's default is 20,
    /// `pkg/controller/apis/config/v1alpha1/defaults.go:59`). Applied per
    /// controller, not shared: every controller gets its own limiter (#1863).
    /// `<= 0` disables client-side throttling, and that is the DEFAULT here:
    /// opt-in until the kine sig-api-machinery gate passes (#1863/#2308), then
    /// the default flips to `CONTROLLER_MANAGER_QPS`. API mode only.
    #[arg(long, default_value_t = 0.0)]
    kube_api_qps: f64,

    /// Burst each controller's api client may send at once (upstream
    /// `--kube-api-burst`, default 30: `defaults.go:62`). API mode only.
    #[arg(long, default_value_t = rusternetes_client::ratelimit::CONTROLLER_MANAGER_BURST)]
    kube_api_burst: f64,

    /// Skip TLS verification for the api-server data-plane connection in API
    /// mode (the kubeconfig CA normally validates the self-signed cert).
    #[arg(long)]
    insecure_skip_tls_verify: bool,

    /// Allocate `node.spec.podCIDR` from `--cluster-cidr` (node IPAM). Off by
    /// default, matching upstream kube-controller-manager. Requires
    /// `--cluster-cidr` when enabled.
    /// A pflag bool: bare `--allocate-node-cidrs` or `=true|false` (kubeadm
    /// passes `--allocate-node-cidrs=true`,
    /// `cmd/kubeadm/app/phases/controlplane/manifests.go:352`).
    #[arg(
        long,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true",
        default_value = "false",
        action = clap::ArgAction::Set
    )]
    allocate_node_cidrs: bool,

    /// NoExecute taints per second applied to nodes of a healthy zone
    /// (upstream `--node-eviction-rate`, default 0.1).
    #[arg(long, default_value = "0.1")]
    node_eviction_rate: f32,

    /// Eviction rate used when a zone is unhealthy and the cluster is large
    /// (upstream `--secondary-node-eviction-rate`, default 0.01).
    #[arg(long, default_value = "0.01")]
    secondary_node_eviction_rate: f32,

    /// Number of nodes above which a partially disrupted zone is "large" and
    /// uses the secondary rate; at or below it evictions stop (upstream
    /// `--large-cluster-size-threshold`, default 50).
    #[arg(long, default_value = "50")]
    large_cluster_size_threshold: i32,

    /// Fraction of not-ready nodes at which a zone is partially disrupted
    /// (upstream `--unhealthy-zone-threshold`, default 0.55).
    #[arg(long, default_value = "0.55")]
    unhealthy_zone_threshold: f32,

    /// Cluster pod-network CIDR(s) carved into per-node subnets (e.g.
    /// `10.244.0.0/16`, or `10.244.0.0/16,fd00::/48` for dual-stack: at most
    /// two, one per IP family). Only used when `--allocate-node-cidrs` is set.
    #[arg(long)]
    cluster_cidr: Option<String>,

    /// Per-node pod-CIDR mask size for a single-stack cluster. Default 24 for
    /// IPv4 and 64 for IPv6; not allowed with a dual-stack `--cluster-cidr`.
    #[arg(long, default_value = "0")]
    node_cidr_mask_size: u8,

    /// IPv4 per-node pod-CIDR mask size in a dual-stack cluster. Default 24.
    #[arg(long, default_value = "0")]
    node_cidr_mask_size_ipv4: u8,

    /// IPv6 per-node pod-CIDR mask size in a dual-stack cluster. Default 64.
    #[arg(long, default_value = "0")]
    node_cidr_mask_size_ipv6: u8,

    /// Service CIDR(s) (e.g. `10.96.0.0/12`, at most two, one per family).
    /// When it overlaps `--cluster-cidr`,
    /// the overlapping subnets are never allocated to nodes (upstream
    /// `--service-cluster-ip-range`, `filterOutServiceRange`). Also the range
    /// the Service controller allocates ClusterIPs from; the node-IPAM filter
    /// only applies when `--allocate-node-cidrs` is set.
    #[arg(long)]
    service_cluster_ip_range: Option<String>,

    /// The period of time since the last usage of a legacy service account
    /// token before it can be deleted (upstream
    /// `--legacy-service-account-token-clean-up-period`, default 365d:
    /// `cmd/kube-controller-manager/app/options/legacyserviceaccounttokencleaner.go:36`).
    /// Go duration syntax (`8760h`).
    #[arg(
        long,
        default_value = "8760h",
        value_parser = parse_clean_up_period
    )]
    legacy_service_account_token_clean_up_period: std::time::Duration,

    /// Upstream `--leader-elect` (`leaderelectionconfig.go:26-30`), a pflag
    /// bool: bare or `=true` enables; overrides `--enable-leader-election`.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    leader_elect: Option<bool>,

    /// Upstream `--leader-elect-lease-duration` (Go duration, e.g. `15s`).
    #[arg(long)]
    leader_elect_lease_duration: Option<String>,

    /// Upstream `--leader-elect-renew-deadline` (Go duration, e.g. `10s`).
    #[arg(long)]
    leader_elect_renew_deadline: Option<String>,

    /// Upstream `--leader-elect-retry-period` (Go duration, e.g. `2s`).
    #[arg(long)]
    leader_elect_retry_period: Option<String>,

    /// Upstream `--leader-elect-resource-lock`; only `leases` is supported
    /// (`leaderelectionconfig.go:48-50`).
    #[arg(long)]
    leader_elect_resource_lock: Option<String>,

    /// Upstream `--profiling` (`controller-manager/options/debugging.go:46`).
    /// Accepted for argv compatibility; no pprof handlers are served.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    profiling: Option<bool>,

    /// Upstream `--contention-profiling` (`debugging.go:48`). Accepted, ignored.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    contention_profiling: Option<bool>,

    /// Upstream `--controllers` (`controller-manager/options/generic.go:65`).
    /// Accepted for argv compatibility; the controller set is not selectable
    /// yet, so it is ignored with a WARN.
    #[arg(long, value_delimiter = ',')]
    controllers: Vec<String>,

    /// Upstream `--use-service-account-credentials`
    /// (`cloud-provider/options/kubecloudshared.go:57`). Accepted, ignored.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    use_service_account_credentials: Option<bool>,

    /// Upstream `--configure-cloud-routes` (`kubecloudshared.go:67`, default
    /// true). Accepted, ignored.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    configure_cloud_routes: Option<bool>,

    /// Upstream `--root-ca-file` (`serviceaccountcontroller.go:38`). Accepted, ignored.
    #[arg(long)]
    root_ca_file: Option<String>,

    /// Upstream `--service-account-private-key-file`
    /// (`serviceaccountcontroller.go:36`). Accepted, ignored.
    #[arg(long)]
    service_account_private_key_file: Option<String>,

    /// Upstream `--cluster-signing-cert-file` (`csrsigningcontroller.go:38`).
    /// Accepted, ignored.
    #[arg(long)]
    cluster_signing_cert_file: Option<String>,

    /// Upstream `--cluster-signing-key-file` (`csrsigningcontroller.go:39`).
    /// Accepted, ignored.
    #[arg(long)]
    cluster_signing_key_file: Option<String>,
}

/// Effective leader-election settings after resolving the upstream flag
/// names (`--leader-elect*`) over the native `--leader-election-*` ones.
#[derive(Debug, PartialEq)]
struct LeaderElectionSettings {
    enabled: bool,
    lease_duration: std::time::Duration,
    renew_deadline: std::time::Duration,
    retry_period: std::time::Duration,
}

impl Args {
    /// Upstream flag names win over the native ones when given (same shape as
    /// the scheduler, #3907).
    fn leader_election_settings(&self) -> Result<LeaderElectionSettings, String> {
        let dur =
            |flag: &str, v: &Option<String>, native: u64| -> Result<std::time::Duration, String> {
                match v {
                    None => Ok(std::time::Duration::from_secs(native)),
                    Some(s) => {
                        let ns = rusternetes_common::go_duration::parse_go_duration(s)
                            .map_err(|e| format!("invalid value {s:?} for --{flag}: {e}"))?;
                        Ok(std::time::Duration::from_nanos(ns.max(0) as u64))
                    }
                }
            };
        if let Some(lock) = &self.leader_elect_resource_lock {
            if lock != "leases" {
                return Err(format!(
                    "invalid --leader-elect-resource-lock {lock:?}: only \"leases\" is supported"
                ));
            }
        }
        Ok(LeaderElectionSettings {
            enabled: self.leader_elect.unwrap_or(self.enable_leader_election),
            lease_duration: dur(
                "leader-elect-lease-duration",
                &self.leader_elect_lease_duration,
                self.leader_election_lease_duration,
            )?,
            renew_deadline: dur(
                "leader-elect-renew-deadline",
                &self.leader_elect_renew_deadline,
                self.leader_election_renew_deadline,
            )?,
            retry_period: dur(
                "leader-elect-retry-period",
                &self.leader_elect_retry_period,
                self.leader_election_retry_period,
            )?,
        })
    }

    /// Names of accepted-but-unimplemented upstream flags that were given.
    fn ignored_upstream_flags(&self) -> Vec<&'static str> {
        let given = [
            (self.profiling == Some(true), "--profiling"),
            (
                self.contention_profiling == Some(true),
                "--contention-profiling",
            ),
            (!self.controllers.is_empty(), "--controllers"),
            (
                self.use_service_account_credentials.is_some(),
                "--use-service-account-credentials",
            ),
            (
                self.configure_cloud_routes.is_some(),
                "--configure-cloud-routes",
            ),
            (self.root_ca_file.is_some(), "--root-ca-file"),
            (
                self.service_account_private_key_file.is_some(),
                "--service-account-private-key-file",
            ),
            (
                self.cluster_signing_cert_file.is_some(),
                "--cluster-signing-cert-file",
            ),
            (
                self.cluster_signing_key_file.is_some(),
                "--cluster-signing-key-file",
            ),
        ];
        given
            .into_iter()
            .filter(|(g, _)| *g)
            .map(|(_, n)| n)
            .collect()
    }
}

/// Parse a Go duration (`time.ParseDuration`) for a clap flag.
fn parse_clean_up_period(s: &str) -> Result<std::time::Duration, String> {
    let nanos = rusternetes_common::go_duration::parse_go_duration(s)?;
    u64::try_from(nanos)
        .map(std::time::Duration::from_nanos)
        .map_err(|_| format!("negative duration {s:?}"))
}

/// The `NodeDeployment` literal of csi-provisioner.go:321-328 (everything but
/// `NodeInfo`, which `GetNodeInfo` fills in from the driver), or `None`
/// without `--csi-node-deployment`. `node_name` is the `NODE_NAME` environment
/// variable, fatal when empty (csi-provisioner.go:146-149).
fn node_deployment_settings(
    args: &Args,
    node_name: Option<String>,
) -> Result<Option<controllers::csi_provisioner::NodeDeployment>> {
    if !args.csi_node_deployment {
        return Ok(None);
    }
    let node_name = node_name.filter(|n| !n.is_empty()).ok_or_else(|| {
        anyhow::anyhow!(
            "The NODE_NAME environment variable must be set when using --csi-node-deployment."
        )
    })?;
    Ok(Some(controllers::csi_provisioner::NodeDeployment {
        node_name,
        immediate_binding: args.csi_node_deployment_immediate_binding,
        base_delay: args.csi_node_deployment_base_delay,
        max_delay: args.csi_node_deployment_max_delay,
        accessible_topology: None,
    }))
}

/// Resolve node-IPAM flags into `(cluster_cidr, mask_sizes, service_cidr)`, or `None` when
/// `--allocate-node-cidrs` is off. Errors if the flag is set without
/// `--cluster-cidr` (mirrors upstream `node_ipam_controller.go`).
fn node_ipam_params(args: &Args) -> Result<Option<(String, NodeCidrMaskSizes, Option<String>)>> {
    if !args.allocate_node_cidrs {
        return Ok(None);
    }
    let cidr = args.cluster_cidr.clone().ok_or_else(|| {
        anyhow::anyhow!("--cluster-cidr is required when --allocate-node-cidrs is set")
    })?;
    Ok(Some((
        cidr,
        NodeCidrMaskSizes {
            general: args.node_cidr_mask_size,
            ipv4: args.node_cidr_mask_size_ipv4,
            ipv6: args.node_cidr_mask_size_ipv6,
        },
        args.service_cluster_ip_range.clone(),
    )))
}

/// The `SecureServing` / `Authentication` / `Authorization` options of
/// kube-controller-manager with the command line applied
/// (cmd/kube-controller-manager/app/options/options.go:224-238).
fn component_secure_serving(
    args: &Args,
) -> rusternetes_middleware::secure_serving::ComponentSecureServing {
    let mut c =
        rusternetes_middleware::secure_serving::ComponentSecureServing::for_controller_manager();
    c.apply_flags(
        &args.secure_serving,
        &args.authentication,
        &args.authorization,
    );
    c
}

/// `utilerrors.NewAggregate(errs).Error()` of the options `Validate`.
fn aggregate_errors(errs: &[String]) -> String {
    if errs.len() == 1 {
        errs[0].clone()
    } else {
        format!("[{}]", errs.join(", "))
    }
}

/// Serve `GET /metrics`, `/statusz` and `/flagz` over the secure port behind
/// the authentication and authorization filters, as
/// `cmd/kube-controller-manager/app/controllermanager.go:195-240` does:
/// `BuildHandlerChain(unsecuredMux, &c.Authorization, &c.Authentication)`.
/// Like upstream there is no insecure port, and a failure to set up secure
/// serving is fatal.
async fn serve_secure(
    args: &Args,
    gather: fn() -> String,
    effective_version: rusternetes_common::compatibility::EffectiveVersion,
    flags: rusternetes_common::flagz::NamedFlagSetsReader,
) -> Result<()> {
    let component = component_secure_serving(args);
    let errs = component.validate();
    if !errs.is_empty() {
        anyhow::bail!("{}", aggregate_errors(&errs));
    }
    let app = axum::Router::new().route(
        "/metrics",
        axum::routing::get(move || async move { gather() }),
    );
    // `flagz.Install` behind the `ComponentFlagz` gate
    // (cmd/kube-controller-manager/app/controllermanager.go:219-223).
    let app = rusternetes_common::flagz::install_if_enabled(
        app,
        "kube-controller-manager",
        Some(Box::new(flags)),
    );
    // `statusz.Install` behind the `ComponentStatusz` gate
    // (cmd/kube-controller-manager/app/controllermanager.go:225-234), with
    // `ComponentGlobalsRegistry.EffectiveVersionFor(DefaultKubeComponent)`
    // and the listed paths of the mux (here only `/metrics`).
    let app =
        rusternetes_common::statusz::install_if_enabled(app, "kube-controller-manager", || {
            rusternetes_common::statusz::Registry::new(Some(effective_version))
                .with_listed_paths(vec!["/metrics".to_string()])
        });
    component
        .serve(app)
        .await
        .map_err(|e| anyhow::anyhow!("failed to start secure serving: {e}"))?;
    Ok(())
}

/// Run the controller-manager as an api-server client (in-cluster static pod).
/// Resolves the api-server CA from `--kubeconfig` and the URL from
/// `--api-server-url`, builds an [`ApiClient`], and delegates to the library's
/// `run_with_api`, which drives every controller through `ApiStorage`.
async fn run_api_mode(
    args: Args,
    effective_version: rusternetes_common::compatibility::EffectiveVersion,
    flags: rusternetes_common::flagz::NamedFlagSetsReader,
) -> Result<()> {
    use rusternetes_client::http::ApiClient;
    use rusternetes_client::kubeconfig::KubeConfig;

    // API mode runs the lib's controllers, so serve the lib's registry (the
    // bin compiles its own copy of `controllers`, with a separate registry).
    serve_secure(
        &args,
        rusternetes_controller_manager::controllers::gather_metrics,
        effective_version,
        flags,
    )
    .await?;

    let (ca_pem, kube_insecure, token, client_cert, client_key) =
        if let Some(path) = args.kubeconfig.as_deref() {
            let cfg = KubeConfig::load_from_file(&std::path::PathBuf::from(path))?;
            let ca = cfg.get_ca_cert_pem().ok().flatten();
            let insecure = cfg.should_skip_tls_verify().unwrap_or(false);
            // Bearer token for the HPA metrics client (the data plane runs
            // against an api-server with --skip-auth today, so the token is
            // usually absent; pass it through so metrics keep working once
            // authn (#1129) lands).
            let token = cfg.get_token().ok().flatten();
            // Client cert/key for mTLS auth (#1578): required when the
            // api-server authenticates clients by certificate.
            let cert = cfg.get_client_cert_pem().ok().flatten();
            let key = cfg.get_client_key_pem().ok().flatten();
            (ca, insecure, token, cert, key)
        } else {
            (None, false, None, None, None)
        };
    let insecure = args.insecure_skip_tls_verify || kube_insecure;
    info!(
        "Controller-manager API mode: api-server={}, ca={}, insecure={}",
        args.api_server_url,
        ca_pem.is_some(),
        insecure
    );

    // The CA validates the server's TLS cert; client cert/key (when the
    // kubeconfig provides them) authenticate this component via mTLS (#1578).
    // This is the SKELETON: `run_with_api` clones it once per controller and
    // each clone gets its own limiter at this QPS/burst (upstream's
    // `SimpleControllerClientBuilder`, clientbuilder/client_builder.go:40-47).
    let client = Arc::new(
        ApiClient::with_tls(
            &args.api_server_url,
            insecure,
            ca_pem.clone(),
            client_cert.map(|p| p.into_bytes()),
            client_key.map(|p| p.into_bytes()),
            None,
        )?
        .with_rate_limit(args.kube_api_qps, args.kube_api_burst),
    );
    let config = rusternetes_controller_manager::ControllerManagerConfig {
        sync_interval: args.sync_interval,
        pv_recycler: recycler_config!(
            &args,
            rusternetes_controller_manager::controllers::pv_recycler
        )?,
        // The standalone binary spawns these itself (with leader election).
        csi_attachers: Vec::new(),
        attach_detach_controller: false,
        bootstrap_signer_controller: false,
        selinux_warning_controller: false,
        terminated_pod_gc_threshold: args.terminated_pod_gc_threshold,
        token_cleaner_controller: false,
        service_cluster_ip_range: args.service_cluster_ip_range.clone().unwrap_or_default(),
        // Route HPA metric fetches through the same kubeconfig-derived
        // credentials as the data plane (CA + bearer token, honouring the
        // insecure flag) instead of the default mTLS client cert under
        // /etc/kubernetes/pki, which the static pod doesn't have. (#1144)
        // NB: the lib-qualified type — `ControllerManagerConfig` lives in the
        // library crate, whose `HttpMetricsConfig` is distinct from the bin's
        // own `mod controllers` copy imported at the top of this file.
        metrics_config: Some(
            rusternetes_controller_manager::controllers::hpa_metrics_client::HttpMetricsConfig {
                api_server_url: args.api_server_url.clone(),
                ca_pem: ca_pem.clone(),
                ca_cert_path: None,
                client_cert_path: None,
                client_key_path: None,
                token,
                insecure_skip_tls_verify: insecure,
            },
        ),
        // The namespace controller can't read the CA from the api-server's
        // cert paths in its own container — hand it the kubeconfig-resolved CA
        // so it can (re)create kube-root-ca.crt in every namespace.
        ca_cert_pem: ca_pem.and_then(|b| String::from_utf8(b).ok()),
        legacy_sa_token_clean_up_period: args.legacy_service_account_token_clean_up_period,
        // Lib-qualified NodeIpamConfig (distinct from the bin's `mod controllers`
        // copy), so it matches the type ControllerManagerConfig expects.
        node_ipam: match node_ipam_params(&args)? {
            Some((cidr, masks, svc)) => Some(
                rusternetes_controller_manager::controllers::node_ipam::NodeIpamConfig::from_flags(
                    &cidr,
                    rusternetes_controller_manager::controllers::node_ipam::NodeCidrMaskSizes {
                        general: masks.general,
                        ipv4: masks.ipv4,
                        ipv6: masks.ipv6,
                    },
                    svc.as_deref(),
                )
                .map_err(|e| anyhow::anyhow!(e))?,
            ),
            None => None,
        },
    };
    rusternetes_controller_manager::run_with_api(client, config).await
}

/// Apply `--emulated-version`, `--min-compatibility-version` and
/// `--feature-gates` process-wide: `ComponentGlobalsRegistry.Set()`
/// (cmd/kube-controller-manager/app/controllermanager.go:124) then `Validate`
/// (staging/src/k8s.io/component-base/compatibility/registry.go:344-451).
fn apply_feature_gates(args: &Args) -> Result<rusternetes_common::compatibility::EffectiveVersion> {
    let effective_version = rusternetes_common::compatibility::apply_flags(
        &args.emulated_version,
        &args.min_compatibility_version,
        &args.feature_gates,
    )
    .map_err(|e| anyhow::anyhow!("invalid --feature-gates/--emulated-version: {e}"))?;
    // `ComponentGlobalsRegistry.AddMetrics()`
    // (cmd/kube-controller-manager/app/controllermanager.go:146;
    // component-base/compatibility/registry.go:123-131): record `version_info`.
    rusternetes_common::compat_version_metrics::add_metrics(
        rusternetes_common::compatibility::DEFAULT_KUBE_COMPONENT,
        &effective_version,
    );
    // #3190: legacy alias for the CrossNamespaceVolumeDataSource gate; the
    // provisioner reads the gate (upstream has only the gate).
    if args.csi_cross_namespace_volume_data_source {
        rusternetes_common::feature_gates::set(
            rusternetes_common::feature_gates::Feature::CrossNamespaceVolumeDataSource,
            true,
        );
    }
    Ok(effective_version)
}

#[tokio::main]
async fn main() -> Result<()> {
    // `options.Flags` / `ParsedFlags` -> `flagz.NamedFlagSetsReader`
    // (cmd/kube-controller-manager/app/options/options.go:535-539).
    let cmd = <Args as clap::CommandFactory>::command();
    let matches = cmd.clone().get_matches();
    let flags = rusternetes_common::flagz::flags_from_clap(&cmd, &matches);
    let args = <Args as clap::FromArgMatches>::from_arg_matches(&matches)
        .map_err(|e| e.exit())
        .unwrap();
    let effective_version = apply_feature_gates(&args)?;

    rusternetes_common::tracing::init_basic_tracing("controller-manager", &args.log_level)?;

    for flag in args.ignored_upstream_flags() {
        warn!("{flag} is accepted for upstream argv compatibility and ignored (not implemented)");
    }
    let le = args
        .leader_election_settings()
        .map_err(|e| anyhow::anyhow!(e))?;

    // In-cluster static-pod path: run as an api-server client, no storage handle.
    if args.kubeconfig.is_some() {
        info!(
            "Starting Rusternetes Controller Manager (API mode) {}",
            rusternetes_common::build_info::version_line()
        );
        return run_api_mode(args, effective_version, flags).await;
    }

    serve_secure(&args, controllers::gather_metrics, effective_version, flags).await?;

    info!(
        "Starting Rusternetes Controller Manager {}",
        rusternetes_common::build_info::version_line()
    );

    // Resolve node-IPAM params before `args` is partially moved below.
    let ipam_params = node_ipam_params(&args)?;

    // Initialize storage
    let storage_config = match args.storage_backend.as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            info!("Using SQLite storage backend at: {}", args.data_dir);
            StorageConfig::Sqlite {
                path: args.data_dir.clone(),
            }
        }
        _ => {
            let etcd_endpoints: Vec<String> = args
                .etcd_servers
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            info!("Connecting to etcd: {:?}", etcd_endpoints);
            StorageConfig::Etcd {
                endpoints: etcd_endpoints,
            }
        }
    };
    let storage = Arc::new(StorageBackend::new(storage_config).await?);

    // Initialize cloud provider if configured
    #[cfg(feature = "cloud-providers")]
    let cloud_provider: Option<Arc<dyn CloudProvider>> = {
        if let Some(provider_str) = &args.cloud_provider {
            use rusternetes_common::cloud_provider::CloudProviderType;

            let provider_type = CloudProviderType::from_str(provider_str)
                .ok_or_else(|| anyhow::anyhow!("Invalid cloud provider: {}", provider_str))?;

            if provider_type != CloudProviderType::None {
                let mut config = std::collections::HashMap::new();
                if let Some(region) = &args.cloud_region {
                    config.insert("region".to_string(), region.clone());
                }

                info!("Initializing {} cloud provider", provider_type.as_str());
                let provider = rusternetes_cloud_providers::create_provider(
                    provider_type,
                    args.cluster_name.clone(),
                    config,
                )
                .await?;
                Some(provider)
            } else {
                None
            }
        } else {
            // Auto-detect cloud provider
            let detected = rusternetes_cloud_providers::detect_cloud_provider();
            if detected != rusternetes_common::cloud_provider::CloudProviderType::None {
                info!("Auto-detected cloud provider: {}", detected.as_str());
                let mut config = std::collections::HashMap::new();
                if let Some(region) = &args.cloud_region {
                    config.insert("region".to_string(), region.clone());
                }
                let provider = rusternetes_cloud_providers::create_provider(
                    detected,
                    args.cluster_name.clone(),
                    config,
                )
                .await
                .ok();
                provider
            } else {
                None
            }
        }
    };

    #[cfg(not(feature = "cloud-providers"))]
    let cloud_provider: Option<Arc<dyn CloudProvider>> = None;

    // Initialize leader election if enabled
    let leader_elector = if le.enabled {
        let identity = args
            .leader_election_identity
            .clone()
            .unwrap_or_else(|| format!("controller-manager-{}", uuid::Uuid::new_v4()));
        if args.leader_election_lock_key.is_some() {
            warn!("--leader-election-lock-key is ignored: the lock is a coordination.k8s.io Lease");
        }

        info!(
            identity = %identity,
            "Leader election enabled - starting in follower mode"
        );

        // OnStoppedLeading (cmd/kube-controller-manager/app/controllermanager.go:367-369):
        // "leaderelection lost", then `klog.FlushAndExit(.., 1)`, so every
        // controller stops with the process; the restart policy brings it
        // back as a fresh follower. Upstream runs with context.Background()
        // and without ReleaseOnCancel (see the shutdown note below).
        let elector = Arc::new(rusternetes_common::leaderelection::LeaderElector::new(
            rusternetes_common::leaderelection::ElectionConfig {
                lock: Arc::new(
                    rusternetes_storage::lease_lock::LeaseLock::new(
                        storage.clone(),
                        args.leader_election_resource_namespace.clone(),
                        args.leader_election_resource_name.clone(),
                        identity.clone(),
                    )
                    // LockConfig.EventRecorder (options.go makeLeaderElectionConfig).
                    .with_event_recorder(
                        rusternetes_storage::EventRecorder::new(storage.clone()),
                        "kube-controller-manager",
                    ),
                ),
                lease_duration: le.lease_duration,
                renew_deadline: le.renew_deadline,
                retry_period: le.retry_period,
                release_on_cancel: false,
                name: args.leader_election_resource_name.clone(),
                callbacks: rusternetes_common::leaderelection::LeaderCallbacks {
                    on_started_leading: Arc::new(|_| {}),
                    on_stopped_leading: Arc::new(|| {
                        tracing::error!("leaderelection lost");
                        std::process::exit(1);
                    }),
                    on_new_leader: None,
                },
            },
        )?);

        // Start leader election in background (`go leaderElectAndRun`).
        let elector_clone = elector.clone();
        tokio::spawn(async move {
            elector_clone
                .run(rusternetes_common::signal::ShutdownContext::new())
                .await
        });

        Some(elector)
    } else {
        warn!("Leader election disabled - running in single-instance mode");
        None
    };

    // Macro to spawn controllers with leader election support
    macro_rules! spawn_controller {
        ($name:expr, $elector:expr, $fut:expr) => {{
            let elector_clone = $elector.clone();
            tokio::spawn(async move {
                // Wait until we're the leader (if leader election is enabled)
                if let Some(ref elector) = elector_clone {
                    loop {
                        while !elector.is_leader() {
                            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                        }
                        info!("{} starting (leader acquired)", $name);

                        // Run controller under crash supervision: a panic inside
                        // tokio::spawn is otherwise swallowed and the controller
                        // silently stops reconciling forever (#1775).
                        rusternetes_controller_manager::supervise_controller(
                            $name,
                            None,
                            std::time::Duration::from_secs(1),
                            || $fut,
                        )
                        .await;

                        // Check if we're still the leader
                        if !elector.is_leader() {
                            warn!("{} stopped (lost leadership)", $name);
                            continue;
                        }
                        break;
                    }
                } else {
                    // No leader election, run directly — still supervised, so a
                    // panicking controller is logged and restarted rather than
                    // disappearing (#1775: a futures Unfold panic killed the
                    // ResourceQuota controller and froze every quota's
                    // status.used until the process was restarted).
                    rusternetes_controller_manager::supervise_controller(
                        $name,
                        None,
                        std::time::Duration::from_secs(1),
                        || $fut,
                    )
                    .await;
                }
            })
        }};
    }

    // Start LoadBalancer controller
    let lb_controller = Arc::new(LoadBalancerController::new(
        storage.clone(),
        cloud_provider.clone(),
        args.cluster_name.clone(),
        args.sync_interval,
    ));
    spawn_controller!("LoadBalancer controller", leader_elector, {
        let controller = lb_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("LoadBalancer controller error: {}", e);
            }
        }
    });

    // Start deployment controller
    let deployment_controller = Arc::new(DeploymentController::new(
        storage.clone(),
        args.sync_interval,
    ));
    spawn_controller!("Deployment controller", leader_elector, {
        let controller = deployment_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Deployment controller error: {}", e);
            }
        }
    });

    // Start replicationcontroller controller
    // One pod informer (one watch, one cache) feeds both controllers' pod
    // event handlers (client-go `sharedIndexInformer` + `sharedProcessor`).
    let pod_informer = Arc::new(controllers::pod_informer::SharedPodInformer::new(
        storage.clone(),
        std::time::Duration::from_secs(args.sync_interval),
    ));
    let rc_controller = Arc::new(ReplicationControllerController::with_pod_informer(
        storage.clone(),
        args.sync_interval,
        pod_informer.clone(),
    ));
    spawn_controller!("ReplicationController controller", leader_elector, {
        let controller = rc_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("ReplicationController controller error: {}", e);
            }
        }
    });

    // Start ReplicaSet controller
    let replicaset_controller = Arc::new(ReplicaSetController::with_pod_informer(
        storage.clone(),
        args.sync_interval,
        pod_informer.clone(),
    ));
    spawn_controller!("ReplicaSet controller", leader_elector, {
        let controller = replicaset_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("ReplicaSet controller error: {}", e);
            }
        }
    });

    // Start StatefulSet controller
    let statefulset_controller = Arc::new(StatefulSetController::with_pod_informer(
        storage.clone(),
        pod_informer.clone(),
    ));
    spawn_controller!("StatefulSet controller", leader_elector, {
        let controller = statefulset_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("StatefulSet controller error: {}", e);
            }
        }
    });

    // Start DaemonSet controller
    let daemonset_controller = Arc::new(DaemonSetController::with_pod_informer(
        storage.clone(),
        pod_informer.clone(),
    ));
    spawn_controller!("DaemonSet controller", leader_elector, {
        let controller = daemonset_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("DaemonSet controller error: {}", e);
            }
        }
    });

    // Start Job controller
    let job_controller = Arc::new(JobController::with_pod_informer(
        storage.clone(),
        pod_informer,
    ));
    spawn_controller!("Job controller", leader_elector, {
        let controller = job_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Job controller error: {}", e);
            }
        }
    });

    // Start CronJob controller
    let cronjob_controller = Arc::new(CronJobController::new(storage.clone()));
    spawn_controller!("CronJob controller", leader_elector, {
        let controller = cronjob_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("CronJob controller error: {}", e);
            }
        }
    });

    // Start PV/PVC Binder controller
    let pv_binder_controller = Arc::new(
        PVBinderController::new(storage.clone())
            .with_recycler_config(recycler_config!(&args, controllers::pv_recycler)?),
    );
    spawn_controller!("PV/PVC Binder controller", leader_elector, {
        let controller = pv_binder_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("PV/PVC Binder controller error: {}", e);
            }
        }
    });

    // Start PVC protection controller (pvcprotection.NewPVCProtectionController)
    let pvc_protection_controller = Arc::new(PvcProtectionController::new(storage.clone()));
    spawn_controller!("PVC protection controller", leader_elector, {
        let controller = pvc_protection_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("PVC protection controller error: {}", e);
            }
        }
    });

    // Start generic ephemeral volume controller (ephemeral.NewController)
    let ephemeral_volume_controller = Arc::new(EphemeralVolumeController::new(storage.clone()));
    spawn_controller!("Ephemeral volume controller", leader_elector, {
        let controller = ephemeral_volume_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Ephemeral volume controller error: {}", e);
            }
        }
    });

    // Start PV protection controller (pvprotection.NewPVProtectionController)
    let pv_protection_controller = Arc::new(PvProtectionController::new(storage.clone()));
    spawn_controller!("PV protection controller", leader_elector, {
        let controller = pv_protection_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("PV protection controller error: {}", e);
            }
        }
    });

    // Start VAC protection controller (vacprotection.NewVACProtectionController)
    let vac_protection_controller = Arc::new(VacProtectionController::new(storage.clone()));
    spawn_controller!("VAC protection controller", leader_elector, {
        let controller = vac_protection_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("VAC protection controller error: {}", e);
            }
        }
    });

    // Start Dynamic Provisioner controller
    let dynamic_provisioner_controller =
        Arc::new(DynamicProvisionerController::new(storage.clone()));
    spawn_controller!("Dynamic Provisioner controller", leader_elector, {
        let controller = dynamic_provisioner_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Dynamic Provisioner controller error: {}", e);
            }
        }
    });

    // Start one CSI external-provisioner per `--csi-provisioner` driver.
    let mut csi_provisioner_handles = Vec::new();
    for spec in &args.csi_provisioners {
        let Some((driver, socket)) = spec.split_once('=') else {
            anyhow::bail!("--csi-provisioner {spec:?}: expected <driver-name>=<socket path>");
        };
        let mut node_deployment = node_deployment_settings(&args, std::env::var("NODE_NAME").ok())?;
        if let Some(nd) = node_deployment.as_mut() {
            // `ctrl.GetNodeInfo` (controller.go:350-355); failure is fatal
            // (csi-provisioner.go:330-332).
            let info =
                rusternetes_csi::controller_client::CsiControllerClient::with_endpoint(socket)
                    .get_node_info()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to get node info from CSI driver: {e}"))?;
            nd.accessible_topology = info.accessible_topology.map(|t| t.segments);
        }
        let mut provisioner = controllers::csi_provisioner::CsiProvisioner::new(
            storage.clone(),
            driver,
            rusternetes_csi::controller_client::CsiControllerClient::with_endpoint(socket),
        );
        let capacity_node_deployment = node_deployment.clone();
        if let Some(nd) = node_deployment {
            provisioner = provisioner.with_node_deployment(nd);
        }
        // csi-provisioner.go:546-548: "Wrap Provision and Delete to detect when
        // it is time to refresh capacity."
        let capacity_refresher: Arc<controllers::csi_capacity_refresh::CapacityRefresher> =
            Default::default();
        if args.csi_enable_capacity {
            provisioner = provisioner.with_capacity_refresher(capacity_refresher.clone());
        }
        let provisioner = Arc::new(
            provisioner
                .with_default_fs_type(args.csi_default_fstype.clone())
                .with_extra_create_metadata(args.csi_extra_create_metadata)
                .with_controller_publish_read_only(args.csi_controller_publish_readonly)
                .with_prevent_volume_mode_conversion(args.csi_prevent_volume_mode_conversion)
                .with_retry_interval_max(args.csi_retry_interval_max)
                .with_cross_namespace_volume_data_source(
                    rusternetes_common::feature_gates::enabled(
                        rusternetes_common::feature_gates::Feature::CrossNamespaceVolumeDataSource,
                    ),
                )
                .with_strict_topology(args.csi_strict_topology)
                .with_immediate_topology(args.csi_immediate_topology),
        );
        if args.csi_enable_capacity {
            // csi-provisioner.go:464-486: NAMESPACE required; POD_NAME unless level < 0.
            let namespace = std::env::var("NAMESPACE").unwrap_or_default();
            if namespace.is_empty() {
                anyhow::bail!("need NAMESPACE env variable for CSIStorageCapacity objects");
            }
            let owner = if args.csi_capacity_ownerref_level >= 0 {
                let pod_name = std::env::var("POD_NAME").unwrap_or_default();
                if pod_name.is_empty() {
                    anyhow::bail!(
                        "need POD_NAME env variable to determine CSIStorageCapacity owner"
                    );
                }
                Some(
                    controllers::csi_capacity::lookup_owner(
                        &*storage,
                        &namespace,
                        &pod_name,
                        "v1",
                        "Pod",
                        args.csi_capacity_ownerref_level,
                        &controllers::csi_capacity::capacity_limiter(
                            args.csi_kube_api_capacity_qps,
                            args.csi_kube_api_capacity_burst,
                        ),
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("look up owner(s) of pod: {e}"))?,
                )
            } else {
                None
            };
            let (topology, node) = match &capacity_node_deployment {
                Some(nd) => (
                    controllers::csi_capacity::TopologySource::Fixed(
                        controllers::csi_capacity::fixed_segment(nd.accessible_topology.as_ref()),
                    ),
                    Some(nd.node_name.clone()),
                ),
                None => (controllers::csi_capacity::TopologySource::Discovered, None),
            };
            let managed_by_id = controllers::csi_capacity::managed_by_id(node.as_deref());
            let immediate_binding = args.csi_capacity_for_immediate_binding;
            let poll = args.csi_capacity_poll_interval;
            // `--retry-interval-start` (csi-provisioner.go, default 1s; no
            // flag here) and `--retry-interval-max`: the capacity queue's
            // `NewTypedItemExponentialFailureRateLimiter` (:528).
            let retry_start = std::time::Duration::from_secs(1);
            let retry_max = args.csi_retry_interval_max;
            let threads = args.csi_capacity_threads;
            // csi-provisioner.go:453-458: "Publishing storage capacity
            // information uses its own client with separate rate limiting."
            let capacity_limiter = controllers::csi_capacity::capacity_limiter(
                args.csi_kube_api_capacity_qps,
                args.csi_kube_api_capacity_burst,
            );
            let cap_storage = storage.clone();
            let cap_driver = driver.to_string();
            let cap_socket = socket.to_string();
            spawn_controller!("CSI capacity publisher", leader_elector, {
                let storage = cap_storage.clone();
                let driver = cap_driver.clone();
                let socket = cap_socket.clone();
                let owner = owner.clone();
                let managed_by_id = managed_by_id.clone();
                let namespace = namespace.clone();
                let topology = topology.clone();
                let refresher = capacity_refresher.clone();
                let limiter = capacity_limiter.clone();
                async move {
                    let client =
                        rusternetes_csi::controller_client::CsiControllerClient::with_endpoint(
                            socket,
                        );
                    let ctl = controllers::csi_capacity::CapacityController {
                        storage: &*storage,
                        client: &client,
                        driver_name: driver,
                        owner,
                        managed_by_id,
                        owner_namespace: namespace,
                        immediate_binding,
                        refresher,
                        limiter,
                    };
                    controllers::csi_capacity::run_capacity_loop(
                        &ctl,
                        &topology,
                        poll,
                        retry_start,
                        retry_max,
                        threads,
                    )
                    .await;
                }
            });
        }
        csi_provisioner_handles.push(provisioner.clone());
        spawn_controller!("CSI provisioner", leader_elector, {
            let controller = provisioner.clone();
            async move {
                if let Err(e) = controller.run().await {
                    tracing::error!("CSI provisioner error: {}", e);
                }
            }
        });
    }

    // Start one CSI external-attacher per `--csi-attacher` driver.
    for spec in &args.csi_attachers {
        let Some((driver, socket)) = spec.split_once('=') else {
            anyhow::bail!("--csi-attacher {spec:?}: expected <driver-name>=<socket path>");
        };
        let attacher = Arc::new(
            controllers::csi_attacher::CsiAttacher::new(
                storage.clone(),
                driver,
                rusternetes_csi::controller_client::CsiControllerClient::with_endpoint(socket),
            )
            .with_default_fs_type(args.csi_default_fstype.clone())
            .with_reconcile_sync(std::time::Duration::from_secs(
                args.csi_attacher_reconcile_sync_seconds,
            ))
            .with_max_entries(args.csi_attacher_max_entries),
        );
        spawn_controller!("CSI attacher", leader_elector, {
            let controller = attacher.clone();
            async move {
                if let Err(e) = controller.run().await {
                    tracing::error!("CSI attacher error: {e}");
                }
            }
        });
    }

    // AttachDetachController (#3048), opt-in.
    if args.attach_detach_controller {
        let attach_detach = Arc::new(
            controllers::attach_detach::AttachDetachController::new(storage.clone())
                .with_max_wait_for_unmount(std::time::Duration::from_secs(
                    args.attach_detach_max_wait_for_unmount_seconds,
                ))
                .with_disable_force_detach_on_timeout(
                    args.attach_detach_disable_force_detach_on_timeout,
                )
                .with_reconciler_sync_period(std::time::Duration::from_secs(
                    args.attach_detach_reconcile_sync_period_seconds,
                ))
                .with_disable_reconciliation_sync(args.disable_attach_detach_reconcile_sync),
        );
        spawn_controller!("AttachDetach controller", leader_elector, {
            let controller = attach_detach.clone();
            async move {
                if let Err(e) = controller.run().await {
                    tracing::error!("AttachDetach controller error: {e}");
                }
            }
        });
    }

    // BootstrapSigner (`newBootstrapSignerController`, bootstrap.go:30-53), opt-in.
    if args.bootstrap_signer_controller {
        let bootstrap_signer =
            Arc::new(controllers::bootstrap_signer::Signer::new(storage.clone()));
        spawn_controller!("BootstrapSigner controller", leader_elector, {
            let controller = bootstrap_signer.clone();
            async move {
                if let Err(e) = controller.run().await {
                    tracing::error!("BootstrapSigner controller error: {e}");
                }
            }
        });
    }

    // TokenCleaner (`newTokenCleanerController`, bootstrap.go:55-80), opt-in.
    if args.token_cleaner_controller {
        let token_cleaner = Arc::new(controllers::bootstrap_token_cleaner::TokenCleaner::new(
            storage.clone(),
        ));
        spawn_controller!("TokenCleaner controller", leader_elector, {
            let controller = token_cleaner.clone();
            async move {
                if let Err(e) = controller.run().await {
                    tracing::error!("TokenCleaner controller error: {e}");
                }
            }
        });
    }

    // `newSELinuxWarningControllerDescriptor` (core.go:1107), opt-in.
    if args.selinux_warning_controller && controllers::selinux_warning::feature_gates_enabled() {
        let selinux_warning = Arc::new(
            controllers::selinux_warning::SELinuxWarningController::new(storage.clone()),
        );
        spawn_controller!("SELinuxWarning controller", leader_elector, {
            let controller = selinux_warning.clone();
            async move {
                if let Err(e) = controller.run().await {
                    tracing::error!("SELinuxWarning controller error: {e}");
                }
            }
        });
    }

    // Start Volume Snapshot controller
    let volume_snapshot_controller = Arc::new(VolumeSnapshotController::new(storage.clone()));
    spawn_controller!("Volume Snapshot controller", leader_elector, {
        let controller = volume_snapshot_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Volume Snapshot controller error: {}", e);
            }
        }
    });

    // Start volume-data-source-validator (kubernetes-csi/volume-data-source-validator)
    let volume_data_source_validator = Arc::new(VolumeDataSourceValidator::new(storage.clone()));
    spawn_controller!("Volume Data Source Validator", leader_elector, {
        let controller = volume_data_source_validator.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Volume Data Source Validator error: {}", e);
            }
        }
    });

    // Start Volume Expansion controller
    let volume_expansion_controller = Arc::new(VolumeExpansionController::new(storage.clone()));
    spawn_controller!("Volume Expansion controller", leader_elector, {
        let controller = volume_expansion_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Volume Expansion controller error: {}", e);
            }
        }
    });

    // Start StorageClass controller
    let storage_class_controller = Arc::new(StorageClassController::new(storage.clone()));
    spawn_controller!("StorageClass controller", leader_elector, {
        let controller = storage_class_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("StorageClass controller error: {}", e);
            }
        }
    });

    // Start Endpoints controller (watch-based)
    let endpoints_controller = Arc::new(EndpointsController::new(storage.clone()));
    spawn_controller!("Endpoints controller", leader_elector, {
        let controller = endpoints_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Endpoints controller error: {}", e);
            }
        }
    });

    // Start EndpointSlice controller (watch-based)
    let endpointslice_controller = Arc::new(EndpointSliceController::new(storage.clone()));
    spawn_controller!("EndpointSlice controller", leader_elector, {
        let controller = endpointslice_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("EndpointSlice controller error: {}", e);
            }
        }
    });

    // Start Events controller
    let events_controller = Arc::new(EventsController::new(storage.clone(), args.sync_interval));
    spawn_controller!("Events controller", leader_elector, {
        let controller = events_controller.clone();
        async move {
            controller.run().await;
        }
    });

    // Start ResourceQuota controller
    let resource_quota_controller = Arc::new(ResourceQuotaController::new(storage.clone()));
    spawn_controller!("ResourceQuota controller", leader_elector, {
        let controller = resource_quota_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("ResourceQuota controller error: {}", e);
            }
        }
    });

    // Start Garbage Collector
    let garbage_collector = Arc::new(GarbageCollector::new(storage.clone()));
    spawn_controller!("Garbage Collector", leader_elector, {
        let controller = garbage_collector.clone();
        async move {
            controller.run().await;
        }
    });

    // Start HPA controller
    let hpa_metrics_cfg = HttpMetricsConfig {
        api_server_url: args.api_server_url.clone(),
        ca_pem: None,
        ca_cert_path: Some(format!("{}/ca.crt", args.pki_dir)),
        client_cert_path: Some(format!("{}/api-server.crt", args.pki_dir)),
        client_key_path: Some(format!("{}/api-server.key", args.pki_dir)),
        token: None,
        insecure_skip_tls_verify: args.metrics_insecure_skip_tls_verify,
    };
    let hpa_controller = Arc::new(HorizontalPodAutoscalerController::with_config(
        storage.clone(),
        hpa_metrics_cfg,
    ));
    spawn_controller!("HPA controller", leader_elector, {
        let controller = hpa_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                error!("HPA controller error: {}", e);
            }
        }
    });

    // Start VPA controller
    let vpa_controller = Arc::new(VerticalPodAutoscalerController::new(Arc::clone(&storage)));
    spawn_controller!("VPA controller", leader_elector, {
        let controller = vpa_controller.clone();
        async move {
            controller.run().await;
        }
    });

    // Start TTL controller
    let ttl_controller = Arc::new(TTLController::new(storage.clone()));
    spawn_controller!("TTL controller", leader_elector, {
        let controller = ttl_controller.clone();
        async move {
            controller.run().await;
        }
    });

    // Start PodDisruptionBudget controller
    let pdb_controller = Arc::new(PodDisruptionBudgetController::new(storage.clone()));
    spawn_controller!("PodDisruptionBudget controller", leader_elector, {
        let controller = pdb_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                error!("PodDisruptionBudget controller error: {}", e);
            }
        }
    });

    // Start StalePodDisruption sub-controller
    let stale_pdb_controller = Arc::new(StalePodDisruptionController::new(storage.clone()));
    spawn_controller!("StalePodDisruption controller", leader_elector, {
        let controller = stale_pdb_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                error!("StalePodDisruption controller error: {}", e);
            }
        }
    });

    // Start NetworkPolicy controller
    let network_policy_controller = Arc::new(NetworkPolicyController::new(storage.clone()));
    spawn_controller!("NetworkPolicy controller", leader_elector, {
        let controller = network_policy_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("NetworkPolicy controller error: {}", e);
            }
        }
    });

    // Start Ingress controller
    let ingress_controller = Arc::new(IngressController::new(storage.clone()));
    spawn_controller!("Ingress controller", leader_elector, {
        let controller = ingress_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Ingress controller error: {}", e);
            }
        }
    });

    // Start CertificateSigningRequest controller, enabling the in-process signer
    // when a cluster CA is provided via env (RUSTERNETES_CA_CERT_FILE/KEY_FILE).
    let csr_controller = {
        let mut controller = CertificateSigningRequestController::new(storage.clone());
        if let Some(ca) = controllers::cert_authority::load_cluster_ca_from_env() {
            controller = controller.with_certificate_authority(ca);
        }
        Arc::new(controller)
    };
    spawn_controller!("CertificateSigningRequest controller", leader_elector, {
        let controller = csr_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("CertificateSigningRequest controller error: {}", e);
            }
        }
    });

    // Start the pod garbage collector (`newPodGarbageCollectorController`,
    // cmd/kube-controller-manager/app/core.go:564-576).
    let podgc = Arc::new(controllers::podgc::PodGcController::new(
        storage.clone(),
        args.terminated_pod_gc_threshold,
    ));
    spawn_controller!("PodGC controller", leader_elector, {
        let controller = podgc.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("PodGC controller error: {}", e);
            }
        }
    });

    // Start the device taint eviction controller
    // (`newDeviceTaintEvictionController`, cmd/kube-controller-manager/app/
    // core.go:251-284; `requiredFeatureGates`: DynamicResourceAllocation and
    // DRADeviceTaints).
    if controllers::device_taint_eviction::feature_enabled() {
        let device_taint_eviction = Arc::new(
            controllers::device_taint_eviction::DeviceTaintEvictionController::new(storage.clone()),
        );
        spawn_controller!("DeviceTaintEviction controller", leader_elector, {
            let controller = device_taint_eviction.clone();
            async move {
                if let Err(e) = controller
                    .run(controllers::device_taint_eviction::DEFAULT_CONCURRENT_SYNCS)
                    .await
                {
                    tracing::error!("DeviceTaintEviction controller error: {}", e);
                }
            }
        });
    }

    // Start the CSR cleaner (`newCertificateSigningRequestCleanerController`,
    // cmd/kube-controller-manager/app/certificates.go:217-230).
    let csr_cleaner = Arc::new(controllers::csr_cleaner::CsrCleanerController::new(
        storage.clone(),
    ));
    spawn_controller!("CSR cleaner controller", leader_elector, {
        let controller = csr_cleaner.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("CSR cleaner controller error: {}", e);
            }
        }
    });

    // Start the kube-apiserver-serving ClusterTrustBundle publisher
    // (certificates.go:289-353): ClusterTrustBundle gate, root CA from pki_dir.
    if rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::ClusterTrustBundle,
    ) {
        let root_ca = std::fs::read_to_string(format!("{}/ca.crt", args.pki_dir)).ok();
        match controllers::clustertrustbundle_publisher::ClusterTrustBundlePublisher::for_kube_apiserver_serving(
            storage.clone(),
            root_ca.as_deref(),
        ) {
            Ok(Some(publisher)) => {
                let publisher = Arc::new(publisher);
                spawn_controller!("ClusterTrustBundle publisher", leader_elector, {
                    let controller = publisher.clone();
                    async move {
                        if let Err(e) = controller.run().await {
                            tracing::error!("ClusterTrustBundle publisher error: {}", e);
                        }
                    }
                });
            }
            Ok(None) => {}
            Err(e) => tracing::error!("ClusterTrustBundle publisher not started: {}", e),
        }
    }

    // Start the PodCertificateRequest cleaner
    // (`newPodCertificateRequestCleanerController`,
    // cmd/kube-controller-manager/app/certificates.go:232-253), gated on the
    // `PodCertificateRequest` feature gate.
    if rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::PodCertificateRequest,
    ) {
        let pcr_cleaner = Arc::new(controllers::pcr_cleaner::PcrCleanerController::new(
            storage.clone(),
        ));
        spawn_controller!(
            "PodCertificateRequest cleaner controller",
            leader_elector,
            {
                let controller = pcr_cleaner.clone();
                async move {
                    if let Err(e) = controller.run().await {
                        tracing::error!("PodCertificateRequest cleaner controller error: {}", e);
                    }
                }
            }
        );
    }

    // Start Namespace controller (watch-based). Hand it the CA cert so it can
    // (re)create kube-root-ca.crt in every namespace (read from pki_dir, the
    // same dir the metrics/cert paths use above).
    let ns_ca = std::fs::read_to_string(format!("{}/ca.crt", args.pki_dir)).ok();
    let namespace_controller =
        Arc::new(NamespaceController::new(storage.clone()).with_ca_cert(ns_ca.clone()));
    spawn_controller!("Namespace controller", leader_elector, {
        let controller = namespace_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Namespace controller error: {}", e);
            }
        }
    });

    // Start TaintEviction controller (watch-based)
    let taint_eviction_controller =
        Arc::new(crate::controllers::taint_eviction::TaintEvictionController::new(storage.clone()));
    spawn_controller!("TaintEviction controller", leader_elector, {
        let controller = taint_eviction_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("TaintEviction controller error: {}", e);
            }
        }
    });

    // Start ClusterRole aggregation controller (watch-based)
    let clusterrole_aggregation_controller =
        Arc::new(ClusterRoleAggregationController::new(storage.clone()));
    spawn_controller!("ClusterRoleAggregator controller", leader_elector, {
        let controller = clusterrole_aggregation_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("ClusterRoleAggregator controller error: {}", e);
            }
        }
    });

    // Start ServiceAccount controller (watch-based)
    let serviceaccount_controller =
        Arc::new(ServiceAccountController::new(storage.clone()).with_ca_cert(ns_ca));
    spawn_controller!("ServiceAccount controller", leader_elector, {
        let controller = serviceaccount_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("ServiceAccount controller error: {}", e);
            }
        }
    });

    // Start legacy service-account token cleaner
    // (`newLegacyServiceAccountTokenCleanerController`, core.go:933-963).
    let legacy_cleaner = Arc::new(
        controllers::legacy_serviceaccount_token_cleaner::LegacySATokenCleaner::new(
            storage.clone(),
            controllers::legacy_serviceaccount_token_cleaner::LegacySATokenCleanerOptions {
                clean_up_period: args.legacy_service_account_token_clean_up_period,
                sync_interval:
                    controllers::legacy_serviceaccount_token_cleaner::DEFAULT_CLEANER_SYNC_INTERVAL,
            },
        )
        .map_err(|e| {
            anyhow::anyhow!("failed to init the legacy service account token cleaner: {e}")
        })?,
    );
    spawn_controller!(
        "LegacyServiceAccountTokenCleaner controller",
        leader_elector,
        {
            let controller = legacy_cleaner.clone();
            async move {
                if let Err(e) = controller.run().await {
                    tracing::error!("LegacyServiceAccountTokenCleaner controller error: {}", e);
                }
            }
        }
    );

    // Start Service controller (watch-based)
    let service_controller = Arc::new(
        ServiceController::new(storage.clone())
            .with_service_cluster_ip_range(args.service_cluster_ip_range.as_deref().unwrap_or(""))
            .map_err(|e| anyhow::anyhow!("invalid --service-cluster-ip-range: {e}"))?,
    );
    spawn_controller!("Service controller", leader_elector, {
        let controller = service_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Service controller error: {}", e);
            }
        }
    });

    // Start Node controller (watch-based)
    let node_controller = Arc::new(NodeController::with_eviction_config(
        storage.clone(),
        controllers::node_lifecycle_zone::EvictionConfig {
            eviction_limiter_qps: args.node_eviction_rate,
            secondary_eviction_limiter_qps: args.secondary_node_eviction_rate,
            large_cluster_threshold: args.large_cluster_size_threshold,
            unhealthy_zone_threshold: args.unhealthy_zone_threshold,
        },
    ));
    spawn_controller!("Node controller", leader_elector, {
        let controller = node_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("Node controller error: {}", e);
            }
        }
    });

    // Start Node IPAM (range allocator: 30 dedicated workers over an in-memory
    // CidrSet; upstream pkg/controller/nodeipam/ipam/range_allocator.go).
    if let Some((cidr, masks, svc)) = ipam_params.clone() {
        let cfg = NodeIpamConfig::from_flags(&cidr, masks, svc.as_deref())
            .map_err(|e| anyhow::anyhow!(e))?;
        info!(
            "Node IPAM enabled: cluster-cidr={cidr}, node-masks={:?}",
            cfg.node_masks
        );
        let ipam_storage = storage.clone();
        spawn_controller!("Node IPAM controller", leader_elector, {
            let storage = ipam_storage.clone();
            let cfg = cfg.clone();
            async move {
                if let Err(e) = node_ipam::run_node_ipam(storage, cfg).await {
                    tracing::error!("Node IPAM controller error: {}", e);
                }
            }
        });
    }

    // Start PriorityClass controller
    let priorityclass_controller = Arc::new(PriorityClassController::new(storage.clone()));
    spawn_controller!("PriorityClass controller", leader_elector, {
        let controller = priorityclass_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("PriorityClass controller error: {}", e);
            }
        }
    });

    // Start APIService availability controller
    let apiservice_controller = Arc::new(APIServiceAvailabilityController::new(storage.clone()));
    spawn_controller!("APIService availability controller", leader_elector, {
        let controller = apiservice_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("APIService availability controller error: {}", e);
            }
        }
    });

    // Start ServiceCIDR controller (upstream `service-cidr-controller`):
    // protection finalizer, Ready condition, and the Terminating path that
    // stops a range being deleted out from under live IPAddresses.
    let servicecidr_controller = Arc::new(ServiceCIDRController::new(storage.clone()));
    spawn_controller!("ServiceCIDR controller", leader_elector, {
        let controller = servicecidr_controller.clone();
        async move {
            if let Err(e) = controller.run().await {
                tracing::error!("ServiceCIDR controller error: {}", e);
            }
        }
    });

    // Start storage version garbage collector (upstream
    // `storage-version-garbage-collector-controller`; requires the
    // APIServerIdentity and StorageVersionAPI gates).
    if controllers::storageversiongc::gates_enabled() {
        let svgc_controller = Arc::new(StorageVersionGCController::new(storage.clone()));
        spawn_controller!("StorageVersionGC controller", leader_elector, {
            let controller = svgc_controller.clone();
            async move {
                if let Err(e) = controller.run().await {
                    tracing::error!("StorageVersionGC controller error: {}", e);
                }
            }
        });
    }

    info!("All controllers started successfully");

    // Keep the main thread alive until SIGTERM/SIGINT. As PID 1 in a container
    // a process with no SIGTERM handler ignores the signal, so register one.
    // Upstream kube-controller-manager runs with `context.Background()`
    // (cmd/kube-controller-manager/app/controllermanager.go:136) and does NOT set
    // ReleaseOnCancel on its election (only kube-scheduler does, options.go:375);
    // a SIGTERM terminates the process and the lease expires, so the elector is
    // deliberately not released here.
    rusternetes_common::signal::setup_signal_context()
        .cancelled()
        .await;
    info!("Shutting down controller manager");
    // Cancel the context Run(ctx) hands to each provisioner (controller.go:1749).
    for p in &csi_provisioner_handles {
        p.shutdown();
    }

    Ok(())
}

#[cfg(test)]
mod csi_node_deployment_flag_tests {
    use super::*;
    use std::time::Duration;

    /// external-provisioner v6.4.0 csi-provisioner.go:106-109: off by default,
    /// immediate binding on, base delay 20s, max delay 60s.
    #[test]
    fn node_deployment_flag_defaults_match_upstream() {
        let args = Args::try_parse_from(["controller-manager"]).unwrap();
        assert!(!args.csi_node_deployment);
        assert!(args.csi_node_deployment_immediate_binding);
        assert_eq!(args.csi_node_deployment_base_delay, Duration::from_secs(20));
        assert_eq!(args.csi_node_deployment_max_delay, Duration::from_secs(60));
        assert!(node_deployment_settings(&args, None).unwrap().is_none());
    }

    /// csi-provisioner.go:147-149: `--node-deployment` without NODE_NAME is fatal.
    #[test]
    fn node_deployment_requires_node_name() {
        let args = Args::try_parse_from(["controller-manager", "--csi-node-deployment"]).unwrap();
        let err = node_deployment_settings(&args, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("NODE_NAME"), "{err}");
        let err = node_deployment_settings(&args, Some(String::new()))
            .unwrap_err()
            .to_string();
        assert!(err.contains("NODE_NAME"), "{err}");
    }

    /// csi-provisioner.go:321-328: NodeName/ImmediateBinding/BaseDelay/MaxDelay
    /// come straight from the flags.
    #[test]
    fn node_deployment_settings_carry_the_flags() {
        let args = Args::try_parse_from([
            "controller-manager",
            "--csi-node-deployment",
            "--csi-node-deployment-immediate-binding=false",
            "--csi-node-deployment-base-delay=5s",
            "--csi-node-deployment-max-delay=2m",
        ])
        .unwrap();
        let nd = node_deployment_settings(&args, Some("node-1".into()))
            .unwrap()
            .unwrap();
        assert_eq!(nd.node_name, "node-1");
        assert!(!nd.immediate_binding);
        assert_eq!(nd.base_delay, Duration::from_secs(5));
        assert_eq!(nd.max_delay, Duration::from_secs(120));
        assert!(nd.accessible_topology.is_none());
    }
}

#[cfg(test)]
mod default_rate_limit_tests {
    use super::*;
    use rusternetes_client::http::ApiClient;

    /// Opt-in until the kine sig-api-machinery gate in #1863 passes: the
    /// default configuration must build UNLIMITED per-controller clients.
    #[test]
    fn default_args_build_unlimited_per_controller_clients() {
        let args = Args::try_parse_from(["controller-manager"]).unwrap();
        assert_eq!(args.kube_api_qps, 0.0, "limiter must be off by default");
        assert_eq!(args.kube_api_burst, 30.0);

        let skeleton = ApiClient::new("http://127.0.0.1:1", true, None)
            .unwrap()
            .with_rate_limit(args.kube_api_qps, args.kube_api_burst);
        let c = skeleton.for_controller("x");
        assert!(c.qps() <= 0.0, "per-controller client must be unlimited");
    }
}

#[cfg(test)]
mod feature_gates_flag_tests {
    use super::*;
    use rusternetes_common::feature_gates::{enabled, reset_to_defaults, Feature};
    use serial_test::serial;

    fn parse(gates: &str) -> Args {
        let mut v: Vec<&str> = vec!["controller-manager"];
        v.push(gates);
        Args::try_parse_from(v).unwrap()
    }

    /// Upstream `--feature-gates` (component-base featuregate `Set`,
    /// feature_gate.go:340-359) is applied process-wide from the flag.
    #[test]
    #[serial]
    fn feature_gates_flag_applies() {
        reset_to_defaults();
        apply_feature_gates(&parse("--feature-gates=StorageCapacityScoring=true")).unwrap();
        assert!(enabled(Feature::StorageCapacityScoring));
        reset_to_defaults();
    }

    /// feature_gate.go:385-389.
    #[test]
    #[serial]
    fn feature_gates_flag_rejects_unknown_gate() {
        reset_to_defaults();
        let err = apply_feature_gates(&parse("--feature-gates=fooBarBaz=true")).unwrap_err();
        assert!(
            err.to_string()
                .contains("unrecognized feature gate: fooBarBaz"),
            "{err}"
        );
    }

    /// #3190: the legacy CSI flag is an alias for the gate; the provisioner
    /// reads the gate.
    #[test]
    #[serial]
    fn csi_cross_namespace_flag_aliases_gate() {
        reset_to_defaults();
        let args = Args::try_parse_from([
            "controller-manager",
            "--csi-cross-namespace-volume-data-source",
        ])
        .unwrap();
        apply_feature_gates(&args).unwrap();
        assert!(enabled(Feature::CrossNamespaceVolumeDataSource));
        reset_to_defaults();
        apply_feature_gates(&parse(
            "--feature-gates=CrossNamespaceVolumeDataSource=true",
        ))
        .unwrap();
        assert!(enabled(Feature::CrossNamespaceVolumeDataSource));
        reset_to_defaults();
    }
}

#[cfg(test)]
mod csi_capacity_flag_tests {
    use super::*;
    use std::time::Duration;

    /// external-provisioner v6.4.0 csi-provisioner.go:88,101-104 defaults.
    #[test]
    fn capacity_flag_defaults_match_upstream() {
        let args = Args::try_parse_from(["controller-manager"]).unwrap();
        assert!(!args.csi_enable_capacity);
        assert!(!args.csi_capacity_for_immediate_binding);
        assert_eq!(args.csi_capacity_poll_interval, Duration::from_secs(60));
        assert_eq!(args.csi_capacity_ownerref_level, 1);
        assert_eq!(args.csi_capacity_threads, 1);
        // csi-provisioner.go:98-99
        assert_eq!(args.csi_kube_api_capacity_qps, 1.0);
        assert_eq!(args.csi_kube_api_capacity_burst, 5);
    }

    #[test]
    fn capacity_flags_parse() {
        let args = Args::try_parse_from([
            "controller-manager",
            "--csi-enable-capacity",
            "--csi-capacity-for-immediate-binding",
            "--csi-capacity-poll-interval=5s",
            "--csi-capacity-ownerref-level=-1",
            "--csi-capacity-threads=3",
            "--csi-kube-api-capacity-qps=-1",
            "--csi-kube-api-capacity-burst=7",
        ])
        .unwrap();
        assert_eq!(args.csi_kube_api_capacity_qps, -1.0);
        assert_eq!(args.csi_kube_api_capacity_burst, 7);
        assert!(args.csi_enable_capacity);
        assert!(args.csi_capacity_for_immediate_binding);
        assert_eq!(args.csi_capacity_poll_interval, Duration::from_secs(5));
        assert_eq!(args.csi_capacity_ownerref_level, -1);
        assert_eq!(args.csi_capacity_threads, 3);
    }
}

#[cfg(test)]
mod pv_recycler_flag_tests {
    use super::*;

    /// `persistentvolumebindercontroller.go:37-42` flag names; defaults from
    /// `config/v1alpha1/defaults.go:81-92`. Note the hostPath increment flag is
    /// `--pv-recycler-timeout-increment-hostpath` upstream.
    #[test]
    fn recycler_flag_defaults_and_names_match_upstream() {
        let args = Args::try_parse_from(["controller-manager"]).unwrap();
        assert_eq!(args.pv_recycler_minimum_timeout_nfs, 300);
        assert_eq!(args.pv_recycler_increment_timeout_nfs, 30);
        assert_eq!(args.pv_recycler_minimum_timeout_hostpath, 60);
        assert_eq!(args.pv_recycler_timeout_increment_hostpath, 30);
        assert_eq!(args.pv_recycler_pod_template_filepath_nfs, "");
        assert_eq!(args.pv_recycler_pod_template_filepath_hostpath, "");
        let args = Args::try_parse_from([
            "controller-manager",
            "--pv-recycler-minimum-timeout-nfs=1",
            "--pv-recycler-increment-timeout-nfs=2",
            "--pv-recycler-minimum-timeout-hostpath=3",
            "--pv-recycler-timeout-increment-hostpath=4",
            "--pv-recycler-pod-template-filepath-nfs=/a",
            "--pv-recycler-pod-template-filepath-hostpath=/b",
        ])
        .unwrap();
        assert_eq!(args.pv_recycler_minimum_timeout_nfs, 1);
        assert_eq!(args.pv_recycler_increment_timeout_nfs, 2);
        assert_eq!(args.pv_recycler_minimum_timeout_hostpath, 3);
        assert_eq!(args.pv_recycler_timeout_increment_hostpath, 4);
        assert_eq!(args.pv_recycler_pod_template_filepath_nfs, "/a");
        assert_eq!(args.pv_recycler_pod_template_filepath_hostpath, "/b");
    }

    /// A bad template file aborts startup with core.go:324's wrapper.
    #[test]
    fn bad_template_file_aborts_controller_start() {
        let args = Args::try_parse_from([
            "controller-manager",
            "--pv-recycler-pod-template-filepath-nfs=/nonexistent/pod.yaml",
        ])
        .unwrap();
        let err = recycler_config!(&args, controllers::pv_recycler).unwrap_err();
        assert!(err.to_string().starts_with(
            "failed to probe volume plugins when starting persistentvolume controller: failed to read file path /nonexistent/pod.yaml: "
        ));
    }
}

/// Upstream-argv fixtures (#3908 / #1882): feed the real kubeadm argv to our
/// clap definition and assert it parses with the expected effective values.
#[cfg(test)]
mod upstream_argv_tests {
    use super::*;
    use std::time::Duration;

    fn fixture_argv(text: &str) -> Vec<String> {
        text.lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(String::from)
            .collect()
    }

    #[test]
    fn kubeadm_kube_controller_manager_argv_parses() {
        let argv = fixture_argv(include_str!(
            "../tests/fixtures/kubeadm-kube-controller-manager.args"
        ));
        let args = Args::try_parse_from(&argv).expect("kubeadm argv must parse");
        assert!(args.allocate_node_cidrs);
        assert_eq!(args.cluster_cidr.as_deref(), Some("10.244.0.0/16"));
        assert_eq!(
            args.service_cluster_ip_range.as_deref(),
            Some("10.96.0.0/12")
        );
        assert_eq!(args.cluster_name, "kubernetes");
        assert_eq!(
            args.kubeconfig.as_deref(),
            Some("/etc/kubernetes/controller-manager.conf")
        );
        let le = args.leader_election_settings().unwrap();
        assert!(le.enabled);
        assert_eq!(le.lease_duration, Duration::from_secs(15));
        // The accepted-but-ignored flags are reported, never silent.
        assert_eq!(
            args.ignored_upstream_flags(),
            vec![
                "--controllers",
                "--use-service-account-credentials",
                "--root-ca-file",
                "--service-account-private-key-file",
                "--cluster-signing-cert-file",
                "--cluster-signing-key-file",
            ]
        );
    }

    // component-base leaderelectionconfig.go:26-57 names; Go durations.
    #[test]
    fn upstream_leader_elect_flags_resolve() {
        let a = Args::try_parse_from([
            "kube-controller-manager",
            "--leader-elect",
            "--leader-elect-lease-duration=30s",
            "--leader-elect-renew-deadline=20s",
            "--leader-elect-retry-period=5s",
            "--leader-elect-resource-lock=leases",
            "--leader-elect-resource-name=cm",
            "--leader-elect-resource-namespace=ns",
        ])
        .unwrap();
        let s = a.leader_election_settings().unwrap();
        assert_eq!(
            s,
            LeaderElectionSettings {
                enabled: true,
                lease_duration: Duration::from_secs(30),
                renew_deadline: Duration::from_secs(20),
                retry_period: Duration::from_secs(5),
            }
        );
        assert_eq!(a.leader_election_resource_name, "cm");
        assert_eq!(a.leader_election_resource_namespace, "ns");
    }

    #[test]
    fn native_leader_election_flags_unchanged() {
        let a = Args::try_parse_from(["cm", "--enable-leader-election"]).unwrap();
        let s = a.leader_election_settings().unwrap();
        assert!(s.enabled);
        assert_eq!(s.retry_period, Duration::from_secs(2));
        let a = Args::try_parse_from(["cm", "--enable-leader-election", "--leader-elect=false"])
            .unwrap();
        assert!(!a.leader_election_settings().unwrap().enabled);
    }

    #[test]
    fn invalid_lock_and_duration_rejected() {
        let a = Args::try_parse_from(["cm", "--leader-elect-resource-lock=configmaps"]).unwrap();
        assert!(a.leader_election_settings().is_err());
        let a = Args::try_parse_from(["cm", "--leader-elect-lease-duration=abc"]).unwrap();
        assert!(a.leader_election_settings().is_err());
    }

    #[test]
    fn allocate_node_cidrs_pflag_bool_forms() {
        assert!(!Args::try_parse_from(["cm"]).unwrap().allocate_node_cidrs);
        assert!(
            Args::try_parse_from(["cm", "--allocate-node-cidrs"])
                .unwrap()
                .allocate_node_cidrs
        );
        assert!(
            !Args::try_parse_from(["cm", "--allocate-node-cidrs=false"])
                .unwrap()
                .allocate_node_cidrs
        );
    }
}

#[cfg(test)]
mod secure_serving_flag_tests {
    use super::*;

    // cmd/kube-controller-manager/app/options/options.go:233-238
    #[test]
    fn default_secure_port_is_10257() {
        let args = Args::try_parse_from(["controller-manager"]).unwrap();
        let c = component_secure_serving(&args);
        assert_eq!(c.secure_serving.bind_port, 10257);
    }

    // `--secure-port` / `--bind-address` (apiserver SecureServingOptions.AddFlags)
    #[test]
    fn secure_port_and_bind_address_flags_apply() {
        let args = Args::try_parse_from([
            "controller-manager",
            "--secure-port=1234",
            "--bind-address=127.0.0.1",
        ])
        .unwrap();
        let c = component_secure_serving(&args);
        assert_eq!(c.secure_serving.bind_port, 1234);
        assert_eq!(c.secure_serving.bind_address.to_string(), "127.0.0.1");
    }

    // upstream has no plain-HTTP metrics port, so the flag is gone
    #[test]
    fn insecure_metrics_port_flag_is_gone() {
        assert!(Args::try_parse_from(["controller-manager", "--metrics-port=8080"]).is_err());
    }
}
