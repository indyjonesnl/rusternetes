//! Port of the cloud-provider `Interface`
//! (`staging/src/k8s.io/cloud-provider/cloud.go`, release-1.35): the pluggable
//! contract a cloud implements so the cloud controllers
//! (`controllers/nodelifecycle`, ...) can talk to it.
//!
//! This is distinct from the bespoke `rusternetes_common::cloud_provider::
//! CloudProvider` trait that `LoadBalancerController` uses; that trait has no
//! upstream equivalent. This module is the upstream one, kept 1:1.
//!
//! Go -> Rust mapping:
//!
//! * `(Iface, bool)` accessors (`Instances()`, `LoadBalancer()`, ...) become
//!   `Option<&dyn Iface>`; `None` is upstream's `false`.
//! * The sentinel errors (`cloud.go:258-263`) are [`CloudError`] variants and
//!   compare by identity, like Go's `err == cloudprovider.InstanceNotFound`.
//! * `types.NodeName` is [`NodeName`] (a `String`).
//! * `ControllerClientBuilder` (`cloud.go:35`) and `InformerUser`
//!   (`cloud.go:71`) are not ported: they carry a `client-go` clientset /
//!   `SharedInformerFactory` that this repo does not have (controllers here
//!   read and write `Storage` directly). `Initialize` takes a placeholder
//!   [`ControllerClientBuilder`] so the method set is unchanged.
//!   Tracked as deferred work in the PR.

pub mod api;
pub mod fake;
pub mod node_helpers;

use async_trait::async_trait;
use rusternetes_common::resources::service::LoadBalancerStatus;
use rusternetes_common::resources::volume::PersistentVolume;
use rusternetes_common::resources::{Node, NodeAddress, Service};
use std::collections::HashMap;
use std::fmt;

/// `types.NodeName`.
pub type NodeName = String;

/// The sentinel errors of `cloud.go:258-263` plus Go's ad-hoc
/// `errors.New` / `fmt.Errorf` values, which are only ever compared by
/// message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloudError {
    /// `DiskNotFound = errors.New("disk is not found")`.
    DiskNotFound,
    /// `ImplementedElsewhere = errors.New("implemented by alternate to cloud provider")`.
    ImplementedElsewhere,
    /// `InstanceNotFound = errors.New("instance not found")`.
    InstanceNotFound,
    /// `NotImplemented = errors.New("unimplemented")`.
    NotImplemented,
    /// Any other error (`errors.New(..)` / `fmt.Errorf(..)`).
    Other(String),
}

impl CloudError {
    pub fn other(msg: impl Into<String>) -> Self {
        CloudError::Other(msg.into())
    }
}

impl fmt::Display for CloudError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CloudError::DiskNotFound => f.write_str("disk is not found"),
            CloudError::ImplementedElsewhere => {
                f.write_str("implemented by alternate to cloud provider")
            }
            CloudError::InstanceNotFound => f.write_str("instance not found"),
            CloudError::NotImplemented => f.write_str("unimplemented"),
            CloudError::Other(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for CloudError {}

pub type CloudResult<T> = Result<T, CloudError>;

/// Placeholder for `cloudprovider.ControllerClientBuilder` (`cloud.go:35`);
/// see the module docs.
pub trait ControllerClientBuilder: Send + Sync {}

/// `cloudprovider.Interface` (`cloud.go:43-69`): an abstract, pluggable
/// interface for cloud providers.
#[async_trait]
pub trait Interface: Send + Sync {
    /// `Initialize`: "provides the cloud with a kubernetes client builder and
    /// may spawn goroutines to perform housekeeping or run custom controllers
    /// specific to the cloud provider. Any tasks started here should be
    /// cleaned up when the stop channel closes." The stop channel is a
    /// `watch::Receiver<bool>` that flips to `true` on stop.
    fn initialize(
        &self,
        client_builder: &dyn ControllerClientBuilder,
        stop: tokio::sync::watch::Receiver<bool>,
    );
    /// `LoadBalancer`.
    fn load_balancer(&self) -> Option<&dyn LoadBalancer>;
    /// `Instances`.
    fn instances(&self) -> Option<&dyn Instances>;
    /// `InstancesV2`: "behaviorally identical to Instances but is optimized to
    /// significantly reduce API calls ... Implementation of this interface
    /// will disable calls to the Zones interface."
    fn instances_v2(&self) -> Option<&dyn InstancesV2>;
    /// `Zones` (deprecated in favour of `InstancesV2`).
    fn zones(&self) -> Option<&dyn Zones>;
    /// `Clusters`.
    fn clusters(&self) -> Option<&dyn Clusters>;
    /// `Routes`.
    fn routes(&self) -> Option<&dyn Routes>;
    /// `ProviderName`: the cloud provider ID.
    fn provider_name(&self) -> String;
    /// `HasClusterID`: true if a ClusterID is required and set.
    fn has_cluster_id(&self) -> bool;
}

/// `cloudprovider.Clusters` (`cloud.go:77-82`).
#[async_trait]
pub trait Clusters: Send + Sync {
    async fn list_clusters(&self) -> CloudResult<Vec<String>>;
    async fn master(&self, cluster_name: &str) -> CloudResult<String>;
}

/// `DefaultLoadBalancerName` (`cloud.go:89-98`).
pub fn default_load_balancer_name(service: &Service) -> String {
    // GCE requires that the name of a load balancer starts with a lower case letter.
    let mut ret = format!("a{}", service.metadata.uid).replace('-', "");
    // AWS requires that the name of a load balancer is shorter than 32 bytes.
    if ret.len() > 32 {
        ret.truncate(32);
    }
    ret
}

/// `GetInstanceProviderID` (`cloud.go:103-120`): builds a ProviderID for a
/// node in a cloud. If the instance does not exist, returns
/// `Err(InstanceNotFound)`.
pub async fn get_instance_provider_id(
    cloud: &dyn Interface,
    node_name: &str,
) -> CloudResult<String> {
    let Some(instances) = cloud.instances() else {
        return Err(CloudError::other(
            "failed to get instances from cloud provider",
        ));
    };
    match instances.instance_id(node_name).await {
        Ok(instance_id) => Ok(format!("{}://{}", cloud.provider_name(), instance_id)),
        Err(e @ CloudError::NotImplemented) | Err(e @ CloudError::InstanceNotFound) => Err(e),
        Err(e) => Err(CloudError::other(format!(
            "failed to get instance ID from cloud provider: {e}"
        ))),
    }
}

/// `cloudprovider.LoadBalancer` (`cloud.go:138-173`).
#[async_trait]
pub trait LoadBalancer: Send + Sync {
    /// `GetLoadBalancer`: `(status, exists)`.
    async fn get_load_balancer(
        &self,
        cluster_name: &str,
        service: &Service,
    ) -> CloudResult<(Option<LoadBalancerStatus>, bool)>;
    async fn get_load_balancer_name(&self, cluster_name: &str, service: &Service) -> String;
    async fn ensure_load_balancer(
        &self,
        cluster_name: &str,
        service: &Service,
        nodes: &[Node],
    ) -> CloudResult<LoadBalancerStatus>;
    async fn update_load_balancer(
        &self,
        cluster_name: &str,
        service: &Service,
        nodes: &[Node],
    ) -> CloudResult<()>;
    async fn ensure_load_balancer_deleted(
        &self,
        cluster_name: &str,
        service: &Service,
    ) -> CloudResult<()>;
}

/// `cloudprovider.Instances` (`cloud.go:176-205`).
#[async_trait]
pub trait Instances: Send + Sync {
    async fn node_addresses(&self, name: &str) -> CloudResult<Vec<NodeAddress>>;
    async fn node_addresses_by_provider_id(
        &self,
        provider_id: &str,
    ) -> CloudResult<Vec<NodeAddress>>;
    /// "Note that if the instance does not exist, we must return
    /// ("", cloudprovider.InstanceNotFound)".
    async fn instance_id(&self, node_name: &str) -> CloudResult<String>;
    async fn instance_type(&self, name: &str) -> CloudResult<String>;
    async fn instance_type_by_provider_id(&self, provider_id: &str) -> CloudResult<String>;
    async fn add_ssh_key_to_all_instances(&self, user: &str, key_data: &[u8]) -> CloudResult<()>;
    async fn current_node_name(&self, hostname: &str) -> CloudResult<NodeName>;
    /// "If false is returned with no error, the instance will be immediately
    /// deleted by the cloud controller manager."
    async fn instance_exists_by_provider_id(&self, provider_id: &str) -> CloudResult<bool>;
    async fn instance_shutdown_by_provider_id(&self, provider_id: &str) -> CloudResult<bool>;
}

/// `cloudprovider.InstancesV2` (`cloud.go:210-223`).
#[async_trait]
pub trait InstancesV2: Send + Sync {
    async fn instance_exists(&self, node: &Node) -> CloudResult<bool>;
    async fn instance_shutdown(&self, node: &Node) -> CloudResult<bool>;
    async fn instance_metadata(&self, node: &Node) -> CloudResult<InstanceMetadata>;
}

/// `cloudprovider.Route` (`cloud.go:226-243`).
#[derive(Debug, Clone, Default)]
pub struct Route {
    pub name: String,
    pub target_node: NodeName,
    pub enable_node_addresses: bool,
    pub target_node_addresses: Vec<NodeAddress>,
    pub destination_cidr: String,
    pub blackhole: bool,
}

/// `cloudprovider.Routes` (`cloud.go:246-256`).
#[async_trait]
pub trait Routes: Send + Sync {
    async fn list_routes(&self, cluster_name: &str) -> CloudResult<Vec<Route>>;
    async fn create_route(
        &self,
        cluster_name: &str,
        name_hint: &str,
        route: &Route,
    ) -> CloudResult<()>;
    async fn delete_route(&self, cluster_name: &str, route: &Route) -> CloudResult<()>;
}

/// `cloudprovider.Zone` (`cloud.go:266-269`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Zone {
    pub failure_domain: String,
    pub region: String,
}

/// `cloudprovider.Zones` (`cloud.go:274-290`).
#[async_trait]
pub trait Zones: Send + Sync {
    async fn get_zone(&self) -> CloudResult<Zone>;
    async fn get_zone_by_provider_id(&self, provider_id: &str) -> CloudResult<Zone>;
    async fn get_zone_by_node_name(&self, node_name: &str) -> CloudResult<Zone>;
}

/// `cloudprovider.PVLabeler` (`cloud.go:294-296`).
#[async_trait]
pub trait PvLabeler: Send + Sync {
    async fn get_labels_for_volume(
        &self,
        pv: &PersistentVolume,
    ) -> CloudResult<HashMap<String, String>>;
}

/// `cloudprovider.InstanceMetadata` (`cloud.go:300-335`).
#[derive(Debug, Clone, Default)]
pub struct InstanceMetadata {
    pub provider_id: String,
    pub instance_type: String,
    pub node_addresses: Vec<NodeAddress>,
    pub zone: String,
    pub region: String,
    pub additional_labels: HashMap<String, String>,
}
