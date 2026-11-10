//! Port of `staging/src/k8s.io/cloud-provider/fake/fake.go` (release-1.35):
//! `fake.Cloud`, a test double implementing `Interface`, `Instances`,
//! `InstancesV2`, `LoadBalancer`, `Routes`, `Zones`, `Clusters` and
//! `PVLabeler`. It touches no real cloud API.
//!
//! Go -> Rust mapping:
//!
//! * Public configuration fields keep their Go names in snake_case and are
//!   set with struct-literal syntax (`..Default::default()`).
//! * Go mutates `f.Err` from `CreateRoute` / `DeleteRoute` (a sticky error
//!   every later call returns). Configuration fields are plain data here, so
//!   that sticky value lives in `sticky_err` and wins over `err`, which is
//!   the same result as Go overwriting `f.Err`.
//! * `ExternalIP net.IP` is `Option<IpAddr>`; Go prints a nil `net.IP` as
//!   `"<nil>"`, preserved by [`Cloud::external_ip_string`].
//! * `Lock` / `addCallLock` / `ensureCallLock` / `updateCallLock` /
//!   `addressesMux` collapse into one `Mutex` around the mutable state.

use super::{
    default_load_balancer_name, CloudError, CloudResult, Clusters, ControllerClientBuilder,
    InstanceMetadata, Instances, InstancesV2, Interface, LoadBalancer, NodeName, PvLabeler, Route,
    Routes, Zone, Zones,
};
use async_trait::async_trait;
use rusternetes_common::resources::service::{
    LoadBalancerIngress, LoadBalancerStatus, PortStatus, ServicePort,
};
use rusternetes_common::resources::volume::PersistentVolume;
use rusternetes_common::resources::{Node, NodeAddress, Service};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// `defaultProviderName`.
const DEFAULT_PROVIDER_NAME: &str = "fake";

/// `fake.Balancer`: fake storage of balancer information.
#[derive(Debug, Clone)]
pub struct Balancer {
    pub name: String,
    pub region: String,
    pub load_balancer_ip: String,
    pub ports: Vec<ServicePort>,
    pub hosts: Vec<Node>,
}

/// `fake.UpdateBalancerCall`: a fake call to update load balancers.
#[derive(Debug, Clone)]
pub struct UpdateBalancerCall {
    pub service: Service,
    pub hosts: Vec<Node>,
}

/// `fake.Route`: a representation of an advanced routing rule.
#[derive(Debug, Clone)]
pub struct FakeRoute {
    pub cluster_name: String,
    pub route: Route,
}

/// `OverrideInstanceMetadata`.
pub type OverrideInstanceMetadata =
    Arc<dyn Fn(&Node) -> CloudResult<InstanceMetadata> + Send + Sync>;

#[derive(Default)]
#[doc(hidden)]
pub struct State {
    calls: Vec<String>,
    addresses: Vec<NodeAddress>,
    balancers: HashMap<String, Balancer>,
    update_calls: Vec<UpdateBalancerCall>,
    ensure_calls: Vec<UpdateBalancerCall>,
    route_map: HashMap<String, FakeRoute>,
    sticky_err: Option<CloudError>,
}

/// `fake.Cloud`: a test-double implementation of Interface, LoadBalancer,
/// Instances, and Routes.
#[derive(Default)]
pub struct Cloud {
    pub disable_instances: bool,
    pub disable_routes: bool,
    pub disable_load_balancers: bool,
    pub disable_zones: bool,
    pub disable_clusters: bool,

    pub exists: bool,
    pub err: Option<CloudError>,

    pub enable_instances_v2: bool,
    pub exists_by_provider_id: bool,
    pub err_by_provider_id: Option<CloudError>,
    pub node_shutdown: bool,
    pub err_shutdown_by_provider_id: Option<CloudError>,
    pub metadata_err: Option<CloudError>,

    /// Initial value of `Addresses`; later changed with
    /// [`Cloud::set_node_addresses`].
    pub addresses: Vec<NodeAddress>,
    pub ext_id: HashMap<NodeName, String>,
    pub ext_id_err: HashMap<NodeName, CloudError>,
    pub instance_types: HashMap<NodeName, String>,
    pub machines: Vec<NodeName>,
    pub node_resources: HashMap<String, String>,
    pub cluster_list: Vec<String>,
    pub master_name: String,
    pub external_ip: Option<IpAddr>,
    pub balancer_ip_mode: Option<String>,
    pub provider: String,
    pub provider_id: HashMap<NodeName, String>,
    /// The embedded `cloudprovider.Zone`.
    pub zone: Zone,
    pub volume_label_map: HashMap<String, HashMap<String, String>>,
    pub additional_labels: HashMap<String, String>,
    pub override_instance_metadata: Option<OverrideInstanceMetadata>,
    pub request_delay: Duration,

    #[doc(hidden)]
    pub state: Mutex<State>,
    #[doc(hidden)]
    pub seeded: Mutex<bool>,
}

impl Cloud {
    /// Copy the configured `addresses` into the mutable state on first use
    /// (Go's `Addresses` is read and written through `addressesMux`).
    fn seed(&self) {
        let mut seeded = self.seeded.lock().unwrap();
        if !*seeded {
            *seeded = true;
            self.state.lock().unwrap().addresses = self.addresses.clone();
        }
    }

    /// The error `f.Err` currently holds.
    fn err(&self) -> Option<CloudError> {
        self.state
            .lock()
            .unwrap()
            .sticky_err
            .clone()
            .or_else(|| self.err.clone())
    }

    fn result_err<T>(&self, v: T) -> CloudResult<T> {
        match self.err() {
            Some(e) => Err(e),
            None => Ok(v),
        }
    }

    /// `addCall`.
    async fn add_call(&self, desc: &str) {
        if !self.request_delay.is_zero() {
            tokio::time::sleep(self.request_delay).await;
        }
        self.state.lock().unwrap().calls.push(desc.to_string());
    }

    /// `Calls`: the recorded method calls.
    pub fn calls(&self) -> Vec<String> {
        self.state.lock().unwrap().calls.clone()
    }

    /// `ClearCalls`.
    pub fn clear_calls(&self) {
        self.state.lock().unwrap().calls.clear();
    }

    /// `SetNodeAddresses`.
    pub fn set_node_addresses(&self, node_addresses: Vec<NodeAddress>) {
        self.seed();
        self.state.lock().unwrap().addresses = node_addresses;
    }

    /// `UpdateCalls`.
    pub fn update_calls(&self) -> Vec<UpdateBalancerCall> {
        self.state.lock().unwrap().update_calls.clone()
    }

    /// `EnsureCalls`.
    pub fn ensure_calls(&self) -> Vec<UpdateBalancerCall> {
        self.state.lock().unwrap().ensure_calls.clone()
    }

    /// `Balancers`.
    pub fn balancers(&self) -> HashMap<String, Balancer> {
        self.state.lock().unwrap().balancers.clone()
    }

    /// `RouteMap`.
    pub fn route_map(&self) -> HashMap<String, FakeRoute> {
        self.state.lock().unwrap().route_map.clone()
    }

    /// `f.ExternalIP.String()`: a nil `net.IP` prints as `<nil>`.
    fn external_ip_string(&self) -> String {
        self.external_ip
            .map(|i| i.to_string())
            .unwrap_or_else(|| "<nil>".to_string())
    }
}

#[async_trait]
impl Interface for Cloud {
    fn initialize(
        &self,
        _client_builder: &dyn ControllerClientBuilder,
        _stop: tokio::sync::watch::Receiver<bool>,
    ) {
    }

    fn load_balancer(&self) -> Option<&dyn LoadBalancer> {
        (!self.disable_load_balancers).then_some(self as &dyn LoadBalancer)
    }

    fn instances(&self) -> Option<&dyn Instances> {
        (!self.disable_instances).then_some(self as &dyn Instances)
    }

    fn instances_v2(&self) -> Option<&dyn InstancesV2> {
        self.enable_instances_v2.then_some(self as &dyn InstancesV2)
    }

    fn zones(&self) -> Option<&dyn Zones> {
        (!self.disable_zones).then_some(self as &dyn Zones)
    }

    fn clusters(&self) -> Option<&dyn Clusters> {
        (!self.disable_clusters).then_some(self as &dyn Clusters)
    }

    fn routes(&self) -> Option<&dyn Routes> {
        (!self.disable_routes).then_some(self as &dyn Routes)
    }

    fn provider_name(&self) -> String {
        if self.provider.is_empty() {
            DEFAULT_PROVIDER_NAME.to_string()
        } else {
            self.provider.clone()
        }
    }

    fn has_cluster_id(&self) -> bool {
        true
    }
}

#[async_trait]
impl Clusters for Cloud {
    async fn list_clusters(&self) -> CloudResult<Vec<String>> {
        self.result_err(self.cluster_list.clone())
    }

    async fn master(&self, _name: &str) -> CloudResult<String> {
        self.result_err(self.master_name.clone())
    }
}

#[async_trait]
impl LoadBalancer for Cloud {
    async fn get_load_balancer(
        &self,
        _cluster_name: &str,
        _service: &Service,
    ) -> CloudResult<(Option<LoadBalancerStatus>, bool)> {
        let status = LoadBalancerStatus {
            ingress: vec![LoadBalancerIngress {
                ip: Some(self.external_ip_string()),
                hostname: None,
                ip_mode: None,
                ports: None,
            }],
        };
        match self.err() {
            Some(e) => Err(e),
            None => Ok((Some(status), self.exists)),
        }
    }

    async fn get_load_balancer_name(&self, _cluster_name: &str, service: &Service) -> String {
        // TODO upstream: replace DefaultLoadBalancerName to generate more
        // meaningful loadbalancer names.
        default_load_balancer_name(service)
    }

    async fn ensure_load_balancer(
        &self,
        cluster_name: &str,
        service: &Service,
        nodes: &[Node],
    ) -> CloudResult<LoadBalancerStatus> {
        self.add_call("create").await;
        self.state
            .lock()
            .unwrap()
            .ensure_calls
            .push(UpdateBalancerCall {
                service: service.clone(),
                hosts: nodes.to_vec(),
            });

        let name = self.get_load_balancer_name(cluster_name, service).await;
        let spec = &service.spec;

        let zone = self.get_zone().await?;
        let region = zone.region;

        self.state.lock().unwrap().balancers.insert(
            name.clone(),
            Balancer {
                name,
                region,
                load_balancer_ip: spec.load_balancer_ip.clone().unwrap_or_default(),
                ports: spec.ports.clone(),
                hosts: nodes.to_vec(),
            },
        );

        // process Ports
        let port_status: Vec<PortStatus> = spec
            .ports
            .iter()
            .map(|p| PortStatus {
                port: i32::from(p.port),
                protocol: p.protocol.clone(),
                error: None,
            })
            .collect();
        let status = LoadBalancerStatus {
            ingress: vec![LoadBalancerIngress {
                ip: Some(self.external_ip_string()),
                hostname: None,
                ip_mode: self.balancer_ip_mode.clone(),
                ports: Some(port_status),
            }],
        };
        self.result_err(status)
    }

    async fn update_load_balancer(
        &self,
        _cluster_name: &str,
        service: &Service,
        nodes: &[Node],
    ) -> CloudResult<()> {
        self.add_call("update").await;
        self.state
            .lock()
            .unwrap()
            .update_calls
            .push(UpdateBalancerCall {
                service: service.clone(),
                hosts: nodes.to_vec(),
            });
        self.result_err(())
    }

    async fn ensure_load_balancer_deleted(
        &self,
        _cluster_name: &str,
        _service: &Service,
    ) -> CloudResult<()> {
        self.add_call("delete").await;
        self.result_err(())
    }
}

#[async_trait]
impl Instances for Cloud {
    async fn node_addresses(&self, _instance: &str) -> CloudResult<Vec<NodeAddress>> {
        self.add_call("node-addresses").await;
        self.seed();
        let a = self.state.lock().unwrap().addresses.clone();
        self.result_err(a)
    }

    async fn node_addresses_by_provider_id(
        &self,
        _provider_id: &str,
    ) -> CloudResult<Vec<NodeAddress>> {
        self.add_call("node-addresses-by-provider-id").await;
        self.seed();
        let a = self.state.lock().unwrap().addresses.clone();
        self.result_err(a)
    }

    /// Returns the cloud provider ID of the node with the specified Name,
    /// unless an entry for the node exists in `ext_id_err`, in which case it
    /// returns the desired error (to facilitate testing of error handling).
    async fn instance_id(&self, node_name: &str) -> CloudResult<String> {
        self.add_call("instance-id").await;
        if let Some(e) = self.ext_id_err.get(node_name) {
            return Err(e.clone());
        }
        Ok(self.ext_id.get(node_name).cloned().unwrap_or_default())
    }

    async fn instance_type(&self, instance: &str) -> CloudResult<String> {
        self.add_call("instance-type").await;
        Ok(self
            .instance_types
            .get(instance)
            .cloned()
            .unwrap_or_default())
    }

    async fn instance_type_by_provider_id(&self, provider_id: &str) -> CloudResult<String> {
        self.add_call("instance-type-by-provider-id").await;
        Ok(self
            .instance_types
            .get(provider_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn add_ssh_key_to_all_instances(&self, _user: &str, _key_data: &[u8]) -> CloudResult<()> {
        Err(CloudError::NotImplemented)
    }

    async fn current_node_name(&self, hostname: &str) -> CloudResult<NodeName> {
        Ok(hostname.to_string())
    }

    async fn instance_exists_by_provider_id(&self, _provider_id: &str) -> CloudResult<bool> {
        self.add_call("instance-exists-by-provider-id").await;
        match &self.err_by_provider_id {
            Some(e) => Err(e.clone()),
            None => Ok(self.exists_by_provider_id),
        }
    }

    async fn instance_shutdown_by_provider_id(&self, provider_id: &str) -> CloudResult<bool> {
        self.add_call("instance-shutdown-by-provider-id").await;
        if provider_id.is_empty() {
            return Err(CloudError::other(
                "cannot shutdown instance with empty providerID",
            ));
        }
        match &self.err_shutdown_by_provider_id {
            Some(e) => Err(e.clone()),
            None => Ok(self.node_shutdown),
        }
    }
}

#[async_trait]
impl InstancesV2 for Cloud {
    async fn instance_exists(&self, _node: &Node) -> CloudResult<bool> {
        self.add_call("instance-exists").await;
        match &self.err_by_provider_id {
            Some(e) => Err(e.clone()),
            None => Ok(self.exists_by_provider_id),
        }
    }

    async fn instance_shutdown(&self, _node: &Node) -> CloudResult<bool> {
        self.add_call("instance-shutdown").await;
        match &self.err_shutdown_by_provider_id {
            Some(e) => Err(e.clone()),
            None => Ok(self.node_shutdown),
        }
    }

    async fn instance_metadata(&self, node: &Node) -> CloudResult<InstanceMetadata> {
        if let Some(f) = &self.override_instance_metadata {
            return f(node);
        }
        self.add_call("instance-metadata-by-provider-id").await;
        self.seed();
        let addresses = self.state.lock().unwrap().addresses.clone();

        let provider_id = self
            .provider_id
            .get(&node.metadata.name)
            .cloned()
            .unwrap_or_default();
        let spec_provider_id = node
            .spec
            .as_ref()
            .and_then(|s| s.provider_id.clone())
            .unwrap_or_default();

        let md = InstanceMetadata {
            provider_id,
            instance_type: self
                .instance_types
                .get(&spec_provider_id)
                .cloned()
                .unwrap_or_default(),
            node_addresses: addresses,
            zone: self.zone.failure_domain.clone(),
            region: self.zone.region.clone(),
            additional_labels: self.additional_labels.clone(),
        };
        match &self.metadata_err {
            Some(e) => Err(e.clone()),
            None => Ok(md),
        }
    }
}

impl Cloud {
    /// `List` (test-spy `Instances.List`): machines whose name matches
    /// `filter`. Go uses `regexp.MatchString`; no regex crate is a dependency
    /// here, so only a literal substring filter is supported (an empty filter
    /// matches everything, as in Go).
    pub async fn list(&self, filter: &str) -> CloudResult<Vec<NodeName>> {
        self.add_call("list").await;
        let result: Vec<NodeName> = self
            .machines
            .iter()
            .filter(|m| m.contains(filter))
            .cloned()
            .collect();
        self.result_err(result)
    }
}

#[async_trait]
impl Zones for Cloud {
    async fn get_zone(&self) -> CloudResult<Zone> {
        self.add_call("get-zone").await;
        self.result_err(self.zone.clone())
    }

    async fn get_zone_by_provider_id(&self, _provider_id: &str) -> CloudResult<Zone> {
        self.add_call("get-zone-by-provider-id").await;
        self.result_err(self.zone.clone())
    }

    async fn get_zone_by_node_name(&self, _node_name: &str) -> CloudResult<Zone> {
        self.add_call("get-zone-by-node-name").await;
        self.result_err(self.zone.clone())
    }
}

#[async_trait]
impl Routes for Cloud {
    async fn list_routes(&self, cluster_name: &str) -> CloudResult<Vec<Route>> {
        self.add_call("list-routes").await;
        let routes: Vec<Route> = self
            .state
            .lock()
            .unwrap()
            .route_map
            .values()
            .filter(|r| r.cluster_name == cluster_name)
            .map(|r| r.route.clone())
            .collect();
        self.result_err(routes)
    }

    async fn create_route(
        &self,
        cluster_name: &str,
        _name_hint: &str,
        route: &Route,
    ) -> CloudResult<()> {
        self.add_call("create-route").await;
        let name = format!(
            "{cluster_name}-{}-{}",
            route.target_node, route.destination_cidr
        );
        let mut st = self.state.lock().unwrap();
        if st.route_map.contains_key(&name) {
            let e = CloudError::other(format!("route {name:?} already exists"));
            st.sticky_err = Some(e.clone());
            return Err(e);
        }
        let mut r = route.clone();
        r.name = name.clone();
        st.route_map.insert(
            name,
            FakeRoute {
                cluster_name: cluster_name.to_string(),
                route: r,
            },
        );
        Ok(())
    }

    async fn delete_route(&self, cluster_name: &str, route: &Route) -> CloudResult<()> {
        self.add_call("delete-route").await;
        let mut st = self.state.lock().unwrap();
        let name = st
            .route_map
            .iter()
            .find(|(_, saved)| {
                route.destination_cidr == saved.route.destination_cidr
                    && route.target_node == saved.route.target_node
                    && cluster_name == saved.cluster_name
            })
            .map(|(k, _)| k.clone());
        match name {
            None => {
                let e = CloudError::other(format!(
                    "no route found for node:{} with DestinationCIDR== {}",
                    route.target_node, route.destination_cidr
                ));
                st.sticky_err = Some(e.clone());
                Err(e)
            }
            Some(name) => {
                st.route_map.remove(&name);
                Ok(())
            }
        }
    }
}

#[async_trait]
impl PvLabeler for Cloud {
    async fn get_labels_for_volume(
        &self,
        pv: &PersistentVolume,
    ) -> CloudResult<HashMap<String, String>> {
        match self.volume_label_map.get(&pv.metadata.name) {
            Some(v) => Ok(v.clone()),
            None => Err(CloudError::other("label not found for volume")),
        }
    }
}
