use crate::resources::policy::IntOrString;
use crate::resources::serde_helpers::empty_string_as_none;
use crate::types::{Condition, ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Service is an abstraction for exposing applications running on a set of Pods
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Service {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: ServiceSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ServiceStatus>,
}

impl Service {
    pub fn new(name: impl Into<String>, spec: ServiceSpec) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec,
            status: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceSpec {
    #[serde(default)]
    pub selector: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub ports: Vec<ServicePort>,

    #[serde(
        default,
        rename = "type",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub service_type: Option<ServiceType>,

    #[serde(skip_serializing_if = "Option::is_none", rename = "clusterIP")]
    pub cluster_ip: Option<String>,

    #[serde(rename = "externalIPs", skip_serializing_if = "Option::is_none")]
    pub external_ips: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_affinity: Option<String>, // ClientIP or None

    /// ExternalName is the external reference that kubedns or equivalent will return as a CNAME record for this service.
    /// Required for ExternalName type services. No proxying will be involved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_name: Option<String>,

    /// ClusterIPs is a list of IP addresses assigned to this service, and is usually assigned randomly.
    /// If specified, must be valid IPs. Used for dual-stack support.
    #[serde(rename = "clusterIPs", skip_serializing_if = "Option::is_none")]
    pub cluster_ips: Option<Vec<String>>,

    /// IPFamilies is a list of IP families (IPv4, IPv6) assigned to this service.
    /// Dual-stack services use both IPv4 and IPv6.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_families: Option<Vec<IPFamily>>,

    /// IPFamilyPolicy represents the dual-stack-ness requested or required by this Service.
    /// Can be SingleStack, PreferDualStack, or RequireDualStack.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_family_policy: Option<IPFamilyPolicy>,

    /// InternalTrafficPolicy specifies if the cluster internal traffic should be routed to all endpoints
    /// or node-local endpoints only. "Cluster" routes to all endpoints. "Local" routes to node-local endpoints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_traffic_policy: Option<ServiceInternalTrafficPolicy>,

    /// ExternalTrafficPolicy denotes if this Service desires to route external traffic to node-local or
    /// cluster-wide endpoints. "Local" preserves the client source IP and avoids a second hop for LoadBalancer
    /// and Nodeport type services, but risks potentially imbalanced traffic spreading.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub external_traffic_policy: Option<ServiceExternalTrafficPolicy>,

    /// HealthCheckNodePort specifies the healthcheck nodePort for the service (when type=LoadBalancer and externalTrafficPolicy=Local)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_check_node_port: Option<i32>,

    /// LoadBalancerClass is the class of the load balancer implementation this Service belongs to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balancer_class: Option<String>,

    /// LoadBalancerIP is the IP to use when creating a load balancer (deprecated, use loadBalancerClass instead)
    #[serde(rename = "loadBalancerIP", skip_serializing_if = "Option::is_none")]
    pub load_balancer_ip: Option<String>,

    /// LoadBalancerSourceRanges restricts traffic through the cloud-provider load-balancer to these CIDRs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balancer_source_ranges: Option<Vec<String>>,

    /// AllocateLoadBalancerNodePorts defines if NodePorts will be allocated for Services with type LoadBalancer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocate_load_balancer_node_ports: Option<bool>,

    /// PublishNotReadyAddresses indicates that endpoints for this Service should be published even when not ready
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_not_ready_addresses: Option<bool>,

    /// SessionAffinityConfig contains the configurations of session affinity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_affinity_config: Option<SessionAffinityConfig>,

    /// TrafficDistribution offers a way to express preferences for how traffic is distributed to Service endpoints
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traffic_distribution: Option<String>,
}

fn default_protocol() -> String {
    "TCP".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServicePort {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(default)]
    pub port: u16,

    /// Number or name of the port to access on the pods targeted by the service.
    /// Can be a port number (e.g., 8080) or a named port (e.g., "http").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_port: Option<IntOrString>,

    #[serde(default = "default_protocol")]
    pub protocol: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_port: Option<u16>,

    /// Application protocol for the port (e.g., "http", "https")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_protocol: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ServiceType {
    ClusterIP,
    NodePort,
    LoadBalancer,
    ExternalName,
    /// A value outside the supported set. Upstream's field is a plain Go
    /// `string` (staging/src/k8s.io/api/core/v1/types.go), so any string
    /// decodes and validation answers `Unsupported value` (422), not a 400
    /// decode failure (#2469).
    #[serde(untagged)]
    Unknown(String),
}

/// ServiceStatus represents the current status of a service
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceStatus {
    #[serde(skip_serializing_if = "Option::is_none", rename = "loadBalancer")]
    pub load_balancer: Option<LoadBalancerStatus>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}

/// LoadBalancerStatus represents the status of a load balancer
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadBalancerStatus {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub ingress: Vec<LoadBalancerIngress>,
}

/// LoadBalancerIngress represents the status of a load balancer ingress point
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadBalancerIngress {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    /// IPMode specifies how the load-balancer IP behaves (VIP or Proxy)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_mode: Option<String>,
    /// Ports is a list of records of service ports
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<PortStatus>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortStatus {
    #[serde(default)]
    pub port: i32,
    #[serde(default = "default_protocol")]
    pub protocol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// SessionAffinityConfig contains the configurations of session affinity
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionAffinityConfig {
    #[serde(rename = "clientIP", skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<ClientIPConfig>,
}

/// ClientIPConfig represents the configurations of Client IP based session affinity
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientIPConfig {
    /// TimeoutSeconds specifies the seconds of ClientIP type session sticky time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,
}

/// IPFamily represents the IP family (IPv4 or IPv6)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IPFamily {
    IPv4,
    IPv6,
    /// A value outside the supported set. Upstream's field is a plain Go
    /// `string` (staging/src/k8s.io/api/core/v1/types.go), so any string
    /// decodes and validation answers `Unsupported value` (422), not a 400
    /// decode failure (#2469).
    #[serde(untagged)]
    Unknown(String),
}

/// IPFamilyPolicy represents the dual-stack-ness requested or required by a Service
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IPFamilyPolicy {
    /// SingleStack indicates that this service is required to have a single IPFamily.
    /// The IPFamily assigned is based on the default IPFamily used by the cluster
    /// or as identified by service.spec.ipFamilies field.
    SingleStack,
    /// PreferDualStack indicates that this service prefers dual-stack when the cluster is configured for dual-stack.
    /// If the cluster is not configured for dual-stack the service will be assigned a single IPFamily.
    PreferDualStack,
    /// RequireDualStack indicates that this service requires dual-stack.
    /// The service will fail if the cluster is not configured for dual-stack.
    RequireDualStack,
    /// A value outside the supported set, including a non-nil `""`. Upstream
    /// keeps it and validation answers `NotSupported` (validation.go:9011-9015),
    /// so decode must not 400 it.
    #[serde(untagged)]
    Unknown(String),
}

/// ServiceInternalTrafficPolicy describes how nodes distribute service traffic they
/// receive on the ClusterIP. If set to "Local", the proxy will assume that pods only
/// want to talk to endpoints of the service on the same node as the pod, dropping the
/// traffic if there are no local endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ServiceInternalTrafficPolicy {
    /// Cluster routes traffic to all endpoints
    Cluster,
    /// Local routes traffic only to node-local endpoints, dropping the traffic if no endpoints exist on the node
    Local,
    /// A value outside the supported set, including a non-nil `""`
    /// (validation.go:6886-6888 answers `NotSupported`).
    #[serde(untagged)]
    Unknown(String),
}

/// ServiceExternalTrafficPolicy describes how nodes distribute service traffic they
/// receive on one of the Service's "externally-facing" addresses (NodePorts, ExternalIPs,
/// and LoadBalancer IPs). If set to "Local", the proxy will assume that pods only want
/// to talk to endpoints of the service on the same node as the pod, dropping the traffic
/// if there are no local endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ServiceExternalTrafficPolicy {
    /// Cluster routes traffic to all endpoints
    Cluster,
    /// Local routes traffic only to node-local endpoints, preserving client source IP and avoiding second hop
    Local,
    /// A value outside the supported set. Upstream's field is a plain Go
    /// `string` (staging/src/k8s.io/api/core/v1/types.go), so any string
    /// decodes and validation answers `Unsupported value` (422), not a 400
    /// decode failure (#2469).
    #[serde(untagged)]
    Unknown(String),
}
