use crate::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// CloudProvider defines the interface for cloud provider integrations
#[async_trait]
pub trait CloudProvider: Send + Sync {
    /// Create a load balancer for the given service
    async fn ensure_load_balancer(
        &self,
        service: &LoadBalancerService,
    ) -> Result<LoadBalancerStatus>;

    /// Delete a load balancer for the given service
    async fn delete_load_balancer(&self, service_namespace: &str, service_name: &str)
        -> Result<()>;

    /// Get the status of a load balancer
    async fn get_load_balancer_status(
        &self,
        service_namespace: &str,
        service_name: &str,
    ) -> Result<Option<LoadBalancerStatus>>;

    /// Get the cloud provider name
    fn name(&self) -> &str;
}

/// LoadBalancerService contains the information needed to create a load balancer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerService {
    pub namespace: String,
    pub name: String,
    pub cluster_name: String,
    pub ports: Vec<LoadBalancerPort>,
    pub node_addresses: Vec<String>,
    pub session_affinity: Option<String>,
    pub annotations: std::collections::HashMap<String, String>,
}

fn default_protocol() -> String {
    "TCP".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerPort {
    pub name: Option<String>,
    #[serde(default = "default_protocol")]
    pub protocol: String,
    pub port: u16,
    pub node_port: u16,
}

/// LoadBalancerStatus represents the status of a load balancer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerStatus {
    /// External IP addresses or DNS names assigned to the load balancer
    pub ingress: Vec<LoadBalancerIngress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerIngress {
    pub ip: Option<String>,
    pub hostname: Option<String>,
}

/// LoadBalancerConfig contains configuration for load balancer provisioning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerConfig {
    pub provider: String,
    pub cluster_name: String,
    pub region: Option<String>,
    pub zone: Option<String>,
    pub tags: std::collections::HashMap<String, String>,
}

/// CloudProviderType represents the supported cloud provider types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloudProviderType {
    AWS,
    GCP,
    Azure,
    None,
}

impl CloudProviderType {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "aws" => Some(CloudProviderType::AWS),
            "gcp" | "google" => Some(CloudProviderType::GCP),
            "azure" => Some(CloudProviderType::Azure),
            "none" | "" => Some(CloudProviderType::None),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            CloudProviderType::AWS => "aws",
            CloudProviderType::GCP => "gcp",
            CloudProviderType::Azure => "azure",
            CloudProviderType::None => "none",
        }
    }
}
