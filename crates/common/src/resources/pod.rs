use crate::resources::policy::IntOrString;
use crate::types::{ObjectMeta, Phase, ResourceRequirements, TypeMeta};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

/// Deserialize an Option<ContainerState> that tolerates empty objects `{}`.
/// Go's Kubernetes client serializes nil ContainerState as `{}` instead of `null`,
/// which would fail to parse as the tagged enum. This treats `{}` as `None`.
fn deserialize_container_state_option<'de, D>(
    deserializer: D,
) -> Result<Option<ContainerState>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Object(ref map)) if map.is_empty() => Ok(None),
        Some(v) => serde_json::from_value(v)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

/// Macro to create a skip_serializing_if function for Option<T> where T has all optional fields.
/// This prevents serializing empty structs as {} when all fields are None.
macro_rules! skip_if_empty {
    ($fn_name:ident, $type:ty, $($field:ident),+) => {
        #[allow(dead_code)]
        fn $fn_name(value: &Option<$type>) -> bool {
            match value {
                None => true,
                Some(v) => {
                    $(v.$field.is_none())&&+
                }
            }
        }
    };
}

/// Pod is the smallest deployable unit in Kubernetes
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pod {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    /// ObjectMeta is `+optional` upstream; a body that omits `metadata`
    /// (e.g. some PreStop e2e payloads) must still decode and let validation
    /// report the missing name, rather than failing with a raw serde
    /// `missing field 'metadata'`.
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<PodSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<PodStatus>,
}

impl Pod {
    pub fn new(name: impl Into<String>, spec: PodSpec) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec: Some(spec),
            status: None,
        }
    }
}

/// PodSpec describes the desired state of a pod
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodSpec {
    #[serde(default)]
    pub containers: Vec<Container>,

    /// Init containers run before app containers and must complete successfully
    /// Sidecar containers are init containers with restartPolicy: Always
    #[serde(skip_serializing_if = "Option::is_none")]
    pub init_containers: Option<Vec<Container>>,

    /// Ephemeral containers are temporary containers for debugging
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral_containers: Option<Vec<EphemeralContainer>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub volumes: Option<Vec<Volume>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>, // Always, OnFailure, Never

    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account_name: Option<String>,

    /// Deprecated: Use serviceAccountName instead
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub subdomain: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_network: Option<bool>,

    #[serde(rename = "hostPID", skip_serializing_if = "Option::is_none")]
    pub host_pid: Option<bool>,

    #[serde(rename = "hostIPC", skip_serializing_if = "Option::is_none")]
    pub host_ipc: Option<bool>,

    /// Affinity rules for pod scheduling
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affinity: Option<Affinity>,

    /// Tolerations for node taints
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tolerations: Option<Vec<Toleration>>,

    /// Priority value - higher priority pods are scheduled first
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,

    /// Priority class name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority_class_name: Option<String>,

    /// AutomountServiceAccountToken indicates whether a service account token should be automatically mounted
    #[serde(skip_serializing_if = "Option::is_none")]
    pub automount_service_account_token: Option<bool>,

    /// TopologySpreadConstraints describes how a group of pods ought to spread across topology domains
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topology_spread_constraints: Option<Vec<TopologySpreadConstraint>>,

    /// Overhead represents the resource overhead associated with running a pod
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub overhead: Option<HashMap<String, String>>,

    /// SchedulerName is the name of the scheduler to be used to schedule this pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduler_name: Option<String>,

    /// ResourceClaims defines which ResourceClaims must be allocated and reserved before the Pod is allowed to start
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_claims: Option<Vec<PodResourceClaim>>,

    /// ActiveDeadlineSeconds is the max duration in seconds before the pod may be terminated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_deadline_seconds: Option<i64>,

    /// DNSPolicy determines DNS policy for the pod (ClusterFirst, ClusterFirstWithHostNet, Default, None)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns_policy: Option<String>,

    /// DNSConfig defines DNS parameters for the pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns_config: Option<PodDNSConfig>,

    /// SecurityContext holds pod-level security attributes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_context: Option<PodSecurityContext>,

    /// ImagePullSecrets is a list of references to secrets for pulling images
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_pull_secrets: Option<Vec<LocalObjectReference>>,

    /// ShareProcessNamespace indicates whether a single process namespace should be shared between all containers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub share_process_namespace: Option<bool>,

    /// ReadinessGates specifies additional conditions to be evaluated for pod readiness
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness_gates: Option<Vec<PodReadinessGate>>,

    /// RuntimeClassName refers to a RuntimeClass object in the node.k8s.io group
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_class_name: Option<String>,

    /// EnableServiceLinks indicates whether information about services should be injected into pod's environment variables
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_service_links: Option<bool>,

    /// PreemptionPolicy is the policy for preempting pods with lower priority (Never, PreemptLowerPriority)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preemption_policy: Option<String>,

    /// HostUsers controls whether host user namespace is used
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_users: Option<bool>,

    /// SetHostnameAsFQDN determines if the pod's hostname will be configured as the pod's FQDN
    #[serde(rename = "setHostnameAsFQDN", skip_serializing_if = "Option::is_none")]
    pub set_hostname_as_fqdn: Option<bool>,

    /// HostnameOverride specifies an explicit override for the pod's hostname (k8s 1.35)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname_override: Option<String>,

    /// WorkloadRef is a reference to the workload this pod belongs to (k8s 1.35, alpha)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload_ref: Option<WorkloadReference>,

    /// TerminationGracePeriodSeconds is the grace period before forcible pod termination
    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_grace_period_seconds: Option<i64>,

    /// HostAliases is a list of hosts and IPs that will be injected into /etc/hosts
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_aliases: Option<Vec<HostAlias>>,

    /// OS is the target operating system for this pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os: Option<PodOS>,

    /// SchedulingGates is a list of conditions that must be satisfied before the pod may be scheduled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduling_gates: Option<Vec<PodSchedulingGate>>,

    /// Resources is the total amount of CPU and Memory resources required by all containers in the pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
}

/// PodResourceClaim references a ResourceClaim that must be allocated for the pod
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodResourceClaim {
    /// Name uniquely identifies this resource claim inside the pod
    #[serde(default)]
    pub name: String,

    /// ResourceClaimName is the name of a ResourceClaim object in the same namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_claim_name: Option<String>,

    /// ResourceClaimTemplateName is the name of a ResourceClaimTemplate object in the same namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_claim_template_name: Option<String>,
}

/// HostAlias holds the mapping between IP and hostnames that will be injected into /etc/hosts
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostAlias {
    #[serde(default)]
    pub ip: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostnames: Option<Vec<String>>,
}

/// PodOS identifies the OS for a pod
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodOS {
    #[serde(default)]
    pub name: String,
}

/// PodSchedulingGate is associated to a Pod to guard its scheduling
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodSchedulingGate {
    #[serde(default)]
    pub name: String,
}

/// PodDNSConfig defines DNS parameters for the pod
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDNSConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nameservers: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub searches: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<PodDNSConfigOption>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDNSConfigOption {
    #[serde(default)]
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

/// PodSecurityContext holds pod-level security attributes
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodSecurityContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_user: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_group: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_non_root: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_group: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fs_group_change_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supplemental_groups: Option<Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sysctls: Option<Vec<Sysctl>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seccomp_profile: Option<SeccompProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_armor_profile: Option<AppArmorProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub se_linux_options: Option<SELinuxOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub windows_options: Option<WindowsSecurityContextOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub se_linux_change_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supplemental_groups_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sysctl {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppArmorProfile {
    #[serde(rename = "type", default)]
    pub type_: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub localhost_profile: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SELinuxOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsSecurityContextOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gmsa_credential_spec_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gmsa_credential_spec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_user_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_process: Option<bool>,
}

/// LocalObjectReference is a reference to an object in the same namespace
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalObjectReference {
    #[serde(default)]
    pub name: String,
}

/// PodReadinessGate specifies an additional condition to evaluate for pod readiness
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodReadinessGate {
    #[serde(default)]
    pub condition_type: String,
}

/// WorkloadReference references the workload a pod belongs to (k8s 1.35, alpha)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadReference {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub pod_group: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_group_replica_key: Option<String>,
}

/// ContainerRestartRule describes how a container exit maps to a restart
/// decision (k8s 1.35).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerRestartRule {
    /// Action taken when the rule matches. Currently only "Restart".
    #[serde(default)]
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_codes: Option<ContainerRestartRuleOnExitCodes>,
}

/// ContainerRestartRuleOnExitCodes matches a container exit by exit code.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerRestartRuleOnExitCodes {
    /// Operator relating the container exit code(s) to `values` (In / NotIn).
    #[serde(default)]
    pub operator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<i32>>,
}

/// PodExtendedResourceClaimStatus is the status of the ResourceClaim generated
/// for a pod's extended resource requests (k8s 1.35).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodExtendedResourceClaimStatus {
    /// RequestMappings map each container extended-resource request to a
    /// DeviceRequest in the generated ResourceClaim.
    #[serde(default)]
    pub request_mappings: Vec<ContainerExtendedResourceRequest>,
    /// ResourceClaimName is the name of the generated ResourceClaim.
    #[serde(default)]
    pub resource_claim_name: String,
}

/// ContainerExtendedResourceRequest maps a container's extended-resource
/// request to a generated DeviceRequest (k8s 1.35).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerExtendedResourceRequest {
    #[serde(default)]
    pub container_name: String,
    #[serde(default)]
    pub request_name: String,
    #[serde(default)]
    pub resource_name: String,
}

/// PodCertificateProjection projects a short-lived X.509 certificate issued for
/// the pod into the volume (k8s 1.35).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodCertificateProjection {
    /// SignerName is the name of the signer that signs the certificate.
    #[serde(default)]
    pub signer_name: String,
    /// KeyType is the type of keypair the kubelet generates (e.g. RSA3072, ED25519).
    #[serde(default)]
    pub key_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_expiration_seconds: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_bundle_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificate_chain_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_annotations: Option<std::collections::HashMap<String, String>>,
}

/// EphemeralContainer is a temporary container added to a running pod for debugging
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EphemeralContainer {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub image: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<Vec<EnvVar>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_pull_policy: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_context: Option<SecurityContext>,

    /// TargetContainerName is the name of the container to attach to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_container_name: Option<String>,

    /// Stdin enables redirecting stdin to the ephemeral container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdin: Option<bool>,

    /// StdinOnce closes stdin after the first attach
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdin_once: Option<bool>,

    /// TTY allocates a pseudo-TTY for the ephemeral container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tty: Option<bool>,

    /// ResizePolicy is a list of resource resize policies for containers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resize_policy: Option<Vec<ContainerResizePolicy>>,

    /// RestartPolicy defines the restart behavior of this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>,

    /// Resources are the compute resource requirements for this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<crate::types::ResourceRequirements>,

    /// TerminationMessagePath is the file path for the container's termination message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_message_path: Option<String>,

    /// TerminationMessagePolicy indicates how the termination message should be populated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_message_policy: Option<String>,

    /// Ports exposed by the ephemeral container (rarely used; kept for parity)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<ContainerPort>>,

    /// EnvFrom sources to populate environment variables
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_from: Option<Vec<EnvFromSource>>,

    /// VolumeDevices is the list of block devices to be used by the container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_devices: Option<Vec<VolumeDevice>>,

    /// Lifecycle is not allowed for ephemeral containers, but present for parity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<Lifecycle>,

    /// LivenessProbe is not allowed for ephemeral containers, but present for parity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liveness_probe: Option<Probe>,

    /// ReadinessProbe is not allowed for ephemeral containers, but present for parity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness_probe: Option<Probe>,

    /// StartupProbe is not allowed for ephemeral containers, but present for parity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub startup_probe: Option<Probe>,

    /// RestartPolicyRules are evaluated in order to determine restart behaviour (k8s 1.35)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_policy_rules: Option<Vec<ContainerRestartRule>>,
}

/// TopologySpreadConstraint specifies how to spread pods across topology domains
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopologySpreadConstraint {
    /// MaxSkew describes the degree to which pods may be unevenly distributed
    #[serde(default)]
    pub max_skew: i32,

    /// TopologyKey is the key of node labels
    #[serde(default)]
    pub topology_key: String,

    /// WhenUnsatisfiable indicates how to deal with a pod if it doesn't satisfy the spread constraint
    /// Possible values: DoNotSchedule, ScheduleAnyway
    #[serde(default)]
    pub when_unsatisfiable: String,

    /// LabelSelector is used to find matching pods
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label_selector: Option<crate::types::LabelSelector>,

    /// MinDomains indicates a minimum number of eligible domains
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_domains: Option<i32>,

    /// NodeAffinityPolicy indicates how we will treat Pod's nodeAffinity/nodeSelector
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_affinity_policy: Option<String>,

    /// NodeTaintsPolicy indicates how we will treat node taints
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_taints_policy: Option<String>,

    /// MatchLabelKeys is a set of pod label keys to select pods
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_label_keys: Option<Vec<String>>,
}

/// Container represents a single container in a pod
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Container {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub image: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<ContainerPort>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<Vec<EnvVar>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_from: Option<Vec<EnvFromSource>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_devices: Option<Vec<VolumeDevice>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_pull_policy: Option<String>, // Always, Never, IfNotPresent

    #[serde(skip_serializing_if = "Option::is_none")]
    pub liveness_probe: Option<Probe>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness_probe: Option<Probe>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub startup_probe: Option<Probe>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_context: Option<SecurityContext>,

    /// RestartPolicy for the container. Only applies to init containers.
    /// Possible values: Always (sidecar container that runs alongside main containers)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>,

    /// RestartPolicyRules are evaluated in order to determine restart behaviour (k8s 1.35)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_policy_rules: Option<Vec<ContainerRestartRule>>,

    /// ResizePolicy is the list of container resource resize policies
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resize_policy: Option<Vec<ContainerResizePolicy>>,

    /// Lifecycle describes actions that the management system should take in response to container lifecycle events
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<Lifecycle>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_message_path: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_message_policy: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdin: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdin_once: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tty: Option<bool>,
}

/// Lifecycle describes actions that management system should take in response to container lifecycle events
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Lifecycle {
    /// PostStart is called immediately after a container is created
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post_start: Option<LifecycleHandler>,

    /// PreStop is called immediately before a container is terminated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pre_stop: Option<LifecycleHandler>,

    /// StopSignal defines the stop signal to send to the container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_signal: Option<String>,
}

/// LifecycleHandler defines a specific action that should be taken in a lifecycle hook
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleHandler {
    /// Exec specifies the action to take
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec: Option<ExecAction>,

    /// HTTPGet specifies the http request to perform
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_get: Option<HTTPGetAction>,

    /// TCPSocket specifies an action involving a TCP port
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_socket: Option<TCPSocketAction>,

    /// Sleep represents the duration that the container should sleep before being terminated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sleep: Option<SleepAction>,
}

/// SleepAction describes a "sleep" action
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SleepAction {
    /// Seconds is the number of seconds to sleep
    #[serde(default)]
    pub seconds: i64,
}

/// ContainerResizePolicy represents resource resize policy for the container
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerResizePolicy {
    /// Name of the resource to which this resource resize policy applies (cpu, memory)
    #[serde(default)]
    pub resource_name: String,
    /// Restart policy to apply when the specified resource is resized (NotRequired or RestartContainer)
    #[serde(default)]
    pub restart_policy: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub privileged: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_user: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_group: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_as_non_root: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only_root_filesystem: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_privilege_escalation: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub proc_mount: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Capabilities>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub seccomp_profile: Option<SeccompProfile>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub se_linux_options: Option<SELinuxOptions>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_armor_profile: Option<AppArmorProfile>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub windows_options: Option<WindowsSecurityContextOptions>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub add: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub drop: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeccompProfile {
    #[serde(default)]
    pub r#type: String, // RuntimeDefault, Unconfined, Localhost

    #[serde(skip_serializing_if = "Option::is_none")]
    pub localhost_profile: Option<String>,
}

fn default_protocol() -> String {
    "TCP".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerPort {
    #[serde(default)]
    pub container_port: u16,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(default = "default_protocol")]
    pub protocol: String, // TCP, UDP, SCTP

    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_port: Option<u16>,

    /// What host IP to bind the external port to.
    #[serde(skip_serializing_if = "Option::is_none", rename = "hostIP")]
    pub host_ip: Option<String>,
}

// Generate skip functions for structs with all-optional fields
// NOTE: Do NOT use skip_if_empty for structs with mutually exclusive fields:
// - EnvVarSource: only one of (config_map_key_ref, secret_key_ref, field_ref, resource_field_ref) should be set
// - ClaimSource: only one of (resource_claim_name, resource_claim_template_name) should be set
// Using skip_if_empty would incorrectly skip serialization when only one field is set.
skip_if_empty!(
    skip_empty_security_context,
    SecurityContext,
    privileged,
    run_as_user,
    run_as_non_root,
    allow_privilege_escalation,
    capabilities,
    seccomp_profile
);
skip_if_empty!(skip_empty_capabilities, Capabilities, add, drop);
skip_if_empty!(
    skip_empty_empty_dir_volume_source,
    EmptyDirVolumeSource,
    medium
);
skip_if_empty!(
    skip_empty_config_map_volume_source,
    ConfigMapVolumeSource,
    name,
    items,
    default_mode,
    optional
);
skip_if_empty!(
    skip_empty_secret_volume_source,
    SecretVolumeSource,
    secret_name,
    items,
    default_mode,
    optional
);
skip_if_empty!(
    skip_empty_downward_api_volume_source,
    DownwardAPIVolumeSource,
    items,
    default_mode
);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvVar {
    #[serde(default)]
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_from: Option<EnvVarSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvVarSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_map_key_ref: Option<ConfigMapKeySelector>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_key_ref: Option<SecretKeySelector>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_ref: Option<ObjectFieldSelector>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_field_ref: Option<ResourceFieldSelector>,

    /// Selects a key from a file volume source
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_key_ref: Option<FileKeySelector>,
}

/// FileKeySelector selects a key from a file volume source
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FileKeySelector {
    /// The name of the volume containing the file
    #[serde(default)]
    pub volume_name: String,
    /// The relative path of the file to map the key to
    #[serde(default)]
    pub path: String,
    /// The key to select
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Specify whether the volume or its key must be defined
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// EnvFromSource represents the source of a set of ConfigMaps/Secrets to populate env vars
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvFromSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_map_ref: Option<ConfigMapEnvSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretEnvSource>,
}

/// ConfigMapEnvSource selects a ConfigMap to populate the env vars with
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapEnvSource {
    #[serde(default)]
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// SecretEnvSource selects a Secret to populate the env vars with
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretEnvSource {
    #[serde(default)]
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// VolumeDevice describes a mapping of a raw block device within a container
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeDevice {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub device_path: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapKeySelector {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub key: String,

    /// Specify whether the ConfigMap or its key must be defined
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeySelector {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub key: String,

    /// Specify whether the Secret or its key must be defined
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMount {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub mount_path: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_path_expr: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mount_propagation: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub recursive_read_only: Option<String>,
}

/// The deprecated in-tree volume sources of core/v1 `VolumeSource`
/// (`staging/src/k8s.io/api/core/v1/types.go`, `VolumeSource`). Rusternetes
/// does not implement these plugins, but the API must still round-trip them
/// losslessly and PodSecurity `restrictedVolumes` names the type of the
/// offending source (`check_restrictedVolumes.go:108-153`). Most sources are
/// kept as opaque JSON; only their presence is interpreted. `gcePersistentDisk`,
/// `awsElasticBlockStore` and `fc` are typed (the same core/v1 structs a PV uses)
/// because csi-translation reads them (`TranslateInTreeInlineVolumeToCSI`,
/// `staging/src/k8s.io/csi-translation-lib/translate.go:67-90`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LegacyVolumeSources {
    #[serde(rename = "gcePersistentDisk", skip_serializing_if = "Option::is_none")]
    pub gce_persistent_disk: Option<crate::resources::volume::GCEPersistentDiskVolumeSource>,

    #[serde(
        rename = "awsElasticBlockStore",
        skip_serializing_if = "Option::is_none"
    )]
    pub aws_elastic_block_store: Option<crate::resources::volume::AWSElasticBlockStoreVolumeSource>,

    #[serde(rename = "gitRepo", skip_serializing_if = "Option::is_none")]
    pub git_repo: Option<serde_json::Value>,

    #[serde(rename = "glusterfs", skip_serializing_if = "Option::is_none")]
    pub glusterfs: Option<serde_json::Value>,

    #[serde(rename = "rbd", skip_serializing_if = "Option::is_none")]
    pub rbd: Option<serde_json::Value>,

    #[serde(rename = "flexVolume", skip_serializing_if = "Option::is_none")]
    pub flex_volume: Option<serde_json::Value>,

    #[serde(rename = "cinder", skip_serializing_if = "Option::is_none")]
    pub cinder: Option<serde_json::Value>,

    #[serde(rename = "cephfs", skip_serializing_if = "Option::is_none")]
    pub cephfs: Option<serde_json::Value>,

    #[serde(rename = "flocker", skip_serializing_if = "Option::is_none")]
    pub flocker: Option<serde_json::Value>,

    #[serde(rename = "fc", skip_serializing_if = "Option::is_none")]
    pub fc: Option<crate::resources::volume::FCVolumeSource>,

    #[serde(rename = "azureFile", skip_serializing_if = "Option::is_none")]
    pub azure_file: Option<serde_json::Value>,

    #[serde(rename = "vsphereVolume", skip_serializing_if = "Option::is_none")]
    pub vsphere_volume: Option<serde_json::Value>,

    #[serde(rename = "quobyte", skip_serializing_if = "Option::is_none")]
    pub quobyte: Option<serde_json::Value>,

    #[serde(rename = "azureDisk", skip_serializing_if = "Option::is_none")]
    pub azure_disk: Option<serde_json::Value>,

    #[serde(
        rename = "photonPersistentDisk",
        skip_serializing_if = "Option::is_none"
    )]
    pub photon_persistent_disk: Option<serde_json::Value>,

    #[serde(rename = "portworxVolume", skip_serializing_if = "Option::is_none")]
    pub portworx_volume: Option<serde_json::Value>,

    #[serde(rename = "scaleIO", skip_serializing_if = "Option::is_none")]
    pub scale_io: Option<serde_json::Value>,

    #[serde(rename = "storageos", skip_serializing_if = "Option::is_none")]
    pub storageos: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    #[serde(default)]
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub empty_dir: Option<EmptyDirVolumeSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_path: Option<HostPathVolumeSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_map: Option<ConfigMapVolumeSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<SecretVolumeSource>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistent_volume_claim: Option<PersistentVolumeClaimVolumeSource>,

    /// Downward API volume source
    #[serde(
        rename = "downwardAPI",
        alias = "downwardApi",
        skip_serializing_if = "Option::is_none"
    )]
    pub downward_api: Option<DownwardAPIVolumeSource>,

    /// CSI (Container Storage Interface) ephemeral inline volume
    #[serde(skip_serializing_if = "Option::is_none")]
    pub csi: Option<crate::resources::csi::CSIVolumeSource>,

    /// Generic ephemeral volume with volumeClaimTemplate
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<EphemeralVolumeSource>,

    /// NFS represents an NFS mount on the host
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nfs: Option<crate::resources::volume::NFSVolumeSource>,

    /// ISCSI represents an ISCSI volume attached and mounted on the host
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iscsi: Option<crate::resources::volume::ISCSIVolumeSource>,

    /// Projected defines a projected volume that maps several existing volume sources into one
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projected: Option<ProjectedVolumeSource>,

    /// Image represents an OCI object (container image or artifact) pulled and mounted on the host
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<ImageVolumeSource>,

    /// Deprecated in-tree sources (gcePersistentDisk, ...), kept for lossless
    /// round-trip and PodSecurity type naming.
    #[serde(flatten)]
    pub legacy_sources: LegacyVolumeSources,
}

/// ProjectedVolumeSource represents a projected volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectedVolumeSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<VolumeProjection>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<i32>,
}

/// VolumeProjection is a single volume source for a projected volume
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeProjection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<SecretProjection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_map: Option<ConfigMapProjection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account_token: Option<ServiceAccountTokenProjection>,
    #[serde(
        rename = "downwardAPI",
        alias = "downwardApi",
        skip_serializing_if = "Option::is_none"
    )]
    pub downward_api: Option<DownwardAPIProjection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_trust_bundle: Option<ClusterTrustBundleProjection>,
    /// PodCertificate projects a freshly-issued X.509 certificate (k8s 1.35)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_certificate: Option<PodCertificateProjection>,
}

/// SecretProjection adapts a Secret into a projected volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretProjection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<KeyToPath>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// ConfigMapProjection adapts a ConfigMap into a projected volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapProjection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<KeyToPath>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// ServiceAccountTokenProjection represents a projected service account token volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountTokenProjection {
    #[serde(default)]
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_seconds: Option<i64>,
}

/// DownwardAPIProjection represents Downward API info for the projected volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownwardAPIProjection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<DownwardAPIVolumeFile>>,
}

/// ClusterTrustBundleProjection projects a ClusterTrustBundle into a projected volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterTrustBundleProjection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signer_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label_selector: Option<crate::types::LabelSelector>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
    #[serde(default)]
    pub path: String,
}

/// ImageVolumeSource represents an OCI object (container image or artifact)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageVolumeSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pull_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmptyDirVolumeSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub medium: Option<String>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt",
        deserialize_with = "crate::quantity::wire::deserialize_opt"
    )]
    pub size_limit: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostPathVolumeSource {
    #[serde(default)]
    pub path: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapVolumeSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<KeyToPath>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretVolumeSource {
    #[serde(skip_serializing_if = "Option::is_none", alias = "secret_name")]
    pub secret_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<KeyToPath>>,

    #[serde(skip_serializing_if = "Option::is_none", alias = "default_mode")]
    pub default_mode: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyToPath {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub path: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeClaimVolumeSource {
    #[serde(default)]
    pub claim_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

/// DownwardAPIVolumeSource represents a downward API volume
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownwardAPIVolumeSource {
    /// Items is a list of downward API volume file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<DownwardAPIVolumeFile>>,

    /// Optional: mode bits to use on created files by default
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_mode: Option<i32>,
}

/// DownwardAPIVolumeFile represents information to create the file containing the pod field
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownwardAPIVolumeFile {
    /// Required: Path is the relative path name of the file to be created
    #[serde(default)]
    pub path: String,

    /// Required: Selects a field of the pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_ref: Option<ObjectFieldSelector>,

    /// Selects a resource of the container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_field_ref: Option<ResourceFieldSelector>,

    /// Optional: mode bits to use on this file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<i32>,
}

/// ObjectFieldSelector selects an API object field
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectFieldSelector {
    /// Path of the field to select in the specified API version
    #[serde(default)]
    pub field_path: String,

    /// Version of the schema the FieldPath is written in terms of, defaults to "v1"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
}

/// ResourceFieldSelector selects a resource of the container
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceFieldSelector {
    /// Container name: required for volumes, optional for env vars
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,

    /// Required: resource to select
    #[serde(default)]
    pub resource: String,

    /// Specifies the output format of the exposed resources, defaults to "1"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub divisor: Option<String>,
}

/// EphemeralVolumeSource represents an ephemeral volume with a volume claim template
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EphemeralVolumeSource {
    /// Will be used to create a stand-alone PVC to provision the volume
    pub volume_claim_template: Option<PersistentVolumeClaimTemplate>,
}

/// PersistentVolumeClaimTemplate is used to produce PVC objects
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeClaimTemplate {
    /// May contain labels and annotations that will be copied into the PVC
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,

    /// The specification for the PersistentVolumeClaim
    #[serde(default)]
    pub spec: crate::resources::volume::PersistentVolumeClaimSpec,
}

/// HostIP represents a single host IP address
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostIP {
    #[serde(default)]
    pub ip: String,
}

/// PodIP represents a single pod IP address
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodIP {
    #[serde(default)]
    pub ip: String,
}

/// PodCondition represents a condition of a pod
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodCondition {
    /// Type of pod condition (e.g. Ready, ContainersReady, Initialized, PodScheduled)
    #[serde(rename = "type", default)]
    pub condition_type: String,

    /// Status of the condition (True, False, or Unknown)
    #[serde(default)]
    pub status: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Last time the condition was probed.
    /// Mirrors upstream `PodCondition.LastProbeTime` (k8s.io/api/core/v1).
    /// Optional in the API and currently not populated by the kubelet, but
    /// strict decoding requires the field to be recognised when clients
    /// echo it back on Pod updates (e.g. e2e conformance test fixtures).
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize",
        default
    )]
    pub last_probe_time: Option<chrono::DateTime<chrono::Utc>>,

    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize",
        default
    )]
    pub last_transition_time: Option<chrono::DateTime<chrono::Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

/// PodStatus represents the current state of a pod
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<Phase>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(rename = "hostIP", skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<String>,

    /// All host IPs for dual-stack
    #[serde(rename = "hostIPs", skip_serializing_if = "Option::is_none")]
    pub host_i_ps: Option<Vec<HostIP>>,

    #[serde(rename = "podIP", skip_serializing_if = "Option::is_none")]
    pub pod_ip: Option<String>,

    /// All pod IPs for dual-stack
    #[serde(rename = "podIPs", skip_serializing_if = "Option::is_none")]
    pub pod_i_ps: Option<Vec<PodIP>>,

    /// Node nominated for preemption to make room for this pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nominated_node_name: Option<String>,

    /// QOS class for the pod (Guaranteed, Burstable, or BestEffort)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qos_class: Option<String>,

    /// Time at which the pod was acknowledged by the kubelet
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize",
        default
    )]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,

    /// Pod-level conditions (Ready, ContainersReady, Initialized, PodScheduled)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<PodCondition>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_statuses: Option<Vec<ContainerStatus>>,

    /// Status of init containers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub init_container_statuses: Option<Vec<ContainerStatus>>,

    /// Status of ephemeral containers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral_container_statuses: Option<Vec<ContainerStatus>>,

    /// Status of resource resize for the pod
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resize: Option<String>,

    /// Total compute resources actually allocated to the pod (pod-level resize)
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub allocated_resources: Option<HashMap<String, String>>,

    /// Compute resources the kubelet enforces for the pod (pod-level resize)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<crate::types::ResourceRequirements>,

    /// Status of resource claims
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_claim_statuses: Option<Vec<PodResourceClaimStatus>>,

    /// ObservedGeneration represents the .metadata.generation that the status was set based upon
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// ExtendedResourceClaimStatus is the status of the extended resource claim
    /// generated for this pod (k8s 1.35, DRA)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended_resource_claim_status: Option<PodExtendedResourceClaimStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerStatus {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub restart_count: u32,

    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "deserialize_container_state_option"
    )]
    pub state: Option<ContainerState>,

    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "deserialize_container_state_option"
    )]
    pub last_state: Option<ContainerState>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    #[serde(rename = "imageID", skip_serializing_if = "Option::is_none")]
    pub image_id: Option<String>,

    #[serde(rename = "containerID", skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,

    /// Whether the container has passed its startup probe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started: Option<bool>,

    /// Resources allocated to this container by the node
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "crate::types::deserialize_quantity_map"
    )]
    pub allocated_resources: Option<HashMap<String, String>>,

    /// Detailed status of allocated resources for this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocated_resources_status: Option<Vec<ResourceStatus>>,

    /// Resources represents the compute resource requests/limits of this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    /// User represents the user identity for this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<ContainerUser>,

    /// VolumeMounts reports status of volume mounts
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMountStatus>>,

    /// StopSignal reports the effective stop signal for this container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_signal: Option<String>,
}

/// ResourceStatus represents the status of an individual resource allocation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceStatus {
    /// Name of the resource (e.g., "cpu", "memory", or extended resource)
    #[serde(default)]
    pub name: String,

    /// List of individual resources tracked for this allocation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<Vec<ResourceHealth>>,
}

/// ResourceHealth represents the health of an individual resource
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceHealth {
    /// Unique identifier of the resource (e.g., device ID)
    #[serde(rename = "resourceID", default)]
    pub resource_id: String,

    /// Health status of the resource (Healthy, Unhealthy, Unknown)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
}

/// ContainerState holds a possible state of container. Only one of its members
/// may be specified; if none are specified, the default one is
/// `ContainerStateWaiting`.
///
/// Ported from `ContainerState` (staging/src/k8s.io/api/core/v1/types.go:3300):
/// a struct of three optional pointers. Nothing requires exactly one
/// (`ValidateContainerStateTransition`, pkg/apis/core/validation/validation.go:5841,
/// only constrains Terminated transitions), so a strategic-merge status patch
/// without `waiting: null` stores `running` and `waiting` together (#2453).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerState {
    /// Details about a waiting container
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<ContainerStateWaiting>,
    /// Details about a running container
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running: Option<ContainerStateRunning>,
    /// Details about a terminated container
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminated: Option<ContainerStateTerminated>,
}

/// ContainerStateWaiting is a waiting state of a container
/// (types.go:3257).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerStateWaiting {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// ContainerStateRunning is a running state of a container (types.go:3267).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerStateRunning {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::types::k8s_time_str::serialize",
        deserialize_with = "crate::types::k8s_time_str::deserialize"
    )]
    pub started_at: Option<String>,
}

/// ContainerStateTerminated is a terminated state of a container
/// (types.go:3274).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerStateTerminated {
    #[serde(default)]
    pub exit_code: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::types::k8s_time_str::serialize",
        deserialize_with = "crate::types::k8s_time_str::deserialize"
    )]
    pub started_at: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::types::k8s_time_str::serialize",
        deserialize_with = "crate::types::k8s_time_str::deserialize"
    )]
    pub finished_at: Option<String>,
    #[serde(
        rename = "containerID",
        alias = "containerId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub container_id: Option<String>,
}

/// Affinity is a group of affinity scheduling rules
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Affinity {
    /// Node affinity scheduling rules
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_affinity: Option<NodeAffinity>,

    /// Pod affinity scheduling rules
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_affinity: Option<PodAffinity>,

    /// Pod anti-affinity scheduling rules
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_anti_affinity: Option<PodAntiAffinity>,
}

/// Node affinity is a group of node affinity scheduling rules
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeAffinity {
    /// Hard node affinity requirements
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_during_scheduling_ignored_during_execution: Option<NodeSelector>,

    /// Soft node affinity preferences
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_during_scheduling_ignored_during_execution: Option<Vec<PreferredSchedulingTerm>>,
}

/// A node selector represents the union of the results of one or more label queries
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelector {
    /// A list of node selector terms (ORed together)
    #[serde(default)]
    pub node_selector_terms: Vec<NodeSelectorTerm>,
}

/// A node selector term is associated with the corresponding weight
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelectorTerm {
    /// A list of node selector requirements by node's labels
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_expressions: Option<Vec<NodeSelectorRequirement>>,

    /// A list of node selector requirements by node's fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_fields: Option<Vec<NodeSelectorRequirement>>,
}

/// A node selector requirement is a selector that contains values, a key, and an operator
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelectorRequirement {
    /// The label key
    #[serde(default)]
    pub key: String,

    /// Operator: In, NotIn, Exists, DoesNotExist, Gt, Lt
    #[serde(default)]
    pub operator: String,

    /// An array of string values
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// An empty preferred scheduling term matches all objects with implicit weight 0
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PreferredSchedulingTerm {
    /// Weight associated with matching the corresponding nodeSelectorTerm, in the range 1-100
    #[serde(default)]
    pub weight: i32,

    /// A node selector term, associated with the corresponding weight
    #[serde(default)]
    pub preference: NodeSelectorTerm,
}

/// Pod affinity is a group of inter pod affinity scheduling rules
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodAffinity {
    /// Hard pod affinity requirements
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_during_scheduling_ignored_during_execution: Option<Vec<PodAffinityTerm>>,

    /// Soft pod affinity preferences
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_during_scheduling_ignored_during_execution: Option<Vec<WeightedPodAffinityTerm>>,
}

/// Pod anti-affinity is a group of inter pod anti affinity scheduling rules
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodAntiAffinity {
    /// Hard pod anti-affinity requirements
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_during_scheduling_ignored_during_execution: Option<Vec<PodAffinityTerm>>,

    /// Soft pod anti-affinity preferences
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_during_scheduling_ignored_during_execution: Option<Vec<WeightedPodAffinityTerm>>,
}

/// Defines a set of pods that should be co-located
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodAffinityTerm {
    /// A label selector over a set of resources.
    ///
    /// A **pointer** upstream (`staging/src/k8s.io/api/core/v1/types.go`,
    /// `PodAffinityTerm.LabelSelector *metav1.LabelSelector`), and the
    /// distinction is load-bearing: `LabelSelectorAsSelector(nil)` returns
    /// `labels.Nothing()` while `&LabelSelector{}` returns `labels.Everything()`
    /// (`apimachinery/pkg/apis/meta/v1/helpers.go:37-43`). An absent selector
    /// therefore matches **no** pods, and an empty one matches every pod — so
    /// this is `Option`, not a defaulted value (#1939).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label_selector: Option<crate::types::LabelSelector>,

    /// Namespaces specifies which namespaces the labelSelector applies to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespaces: Option<Vec<String>>,

    /// Topology key for pod placement
    #[serde(default)]
    pub topology_key: String,

    /// A label query over the set of namespaces that the term applies to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_selector: Option<crate::types::LabelSelector>,

    /// Keys of pod labels merged into the labelSelector to select against
    /// pods that have the same values for these keys.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_label_keys: Option<Vec<String>>,

    /// Keys of pod labels merged into the labelSelector to select against
    /// pods that have different values for these keys.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mismatch_label_keys: Option<Vec<String>>,
}

/// The weights of all the matched WeightedPodAffinityTerm fields are added per-node to find the most preferred node(s)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WeightedPodAffinityTerm {
    /// Weight associated with matching the corresponding podAffinityTerm, in the range 1-100
    #[serde(default)]
    pub weight: i32,

    /// Required pod affinity term
    #[serde(default)]
    pub pod_affinity_term: PodAffinityTerm,
}

/// The pod this Toleration is attached to tolerates any taint that matches the triple using the matching operator
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Toleration {
    /// Key is the taint key that the toleration applies to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// Operator represents a key's relationship to the value: Equal or Exists
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,

    /// Value is the taint value the toleration matches to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    /// Effect indicates the taint effect to match: NoSchedule, PreferNoSchedule, NoExecute
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effect: Option<String>,

    /// TolerationSeconds represents the period of time the toleration tolerates the taint
    #[serde(skip_serializing_if = "Option::is_none")]
    pub toleration_seconds: Option<i64>,
}

/// Probe describes a health check to be performed against a container
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Probe {
    /// HTTP GET probe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_get: Option<HTTPGetAction>,

    /// TCP socket probe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp_socket: Option<TCPSocketAction>,

    /// Exec command probe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec: Option<ExecAction>,

    /// Number of seconds after the container has started before probes are initiated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_delay_seconds: Option<i32>,

    /// Number of seconds after which the probe times out
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,

    /// How often (in seconds) to perform the probe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period_seconds: Option<i32>,

    /// Minimum consecutive successes for the probe to be considered successful
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success_threshold: Option<i32>,

    /// Minimum consecutive failures for the probe to be considered failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_threshold: Option<i32>,

    /// GRPC probe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc: Option<GRPCAction>,

    /// Optional duration in seconds the pod needs to terminate gracefully upon probe failure
    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination_grace_period_seconds: Option<i64>,
}

/// GRPCAction describes an action based on GRPC health check
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GRPCAction {
    /// Port number of the gRPC service.
    /// K8s API: IntOrString — int port or named port from container.ports[].name.
    #[serde(default)]
    pub port: IntOrString,

    /// Service is the name of the service to place in the gRPC HealthCheckRequest
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
}

/// Resolve a probe port (which K8s spec'd as IntOrString) against a Container's
/// declared ports. Integer values pass through; string values are looked up by
/// `containerPort.name`. Returns None if a named port has no matching entry —
/// callers should treat that as a probe failure.
pub fn resolve_probe_port(port: &IntOrString, container: &Container) -> Option<u16> {
    match port {
        IntOrString::Int(n) => {
            if *n < 0 || *n > u16::MAX as i32 {
                None
            } else {
                Some(*n as u16)
            }
        }
        IntOrString::String(name) => container
            .ports
            .as_ref()
            .and_then(|ports| {
                ports
                    .iter()
                    .find(|p| p.name.as_deref() == Some(name.as_str()))
                    .map(|p| p.container_port)
            })
            .or_else(|| name.parse::<u16>().ok()),
    }
}

/// HTTPGetAction describes an action based on HTTP Get requests
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HTTPGetAction {
    /// Path to access on the HTTP server
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// Port to access on the container.
    /// K8s API: IntOrString — int port or named port from container.ports[].name.
    #[serde(default)]
    pub port: IntOrString,

    /// Host name to connect to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,

    /// Scheme to use for connecting (HTTP or HTTPS)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheme: Option<String>,

    /// Custom headers to set in the request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_headers: Option<Vec<HTTPHeader>>,
}

/// HTTPHeader describes a custom header to be used in HTTP probes
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HTTPHeader {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub value: String,
}

/// TCPSocketAction describes an action based on opening a socket
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TCPSocketAction {
    /// Port to connect to on the container.
    /// K8s API: IntOrString — int port or named port from container.ports[].name.
    #[serde(default)]
    pub port: IntOrString,

    /// Host name to connect to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

/// ExecAction describes a command-based action
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecAction {
    /// Command to execute
    #[serde(default)]
    pub command: Vec<String>,
}

/// ContainerUser represents user identity information for a container
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ContainerUser {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub linux: Option<LinuxContainerUser>,
}

/// LinuxContainerUser represents Linux-specific user identity
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LinuxContainerUser {
    #[serde(default)]
    pub uid: i64,
    #[serde(default)]
    pub gid: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supplemental_groups: Option<Vec<i64>>,
}

/// VolumeMountStatus shows status of a VolumeMount
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMountStatus {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub mount_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recursive_read_only: Option<String>,
}

/// PodResourceClaimStatus describes the status of a resource claim in the pod
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PodResourceClaimStatus {
    #[serde(default)]
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_claim_name: Option<String>,
}
