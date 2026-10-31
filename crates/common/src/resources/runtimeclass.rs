use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// RuntimeClass defines a class of container runtime supported in the cluster.
/// The RuntimeClass is used to determine which container runtime is used to run
/// all containers in a pod. RuntimeClasses are manually defined by a user or cluster
/// provisioner, and referenced in the PodSpec. The Kubelet is responsible for
/// resolving the RuntimeClassName reference before running the pod.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeClass {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    #[serde(default)]
    pub metadata: ObjectMeta,

    /// Handler specifies the underlying runtime and configuration that the CRI
    /// implementation will use to handle pods of this class. The possible values
    /// are specific to the node & CRI configuration. It is assumed that all
    /// handlers are available on every node, and handlers of the same name are
    /// equivalent on every node. For example, a handler called "runc" might specify
    /// that the runc OCI runtime (using native Linux containers) will be used to
    /// run the containers in a pod. The Handler must be lowercase, conform to the
    /// DNS Label (RFC 1123) requirements, and is immutable.
    #[serde(default)]
    pub handler: String,

    /// Overhead represents the resource overhead associated with running a pod for a
    /// given RuntimeClass. For more details, see
    /// https://kubernetes.io/docs/concepts/scheduling-eviction/pod-overhead/
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overhead: Option<Overhead>,

    /// Scheduling holds the scheduling constraints to ensure that pods running with
    /// this RuntimeClass are scheduled to nodes that support it. If scheduling is nil,
    /// this RuntimeClass is assumed to be supported by all nodes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduling: Option<Scheduling>,
}

impl RuntimeClass {
    pub fn new(name: impl Into<String>, handler: impl Into<String>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "RuntimeClass".to_string(),
                api_version: "node.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            handler: handler.into(),
            overhead: None,
            scheduling: None,
        }
    }

    pub fn with_overhead(mut self, overhead: Overhead) -> Self {
        self.overhead = Some(overhead);
        self
    }

    pub fn with_scheduling(mut self, scheduling: Scheduling) -> Self {
        self.scheduling = Some(scheduling);
        self
    }
}

/// Overhead structure represents the resource overhead associated with running a pod.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Overhead {
    /// PodFixed represents the fixed resource overhead associated with running a pod.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::quantity::wire::serialize_opt_map",
        deserialize_with = "crate::quantity::wire::deserialize_opt_map"
    )]
    pub pod_fixed: Option<HashMap<String, String>>,
}

/// Scheduling specifies the scheduling constraints for nodes supporting a RuntimeClass.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Scheduling {
    /// NodeSelector lists labels that must be present on nodes that support this
    /// RuntimeClass. Pods using this RuntimeClass can only be scheduled to a
    /// node matched by this selector. The RuntimeClass NodeSelector is merged
    /// with a pod's existing nodeSelector. Any conflicts will cause the pod to
    /// be rejected in admission.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<HashMap<String, String>>,

    /// Tolerations are appended (excluding duplicates) to pods running with this
    /// RuntimeClass during admission, effectively unioning the set of nodes
    /// tolerated by the pod and the RuntimeClass.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tolerations: Option<Vec<crate::resources::pod::Toleration>>,
}
