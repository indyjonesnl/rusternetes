//! Port of `staging/src/k8s.io/cloud-provider/api/well_known_taints.go`.

/// `TaintExternalCloudProvider`: set on a node to mark it as unusable, when
/// kubelet is started with the "external" cloud provider, until a controller
/// from the cloud-controller-manager initializes this node, and then removes
/// the taint.
pub const TAINT_EXTERNAL_CLOUD_PROVIDER: &str = "node.cloudprovider.kubernetes.io/uninitialized";

/// `TaintNodeShutdown`: when node is shutdown in external cloud provider.
pub const TAINT_NODE_SHUTDOWN: &str = "node.cloudprovider.kubernetes.io/shutdown";
