//! Cloud node lifecycle controller.
//!
//! Ported from
//! `staging/src/k8s.io/cloud-provider/controllers/nodelifecycle/node_lifecycle_controller.go`
//! (release-1.35). It deletes Node objects whose instance no longer exists on
//! the cloud provider and taints (`node.cloudprovider.kubernetes.io/shutdown`,
//! `NoSchedule`) nodes whose instance is shut down. It is a cloud-controller-
//! manager controller: nothing starts it unless a cloud provider
//! ([`crate::cloud_provider::Interface`]) is configured, and
//! [`CloudNodeLifecycleController::new`] refuses to construct without one,
//! exactly like `NewCloudNodeLifecycleController`.
//!
//! Go -> Rust differences:
//!
//! * `nodeLister` / `kubeClient` are one `Storage` handle (as in the sibling
//!   controllers): the "cache read" in `MonitorNodes` is `storage.list`.
//!   `NewCloudNodeLifecycleController`'s `kubeClient == nil` check
//!   ("kubernetes client is nil") is unrepresentable: `Arc<S>` is non-null.
//! * `controllersmetrics.ControllerStarted/Stopped("cloud-node-lifecycle")`
//!   are not ported: this repo has no `ControllerManagerMetrics` registry for
//!   the cloud controller manager (deferred, see PR).
//! * The `record.EventBroadcaster` is the shared `EventRecorder`; the
//!   structured-logging sink (`StartStructuredLogging`) is `tracing` output.

use crate::cloud_provider::api::TAINT_NODE_SHUTDOWN;
use crate::cloud_provider::node_helpers::{add_or_update_taint_on_node, remove_taint_off_node};
use crate::cloud_provider::{get_instance_provider_id, CloudError, Interface};
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::{EventSource, EventType, Node, Taint};
use rusternetes_storage::{build_key, build_prefix, EventRecorder, Storage};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

/// `deleteNodeEvent`.
pub const DELETE_NODE_EVENT: &str = "DeletingNode";
/// `deleteNodeFailedEvent`.
pub const DELETE_NODE_FAILED_EVENT: &str = "DeletingNodeFailed";
/// The `v1.EventSource.Component` of `Run`'s recorder.
pub const EVENT_SOURCE_COMPONENT: &str = "cloud-node-lifecycle-controller";

/// `ShutdownTaint`: `Key: TaintNodeShutdown, Effect: NoSchedule`.
pub fn shutdown_taint() -> Taint {
    Taint {
        key: TAINT_NODE_SHUTDOWN.to_string(),
        value: None,
        effect: "NoSchedule".to_string(),
        time_added: None,
    }
}

/// `nodeutil.GetNodeCondition(&node.Status, v1.NodeReady)` defaulted to
/// `ConditionUnknown`, as `MonitorNodes` does.
fn node_ready_status(node: &Node) -> String {
    node.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.condition_type == "Ready"))
        .map(|c| c.status.clone())
        .unwrap_or_else(|| "Unknown".to_string())
}

/// `CloudNodeLifecycleController`: responsible for deleting/updating
/// kubernetes nodes that have been deleted/shutdown on the cloud provider.
pub struct CloudNodeLifecycleController<S: Storage> {
    storage: Arc<S>,
    recorder: EventRecorder<S>,
    cloud: Arc<dyn Interface>,
    /// Value controlling NodeController monitoring period, i.e. how often
    /// does NodeController check node status posted from kubelet. This value
    /// should be lower than nodeMonitorGracePeriod set in controller-manager.
    node_monitor_period: Duration,
}

impl<S: Storage + 'static> CloudNodeLifecycleController<S> {
    /// `NewCloudNodeLifecycleController` (:70-96).
    pub fn new(
        storage: Arc<S>,
        cloud: Option<Arc<dyn Interface>>,
        node_monitor_period: Duration,
    ) -> Result<Self, String> {
        let Some(cloud) = cloud else {
            return Err("no cloud provider provided".to_string());
        };

        let instances_supported = cloud.instances().is_some();
        let instances_v2_supported = cloud.instances_v2().is_some();
        if !instances_supported && !instances_v2_supported {
            return Err("cloud provider does not support instances".to_string());
        }

        Ok(Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            storage,
            cloud,
            node_monitor_period,
        })
    }

    /// `Run` (:98-123): periodically check if any nodes have been deleted or
    /// shutdown from the cloudprovider. `wait.UntilWithContext` runs a pass
    /// first, then sleeps `nodeMonitorPeriod`.
    ///
    /// The loops below communicate with the API server with a worst case
    /// complexity of O(num_nodes) per cycle. Upstream: "DO NOT MODIFY this to
    /// perform frequent operations."
    pub async fn run(self: Arc<Self>) {
        info!("Sending events to api server");
        loop {
            self.monitor_nodes().await;
            tokio::time::sleep(self.node_monitor_period).await;
        }
    }

    /// `MonitorNodes` (:125-198): checks to see if nodes in the cluster have
    /// been deleted or shutdown. If deleted, it deletes the node resource. If
    /// shutdown it applies a shutdown taint to the node.
    pub async fn monitor_nodes(&self) {
        let nodes: Vec<Node> = match self.storage.list(&build_prefix("nodes", None)).await {
            Ok(n) => n,
            Err(e) => {
                error!("error listing nodes from cache: {e}");
                return;
            }
        };

        let shutdown_taint = shutdown_taint();
        for node in nodes {
            // Default NodeReady status to v1.ConditionUnknown.
            let status = node_ready_status(&node);

            if status == "True" {
                // if taint exist remove taint
                if let Err(e) = remove_taint_off_node(
                    &*self.storage,
                    &node.metadata.name,
                    Some(&node),
                    &[&shutdown_taint],
                )
                .await
                {
                    error!("error patching node taints: {e}");
                }
                continue;
            }

            // At this point the node has NotReady status, we need to check if
            // the node has been removed from the cloud provider. If node
            // cannot be found in cloudprovider, then delete the node.
            let exists = match self.ensure_node_exists_by_provider_id(&node).await {
                Ok(v) => v,
                Err(e) => {
                    error!("error checking if node {} exists: {e}", node.metadata.name);
                    continue;
                }
            };

            if !exists {
                // Current node does not exist, we should delete it, its
                // taints do not matter anymore.
                info!(
                    "deleting node since it is no longer present in cloud provider: {}",
                    node.metadata.name
                );

                let node_ref = ObjectReference {
                    api_version: Some("v1".to_string()),
                    kind: Some("Node".to_string()),
                    name: Some(node.metadata.name.clone()),
                    uid: Some(node.metadata.uid.clone()),
                    namespace: None,
                    ..Default::default()
                };

                self.event(
                    &node_ref,
                    EventType::Normal,
                    DELETE_NODE_EVENT,
                    &format!(
                        "Deleting node {} because it does not exist in the cloud provider",
                        node.metadata.name
                    ),
                )
                .await;

                if let Err(e) = self
                    .storage
                    .delete(&build_key("nodes", None, &node.metadata.name))
                    .await
                {
                    error!("unable to delete node {:?}: {e}", node.metadata.name);
                    self.event(
                        &node_ref,
                        EventType::Warning,
                        DELETE_NODE_FAILED_EVENT,
                        &format!("Failed deleting node {}: {e}", node.metadata.name),
                    )
                    .await;
                }
            } else {
                // Node exists. We need to check this to get taint working in
                // similar in all cloudproviders; current problem is that
                // shutdown nodes are not working in similar way ie. all
                // cloudproviders does not delete node from kubernetes cluster
                // when instance it is shutdown see issue #46442.
                let shutdown = match self.shutdown_in_cloud_provider(&node).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!(
                            "error checking if node {} is shutdown: {e}",
                            node.metadata.name
                        );
                        continue; // `shutdown && err == nil` is false
                    }
                };

                if shutdown {
                    // if node is shutdown add shutdown taint
                    if add_or_update_taint_on_node(
                        &*self.storage,
                        &node.metadata.name,
                        &[&shutdown_taint],
                    )
                    .await
                    .is_err()
                    {
                        error!(
                            "failed to apply shutdown taint to node {}, it may have been deleted.",
                            node.metadata.name
                        );
                    }
                }
            }
        }
    }

    async fn event(&self, node_ref: &ObjectReference, ty: EventType, reason: &str, msg: &str) {
        let source = EventSource {
            component: EVENT_SOURCE_COMPONENT.to_string(),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(node_ref, &source, ty, reason, msg)
            .await
        {
            error!("failed to record {reason} event: {e}");
        }
    }

    /// `getProviderID` (:200-221): returns the provider ID for the node. If
    /// the Node has no provider ID, it will be the one from the cloud
    /// provider.
    pub async fn get_provider_id(&self, node: &Node) -> Result<String, CloudError> {
        if let Some(id) = node.spec.as_ref().and_then(|s| s.provider_id.as_deref()) {
            if !id.is_empty() {
                return Ok(id.to_string());
            }
        }

        if let Some(instance_v2) = self.cloud.instances_v2() {
            let metadata = instance_v2.instance_metadata(node).await?;
            return Ok(metadata.provider_id);
        }

        get_instance_provider_id(&*self.cloud, &node.metadata.name).await
    }

    /// `shutdownInCloudProvider` (:224-247): true if the node is shutdown on
    /// the cloud provider.
    async fn shutdown_in_cloud_provider(&self, node: &Node) -> Result<bool, CloudError> {
        if let Some(instance_v2) = self.cloud.instances_v2() {
            return instance_v2.instance_shutdown(node).await;
        }

        let Some(instances) = self.cloud.instances() else {
            return Err(CloudError::other(
                "cloud provider does not support instances",
            ));
        };

        let provider_id = match self.get_provider_id(node).await {
            Ok(id) => id,
            Err(CloudError::InstanceNotFound) => return Ok(false),
            Err(e) => return Err(e),
        };

        match instances
            .instance_shutdown_by_provider_id(&provider_id)
            .await
        {
            Err(CloudError::NotImplemented) => Ok(false),
            other => other,
        }
    }

    /// `ensureNodeExistsByProviderID` (:250-270): checks if the instance
    /// exists by the provider id.
    async fn ensure_node_exists_by_provider_id(&self, node: &Node) -> Result<bool, CloudError> {
        if let Some(instance_v2) = self.cloud.instances_v2() {
            return instance_v2.instance_exists(node).await;
        }

        let Some(instances) = self.cloud.instances() else {
            return Err(CloudError::other(
                "instances interface not supported in the cloud provider",
            ));
        };

        let provider_id = match self.get_provider_id(node).await {
            Ok(id) => id,
            Err(CloudError::InstanceNotFound) => return Ok(false),
            Err(e) => return Err(e),
        };

        instances.instance_exists_by_provider_id(&provider_id).await
    }
}
