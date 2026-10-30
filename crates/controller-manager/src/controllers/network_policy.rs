use anyhow::Result;
use rusternetes_common::resources::{NetworkPolicy, Pod};
use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// NetworkPolicyController watches NetworkPolicy resources and coordinates with
/// the CNI plugin to enforce network policies.
///
/// Note: Actual enforcement is typically delegated to CNI plugins (Calico, Cilium, etc.)
/// that support NetworkPolicy. This controller:
/// 1. Validates NetworkPolicy resources
/// 2. Maintains policy state for CNI plugins to consume
/// 3. Provides status updates on policy application
///
/// For conformance testing, ensure a NetworkPolicy-capable CNI plugin is installed.
pub struct NetworkPolicyController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> NetworkPolicyController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting NetworkPolicy controller");

        let queue = WorkQueue::new();

        // ONE worker (#2517): kube-controller-manager has no NetworkPolicy controller
        // (enforcement is the CNI's: kube-router/Calico), so there is no upstream
        // worker count to port.
        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = rusternetes_storage::build_prefix("networkpolicies", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(std::time::Duration::from_secs(30));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            let storage_key = build_key("networkpolicies", Some(ns), name);
            match self.storage.get::<NetworkPolicy>(&storage_key).await {
                Ok(resource) => match self.reconcile_policy(&resource).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        error!("Failed to reconcile {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    // Resource was deleted — nothing to reconcile
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<NetworkPolicy>("/registry/networkpolicies/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("networkpolicies/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list networkpolicies for enqueue: {}", e);
            }
        }
    }

    /// Main reconciliation loop - processes all NetworkPolicies
    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting NetworkPolicy reconciliation");

        // List all NetworkPolicies across all namespaces
        let policies: Vec<NetworkPolicy> = self.storage.list("/registry/networkpolicies/").await?;

        debug!("Found {} network policies to reconcile", policies.len());

        for policy in policies {
            if let Err(e) = self.reconcile_policy(&policy).await {
                error!(
                    "Failed to reconcile network policy {}/{}: {}",
                    policy
                        .metadata
                        .namespace
                        .as_ref()
                        .unwrap_or(&"default".to_string()),
                    &policy.metadata.name,
                    e
                );
            }
        }

        Ok(())
    }

    /// Reconcile a single NetworkPolicy
    async fn reconcile_policy(&self, policy: &NetworkPolicy) -> Result<()> {
        let namespace = policy
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("NetworkPolicy has no namespace"))?;
        let policy_name = &policy.metadata.name;

        debug!("Reconciling network policy {}/{}", namespace, policy_name);

        // Validate the policy
        if let Err(e) = self.validate_policy(policy) {
            warn!(
                "NetworkPolicy {}/{} validation failed: {}",
                namespace, policy_name, e
            );
            return Ok(());
        }

        // Find all pods in the namespace that match the policy's pod selector
        let affected_pods = self.find_affected_pods(policy).await?;

        debug!(
            "NetworkPolicy {}/{} affects {} pods",
            namespace,
            policy_name,
            affected_pods.len()
        );

        // In a real implementation, this is where we would:
        // 1. Translate policy rules to CNI-specific format
        // 2. Call CNI plugin to apply rules
        // 3. Update policy status with application results
        //
        // For conformance, we rely on CNI plugins (Calico/Cilium) to:
        // - Watch NetworkPolicy resources from etcd/API server
        // - Translate policies to iptables/eBPF rules
        // - Enforce traffic filtering

        debug!(
            "NetworkPolicy {}/{} reconciled (enforcement delegated to CNI plugin)",
            namespace, policy_name
        );

        Ok(())
    }

    /// Validate NetworkPolicy resource
    pub fn validate_policy(&self, policy: &NetworkPolicy) -> Result<()> {
        // Validate pod selector exists
        let _pod_selector = &policy.spec.pod_selector;

        // Validate policy types if specified
        if let Some(policy_types) = &policy.spec.policy_types {
            for pt in policy_types {
                if pt != "Ingress" && pt != "Egress" {
                    return Err(anyhow::anyhow!(
                        "Invalid policy type '{}', must be 'Ingress' or 'Egress'",
                        pt
                    ));
                }
            }
        }

        // Validate ingress rules
        if let Some(ingress_rules) = &policy.spec.ingress {
            for (idx, rule) in ingress_rules.iter().enumerate() {
                self.validate_ingress_rule(rule, idx)?;
            }
        }

        // Validate egress rules
        if let Some(egress_rules) = &policy.spec.egress {
            for (idx, rule) in egress_rules.iter().enumerate() {
                self.validate_egress_rule(rule, idx)?;
            }
        }

        Ok(())
    }

    /// Validate ingress rule
    pub fn validate_ingress_rule(
        &self,
        rule: &rusternetes_common::resources::NetworkPolicyIngressRule,
        idx: usize,
    ) -> Result<()> {
        // Validate ports if specified
        if let Some(ports) = &rule.ports {
            for (port_idx, port) in ports.iter().enumerate() {
                self.validate_network_policy_port(port, idx, port_idx)?;
            }
        }

        // Validate peers if specified
        if let Some(peers) = &rule.from {
            for (peer_idx, peer) in peers.iter().enumerate() {
                self.validate_network_policy_peer(peer, idx, peer_idx)?;
            }
        }

        Ok(())
    }

    /// Validate egress rule
    fn validate_egress_rule(
        &self,
        rule: &rusternetes_common::resources::NetworkPolicyEgressRule,
        idx: usize,
    ) -> Result<()> {
        // Validate ports if specified
        if let Some(ports) = &rule.ports {
            for (port_idx, port) in ports.iter().enumerate() {
                self.validate_network_policy_port(port, idx, port_idx)?;
            }
        }

        // Validate peers if specified
        if let Some(peers) = &rule.to {
            for (peer_idx, peer) in peers.iter().enumerate() {
                self.validate_network_policy_peer(peer, idx, peer_idx)?;
            }
        }

        Ok(())
    }

    /// Validate NetworkPolicyPort
    pub fn validate_network_policy_port(
        &self,
        port: &rusternetes_common::resources::NetworkPolicyPort,
        rule_idx: usize,
        port_idx: usize,
    ) -> Result<()> {
        // Validate protocol
        let protocol = port.protocol.as_deref().unwrap_or("TCP");
        if protocol != "TCP" && protocol != "UDP" && protocol != "SCTP" {
            return Err(anyhow::anyhow!(
                "Invalid protocol '{}' in rule {} port {}, must be TCP, UDP, or SCTP",
                protocol,
                rule_idx,
                port_idx
            ));
        }

        // Validate end_port if specified
        if let Some(end_port) = port.end_port {
            if !(1..=65535).contains(&end_port) {
                return Err(anyhow::anyhow!(
                    "Invalid endPort {} in rule {} port {}, must be 1-65535",
                    end_port,
                    rule_idx,
                    port_idx
                ));
            }
        }

        Ok(())
    }

    /// Validate NetworkPolicyPeer
    fn validate_network_policy_peer(
        &self,
        peer: &rusternetes_common::resources::NetworkPolicyPeer,
        rule_idx: usize,
        peer_idx: usize,
    ) -> Result<()> {
        // At least one selector must be specified
        if peer.pod_selector.is_none()
            && peer.namespace_selector.is_none()
            && peer.ip_block.is_none()
        {
            return Err(anyhow::anyhow!(
                "NetworkPolicyPeer in rule {} peer {} must specify at least one of: podSelector, namespaceSelector, or ipBlock",
                rule_idx,
                peer_idx
            ));
        }

        // Validate IP block if specified
        if let Some(ip_block) = &peer.ip_block {
            // Basic CIDR validation (CNI plugin will do more thorough validation)
            if ip_block.cidr.is_empty() {
                return Err(anyhow::anyhow!(
                    "IPBlock CIDR cannot be empty in rule {} peer {}",
                    rule_idx,
                    peer_idx
                ));
            }

            // Validate except CIDRs
            if let Some(except) = &ip_block.except {
                for cidr in except {
                    if cidr.is_empty() {
                        return Err(anyhow::anyhow!(
                            "IPBlock except CIDR cannot be empty in rule {} peer {}",
                            rule_idx,
                            peer_idx
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Find all pods affected by this NetworkPolicy
    async fn find_affected_pods(&self, policy: &NetworkPolicy) -> Result<Vec<Pod>> {
        let namespace = policy
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("NetworkPolicy has no namespace"))?;

        // List all pods in the same namespace
        let pod_prefix = format!("/registry/pods/{}/", namespace);
        let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        // Filter pods that match the policy's pod selector
        let matching_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|pod| self.pod_matches_selector(pod, &policy.spec.pod_selector))
            .collect();

        Ok(matching_pods)
    }

    /// Whether `pod` is selected by a NetworkPolicy selector.
    ///
    /// One matcher, not a copy: `label_selector_as_selector`
    /// (`apimachinery/pkg/apis/meta/v1/helpers.go:36-72`) + `Selector::matches`.
    /// An empty `podSelector` selects every pod in the namespace, which is what
    /// `networking/v1`'s own doc says, and it does so including pods with no
    /// labels — the hand-rolled version answered a label-less pod from
    /// `matchLabels` alone and ignored `matchExpressions` entirely, so a
    /// `DoesNotExist` term never selected one (#2012).
    pub fn pod_matches_selector(
        &self,
        pod: &Pod,
        selector: &rusternetes_common::types::LabelSelector,
    ) -> bool {
        rusternetes_common::types::label_selector_as_selector(Some(selector))
            .map(|selector| selector.matches(pod.metadata.labels.as_ref()))
            .unwrap_or(false)
    }
}
