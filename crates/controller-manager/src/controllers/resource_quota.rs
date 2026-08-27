use anyhow::Result;
use chrono::Utc;
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::quota;
use rusternetes_common::resources::{Pod, ResourceQuota, ResourceQuotaStatus, Service};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

/// Compute keys the pod evaluator tracks, each defaulted to zero before the
/// per-pod sum so `status.used` reports `"0"` rather than omitting the key.
///
/// Upstream seeds the same set from the quota's own hard keys — `Resources` in
/// `UsageStatsOptions`, defaulted in `generic.CalculateUsageStats` — and then
/// masks the result back down to them. The mask in `reconcile_quota` does the
/// trimming here, so this list only has to cover what the evaluator can charge.
const QUOTA_COMPUTE_KEYS: &[&str] = &[
    "pods",
    "count/pods",
    "cpu",
    "requests.cpu",
    "limits.cpu",
    "memory",
    "requests.memory",
    "limits.memory",
    "ephemeral-storage",
    "requests.ephemeral-storage",
    "limits.ephemeral-storage",
];

/// Poll one *auxiliary* watch stream, retiring it when it ends or errors.
///
/// `WatchStream` is built on `futures::stream::unfold`, which **panics** when
/// polled after it has returned `Poll::Ready(None)`:
///
/// ```text
/// thread 'tokio-rt-worker' panicked at futures-util/src/stream/unfold.rs:108:21:
/// Unfold must not be polled after it returned `Poll::Ready(None)`
/// ```
///
/// The usage-driving watches (pods, services, configmaps, secrets, PVCs) live in
/// the same `select!` as the ResourceQuota watch. Matching only `Some(Ok(ev))`
/// and ignoring `None` left the arm enabled, so the next loop iteration polled an
/// exhausted stream and panicked — killing the whole controller task, because
/// `tokio::spawn` swallows panics. Quota `status.used` then stopped being
/// published until the controller-manager was restarted (#1775; the panic was
/// observed live twice, each time in the burst of "Watch stream ended,
/// reconnecting" that follows an api-server disconnect).
///
/// Taking the stream out of the `Option` on termination is what makes the arm's
/// `if aux.is_some()` guard disable it — the same "resync/reconnect rather than
/// re-poll a dead stream" property upstream gets from a reflector's
/// ListAndWatch loop (`k8s.io/client-go/tools/cache/reflector.go`).
///
/// Returns `Some(event)` while the stream is alive, `None` once it has been
/// retired (the caller should reconnect).
async fn next_aux_event(
    aux: &mut Option<rusternetes_storage::WatchStream>,
    what: &str,
) -> Option<rusternetes_storage::WatchEvent> {
    use futures::StreamExt;

    let stream = aux.as_mut()?;
    match stream.next().await {
        Some(Ok(ev)) => Some(ev),
        Some(Err(e)) => {
            tracing::warn!(
                "{} watch error: {} — retiring the stream, will reconnect",
                what,
                e
            );
            *aux = None;
            None
        }
        None => {
            tracing::warn!(
                "{} watch stream ended — retiring the stream, will reconnect",
                what
            );
            *aux = None;
            None
        }
    }
}

/// ResourceQuotaController tracks resource usage per namespace and enforces quota limits.
/// It:
/// 1. Watches ResourceQuotas across all namespaces
/// 2. Calculates current resource usage (pods, cpu, memory, etc.)
/// 3. Updates ResourceQuota status with used vs hard limits
pub struct ResourceQuotaController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> ResourceQuotaController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting ResourceQuota controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("resourcequotas", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            // Also watch resources whose lifecycle drives quota usage. When a
            // pod (or other tracked resource) is created/updated/deleted we
            // re-enqueue every quota in the affected namespace so status.used
            // reflects the change quickly — without this, usage only refreshes
            // on the 30s resync, which is what conformance tests probing the
            // ResourceQuota lifecycle (pod create/delete → used updated) hit.
            let mut pod_watch = self.storage.watch(&build_prefix("pods", None)).await.ok();
            let mut svc_watch = self
                .storage
                .watch(&build_prefix("services", None))
                .await
                .ok();
            let mut cm_watch = self
                .storage
                .watch(&build_prefix("configmaps", None))
                .await
                .ok();
            let mut secret_watch = self
                .storage
                .watch(&build_prefix("secrets", None))
                .await
                .ok();
            let mut pvc_watch = self
                .storage
                .watch(&build_prefix("persistentvolumeclaims", None))
                .await
                .ok();

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
                                tracing::warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    event = next_aux_event(&mut pod_watch, "Pod"), if pod_watch.is_some() => {
                        match event {
                            Some(ev) => self.enqueue_quotas_for_event(&queue, &ev).await,
                            // Stream retired inside next_aux_event; reconnect all
                            // watches rather than run on with a dead one.
                            None => watch_broken = true,
                        }
                    }
                    event = next_aux_event(&mut svc_watch, "Service"), if svc_watch.is_some() => {
                        match event {
                            Some(ev) => self.enqueue_quotas_for_event(&queue, &ev).await,
                            // Stream retired inside next_aux_event; reconnect all
                            // watches rather than run on with a dead one.
                            None => watch_broken = true,
                        }
                    }
                    event = next_aux_event(&mut cm_watch, "ConfigMap"), if cm_watch.is_some() => {
                        match event {
                            Some(ev) => self.enqueue_quotas_for_event(&queue, &ev).await,
                            // Stream retired inside next_aux_event; reconnect all
                            // watches rather than run on with a dead one.
                            None => watch_broken = true,
                        }
                    }
                    event = next_aux_event(&mut secret_watch, "Secret"), if secret_watch.is_some() => {
                        match event {
                            Some(ev) => self.enqueue_quotas_for_event(&queue, &ev).await,
                            // Stream retired inside next_aux_event; reconnect all
                            // watches rather than run on with a dead one.
                            None => watch_broken = true,
                        }
                    }
                    event = next_aux_event(&mut pvc_watch, "PersistentVolumeClaim"), if pvc_watch.is_some() => {
                        match event {
                            Some(ev) => self.enqueue_quotas_for_event(&queue, &ev).await,
                            // Stream retired inside next_aux_event; reconnect all
                            // watches rather than run on with a dead one.
                            None => watch_broken = true,
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }

    /// Extract the namespace from a watched resource's storage key and
    /// enqueue every ResourceQuota in that namespace for reconciliation.
    async fn enqueue_quotas_for_event(
        &self,
        queue: &WorkQueue,
        event: &rusternetes_storage::WatchEvent,
    ) {
        let key = extract_key(event);
        // Key format: "{resource_type}/{namespace}/{name}"
        let parts: Vec<&str> = key.splitn(3, '/').collect();
        let ns = match parts.get(1) {
            Some(ns) => *ns,
            None => return,
        };
        if let Ok(quotas) = self
            .storage
            .list::<ResourceQuota>(&build_prefix("resourcequotas", Some(ns)))
            .await
        {
            for q in &quotas {
                queue
                    .add(format!("resourcequotas/{}/{}", ns, q.metadata.name))
                    .await;
            }
        }
    }

    /// Main reconciliation loop - syncs all resource quotas
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
            let storage_key = build_key("resourcequotas", Some(ns), name);
            match self.storage.get::<ResourceQuota>(&storage_key).await {
                Ok(resource) => match self.reconcile_quota(&resource).await {
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
            .list::<ResourceQuota>("/registry/resourcequotas/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("resourcequotas/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list resourcequotas for enqueue: {}", e);
            }
        }
    }

    /// Reconcile a single ResourceQuota by namespace + name.
    ///
    /// Re-reads the quota from storage (so the recompute always sees fresh
    /// state, including the deletion of tracked objects since the last
    /// reconcile) and recomputes `.status.used`. This is the per-key entry
    /// point used by the controller's worker loop and by integration tests.
    ///
    /// `#[allow(dead_code)]` — only reachable from the downstream
    /// integration-test crate (`tests/resource_quota_usage_recompute_test.rs`)
    /// and the controller-manager binary's worker loop, neither of which
    /// the lib-target dead-code analysis sees.
    #[allow(dead_code)]
    pub async fn reconcile_one(&self, namespace: &str, name: &str) -> Result<()> {
        let key = build_key("resourcequotas", Some(namespace), name);
        let quota: ResourceQuota = self.storage.get(&key).await?;
        self.reconcile_quota(&quota).await
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting resource quota reconciliation");

        // List all resource quotas across all namespaces
        let quotas: Vec<ResourceQuota> = self
            .storage
            .list(&build_prefix("resourcequotas", None))
            .await?;

        for quota in quotas {
            if let Err(e) = self.reconcile_quota(&quota).await {
                error!(
                    "Failed to reconcile quota {}/{}: {}",
                    quota
                        .metadata
                        .namespace
                        .as_ref()
                        .unwrap_or(&"default".to_string()),
                    &quota.metadata.name,
                    e
                );
            }
        }

        Ok(())
    }

    /// Reconcile a single resource quota
    async fn reconcile_quota(&self, quota: &ResourceQuota) -> Result<()> {
        let namespace = quota
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ResourceQuota has no namespace"))?;
        let quota_name = &quota.metadata.name;

        debug!("Reconciling quota {}/{}", namespace, quota_name);

        // Collect the set of resource names tracked by this quota
        let hard_keys: Vec<String> = quota
            .spec
            .hard
            .as_ref()
            .map(|h| h.keys().cloned().collect())
            .unwrap_or_default();

        // Calculate current resource usage in the namespace, respecting scopes
        let scopes = quota.spec.scopes.as_deref().unwrap_or(&[]);
        let scope_selector = quota.spec.scope_selector.as_ref();
        let mut used = self
            .calculate_usage(namespace, scopes, scope_selector, &hard_keys)
            .await?;

        // `status.used` reports exactly the quota's own hard keys: every one of
        // them (defaulted to "0" when nothing charges it) and nothing else.
        // Upstream does both — the zero default in `generic.CalculateUsageStats`
        // and `used = quota.Mask(used, hardResources)` in `syncResourceQuota` —
        // so a key the evaluator happens to compute but the quota does not
        // constrain must not leak into the status.
        if let Some(hard) = &quota.spec.hard {
            for key in hard.keys() {
                used.entry(key.clone()).or_insert_with(|| "0".to_string());
            }
            used.retain(|key, _| hard.contains_key(key));
        }

        // Build the desired status
        let new_status = Some(ResourceQuotaStatus {
            hard: quota.spec.hard.clone(),
            used: Some(used),
        });

        // Only write if status actually changed to avoid unnecessary storage writes
        // that cause resourceVersion conflicts with concurrent test PATCH operations
        if quota.status != new_status {
            let key = build_key("resourcequotas", Some(namespace), quota_name);
            // Write the status SUBRESOURCE only. `update_status` re-reads the
            // current object and grafts just `.status` onto it under a CAS
            // retry, so this reconcile — which computed status from a possibly
            // stale list snapshot — can never write a stale spec back. Writing
            // the whole object here would revert a spec the client just updated
            // (the ResourceQuota update+delete conformance flake, #268).
            let mut desired = quota.clone();
            desired.status = new_status;
            match self.storage.update_status(&key, &desired).await {
                Ok(_) => debug!("Updated quota {}/{} status", namespace, quota_name),
                // Deleted concurrently (e.g. a DeleteCollection racing this
                // reconcile): nothing to update, and update_status never
                // re-creates, so the quota stays deleted (no resurrection).
                Err(rusternetes_common::Error::NotFound(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    /// Check if a pod is BestEffort QoS class — the `BestEffort` ResourceQuota
    /// scope.
    ///
    /// Port of upstream `isBestEffort`
    /// (`pkg/quota/v1/evaluator/core/pods.go:412-414`):
    /// `qos.GetPodQOS(pod) == corev1.PodQOSBestEffort`. The quota controller and
    /// the api-server's quota admission
    /// (`crates/api-server/src/admission.rs`) must agree on which pods a
    /// `BestEffort`-scoped quota covers, or admission charges a pod the
    /// controller then does not count.
    fn is_pod_best_effort(pod: &Pod) -> bool {
        rusternetes_common::qos::get_pod_qos(pod) == rusternetes_common::qos::QoSClass::BestEffort
    }

    /// Check if a pod matches the given scopes
    fn pod_matches_scopes(
        pod: &Pod,
        scopes: &[String],
        scope_selector: Option<&rusternetes_common::resources::ScopeSelector>,
    ) -> bool {
        let is_terminating = pod.metadata.deletion_timestamp.is_some()
            || pod
                .spec
                .as_ref()
                .and_then(|s| s.active_deadline_seconds)
                .is_some();
        let is_best_effort = Self::is_pod_best_effort(pod);

        // All scopes must match (AND logic)
        for scope in scopes {
            match scope.as_str() {
                "Terminating" if !is_terminating => {
                    return false;
                }
                "NotTerminating" if is_terminating => {
                    return false;
                }
                "BestEffort" if !is_best_effort => {
                    return false;
                }
                "NotBestEffort" if is_best_effort => {
                    return false;
                }
                _ => {}
            }
        }

        // Check scopeSelector if present (all match expressions must match, AND logic)
        if let Some(selector) = scope_selector {
            for req in &selector.match_expressions {
                match req.scope_name.as_str() {
                    "Terminating" => {
                        let matches = match req.operator.as_str() {
                            "Exists" => is_terminating,
                            "DoesNotExist" => !is_terminating,
                            _ => true,
                        };
                        if !matches {
                            return false;
                        }
                    }
                    "NotTerminating" => {
                        let matches = match req.operator.as_str() {
                            "Exists" => !is_terminating,
                            "DoesNotExist" => is_terminating,
                            _ => true,
                        };
                        if !matches {
                            return false;
                        }
                    }
                    "BestEffort" => {
                        let matches = match req.operator.as_str() {
                            "Exists" => is_best_effort,
                            "DoesNotExist" => !is_best_effort,
                            _ => true,
                        };
                        if !matches {
                            return false;
                        }
                    }
                    "NotBestEffort" => {
                        let matches = match req.operator.as_str() {
                            "Exists" => !is_best_effort,
                            "DoesNotExist" => is_best_effort,
                            _ => true,
                        };
                        if !matches {
                            return false;
                        }
                    }
                    "PriorityClass" => {
                        let pod_priority_class = pod
                            .spec
                            .as_ref()
                            .and_then(|s| s.priority_class_name.as_deref())
                            .unwrap_or("");
                        let matches = match req.operator.as_str() {
                            "In" => req
                                .values
                                .as_ref()
                                .is_some_and(|v| v.iter().any(|val| val == pod_priority_class)),
                            "NotIn" => req
                                .values
                                .as_ref()
                                .is_none_or(|v| !v.iter().any(|val| val == pod_priority_class)),
                            "Exists" => !pod_priority_class.is_empty(),
                            "DoesNotExist" => pod_priority_class.is_empty(),
                            _ => true,
                        };
                        if !matches {
                            return false;
                        }
                    }
                    _ => {}
                }
            }
        }

        true
    }

    /// Calculate resource usage in a namespace, respecting quota scopes.
    /// Only counts resources that appear in `hard_keys` to avoid unnecessary work.
    async fn calculate_usage(
        &self,
        namespace: &str,
        scopes: &[String],
        scope_selector: Option<&rusternetes_common::resources::ScopeSelector>,
        hard_keys: &[String],
    ) -> Result<HashMap<String, String>> {
        let mut usage = HashMap::new();

        // Determine which resource categories we need
        let needs_pods = hard_keys.iter().any(|k| {
            k == "pods"
                || k.starts_with("requests.")
                || k.starts_with("limits.")
                || k == "cpu"
                || k == "memory"
                || k.starts_with("count/pods")
        });
        let needs_services = hard_keys.iter().any(|k| {
            k == "services"
                || k == "count/services"
                || k == "services.nodeports"
                || k == "services.loadbalancers"
        });
        let needs_configmaps = hard_keys
            .iter()
            .any(|k| k == "configmaps" || k == "count/configmaps");
        let needs_secrets = hard_keys
            .iter()
            .any(|k| k == "secrets" || k == "count/secrets");
        let needs_replicasets = hard_keys.iter().any(|k| {
            k == "count/replicasets" || k == "count/replicasets.apps" || k == "replicasets"
        });
        let needs_pvcs = hard_keys
            .iter()
            .any(|k| k == "persistentvolumeclaims" || k == "count/persistentvolumeclaims");
        let needs_rcs = hard_keys
            .iter()
            .any(|k| k == "replicationcontrollers" || k == "count/replicationcontrollers");
        let needs_rqs = hard_keys
            .iter()
            .any(|k| k == "resourcequotas" || k == "count/resourcequotas");

        // Sum the pod evaluator's usage over the scope-matched pods.
        if needs_pods {
            // Sum each scope-matched pod's quota footprint as a `ResourceList`
            // of `Quantity`. Upstream `generic.CalculateUsageStats` does exactly
            // this — filter by scope, then `quota.Add` each pod's `Usage` —
            // and never reduces to an integer in between
            // (`staging/src/k8s.io/apiserver/pkg/quota/v1/generic/evaluator.go`).
            //
            // `pod_usage` decides both which keys a pod is charged under
            // (`podComputeUsageHelper`) and whether the compute keys are
            // charged at all (`QuotaV1Pod`): a terminal pod contributes only
            // `count/pods`, and a *terminating* pod keeps its charge until its
            // deletion grace period elapses.
            let pod_prefix = format!("/registry/pods/{}/", namespace);
            let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;
            let now = Utc::now();
            let mut totals = quota::ResourceList::new();
            for pod in all_pods
                .iter()
                .filter(|p| Self::pod_matches_scopes(p, scopes, scope_selector))
            {
                totals = quota::add(&totals, &quota::pod_usage(pod, now));
            }

            // Default every compute key this evaluator tracks to zero, matching
            // upstream `CalculateUsageStats`, which seeds `result.Used` with a
            // zero `Quantity` for each requested resource before summing
            // (`generic/evaluator.go`). `status.used` must report `"0"` for a
            // hard key with no usage, not omit it.
            let zero = Quantity::from_value(0, Format::DecimalSI);
            for key in QUOTA_COMPUTE_KEYS {
                totals.entry((*key).to_string()).or_insert(zero);
            }

            usage.extend(quota::to_string_map(&totals));
        }

        // Count services and service subtypes
        if needs_services {
            let svc_prefix = format!("/registry/services/{}/", namespace);
            let services: Vec<Service> = self.storage.list(&svc_prefix).await.unwrap_or_default();
            usage.insert("count/services".to_string(), services.len().to_string());
            usage.insert("services".to_string(), services.len().to_string());

            // Count NodePort-consuming services. Upstream
            // `pkg/quota/v1/evaluator/core/services.go` counts the node-port slot for:
            //   * Service type=NodePort       — always
            //   * Service type=LoadBalancer   — only when allocateLoadBalancerNodePorts
            //                                   is nil or true; an explicit `false`
            //                                   means the LB has no node ports to count.
            let nodeport_count = services
                .iter()
                .filter(|s| match s.spec.service_type {
                    Some(rusternetes_common::resources::ServiceType::NodePort) => true,
                    Some(rusternetes_common::resources::ServiceType::LoadBalancer) => {
                        s.spec.allocate_load_balancer_node_ports.unwrap_or(true)
                    }
                    _ => false,
                })
                .count();
            usage.insert("services.nodeports".to_string(), nodeport_count.to_string());

            // Count LoadBalancer services
            let lb_count = services
                .iter()
                .filter(|s| {
                    matches!(
                        s.spec.service_type,
                        Some(rusternetes_common::resources::ServiceType::LoadBalancer)
                    )
                })
                .count();
            usage.insert("services.loadbalancers".to_string(), lb_count.to_string());
        }

        if needs_configmaps {
            let count_prefix = format!("/registry/configmaps/{}/", namespace);
            let configmaps: Vec<serde_json::Value> =
                self.storage.list(&count_prefix).await.unwrap_or_default();
            usage.insert("count/configmaps".to_string(), configmaps.len().to_string());
            usage.insert("configmaps".to_string(), configmaps.len().to_string());
        }

        if needs_secrets {
            let secret_prefix = format!("/registry/secrets/{}/", namespace);
            let secrets: Vec<serde_json::Value> =
                self.storage.list(&secret_prefix).await.unwrap_or_default();
            usage.insert("count/secrets".to_string(), secrets.len().to_string());
            usage.insert("secrets".to_string(), secrets.len().to_string());
        }

        if needs_replicasets {
            let rs_prefix = format!("/registry/replicasets/{}/", namespace);
            let replicasets: Vec<serde_json::Value> =
                self.storage.list(&rs_prefix).await.unwrap_or_default();
            let rs_count = replicasets.len().to_string();
            usage.insert("count/replicasets".to_string(), rs_count.clone());
            usage.insert("count/replicasets.apps".to_string(), rs_count.clone());
            usage.insert("replicasets".to_string(), rs_count);
        }

        if needs_pvcs {
            let pvc_prefix = format!("/registry/persistentvolumeclaims/{}/", namespace);
            let pvcs: Vec<serde_json::Value> =
                self.storage.list(&pvc_prefix).await.unwrap_or_default();
            usage.insert("persistentvolumeclaims".to_string(), pvcs.len().to_string());
            usage.insert(
                "count/persistentvolumeclaims".to_string(),
                pvcs.len().to_string(),
            );
        }

        if needs_rcs {
            let rc_prefix = format!("/registry/replicationcontrollers/{}/", namespace);
            let rcs: Vec<serde_json::Value> =
                self.storage.list(&rc_prefix).await.unwrap_or_default();
            usage.insert("replicationcontrollers".to_string(), rcs.len().to_string());
            usage.insert(
                "count/replicationcontrollers".to_string(),
                rcs.len().to_string(),
            );
        }

        if needs_rqs {
            let rq_prefix = format!("/registry/resourcequotas/{}/", namespace);
            let rqs: Vec<serde_json::Value> =
                self.storage.list(&rq_prefix).await.unwrap_or_default();
            usage.insert("resourcequotas".to_string(), rqs.len().to_string());
            usage.insert("count/resourcequotas".to_string(), rqs.len().to_string());
        }

        Ok(usage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        Container, PodSpec, ResourceQuotaSpec, ScopeSelector, ScopedResourceSelectorRequirement,
    };
    use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
    use rusternetes_storage::memory::MemoryStorage;

    fn make_container(name: &str, resources: Option<ResourceRequirements>) -> Container {
        Container {
            name: name.to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            env_from: None,
            resources,
            volume_mounts: None,
            volume_devices: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            image_pull_policy: None,
            security_context: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            resize_policy: None,
            restart_policy: None,
            ..Default::default()
        }
    }

    fn make_pod(name: &str, namespace: &str, resources: Option<ResourceRequirements>) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec: Some(PodSpec {
                containers: vec![make_container("test", resources)],
                ..Default::default()
            }),
            status: None,
        }
    }

    fn make_pod_with_deadline(name: &str, namespace: &str, active_deadline: Option<i64>) -> Pod {
        let mut pod = make_pod(name, namespace, None);
        if let Some(spec) = &mut pod.spec {
            spec.active_deadline_seconds = active_deadline;
        }
        pod
    }

    #[test]
    fn test_is_pod_best_effort() {
        // Pod with no resources is BestEffort
        let pod = make_pod("test", "default", None);
        assert!(ResourceQuotaController::<MemoryStorage>::is_pod_best_effort(&pod));

        // Pod with empty resources is BestEffort
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: None,
                limits: None,
                claims: None,
            }),
        );
        assert!(ResourceQuotaController::<MemoryStorage>::is_pod_best_effort(&pod));

        // Pod with empty maps is BestEffort
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: Some(HashMap::new()),
                limits: Some(HashMap::new()),
                claims: None,
            }),
        );
        assert!(ResourceQuotaController::<MemoryStorage>::is_pod_best_effort(&pod));

        // Pod with CPU request is NOT BestEffort
        let mut reqs = HashMap::new();
        reqs.insert("cpu".to_string(), "100m".to_string());
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: Some(reqs),
                limits: None,
                claims: None,
            }),
        );
        assert!(!ResourceQuotaController::<MemoryStorage>::is_pod_best_effort(&pod));

        // Pod with only limits is NOT BestEffort
        let mut limits = HashMap::new();
        limits.insert("memory".to_string(), "128Mi".to_string());
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: None,
                limits: Some(limits),
                claims: None,
            }),
        );
        assert!(!ResourceQuotaController::<MemoryStorage>::is_pod_best_effort(&pod));
    }

    #[test]
    fn test_pod_matches_scopes_terminating() {
        // Pod with active_deadline_seconds is Terminating
        let pod = make_pod_with_deadline("test", "default", Some(30));
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["Terminating".to_string()],
                None
            )
        );
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["NotTerminating".to_string()],
                None
            )
        );

        // Pod without active_deadline_seconds is NotTerminating
        let pod = make_pod_with_deadline("test", "default", None);
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["Terminating".to_string()],
                None
            )
        );
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["NotTerminating".to_string()],
                None
            )
        );
    }

    #[test]
    fn test_pod_matches_scopes_best_effort() {
        // Pod with no resources = BestEffort
        let pod = make_pod("test", "default", None);
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["BestEffort".to_string()],
                None
            )
        );
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["NotBestEffort".to_string()],
                None
            )
        );

        // Pod with resources = NotBestEffort
        let mut reqs = HashMap::new();
        reqs.insert("cpu".to_string(), "100m".to_string());
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: Some(reqs),
                limits: None,
                claims: None,
            }),
        );
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["BestEffort".to_string()],
                None
            )
        );
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["NotBestEffort".to_string()],
                None
            )
        );
    }

    #[test]
    fn test_pod_matches_scope_selector() {
        let pod = make_pod_with_deadline("test", "default", Some(30));
        let selector = ScopeSelector {
            match_expressions: vec![ScopedResourceSelectorRequirement {
                scope_name: "Terminating".to_string(),
                operator: "Exists".to_string(),
                values: None,
            }],
        };
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &[],
                Some(&selector)
            )
        );

        let selector_not = ScopeSelector {
            match_expressions: vec![ScopedResourceSelectorRequirement {
                scope_name: "Terminating".to_string(),
                operator: "DoesNotExist".to_string(),
                values: None,
            }],
        };
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &[],
                Some(&selector_not)
            )
        );
    }

    #[tokio::test]
    async fn test_calculate_usage_with_scopes() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        // Create a BestEffort pod (no resources)
        let pod1 = make_pod("be-pod", "test-ns", None);
        storage
            .create("/registry/pods/test-ns/be-pod", &pod1)
            .await
            .unwrap();

        // Create a NotBestEffort pod (has resources)
        let mut reqs = HashMap::new();
        reqs.insert("cpu".to_string(), "100m".to_string());
        let pod2 = make_pod(
            "nbe-pod",
            "test-ns",
            Some(ResourceRequirements {
                requests: Some(reqs),
                limits: None,
                claims: None,
            }),
        );
        storage
            .create("/registry/pods/test-ns/nbe-pod", &pod2)
            .await
            .unwrap();

        // BestEffort scope should count only the BE pod
        let usage = controller
            .calculate_usage(
                "test-ns",
                &["BestEffort".to_string()],
                None,
                &["pods".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(usage.get("pods").unwrap(), "1");

        // NotBestEffort scope should count only the NBE pod
        let usage = controller
            .calculate_usage(
                "test-ns",
                &["NotBestEffort".to_string()],
                None,
                &["pods".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(usage.get("pods").unwrap(), "1");

        // No scope should count both
        let usage = controller
            .calculate_usage("test-ns", &[], None, &["pods".to_string()])
            .await
            .unwrap();
        assert_eq!(usage.get("pods").unwrap(), "2");
    }

    #[tokio::test]
    async fn test_reconcile_quota_sets_status_used() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        // Create a quota
        let mut hard = HashMap::new();
        hard.insert("pods".to_string(), "10".to_string());
        hard.insert("requests.cpu".to_string(), "4".to_string());
        let quota = ResourceQuota {
            type_meta: TypeMeta {
                kind: "ResourceQuota".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-quota").with_namespace("test-ns"),
            spec: ResourceQuotaSpec {
                hard: Some(hard),
                scopes: None,
                scope_selector: None,
            },
            status: None,
        };
        storage
            .create("/registry/resourcequotas/test-ns/test-quota", &quota)
            .await
            .unwrap();

        // Reconcile
        controller.reconcile_all().await.unwrap();

        // Check status was set
        let updated: ResourceQuota = storage
            .get("/registry/resourcequotas/test-ns/test-quota")
            .await
            .unwrap();
        let status = updated.status.unwrap();
        assert!(status.hard.is_some());
        assert!(status.used.is_some());
        let used = status.used.unwrap();
        assert_eq!(used.get("pods").unwrap(), "0");
        // `NewMilliQuantity(0, DecimalSI).String()` is "0" — upstream's
        // `CanonicalizeBytes` short-circuits on `IsZero` (`quantity.go:426`),
        // so a zero cpu usage never carries a suffix. Was "0m".
        assert_eq!(used.get("requests.cpu").unwrap(), "0");
    }

    #[tokio::test]
    async fn test_enqueue_quotas_for_event_reacts_to_pod_change() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        // Two quotas in the same namespace should both be enqueued
        let mut hard = HashMap::new();
        hard.insert("pods".to_string(), "10".to_string());
        for name in ["quota-a", "quota-b"] {
            let q = ResourceQuota {
                type_meta: TypeMeta {
                    kind: "ResourceQuota".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: ObjectMeta::new(name).with_namespace("ns-1"),
                spec: ResourceQuotaSpec {
                    hard: Some(hard.clone()),
                    scopes: None,
                    scope_selector: None,
                },
                status: None,
            };
            storage
                .create(&format!("/registry/resourcequotas/ns-1/{}", name), &q)
                .await
                .unwrap();
        }
        // And one in a different namespace that should NOT be enqueued
        let other = ResourceQuota {
            type_meta: TypeMeta {
                kind: "ResourceQuota".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("quota-x").with_namespace("ns-2"),
            spec: ResourceQuotaSpec {
                hard: Some(hard),
                scopes: None,
                scope_selector: None,
            },
            status: None,
        };
        storage
            .create("/registry/resourcequotas/ns-2/quota-x", &other)
            .await
            .unwrap();

        let queue = WorkQueue::new();
        // Simulate a pod create event in ns-1
        let ev = rusternetes_storage::WatchEvent::Added(
            "/registry/pods/ns-1/some-pod".to_string(),
            "{}".to_string(),
        );
        controller.enqueue_quotas_for_event(&queue, &ev).await;

        // Both ns-1 quotas should be queued, ns-2 should not
        assert_eq!(queue.len().await, 2);
        let k1 = queue.get().await.unwrap();
        let k2 = queue.get().await.unwrap();
        let mut keys = vec![k1, k2];
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "resourcequotas/ns-1/quota-a".to_string(),
                "resourcequotas/ns-1/quota-b".to_string(),
            ]
        );
    }

    /// `status.used` for an extended resource used to go through
    /// `qty.parse::<i64>().unwrap_or(0)`, so any quantity carrying a suffix —
    /// `"2k"`, `"1Ki"` — reported as `0` and the dimension went unaccounted.
    #[tokio::test]
    async fn test_calculate_usage_extended_resource_with_suffix() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        let mut reqs = HashMap::new();
        reqs.insert("example.com/dongle".to_string(), "2k".to_string());
        let pod = make_pod(
            "dongle-pod",
            "test-ns",
            Some(ResourceRequirements {
                requests: Some(reqs),
                limits: None,
                claims: None,
            }),
        );
        storage
            .create("/registry/pods/test-ns/dongle-pod", &pod)
            .await
            .unwrap();

        let usage = controller
            .calculate_usage(
                "test-ns",
                &[],
                None,
                &["requests.example.com/dongle".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(
            usage.get("requests.example.com/dongle").map(String::as_str),
            Some("2k")
        );
        // Upstream charges an extended resource only under `requests.<name>`
        // (`podComputeUsageHelper`, `pods.go:324-328`).
        assert!(!usage.contains_key("example.com/dongle"));
    }

    /// A pod's peak footprint includes its init containers: upstream
    /// `PodRequests` takes `max(sum(containers), max over init containers)`.
    /// Summing only `spec.containers` charged the 1Gi app container and ignored
    /// the 4Gi init container entirely.
    #[tokio::test]
    async fn test_calculate_usage_charges_init_container_peak() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        let mut app_reqs = HashMap::new();
        app_reqs.insert("memory".to_string(), "1Gi".to_string());
        let mut init_reqs = HashMap::new();
        init_reqs.insert("memory".to_string(), "4Gi".to_string());

        let mut pod = make_pod(
            "init-heavy",
            "test-ns",
            Some(ResourceRequirements {
                requests: Some(app_reqs),
                limits: None,
                claims: None,
            }),
        );
        pod.spec.as_mut().unwrap().init_containers = Some(vec![make_container(
            "init",
            Some(ResourceRequirements {
                requests: Some(init_reqs),
                limits: None,
                claims: None,
            }),
        )]);
        storage
            .create("/registry/pods/test-ns/init-heavy", &pod)
            .await
            .unwrap();

        let usage = controller
            .calculate_usage("test-ns", &[], None, &["requests.memory".to_string()])
            .await
            .unwrap();
        assert_eq!(
            usage.get("requests.memory").map(String::as_str),
            Some("4Gi")
        );
    }

    /// A fractional memory request must not round to zero, and the total must
    /// come back in canonical form rather than raw bytes.
    #[tokio::test]
    async fn test_calculate_usage_fractional_memory() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        for name in ["a", "b"] {
            let mut reqs = HashMap::new();
            reqs.insert("memory".to_string(), "0.5Gi".to_string());
            let pod = make_pod(
                name,
                "test-ns",
                Some(ResourceRequirements {
                    requests: Some(reqs),
                    limits: None,
                    claims: None,
                }),
            );
            storage
                .create(&format!("/registry/pods/test-ns/{name}"), &pod)
                .await
                .unwrap();
        }

        let usage = controller
            .calculate_usage("test-ns", &[], None, &["requests.memory".to_string()])
            .await
            .unwrap();
        assert_eq!(
            usage.get("requests.memory").map(String::as_str),
            Some("1Gi")
        );
    }

    /// Upstream charges a *terminating* pod until its deletion grace period has
    /// elapsed (`QuotaV1Pod`, `pods.go:499-506`); only a terminal phase drops
    /// the compute charge outright. `count/pods` is charged either way.
    #[tokio::test]
    async fn test_calculate_usage_terminal_and_terminating_pods() {
        use rusternetes_common::resources::PodStatus;
        use rusternetes_common::types::Phase;

        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        let with_cpu = || {
            let mut reqs = HashMap::new();
            reqs.insert("cpu".to_string(), "100m".to_string());
            Some(ResourceRequirements {
                requests: Some(reqs),
                limits: None,
                claims: None,
            })
        };

        let running = make_pod("running", "test-ns", with_cpu());
        storage
            .create("/registry/pods/test-ns/running", &running)
            .await
            .unwrap();

        let mut succeeded = make_pod("succeeded", "test-ns", with_cpu());
        succeeded.status = Some(PodStatus {
            phase: Some(Phase::Succeeded),
            ..Default::default()
        });
        storage
            .create("/registry/pods/test-ns/succeeded", &succeeded)
            .await
            .unwrap();

        let mut terminating = make_pod("terminating", "test-ns", with_cpu());
        terminating.metadata.deletion_timestamp = Some(chrono::Utc::now());
        terminating.metadata.deletion_grace_period_seconds = Some(3600);
        storage
            .create("/registry/pods/test-ns/terminating", &terminating)
            .await
            .unwrap();

        let usage = controller
            .calculate_usage(
                "test-ns",
                &[],
                None,
                &[
                    "pods".to_string(),
                    "count/pods".to_string(),
                    "requests.cpu".to_string(),
                ],
            )
            .await
            .unwrap();

        // running + terminating (still inside its grace period)
        assert_eq!(usage.get("pods").map(String::as_str), Some("2"));
        assert_eq!(usage.get("requests.cpu").map(String::as_str), Some("200m"));
        // object-count quota tracks everything in storage, terminal included
        assert_eq!(usage.get("count/pods").map(String::as_str), Some("3"));
    }

    /// `status.used` carries exactly the quota's hard keys — upstream masks the
    /// evaluator's output down to them (`syncResourceQuota`). A key the
    /// evaluator computes but the quota does not constrain must not leak.
    #[tokio::test]
    async fn test_reconcile_quota_masks_status_used_to_hard_keys() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        let mut hard = HashMap::new();
        hard.insert("requests.cpu".to_string(), "4".to_string());
        let quota = ResourceQuota {
            type_meta: TypeMeta {
                kind: "ResourceQuota".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("masked").with_namespace("test-ns"),
            spec: ResourceQuotaSpec {
                hard: Some(hard),
                scopes: None,
                scope_selector: None,
            },
            status: None,
        };
        storage
            .create("/registry/resourcequotas/test-ns/masked", &quota)
            .await
            .unwrap();

        controller.reconcile_quota(&quota).await.unwrap();

        let stored: ResourceQuota = storage
            .get("/registry/resourcequotas/test-ns/masked")
            .await
            .unwrap();
        let used = stored.status.unwrap().used.unwrap();
        assert_eq!(used.keys().collect::<Vec<_>>(), vec!["requests.cpu"]);
        assert_eq!(used.get("requests.cpu").map(String::as_str), Some("0"));
    }
    /// #1775: an auxiliary watch stream that has ended must be retired, never
    /// polled again.
    ///
    /// `WatchStream` is a `futures::stream::unfold`, which panics with
    /// "Unfold must not be polled after it returned `Poll::Ready(None)`" on a
    /// second poll. Before the fix the select arm matched only `Some(Ok(ev))`,
    /// so a terminated stream stayed enabled and the next iteration panicked —
    /// killing the controller task (tokio::spawn swallows panics) and freezing
    /// every quota's `status.used` until the controller-manager restarted.
    ///
    /// Polling the raw stream twice here is what the old code did; going
    /// through `next_aux_event` twice must be safe.
    #[tokio::test]
    async fn an_exhausted_auxiliary_watch_is_retired_not_repolled() {
        use futures::stream::StreamExt;

        // A stream that ends immediately, built the same way storage builds its
        // watches (unfold), so it carries the same "no polling after None" rule.
        let make_stream = || -> rusternetes_storage::WatchStream {
            futures::stream::unfold(false, |done| async move {
                if done {
                    None
                } else {
                    // End on the first poll: no events, just termination.
                    None::<(
                        rusternetes_common::Result<rusternetes_storage::WatchEvent>,
                        bool,
                    )>
                }
            })
            .boxed()
        };

        let mut aux: Option<rusternetes_storage::WatchStream> = Some(make_stream());

        // First call observes the end of the stream and retires it.
        let first = next_aux_event(&mut aux, "Pod").await;
        assert!(first.is_none(), "a terminated stream yields no event");
        assert!(
            aux.is_none(),
            "the stream must be retired so the select arm's `is_some()` guard disables it"
        );

        // Second call must short-circuit on the None rather than poll the dead
        // stream — this is the call that used to panic.
        let second = next_aux_event(&mut aux, "Pod").await;
        assert!(
            second.is_none(),
            "a retired stream must keep yielding None without being polled again"
        );
    }

    /// #1775: an auxiliary watch that errors is retired too — the same
    /// re-poll panic applies once an unfold stream has reported failure and
    /// terminated.
    #[tokio::test]
    async fn an_erroring_auxiliary_watch_is_retired() {
        use futures::stream::StreamExt;

        let stream: rusternetes_storage::WatchStream = futures::stream::once(async {
            Err(rusternetes_common::Error::Storage("watch died".to_string()))
        })
        .boxed();
        let mut aux: Option<rusternetes_storage::WatchStream> = Some(stream);

        assert!(next_aux_event(&mut aux, "Pod").await.is_none());
        assert!(
            aux.is_none(),
            "an errored stream must be retired, not left enabled for another poll"
        );
    }

    /// A live auxiliary watch still delivers its events — the fix must not
    /// silence the fast path that keeps `status.used` fresh between resyncs.
    #[tokio::test]
    async fn a_live_auxiliary_watch_still_delivers_events() {
        use futures::stream::StreamExt;

        let stream: rusternetes_storage::WatchStream = futures::stream::iter(vec![Ok(
            rusternetes_storage::WatchEvent::Added("pods/default/p1".to_string(), "{}".to_string()),
        )])
        .boxed();
        let mut aux: Option<rusternetes_storage::WatchStream> = Some(stream);

        let ev = next_aux_event(&mut aux, "Pod").await;
        assert!(ev.is_some(), "a live stream's event must be delivered");
        assert!(aux.is_some(), "a live stream must stay enabled");
    }
}
