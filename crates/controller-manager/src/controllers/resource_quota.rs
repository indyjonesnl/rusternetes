use crate::controllers::worker_pool::spawn_workers;
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
pub async fn next_aux_event(
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

/// What the quota monitor remembers about an object between watch events, in
/// place of the informer's `oldObj`.
#[derive(Default)]
pub struct ReplenishFilter {
    /// pod key -> (still charged, quota-terminating, container resources signature)
    pods: HashMap<String, (bool, bool, String)>,
    /// service key -> quota service type ("", "NodePort", "LoadBalancer")
    services: HashMap<String, &'static str>,
}

impl ReplenishFilter {
    /// Whether a watch event should re-enqueue the namespace's quotas.
    ///
    /// Port of `QuotaMonitor.controllerFor`
    /// (pkg/controller/resourcequota/resource_quota_monitor.go:146-172), which
    /// registers an UpdateFunc and a DeleteFunc and NO AddFunc, and of
    /// `DefaultUpdateFilter` (pkg/quota/v1/install/update_filter.go:31-60).
    /// A create is already charged by quota admission, which writes
    /// `status.used` before the object lands; recomputing right after a create
    /// can read the quota with an in-flight charge, list the objects before the
    /// new one is stored and CAS-erase the charge, letting a later create past
    /// the hard limit (#2812: 3 pods under `pods=2`).
    ///
    /// Updates are only replenishing events when:
    /// * pods: the quota scope changed (`IsTerminating`), a container's
    ///   resources changed, or the pod stopped being charged
    ///   (`QuotaV1Pod(old) && !QuotaV1Pod(new)`);
    /// * services: `GetQuotaServiceType` changed;
    /// * PVCs: always (upstream's `RequiresQuotaReplenish` fields are not
    ///   modelled, so this stays conservative);
    /// * configmaps, secrets and the rest: never (the filter returns false).
    ///
    /// An update for an object never seen before (no `oldObj`) enqueues.
    pub fn should_replenish(&mut self, ev: &rusternetes_storage::WatchEvent) -> bool {
        use rusternetes_storage::WatchEvent;
        let key = extract_key(ev);
        let kind = key.split('/').next().unwrap_or("");
        match (kind, ev) {
            (_, WatchEvent::Deleted(..)) => {
                self.pods.remove(&key);
                self.services.remove(&key);
                true
            }
            ("pods", WatchEvent::Added(_, v) | WatchEvent::Modified(_, v)) => {
                let is_add = matches!(ev, WatchEvent::Added(..));
                let Ok(pod) = serde_json::from_str::<Pod>(v) else {
                    return !is_add;
                };
                let new = (
                    quota::is_quota_charged(&pod, Utc::now()),
                    quota::is_terminating(&pod),
                    pod.spec
                        .as_ref()
                        .and_then(|s| {
                            serde_json::to_string(
                                &s.containers
                                    .iter()
                                    .map(|c| &c.resources)
                                    .collect::<Vec<_>>(),
                            )
                            .ok()
                        })
                        .unwrap_or_default(),
                );
                let old = self.pods.insert(key, new.clone());
                if is_add {
                    return false;
                }
                match old {
                    None => true,
                    Some(o) => o.1 != new.1 || o.2 != new.2 || (o.0 && !new.0),
                }
            }
            ("services", WatchEvent::Added(_, v) | WatchEvent::Modified(_, v)) => {
                let is_add = matches!(ev, WatchEvent::Added(..));
                let Ok(svc) = serde_json::from_str::<Service>(v) else {
                    return !is_add;
                };
                let new = match svc.spec.service_type {
                    Some(rusternetes_common::resources::ServiceType::NodePort) => "NodePort",
                    Some(rusternetes_common::resources::ServiceType::LoadBalancer) => {
                        "LoadBalancer"
                    }
                    _ => "",
                };
                let old = self.services.insert(key, new);
                !is_add && old.is_none_or(|o| o != new)
            }
            ("persistentvolumeclaims", WatchEvent::Modified(..)) => true,
            _ => false,
        }
    }
}

/// Workers draining this controller's queue: upstream `ConcurrentResourceQuotaSyncs` default,
/// pkg/controller/resourcequota/config/v1alpha1/defaults.go:37-38, launched at cmd/kube-controller-manager/app/core.go:621.
const CONCURRENT_RESOURCE_QUOTA_SYNCS: usize = 5;

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

        // Upstream launches `ConcurrentResourceQuotaSyncs` workers over one shared queue
        // (`for i := 0; i < workers; i++ { go wait.UntilWithContext(ctx, worker, time.Second) }`);
        // a key in flight is never handed to a second worker.
        spawn_workers(CONCURRENT_RESOURCE_QUOTA_SYNCS, &queue, |worker_queue| {
            let worker_self = Arc::clone(&self);
            async move {
                worker_self.worker(worker_queue).await;
            }
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

            let mut filter = ReplenishFilter::default();
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
                            Some(ev) => {
                                if filter.should_replenish(&ev) {
                                    self.enqueue_quotas_for_event(&queue, &ev).await;
                                }
                            }
                            // Stream retired inside next_aux_event; reconnect all
                            // watches rather than run on with a dead one.
                            None => watch_broken = true,
                        }
                    }
                    event = next_aux_event(&mut svc_watch, "Service"), if svc_watch.is_some() => {
                        match event {
                            Some(ev) => {
                                if filter.should_replenish(&ev) {
                                    self.enqueue_quotas_for_event(&queue, &ev).await;
                                }
                            }
                            // Stream retired inside next_aux_event; reconnect all
                            // watches rather than run on with a dead one.
                            None => watch_broken = true,
                        }
                    }
                    event = next_aux_event(&mut cm_watch, "ConfigMap"), if cm_watch.is_some() => {
                        match event {
                            Some(ev) => {
                                if filter.should_replenish(&ev) {
                                    self.enqueue_quotas_for_event(&queue, &ev).await;
                                }
                            }
                            // Stream retired inside next_aux_event; reconnect all
                            // watches rather than run on with a dead one.
                            None => watch_broken = true,
                        }
                    }
                    event = next_aux_event(&mut secret_watch, "Secret"), if secret_watch.is_some() => {
                        match event {
                            Some(ev) => {
                                if filter.should_replenish(&ev) {
                                    self.enqueue_quotas_for_event(&queue, &ev).await;
                                }
                            }
                            // Stream retired inside next_aux_event; reconnect all
                            // watches rather than run on with a dead one.
                            None => watch_broken = true,
                        }
                    }
                    event = next_aux_event(&mut pvc_watch, "PersistentVolumeClaim"), if pvc_watch.is_some() => {
                        match event {
                            Some(ev) => {
                                if filter.should_replenish(&ev) {
                                    self.enqueue_quotas_for_event(&queue, &ev).await;
                                }
                            }
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
    pub async fn enqueue_quotas_for_event(
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
    pub async fn reconcile_one(&self, namespace: &str, name: &str) -> Result<()> {
        let key = build_key("resourcequotas", Some(namespace), name);
        let quota: ResourceQuota = self.storage.get(&key).await?;
        self.reconcile_quota(&quota).await
    }

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
    pub async fn reconcile_quota(&self, quota: &ResourceQuota) -> Result<()> {
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
            // Write the status SUBRESOURCE only, conditional on the
            // resourceVersion of the quota this reconcile read (#2153):
            // upstream `syncResourceQuota`
            // (pkg/controller/resourcequota/resource_quota_controller.go) calls
            // `UpdateStatus` on a copy of the quota it computed usage for. The
            // status is derived from a possibly stale list snapshot, so a quota
            // changed since conflicts and the worker requeues it rate-limited,
            // rather than writing usage computed against the old spec. Only
            // `.status` is ever applied, so a stale spec can never be written
            // back (the update+delete conformance flake, #268).
            let mut desired = quota.clone();
            desired.status = new_status;
            match self.storage.update_status_cas(&key, &desired).await {
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

    /// Whether a pod matches every scope of the quota: the scope half of
    /// `generic.CalculateUsageStats` (`staging/src/k8s.io/apiserver/pkg/quota/
    /// v1/generic/evaluator.go`) through `podMatchesScopeFunc`
    /// (`pkg/quota/v1/evaluator/core/pods.go:331-356`), which the api-server's
    /// quota admission shares (`rusternetes_common::quota`), so admission and
    /// this controller agree on which pods a scoped quota covers.
    pub fn pod_matches_scopes(
        pod: &Pod,
        scopes: &[String],
        scope_selector: Option<&rusternetes_common::resources::ScopeSelector>,
    ) -> bool {
        let spec = rusternetes_common::resources::ResourceQuotaSpec {
            hard: None,
            scopes: Some(scopes.to_vec()),
            scope_selector: scope_selector.cloned(),
        };
        quota::pod_matches_quota_scopes(pod, &spec)
    }

    /// Calculate resource usage in a namespace, respecting quota scopes.
    /// Only counts resources that appear in `hard_keys` to avoid unnecessary work.
    pub async fn calculate_usage(
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
            .any(|k| quota::pvc_matches_resource_name(k));
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

            // Node ports, counted per port as upstream `serviceEvaluator.Usage`
            // does (`pkg/quota/v1/evaluator/core/services.go:117-147`): every
            // port of a NodePort Service, and of a LoadBalancer unless
            // `allocateLoadBalancerNodePorts` is false, when only the ports
            // with an explicit node port count (`portsWithNodePorts`).
            let nodeport_count: usize = services
                .iter()
                .map(|s| match s.spec.service_type {
                    Some(rusternetes_common::resources::ServiceType::NodePort) => {
                        s.spec.ports.len()
                    }
                    Some(rusternetes_common::resources::ServiceType::LoadBalancer) => {
                        if s.spec.allocate_load_balancer_node_ports == Some(false) {
                            s.spec
                                .ports
                                .iter()
                                .filter(|p| p.node_port.is_some_and(|n| n != 0))
                                .count()
                        } else {
                            s.spec.ports.len()
                        }
                    }
                    _ => 0,
                })
                .sum();
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
            // `pvcEvaluator.UsageStats` (persistent_volume_claims.go:226-232):
            // `CalculateUsageStats` seeds every tracked key with zero, then
            // adds each scope-matching claim's `Usage` - the same
            // `pvc_usage` quota admission charges, so `status.used` carries
            // `requests.storage` and the per-class keys admission reads.
            // `pvcMatchesScopeFunc` (:293-309) matches only the
            // VolumeAttributesClass scope, so any other scope matches no
            // claim; `CalculateUsageStats` (generic/evaluator.go:185-229)
            // ANDs every `spec.scopes` entry (as `Exists`) and every
            // `scopeSelector` expression.
            let zero = Quantity::from_value(0, Format::DecimalSI);
            let mut totals = quota::ResourceList::new();
            for key in hard_keys
                .iter()
                .filter(|k| quota::pvc_matches_resource_name(k))
            {
                totals.insert(key.clone(), zero);
            }
            let spec = rusternetes_common::resources::ResourceQuotaSpec {
                hard: None,
                scopes: Some(scopes.to_vec()),
                scope_selector: scope_selector.cloned(),
            };
            let selectors = quota::scope_selectors_from_quota(&spec);
            let pvc_prefix = format!("/registry/persistentvolumeclaims/{}/", namespace);
            let pvcs: Vec<rusternetes_common::resources::PersistentVolumeClaim> =
                self.storage.list(&pvc_prefix).await.unwrap_or_default();
            // An erroring scope function ends the walk with what has been
            // summed so far (evaluator.go:203-205, `return result, nil`).
            'claims: for pvc in &pvcs {
                let mut matches_scopes = true;
                for selector in &selectors {
                    match quota::pvc_matches_scope(selector, pvc) {
                        Ok(m) => matches_scopes = matches_scopes && m,
                        Err(_) => break 'claims,
                    }
                }
                if matches_scopes {
                    totals = quota::add(&totals, &quota::pvc_usage(pvc));
                }
            }
            usage.extend(quota::to_string_map(&totals));
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

        // Any other `count/<resource>[.<group>]`: the object-count evaluator
        // quota admission falls back to for a resource without its own
        // (`generic.NewObjectCountEvaluator`, apiserver/pkg/quota/v1/generic/
        // evaluator.go:325-340, used by plugin/resourcequota/controller.go:
        // 667-674). Its usage is one per object in the namespace.
        for key in hard_keys {
            let Some(name) = key.strip_prefix("count/") else {
                continue;
            };
            if usage.contains_key(key) {
                continue;
            }
            let n = self.count_objects(namespace, name).await;
            usage.insert(key.clone(), n.to_string());
        }

        Ok(usage)
    }

    /// The number of `<resource>[.<group>]` objects in `namespace`. A
    /// built-in resource is stored under its resource name, a custom
    /// resource under `<group with _>_<plural>`
    /// (`crates/api-server/src/handlers/custom_resource.rs`); only one of
    /// the two prefixes holds anything.
    async fn count_objects(&self, namespace: &str, name: &str) -> usize {
        let (resource, group) = name.split_once('.').unwrap_or((name, ""));
        let mut prefixes = vec![build_prefix(resource, Some(namespace))];
        if !group.is_empty() {
            let custom = format!("{}_{}", group.replace('.', "_"), resource);
            prefixes.push(build_prefix(&custom, Some(namespace)));
        }
        let mut n = 0;
        for prefix in prefixes {
            let items: Vec<serde_json::Value> =
                self.storage.list(&prefix).await.unwrap_or_default();
            n += items.len();
        }
        n
    }
}
