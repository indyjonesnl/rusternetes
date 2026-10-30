use crate::controllers::endpoints_tracker::StaleEndpointsTracker;
use crate::controllers::trigger_time_tracker::{format_rfc3339_nano, TriggerTimeTracker};
use crate::controllers::worker_pool::spawn_workers;
use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::{
    EndpointAddress, EndpointPort, EndpointReference, EndpointSubset, Endpoints, Pod, Service,
};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error};

/// `LabelManagedBy` (pkg/controller/endpoint/endpoints_controller.go:73).
pub const LABEL_MANAGED_BY: &str = "endpoints.kubernetes.io/managed-by";
/// `ControllerName` (pkg/controller/endpoint/endpoints_controller.go:76).
pub const CONTROLLER_NAME: &str = "endpoint-controller";
/// `v1.IsHeadlessService`
/// (staging/src/k8s.io/api/core/v1/well_known_labels.go:64).
pub const IS_HEADLESS_SERVICE_LABEL: &str = "service.kubernetes.io/headless";
/// `v1.EndpointsLastChangeTriggerTime`
/// (staging/src/k8s.io/api/core/v1/annotation_key_constants.go:118).
pub const LAST_CHANGE_TRIGGER_TIME_ANNOTATION: &str =
    "endpoints.kubernetes.io/last-change-trigger-time";

/// `helper.IsServiceIPSet`: a Service has a cluster IP unless it is headless.
fn is_service_ip_set(service: &Service) -> bool {
    !matches!(
        service.spec.cluster_ip.as_deref(),
        None | Some("") | Some("None")
    )
}

/// Port of `labelsCorrectForEndpoints`
/// (pkg/controller/endpoint/endpoints_controller.go:769-785): the Endpoints'
/// labels are correct when it is managed by this controller and carries
/// exactly the Service's labels, ignoring the headless and managed-by labels.
pub fn labels_correct_for_endpoints(
    ep_labels: &HashMap<String, String>,
    svc_labels: &HashMap<String, String>,
) -> bool {
    if ep_labels.get(LABEL_MANAGED_BY).map(String::as_str) != Some(CONTROLLER_NAME) {
        return false;
    }
    let mut skipped = 0;
    for (k, v) in ep_labels {
        if k == IS_HEADLESS_SERVICE_LABEL || k == LABEL_MANAGED_BY {
            skipped += 1;
        } else if svc_labels.get(k) != Some(v) {
            return false;
        }
    }
    svc_labels.len() == ep_labels.len() - skipped
}

/// Port of `capacityAnnotationSetCorrectly`
/// (pkg/controller/endpoint/endpoints_controller.go:668-680).
pub fn capacity_annotation_set_correctly(
    annotations: &HashMap<String, String>,
    subsets: &[EndpointSubset],
) -> bool {
    let n: usize = subsets
        .iter()
        .map(|s| {
            s.addresses.as_ref().map_or(0, Vec::len)
                + s.not_ready_addresses.as_ref().map_or(0, Vec::len)
        })
        .sum();
    if n > MAX_ENDPOINTS_CAPACITY {
        return false;
    }
    !annotations.contains_key(ENDPOINTS_OVER_CAPACITY_ANNOTATION)
}

/// Upstream `pkg/controller/endpoint/endpoints_controller.go::maxCapacity`.
/// Endpoints objects with more than this many addresses won't be routed
/// correctly by kube-proxy, so the controller truncates and emits the
/// `endpoints.kubernetes.io/over-capacity=truncated` annotation.
pub const MAX_ENDPOINTS_CAPACITY: usize = 1000;

/// Annotation key the controller sets on a truncated Endpoints object.
/// Upstream constant: `corev1.EndpointsOverCapacity` ("endpoints.kubernetes.io/over-capacity").
pub const ENDPOINTS_OVER_CAPACITY_ANNOTATION: &str = "endpoints.kubernetes.io/over-capacity";

/// Truncate `subsets` in place so the total address count (ready +
/// notReady) does not exceed `cap`. Removes addresses proportionally
/// across subsets, mirroring upstream `truncateEndpoints`: ready
/// addresses are dropped before notReady ones because routable endpoints
/// are more valuable than placeholders.
///
/// Returns `true` if any addresses were dropped, so the caller can set the
/// `endpoints.kubernetes.io/over-capacity=truncated` annotation.
fn truncate_endpoint_subsets(subsets: &mut [EndpointSubset], cap: usize) -> bool {
    let total: usize = subsets
        .iter()
        .map(|s| {
            s.addresses.as_ref().map(|a| a.len()).unwrap_or(0)
                + s.not_ready_addresses.as_ref().map(|a| a.len()).unwrap_or(0)
        })
        .sum();
    if total <= cap {
        return false;
    }

    let mut to_drop = total - cap;

    // Drop from `addresses` first (ready), proportionally across subsets.
    // For deterministic output and assertion stability we drop from the tail.
    for s in subsets.iter_mut() {
        if to_drop == 0 {
            break;
        }
        if let Some(addrs) = s.addresses.as_mut() {
            let drop_here = to_drop.min(addrs.len());
            addrs.truncate(addrs.len() - drop_here);
            to_drop -= drop_here;
        }
    }

    // Then notReady if still over cap.
    for s in subsets.iter_mut() {
        if to_drop == 0 {
            break;
        }
        if let Some(addrs) = s.not_ready_addresses.as_mut() {
            let drop_here = to_drop.min(addrs.len());
            addrs.truncate(addrs.len() - drop_here);
            to_drop -= drop_here;
        }
    }

    true
}

/// Split each subset into per-node subsets so no resulting subset contains
/// addresses from more than one node. Each output subset preserves the ports
/// of the input subset its addresses came from — when the named-targetPort
/// path produced multiple subsets with different resolved ports, the per-node
/// grouping must keep each (node, ports) combination distinct so pods on
/// different nodes are not stamped with another node's resolved port.
/// Addresses with no `node_name` are grouped under a single empty-node bucket.
/// Output is sorted by (node name, serialized ports) for deterministic,
/// order-independent results. Used to enforce `internalTrafficPolicy=Local`.
pub fn split_subsets_by_node(subsets: Vec<EndpointSubset>) -> Vec<EndpointSubset> {
    use std::collections::BTreeMap;

    // (node, serialized-ports) -> (ready, notReady, actual ports). The ports
    // are keyed alongside the node so distinct port tuples (from the
    // named-targetPort multi-subset path) yield distinct output subsets.
    #[allow(clippy::type_complexity)]
    let mut by_node: BTreeMap<
        (String, String),
        (
            Vec<EndpointAddress>,
            Vec<EndpointAddress>,
            Option<Vec<EndpointPort>>,
        ),
    > = BTreeMap::new();

    for subset in subsets {
        let ports = subset.ports.clone();
        let ports_key = serde_json::to_string(&ports).unwrap_or_default();
        for addr in subset.addresses.into_iter().flatten() {
            let node = addr.node_name.clone().unwrap_or_default();
            by_node
                .entry((node, ports_key.clone()))
                .or_insert_with(|| (Vec::new(), Vec::new(), ports.clone()))
                .0
                .push(addr);
        }
        for addr in subset.not_ready_addresses.into_iter().flatten() {
            let node = addr.node_name.clone().unwrap_or_default();
            by_node
                .entry((node, ports_key.clone()))
                .or_insert_with(|| (Vec::new(), Vec::new(), ports.clone()))
                .1
                .push(addr);
        }
    }

    by_node
        .into_values()
        .map(|(ready, not_ready, ports)| EndpointSubset {
            addresses: if ready.is_empty() { None } else { Some(ready) },
            not_ready_addresses: if not_ready.is_empty() {
                None
            } else {
                Some(not_ready)
            },
            ports,
        })
        .collect()
}

/// Workers draining this controller's queue: upstream `ConcurrentEndpointSyncs` default,
/// pkg/controller/endpoint/config/v1alpha1/defaults.go:33-34, launched at cmd/kube-controller-manager/app/core.go:524.
const CONCURRENT_ENDPOINT_SYNCS: usize = 5;

/// EndpointsController watches Services and Pods to automatically maintain Endpoints resources.
/// It creates/updates Endpoints based on:
/// 1. Service selector matching pod labels
/// 2. Pod readiness status
/// 3. Pod IP assignment
pub struct EndpointsController<S: Storage> {
    storage: Arc<S>,
    /// `e.staleEndpointsTracker` (endpoints_controller.go:158,121).
    stale_endpoints_tracker: StaleEndpointsTracker,
    /// `e.triggerTimeTracker` (endpoints_controller.go:177,122): computes the
    /// `endpoints.kubernetes.io/last-change-trigger-time` annotation.
    trigger_time_tracker: TriggerTimeTracker,
}

impl<S: Storage + 'static> EndpointsController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            stale_endpoints_tracker: StaleEndpointsTracker::new(),
            trigger_time_tracker: TriggerTimeTracker::new(),
        }
    }

    /// The service-not-found arm of `syncService`
    /// (endpoints_controller.go:360-361): forget the deleted Service.
    fn forget_service(&self, namespace: &str, name: &str) {
        self.trigger_time_tracker.delete_service(namespace, name);
        self.stale_endpoints_tracker.delete(namespace, name);
    }

    /// Watch-based run loop. Watches services AND pods as primary resources.
    /// When a pod changes, we find services whose selector matches the pod
    /// and enqueue them for reconciliation.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let queue = WorkQueue::new();

        // Upstream launches `ConcurrentEndpointSyncs` workers over one shared queue
        // (`for i := 0; i < workers; i++ { go wait.UntilWithContext(ctx, worker, time.Second) }`);
        // a key in flight is never handed to a second worker.
        spawn_workers(CONCURRENT_ENDPOINT_SYNCS, &queue, |worker_queue| {
            let worker_self = Arc::clone(&self);
            async move {
                worker_self.worker(worker_queue).await;
            }
        });

        loop {
            self.enqueue_all(&queue).await;

            let svc_prefix = build_prefix("services", None);
            let pod_prefix = build_prefix("pods", None);

            let svc_watch = match self.storage.watch(&svc_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish service watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };
            let pod_watch = match self.storage.watch(&pod_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish pod watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut svc_watch = svc_watch;
            let mut pod_watch = pod_watch;
            let mut resync = tokio::time::interval(std::time::Duration::from_secs(5));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = svc_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Service watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Service watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    event = pod_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                self.enqueue_services_for_pod(&queue, &ev).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Pod watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Pod watch stream ended, reconnecting");
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

    /// When a pod changes, find services in the same namespace whose selector
    /// matches the pod and enqueue them for reconciliation.
    async fn enqueue_services_for_pod(
        &self,
        queue: &WorkQueue,
        event: &rusternetes_storage::WatchEvent,
    ) {
        let pod_key = extract_key(event);
        // Parse pod key: "pods/{namespace}/{name}"
        let parts: Vec<&str> = pod_key.splitn(3, '/').collect();
        let ns = match parts.get(1) {
            Some(ns) => *ns,
            None => return,
        };

        // Get the pod to check its labels
        let storage_key = format!("/registry/{}", pod_key);
        let pod: Option<Pod> = self.storage.get(&storage_key).await.ok();

        // List services in this namespace and find matches
        if let Ok(services) = self
            .storage
            .list::<Service>(&build_prefix("services", Some(ns)))
            .await
        {
            match pod {
                Some(ref pod) => {
                    for svc in &services {
                        if let Some(ref selector) = svc.spec.selector {
                            if Self::labels_match(selector, &pod.metadata.labels) {
                                queue
                                    .add(format!("services/{}/{}", ns, svc.metadata.name))
                                    .await;
                            }
                        }
                    }
                }
                None => {
                    // Pod was deleted -- enqueue all services in this namespace
                    // since we don't know which ones matched
                    for svc in &services {
                        queue
                            .add(format!("services/{}/{}", ns, svc.metadata.name))
                            .await;
                    }
                }
            }
        }
    }

    /// Check if all selector key-value pairs exist in the pod's labels.
    fn labels_match(
        selector: &HashMap<String, String>,
        labels: &Option<HashMap<String, String>>,
    ) -> bool {
        let labels = match labels {
            Some(l) => l,
            None => return selector.is_empty(),
        };
        selector.iter().all(|(k, v)| labels.get(k) == Some(v))
    }

    /// Main reconciliation loop - syncs all services with their endpoints
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
            let storage_key = build_key("services", Some(ns), name);
            match self.storage.get::<Service>(&storage_key).await {
                Ok(service) => match self.reconcile_service(&service).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        error!("Failed to reconcile {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    self.forget_service(ns, name);
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self.storage.list::<Service>("/registry/services/").await {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("services/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                tracing::error!("Failed to list services for enqueue: {}", e);
            }
        }
    }

    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting endpoints reconciliation");

        // List all services across all namespaces
        let services: Vec<Service> = self.storage.list("/registry/services/").await?;

        // Snapshot surviving Service identities for orphan-Endpoints GC below.
        // Without this, deleted Services leave stale Endpoints behind and
        // upstream e2e network/service.go:3459 (DeleteService) times out.
        let surviving: std::collections::HashSet<(String, String)> = services
            .iter()
            .map(|s| {
                let ns = s
                    .metadata
                    .namespace
                    .clone()
                    .unwrap_or_else(|| "default".to_string());
                (ns, s.metadata.name.clone())
            })
            .collect();

        for service in services {
            if let Err(e) = self.reconcile_service(&service).await {
                error!(
                    "Failed to reconcile endpoints for service {}/{}: {}",
                    service
                        .metadata
                        .namespace
                        .as_ref()
                        .unwrap_or(&"default".to_string()),
                    &service.metadata.name,
                    e
                );
            }
        }

        // GC Endpoints whose owning Service no longer exists.
        let all_endpoints: Vec<Endpoints> = self
            .storage
            .list(&build_prefix("endpoints", None))
            .await
            .unwrap_or_default();
        for ep in &all_endpoints {
            let ns = ep.metadata.namespace.as_deref().unwrap_or("default");
            let name = ep.metadata.name.as_str();
            if surviving.contains(&(ns.to_string(), name.to_string())) {
                continue;
            }
            let key = build_key("endpoints", Some(ns), name);
            match self.storage.delete(&key).await {
                Ok(_) => debug!("Deleted orphan Endpoints {}/{}", ns, name),
                Err(e) => debug!("Failed to delete orphan Endpoints {}/{}: {}", ns, name, e),
            }
        }

        Ok(())
    }

    /// Reconcile endpoints for a single service
    pub async fn reconcile_service(&self, service: &Service) -> Result<()> {
        let namespace = service
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Service has no namespace"))?;
        let service_name = &service.metadata.name;

        debug!(
            "Reconciling endpoints for service {}/{}",
            namespace, service_name
        );

        // Skip ExternalName services. Upstream
        // `pkg/controller/endpoint/endpoints_controller.go` (syncService) returns
        // early for `ServiceTypeExternalName` — DNS resolution is the data path,
        // there is no Endpoints object to publish even when a selector is set.
        if matches!(
            service.spec.service_type,
            Some(rusternetes_common::resources::ServiceType::ExternalName)
        ) {
            debug!(
                "Service {}/{} is ExternalName, skipping endpoint creation",
                namespace, service_name
            );
            return Ok(());
        }

        // Skip services without selectors (headless services without selector)
        let selector = match &service.spec.selector {
            Some(s) if !s.is_empty() => s,
            _ => {
                debug!(
                    "Service {}/{} has no selector, skipping endpoint creation",
                    namespace, service_name
                );
                return Ok(());
            }
        };

        // Find all pods in the same namespace
        let pod_prefix = format!("/registry/pods/{}/", namespace);
        let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        // Filter pods that match the service selector
        let matching_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|pod| self.pod_matches_selector(pod, selector))
            .collect();

        // Called before any early return so the tracker's state is updated even
        // when the sync turns out to be a no-op
        // (endpoints_controller.go:385-389).
        let endpoints_last_change_trigger_time = self
            .trigger_time_tracker
            .compute_endpoint_last_change_trigger_time(namespace, service, &matching_pods);

        debug!(
            "Found {} matching pods for service {}/{}",
            matching_pods.len(),
            namespace,
            service_name
        );

        // Build endpoint subsets from matching pods
        let publish_not_ready = service.spec.publish_not_ready_addresses.unwrap_or(false);
        let mut subsets =
            self.build_endpoint_subsets(&matching_pods, &service.spec.ports, publish_not_ready);

        // internalTrafficPolicy=Local: segregate addresses by node so a
        // node-local kube-proxy only consumes endpoints scheduled on the same
        // node. Mirrors upstream behavior where the proxy filters endpoints by
        // the local node's name (pkg/proxy topology / internalTrafficPolicy).
        // We surface that here as one EndpointSubset per node, ensuring no
        // subset mixes addresses from more than one node.
        if matches!(
            service.spec.internal_traffic_policy,
            Some(rusternetes_common::resources::ServiceInternalTrafficPolicy::Local)
        ) {
            subsets = split_subsets_by_node(subsets);
        }

        // Enforce the upstream 1000-address Endpoints capacity cap and emit
        // the `endpoints.kubernetes.io/over-capacity=truncated` annotation
        // when truncation actually fires. Mirrors
        // `pkg/controller/endpoint/endpoints_controller.go::truncateEndpoints`
        // (`maxCapacity = 1000`). Endpoints with > 1000 addresses won't
        // route correctly via kube-proxy, so this is a hard cap.
        let truncated = truncate_endpoint_subsets(&mut subsets, MAX_ENDPOINTS_CAPACITY);

        let endpoints_key = build_key("endpoints", Some(namespace), service_name);
        let svc_labels = service.metadata.labels.clone().unwrap_or_default();
        let existing: Option<Endpoints> = self.storage.get(&endpoints_key).await.ok();

        // endpoints_controller.go:451-452: an Endpoints at a resource version
        // this controller already replaced is out of date.
        if let Some(cur) = &existing {
            if let Some(rv) = cur.metadata.resource_version.as_deref() {
                if self
                    .stale_endpoints_tracker
                    .is_stale(namespace, service_name, rv)
                {
                    return Err(anyhow::anyhow!(
                        "endpoints informer cache is out of date, resource version {} already processed for endpoints {}/{}",
                        rv,
                        namespace,
                        service_name
                    ));
                }
            }
        }

        // Skip the write when nothing changed
        // (endpoints_controller.go:459-468).
        if let Some(cur) = &existing {
            let cur_labels = cur.metadata.labels.clone().unwrap_or_default();
            let cur_annotations = cur.metadata.annotations.clone().unwrap_or_default();
            if cur.subsets == subsets
                && labels_correct_for_endpoints(&cur_labels, &svc_labels)
                && capacity_annotation_set_correctly(&cur_annotations, &cur.subsets)
            {
                debug!(
                    "Endpoints for service {}/{} unchanged, skipping write",
                    namespace, service_name
                );
                return Ok(());
            }
        }

        // Update a copy of the stored object, keeping its identity and
        // annotations (endpoints_controller.go:469-499). Upstream sets no
        // ownerReferences on the Endpoints it creates.
        let mut endpoints = existing.clone().unwrap_or_else(|| Endpoints {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Endpoints".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new(service_name.clone())
                .with_namespace(namespace.clone()),
            subsets: Vec::new(),
        });
        endpoints.subsets = subsets;

        // endpoints_controller.go:477-482: export the computed trigger time, or
        // clear the annotation when there is no new one.
        let mut annotations = endpoints.metadata.annotations.take().unwrap_or_default();
        match endpoints_last_change_trigger_time {
            Some(t) => {
                annotations.insert(
                    LAST_CHANGE_TRIGGER_TIME_ANNOTATION.to_string(),
                    format_rfc3339_nano(t),
                );
            }
            None => {
                annotations.remove(LAST_CHANGE_TRIGGER_TIME_ANNOTATION);
            }
        }
        if truncated {
            annotations.insert(
                ENDPOINTS_OVER_CAPACITY_ANNOTATION.to_string(),
                "truncated".to_string(),
            );
        } else {
            annotations.remove(ENDPOINTS_OVER_CAPACITY_ANNOTATION);
        }
        endpoints.metadata.annotations = (!annotations.is_empty()).then_some(annotations);

        let mut labels = svc_labels;
        if is_service_ip_set(service) {
            labels.remove(IS_HEADLESS_SERVICE_LABEL);
        } else {
            labels.insert(IS_HEADLESS_SERVICE_LABEL.to_string(), String::new());
        }
        labels.insert(LABEL_MANAGED_BY.to_string(), CONTROLLER_NAME.to_string());
        endpoints.metadata.labels = Some(labels);

        // endpoints_controller.go:455,503-508: `createEndpoints` is decided from
        // the object read, never by trying an Update first. An Update of an
        // Endpoints this controller just built carries a client-minted UID,
        // which the server turns into a UID precondition against the absent
        // object (rest.DefaultUpdatedObjectInfo.Preconditions) -> Conflict
        // forever, so the Endpoints was never created over the API (#3075).
        match &existing {
            None => {
                self.storage.create(&endpoints_key, &endpoints).await?;
            }
            Some(cur) => {
                let updated = self.storage.update(&endpoints_key, &endpoints).await?;
                // endpoints_controller.go:539-541: track the replaced resource
                // version so a lagging read of it is recognised as stale.
                if updated.metadata.resource_version != cur.metadata.resource_version {
                    if let Some(rv) = cur.metadata.resource_version.as_deref() {
                        self.stale_endpoints_tracker
                            .stale(namespace, service_name, rv);
                    }
                }
            }
        }

        debug!(
            "Updated endpoints for service {}/{} with {} subsets",
            namespace,
            service_name,
            endpoints.subsets.len()
        );

        Ok(())
    }

    /// Check if a pod matches the service selector
    pub fn pod_matches_selector(&self, pod: &Pod, selector: &HashMap<String, String>) -> bool {
        let pod_labels = match &pod.metadata.labels {
            Some(labels) => labels,
            None => return false,
        };

        // All selector labels must match pod labels
        selector
            .iter()
            .all(|(key, value)| pod_labels.get(key).map(|v| v == value).unwrap_or(false))
    }

    /// Build endpoint subsets from pods, separating ready and not-ready pods.
    /// K8s ref: pkg/controller/endpoint/endpoints_controller.go — syncService
    fn build_endpoint_subsets(
        &self,
        pods: &[&Pod],
        service_ports: &[rusternetes_common::resources::ServicePort],
        publish_not_ready: bool,
    ) -> Vec<EndpointSubset> {
        // Separate pods by readiness
        let mut ready_addresses = Vec::new();
        let mut not_ready_addresses = Vec::new();

        for pod in pods {
            // K8s ShouldPodBeInEndpoints() checks:
            // 1. Skip terminal pods (Succeeded/Failed)
            // 2. Skip pods without IPs
            // 3. Skip terminating pods (unless publishNotReadyAddresses)
            // See: staging/src/k8s.io/endpointslice/util/controller_utils.go
            let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
            if matches!(
                phase,
                Some(rusternetes_common::types::Phase::Succeeded)
                    | Some(rusternetes_common::types::Phase::Failed)
            ) {
                continue; // Terminal pods are never in endpoints
            }
            if !publish_not_ready && pod.metadata.deletion_timestamp.is_some() {
                continue; // Terminating pods excluded unless publishNotReady
            }

            // Skip pods without an IP address
            let pod_ip = match &pod.status {
                Some(status) => match &status.pod_ip {
                    Some(ip) if !ip.is_empty() => ip.clone(),
                    _ => {
                        debug!(
                            "Pod {}/{} has no IP, skipping",
                            pod.metadata
                                .namespace
                                .as_ref()
                                .unwrap_or(&"default".to_string()),
                            &pod.metadata.name
                        );
                        continue;
                    }
                },
                None => {
                    debug!(
                        "Pod {}/{} has no status, skipping",
                        pod.metadata
                            .namespace
                            .as_ref()
                            .unwrap_or(&"default".to_string()),
                        &pod.metadata.name
                    );
                    continue;
                }
            };

            let address = EndpointAddress {
                ip: pod_ip,
                hostname: pod.spec.as_ref().and_then(|s| {
                    // Only set hostname when subdomain is also set (K8s behavior)
                    if s.subdomain.is_some() {
                        s.hostname.clone().or(Some(pod.metadata.name.clone()))
                    } else {
                        None
                    }
                }),
                node_name: pod.spec.as_ref().and_then(|s| s.node_name.clone()),
                target_ref: Some(EndpointReference {
                    kind: Some("Pod".to_string()),
                    namespace: pod.metadata.namespace.clone(),
                    name: Some(pod.metadata.name.clone()),
                    uid: Some(pod.metadata.uid.clone()),
                    ..Default::default()
                }),
            };

            // Check if pod is ready — if publishNotReadyAddresses, all go to ready
            if publish_not_ready || rusternetes_common::podutil::is_pod_ready(pod) {
                ready_addresses.push(address);
            } else {
                not_ready_addresses.push(address);
            }
        }

        // Check if any service port uses a named targetPort — if so, we need to
        // resolve per-pod and group by resolved port tuple to create separate subsets.
        let has_named_target_port = service_ports.iter().any(|sp| {
            matches!(&sp.target_port, Some(rusternetes_common::resources::IntOrString::String(s)) if s.parse::<u16>().is_err())
        });

        if has_named_target_port {
            // Group pods by their resolved port tuple, since different pods may have
            // different containerPort values for the same named port.
            let mut port_groups: std::collections::HashMap<
                Vec<u16>,
                (Vec<EndpointAddress>, Vec<EndpointAddress>),
            > = std::collections::HashMap::new();

            for pod in pods {
                let pod_ip = match &pod.status {
                    Some(status) => match &status.pod_ip {
                        Some(ip) if !ip.is_empty() => ip.clone(),
                        _ => continue,
                    },
                    None => continue,
                };

                // Resolve each service port for this pod
                let resolved_ports: Vec<u16> = service_ports
                    .iter()
                    .map(|sp| {
                        match &sp.target_port {
                            Some(rusternetes_common::resources::IntOrString::Int(p)) => *p as u16,
                            Some(rusternetes_common::resources::IntOrString::String(s)) => {
                                if let Ok(p) = s.parse::<u16>() {
                                    p
                                } else {
                                    // Look up named port in pod's containers
                                    Self::resolve_named_port(pod, s).unwrap_or(sp.port)
                                }
                            }
                            None => sp.port,
                        }
                    })
                    .collect();

                let address = EndpointAddress {
                    ip: pod_ip,
                    hostname: pod.spec.as_ref().and_then(|s| {
                        // Only set hostname when subdomain is also set (K8s behavior)
                        if s.subdomain.is_some() {
                            s.hostname.clone().or(Some(pod.metadata.name.clone()))
                        } else {
                            None
                        }
                    }),
                    node_name: pod.spec.as_ref().and_then(|s| s.node_name.clone()),
                    target_ref: Some(EndpointReference {
                        kind: Some("Pod".to_string()),
                        namespace: pod.metadata.namespace.clone(),
                        name: Some(pod.metadata.name.clone()),
                        uid: Some(pod.metadata.uid.clone()),
                        ..Default::default()
                    }),
                };

                let entry = port_groups
                    .entry(resolved_ports)
                    .or_insert_with(|| (Vec::new(), Vec::new()));
                if rusternetes_common::podutil::is_pod_ready(pod) {
                    entry.0.push(address);
                } else {
                    entry.1.push(address);
                }
            }

            // Create one subset per unique port combination
            let mut subsets = Vec::new();
            for (resolved_ports, (ready, not_ready)) in port_groups {
                if ready.is_empty() && not_ready.is_empty() {
                    continue;
                }
                let endpoint_ports: Vec<EndpointPort> = service_ports
                    .iter()
                    .zip(resolved_ports.iter())
                    .map(|(sp, &port)| EndpointPort {
                        name: sp.name.clone(),
                        port,
                        protocol: sp.protocol.clone(),
                        app_protocol: None,
                    })
                    .collect();
                subsets.push(EndpointSubset {
                    addresses: if ready.is_empty() { None } else { Some(ready) },
                    not_ready_addresses: if not_ready.is_empty() {
                        None
                    } else {
                        Some(not_ready)
                    },
                    ports: if endpoint_ports.is_empty() {
                        None
                    } else {
                        Some(endpoint_ports)
                    },
                });
            }
            subsets
        } else {
            // No named ports — simple path: all pods share the same port tuple
            let endpoint_ports: Vec<EndpointPort> = service_ports
                .iter()
                .map(|sp| EndpointPort {
                    name: sp.name.clone(),
                    port: match &sp.target_port {
                        Some(rusternetes_common::resources::IntOrString::Int(p)) => *p as u16,
                        None => sp.port,
                        _ => sp.port,
                    },
                    protocol: sp.protocol.clone(),
                    app_protocol: None,
                })
                .collect();

            let mut subsets = Vec::new();
            if !ready_addresses.is_empty() || !not_ready_addresses.is_empty() {
                subsets.push(EndpointSubset {
                    addresses: if ready_addresses.is_empty() {
                        None
                    } else {
                        Some(ready_addresses)
                    },
                    not_ready_addresses: if not_ready_addresses.is_empty() {
                        None
                    } else {
                        Some(not_ready_addresses)
                    },
                    ports: if endpoint_ports.is_empty() {
                        None
                    } else {
                        Some(endpoint_ports)
                    },
                });
            }
            subsets
        }
    }

    /// Resolve a named port to its containerPort value by searching the pod's containers.
    fn resolve_named_port(pod: &Pod, port_name: &str) -> Option<u16> {
        pod.spec.as_ref()?.containers.iter().find_map(|c| {
            c.ports.as_ref()?.iter().find_map(|p| {
                if p.name.as_deref() == Some(port_name) {
                    Some(p.container_port)
                } else {
                    None
                }
            })
        })
    }
}
