use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use rusternetes_common::resources::{Namespace, NamespaceCondition, NamespaceStatus, Pod};
use rusternetes_common::types::Phase;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Map an internal resource-type slug (the segment used in the `/registry/...`
/// storage prefix) to its canonical Kubernetes `<resource>.<group>` label, as
/// used in upstream's `NamespaceContentRemaining` condition message.
///
/// Mirrors how upstream's `deleteAllContent` formats remaining resources via
/// `fmt.Sprintf("%s.%s has %d resource instances", gvr.Resource, gvr.Group, ...)`
/// in `pkg/controller/namespace/deletion/status_condition_utils.go` —
/// `pods` becomes `"pods."` (empty group), `deployments` becomes
/// `"deployments.apps"`, etc.
fn resource_type_to_gvr_label(resource_type: &str) -> String {
    let group = match resource_type {
        // Core (group "")
        "pods"
        | "replicationcontrollers"
        | "configmaps"
        | "secrets"
        | "serviceaccounts"
        | "services"
        | "endpoints"
        | "persistentvolumeclaims"
        | "resourcequotas"
        | "limitranges"
        | "events"
        | "podtemplates" => "",
        // apps
        "replicasets" | "deployments" | "statefulsets" | "daemonsets" | "controllerrevisions" => {
            "apps"
        }
        // batch
        "jobs" | "cronjobs" => "batch",
        // discovery.k8s.io
        "endpointslices" => "discovery.k8s.io",
        // networking.k8s.io
        "ingresses" | "networkpolicies" => "networking.k8s.io",
        // policy
        "poddisruptionbudgets" => "policy",
        // rbac.authorization.k8s.io
        "roles" | "rolebindings" => "rbac.authorization.k8s.io",
        // autoscaling
        "horizontalpodautoscalers" => "autoscaling",
        // coordination.k8s.io
        "leases" => "coordination.k8s.io",
        // resource.k8s.io (DRA)
        "resourceclaims" | "resourceclaimtemplates" => "resource.k8s.io",
        // storage.k8s.io
        "csistoragecapacities" => "storage.k8s.io",
        // Unknown: fall back to no group; mirrors upstream's empty Group
        _ => "",
    };
    format!("{}.{}", resource_type, group)
}

/// Per-resource-type cleanup outcome.
///
/// Mirrors upstream's `gvrDeletionMetadata` from
/// `pkg/controller/namespace/deletion/namespaced_resources_deleter.go`
/// (release-1.35) — used to accumulate "how many of this GVR are still
/// stuck, and which finalizers are blocking them" so we can render
/// `NamespaceContentRemaining` / `NamespaceFinalizersRemaining` messages
/// byte-for-byte compatible with upstream.
#[derive(Debug, Default)]
struct GvrDeletionMetadata {
    /// How many instances of this resource type still live in storage.
    num_remaining: usize,
    /// finalizer-token -> # of resources of this type stuck on it.
    finalizers_to_num_remaining: BTreeMap<String, usize>,
    /// Seconds to wait before looking again; 0 means no estimate. Upstream's
    /// `gvrDeletionMetadata.finalizerEstimateSeconds`.
    finalizer_estimate_seconds: i64,
}

/// Wait applied when items remain only because of finalizers, "to allow for GC
/// to complete". Upstream `finalizerEstimateSeconds = int64(15)`
/// (`pkg/controller/namespace/deletion/namespaced_resources_deleter.go:212`).
pub const FINALIZER_ESTIMATE_SECONDS: i64 = 15;

/// Port of `estimateGracefulTerminationForPods`
/// (`namespaced_resources_deleter.go:630-657`): the largest
/// `terminationGracePeriodSeconds` among the namespace's non-terminal pods.
pub fn estimate_graceful_termination_for_pods(pods: &[serde_json::Value]) -> i64 {
    let mut estimate = 0i64;
    for pod in pods {
        // filter out terminal pods
        let phase = pod.pointer("/status/phase").and_then(|p| p.as_str());
        if matches!(phase, Some("Succeeded") | Some("Failed")) {
            continue;
        }
        if let Some(grace) = pod
            .pointer("/spec/terminationGracePeriodSeconds")
            .and_then(|g| g.as_i64())
        {
            if grace > estimate {
                estimate = grace;
            }
        }
    }
    estimate
}

/// Tail of `estimateGracefulTermination` (`:621-627`): "determine if the
/// estimate is greater than the deletion timestamp" - once the namespace has
/// been deleting for at least the estimate, the estimate is spent.
pub fn clamp_estimate_to_namespace_age(
    estimate: i64,
    namespace_deleted_at: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
) -> i64 {
    let elapsed = now.signed_duration_since(namespace_deleted_at);
    if elapsed >= chrono::Duration::seconds(estimate) {
        0
    } else {
        estimate
    }
}

/// Requeue delay for a namespace that reported `estimate` seconds remaining:
/// `t := estimate.Estimate/2 + 1` (`pkg/controller/namespace/
/// namespace_controller.go:158`), then `queue.AddAfter(key, t seconds)`.
pub fn requeue_delay_for_estimate(estimate: i64) -> Duration {
    Duration::from_secs((estimate / 2 + 1) as u64)
}

/// Namespaces reconciled at once.
///
/// Upstream's `ConcurrentNamespaceSyncs` default
/// (pkg/controller/namespace/config/v1alpha1/defaults.go:38), tunable via
/// `--concurrent-namespace-syncs`
/// (cmd/kube-controller-manager/app/options/namespacecontroller.go:37), whose
/// help text states the trade directly: "Larger number = more responsive
/// namespace termination, but more CPU (and network) load".
const CONCURRENT_NAMESPACE_SYNCS: usize = 10;

/// NamespaceController handles namespace lifecycle and finalization.
/// When a namespace is marked for deletion, it:
/// 1. Discovers all resources in the namespace
/// 2. Deletes all resources (respecting finalizers)
/// 3. Removes finalizers from the namespace
/// 4. Allows the namespace to be deleted
pub struct NamespaceController<S: Storage> {
    storage: Arc<S>,
    /// Cluster CA cert PEM, used to (re)create the `kube-root-ca.crt` ConfigMap
    /// in every namespace (upstream `rootcacertpublisher`). The controller-
    /// manager container does not have the CA at the api-server's hardcoded
    /// paths, so it is threaded in from the resolved kubeconfig CA. `None`
    /// falls back to the legacy file-path reads (used by unit tests).
    ca_cert: Option<String>,
    /// Resource types whose `deletecollection` verb is unsupported, so the
    /// deleter goes straight to per-item deletes. Port of upstream's
    /// `operationNotSupportedCache` (`opCache`,
    /// `pkg/controller/namespace/deletion/namespaced_resources_deleter.go:187-197`).
    delete_collection_unsupported: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl<S: Storage + 'static> NamespaceController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            ca_cert: None,
            delete_collection_unsupported: Default::default(),
        }
    }

    /// Provide the cluster CA cert PEM so the controller can (re)create
    /// `kube-root-ca.crt`. Without it the controller cannot read the CA (its
    /// container lacks the api-server's cert paths) and recreation-on-delete
    /// silently no-ops (#1161-adjacent: kube-root-ca conformance test).
    pub fn with_ca_cert(mut self, ca_cert: Option<String>) -> Self {
        self.ca_cert = ca_cert.filter(|s| !s.is_empty());
        self
    }

    /// Work-queue-based run loop. Watch events enqueue resource keys;
    /// a pool of [`CONCURRENT_NAMESPACE_SYNCS`] workers drains the queue with
    /// deduplication and exponential backoff on failures.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let queue = WorkQueue::new();

        // One worker per upstream `ConcurrentNamespaceSyncs`
        // (pkg/controller/namespace/config/v1alpha1/defaults.go:38), all
        // draining the same queue — which is upstream's shape too: N workers
        // over one shared workqueue, not N queues.
        //
        // This is safe because `WorkQueue::get` moves the key into a
        // `processing` set before returning it, so a namespace in flight is
        // never handed to a second worker (client-go's workqueue guarantees
        // the same, and for the same reason).
        //
        // A single worker meant one namespace at a time. Measured on a live
        // kine cluster during a conformance phase: ~47 seconds per namespace,
        // strictly sequential, with 307 namespaces waiting and ZERO draining
        // over a 90-second window:
        //
        //   09:11:35  Finalizing namespace csinodes-8557
        //   09:12:20  ... resources cleared, conditions set
        //   09:12:20  csistoragecapacity-3787 ... starting finalization
        //   09:13:09  ... resources cleared, conditions set
        //
        // The controller was not wedged — it logged throughout. It simply
        // could not keep up, and hydrophone's between-phase cleanup waits on
        // the `conformance` namespace, so the conformance suite stalls behind
        // it.
        for _ in 0..CONCURRENT_NAMESPACE_SYNCS {
            let worker_queue = queue.clone();
            let worker_self = Arc::clone(&self);
            tokio::spawn(async move {
                worker_self.worker(worker_queue).await;
            });
        }

        // Watch loop: enqueue keys from watch events
        loop {
            // Enqueue all existing namespaces for initial reconciliation
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("namespaces", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(Duration::from_secs(30));
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

    /// Enqueue all existing namespace keys for reconciliation.
    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<Namespace>("/registry/namespaces/")
            .await
        {
            Ok(namespaces) => {
                for ns in &namespaces {
                    let key = format!("namespaces/{}", ns.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list namespaces for enqueue: {}", e);
            }
        }
    }

    /// Worker loop: pulls keys from the queue and reconciles one at a time.
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            // Parse key: "namespaces/{name}"
            let name = key.strip_prefix("namespaces/").unwrap_or(&key);
            let storage_key = build_key("namespaces", None, name);

            match self.storage.get::<Namespace>(&storage_key).await {
                Ok(ns) => match self.reconcile_namespace(&ns).await {
                    Ok(estimate) => {
                        queue.forget(&key).await;
                        if estimate > 0 {
                            // namespace_controller.go:157-160
                            queue
                                .add_after(key.clone(), requeue_delay_for_estimate(estimate))
                                .await;
                        }
                    }
                    Err(e) => {
                        error!("Failed to reconcile namespace {}: {}", name, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    // Namespace was deleted or not found — nothing to reconcile
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    /// Main reconciliation loop - processes all namespaces
    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting namespace reconciliation");

        // List all namespaces
        let namespaces: Vec<Namespace> = self.storage.list("/registry/namespaces/").await?;

        // Reconcile namespaces concurrently.
        //
        // Upstream runs `ConcurrentNamespaceSyncs` = 10 namespace workers
        // (pkg/controller/namespace/config/v1alpha1/defaults.go:38). Within a
        // single namespace it stays serial across resource types — the
        // concurrency it buys is *across namespaces*, which is the axis that
        // was missing here.
        //
        // Serially this drained roughly one namespace per 90 seconds on a live
        // cluster, ~48s of it inside a single namespace's resource sweep. With
        // 354 namespaces waiting that is about nine hours, and it stalled a
        // conformance run outright: hydrophone's between-phase cleanup sat 63
        // minutes on "Waiting for Namespace conformance to be deleted" and
        // phase 2 never started.
        for chunk in namespaces.chunks(CONCURRENT_NAMESPACE_SYNCS) {
            let results = futures::future::join_all(
                chunk
                    .iter()
                    .map(|namespace| self.reconcile_namespace(namespace)),
            )
            .await;
            for (namespace, result) in chunk.iter().zip(results) {
                if let Err(e) = result {
                    error!(
                        "Failed to reconcile namespace {}: {}",
                        &namespace.metadata.name, e
                    );
                }
            }
        }

        Ok(())
    }

    /// Reconcile a single namespace
    pub async fn reconcile_namespace(&self, namespace: &Namespace) -> Result<i64> {
        let name = &namespace.metadata.name;

        // Check if namespace is being deleted
        if namespace.metadata.deletion_timestamp.is_some() {
            info!("Namespace {} is being deleted, starting finalization", name);
            return self.finalize_namespace(namespace).await;
        }

        // Ensure kube-root-ca.crt ConfigMap exists with correct CA data.
        // K8s rootcacertpublisher checks if the data matches and updates if not.
        // See: pkg/controller/certificates/rootcacertpublisher/publisher.go:syncNamespace()
        let cm_key = build_key("configmaps", Some(name), "kube-root-ca.crt");
        // Prefer the CA threaded in from the resolved kubeconfig; fall back to
        // the api-server's hardcoded cert paths (present only when the
        // controller shares the api-server's filesystem, e.g. some tests).
        let ca_cert = self.ca_cert.clone().unwrap_or_else(|| {
            std::fs::read_to_string("/etc/kubernetes/pki/ca.crt")
                .or_else(|_| std::fs::read_to_string("/root/.rusternetes/certs/ca.crt"))
                .unwrap_or_default()
        });
        if !ca_cert.is_empty() {
            let expected_data = serde_json::json!({ "ca.crt": ca_cert });
            match self.storage.get::<serde_json::Value>(&cm_key).await {
                Ok(existing) => {
                    // Check if data matches — update if not (handles manual modification)
                    let current_data = existing.get("data");
                    if current_data != Some(&expected_data) {
                        let mut cm = existing.clone();
                        if let Some(obj) = cm.as_object_mut() {
                            obj.insert("data".to_string(), expected_data);
                        }
                        let _ = self.storage.update(&cm_key, &cm).await;
                        debug!(
                            "Updated kube-root-ca.crt in namespace {} (data mismatch)",
                            name
                        );
                    }
                }
                Err(_) => {
                    // ConfigMap doesn't exist — create it
                    let cm = serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "kube-root-ca.crt",
                            "namespace": name
                        },
                        "data": expected_data
                    });
                    if self.storage.create(&cm_key, &cm).await.is_ok() {
                        info!("Created kube-root-ca.crt ConfigMap in namespace {}", name);
                    }
                }
            }
        }

        debug!("Namespace {} is active", name);
        Ok(0)
    }

    /// Build the standard set of namespace deletion conditions.
    /// These conditions indicate the namespace controller has processed the namespace.
    ///
    /// Backward-compatible shim that derives the detailed-totals form below
    /// without per-GVR / per-finalizer breakdowns. Only used by the existing
    /// unit tests in this file, which assert the *bool-summary* shape — the
    /// production finalizer path goes through
    /// [`Self::build_deletion_conditions_with_totals`] directly.
    pub fn build_deletion_conditions(
        content_remaining: bool,
        finalizers_remaining: bool,
    ) -> Vec<NamespaceCondition> {
        let mut gvr_remaining: BTreeMap<String, usize> = BTreeMap::new();
        if content_remaining {
            // Generic placeholder so the "ContentRemaining" branch fires with
            // a stable message in the legacy callers/unit tests.
            gvr_remaining.insert("resources.".to_string(), 1);
        }
        let mut finalizers: BTreeMap<String, usize> = BTreeMap::new();
        if finalizers_remaining {
            finalizers.insert("kubernetes.io/finalizer".to_string(), 1);
        }
        Self::build_deletion_conditions_with_totals(&gvr_remaining, &finalizers, &[])
    }

    /// Upstream-faithful variant of [`Self::build_deletion_conditions`]:
    /// renders the per-GVR remaining count and per-finalizer breakdown into
    /// the exact message format produced by
    /// `pkg/controller/namespace/deletion/status_condition_utils.go`
    /// (release-1.35) so integration tests can byte-compare.
    ///
    /// `gvr_remaining` keys are `"<resource>.<group>"` strings (see
    /// [`resource_type_to_gvr_label`]); the function preserves insertion order
    /// by relying on `BTreeMap`'s sorted iteration — matching upstream's
    /// `sort.Strings` on the rendered entries.
    ///
    /// `content_delete_errors` lists *transport / API-level* errors the
    /// deleter hit while removing content (NOT "items still remain because of
    /// finalizers"). When empty, `NamespaceDeletionContentFailure` stays at
    /// its "ok" message — this is the upstream invariant the integration
    /// test pins.
    fn build_deletion_conditions_with_totals(
        gvr_remaining: &BTreeMap<String, usize>,
        finalizers_to_num_remaining: &BTreeMap<String, usize>,
        content_delete_errors: &[String],
    ) -> Vec<NamespaceCondition> {
        let now = Utc::now();

        // NamespaceDeletionDiscoveryFailure — always "ok" since we do not
        // perform discovery (no aggregated API). Mirrors upstream's
        // `newSuccessfulCondition` branch.
        let discovery_failure = NamespaceCondition {
            condition_type: "NamespaceDeletionDiscoveryFailure".to_string(),
            status: "False".to_string(),
            last_transition_time: Some(now),
            reason: Some("ResourcesDiscovered".to_string()),
            message: Some("All resources successfully discovered".to_string()),
        };

        // NamespaceDeletionGroupVersionParsingFailure — same: no upstream-style
        // GroupVersion parsing happens, so always "ok".
        let gv_parsing_failure = NamespaceCondition {
            condition_type: "NamespaceDeletionGroupVersionParsingFailure".to_string(),
            status: "False".to_string(),
            last_transition_time: Some(now),
            reason: Some("ParsedGroupVersions".to_string()),
            message: Some("All legacy kube types successfully parsed".to_string()),
        };

        // NamespaceDeletionContentFailure — "ok" unless we recorded a real
        // delete error. Items remaining because of finalizers do NOT flip
        // this condition (upstream: `makeDeleteContentCondition` only fires
        // when `deleteContentErrors` is non-empty).
        let content_failure = if content_delete_errors.is_empty() {
            NamespaceCondition {
                condition_type: "NamespaceDeletionContentFailure".to_string(),
                status: "False".to_string(),
                last_transition_time: Some(now),
                reason: Some("ContentDeleted".to_string()),
                message: Some(
                    "All content successfully deleted, may be waiting on finalization".to_string(),
                ),
            }
        } else {
            let mut sorted = content_delete_errors.to_vec();
            sorted.sort();
            NamespaceCondition {
                condition_type: "NamespaceDeletionContentFailure".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(now),
                reason: Some("ContentDeletionFailed".to_string()),
                message: Some(format!(
                    "Failed to delete all resource types, {} remaining: {}",
                    sorted.len(),
                    sorted.join(", ")
                )),
            }
        };

        // NamespaceContentRemaining — only "True" with detailed message when
        // we still have at least one remaining instance of any GVR.
        let content_remaining = if gvr_remaining.is_empty() {
            NamespaceCondition {
                condition_type: "NamespaceContentRemaining".to_string(),
                status: "False".to_string(),
                last_transition_time: Some(now),
                reason: Some("ContentRemoved".to_string()),
                message: Some("All content successfully removed".to_string()),
            }
        } else {
            // upstream sorts the rendered fragments. BTreeMap iteration is
            // already sorted by key, but the rendered string includes the
            // count too, so we collect-then-sort to mirror that exactly.
            let mut parts: Vec<String> = gvr_remaining
                .iter()
                .filter(|(_, n)| **n > 0)
                .map(|(gvr, n)| format!("{} has {} resource instances", gvr, n))
                .collect();
            parts.sort();
            NamespaceCondition {
                condition_type: "NamespaceContentRemaining".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(now),
                reason: Some("SomeResourcesRemain".to_string()),
                message: Some(format!(
                    "Some resources are remaining: {}",
                    parts.join(", ")
                )),
            }
        };

        // NamespaceFinalizersRemaining — likewise. Upstream message format:
        // `"Some content in the namespace has finalizers remaining: %s in %d resource instances, ..."`
        let finalizers_remaining = if finalizers_to_num_remaining.is_empty() {
            NamespaceCondition {
                condition_type: "NamespaceFinalizersRemaining".to_string(),
                status: "False".to_string(),
                last_transition_time: Some(now),
                reason: Some("ContentHasNoFinalizers".to_string()),
                message: Some("All content-preserving finalizers finished".to_string()),
            }
        } else {
            let mut parts: Vec<String> = finalizers_to_num_remaining
                .iter()
                .filter(|(_, n)| **n > 0)
                .map(|(fin, n)| format!("{} in {} resource instances", fin, n))
                .collect();
            parts.sort();
            NamespaceCondition {
                condition_type: "NamespaceFinalizersRemaining".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(now),
                reason: Some("SomeFinalizersRemain".to_string()),
                message: Some(format!(
                    "Some content in the namespace has finalizers remaining: {}",
                    parts.join(", ")
                )),
            }
        };

        // Order matches upstream's `conditionTypes` slice for stable output.
        vec![
            discovery_failure,
            gv_parsing_failure,
            content_failure,
            content_remaining,
            finalizers_remaining,
        ]
    }

    /// Finalize a namespace by deleting all resources within it
    /// Returns the seconds to wait before the next pass (upstream's
    /// `ResourcesRemainingError.Estimate`, `Delete` `:140-142`); 0 = none.
    pub async fn finalize_namespace(&self, namespace: &Namespace) -> Result<i64> {
        let name = &namespace.metadata.name;

        info!("Finalizing namespace {}", name);

        // List of resource types to delete (in dependency order)
        let resource_types = vec![
            // Workload resources first
            "pods",
            "replicationcontrollers",
            "replicasets",
            "deployments",
            "statefulsets",
            "daemonsets",
            "jobs",
            "cronjobs",
            // Configuration resources
            "configmaps",
            "secrets",
            "serviceaccounts",
            // Networking resources
            "services",
            "endpoints",
            "endpointslices",
            "ingresses",
            "networkpolicies",
            // Storage resources
            "persistentvolumeclaims",
            // Policy resources
            "poddisruptionbudgets",
            "resourcequotas",
            "limitranges",
            // RBAC resources
            "roles",
            "rolebindings",
            // Events
            "events",
            // Autoscaling
            "horizontalpodautoscalers",
            // Leases
            "leases",
            // Resource claims (DRA)
            "resourceclaims",
            "resourceclaimtemplates",
            // Other
            "controllerrevisions",
            "podtemplates",
            "csistoragecapacities",
        ];

        // Delete pods first and wait briefly for graceful termination.
        // K8s deletes pods before other resources so pods can access configmaps/secrets
        // during shutdown. We set deletionTimestamp on pods, wait, then delete everything.
        {
            let pod_prefix = build_prefix("pods", Some(name));
            if let Ok(pods) = self.storage.list::<Pod>(&pod_prefix).await {
                for pod in &pods {
                    if pod.metadata.deletion_timestamp.is_none() {
                        let pod_key = build_key("pods", Some(name), &pod.metadata.name);
                        if let Ok(mut p) = self.storage.get::<Pod>(&pod_key).await {
                            p.metadata.deletion_timestamp = Some(chrono::Utc::now());
                            let _ = self.storage.update(&pod_key, &p).await;
                        }
                    }
                }
                if !pods.is_empty() {
                    // Brief wait for kubelet to process termination
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                }
            }
        }

        // Delete resources in the namespace in TWO phases to match K8s ordering.
        // Phase 1: Delete pods. K8s deletes pods before other resources so
        // pods can access configmaps/secrets during shutdown.
        // K8s ref: pkg/controller/namespace/deletion/namespaced_resources_deleter.go
        let mut gvr_to_num_remaining: BTreeMap<String, usize> = BTreeMap::new();
        let mut finalizers_to_num_remaining: BTreeMap<String, usize> = BTreeMap::new();
        // Per-resource-type *delete* errors (API/transport-level), separate
        // from "items still remain because of finalizers". Mirrors
        // upstream's `deleteContentErrors` slice that powers
        // `NamespaceDeletionContentFailure` (namespaced_resources_deleter.go:530-536).
        let mut content_delete_errors: Vec<String> = Vec::new();

        let mut any_finalizers_remaining = false;
        // Pods still present after the delete pass. Upstream keys the ordering
        // gate on exactly this (`gvrToNumRemaining[podsGVR] > 0`), not on
        // whether the pod had a finalizer or on any prior bookkeeping.
        let mut pods_remaining = 0usize;
        let deleted_at = namespace
            .metadata
            .deletion_timestamp
            .unwrap_or_else(Utc::now);
        let mut estimate = 0i64;
        match self
            .delete_all_resources_with_metadata(name, "pods", deleted_at)
            .await
        {
            Ok(meta) => {
                estimate = estimate.max(meta.finalizer_estimate_seconds);
                pods_remaining = meta.num_remaining;
                if meta.num_remaining > 0 {
                    gvr_to_num_remaining
                        .insert(resource_type_to_gvr_label("pods"), meta.num_remaining);
                    for (f, c) in &meta.finalizers_to_num_remaining {
                        *finalizers_to_num_remaining.entry(f.clone()).or_insert(0) += *c;
                    }
                    any_finalizers_remaining = true;
                }
            }
            Err(e) => {
                warn!("Failed to delete pods in namespace {}: {}", name, e);
                content_delete_errors.push(format!("pods: {e}"));
            }
        }

        // Ordered deletion: while ANY pod remains, refresh the conditions and
        // stop. Other content must outlive every pod in the namespace, because
        // a terminating pod may still read its ConfigMaps and Secrets during
        // shutdown.
        //
        // Upstream returns early here on EVERY pass for as long as pods remain
        // (`pkg/controller/namespace/deletion/namespaced_resources_deleter.go:553-562`):
        //
        //     // Check if any pods remain before proceeding to delete other resources
        //     if numRemainingTotals.gvrToNumRemaining[podsGVR] > 0 {
        //         ... conditionUpdater.Update(ns) ... UpdateStatus ...
        //         return estimate, utilerrors.NewAggregate(errs)
        //     }
        //
        // This gate used to be "pods have finalizers AND we have not written
        // the conditions yet", which let the SECOND reconcile delete
        // ConfigMaps and Secrets out from under a pod that was still
        // terminating — and failed the `OrderedNamespaceDeletion` Conformance
        // spec, which holds a pod open with a finalizer and then asserts its
        // ConfigMap still exists.
        let _ = any_finalizers_remaining;
        if pods_remaining > 0 {
            let conditions = Self::build_deletion_conditions_with_totals(
                &gvr_to_num_remaining,
                &finalizers_to_num_remaining,
                &content_delete_errors,
            );
            let key = build_key("namespaces", None, name);
            if let Ok(mut ns) = self.storage.get::<Namespace>(&key).await {
                ns.status = Some(NamespaceStatus {
                    phase: Some(Phase::Terminating),
                    conditions: Some(conditions),
                });
                // Status subresource write: a full-object PUT through the
                // api-server strips `.status` (#1723).
                let _ = self.storage.update_status(&key, &ns).await;
                info!(
                    "Namespace {} still has {} pod(s); delaying deletion of other content",
                    name, pods_remaining
                );
            }
            return Ok(estimate);
        }

        // Phase 2: Delete every other resource type. For each, accumulate
        // per-GVR remaining count and per-finalizer breakdown into totals,
        // mirroring upstream's `numRemainingTotals` aggregation.
        for resource_type in &resource_types {
            if *resource_type == "pods" {
                continue; // Already processed
            }
            match self
                .delete_all_resources_with_metadata(name, resource_type, deleted_at)
                .await
            {
                Ok(meta) => {
                    estimate = estimate.max(meta.finalizer_estimate_seconds);
                    if meta.num_remaining > 0 {
                        gvr_to_num_remaining.insert(
                            resource_type_to_gvr_label(resource_type),
                            meta.num_remaining,
                        );
                        any_finalizers_remaining = true;
                        for (f, c) in &meta.finalizers_to_num_remaining {
                            *finalizers_to_num_remaining.entry(f.clone()).or_insert(0) += *c;
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to delete {} in namespace {}: {}",
                        resource_type, name, e
                    );
                    content_delete_errors.push(format!("{resource_type}: {e}"));
                }
            }
        }

        // Clean up cluster-scoped webhook configurations that reference this namespace.
        // Without this, stale webhooks cause watch cancel loops in subsequent tests.
        self.cleanup_webhook_configs_for_namespace(name).await;

        // Check if all resources are deleted
        let remaining_count = self.count_remaining_resources(name).await?;

        // Update namespace status with conditions indicating the controller has processed it.
        // This is required for conformance — tests check that the namespace controller
        // has set these conditions before considering the namespace "processed".
        {
            let key = build_key("namespaces", None, name);
            let mut ns: Namespace = self.storage.get(&key).await?;

            let conditions = Self::build_deletion_conditions_with_totals(
                &gvr_to_num_remaining,
                &finalizers_to_num_remaining,
                &content_delete_errors,
            );

            ns.status = Some(NamespaceStatus {
                phase: Some(Phase::Terminating),
                conditions: Some(conditions),
            });

            // Save the updated status with conditions.
            // Retry up to 3 times on CAS conflict — other writers (API server,
            // garbage collector) may update the namespace concurrently.
            info!(
                "Setting deletion conditions on namespace {} (remaining={}, finalizers={})",
                name,
                remaining_count > 0,
                any_finalizers_remaining
            );
            for attempt in 0..3 {
                let fresh_ns_result = if attempt == 0 {
                    Ok(ns.clone())
                } else {
                    self.storage.get::<Namespace>(&key).await
                };
                match fresh_ns_result {
                    Ok(mut fresh_ns) => {
                        let conditions = Self::build_deletion_conditions_with_totals(
                            &gvr_to_num_remaining,
                            &finalizers_to_num_remaining,
                            &content_delete_errors,
                        );
                        fresh_ns.status = Some(NamespaceStatus {
                            phase: Some(Phase::Terminating),
                            conditions: Some(conditions),
                        });
                        // Status subresource write (#1723).
                        match self.storage.update_status(&key, &fresh_ns).await {
                            Ok(_) => {
                                info!("Namespace {} conditions set successfully", name);
                                break;
                            }
                            Err(e) => {
                                warn!(
                                    "Namespace {} condition update attempt {} failed: {}",
                                    name,
                                    attempt + 1,
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to re-read namespace {}: {}", name, e);
                        break;
                    }
                }
            }
        }

        if remaining_count > 0 {
            info!(
                "Namespace {} still has {} resources, will retry",
                name, remaining_count
            );
            return Ok(estimate); // Retried after the estimate, else on resync
        }

        // Check if conditions were ALREADY set when we entered this function.
        // We set conditions above (line 295), but we must not finalize in the
        // same cycle — the test needs time to observe the Terminating state.
        // Only proceed to finalization if conditions were present at function entry.
        let conditions_already_set_at_entry = namespace
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|c| {
                c.iter()
                    .any(|cond| cond.condition_type == "NamespaceDeletionContentFailure")
            })
            .unwrap_or(false);
        if !conditions_already_set_at_entry {
            info!(
                "Namespace {} resources cleared, conditions set (will finalize next cycle)",
                name
            );
            return Ok(0);
        }

        // Re-fetch the live namespace, then retire ONLY the built-in
        // `kubernetes` finalizer — custom finalizers belong to external actors
        // and must keep the namespace Terminating until they clear them
        // (upstream pkg/controller/namespace/deletion).
        //
        // Persist the finalizer removal through the `/finalize` subresource —
        // do NOT call a normal update. The api-server preserves
        // `spec.finalizers` on ordinary namespace PUTs, and namespace DELETE is
        // a no-op that only sets Terminating. Actual storage removal happens on
        // the finalize path once the finalizer slice drains while the
        // namespace is Terminating (mirrors upstream
        // `ShouldDeleteNamespaceDuringUpdate`). Calling `delete` here returned a
        // false `Ok` and left the namespace stuck Terminating, looping forever
        // (#1161). Persisting the empty finalizer list lets the api-server
        // collect it; a remaining custom finalizer keeps it Terminating until
        // its owner clears it (the next finalize, once empty, collects it).
        let key = build_key("namespaces", None, name);
        let mut ns: Namespace = self.storage.get(&key).await?;

        if let Some(ref mut fins) = ns.spec.as_mut().and_then(|spec| spec.finalizers.as_mut()) {
            fins.retain(|f| f != "kubernetes");
        }
        let drained = ns
            .spec
            .as_ref()
            .and_then(|spec| spec.finalizers.as_ref())
            .is_none_or(|f| f.is_empty());

        match self.storage.update_subresource(&key, "finalize", &ns).await {
            Ok(_) => info!(
                "Namespace {} finalized (kubernetes finalizer cleared)",
                name
            ),
            Err(e) => warn!("Failed to persist namespace {} finalization: {}", name, e),
        }

        // Once the finalizer slice is empty, remove the object. Against the
        // api-server the update above already collected it (the finalize path
        // removes a drained Terminating namespace — `ShouldDeleteNamespace-
        // DuringUpdate`), so this `delete` is a harmless NotFound there; but a
        // direct-storage backend (the all-in-one's StorageBackend, and tests
        // that drive the controller over `MemoryStorage`) has no finalize hook,
        // so the explicit delete is what actually collects it. A remaining
        // custom finalizer leaves `drained` false → not removed.
        if drained {
            let _ = self.storage.delete(&key).await;
        }

        info!("Namespace {} finalization complete", name);
        Ok(0)
    }

    /// Clean up cluster-scoped webhook configurations that reference a deleted namespace.
    async fn cleanup_webhook_configs_for_namespace(&self, namespace: &str) {
        // ValidatingWebhookConfigurations
        let vwc_prefix = "/registry/validatingwebhookconfigurations/";
        if let Ok(configs) = self.storage.list::<serde_json::Value>(vwc_prefix).await {
            for config in configs {
                let references_ns = config
                    .pointer("/webhooks")
                    .and_then(|w| w.as_array())
                    .map(|webhooks| {
                        webhooks.iter().any(|wh| {
                            wh.pointer("/clientConfig/service/namespace")
                                .and_then(|n| n.as_str())
                                == Some(namespace)
                        })
                    })
                    .unwrap_or(false);
                if references_ns {
                    let name = config
                        .pointer("/metadata/name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("");
                    let key = format!("{}{}", vwc_prefix, name);
                    let _ = self.storage.delete(&key).await;
                    info!(
                        "Cleaned up ValidatingWebhookConfiguration {} (namespace {} deleted)",
                        name, namespace
                    );
                }
            }
        }
        // MutatingWebhookConfigurations
        let mwc_prefix = "/registry/mutatingwebhookconfigurations/";
        if let Ok(configs) = self.storage.list::<serde_json::Value>(mwc_prefix).await {
            for config in configs {
                let references_ns = config
                    .pointer("/webhooks")
                    .and_then(|w| w.as_array())
                    .map(|webhooks| {
                        webhooks.iter().any(|wh| {
                            wh.pointer("/clientConfig/service/namespace")
                                .and_then(|n| n.as_str())
                                == Some(namespace)
                        })
                    })
                    .unwrap_or(false);
                if references_ns {
                    let name = config
                        .pointer("/metadata/name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("");
                    let key = format!("{}{}", mwc_prefix, name);
                    let _ = self.storage.delete(&key).await;
                    info!(
                        "Cleaned up MutatingWebhookConfiguration {} (namespace {} deleted)",
                        name, namespace
                    );
                }
            }
        }
    }

    /// Per-resource-type cleanup outcome, mirroring upstream's
    /// `gvrDeletionMetadata` from
    /// `pkg/controller/namespace/deletion/namespaced_resources_deleter.go`.
    /// Used to drive the upstream-faithful condition messages.
    async fn delete_all_resources_with_metadata(
        &self,
        namespace: &str,
        resource_type: &str,
        namespace_deleted_at: chrono::DateTime<Utc>,
    ) -> Result<GvrDeletionMetadata> {
        let prefix = build_prefix(resource_type, Some(namespace));

        // `estimateGracefulTermination` (`:421`) runs BEFORE the delete.
        let mut estimate = 0i64;
        if resource_type == "pods" {
            let pods: Vec<serde_json::Value> = self.storage.list(&prefix).await?;
            estimate = clamp_estimate_to_namespace_age(
                estimate_graceful_termination_for_pods(&pods),
                namespace_deleted_at,
                Utc::now(),
            );
        }

        // First try to delete the entire collection with one call
        // (`deleteAllContentForGroupVersionResource`, upstream
        // namespaced_resources_deleter.go:429-435). When the verb is
        // unsupported — remembered per type, as upstream's opCache — fall
        // through to the per-item path below (`deleteEachItem`, `:393-415`).
        // An error is returned to the caller, as upstream does (`:431-433`).
        let cached_unsupported = self
            .delete_collection_unsupported
            .lock()
            .unwrap()
            .contains(resource_type);
        if !cached_unsupported && !self.storage.delete_collection(&prefix).await? {
            self.delete_collection_unsupported
                .lock()
                .unwrap()
                .insert(resource_type.to_string());
        }

        // The verification list (`:437-441`): whatever is still here is stuck
        // on a finalizer or is mid-graceful-termination.
        let resources: Vec<serde_json::Value> =
            self.storage.list(&prefix).await.unwrap_or_default();
        if resources.is_empty() {
            return Ok(GvrDeletionMetadata::default()); // `:454-457`
        }

        let mut finalizers_to_num_remaining: BTreeMap<String, usize> = BTreeMap::new();
        let mut num_remaining = 0usize;

        for resource in resources {
            let Some(metadata) = resource.get("metadata") else {
                continue;
            };
            let Some(name) = metadata.get("name").and_then(|n| n.as_str()) else {
                continue;
            };
            let key = build_key(resource_type, Some(namespace), name);

            // A pod's phase does NOT license removing it while a finalizer is
            // still attached. Upstream's only terminal-pod special case is in
            // `estimateGracefulTerminationForPods`
            // (namespaced_resources_deleter.go:643-648), where a Succeeded or
            // Failed pod contributes nothing to the grace-period *estimate*;
            // the deletion itself goes through the API, which honours
            // finalizers for every object regardless of phase. Hard-deleting a
            // terminal pod here retired the namespace out from under a pod
            // whose finalizer had not been cleared, failing the conformance
            // spec "[sig-api-machinery] OrderedNamespaceDeletion namespace
            // deletion should delete pod first" with "namespace was deleted
            // unexpectedly". Terminal pods without finalizers still fall
            // through to the plain hard delete below.
            let finalizers = metadata
                .get("finalizers")
                .and_then(|f| f.as_array())
                .map(|f| {
                    f.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            if !finalizers.is_empty() {
                num_remaining += 1;
                for f in &finalizers {
                    *finalizers_to_num_remaining.entry(f.clone()).or_insert(0) += 1;
                }
                // Stays in storage until its finalizers drain. Mark it for
                // deletion with a DELETE and let the registry stamp
                // `deletionTimestamp` (upstream `deleteEachItem`,
                // namespaced_resources_deleter.go:393-415, only ever calls
                // Delete). A PUT cannot: the generic Store keeps the stored
                // `deletionTimestamp` on update (#2723).
                let already_terminating = metadata
                    .get("deletionTimestamp")
                    .and_then(|d| d.as_str())
                    .is_some();
                if !already_terminating {
                    if let Err(e) = self.storage.delete_gracefully(&key).await {
                        warn!(
                            "Failed to mark {}/{}/{} for deletion: {}",
                            resource_type, namespace, name, e
                        );
                    }
                }
            } else {
                // No finalizers. Upstream `deleteEachItem`
                // (namespaced_resources_deleter.go:393-415) sends
                // `DeleteOptions{PropagationPolicy: Background}` with no grace
                // override, so the server picks the default (graceful) period;
                // a grace-0 hard delete here diverged (#2729).
                if let Err(e) = self.storage.delete_gracefully(&key).await {
                    if !matches!(e, rusternetes_common::Error::NotFound(_)) {
                        warn!(
                            "Failed to delete {}/{}/{}: {}",
                            resource_type, namespace, name, e
                        );
                    }
                }
            }
        }

        // `:467-484`: an estimate wins; otherwise finalizer-held items get the
        // default so GC can finish.
        if estimate == 0 && !finalizers_to_num_remaining.is_empty() {
            estimate = FINALIZER_ESTIMATE_SECONDS;
        }
        Ok(GvrDeletionMetadata {
            num_remaining,
            finalizers_to_num_remaining,
            finalizer_estimate_seconds: estimate,
        })
    }

    // Note: the original `delete_all_resources` helper has been replaced by
    // `delete_all_resources_with_metadata` above, which returns a
    // `GvrDeletionMetadata` so the controller can produce upstream-faithful
    // condition messages (per-GVR remaining count + finalizer breakdown).

    /// Count remaining resources in a namespace.
    /// Checks all resource types that are deleted during finalization.
    async fn count_remaining_resources(&self, namespace: &str) -> Result<usize> {
        let resource_types = vec![
            "pods",
            "replicationcontrollers",
            "replicasets",
            "deployments",
            "statefulsets",
            "daemonsets",
            "jobs",
            "cronjobs",
            "configmaps",
            "secrets",
            "serviceaccounts",
            "services",
            "endpoints",
            "endpointslices",
            "ingresses",
            "networkpolicies",
            "persistentvolumeclaims",
            "poddisruptionbudgets",
            "resourcequotas",
            "limitranges",
            "roles",
            "rolebindings",
            "events",
            "horizontalpodautoscalers",
            "leases",
            "controllerrevisions",
            "podtemplates",
        ];
        let mut total = 0;

        for resource_type in resource_types {
            let prefix = build_prefix(resource_type, Some(namespace));
            let resources: Vec<serde_json::Value> =
                self.storage.list(&prefix).await.unwrap_or_default();
            total += resources.len();
        }

        Ok(total)
    }

    /// Remove lifecycle finalizers from a namespace through `/finalize`.
    #[allow(dead_code)]
    async fn remove_namespace_finalizers(&self, name: &str) -> Result<()> {
        let key = build_key("namespaces", None, name);

        // Get current namespace
        let mut namespace: Namespace = self.storage.get(&key).await?;

        // Remove all finalizers
        namespace
            .spec
            .get_or_insert_with(Default::default)
            .finalizers = None;

        self.storage
            .update_subresource(&key, "finalize", &namespace)
            .await?;

        Ok(())
    }
}
