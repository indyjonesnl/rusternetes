use crate::controllers::worker_pool::spawn_workers;
use anyhow::Result;
use chrono::Utc;
use rusternetes_common::resources::autoscaling::ResourceMetricStatus;
use rusternetes_common::resources::{
    Deployment, HorizontalPodAutoscaler, HorizontalPodAutoscalerCondition,
    HorizontalPodAutoscalerStatus, MetricSpec, MetricStatus, MetricValueStatus, ReplicaSet,
    StatefulSet,
};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::controllers::hpa_metrics_client::{
    format_milli_quantity, FakeMetricsClient, HttpMetricsClient, HttpMetricsConfig, MetricsClient,
    PodMetricsInfo,
};
use crate::controllers::hpa_replica_calculator as calc;

/// Workers draining this controller's queue: upstream `ConcurrentHorizontalPodAutoscalerSyncs` default,
/// pkg/controller/podautoscaler/config/v1alpha1/defaults.go:37-38, launched at cmd/kube-controller-manager/app/autoscaling.go:103.
const CONCURRENT_HPA_SYNCS: usize = 5;

pub struct HorizontalPodAutoscalerController<S: Storage> {
    storage: Arc<S>,
    metrics_client: Arc<dyn MetricsClient>,
    /// Per-HPA scale-event history feeding `spec.behavior` rate policies.
    scale_events: crate::controllers::hpa_behavior::ScaleEventStore,
    /// Per-HPA recommendation history feeding `spec.behavior`
    /// stabilizationWindowSeconds.
    recommendations: crate::controllers::hpa_behavior::RecommendationStore,
}

impl<S: Storage + 'static> HorizontalPodAutoscalerController<S> {
    /// Default-config constructor — builds an `HttpMetricsClient` from
    /// `HttpMetricsConfig::default()`. Binaries thread explicit config via
    /// `with_config`, so this is only used by tests.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new(storage: Arc<S>) -> Self {
        Self::with_config(storage, HttpMetricsConfig::default())
    }

    pub fn with_config(storage: Arc<S>, cfg: HttpMetricsConfig) -> Self {
        let metrics_client: Arc<dyn MetricsClient> = match HttpMetricsClient::new(cfg) {
            Ok(c) => Arc::new(c),
            Err(e) => {
                warn!("HPA metrics client unavailable ({e:#}); metric fetches will fail");
                Arc::new(FakeMetricsClient::new())
            }
        };
        Self {
            storage,
            metrics_client,
            scale_events: Default::default(),
            recommendations: Default::default(),
        }
    }

    /// Test/explicit constructor — inject any MetricsClient.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_metrics_client(storage: Arc<S>, metrics_client: Arc<dyn MetricsClient>) -> Self {
        Self {
            storage,
            metrics_client,
            scale_events: Default::default(),
            recommendations: Default::default(),
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting HorizontalPodAutoscaler controller");

        let queue = WorkQueue::new();

        // Upstream launches `ConcurrentHorizontalPodAutoscalerSyncs` workers over one shared queue
        // (`for i := 0; i < workers; i++ { go wait.UntilWithContext(ctx, worker, time.Second) }`);
        // a key in flight is never handed to a second worker.
        spawn_workers(CONCURRENT_HPA_SYNCS, &queue, |worker_queue| {
            let worker_self = Arc::clone(&self);
            async move {
                worker_self.worker(worker_queue).await;
            }
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("horizontalpodautoscalers", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
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
                                tracing::warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Watch stream ended, reconnecting");
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
            let storage_key = build_key("horizontalpodautoscalers", Some(ns), name);
            match self
                .storage
                .get::<HorizontalPodAutoscaler>(&storage_key)
                .await
            {
                Ok(resource) => match self.reconcile_hpa(&resource).await {
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
            .list::<HorizontalPodAutoscaler>("/registry/horizontalpodautoscalers/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("horizontalpodautoscalers/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list horizontalpodautoscalers for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Reconciling all HorizontalPodAutoscalers");

        // Get all HPAs across all namespaces
        let prefix = build_prefix("horizontalpodautoscalers", None);
        let hpas: Vec<HorizontalPodAutoscaler> = self.storage.list(&prefix).await?;

        for hpa in hpas {
            if let Err(e) = self.reconcile_hpa(&hpa).await {
                warn!(
                    "Failed to reconcile HPA {}/{}: {}",
                    hpa.metadata.namespace.as_deref().unwrap_or("default"),
                    hpa.metadata.name,
                    e
                );
            }
        }

        Ok(())
    }

    async fn reconcile_hpa(&self, hpa: &HorizontalPodAutoscaler) -> Result<()> {
        let namespace = hpa.metadata.namespace.as_deref().unwrap_or("default");
        debug!("Reconciling HPA: {}/{}", namespace, hpa.metadata.name);

        let target_ref = &hpa.spec.scale_target_ref;
        debug!(
            "HPA {} targets {}/{} - min: {:?}, max: {}",
            hpa.metadata.name,
            target_ref.kind,
            target_ref.name,
            hpa.spec.min_replicas,
            hpa.spec.max_replicas
        );

        // 1. Get current replica count from target resource
        let current_replicas = match self.get_current_replicas(namespace, target_ref).await {
            Ok(replicas) => replicas,
            Err(e) => {
                warn!(
                    "Failed to get current replicas for HPA {}/{}: {}",
                    namespace, hpa.metadata.name, e
                );
                // Update status with error condition
                self.update_hpa_status_with_error(hpa, &format!("Failed to get target: {}", e))
                    .await?;
                return Ok(());
            }
        };

        debug!(
            "Current replicas for {}/{}: {}",
            namespace, target_ref.name, current_replicas
        );

        // 2. Calculate desired replica count based on metrics
        let (desired_replicas, metric_statuses) = match self
            .calculate_desired_replicas(hpa, current_replicas, namespace)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "Failed to calculate desired replicas for HPA {}/{}: {}",
                    namespace, hpa.metadata.name, e
                );
                // Update status with error condition
                self.update_hpa_status_with_error(
                    hpa,
                    &format!("Failed to compute replicas: {}", e),
                )
                .await?;
                return Ok(());
            }
        };

        debug!(
            "Desired replicas for {}/{}: {}",
            namespace, target_ref.name, desired_replicas
        );

        // 3. If desired replicas differ from current, scale the target
        if desired_replicas != current_replicas {
            info!(
                "Scaling {}/{} from {} to {} replicas",
                namespace, target_ref.name, current_replicas, desired_replicas
            );

            if let Err(e) = self
                .scale_target(namespace, target_ref, desired_replicas)
                .await
            {
                error!(
                    "Failed to scale target for HPA {}/{}: {}",
                    namespace, hpa.metadata.name, e
                );
                self.update_hpa_status_with_error(hpa, &format!("Failed to scale: {}", e))
                    .await?;
                return Ok(());
            }

            // Record the scale event so spec.behavior period windows rate-limit
            // subsequent reconciles (upstream storeScaleEvent).
            if let Some(behavior) = hpa.spec.behavior.as_ref() {
                let key = format!("{}/{}", namespace, hpa.metadata.name);
                self.scale_events.record(
                    &key,
                    behavior,
                    current_replicas,
                    desired_replicas,
                    Utc::now(),
                );
            }
        }

        // 4. Update HPA status
        self.update_hpa_status_success(hpa, current_replicas, desired_replicas, metric_statuses)
            .await?;

        Ok(())
    }

    /// Get current replica count from the target resource
    pub async fn get_current_replicas(
        &self,
        namespace: &str,
        target_ref: &rusternetes_common::resources::CrossVersionObjectReference,
    ) -> Result<i32> {
        match target_ref.kind.as_str() {
            "Deployment" => {
                let key = build_key("deployments", Some(namespace), &target_ref.name);
                let deployment: Deployment = self.storage.get(&key).await?;
                Ok(deployment.spec.replicas.unwrap_or(1))
            }
            "ReplicaSet" => {
                let key = build_key("replicasets", Some(namespace), &target_ref.name);
                let replicaset: ReplicaSet = self.storage.get(&key).await?;
                Ok(replicaset.spec.replicas)
            }
            "StatefulSet" => {
                let key = build_key("statefulsets", Some(namespace), &target_ref.name);
                let statefulset: StatefulSet = self.storage.get(&key).await?;
                Ok(statefulset.spec.replicas.unwrap_or(1))
            }
            _ => Err(anyhow::anyhow!(
                "Unsupported scale target kind: {}",
                target_ref.kind
            )),
        }
    }

    /// Scale the target resource to the desired replica count
    pub async fn scale_target(
        &self,
        namespace: &str,
        target_ref: &rusternetes_common::resources::CrossVersionObjectReference,
        desired_replicas: i32,
    ) -> Result<()> {
        match target_ref.kind.as_str() {
            "Deployment" => {
                let key = build_key("deployments", Some(namespace), &target_ref.name);
                let mut deployment: Deployment = self.storage.get(&key).await?;
                deployment.spec.replicas = Some(desired_replicas);
                self.storage.update(&key, &deployment).await?;
                info!(
                    "Scaled Deployment {}/{} to {} replicas",
                    namespace, target_ref.name, desired_replicas
                );
            }
            "ReplicaSet" => {
                let key = build_key("replicasets", Some(namespace), &target_ref.name);
                let mut replicaset: ReplicaSet = self.storage.get(&key).await?;
                replicaset.spec.replicas = desired_replicas;
                self.storage.update(&key, &replicaset).await?;
                info!(
                    "Scaled ReplicaSet {}/{} to {} replicas",
                    namespace, target_ref.name, desired_replicas
                );
            }
            "StatefulSet" => {
                let key = build_key("statefulsets", Some(namespace), &target_ref.name);
                let mut statefulset: StatefulSet = self.storage.get(&key).await?;
                statefulset.spec.replicas = Some(desired_replicas);
                self.storage.update(&key, &statefulset).await?;
                info!(
                    "Scaled StatefulSet {}/{} to {} replicas",
                    namespace, target_ref.name, desired_replicas
                );
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported scale target kind: {}",
                    target_ref.kind
                ));
            }
        }
        Ok(())
    }

    /// Resolve the label selector of the HPA's scale target (for pod metrics).
    /// List pods in `namespace` whose labels match every entry of the target's
    /// `matchLabels` (the HPA target's pods).
    async fn list_pods_for_selector(
        &self,
        namespace: &str,
        selector: &rusternetes_common::types::LabelSelector,
    ) -> Vec<rusternetes_common::resources::pod::Pod> {
        let prefix = build_prefix("pods", Some(namespace));
        let pods: Vec<rusternetes_common::resources::pod::Pod> =
            self.storage.list(&prefix).await.unwrap_or_default();
        let want = selector.match_labels.clone().unwrap_or_default();
        pods.into_iter()
            .filter(|p| {
                let labels = p.metadata.labels.as_ref();
                want.iter()
                    .all(|(k, v)| labels.and_then(|l| l.get(k)) == Some(v))
            })
            .collect()
    }

    /// Restrict a cpu `PodMetricsInfo` to pods that are ready/measurable per the
    /// initial-readiness / cpu-initialization windows (upstream `groupPods`).
    /// If no pods can be listed (no pod state available) the metrics are left
    /// unfiltered, preserving prior behaviour.
    async fn filter_ready_cpu_pods(
        &self,
        namespace: &str,
        selector: &rusternetes_common::types::LabelSelector,
        info: PodMetricsInfo,
    ) -> PodMetricsInfo {
        use crate::controllers::hpa_pod_grouping as grp;
        let pods = self.list_pods_for_selector(namespace, selector).await;
        if pods.is_empty() {
            return info;
        }
        let ready = grp::ready_cpu_pods(
            &pods,
            &info,
            Utc::now(),
            grp::cpu_initialization_period(),
            grp::initial_readiness_delay(),
        );
        info.into_iter()
            .filter(|(k, _)| ready.contains(k))
            .collect()
    }

    async fn target_selector(
        &self,
        namespace: &str,
        target_ref: &rusternetes_common::resources::CrossVersionObjectReference,
    ) -> rusternetes_common::types::LabelSelector {
        use rusternetes_common::types::LabelSelector;
        match target_ref.kind.as_str() {
            "Deployment" => {
                let key = build_key("deployments", Some(namespace), &target_ref.name);
                self.storage
                    .get::<Deployment>(&key)
                    .await
                    .ok()
                    .map(|d| d.spec.selector)
                    .unwrap_or_default()
            }
            "ReplicaSet" => {
                let key = build_key("replicasets", Some(namespace), &target_ref.name);
                self.storage
                    .get::<ReplicaSet>(&key)
                    .await
                    .ok()
                    .map(|r| r.spec.selector)
                    .unwrap_or_default()
            }
            "StatefulSet" => {
                let key = build_key("statefulsets", Some(namespace), &target_ref.name);
                self.storage
                    .get::<StatefulSet>(&key)
                    .await
                    .ok()
                    .map(|s| s.spec.selector)
                    .unwrap_or_default()
            }
            _ => LabelSelector::default(),
        }
    }

    /// Calculate desired replica count based on metrics
    /// Implements the HPA algorithm: desiredReplicas = ceil[currentReplicas * (currentMetricValue / targetMetricValue)]
    pub async fn calculate_desired_replicas(
        &self,
        hpa: &HorizontalPodAutoscaler,
        current_replicas: i32,
        namespace: &str,
    ) -> Result<(i32, Vec<MetricStatus>)> {
        let metrics = match &hpa.spec.metrics {
            Some(m) if !m.is_empty() => m,
            _ => return Ok((current_replicas, Vec::new())),
        };

        let mut max_desired = current_replicas;
        let mut statuses = Vec::new();
        for metric in metrics {
            let (desired, status) = self
                .calculate_replicas_for_metric(metric, current_replicas, namespace, hpa)
                .await?;
            if desired > max_desired {
                max_desired = desired;
            }
            statuses.push(status);
        }

        let min_replicas = hpa.spec.min_replicas.unwrap_or(1);
        let max_replicas = hpa.spec.max_replicas;
        // Apply spec.behavior scale-rate policies (Pods/Percent, selectPolicy,
        // periodSeconds history) before the final min/max clamp; mirrors
        // upstream normalizeDesiredReplicasWithBehaviors. Without behavior, the
        // plain clamp is unchanged.
        let rated = if let Some(behavior) = hpa.spec.behavior.as_ref() {
            use crate::controllers::hpa_behavior as bhv;
            let key = format!("{}/{}", namespace, hpa.metadata.name);
            let now = Utc::now();

            // 1. Stabilize against the recommendation history (upstream
            // stabilizeRecommendationWithBehaviors): hold at a recent high
            // during a transient dip / recent low during a spike. Record the
            // *unstabilized* recommendation, then prune to the longer window.
            let up_w = behavior
                .scale_up
                .as_ref()
                .and_then(|r| r.stabilization_window_seconds)
                .unwrap_or(0);
            let down_w = behavior
                .scale_down
                .as_ref()
                .and_then(|r| r.stabilization_window_seconds)
                .unwrap_or(0);
            let recs = self.recommendations.snapshot(&key);
            let stabilized = bhv::stabilize_recommendation(
                current_replicas,
                max_desired,
                up_w,
                down_w,
                &recs,
                now,
            );
            self.recommendations
                .record(&key, max_desired, up_w.max(down_w), now);

            // 2. Rate-limit the stabilized recommendation per the scale policies.
            let events = self.scale_events.snapshot(&key);
            bhv::convert_with_behavior_rate(
                current_replicas,
                stabilized,
                min_replicas,
                max_replicas,
                behavior,
                &events,
                now,
            )
        } else {
            max_desired
        };
        let bounded = rated.max(min_replicas).min(max_replicas);
        Ok((bounded, statuses))
    }

    /// Calculate desired replicas + status for a single metric.
    pub async fn calculate_replicas_for_metric(
        &self,
        metric: &MetricSpec,
        current_replicas: i32,
        namespace: &str,
        hpa: &HorizontalPodAutoscaler,
    ) -> Result<(i32, MetricStatus)> {
        let selector = self
            .target_selector(namespace, &hpa.spec.scale_target_ref)
            .await;
        match metric.metric_type.as_str() {
            "Resource" => {
                let r = metric
                    .resource
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Resource metric without resource field"))?;
                let target = r.target.average_utilization.unwrap_or(80);
                let info = self
                    .metrics_client
                    .get_resource_metric(&r.name, namespace, &selector)
                    .await?;
                // For cpu, drop not-yet-ready / just-initializing pods so a
                // cold-start spike doesn't trigger a scale-up (upstream
                // groupPods CPU branch). When all pods are excluded the metric
                // set is empty and get_resource_replicas errors → no scale.
                let info = if r.name == "cpu" {
                    self.filter_ready_cpu_pods(namespace, &selector, info).await
                } else {
                    info
                };
                let (replicas, avg) = calc::get_resource_replicas(&info, current_replicas, target)?;
                let status = MetricStatus {
                    metric_type: "Resource".into(),
                    resource: Some(ResourceMetricStatus {
                        name: r.name.clone(),
                        current: MetricValueStatus {
                            value: None,
                            average_value: None,
                            average_utilization: Some(avg),
                        },
                    }),
                    pods: None,
                    object: None,
                    external: None,
                    container_resource: None,
                };
                Ok((replicas, status))
            }
            "ContainerResource" => {
                let cr = metric.container_resource.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("ContainerResource metric without containerResource field")
                })?;
                let target = cr.target.average_utilization.unwrap_or(80);
                let info = self
                    .metrics_client
                    .get_container_resource_metric(&cr.name, &cr.container, namespace, &selector)
                    .await?;
                let (replicas, avg) = calc::get_resource_replicas(&info, current_replicas, target)?;
                let status = MetricStatus {
                    metric_type: "ContainerResource".into(),
                    resource: None,
                    pods: None,
                    object: None,
                    external: None,
                    container_resource: Some(
                        rusternetes_common::resources::ContainerResourceMetricStatus {
                            name: cr.name.clone(),
                            container: cr.container.clone(),
                            current: MetricValueStatus {
                                value: None,
                                average_value: None,
                                average_utilization: Some(avg),
                            },
                        },
                    ),
                };
                Ok((replicas, status))
            }
            "Pods" => {
                let p = metric
                    .pods
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Pods metric without pods field"))?;
                let target = parse_target_avg(&p.target)?;
                // horizontal.go:478-482: LabelSelectorAsSelector(spec.Pods.Metric.Selector).
                let metric_selector = p
                    .metric
                    .selector
                    .as_ref()
                    .map(|s| s.as_selector_string())
                    .transpose()
                    .map_err(|e| anyhow::anyhow!("failed to get pods metric value: {e}"))?
                    .unwrap_or_default();
                let (info, _timestamp) = self
                    .metrics_client
                    .get_raw_metric(&p.metric.name, namespace, &selector, &metric_selector)
                    .await
                    // replica_calculator.go:185 `GetMetricReplicas`.
                    .map_err(|e| anyhow::anyhow!("unable to get metric {}: {e}", p.metric.name))?;
                let (replicas, avg) = calc::get_metric_replicas(&info, current_replicas, target)?;
                let status = MetricStatus {
                    metric_type: "Pods".into(),
                    resource: None,
                    pods: Some(rusternetes_common::resources::PodsMetricStatus {
                        metric: p.metric.clone(),
                        current: MetricValueStatus {
                            value: None,
                            average_value: Some(format_milli_quantity(avg)),
                            average_utilization: None,
                        },
                    }),
                    object: None,
                    external: None,
                    container_resource: None,
                };
                Ok((replicas, status))
            }
            "Object" => {
                let o = metric
                    .object
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Object metric without object field"))?;
                let target = parse_target_value(&o.target)?;
                // horizontal.go:468-472: LabelSelectorAsSelector(spec.Object.Metric.Selector).
                let metric_selector = o
                    .metric
                    .selector
                    .as_ref()
                    .map(|s| s.as_selector_string())
                    .transpose()
                    .map_err(|e| anyhow::anyhow!("failed to get object metric value: {e}"))?
                    .unwrap_or_default();
                let (value, _) = self
                    .metrics_client
                    .get_object_metric(
                        &o.metric.name,
                        namespace,
                        &o.described_object,
                        &metric_selector,
                    )
                    .await
                    // replica_calculator.go:272 `GetObjectMetricReplicas`
                    // (the message order is upstream's).
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "unable to get metric {}: {} on {} {}/{e}",
                            o.metric.name,
                            o.described_object.kind,
                            namespace,
                            o.described_object.name
                        )
                    })?;
                let (replicas, v) =
                    calc::get_object_metric_replicas(value, current_replicas, target)?;
                // Report the field matching the target type: AverageValue
                // targets report currentAverageValue = value / replicas; Value
                // targets report the raw value. (k8s ObjectMetricStatus.)
                let current = if o.target.target_type == "AverageValue" {
                    let replicas = current_replicas.max(1) as i64;
                    MetricValueStatus {
                        value: None,
                        average_value: Some(format_milli_quantity(v / replicas)),
                        average_utilization: None,
                    }
                } else {
                    MetricValueStatus {
                        value: Some(format_milli_quantity(v)),
                        average_value: None,
                        average_utilization: None,
                    }
                };
                let status = MetricStatus {
                    metric_type: "Object".into(),
                    resource: None,
                    pods: None,
                    object: Some(rusternetes_common::resources::ObjectMetricStatus {
                        metric: o.metric.clone(),
                        described_object: o.described_object.clone(),
                        current,
                    }),
                    external: None,
                    container_resource: None,
                };
                Ok((replicas, status))
            }
            "External" => {
                let e = metric
                    .external
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("External metric without external field"))?;
                let target = parse_target_avg(&e.target)?;
                // replica_calculator.go:355-358: LabelSelectorAsSelector(metricSelector).
                let metric_selector = e
                    .metric
                    .selector
                    .as_ref()
                    .map(|s| s.as_selector_string())
                    .transpose()
                    .map_err(|e| anyhow::anyhow!("{e}"))?
                    .unwrap_or_default();
                let (values, _) = self
                    .metrics_client
                    .get_external_metric(&e.metric.name, namespace, &metric_selector)
                    .await
                    // replica_calculator.go:361 `GetExternalMetricReplicas`.
                    .map_err(|err| {
                        anyhow::anyhow!(
                            "unable to get external metric {}/{}/{}: {err}",
                            namespace,
                            e.metric.name,
                            go_label_selector_string(e.metric.selector.as_ref())
                        )
                    })?;
                let (replicas, sum) = calc::get_external_metric_replicas(&values, target)?;
                // k8s reports currentAverageValue = sum / currentReplicas for an
                // AverageValue target (the scaling math already used the per-pod
                // target); the raw sum would mislabel the status field.
                let avg = sum / (current_replicas.max(1) as i64);
                let status = MetricStatus {
                    metric_type: "External".into(),
                    resource: None,
                    pods: None,
                    object: None,
                    external: Some(rusternetes_common::resources::ExternalMetricStatus {
                        metric: e.metric.clone(),
                        current: MetricValueStatus {
                            value: None,
                            average_value: Some(format_milli_quantity(avg)),
                            average_utilization: None,
                        },
                    }),
                    container_resource: None,
                };
                Ok((replicas, status))
            }
            other => anyhow::bail!("unsupported metric type: {other}"),
        }
    }

    /// Update HPA status with success
    async fn update_hpa_status_success(
        &self,
        hpa: &HorizontalPodAutoscaler,
        current_replicas: i32,
        desired_replicas: i32,
        metric_statuses: Vec<MetricStatus>,
    ) -> Result<()> {
        let namespace = hpa.metadata.namespace.as_deref().unwrap_or("default");
        let key = build_key(
            "horizontalpodautoscalers",
            Some(namespace),
            &hpa.metadata.name,
        );

        let mut updated_hpa = hpa.clone();

        let current_metrics = if metric_statuses.is_empty() {
            None
        } else {
            Some(metric_statuses)
        };

        // Build conditions
        let now = Utc::now();
        let mut conditions = vec![
            HorizontalPodAutoscalerCondition {
                condition_type: "AbleToScale".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(now),
                reason: Some("ReadyForNewScale".to_string()),
                message: Some(
                    "the HPA controller was able to get the target's current scale".to_string(),
                ),
            },
            HorizontalPodAutoscalerCondition {
                condition_type: "ScalingActive".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(now),
                reason: Some("ValidMetricFound".to_string()),
                message: Some(
                    "the HPA was able to successfully calculate a replica count from the metrics"
                        .to_string(),
                ),
            },
        ];

        // Add ScalingLimited condition if at min/max
        let min_replicas = hpa.spec.min_replicas.unwrap_or(1);
        let max_replicas = hpa.spec.max_replicas;
        if desired_replicas >= max_replicas {
            conditions.push(HorizontalPodAutoscalerCondition {
                condition_type: "ScalingLimited".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(now),
                reason: Some("TooManyReplicas".to_string()),
                message: Some(format!(
                    "the desired replica count is more than the maximum replica count of {}",
                    max_replicas
                )),
            });
        } else if desired_replicas <= min_replicas {
            conditions.push(HorizontalPodAutoscalerCondition {
                condition_type: "ScalingLimited".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(now),
                reason: Some("TooFewReplicas".to_string()),
                message: Some(format!(
                    "the desired replica count is less than the minimum replica count of {}",
                    min_replicas
                )),
            });
        } else {
            conditions.push(HorizontalPodAutoscalerCondition {
                condition_type: "ScalingLimited".to_string(),
                status: "False".to_string(),
                last_transition_time: Some(now),
                reason: Some("DesiredWithinRange".to_string()),
                message: Some(
                    "the desired replica count is within the acceptable range".to_string(),
                ),
            });
        }

        let last_scale_time = if current_replicas != desired_replicas {
            Some(Utc::now())
        } else {
            hpa.status.as_ref().and_then(|s| s.last_scale_time)
        };

        // K8s convention: condition.last_transition_time updates ONLY when the
        // condition's .status field actually transitions (e.g. False -> True).
        // Without this preservation we'd stamp Utc::now() every reconcile and
        // produce a MODIFIED watch event every interval, even when nothing
        // changed — a controller-side hot-loop source.
        let prev_conditions = hpa
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        for new_cond in conditions.iter_mut() {
            if let Some(prev) = prev_conditions
                .iter()
                .find(|p| p.condition_type == new_cond.condition_type)
            {
                if prev.status == new_cond.status {
                    new_cond.last_transition_time = prev.last_transition_time;
                }
            }
        }

        updated_hpa.status = Some(HorizontalPodAutoscalerStatus {
            observed_generation: None, // Would need generation tracking in ObjectMeta
            last_scale_time,
            current_replicas,
            desired_replicas,
            current_metrics,
            conditions: Some(conditions),
        });

        // Skip the write when nothing actually changed. Compare via canonical
        // JSON so optional field ordering doesn't trigger spurious updates.
        let old_status_json = serde_json::to_value(hpa.status.as_ref()).ok();
        let new_status_json = serde_json::to_value(updated_hpa.status.as_ref()).ok();
        if old_status_json == new_status_json {
            debug!(
                "HPA {}/{} status unchanged — skipping write",
                namespace, hpa.metadata.name
            );
            return Ok(());
        }

        // Status subresource write (a full-object PUT strips `.status`,
        // #1723), conditional on the resourceVersion of the HPA this sync read:
        // upstream `updateStatus` (pkg/controller/podautoscaler/horizontal.go)
        // is `UpdateStatus` on `hpa.DeepCopy()`. A Conflict propagates so the
        // worker requeues rate-limited (#2153).
        self.storage.update_status_cas(&key, &updated_hpa).await?;
        debug!("Updated HPA status: {}/{}", namespace, hpa.metadata.name);

        Ok(())
    }

    /// Update HPA status with error condition
    async fn update_hpa_status_with_error(
        &self,
        hpa: &HorizontalPodAutoscaler,
        error_msg: &str,
    ) -> Result<()> {
        let namespace = hpa.metadata.namespace.as_deref().unwrap_or("default");
        let key = build_key(
            "horizontalpodautoscalers",
            Some(namespace),
            &hpa.metadata.name,
        );

        let mut updated_hpa = hpa.clone();
        let now = Utc::now();

        let current_replicas = hpa.status.as_ref().map(|s| s.current_replicas).unwrap_or(0);

        let conditions = vec![
            HorizontalPodAutoscalerCondition {
                condition_type: "AbleToScale".to_string(),
                status: "False".to_string(),
                last_transition_time: Some(now),
                reason: Some("FailedGetScale".to_string()),
                message: Some(error_msg.to_string()),
            },
            HorizontalPodAutoscalerCondition {
                condition_type: "ScalingActive".to_string(),
                status: "False".to_string(),
                last_transition_time: Some(now),
                reason: Some("FailedComputeMetricsReplicas".to_string()),
                message: Some(error_msg.to_string()),
            },
        ];

        updated_hpa.status = Some(HorizontalPodAutoscalerStatus {
            observed_generation: None,
            last_scale_time: hpa.status.as_ref().and_then(|s| s.last_scale_time),
            current_replicas,
            desired_replicas: current_replicas,
            current_metrics: None,
            conditions: Some(conditions),
        });

        // Same conditional status write as the success path (#2153).
        self.storage.update_status_cas(&key, &updated_hpa).await?;
        debug!(
            "Updated HPA status with error: {}/{}",
            namespace, hpa.metadata.name
        );

        Ok(())
    }
}

/// `fmt.Sprintf("%+v", *metav1.LabelSelector)`: the generated `String()`
/// (apimachinery/pkg/apis/meta/v1/generated.pb.go:3157-3194).
pub fn go_label_selector_string(sel: Option<&rusternetes_common::types::LabelSelector>) -> String {
    let Some(sel) = sel else {
        return "nil".to_string();
    };
    let mut exprs = String::from("[]LabelSelectorRequirement{");
    for r in sel.match_expressions.iter().flatten() {
        exprs.push_str(&format!(
            "LabelSelectorRequirement{{Key:{},Operator:{},Values:[{}],}},",
            r.key,
            r.operator,
            r.values.as_deref().unwrap_or_default().join(" ")
        ));
    }
    exprs.push('}');
    let mut labels: Vec<_> = sel.match_labels.iter().flatten().collect();
    labels.sort();
    let mut map = String::from("map[string]string{");
    for (k, v) in labels {
        map.push_str(&format!("{k}: {v},"));
    }
    map.push('}');
    format!("&LabelSelector{{MatchLabels:{map},MatchExpressions:{exprs},}}")
}

/// `Target.AverageValue.MilliValue()` (horizontal.go:468,476,727).
fn parse_target_avg(t: &rusternetes_common::resources::MetricTarget) -> Result<i64> {
    t.average_value
        .as_ref()
        .and_then(|s| crate::controllers::hpa_metrics_client::parse_milli(s))
        .ok_or_else(|| anyhow::anyhow!("metric target missing/invalid averageValue"))
}

fn parse_target_value(t: &rusternetes_common::resources::MetricTarget) -> Result<i64> {
    t.value
        .as_ref()
        .or(t.average_value.as_ref())
        .and_then(|s| crate::controllers::hpa_metrics_client::parse_milli(s))
        .ok_or_else(|| anyhow::anyhow!("metric target missing/invalid value"))
}
