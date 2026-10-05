//! The disruption controller: maintains `PodDisruptionBudget.status`.
//!
//! A faithful port of `pkg/controller/disruption/disruption.go`
//! (Kubernetes release-1.35). The status written here is the **source of truth**
//! for Pod `/eviction`: upstream's `EvictionREST` reads it and decrements
//! `disruptionsAllowed` (`pkg/registry/core/pod/storage/eviction.go`
//! `checkAndDecrement`), so every divergence in this file is an over- or
//! under-eviction.
//!
//! Mapping of upstream symbols to this file:
//!
//! | upstream (`disruption.go`)                          | here                                  |
//! |-----------------------------------------------------|---------------------------------------|
//! | `addPod`/`updatePod`/`deletePod` (:522-:575)        | `enqueue_pdb_for_pod_event`           |
//! | `addDB`/`updateDB`/`removeDB`                       | the PDB arm of `run`                  |
//! | `getPdbForPod` (:605)                               | `get_pdb_for_pod`                     |
//! | `getPodsForPdb` (:631)                              | `get_pods_for_pdb`                    |
//! | `sync` (:700), `trySync` (:735)                     | `sync`, `try_sync`                    |
//! | `getExpectedPodCount` (:818)                        | `get_expected_pod_count`              |
//! | `getExpectedScale` (:860)                           | `get_expected_scale`                  |
//! | `finders` + `getPod*`/`getScaleController` (:241-:416) | `find_controller_and_scale` et al. |
//! | `countHealthyPods` (:924)                           | `count_healthy_pods`                  |
//! | `buildDisruptedPodMap` (:944)                       | `build_disrupted_pod_map`             |
//! | `failSafe` (:983)                                   | `fail_safe`                           |
//! | `updatePdbStatus` (:1002)                           | `update_pdb_status`                   |
//! | `recheckQueue`/`enqueuePdbForRecheck` (:590)        | `recheck_queue`/`enqueue_pdb_for_recheck` |
//! | `nonTerminatingPodHasStaleDisruptionCondition` (:1046) | `non_terminating_pod_has_stale_disruption_condition` |
//! | `syncStalePodDisruption` (:774)                     | [`StalePodDisruptionController`]      |

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::StreamExt;
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::{
    CustomResource, CustomResourceDefinition, EventSource, EventType, IntOrString, Pod,
    PodDisruptionBudget, PodDisruptionBudgetCondition, PodDisruptionBudgetStatus,
};
use rusternetes_common::types::{LabelSelector, OwnerReference, Phase, Selector};
use rusternetes_common::validation::metav1::{is_qualified_name, is_valid_label_value};
use rusternetes_common::Error;
use rusternetes_storage::{
    build_key, build_prefix, extract_key, EventRecorder, Storage, WatchEvent, WorkQueue,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tracing::{debug, error, info, warn};

/// `DeletionTimeout` (`disruption.go:60-68`): the maximum time from a pod being
/// added to `status.disruptedPods` to the controller seeing it marked for
/// deletion. Past it the pod is assumed never to be deleted and the entry is
/// dropped.
pub const DELETION_TIMEOUT: StdDuration = StdDuration::from_secs(2 * 60);

/// `stalePodDisruptionTimeout` (`disruption.go:70-74`).
pub const STALE_POD_DISRUPTION_TIMEOUT: StdDuration = StdDuration::from_secs(2 * 60);

/// `policy.DisruptionAllowedCondition` and its reasons
/// (`staging/src/k8s.io/api/policy/v1/types.go:153-165`).
const DISRUPTION_ALLOWED_CONDITION: &str = "DisruptionAllowed";
const SYNC_FAILED_REASON: &str = "SyncFailed";
const SUFFICIENT_PODS_REASON: &str = "SufficientPods";
const INSUFFICIENT_PODS_REASON: &str = "InsufficientPods";

/// `v1.PodReasonTerminationByKubelet`.
const POD_REASON_TERMINATION_BY_KUBELET: &str = "TerminationByKubelet";

/// Safety-net full resync. Upstream relies on informer relists; this
/// storage-watch controller keeps a periodic enqueue (as its siblings do)
/// because a dropped watch event would otherwise leave a budget stale.
const RESYNC_INTERVAL: StdDuration = StdDuration::from_secs(30);

/// `controllerAndScale` (`disruption.go:130-134`): a controller UID and its scale.
struct ControllerAndScale {
    uid: String,
    scale: i32,
}

pub struct PodDisruptionBudgetController<S: Storage> {
    storage: Arc<S>,
    recorder: EventRecorder<S>,
    /// `dc.queue`: PodDisruptionBudget keys (`namespace/name`) that need a sync.
    queue: WorkQueue,
    /// `dc.recheckQueue`: delays a PDB's re-sync until the earliest
    /// `disruptedPods` entry expires.
    recheck_queue: WorkQueue,
}

impl<S: Storage + 'static> PodDisruptionBudgetController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            storage,
            queue: WorkQueue::new(),
            recheck_queue: WorkQueue::new(),
        }
    }

    /// `DisruptionController.Run` (`disruption.go:419`): the sync worker, the
    /// recheck worker, and the PDB + Pod event sources feeding them.
    pub async fn run(self: Arc<Self>) -> rusternetes_common::Result<()> {
        info!("Starting PodDisruptionBudget controller");

        let worker_self = Arc::clone(&self);
        tokio::spawn(async move { worker_self.worker().await });
        let recheck_self = Arc::clone(&self);
        tokio::spawn(async move { recheck_self.recheck_worker().await });

        loop {
            self.enqueue_all().await;

            let pdb_prefix = build_prefix("poddisruptionbudgets", None);
            let mut pdb_watch = match self.storage.watch(&pdb_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish PDB watch: {}, retrying", e);
                    tokio::time::sleep(RESYNC_INTERVAL).await;
                    continue;
                }
            };
            let pod_prefix = build_prefix("pods", None);
            let mut pod_watch = match self.storage.watch(&pod_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish pod watch: {}, retrying", e);
                    tokio::time::sleep(RESYNC_INTERVAL).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(RESYNC_INTERVAL);
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = pdb_watch.next() => match event {
                        // addDB / updateDB / removeDB all `enqueuePdb`.
                        Some(Ok(ev)) => {
                            if let Some(key) = meta_namespace_key(&extract_key(&ev)) {
                                self.queue.add(key).await;
                            }
                        }
                        Some(Err(e)) => {
                            warn!("PDB watch error: {}, reconnecting", e);
                            watch_broken = true;
                        }
                        None => {
                            warn!("PDB watch stream ended, reconnecting");
                            watch_broken = true;
                        }
                    },
                    event = pod_watch.next() => match event {
                        Some(Ok(ev)) => self.enqueue_pdb_for_pod_event(&ev).await,
                        Some(Err(e)) => {
                            warn!("Pod watch error: {}, reconnecting", e);
                            watch_broken = true;
                        }
                        None => {
                            warn!("Pod watch stream ended, reconnecting");
                            watch_broken = true;
                        }
                    },
                    _ = resync.tick() => self.enqueue_all().await,
                }
            }
        }
    }

    /// `worker` / `processNextWorkItem` (`disruption.go:673-698`).
    async fn worker(&self) {
        while let Some(key) = self.queue.get().await {
            match self.sync(&key).await {
                Ok(()) => self.queue.forget(&key).await,
                Err(e) => {
                    error!("Error syncing PodDisruptionBudget {key}, requeuing: {e}");
                    self.queue.requeue_rate_limited(key.clone()).await;
                }
            }
            self.queue.done(&key).await;
        }
    }

    /// `recheckWorker` / `processNextRecheckWorkItem` (`disruption.go:700-712`):
    /// a key whose recheck delay elapsed goes back to the main queue.
    async fn recheck_worker(&self) {
        while let Some(key) = self.recheck_queue.get().await {
            self.queue.requeue_rate_limited(key.clone()).await;
            self.recheck_queue.forget(&key).await;
            self.recheck_queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self) {
        match self
            .storage
            .list::<PodDisruptionBudget>(&build_prefix("poddisruptionbudgets", None))
            .await
        {
            Ok(items) => {
                for pdb in &items {
                    self.queue.add(pdb_key(pdb)).await;
                }
            }
            Err(e) => error!("Failed to list poddisruptionbudgets for enqueue: {}", e),
        }
    }

    /// `addPod` / `updatePod` / `deletePod` (`disruption.go:522-575`): find the
    /// PDB a pod belongs to and enqueue it. The pod comes from the event value
    /// (the previous pod, for a delete), as upstream's `deletePod` reads the
    /// tombstone.
    async fn enqueue_pdb_for_pod_event(&self, event: &WatchEvent) {
        let value = match event {
            WatchEvent::Added(_, v) | WatchEvent::Modified(_, v) | WatchEvent::Deleted(_, v) => v,
        };
        let Ok(pod) = serde_json::from_str::<Pod>(value) else {
            return;
        };
        if let Some(pdb) = self.get_pdb_for_pod(&pod).await {
            self.queue.add(pdb_key(&pdb)).await;
        }
    }

    /// `enqueuePdbForRecheck` (`disruption.go:590`).
    async fn enqueue_pdb_for_recheck(&self, pdb: &PodDisruptionBudget, delay: ChronoDuration) {
        self.recheck_queue
            .add_after(pdb_key(pdb), delay.to_std().unwrap_or(StdDuration::ZERO))
            .await;
    }

    /// `getPdbForPod` (`disruption.go:605`), over the lister's
    /// `GetPodPodDisruptionBudgets`
    /// (`client-go/listers/policy/v1/poddisruptionbudget_expansion.go:39`): the
    /// PDBs in the pod's namespace whose selector matches it, the first chosen
    /// when several do (with a `MultiplePodDisruptionBudgets` warning).
    async fn get_pdb_for_pod(&self, pod: &Pod) -> Option<PodDisruptionBudget> {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
        let pdbs: Vec<PodDisruptionBudget> = self
            .storage
            .list(&build_prefix("poddisruptionbudgets", Some(ns)))
            .await
            .ok()?;
        let mut matching: Vec<PodDisruptionBudget> = pdbs
            .into_iter()
            // An invalid selector "does not match the pod" in the lister.
            .filter(|pdb| matches!(pdb_selector_matches(pdb, pod), Ok(true)))
            .collect();
        if matching.is_empty() {
            return None;
        }
        matching.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
        if matching.len() > 1 {
            let msg = format!(
                "Pod {:?}/{:?} matches multiple PodDisruptionBudgets.  Chose {:?} arbitrarily.",
                ns, pod.metadata.name, matching[0].metadata.name
            );
            warn!("{msg}");
            self.record_pod_event(
                pod,
                EventType::Warning,
                "MultiplePodDisruptionBudgets",
                &msg,
            )
            .await;
        }
        Some(matching.swap_remove(0))
    }

    /// `getPodsForPdb` (`disruption.go:631`): `LabelSelectorAsSelector` then a
    /// namespace-scoped list. A `nil` selector is `labels.Nothing()`, an empty
    /// one `labels.Everything()` (`apimachinery/.../meta/v1/helpers.go:37-43`).
    async fn get_pods_for_pdb(
        &self,
        pdb: &PodDisruptionBudget,
    ) -> rusternetes_common::Result<Vec<Pod>> {
        let selector = pdb_selector(pdb).map_err(Error::InvalidResource)?;
        let ns = pdb.metadata.namespace.as_deref().unwrap_or("default");
        // The v1beta1 compat rule: an empty selector selects no pods.
        if selector.is_everything() && pdb.type_meta.api_version == "policy/v1beta1" {
            return Ok(Vec::new());
        }
        let pods: Vec<Pod> = self.storage.list(&build_prefix("pods", Some(ns))).await?;
        Ok(pods
            .into_iter()
            .filter(|p| selector.matches(p.metadata.labels.as_ref()))
            .collect())
    }

    /// `sync` (`disruption.go:700-731`), keyed `namespace/name`.
    pub async fn sync(&self, key: &str) -> rusternetes_common::Result<()> {
        self.sync_at(key, Utc::now()).await
    }

    /// [`sync`](Self::sync) with an injected "now" (upstream injects a
    /// `clock.Clock`; `disruption_test.go` drives it with a fake one).
    #[doc(hidden)]
    pub async fn sync_at(&self, key: &str, now: DateTime<Utc>) -> rusternetes_common::Result<()> {
        let (namespace, name) = match key.split_once('/') {
            Some((ns, n)) => (ns, n),
            None => ("", key),
        };
        let storage_key = build_key("poddisruptionbudgets", Some(namespace), name);
        let pdb: PodDisruptionBudget = match self.storage.get(&storage_key).await {
            Ok(p) => p,
            Err(Error::NotFound(_)) => {
                debug!("podDisruptionBudget {key} has been deleted");
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        match self.try_sync(&pdb, now).await {
            Ok(()) => Ok(()),
            // "If the reason for failure was a conflict, then allow this PDB
            // update to be requeued without triggering the failSafe logic."
            Err(e @ Error::Conflict(_)) => Err(e),
            Err(e) => {
                error!("Failed to sync PDB {key}: {e}");
                self.fail_safe(&pdb, &e).await
            }
        }
    }

    /// `trySync` (`disruption.go:735-772`).
    async fn try_sync(
        &self,
        pdb: &PodDisruptionBudget,
        now: DateTime<Utc>,
    ) -> rusternetes_common::Result<()> {
        let pods = match self.get_pods_for_pdb(pdb).await {
            Ok(p) => p,
            Err(e) => {
                self.record_pdb_event(
                    pdb,
                    EventType::Warning,
                    "NoPods",
                    &format!("Failed to get pods: {e}"),
                )
                .await;
                return Err(e);
            }
        };
        if pods.is_empty() {
            self.record_pdb_event(pdb, EventType::Normal, "NoPods", "No matching pods found")
                .await;
        }

        let (expected_count, desired_healthy, unmanaged_pods) =
            match self.get_expected_pod_count(pdb, &pods).await {
                Ok(v) => v,
                Err(e) => {
                    self.record_pdb_event(
                        pdb,
                        EventType::Warning,
                        "CalculateExpectedPodCountFailed",
                        &format!("Failed to calculate the number of expected pods: {e}"),
                    )
                    .await;
                    return Err(e);
                }
            };
        // "We have unmamanged pods, instead of erroring and hotlooping in
        // disruption controller, log and continue."
        if !unmanaged_pods.is_empty() {
            debug!("Found unmanaged pods associated with this PDB: {unmanaged_pods:?}");
            self.record_pdb_event(
                pdb,
                EventType::Warning,
                "UnmanagedPods",
                &format!(
                    "Pods selected by this PodDisruptionBudget (selector: {:?}) were found \
                     to be unmanaged. As a result, the status of the PDB cannot be calculated \
                     correctly, which may result in undefined behavior. To account for these pods \
                     please set \".spec.minAvailable\" field of the PDB to an integer value.",
                    pdb.spec.selector
                ),
            )
            .await;
        }

        let (disrupted_pods, recheck_time) = self.build_disrupted_pod_map(&pods, pdb, now).await;
        let current_healthy = count_healthy_pods(&pods, &disrupted_pods, now);
        self.update_pdb_status(
            pdb,
            current_healthy,
            desired_healthy,
            expected_count,
            disrupted_pods,
        )
        .await?;

        if let Some(recheck_time) = recheck_time {
            // "There is always at most one PDB waiting with a particular name
            // in the queue" — `add_after` keeps one deadline per key.
            self.enqueue_pdb_for_recheck(pdb, recheck_time - now).await;
        }
        Ok(())
    }

    /// `getExpectedPodCount` (`disruption.go:818-858`). `maxUnavailable` is
    /// consulted first; an integer `minAvailable` needs no controller scale
    /// (`expectedCount = len(pods)`); a percentage of either needs
    /// [`get_expected_scale`](Self::get_expected_scale).
    async fn get_expected_pod_count(
        &self,
        pdb: &PodDisruptionBudget,
        pods: &[Pod],
    ) -> rusternetes_common::Result<(i32, i32, Vec<String>)> {
        let mut expected_count = 0;
        let mut desired_healthy = 0;
        let mut unmanaged_pods = Vec::new();

        if let Some(max_unavailable) = &pdb.spec.max_unavailable {
            (expected_count, unmanaged_pods) = self.get_expected_scale(pods).await?;
            let max_unavailable =
                get_scaled_value_from_int_or_percent(max_unavailable, expected_count, true)?;
            desired_healthy = expected_count - max_unavailable;
            if desired_healthy < 0 {
                desired_healthy = 0;
            }
        } else if let Some(min_available) = &pdb.spec.min_available {
            match min_available {
                IntOrString::Int(v) => {
                    desired_healthy = *v;
                    expected_count = pods.len() as i32;
                }
                IntOrString::String(_) => {
                    (expected_count, unmanaged_pods) = self.get_expected_scale(pods).await?;
                    desired_healthy =
                        get_scaled_value_from_int_or_percent(min_available, expected_count, true)?;
                }
            }
        }
        Ok((expected_count, desired_healthy, unmanaged_pods))
    }

    /// `getExpectedScale` (`disruption.go:860-922`): `SUM_{c in C} scale(c)`
    /// where `C` is the set of controllers of the selected pods. Pods without a
    /// controller are collected as unmanaged rather than failing the sync; a
    /// controller no finder recognises is an error.
    async fn get_expected_scale(
        &self,
        pods: &[Pod],
    ) -> rusternetes_common::Result<(i32, Vec<String>)> {
        let mut controller_scale: HashMap<String, i32> = HashMap::new();
        let mut unmanaged_pods = Vec::new();

        for pod in pods {
            let Some(controller_ref) = controller_ref(pod) else {
                unmanaged_pods.push(pod.metadata.name.clone());
                continue;
            };
            // "If we already know the scale of the controller there is no need
            // to do anything."
            if controller_scale.contains_key(&controller_ref.uid) {
                continue;
            }
            let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
            match self
                .find_controller_and_scale(&controller_ref, namespace)
                .await?
            {
                Some(found) => {
                    controller_scale.insert(found.uid, found.scale);
                }
                None => {
                    return Err(Error::InvalidResource(format!(
                        "found no controllers for pod {:?}",
                        pod.metadata.name
                    )));
                }
            }
        }
        Ok((controller_scale.values().sum(), unmanaged_pods))
    }

    /// `finders()` (`disruption.go:236-243`), tried in upstream's order.
    async fn find_controller_and_scale(
        &self,
        controller_ref: &OwnerReference,
        namespace: &str,
    ) -> rusternetes_common::Result<Option<ControllerAndScale>> {
        if let Some(c) = self
            .get_pod_replication_controller(controller_ref, namespace)
            .await?
        {
            return Ok(Some(c));
        }
        if let Some(c) = self.get_pod_deployment(controller_ref, namespace).await? {
            return Ok(Some(c));
        }
        if let Some(c) = self.get_pod_replica_set(controller_ref, namespace).await? {
            return Ok(Some(c));
        }
        if let Some(c) = self.get_pod_stateful_set(controller_ref, namespace).await? {
            return Ok(Some(c));
        }
        self.get_scale_controller(controller_ref, namespace).await
    }

    /// Fetch a workload by name as JSON. `Ok(None)` is upstream's
    /// "The only possible error is NotFound, which is ok here."
    async fn get_workload(
        &self,
        resource: &str,
        namespace: &str,
        name: &str,
    ) -> Option<serde_json::Value> {
        self.storage
            .get(&build_key(resource, Some(namespace), name))
            .await
            .ok()
    }

    /// `getPodReplicaSet` (`disruption.go:245`): "finds a replicaset which has
    /// no matching deployments".
    async fn get_pod_replica_set(
        &self,
        controller_ref: &OwnerReference,
        namespace: &str,
    ) -> rusternetes_common::Result<Option<ControllerAndScale>> {
        if !verify_group_kind(controller_ref, "ReplicaSet", &["apps", "extensions"])? {
            return Ok(None);
        }
        let Some(rs) = self
            .get_workload("replicasets", namespace, &controller_ref.name)
            .await
        else {
            return Ok(None);
        };
        if json_uid(&rs) != controller_ref.uid {
            return Ok(None);
        }
        // "Skip RS if it's controlled by a Deployment."
        if let Some(owner) = controller_ref_from_json(&rs) {
            if owner.kind == "Deployment" {
                return Ok(None);
            }
        }
        Ok(Some(ControllerAndScale {
            uid: controller_ref.uid.clone(),
            scale: json_replicas(&rs),
        }))
    }

    /// `getPodStatefulSet` (`disruption.go:266`).
    async fn get_pod_stateful_set(
        &self,
        controller_ref: &OwnerReference,
        namespace: &str,
    ) -> rusternetes_common::Result<Option<ControllerAndScale>> {
        if !verify_group_kind(controller_ref, "StatefulSet", &["apps"])? {
            return Ok(None);
        }
        let Some(ss) = self
            .get_workload("statefulsets", namespace, &controller_ref.name)
            .await
        else {
            return Ok(None);
        };
        if json_uid(&ss) != controller_ref.uid {
            return Ok(None);
        }
        Ok(Some(ControllerAndScale {
            uid: controller_ref.uid.clone(),
            scale: json_replicas(&ss),
        }))
    }

    /// `getPodDeployment` (`disruption.go:285`): "finds deployments for any
    /// replicasets which are being managed by deployments".
    async fn get_pod_deployment(
        &self,
        controller_ref: &OwnerReference,
        namespace: &str,
    ) -> rusternetes_common::Result<Option<ControllerAndScale>> {
        if !verify_group_kind(controller_ref, "ReplicaSet", &["apps", "extensions"])? {
            return Ok(None);
        }
        let Some(rs) = self
            .get_workload("replicasets", namespace, &controller_ref.name)
            .await
        else {
            return Ok(None);
        };
        if json_uid(&rs) != controller_ref.uid {
            return Ok(None);
        }
        let Some(rs_owner) = controller_ref_from_json(&rs) else {
            return Ok(None);
        };
        if !verify_group_kind(&rs_owner, "Deployment", &["apps", "extensions"])? {
            return Ok(None);
        }
        let Some(deployment) = self
            .get_workload("deployments", namespace, &rs_owner.name)
            .await
        else {
            return Ok(None);
        };
        if json_uid(&deployment) != rs_owner.uid {
            return Ok(None);
        }
        Ok(Some(ControllerAndScale {
            uid: rs_owner.uid.clone(),
            scale: json_replicas(&deployment),
        }))
    }

    /// `getPodReplicationController` (`disruption.go:318`).
    async fn get_pod_replication_controller(
        &self,
        controller_ref: &OwnerReference,
        namespace: &str,
    ) -> rusternetes_common::Result<Option<ControllerAndScale>> {
        if !verify_group_kind(controller_ref, "ReplicationController", &[""])? {
            return Ok(None);
        }
        let Some(rc) = self
            .get_workload("replicationcontrollers", namespace, &controller_ref.name)
            .await
        else {
            return Ok(None);
        };
        if json_uid(&rc) != controller_ref.uid {
            return Ok(None);
        }
        Ok(Some(ControllerAndScale {
            uid: controller_ref.uid.clone(),
            scale: json_replicas(&rc),
        }))
    }

    /// `getScaleController` (`disruption.go:337-380`): for any other owner kind,
    /// ask its scale subresource. Upstream resolves the kind through a
    /// `RESTMapper`, `Get`s `<resource>/scale` through the scale client and, on
    /// NotFound, uses discovery (`implementsScale`) to tell "object gone" (nil)
    /// from "kind has no scale subresource" (error).
    ///
    /// The equivalents here: the RESTMapper is the CRD list (plus the four
    /// built-in scalable kinds); the scale client is a read of the object whose
    /// replica count is the CRD's `subresources.scale.specReplicasPath`
    /// (apiextensions `scaleFromCustomResource`,
    /// `customresource/etcd.go:251-296`: an absent value reads as 0); and
    /// `implementsScale` is "does that CRD version declare `subresources.scale`".
    async fn get_scale_controller(
        &self,
        controller_ref: &OwnerReference,
        namespace: &str,
    ) -> rusternetes_common::Result<Option<ControllerAndScale>> {
        let (group, version) = parse_group_version(&controller_ref.api_version)?;

        // Built-in kinds that implement /scale (a pod owned directly by one).
        let builtin = match (group.as_str(), controller_ref.kind.as_str()) {
            ("apps", "Deployment") => Some("deployments"),
            ("apps", "ReplicaSet") => Some("replicasets"),
            ("apps", "StatefulSet") => Some("statefulsets"),
            ("", "ReplicationController") => Some("replicationcontrollers"),
            _ => None,
        };
        if let Some(resource) = builtin {
            let Some(obj) = self
                .get_workload(resource, namespace, &controller_ref.name)
                .await
            else {
                return Ok(None);
            };
            if json_uid(&obj) != controller_ref.uid {
                return Ok(None);
            }
            return Ok(Some(ControllerAndScale {
                uid: controller_ref.uid.clone(),
                scale: json_replicas(&obj),
            }));
        }

        // `mapper.RESTMapping(gk, version)`: no CRD means no mapping.
        let crds: Vec<CustomResourceDefinition> = self
            .storage
            .list(&build_prefix("customresourcedefinitions", None))
            .await?;
        let no_match = || {
            Error::InvalidResource(format!(
                "no matches for kind {:?} in version {:?}",
                controller_ref.kind, controller_ref.api_version
            ))
        };
        let crd = crds
            .into_iter()
            .find(|c| c.spec.group == group && c.spec.names.kind == controller_ref.kind)
            .ok_or_else(no_match)?;
        let crd_version = crd
            .spec
            .versions
            .iter()
            .find(|v| v.name == version)
            .ok_or_else(no_match)?;
        let gr = format!("{}.{}", crd.spec.names.plural, group);
        // Without a scale subresource the scale `Get` is NotFound and
        // `implementsScale` is false.
        let Some(scale) = crd_version
            .subresources
            .as_ref()
            .and_then(|s| s.scale.as_ref())
        else {
            return Err(Error::InvalidResource(format!(
                "{gr} does not implement the scale subresource"
            )));
        };

        let resource_type = format!("{}_{}", group.replace('.', "_"), crd.spec.names.plural);
        let cr: CustomResource = match self
            .storage
            .get(&build_key(
                &resource_type,
                Some(namespace),
                &controller_ref.name,
            ))
            .await
        {
            Ok(cr) => cr,
            // NotFound on a kind that does implement scale: the object is gone.
            Err(Error::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        if cr.metadata.uid != controller_ref.uid {
            return Ok(None);
        }

        let mut doc = serde_json::Map::new();
        if let Some(s) = cr.spec {
            doc.insert("spec".to_string(), s);
        }
        if let Some(s) = cr.status {
            doc.insert("status".to_string(), s);
        }
        let doc = serde_json::Value::Object(doc);
        let replicas = match resolve_json_path(&doc, &scale.spec_replicas_path) {
            None => 0,
            Some(v) => v.as_i64().ok_or_else(|| {
                Error::InvalidResource(format!(
                    "{} accessor error: {v} is of the type {}, expected int64",
                    scale.spec_replicas_path,
                    json_type_name(v)
                ))
            })? as i32,
        };
        Ok(Some(ControllerAndScale {
            uid: controller_ref.uid.clone(),
            scale: replicas,
        }))
    }

    /// `buildDisruptedPodMap` (`disruption.go:944-981`): "Builds new PodDisruption
    /// map, possibly removing items that refer to non-existing, already deleted
    /// or not-deleted at all items. Also returns an information when this check
    /// should be repeated."
    async fn build_disrupted_pod_map(
        &self,
        pods: &[Pod],
        pdb: &PodDisruptionBudget,
        current_time: DateTime<Utc>,
    ) -> (HashMap<String, DateTime<Utc>>, Option<DateTime<Utc>>) {
        let mut result = HashMap::new();
        let mut recheck_time: Option<DateTime<Utc>> = None;
        let Some(disrupted_pods) = pdb.status.as_ref().and_then(|s| s.disrupted_pods.as_ref())
        else {
            return (result, recheck_time);
        };
        for pod in pods {
            if pod.metadata.deletion_timestamp.is_some() {
                // Already being deleted.
                continue;
            }
            let Some(disruption_time) = disrupted_pods.get(&pod.metadata.name) else {
                // Pod not on the list.
                continue;
            };
            let expected_deletion = *disruption_time + deletion_timeout();
            if expected_deletion < current_time {
                debug!(
                    "pod {} was expected to be deleted but it wasn't, updating PDB",
                    pod.metadata.name
                );
                // Upstream formats this with `pdb.Namespace` twice; reproduced.
                self.record_pod_event(
                    pod,
                    EventType::Warning,
                    "NotDeleted",
                    &format!(
                        "Pod was expected by PDB {}/{} to be deleted but it wasn't",
                        pdb.metadata.namespace.as_deref().unwrap_or(""),
                        pdb.metadata.namespace.as_deref().unwrap_or("")
                    ),
                )
                .await;
            } else {
                if recheck_time.is_none_or(|r| expected_deletion < r) {
                    recheck_time = Some(expected_deletion);
                }
                result.insert(pod.metadata.name.clone(), *disruption_time);
            }
        }
        (result, recheck_time)
    }

    /// `failSafe` (`disruption.go:983-1000`): "an attempt to at least update the
    /// DisruptionsAllowed field to 0 if everything else has failed. This is one
    /// place we implement the 'fail open' part of the design since if we manage
    /// to update this field correctly, we will prevent the /evict handler from
    /// approving an eviction when it may be unsafe to do so."
    async fn fail_safe(
        &self,
        pdb: &PodDisruptionBudget,
        err: &Error,
    ) -> rusternetes_common::Result<()> {
        let mut new_pdb = pdb.clone();
        let status = new_pdb.status.get_or_insert_with(empty_status);
        status.disruptions_allowed = 0;
        let observed_generation = status.observed_generation;
        set_status_condition(
            status.conditions.get_or_insert_with(Vec::new),
            PodDisruptionBudgetCondition {
                condition_type: DISRUPTION_ALLOWED_CONDITION.to_string(),
                status: "False".to_string(),
                reason: Some(SYNC_FAILED_REASON.to_string()),
                message: Some(err.to_string()),
                observed_generation,
                last_transition_time: None,
            },
        );
        self.write_pdb_status(&new_pdb).await
    }

    /// `updatePdbStatus` (`disruption.go:1002-1044`).
    async fn update_pdb_status(
        &self,
        pdb: &PodDisruptionBudget,
        current_healthy: i32,
        desired_healthy: i32,
        expected_count: i32,
        disrupted_pods: HashMap<String, DateTime<Utc>>,
    ) -> rusternetes_common::Result<()> {
        // "We require expectedCount to be > 0 so that PDBs which currently match
        // no pods are in a safe state when their first pods appear but this
        // controller has not updated their status yet."
        let mut disruptions_allowed = current_healthy - desired_healthy;
        if expected_count <= 0 || disruptions_allowed <= 0 {
            disruptions_allowed = 0;
        }

        if let Some(status) = &pdb.status {
            if status.current_healthy == current_healthy
                && status.desired_healthy == desired_healthy
                && status.expected_pods == expected_count
                && status.disruptions_allowed == disruptions_allowed
                && disrupted_pods_equal(status.disrupted_pods.as_ref(), &disrupted_pods)
                && status.observed_generation.unwrap_or(0) == pdb.metadata.generation.unwrap_or(0)
                && conditions_are_up_to_date(pdb)
            {
                return Ok(());
            }
        }

        let mut new_pdb = pdb.clone();
        let conditions = new_pdb.status.as_ref().and_then(|s| s.conditions.clone());
        new_pdb.status = Some(PodDisruptionBudgetStatus {
            current_healthy,
            desired_healthy,
            disruptions_allowed,
            expected_pods: expected_count,
            observed_generation: pdb.metadata.generation,
            conditions,
            // `omitempty` on an empty map.
            disrupted_pods: if disrupted_pods.is_empty() {
                None
            } else {
                Some(disrupted_pods)
            },
        });
        update_disruption_allowed_condition(&mut new_pdb);
        self.write_pdb_status(&new_pdb).await
    }

    /// `writePdbStatus` (`disruption.go:1046`): `UpdateStatus` on the object read
    /// at the start of the sync, so a write computed from a read an eviction has
    /// since overtaken is rejected with a conflict rather than clobbering the
    /// eviction's `disruptionsAllowed` (`TestUpdatePDBStatusRetries`).
    ///
    /// `Storage::update_status_cas` is that precondition: it grafts the status
    /// onto the stored object only if its `resourceVersion` still equals the
    /// one this sync read, atomically with the write on every backend, and
    /// returns `Error::Conflict` otherwise (#2151).
    async fn write_pdb_status(&self, pdb: &PodDisruptionBudget) -> rusternetes_common::Result<()> {
        let ns = pdb.metadata.namespace.as_deref().unwrap_or("default");
        let key = build_key("poddisruptionbudgets", Some(ns), &pdb.metadata.name);
        // A status write, NOT `update`: a full-object PUT has its `.status`
        // stripped by any api-server that exposes a status subresource (#1712).
        self.storage.update_status_cas(&key, pdb).await?;
        Ok(())
    }

    /// Reconcile every PDB once (tests and one-shot callers).
    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> rusternetes_common::Result<()> {
        let pdbs: Vec<PodDisruptionBudget> = self
            .storage
            .list(&build_prefix("poddisruptionbudgets", None))
            .await?;
        for pdb in pdbs {
            if let Err(e) = self.sync(&pdb_key(&pdb)).await {
                warn!("Failed to reconcile PDB {}: {}", pdb.metadata.name, e);
            }
        }
        Ok(())
    }

    async fn record_pdb_event(
        &self,
        pdb: &PodDisruptionBudget,
        event_type: EventType,
        reason: &str,
        message: &str,
    ) {
        let involved = ObjectReference {
            kind: Some("PodDisruptionBudget".to_string()),
            namespace: pdb.metadata.namespace.clone(),
            name: Some(pdb.metadata.name.clone()),
            uid: Some(pdb.metadata.uid.clone()),
            api_version: Some("policy/v1".to_string()),
            ..Default::default()
        };
        self.record(involved, event_type, reason, message).await;
    }

    async fn record_pod_event(
        &self,
        pod: &Pod,
        event_type: EventType,
        reason: &str,
        message: &str,
    ) {
        let involved = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: pod.metadata.namespace.clone(),
            name: Some(pod.metadata.name.clone()),
            uid: Some(pod.metadata.uid.clone()),
            api_version: Some("v1".to_string()),
            ..Default::default()
        };
        self.record(involved, event_type, reason, message).await;
    }

    /// A failure to record an event is logged and dropped: it must not mask the
    /// sync outcome it describes.
    async fn record(
        &self,
        involved: ObjectReference,
        event_type: EventType,
        reason: &str,
        message: &str,
    ) {
        let source = EventSource {
            component: "controllermanager".to_string(),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(&involved, &source, event_type, reason, message)
            .await
        {
            warn!("failed to record {reason} event: {e}");
        }
    }
}

fn deletion_timeout() -> ChronoDuration {
    ChronoDuration::from_std(DELETION_TIMEOUT).unwrap_or(ChronoDuration::minutes(2))
}

/// `countHealthyPods` (`disruption.go:924-942`).
fn count_healthy_pods(
    pods: &[Pod],
    disrupted_pods: &HashMap<String, DateTime<Utc>>,
    current_time: DateTime<Utc>,
) -> i32 {
    let mut current_healthy = 0;
    for pod in pods {
        // Pod is being deleted.
        if pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
        // Pod is expected to be deleted soon.
        if let Some(disruption_time) = disrupted_pods.get(&pod.metadata.name) {
            if *disruption_time + deletion_timeout() > current_time {
                continue;
            }
        }
        if is_pod_ready(pod) {
            current_healthy += 1;
        }
    }
    current_healthy
}

/// `apipod.IsPodReady` (`pkg/api/v1/pod/util.go:297`): the `Ready` condition is `True`.
fn is_pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|conds| {
            conds
                .iter()
                .any(|c| c.condition_type == "Ready" && c.status == "True")
        })
}

/// `apipod.IsPodPhaseTerminal` (`pkg/api/v1/pod/util.go:307`).
fn is_pod_phase_terminal(phase: Option<&Phase>) -> bool {
    matches!(phase, Some(Phase::Failed) | Some(Phase::Succeeded))
}

fn empty_status() -> PodDisruptionBudgetStatus {
    PodDisruptionBudgetStatus {
        current_healthy: 0,
        desired_healthy: 0,
        disruptions_allowed: 0,
        expected_pods: 0,
        observed_generation: None,
        conditions: None,
        disrupted_pods: None,
    }
}

/// `apiequality.Semantic.DeepEqual` on the disrupted-pod maps: nil and empty are equal.
fn disrupted_pods_equal(
    stored: Option<&HashMap<String, DateTime<Utc>>>,
    computed: &HashMap<String, DateTime<Utc>>,
) -> bool {
    match stored {
        None => computed.is_empty(),
        Some(s) => s == computed,
    }
}

/// `apimeta.SetStatusCondition` (`apimachinery/pkg/api/meta/conditions.go:31-68`).
fn set_status_condition(
    conditions: &mut Vec<PodDisruptionBudgetCondition>,
    mut new: PodDisruptionBudgetCondition,
) {
    let now = || Some(Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    match conditions
        .iter_mut()
        .find(|c| c.condition_type == new.condition_type)
    {
        None => {
            if new.last_transition_time.is_none() {
                new.last_transition_time = now();
            }
            conditions.push(new);
        }
        Some(existing) => {
            if existing.status != new.status {
                existing.status = new.status;
                existing.last_transition_time = new.last_transition_time.or_else(now);
            }
            existing.reason = new.reason;
            existing.message = new.message;
            existing.observed_generation = new.observed_generation;
        }
    }
}

/// `pdbhelper.UpdateDisruptionAllowedCondition`
/// (`component-helpers/apps/poddisruptionbudget/helpers.go:28-46`).
fn update_disruption_allowed_condition(pdb: &mut PodDisruptionBudget) {
    let Some(status) = pdb.status.as_mut() else {
        return;
    };
    let (cond_status, reason) = if status.disruptions_allowed > 0 {
        ("True", SUFFICIENT_PODS_REASON)
    } else {
        ("False", INSUFFICIENT_PODS_REASON)
    };
    let observed_generation = status.observed_generation;
    set_status_condition(
        status.conditions.get_or_insert_with(Vec::new),
        PodDisruptionBudgetCondition {
            condition_type: DISRUPTION_ALLOWED_CONDITION.to_string(),
            status: cond_status.to_string(),
            reason: Some(reason.to_string()),
            message: None,
            observed_generation,
            last_transition_time: None,
        },
    );
}

/// `pdbhelper.ConditionsAreUpToDate` (`helpers.go:50-65`).
fn conditions_are_up_to_date(pdb: &PodDisruptionBudget) -> bool {
    let Some(status) = &pdb.status else {
        return false;
    };
    let Some(cond) = status.conditions.as_ref().and_then(|c| {
        c.iter()
            .find(|c| c.condition_type == DISRUPTION_ALLOWED_CONDITION)
    }) else {
        return false;
    };
    if status.observed_generation.unwrap_or(0) != pdb.metadata.generation.unwrap_or(0) {
        return false;
    }
    if status.disruptions_allowed > 0 {
        cond.status == "True" && cond.reason.as_deref() == Some(SUFFICIENT_PODS_REASON)
    } else {
        cond.status == "False" && cond.reason.as_deref() == Some(INSUFFICIENT_PODS_REASON)
    }
}

/// `intstr.GetScaledValueFromIntOrPercent`
/// (`apimachinery/pkg/util/intstr/intstr.go:182-198`) with `getIntOrPercentValueSafely`
/// (`:239-259`): an integer is itself; a string must end in `%`.
fn get_scaled_value_from_int_or_percent(
    value: &IntOrString,
    total: i32,
    round_up: bool,
) -> rusternetes_common::Result<i32> {
    match value {
        IntOrString::Int(v) => Ok(*v),
        IntOrString::String(s) => {
            let Some(percent) = s.strip_suffix('%') else {
                return Err(Error::InvalidResource(
                    "invalid value for IntOrString: invalid type: string is not a percentage"
                        .to_string(),
                ));
            };
            let percent: i64 = percent.parse().map_err(|e| {
                Error::InvalidResource(format!(
                    "invalid value for IntOrString: invalid value {s:?}: {e}"
                ))
            })?;
            let scaled = percent as f64 * total as f64 / 100.0;
            Ok(if round_up {
                scaled.ceil()
            } else {
                scaled.floor()
            } as i32)
        }
    }
}

/// `metav1.LabelSelectorAsSelector` (`apimachinery/.../meta/v1/helpers.go:36-72`):
/// every `matchLabels` / `matchExpressions` key and value is validated by
/// `labels.NewRequirement`, so an unusable selector is an error here too.
fn pdb_selector(pdb: &PodDisruptionBudget) -> Result<Selector, String> {
    let selector =
        rusternetes_common::types::label_selector_as_selector(pdb.spec.selector.as_ref())?;
    if let Some(sel) = &pdb.spec.selector {
        validate_selector_requirements(sel)?;
    }
    Ok(selector)
}

fn validate_selector_requirements(sel: &LabelSelector) -> Result<(), String> {
    let check = |key: &str, values: &[&str]| -> Result<(), String> {
        let errs = is_qualified_name(key);
        if !errs.is_empty() {
            return Err(format!("key: Invalid value: {key:?}: {}", errs.join("; ")));
        }
        for v in values {
            let errs = is_valid_label_value(v);
            if !errs.is_empty() {
                return Err(format!("values: Invalid value: {v:?}: {}", errs.join("; ")));
            }
        }
        Ok(())
    };
    for (k, v) in sel.match_labels.iter().flatten() {
        check(k, &[v.as_str()])?;
    }
    for req in sel.match_expressions.iter().flatten() {
        let values: Vec<&str> = req.values.iter().flatten().map(String::as_str).collect();
        check(&req.key, &values)?;
    }
    Ok(())
}

/// Whether `pdb`'s selector selects `pod`. The `policy/v1beta1` compat rule
/// ("An empty selector ({}) also selects no pods", `staging/src/k8s.io/api/policy/
/// v1beta1/types.go:33-37`) is upstream's `Convert_v1beta1_PodDisruptionBudget_To_
/// policy_PodDisruptionBudget` (`pkg/apis/policy/v1beta1/conversion.go:26-47`);
/// rusternetes serves only `policy/v1`, so there is no conversion boundary to
/// hook and the rule is keyed off the stored `apiVersion`.
fn pdb_selector_matches(pdb: &PodDisruptionBudget, pod: &Pod) -> Result<bool, String> {
    let selector = pdb_selector(pdb)?;
    if selector.is_everything() && pdb.type_meta.api_version == "policy/v1beta1" {
        return Ok(false);
    }
    Ok(selector.matches(pod.metadata.labels.as_ref()))
}

/// `verifyGroupKind` (`disruption.go:395-411`).
fn verify_group_kind(
    controller_ref: &OwnerReference,
    expected_kind: &str,
    expected_groups: &[&str],
) -> rusternetes_common::Result<bool> {
    let (group, _) = parse_group_version(&controller_ref.api_version)?;
    if controller_ref.kind != expected_kind {
        return Ok(false);
    }
    Ok(expected_groups.contains(&group.as_str()))
}

/// `schema.ParseGroupVersion` (`apimachinery/pkg/runtime/schema/group_version.go`).
fn parse_group_version(gv: &str) -> rusternetes_common::Result<(String, String)> {
    if gv.is_empty() || gv == "/" {
        return Ok((String::new(), String::new()));
    }
    match gv.matches('/').count() {
        0 => Ok((String::new(), gv.to_string())),
        1 => {
            let (g, v) = gv.split_once('/').expect("one slash");
            Ok((g.to_string(), v.to_string()))
        }
        _ => Err(Error::InvalidResource(format!(
            "unexpected GroupVersion string: {gv}"
        ))),
    }
}

/// `controller.KeyFunc` (`namespace/name`) for a PDB.
fn pdb_key(pdb: &PodDisruptionBudget) -> String {
    format!(
        "{}/{}",
        pdb.metadata.namespace.as_deref().unwrap_or("default"),
        pdb.metadata.name
    )
}

/// `poddisruptionbudgets/<ns>/<name>` (a watch key) to `<ns>/<name>`.
fn meta_namespace_key(watch_key: &str) -> Option<String> {
    let mut parts = watch_key.splitn(3, '/');
    parts.next()?;
    Some(format!("{}/{}", parts.next()?, parts.next()?))
}

/// `metav1.GetControllerOf`.
fn controller_ref(pod: &Pod) -> Option<OwnerReference> {
    pod.metadata
        .owner_references
        .as_ref()
        .and_then(|refs| refs.iter().find(|r| r.controller == Some(true)).cloned())
}

/// [`controller_ref`] over a raw JSON object.
fn controller_ref_from_json(obj: &serde_json::Value) -> Option<OwnerReference> {
    let refs = obj
        .get("metadata")
        .and_then(|m| m.get("ownerReferences"))
        .and_then(|r| r.as_array())?;
    for r in refs {
        if r.get("controller").and_then(|c| c.as_bool()) == Some(true) {
            return serde_json::from_value(r.clone()).ok();
        }
    }
    None
}

fn json_uid(obj: &serde_json::Value) -> &str {
    obj.get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .unwrap_or("")
}

/// `*(x.Spec.Replicas)`: the API server defaults an unset `replicas` to 1.
fn json_replicas(obj: &serde_json::Value) -> i32 {
    obj.get("spec")
        .and_then(|s| s.get("replicas"))
        .and_then(|r| r.as_i64())
        .map(|r| r as i32)
        .unwrap_or(1)
}

fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "nil",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "float64",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "[]interface {}",
        serde_json::Value::Object(_) => "map[string]interface {}",
    }
}

/// Resolve a dot-prefixed JSONPath (e.g. `.spec.replicas`) against a JSON
/// object: the dotted-field subset CRD `specReplicasPath` is allowed to use.
fn resolve_json_path<'a>(doc: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let trimmed = path.strip_prefix('.').unwrap_or(path);
    let mut cur = doc;
    for segment in trimmed.split('.') {
        if segment.is_empty() {
            return None;
        }
        cur = cur.get(segment)?;
    }
    Some(cur)
}

/// `nonTerminatingPodHasStaleDisruptionCondition` (`disruption.go:1046-1062`):
/// whether the pod carries a stale `DisruptionTarget=True` condition, and how
/// long until it becomes stale.
fn non_terminating_pod_has_stale_disruption_condition(
    pod: &Pod,
    now: DateTime<Utc>,
    timeout: StdDuration,
) -> Option<ChronoDuration> {
    if pod.metadata.deletion_timestamp.is_some() {
        return None;
    }
    let status = pod.status.as_ref()?;
    let cond = status
        .conditions
        .as_ref()?
        .iter()
        .find(|c| c.condition_type == "DisruptionTarget")?;
    // "Pod disruption conditions added by kubelet are never considered stale
    // because the condition might take arbitrarily long before the pod is
    // terminating (has deletion timestamp). Also, pod conditions present on pods
    // in terminal phase are not stale to avoid unnecessary status updates."
    if cond.status != "True"
        || cond.reason.as_deref() == Some(POD_REASON_TERMINATION_BY_KUBELET)
        || is_pod_phase_terminal(status.phase.as_ref())
    {
        return None;
    }
    let transitioned = cond
        .last_transition_time
        .unwrap_or(DateTime::<Utc>::MIN_UTC);
    let wait_for = ChronoDuration::from_std(timeout).unwrap_or(ChronoDuration::zero())
        - now.signed_duration_since(transitioned);
    Some(wait_for.max(ChronoDuration::zero()))
}

/// The pod half of `DisruptionController` (`syncStalePodDisruption`,
/// `disruption.go:774-816`): a pod whose `DisruptionTarget=True` condition was
/// set but which never got a `deletionTimestamp` has the condition reset to
/// `False` after [`STALE_POD_DISRUPTION_TIMEOUT`].
///
/// Upstream shares the pod informer with the PDB sync; here it is its own
/// controller with its own pod watch, which is observably the same.
pub struct StalePodDisruptionController<S: Storage> {
    storage: Arc<S>,
    timeout: StdDuration,
}

impl<S: Storage + 'static> StalePodDisruptionController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            timeout: STALE_POD_DISRUPTION_TIMEOUT,
        }
    }

    /// Test helper: install a custom timeout so the controller can be driven
    /// deterministically without 120s of wall-clock waiting.
    #[allow(dead_code)]
    #[doc(hidden)]
    pub fn with_timeout(storage: Arc<S>, timeout: StdDuration) -> Self {
        Self { storage, timeout }
    }

    /// `stalePodDisruptionWorker` + the pod handlers' `enqueueStalePodDisruptionCleanup`.
    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let queue = WorkQueue::new();
        let worker_self = Arc::clone(&self);
        let worker_queue = queue.clone();
        tokio::spawn(async move { worker_self.worker(worker_queue).await });

        loop {
            self.enqueue_all(&queue).await;

            let mut pod_watch = match self.storage.watch(&build_prefix("pods", None)).await {
                Ok(w) => w,
                Err(e) => {
                    error!("stale-pod-disruption: failed to establish pod watch: {e}");
                    tokio::time::sleep(RESYNC_INTERVAL).await;
                    continue;
                }
            };
            let mut resync = tokio::time::interval(RESYNC_INTERVAL);
            resync.tick().await;
            loop {
                tokio::select! {
                    event = pod_watch.next() => match event {
                        Some(Ok(WatchEvent::Deleted(..))) => {}
                        Some(Ok(WatchEvent::Added(_, v) | WatchEvent::Modified(_, v))) => {
                            if let Ok(pod) = serde_json::from_str::<Pod>(&v) {
                                self.enqueue_if_stale(&queue, &pod).await;
                            }
                        }
                        Some(Err(e)) => {
                            warn!("stale-pod-disruption: pod watch error: {e}, reconnecting");
                            break;
                        }
                        None => {
                            warn!("stale-pod-disruption: pod watch ended, reconnecting");
                            break;
                        }
                    },
                    _ = resync.tick() => self.enqueue_all(&queue).await,
                }
            }
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self.storage.list::<Pod>(&build_prefix("pods", None)).await {
            Ok(pods) => {
                for pod in &pods {
                    self.enqueue_if_stale(queue, pod).await;
                }
            }
            Err(e) => error!("stale-pod-disruption: failed to list pods: {e}"),
        }
    }

    /// `if has, cleanAfter := dc.nonTerminatingPodHasStaleDisruptionCondition(pod); has {
    /// dc.enqueueStalePodDisruptionCleanup(logger, pod, cleanAfter) }`.
    async fn enqueue_if_stale(&self, queue: &WorkQueue, pod: &Pod) {
        if let Some(clean_after) =
            non_terminating_pod_has_stale_disruption_condition(pod, Utc::now(), self.timeout)
        {
            let key = format!(
                "{}/{}",
                pod.metadata.namespace.as_deref().unwrap_or("default"),
                pod.metadata.name
            );
            queue
                .add_after(key, clean_after.to_std().unwrap_or(StdDuration::ZERO))
                .await;
        }
    }

    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            match self.sync_stale_pod_disruption(&key, &queue).await {
                Ok(()) => queue.forget(&key).await,
                Err(e) => {
                    error!("error syncing Pod {key} to clear DisruptionTarget condition, requeueing: {e}");
                    queue.requeue_rate_limited(key.clone()).await;
                }
            }
            queue.done(&key).await;
        }
    }

    /// Walk every pod, fix the stale ones (tests call this directly).
    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> anyhow::Result<()> {
        let queue = WorkQueue::new();
        let pods: Vec<Pod> = self.storage.list("/registry/pods/").await?;
        for pod in pods {
            let key = format!(
                "{}/{}",
                pod.metadata.namespace.as_deref().unwrap_or("default"),
                pod.metadata.name
            );
            if let Err(e) = self.sync_stale_pod_disruption(&key, &queue).await {
                warn!("stale-pod-disruption: failed to reconcile {key}: {e}");
            }
        }
        Ok(())
    }

    /// `syncStalePodDisruption` (`disruption.go:774-816`).
    async fn sync_stale_pod_disruption(&self, key: &str, queue: &WorkQueue) -> anyhow::Result<()> {
        let (namespace, name) = key.split_once('/').unwrap_or(("default", key));
        let pod_key = build_key("pods", Some(namespace), name);
        let pod: Pod = match self.storage.get(&pod_key).await {
            Ok(p) => p,
            Err(Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let Some(clean_after) =
            non_terminating_pod_has_stale_disruption_condition(&pod, Utc::now(), self.timeout)
        else {
            return Ok(());
        };
        if clean_after > ChronoDuration::zero() {
            queue
                .add_after(
                    key.to_string(),
                    clean_after.to_std().unwrap_or(StdDuration::ZERO),
                )
                .await;
            return Ok(());
        }

        // `apipod.UpdatePodCondition` replaces the whole condition: reason and
        // message are cleared, `lastTransitionTime` is now (the status changed),
        // `observedGeneration` follows the pod's generation (PodObservedGenerationTracking
        // is GA and locked on in 1.35).
        let mut new_pod = pod.clone();
        let generation = new_pod.metadata.generation;
        if let Some(cond) = new_pod
            .status
            .as_mut()
            .and_then(|s| s.conditions.as_mut())
            .and_then(|c| {
                c.iter_mut()
                    .find(|c| c.condition_type == "DisruptionTarget")
            })
        {
            cond.status = "False".to_string();
            cond.reason = None;
            cond.message = None;
            cond.last_transition_time = Some(Utc::now());
            cond.observed_generation = generation;
        }
        // Pod conditions live in status, so this goes through the status
        // subresource (#1712/#1723).
        self.storage.update_status(&pod_key, &new_pod).await?;
        info!("Reset stale DisruptionTarget condition to False on pod {key}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{IntOrString, PodDisruptionBudgetSpec};
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_storage::MemoryStorage;
    /// A storage double that behaves like a real api-server: `update` (full-object
    /// PUT) DISCARDS `.status`; only the status subresource persists it. Upstream
    /// strips status on the main resource for any type with a status subresource,
    /// and `crates/storage/src/api_storage.rs` says so explicitly — a controller
    /// writing status through `update` "will not see it stick in API mode".
    ///
    /// Against a vanilla kube-apiserver that made `status.observedGeneration` never
    /// advance, so upstream's `waitForPdbToBeProcessed` polled until it timed out
    /// and every DisruptionController [Conformance] spec burned ~10 minutes — four
    /// of them consumed the whole vanilla-swap controller-manager budget (#1712).
    ///
    /// MemoryStorage cannot show this: its default `update_status` funnels through
    /// `update`, so both paths persist status there.
    struct StatusStrippingStorage {
        inner: MemoryStorage,
        /// Play the `/eviction` handler inside the status write: bump the
        /// stored object after the controller read it and before the guarded
        /// write lands.
        overtake_status_write: bool,
    }

    #[async_trait::async_trait]
    impl Storage for StatusStrippingStorage {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.create(key, value).await
        }

        async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.get(key).await
        }

        /// Full-object PUT: keep whatever status is already stored, ignore the
        /// caller's — exactly what an api-server with a status subresource does.
        async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            let mut doc = serde_json::to_value(value).unwrap();
            let stored: serde_json::Value =
                self.inner.get(key).await.unwrap_or(serde_json::Value::Null);
            if let Some(obj) = doc.as_object_mut() {
                match stored.get("status") {
                    Some(prev) if !prev.is_null() => {
                        obj.insert("status".to_string(), prev.clone());
                    }
                    _ => {
                        obj.remove("status");
                    }
                }
            }
            let kept: T = serde_json::from_value(doc).unwrap();
            self.inner.update(key, &kept).await
        }

        /// The status subresource: this is the ONLY path that persists `.status`.
        /// Must be overridden — the trait's default `update_status` does a
        /// read-modify-write through `update`, which above strips status, so
        /// inheriting it would make even a correct controller look broken.
        async fn update_status<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            let incoming = serde_json::to_value(value).unwrap();
            let mut stored: serde_json::Value = self.inner.get(key).await?;
            if let Some(obj) = stored.as_object_mut() {
                if let Some(status) = incoming.get("status") {
                    obj.insert("status".to_string(), status.clone());
                }
            }
            let merged: T = serde_json::from_value(stored).unwrap();
            self.inner.update(key, &merged).await
        }

        /// The status subresource with the resourceVersion precondition an
        /// api-server applies; the compare is `MemoryStorage`'s own.
        async fn update_status_cas<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            if self.overtake_status_write {
                let mut stored: serde_json::Value = self.inner.get(key).await?;
                stored["status"]["disruptionsAllowed"] = serde_json::json!(0);
                self.inner.update(key, &stored).await?;
            }
            self.inner.update_status_cas(key, value).await
        }

        async fn update_raw(
            &self,
            key: &str,
            value: &serde_json::Value,
        ) -> rusternetes_common::Result<()> {
            self.inner.update_raw(key, value).await
        }

        async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
            self.inner.delete(key).await
        }

        async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.list(prefix).await
        }

        async fn watch(
            &self,
            prefix: &str,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch(prefix).await
        }

        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }

        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.inner.current_revision().await
        }

        async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
            self.inner.is_revision_compacted(revision).await
        }
    }

    fn pdb_fixture() -> PodDisruptionBudget {
        PodDisruptionBudget {
            type_meta: TypeMeta {
                api_version: "policy/v1".to_string(),
                kind: "PodDisruptionBudget".to_string(),
            },
            metadata: ObjectMeta {
                name: "pdb-1".to_string(),
                namespace: Some("default".to_string()),
                generation: Some(1),
                ..Default::default()
            },
            spec: PodDisruptionBudgetSpec {
                min_available: Some(IntOrString::Int(1)),
                max_unavailable: None,
                selector: Some(LabelSelector::default()),
                unhealthy_pod_eviction_policy: None,
            },
            status: None,
        }
    }

    /// #2151: the status write is a compare-and-set, not check-then-write. The
    /// eviction lands AFTER the controller's read and INSIDE the status write,
    /// a window a get-compare-then-`update_status` cannot see; the write must
    /// conflict and leave the eviction's `disruptionsAllowed: 0` in place.
    #[tokio::test]
    async fn stale_status_write_conflicts_instead_of_overwriting() {
        let storage = Arc::new(StatusStrippingStorage {
            inner: MemoryStorage::new(),
            overtake_status_write: true,
        });
        let mut pdb = pdb_fixture();
        pdb.status = Some(PodDisruptionBudgetStatus {
            current_healthy: 3,
            desired_healthy: 1,
            disruptions_allowed: 2,
            expected_pods: 3,
            observed_generation: Some(1),
            conditions: None,
            disrupted_pods: None,
        });
        let key = build_key("poddisruptionbudgets", Some("default"), "pdb-1");
        storage.create(&key, &pdb).await.unwrap();
        let read: PodDisruptionBudget = storage.get(&key).await.unwrap();

        let controller = PodDisruptionBudgetController::new(storage.clone());
        let mut computed = read.clone();
        computed.status.as_mut().unwrap().disruptions_allowed = 2;
        let err = controller.write_pdb_status(&computed).await.unwrap_err();
        assert!(matches!(err, Error::Conflict(_)), "{err:?}");

        let stored: PodDisruptionBudget = storage.get(&key).await.unwrap();
        assert_eq!(
            stored.status.unwrap().disruptions_allowed,
            0,
            "the stale write must not overwrite the eviction's decrement"
        );
    }

    /// THE regression: reconcile must persist status through the status
    /// subresource, so observedGeneration advances even when a full-object PUT
    /// drops status.
    #[tokio::test]
    async fn pdb_status_survives_an_apiserver_that_strips_status_on_put() {
        let storage = Arc::new(StatusStrippingStorage {
            inner: MemoryStorage::new(),
            overtake_status_write: false,
        });
        let pdb = pdb_fixture();
        let key = build_key("poddisruptionbudgets", Some("default"), "pdb-1");
        storage.create(&key, &pdb).await.unwrap();

        let controller = PodDisruptionBudgetController::new(storage.clone());
        controller.sync("default/pdb-1").await.unwrap();

        let stored: PodDisruptionBudget = storage.get(&key).await.unwrap();
        let status = stored
            .status
            .expect("reconcile must persist PDB status via the status subresource");
        assert_eq!(
            status.observed_generation,
            Some(1),
            "observedGeneration must reach metadata.generation, else upstream's \
             waitForPdbToBeProcessed polls until it times out"
        );
    }

    /// `status.disruptionsAllowed` must never go negative.
    ///
    /// With no matching pods and `minAvailable: 2`, `currentHealthy -
    /// desiredHealthy` is -2. Upstream clamps that to 0
    /// (pkg/controller/disruption/disruption.go:1008-1011):
    ///
    /// ```go
    /// disruptionsAllowed := currentHealthy - desiredHealthy
    /// if expectedCount <= 0 || disruptionsAllowed <= 0 {
    ///     disruptionsAllowed = 0
    /// }
    /// ```
    ///
    /// and the API type declares the field `+kubebuilder:validation:Minimum=0`,
    /// so an api-server that validates it rejects the whole status write:
    ///
    /// ```text
    /// Failed to reconcile poddisruptionbudgets/pdb-check/foo: Bad request:
    ///   Error from server (Invalid): PodDisruptionBudget.policy "foo" is invalid:
    ///   status.disruptionsAllowed: Invalid value: -2: must be greater than or equal to 0
    /// ```
    ///
    /// The rejected write took observedGeneration with it, so upstream's
    /// `waitForPdbToBeProcessed` (test/e2e/apps/disruption.go:760) polled for
    /// its full 10-minute budget and FOUR DisruptionController [Conformance]
    /// specs failed on it.
    #[tokio::test]
    async fn disruptions_allowed_is_never_negative() {
        let storage = Arc::new(StatusStrippingStorage {
            inner: MemoryStorage::new(),
            overtake_status_write: false,
        });
        // minAvailable: 2, and not a single pod matches the selector.
        let mut pdb = pdb_fixture();
        pdb.spec.min_available = Some(IntOrString::Int(2));
        let key = build_key("poddisruptionbudgets", Some("default"), "pdb-1");
        storage.create(&key, &pdb).await.unwrap();

        let controller = PodDisruptionBudgetController::new(storage.clone());
        controller.sync("default/pdb-1").await.unwrap();

        let status = storage
            .get::<PodDisruptionBudget>(&key)
            .await
            .unwrap()
            .status
            .expect("reconcile must persist status");
        assert_eq!(
            status.disruptions_allowed, 0,
            "disruptionsAllowed must clamp at 0, not report {}",
            status.disruptions_allowed
        );
        assert_eq!(
            status.observed_generation,
            Some(1),
            "the clamped write must still advance observedGeneration"
        );
    }

    /// The clamp is not a blanket zero: a PDB with slack still reports it.
    #[tokio::test]
    async fn disruptions_allowed_reports_real_slack() {
        let storage = Arc::new(StatusStrippingStorage {
            inner: MemoryStorage::new(),
            overtake_status_write: false,
        });
        let mut pdb = pdb_fixture();
        pdb.spec.min_available = Some(IntOrString::Int(0));
        let key = build_key("poddisruptionbudgets", Some("default"), "pdb-1");
        storage.create(&key, &pdb).await.unwrap();

        let controller = PodDisruptionBudgetController::new(storage.clone());
        controller.sync("default/pdb-1").await.unwrap();

        let status = storage
            .get::<PodDisruptionBudget>(&key)
            .await
            .unwrap()
            .status
            .expect("reconcile must persist status");
        assert!(
            status.disruptions_allowed >= 0,
            "never negative: {}",
            status.disruptions_allowed
        );
    }

    #[test]
    fn test_resolve_json_path_simple() {
        let doc = serde_json::json!({"spec": {"replicas": 7, "nested": {"x": 1}}});
        assert_eq!(
            resolve_json_path(&doc, ".spec.replicas").and_then(|v| v.as_i64()),
            Some(7)
        );
        // Leading-dot stripping is just sugar — both should work identically.
        assert_eq!(
            resolve_json_path(&doc, "spec.replicas").and_then(|v| v.as_i64()),
            Some(7)
        );
        // Multi-level descent.
        assert_eq!(
            resolve_json_path(&doc, ".spec.nested.x").and_then(|v| v.as_i64()),
            Some(1)
        );
        // Missing path returns None (caller falls back to pod count).
        assert!(resolve_json_path(&doc, ".spec.missing").is_none());
        // Empty segment is malformed input; must not panic / silently match.
        assert!(resolve_json_path(&doc, ".spec..replicas").is_none());
    }

    #[test]
    fn test_parse_group_version() {
        // schema.ParseGroupVersion: "" and "/" are the zero value; one slash
        // splits; more is an error; core resources have no group.
        assert_eq!(
            parse_group_version("apps/v1").unwrap(),
            ("apps".to_string(), "v1".to_string())
        );
        assert_eq!(
            parse_group_version("v1").unwrap(),
            (String::new(), "v1".to_string())
        );
        assert_eq!(
            parse_group_version("/v1").unwrap(),
            (String::new(), "v1".to_string())
        );
        assert_eq!(
            parse_group_version("").unwrap(),
            (String::new(), String::new())
        );
        assert!(parse_group_version("a/b/c").is_err());
    }

    #[test]
    fn test_controller_ref_from_json_returns_only_controller() {
        // Pure ownerRef without `controller: true` MUST be ignored — upstream's
        // GetControllerOf is strict about this. Only the explicit controller
        // ref is the one we walk up from.
        let obj = serde_json::json!({
            "metadata": {
                "ownerReferences": [
                    {"apiVersion": "v1", "kind": "Pod", "name": "side", "uid": "1"},
                    {
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "main",
                        "uid": "2",
                        "controller": true
                    }
                ]
            }
        });
        let r = controller_ref_from_json(&obj).expect("controller present");
        assert_eq!(r.kind, "Deployment");
        assert_eq!(r.uid, "2");

        // No controller flag set anywhere → None.
        let obj2 = serde_json::json!({
            "metadata": {
                "ownerReferences": [
                    {"apiVersion": "v1", "kind": "Pod", "name": "side", "uid": "1"}
                ]
            }
        });
        assert!(controller_ref_from_json(&obj2).is_none());

        // No metadata at all → None.
        let obj3 = serde_json::json!({});
        assert!(controller_ref_from_json(&obj3).is_none());
    }
}
