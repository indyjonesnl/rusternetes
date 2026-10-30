use super::expectations::ControllerExpectations;
use super::pod_informer::{ControllerRef, PodEventTarget, ResolvedController, SharedPodInformer};
use super::replicationcontroller::slow_start_batches_capped;
use crate::controllers::worker_pool::spawn_workers;
use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::node::Taint;
use rusternetes_common::resources::pod::Toleration;
use rusternetes_common::resources::policy::IntOrString;
use rusternetes_common::resources::workloads::DEPRECATED_TEMPLATE_GENERATION;
use rusternetes_common::resources::{
    ControllerRevision, DaemonSet, DaemonSetStatus, Node, Pod, PodStatus,
};
use rusternetes_common::types::{label_selector_as_selector, OwnerReference, Phase, Selector};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

/// `extensions.DaemonSetTemplateGenerationKey`
/// (pkg/apis/extensions/types.go).
pub const TEMPLATE_GENERATION_LABEL: &str = "pod-template-generation";
/// `extensions.DefaultDaemonSetUniqueLabelKey` (pkg/apis/extensions/types.go).
pub const UNIQUE_LABEL: &str = "controller-revision-hash";

/// Port of `util.GetTemplateGeneration`
/// (pkg/controller/daemon/util/daemonset_util.go:35-46): reads the deprecated
/// annotation; absent or unparseable yields `None` (callers in
/// daemon_controller.go:1176-1179 and update.go:271-274 treat a parse error
/// as nil and rely on the hash).
pub fn get_template_generation(ds: &DaemonSet) -> Option<i64> {
    ds.metadata
        .annotations
        .as_ref()?
        .get(DEPRECATED_TEMPLATE_GENERATION)?
        .parse::<i64>()
        .ok()
}

/// Port of `util.IsPodUpdated` (daemonset_util.go:165-171): a pod is updated
/// when its template-generation label OR its controller-revision-hash label
/// matches.
pub fn is_pod_updated(pod: &Pod, hash: &str, ds_template_generation: Option<i64>) -> bool {
    let labels = pod.metadata.labels.as_ref();
    let template_matches = ds_template_generation.is_some_and(|g| {
        labels
            .and_then(|l| l.get(TEMPLATE_GENERATION_LABEL))
            .is_some_and(|v| *v == g.to_string())
    });
    let hash_matches = !hash.is_empty()
        && labels
            .and_then(|l| l.get(UNIQUE_LABEL))
            .is_some_and(|v| v == hash);
    hash_matches || template_matches
}

/// Check whether a set of tolerations tolerates all NoSchedule and NoExecute taints on a node.
pub fn pod_tolerates_node_taints(tolerations: &[Toleration], taints: &[Taint]) -> bool {
    for taint in taints {
        // Only NoSchedule and NoExecute taints must be tolerated
        if taint.effect == "NoSchedule" || taint.effect == "NoExecute" {
            let tolerated = tolerations.iter().any(|t| {
                // Empty/missing key with Exists operator matches all taints
                if t.operator.as_deref() == Some("Exists")
                    && (t.key.is_none() || t.key.as_deref() == Some(""))
                {
                    return true;
                }
                // Key must match
                let key_matches = t.key.as_deref() == Some(&taint.key);
                // Effect must match (or be empty/None = match all effects)
                let effect_matches =
                    t.effect.is_none() || t.effect.as_deref() == Some(&taint.effect);
                // Operator: Equal requires value match, Exists only needs key
                let value_matches = match t.operator.as_deref() {
                    Some("Exists") => true,
                    _ => t.value.as_deref() == taint.value.as_deref(),
                };
                key_matches && effect_matches && value_matches
            });
            if !tolerated {
                return false;
            }
        }
    }
    true
}

/// Workers draining this controller's queue: upstream `ConcurrentDaemonSetSyncs` default,
/// pkg/controller/daemon/config/v1alpha1/defaults.go:33-34, launched at cmd/kube-controller-manager/app/apps.go:63.
const CONCURRENT_DAEMONSET_SYNCS: usize = 2;

pub struct DaemonSetController<S: Storage> {
    pub storage: Arc<S>,
    /// Per-DaemonSet ("ns/name") expectations of in-flight pod deletions.
    /// Upstream `dsc.expectations` (`daemon_controller.go`), a
    /// `UIDTrackingControllerExpectations`; shared implementation in
    /// [`super::expectations`]. Both halves are wired (creations and
    /// deletions, set together by `sync_nodes`).
    pub expectations: Arc<ControllerExpectations>,
    /// The shared pod informer whose `addPod`/`updatePod`/`deletePod`
    /// handlers this controller registers (daemon_controller.go:207-218,
    /// `podInformer.Informer().AddEventHandler`).
    pub pod_informer: Arc<SharedPodInformer<S>>,
}

/// `BurstReplicas` (pkg/controller/daemon/daemon_controller.go:61): caps the
/// creates and the deletes one `syncNodes` call issues.
pub const BURST_REPLICAS: usize = 250;
/// `controller.SlowStartInitialBatchSize`
/// (pkg/controller/controller_utils.go:87).
const SLOW_START_INITIAL_BATCH_SIZE: usize = 1;
/// `maxNameGenerationCreateAttempts`
/// (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:440).
const MAX_NAME_GENERATION_CREATE_ATTEMPTS: usize = 8;

/// `<base>-<5 random chars>`, as upstream's `simpleNameGenerator`.
fn generate_pod_name(base: &str) -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";
    let mut rng = rand::rng();
    let suffix: String = (0..5)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect();
    format!("{base}-{suffix}")
}

impl<S: Storage + 'static> DaemonSetController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        let pod_informer = Arc::new(SharedPodInformer::new(
            storage.clone(),
            Duration::from_secs(5),
        ));
        Self::with_pod_informer(storage, pod_informer)
    }

    /// A controller whose pod handlers hang off `pod_informer`, the one
    /// informer the binary shares between controllers.
    pub fn with_pod_informer(storage: Arc<S>, pod_informer: Arc<SharedPodInformer<S>>) -> Self {
        Self {
            storage,
            expectations: Arc::new(ControllerExpectations::new()),
            pod_informer,
        }
    }

    /// Port of `syncNodes` (`pkg/controller/daemon/daemon_controller.go:984-1073`).
    ///
    /// Expectations are recorded BEFORE anything is issued
    /// (`SetExpectations(dsKey, createDiff, deleteDiff)`, :1000), both counts
    /// capped at `burstReplicas` (:987-992). Pods are created in slow-start
    /// batches (:1019-1057): a failed create lowers its own expectation
    /// (`CreationObserved`, :1043) and a failing batch abandons the rest,
    /// lowering by the skipped count (`LowerExpectations`, :1055). Deletes are
    /// graceful (`RealPodControl.DeletePod`, controller_utils.go:618); a
    /// failed one lowers its expectation (`DeletionObserved`, :1068) because
    /// the informer will never see it, NotFound being benign (:1069).
    /// Errors are aggregated; the first is returned.
    async fn sync_nodes(
        &self,
        daemonset: &DaemonSet,
        exp_key: &str,
        namespace: &str,
        pods_to_delete: &[String],
        nodes_needing_pods: &[String],
    ) -> Result<()> {
        if pods_to_delete.is_empty() && nodes_needing_pods.is_empty() {
            return Ok(());
        }
        let create_diff = nodes_needing_pods.len().min(BURST_REPLICAS);
        let delete_diff = pods_to_delete.len().min(BURST_REPLICAS);
        let pod_names = &pods_to_delete[..delete_diff];
        let nodes_needing_pods = &nodes_needing_pods[..create_diff];
        let observed_keys: Vec<String> = pod_names
            .iter()
            .map(|n| format!("{}/{}", namespace, n))
            .collect();
        // Track which pods the deletions wait on, then record both halves in
        // one `SetExpectations`.
        self.expectations
            .expect_deletions_of(exp_key, &observed_keys);
        self.expectations
            .set_expectations(exp_key, create_diff as i64, delete_diff as i64);
        self.expectations.clear_created(exp_key);

        let mut first_err: Option<anyhow::Error> = None;
        let mut attempted = 0usize;
        for batch in
            slow_start_batches_capped(create_diff, SLOW_START_INITIAL_BATCH_SIZE, usize::MAX)
        {
            let nodes = &nodes_needing_pods[attempted..attempted + batch];
            attempted += batch;
            let results = futures::future::join_all(
                nodes
                    .iter()
                    .map(|node| self.create_pod(daemonset, node, namespace)),
            )
            .await;
            let mut batch_failed = false;
            for result in results {
                match result {
                    Ok(pod_name) => {
                        self.expectations
                            .record_created(exp_key, &format!("{}/{}", namespace, pod_name));
                    }
                    Err(e) => {
                        // Rusternetes deviation: upstream returns without
                        // lowering for a NamespaceTerminating cause
                        // (:1034-1038); we lower for every failure so the
                        // record never waits out its TTL. AlreadyExists is
                        // benign (race / re-reconcile), as before.
                        self.expectations.creation_observed(exp_key);
                        let text = e.to_string();
                        // Typed `apierrors.IsAlreadyExists` (errors.go:542-544);
                        // upstream pods use generateName so it never branches
                        // here — a Rusternetes-only benign case (named pods).
                        if super::is_already_exists_anyhow(&e) {
                            debug!("DaemonSet pod already exists, skipping: {}", text);
                            continue;
                        }
                        warn!("Failed to create DaemonSet pod in {}: {}", namespace, e);
                        batch_failed = true;
                        first_err.get_or_insert(e);
                    }
                }
            }
            // Pods we never attempted shouldn't be expected (:1051-1058).
            let skipped = create_diff - attempted;
            if batch_failed && skipped > 0 {
                self.expectations
                    .lower_expectations(exp_key, skipped as i64, 0);
                break;
            }
        }

        for (name, observed) in pod_names.iter().zip(&observed_keys) {
            let pod_key = build_key("pods", Some(namespace), name);
            if let Err(e) = self.storage.delete_gracefully(&pod_key).await {
                self.expectations.deletion_observed_of(exp_key, observed);
                if !matches!(e, rusternetes_common::Error::NotFound(_)) {
                    warn!(
                        "Failed to delete DaemonSet pod {}/{}: {}",
                        namespace, name, e
                    );
                    first_err.get_or_insert(e.into());
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting DaemonSetController (watch-based)");
        let retry_interval = Duration::from_secs(5);

        let queue = WorkQueue::new();

        // Upstream launches `ConcurrentDaemonSetSyncs` workers over one shared queue
        // (`for i := 0; i < workers; i++ { go wait.UntilWithContext(ctx, worker, time.Second) }`);
        // a key in flight is never handed to a second worker.
        spawn_workers(CONCURRENT_DAEMONSET_SYNCS, &queue, |worker_queue| {
            let worker_self = Arc::clone(&self);
            async move {
                worker_self.worker(worker_queue).await;
            }
        });

        // Pod events come from the shared pod informer, as upstream's
        // `podInformer.Informer().AddEventHandler` (daemon_controller.go:207-218).
        self.pod_informer
            .add_event_handler(Arc::clone(&self), queue.clone())
            .await;
        self.pod_informer.start();

        loop {
            // Initial full reconciliation
            self.enqueue_all(&queue).await;

            // Watch daemonsets and nodes; pods arrive via the shared informer.
            let ds_prefix = "/registry/daemonsets/";
            let node_prefix = "/registry/nodes/";

            let ds_watch = match self.storage.watch(ds_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish daemonset watch: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };
            let node_watch = match self.storage.watch(node_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish node watch: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };
            let mut ds_watch = ds_watch;
            let mut node_watch = node_watch;

            // Periodic full resync as safety net
            let mut resync = tokio::time::interval(Duration::from_secs(5));
            resync.tick().await; // consume first immediate tick

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = ds_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                warn!("DaemonSet watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("DaemonSet watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    event = node_watch.next() => {
                        match event {
                            Some(Ok(_ev)) => {
                                // Any node change could affect any DaemonSet
                                self.enqueue_all_for_node_change(&queue).await;
                            }
                            Some(Err(e)) => {
                                warn!("Node watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Node watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
            // Watch broke — loop back to re-establish
        }
    }

    /// Lower the creation expectation for `pod_key`, only if this controller
    /// is still waiting to see that pod (so an event and a listing never
    /// both count it).
    fn observe_creation(&self, exp_key: &str, pod_key: &str) {
        self.expectations.creation_observed_of(exp_key, pod_key);
    }

    /// When a node changes, enqueue ALL daemonsets since any DS might need
    /// to create/delete a pod on the changed node.
    async fn enqueue_all_for_node_change(&self, queue: &WorkQueue) {
        if let Ok(daemonsets) = self
            .storage
            .list::<DaemonSet>("/registry/daemonsets/")
            .await
        {
            for ds in &daemonsets {
                let ns = ds.metadata.namespace.as_deref().unwrap_or("");
                queue
                    .add(format!("daemonsets/{}/{}", ns, ds.metadata.name))
                    .await;
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
            let storage_key = build_key("daemonsets", Some(ns), name);
            match self.storage.get::<DaemonSet>(&storage_key).await {
                Ok(resource) => {
                    let mut resource = resource;
                    match self.reconcile(&mut resource).await {
                        Ok(()) => queue.forget(&key).await,
                        Err(e) => {
                            error!("Failed to reconcile {}: {}", key, e);
                            queue.requeue_rate_limited(key.clone()).await;
                        }
                    }
                }
                Err(_) => {
                    // Resource was deleted — nothing to reconcile. Drop its
                    // expectations so a DaemonSet recreated under the same
                    // name starts clean (`DeleteExpectations`,
                    // daemon_controller.go syncDaemonSet NotFound branch;
                    // upstream TestExpectationsOnRecreate).
                    self.expectations
                        .delete_expectations(&format!("{}/{}", ns, name));
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<DaemonSet>("/registry/daemonsets/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("daemonsets/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list daemonsets for enqueue: {}", e);
            }
        }
    }

    pub async fn reconcile_all(&self) -> Result<()> {
        let daemonsets: Vec<DaemonSet> = self.storage.list("/registry/daemonsets/").await?;

        for mut daemonset in daemonsets {
            if let Err(e) = self.reconcile(&mut daemonset).await {
                error!(
                    "Failed to reconcile DaemonSet {}: {}",
                    daemonset.metadata.name, e
                );
            }
        }

        Ok(())
    }

    pub async fn reconcile(&self, daemonset: &mut DaemonSet) -> Result<()> {
        let name = &daemonset.metadata.name;
        let namespace = daemonset.metadata.namespace.as_ref().unwrap();

        // When DaemonSet is being deleted, actively delete all owned pods
        // instead of waiting for GC (which can be slow and cause cascading delays)
        if daemonset.metadata.is_being_deleted() {
            return self.delete_owned_pods(daemonset).await;
        }

        debug!("Reconciling DaemonSet {}/{}", namespace, name);

        // Ensure a ControllerRevision exists for the current template.
        // K8s uses FNV-32a hash of the template (via controller.ComputeHash).
        // The ControllerRevision data must match getPatch() format exactly:
        //   {"spec":{"template":{...,"$patch":"replace"}}}
        let template_hash = Self::compute_template_hash(&daemonset.spec.template);
        let cr_name = format!("{}-{}", name, template_hash);
        // A parse error is treated as nil (daemon_controller.go:1176-1179).
        let template_generation = get_template_generation(daemonset);

        // Check if ControllerRevision already exists before creating
        let cr_key =
            rusternetes_storage::build_key("controllerrevisions", Some(namespace), &cr_name);
        if self
            .storage
            .get::<ControllerRevision>(&cr_key)
            .await
            .is_err()
        {
            let mut cr_labels = std::collections::HashMap::new();
            cr_labels.insert(
                "controller-revision-hash".to_string(),
                template_hash.clone(),
            );
            cr_labels.insert(
                "controller.kubernetes.io/hash".to_string(),
                template_hash.clone(),
            );

            // Copy DaemonSet's matchLabels to ControllerRevision labels for label selector matching
            if let Some(match_labels) = &daemonset.spec.selector.match_labels {
                for (k, v) in match_labels {
                    cr_labels.insert(k.clone(), v.clone());
                }
            }

            // Count existing revisions to get the next revision number
            let cr_prefix =
                rusternetes_storage::build_prefix("controllerrevisions", Some(namespace));
            let existing_revisions: Vec<ControllerRevision> =
                self.storage.list(&cr_prefix).await.unwrap_or_default();
            let max_revision = existing_revisions
                .iter()
                .filter(|r| {
                    r.metadata
                        .owner_references
                        .as_ref()
                        .map(|refs| {
                            refs.iter().any(|ref_| {
                                ref_.uid == daemonset.metadata.uid
                                    || ref_.name == daemonset.metadata.name
                            })
                        })
                        .unwrap_or(false)
                })
                .map(|r| r.revision)
                .max()
                .unwrap_or(0);
            let mut cr =
                ControllerRevision::new(cr_name.clone(), namespace.clone(), max_revision + 1);
            cr.metadata.labels = Some(cr_labels);
            cr.metadata.ensure_uid();
            cr.metadata.ensure_creation_timestamp();
            cr.metadata.owner_references = Some(vec![OwnerReference {
                api_version: "apps/v1".to_string(),
                kind: "DaemonSet".to_string(),
                name: name.clone(),
                uid: daemonset.metadata.uid.clone(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]);

            // Build ControllerRevision data matching K8s getPatch() format.
            // K8s Match() does byte-level comparison between getPatch(dsFromAPI)
            // and history.Data.Raw. getPatch() marshals the DaemonSet through Go's
            // typed struct serialization (which applies omitempty to drop zero-value
            // fields), extracts spec.template, adds "$patch":"replace", and
            // re-marshals with sorted keys.
            //
            // We use build_patch_data() which serializes through our typed struct
            // (applying skip_serializing_if), then strips remaining Go-omitempty
            // zero values (like empty "name" fields in metadata), and sorts keys.
            let cr_data = Self::build_patch_data(&daemonset.spec.template);
            cr.data = cr_data;

            if self.storage.create(&cr_key, &cr).await.is_ok() {
                info!(
                    "Created ControllerRevision {} for DaemonSet {}/{}",
                    cr_name, namespace, name
                );
            }
        }

        // Get all nodes
        let nodes: Vec<Node> = self.storage.list("/registry/nodes/").await?;

        // Get pod tolerations from the DaemonSet's pod template
        let tolerations = daemonset
            .spec
            .template
            .spec
            .tolerations
            .as_deref()
            .unwrap_or(&[]);

        // Build a candidate pod from the DaemonSet template. This mirrors
        // upstream `NewPod(ds, nodeName)` (pkg/controller/daemon/daemon_controller.go),
        // which constructs a pod with the template's PodSpec so the scheduling
        // predicates (nodeAffinity, podAntiAffinity) can be evaluated against
        // each candidate node before placing a DS pod there.
        let candidate_pod = Self::candidate_pod_from_template(daemonset);

        // For pod (anti-)affinity we need to know what pods already run in the
        // namespace and on which nodes. `check_pod_anti_affinity` resolves a
        // matching pod's node topology against the candidate node, so it needs
        // both the pod list and the full node list.
        let ns_pods: Vec<Pod> = self
            .storage
            .list(&format!("/registry/pods/{}/", namespace))
            .await
            .unwrap_or_default();

        // Filter nodes based on node selector, taint toleration, required
        // nodeAffinity, and required podAntiAffinity.
        let eligible_nodes: Vec<Node> = nodes
            .iter()
            .filter(|node| {
                if !self.matches_node_selector(node, daemonset) {
                    return false;
                }
                // Check if the pod tolerates the node's taints
                let taints = node
                    .spec
                    .as_ref()
                    .and_then(|s| s.taints.as_deref())
                    .unwrap_or(&[]);
                if !pod_tolerates_node_taints(tolerations, taints) {
                    debug!(
                        "DaemonSet {}/{}: skipping node {} due to untolerated taints",
                        namespace, name, node.metadata.name
                    );
                    return false;
                }
                // Honour required nodeAffinity from the template. Upstream's
                // `predicates()` calls
                // `nodeaffinity.GetRequiredNodeAffinity(pod).Match(node)`; we
                // reuse `rusternetes_common::affinity::check_node_affinity`,
                // which evaluates the same required NodeSelectorTerms.
                let (node_affinity_ok, _) =
                    rusternetes_common::affinity::check_node_affinity(node, &candidate_pod);
                if !node_affinity_ok {
                    debug!(
                        "DaemonSet {}/{}: skipping node {} due to nodeAffinity mismatch",
                        namespace, name, node.metadata.name
                    );
                    return false;
                }
                // Honour required podAntiAffinity: skip a node when an existing
                // pod in the same topology domain matches the anti-affinity
                // labelSelector. Reuses
                // `rusternetes_common::affinity::check_pod_anti_affinity`
                // (interpodaffinity predicate parity).
                let (anti_affinity_ok, _) = rusternetes_common::affinity::check_pod_anti_affinity(
                    node,
                    &candidate_pod,
                    &ns_pods,
                    &nodes,
                );
                if !anti_affinity_ok {
                    debug!(
                        "DaemonSet {}/{}: skipping node {} due to podAntiAffinity conflict",
                        namespace, name, node.metadata.name
                    );
                    return false;
                }
                true
            })
            .cloned()
            .collect();

        debug!(
            "DaemonSet {}/{}: {} eligible nodes",
            namespace,
            name,
            eligible_nodes.len()
        );

        // Expectations are read BEFORE listing pods: upstream's
        // `TestRSSyncExpectations` pins the ordering (a pod arriving between
        // list and check would make the record look fulfilled while the list
        // still lacks it). `syncDaemonSet` (daemon_controller.go:1263-1266)
        // only updates status while expectations are unmet.
        let exp_key = format!("{}/{}", namespace, name);
        let mut exp_satisfied = self.expectations.satisfied(&exp_key);
        // Deletion errors don't stop the sync (upstream `syncNodes` aggregates
        // them after creating); they are returned once status is written.
        let mut deferred_err: Option<anyhow::Error> = None;

        // Get current pods for this DaemonSet using owner references
        let pod_prefix = format!("/registry/pods/{}/", namespace);
        let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        // Rusternetes deviation (no upstream equivalent; same intent as the
        // ReplicaSet controller's settle-from-observation step): a pod this
        // DaemonSet is still waiting to see deleted that the fresh listing
        // already shows gone or terminating HAS been deleted, so settle that
        // expectation now rather than blocking on a watch event that may
        // never reach a controller driven by direct `reconcile` calls.
        if !exp_satisfied {
            // Same for creations: a pod this DaemonSet created that the
            // listing already shows has been observed.
            let pending_creates: Vec<String> = self.expectations.pending_creations(&exp_key);
            for pending in pending_creates {
                if all_pods
                    .iter()
                    .any(|p| format!("{}/{}", namespace, p.metadata.name) == pending)
                {
                    self.observe_creation(&exp_key, &pending);
                } else {
                    // Rusternetes deviation: upstream settles a creation from
                    // the pod informer's add event (`addPod` ->
                    // `CreationObserved`, daemon_controller.go:~360), which
                    // fires even if the pod is deleted right after (a Failed
                    // daemon pod being replaced, e2e daemon_set.go "should
                    // retry creating failed daemon pods"). Without a watch,
                    // a pod missing from the listing is either not yet
                    // visible (stale list: keep waiting) or created and
                    // already gone. A direct read tells them apart: NotFound
                    // after a successful create means it was observable.
                    let name = pending.rsplit('/').next().unwrap_or(&pending);
                    let pod_key = build_key("pods", Some(namespace), name);
                    if matches!(
                        self.storage.get::<Pod>(&pod_key).await,
                        Err(rusternetes_common::Error::NotFound(_))
                    ) {
                        self.observe_creation(&exp_key, &pending);
                    }
                }
            }
            for pending in self.expectations.pending_deletions(&exp_key) {
                let still_live = all_pods.iter().any(|p| {
                    p.metadata.deletion_timestamp.is_none()
                        && format!("{}/{}", namespace, p.metadata.name) == pending
                });
                if !still_live {
                    self.expectations.deletion_observed_of(&exp_key, &pending);
                }
            }
            exp_satisfied = self.expectations.satisfied(&exp_key);
        }

        // Find pods owned by this DaemonSet via ownerReferences (authoritative)
        // Fall back to label matching for backwards compatibility with pods created before this fix
        let daemonset_uid = &daemonset.metadata.uid;
        let daemonset_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|pod| {
                let owned_by_ref = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| &r.uid == daemonset_uid))
                    .unwrap_or(false);
                let owned_by_label = pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("controller-uid"))
                    .map(|uid| uid == daemonset_uid)
                    .unwrap_or(false);
                owned_by_ref || owned_by_label
            })
            .collect();

        let mut pods_by_node = std::collections::HashMap::new();
        // Pods this sync removes: failed/succeeded pods and pods on nodes that
        // no longer qualify (upstream `podsToDelete` from `podsShouldBeOnNode`).
        let mut pods_to_delete: Vec<String> = Vec::new();
        for pod in daemonset_pods.iter() {
            if let Some(node_name) = pod.spec.as_ref().and_then(|s| s.node_name.as_ref()) {
                // Check if pod is in a terminal phase (Failed or Succeeded)
                let is_terminal = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.phase.as_ref())
                    .map(|phase| matches!(phase, Phase::Failed | Phase::Succeeded))
                    .unwrap_or(false);

                if is_terminal {
                    // Delete failed/succeeded pods so they can be recreated.
                    // K8s DS controller deletes failed pods and creates replacements
                    // in the same sync cycle (the node won't be in pods_by_node,
                    // so it will be treated as needing a new pod).
                    // K8s ref: pkg/controller/daemon/daemon_controller.go — podsShouldBeOnNode
                    //
                    // A terminal pod that is already terminating is skipped:
                    // getNodesToDaemonPods(includeDeletedTerminal=false)
                    // (daemon_controller.go:747) and podsShouldBeOnNode
                    // (:811) leave it to the kubelet / PodGC.
                    // The delete itself is issued gracefully, in one batch
                    // with the other pods this sync removes (`syncNodes`).
                    if pod.metadata.deletion_timestamp.is_none() {
                        pods_to_delete.push(pod.metadata.name.clone());
                    }
                    // Don't add to pods_by_node — the node needs a new pod.
                    // The replacement will be created in the same reconcile cycle below.
                } else {
                    pods_by_node.insert(node_name.clone(), pod.clone());
                }
            }
        }

        // Determine update strategy
        let update_strategy = daemonset
            .spec
            .update_strategy
            .as_ref()
            .and_then(|s| s.strategy_type.as_deref())
            .unwrap_or("RollingUpdate");

        // Pods on nodes that are no longer eligible are removed
        // (`!shouldContinueRunning && exists`, daemon_controller.go:900-908),
        // skipping pods that are already terminating.
        let eligible_node_names: std::collections::HashSet<_> = eligible_nodes
            .iter()
            .map(|n| n.metadata.name.as_str())
            .collect();
        for (node_name, pod) in pods_by_node.iter() {
            if !eligible_node_names.contains(node_name.as_str())
                && pod.metadata.deletion_timestamp.is_none()
            {
                info!(
                    "Deleting DaemonSet pod {} from ineligible node {}",
                    pod.metadata.name, node_name
                );
                pods_to_delete.push(pod.metadata.name.clone());
            }
        }

        // `syncDaemonSet` (daemon_controller.go:1263-1266): while earlier
        // creations/deletions are unobserved, do nothing but update status.
        if exp_satisfied {
            // Manage phase: one pod per eligible node (only nodes with NO
            // pod) plus everything collected above to delete, in ONE
            // `syncNodes` call (daemon_controller.go:984-1073) so both
            // expectation halves are set together. Runs BEFORE the rolling
            // update phase, matching K8s: manage() then rollingUpdate().
            let nodes_needing_pods: Vec<String> = eligible_nodes
                .iter()
                .map(|n| n.metadata.name.clone())
                .filter(|n| !pods_by_node.contains_key(n))
                .collect();
            if let Err(e) = self
                .sync_nodes(
                    daemonset,
                    &exp_key,
                    namespace,
                    &pods_to_delete,
                    &nodes_needing_pods,
                )
                .await
            {
                deferred_err = Some(e);
            }
        }

        // --- Rolling update phase ---
        // K8s rolling update algorithm (maxSurge=0, the default):
        // 1. Classify each node's pod as "old" (hash != current) or "new" (hash == current)
        // 2. Count already-unavailable pods (new pods not yet available, nodes without pods)
        // 3. Delete old pods only if within the maxUnavailable budget
        // 4. Do NOT create replacement pods here — the next reconcile's manage phase does that
        //
        // This ensures that at any point in time, the number of unavailable pods
        // never exceeds maxUnavailable, which is what the conformance test checks.
        //
        // Gated on expectations RE-CHECKED after manage (`updateDaemonSet`,
        // daemon_controller.go:919-927): manage's own deletions leave them
        // unmet, so no rolling-update deletion piles on top in the same sync.
        if update_strategy == "RollingUpdate" && self.expectations.satisfied(&exp_key) {
            let max_unavailable_raw = daemonset
                .spec
                .update_strategy
                .as_ref()
                .and_then(|s| s.rolling_update.as_ref())
                .and_then(|r| r.max_unavailable.as_ref());
            let desired = eligible_nodes.len() as i32;
            let max_unavailable = resolve_max_unavailable(max_unavailable_raw, desired);

            // Re-read pods after manage phase to get accurate state
            let all_pods_now: Vec<Pod> = self.storage.list(&pod_prefix).await?;
            let daemonset_pods_now: Vec<Pod> = all_pods_now
                .into_iter()
                .filter(|pod| {
                    pod.metadata
                        .owner_references
                        .as_ref()
                        .map(|refs| refs.iter().any(|r| &r.uid == daemonset_uid))
                        .unwrap_or(false)
                })
                .collect();

            let mut current_pods_by_node: std::collections::HashMap<String, Vec<Pod>> =
                std::collections::HashMap::new();
            for pod in daemonset_pods_now.iter() {
                if let Some(node_name) = pod.spec.as_ref().and_then(|s| s.node_name.as_ref()) {
                    let is_terminal = pod
                        .status
                        .as_ref()
                        .and_then(|s| s.phase.as_ref())
                        .map(|phase| matches!(phase, Phase::Failed | Phase::Succeeded))
                        .unwrap_or(false);
                    // `findUpdatedPodsOnNode` skips pods with a
                    // deletionTimestamp (update.go:267-269): a node whose only
                    // pod is terminating has "no pod" and counts as
                    // unavailable (update.go `oldPod == nil && newPod == nil`).
                    if !is_terminal && pod.metadata.deletion_timestamp.is_none() {
                        current_pods_by_node
                            .entry(node_name.clone())
                            .or_default()
                            .push(pod.clone());
                    }
                }
            }

            // Helper: check if a pod is "available" (has Ready condition True)
            let is_pod_available = |pod: &Pod| -> bool {
                pod.status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .map(|conditions| {
                        conditions
                            .iter()
                            .any(|c| c.condition_type == "Ready" && c.status == "True")
                    })
                    .unwrap_or(false)
            };

            // Classify pods on each node and count current unavailability
            let mut num_unavailable: i32 = 0;
            let mut old_available_pods: Vec<(String, Pod)> = Vec::new(); // (node_name, pod)
            let mut old_unavailable_pods: Vec<(String, Pod)> = Vec::new();

            for node in eligible_nodes.iter() {
                let node_name = &node.metadata.name;
                let node_pods = current_pods_by_node
                    .get(node_name.as_str())
                    .cloned()
                    .unwrap_or_default();

                if node_pods.is_empty() {
                    // No pod on this node — counts as unavailable
                    num_unavailable += 1;
                    continue;
                }

                // Find old and new pods on this node
                let mut has_new_available = false;
                let mut has_new_unavailable = false;
                let mut old_pod: Option<Pod> = None;

                for pod in &node_pods {
                    if is_pod_updated(pod, &template_hash, template_generation) {
                        // New pod
                        if is_pod_available(pod) {
                            has_new_available = true;
                        } else {
                            has_new_unavailable = true;
                        }
                    } else {
                        // Old pod
                        old_pod = Some(pod.clone());
                    }
                }

                if has_new_unavailable {
                    // New pod exists but isn't available yet — counts against budget
                    num_unavailable += 1;
                }

                if let Some(old) = old_pod {
                    if has_new_available {
                        // New pod is ready; old pod can be cleaned up (doesn't count as unavailable)
                        // This shouldn't happen in maxSurge=0 mode, but handle it gracefully
                        old_unavailable_pods.push((node_name.clone(), old));
                    } else if !is_pod_available(&old) {
                        // Old pod is unavailable — delete it immediately (free slot)
                        old_unavailable_pods.push((node_name.clone(), old));
                    } else {
                        // Old pod is available — candidate for deletion within budget
                        old_available_pods.push((node_name.clone(), old));
                    }
                }
            }

            // Delete old pods within the maxUnavailable budget.
            // Unavailable old pods are preferred (they're already not serving) but ALL
            // deletions count against the budget. K8s maxSurge=0 means we can never have
            // more than maxUnavailable pods missing at any time.
            let allowed_deletions = (max_unavailable - num_unavailable).max(0);
            // Unavailable old pods first (already not serving), then
            // available ones with the remaining budget; one graceful batch
            // through `syncNodes` (update.go:253), expectations set first.
            let rolling_deletes: Vec<String> = old_unavailable_pods
                .iter()
                .chain(old_available_pods.iter())
                .take(allowed_deletions as usize)
                .map(|(node_name, pod)| {
                    info!(
                        "Rolling update: deleting old pod {} on node {} (hash != {}, budget {})",
                        pod.metadata.name, node_name, template_hash, allowed_deletions
                    );
                    pod.metadata.name.clone()
                })
                .collect();
            if let Err(e) = self
                .sync_nodes(daemonset, &exp_key, namespace, &rolling_deletes, &[])
                .await
            {
                deferred_err.get_or_insert(e);
            }
        }

        // Re-fetch pods after creating/deleting to get accurate count for status
        let all_pods_after: Vec<Pod> = self.storage.list(&pod_prefix).await?;
        let daemonset_pods_after: Vec<Pod> = all_pods_after
            .into_iter()
            .filter(|pod| {
                let owned_by_ref = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| &r.uid == daemonset_uid))
                    .unwrap_or(false);
                let owned_by_label = pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|labels| labels.get("controller-uid"))
                    .map(|uid| uid == daemonset_uid)
                    .unwrap_or(false);
                owned_by_ref || owned_by_label
            })
            .collect();

        let mut final_pods_by_node = std::collections::HashMap::new();
        for pod in daemonset_pods_after.iter() {
            if let Some(node_name) = pod.spec.as_ref().and_then(|s| s.node_name.as_ref()) {
                final_pods_by_node.insert(node_name.clone(), pod.clone());
            }
        }

        // Update status with accurate counts
        let current_number_scheduled = final_pods_by_node.len() as i32;
        let desired_number_scheduled = eligible_nodes.len() as i32;
        let number_ready = final_pods_by_node
            .values()
            .filter(|pod| {
                // K8s numberReady counts pods with Ready condition True, not just Running phase
                pod.status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .map(|conditions| {
                        conditions
                            .iter()
                            .any(|c| c.condition_type == "Ready" && c.status == "True")
                    })
                    .unwrap_or(false)
            })
            .count() as i32;

        // Count pods with the current template hash as "updated"
        // Use final_pods_by_node (re-fetched after create/delete) for accurate count
        let updated_count = final_pods_by_node
            .values()
            .filter(|pod| is_pod_updated(pod, &template_hash, template_generation))
            .count() as i32;

        // Garbage-collect old ControllerRevisions beyond revisionHistoryLimit.
        // Mirrors upstream `cleanupHistory` (pkg/controller/daemon/update.go):
        // delete oldest revisions first, never touching a revision whose hash
        // is still carried by a live pod (nor the current template's revision).
        self.cleanup_history(daemonset, &template_hash, &daemonset_pods_after)
            .await;

        // Preserve existing conditions from current status (merge pattern)
        let existing_conditions = daemonset.status.as_ref().and_then(|s| s.conditions.clone());

        let new_status = Some(DaemonSetStatus {
            desired_number_scheduled,
            current_number_scheduled,
            number_ready,
            number_misscheduled: 0,
            number_available: Some(number_ready),
            number_unavailable: Some(desired_number_scheduled - number_ready),
            updated_number_scheduled: Some(updated_count),
            observed_generation: daemonset.metadata.generation,
            collision_count: None,
            conditions: existing_conditions,
        });

        // Only write status if it actually changed to avoid unnecessary storage writes
        // that trigger watch events and cause feedback loops
        if daemonset.status != new_status {
            daemonset.status = new_status;
            let key = format!("/registry/daemonsets/{}/{}", namespace, name);
            // Status subresource (a full-object PUT strips `.status` through
            // the api-server), conditional on the resourceVersion of the
            // DaemonSet this sync read: upstream `updateDaemonSetStatus`
            // (pkg/controller/daemon/daemon_controller.go) calls
            // `UpdateStatus` on the ds it was handed. A Conflict propagates so
            // the worker requeues rate-limited (#2153).
            self.storage.update_status_cas(&key, daemonset).await?;
        }

        match deferred_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Delete all pods owned by a DaemonSet that is being deleted.
    ///
    /// Issues a DELETE per pod, as upstream's DaemonSet controller does
    /// (pkg/controller/daemon/daemon_controller.go -> podControl.DeletePod).
    /// Writing `deletionTimestamp` directly is rejected by any api-server that
    /// enforces its immutability, leaving the pods running forever.
    async fn delete_owned_pods(&self, daemonset: &DaemonSet) -> Result<()> {
        let ns = daemonset.metadata.namespace.as_deref().unwrap_or("default");
        let ds_name = &daemonset.metadata.name;
        let pod_prefix = rusternetes_storage::build_prefix("pods", Some(ns));
        let pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        for pod in &pods {
            // Check if pod is owned by this DaemonSet
            if let Some(refs) = &pod.metadata.owner_references {
                for owner_ref in refs {
                    if owner_ref.kind == "DaemonSet"
                        && owner_ref.name == *ds_name
                        && pod.metadata.deletion_timestamp.is_none()
                    {
                        let pod_key = build_key("pods", Some(ns), &pod.metadata.name);
                        let _ = self.storage.delete_gracefully(&pod_key).await;
                        info!(
                            "Deleted pod {} (DaemonSet {} being deleted)",
                            pod.metadata.name, ds_name
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Garbage-collect ControllerRevisions owned by this DaemonSet that exceed
    /// `spec.revisionHistoryLimit`. Mirrors upstream `cleanupHistory`
    /// (pkg/controller/daemon/update.go) with one rusternetes refinement: the
    /// limit caps the *total* number of retained revisions (current included),
    /// matching the controller-level contract our tests assert.
    ///
    /// Deletion order is oldest-revision-first. A revision is never deleted if
    /// it is the current template's revision (`current_hash`) or if its hash is
    /// still carried by a live DaemonSet pod — those are needed for OnDelete
    /// pods and in-flight rollbacks.
    ///
    /// The helper is intentionally self-contained (operates on a revision list +
    /// a set of live hashes) so the StatefulSet controller can reuse the same
    /// shape later.
    async fn cleanup_history(&self, daemonset: &DaemonSet, current_hash: &str, live_pods: &[Pod]) {
        // revisionHistoryLimit defaults to 10 in Kubernetes; when unset there is
        // effectively a generous cap, so only GC when the field is present.
        let limit = match daemonset.spec.revision_history_limit {
            Some(l) if l >= 0 => l,
            _ => return,
        };

        let namespace = match daemonset.metadata.namespace.as_deref() {
            Some(ns) => ns,
            None => return,
        };

        let cr_prefix = rusternetes_storage::build_prefix("controllerrevisions", Some(namespace));
        let mut owned: Vec<ControllerRevision> = match self.storage.list(&cr_prefix).await {
            Ok(list) => list
                .into_iter()
                .filter(|r: &ControllerRevision| {
                    r.metadata
                        .owner_references
                        .as_ref()
                        .map(|refs| {
                            refs.iter().any(|owner| {
                                owner.uid == daemonset.metadata.uid
                                    || owner.name == daemonset.metadata.name
                            })
                        })
                        .unwrap_or(false)
                })
                .collect(),
            Err(_) => return,
        };

        // Hashes still carried by live pods must be preserved.
        let live_hashes: std::collections::HashSet<&str> = live_pods
            .iter()
            .filter_map(|p| {
                p.metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("controller-revision-hash"))
                    .map(|s| s.as_str())
            })
            .collect();

        // Protected revisions — the current template's revision and any still
        // carried by a live pod — are never deleted and do NOT count against
        // revisionHistoryLimit. The limit caps only the NON-current history,
        // matching upstream cleanupHistory (keep `limit` old + the live ones).
        let protected = owned
            .iter()
            .filter(|r| {
                let h = r
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("controller-revision-hash"))
                    .map(|s| s.as_str());
                h == Some(current_hash) || h.map(|h| live_hashes.contains(h)).unwrap_or(false)
            })
            .count() as i32;
        let to_kill = (owned.len() as i32 - protected) - limit;
        if to_kill <= 0 {
            return;
        }

        // Oldest revision first.
        owned.sort_by_key(|r| r.revision);

        let mut remaining_to_kill = to_kill;
        for revision in &owned {
            if remaining_to_kill <= 0 {
                break;
            }
            // Never delete the current template's revision.
            let rev_hash = revision
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("controller-revision-hash"))
                .map(|s| s.as_str());
            if rev_hash == Some(current_hash) {
                continue;
            }
            // Never delete a revision whose hash is still in use by a live pod.
            if let Some(h) = rev_hash {
                if live_hashes.contains(h) {
                    continue;
                }
            }
            let cr_key = rusternetes_storage::build_key(
                "controllerrevisions",
                Some(namespace),
                &revision.metadata.name,
            );
            if self.storage.delete(&cr_key).await.is_ok() {
                info!(
                    "Garbage-collected old ControllerRevision {} (revision {}) for DaemonSet {}/{}",
                    revision.metadata.name, revision.revision, namespace, daemonset.metadata.name
                );
                remaining_to_kill -= 1;
            }
        }
    }

    pub fn matches_node_selector(&self, node: &Node, daemonset: &DaemonSet) -> bool {
        // Check if node matches the DaemonSet's node selector
        let node_labels = match &node.metadata.labels {
            Some(labels) => labels,
            None => return daemonset.spec.template.spec.node_selector.is_none(),
        };

        match &daemonset.spec.template.spec.node_selector {
            Some(selector) => {
                // All selector labels must match node labels
                selector.iter().all(|(k, v)| {
                    node_labels
                        .get(k)
                        .map(|node_v| node_v == v)
                        .unwrap_or(false)
                })
            }
            None => true, // No selector means all nodes match
        }
    }

    pub async fn create_pod(
        &self,
        daemonset: &DaemonSet,
        node_name: &str,
        namespace: &str,
    ) -> Result<String> {
        self.create_pod_with_namer(daemonset, node_name, namespace, &mut || {
            generate_pod_name(&daemonset.metadata.name)
        })
        .await
    }

    /// `create_pod` with the generateName generator injected so a collision
    /// is testable. `next_name` is called once per create attempt.
    pub async fn create_pod_with_namer(
        &self,
        daemonset: &DaemonSet,
        node_name: &str,
        namespace: &str,
        next_name: &mut (dyn FnMut() -> String + Send),
    ) -> Result<String> {
        let daemonset_name = &daemonset.metadata.name;
        // Pod naming depends on the update strategy.
        //
        // OnDelete: the controller never auto-replaces a pod, so the pod is a
        // stable per-node identity. We name it deterministically as
        // `<ds-name>-<node-name>` so an operator can target it by name with
        // `kubectl delete pod <ds>-<node>` to trigger a manual roll, matching
        // the upstream OnDelete workflow (pkg/controller/daemon/update.go —
        // OnDelete leaves pod replacement to manual deletion).
        //
        // RollingUpdate (and the default): use a random suffix like upstream's
        // `generateName: <ds-name>-`. A fresh suffix on every create means a
        // rolled pod always gets a NEW name, so the failed-pod's old name
        // returns NotFound and rolling-update replacement is observable.
        let on_delete = daemonset
            .spec
            .update_strategy
            .as_ref()
            .and_then(|s| s.strategy_type.as_deref())
            == Some("OnDelete");
        let mut pod_name = if on_delete {
            format!("{}-{}", daemonset_name, node_name)
        } else {
            next_name()
        };

        // Create pod from template
        let template = &daemonset.spec.template;
        let mut labels = template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        labels.insert("app".to_string(), daemonset_name.clone());
        labels.insert("controller-uid".to_string(), daemonset.metadata.uid.clone());
        // Add controller-revision-hash label (computed from template)
        let template_hash = { Self::compute_template_hash(&daemonset.spec.template) };
        labels.insert("controller-revision-hash".to_string(), template_hash);
        // CreatePodTemplate (daemonset_util.go:104-120): label the pod with the
        // template generation when the DaemonSet carries one.
        if let Some(generation) = get_template_generation(daemonset) {
            labels.insert(
                TEMPLATE_GENERATION_LABEL.to_string(),
                generation.to_string(),
            );
        }

        let mut spec = template.spec.clone();

        // CRITICAL: Assign the pod to the specific node
        spec.node_name = Some(node_name.to_string());

        // Debug: Check if NODE_NAME env var has valueFrom before and after
        debug!("Before injection - Checking environment variables in pod template:");
        for container in &spec.containers {
            if let Some(env) = &container.env {
                for env_var in env {
                    if env_var.name.contains("NODE_NAME")
                        || env_var.name.contains("SONOBUOY_NS")
                        || env_var.name.contains("SONOBUOY_PLUGIN_POD")
                    {
                        debug!(
                            "  Container '{}': {} - value={:?}, value_from.field_ref={:?}",
                            container.name,
                            env_var.name,
                            env_var.value,
                            env_var
                                .value_from
                                .as_ref()
                                .and_then(|vf| vf.field_ref.as_ref())
                        );
                    }
                }
            }
        }

        // Inject service account token volume
        self.inject_service_account_token(&mut spec, namespace)
            .await;

        // Propagate the SA's imagePullSecrets (#1084) — controllers bypass the
        // api-server admission path that normally does this.
        super::propagate_sa_image_pull_secrets(&*self.storage, namespace, &mut spec).await;

        // DefaultTolerationSeconds admission (#442): controllers write pods
        // straight to storage and bypass the api-server admission path that adds
        // these, so apply the same NotReady/Unreachable NoExecute tolerations.
        rusternetes_common::tolerations::add_default_tolerations(&mut spec);

        // Debug: Check again after injection
        debug!("After injection - Checking environment variables:");
        for container in &spec.containers {
            if let Some(env) = &container.env {
                for env_var in env {
                    if env_var.name.contains("NODE_NAME")
                        || env_var.name.contains("SONOBUOY_NS")
                        || env_var.name.contains("SONOBUOY_PLUGIN_POD")
                    {
                        debug!(
                            "  Container '{}': {} - value={:?}, value_from.field_ref={:?}",
                            container.name,
                            env_var.name,
                            env_var.value,
                            env_var
                                .value_from
                                .as_ref()
                                .and_then(|vf| vf.field_ref.as_ref())
                        );
                    }
                }
            }
        }

        let mut metadata = rusternetes_common::types::ObjectMeta::new(pod_name.clone())
            .with_namespace(namespace.to_string())
            .with_labels(labels)
            .with_owner_reference(OwnerReference {
                api_version: "apps/v1".to_string(),
                kind: "DaemonSet".to_string(),
                name: daemonset_name.clone(),
                uid: daemonset.metadata.uid.clone(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            });

        if let Some(template_meta) = &template.metadata {
            if let Some(ref annotations) = template_meta.annotations {
                metadata.annotations = Some(annotations.clone());
            }
        }

        let mut pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata,
            spec: Some(spec),
            status: Some(PodStatus {
                phase: Some(Phase::Pending),
                message: None,
                reason: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                host_ip: None,
                host_i_ps: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
        };

        // generateName retry: `createWithGenerateNameRetry` redraws the name on
        // AlreadyExists, up to `maxNameGenerationCreateAttempts`
        // (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:440,
        // :460-470). Only for generated names; OnDelete pods have a fixed name.
        let attempts = if on_delete {
            1
        } else {
            MAX_NAME_GENERATION_CREATE_ATTEMPTS
        };
        let mut attempt = 1;
        loop {
            let key = format!("/registry/pods/{}/{}", namespace, pod_name);
            match self.storage.create(&key, &pod).await {
                Ok(_) => return Ok(pod_name),
                Err(rusternetes_common::Error::AlreadyExists(_)) if attempt < attempts => {
                    attempt += 1;
                    pod_name = next_name();
                    pod.metadata.name = pod_name.clone();
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Inject the projected `kube-api-access` volume, as upstream's ServiceAccount
    /// admission plugin does (plugin/pkg/admission/serviceaccount/admission.go).
    /// Controllers write pods straight to storage and bypass that plugin, so the
    /// automount decision (pod setting wins over the ServiceAccount's, default
    /// true) is replicated here exactly like the ReplicaSet controller does.
    /// There is no `<sa>-token` Secret to reference: upstream does not mint one
    /// (LegacyServiceAccountTokenNoAutoGeneration).
    async fn inject_service_account_token(
        &self,
        spec: &mut rusternetes_common::resources::PodSpec,
        namespace: &str,
    ) {
        let sa_name = rusternetes_common::serviceaccount::ensure_service_account_name(spec);
        let sa_key = format!("/registry/serviceaccounts/{}/{}", namespace, sa_name);
        let sa_automount = self
            .storage
            .get::<rusternetes_common::resources::ServiceAccount>(&sa_key)
            .await
            .ok()
            .and_then(|sa| sa.automount_service_account_token);
        let should_mount = match spec.automount_service_account_token {
            Some(v) => v,
            None => sa_automount.unwrap_or(true),
        };
        if should_mount {
            rusternetes_common::serviceaccount::add_kube_api_access_volume(spec);
        }
    }

    /// Build a candidate `Pod` from the DaemonSet template for scheduling
    /// predicate evaluation (nodeAffinity / podAntiAffinity). Mirrors upstream
    /// `NewPod(ds, nodeName)` in pkg/controller/daemon/daemon_controller.go,
    /// which copies the template's PodSpec and ObjectMeta into a fresh pod.
    ///
    /// The `nodeName` is intentionally left unset: predicates evaluate the
    /// candidate pod against each node in turn, so it must not be pre-bound.
    pub fn candidate_pod_from_template(daemonset: &DaemonSet) -> Pod {
        let template = &daemonset.spec.template;
        let metadata = template
            .metadata
            .clone()
            .map(|mut m| {
                m.namespace = daemonset.metadata.namespace.clone();
                m
            })
            .unwrap_or_default();
        Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata,
            spec: Some(template.spec.clone()),
            status: None,
        }
    }

    /// Compute template hash matching K8s's controller.ComputeHash.
    /// K8s uses FNV-32a hash of DeepHashObject of the template.
    /// We approximate this with FNV-32a of the JSON-serialized template.
    fn compute_template_hash(template: &rusternetes_common::resources::PodTemplateSpec) -> String {
        use std::hash::Hasher;
        let value = serde_json::to_value(template).unwrap_or_default();
        let serialized = serde_json::to_string(&value).unwrap_or_default();
        let mut hasher = fnv::FnvHasher::with_key(0x811c9dc5);
        hasher.write(serialized.as_bytes());
        let hash = hasher.finish() as u32;
        // K8s uses rand.SafeEncodeString(fmt.Sprint(hash)) which maps each
        // character of the decimal string through a safe alphabet.
        // See: staging/src/k8s.io/apimachinery/pkg/util/rand/rand.go
        let decimal = format!("{}", hash);
        const ALPHANUMS: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";
        decimal
            .bytes()
            .map(|b| ALPHANUMS[(b as usize) % ALPHANUMS.len()] as char)
            .collect()
    }

    /// Build ControllerRevision data in K8s getPatch() format.
    /// Format: {"spec":{"template":{...,"$patch":"replace"}}}
    ///
    /// K8s Match() does byte-level comparison of getPatch() output with
    /// history.Data.Raw. Go's encoding/json sorts map keys alphabetically
    /// and omits zero-value fields with `omitempty`. We must:
    /// 1. Sort keys the same way
    /// 2. Strip zero-value fields (empty strings, false bools, 0 ints, empty
    ///    arrays/maps, nulls) that Go's omitempty would drop
    pub fn build_patch_data(
        template: &rusternetes_common::resources::PodTemplateSpec,
    ) -> Option<serde_json::Value> {
        let mut template_value = serde_json::to_value(template).ok()?;
        // Strip Go-omitempty zero values BEFORE adding $patch marker.
        // Go's getPatch() round-trips the DaemonSet through Go's typed struct
        // serialization which applies omitempty, dropping empty strings, null
        // pointers, empty slices/maps, zero ints, and false booleans.
        template_value = Self::strip_go_omitempty_zeros(&template_value, "");
        // Add $patch: "replace" to the template object (K8s strategic merge patch marker)
        if let Some(obj) = template_value.as_object_mut() {
            obj.insert("$patch".to_string(), serde_json::json!("replace"));
        }
        // Sort keys alphabetically to match Go's encoding/json behavior.
        // K8s Match() compares bytes, so key order must be identical.
        let sorted = Self::sort_json_keys(&serde_json::json!({
            "spec": {
                "template": template_value
            }
        }));
        Some(sorted)
    }

    /// Recursively sort all JSON object keys alphabetically.
    /// Go's encoding/json sorts map keys; we must match this for
    /// byte-level comparisons in DaemonSet ControllerRevision Match().
    fn sort_json_keys(value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let mut sorted = serde_json::Map::new();
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                for key in keys {
                    sorted.insert(key.clone(), Self::sort_json_keys(&map[key]));
                }
                serde_json::Value::Object(sorted)
            }
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| Self::sort_json_keys(v)).collect())
            }
            other => other.clone(),
        }
    }

    /// Strip zero-value fields that Go's `omitempty` json tag would omit.
    /// Go's encoding/json omits:
    ///   - false booleans
    ///   - 0 integers/floats
    ///   - empty strings ""
    ///   - nil pointers (null)
    ///   - empty arrays []
    ///   - empty maps {}
    ///
    /// This is critical for byte-level comparison with Go's getPatch() output.
    /// When Go round-trips a DaemonSet through its typed struct serialization,
    /// all fields with `json:",omitempty"` that have zero values are dropped.
    /// Our ControllerRevision data must match those exact bytes.
    fn strip_go_omitempty_zeros(value: &serde_json::Value, parent_key: &str) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let mut cleaned = serde_json::Map::new();
                for (key, val) in map {
                    // Recursively clean nested values first
                    let cleaned_val = Self::strip_go_omitempty_zeros(val, key);
                    // Skip zero-value fields (Go's omitempty behavior).
                    // Exception: Go keeps empty structs that come from non-nil
                    // pointers (e.g., securityContext: {} from &SecurityContext{}).
                    // These are NOT dropped by omitempty because the pointer is
                    // non-nil even though the struct is empty.
                    // K8s ref: api/core/v1/types.go — SecurityContext is *SecurityContext
                    let is_preserved_empty_struct = cleaned_val.is_object()
                        && cleaned_val.as_object().unwrap().is_empty()
                        && matches!(
                            key.as_str(),
                            "securityContext" | "resources" | "capabilities"
                        );
                    if !is_preserved_empty_struct
                        && Self::is_go_zero_value(&cleaned_val)
                        && Self::go_drops_zero(key, parent_key, &cleaned_val)
                    {
                        continue;
                    }
                    cleaned.insert(key.clone(), cleaned_val);
                }
                serde_json::Value::Object(cleaned)
            }
            serde_json::Value::Array(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|v| Self::strip_go_omitempty_zeros(v, parent_key))
                    .collect(),
            ),
            other => other.clone(),
        }
    }

    /// Whether Go's encoding/json would actually omit this zero value.
    ///
    /// `omitempty` drops a zero scalar only when the Go field is a plain
    /// `bool`/`int*`; a `*bool`/`*int*` is omitted only when nil, so an
    /// explicit 0/false is kept (`TerminationGracePeriodSeconds *int64`
    /// types.go:2749, `RunAsUser *int64` :4826, `Privileged *bool` :8197).
    /// Rusternetes serializes `None` as an absent key, so any zero scalar
    /// that is present is an explicit pointer value unless its key is one of
    /// the plain `omitempty` scalars below. Strings/arrays/maps/objects keep
    /// the previous behaviour (out of scope for #2954).
    ///
    /// The plain-omitempty keys reachable from a PodTemplateSpec are taken
    /// from the same upstream `core/v1/types.go` (release-1.35) as
    /// `PLAIN_ZERO_SCALARS` in crates/protobuf/src/plain_zero_scalars.rs
    /// (that table is keyed by message name, which the schema-less JSON
    /// walk here does not have; the controller-manager crate also does not
    /// depend on it): PodSpec hostNetwork/hostPID/hostIPC (:4209-4219),
    /// Container stdin/stdinOnce/tty (:3092-3105), ContainerPort hostPort
    /// (:2355), Probe initialDelaySeconds/timeoutSeconds/periodSeconds/
    /// successThreshold/failureThreshold (:2720-2737), VolumeMount readOnly
    /// (:2376), and the volume-source scalars partition, chapAuthDiscovery,
    /// chapAuthSession, sslEnabled, readOnly (:236-1858). Exceptions where
    /// `readOnly` is a `*bool`: azureDisk (:1711) and csi (:2275).
    fn go_drops_zero(key: &str, parent_key: &str, value: &serde_json::Value) -> bool {
        if !matches!(
            value,
            serde_json::Value::Bool(_) | serde_json::Value::Number(_)
        ) {
            return true;
        }
        match key {
            "readOnly" => !matches!(parent_key, "azureDisk" | "csi"),
            "hostNetwork"
            | "hostPID"
            | "hostIPC"
            | "stdin"
            | "stdinOnce"
            | "tty"
            | "hostPort"
            | "initialDelaySeconds"
            | "timeoutSeconds"
            | "periodSeconds"
            | "successThreshold"
            | "failureThreshold"
            | "partition"
            | "chapAuthDiscovery"
            | "chapAuthSession"
            | "sslEnabled" => true,
            _ => false,
        }
    }

    /// Check if a JSON value is a Go zero value (would be omitted by omitempty).
    fn is_go_zero_value(value: &serde_json::Value) -> bool {
        match value {
            serde_json::Value::Null => true,
            serde_json::Value::Bool(b) => !b,
            serde_json::Value::Number(n) => n.as_f64().map(|f| f == 0.0).unwrap_or(false),
            serde_json::Value::String(s) => s.is_empty(),
            serde_json::Value::Array(arr) => arr.is_empty(),
            serde_json::Value::Object(map) => map.is_empty(),
        }
    }
}

/// The DaemonSet half of the pod event handlers (daemon_controller.go
/// `addPod` :530, `updatePod` :573, `deletePod` :634, `resolveControllerRef`
/// :769, `getDaemonSetsForPod` :385 over client-go
/// `listers/apps/v1/daemonset_expansion.go` `GetPodDaemonSets` :42).
impl<S: Storage + 'static> PodEventTarget for DaemonSetController<S> {
    fn queue_prefix(&self) -> &'static str {
        "daemonsets"
    }

    /// "We can't look up by UID, so look up by Name and then verify UID.
    /// Don't even try to look up by Name if it's the wrong Kind."
    async fn resolve_controller_ref(
        &self,
        namespace: &str,
        controller_ref: &ControllerRef,
    ) -> Option<ResolvedController> {
        if controller_ref.kind != "DaemonSet" {
            return None;
        }
        let ds: DaemonSet = self
            .storage
            .get(&build_key(
                "daemonsets",
                Some(namespace),
                &controller_ref.name,
            ))
            .await
            .ok()?;
        (ds.metadata.uid == controller_ref.uid).then(|| ResolvedController {
            name: ds.metadata.name.clone(),
            min_ready_seconds: ds.spec.min_ready_seconds.unwrap_or(0),
        })
    }

    /// `GetPodDaemonSets`: a pod without labels matches none; an invalid
    /// selector does not match; an empty selector matches nothing.
    async fn pod_controllers(
        &self,
        namespace: &str,
        pod_labels: &HashMap<String, String>,
    ) -> Vec<String> {
        if pod_labels.is_empty() {
            return Vec::new();
        }
        let Ok(sets) = self
            .storage
            .list::<DaemonSet>(&build_prefix("daemonsets", Some(namespace)))
            .await
        else {
            return Vec::new();
        };
        sets.into_iter()
            .filter(|ds| {
                matches!(
                    label_selector_as_selector(Some(&ds.spec.selector)),
                    Ok(selector @ Selector::Requirements(_)) if selector.matches(Some(pod_labels))
                )
            })
            .map(|ds| ds.metadata.name)
            .collect()
    }

    /// `dsc.expectations.CreationObserved(dsKey)` (:551) for the plain count.
    fn creation_observed(&self, key: &str) {
        self.expectations.creation_observed(key);
    }

    /// The DaemonSet controller's creation expectations are tracked per pod
    /// (`record_created` in `sync_nodes`), so an event and a listing never
    /// both count the same pod.
    fn creation_observed_of(&self, key: &str, pod_key: &str) {
        self.observe_creation(key, pod_key);
    }

    /// `dsc.expectations.DeletionObserved(dsKey)` (:668), UID-tracked per pod
    /// so the later DELETED event of an already-observed pod is a no-op.
    fn deletion_observed(&self, key: &str, pod_key: &str) {
        self.expectations.deletion_observed_of(key, pod_key);
    }

    /// "Add a second to avoid milliseconds skew in AddAfter"
    /// (`updatePod`, :573-632).
    fn min_ready_requeue_delay(&self, min_ready_seconds: i32) -> Duration {
        Duration::from_secs(min_ready_seconds as u64 + 1)
    }
}

/// Resolve a DaemonSet `maxUnavailable` IntOrString against the desired pod
/// count (number of eligible nodes). Percentages are rounded UP per K8s
/// semantics (`intstr.GetScaledValueFromIntOrPercent` with `roundUp=true`).
/// Absolute values pass through, and any unparseable input defaults to 1.
/// The result is clamped to at least 1 so the rolling update can always make
/// progress on small clusters.
/// Mirrors upstream `daemon/util.UnavailableCount`
/// (`pkg/controller/daemon/util/daemonset_util.go`), which delegates to
/// `intstr.GetScaledValueFromIntOrPercent(..., roundUp = true)`.
///
/// The dispatch is on the `IntOrString` **variant**, not on the text of a
/// stringified value: upstream decides "percentage vs absolute" from
/// `Type`, and a value that arrives as an integer can never be read as a
/// percentage. Flattening both variants to a `&str` first is what let the
/// int/string distinction be lost on the wire in the first place.
pub fn resolve_max_unavailable(raw: Option<&IntOrString>, desired: i32) -> i32 {
    match raw {
        Some(IntOrString::String(s)) if s.ends_with('%') => {
            let pct = s.trim_end_matches('%').parse::<f64>().unwrap_or(0.0);
            let scaled = (pct * desired as f64 / 100.0).ceil() as i32;
            scaled.max(1)
        }
        // A non-percent string is not valid upstream, but our validator still
        // tolerates a bare numeric string; keep reading it as an absolute.
        Some(IntOrString::String(s)) => s.parse::<i32>().unwrap_or(1).max(1),
        Some(IntOrString::Int(i)) => (*i).max(1),
        None => 1,
    }
}
