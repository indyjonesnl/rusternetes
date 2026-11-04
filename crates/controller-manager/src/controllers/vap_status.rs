//! ValidatingAdmissionPolicy status controller: reconciles
//! `status.observedGeneration` and `status.typeChecking` of every policy.
//!
//! Ported from `pkg/controller/validatingadmissionpolicystatus/controller.go`
//! (release-1.35) and launched like
//! `newValidatingAdmissionPolicyStatusController`
//! (`cmd/kube-controller-manager/app/validatingadmissionpolicystatus.go:41-77`):
//! no feature gate and not disabled by default, `ConcurrentPolicySyncs`
//! workers (default 5, `config/v1alpha1/defaults.go:33-34`), one rate-limited
//! queue keyed by the policy name (`DefaultTypedControllerRateLimiter`).
//!
//! * Informer handlers `AddFunc`/`UpdateFunc` -> `enqueuePolicy` (the name; the
//!   resource is cluster-scoped). Deletes are not enqueued.
//! * `processNextWorkItem`: a missing policy is "being deleted, do nothing";
//!   success -> `Forget`; error -> `AddRateLimited`.
//! * `reconcile`: skipped when `generation <= status.observedGeneration`;
//!   otherwise type-checks the policy (`TypeChecker.Check`, ported in
//!   `rusternetes_discovery::vap_typechecking::check`) and applies
//!   `status.observedGeneration = generation` and
//!   `status.typeChecking.expressionWarnings`.
//!
//! Deviations: the informer is a storage watch plus a periodic full list (same
//! seam as the sibling controllers) and there is no cache-sync wait. The
//! server-side-apply of the two status fields (`FieldManager:
//! validatingadmissionpolicy-status`, `Force: true`) is a read-modify-write of
//! exactly those two fields through `Storage::update_status` (a
//! compare-and-swap that grafts only `.status`), leaving `status.conditions`
//! untouched, as apply does for fields the manager does not own. The apply
//! also records managedFields for the manager, which storage-level writes do
//! not. The schema resolver is the embedded upstream definitions only; upstream
//! combines it with a `ClientDiscoveryResolver` for custom resources, which
//! are skipped here.

use anyhow::{anyhow, Result};
use futures::StreamExt;
use rusternetes_common::cel_versions::{self, EnvSet};
use rusternetes_common::resources::{
    TypeChecking, ValidatingAdmissionPolicy, ValidatingAdmissionPolicyStatus,
};
use rusternetes_controller_core::worker_pool::spawn_workers;
use rusternetes_discovery::vap_typechecking::check;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

/// `ControllerName` (controller.go:39). Has "Status" in it to differentiate
/// this controller from the one that runs in the API server.
pub const CONTROLLER_NAME: &str = "validatingadmissionpolicy-status";

/// `ConcurrentPolicySyncs` default (`config/v1alpha1/defaults.go:33-34`).
pub const CONCURRENT_POLICY_SYNCS: usize = 5;

const PLURAL: &str = "validatingadmissionpolicies";

/// `Controller` (controller.go:43-53).
pub struct VapStatusController<S: Storage> {
    storage: Arc<S>,
    /// `typeChecker.Check` runs in `EnvSet` of the default compatibility
    /// version (`environment.MustBaseEnvSet(DefaultCompatibilityVersion)`).
    env_set: EnvSet,
}

impl<S: Storage + 'static> VapStatusController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            env_set: EnvSet::must_base_env_set(cel_versions::default_compatibility_version()),
        }
    }

    /// `Run` (controller.go:55-74): informer handlers feed the queue, workers
    /// drain it.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting {CONTROLLER_NAME} controller");
        let queue = WorkQueue::new();
        let me = Arc::clone(&self);
        spawn_workers(CONCURRENT_POLICY_SYNCS, &queue, move |q| {
            let me = Arc::clone(&me);
            async move { me.run_worker(q).await }
        });

        loop {
            self.enqueue_all(&queue).await;
            let mut watch = match self.storage.watch(&build_prefix(PLURAL, None)).await {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch on {PLURAL}: {e}, retrying");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let mut resync = tokio::time::interval(Duration::from_secs(30));
            resync.tick().await;
            loop {
                tokio::select! {
                    event = watch.next() => match event {
                        Some(Ok(ev)) => {
                            // `AddFunc` / `UpdateFunc` only (controller.go:86-93).
                            if !matches!(ev, rusternetes_storage::WatchEvent::Deleted(..)) {
                                queue.add(extract_key(&ev)).await;
                            }
                        }
                        Some(Err(e)) => {
                            warn!("{CONTROLLER_NAME} watch error: {e}, reconnecting");
                            break;
                        }
                        None => {
                            warn!("{CONTROLLER_NAME} watch stream ended, reconnecting");
                            break;
                        }
                    },
                    _ = resync.tick() => self.enqueue_all(&queue).await,
                }
            }
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<ValidatingAdmissionPolicy>(&build_prefix(PLURAL, None))
            .await
        {
            Ok(policies) => {
                for p in &policies {
                    self.enqueue_policy(queue, p).await;
                }
            }
            Err(e) => error!("Failed to list {PLURAL}: {e}"),
        }
    }

    /// `enqueuePolicy` (controller.go:101-110): the key is the bare name.
    async fn enqueue_policy(&self, queue: &WorkQueue, policy: &ValidatingAdmissionPolicy) {
        if policy.metadata.name.is_empty() {
            error!("cannot get name of object {policy:?}");
            return;
        }
        queue
            .add(format!("{PLURAL}/{}", policy.metadata.name))
            .await;
    }

    /// `runWorker` (controller.go:112-115).
    async fn run_worker(&self, queue: WorkQueue) {
        while self.process_next_work_item(&queue).await {}
    }

    /// `processNextWorkItem` (controller.go:117-145).
    pub async fn process_next_work_item(&self, queue: &WorkQueue) -> bool {
        let Some(key) = queue.get().await else {
            return false;
        };
        match self.sync(&key).await {
            Ok(()) => queue.forget(&key).await,
            Err(e) => {
                error!("{CONTROLLER_NAME}: {e}");
                queue.requeue_rate_limited(key.clone()).await;
            }
        }
        queue.done(&key).await;
        true
    }

    /// The closure in `processNextWorkItem` (controller.go:124-134): lister
    /// `Get`, NotFound means the policy is being deleted.
    pub async fn sync(&self, key: &str) -> Result<()> {
        let name = key.rsplit('/').next().unwrap_or(key);
        let policy: ValidatingAdmissionPolicy =
            match self.storage.get(&build_key(PLURAL, None, name)).await {
                Ok(p) => p,
                Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
                Err(e) => return Err(anyhow!("failed to get policy {name:?}: {e}")),
            };
        self.reconcile(&policy).await
    }

    /// `reconcile` (controller.go:147-168).
    pub async fn reconcile(&self, policy: &ValidatingAdmissionPolicy) -> Result<()> {
        let generation = policy.metadata.generation.unwrap_or(0);
        let observed = policy
            .status
            .as_ref()
            .and_then(|s| s.observed_generation)
            .unwrap_or(0);
        if generation <= observed {
            return Ok(());
        }
        let warnings = check(policy, &self.env_set);
        let key = build_key(PLURAL, None, &policy.metadata.name);
        // ApplyStatus of observedGeneration + typeChecking.expressionWarnings
        // (see the module docs for the deviation).
        let mut current: ValidatingAdmissionPolicy = self.storage.get(&key).await.map_err(|e| {
            anyhow!(
                "failed to apply status of policy {:?}: {e}",
                policy.metadata.name
            )
        })?;
        let status = current
            .status
            .get_or_insert(ValidatingAdmissionPolicyStatus {
                observed_generation: None,
                type_checking: None,
                conditions: None,
            });
        status.observed_generation = Some(generation);
        status.type_checking = Some(TypeChecking {
            expression_warnings: if warnings.is_empty() {
                None
            } else {
                Some(warnings)
            },
        });
        self.storage
            .update_status(&key, &current)
            .await
            .map_err(|e| {
                anyhow!(
                    "failed to apply status of policy {:?}: {e}",
                    policy.metadata.name
                )
            })?;
        Ok(())
    }
}
