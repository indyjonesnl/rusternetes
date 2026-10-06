//! A controller's status write must be conditional on the resourceVersion of
//! the object it read (#2153).
//!
//! Upstream controllers compute status from an informer snapshot and write it
//! with `UpdateStatus` on that same object; the registry's `Store.Update`
//! enforces the `resourceVersion` precondition, and a lost race surfaces as a
//! Conflict that the workqueue retries
//! (pkg/registry/generic/registry/store.go `Update` -> `GuaranteedUpdate`).
//! `Storage::update_status` ignores the caller's resourceVersion on the direct
//! backends, so a controller using it silently overwrites whatever landed
//! between its read and its write. `Storage::update_status_cas` is the
//! conditional write (#2154).
//!
//! The double below models the race: the first conditional status write finds
//! another writer has bumped the object since the controller read it. A
//! controller that adopted `update_status_cas` observes that (`raced` flips and
//! its stale write is refused, leaving the stored status untouched); one still
//! on `update_status` never reaches the conditional path, so `raced` stays
//! false and its stale status lands.

use rusternetes_controller_manager::controllers::{
    cronjob::CronJobController, daemonset::DaemonSetController,
    hpa::HorizontalPodAutoscalerController, pod_disruption_budget::StalePodDisruptionController,
    replicaset::ReplicaSetController, replicationcontroller::ReplicationControllerController,
    resource_quota::ResourceQuotaController, statefulset::StatefulSetController,
};
use rusternetes_storage::{memory::MemoryStorage, Storage, WatchStream};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// `MemoryStorage` whose first conditional status write is preceded by a
/// concurrent writer bumping the object's resourceVersion.
struct RacingStatusStorage {
    inner: MemoryStorage,
    raced: AtomicBool,
}

impl RacingStatusStorage {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryStorage::new(),
            raced: AtomicBool::new(false),
        })
    }

    fn raced(&self) -> bool {
        self.raced.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Storage for RacingStatusStorage {
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

    async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.update(key, value).await
    }

    async fn update_subresource<T>(
        &self,
        key: &str,
        subresource: &str,
        value: &T,
    ) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.update_subresource(key, subresource, value).await
    }

    async fn update_status<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.update_status(key, value).await
    }

    async fn update_status_cas<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        if !self.raced.swap(true, Ordering::SeqCst) {
            // Another writer lands between the controller's read and its write.
            let mut other: Value = self.inner.get(key).await?;
            other["metadata"]["annotations"] = json!({"raced": "1"});
            self.inner.update(key, &other).await?;
        }
        self.inner.update_status_cas(key, value).await
    }

    async fn update_raw(&self, key: &str, value: &Value) -> rusternetes_common::Result<()> {
        self.inner.update_raw(key, value).await
    }

    async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
        self.inner.delete(key).await
    }

    async fn delete_gracefully(&self, key: &str) -> rusternetes_common::Result<()> {
        self.inner.delete_gracefully(key).await
    }

    async fn watch(&self, prefix: &str) -> rusternetes_common::Result<WatchStream> {
        self.inner.watch(prefix).await
    }

    async fn watch_from_revision(
        &self,
        prefix: &str,
        revision: i64,
    ) -> rusternetes_common::Result<WatchStream> {
        self.inner.watch_from_revision(prefix, revision).await
    }

    async fn current_revision(&self) -> rusternetes_common::Result<i64> {
        self.inner.current_revision().await
    }

    async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
        self.inner.is_revision_compacted(revision).await
    }
}

fn template(app: &str) -> Value {
    json!({
        "metadata": {"labels": {"app": app}},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    })
}

async fn seed(storage: &RacingStatusStorage, key: &str, obj: Value) {
    storage.create(key, &obj).await.unwrap();
}

/// The first reconcile loses the race: the stale status must NOT land.
/// The next reconcile re-reads and converges.
async fn assert_lost_race_is_refused(
    storage: &RacingStatusStorage,
    key: &str,
    has_status: impl Fn(&Value) -> bool,
) {
    assert!(
        storage.raced(),
        "{key}: the status write never went through update_status_cas"
    );
    let stored: Value = storage.get(key).await.unwrap();
    assert!(
        !has_status(&stored),
        "{key}: a stale status write overwrote a concurrent change: {stored}"
    );
}

async fn assert_converges(
    storage: &RacingStatusStorage,
    key: &str,
    has_status: impl Fn(&Value) -> bool,
) {
    let stored: Value = storage.get(key).await.unwrap();
    assert!(
        has_status(&stored),
        "{key}: status never converged after the retry: {stored}"
    );
}

fn status_is_set(v: &Value) -> bool {
    v.get("status").is_some_and(|s| !s.is_null())
}

#[tokio::test]
async fn replicaset_status_write_conflicts_on_stale_read() {
    let storage = RacingStatusStorage::new();
    let key = "/registry/replicasets/default/rs";
    seed(
        &storage,
        key,
        json!({
            "apiVersion": "apps/v1", "kind": "ReplicaSet",
            "metadata": {"name": "rs", "namespace": "default", "uid": "u-rs"},
            "spec": {"replicas": 0, "selector": {"matchLabels": {"app": "rs"}}, "template": template("rs")}
        }),
    )
    .await;
    let c = ReplicaSetController::new(storage.clone(), 10);
    c.reconcile_all().await.unwrap();
    assert_lost_race_is_refused(&storage, key, status_is_set).await;
    c.reconcile_all().await.unwrap();
    assert_converges(&storage, key, status_is_set).await;
}

#[tokio::test]
async fn replicationcontroller_status_write_conflicts_on_stale_read() {
    let storage = RacingStatusStorage::new();
    let key = "/registry/replicationcontrollers/default/rc";
    seed(
        &storage,
        key,
        json!({
            "apiVersion": "v1", "kind": "ReplicationController",
            "metadata": {"name": "rc", "namespace": "default", "uid": "u-rc"},
            "spec": {"replicas": 0, "selector": {"app": "rc"}, "template": template("rc")}
        }),
    )
    .await;
    let c = ReplicationControllerController::new(storage.clone(), 10);
    c.reconcile_all().await.unwrap();
    assert_lost_race_is_refused(&storage, key, status_is_set).await;
    c.reconcile_all().await.unwrap();
    assert_converges(&storage, key, status_is_set).await;
}

#[tokio::test]
async fn daemonset_status_write_conflicts_on_stale_read() {
    let storage = RacingStatusStorage::new();
    let key = "/registry/daemonsets/default/ds";
    seed(
        &storage,
        key,
        json!({
            "apiVersion": "apps/v1", "kind": "DaemonSet",
            "metadata": {"name": "ds", "namespace": "default", "uid": "u-ds"},
            "spec": {"selector": {"matchLabels": {"app": "ds"}}, "template": template("ds")}
        }),
    )
    .await;
    let c = DaemonSetController::new(storage.clone());
    c.reconcile_all().await.unwrap();
    assert_lost_race_is_refused(&storage, key, status_is_set).await;
    c.reconcile_all().await.unwrap();
    assert_converges(&storage, key, status_is_set).await;
}

#[tokio::test]
async fn statefulset_status_write_conflicts_on_stale_read() {
    let storage = RacingStatusStorage::new();
    let key = "/registry/statefulsets/default/sts";
    seed(
        &storage,
        key,
        json!({
            "apiVersion": "apps/v1", "kind": "StatefulSet",
            "metadata": {"name": "sts", "namespace": "default", "uid": "u-sts"},
            "spec": {"replicas": 0, "serviceName": "sts",
                     "selector": {"matchLabels": {"app": "sts"}}, "template": template("sts")}
        }),
    )
    .await;
    let c = StatefulSetController::new(storage.clone());
    c.reconcile_all().await.unwrap();
    assert_lost_race_is_refused(&storage, key, status_is_set).await;
    c.reconcile_all().await.unwrap();
    assert_converges(&storage, key, status_is_set).await;
}

#[tokio::test]
async fn hpa_status_write_conflicts_on_stale_read() {
    let storage = RacingStatusStorage::new();
    let key = "/registry/horizontalpodautoscalers/default/hpa";
    seed(
        &storage,
        key,
        json!({
            "apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
            "metadata": {"name": "hpa", "namespace": "default", "uid": "u-hpa"},
            "spec": {"scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "gone"},
                     "minReplicas": 1, "maxReplicas": 3}
        }),
    )
    .await;
    let c = HorizontalPodAutoscalerController::new(storage.clone());
    c.reconcile_all().await.unwrap();
    assert_lost_race_is_refused(&storage, key, status_is_set).await;
    c.reconcile_all().await.unwrap();
    assert_converges(&storage, key, status_is_set).await;
}

#[tokio::test]
async fn resourcequota_status_write_conflicts_on_stale_read() {
    let storage = RacingStatusStorage::new();
    let key = "/registry/resourcequotas/default/rq";
    seed(
        &storage,
        key,
        json!({
            "apiVersion": "v1", "kind": "ResourceQuota",
            "metadata": {"name": "rq", "namespace": "default", "uid": "u-rq"},
            "spec": {"hard": {"pods": "10"}}
        }),
    )
    .await;
    let c = ResourceQuotaController::new(storage.clone());
    c.reconcile_all().await.unwrap();
    assert_lost_race_is_refused(&storage, key, status_is_set).await;
    c.reconcile_all().await.unwrap();
    assert_converges(&storage, key, status_is_set).await;
}

/// `syncStalePodDisruption` (pkg/controller/disruption/disruption.go:774):
/// `UpdateStatus` on the pod it read. A pod updated since (here: the kubelet
/// finishing the pod) must conflict and be requeued, not be overwritten.
#[tokio::test]
async fn stale_pod_disruption_cleanup_conflicts_on_stale_read() {
    let storage = RacingStatusStorage::new();
    let key = "/registry/pods/default/p";
    seed(
        &storage,
        key,
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "default", "uid": "u-p"},
            "spec": {"containers": [{"name": "c", "image": "busybox"}]},
            "status": {"phase": "Running", "conditions": [{
                "type": "DisruptionTarget", "status": "True",
                "lastTransitionTime": "2020-01-01T00:00:00Z"}]}
        }),
    )
    .await;
    let c = StalePodDisruptionController::with_timeout(storage.clone(), Duration::ZERO);
    c.reconcile_all().await.unwrap();
    assert!(storage.raced(), "cleanup never used update_status_cas");
    let stored: Value = storage.get(key).await.unwrap();
    let cond_status = |v: &Value| {
        v.pointer("/status/conditions/0/status")
            .and_then(|s| s.as_str())
            .map(str::to_string)
    };
    assert_eq!(
        cond_status(&stored).as_deref(),
        Some("True"),
        "a stale cleanup overwrote a concurrent change"
    );
    // The requeue: the next sync re-reads and clears the condition.
    c.reconcile_all().await.unwrap();
    let stored: Value = storage.get(key).await.unwrap();
    assert_eq!(cond_status(&stored).as_deref(), Some("False"));
}

/// `syncCronJob` (pkg/controller/cronjob/cronjob_controllerv2.go:603-642):
/// the Job is created BEFORE `UpdateStatus`, under the deterministic name
/// `getJobName` (`{cronjob}-{scheduledTime/60}`, utils.go / v2.go:676), so a
/// Conflict on the status write requeues and the retry's create returns
/// AlreadyExists (treated as success) instead of spawning a second Job (#2159).
#[tokio::test]
async fn cronjob_status_write_conflicts_without_duplicating_the_job() {
    use rusternetes_common::resources::workloads::Job;
    let storage = RacingStatusStorage::new();
    let key = "/registry/cronjobs/default/cj";
    seed(
        &storage,
        key,
        json!({
            "apiVersion": "batch/v1", "kind": "CronJob",
            "metadata": {"name": "cj", "namespace": "default", "uid": "u-cj",
                         // utils.go:101: the first run walks from creationTimestamp.
                         "creationTimestamp": (chrono::Utc::now() - chrono::Duration::minutes(10)).to_rfc3339()},
            "spec": {"schedule": "* * * * *",
                     "jobTemplate": {"spec": {"template": {"spec": {
                         "restartPolicy": "Never",
                         "containers": [{"name": "c", "image": "busybox"}]}}}}}
        }),
    )
    .await;
    let c = CronJobController::new(storage.clone());
    c.reconcile_all().await.unwrap();
    assert_lost_race_is_refused(&storage, key, status_is_set).await;
    let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(jobs.len(), 1, "the lost race created a Job");

    // The requeue: the retry hits AlreadyExists and still converges.
    c.reconcile_all().await.unwrap();
    let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
    assert_eq!(
        jobs.len(),
        1,
        "the retry created a duplicate Job: {:?}",
        jobs.iter().map(|j| &j.metadata.name).collect::<Vec<_>>()
    );
    assert_converges(&storage, key, |v| {
        v.pointer("/status/active/0/name")
            .and_then(|n| n.as_str())
            .is_some_and(|n| n == jobs[0].metadata.name)
    })
    .await;
}
