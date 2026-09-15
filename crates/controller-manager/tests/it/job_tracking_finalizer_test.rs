//! Exactly-once Job pod accounting via the `batch.kubernetes.io/job-tracking`
//! finalizer.
//!
//! Upstream: `pkg/controller/job/job_controller.go`
//! (`trackJobStatusAndRemoveFinalizers`, `flushUncountedAndRemoveFinalizers`,
//! `cleanUncountedPodsWithoutFinalizers`) and `pkg/controller/job/tracking_utils.go`.
//!
//! Before this mechanism existed here, the controller recomputed
//! `status.succeeded` / `status.failed` from the *live* pod list on every pass.
//! That makes the counters a function of which pods happen to still exist, so a
//! pod that terminates and is deleted between two reconciles is never counted —
//! its outcome is simply lost, and the Job either hangs short of `completions`
//! or forgets a failure that should have counted against `backoffLimit`.
//! Upstream solved this by holding a finalizer on every Job pod until the pod
//! has been recorded in the Job status. These tests pin that contract.

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::workloads::*;
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::job::JobController;
use rusternetes_controller_manager::controllers::job_tracking::JOB_TRACKING_FINALIZER;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

async fn setup() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn make_job(name: &str, namespace: &str, completions: i32, parallelism: i32) -> Job {
    let mut labels = HashMap::new();
    labels.insert("job-name".to_string(), name.to_string());

    Job {
        type_meta: TypeMeta {
            kind: "Job".to_string(),
            api_version: "batch/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: JobSpec {
            completions: Some(completions),
            parallelism: Some(parallelism),
            backoff_limit: Some(6),
            active_deadline_seconds: None,
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new(format!("{}-pod", name));
                    meta.labels = Some(labels);
                    meta
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "task".to_string(),
                        image: "registry.k8s.io/e2e-test-images/busybox:1.36.1-1".to_string(),
                        command: Some(vec![
                            "sh".to_string(),
                            "-c".to_string(),
                            "echo Hello".to_string(),
                        ]),
                        ..Default::default()
                    }],
                    restart_policy: Some("Never".to_string()),
                    ..Default::default()
                },
            },
            selector: None,
            manual_selector: None,
            suspend: None,
            ttl_seconds_after_finished: None,
            completion_mode: None,
            backoff_limit_per_index: None,
            max_failed_indexes: None,
            pod_failure_policy: None,
            pod_replacement_policy: None,
            success_policy: None,
            managed_by: None,
        },
        status: None,
    }
}

async fn job_pods(storage: &Arc<MemoryStorage>, namespace: &str) -> Vec<Pod> {
    let mut pods: Vec<Pod> = storage
        .list(&format!("/registry/pods/{}/", namespace))
        .await
        .unwrap();
    pods.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
    pods
}

/// Drive a pod to a terminal phase the way a kubelet would: phase only, every
/// other field (finalizers included) untouched.
async fn set_phase(storage: &Arc<MemoryStorage>, namespace: &str, pod: &Pod, phase: Phase) {
    let key = build_key("pods", Some(namespace), &pod.metadata.name);
    let mut p: Pod = storage.get(&key).await.unwrap();
    p.status = Some(PodStatus {
        phase: Some(phase),
        ..Default::default()
    });
    storage.update(&key, &p).await.unwrap();
}

/// Delete a pod outright, as the api-server does the instant the last
/// finalizer comes off — and as a node pressure eviction or a `kubectl delete`
/// does whenever nothing holds the object back.
async fn hard_delete(storage: &Arc<MemoryStorage>, namespace: &str, pod: &Pod) {
    let key = build_key("pods", Some(namespace), &pod.metadata.name);
    storage.delete(&key).await.unwrap();
}

fn status_of(job: &Job) -> JobStatus {
    job.status.clone().expect("controller writes a status")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Every pod the Job controller creates carries the tracking finalizer.
///
/// Upstream `pkg/controller/job/job_controller.go` builds the pod template with
/// `Finalizers: []string{batch.JobTrackingFinalizer}`. Without it the whole
/// accounting protocol has nothing to hold the pod in place.
#[tokio::test]
async fn a_created_pod_carries_the_tracking_finalizer() {
    let storage = setup().await;
    let job = make_job("tracked", "default", 2, 2);
    storage
        .create(&build_key("jobs", Some("default"), "tracked"), &job)
        .await
        .unwrap();

    JobController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();

    let pods = job_pods(&storage, "default").await;
    assert_eq!(pods.len(), 2, "first reconcile creates `parallelism` pods");
    for pod in &pods {
        assert!(
            pod.metadata
                .finalizers
                .as_ref()
                .is_some_and(|f| f.iter().any(|x| x == JOB_TRACKING_FINALIZER)),
            "pod {} must carry {} so it cannot be deleted before it is counted",
            pod.metadata.name,
            JOB_TRACKING_FINALIZER
        );
    }
}

/// Once a pod has been accounted for, the controller releases it.
///
/// The finalizer is a means, not an end: leaving it behind would wedge every
/// finished Job pod in `Terminating` forever, which is a worse bug than the one
/// it fixes.
#[tokio::test]
async fn the_finalizer_comes_off_once_the_pod_is_accounted_for() {
    let storage = setup().await;
    let job = make_job("release", "default", 1, 1);
    storage
        .create(&build_key("jobs", Some("default"), "release"), &job)
        .await
        .unwrap();
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pod = job_pods(&storage, "default").await.remove(0);
    set_phase(&storage, "default", &pod, Phase::Succeeded).await;
    controller.reconcile_all().await.unwrap();

    let pod: Pod = storage
        .get(&build_key("pods", Some("default"), &pod.metadata.name))
        .await
        .unwrap();
    assert!(
        !pod.metadata
            .finalizers
            .as_ref()
            .is_some_and(|f| f.iter().any(|x| x == JOB_TRACKING_FINALIZER)),
        "a counted pod must be released, or it lingers in Terminating forever"
    );
}

/// The bug this whole mechanism exists to prevent.
///
/// A pod succeeds, is counted, and is then deleted — by the api-server the
/// moment its finalizer came off, by an eviction, or by a user. Its
/// contribution to `status.succeeded` must survive its own deletion, because
/// the Job is only Complete when `succeeded == completions` and there is
/// nothing left to recount it from.
#[tokio::test]
async fn a_counted_pod_still_counts_after_it_is_deleted() {
    let storage = setup().await;
    let job = make_job("survive", "default", 2, 1);
    let key = build_key("jobs", Some("default"), "survive");
    storage.create(&key, &job).await.unwrap();
    let controller = JobController::new(storage.clone());

    // Attempt 1 succeeds and is counted.
    controller.reconcile_all().await.unwrap();
    let first = job_pods(&storage, "default").await.remove(0);
    set_phase(&storage, "default", &first, Phase::Succeeded).await;
    controller.reconcile_all().await.unwrap();
    controller.reconcile_all().await.unwrap();
    assert_eq!(
        status_of(&storage.get::<Job>(&key).await.unwrap()).succeeded,
        Some(1),
        "the first success is counted"
    );

    // It is now released, so it can vanish at any moment.
    hard_delete(&storage, "default", &first).await;
    controller.reconcile_all().await.unwrap();

    let status = status_of(&storage.get::<Job>(&key).await.unwrap());
    assert_eq!(
        status.succeeded,
        Some(1),
        "deleting an already-counted pod must not un-count it"
    );
}

/// Two reconciles over the same terminal pod must not count it twice.
///
/// This is the other half of exactly-once. Upstream's `uidTrackingExpectations`
/// exists precisely because a pod whose finalizer removal is in flight still
/// reads as "finalizer present" on a stale list.
#[tokio::test]
async fn a_pod_is_counted_exactly_once_across_repeated_reconciles() {
    let storage = setup().await;
    let job = make_job("once", "default", 3, 1);
    let key = build_key("jobs", Some("default"), "once");
    storage.create(&key, &job).await.unwrap();
    let controller = JobController::new(storage.clone());

    controller.reconcile_all().await.unwrap();
    let pod = job_pods(&storage, "default").await.remove(0);
    set_phase(&storage, "default", &pod, Phase::Succeeded).await;

    for _ in 0..5 {
        controller.reconcile_all().await.unwrap();
    }

    let status = status_of(&storage.get::<Job>(&key).await.unwrap());
    assert_eq!(
        status.succeeded,
        Some(1),
        "one succeeded pod is worth exactly one, however often we reconcile"
    );
}

/// A failure matching an `Ignore` podFailurePolicy rule is never counted —
/// not even transiently.
///
/// Upstream `matchPodFailurePolicy` returns `(nil, false, &ignore)` for an
/// `Ignore` match, and the caller then never appends the UID to
/// `uncountedTerminatedPods`. Counting it first and subtracting later is not
/// equivalent: the inflated value can be observed, and once the pod is deleted
/// there is nothing left to subtract.
#[tokio::test]
async fn an_ignored_failure_is_never_counted_even_after_the_pod_is_gone() {
    let storage = setup().await;
    let mut job = make_job("ignored", "default", 1, 1);
    job.spec.backoff_limit = Some(0);
    job.spec.pod_failure_policy = Some(PodFailurePolicy {
        rules: vec![PodFailurePolicyRule {
            action: "Ignore".to_string(),
            on_exit_codes: None,
            on_pod_conditions: vec![PodFailurePolicyOnPodConditionsPattern {
                condition_type: "DisruptionTarget".to_string(),
                status: Some("True".to_string()),
            }],
        }],
    });
    let key = build_key("jobs", Some("default"), "ignored");
    storage.create(&key, &job).await.unwrap();
    let controller = JobController::new(storage.clone());

    controller.reconcile_all().await.unwrap();
    let pod = job_pods(&storage, "default").await.remove(0);

    // Evict it: Failed, with the DisruptionTarget condition the rule matches.
    let pod_key = build_key("pods", Some("default"), &pod.metadata.name);
    let mut evicted: Pod = storage.get(&pod_key).await.unwrap();
    evicted.status = Some(PodStatus {
        phase: Some(Phase::Failed),
        conditions: Some(vec![PodCondition {
            condition_type: "DisruptionTarget".to_string(),
            status: "True".to_string(),
            reason: Some("EvictionByEvictionAPI".to_string()),
            message: None,
            last_probe_time: None,
            last_transition_time: None,
            observed_generation: None,
        }]),
        ..Default::default()
    });
    storage.update(&pod_key, &evicted).await.unwrap();

    controller.reconcile_all().await.unwrap();
    assert_eq!(
        status_of(&storage.get::<Job>(&key).await.unwrap()).failed,
        Some(0),
        "an Ignore-matched failure must not reach status.failed"
    );

    // And it stays uncounted once the evicted pod is collected.
    hard_delete(&storage, "default", &evicted).await;
    controller.reconcile_all().await.unwrap();
    let status = status_of(&storage.get::<Job>(&key).await.unwrap());
    assert_eq!(
        status.failed,
        Some(0),
        "with backoffLimit 0, counting it would wrongly fail the Job"
    );
    assert!(
        !status
            .conditions
            .unwrap_or_default()
            .iter()
            .any(|c| c.condition_type == "Failed" && c.status == "True"),
        "the Job must still be running, not BackoffLimitExceeded"
    );
}

/// A failure that vanishes before the next pass still counts against
/// `backoffLimit`.
///
/// The mirror image of [`a_counted_pod_still_counts_after_it_is_deleted`]: if a
/// failure can be lost, a Job that should have hit its backoff limit runs
/// forever instead.
#[tokio::test]
async fn a_failure_still_counts_after_the_pod_is_deleted() {
    let storage = setup().await;
    let mut job = make_job("backoff", "default", 1, 1);
    job.spec.backoff_limit = Some(2);
    let key = build_key("jobs", Some("default"), "backoff");
    storage.create(&key, &job).await.unwrap();
    let controller = JobController::new(storage.clone());

    for _ in 0..3 {
        controller.reconcile_all().await.unwrap();
        // Fail whichever attempt is currently live, count it, then let it be
        // collected — the pod no longer exists on the next pass.
        let live: Vec<Pod> = job_pods(&storage, "default")
            .await
            .into_iter()
            .filter(|p| {
                !matches!(
                    p.status.as_ref().and_then(|s| s.phase.as_ref()),
                    Some(Phase::Failed) | Some(Phase::Succeeded)
                )
            })
            .collect();
        let Some(pod) = live.first() else { break };
        set_phase(&storage, "default", pod, Phase::Failed).await;
        controller.reconcile_all().await.unwrap();
        let pod: Pod = storage
            .get(&build_key("pods", Some("default"), &pod.metadata.name))
            .await
            .unwrap();
        hard_delete(&storage, "default", &pod).await;
    }

    controller.reconcile_all().await.unwrap();
    let status = status_of(&storage.get::<Job>(&key).await.unwrap());
    assert_eq!(
        status.failed,
        Some(3),
        "three failures happened; deleting the pods must not forget them"
    );
    assert!(
        status
            .conditions
            .unwrap_or_default()
            .iter()
            .any(|c| c.condition_type == "Failed" && c.status == "True"),
        "3 failures with backoffLimit 2 must fail the Job"
    );
}

/// A pod deleted while still Running counts as a failure and is released.
///
/// Upstream `isPodFailed` (`pkg/controller/job/job_controller.go:2011`):
/// "Count deleted Pods as failures to account for orphan Pods that never have a
/// chance to reach the Failed phase." Without this the pod keeps its tracking
/// finalizer while the controller waits for a terminal phase the kubelet will
/// never write, and it sits in `Terminating` forever.
#[tokio::test]
async fn a_pod_deleted_while_running_counts_as_failed_and_is_released() {
    let storage = setup().await;
    let job = make_job("evict", "default", 2, 1);
    let key = build_key("jobs", Some("default"), "evict");
    storage.create(&key, &job).await.unwrap();
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // Running, then deleted — no terminal phase ever arrives.
    let pod = job_pods(&storage, "default").await.remove(0);
    let pod_key = build_key("pods", Some("default"), &pod.metadata.name);
    let mut running: Pod = storage.get(&pod_key).await.unwrap();
    running.status = Some(PodStatus {
        phase: Some(Phase::Running),
        ..Default::default()
    });
    running.metadata.deletion_timestamp = Some(chrono::Utc::now());
    storage.update(&pod_key, &running).await.unwrap();

    controller.reconcile_all().await.unwrap();
    controller.reconcile_all().await.unwrap();

    assert_eq!(
        status_of(&storage.get::<Job>(&key).await.unwrap()).failed,
        Some(1),
        "a pod deleted before it could report a phase is still a failure"
    );
    let released: Pod = storage.get(&pod_key).await.unwrap();
    assert!(
        !released
            .metadata
            .finalizers
            .as_ref()
            .is_some_and(|f| f.iter().any(|x| x == JOB_TRACKING_FINALIZER)),
        "it must not be left holding the finalizer in Terminating"
    );
}

/// Once the Job is finished, every pod it still holds is let go.
///
/// Upstream `canRemoveFinalizer` (`job_controller.go:1359`) returns true
/// outright when `jobCtx.finishedCondition != nil`: nothing further will be
/// counted, so holding a still-Running pod back only wedges it.
#[tokio::test]
async fn a_finished_job_releases_every_pod_it_still_holds() {
    let storage = setup().await;
    let mut job = make_job("done", "default", 2, 2);
    job.spec.backoff_limit = Some(0);
    let key = build_key("jobs", Some("default"), "done");
    storage.create(&key, &job).await.unwrap();
    let controller = JobController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // One pod fails past `backoffLimit` — finishing the Job — while the other
    // is still running and still held by the tracking finalizer.
    let pods = job_pods(&storage, "default").await;
    assert_eq!(pods.len(), 2, "parallelism 2 creates two pods");
    set_phase(&storage, "default", &pods[0], Phase::Failed).await;
    set_phase(&storage, "default", &pods[1], Phase::Running).await;

    controller.reconcile_all().await.unwrap();
    let finished = storage.get::<Job>(&key).await.unwrap();
    assert!(
        status_of(&finished)
            .conditions
            .unwrap_or_default()
            .iter()
            .any(|c| c.condition_type == "Failed" && c.status == "True"),
        "exceeding backoffLimit must finish the Job"
    );

    for pod in &pods {
        let pod_key = build_key("pods", Some("default"), &pod.metadata.name);
        // The running pod is deleted outright on completion; either way nothing
        // may be left holding the finalizer.
        if let Ok(p) = storage.get::<Pod>(&pod_key).await {
            assert!(
                !p.metadata
                    .finalizers
                    .as_ref()
                    .is_some_and(|f| f.iter().any(|x| x == JOB_TRACKING_FINALIZER)),
                "pod {} still holds the tracking finalizer after the Job finished",
                p.metadata.name
            );
        }
    }
}
