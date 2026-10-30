//! Unit tests moved from `src/controllers/job.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::workloads::{
        Job, JobCondition, JobStatus, SuccessPolicy, UncountedTerminatedPods,
    };
    pub use rusternetes_common::resources::{Pod, PodStatus};
    pub use rusternetes_common::types::{OwnerReference, Phase};
    pub use rusternetes_controller_manager::controllers::controller_utils::{
        is_pod_ready, sort_active_pods,
    };
    pub use rusternetes_controller_manager::controllers::expectations::ControllerExpectations;
    pub use rusternetes_controller_manager::controllers::job_tracking::{
        clean_uncounted_pods_without_finalizers, has_job_tracking_finalizer, push_uncounted_failed,
        push_uncounted_succeeded, remove_tracking_finalizer, uncounted_has_failed,
        uncounted_has_succeeded, FinalizerExpectations, JOB_TRACKING_FINALIZER,
    };
    pub use rusternetes_controller_manager::controllers::pod_informer::{
        ControllerRef, PodEventTarget, ResolvedController, SharedPodInformer,
    };
    pub use rusternetes_controller_manager::controllers::replicationcontroller::{
        generate_pod_name, slow_start_batches_capped, MAX_NAME_GENERATION_CREATE_ATTEMPTS,
    };
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::workloads::{Job, JobSpec, PodTemplateSpec};
    use rusternetes_common::resources::{
        Container, ContainerState, ContainerStateRunning, ContainerStateTerminated,
        ContainerStatus, Pod, PodCondition, PodSpec, PodStatus,
    };
    use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
    use rusternetes_controller_manager::controllers::job::*;
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    fn test_container() -> Container {
        Container {
            name: "test".to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            env_from: None,
            resources: None,
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

    fn make_job(name: &str, namespace: &str, completions: i32, parallelism: i32) -> Job {
        Job {
            type_meta: TypeMeta {
                kind: "Job".to_string(),
                api_version: "batch/v1".to_string(),
            },
            metadata: ObjectMeta {
                name: name.to_string(),
                namespace: Some(namespace.to_string()),
                uid: "job-uid-1".to_string(),
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: JobSpec {
                template: PodTemplateSpec {
                    metadata: None,
                    spec: PodSpec {
                        containers: vec![test_container()],
                        ..Default::default()
                    },
                },
                completions: Some(completions),
                parallelism: Some(parallelism),
                backoff_limit: Some(6),
                active_deadline_seconds: None,
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

    fn make_pod(name: &str, namespace: &str, phase: Phase, job_name: &str, job_uid: &str) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: name.to_string(),
                namespace: Some(namespace.to_string()),
                uid: format!("pod-uid-{}", name),
                // Model a controller-created pod: the Job controller stamps
                // every pod it creates with the tracking finalizer, and a pod
                // without it is invisible to the accounting protocol by design
                // (upstream `getValidPodsWithFilter`: "Pods that don't have a
                // completion finalizer ... have already been accounted for").
                finalizers: Some(vec![JOB_TRACKING_FINALIZER.to_string()]),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("job-name".to_string(), job_name.to_string());
                    m
                }),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: job_name.to_string(),
                    uid: job_uid.to_string(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(phase),
                ..Default::default()
            }),
        }
    }

    fn make_indexed_pod(
        name: &str,
        namespace: &str,
        phase: Phase,
        job_name: &str,
        job_uid: &str,
        index: i32,
    ) -> Pod {
        let mut pod = make_pod(name, namespace, phase, job_name, job_uid);
        pod.metadata.annotations = Some({
            let mut m = HashMap::new();
            m.insert(
                "batch.kubernetes.io/job-completion-index".to_string(),
                index.to_string(),
            );
            m
        });
        if let Some(ref mut labels) = pod.metadata.labels {
            labels.insert(
                "batch.kubernetes.io/job-completion-index".to_string(),
                index.to_string(),
            );
        }
        pod
    }

    fn make_failed_pod_with_exit_code(
        name: &str,
        namespace: &str,
        job_name: &str,
        job_uid: &str,
        index: i32,
        exit_code: i32,
    ) -> Pod {
        let mut pod = make_indexed_pod(name, namespace, Phase::Failed, job_name, job_uid, index);
        if let Some(ref mut status) = pod.status {
            status.container_statuses = Some(vec![ContainerStatus {
                name: "test".to_string(),
                ready: false,
                restart_count: 0,
                state: Some(ContainerState {
                    terminated: Some(ContainerStateTerminated {
                        exit_code,
                        signal: None,
                        reason: Some("Error".to_string()),
                        message: None,
                        started_at: None,
                        finished_at: None,
                        container_id: None,
                    }),
                    ..Default::default()
                }),
                last_state: None,
                image: Some("busybox".to_string()),
                image_id: None,
                container_id: None,
                started: None,
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                volume_mounts: None,
                stop_signal: None,
                user: None,
            }]);
        }
        pod
    }

    #[test]
    fn test_pods_needed_calculation() {
        let parallelism = 3;
        let completions = 10;
        let active = 2;
        let succeeded = 5;

        let pods_needed = std::cmp::min(
            parallelism - active,             // 1 (can run 1 more in parallel)
            completions - succeeded - active, // 3 (need 3 more to complete)
        );

        assert_eq!(pods_needed, 1);
    }

    #[test]
    fn test_job_completion() {
        let completions = 5;
        let succeeded = 5;
        assert!(succeeded >= completions);
    }

    #[test]
    fn test_backoff_limit_exceeded() {
        let backoff_limit = 6;
        let failed = 7;
        assert!(failed > backoff_limit);
    }

    #[test]
    fn test_parse_index_ranges() {
        assert_eq!(parse_index_ranges(""), HashSet::new());
        assert_eq!(
            parse_index_ranges("0"),
            [0].into_iter().collect::<HashSet<i32>>()
        );
        assert_eq!(
            parse_index_ranges("0,1,2"),
            [0, 1, 2].into_iter().collect::<HashSet<i32>>()
        );
        assert_eq!(
            parse_index_ranges("0-3"),
            [0, 1, 2, 3].into_iter().collect::<HashSet<i32>>()
        );
        assert_eq!(
            parse_index_ranges("0,2-4,7"),
            [0, 2, 3, 4, 7].into_iter().collect::<HashSet<i32>>()
        );
    }

    #[test]
    fn test_format_index_ranges() {
        assert_eq!(format_index_ranges(&[]), "");
        assert_eq!(format_index_ranges(&[0]), "0");
        assert_eq!(format_index_ranges(&[0, 1, 2]), "0-2");
        assert_eq!(format_index_ranges(&[0, 1, 2, 5]), "0-2,5");
        assert_eq!(format_index_ranges(&[0, 2, 3, 4, 7, 8]), "0,2-4,7-8");
    }

    /// A failing Job must reach `Failed` the way upstream stages it, or the
    /// api-server rejects the whole status write.
    ///
    /// Live, against a vanilla control plane, every failing-Job spec died here:
    ///
    /// ```text
    /// Failed to reconcile jobs/job-check/failjob: Bad request:
    ///   Error from server (Invalid): Job.batch "failjob" is invalid:
    ///   [status.completionTime: Invalid value: "...": cannot set completionTime
    ///      when there is no Complete=True condition,
    ///    status.conditions: Invalid value: cannot set Failed=True condition
    ///      without the FailureTarget=true condition]
    /// ```
    ///
    /// (pkg/apis/batch/validation/validation.go:505-522). Upstream appends the
    /// interim `FailureTarget` condition first and only then adds `Failed` with
    /// the same reason/message — `job_controller.go:1307-1316` ->
    /// `newFailedConditionForFailureTarget` — and sets `completionTime` only
    /// for Complete jobs. The rejected write left the Job `active: 1` forever,
    /// so `WaitForJobFailed` timed out in all seven Job [Conformance] specs.
    /// A Job that reaches its completions must stage `SuccessCriteriaMet`
    /// before `Complete`, the mirror of the FailureTarget rule.
    ///
    /// Live, a two-of-two successful Job stalled at `succeeded: 1, active: 1`
    /// forever because the api-server rejected every status write:
    ///
    /// ```text
    /// Failed to reconcile jobs/job-complete/ok: Bad request:
    ///   Error from server (Invalid): Job.batch "ok" is invalid:
    ///   status.conditions: Invalid value: cannot set Complete=True condition
    ///     without the SuccessCriteriaMet=true condition
    /// ```
    ///
    /// (pkg/apis/batch/validation/validation.go:525-527). Upstream appends the
    /// interim SuccessCriteriaMet condition and derives Complete from it with
    /// the same reason and message (job_controller.go:1317-1327). Three Job
    /// [Conformance] specs died on "failed to ensure job completion ... Timed
    /// out after 900s" because of this. The success-policy path already staged
    /// the pair; the ordinary completions-reached path did not.
    ///
    /// `completionTime` stays REQUIRED here — validation.go:505-513 demands it
    /// for Complete jobs (the inverse of the failed-job rule).
    /// Job status counters are monotonically non-decreasing: upstream's
    /// api-server rejects any status update that lowers them —
    /// `RejectDecreasingFailedCounter` / `RejectDecreasingSucceededCounter` in
    /// `pkg/apis/batch/validation/validation.go:722-730`, producing
    /// `status.failed: Invalid value: 0: cannot decrease the failed counter`.
    ///
    /// We recompute the counters from the live pod list every reconcile and
    /// additionally subtract pods matched by an `Ignore` podFailurePolicy rule,
    /// so a `failed` count that was already persisted can drop back down. When
    /// it does, a real api-server rejects every subsequent write, the terminal
    /// `Complete=True` condition never lands, and the Job hangs until the e2e
    /// timeout. That is #1955: the `vanilla-swap-controller-manager` leg's
    /// "ignore failure matching on DisruptionTarget condition" spec, where the
    /// evicted pods' failures are ignored after having been counted.
    #[tokio::test]
    async fn job_status_counters_never_decrease() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("monotonic", "default", 3, 3);
        job.spec.backoff_limit = Some(2);
        // The evicted failures were already accounted into the persisted status
        // by an earlier reconcile, before the Ignore rule matched them.
        job.status = Some(JobStatus {
            active: Some(0),
            succeeded: Some(1),
            failed: Some(3),
            conditions: None,
            start_time: Some(chrono::Utc::now()),
            completion_time: None,
            ready: Some(0),
            terminating: None,
            completed_indexes: None,
            failed_indexes: None,
            uncounted_terminated_pods: None,
            observed_generation: None,
        });
        let job_key = build_key("jobs", Some("default"), "monotonic");
        storage.create(&job_key, &job).await.unwrap();

        // The pods that carried those failures are gone (evicted and deleted);
        // only the succeeded replacements remain, so a recompute from live pods
        // yields failed=0 — lower than what is already persisted.
        for name in ["monotonic-1", "monotonic-2", "monotonic-3"] {
            let pod = make_pod(name, "default", Phase::Succeeded, "monotonic", "job-uid-1");
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }

        let controller = JobController::new(storage.clone());
        controller.reconcile_all().await.unwrap();

        let status = storage
            .get::<Job>(&job_key)
            .await
            .unwrap()
            .status
            .expect("job must have status");

        assert!(
            status.failed.unwrap_or(0) >= 3,
            "status.failed must never drop below the persisted 3 (got {:?}) — \
             a real api-server rejects the write with \"cannot decrease the \
             failed counter\" and the Job never reaches Complete",
            status.failed
        );
        assert!(
            status.succeeded.unwrap_or(0) >= 1,
            "status.succeeded must never drop below the persisted 1 (got {:?})",
            status.succeeded
        );
    }

    #[tokio::test]
    async fn completed_job_stages_success_criteria_met_before_complete() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("ok", "default", 2, 2);
        let job_key = build_key("jobs", Some("default"), "ok");
        storage.create(&job_key, &job).await.unwrap();

        for name in ["ok-1", "ok-2"] {
            let pod = make_pod(name, "default", Phase::Succeeded, "ok", "job-uid-1");
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }

        let controller = JobController::new(storage.clone());
        controller.reconcile_all().await.unwrap();

        let status = storage
            .get::<Job>(&job_key)
            .await
            .unwrap()
            .status
            .expect("job must have status");
        let conditions = status.conditions.unwrap_or_default();

        let complete = conditions
            .iter()
            .find(|c| c.condition_type == "Complete" && c.status == "True")
            .expect("a job that reached its completions must be Complete");
        let met = conditions
            .iter()
            .find(|c| c.condition_type == "SuccessCriteriaMet" && c.status == "True")
            .expect(
                "Complete=True requires SuccessCriteriaMet=True, or the api-server rejects the write",
            );
        assert_eq!(
            met.reason, complete.reason,
            "the Complete condition inherits the interim condition's reason"
        );
        assert!(
            status.completion_time.is_some(),
            "completionTime is required for Complete jobs"
        );
    }

    #[tokio::test]
    async fn failed_job_sets_failure_target_and_no_completion_time() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("failjob", "default", 1, 1);
        job.spec.backoff_limit = Some(0);
        let job_key = build_key("jobs", Some("default"), "failjob");
        storage.create(&job_key, &job).await.unwrap();

        // One pod, already Failed: with backoffLimit 0 the job must fail.
        let pod = make_pod(
            "failjob-1",
            "default",
            Phase::Failed,
            "failjob",
            "job-uid-1",
        );
        storage
            .create(&build_key("pods", Some("default"), "failjob-1"), &pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        controller.reconcile_all().await.unwrap();

        let stored: Job = storage.get(&job_key).await.unwrap();
        let status = stored.status.expect("job must have status");
        let conditions = status.conditions.unwrap_or_default();

        let failed = conditions
            .iter()
            .find(|c| c.condition_type == "Failed" && c.status == "True");
        assert!(failed.is_some(), "job must end Failed, got {conditions:?}");

        let target = conditions
            .iter()
            .find(|c| c.condition_type == "FailureTarget" && c.status == "True")
            .expect("Failed=True requires FailureTarget=True, or the api-server rejects the write");
        assert_eq!(
            target.reason,
            failed.unwrap().reason,
            "FailureTarget carries the same reason the Failed condition reports"
        );

        assert!(
            status.completion_time.is_none(),
            "completionTime is only valid for Complete jobs; a failed job must not set it"
        );
    }

    /// Port of the `generateName` retry in the generic registry's
    /// `Store.Create` (store.go:464-475): a colliding generated pod name is
    /// redrawn, up to 8 attempts, rather than failing the create.
    #[tokio::test]
    async fn test_job_retries_pod_create_when_the_generated_name_collides() {
        use crate::z_unit::controllers__replicationcontroller::tests::NameCollisionStorage;
        // Seven collisions: the eighth (last) attempt succeeds.
        let storage = Arc::new(NameCollisionStorage::new(
            Arc::new(MemoryStorage::new()),
            MAX_NAME_GENERATION_CREATE_ATTEMPTS - 1,
        ));
        let controller = JobController::new(storage.clone());
        let mut job = make_job("collide-job", "default", 1, 1);
        storage
            .create("/registry/jobs/default/collide-job", &job)
            .await
            .unwrap();
        controller.reconcile(&mut job).await.unwrap();
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 1, "collisions must be retried under a new name");
        assert_eq!(pods[0].metadata.name.len(), "collide-job-".len() + 5);
    }

    /// Eight collisions exhaust the budget (store.go:440): the create fails.
    #[tokio::test]
    async fn test_job_pod_create_gives_up_after_eight_name_collisions() {
        use crate::z_unit::controllers__replicationcontroller::tests::NameCollisionStorage;
        let storage = Arc::new(NameCollisionStorage::new(
            Arc::new(MemoryStorage::new()),
            MAX_NAME_GENERATION_CREATE_ATTEMPTS,
        ));
        let controller = JobController::new(storage.clone());
        let job = make_job("exhaust-job", "default", 1, 1);
        let err = controller
            .create_pod(&job, "default", 0, false, None)
            .await
            .unwrap_err();
        assert!(rusternetes_controller_manager::controllers::is_already_exists_anyhow(&err));
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(pods.is_empty());
    }

    #[tokio::test]
    async fn test_job_pods_inherit_sa_image_pull_secrets() {
        // #1084: SA imagePullSecrets must reach controller-created pods, which
        // bypass the api-server admission path.
        let storage = Arc::new(MemoryStorage::new());
        let controller = JobController::new(storage.clone());

        let mut sa = rusternetes_common::resources::ServiceAccount::new("default", "default");
        sa.image_pull_secrets = Some(vec![
            rusternetes_common::resources::service_account::LocalObjectReference {
                name: "regcred".to_string(),
            },
        ]);
        storage
            .create("/registry/serviceaccounts/default/default", &sa)
            .await
            .unwrap();

        let mut job = make_job("pullsecrets-job", "default", 1, 1);
        storage
            .create("/registry/jobs/default/pullsecrets-job", &job)
            .await
            .unwrap();

        controller.reconcile(&mut job).await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 1, "Job should create one pod");
        let secrets = pods[0]
            .spec
            .as_ref()
            .unwrap()
            .image_pull_secrets
            .as_ref()
            .expect("pod must inherit the SA's imagePullSecrets");
        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].name, "regcred");
    }

    #[tokio::test]
    async fn test_backoff_limit_per_index_tracks_failures() {
        let storage = Arc::new(MemoryStorage::new());

        // Create an indexed job with backoffLimitPerIndex=1, 3 completions
        let mut job = make_job("test-job", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit_per_index = Some(1);
        job.spec.backoff_limit = Some(100); // high so global limit doesn't kick in

        let job_key = "/registry/jobs/default/test-job";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "test-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();

        // Index 1 failed twice (exceeds backoffLimitPerIndex=1)
        let pod1a = make_indexed_pod(
            "pod-1a",
            "default",
            Phase::Failed,
            "test-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1a", &pod1a)
            .await
            .unwrap();
        let pod1b = make_indexed_pod(
            "pod-1b",
            "default",
            Phase::Failed,
            "test-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1b", &pod1b)
            .await
            .unwrap();

        // Index 2 succeeded
        let pod2 = make_indexed_pod(
            "pod-2",
            "default",
            Phase::Succeeded,
            "test-job",
            "job-uid-1",
            2,
        );
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // Re-read the job
        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // Job should be failed because: succeeded(0,2) + failed(1) = 3 = completions
        // Index 1 exceeded backoffLimitPerIndex
        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Failed" && c.status == "True"),
            "Job should be marked as Failed"
        );

        // Failed indexes should contain index 1
        assert!(status.failed_indexes.is_some());
        let fi = parse_index_ranges(status.failed_indexes.as_deref().unwrap());
        assert!(fi.contains(&1), "Index 1 should be in failed_indexes");

        // Completed indexes should contain 0 and 2
        assert!(status.completed_indexes.is_some());
        let ci = parse_index_ranges(status.completed_indexes.as_deref().unwrap());
        assert!(
            ci.contains(&0) && ci.contains(&2),
            "Completed indexes should have 0 and 2"
        );
    }

    #[tokio::test]
    async fn test_backoff_limit_per_index_no_retry_for_exhausted_index() {
        let storage = Arc::new(MemoryStorage::new());

        // Create an indexed job with backoffLimitPerIndex=0, 3 completions
        let mut job = make_job("test-job2", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit_per_index = Some(0);
        job.spec.backoff_limit = Some(100);

        let job_key = "/registry/jobs/default/test-job2";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 failed once (exceeds limit of 0)
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Failed,
            "test-job2",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();

        // Index 1 running
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Running,
            "test-job2",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // After reconciliation, no new pod should be created for index 0
        // Check that no new pod with index 0 was created
        let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let index_0_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| {
                p.metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("batch.kubernetes.io/job-completion-index"))
                    .map(|v| v == "0")
                    .unwrap_or(false)
            })
            .collect();

        // Should still be just the one failed pod for index 0, no retry
        assert_eq!(
            index_0_pods.len(),
            1,
            "Should not create a retry pod for exhausted index 0"
        );
    }

    /// `syncJob` evaluates the failure scenarios in order: pod failure policy,
    /// backoffLimit, `pastActiveDeadline`, and only THEN the per-index ones
    /// (maxFailedIndexes / FailedIndexes) (job_controller.go:969-995). A Job
    /// past its deadline that also has a failed index reports DeadlineExceeded.
    #[tokio::test]
    async fn deadline_exceeded_outranks_failed_indexes() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("dl-job", "default", 1, 1);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit_per_index = Some(0);
        job.spec.backoff_limit = Some(100);
        job.spec.active_deadline_seconds = Some(1);
        job.status = Some(JobStatus {
            start_time: Some(chrono::Utc::now() - chrono::Duration::seconds(60)),
            ..Default::default()
        });
        let job_key = build_key("jobs", Some("default"), "dl-job");
        storage.create(&job_key, &job).await.unwrap();
        let pod = make_indexed_pod(
            "dl-job-0",
            "default",
            Phase::Failed,
            "dl-job",
            "job-uid-1",
            0,
        );
        storage
            .create(&build_key("pods", Some("default"), "dl-job-0"), &pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(&job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let stored: Job = storage.get(&job_key).await.unwrap();
        let conditions = stored.status.unwrap().conditions.unwrap_or_default();
        let failed = conditions
            .iter()
            .find(|c| c.condition_type == "Failed" && c.status == "True")
            .unwrap_or_else(|| panic!("job must end Failed, got {conditions:?}"));
        assert_eq!(failed.reason.as_deref(), Some("DeadlineExceeded"));
    }

    /// Every failure scenario feeds the one `finishedCondition`
    /// (`job_controller.go:960-985`) whose message is the upstream literal:
    /// "Job has reached the specified backoff limit" (:964), "Job was active
    /// longer than specified deadline" (:969, no seconds suffix),
    /// "Job has exceeded the specified maximal number of failed indexes"
    /// (:980) and "Job has failed indexes" (:982).
    #[tokio::test]
    async fn finished_condition_messages_match_upstream() {
        async fn run(configure: impl FnOnce(&mut Job), pods: Vec<Pod>) -> String {
            let storage = Arc::new(MemoryStorage::new());
            let mut job = make_job("msg-job", "default", 2, 2);
            configure(&mut job);
            let job_key = build_key("jobs", Some("default"), "msg-job");
            storage.create(&job_key, &job).await.unwrap();
            for p in &pods {
                let k = build_key("pods", Some("default"), &p.metadata.name);
                storage.create(&k, p).await.unwrap();
            }
            let controller = JobController::new(storage.clone());
            let mut job: Job = storage.get(&job_key).await.unwrap();
            controller.reconcile(&mut job).await.unwrap();
            let stored: Job = storage.get(&job_key).await.unwrap();
            let conds = stored.status.unwrap().conditions.unwrap_or_default();
            conds
                .iter()
                .find(|c| c.condition_type == "Failed" && c.status == "True")
                .unwrap_or_else(|| panic!("job must end Failed, got {conds:?}"))
                .message
                .clone()
                .unwrap_or_default()
        }
        let failed_pod = |i: i32| {
            make_indexed_pod(
                &format!("msg-job-{i}"),
                "default",
                Phase::Failed,
                "msg-job",
                "job-uid-1",
                i,
            )
        };

        let backoff = run(
            |j| j.spec.backoff_limit = Some(0),
            vec![make_pod(
                "msg-job-a",
                "default",
                Phase::Failed,
                "msg-job",
                "job-uid-1",
            )],
        )
        .await;
        assert_eq!(backoff, "Job has reached the specified backoff limit");

        let deadline = run(
            |j| {
                j.spec.active_deadline_seconds = Some(1);
                j.status = Some(JobStatus {
                    start_time: Some(chrono::Utc::now() - chrono::Duration::seconds(60)),
                    ..Default::default()
                });
            },
            vec![],
        )
        .await;
        assert_eq!(deadline, "Job was active longer than specified deadline");

        let max_failed = run(
            |j| {
                j.spec.completion_mode = Some("Indexed".to_string());
                j.spec.backoff_limit_per_index = Some(0);
                j.spec.max_failed_indexes = Some(0);
            },
            vec![failed_pod(0)],
        )
        .await;
        assert_eq!(
            max_failed,
            "Job has exceeded the specified maximal number of failed indexes"
        );

        let failed_idx = run(
            |j| {
                j.spec.completion_mode = Some("Indexed".to_string());
                j.spec.backoff_limit_per_index = Some(0);
            },
            vec![failed_pod(0), failed_pod(1)],
        )
        .await;
        assert_eq!(failed_idx, "Job has failed indexes");
    }

    #[tokio::test]
    async fn test_pod_failure_policy_fail_index() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("failindex-job", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit = Some(100);
        // Set up a FailIndex policy for exit code 42
        job.spec.pod_failure_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "action": "FailIndex",
                        "onExitCodes": {
                            "operator": "In",
                            "values": [42]
                        }
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/failindex-job";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "failindex-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();

        // Index 1 failed with exit code 42 -> should trigger FailIndex
        let pod1 =
            make_failed_pod_with_exit_code("pod-1", "default", "failindex-job", "job-uid-1", 1, 42);
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        // Index 2 succeeded
        let pod2 = make_indexed_pod(
            "pod-2",
            "default",
            Phase::Succeeded,
            "failindex-job",
            "job-uid-1",
            2,
        );
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // Failed indexes should include index 1
        assert!(status.failed_indexes.is_some());
        let fi = parse_index_ranges(status.failed_indexes.as_deref().unwrap());
        assert!(
            fi.contains(&1),
            "Index 1 should be in failed_indexes due to FailIndex policy"
        );

        // No new pod should be created for index 1
        let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let index_1_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| {
                p.metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("batch.kubernetes.io/job-completion-index"))
                    .map(|v| v == "1")
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(
            index_1_pods.len(),
            1,
            "No retry pod for FailIndex-ed index 1"
        );
    }

    #[tokio::test]
    async fn test_pod_failure_policy_fail_job() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("failjob-job", "default", 3, 3);
        job.spec.backoff_limit = Some(100);
        job.spec.pod_failure_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "action": "FailJob",
                        "onExitCodes": {
                            "operator": "In",
                            "values": [99]
                        }
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/failjob-job";
        storage.create(job_key, &job).await.unwrap();

        // One pod failed with exit code 99 -> should trigger FailJob
        let pod = make_failed_pod_with_exit_code(
            "pod-fail",
            "default",
            "failjob-job",
            "job-uid-1",
            0,
            99,
        );
        storage
            .create("/registry/pods/default/pod-fail", &pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Failed"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("PodFailurePolicy")),
            "Job should be Failed due to PodFailurePolicy"
        );
    }

    #[tokio::test]
    async fn test_local_restart_completion() {
        let storage = Arc::new(MemoryStorage::new());

        // Job with 2 completions, template has restartPolicy: OnFailure
        let mut job = make_job("restart-job", "default", 2, 2);
        job.spec.template.spec.restart_policy = Some("OnFailure".to_string());

        let job_key = "/registry/jobs/default/restart-job";
        storage.create(job_key, &job).await.unwrap();

        // Pod 1 succeeded (was restarted locally, restart_count > 0, now succeeded)
        let mut pod1 = make_pod(
            "pod-1",
            "default",
            Phase::Succeeded,
            "restart-job",
            "job-uid-1",
        );
        if let Some(ref mut status) = pod1.status {
            status.container_statuses = Some(vec![ContainerStatus {
                name: "test".to_string(),
                ready: false,
                restart_count: 3, // restarted 3 times before succeeding
                state: Some(ContainerState {
                    terminated: Some(ContainerStateTerminated {
                        exit_code: 0,
                        signal: None,
                        reason: Some("Completed".to_string()),
                        message: None,
                        started_at: None,
                        finished_at: None,
                        container_id: None,
                    }),
                    ..Default::default()
                }),
                last_state: None,
                image: Some("busybox".to_string()),
                image_id: None,
                container_id: None,
                started: None,
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                volume_mounts: None,
                stop_signal: None,
                user: None,
            }]);
        }
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        // Pod 2 succeeded
        let pod2 = make_pod(
            "pod-2",
            "default",
            Phase::Succeeded,
            "restart-job",
            "job-uid-1",
        );
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True"),
            "Job should be Complete when locally restarted pods succeed"
        );
        assert_eq!(status.succeeded, Some(2));
    }

    #[tokio::test]
    async fn test_restart_policy_on_failure_preserved() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("onfailure-job", "default", 1, 1);
        job.spec.template.spec.restart_policy = Some("OnFailure".to_string());

        let job_key = "/registry/jobs/default/onfailure-job";
        storage.create(job_key, &job).await.unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // Check that the created pod preserved the OnFailure restart policy
        let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let job_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| {
                p.metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("job-name"))
                    .map(|v| v == "onfailure-job")
                    .unwrap_or(false)
            })
            .collect();

        assert_eq!(job_pods.len(), 1);
        let restart_policy = job_pods[0].spec.as_ref().unwrap().restart_policy.as_deref();
        assert_eq!(
            restart_policy,
            Some("OnFailure"),
            "Pod should preserve OnFailure restart policy from template"
        );
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_indexes() {
        let storage = Arc::new(MemoryStorage::new());

        // Indexed job with 5 completions, successPolicy requiring indexes 0-1
        let mut job = make_job("sp-job", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "succeededIndexes": "0-1"
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/sp-job";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 and 1 succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Succeeded,
            "sp-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        // Indexes 2-4 are still pending
        let pod2 = make_indexed_pod("pod-2", "default", Phase::Pending, "sp-job", "job-uid-1", 2);
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();
        let pod3 = make_indexed_pod("pod-3", "default", Phase::Pending, "sp-job", "job-uid-1", 3);
        storage
            .create("/registry/pods/default/pod-3", &pod3)
            .await
            .unwrap();
        let pod4 = make_indexed_pod("pod-4", "default", Phase::Pending, "sp-job", "job-uid-1", 4);
        storage
            .create("/registry/pods/default/pod-4", &pod4)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        reconcile_settled(&controller, &storage, job_key).await;

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // Job should be complete via successPolicy even though indexes 2-4 are pending
        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True"),
            "Job should be Complete via successPolicy"
        );
        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet" && c.status == "True"),
            "SuccessCriteriaMet condition should be set"
        );
        assert!(status.completion_time.is_some());
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_indexes_terminates_running_ready_pod() {
        // Mirrors [sig-apps] "with successPolicy succeededIndexes rule": an
        // indexed job (completions=5, parallelism=2) whose required index 0
        // succeeds while another index is still Running and Ready. Once the
        // success policy is met the job must complete with active/ready/
        // terminating all 0 and completedIndexes "0" — the conformance test
        // asserts job.Status.Ready == 0.
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-idx-job", "default", 5, 2);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [{ "succeededIndexes": "0" }]
            }))
            .unwrap(),
        );
        let job_key = "/registry/jobs/default/sp-idx-job";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 succeeded (the required index).
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-idx-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();

        // Index 2 is still Running AND Ready — must not be counted once the
        // success policy completes the job.
        let mut pod2 = make_indexed_pod(
            "pod-2",
            "default",
            Phase::Running,
            "sp-idx-job",
            "job-uid-1",
            2,
        );
        if let Some(ref mut st) = pod2.status {
            st.conditions = Some(vec![rusternetes_common::resources::PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]);
        }
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());

        // Two reconciles: the second simulates a follow-up loop where the
        // terminating pod is still present in storage (kubelet hasn't removed
        // it yet) — status must stay settled at 0s.
        for _ in 0..2 {
            reconcile_settled(&controller, &storage, job_key).await;
        }

        let status: JobStatus = storage.get::<Job>(job_key).await.unwrap().status.unwrap();
        assert_eq!(
            status.completed_indexes.as_deref(),
            Some("0"),
            "completedIndexes must be exactly the succeeded index"
        );
        assert_eq!(
            status.active,
            Some(0),
            "active must be 0 after successPolicy"
        );
        assert_eq!(
            status.ready,
            Some(0),
            "ready must be 0 after successPolicy — the still-Running pod is terminating"
        );
        assert_eq!(status.terminating, Some(0), "terminating must be 0");
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_count() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-count-job", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "succeededCount": 2
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/sp-count-job";
        storage.create(job_key, &job).await.unwrap();

        // 2 indexes succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-count-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Succeeded,
            "sp-count-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        // Others still pending
        let pod2 = make_indexed_pod(
            "pod-2",
            "default",
            Phase::Pending,
            "sp-count-job",
            "job-uid-1",
            2,
        );
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        reconcile_settled(&controller, &storage, job_key).await;

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True"),
            "Job should be Complete via succeededCount successPolicy"
        );
    }

    #[tokio::test]
    async fn test_success_policy_not_met_yet() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-notyet", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "succeededIndexes": "0-2"
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/sp-notyet";
        storage.create(job_key, &job).await.unwrap();

        // Only index 0 succeeded — need 0, 1, 2
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-notyet",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Running,
            "sp-notyet",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // Should NOT be complete yet
        let is_complete = status
            .conditions
            .as_ref()
            .map(|c| {
                c.iter()
                    .any(|cond| cond.condition_type == "Complete" && cond.status == "True")
            })
            .unwrap_or(false);
        assert!(
            !is_complete,
            "Job should NOT be complete when not all required indexes succeeded"
        );
    }

    #[tokio::test]
    async fn test_pod_failure_policy_ignore_action() {
        let storage = Arc::new(MemoryStorage::new());

        // Create an indexed job with backoffLimit=0 and a pod failure policy
        // that ignores pods with DisruptionTarget condition
        let mut job = make_job("ignore-job", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit = Some(0); // Would fail immediately if the pod is counted
        job.spec.pod_failure_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "action": "Ignore",
                        "onPodConditions": [
                            {
                                "type": "DisruptionTarget",
                                "status": "True"
                            }
                        ]
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/ignore-job";
        storage.create(job_key, &job).await.unwrap();

        // Create a failed pod with a DisruptionTarget condition — should be ignored
        let mut pod0 = make_indexed_pod(
            "pod-0-fail",
            "default",
            Phase::Failed,
            "ignore-job",
            "job-uid-1",
            0,
        );
        if let Some(ref mut status) = pod0.status {
            status.conditions = Some(vec![PodCondition {
                condition_type: "DisruptionTarget".to_string(),
                status: "True".to_string(),
                reason: Some("EvictionByEvictionAPI".to_string()),
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]);
        }
        storage
            .create("/registry/pods/default/pod-0-fail", &pod0)
            .await
            .unwrap();

        // Index 1 running
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Running,
            "ignore-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // The job should NOT have a Failed condition — the ignored pod shouldn't
        // trigger backoff limit exceeded
        let has_failed = status
            .conditions
            .as_ref()
            .map(|c| {
                c.iter()
                    .any(|cond| cond.condition_type == "Failed" && cond.status == "True")
            })
            .unwrap_or(false);
        assert!(
            !has_failed,
            "Job should NOT be Failed — the pod with DisruptionTarget should be ignored"
        );

        // status.failed should not count the ignored pod
        assert_eq!(
            status.failed,
            Some(0),
            "Ignored pod should not be counted in status.failed"
        );
    }

    #[tokio::test]
    async fn test_adopt_matching_orphans() {
        let storage = Arc::new(MemoryStorage::new());

        // Create a job with a selector that matches controller-uid
        let mut job = make_job("adopt-job", "default", 2, 2);
        let job_uid = "adopt-uid-123";
        job.metadata.uid = job_uid.to_string();
        // Set up a selector with controller-uid (like the API server auto-generates)
        let mut match_labels = HashMap::new();
        match_labels.insert("controller-uid".to_string(), job_uid.to_string());
        job.spec.selector = Some(rusternetes_common::types::LabelSelector {
            match_labels: Some(match_labels),
            match_expressions: None,
        });
        // Also set template labels to include controller-uid
        job.spec.template.metadata = Some(ObjectMeta {
            labels: Some({
                let mut m = HashMap::new();
                m.insert("controller-uid".to_string(), job_uid.to_string());
                m.insert("job-name".to_string(), "adopt-job".to_string());
                m
            }),
            ..Default::default()
        });

        let job_key = "/registry/jobs/default/adopt-job";
        storage.create(job_key, &job).await.unwrap();

        // Create an orphan pod that has matching labels but NO ownerReference
        let orphan_pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "orphan-pod-1".to_string(),
                namespace: Some("default".to_string()),
                uid: "orphan-uid-1".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("controller-uid".to_string(), job_uid.to_string());
                    m.insert("job-name".to_string(), "adopt-job".to_string());
                    m
                }),
                owner_references: None, // No ownerReference — orphan
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Succeeded),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/orphan-pod-1", &orphan_pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // The orphan pod should now have an ownerReference to the job
        let updated_pod: Pod = storage
            .get("/registry/pods/default/orphan-pod-1")
            .await
            .unwrap();
        let has_owner_ref = updated_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.uid == job_uid && r.kind == "Job"))
            .unwrap_or(false);
        assert!(
            has_owner_ref,
            "Orphan pod should be adopted with ownerReference pointing to the job"
        );
    }

    #[tokio::test]
    async fn test_release_non_matching_pods() {
        let storage = Arc::new(MemoryStorage::new());

        // Create a job with a selector
        let mut job = make_job("release-job", "default", 2, 2);
        let job_uid = "release-uid-456";
        job.metadata.uid = job_uid.to_string();
        let mut match_labels = HashMap::new();
        match_labels.insert("controller-uid".to_string(), job_uid.to_string());
        job.spec.selector = Some(rusternetes_common::types::LabelSelector {
            match_labels: Some(match_labels),
            match_expressions: None,
        });

        let job_key = "/registry/jobs/default/release-job";
        storage.create(job_key, &job).await.unwrap();

        // Create a pod that has an ownerReference to this job but WRONG labels
        let non_matching_pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "wrong-label-pod".to_string(),
                namespace: Some("default".to_string()),
                uid: "wrong-uid-1".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("controller-uid".to_string(), "different-uid".to_string());
                    m.insert("job-name".to_string(), "release-job".to_string());
                    m
                }),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: "release-job".to_string(),
                    uid: job_uid.to_string(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/wrong-label-pod", &non_matching_pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // The non-matching pod should have had its ownerReference removed (released)
        let updated_pod: Pod = storage
            .get("/registry/pods/default/wrong-label-pod")
            .await
            .unwrap();
        let still_owned = updated_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.uid == job_uid))
            .unwrap_or(false);
        assert!(
            !still_owned,
            "Pod with non-matching labels should be released (ownerReference removed)"
        );
    }

    #[tokio::test]
    async fn test_do_not_adopt_pods_owned_by_another_controller() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("adopt-job2", "default", 2, 2);
        let job_uid = "adopt-uid-789";
        job.metadata.uid = job_uid.to_string();
        let mut match_labels = HashMap::new();
        match_labels.insert("controller-uid".to_string(), job_uid.to_string());
        job.spec.selector = Some(rusternetes_common::types::LabelSelector {
            match_labels: Some(match_labels),
            match_expressions: None,
        });

        let job_key = "/registry/jobs/default/adopt-job2";
        storage.create(job_key, &job).await.unwrap();

        // Create a pod with matching labels but already owned by ANOTHER controller
        let owned_pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "other-owned-pod".to_string(),
                namespace: Some("default".to_string()),
                uid: "other-uid-1".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("controller-uid".to_string(), job_uid.to_string());
                    m.insert("job-name".to_string(), "adopt-job2".to_string());
                    m
                }),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: "other-job".to_string(),
                    uid: "other-job-uid".to_string(),
                    controller: Some(true), // Already owned by another controller
                    block_owner_deletion: Some(true),
                }]),
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/other-owned-pod", &owned_pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // The pod should NOT be adopted — it's owned by another controller
        let updated_pod: Pod = storage
            .get("/registry/pods/default/other-owned-pod")
            .await
            .unwrap();
        let adopted_by_us = updated_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.uid == job_uid))
            .unwrap_or(false);
        assert!(
            !adopted_by_us,
            "Pod owned by another controller should NOT be adopted"
        );
    }

    #[tokio::test]
    async fn test_auto_selector_adoption_without_explicit_selector() {
        let storage = Arc::new(MemoryStorage::new());

        // Job WITHOUT an explicit selector (backwards compat — old-style)
        let mut job = make_job("legacy-job", "default", 1, 1);
        job.metadata.uid = "legacy-uid".to_string();
        // No selector set — relies on job-name label

        let job_key = "/registry/jobs/default/legacy-job";
        storage.create(job_key, &job).await.unwrap();

        // Create orphan pod with just job-name label, no ownerRef
        let orphan = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "legacy-orphan".to_string(),
                namespace: Some("default".to_string()),
                uid: "legacy-orphan-uid".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("job-name".to_string(), "legacy-job".to_string());
                    m
                }),
                owner_references: None,
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Succeeded),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/legacy-orphan", &orphan)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // The orphan should be adopted via job-name label fallback
        let updated_pod: Pod = storage
            .get("/registry/pods/default/legacy-orphan")
            .await
            .unwrap();
        let adopted = updated_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter()
                    .any(|r| r.uid == "legacy-uid" && r.kind == "Job")
            })
            .unwrap_or(false);
        assert!(
            adopted,
            "Orphan pod should be adopted via job-name label fallback"
        );
    }

    #[tokio::test]
    async fn test_success_policy_all_indexes_succeeded() {
        // Test 47: "with successPolicy should succeeded when all indexes succeeded"
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-all-job", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "succeededIndexes": "0-2"
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/sp-all-job";
        storage.create(job_key, &job).await.unwrap();

        // All indexes succeed
        for i in 0..3 {
            let pod = make_indexed_pod(
                &format!("pod-{}", i),
                "default",
                Phase::Succeeded,
                "sp-all-job",
                "job-uid-1",
                i,
            );
            storage
                .create(&format!("/registry/pods/default/pod-{}", i), &pod)
                .await
                .unwrap();
        }

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();
        let conditions = status.conditions.as_ref().unwrap();

        // Should have SuccessCriteriaMet condition
        assert!(
            conditions
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("SuccessPolicy")),
            "Should have SuccessCriteriaMet condition with reason SuccessPolicy"
        );

        // Should have Complete condition
        assert!(
            conditions.iter().any(|c| c.condition_type == "Complete"
                && c.status == "True"
                && c.reason.as_deref() == Some("SuccessPolicy")),
            "Should have Complete condition with reason SuccessPolicy"
        );

        assert!(status.completion_time.is_some());
        assert_eq!(status.succeeded, Some(3));
        // completedIndexes should list all three
        assert_eq!(status.completed_indexes.as_deref(), Some("0-2"));
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_count_rule() {
        // Test 48: "with successPolicy succeededCount rule"
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-count2", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "succeededCount": 3
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/sp-count2";
        storage.create(job_key, &job).await.unwrap();

        // 3 indexes succeed, 2 still running
        for i in 0..3 {
            let pod = make_indexed_pod(
                &format!("pod-{}", i),
                "default",
                Phase::Succeeded,
                "sp-count2",
                "job-uid-1",
                i,
            );
            storage
                .create(&format!("/registry/pods/default/pod-{}", i), &pod)
                .await
                .unwrap();
        }
        for i in 3..5 {
            let pod = make_indexed_pod(
                &format!("pod-{}", i),
                "default",
                Phase::Running,
                "sp-count2",
                "job-uid-1",
                i,
            );
            storage
                .create(&format!("/registry/pods/default/pod-{}", i), &pod)
                .await
                .unwrap();
        }

        let controller = JobController::new(storage.clone());
        reconcile_settled(&controller, &storage, job_key).await;

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();
        let conditions = status.conditions.as_ref().unwrap();

        assert!(
            conditions
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("SuccessPolicy")),
            "SuccessCriteriaMet should be set when succeededCount rule met"
        );
        assert!(
            conditions
                .iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True"),
            "Complete condition should be set"
        );
        // K8s sets terminating to 0 when the job completes, even if pods
        // are still being cleaned up. The job status reflects the final state.
        assert_eq!(status.terminating, Some(0));
        assert_eq!(status.active, Some(0));
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_indexes_rule() {
        // Test 49: "with successPolicy succeededIndexes rule"
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-idx-rule", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        // Only indexes 0 and 4 need to succeed
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [
                    {
                        "succeededIndexes": "0,4"
                    }
                ]
            }))
            .unwrap(),
        );

        let job_key = "/registry/jobs/default/sp-idx-rule";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 and 4 succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-idx-rule",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();
        let pod4 = make_indexed_pod(
            "pod-4",
            "default",
            Phase::Succeeded,
            "sp-idx-rule",
            "job-uid-1",
            4,
        );
        storage
            .create("/registry/pods/default/pod-4", &pod4)
            .await
            .unwrap();

        // Indexes 1-3 still running
        for i in 1..4 {
            let pod = make_indexed_pod(
                &format!("pod-{}", i),
                "default",
                Phase::Running,
                "sp-idx-rule",
                "job-uid-1",
                i,
            );
            storage
                .create(&format!("/registry/pods/default/pod-{}", i), &pod)
                .await
                .unwrap();
        }

        let controller = JobController::new(storage.clone());
        reconcile_settled(&controller, &storage, job_key).await;

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();
        let conditions = status.conditions.as_ref().unwrap();

        assert!(
            conditions
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("SuccessPolicy")),
            "SuccessCriteriaMet should be set when required indexes succeeded"
        );
        assert!(
            conditions.iter().any(|c| c.condition_type == "Complete"
                && c.status == "True"
                && c.reason.as_deref() == Some("SuccessPolicy")),
            "Complete condition should have reason SuccessPolicy"
        );
        // K8s sets terminating to 0 when the job completes
        assert_eq!(status.terminating, Some(0));
        assert_eq!(status.active, Some(0));
        assert_eq!(status.succeeded, Some(2));
        assert!(status.completion_time.is_some());
    }

    /// Test that when a Job completes via successPolicy, the status has
    /// terminating=0 (not the count of pods being terminated).
    /// K8s ref: test/e2e/apps/job.go:596 checks terminating==0
    #[tokio::test]
    async fn test_success_policy_sets_terminating_zero() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-job", "default", 2, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [{"succeededCount": 1}]
            }))
            .unwrap(),
        );
        storage
            .create("/registry/jobs/default/sp-job", &job)
            .await
            .unwrap();

        let job_uid = job.metadata.uid.clone();

        // Create one succeeded pod (index 0)
        let mut pod = make_pod("sp-job-0", "default", Phase::Succeeded, "sp-job", &job_uid);
        pod.metadata.labels.as_mut().unwrap().insert(
            "batch.kubernetes.io/job-completion-index".to_string(),
            "0".to_string(),
        );
        storage
            .create("/registry/pods/default/sp-job-0", &pod)
            .await
            .unwrap();

        // Create one running pod (index 1) that will need termination
        let mut pod1 = make_pod("sp-job-1", "default", Phase::Running, "sp-job", &job_uid);
        pod1.metadata.labels.as_mut().unwrap().insert(
            "batch.kubernetes.io/job-completion-index".to_string(),
            "1".to_string(),
        );
        storage
            .create("/registry/pods/default/sp-job-1", &pod1)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        controller.reconcile_all().await.unwrap();
        reap_terminating(&storage).await;
        controller.reconcile_all().await.unwrap();

        let updated_job: Job = storage.get("/registry/jobs/default/sp-job").await.unwrap();
        let status = updated_job.status.unwrap();

        // Job should be complete via success policy
        assert!(
            status.conditions.as_ref().is_some_and(|c| c
                .iter()
                .any(|cond| cond.condition_type == "SuccessCriteriaMet" && cond.status == "True")),
            "Job should have SuccessCriteriaMet condition"
        );

        // terminating MUST be 0, not the count of pods being terminated
        assert_eq!(
            status.terminating,
            Some(0),
            "terminating should be 0 when job completes via successPolicy"
        );

        // ready should be 0
        assert_eq!(status.ready, Some(0));
    }

    // --- orphan pods through the running controller (conformance "Job should
    // delete a job") --------------------------------------------------------
    //
    // `DeleteResourceAndWaitForGC` deletes the Job with background propagation
    // and waits for its pods to be *gone*. The garbage collector deletes them,
    // but each still carries `batch.kubernetes.io/job-tracking`; with the Job
    // gone, only the Job controller's orphan path removes it. Upstream reaches
    // that path from `deleteJob` -> `enqueueLabelSelector`
    // (`pkg/controller/job/job_controller.go:561-581`) and from the pod event
    // handlers (`addPod` :344-347, `updatePod` :417-420, `deletePod` :458-471),
    // all into `handleSingleOrphanPod` (:736-767). Ours existed but only
    // `reconcile_all` — a test entry point — called it, so in a cluster the
    // pods kept the finalizer forever: "there are 2 pods left".

    fn job_with_selector(name: &str) -> Job {
        let mut job = make_job(name, "default", 4, 2);
        job.spec.selector = Some(rusternetes_common::types::LabelSelector {
            match_labels: Some(HashMap::from([("job-name".to_string(), name.to_string())])),
            match_expressions: None,
        });
        job
    }

    async fn wait_until_released(storage: &MemoryStorage, pods: &[&str]) -> Vec<String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let mut held = Vec::new();
            for name in pods {
                let pod: Pod = storage
                    .get(&build_key("pods", Some("default"), name))
                    .await
                    .unwrap();
                if has_job_tracking_finalizer(&pod) {
                    held.push(name.to_string());
                }
            }
            if held.is_empty() || tokio::time::Instant::now() > deadline {
                return held;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn run_releases_the_pods_of_a_deleted_job() {
        let storage = Arc::new(MemoryStorage::new());
        let job_key = build_key("jobs", Some("default"), "foo");
        storage
            .create(&job_key, &job_with_selector("foo"))
            .await
            .unwrap();
        for name in ["foo-a", "foo-b"] {
            let pod = make_pod(name, "default", Phase::Running, "foo", "job-uid-1");
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }

        let controller = Arc::new(JobController::new(storage.clone()));
        let handle = tokio::spawn(controller.run());
        // Let the controller settle on the live Job before it goes away.
        tokio::time::sleep(Duration::from_millis(300)).await;

        storage.delete(&job_key).await.unwrap();

        let held = wait_until_released(&storage, &["foo-a", "foo-b"]).await;
        handle.abort();
        assert!(
            held.is_empty(),
            "pods of the deleted Job still hold the job-tracking finalizer: {held:?}"
        );
    }

    #[tokio::test]
    async fn run_releases_pods_orphaned_before_it_started() {
        let storage = Arc::new(MemoryStorage::new());
        // The Job is already gone: only its pods remain.
        for name in ["bar-a", "bar-b"] {
            let pod = make_pod(name, "default", Phase::Running, "bar", "job-uid-1");
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }

        let controller = Arc::new(JobController::new(storage.clone()));
        let handle = tokio::spawn(controller.run());

        let held = wait_until_released(&storage, &["bar-a", "bar-b"]).await;
        handle.abort();
        assert!(
            held.is_empty(),
            "orphaned pods still hold the job-tracking finalizer: {held:?}"
        );
    }

    /// A failed pod for `index` that carries the index-failure-count
    /// annotation and finished `finished_secs_ago` seconds ago.
    fn failed_pod_with_count(
        name: &str,
        index: i32,
        failure_count: i32,
        finished_secs_ago: i64,
    ) -> Pod {
        let mut pod = make_indexed_pod(
            name,
            "default",
            Phase::Failed,
            "idx-job",
            "job-uid-1",
            index,
        );
        pod.metadata.annotations.as_mut().unwrap().insert(
            "batch.kubernetes.io/job-index-failure-count".to_string(),
            failure_count.to_string(),
        );
        let finished = chrono::Utc::now() - chrono::Duration::seconds(finished_secs_ago);
        pod.status.as_mut().unwrap().container_statuses = Some(vec![ContainerStatus {
            name: "test".to_string(),
            ready: false,
            restart_count: 0,
            state: Some(ContainerState {
                terminated: Some(ContainerStateTerminated {
                    exit_code: 1,
                    signal: None,
                    reason: Some("Error".to_string()),
                    message: None,
                    started_at: None,
                    finished_at: Some(finished.to_rfc3339()),
                    container_id: None,
                }),
                ..Default::default()
            }),
            last_state: None,
            image: Some("busybox".to_string()),
            image_id: None,
            container_id: None,
            started: None,
            allocated_resources: None,
            resources: None,
            volume_mounts: None,
            user: None,
            allocated_resources_status: None,
            stop_signal: None,
        }]);
        pod
    }

    async fn per_index_job(storage: &Arc<MemoryStorage>, limit: i32) -> Job {
        let mut job = make_job("idx-job", "default", 2, 2);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit_per_index = Some(limit);
        job.spec.backoff_limit = Some(100);
        storage
            .create("/registry/jobs/default/idx-job", &job)
            .await
            .unwrap();
        job
    }

    /// Port of `getPodsWithDelayedDeletionPerIndex`
    /// (`indexed_job_utils.go:323`) and `addIndexFailureCountAnnotation`
    /// (`indexed_job_utils.go:350`): the replacement for an index whose failed
    /// pod is the only record of its failure count must inherit that count,
    /// and the failed pod must keep its finalizer until the replacement exists.
    #[tokio::test]
    async fn test_delayed_deletion_per_index_carries_failure_count() {
        let storage = Arc::new(MemoryStorage::new());
        let job = per_index_job(&storage, 5).await;
        // Index 0 has failed twice already; this pod is the last of them and
        // finished long ago, so no backoff is pending. Index 1 is running.
        let failed = failed_pod_with_count("idx-0-failed", 0, 1, 3600);
        storage
            .create("/registry/pods/default/idx-0-failed", &failed)
            .await
            .unwrap();
        let running = make_indexed_pod(
            "idx-1-run",
            "default",
            Phase::Running,
            "idx-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/idx-1-run", &running)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job = job;
        controller.reconcile(&mut job).await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let old = pods
            .iter()
            .find(|p| p.metadata.name == "idx-0-failed")
            .unwrap();
        assert!(
            has_job_tracking_finalizer(old),
            "the last failed pod of an index keeps its finalizer until the replacement exists"
        );
        let replacement = pods
            .iter()
            .find(|p| p.metadata.name != "idx-0-failed" && get_pod_index(p) == Some(0))
            .expect("a replacement pod for index 0 must be created");
        assert_eq!(
            replacement
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("batch.kubernetes.io/job-index-failure-count"))
                .map(String::as_str),
            Some("2"),
            "the replacement inherits the failure count plus one"
        );
    }

    /// Once the replacement is active the failed pod is no longer delayed and
    /// its finalizer comes off (`canRemoveFinalizer`, `job_controller.go:1359`).
    #[tokio::test]
    async fn test_delayed_deletion_released_once_replacement_active() {
        let storage = Arc::new(MemoryStorage::new());
        let job = per_index_job(&storage, 5).await;
        let failed = failed_pod_with_count("idx-0-failed", 0, 1, 3600);
        storage
            .create("/registry/pods/default/idx-0-failed", &failed)
            .await
            .unwrap();
        for (n, i) in [("idx-0-new", 0), ("idx-1-run", 1)] {
            let p = make_indexed_pod(n, "default", Phase::Running, "idx-job", "job-uid-1", i);
            storage
                .create(&format!("/registry/pods/default/{n}"), &p)
                .await
                .unwrap();
        }
        let controller = JobController::new(storage.clone());
        let mut job = job;
        controller.reconcile(&mut job).await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        // Released: either gone or without the tracking finalizer.
        let held = pods
            .iter()
            .any(|p| p.metadata.name == "idx-0-failed" && has_job_tracking_finalizer(p));
        assert!(
            !held,
            "failed pod must be released once its index is active"
        );
    }

    /// Port of `getPodCreationInfoForIndependentIndexes` /
    /// `getRemainingTimePerIndex` (`job_controller.go:1850`,
    /// `backoff_utils.go:248`): an index that failed just now is not retried
    /// until DefaultJobPodFailureBackOff * 2^(count) has elapsed.
    #[tokio::test]
    async fn test_per_index_failure_backoff_defers_replacement() {
        let storage = Arc::new(MemoryStorage::new());
        let job = per_index_job(&storage, 5).await;
        let failed = failed_pod_with_count("idx-0-failed", 0, 0, 0);
        storage
            .create("/registry/pods/default/idx-0-failed", &failed)
            .await
            .unwrap();
        let running = make_indexed_pod(
            "idx-1-run",
            "default",
            Phase::Running,
            "idx-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/idx-1-run", &running)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job = job;
        controller.reconcile(&mut job).await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(
            !pods
                .iter()
                .any(|p| p.metadata.name != "idx-0-failed" && get_pod_index(p) == Some(0)),
            "no replacement may be created inside the per-index backoff window"
        );
    }

    #[test]
    fn test_remaining_time_per_index_doubles_up_to_max() {
        let now = chrono::Utc::now();
        let p = failed_pod_with_count("p", 0, 0, 0);
        // absolute failure count 0 + 1 -> 10s window.
        let r = remaining_time_per_index(now, Some(&p));
        // The finish time is stored at whole-second precision, so up to a
        // second of the window is already gone; allow for a slow runner too.
        assert!(
            r > Duration::from_secs(7) && r <= Duration::from_secs(10),
            "{r:?}"
        );
        let p = failed_pod_with_count("p", 0, 2, 0);
        // count 3 -> 10s * 2^2 = 40s.
        let r = remaining_time_per_index(now, Some(&p));
        assert!(
            r > Duration::from_secs(38) && r <= Duration::from_secs(40),
            "{r:?}"
        );
        let p = failed_pod_with_count("p", 0, 30, 0);
        let r = remaining_time_per_index(now, Some(&p));
        assert!(
            r <= Duration::from_secs(600) && r > Duration::from_secs(598),
            "{r:?}"
        );
        assert_eq!(remaining_time_per_index(now, None), Duration::ZERO);
    }

    fn terminated_status(
        name: &str,
        finished: Option<chrono::DateTime<chrono::Utc>>,
    ) -> ContainerStatus {
        let mut cs = failed_pod_with_count("tmp", 0, 0, 0)
            .status
            .unwrap()
            .container_statuses
            .unwrap()
            .remove(0);
        cs.name = name.to_string();
        cs.state = Some(ContainerState {
            terminated: Some(ContainerStateTerminated {
                exit_code: 0,
                signal: None,
                reason: None,
                message: None,
                started_at: None,
                finished_at: finished.map(|t| t.to_rfc3339()),
                container_id: None,
            }),
            ..Default::default()
        });
        cs
    }

    /// `getFinishTimeFromContainers` (`backoff_utils.go:188`): a restartable
    /// init container (sidecar) finishes after the regular containers, so its
    /// finish time wins.
    #[test]
    fn test_pod_finished_time_includes_sidecar_init_containers() {
        let now = chrono::Utc::now();
        let main_done = now - chrono::Duration::seconds(100);
        let sidecar_done = now - chrono::Duration::seconds(30);
        let mut pod = failed_pod_with_count("p", 0, 0, 0);
        pod.spec.as_mut().unwrap().init_containers = Some(vec![
            Container {
                name: "sidecar".to_string(),
                restart_policy: Some("Always".to_string()),
                ..Default::default()
            },
            Container {
                name: "plain-init".to_string(),
                ..Default::default()
            },
        ]);
        let status = pod.status.as_mut().unwrap();
        status.container_statuses = Some(vec![terminated_status("test", Some(main_done))]);
        // The plain init container finished latest of all but is not a sidecar.
        status.init_container_statuses = Some(vec![
            terminated_status("sidecar", Some(sidecar_done)),
            terminated_status("plain-init", Some(now)),
        ]);
        let got = pod_finished_time(&pod);
        assert_eq!(got.timestamp(), sidecar_done.timestamp());
    }

    /// `latestFinishTime` (`backoff_utils.go:213`): one sidecar that has not
    /// terminated (or has a zero finish time) makes the container lookup yield
    /// nothing, so the Ready=False transition is used instead.
    #[test]
    fn test_pod_finished_time_unfinished_sidecar_falls_back_to_ready_false() {
        let now = chrono::Utc::now();
        let ready_false = now - chrono::Duration::seconds(7);
        let mut pod = failed_pod_with_count("p", 0, 0, 50);
        pod.spec.as_mut().unwrap().init_containers = Some(vec![Container {
            name: "sidecar".to_string(),
            restart_policy: Some("Always".to_string()),
            ..Default::default()
        }]);
        let status = pod.status.as_mut().unwrap();
        let mut running = terminated_status("sidecar", None);
        running.state = Some(ContainerState {
            running: Some(ContainerStateRunning { started_at: None }),
            ..Default::default()
        });
        status.init_container_statuses = Some(vec![running]);
        status.conditions = Some(vec![PodCondition {
            condition_type: "Ready".to_string(),
            status: "False".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(ready_false),
            observed_generation: None,
        }]);
        assert_eq!(pod_finished_time(&pod).timestamp(), ready_false.timestamp());
    }

    /// `getFinishTimeFromDeletionTimestamp` (`backoff_utils.go:231`):
    /// deletionTimestamp minus the grace period.
    #[test]
    fn test_pod_finished_time_deletion_timestamp_minus_grace() {
        let now = chrono::Utc::now();
        let mut pod = failed_pod_with_count("p", 0, 0, 0);
        pod.status.as_mut().unwrap().container_statuses = None;
        pod.metadata.deletion_timestamp = Some(now);
        pod.metadata.deletion_grace_period_seconds = Some(30);
        let got = pod_finished_time(&pod);
        assert_eq!(
            got.timestamp(),
            (now - chrono::Duration::seconds(30)).timestamp()
        );
    }

    /// `getPodCreationInfoForIndependentIndexes` + `enqueueSyncJobWithDelay`
    /// (`job_controller.go:1746-1749`): when every pending index is inside its
    /// backoff window the sync asks to be re-run after the remaining time
    /// instead of waiting for the resync.
    #[tokio::test]
    async fn test_per_index_backoff_requests_requeue_after_remaining_time() {
        let storage = Arc::new(MemoryStorage::new());
        let job = per_index_job(&storage, 5).await;
        let failed = failed_pod_with_count("idx-0-failed", 0, 0, 0);
        storage
            .create("/registry/pods/default/idx-0-failed", &failed)
            .await
            .unwrap();
        let running = make_indexed_pod(
            "idx-1-run",
            "default",
            Phase::Running,
            "idx-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/idx-1-run", &running)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job = job;
        controller.reconcile(&mut job).await.unwrap();
        let d = controller
            .take_requeue_delay("default", "idx-job")
            .expect("a pending per-index backoff must request a delayed requeue");
        // The pod finished just now (`finished_secs_ago` = 0) and the
        // first-failure backoff is 10s, so the remaining time is <= 10s
        // (observed: 9.765s against a `<= 9s` bound). The lower bound is loose on
        // purpose: this runs on the wall clock and a loaded CI runner can
        // stall for seconds between building the pod and the sync (observed:
        // 7.999s against a `> 8s` bound).
        assert!(
            d > Duration::from_secs(1) && d <= Duration::from_secs(10),
            "{d:?}"
        );
        assert!(controller
            .take_requeue_delay("default", "idx-job")
            .is_none());
    }

    /// A non-indexed failed pod of `job-uid-1` that finished
    /// `finished_secs_ago` seconds ago (and still holds the tracking finalizer).
    fn failed_plain_pod(name: &str, finished_secs_ago: i64) -> Pod {
        let mut pod = failed_pod_with_count(name, 0, 0, finished_secs_ago);
        pod.metadata.annotations = None;
        pod.metadata.labels = Some(HashMap::from([(
            "job-name".to_string(),
            "plain-job".to_string(),
        )]));
        pod.metadata.owner_references.as_mut().unwrap()[0].name = "plain-job".to_string();
        pod
    }

    async fn plain_job_with_failed_pod(
        storage: &Arc<MemoryStorage>,
        finished_secs_ago: i64,
    ) -> Job {
        let job = make_job("plain-job", "default", 1, 1);
        storage
            .create("/registry/jobs/default/plain-job", &job)
            .await
            .unwrap();
        let failed = failed_plain_pod("failed-0", finished_secs_ago);
        storage
            .create("/registry/pods/default/failed-0", &failed)
            .await
            .unwrap();
        job
    }

    async fn plain_job_pod_names(storage: &Arc<MemoryStorage>) -> Vec<String> {
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        pods.into_iter().map(|p| p.metadata.name).collect()
    }

    /// `manageJob` (`job_controller.go:1731-1738`): a replacement pod is not
    /// created while `newBackoffRecord.getRemainingTime(DefaultJobPodFailureBackOff,
    /// MaxJobPodFailureBackOff)` is positive; the sync is re-enqueued with that
    /// delay instead.
    #[tokio::test]
    async fn test_replacement_pod_delayed_by_failure_backoff() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = plain_job_with_failed_pod(&storage, 1).await;
        let controller = JobController::new(storage.clone());
        controller.reconcile(&mut job).await.unwrap();
        assert_eq!(
            plain_job_pod_names(&storage).await,
            vec!["failed-0".to_string()],
            "no replacement may be created inside the failure backoff window"
        );
        let d = controller
            .take_requeue_delay("default", "plain-job")
            .expect("the delayed creation must request a delayed requeue");
        // The pod finished 1s ago and the finish time has whole-second
        // precision, so 1-2s of the 10s window are already gone.
        assert!(
            d > Duration::from_secs(7) && d <= Duration::from_secs(10),
            "{d:?}"
        );
    }

    /// Once the window has elapsed the replacement is created at once.
    #[tokio::test]
    async fn test_replacement_pod_created_after_failure_backoff_elapsed() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = plain_job_with_failed_pod(&storage, 60).await;
        let controller = JobController::new(storage.clone());
        controller.reconcile(&mut job).await.unwrap();
        assert_eq!(plain_job_pod_names(&storage).await.len(), 2);
    }

    /// `podBackoffStore` (`backoff_utils.go:44-91`) outlives a sync: after the
    /// failed pod's finalizer is released it is no longer a "new" failed pod,
    /// yet the next sync must still be inside the backoff window.
    #[tokio::test]
    async fn test_failure_backoff_survives_across_syncs() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = plain_job_with_failed_pod(&storage, 1).await;
        let controller = JobController::new(storage.clone());
        controller.reconcile(&mut job).await.unwrap();
        let _ = controller.take_requeue_delay("default", "plain-job");
        let mut job: Job = storage
            .get("/registry/jobs/default/plain-job")
            .await
            .unwrap();
        controller.reconcile(&mut job).await.unwrap();
        assert_eq!(
            plain_job_pod_names(&storage).await,
            vec!["failed-0".to_string()],
            "the second sync must still honour the recorded failure"
        );
        assert!(controller
            .take_requeue_delay("default", "plain-job")
            .is_some());
    }

    /// Port of `TestGetRemainingBackoffTime` (`backoff_utils_test.go:404`).
    #[test]
    fn test_get_remaining_backoff_time() {
        use chrono::TimeZone;
        let t0 = chrono::Utc
            .with_ymd_and_hms(2009, 11, 10, 23, 0, 0)
            .unwrap();
        let secs = chrono::Duration::seconds;
        let d = Duration::from_secs;
        // (name, failures, lastFailureTime set, now offset, want)
        let cases: Vec<(&str, i32, bool, i64, u64)> = vec![
            ("no failures", 0, false, 0, 0),
            ("one failure; same time", 1, true, 0, 5),
            ("one failure; +1s", 1, true, 1, 4),
            ("one failure; +5s", 1, true, 5, 0),
            ("one failure; +6s", 1, true, 6, 0),
            ("three failures; same time", 3, true, 0, 20),
            ("eight failures; below max", 8, true, 0, 640),
            ("nine failures; capped at max", 9, true, 0, 700),
        ];
        for (name, failures, has_time, offset, want) in cases {
            let record = BackoffRecord {
                failures_after_last_success: failures,
                last_failure_time: has_time.then_some(t0),
            };
            let got = record.remaining_time(t0 + secs(offset), d(5), d(700));
            assert_eq!(got, d(want), "{name}");
        }
    }

    fn pod_finished_at(t: chrono::DateTime<chrono::Utc>) -> Pod {
        let mut pod = failed_plain_pod("p", 0);
        pod.status.as_mut().unwrap().container_statuses =
            Some(vec![terminated_status("test", Some(t))]);
        pod
    }

    /// Port of `TestNewBackoffRecord` (`backoff_utils_test.go:31`).
    #[test]
    fn test_new_backoff_record() {
        use chrono::TimeZone;
        let t0 = chrono::Utc
            .with_ymd_and_hms(2009, 11, 10, 23, 0, 0)
            .unwrap();
        let ms = chrono::Duration::milliseconds;
        // (name, stored failures, succeeded offsets, failed offsets,
        //  want failures, want lastFailureTime offset)
        type Case = (
            &'static str,
            Option<i32>,
            Vec<i64>,
            Vec<i64>,
            i32,
            Option<i64>,
        );
        let cases: Vec<Case> = vec![
            ("one new failure", None, vec![], vec![0], 1, Some(0)),
            ("two new failures", None, vec![], vec![0, -1], 2, Some(0)),
            (
                "two failures then success",
                None,
                vec![0],
                vec![-2, -1],
                0,
                None,
            ),
            (
                "two failures, success, two more failures",
                None,
                vec![-2],
                vec![0, -4, -3, -1],
                2,
                Some(0),
            ),
            (
                "stored count 2 plus one failure",
                Some(2),
                vec![],
                vec![0],
                3,
                Some(0),
            ),
            (
                "success and failure at same timestamp",
                None,
                vec![0],
                vec![0],
                0,
                None,
            ),
            ("nothing new", None, vec![], vec![], 0, None),
            ("one success", None, vec![0], vec![], 0, None),
        ];
        for (name, stored, succeeded, failed, want_failures, want_time) in cases {
            let store = BackoffStore::default();
            if let Some(n) = stored {
                store.update(
                    "key",
                    &BackoffRecord {
                        failures_after_last_success: n,
                        last_failure_time: None,
                    },
                );
            }
            let succ: Vec<Pod> = succeeded
                .iter()
                .map(|o| pod_finished_at(t0 + ms(*o)))
                .collect();
            let fail: Vec<Pod> = failed
                .iter()
                .map(|o| pod_finished_at(t0 + ms(*o)))
                .collect();
            let succ: Vec<&Pod> = succ.iter().collect();
            let fail: Vec<&Pod> = fail.iter().collect();
            let got = store.new_backoff_record("key", &succ, &fail);
            assert_eq!(got.failures_after_last_success, want_failures, "{name}");
            assert_eq!(
                got.last_failure_time.map(|t| (t - t0).num_milliseconds()),
                want_time,
                "{name}"
            );
        }
    }

    /// `removeBackoffRecord` (`backoff_utils.go:64`) forgets the Job's history.
    #[tokio::test]
    async fn test_backoff_record_removed_when_job_deleted() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = plain_job_with_failed_pod(&storage, 1).await;
        let controller = JobController::new(storage.clone());
        controller.reconcile(&mut job).await.unwrap();
        assert!(controller
            .backoff_store
            .records
            .lock()
            .unwrap()
            .contains_key("default/plain-job"));
        job.metadata.deletion_timestamp = Some(chrono::Utc::now());
        controller.reconcile(&mut job).await.unwrap();
        assert!(controller.backoff_store.records.lock().unwrap().is_empty());
    }

    /// `enqueueSyncJobWithDelay` never delays less than `SyncJobBatchPeriod`
    /// (`job_controller.go:620`).
    #[test]
    fn test_requeue_delay_floor_is_sync_job_batch_period() {
        assert_eq!(
            requeue_delay_for(Duration::from_millis(10)),
            SYNC_JOB_BATCH_PERIOD
        );
        assert_eq!(
            requeue_delay_for(Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }
    /// `MemoryStorage` that can refuse Pod updates by name and records every
    /// Job write, so a test can stage a PARTIAL `uncountedTerminatedPods` drain
    /// (upstream's "pod patch errors with partial success" case) and inspect
    /// the sequence of statuses that went out.
    struct PartialDrainStorage {
        inner: MemoryStorage,
        fail_pod_updates: std::sync::Mutex<HashSet<String>>,
        job_writes: std::sync::Mutex<Vec<JobStatus>>,
    }

    impl PartialDrainStorage {
        fn new() -> Self {
            Self {
                inner: MemoryStorage::new(),
                fail_pod_updates: Default::default(),
                job_writes: Default::default(),
            }
        }
        fn record(&self, key: &str, value: &serde_json::Value) {
            if key.starts_with("/registry/jobs/") {
                if let Some(st) = value
                    .get("status")
                    .and_then(|s| serde_json::from_value::<JobStatus>(s.clone()).ok())
                {
                    self.job_writes.lock().unwrap().push(st);
                }
            }
        }
        fn refuse(&self, key: &str) -> bool {
            self.fail_pod_updates
                .lock()
                .unwrap()
                .iter()
                .any(|n| key.ends_with(&format!("/pods/default/{n}")))
        }
    }

    type CResult<T> = std::result::Result<T, rusternetes_common::Error>;

    #[async_trait::async_trait]
    impl Storage for PartialDrainStorage {
        async fn create<T>(&self, key: &str, value: &T) -> CResult<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.create(key, value).await
        }
        async fn get<T>(&self, key: &str) -> CResult<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.get(key).await
        }
        async fn update<T>(&self, key: &str, value: &T) -> CResult<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            if self.refuse(key) {
                return Err(rusternetes_common::Error::Internal(
                    "injected pod update failure".into(),
                ));
            }
            self.record(key, &serde_json::to_value(value).unwrap());
            self.inner.update(key, value).await
        }
        async fn update_status<T>(&self, key: &str, value: &T) -> CResult<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.record(key, &serde_json::to_value(value).unwrap());
            self.inner.update_status(key, value).await
        }
        async fn delete(&self, key: &str) -> CResult<()> {
            self.inner.delete(key).await
        }
        async fn list<T>(&self, prefix: &str) -> CResult<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.list(prefix).await
        }
        async fn watch(&self, prefix: &str) -> CResult<rusternetes_storage::WatchStream> {
            self.inner.watch(prefix).await
        }
        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> CResult<rusternetes_storage::WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }
        async fn current_revision(&self) -> CResult<i64> {
            self.inner.current_revision().await
        }
        async fn is_revision_compacted(&self, revision: i64) -> CResult<bool> {
            self.inner.is_revision_compacted(revision).await
        }
        async fn update_raw(&self, key: &str, value: &serde_json::Value) -> CResult<()> {
            self.inner.update_raw(key, value).await
        }
    }

    fn uncounted_len_of(st: &JobStatus) -> usize {
        st.uncounted_terminated_pods.as_ref().map_or(0, |u| {
            u.succeeded.as_ref().map_or(0, |v| v.len()) + u.failed.as_ref().map_or(0, |v| v.len())
        })
    }

    fn has_terminal(st: &JobStatus) -> bool {
        st.conditions.as_ref().is_some_and(|c| {
            c.iter().any(|c| {
                (c.condition_type == "Complete" || c.condition_type == "Failed")
                    && c.status == "True"
            })
        })
    }

    /// #2378 (follow-up to #2377): only part of `uncountedTerminatedPods`
    /// drains in one sync (one pod's finalizer removal fails). Upstream's
    /// `enactJobFinished` (`job_controller.go:1509-1519`) returns false while
    /// any UID remains, and the next sync re-derives `finishedCondition` from
    /// scratch and finishes the Job. So: no write may carry a terminal
    /// condition while UIDs remain, and the terminal condition lands once.
    #[tokio::test]
    async fn partial_uncounted_drain_defers_terminal_condition_to_next_sync() {
        let storage = Arc::new(PartialDrainStorage::new());
        let job = make_job("part", "default", 2, 2);
        let job_key = build_key("jobs", Some("default"), "part");
        storage.create(&job_key, &job).await.unwrap();
        for name in ["part-1", "part-2"] {
            let pod = make_pod(name, "default", Phase::Succeeded, "part", "job-uid-1");
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }
        storage
            .fail_pod_updates
            .lock()
            .unwrap()
            .insert("part-2".into());

        let controller = JobController::new(storage.clone());
        controller.reconcile_all().await.unwrap();

        let after_first: Job = storage.get(&job_key).await.unwrap();
        let st = after_first.status.clone().unwrap();
        assert_eq!(
            uncounted_len_of(&st),
            1,
            "the pod whose finalizer could not be removed stays uncounted: {st:?}"
        );
        assert!(
            !has_terminal(&st),
            "no Complete/Failed while UIDs remain uncounted: {st:?}"
        );
        assert!(
            st.conditions.as_ref().is_some_and(|c| c
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet" && c.status == "True")),
            "the interim SuccessCriteriaMet condition is written: {st:?}"
        );

        storage.fail_pod_updates.lock().unwrap().clear();
        controller.reconcile_all().await.unwrap();

        let done: Job = storage.get(&job_key).await.unwrap();
        let st = done.status.unwrap();
        assert_eq!(uncounted_len_of(&st), 0, "second sync drains the rest");
        assert_eq!(st.succeeded, Some(2));
        assert!(has_terminal(&st), "second sync re-derives Complete: {st:?}");
        assert!(st.completion_time.is_some());

        let writes = storage.job_writes.lock().unwrap().clone();
        for w in &writes {
            assert!(
                !(has_terminal(w) && uncounted_len_of(w) > 0),
                "a write marked the Job finished while UIDs remained: {w:?}"
            );
        }
        let first_terminal = writes.iter().position(has_terminal);
        let terminal_idx: Vec<usize> = writes
            .iter()
            .enumerate()
            .filter(|(_, w)| has_terminal(w))
            .map(|(i, _)| i)
            .collect();
        assert!(first_terminal.is_some());
        // Once terminal, every later write stays terminal (written once, never
        // flapped off and on).
        assert_eq!(
            terminal_idx,
            (first_terminal.unwrap()..writes.len()).collect::<Vec<_>>(),
            "terminal condition must not flap: {writes:?}"
        );
    }

    /// Same, for the failing path (`FailureTarget` interim, `Failed` final).
    #[tokio::test]
    async fn partial_uncounted_drain_defers_failed_condition_to_next_sync() {
        let storage = Arc::new(PartialDrainStorage::new());
        let mut job = make_job("pfail", "default", 2, 2);
        job.spec.backoff_limit = Some(0);
        let job_key = build_key("jobs", Some("default"), "pfail");
        storage.create(&job_key, &job).await.unwrap();
        for name in ["pfail-1", "pfail-2"] {
            let pod = make_pod(name, "default", Phase::Failed, "pfail", "job-uid-1");
            storage
                .create(&build_key("pods", Some("default"), name), &pod)
                .await
                .unwrap();
        }
        storage
            .fail_pod_updates
            .lock()
            .unwrap()
            .insert("pfail-2".into());

        let controller = JobController::new(storage.clone());
        controller.reconcile_all().await.unwrap();
        let st = storage.get::<Job>(&job_key).await.unwrap().status.unwrap();
        assert_eq!(uncounted_len_of(&st), 1, "{st:?}");
        assert!(!has_terminal(&st), "{st:?}");

        storage.fail_pod_updates.lock().unwrap().clear();
        controller.reconcile_all().await.unwrap();
        let st = storage.get::<Job>(&job_key).await.unwrap().status.unwrap();
        assert_eq!(uncounted_len_of(&st), 0, "{st:?}");
        assert!(has_terminal(&st), "{st:?}");
        for w in storage.job_writes.lock().unwrap().iter() {
            assert!(!(has_terminal(w) && uncounted_len_of(w) > 0), "{w:?}");
        }
    }

    // ---- #2465: deleteActivePods is a graceful DeletePod ----

    /// Replay the kubelet: physically remove pods whose deletionTimestamp the
    /// controller stamped with its graceful delete.
    async fn reap_terminating(storage: &Arc<MemoryStorage>) {
        let pods: Vec<Pod> = storage.list("/registry/pods/").await.unwrap();
        for pod in pods
            .iter()
            .filter(|p| p.metadata.deletion_timestamp.is_some())
        {
            let key = format!(
                "/registry/pods/{}/{}",
                pod.metadata.namespace.as_deref().unwrap_or("default"),
                pod.metadata.name
            );
            let _ = storage.delete(&key).await;
        }
    }

    /// Reconcile, let the kubelet reap the pods the sync deleted gracefully,
    /// reconcile again: the terminal condition is delayed while pods are
    /// terminating (`enactJobFinished`, job_controller.go:1520-1524).
    async fn reconcile_settled(
        controller: &JobController<MemoryStorage>,
        storage: &Arc<MemoryStorage>,
        job_key: &str,
    ) {
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        reap_terminating(storage).await;
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
    }

    /// While a pod is still terminating the Job publishes only the interim
    /// SuccessCriteriaMet condition and a non-zero `terminating`; Complete
    /// follows once the pod is gone (job_controller.go:1520-1524).
    #[tokio::test]
    async fn test_success_policy_complete_is_delayed_while_pods_terminate() {
        let mut job = make_job("dly", "default", 5, 2);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [{ "succeededIndexes": "0" }]
            }))
            .unwrap(),
        );
        let storage = Arc::new(MemoryStorage::new());
        let job_key = "/registry/jobs/default/dly";
        storage.create(job_key, &job).await.unwrap();
        for (n, phase, idx) in [("p0", Phase::Succeeded, 0), ("p2", Phase::Running, 2)] {
            let pod = make_indexed_pod(n, "default", phase, "dly", "job-uid-1", idx);
            storage
                .create(&format!("/registry/pods/default/{n}"), &pod)
                .await
                .unwrap();
        }
        let controller = JobController::new(storage.clone());
        let mut j: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();

        let st = storage.get::<Job>(job_key).await.unwrap().status.unwrap();
        let conds = st.conditions.unwrap_or_default();
        assert!(conds
            .iter()
            .any(|c| c.condition_type == "SuccessCriteriaMet"));
        assert!(
            !conds.iter().any(|c| c.condition_type == "Complete"),
            "Complete must wait for the terminating pod"
        );
        assert_eq!(st.terminating, Some(1));

        reap_terminating(&storage).await;
        let mut j: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();
        let st = storage.get::<Job>(job_key).await.unwrap().status.unwrap();
        assert!(st
            .conditions
            .unwrap_or_default()
            .iter()
            .any(|c| c.condition_type == "Complete"));
        assert_eq!(st.terminating, Some(0));
    }

    /// Create `job` plus one Running pod `p1`; reconcile once; return the
    /// storage and the pod's key.
    async fn graceful_fixture(job: Job) -> (Arc<MemoryStorage>, String) {
        let storage = Arc::new(MemoryStorage::new());
        let job_key = format!("/registry/jobs/default/{}", job.metadata.name);
        storage.create(&job_key, &job).await.unwrap();
        let pod = make_pod(
            "p1",
            "default",
            Phase::Running,
            &job.metadata.name,
            "job-uid-1",
        );
        storage
            .create("/registry/pods/default/p1", &pod)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(&job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        (storage, "/registry/pods/default/p1".to_string())
    }

    /// Suspending deletes active pods through `deleteActivePods` ->
    /// `podControl.DeletePod` (job_controller.go:1122-1140;
    /// controller_utils.go:618): graceful, the pod keeps its finalizer-backed
    /// object with a deletionTimestamp.
    #[tokio::test]
    async fn test_suspend_deletes_active_pods_gracefully() {
        let mut job = make_job("susp", "default", 1, 1);
        job.spec.suspend = Some(true);
        let (storage, pod_key) = graceful_fixture(job).await;
        let pod: Pod = storage
            .get(&pod_key)
            .await
            .expect("pod must still exist: delete is graceful");
        assert!(pod.metadata.deletion_timestamp.is_some());
    }

    #[tokio::test]
    async fn test_active_deadline_deletes_active_pods_gracefully() {
        let mut job = make_job("dl", "default", 1, 1);
        job.spec.active_deadline_seconds = Some(1);
        job.status = Some(JobStatus {
            start_time: Some(chrono::Utc::now() - chrono::Duration::seconds(60)),
            ..Default::default()
        });
        let (storage, pod_key) = graceful_fixture(job).await;
        let pod: Pod = storage
            .get(&pod_key)
            .await
            .expect("pod must still exist: delete is graceful");
        assert!(pod.metadata.deletion_timestamp.is_some());
    }

    #[tokio::test]
    async fn test_success_policy_deletes_active_pods_gracefully() {
        let mut job = make_job("sp", "default", 5, 2);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [{ "succeededIndexes": "0" }]
            }))
            .unwrap(),
        );
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create("/registry/jobs/default/sp", &job)
            .await
            .unwrap();
        let done = make_indexed_pod("p0", "default", Phase::Succeeded, "sp", "job-uid-1", 0);
        storage
            .create("/registry/pods/default/p0", &done)
            .await
            .unwrap();
        let running = make_indexed_pod("p2", "default", Phase::Running, "sp", "job-uid-1", 2);
        storage
            .create("/registry/pods/default/p2", &running)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/sp").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let pod: Pod = storage
            .get("/registry/pods/default/p2")
            .await
            .expect("pod must still exist: delete is graceful");
        assert!(pod.metadata.deletion_timestamp.is_some());
    }

    /// `FilterActivePods` (controller_utils.go:1001, `IsPodActive` :1085)
    /// excludes pods with a deletionTimestamp, so a terminating Running pod
    /// is replaced rather than counted against `parallelism`.
    #[tokio::test]
    async fn test_terminating_pod_is_not_active() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("term", "default", 1, 1);
        storage
            .create("/registry/jobs/default/term", &job)
            .await
            .unwrap();
        let mut pod = make_pod("p1", "default", Phase::Running, "term", "job-uid-1");
        // A pod deleted while Running counts as failed (`isPodFailed`), and its
        // finish time is `deletionTimestamp - grace` (`backoff_utils.go:231`),
        // so the deletion must be older than the 10s failure backoff for the
        // replacement to be created in this sync.
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now() - chrono::Duration::seconds(60));
        storage
            .create("/registry/pods/default/p1", &pod)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/term").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let live = pods
            .iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
            .count();
        assert_eq!(live, 1, "a replacement for the terminating pod is created");
    }

    // ---- #2553: manageJob — excess deletion, cap, expectations ----

    async fn live_pods(storage: &Arc<MemoryStorage>) -> Vec<Pod> {
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        pods.into_iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
            .collect()
    }

    /// `manageJob` (job_controller.go:1692-1716): `rmAtLeast = active -
    /// wantActive`; the excess pods are deleted and, deletion taking
    /// precedence, nothing is created in the same sync.
    #[tokio::test]
    async fn test_manage_job_deletes_excess_active_pods() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("ex", "default", 5, 1);
        storage
            .create("/registry/jobs/default/ex", &job)
            .await
            .unwrap();
        for n in ["p1", "p2", "p3"] {
            let pod = make_pod(n, "default", Phase::Running, "ex", "job-uid-1");
            storage
                .create(&format!("/registry/pods/default/{n}"), &pod)
                .await
                .unwrap();
        }
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/ex").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let all: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(all.len(), 3, "no replacement created in a delete sync");
        assert_eq!(live_pods(&storage).await.len(), 1, "2 excess deleted");
        let st: Job = storage.get("/registry/jobs/default/ex").await.unwrap();
        assert_eq!(st.status.unwrap().active, Some(1));
    }

    /// A pod removed as excess must not later be counted as a failure or
    /// success: `deleteJobPods` strips the tracking finalizer first
    /// (`removeTrackingFinalizerPatch`, job_controller.go:1181-1186).
    #[tokio::test]
    async fn test_delete_job_pods_strips_tracking_finalizer() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("fin", "default", 5, 1);
        storage
            .create("/registry/jobs/default/fin", &job)
            .await
            .unwrap();
        for n in ["p1", "p2"] {
            let pod = make_pod(n, "default", Phase::Running, "fin", "job-uid-1");
            storage
                .create(&format!("/registry/pods/default/{n}"), &pod)
                .await
                .unwrap();
        }
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/fin").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        let all: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let deleted: Vec<&Pod> = all
            .iter()
            .filter(|p| p.metadata.deletion_timestamp.is_some())
            .collect();
        assert_eq!(deleted.len(), 1);
        assert!(!has_job_tracking_finalizer(deleted[0]));
    }

    /// `MaxPodCreateDeletePerSync = 500` (job_controller.go:79) caps one
    /// sync's creations (`:1735-1737`); the rest wait for the next sync.
    #[tokio::test]
    async fn test_manage_job_caps_creations_per_sync() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("cap", "default", 600, 600);
        storage
            .create("/registry/jobs/default/cap", &job)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/cap").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        let all: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(all.len(), 500);
    }

    /// `diff := wantActive - terminating - active` for
    /// podReplacementPolicy=Failed (job_controller.go:1722-1728): a pod that
    /// is still terminating is not replaced yet.
    #[tokio::test]
    async fn test_pod_replacement_policy_failed_waits_for_terminating() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("prf", "default", 1, 1);
        job.spec.pod_replacement_policy = Some("Failed".to_string());
        storage
            .create("/registry/jobs/default/prf", &job)
            .await
            .unwrap();
        let mut pod = make_pod("p1", "default", Phase::Running, "prf", "job-uid-1");
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
        storage
            .create("/registry/pods/default/p1", &pod)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/prf").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        let all: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(all.len(), 1, "no replacement while the pod is terminating");
    }

    /// The brake: while creations are unobserved the sync only updates
    /// status (job_controller.go:1016 `if satisfiedExpectations`).
    #[tokio::test]
    async fn test_unmet_creation_expectations_gate_manage_job() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("gate", "default", 3, 3);
        storage
            .create("/registry/jobs/default/gate", &job)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        controller.expectations.expect_creations("default/gate", 2);
        let mut job: Job = storage.get("/registry/jobs/default/gate").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        assert!(live_pods(&storage).await.is_empty());

        controller.expectations.creation_observed("default/gate");
        controller.expectations.creation_observed("default/gate");
        let mut job: Job = storage.get("/registry/jobs/default/gate").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        assert_eq!(live_pods(&storage).await.len(), 3);
    }

    /// `ExpectCreations` is recorded before the creates, and an `Added` pod
    /// event observes each creation exactly once (`addPod`, :339).
    #[tokio::test]
    async fn test_created_pods_are_expected_then_observed_once() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("obs", "default", 2, 2);
        storage
            .create("/registry/jobs/default/obs", &job)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/obs").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        assert_eq!(
            controller.expectations.get_expectations("default/obs"),
            Some((2, 0))
        );
        let pods = live_pods(&storage).await;
        for pod in &pods {
            let ev = rusternetes_storage::WatchEvent::Added(
                format!("pods/default/{}", pod.metadata.name),
                serde_json::to_string(pod).unwrap(),
            );
            deliver(&controller, &WorkQueue::new(), ev.clone()).await;
            deliver(&controller, &WorkQueue::new(), ev).await; // duplicate: an update, not a second creation
        }
        assert_eq!(
            controller.expectations.get_expectations("default/obs"),
            Some((0, 0))
        );
        assert!(controller.expectations.satisfied("default/obs"));
    }

    /// Suspend deletes through `deleteJobPods` with `ExpectDeletions`
    /// (job_controller.go:1666-1668), and the terminating-pod event
    /// observes it.
    #[tokio::test]
    async fn test_suspend_expects_deletions_and_observes_them() {
        let mut job = make_job("sus2", "default", 1, 1);
        job.spec.suspend = Some(true);
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create("/registry/jobs/default/sus2", &job)
            .await
            .unwrap();
        let pod = make_pod("p1", "default", Phase::Running, "sus2", "job-uid-1");
        storage
            .create("/registry/pods/default/p1", &pod)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get("/registry/jobs/default/sus2").await.unwrap();
        controller.reconcile(&mut job).await.unwrap();
        assert!(!controller.expectations.satisfied("default/sus2"));
        let gone: Pod = storage.get("/registry/pods/default/p1").await.unwrap();
        let ev = rusternetes_storage::WatchEvent::Modified(
            "pods/default/p1".to_string(),
            serde_json::to_string(&gone).unwrap(),
        );
        deliver(&controller, &WorkQueue::new(), ev).await;
        assert!(controller.expectations.satisfied("default/sus2"));
    }

    // ---- #3869: the pod handlers on the shared pod informer ----------------
    use rusternetes_controller_manager::controllers::pod_informer::apply_pod_observation;
    //
    // Ports of `TestAddPod*`/`TestUpdatePod*`/`TestDeletePod*`-style cases
    // (job_controller_test.go) and the handler bodies they pin
    // (job_controller.go:319-492), driven through the informer cache + the
    // `PodEventTarget` effects, as the running controller does.

    /// One pod watch event through the informer, delivered to `controller`.
    async fn deliver(
        controller: &JobController<MemoryStorage>,
        queue: &WorkQueue,
        event: rusternetes_storage::WatchEvent,
    ) {
        if let Some(observation) = controller.pod_informer.cache().on_event(&event) {
            apply_pod_observation(controller, queue, &observation).await;
        }
    }

    fn pod_event(
        kind: fn(String, String) -> rusternetes_storage::WatchEvent,
        pod: &Pod,
    ) -> rusternetes_storage::WatchEvent {
        kind(
            format!("/registry/pods/default/{}", pod.metadata.name),
            serde_json::to_string(pod).unwrap(),
        )
    }

    /// Everything the queue yields within `wait` (pod events enqueue after
    /// `SyncJobBatchPeriod`, `enqueueSyncJobBatched`, :599).
    async fn drain(queue: &WorkQueue, wait: Duration) -> Vec<String> {
        let mut keys = Vec::new();
        while let Ok(Some(key)) = tokio::time::timeout(wait, queue.get()).await {
            queue.done(&key).await;
            keys.push(key);
        }
        keys
    }

    async fn controller_with_job(job: Job) -> (Arc<MemoryStorage>, JobController<MemoryStorage>) {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create(
                &build_key("jobs", Some("default"), &job.metadata.name),
                &job,
            )
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        (storage, controller)
    }

    async fn store_pod(storage: &MemoryStorage, pod: &Pod) {
        storage
            .create(&build_key("pods", Some("default"), &pod.metadata.name), pod)
            .await
            .unwrap();
    }

    /// `updatePod` (:361-431): "Don't check if oldPod has the finalizer" —
    /// a pod update that leaves the pod without the tracking finalizer is
    /// `finalizerRemovalObserved` for its UID, and the Job is enqueued.
    #[tokio::test]
    async fn update_without_finalizer_observes_the_removal_and_enqueues() {
        let (_storage, controller) = controller_with_job(make_job("fr", "default", 1, 1)).await;
        let queue = WorkQueue::new();
        let mut pod = make_pod("fr-a", "default", Phase::Running, "fr", "job-uid-1");
        pod.metadata.resource_version = Some("1".into());
        deliver(
            &controller,
            &queue,
            pod_event(rusternetes_storage::WatchEvent::Added, &pod),
        )
        .await;
        drain(&queue, Duration::from_millis(1500)).await;

        controller
            .finalizer_expectations
            .expect_removed("default/fr", [pod.metadata.uid.clone()]);
        pod.metadata.finalizers = None;
        pod.metadata.resource_version = Some("2".into());
        deliver(
            &controller,
            &queue,
            pod_event(rusternetes_storage::WatchEvent::Modified, &pod),
        )
        .await;

        assert!(
            controller
                .finalizer_expectations
                .expected("default/fr")
                .is_empty(),
            "finalizerRemovalObserved (:389-394, :404-411)"
        );
        assert_eq!(
            drain(&queue, Duration::from_millis(1500)).await,
            vec!["jobs/default/fr".to_string()]
        );
    }

    /// `updatePod`: a pod that STILL holds the finalizer is not a removal.
    #[tokio::test]
    async fn update_with_finalizer_leaves_the_removal_expected() {
        let (_storage, controller) = controller_with_job(make_job("fk", "default", 1, 1)).await;
        let queue = WorkQueue::new();
        let mut pod = make_pod("fk-a", "default", Phase::Running, "fk", "job-uid-1");
        pod.metadata.resource_version = Some("1".into());
        deliver(
            &controller,
            &queue,
            pod_event(rusternetes_storage::WatchEvent::Added, &pod),
        )
        .await;
        controller
            .finalizer_expectations
            .expect_removed("default/fk", [pod.metadata.uid.clone()]);
        pod.metadata.resource_version = Some("2".into());
        deliver(
            &controller,
            &queue,
            pod_event(rusternetes_storage::WatchEvent::Modified, &pod),
        )
        .await;
        assert_eq!(
            controller
                .finalizer_expectations
                .expected("default/fk")
                .len(),
            1
        );
    }

    /// `deletePod` (:434-492): "Consider the finalizer removed if this is the
    /// final delete", even when the (stale) pod still lists the finalizer.
    #[tokio::test]
    async fn final_delete_observes_deletion_and_finalizer_removal() {
        let (_storage, controller) = controller_with_job(make_job("fd", "default", 1, 1)).await;
        let queue = WorkQueue::new();
        let pod = make_pod("fd-a", "default", Phase::Running, "fd", "job-uid-1");
        controller
            .expectations
            .expect_deletions_of("default/fd", &["default/fd-a".to_string()]);
        controller
            .finalizer_expectations
            .expect_removed("default/fd", [pod.metadata.uid.clone()]);
        deliver(
            &controller,
            &queue,
            pod_event(rusternetes_storage::WatchEvent::Deleted, &pod),
        )
        .await;
        assert!(controller.expectations.satisfied("default/fd"));
        assert!(controller
            .finalizer_expectations
            .expected("default/fd")
            .is_empty());
        assert_eq!(
            drain(&queue, Duration::from_millis(1500)).await,
            vec!["jobs/default/fd".to_string()]
        );
    }

    /// `deletePod(final=false)` — the deletionTimestamp update: "Otherwise,
    /// it's an update for the deletion timestamp, then check finalizer"
    /// (:484-488). A pod that still holds the finalizer is not yet removed.
    #[tokio::test]
    async fn terminating_pod_with_finalizer_is_not_a_finalizer_removal() {
        let (_storage, controller) = controller_with_job(make_job("ft", "default", 1, 1)).await;
        let queue = WorkQueue::new();
        let mut pod = make_pod("ft-a", "default", Phase::Running, "ft", "job-uid-1");
        pod.metadata.resource_version = Some("1".into());
        deliver(
            &controller,
            &queue,
            pod_event(rusternetes_storage::WatchEvent::Added, &pod),
        )
        .await;
        controller
            .expectations
            .expect_deletions_of("default/ft", &["default/ft-a".to_string()]);
        controller
            .finalizer_expectations
            .expect_removed("default/ft", [pod.metadata.uid.clone()]);
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
        pod.metadata.resource_version = Some("2".into());
        deliver(
            &controller,
            &queue,
            pod_event(rusternetes_storage::WatchEvent::Modified, &pod),
        )
        .await;
        assert!(controller.expectations.satisfied("default/ft"));
        assert_eq!(
            controller
                .finalizer_expectations
                .expected("default/ft")
                .len(),
            1,
            "still holds the finalizer, so its removal is not observed"
        );
    }

    /// `deletePod`: "if job == nil || util.IsJobFinished(job) { // syncJob will
    /// not remove this finalizer. if hasFinalizer { enqueueOrphanPod } return }"
    /// (:468-474): nothing is observed or enqueued, the finalizer is released.
    #[tokio::test]
    async fn terminating_pod_of_a_finished_job_is_released_not_observed() {
        let mut job = make_job("fin", "default", 1, 1);
        job.status = Some(JobStatus {
            conditions: Some(vec![JobCondition {
                condition_type: "Complete".to_string(),
                status: "True".to_string(),
                last_probe_time: None,
                last_transition_time: None,
                reason: None,
                message: None,
            }]),
            ..Default::default()
        });
        let (storage, controller) = controller_with_job(job).await;
        let queue = WorkQueue::new();
        let mut pod = make_pod("fin-a", "default", Phase::Running, "fin", "job-uid-1");
        pod.metadata.resource_version = Some("1".into());
        store_pod(&storage, &pod).await;
        controller
            .expectations
            .expect_deletions_of("default/fin", &["default/fin-a".to_string()]);
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
        pod.metadata.resource_version = Some("2".into());
        deliver(
            &controller,
            &queue,
            pod_event(rusternetes_storage::WatchEvent::Modified, &pod),
        )
        .await;
        assert!(
            !controller.expectations.satisfied("default/fin"),
            "a finished Job does not observe the deletion"
        );
        assert!(drain(&queue, Duration::from_millis(1500)).await.is_empty());
        let stored: Pod = storage
            .get(&build_key("pods", Some("default"), "fin-a"))
            .await
            .unwrap();
        assert!(!has_job_tracking_finalizer(&stored), "enqueueOrphanPod");
    }

    /// `addPod`: "Otherwise, it's an orphan. Clean the finalizer." (:344-347)
    #[tokio::test]
    async fn added_orphan_pod_holding_the_finalizer_is_released() {
        let (storage, controller) = controller_with_job(make_job("orph", "default", 1, 1)).await;
        let queue = WorkQueue::new();
        let mut pod = make_pod("orph-a", "default", Phase::Running, "orph", "job-uid-1");
        pod.metadata.owner_references = None;
        store_pod(&storage, &pod).await;
        deliver(
            &controller,
            &queue,
            pod_event(rusternetes_storage::WatchEvent::Added, &pod),
        )
        .await;
        let stored: Pod = storage
            .get(&build_key("pods", Some("default"), "orph-a"))
            .await
            .unwrap();
        assert!(!has_job_tracking_finalizer(&stored));
    }

    /// `addPod` orphan: "Get a list of all matching controllers and sync them
    /// to see if anyone wants to adopt it" via `getPodJobs` (:349-353,
    /// `GetPodJobs`, client-go listers/batch/v1/job_expansion.go).
    #[tokio::test]
    async fn added_orphan_pod_wakes_the_jobs_whose_selector_matches() {
        let storage = Arc::new(MemoryStorage::new());
        for name in ["match", "other"] {
            storage
                .create(
                    &build_key("jobs", Some("default"), name),
                    &job_with_selector(name),
                )
                .await
                .unwrap();
        }
        let controller = JobController::new(storage.clone());
        let queue = WorkQueue::new();
        let mut pod = make_pod("o-a", "default", Phase::Running, "match", "job-uid-1");
        pod.metadata.owner_references = None;
        pod.metadata.finalizers = None;
        deliver(
            &controller,
            &queue,
            pod_event(rusternetes_storage::WatchEvent::Added, &pod),
        )
        .await;
        assert_eq!(
            drain(&queue, Duration::from_millis(1500)).await,
            vec!["jobs/default/match".to_string()]
        );
    }

    /// `GetPodJobs`: "no jobs found for pod because it has no labels", a nil
    /// selector is `labels.Nothing()`, an empty one `labels.Everything()`.
    #[tokio::test]
    async fn get_pod_jobs_selector_semantics() {
        let storage = Arc::new(MemoryStorage::new());
        let mut nil = make_job("nil", "default", 1, 1);
        nil.spec.selector = None;
        let mut empty = make_job("empty", "default", 1, 1);
        empty.spec.selector = Some(Default::default());
        for job in [&nil, &empty] {
            storage
                .create(&build_key("jobs", Some("default"), &job.metadata.name), job)
                .await
                .unwrap();
        }
        let controller = JobController::new(storage);
        let labelled = HashMap::from([("a".to_string(), "b".to_string())]);
        assert_eq!(
            controller.pod_controllers("default", &labelled).await,
            vec!["empty".to_string()]
        );
        assert!(controller
            .pod_controllers("default", &HashMap::new())
            .await
            .is_empty());
    }

    /// `resolveControllerRef`: a Job with the same name but a different UID is
    /// not the controller (:309-314) — nothing is observed or enqueued.
    #[tokio::test]
    async fn pod_of_a_recreated_job_uid_mismatch_is_ignored() {
        let (_storage, controller) = controller_with_job(make_job("uid", "default", 1, 1)).await;
        let queue = WorkQueue::new();
        let pod = make_pod("uid-a", "default", Phase::Running, "uid", "some-other-uid");
        controller.expectations.expect_creations("default/uid", 1);
        deliver(
            &controller,
            &queue,
            pod_event(rusternetes_storage::WatchEvent::Added, &pod),
        )
        .await;
        assert!(!controller.expectations.satisfied("default/uid"));
        assert!(drain(&queue, Duration::from_millis(1500)).await.is_empty());
    }

    /// `enqueueSyncJobBatched` (:593-600): pod events enqueue after
    /// `SyncJobBatchPeriod`, not immediately.
    #[tokio::test]
    async fn pod_events_enqueue_after_the_batch_period() {
        let (_storage, controller) = controller_with_job(make_job("bp", "default", 1, 1)).await;
        let queue = WorkQueue::new();
        let pod = make_pod("bp-a", "default", Phase::Running, "bp", "job-uid-1");
        deliver(
            &controller,
            &queue,
            pod_event(rusternetes_storage::WatchEvent::Added, &pod),
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(300), queue.get())
                .await
                .is_err(),
            "must not be enqueued before SyncJobBatchPeriod"
        );
        assert_eq!(
            drain(&queue, Duration::from_millis(1500)).await,
            vec!["jobs/default/bp".to_string()]
        );
    }

    /// The running controller registers on the informer: an orphan that held
    /// the finalizer before the informer started is released from the
    /// informer's initial list (the `addPod` of `processDeltas`), with the
    /// informer started by ANOTHER controller first.
    #[tokio::test]
    async fn run_releases_orphans_when_the_informer_was_already_running() {
        let storage = Arc::new(MemoryStorage::new());
        let mut pod = make_pod("late-a", "default", Phase::Running, "late", "job-uid-1");
        pod.metadata.owner_references = None;
        store_pod(&storage, &pod).await;
        let informer = Arc::new(SharedPodInformer::new(
            storage.clone(),
            Duration::from_secs(5),
        ));
        informer.relist().await;
        let controller = Arc::new(JobController::with_pod_informer(storage.clone(), informer));
        let handle = tokio::spawn(controller.run());
        let held = wait_until_released(&storage, &["late-a"]).await;
        handle.abort();
        assert!(held.is_empty(), "still held: {held:?}");
    }

    fn pod_for_ordering(name: &str, phase: Phase, node: Option<&str>, ready: bool) -> Pod {
        let mut p = make_pod(name, "default", phase, "j", "job-uid-1");
        p.spec.as_mut().unwrap().node_name = node.map(str::to_string);
        if ready {
            p.status.as_mut().unwrap().conditions =
                Some(vec![rusternetes_common::resources::pod::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]);
        }
        p
    }

    /// `controller.ActivePods` (controller_utils.go:741): unassigned <
    /// assigned, Pending < Running, not-ready < ready — the first are
    /// removed first.
    #[test]
    fn test_active_pods_for_removal_prefers_least_progressed() {
        let job = make_job("j", "default", 5, 1);
        let ready_running = pod_for_ordering("a", Phase::Running, Some("n1"), true);
        let pending_assigned = pod_for_ordering("b", Phase::Pending, Some("n1"), false);
        let unassigned = pod_for_ordering("c", Phase::Pending, None, false);
        let pods = vec![&ready_running, &pending_assigned, &unassigned];
        let rm = active_pods_for_removal(&job, &pods, 2);
        let names: Vec<&str> = rm.iter().map(|p| p.metadata.name.as_str()).collect();
        assert_eq!(names, vec!["c", "b"]);
    }

    /// `appendDuplicatedIndexPodsForRemoval` (indexed_job_utils.go:295): for
    /// an Indexed Job, duplicates of an index and out-of-range indexes are
    /// removed even beyond `rmAtLeast`.
    #[test]
    fn test_active_pods_for_removal_indexed_duplicates() {
        let mut job = make_job("j", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        let a0 = make_indexed_pod("a0", "default", Phase::Running, "j", "job-uid-1", 0);
        let b0 = make_indexed_pod("b0", "default", Phase::Pending, "j", "job-uid-1", 0);
        let a1 = make_indexed_pod("a1", "default", Phase::Running, "j", "job-uid-1", 1);
        let a7 = make_indexed_pod("a7", "default", Phase::Running, "j", "job-uid-1", 7);
        let pods = vec![&a0, &b0, &a1, &a7];
        let rm = active_pods_for_removal(&job, &pods, 0);
        let mut names: Vec<&str> = rm.iter().map(|p| p.metadata.name.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["a7", "b0"]);
    }

    // --- `enactJobFinished` delay on EVERY finish path (#2651) -------------
    //
    // job_controller.go:1520-1524 holds the terminal condition back while
    // pods terminate, whichever path produced `finishedCondition`; and
    // :1003-1007 clears `finishedCondition` when `deleted != active ||
    // !satisfiedExpectations`.

    fn conds_of(job: &Job) -> Vec<String> {
        job.status
            .as_ref()
            .and_then(|s| s.conditions.clone())
            .unwrap_or_default()
            .into_iter()
            .filter(|c| c.status == "True")
            .map(|c| c.condition_type)
            .collect()
    }

    /// A pod that is Running but already marked for deletion, as the kubelet
    /// leaves it while the container stops.
    fn terminating_pod(name: &str, job_name: &str) -> Pod {
        let mut pod = make_pod(name, "default", Phase::Running, job_name, "job-uid-1");
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
        pod
    }

    #[tokio::test]
    async fn complete_is_delayed_while_pods_terminate() {
        let storage = Arc::new(MemoryStorage::new());
        let job = make_job("cdel", "default", 1, 1);
        let job_key = "/registry/jobs/default/cdel";
        storage.create(job_key, &job).await.unwrap();
        for pod in [
            make_pod("c-ok", "default", Phase::Succeeded, "cdel", "job-uid-1"),
            terminating_pod("c-term", "cdel"),
        ] {
            let key = format!("/registry/pods/default/{}", pod.metadata.name);
            storage.create(&key, &pod).await.unwrap();
        }
        let controller = JobController::new(storage.clone());
        let mut j: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();

        let got: Job = storage.get(job_key).await.unwrap();
        let conds = conds_of(&got);
        assert!(
            conds.contains(&"SuccessCriteriaMet".to_string()),
            "{conds:?}"
        );
        assert!(
            !conds.contains(&"Complete".to_string()),
            "Complete must wait for the terminating pod: {conds:?}"
        );
        assert!(got.status.as_ref().unwrap().completion_time.is_none());
        assert_eq!(got.status.as_ref().unwrap().terminating, Some(1));

        reap_terminating(&storage).await;
        let mut j: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();
        let got: Job = storage.get(job_key).await.unwrap();
        assert!(conds_of(&got).contains(&"Complete".to_string()));
        assert!(got.status.as_ref().unwrap().completion_time.is_some());
    }

    #[tokio::test]
    async fn backoff_failed_is_delayed_while_pods_terminate() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("bdel", "default", 1, 2);
        job.spec.backoff_limit = Some(0);
        let job_key = "/registry/jobs/default/bdel";
        storage.create(job_key, &job).await.unwrap();
        for pod in [
            make_pod("b-fail", "default", Phase::Failed, "bdel", "job-uid-1"),
            terminating_pod("b-term", "bdel"),
        ] {
            let key = format!("/registry/pods/default/{}", pod.metadata.name);
            storage.create(&key, &pod).await.unwrap();
        }
        let controller = JobController::new(storage.clone());
        let mut j: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();

        let got: Job = storage.get(job_key).await.unwrap();
        let conds = conds_of(&got);
        assert!(conds.contains(&"FailureTarget".to_string()), "{conds:?}");
        assert!(
            !conds.contains(&"Failed".to_string()),
            "Failed must wait for the terminating pod: {conds:?}"
        );
        assert_eq!(got.status.as_ref().unwrap().terminating, Some(1));

        reap_terminating(&storage).await;
        let mut j: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();
        let got: Job = storage.get(job_key).await.unwrap();
        assert!(conds_of(&got).contains(&"Failed".to_string()));
    }

    /// `deleteActivePods` (:1002): a Job past its backoff limit deletes its
    /// still-active pods, and only finishes once they are gone.
    #[tokio::test]
    async fn backoff_failed_deletes_active_pods_then_finishes() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("bact", "default", 1, 2);
        job.spec.backoff_limit = Some(0);
        let job_key = "/registry/jobs/default/bact";
        storage.create(job_key, &job).await.unwrap();
        for pod in [
            make_pod("a-fail", "default", Phase::Failed, "bact", "job-uid-1"),
            make_pod("a-run", "default", Phase::Running, "bact", "job-uid-1"),
        ] {
            let key = format!("/registry/pods/default/{}", pod.metadata.name);
            storage.create(&key, &pod).await.unwrap();
        }
        let controller = JobController::new(storage.clone());
        let mut j: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();

        let run: Pod = storage.get("/registry/pods/default/a-run").await.unwrap();
        assert!(
            run.metadata.deletion_timestamp.is_some(),
            "the active pod of a failed Job must be deleted"
        );
        let got: Job = storage.get(job_key).await.unwrap();
        assert!(!conds_of(&got).contains(&"Failed".to_string()));

        reap_terminating(&storage).await;
        let mut j: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();
        let got: Job = storage.get(job_key).await.unwrap();
        assert!(conds_of(&got).contains(&"Failed".to_string()));
    }

    /// `if deleted != active || !satisfiedExpectations { finishedCondition =
    /// nil }` (:1003-1007): no terminal (nor interim) condition while a pod
    /// create is still unobserved.
    #[tokio::test]
    async fn failed_is_not_declared_while_expectations_unsatisfied() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("bexp", "default", 1, 1);
        job.spec.backoff_limit = Some(0);
        let job_key = "/registry/jobs/default/bexp";
        storage.create(job_key, &job).await.unwrap();
        let pod = make_pod("e-fail", "default", Phase::Failed, "bexp", "job-uid-1");
        storage
            .create("/registry/pods/default/e-fail", &pod)
            .await
            .unwrap();
        let controller = JobController::new(storage.clone());
        controller.expectations.expect_creations("default/bexp", 1);
        let mut j: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();
        let got: Job = storage.get(job_key).await.unwrap();
        let conds = conds_of(&got);
        assert!(
            !conds.contains(&"Failed".to_string()) && !conds.contains(&"FailureTarget".to_string()),
            "must not finish while a pod create is unobserved: {conds:?}"
        );

        controller.expectations.creation_observed("default/bexp");
        let mut j: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();
        let got: Job = storage.get(job_key).await.unwrap();
        assert!(conds_of(&got).contains(&"Failed".to_string()));
    }

    /// Regression of #2784 (sig-apps "with successPolicy succeededIndexes rule
    /// should succeeded even when some indexes remain pending"): the spec waits
    /// for `status.succeeded == 1`. Before #2784 Complete was written in the
    /// same pass that met the policy, so a later Succeeded pod was never
    /// counted; now the Job stays unfinished while pods terminate, and a pod
    /// the sync itself deleted that then exits 0 must not add an index.
    #[tokio::test]
    async fn success_policy_ignores_late_success_of_pods_it_deleted() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("late", "default", 5, 2);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [{ "succeededIndexes": "0" }]
            }))
            .unwrap(),
        );
        let key = "/registry/jobs/default/late";
        storage.create(key, &job).await.unwrap();
        let p0 = make_indexed_pod("p0", "default", Phase::Succeeded, "late", "job-uid-1", 0);
        let p1 = make_indexed_pod("p1", "default", Phase::Running, "late", "job-uid-1", 1);
        storage
            .create("/registry/pods/default/p0", &p0)
            .await
            .unwrap();
        storage
            .create("/registry/pods/default/p1", &p1)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut j: Job = storage.get(key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();

        // The kubelet: the deleted pod's container exits 0 before it is killed.
        let mut p1: Pod = storage.get("/registry/pods/default/p1").await.unwrap();
        assert!(p1.metadata.deletion_timestamp.is_some(), "sync deletes p1");
        p1.status.as_mut().unwrap().phase = Some(Phase::Succeeded);
        storage
            .update("/registry/pods/default/p1", &p1)
            .await
            .unwrap();

        let mut j: Job = storage.get(key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();
        reap_terminating(&storage).await;
        let mut j: Job = storage.get(key).await.unwrap();
        controller.reconcile(&mut j).await.unwrap();

        let got: Job = storage.get(key).await.unwrap();
        let st = got.status.as_ref().unwrap();
        assert_eq!(st.succeeded, Some(1), "{st:?}");
        assert_eq!(st.completed_indexes.as_deref(), Some("0"), "{st:?}");
        assert!(
            st.conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True"),
            "{st:?}"
        );
    }

    // ---- #2780: persisted interim condition + upstream evaluation order ----
    //
    // Upstream syncJob (pkg/controller/job/job_controller.go:945-998):
    //   jobCtx.finishedCondition = hasSuccessCriteriaMetCondition(&job)   // :947
    //   if nil { FailureTarget=True -> newFailedConditionForFailureTarget // :953-955
    //            else podFailurePolicy }
    //   if nil { backoffLimit, then activeDeadline }
    //   ... per-index failures, then successPolicy, then completions.

    fn interim(condition_type: &str, reason: &str, message: &str) -> JobCondition {
        let now = chrono::Utc::now();
        JobCondition {
            condition_type: condition_type.to_string(),
            status: "True".to_string(),
            last_probe_time: Some(now),
            last_transition_time: Some(now),
            reason: Some(reason.to_string()),
            message: Some(message.to_string()),
        }
    }

    fn cond_of<'a>(job: &'a Job, t: &str) -> Option<&'a JobCondition> {
        job.status
            .as_ref()?
            .conditions
            .as_ref()?
            .iter()
            .find(|c| c.condition_type == t && c.status == "True")
    }

    /// job_controller_test.go:4977 "job with SuccessCriteriaMet has never been
    /// transitioned to FailureTarget and Failed even if job meets backoffLimit"
    /// (the persisted condition, not the spec's successPolicy, decides).
    #[tokio::test]
    async fn persisted_success_criteria_met_beats_backoff_limit() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("psc", "default", 2, 2);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit = Some(0);
        job.status = Some(JobStatus {
            conditions: Some(vec![interim(
                "SuccessCriteriaMet",
                "SuccessPolicy",
                "Matched rules at index 0",
            )]),
            ..Default::default()
        });
        let key = "/registry/jobs/default/psc";
        storage.create(key, &job).await.unwrap();
        for (n, ph, i) in [("a", Phase::Failed, 0), ("b", Phase::Succeeded, 1)] {
            let p = make_indexed_pod(n, "default", ph, "psc", "job-uid-1", i);
            storage
                .create(&format!("/registry/pods/default/{n}"), &p)
                .await
                .unwrap();
        }
        let c = JobController::new(storage.clone());
        reconcile_settled(&c, &storage, key).await;
        let got: Job = storage.get(key).await.unwrap();
        assert!(cond_of(&got, "Failed").is_none(), "{:?}", got.status);
        let done = cond_of(&got, "Complete").expect("Complete follows SuccessCriteriaMet");
        assert_eq!(done.reason.as_deref(), Some("SuccessPolicy"));
        assert_eq!(done.message.as_deref(), Some("Matched rules at index 0"));
    }

    /// job_controller_test.go:5039 "job with FailureTarget has never been
    /// transitioned to SuccessCriteriaMet even if job meets successPolicy".
    #[tokio::test]
    async fn persisted_failure_target_beats_success_policy() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("pft", "default", 2, 2);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(
            serde_json::from_value(serde_json::json!({"rules": [{"succeededCount": 1}]})).unwrap(),
        );
        let msg =
            "Pod default/mypod-0 has condition DisruptionTarget matching FailJob rule at index 0";
        job.status = Some(JobStatus {
            conditions: Some(vec![interim("FailureTarget", "PodFailurePolicy", msg)]),
            ..Default::default()
        });
        let key = "/registry/jobs/default/pft";
        storage.create(key, &job).await.unwrap();
        let p = make_indexed_pod("b", "default", Phase::Succeeded, "pft", "job-uid-1", 1);
        storage
            .create("/registry/pods/default/b", &p)
            .await
            .unwrap();
        let c = JobController::new(storage.clone());
        reconcile_settled(&c, &storage, key).await;
        let got: Job = storage.get(key).await.unwrap();
        assert!(
            cond_of(&got, "SuccessCriteriaMet").is_none(),
            "{:?}",
            got.status
        );
        assert!(cond_of(&got, "Complete").is_none());
        let failed = cond_of(&got, "Failed").expect("Failed follows FailureTarget");
        assert_eq!(failed.reason.as_deref(), Some("PodFailurePolicy"));
        assert_eq!(failed.message.as_deref(), Some(msg));
    }

    /// `newFailedConditionForFailureTarget` (job_controller.go:1562): Failed
    /// carries the persisted FailureTarget's reason even when nothing in the
    /// current pod list would re-derive it, and the Job stops creating pods.
    #[tokio::test]
    async fn persisted_failure_target_finishes_without_rederiving() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("pfr", "default", 3, 3);
        job.status = Some(JobStatus {
            conditions: Some(vec![interim(
                "FailureTarget",
                "DeadlineExceeded",
                "Job was active longer than specified deadline",
            )]),
            ..Default::default()
        });
        let key = "/registry/jobs/default/pfr";
        storage.create(key, &job).await.unwrap();
        let p = make_pod("r", "default", Phase::Running, "pfr", "job-uid-1");
        storage
            .create("/registry/pods/default/r", &p)
            .await
            .unwrap();
        let c = JobController::new(storage.clone());
        reconcile_settled(&c, &storage, key).await;
        let got: Job = storage.get(key).await.unwrap();
        let failed = cond_of(&got, "Failed").expect("Failed follows FailureTarget");
        assert_eq!(failed.reason.as_deref(), Some("DeadlineExceeded"));
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(pods.len() <= 1, "no replacement pods: {}", pods.len());
    }

    /// Evaluation order, job_controller.go:960-985: backoffLimit is evaluated
    /// before activeDeadlineSeconds.
    #[tokio::test]
    async fn backoff_limit_is_evaluated_before_active_deadline() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("ord", "default", 1, 1);
        job.spec.backoff_limit = Some(0);
        job.spec.active_deadline_seconds = Some(1);
        job.status = Some(JobStatus {
            start_time: Some(chrono::Utc::now() - chrono::Duration::seconds(60)),
            ..Default::default()
        });
        let key = "/registry/jobs/default/ord";
        storage.create(key, &job).await.unwrap();
        let p = make_pod("f", "default", Phase::Failed, "ord", "job-uid-1");
        storage
            .create("/registry/pods/default/f", &p)
            .await
            .unwrap();
        let c = JobController::new(storage.clone());
        reconcile_settled(&c, &storage, key).await;
        let got: Job = storage.get(key).await.unwrap();
        let failed = cond_of(&got, "Failed").expect("Failed");
        assert_eq!(failed.reason.as_deref(), Some("BackoffLimitExceeded"));
    }

    /// Failure scenarios are evaluated before completions
    /// (job_controller.go:945-998, then `complete` at :1035 only when
    /// finishedCondition is nil).
    #[tokio::test]
    async fn failure_is_evaluated_before_completions() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("fbc", "default", 1, 1);
        job.spec.backoff_limit = Some(0);
        let key = "/registry/jobs/default/fbc";
        storage.create(key, &job).await.unwrap();
        for (n, ph) in [("s", Phase::Succeeded), ("f", Phase::Failed)] {
            let p = make_pod(n, "default", ph, "fbc", "job-uid-1");
            storage
                .create(&format!("/registry/pods/default/{n}"), &p)
                .await
                .unwrap();
        }
        let c = JobController::new(storage.clone());
        reconcile_settled(&c, &storage, key).await;
        let got: Job = storage.get(key).await.unwrap();
        assert!(cond_of(&got, "Complete").is_none(), "{:?}", got.status);
        assert!(cond_of(&got, "Failed").is_some());
    }

    // ---- #2818: failure scenarios outrank suspend ----
    //
    // syncJob (job_controller.go:945-998) evaluates finishedCondition for every
    // Job; `manageJob` (:1663) is where suspend is handled, and only runs when
    // finishedCondition is nil. `pastActiveDeadline` (:1596) is false while
    // suspended ("activeDeadlineSeconds is not triggered when Job is
    // suspended", job_controller_test.go:2603).

    /// A suspended Job that exceeds backoffLimit still fails.
    #[tokio::test]
    async fn suspended_job_over_backoff_limit_fails() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("sbl", "default", 1, 1);
        job.spec.suspend = Some(true);
        job.spec.backoff_limit = Some(0);
        let key = "/registry/jobs/default/sbl";
        storage.create(key, &job).await.unwrap();
        let p = make_pod("f", "default", Phase::Failed, "sbl", "job-uid-1");
        storage
            .create("/registry/pods/default/f", &p)
            .await
            .unwrap();
        let c = JobController::new(storage.clone());
        reconcile_settled(&c, &storage, key).await;
        let got: Job = storage.get(key).await.unwrap();
        let failed = cond_of(&got, "Failed").expect("failure outranks suspend");
        assert_eq!(failed.reason.as_deref(), Some("BackoffLimitExceeded"));
    }

    /// A suspended Job with a persisted FailureTarget still becomes Failed.
    #[tokio::test]
    async fn suspended_job_with_persisted_failure_target_fails() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("spf", "default", 1, 1);
        job.spec.suspend = Some(true);
        job.status = Some(JobStatus {
            conditions: Some(vec![interim(
                "FailureTarget",
                "BackoffLimitExceeded",
                "Job has reached the specified backoff limit",
            )]),
            ..Default::default()
        });
        let key = "/registry/jobs/default/spf";
        storage.create(key, &job).await.unwrap();
        let c = JobController::new(storage.clone());
        reconcile_settled(&c, &storage, key).await;
        let got: Job = storage.get(key).await.unwrap();
        assert!(cond_of(&got, "Failed").is_some(), "{:?}", got.status);
    }

    /// job_controller_test.go:2603: the deadline does not fire while suspended,
    /// and the suspended Job keeps no active pods.
    #[tokio::test]
    async fn suspended_job_past_deadline_does_not_fail() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("sdl", "default", 2, 1);
        job.spec.suspend = Some(true);
        job.spec.active_deadline_seconds = Some(10);
        job.status = Some(JobStatus {
            start_time: Some(chrono::Utc::now() - chrono::Duration::seconds(15)),
            ..Default::default()
        });
        let key = "/registry/jobs/default/sdl";
        storage.create(key, &job).await.unwrap();
        let c = JobController::new(storage.clone());
        reconcile_settled(&c, &storage, key).await;
        let got: Job = storage.get(key).await.unwrap();
        assert!(cond_of(&got, "Failed").is_none(), "{:?}", got.status);
    }

    /// A suspended Job that is already done (`complete`, :1035) completes.
    #[tokio::test]
    async fn suspended_job_with_all_completions_completes() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("ssc", "default", 1, 1);
        job.spec.suspend = Some(true);
        let key = "/registry/jobs/default/ssc";
        storage.create(key, &job).await.unwrap();
        let p = make_pod("s", "default", Phase::Succeeded, "ssc", "job-uid-1");
        storage
            .create("/registry/pods/default/s", &p)
            .await
            .unwrap();
        let c = JobController::new(storage.clone());
        reconcile_settled(&c, &storage, key).await;
        let got: Job = storage.get(key).await.unwrap();
        assert!(cond_of(&got, "Complete").is_some(), "{:?}", got.status);
    }

    /// Run one settled reconcile of a 3-completion job whose pods are
    /// `(index, phase)`, and return the stored Job.
    async fn settle_sp(
        name: &str,
        indexed: bool,
        patch: impl FnOnce(&mut Job),
        pods: &[(i32, Phase)],
    ) -> Job {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job(name, "default", 3, 3);
        if indexed {
            job.spec.completion_mode = Some("Indexed".to_string());
        }
        patch(&mut job);
        let key = format!("/registry/jobs/default/{name}");
        storage.create(&key, &job).await.unwrap();
        for (i, phase) in pods {
            let pn = format!("{name}-{i}");
            let p = make_indexed_pod(&pn, "default", phase.clone(), name, "job-uid-1", *i);
            storage
                .create(&format!("/registry/pods/default/{pn}"), &p)
                .await
                .unwrap();
        }
        let c = JobController::new(storage.clone());
        reconcile_settled(&c, &storage, &key).await;
        storage.get(&key).await.unwrap()
    }

    fn sp(rules: serde_json::Value) -> Option<SuccessPolicy> {
        Some(serde_json::from_value(serde_json::json!({ "rules": rules })).unwrap())
    }

    /// job_controller.go:984 sits inside `if hasBackoffLimitPerIndex(&job)`
    /// (:978): without backoffLimitPerIndex, maxFailedIndexes is never
    /// evaluated and failed pods count only against backoffLimit.
    #[tokio::test]
    async fn max_failed_indexes_needs_backoff_limit_per_index() {
        let got = settle_sp(
            "mfi",
            true,
            |j| {
                j.spec.backoff_limit = Some(6);
                j.spec.max_failed_indexes = Some(0);
            },
            &[(0, Phase::Failed), (1, Phase::Running), (2, Phase::Running)],
        )
        .await;
        assert!(cond_of(&got, "Failed").is_none(), "{:?}", got.status);
    }

    /// success_policy.go matchSuccessPolicy only has a `SucceededIndexes` and
    /// a `SucceededCount` branch; a rule with neither never matches.
    #[tokio::test]
    async fn success_policy_rule_without_criteria_never_matches() {
        let got = settle_sp(
            "nocrit",
            true,
            |j| j.spec.success_policy = sp(serde_json::json!([{}])),
            &[
                (0, Phase::Succeeded),
                (1, Phase::Succeeded),
                (2, Phase::Running),
            ],
        )
        .await;
        assert!(
            cond_of(&got, "SuccessCriteriaMet").is_none(),
            "{:?}",
            got.status
        );
        assert!(cond_of(&got, "Complete").is_none(), "{:?}", got.status);
    }

    /// success_policy.go matchSuccessPolicy: a rule whose succeededIndexes
    /// parses to nothing is skipped (`continue`) and the next rule is tried.
    #[tokio::test]
    async fn success_policy_skips_unparsable_rule() {
        let got = settle_sp(
            "badrule",
            true,
            |j| {
                j.spec.success_policy = sp(serde_json::json!([
                    {"succeededIndexes": "abc", "succeededCount": 1},
                    {"succeededCount": 1}
                ]))
            },
            &[
                (0, Phase::Succeeded),
                (1, Phase::Running),
                (2, Phase::Running),
            ],
        )
        .await;
        let c = cond_of(&got, "SuccessCriteriaMet")
            .or_else(|| cond_of(&got, "Complete"))
            .expect("policy met");
        assert_eq!(c.message.as_deref(), Some("Matched rules at index 1"));
    }

    /// job_controller.go:991-996: success policy is evaluated only inside
    /// `if isIndexedJob(&job)`.
    #[tokio::test]
    async fn success_policy_ignored_for_non_indexed_job() {
        let got = settle_sp(
            "nonidx",
            false,
            |j| j.spec.success_policy = sp(serde_json::json!([{"succeededCount": 1}])),
            &[
                (0, Phase::Succeeded),
                (1, Phase::Running),
                (2, Phase::Running),
            ],
        )
        .await;
        assert!(
            cond_of(&got, "SuccessCriteriaMet").is_none(),
            "{:?}",
            got.status
        );
        assert!(cond_of(&got, "Complete").is_none(), "{:?}", got.status);
    }

    /// matchSucceededIndexesRule: with both fields set, succeededCount counts
    /// only succeeded indexes inside the rule's set (`contains >=
    /// succeededCount`), not every succeeded index.
    #[tokio::test]
    async fn success_policy_count_is_scoped_to_rule_indexes() {
        // Indexes 1 and 2 succeeded; rule set {0,1}: contains == 1 < 2.
        let got = settle_sp(
            "scoped",
            true,
            |j| {
                j.spec.success_policy =
                    sp(serde_json::json!([{"succeededIndexes": "0-1", "succeededCount": 2}]))
            },
            &[
                (0, Phase::Running),
                (1, Phase::Succeeded),
                (2, Phase::Succeeded),
            ],
        )
        .await;
        assert!(
            cond_of(&got, "SuccessCriteriaMet").is_none(),
            "{:?}",
            got.status
        );
        assert!(cond_of(&got, "Complete").is_none(), "{:?}", got.status);
        // Indexes 0 and 1 of rule set {0-2} succeeded: contains == 2 >= 2.
        let got = settle_sp(
            "scoped2",
            true,
            |j| {
                j.spec.success_policy =
                    sp(serde_json::json!([{"succeededIndexes": "0-2", "succeededCount": 2}]))
            },
            &[
                (0, Phase::Succeeded),
                (1, Phase::Succeeded),
                (2, Phase::Running),
            ],
        )
        .await;
        assert!(
            cond_of(&got, "SuccessCriteriaMet")
                .or_else(|| cond_of(&got, "Complete"))
                .is_some(),
            "{:?}",
            got.status
        );
    }

    // ---- #2841: one finishedCondition path ----

    /// `pastActiveDeadline` (job_controller.go:1601): `duration >= allowedDuration`.
    /// A Job active for exactly the deadline has exceeded it.
    #[tokio::test]
    async fn deadline_is_exceeded_at_exactly_the_deadline() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("dlb", "default", 1, 1);
        job.spec.active_deadline_seconds = Some(10);
        job.status = Some(JobStatus {
            start_time: Some(chrono::Utc::now() - chrono::Duration::seconds(10)),
            ..Default::default()
        });
        let key = "/registry/jobs/default/dlb";
        storage.create(key, &job).await.unwrap();
        let c = JobController::new(storage.clone());
        reconcile_settled(&c, &storage, key).await;
        let got: Job = storage.get(key).await.unwrap();
        let failed = cond_of(&got, "Failed").expect("deadline reached => Failed");
        assert_eq!(failed.reason.as_deref(), Some("DeadlineExceeded"));
    }

    /// `syncJob` (job_controller.go:970-973): while a Job has not yet passed
    /// `activeDeadlineSeconds` (and is not suspended), the sync is re-enqueued
    /// `AddAfter(key, deadline - Since(startTime))` so the deadline fires on
    /// time rather than at the next unrelated event.
    #[tokio::test]
    async fn unexpired_deadline_requeues_for_the_remaining_time() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("dlr", "default", 1, 1);
        job.spec.active_deadline_seconds = Some(100);
        job.status = Some(JobStatus {
            start_time: Some(chrono::Utc::now() - chrono::Duration::seconds(40)),
            ..Default::default()
        });
        storage
            .create("/registry/jobs/default/dlr", &job)
            .await
            .unwrap();
        let c = JobController::new(storage.clone());
        c.reconcile(&mut job).await.unwrap();
        let d = c
            .take_requeue_delay("default", "dlr")
            .expect("an unexpired deadline must request a delayed requeue");
        assert!(
            d > Duration::from_secs(55) && d <= Duration::from_secs(60),
            "{d:?}"
        );
    }

    /// Suspended Jobs never requeue for the deadline (:971 `!jobSuspended`).
    #[tokio::test]
    async fn suspended_job_does_not_requeue_for_deadline() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("dls", "default", 1, 1);
        job.spec.active_deadline_seconds = Some(100);
        job.spec.suspend = Some(true);
        job.status = Some(JobStatus {
            start_time: Some(chrono::Utc::now() - chrono::Duration::seconds(40)),
            ..Default::default()
        });
        storage
            .create("/registry/jobs/default/dls", &job)
            .await
            .unwrap();
        let c = JobController::new(storage.clone());
        c.reconcile(&mut job).await.unwrap();
        assert!(c.take_requeue_delay("default", "dls").is_none());
    }

    /// The deadline is a failure scenario evaluated before completions
    /// (job_controller.go:968-970, `complete` at :1035 only when
    /// finishedCondition is nil): a Job past its deadline fails even if every
    /// completion has already succeeded.
    #[tokio::test]
    async fn deadline_outranks_completions() {
        let storage = Arc::new(MemoryStorage::new());
        let mut job = make_job("dlc", "default", 1, 1);
        job.spec.active_deadline_seconds = Some(10);
        job.status = Some(JobStatus {
            start_time: Some(chrono::Utc::now() - chrono::Duration::seconds(60)),
            ..Default::default()
        });
        let key = "/registry/jobs/default/dlc";
        storage.create(key, &job).await.unwrap();
        let p = make_pod("s", "default", Phase::Succeeded, "dlc", "job-uid-1");
        storage
            .create("/registry/pods/default/s", &p)
            .await
            .unwrap();
        let c = JobController::new(storage.clone());
        reconcile_settled(&c, &storage, key).await;
        let got: Job = storage.get(key).await.unwrap();
        assert!(cond_of(&got, "Complete").is_none(), "{:?}", got.status);
        let failed = cond_of(&got, "Failed").expect("deadline outranks completions");
        assert_eq!(failed.reason.as_deref(), Some("DeadlineExceeded"));
    }
}
