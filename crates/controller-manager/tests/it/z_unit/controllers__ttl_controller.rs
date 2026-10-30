//! Unit tests moved from `src/controllers/ttl_controller.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Duration, Utc};
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::workloads::Job;
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use std::sync::Arc;
    pub use tokio::time::{sleep, Duration as TokioDuration};
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use chrono::Utc;
    use rusternetes_common::resources::workloads::{
        JobCondition, JobSpec, JobStatus, PodTemplateSpec,
    };
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_controller_manager::controllers::ttl_controller::*;
    use std::collections::HashMap;

    fn create_test_job(name: &str, namespace: &str, ttl_seconds: i32) -> Job {
        let mut annotations = HashMap::new();
        annotations.insert(
            "ttlSecondsAfterFinished".to_string(),
            ttl_seconds.to_string(),
        );

        Job {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Job".to_string(),
                api_version: "batch/v1".to_string(),
            },
            metadata: ObjectMeta::new(name)
                .with_namespace(namespace)
                .with_labels(HashMap::new())
                .with_annotations(annotations),
            spec: JobSpec {
                template: PodTemplateSpec {
                    metadata: None,
                    spec: rusternetes_common::resources::pod::PodSpec {
                        containers: vec![],
                        init_containers: None,
                        volumes: None,
                        restart_policy: Some("Never".to_string()),
                        node_name: None,
                        node_selector: None,
                        service_account_name: None,
                        service_account: None,
                        hostname: None,
                        subdomain: None,
                        host_network: None,
                        host_pid: None,
                        host_ipc: None,
                        affinity: None,
                        tolerations: None,
                        priority: None,
                        priority_class_name: None,
                        automount_service_account_token: None,
                        ephemeral_containers: None,
                        overhead: None,
                        scheduler_name: None,
                        topology_spread_constraints: None,
                        resource_claims: None,
                        active_deadline_seconds: None,
                        dns_policy: None,
                        dns_config: None,
                        security_context: None,
                        image_pull_secrets: None,
                        share_process_namespace: None,
                        readiness_gates: None,
                        runtime_class_name: None,
                        enable_service_links: None,
                        preemption_policy: None,
                        host_users: None,
                        set_hostname_as_fqdn: None,
                        termination_grace_period_seconds: None,
                        host_aliases: None,
                        os: None,
                        scheduling_gates: None,
                        resources: None,
                        ..Default::default()
                    },
                },
                completions: Some(1),
                parallelism: Some(1),
                backoff_limit: Some(3),
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
            status: Some(JobStatus {
                active: Some(0),
                succeeded: Some(1),
                failed: Some(0),
                conditions: Some(vec![JobCondition {
                    condition_type: "Complete".to_string(),
                    status: "True".to_string(),
                    last_probe_time: Some(Utc::now()),
                    last_transition_time: Some(Utc::now() - Duration::seconds(120)),
                    reason: Some("JobComplete".to_string()),
                    message: Some("Job completed successfully".to_string()),
                }]),
                start_time: None,
                completion_time: None,
                ready: None,
                terminating: None,
                completed_indexes: None,
                failed_indexes: None,
                uncounted_terminated_pods: None,
                observed_generation: None,
            }),
        }
    }

    #[tokio::test]
    async fn test_ttl_controller_identifies_finished_job() {
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let controller =
            TTLController::<rusternetes_storage::memory::MemoryStorage>::new(storage.clone());

        let job = create_test_job("test-job", "default", 60);
        assert!(controller.is_job_finished(&job));
    }

    #[tokio::test]
    async fn test_ttl_controller_gets_finish_time() {
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let controller =
            TTLController::<rusternetes_storage::memory::MemoryStorage>::new(storage.clone());

        let job = create_test_job("test-job", "default", 60);
        let finish_time = controller.get_job_finish_time(&job);
        assert!(finish_time.is_some());
    }

    #[tokio::test]
    async fn test_ttl_seconds_parsing() {
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let controller =
            TTLController::<rusternetes_storage::memory::MemoryStorage>::new(storage.clone());

        let job = create_test_job("test-job", "default", 100);
        let ttl = controller.get_ttl_seconds_after_finished(&job);
        assert_eq!(ttl, Some(100));
    }

    #[tokio::test]
    async fn test_should_cleanup_expired_job() {
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let controller =
            TTLController::<rusternetes_storage::memory::MemoryStorage>::new(storage.clone());

        // Create a job that finished 120 seconds ago with 60 second TTL
        let job = create_test_job("test-job", "default", 60);
        let now = Utc::now();

        // Should be cleaned up since it finished 120 seconds ago and TTL is 60 seconds
        let should_cleanup = controller.should_cleanup(&job, 60, now).await;
        assert!(should_cleanup);
    }

    #[tokio::test]
    async fn test_should_not_cleanup_recent_job() {
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let controller =
            TTLController::<rusternetes_storage::memory::MemoryStorage>::new(storage.clone());

        // Create a job that just finished with 3600 second TTL
        let mut job = create_test_job("test-job", "default", 3600);

        // Update the finish time to be very recent
        if let Some(ref mut status) = job.status {
            if let Some(ref mut conditions) = status.conditions {
                if let Some(condition) = conditions.first_mut() {
                    condition.last_transition_time = Some(Utc::now() - Duration::seconds(10));
                }
            }
        }

        let now = Utc::now();

        // Should NOT be cleaned up since it just finished and TTL is 1 hour
        let should_cleanup = controller.should_cleanup(&job, 3600, now).await;
        assert!(!should_cleanup);
    }
}
