//! Unit tests moved from `src/controllers/cronjob.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::service_account::ObjectReference;
    pub use rusternetes_common::resources::workloads::{CronJob, Job};
    pub use rusternetes_common::resources::{EventSource, EventType};
    pub use rusternetes_common::types::OwnerReference;
    pub use rusternetes_controller_manager::controllers::replicationcontroller::is_namespace_terminating_rejection;
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, extract_key, EventRecorder, Storage, WorkQueue};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use std::time::Duration;

    #[test]
    fn test_cron_schedule_parsing() {
        // Test that schedule patterns are recognized
        assert!("*/5 * * * *".starts_with("*/"));
        assert_eq!("@hourly", "@hourly");
        assert_eq!("@daily", "@daily");
    }

    #[test]
    fn test_job_name_generation() {
        let cronjob_name = "backup";
        let timestamp = 1234567890;
        let job_name = format!("{}-{}", cronjob_name, timestamp);
        assert_eq!(job_name, "backup-1234567890");
    }

    /// `spec.timeZone` must change the firing decision. Discriminating case:
    /// a daily-midnight schedule, last run at 02:00Z, evaluated at 06:00Z.
    ///   * UTC  → next midnight after 02:00Z is tomorrow 00:00Z → NOT due.
    ///   * America/New_York (EST, UTC-5) → last is 21:00 (prev day) local, next
    ///     local midnight is 00:00 EST = 05:00Z, which 06:00Z has passed → DUE.
    /// So the SAME inputs must fire under New York but not under UTC; an invalid
    /// zone must never fire (upstream UnknownTimeZone behaviour). This fails if
    /// the controller ignores `spec.timeZone` (evaluates everything as UTC).
    #[tokio::test]
    async fn should_run_now_honours_time_zone() {
        use std::sync::Arc;
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let ctrl =
            rusternetes_controller_manager::controllers::cronjob::CronJobController::new(storage);

        let now = chrono::DateTime::parse_from_rfc3339("2025-01-15T06:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let mk = |tz: serde_json::Value| -> rusternetes_common::resources::CronJob {
            serde_json::from_value(serde_json::json!({
                "apiVersion": "batch/v1", "kind": "CronJob",
                "metadata": {"name": "tz", "namespace": "default"},
                "spec": {
                    "schedule": "0 0 * * *",
                    "timeZone": tz,
                    "jobTemplate": {"spec": {"template": {"spec": {
                        "containers": [{"name": "c", "image": "busybox"}]
                    }}}},
                },
                "status": {"lastScheduleTime": "2025-01-15T02:00:00Z"},
            }))
            .unwrap()
        };

        let ny = mk(serde_json::json!("America/New_York"));
        assert!(
            ctrl.should_run_now("0 0 * * *", now, &ny).await.unwrap(),
            "New York local midnight has passed → must fire"
        );

        let utc = mk(serde_json::Value::Null);
        assert!(
            !ctrl.should_run_now("0 0 * * *", now, &utc).await.unwrap(),
            "UTC next midnight is tomorrow → must NOT fire"
        );

        let invalid = mk(serde_json::json!("Mars/Phobos"));
        assert!(
            !ctrl
                .should_run_now("0 0 * * *", now, &invalid)
                .await
                .unwrap(),
            "invalid timeZone → must not schedule"
        );
    }

    /// A grandfathered inline `TZ=`/`CRON_TZ=` schedule must parse and fire in
    /// that zone (robfig/cron v3 parser.go:95-103; formatSchedule keeps the
    /// inline zone over spec.timeZone, cronjob_controllerv2.go:766-773).
    /// An unknown inline zone must not schedule.
    #[tokio::test]
    async fn should_run_now_honours_inline_tz_prefix() {
        use std::sync::Arc;
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let ctrl =
            rusternetes_controller_manager::controllers::cronjob::CronJobController::new(storage);
        let now = chrono::DateTime::parse_from_rfc3339("2025-01-15T06:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let cj: rusternetes_common::resources::CronJob =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "batch/v1", "kind": "CronJob",
                "metadata": {"name": "tz", "namespace": "default"},
                "spec": {
                    "schedule": "x",
                    "jobTemplate": {"spec": {"template": {"spec": {
                        "containers": [{"name": "c", "image": "busybox"}]
                    }}}},
                },
                "status": {"lastScheduleTime": "2025-01-15T02:00:00Z"},
            }))
            .unwrap();
        assert!(ctrl
            .should_run_now("CRON_TZ=America/New_York 0 0 * * *", now, &cj)
            .await
            .unwrap());
        assert!(ctrl
            .should_run_now("TZ=America/New_York @daily", now, &cj)
            .await
            .unwrap());
        assert!(!ctrl
            .should_run_now("CRON_TZ=UTC 0 0 * * *", now, &cj)
            .await
            .unwrap());
        assert!(!ctrl
            .should_run_now("CRON_TZ=Mars/Phobos 0 0 * * *", now, &cj)
            .await
            .unwrap());
        assert!(!ctrl.should_run_now("TZ=UTC", now, &cj).await.unwrap());
    }

    /// syncCronJob / formatSchedule record Warning events for a bad schedule
    /// or zone (cronjob_controllerv2.go:507-526, :766-773). Reasons and
    /// message text are upstream's.
    #[tokio::test]
    async fn schedule_problems_record_upstream_events() {
        use rusternetes_common::resources::Event;
        use std::sync::Arc;
        type Mem = rusternetes_storage::memory::MemoryStorage;
        let now = chrono::DateTime::parse_from_rfc3339("2025-01-15T06:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let mk =
            |schedule: &str, tz: serde_json::Value| -> rusternetes_common::resources::CronJob {
                serde_json::from_value(serde_json::json!({
                    "apiVersion": "batch/v1", "kind": "CronJob",
                    "metadata": {"name": "cj", "namespace": "default", "uid": "u1"},
                    "spec": {
                        "schedule": schedule,
                        "timeZone": tz,
                        "jobTemplate": {"spec": {"template": {"spec": {
                            "containers": [{"name": "c", "image": "busybox"}]
                        }}}},
                    },
                }))
                .unwrap()
            };
        async fn events(storage: &Arc<Mem>) -> Vec<Event> {
            use rusternetes_storage::Storage;
            storage
                .list::<Event>("/registry/events/default/")
                .await
                .unwrap()
        }

        // Unparseable schedule -> UnparseableSchedule, `unparseable schedule: %q : %s`.
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let cj = mk("not a cron", serde_json::Value::Null);
        assert!(!ctrl.should_run_now("not a cron", now, &cj).await.unwrap());
        let evs = events(&storage).await;
        assert_eq!(evs.len(), 1, "{evs:?}");
        assert_eq!(evs[0].reason, "UnparseableSchedule");
        assert!(
            evs[0]
                .message
                .starts_with("unparseable schedule: \"not a cron\" : "),
            "{}",
            evs[0].message
        );
        assert_eq!(evs[0].involved_object.kind.as_deref(), Some("CronJob"));
        assert_eq!(evs[0].involved_object.name.as_deref(), Some("cj"));

        // TZ in schedule -> UnsupportedSchedule (and it still schedules).
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let s = "CRON_TZ=UTC 0 0 * * *";
        let cj = mk(s, serde_json::Value::Null);
        ctrl.should_run_now(s, now, &cj).await.unwrap();
        let evs = events(&storage).await;
        assert_eq!(evs.len(), 1, "{evs:?}");
        assert_eq!(evs[0].reason, "UnsupportedSchedule");
        assert_eq!(
            evs[0].message,
            "CRON_TZ or TZ used in schedule \"CRON_TZ=UTC 0 0 * * *\" is not officially supported, see https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/ for more details"
        );

        // Invalid spec.timeZone -> UnknownTimeZone, `invalid timeZone: %q: %s`.
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let cj = mk("0 0 * * *", serde_json::json!("Mars/Phobos"));
        assert!(!ctrl.should_run_now("0 0 * * *", now, &cj).await.unwrap());
        let evs = events(&storage).await;
        assert_eq!(evs.len(), 1, "{evs:?}");
        assert_eq!(evs[0].reason, "UnknownTimeZone");
        assert_eq!(
            evs[0].message,
            "invalid timeZone: \"Mars/Phobos\": unknown time zone Mars/Phobos"
        );

        // A valid schedule records nothing.
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let cj = mk("0 0 * * *", serde_json::Value::Null);
        ctrl.should_run_now("0 0 * * *", now, &cj).await.unwrap();
        assert!(events(&storage).await.is_empty());
    }

    fn cj_fixture(extra: serde_json::Value) -> rusternetes_common::resources::CronJob {
        let mut v = serde_json::json!({
            "apiVersion": "batch/v1", "kind": "CronJob",
            "metadata": {"name": "cj", "namespace": "default", "uid": "u1",
                "creationTimestamp": "2025-01-15T05:50:00Z"},
            "spec": {
                "schedule": "0 6 * * *",
                "jobTemplate": {"spec": {"template": {"spec": {
                    "containers": [{"name": "c", "image": "busybox"}]
                }}}},
            },
        });
        for (k, val) in extra.as_object().unwrap() {
            v["spec"][k] = val.clone();
        }
        serde_json::from_value(v).unwrap()
    }

    // ---- status.active sync (#2366) -------------------------------------
    // Ported from syncCronJob (cronjob_controllerv2.go:426-496) and
    // inActiveList / inActiveListByName (utils.go:58-77).

    type Mem = rusternetes_storage::memory::MemoryStorage;

    fn cj_json(
        schedule: &str,
        policy: &str,
        status: serde_json::Value,
    ) -> rusternetes_common::resources::CronJob {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "batch/v1", "kind": "CronJob",
            "metadata": {"name": "cj", "namespace": "default", "uid": "cj-uid",
                "creationTimestamp": (chrono::Utc::now() - chrono::Duration::minutes(10)).to_rfc3339()},
            "spec": {"schedule": schedule, "concurrencyPolicy": policy,
                "jobTemplate": {"spec": {"template": {"spec": {
                    "containers": [{"name": "c", "image": "busybox"}]}}}}},
            "status": status,
        }))
        .unwrap()
    }

    fn job_json(
        name: &str,
        uid: &str,
        owned: bool,
        finished: Option<&str>,
    ) -> rusternetes_common::resources::Job {
        let mut v = serde_json::json!({
            "apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": name, "namespace": "default", "uid": uid,
                "labels": {"cronjob-name": "cj"}},
            "spec": {"template": {"spec": {"containers": [{"name": "c", "image": "busybox"}]}}},
        });
        if owned {
            v["metadata"]["ownerReferences"] = serde_json::json!([{
                "apiVersion": "batch/v1", "kind": "CronJob", "name": "cj",
                "uid": "cj-uid", "controller": true}]);
        }
        if let Some(t) = finished {
            v["status"] = serde_json::json!({
                "conditions": [{"type": "Complete", "status": "True"}],
                "completionTime": t});
        }
        serde_json::from_value(v).unwrap()
    }

    /// utils.go:101 `earliestTime := cj.ObjectMeta.CreationTimestamp.Time`:
    /// with no lastScheduleTime the walk starts at creationTimestamp, not a
    /// "within the past minute" window. Created 05:50, now 06:02: the 06:00
    /// run is due.
    #[tokio::test]
    async fn first_run_starts_from_creation_timestamp() {
        use std::sync::Arc;
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let ctrl =
            rusternetes_controller_manager::controllers::cronjob::CronJobController::new(storage);
        let now = chrono::DateTime::parse_from_rfc3339("2025-01-15T06:02:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let cj = cj_fixture(serde_json::json!({}));
        let due = ctrl
            .scheduled_run_time("0 6 * * *", now, &cj)
            .await
            .unwrap();
        assert_eq!(
            due.unwrap().to_rfc3339(),
            "2025-01-15T06:00:00+00:00",
            "must fire the 06:00 run created after creationTimestamp"
        );
        // A run before creationTimestamp is never due.
        let early = chrono::DateTime::parse_from_rfc3339("2025-01-15T05:55:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert!(ctrl
            .scheduled_run_time("0 5 * * *", early, &cj)
            .await
            .unwrap()
            .is_none());
    }

    /// getJobFromTemplate2 (utils.go:254) sets
    /// batch.kubernetes.io/cronjob-scheduled-timestamp = scheduledTime.In(tz)
    /// formatted RFC3339 (`Z` for a zero offset).
    #[tokio::test]
    async fn job_gets_scheduled_timestamp_annotation() {
        use rusternetes_common::resources::Job;
        use rusternetes_storage::Storage;
        use std::sync::Arc;
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let t = chrono::DateTime::parse_from_rfc3339("2025-01-15T06:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let key = |cj: &str| {
            format!(
                "/registry/jobs/default/{}",
                rusternetes_controller_manager::controllers::cronjob::job_name_for(cj, t)
            )
        };
        const ANN: &str = "batch.kubernetes.io/cronjob-scheduled-timestamp";

        let cj = cj_fixture(serde_json::json!({}));
        assert!(ctrl.create_job(&cj, "default", t).await.unwrap().is_some());
        let job: Job = storage.get(&key("cj")).await.unwrap();
        assert_eq!(
            job.metadata.annotations.unwrap().get(ANN).unwrap(),
            "2025-01-15T06:00:00Z"
        );

        let mut cj = cj_fixture(serde_json::json!({"timeZone": "America/New_York"}));
        cj.metadata.name = "ny".into();
        ctrl.create_job(&cj, "default", t).await.unwrap();
        let job: Job = storage.get(&key("ny")).await.unwrap();
        assert_eq!(
            job.metadata.annotations.unwrap().get(ANN).unwrap(),
            "2025-01-15T01:00:00-05:00"
        );
    }

    /// cronjob_controllerv2.go:628-631: on AlreadyExists, a Job not
    /// controlled by this CronJob means another actor owns it and updates the
    /// status; we must NOT update the status (`return nil, updateStatus, nil`).
    #[tokio::test]
    async fn already_exists_uncontrolled_job_skips_status_update() {
        use rusternetes_common::resources::Job;
        use rusternetes_storage::Storage;
        use std::sync::Arc;
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let t = chrono::DateTime::parse_from_rfc3339("2025-01-15T06:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let cj = cj_fixture(serde_json::json!({}));
        // Foreign Job with the deterministic name, no ownerReference.
        let foreign: Job = serde_json::from_value(serde_json::json!({
            "apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": rusternetes_controller_manager::controllers::cronjob::job_name_for("cj", t), "namespace": "default", "uid": "foreign"},
            "spec": {"template": {"spec": {"containers": [{"name": "c", "image": "busybox"}]}}},
        }))
        .unwrap();
        storage
            .create(
                &format!("/registry/jobs/default/{}", foreign.metadata.name),
                &foreign,
            )
            .await
            .unwrap();
        assert!(
            ctrl.create_job(&cj, "default", t).await.unwrap().is_none(),
            "uncontrolled existing Job: no status update"
        );

        // Our own Job (controlled by uid u1) -> status update proceeds.
        let mut cj2 = cj_fixture(serde_json::json!({}));
        cj2.metadata.name = "mine".into();
        assert!(ctrl.create_job(&cj2, "default", t).await.unwrap().is_some());
        assert!(ctrl.create_job(&cj2, "default", t).await.unwrap().is_some());
    }

    fn job_ref(name: &str, uid: &str) -> serde_json::Value {
        serde_json::json!({"kind": "Job", "namespace": "default", "name": name,
            "uid": uid, "apiVersion": "batch/v1"})
    }

    async fn seed(
        storage: &std::sync::Arc<Mem>,
        cj: &rusternetes_common::resources::CronJob,
        jobs: &[rusternetes_common::resources::Job],
    ) -> rusternetes_common::resources::CronJob {
        use rusternetes_storage::Storage;
        storage
            .create("/registry/cronjobs/default/cj", cj)
            .await
            .unwrap();
        for j in jobs {
            storage
                .create(&format!("/registry/jobs/default/{}", j.metadata.name), j)
                .await
                .unwrap();
        }
        storage.get("/registry/cronjobs/default/cj").await.unwrap()
    }

    async fn reasons(storage: &std::sync::Arc<Mem>) -> Vec<String> {
        use rusternetes_common::resources::Event;
        use rusternetes_storage::Storage;
        let mut r: Vec<String> = storage
            .list::<Event>("/registry/events/default/")
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.reason)
            .collect();
        r.sort();
        r
    }

    /// Finished Job still in status.active: removed, SawCompletedJob recorded,
    /// lastSuccessfulTime taken from its completionTime (:450-471). The run is
    /// not due (yearly schedule), so nothing else touches status.
    #[tokio::test]
    async fn finished_job_leaves_active_and_sets_last_successful_time() {
        use rusternetes_storage::Storage;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let cj = cj_json(
            "0 0 1 1 *",
            "Allow",
            serde_json::json!({"active": [job_ref("done", "ju1")]}),
        );
        let mut cj = seed(
            &storage,
            &cj,
            &[job_json("done", "ju1", true, Some("2025-01-15T06:00:30Z"))],
        )
        .await;
        ctrl.reconcile(&mut cj).await.unwrap();
        let got: rusternetes_common::resources::CronJob =
            storage.get("/registry/cronjobs/default/cj").await.unwrap();
        let st = got.status.unwrap();
        assert!(st.active.is_empty(), "{:?}", st.active);
        assert_eq!(
            st.last_successful_time.unwrap().to_rfc3339(),
            "2025-01-15T06:00:30+00:00"
        );
        assert_eq!(reasons(&storage).await, vec!["SawCompletedJob"]);
    }

    /// status.active entry whose Job no longer exists: MissingJob, removed
    /// (:477-496), even when no run is due.
    #[tokio::test]
    async fn missing_job_is_removed_from_active() {
        use rusternetes_storage::Storage;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let cj = cj_json(
            "0 0 1 1 *",
            "Allow",
            serde_json::json!({"active": [job_ref("gone", "ju9")]}),
        );
        let mut cj = seed(&storage, &cj, &[]).await;
        ctrl.reconcile(&mut cj).await.unwrap();
        let got: rusternetes_common::resources::CronJob =
            storage.get("/registry/cronjobs/default/cj").await.unwrap();
        assert!(got.status.unwrap().active.is_empty());
        assert_eq!(reasons(&storage).await, vec!["MissingJob"]);
    }

    /// Forbid consults status.active, not a label scan: a label-only Job with
    /// no controllerRef is not ours (getJobsToBeReconciled :258-277) and does
    /// not block the run (:565-578 uses len(Status.Active)).
    #[tokio::test]
    async fn forbid_uses_status_active_not_label_scan() {
        use rusternetes_common::resources::Job;
        use rusternetes_storage::Storage;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let cj = cj_json("* * * * *", "Forbid", serde_json::json!({}));
        let mut cj = seed(&storage, &cj, &[job_json("stranger", "ju2", false, None)]).await;
        ctrl.reconcile(&mut cj).await.unwrap();
        let jobs: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
        assert_eq!(jobs.len(), 2, "run must not be blocked by a foreign Job");
        let got: rusternetes_common::resources::CronJob =
            storage.get("/registry/cronjobs/default/cj").await.unwrap();
        let active = got.status.unwrap().active;
        assert_eq!(active.len(), 1, "only the Job we created is active");
        assert_ne!(active[0].name.as_deref(), Some("stranger"));
    }

    /// An unfinished controlled Job missing from status.active is NOT adopted
    /// into it; UnexpectedJob warning only (:437-449).
    #[tokio::test]
    async fn unexpected_job_is_warned_not_adopted() {
        use rusternetes_storage::Storage;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let cj = cj_json("0 0 1 1 *", "Allow", serde_json::json!({}));
        let mut cj = seed(&storage, &cj, &[job_json("orphan", "ju3", true, None)]).await;
        ctrl.reconcile(&mut cj).await.unwrap();
        let got: rusternetes_common::resources::CronJob =
            storage.get("/registry/cronjobs/default/cj").await.unwrap();
        assert!(got.status.map(|s| s.active.is_empty()).unwrap_or(true));
        assert_eq!(reasons(&storage).await, vec!["UnexpectedJob"]);
    }

    // ---- Job event handlers (#2556) -------------------------------------

    /// `addJob` / `updateJob` / `deleteJob`
    /// (cronjob_controllerv2.go:279-362): a Job's controllerRef is resolved to
    /// its CronJob (:240-256, Kind + name + UID) and that CronJob is enqueued.
    #[tokio::test]
    async fn job_events_enqueue_the_owning_cronjob() {
        use rusternetes_storage::WatchEvent;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let job = job_json("j1", "j1-uid", true, None);
        seed(
            &storage,
            &cj_json("* * * * *", "Allow", serde_json::json!({})),
            std::slice::from_ref(&job),
        )
        .await;
        let body = serde_json::to_string(&job).unwrap();
        let k = "/registry/jobs/default/j1".to_string();
        for ev in [
            WatchEvent::Added(k.clone(), body.clone()),
            WatchEvent::Modified(k.clone(), body.clone()),
            WatchEvent::Deleted(k.clone(), body.clone()),
        ] {
            assert_eq!(
                ctrl.job_event_cronjob_keys(&mut Default::default(), &ev)
                    .await,
                vec!["cronjobs/default/cj".to_string()],
                "{ev:?}"
            );
        }
    }

    /// :357-361 / :240-256: orphans, a non-CronJob controller, a missing
    /// CronJob and a UID mismatch all enqueue nothing.
    #[tokio::test]
    async fn job_events_ignore_unresolvable_controller_refs() {
        use rusternetes_storage::WatchEvent;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        seed(
            &storage,
            &cj_json("* * * * *", "Allow", serde_json::json!({})),
            &[],
        )
        .await;
        let ev = |j: &rusternetes_common::resources::Job| {
            WatchEvent::Added(
                "/registry/jobs/default/j".into(),
                serde_json::to_string(j).unwrap(),
            )
        };
        let orphan = job_json("j", "u", false, None);
        assert_eq!(
            ctrl.job_event_cronjob_keys(&mut Default::default(), &ev(&orphan))
                .await,
            Vec::<String>::new()
        );
        let mut wrong_kind = job_json("j", "u", true, None);
        wrong_kind.metadata.owner_references.as_mut().unwrap()[0].kind = "Deployment".into();
        assert_eq!(
            ctrl.job_event_cronjob_keys(&mut Default::default(), &ev(&wrong_kind))
                .await,
            Vec::<String>::new()
        );
        let mut wrong_uid = job_json("j", "u", true, None);
        wrong_uid.metadata.owner_references.as_mut().unwrap()[0].uid = "other".into();
        assert_eq!(
            ctrl.job_event_cronjob_keys(&mut Default::default(), &ev(&wrong_uid))
                .await,
            Vec::<String>::new()
        );
        let mut missing = job_json("j", "u", true, None);
        missing.metadata.owner_references.as_mut().unwrap()[0].name = "gone".into();
        assert_eq!(
            ctrl.job_event_cronjob_keys(&mut Default::default(), &ev(&missing))
                .await,
            Vec::<String>::new()
        );
        let mut not_ctrl = job_json("j", "u", true, None);
        not_ctrl.metadata.owner_references.as_mut().unwrap()[0].controller = Some(false);
        assert_eq!(
            ctrl.job_event_cronjob_keys(&mut Default::default(), &ev(&not_ctrl))
                .await,
            Vec::<String>::new()
        );
    }

    // ---- CronJob / Job informer old-object handlers (#2607) -------------
    // Ported from TestControllerV2UpdateCronJob
    // (cronjob_controllerv2_test.go:1447-1671; the `@every` cases wait on
    // #2624) and updateJob (cronjob_controllerv2.go:303-331).

    fn cj_event(kind: &str, schedule: &str, tz: Option<&str>) -> rusternetes_storage::WatchEvent {
        use rusternetes_storage::WatchEvent;
        let mut cj = cj_fixture(serde_json::json!({"schedule": schedule}));
        cj.spec.time_zone = tz.map(str::to_string);
        // justBeforeTheHour (cronjob_controllerv2_test.go:150).
        cj.status = Some(Default::default());
        cj.status.as_mut().unwrap().last_schedule_time = Some(
            chrono::DateTime::parse_from_rfc3339("2016-05-19T09:59:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        );
        let k = "/registry/cronjobs/default/cj".to_string();
        let body = serde_json::to_string(&cj).unwrap();
        match kind {
            "add" => WatchEvent::Added(k, body),
            "del" => WatchEvent::Deleted(k, body),
            _ => WatchEvent::Modified(k, body),
        }
    }

    // justASecondBeforeTheHour (cronjob_controllerv2_test.go:105).
    fn just_a_second_before_the_hour() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2016-05-19T09:59:59Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    #[tokio::test]
    async fn update_cronjob_schedule_change_enqueues_after_next_slot() {
        use rusternetes_controller_manager::controllers::cronjob::CronJobEnqueue::*;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let now = just_a_second_before_the_hour();
        let key = || "cronjobs/default/cj".to_string();
        let delta = Duration::from_millis(100);
        type Spec = (&'static str, Option<&'static str>);
        let cases: Vec<(
            &str,
            Spec,
            Spec,
            rusternetes_controller_manager::controllers::cronjob::CronJobEnqueue,
        )> = vec![
            // "spec.schedule changed": 1s to the next minute + nextScheduleDelta.
            (
                "spec.schedule changed",
                ("30 * * * *", None),
                ("*/1 * * * *", None),
                After(key(), Duration::from_secs(1) + delta),
            ),
            // "spec.template changed": same schedule -> enqueueController.
            (
                "spec.template changed",
                ("*/1 * * * *", None),
                ("*/1 * * * *", None),
                Now(key()),
            ),
            (
                "spec.timeZone not changed",
                ("", Some("America/New_York")),
                ("", Some("America/New_York")),
                Now(key()),
            ),
            // Unparseable (here: empty) schedule after a change: event, no enqueue.
            (
                "spec.timeZone changed, unparseable schedule",
                ("", Some("America/New_York")),
                ("", None),
                Nothing,
            ),
        ];
        for (name, old, new, want) in cases {
            let mut cache =
                rusternetes_controller_manager::controllers::cronjob::CronJobSpecCache::new();
            // The first event only primes the cache.
            let o = cj_event("add", old.0, old.1);
            ctrl.cronjob_event_enqueue(&mut cache, &o, now).await;
            let n = cj_event("mod", new.0, new.1);
            assert_eq!(
                ctrl.cronjob_event_enqueue(&mut cache, &n, now).await,
                want,
                "{name}"
            );
        }
        assert_eq!(reasons(&storage).await, vec!["UnParseableCronJobSchedule"]);
    }

    /// addFunc / deleteFunc enqueue now (:121-132); a time-zone-only change
    /// re-aims the requeue (:397).
    #[tokio::test]
    async fn cronjob_add_delete_enqueue_now_and_timezone_change_requeues_after() {
        use rusternetes_controller_manager::controllers::cronjob::CronJobEnqueue::*;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let now = just_a_second_before_the_hour();
        let key = "cronjobs/default/cj".to_string();
        let mut cache =
            rusternetes_controller_manager::controllers::cronjob::CronJobSpecCache::new();
        let add = cj_event("add", "0 12 * * *", None);
        assert_eq!(
            ctrl.cronjob_event_enqueue(&mut cache, &add, now).await,
            Now(key.clone())
        );
        // 12:00 UTC -> 12:00 New York (16:00 UTC): a different next slot.
        let m = cj_event("mod", "0 12 * * *", Some("America/New_York"));
        match ctrl.cronjob_event_enqueue(&mut cache, &m, now).await {
            After(k, d) => {
                assert_eq!(k, key);
                // 09:59:59Z -> 16:00:00Z, plus nextScheduleDelta.
                assert_eq!(
                    d,
                    Duration::from_secs(6 * 3600 + 1) + Duration::from_millis(100)
                );
            }
            other => panic!("{other:?}"),
        }
        let del = cj_event("del", "0 12 * * *", None);
        assert_eq!(
            ctrl.cronjob_event_enqueue(&mut cache, &del, now).await,
            Now(key)
        );
        assert!(cache.is_empty());
    }

    /// updateJob (:312-320): a controllerRef change also wakes the OLD
    /// controller. The old ref comes from the cache a Modified event lacks.
    #[tokio::test]
    async fn job_controller_ref_change_wakes_old_and_new_controller() {
        use rusternetes_storage::{Storage, WatchEvent};
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        seed(
            &storage,
            &cj_json("* * * * *", "Allow", serde_json::json!({})),
            &[],
        )
        .await;
        let mut other = cj_json("* * * * *", "Allow", serde_json::json!({}));
        other.metadata.name = "other".into();
        other.metadata.uid = "other-uid".into();
        storage
            .create("/registry/cronjobs/default/other", &other)
            .await
            .unwrap();

        let k = "/registry/jobs/default/j".to_string();
        let owned = job_json("j", "ju", true, None);
        let mut moved = owned.clone();
        moved.metadata.owner_references.as_mut().unwrap()[0].name = "other".into();
        moved.metadata.owner_references.as_mut().unwrap()[0].uid = "other-uid".into();
        let ev = |j: &rusternetes_common::resources::Job| {
            WatchEvent::Modified(k.clone(), serde_json::to_string(j).unwrap())
        };
        let mut cache = rusternetes_controller_manager::controllers::cronjob::JobRefCache::new();
        // Seen as an add (or the initial list): owned by cj.
        let added = WatchEvent::Added(k.clone(), serde_json::to_string(&owned).unwrap());
        assert_eq!(
            ctrl.job_event_cronjob_keys(&mut cache, &added).await,
            vec!["cronjobs/default/cj".to_string()]
        );
        // Unchanged ref: only the owner.
        assert_eq!(
            ctrl.job_event_cronjob_keys(&mut cache, &ev(&owned)).await,
            vec!["cronjobs/default/cj".to_string()]
        );
        // Re-parented: old controller first, then the new one.
        assert_eq!(
            ctrl.job_event_cronjob_keys(&mut cache, &ev(&moved)).await,
            vec![
                "cronjobs/default/cj".to_string(),
                "cronjobs/default/other".to_string()
            ]
        );
        // Orphaned: the old controller (the new ref is nil, :323).
        let orphan = job_json("j", "ju", false, None);
        assert_eq!(
            ctrl.job_event_cronjob_keys(&mut cache, &ev(&orphan)).await,
            vec!["cronjobs/default/other".to_string()]
        );
        // Delete forgets the cache entry.
        let del = WatchEvent::Deleted(k, serde_json::to_string(&orphan).unwrap());
        ctrl.job_event_cronjob_keys(&mut cache, &del).await;
        assert!(cache.is_empty());
    }

    /// run() end to end (#2606 follow-up): a Job finishing reaches the
    /// CronJob through the Job informer, with no periodic resync (the old
    /// 10s timer is gone). The yearly schedule never fires, so only the Job
    /// event can clear status.active.
    #[tokio::test]
    async fn run_syncs_cronjob_when_its_job_finishes() {
        use rusternetes_storage::Storage;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = Arc::new(
            rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
                Arc::clone(&storage),
            ),
        );
        let cj = cj_json(
            "0 0 1 1 *",
            "Allow",
            serde_json::json!({"active": [job_ref("j", "ju")]}),
        );
        seed(&storage, &cj, &[job_json("j", "ju", true, None)]).await;
        let handle = tokio::spawn(Arc::clone(&ctrl).run());
        // Let the first sync and both watches establish.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let finished = job_json("j", "ju", true, Some("2025-01-15T06:00:30Z"));
        storage
            .update("/registry/jobs/default/j", &finished)
            .await
            .unwrap();
        let mut cleared = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let got: rusternetes_common::resources::CronJob =
                storage.get("/registry/cronjobs/default/cj").await.unwrap();
            if got.status.is_some_and(|s| s.active.is_empty()) {
                cleared = true;
                break;
            }
        }
        handle.abort();
        assert!(
            cleared,
            "Job completion did not reach the CronJob within 4s"
        );
    }

    // ---- requeueAfter (#2398) -------------------------------------------
    // Ported from TestNextScheduleTimeDuration
    // (pkg/controller/cronjob/utils_test.go:614-702).

    fn std_schedule(s: &str) -> rusternetes_controller_manager::controllers::cronjob::Schedule {
        rusternetes_controller_manager::controllers::cronjob::parse_standard_schedule(s).unwrap()
    }

    fn at(offset: chrono::Duration) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2016-05-19T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
            + offset
    }

    fn cj_at(
        schedule: &str,
        last: Option<chrono::Duration>,
    ) -> rusternetes_common::resources::CronJob {
        let mut cj = cj_fixture(serde_json::json!({"schedule": schedule}));
        cj.metadata.creation_timestamp = Some(at(chrono::Duration::zero()));
        if let Some(l) = last {
            cj.status = Some(Default::default());
            cj.status.as_mut().unwrap().last_schedule_time = Some(at(l));
        }
        cj
    }

    #[test]
    fn next_schedule_time_duration_matches_upstream_table() {
        use chrono::Duration as D;
        let delta = D::milliseconds(100); // nextScheduleDelta (:58)
        let cases = [
            (
                "complex schedule skipping weekend",
                "30 6-16/4 * * 1-5",
                Some(D::minutes(30)),
                D::hours(24) + D::minutes(31),
                D::hours(3) + D::minutes(59) + delta,
            ),
            (
                "another complex schedule skipping weekend",
                "30 10,11,12 * * 1-5",
                Some(D::minutes(30)),
                D::hours(30) + D::minutes(30),
                D::hours(66) + delta,
            ),
            (
                "once a week cronjob, missed two runs",
                "0 12 * * 4",
                Some(D::hours(2)),
                D::days(19) + D::hours(1) + D::minutes(30),
                D::hours(48) + D::minutes(30) + delta,
            ),
            (
                "no previous run of a cronjob",
                "0 12 * * 5",
                None,
                D::hours(6),
                D::hours(20) + delta,
            ),
        ];
        for (name, schedule, last, now, want) in cases {
            let cj = cj_at(schedule, last);
            let got = rusternetes_controller_manager::controllers::cronjob::next_schedule_duration(
                &std_schedule(schedule),
                chrono_tz::UTC,
                &cj,
                at(now),
            )
            .unwrap();
            assert_eq!(got, want.to_std().unwrap(), "{name}");
        }
    }

    /// `processNextWorkItem` (:176-185): a sync that returns requeueAfter is
    /// Forgotten and re-added after that delay; the sync with nothing due
    /// (`syncCronJob` :536-543) returns the time to the next slot.
    #[tokio::test]
    async fn sync_requeues_at_next_schedule_slot() {
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        // Yearly schedule, created 10 minutes ago: nothing due.
        let cj = cj_json("0 0 1 1 *", "Allow", serde_json::json!({}));
        let mut cj = seed(&storage, &cj, &[]).await;
        let after = ctrl.reconcile(&mut cj).await.unwrap();
        let after = after.expect("an unmet schedule must requeue at the next slot");
        assert!(after > std::time::Duration::from_secs(60), "{after:?}");

        // Suspended: upstream returns nil -> no requeue (:514-517).
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let mut sus = cj_json("* * * * *", "Allow", serde_json::json!({}));
        sus.spec.suspend = Some(true);
        let mut sus = seed(&storage, &sus, &[]).await;
        assert!(ctrl.reconcile(&mut sus).await.unwrap().is_none());
    }

    /// After starting a Job, the requeue is the next slot after the run just
    /// scheduled (:672-673).
    #[tokio::test]
    async fn sync_requeues_after_starting_a_job() {
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let cj = cj_json("* * * * *", "Allow", serde_json::json!({}));
        let mut cj = seed(&storage, &cj, &[]).await;
        let after = ctrl.reconcile(&mut cj).await.unwrap().unwrap();
        assert!(after <= std::time::Duration::from_secs(61), "{after:?}");
    }

    // ---- startingDeadlineSeconds / missed schedules (#2557) -------------
    // Ported from TestMostRecentScheduleTime (utils_test.go:348-612) and
    // TestNextScheduleTime (utils_test.go:150-283).

    fn cj_sds(
        schedule: &str,
        created: chrono::Duration,
        last: Option<chrono::Duration>,
        sds: Option<i64>,
    ) -> rusternetes_common::resources::CronJob {
        let mut cj = cj_fixture(serde_json::json!({"schedule": schedule}));
        cj.metadata.creation_timestamp = Some(at(created));
        cj.spec.starting_deadline_seconds = sds;
        if let Some(l) = last {
            cj.status = Some(Default::default());
            cj.status.as_mut().unwrap().last_schedule_time = Some(at(l));
        }
        cj
    }

    #[test]
    fn most_recent_schedule_time_matches_upstream_table() {
        use chrono::Duration as D;
        use rusternetes_controller_manager::controllers::cronjob::MissedSchedules::{
            Few, Many, None as NoneMissed,
        };
        let z = D::zero();
        let m = D::minutes;
        let h = D::hours;
        // (name, schedule, created, last, sds, includeSDS, now, earliest, recent, missed)
        #[allow(clippy::type_complexity)]
        let cases: Vec<(
            &str,
            &str,
            D,
            Option<D>,
            Option<i64>,
            bool,
            D,
            D,
            Option<D>,
            rusternetes_controller_manager::controllers::cronjob::MissedSchedules,
        )> = vec![
            (
                "now before next schedule",
                "0 * * * *",
                z,
                None,
                None,
                false,
                D::seconds(30),
                z,
                None,
                NoneMissed,
            ),
            (
                "now just after next schedule",
                "0 * * * *",
                z,
                None,
                None,
                false,
                m(61),
                z,
                Some(m(60)),
                NoneMissed,
            ),
            (
                "missed 5 schedules",
                "0 * * * *",
                D::seconds(10),
                None,
                None,
                false,
                m(301),
                D::seconds(10),
                Some(m(300)),
                Few,
            ),
            (
                "complex schedule",
                "30 6-16/4 * * 1-5",
                z,
                Some(m(30)),
                None,
                false,
                h(24) + m(31),
                m(30),
                Some(h(24) + m(30)),
                Few,
            ),
            (
                "another complex schedule",
                "30 10,11,12 * * 1-5",
                z,
                Some(m(30)),
                None,
                false,
                h(30) + m(30),
                m(30),
                None,
                Few,
            ),
            (
                "longer diff between executions",
                "30 6-16/4 * * 1-5",
                z,
                Some(m(30)),
                None,
                false,
                h(96) + m(31),
                m(30),
                Some(h(96) + m(30)),
                Few,
            ),
            (
                "shorter diff between executions",
                "30 6-16/4 * * 1-5",
                z,
                None,
                None,
                false,
                h(24) + m(31),
                z,
                Some(h(24) + m(30)),
                Few,
            ),
            (
                "earliestTime being CreationTimestamp and LastScheduleTime",
                "0 * * * *",
                z,
                Some(z),
                None,
                false,
                D::seconds(30),
                z,
                None,
                NoneMissed,
            ),
            (
                "earliestTime being LastScheduleTime",
                "*/5 * * * *",
                z,
                Some(m(30)),
                None,
                false,
                m(31),
                m(30),
                None,
                NoneMissed,
            ),
            (
                "earliestTime being LastScheduleTime (within StartingDeadlineSeconds)",
                "*/5 * * * *",
                z,
                Some(m(30)),
                Some(60),
                false,
                m(31),
                m(30),
                None,
                NoneMissed,
            ),
            (
                "earliestTime being LastScheduleTime (outside StartingDeadlineSeconds)",
                "*/5 * * * *",
                z,
                Some(m(30)),
                Some(60),
                true,
                m(32),
                m(31),
                None,
                NoneMissed,
            ),
            // The deadline raises earliestTime (utils.go:106-113), so no
            // backlog is counted: the next slot is after `now`.
            (
                "deadline bounds the missed count",
                "0 * * * *",
                z,
                Some(m(1)),
                Some(10),
                true,
                D::days(7) + m(5),
                D::days(7) + m(5) - D::seconds(10),
                None,
                NoneMissed,
            ),
            // utils_test.go:478-495 "@every schedule"; creation -59m is
            // overridden by lastScheduleTime (+1m).
            (
                "@every schedule",
                "@every 1h",
                m(-59),
                Some(m(1)),
                Some(10),
                false,
                D::days(7),
                m(1),
                Some(D::days(6) + h(23) + m(1)),
                Many,
            ),
        ];
        for (name, sched, created, last, sds, include, now, earliest, recent, missed) in cases {
            let cj = cj_sds(sched, created, last, sds);
            let (e, r, mm) =
                rusternetes_controller_manager::controllers::cronjob::most_recent_schedule_time(
                    &cj,
                    at(now),
                    &std_schedule(sched),
                    chrono_tz::UTC,
                    include,
                )
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(e, at(earliest), "{name}: earliest");
            assert_eq!(r, recent.map(at), "{name}: recent");
            assert_eq!(mm, missed, "{name}: missed");
        }
        // > 100 missed schedules and no deadline: manyMissed (utils.go:147-148).
        let cj = cj_sds("0 * * * *", z, None, None);
        let (_, r, mm) =
            rusternetes_controller_manager::controllers::cronjob::most_recent_schedule_time(
                &cj,
                at(D::days(7)),
                &std_schedule("0 * * * *"),
                chrono_tz::UTC,
                true,
            )
            .unwrap();
        assert_eq!(r, Some(at(D::days(7))));
        assert_eq!(mm, Many);
    }

    /// "rogue cronjob" (utils_test.go:496-510): a grammatically valid schedule
    /// that never fires errors (utils.go:118-121).
    #[test]
    fn rogue_schedule_errors() {
        let cj = cj_sds("59 23 31 2 *", chrono::Duration::seconds(10), None, None);
        let err = rusternetes_controller_manager::controllers::cronjob::most_recent_schedule_time(
            &cj,
            at(chrono::Duration::hours(1)),
            &std_schedule("59 23 31 2 *"),
            chrono_tz::UTC,
            false,
        )
        .unwrap_err();
        assert_eq!(
            err,
            "time difference between two schedules is less than 1 second"
        );
    }

    #[test]
    fn next_schedule_time_matches_upstream_cases() {
        use chrono::Duration as D;
        let s = "0 * * * *";
        let sch = std_schedule(s);
        let next = |cj: &rusternetes_common::resources::CronJob, now: D| {
            rusternetes_controller_manager::controllers::cronjob::next_schedule_time(
                cj,
                at(now),
                &sch,
                chrono_tz::UTC,
            )
        };
        // Case 1: nothing due yet.
        let cj = cj_sds(s, D::minutes(-10), None, None);
        assert_eq!(next(&cj, D::minutes(-7)).unwrap().0, None);
        // Case 2: one needed.
        assert_eq!(next(&cj, D::seconds(2)).unwrap().0, Some(at(D::zero())));
        // Case 3: known LastScheduleTime, no start needed.
        let cj = cj_sds(s, D::minutes(-10), Some(D::zero()), None);
        assert_eq!(next(&cj, D::minutes(2)).unwrap().0, None);
        // Case 4: known LastScheduleTime, a start needed.
        assert_eq!(
            next(&cj, D::hours(1) + D::minutes(5)).unwrap().0,
            Some(at(D::hours(1)))
        );
        // Case 5.
        let cj = cj_sds(s, D::hours(-2), Some(D::hours(-1)), None);
        assert_eq!(
            next(&cj, D::hours(1) + D::minutes(5)).unwrap().0,
            Some(at(D::hours(1)))
        );
        // Case 6: way ahead, no deadline.
        let now = D::hours(1) + D::days(10);
        assert!(next(&cj, now).unwrap().0.is_some());
        // Case 7: way ahead, short deadline.
        let cj = cj_sds(s, D::hours(-2), Some(D::hours(-1)), Some(7200));
        assert_eq!(next(&cj, now).unwrap().0, Some(at(now)));
        // Case 8: the error is propagated.
        let cj = cj_sds("59 23 31 2 *", D::seconds(10), None, None);
        let rogue = std_schedule("59 23 31 2 *");
        assert!(
            rusternetes_controller_manager::controllers::cronjob::next_schedule_time(
                &cj,
                at(D::hours(1)),
                &rogue,
                chrono_tz::UTC
            )
            .is_err()
        );
    }

    /// TooManyMissedTimes (utils.go:170-173), recorded as a Warning when more
    /// than 100 starts were missed and there is no deadline.
    #[tokio::test]
    async fn too_many_missed_times_records_warning() {
        use rusternetes_common::resources::Event;
        use rusternetes_storage::Storage;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let mut cj = cj_json("* * * * *", "Allow", serde_json::json!({}));
        cj.metadata.creation_timestamp = Some(chrono::Utc::now() - chrono::Duration::hours(10));
        let mut cj = seed(&storage, &cj, &[]).await;
        ctrl.reconcile(&mut cj).await.unwrap();
        let evs = storage
            .list::<Event>("/registry/events/default/")
            .await
            .unwrap();
        let ev = evs
            .iter()
            .find(|e| e.reason == "TooManyMissedTimes")
            .unwrap_or_else(|| panic!("no TooManyMissedTimes in {evs:?}"));
        assert_eq!(
            ev.message,
            "too many missed start times. Set or decrease .spec.startingDeadlineSeconds or check clock skew"
        );
    }

    /// With a deadline the same backlog is not "many" and records nothing.
    #[tokio::test]
    async fn deadline_suppresses_too_many_missed_times() {
        use rusternetes_common::resources::Event;
        use rusternetes_storage::Storage;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let mut cj = cj_json("* * * * *", "Allow", serde_json::json!({}));
        cj.metadata.creation_timestamp = Some(chrono::Utc::now() - chrono::Duration::hours(10));
        cj.spec.starting_deadline_seconds = Some(120);
        let mut cj = seed(&storage, &cj, &[]).await;
        ctrl.reconcile(&mut cj).await.unwrap();
        let evs = storage
            .list::<Event>("/registry/events/default/")
            .await
            .unwrap();
        assert!(
            evs.iter().all(|e| e.reason != "TooManyMissedTimes"),
            "{evs:?}"
        );
    }

    /// InvalidSchedule (cronjob_controllerv2.go:528-535):
    /// `invalid schedule: %s : %s`, and nothing is requeued.
    #[tokio::test]
    async fn invalid_schedule_records_event_and_does_not_requeue() {
        use rusternetes_common::resources::Event;
        use rusternetes_storage::Storage;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let mut cj = cj_json("59 23 31 2 *", "Allow", serde_json::json!({}));
        cj.metadata.creation_timestamp = Some(chrono::Utc::now() - chrono::Duration::hours(2));
        let mut cj = seed(&storage, &cj, &[]).await;
        assert!(ctrl.reconcile(&mut cj).await.unwrap().is_none());
        let evs = storage
            .list::<Event>("/registry/events/default/")
            .await
            .unwrap();
        let ev = evs.iter().find(|e| e.reason == "InvalidSchedule").unwrap();
        assert_eq!(
            ev.message,
            "invalid schedule: 59 23 31 2 * : time difference between two schedules is less than 1 second"
        );
    }

    /// `@every` is robfig's ConstantDelaySchedule (parser.go:424-431,
    /// constantdelay.go:13-26): sub-second delays round up to 1s, `Next`
    /// drops the nanoseconds of its input, bad durations are parse errors.
    #[test]
    fn every_descriptor_matches_robfig() {
        use chrono::{Duration as D, TimeZone};
        let next = |s: &str, t: chrono::DateTime<chrono::Utc>| {
            std_schedule(s).next(chrono_tz::UTC, t).unwrap()
        };
        let t = chrono::Utc.timestamp_opt(1_000, 500_000_000).unwrap();
        assert_eq!(
            next("@every 1h30m", t),
            chrono::Utc.timestamp_opt(1_000, 0).unwrap() + D::minutes(90)
        );
        assert_eq!(
            next("@every 100ms", t),
            chrono::Utc.timestamp_opt(1_001, 0).unwrap()
        );
        assert_eq!(
            next("@every 1500ms", t),
            chrono::Utc.timestamp_opt(1_001, 0).unwrap()
        );
        let e = rusternetes_controller_manager::controllers::cronjob::parse_standard_schedule(
            "@every nope",
        )
        .unwrap_err();
        assert_eq!(
            e,
            "failed to parse duration @every nope: time: invalid duration \"nope\""
        );
        assert!(
            rusternetes_controller_manager::controllers::cronjob::parse_standard_schedule("@every")
                .is_err()
        );
    }

    /// robfig/cron v3 dow bounds are {0, 6} (vendor/github.com/robfig/cron/v3/
    /// spec.go:40), so 7 and reversed ranges are parse errors
    /// (parser.go:304-309), including the `n-0` and `n-7` edges.
    #[test]
    fn day_of_week_bounds_match_robfig() {
        let err = |s: &str| {
            rusternetes_controller_manager::controllers::cronjob::parse_standard_schedule(s)
                .unwrap_err()
        };
        assert_eq!(err("0 0 * * 7"), "end of range (7) above maximum (6): 7");
        assert_eq!(
            err("0 0 * * 1-7"),
            "end of range (7) above maximum (6): 1-7"
        );
        assert_eq!(
            err("0 0 * * 0-7"),
            "end of range (7) above maximum (6): 0-7"
        );
        assert_eq!(
            err("0 0 * * 5-0"),
            "beginning of range (5) beyond end of range (0): 5-0"
        );
        assert_eq!(
            err("0 0 * * 1,6-2"),
            "beginning of range (6) beyond end of range (2): 6-2"
        );
        // Valid forms still parse, `N/step` meaning `N-6/step`.
        for ok in [
            "0 0 * * 0-6",
            "0 0 * * 4",
            "0 0 * * 1-5",
            "0 0 * * 4/2",
            "0 0 * * SUN-SAT",
        ] {
            assert!(
                rusternetes_controller_manager::controllers::cronjob::parse_standard_schedule(ok)
                    .is_ok(),
                "{ok}"
            );
        }
    }

    // ---- NamespaceTerminatingCause on create (#2398) --------------------

    /// Rejects every Job create with `message`. Everything else delegates.
    struct RejectJobCreate {
        inner: std::sync::Arc<Mem>,
        message: &'static str,
    }

    #[async_trait::async_trait]
    impl rusternetes_storage::Storage for RejectJobCreate {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            if key.starts_with("/registry/jobs/") {
                return Err(rusternetes_common::Error::Forbidden(
                    self.message.to_string(),
                ));
            }
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
        async fn update_raw(
            &self,
            key: &str,
            value: &serde_json::Value,
        ) -> rusternetes_common::Result<()> {
            self.inner.update_raw(key, value).await
        }
        // Events are written with a TTL (#2303); the default impl refuses ttl != 0.
        async fn create_with_ttl<T>(
            &self,
            key: &str,
            value: &T,
            ttl: u64,
        ) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.create_with_ttl(key, value, ttl).await
        }
        async fn update_with_ttl<T>(
            &self,
            key: &str,
            value: &T,
            ttl: u64,
        ) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.update_with_ttl(key, value, ttl).await
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

    /// cronjob_controllerv2.go:611-614: a create refused with
    /// NamespaceTerminatingCause returns the error WITHOUT the FailedCreate
    /// event (the event is in the default arm, :643-647); any other failure
    /// still records it.
    #[tokio::test]
    async fn namespace_terminating_create_records_no_failed_create() {
        use std::sync::Arc;
        for (message, want_event) in [
            (
                "POST /registry/jobs failed: (Forbidden) jobs.batch is forbidden: unable to \
                 create new content in namespace default because it is being terminated",
                false,
            ),
            ("(Forbidden) exceeded quota: count/jobs.batch", true),
        ] {
            let inner = Arc::new(Mem::new());
            let storage = Arc::new(RejectJobCreate {
                inner: Arc::clone(&inner),
                message,
            });
            let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
                Arc::clone(&storage),
            );
            let cj = cj_json("* * * * *", "Allow", serde_json::json!({}));
            let mut cj = seed(&inner, &cj, &[]).await;
            assert!(ctrl.reconcile(&mut cj).await.is_err());
            let got = reasons(&inner).await.contains(&"FailedCreate".to_string());
            assert_eq!(got, want_event, "{message}");
        }
    }

    // ---- history limits (#2398) -----------------------------------------

    /// cleanupFinishedJobs (:684-686): with BOTH limits nil nothing is
    /// deleted -- defaulting is the API server's job (SetDefaults_CronJob,
    /// pkg/apis/batch/v1/defaults.go:83-88), not the controller's.
    #[tokio::test]
    async fn cleanup_does_nothing_when_both_limits_nil() {
        use rusternetes_common::resources::Job;
        use rusternetes_storage::Storage;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = rusternetes_controller_manager::controllers::cronjob::CronJobController::new(
            Arc::clone(&storage),
        );
        let jobs: Vec<_> = (0..5)
            .map(|i| {
                job_json(
                    &format!("j{i}"),
                    &format!("u{i}"),
                    true,
                    Some("2025-01-01T00:00:00Z"),
                )
            })
            .collect();
        let cj = cj_json("0 0 1 1 *", "Allow", serde_json::json!({}));
        let mut cj = seed(&storage, &cj, &jobs).await;
        assert!(cj.spec.successful_jobs_history_limit.is_none());
        assert!(!ctrl.cleanup_finished_jobs(&mut cj, &jobs).await);
        let left: Vec<Job> = storage.list("/registry/jobs/default/").await.unwrap();
        assert_eq!(left.len(), 5, "no limit set -> no Job deleted");
    }
}
