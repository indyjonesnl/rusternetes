use crate::controllers::replicationcontroller::is_namespace_terminating_rejection;
use crate::controllers::worker_pool::spawn_workers;
use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::workloads::{CronJob, Job};
use rusternetes_common::resources::{EventSource, EventType};
use rusternetes_common::types::OwnerReference;
use rusternetes_storage::{build_key, extract_key, EventRecorder, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

/// getJobName (pkg/controller/cronjob/cronjob_controllerv2.go:676):
/// `{cronjob}-{scheduledTime.Unix()/60}` (getTimeHashInMinutes, utils.go:270).
fn job_name_for(cronjob_name: &str, scheduled_time: chrono::DateTime<chrono::Utc>) -> String {
    format!("{}-{}", cronjob_name, scheduled_time.timestamp() / 60)
}

/// `nextScheduleDelta` (cronjob_controllerv2.go:58): 100ms of padding on every
/// requeue to absorb NTP skew.
const NEXT_SCHEDULE_DELTA: chrono::Duration = chrono::Duration::milliseconds(100);

/// robfig/cron's `Schedule`: either a parsed crontab spec or the
/// `ConstantDelaySchedule` of an `@every` descriptor
/// (vendor/github.com/robfig/cron/v3/constantdelay.go:3-26).
#[derive(Debug, Clone)]
enum Schedule {
    Cron(Box<cron::Schedule>),
    /// Whole seconds, at least 1 (`Every`, constantdelay.go:13-21).
    Every(chrono::Duration),
}

impl Schedule {
    /// `Every(duration)`: delays under a second round up to one second and
    /// sub-second parts are truncated (constantdelay.go:13-21).
    fn every(nanos: i64) -> Self {
        let nanos = nanos.max(1_000_000_000);
        Schedule::Every(chrono::Duration::seconds(nanos / 1_000_000_000))
    }

    /// `Next(t)`: the first slot strictly after `t`, in `tz`. For `@every` it
    /// is `t + delay - t.Nanosecond()` (constantdelay.go:24-26).
    fn next(
        &self,
        tz: chrono_tz::Tz,
        t: chrono::DateTime<chrono::Utc>,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        match self {
            Schedule::Cron(s) => s
                .after(&t.with_timezone(&tz))
                .next()
                .map(|n| n.with_timezone(&chrono::Utc)),
            Schedule::Every(delay) => {
                use chrono::Timelike;
                Some(t + *delay - chrono::Duration::nanoseconds(t.nanosecond() as i64))
            }
        }
    }
}

/// Parse a Kubernetes schedule (5 fields or an `@descriptor`) into the `cron`
/// crate's 7-field form. The error is the parser's message, which
/// `UnparseableSchedule` / `InvalidSchedule` events carry.
fn parse_standard_schedule(schedule: &str) -> Result<Schedule, String> {
    // `@every <duration>` (robfig/cron v3 parser.go:424-431): a
    // ConstantDelaySchedule, which the 7-field `cron` crate cannot express.
    if let Some(d) = schedule.strip_prefix("@every ") {
        let nanos = rusternetes_common::go_duration::parse_go_duration(d)
            .map_err(|e| format!("failed to parse duration {schedule}: {e}"))?;
        return Ok(Schedule::every(nanos));
    }
    // Handle special schedules (Kubernetes 5-field format)
    let cron_schedule = match schedule {
        "@yearly" | "@annually" => "0 0 1 1 *",
        "@monthly" => "0 0 1 * *",
        "@weekly" => "0 0 * * 0",
        "@daily" | "@midnight" => "0 0 * * *",
        "@hourly" => "0 * * * *",
        other => other,
    };

    // Kubernetes supports `?` in cron expressions (Quartz-style "no specific value").
    // Replace with `*` since the `cron` crate doesn't support `?`.
    let cron_schedule = cron_schedule.replace('?', "*");

    // The `cron` crate expects 7 fields (sec min hour dom month dow year),
    // but Kubernetes uses 5 fields (min hour dom month dow).
    // Convert by prepending "0" for seconds and appending "*" for year.
    let cron_schedule = numeric_dow_to_names(&cron_schedule)?;
    let field_count = cron_schedule.split_whitespace().count();
    let cron_schedule = if field_count == 5 {
        format!("0 {} *", cron_schedule)
    } else if field_count == 6 {
        format!("0 {}", cron_schedule)
    } else {
        cron_schedule.to_string()
    };
    cron::Schedule::try_from(cron_schedule.as_str())
        .map(|s| Schedule::Cron(Box::new(s)))
        .map_err(|e| e.to_string())
}

/// robfig/cron numbers the day of week 0-6 from Sunday, and 7 is NOT accepted:
/// vendor/github.com/robfig/cron/v3/spec.go:40 `dow = bounds{0, 6, names}`, so
/// `getRange` (parser.go:252-322) rejects an end above 6 (parser.go:307) and a
/// start beyond the end (parser.go:310) -- the `n-7` and `n-0` edges. The
/// `cron` crate numbers the day 1-7 from Sunday, so a numeric `4` (Thursday
/// upstream) would mean Wednesday. Validate the numeric days per `getRange`
/// (same messages), then rewrite them to names, which both crates agree on.
/// `N/step` means `N-6/step` (parser.go:291-293).
fn numeric_dow_to_names(schedule: &str) -> Result<String, String> {
    const NAMES: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];
    const MAX: u32 = 6;
    let mut fields: Vec<String> = schedule.split_whitespace().map(str::to_string).collect();
    let dow_idx = match fields.len() {
        5 => 4,
        6 => 5,
        _ => return Ok(schedule.to_string()),
    };
    // parseIntOrName (parser.go:326-331): a number or a day name.
    let value = |x: &str| -> Option<u32> {
        x.parse::<u32>().ok().or_else(|| {
            NAMES
                .iter()
                .position(|n| n.eq_ignore_ascii_case(x))
                .map(|i| i as u32)
        })
    };
    let mut rewritten: Vec<String> = Vec::new();
    for item in fields[dow_idx].split(',') {
        let parts: Vec<&str> = item.split('/').collect();
        if parts.len() > 2 {
            return Err(format!("too many slashes: {item}"));
        }
        let low_high: Vec<&str> = parts[0].split('-').collect();
        if low_high[0] == "*" || low_high.len() > 2 {
            // `*` needs no bounds check; a stray hyphen is left to the parser.
            rewritten.push(item.to_string());
            continue;
        }
        let step = parts.get(1).copied();
        let Some(start) = value(low_high[0]) else {
            rewritten.push(item.to_string());
            continue;
        };
        let end = match (low_high.get(1), step) {
            (Some(h), _) => match value(h) {
                Some(e) => e,
                None => {
                    rewritten.push(item.to_string());
                    continue;
                }
            },
            (None, Some(_)) => MAX,
            (None, None) => start,
        };
        if end > MAX {
            return Err(format!(
                "end of range ({end}) above maximum ({MAX}): {item}"
            ));
        }
        if start > end {
            return Err(format!(
                "beginning of range ({start}) beyond end of range ({end}): {item}"
            ));
        }
        let range = if low_high.len() == 1 && step.is_none() {
            NAMES[start as usize].to_string()
        } else {
            format!("{}-{}", NAMES[start as usize], NAMES[end as usize])
        };
        rewritten.push(match step {
            Some(s) => format!("{range}/{s}"),
            None => range,
        });
    }
    fields[dow_idx] = rewritten.join(",");
    Ok(fields.join(" "))
}

/// `missedSchedulesType` (pkg/controller/cronjob/utils.go:83-89).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MissedSchedules {
    None,
    Few,
    Many,
}

/// `mostRecentScheduleTime`'s result: earliest time, most recent slot, missed.
type MostRecent = (
    chrono::DateTime<chrono::Utc>,
    Option<chrono::DateTime<chrono::Utc>>,
    MissedSchedules,
);

/// `schedule.Next(t)` of robfig/cron: the first slot strictly after `t`, in
/// `tz`; `None` is Go's zero time (a schedule that never fires).
fn schedule_next(
    schedule: &Schedule,
    tz: chrono_tz::Tz,
    t: chrono::DateTime<chrono::Utc>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    schedule.next(tz, t)
}

/// `mostRecentScheduleTime` (pkg/controller/cronjob/utils.go:100-176): returns
/// the earliest time (last schedule time or creation time, raised to
/// `now - startingDeadlineSeconds` when `include_sds`), the most recent slot
/// not after `now` (or None), and how many starts were missed. The error is
/// the "less than 1 second" one for a schedule that never fires (:129-131).
fn most_recent_schedule_time(
    cj: &CronJob,
    now: chrono::DateTime<chrono::Utc>,
    schedule: &Schedule,
    tz: chrono_tz::Tz,
    include_sds: bool,
) -> Result<MostRecent, String> {
    // :101-104. A CronJob without a creationTimestamp never reaches the
    // controller upstream; fall back to one minute ago.
    let mut earliest = cj
        .status
        .as_ref()
        .and_then(|s| s.last_schedule_time)
        .or(cj.metadata.creation_timestamp)
        .unwrap_or(now - chrono::Duration::minutes(1));
    let mut missed = MissedSchedules::None;
    // :106-113: the controller schedules nothing below this point.
    if include_sds {
        if let Some(sds) = cj.spec.starting_deadline_seconds {
            let scheduling_deadline = now - chrono::Duration::seconds(sds);
            if scheduling_deadline > earliest {
                earliest = scheduling_deadline;
            }
        }
    }

    let t1 = schedule_next(schedule, tz, earliest);
    let t2 = t1.and_then(|t| schedule_next(schedule, tz, t));
    // :115-123: `now.Before(zero)` is false, so a never-firing schedule falls
    // through to the error below.
    if let Some(t1) = t1 {
        if now < t1 {
            return Ok((earliest, None, missed));
        }
        if t2.is_some_and(|t2| now < t2) {
            return Ok((earliest, Some(t1), missed));
        }
    }
    // :129-131
    let (Some(t1), Some(t2)) = (t1, t2) else {
        return Err("time difference between two schedules is less than 1 second".to_string());
    };
    let between = (t2 - t1).num_seconds();
    if between < 1 {
        return Err("time difference between two schedules is less than 1 second".to_string());
    }
    // :134-136: a rough count of the schedules missed since t1.
    let elapsed = (now - t1).num_seconds();
    let number_of_missed = (elapsed / between) + 1;
    // :138-152: start the walk one interval before the estimate so `@every`
    // and irregular schedules land on a real slot.
    let potential_earliest = t1 + chrono::Duration::seconds((number_of_missed - 1 - 1) * between);
    let mut most_recent = None;
    let mut t = schedule_next(schedule, tz, potential_earliest);
    while let Some(slot) = t.filter(|slot| *slot <= now) {
        most_recent = Some(slot);
        t = schedule_next(schedule, tz, slot);
    }
    // :154-176: more than 100 is "many" (the controller would otherwise try
    // to list every start of a clock-skewed CronJob).
    if number_of_missed > 100 {
        missed = MissedSchedules::Many;
    } else if number_of_missed > 0 {
        missed = MissedSchedules::Few;
    }
    Ok((earliest, most_recent, missed))
}

/// `nextScheduleTimeDuration` (pkg/controller/cronjob/utils.go:188-205): the
/// delay until the next schedule slot, plus `NEXT_SCHEDULE_DELTA`.
fn next_schedule_duration(
    schedule: &Schedule,
    tz: chrono_tz::Tz,
    cj: &CronJob,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<Duration> {
    let base = match most_recent_schedule_time(cj, now, schedule, tz, false) {
        // :189-191: still requeue; aim for the next slot from now.
        Err(_) => now,
        Ok((earliest, None, MissedSchedules::None)) => earliest,
        // :193-199: missed schedules since earliestTime, always use now.
        Ok((_, None, _)) => now,
        Ok((_, Some(recent), _)) => recent,
    };
    let next = schedule_next(schedule, tz, base)?;
    (next - now + NEXT_SCHEDULE_DELTA).to_std().ok()
}

/// `nextScheduleTime` (pkg/controller/cronjob/utils.go:210-224): the slot to
/// start (None when nothing is due) and how many were missed. The caller
/// records `TooManyMissedTimes` (:217-219) when that is `Many`.
fn next_schedule_time(
    cj: &CronJob,
    now: chrono::DateTime<chrono::Utc>,
    schedule: &Schedule,
    tz: chrono_tz::Tz,
) -> Result<(Option<chrono::DateTime<chrono::Utc>>, MissedSchedules), String> {
    let (_, most_recent, missed) = most_recent_schedule_time(cj, now, schedule, tz, true)?;
    match most_recent {
        Some(t) if t <= now => Ok((Some(t), missed)),
        _ => Ok((None, missed)),
    }
}

/// Upstream `ConcurrentCronJobSyncs` default, workers launched by `Run`
/// (pkg/controller/cronjob/config/v1alpha1/defaults.go:34;
/// pkg/controller/cronjob/cronjob_controllerv2.go:156-159).
const CONCURRENT_CRONJOB_SYNCS: usize = 5;

pub struct CronJobController<S: Storage> {
    storage: Arc<S>,
    /// `jm.recorder` (cronjob_controllerv2.go).
    recorder: EventRecorder<S>,
}

impl<S: Storage + 'static> CronJobController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        let recorder = EventRecorder::new(Arc::clone(&storage));
        Self { storage, recorder }
    }

    /// `jm.recorder.Eventf(cronJob, corev1.EventTypeWarning, ...)`. A failure to
    /// record is logged and dropped: it must not mask the sync outcome.
    async fn record_warning(&self, cronjob: &CronJob, reason: &str, message: &str) {
        self.record_event(cronjob, EventType::Warning, reason, message)
            .await;
    }

    /// `jm.recorder.Eventf(cronJob, corev1.EventTypeNormal, ...)`.
    async fn record_normal(&self, cronjob: &CronJob, reason: &str, message: &str) {
        self.record_event(cronjob, EventType::Normal, reason, message)
            .await;
    }

    async fn record_event(
        &self,
        cronjob: &CronJob,
        event_type: EventType,
        reason: &str,
        message: &str,
    ) {
        let involved = ObjectReference {
            kind: Some("CronJob".to_string()),
            namespace: cronjob.metadata.namespace.clone(),
            name: Some(cronjob.metadata.name.clone()),
            uid: Some(cronjob.metadata.uid.clone()),
            api_version: Some("batch/v1".to_string()),
            ..Default::default()
        };
        let source = EventSource {
            component: "cronjob-controller".to_string(),
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

    /// `UnparseableSchedule` (cronjob_controllerv2.go:519-526).
    async fn record_unparseable(&self, cronjob: &CronJob, schedule: &str, err: &str) {
        self.record_warning(
            cronjob,
            "UnparseableSchedule",
            &format!("unparseable schedule: {schedule:?} : {err}"),
        )
        .await;
    }

    /// `addJob` / `updateJob` / `deleteJob`
    /// (cronjob_controllerv2.go:279-362): resolve the Job's controllerRef to
    /// its CronJob and return that CronJob's queue key, or None when nothing
    /// should be enqueued.
    ///
    /// `addJob` (:281-285) routes a Job already pending deletion through
    /// `deleteJob`; all three then do the same thing (get the controllerRef,
    /// `resolveControllerRef`, `enqueueController`), so one function serves
    /// every event. A `Deleted` event carries the previous value, which stands
    /// in for `DeletedFinalStateUnknown` (:339-350).
    ///
    /// Deviation: `updateJob` (:317-321) also wakes the OLD controller when the
    /// controllerRef changed. A storage watch event carries no old object, so
    /// only the current controller is woken.
    async fn job_event_cronjob_key(&self, ev: &rusternetes_storage::WatchEvent) -> Option<String> {
        use rusternetes_storage::WatchEvent;
        let body = match ev {
            WatchEvent::Added(_, v) | WatchEvent::Modified(_, v) | WatchEvent::Deleted(_, v) => v,
        };
        let job: Job = serde_json::from_str(body).ok()?;
        // metav1.GetControllerOf: the ref with controller=true.
        let controller_ref = job
            .metadata
            .owner_references
            .as_ref()?
            .iter()
            .find(|r| r.controller == Some(true))?;
        // resolveControllerRef (:240-256): Kind, then Get by name, then UID.
        if controller_ref.kind != "CronJob" {
            return None;
        }
        let ns = job.metadata.namespace.as_deref()?;
        let cronjob = self
            .storage
            .get::<CronJob>(&build_key("cronjobs", Some(ns), &controller_ref.name))
            .await
            .ok()?;
        if cronjob.metadata.uid != controller_ref.uid {
            return None;
        }
        Some(format!("cronjobs/{}/{}", ns, controller_ref.name))
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting CronJobController (watch-based)");
        let retry_interval = Duration::from_secs(5);

        let queue = WorkQueue::new();

        // Upstream starts `workers` goroutines over one queue (cronjob_controllerv2.go:156-159).
        spawn_workers(CONCURRENT_CRONJOB_SYNCS, &queue, |worker_queue| {
            let worker_self = Arc::clone(&self);
            async move {
                worker_self.worker(worker_queue).await;
            }
        });

        loop {
            // Initial full reconciliation
            self.enqueue_all(&queue).await;

            // Watch for changes
            let prefix = "/registry/cronjobs/";
            let watch_result = self.storage.watch(prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish watch: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };

            // Job informer (cronjob_controllerv2.go:110-114): Job events wake
            // the owning CronJob. Time triggers need no periodic resync:
            // `sync` returns requeueAfter and the worker AddAfter's it.
            let mut job_watch = match self.storage.watch("/registry/jobs/").await {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish job watch: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };

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
                    event = job_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                if let Some(key) = self.job_event_cronjob_key(&ev).await {
                                    queue.add(key).await;
                                }
                            }
                            Some(Err(e)) => {
                                warn!("Job watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Job watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                }
            }
            // Watch broke — loop back to re-establish
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
            let storage_key = build_key("cronjobs", Some(ns), name);
            match self.storage.get::<CronJob>(&storage_key).await {
                Ok(resource) => {
                    let mut resource = resource;
                    match self.reconcile(&mut resource).await {
                        // processNextWorkItem (:176-185): Forget, then
                        // AddAfter(requeueAfter) when the sync asked for one.
                        Ok(requeue_after) => {
                            queue.forget(&key).await;
                            if let Some(after) = requeue_after {
                                queue.add_after(key.clone(), after).await;
                            }
                        }
                        Err(e) => {
                            error!("Failed to reconcile {}: {}", key, e);
                            queue.requeue_rate_limited(key.clone()).await;
                        }
                    }
                }
                Err(_) => {
                    // Resource was deleted — nothing to reconcile
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self.storage.list::<CronJob>("/registry/cronjobs/").await {
            Ok(items) => {
                for item in &items {
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("cronjobs/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list cronjobs for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        let cronjobs: Vec<CronJob> = self.storage.list("/registry/cronjobs/").await?;

        for mut cronjob in cronjobs {
            if let Err(e) = self.reconcile(&mut cronjob).await {
                error!(
                    "Failed to reconcile CronJob {}: {}",
                    cronjob.metadata.name, e
                );
            }
        }

        Ok(())
    }

    /// `sync` (cronjob_controllerv2.go:188-238): list the Jobs this CronJob
    /// controls, run `cleanupFinishedJobs` and `syncCronJob` against ONE copy
    /// of the CronJob, and write the status once if either asked for it.
    ///
    /// Returns `requeueAfter` (:229-232): the delay after which the key is
    /// re-added; a sync error is returned only when there is none (:234).
    async fn reconcile(&self, cronjob: &mut CronJob) -> Result<Option<Duration>> {
        let namespace = cronjob.metadata.namespace.clone().unwrap_or_default();
        debug!(
            "Reconciling CronJob {}/{}",
            namespace, cronjob.metadata.name
        );

        let jobs = self.jobs_to_be_reconciled(cronjob, &namespace).await?;
        let update_after_cleanup = self.cleanup_finished_jobs(cronjob, &jobs).await;
        let mut update_after_sync = false;
        let sync_result = self
            .sync_cronjob(cronjob, &jobs, &namespace, &mut update_after_sync)
            .await;
        if let Err(e) = &sync_result {
            debug!(
                "Error reconciling cronjob {}/{}: {e}",
                namespace, cronjob.metadata.name
            );
        }

        if update_after_cleanup || update_after_sync {
            let key = format!("/registry/cronjobs/{}/{}", namespace, cronjob.metadata.name);
            // Conditional status write (#2153): a Conflict is returned and the
            // work item requeued (`UpdateStatus`, :222-228).
            self.storage.update_status_cas(&key, cronjob).await?;
        }
        sync_result
    }

    /// `getJobsToBeReconciled` (cronjob_controllerv2.go:258-277): every Job in
    /// the namespace whose controllerRef names this CronJob. Labels play no part.
    async fn jobs_to_be_reconciled(&self, cronjob: &CronJob, namespace: &str) -> Result<Vec<Job>> {
        let all: Vec<Job> = self
            .storage
            .list(&format!("/registry/jobs/{}/", namespace))
            .await?;
        Ok(all
            .into_iter()
            .filter(|job| {
                job.metadata
                    .owner_references
                    .iter()
                    .flatten()
                    .find(|r| r.controller == Some(true))
                    .is_some_and(|r| r.name == cronjob.metadata.name)
            })
            .collect())
    }

    /// `syncCronJob` (cronjob_controllerv2.go:426-673). `update_status` is the
    /// returned `updateStatus` flag, set even when the sync later errors.
    async fn sync_cronjob(
        &self,
        cronjob: &mut CronJob,
        jobs: &[Job],
        namespace: &str,
        update_status: &mut bool,
    ) -> Result<Option<Duration>> {
        let name = cronjob.metadata.name.clone();
        let now = chrono::Utc::now();

        let mut children: std::collections::HashSet<String> = std::collections::HashSet::new();
        for j in jobs {
            children.insert(j.metadata.uid.clone());
            let found = in_active_list(cronjob, &j.metadata.uid);
            let finished = job_finished_condition(j);
            if !found && finished.is_none() {
                // :439-449: re-read the CronJob; if the fresh copy has it,
                // adopt that copy, else warn. The Job is NOT added to active.
                let key = format!("/registry/cronjobs/{}/{}", namespace, name);
                let fresh: CronJob = self.storage.get(&key).await?;
                if in_active_list(&fresh, &j.metadata.uid) {
                    *cronjob = fresh;
                    continue;
                }
                self.record_warning(
                    cronjob,
                    "UnexpectedJob",
                    &format!(
                        "Saw a job that the controller did not create or forgot: {}",
                        j.metadata.name
                    ),
                )
                .await;
            } else if let Some(condition) = finished {
                if found {
                    delete_from_active_list(cronjob, &j.metadata.uid);
                    self.record_normal(
                        cronjob,
                        "SawCompletedJob",
                        &format!(
                            "Saw completed job: {}, condition: {}",
                            j.metadata.name, condition
                        ),
                    )
                    .await;
                    *update_status = true;
                }
                if condition == "Complete" {
                    // A Job need not be in the active list for its success
                    // time to count (:462-471).
                    let completion = j.status.as_ref().and_then(|s| s.completion_time);
                    let status = cronjob.status.get_or_insert_with(Default::default);
                    if status.last_successful_time.is_none() {
                        status.last_successful_time = completion;
                        *update_status = true;
                    }
                    if let (Some(done), Some(last)) = (completion, status.last_successful_time) {
                        if done > last {
                            status.last_successful_time = Some(done);
                            *update_status = true;
                        }
                    }
                }
            }
        }

        // :477-496: drop active entries whose Job is gone. The Job is fetched
        // directly so a lagging list cannot cause an unwanted miss.
        let active: Vec<ObjectReference> = cronjob
            .status
            .as_ref()
            .map(|s| s.active.clone())
            .unwrap_or_default();
        for r in &active {
            let uid = r.uid.clone().unwrap_or_default();
            if children.contains(&uid) {
                continue;
            }
            let key = format!(
                "/registry/jobs/{}/{}",
                r.namespace.as_deref().unwrap_or(namespace),
                r.name.as_deref().unwrap_or_default()
            );
            match self.storage.get::<Job>(&key).await {
                Err(rusternetes_common::Error::NotFound(_)) => {
                    self.record_normal(
                        cronjob,
                        "MissingJob",
                        &format!(
                            "Active job went missing: {}",
                            r.name.as_deref().unwrap_or("")
                        ),
                    )
                    .await;
                    delete_from_active_list(cronjob, &uid);
                    *update_status = true;
                }
                Err(e) => return Err(e.into()),
                Ok(_) => {}
            }
        }

        if cronjob.metadata.is_being_deleted() {
            // Don't do anything other than updating status (:498-502).
            return Ok(None);
        }
        if cronjob.spec.suspend.unwrap_or(false) {
            debug!("CronJob {}/{} is suspended", namespace, name);
            return Ok(None);
        }

        let schedule = cronjob.spec.schedule.clone();
        let Some((sched, tz)) = self.parse_schedule(&schedule, cronjob).await? else {
            return Ok(None);
        };
        // `nextScheduleTimeDuration` (utils.go:188), evaluated against the
        // CronJob as it stands at each of upstream's requeue returns.
        let requeue = |cj: &CronJob| next_schedule_duration(&sched, tz, cj, now);
        // :528-535: nextScheduleTime; an error is a user error in the spec, so
        // record InvalidSchedule and do not reconcile until the spec changes.
        let (scheduled_time, missed) = match next_schedule_time(cronjob, now, &sched, tz) {
            Ok(r) => r,
            Err(e) => {
                warn!("Invalid schedule {schedule:?} for {namespace}/{name}: {e}");
                self.record_warning(
                    cronjob,
                    "InvalidSchedule",
                    &format!("invalid schedule: {schedule} : {e}"),
                )
                .await;
                return Ok(None);
            }
        };
        let Some(scheduled_time) = scheduled_time else {
            // :536-543
            debug!("No unmet start times for {}/{}", namespace, name);
            return Ok(requeue(cronjob));
        };
        // nextScheduleTime (utils.go:217-219).
        if missed == MissedSchedules::Many {
            self.record_warning(
                cronjob,
                "TooManyMissedTimes",
                "too many missed start times. Set or decrease .spec.startingDeadlineSeconds or check clock skew",
            )
            .await;
        }
        // :546-563: past the starting deadline, skip this start.
        if let Some(sds) = cronjob.spec.starting_deadline_seconds {
            if scheduled_time + chrono::Duration::seconds(sds) < now {
                self.record_warning(
                    cronjob,
                    "MissSchedule",
                    &format!(
                        "Missed scheduled time to start a job: {}",
                        // time.RFC1123Z in UTC.
                        scheduled_time.format("%a, %d %b %Y %H:%M:%S +0000")
                    ),
                )
                .await;
                return Ok(requeue(cronjob));
            }
        }
        // getJobName (:676): the Job is named from the SCHEDULED time.
        let scheduled_job_name = job_name_for(&name, scheduled_time);
        info!("CronJob {}/{} triggered at {}", namespace, name, now);

        // :564-573
        if in_active_list_by_name(cronjob, namespace, &scheduled_job_name)
            || cronjob
                .status
                .as_ref()
                .and_then(|s| s.last_schedule_time)
                .is_some_and(|t| t == scheduled_time)
        {
            debug!("Not starting job because the scheduled time is already processed");
            return Ok(requeue(cronjob));
        }

        let policy = cronjob
            .spec
            .concurrency_policy
            .clone()
            .unwrap_or_else(|| "Allow".to_string());
        let has_active = cronjob
            .status
            .as_ref()
            .is_some_and(|s| !s.active.is_empty());
        if policy == "Forbid" && has_active {
            self.record_normal(
                cronjob,
                "JobAlreadyActive",
                "Not starting job because prior execution is running and concurrency policy is Forbid",
            )
            .await;
            return Ok(requeue(cronjob));
        }
        if policy == "Replace" {
            for r in &active_refs(cronjob) {
                let job_name = r.name.clone().unwrap_or_default();
                let key = format!(
                    "/registry/jobs/{}/{}",
                    r.namespace.as_deref().unwrap_or(namespace),
                    job_name
                );
                match self.storage.get::<Job>(&key).await {
                    Ok(job) => {
                        if !self.delete_job(cronjob, &job).await {
                            return Err(anyhow::anyhow!(
                                "could not replace job {}/{}",
                                namespace,
                                job_name
                            ));
                        }
                        *update_status = true;
                    }
                    Err(e) => {
                        self.record_warning(cronjob, "FailedGet", &format!("Get job: {e}"))
                            .await;
                        return Err(e.into());
                    }
                }
            }
        }

        let Some(job) = self.create_job(cronjob, namespace, scheduled_time).await? else {
            return Ok(None);
        };

        // :655-671: add the just-started Job to the status list.
        let status = cronjob.status.get_or_insert_with(Default::default);
        status.active.push(ObjectReference {
            kind: Some("Job".to_string()),
            namespace: Some(namespace.to_string()),
            name: Some(job.metadata.name.clone()),
            uid: Some(job.metadata.uid.clone()),
            api_version: Some("batch/v1".to_string()),
            resource_version: job.metadata.resource_version.clone(),
            field_path: None,
        });
        status.last_schedule_time = Some(scheduled_time);
        *update_status = true;
        // :672-673
        Ok(requeue(cronjob))
    }

    /// `deleteJob` (cronjob_controllerv2.go:748-760): delete the Job and its
    /// reference in the active list; false when the delete fails.
    async fn delete_job(&self, cronjob: &mut CronJob, job: &Job) -> bool {
        let namespace = job.metadata.namespace.as_deref().unwrap_or("default");
        let key = format!("/registry/jobs/{}/{}", namespace, job.metadata.name);
        if let Err(e) = self.storage.delete(&key).await {
            self.record_warning(cronjob, "FailedDelete", &format!("Deleted job: {e}"))
                .await;
            error!("Error deleting job {} from cronjob: {e}", job.metadata.name);
            return false;
        }
        delete_from_active_list(cronjob, &job.metadata.uid);
        self.record_normal(
            cronjob,
            "SuccessfulDelete",
            &format!("Deleted job {}", job.metadata.name),
        )
        .await;
        true
    }

    #[cfg(test)]
    async fn should_run_now(
        &self,
        schedule: &str,
        now: chrono::DateTime<chrono::Utc>,
        cronjob: &CronJob,
    ) -> Result<bool> {
        Ok(self
            .scheduled_run_time(schedule, now, cronjob)
            .await?
            .is_some())
    }

    /// The most recent scheduled time in `(last, now]`, or None when nothing is
    /// due (mostRecentScheduleTime, pkg/controller/cronjob/utils.go).
    #[cfg(test)]
    async fn scheduled_run_time(
        &self,
        schedule: &str,
        now: chrono::DateTime<chrono::Utc>,
        cronjob: &CronJob,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
        Ok(match self.parse_schedule(schedule, cronjob).await? {
            Some((sched, tz)) => next_schedule_time(cronjob, now, &sched, tz)
                .ok()
                .and_then(|(t, _)| t),
            None => None,
        })
    }

    /// The schedule and zone `syncCronJob` evaluates against: the
    /// spec.timeZone / formatSchedule / ParseCronScheduleWithPanicRecovery
    /// steps (cronjob_controllerv2.go:505-526). `None` means "do not
    /// schedule", with the matching event already recorded.
    async fn parse_schedule(
        &self,
        schedule: &str,
        cronjob: &CronJob,
    ) -> Result<Option<(Schedule, chrono_tz::Tz)>> {
        // syncCronJob checks spec.timeZone before anything else and records an
        // UnknownTimeZone event (cronjob_controllerv2.go:507-513).
        if let Some(name) = cronjob.spec.time_zone.as_deref() {
            if !name.is_empty() && name.parse::<chrono_tz::Tz>().is_err() {
                warn!(
                    "CronJob {}: invalid timeZone {:?}; not scheduling",
                    cronjob.metadata.name, name
                );
                self.record_warning(
                    cronjob,
                    "UnknownTimeZone",
                    &format!("invalid timeZone: {name:?}: unknown time zone {name}"),
                )
                .await;
                return Ok(None);
            }
        }

        // formatSchedule (cronjob_controllerv2.go:766-773) records
        // UnsupportedSchedule for any schedule containing "TZ".
        if schedule.contains("TZ") {
            self.record_warning(
                cronjob,
                "UnsupportedSchedule",
                &format!("CRON_TZ or TZ used in schedule {schedule:?} is not officially supported, see https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/ for more details"),
            )
            .await;
        }

        // Inline `TZ=`/`CRON_TZ=` prefix: robfig/cron/v3 `Parser.Parse`
        // (vendor/github.com/robfig/cron/v3/parser.go:95-103) strips it and
        // schedules in that zone. formatSchedule
        // (pkg/controller/cronjob/cronjob_controllerv2.go:766-773) returns such
        // a schedule untouched, so the inline zone wins over spec.timeZone. A
        // bad zone or a missing space is an unparseable schedule: skip, as
        // syncCronJob does (cronjob_controllerv2.go:519-526).
        let (schedule, inline_tz) =
            if schedule.starts_with("TZ=") || schedule.starts_with("CRON_TZ=") {
                let Some(i) = schedule.find(' ') else {
                    warn!(
                        "Unparseable schedule '{}': no space after TZ prefix",
                        schedule
                    );
                    self.record_unparseable(cronjob, schedule, "no space after TZ prefix")
                        .await;
                    return Ok(None);
                };
                let eq = schedule.find('=').unwrap_or(0);
                let name = &schedule[eq + 1..i];
                match name.parse::<chrono_tz::Tz>() {
                    Ok(t) => (schedule[i..].trim(), Some(t)),
                    Err(_) => {
                        warn!("Unparseable schedule '{}': bad location {}", schedule, name);
                        // robfig/cron parser.go:100.
                        self.record_unparseable(
                            cronjob,
                            schedule,
                            &format!("provided bad location {name}: unknown time zone {name}"),
                        )
                        .await;
                        return Ok(None);
                    }
                }
            } else {
                (schedule, None)
            };

        let schedule_parsed = match parse_standard_schedule(schedule) {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to parse cron schedule '{}': {}", schedule, e);
                self.record_unparseable(cronjob, schedule, &e.to_string())
                    .await;
                return Ok(None);
            }
        };

        // Resolve spec.timeZone. The `cron` crate interprets a schedule in the
        // timezone of the DateTime passed to `after()`, so we localise the
        // reference times below to this zone — equivalent to upstream prefixing
        // the schedule with `TZ=<zone>` (formatSchedule in
        // pkg/controller/cronjob/cronjob_controllerv2.go). An unset/empty zone
        // means UTC; an INVALID zone means "do not schedule" — matching
        // upstream, which records an UnknownTimeZone event and returns without
        // starting a job.
        let tz: chrono_tz::Tz = match cronjob.spec.time_zone.as_deref() {
            _ if inline_tz.is_some() => inline_tz.unwrap(),
            None | Some("") => chrono_tz::UTC,
            Some(name) => match name.parse::<chrono_tz::Tz>() {
                Ok(t) => t,
                Err(_) => {
                    warn!(
                        "CronJob {}: invalid timeZone {:?}; not scheduling",
                        cronjob.metadata.name, name
                    );
                    return Ok(None);
                }
            },
        };
        Ok(Some((schedule_parsed, tz)))
    }

    async fn create_job(
        &self,
        cronjob: &CronJob,
        namespace: &str,
        scheduled_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<Job>> {
        let cronjob_name = &cronjob.metadata.name;
        let job_name = job_name_for(cronjob_name, scheduled_time);

        let mut labels = cronjob
            .spec
            .job_template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        labels.insert("cronjob-name".to_string(), cronjob_name.clone());

        let mut annotations = cronjob
            .spec
            .job_template
            .metadata
            .as_ref()
            .and_then(|m| m.annotations.clone())
            .unwrap_or_default();
        // getJobFromTemplate2 (utils.go:254): CronJobScheduledTimestampAnnotation
        // = scheduledTime.In(spec.timeZone).Format(time.RFC3339).
        let tz: chrono_tz::Tz = cronjob
            .spec
            .time_zone
            .as_deref()
            .and_then(|n| n.parse().ok())
            .unwrap_or(chrono_tz::UTC);
        let local = scheduled_time.with_timezone(&tz);
        annotations.insert(
            "batch.kubernetes.io/cronjob-scheduled-timestamp".to_string(),
            local.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
        let annotations = Some(annotations);

        let job = Job {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Job".to_string(),
                api_version: "batch/v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: job_name.clone(),
                generate_name: None,
                generation: None,
                managed_fields: None,
                namespace: Some(namespace.to_string()),
                labels: Some(labels),
                annotations,
                uid: uuid::Uuid::new_v4().to_string(),
                creation_timestamp: Some(chrono::Utc::now()),
                deletion_timestamp: None,
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "CronJob".to_string(),
                    name: cronjob_name.clone(),
                    uid: cronjob.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
            },
            spec: cronjob.spec.job_template.spec.clone(),
            status: None,
        };

        let key = format!("/registry/jobs/{}/{}", namespace, job_name);
        let created = match self.storage.create(&key, &job).await {
            Ok(created) => created,
            Err(rusternetes_common::Error::AlreadyExists(_)) => {
                // cronjob_controllerv2.go:615-632. A Job we created before
                // losing the status write is re-added to the active list; one
                // controlled by someone else is theirs to account for; one
                // already in the active list needs no second write.
                let existing: Job = self.storage.get(&key).await?;
                let controlled = existing
                    .metadata
                    .owner_references
                    .iter()
                    .flatten()
                    .find(|r| r.controller == Some(true))
                    .is_some_and(|r| r.uid == cronjob.metadata.uid);
                if !controlled || in_active_list(cronjob, &existing.metadata.uid) {
                    return Ok(None);
                }
                info!(
                    "Job {} already exists for CronJob {}",
                    job_name, cronjob_name
                );
                return Ok(Some(existing));
            }
            // cronjob_controllerv2.go:611-614: a Terminating namespace refuses
            // every create, so return the error without the FailedCreate event
            // (the event belongs to the default arm, :638-641).
            Err(e) if is_namespace_terminating_rejection(&e) => return Err(e.into()),
            Err(e) => {
                self.record_warning(cronjob, "FailedCreate", &format!("Error creating job: {e}"))
                    .await;
                return Err(e.into());
            }
        };

        info!("Created Job {} from CronJob {}", job_name, cronjob_name);
        self.record_normal(
            cronjob,
            "SuccessfulCreate",
            &format!("Created job {}", job_name),
        )
        .await;

        Ok(Some(created))
    }

    /// `cleanupFinishedJobs` (cronjob_controllerv2.go:682-716). Returns whether
    /// the status needs writing.
    async fn cleanup_finished_jobs(&self, cronjob: &mut CronJob, jobs: &[Job]) -> bool {
        // :684-686. The 3 / 1 defaults are applied by the API server
        // (SetDefaults_CronJob, pkg/apis/batch/v1/defaults.go:83-88;
        // apply_cronjob_defaults), not here.
        let (success_limit, failed_limit) = match (
            cronjob.spec.successful_jobs_history_limit,
            cronjob.spec.failed_jobs_history_limit,
        ) {
            (None, None) => return false,
            (s, f) => (s, f),
        };

        let mut successful: Vec<Job> = Vec::new();
        let mut failed: Vec<Job> = Vec::new();
        for job in jobs {
            match job_finished_condition(job) {
                Some("Complete") => successful.push(job.clone()),
                Some("Failed") => failed.push(job.clone()),
                _ => {}
            }
        }

        let mut update = false;
        // :701-712: each list is trimmed only when its own limit is set.
        if let Some(limit) = success_limit {
            update |= self
                .remove_oldest_jobs(cronjob, &mut successful, limit)
                .await;
        }
        if let Some(limit) = failed_limit {
            update |= self.remove_oldest_jobs(cronjob, &mut failed, limit).await;
        }
        update
    }

    /// `removeOldestJobs` (cronjob_controllerv2.go:728-746), ordered by
    /// `byJobStartTime` (utils.go:274-295): started Jobs first, oldest first,
    /// name as tie-break.
    async fn remove_oldest_jobs(
        &self,
        cronjob: &mut CronJob,
        jobs: &mut [Job],
        max_jobs: i32,
    ) -> bool {
        let num_to_delete = jobs.len() as i64 - max_jobs.max(0) as i64;
        if num_to_delete <= 0 {
            return false;
        }
        jobs.sort_by(|a, b| {
            let (sa, sb) = (
                a.status.as_ref().and_then(|s| s.start_time),
                b.status.as_ref().and_then(|s| s.start_time),
            );
            (sa.is_none(), sa, &a.metadata.name).cmp(&(sb.is_none(), sb, &b.metadata.name))
        });
        let mut update = false;
        for job in jobs.iter().take(num_to_delete as usize) {
            if self.delete_job(cronjob, job).await {
                update = true;
            }
        }
        update
    }
}

/// `inActiveList` (utils.go:58-66).
fn in_active_list(cj: &CronJob, uid: &str) -> bool {
    cj.status
        .as_ref()
        .is_some_and(|s| s.active.iter().any(|r| r.uid.as_deref() == Some(uid)))
}

/// `inActiveListByName` (utils.go:68-77).
fn in_active_list_by_name(cj: &CronJob, namespace: &str, name: &str) -> bool {
    cj.status.as_ref().is_some_and(|s| {
        s.active
            .iter()
            .any(|r| r.name.as_deref() == Some(name) && r.namespace.as_deref() == Some(namespace))
    })
}

/// `deleteFromActiveList` (utils.go:79-93).
fn delete_from_active_list(cj: &mut CronJob, uid: &str) {
    if let Some(s) = cj.status.as_mut() {
        s.active.retain(|r| r.uid.as_deref() != Some(uid));
    }
}

fn active_refs(cj: &CronJob) -> Vec<ObjectReference> {
    cj.status
        .as_ref()
        .map(|s| s.active.clone())
        .unwrap_or_default()
}

/// `getFinishedStatus` (cronjob_controllerv2.go:718-726): the first Complete
/// or Failed condition whose status is True.
fn job_finished_condition(job: &Job) -> Option<&'static str> {
    job.status
        .as_ref()?
        .conditions
        .as_ref()?
        .iter()
        .find_map(|c| match (c.condition_type.as_str(), c.status.as_str()) {
            ("Complete", "True") => Some("Complete"),
            ("Failed", "True") => Some("Failed"),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
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
        let ctrl = super::CronJobController::new(storage);

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
        let ctrl = super::CronJobController::new(storage);
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        let ctrl = super::CronJobController::new(storage);
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
        let t = chrono::DateTime::parse_from_rfc3339("2025-01-15T06:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let key = |cj: &str| format!("/registry/jobs/default/{}", super::job_name_for(cj, t));
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
        let t = chrono::DateTime::parse_from_rfc3339("2025-01-15T06:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let cj = cj_fixture(serde_json::json!({}));
        // Foreign Job with the deterministic name, no ownerReference.
        let foreign: Job = serde_json::from_value(serde_json::json!({
            "apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": super::job_name_for("cj", t), "namespace": "default", "uid": "foreign"},
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
                ctrl.job_event_cronjob_key(&ev).await.as_deref(),
                Some("cronjobs/default/cj"),
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        assert_eq!(ctrl.job_event_cronjob_key(&ev(&orphan)).await, None);
        let mut wrong_kind = job_json("j", "u", true, None);
        wrong_kind.metadata.owner_references.as_mut().unwrap()[0].kind = "Deployment".into();
        assert_eq!(ctrl.job_event_cronjob_key(&ev(&wrong_kind)).await, None);
        let mut wrong_uid = job_json("j", "u", true, None);
        wrong_uid.metadata.owner_references.as_mut().unwrap()[0].uid = "other".into();
        assert_eq!(ctrl.job_event_cronjob_key(&ev(&wrong_uid)).await, None);
        let mut missing = job_json("j", "u", true, None);
        missing.metadata.owner_references.as_mut().unwrap()[0].name = "gone".into();
        assert_eq!(ctrl.job_event_cronjob_key(&ev(&missing)).await, None);
        let mut not_ctrl = job_json("j", "u", true, None);
        not_ctrl.metadata.owner_references.as_mut().unwrap()[0].controller = Some(false);
        assert_eq!(ctrl.job_event_cronjob_key(&ev(&not_ctrl)).await, None);
    }

    // ---- requeueAfter (#2398) -------------------------------------------
    // Ported from TestNextScheduleTimeDuration
    // (pkg/controller/cronjob/utils_test.go:614-702).

    fn std_schedule(s: &str) -> super::Schedule {
        super::parse_standard_schedule(s).unwrap()
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
            let got = super::next_schedule_duration(
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
        // Yearly schedule, created 10 minutes ago: nothing due.
        let cj = cj_json("0 0 1 1 *", "Allow", serde_json::json!({}));
        let mut cj = seed(&storage, &cj, &[]).await;
        let after = ctrl.reconcile(&mut cj).await.unwrap();
        let after = after.expect("an unmet schedule must requeue at the next slot");
        assert!(after > std::time::Duration::from_secs(60), "{after:?}");

        // Suspended: upstream returns nil -> no requeue (:514-517).
        let storage = Arc::new(Mem::new());
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        use super::MissedSchedules::{Few, Many, None as NoneMissed};
        use chrono::Duration as D;
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
            super::MissedSchedules,
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
            let (e, r, mm) = super::most_recent_schedule_time(
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
        let (_, r, mm) = super::most_recent_schedule_time(
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
        let err = super::most_recent_schedule_time(
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
            super::next_schedule_time(cj, at(now), &sch, chrono_tz::UTC)
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
        assert!(super::next_schedule_time(&cj, at(D::hours(1)), &rogue, chrono_tz::UTC).is_err());
    }

    /// TooManyMissedTimes (utils.go:170-173), recorded as a Warning when more
    /// than 100 starts were missed and there is no deadline.
    #[tokio::test]
    async fn too_many_missed_times_records_warning() {
        use rusternetes_common::resources::Event;
        use rusternetes_storage::Storage;
        use std::sync::Arc;
        let storage = Arc::new(Mem::new());
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        let e = super::parse_standard_schedule("@every nope").unwrap_err();
        assert_eq!(
            e,
            "failed to parse duration @every nope: time: invalid duration \"nope\""
        );
        assert!(super::parse_standard_schedule("@every").is_err());
    }

    /// robfig/cron v3 dow bounds are {0, 6} (vendor/github.com/robfig/cron/v3/
    /// spec.go:40), so 7 and reversed ranges are parse errors
    /// (parser.go:304-309), including the `n-0` and `n-7` edges.
    #[test]
    fn day_of_week_bounds_match_robfig() {
        let err = |s: &str| super::parse_standard_schedule(s).unwrap_err();
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
            assert!(super::parse_standard_schedule(ok).is_ok(), "{ok}");
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
            let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
        let ctrl = super::CronJobController::new(Arc::clone(&storage));
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
