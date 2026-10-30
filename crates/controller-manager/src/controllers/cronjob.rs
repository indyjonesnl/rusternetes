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
pub fn job_name_for(cronjob_name: &str, scheduled_time: chrono::DateTime<chrono::Utc>) -> String {
    format!("{}-{}", cronjob_name, scheduled_time.timestamp() / 60)
}

/// `nextScheduleDelta` (cronjob_controllerv2.go:58): 100ms of padding on every
/// requeue to absorb NTP skew.
const NEXT_SCHEDULE_DELTA: chrono::Duration = chrono::Duration::milliseconds(100);

/// robfig/cron's `Schedule`: either a parsed crontab spec or the
/// `ConstantDelaySchedule` of an `@every` descriptor
/// (vendor/github.com/robfig/cron/v3/constantdelay.go:3-26).
#[derive(Debug, Clone)]
pub enum Schedule {
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
    pub fn next(
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
pub fn parse_standard_schedule(schedule: &str) -> Result<Schedule, String> {
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
pub enum MissedSchedules {
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
pub fn most_recent_schedule_time(
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

/// The inline `TZ=`/`CRON_TZ=` prefix handling of robfig/cron/v3
/// `Parser.Parse` (vendor/github.com/robfig/cron/v3/parser.go:95-103): returns
/// the schedule without the prefix and the zone it names. The error is the
/// parser's message (a missing space, or `provided bad location`).
fn split_inline_tz(schedule: &str) -> Result<(&str, Option<chrono_tz::Tz>), String> {
    if !(schedule.starts_with("TZ=") || schedule.starts_with("CRON_TZ=")) {
        return Ok((schedule, None));
    }
    let Some(i) = schedule.find(' ') else {
        return Err("no space after TZ prefix".to_string());
    };
    let eq = schedule.find('=').unwrap_or(0);
    let name = &schedule[eq + 1..i];
    match name.parse::<chrono_tz::Tz>() {
        Ok(t) => Ok((schedule[i..].trim(), Some(t))),
        // robfig/cron parser.go:100.
        Err(_) => Err(format!(
            "provided bad location {name}: unknown time zone {name}"
        )),
    }
}

/// `metav1.GetControllerOf` reduced to what `resolveControllerRef`
/// (cronjob_controllerv2.go:240-256) reads: (kind, name, uid).
type ControllerRef = (String, String, String);

fn controller_ref_of(job: &Job) -> Option<ControllerRef> {
    job.metadata
        .owner_references
        .as_ref()?
        .iter()
        .find(|r| r.controller == Some(true))
        .map(|r| (r.kind.clone(), r.name.clone(), r.uid.clone()))
}

/// What the informer's old object gives `updateJob` (:303-331): the last
/// controllerRef seen per Job key. A storage `Modified` event carries only the
/// new object, so the previous value is remembered here.
pub type JobRefCache = std::collections::HashMap<String, Option<ControllerRef>>;

/// The (spec.schedule, spec.timeZone) the informer's old CronJob gives
/// `updateCronJob` (:386-397), per CronJob key.
pub type CronJobSpecCache = std::collections::HashMap<String, (String, Option<String>)>;

/// What the CronJob informer handlers do with one event
/// (cronjob_controllerv2.go:121-132).
#[derive(Debug, PartialEq)]
pub enum CronJobEnqueue {
    /// `enqueueController`.
    Now(String),
    /// `enqueueControllerAfter`.
    After(String, Duration),
    /// Nothing is enqueued.
    Nothing,
}

/// `nextScheduleTimeDuration` (pkg/controller/cronjob/utils.go:188-205): the
/// delay until the next schedule slot, plus `NEXT_SCHEDULE_DELTA`.
pub fn next_schedule_duration(
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
pub fn next_schedule_time(
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
    /// `jm.now` (cronjob_controllerv2.go:79): "a function that returns current
    /// time, done to facilitate unit tests".
    now: Arc<dyn Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync>,
}

impl<S: Storage + 'static> CronJobController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        let recorder = EventRecorder::new(Arc::clone(&storage));
        Self {
            storage,
            recorder,
            now: Arc::new(chrono::Utc::now),
        }
    }

    /// Replace the clock, as upstream tests set `jm.now`. Used by the `it`
    /// integration tests; the bin target compiles this module without them.
    pub fn with_now(
        mut self,
        now: impl Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync + 'static,
    ) -> Self {
        self.now = Arc::new(now);
        self
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
    /// its CronJob and return the queue keys to enqueue.
    ///
    /// `addJob` (:281-285) routes a Job already pending deletion through
    /// `deleteJob`; all three then do the same thing (get the controllerRef,
    /// `resolveControllerRef`, `enqueueController`). A `Deleted` event carries
    /// the previous value, which stands in for `DeletedFinalStateUnknown`
    /// (:339-350). `updateJob` (:312-320) additionally wakes the OLD controller
    /// when the controllerRef changed; `cache` supplies the old controllerRef
    /// because a storage `Modified` event carries only the new object.
    pub async fn job_event_cronjob_keys(
        &self,
        cache: &mut JobRefCache,
        ev: &rusternetes_storage::WatchEvent,
    ) -> Vec<String> {
        use rusternetes_storage::WatchEvent;
        let (storage_key, body) = match ev {
            WatchEvent::Added(k, v) | WatchEvent::Modified(k, v) | WatchEvent::Deleted(k, v) => {
                (k, v)
            }
        };
        let Ok(job) = serde_json::from_str::<Job>(body) else {
            return Vec::new();
        };
        let cur = controller_ref_of(&job);
        let mut refs = Vec::new();
        match ev {
            WatchEvent::Deleted(..) => {
                cache.remove(storage_key);
            }
            WatchEvent::Added(..) => {
                cache.insert(storage_key.clone(), cur.clone());
            }
            WatchEvent::Modified(..) => {
                // :314-320: controllerRefChanged && oldControllerRef != nil.
                let old = cache.insert(storage_key.clone(), cur.clone()).flatten();
                if old != cur {
                    refs.extend(old);
                }
            }
        }
        refs.extend(cur);
        let Some(ns) = job.metadata.namespace.as_deref() else {
            return Vec::new();
        };
        let mut keys = Vec::new();
        for r in &refs {
            if let Some(k) = self.resolve_controller_ref(ns, r).await {
                keys.push(k);
            }
        }
        keys
    }

    /// `resolveControllerRef` (:240-256): Kind, then Get by name, then UID.
    async fn resolve_controller_ref(&self, ns: &str, r: &ControllerRef) -> Option<String> {
        let (kind, name, uid) = r;
        if kind != "CronJob" {
            return None;
        }
        let cronjob = self
            .storage
            .get::<CronJob>(&build_key("cronjobs", Some(ns), name))
            .await
            .ok()?;
        if &cronjob.metadata.uid != uid {
            return None;
        }
        Some(format!("cronjobs/{}/{}", ns, name))
    }

    /// The CronJob informer handlers (cronjob_controllerv2.go:121-132): add and
    /// delete enqueue now; `updateCronJob` (:386-420) enqueues after the next
    /// schedule time when spec.schedule or spec.timeZone changed, else now.
    /// `cache` supplies the old spec (a storage `Modified` event carries only
    /// the new object).
    pub async fn cronjob_event_enqueue(
        &self,
        cache: &mut CronJobSpecCache,
        ev: &rusternetes_storage::WatchEvent,
        now: chrono::DateTime<chrono::Utc>,
    ) -> CronJobEnqueue {
        use rusternetes_storage::WatchEvent;
        let key = extract_key(ev);
        let body = match ev {
            WatchEvent::Deleted(..) => {
                cache.remove(&key);
                return CronJobEnqueue::Now(key);
            }
            WatchEvent::Added(_, b) | WatchEvent::Modified(_, b) => b,
        };
        let Ok(cj) = serde_json::from_str::<CronJob>(body) else {
            return CronJobEnqueue::Now(key);
        };
        let cur = (cj.spec.schedule.clone(), cj.spec.time_zone.clone());
        let old = cache.insert(key.clone(), cur.clone());
        // :397: oldCJ.Spec.Schedule != newCJ.Spec.Schedule || TimeZone differs.
        // An add, or a Modified with no remembered old spec, has nothing to
        // compare: enqueue now.
        if !matches!(ev, WatchEvent::Modified(..)) || old.is_none() || old.as_ref() == Some(&cur) {
            return CronJobEnqueue::Now(key);
        }
        // :399 formatSchedule (:766-784): a schedule containing "TZ" is used
        // as-is; else a loadable spec.timeZone is applied; an unloadable one is
        // ignored (UTC).
        let parsed = split_inline_tz(&cj.spec.schedule)
            .and_then(|(sched, inline)| parse_standard_schedule(sched).map(|p| (p, inline)));
        let (sched, inline) = match parsed {
            Ok(v) => v,
            Err(err) => {
                // :400-405: log, record UnParseableCronJobSchedule, enqueue nothing.
                debug!(
                    "Unparseable schedule for cronjob {key}: {:?}: {err}",
                    cj.spec.schedule
                );
                self.record_warning(
                    &cj,
                    "UnParseableCronJobSchedule",
                    &format!("unparseable schedule for cronjob: {}", cj.spec.schedule),
                )
                .await;
                return CronJobEnqueue::Nothing;
            }
        };
        let tz = inline.unwrap_or_else(|| {
            cj.spec
                .time_zone
                .as_deref()
                .and_then(|n| n.parse::<chrono_tz::Tz>().ok())
                .unwrap_or(chrono_tz::UTC)
        });
        // :408-410. Upstream dereferences the result; with no next slot there
        // is nothing to wait for.
        match next_schedule_duration(&sched, tz, &cj, now) {
            Some(d) => CronJobEnqueue::After(key, d),
            None => CronJobEnqueue::Nothing,
        }
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

            // The informer stores' old objects, seeded from a list taken after
            // the watches are established so no change is missed.
            let mut job_refs = JobRefCache::new();
            if let Ok(jobs) = self.storage.list::<Job>("/registry/jobs/").await {
                for j in &jobs {
                    let ns = j.metadata.namespace.as_deref().unwrap_or("");
                    job_refs.insert(
                        format!("/registry/jobs/{}/{}", ns, j.metadata.name),
                        controller_ref_of(j),
                    );
                }
            }
            let mut cronjob_specs = CronJobSpecCache::new();
            if let Ok(cjs) = self.storage.list::<CronJob>("/registry/cronjobs/").await {
                for c in &cjs {
                    let ns = c.metadata.namespace.as_deref().unwrap_or("");
                    cronjob_specs.insert(
                        format!("cronjobs/{}/{}", ns, c.metadata.name),
                        (c.spec.schedule.clone(), c.spec.time_zone.clone()),
                    );
                }
            }

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                match self
                                    .cronjob_event_enqueue(&mut cronjob_specs, &ev, chrono::Utc::now())
                                    .await
                                {
                                    CronJobEnqueue::Now(key) => queue.add(key).await,
                                    CronJobEnqueue::After(key, d) => queue.add_after(key, d).await,
                                    CronJobEnqueue::Nothing => {}
                                }
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
                                for key in self.job_event_cronjob_keys(&mut job_refs, &ev).await {
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
    pub async fn reconcile(&self, cronjob: &mut CronJob) -> Result<Option<Duration>> {
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
        let now = (self.now)();

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

    pub async fn should_run_now(
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
    pub async fn scheduled_run_time(
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
        let (schedule, inline_tz) = match split_inline_tz(schedule) {
            Ok(v) => v,
            Err(e) => {
                warn!("Unparseable schedule '{}': {}", schedule, e);
                self.record_unparseable(cronjob, schedule, &e).await;
                return Ok(None);
            }
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

    pub async fn create_job(
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
    pub async fn cleanup_finished_jobs(&self, cronjob: &mut CronJob, jobs: &[Job]) -> bool {
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
