//! Ported from `pkg/controller/ttlafterfinished/ttlafterfinished_controller_test.go`
//! (release-1.35): `TestTimeLeft`. The remaining tests cover the pieces of the
//! controller that file leaves untested, each pinned to the upstream symbol it
//! exercises: the `addJob`/`updateJob` handlers, `processJob`/`processTTL`
//! (expiry -> `Foreground` delete with a UID precondition, not-yet-expired ->
//! `enqueueAfter`), and the `job_deletion_duration_seconds` metric. The
//! controller runs against `MemoryStorage` through its reflector, so the
//! `run()` tests drive the real list+watch -> store -> queue -> sync path.
use chrono::{DateTime, Duration, Utc};
use k3rs_client::reflector::StoreEvent;
use k3rs_common::resources::workloads::Job;
use k3rs_controller_manager::controllers::ttl_after_finished::*;
use k3rs_controller_manager::controllers::worker_pool::test_support::template;
use k3rs_storage::memory::MemoryStorage;
use k3rs_storage::{build_key, Preconditions, Storage};
use serde_json::{json, Value};
use std::future::Future;
use std::sync::Arc;

type C = TTLAfterFinishedController<MemoryStorage>;

fn condition(kind: &str, at: DateTime<Utc>) -> Value {
    json!({"type": kind, "status": "True", "lastTransitionTime": at.to_rfc3339()})
}

/// `newJob(completionTime, failedTime, ttl)`.
fn new_job(
    completion_time: Option<DateTime<Utc>>,
    failed_time: Option<DateTime<Utc>>,
    ttl: Option<i32>,
) -> Job {
    let mut conditions = Vec::new();
    if let Some(t) = completion_time {
        conditions.push(condition("Complete", t));
    }
    if let Some(t) = failed_time {
        conditions.push(condition("Failed", t));
    }
    let mut spec = json!({"template": template(), "selector": {"matchLabels": {"foo": "bar"}}});
    if let Some(ttl) = ttl {
        spec["ttlSecondsAfterFinished"] = json!(ttl);
    }
    serde_json::from_value(json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": "foobar", "namespace": "default", "uid": "uid-foobar"},
        "spec": spec,
        "status": {"conditions": conditions},
    }))
    .unwrap()
}

#[test]
fn test_time_left() {
    let now = Utc::now();
    let s = Duration::seconds;
    struct Case {
        name: &'static str,
        completion: Option<DateTime<Utc>>,
        failed: Option<DateTime<Utc>>,
        ttl: Option<i32>,
        // None => expect an error containing "should not be cleaned up".
        expected: Option<(Duration, DateTime<Utc>)>,
    }
    let cases = vec![
        Case {
            name: "Error case: Job unfinished",
            completion: None,
            failed: None,
            ttl: Some(100),
            expected: None,
        },
        Case {
            name: "Error case: Job completed now, no TTL",
            completion: Some(now),
            failed: None,
            ttl: None,
            expected: None,
        },
        Case {
            name: "Job completed now, 0s TTL",
            completion: Some(now),
            failed: None,
            ttl: Some(0),
            expected: Some((s(0), now)),
        },
        Case {
            name: "Job completed now, 10s TTL",
            completion: Some(now),
            failed: None,
            ttl: Some(10),
            expected: Some((s(10), now + s(10))),
        },
        Case {
            name: "Job completed 10s ago, 15s TTL",
            completion: Some(now - s(10)),
            failed: None,
            ttl: Some(15),
            expected: Some((s(5), now + s(5))),
        },
        Case {
            name: "Error case: Job failed now, no TTL",
            completion: None,
            failed: Some(now),
            ttl: None,
            expected: None,
        },
        Case {
            name: "Job failed now, 0s TTL",
            completion: None,
            failed: Some(now),
            ttl: Some(0),
            expected: Some((s(0), now)),
        },
        Case {
            name: "Job failed now, 10s TTL",
            completion: None,
            failed: Some(now),
            ttl: Some(10),
            expected: Some((s(10), now + s(10))),
        },
        Case {
            name: "Job failed 10s ago, 15s TTL",
            completion: None,
            failed: Some(now - s(10)),
            ttl: Some(15),
            expected: Some((s(5), now + s(5))),
        },
    ];
    for c in cases {
        let job = new_job(c.completion, c.failed, c.ttl);
        // RFC3339 round-trips at nanosecond precision; compare like for like.
        let got = time_left(&job, now);
        match c.expected {
            None => {
                let err = got.expect_err(c.name);
                assert!(
                    err.to_string().contains("should not be cleaned up"),
                    "{}: {err}",
                    c.name
                );
            }
            Some((left, expire_at)) => {
                let (got_left, got_expire) = got.expect(c.name);
                assert_eq!(got_left, left, "{}", c.name);
                assert_eq!(got_expire, expire_at, "{}", c.name);
            }
        }
    }
}

#[test]
fn needs_cleanup_requires_a_ttl_and_a_finished_job() {
    let now = Utc::now();
    assert!(needs_cleanup(&new_job(Some(now), None, Some(0))));
    assert!(needs_cleanup(&new_job(None, Some(now), Some(5))));
    assert!(!needs_cleanup(&new_job(Some(now), None, None)));
    assert!(!needs_cleanup(&new_job(None, None, Some(5))));
}

#[test]
fn a_finished_condition_without_a_transition_time_is_an_error() {
    // `jobFinishTime`: "unable to find the time when the Job ... finished".
    let mut job = new_job(Some(Utc::now()), None, Some(1));
    job.status.as_mut().unwrap().conditions.as_mut().unwrap()[0].last_transition_time = None;
    let err = time_left(&job, Utc::now()).unwrap_err();
    assert!(
        err.to_string()
            .contains("unable to find the time when the Job default/foobar finished"),
        "{err}"
    );
}

async fn put_job(s: &MemoryStorage, name: &str, finished_at: Option<DateTime<Utc>>, ttl: i32) {
    let mut job = new_job(finished_at, None, Some(ttl));
    job.metadata.name = name.to_string();
    job.metadata.uid = format!("uid-{name}");
    s.create(&build_key("jobs", Some("default"), name), &job)
        .await
        .unwrap();
}

async fn get_job(s: &MemoryStorage, name: &str) -> Option<Value> {
    s.get(&build_key("jobs", Some("default"), name)).await.ok()
}

/// Poll until `f` yields `Some`, failing after 10s.
async fn eventually<T, F, Fut>(what: &str, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    for _ in 0..500 {
        if let Some(v) = f().await {
            return v;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for: {what}");
}

fn start(s: &Arc<MemoryStorage>) -> Arc<C> {
    let c = Arc::new(TTLAfterFinishedController::new(s.clone()));
    tokio::spawn(c.clone().run());
    c
}

fn is_foreground_deleting(job: &Value) -> bool {
    job.pointer("/metadata/deletionTimestamp").is_some()
        && job
            .pointer("/metadata/finalizers")
            .and_then(Value::as_array)
            .is_some_and(|f| f.iter().any(|x| x == "foregroundDeletion"))
}

#[tokio::test]
async fn handlers_enqueue_only_live_jobs_that_need_cleanup() {
    // `addJob` / `updateJob`: no `deletionTimestamp` and `needsCleanup`; the
    // informer has no `DeleteFunc`.
    let c = TTLAfterFinishedController::new(Arc::new(MemoryStorage::new()));
    let now = Utc::now();
    let finished = new_job(Some(now), None, Some(10));

    c.on_job_event(&StoreEvent::Added(finished.clone())).await;
    assert_eq!(c.queue().len().await, 1);
    // Same key: the queue dedups.
    c.on_job_event(&StoreEvent::Modified(finished.clone()))
        .await;
    assert_eq!(c.queue().len().await, 1);

    let c = TTLAfterFinishedController::new(Arc::new(MemoryStorage::new()));
    c.on_job_event(&StoreEvent::Added(new_job(None, None, Some(10))))
        .await;
    c.on_job_event(&StoreEvent::Added(new_job(Some(now), None, None)))
        .await;
    let mut deleting = finished.clone();
    deleting.metadata.deletion_timestamp = Some(now);
    c.on_job_event(&StoreEvent::Modified(deleting)).await;
    c.on_job_event(&StoreEvent::Deleted(finished)).await;
    assert_eq!(c.queue().len().await, 0);
}

#[tokio::test]
async fn expired_job_is_deleted_with_foreground_propagation() {
    // `processJob`: delete with `PropagationPolicy: Foreground` once the TTL
    // truly expired; observe `job_deletion_duration_seconds`.
    let s = Arc::new(MemoryStorage::new());
    put_job(&s, "expired", Some(Utc::now() - Duration::seconds(30)), 10).await;
    put_job(&s, "live", Some(Utc::now()), 3600).await;
    put_job(&s, "unfinished", None, 0).await;
    let before = job_deletion_observations();
    let _c = start(&s);

    let job = eventually("expired Job is foreground-deleted", || async {
        get_job(&s, "expired").await.filter(is_foreground_deleting)
    })
    .await;
    assert_eq!(job["metadata"]["uid"], "uid-expired");
    assert!(job_deletion_observations() > before);

    for name in ["live", "unfinished"] {
        let j = get_job(&s, name).await.unwrap();
        assert!(!is_foreground_deleting(&j), "{name} must be left alone");
    }
}

#[tokio::test]
async fn unexpired_job_is_requeued_for_when_the_ttl_expires() {
    // `processTTL`: a Job whose TTL has not expired is `AddAfter`-ed to the
    // moment it is expected to expire, and deleted then, with no resync.
    let s = Arc::new(MemoryStorage::new());
    put_job(&s, "soon", Some(Utc::now()), 2).await;
    let c = start(&s);

    // Not deleted at first...
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!is_foreground_deleting(&get_job(&s, "soon").await.unwrap()));
    // ...then deleted once the TTL has run out.
    eventually("Job deleted after its TTL", || async {
        get_job(&s, "soon").await.filter(is_foreground_deleting)
    })
    .await;
    drop(c);
}

#[tokio::test]
async fn process_job_ignores_unknown_and_malformed_keys() {
    let s = Arc::new(MemoryStorage::new());
    let c = TTLAfterFinishedController::new(s);
    // Not in the informer store: `errors.IsNotFound(err)` -> nil.
    assert!(c.process_job("default/missing").await.is_ok());
    // `cache.SplitMetaNamespaceKey` rejects a key with two separators.
    assert!(c.process_job("a/b/c").await.is_err());
}

#[tokio::test]
async fn foreground_delete_honours_the_uid_precondition() {
    // `Preconditions{UID: &fresh.UID}`: a Job recreated under the same name
    // (different UID) is not deleted by a delete aimed at the old one.
    let s = MemoryStorage::new();
    put_job(&s, "j", Some(Utc::now()), 0).await;
    let key = build_key("jobs", Some("default"), "j");

    let err = s
        .delete_foreground_with_preconditions(&key, &Preconditions::new_uid("someone-else"))
        .await
        .unwrap_err();
    assert!(matches!(err, k3rs_common::Error::Conflict(_)), "{err}");
    assert!(!is_foreground_deleting(&get_job(&s, "j").await.unwrap()));

    s.delete_foreground_with_preconditions(&key, &Preconditions::new_uid("uid-j"))
        .await
        .unwrap();
    assert!(is_foreground_deleting(&get_job(&s, "j").await.unwrap()));
}
