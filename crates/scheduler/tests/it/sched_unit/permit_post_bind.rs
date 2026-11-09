//! Permit and PostBind: `RunPermitPlugins`, `WaitOnPermit`, the waiting-pods
//! map and `RunPostBindPlugins`, and their place in the binding cycle.
//!
//! Ports of `pkg/scheduler/framework/runtime/framework_test.go`
//! `TestPermitPlugins` and `TestWaitOnPermit`, plus the behaviour of
//! `waiting_pods_map.go` (`Allow` waits for every pending plugin, `Reject`
//! and the timeout reject the pod) and of `bindingCycle` /
//! `schedulingCycle` (schedule_one.go:234-256, :302-315, :344).
use async_trait::async_trait;
use rusternetes_common::resources::Pod;
use rusternetes_scheduler::framework_runtime::{Framework, MAX_PERMIT_TIMEOUT};
use rusternetes_scheduler::fwk::{
    Code, CycleState, Handle, NoHandle, PermitPlugin, Plugin, PostBindPlugin, Status,
};
use rusternetes_scheduler::profile::{Map, Profile};
use rusternetes_scheduler::queue::scheduling_queue::{
    priority_sort_less, PriorityQueue, PriorityQueueOptions,
};
use rusternetes_scheduler::scheduler::{ScheduleOutcome, Scheduler};
use rusternetes_storage::{MemoryStorage, Storage};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const INJECT_REASON: &str = "injected status";

type Log = Arc<Mutex<Vec<String>>>;

fn pod_named(name: &str, uid: &str) -> Pod {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "uid": uid},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]},
        "status": {"phase": "Pending"}
    }))
    .unwrap()
}

/// `TestPlugin` with an injected `PermitStatus` (framework_test.go:249-251).
struct FakePermit {
    name: &'static str,
    code: Code,
    timeout: Duration,
    log: Log,
}

impl Plugin for FakePermit {
    fn name(&self) -> &'static str {
        self.name
    }
}

#[async_trait]
impl PermitPlugin for FakePermit {
    async fn permit(
        &self,
        _: &CycleState,
        pod: &Pod,
        node_name: &str,
        _: &dyn Handle,
    ) -> (Status, Duration) {
        self.log.lock().unwrap().push(format!(
            "permit:{}:{}:{}",
            self.name, pod.metadata.name, node_name
        ));
        let status = match self.code {
            Code::Success => Status::success(),
            Code::Error => Status::from_error(INJECT_REASON.to_string()),
            c => Status::new(c, vec![INJECT_REASON.to_string()]),
        };
        (status, self.timeout)
    }
}

struct FakePostBind {
    name: &'static str,
    log: Log,
}

impl Plugin for FakePostBind {
    fn name(&self) -> &'static str {
        self.name
    }
}

#[async_trait]
impl PostBindPlugin for FakePostBind {
    async fn post_bind(&self, _: &CycleState, pod: &Pod, node_name: &str, _: &dyn Handle) {
        self.log.lock().unwrap().push(format!(
            "post_bind:{}:{}:{}",
            self.name, pod.metadata.name, node_name
        ));
    }
}

fn permit(name: &'static str, code: Code, timeout: Duration, log: &Log) -> Arc<dyn PermitPlugin> {
    Arc::new(FakePermit {
        name,
        code,
        timeout,
        log: log.clone(),
    })
}

fn framework(plugins: Vec<Arc<dyn PermitPlugin>>) -> Framework {
    Framework::default().with_permit_plugins(plugins)
}

async fn run_permit(f: &Framework, pod: &Pod) -> Status {
    f.run_permit_plugins(&CycleState::new(), pod, "n1", &NoHandle)
        .await
}

fn unschedulable(code: Code, plugin: &str) -> Status {
    let mut s = Status::new(code, vec![INJECT_REASON.to_string()]);
    s.set_plugin(plugin);
    s
}

/// `TestPermitPlugins` (framework_test.go:2882-3030).
#[tokio::test]
async fn permit_plugins_table() {
    let log: Log = Default::default();
    let d0 = Duration::ZERO;
    let mut err =
        Status::from_error("running Permit plugin \"TestPlugin\": injected status".to_string());
    err.set_plugin("TestPlugin");

    type Case = (&'static str, Vec<Arc<dyn PermitPlugin>>, Status);
    let cases: Vec<Case> = vec![
        ("NilPermitPlugin", vec![], Status::success()),
        (
            "SuccessPermitPlugin",
            vec![permit("TestPlugin", Code::Success, d0, &log)],
            Status::success(),
        ),
        (
            "UnschedulablePermitPlugin",
            vec![permit("TestPlugin", Code::Unschedulable, d0, &log)],
            unschedulable(Code::Unschedulable, "TestPlugin"),
        ),
        (
            "ErrorPermitPlugin",
            vec![permit("TestPlugin", Code::Error, d0, &log)],
            err.clone(),
        ),
        (
            "UnschedulableAndUnresolvablePermitPlugin",
            vec![permit(
                "TestPlugin",
                Code::UnschedulableAndUnresolvable,
                d0,
                &log,
            )],
            unschedulable(Code::UnschedulableAndUnresolvable, "TestPlugin"),
        ),
        (
            "WaitPermitPlugin",
            vec![permit("TestPlugin", Code::Wait, d0, &log)],
            Status::new(
                Code::Wait,
                vec!["one or more plugins asked to wait and no plugin rejected pod \"\"".into()],
            ),
        ),
        (
            "SuccessSuccessPermitPlugin",
            vec![
                permit("TestPlugin", Code::Success, d0, &log),
                permit("TestPlugin 1", Code::Success, d0, &log),
            ],
            Status::success(),
        ),
        (
            "ErrorAndErrorPlugins",
            vec![
                permit("TestPlugin", Code::Error, d0, &log),
                permit("TestPlugin 1", Code::Error, d0, &log),
            ],
            err,
        ),
    ];
    for (name, plugins, want) in cases {
        let f = framework(plugins);
        // The upstream test pod has an empty name and UID.
        let got = run_permit(&f, &pod_named("", "")).await;
        assert_eq!(got, want, "{name}");
    }
}

/// An erroring or rejecting plugin stops the run: the later plugin never
/// runs (RunPermitPlugins returns at once).
#[tokio::test]
async fn failure_stops_the_remaining_plugins() {
    let log: Log = Default::default();
    let f = framework(vec![
        permit("a", Code::Error, Duration::ZERO, &log),
        permit("b", Code::Success, Duration::ZERO, &log),
    ]);
    assert_eq!(
        run_permit(&f, &pod_named("p", "p")).await.code(),
        Code::Error
    );
    assert_eq!(log.lock().unwrap().len(), 1);
}

/// A Wait does not stop the plugins after it, and a later rejection wins:
/// no waiting pod is created.
#[tokio::test]
async fn wait_then_reject_is_a_rejection_without_a_waiting_pod() {
    let log: Log = Default::default();
    let f = framework(vec![
        permit("waiter", Code::Wait, Duration::from_secs(10), &log),
        permit("rejecter", Code::Unschedulable, Duration::ZERO, &log),
    ]);
    let pod = pod_named("p", "uid-p");
    let got = run_permit(&f, &pod).await;
    assert_eq!(got, unschedulable(Code::Unschedulable, "rejecter"));
    assert_eq!(log.lock().unwrap().len(), 2);
    assert!(!f.will_wait_on_permit(&pod));
    assert!(f.get_waiting_pod("uid-p").is_none());
}

/// A wait plus a success after it still waits.
#[tokio::test]
async fn wait_then_success_waits() {
    let log: Log = Default::default();
    let f = framework(vec![
        permit("waiter", Code::Wait, Duration::from_secs(10), &log),
        permit("ok", Code::Success, Duration::ZERO, &log),
    ]);
    let pod = pod_named("p", "uid-p");
    assert!(run_permit(&f, &pod).await.is_wait());
    assert!(f.will_wait_on_permit(&pod));
    assert_eq!(
        f.get_waiting_pod("uid-p").unwrap().pending_plugins(),
        vec!["waiter".to_string()]
    );
}

/// `maxTimeout` (framework.go:52).
#[test]
fn max_timeout_is_fifteen_minutes() {
    assert_eq!(MAX_PERMIT_TIMEOUT, Duration::from_secs(15 * 60));
}

/// `TestWaitOnPermit` (framework_test.go:3375-3440): "Reject Waiting Pod" and
/// "Allow Waiting Pod". The plugin waits 10 seconds; the action runs in
/// another task.
#[tokio::test]
async fn wait_on_permit_reject_and_allow() {
    for reject in [true, false] {
        let log: Log = Default::default();
        let f = framework(vec![permit(
            "TestPermitPlugin",
            Code::Wait,
            Duration::from_secs(10),
            &log,
        )]);
        let pod = pod_named("pod", "pod");
        assert_eq!(run_permit(&f, &pod).await.code(), Code::Wait);

        let wp = f.get_waiting_pod("pod").unwrap();
        tokio::spawn(async move {
            if reject {
                wp.reject("TestPermitPlugin", "reject message");
            } else {
                wp.allow("TestPermitPlugin");
            }
        });
        let got = f.wait_on_permit(&pod).await;
        if reject {
            let mut want = Status::new(Code::Unschedulable, vec!["reject message".into()]);
            want.set_plugin("TestPermitPlugin");
            assert_eq!(got, want);
        } else {
            assert_eq!(got, Status::success());
        }
        // "defer f.waitingPods.remove(pod.UID)"
        assert!(!f.will_wait_on_permit(&pod));
    }
}

/// A pod that is not waiting passes straight through `WaitOnPermit`.
#[tokio::test]
async fn wait_on_permit_of_a_pod_that_is_not_waiting_succeeds() {
    let f = Framework::default();
    assert!(f.wait_on_permit(&pod_named("p", "p")).await.is_success());
}

/// `waitingPod.Allow` signals success only once every pending plugin allowed.
#[tokio::test]
async fn allow_needs_every_pending_plugin() {
    let log: Log = Default::default();
    let f = framework(vec![
        permit("a", Code::Wait, Duration::from_secs(10), &log),
        permit("b", Code::Wait, Duration::from_secs(10), &log),
    ]);
    let pod = pod_named("p", "p");
    assert!(run_permit(&f, &pod).await.is_wait());
    let wp = f.get_waiting_pod("p").unwrap();
    let mut pending = wp.pending_plugins();
    pending.sort();
    assert_eq!(pending, ["a", "b"]);

    wp.allow("a");
    assert_eq!(wp.pending_plugins(), ["b"]);
    // Still blocked: nothing was signalled.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), f.wait_on_permit(&pod))
            .await
            .is_err()
    );
    // The timed-out wait dropped its guard and removed the pod, as a
    // cancelled goroutine's defer would; re-add by running Permit again.
    let f2 = framework(vec![
        permit("a", Code::Wait, Duration::from_secs(10), &log),
        permit("b", Code::Wait, Duration::from_secs(10), &log),
    ]);
    assert!(run_permit(&f2, &pod).await.is_wait());
    let wp = f2.get_waiting_pod("p").unwrap();
    wp.allow("a");
    wp.allow("b");
    assert!(f2.wait_on_permit(&pod).await.is_success());
}

/// The timeout of a waiting plugin rejects the pod, tagged with the plugin
/// (`newWaitingPod`, waiting_pods_map.go:96-104).
#[tokio::test]
async fn a_timeout_rejects_the_pod() {
    let log: Log = Default::default();
    let f = framework(vec![permit(
        "slow",
        Code::Wait,
        Duration::from_millis(50),
        &log,
    )]);
    let pod = pod_named("p", "p");
    assert!(run_permit(&f, &pod).await.is_wait());
    let got = f.wait_on_permit(&pod).await;
    assert_eq!(got.code(), Code::Unschedulable);
    assert_eq!(got.plugin(), "slow");
    assert_eq!(
        got.message(),
        "rejected due to timeout after waiting 50ms at plugin slow"
    );
}

/// `RejectWaitingPod` (framework.go:1793-1800): rejects with "removed" and
/// reports whether the pod was waiting.
#[tokio::test]
async fn reject_waiting_pod() {
    let log: Log = Default::default();
    let f = framework(vec![permit("w", Code::Wait, Duration::from_secs(10), &log)]);
    let pod = pod_named("p", "p");
    assert!(!f.reject_waiting_pod("p"));
    assert!(run_permit(&f, &pod).await.is_wait());
    assert!(f.reject_waiting_pod("p"));
    let got = f.wait_on_permit(&pod).await;
    assert_eq!(got.code(), Code::Unschedulable);
    assert_eq!(got.plugin(), "");
    assert_eq!(got.message(), "removed");
}

/// `IterateOverWaitingPods` / `GetWaitingPod` (framework.go:1780-1791).
#[tokio::test]
async fn iterate_and_get_waiting_pods() {
    let log: Log = Default::default();
    let f = framework(vec![permit("w", Code::Wait, Duration::from_secs(10), &log)]);
    for n in ["p1", "p2"] {
        assert!(run_permit(&f, &pod_named(n, n)).await.is_wait());
    }
    let mut seen = Vec::new();
    f.iterate_over_waiting_pods(|wp| seen.push(wp.pod().metadata.name.clone()));
    seen.sort();
    assert_eq!(seen, ["p1", "p2"]);
    assert!(f.get_waiting_pod("p1").is_some());
    assert!(f.get_waiting_pod("nope").is_none());
}

/// A clone of the framework shares the waiting pods, like the one map of a
/// profile's framework upstream.
#[tokio::test]
async fn clones_share_waiting_pods() {
    let log: Log = Default::default();
    let f = framework(vec![permit("w", Code::Wait, Duration::from_secs(10), &log)]);
    let g = f.clone();
    assert!(run_permit(&f, &pod_named("p", "p")).await.is_wait());
    assert!(g.will_wait_on_permit(&pod_named("p", "p")));
}

/// `RunPostBindPlugins` (framework.go:1562-1581): every plugin runs, in order.
#[tokio::test]
async fn post_bind_plugins_run_in_order() {
    let log: Log = Default::default();
    let f = Framework::default().with_post_bind_plugins(vec![
        Arc::new(FakePostBind {
            name: "first",
            log: log.clone(),
        }),
        Arc::new(FakePostBind {
            name: "second",
            log: log.clone(),
        }),
    ]);
    f.run_post_bind_plugins(&CycleState::new(), &pod_named("p", "p"), "n1", &NoHandle)
        .await;
    assert_eq!(
        *log.lock().unwrap(),
        ["post_bind:first:p:n1", "post_bind:second:p:n1"]
    );
}

// ---------------------------------------------------------------------------
// The binding cycle
// ---------------------------------------------------------------------------

fn node() -> rusternetes_common::resources::Node {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": "n1", "uid": "n1-uid"},
        "status": {
            "capacity": {"cpu": "4", "memory": "8Gi", "pods": "110"},
            "allocatable": {"cpu": "4", "memory": "8Gi", "pods": "110"}
        }
    }))
    .unwrap()
}

struct Cluster {
    scheduler: Arc<Scheduler<MemoryStorage>>,
    storage: Arc<MemoryStorage>,
    queue: Arc<PriorityQueue>,
}

/// A scheduler whose default profile has `permit` and the recording PostBind
/// plugin, over one node and one pending pod `p` (uid `uid-p`).
async fn cluster(permit_plugins: Vec<Arc<dyn PermitPlugin>>, log: &Log) -> Cluster {
    let storage = Arc::new(MemoryStorage::new());
    storage.create("/registry/nodes/n1", &node()).await.unwrap();
    storage
        .create("/registry/pods/default/p", &pod_named("p", "uid-p"))
        .await
        .unwrap();
    let mut profiles = Map::new_map(vec![Profile::named("default-scheduler")]).unwrap();
    let profile = profiles.get_mut("default-scheduler").unwrap();
    profile.framework = profile
        .framework
        .clone()
        .with_permit_plugins(permit_plugins)
        .with_post_bind_plugins(vec![Arc::new(FakePostBind {
            name: "recorder",
            log: log.clone(),
        })]);
    Cluster {
        scheduler: Arc::new(Scheduler::new_with_profiles(storage.clone(), 1, profiles)),
        storage,
        queue: PriorityQueue::new(priority_sort_less(), PriorityQueueOptions::default()),
    }
}

async fn node_of(storage: &MemoryStorage) -> Option<String> {
    let p: Pod = storage.get("/registry/pods/default/p").await.unwrap();
    p.spec.and_then(|s| s.node_name)
}

async fn wait_for_waiting_pod(c: &Cluster) -> Arc<rusternetes_scheduler::waiting_pods::WaitingPod> {
    let fw = c
        .scheduler
        .profile_for(&pod_named("p", "uid-p"))
        .unwrap()
        .framework
        .clone();
    for _ in 0..500 {
        if let Some(wp) = fw.get_waiting_pod("uid-p") {
            return wp;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the pod never started waiting on permit");
}

#[tokio::test]
async fn permit_success_binds_then_runs_post_bind() {
    let log: Log = Default::default();
    let c = cluster(
        vec![permit("Gate", Code::Success, Duration::ZERO, &log)],
        &log,
    )
    .await;
    let out = c
        .scheduler
        .try_schedule_pod(&c.queue, "pods/default/p")
        .await;
    assert!(matches!(out, Ok(ScheduleOutcome::Done)), "{out:?}");
    assert_eq!(node_of(&c.storage).await.as_deref(), Some("n1"));
    assert_eq!(
        *log.lock().unwrap(),
        ["permit:Gate:p:n1", "post_bind:recorder:p:n1"]
    );
}

/// schedule_one.go:234-256: a rejecting Permit plugin fails the scheduling
/// cycle as a FitError naming the plugin; nothing is bound and PostBind does
/// not run.
#[tokio::test]
async fn permit_rejection_is_unschedulable_and_names_the_plugin() {
    let log: Log = Default::default();
    let c = cluster(
        vec![permit("Gate", Code::Unschedulable, Duration::ZERO, &log)],
        &log,
    )
    .await;
    let out = c
        .scheduler
        .try_schedule_pod(&c.queue, "pods/default/p")
        .await
        .unwrap();
    match out {
        ScheduleOutcome::Unschedulable(plugins, pending) => {
            assert_eq!(plugins, ["Gate".to_string()].into_iter().collect());
            assert!(pending.is_empty());
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(node_of(&c.storage).await, None);
    assert_eq!(*log.lock().unwrap(), ["permit:Gate:p:n1"]);
}

/// A Permit plugin that errors is a scheduling error, not a rejection.
#[tokio::test]
async fn permit_error_is_a_scheduling_error() {
    let log: Log = Default::default();
    let c = cluster(
        vec![permit("Gate", Code::Error, Duration::ZERO, &log)],
        &log,
    )
    .await;
    let out = c
        .scheduler
        .try_schedule_pod(&c.queue, "pods/default/p")
        .await;
    let err = out.unwrap_err().to_string();
    assert!(
        err.contains("running Permit plugin \"Gate\": injected status"),
        "{err}"
    );
    assert_eq!(node_of(&c.storage).await, None);
}

/// A waiting pod is held in the binding cycle until it is allowed, then
/// bound, then PostBind runs (bindingCycle: WaitOnPermit -> PreBind -> Bind
/// -> PostBind).
#[tokio::test]
async fn waiting_pod_binds_once_allowed() {
    let log: Log = Default::default();
    let c = cluster(
        vec![permit("Gate", Code::Wait, Duration::from_secs(60), &log)],
        &log,
    )
    .await;
    let sched = c.scheduler.clone();
    let queue = c.queue.clone();
    let task = tokio::spawn(async move { sched.try_schedule_pod(&queue, "pods/default/p").await });

    let wp = wait_for_waiting_pod(&c).await;
    assert_eq!(wp.pending_plugins(), ["Gate"]);
    assert_eq!(node_of(&c.storage).await, None, "not bound while waiting");
    assert_eq!(*log.lock().unwrap(), ["permit:Gate:p:n1"]);

    wp.allow("Gate");
    let out = task.await.unwrap();
    assert!(matches!(out, Ok(ScheduleOutcome::Done)), "{out:?}");
    assert_eq!(node_of(&c.storage).await.as_deref(), Some("n1"));
    assert_eq!(
        *log.lock().unwrap(),
        ["permit:Gate:p:n1", "post_bind:recorder:p:n1"]
    );
}

/// A waiting pod rejected while waiting fails its binding cycle: it stays
/// unbound and PostBind does not run.
#[tokio::test]
async fn waiting_pod_rejected_is_not_bound() {
    let log: Log = Default::default();
    let c = cluster(
        vec![permit("Gate", Code::Wait, Duration::from_secs(60), &log)],
        &log,
    )
    .await;
    let sched = c.scheduler.clone();
    let queue = c.queue.clone();
    let task = tokio::spawn(async move { sched.try_schedule_pod(&queue, "pods/default/p").await });

    let wp = wait_for_waiting_pod(&c).await;
    wp.reject("Gate", "no thanks");
    let out = task.await.unwrap();
    let err = out.unwrap_err().to_string();
    assert!(err.contains("no thanks"), "{err}");
    assert_eq!(node_of(&c.storage).await, None);
    assert_eq!(*log.lock().unwrap(), ["permit:Gate:p:n1"]);
}
