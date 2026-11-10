//! Out-of-tree QueueSort and PreEnqueue plugins (#3455).
//!
//! Ported mechanism: `frameworkImpl.QueueSortFunc` / `PreEnqueuePlugins`
//! (`pkg/scheduler/framework/runtime/framework.go:731-755`) feeding
//! `scheduler.New` (`pkg/scheduler/scheduler.go:386-414`), and `NewFramework`'s
//! "only one queue sort plugin" check (`framework.go:414`).

use rusternetes_common::resources::{Pod, PodSpec};
use rusternetes_scheduler::framework_runtime::{ExtensionPoint, PluginInstance, Registry};
use rusternetes_scheduler::fwk::{Code, Plugin, QueueSortPlugin, Status};
use rusternetes_scheduler::queue::scheduling_gates::build_pre_enqueue_plugin_map;
use rusternetes_scheduler::queue::scheduling_queue::{
    PreEnqueuePlugin, PriorityQueue, PriorityQueueOptions, QueuedPodInfo,
};
use rusternetes_scheduler::scheduler_config::profiles_from_config_with_registry;
use std::sync::Arc;

/// Lowest priority first (the reverse of `PrioritySort`), so the test can tell
/// which `Less` the queue uses.
struct LowestPriorityFirst;

impl Plugin for LowestPriorityFirst {
    fn name(&self) -> &'static str {
        "LowestPriorityFirst"
    }
}

impl QueueSortPlugin for LowestPriorityFirst {
    fn less(&self, a: &QueuedPodInfo, b: &QueuedPodInfo) -> bool {
        let p = |i: &QueuedPodInfo| i.pod.spec.as_ref().and_then(|s| s.priority).unwrap_or(0);
        p(a) < p(b)
    }
}

/// Gates pods labelled `hold=true`.
struct HoldLabelGate;

impl PreEnqueuePlugin for HoldLabelGate {
    fn name(&self) -> &str {
        "HoldLabelGate"
    }
    fn pre_enqueue(&self, pod: &Pod) -> Status {
        let held = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("hold"))
            .is_some_and(|v| v == "true");
        if held {
            Status::new(Code::Unschedulable, vec!["pod is on hold".to_string()])
        } else {
            Status::success()
        }
    }
}

fn registry() -> Registry {
    let mut r = Registry::in_tree();
    r.register_factory(
        "LowestPriorityFirst",
        &[ExtensionPoint::QueueSort],
        Arc::new(|_| {
            Ok(PluginInstance {
                queue_sort: Some(Arc::new(LowestPriorityFirst)),
                ..Default::default()
            })
        }),
    )
    .unwrap();
    r.register_factory(
        "HoldLabelGate",
        &[ExtensionPoint::PreEnqueue],
        Arc::new(|_| {
            Ok(PluginInstance {
                pre_enqueue: Some(Arc::new(HoldLabelGate)),
                ..Default::default()
            })
        }),
    )
    .unwrap();
    r
}

const CONFIG: &str = r#"
profiles:
- schedulerName: custom
  plugins:
    queueSort:
      enabled:
      - name: LowestPriorityFirst
      disabled:
      - name: PrioritySort
    preEnqueue:
      enabled:
      - name: HoldLabelGate
"#;

fn pod_with(name: &str, priority: i32, hold: bool) -> Pod {
    let mut pod = Pod::new(
        name,
        PodSpec {
            priority: Some(priority),
            scheduler_name: Some("custom".to_string()),
            ..Default::default()
        },
    );
    pod.metadata.uid = name.to_string();
    if hold {
        pod.metadata.labels = Some([("hold".to_string(), "true".to_string())].into());
    }
    pod
}

/// The profile's QueueSort plugin orders activeQ and its PreEnqueue plugins
/// gate pods, as `scheduler.New` wires them.
#[tokio::test]
async fn out_of_tree_queue_sort_and_pre_enqueue_drive_the_queue() {
    let profiles = profiles_from_config_with_registry(CONFIG, false, &registry()).unwrap();
    let profile = profiles.profiles().next().unwrap();
    let queue = PriorityQueue::new(
        profile.framework.queue_sort_func(),
        PriorityQueueOptions {
            pre_enqueue_plugin_map: build_pre_enqueue_plugin_map([profile]),
            ..Default::default()
        },
    );
    queue.add(pod_with("high", 100, false));
    queue.add(pod_with("low", 1, false));
    queue.add(pod_with("held", 50, true));
    // The gated pod never reaches activeQ.
    assert_eq!(queue.pods_in_active_q().len(), 2);
    assert_eq!(queue.unschedulable_pods().len(), 1);
    // LowestPriorityFirst, not PrioritySort, orders activeQ.
    assert_eq!(queue.pop().await.unwrap().pod.metadata.name, "low");
    assert_eq!(queue.pop().await.unwrap().pod.metadata.name, "high");
}

/// framework.go:414: exactly one QueueSort plugin per profile.
#[test]
fn two_queue_sort_plugins_are_rejected() {
    let doc = r#"
profiles:
- schedulerName: custom
  plugins:
    queueSort:
      enabled:
      - name: LowestPriorityFirst
"#;
    let err = profiles_from_config_with_registry(doc, false, &registry()).unwrap_err();
    assert!(
        err.contains(
            "only one queue sort plugin required for profile with scheduler name \"custom\", but got 2"
        ),
        "{err}"
    );
}

/// framework.go:376-393: a factory whose plugin lacks the extension point it
/// was registered at fails the profile.
#[test]
fn queue_sort_factory_without_queue_sort_object_is_rejected() {
    let mut r = Registry::in_tree();
    r.register_factory(
        "Broken",
        &[ExtensionPoint::QueueSort],
        Arc::new(|_| Ok(PluginInstance::default())),
    )
    .unwrap();
    let doc = r#"
profiles:
- schedulerName: custom
  plugins:
    queueSort:
      enabled:
      - name: Broken
      disabled:
      - name: PrioritySort
"#;
    let err = profiles_from_config_with_registry(doc, false, &r).unwrap_err();
    assert!(err.contains("does not implement it"), "{err}");
}

/// Default profile: PrioritySort and SchedulingGates are framework objects.
#[test]
fn default_profile_objects() {
    let p = rusternetes_scheduler::profile::Profile::named("default-scheduler");
    let names: Vec<_> = p
        .framework
        .pre_enqueue_plugins()
        .iter()
        .map(|pl| pl.name().to_string())
        .collect();
    assert_eq!(names, ["SchedulingGates"]);
    let less = p.framework.queue_sort_func();
    let hi = QueuedPodInfo::for_lookup(pod_with("hi", 10, false), &[]);
    let lo = QueuedPodInfo::for_lookup(pod_with("lo", 1, false), &[]);
    assert!(less(&hi, &lo));
    assert!(!less(&lo, &hi));
}
