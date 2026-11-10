//! #3401: per-profile plugin sets (`plugins.enabled/disabled`, `multiPoint`).
//!
//! Ported from:
//! - `pkg/scheduler/profile/profile_test.go` `TestNewMap` "invalid framework
//!   configuration" (`at least one bind plugin is needed`) and `NewMap`'s
//!   wrapped error (`profile.go:57`);
//! - `pkg/scheduler/framework/runtime/framework_test.go`
//!   `TestInitFrameworkWithScorePlugins` ("enabled Score plugin doesn't exist
//!   in registry");
//! - `pkg/scheduler/schedule_one_test.go` `TestSchedulerMultipleProfilesScheduling`
//!   (two profiles whose plugin sets differ route the same cluster to
//!   different decisions). The out-of-tree `FakeNodeSelector` of that test is
//!   ported below (`multiple_profiles_with_out_of_tree_fake_node_selector`),
//!   registered through `Registry::register_factory`
//!   (`WithFrameworkOutOfTreeRegistry`); the in-tree `TaintToleration` variant
//!   stays as a second shape.

use async_trait::async_trait;
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::framework_runtime::{ExtensionPoint, PluginInstance, Registry};
use rusternetes_scheduler::fwk::{Code, CycleState, FilterPlugin, NodeInfo, Plugin, Status};
use rusternetes_scheduler::profile::Map;
use rusternetes_scheduler::scheduler::Scheduler;
use rusternetes_scheduler::scheduler_config::{
    profiles_from_config, profiles_from_config_with_registry,
};
use rusternetes_storage::{memory::MemoryStorage, Storage};
use serde_json::json;
use std::sync::Arc;

fn profiles(doc: &str) -> Result<Map, String> {
    profiles_from_config(doc, false)
}

#[test]
fn unknown_filter_plugin_fails_profile_construction() {
    let doc = r#"
profiles:
- schedulerName: my-sched
  plugins:
    filter:
      enabled:
      - name: NoSuchPlugin
"#;
    let err = profiles(doc).unwrap_err();
    assert!(
        err.contains(
            "creating profile for scheduler name my-sched: FilterPlugin \"NoSuchPlugin\" does not exist"
        ),
        "{err}"
    );
}

#[test]
fn profile_without_bind_plugin_is_invalid_framework() {
    // multiPoint "*" disabled drops every default plugin; only queueSort is
    // enabled, as profile_test.go "invalid framework configuration".
    let doc = r#"
profiles:
- schedulerName: invalid-profile
  plugins:
    multiPoint:
      disabled:
      - name: '*'
    queueSort:
      enabled:
      - name: PrioritySort
"#;
    let err = profiles(doc).unwrap_err();
    assert!(
        err.contains(
            "creating profile for scheduler name invalid-profile: at least one bind plugin is needed for profile with scheduler name \"invalid-profile\""
        ),
        "{err}"
    );
}

fn tainted_node() -> Node {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": "tainted", "uid": "n-uid"},
        "spec": {"taints": [{"key": "dedicated", "value": "x", "effect": "NoSchedule"}]},
        "status": {
            "capacity": {"cpu": "4", "memory": "8Gi", "pods": "110"},
            "allocatable": {"cpu": "4", "memory": "8Gi", "pods": "110"}
        }
    }))
    .unwrap()
}

fn pod(name: &str, scheduler: &str) -> Pod {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "uid": format!("{name}-uid")},
        "spec": {
            "schedulerName": scheduler,
            "containers": [{"name": "c", "image": "busybox"}]
        },
        "status": {"phase": "Pending"}
    }))
    .unwrap()
}

async fn node_of(storage: &MemoryStorage, name: &str) -> Option<String> {
    storage
        .get::<Pod>(&format!("/registry/pods/default/{name}"))
        .await
        .unwrap()
        .spec
        .and_then(|s| s.node_name)
}

/// Two profiles over one cluster: the profile that disables the
/// `TaintToleration` filter schedules onto the tainted node, the default
/// profile cannot (TestSchedulerMultipleProfilesScheduling shape).
#[tokio::test]
async fn per_profile_filter_sets_route_pods_differently() {
    let doc = r#"
profiles:
- schedulerName: default-scheduler
- schedulerName: ignores-taints
  plugins:
    filter:
      disabled:
      - name: TaintToleration
"#;
    let profiles = profiles(doc).unwrap();
    let storage = Arc::new(MemoryStorage::new());
    storage
        .create("/registry/nodes/tainted", &tainted_node())
        .await
        .unwrap();
    for (name, sn) in [
        ("p-default", "default-scheduler"),
        ("p-free", "ignores-taints"),
    ] {
        storage
            .create(&format!("/registry/pods/default/{name}"), &pod(name, sn))
            .await
            .unwrap();
    }
    let scheduler = Scheduler::new_with_profiles(storage.clone(), 1, profiles);
    scheduler.schedule_pending_pods().await.unwrap();
    assert_eq!(node_of(&storage, "p-default").await, None);
    assert_eq!(
        node_of(&storage, "p-free").await.as_deref(),
        Some("tainted")
    );
}

fn plain_node(name: &str, labels: serde_json::Value) -> Node {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": name, "uid": format!("{name}-uid"), "labels": labels},
        "status": {
            "capacity": {"cpu": "4", "memory": "8Gi", "pods": "110"},
            "allocatable": {"cpu": "4", "memory": "8Gi", "pods": "110"}
        }
    }))
    .unwrap()
}

fn pod_preferring_ssd(name: &str, scheduler: &str) -> Pod {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "uid": format!("{name}-uid")},
        "spec": {
            "schedulerName": scheduler,
            "containers": [{"name": "c", "image": "busybox"}],
            "affinity": {"nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [{
                "weight": 100,
                "preference": {"matchExpressions": [
                    {"key": "disk", "operator": "In", "values": ["ssd"]}
                ]}
            }]}}
        },
        "status": {"phase": "Pending"}
    }))
    .unwrap()
}

/// Score plugin sets differ per profile: the default profile scores
/// `NodeAffinity` and follows the preferred term, the profile that disables
/// `NodeAffinity` at Score does not.
#[tokio::test]
async fn per_profile_score_sets_route_pods_differently() {
    let doc = r#"
profiles:
- schedulerName: default-scheduler
- schedulerName: no-affinity-score
  plugins:
    score:
      disabled:
      - name: NodeAffinity
"#;
    // A fresh cluster per profile so the first placement cannot skew the
    // second pod's resource scores.
    let run = |sn: &'static str| {
        let profiles = profiles(doc).unwrap();
        async move {
            let storage = Arc::new(MemoryStorage::new());
            storage
                .create("/registry/nodes/n1", &plain_node("n1", json!({})))
                .await
                .unwrap();
            storage
                .create(
                    "/registry/nodes/n2",
                    &plain_node("n2", json!({"disk": "ssd"})),
                )
                .await
                .unwrap();
            storage
                .create("/registry/pods/default/p", &pod_preferring_ssd("p", sn))
                .await
                .unwrap();
            let scheduler = Scheduler::new_with_profiles(storage.clone(), 1, profiles);
            scheduler.schedule_pending_pods().await.unwrap();
            node_of(&storage, "p").await
        }
    };
    assert_eq!(run("default-scheduler").await.as_deref(), Some("n2"));
    assert_eq!(run("no-affinity-score").await.as_deref(), Some("n1"));
}

/// `fakeNodeSelector` (schedule_one_test.go:306-330): admits only the node
/// named in its `pluginConfig` args (`{"nodeName": ...}`).
struct FakeNodeSelector {
    node_name: String,
}

impl Plugin for FakeNodeSelector {
    fn name(&self) -> &'static str {
        "FakeNodeSelector"
    }
}

#[async_trait]
impl FilterPlugin for FakeNodeSelector {
    async fn filter(&self, _state: &CycleState, _pod: &Pod, node_info: &NodeInfo<'_>) -> Status {
        if node_info.node().metadata.name != self.node_name {
            return Status::new(Code::UnschedulableAndUnresolvable, Vec::new());
        }
        Status::success()
    }
}

/// `newFakeNodeSelector`: decodes the args (`frameworkruntime.DecodeInto`).
fn out_of_tree_registry() -> Registry {
    let mut r = Registry::in_tree();
    r.register_factory(
        "FakeNodeSelector",
        &[ExtensionPoint::Filter],
        Arc::new(|args| {
            let node_name = args
                .and_then(|a| a.get("nodeName"))
                .and_then(|n| n.as_str())
                .ok_or("missing nodeName")?
                .to_string();
            Ok(PluginInstance {
                filter: Some(Arc::new(FakeNodeSelector { node_name })),
                ..Default::default()
            })
        }),
    )
    .unwrap();
    r
}

const FAKE_NODE_SELECTOR_CONFIG: &str = r#"
profiles:
- schedulerName: match-node2
  plugins:
    filter:
      enabled:
      - name: FakeNodeSelector
  pluginConfig:
  - name: FakeNodeSelector
    args:
      nodeName: node2
- schedulerName: match-node3
  plugins:
    filter:
      enabled:
      - name: FakeNodeSelector
  pluginConfig:
  - name: FakeNodeSelector
    args:
      nodeName: node3
"#;

/// `TestSchedulerMultipleProfilesScheduling` (schedule_one_test.go:400-): two
/// profiles run the same out-of-tree filter configured for different nodes,
/// and each pod is bound by the profile named in its `schedulerName`.
#[tokio::test]
async fn multiple_profiles_with_out_of_tree_fake_node_selector() {
    let profiles = profiles_from_config_with_registry(
        FAKE_NODE_SELECTOR_CONFIG,
        false,
        &out_of_tree_registry(),
    )
    .unwrap();
    let storage = Arc::new(MemoryStorage::new());
    for n in ["node1", "node2", "node3"] {
        storage
            .create(&format!("/registry/nodes/{n}"), &plain_node(n, json!({})))
            .await
            .unwrap();
    }
    let pods = [
        ("pod1", "match-node3", "node3"),
        ("pod2", "match-node2", "node2"),
        ("pod3", "match-node2", "node2"),
        ("pod4", "match-node3", "node3"),
    ];
    for (name, sn, _) in pods {
        storage
            .create(&format!("/registry/pods/default/{name}"), &pod(name, sn))
            .await
            .unwrap();
    }
    let scheduler = Scheduler::new_with_profiles(storage.clone(), 1, profiles);
    scheduler.schedule_pending_pods().await.unwrap();
    for (name, _, want) in pods {
        assert_eq!(
            node_of(&storage, name).await.as_deref(),
            Some(want),
            "{name}"
        );
    }
}

/// Without the out-of-tree registry the plugin is unknown, as `NewFramework`
/// reports for a name missing from the registry.
#[test]
fn out_of_tree_plugin_is_unknown_without_its_registry() {
    let err = profiles(FAKE_NODE_SELECTOR_CONFIG).unwrap_err();
    assert!(
        err.contains("FilterPlugin \"FakeNodeSelector\" does not exist"),
        "{err}"
    );
}

/// A failing factory is reported as `initializing plugin "<name>": <err>`
/// (framework.go:392-394).
#[test]
fn out_of_tree_factory_error_is_wrapped() {
    let doc = r#"
profiles:
- schedulerName: bad
  plugins:
    filter:
      enabled:
      - name: FakeNodeSelector
"#;
    let err = profiles_from_config_with_registry(doc, false, &out_of_tree_registry()).unwrap_err();
    assert!(
        err.contains("initializing plugin \"FakeNodeSelector\": missing nodeName"),
        "{err}"
    );
}

/// `Registry.Register` rejects a duplicate name, and a plugin must implement
/// every extension point it is registered at.
#[test]
fn registry_rejects_duplicates_and_unimplemented_points() {
    let mut r = out_of_tree_registry();
    let err = r
        .register_factory(
            "FakeNodeSelector",
            &[ExtensionPoint::Filter],
            Arc::new(|_| Ok(PluginInstance::default())),
        )
        .unwrap_err();
    assert_eq!(err, "a plugin named FakeNodeSelector already exists");

    r.register_factory(
        "Liar",
        &[ExtensionPoint::Filter, ExtensionPoint::Score],
        Arc::new(|_| {
            Ok(PluginInstance {
                filter: Some(Arc::new(FakeNodeSelector {
                    node_name: String::new(),
                })),
                ..Default::default()
            })
        }),
    )
    .unwrap();
    let doc = r#"
profiles:
- schedulerName: liar
  plugins:
    filter:
      enabled:
      - name: Liar
"#;
    let err = profiles_from_config_with_registry(doc, false, &r).unwrap_err();
    assert!(
        err.contains("initializing plugin \"Liar\"") && err.contains("does not implement"),
        "{err}"
    );

    let err = r
        .register_factory(
            "Post",
            &[ExtensionPoint::PostFilter],
            Arc::new(|_| Ok(PluginInstance::default())),
        )
        .unwrap_err();
    assert!(err.contains("cannot be registered out of tree"), "{err}");
}
