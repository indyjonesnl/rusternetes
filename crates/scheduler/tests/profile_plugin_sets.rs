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
//!   different decisions). Upstream's out-of-tree `FakeNodeSelector` has no
//!   Rusternetes equivalent (no out-of-tree registry), so the in-tree
//!   `TaintToleration` filter is disabled in one profile instead.

use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::profile::Map;
use rusternetes_scheduler::scheduler::Scheduler;
use rusternetes_scheduler::scheduler_config::profiles_from_config;
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
    for (name, sn) in [("p-default", "default-scheduler"), ("p-free", "ignores-taints")] {
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
