//! #3454 / #3460: the Score extension point as upstream runs it
//! (`RunScorePlugins`, `prioritizeNodes`): each plugin scores every feasible
//! node, `NormalizeScore` maps the plugin's scores to [0, 100], and the
//! weighted sum picks the node.
//!
//! Ported from:
//! - `pkg/scheduler/framework/runtime/framework.go` `RunScorePlugins` :1273-1382
//! - `pkg/scheduler/schedule_one.go` `prioritizeNodes` :791-901,
//!   `schedulePod` :425-480 (single feasible node short-circuit)
//! - `noderesources/{fit,least_allocated,most_allocated,balanced_allocation}.go`
//! - `tainttoleration/taint_toleration.go` `Score`/`NormalizeScore`
//! - `imagelocality/image_locality.go`

use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::scheduler::Scheduler;
use rusternetes_scheduler::scheduler_config::profiles_from_config;
use rusternetes_storage::{memory::MemoryStorage, Storage};
use serde_json::json;
use std::sync::Arc;

fn node(name: &str, extra: serde_json::Value) -> Node {
    let mut v = json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": name, "uid": format!("{name}-uid"),
                     "labels": {"kubernetes.io/hostname": name}},
        "status": {
            "capacity": {"cpu": "4", "memory": "8Gi", "pods": "110"},
            "allocatable": {"cpu": "4", "memory": "8Gi", "pods": "110"}
        }
    });
    if let Some(o) = extra.as_object() {
        for (k, val) in o {
            if k == "spec" {
                v["spec"] = val.clone();
            } else {
                v["status"][k] = val.clone();
            }
        }
    }
    serde_json::from_value(v).unwrap()
}

fn running(name: &str, on: &str, cpu: &str, mem: &str) -> Pod {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "uid": format!("{name}-uid")},
        "spec": {
            "nodeName": on,
            "containers": [{"name": "c", "image": "busybox",
                "resources": {"requests": {"cpu": cpu, "memory": mem}}}]
        },
        "status": {"phase": "Running"}
    }))
    .unwrap()
}

fn incoming(cpu: &str, mem: &str, image: &str) -> Pod {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p", "namespace": "default", "uid": "p-uid"},
        "spec": {
            "schedulerName": "default-scheduler",
            "containers": [{"name": "c", "image": image,
                "resources": {"requests": {"cpu": cpu, "memory": mem}}}]
        },
        "status": {"phase": "Pending"}
    }))
    .unwrap()
}

async fn schedule(doc: &str, nodes: Vec<Node>, existing: Vec<Pod>, pod: Pod) -> Option<String> {
    let profiles = profiles_from_config(doc, false).unwrap();
    let storage = Arc::new(MemoryStorage::new());
    for n in nodes {
        storage
            .create(&format!("/registry/nodes/{}", n.metadata.name), &n)
            .await
            .unwrap();
    }
    for p in existing {
        storage
            .create(&format!("/registry/pods/default/{}", p.metadata.name), &p)
            .await
            .unwrap();
    }
    storage
        .create("/registry/pods/default/p", &pod)
        .await
        .unwrap();
    let s = Scheduler::new_with_profiles(storage.clone(), 1, profiles);
    s.schedule_pending_pods().await.unwrap();
    storage
        .get::<Pod>("/registry/pods/default/p")
        .await
        .unwrap()
        .spec
        .and_then(|s| s.node_name)
}

const DEFAULT_DOC: &str = "profiles:\n- schedulerName: default-scheduler\n";

const MOST_ALLOCATED_DOC: &str = r#"
profiles:
- schedulerName: default-scheduler
  pluginConfig:
  - name: NodeResourcesFit
    args:
      scoringStrategy:
        type: MostAllocated
"#;

/// least_allocated.go / most_allocated.go: the strategy in
/// `NodeResourcesFitArgs.scoringStrategy` decides whether the busy or the idle
/// node wins (`fit.go` `nodeResourceStrategyTypeMap`). BalancedAllocation is
/// disabled so only the Fit strategy differs.
#[tokio::test]
async fn fit_scoring_strategy_most_allocated_bin_packs() {
    let nodes = || vec![node("a-busy", json!({})), node("b-idle", json!({}))];
    let existing = || vec![running("e1", "a-busy", "2", "4Gi")];
    let doc = |strategy: &str| {
        format!(
            r#"
profiles:
- schedulerName: default-scheduler
  plugins:
    score:
      disabled:
      - name: NodeResourcesBalancedAllocation
  pluginConfig:
  - name: NodeResourcesFit
    args:
      scoringStrategy:
        type: {strategy}
"#
        )
    };
    let least = schedule(
        &doc("LeastAllocated"),
        nodes(),
        existing(),
        incoming("1", "1Gi", "busybox"),
    )
    .await;
    assert_eq!(least.as_deref(), Some("b-idle"));
    let most = schedule(
        &doc("MostAllocated"),
        nodes(),
        existing(),
        incoming("1", "1Gi", "busybox"),
    )
    .await;
    assert_eq!(most.as_deref(), Some("a-busy"));
    // The default profile (LeastAllocated) agrees with the explicit one.
    let default = schedule(
        DEFAULT_DOC,
        nodes(),
        existing(),
        incoming("1", "1Gi", "busybox"),
    )
    .await;
    assert_eq!(default.as_deref(), Some("b-idle"));
    let _ = MOST_ALLOCATED_DOC;
}

/// balanced_allocation.go `balancedResourceScorer`: with equal Fit averages
/// the node whose cpu and memory fractions are closer wins; the compiled-in
/// blend had no such term.
///
/// A: cpu 3/4, mem 2/8 -> Fit (25+75)/2=50, Balanced (1-0.25)*100=75
/// B: cpu 2/4, mem 5/8 -> Fit (50+37)/2=43, Balanced int((1-0.0625)*100)=93
/// Totals (weights 1/1): A 125, B 136.
#[tokio::test]
async fn balanced_allocation_scores_participate() {
    let nodes = vec![node("a", json!({})), node("b", json!({}))];
    let existing = vec![
        running("ea", "a", "2", "1Gi"),
        running("eb", "b", "1", "4Gi"),
    ];
    let got = schedule(
        DEFAULT_DOC,
        nodes,
        existing,
        incoming("1", "1Gi", "busybox"),
    )
    .await;
    assert_eq!(got.as_deref(), Some("b"));
}

/// taint_toleration.go `Score` + `NormalizeScore` (reverse default
/// normalisation), weight 3: a node with an intolerable PreferNoSchedule
/// taint loses to a clean node even though the clean node already runs a pod.
#[tokio::test]
async fn taint_toleration_prefers_node_without_prefer_no_schedule_taint() {
    let tainted = node(
        "a-tainted",
        json!({"spec": {"taints": [
            {"key": "k", "value": "v", "effect": "PreferNoSchedule"}
        ]}}),
    );
    let clean = node("b-clean", json!({}));
    let existing = vec![running("e", "b-clean", "100m", "100Mi")];
    let got = schedule(
        DEFAULT_DOC,
        vec![tainted, clean],
        existing,
        incoming("100m", "100Mi", "busybox"),
    )
    .await;
    assert_eq!(got.as_deref(), Some("b-clean"));
}

/// image_locality.go: a node that already has the image scores higher.
#[tokio::test]
async fn image_locality_prefers_node_with_image() {
    let has_image = json!({"images": [
        {"names": ["registry.example/app:v1"], "sizeBytes": 800_000_000u64}
    ]});
    let nodes = vec![node("a", json!({})), node("b", has_image)];
    let got = schedule(
        DEFAULT_DOC,
        nodes,
        vec![],
        incoming("100m", "100Mi", "registry.example/app:v1"),
    )
    .await;
    assert_eq!(got.as_deref(), Some("b"));
}

/// `NodeResourcesFitArgs.IgnoredResources` (fit.go:218): through the profile,
/// a pod requesting an extended resource no node advertises is scheduled only
/// when that resource is ignored.
#[tokio::test]
async fn fit_ignored_resources_from_the_profile() {
    let pod_wanting_foo = || -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "default", "uid": "p-uid"},
            "spec": {"containers": [{"name": "c", "image": "x",
                "resources": {"requests": {"example.com/foo": "1"}}}]}
        }))
        .unwrap()
    };
    let nodes = || vec![node("a", json!({}))];
    assert_eq!(
        schedule(DEFAULT_DOC, nodes(), vec![], pod_wanting_foo()).await,
        None
    );
    let doc = r#"
profiles:
- schedulerName: default-scheduler
  pluginConfig:
  - name: NodeResourcesFit
    args:
      ignoredResources:
      - example.com/foo
"#;
    assert_eq!(
        schedule(doc, nodes(), vec![], pod_wanting_foo())
            .await
            .as_deref(),
        Some("a")
    );
}
