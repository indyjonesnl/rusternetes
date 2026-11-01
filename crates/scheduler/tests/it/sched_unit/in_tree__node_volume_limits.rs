//! NodeVolumeLimits PreFilter/Filter through the plugin object and the
//! framework (`nodevolumelimits/csi.go:219-229`, `:236-345`); the limit
//! arithmetic itself is covered in `node_volume_limits.rs`.
use async_trait::async_trait;
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{
    Code, CycleState, FilterPlugin, Handle, NoHandle, NodeInfo, PreFilterPlugin,
};
use rusternetes_scheduler::in_tree::NodeVolumeLimits;
use rusternetes_scheduler::node_volume_limits::NodeVolumeLimitsSnapshot;
use rusternetes_scheduler::profile::Profile;
use serde_json::json;

struct Listers(Result<(), String>);

#[async_trait]
impl Handle for Listers {
    async fn node_volume_limits_snapshot(&self) -> Result<NodeVolumeLimitsSnapshot, String> {
        self.0.clone().map(|_| NodeVolumeLimitsSnapshot::default())
    }
}

fn pod(volumes: serde_json::Value) -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": "p", "namespace": "default"},
        "spec": {"containers": [{"name": "c"}], "volumes": volumes}
    }))
    .unwrap()
}

fn node() -> Node {
    serde_json::from_value(json!({"metadata": {"name": "n"}})).unwrap()
}

#[tokio::test]
async fn pod_without_pvc_volumes_is_skipped() {
    let p = pod(json!([{"name": "v", "emptyDir": {}}]));
    let (r, s) = NodeVolumeLimits
        .pre_filter(&mut CycleState::new(), &p, &[], &[], &NoHandle)
        .await;
    assert!(r.is_none() && s.is_skip());
}

#[tokio::test]
async fn snapshot_failure_is_a_pre_filter_error() {
    let p = pod(json!([{"name": "v", "persistentVolumeClaim": {"claimName": "c"}}]));
    let (_, s) = NodeVolumeLimits
        .pre_filter(
            &mut CycleState::new(),
            &p,
            &[],
            &[],
            &Listers(Err("boom".into())),
        )
        .await;
    assert_eq!(s.code(), Code::Error);
    // Through the framework the error is wrapped and names the plugin.
    let fw = Profile::named("default-scheduler").framework;
    let (_, s, _) = fw
        .run_pre_filter_plugins(
            &mut CycleState::new(),
            &p,
            &[],
            &[],
            &Listers(Err("boom".into())),
        )
        .await;
    assert_eq!(s.code(), Code::Error);
    assert_eq!(
        s.message(),
        "running PreFilter plugin \"NodeVolumeLimits\": boom"
    );
}

#[tokio::test]
async fn missing_pvc_is_unresolvable_on_filter() {
    let p = pod(json!([{"name": "v", "persistentVolumeClaim": {"claimName": "gone"}}]));
    let mut state = CycleState::new();
    let (_, s) = NodeVolumeLimits
        .pre_filter(&mut state, &p, &[], &[], &Listers(Ok(())))
        .await;
    assert!(s.is_success());
    let n = node();
    let pods: Vec<Pod> = vec![];
    let s = NodeVolumeLimits
        .filter(&state, &p, &NodeInfo::new(&n, &pods))
        .await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
}
