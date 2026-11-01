//! ImageLocality through the plugin object and `Framework::run_score_plugins`
//! (`imagelocality/image_locality_test.go`: `TestImageLocalityPriority`).
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{CycleState, NoHandle, SignPlugin};
use rusternetes_scheduler::in_tree::ImageLocality;
use rusternetes_scheduler::profile::Profile;
use serde_json::{json, Value};

const MB: u64 = 1024 * 1024;

fn pod(containers: Value) -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": "p", "namespace": "default"},
        "spec": {"containers": containers}
    }))
    .unwrap()
}

fn node(name: &str, images: Value) -> Node {
    serde_json::from_value(json!({
        "metadata": {"name": name},
        "status": {"allocatable": {"cpu": "4", "memory": "4Gi", "pods": "110"}, "images": images}
    }))
    .unwrap()
}

async fn scores(p: &Pod, nodes: &[Node]) -> Vec<i64> {
    let fw = Profile::named("default-scheduler").framework;
    let refs: Vec<&Node> = nodes.iter().collect();
    let mut state = CycleState::new();
    let s = fw
        .run_pre_score_plugins(&mut state, p, &refs, &[], nodes, &NoHandle)
        .await;
    assert!(s.is_success(), "{s:?}");
    fw.run_score_plugins(&state, p, &refs, &[], nodes)
        .unwrap()
        .into_iter()
        .find(|s| s.name == "ImageLocality")
        .map(|s| s.scores)
        .expect("ImageLocality scores")
}

#[tokio::test]
async fn image_spread_over_the_cluster_scales_the_score() {
    // 600MB image on one of two nodes: 600MB * 1/2 = 300MB, then
    // 100 * (300 - 23) / (1000 - 23) = 28.
    let nodes = vec![
        node(
            "a",
            json!([{"names": ["gcr.io/x:latest"], "sizeBytes": 600 * MB}]),
        ),
        node("b", json!([])),
    ];
    let p = pod(json!([{"name": "c", "image": "gcr.io/x:latest"}]));
    assert_eq!(scores(&p, &nodes).await, vec![28, 0]);
}

#[tokio::test]
async fn image_present_everywhere_keeps_its_full_size() {
    // 600MB on both nodes: spread 1, 100 * (600 - 23) / 977 = 59.
    let img = json!([{"names": ["gcr.io/x:latest"], "sizeBytes": 600 * MB}]);
    let nodes = vec![node("a", img.clone()), node("b", img)];
    let p = pod(json!([{"name": "c", "image": "gcr.io/x:latest"}]));
    assert_eq!(scores(&p, &nodes).await, vec![59, 59]);
}

#[tokio::test]
async fn small_images_score_zero() {
    let img = json!([{"names": ["gcr.io/x:latest"], "sizeBytes": 10 * MB}]);
    let nodes = vec![node("a", img.clone()), node("b", img)];
    let p = pod(json!([{"name": "c", "image": "gcr.io/x:latest"}]));
    assert_eq!(scores(&p, &nodes).await, vec![0, 0]);
}

#[test]
fn sign_pod_is_the_sorted_deduplicated_normalized_image_names() {
    let a = pod(json!([{"name": "c1", "image": "busybox"}, {"name": "c2", "image": "alpine:3"}]));
    let b =
        pod(json!([{"name": "x", "image": "alpine:3"}, {"name": "y", "image": "busybox:latest"}]));
    let (fa, sa) = ImageLocality.sign_pod(&a);
    let (fb, sb) = ImageLocality.sign_pod(&b);
    assert!(sa.is_success() && sb.is_success());
    assert_eq!(fa.len(), 1);
    assert_eq!(fa[0].key, fb[0].key);
    assert_eq!(fa[0].value, fb[0].value);
}
