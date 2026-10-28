//! The storage-fed graph populator behind the Node authorizer
//! (`plugin/pkg/auth/authorizer/node/graph_populator.go`): pods, PVs and
//! volume attachments reach the graph through list+watch, deletes remove
//! their edges, and a decision follows.

use rusternetes_api_server::node_graph_populator;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::node_graph::NodeGraph;
use rusternetes_common::authz::Authorizer;
use rusternetes_common::authz::{
    NodeAuthorizer, NodeAuthorizerFeatures, Opinion, RequestAttributes,
};
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

fn node(name: &str) -> UserInfo {
    UserInfo {
        username: format!("system:node:{name}"),
        uid: String::new(),
        groups: vec!["system:nodes".into()],
        extra: Default::default(),
    }
}

async fn allowed(
    az: &NodeAuthorizer,
    node_name: &str,
    resource: &str,
    ns: &str,
    name: &str,
) -> bool {
    let mut attrs = RequestAttributes::new(node(node_name), "get", resource)
        .with_api_group("")
        .with_name(name);
    if !ns.is_empty() {
        attrs = attrs.with_namespace(ns);
    }
    matches!(
        az.authorize_opinion(&attrs).await.unwrap(),
        Opinion::Allow(_)
    )
}

async fn eventually(
    az: &NodeAuthorizer,
    node_name: &str,
    resource: &str,
    ns: &str,
    name: &str,
    want: bool,
) {
    for _ in 0..100 {
        if allowed(az, node_name, resource, ns, name).await == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{resource} {ns}/{name} for {node_name}: never became allowed={want}");
}

#[tokio::test]
async fn populator_tracks_pods_pvs_and_attachments() {
    let storage = Arc::new(MemoryStorage::new());

    // Objects that exist before the populator starts are delivered by the list.
    let pod = json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p1", "namespace": "ns", "uid": "u1"},
        "spec": {
            "nodeName": "worker-1",
            "containers": [],
            "volumes": [
                {"name": "s", "secret": {"secretName": "pod-secret"}},
                {"name": "c", "persistentVolumeClaim": {"claimName": "claim"}},
            ],
        },
    });
    storage
        .create(&build_key("pods", Some("ns"), "p1"), &pod)
        .await
        .unwrap();

    let graph = Arc::new(NodeGraph::new());
    node_graph_populator::spawn(storage.clone(), graph.clone());
    let az = NodeAuthorizer::new(graph).with_features(NodeAuthorizerFeatures::current());

    eventually(&az, "worker-1", "secrets", "ns", "pod-secret", true).await;
    assert!(!allowed(&az, "worker-2", "secrets", "ns", "pod-secret").await);
    eventually(
        &az,
        "worker-1",
        "persistentvolumeclaims",
        "ns",
        "claim",
        true,
    )
    .await;

    // A PV bound to the claim, with a kubelet-visible secret, extends the chain
    // pod <- pvc <- pv <- secret.
    let pv = json!({
        "apiVersion": "v1", "kind": "PersistentVolume",
        "metadata": {"name": "pv1"},
        "spec": {
            "claimRef": {"name": "claim", "namespace": "ns"},
            "csi": {"driver": "d", "volumeHandle": "h",
                    "nodePublishSecretRef": {"name": "pv-secret", "namespace": "ns"}},
        },
    });
    storage
        .create(&build_key("persistentvolumes", None, "pv1"), &pv)
        .await
        .unwrap();
    eventually(&az, "worker-1", "persistentvolumes", "", "pv1", true).await;
    eventually(&az, "worker-1", "secrets", "ns", "pv-secret", true).await;
    assert!(!allowed(&az, "worker-2", "persistentvolumes", "", "pv1").await);

    // Volume attachments hang directly off their node.
    let va = json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
        "metadata": {"name": "va1"},
        "spec": {"attacher": "a", "nodeName": "worker-1", "source": {"persistentVolumeName": "pv1"}},
    });
    storage
        .create(&build_key("volumeattachments", None, "va1"), &va)
        .await
        .unwrap();
    let mut attrs = RequestAttributes::new(node("worker-1"), "get", "volumeattachments")
        .with_api_group("storage.k8s.io")
        .with_name("va1");
    let mut ok = false;
    for _ in 0..100 {
        ok = matches!(
            az.authorize_opinion(&attrs).await.unwrap(),
            Opinion::Allow(_)
        );
        if ok {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ok, "volumeattachment never reached the graph");
    attrs.user = node("worker-2");
    assert!(!matches!(
        az.authorize_opinion(&attrs).await.unwrap(),
        Opinion::Allow(_)
    ));

    // Deleting the pod removes everything it granted.
    storage
        .delete(&build_key("pods", Some("ns"), "p1"))
        .await
        .unwrap();
    eventually(&az, "worker-1", "secrets", "ns", "pod-secret", false).await;
}
