//! The Node authorizer's graph is fed from the store
//! (`graph_populator.go` `AddGraphEventHandlers`): a secret is readable by a
//! kubelet only while a pod bound to its node references it.

use rusternetes_api_server::authorizer::build_authorizer;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, RequestAttributes};
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use serde_json::{json, Value};
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

fn secret_get(node_name: &str, secret: &str) -> RequestAttributes {
    RequestAttributes::new(node(node_name), "get", "secrets")
        .with_api_group("")
        .with_namespace("ns1")
        .with_name(secret)
}

fn pod(node_name: &str, secret: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p1", "namespace": "ns1", "uid": "u1"},
        "spec": {"nodeName": node_name, "containers": [{"name": "c", "image": "i"}],
                 "volumes": [{"name": "v", "secret": {"secretName": secret}}]},
    })
}

async fn eventually(az: &Arc<dyn Authorizer>, attrs: &RequestAttributes, want: bool) {
    for _ in 0..100 {
        let got = az.authorize(attrs).await.unwrap() == Decision::Allow;
        if got == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("authorization of {attrs:?} never became {want}");
}

#[tokio::test]
async fn node_reads_only_secrets_of_its_pods() {
    let storage = Arc::new(MemoryStorage::new());
    // A pod that exists before the authorizer starts is picked up by the
    // initial list.
    let key = build_key("pods", Some("ns1"), "p1");
    storage.create(&key, &pod("node1", "s1")).await.unwrap();

    let az = build_authorizer(storage.clone(), false, &["Node".to_string()], "").unwrap();
    eventually(&az, &secret_get("node1", "s1"), true).await;
    assert_eq!(
        az.authorize(&secret_get("node2", "s1")).await.unwrap(),
        Decision::Deny("no relationship found between node 'node2' and this object".to_string())
    );
    assert!(matches!(
        az.authorize(&secret_get("node1", "other")).await.unwrap(),
        Decision::Deny(_)
    ));

    // A later update that rebinds the pod is applied from the watch.
    storage.update(&key, &pod("node2", "s1")).await.unwrap();
    eventually(&az, &secret_get("node2", "s1"), true).await;
    eventually(&az, &secret_get("node1", "s1"), false).await;

    // Deleting the pod removes the relationship.
    storage.delete(&key).await.unwrap();
    eventually(&az, &secret_get("node2", "s1"), false).await;
}
