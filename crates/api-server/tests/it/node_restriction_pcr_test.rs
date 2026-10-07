//! NodeRestriction `admitPodCertificateRequest` (#2582): the table cases of
//! `plugin/pkg/admission/noderestriction/admission_test.go:1847-1976`
//! (error strings verbatim), plus the operation/subresource/mirror/node-binding
//! branches of `admission.go:825-875` the upstream table does not exercise.
//!
//! The feature-gate-disabled case (`admission.go:821-823`) has no counterpart:
//! the resource is only served when the gate is on.

use rusternetes_api_server::handlers::node_restriction::{
    admit_pod_certificate_request, MIRROR_POD_ANNOTATION_KEY,
};
use rusternetes_common::auth::UserInfo;
use rusternetes_common::resources::podcertificaterequest::PodCertificateRequest;
use rusternetes_common::resources::{Node, Pod, ServiceAccount};
use rusternetes_common::types::ObjectMeta;
use rusternetes_common::Error;
use rusternetes_middleware::AuthContext;
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use serde_json::json;
use std::collections::HashMap;

const NODE: &str = "pcr-node-1";
const NODE_UID: &str = "pcr-node-1-uid";
const POD_UID: &str = "pod-uid";
const SA_UID: &str = "pcr-sa-uid";

fn user(username: &str, groups: &[&str]) -> AuthContext {
    AuthContext {
        user: UserInfo {
            username: username.to_string(),
            uid: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            extra: HashMap::new(),
        },
    }
}

fn node_ctx(name: &str) -> AuthContext {
    user(&format!("system:node:{name}"), &["system:nodes"])
}

async fn seeded() -> MemoryStorage {
    let storage = MemoryStorage::new();
    let node = Node {
        metadata: ObjectMeta {
            uid: NODE_UID.to_string(),
            ..ObjectMeta::new(NODE)
        },
        ..Node::new(NODE)
    };
    storage
        .create(&build_key("nodes", None, NODE), &node)
        .await
        .unwrap();
    let pod: Pod = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "pcrpod", "namespace": "ns", "uid": POD_UID},
        "spec": {"containers": [{"name": "c", "image": "i"}],
                 "nodeName": NODE, "serviceAccountName": "pcr-sa"},
    }))
    .unwrap();
    storage
        .create(&build_key("pods", Some("ns"), "pcrpod"), &pod)
        .await
        .unwrap();
    let sa: ServiceAccount = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "ServiceAccount",
        "metadata": {"name": "pcr-sa", "namespace": "ns", "uid": SA_UID},
    }))
    .unwrap();
    storage
        .create(&build_key("serviceaccounts", Some("ns"), "pcr-sa"), &sa)
        .await
        .unwrap();
    storage
}

fn pcr(
    pod: &str,
    pod_uid: &str,
    sa: &str,
    sa_uid: &str,
    node: &str,
    node_uid: &str,
) -> PodCertificateRequest {
    serde_json::from_value(json!({
        "apiVersion": "certificates.k8s.io/v1beta1", "kind": "PodCertificateRequest",
        "metadata": {"name": "foo", "namespace": "ns"},
        "spec": {"signerName": "example.com/foo", "podName": pod, "podUID": pod_uid,
                 "serviceAccountName": sa, "serviceAccountUID": sa_uid,
                 "nodeName": node, "nodeUID": node_uid},
    }))
    .unwrap()
}

fn good() -> PodCertificateRequest {
    pcr("pcrpod", POD_UID, "pcr-sa", SA_UID, NODE, NODE_UID)
}

async fn run(ctx: &AuthContext, r: &PodCertificateRequest) -> Result<(), Error> {
    admit_pod_certificate_request(&seeded().await, ctx, true, None, "ns", r).await
}

fn forbidden(e: Error) -> String {
    match e {
        Error::Forbidden(m) => m,
        other => panic!("expected Forbidden, got {other:?}"),
    }
}

/// Upstream's plain (non-Forbidden) error: a retry may succeed.
fn retryable(e: Error) -> String {
    match e {
        Error::Internal(m) => m,
        other => panic!("expected a retryable (non-Forbidden) error, got {other:?}"),
    }
}

#[tokio::test]
async fn allow_node1_create_pcr_that_references_pod_on_node1() {
    run(&node_ctx(NODE), &good()).await.unwrap();
}

#[tokio::test]
async fn a_non_node_user_is_not_restricted() {
    let admin = user("kubernetes-admin", &["system:masters"]);
    run(&admin, &pcr("x", "x", "x", "x", "other", "x"))
        .await
        .unwrap();
}

#[tokio::test]
async fn deny_node2_create_pcr_that_references_pod_on_node1() {
    let e = run(&node_ctx("pcr-node-2"), &good()).await.unwrap_err();
    assert_eq!(
        forbidden(e),
        r#"PodCertificateRequest.Spec.NodeName="pcr-node-1", which is not the requesting node "pcr-node-2""#
    );
}

#[tokio::test]
async fn deny_node1_create_pcr_that_references_nonexistent_pod() {
    let r = pcr("nonexistent-pod", "u", "pcr-sa", SA_UID, NODE, NODE_UID);
    let m = retryable(run(&node_ctx(NODE), &r).await.unwrap_err());
    assert!(m.contains(r#"pod "nonexistent-pod" not found"#), "{m}");
}

#[tokio::test]
async fn deny_node1_create_pcr_that_references_nonexistent_sa() {
    let r = pcr("pcrpod", POD_UID, "nonexistent-sa", "u", NODE, NODE_UID);
    assert_eq!(
        forbidden(run(&node_ctx(NODE), &r).await.unwrap_err()),
        r#"PodCertificateRequest for pod "ns/pcrpod" contains serviceAccountName ("nonexistent-sa") that differs from running pod ("pcr-sa")"#
    );
}

#[tokio::test]
async fn deny_node1_create_pcr_that_references_nonexistent_node() {
    let r = pcr("pcrpod", POD_UID, "pcr-sa", SA_UID, "nonexistent-node", "u");
    assert_eq!(
        forbidden(run(&node_ctx(NODE), &r).await.unwrap_err()),
        r#"PodCertificateRequest.Spec.NodeName="nonexistent-node", which is not the requesting node "pcr-node-1""#
    );
}

#[tokio::test]
async fn deny_node1_create_pcr_with_mismatched_pod_uid() {
    let r = pcr("pcrpod", "wrong-uid", "pcr-sa", SA_UID, NODE, NODE_UID);
    assert_eq!(
        retryable(run(&node_ctx(NODE), &r).await.unwrap_err()),
        r#"PodCertificateRequest for pod "ns/pcrpod" contains pod UID ("wrong-uid") which differs from running pod "pod-uid""#
    );
}

#[tokio::test]
async fn deny_node1_create_pcr_with_mismatched_sa_uid() {
    let r = pcr("pcrpod", POD_UID, "pcr-sa", "wrong-uid", NODE, NODE_UID);
    assert_eq!(
        retryable(run(&node_ctx(NODE), &r).await.unwrap_err()),
        r#"PodCertificateRequest for pod "ns/pcrpod" names service account UID "wrong-uid", which differs from the running service account ("pcr-sa-uid")"#
    );
}

#[tokio::test]
async fn deny_node1_create_pcr_with_mismatched_node_uid() {
    let r = pcr("pcrpod", POD_UID, "pcr-sa", SA_UID, NODE, "wrong-uid");
    assert_eq!(
        retryable(run(&node_ctx(NODE), &r).await.unwrap_err()),
        r#"PodCertificateRequest for pod "ns/pcrpod" names node UID "wrong-uid", inconsistent with the running node ("pcr-node-1-uid")"#
    );
}

#[tokio::test]
async fn deny_any_operation_but_create_and_any_subresource() {
    let s = seeded().await;
    let e = admit_pod_certificate_request(&s, &node_ctx(NODE), false, None, "ns", &good())
        .await
        .unwrap_err();
    assert_eq!(forbidden(e), "unexpected operation");
    let e = admit_pod_certificate_request(&s, &node_ctx(NODE), true, Some("status"), "ns", &good())
        .await
        .unwrap_err();
    assert_eq!(forbidden(e), "unexpected subresource status");
}

#[tokio::test]
async fn deny_pod_on_another_node_and_mirror_pod() {
    let s = seeded().await;
    let key = build_key("pods", Some("ns"), "pcrpod");
    let mut pod: Pod = s.get(&key).await.unwrap();
    pod.spec.as_mut().unwrap().node_name = Some("pcr-node-2".into());
    s.update(&key, &pod).await.unwrap();
    let e = admit_pod_certificate_request(&s, &node_ctx(NODE), true, None, "ns", &good())
        .await
        .unwrap_err();
    assert_eq!(
        forbidden(e),
        r#"pod "ns/pcrpod" is not running on node "pcr-node-1" named in the PodCertificateRequest"#
    );

    let mut pod: Pod = s.get(&key).await.unwrap();
    pod.spec.as_mut().unwrap().node_name = Some(NODE.into());
    pod.metadata.annotations = Some(HashMap::from([(
        MIRROR_POD_ANNOTATION_KEY.to_string(),
        "h".to_string(),
    )]));
    s.update(&key, &pod).await.unwrap();
    let e = admit_pod_certificate_request(&s, &node_ctx(NODE), true, None, "ns", &good())
        .await
        .unwrap_err();
    assert_eq!(forbidden(e), r#"pod "ns/pcrpod" is a mirror pod"#);
}

/// The plugin is only a gate if the admission chain calls it.
#[test]
fn the_admission_chain_applies_the_restriction() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/endpoints/handlers/admission.rs"),
    )
    .unwrap();
    assert!(src.contains("node_restriction::admit_pod_certificate_request"));
}
