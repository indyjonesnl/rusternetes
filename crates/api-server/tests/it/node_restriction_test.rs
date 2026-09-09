//! NodeRestriction admission for pods (#1906, closing the gap #1721 recorded).
//!
//! A kubelet must be able to create and delete the mirror pod of a static pod.
//! Upstream allows it in two halves: the node's RBAC rules grant `create` and
//! `delete` on `pods` outright
//! (`bootstrappolicy/policy.go:215-217`), and the NodeRestriction admission
//! plugin narrows that to the node's own mirror pods
//! (`plugin/pkg/admission/noderestriction/admission.go:252-340`).
//!
//! Rusternetes had neither half, so a real kubelet against our api-server
//! looped on `Failed deleting a mirror pod: Node "…" is not authorized to
//! delete pods in namespace kube-system`. These tests cover the admission half;
//! the authorization half is in `crates/common/tests/it/authz_rbac_node.rs`.

use rusternetes_api_server::handlers::node_restriction::{
    admit_pod_create, admit_pod_delete, MIRROR_POD_ANNOTATION_KEY,
};
use rusternetes_common::auth::UserInfo;
use rusternetes_common::resources::{Node, Pod};
use rusternetes_common::types::ObjectMeta;
use rusternetes_common::Error;
use rusternetes_middleware::AuthContext;
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use serde_json::json;
use std::collections::HashMap;

const NODE: &str = "node-1";
const NODE_UID: &str = "11111111-2222-3333-4444-555555555555";

fn node_ctx(name: &str) -> AuthContext {
    AuthContext {
        user: UserInfo {
            username: format!("system:node:{name}"),
            uid: String::new(),
            groups: vec!["system:nodes".to_string()],
            extra: HashMap::new(),
        },
    }
}

fn human_ctx() -> AuthContext {
    AuthContext {
        user: UserInfo {
            username: "kubernetes-admin".to_string(),
            uid: String::new(),
            groups: vec!["system:masters".to_string()],
            extra: HashMap::new(),
        },
    }
}

async fn storage_with_node() -> MemoryStorage {
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
        .expect("seed node");
    storage
}

/// A well-formed mirror pod: the shape a kubelet actually sends. Built from
/// JSON so the fixture is the wire body, not a struct literal that drifts as
/// PodSpec grows fields.
fn mirror_pod() -> Pod {
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "static-web-node-1",
            "namespace": "kube-system",
            "annotations": { MIRROR_POD_ANNOTATION_KEY: "abc123" },
            "ownerReferences": [{
                "apiVersion": "v1",
                "kind": "Node",
                "name": NODE,
                "uid": NODE_UID,
                "controller": true,
            }],
        },
        "spec": {
            "containers": [{ "name": "web", "image": "nginx" }],
            "nodeName": NODE,
        },
    }))
    .expect("mirror pod fixture decodes")
}

fn forbidden_message(e: Error) -> String {
    match e {
        Error::Forbidden(msg) => msg,
        other => panic!("expected Forbidden, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// create
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_node_may_create_its_own_mirror_pod() {
    let storage = storage_with_node().await;
    admit_pod_create(&storage, &node_ctx(NODE), &mirror_pod())
        .await
        .expect("a well-formed mirror pod bound to the node is admitted");
}

#[tokio::test]
async fn a_node_may_not_create_a_pod_without_the_mirror_annotation() {
    let storage = storage_with_node().await;
    let mut pod = mirror_pod();
    pod.metadata.annotations = None;

    let msg = forbidden_message(
        admit_pod_create(&storage, &node_ctx(NODE), &pod)
            .await
            .expect_err("a non-mirror pod must be refused"),
    );
    assert!(
        msg.contains("can only create mirror pods") && msg.contains(MIRROR_POD_ANNOTATION_KEY),
        "unexpected message: {msg}"
    );
}

#[tokio::test]
async fn a_node_may_not_create_a_pod_bound_to_another_node() {
    let storage = storage_with_node().await;
    let mut pod = mirror_pod();
    pod.spec.as_mut().unwrap().node_name = Some("node-2".to_string());

    let msg = forbidden_message(
        admit_pod_create(&storage, &node_ctx(NODE), &pod)
            .await
            .expect_err("a pod bound elsewhere must be refused"),
    );
    assert!(
        msg.contains("can only create pods with spec.nodeName set to itself"),
        "unexpected message: {msg}"
    );
}

#[tokio::test]
async fn a_mirror_pod_needs_a_controller_owner_reference_to_the_node() {
    let storage = storage_with_node().await;

    // No owner reference at all.
    let mut pod = mirror_pod();
    pod.metadata.owner_references = None;
    let msg = forbidden_message(
        admit_pod_create(&storage, &node_ctx(NODE), &pod)
            .await
            .expect_err("an ownerless mirror pod must be refused"),
    );
    assert!(
        msg.contains("owner reference set to itself"),
        "unexpected message: {msg}"
    );

    // Present, but not marked as the controller.
    let mut pod = mirror_pod();
    pod.metadata.owner_references.as_mut().unwrap()[0].controller = None;
    let msg = forbidden_message(
        admit_pod_create(&storage, &node_ctx(NODE), &pod)
            .await
            .expect_err("a non-controller owner reference must be refused"),
    );
    assert!(
        msg.contains("controller owner reference"),
        "unexpected message: {msg}"
    );

    // Present and controller, but with a uid that is not the node's.
    let mut pod = mirror_pod();
    pod.metadata.owner_references.as_mut().unwrap()[0].uid = "not-the-node".to_string();
    let msg = forbidden_message(
        admit_pod_create(&storage, &node_ctx(NODE), &pod)
            .await
            .expect_err("a uid mismatch must be refused"),
    );
    assert!(msg.contains("UID mismatch"), "unexpected message: {msg}");

    // blockOwnerDeletion set.
    let mut pod = mirror_pod();
    pod.metadata.owner_references.as_mut().unwrap()[0].block_owner_deletion = Some(true);
    let msg = forbidden_message(
        admit_pod_create(&storage, &node_ctx(NODE), &pod)
            .await
            .expect_err("blockOwnerDeletion must be refused"),
    );
    assert!(
        msg.contains("must not set blockOwnerDeletion"),
        "unexpected message: {msg}"
    );
}

/// `HasAPIObjectReference` (`pkg/api/pod/util.go:1696-1766`) — a mirror pod may
/// not pull anything out of the API, which is what keeps the grant safe.
#[tokio::test]
async fn a_mirror_pod_may_not_reference_api_objects() {
    let storage = storage_with_node().await;

    let mut with_sa = mirror_pod();
    with_sa.spec.as_mut().unwrap().service_account_name = Some("default".to_string());
    let msg = forbidden_message(
        admit_pod_create(&storage, &node_ctx(NODE), &with_sa)
            .await
            .expect_err("a serviceAccountName must be refused"),
    );
    assert!(msg.contains("serviceaccounts"), "unexpected message: {msg}");

    let mut with_secret_volume = mirror_pod();
    with_secret_volume.spec.as_mut().unwrap().volumes = Some(
        serde_json::from_value(json!([{
            "name": "creds",
            "secret": { "secretName": "creds" },
        }]))
        .expect("volume fixture decodes"),
    );
    let msg = forbidden_message(
        admit_pod_create(&storage, &node_ctx(NODE), &with_secret_volume)
            .await
            .expect_err("a secret volume must be refused"),
    );
    assert!(
        msg.contains("secrets (via secret volumes)"),
        "unexpected message: {msg}"
    );
}

// ---------------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------------

/// The repro from #1906: a kubelet deleting the mirror pod it owns.
#[tokio::test]
async fn a_node_may_delete_a_pod_bound_to_itself() {
    admit_pod_delete(&node_ctx(NODE), &mirror_pod()).expect("a node may delete its own mirror pod");
}

/// Upstream's delete rule is `spec.nodeName` only — mirror-ness is not part of
/// it (`admission.go:257-270`).
#[tokio::test]
async fn a_node_may_delete_a_non_mirror_pod_bound_to_itself() {
    let mut pod = mirror_pod();
    pod.metadata.annotations = None;
    admit_pod_delete(&node_ctx(NODE), &pod)
        .expect("the delete rule looks at spec.nodeName, not the mirror annotation");
}

#[tokio::test]
async fn a_node_may_not_delete_a_pod_bound_to_another_node() {
    let mut pod = mirror_pod();
    pod.spec.as_mut().unwrap().node_name = Some("node-2".to_string());

    let msg = forbidden_message(
        admit_pod_delete(&node_ctx(NODE), &pod)
            .expect_err("deleting another node's pod must be refused"),
    );
    assert!(
        msg.contains("can only delete pods with spec.nodeName set to itself"),
        "unexpected message: {msg}"
    );
}

#[tokio::test]
async fn an_unscheduled_pod_is_not_deletable_by_a_node() {
    let mut pod = mirror_pod();
    pod.spec.as_mut().unwrap().node_name = None;
    admit_pod_delete(&node_ctx(NODE), &pod)
        .expect_err("an unbound pod is bound to no node, so no node may delete it");
}

// ---------------------------------------------------------------------------
// identity
// ---------------------------------------------------------------------------

/// The plugin only applies to node identities, so an ordinary user's pod writes
/// must be untouched by it -- otherwise every create would need a mirror
/// annotation.
#[tokio::test]
async fn a_non_node_user_is_not_restricted() {
    let storage = storage_with_node().await;
    let mut pod = mirror_pod();
    pod.metadata.annotations = None;
    pod.metadata.owner_references = None;
    pod.spec.as_mut().unwrap().node_name = Some("node-2".to_string());

    admit_pod_create(&storage, &human_ctx(), &pod)
        .await
        .expect("a human's pod create is not this plugin's business");
    admit_pod_delete(&human_ctx(), &pod).expect("nor is a human's delete");
}

/// `DefaultNodeIdentity` requires the `system:nodes` group as well as the
/// username prefix (`pkg/auth/nodeidentifier/default.go:42-60`). A token minted
/// with only the name is not a node -- and must not be *restricted* as one
/// either, since the authorizer will not grant it the node rules.
#[tokio::test]
async fn the_username_prefix_alone_is_not_a_node_identity() {
    let storage = storage_with_node().await;
    let mut impostor = node_ctx(NODE);
    impostor.user.groups = vec!["system:authenticated".to_string()];

    let mut pod = mirror_pod();
    pod.metadata.annotations = None;
    admit_pod_create(&storage, &impostor, &pod)
        .await
        .expect("without the system:nodes group this is not a node identity");
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

/// The plugin is only worth anything if the handlers call it. Upstream cannot
/// have this gap -- admission is a chain every request passes through -- so the
/// equivalent here is asserting the two pod write paths that the node's RBAC
/// rules now grant (`create`, `delete`) both invoke it.
///
/// Keyed on the mechanism (the authorizer grant), not on handler names: if a
/// verb is added to `NODE_RULES` for pods, this test names the handler that has
/// to gain the check.
#[test]
fn the_pod_create_and_delete_handlers_apply_the_restriction() {
    let pod_rs = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/handlers/pod.rs"),
    )
    .expect("read pod.rs");
    let src = pod_rs
        .split("\n#[cfg(test)]")
        .next()
        .unwrap_or("")
        .to_string();

    for (fname, call) in [
        ("pub async fn create(", "node_restriction::admit_pod_create"),
        (
            "pub async fn delete_pod(",
            "node_restriction::admit_pod_delete",
        ),
    ] {
        let start = src
            .find(fname)
            .unwrap_or_else(|| panic!("{fname} not found in pod.rs -- the handler was renamed"));
        let body = &src[start..];
        let end = body
            .find("\npub async fn ")
            .map(|i| i + 1)
            .unwrap_or(body.len());
        assert!(
            body[..end].contains(call),
            "{fname} does not call {call}. A node's RBAC rules grant create and \
             delete on pods outright (bootstrappolicy/policy.go:215-217); \
             without the NodeRestriction check that grant lets any node write \
             any pod (#1906)."
        );
    }
}
