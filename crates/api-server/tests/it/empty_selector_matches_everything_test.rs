//! An empty (`{}`) label selector matches everything.
//!
//! `LabelSelectorAsSelector`
//! (`staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/helpers.go:36-42`) gives
//! three answers, not two:
//!
//! ```go
//! if ps == nil {
//!     return labels.Nothing(), nil
//! }
//! if len(ps.MatchLabels)+len(ps.MatchExpressions) == 0 {
//!     return labels.Everything(), nil
//! }
//! ```
//!
//! `policy/v1` states it in the API itself — "A null selector will match no
//! pods, while an empty ({}) selector will select all pods within the
//! namespace" (`staging/src/k8s.io/api/policy/v1/types.go:36-42`).
//!
//! Rusternetes collapsed the middle case into the first, so a
//! `PodDisruptionBudget` with `selector: {}` protected nothing and eviction let
//! through a disruption upstream refuses (#2012). Eviction reads the selector
//! through `LabelSelectorAsSelector` with no `selector.Empty()` guard
//! (`pkg/registry/core/pod/storage/eviction.go:498-505`).

use rusternetes_common::resources::{
    Container, IntOrString, Pod, PodDisruptionBudget, PodDisruptionBudgetSpec, PodSpec, PodStatus,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

fn running_pod(name: &str, namespace: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            labels: Some(
                [("app".to_string(), "web".to_string())]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "nginx".to_string(),
                image: "nginx:latest".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        }),
    }
}

fn pdb(name: &str, namespace: &str, selector: Option<LabelSelector>) -> PodDisruptionBudget {
    PodDisruptionBudget {
        type_meta: TypeMeta {
            api_version: "policy/v1".to_string(),
            kind: "PodDisruptionBudget".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        spec: PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(1)),
            max_unavailable: None,
            selector,
            unhealthy_pod_eviction_policy: None,
        },
        status: None,
    }
}

async fn seed(api: &TestApiServer, pod: &Pod, budget: &PodDisruptionBudget) {
    let ns = pod.metadata.namespace.as_deref().unwrap();
    api.storage
        .create(&build_key("pods", Some(ns), &pod.metadata.name), pod)
        .await
        .expect("seed pod");
    api.storage
        .create(
            &build_key("poddisruptionbudgets", Some(ns), &budget.metadata.name),
            budget,
        )
        .await
        .expect("seed pdb");
}

async fn evict(api: &TestApiServer, pod: &Pod) -> axum::http::StatusCode {
    let ns = pod.metadata.namespace.as_deref().unwrap();
    api.send(
        "POST",
        &format!(
            "/api/v1/namespaces/{ns}/pods/{}/eviction",
            pod.metadata.name
        ),
        Some("application/json"),
        Some(&json!({
            "apiVersion": "policy/v1",
            "kind": "Eviction",
            "metadata": { "name": pod.metadata.name, "namespace": ns },
        })),
    )
    .await
    .0
}

/// `minAvailable: 1` with one healthy pod and a `{}` selector: the PDB covers
/// that pod, so evicting it would leave zero and the request is refused.
#[tokio::test]
async fn an_empty_pdb_selector_protects_every_pod_in_the_namespace() {
    let api = TestApiServer::new();
    let pod = running_pod("web-0", "default");
    seed(
        &api,
        &pod,
        &pdb(
            "everything",
            "default",
            Some(LabelSelector {
                match_labels: None,
                match_expressions: None,
            }),
        ),
    )
    .await;

    assert_eq!(
        evict(&api, &pod).await.as_u16(),
        429,
        "an empty selector is labels.Everything(), so the PDB covers this pod"
    );
}

/// And the same with the maps present but empty — still `{}`.
#[tokio::test]
async fn an_explicitly_empty_pdb_selector_protects_every_pod() {
    let api = TestApiServer::new();
    let pod = running_pod("web-0", "default");
    seed(
        &api,
        &pod,
        &pdb(
            "everything",
            "default",
            Some(LabelSelector {
                match_labels: Some(Default::default()),
                match_expressions: Some(Vec::new()),
            }),
        ),
    )
    .await;

    assert_eq!(evict(&api, &pod).await.as_u16(), 429);
}

/// An **absent** selector is the other case: `labels.Nothing()`, so the PDB
/// covers no pod and the eviction goes through.
#[tokio::test]
async fn an_absent_pdb_selector_protects_nothing() {
    let api = TestApiServer::new();
    let pod = running_pod("web-0", "default");
    seed(&api, &pod, &pdb("nothing", "default", None)).await;

    let status = evict(&api, &pod).await.as_u16();
    assert!(
        status == 200 || status == 201,
        "a null selector matches no pods, so nothing blocks the eviction: {status}"
    );
}
