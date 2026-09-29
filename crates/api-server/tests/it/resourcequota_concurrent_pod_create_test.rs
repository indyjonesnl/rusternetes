//! Concurrent pod creates must not overrun a ResourceQuota.
//!
//! The conformance spec is `[sig-apps] ReplicationController should surface a
//! failure condition on a common issue like exceeded quota`
//! (`test/e2e/apps/rc.go:540-600`): a quota allows two pods, an RC asks for
//! three, and the RC must get a `ReplicaFailure` condition. The RC's slow-start
//! batches (`slowStartBatch`, `pkg/controller/replicaset/replica_set.go`)
//! issue a batch's creates concurrently, so the second batch sends two creates
//! at once against one remaining pod of quota.
//!
//! Upstream admits them one at a time: `quotaEvaluator.addWork` queues every
//! request under its namespace, and `getWork` marks the namespace `inProgress`
//! so a single worker evaluates it
//! (`staging/src/k8s.io/apiserver/pkg/admission/plugin/resourcequota/controller.go:688-735`),
//! each admitted request's usage landing in `status.used` before the next is
//! checked. Evaluated in parallel, both creates saw one pod used and both were
//! admitted — three pods, no failed create, no condition.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

const PODS: &str = "/api/v1/namespaces/default/pods";

fn pod(name: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "namespace": "default" },
        "spec": { "containers": [{ "name": "c", "image": "busybox" }] },
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pod_creates_are_admitted_one_at_a_time_against_quota() {
    let api = TestApiServer::new();
    let (status, body) = api
        .post(
            "/api/v1/namespaces/default/resourcequotas",
            &json!({
                "apiVersion": "v1",
                "kind": "ResourceQuota",
                "metadata": { "name": "condition-test", "namespace": "default" },
                "spec": { "hard": { "pods": "2" } },
            }),
        )
        .await;
    assert!(status.is_success(), "quota create: {status} {body}");

    // Each create on its own task, so the admissions genuinely overlap.
    let api = std::sync::Arc::new(api);
    let handles: Vec<_> = (0..8)
        .map(|i| {
            let api = api.clone();
            tokio::spawn(async move { api.post(PODS, &pod(&format!("p{i}"))).await })
        })
        .collect();
    let mut results = Vec::new();
    for h in handles {
        results.push(h.await.unwrap());
    }

    let admitted = results.iter().filter(|(s, _)| s.is_success()).count();
    let forbidden = results.iter().filter(|(s, _)| s.as_u16() == 403).count();
    assert_eq!(
        admitted, 2,
        "a quota of pods=2 admitted {admitted} of 8 concurrent creates: {results:?}"
    );
    assert_eq!(forbidden, 6, "the rest must be Forbidden: {results:?}");
}
