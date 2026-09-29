//! #1840: a ResourceQuota spec update must survive a `/status` write built from
//! an older snapshot.
//!
//! The conformance spec is `[sig-api-machinery] ResourceQuota should be able to
//! update and delete ResourceQuota` (`test/e2e/apimachinery/resource_quota.go:
//! 950-1000`). It creates a quota with `cpu=1, memory=500Mi`, then `Update`s the
//! *original local object* — which never received a resourceVersion, so the PUT
//! is unconditional — to `cpu=2, memory=1Gi`, and finally `Get`s it back:
//!
//! ```go
//! resourceQuota.Spec.Hard[v1.ResourceCPU] = resource.MustParse("2")
//! resourceQuota.Spec.Hard[v1.ResourceMemory] = resource.MustParse("1Gi")
//! resourceQuotaResult, err = client.CoreV1().ResourceQuotas(ns).Update(ctx, resourceQuota, metav1.UpdateOptions{})
//! ...
//! resourceQuotaResult, err = client.CoreV1().ResourceQuotas(ns).Get(ctx, quotaName, metav1.GetOptions{})
//! gomega.Expect(resourceQuotaResult.Spec.Hard).To(gomega.HaveKeyWithValue(v1.ResourceCPU, resource.MustParse("2")))
//! ```
//!
//! Meanwhile the resource-quota controller is writing `status.used` from its own
//! copy of the quota, which predates the update. Upstream makes that safe with
//! `resourcequotaStatusStrategy.PrepareForUpdate`
//! (`pkg/registry/core/resourcequota/strategy.go`), which copies the stored
//! spec over the incoming one, applied inside `GuaranteedUpdate`'s retry loop to
//! the object it has just read — so a status write can only ever carry the
//! *current* spec.
//!
//! These tests drive the real handlers. They only mean something because
//! `MemoryStorage` now rejects a superseded resourceVersion like every other
//! backend; before that, a lost update was unobservable in-process.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";
const QUOTAS: &str = "/api/v1/namespaces/default/resourcequotas";

fn quota(cpu: &str, memory: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": { "name": "test-quota", "namespace": NS },
        "spec": { "hard": { "cpu": cpu, "memory": memory } },
    })
}

fn item() -> String {
    format!("{QUOTAS}/test-quota")
}

/// What the quota controller sends: its own (older) copy of the object, with a
/// freshly computed status grafted on.
fn status_write_from(snapshot: &Value, used_cpu: &str) -> Value {
    let mut body = snapshot.clone();
    body["status"] = json!({
        "hard": snapshot["spec"]["hard"].clone(),
        "used": { "cpu": used_cpu, "memory": "0" },
    });
    body
}

async fn create(api: &TestApiServer) {
    let (status, body) = api.post(QUOTAS, &quota("1", "500Mi")).await;
    assert!(status.is_success(), "create: {status} {body}");
}

fn assert_hard(body: &Value, cpu: &str, memory: &str, context: &str) {
    assert_eq!(
        body["spec"]["hard"]["cpu"], cpu,
        "{context}: spec.hard.cpu reverted: {body}"
    );
    assert_eq!(
        body["spec"]["hard"]["memory"], memory,
        "{context}: spec.hard.memory reverted: {body}"
    );
}

/// The issue's own recipe, deterministically: the controller read the quota,
/// the client updated it, and only then does the controller's status write
/// arrive — carrying the pre-update spec and resourceVersion.
#[tokio::test]
async fn a_status_write_from_before_the_update_does_not_revert_the_spec() {
    let api = TestApiServer::new();
    create(&api).await;

    let (_, snapshot) = api.get(&item()).await;

    // The e2e's unconditional Update: no resourceVersion in the body.
    let (status, updated) = api.put(&item(), &quota("2", "1Gi")).await;
    assert!(status.is_success(), "update: {status} {updated}");
    assert_hard(&updated, "2", "1Gi", "the update's own response");

    let (status, body) = api
        .put(
            &format!("{}/status", item()),
            &status_write_from(&snapshot, "0"),
        )
        .await;
    assert!(status.is_success(), "status write: {status} {body}");
    assert_hard(&body, "2", "1Gi", "the status write's response");

    let (_, got) = api.get(&item()).await;
    assert_hard(&got, "2", "1Gi", "the Get after the status write");
    assert_eq!(
        got["status"]["used"]["cpu"], "0",
        "the status write itself must still land: {got}"
    );
}

/// The same, as a merge-patch — the other verb the `/status` route serves.
#[tokio::test]
async fn a_status_merge_patch_from_before_the_update_does_not_revert_the_spec() {
    let api = TestApiServer::new();
    create(&api).await;

    let (_, snapshot) = api.get(&item()).await;
    let (status, _) = api.put(&item(), &quota("2", "1Gi")).await;
    assert!(status.is_success());

    let (status, body) = api
        .send(
            "PATCH",
            &format!("{}/status", item()),
            Some("application/merge-patch+json"),
            Some(&status_write_from(&snapshot, "0")),
        )
        .await;
    assert!(status.is_success(), "status patch: {status} {body}");

    let (_, got) = api.get(&item()).await;
    assert_hard(&got, "2", "1Gi", "the Get after the status patch");
}

/// Overlapping, as in the conformance timeline: the controller's status writes
/// land while the client is updating. Every round re-runs the e2e's
/// update-then-verify against a writer hammering `/status` from the
/// pre-update snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_status_writes_never_revert_a_spec_update() {
    const ROUNDS: usize = 25;
    for round in 0..ROUNDS {
        let api = TestApiServer::new();
        create(&api).await;
        let (_, snapshot) = api.get(&item()).await;

        let writer = {
            let api = api.clone();
            let snapshot = snapshot.clone();
            tokio::spawn(async move {
                for n in 0..8 {
                    let (status, body) = api
                        .put(
                            &format!("{}/status", item()),
                            &status_write_from(&snapshot, &n.to_string()),
                        )
                        .await;
                    assert!(
                        status.is_success(),
                        "status write {n} must succeed (the handler retries its own CAS): {status} {body}"
                    );
                }
            })
        };

        let (status, updated) = api.put(&item(), &quota("2", "1Gi")).await;
        assert!(
            status.is_success(),
            "round {round}: update: {status} {updated}"
        );
        assert_hard(
            &updated,
            "2",
            "1Gi",
            &format!("round {round}: update response"),
        );

        writer.await.expect("status writer panicked");

        let (_, got) = api.get(&item()).await;
        assert_hard(&got, "2", "1Gi", &format!("round {round}: final Get"));
    }
}
