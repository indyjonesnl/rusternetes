//! NamespaceLifecycle admission, ported case-by-case from upstream
//! `staging/src/k8s.io/apiserver/pkg/admission/plugin/namespace/lifecycle/admission_test.go`
//! (release-1.35): `TestAdmissionNamespaceDoesNotExist` (:124),
//! `TestAdmissionNamespaceActive` (:166), `TestAdmissionNamespaceTerminating`
//! (:186), `TestAdmissionNamespaceForceLiveLookup` (:239) and
//! `TestAccessReviewCheckOnMissingNamespace` (:101).
//!
//! These drive the real router with `strict_namespaces()`, which turns off the
//! harness's namespace auto-seeding so a missing namespace stays missing.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn strict() -> TestApiServer {
    TestApiServer::builder().strict_namespaces().build()
}

fn pod() -> Value {
    json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},
           "spec":{"containers":[{"name":"c","image":"i"}]}})
}

async fn make_ns(s: &TestApiServer, name: &str) {
    let (st, _) = s
        .post(
            "/api/v1/namespaces",
            &json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":name}}),
        )
        .await;
    assert_eq!(st.as_u16(), 201);
}

/// admission_test.go:124 TestAdmissionNamespaceDoesNotExist: create AND update
/// are rejected (NotFound, `namespaces "test" not found`); delete proceeds.
#[tokio::test]
async fn missing_namespace_rejects_create_and_update_but_not_delete() {
    let s = strict();
    let (st, body) = s.post("/api/v1/namespaces/test/pods", &pod()).await;
    assert_eq!(st.as_u16(), 404, "{body}");
    assert_eq!(body["reason"], "NotFound");
    assert_eq!(body["message"], "namespaces \"test\" not found");
    assert_eq!(body["details"]["name"], "test");
    assert_eq!(body["details"]["kind"], "namespaces");

    let (st, body) = s.put("/api/v1/namespaces/test/pods/p", &pod()).await;
    assert_eq!(st.as_u16(), 404, "{body}");
    assert_eq!(body["message"], "namespaces \"test\" not found");

    // Delete is never refused by the lifecycle plugin: it reaches the handler,
    // whose own answer is about the pod, not the namespace.
    let (st, body) = s.delete("/api/v1/namespaces/test/pods/p").await;
    assert_eq!(st.as_u16(), 404);
    assert_ne!(body["message"], "namespaces \"test\" not found");
}

/// admission_test.go:166 TestAdmissionNamespaceActive.
#[tokio::test]
async fn active_namespace_admits_create() {
    let s = strict();
    make_ns(&s, "test").await;
    let (st, body) = s.post("/api/v1/namespaces/test/pods", &pod()).await;
    assert_eq!(st.as_u16(), 201, "{body}");
}

/// admission_test.go:186 TestAdmissionNamespaceTerminating: create Forbidden
/// with the NamespaceTerminating cause; update and delete proceed; delete of
/// `default` never proceeds, delete of another namespace does.
#[tokio::test]
async fn terminating_namespace_and_immortal_deletes() {
    let s = strict();
    make_ns(&s, "test").await;
    let (st, _) = s.post("/api/v1/namespaces/test/pods", &pod()).await;
    assert_eq!(st.as_u16(), 201);
    let (st, _) = s.delete("/api/v1/namespaces/test").await;
    assert!(st.is_success(), "{st}");

    let mut p2 = pod();
    p2["metadata"]["name"] = json!("q");
    let (st, body) = s.post("/api/v1/namespaces/test/pods", &p2).await;
    assert_eq!(st.as_u16(), 403, "{body}");
    assert_eq!(
        body["details"]["causes"][0]["reason"],
        "NamespaceTerminating"
    );
    assert_eq!(
        body["details"]["causes"][0]["message"],
        "namespace test is being terminated"
    );

    // Update proceeds (past lifecycle admission): not the namespace's 404/403.
    let (st, body) = s.put("/api/v1/namespaces/test/pods/p", &pod()).await;
    assert_ne!(st.as_u16(), 403, "{body}");
    assert_ne!(body["message"], "namespaces \"test\" not found");

    for imm in ["default", "kube-system", "kube-public"] {
        let (st, body) = s.delete(&format!("/api/v1/namespaces/{imm}")).await;
        assert_eq!(st.as_u16(), 403, "{imm}: {body}");
        assert_eq!(
            body["message"],
            format!("namespaces \"{imm}\" is forbidden: this namespace may not be deleted")
        );
        assert_eq!(body["details"]["name"], imm);
        assert_eq!(body["details"]["kind"], "namespaces");
    }

    make_ns(&s, "other").await;
    let (st, body) = s.delete("/api/v1/namespaces/other").await;
    assert!(st.is_success(), "{body}");
}

/// admission_test.go:239 TestAdmissionNamespaceForceLiveLookup: the cache-miss
/// path exists upstream so a create right after the namespace's DELETE never
/// sees stale Active state. Rusternetes reads storage live on every request,
/// so the equivalent invariant is: create immediately after DELETE is refused.
#[tokio::test]
async fn create_right_after_namespace_delete_is_refused() {
    let s = strict();
    make_ns(&s, "test").await;
    let (st, _) = s.post("/api/v1/namespaces/test/pods", &pod()).await;
    assert_eq!(st.as_u16(), 201);
    let (st, _) = s.delete("/api/v1/namespaces/test").await;
    assert!(st.is_success());
    for n in ["a", "b"] {
        let mut p = pod();
        p["metadata"]["name"] = json!(n);
        let (st, _) = s.post("/api/v1/namespaces/test/pods", &p).await;
        assert_eq!(st.as_u16(), 403);
    }
}

/// admission_test.go:101 TestAccessReviewCheckOnMissingNamespace: a
/// localsubjectaccessreview in a missing namespace is not answered NotFound.
#[tokio::test]
async fn access_review_in_missing_namespace_is_not_not_found() {
    let s = strict();
    let (st, body) = s
        .post(
            "/apis/authorization.k8s.io/v1/namespaces/test/localsubjectaccessreviews",
            &json!({"apiVersion":"authorization.k8s.io/v1","kind":"LocalSubjectAccessReview",
                    "metadata":{"namespace":"test"},
                    "spec":{"user":"u","resourceAttributes":{"namespace":"test","verb":"get","resource":"pods"}}}),
        )
        .await;
    assert_ne!(
        body["message"], "namespaces \"test\" not found",
        "{st} {body}"
    );
}

/// Exempt: the namespace collection itself, and cluster-scoped resources.
#[tokio::test]
async fn namespace_create_and_cluster_scoped_are_exempt() {
    let s = strict();
    make_ns(&s, "fresh").await;
    let (st, body) = s
        .post(
            "/api/v1/nodes",
            &json!({"apiVersion":"v1","kind":"Node","metadata":{"name":"n1"}}),
        )
        .await;
    assert_ne!(st.as_u16(), 404, "{body}");
}
