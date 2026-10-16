//! Mirror of the PriorityClass endpoints conformance case (#3758).
//!
//! Upstream (`release-1.35`): `test/e2e/scheduling/preemption.go:944`
//! "verify PriorityClass endpoints can be operated with different HTTP
//! methods". Pure API: the `BeforeEach` (`:896-908`) creates p1 and p2, then
//! the body patches and updates immutable and mutable fields.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PCS: &str = "/apis/scheduling.k8s.io/v1/priorityclasses";

fn rv(o: &Value) -> u128 {
    o["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

/// `patchPriorityClass` (preemption.go:1055): a two-way strategic merge patch
/// of the changed field only.
async fn patch(api: &TestApiServer, name: &str, p: Value) -> (u16, Value) {
    let (s, v) = api
        .send(
            "PATCH",
            &format!("{PCS}/{name}"),
            Some("application/strategic-merge-patch+json"),
            Some(&p),
        )
        .await;
    (s.as_u16(), v)
}

#[tokio::test]
async fn conformance_priorityclass_endpoints_http_methods() {
    let api = TestApiServer::builder().strict_namespaces().build();
    // :896-908 BeforeEach: p1 (value 1) and p2 (value 2) labelled e2e=<uuid>.
    let mut pcs = Vec::new();
    for i in 1..=2 {
        let (s, pc) = api
            .post(
                PCS,
                &json!({"apiVersion":"scheduling.k8s.io/v1","kind":"PriorityClass",
                        "metadata":{"name":format!("p{i}"),"labels":{"e2e":"uuid"}},"value":i}),
            )
            .await;
        assert_eq!(s, 201, "{pc}");
        pcs.push(pc);
    }

    // :946-950 1. patching an immutable field (value * 10) fails.
    let (s, e) = patch(&api, "p1", json!({"value": 10})).await;
    assert!(
        s >= 400,
        "expect a patch error on an immutable field: {s} {e}"
    );

    // :952-957 updating an immutable field fails.
    let mut copy = pcs[1].clone();
    copy["value"] = json!(20);
    let (s, e) = api.put(&format!("{PCS}/p2"), &copy).await;
    assert!(
        s.as_u16() >= 400,
        "expect an update error on an immutable field: {s} {e}"
    );

    // :959-969 2. mutable field (description): patch p1, update p2.
    let new_desc = "updated description";
    let (s, e) = patch(&api, "p1", json!({"description": new_desc})).await;
    assert_eq!(s, 200, "{e}");
    let mut copy = pcs[1].clone();
    copy["description"] = json!(new_desc);
    let (s, e) = api.put(&format!("{PCS}/p2"), &copy).await;
    assert_eq!(s, 200, "{e}");

    // :971-973 3. list.
    let (s, l) = api.get(PCS).await;
    assert_eq!(s, 200, "{l}");

    // :975-982 4. verify the live objects.
    for pc in &pcs {
        let name = pc["metadata"]["name"].as_str().unwrap();
        let (s, live) = api.get(&format!("{PCS}/{name}")).await;
        assert_eq!(s, 200, "{live}");
        assert_eq!(live["value"], pc["value"]);
        assert_eq!(live["description"], new_desc);
        assert!(
            rv(pc) < rv(&live),
            "changed object should have a larger resource version"
        );
    }
}
