//! Mirrors of the AggregatedDiscovery conformance cases (#3758).
//!
//! Upstream (`release-1.35`), `test/e2e/apimachinery/aggregated_discovery.go`:
//!
//! | case | line |
//! |---|---|
//! | raw aggregated discovery endpoint Accept headers | `:151` |
//! | raw aggregated discovery request for CRDs | `:194` |
//! | aggregated discovery interface | `:259` |
//! | aggregated discovery interface for CRDs | `:303` |
//!
//! The client-go `AggregatedDiscoveryInterface.GroupsAndMaybeResources` call
//! is two GETs (`/api`, `/apis`) with the aggregated `Accept` header whose
//! documents are folded into per-group-version resource lists; the mirror
//! issues the same GETs and folds them the same way.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

/// aggregated_discovery.go:49 `aggregatedAccept`.
const ACCEPT: &str = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList";

/// aggregated_discovery.go:42-48 `expectedLegacyGVR`.
const LEGACY_GVR: &[(&str, &str, &str)] = &[("", "v1", "namespaces")];

/// aggregated_discovery.go:49-123 `expectedGVR`.
const GVR: &[(&str, &str, &str)] = &[
    (
        "admissionregistration.k8s.io",
        "v1",
        "validatingwebhookconfigurations",
    ),
    ("apiextensions.k8s.io", "v1", "customresourcedefinitions"),
    ("apiregistration.k8s.io", "v1", "apiservices"),
    ("apps", "v1", "deployments"),
    ("authentication.k8s.io", "v1", "tokenreviews"),
    ("authorization.k8s.io", "v1", "selfsubjectaccessreviews"),
    ("autoscaling", "v1", "horizontalpodautoscalers"),
    ("autoscaling", "v2", "horizontalpodautoscalers"),
    ("batch", "v1", "jobs"),
    ("certificates.k8s.io", "v1", "certificatesigningrequests"),
    ("coordination.k8s.io", "v1", "leases"),
    ("discovery.k8s.io", "v1", "endpointslices"),
    ("events.k8s.io", "v1", "events"),
    ("networking.k8s.io", "v1", "ingresses"),
    ("node.k8s.io", "v1", "runtimeclasses"),
    ("policy", "v1", "poddisruptionbudgets"),
    ("scheduling.k8s.io", "v1", "priorityclasses"),
    ("storage.k8s.io", "v1", "csinodes"),
];

async fn aggregated(api: &TestApiServer, path: &str) -> Value {
    let (s, _h, bytes, v) = api.send_full("GET", path, None, Some(ACCEPT), None).await;
    assert_eq!(
        s, 200,
        "Failed to get raw aggregated discovery document: {path}"
    );
    assert!(
        v["kind"] == "APIGroupDiscoveryList"
            && v["apiVersion"].as_str() == Some("apidiscovery.k8s.io/v2"),
        "Failed to parse discovery: {}",
        String::from_utf8_lossy(&bytes)
    );
    v
}

/// `isGVRPresentAPIDiscovery` (aggregated_discovery.go:397-413).
fn gvr_present_api_discovery(list: &Value, (g, v, r): (&str, &str, &str)) -> bool {
    list["items"].as_array().is_some_and(|items| {
        items.iter().any(|group| {
            group["metadata"]["name"].as_str().unwrap_or("") == g
                && group["versions"].as_array().is_some_and(|vs| {
                    vs.iter().any(|ver| {
                        ver["version"] == v
                            && ver["resources"]
                                .as_array()
                                .is_some_and(|rs| rs.iter().any(|res| res["resource"] == r))
                    })
                })
        })
    })
}

/// A CRD with a random group, `testcrd`, one served+storage `v1`, namespaced
/// (aggregated_discovery.go:205-227), created via
/// `fixtures.CreateNewV1CustomResourceDefinition`.
async fn create_testcrd(api: &TestApiServer, group_prefix: &str) -> (String, String, String) {
    let group = format!("{group_prefix}.example.com");
    let name = format!("testcrds.{group}");
    let (s, v) = api
        .post(
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            &json!({
                "apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
                "metadata": {"name": name},
                "spec": {
                    "group": group,
                    "versions": [{"name": "v1", "served": true, "storage": true,
                        "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}],
                    "names": {"plural": "testcrds", "singular": "testcrd", "kind": "testcrd", "listKind": "testcrdList"},
                    "scope": "Namespaced",
                },
            }),
        )
        .await;
    assert_eq!(s, 201, "{v}");
    api.wait_crd_established(&name).await;
    (group, "v1".into(), "testcrds".into())
}

/// aggregated_discovery.go:151 "should support raw aggregated discovery
/// endpoint Accept headers".
#[tokio::test]
async fn conformance_aggregated_discovery_raw_accept_headers() {
    let api = TestApiServer::new();
    // :152-165
    let group_list = aggregated(&api, "/apis").await;
    for gvr in GVR {
        assert!(
            gvr_present_api_discovery(&group_list, *gvr),
            "Expected gvr {} {} {} to exist in discovery",
            gvr.0,
            gvr.1,
            gvr.2
        );
    }
    // :167-182
    let legacy = aggregated(&api, "/api").await;
    for gvr in LEGACY_GVR {
        assert!(
            gvr_present_api_discovery(&legacy, *gvr),
            "Expected legacy gvr api {} {} to exist in discovery",
            gvr.1,
            gvr.2
        );
    }
}

/// aggregated_discovery.go:194 "should support raw aggregated discovery
/// request for CRDs".
#[tokio::test]
async fn conformance_aggregated_discovery_raw_request_for_crds() {
    let api = TestApiServer::new();
    // :205-235 create the CRD, wait until established.
    let (g, v, r) = create_testcrd(&api, "group1").await;
    // :240-258 poll /apis with the aggregated Accept header until the GVR is
    // present (synchronous store: one poll, retried briefly for the
    // asynchronous discovery refresh).
    let mut present = false;
    for _ in 0..300 {
        let list = aggregated(&api, "/apis").await;
        if gvr_present_api_discovery(&list, (&g, &v, &r)) {
            present = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        present,
        "timed out waiting for CustomResourceDefinition GVR to appear in Discovery"
    );
}

/// `GroupsAndMaybeResources` (client-go discovery/aggregated_discovery.go
/// `SplitGroupsAndResources`): fold the `/api` and `/apis` aggregated
/// documents into the `(groups, resourcesByGV)` pair.
async fn groups_and_maybe_resources(
    api: &TestApiServer,
) -> (Vec<(String, String)>, Vec<(String, String, String)>) {
    let mut gvs = Vec::new();
    let mut gvrs = Vec::new();
    for path in ["/api", "/apis"] {
        let doc = aggregated(api, path).await;
        for group in doc["items"].as_array().unwrap() {
            let g = group["metadata"]["name"].as_str().unwrap_or("").to_string();
            for ver in group["versions"].as_array().into_iter().flatten() {
                let v = ver["version"].as_str().unwrap().to_string();
                gvs.push((g.clone(), v.clone()));
                for res in ver["resources"].as_array().into_iter().flatten() {
                    gvrs.push((
                        g.clone(),
                        v.clone(),
                        res["resource"].as_str().unwrap().to_string(),
                    ));
                }
            }
        }
    }
    (gvs, gvrs)
}

/// aggregated_discovery.go:259 "should support aggregated discovery
/// interface".
#[tokio::test]
async fn conformance_aggregated_discovery_interface() {
    let api = TestApiServer::new();
    // :260-270
    let (server_groups, by_gv) = groups_and_maybe_resources(&api).await;
    // :272-281 combined = expectedGVR + expectedLegacyGVR.
    let combined: Vec<_> = GVR.iter().chain(LEGACY_GVR.iter()).collect();
    // :283-287
    for (g, v, r) in &combined {
        assert!(
            by_gv.iter().any(|x| x.0 == *g && x.1 == *v && x.2 == *r),
            "Expected {g}/{v}/{r} to be present"
        );
    }
    // :289-297
    assert!(
        !server_groups.is_empty(),
        "Expected serverGroups to be non-nil"
    );
    for (g, v, _) in &combined {
        assert!(
            server_groups.iter().any(|x| x.0 == *g && x.1 == *v),
            "Expected {g}/{v} to be present"
        );
    }
}

/// aggregated_discovery.go:303 "should support aggregated discovery interface
/// for CRDs".
#[tokio::test]
async fn conformance_aggregated_discovery_interface_for_crds() {
    let api = TestApiServer::new();
    // :316-345
    let (g, v, r) = create_testcrd(&api, "group2").await;
    // :347-361 poll `GroupsAndMaybeResources` until the GVR shows up.
    let mut present = false;
    for _ in 0..300 {
        let (_, by_gv) = groups_and_maybe_resources(&api).await;
        if by_gv.iter().any(|x| x.0 == g && x.1 == v && x.2 == r) {
            present = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        present,
        "timed out waiting for CustomResourceDefinition GVR to appear in Discovery"
    );
}
