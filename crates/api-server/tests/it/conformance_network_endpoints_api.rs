//! Mirror of the two API-operations conformance cases for Endpoints and
//! EndpointSlices (#3741, the rest of #1786), Kubernetes release-1.35:
//!
//! * `test/e2e/network/endpointslice.go:274`
//!   "should support creating EndpointSlice API operations"
//! * `test/e2e/network/endpoints.go:58`
//!   "should test the lifecycle of an Endpoint"
//!
//! Each follows its case body step by step through the HTTP API. The
//! EndpointSlice case description (endpointslice.go:269-272) states the
//! discovery documents as requirements, so those are asserted first.
//!
//! Upstream reads its watches with `watchtools.Until` from a list
//! resourceVersion (endpoints.go:95, :133, :183, :216) or keeps one watch
//! open across steps (endpointslice.go:367, :402-424). [`watch_until`] does
//! the former: open a watch from a resourceVersion and read until a
//! predicate matches.
//!
//! Beyond upstream: both label-selected `deletecollection` steps add a
//! bystander outside the selector that must survive, so a handler that
//! ignored the label selector would fail here (#1786, the #1781 lesson).

use std::time::Duration;

use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "endpoints-e2e";
const DISCOVERY_GV: &str = "/apis/discovery.k8s.io/v1";

fn rv_str(obj: &Value) -> String {
    obj["metadata"]["resourceVersion"]
        .as_str()
        .unwrap_or_else(|| panic!("no resourceVersion: {obj}"))
        .to_string()
}

fn rv(obj: &Value) -> u64 {
    rv_str(obj)
        .parse()
        .unwrap_or_else(|_| panic!("non-numeric resourceVersion: {obj}"))
}

/// `watchtools.Until(ctx, rv, w, cond)`: open a watch on `uri` and return the
/// first event for which `cond` holds; panic after 5s, as upstream's context
/// deadline fails the case.
async fn watch_until(
    server: &TestApiServer,
    uri: &str,
    cond: impl Fn(&str, &Value) -> bool,
) -> Value {
    let resp = server.respond("GET", uri, None, None).await;
    assert_eq!(resp.status().as_u16(), 200, "watch {uri} must open");
    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();
    let mut seen = Vec::new();
    let found = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(bytes)) = stream.next().await {
            buf.push_str(&String::from_utf8_lossy(&bytes));
            while let Some(i) = buf.find('\n') {
                let line = buf[..i].to_string();
                buf.drain(..=i);
                let Ok(ev) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                let ty = ev["type"].as_str().unwrap_or_default().to_string();
                if cond(&ty, &ev["object"]) {
                    return Some(ev);
                }
                seen.push(ty);
            }
        }
        None
    })
    .await;
    match found {
        Ok(Some(ev)) => ev,
        _ => panic!("watch {uri}: no matching event; saw {seen:?}"),
    }
}

/// [sig-network] EndpointSlice should support creating EndpointSlice API
/// operations [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpointslice.go:274
#[tokio::test]
async fn endpointslice_api_operations() {
    let server = TestApiServer::new();
    let label = "special-label=endpointslice-e2e";
    let sel = "special-label%3Dendpointslice-e2e";
    let eps_uri = format!("{DISCOVERY_GV}/namespaces/{NS}/endpointslices");

    // endpointslice.go:280-291.
    let template = json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "generateName": "e2e-example-ing",
            "labels": {"special-label": "endpointslice-e2e"},
        },
        "addressType": "IPv4",
        "endpoints": [
            {"addresses": ["1.2.3.4", "5.6.7.8"]},
            {"addresses": ["2.2.3.4", "6.6.7.8"]},
        ],
    });

    // getting /apis (endpointslice.go:292-310)
    let (status, groups) = server.get("/apis").await;
    assert_eq!(status.as_u16(), 200);
    let group = groups["groups"]
        .as_array()
        .expect("groups")
        .iter()
        .find(|g| g["name"] == "discovery.k8s.io")
        .unwrap_or_else(|| panic!("discovery.k8s.io missing from /apis: {groups}"))
        .clone();
    assert!(
        group["versions"]
            .as_array()
            .expect("versions")
            .iter()
            .any(|v| v["version"] == "v1"),
        "discovery.k8s.io/v1 missing from /apis: {group}"
    );

    // getting /apis/discovery.k8s.io (endpointslice.go:312-327)
    let (status, group_doc) = server.get("/apis/discovery.k8s.io").await;
    assert_eq!(status.as_u16(), 200);
    assert!(
        group_doc["versions"]
            .as_array()
            .expect("versions")
            .iter()
            .any(|v| v["version"] == "v1"),
        "v1 missing from /apis/discovery.k8s.io: {group_doc}"
    );

    // getting /apis/discovery.k8s.io/v1 (endpointslice.go:329-344); the
    // description (:272) also names the verb set.
    let (status, resources) = server.get(DISCOVERY_GV).await;
    assert_eq!(status.as_u16(), 200);
    let eps = resources["resources"]
        .as_array()
        .expect("resources")
        .iter()
        .find(|r| r["name"] == "endpointslices")
        .unwrap_or_else(|| panic!("endpointslices missing from {DISCOVERY_GV}: {resources}"))
        .clone();
    for verb in [
        "create",
        "get",
        "list",
        "watch",
        "update",
        "patch",
        "delete",
        "deletecollection",
    ] {
        assert!(
            eps["verbs"]
                .as_array()
                .expect("verbs")
                .iter()
                .any(|v| v == verb),
            "endpointslices must advertise {verb}: {eps}"
        );
    }

    // creating (endpointslice.go:346-352): three from one template.
    let mut created = Value::Null;
    for _ in 0..3 {
        let (status, body) = server.post(&eps_uri, &template).await;
        assert_eq!(status.as_u16(), 201, "create EndpointSlice: {body}");
        created = body;
    }
    let created_name = created["metadata"]["name"]
        .as_str()
        .expect("generated name")
        .to_string();
    assert!(created_name.starts_with("e2e-example-ing"));
    // Not upstream: a bystander outside the label selector.
    let mut bystander = template.clone();
    bystander["metadata"] = json!({"name": "bystander", "labels": {"special-label": "other"}});
    let (status, body) = server.post(&eps_uri, &bystander).await;
    assert_eq!(status.as_u16(), 201, "create bystander: {body}");

    // getting (endpointslice.go:354-358)
    let (status, queried) = server.get(&format!("{eps_uri}/{created_name}")).await;
    assert_eq!(status.as_u16(), 200, "get: {queried}");
    assert_eq!(queried["metadata"]["uid"], created["metadata"]["uid"]);
    rv(&queried);

    // listing (endpointslice.go:360-363)
    let (status, list) = server.get(&format!("{eps_uri}?labelSelector={sel}")).await;
    assert_eq!(status.as_u16(), 200);
    assert_eq!(
        list["items"].as_array().expect("items").len(),
        3,
        "filtered list should have 3 items: {list}"
    );
    let list_rv = rv_str(&list);

    // watching (endpointslice.go:365-368): opened from the list RV and read
    // after the update below.
    let watch_uri = format!("{eps_uri}?watch=true&resourceVersion={list_rv}&labelSelector={sel}");

    // cluster-wide listing (endpointslice.go:372-375)
    let (status, cluster_list) = server
        .get(&format!(
            "{DISCOVERY_GV}/endpointslices?labelSelector={sel}"
        ))
        .await;
    assert_eq!(status.as_u16(), 200);
    assert_eq!(
        cluster_list["items"].as_array().expect("items").len(),
        3,
        "filtered cluster-wide list should have 3 items: {cluster_list}"
    );

    // cluster-wide watching (endpointslice.go:377-380)
    let resp = server
        .respond(
            "GET",
            &format!(
                "{DISCOVERY_GV}/endpointslices?watch=true&resourceVersion={list_rv}&labelSelector={sel}"
            ),
            None,
            None,
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200, "cluster-wide watch must open");
    drop(resp);

    // patching (endpointslice.go:382-386): a merge patch.
    let (status, patched) = server
        .patch(
            &format!("{eps_uri}/{created_name}"),
            &json!({"metadata": {"annotations": {"patched": "true"}}}),
        )
        .await;
    assert_eq!(status.as_u16(), 200, "patch: {patched}");
    assert_eq!(patched["metadata"]["annotations"]["patched"], "true");
    assert!(
        rv(&patched) > rv(&created),
        "patched object should have a larger resource version"
    );

    // updating (endpointslice.go:388-399)
    let (status, mut to_update) = server.get(&format!("{eps_uri}/{created_name}")).await;
    assert_eq!(status.as_u16(), 200);
    to_update["metadata"]["annotations"]["updated"] = json!("true");
    let (status, updated) = server
        .put(&format!("{eps_uri}/{created_name}"), &to_update)
        .await;
    assert_eq!(status.as_u16(), 200, "update: {updated}");
    assert_eq!(updated["metadata"]["annotations"]["updated"], "true");

    // waiting for watch events with expected annotations
    // (endpointslice.go:401-424): every event after the list RV is MODIFIED,
    // and one carries the patched annotation.
    let ev = watch_until(&server, &watch_uri, |ty, obj| {
        assert_eq!(ty, "MODIFIED", "unexpected event: {obj}");
        obj["metadata"]["annotations"]["patched"] == "true"
    })
    .await;
    assert_eq!(ev["object"]["kind"], "EndpointSlice");

    // deleting (endpointslice.go:426-441)
    let (status, body) = server.delete(&format!("{eps_uri}/{created_name}")).await;
    assert_eq!(status.as_u16(), 200, "delete: {body}");
    let (status, _) = server.get(&format!("{eps_uri}/{created_name}")).await;
    assert_eq!(status.as_u16(), 404, "expected 404 after delete");
    let (status, list) = server.get(&format!("{eps_uri}?labelSelector={sel}")).await;
    assert_eq!(status.as_u16(), 200);
    let items = list["items"].as_array().expect("items");
    assert_eq!(items.len(), 2, "filtered list should have 2 items: {list}");
    assert!(
        items
            .iter()
            .all(|i| i["metadata"]["name"] != created_name.as_str()),
        "listing after deleting createdEPS: {list}"
    );

    // deleting a collection (endpointslice.go:443-448)
    let (status, body) = server
        .delete(&format!("{eps_uri}?labelSelector={sel}"))
        .await;
    assert_eq!(status.as_u16(), 200, "deletecollection ({label}): {body}");
    let (status, list) = server.get(&format!("{eps_uri}?labelSelector={sel}")).await;
    assert_eq!(status.as_u16(), 200);
    assert!(
        list["items"].as_array().expect("items").is_empty(),
        "filtered list should have 0 items: {list}"
    );

    // Not upstream: the selector bounded the collection delete.
    let (status, survivor) = server.get(&format!("{eps_uri}/bystander")).await;
    assert_eq!(status.as_u16(), 200, "bystander must survive: {survivor}");
}

/// [sig-network] Endpoints should test the lifecycle of an Endpoint
/// [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/endpoints.go:58
#[tokio::test]
async fn endpoint_lifecycle() {
    let server = TestApiServer::new();
    let ep_uri = format!("/api/v1/namespaces/{NS}/endpoints");
    let sel = "test-endpoint-static%3Dtrue";
    let watch_from =
        |rv: &str| format!("{ep_uri}?watch=true&resourceVersion={rv}&labelSelector={sel}");
    // The predicate of every `watchtools.Until` in the case.
    let is_test_ep = |obj: &Value| {
        obj["metadata"]["name"] == "testservice"
            && obj["metadata"]["labels"]["test-endpoint-static"] == "true"
    };

    // endpoints.go:61-78.
    let test_endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "testservice",
            "labels": {"test-endpoint-static": "true"},
        },
        "subsets": [{
            "addresses": [{"ip": "10.0.0.24"}],
            "ports": [{"name": "http", "port": 80, "protocol": "TCP"}],
        }],
    });

    // endpoints.go:85: a cluster-wide list for the starting RV.
    let (status, list) = server
        .get(&format!("/api/v1/endpoints?labelSelector={sel}"))
        .await;
    assert_eq!(status.as_u16(), 200);
    let list_rv = rv_str(&list);

    // Not upstream: a bystander outside the deletecollection selector.
    let (status, body) = server
        .post(
            &ep_uri,
            &json!({
                "apiVersion": "v1",
                "kind": "Endpoints",
                "metadata": {"name": "bystander", "labels": {"test-endpoint-static": "false"}},
            }),
        )
        .await;
    assert_eq!(status.as_u16(), 201, "create bystander: {body}");

    // creating an Endpoint (endpoints.go:88-91)
    let (status, created) = server.post(&ep_uri, &test_endpoints).await;
    assert_eq!(status.as_u16(), 201, "create Endpoint: {created}");
    rv(&created);

    // waiting for available Endpoint (endpoints.go:92-108)
    watch_until(&server, &watch_from(&list_rv), |ty, obj| {
        ty == "ADDED" && is_test_ep(obj)
    })
    .await;

    // listing all Endpoints (endpoints.go:110-124)
    let (status, list) = server
        .get(&format!("/api/v1/endpoints?labelSelector={sel}"))
        .await;
    assert_eq!(status.as_u16(), 200);
    let list_rv = rv_str(&list);
    let mut found = list["items"]
        .as_array()
        .expect("items")
        .iter()
        .find(|e| e["metadata"]["name"] == "testservice" && e["metadata"]["namespace"] == NS)
        .unwrap_or_else(|| panic!("unable to find Endpoint Service in list of Endpoints: {list}"))
        .clone();

    // updating the Endpoint (endpoints.go:126-129)
    found["metadata"]["labels"]["test-service"] = json!("updated");
    let (status, body) = server.put(&format!("{ep_uri}/testservice"), &found).await;
    assert_eq!(status.as_u16(), 200, "update Endpoint: {body}");

    // endpoints.go:131-146
    watch_until(&server, &watch_from(&list_rv), |ty, obj| {
        ty == "MODIFIED" && is_test_ep(obj)
    })
    .await;

    // fetching the Endpoint (endpoints.go:148-151)
    let (status, endpoints) = server.get(&format!("{ep_uri}/testservice")).await;
    assert_eq!(status.as_u16(), 200);
    assert_eq!(endpoints["metadata"]["labels"]["test-service"], "updated");

    // patching the Endpoint (endpoints.go:153-180): a strategic merge patch.
    // `subsets` carries no patch strategy, so the list is replaced.
    let patch = json!({
        "metadata": {"labels": {"test-service": "patched"}},
        "subsets": [{
            "addresses": [{"ip": "10.0.0.25"}],
            "ports": [{"name": "http-test", "port": 8080}],
        }],
    });
    let (status, patched_ep) = server
        .send(
            "PATCH",
            &format!("{ep_uri}/testservice"),
            Some("application/strategic-merge-patch+json"),
            Some(&patch),
        )
        .await;
    assert_eq!(status.as_u16(), 200, "patch Endpoint: {patched_ep}");
    assert!(
        rv(&patched_ep) > rv(&created),
        "patched object should have a larger resource version"
    );

    // endpoints.go:181-196
    watch_until(&server, &watch_from(&rv_str(&endpoints)), |ty, obj| {
        ty == "MODIFIED" && is_test_ep(obj)
    })
    .await;

    // fetching the Endpoint (endpoints.go:198-207)
    let (status, endpoints) = server.get(&format!("{ep_uri}/testservice")).await;
    assert_eq!(status.as_u16(), 200);
    assert_eq!(endpoints["metadata"]["labels"]["test-service"], "patched");
    let subset = &endpoints["subsets"][0];
    assert_eq!(subset["addresses"][0]["ip"], "10.0.0.25", "{endpoints}");
    assert_eq!(subset["ports"][0]["name"], "http-test", "{endpoints}");
    assert_eq!(subset["ports"][0]["port"], 8080, "{endpoints}");

    // deleting the Endpoint by Collection (endpoints.go:209-211)
    let (status, body) = server
        .delete(&format!("{ep_uri}?labelSelector={sel}"))
        .await;
    assert_eq!(status.as_u16(), 200, "deletecollection: {body}");

    // waiting for Endpoint deletion (endpoints.go:213-229)
    watch_until(&server, &watch_from(&rv_str(&endpoints)), |ty, obj| {
        ty == "DELETED" && is_test_ep(obj)
    })
    .await;

    // fetching the Endpoint (endpoints.go:231-233)
    let (status, _) = server.get(&format!("{ep_uri}/testservice")).await;
    assert_eq!(status.as_u16(), 404, "should not be able to fetch Endpoint");

    // Not upstream: the selector bounded the collection delete.
    let (status, survivor) = server.get(&format!("{ep_uri}/bystander")).await;
    assert_eq!(status.as_u16(), 200, "bystander must survive: {survivor}");
}
