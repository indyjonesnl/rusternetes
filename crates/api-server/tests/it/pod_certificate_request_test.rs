//! PodCertificateRequest on the generic Store: the strategy rules of
//! `pkg/registry/certificates/podcertificaterequest/strategy.go`, served only
//! under the `PodCertificateRequest` feature gate.

use axum::http::StatusCode;
use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PCRS: &str = "/apis/certificates.k8s.io/v1beta1/namespaces/default/podcertificaterequests";

/// Ed25519 SPKI and the signature over the bytes "pod-uid-1" (base64), made
/// with `openssl pkeyutl -sign -rawin`.
const PUB: &str = "MCowBQYDK2VwAyEA9MWfsHCpyPcshwusOEQuuggOyngHQW+HnrWWLElRlAM=";
const POP: &str =
    "YbSVWb8bJBrwbDKgGdvcXuYxvTud2jUNjUfvj34xvR/ABwGNXh2KQgyq7QfyxLvizyOWcp7OBSe1aTpwZQZ+Dg==";

fn pcr(name: &str) -> Value {
    json!({
        "apiVersion": "certificates.k8s.io/v1beta1",
        "kind": "PodCertificateRequest",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {
            "signerName": "example.com/signer",
            "podName": "pod-1", "podUID": "pod-uid-1",
            "serviceAccountName": "sa", "serviceAccountUID": "sa-uid",
            "nodeName": "node-1", "nodeUID": "node-uid",
            "maxExpirationSeconds": 86400,
            "pkixPublicKey": PUB,
            "proofOfPossession": POP,
        },
        // A client cannot create a request with a status.
        "status": {"certificateChain": "ignored"}
    })
}

fn denied() -> Value {
    json!({"type": "Denied", "status": "True", "reason": "NoWay",
           "lastTransitionTime": "2030-01-01T00:00:00Z"})
}

/// With the gate off (the v1.35 default) the storage was never installed.
#[tokio::test]
#[serial_test::serial]
async fn every_verb_is_a_404_while_the_gate_is_off() {
    let _gate = with_feature(Feature::PodCertificateRequest, false);
    let api = TestApiServer::new();
    let (s, body) = api.post(PCRS, &pcr("p")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
    let (s, body) = api.get(PCRS).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
}

/// `PrepareForCreate` (strategy.go:63-66) drops the client's status.
#[tokio::test]
#[serial_test::serial]
async fn create_clears_status_and_validates() {
    let _gate = with_feature(Feature::PodCertificateRequest, true);
    let api = TestApiServer::new();
    let (s, body) = api.post(PCRS, &pcr("p")).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["apiVersion"], "certificates.k8s.io/v1beta1", "{body}");
    assert!(
        body["status"].get("certificateChain").is_none(),
        "status must be wiped on create: {body}"
    );

    // ValidatePodCertificateRequestCreate: a bad proof of possession.
    let mut bad = pcr("bad");
    bad["spec"]["podUID"] = json!("another-uid");
    let (s, body) = api.post(PCRS, &bad).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("could not verify proof-of-possession signature"),
        "{body}"
    );
}

/// `ValidatePodCertificateRequestUpdate`: the spec is immutable; a plain PUT
/// does not touch the status (`PrepareForUpdate`, strategy.go:83-87).
#[tokio::test]
#[serial_test::serial]
async fn a_put_cannot_change_spec_or_status() {
    let _gate = with_feature(Feature::PodCertificateRequest, true);
    let api = TestApiServer::new();
    let (_, created) = api.post(PCRS, &pcr("p")).await;
    let rv = created["metadata"]["resourceVersion"].clone();

    let mut put = pcr("p");
    put["metadata"]["resourceVersion"] = rv.clone();
    put["spec"]["nodeName"] = json!("other-node");
    let (s, body) = api.put(&format!("{PCRS}/p"), &put).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"]
        .as_str()
        .unwrap_or_default()
        .contains("spec"));

    let mut put = pcr("p");
    put["metadata"]["resourceVersion"] = rv;
    put["status"] = json!({"conditions": [denied()]});
    let (s, body) = api.put(&format!("{PCRS}/p"), &put).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert!(body["status"].get("conditions").is_none(), "{body}");
}

/// The status strategy: `/status` takes the conditions, ignores a spec change,
/// and the terminal status is then immutable.
#[tokio::test]
#[serial_test::serial]
async fn the_status_subresource_takes_a_terminal_condition_once() {
    let _gate = with_feature(Feature::PodCertificateRequest, true);
    let api = TestApiServer::new();
    let (_, created) = api.post(PCRS, &pcr("p")).await;

    let mut put = created.clone();
    put["spec"]["nodeName"] = json!("ignored");
    put["status"] = json!({"conditions": [denied()]});
    let (s, body) = api.put(&format!("{PCRS}/p/status"), &put).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["status"]["conditions"][0]["type"], "Denied", "{body}");
    assert_eq!(body["spec"]["nodeName"], "node-1", "{body}");

    // Denied is terminal: the whole status is immutable.
    let mut again = body.clone();
    again["status"]["certificateChain"] = json!("x");
    let (s, body) = api.put(&format!("{PCRS}/p/status"), &again).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"]
        .as_str()
        .unwrap_or_default()
        .contains("immutable after PodCertificateRequest is issued, denied, or failed"));

    // A condition type outside Issued/Denied/Failed is unsupported.
    let (_, other) = api.post(PCRS, &pcr("q")).await;
    let mut put = other.clone();
    put["status"] = json!({"conditions": [{"type": "Bogus", "status": "True",
        "reason": "R", "lastTransitionTime": "2030-01-01T00:00:00Z"}]});
    let (s, body) = api.put(&format!("{PCRS}/q/status"), &put).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"]
        .as_str()
        .unwrap_or_default()
        .contains("status.conditions[0].type"));
}

/// `getAttrs` (storage.go): spec.signerName / podName / nodeName are
/// selectable fields.
#[tokio::test]
#[serial_test::serial]
async fn list_filters_by_the_selectable_spec_fields() {
    let _gate = with_feature(Feature::PodCertificateRequest, true);
    let api = TestApiServer::new();
    api.post(PCRS, &pcr("a")).await;
    // node-2 needs its own proof? No: the proof signs the podUID only.
    let mut b = pcr("b");
    b["spec"]["nodeName"] = json!("node-2");
    let (s, body) = api.post(PCRS, &b).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");

    let (s, list) = api
        .get(&format!("{PCRS}?fieldSelector=spec.nodeName%3Dnode-2"))
        .await;
    assert_eq!(s, StatusCode::OK, "{list}");
    assert_eq!(list["kind"], "PodCertificateRequestList", "{list}");
    let items = list["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "{list}");
    assert_eq!(items[0]["metadata"]["name"], "b");
}

/// storage_certificates.go:106-117: `podcertificaterequests` and its `/status`
/// are installed under the PodCertificateRequest gate alone (a ClusterTrustBundle
/// gate that is off does not hide them), and the v1beta1 version is dropped
/// only when neither is on.
#[tokio::test]
#[serial_test::serial]
async fn discovery_lists_podcertificaterequests_under_their_own_gate() {
    let _ctb = with_feature(Feature::ClusterTrustBundle, false);
    let _pcr = with_feature(Feature::PodCertificateRequest, true);
    let api = TestApiServer::new();

    let (s, list) = api.get("/apis/certificates.k8s.io/v1beta1").await;
    assert_eq!(s, StatusCode::OK, "{list}");
    let res = list["resources"].as_array().unwrap();
    let names: Vec<&str> = res.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert_eq!(
        names,
        vec!["podcertificaterequests", "podcertificaterequests/status"],
        "{list}"
    );
    assert_eq!(res[0]["kind"], "PodCertificateRequest");
    assert_eq!(res[0]["namespaced"], true);
    assert_eq!(res[0]["singularName"], "podcertificaterequest");
    assert_eq!(
        res[0]["verbs"],
        json!([
            "create",
            "delete",
            "deletecollection",
            "get",
            "list",
            "patch",
            "update",
            "watch"
        ])
    );
    assert_eq!(res[1]["verbs"], json!(["get", "patch", "update"]));

    let (_, groups) = api.get("/apis").await;
    let certs = groups["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["name"] == "certificates.k8s.io")
        .unwrap();
    assert_eq!(certs["versions"].as_array().unwrap().len(), 2, "{certs}");
}

/// Both gates on: both resources, ClusterTrustBundle first.
#[tokio::test]
#[serial_test::serial]
async fn discovery_lists_both_v1beta1_resources_when_both_gates_are_on() {
    let _ctb = with_feature(Feature::ClusterTrustBundle, true);
    let _pcr = with_feature(Feature::PodCertificateRequest, true);
    let api = TestApiServer::new();
    let (_, list) = api.get("/apis/certificates.k8s.io/v1beta1").await;
    let names: Vec<&str> = list["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec![
            "clustertrustbundles",
            "podcertificaterequests",
            "podcertificaterequests/status"
        ]
    );
}

/// installer.go registers the deprecated `/watch/` routes for the namespaced
/// and the all-namespaces scope; with the gate off they are 404.
#[tokio::test]
#[serial_test::serial]
async fn watch_routes_exist_and_follow_the_gate() {
    let paths = [
        "/apis/certificates.k8s.io/v1beta1/watch/podcertificaterequests?timeoutSeconds=1",
        "/apis/certificates.k8s.io/v1beta1/watch/namespaces/default/podcertificaterequests?timeoutSeconds=1",
    ];
    {
        let _gate = with_feature(Feature::PodCertificateRequest, false);
        let api = TestApiServer::new();
        for path in paths {
            let (s, body) = api.get(path).await;
            assert_eq!(s, StatusCode::NOT_FOUND, "{path} {body}");
        }
    }
    let _gate = with_feature(Feature::PodCertificateRequest, true);
    let api = TestApiServer::new();
    for path in paths {
        let (s, _) = api.get(path).await;
        assert_eq!(s, StatusCode::OK, "{path}");
    }
}
