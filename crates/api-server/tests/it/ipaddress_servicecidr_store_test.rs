//! HTTP contracts for the IPAddress / ServiceCIDR generic Store migration.
//!
//! Upstream: pkg/registry/networking/{ipaddress,servicecidr}/strategy.go and
//! storage/storage.go (NewREST, StatusREST), and
//! pkg/apis/networking/validation/validation.go (ValidateIPAddress*,
//! ValidateServiceCIDR*). The strategy tests there supply the cases.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const IP: &str = "/apis/networking.k8s.io/v1/ipaddresses";
const SC: &str = "/apis/networking.k8s.io/v1/servicecidrs";

fn ip(name: &str) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "IPAddress",
        "metadata": {"name": name},
        "spec": {"parentRef": {"group": "", "resource": "services",
            "namespace": "default", "name": "web"}}
    })
}

fn sc(name: &str, cidrs: Value) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "ServiceCIDR",
        "metadata": {"name": name}, "spec": {"cidrs": cidrs}
    })
}

fn causes(out: &Value) -> Vec<String> {
    out["details"]["causes"]
        .as_array()
        .map(|c| {
            c.iter()
                .filter_map(|c| c["field"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// validation.go:759-765 ValidateIPAddress runs ValidateObjectMeta with
/// ValidateIPAddressName, and the strategy's noopNameGenerator
/// (ipaddress/strategy.go:38-42) never invents a name: generateName is the
/// name verbatim, so "10.0.0." fails the IP-name rule instead of getting a
/// random suffix.
#[tokio::test]
async fn ipaddress_name_must_be_canonical_ip_and_is_never_generated() {
    let api = TestApiServer::new();
    for name in ["not-an-ip", "2001:db8:0:0:0:0:0:1"] {
        let (status, out) = api.post(IP, &ip(name)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{name}: {out}");
        assert!(causes(&out).contains(&"metadata.name".to_string()), "{out}");
    }
    let mut body = ip("ignored");
    body["metadata"] = json!({"generateName": "10.0.0."});
    let (status, out) = api.post(IP, &body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(causes(&out).contains(&"metadata.name".to_string()), "{out}");

    let (status, out) = api.post(IP, &ip("2001:db8::1")).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
}

/// validation.go:768-774 / :771-773: parentRef is required, and resource and
/// name inside it.
#[tokio::test]
async fn ipaddress_requires_parent_ref() {
    let api = TestApiServer::new();
    let mut body = ip("10.0.0.1");
    body["spec"] = json!({});
    let (status, out) = api.post(IP, &body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert_eq!(causes(&out), vec!["spec.parentRef"], "{out}");

    body["spec"] = json!({"parentRef": {"group": ""}});
    let (status, out) = api.post(IP, &body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert_eq!(
        causes(&out),
        vec!["spec.parentRef.resource", "spec.parentRef.name"],
        "{out}"
    );
}

/// validation.go:812-817 ValidateIPAddressUpdate: spec.parentRef is immutable;
/// ipaddress/strategy.go:84-86 AllowCreateOnUpdate is false; :98-100 allows an
/// update that carries no resourceVersion.
#[tokio::test]
async fn ipaddress_update_parent_ref_immutable_no_create_on_update() {
    let api = TestApiServer::new();
    assert_eq!(api.post(IP, &ip("10.0.0.2")).await.0, StatusCode::CREATED);
    let uri = format!("{IP}/10.0.0.2");

    let mut changed = ip("10.0.0.2");
    changed["spec"]["parentRef"]["name"] = json!("other");
    let (status, out) = api.put(&uri, &changed).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert_eq!(causes(&out), vec!["spec.parentRef"], "{out}");

    let (status, out) = api
        .patch(&uri, &json!({"spec": {"parentRef": {"name": "other"}}}))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");

    let mut labelled = ip("10.0.0.2");
    labelled["metadata"]["labels"] = json!({"team": "a"});
    let (status, out) = api.put(&uri, &labelled).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["labels"]["team"], "a", "{out}");

    let (status, out) = api.put(&format!("{IP}/10.0.0.9"), &ip("10.0.0.9")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
}

/// There is no ipaddresses/status upstream (ipaddress/storage/storage.go
/// registers only the main store).
#[tokio::test]
async fn ipaddress_has_no_status_subresource() {
    let api = TestApiServer::new();
    assert_eq!(api.post(IP, &ip("10.0.0.3")).await.0, StatusCode::CREATED);
    let (status, _) = api.get(&format!("{IP}/10.0.0.3/status")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// Delete retains finalizers and the update that drains the last one deletes
/// the object (store.go ShouldDeleteDuringUpdate); DeleteCollection honours
/// selectors.
#[tokio::test]
async fn ipaddress_finalizers_and_deletecollection() {
    let api = TestApiServer::new();
    let mut body = ip("10.0.0.4");
    body["metadata"]["finalizers"] = json!(["example.com/cleanup"]);
    body["metadata"]["labels"] = json!({"team": "a"});
    assert_eq!(api.post(IP, &body).await.0, StatusCode::CREATED);
    let mut other = ip("10.0.0.5");
    other["metadata"]["labels"] = json!({"team": "b"});
    assert_eq!(api.post(IP, &other).await.0, StatusCode::CREATED);

    let uri = format!("{IP}/10.0.0.4");
    let (status, out) = api.delete(&uri).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let (_, out) = api.get(&uri).await;
    assert!(out["metadata"]["deletionTimestamp"].is_string(), "{out}");

    let (status, out) = api.delete(&format!("{IP}?labelSelector=team%3Db")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        api.get(&format!("{IP}/10.0.0.5")).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(api.get(&uri).await.0, StatusCode::OK);

    let (status, out) = api
        .patch(&uri, &json!({"metadata": {"finalizers": []}}))
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(api.get(&uri).await.0, StatusCode::NOT_FOUND);
}

/// validation.go:821-825 ValidateServiceCIDR validates ObjectMeta with
/// NameIsDNSSubdomain; servicecidr/strategy.go:40 uses SimpleNameGenerator so
/// generateName works.
#[tokio::test]
async fn servicecidr_name_validated_and_generate_name_works() {
    let api = TestApiServer::new();
    let (status, out) = api.post(SC, &sc("Bad_Name", json!(["10.96.0.0/12"]))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(causes(&out).contains(&"metadata.name".to_string()), "{out}");

    let mut body = sc("ignored", json!(["10.96.0.0/12"]));
    body["metadata"] = json!({"generateName": "gen-"});
    let (status, out) = api.post(SC, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(
        out["metadata"]["name"]
            .as_str()
            .unwrap()
            .starts_with("gen-"),
        "{out}"
    );
}

/// validation.go:827-852 validateServiceCIDRSpec.
#[tokio::test]
async fn servicecidr_spec_validation() {
    let api = TestApiServer::new();
    for (cidrs, field) in [
        (json!([]), "spec.cidrs"),
        (json!(["10.0.0.0/8", "11.0.0.0/8"]), "spec.cidrs"),
        (json!(["nope"]), "spec.cidrs[0]"),
        (
            json!(["10.0.0.0/8", "11.0.0.0/8", "12.0.0.0/8"]),
            "spec.cidrs",
        ),
    ] {
        let (status, out) = api.post(SC, &sc("bad", cidrs.clone())).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{cidrs}: {out}");
        assert!(causes(&out).contains(&field.to_string()), "{out}");
    }
}

/// servicecidr/strategy.go:68-71: PrepareForCreate does not touch status, so
/// a status sent on create is stored (the strategy's doc comment says
/// "clears the status" but the body is empty).
#[tokio::test]
async fn servicecidr_create_keeps_status_sent_by_client() {
    let api = TestApiServer::new();
    let mut body = sc("with-status", json!(["10.96.0.0/12"]));
    body["status"] = json!({"conditions": [{"type": "Ready", "status": "True",
        "reason": "r", "message": "m"}]});
    let (status, out) = api.post(SC, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["status"]["conditions"][0]["type"], "Ready", "{out}");
}

/// validation.go:854-880 ValidateServiceCIDRUpdate: immutable cidrs, except
/// single -> dual-stack by appending; strategy.go:93-95 no create-on-update.
#[tokio::test]
async fn servicecidr_update_immutability_and_no_create_on_update() {
    let api = TestApiServer::new();
    assert_eq!(
        api.post(SC, &sc("one", json!(["10.96.0.0/12"]))).await.0,
        StatusCode::CREATED
    );
    let uri = format!("{SC}/one");
    let (status, out) = api.put(&uri, &sc("one", json!(["10.97.0.0/12"]))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert_eq!(causes(&out), vec!["spec.cidrs[0]"], "{out}");

    let (status, out) = api
        .put(&uri, &sc("one", json!(["10.96.0.0/12", "11.0.0.0/8"])))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");

    let (status, out) = api
        .put(&uri, &sc("one", json!(["10.96.0.0/12", "2001:db8::/64"])))
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["cidrs"].as_array().unwrap().len(), 2, "{out}");

    let (status, out) = api.put(&uri, &sc("one", json!(["10.96.0.0/12"]))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");

    let (status, out) = api
        .put(&format!("{SC}/ghost"), &sc("ghost", json!(["10.0.0.0/8"])))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
}

/// servicecidr/strategy.go:142-149 and storage.go:83-92: /status updates
/// only status; spec and the metadata ResetObjectMetaForStatus keeps cannot be
/// changed through it, and the /status validation is metadata-only.
#[tokio::test]
async fn servicecidr_status_subresource_changes_only_status() {
    let api = TestApiServer::new();
    let mut body = sc("st", json!(["10.96.0.0/12"]));
    body["metadata"]["labels"] = json!({"team": "a"});
    assert_eq!(api.post(SC, &body).await.0, StatusCode::CREATED);
    let uri = format!("{SC}/st");
    let status_uri = format!("{uri}/status");

    let (status, got) = api.get(&status_uri).await;
    assert_eq!(status, StatusCode::OK, "{got}");

    let mut update = got.clone();
    update["spec"]["cidrs"] = json!(["10.99.0.0/16"]);
    update["metadata"]["labels"] = json!({"team": "changed"});
    update["status"] = json!({"conditions": [{"type": "Ready", "status": "True",
        "reason": "Provisioned", "message": "ok"}]});
    let (status, out) = api.put(&status_uri, &update).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["cidrs"], json!(["10.96.0.0/12"]), "{out}");
    assert_eq!(out["metadata"]["labels"]["team"], "a", "{out}");
    assert_eq!(
        out["status"]["conditions"][0]["reason"], "Provisioned",
        "{out}"
    );

    let (status, out) = api
        .patch(
            &status_uri,
            &json!({"status": {"conditions": [{"type": "Ready", "status": "False",
                "reason": "Terminating", "message": "bye"}]}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        out["status"]["conditions"][0]["reason"], "Terminating",
        "{out}"
    );

    // Subresources never allow create on update (storage.go:90-91).
    let (status, out) = api
        .put(
            &format!("{SC}/ghost/status"),
            &sc("ghost", json!(["10.0.0.0/8"])),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
}

/// The servicecidrs controller adds and removes
/// networking.k8s.io/service-cidr-finalizer with strategic merge patches
/// (servicecidrs_controller.go:420-456; the removal's `$deleteFromPrimitiveList/
/// finalizers` form is not implemented by patch.rs, tracked separately, so the
/// removal here is a merge patch); the server runs no other delete
/// check, so delete is retained until the finalizer is patched away.
#[tokio::test]
async fn servicecidr_finalizer_protection_via_strategic_merge_patch() {
    let api = TestApiServer::new();
    assert_eq!(
        api.post(SC, &sc("prot", json!(["10.96.0.0/12"]))).await.0,
        StatusCode::CREATED
    );
    let uri = format!("{SC}/prot");
    let finalizer = "networking.k8s.io/service-cidr-finalizer";
    let (status, out) = api
        .send(
            "PATCH",
            &uri,
            Some("application/strategic-merge-patch+json"),
            Some(&json!({"metadata": {"finalizers": [finalizer]}})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["finalizers"], json!([finalizer]), "{out}");

    let (status, out) = api.delete(&uri).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let (_, out) = api.get(&uri).await;
    assert!(out["metadata"]["deletionTimestamp"].is_string(), "{out}");

    let (status, out) = api
        .send(
            "PATCH",
            &uri,
            Some("application/merge-patch+json"),
            Some(&json!({"metadata": {"finalizers": []}})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(api.get(&uri).await.0, StatusCode::NOT_FOUND);
}
