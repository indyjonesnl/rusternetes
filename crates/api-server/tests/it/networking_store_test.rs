//! HTTP contracts for the networking generic Store migration.
//!
//! Upstream: pkg/registry/networking/{ingress,ingressclass,networkpolicy}/
//! strategy.go and storage/storage.go (NewREST). Strategy tests and Ingress /
//! NetworkPolicy storage tests supply the create/update/status/list/watch cases.

use axum::http::StatusCode;
use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;

const ING: &str = "/apis/networking.k8s.io/v1/namespaces/default/ingresses";
const CLASS: &str = "/apis/networking.k8s.io/v1/ingressclasses";
const POLICY: &str = "/apis/networking.k8s.io/v1/namespaces/default/networkpolicies";

fn ingress(name: &str) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "Ingress",
        "metadata": {"name": name},
        "spec": {"defaultBackend": {"service": {"name": "web", "port": {"number": 80}}}}
    })
}

fn ingress_class(name: &str) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "IngressClass",
        "metadata": {"name": name},
        "spec": {"controller": "example.com/ingress-controller",
            "parameters": {"apiGroup": "example.com", "kind": "Config", "name": "config"}}
    })
}

fn policy(name: &str) -> Value {
    json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": {"name": name},
        "spec": {"podSelector": {}, "ingress": [{"ports": [{"port": 80}]}],
            "egress": [{"ports": [{"port": 53}]}]}
    })
}

fn resources(name: &str) -> [(&'static str, Value); 3] {
    [
        (ING, ingress(name)),
        (CLASS, ingress_class(name)),
        (POLICY, policy(name)),
    ]
}

async fn create(api: &TestApiServer, collection: &str, body: &Value) -> Value {
    let (status, out) = api.post(collection, body).await;
    assert_eq!(status, StatusCode::CREATED, "{collection}: {out}");
    out
}

fn assert_invalid(status: StatusCode, out: &Value, field: &str) {
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        out["message"].as_str().unwrap_or_default().contains(field),
        "{out}"
    );
}

/// PrepareForCreate / PrepareForUpdate: ingress/strategy.go:71-91,
/// ingressclass/strategy.go:49-65, networkpolicy/strategy.go:47-63.
#[tokio::test]
async fn networking_generation_starts_at_one_and_only_spec_changes_bump_it() {
    let api = TestApiServer::new();
    for (collection, mut body) in resources("generation") {
        body["metadata"]["generation"] = json!(77);
        let created = create(&api, collection, &body).await;
        assert_eq!(
            created["metadata"]["generation"], 1,
            "{collection}: {created}"
        );
        let uri = format!("{collection}/generation");
        let (status, mut out) = api
            .patch(&uri, &json!({"metadata": {"labels": {"team": "a"}}}))
            .await;
        assert_eq!(status, StatusCode::OK, "{out}");
        assert_eq!(out["metadata"]["generation"], 1, "{collection}: {out}");
        match collection {
            ING => out["spec"]["defaultBackend"]["service"]["name"] = json!("other"),
            CLASS => out["spec"]["parameters"]["name"] = json!("other"),
            POLICY => out["spec"]["podSelector"] = json!({"matchLabels": {"app": "other"}}),
            _ => unreachable!(),
        }
        let (status, out) = api.put(&uri, &out).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        assert_eq!(out["metadata"]["generation"], 2, "{collection}: {out}");
        let (status, out) = api.put(&uri, &out).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        assert_eq!(out["metadata"]["generation"], 2, "{collection}: {out}");
    }
}

/// pkg/apis/networking/v1/defaults.go:48-51 defaults parameters.scope on
/// every decoded object, including PUT and the result of a PATCH.
#[tokio::test]
async fn ingressclass_scope_defaults_on_create_update_and_patch() {
    let api = TestApiServer::new();
    let body = ingress_class("defaults");
    let created = create(&api, CLASS, &body).await;
    assert_eq!(
        created["spec"]["parameters"]["scope"], "Cluster",
        "{created}"
    );
    let uri = format!("{CLASS}/defaults");
    let (status, out) = api.put(&uri, &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["parameters"]["scope"], "Cluster", "{out}");
    let (status, out) = api
        .patch(&uri, &json!({"spec": {"parameters": {"scope": null}}}))
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["parameters"]["scope"], "Cluster", "{out}");
}

/// pkg/apis/networking/v1/defaults.go:30-45 defaults both rule directions'
/// protocols and adds Egress only when an egress rule exists.
#[tokio::test]
async fn networkpolicy_defaults_on_create_update_and_patch() {
    let api = TestApiServer::new();
    let body = policy("defaults");
    let created = create(&api, POLICY, &body).await;
    let check = |out: &Value| {
        assert_eq!(
            out["spec"]["policyTypes"],
            json!(["Ingress", "Egress"]),
            "{out}"
        );
        assert_eq!(
            out["spec"]["ingress"][0]["ports"][0]["protocol"], "TCP",
            "{out}"
        );
        assert_eq!(
            out["spec"]["egress"][0]["ports"][0]["protocol"], "TCP",
            "{out}"
        );
    };
    check(&created);
    let uri = format!("{POLICY}/defaults");
    let (status, out) = api.put(&uri, &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    check(&out);
    let (status, out) = api
        .patch(
            &uri,
            &json!({"spec": {
                "policyTypes": null, "ingress": [{"ports": [{"port": 81}]}],
                "egress": [{"ports": [{"port": 54}]}]
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    check(&out);

    let mut ingress_only = policy("ingress-only");
    ingress_only["spec"]["egress"] = json!([]);
    let out = create(&api, POLICY, &ingress_only).await;
    assert_eq!(out["spec"]["policyTypes"], json!(["Ingress"]), "{out}");
}

/// ingress/strategy.go:71-84 resets status on create and preserves stored
/// status on ordinary PUT/PATCH; strategy_test.go:73-106 covers preparation.
#[tokio::test]
async fn ingress_create_and_main_updates_cannot_write_status() {
    let api = TestApiServer::new();
    let mut body = ingress("protected");
    body["status"] = json!({"loadBalancer": {"ingress": [{"ip": "192.0.2.1"}]}});
    let created = create(&api, ING, &body).await;
    assert!(
        created["status"]["loadBalancer"]["ingress"]
            .as_array()
            .is_none_or(Vec::is_empty),
        "{created}"
    );
    let uri = format!("{ING}/protected");
    let (status, out) = api
        .patch(
            &format!("{uri}/status"),
            &json!({
                "status": {"loadBalancer": {"ingress": [{"ip": "192.0.2.2"}]}}
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let expected_status = out["status"].clone();
    let (status, out) = api.put(&uri, &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["status"], expected_status, "{out}");
    let (status, out) = api.patch(&uri, &json!({"status": body["status"]})).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["status"], expected_status, "{out}");
}

/// ingress/strategy.go:132-134,162-171; strategy_test.go:109-138. Status
/// inherits AllowUnconditionalUpdate, restores spec, and validates status.
#[tokio::test]
async fn ingress_status_unconditional_put_and_patch_preserve_spec() {
    let api = TestApiServer::new();
    let created = create(&api, ING, &ingress("status")).await;
    let uri = format!("{ING}/status/status");
    let mut update = created.clone();
    update["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("resourceVersion");
    update["spec"] = json!({});
    update["status"] = json!({"loadBalancer": {"ingress": [{"ip": "192.0.2.5"}]}});
    let (status, out) = api.put(&uri, &update).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"], created["spec"], "{out}");
    assert_eq!(
        out["metadata"]["generation"], created["metadata"]["generation"],
        "{out}"
    );
    assert_eq!(out["status"], update["status"], "{out}");
    let (status, out) = api
        .patch(
            &uri,
            &json!({
                "spec": {"defaultBackend": {"service": {"name": "ignored"}}},
                "status": {"loadBalancer": {"ingress": [{"hostname": "lb.example.com"}]}}
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"], created["spec"], "{out}");
    assert_eq!(
        out["status"]["loadBalancer"]["ingress"][0]["hostname"], "lb.example.com",
        "{out}"
    );
}

/// ValidateIngressStatusUpdate / ValidateIngressLoadBalancerStatus,
/// pkg/apis/networking/validation/validation.go:382-420.
#[tokio::test]
async fn ingress_status_rejects_invalid_ip_and_hostname_without_persisting() {
    let api = TestApiServer::new();
    let created = create(&api, ING, &ingress("invalid-status")).await;
    let uri = format!("{ING}/invalid-status/status");
    let mut invalid = created.clone();
    invalid["status"] = json!({"loadBalancer": {"ingress": [{"ip": "invalid-ip"}]}});
    let (status, out) = api.put(&uri, &invalid).await;
    assert_invalid(status, &out, "status.loadBalancer.ingress[0].ip");
    let (status, out) = api
        .patch(
            &uri,
            &json!({
                "status": {"loadBalancer": {"ingress": [{"hostname": "192.0.2.1"}]}}
            }),
        )
        .await;
    assert_invalid(status, &out, "status.loadBalancer.ingress[0].hostname");
    let (status, stored) = api.get(&format!("{ING}/invalid-status")).await;
    assert_eq!(status, StatusCode::OK, "{stored}");
    assert_eq!(stored["status"], created["status"], "{stored}");
    assert_eq!(
        stored["metadata"]["resourceVersion"], created["metadata"]["resourceVersion"],
        "{stored}"
    );
}

/// Update validation runs for patches too: ingress/strategy.go:122-124,
/// ingressclass/strategy.go:89-94, networkpolicy/strategy.go:86-91.
#[tokio::test]
async fn networking_patch_validates_spec_and_preserves_rejected_objects() {
    let api = TestApiServer::new();
    for (collection, body, patch, field) in [
        (
            ING,
            ingress("invalid-patch"),
            json!({"spec": {"defaultBackend": {"service": {"port": {"number": 0}}}}}),
            "spec.defaultBackend.service.port",
        ),
        (
            CLASS,
            ingress_class("invalid-patch"),
            json!({"spec": {"controller": "example.com/other"}}),
            "spec.controller",
        ),
        (
            POLICY,
            policy("invalid-patch"),
            json!({"spec": {"ingress": [{"ports": [{"protocol": "ICMP", "port": 80}]}]}}),
            "spec.ingress[0].ports[0].protocol",
        ),
    ] {
        let created = create(&api, collection, &body).await;
        let uri = format!("{collection}/invalid-patch");
        let (status, out) = api.patch(&uri, &patch).await;
        assert_invalid(status, &out, field);
        let (status, out) = api.get(&uri).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        assert_eq!(out["spec"], created["spec"], "{out}");
        assert_eq!(
            out["metadata"]["resourceVersion"], created["metadata"]["resourceVersion"],
            "{out}"
        );
    }
}

/// WarningsOnCreate, ingress/strategy.go:102-109; update warnings are empty
/// (:127-129). The deprecated annotation only warns without ingressClassName.
#[tokio::test]
async fn ingress_deprecated_class_annotation_warns_only_on_create_without_class_name() {
    let api = TestApiServer::new();
    for (name, class_name, warns) in [("legacy", None, true), ("modern", Some("nginx"), false)] {
        let mut body = ingress(name);
        body["metadata"]["annotations"] = json!({"kubernetes.io/ingress.class": "nginx"});
        if let Some(class_name) = class_name {
            body["spec"]["ingressClassName"] = json!(class_name);
        }
        let (status, headers, _, out) = api
            .send_full(
                "POST",
                ING,
                Some("application/json"),
                None,
                Some(serde_json::to_vec(&body).unwrap()),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{out}");
        let has_warning = headers.get_all("warning").iter().any(|v| {
            v.to_str()
                .unwrap_or_default()
                .contains("annotation \"kubernetes.io/ingress.class\" is deprecated")
        });
        assert_eq!(has_warning, warns, "{headers:?}");
        let (status, headers, _, out) = api
            .send_full(
                "PUT",
                &format!("{ING}/{name}"),
                Some("application/json"),
                None,
                Some(serde_json::to_vec(&body).unwrap()),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{out}");
        assert!(
            !headers
                .get_all("warning")
                .iter()
                .any(|v| v.to_str().unwrap_or_default().contains("is deprecated")),
            "{headers:?}"
        );
    }
}

/// AllowCreateOnUpdate is false for all three strategies and status storage
/// forces it false (ingress/storage/storage.go:106-110).
#[tokio::test]
async fn networking_put_does_not_create_missing_objects() {
    let api = TestApiServer::new();
    for (collection, body) in resources("missing") {
        let uri = format!("{collection}/missing");
        let (status, out) = api.put(&uri, &body).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{collection}: {out}");
        assert_eq!(api.get(&uri).await.0, StatusCode::NOT_FOUND);
    }
    let (status, out) = api
        .put(&format!("{ING}/missing/status"), &ingress("missing"))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
}

/// All three AllowUnconditionalUpdate methods return true; generic
/// registry/store.go:733-738 still rejects stale explicit resourceVersions.
#[tokio::test]
async fn networking_unconditional_put_preserves_uid_and_stale_put_conflicts() {
    let api = TestApiServer::new();
    for (collection, mut body) in resources("versions") {
        let created = create(&api, collection, &body).await;
        body["metadata"]["labels"] = json!({"updated": "yes"});
        let uri = format!("{collection}/versions");
        let (status, out) = api.put(&uri, &body).await;
        assert_eq!(status, StatusCode::OK, "{collection}: {out}");
        assert_eq!(out["metadata"]["uid"], created["metadata"]["uid"], "{out}");
        assert_ne!(
            out["metadata"]["resourceVersion"], created["metadata"]["resourceVersion"],
            "{out}"
        );
        let (status, rejected) = api.put(&uri, &created).await;
        assert_eq!(status, StatusCode::CONFLICT, "{collection}: {rejected}");
    }
}

/// generic registry/store.go:1146-1149 passes both delete preconditions;
/// :1200-1207 respects dry-run, :1386-1411 builds a successful Status.
#[tokio::test]
async fn networking_delete_preconditions_and_dry_run_preserve_objects() {
    let api = TestApiServer::new();
    for (collection, body) in resources("delete") {
        let created = create(&api, collection, &body).await;
        let uri = format!("{collection}/delete");
        for precondition in [
            json!({"uid": "different-uid"}),
            json!({"resourceVersion": "999999999"}),
        ] {
            let (status, out) = api
                .send(
                    "DELETE",
                    &uri,
                    Some("application/json"),
                    Some(&json!({"preconditions": precondition})),
                )
                .await;
            assert_eq!(status, StatusCode::CONFLICT, "{collection}: {out}");
            assert_eq!(
                api.get(&uri).await.1["metadata"]["resourceVersion"],
                created["metadata"]["resourceVersion"]
            );
        }
        for (dry_uri, options) in [
            (format!("{uri}?dryRun=All"), json!({})),
            (uri.clone(), json!({"dryRun": ["All"]})),
        ] {
            let (status, out) = api
                .send("DELETE", &dry_uri, Some("application/json"), Some(&options))
                .await;
            assert_eq!(status, StatusCode::OK, "{collection}: {out}");
            assert_eq!(api.get(&uri).await.0, StatusCode::OK);
        }
        let (status, out) = api.send("DELETE", &uri, Some("application/json"), Some(&json!({"preconditions": {"uid": created["metadata"]["uid"], "resourceVersion": created["metadata"]["resourceVersion"]}}))).await;
        assert_eq!(status, StatusCode::OK, "{collection}: {out}");
        assert_eq!(out["kind"], "Status", "{out}");
        assert_eq!(out["details"]["uid"], created["metadata"]["uid"], "{out}");
        assert_eq!(api.get(&uri).await.0, StatusCode::NOT_FOUND);
    }
}

/// generic registry/store.go:1044-1104 retains finalizers; Update deletes
/// an object once its last finalizer is removed (ShouldDeleteDuringUpdate).
#[tokio::test]
async fn networking_finalizers_delay_deletion_until_patch_removes_them() {
    let api = TestApiServer::new();
    for (collection, mut body) in resources("finalized") {
        body["metadata"]["finalizers"] = json!(["example.com/cleanup"]);
        create(&api, collection, &body).await;
        let uri = format!("{collection}/finalized");
        let (status, out) = api.delete(&uri).await;
        assert_eq!(status, StatusCode::OK, "{collection}: {out}");
        let (status, out) = api.get(&uri).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        assert!(out["metadata"]["deletionTimestamp"].is_string(), "{out}");
        assert_eq!(
            out["metadata"]["finalizers"],
            json!(["example.com/cleanup"]),
            "{out}"
        );
        let (status, out) = api
            .patch(&uri, &json!({"metadata": {"finalizers": []}}))
            .await;
        assert_eq!(status, StatusCode::OK, "{out}");
        assert_eq!(api.get(&uri).await.0, StatusCode::NOT_FOUND);
    }
}

/// DeleteCollection lists by selectors then calls Delete for each item,
/// forwarding options (generic registry/store.go:1237-1384, especially :1281).
#[tokio::test]
async fn networking_collection_delete_respects_selectors_dry_run_and_finalizers() {
    let api = TestApiServer::new();
    for (collection, template) in resources("unused") {
        for (name, team, finalizers) in [
            ("remove", "a", json!([])),
            ("retain", "a", json!(["example.com/cleanup"])),
            ("other", "b", json!([])),
        ] {
            let mut body = template.clone();
            body["metadata"] =
                json!({"name": name, "labels": {"team": team}, "finalizers": finalizers});
            create(&api, collection, &body).await;
        }
        let (status, out) = api
            .delete(&format!("{collection}?labelSelector=team%3Da&dryRun=All"))
            .await;
        assert_eq!(status, StatusCode::OK, "{collection}: {out}");
        assert_eq!(
            api.get(&format!("{collection}/remove")).await.0,
            StatusCode::OK
        );
        assert!(
            api.get(&format!("{collection}/retain")).await.1["metadata"]["deletionTimestamp"]
                .is_null()
        );
        let (status, out) = api
            .delete(&format!("{collection}?labelSelector=team%3Da"))
            .await;
        assert_eq!(status, StatusCode::OK, "{collection}: {out}");
        assert_eq!(out["items"].as_array().unwrap().len(), 2, "{out}");
        assert_eq!(
            api.get(&format!("{collection}/remove")).await.0,
            StatusCode::NOT_FOUND
        );
        let (status, retained) = api.get(&format!("{collection}/retain")).await;
        assert_eq!(status, StatusCode::OK, "{retained}");
        assert!(
            retained["metadata"]["deletionTimestamp"].is_string(),
            "{retained}"
        );
        assert_eq!(
            api.get(&format!("{collection}/other")).await.0,
            StatusCode::OK
        );
    }
}

/// Retain list/watch behavior from ingress/storage/storage_test.go:215-246
/// and networkpolicy/storage/storage_test.go:153-183 during the migration.
#[tokio::test]
async fn networking_list_and_watch_keep_name_and_label_selectors() {
    let api = TestApiServer::new();
    for (collection, mut body) in resources("selected") {
        body["metadata"]["labels"] = json!({"team": "a"});
        create(&api, collection, &body).await;
        body["metadata"] = json!({"name": "excluded", "labels": {"team": "b"}});
        create(&api, collection, &body).await;
        let query = "labelSelector=team%3Da&fieldSelector=metadata.name%3Dselected";
        let (status, list) = api.get(&format!("{collection}?{query}")).await;
        assert_eq!(status, StatusCode::OK, "{list}");
        assert_eq!(list["items"].as_array().unwrap().len(), 1, "{list}");
        assert_eq!(list["items"][0]["metadata"]["name"], "selected", "{list}");
        let response = api
            .respond(
                "GET",
                &format!("{collection}?watch=true&resourceVersion=0&{query}"),
                None,
                None,
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let mut stream = response.into_body().into_data_stream();
        let event = tokio::time::timeout(Duration::from_secs(3), async {
            let mut buffer = Vec::new();
            while let Some(chunk) = stream.next().await {
                buffer.extend_from_slice(&chunk.unwrap());
                if let Some(end) = buffer.iter().position(|b| *b == b'\n') {
                    return serde_json::from_slice::<Value>(&buffer[..end]).unwrap();
                }
            }
            panic!("watch closed before an event for {collection}");
        })
        .await
        .expect("watch must replay the selected object");
        assert_eq!(event["type"], "ADDED", "{event}");
        assert_eq!(event["object"]["metadata"]["name"], "selected", "{event}");
    }
}
