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
            "spec.defaultBackend",
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

/// SetDefaults_NetworkPolicyPort defaults nil, not an explicit empty string;
/// SetDefaults_NetworkPolicy defaults an empty policyTypes slice too
/// (pkg/apis/networking/v1/defaults.go:30-45).
#[tokio::test]
async fn networkpolicy_null_protocol_defaults_but_empty_protocol_is_invalid() {
    let api = TestApiServer::new();
    let mut body = policy("nullable");
    body["spec"]["policyTypes"] = json!([]);
    body["spec"]["ingress"][0]["ports"][0]["protocol"] = Value::Null;
    let created = create(&api, POLICY, &body).await;
    assert_eq!(created["spec"]["policyTypes"], json!(["Ingress", "Egress"]));
    assert_eq!(created["spec"]["ingress"][0]["ports"][0]["protocol"], "TCP");
    let uri = format!("{POLICY}/nullable");
    let (status, out) = api
        .patch(
            &uri,
            &json!({"spec": {
                "policyTypes": [], "ingress": [{"ports": [{"protocol": null, "port": 81}]}]
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["policyTypes"], json!(["Ingress", "Egress"]));
    assert_eq!(out["spec"]["ingress"][0]["ports"][0]["protocol"], "TCP");

    body["metadata"]["name"] = json!("empty-protocol");
    body["spec"]["ingress"][0]["ports"][0]["protocol"] = json!("");
    let (status, out) = api.post(POLICY, &body).await;
    assert_invalid(status, &out, "spec.ingress[0].ports[0].protocol");
    let (status, out) = api
        .patch(
            &uri,
            &json!({"spec": {
                "ingress": [{"ports": [{"protocol": "", "port": 81}]}]
            }}),
        )
        .await;
    assert_invalid(status, &out, "spec.ingress[0].ports[0].protocol");
    let (status, out) = api.get(&uri).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["ingress"][0]["ports"][0]["protocol"], "TCP");
}

/// NetworkPolicy uses reflect.DeepEqual (networkpolicy/strategy.go:60),
/// while Ingress uses Semantic.DeepEqual (ingress/strategy.go:89). Only the
/// latter treats nil and empty slices as equal.
#[tokio::test]
async fn networking_generation_respects_resource_specific_slice_equality() {
    let api = TestApiServer::new();
    let mut body = policy("slice-equality");
    body["spec"].as_object_mut().unwrap().remove("ingress");
    let created = create(&api, POLICY, &body).await;
    assert_eq!(created["metadata"]["generation"], 1);
    let (status, out) = api
        .patch(
            &format!("{POLICY}/slice-equality"),
            &json!({"spec": {"ingress": []}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        out["metadata"]["generation"], 2,
        "nil -> empty changes NetworkPolicy spec: {out}"
    );

    let created = create(&api, ING, &ingress("slice-equality")).await;
    assert_eq!(created["metadata"]["generation"], 1);
    let (status, out) = api
        .patch(
            &format!("{ING}/slice-equality"),
            &json!({"spec": {"rules": [], "tls": []}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        out["metadata"]["generation"], 1,
        "nil -> empty is semantically equal for Ingress: {out}"
    );
}

async fn send_with_warnings(
    api: &TestApiServer,
    method: &str,
    uri: &str,
    body: &Value,
) -> (StatusCode, Vec<String>, Value) {
    let content_type = if method == "PATCH" {
        "application/merge-patch+json"
    } else {
        "application/json"
    };
    let (status, headers, _, out) = api
        .send_full(
            method,
            uri,
            Some(content_type),
            None,
            Some(serde_json::to_vec(body).unwrap()),
        )
        .await;
    let warnings = headers
        .get_all("warning")
        .iter()
        .map(|value| value.to_str().unwrap().to_owned())
        .collect();
    (status, warnings, out)
}

/// networkPolicyWarnings visits ingress and egress on create and update
/// (networkpolicy/strategy.go:73-74,94-95,103-129). Warning kinds come from
/// apimachinery/pkg/util/validation/ip.go:GetWarningsForCIDR:211-257.
#[tokio::test]
async fn networkpolicy_nonstandard_cidrs_warn_on_create_and_update() {
    let api = TestApiServer::new();
    for (name, cidr, warning_kind) in [
        (
            "leading-zero",
            "010.000.000.000/8",
            "non-standard CIDR value",
        ),
        ("mapped", "::ffff:10.0.0.0/104", "non-standard CIDR value"),
        ("host-bits", "10.0.0.1/8", "is ambiguous in this context"),
    ] {
        let mut body = policy(name);
        body["spec"]["ingress"] = json!([{"from": [{"ipBlock": {"cidr": cidr}}]}]);
        body["spec"]["egress"] = json!([{"to": [{"ipBlock": {"cidr": cidr}}]}]);
        let (status, warnings, out) = send_with_warnings(&api, "POST", POLICY, &body).await;
        assert_eq!(status, StatusCode::CREATED, "{out}");
        for field in [
            "spec.ingress[0].from[0].ipBlock.cidr:",
            "spec.egress[0].to[0].ipBlock.cidr:",
        ] {
            assert!(
                warnings
                    .iter()
                    .any(|w| w.contains(field) && w.contains(warning_kind)),
                "{warnings:?}"
            );
        }
        let (status, warnings, out) =
            send_with_warnings(&api, "PUT", &format!("{POLICY}/{name}"), &body).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("spec.egress[0].to[0].ipBlock.cidr:")
                    && w.contains(warning_kind)),
            "{warnings:?}"
        );
    }
}

/// ingress/strategy.go:175-186 passes each IP's entry path to warnings;
/// strategy_test.go:141-151 checks a leading-zero IP. Warning text comes
/// from apimachinery/pkg/util/validation/ip.go:GetWarningsForIP:105-131.
#[tokio::test]
async fn ingress_status_nonstandard_ip_warns_for_put_and_patch() {
    let api = TestApiServer::new();
    let created = create(&api, ING, &ingress("ip-warning")).await;
    let uri = format!("{ING}/ip-warning/status");
    for (method, ip) in [("PUT", "192.000.002.001"), ("PATCH", "::ffff:192.0.2.1")] {
        let mut body = created.clone();
        body["metadata"]
            .as_object_mut()
            .unwrap()
            .remove("resourceVersion");
        body["status"] = json!({"loadBalancer": {"ingress": [{"ip": ip}]}});
        let (status, warnings, out) = send_with_warnings(&api, method, &uri, &body).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        assert_eq!(out["status"]["loadBalancer"]["ingress"][0]["ip"], ip);
        assert!(
            warnings.iter().any(|w| w
                .contains("status.loadBalancer.ingress[0]: non-standard IP address")
                && w.contains("192.0.2.1")),
            "{warnings:?}"
        );
    }
}

/// Annotation/class consistency is create-only: networking/validation/
/// validation.go:300-314 rejects mismatch, ValidateIngressUpdate:318-327
/// deliberately calls validateIngress without that extra create check.
#[tokio::test]
async fn ingress_class_annotation_mismatch_is_rejected_only_on_create() {
    let api = TestApiServer::new();
    let mut body = ingress("class-mismatch");
    body["metadata"]["annotations"] = json!({"kubernetes.io/ingress.class": "legacy"});
    body["spec"]["ingressClassName"] = json!("modern");
    let (status, out) = api.post(ING, &body).await;
    assert_invalid(status, &out, "annotations[kubernetes.io/ingress.class]");

    body["spec"]["ingressClassName"] = json!("legacy");
    create(&api, ING, &body).await;
    let uri = format!("{ING}/class-mismatch");
    body["spec"]["ingressClassName"] = json!("modern");
    let (status, out) = api.put(&uri, &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["ingressClassName"], "modern");
    let (status, out) = api
        .patch(
            &uri,
            &json!({"metadata": {"annotations": {"kubernetes.io/ingress.class": "other"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        out["metadata"]["annotations"]["kubernetes.io/ingress.class"],
        "other"
    );
}

/// Ingress PrepareForUpdate compares internal Go specs semantically
/// (ingress/strategy.go:80-91). Host, secretName and service port name are
/// non-pointer strings (pkg/apis/networking/types.go), so omitted and empty
/// are the same zero value and must not trigger a generation bump.
#[tokio::test]
async fn ingress_explicit_empty_scalar_strings_do_not_bump_generation() {
    let api = TestApiServer::new();
    let mut body = ingress("empty-strings");
    body["spec"]["rules"] = json!([{"http": {"paths": [{
        "path": "/", "pathType": "Prefix",
        "backend": {"service": {"name": "web", "port": {"number": 80}}}
    }]}}]);
    body["spec"]["tls"] = json!([{}]);
    let created = create(&api, ING, &body).await;
    assert_eq!(created["metadata"]["generation"], 1);
    body["spec"]["rules"][0]["host"] = json!("");
    body["spec"]["tls"][0]["secretName"] = json!("");
    body["spec"]["defaultBackend"]["service"]["port"]["name"] = json!("");
    let (status, out) = api.put(&format!("{ING}/empty-strings"), &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        out["metadata"]["generation"], 1,
        "Go zero strings do not change the spec: {out}"
    );
}

/// Go JSON null leaves non-pointer values at zero. NetworkPolicy's zero spec
/// is valid (networking/validation/validation.go:137-185); Ingress zero paths,
/// backend and service name reach field validation (:457-465,507-550).
#[tokio::test]
async fn networking_json_null_uses_go_zero_values_before_validation() {
    let api = TestApiServer::new();
    for (name, spec) in [
        ("null-spec", Value::Null),
        ("null-selector", json!({"podSelector": null})),
    ] {
        let mut body = policy(name);
        body["spec"] = spec;
        let out = create(&api, POLICY, &body).await;
        assert_eq!(out["spec"]["policyTypes"], json!(["Ingress"]), "{out}");
    }
    for (name, http, field) in [
        (
            "null-paths",
            json!({"paths": null}),
            "spec.rules[0].http.paths",
        ),
        (
            "null-backend",
            json!({"paths": [{"path": "/", "pathType": "Prefix", "backend": null}]}),
            "spec.rules[0].http.paths[0].backend",
        ),
        (
            "null-service-name",
            json!({"paths": [{"path": "/", "pathType": "Prefix", "backend": {"service": {"name": null, "port": {"number": 80}}}}]}),
            "spec.rules[0].http.paths[0].backend.service.name",
        ),
    ] {
        let mut body = ingress(name);
        body["spec"]["rules"] = json!([{"http": http}]);
        let (status, out) = api.post(ING, &body).await;
        assert_invalid(status, &out, field);
    }
    // IngressPortStatus uses value fields, without a networking/v1 defaulter;
    // ValidateIngressLoadBalancerStatus (:389-420) validates IP/hostname only.
    create(&api, ING, &ingress("null-status-port")).await;
    for port in [json!({"port": 80}), json!({"port": null, "protocol": null})] {
        let expected_port = port["port"].as_i64().unwrap_or(0);
        let (status, out) = api
            .patch(
                &format!("{ING}/null-status-port/status"),
                &json!({
                    "status": {"loadBalancer": {"ingress": [{"ip": "192.0.2.1", "ports": [port]}]}}
                }),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{out}");
        let port = &out["status"]["loadBalancer"]["ingress"][0]["ports"][0];
        assert_eq!(port["protocol"], "", "{out}");
        assert_eq!(port["port"], expected_port, "{out}");
    }
}

/// Semantic.DeepEqual preserves nil versus non-nil STRUCT pointers, while
/// equating nil/empty slices (ingress/strategy.go:89; apimachinery/pkg/api/equality).
#[test]
fn ingress_generation_preserves_http_pointer_presence() {
    use rusternetes_api_server::registry::{
        networking::ingress::Strategy,
        rest::{RequestContext, RestUpdateStrategy},
    };
    use rusternetes_common::resources::Ingress;
    let mut body = ingress("http-pointer");
    body["metadata"]["generation"] = json!(1);
    body["spec"]["rules"] = json!([{"host":"*.example.com", "http":{"paths":[]}}]);
    let old: Ingress = serde_json::from_value(body.clone()).unwrap();
    body["spec"]["rules"][0]
        .as_object_mut()
        .unwrap()
        .remove("http");
    let mut new: Ingress = serde_json::from_value(body).unwrap();
    Strategy.prepare_for_update(&RequestContext::new(Some("default")), &mut new, &old);
    assert_eq!(new.metadata.generation, Some(2));
}
