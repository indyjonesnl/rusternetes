//! Ported from `pkg/controller/certificates/rootcacertpublisher/
//! publisher_test.go` (release-1.35): `TestConfigMapCreation` (event handler
//! table) and `TestConfigMapUpdateNoHotLoop`.
use rusternetes_common::resources::Namespace;
use rusternetes_common::types::Phase;
use rusternetes_controller_manager::controllers::root_ca_cert_publisher::*;
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_storage::{build_key, Storage, WatchEvent};
use serde_json::{json, Value};
use std::sync::Arc;

const CA: &str = "-----BEGIN CERTIFICATE-----\nroot\n-----END CERTIFICATE-----\n";

fn ns(name: &str, phase: Phase) -> Namespace {
    let mut n = Namespace::new(name);
    n.status.get_or_insert_with(Default::default).phase = Some(phase);
    n
}

async fn put_ns(s: &MemoryStorage, name: &str, phase: Phase) {
    s.create(&build_key("namespaces", None, name), &ns(name, phase))
        .await
        .unwrap();
}

async fn cm(s: &MemoryStorage, namespace: &str) -> Option<Value> {
    s.get(&build_key(
        "configmaps",
        Some(namespace),
        ROOT_CA_CERT_CONFIG_MAP_NAME,
    ))
    .await
    .ok()
}

async fn drain(p: &RootCACertPublisher<MemoryStorage>) {
    while p.queue().len().await > 0 {
        assert!(p.process_next_work_item().await);
    }
}

fn cm_key(ns: &str, name: &str) -> String {
    format!("/registry/configmaps/{ns}/{name}")
}

#[test]
fn config_map_handlers_only_enqueue_for_the_root_ca_configmap() {
    let ca = cm_key("default", ROOT_CA_CERT_CONFIG_MAP_NAME);
    let other = cm_key("default", "other");
    let ev = |f: fn(String, String) -> WatchEvent, k: &str| f(k.to_string(), "{}".to_string());
    // "delete ca configmap" / "update ca configmap ..." enqueue the namespace.
    assert_eq!(
        RootCACertPublisher::<MemoryStorage>::on_config_map_event(&ev(WatchEvent::Deleted, &ca)),
        Enqueue::Namespace("default".into())
    );
    assert_eq!(
        RootCACertPublisher::<MemoryStorage>::on_config_map_event(&ev(WatchEvent::Modified, &ca)),
        Enqueue::Namespace("default".into())
    );
    // "delete other configmap" / "update with other configmap" do nothing.
    assert_eq!(
        RootCACertPublisher::<MemoryStorage>::on_config_map_event(&ev(WatchEvent::Deleted, &other)),
        Enqueue::Ignore
    );
    assert_eq!(
        RootCACertPublisher::<MemoryStorage>::on_config_map_event(&ev(
            WatchEvent::Modified,
            &other
        )),
        Enqueue::Ignore
    );
    // The ConfigMap informer has no AddFunc.
    assert_eq!(
        RootCACertPublisher::<MemoryStorage>::on_config_map_event(&ev(WatchEvent::Added, &ca)),
        Enqueue::Ignore
    );
}

#[test]
fn namespace_handlers_enqueue_added_and_active_updates_only() {
    let active = serde_json::to_string(&ns("new", Phase::Active)).unwrap();
    let terminating = serde_json::to_string(&ns("new", Phase::Terminating)).unwrap();
    let key = "/registry/namespaces/new".to_string();
    type P = RootCACertPublisher<MemoryStorage>;
    // "create new namespace"
    assert_eq!(
        P::on_namespace_event(&WatchEvent::Added(key.clone(), active.clone())),
        Enqueue::Namespace("new".into())
    );
    // Added is unconditional (namespaceAdded).
    assert_eq!(
        P::on_namespace_event(&WatchEvent::Added(key.clone(), terminating.clone())),
        Enqueue::Namespace("new".into())
    );
    assert_eq!(
        P::on_namespace_event(&WatchEvent::Modified(key.clone(), active)),
        Enqueue::Namespace("new".into())
    );
    // "update namespace with terminating state"
    assert_eq!(
        P::on_namespace_event(&WatchEvent::Modified(key.clone(), terminating)),
        Enqueue::Ignore
    );
    assert_eq!(
        P::on_namespace_event(&WatchEvent::Deleted(key, "{}".into())),
        Enqueue::Ignore
    );
}

#[tokio::test]
async fn creates_the_configmap_with_ca_and_description_annotation() {
    let s = Arc::new(MemoryStorage::new());
    put_ns(&s, "new", Phase::Active).await;
    let p = RootCACertPublisher::new(s.clone(), CA);
    p.sync_namespace("new").await.unwrap();
    let c = cm(&s, "new").await.expect("created");
    assert_eq!(c["data"], json!({ "ca.crt": CA }));
    assert_eq!(
        c["metadata"]["annotations"][DESCRIPTION_ANNOTATION],
        DESCRIPTION
    );
}

#[tokio::test]
async fn deleted_configmap_is_recreated_through_the_queue() {
    let s = Arc::new(MemoryStorage::new());
    put_ns(&s, "default", Phase::Active).await;
    let p = Arc::new(RootCACertPublisher::new(s.clone(), CA));
    p.sync_namespace("default").await.unwrap();
    s.delete(&build_key(
        "configmaps",
        Some("default"),
        ROOT_CA_CERT_CONFIG_MAP_NAME,
    ))
    .await
    .unwrap();
    let ev = WatchEvent::Deleted(cm_key("default", ROOT_CA_CERT_CONFIG_MAP_NAME), "{}".into());
    match RootCACertPublisher::<MemoryStorage>::on_config_map_event(&ev) {
        Enqueue::Namespace(n) => p.queue().add(n).await,
        Enqueue::Ignore => panic!("must enqueue"),
    }
    drain(&p).await;
    assert!(cm(&s, "default").await.is_some());
}

/// service_accounts.go:806-832: blanking `ca.crt` is reconciled back.
#[tokio::test]
async fn modified_configmap_is_reconciled() {
    let s = Arc::new(MemoryStorage::new());
    put_ns(&s, "default", Phase::Active).await;
    let p = RootCACertPublisher::new(s.clone(), CA);
    p.sync_namespace("default").await.unwrap();
    let key = build_key("configmaps", Some("default"), ROOT_CA_CERT_CONFIG_MAP_NAME);
    let mut c: Value = s.get(&key).await.unwrap();
    c["data"] = json!({ "ca.crt": "" });
    s.update(&key, &c).await.unwrap();
    p.sync_namespace("default").await.unwrap();
    assert_eq!(
        cm(&s, "default").await.unwrap()["data"],
        json!({ "ca.crt": CA })
    );
    // addFieldCM: extra data keys are removed.
    let mut c: Value = s.get(&key).await.unwrap();
    c["data"]["extra"] = json!("x");
    s.update(&key, &c).await.unwrap();
    p.sync_namespace("default").await.unwrap();
    assert_eq!(
        cm(&s, "default").await.unwrap()["data"],
        json!({ "ca.crt": CA })
    );
}

/// `TestConfigMapUpdateNoHotLoop`.
#[tokio::test]
async fn annotation_only_updates_once_and_never_hot_loops() {
    let s = Arc::new(MemoryStorage::new());
    put_ns(&s, "default", Phase::Active).await;
    let key = build_key("configmaps", Some("default"), ROOT_CA_CERT_CONFIG_MAP_NAME);
    let base = |annotations: Option<Value>| {
        let mut m = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": { "name": ROOT_CA_CERT_CONFIG_MAP_NAME, "namespace": "default" },
            "data": { "ca.crt": CA },
        });
        if let Some(a) = annotations {
            m["metadata"]["annotations"] = a;
        }
        m
    };
    let p = RootCACertPublisher::new(s.clone(), CA);

    // No annotation: updated, annotation set, data untouched.
    s.create(&key, &base(None)).await.unwrap();
    p.sync_namespace("default").await.unwrap();
    let c: Value = s.get(&key).await.unwrap();
    assert_eq!(
        c["metadata"]["annotations"][DESCRIPTION_ANNOTATION],
        DESCRIPTION
    );
    assert_eq!(c["data"]["ca.crt"], CA);

    // Annotation present (equal or different): no write.
    for annotations in [
        json!({DESCRIPTION_ANNOTATION: DESCRIPTION}),
        json!({DESCRIPTION_ANNOTATION: "different"}),
    ] {
        s.delete(&key).await.unwrap();
        s.create(&key, &base(Some(annotations.clone())))
            .await
            .unwrap();
        let before: Value = s.get(&key).await.unwrap();
        p.sync_namespace("default").await.unwrap();
        let after: Value = s.get(&key).await.unwrap();
        assert_eq!(before, after, "must not rewrite when annotation is present");
    }
}

/// Upstream swallows the create error for a missing / terminating namespace
/// (`NotFound` / `NamespaceTerminatingCause`) instead of retrying.
#[tokio::test]
async fn no_configmap_for_missing_or_terminating_namespace() {
    let s = Arc::new(MemoryStorage::new());
    put_ns(&s, "dying", Phase::Terminating).await;
    let p = RootCACertPublisher::new(s.clone(), CA);
    p.sync_namespace("dying").await.unwrap();
    p.sync_namespace("gone").await.unwrap();
    assert!(cm(&s, "dying").await.is_none());
    assert!(cm(&s, "gone").await.is_none());
}
