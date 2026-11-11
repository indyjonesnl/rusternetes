//! Ported from `pkg/controller/certificates/rootcacertpublisher/
//! publisher_test.go` (release-1.35): `TestConfigMapCreation` (event handler
//! table) and `TestConfigMapUpdateNoHotLoop`. The controller runs against
//! `MemoryStorage` through its reflectors, so these drive the real
//! list+watch -> store -> queue -> sync path.
use k3rs_client::reflector::StoreEvent;
use k3rs_common::resources::Namespace;
use k3rs_common::types::Phase;
use k3rs_controller_manager::controllers::root_ca_cert_publisher::*;
use k3rs_storage::memory::MemoryStorage;
use k3rs_storage::{build_key, Storage};
use serde_json::{json, Value};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

const CA: &str = "-----BEGIN CERTIFICATE-----\nroot\n-----END CERTIFICATE-----\n";
type P = RootCACertPublisher<MemoryStorage>;

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

fn cm_obj(namespace: &str, name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": { "name": name, "namespace": namespace },
        "data": { "ca.crt": CA },
    })
}

/// Poll until `f` yields `Some`, failing after 10s.
async fn eventually<T, F, Fut>(what: &str, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    for _ in 0..500 {
        if let Some(v) = f().await {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for: {what}");
}

/// Start the controller (informers + one worker) over `s`.
fn start(s: &Arc<MemoryStorage>) -> Arc<P> {
    let p = Arc::new(RootCACertPublisher::new(s.clone(), CA));
    tokio::spawn(p.clone().run());
    p
}

#[test]
fn config_map_handlers_only_enqueue_for_the_root_ca_configmap() {
    let ca = cm_obj("default", ROOT_CA_CERT_CONFIG_MAP_NAME);
    let other = cm_obj("default", "other");
    // "delete ca configmap" / "update ca configmap ..." enqueue the namespace.
    assert_eq!(
        P::on_config_map_event(&StoreEvent::Deleted(ca.clone())),
        Enqueue::Namespace("default".into())
    );
    assert_eq!(
        P::on_config_map_event(&StoreEvent::Modified(ca.clone())),
        Enqueue::Namespace("default".into())
    );
    // "delete other configmap" / "update with other configmap" do nothing.
    assert_eq!(
        P::on_config_map_event(&StoreEvent::Deleted(other.clone())),
        Enqueue::Ignore
    );
    assert_eq!(
        P::on_config_map_event(&StoreEvent::Modified(other)),
        Enqueue::Ignore
    );
    // The ConfigMap informer has no AddFunc.
    assert_eq!(
        P::on_config_map_event(&StoreEvent::Added(ca)),
        Enqueue::Ignore
    );
}

#[test]
fn namespace_handlers_enqueue_added_and_active_updates_only() {
    // "create new namespace": Added is unconditional (namespaceAdded).
    assert_eq!(
        P::on_namespace_event(&StoreEvent::Added(ns("new", Phase::Active))),
        Enqueue::Namespace("new".into())
    );
    assert_eq!(
        P::on_namespace_event(&StoreEvent::Added(ns("new", Phase::Terminating))),
        Enqueue::Namespace("new".into())
    );
    assert_eq!(
        P::on_namespace_event(&StoreEvent::Modified(ns("new", Phase::Active))),
        Enqueue::Namespace("new".into())
    );
    // "update namespace with terminating state"
    assert_eq!(
        P::on_namespace_event(&StoreEvent::Modified(ns("new", Phase::Terminating))),
        Enqueue::Ignore
    );
    assert_eq!(
        P::on_namespace_event(&StoreEvent::Deleted(ns("new", Phase::Active))),
        Enqueue::Ignore
    );
}

#[tokio::test]
async fn creates_the_configmap_for_new_and_existing_namespaces() {
    let s = Arc::new(MemoryStorage::new());
    put_ns(&s, "existing", Phase::Active).await;
    let _p = start(&s);
    // The informer's initial list is processed like Add events.
    let c = eventually("existing ns gets the configmap", || async {
        cm(&s, "existing").await
    })
    .await;
    assert_eq!(c["data"], json!({ "ca.crt": CA }));
    assert_eq!(
        c["metadata"]["annotations"][DESCRIPTION_ANNOTATION],
        DESCRIPTION
    );
    // "create new namespace"
    put_ns(&s, "new", Phase::Active).await;
    eventually("new ns gets the configmap", || async {
        cm(&s, "new").await
    })
    .await;
}

#[tokio::test]
async fn deleted_configmap_is_recreated() {
    let s = Arc::new(MemoryStorage::new());
    put_ns(&s, "default", Phase::Active).await;
    let _p = start(&s);
    eventually("created", || async { cm(&s, "default").await }).await;
    s.delete(&build_key(
        "configmaps",
        Some("default"),
        ROOT_CA_CERT_CONFIG_MAP_NAME,
    ))
    .await
    .unwrap();
    eventually("recreated after delete", || async {
        cm(&s, "default").await
    })
    .await;
}

/// service_accounts.go:806-832: blanking `ca.crt` is reconciled back, and
/// extra keys are removed (`addFieldCM` / `modifyFieldCM`).
#[tokio::test]
async fn modified_configmap_is_reconciled() {
    let s = Arc::new(MemoryStorage::new());
    put_ns(&s, "default", Phase::Active).await;
    let _p = start(&s);
    let key = build_key("configmaps", Some("default"), ROOT_CA_CERT_CONFIG_MAP_NAME);
    eventually("created", || async { cm(&s, "default").await }).await;

    let mut c: Value = s.get(&key).await.unwrap();
    c["data"] = json!({ "ca.crt": "" });
    s.update(&key, &c).await.unwrap();
    eventually("blanked ca.crt restored", || async {
        let c = cm(&s, "default").await?;
        (c["data"] == json!({ "ca.crt": CA })).then_some(())
    })
    .await;

    let mut c: Value = s.get(&key).await.unwrap();
    c["data"]["extra"] = json!("x");
    s.update(&key, &c).await.unwrap();
    eventually("extra key removed", || async {
        let c = cm(&s, "default").await?;
        (c["data"] == json!({ "ca.crt": CA })).then_some(())
    })
    .await;
}

/// `TestConfigMapUpdateNoHotLoop`.
#[tokio::test]
async fn annotation_is_added_once_and_a_present_annotation_is_never_rewritten() {
    let s = Arc::new(MemoryStorage::new());
    put_ns(&s, "no-annotation", Phase::Active).await;
    put_ns(&s, "equal", Phase::Active).await;
    put_ns(&s, "different", Phase::Active).await;
    let mk = |ns: &str, annotations: Option<Value>| {
        let mut m = cm_obj(ns, ROOT_CA_CERT_CONFIG_MAP_NAME);
        if let Some(a) = annotations {
            m["metadata"]["annotations"] = a;
        }
        m
    };
    for (ns, a) in [
        ("no-annotation", None),
        (
            "equal",
            Some(json!({ DESCRIPTION_ANNOTATION: DESCRIPTION })),
        ),
        (
            "different",
            Some(json!({ DESCRIPTION_ANNOTATION: "different" })),
        ),
    ] {
        s.create(
            &build_key("configmaps", Some(ns), ROOT_CA_CERT_CONFIG_MAP_NAME),
            &mk(ns, a),
        )
        .await
        .unwrap();
    }
    let before_equal = cm(&s, "equal").await.unwrap();
    let before_different = cm(&s, "different").await.unwrap();
    let _p = start(&s);

    // No annotation: updated, annotation set, data untouched.
    let c = eventually("annotation added", || async {
        let c = cm(&s, "no-annotation").await?;
        c["metadata"]["annotations"][DESCRIPTION_ANNOTATION]
            .is_string()
            .then_some(c)
    })
    .await;
    assert_eq!(
        c["metadata"]["annotations"][DESCRIPTION_ANNOTATION],
        DESCRIPTION
    );
    assert_eq!(c["data"]["ca.crt"], CA);

    // Give the controller time to (wrongly) touch the others.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(cm(&s, "equal").await.unwrap(), before_equal);
    assert_eq!(cm(&s, "different").await.unwrap(), before_different);
}

/// Upstream swallows the create error for a missing / terminating namespace
/// (`NotFound` / `NamespaceTerminatingCause`) instead of retrying.
#[tokio::test]
async fn no_configmap_for_a_terminating_namespace() {
    let s = Arc::new(MemoryStorage::new());
    put_ns(&s, "dying", Phase::Terminating).await;
    put_ns(&s, "alive", Phase::Active).await;
    let _p = start(&s);
    eventually("active ns processed", || async { cm(&s, "alive").await }).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(cm(&s, "dying").await.is_none());
}
