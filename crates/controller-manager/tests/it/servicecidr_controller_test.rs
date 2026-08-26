//! Integration tests for the ServiceCIDR controller.
//!
//! Ports the behaviours of upstream's `service-cidr-controller`
//! (`pkg/controller/servicecidrs/servicecidrs_controller.go`): a protection
//! finalizer on every live range, `Ready=True` while healthy, and — the
//! load-bearing half — `Ready=False` with reason `Terminating` plus a retained
//! object whenever an IPAddress still references a range being deleted.
//!
//! Regression target: #1747 — before this controller existed, a `DELETE` on a
//! ServiceCIDR removed it immediately, whatever held IPs from it.

use rusternetes_common::resources::{
    IPAddress, ParentReference, ServiceCIDR, ServiceCIDRCondition, ServiceCIDRStatus,
};
use rusternetes_controller_manager::controllers::servicecidr::{
    ServiceCIDRController, IP_ALLOCATOR_CONTROLLER_NAME, LABEL_IP_ADDRESS_FAMILY, LABEL_MANAGED_BY,
    READY_MESSAGE, SERVICE_CIDR_PROTECTION_FINALIZER, TERMINATING_MESSAGE,
};
use rusternetes_storage::{build_key, MemoryStorage, Storage};
use std::sync::Arc;
use std::time::Duration;

fn cidr_key(name: &str) -> String {
    build_key("servicecidrs", None, name)
}

fn service_cidr(name: &str, cidrs: &[&str]) -> ServiceCIDR {
    ServiceCIDR::new(name, cidrs.iter().map(|c| c.to_string()).collect())
}

/// An IPAddress as the kube-apiserver's service allocator writes it: labelled
/// with its family and `managed-by: ipallocator.k8s.io`
/// (`pkg/registry/core/service/ipallocator/ipallocator.go:396-399`).
fn allocator_ip(addr: &str, family: &str) -> IPAddress {
    let mut ip = IPAddress::new(
        addr,
        ParentReference {
            group: None,
            resource: "services".to_string(),
            namespace: Some("default".to_string()),
            name: "svc".to_string(),
            uid: None,
        },
    );
    let labels = ip.metadata.labels.get_or_insert_with(Default::default);
    labels.insert(LABEL_IP_ADDRESS_FAMILY.to_string(), family.to_string());
    labels.insert(
        LABEL_MANAGED_BY.to_string(),
        IP_ALLOCATOR_CONTROLLER_NAME.to_string(),
    );
    ip
}

async fn store_cidr(storage: &MemoryStorage, sc: &ServiceCIDR) {
    storage
        .create(&cidr_key(&sc.metadata.name), sc)
        .await
        .unwrap();
}

async fn store_ip(storage: &MemoryStorage, ip: &IPAddress) {
    storage
        .create(&build_key("ipaddresses", None, &ip.metadata.name), ip)
        .await
        .unwrap();
}

async fn get_cidr(storage: &MemoryStorage, name: &str) -> Option<ServiceCIDR> {
    storage.get::<ServiceCIDR>(&cidr_key(name)).await.ok()
}

fn ready_condition(sc: &ServiceCIDR) -> Option<ServiceCIDRCondition> {
    sc.status
        .as_ref()?
        .conditions
        .as_ref()?
        .iter()
        .find(|c| c.condition_type == "Ready")
        .cloned()
}

fn controller(storage: &Arc<MemoryStorage>) -> ServiceCIDRController<MemoryStorage> {
    ServiceCIDRController::new(Arc::clone(storage))
}

// --- Healthy range: finalizer + Ready=True --------------------------------

#[tokio::test]
async fn adds_protection_finalizer_and_ready_condition() {
    let storage = Arc::new(MemoryStorage::new());
    store_cidr(&storage, &service_cidr("kubernetes", &["10.96.0.0/12"])).await;

    controller(&storage).reconcile_all().await.unwrap();

    let sc = get_cidr(&storage, "kubernetes")
        .await
        .expect("still exists");
    assert_eq!(
        sc.metadata.finalizers.as_deref(),
        Some([SERVICE_CIDR_PROTECTION_FINALIZER.to_string()].as_slice()),
        "every live ServiceCIDR must carry the protection finalizer"
    );

    let cond = ready_condition(&sc).expect("Ready condition set by the controller");
    assert_eq!(cond.status, "True");
    assert_eq!(cond.message, READY_MESSAGE);
    assert_eq!(
        cond.reason, "",
        "upstream applies Ready=True with no reason"
    );
}

#[tokio::test]
async fn condition_is_not_rewritten_when_unchanged() {
    let storage = Arc::new(MemoryStorage::new());
    store_cidr(&storage, &service_cidr("kubernetes", &["10.96.0.0/12"])).await;

    controller(&storage).reconcile_all().await.unwrap();
    let first = ready_condition(&get_cidr(&storage, "kubernetes").await.unwrap()).unwrap();

    controller(&storage).reconcile_all().await.unwrap();
    let second = ready_condition(&get_cidr(&storage, "kubernetes").await.unwrap()).unwrap();

    assert_eq!(
        first.last_transition_time, second.last_transition_time,
        "lastTransitionTime must only move on a real transition"
    );
}

// --- Deletion blocked by a referencing IPAddress --------------------------

#[tokio::test]
async fn deletion_blocked_while_an_ipaddress_references_the_range() {
    let storage = Arc::new(MemoryStorage::new());
    store_cidr(&storage, &service_cidr("kubernetes", &["10.96.0.0/12"])).await;
    store_ip(&storage, &allocator_ip("10.96.0.10", "IPv4")).await;

    // Live pass: finalizer + Ready=True.
    controller(&storage).reconcile_all().await.unwrap();

    // The api-server's DELETE marks it terminating because of the finalizer.
    let mut sc = get_cidr(&storage, "kubernetes").await.unwrap();
    sc.metadata.deletion_timestamp = Some(chrono::Utc::now());
    storage.update(&cidr_key("kubernetes"), &sc).await.unwrap();

    // Grace period is irrelevant here: the block comes first.
    controller(&storage).reconcile_all().await.unwrap();

    let sc = get_cidr(&storage, "kubernetes")
        .await
        .expect("a referenced ServiceCIDR must NOT be removed");
    assert!(
        sc.metadata
            .finalizers
            .as_ref()
            .is_some_and(|f| f.iter().any(|x| x == SERVICE_CIDR_PROTECTION_FINALIZER)),
        "the finalizer must stay while the range is referenced"
    );
    let cond = ready_condition(&sc).expect("Ready condition");
    assert_eq!(cond.status, "False");
    assert_eq!(cond.reason, "Terminating");
    assert_eq!(cond.message, TERMINATING_MESSAGE);
}

#[tokio::test]
async fn deletion_unblocks_once_the_ipaddress_goes_away() {
    let storage = Arc::new(MemoryStorage::new());
    store_cidr(&storage, &service_cidr("kubernetes", &["10.96.0.0/12"])).await;
    store_ip(&storage, &allocator_ip("10.96.0.10", "IPv4")).await;
    controller(&storage).reconcile_all().await.unwrap();

    let mut sc = get_cidr(&storage, "kubernetes").await.unwrap();
    // Deletion started well outside the grace period.
    sc.metadata.deletion_timestamp = Some(chrono::Utc::now() - chrono::Duration::seconds(60));
    storage.update(&cidr_key("kubernetes"), &sc).await.unwrap();
    controller(&storage).reconcile_all().await.unwrap();
    assert!(get_cidr(&storage, "kubernetes").await.is_some());

    storage
        .delete(&build_key("ipaddresses", None, "10.96.0.10"))
        .await
        .unwrap();
    controller(&storage).reconcile_all().await.unwrap();

    assert!(
        get_cidr(&storage, "kubernetes").await.is_none(),
        "with no IPAddress left the finalizer is dropped and the object goes"
    );
}

#[tokio::test]
async fn ipaddress_not_managed_by_the_service_allocator_does_not_block() {
    let storage = Arc::new(MemoryStorage::new());
    store_cidr(&storage, &service_cidr("kubernetes", &["10.96.0.0/12"])).await;

    // Same address, but managed by something other than the service allocator.
    let mut ip = allocator_ip("10.96.0.10", "IPv4");
    ip.metadata
        .labels
        .as_mut()
        .unwrap()
        .insert(LABEL_MANAGED_BY.to_string(), "some-other-controller".into());
    store_ip(&storage, &ip).await;

    controller(&storage).reconcile_all().await.unwrap();
    let mut sc = get_cidr(&storage, "kubernetes").await.unwrap();
    sc.metadata.deletion_timestamp = Some(chrono::Utc::now() - chrono::Duration::seconds(60));
    storage.update(&cidr_key("kubernetes"), &sc).await.unwrap();

    controller(&storage).reconcile_all().await.unwrap();
    assert!(
        get_cidr(&storage, "kubernetes").await.is_none(),
        "only ipallocator.k8s.io-managed IPAddresses block a ServiceCIDR delete"
    );
}

#[tokio::test]
async fn a_containing_servicecidr_unblocks_the_delete() {
    let storage = Arc::new(MemoryStorage::new());
    // The narrow range is fully contained by the wide one, so nothing is
    // orphaned by removing it (upstream `canDeleteCIDR`'s `hasParent` path).
    store_cidr(&storage, &service_cidr("wide", &["10.96.0.0/12"])).await;
    store_cidr(&storage, &service_cidr("narrow", &["10.96.0.0/24"])).await;
    store_ip(&storage, &allocator_ip("10.96.0.10", "IPv4")).await;
    controller(&storage).reconcile_all().await.unwrap();

    let mut sc = get_cidr(&storage, "narrow").await.unwrap();
    sc.metadata.deletion_timestamp = Some(chrono::Utc::now() - chrono::Duration::seconds(60));
    storage.update(&cidr_key("narrow"), &sc).await.unwrap();

    controller(&storage).reconcile_all().await.unwrap();
    assert!(
        get_cidr(&storage, "narrow").await.is_none(),
        "a range whose IPs stay covered by a parent range is safe to delete"
    );
    assert!(get_cidr(&storage, "wide").await.is_some());
}

// --- Deletion grace period -------------------------------------------------

#[tokio::test]
async fn finalizer_is_held_for_the_deletion_grace_period() {
    let storage = Arc::new(MemoryStorage::new());
    store_cidr(&storage, &service_cidr("kubernetes", &["10.96.0.0/12"])).await;
    controller(&storage).reconcile_all().await.unwrap();

    // Nothing references the range, but the deletion just happened: the
    // allocators have not had time to observe it yet.
    let mut sc = get_cidr(&storage, "kubernetes").await.unwrap();
    sc.metadata.deletion_timestamp = Some(chrono::Utc::now());
    storage.update(&cidr_key("kubernetes"), &sc).await.unwrap();

    controller(&storage).reconcile_all().await.unwrap();
    assert!(
        get_cidr(&storage, "kubernetes").await.is_some(),
        "the finalizer must survive the grace period even with no IPs"
    );

    // Same object once the window has passed.
    let mut sc = get_cidr(&storage, "kubernetes").await.unwrap();
    sc.metadata.deletion_timestamp = Some(chrono::Utc::now() - chrono::Duration::seconds(30));
    storage.update(&cidr_key("kubernetes"), &sc).await.unwrap();

    controller(&storage).reconcile_all().await.unwrap();
    assert!(get_cidr(&storage, "kubernetes").await.is_none());
}

#[tokio::test]
async fn a_shortened_grace_period_lets_the_delete_through() {
    let storage = Arc::new(MemoryStorage::new());
    store_cidr(&storage, &service_cidr("kubernetes", &["10.96.0.0/12"])).await;
    let c = ServiceCIDRController::new(Arc::clone(&storage))
        .with_deletion_grace_period(Duration::from_millis(1));
    c.reconcile_all().await.unwrap();

    let mut sc = get_cidr(&storage, "kubernetes").await.unwrap();
    sc.metadata.deletion_timestamp = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
    storage.update(&cidr_key("kubernetes"), &sc).await.unwrap();

    c.reconcile_all().await.unwrap();
    assert!(get_cidr(&storage, "kubernetes").await.is_none());
}

// --- Foreign finalizers ----------------------------------------------------

#[tokio::test]
async fn other_finalizers_still_hold_the_object() {
    let storage = Arc::new(MemoryStorage::new());
    let mut sc = service_cidr("kubernetes", &["10.96.0.0/12"]);
    sc.metadata.finalizers = Some(vec!["example.com/keep".to_string()]);
    store_cidr(&storage, &sc).await;
    controller(&storage).reconcile_all().await.unwrap();

    let mut sc = get_cidr(&storage, "kubernetes").await.unwrap();
    sc.metadata.deletion_timestamp = Some(chrono::Utc::now() - chrono::Duration::seconds(60));
    storage.update(&cidr_key("kubernetes"), &sc).await.unwrap();

    controller(&storage).reconcile_all().await.unwrap();

    let sc = get_cidr(&storage, "kubernetes")
        .await
        .expect("a foreign finalizer still blocks removal");
    assert_eq!(
        sc.metadata.finalizers.as_deref(),
        Some(["example.com/keep".to_string()].as_slice()),
        "only our own protection finalizer is dropped"
    );
}

// --- Status shape ----------------------------------------------------------

#[tokio::test]
async fn reconcile_preserves_spec_when_writing_status() {
    let storage = Arc::new(MemoryStorage::new());
    let mut sc = service_cidr("kubernetes", &["10.96.0.0/12"]);
    sc.status = Some(ServiceCIDRStatus { conditions: None });
    store_cidr(&storage, &sc).await;

    controller(&storage).reconcile_all().await.unwrap();

    let sc = get_cidr(&storage, "kubernetes").await.unwrap();
    assert_eq!(sc.spec.as_ref().unwrap().cidrs, vec!["10.96.0.0/12"]);
    assert_eq!(ready_condition(&sc).unwrap().status, "True");
}
