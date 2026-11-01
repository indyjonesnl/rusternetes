//! DynamicResources Reserve/PreBind/PreBindPreFlight/Unreserve through the
//! plugin object (`dynamicresources.go:919-1066`, `:1068-1144`, `bindClaim`
//! `:1146-1244`), for already allocated claims.
use async_trait::async_trait;
use rusternetes_common::resources::{Pod, ResourceClaim};
use rusternetes_scheduler::fwk::{
    ClaimWriteError, Code, CycleState, Handle, NoHandle, PreBindPlugin, PreFilterPlugin,
    ReservePlugin,
};
use rusternetes_scheduler::in_tree::DynamicResources;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::Mutex;

/// A claim store whose writes can be scripted to fail.
struct Claims {
    claim: Mutex<ResourceClaim>,
    /// Results of the next writes; a write without one succeeds.
    script: Mutex<VecDeque<ClaimWriteError>>,
    writes: Mutex<usize>,
}

impl Claims {
    fn new(claim: ResourceClaim) -> Self {
        Self {
            claim: Mutex::new(claim),
            script: Default::default(),
            writes: Mutex::new(0),
        }
    }
    fn failing(self, errs: Vec<ClaimWriteError>) -> Self {
        *self.script.lock().unwrap() = errs.into();
        self
    }
    fn writes(&self) -> usize {
        *self.writes.lock().unwrap()
    }
    fn reserved_for(&self) -> Vec<String> {
        self.claim
            .lock()
            .unwrap()
            .status
            .as_ref()
            .map(|s| s.reserved_for.iter().map(|r| r.uid.clone()).collect())
            .unwrap_or_default()
    }
}

#[async_trait]
impl Handle for Claims {
    async fn resource_claim(&self, _ns: &str, _name: &str) -> Result<ResourceClaim, String> {
        Ok(self.claim.lock().unwrap().clone())
    }
    async fn update_resource_claim_status(
        &self,
        claim: &ResourceClaim,
    ) -> Result<ResourceClaim, ClaimWriteError> {
        *self.writes.lock().unwrap() += 1;
        if let Some(e) = self.script.lock().unwrap().pop_front() {
            return Err(e);
        }
        *self.claim.lock().unwrap() = claim.clone();
        Ok(claim.clone())
    }
}

fn pod(uid: &str) -> Pod {
    serde_json::from_value(json!({
        "metadata": {"name": uid, "namespace": "default", "uid": uid},
        "spec": {"containers": [{"name": "c"}],
                 "resourceClaims": [{"name": "r", "resourceClaimName": "c"}]}
    }))
    .unwrap()
}

fn allocated(reserved: &[&str]) -> ResourceClaim {
    let reserved: Vec<Value> = reserved
        .iter()
        .map(|u| json!({"resource": "pods", "name": u, "uid": u}))
        .collect();
    serde_json::from_value(json!({
        "metadata": {"name": "c", "namespace": "default"},
        "spec": {"devices": {}},
        "status": {"allocation": {"devices": {}}, "reservedFor": reserved}
    }))
    .unwrap()
}

/// PreFilter has resolved the pod's claims.
async fn state(h: &Claims, p: &Pod) -> CycleState {
    let mut state = CycleState::new();
    let (_, s) = DynamicResources
        .pre_filter(&mut state, p, &[], &[], h)
        .await;
    assert!(s.is_success(), "{s:?}");
    state
}

#[tokio::test]
async fn pre_bind_records_the_pod_in_reserved_for() {
    let h = Claims::new(allocated(&["other"]));
    let p = pod("p1");
    let st = state(&h, &p).await;
    assert!(DynamicResources
        .reserve(&st, &p, "n1", &h)
        .await
        .is_success());
    assert!(DynamicResources
        .pre_bind_pre_flight(&st, &p, "n1")
        .is_success());
    let s = DynamicResources.pre_bind(&st, &p, "n1", &h).await;
    assert!(s.is_success(), "{s:?}");
    assert_eq!(h.reserved_for(), ["other", "p1"]);
    assert_eq!(h.writes(), 1);
}

#[tokio::test]
async fn pre_bind_skips_a_claim_already_reserved_for_the_pod() {
    let h = Claims::new(allocated(&["p1"]));
    let p = pod("p1");
    let st = state(&h, &p).await;
    assert!(DynamicResources
        .pre_bind(&st, &p, "n1", &h)
        .await
        .is_success());
    assert_eq!(h.writes(), 0);
    assert_eq!(h.reserved_for(), ["p1"]);
}

#[tokio::test]
async fn pre_bind_retries_a_conflict_with_the_latest_claim() {
    let h = Claims::new(allocated(&[])).failing(vec![ClaimWriteError::Conflict("rv".to_string())]);
    let p = pod("p1");
    let st = state(&h, &p).await;
    // Someone else reserved the claim while the first write was in flight;
    // the retry builds on the latest claim.
    *h.claim.lock().unwrap() = allocated(&["racer"]);
    assert!(DynamicResources
        .pre_bind(&st, &p, "n1", &h)
        .await
        .is_success());
    assert_eq!(h.writes(), 2);
    assert_eq!(h.reserved_for(), ["racer", "p1"]);
}

#[tokio::test]
async fn pre_bind_errors_are_wrapped_and_a_deleted_claim_is_refused() {
    let h = Claims::new(allocated(&[])).failing(vec![ClaimWriteError::Other("boom".to_string())]);
    let p = pod("p1");
    let st = state(&h, &p).await;
    let s = DynamicResources.pre_bind(&st, &p, "n1", &h).await;
    assert_eq!(s.code(), Code::Error);
    assert_eq!(s.message(), "add reservation to claim default/c: boom");

    let h = Claims::new(allocated(&[])).failing(vec![ClaimWriteError::Conflict("rv".to_string())]);
    let st = state(&h, &p).await;
    h.claim.lock().unwrap().metadata.deletion_timestamp = Some(chrono::Utc::now());
    let s = DynamicResources.pre_bind(&st, &p, "n1", &h).await;
    assert_eq!(s.message(), "claim default/c got deleted in the meantime");
}

#[tokio::test]
async fn pre_flight_skips_without_claims() {
    let p = pod("p1");
    // No PreFilter state: the pod has no claims, PreFilter skipped.
    assert!(DynamicResources
        .pre_bind_pre_flight(&CycleState::new(), &p, "n1")
        .is_skip());
    // PreBind and Unreserve are no-ops without state.
    assert!(DynamicResources
        .pre_bind(&CycleState::new(), &p, "n1", &NoHandle)
        .await
        .is_success());
    DynamicResources
        .unreserve(&CycleState::new(), &p, "n1", &NoHandle)
        .await;
}

#[tokio::test]
async fn unreserve_removes_only_this_pod_after_a_failed_binding_cycle() {
    let h = Claims::new(allocated(&["other"]));
    let p = pod("p1");
    let st = state(&h, &p).await;
    assert!(DynamicResources
        .pre_bind(&st, &p, "n1", &h)
        .await
        .is_success());
    assert_eq!(h.reserved_for(), ["other", "p1"]);
    DynamicResources.unreserve(&st, &p, "n1", &h).await;
    assert_eq!(h.reserved_for(), ["other"]);
    // Idempotent (the entry is already gone): the result does not change.
    DynamicResources.unreserve(&st, &p, "n1", &h).await;
    assert_eq!(h.reserved_for(), ["other"]);
}

#[tokio::test]
async fn unreserve_leaves_a_claim_that_never_listed_the_pod() {
    let h = Claims::new(allocated(&["other"]));
    let p = pod("p1");
    let st = state(&h, &p).await;
    DynamicResources.unreserve(&st, &p, "n1", &h).await;
    assert_eq!(h.writes(), 0);
    assert_eq!(h.reserved_for(), ["other"]);
}

#[tokio::test]
async fn unreserve_retries_a_conflict() {
    let h = Claims::new(allocated(&["p1", "other"]));
    let p = pod("p1");
    let st = state(&h, &p).await;
    *h.script.lock().unwrap() = vec![ClaimWriteError::Conflict("rv".to_string())].into();
    DynamicResources.unreserve(&st, &p, "n1", &h).await;
    assert_eq!(h.reserved_for(), ["other"]);
    assert_eq!(h.writes(), 2);
}
