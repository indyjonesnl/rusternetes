//! Pod `/eviction` on the generic Store (#1990 step 6, #2145): upstream's
//! `EvictionREST` (`pkg/registry/core/pod/storage/eviction.go`, release-1.35),
//! ported as [`EvictionRest`] over the pod status store and a PodDisruptionBudget
//! client. These are the ports of its own tests,
//! `pkg/registry/core/pod/storage/eviction_test.go`: `TestEviction` (scripted
//! `mockStore`), `TestEvictionWithETCD`, `TestEvictionWithDeleteOptions`,
//! `TestEvictionPDBStatus` and `TestAddConditionAndDelete`, plus the HTTP
//! surface (a 201 `Status`, the 429 `Retry-After`, a concurrent burst) and
//! #1802 (`unhealthyPodEvictionPolicy` was ignored).
//!
//! The budget the eviction consults is the PDB **status** the disruption
//! controller maintains (#2149), not a count of pods: every test that needs a
//! budget seeds it, as upstream's tests do.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rusternetes_api_server::registry::core::pod::{
    new_eviction_rest, EvictionPodStore, EvictionRest, PdbClient, StoreEvictionRest,
};
use rusternetes_api_server::registry::core::pod::{new_status_store, Backoff};
use rusternetes_api_server::registry::rest::{RequestContext, UpdatedObjectInfo};
use rusternetes_common::deletion::{DeleteOptions, Preconditions};
use rusternetes_common::resources::{Eviction, Pod, PodDisruptionBudget};
use rusternetes_common::types::Phase;
use rusternetes_common::validation::metav1::CreateOptions;
use rusternetes_common::{Error, Status};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage, StorageBackend};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// A retry schedule with upstream's attempt count and no pauses.
fn no_pause() -> Backoff {
    Backoff {
        steps: 20,
        duration: Duration::ZERO,
        factor: 1.0,
        jitter: 0.0,
    }
}

fn ctx() -> RequestContext {
    RequestContext::new(Some("default")).with_group_version("", "v1")
}

/// `metav1.NewDeleteOptions(grace)`.
fn delete_options(grace: i64) -> DeleteOptions {
    DeleteOptions {
        grace_period_seconds: Some(grace),
        ..rusternetes_api_server::registry::rest::zero_delete_options()
    }
}

/// `metav1.NewRVDeletionPrecondition`.
fn rv_precondition(rv: &str) -> DeleteOptions {
    DeleteOptions {
        preconditions: Some(Preconditions {
            uid: None,
            resource_version: Some(rv.to_string()),
        }),
        ..rusternetes_api_server::registry::rest::zero_delete_options()
    }
}

fn eviction(name: &str, options: Option<DeleteOptions>) -> Eviction {
    serde_json::from_value::<Eviction>(json!({
        "apiVersion": "policy/v1", "kind": "Eviction",
        "metadata": {"name": name, "namespace": "default"},
    }))
    .map(|mut e| {
        e.delete_options = options;
        e
    })
    .unwrap()
}

/// A PDB as upstream's tests build it: `foo` in `default`, selecting
/// `a=true` (or `selector`), with `status` as given.
fn pdb(selector: Value, policy: Option<&str>, status: Value) -> PodDisruptionBudget {
    let mut spec = json!({ "selector": selector });
    if let Some(policy) = policy {
        spec["unhealthyPodEvictionPolicy"] = json!(policy);
    }
    serde_json::from_value(json!({
        "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
        "metadata": {"name": "foo", "namespace": "default"},
        "spec": spec,
        "status": status,
    }))
    .unwrap()
}

fn match_a() -> Value {
    json!({"matchLabels": {"a": "true"}})
}

/// `err.Error()` plus `: <reason>` plus the causes that are not already its
/// suffix (`errToString`, eviction_test.go).
fn err_to_string(result: &Result<Status, Error>) -> String {
    match result {
        Ok(_) => String::new(),
        Err(Error::Status(status)) => {
            let message = status.message.clone().unwrap_or_default();
            let mut out = format!("{message}: {}", status.reason.clone().unwrap_or_default());
            for cause in status
                .details
                .iter()
                .flat_map(|d| d.causes.iter().flatten())
            {
                let cause_message = cause.message.clone().unwrap_or_default();
                if !message.ends_with(&cause_message) {
                    out.push_str(&format!(": {cause_message}"));
                }
            }
            out
        }
        Err(Error::Conflict(message)) => format!("{message}: Conflict"),
        Err(Error::BadRequest(message)) => format!("{message}: BadRequest"),
        Err(other) => other.to_string(),
    }
}

/// `errors.NewConflict(resource("tests"), "2", errors.New("message"))`.
fn mock_conflict() -> Error {
    Error::Conflict("Operation cannot be fulfilled on tests \"2\": message".to_string())
}

/// `validNewPod` with the fields the tests set.
fn valid_pod(name: &str, phase: Option<&str>, terminating: bool, ready: Option<&str>) -> Pod {
    let mut pod: Pod = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "labels": {"a": "true"}, "uid": "uid-1"},
        "spec": {"nodeName": "foo", "containers": [{"name": "c", "image": "busybox"}]},
        "status": {},
    }))
    .unwrap();
    if let Some(phase) = phase {
        pod.status.as_mut().unwrap().phase = Some(serde_json::from_value(json!(phase)).unwrap());
    }
    if terminating {
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
    }
    if let Some(ready) = ready {
        pod.status.as_mut().unwrap().conditions = Some(vec![serde_json::from_value(
            json!({"type": "Ready", "status": ready}),
        )
        .unwrap()]);
    }
    pod
}

// ---------------------------------------------------------------------------
// the fakes: `mockStore` and `fake.NewSimpleClientset`
// ---------------------------------------------------------------------------

/// `mockStore` (eviction_test.go): scripted `Delete` outcomes keyed on the
/// pod's name.
struct MockStore {
    delete_count: Mutex<usize>,
    pod: Mutex<Pod>,
}

impl MockStore {
    fn mutator_delete_func(&self, count: usize, options: &DeleteOptions) -> Result<(), Error> {
        let mut pod = self.pod.lock().unwrap();
        let name = pod.metadata.name.clone();
        if name == "t4" {
            // Always return error for this pod
            return Err(mock_conflict());
        }
        if name == "t6" || name == "t8" {
            // t6: This pod has a deletionTimestamp and should not raise
            // conflict on delete
            // t8: This pod should not have a resource conflict.
            return Ok(());
        }
        if name == "t10" {
            return Err(Error::BadRequest("test designed to error".to_string()));
        }
        if count == 1 {
            // This is a hack to ensure that some test pods don't change phase
            // but do change resource version
            if name != "t1" && name != "t5" {
                pod.status.as_mut().unwrap().phase = Some(Phase::Running);
            }
            pod.metadata.resource_version = Some("999".to_string());
            // Always return conflict on the first attempt
            return Err(mock_conflict());
        }
        // Compare enforce deletionOptions
        let rv = options
            .preconditions
            .as_ref()
            .and_then(|p| p.resource_version.as_deref());
        match rv {
            None => Ok(()),
            Some("1000") => Ok(()),
            Some(_) => {
                // Here we're simulating that the pod has changed resource
                // version again: pod "t4" should make it here, this validates
                // we're getting the latest resourceVersion of the pod and
                // successfully delete on the next deletion attempt after this
                // one.
                pod.metadata.resource_version = Some("1000".to_string());
                Err(mock_conflict())
            }
        }
    }
}

/// The store handle the rest owns; the test keeps the other `Arc`.
struct SharedMock(Arc<MockStore>);

#[async_trait]
impl EvictionPodStore for SharedMock {
    async fn get(&self, _: &RequestContext, _: &str) -> Result<Pod, Error> {
        Ok(self.0.pod.lock().unwrap().clone())
    }

    async fn update(
        &self,
        _: &RequestContext,
        _: &str,
        _: &dyn UpdatedObjectInfo<Pod>,
    ) -> Result<Pod, Error> {
        Ok(self.0.pod.lock().unwrap().clone())
    }

    async fn delete(&self, _: &RequestContext, _: &str, options: DeleteOptions) -> Result<(), Error> {
        let count = {
            let mut c = self.0.delete_count.lock().unwrap();
            *c += 1;
            *c
        };
        self.0.mutator_delete_func(count, &options)
    }
}

/// `fake.NewSimpleClientset(pdbs...).PolicyV1()`: an object tracker. A write
/// replaces the object, with no resourceVersion bookkeeping.
#[derive(Clone, Default)]
struct FakePdbClient {
    pdbs: Arc<Mutex<Vec<PodDisruptionBudget>>>,
}

impl FakePdbClient {
    fn with(pdbs: Vec<PodDisruptionBudget>) -> Self {
        Self {
            pdbs: Arc::new(Mutex::new(pdbs)),
        }
    }

    fn get_by_name(&self, name: &str) -> PodDisruptionBudget {
        self.pdbs
            .lock()
            .unwrap()
            .iter()
            .find(|p| p.metadata.name == name)
            .cloned()
            .unwrap()
    }
}

#[async_trait]
impl PdbClient for FakePdbClient {
    async fn list(&self, namespace: &str) -> Result<Vec<PodDisruptionBudget>, Error> {
        Ok(self
            .pdbs
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.metadata.namespace.as_deref() == Some(namespace))
            .cloned()
            .collect())
    }

    async fn get(&self, namespace: &str, name: &str) -> Result<PodDisruptionBudget, Error> {
        self.list(namespace)
            .await?
            .into_iter()
            .find(|p| p.metadata.name == name)
            .ok_or_else(|| Error::NotFound(format!("poddisruptionbudgets \"{name}\" not found")))
    }

    async fn update_status(
        &self,
        _namespace: &str,
        pdb: &PodDisruptionBudget,
    ) -> Result<PodDisruptionBudget, Error> {
        let mut pdbs = self.pdbs.lock().unwrap();
        let slot = pdbs
            .iter_mut()
            .find(|p| p.metadata.name == pdb.metadata.name)
            .unwrap();
        *slot = pdb.clone();
        Ok(pdb.clone())
    }
}

// ---------------------------------------------------------------------------
// TestEviction (eviction_test.go:251-645)
// ---------------------------------------------------------------------------

struct EvictionCase {
    name: &'static str,
    pdb_selector: Value,
    pdb_status: Value,
    eviction: Eviction,
    expect_error: &'static str,
    pod_phase: Option<&'static str>,
    pod_name: &'static str,
    expected_delete_count: usize,
    pod_terminating: bool,
    prc: Option<&'static str>,
    /// `None`: every policy; otherwise only these (`nil` is `""`).
    policies: Option<Vec<Option<&'static str>>>,
}

const BUDGET_MSG: &str = "Cannot evict pod as it would violate the pod's disruption budget.: TooManyRequests: ";

fn eviction_cases() -> Vec<EvictionCase> {
    let disruptions = |n: i64| json!({"disruptionsAllowed": n});
    let base = |name, pod_name| EvictionCase {
        name,
        pdb_selector: match_a(),
        pdb_status: disruptions(0),
        eviction: eviction(pod_name, Some(delete_options(0))),
        expect_error: "",
        pod_phase: None,
        pod_name,
        expected_delete_count: 0,
        pod_terminating: false,
        prc: None,
        policies: None,
    };
    let still_processing: &'static str = Box::leak(
        format!("{BUDGET_MSG}The disruption budget foo is still being processed by the server.")
            .into_boxed_str(),
    );
    let needs = |s: &str| -> &'static str {
        Box::leak(format!("{BUDGET_MSG}{s}").into_boxed_str())
    };
    vec![
        EvictionCase {
            pod_phase: Some("Pending"),
            expected_delete_count: 3,
            ..base(
                "pdbs No disruptions allowed, pod pending, first delete conflict, pod still pending, pod deleted successfully",
                "t1",
            )
        },
        // This test case is critical. If it is removed or broken we may
        // regress and allow a pod to be deleted without checking PDBs when
        // the pod should not be deleted.
        EvictionCase {
            expect_error: needs("The disruption budget foo needs 0 healthy pods and has 0 currently"),
            pod_phase: Some("Pending"),
            expected_delete_count: 1,
            // AlwaysAllow does not continueToPDBs, but straight to deletion
            policies: Some(vec![None, Some("IfHealthyBudget")]),
            ..base(
                "pdbs No disruptions allowed, pod pending, first delete conflict, pod becomes running, continueToPDBs",
                "t2",
            )
        },
        EvictionCase {
            expect_error: still_processing,
            pod_phase: Some("Pending"),
            expected_delete_count: 2,
            prc: Some("False"),
            // nil, IfHealthyBudget continueToPDBs
            policies: Some(vec![Some("AlwaysAllow")]),
            ..base(
                "pdbs No disruptions allowed, pod pending, first delete conflict, pod becomes running, skip PDB check, conflict",
                "t2",
            )
        },
        EvictionCase {
            pdb_status: disruptions(1),
            pod_phase: Some("Pending"),
            expected_delete_count: 2,
            // AlwaysAllow does not continueToPDBs, but straight to deletion
            // if pod not healthy (ready)
            policies: Some(vec![None, Some("IfHealthyBudget")]),
            ..base(
                "pdbs disruptions allowed, pod pending, first delete conflict, pod becomes running, continueToPDBs",
                "t3",
            )
        },
        EvictionCase {
            expect_error: "Operation cannot be fulfilled on tests \"2\": message: Conflict",
            pod_phase: Some("Pending"),
            expected_delete_count: 20, // EvictionsRetry.Steps
            ..base("pod pending, always conflict on delete", "t4")
        },
        EvictionCase {
            eviction: eviction("t5", Some(rv_precondition("userProvided"))),
            expect_error: "Operation cannot be fulfilled on tests \"2\": message: Conflict",
            pod_phase: Some("Pending"),
            expected_delete_count: 1,
            ..base(
                "pod pending, always conflict on delete, user provided ResourceVersion constraint",
                "t5",
            )
        },
        EvictionCase {
            eviction: eviction("t6", Some(delete_options(300))),
            expected_delete_count: 1,
            pod_terminating: true,
            ..base("matching pdbs with no disruptions allowed, pod terminating", "t6")
        },
        EvictionCase {
            // This simulates 3 pods expected, our pod healthy, unhealthy pod
            // is not ours.
            pdb_status: json!({"disruptionsAllowed": 0, "currentHealthy": 2, "desiredHealthy": 2}),
            expect_error: needs("The disruption budget foo needs 2 healthy pods and has 2 currently"),
            pod_phase: Some("Running"),
            prc: Some("True"),
            ..base(
                "matching pdbs with no disruptions allowed, pod running, pod healthy, unhealthy pod not ours",
                "t7",
            )
        },
        EvictionCase {
            pdb_status: json!({"disruptionsAllowed": 1, "currentHealthy": 3, "desiredHealthy": 3}),
            expected_delete_count: 1,
            pod_phase: Some("Running"),
            prc: Some("True"),
            ..base(
                "matching pdbs with disruptions allowed, pod running, pod healthy, healthy pod ours, deletes pod by honoring the PDB",
                "t8",
            )
        },
        EvictionCase {
            // This simulates 3 pods expected, our pod unhealthy
            pdb_status: json!({"disruptionsAllowed": 0, "currentHealthy": 2, "desiredHealthy": 2}),
            expected_delete_count: 1,
            pod_phase: Some("Running"),
            prc: Some("False"),
            ..base(
                "matching pdbs with no disruptions allowed, pod running, pod unhealthy, unhealthy pod ours",
                "t8",
            )
        },
        EvictionCase {
            expected_delete_count: 1,
            pod_phase: Some("Running"),
            prc: Some("False"),
            // nil, IfHealthyBudget would not skip the PDB check
            policies: Some(vec![Some("AlwaysAllow")]),
            ..base(
                "matching pdbs with no disruptions allowed, pod running, pod unhealthy, unhealthy pod ours, skips the PDB check and deletes",
                "t8",
            )
        },
        EvictionCase {
            // This case should return the 529 retry error.
            pdb_status: json!({"disruptionsAllowed": 0, "currentHealthy": 2, "desiredHealthy": 2}),
            expect_error: still_processing,
            expected_delete_count: 1,
            pod_phase: Some("Running"),
            prc: Some("False"),
            ..base(
                "matching pdbs with no disruptions allowed, pod running, pod unhealthy, unhealthy pod ours, resource version conflict",
                "t9",
            )
        },
        EvictionCase {
            pdb_status: json!({"disruptionsAllowed": 0, "currentHealthy": 2, "desiredHealthy": 2}),
            expect_error: "test designed to error: BadRequest",
            expected_delete_count: 1,
            pod_phase: Some("Running"),
            prc: Some("False"),
            ..base(
                "matching pdbs with no disruptions allowed, pod running, pod unhealthy, unhealthy pod ours, other error on delete",
                "t10",
            )
        },
        EvictionCase {
            pdb_selector: json!({}),
            pdb_status: json!({"disruptionsAllowed": 0, "currentHealthy": 3, "desiredHealthy": 3}),
            expect_error: needs("The disruption budget foo needs 3 healthy pods and has 3 currently"),
            pod_phase: Some("Running"),
            prc: Some("True"),
            ..base(
                "matching pdbs with no disruptions allowed, pod running, pod healthy, empty selector, pod not deleted by honoring the PDB",
                "t11",
            )
        },
        EvictionCase {
            pdb_selector: json!({}),
            pdb_status: json!({
                "disruptionsAllowed": 0,
                "conditions": [{"type": "DisruptionAllowed", "status": "False",
                                "reason": "SyncFailed", "message": "hoge"}]
            }),
            expect_error: needs("The disruption budget foo does not allow evicting pods currently because it failed sync: hoge"),
            pod_phase: Some("Running"),
            prc: Some("True"),
            ..base(
                "the error is about sync failure when the condition reason is SyncFailedReason",
                "t12",
            )
        },
        EvictionCase {
            pdb_selector: json!({}),
            pdb_status: json!({
                "disruptionsAllowed": 0, "currentHealthy": 4, "desiredHealthy": 3,
                "conditions": [{"type": "DisruptionAllowed", "status": "False",
                                "reason": "bar-reason", "message": "hoge"}]
            }),
            expect_error: needs("The disruption budget foo does not allow evicting pods currently (bar-reason): hoge"),
            pod_phase: Some("Running"),
            prc: Some("True"),
            ..base(
                "the error includes the reason when the condition.Status is False",
                "t12",
            )
        },
    ]
}

#[tokio::test]
async fn test_eviction() {
    for policy in [Some("AlwaysAllow"), None, Some("IfHealthyBudget")] {
        for case in eviction_cases() {
            if let Some(policies) = &case.policies {
                if !policies.contains(&policy) {
                    // unhealthyPodEvictionPolicy is not covered by this test
                    continue;
                }
            }
            let label = format!("{} with {} policy", case.name, policy.unwrap_or("nil"));

            let pod = valid_pod(
                case.pod_name,
                case.pod_phase,
                case.pod_terminating,
                case.prc,
            );
            let store = Arc::new(MockStore {
                delete_count: Mutex::new(0),
                pod: Mutex::new(pod),
            });
            let client = FakePdbClient::with(vec![pdb(
                case.pdb_selector.clone(),
                policy,
                case.pdb_status.clone(),
            )]);
            let rest = {
                let mut rest = EvictionRest::new(SharedMock(store.clone()), client);
                rest.retry = no_pause();
                rest
            };

            let result = rest
                .create(
                    &ctx(),
                    case.pod_name,
                    case.eviction.clone(),
                    None,
                    &CreateOptions::default(),
                )
                .await;
            assert_eq!(err_to_string(&result), case.expect_error, "{label}");
            assert_eq!(
                *store.delete_count.lock().unwrap(),
                case.expected_delete_count,
                "delete count: {label}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// TestEvictionWithETCD (eviction_test.go:48-249): the real pod store
// ---------------------------------------------------------------------------

/// The api-server's pod store and the pods the test creates through its
/// HTTP surface (so they carry the defaults a decoded pod has).
struct StoreFixture {
    api: TestApiServer,
    backend: Arc<StorageBackend>,
}

impl StoreFixture {
    fn new() -> Self {
        let api = TestApiServer::new();
        let backend = Arc::new(StorageBackend::Memory(api.storage.clone()));
        Self { api, backend }
    }

    fn mem(&self) -> &Arc<MemoryStorage> {
        &self.api.storage
    }

    /// Create a pod labelled `a=true` on node `foo`, then move it to `phase`
    /// the way the kubelet would (the status store's `Update`).
    async fn create_pod(&self, name: &str, phase: Option<&str>) {
        let (status, body) = self
            .api
            .post(
                "/api/v1/namespaces/default/pods",
                &json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": name, "labels": {"a": "true"}},
                    "spec": {"nodeName": "foo", "containers": [{"name": "c", "image": "busybox"}]},
                }),
            )
            .await;
        assert_eq!(status.as_u16(), 201, "{body}");
        if let Some(phase) = phase {
            self.set_status(name, |status| {
                status.phase = Some(serde_json::from_value(json!(phase)).unwrap())
            })
            .await;
        }
    }

    async fn set_status(&self, name: &str, f: impl FnOnce(&mut rusternetes_common::resources::PodStatus)) {
        let key = build_key("pods", Some("default"), name);
        let mut pod: Pod = self.mem().get(&key).await.unwrap();
        f(pod.status.get_or_insert_with(Default::default));
        self.mem().update(&key, &pod).await.unwrap();
    }

    async fn pod(&self, name: &str) -> Result<Pod, Error> {
        self.mem()
            .get(&build_key("pods", Some("default"), name))
            .await
    }

    fn rest(&self, client: FakePdbClient) -> EvictionRest<rusternetes_api_server::registry::generic::Store<Pod, StorageBackend>, FakePdbClient> {
        {
            let mut rest = EvictionRest::new(new_status_store(self.backend.clone()), client);
            rest.retry = no_pause();
            rest
        }
    }
}

struct EtcdCase {
    name: &'static str,
    pdb_selector: Value,
    pdb_status: Value,
    pod_phase: Option<&'static str>,
    pod_name: &'static str,
    bad_name_in_url: bool,
    expect_error: &'static str,
    expect_deleted: bool,
    policies: Option<Vec<Option<&'static str>>>,
}

#[tokio::test]
async fn test_eviction_with_etcd() {
    let allowed = |n: i64| json!({"disruptionsAllowed": n});
    let c = |name, pod_name| EtcdCase {
        name,
        pdb_selector: match_a(),
        pdb_status: allowed(0),
        pod_phase: None,
        pod_name,
        bad_name_in_url: false,
        expect_error: "",
        expect_deleted: false,
        policies: None,
    };
    let violation: &'static str = Box::leak(
        format!("{BUDGET_MSG}The disruption budget foo needs 0 healthy pods and has 0 currently")
            .into_boxed_str(),
    );
    let cases = vec![
        EtcdCase {
            expect_error: violation,
            pod_phase: Some("Running"),
            // AlwaysAllow would terminate the pod since Running pods are not
            // guarded by this policy
            policies: Some(vec![None, Some("IfHealthyBudget")]),
            ..c("matching pdbs with no disruptions allowed, pod running", "t1")
        },
        EtcdCase {
            pod_phase: Some("Pending"),
            expect_deleted: true,
            ..c("matching pdbs with no disruptions allowed, pod pending", "t2")
        },
        EtcdCase {
            pod_phase: Some("Succeeded"),
            expect_deleted: true,
            ..c("matching pdbs with no disruptions allowed, pod succeeded", "t3")
        },
        EtcdCase {
            pod_phase: Some("Failed"),
            expect_deleted: true,
            ..c("matching pdbs with no disruptions allowed, pod failed", "t4")
        },
        EtcdCase {
            pdb_status: allowed(1),
            expect_deleted: true,
            ..c("matching pdbs with disruptions allowed", "t5")
        },
        EtcdCase {
            pdb_selector: json!({"matchLabels": {"b": "true"}}),
            expect_deleted: true,
            ..c("non-matching pdbs", "t6")
        },
        EtcdCase {
            pdb_status: allowed(1),
            bad_name_in_url: true,
            expect_error: "name in URL does not match name in Eviction object: BadRequest",
            ..c("matching pdbs with disruptions allowed but bad name in Url", "t7")
        },
        EtcdCase {
            pdb_selector: json!({}),
            expect_error: violation,
            pod_phase: Some("Running"),
            policies: Some(vec![None, Some("IfHealthyBudget")]),
            ..c("matching pdbs with no disruptions allowed, pod running, empty selector", "t8")
        },
    ];

    for policy in [None, Some("IfHealthyBudget"), Some("AlwaysAllow")] {
        for case in &cases {
            if let Some(policies) = &case.policies {
                if !policies.contains(&policy) {
                    continue;
                }
            }
            let label = format!("{} with {} policy", case.name, policy.unwrap_or("nil"));
            let fx = StoreFixture::new();
            fx.create_pod(case.pod_name, case.pod_phase).await;
            let client = FakePdbClient::with(vec![pdb(
                case.pdb_selector.clone(),
                policy,
                case.pdb_status.clone(),
            )]);
            let rest = fx.rest(client);

            let url_name = if case.bad_name_in_url {
                format!("{}bad-name", case.pod_name)
            } else {
                case.pod_name.to_string()
            };
            let result = rest
                .create(
                    &ctx(),
                    &url_name,
                    eviction(case.pod_name, Some(delete_options(0))),
                    None,
                    &CreateOptions::default(),
                )
                .await;
            assert_eq!(err_to_string(&result), case.expect_error, "{label}");
            if !case.expect_error.is_empty() {
                continue;
            }
            let existing = fx.pod(case.pod_name).await;
            if case.expect_deleted {
                assert!(
                    matches!(existing, Err(Error::NotFound(_))),
                    "expected to be deleted, lookup returned {existing:?}: {label}"
                );
            } else {
                // graceful deletion
                let existing = existing.unwrap_or_else(|e| panic!("expected graceful deletion, got {e}: {label}"));
                assert!(existing.metadata.deletion_timestamp.is_some(), "{label}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TestEvictionWithDeleteOptions (eviction_test.go:647-716)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_eviction_with_delete_options() {
    struct Case {
        name: &'static str,
        eviction_options: DeleteOptions,
        request_dry_run: Option<Vec<String>>,
        pdbs: Vec<PodDisruptionBudget>,
        /// `deleteOptions.ignoreStoreReadErrorWithClusterBreakingPotential`
        /// is refused as Invalid.
        invalid: bool,
        /// A dry run leaves the pod where it is.
        dry_run: bool,
    }
    let all = || Some(vec!["All".to_string()]);
    let zero = rusternetes_api_server::registry::rest::zero_delete_options;
    let cases = vec![
        Case {
            name: "dry run - just request-options",
            eviction_options: zero(),
            request_dry_run: all(),
            pdbs: vec![],
            invalid: false,
            dry_run: true,
        },
        Case {
            name: "dry run - just eviction-options",
            eviction_options: DeleteOptions { dry_run: all(), ..zero() },
            request_dry_run: None,
            pdbs: vec![],
            invalid: false,
            dry_run: true,
        },
        Case {
            name: "dry run - both options",
            eviction_options: DeleteOptions { dry_run: all(), ..zero() },
            request_dry_run: all(),
            pdbs: vec![],
            invalid: false,
            dry_run: true,
        },
        Case {
            name: "dry run - with pdbs",
            eviction_options: DeleteOptions { dry_run: all(), ..zero() },
            request_dry_run: all(),
            pdbs: vec![pdb(match_a(), None, json!({"disruptionsAllowed": 1}))],
            invalid: false,
            dry_run: true,
        },
        Case {
            name: "ignoreStoreReadErrorWithClusterBreakingPotential is set, invalid error expected",
            eviction_options: DeleteOptions {
                ignore_store_read_error_with_cluster_breaking_potential: Some(true),
                ..zero()
            },
            request_dry_run: None,
            pdbs: vec![],
            invalid: true,
            dry_run: false,
        },
    ];
    for case in cases {
        let fx = StoreFixture::new();
        fx.create_pod("foo", Some("Running")).await;
        let client = FakePdbClient::with(case.pdbs.clone());
        let rest = fx.rest(client.clone());
        let options = CreateOptions {
            dry_run: case.request_dry_run.clone(),
            ..Default::default()
        };
        let result = rest
            .create(
                &ctx(),
                "foo",
                eviction("foo", Some(case.eviction_options.clone())),
                None,
                &options,
            )
            .await;
        if case.invalid {
            match result {
                Err(Error::Invalid(errs)) => {
                    assert_eq!(errs.len(), 1, "{}", case.name);
                    assert_eq!(
                        errs[0].field,
                        "deleteOptions.ignoreStoreReadErrorWithClusterBreakingPotential",
                        "{}",
                        case.name
                    );
                    assert_eq!(
                        errs[0].detail, "can not be set for pod eviction, try after removing the option",
                        "{}",
                        case.name
                    );
                }
                other => panic!("{}: expected Invalid, got {other:?}", case.name),
            }
            continue;
        }
        assert!(result.is_ok(), "{}: {result:?}", case.name);
        if case.dry_run {
            let pod = fx.pod("foo").await.expect("a dry run keeps the pod");
            assert!(pod.metadata.deletion_timestamp.is_none(), "{}", case.name);
            // ... and does not add the condition, nor touch a budget.
            assert!(
                pod.status
                    .and_then(|s| s.conditions)
                    .unwrap_or_default()
                    .iter()
                    .all(|c| c.condition_type != "DisruptionTarget"),
                "{}",
                case.name
            );
            for budget in &case.pdbs {
                let after = client.get_by_name(&budget.metadata.name);
                assert_eq!(after.status.unwrap().disruptions_allowed, 1, "{}", case.name);
            }
        }
    }
}

/// `propagateDryRun`: contradicting dry-run options are refused
/// (eviction.go:108-126).
#[tokio::test]
async fn non_matching_dry_run_options_are_refused() {
    let fx = StoreFixture::new();
    fx.create_pod("foo", Some("Running")).await;
    let rest = fx.rest(FakePdbClient::default());
    let options = CreateOptions {
        dry_run: Some(vec!["All".to_string()]),
        ..Default::default()
    };
    let result = rest
        .create(
            &ctx(),
            "foo",
            eviction(
                "foo",
                Some(DeleteOptions {
                    dry_run: Some(vec!["Other".to_string()]),
                    ..rusternetes_api_server::registry::rest::zero_delete_options()
                }),
            ),
            None,
            &options,
        )
        .await;
    match result {
        Err(Error::Internal(message)) => assert!(
            message.starts_with("Non-matching dry-run options in request and content"),
            "{message}"
        ),
        other => panic!("expected the dry-run mismatch error, got {other:?}"),
    }
    assert!(fx.pod("foo").await.is_ok());
}

// ---------------------------------------------------------------------------
// TestEvictionPDBStatus (eviction_test.go:718-813)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_eviction_pdb_status() {
    for (name, allowed, expected_allowed, expected_reason) in [
        ("pdb status is updated after eviction", 1, 0, "InsufficientPods"),
        (
            "condition reason is only updated if AllowedDisruptions becomes 0",
            3,
            2,
            "SufficientPods",
        ),
    ] {
        let fx = StoreFixture::new();
        let client = FakePdbClient::with(vec![pdb(
            match_a(),
            None,
            json!({
                "disruptionsAllowed": allowed,
                "conditions": [{"type": "DisruptionAllowed", "status": "True",
                                "reason": "SufficientPods"}]
            }),
        )]);
        for pod_name in ["foo-1", "foo-2"] {
            fx.create_pod(pod_name, Some("Running")).await;
            // `Status.Phase = Running` with no Ready condition: unhealthy, so
            // make them ready for the budget to guard them.
            fx.set_status(pod_name, |s| {
                s.conditions = Some(vec![serde_json::from_value(
                    json!({"type": "Ready", "status": "True"}),
                )
                .unwrap()])
            })
            .await;
        }
        let rest = fx.rest(client.clone());
        rest.create(
            &ctx(),
            "foo-1",
            eviction("foo-1", Some(zero_options())),
            None,
            &CreateOptions::default(),
        )
        .await
        .unwrap_or_else(|e| panic!("{name}: failed to run eviction: {e}"));

        let existing = client.get_by_name("foo");
        let status = existing.status.unwrap();
        assert_eq!(status.disruptions_allowed, expected_allowed, "{name}");
        let condition = status
            .conditions
            .unwrap()
            .into_iter()
            .find(|c| c.condition_type == "DisruptionAllowed")
            .unwrap();
        assert_eq!(condition.reason.as_deref(), Some(expected_reason), "{name}");
        // The pod is recorded as disrupted, for the controller to see.
        assert!(
            status.disrupted_pods.unwrap().contains_key("foo-1"),
            "{name}"
        );
    }
}

fn zero_options() -> DeleteOptions {
    rusternetes_api_server::registry::rest::zero_delete_options()
}

// ---------------------------------------------------------------------------
// TestAddConditionAndDelete (eviction_test.go:815-935)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_add_condition_and_delete() {
    type MakeOptions = fn(&Pod) -> DeleteOptions;
    let cases: Vec<(&str, bool, MakeOptions, &str)> = vec![
        ("simple", true, |_| zero_options(), ""),
        ("missing", false, |_| zero_options(), "not found"),
        (
            "valid uid",
            true,
            |pod| DeleteOptions {
                preconditions: Some(Preconditions {
                    uid: Some(pod.metadata.uid.clone()),
                    resource_version: None,
                }),
                ..zero_options()
            },
            "",
        ),
        (
            "invalid uid",
            true,
            |pod| DeleteOptions {
                preconditions: Some(Preconditions {
                    uid: Some(format!("{}1", pod.metadata.uid)),
                    resource_version: None,
                }),
                ..zero_options()
            },
            "The object might have been deleted and then recreated",
        ),
        (
            "valid resourceVersion",
            true,
            |pod| DeleteOptions {
                preconditions: Some(Preconditions {
                    uid: None,
                    resource_version: pod.metadata.resource_version.clone(),
                }),
                ..zero_options()
            },
            "",
        ),
        (
            "invalid resourceVersion",
            true,
            |pod| DeleteOptions {
                preconditions: Some(Preconditions {
                    uid: None,
                    resource_version: Some(format!(
                        "{}1",
                        pod.metadata.resource_version.clone().unwrap()
                    )),
                }),
                ..zero_options()
            },
            "The object might have been modified",
        ),
    ];
    for (name, initial_pod, make_options, expect_err) in cases {
        let fx = StoreFixture::new();
        let rest = fx.rest(FakePdbClient::default());
        let options = if initial_pod {
            fx.create_pod("foo", None).await;
            make_options(&fx.pod("foo").await.unwrap())
        } else {
            make_options(&valid_pod("foo", None, false, None))
        };
        let result = rest.add_condition_and_delete_pod(&ctx(), "foo", &options).await;
        match (result, expect_err) {
            (Ok(()), "") => {}
            (Ok(()), want) => panic!("{name}: expected err containing {want:?}, got none"),
            (Err(e), "") => panic!("{name}: unexpected err: {e}"),
            (Err(e), want) => assert!(e.to_string().contains(want), "{name}: {e}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Behaviour of the store-backed EvictionRest and its HTTP surface
// ---------------------------------------------------------------------------

const PODS: &str = "/api/v1/namespaces/default/pods";

fn pod_body(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "labels": {"app": "web"}},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
    })
}

fn eviction_body(name: &str) -> Value {
    json!({"apiVersion": "policy/v1", "kind": "Eviction",
           "metadata": {"name": name, "namespace": "default"}})
}

/// Mark a stored pod ready (a `Running` pod the budget guards).
async fn make_ready(api: &TestApiServer, name: &str) {
    let key = build_key("pods", Some("default"), name);
    let mut pod: Pod = api.storage.get(&key).await.unwrap();
    let status = pod.status.get_or_insert_with(Default::default);
    status.phase = Some(Phase::Running);
    status.conditions = Some(vec![serde_json::from_value(
        json!({"type": "Ready", "status": "True"}),
    )
    .unwrap()]);
    api.storage.update(&key, &pod).await.unwrap();
}

/// Seed a PDB straight into storage, as an up-to-date disruption controller
/// would have left it.
async fn seed_pdb(api: &TestApiServer, name: &str, spec: Value, status: Value) {
    let pdb: PodDisruptionBudget = serde_json::from_value(json!({
        "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
        "metadata": {"name": name, "namespace": "default"},
        "spec": spec,
        "status": status,
    }))
    .unwrap();
    api.storage
        .create(&build_key("poddisruptionbudgets", Some("default"), name), &pdb)
        .await
        .unwrap();
}

async fn stored_pdb(api: &TestApiServer, name: &str) -> PodDisruptionBudget {
    api.storage
        .get(&build_key("poddisruptionbudgets", Some("default"), name))
        .await
        .unwrap()
}

async fn evict(api: &TestApiServer, name: &str, uri_suffix: &str) -> (u16, Value, axum::http::HeaderMap) {
    let body = serde_json::to_vec(&eviction_body(name)).unwrap();
    let (status, headers, _, value) = api
        .send_with_headers(
            "POST",
            &format!("{PODS}/{name}/eviction{uri_suffix}"),
            &[("content-type", "application/json")],
            Some(body),
        )
        .await;
    (status.as_u16(), value, headers)
}

/// create.go:227-231: the success result is a `Status` with code 201.
#[tokio::test]
async fn a_successful_eviction_is_a_201_status() {
    let api = TestApiServer::new();
    assert_eq!(api.post(PODS, &pod_body("p")).await.0.as_u16(), 201);
    let (code, body, _) = evict(&api, "p", "").await;
    assert_eq!(code, 201, "{body}");
    assert_eq!(body["kind"], "Status", "{body}");
    assert_eq!(body["status"], "Success", "{body}");
    assert_eq!(body["code"], 201, "{body}");
}

/// `checkAndDecrement` (eviction.go:425-427): a budget the controller has not
/// caught up with (`observedGeneration < generation`) is a 429 with
/// `Retry-After: 10` and the `DisruptionBudget` cause.
#[tokio::test]
async fn a_budget_not_yet_observed_is_a_429_with_retry_after() {
    let api = TestApiServer::new();
    assert_eq!(api.post(PODS, &pod_body("p")).await.0.as_u16(), 201);
    make_ready(&api, "p").await;
    // Created through the API, so generation 1 and a zero status: not yet
    // observed by any controller.
    let (s, body) = api
        .post(
            "/apis/policy/v1/namespaces/default/poddisruptionbudgets",
            &json!({"apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
                    "metadata": {"name": "b"},
                    "spec": {"minAvailable": 0, "selector": {"matchLabels": {"app": "web"}}}}),
        )
        .await;
    assert_eq!(s.as_u16(), 201, "{body}");

    let (code, body, headers) = evict(&api, "p", "").await;
    assert_eq!(code, 429, "{body}");
    assert_eq!(headers.get("retry-after").unwrap(), "10");
    assert_eq!(body["reason"], "TooManyRequests", "{body}");
    assert_eq!(
        body["message"],
        "Cannot evict pod as it would violate the pod's disruption budget."
    );
    assert_eq!(body["details"]["retryAfterSeconds"], 10, "{body}");
    assert_eq!(
        body["details"]["causes"][0],
        json!({"reason": "DisruptionBudget",
               "message": "The disruption budget b is still being processed by the server."})
    );
    assert!(api.storage.get::<Pod>(&build_key("pods", Some("default"), "p")).await.is_ok());
}

/// A budget with no disruptions left is a 429 without `Retry-After`
/// (`NewTooManyRequests(msg, 0)`), naming the numbers the controller left in
/// its status.
#[tokio::test]
async fn an_exhausted_budget_is_a_429_naming_its_counts() {
    let api = TestApiServer::new();
    assert_eq!(api.post(PODS, &pod_body("p")).await.0.as_u16(), 201);
    make_ready(&api, "p").await;
    seed_pdb(
        &api,
        "b",
        json!({"minAvailable": 1, "selector": {"matchLabels": {"app": "web"}}}),
        json!({"currentHealthy": 1, "desiredHealthy": 1, "disruptionsAllowed": 0, "expectedPods": 1}),
    )
    .await;
    let (code, body, headers) = evict(&api, "p", "").await;
    assert_eq!(code, 429, "{body}");
    assert!(headers.get("retry-after").is_none(), "{headers:?}");
    assert_eq!(
        body["details"]["causes"][0]["message"],
        "The disruption budget b needs 1 healthy pods and has 1 currently"
    );
}

/// An eviction that fits the budget decrements it, records the pod in
/// `disruptedPods`, and flips `DisruptionAllowed` when it reaches zero.
#[tokio::test]
async fn an_eviction_decrements_the_budget_status() {
    let api = TestApiServer::new();
    for name in ["p1", "p2"] {
        assert_eq!(api.post(PODS, &pod_body(name)).await.0.as_u16(), 201);
        make_ready(&api, name).await;
    }
    seed_pdb(
        &api,
        "b",
        json!({"minAvailable": 1, "selector": {"matchLabels": {"app": "web"}}}),
        json!({"currentHealthy": 2, "desiredHealthy": 1, "disruptionsAllowed": 1, "expectedPods": 2,
               "conditions": [{"type": "DisruptionAllowed", "status": "True", "reason": "SufficientPods"}]}),
    )
    .await;

    let (code, body, _) = evict(&api, "p1", "").await;
    assert_eq!(code, 201, "{body}");
    let after = stored_pdb(&api, "b").await.status.unwrap();
    assert_eq!(after.disruptions_allowed, 0);
    assert!(after.disrupted_pods.unwrap().contains_key("p1"));
    let condition = &after.conditions.unwrap()[0];
    assert_eq!(condition.status, "False");
    assert_eq!(condition.reason.as_deref(), Some("InsufficientPods"));

    // The second is refused: the budget is spent.
    let (code, body, _) = evict(&api, "p2", "").await;
    assert_eq!(code, 429, "{body}");
}

/// `dryRun=All` runs the budget check but writes nothing: no decrement, no
/// condition, the pod stays (eviction.go:317-318, :465-468).
#[tokio::test]
async fn a_dry_run_eviction_changes_nothing() {
    let api = TestApiServer::new();
    assert_eq!(api.post(PODS, &pod_body("p")).await.0.as_u16(), 201);
    make_ready(&api, "p").await;
    seed_pdb(
        &api,
        "b",
        json!({"minAvailable": 0, "selector": {"matchLabels": {"app": "web"}}}),
        json!({"currentHealthy": 1, "desiredHealthy": 0, "disruptionsAllowed": 1, "expectedPods": 1}),
    )
    .await;
    let (code, body, _) = evict(&api, "p", "?dryRun=All").await;
    assert_eq!(code, 201, "{body}");
    let pod: Pod = api
        .storage
        .get(&build_key("pods", Some("default"), "p"))
        .await
        .unwrap();
    assert!(pod.metadata.deletion_timestamp.is_none());
    assert!(pod
        .status
        .and_then(|s| s.conditions)
        .unwrap_or_default()
        .iter()
        .all(|c| c.condition_type != "DisruptionTarget"));
    let status = stored_pdb(&api, "b").await.status.unwrap();
    assert_eq!(status.disruptions_allowed, 1);
    assert!(status.disrupted_pods.is_none());
}

/// More than one budget covering a pod is a 500 `Status` returned as the
/// result (eviction.go:223-230).
#[tokio::test]
async fn a_pod_under_two_budgets_is_a_500_status() {
    let api = TestApiServer::new();
    assert_eq!(api.post(PODS, &pod_body("p")).await.0.as_u16(), 201);
    make_ready(&api, "p").await;
    for name in ["b1", "b2"] {
        seed_pdb(
            &api,
            name,
            json!({"minAvailable": 0, "selector": {"matchLabels": {"app": "web"}}}),
            json!({"disruptionsAllowed": 5}),
        )
        .await;
    }
    let (code, body, _) = evict(&api, "p", "").await;
    assert_eq!(code, 500, "{body}");
    assert_eq!(
        body["message"],
        "This pod has more than one PodDisruptionBudget, which the eviction subresource does not support."
    );
    assert!(api.storage.get::<Pod>(&build_key("pods", Some("default"), "p")).await.is_ok());
}

/// #1802: `unhealthyPodEvictionPolicy` was ignored by eviction. An unready
/// pod under `AlwaysAllow` is deleted without touching the budget
/// (eviction.go:241-245).
#[tokio::test]
async fn always_allow_evicts_an_unready_pod_without_the_budget() {
    let api = TestApiServer::new();
    assert_eq!(api.post(PODS, &pod_body("p")).await.0.as_u16(), 201);
    // Running, but not ready.
    let key = build_key("pods", Some("default"), "p");
    let mut pod: Pod = api.storage.get(&key).await.unwrap();
    pod.status.get_or_insert_with(Default::default).phase = Some(Phase::Running);
    api.storage.update(&key, &pod).await.unwrap();
    seed_pdb(
        &api,
        "b",
        json!({"minAvailable": 1, "unhealthyPodEvictionPolicy": "AlwaysAllow",
               "selector": {"matchLabels": {"app": "web"}}}),
        json!({"currentHealthy": 0, "desiredHealthy": 1, "disruptionsAllowed": 0, "expectedPods": 1}),
    )
    .await;
    let (code, body, _) = evict(&api, "p", "").await;
    assert_eq!(code, 201, "{body}");
    assert!(api.storage.get::<Pod>(&key).await.is_err(), "the pod is gone");
    let status = stored_pdb(&api, "b").await.status.unwrap();
    assert_eq!(status.disruptions_allowed, 0);
    assert!(status.disrupted_pods.is_none());
}

/// The default policy (`IfHealthyBudget`) deletes an unready pod only while
/// the budget is healthy (eviction.go:246-252); otherwise it is checked like
/// a healthy one, and a 0 budget refuses it.
#[tokio::test]
async fn if_healthy_budget_guards_an_unready_pod_when_the_budget_is_not_healthy() {
    let api = TestApiServer::new();
    assert_eq!(api.post(PODS, &pod_body("p")).await.0.as_u16(), 201);
    let key = build_key("pods", Some("default"), "p");
    let mut pod: Pod = api.storage.get(&key).await.unwrap();
    pod.status.get_or_insert_with(Default::default).phase = Some(Phase::Running);
    api.storage.update(&key, &pod).await.unwrap();
    seed_pdb(
        &api,
        "b",
        json!({"minAvailable": 2, "selector": {"matchLabels": {"app": "web"}}}),
        json!({"currentHealthy": 1, "desiredHealthy": 2, "disruptionsAllowed": 0, "expectedPods": 2}),
    )
    .await;
    let (code, body, _) = evict(&api, "p", "").await;
    assert_eq!(code, 429, "{body}");
    assert!(api.storage.get::<Pod>(&key).await.is_ok());
}

/// A burst of evictions against one budget: each decrement is a
/// resourceVersion compare-and-set, so a lost race re-reads the PDB and
/// retries (`retry.RetryOnConflict`, eviction.go:257) and none is lost.
#[tokio::test]
async fn concurrent_evictions_each_take_one_disruption() {
    const N: usize = 6;
    let api = TestApiServer::new();
    for i in 0..N {
        let name = format!("p{i}");
        assert_eq!(api.post(PODS, &pod_body(&name)).await.0.as_u16(), 201);
        make_ready(&api, &name).await;
    }
    seed_pdb(
        &api,
        "b",
        json!({"minAvailable": 0, "selector": {"matchLabels": {"app": "web"}}}),
        json!({"currentHealthy": N, "desiredHealthy": 0, "disruptionsAllowed": N, "expectedPods": N}),
    )
    .await;
    let mut handles = Vec::new();
    for i in 0..N {
        let api = TestApiServer {
            storage: api.storage.clone(),
            router: api.router.clone(),
        };
        handles.push(tokio::spawn(async move {
            evict(&api, &format!("p{i}"), "").await.0
        }));
    }
    for handle in handles {
        assert_eq!(handle.await.unwrap(), 201);
    }
    let status = stored_pdb(&api, "b").await.status.unwrap();
    assert_eq!(status.disruptions_allowed, 0, "no decrement was lost");
    assert_eq!(status.disrupted_pods.unwrap().len(), N);
}

/// Preconditions in the eviction's `deleteOptions` are the pod delete's: a
/// stale UID leaves the pod alone, with a 409.
#[tokio::test]
async fn a_uid_precondition_that_does_not_match_is_a_409() {
    let api = TestApiServer::new();
    assert_eq!(api.post(PODS, &pod_body("p")).await.0.as_u16(), 201);
    let mut body = eviction_body("p");
    body["deleteOptions"] = json!({"preconditions": {"uid": "not-the-uid"}});
    let (status, _, _, value) = api
        .send_with_headers(
            "POST",
            &format!("{PODS}/p/eviction"),
            &[("content-type", "application/json")],
            Some(serde_json::to_vec(&body).unwrap()),
        )
        .await;
    assert_eq!(status.as_u16(), 409, "{value}");
    assert!(api
        .storage
        .get::<Pod>(&build_key("pods", Some("default"), "p"))
        .await
        .is_ok());
}

/// The router's eviction is the store-backed rest.
#[allow(dead_code)]
fn the_router_serves_the_store_backed_rest(storage: Arc<StorageBackend>) -> StoreEvictionRest {
    new_eviction_rest(storage)
}
