//! The etcd backend must speak only the etcd v3 RPC subset that upstream's
//! `etcd3` store emits.
//!
//! Upstream Kubernetes drives etcd through a deliberately narrow set of calls —
//! see `staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go` and the
//! `OptimisticPut`/`OptimisticDelete` helpers in
//! `vendor/go.etcd.io/etcd/client/v3/kubernetes/client.go`. Every mutation is a
//! single-op transaction guarded on `ModRevision`; there are no bare `Put`s, no
//! bare `DeleteRange`s, no `Version` comparisons and never more than one
//! operation in the success branch.
//!
//! Staying inside that subset is not a self-imposed limitation: it is the
//! upstream-first rule applied to the storage layer, and it is what keeps the
//! backend portable across etcd-API implementations. `kine` (the etcd shim k3s
//! and k0s ship, backed by SQLite/Postgres/MySQL/NATS) implements exactly the
//! subset upstream emits and rejects everything else with `Unimplemented` or
//! `InvalidArgument` — which makes it a precise, runnable oracle for the rule.
//!
//! These tests therefore run the *same* exercise against both real etcd and
//! kine. Divergence in either direction is a bug:
//!   - kine red, etcd green  → we drifted outside the upstream subset.
//!   - etcd red              → we broke the backend outright.

use rusternetes_storage::{etcd::EtcdStorage, Storage, WatchEvent};
use serde_json::json;
use std::time::Duration;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt, TestcontainersError,
};

/// Detects whether the host has a usable Docker (or Docker-compatible) socket,
/// so Docker-less runners soft-skip instead of failing the job. Mirrors the
/// helper in `src/etcd.rs`'s unit tests.
fn is_docker_unavailable(err: &TestcontainersError) -> bool {
    matches!(
        err,
        TestcontainersError::Client(testcontainers::core::client::ClientError::Init(_))
    )
}

/// Escape hatch for environments where Docker is mandatory — same contract as
/// the copy in `contract/fixtures.rs` (these are separate test binaries and
/// cannot share a module). Unset: soft-skip. `1`: panic.
const REQUIRE_DOCKER_ENV: &str = "STORAGE_CONTRACT_REQUIRE_DOCKER";

/// Report a Docker-unavailable container: a soft skip by default, a panic under
/// `STORAGE_CONTRACT_REQUIRE_DOCKER=1`.
fn skip_or_require(what: &str, err: &TestcontainersError) {
    if std::env::var(REQUIRE_DOCKER_ENV).as_deref() == Ok("1") {
        panic!("{what}: Docker unavailable ({err}), but {REQUIRE_DOCKER_ENV}=1 requires it");
    }
    eprintln!("skipping {what}: Docker unavailable ({err})");
}

/// Connect an `EtcdStorage` to a started container's mapped 2379 port.
///
/// `page_size` is deliberately tiny so `list` has to walk several pages without
/// the test writing 500+ keys.
async fn connect(container: &ContainerAsync<GenericImage>, page_size: i64) -> EtcdStorage {
    let host = container
        .get_host()
        .await
        .expect("failed to resolve test container host");
    let port = container
        .get_host_port_ipv4(2379)
        .await
        .expect("failed to read mapped client port");

    EtcdStorage::new(vec![format!("http://{host}:{port}")])
        .await
        .expect("failed to connect to test backend")
        .with_page_size(page_size)
}

/// Boot single-node etcd. `None` when Docker is unreachable.
async fn start_etcd() -> Option<ContainerAsync<GenericImage>> {
    let result = GenericImage::new("quay.io/coreos/etcd", "v3.5.17")
        .with_exposed_port(2379.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
        .with_cmd([
            "/usr/local/bin/etcd",
            "--name=etcd-test",
            "--data-dir=/etcd-data",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://0.0.0.0:2379",
        ])
        .start()
        .await;

    match result {
        Ok(c) => Some(c),
        Err(e) if is_docker_unavailable(&e) => {
            skip_or_require("etcd RPC-subset test", &e);
            None
        }
        Err(e) => panic!("failed to start etcd test container: {e}"),
    }
}

/// Boot kine over an ephemeral SQLite file. `None` when Docker is unreachable.
async fn start_kine() -> Option<ContainerAsync<GenericImage>> {
    let result = GenericImage::new("rancher/kine", "v0.13.11")
        .with_exposed_port(2379.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Kine available at"))
        .with_cmd([
            "--endpoint=sqlite:///tmp/kine.db",
            "--listen-address=0.0.0.0:2379",
        ])
        .start()
        .await;

    match result {
        Ok(c) => Some(c),
        Err(e) if is_docker_unavailable(&e) => {
            skip_or_require("kine RPC-subset test", &e);
            None
        }
        Err(e) => panic!("failed to start kine test container: {e}"),
    }
}

/// Every `Storage` operation the etcd backend implements, exercised end to end.
///
/// `backend` only labels assertion failures so a red run says which oracle
/// disagreed.
async fn exercise_storage_surface(storage: &EtcdStorage, backend: &str) {
    let key = "/registry/pods/default/subset";

    // --- create: single-op ModRevision(key)==0 txn ---------------------------
    let created: serde_json::Value = storage
        .create(key, &json!({"spec": {"replicas": 1}}))
        .await
        .unwrap_or_else(|e| panic!("[{backend}] create failed: {e}"));
    let rv = created["metadata"]["resourceVersion"]
        .as_str()
        .unwrap_or_else(|| panic!("[{backend}] create returned no resourceVersion: {created}"))
        .to_string();
    assert_ne!(
        rv, "0",
        "[{backend}] create returned a zero resourceVersion"
    );

    // Creating the same key twice must conflict, not overwrite.
    let dup = storage.create(key, &json!({"spec": {"replicas": 9}})).await;
    assert!(
        matches!(dup, Err(rusternetes_common::Error::AlreadyExists(_))),
        "[{backend}] duplicate create should be AlreadyExists, got {dup:?}"
    );

    // --- get -----------------------------------------------------------------
    let fetched: serde_json::Value = storage
        .get(key)
        .await
        .unwrap_or_else(|e| panic!("[{backend}] get failed: {e}"));
    assert_eq!(
        fetched["spec"]["replicas"], 1,
        "[{backend}] get returned the wrong value"
    );

    // --- update with a resourceVersion: guarded ModRevision txn --------------
    let mut next = fetched.clone();
    next["spec"]["replicas"] = json!(2);
    let updated: serde_json::Value = storage
        .update(key, &next)
        .await
        .unwrap_or_else(|e| panic!("[{backend}] update failed: {e}"));
    assert_eq!(
        updated["spec"]["replicas"], 2,
        "[{backend}] update did not persist"
    );
    assert_ne!(
        updated["metadata"]["resourceVersion"].as_str(),
        Some(rv.as_str()),
        "[{backend}] update did not bump resourceVersion"
    );

    // A stale resourceVersion must lose the race.
    let mut stale = next.clone();
    stale["metadata"]["resourceVersion"] = json!(rv);
    stale["spec"]["replicas"] = json!(99);
    let conflict = storage.update(key, &stale).await;
    assert!(
        matches!(conflict, Err(rusternetes_common::Error::Conflict(_))),
        "[{backend}] stale update should Conflict, got {conflict:?}"
    );

    // --- update without a resourceVersion: read-modify-write, still guarded --
    let mut unversioned = updated.clone();
    unversioned["metadata"]
        .as_object_mut()
        .expect("metadata object")
        .remove("resourceVersion");
    unversioned["spec"]["replicas"] = json!(3);
    let blind: serde_json::Value = storage
        .update(key, &unversioned)
        .await
        .unwrap_or_else(|e| panic!("[{backend}] unversioned update failed: {e}"));
    assert_eq!(
        blind["spec"]["replicas"], 3,
        "[{backend}] unversioned update did not persist"
    );

    // Updating a key that does not exist is NotFound, not a silent create.
    let missing = storage
        .update("/registry/pods/default/ghost", &json!({"spec": {}}))
        .await;
    assert!(
        matches!(missing, Err(rusternetes_common::Error::NotFound(_))),
        "[{backend}] update of a missing key should be NotFound, got {missing:?}"
    );

    // --- update_raw ----------------------------------------------------------
    storage
        .update_raw(key, &json!({"spec": {"replicas": 4}}))
        .await
        .unwrap_or_else(|e| panic!("[{backend}] update_raw failed: {e}"));
    let after_raw: serde_json::Value = storage.get(key).await.expect("get after update_raw");
    assert_eq!(
        after_raw["spec"]["replicas"], 4,
        "[{backend}] update_raw did not persist"
    );

    // --- current_revision ----------------------------------------------------
    let revision = storage
        .current_revision()
        .await
        .unwrap_or_else(|e| panic!("[{backend}] current_revision failed: {e}"));
    assert!(
        revision > 0,
        "[{backend}] current_revision returned {revision}"
    );

    // --- delete: guarded ModRevision txn -------------------------------------
    storage
        .delete(key)
        .await
        .unwrap_or_else(|e| panic!("[{backend}] delete failed: {e}"));
    let gone = storage.get::<serde_json::Value>(key).await;
    assert!(
        matches!(gone, Err(rusternetes_common::Error::NotFound(_))),
        "[{backend}] delete left the key readable, got {gone:?}"
    );
    let twice = storage.delete(key).await;
    assert!(
        matches!(twice, Err(rusternetes_common::Error::NotFound(_))),
        "[{backend}] second delete should be NotFound, got {twice:?}"
    );
}

/// `list` must return every key under the prefix across page boundaries, and
/// must not leak keys from a sibling prefix.
///
/// Regression: page 2+ used `with_prefix()` + `with_from_key()` — the latter
/// wins in `etcd-client`, producing an unbounded `[key, +inf)` range. Real etcd
/// tolerated it (a manual prefix re-check trimmed the overshoot); kine returned
/// nothing at all, silently truncating every list to its first page. Upstream
/// instead continues inside the prefix range with `lastKey + "\x00"`
/// (`etcd3/store.go`, `continueKey = string(lastKey) + "\x00"`).
async fn exercise_list_paging(storage: &EtcdStorage, backend: &str, page_size: usize) {
    let total = page_size * 2 + 1; // force at least three pages
    for i in 0..total {
        storage
            .create(
                &format!("/registry/pods/paged/p{i:04}"),
                &json!({"spec": {"n": i}}),
            )
            .await
            .unwrap_or_else(|e| panic!("[{backend}] seeding p{i:04} failed: {e}"));
    }

    // A sibling prefix that sorts immediately after the one under test: an
    // unbounded from-key range would run straight into it.
    storage
        .create("/registry/pods/pagedx/intruder", &json!({"spec": {}}))
        .await
        .expect("seeding sibling prefix");

    let listed: Vec<serde_json::Value> = storage
        .list("/registry/pods/paged/")
        .await
        .unwrap_or_else(|e| panic!("[{backend}] list failed: {e}"));

    assert_eq!(
        listed.len(),
        total,
        "[{backend}] list returned {} of {total} keys — pagination dropped rows",
        listed.len()
    );

    let mut seen: Vec<i64> = listed
        .iter()
        .map(|v| v["spec"]["n"].as_i64().expect("seeded n"))
        .collect();
    seen.sort_unstable();
    assert_eq!(
        seen,
        (0..total as i64).collect::<Vec<_>>(),
        "[{backend}] list returned the wrong set of keys"
    );
}

/// Watch must label a first write ADDED and a subsequent write MODIFIED.
///
/// Regression: the backend inferred "created" from etcd's per-key `Version`
/// field (`version == 1`). Upstream never does — `clientv3.Event.IsCreate()` is
/// `Type == PUT && CreateRevision == ModRevision`
/// (`vendor/go.etcd.io/etcd/client/v3/watch.go`), which survives compaction of
/// the previous revision *and* is populated by kine. kine sends no `Version` at
/// all, so the old heuristic reported every create as MODIFIED.
async fn exercise_watch_create_vs_update(storage: &EtcdStorage, backend: &str) {
    use futures::StreamExt;

    let prefix = "/registry/pods/watched/";
    let key = "/registry/pods/watched/w1";

    let mut stream = storage
        .watch(prefix)
        .await
        .unwrap_or_else(|e| panic!("[{backend}] watch failed: {e}"));

    // Give the watch a moment to be established server-side before writing.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let created: serde_json::Value = storage
        .create(key, &json!({"spec": {"replicas": 1}}))
        .await
        .expect("watch fixture create");
    let mut next = created.clone();
    next["spec"]["replicas"] = json!(2);
    storage
        .update(key, &next)
        .await
        .expect("watch fixture update");

    let mut events = Vec::new();
    let collect = async {
        while let Some(event) = stream.next().await {
            events.push(event.expect("watch stream error"));
            if events.len() == 2 {
                break;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(15), collect)
        .await
        .unwrap_or_else(|_| {
            panic!("[{backend}] timed out waiting for watch events, got {events:?}")
        });

    assert!(
        matches!(events[0], WatchEvent::Added(..)),
        "[{backend}] first write should be ADDED, got {:?}",
        events[0]
    );
    assert!(
        matches!(events[1], WatchEvent::Modified(..)),
        "[{backend}] second write should be MODIFIED, got {:?}",
        events[1]
    );
}

const PAGE_SIZE: usize = 3;

#[tokio::test]
async fn etcd_backend_speaks_the_upstream_rpc_subset_on_etcd() {
    let Some(container) = start_etcd().await else {
        return;
    };
    let storage = connect(&container, PAGE_SIZE as i64).await;

    exercise_storage_surface(&storage, "etcd").await;
    exercise_list_paging(&storage, "etcd", PAGE_SIZE).await;
    exercise_watch_create_vs_update(&storage, "etcd").await;
}

#[tokio::test]
async fn etcd_backend_speaks_the_upstream_rpc_subset_on_kine() {
    let Some(container) = start_kine().await else {
        return;
    };
    let storage = connect(&container, PAGE_SIZE as i64).await;

    exercise_storage_surface(&storage, "kine").await;
    exercise_list_paging(&storage, "kine", PAGE_SIZE).await;
    exercise_watch_create_vs_update(&storage, "kine").await;
}
