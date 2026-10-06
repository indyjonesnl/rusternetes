//! Ported from `storage/testing/store_tests.go`.

use super::fixtures::{pod, pod_key};
use rusternetes_storage::Storage;
use serde_json::Value;

/// Ported from `RunTestCreate` (`store_tests.go:52-93`).
///
/// Create must store the object and hand back a copy carrying a non-zero
/// resourceVersion.
///
/// `expects_resource_version` is false for backends with no revision concept.
/// `MemoryStorage` is one: it stamps uid and creationTimestamp but no
/// resourceVersion, its `current_revision()` returns a wall-clock timestamp,
/// and `watch_from_revision` states outright that it does not support
/// revisions. Upstream's `RunTestCreate` asserts the returned object carries a
/// valid RV, so this is a real gap in that backend, deliberately recorded here
/// rather than asserted away.
///
/// **Dropped upstream row: "create with ResourceVersion set".** Upstream is a
/// two-row table; row 2 creates a pod carrying `ResourceVersion: "1"` and
/// requires `storage.ErrResourceVersionSetOnCreate` — "resourceVersion should
/// not be set on objects to be created" (`storage/errors.go:29`), enforced in
/// `etcd3/store.go:288`:
///
/// ```go
/// if version, err := s.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
///     return storage.ErrResourceVersionSetOnCreate
/// }
/// ```
///
/// No `Storage` backend here rejects it and there is no equivalent `Error`
/// variant, so the row has no target. Measured 2026-09-01: etcd and kine both
/// accept the create and return the real mod_revision, but persist the caller's
/// stale `"1"` in the stored blob (masked on read, because
/// `inject_resource_version` overwrites it). `MemoryStorage` accepts it *and*
/// keeps `"1"` as the object's resourceVersion, so a caller can forge one. We
/// also have no equivalent of upstream's `PrepareObjectForStorage`, which zeroes
/// the RV before encoding. Recorded as a real gap rather than asserted away.
pub async fn run_test_create<S: Storage>(storage: &S, expects_resource_version: bool) {
    let key = pod_key("test-ns", "foo");

    // Upstream asserts the key is empty before the create (`store.Get` must be
    // NotFound) — the create's success proves nothing if something seeded it.
    let before = storage.get::<Value>(&key).await;
    assert!(
        matches!(before, Err(rusternetes_common::Error::NotFound(_))),
        "expected an empty key before create, got {before:?}"
    );

    let out: Value = storage
        .create(&key, &pod("test-ns", "foo"))
        .await
        .expect("create failed");

    if expects_resource_version {
        let rv = out["metadata"]["resourceVersion"]
            .as_str()
            .expect("create returned no resourceVersion");
        assert_ne!(rv, "0", "create returned a zero resourceVersion");
    }
    assert_eq!(out["metadata"]["name"], "foo");

    let stored: Value = storage.get(&key).await.expect("get after create failed");
    assert_eq!(stored["metadata"]["name"], "foo");
}

/// Ported from `RunTestCreateWithKeyExist` (`store_tests.go`): creating over an
/// existing key is a conflict, never an overwrite.
pub async fn run_test_create_with_key_exist<S: Storage>(storage: &S) {
    let key = pod_key("test-ns", "exists");
    storage
        .create(&key, &pod("test-ns", "exists"))
        .await
        .expect("first create failed");

    let err = storage
        .create::<Value>(&key, &pod("test-ns", "exists"))
        .await
        .expect_err("second create should fail");
    assert!(
        matches!(err, rusternetes_common::Error::AlreadyExists(_)),
        "expected AlreadyExists, got {err:?}"
    );
}

/// Ported from `RunTestGetListRecursivePrefix` (`store_tests.go:2740-2800`),
/// the two recursive *prefix* rows only.
///
/// The `test-ns` / `test-ns2` pair is the point: `test-ns2` sorts immediately
/// after the `test-ns/` prefix, so a scan whose upper bound is wrong swallows
/// it.
///
/// **Dropped upstream rows.** Six of the eight — the two that are ported below
/// are "Recursive on resource prefix returns all objects" and "Recursive on
/// resource prefix returns objects in the namespace". The six fall into two
/// groups:
///
/// * The four `recursive: false` rows (`store_tests.go:2750`, `:2762`, `:2774`
///   and `:2786`) have no target — our `list` is always recursive and there is
///   no non-recursive variant to call.
/// * The two RECURSIVE rows on an *object* key (`store_tests.go:2778` and
///   `:2790`, "Recursive on object key (prefix) doesn't return anything" and
///   its no-prefix twin) expect an empty result, because upstream appends a `/` to
///   a recursive key before ranging (`etcd3/store.go`, `GetList`): the key
///   `/pods/test-ns/foo` becomes the prefix `/pods/test-ns/foo/`, which matches
///   nothing.
///
///   Our `list` is a raw prefix scan with no such normalisation, and the
///   backends do not agree on the result. Measured 2026-09-01 with the three
///   objects below seeded — `list("/registry/pods/test-ns/foo")` returns
///   `["foo", "foobar"]` on etcd and on MemoryStorage but `[]` on kine, and
///   `list("/registry/pods/test-ns/foobar")` returns `["foobar"]` on etcd and
///   MemoryStorage but `[]` on kine. Upstream returns nothing for both.
///
///   Kine's empty result coincides with upstream's, but the mechanism is
///   unexplained and it should not be read as agreement on semantics. Kine's
///   list path applies no trailing-slash normalisation: `pkg/server/list.go`
///   passes key and range_end through unchanged, `LogStructured::List` passes
///   them on, and `pkg/drivers/generic/generic.go` resolves them to a plain
///   `name >= ? AND name < ?` byte range — the same shape etcd evaluates. Why
///   kine answers `[]` where etcd answers two rows is not established. (A
///   probe that would narrow it: seed `/registry/pods/test-ns/foo/child` as
///   well and list `/registry/pods/test-ns/foo` — a `/`-append normalisation
///   answers `["child"]`, anything else answers `[]`.)
///
///   Either way the etcd and MemoryStorage results are a real divergence from
///   upstream's key semantics. It is unreachable from the api-server (every
///   caller builds its prefix with `rusternetes_storage::build_prefix`, which
///   always ends in `/`), so the rows are left out rather than made to pass by
///   weakening anything.
///
/// **Requires an exclusive store.** The `all.len() == 3` assertion covers the
/// whole `/registry/pods/` prefix, and `run_test_create` writes `test-ns/foo`
/// under it too. That is only sound because `contract_suite!` gives every test
/// its own fixture — a fresh `MemoryStorage`, or its own etcd/kine container.
/// A shared fixture would break this assertion and `run_test_create`'s
/// empty-before-create one together.
pub async fn run_test_list_recursive_prefix<S: Storage>(storage: &S) {
    for (ns, name) in [
        ("test-ns", "foo"),
        ("test-ns", "foobar"),
        ("test-ns2", "baz"),
    ] {
        storage
            .create(&pod_key(ns, name), &pod(ns, name))
            .await
            .unwrap_or_else(|e| panic!("seeding {ns}/{name} failed: {e}"));
    }

    let all: Vec<Value> = storage
        .list("/registry/pods/")
        .await
        .expect("list on resource prefix failed");
    assert_eq!(all.len(), 3, "recursive list on the resource prefix");

    let ns: Vec<Value> = storage
        .list("/registry/pods/test-ns/")
        .await
        .expect("list on namespace prefix failed");
    let mut names: Vec<&str> = ns
        .iter()
        .map(|p| p["metadata"]["name"].as_str().expect("name"))
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        ["foo", "foobar"],
        "namespace prefix scan leaked into test-ns2"
    );
}

/// Ported from `RunTestUnconditionalDelete` (`store_tests.go`).
///
/// Upstream also asserts that the *returned* object carries a bumped
/// resourceVersion. Our `Storage::delete` returns `Result<()>`, so that row has
/// no target; the rest of the table ports directly.
pub async fn run_test_unconditional_delete<S: Storage>(storage: &S) {
    let key = pod_key("test-ns", "victim");
    storage
        .create(&key, &pod("test-ns", "victim"))
        .await
        .expect("seed failed");

    storage
        .delete(&key)
        .await
        .expect("delete of existing key failed");

    let gone = storage.get::<Value>(&key).await;
    assert!(
        matches!(gone, Err(rusternetes_common::Error::NotFound(_))),
        "object readable after delete: {gone:?}"
    );

    let missing = storage.delete(&pod_key("test-ns", "never-existed")).await;
    assert!(
        matches!(missing, Err(rusternetes_common::Error::NotFound(_))),
        "delete of a non-existing key should be NotFound, got {missing:?}"
    );
}

/// Ported from `RunTestListContinuation` (`store_tests.go`), reduced to the
/// invariant our `list_paginated` can express: walking a prefix with a limit
/// returns every key exactly once, in sort order, with no gaps or duplicates.
pub async fn run_test_list_continuation<S: Storage>(storage: &S) {
    let prefix = "/registry/pods/cont/";
    for i in 0..5 {
        let name = format!("test-{i}");
        storage
            .create(&pod_key("cont", &name), &pod("cont", &name))
            .await
            .unwrap_or_else(|e| panic!("seeding {name} failed: {e}"));
    }

    let mut seen: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let (items, next): (Vec<Value>, Option<String>) = storage
            .list_paginated(prefix, 2, token.as_deref())
            .await
            .expect("list_paginated failed");
        assert!(items.len() <= 2, "page exceeded the requested limit");
        for item in &items {
            seen.push(item["metadata"]["name"].as_str().expect("name").to_string());
        }
        match next {
            Some(t) => token = Some(t),
            None => break,
        }
    }

    assert_eq!(
        seen,
        vec!["test-0", "test-1", "test-2", "test-3", "test-4"],
        "continuation dropped, duplicated or reordered keys"
    );
}

/// Ported from `RunTestListPaging` (`store_tests.go`).
///
/// Four objects are paged one at a time; a fifth is created after the second
/// page. On a snapshot-capable backend the walk must still take exactly four
/// calls and return only the original four — a page sequence reflects the
/// revision the first page was taken at, not a live view.
///
/// `expects_snapshot` is false for backends that keep only current state
/// (`MemoryStorage`). Those still have to return every original object exactly
/// once; they are just allowed to observe the mid-walk write.
pub async fn run_test_list_paging<S: Storage>(storage: &S, expects_snapshot: bool) {
    let prefix = "/registry/pods/paging/";
    for i in 0..4 {
        let name = format!("test-{i}");
        storage
            .create(&pod_key("paging", &name), &pod("paging", &name))
            .await
            .unwrap_or_else(|e| panic!("seeding {name} failed: {e}"));
    }

    let mut names: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    let mut calls = 0;
    loop {
        calls += 1;
        let (items, next): (Vec<Value>, Option<String>) = storage
            .list_paginated(prefix, 1, token.as_deref())
            .await
            .expect("list_paginated failed");
        for item in &items {
            names.push(item["metadata"]["name"].as_str().expect("name").to_string());
        }
        let Some(next_token) = next else { break };
        if calls == 2 {
            storage
                .create(&pod_key("paging", "test-5"), &pod("paging", "test-5"))
                .await
                .expect("mid-pagination create failed");
        }
        token = Some(next_token);
    }

    // Every backend: the four originals, each exactly once, in order.
    let originals: Vec<&String> = names.iter().filter(|n| n.as_str() != "test-5").collect();
    assert_eq!(
        originals,
        vec!["test-0", "test-1", "test-2", "test-3"],
        "continuation dropped, duplicated or reordered the original objects"
    );

    if expects_snapshot {
        assert_eq!(calls, 4, "unexpected number of list calls");
        assert_eq!(
            names,
            vec!["test-0", "test-1", "test-2", "test-3"],
            "an object created mid-pagination leaked into the page sequence"
        );
    }
}

/// Ported from `RunTestGuaranteedUpdateWithConflict`
/// (`staging/src/k8s.io/apiserver/pkg/storage/testing/store_tests.go:3633-3676`).
///
/// Upstream drives two `GuaranteedUpdate`s that overlap in time from the same
/// starting revision, and asserts the second one's mutation function runs
/// **twice**:
///
/// ```go
/// if updateCount != 2 {
///     t.Errorf("Should have conflict and called update func twice")
/// }
/// ```
///
/// The second call runs again only because its first compare-and-swap lost and
/// it re-read. `GuaranteedUpdate` is built on top of the CAS primitive, which
/// here is `Storage::update` keyed on `metadata.resourceVersion`, so the same
/// property states as: **two updates carrying the same resourceVersion must
/// never both succeed.** One wins; the other is `Error::Conflict`.
///
/// This is the *sequential* row: the second update starts after the first has
/// committed, so only the stored revision has moved on. Any backend that
/// compares at all catches it. The overlapping row — the one that finds a
/// compare which is not atomic with the write — is
/// `run_test_concurrent_update_is_atomic`.
pub async fn run_test_update_with_conflict<S: Storage>(storage: &S) {
    // Row 1: sequential.
    let key = pod_key("test-ns", "conflict-seq");
    storage
        .create::<Value>(&key, &pod("test-ns", "conflict-seq"))
        .await
        .expect("create failed");
    let base: Value = storage.get(&key).await.expect("get failed");

    let mut first = base.clone();
    first["metadata"]["labels"] = serde_json::json!({ "writer": "first" });
    storage
        .update::<Value>(&key, &first)
        .await
        .expect("an update from a freshly-read object must succeed");

    let mut second = base.clone();
    second["metadata"]["labels"] = serde_json::json!({ "writer": "second" });
    let err = storage
        .update::<Value>(&key, &second)
        .await
        .expect_err("an update from a superseded resourceVersion must be rejected");
    assert!(
        matches!(err, rusternetes_common::Error::Conflict(_)),
        "expected Conflict for a superseded resourceVersion, got {err:?}"
    );
}

/// Row 2 of `RunTestGuaranteedUpdateWithConflict`: the writers *overlap*.
///
/// Upstream's version is a two-goroutine interleaving pinned with
/// `secondToEnter` / `firstToFinish` so the second `GuaranteedUpdate` is
/// guaranteed to be inside its mutation when the first commits. We cannot pin
/// the interleaving from outside the `Storage` trait, so this runs many rounds
/// of several writers on a multi-threaded runtime and asserts the invariant
/// every round: of N updates that all carry the same starting resourceVersion,
/// exactly one succeeds.
///
/// A backend that reads the current revision, compares it, and only then writes
/// — with the read and the write in separate transactions — lets every writer
/// pass the comparison before any of them writes. Each then reports success,
/// and the last write silently reverts the others. That is what the sequential
/// row cannot see: there, the only writer has already committed.
///
/// This is the storage-layer half of #1840: a `/status` write built from a
/// snapshot taken microseconds earlier restores that snapshot's `spec` over a
/// spec the client just changed, and the conflict that should have stopped it
/// is never raised.
pub async fn run_test_concurrent_update_is_atomic<S>(storage: std::sync::Arc<S>)
where
    S: Storage + Send + Sync + 'static,
{
    const ROUNDS: usize = 40;
    const WRITERS: usize = 4;

    for round in 0..ROUNDS {
        let name = format!("race-{round}");
        let key = pod_key("test-ns", &name);
        storage
            .create::<Value>(&key, &pod("test-ns", &name))
            .await
            .expect("create failed");
        let base: Value = storage.get(&key).await.expect("get failed");

        let mut handles = Vec::with_capacity(WRITERS);
        for writer in 0..WRITERS {
            let storage = std::sync::Arc::clone(&storage);
            let key = key.clone();
            let mut object = base.clone();
            object["metadata"]["labels"] = serde_json::json!({ "writer": writer.to_string() });
            handles.push(tokio::spawn(async move {
                storage.update::<Value>(&key, &object).await
            }));
        }

        let mut winners = Vec::new();
        let mut losers = Vec::new();
        for (writer, handle) in handles.into_iter().enumerate() {
            match handle.await.expect("writer task panicked") {
                Ok(_) => winners.push(writer),
                Err(e) => losers.push((writer, e)),
            }
        }

        assert_eq!(
            winners.len(),
            1,
            "round {round}: {} of {WRITERS} updates from the same resourceVersion \
             succeeded, expected exactly 1 (winners: {winners:?})",
            winners.len()
        );
        for (writer, e) in &losers {
            assert!(
                matches!(e, rusternetes_common::Error::Conflict(_)),
                "round {round}: writer {writer} must lose with Conflict, got {e:?}"
            );
        }

        // The survivor is the winner's object, whole.
        let stored: Value = storage.get(&key).await.expect("get after race failed");
        assert_eq!(
            stored["metadata"]["labels"]["writer"],
            winners[0].to_string(),
            "round {round}: the stored object is not the winning writer's"
        );
    }
}

/// Whether `key` is gone, polling up to `within`. Expiry is asynchronous in
/// every backend (an etcd lease sweep, rhino's one-second TTL loop).
async fn gone_within<S: Storage>(storage: &S, key: &str, within: std::time::Duration) -> bool {
    let deadline = std::time::Instant::now() + within;
    loop {
        if matches!(
            storage.get::<Value>(key).await,
            Err(rusternetes_common::Error::NotFound(_))
        ) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

/// An open watch for the length of a TTL test. rhino starts its background
/// loops, the TTL reaper among them, on the first watch subscription
/// (`SqliteBackend::watch` -> "background tasks started (first watch
/// subscription)"), so a store nobody watches never expires anything. The
/// api-server always holds watches, so this is the production condition.
async fn hold_a_watch<S: Storage>(storage: &S) -> rusternetes_storage::WatchStream {
    storage
        .watch("/registry/")
        .await
        .expect("open a watch to start the backend's background loops")
}

/// Ported from `RunTestCreateWithTTL` (`store_tests.go`): an object created
/// with a TTL is readable, then evicted once the TTL expires; one created
/// with TTL 0 never is.
pub async fn run_test_create_with_ttl<S: Storage>(storage: &S) {
    let _watch = hold_a_watch(storage).await;
    let ttl_key = pod_key("test-ns", "ephemeral");
    let keep_key = pod_key("test-ns", "permanent");

    storage
        .create_with_ttl::<Value>(&ttl_key, &pod("test-ns", "ephemeral"), 1)
        .await
        .expect("create with ttl failed");
    storage
        .create_with_ttl::<Value>(&keep_key, &pod("test-ns", "permanent"), 0)
        .await
        .expect("create without ttl failed");
    storage
        .get::<Value>(&ttl_key)
        .await
        .expect("object must be readable before its TTL expires");

    assert!(
        gone_within(storage, &ttl_key, std::time::Duration::from_secs(15)).await,
        "object created with ttl=1 was not evicted"
    );
    storage
        .get::<Value>(&keep_key)
        .await
        .expect("an object created with ttl=0 must not expire");
}

/// Ported from the TTL rows of `RunTestGuaranteedUpdate`: an update replaces
/// the key's TTL. ttl=0 makes the object permanent, a TTL on a permanent
/// object makes it expire.
pub async fn run_test_update_with_ttl<S: Storage>(storage: &S) {
    let _watch = hold_a_watch(storage).await;
    let detach_key = pod_key("test-ns", "detached");
    let attach_key = pod_key("test-ns", "attached");

    let created: Value = storage
        .create_with_ttl(&detach_key, &pod("test-ns", "detached"), 1)
        .await
        .expect("create with ttl failed");
    storage
        .update_with_ttl::<Value>(&detach_key, &created, 0)
        .await
        .expect("update with ttl=0 failed");

    let created: Value = storage
        .create(&attach_key, &pod("test-ns", "attached"))
        .await
        .expect("create failed");
    storage
        .update_with_ttl::<Value>(&attach_key, &created, 1)
        .await
        .expect("update with ttl failed");

    assert!(
        gone_within(storage, &attach_key, std::time::Duration::from_secs(15)).await,
        "update with ttl=1 did not make the object expire"
    );
    // Well past the 1s TTL the first object was created with.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    storage
        .get::<Value>(&detach_key)
        .await
        .expect("update with ttl=0 must detach the TTL");
}
