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
/// **Dropped upstream rows.** Four of the eight, in two groups:
///
/// * The three `recursive: false` rows have no target — our `list` is always
///   recursive and there is no non-recursive variant to call.
/// * The two RECURSIVE rows on an *object* key (`store_tests.go:2772-2782`,
///   "Recursive on object key (prefix) doesn't return anything" and its
///   no-prefix twin) expect an empty result, because upstream appends a `/` to
///   a recursive key before ranging (`etcd3/store.go`, `GetList`): the key
///   `/pods/test-ns/foo` becomes the prefix `/pods/test-ns/foo/`, which matches
///   nothing.
///
///   Our `list` is a raw prefix scan with no such normalisation, and the
///   backends do not even agree with each other. Measured 2026-09-01 with the
///   three objects below seeded — `list("/registry/pods/test-ns/foo")` returns
///   `["foo", "foobar"]` on etcd and on MemoryStorage but `[]` on kine, and
///   `list("/registry/pods/test-ns/foobar")` returns `["foobar"]` on etcd and
///   MemoryStorage but `[]` on kine. Upstream returns nothing for both.
///
///   That is a real divergence between our prefix-scan `list` and upstream's
///   key semantics, and a three-way one: kine's range handling already matches
///   upstream while ours does not. It is unreachable from the api-server (every
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
