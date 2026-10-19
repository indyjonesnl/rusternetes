//! Backend-generic conformance suite for `Storage::delete_with_preconditions`,
//! ported from upstream `storage/testing/store_tests.go`
//! (`RunTestPreconditionalDeleteWithSuggestion`,
//! `RunTestPreconditionalDeleteWithOnlySuggestionPass`) and
//! `etcd3/store_test.go` `TestPreconditions`. Each backend's test module calls
//! `run_all` with a fresh-store constructor.

use crate::{Preconditions, Storage};
use rusternetes_common::Error;
use serde_json::{json, Value};
use std::sync::Arc;

const KEY: &str = "/registry/pods/test-ns/name";

async fn seed<S: Storage>(s: &S) -> Value {
    s.create(
        KEY,
        &json!({"apiVersion":"v1","kind":"Pod",
                "metadata":{"name":"name","namespace":"test-ns"}}),
    )
    .await
    .unwrap()
}

async fn set_uid<S: Storage>(s: &S, uid: &str) -> Value {
    let mut cur: Value = s.get(KEY).await.unwrap();
    cur["metadata"]["uid"] = json!(uid);
    s.update(KEY, &cur).await.unwrap()
}

async fn exists<S: Storage>(s: &S) -> bool {
    s.get::<Value>(KEY).await.is_ok()
}

fn rv_of(v: &Value) -> String {
    v["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .to_string()
}

/// Runs every case against one shared store (cases clean up their own key,
/// so a single etcd container can serve the whole suite).
pub(crate) async fn run_all<S: Storage + 'static>(s: Arc<S>) {
    let _ = s.delete(KEY).await;
    matching_uid_deletes(&*s).await;
    mismatching_uid_conflicts_and_keeps_object(&*s).await;
    let _ = s.delete(KEY).await;
    matching_resource_version_deletes_and_stale_conflicts(&*s).await;
    both_preconditions_must_hold(&*s).await;
    missing_key_is_not_found(&*s).await;
    empty_preconditions_delete_unconditionally(&*s).await;
    racing_uid_change_never_deletes_the_new_object(s.clone()).await;
    let _ = s.delete(KEY).await;
    patch_with_preconditions(&*s).await;
    racing_recreate_never_patches_the_new_object(s.clone()).await;
    let _ = s.delete(KEY).await;
}

/// `GuaranteedUpdate` with `Preconditions` (`etcd3/store.go:463`, `:500-517`)
/// driven through `patch_strategic_merge_with_preconditions`.
async fn patch_with_preconditions<S: Storage>(s: &S) {
    let _ = s.delete(KEY).await;
    seed(s).await;
    let cur = set_uid(s, "myUID").await;
    let patch = json!({"metadata": {"labels": {"k": "v"}}});

    // Mismatching UID: Conflict, object untouched.
    let err = s
        .patch_strategic_merge_with_preconditions::<Value>(
            KEY,
            &patch,
            &Preconditions::new_uid("other"),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Conflict(_)), "{err:?}");
    let now: Value = s.get(KEY).await.unwrap();
    assert!(now["metadata"]["labels"].is_null());

    // Stale resourceVersion: Conflict.
    let err = s
        .patch_strategic_merge_with_preconditions::<Value>(
            KEY,
            &patch,
            &Preconditions::new_resource_version("1"),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Conflict(_)), "{err:?}");

    // Matching UID + resourceVersion: patched.
    let both = Preconditions {
        uid: Some("myUID".into()),
        resource_version: Some(rv_of(&cur)),
    };
    let out = s
        .patch_strategic_merge_with_preconditions::<Value>(KEY, &patch, &both)
        .await
        .unwrap();
    assert_eq!(out["metadata"]["labels"]["k"], "v");
    assert_eq!(out["metadata"]["uid"], "myUID");

    // Empty preconditions patch unconditionally; missing key is NotFound.
    s.patch_strategic_merge_with_preconditions::<Value>(
        KEY,
        &json!({"metadata": {"labels": {"j": "w"}}}),
        &Preconditions::default(),
    )
    .await
    .unwrap();
    s.delete(KEY).await.unwrap();
    let err = s
        .patch_strategic_merge_with_preconditions::<Value>(
            KEY,
            &patch,
            &Preconditions::new_uid("myUID"),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err:?}");
}

/// The object is deleted and recreated (new UID) while a UID-guarded patch is
/// in flight: the patch may land on the old generation or be rejected, but it
/// must never land on the new one.
async fn racing_recreate_never_patches_the_new_object<S: Storage + 'static>(s: Arc<S>) {
    for i in 0..50 {
        let _ = s.delete(KEY).await;
        seed(&*s).await;
        set_uid(&*s, "old").await;
        let r = {
            let s = s.clone();
            tokio::spawn(async move {
                s.delete(KEY).await?;
                seed(&*s).await;
                set_uid(&*s, "new").await;
                Ok::<_, Error>(())
            })
        };
        let p = {
            let s = s.clone();
            tokio::spawn(async move {
                s.patch_strategic_merge_with_preconditions::<Value>(
                    KEY,
                    &json!({"metadata": {"labels": {"k": "v"}}}),
                    &Preconditions::new_uid("old"),
                )
                .await
            })
        };
        let _ = r.await.unwrap();
        let _ = p.await.unwrap();
        if let Ok(now) = s.get::<Value>(KEY).await {
            if now["metadata"]["uid"] == "new" {
                assert!(
                    now["metadata"]["labels"].is_null(),
                    "iter {i}: patch for UID old landed on the recreated object"
                );
            }
        }
    }
}

/// `RunTestPreconditionalDeleteWithSuggestion`.
async fn matching_uid_deletes<S: Storage>(s: &S) {
    seed(s).await;
    set_uid(s, "myUID").await;
    s.delete_with_preconditions(KEY, &Preconditions::new_uid("myUID"))
        .await
        .unwrap();
    assert!(matches!(s.get::<Value>(KEY).await, Err(Error::NotFound(_))));
}

/// `RunTestPreconditionalDeleteWithOnlySuggestionPass`: the object has moved
/// to another UID, so the delete must fail the precondition and leave the
/// object intact (409 via `InterpretDeleteError`).
async fn mismatching_uid_conflicts_and_keeps_object<S: Storage>(s: &S) {
    seed(s).await;
    let updated = set_uid(s, "otherUID").await;
    let err = s
        .delete_with_preconditions(KEY, &Preconditions::new_uid("myUID"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Conflict(_)), "got {err:?}");
    let now: Value = s.get(KEY).await.unwrap();
    assert_eq!(now, updated, "object must be untouched");
}

async fn matching_resource_version_deletes_and_stale_conflicts<S: Storage>(s: &S) {
    let created = seed(s).await;
    let stale_rv = rv_of(&created);
    let cur_rv = rv_of(&set_uid(s, "u").await);
    assert_ne!(stale_rv, cur_rv);
    let err = s
        .delete_with_preconditions(KEY, &Preconditions::new_resource_version(stale_rv))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Conflict(_)), "got {err:?}");
    assert!(exists(s).await);
    s.delete_with_preconditions(KEY, &Preconditions::new_resource_version(cur_rv))
        .await
        .unwrap();
    assert!(!exists(s).await);
}

async fn both_preconditions_must_hold<S: Storage>(s: &S) {
    seed(s).await;
    let rv = rv_of(&set_uid(s, "u").await);
    let bad = Preconditions {
        uid: Some("u".into()),
        resource_version: Some("0".into()),
    };
    assert!(matches!(
        s.delete_with_preconditions(KEY, &bad).await,
        Err(Error::Conflict(_))
    ));
    assert!(exists(s).await);
    let good = Preconditions {
        uid: Some("u".into()),
        resource_version: Some(rv),
    };
    s.delete_with_preconditions(KEY, &good).await.unwrap();
    assert!(!exists(s).await);
}

async fn missing_key_is_not_found<S: Storage>(s: &S) {
    let err = s
        .delete_with_preconditions(KEY, &Preconditions::new_uid("x"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
}

async fn empty_preconditions_delete_unconditionally<S: Storage>(s: &S) {
    seed(s).await;
    s.delete_with_preconditions(KEY, &Preconditions::default())
        .await
        .unwrap();
    assert!(!exists(s).await);
}

/// The atomicity the get+compare+delete emulation lacked: a writer that
/// changes the UID concurrently must never lose its object to a delete keyed
/// on the OLD uid. Either the delete wins (object gone) or the writer wins
/// (delete is a Conflict, object stays with the new UID).
async fn racing_uid_change_never_deletes_the_new_object<S: Storage + 'static>(s: Arc<S>) {
    for i in 0..50 {
        let _ = s.delete(KEY).await;
        seed(&*s).await;
        set_uid(&*s, "old").await;
        let w = {
            let s = s.clone();
            tokio::spawn(async move {
                let mut cur: Value = s.get(KEY).await?;
                cur["metadata"]["uid"] = json!("new");
                s.update(KEY, &cur).await.map(|_| ())
            })
        };
        let d = {
            let s = s.clone();
            tokio::spawn(async move {
                s.delete_with_preconditions(KEY, &Preconditions::new_uid("old"))
                    .await
            })
        };
        let (w, d) = (w.await.unwrap(), d.await.unwrap());
        match (w, d) {
            (Ok(()), Err(Error::Conflict(_))) => {
                let now: Value = s.get(KEY).await.unwrap();
                assert_eq!(now["metadata"]["uid"], "new", "iter {i}");
            }
            // Delete won; the writer then hits a missing key. A writer that
            // ALSO succeeded means the delete removed the NEW object.
            (Err(_), Ok(())) => assert!(!exists(&*s).await, "iter {i}: deleted => gone"),
            other => panic!("iter {i}: unexpected {other:?}"),
        }
    }
}
