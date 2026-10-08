//! Guard: a controller that builds an object with a fresh identity and also
//! writes with `update*` must adopt the stored object's identity first.
//!
//! Upstream cannot express this bug. Its controllers update a `DeepCopy()` of
//! the object they read (`staging/src/k8s.io/endpointslice/reconciler.go:551`,
//! `pkg/controller/endpoint/endpoints_controller.go:470`), so the PUT carries
//! the stored UID, creationTimestamp, finalizers and managedFields. The API
//! server's generic Store turns a non-empty `metadata.uid` on a PUT into a UID
//! precondition (`rest/update.go:188-203`), so a controller that rebuilds the
//! object with `ObjectMeta::new(..)` (random UID) 409s on every update. That
//! took sig-network from 47/47 to 27/47 after #2108 (#2718); #2719 audited the
//! rest.
//!
//! The rule is syntactic and deliberately narrow: a function in
//! `src/controllers/` that mentions a fresh-identity constructor
//! (`ObjectMeta::new(`, `ObjectMeta {`, `EndpointSlice::new(`,
//! `Endpoints::new(`) AND an update verb (`.update(`, `update_status_cas(`,
//! `update_raw(`, `update_subresource(`) must call `adopt_existing_identity`.
//!
//! It cannot tell which object the update verb writes, so a function that
//! builds one object and updates another (re-read) one needs an entry in
//! `REVIEWED`, with the reason. That list is not an escape hatch for a real
//! hit: add to it only after reading the function.

use std::path::Path;

/// `(file, fn)` pairs that match the rule but were read and are safe.
const REVIEWED: &[(&str, &str, &str)] = &[
    (
        "deployment.rs",
        "create_replicaset_with_replicas",
        "creates the ReplicaSet; its one `update` writes a Deployment it just re-read",
    ),
    (
        "endpoints.rs",
        "reconcile_service",
        "updates `existing.clone()` (upstream's DeepCopy, endpoints_controller.go:469); \
         `ObjectMeta::new` is only the create-path default",
    ),
    (
        "statefulset.rs",
        "reconcile",
        "`Uuid::new_v4` is the UID of a ControllerRevision it CREATES; its one write \
         is `update_status_cas` of the StatefulSet this sync was handed",
    ),
];

const FRESH: &[&str] = &[
    "Uuid::new_v4(",
    "ObjectMeta::new(",
    "ObjectMeta {",
    "EndpointSlice::new(",
    "Endpoints::new(",
];
const WRITE: &[&str] = &[
    ".update(",
    "update_status_cas(",
    "update_raw(",
    "update_subresource(",
];

/// `(name, body)` of every fn, ended by the first line that closes at the
/// signature's own indentation. Test modules are cut off first.
fn fn_bodies(src: &str) -> Vec<(String, String)> {
    let src = src.split("#[cfg(test)]\nmod tests").next().unwrap_or(src);
    let lines: Vec<&str> = src.lines().collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        let indent = &line[..line.len() - trimmed.len()];
        let rest = trimmed
            .strip_prefix("pub(crate) ")
            .or_else(|| trimmed.strip_prefix("pub "))
            .unwrap_or(trimmed);
        let rest = rest.strip_prefix("async ").unwrap_or(rest);
        let Some(rest) = rest.strip_prefix("fn ") else {
            continue;
        };
        let name = rest
            .split(['(', '<'])
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
        let close = format!("{indent}}}");
        let mut body = String::new();
        for l in &lines[i..] {
            body.push_str(l);
            body.push('\n');
            if *l == close {
                break;
            }
        }
        out.push((name, body));
    }
    out
}

#[test]
fn a_controller_that_builds_fresh_and_updates_adopts_the_stored_identity() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/controllers");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("controllers dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "rs"))
        .collect();
    files.sort();

    let mut offenders = Vec::new();
    let mut scanned = 0;
    for path in files {
        let file = path.file_name().unwrap().to_string_lossy().to_string();
        let src = std::fs::read_to_string(&path).unwrap();
        for (name, body) in fn_bodies(&src) {
            scanned += 1;
            let fresh = FRESH.iter().any(|p| body.contains(p));
            let write = WRITE.iter().any(|p| body.contains(p));
            if fresh
                && write
                && !body.contains("adopt_existing_identity")
                && !REVIEWED.iter().any(|(f, n, _)| *f == file && *n == name)
            {
                offenders.push(format!("{file}::{name}"));
            }
        }
    }
    assert!(scanned > 500, "the scan found only {scanned} functions");
    assert!(
        offenders.is_empty(),
        "these controller functions build an object with a fresh identity and \
         write with update*, but never adopt the stored object's identity \
         (uid, creationTimestamp, finalizers, resourceVersion). The generic \
         Store turns a non-empty metadata.uid into a UID precondition \
         (rest/update.go:188-203), so every update 409s. Update a clone of the \
         object you read, as upstream's DeepCopy does: {offenders:#?}"
    );
}

/// Every non-test `.rs` file under `dir`, recursively, sorted.
fn rs_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).expect("read_dir").flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(rs_files(&path));
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let is_test_file = name == "tests.rs" || name.ends_with("_tests.rs");
        if name.ends_with(".rs") && !is_test_file {
            out.push(path);
        }
    }
    out.sort();
    out
}

/// `(path under crates/, fn, reason)` for functions outside `src/controllers/`
/// that match the rule but were read and are safe.
const REVIEWED_ELSEWHERE: &[(&str, &str, &str)] = &[
    (
        "api-server/src/bootstrap.rs",
        "bootstrap_default_rbac",
        "the api-server holds the StorageBackend directly: no Store, no UID precondition",
    ),
    (
        "api-server/src/bootstrap.rs",
        "reconcile_endpoints",
        "direct StorageBackend write: no Store, no UID precondition",
    ),
    (
        "api-server/src/bootstrap.rs",
        "reconcile_endpointslice",
        "direct StorageBackend write: no Store, no UID precondition",
    ),
    (
        "api-server/src/bootstrap.rs",
        "sync",
        "direct StorageBackend write: no Store, no UID precondition",
    ),
    (
        "api-server/src/bootstrap.rs",
        "sync_cluster_authentication_trust",
        "direct StorageBackend write of a re-read ConfigMap; no Store in front",
    ),
];

/// #2719 audited the writers outside controller-manager. Anything that PUTs
/// through `ApiStorage` (kubelet, scheduler, the shared event recorder) meets
/// the same Store precondition as a controller does, so the rule applies there
/// too. The api-server's own bootstrap holds the backend directly.
#[test]
fn other_components_that_build_fresh_and_update_adopt_the_stored_identity() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut files = rs_files(&crates.join("kubelet/src"));
    files.extend(rs_files(&crates.join("scheduler/src")));
    files.extend(rs_files(&crates.join("kube-proxy/src")));
    files.extend(rs_files(&crates.join("dns/src")));
    files.push(crates.join("storage/src/event_recorder.rs"));
    for f in [
        "bootstrap.rs",
        "apiserver_identity.rs",
        "legacy_token_tracking.rs",
    ] {
        files.push(crates.join("api-server/src").join(f));
    }

    let mut offenders = Vec::new();
    let mut scanned = 0;
    for path in &files {
        let rel = path
            .strip_prefix(&crates)
            .unwrap()
            .to_string_lossy()
            .to_string();
        let src = std::fs::read_to_string(path).unwrap();
        for (name, body) in fn_bodies(&src) {
            scanned += 1;
            let fresh = FRESH.iter().any(|p| body.contains(p));
            let write = WRITE.iter().any(|p| body.contains(p));
            let reviewed = REVIEWED_ELSEWHERE
                .iter()
                .any(|(f, n, _)| *f == rel && *n == name);
            if fresh && write && !body.contains("adopt_existing_identity") && !reviewed {
                offenders.push(format!("{rel}::{name}"));
            }
        }
    }
    assert!(scanned > 500, "the scan found only {scanned} functions");
    assert!(
        offenders.is_empty(),
        "these functions build an object with a fresh identity and write with \
         update*, but never adopt the stored object's identity. The generic \
         Store turns a non-empty metadata.uid into a UID precondition \
         (rest/update.go:188-203), so every update 409s. Update a clone of the \
         object you read, as upstream's DeepCopy does: {offenders:#?}"
    );
}
