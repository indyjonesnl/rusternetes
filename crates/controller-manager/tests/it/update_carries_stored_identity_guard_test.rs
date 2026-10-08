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
    // EndpointSlice is fixed by #2718, which owns endpointslice.rs. Delete
    // these three entries when it merges (#2719).
    ("endpointslice.rs", "mirror_endpoint", "fixed by #2718"),
    ("endpointslice.rs", "reconcile_all", "fixed by #2718"),
    (
        "endpointslice.rs",
        "reconcile_service_with_pods",
        "fixed by #2718",
    ),
];

const FRESH: &[&str] = &[
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
