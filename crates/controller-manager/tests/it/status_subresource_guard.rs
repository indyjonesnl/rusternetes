//! Guard: a controller that computes `.status` must persist it through the
//! status subresource.
//!
//! A conformant api-server **strips `.status` on a full-object PUT** — the
//! invariant already documented at `crates/storage/src/api_storage.rs:24-28`.
//! So a controller that assigns `.status = Some(...)` and then persists with
//! `Storage::update` silently loses that status in API mode, while appearing to
//! work in storage mode. That is why the controller-manager drop-in leg scored
//! 6/12 while the equivalent per-area conformance targets looked healthy.
//!
//! Upstream never does this: every controller writes status via the typed
//! client's `UpdateStatus` (e.g. `pkg/controller/job/job_controller.go`,
//! `pkg/controller/deployment/sync.go`).
//!
//! This is a source-level guard rather than a behavioural one on purpose. The
//! defect is *which method is called*, and `MemoryStorage` cannot tell the two
//! apart — it happily persists status either way, which is exactly how the bug
//! survived. The behavioural check is the drop-in leg against a real
//! api-server; this test is what stops a regression between those runs.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Controllers that assign `.status` but legitimately never persist it here,
/// with the reason. Keep this list short and justified — every entry is a
/// place the guard is blind.
const ALLOWED: &[(&str, &str)] = &[
    // Assigns status onto an object it then hands to another writer, rather
    // than persisting it itself.
    ("hpa_pod_grouping.rs", "helper: groups pods, never writes"),
];

fn controllers_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src/controllers")
}

/// Files that construct a status and write it back through `Storage::update`.
fn offenders() -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for entry in fs::read_dir(controllers_dir()).expect("read controllers dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .expect("file name")
            .to_string();
        if name == "mod.rs" || name.ends_with("_tests.rs") {
            continue;
        }
        let src = fs::read_to_string(&path).expect("read controller source");

        let assigns_status = src.contains("status = Some(");
        let writes_status = src.contains("update_status(");
        if assigns_status && !writes_status && !ALLOWED.iter().any(|(f, _)| *f == name) {
            out.insert(name);
        }
    }
    out
}

#[test]
fn controllers_that_compute_status_persist_it_via_the_status_subresource() {
    let offenders = offenders();
    assert!(
        offenders.is_empty(),
        "these controllers assign `.status` but never call `Storage::update_status`, \
         so their status is stripped by a conformant api-server (see #1723):\n  {}\n\n\
         Fix: persist status with `self.storage.update_status(&key, &obj)`. It performs \
         its own CAS read-modify-write and grafts only `.status`, so any manual \
         re-read-then-update dance around the call becomes redundant. \
         See `controllers/replicaset.rs` for the shape.",
        offenders.iter().cloned().collect::<Vec<_>>().join("\n  ")
    );
}

/// The allow-list must not rot: an entry naming a file that no longer exists
/// hides a controller that was renamed rather than fixed.
#[test]
fn status_guard_allow_list_has_no_stale_entries() {
    for (file, reason) in ALLOWED {
        assert!(
            controllers_dir().join(file).exists(),
            "allow-list entry `{file}` ({reason}) no longer exists — remove it"
        );
    }
}
