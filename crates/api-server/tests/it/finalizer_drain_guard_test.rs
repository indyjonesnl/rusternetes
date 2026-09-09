//! Guard: every write handler must finish a deletion the request drains.
//!
//! Upstream `Store.Update` consults `ShouldDeleteDuringUpdate`
//! (`registry/generic/registry/store.go:565`): a write that removes the last
//! finalizer from an object already carrying a `deletionTimestamp` completes
//! the deletion *as part of that same request*. There is one write path
//! upstream — PUT, PATCH and apply all funnel through that `Update` — so this
//! holds for every resource and every verb by construction.
//!
//! Rusternetes has one handler per resource per verb, each calling
//! `finalizers::finish_deletion_if_finalizers_drained` (PUT, stored object) or
//! `finalizers::finish_deletion_if_write_drained_finalizers` (PATCH, patched +
//! pre-write object) after it persists.
//!
//! Both verbs are in scope, and the PATCH half is the one that matters most:
//! upstream's garbage collector removes a finalizer with a JSON **merge
//! patch** — `removeFinalizer` builds `objectForFinalizersPatch` and sends it
//! as `types.MergePatchType`
//! (`pkg/controller/garbagecollector/operations.go:104-146`). This guard used
//! to *say* it covered `patch*` while its parser only matched
//! `pub async fn update`, so six PATCH handlers had no check at all and a
//! foreground delete driven by a real GC never completed (#1919).
//!
//! **No allowlist.** A handler that cannot finish the deletion is a reason to
//! change the handler or the helper, not to record a name here. The two exits
//! below are keyed on mechanism: a subresource cannot carry
//! `metadata.finalizers`, and a handler that delegates to `generic_patch`
//! inherits the check from it.

use std::path::{Path, PathBuf};

/// Extract `(name, body)` for each `pub async fn update*` / `pub async fn
/// patch*` in rustfmt output, where a top-level item closes with a bare `}` in
/// column 0.
fn write_fn_bodies(src: &str) -> Vec<(String, String)> {
    let lines: Vec<&str> = src.lines().collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let (verb, rest) = match line
            .strip_prefix("pub async fn update")
            .map(|r| ("update", r))
            .or_else(|| {
                line.strip_prefix("pub async fn patch")
                    .map(|r| ("patch", r))
            }) {
            Some(m) => m,
            None => continue,
        };
        let name = format!(
            "{verb}{}",
            rest.split(['(', '<']).next().unwrap_or("").trim()
        );
        let mut body = String::new();
        for (n, l) in lines[i..].iter().enumerate() {
            body.push_str(l);
            body.push('\n');
            if n > 0 && *l == "}" {
                break;
            }
        }
        out.push((name, body));
    }
    out
}

fn handler_files() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/handlers");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("handlers dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "rs"))
        .collect();
    files.sort();
    files
}

#[test]
fn every_write_handler_finishes_a_drained_deletion() {
    let mut offenders = Vec::new();
    let mut checked = 0usize;
    let mut patches_checked = 0usize;

    for path in handler_files() {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        // The helper's own home, and the shared status/scale machinery.
        if name == "finalizers.rs" || name == "lifecycle.rs" {
            continue;
        }
        let src = std::fs::read_to_string(&path).expect("read handler");
        let src = src.split("\n#[cfg(test)]").next().unwrap_or("").to_string();

        for (fname, body) in write_fn_bodies(&src) {
            // A status, scale or other subresource cannot drain a finalizer:
            // upstream gives those their own strategy and `metadata.finalizers`
            // is not part of the subresource. Keyed on the name of the
            // *subresource*, which is the structural fact, not on which
            // resource it belongs to.
            if fname.ends_with("_status")
                || fname.ends_with("_scale")
                || fname.contains("_subresource")
            {
                continue;
            }
            // Only handlers that actually persist are in scope.
            if !body.contains("storage.update(") && !body.contains("update_inheriting") {
                continue;
            }
            // A handler that delegates to the generic patch implementation
            // inherits the check from it — that IS the shared mechanism, not an
            // exemption. `generic_patch.rs` itself is still measured, because
            // its own two functions appear in this scan.
            if name != "generic_patch.rs"
                && (body.contains("generic_patch::patch_namespaced_resource")
                    || body.contains("generic_patch::patch_cluster_resource"))
            {
                continue;
            }
            checked += 1;
            if fname.starts_with("patch") {
                patches_checked += 1;
            }
            if !body.contains("finish_deletion_if_finalizers_drained")
                && !body.contains("finish_deletion_if_write_drained_finalizers")
            {
                offenders.push(format!("{name}::{fname}"));
            }
        }
    }

    assert!(
        checked >= 30,
        "guard scanned only {checked} write handlers -- the parser stopped \
         matching, which would make this test vacuously green"
    );
    assert!(
        patches_checked >= 4,
        "guard scanned {patches_checked} PATCH handlers -- the `patch*` half of \
         the parser stopped matching, which is exactly the hole that let #1919 \
         ship: the GC removes finalizers with a merge PATCH, not a PUT"
    );

    assert!(
        offenders.is_empty(),
        "{} write handler(s) persist without finishing a deletion the request \
         drained, so a PUT or PATCH that removes the last finalizer from an \
         object pending deletion leaves it in storage with nothing left to \
         delete it. Upstream completes the deletion in the same request via \
         ShouldDeleteDuringUpdate (registry/generic/registry/store.go:565), for \
         every resource and every verb.\n\n\
         Offenders:\n  {}",
        offenders.len(),
        offenders.join("\n  ")
    );
}
