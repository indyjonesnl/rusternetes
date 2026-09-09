//! Guard: every update handler must finish a deletion the request drains.
//!
//! Upstream `Store.Update` consults `ShouldDeleteDuringUpdate`
//! (`registry/generic/registry/store.go:565`): an update that removes the last
//! finalizer from an object already carrying a `deletionTimestamp` completes
//! the deletion *as part of that same request*. There is one update path
//! upstream, so this holds for every resource by construction.
//!
//! Rusternetes has one update handler per resource, each calling
//! `finalizers::finish_deletion_if_finalizers_drained` after it persists. 34
//! handler files did; the four DRA ones did not, and nothing measured it — so
//! a DRA object whose last finalizer was drained stayed in storage forever,
//! `deletionTimestamp` set and nothing left to remove it (#1895).
//!
//! **No allowlist.** A handler that cannot finish the deletion is a reason to
//! change the handler or the helper, not to record a name here.

use std::path::{Path, PathBuf};

/// Extract `(name, body)` for each `pub async fn update*` / `pub async fn
/// patch*` in rustfmt output, where a top-level item closes with a bare `}` in
/// column 0.
fn update_fn_bodies(src: &str) -> Vec<(String, String)> {
    let lines: Vec<&str> = src.lines().collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let rest = match line.strip_prefix("pub async fn update") {
            Some(r) => r,
            None => continue,
        };
        let name = format!(
            "update{}",
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
fn every_update_handler_finishes_a_drained_deletion() {
    let mut offenders = Vec::new();
    let mut checked = 0usize;

    for path in handler_files() {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        // The helper's own home, and the shared status/scale machinery.
        if name == "finalizers.rs" || name == "lifecycle.rs" {
            continue;
        }
        let src = std::fs::read_to_string(&path).expect("read handler");
        let src = src.split("\n#[cfg(test)]").next().unwrap_or("").to_string();

        for (fname, body) in update_fn_bodies(&src) {
            // A status or scale subresource cannot drain a finalizer: upstream
            // gives those their own strategy, and metadata.finalizers is not
            // part of the subresource. Keyed on the name of the *subresource*,
            // which is the structural fact, not on which resource it belongs
            // to.
            if fname.ends_with("_status") || fname.ends_with("_scale") {
                continue;
            }
            // Only handlers that actually persist are in scope.
            if !body.contains("storage.update(") && !body.contains("update_inheriting") {
                continue;
            }
            checked += 1;
            if !body.contains("finish_deletion_if_finalizers_drained") {
                offenders.push(format!("{name}::{fname}"));
            }
        }
    }

    assert!(
        checked >= 30,
        "guard scanned only {checked} update handlers -- the parser stopped \
         matching, which would make this test vacuously green"
    );

    assert!(
        offenders.is_empty(),
        "{} update handler(s) persist without calling \
         `finalizers::finish_deletion_if_finalizers_drained`, so a PUT that \
         removes the last finalizer from an object pending deletion leaves it \
         in storage with nothing left to delete it. Upstream completes the \
         deletion in the same request via ShouldDeleteDuringUpdate \
         (registry/generic/registry/store.go:565), for every resource.\n\n\
         Offenders:\n  {}",
        offenders.len(),
        offenders.join("\n  ")
    );
}
