//! Guard: a LIST's `metadata.resourceVersion` must come from
//! `handlers::list_collection_resource_version`, never from a bare
//! `storage.current_revision()`.
//!
//! Upstream cannot express this bug. A list and its `ResourceVersion` come from
//! ONE etcd range response — `storage/etcd3.GetList` stamps
//! `getResp.Header.Revision` — so the RV is, by construction, the revision the
//! snapshot was taken at.
//!
//! Here the store revision and the items are read separately, so a write that
//! lands between the two reads makes the list carry an item whose
//! `resourceVersion` is ABOVE the list's own. A client that then watches from
//! the list RV re-receives that item, and a client that treats the list RV as a
//! high-water mark misses events. #1825 introduced the helper that takes the
//! max of the two; twelve list handlers were never converted to it.
//!
//! The bare pattern also invented a fallback:
//!
//! ```ignore
//! let resource_version = match state.storage.current_revision().await {
//!     Ok(rev) => rev.to_string(),
//!     Err(_) => "1".to_string(),
//! };
//! ```
//!
//! On a storage error that hands the client `"1"` — a real, very old revision
//! that a watch will happily accept and then replay from, or 410 on.
//!
//! **No allowlist**, deliberately: see `delete_propagation_guard_test`.
//!
//! WATCH handlers are out of scope and excluded by the rule below, not by an
//! exception list: a watch's start revision legitimately IS the store revision.
//! It is not a floor over a returned item set, because a watch returns no item
//! set.

use std::path::{Path, PathBuf};

fn fn_bodies(src: &str) -> Vec<(String, String)> {
    let lines: Vec<&str> = src.lines().collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let sig = line
            .strip_prefix("pub async fn ")
            .or_else(|| line.strip_prefix("async fn "));
        let Some(rest) = sig else { continue };
        let name = rest
            .split(['(', '<'])
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
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

/// A function is on the LIST path if it is named like a list handler or if it
/// paginates a collection. Watch handlers match neither.
fn is_list_path(name: &str, body: &str) -> bool {
    name.starts_with("list") || body.contains("paginate(")
}

#[test]
fn no_list_handler_stamps_its_resource_version_from_a_bare_store_revision() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/handlers");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("handlers dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "rs"))
        .collect();
    files.sort();

    let mut offenders = Vec::new();
    let mut list_fns = 0usize;

    for path in files {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        // mod.rs DEFINES the sanctioned helper, which necessarily calls it.
        if name == "mod.rs" {
            continue;
        }
        let src = std::fs::read_to_string(&path).expect("read handler");
        let src = src.split("\n#[cfg(test)]").next().unwrap_or("").to_string();

        for (fname, body) in fn_bodies(&src) {
            if !is_list_path(&fname, &body) {
                continue;
            }
            list_fns += 1;
            if body.contains("current_revision()") {
                offenders.push(format!("{name}::{fname}"));
            }
        }
    }

    assert!(
        list_fns >= 40,
        "guard scanned only {list_fns} list functions -- the parser stopped \
         matching, which would make this test vacuously green"
    );

    assert!(
        offenders.is_empty(),
        "{} list handler(s) stamp `metadata.resourceVersion` from a bare \
         `storage.current_revision()` instead of \
         `handlers::list_collection_resource_version`, so the list RV can land \
         BELOW an item the same list returned (#1825). Upstream takes both from \
         one etcd range response.\n\nOffenders:\n  {}",
        offenders.len(),
        offenders.join("\n  ")
    );
}
