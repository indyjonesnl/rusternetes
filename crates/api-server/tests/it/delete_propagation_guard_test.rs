//! Guard: every delete handler must pass the request's DeleteOptions to the
//! shared finalizer helper, so `propagationPolicy` works on every resource.
//!
//! Upstream cannot have a per-resource gap here: `Store.Delete` calls
//! `deletionFinalizersForGarbageCollection`
//! (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:976)
//! for every resource, with no kind check. Rusternetes has one delete handler
//! per resource, and only 8 of ~50 passed a policy before this guard existed.
//!
//! **This guard has no allowlist, deliberately.** The PUT-metadata rule shipped
//! with an allowlist of known-broken handlers, and the test then sat green for
//! 21 handlers across three "fixes" (#1788, #1793, #1795) until #1896 drained
//! it. A guard that records exceptions measures nothing. If a handler genuinely
//! cannot pass the options, that is a reason to change the handler or the
//! helper, not to add a line here.

use std::path::{Path, PathBuf};

/// Reuses the literal-stripping brace matcher's contract: a top-level item in
/// rustfmt output closes with a bare `}` in column 0.
fn delete_fn_bodies(src: &str) -> Vec<(String, String)> {
    let lines: Vec<&str> = src.lines().collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let Some(rest) = line.strip_prefix("pub async fn delete") else {
            continue;
        };
        let name = format!(
            "delete{}",
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

#[test]
fn every_delete_handler_passes_the_request_delete_options() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/handlers");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("handlers dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "rs"))
        .collect();
    files.sort();

    let mut offenders = Vec::new();
    let mut checked = 0usize;

    for path in files {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        // The helper itself, and the eviction path, build their own context.
        if name == "finalizers.rs" {
            continue;
        }
        let src = std::fs::read_to_string(&path).expect("read handler");
        let src = src.split("\n#[cfg(test)]").next().unwrap_or("").to_string();

        for (fname, body) in delete_fn_bodies(&src) {
            // Only handlers that actually route through the shared helper are
            // in scope; a handler with bespoke deletion (namespaces) is not.
            if !body.contains("handle_delete_with_finalizers")
                && !body.contains("delete_collection_item")
            {
                continue;
            }
            checked += 1;
            if !body.contains("&delete_opts") && !body.contains("delete_opts,") {
                offenders.push(format!("{name}::{fname}"));
            }
        }
    }

    assert!(
        checked >= 40,
        "guard scanned only {checked} delete handlers -- the parser stopped \
         matching, which would make this test vacuously green"
    );

    assert!(
        offenders.is_empty(),
        "{} delete handler(s) call the shared finalizer helper without passing \
         the request's DeleteOptions, so `propagationPolicy` and \
         `orphanDependents` are silently ignored for those resources. Upstream \
         applies them to every resource in \
         registry/generic/registry/store.go:976 with no kind check. Take \
         `Extension(delete_opts): Extension<rusternetes_middleware::DeleteOptionsCtx>` \
         and pass `&delete_opts`.\n\nOffenders:\n  {}",
        offenders.len(),
        offenders.join("\n  ")
    );
}
