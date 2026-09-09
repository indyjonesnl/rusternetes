//! Guard: every delete handler must pass the request's DeleteOptions to the
//! shared finalizer helper, so `propagationPolicy` works on every resource.
//!
//! Upstream cannot have a per-resource gap here: `Store.Delete` calls
//! `deletionFinalizersForGarbageCollection`
//! (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:976)
//! for every resource, with no kind check. Rusternetes has one delete handler
//! per resource, and only 8 of ~50 passed a policy before this guard existed.
//!
//! The scope rule matters as much as the check. This guard originally examined
//! only handlers that *already* called the shared helper, so a handler which
//! bypassed finalizers entirely — `storage.delete(&key)` and nothing else — was
//! skipped rather than flagged. That is an allowlist wearing a `continue`, and
//! it hid eight DRA delete paths (#1895). Anything that deletes an object is in
//! scope now.
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
            // Matches the `_json` variants too — they are thin wrappers over
            // the same helper (see `JsonResource`).
            let uses_helper = body.contains("handle_delete_with_finalizers")
                || body.contains("delete_collection_item");
            // A raw `storage.delete(...)` is the bypass shape: it removes the
            // object outright, so finalizers never run and propagationPolicy
            // is meaningless.
            let deletes_directly = body.contains("storage.delete(");

            // Anything that deletes an object is in scope. This used to
            // `continue` unless the handler already called the helper, which
            // made the guard's scope depend on the very property it checks: a
            // handler that bypassed finalizers altogether was skipped, not
            // flagged. Eight DRA delete paths sat green that way until #1895.
            if !uses_helper && !deletes_directly {
                continue;
            }
            checked += 1;

            // Two structural reasons a handler may delete without the shared
            // helper. Both are keyed on a *mechanism visible in the code*, not
            // on a handler name — a name list is the allowlist this guard
            // exists to avoid.
            //
            // 1. It implements the finalizing/graceful path itself, which it
            //    proves by assigning `metadata.deletion_timestamp`. Upstream
            //    has exactly these: Pod is its only
            //    `RESTGracefulDeleteStrategy`, and namespaces finalize through
            //    `spec.finalizers`.
            let handles_deletion_itself = body.contains("deletion_timestamp = Some(");
            //
            // There used to be a second reason here: a resource stored as an
            // untyped `serde_json::Value` could not implement `HasMetadata`, so
            // the typed helper did not apply. That was a real gap, not an
            // exemption, and #1911 closed it — `JsonResource` adapts a document
            // to `HasMetadata` and `handle_delete_with_finalizers_json` /
            // `delete_collection_item_json` run the same propagation logic. An
            // untyped handler that deletes directly is now an offender like any
            // other.

            if deletes_directly && !uses_helper && !handles_deletion_itself {
                offenders.push(format!(
                    "{name}::{fname} (deletes via storage.delete() without the \
                     shared finalizer helper)"
                ));
                continue;
            }
            if deletes_directly && !uses_helper {
                // In scope and accounted for; nothing further to check.
                continue;
            }
            if !body.contains("&delete_opts") && !body.contains("delete_opts,") {
                offenders.push(format!(
                    "{name}::{fname} (helper called without &delete_opts)"
                ));
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
