//! Guard: every boolean QUERY PARAMETER must be decoded with
//! `rusternetes_common::query::k8s_query_bool`, never a hand-rolled
//! comparison or a strict `parse::<bool>()`.
//!
//! This exists because the same defect kept being fixed in one place and
//! missed in dozens of others. When it was found, the codebase had:
//!
//!   * 54 sites parsing `allowWatchBookmarks` with `parse::<bool>()`
//!   * 54 sites parsing `sendInitialEvents` the same way
//!   *  6 sites for `force`, 1 for `orphanDependents`
//!   *  4 hand-rolled `== "true" || == "1"` for stdin/stdout/stderr/tty
//!   *  1 `is_true` helper in the kubelet for the pod-log parameters
//!
//! All of them silently under-accepted: upstream decodes these with
//! `runtime.Convert_Slice_string_To_bool`
//! (`apimachinery/pkg/runtime/conversion.go:79-95`), where ONLY absence, `"0"`
//! and a case-insensitive `"false"` are false and *every other value* --
//! including `"1"`, `"t"`, `"yes"` and the empty string -- is true. So
//! `?watch=1` became a plain list, `?force=1` a non-forced apply, and
//! `?stdin=t` no stdin at all.
//!
//! A unit test per call site is not feasible and would not have helped: the
//! problem was never a wrong line, it was an unenumerated set of lines. This
//! scans the source instead, so the next occurrence fails CI rather than
//! shipping.

use std::path::{Path, PathBuf};

/// The boolean query parameters upstream decodes with
/// `Convert_Slice_string_To_bool` / `Convert_Slice_string_To_Pointer_bool`,
/// enumerated from the generated conversions rather than by memory.
const BOOL_QUERY_PARAMS: &[&str] = &[
    "watch",
    "allowWatchBookmarks",
    "sendInitialEvents",
    "force",
    "orphanDependents",
    "follow",
    "previous",
    "timestamps",
    "insecureSkipTLSVerifyBackend",
    "stdin",
    "stdout",
    "stderr",
    "tty",
    "ignoreStoreReadErrorWithClusterBreakingPotential",
];

fn crates_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is crates/common
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .to_path_buf()
}

fn rust_sources(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if p.is_dir() {
            // Skip build output, vendored code, and test trees (tests may
            // legitimately spell out a bad value to assert it is handled).
            if matches!(name, "target" | "tests" | ".git" | "proto" | "third_party") {
                continue;
            }
            rust_sources(&p, out);
        } else if name.ends_with(".rs") {
            out.push(p);
        }
    }
}

/// A line is suspicious if it decodes a bool with a narrow rule. We only flag
/// it when a nearby line names one of the known bool query parameters, so
/// unrelated bool parsing (config files, annotations, CLI flags) is untouched.
fn offending_lines(src: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = src.lines().collect();
    let mut hits = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let narrow = line.contains("parse::<bool>")
            || line.contains(r#"== "true""#)
            || line.contains(r#"== "1""#);
        if !narrow {
            continue;
        }
        // Look at this line and the 3 above it for a bool query param name.
        let lo = i.saturating_sub(3);
        let ctx = lines[lo..=i].join("\n");
        if BOOL_QUERY_PARAMS
            .iter()
            .any(|k| ctx.contains(&format!("\"{k}\"")))
        {
            hits.push((i + 1, line.trim().to_string()));
        }
    }
    hits
}

#[test]
fn bool_query_params_use_the_shared_decoder() {
    let mut files = Vec::new();
    rust_sources(&crates_root(), &mut files);
    assert!(
        files.len() > 50,
        "source scan found only {} files — the walk is broken, not the code",
        files.len()
    );

    let mut offenders = Vec::new();
    for f in &files {
        let Ok(src) = std::fs::read_to_string(f) else {
            continue;
        };
        for (line, text) in offending_lines(&src) {
            let rel = f
                .strip_prefix(crates_root())
                .unwrap_or(f)
                .display()
                .to_string();
            offenders.push(format!("  {rel}:{line}: {text}"));
        }
    }

    assert!(
        offenders.is_empty(),
        "these decode a boolean QUERY PARAMETER with a narrow rule instead of \
         rusternetes_common::query::k8s_query_bool.\n\nUpstream \
         (apimachinery/pkg/runtime/conversion.go:79-95) treats ONLY absence, \
         \"0\" and a case-insensitive \"false\" as false; every other value, \
         including \"1\", \"t\", \"yes\" and \"\", is true. A `parse::<bool>()` \
         or `== \"true\"` here silently drops those.\n\n{}\n",
        offenders.join("\n")
    );
}

#[test]
fn the_guard_actually_detects_a_regression() {
    // Proves the scanner is not vacuously green: a realistic reintroduction
    // must be flagged, and unrelated bool parsing must not be.
    let bad = r#"
        let allow = params
            .get("allowWatchBookmarks")
            .and_then(|v| v.parse::<bool>().ok());
    "#;
    assert_eq!(
        offending_lines(bad).len(),
        1,
        "the guard must flag a strict parse on a bool query param"
    );

    let also_bad = r#"    "tty" => tty = value == "true" || value == "1","#;
    assert_eq!(
        offending_lines(also_bad).len(),
        1,
        "the guard must flag a hand-rolled comparison on a bool query param"
    );

    let unrelated = r#"
        let debug = std::env::var("RUSTERNETES_DEBUG")
            .ok()
            .and_then(|v| v.parse::<bool>().ok());
        let is_default = ann.get("storageclass.kubernetes.io/is-default-class") == Some("true");
    "#;
    assert!(
        offending_lines(unrelated).is_empty(),
        "env vars and annotations are a different contract and must not be flagged"
    );
}
