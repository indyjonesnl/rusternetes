//! Guard: every workspace build script must declare its inputs.
//!
//! Cargo's documented fallback when a build script emits *no*
//! `cargo:rerun-if-changed` / `cargo:rerun-if-env-changed` instruction is to
//! re-run that script whenever **any** file in the package changes
//! (see the Cargo Book, "Build Scripts" → "Change Detection"). On a crate the
//! size of `api-server` that means a one-line edit to any source file re-runs
//! protobuf codegen before rustc even starts — pure warm-rebuild tax.
//!
//! `crates/cri/build.rs` always got this right; `crates/api-server/build.rs`
//! did not, which is what this test was written to catch. It is a build-speed
//! regression guard, not a correctness one: a new crate that adds a build
//! script without declaring inputs silently slows everyone's inner loop, and
//! nothing else in CI notices.
//!
//! Rust/Cargo-specific concern — there is no upstream Kubernetes equivalent to
//! port here.

use std::path::{Path, PathBuf};

/// Instructions that tell Cargo what a build script actually depends on.
/// Either one is enough to opt out of the "any file in the package" fallback.
const INPUT_DECLARATIONS: [&str; 2] = ["cargo:rerun-if-changed", "cargo:rerun-if-env-changed"];

fn crates_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR is crates/test_support; the workspace root is two up.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("test_support should live at <workspace>/crates/test_support")
        .join("crates")
}

/// Every `crates/*/build.rs` in the workspace, sorted for stable output.
fn build_scripts() -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(crates_dir())
        .expect("crates/ should be readable")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path().join("build.rs"))
        .filter(|path| path.is_file())
        .collect();
    found.sort();
    found
}

fn declares_its_inputs(source: &str) -> bool {
    INPUT_DECLARATIONS
        .iter()
        .any(|instruction| source.contains(instruction))
}

/// A build script that watches git metadata is dirtied by every commit, fetch,
/// and branch switch. On a crate that the whole workspace depends on — `common`
/// — that turns `git commit` into a full-workspace rebuild. Watching refs is
/// still the only way to keep a locally-stamped SHA honest, so it is allowed,
/// but only behind the `RUSTERNETES_STAMP_GIT` opt-in rather than on by default.
#[test]
fn git_metadata_is_only_watched_behind_the_opt_in() {
    let offenders: Vec<String> = build_scripts()
        .iter()
        .filter(|path| {
            let source = std::fs::read_to_string(path).expect("build.rs should be readable");
            let watches_git = source.contains("--git-path") || source.contains("watch_git_refs");
            watches_git && !source.contains("RUSTERNETES_STAMP_GIT")
        })
        .map(|path| path.display().to_string())
        .collect();

    assert!(
        offenders.is_empty(),
        "build script(s) watch git metadata unconditionally, so every commit \
         rebuilds their dependents:\n  {}\n\nGate the watch behind \
         `RUSTERNETES_STAMP_GIT` (see crates/common/build.rs).",
        offenders.join("\n  ")
    );
}

#[test]
fn every_build_script_declares_its_inputs() {
    let scripts = build_scripts();
    assert!(
        !scripts.is_empty(),
        "found no crates/*/build.rs — the discovery logic in this test is broken, \
         not the workspace"
    );

    let offenders: Vec<String> = scripts
        .iter()
        .filter(|path| {
            let source = std::fs::read_to_string(path).expect("build.rs should be readable");
            !declares_its_inputs(&source)
        })
        .map(|path| path.display().to_string())
        .collect();

    assert!(
        offenders.is_empty(),
        "build script(s) declare no inputs, so Cargo re-runs them on every edit \
         to any file in their package:\n  {}\n\nAdd a \
         `println!(\"cargo:rerun-if-changed=<input>\")` for each real input \
         (see crates/cri/build.rs).",
        offenders.join("\n  ")
    );
}
