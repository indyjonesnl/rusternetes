//! Guard: every file in a consolidated `tests/it/` directory is actually wired
//! into that crate's `tests/it/main.rs`.
//!
//! The consolidated layout trades one hazard for a lot of build speed. With the
//! old one-target-per-file layout, dropping a new `tests/foo.rs` into a crate
//! was enough — Cargo discovered and ran it. Under `tests/it/`, a file that
//! nobody adds a `mod` line for is not compiled and not run, and *nothing*
//! complains: the suite stays green because those tests simply do not exist.
//! Silently-dead tests are worse than slow tests, so this asserts the wiring.
//!
//! Rust/Cargo-specific layout concern; nothing to port from upstream
//! Kubernetes.

use std::path::{Path, PathBuf};

fn crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("test_support should live at <workspace>/crates/test_support")
        .join("crates")
}

/// Every `crates/*/tests/it` directory that has a `main.rs` target root.
fn consolidated_test_dirs() -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(crates_dir())
        .expect("crates/ should be readable")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path().join("tests").join("it"))
        .filter(|dir| dir.join("main.rs").is_file())
        .collect();
    found.sort();
    found
}

/// Module names declared by a `main.rs`, taken from `mod <name>;` lines.
fn declared_modules(main_rs: &str) -> Vec<String> {
    main_rs
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("mod "))
        .filter_map(|rest| rest.strip_suffix(';'))
        .map(|name| name.trim().to_string())
        .collect()
}

/// Sibling `*.rs` files that ought to be declared, i.e. everything but the
/// target root itself.
fn module_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("tests/it should be readable")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .filter_map(|path| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .map(str::to_string)
        })
        .filter(|stem| stem != "main")
        .collect();
    names.sort();
    names
}

#[test]
fn every_it_module_file_is_declared() {
    let dirs = consolidated_test_dirs();
    assert!(
        !dirs.is_empty(),
        "found no crates/*/tests/it/main.rs — either the consolidated layout is \
         gone or the discovery logic in this test is broken"
    );

    let mut problems = Vec::new();

    for dir in &dirs {
        let main_rs = std::fs::read_to_string(dir.join("main.rs")).expect("main.rs is readable");
        let declared = declared_modules(&main_rs);
        let files = module_files(dir);

        for file in &files {
            if !declared.contains(file) {
                problems.push(format!(
                    "{}: `{}.rs` exists but no `mod {};` line in main.rs — \
                     its tests never run",
                    dir.display(),
                    file,
                    file
                ));
            }
        }

        for name in &declared {
            if !files.contains(name) {
                problems.push(format!(
                    "{}: main.rs declares `mod {};` but {}.rs is missing",
                    dir.display(),
                    name,
                    name
                ));
            }
        }
    }

    assert!(
        problems.is_empty(),
        "consolidated test layout is out of sync:\n  {}",
        problems.join("\n  ")
    );
}

/// A stray `tests/*.rs` next to `tests/it/` silently reintroduces the very
/// per-file test binary the consolidation removed. Cheap to detect, so detect
/// it rather than letting the win erode a file at a time.
#[test]
fn consolidated_crates_have_no_stray_top_level_test_files() {
    let strays: Vec<String> = consolidated_test_dirs()
        .iter()
        .filter_map(|it_dir| it_dir.parent().map(Path::to_path_buf))
        .flat_map(|tests_dir| {
            std::fs::read_dir(&tests_dir)
                .expect("tests/ should be readable")
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
        })
        .collect();

    assert!(
        strays.is_empty(),
        "these files are separate test binaries again; move them into \
         `tests/it/` and add a `mod` line:\n  {}",
        strays.join("\n  ")
    );
}
