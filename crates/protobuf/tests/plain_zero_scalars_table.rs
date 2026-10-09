//! #2931: `src/plain_zero_scalars.rs` is generated from upstream `types.go` by
//! `scripts/gen-protobuf-plain-zero-scalars.py`. Fail when the committed table
//! drifts from what the generator emits.
//!
//! Needs a `kubernetes` checkout (release-1.35) next to the repo, or
//! `K8S_SRC`, plus `python3`; skips with a message when either is absent (CI
//! runners do not carry `../kubernetes`).
use std::path::Path;
use std::process::Command;

#[test]
fn committed_table_matches_generator_output() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let script = root.join("scripts/gen-protobuf-plain-zero-scalars.py");
    let out = match Command::new("python3")
        .arg(&script)
        .arg("--stdout")
        .output()
    {
        Ok(o) => o,
        Err(e) => {
            eprintln!("SKIP: python3 unavailable: {e}");
            return;
        }
    };
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        if err.contains("kubernetes checkout not found") {
            eprintln!("SKIP: no kubernetes checkout (set K8S_SRC): {err}");
            return;
        }
        panic!("generator failed: {err}");
    }
    let generated = String::from_utf8(out.stdout).unwrap();
    let committed = include_str!("../src/plain_zero_scalars.rs");
    assert_eq!(
        committed, generated,
        "crates/protobuf/src/plain_zero_scalars.rs drifted; run \
         scripts/gen-protobuf-plain-zero-scalars.py"
    );
}
