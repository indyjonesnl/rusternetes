//! Smoke test: the vendored upstream Kubernetes 1.35 golden oracles are
//! present and well-formed. See `tests/upstream/README.md`. No harness logic
//! lives here; fixture-driven round-trip tests build on these files.
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/upstream/k8s-1.35")
}

fn files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_file())
        .collect();
    v.sort();
    v
}

/// Every `.pb` has a sibling `.json` and `.yaml` (upstream roundtrip triple).
fn assert_triples(dir: &Path) {
    let stems = |ext: &str| -> BTreeSet<String> {
        files(dir)
            .iter()
            .filter(|p| p.extension().is_some_and(|e| e == ext))
            .map(|p| p.file_stem().unwrap().to_string_lossy().into_owned())
            .collect()
    };
    let (pb, json, yaml) = (stems("pb"), stems("json"), stems("yaml"));
    assert!(!pb.is_empty(), "no .pb in {}", dir.display());
    assert_eq!(pb, json, "pb/json mismatch in {}", dir.display());
    assert_eq!(pb, yaml, "pb/yaml mismatch in {}", dir.display());
}

#[test]
fn api_testdata_is_present() {
    let dir = root().join("api-testdata");
    assert_eq!(
        files(&dir).len(),
        549,
        "upstream api testdata/HEAD file count"
    );
    assert_triples(&dir);
}

#[test]
fn apiextensions_testdata_is_present() {
    assert_triples(&root().join("apiextensions-testdata"));
}

#[test]
fn swagger_json_has_definitions() {
    let text = fs::read_to_string(root().join("openapi-spec/swagger.json")).expect("swagger.json");
    let v: serde_json::Value = serde_json::from_str(&text).expect("swagger.json parses");
    assert!(
        v["definitions"].is_object(),
        "swagger.json has `definitions`"
    );
}

#[test]
fn openapi_v3_files_parse() {
    let mut n = 0;
    for p in files(&root().join("openapi-spec/v3")) {
        if p.extension().is_some_and(|e| e == "json") {
            let text = fs::read_to_string(&p).unwrap();
            serde_json::from_str::<serde_json::Value>(&text)
                .unwrap_or_else(|e| panic!("{} does not parse: {e}", p.display()));
            n += 1;
        }
    }
    assert!(n > 0, "no v3 json files");
}
