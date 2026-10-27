//! OpenAPI golden comparison: our served `/openapi/v2` and `/openapi/v3/*`
//! documents against the vendored upstream release-1.35 specs
//! (`tests/upstream/k8s-1.35/openapi-spec/{swagger.json,v3/}`), always run.
//!
//! Upstream mechanism ported:
//! * `test/integration/apiserver/openapi/openapi_test.go`
//!   `TestOpenAPIV3SpecRoundTrip` / `TestOpenAPIRoundtrip`: every served
//!   document must match the checked-in spec under `api/openapi-spec`.
//! * `staging/src/k8s.io/kube-openapi/pkg/builder` (`x-kubernetes-group-version-kind`
//!   is how a definition is tied to its GVK).
//!
//! Unit of comparison: one (spec, group/version, Kind) per vendored definition
//! carrying `x-kubernetes-group-version-kind`. The schema tree of both sides is
//! walked with `$ref`s resolved and compared on: `type`, `format`, property
//! name sets, `required`, `items`, `additionalProperties` and every
//! `x-kubernetes-*` extension (descriptions are ignored). Known gaps live in
//! `upstream_openapi_golden_allowlist.txt` (xfail: a listed unit that now
//! matches fails the test). `GOLDEN_DUMP=1` prints every mismatch.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const ALLOWLIST: &str = include_str!("upstream_openapi_golden_allowlist.txt");

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/upstream/k8s-1.35/openapi-spec")
}

type Defs = Map<String, Value>;

/// Resolve `$ref` (v2 `#/definitions/X`, v3 `#/components/schemas/X`), v3's
/// `allOf: [{$ref}]` single-ref wrapper, and report the target name.
fn resolve<'a>(defs: &'a Defs, mut node: &'a Value) -> (&'a Value, Option<String>) {
    let mut name = None;
    for _ in 0..16 {
        let r = node.get("$ref").and_then(Value::as_str).or_else(|| {
            let all = node.get("allOf")?.as_array()?;
            (all.len() == 1).then(|| all[0].get("$ref")?.as_str())?
        });
        let Some(r) = r else { break };
        let n = r.rsplit('/').next().unwrap_or(r);
        match defs.get(n) {
            Some(t) => {
                name = Some(n.to_string());
                node = t;
            }
            None => break,
        }
    }
    (node, name)
}

fn ext_keys(n: &Value) -> BTreeMap<String, Value> {
    n.as_object()
        .into_iter()
        .flatten()
        .filter(|(k, _)| k.starts_with("x-kubernetes-") && *k != "x-kubernetes-group-version-kind")
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn required(n: &Value) -> BTreeSet<String> {
    n["required"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(String::from))
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn cmp(
    ours: &Defs,
    theirs: &Defs,
    o: &Value,
    t: &Value,
    path: &str,
    seen: &mut BTreeSet<(String, String)>,
    depth: usize,
    out: &mut Vec<String>,
) {
    if depth > 14 || out.len() > 400 {
        return;
    }
    let (o, on) = resolve(ours, o);
    let (t, tn) = resolve(theirs, t);
    // Break recursion on a (ours-name, theirs-name) pair already being compared.
    if let (Some(a), Some(b)) = (on, tn) {
        if !seen.insert((a, b)) {
            return;
        }
    }
    for k in ["type", "format"] {
        let (a, b) = (o.get(k), t.get(k));
        if a != b {
            out.push(format!("{path}: {k} ours {} upstream {}", sh(a), sh(b)));
        }
    }
    let (ro, rt) = (required(o), required(t));
    if ro != rt {
        out.push(format!("{path}: required ours {ro:?} upstream {rt:?}"));
    }
    let (eo, et) = (ext_keys(o), ext_keys(t));
    if eo != et {
        out.push(format!(
            "{path}: x-kubernetes-* ours {:?} upstream {:?}",
            eo.keys().collect::<Vec<_>>(),
            et.keys().collect::<Vec<_>>()
        ));
    }
    let po = o["properties"].as_object();
    let pt = t["properties"].as_object();
    let names = |p: Option<&Map<String, Value>>| -> BTreeSet<String> {
        p.map(|m| m.keys().cloned().collect()).unwrap_or_default()
    };
    let (no, nt) = (names(po), names(pt));
    for missing in nt.difference(&no) {
        out.push(format!("{path}/{missing}: property missing in ours"));
    }
    for extra in no.difference(&nt) {
        out.push(format!("{path}/{extra}: property not in upstream"));
    }
    if let (Some(po), Some(pt)) = (po, pt) {
        for (k, tv) in pt {
            if let Some(ov) = po.get(k) {
                cmp(
                    ours,
                    theirs,
                    ov,
                    tv,
                    &format!("{path}/{k}"),
                    seen,
                    depth + 1,
                    out,
                );
            }
        }
    }
    for k in ["items", "additionalProperties"] {
        if let (Some(a), Some(b)) = (o.get(k), t.get(k)) {
            if a.is_object() && b.is_object() {
                cmp(
                    ours,
                    theirs,
                    a,
                    b,
                    &format!("{path}/{k}"),
                    seen,
                    depth + 1,
                    out,
                );
            }
        } else if o.get(k).is_some() != t.get(k).is_some() {
            out.push(format!(
                "{path}: {k} present ours {} upstream {}",
                o.get(k).is_some(),
                t.get(k).is_some()
            ));
        }
    }
}

fn sh(v: Option<&Value>) -> String {
    v.map_or("<absent>".into(), |v| v.to_string())
}

/// `(group, version, kind)` of a definition's `x-kubernetes-group-version-kind`.
fn gvks(def: &Value) -> Vec<(String, String, String)> {
    def["x-kubernetes-group-version-kind"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|g| {
            let s = |k: &str| g[k].as_str().unwrap_or_default().to_string();
            (s("group"), s("version"), s("kind"))
        })
        .collect()
}

/// Definition of `kind` for gv in `defs`: by GVK extension, else by name suffix.
fn find<'a>(defs: &'a Defs, g: &str, v: &str, kind: &str) -> Option<&'a Value> {
    defs.values()
        .find(|d| gvks(d).iter().any(|x| x.0 == g && x.1 == v && x.2 == kind))
}

fn unit_name(g: &str, v: &str, kind: &str) -> String {
    if g.is_empty() {
        format!("{v}/{kind}")
    } else {
        format!("{g}/{v}/{kind}")
    }
}

struct Entry {
    check: String,
    pattern: String,
    tracking: String,
    reason: String,
}

fn glob(pat: &str, s: &str) -> bool {
    let parts: Vec<&str> = pat.split('*').collect();
    if parts.len() == 1 {
        return pat == s;
    }
    let (first, last) = (parts[0], parts[parts.len() - 1]);
    if !s.starts_with(first) || !s.ends_with(last) || s.len() < first.len() + last.len() {
        return false;
    }
    let mut rest = &s[first.len()..s.len() - last.len()];
    for mid in &parts[1..parts.len() - 1] {
        match rest.find(mid) {
            Some(i) => rest = &rest[i + mid.len()..],
            None => return false,
        }
    }
    true
}

fn parse_allowlist() -> Vec<Entry> {
    ALLOWLIST
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| {
            let p: Vec<&str> = l.splitn(4, '|').map(str::trim).collect();
            assert!(
                p.len() == 4 && p.iter().all(|x| !x.is_empty()),
                "allowlist line needs check|unit|tracking|reason: {l}"
            );
            Entry {
                check: p[0].into(),
                pattern: p[1].into(),
                tracking: p[2].into(),
                reason: p[3].into(),
            }
        })
        .collect()
}

#[tokio::test]
async fn openapi_matches_vendored_upstream_specs() {
    let api = TestApiServer::new();

    // Ours.
    let (st, v2) = api.get("/openapi/v2").await;
    assert_eq!(st, 200, "harness error: GET /openapi/v2");
    let our_v2: Defs = v2["definitions"].as_object().cloned().unwrap_or_default();
    let (st, index) = api.get("/openapi/v3").await;
    assert_eq!(st, 200, "harness error: GET /openapi/v3");
    let mut our_v3: BTreeMap<String, Defs> = BTreeMap::new();
    for (path, entry) in index["paths"].as_object().into_iter().flatten() {
        let Some(url) = entry["serverRelativeURL"].as_str() else {
            continue;
        };
        let (st, doc) = api.get(url).await;
        if st.is_success() {
            let gv = path
                .strip_prefix("apis/")
                .or_else(|| path.strip_prefix("api/"))
                .unwrap_or(path)
                .to_string();
            our_v3.insert(
                gv,
                doc["components"]["schemas"]
                    .as_object()
                    .cloned()
                    .unwrap_or_default(),
            );
        }
    }

    // Upstream.
    let swagger: Value = serde_json::from_slice(
        &std::fs::read(root().join("swagger.json")).expect("vendored swagger.json"),
    )
    .expect("swagger.json parses");
    let up_v2: Defs = swagger["definitions"].as_object().cloned().unwrap();
    let mut up_v3: BTreeMap<String, Defs> = BTreeMap::new();
    for e in std::fs::read_dir(root().join("v3")).expect("vendored v3 dir") {
        let p = e.unwrap().path();
        let stem = p.file_name().unwrap().to_string_lossy().into_owned();
        let Some(gv) = stem.strip_suffix("_openapi.json") else {
            continue;
        };
        let gv = gv.replace("__", "/");
        // Group documents ("apis/apps", "api") carry no schemas of their own.
        let Some(gv) = gv.strip_prefix("apis/").or_else(|| gv.strip_prefix("api/")) else {
            continue;
        };
        let doc: Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        up_v3.insert(
            gv.to_string(),
            doc["components"]["schemas"]
                .as_object()
                .cloned()
                .unwrap_or_default(),
        );
    }
    assert!(up_v3.len() > 30, "vendored v3 documents: {}", up_v3.len());

    // Units.
    let mut results: BTreeMap<(&'static str, String), Result<(), String>> = BTreeMap::new();
    let mut unit = |check: &'static str,
                    g: &str,
                    v: &str,
                    kind: &str,
                    ours: Option<(&Defs, &Value)>,
                    theirs: (&Defs, &Value)| {
        let r = match ours {
            None => Err("kind missing from our document".to_string()),
            Some((od, ov)) => {
                let mut out = Vec::new();
                cmp(
                    od,
                    theirs.0,
                    ov,
                    theirs.1,
                    "",
                    &mut BTreeSet::new(),
                    0,
                    &mut out,
                );
                out.dedup();
                if out.is_empty() {
                    Ok(())
                } else {
                    let n = out.len();
                    out.truncate(4);
                    Err(format!("{n} diff(s): {}", out.join(" | ")))
                }
            }
        };
        results.insert((check, unit_name(g, v, kind)), r);
    };

    for def in up_v2.values() {
        for (g, v, kind) in gvks(def) {
            let ours = find(&our_v2, &g, &v, &kind).map(|d| (&our_v2, d));
            unit("openapi-v2", &g, &v, &kind, ours, (&up_v2, def));
        }
    }
    for (gv, schemas) in &up_v3 {
        let empty = Defs::new();
        let ours_doc = our_v3.get(gv).unwrap_or(&empty);
        for def in schemas.values() {
            for (g, v, kind) in gvks(def) {
                let ours = find(ours_doc, &g, &v, &kind).map(|d| (ours_doc, d));
                unit("openapi-v3", &g, &v, &kind, ours, (schemas, def));
            }
        }
    }

    if std::env::var_os("GOLDEN_DUMP").is_some() {
        for ((c, u), r) in &results {
            if let Err(e) = r {
                eprintln!("FAIL {c} {u}: {e}");
            }
        }
    }

    let entries = parse_allowlist();
    let mut problems = Vec::new();
    let mut allowlisted = 0;
    for ((check, u), r) in &results {
        let listed = entries
            .iter()
            .any(|e| e.check == *check && glob(&e.pattern, u));
        match (r, listed) {
            (Err(e), false) => problems.push(format!("FAIL {check} {u}: {e}")),
            (Err(_), true) => allowlisted += 1,
            _ => {}
        }
    }
    for e in &entries {
        let matched: Vec<_> = results
            .iter()
            .filter(|((c, u), _)| *c == e.check && glob(&e.pattern, u))
            .collect();
        if matched.is_empty() {
            problems.push(format!(
                "STALE allowlist entry matches no unit: {}|{} ({}: {})",
                e.check, e.pattern, e.tracking, e.reason
            ));
        }
        for ((c, u), r) in matched {
            if r.is_ok() {
                problems.push(format!(
                    "NOW MATCHES (remove/narrow allowlist entry {}|{}): {c} {u}",
                    e.check, e.pattern
                ));
            }
        }
    }
    let total = results.len();
    let failing = results.values().filter(|r| r.is_err()).count();
    eprintln!(
        "openapi golden: {total} units: {} match, {allowlisted} allowlisted xfail, {} problems",
        total - failing,
        problems.len()
    );
    assert!(
        problems.is_empty(),
        "{} openapi golden problem(s):\n{}",
        problems.len(),
        problems.join("\n")
    );
}
