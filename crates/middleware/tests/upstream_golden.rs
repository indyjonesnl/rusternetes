//! Golden-fixture harness: every vendored upstream Kubernetes 1.35 `testdata/HEAD`
//! object (json + pb) is pushed through our protobuf codec and compared with
//! the upstream bytes. Always runs: the oracles live in-repo under
//! `tests/upstream/k8s-1.35/` (see `tests/upstream/README.md`), never edited.
//!
//! Upstream mechanism ported (release-1.35):
//! * `staging/src/k8s.io/apimachinery/pkg/api/apitesting/roundtrip/compatibility.go`
//!   `CompatibilityTestOptions.Run` / `TestCompatibility`: for each GVK read
//!   `testdata/HEAD/<group>.<version>.<Kind>.{json,pb}`; decode each and require
//!   equality with the freshly-filled object (`expectedObject`), then re-encode and
//!   require the bytes equal the fixture (`expectedBytes`), json by `bytes.Equal`
//!   on the normalised form and protobuf by exact bytes (the encoder is
//!   deterministic: sorted map keys, tag order).
//! * `staging/src/k8s.io/api/roundtrip_test.go` `TestCompatibility` drives the
//!   above over the `k8s.io/api` scheme.
//! * `staging/src/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/roundtrip_test.go`
//!   does the same for the CRD types (`apiextensions-testdata/`).
//!
//! Checks (each runs per fixture; the name is the key in the allowlist):
//!
//! `decode`: pb -> JSON via `PROTO_REGISTRY.decode_k8s_resource` equals the fixture JSON.
//!
//! `encode-bytes`: fixture JSON -> pb via the production response encoder
//! (`rusternetes_middleware::response::encode_native`); the payload bytes
//! (`Unknown.raw`) equal the fixture pb payload, byte for byte.
//!
//! `encode-semantic`: decode(encode(JSON)) equals JSON (lossless codec,
//! independent of byte ordering).
//!
//! `zero-fixpoint`: every scalar zeroed, then decode(encode(.)) is a fixed point.
//!
//! `typed`: fixture JSON -> our typed struct -> JSON equals the fixture JSON (field
//! presence/naming of the serde types), for kinds with a typed model (see
//! `typed/mod.rs` for the selfLink / strict-enum adjustments).
//!
//! `typed-wire`: fixture JSON -> typed -> JSON -> pb -> JSON equals the fixture JSON
//! (the server write path: typed model, then the response encoder).
//!
//! Normalisation applied to both sides before JSON comparison: explicit `null`
//! members are dropped (Go emits `"creationTimestamp": null` for a zero
//! `metav1.Time`; absent == null for every client).
//!
//! Known gaps live in `upstream_golden_allowlist.txt` (xfail style): every entry
//! names a check, a fixture stem (`*` globs allowed), a tracking reference and a
//! reason. A listed fixture that now PASSES fails the test, so the list can only
//! shrink. Set `GOLDEN_DUMP=1` to print every failing (check, stem) with its diff
//! when (re)generating entries.

use prost::Message;
use rusternetes_common::protobuf::Unknown;
use rusternetes_middleware::response::encode_native;
use rusternetes_protobuf::PROTO_REGISTRY;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

mod typed;
use typed::{DISPATCHED, NO_TYPED_MODEL};

const ALLOWLIST: &str = include_str!("upstream_golden_allowlist.txt");

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/upstream/k8s-1.35")
}

pub struct Fixture {
    pub stem: String,
    /// `apiVersion`/`kind` of the object. For types that are not part of the
    /// scheme and carry no TypeMeta (`core.v1.WatchEvent`: empty `kind` in the
    /// JSON and an empty `Unknown.typeMeta` in the pb) these are derived from
    /// the `<group>.<version>.<Kind>` stem, as upstream's `TestCompatibility`
    /// takes the GVK from the testdata file name.
    pub api_version: String,
    pub kind: String,
    /// The pb fixture's `Unknown.typeMeta` is empty, so the codec cannot
    /// dispatch on it and decode goes straight to the schema for `kind`.
    pub untyped: bool,
    pub json: Value,
    pub pb: Vec<u8>,
}

fn load_fixtures() -> Vec<Fixture> {
    let mut out = Vec::new();
    for sub in ["api-testdata", "apiextensions-testdata"] {
        let dir = root().join(sub);
        let mut pbs: Vec<PathBuf> = fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "pb"))
            .collect();
        pbs.sort();
        for p in pbs {
            let json: Value = serde_json::from_slice(&fs::read(p.with_extension("json")).unwrap())
                .unwrap_or_else(|e| panic!("{}: {e}", p.display()));
            let stem = p.file_stem().unwrap().to_string_lossy().into_owned();
            let kind = json["kind"].as_str().unwrap_or_default().to_string();
            let untyped = kind.is_empty();
            let (api_version, kind) = if untyped {
                let parts: Vec<&str> = stem.split('.').collect();
                let (kind, version) = (parts[parts.len() - 1], parts[parts.len() - 2]);
                let group = parts[..parts.len() - 2].join(".");
                let api = if group == "core" {
                    version.to_string()
                } else {
                    format!("{group}/{version}")
                };
                (api, kind.to_string())
            } else {
                (
                    json["apiVersion"].as_str().unwrap_or_default().to_string(),
                    kind,
                )
            };
            out.push(Fixture {
                stem,
                api_version,
                kind,
                untyped,
                json,
                pb: fs::read(&p).unwrap(),
            });
        }
    }
    out
}

/// In-tree volume plugins our `Volume` model intentionally does not implement
/// (upstream marks them deprecated/removed). Same set as
/// `completeness_skip("VolumeSource", ..)` in
/// `crates/api-server/tests/it/protobuf_schema_parity_upstream.rs` and the
/// `KNOWN_GAPS` of `k8s_openapi_parity`; stripped from `volumes[]` entries on
/// both sides of every JSON comparison. (Byte comparison cannot strip them
/// from the upstream pb, so fixtures that carry them are allowlisted under
/// `encode-bytes`.)
const LEGACY_VOLUME_SOURCES: &[&str] = &[
    "gcePersistentDisk",
    "awsElasticBlockStore",
    "gitRepo",
    "glusterfs",
    "rbd",
    "flexVolume",
    "cinder",
    "cephfs",
    "flocker",
    "fc",
    "azureFile",
    "vsphereVolume",
    "quobyte",
    "azureDisk",
    "photonPersistentDisk",
    "portworxVolume",
    "scaleIO",
    "storageos",
];

pub fn normalize(v: &mut Value) {
    match v {
        Value::Object(m) => {
            m.retain(|_, x| !x.is_null());
            if let Some(Value::Array(vols)) = m.get_mut("volumes") {
                for vol in vols.iter_mut().filter_map(Value::as_object_mut) {
                    vol.retain(|k, _| !LEGACY_VOLUME_SOURCES.contains(&k.as_str()));
                }
            }
            m.values_mut().for_each(normalize);
        }
        Value::Array(a) => a.iter_mut().for_each(normalize),
        _ => {}
    }
}

fn short(v: Option<&Value>) -> String {
    match v {
        None => "<absent>".into(),
        Some(v) => {
            let s = v.to_string();
            if s.chars().count() > 60 {
                format!("{}...", s.chars().take(60).collect::<String>())
            } else {
                s
            }
        }
    }
}

/// Structural diff; array positions are collapsed to `[]` and only leaf/absent
/// mismatches are reported so one systematic gap is one line.
fn diff(got: &Value, want: &Value, path: &str, out: &mut Vec<String>) {
    match (got, want) {
        (Value::Object(g), Value::Object(w)) => {
            for k in g.keys().chain(w.keys().filter(|k| !g.contains_key(*k))) {
                match (g.get(k), w.get(k)) {
                    (Some(a), Some(b)) => diff(a, b, &format!("{path}/{k}"), out),
                    (a, b) => out.push(format!("{path}/{k}: got {} want {}", short(a), short(b))),
                }
            }
        }
        (Value::Array(g), Value::Array(w)) => {
            if g.len() != w.len() {
                out.push(format!("{path}: len got {} want {}", g.len(), w.len()));
            }
            for (a, b) in g.iter().zip(w) {
                diff(a, b, &format!("{path}[]"), out);
            }
        }
        _ if got != want => out.push(format!(
            "{path}: got {} want {}",
            short(Some(got)),
            short(Some(want))
        )),
        _ => {}
    }
}

pub fn json_eq(got: &Value, want: &Value) -> Result<(), String> {
    let (mut g, mut w) = (got.clone(), want.clone());
    normalize(&mut g);
    normalize(&mut w);
    let mut d = Vec::new();
    diff(&g, &w, "", &mut d);
    d.dedup();
    if d.is_empty() {
        Ok(())
    } else {
        Err(d.join(" | "))
    }
}

pub fn envelope_raw(framed: &[u8]) -> Result<Vec<u8>, String> {
    let body = framed
        .strip_prefix(b"k8s\0")
        .ok_or("missing k8s\\0 magic")?;
    Ok(Unknown::decode(body).map_err(|e| e.to_string())?.raw)
}

type Outcome = Result<(), String>;

/// One decoded wire field: (number, wire type, bytes of payload / varint value).
type WireField = (u64, u8, Vec<u8>);

fn read_varint(b: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = *b.get(*pos)?;
        *pos += 1;
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

/// Parse `b` as a protobuf message; `None` if it is not one (a string/bytes leaf).
fn parse_wire(b: &[u8]) -> Option<Vec<WireField>> {
    let (mut pos, mut out) = (0, Vec::new());
    while pos < b.len() {
        let tag = read_varint(b, &mut pos)?;
        let (num, wt) = (tag >> 3, (tag & 7) as u8);
        if num == 0 {
            return None;
        }
        let payload = match wt {
            0 => read_varint(b, &mut pos)?.to_le_bytes().to_vec(),
            1 => {
                let s = b.get(pos..pos + 8)?.to_vec();
                pos += 8;
                s
            }
            2 => {
                let n = read_varint(b, &mut pos)? as usize;
                let s = b.get(pos..pos.checked_add(n)?)?.to_vec();
                pos += n;
                s
            }
            5 => {
                let s = b.get(pos..pos + 4)?.to_vec();
                pos += 4;
                s
            }
            _ => return None,
        };
        out.push((num, wt, payload));
    }
    Some(out)
}

fn printable(b: &[u8]) -> String {
    let s = String::from_utf8_lossy(b);
    if s.chars().all(|c| !c.is_control()) && !b.is_empty() {
        format!("{:?}", s.chars().take(24).collect::<String>())
    } else {
        format!("{} bytes", b.len())
    }
}

/// Field-number level diff of two serialized messages (no schema needed):
/// recurses into length-delimited fields that parse as messages and reports
/// fields present on one side only or with differing leaf values. The path is
/// the chain of field numbers, so `1.5` = field 5 inside field 1 (ObjectMeta).
fn wire_diff(got: &[u8], want: &[u8]) -> String {
    let mut out = Vec::new();
    wire_diff_into(got, want, "", &mut out);
    out.dedup();
    if out.is_empty() {
        "bytes differ but same field set (ordering/packing)".into()
    } else {
        out.join(" | ")
    }
}

fn wire_diff_into(got: &[u8], want: &[u8], path: &str, out: &mut Vec<String>) {
    let (Some(g), Some(w)) = (parse_wire(got), parse_wire(want)) else {
        if got != want {
            out.push(format!(
                "{path}: got {} want {}",
                printable(got),
                printable(want)
            ));
        }
        return;
    };
    let mut nums: Vec<u64> = g.iter().chain(&w).map(|f| f.0).collect();
    nums.sort_unstable();
    nums.dedup();
    for n in nums {
        let gs: Vec<&WireField> = g.iter().filter(|f| f.0 == n).collect();
        let ws: Vec<&WireField> = w.iter().filter(|f| f.0 == n).collect();
        let here = format!("{path}.{n}");
        if gs.len() != ws.len() {
            out.push(format!("{here}: count got {} want {}", gs.len(), ws.len()));
            continue;
        }
        for (a, b) in gs.iter().zip(&ws) {
            if a.2 != b.2 || a.1 != b.1 {
                if a.1 == 2 && b.1 == 2 {
                    wire_diff_into(&a.2, &b.2, &here, out);
                } else {
                    out.push(format!("{here}: scalar differs"));
                }
            }
        }
    }
}

fn check_decode(f: &Fixture) -> Outcome {
    if f.untyped {
        let raw = envelope_raw(&f.pb)?;
        let got = PROTO_REGISTRY
            .decode_message(&f.kind, &raw)
            .ok_or("no registry schema (decode_message returned None)")?;
        return json_eq(&got, &f.json);
    }
    let bytes = PROTO_REGISTRY
        .decode_k8s_resource(&f.pb)
        .ok_or("no registry schema (decode_k8s_resource returned None)")?;
    let got: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    json_eq(&got, &f.json)
}

fn check_encode_bytes(f: &Fixture) -> Outcome {
    let body = serde_json::to_vec(&f.json).unwrap();
    let framed = encode_native(&body, &f.api_version, &f.kind)
        .ok_or("no registry schema (encode_native returned None)")?;
    let got = envelope_raw(&framed)?;
    let want = envelope_raw(&f.pb)?;
    if got == want {
        return Ok(());
    }
    Err(wire_diff(&got, &want))
}

fn check_encode_semantic(f: &Fixture) -> Outcome {
    let body = serde_json::to_vec(&f.json).unwrap();
    let framed = encode_native(&body, &f.api_version, &f.kind).ok_or("no registry schema")?;
    let back = PROTO_REGISTRY
        .decode_k8s_resource(&framed)
        .ok_or("encoded bytes do not decode")?;
    let mut got: Value = serde_json::from_slice(&back).map_err(|e| e.to_string())?;
    if f.untyped {
        // The codec stamps the derived apiVersion/kind; the fixture has none.
        got.as_object_mut().map(|m| {
            m.remove("apiVersion");
            m.remove("kind")
        });
    }
    json_eq(&got, &f.json)
}

/// Zero variant: every scalar leaf of the fixture set to its Go zero value
/// (numbers 0, bools false, strings "") except `apiVersion`/`kind` and
/// RFC3339 timestamps (an empty `metav1.Time` is `null`, not `""`). The result
/// must survive the server codec as a semantic fixed point:
/// `d = decode(encode(z))` satisfies `decode(encode(d)) == d` (bytes are not
/// compared: Go writes plain zero scalars on the wire, we write only the
/// present ones, which every decoder reads identically). Catches asymmetric
/// encode/decode of zero values (e.g. a plain-omitempty zero emitted by one
/// side only, the PVC `storageClassName: ""` family).
///
/// Deviation from the issue text: the issue also asks to assert "plain zero
/// absent / pointer zero retained" from types.go pointer-ness. That oracle is
/// not derivable from the vendored fixtures or swagger (pointer vs
/// plain-omitempty is not recorded there); it is owned by #2957's generated
/// types.go table, so only the fixed-point invariant is checked here.
fn zero_variant(v: &Value, key: &str) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, x)| (k.clone(), zero_variant(x, k)))
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(|x| zero_variant(x, key)).collect()),
        Value::Number(_) => Value::from(0),
        Value::Bool(_) => Value::Bool(false),
        Value::String(s)
            if key == "apiVersion"
                || key == "kind"
                || chrono::DateTime::parse_from_rfc3339(s).is_ok() =>
        {
            Value::String(s.clone())
        }
        Value::String(_) => Value::String(String::new()),
        Value::Null => Value::Null,
    }
}

fn check_zero_fixpoint(f: &Fixture) -> Outcome {
    let z = zero_variant(&f.json, "");
    let enc = |v: &Value| {
        encode_native(&serde_json::to_vec(v).unwrap(), &f.api_version, &f.kind)
            .ok_or_else(|| "no registry schema".to_string())
    };
    let dec = |framed: &[u8]| -> Result<Value, String> {
        let b = PROTO_REGISTRY
            .decode_k8s_resource(framed)
            .ok_or("encoded bytes do not decode")?;
        serde_json::from_slice(&b).map_err(|e| e.to_string())
    };
    let d1 = dec(&enc(&z)?)?;
    let d2 = dec(&enc(&d1)?)?;
    json_eq(&d2, &d1)
}

fn check_typed(f: &Fixture) -> Option<Outcome> {
    let to_typed = typed::dispatch(&f.stem)?;
    Some(to_typed(&f.json).and_then(|(expected, got)| json_eq(&got, &expected)))
}

/// Server write path: typed struct -> JSON -> native protobuf -> JSON.
fn check_typed_wire(f: &Fixture) -> Option<Outcome> {
    let to_typed = typed::dispatch(&f.stem)?;
    Some((|| {
        let (expected, got) = to_typed(&f.json)?;
        let framed = encode_native(&serde_json::to_vec(&got).unwrap(), &f.api_version, &f.kind)
            .ok_or("no registry schema (encode)")?;
        let back = PROTO_REGISTRY
            .decode_k8s_resource(&framed)
            .ok_or("encoded bytes do not decode")?;
        let back: Value = serde_json::from_slice(&back).map_err(|e| e.to_string())?;
        json_eq(&back, &expected)
    })())
}

struct Entry {
    /// Comma-separated check names.
    check: String,
    pattern: String,
    tracking: String,
    reason: String,
}

impl Entry {
    fn checks(&self) -> Vec<&str> {
        self.check.split(',').map(str::trim).collect()
    }
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
                "allowlist line needs check|stem|tracking|reason: {l}"
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

#[test]
fn upstream_golden_fixtures() {
    let fixtures = load_fixtures();
    assert_eq!(
        fixtures.len(),
        183 + 4,
        "api-testdata GVKs + apiextensions-testdata GVKs"
    );

    // Every GA fixture is either dispatched to a typed model or explicitly listed.
    for f in &fixtures {
        let ga = !f.untyped
            && f.api_version
                .rsplit('/')
                .next()
                .is_some_and(|v| v == "v1" || v == "v2");
        if ga {
            assert!(
                DISPATCHED.contains(&f.stem.as_str())
                    || NO_TYPED_MODEL.iter().any(|(s, _)| *s == f.stem),
                "GA fixture {} has no typed dispatch and is not in NO_TYPED_MODEL",
                f.stem
            );
        }
    }
    for (s, _) in NO_TYPED_MODEL {
        assert!(
            fixtures.iter().any(|f| f.stem == *s) && !DISPATCHED.contains(s),
            "stale NO_TYPED_MODEL entry {s}"
        );
    }

    let mut results: BTreeMap<(&'static str, String), Outcome> = BTreeMap::new();
    for f in &fixtures {
        results.insert(("decode", f.stem.clone()), check_decode(f));
        results.insert(("encode-bytes", f.stem.clone()), check_encode_bytes(f));
        results.insert(
            ("encode-semantic", f.stem.clone()),
            check_encode_semantic(f),
        );
        results.insert(("zero-fixpoint", f.stem.clone()), check_zero_fixpoint(f));
        if let Some(r) = check_typed(f) {
            results.insert(("typed", f.stem.clone()), r);
        }
        if let Some(r) = check_typed_wire(f) {
            results.insert(("typed-wire", f.stem.clone()), r);
        }
    }

    if std::env::var_os("GOLDEN_DUMP").is_some() {
        for ((check, stem), r) in &results {
            if let Err(e) = r {
                eprintln!("FAIL {check} {stem}: {e}");
            }
        }
    }

    let entries = parse_allowlist();
    let mut problems = Vec::new();
    let mut allowlisted = 0usize;

    for ((check, stem), r) in &results {
        let listed = entries
            .iter()
            .any(|e| e.checks().contains(check) && glob(&e.pattern, stem));
        match (r, listed) {
            (Err(e), false) => problems.push(format!("FAIL {check} {stem}: {e}")),
            (Err(_), true) => allowlisted += 1,
            _ => {}
        }
    }
    for e in &entries {
        let matched: Vec<_> = results
            .iter()
            .filter(|((c, s), _)| e.checks().contains(c) && glob(&e.pattern, s))
            .collect();
        if matched.is_empty() {
            problems.push(format!(
                "STALE allowlist entry matches no fixture: {}|{} ({}: {})",
                e.check, e.pattern, e.tracking, e.reason
            ));
        }
        for ((c, s), r) in matched {
            if r.is_ok() {
                problems.push(format!(
                    "NOW PASSES (remove/narrow allowlist entry {}|{}): {c} {s}",
                    e.check, e.pattern
                ));
            }
        }
    }

    let total = results.len();
    let failing = results.values().filter(|r| r.is_err()).count();
    eprintln!(
        "golden fixtures: {} fixtures, {total} checks: {} pass, {allowlisted} allowlisted xfail, {} problems",
        fixtures.len(),
        total - failing,
        problems.len()
    );
    assert!(
        problems.is_empty(),
        "{} golden problem(s):\n{}",
        problems.len(),
        problems.join("\n")
    );
}
