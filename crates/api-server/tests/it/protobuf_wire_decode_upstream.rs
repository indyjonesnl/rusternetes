//! Wire-level decode net: every message in the bundled upstream
//! `generated.proto` hard-gate files that has a registry entry is filled with
//! a maximal synthesized value, encoded with OUR codec, and then decoded with
//! an independent, strict wire walker driven by the UPSTREAM descriptor.
//!
//! Why this exists: `registry_parity_with_upstream` only compares the registry
//! to upstream for core/v1 (bare keys) and for qualified keys. Bare registry
//! keys for non-core groups (batch/apps/...) were never compared, so
//! `JobStatus` shipped with `terminating`/`failedIndexes` field numbers
//! swapped (upstream: failedIndexes=10, terminating=11 —
//! `k8s.io/api/batch/v1/generated.proto` `message JobStatus`) and client-go's
//! generated Unmarshal rejected the body with
//! `proto: wrong wireType = 0 for field FailedIndexes`.
//!
//! Coverage classes: messages with a qualified key or an unambiguous bare key
//! are a HARD gate. Messages whose bare name is shared by several groups with
//! no qualified key (checked against the single bare entry), and the
//! formerly skip-listed `Validation`/`Variable`/`JSONSchemaProps`, are
//! WARNING-ONLY (`ambiguous_and_formerly_skipped_messages_wire_check_warn_only`,
//! `::warning` under GitHub Actions); gaps are tracked in #3057. Enum values
//! are not checked: k8s protos declare enum-like fields as `string`.
//!
//! The decoder mirrors the checks of the gogo-generated `Unmarshal` in
//! `staging/src/k8s.io/api/batch/v1/generated.pb.go` (wire type must match the
//! field's declared type), and additionally requires that every value we sent
//! comes back (a dropped field is a lossy codec).

use crate::protobuf_schema_parity_upstream::{parse_all_files, qualified_prefix_for};
use base64::Engine;
use prost_types::field_descriptor_proto::{Label, Type as T};
use prost_types::{DescriptorProto, FieldDescriptorProto};
use rusternetes_api_server::protobuf::ProtoRegistry;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

type Msgs = HashMap<String, DescriptorProto>;

fn collect(msg: &DescriptorProto, fq: &str, out: &mut Msgs) {
    out.insert(fq.to_string(), msg.clone());
    for n in &msg.nested_type {
        collect(n, &format!("{}.{}", fq, n.name()), out);
    }
}

fn is_map_entry(m: &DescriptorProto) -> bool {
    m.options.as_ref().and_then(|o| o.map_entry) == Some(true)
}

const TIME: &str = ".k8s.io.apimachinery.pkg.apis.meta.v1.Time";
const MICRO_TIME: &str = ".k8s.io.apimachinery.pkg.apis.meta.v1.MicroTime";
const DURATION: &str = ".k8s.io.apimachinery.pkg.apis.meta.v1.Duration";
const QUANTITY: &str = ".k8s.io.apimachinery.pkg.api.resource.Quantity";
const INTSTR: &str = ".k8s.io.apimachinery.pkg.util.intstr.IntOrString";
const RAW: &str = ".k8s.io.apimachinery.pkg.runtime.RawExtension";
const FIELDSV1: &str = ".k8s.io.apimachinery.pkg.apis.meta.v1.FieldsV1";
const JSON_T: &str = ".k8s.io.apiextensions_apiserver.pkg.apis.apiextensions.v1.JSON";

/// Types whose JSON form is not the structural form of their proto message.
fn special(t: &str) -> bool {
    matches!(
        t,
        TIME | MICRO_TIME | DURATION | QUANTITY | INTSTR | RAW | FIELDSV1 | JSON_T
    )
}

/// JSON key the registry uses for an upstream field. Upstream's Go field
/// names diverge from the JSON names for a few messages (protoc-gen-go
/// derives `schema`/`ref`/`xKubernetesFoo`/`jSONSchemas`, and PascalCase
/// `Name`/`Expression`); the parity test covers them with its
/// name-aware `intentional_field_skip`. The synthesized input and the decoded
/// output are both keyed by the JSON name so the registry can be exercised.
fn jname(msg: &str, field: &str) -> String {
    // Only the formerly skip-listed messages are renamed; every other
    // message (e.g. core/v1 `DaemonEndpoint.Port`) is keyed by its proto name.
    if !matches!(
        msg,
        "Validation" | "Variable" | "JSONSchemaProps" | "JSONSchemaPropsOrArray"
    ) {
        return field.to_string();
    }
    match (msg, field) {
        ("JSONSchemaProps", "schema") => return "$schema".into(),
        ("JSONSchemaProps", "ref") => return "$ref".into(),
        ("JSONSchemaPropsOrArray", "jSONSchemas") => return "jsonSchemas".into(),
        _ => {}
    }
    if let Some(rest) = field.strip_prefix("xKubernetes") {
        let mut out = String::from("x-kubernetes");
        for c in rest.chars() {
            if c.is_ascii_uppercase() {
                out.push('-');
                out.push(c.to_ascii_lowercase());
            } else {
                out.push(c);
            }
        }
        return out;
    }
    let mut cs = field.chars();
    match cs.next() {
        Some(c) if c.is_ascii_uppercase() => format!("{}{}", c.to_ascii_lowercase(), cs.as_str()),
        _ => field.to_string(),
    }
}

fn simple(fq: &str) -> &str {
    fq.rsplit('.').next().unwrap_or(fq)
}

struct Ctx<'a> {
    msgs: &'a Msgs,
}

fn synth_special(t: &str) -> Value {
    match t {
        TIME | MICRO_TIME => json!("2024-01-02T03:04:05Z"),
        DURATION => json!("1s"),
        QUANTITY => json!("1Gi"),
        INTSTR => json!(5),
        _ => json!({"a": "b"}),
    }
}

fn synth_scalar(f: &FieldDescriptorProto) -> Value {
    match f.r#type() {
        T::String => json!("s"),
        T::Bool => json!(true),
        T::Bytes => json!(base64::engine::general_purpose::STANDARD.encode("hi")),
        T::Double | T::Float => json!(1.5),
        T::Enum => json!(1),
        _ => json!(7),
    }
}

impl Ctx<'_> {
    fn synth_msg(&self, fq: &str, stack: &mut Vec<String>) -> Value {
        let m = &self.msgs[fq];
        let mut o = Map::new();
        for f in &m.field {
            if let Some(v) = self.synth_field(f, stack) {
                o.insert(jname(simple(fq), f.name()), v);
            }
        }
        Value::Object(o)
    }

    fn synth_one(&self, f: &FieldDescriptorProto, stack: &mut Vec<String>) -> Option<Value> {
        if f.type_name().is_empty() {
            return Some(synth_scalar(f));
        }
        let t = f.type_name();
        if special(t) {
            return Some(synth_special(t));
        }
        if matches!(f.r#type(), T::Enum) {
            return Some(json!(1));
        }
        if !self.msgs.contains_key(t) || stack.iter().any(|s| s == t) {
            return None;
        }
        stack.push(t.to_string());
        let v = self.synth_msg(t, stack);
        stack.pop();
        Some(v)
    }

    fn synth_field(&self, f: &FieldDescriptorProto, stack: &mut Vec<String>) -> Option<Value> {
        if f.label() != Label::Repeated {
            return self.synth_one(f, stack);
        }
        if let Some(e) = self.msgs.get(f.type_name()).filter(|e| is_map_entry(e)) {
            let vf = e.field.iter().find(|x| x.number() == 2).unwrap();
            let v = self.synth_one(vf, stack)?;
            return Some(json!({ "k": v }));
        }
        Some(json!([self.synth_one(f, stack)?]))
    }
}

// ---- strict wire walker driven by the upstream descriptor -----------------

fn varint(b: &[u8], i: &mut usize) -> Result<u64, String> {
    let mut v = 0u64;
    let mut s = 0;
    loop {
        let c = *b.get(*i).ok_or("unexpected EOF")?;
        *i += 1;
        v |= ((c & 0x7f) as u64) << s;
        if c < 0x80 {
            return Ok(v);
        }
        s += 7;
        if s > 63 {
            return Err("varint overflow".into());
        }
    }
}

fn expected_wire(f: &FieldDescriptorProto) -> u64 {
    // protox-parse leaves r#type() at its default when type_name is set.
    if !f.type_name().is_empty() {
        return 2;
    }
    match f.r#type() {
        T::String | T::Bytes | T::Message => 2,
        T::Double | T::Fixed64 | T::Sfixed64 => 1,
        T::Float | T::Fixed32 | T::Sfixed32 => 5,
        _ => 0,
    }
}

fn push(o: &mut Map<String, Value>, name: &str, v: Value, repeated: bool) {
    if repeated {
        o.entry(name)
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .unwrap()
            .push(v);
    } else {
        o.insert(name.to_string(), v);
    }
}

impl Ctx<'_> {
    fn dec_msg(&self, fq: &str, b: &[u8]) -> Result<Value, String> {
        let m = &self.msgs[fq];
        let mut o: Map<String, Value> = Map::new();
        let mut i = 0;
        while i < b.len() {
            let tag = varint(b, &mut i)?;
            let (num, wt) = ((tag >> 3) as i32, tag & 7);
            let Some(f) = m.field.iter().find(|f| f.number() == num) else {
                return Err(format!("{fq}: unknown field {num} (wire {wt})"));
            };
            let repeated = f.label() == Label::Repeated;
            let ew = expected_wire(f);
            let jn = jname(simple(fq), f.name());
            let name = jn.as_str();
            if repeated && wt == 2 && ew != 2 {
                // packed repeated scalars
                let n = varint(b, &mut i)? as usize;
                let end = i + n;
                if end > b.len() {
                    return Err("short packed".into());
                }
                while i < end {
                    let v = varint(b, &mut i)?;
                    push(&mut o, name, json!(v as i64), true);
                }
                continue;
            }
            if wt != ew {
                return Err(format!(
                    "{fq}: proto: wrong wireType = {wt} for field {name} (#{num}, expected {ew})"
                ));
            }
            let val = match wt {
                0 => {
                    let v = varint(b, &mut i)?;
                    if matches!(f.r#type(), T::Bool) {
                        json!(v != 0)
                    } else {
                        json!(v as i64)
                    }
                }
                1 => {
                    let x = f64::from_le_bytes(b.get(i..i + 8).ok_or("EOF")?.try_into().unwrap());
                    i += 8;
                    json!(x)
                }
                5 => {
                    let x = f32::from_le_bytes(b.get(i..i + 4).ok_or("EOF")?.try_into().unwrap());
                    i += 4;
                    json!(x)
                }
                _ => {
                    let n = varint(b, &mut i)? as usize;
                    let sl = b.get(i..i + n).ok_or("short field")?;
                    i += n;
                    self.dec_len(f, sl)?
                }
            };
            if let Some(e) = self.msgs.get(f.type_name()).filter(|e| is_map_entry(e)) {
                let _ = e;
                let k = val["key"].as_str().unwrap_or("").to_string();
                let v = val.get("value").cloned().unwrap_or(Value::Null);
                o.entry(name)
                    .or_insert_with(|| json!({}))
                    .as_object_mut()
                    .unwrap()
                    .insert(k, v);
            } else {
                push(&mut o, name, val, repeated);
            }
        }
        Ok(Value::Object(o))
    }

    fn dec_len(&self, f: &FieldDescriptorProto, sl: &[u8]) -> Result<Value, String> {
        match f.r#type() {
            T::String => Ok(json!(std::str::from_utf8(sl).map_err(|e| e.to_string())?)),
            T::Bytes => Ok(json!(base64::engine::general_purpose::STANDARD.encode(sl))),
            _ => {
                let t = f.type_name();
                match t {
                    TIME | MICRO_TIME => {
                        let v = self.dec_msg(t, sl)?;
                        Ok(json!(if v["seconds"] == 1704164645 {
                            "2024-01-02T03:04:05Z"
                        } else {
                            "BAD_TIME"
                        }))
                    }
                    DURATION => Ok(json!("1s")),
                    QUANTITY => self.dec_msg(t, sl).map(|v| v["string"].clone()),
                    INTSTR => self.dec_msg(t, sl).map(|v| {
                        if v["type"] == 1 {
                            v["strVal"].clone()
                        } else {
                            v["intVal"].clone()
                        }
                    }),
                    RAW | FIELDSV1 | JSON_T => Ok(json!({"a": "b"})),
                    _ => {
                        let e = &self.msgs[t];
                        if is_map_entry(e) {
                            let mut o = Map::new();
                            let mut i = 0;
                            while i < sl.len() {
                                let tag = varint(sl, &mut i)?;
                                let num = (tag >> 3) as i32;
                                let f2 = e
                                    .field
                                    .iter()
                                    .find(|x| x.number() == num)
                                    .ok_or("bad entry field")?;
                                if (tag & 7) != expected_wire(f2) {
                                    return Err(format!(
                                        "{t}: wrong wireType {} for map field {}",
                                        tag & 7,
                                        f2.name()
                                    ));
                                }
                                let n = varint(sl, &mut i)? as usize;
                                let part = sl.get(i..i + n).ok_or("short entry")?;
                                i += n;
                                o.insert(f2.name().to_string(), self.dec_len(f2, part)?);
                            }
                            Ok(Value::Object(o))
                        } else {
                            self.dec_msg(t, sl)
                        }
                    }
                }
            }
        }
    }
}

fn diff(path: &str, want: &Value, got: &Value, out: &mut Vec<String>) {
    match (want, got) {
        (Value::Object(w), Value::Object(g)) => {
            for (k, wv) in w {
                match g.get(k) {
                    None => out.push(format!("{path}/{k}: dropped")),
                    Some(gv) => diff(&format!("{path}/{k}"), wv, gv, out),
                }
            }
        }
        (Value::Array(w), Value::Array(g)) if w.len() == g.len() => {
            for (i, (a, b)) in w.iter().zip(g).enumerate() {
                diff(&format!("{path}/{i}"), a, b, out);
            }
        }
        _ if want == got => {}
        _ => out.push(format!("{path}: want {want} got {got}")),
    }
}

/// How a registry key was found for an upstream message.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Resolution {
    /// Qualified key, or a bare key that is unambiguous (unique simple name
    /// across the bundled files, or core/apimachinery). A failure is a hard
    /// failure: this is the original gate.
    Direct,
    /// Bare simple name shared by several groups with NO qualified key: the
    /// single bare registry entry is checked against each group's descriptor.
    /// Previously skipped, so a wrong field number was invisible. Failures
    /// are warnings (tracked in #3057), not test failures.
    AmbiguousBare,
    /// Simple name on the old skip list (Go field names differ from JSON
    /// names; now handled by `jname`). Failures are warnings.
    NewlyCovered,
}

/// Registry key for an upstream message, if the registry plausibly carries it.
/// Qualified key first; then the bare key (see [`Resolution`]).
fn registry_key(
    reg: &ProtoRegistry,
    prefix: &str,
    name: &str,
    counts: &BTreeMap<String, usize>,
) -> Option<(String, Resolution)> {
    let q = format!("{prefix}.{name}");
    if !prefix.is_empty() && reg.encode_message(&q, &json!({})).is_some() {
        return Some((q, Resolution::Direct));
    }
    reg.encode_message(name, &json!({}))?;
    let unique = counts.get(name).copied().unwrap_or(0) == 1;
    let res = if prefix.is_empty() || unique {
        Resolution::Direct
    } else {
        Resolution::AmbiguousBare
    };
    Some((name.to_string(), res))
}

/// Messages previously skipped wholesale (Go field names differ from the JSON
/// names the registry is keyed by).
const FORMERLY_SKIPPED: &[&str] = &["Validation", "Variable", "JSONSchemaProps"];

#[derive(Default)]
pub(crate) struct WireResults {
    /// Messages checked in the hard-gated (`Direct`) class.
    pub checked: usize,
    /// Hard failures keyed by `<proto file>::<Message>`.
    pub fails: BTreeMap<String, Vec<String>>,
    /// Messages checked in the warning-only classes.
    pub soft_checked: usize,
    /// Warning-only failures keyed by `<proto file>::<Message>`, with the
    /// reason the message is warning-only.
    pub warns: BTreeMap<String, Vec<String>>,
}

/// Run the wire check for every upstream message that has a registry entry.
pub(crate) fn run_wire_check_full() -> WireResults {
    let files = parse_all_files();
    let mut msgs: Msgs = HashMap::new();
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for f in &files {
        for m in &f.message_type {
            collect(m, &format!(".{}.{}", f.package(), m.name()), &mut msgs);
            if !is_map_entry(m) {
                *counts.entry(m.name().to_string()).or_default() += 1;
            }
        }
    }
    let ctx = Ctx { msgs: &msgs };
    let reg = ProtoRegistry::new();
    let mut out = WireResults::default();
    for f in &files {
        let prefix = qualified_prefix_for(f.name());
        for m in &f.message_type {
            let fq = format!(".{}.{}", f.package(), m.name());
            if special(&fq) {
                continue;
            }
            let Some((key, mut res)) = registry_key(&reg, prefix, m.name(), &counts) else {
                continue;
            };
            if FORMERLY_SKIPPED.contains(&m.name()) && res == Resolution::Direct {
                res = Resolution::NewlyCovered;
            }
            let label = format!("{}::{}", f.name(), m.name());
            let input = ctx.synth_msg(&fq, &mut vec![fq.clone()]);
            let bytes = reg.encode_message(&key, &input).unwrap();
            let mut errs = Vec::new();
            match ctx.dec_msg(&fq, &bytes) {
                Err(e) => errs.push(format!("decode error: {e}")),
                Ok(got) => diff("", &input, &got, &mut errs),
            }
            // `selfLink` was removed from the API in 1.20 and is intentionally
            // never populated; dropping it is not lossy.
            errs.retain(|e| !e.ends_with("/selfLink: dropped"));
            if res == Resolution::Direct {
                out.checked += 1;
                if !errs.is_empty() {
                    out.fails.insert(label, errs);
                }
            } else {
                out.soft_checked += 1;
                if !errs.is_empty() {
                    let why = match res {
                        Resolution::AmbiguousBare => format!(
                            "no qualified registry key `{prefix}.{}`; checked against bare key `{key}`",
                            m.name()
                        ),
                        _ => "formerly skip-listed (Go/JSON field-name divergence)".to_string(),
                    };
                    let mut v = vec![why];
                    v.extend(errs);
                    out.warns.insert(label, v);
                }
            }
        }
    }
    out
}

/// Back-compat shape: (checked, hard failures).
pub(crate) fn run_wire_check() -> (usize, BTreeMap<String, Vec<String>>) {
    let r = run_wire_check_full();
    (r.checked, r.fails)
}

#[test]
fn ambiguous_and_formerly_skipped_messages_wire_check_warn_only() {
    let r = run_wire_check_full();
    eprintln!(
        "wire check (warning-only classes): {} messages, {} warnings",
        r.soft_checked,
        r.warns.len()
    );
    for (k, v) in &r.warns {
        let msg = format!(
            "{k}: {}",
            v.iter().take(4).cloned().collect::<Vec<_>>().join("; ")
        );
        if std::env::var("GITHUB_ACTIONS").is_ok() {
            println!(
                "::warning title=Wire decode (ambiguous/skip-listed)::{msg} (tracked in #3057)"
            );
        } else {
            eprintln!("WARN {msg}");
        }
    }
    // Coverage must actually grow: a regression to the old skip behaviour
    // (checking nothing here) would silently hide wrong field numbers again.
    assert!(
        r.soft_checked > 0,
        "ambiguous/skip-listed messages are no longer wire-checked"
    );
}

#[test]
fn every_registered_upstream_message_decodes_against_upstream_schema() {
    let (checked, fails) = run_wire_check();
    eprintln!("wire check: {checked} messages, {} failing", fails.len());
    for (k, v) in &fails {
        eprintln!("FAIL {k}");
        for e in v.iter().take(6) {
            eprintln!("    {e}");
        }
    }
    assert!(
        checked > 100,
        "suspiciously few messages checked: {checked}"
    );
    assert!(
        fails.is_empty(),
        "{} messages fail the wire check",
        fails.len()
    );
}

#[test]
fn job_status_wire_numbers_match_upstream() {
    // Regression for sig-apps run 38051166411:
    // `proto: wrong wireType = 0 for field FailedIndexes`.
    let reg = ProtoRegistry::new();
    let st = json!({"failedIndexes": "1-3", "terminating": 2});
    let b = reg.encode_message("JobStatus", &st).unwrap();
    // failedIndexes = field 10 (string) => tag 0x52; terminating = field 11 (varint) => tag 0x58
    assert!(
        b.windows(2).any(|w| w == [0x52, 3]),
        "failedIndexes must be #10"
    );
    assert!(
        b.windows(2).any(|w| w == [0x58, 2]),
        "terminating must be #11"
    );
}
