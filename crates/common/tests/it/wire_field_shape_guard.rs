//! Guard: the two serialization rules that apply to *every* resource must be
//! visible in the type, not left to each struct to remember.
//!
//! Both rules were broken one struct at a time and found one struct at a time,
//! most recently by the wide serialization sweep in
//! `crates/api-server/tests/it/conformance_wide_serialization_sweep_test.rs`.
//! That sweep can only reach fields a client can send on a create, so it
//! cannot see a status field a controller fills in later — `lastTransitionTime`
//! on a condition, `managedFields[].time`, `NodeMetrics.timestamp`. This file
//! covers what it cannot, by reading the declarations instead of the wire.
//!
//! # Rule 1 — a timestamp needs an explicit serializer
//!
//! `chrono`'s default `Serialize` for `DateTime<Utc>` writes **nanoseconds**.
//! Upstream never does:
//!
//! * `metav1.Time` uses `time.RFC3339`
//!   (`staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/time.go:167-170`),
//!   layout `"2006-01-02T15:04:05Z07:00"` — no fractional part at all.
//! * `metav1.MicroTime` uses `RFC3339Micro`
//!   (`micro_time.go:26,181`), `"2006-01-02T15:04:05.000000Z07:00"` — exactly
//!   six digits.
//!
//! So a `DateTime<Utc>` on the wire with no `serialize_with` is a
//! non-conformance by default. It also breaks protobuf `Timestamp`
//! round-tripping and made the duplicated DRA `ObjectMeta` emit nanosecond
//! `creationTimestamp`s (#1895).
//!
//! # Rule 2 — `metadata` and the TypeMeta pair must decode when absent
//!
//! Upstream decodes a create body with
//! `decoder.Decode(body, &defaultGVK, obj)`
//! (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go`), where
//! `defaultGVK` comes from the *request path*. `apiVersion` and `kind` in the
//! body are therefore optional, and `ObjectMeta` is a value type that decodes
//! from an absent `metadata` — validation, not the decoder, is what rejects a
//! missing name, and it does so with a `Status` object.
//!
//! When the Rust field is a bare non-`Option` with no `#[serde(default)]`,
//! serde rejects the body before any of that: the client gets axum's plain
//! `422 Failed to deserialize the JSON body` with no `Status`, no `reason`,
//! and no field path. The sweep hit exactly this on the webhook
//! configurations, the validating-admission policies and
//! `SelfSubjectRulesReview`, and on the CRD it was worse — the create was
//! accepted, and the *read-back* then failed with
//! `400 missing field 'apiVersion'`: an object that could be written and never
//! read.
//!
//! Note `kind` is only covered on a struct that also declares
//! `metadata: ObjectMeta`, i.e. where `kind` sits in TypeMeta position. On a
//! reference struct — `TypedLocalObjectReference`, `RoleRef`, `Subject`,
//! `ParamKind` — `kind` is genuinely required upstream, and defaulting it
//! there would silently accept an under-specified reference.

use std::path::{Path, PathBuf};

fn common_src() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The contiguous attribute / doc-comment block immediately above `line`,
/// joined into one string.
///
/// Walking up line-by-line is not enough: a `#[serde(...)]` attribute is often
/// spread over several lines, and stopping at the first line that does not
/// itself start with `#[` reads only its closing `)]` — which made an earlier
/// version of this scan report `ObjectMeta::creation_timestamp` (correctly
/// wired since forever) as unserialized. Balance the brackets instead.
fn attributes_above(lines: &[&str], line: usize) -> String {
    let mut j = line as isize - 1;
    let mut block: Vec<&str> = Vec::new();
    while j >= 0 {
        let t = lines[j as usize].trim();
        if t.starts_with("#[") || t.ends_with(']') {
            let mut depth: isize = 0;
            let mut k = j;
            while k >= 0 {
                let l = lines[k as usize];
                depth += l.matches(']').count() as isize - l.matches('[').count() as isize;
                block.insert(0, l);
                if depth <= 0 && l.trim().starts_with("#[") {
                    break;
                }
                k -= 1;
            }
            j = k - 1;
        } else if t.starts_with("//") || t.is_empty() {
            block.insert(0, lines[j as usize]);
            j -= 1;
        } else {
            break;
        }
    }
    block.join("\n")
}

/// Is the struct containing `line` a serde type at all? A plain internal
/// struct (the event correlator's in-memory state, say) never reaches the wire
/// and needs no serializer.
fn in_serde_struct(lines: &[&str], line: usize) -> bool {
    let mut start = None;
    for j in (0..=line).rev() {
        if lines[j].starts_with("pub struct ") || lines[j].starts_with("struct ") {
            start = Some(j);
            break;
        }
    }
    let Some(start) = start else { return false };
    lines[start.saturating_sub(8)..start]
        .iter()
        .any(|l| l.contains("derive(") && (l.contains("Serialize") || l.contains("Deserialize")))
}

/// Field name → declared type, for `pub <name>: <type>,` on one line.
fn field_decl(line: &str) -> Option<(&str, &str)> {
    let t = line.trim();
    let rest = t.strip_prefix("pub ")?;
    let (name, ty) = rest.split_once(": ")?;
    let ty = ty.strip_suffix(',')?;
    if name.contains(' ') || ty.contains(' ') && !ty.starts_with("Option<") {
        return None;
    }
    Some((name, ty))
}

#[test]
fn every_wire_timestamp_declares_a_kubernetes_serializer() {
    let mut files = Vec::new();
    rust_sources(&common_src(), &mut files);
    files.sort();

    let mut offenders: Vec<String> = Vec::new();
    let mut checked = 0usize;

    for path in &files {
        let src = std::fs::read_to_string(path).expect("read source");
        let lines: Vec<&str> = src.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            let Some((name, ty)) = field_decl(line) else {
                continue;
            };
            if ty != "DateTime<Utc>" && ty != "Option<DateTime<Utc>>" {
                continue;
            }
            if !in_serde_struct(&lines, i) {
                continue;
            }
            checked += 1;
            let attrs = attributes_above(&lines, i);
            if attrs.contains("k8s_time") || attrs.contains("k8s_micro_time") {
                continue;
            }
            offenders.push(format!(
                "{}:{} `{name}: {ty}` has no `serialize_with`, so chrono writes it with \
                 nanosecond precision",
                path.display(),
                i + 1,
            ));
        }
    }

    assert!(
        checked > 30,
        "only {checked} timestamp fields were examined — the scan broke and an \
         empty guard passes vacuously"
    );
    assert!(
        offenders.is_empty(),
        "{} wire timestamp field(s) fall back to chrono's nanosecond format. \
         Upstream `metav1.Time` is RFC3339 seconds and `metav1.MicroTime` is \
         exactly six digits; use `crate::types::k8s_time`, `k8s_time_required`, \
         `k8s_micro_time` or `k8s_micro_time_required`:\n  {}",
        offenders.len(),
        offenders.join("\n  "),
    );
}

#[test]
fn metadata_and_typemeta_decode_when_the_body_omits_them() {
    let mut files = Vec::new();
    rust_sources(&common_src().join("resources"), &mut files);
    files.sort();

    let mut offenders: Vec<String> = Vec::new();
    let mut checked = 0usize;

    for path in &files {
        let src = std::fs::read_to_string(path).expect("read source");
        let lines: Vec<&str> = src.lines().collect();

        // Which struct bodies declare `metadata: ObjectMeta`? Those are the
        // object structs, and only there is `kind` in TypeMeta position.
        let mut object_struct_ranges: Vec<(usize, usize)> = Vec::new();
        for (i, line) in lines.iter().enumerate() {
            if !line.starts_with("pub struct ") || !line.trim_end().ends_with('{') {
                continue;
            }
            let mut depth = 0i32;
            for (j, l) in lines.iter().enumerate().skip(i) {
                depth += l.matches('{').count() as i32 - l.matches('}').count() as i32;
                if depth == 0 {
                    if lines[i..=j]
                        .iter()
                        .any(|f| f.trim() == "pub metadata: ObjectMeta,")
                    {
                        object_struct_ranges.push((i, j));
                    }
                    break;
                }
            }
        }
        let in_object_struct =
            |i: usize| object_struct_ranges.iter().any(|(a, b)| *a <= i && i <= *b);

        for (i, line) in lines.iter().enumerate() {
            let Some((name, ty)) = field_decl(line) else {
                continue;
            };
            let is_metadata = name == "metadata" && ty == "ObjectMeta";
            let is_type_meta =
                matches!(name, "api_version" | "kind") && ty == "String" && in_object_struct(i);
            if !is_metadata && !is_type_meta {
                continue;
            }
            checked += 1;
            let attrs = attributes_above(&lines, i);
            // `default` alone, `default = "..."`, or a `flatten`ed TypeMeta
            // (which carries its own defaults) all satisfy the rule.
            if attrs.contains("default") || attrs.contains("flatten") {
                continue;
            }
            offenders.push(format!(
                "{}:{} `{name}: {ty}` is required at decode time",
                path.display(),
                i + 1,
            ));
        }
    }

    assert!(
        checked > 40,
        "only {checked} fields were examined — the scan broke"
    );
    assert!(
        offenders.is_empty(),
        "{} field(s) make a legitimate request body undecodable. Upstream takes \
         the GVK from the request path and `ObjectMeta` decodes from an absent \
         `metadata`, so serde must not reject the body before validation runs — \
         add `#[serde(default)]`:\n  {}",
        offenders.len(),
        offenders.join("\n  "),
    );
}

/// # Rule 3 — every field of an object struct must decode when absent
///
/// Rule 2 is the two-field case of a rule that holds for the whole body. Go has
/// no required JSON fields: `spec`, `rules`, `roleRef`, `subsets` and the rest
/// are plain struct fields, so an absent key decodes to the zero value and the
/// object reaches validation, which answers 422 with a field path. A bare
/// non-`Option` Rust field answers 400 instead, before any validator, with no
/// `details.causes` — and for a strategy that allows create-on-update it also
/// changes the outcome (#1931).
///
/// The obligation `#[serde(default)]` carries is that *some validator rejects
/// the zero value*. A field that defaults and then passes validation silently
/// accepts an invalid object, which is worse than the 400. The wire-side sweep
/// `crates/api-server/tests/it/minimal_body_decodes_test.rs` is what checks
/// that half.
#[test]
fn every_field_of_an_object_struct_decodes_when_absent() {
    let mut files = Vec::new();
    rust_sources(&common_src().join("resources"), &mut files);
    files.sort();

    let mut offenders: Vec<String> = Vec::new();
    let mut checked = 0usize;

    for path in &files {
        let src = std::fs::read_to_string(path).expect("read source");
        let lines: Vec<&str> = src.lines().collect();

        for (start, end) in object_struct_ranges(&lines) {
            for i in (start + 1)..end {
                let Some((name, ty)) = field_decl_generic(lines[i]) else {
                    continue;
                };
                // `Option` decodes from an absent key on its own; `metadata`
                // and the TypeMeta pair are rule 2's business.
                if ty.starts_with("Option<") || matches!(name, "metadata" | "api_version" | "kind")
                {
                    continue;
                }
                checked += 1;
                let attrs = attributes_above(&lines, i);
                if attrs.contains("default") || attrs.contains("flatten") {
                    continue;
                }
                offenders.push(format!(
                    "{}:{} `{name}: {ty}` on {} is required at decode time",
                    path.display(),
                    i + 1,
                    lines[start].trim(),
                ));
            }
        }
    }

    assert!(
        checked > 90,
        "only {checked} object-struct fields were examined — the scan broke and \
         an empty guard passes vacuously"
    );
    assert!(
        offenders.is_empty(),
        "{} field(s) of a top-level API object are required at decode time, so \
         a body upstream decodes answers 400 BadRequest here instead of the 422 \
         Invalid a client can act on. Add `#[serde(default)]` (and `Default` \
         down the field's type tree) — and confirm a validator rejects the zero \
         value, or the object is silently accepted:\n  {}",
        offenders.len(),
        offenders.join("\n  "),
    );
}

/// # Rule 4 — every field of a status condition must decode when absent
///
/// Rule 3 stops at the fields of the object struct, so it cannot see a field
/// *inside* a status. Conditions are the first slice of that nested surface
/// (#1939) and the one clients hit: a condition list is what a controller
/// writes and what a client sends back when it PUTs an object it read.
///
/// Upstream decodes a condition like everything else — an absent `status`,
/// `reason` or `message` becomes `""` — and then either
///
/// * validates it, for the `[]metav1.Condition` lists:
///   `metav1validation.ValidateCondition` requires `type`, `status`, `reason`
///   and `lastTransitionTime`
///   (`staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/validation/validation.go:315-350`),
///   called from `pkg/apis/policy/validation/validation.go:82`,
///   `certificates/validation/validation.go:764`,
///   `resource/validation/validation.go:1288,1494` and
///   `admissionregistration/validation/validation.go:1259`; or
/// * accepts it, for the older typed conditions (`DeploymentCondition`,
///   `PodCondition`, `JobCondition`, …) that upstream validates nowhere.
///
/// Either way the answer is a `Status` a client can act on, or a write. A bare
/// non-`Option` Rust field answers serde's 400 before any of that. The wire
/// half is `crates/api-server/tests/it/condition_decodes_when_absent_test.rs`.
#[test]
fn every_field_of_a_status_condition_decodes_when_absent() {
    let mut files = Vec::new();
    rust_sources(&common_src().join("resources"), &mut files);
    files.sort();

    let mut offenders: Vec<String> = Vec::new();
    let mut checked = 0usize;

    for path in &files {
        let src = std::fs::read_to_string(path).expect("read source");
        let lines: Vec<&str> = src.lines().collect();

        for (start, end) in condition_struct_ranges(&lines) {
            for i in (start + 1)..end {
                let Some((name, ty)) = field_decl_generic(lines[i]) else {
                    continue;
                };
                if ty.starts_with("Option<") {
                    continue;
                }
                checked += 1;
                let attrs = attributes_above(&lines, i);
                if attrs.contains("default") || attrs.contains("flatten") {
                    continue;
                }
                offenders.push(format!(
                    "{}:{} `{name}: {ty}` on {} is required at decode time",
                    path.display(),
                    i + 1,
                    lines[start].trim(),
                ));
            }
        }
    }

    assert!(
        checked > 40,
        "only {checked} condition fields were examined — the scan broke and an \
         empty guard passes vacuously"
    );
    assert!(
        offenders.is_empty(),
        "{} condition field(s) are required at decode time, so a status a \
         controller or a client sends answers 400 BadRequest from serde instead \
         of the 422 Invalid (or the write) upstream answers. Add \
         `#[serde(default)]`:\n  {}",
        offenders.len(),
        offenders.join("\n  "),
    );
}

/// `(first, last)` line of every struct body shaped like a status condition:
/// it declares a field serialized as `type` and one serialized as `status`.
///
/// Keyed on the shape rather than on the name ending in `Condition`, so a
/// struct that merely borrows the word — `MatchCondition`, which is a CEL
/// name/expression pair — is not swept in, and one that does not borrow it is
/// not missed.
fn condition_struct_ranges(lines: &[&str]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        if !line.starts_with("pub struct ") || !line.trim_end().ends_with('{') {
            continue;
        }
        let mut depth = 0i32;
        for (j, l) in lines.iter().enumerate().skip(i) {
            depth += l.matches('{').count() as i32 - l.matches('}').count() as i32;
            if depth == 0 {
                let mut has_type = false;
                let mut has_status = false;
                for k in (i + 1)..j {
                    let Some((name, _)) = field_decl_generic(lines[k]) else {
                        continue;
                    };
                    let serde_name_is_type = matches!(name, "condition_type" | "type_" | "r#type")
                        || attributes_above(lines, k).contains("rename = \"type\"");
                    has_type |= serde_name_is_type;
                    has_status |= name == "status";
                }
                if has_type && has_status {
                    out.push((i, j));
                }
                break;
            }
        }
    }
    out
}

/// `(first, last)` line of every struct body that declares `metadata:
/// ObjectMeta` — i.e. every top-level API object.
fn object_struct_ranges(lines: &[&str]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        if !line.starts_with("pub struct ") || !line.trim_end().ends_with('{') {
            continue;
        }
        let mut depth = 0i32;
        for (j, l) in lines.iter().enumerate().skip(i) {
            depth += l.matches('{').count() as i32 - l.matches('}').count() as i32;
            if depth == 0 {
                if lines[i..=j]
                    .iter()
                    .any(|f| f.trim() == "pub metadata: ObjectMeta,")
                {
                    out.push((i, j));
                }
                break;
            }
        }
    }
    out
}

/// Like [`field_decl`], but keeps a generic type whose arguments contain a
/// comma — `BTreeMap<String, String>` was invisible to the simpler parser, and
/// an invisible field is an unguarded one.
fn field_decl_generic(line: &str) -> Option<(&str, &str)> {
    let t = line.trim();
    let rest = t.strip_prefix("pub ")?;
    let (name, ty) = rest.split_once(": ")?;
    let ty = ty.strip_suffix(',')?;
    if name.contains(' ') || name.contains('(') {
        return None;
    }
    // A type is one token or a generic application; anything with a space
    // outside angle brackets is not a field declaration.
    let outside_space = {
        let mut depth = 0i32;
        ty.chars().any(|c| {
            match c {
                '<' => depth += 1,
                '>' => depth -= 1,
                _ => {}
            }
            c == ' ' && depth == 0
        })
    };
    if outside_space {
        return None;
    }
    Some((name, ty))
}

/// Neither scan above can be trusted on its own: each one is a text search
/// whose *scope* is the thing under test, and a scope bug reads as a pass.
/// Pin both detectors against hand-written sources, including the shapes they
/// must leave alone.
#[test]
fn the_scans_flag_exactly_the_wrong_declarations() {
    // Multi-line attributes must be read whole — the failure mode that once
    // made this scan report ObjectMeta's own creationTimestamp.
    let multi_line = vec![
        "#[derive(Serialize, Deserialize)]",
        "pub struct Meta {",
        "    #[serde(",
        "        skip_serializing_if = \"Option::is_none\",",
        "        serialize_with = \"k8s_time::serialize\",",
        "        deserialize_with = \"k8s_time::deserialize\",",
        "        default",
        "    )]",
        "    pub creation_timestamp: Option<DateTime<Utc>>,",
        "}",
    ];
    assert!(
        attributes_above(&multi_line, 8).contains("k8s_time"),
        "a multi-line #[serde(...)] block must be read in full"
    );
    assert!(in_serde_struct(&multi_line, 8));

    // A doc comment between the attribute and the field must not hide it.
    let with_doc = vec![
        "#[derive(Serialize)]",
        "pub struct S {",
        "    #[serde(serialize_with = \"k8s_time::serialize\")]",
        "    /// when it happened",
        "    pub time: Option<DateTime<Utc>>,",
        "}",
    ];
    assert!(attributes_above(&with_doc, 4).contains("k8s_time"));

    // A bare timestamp has nothing above it to find.
    let bare = vec![
        "#[derive(Serialize)]",
        "pub struct S {",
        "    pub status: String,",
        "    pub last_transition_time: Option<DateTime<Utc>>,",
        "}",
    ];
    assert!(!attributes_above(&bare, 3).contains("k8s_time"));

    // A struct with no serde derive is not a wire type.
    let internal = vec![
        "#[derive(Debug, Clone)]",
        "pub struct CorrelatorEvent {",
        "    pub first_timestamp: DateTime<Utc>,",
        "}",
    ];
    assert!(!in_serde_struct(&internal, 2));

    // field_decl must not mistake a method or a where-clause for a field.
    assert_eq!(
        field_decl("    pub metadata: ObjectMeta,"),
        Some(("metadata", "ObjectMeta"))
    );
    assert_eq!(field_decl("    pub fn name(&self) -> &str {"), None);
    assert_eq!(field_decl("    metadata: ObjectMeta,"), None);

    // field_decl_generic must see through a comma inside a generic — the
    // omission that hid `usage: BTreeMap<String, String>` from rule 3.
    assert_eq!(
        field_decl_generic("    pub usage: BTreeMap<String, String>,"),
        Some(("usage", "BTreeMap<String, String>"))
    );
    assert_eq!(
        field_decl_generic("    pub spec: DeploymentSpec,"),
        Some(("spec", "DeploymentSpec"))
    );
    assert_eq!(field_decl_generic("    pub fn name(&self) -> &str {"), None);
    assert_eq!(field_decl_generic("    metadata: ObjectMeta,"), None);

    // condition_struct_ranges must key on the shape, not the name: a type +
    // status pair is a condition, a name + expression pair is not, however it
    // is called.
    let conditions = vec![
        "pub struct ThingCondition {",
        "    #[serde(rename = \"type\")]",
        "    pub condition_type: String,",
        "    pub status: String,",
        "}",
        "pub struct MatchCondition {",
        "    pub name: String,",
        "    pub expression: String,",
        "}",
        "pub struct ThingSpec {",
        "    pub status: String,",
        "}",
    ];
    assert_eq!(condition_struct_ranges(&conditions), vec![(0, 4)]);

    // And object_struct_ranges must pick the object struct, not its spec.
    let two = vec![
        "pub struct Widget {",
        "    pub metadata: ObjectMeta,",
        "    pub spec: WidgetSpec,",
        "}",
        "pub struct WidgetSpec {",
        "    pub replicas: i32,",
        "}",
    ];
    assert_eq!(object_struct_ranges(&two), vec![(0, 3)]);
}
