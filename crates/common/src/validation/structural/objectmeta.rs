//! `schemaobjectmeta`: `GetObjectMetaWithOptions`, `SetObjectMeta` and
//! `CoerceWithOptions`.
//!
//! Port of `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/objectmeta/`
//! (`coerce.go:30-151`, `algorithm.go:29-147`), release-1.35.
//!
//! Go decodes `metadata` into a `metav1.ObjectMeta` with
//! `utiljson.Unmarshal` (`kjson.UnmarshalCaseSensitivePreserveInts`) or, to
//! collect unknown fields, `kjson.UnmarshalStrict`
//! (`vendor/sigs.k8s.io/json/json.go:63-130`), and writes it back with
//! `runtime.DefaultUnstructuredConverter.ToUnstructured`. This module keeps
//! the *mechanism* — decode with Go's `encoding/json` semantics (null is a
//! no-op, case-sensitive names, `UnmarshalTypeError` wording, first error
//! wins, strict unknown-field paths) and re-encode with `ToUnstructured`
//! semantics (`omitempty`, `omitzero`, times normalised to UTC seconds) — over
//! a table describing `metav1.ObjectMeta`, `OwnerReference` and
//! `ManagedFieldsEntry` (`apimachinery/pkg/apis/meta/v1/types.go:111-300,
//! 1340-1390`). The typed result is therefore returned in its unstructured
//! form: that is what `SetObjectMeta` would produce from it, and it is the
//! only representation that can carry `selfLink`, which `ObjectMeta` in this
//! crate does not model.
//!
//! Error wording for malformed values is Go's (`UnmarshalTypeError.Error()`,
//! `time.ParseError.Error()`); `Time.UnmarshalJSON` is `apimachinery/pkg/apis/meta/v1/time.go:100-119`.

use chrono::NaiveDate;
use serde_json::{Map, Value};

use super::Structural;
use crate::types::ObjectMeta;
use crate::validation::field::{BadValue, Error, ErrorList, Path};
use crate::validation::hpa::parse_group_version;
use crate::validation::metav1::is_dns1035_label;
use crate::validation::objectmeta::{name_is_path_segment, validate_object_meta};

/// `ObjectMetaOptions` (coerce.go:34-48).
#[derive(Debug, Clone, Default)]
pub struct ObjectMetaOptions {
    /// Discard malformed serialized metadata fields instead of failing.
    pub drop_malformed_fields: bool,
    /// Return the paths of fields not recognised as part of the schema.
    pub return_unknown_field_paths: bool,
    /// The path up to the given ObjectMeta; `None` means the object root.
    pub parent_path: Option<Path>,
}

/// What `GetObjectMetaWithOptions` returns: the decoded `metav1.ObjectMeta`
/// in its `ToUnstructured` form, whether `metadata` was found, and the
/// unknown field paths.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ObjectMetaResult {
    pub meta: Option<Map<String, Value>>,
    pub found: bool,
    pub unknown_field_paths: Vec<String>,
}

// ---------------------------------------------------------------------------
// The shape of the types ObjectMeta decodes into.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Ty {
    /// A Go string kind; carries the Go type name for error messages.
    Str(&'static str),
    Int64,
    Bool,
    /// `metav1.Time` (custom `UnmarshalJSON`).
    Time,
    /// `map[string]string`.
    StrMap,
    /// `[]string`.
    StrSlice,
    /// `[]<struct>`.
    StructSlice(&'static StructDef),
    /// `*metav1.FieldsV1` (custom `UnmarshalJSON` keeping the raw bytes).
    Raw,
}

struct FieldDef {
    name: &'static str,
    ty: Ty,
    /// The Go field is a pointer.
    ptr: bool,
    omit_empty: bool,
    /// `omitzero` (only `creationTimestamp`).
    omit_zero: bool,
}

struct StructDef {
    /// `reflect.Type.Name()`, used by `UnmarshalTypeError.Struct`.
    name: &'static str,
    /// `reflect.Type.String()`, used by `UnmarshalTypeError.Type`.
    go_type: &'static str,
    fields: &'static [FieldDef],
}

const fn f(name: &'static str, ty: Ty, ptr: bool, omit_empty: bool) -> FieldDef {
    FieldDef {
        name,
        ty,
        ptr,
        omit_empty,
        omit_zero: false,
    }
}

static OWNER_REFERENCE: StructDef = StructDef {
    name: "OwnerReference",
    go_type: "v1.OwnerReference",
    fields: &[
        f("apiVersion", Ty::Str("string"), false, false),
        f("kind", Ty::Str("string"), false, false),
        f("name", Ty::Str("string"), false, false),
        f("uid", Ty::Str("types.UID"), false, false),
        f("controller", Ty::Bool, true, true),
        f("blockOwnerDeletion", Ty::Bool, true, true),
    ],
};

static MANAGED_FIELDS_ENTRY: StructDef = StructDef {
    name: "ManagedFieldsEntry",
    go_type: "v1.ManagedFieldsEntry",
    fields: &[
        f("manager", Ty::Str("string"), false, true),
        f(
            "operation",
            Ty::Str("v1.ManagedFieldsOperationType"),
            false,
            true,
        ),
        f("apiVersion", Ty::Str("string"), false, true),
        f("time", Ty::Time, true, true),
        f("fieldsType", Ty::Str("string"), false, true),
        f("fieldsV1", Ty::Raw, true, true),
        f("subresource", Ty::Str("string"), false, true),
    ],
};

static OBJECT_META: StructDef = StructDef {
    name: "ObjectMeta",
    go_type: "v1.ObjectMeta",
    fields: &[
        f("name", Ty::Str("string"), false, true),
        f("generateName", Ty::Str("string"), false, true),
        f("namespace", Ty::Str("string"), false, true),
        f("selfLink", Ty::Str("string"), false, true),
        f("uid", Ty::Str("types.UID"), false, true),
        f("resourceVersion", Ty::Str("string"), false, true),
        f("generation", Ty::Int64, false, true),
        FieldDef {
            name: "creationTimestamp",
            ty: Ty::Time,
            ptr: false,
            omit_empty: true,
            omit_zero: true,
        },
        f("deletionTimestamp", Ty::Time, true, true),
        f("deletionGracePeriodSeconds", Ty::Int64, true, true),
        f("labels", Ty::StrMap, false, true),
        f("annotations", Ty::StrMap, false, true),
        f(
            "ownerReferences",
            Ty::StructSlice(&OWNER_REFERENCE),
            false,
            true,
        ),
        f("finalizers", Ty::StrSlice, false, true),
        f(
            "managedFields",
            Ty::StructSlice(&MANAGED_FIELDS_ENTRY),
            false,
            true,
        ),
    ],
};

// ---------------------------------------------------------------------------
// Decoding with encoding/json semantics.
// ---------------------------------------------------------------------------

/// The `d.errorContext` + `d.strictFieldStack` of
/// `vendor/sigs.k8s.io/json/internal/golang/encoding/json/decode.go`.
struct Decoder {
    strict: bool,
    unknown: Vec<String>,
    /// `d.savedError`: the first `UnmarshalTypeError`.
    saved: Option<String>,
    /// `d.errorContext.Struct` (name of the innermost struct).
    err_struct: &'static str,
    /// `d.errorContext.FieldStack`.
    field_stack: Vec<&'static str>,
    /// `d.strictFieldStack`, joined.
    strict_stack: String,
}

/// The JSON kind Go reports in `UnmarshalTypeError.Value`.
fn json_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Go's `%T` of a value produced by the unstructured JSON decoder.
fn go_type_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "<nil>",
        Value::Bool(_) => "bool",
        Value::Number(n) => {
            if n.is_i64() {
                "int64"
            } else {
                "float64"
            }
        }
        Value::String(_) => "string",
        Value::Array(_) => "[]interface {}",
        Value::Object(_) => "map[string]interface {}",
    }
}

/// How `json.Marshal` renders a number, as `strconv.ParseInt` will see it.
fn number_text(n: &serde_json::Number) -> String {
    if let Some(fl) = n.as_f64() {
        if !n.is_i64() && !n.is_u64() && fl.fract() == 0.0 && fl.abs() < 1e21 {
            return format!("{fl:.0}");
        }
    }
    n.to_string()
}

fn int_value(n: &serde_json::Number) -> Option<i64> {
    n.as_i64().or_else(|| number_text(n).parse::<i64>().ok())
}

impl Decoder {
    fn new(strict: bool) -> Self {
        Decoder {
            strict,
            unknown: Vec::new(),
            saved: None,
            err_struct: "",
            field_stack: Vec::new(),
            strict_stack: String::new(),
        }
    }

    /// `d.saveError(&UnmarshalTypeError{...})` + `addErrorContext`.
    fn type_error(&mut self, value: &str, ty: &str) {
        if self.saved.is_some() {
            return;
        }
        self.saved = Some(
            if self.err_struct.is_empty() && self.field_stack.is_empty() {
                format!("json: cannot unmarshal {value} into Go value of type {ty}")
            } else {
                format!(
                    "json: cannot unmarshal {value} into Go struct field {}.{} of type {ty}",
                    self.err_struct,
                    self.field_stack.join(".")
                )
            },
        );
    }

    fn record_unknown(&mut self, key: &str) {
        if !self.strict {
            return;
        }
        // newFieldError (kubernetes_patch.go:73-85).
        if self.strict_stack.is_empty() {
            self.unknown.push(key.to_string());
        } else {
            self.unknown.push(format!("{}.{key}", self.strict_stack));
        }
    }

    /// `d.object` into a struct. `Err` is a hard error (a custom unmarshaler
    /// failed), which aborts decoding immediately.
    fn decode_struct(
        &mut self,
        def: &'static StructDef,
        v: &Value,
        ty_name: &str,
    ) -> Result<Map<String, Value>, String> {
        let mut out = Map::new();
        let m = match v {
            Value::Null => return Ok(zero_struct(def)),
            Value::Object(m) => m,
            other => {
                self.type_error(json_kind(other), ty_name);
                return Ok(zero_struct(def));
            }
        };
        let orig_stack_len = self.field_stack.len();
        let orig_struct = self.err_struct;
        let orig_strict = self.strict_stack.clone();
        // json.Marshal of a map sorts the keys, which fixes the order of
        // "first error wins" and of the unknown-field paths.
        let mut keys: Vec<&String> = m.keys().collect();
        keys.sort();
        let mut decoded: Vec<(&'static FieldDef, Option<Value>)> = Vec::new();
        for key in keys {
            let val = &m[key];
            let Some(field) = def.fields.iter().find(|fd| fd.name == key.as_str()) else {
                self.record_unknown(key);
                continue;
            };
            self.err_struct = def.name;
            self.field_stack.push(field.name);
            if self.strict {
                if !self.strict_stack.is_empty() {
                    self.strict_stack.push('.');
                }
                self.strict_stack.push_str(field.name);
            }
            let dv = self.decode_field(field, val)?;
            decoded.push((field, dv));
            self.field_stack.truncate(orig_stack_len);
            self.err_struct = orig_struct;
            self.strict_stack = orig_strict.clone();
        }
        // ToUnstructured of the decoded struct.
        for fd in def.fields {
            let dv = decoded
                .iter()
                .find(|(d, _)| std::ptr::eq(*d, fd))
                .and_then(|(_, v)| v.clone());
            emit_field(&mut out, fd, dv);
        }
        Ok(out)
    }

    /// Decode one field value; `Ok(None)` is the Go zero value (nil pointer,
    /// zero scalar, nil map/slice), `Ok(Some(_))` the value to keep.
    fn decode_field(&mut self, fd: &'static FieldDef, v: &Value) -> Result<Option<Value>, String> {
        if v.is_null() {
            // literalStore on null: a no-op (pointers are nilled; Time's
            // UnmarshalJSON resets it; FieldsV1 keeps nil).
            return Ok(None);
        }
        match fd.ty {
            Ty::Str(go) => Ok(match v {
                Value::String(s) => Some(Value::String(s.clone())),
                other => {
                    self.type_error(json_kind(other), go);
                    None
                }
            }),
            Ty::Bool => Ok(match v {
                Value::Bool(b) => Some(Value::Bool(*b)),
                other => {
                    self.type_error(json_kind(other), "bool");
                    None
                }
            }),
            Ty::Int64 => Ok(match v {
                Value::Number(n) => match int_value(n) {
                    Some(i) => Some(Value::from(i)),
                    None => {
                        self.type_error(&format!("number {}", number_text(n)), "int64");
                        None
                    }
                },
                other => {
                    self.type_error(json_kind(other), "int64");
                    None
                }
            }),
            Ty::Time => match v {
                // Time.UnmarshalJSON: json.Unmarshal(b, &str) then time.Parse.
                Value::String(s) => match parse_rfc3339(s)? {
                    Some(t) => Ok(Some(Value::String(t))),
                    // zero time: omitted by omitzero, but a non-nil
                    // pointer marshals to null.
                    None => Ok(Some(Value::Null)),
                },
                other => {
                    // The nested json.Unmarshal's UnmarshalTypeError reaches
                    // the outer addErrorContext, which fills Struct/Field.
                    // It is returned, not saved: a hard error.
                    let kind = json_kind(other);
                    Err(format!(
                        "json: cannot unmarshal {kind} into Go struct field {}.{} of type string",
                        self.err_struct,
                        self.field_stack.join(".")
                    ))
                }
            },
            Ty::StrMap => Ok(match v {
                Value::Object(m) => {
                    let mut keys: Vec<&String> = m.keys().collect();
                    keys.sort();
                    let mut out = Map::new();
                    for k in keys {
                        match &m[k] {
                            Value::String(s) => {
                                out.insert(k.clone(), Value::String(s.clone()));
                            }
                            Value::Null => {
                                out.insert(k.clone(), Value::String(String::new()));
                            }
                            other => {
                                self.type_error(json_kind(other), "string");
                            }
                        }
                    }
                    Some(Value::Object(out))
                }
                other => {
                    self.type_error(json_kind(other), "map[string]string");
                    None
                }
            }),
            Ty::StrSlice => Ok(match v {
                Value::Array(a) => {
                    let mut out = Vec::new();
                    for item in a {
                        match item {
                            Value::String(s) => out.push(Value::String(s.clone())),
                            Value::Null => out.push(Value::String(String::new())),
                            other => {
                                self.type_error(json_kind(other), "string");
                                out.push(Value::String(String::new()));
                            }
                        }
                    }
                    Some(Value::Array(out))
                }
                other => {
                    self.type_error(json_kind(other), "[]string");
                    None
                }
            }),
            Ty::StructSlice(def) => match v {
                Value::Array(a) => {
                    let orig = self.strict_stack.clone();
                    let mut out = Vec::new();
                    for (i, item) in a.iter().enumerate() {
                        if self.strict {
                            self.strict_stack = format!("{orig}[{i}]");
                        }
                        // A struct element keeps the enclosing struct
                        // context unless it is an object, in which case
                        // decode_struct switches to the element type.
                        out.push(Value::Object(self.decode_struct(def, item, def.go_type)?));
                    }
                    self.strict_stack = orig;
                    Ok(Some(Value::Array(out)))
                }
                other => {
                    self.type_error(json_kind(other), &format!("[]{}", def.go_type));
                    Ok(None)
                }
            },
            Ty::Raw => Ok(Some(v.clone())),
        }
    }
}

/// A struct decoded from `null`/a wrong type: the zero value.
fn zero_struct(def: &'static StructDef) -> Map<String, Value> {
    let mut out = Map::new();
    for fd in def.fields {
        emit_field(&mut out, fd, None);
    }
    out
}

/// `fromUnstructured`'s inverse, `toUnstructured` (runtime/converter.go:
/// 826-833): `omitempty` and `omitzero` skip zero fields; fields tagged
/// neither are always written.
fn emit_field(out: &mut Map<String, Value>, fd: &FieldDef, dv: Option<Value>) {
    let is_zero_time_value = matches!(dv, Some(Value::Null));
    let value = match (dv, fd.ptr) {
        // nil pointer.
        (None, true) => return,
        (None, false) => zero_value(fd.ty),
        (Some(v), _) => v,
    };
    if fd.omit_zero && is_zero_time_value {
        return;
    }
    if fd.omit_empty && !fd.ptr && is_empty(&value) {
        return;
    }
    out.insert(fd.name.to_string(), value);
}

fn zero_value(ty: Ty) -> Value {
    match ty {
        Ty::Str(_) => Value::String(String::new()),
        Ty::Int64 => Value::from(0),
        Ty::Bool => Value::Bool(false),
        Ty::Time => Value::Null,
        Ty::StrMap | Ty::StrSlice | Ty::StructSlice(_) | Ty::Raw => Value::Null,
    }
}

/// `isEmpty` (runtime/converter.go) for the kinds ObjectMeta uses.
fn is_empty(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Bool(b) => !*b,
        Value::Number(n) => n.as_i64() == Some(0),
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(m) => m.is_empty(),
    }
}

// ---------------------------------------------------------------------------
// time.Parse(time.RFC3339, s), with Go's error text.
// ---------------------------------------------------------------------------

const RFC3339: &str = "2006-01-02T15:04:05Z07:00";

fn parse_error(value: &str, layout_elem: &str, value_elem: &str) -> String {
    format!("parsing time {value:?} as {RFC3339:?}: cannot parse {value_elem:?} as {layout_elem:?}")
}

fn parse_error_msg(value: &str, msg: &str) -> String {
    format!("parsing time {value:?}{msg}")
}

fn digits(s: &str, n: usize) -> Option<u32> {
    let b = s.as_bytes();
    if b.len() < n || !b[..n].iter().all(u8::is_ascii_digit) {
        return None;
    }
    s[..n].parse().ok()
}

/// Returns `Ok(None)` for the zero time, `Ok(Some(rfc3339-utc))` otherwise,
/// and `Err(go error text)` when Go's `time.Parse` rejects `value`.
fn parse_rfc3339(value: &str) -> Result<Option<String>, String> {
    let orig = value;
    let mut rest = value;
    let take_num = |rest: &mut &str, n: usize, std: &str| -> Result<u32, String> {
        match digits(rest, n) {
            Some(v) => {
                *rest = &rest[n..];
                Ok(v)
            }
            None => Err(parse_error(orig, std, rest)),
        }
    };
    let skip = |rest: &mut &str, prefix: &str| -> Result<(), String> {
        if rest.starts_with(prefix) {
            *rest = &rest[prefix.len()..];
            Ok(())
        } else {
            Err(parse_error(orig, prefix, rest))
        }
    };
    let range = |name: &str| parse_error_msg(orig, &format!(": {name} out of range"));

    let year = take_num(&mut rest, 4, "2006")?;
    skip(&mut rest, "-")?;
    let month = take_num(&mut rest, 2, "01")?;
    if !(1..=12).contains(&month) {
        return Err(range("month"));
    }
    skip(&mut rest, "-")?;
    let day = take_num(&mut rest, 2, "02")?;
    skip(&mut rest, "T")?;
    let hour = take_num(&mut rest, 2, "15")?;
    if hour >= 24 {
        return Err(range("hour"));
    }
    skip(&mut rest, ":")?;
    let min = take_num(&mut rest, 2, "04")?;
    if min >= 60 {
        return Err(range("minute"));
    }
    skip(&mut rest, ":")?;
    let sec = take_num(&mut rest, 2, "05")?;
    if sec >= 60 {
        return Err(range("second"));
    }
    // A fractional second is accepted even though the layout has none
    // (time/format.go "Special case: do we have a fractional second but no
    // fractional second in the format?").
    let b = rest.as_bytes();
    if b.len() >= 2 && (b[0] == b'.' || b[0] == b',') && b[1].is_ascii_digit() {
        let n = 1 + b[1..].iter().take_while(|c| c.is_ascii_digit()).count();
        rest = &rest[n..];
    }
    let offset_secs: i64 = if let Some(r) = rest.strip_prefix('Z') {
        rest = r;
        0
    } else {
        let b = rest.as_bytes();
        if b.len() < 6 || b[3] != b':' || (b[0] != b'+' && b[0] != b'-') {
            return Err(parse_error(orig, "Z07:00", rest));
        }
        let (Some(hh), Some(mm)) = (digits(&rest[1..], 2), digits(&rest[4..], 2)) else {
            return Err(parse_error(orig, "Z07:00", rest));
        };
        if hh > 24 {
            return Err(parse_error_msg(
                orig,
                ": time zone offset hour out of range",
            ));
        }
        if mm > 60 {
            return Err(parse_error_msg(
                orig,
                ": time zone offset minute out of range",
            ));
        }
        let secs = i64::from(hh) * 3600 + i64::from(mm) * 60;
        let signed = if b[0] == b'-' { -secs } else { secs };
        rest = &rest[6..];
        signed
    };
    if !rest.is_empty() {
        return Err(parse_error_msg(orig, &format!(": extra text: {rest:?}")));
    }
    let date = NaiveDate::from_ymd_opt(year as i32, month, day)
        .ok_or_else(|| parse_error_msg(orig, ": day out of range"))?;
    let local = date
        .and_hms_opt(hour, min, sec)
        .ok_or_else(|| parse_error_msg(orig, ": hour out of range"))?;
    let utc = local - chrono::Duration::seconds(offset_secs);
    if Some(utc) == NaiveDate::from_ymd_opt(1, 1, 1).and_then(|d| d.and_hms_opt(0, 0, 0)) {
        return Ok(None);
    }
    Ok(Some(utc.format("%Y-%m-%dT%H:%M:%SZ").to_string()))
}

// ---------------------------------------------------------------------------
// GetObjectMetaWithOptions / SetObjectMeta.
// ---------------------------------------------------------------------------

/// One `Unmarshal` of `metadata` into `metav1.ObjectMeta`: the typed result
/// (unstructured form) or Go's error, plus the strict unknown paths.
fn unmarshal_meta(
    metadata: &Value,
    strict: bool,
) -> Result<(Map<String, Value>, Vec<String>), String> {
    let mut d = Decoder::new(strict);
    let meta = d.decode_struct(&OBJECT_META, metadata, OBJECT_META.go_type)?;
    match d.saved.take() {
        Some(e) => Err(e),
        None => Ok((meta, d.unknown)),
    }
}

/// `GetObjectMetaWithOptions` (coerce.go:62-135).
pub fn get_object_meta_with_options(
    obj: &Map<String, Value>,
    opts: &ObjectMetaOptions,
) -> Result<ObjectMetaResult, String> {
    let Some(metadata) = obj.get("metadata") else {
        return Ok(ObjectMetaResult::default());
    };
    let prefix = || child(opts.parent_path.as_ref(), "metadata").to_string();

    // round-trip through JSON first, hoping that unmarshalling just works
    let unmarshal_err = match unmarshal_meta(metadata, opts.return_unknown_field_paths) {
        Ok((meta, unknown)) => {
            let paths = unknown
                .into_iter()
                .map(|p| format!("{}.{p}", prefix()))
                .collect();
            return Ok(ObjectMetaResult {
                meta: Some(meta),
                found: true,
                unknown_field_paths: paths,
            });
        }
        Err(e) => e,
    };
    if !opts.drop_malformed_fields {
        // if we're not trying to drop malformed fields, return the error
        return Err(unmarshal_err);
    }

    let Some(metadata_map) = metadata.as_object() else {
        return Err(format!(
            "invalid metadata: expected object, got {}",
            go_type_of(metadata)
        ));
    };

    // Go field by field accumulating into the metadata object.
    let mut accumulated = Map::new();
    let mut unknown_fields = Vec::new();
    let mut keys: Vec<&String> = metadata_map.keys().collect();
    keys.sort();
    for k in keys {
        let single = Value::Object(Map::from_iter([(k.clone(), metadata_map[k].clone())]));
        // do a test unmarshal
        if unmarshal_meta(&single, false).is_err() {
            continue;
        }
        // if that succeeds, unmarshal for real
        let Ok((part, unknown)) = unmarshal_meta(&single, opts.return_unknown_field_paths) else {
            continue;
        };
        unknown_fields.extend(unknown.into_iter().map(|p| format!("{}.{p}", prefix())));
        for (pk, pv) in part {
            accumulated.insert(pk, pv);
        }
    }
    Ok(ObjectMetaResult {
        meta: Some(accumulated),
        found: true,
        unknown_field_paths: unknown_fields,
    })
}

/// `GetObjectMeta` (coerce.go:25-31).
pub fn get_object_meta(
    obj: &Map<String, Value>,
    drop_malformed_fields: bool,
) -> Result<ObjectMetaResult, String> {
    get_object_meta_with_options(
        obj,
        &ObjectMetaOptions {
            drop_malformed_fields,
            ..Default::default()
        },
    )
}

/// `SetObjectMeta` (coerce.go:137-151): writes the decoded ObjectMeta back;
/// `None` removes `metadata`.
pub fn set_object_meta(obj: &mut Map<String, Value>, meta: Option<Map<String, Value>>) {
    match meta {
        None => {
            obj.remove("metadata");
        }
        Some(m) => {
            obj.insert("metadata".to_string(), Value::Object(m));
        }
    }
}

// ---------------------------------------------------------------------------
// CoerceWithOptions.
// ---------------------------------------------------------------------------

/// `CoerceOptions` (algorithm.go:29-40).
#[derive(Debug, Clone, Copy, Default)]
pub struct CoerceOptions {
    /// Discard malformed serialized metadata fields; wrong-typed
    /// `apiVersion`/`kind` are dropped too.
    pub drop_invalid_fields: bool,
    /// Return the paths of fields not recognised as part of the schema.
    pub return_unknown_field_paths: bool,
}

/// `(*field.Path).Child` on a possibly-nil path.
fn child(p: Option<&Path>, name: &str) -> Path {
    match p {
        Some(p) => p.child(name),
        None => Path::new(name),
    }
}

fn key(p: Option<&Path>, k: &str) -> Path {
    match p {
        Some(p) => p.key(k),
        None => Path::default().key(k),
    }
}

fn index(p: Option<&Path>, i: usize) -> Path {
    match p {
        Some(p) => p.index(i),
        None => Path::default().index(i),
    }
}

/// `CoerceWithOptions` (algorithm.go:42-69): checks the types of embedded
/// ObjectMeta and TypeMeta and prunes unknown fields inside the former. It
/// also coerces ObjectMeta and TypeMeta at the root if `is_resource_root`.
/// Returns the first field error and the sorted unknown field paths.
pub fn coerce_with_options(
    pth: Option<&Path>,
    obj: &mut Value,
    s: Option<&Structural>,
    is_resource_root: bool,
    opts: CoerceOptions,
) -> (Option<Error>, Vec<String>) {
    let mut unknown = Vec::new();
    let err = if is_resource_root {
        let mut clone = s.cloned().unwrap_or_default();
        clone.extensions.x_embedded_resource = true;
        coerce(pth, obj, Some(&clone), opts, &mut unknown)
    } else {
        coerce(pth, obj, s, opts, &mut unknown)
    };
    unknown.sort();
    (err, unknown)
}

/// `Coerce` (algorithm.go:71-75): `CoerceWithOptions` without the paths.
pub fn coerce_without_paths(
    pth: Option<&Path>,
    obj: &mut Value,
    s: Option<&Structural>,
    is_resource_root: bool,
    drop_invalid_fields: bool,
) -> Option<Error> {
    coerce_with_options(
        pth,
        obj,
        s,
        is_resource_root,
        CoerceOptions {
            drop_invalid_fields,
            return_unknown_field_paths: false,
        },
    )
    .0
}

/// `coercer.coerce` (algorithm.go:82-147).
fn coerce(
    pth: Option<&Path>,
    x: &mut Value,
    s: Option<&Structural>,
    opts: CoerceOptions,
    unknown: &mut Vec<String>,
) -> Option<Error> {
    let s = s?;
    match x {
        Value::Object(map) => {
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            for k in keys {
                // Go's `v` is the value at iteration time; SetObjectMeta
                // below replaces map[k], leaving `v` the detached original.
                let mut detached: Option<Value> = None;
                if s.extensions.x_embedded_resource {
                    match k.as_str() {
                        "apiVersion" | "kind" => {
                            let is_str = matches!(map.get(&k), Some(Value::String(_)));
                            if !is_str && opts.drop_invalid_fields {
                                map.remove(&k);
                                continue;
                            } else if !is_str {
                                let v = map.get(&k).cloned().unwrap_or(Value::Null);
                                return Some(Error::invalid(
                                    &child(pth, &k),
                                    BadValue::Json(v),
                                    "must be a string",
                                ));
                            }
                        }
                        "metadata" => {
                            let original = map.get(&k).cloned().unwrap_or(Value::Null);
                            let got = get_object_meta_with_options(
                                map,
                                &ObjectMetaOptions {
                                    drop_malformed_fields: opts.drop_invalid_fields,
                                    return_unknown_field_paths: opts.return_unknown_field_paths,
                                    parent_path: pth.cloned(),
                                },
                            );
                            match got {
                                Err(e) => {
                                    if !opts.drop_invalid_fields {
                                        return Some(Error::invalid(
                                            &child(pth, "metadata"),
                                            BadValue::Json(original),
                                            e,
                                        ));
                                    }
                                    // pass through on error if DropInvalidFields is true
                                }
                                Ok(r) => {
                                    unknown.extend(r.unknown_field_paths);
                                    if r.found {
                                        let meta = r.meta;
                                        let creation_zero = meta
                                            .as_ref()
                                            .is_none_or(|m| !m.contains_key("creationTimestamp"));
                                        set_object_meta(map, meta);
                                        if creation_zero {
                                            if let Some(Value::Object(m)) = map.get_mut("metadata")
                                            {
                                                m.remove("creationTimestamp");
                                            }
                                        }
                                        detached = Some(original);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                if let Some(prop) = s.properties.get(&k) {
                    let p = child(pth, &k);
                    let err = match detached.as_mut() {
                        Some(d) => coerce(Some(&p), d, Some(prop), opts, unknown),
                        None => match map.get_mut(&k) {
                            Some(v) => coerce(Some(&p), v, Some(prop), opts, unknown),
                            None => None,
                        },
                    };
                    if err.is_some() {
                        return err;
                    }
                } else if let Some(ap) = s.additional_properties.as_deref() {
                    let p = key(pth, &k);
                    let err = match detached.as_mut() {
                        Some(d) => coerce(Some(&p), d, ap.structural.as_ref(), opts, unknown),
                        None => match map.get_mut(&k) {
                            Some(v) => coerce(Some(&p), v, ap.structural.as_ref(), opts, unknown),
                            None => None,
                        },
                    };
                    if err.is_some() {
                        return err;
                    }
                }
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter_mut().enumerate() {
                let p = index(pth, i);
                if let Some(err) = coerce(Some(&p), v, s.items.as_deref(), opts, unknown) {
                    return Some(err);
                }
            }
        }
        _ => {}
    }
    None
}

// ---------------------------------------------------------------------------
// Validate (validation.go).
// ---------------------------------------------------------------------------

/// `Validate` (validation.go:30-43): validates embedded ObjectMeta and
/// TypeMeta — every `x-kubernetes-embedded-resource` object must carry a
/// valid `apiVersion` and `kind`, and its `metadata` passes
/// `ValidateObjectMeta`. It also validates those at the root if
/// `is_resource_root`.
pub fn validate(
    pth: Option<&Path>,
    obj: &Value,
    s: Option<&Structural>,
    is_resource_root: bool,
) -> ErrorList {
    if is_resource_root {
        let mut clone = s.cloned().unwrap_or_default();
        clone.extensions.x_embedded_resource = true;
        return validate_value(pth, obj, Some(&clone));
    }
    validate_value(pth, obj, s)
}

/// `validate` (validation.go:45-73).
fn validate_value(pth: Option<&Path>, x: &Value, s: Option<&Structural>) -> ErrorList {
    let Some(s) = s else {
        return Vec::new();
    };
    let mut errs = Vec::new();
    match x {
        Value::Object(map) => {
            if s.extensions.x_embedded_resource {
                errs.extend(validate_embedded_resource(pth, map, s));
            }
            for (k, v) in map {
                if let Some(prop) = s.properties.get(k) {
                    errs.extend(validate_value(Some(&child(pth, k)), v, Some(prop)));
                } else if let Some(ap) = s.additional_properties.as_deref() {
                    errs.extend(validate_value(
                        Some(&key(pth, k)),
                        v,
                        ap.structural.as_ref(),
                    ));
                }
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                errs.extend(validate_value(Some(&index(pth, i)), v, s.items.as_deref()));
            }
        }
        // scalars, do nothing
        _ => {}
    }
    errs
}

/// `validateEmbeddedResource` (validation.go:75-123). Go ranges over the map
/// in random order; the keys are visited sorted here, which is one of the
/// orders Go may produce.
fn validate_embedded_resource(
    pth: Option<&Path>,
    x: &Map<String, Value>,
    _s: &Structural,
) -> ErrorList {
    let mut errs = Vec::new();

    // require apiVersion and kind, but not metadata
    if !x.contains_key("apiVersion") {
        errs.push(Error::required(&child(pth, "apiVersion"), ""));
    }
    if !x.contains_key("kind") {
        errs.push(Error::required(&child(pth, "kind"), ""));
    }

    for (k, v) in x {
        match k.as_str() {
            "apiVersion" => {
                let p = child(pth, "apiVersion");
                match v {
                    Value::String(api_version) if api_version.is_empty() => {
                        errs.push(Error::invalid(&p, api_version.clone(), "must not be empty"));
                    }
                    Value::String(api_version) => {
                        if let Err(e) = parse_group_version(api_version) {
                            errs.push(Error::invalid(&p, api_version.clone(), e));
                        }
                    }
                    other => {
                        errs.push(Error::invalid(
                            &p,
                            BadValue::Json(other.clone()),
                            "must be a string",
                        ));
                    }
                }
            }
            "kind" => {
                let p = child(pth, "kind");
                match v {
                    Value::String(kind) if kind.is_empty() => {
                        errs.push(Error::invalid(&p, kind.clone(), "must not be empty"));
                    }
                    Value::String(kind) => {
                        let msgs = is_dns1035_label(&kind.to_lowercase());
                        if !msgs.is_empty() {
                            errs.push(Error::invalid(
                                &p,
                                kind.clone(),
                                format!(
                                    "may have mixed case, but should otherwise match: {}",
                                    msgs.join(",")
                                ),
                            ));
                        }
                    }
                    other => {
                        errs.push(Error::invalid(
                            &p,
                            BadValue::Json(other.clone()),
                            "must be a string",
                        ));
                    }
                }
            }
            "metadata" => {
                let p = child(pth, "metadata");
                let meta = get_object_meta(x, false).and_then(|r| {
                    // GetObjectMeta returns the decoded `metav1.ObjectMeta`;
                    // here it comes back in its ToUnstructured form, which the
                    // typed `ObjectMeta` reads.
                    serde_json::from_value::<ObjectMeta>(Value::Object(r.meta.unwrap_or_default()))
                        .map_err(|e| e.to_string())
                });
                match meta {
                    Err(e) => {
                        errs.push(Error::invalid(&p, BadValue::Json(v.clone()), e));
                    }
                    Ok(mut meta) => {
                        if meta.name.is_empty() {
                            // we have to set something to avoid an error
                            meta.name = "fakename".to_string();
                        }
                        let namespaced = meta.namespace.as_deref().is_some_and(|n| !n.is_empty());
                        errs.extend(validate_object_meta(
                            &meta,
                            namespaced,
                            name_is_path_segment,
                            &p,
                        ));
                    }
                }
            }
            _ => {}
        }
    }

    errs
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod validation_tests;
