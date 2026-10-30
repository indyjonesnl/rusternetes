//! Port of the strategic-merge-patch error constructors in
//! `k8s.io/apimachinery/pkg/util/mergepatch/errors.go:35-62`, plus the
//! slice of Go's `fmt` they rely on to render a decoded JSON value.
//!
//! The apiserver decodes a patch body with
//! `kjson.UnmarshalCaseSensitivePreserveInts` (apiserver
//! `endpoints/handlers/patch.go:567`) and the live object with
//! `DefaultUnstructuredConverter.ToUnstructured` (:554), so every value these
//! messages print is one of `nil`, `bool`, `int64`, `float64`, `string`,
//! `[]interface{}` or `map[string]interface{}`. The helpers below render
//! exactly those types the way `fmt` does.

use serde_json::Value;

/// `reflect.TypeOf(v).String()` for a decoded JSON value; `None` for `nil`
/// (`reflect.TypeOf(nil)` is a nil `reflect.Type`).
pub(crate) fn go_type_name(v: &Value) -> Option<&'static str> {
    match v {
        Value::Null => None,
        Value::Bool(_) => Some("bool"),
        Value::Number(n) if n.is_i64() || n.is_u64() => Some("int64"),
        Value::Number(_) => Some("float64"),
        Value::String(_) => Some("string"),
        Value::Array(_) => Some("[]interface {}"),
        Value::Object(_) => Some("map[string]interface {}"),
    }
}

/// The Go type name `[]interface {}` — what `ErrBadArgType` prints for the
/// typed-nil `[]interface{}` "expected" argument at every slice assertion.
pub const GO_SLICE: &str = "[]interface {}";

/// `mergepatch.ErrNoMergeKey` (errors.go:35-37):
/// `map: %v does not contain declared merge key: %s`.
pub fn err_no_merge_key(m: &Value, k: &str) -> String {
    format!(
        "map: {} does not contain declared merge key: {k}",
        go_fmt_v(m)
    )
}

/// `mergepatch.ErrBadArgType` (errors.go:39-43):
/// `expected a %s, but received a %s` over `reflect.TypeOf` of both. A nil
/// `reflect.Type` under `%s` prints `%!s(<nil>)`.
pub fn err_bad_arg_type(expected: &str, actual: &Value) -> String {
    format!(
        "expected a {expected}, but received a {}",
        go_type_name(actual).unwrap_or("%!s(<nil>)")
    )
}

/// `mergepatch.ErrBadPatchType` (errors.go:60-62):
/// `unknown patch type: %s in map: %v`.
pub fn err_bad_patch_type(t: &Value, m: &Value) -> String {
    format!(
        "unknown patch type: {} in map: {}",
        go_fmt(t, Verb::S, true),
        go_fmt_v(m)
    )
}

/// `fmt.Sprintf("%v", v)`.
pub fn go_fmt_v(v: &Value) -> String {
    go_fmt(v, Verb::V, true)
}

#[derive(Clone, Copy, PartialEq)]
enum Verb {
    V,
    S,
}

/// `pp.printArg` / `pp.printValue` (fmt/print.go) for the decoded-JSON
/// types. Maps print as `map[k:v ...]` with keys sorted (fmt sorts map keys,
/// internal/fmtsort), slices as `[a b]`. A nil at the top level under `%s`
/// is `badVerb` (`%!s(<nil>)`); a nil nested in a container prints
/// `<nil>` whatever the verb (printValue's `reflect.Interface` arm). A
/// non-string scalar under `%s` is `badVerb`: `%!s(int64=5)`.
fn go_fmt(v: &Value, verb: Verb, top: bool) -> String {
    match v {
        Value::Null => {
            if top && verb == Verb::S {
                "%!s(<nil>)".to_string()
            } else {
                "<nil>".to_string()
            }
        }
        Value::String(s) => s.clone(),
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(|i| go_fmt(i, verb, false)).collect();
            format!("[{}]", parts.join(" "))
        }
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let parts: Vec<String> = keys
                .into_iter()
                .map(|k| format!("{k}:{}", go_fmt(&m[k], verb, false)))
                .collect();
            format!("map[{}]", parts.join(" "))
        }
        Value::Bool(_) | Value::Number(_) => {
            let rendered = match v {
                Value::Bool(b) => b.to_string(),
                Value::Number(n) => go_number_v(n),
                _ => unreachable!(),
            };
            match verb {
                Verb::V => rendered,
                Verb::S => format!(
                    "%!s({}={rendered})",
                    go_type_name(v).expect("non-nil scalar")
                ),
            }
        }
    }
}

/// `%v` of an `int64` (decimal) or a `float64`: `strconv.FormatFloat(f,
/// 'g', -1, 64)` (fmt/print.go `fmtFloat` 'v' → 'g' shortest). Shortest
/// `%g` switches to `%e` when the exponent is `< -4` or `>= 6`
/// (strconv/ftoa.go: "if precision was the shortest possible, use precision
/// 6 for this decision"), with an at-least-two-digit exponent.
fn go_number_v(n: &serde_json::Number) -> String {
    if let Some(i) = n.as_i64() {
        return i.to_string();
    }
    if let Some(u) = n.as_u64() {
        return u.to_string();
    }
    let f = n.as_f64().unwrap_or(0.0);
    let sci = format!("{f:e}");
    let (mantissa, exp) = sci.split_once('e').expect("{:e} always has an exponent");
    let exp: i32 = exp.parse().expect("integer exponent");
    if !(-4..6).contains(&exp) {
        let sign = if exp < 0 { '-' } else { '+' };
        format!("{mantissa}e{sign}{:02}", exp.abs())
    } else {
        format!("{f}")
    }
}
