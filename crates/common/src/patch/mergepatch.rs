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
pub(crate) const GO_SLICE: &str = "[]interface {}";

/// `mergepatch.ErrNoMergeKey` (errors.go:35-37):
/// `map: %v does not contain declared merge key: %s`.
pub(crate) fn err_no_merge_key(m: &Value, k: &str) -> String {
    format!(
        "map: {} does not contain declared merge key: {k}",
        go_fmt_v(m)
    )
}

/// `mergepatch.ErrBadArgType` (errors.go:39-43):
/// `expected a %s, but received a %s` over `reflect.TypeOf` of both. A nil
/// `reflect.Type` under `%s` prints `%!s(<nil>)`.
pub(crate) fn err_bad_arg_type(expected: &str, actual: &Value) -> String {
    format!(
        "expected a {expected}, but received a {}",
        go_type_name(actual).unwrap_or("%!s(<nil>)")
    )
}

/// `mergepatch.ErrBadPatchType` (errors.go:60-62):
/// `unknown patch type: %s in map: %v`.
pub(crate) fn err_bad_patch_type(t: &Value, m: &Value) -> String {
    format!(
        "unknown patch type: {} in map: {}",
        go_fmt(t, Verb::S, true),
        go_fmt_v(m)
    )
}

/// `fmt.Sprintf("%v", v)`.
pub(crate) fn go_fmt_v(v: &Value) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn go_fmt_v_matches_fmt_for_decoded_json() {
        assert_eq!(go_fmt_v(&json!({"b": 1, "a": "x"})), "map[a:x b:1]");
        assert_eq!(
            go_fmt_v(&json!([{"name": "c"}, [1, 2], null, true])),
            "[map[name:c] [1 2] <nil> true]"
        );
        assert_eq!(go_fmt_v(&json!({})), "map[]");
        assert_eq!(go_fmt_v(&json!([])), "[]");
        assert_eq!(go_fmt_v(&json!(null)), "<nil>");
        assert_eq!(go_fmt_v(&json!(1.5)), "1.5");
        assert_eq!(go_fmt_v(&json!(1.0)), "1");
        assert_eq!(go_fmt_v(&json!(123456.0)), "123456");
        assert_eq!(go_fmt_v(&json!(1000000.0)), "1e+06");
        assert_eq!(go_fmt_v(&json!(1234567.5)), "1.2345675e+06");
        assert_eq!(go_fmt_v(&json!(0.0001)), "0.0001");
        assert_eq!(go_fmt_v(&json!(0.00001)), "1e-05");
        assert_eq!(go_fmt_v(&json!(-2.5e-300)), "-2.5e-300");
        assert_eq!(go_fmt_v(&json!(1000000)), "1000000");
    }

    #[test]
    fn err_bad_patch_type_formats_directive_with_percent_s() {
        assert_eq!(
            err_bad_patch_type(&json!("foo"), &json!({"$patch": "foo", "x": "y"})),
            "unknown patch type: foo in map: map[$patch:foo x:y]"
        );
        assert_eq!(
            err_bad_patch_type(&json!(5), &json!({"$patch": 5})),
            "unknown patch type: %!s(int64=5) in map: map[$patch:5]"
        );
        assert_eq!(
            err_bad_patch_type(&json!(true), &json!({"$patch": true})),
            "unknown patch type: %!s(bool=true) in map: map[$patch:true]"
        );
        assert_eq!(
            err_bad_patch_type(&json!(null), &json!({"$patch": null})),
            "unknown patch type: %!s(<nil>) in map: map[$patch:<nil>]"
        );
        assert_eq!(
            err_bad_patch_type(&json!(["a", 1]), &json!({})),
            "unknown patch type: [a %!s(int64=1)] in map: map[]"
        );
    }

    #[test]
    fn err_bad_arg_type_and_no_merge_key() {
        assert_eq!(
            err_bad_arg_type(GO_SLICE, &json!("x")),
            "expected a []interface {}, but received a string"
        );
        assert_eq!(
            err_bad_arg_type(GO_SLICE, &json!({"a": 1})),
            "expected a []interface {}, but received a map[string]interface {}"
        );
        assert_eq!(
            err_bad_arg_type(GO_SLICE, &json!(null)),
            "expected a []interface {}, but received a %!s(<nil>)"
        );
        assert_eq!(
            err_bad_arg_type(GO_SLICE, &json!(2.5)),
            "expected a []interface {}, but received a float64"
        );
        assert_eq!(
            err_no_merge_key(&json!({"image": "x"}), "name"),
            "map: map[image:x] does not contain declared merge key: name"
        );
    }
}
