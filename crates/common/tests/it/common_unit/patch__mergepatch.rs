//! Unit tests moved from `src/patch/mergepatch.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use serde_json::Value;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::patch::mergepatch::*;
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
