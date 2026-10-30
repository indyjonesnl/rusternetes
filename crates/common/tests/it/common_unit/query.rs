//! Unit tests moved from `src/query.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {}

mod tests {
    use super::__parent::*;
    use rusternetes_common::query::*;

    #[test]
    fn only_zero_and_false_are_false() {
        for f in ["0", "false", "False", "FALSE", "fAlSe"] {
            assert!(!k8s_query_bool(f), "{f:?} must be false");
        }
    }

    #[test]
    fn everything_else_is_true_including_empty() {
        // Upstream: "Any other value (including empty string) resolves to true."
        for t in [
            "1", "true", "True", "TRUE", "t", "T", "yes", "on", "banana", "",
        ] {
            assert!(k8s_query_bool(t), "{t:?} must be true");
        }
    }

    #[test]
    fn diverges_from_parse_bool_on_single_letters() {
        // Go's strconv.ParseBool reads "f"/"F" as false; the query conversion
        // does not, because it only special-cases "0" and "false".
        assert!(k8s_query_bool("f"));
        assert!(k8s_query_bool("F"));
    }

    #[test]
    fn absence_is_preserved_for_pointer_bool_params() {
        assert_eq!(k8s_query_bool_opt(None), None);
        assert_eq!(k8s_query_bool_opt(Some("1")), Some(true));
        assert_eq!(k8s_query_bool_opt(Some("0")), Some(false));
    }
}
