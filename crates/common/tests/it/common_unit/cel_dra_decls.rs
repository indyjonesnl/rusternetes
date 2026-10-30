//! Unit tests moved from `src/cel_dra_decls.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::HashMap;
    pub use std::sync::OnceLock;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::cel_dra_decls::*;

    fn call(name: &str, member: bool, args: &[DType]) -> Option<DType> {
        match resolve(lookup(name)?, member, args) {
            Resolution::Resolved(t) => Some(t),
            Resolution::NoMatch(_) => None,
        }
    }

    #[test]
    fn table_parses_and_every_overload_has_its_call_style() {
        for (name, ovs) in table() {
            assert!(!ovs.is_empty(), "{name}");
            for o in ovs {
                if o.member {
                    assert!(!o.args.is_empty(), "{name} member overload has no receiver");
                }
            }
        }
    }

    #[test]
    fn arithmetic_needs_matching_types() {
        assert_eq!(
            call("_+_", false, &[DType::Int, DType::Int]),
            Some(DType::Int)
        );
        assert_eq!(call("_+_", false, &[DType::Int, DType::Str]), None);
        // dyn is assignable to everything
        assert_eq!(
            call("_+_", false, &[DType::Int, DType::Dyn]),
            Some(DType::Int)
        );
    }

    #[test]
    fn equality_binds_one_type_parameter() {
        assert_eq!(
            call("_==_", false, &[DType::Str, DType::Str]),
            Some(DType::Bool)
        );
        assert_eq!(call("_==_", false, &[DType::Int, DType::Double]), None);
        assert_eq!(
            call("_==_", false, &[DType::Any, DType::Double]),
            Some(DType::Bool)
        );
    }

    #[test]
    fn member_and_global_overloads_are_separate() {
        assert_eq!(
            call("startsWith", true, &[DType::Str, DType::Str]),
            Some(DType::Bool)
        );
        assert_eq!(call("startsWith", false, &[DType::Str, DType::Str]), None);
        assert_eq!(
            call("size", false, &[DType::List(Box::new(DType::Int))]),
            Some(DType::Int)
        );
    }

    #[test]
    fn list_overloads_unify_element_types() {
        let li = DType::List(Box::new(DType::Int));
        assert_eq!(
            call("_+_", false, &[li.clone(), li.clone()]),
            Some(li.clone())
        );
        assert_eq!(
            call(
                "_+_",
                false,
                &[li.clone(), DType::List(Box::new(DType::Str))]
            ),
            None
        );
        assert_eq!(call("indexOf", true, &[li, DType::Int]), Some(DType::Int));
    }

    #[test]
    fn namespaced_functions_and_undeclared_ones() {
        assert!(lookup("sets.contains").is_some());
        assert!(lookup("strings.quote").is_some());
        assert!(lookup("format.named").is_some());
        assert!(lookup("math.greatest").is_none());
        assert!(lookup("base64.encode").is_none());
    }

    #[test]
    fn signature_formatting() {
        assert_eq!(
            format_signature(&[DType::Int, DType::Str], false),
            "(int, string)"
        );
        assert_eq!(
            format_signature(&[DType::Str, DType::Int], true),
            "string.(int)"
        );
    }
}
