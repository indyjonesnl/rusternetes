//! Unit tests moved from `src/validation/field.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::fmt;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::field::*;

    /// #2086: a struct-valued bad value renders in field order, not sorted.
    /// Go: `json.Marshal` keeps struct field order (errors.go:92-97).
    #[test]
    fn marshaled_bad_value_keeps_struct_field_order() {
        #[derive(serde::Serialize)]
        struct S {
            #[serde(rename = "MinAvailable")]
            min: &'static str,
            #[serde(rename = "Selector")]
            sel: Option<u8>,
            #[serde(rename = "MaxUnavailable")]
            max: u8,
        }
        let e = Error::invalid(
            &Path::new("foo"),
            BadValue::marshal(&S {
                min: "10%",
                sel: None,
                max: 1,
            }),
            "d",
        );
        assert_eq!(
            e.error_body(),
            r#"Invalid value: {"MinAvailable":"10%","Selector":null,"MaxUnavailable":1}: d"#
        );
    }

    #[test]
    fn path_render_matches_upstream() {
        let p = Path::new("metadata").child("labels").key("app");
        assert_eq!(p.to_string(), "metadata.labels[app]");
        let p2 = Path::new("status")
            .child("conditions")
            .index(2)
            .child("type");
        assert_eq!(p2.to_string(), "status.conditions[2].type");
    }

    #[test]
    fn invalid_string_quoting() {
        let p = Path::new("status")
            .child("conditions")
            .index(0)
            .child("type");
        let e = Error::invalid(&p, ":invalid", "name part must");
        assert_eq!(
            e.to_string(),
            "status.conditions[0].type: Invalid value: \":invalid\": name part must"
        );
    }

    #[test]
    fn required_omits_value() {
        let p = Path::new("status")
            .child("conditions")
            .index(0)
            .child("lastTransitionTime");
        let e = Error::required(&p, "");
        assert_eq!(
            e.to_string(),
            "status.conditions[0].lastTransitionTime: Required value"
        );
    }

    #[test]
    fn duplicate_quotes_value() {
        let p = Path::new("status")
            .child("conditions")
            .index(2)
            .child("type");
        let e = Error::duplicate(&p, "First");
        assert_eq!(
            e.to_string(),
            "status.conditions[2].type: Duplicate value: \"First\""
        );
    }

    #[test]
    fn not_supported_renders_supported_list() {
        let p = Path::new("status")
            .child("conditions")
            .index(0)
            .child("status");
        let e = Error::not_supported(&p, "unknown", &["False", "True", "Unknown"]);
        assert_eq!(
            e.to_string(),
            "status.conditions[0].status: Unsupported value: \"unknown\": supported values: \"False\", \"True\", \"Unknown\""
        );
    }

    #[test]
    fn invalid_int_no_quotes() {
        let p = Path::new("status")
            .child("conditions")
            .index(0)
            .child("observedGeneration");
        let e = Error::invalid(&p, -1i64, "must be greater than or equal to zero");
        assert_eq!(
            e.to_string(),
            "status.conditions[0].observedGeneration: Invalid value: -1: must be greater than or equal to zero"
        );
    }
}
