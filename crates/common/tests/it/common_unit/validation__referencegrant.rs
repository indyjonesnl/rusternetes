//! Unit tests moved from `src/validation/referencegrant.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use once_cell::sync::Lazy;
    pub use regex::Regex;
    pub use rusternetes_common::resources::volume::{
        ReferenceGrant, ReferenceGrantFrom, ReferenceGrantSpec,
    };
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::volume::ReferenceGrantTo;
    use rusternetes_common::validation::referencegrant::*;

    fn valid() -> ReferenceGrant {
        ReferenceGrant {
            spec: Some(ReferenceGrantSpec {
                from: Some(vec![ReferenceGrantFrom {
                    group: Some(String::new()),
                    kind: Some("PersistentVolumeClaim".into()),
                    namespace: Some("other".into()),
                }]),
                to: Some(vec![ReferenceGrantTo {
                    group: Some("snapshot.storage.k8s.io".into()),
                    kind: Some("VolumeSnapshot".into()),
                    name: None,
                }]),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn a_valid_grant_passes() {
        assert!(validate_reference_grant(&valid()).is_empty());
    }

    #[test]
    fn an_empty_core_group_is_valid_but_uppercase_is_not() {
        let mut g = valid();
        g.spec.as_mut().unwrap().to.as_mut().unwrap()[0].group = Some("Upper.example".into());
        assert_eq!(validate_reference_grant(&g).len(), 1);
    }

    #[test]
    fn name_must_not_be_empty_when_set() {
        let mut g = valid();
        g.spec.as_mut().unwrap().to.as_mut().unwrap()[0].name = Some(String::new());
        assert_eq!(validate_reference_grant(&g).len(), 1);
    }

    /// `validate.MaxItems` -> `field.TooMany(fldPath, len(value), max)`
    /// (`apimachinery/pkg/api/validate/limits.go:43`) carries the actual count.
    #[test]
    fn too_many_from_items_reports_actual_count() {
        let mut g = valid();
        let spec = g.spec.as_mut().unwrap();
        let item = spec.from.as_ref().unwrap()[0].clone();
        spec.from = Some(vec![item; 17]);
        let errs = validate_reference_grant(&g);
        assert_eq!(
            errs.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            vec!["spec.from: Too many: 17: must have at most 16 items"]
        );
    }

    fn errs_of(spec: serde_json::Value) -> Vec<String> {
        let g: ReferenceGrant = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "g"},
            "spec": spec,
        }))
        .unwrap();
        validate_reference_grant(&g)
            .iter()
            .map(|e| e.to_string())
            .collect()
    }

    /// go-openapi `required` -> `field.Required(path, "")`
    /// (validation.go:177-178): an absent `from` / `to` is `Required`, not the
    /// `minItems` failure an empty array gets.
    #[test]
    fn absent_from_and_to_are_required() {
        assert_eq!(
            errs_of(serde_json::json!({})),
            vec!["spec.from: Required value", "spec.to: Required value"]
        );
        let empty = errs_of(serde_json::json!({"from": [], "to": []}));
        assert_eq!(empty.len(), 2);
        assert!(
            empty[0].contains("should have at least 1 items"),
            "{empty:?}"
        );
    }

    /// `required: [group, kind, namespace]` on `from[]`, `[group, kind]` on
    /// `to[]`: an absent key is `Required`; an empty `group` is present.
    #[test]
    fn absent_item_fields_are_required() {
        assert_eq!(
            errs_of(serde_json::json!({"from": [{}], "to": [{}]})),
            vec![
                "spec.from[0].group: Required value",
                "spec.from[0].kind: Required value",
                "spec.from[0].namespace: Required value",
                "spec.to[0].group: Required value",
                "spec.to[0].kind: Required value",
            ]
        );
        assert!(errs_of(serde_json::json!({
            "from": [{"group": "", "kind": "PersistentVolumeClaim", "namespace": "n"}],
            "to": [{"group": "", "kind": "Secret"}],
        }))
        .is_empty());
    }

    /// `spec` itself is not in the CRD's top-level `required`, so an object
    /// without one is valid.
    #[test]
    fn absent_spec_is_valid() {
        let g: ReferenceGrant =
            serde_json::from_value(serde_json::json!({"metadata": {"name": "g"}})).unwrap();
        assert!(validate_reference_grant(&g).is_empty());
        assert!(serde_json::to_value(&g).unwrap().get("spec").is_none());
    }
}
