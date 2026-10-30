//! Unit tests moved from `src/validation/priorityclass.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::PriorityClass;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
}

mod update_tests {
    use super::__parent::*;
    use rusternetes_common::validation::priorityclass::*;

    fn pc(value: i32, preemption: Option<&str>) -> PriorityClass {
        let mut v = serde_json::json!({
            "metadata": {"name": "high"},
            "value": value,
        });
        if let Some(p) = preemption {
            v["preemptionPolicy"] = serde_json::json!(p);
        }
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn unchanged_passes() {
        assert!(
            validate_priority_class_update(&pc(100, Some("Never")), &pc(100, Some("Never")))
                .is_empty()
        );
    }

    #[test]
    fn value_immutable() {
        let errs = validate_priority_class_update(&pc(200, None), &pc(100, None));
        assert!(
            errs.iter()
                .any(|e| e.field == "value" && e.detail.contains("may not be changed")),
            "{errs:?}"
        );
    }

    #[test]
    fn preemption_policy_immutable() {
        let errs = validate_priority_class_update(
            &pc(100, Some("Never")),
            &pc(100, Some("PreemptLowerPriority")),
        );
        assert!(
            errs.iter()
                .any(|e| e.field == "preemptionPolicy" && e.detail == "field is immutable"),
            "{errs:?}"
        );
    }
}
