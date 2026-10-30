//! Unit tests moved from `src/validation/structural/prune.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::validation::structural::Structural;
    pub use serde_json::Value;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::structural::prune::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn props(p: &[(&str, Structural)]) -> BTreeMap<String, Structural> {
        p.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    /// algorithm_test.go `TestPrune` "object array" /
    /// "object array with nil schema".
    #[test]
    fn object_array_paths() {
        let mut obj = json!([{"a":1},{"b":1},{"a":1,"b":2,"c":3}]);
        let s = Structural {
            items: Some(Box::new(Structural {
                properties: props(&[("a", Structural::default()), ("c", Structural::default())]),
                ..Default::default()
            })),
            ..Default::default()
        };
        let pruned = prune_with_options(&mut obj, Some(&s), false, true);
        assert_eq!(obj, json!([{"a":1},{},{"a":1,"c":3}]));
        assert_eq!(pruned, vec!["[1].b", "[2].b"]);

        let mut obj = json!([{"a":1},{"b":1},{"a":1,"b":2,"c":3}]);
        let pruned = prune_with_options(&mut obj, None, false, true);
        assert_eq!(obj, json!([{}, {}, {}]));
        assert_eq!(pruned, vec!["[0].a", "[1].b", "[2].a", "[2].b", "[2].c"]);
    }

    /// The root is a resource: top-level meta fields are neither pruned nor
    /// reported (algorithm.go:36-39, :88-92).
    #[test]
    fn resource_root_keeps_meta_and_reports_others() {
        let mut obj = json!({"apiVersion":"v1","kind":"K","metadata":{"x":1},"extra":1,
            "spec":{"a":1,"b":{"c":1}}});
        let s = Structural {
            properties: props(&[(
                "spec",
                Structural {
                    properties: props(&[("a", Structural::default())]),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let pruned = prune_with_options(&mut obj, Some(&s), true, true);
        assert_eq!(pruned, vec!["extra", "spec.b"]);
        assert!(obj.get("metadata").is_some() && obj.get("kind").is_some());
    }

    /// Nothing is recorded unless tracked, and a preserving schema keeps
    /// unknown fields while still pruning declared children.
    #[test]
    fn untracked_and_preserving() {
        let mut obj = json!({"a":1,"keep":{"z":1}});
        let mut s = Structural {
            properties: props(&[(
                "keep",
                Structural {
                    properties: props(&[("y", Structural::default())]),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        s.extensions.x_preserve_unknown_fields = true;
        assert!(prune_with_options(&mut obj.clone(), Some(&s), false, false).is_empty());
        let pruned = prune_with_options(&mut obj, Some(&s), false, true);
        assert_eq!(obj, json!({"a":1,"keep":{}}));
        assert_eq!(pruned, vec!["keep.z"]);
    }
}
