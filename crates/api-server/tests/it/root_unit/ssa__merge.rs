//! Unit tests moved from `src/ssa/merge.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use serde_json::{json, Map, Value};
    pub use std::collections::BTreeSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::ssa::merge::*;
    use serde_json::json;

    #[test]
    fn fields_v1_roundtrip_string_keys() {
        let mut paths = OwnedPaths::new();
        paths.insert("data/foo");
        paths.insert("data/bar");
        paths.insert("metadata/labels/app");
        paths.insert("metadata/annotations/note");

        let encoded = paths.to_fields_v1();
        // Expected shape mirrors upstream fieldsV1.
        assert_eq!(
            encoded,
            json!({
                "f:data": { "f:foo": {}, "f:bar": {} },
                "f:metadata": {
                    "f:labels":      { "f:app": {} },
                    "f:annotations": { "f:note": {} }
                }
            })
        );

        let decoded = OwnedPaths::from_fields_v1(&encoded);
        assert_eq!(decoded, paths);
    }

    #[test]
    fn fields_v1_label_annotation_keys_with_slash_stay_atomic() {
        // Regression: cert-manager labels/annotates Secrets with keys that
        // contain '/' (e.g. controller.cert-manager.io/fao). The fieldsV1 key
        // must be the WHOLE key, not split on the slash — otherwise the applier
        // sees its managed fieldset diverge and re-applies forever (#1057).
        let mut paths = OwnedPaths::new();
        paths.insert("metadata/labels/controller.cert-manager.io/fao");
        paths.insert("metadata/annotations/cert-manager.io/alt-names");
        paths.insert("data/tls.crt");
        paths.insert("type");

        let encoded = paths.to_fields_v1();
        assert_eq!(
            encoded,
            json!({
                "f:type": {},
                "f:data": { "f:tls.crt": {} },
                "f:metadata": {
                    "f:labels":      { "f:controller.cert-manager.io/fao": {} },
                    "f:annotations": { "f:cert-manager.io/alt-names": {} }
                }
            })
        );

        // Round-trips: decoding the (correct) tree yields the same paths.
        assert_eq!(OwnedPaths::from_fields_v1(&encoded), paths);
    }

    #[test]
    fn fields_v1_skips_non_fieldname_discriminators() {
        // `v:` is a value-set discriminator we don't support yet. It must
        // not contaminate the OwnedPaths flat set.
        let encoded = json!({
            "f:spec": {
                "f:containers": {
                    "k:{\"name\":\"nginx\"}": {
                        "f:image": {}
                    }
                }
            }
        });
        let decoded = OwnedPaths::from_fields_v1(&encoded);
        // Nothing under the unsupported `k:` key should appear.
        assert!(decoded.is_empty(), "got: {:?}", decoded);
    }

    #[test]
    fn merge_claims_new_key_when_unowned() {
        let current = Map::new();
        let mut desired = Map::new();
        desired.insert("foo".to_string(), json!("hello"));

        let (merged, outcome) = merge_string_map_group(
            LeafGroup::Data,
            &current,
            &desired,
            "kubectl",
            &Default::default(),
            &OwnedPaths::new(),
            false,
        );

        assert_eq!(merged.get("foo").unwrap(), &json!("hello"));
        assert!(outcome.conflicts.is_empty());
        assert!(outcome.claimed.contains("data/foo"));
        assert!(outcome.released.is_empty());
    }

    #[test]
    fn merge_releases_key_dropped_by_owning_applier() {
        let mut current = Map::new();
        current.insert("foo".to_string(), json!("old"));
        let desired = Map::new();

        let mut previously_owned = OwnedPaths::new();
        previously_owned.insert("data/foo");

        let (merged, outcome) = merge_string_map_group(
            LeafGroup::Data,
            &current,
            &desired,
            "kubectl",
            &Default::default(),
            &previously_owned,
            false,
        );

        assert!(!merged.contains_key("foo"));
        assert!(outcome.released.contains("data/foo"));
        assert!(outcome.claimed.is_empty());
    }

    #[test]
    fn merge_conflict_when_other_owner_and_value_differs() {
        let mut current = Map::new();
        current.insert("foo".to_string(), json!("controller-set"));
        let mut desired = Map::new();
        desired.insert("foo".to_string(), json!("kubectl-set"));

        let mut others = std::collections::BTreeMap::new();
        others.insert("data/foo".to_string(), "controller".to_string());

        // No force.
        let (merged, outcome) = merge_string_map_group(
            LeafGroup::Data,
            &current,
            &desired,
            "kubectl",
            &others,
            &OwnedPaths::new(),
            false,
        );

        assert_eq!(merged.get("foo").unwrap(), &json!("controller-set"));
        assert_eq!(outcome.conflicts.len(), 1);
        assert_eq!(outcome.conflicts[0].current_manager, "controller");
        assert_eq!(outcome.conflicts[0].applying_manager, "kubectl");
        assert!(outcome.claimed.is_empty());

        // Force.
        let (merged_forced, outcome_forced) = merge_string_map_group(
            LeafGroup::Data,
            &current,
            &desired,
            "kubectl",
            &others,
            &OwnedPaths::new(),
            true,
        );
        assert_eq!(merged_forced.get("foo").unwrap(), &json!("kubectl-set"));
        // Conflict still reported even under force so the caller can audit.
        assert_eq!(outcome_forced.conflicts.len(), 1);
        assert!(outcome_forced.claimed.contains("data/foo"));
    }

    #[test]
    fn merge_shared_ownership_when_value_matches() {
        let mut current = Map::new();
        current.insert("foo".to_string(), json!("same"));
        let mut desired = Map::new();
        desired.insert("foo".to_string(), json!("same"));

        let mut others = std::collections::BTreeMap::new();
        others.insert("data/foo".to_string(), "controller".to_string());

        let (merged, outcome) = merge_string_map_group(
            LeafGroup::Data,
            &current,
            &desired,
            "kubectl",
            &others,
            &OwnedPaths::new(),
            false,
        );
        assert_eq!(merged.get("foo").unwrap(), &json!("same"));
        assert!(outcome.conflicts.is_empty());
        assert!(outcome.claimed.contains("data/foo"));
    }
}
