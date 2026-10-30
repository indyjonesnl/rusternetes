//! Unit tests moved from `src/validation/configmap.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::ConfigMap;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
    };
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::ConfigMap;
    use rusternetes_common::validation::configmap::*;
    use std::collections::HashMap;

    fn cm() -> ConfigMap {
        ConfigMap {
            type_meta: Default::default(),
            metadata: rusternetes_common::types::ObjectMeta {
                name: "cm".to_string(),
                namespace: Some("default".to_string()),
                resource_version: Some("1".to_string()),
                ..Default::default()
            },
            data: None,
            binary_data: None,
            immutable: None,
        }
    }

    #[test]
    fn config_map_valid_keys_pass() {
        let mut c = cm();
        let mut data = HashMap::new();
        data.insert("good.key-1_2".to_string(), "v".to_string());
        c.data = Some(data);
        assert!(validate_config_map(&c).is_empty());
    }

    #[test]
    fn config_map_invalid_key_char_fails() {
        let mut c = cm();
        let mut data = HashMap::new();
        data.insert("bad key!".to_string(), "v".to_string());
        c.data = Some(data);
        let errs = validate_config_map(&c);
        assert!(errs
            .iter()
            .any(|e| e.detail.contains("a valid config key must consist")));
    }

    #[test]
    fn config_map_chdir_keys_fail_with_distinct_messages() {
        assert_eq!(config_map_key_errors("."), vec!["must not be '.'"]);
        assert_eq!(config_map_key_errors(".."), vec!["must not be '..'"]);
        assert_eq!(
            config_map_key_errors("..foo"),
            vec!["must not start with '..'"]
        );
        // A single leading dot is allowed (e.g. `.dockercfg`).
        assert!(config_map_key_errors(".foo").is_empty());
    }

    #[test]
    fn config_map_long_key_fails() {
        let key = "a".repeat(254);
        let errs = config_map_key_errors(&key);
        assert!(errs.iter().any(|m| m.contains("no more than 253")));
    }

    #[test]
    fn config_map_duplicate_key_across_bags_fails() {
        let mut c = cm();
        let mut data = HashMap::new();
        data.insert("dup".to_string(), "v".to_string());
        c.data = Some(data);
        let mut bin = HashMap::new();
        bin.insert("dup".to_string(), vec![1u8, 2, 3]);
        c.binary_data = Some(bin);
        let errs = validate_config_map(&c);
        assert!(errs
            .iter()
            .any(|e| e.detail.contains("duplicate of key present in binaryData")));
    }

    #[test]
    fn config_map_oversize_fails() {
        let mut c = cm();
        let mut data = HashMap::new();
        data.insert("big".to_string(), "x".repeat(MAX_SECRET_SIZE + 1));
        c.data = Some(data);
        let errs = validate_config_map(&c);
        assert!(!errs.is_empty());
    }

    #[test]
    fn config_map_at_size_limit_passes() {
        let mut c = cm();
        let mut data = HashMap::new();
        // key length counts toward IsConfigMapKey only, not total size; total
        // size is the sum of value lengths.
        data.insert("k".to_string(), "x".repeat(MAX_SECRET_SIZE));
        c.data = Some(data);
        assert!(validate_config_map(&c).is_empty());
    }

    #[test]
    fn config_map_update_mutable_allows_data_change() {
        let mut old = cm();
        let mut new = cm();
        let mut d1 = HashMap::new();
        d1.insert("a".to_string(), "1".to_string());
        old.data = Some(d1);
        let mut d2 = HashMap::new();
        d2.insert("a".to_string(), "2".to_string());
        new.data = Some(d2);
        assert!(validate_config_map_update(&old, &new).is_empty());
    }

    #[test]
    fn config_map_update_immutable_forbids_data_change() {
        let mut old = cm();
        old.immutable = Some(true);
        let mut d1 = HashMap::new();
        d1.insert("a".to_string(), "1".to_string());
        old.data = Some(d1);

        let mut new = cm();
        new.immutable = Some(true);
        let mut d2 = HashMap::new();
        d2.insert("a".to_string(), "2".to_string());
        new.data = Some(d2);

        let errs = validate_config_map_update(&old, &new);
        assert!(errs.iter().any(|e| e.field == "data"
            && e.detail
                .contains("field is immutable when `immutable` is set")));
    }

    #[test]
    fn config_map_update_immutable_forbids_clearing_immutable_flag() {
        let mut old = cm();
        old.immutable = Some(true);
        let mut new = cm();
        new.immutable = Some(false);
        let errs = validate_config_map_update(&old, &new);
        assert!(errs.iter().any(|e| e.field == "immutable"));
        // Clearing immutable to None is also forbidden.
        let mut new2 = cm();
        new2.immutable = None;
        let errs2 = validate_config_map_update(&old, &new2);
        assert!(errs2.iter().any(|e| e.field == "immutable"));
    }

    #[test]
    fn config_map_update_immutable_forbids_binary_data_change() {
        let mut old = cm();
        old.immutable = Some(true);
        let mut b1 = HashMap::new();
        b1.insert("k".to_string(), vec![1u8]);
        old.binary_data = Some(b1);

        let mut new = cm();
        new.immutable = Some(true);
        let mut b2 = HashMap::new();
        b2.insert("k".to_string(), vec![2u8]);
        new.binary_data = Some(b2);

        let errs = validate_config_map_update(&old, &new);
        assert!(errs.iter().any(|e| e.field == "binaryData"));
    }

    #[test]
    fn config_map_update_immutable_unchanged_passes() {
        let mut old = cm();
        old.immutable = Some(true);
        let mut d = HashMap::new();
        d.insert("a".to_string(), "1".to_string());
        old.data = Some(d.clone());
        let mut new = cm();
        new.immutable = Some(true);
        new.data = Some(d);
        // Metadata (labels) may still change without tripping immutability.
        let mut labels = HashMap::new();
        labels.insert("x".to_string(), "y".to_string());
        new.metadata.labels = Some(labels);
        assert!(validate_config_map_update(&old, &new).is_empty());
    }

    // Upstream `ValidateConfigMapUpdate` starts with `ValidateObjectMetaUpdate`.
    #[test]
    fn config_map_update_validates_object_meta() {
        let old = cm();
        let mut new = cm();
        new.metadata.name = "renamed".to_string();
        new.metadata.resource_version = None;
        let errs = validate_config_map_update(&old, &new);
        assert!(errs.iter().any(|e| e.field == "metadata.name"), "{errs:?}");
        assert!(
            errs.iter().any(|e| e.field == "metadata.resourceVersion"),
            "{errs:?}"
        );
    }
}
