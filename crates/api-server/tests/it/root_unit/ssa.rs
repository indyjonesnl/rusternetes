//! Unit tests moved from `src/ssa/mod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::Utc;
    pub use rusternetes_api_server::ssa::merge::{
        merge_string_map_group, LeafGroup, OwnedPaths, PathConflict, ResourceLeafSchema,
        CONFIGMAP_SCHEMA, SECRET_SCHEMA,
    };
    pub use rusternetes_common::fieldmanager::ResetFields;
    pub use rusternetes_common::resources::{ConfigMap, Secret};
    pub use rusternetes_common::types::ManagedFieldsEntry;
    pub use serde::de::DeserializeOwned;
    pub use serde::Serialize;
    pub use serde_json::{json, Map, Value};
    pub use std::collections::BTreeMap;
    pub use thiserror::Error;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::ssa::*;

    #[test]
    fn decode_yaml_body() {
        let body = b"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: foo\n";
        let v = decode_apply_body("application/apply-patch+yaml", body).unwrap();
        assert_eq!(v["metadata"]["name"], "foo");
    }

    #[test]
    fn decode_json_body() {
        let body = br#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"foo"}}"#;
        let v = decode_apply_body("application/apply-patch+json", body).unwrap();
        assert_eq!(v["metadata"]["name"], "foo");
    }

    #[test]
    fn empty_body_rejected() {
        let result = apply_configmap(None, &json!({}), &ApplyOptions::new("kubectl"));
        assert!(matches!(result, Err(ApplyError::InvalidBody { .. })));
    }

    #[test]
    fn empty_secret_body_rejected() {
        let result = apply_secret(None, &json!({}), &ApplyOptions::new("kubectl"));
        assert!(matches!(result, Err(ApplyError::InvalidBody { .. })));
    }

    /// #2736: `apply_legacy` writes real `FieldsV1` entries (the same
    /// `Set`/`Managed` types the Update path decodes), tracks ownership per
    /// field path rather than per top-level key (`merge.Updater.Apply`,
    /// structured-merge-diff `merge/update.go`), and a later Update keeps the
    /// applier's entry.
    mod legacy_apply_managed_fields {
        use super::*;
        use rusternetes_api_server::fieldmanager::FieldManager;
        use rusternetes_common::resources::Lease;

        fn lease(spec: Value) -> Value {
            json!({"apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
                   "metadata": {"name": "l", "namespace": "ns", "uid": "u"}, "spec": spec})
        }

        fn applied(out: ApplyOutcome<Lease>) -> Lease {
            match out {
                ApplyOutcome::Applied { object } => *object,
                ApplyOutcome::Conflicts(c) => panic!("unexpected conflicts: {c:?}"),
            }
        }

        #[test]
        fn entries_are_real_fields_v1_with_per_path_ownership() {
            let a = applied(
                apply_legacy::<Lease>(
                    None,
                    &lease(json!({"holderIdentity": "a"})),
                    &ApplyOptions::new("a"),
                )
                .unwrap(),
            );
            let v = serde_json::to_value(&a).unwrap();
            let entry = &v["metadata"]["managedFields"][0];
            assert_eq!(entry["operation"], "Apply");
            assert_eq!(entry["fieldsType"], "FieldsV1");
            assert_eq!(entry["fieldsV1"]["f:spec"]["f:holderIdentity"], json!({}));

            // A different field of the same top-level struct: no conflict,
            // and the earlier field survives the merge.
            let b = applied(
                apply_legacy::<Lease>(
                    Some(&a),
                    &lease(json!({"leaseDurationSeconds": 5})),
                    &ApplyOptions::new("b"),
                )
                .unwrap(),
            );
            let v = serde_json::to_value(&b).unwrap();
            assert_eq!(v["spec"]["holderIdentity"], "a");
            assert_eq!(v["spec"]["leaseDurationSeconds"], 5);

            // The same field: a conflict naming the path and its owner.
            let out = apply_legacy::<Lease>(
                Some(&b),
                &lease(json!({"holderIdentity": "b"})),
                &ApplyOptions::new("b"),
            )
            .unwrap();
            match out {
                ApplyOutcome::Conflicts(c) => {
                    assert_eq!(c.len(), 1, "{c:?}");
                    assert_eq!(c[0].path, "spec.holderIdentity");
                    assert_eq!(c[0].current_manager, "a");
                }
                _ => panic!("expected a conflict"),
            }

            // A later Update decodes the entries instead of dropping them.
            let mut changed = b.clone();
            changed.spec.as_mut().unwrap().lease_duration_seconds = Some(9);
            let fm = FieldManager::new("coordination.k8s.io/v1", None, ResetFields::new());
            let updated = fm.update(Some(&b), changed, "u").unwrap();
            let v = serde_json::to_value(&updated).unwrap();
            let managers: Vec<&str> = v["metadata"]["managedFields"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["manager"].as_str().unwrap())
                .collect();
            assert!(managers.contains(&"a"), "{managers:?}");
        }

        /// #3039 item 1: a `/status` or `/scale` apply records the
        /// subresource in the entry (buildmanagerinfo.go:55-61), so it is a
        /// different manager identity from the main-resource apply.
        #[test]
        fn subresource_apply_entry_carries_the_subresource() {
            let a = applied(
                apply_legacy::<Lease>(
                    None,
                    &lease(json!({"holderIdentity": "a"})),
                    &ApplyOptions::new("a").with_subresource(Some("status")),
                )
                .unwrap(),
            );
            let v = serde_json::to_value(&a).unwrap();
            let entry = &v["metadata"]["managedFields"][0];
            assert_eq!(entry["subresource"], "status", "{entry}");
        }
    }
}
