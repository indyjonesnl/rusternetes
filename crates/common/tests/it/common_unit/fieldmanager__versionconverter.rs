//! Unit tests moved from `src/fieldmanager/versionconverter.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::fieldmanager::apply::remove_path;
    pub use rusternetes_common::fieldmanager::apply::ApplyConflict;
    pub use rusternetes_common::fieldmanager::fieldset::Set;
    pub use rusternetes_common::fieldmanager::{FieldManager, Managed, ManagerIdentity};
    pub use serde_json::{Map, Value};
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::fieldmanager::fieldset::field_path;
    use rusternetes_common::fieldmanager::versionconverter::*;
    use rusternetes_common::fieldmanager::{build_manager_identifier, ResetFields, VersionedSet};
    use serde_json::json;

    /// Two versions of one kind that spell the replica count differently:
    /// `example.io/v1` `.spec.replicas`, `example.io/v2` `.spec.size`. The
    /// hub (`example.io/__internal`) spells it as v2 does. Mirrors the fake
    /// object convertor of `versionconverter_test.go`.
    struct Renamer;

    impl ObjectConvertor for Renamer {
        fn convert_to_version(&self, obj: &Value, gv: &str) -> Result<Value, ConvertError> {
            if gv == "example.io/v0" {
                return Err(ConvertError::MissingVersion("example.io/v0".into()));
            }
            let mut out = obj.clone();
            if let Some(spec) = out.get_mut("spec").and_then(Value::as_object_mut) {
                let count = spec.remove("replicas").or_else(|| spec.remove("size"));
                if let Some(c) = count {
                    let name = if gv == "example.io/v1" {
                        "replicas"
                    } else {
                        "size"
                    };
                    spec.insert(name.into(), c);
                }
            }
            out["apiVersion"] = json!(gv);
            Ok(out)
        }
    }

    fn converter() -> Arc<dyn Converter> {
        Arc::new(VersionConverter::new(
            Arc::new(Renamer),
            API_VERSION_INTERNAL,
        ))
    }

    fn fm(version: &str) -> FieldManager {
        FieldManager::new(version, None, ResetFields::new()).with_converter(Some(converter()))
    }

    fn obj(version: &str, field: &str, value: i64) -> Value {
        json!({"apiVersion": version, "kind": "Foo", "spec": {field: value}})
    }

    fn entry(manager: &str, op: &str, version: &str, path: &[&str]) -> (String, VersionedSet) {
        let mut set = Set::new();
        set.insert(&field_path(path));
        (
            build_manager_identifier(manager, op, version, ""),
            VersionedSet {
                set,
                api_version: version.to_string(),
                applied: op == "Apply",
            },
        )
    }

    fn managed_of(entries: Vec<(String, VersionedSet)>) -> Managed {
        let mut managed = Managed::default();
        for (id, vs) in entries {
            managed.times.insert(id.clone(), None);
            managed.fields.insert(id, vs);
        }
        managed
    }

    fn id(manager: &str, op: &str, version: &str) -> String {
        build_manager_identifier(manager, op, version, "")
    }

    /// versionconverter_test.go `TestVersionConverter`.
    #[test]
    fn convert_goes_through_the_hub_and_skips_the_same_version() {
        let c = converter();
        let v1 = obj("example.io/v1", "replicas", 3);
        assert_eq!(c.convert(&v1, "example.io/v1").unwrap(), v1);
        assert_eq!(
            c.convert(&v1, "example.io/v2").unwrap(),
            obj("example.io/v2", "size", 3)
        );
    }

    /// `newCRDVersionConverter`: one hub whatever the version.
    #[test]
    fn crd_converter_uses_a_single_hub() {
        let c = VersionConverter::new_crd(Arc::new(NopObjectConvertor), "example.io/v1");
        let out = c
            .convert(&obj("example.io/v2", "size", 1), "example.io/v1")
            .unwrap();
        assert_eq!(out["apiVersion"], "example.io/v1");
        assert_eq!(out["spec"]["size"], 1);
    }

    #[test]
    fn parse_group_version_rejects_two_slashes() {
        assert!(parse_group_version("a/b/c").is_err());
        assert_eq!(
            parse_group_version("v1").unwrap(),
            (String::new(), "v1".into())
        );
    }

    /// update.go:98-139: an update at v2 changes `.spec.size`, which is the
    /// v1 manager's `.spec.replicas` once the objects are converted to v1.
    #[test]
    fn an_update_takes_the_fields_of_a_manager_at_another_version() {
        let managed = managed_of(vec![entry(
            "a",
            "Update",
            "example.io/v1",
            &["spec", "replicas"],
        )]);
        let out = fm("example.io/v2")
            .update_managed(
                &obj("example.io/v2", "size", 1),
                &obj("example.io/v2", "size", 2),
                managed,
                "b",
                false,
            )
            .unwrap();
        assert!(
            !out.fields.contains_key(&id("a", "Update", "example.io/v1")),
            "a's only field was taken, so its entry goes"
        );
        let b = &out.fields[&id("b", "Update", "example.io/v2")];
        assert!(b.set.has(&field_path(&["spec", "size"])));
    }

    /// Without a converter the sets are compared as-is, so the same update
    /// leaves the v1 entry alone: the behaviour #2736 replaces.
    #[test]
    fn without_a_converter_other_versions_are_compared_as_is() {
        let managed = managed_of(vec![entry(
            "a",
            "Update",
            "example.io/v1",
            &["spec", "replicas"],
        )]);
        let out = FieldManager::new("example.io/v2", None, ResetFields::new())
            .update_managed(
                &obj("example.io/v2", "size", 1),
                &obj("example.io/v2", "size", 2),
                managed,
                "b",
                false,
            )
            .unwrap();
        assert!(out.fields.contains_key(&id("a", "Update", "example.io/v1")));
    }

    /// update.go:107-111: a version with no corresponding type drops the
    /// manager.
    #[test]
    fn a_manager_at_a_missing_version_is_dropped() {
        let managed = managed_of(vec![
            entry("gone", "Update", "example.io/v0", &["spec", "replicas"]),
            entry("kept", "Update", "example.io/v1", &["spec", "other"]),
        ]);
        let out = fm("example.io/v2")
            .update_managed(
                &obj("example.io/v2", "size", 1),
                &obj("example.io/v2", "size", 2),
                managed,
                "b",
                false,
            )
            .unwrap();
        assert!(!out
            .fields
            .contains_key(&id("gone", "Update", "example.io/v0")));
        assert!(out
            .fields
            .contains_key(&id("kept", "Update", "example.io/v1")));
    }

    /// A conversion that is not a missing-version error fails the update
    /// (`failed to convert old object`).
    #[test]
    fn another_conversion_error_fails_the_update() {
        struct Broken;
        impl ObjectConvertor for Broken {
            fn convert_to_version(&self, _: &Value, _: &str) -> Result<Value, ConvertError> {
                Err(ConvertError::Other("boom".into()))
            }
        }
        let fm = FieldManager::new("example.io/v2", None, ResetFields::new()).with_converter(Some(
            Arc::new(VersionConverter::new(
                Arc::new(Broken),
                API_VERSION_INTERNAL,
            )),
        ));
        let managed = managed_of(vec![entry(
            "a",
            "Update",
            "example.io/v1",
            &["spec", "replicas"],
        )]);
        let err = fm
            .update_managed(
                &obj("example.io/v2", "size", 1),
                &obj("example.io/v2", "size", 2),
                managed,
                "b",
                false,
            )
            .unwrap_err();
        assert_eq!(err, "failed to convert old object: boom");
    }

    /// update.go:123-139 on the Apply path: b applies v2 `.spec.size`, a owns
    /// the v1 spelling of it.
    #[test]
    fn apply_conflicts_with_a_manager_at_another_version() {
        let live = fm("example.io/v1")
            .apply_value(None, &obj("example.io/v1", "replicas", 1), "a", false)
            .unwrap();
        let err = fm("example.io/v2")
            .apply_value(Some(&live), &obj("example.io/v2", "size", 5), "b", false)
            .unwrap_err();
        match err {
            UpdateFailure::Conflicts(c) => {
                assert_eq!(c.len(), 1);
                assert_eq!(c[0].manager, "a");
                assert_eq!(c[0].path, ".spec.replicas");
            }
            other => panic!("{other:?}"),
        }
        // Forced, a loses the field.
        let forced = fm("example.io/v2")
            .apply_value(Some(&live), &obj("example.io/v2", "size", 5), "b", true)
            .unwrap();
        let entries = forced["metadata"]["managedFields"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["manager"], "b");
    }

    /// update.go:256-282: the applier owned `.spec.replicas` at v1 and no
    /// longer applies it at v2, so the pruned field is removed.
    #[test]
    fn prune_works_at_the_version_of_the_last_apply() {
        let live = fm("example.io/v1")
            .apply_value(None, &obj("example.io/v1", "replicas", 1), "a", false)
            .unwrap();
        let config = json!({"apiVersion": "example.io/v2", "kind": "Foo",
            "metadata": {"labels": {"x": "y"}}});
        let out = fm("example.io/v2")
            .apply_value(Some(&live), &config, "a", false)
            .unwrap();
        assert!(out["spec"].get("size").is_none(), "{out}");
        assert!(out["spec"].get("replicas").is_none(), "{out}");
        assert_eq!(out["metadata"]["labels"]["x"], "y");
    }

    /// `addBackOwnedItems`: what another manager owns survives the prune,
    /// even when it owns it at another version.
    #[test]
    fn prune_keeps_what_another_version_owns() {
        let live = fm("example.io/v1")
            .apply_value(None, &obj("example.io/v1", "replicas", 1), "a", false)
            .unwrap();
        let entries: Vec<rusternetes_common::types::ManagedFieldsEntry> =
            serde_json::from_value(live["metadata"]["managedFields"].clone()).unwrap();
        let mut managed = Managed::decode(&entries).unwrap();
        let (bid, vs) = entry("b", "Update", "example.io/v2", &["spec", "size"]);
        managed.times.insert(bid.clone(), None);
        managed.fields.insert(bid, vs);
        let mut co = live.clone();
        co["metadata"]["managedFields"] = serde_json::to_value(managed.encode().unwrap()).unwrap();

        let config = json!({"apiVersion": "example.io/v2", "kind": "Foo",
            "metadata": {"labels": {"x": "y"}}});
        let out = fm("example.io/v2")
            .apply_value(Some(&co), &config, "a", false)
            .unwrap();
        assert_eq!(out["spec"]["size"], 1, "{out}");
    }
}
