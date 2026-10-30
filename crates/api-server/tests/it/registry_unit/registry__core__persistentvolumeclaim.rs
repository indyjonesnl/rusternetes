//! Unit tests moved from `src/registry/core/persistentvolumeclaim.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::volume::{
        PersistentVolumeClaimSpec, TypedLocalObjectReference, TypedObjectReference,
    };
    pub use rusternetes_common::resources::{PersistentVolumeClaim, PersistentVolumeClaimStatus};
    pub use rusternetes_common::validation::field::{ErrorList, Path};
    pub use rusternetes_common::validation::pvc::{
        validate_persistent_volume_claim, validate_persistent_volume_claim_status_update,
        validate_persistent_volume_claim_update,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::persistentvolumeclaim::*;
    use rusternetes_common::resources::volume::PersistentVolumeClaimPhase;
    use rusternetes_common::resources::volume::PersistentVolumeMode;

    fn pvc(extra: serde_json::Value) -> PersistentVolumeClaim {
        let mut body = serde_json::json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": "c", "namespace": "default", "resourceVersion": "1"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "1Gi"}}
            },
            "status": {"phase": "Bound"}
        });
        for (k, v) in extra.as_object().unwrap() {
            body["spec"][k] = v.clone();
        }
        let mut pvc: PersistentVolumeClaim = serde_json::from_value(body).unwrap();
        convert_to_internal(&mut pvc);
        pvc
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert!(Strategy.default_garbage_collection_policy(&ctx()).is_none());
        assert!(!StatusStrategy.allow_create_on_update());
    }

    #[test]
    fn defaults_volume_mode_and_create_clears_status() {
        let mut c = pvc(serde_json::json!({}));
        assert_eq!(c.spec.volume_mode, Some(PersistentVolumeMode::Filesystem));
        Strategy.prepare_for_create(&ctx(), &mut c);
        let status = c.status.as_ref().unwrap();
        assert_eq!(status.phase, PersistentVolumeClaimPhase::Pending);
        let errs = Strategy.validate(&ctx(), &c);
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// `TestDataSourceFilter` / `TestDataSourceRef`
    /// (pkg/api/persistentvolumeclaim/util_test.go:188, :294):
    /// an arbitrary `dataSource` alone is dropped, a PVC source is mirrored
    /// into `dataSourceRef`, and a cross-namespace ref is dropped.
    #[test]
    fn data_sources_are_made_consistent() {
        let mut c = pvc(serde_json::json!({
            "dataSource": {"kind": "Widget", "apiGroup": "example.com", "name": "w"}
        }));
        Strategy.prepare_for_create(&ctx(), &mut c);
        assert!(c.spec.data_source.is_none());
        assert!(c.spec.data_source_ref.is_none());

        let mut c = pvc(serde_json::json!({
            "dataSource": {"kind": "PersistentVolumeClaim", "name": "src"}
        }));
        Strategy.prepare_for_create(&ctx(), &mut c);
        let r = c.spec.data_source_ref.as_ref().unwrap();
        assert_eq!(
            (r.kind.as_str(), r.name.as_str()),
            ("PersistentVolumeClaim", "src")
        );

        let mut c = pvc(serde_json::json!({
            "dataSourceRef": {"kind": "Widget", "apiGroup": "example.com", "name": "w"}
        }));
        Strategy.prepare_for_create(&ctx(), &mut c);
        assert_eq!(c.spec.data_source.as_ref().unwrap().kind, "Widget");

        let mut c = pvc(serde_json::json!({
            "dataSourceRef": {"kind": "PersistentVolumeClaim", "name": "src", "namespace": "other"}
        }));
        Strategy.prepare_for_create(&ctx(), &mut c);
        assert!(c.spec.data_source_ref.is_none());
        assert!(c.spec.data_source.is_none());
    }

    #[test]
    fn update_keeps_status_and_status_update_keeps_spec() {
        let old = pvc(serde_json::json!({}));
        let mut new = pvc(serde_json::json!({"resources": {"requests": {"storage": "2Gi"}}}));
        new.status = None;
        Strategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(
            new.status.as_ref().map(|s| &s.phase),
            Some(&PersistentVolumeClaimPhase::Bound)
        );

        let mut status = pvc(serde_json::json!({"resources": {"requests": {"storage": "9Gi"}}}));
        StatusStrategy.prepare_for_update(&ctx(), &mut status, &old);
        assert_eq!(status.spec.resources.requests, old.spec.resources.requests);
    }

    /// `TestWarnings` (pkg/api/persistentvolumeclaim/util_test.go:662-735).
    #[test]
    fn warnings_match_upstream() {
        let cases = [
            (
                serde_json::json!({"requests": {"storage": "200Mi"}, "limits": {"storage": "200Mi"}}),
                vec![],
            ),
            (
                serde_json::json!({"requests": {"storage": "200m"}, "limits": {"storage": "100m"}}),
                vec![
                    r#"spec.resources.requests[storage]: fractional byte value "200m" is invalid, must be an integer"#,
                    r#"spec.resources.limits[storage]: fractional byte value "100m" is invalid, must be an integer"#,
                ],
            ),
            (serde_json::json!({"requests": {"storage": "200"}}), vec![]),
        ];
        for (resources, want) in cases {
            let c = pvc(serde_json::json!({"resources": resources}));
            assert_eq!(Strategy.warnings_on_create(&ctx(), &c), want, "{resources}");
        }

        let mut c = pvc(serde_json::json!({}));
        c.metadata.annotations =
            Some([(BETA_STORAGE_CLASS_ANNOTATION.to_string(), String::new())].into());
        assert_eq!(
            Strategy.warnings_on_create(&ctx(), &c),
            vec![
                r#"metadata.annotations[volume.beta.kubernetes.io/storage-class]: deprecated since v1.8; use "storageClassName" attribute instead"#
            ]
        );
    }
}
