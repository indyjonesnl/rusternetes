//! PersistentVolumeClaim strategies and storage — port of
//! `pkg/registry/core/persistentvolumeclaim/strategy.go`,
//! `pkg/registry/core/persistentvolumeclaim/storage/storage.go` and the
//! `pkg/api/persistentvolumeclaim/util.go` helpers they call.

use std::sync::Arc;

use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::volume::{
    PersistentVolumeClaimSpec, TypedLocalObjectReference, TypedObjectReference,
};
use rusternetes_common::resources::{PersistentVolumeClaim, PersistentVolumeClaimStatus};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::pvc::{
    validate_persistent_volume_claim, validate_persistent_volume_claim_status_update,
    validate_persistent_volume_claim_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `core.BetaStorageClassAnnotation`.
const BETA_STORAGE_CLASS_ANNOTATION: &str = "volume.beta.kubernetes.io/storage-class";

/// `SetDefaults_PersistentVolumeClaim` and `SetDefaults_PersistentVolumeClaimSpec`
/// (pkg/apis/core/v1/defaults.go:294-304): the phase is `Pending` and the
/// volume mode `Filesystem`. Status is a struct upstream, so it is always
/// there to default.
pub fn convert_to_internal(pvc: &mut PersistentVolumeClaim) {
    crate::handlers::defaults::apply_pvc_spec_defaults(&mut pvc.spec);
    // `PersistentVolumeClaimPhase` has no empty value: a missing phase
    // decodes as `Pending` already.
    pvc.status.get_or_insert_with(Default::default);
}

/// `DropDisabledFields` (util.go:37-58). VolumeAttributesClass and
/// AnyVolumeDataSource are GA; only CrossNamespaceVolumeDataSource, alpha and
/// off (pkg/features/kube_features.go:1223-1225), drops anything: a
/// cross-namespace `dataSourceRef`, unless the old claim already used one.
fn drop_disabled_fields(
    spec: &mut PersistentVolumeClaimSpec,
    old: Option<&PersistentVolumeClaimSpec>,
) {
    let cross_namespace = spec
        .data_source_ref
        .as_ref()
        .and_then(|r| r.namespace.as_deref())
        .is_some_and(|ns| !ns.is_empty());
    let ref_in_use = old.is_some_and(|o| o.data_source_ref.is_some());
    if cross_namespace && !ref_in_use {
        spec.data_source_ref = None;
    }
}

/// `dataSourceIsPvcOrSnapshot` (util.go:131-147).
fn data_source_is_pvc_or_snapshot(data_source: Option<&TypedLocalObjectReference>) -> bool {
    let Some(ds) = data_source else {
        return false;
    };
    let api_group = ds.api_group.as_deref().unwrap_or("");
    (ds.kind == "PersistentVolumeClaim" && api_group.is_empty())
        || (ds.kind == "VolumeSnapshot" && api_group == "snapshot.storage.k8s.io")
}

/// `EnforceDataSourceBackwardsCompatibility` (util.go:76-99): without a
/// `dataSourceRef`, a `dataSource` that is neither a PVC nor a snapshot is
/// dropped, unless the old claim already had a data source.
fn enforce_data_source_backwards_compatibility(
    spec: &mut PersistentVolumeClaimSpec,
    old: Option<&PersistentVolumeClaimSpec>,
) {
    if old.is_some_and(|o| o.data_source.is_some() || o.data_source_ref.is_some()) {
        return;
    }
    if spec.data_source_ref.is_some() {
        return;
    }
    if !data_source_is_pvc_or_snapshot(spec.data_source.as_ref()) {
        spec.data_source = None;
    }
}

/// `NormalizeDataSources` (util.go:162-190): `dataSource` and
/// `dataSourceRef` mirror each other when only one is set; a cross-namespace
/// ref has no local counterpart.
fn normalize_data_sources(spec: &mut PersistentVolumeClaimSpec) {
    match (&spec.data_source, &spec.data_source_ref) {
        (Some(ds), None) => {
            spec.data_source_ref = Some(TypedObjectReference {
                api_group: ds.api_group.clone(),
                kind: ds.kind.clone(),
                name: ds.name.clone(),
                namespace: None,
            });
        }
        (None, Some(r)) if r.namespace.as_deref().unwrap_or("").is_empty() => {
            spec.data_source = Some(TypedLocalObjectReference {
                api_group: r.api_group.clone(),
                kind: r.kind.clone(),
                name: r.name.clone(),
            });
        }
        _ => {}
    }
}

/// `GetWarningsForPersistentVolumeClaim` and
/// `GetWarningsForPersistentVolumeClaimSpec` (util.go:192-236).
fn warnings_for_persistent_volume_claim(pvc: &PersistentVolumeClaim) -> Vec<String> {
    let mut warnings = Vec::new();
    let annotated = pvc
        .metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.contains_key(BETA_STORAGE_CLASS_ANNOTATION));
    if annotated {
        warnings.push(format!(
            r#"metadata.annotations[{BETA_STORAGE_CLASS_ANNOTATION}]: deprecated since v1.8; use "storageClassName" attribute instead"#
        ));
    }
    let resources = &pvc.spec.resources;
    for (list, map) in [
        ("requests", &resources.requests),
        ("limits", &resources.limits),
    ] {
        let Some(q) = map
            .as_ref()
            .and_then(|m| m.get("storage"))
            .and_then(|v| Quantity::parse(v).ok())
        else {
            continue;
        };
        if q.milli_value() % 1000 != 0 {
            warnings.push(format!(
                "spec.resources.{list}[storage]: fractional byte value \"{q}\" is invalid, must be an integer"
            ));
        }
    }
    warnings
}

/// `persistentvolumeclaimStrategy` (strategy.go:39-46).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<PersistentVolumeClaim> for Strategy {
    /// `PrepareForCreate` (strategy.go:65-79): status is cleared (the phase
    /// defaults back to `Pending`), and the data sources are made consistent.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut PersistentVolumeClaim) {
        obj.status = Some(PersistentVolumeClaimStatus::default());
        drop_disabled_fields(&mut obj.spec, None);
        enforce_data_source_backwards_compatibility(&mut obj.spec, None);
        normalize_data_sources(&mut obj.spec);
    }

    /// strategy.go:81-86.
    fn validate(&self, _ctx: &RequestContext, obj: &PersistentVolumeClaim) -> ErrorList {
        validate_persistent_volume_claim(obj)
    }

    /// strategy.go:88-91.
    fn warnings_on_create(
        &self,
        _ctx: &RequestContext,
        obj: &PersistentVolumeClaim,
    ) -> Vec<String> {
        warnings_for_persistent_volume_claim(obj)
    }
}

impl RestUpdateStrategy<PersistentVolumeClaim> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:101-119): status only changes through
    /// `/status`; the data sources are made consistent on both objects.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut PersistentVolumeClaim,
        old: &PersistentVolumeClaim,
    ) {
        obj.status = old.status.clone();
        drop_disabled_fields(&mut obj.spec, Some(&old.spec));
        enforce_data_source_backwards_compatibility(&mut obj.spec, Some(&old.spec));
        normalize_data_sources(&mut obj.spec);
    }

    /// strategy.go:121-127. Upstream also normalizes the old object's data
    /// sources before this compare (:117); the stored claim was normalized
    /// when it was written.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &PersistentVolumeClaim,
        old: &PersistentVolumeClaim,
    ) -> ErrorList {
        validate_persistent_volume_claim_update(obj, old)
    }

    /// strategy.go:129-131.
    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &PersistentVolumeClaim,
        _old: &PersistentVolumeClaim,
    ) -> Vec<String> {
        warnings_for_persistent_volume_claim(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// PersistentVolumeClaims use the default delete strategy.
impl RestDeleteStrategy<PersistentVolumeClaim> for Strategy {}

/// `persistentvolumeclaimStatusStrategy` (strategy.go:137-174): the update
/// strategy of `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<PersistentVolumeClaim> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:156-161: only status may change.
    /// `DropDisabledFieldsFromStatus` drops nothing: VolumeAttributesClass and
    /// RecoverVolumeExpansionFailure are GA.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut PersistentVolumeClaim,
        old: &PersistentVolumeClaim,
    ) {
        obj.spec = old.spec.clone();
    }

    /// strategy.go:163-169.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &PersistentVolumeClaim,
        old: &PersistentVolumeClaim,
    ) -> ErrorList {
        validate_persistent_volume_claim_status_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `NewREST` (storage/storage.go:41-68): the PersistentVolumeClaim store,
/// which returns the deleted object.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<PersistentVolumeClaim, StorageBackend> {
    let mut store = Store::new(
        storage,
        GroupResource::new("", "persistentvolumeclaims"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    store.return_deleted_object = true;
    store
}

/// The `/status` store: the PersistentVolumeClaim store updating with
/// [`StatusStrategy`] (storage.go:63-65).
pub fn new_status_store(
    storage: Arc<StorageBackend>,
) -> Store<PersistentVolumeClaim, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}

#[cfg(test)]
mod tests {
    use super::*;
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
