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
use rusternetes_common::validation::field::{ErrorList, Path};
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
pub const BETA_STORAGE_CLASS_ANNOTATION: &str = "volume.beta.kubernetes.io/storage-class";

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
    if !rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::CrossNamespaceVolumeDataSource,
    ) && cross_namespace
        && !ref_in_use
    {
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
    warnings.extend(get_warnings_for_persistent_volume_claim_spec(
        &Path::new("spec"),
        &pvc.spec,
    ));
    warnings
}

/// `GetWarningsForPersistentVolumeClaimSpec(fieldPath, pvSpec)`
/// (util.go:213-236), shared with the StatefulSet strategy's
/// per-`volumeClaimTemplates` warnings.
pub fn get_warnings_for_persistent_volume_claim_spec(
    field_path: &Path,
    spec: &PersistentVolumeClaimSpec,
) -> Vec<String> {
    let mut warnings = Vec::new();
    let resources = &spec.resources;
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
                "{}: fractional byte value \"{q}\" is invalid, must be an integer",
                field_path.child("resources").child(list).key("storage")
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
    /// `GetResetFields` (pkg/registry/core/persistentvolumeclaim/strategy.go:54): status.
    fn get_reset_fields(&self) -> crate::ssa::ResetFields {
        crate::ssa::ResetFields::new().with("v1", &[&["status"]])
    }

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
    /// `GetResetFields` (pkg/registry/core/persistentvolumeclaim/strategy.go:145): spec.
    fn get_reset_fields(&self) -> crate::ssa::ResetFields {
        crate::ssa::ResetFields::new().with("v1", &[&["spec"]])
    }

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
