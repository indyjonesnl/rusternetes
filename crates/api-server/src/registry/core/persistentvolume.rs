//! PersistentVolume strategies and storage — port of
//! `pkg/registry/core/persistentvolume/strategy.go` and
//! `pkg/registry/core/persistentvolume/storage/storage.go`.

use std::sync::Arc;

use chrono::Utc;
use rusternetes_common::pod_warnings::warnings_for_volume_node_selector_term;
use rusternetes_common::resources::volume::{PersistentVolumeMode, PersistentVolumeReclaimPolicy};
use rusternetes_common::resources::{
    PersistentVolume, PersistentVolumePhase, PersistentVolumeStatus,
};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::persistentvolume::{
    validate_persistent_volume, validate_persistent_volume_plugin,
    validate_persistent_volume_status_update, validate_persistent_volume_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `deprecatedAnnotations` (pkg/api/persistentvolume/util.go:47-59).
const DEPRECATED_ANNOTATIONS: &[(&str, &str)] = &[
    (
        "volume.beta.kubernetes.io/storage-class",
        r#"deprecated since v1.8; use "storageClassName" attribute instead"#,
    ),
    (
        "volume.beta.kubernetes.io/mount-options",
        r#"deprecated since v1.31; use "mountOptions" attribute instead"#,
    ),
];

/// `SetDefaults_PersistentVolume` (pkg/apis/core/v1/defaults.go:282-293).
pub fn convert_to_internal(pv: &mut PersistentVolume) {
    if pv.spec.persistent_volume_reclaim_policy.is_none() {
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Retain);
    }
    if pv.spec.volume_mode.is_none() {
        pv.spec.volume_mode = Some(PersistentVolumeMode::Filesystem);
    }
    // SetObjectDefaults_PersistentVolume (zz_generated.defaults.go:151-155).
    if let Some(iscsi) = pv.spec.iscsi.as_mut() {
        iscsi.set_defaults();
    }

    // SetObjectDefaults_PersistentVolume (zz_generated.defaults.go:137-181)
    pv.spec.more_sources.apply_defaults();
}

/// `GetWarningsForPersistentVolume` (pkg/api/persistentvolume/util.go:40-110).
/// Of the deprecated-plugin warnings it carries, none applies to a volume
/// source modelled here.
fn warnings_for_persistent_volume(pv: &PersistentVolume) -> Vec<String> {
    let mut warnings = Vec::new();
    let annotations = pv.metadata.annotations.as_ref();
    for (key, message) in DEPRECATED_ANNOTATIONS {
        if annotations.is_some_and(|a| a.contains_key(*key)) {
            warnings.push(format!("metadata.annotations[{key}]: {message}"));
        }
    }
    if pv.spec.persistent_volume_reclaim_policy == Some(PersistentVolumeReclaimPolicy::Recycle) {
        warnings.push("spec.persistentVolumeReclaimPolicy: The Recycle reclaim policy is deprecated. Instead, the recommended approach is to use dynamic provisioning.".to_string());
    }
    // pkg/api/persistentvolume/util.go:84-90
    if let Some(required) = pv
        .spec
        .node_affinity
        .as_ref()
        .and_then(|a| a.required.as_ref())
    {
        let term_path = Path::new("spec")
            .child("nodeAffinity")
            .child("required")
            .child("nodeSelectorTerms");
        for (i, term) in required.node_selector_terms.iter().enumerate() {
            warnings.extend(warnings_for_volume_node_selector_term(
                term,
                false,
                &term_path.index(i),
            ));
        }
    }
    warnings
}

/// `persistentvolumeStrategy` (strategy.go:39-47).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<PersistentVolume> for Strategy {
    /// `PrepareForCreate` (strategy.go:66-74): status is reset to `Pending`
    /// with the transition time set. `DropDisabledSpecFields` drops nothing:
    /// VolumeAttributesClass is enabled.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut PersistentVolume) {
        obj.status = Some(PersistentVolumeStatus {
            phase: PersistentVolumePhase::Pending,
            last_phase_transition_time: Some(Utc::now()),
            ..Default::default()
        });
    }

    /// strategy.go:76-81.
    fn validate(&self, _ctx: &RequestContext, obj: &PersistentVolume) -> ErrorList {
        let mut errs = validate_persistent_volume(obj);
        errs.extend(validate_persistent_volume_plugin(obj));
        errs
    }

    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &PersistentVolume) -> Vec<String> {
        warnings_for_persistent_volume(obj)
    }
}

impl RestUpdateStrategy<PersistentVolume> for Strategy {
    /// `GetResetFields` (pkg/registry/core/persistentvolume/strategy.go:55): status.
    fn get_reset_fields(&self) -> crate::ssa::ResetFields {
        crate::ssa::ResetFields::new().with("v1", &[&["status"]])
    }

    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:97-102): status only changes through
    /// `/status`.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut PersistentVolume,
        old: &PersistentVolume,
    ) {
        obj.status = old.status.clone();
    }

    /// strategy.go:104-111. `validate_persistent_volume_update` runs
    /// `ValidatePersistentVolume` itself.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &PersistentVolume,
        old: &PersistentVolume,
    ) -> ErrorList {
        let mut errs = validate_persistent_volume_update(obj, old);
        errs.extend(validate_persistent_volume_plugin(obj));
        errs
    }

    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &PersistentVolume,
        _old: &PersistentVolume,
    ) -> Vec<String> {
        warnings_for_persistent_volume(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// PersistentVolumes use the default delete strategy.
impl RestDeleteStrategy<PersistentVolume> for Strategy {}

/// `persistentvolumeStatusStrategy` (strategy.go:122-169): the update
/// strategy of `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<PersistentVolume> for StatusStrategy {
    /// `GetResetFields` (pkg/registry/core/persistentvolume/strategy.go:130): spec.
    fn get_reset_fields(&self) -> crate::ssa::ResetFields {
        crate::ssa::ResetFields::new().with("v1", &[&["spec"]])
    }

    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:143-159: only status may change, and a phase change stamps
    /// `lastPhaseTransitionTime` unless the client set a new one. An unchanged
    /// phase keeps the stored time when the client sent none.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut PersistentVolume,
        old: &PersistentVolume,
    ) {
        obj.spec = old.spec.clone();
        let old_status = old.status.clone().unwrap_or_default();
        let new_status = obj.status.get_or_insert_with(Default::default);
        let new_time = new_status.last_phase_transition_time;
        if old_status.phase == new_status.phase && new_time.is_none() {
            new_status.last_phase_transition_time = old_status.last_phase_transition_time;
        } else if old_status.phase != new_status.phase
            && (new_time.is_none() || new_time == old_status.last_phase_transition_time)
        {
            new_status.last_phase_transition_time = Some(Utc::now());
        }
    }

    /// strategy.go:161-163.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &PersistentVolume,
        old: &PersistentVolume,
    ) -> ErrorList {
        validate_persistent_volume_status_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `NewREST` (storage/storage.go:40-67): the PersistentVolume store, which
/// returns the deleted object.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<PersistentVolume, StorageBackend> {
    let mut store = Store::new(
        storage,
        GroupResource::new("", "persistentvolumes"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    store.return_deleted_object = true;
    store
}

/// The `/status` store: the PersistentVolume store updating with
/// [`StatusStrategy`] (storage.go:62-64).
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<PersistentVolume, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}
