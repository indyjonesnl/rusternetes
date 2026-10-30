//! PodDisruptionBudget strategies and storage — port of
//! `pkg/registry/policy/poddisruptionbudget/strategy.go` and
//! `pkg/registry/policy/poddisruptionbudget/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::equality::semantic_equal;
use rusternetes_common::resources::{PodDisruptionBudget, PodDisruptionBudgetStatus};
use rusternetes_common::types::{LabelSelector, LabelSelectorRequirement};
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::metav1::{
    validate_label_selector, LabelSelectorValidationOptions,
};
use rusternetes_common::validation::pdb::{
    validate_pod_disruption_budget, validate_pod_disruption_budget_status_update,
    PodDisruptionBudgetValidationOptions,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};
use crate::ssa::ResetFields;

/// `policy.PDBV1beta1Label` (pkg/apis/policy/helper.go:24).
pub const PDB_V1BETA1_LABEL: &str = "pdb.kubernetes.io/deprecated-v1beta1-empty-selector-match";

/// `policy.NonV1beta1MatchNoneSelector` (pkg/apis/policy/helper.go:29-31).
fn non_v1beta1_match_none_selector() -> LabelSelector {
    LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![LabelSelectorRequirement {
            key: PDB_V1BETA1_LABEL.to_string(),
            operator: "Exists".to_string(),
            values: None,
        }]),
    }
}

/// The v1 → internal conversion of a decoded PodDisruptionBudget:
/// `Convert_v1_PodDisruptionBudget_To_policy_PodDisruptionBudget`
/// (pkg/apis/policy/v1/conversion.go:26-42). Unless the selector is exactly
/// the match-none form, the label v1beta1 uses to spell "match all" and
/// "match none" is stripped from it (`StripPDBV1beta1Label`, helper.go:39-51),
/// so it never combines with user-specified requirements. The match-all
/// form, `{}`, has no requirement to strip. policy/v1 has no defaulters
/// (pkg/apis/policy/v1/zz_generated.defaults.go).
pub fn convert_to_internal(pdb: &mut PodDisruptionBudget) {
    let Some(selector) = pdb.spec.selector.as_mut() else {
        return;
    };
    if semantic_equal(&*selector, &non_v1beta1_match_none_selector()) {
        return;
    }
    if let Some(exprs) = selector.match_expressions.as_mut() {
        exprs.retain(|e| e.key != PDB_V1BETA1_LABEL);
        // An empty `matchExpressions` is omitted on the wire (omitempty).
        if exprs.is_empty() {
            selector.match_expressions = None;
        }
    }
}

/// `hasInvalidLabelValueInLabelSelector` (strategy.go:190-196).
fn has_invalid_label_value_in_label_selector(pdb: &PodDisruptionBudget) -> bool {
    match &pdb.spec.selector {
        Some(selector) => !validate_label_selector(
            selector,
            LabelSelectorValidationOptions::default(),
            &Path::new(""),
        )
        .is_empty(),
        None => false,
    }
}

/// `podDisruptionBudgetStrategy` (strategy.go:37-43).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<PodDisruptionBudget> for Strategy {
    /// `PrepareForCreate` (strategy.go:67-73): status is cleared and the
    /// generation starts at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut PodDisruptionBudget) {
        obj.status = Some(PodDisruptionBudgetStatus {
            current_healthy: 0,
            desired_healthy: 0,
            disruptions_allowed: 0,
            expected_pods: 0,
            observed_generation: None,
            conditions: None,
            disrupted_pods: None,
        });
        obj.metadata.generation = Some(1);
    }

    /// `Validate` (strategy.go:90-96).
    fn validate(&self, _ctx: &RequestContext, obj: &PodDisruptionBudget) -> ErrorList {
        validate_pod_disruption_budget(obj, PodDisruptionBudgetValidationOptions::default())
    }
}

impl RestUpdateStrategy<PodDisruptionBudget> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:76-88): status is kept and a spec
    /// change bumps the generation.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut PodDisruptionBudget,
        old: &PodDisruptionBudget,
    ) {
        obj.status = old.status.clone();
        if !semantic_equal(&obj.spec, &old.spec) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    /// `ValidateUpdate` (strategy.go:112-118): a selector already stored with
    /// an invalid label value may keep it.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &PodDisruptionBudget,
        old: &PodDisruptionBudget,
    ) -> ErrorList {
        let opts = PodDisruptionBudgetValidationOptions {
            allow_invalid_label_value_in_selector: has_invalid_label_value_in_label_selector(old),
        };
        validate_pod_disruption_budget(obj, opts)
    }

    /// `AllowUnconditionalUpdate` (strategy.go:125-129): a PUT must carry a
    /// resourceVersion.
    fn allow_unconditional_update(&self) -> bool {
        false
    }

    /// `GetResetFields` (policy/poddisruptionbudget/strategy.go:51-62): `status`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new()
            .with("policy/v1beta1", &[&["status"]])
            .with("policy/v1", &[&["status"]])
    }
}

/// `podDisruptionBudgetStrategy` implements no
/// `GarbageCollectionDeleteStrategy`.
impl RestDeleteStrategy<PodDisruptionBudget> for Strategy {}

/// `podDisruptionBudgetStatusStrategy` (strategy.go:131-136): embeds the main
/// strategy, so create-on-update and unconditional updates stay off.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<PodDisruptionBudget> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:156-161: only status may change.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut PodDisruptionBudget,
        old: &PodDisruptionBudget,
    ) {
        obj.spec = old.spec.clone();
    }

    /// strategy.go:164-174. Only policy/v1 is served, so the v1beta1
    /// short-circuit of `ValidatePodDisruptionBudgetStatusUpdate` never
    /// applies.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &PodDisruptionBudget,
        old: &PodDisruptionBudget,
    ) -> ErrorList {
        validate_pod_disruption_budget_status_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        false
    }

    /// `GetResetFields` (policy/poddisruptionbudget/strategy.go:139-150): `spec`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new()
            .with("policy/v1beta1", &[&["spec"]])
            .with("policy/v1", &[&["spec"]])
    }
}

/// `NewREST` (storage/storage.go:40-65): the PodDisruptionBudget store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<PodDisruptionBudget, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("policy", "poddisruptionbudgets"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}

/// The `/status` store: the PodDisruptionBudget store updating with
/// [`StatusStrategy`] (storage.go:60-62).
pub fn new_status_store(
    storage: Arc<StorageBackend>,
) -> Store<PodDisruptionBudget, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}
