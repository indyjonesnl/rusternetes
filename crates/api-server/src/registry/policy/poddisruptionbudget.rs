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

/// `policy.PDBV1beta1Label` (pkg/apis/policy/helper.go:24).
const PDB_V1BETA1_LABEL: &str = "pdb.kubernetes.io/deprecated-v1beta1-empty-selector-match";

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

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::IntOrString;

    fn pdb(spec: serde_json::Value) -> PodDisruptionBudget {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
            "metadata": {"name": "abc", "namespace": "default", "resourceVersion": "10"},
            "spec": spec
        }))
        .unwrap()
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    /// `TestPodDisruptionBudgetStrategy` (strategy_test.go:29-119).
    #[test]
    fn strategy_matches_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(!Strategy.allow_unconditional_update());

        let mut old = pdb(serde_json::json!({
            "minAvailable": 3, "selector": {"matchLabels": {"a": "b"}},
            "unhealthyPodEvictionPolicy": "AlwaysAllow"
        }));
        Strategy.prepare_for_create(&ctx(), &mut old);
        assert_eq!(old.metadata.generation, Some(1));
        assert!(Strategy.validate(&ctx(), &old).is_empty());

        let mut new = old.clone();
        new.status.as_mut().unwrap().current_healthy = 3;
        Strategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.status, old.status, "an update may not set status");
        assert_eq!(new.metadata.generation, Some(1), "spec unchanged");
        assert!(Strategy.validate_update(&ctx(), &new, &old).is_empty());

        let mut swapped = old.clone();
        swapped.spec.min_available = None;
        swapped.spec.max_unavailable = Some(IntOrString::String("28%".into()));
        Strategy.prepare_for_update(&ctx(), &mut swapped, &old);
        assert_eq!(swapped.metadata.generation, Some(2));
        assert!(Strategy.validate_update(&ctx(), &swapped, &old).is_empty());

        let mut invalid = old.clone();
        invalid.spec.unhealthy_pod_eviction_policy = Some("invalid".into());
        assert!(!Strategy.validate_update(&ctx(), &invalid, &old).is_empty());
    }

    /// `TestPodDisruptionBudgetStatusStrategy` (strategy_test.go:121-171).
    #[test]
    fn status_strategy_keeps_spec() {
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(!StatusStrategy.allow_unconditional_update());
        let mut old = pdb(serde_json::json!({
            "minAvailable": 3, "selector": {"matchLabels": {"a": "b"}}
        }));
        old.status = Some(PodDisruptionBudgetStatus {
            current_healthy: 3,
            desired_healthy: 3,
            disruptions_allowed: 1,
            expected_pods: 3,
            observed_generation: None,
            conditions: None,
            disrupted_pods: None,
        });
        let mut new = old.clone();
        new.spec.min_available = Some(IntOrString::Int(2));
        new.status.as_mut().unwrap().current_healthy = 2;
        new.status.as_mut().unwrap().disruptions_allowed = 0;
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.status.as_ref().unwrap().current_healthy, 2);
        assert_eq!(new.spec.min_available, Some(IntOrString::Int(3)));
        assert!(StatusStrategy
            .validate_update(&ctx(), &new, &old)
            .is_empty());
    }

    /// A selector stored with an invalid label value may be kept on update,
    /// but a new one may not introduce it (strategy.go:112-118).
    #[test]
    fn an_invalid_selector_value_survives_only_if_already_stored() {
        let bad = pdb(serde_json::json!({"selector": {"matchExpressions": [
            {"key": "a", "operator": "In", "values": ["-bad-"]}
        ]}}));
        let good = pdb(serde_json::json!({"selector": {"matchLabels": {"a": "b"}}}));
        assert!(!Strategy.validate(&ctx(), &bad).is_empty());
        assert!(Strategy.validate_update(&ctx(), &bad, &bad).is_empty());
        assert!(!Strategy.validate_update(&ctx(), &bad, &good).is_empty());
    }

    /// `Convert_v1_PodDisruptionBudget_To_policy_PodDisruptionBudget`
    /// (conversion.go:26-42) and its test cases (conversion_test.go).
    #[test]
    fn conversion_strips_the_v1beta1_label_unless_match_none() {
        let mut none = pdb(serde_json::json!({"selector": {"matchExpressions": [
            {"key": PDB_V1BETA1_LABEL, "operator": "Exists"}
        ]}}));
        let before = none.spec.selector.clone();
        convert_to_internal(&mut none);
        assert_eq!(none.spec.selector, before, "match-none is preserved");

        // "v1 to internal with match all selector": the DoesNotExist form
        // v1beta1 used for match-all is stripped to `{}`.
        let mut all = pdb(serde_json::json!({"selector": {"matchExpressions": [
            {"key": PDB_V1BETA1_LABEL, "operator": "DoesNotExist"}
        ]}}));
        convert_to_internal(&mut all);
        assert_eq!(all.spec.selector, Some(LabelSelector::default()));

        let mut nil = pdb(serde_json::json!({}));
        convert_to_internal(&mut nil);
        assert_eq!(nil.spec.selector, None);

        let mut mixed = pdb(serde_json::json!({"selector": {
            "matchLabels": {"a": "b"},
            "matchExpressions": [{"key": PDB_V1BETA1_LABEL, "operator": "Exists"}]
        }}));
        convert_to_internal(&mut mixed);
        let sel = mixed.spec.selector.unwrap();
        assert!(sel.match_expressions.is_none(), "{sel:?}");
        assert_eq!(sel.match_labels.unwrap()["a"], "b");
    }
}
