//! PodDisruptionBudget validation — port of upstream Kubernetes
//! `pkg/apis/policy/validation/validation.go::ValidatePodDisruptionBudgetSpec`
//! (release-1.35).

use crate::resources::policy::{
    IntOrString, PodDisruptionBudget, PodDisruptionBudgetCondition, PodDisruptionBudgetSpec,
    PodDisruptionBudgetStatus,
};
use crate::types::Condition;
use crate::validation::apps::{is_not_more_than_100_percent, validate_positive_int_or_percent};
use crate::validation::field::{BadValue, Error, ErrorList, Path};
use crate::validation::metav1::{
    validate_conditions, validate_label_selector, LabelSelectorValidationOptions,
};
use crate::validation::objectmeta::validate_nonnegative_field;

/// `PodDisruptionBudgetValidationOptions`
/// (pkg/apis/policy/validation/validation.go:37-39).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PodDisruptionBudgetValidationOptions {
    pub allow_invalid_label_value_in_selector: bool,
}

/// The bad value upstream reports when both bounds are set: the internal
/// `policy.PodDisruptionBudgetSpec` itself (`field.Invalid(fldPath, spec, …)`,
/// validation.go:53). The internal type has no json tags
/// (pkg/apis/policy/types.go), so it marshals with Go field names and a nil
/// pointer as `null`; the selector is a `metav1.LabelSelector`, which does
/// carry tags.
fn spec_bad_value(spec: &PodDisruptionBudgetSpec) -> BadValue {
    // Serialized from a struct, not a `json!` map: `serde_json::Value` sorts
    // keys, Go's `json.Marshal` keeps struct field order (#2086).
    #[derive(serde::Serialize)]
    struct InternalSpec<'a> {
        #[serde(rename = "MinAvailable")]
        min_available: Option<serde_json::Value>,
        #[serde(rename = "Selector")]
        selector: &'a Option<crate::types::LabelSelector>,
        #[serde(rename = "MaxUnavailable")]
        max_unavailable: Option<serde_json::Value>,
        #[serde(rename = "UnhealthyPodEvictionPolicy")]
        unhealthy_pod_eviction_policy: &'a Option<String>,
    }
    let int_or_string = |v: &Option<IntOrString>| v.as_ref().map(IntOrString::to_json);
    BadValue::marshal(&InternalSpec {
        min_available: int_or_string(&spec.min_available),
        selector: &spec.selector,
        max_unavailable: int_or_string(&spec.max_unavailable),
        unhealthy_pod_eviction_policy: &spec.unhealthy_pod_eviction_policy,
    })
}

/// `ValidatePodDisruptionBudgetSpec`
/// (pkg/apis/policy/validation/validation.go:48-76).
pub fn validate_pod_disruption_budget_spec(
    spec: &PodDisruptionBudgetSpec,
    opts: PodDisruptionBudgetValidationOptions,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if spec.min_available.is_some() && spec.max_unavailable.is_some() {
        errs.push(Error::invalid(
            fld_path,
            spec_bad_value(spec),
            "minAvailable and maxUnavailable cannot be both set",
        ));
    }

    if let Some(mn) = &spec.min_available {
        let path = fld_path.child("minAvailable");
        errs.extend(validate_positive_int_or_percent(mn, &path).1);
        errs.extend(is_not_more_than_100_percent(mn, &path));
    }
    if let Some(mx) = &spec.max_unavailable {
        let path = fld_path.child("maxUnavailable");
        errs.extend(validate_positive_int_or_percent(mx, &path).1);
        errs.extend(is_not_more_than_100_percent(mx, &path));
    }

    // `ValidateLabelSelector` returns no errors for a nil selector
    // (apimachinery/pkg/apis/meta/v1/validation/validation.go). A PDB with no
    // selector is valid; it guards no pods.
    if let Some(selector) = &spec.selector {
        errs.extend(validate_label_selector(
            selector,
            LabelSelectorValidationOptions {
                allow_invalid_label_value_in_selector: opts.allow_invalid_label_value_in_selector,
                ..Default::default()
            },
            &fld_path.child("selector"),
        ));
    }

    // `supportedUnhealthyPodEvictionPolicies` (validation.go:33-35).
    if let Some(policy) = &spec.unhealthy_pod_eviction_policy {
        if policy != "IfHealthyBudget" && policy != "AlwaysAllow" {
            errs.push(Error::not_supported(
                &fld_path.child("unhealthyPodEvictionPolicy"),
                policy.clone(),
                &["AlwaysAllow", "IfHealthyBudget"],
            ));
        }
    }

    errs
}

/// `ValidatePodDisruptionBudget` (validation.go:41-46).
pub fn validate_pod_disruption_budget(
    pdb: &PodDisruptionBudget,
    opts: PodDisruptionBudgetValidationOptions,
) -> ErrorList {
    validate_pod_disruption_budget_spec(&pdb.spec, opts, &Path::new("spec"))
}

/// Convert a PDB-specific condition into the generic `metav1.Condition`
/// understood by [`validate_conditions`]. Upstream stores PDB conditions as
/// `[]metav1.Condition` directly; our resource type carries a dedicated struct,
/// so we map field-for-field. Upstream treats a zero `metav1.Time` as
/// "missing"; an empty or unparseable `lastTransitionTime` string maps to
/// `None` so `ValidateCondition`'s required-time check fires identically.
fn to_metav1_condition(c: &PodDisruptionBudgetCondition) -> Condition {
    let last_transition_time = c
        .last_transition_time
        .as_deref()
        .filter(|s| !s.is_empty())
        .and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        });
    Condition {
        condition_type: c.condition_type.clone(),
        status: c.status.clone(),
        observed_generation: c.observed_generation,
        last_transition_time,
        reason: c.reason.clone(),
        message: c.message.clone(),
    }
}

/// Validate a `PodDisruptionBudgetStatus` on a status-subresource update.
/// Mirrors upstream `ValidatePodDisruptionBudgetStatusUpdate`
/// (`pkg/apis/policy/validation/validation.go`): validate the condition list,
/// then require the disruption counters to be non-negative.
///
/// Upstream takes `oldStatus` and `apiVersion` parameters. The `oldStatus` is
/// unused in the upstream body (no transition checks), and the `apiVersion`
/// guard only short-circuits the non-negative checks for the legacy
/// `policy/v1beta1` group — which rusternetes does not serve (target is
/// `policy/v1`, k8s v1.35), so the non-negative checks always run here.
pub fn validate_pod_disruption_budget_status(
    status: &PodDisruptionBudgetStatus,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if let Some(conditions) = &status.conditions {
        let metav1_conditions: Vec<Condition> =
            conditions.iter().map(to_metav1_condition).collect();
        errs.extend(validate_conditions(
            &metav1_conditions,
            &fld_path.child("conditions"),
        ));
    }

    errs.extend(validate_nonnegative_field(
        i64::from(status.disruptions_allowed),
        &fld_path.child("disruptionsAllowed"),
    ));
    errs.extend(validate_nonnegative_field(
        i64::from(status.current_healthy),
        &fld_path.child("currentHealthy"),
    ));
    errs.extend(validate_nonnegative_field(
        i64::from(status.desired_healthy),
        &fld_path.child("desiredHealthy"),
    ));
    errs.extend(validate_nonnegative_field(
        i64::from(status.expected_pods),
        &fld_path.child("expectedPods"),
    ));

    errs
}

/// Validate a `PodDisruptionBudget` status-subresource update. Mirrors the
/// `status` path of upstream `ValidatePodDisruptionBudgetStatusUpdate`, rooted
/// at `status`. `_old` is accepted for upstream-signature parity (the upstream
/// body performs no old-vs-new transition checks).
pub fn validate_pod_disruption_budget_status_update(
    pdb: &PodDisruptionBudget,
    _old: &PodDisruptionBudget,
) -> ErrorList {
    match &pdb.status {
        Some(status) => validate_pod_disruption_budget_status(status, &Path::new("status")),
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::policy::PodDisruptionBudgetSpec;
    use crate::types::LabelSelector;

    fn empty_status() -> PodDisruptionBudgetStatus {
        PodDisruptionBudgetStatus {
            current_healthy: 0,
            desired_healthy: 0,
            disruptions_allowed: 0,
            expected_pods: 0,
            observed_generation: None,
            conditions: None,
            disrupted_pods: None,
        }
    }

    fn pdb_with_status(status: PodDisruptionBudgetStatus) -> PodDisruptionBudget {
        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(1)),
            max_unavailable: None,
            selector: Some(LabelSelector {
                match_labels: None,
                match_expressions: None,
            }),
            unhealthy_pod_eviction_policy: None,
        };
        let mut pdb = PodDisruptionBudget::new("pdb", "default", spec);
        pdb.status = Some(status);
        pdb
    }

    #[test]
    fn status_update_accepts_valid_status() {
        let status = empty_status();
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn status_update_no_status_is_ok() {
        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(1)),
            max_unavailable: None,
            selector: Some(LabelSelector {
                match_labels: None,
                match_expressions: None,
            }),
            unhealthy_pod_eviction_policy: None,
        };
        let pdb = PodDisruptionBudget::new("pdb", "default", spec);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(errs.is_empty());
    }

    #[test]
    fn status_update_rejects_negative_disruptions_allowed() {
        let mut status = empty_status();
        status.disruptions_allowed = -1;
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert_eq!(errs[0].field, "status.disruptionsAllowed");
    }

    #[test]
    fn status_update_rejects_negative_current_healthy() {
        let mut status = empty_status();
        status.current_healthy = -5;
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert_eq!(errs[0].field, "status.currentHealthy");
    }

    #[test]
    fn status_update_rejects_negative_desired_healthy() {
        let mut status = empty_status();
        status.desired_healthy = -1;
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert_eq!(errs[0].field, "status.desiredHealthy");
    }

    #[test]
    fn status_update_rejects_negative_expected_pods() {
        let mut status = empty_status();
        status.expected_pods = -1;
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert_eq!(errs[0].field, "status.expectedPods");
    }

    #[test]
    fn status_update_accepts_valid_condition() {
        let mut status = empty_status();
        status.conditions = Some(vec![PodDisruptionBudgetCondition {
            condition_type: "DisruptionAllowed".to_string(),
            status: "True".to_string(),
            last_transition_time: Some("2024-01-01T00:00:00Z".to_string()),
            reason: Some("SufficientPods".to_string()),
            message: Some("ok".to_string()),
            observed_generation: Some(1),
        }]);
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(errs.is_empty(), "expected no errors, got {errs:?}");
    }

    #[test]
    fn status_update_rejects_bad_condition_status() {
        let mut status = empty_status();
        status.conditions = Some(vec![PodDisruptionBudgetCondition {
            condition_type: "DisruptionAllowed".to_string(),
            status: "Maybe".to_string(),
            last_transition_time: Some("2024-01-01T00:00:00Z".to_string()),
            reason: Some("SufficientPods".to_string()),
            message: None,
            observed_generation: None,
        }]);
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(
            errs.iter()
                .any(|e| e.field == "status.conditions[0].status"),
            "expected condition status error, got {errs:?}"
        );
    }

    #[test]
    fn status_update_rejects_condition_missing_transition_time() {
        // Empty lastTransitionTime maps to None → required-time check fires.
        let mut status = empty_status();
        status.conditions = Some(vec![PodDisruptionBudgetCondition {
            condition_type: "DisruptionAllowed".to_string(),
            status: "True".to_string(),
            last_transition_time: Some(String::new()),
            reason: Some("SufficientPods".to_string()),
            message: None,
            observed_generation: None,
        }]);
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(
            errs.iter()
                .any(|e| e.field == "status.conditions[0].lastTransitionTime"),
            "expected lastTransitionTime required error, got {errs:?}"
        );
    }

    #[test]
    fn status_update_rejects_duplicate_condition_types() {
        let mut status = empty_status();
        let cond = PodDisruptionBudgetCondition {
            condition_type: "DisruptionAllowed".to_string(),
            status: "True".to_string(),
            last_transition_time: Some("2024-01-01T00:00:00Z".to_string()),
            reason: Some("SufficientPods".to_string()),
            message: None,
            observed_generation: None,
        };
        status.conditions = Some(vec![cond.clone(), cond]);
        let pdb = pdb_with_status(status);
        let errs = validate_pod_disruption_budget_status_update(&pdb, &pdb);
        assert!(
            errs.iter().any(|e| e.field == "status.conditions[1].type"),
            "expected duplicate-type error, got {errs:?}"
        );
    }
}
