//! `RuntimeCELCostBudget` enforcement of the MutatingAdmissionPolicy patchers
//! (#3507): `dispatcher.go:259` passes `celconfig.RuntimeCELCostBudget` to
//! `Patcher.Patch`, which hands it to `MutatingEvaluator.ForInput`
//! (`json_patch.go:82`, `smd.go:77`); `activation.Evaluate`
//! (`activation.go:132-189`) charges the composited variables and the
//! expression against it and fails with `ErrOutOfBudget` when it runs out.

use super::*;
use crate::admission::policy_apply_config::ApplyConfigPatcher;
use crate::admission::policy_matching::Attributes;
use crate::admission::policy_mutating::VersionedAttributes;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
use rusternetes_common::cel_cost::RUNTIME_CEL_COST_BUDGET;
use serde_json::json;

const OUT_OF_BUDGET: &str =
    "validation failed due to running out of cost budget, no further validation rules will be run";

/// Each `object.xs.all(i, i >= 0)` over this object costs 500,003: under the
/// 1M per-call limit, so every variable evaluates, but more than twenty of
/// them are over the 10M `RuntimeCELCostBudget`.
fn heavy_object() -> Value {
    let xs: Vec<u32> = (0..100_000).collect();
    json!({"xs": xs})
}

fn variables(n: usize) -> Vec<(String, String)> {
    (0..n)
        .map(|i| (format!("v{i}"), "object.xs.all(i, i >= 0)".to_string()))
        .collect()
}

fn reads(n: usize) -> String {
    (0..n)
        .map(|i| format!("variables.v{i}"))
        .collect::<Vec<_>>()
        .join(" && ")
}

fn run(patcher: Arc<dyn Patcher>) -> Result<Value, String> {
    let gvk = GroupVersionKind {
        group: "apps".into(),
        version: "v1".into(),
        kind: "Deployment".into(),
    };
    let gvr = GroupVersionResource {
        group: "apps".into(),
        version: "v1".into(),
        resource: "deployments".into(),
    };
    let object = heavy_object();
    let versioned = VersionedAttributes {
        versioned_kind: gvk.clone(),
        versioned_object: Some(object.clone()),
        versioned_old_object: None,
        dirty: false,
    };
    let attributes = Attributes {
        kind: gvk,
        resource: gvr.clone(),
        subresource: "".into(),
        namespace: "default".into(),
        name: "d1".into(),
        operation: Operation::Create,
        object: Some(object),
        old_object: None,
        user_info: None,
        dry_run: false,
        operation_options: None,
    };
    let req = PatchRequest {
        matched_resource: &gvr,
        versioned_attributes: &versioned,
        attributes: &attributes,
        param: None,
        namespace: None,
        authorizer: None,
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(patcher.patch(&req, RUNTIME_CEL_COST_BUDGET))
        .map_err(|e| match e {
            PatchError::Status(e) => e.to_string(),
            PatchError::Other(m) => m,
        })
}

fn apply_config(n: usize) -> Result<Value, String> {
    let expr = format!("{} ? Object{{}} : Object{{}}", reads(n));
    run(ApplyConfigPatcher::new_patcher_with_variables(
        &expr,
        &variables(n),
    ))
}

fn json_patch(n: usize) -> Result<Value, String> {
    let expr = format!(
        "{} ? [JSONPatch{{op: \"add\", path: \"/spec\", value: 1}}] : []",
        reads(n)
    );
    run(JsonPatcher::new_patcher_with_variables(
        &expr,
        &variables(n),
    ))
}

/// activation.go:162-168: the composited variables spend more than the
/// budget, so the patch fails instead of applying.
#[test]
fn json_patch_variables_over_budget_fail_the_patch() {
    assert_eq!(json_patch(24).unwrap_err(), OUT_OF_BUDGET);
}

/// smd.go:77 `ForInput(..., runtimeCELCostBudget)`.
#[test]
fn apply_configuration_variables_over_budget_fail_the_patch() {
    assert_eq!(apply_config(24).unwrap_err(), OUT_OF_BUDGET);
}

/// Within the budget nothing changes.
#[test]
fn within_budget_still_patches() {
    json_patch(3).unwrap();
    apply_config(3).unwrap();
}
