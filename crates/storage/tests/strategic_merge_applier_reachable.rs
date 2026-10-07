//! #2610: the strategic-merge applier must be reachable from `storage`, which
//! cannot depend on `api-server`. Direct backends (sqlite/redis/memory) need it
//! to apply a status patch instead of overwriting with the full object.
//!
//! Upstream: strategicpatch.StrategicMergePatch
//! (apimachinery/pkg/util/strategicpatch/patch.go:812) merges `status.conditions`
//! by its `type` merge key, so a delta carrying one condition leaves the rest.

use rusternetes_common::patch::{apply_patch, PatchType};
use serde_json::json;

#[test]
fn status_patch_merges_conditions_by_type_not_replace() {
    let current = json!({
        "status": {"conditions": [
            {"type": "Ready", "status": "True"},
            {"type": "MemoryPressure", "status": "False"},
        ]}
    });
    let delta = json!({
        "status": {"conditions": [{"type": "Ready", "status": "False"}]}
    });
    let out = apply_patch(&current, &delta, PatchType::StrategicMergePatch).unwrap();
    let conds = out["status"]["conditions"].as_array().unwrap();
    assert_eq!(conds.len(), 2, "merge-keyed list must not be replaced");
    let ready = conds.iter().find(|c| c["type"] == "Ready").unwrap();
    assert_eq!(ready["status"], "False");
}
