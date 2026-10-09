//! #2610: direct backends must APPLY a status strategic-merge patch to the
//! freshly read object, not overwrite status with the caller's full object.
//!
//! Upstream: nodeutil.PatchNodeStatus
//! (staging/src/k8s.io/component-helpers/node/util/status.go:33-44) PATCHes
//! `/status`; the server applies it with strategicpatch.StrategicMergePatch
//! (apimachinery/pkg/util/strategicpatch/patch.go:812) onto the CURRENT object,
//! so a condition another writer added after the caller's read survives.

use rusternetes_storage::{MemoryStorage, Storage};
use serde_json::json;

#[tokio::test]
async fn direct_backend_applies_delta_onto_current_status() {
    let s = MemoryStorage::new();
    let key = "/registry/minions/n1";
    let seed = json!({"apiVersion":"v1","kind":"Node","metadata":{"name":"n1"},
        "status":{"conditions":[
            {"type":"Ready","status":"True"},
            {"type":"MemoryPressure","status":"False"}]}});
    s.create(key, &seed).await.unwrap();

    // Another writer adds "Extra" after the caller's read.
    let mut cur: serde_json::Value = s.get(key).await.unwrap();
    cur["status"]["conditions"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"Extra","status":"True"}));
    s.update(key, &cur).await.unwrap();

    let stale_full = seed.clone();
    let delta = json!({"status":{"conditions":[{"type":"Ready","status":"False"}]}});
    let out: serde_json::Value = s
        .patch_status_strategic_merge(key, &delta, &stale_full)
        .await
        .unwrap();

    let conds = out["status"]["conditions"].as_array().unwrap();
    assert_eq!(conds.len(), 3, "got {conds:?}");
    let ready = conds.iter().find(|c| c["type"] == "Ready").unwrap();
    assert_eq!(ready["status"], "False");
    assert!(conds.iter().any(|c| c["type"] == "Extra"));
}
