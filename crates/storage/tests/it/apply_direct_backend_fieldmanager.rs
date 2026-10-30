//! #3292: `Storage::apply` on direct backends must run the api-server's
//! field manager (structured `fieldsV1`, list-map merge by key), so the
//! `managedFields` entry equals the one ApiStorage gets from a real server.
//!
//! Upstream: `FieldManager.Apply`
//! (staging/src/k8s.io/apimachinery/pkg/util/managedfields/internal/fieldmanager.go:183-209)
//! over `structured-merge-diff` `Updater.Apply`
//! (sigs.k8s.io/structured-merge-diff/v6/merge/update.go:209-254); conditions
//! merge by `type` (`+listType=map`, `+listMapKey=type`).

use rusternetes_storage::{MemoryStorage, Storage};
use serde_json::{json, Value};

#[tokio::test]
async fn status_apply_merges_conditions_by_type_with_structured_fields() {
    let s = MemoryStorage::new();
    let key = "/registry/flowschemas/fs1";
    s.create(
        key,
        &json!({"apiVersion":"flowcontrol.apiserver.k8s.io/v1","kind":"FlowSchema",
            "metadata":{"name":"fs1"},
            "status":{"conditions":[{"type":"Other","status":"True"}]}}),
    )
    .await
    .unwrap();

    let cfg = json!({"apiVersion":"flowcontrol.apiserver.k8s.io/v1","kind":"FlowSchema",
        "metadata":{"name":"fs1"},
        "status":{"conditions":[{"type":"Dangling","status":"True"}]}});
    let out: Value = s
        .apply(
            key,
            &cfg,
            "api-priority-and-fairness-config-consumer-v1",
            true,
            Some("status"),
        )
        .await
        .unwrap();

    let conds = out["status"]["conditions"].as_array().unwrap();
    assert_eq!(conds.len(), 2, "conditions must merge by type: {conds:?}");

    let entry = out["metadata"]["managedFields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["operation"] == "Apply")
        .expect("apply entry");
    let fields = &entry["fieldsV1"];
    assert!(
        fields.get("rules").is_none(),
        "top-level-key engine shape: {fields}"
    );
    assert!(
        fields["f:status"]["f:conditions"]
            .get("k:{\"type\":\"Dangling\"}")
            .is_some(),
        "structured fieldsV1 expected: {fields}"
    );
}
