pub mod admission_helper;
pub mod admission_webhook;
pub mod apply;
pub mod authentication;
pub mod authorization;
pub mod cel_validation;
pub mod certificates;
pub mod componentstatus;
pub mod configmap;
pub mod conflict_retry;
pub mod controllerrevision;
pub mod crd;
pub mod cronjob;
pub mod csidriver;
pub mod csinode;
pub mod csistoragecapacity;
pub mod custom_metrics;
pub mod custom_resource;
pub mod daemonset;
pub mod defaults;
pub mod deployment;
pub mod deviceclass;
pub mod external_metrics;
pub use rusternetes_discovery as discovery;
pub mod dryrun;
pub mod endpoints;
pub mod endpointslice;
pub mod event;
#[allow(dead_code)]
pub mod filtering;
pub mod finalizers;
pub mod flowcontrol;
pub mod generic;
pub mod generic_patch;
#[allow(dead_code)]
pub mod health;
pub mod horizontalpodautoscaler;
pub mod ingress;
pub mod ingressclass;
pub mod ipaddress;
pub mod job;
pub mod lease;
pub mod lifecycle;
pub mod limitrange;
pub mod metrics;
pub mod namespace;
pub mod networkpolicy;
pub mod node;
pub mod node_conn;
pub mod openapi;
pub mod persistentvolume;
pub mod persistentvolumeclaim;
pub mod pod;
pub mod pod_subresources;
pub mod poddisruptionbudget;
pub mod podtemplate;
pub mod priorityclass;
pub mod proxy;
pub mod ratcheting;
pub mod rbac;
pub mod replicaset;
pub mod replicationcontroller;
pub mod resourceclaim;
pub mod resourceclaimtemplate;
pub mod resourcequota;
pub mod resourceslice;
pub mod runtimeclass;
pub mod scale;
pub mod secret;
pub mod service;
pub mod service_account;
pub mod servicecidr;
pub mod statefulset;
pub mod status;
pub mod storageclass;
pub mod table;
pub mod validating_admission_policy;
pub mod validation;
pub mod volumeattachment;
pub mod volumeattributesclass;
pub mod volumesnapshot;
pub mod volumesnapshotclass;
pub mod volumesnapshotcontent;
pub mod watch;

/// Compute the list-level resourceVersion from the max item resourceVersion.
/// This uses etcd mod_revisions (from individual items) rather than timestamps.
/// Using timestamps causes LIST+WATCH failures because watches start from a
/// revision that etcd never reaches.
pub fn list_resource_version<T: serde::Serialize>(items: &[T]) -> String {
    let mut max_rv: i64 = 0;
    for item in items {
        if let Ok(v) = serde_json::to_value(item) {
            if let Some(rv_str) = v
                .get("metadata")
                .and_then(|m| m.get("resourceVersion"))
                .and_then(|r| r.as_str())
            {
                if let Ok(rv) = rv_str.parse::<i64>() {
                    if rv > max_rv {
                        max_rv = rv;
                    }
                }
            }
        }
    }
    if max_rv > 0 {
        max_rv.to_string()
    } else {
        "1".to_string()
    }
}

/// Resolve the `metadata.resourceVersion` to stamp on a LIST collection
/// response.
///
/// Upstream Kubernetes semantics: a LIST's `metadata.resourceVersion` is the
/// store revision at which the list was taken (etcd's header revision).
/// client-go's `Reflector.ListAndWatch` (every informer, e.g. Lens) does
/// LIST -> read `list.metadata.resourceVersion` -> WATCH from it. An empty or
/// "0" value makes the reflector unable to start a watch and it falls into a
/// constant relist loop, so live updates never arrive.
///
/// This queries `storage.current_revision()` and renders it as a decimal
/// string. If that call fails (or returns a non-positive revision) it falls
/// back to the maximum item resourceVersion via [`list_resource_version`],
/// which itself never returns `""` (it falls back to `"1"`). The result is
/// therefore guaranteed to be a non-empty `^[0-9]+$` string.
///
/// **The result is never below an item the list returned.** The two sources
/// were previously independent — the revision came from the store, the items
/// from a separate read — so nothing tied them together, and a
/// `current_revision()` that answered even one revision stale handed back a
/// collection RV below an object in its own `items`. A client that then
/// watches from that RV (`Reflector.ListAndWatch`, and every e2e
/// `List` -> `Watch(ResourceVersion: list.ResourceVersion)` pair) gets that
/// object's CREATE replayed: the first event is `ADDED` where the client
/// expects `MODIFIED`. Verified against a rhino-backed api-server — a watch
/// one revision below an object's creation replays it as `ADDED` (#1824).
///
/// Upstream cannot express this bug: the list and its `ResourceVersion` come
/// from one etcd range response (`storage/etcd3.GetList` stamps
/// `getResp.Header.Revision`), so the RV is the revision the snapshot was
/// taken at by construction. Taking the max restores that invariant here.
pub async fn list_collection_resource_version<T: serde::Serialize>(
    storage: &rusternetes_storage::StorageBackend,
    items: &[T],
) -> String {
    use rusternetes_storage::Storage;
    let current = match storage.current_revision().await {
        Ok(rev) if rev > 0 => Some(rev),
        _ => None,
    };
    collection_resource_version(current, &list_resource_version(items))
}

/// Pure core of [`list_collection_resource_version`]: the greater of the store
/// revision and the highest item resourceVersion, as a decimal string.
pub fn collection_resource_version(current_revision: Option<i64>, items_max_rv: &str) -> String {
    let items_rv = items_max_rv.parse::<i64>().unwrap_or(0);
    match current_revision {
        Some(rev) => rev.max(items_rv).to_string(),
        None => items_max_rv.to_string(),
    }
}

#[cfg(test)]
mod collection_rv_tests {
    use super::{collection_resource_version, list_resource_version};
    use serde_json::json;

    /// A LIST's `metadata.resourceVersion` must never be below an item the
    /// same LIST returned.
    ///
    /// The store revision and the items are read separately here, so a
    /// `current_revision()` that answers even one revision stale used to win
    /// outright. A client watching from that RV
    /// (`List` -> `Watch(ResourceVersion: list.ResourceVersion)`) then gets the
    /// newest object's CREATE replayed and sees `ADDED` where it expects
    /// `MODIFIED` — verified against a rhino-backed api-server, where a watch
    /// one revision below an object's creation replays it as `ADDED` (#1824).
    #[test]
    fn collection_rv_is_never_below_a_returned_item() {
        // Stale store revision, fresher items: the items win.
        assert_eq!(collection_resource_version(Some(17), "18"), "18");
        assert_eq!(collection_resource_version(Some(1), "4143"), "4143");
        // Store ahead of the items (writes to other collections): store wins.
        assert_eq!(collection_resource_version(Some(4200), "4143"), "4200");
        // Agreement.
        assert_eq!(collection_resource_version(Some(18), "18"), "18");
        // No store revision available: fall back to the items untouched.
        assert_eq!(collection_resource_version(None, "18"), "18");
        // Empty collection keeps `list_resource_version`'s non-empty guarantee.
        assert_eq!(collection_resource_version(None, "1"), "1");
        assert_eq!(collection_resource_version(Some(42), "1"), "42");
    }

    /// The result stays a non-empty decimal string in every branch — a
    /// reflector cannot start a watch from `""` or `"0"`.
    #[test]
    fn collection_rv_is_always_a_positive_decimal() {
        for (cur, items) in [
            (Some(5_i64), "3"),
            (Some(3), "5"),
            (None, "7"),
            (None, "1"),
            (Some(1), "1"),
        ] {
            let rv = collection_resource_version(cur, items);
            assert!(
                rv.chars().all(|c| c.is_ascii_digit()) && rv != "0" && !rv.is_empty(),
                "bad collection rv {rv:?} for ({cur:?}, {items})"
            );
        }
    }

    /// The item-side input this composes with: highest RV wins, and an empty
    /// collection still yields a usable value.
    #[test]
    fn items_max_rv_feeds_the_collection_rv() {
        let items = vec![
            json!({"metadata": {"resourceVersion": "16"}}),
            json!({"metadata": {"resourceVersion": "18"}}),
            json!({"metadata": {"resourceVersion": "17"}}),
        ];
        assert_eq!(list_resource_version(&items), "18");
        assert_eq!(
            collection_resource_version(Some(17), &list_resource_version(&items)),
            "18",
            "the third FlowSchema's revision must not be left outside the list RV"
        );
        let empty: Vec<serde_json::Value> = vec![];
        assert_eq!(list_resource_version(&empty), "1");
    }
}
