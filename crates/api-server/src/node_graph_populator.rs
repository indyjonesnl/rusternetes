//! Feeds the Node authorizer's graph from the store.
//!
//! Ported from `plugin/pkg/auth/authorizer/node/graph_populator.go`
//! `AddGraphEventHandlers`: one informer per type (pods, persistent volumes,
//! volume attachments, resource slices and, when `PodCertificateRequest` is on,
//! pod certificate requests) whose add/update/delete handlers are the
//! `populator_*` functions in `rusternetes_common::node_graph`.
//!
//! An informer is a list plus a watch plus a store of the last seen object. The
//! store is what supplies the `oldObj` argument of the update handlers
//! (`updatePod`'s "node unchanged" filter, `updateVolumeAttachment`'s "node
//! unchanged" filter); a relist after a dropped watch deletes what the list no
//! longer holds, as an informer's `Replace` does with `DeletedFinalStateUnknown`.
//! Until the first list has been applied the graph is empty and the authorizer
//! denies, exactly as upstream does before `WaitForNamedCacheSync` returns.

use futures::StreamExt;
use rusternetes_common::node_graph::{
    populator_add_pcr, populator_add_resource_slice, populator_delete_pcr, populator_delete_pod,
    populator_delete_pv, populator_delete_resource_slice, populator_delete_volume_attachment,
    populator_update_pod, populator_update_pv, populator_update_volume_attachment, NodeGraph,
};
use rusternetes_storage::{build_prefix, Storage, WatchEvent};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// How long to wait before re-establishing a failed list or watch.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// `cache.ResourceEventHandlerFuncs`. `old` is the previously seen object, `None`
/// for an add.
struct Handlers {
    upsert: fn(&NodeGraph, Option<&Value>, &Value),
    delete: fn(&NodeGraph, &Value),
}

fn pods() -> Handlers {
    Handlers {
        upsert: populator_update_pod,
        delete: populator_delete_pod,
    }
}

fn pvs() -> Handlers {
    Handlers {
        upsert: |g, _old, new| populator_update_pv(g, new),
        delete: populator_delete_pv,
    }
}

fn attachments() -> Handlers {
    Handlers {
        upsert: populator_update_volume_attachment,
        delete: populator_delete_volume_attachment,
    }
}

/// `UpdateFunc: nil, // Not needed, NodeName is immutable.`
fn slices() -> Handlers {
    Handlers {
        upsert: |g, old, new| {
            if old.is_none() {
                populator_add_resource_slice(g, new)
            }
        },
        delete: populator_delete_resource_slice,
    }
}

/// `UpdateFunc: nil, // Not needed, spec fields are immutable.`
fn pcrs() -> Handlers {
    Handlers {
        upsert: |g, old, new| {
            if old.is_none() {
                populator_add_pcr(g, new)
            }
        },
        delete: populator_delete_pcr,
    }
}

/// `AddGraphEventHandlers`: start the informers. `pod_certificate_requests` is
/// the `PodCertificateRequest` feature gate (upstream passes a nil informer when
/// it is off).
pub fn spawn_graph_populator<S: Storage + 'static>(
    storage: Arc<S>,
    graph: Arc<NodeGraph>,
    pod_certificate_requests: bool,
) {
    let mut informers: Vec<(&'static str, Handlers)> = vec![
        ("pods", pods()),
        ("persistentvolumes", pvs()),
        ("volumeattachments", attachments()),
        ("resourceslices", slices()),
    ];
    if pod_certificate_requests {
        informers.push(("podcertificaterequests", pcrs()));
    }
    for (resource, handlers) in informers {
        let storage = storage.clone();
        let graph = graph.clone();
        tokio::spawn(async move { run_informer(storage, graph, resource, handlers).await });
    }
}

fn object_key(obj: &Value) -> String {
    let meta = |k: &str| {
        obj.get("metadata")
            .and_then(|m| m.get(k))
            .and_then(Value::as_str)
            .unwrap_or("")
    };
    format!("{}/{}", meta("namespace"), meta("name"))
}

async fn run_informer<S: Storage + 'static>(
    storage: Arc<S>,
    graph: Arc<NodeGraph>,
    resource: &'static str,
    handlers: Handlers,
) {
    let prefix = build_prefix(resource, None);
    let mut store: HashMap<String, Value> = HashMap::new();
    loop {
        // Watch first, then list, so no event falls between the two; the
        // handlers are idempotent for the overlap.
        let mut stream = match storage.watch(&prefix).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("node authorizer graph: cannot watch {resource}: {e}");
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }
        };
        let listed: Vec<Value> = match storage.list::<Value>(&prefix).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!("node authorizer graph: cannot list {resource}: {e}");
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }
        };

        // Replace: upsert everything listed, delete what is no longer there.
        let mut present = std::collections::HashSet::new();
        for obj in listed {
            let key = object_key(&obj);
            present.insert(key.clone());
            (handlers.upsert)(&graph, store.get(&key), &obj);
            store.insert(key, obj);
        }
        let gone: Vec<String> = store
            .keys()
            .filter(|k| !present.contains(*k))
            .cloned()
            .collect();
        for key in gone {
            if let Some(old) = store.remove(&key) {
                (handlers.delete)(&graph, &old);
            }
        }
        tracing::info!("node authorizer graph: {resource} synced");

        while let Some(event) = stream.next().await {
            let Ok(event) = event else { continue };
            match event {
                WatchEvent::Added(_, v) | WatchEvent::Modified(_, v) => {
                    let Ok(obj) = serde_json::from_str::<Value>(&v) else {
                        continue;
                    };
                    let key = object_key(&obj);
                    (handlers.upsert)(&graph, store.get(&key), &obj);
                    store.insert(key, obj);
                }
                WatchEvent::Deleted(_, v) => {
                    let Ok(obj) = serde_json::from_str::<Value>(&v) else {
                        continue;
                    };
                    store.remove(&object_key(&obj));
                    (handlers.delete)(&graph, &obj);
                }
            }
        }
        // The watch ended: relist and re-watch.
        tokio::time::sleep(RETRY_DELAY).await;
    }
}
