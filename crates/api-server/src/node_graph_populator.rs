//! Feeds the Node authorizer's relationship graph from storage.
//!
//! Upstream builds the graph from shared informers
//! (`plugin/pkg/auth/authorizer/node/graph_populator.go`
//! `AddGraphEventHandlers`: pods, persistentvolumes, volumeattachments, and,
//! when the feature is on, resourceslices and podcertificaterequests; wired in
//! `pkg/kubeapiserver/authorizer/config.go` and
//! `pkg/controlplane/instance.go`). The event handlers themselves are
//! [`GraphPopulator`]; this is the list+watch loop in front of them, the
//! equivalent of a client-go reflector over the storage layer the api-server
//! already owns.
//!
//! A reflector's resync replaces its store, so objects deleted while the watch
//! was down are delivered as deletes (`DeletedFinalStateUnknown` in
//! graph_populator.go's `delete*` handlers). Each loop here does the same by
//! remembering the keys it applied.

use futures::StreamExt;
use rusternetes_common::authz::node_graph::{GraphPopulator, NodeGraph};
use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::resources::dra::ResourceSlice;
use rusternetes_common::resources::podcertificaterequest::PodCertificateRequest;
use rusternetes_common::resources::volume::PersistentVolume;
use rusternetes_common::resources::{Pod, VolumeAttachment};
use rusternetes_storage::{build_prefix, Storage, WatchEvent};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// A namespace/name pair; namespace is empty for cluster-scoped objects.
type Key = (String, String);

const MIN_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Start the populator loops for `graph`. Must run inside a tokio runtime; a
/// caller without one (a synchronous unit test) gets an empty graph, which
/// denies every graph-decided request, as an unsynced informer would.
///
/// The slice and PCR loops mirror the optional informers of
/// `AddGraphEventHandlers`, which are only passed when
/// `DynamicResourceAllocation` / `PodCertificateRequest` are enabled.
pub fn spawn<S: Storage + 'static>(storage: Arc<S>, graph: Arc<NodeGraph>) {
    if tokio::runtime::Handle::try_current().is_err() {
        warn!("node authorizer graph populator not started: no tokio runtime");
        return;
    }
    rusternetes_common::authz::node_graph::register_metrics();
    let populator = Arc::new(GraphPopulator::new(graph));

    {
        let (s, p) = (storage.clone(), populator.clone());
        tokio::spawn(run::<S, Pod>(
            s,
            "pods",
            "pod",
            |o: &Pod| (ns(&o.metadata), o.metadata.name.clone()),
            {
                let p = p.clone();
                move |o: &Pod| p.apply_pod(o)
            },
            {
                let p = p.clone();
                move |(ns, name): &Key| p.delete_pod_by_key(ns, name)
            },
        ));
    }
    {
        let (s, p) = (storage.clone(), populator.clone());
        tokio::spawn(run::<S, PersistentVolume>(
            s,
            "persistentvolumes",
            "persistentvolume",
            |o: &PersistentVolume| (String::new(), o.metadata.name.clone()),
            {
                let p = p.clone();
                move |o: &PersistentVolume| p.add_pv(o)
            },
            {
                let p = p.clone();
                move |(_, name): &Key| p.graph().delete_pv(name)
            },
        ));
    }
    {
        let (s, p) = (storage.clone(), populator.clone());
        tokio::spawn(run::<S, VolumeAttachment>(
            s,
            "volumeattachments",
            "volumeattachment",
            |o: &VolumeAttachment| (String::new(), o.metadata.name.clone()),
            {
                let p = p.clone();
                move |o: &VolumeAttachment| {
                    p.apply_volume_attachment(&o.metadata.name, &o.spec.node_name)
                }
            },
            {
                let p = p.clone();
                move |(_, name): &Key| p.delete_volume_attachment(name)
            },
        ));
    }
    if feature_gates::enabled(Feature::DynamicResourceAllocation) {
        let (s, p) = (storage.clone(), populator.clone());
        tokio::spawn(run::<S, ResourceSlice>(
            s,
            "resourceslices",
            "resourceslice",
            |o: &ResourceSlice| (String::new(), o.metadata.name.clone()),
            {
                let p = p.clone();
                move |o: &ResourceSlice| p.add_resource_slice(o)
            },
            {
                let p = p.clone();
                move |(_, name): &Key| p.graph().delete_resource_slice(name)
            },
        ));
    }
    if feature_gates::enabled(Feature::PodCertificateRequest) {
        let (s, p) = (storage, populator);
        tokio::spawn(run::<S, PodCertificateRequest>(
            s,
            "podcertificaterequests",
            "podcertificaterequest",
            |o: &PodCertificateRequest| (ns(&o.metadata), o.metadata.name.clone()),
            {
                let p = p.clone();
                move |o: &PodCertificateRequest| p.add_pcr(o)
            },
            {
                let p = p.clone();
                move |(ns, name): &Key| p.delete_pcr_by_key(ns, name)
            },
        ));
    }
}

fn ns(meta: &rusternetes_common::types::ObjectMeta) -> String {
    meta.namespace.clone().unwrap_or_default()
}

/// One reflector: watch, list, apply; on any failure back off and start over.
///
/// The watch is opened before the list so no change between the two is
/// missed; an event for an object the list already reflects is idempotent
/// (every `Add*` first clears the object's old edges).
async fn run<S, T>(
    storage: Arc<S>,
    resource: &'static str,
    label: &'static str,
    key_of: impl Fn(&T) -> Key + Send + Sync + 'static,
    upsert: impl Fn(&T) + Send + Sync + 'static,
    delete: impl Fn(&Key) + Send + Sync + 'static,
) where
    S: Storage + 'static,
    T: DeserializeOwned + Serialize + Send + Sync + 'static,
{
    let prefix = build_prefix(resource, None);
    let mut backoff = MIN_BACKOFF;
    let mut known: HashSet<Key> = HashSet::new();
    loop {
        match storage.watch(&prefix).await {
            Err(e) => warn!("node authorizer graph: watch {label}s: {e}"),
            Ok(mut stream) => match storage.list::<T>(&prefix).await {
                Err(e) => warn!("node authorizer graph: list {label}s: {e}"),
                Ok(items) => {
                    // Replace: what the list no longer has was deleted while
                    // nothing was watching.
                    let mut listed: HashSet<Key> = HashSet::with_capacity(items.len());
                    for item in &items {
                        let key = key_of(item);
                        upsert(item);
                        listed.insert(key);
                    }
                    for gone in known.difference(&listed) {
                        delete(gone);
                    }
                    known = listed;
                    backoff = MIN_BACKOFF;
                    debug!("node authorizer graph: {} {label}s synced", known.len());

                    while let Some(event) = stream.next().await {
                        match event {
                            Ok(WatchEvent::Added(_, value) | WatchEvent::Modified(_, value)) => {
                                match serde_json::from_str::<T>(&value) {
                                    Ok(item) => {
                                        known.insert(key_of(&item));
                                        upsert(&item);
                                    }
                                    Err(e) => warn!("node authorizer graph: decode {label}: {e}"),
                                }
                            }
                            Ok(WatchEvent::Deleted(_, value)) => {
                                match serde_json::from_str::<T>(&value) {
                                    Ok(item) => {
                                        let key = key_of(&item);
                                        known.remove(&key);
                                        delete(&key);
                                    }
                                    Err(e) => warn!("node authorizer graph: decode {label}: {e}"),
                                }
                            }
                            Err(e) => {
                                warn!("node authorizer graph: watch {label}s: {e}");
                                break;
                            }
                        }
                    }
                }
            },
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}
