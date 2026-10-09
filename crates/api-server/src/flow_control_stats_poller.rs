//! API Priority and Fairness: the producer of the object-count tracker.
//!
//! Port of `registry/generic/registry/store.go` `Store.startObservingCount`
//! (release-1.35, :1663-1682): every `CountMetricPollPeriod` (1m,
//! `server/options/etcd.go:87`, jitter 1.2 = `resourceCountPollPeriodJitter`,
//! store.go:263) it calls `Storage.Stats` for the resource and, on success,
//! `objectCountTracker.Set(resourceName, stats)`.

use std::sync::Arc;
use std::time::Duration;

use rusternetes_storage::Storage;

use crate::flow_control_object_count::ObjectCountTracker;
use crate::flow_control_work_estimator::Stats;

/// The `(group, resource)` pairs observed. Upstream starts one observer per
/// registered `genericregistry.Store`; this server has no store registry, so
/// the built-in resources are listed here. Custom resources are not observed
/// (their lists cost the minimum seats: `ObjectCountNotFoundErr`).
pub const OBSERVED_RESOURCES: &[(&str, &str)] = &[
    ("", "pods"),
    ("", "services"),
    ("", "endpoints"),
    ("", "configmaps"),
    ("", "secrets"),
    ("", "serviceaccounts"),
    ("", "namespaces"),
    ("", "nodes"),
    ("", "events"),
    ("", "persistentvolumes"),
    ("", "persistentvolumeclaims"),
    ("", "replicationcontrollers"),
    ("", "resourcequotas"),
    ("", "limitranges"),
    ("apps", "deployments"),
    ("apps", "replicasets"),
    ("apps", "statefulsets"),
    ("apps", "daemonsets"),
    ("apps", "controllerrevisions"),
    ("batch", "jobs"),
    ("batch", "cronjobs"),
    ("discovery.k8s.io", "endpointslices"),
    ("networking.k8s.io", "ingresses"),
    ("networking.k8s.io", "networkpolicies"),
    ("rbac.authorization.k8s.io", "roles"),
    ("rbac.authorization.k8s.io", "rolebindings"),
    ("rbac.authorization.k8s.io", "clusterroles"),
    ("rbac.authorization.k8s.io", "clusterrolebindings"),
    ("storage.k8s.io", "storageclasses"),
    ("coordination.k8s.io", "leases"),
    ("apiextensions.k8s.io", "customresourcedefinitions"),
    ("autoscaling", "horizontalpodautoscalers"),
    ("policy", "poddisruptionbudgets"),
    ("flowcontrol.apiserver.k8s.io", "flowschemas"),
    (
        "flowcontrol.apiserver.k8s.io",
        "prioritylevelconfigurations",
    ),
];

/// `schema.GroupResource.String()`, the tracker's key.
fn group_resource(group: &str, resource: &str) -> String {
    if group.is_empty() {
        resource.to_string()
    } else {
        format!("{resource}.{group}")
    }
}

/// One poll of every observed resource (the body of the `JitterUntil`
/// closure, store.go:1668-1678): on a `Stats` error, log and skip; otherwise
/// `Set`.
pub async fn poll_once<S: Storage>(storage: &S, tracker: &ObjectCountTracker) {
    for (group, resource) in OBSERVED_RESOURCES {
        let prefix = rusternetes_storage::build_prefix(resource, None);
        observe(storage, tracker, group, resource, &prefix).await;
    }
    for (group, plural) in registered_custom_resources(storage).await {
        // The storage type of `handlers/custom_resource.rs`.
        let resource_type = format!("{}_{}", group.replace('.', "_"), plural);
        let prefix = rusternetes_storage::build_prefix(&resource_type, None);
        observe(storage, tracker, &group, &plural, &prefix).await;
    }
}

/// The `Stats` call and `Set` of the `JitterUntil` closure, store.go:1668-1678.
async fn observe<S: Storage>(
    storage: &S,
    tracker: &ObjectCountTracker,
    group: &str,
    resource: &str,
    prefix: &str,
) {
    match storage.stats(prefix).await {
        Ok(st) => tracker.set(
            &group_resource(group, resource),
            Stats {
                object_count: st.object_count,
                estimated_average_object_size_bytes: st.estimated_average_object_size_bytes,
            },
        ),
        Err(e) => {
            tracing::debug!(resource, error = %e, "Failed to update storage count metric");
        }
    }
}

/// `(spec.group, spec.names.plural)` of every registered CRD. Upstream gives
/// each CRD version its own `genericregistry.Store`
/// (`apiextensions-apiserver/pkg/apiserver/customresource_handler.go:855`
/// `customresource.NewStorage`), and every Store starts its own observer
/// (`store.go:1638` `startObservingCount`), so custom
/// resources are counted exactly like built-ins. This server has no store
/// registry; the registered CRDs are read each poll, which also stops
/// observing a CRD once it is deleted (upstream's `destroy`).
async fn registered_custom_resources<S: Storage>(storage: &S) -> Vec<(String, String)> {
    let prefix = rusternetes_storage::build_prefix("customresourcedefinitions", None);
    let crds: Vec<serde_json::Value> = match storage.list(&prefix).await {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!(error = %e, "Failed to list CRDs for the object count poller");
            return Vec::new();
        }
    };
    crds.iter()
        .filter_map(|crd| {
            let group = crd.pointer("/spec/group")?.as_str()?;
            let plural = crd.pointer("/spec/names/plural")?.as_str()?;
            Some((group.to_string(), plural.to_string()))
        })
        .collect()
}

/// `CountMetricPollPeriod` default (server/options/etcd.go:87).
pub const COUNT_METRIC_POLL_PERIOD: Duration = Duration::from_secs(60);

/// `resourceCountPollPeriodJitter` (store.go:263).
const RESOURCE_COUNT_POLL_PERIOD_JITTER: f64 = 1.2;

/// `wait.JitterUntil(f, period, 1.2, sliding=true, stopCh)`: runs `f`
/// immediately, then after `period + rand*1.2*period`, until stopped.
pub async fn run<S: Storage>(
    storage: Arc<S>,
    tracker: Arc<ObjectCountTracker>,
    period: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *stop.borrow() {
            return;
        }
        poll_once(storage.as_ref(), &tracker).await;
        let wait = period.mul_f64(1.0 + rand::random::<f64>() * RESOURCE_COUNT_POLL_PERIOD_JITTER);
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            r = stop.changed() => {
                if r.is_err() || *stop.borrow() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow_control_work_estimator::StatsError;
    use rusternetes_storage::memory::MemoryStorage;

    async fn seed(s: &MemoryStorage, key: &str) {
        let v = serde_json::json!({"metadata": {"name": "x"}});
        s.create(key, &v).await.unwrap();
    }

    #[tokio::test]
    async fn poll_sets_count_and_size_under_the_group_resource_name() {
        let s = MemoryStorage::new();
        seed(&s, "/registry/pods/ns/a").await;
        seed(&s, "/registry/pods/ns/b").await;
        seed(&s, "/registry/deployments/ns/d").await;
        let t = ObjectCountTracker::new();
        poll_once(&s, &t).await;
        let (st, err) = t.get("pods");
        assert_eq!(err, None);
        assert_eq!(st.object_count, 2);
        assert!(st.estimated_average_object_size_bytes > 0);
        // Group-qualified like `schema.GroupResource.String()`.
        let (st, err) = t.get("deployments.apps");
        assert_eq!(err, None);
        assert_eq!(st.object_count, 1);
    }

    #[tokio::test]
    async fn an_empty_resource_is_recorded_as_zero_objects() {
        let s = MemoryStorage::new();
        let t = ObjectCountTracker::new();
        poll_once(&s, &t).await;
        let (st, err) = t.get("pods");
        assert_eq!(err, None, "polled and empty is not NotFound");
        assert_eq!(st.object_count, 0);
        assert_eq!(
            t.get("nonexistent.example.com").1,
            Some(StatsError::NotFound)
        );
    }

    /// Upstream starts an observer per registered store, CRD stores included
    /// (`customresource_handler.go` builds a `genericregistry.Store` per CRD
    /// version), so a custom resource's list is costed by its object count.
    #[tokio::test]
    async fn a_registered_crd_is_observed_under_plural_dot_group() {
        let s = MemoryStorage::new();
        let crd = serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "widgets.example.com"},
            "spec": {
                "group": "example.com",
                "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
                "scope": "Namespaced",
                "versions": [{"name": "v1", "served": true, "storage": true}]
            }
        });
        s.create(
            "/registry/customresourcedefinitions/widgets.example.com",
            &crd,
        )
        .await
        .unwrap();
        let cr = serde_json::json!({"metadata": {"name": "w1", "namespace": "ns"}});
        s.create("/registry/example_com_widgets/ns/w1", &cr)
            .await
            .unwrap();
        let t = ObjectCountTracker::new();
        poll_once(&s, &t).await;
        let (st, err) = t.get("widgets.example.com");
        assert_eq!(err, None, "CRD resources are polled, not NotFound");
        assert_eq!(st.object_count, 1);
    }

    #[tokio::test]
    async fn run_polls_immediately_then_stops_on_signal() {
        let s = Arc::new(MemoryStorage::new());
        seed(&s, "/registry/pods/ns/a").await;
        let t = Arc::new(ObjectCountTracker::new());
        let (tx, rx) = tokio::sync::watch::channel(false);
        let h = tokio::spawn(run(s, t.clone(), Duration::from_secs(3600), rx));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(t.get("pods").0.object_count, 1);
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), h)
            .await
            .expect("poller exits on stop")
            .unwrap();
    }
}
