pub mod apiservice;
pub mod attach_detach;
pub mod cascade_tests;
pub mod cert_authority;
pub mod certificate_signing_request;
pub mod cidrset_metrics;
pub mod clusterrole_aggregation;
pub mod cronjob;
pub mod csi_attacher;
pub mod csi_capacity;
pub mod csi_capacity_metrics;
pub mod csi_provisioner;
pub mod csi_topology;
pub mod daemonset;
pub mod deployment;
pub mod dynamic_provisioner;
pub mod endpoints;
pub mod endpoints_tracker;
pub mod endpointslice;
pub mod endpointslice_tracker;
pub mod ephemeral_volume;
pub mod ephemeral_volume_metrics;
pub mod events;
pub mod expectations;
pub mod garbage_collector;
pub mod hpa;
pub mod hpa_behavior;
pub mod hpa_metrics_client;
pub mod hpa_pod_grouping;
pub mod hpa_replica_calculator;
pub mod ingress;
pub mod job;
pub mod job_tracking;
pub mod legacy_serviceaccount_token_cleaner;
pub mod limitrange;
pub mod loadbalancer;
pub mod namespace;
pub mod network_policy;
pub mod node;
pub mod node_ipam;
pub mod node_lifecycle_metrics;
pub mod node_lifecycle_queue;
pub mod node_lifecycle_zone;
pub mod pod_disruption_budget;
pub mod priorityclass;
pub mod pv_binder;
pub mod pv_protection;
pub mod pv_recycler;
pub mod pvc_protection;
pub mod replicaset;
pub mod replicationcontroller;
pub mod resource_quota;
#[allow(dead_code)]
pub mod resourceclaim;
pub mod service;
pub mod serviceaccount;
pub mod servicecidr;
pub mod statefulset;
#[cfg(test)]
pub mod status_subresource_double;
pub mod storage_class;
pub mod taint_eviction;
pub mod taint_eviction_timed_workers;
pub mod trigger_time_tracker;
pub mod ttl_controller;
#[cfg(test)]
pub mod uid_precondition_double;
#[allow(dead_code)]
pub mod volume_expansion;
pub mod volume_snapshot;
pub mod vpa;
pub mod worker_pool;
#[cfg(test)]
mod worker_pool_tests;

/// Propagate the pod's ServiceAccount `imagePullSecrets` onto a
/// controller-created pod spec (#1084). Controllers write pods straight to
/// storage and bypass the api-server admission path, so without this the SA's
/// pull secrets never reach DaemonSet/ReplicaSet/StatefulSet/Job pods.
///
/// Resolves the SA named by the spec (default `"default"`) and applies the
/// same semantics as admission (PR #1083): copy only when the pod declares no
/// secrets of its own, regardless of automount. A missing SA is a no-op.
pub async fn propagate_sa_image_pull_secrets<S: rusternetes_storage::Storage>(
    storage: &S,
    namespace: &str,
    spec: &mut rusternetes_common::resources::PodSpec,
) {
    let sa_name = spec.service_account_name.as_deref().unwrap_or("default");
    let sa_key = format!("/registry/serviceaccounts/{}/{}", namespace, sa_name);
    if let Ok(sa) = storage
        .get::<rusternetes_common::resources::ServiceAccount>(&sa_key)
        .await
    {
        rusternetes_common::serviceaccount::propagate_image_pull_secrets(
            spec,
            sa.image_pull_secrets.as_deref(),
        );
    }
}

/// Check ResourceQuota before creating a pod in a namespace.
/// Returns Ok(()) if quota allows, Err with quota exceeded message otherwise.
pub async fn check_resource_quota<S: rusternetes_storage::Storage>(
    storage: &S,
    namespace: &str,
) -> anyhow::Result<()> {
    let quota_prefix = format!("/registry/resourcequotas/{}/", namespace);
    let quotas: Vec<serde_json::Value> = storage.list(&quota_prefix).await.unwrap_or_default();
    for quota in &quotas {
        if let Some(hard) = quota.pointer("/spec/hard") {
            for limit_key in ["pods", "count/pods"] {
                if let Some(limit_str) = hard.get(limit_key).and_then(|v| v.as_str()) {
                    let limit: i64 = limit_str.parse().unwrap_or(i64::MAX);
                    let pod_prefix = format!("/registry/pods/{}/", namespace);
                    let current: Vec<serde_json::Value> =
                        storage.list(&pod_prefix).await.unwrap_or_default();
                    // Only count active pods (not Failed/Succeeded/terminating)
                    let active_count = current
                        .iter()
                        .filter(|p| {
                            let phase = p
                                .pointer("/status/phase")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            let terminating = p.pointer("/metadata/deletionTimestamp").is_some();
                            !terminating && phase != "Failed" && phase != "Succeeded"
                        })
                        .count();
                    if active_count as i64 >= limit {
                        return Err(anyhow::anyhow!(
                            "exceeded quota: {}, requested: 1, used: {}, limited: {}",
                            limit_key,
                            active_count,
                            limit_str
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Every controller's Prometheus series in text exposition format: the
/// `legacyregistry` equivalent served on the controller-manager `/metrics`.
pub fn gather_metrics() -> String {
    use prometheus::Encoder;
    let mut out = cidrset_metrics::gather();
    out.push_str(&ephemeral_volume_metrics::gather());
    out.push_str(&node_lifecycle_metrics::gather());
    out.push_str(&csi_capacity_metrics::gather());
    // Process-global `legacyregistry` (prometheus default registry): holds
    // `version_info` from `ComponentGlobalsRegistry.AddMetrics()`
    // (cmd/kube-controller-manager/app/controllermanager.go:146).
    let mut buf = Vec::new();
    prometheus::TextEncoder::new()
        .encode(&prometheus::default_registry().gather(), &mut buf)
        .expect("text encode");
    out.push_str(&String::from_utf8(buf).expect("utf8"));
    out
}

#[cfg(test)]
mod gather_metrics_tests {
    #[test]
    fn serves_version_info_from_default_registry() {
        rusternetes_common::compat_version_metrics::record_compat_version_info(
            "kube", "1.35", "1.35", "1.34",
        );
        assert!(super::gather_metrics().contains(
            "version_info{binary=\"1.35\",component=\"kube\",emulation=\"1.35\",min_compat=\"1.34\"} 1"
        ));
    }
}

/// `apierrors.IsAlreadyExists(err)` for a storage create error
/// (apimachinery/pkg/api/errors/errors.go `IsAlreadyExists`, release-1.35).
pub(crate) fn is_already_exists(e: &rusternetes_common::Error) -> bool {
    let text = e.to_string();
    text.contains("already exists") || text.contains("AlreadyExists")
}

/// [`is_already_exists`] for an `anyhow::Error` carrying a storage error
/// (`create_pod` helpers return `anyhow::Result`).
pub(crate) fn is_already_exists_anyhow(e: &anyhow::Error) -> bool {
    let text = e.to_string();
    text.contains("already exists") || text.contains("AlreadyExists")
}

#[cfg(test)]
mod error_predicate_tests {
    use super::{is_already_exists, is_already_exists_anyhow};
    use rusternetes_common::Error;

    #[test]
    fn typed_already_exists_is_matched() {
        assert!(is_already_exists(&Error::AlreadyExists("k".into())));
    }

    /// A non-AlreadyExists variant whose message merely mentions the token
    /// must not be classified benign (upstream branches on the Status reason,
    /// never on message text).
    #[test]
    fn other_variants_with_the_token_are_not_matched() {
        assert!(!is_already_exists(&Error::Storage(
            "backend: lease already exists in cache".into()
        )));
        assert!(!is_already_exists(&Error::BadRequest(
            "label AlreadyExists is reserved".into()
        )));
        assert!(!is_already_exists(&Error::Conflict(
            "resourceVersion mismatch; object already exists elsewhere".into()
        )));
    }

    #[test]
    fn anyhow_wrapped_variants_are_classified_by_variant() {
        let typed: anyhow::Error = Error::AlreadyExists("k".into()).into();
        assert!(is_already_exists_anyhow(&typed));
        let wrapped = anyhow::Error::from(Error::AlreadyExists("k".into())).context("creating pod");
        assert!(is_already_exists_anyhow(&wrapped));
        let other: anyhow::Error = Error::Storage("lease already exists in cache".into()).into();
        assert!(!is_already_exists_anyhow(&other));
        let plain = anyhow::anyhow!("pod already exists");
        assert!(!is_already_exists_anyhow(&plain));
    }

    #[test]
    fn untokened_non_matching_variants_are_not_matched() {
        assert!(!is_already_exists(&Error::NotFound("k".into())));
    }
}
