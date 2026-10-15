//! `WithStepMetrics` around the admission chain
//! (`staging/src/k8s.io/apiserver/pkg/server/options/admission.go:179`) and
//! `WithControllerMetrics` around ResourceQuota
//! (`admission/metrics/metrics.go:63-71`, `plugins.go:142-144`).

use std::collections::HashMap;
use std::sync::Arc;

use rusternetes_api_server::endpoints::handlers::admission::Admission;
use rusternetes_api_server::registry::rest::RequestContext;
use rusternetes_api_server::state::ApiServerState;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
use rusternetes_common::auth::{TokenManager, UserInfo};
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_common::resources::ConfigMap;
use rusternetes_storage::{MemoryStorage, StorageBackend};
use serde_json::json;

fn state() -> ApiServerState {
    ApiServerState::new(
        Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new()))),
        Arc::new(TokenManager::new(b"secret")),
        Arc::new(AlwaysAllowAuthorizer),
        Arc::new(MetricsRegistry::new()),
        true,
    )
}

/// Sample count of the histogram series `name{labels}`, 0 if unobserved.
fn hist_count(name: &str, labels: &[(&str, &str)]) -> u64 {
    prometheus::gather()
        .into_iter()
        .find(|f| f.name() == name)
        .and_then(|mf| {
            mf.get_metric()
                .iter()
                .find(|m| {
                    labels.iter().all(|(k, v)| {
                        m.get_label()
                            .iter()
                            .any(|l| l.name() == *k && l.value() == *v)
                    })
                })
                .map(|m| m.get_histogram().get_sample_count())
        })
        .unwrap_or(0)
}

fn step(ty: &str, rejected: &str) -> u64 {
    hist_count(
        "apiserver_admission_step_admission_duration_seconds",
        &[
            ("type", ty),
            ("operation", "CREATE"),
            ("rejected", rejected),
        ],
    )
}

fn quota(rejected: &str) -> u64 {
    hist_count(
        "apiserver_admission_controller_admission_duration_seconds",
        &[
            ("name", "ResourceQuota"),
            ("type", "validate"),
            ("operation", "CREATE"),
            ("rejected", rejected),
        ],
    )
}

#[tokio::test]
async fn admit_and_validate_observe_the_step_metrics() {
    let state = state();
    let kind = GroupVersionKind {
        group: String::new(),
        version: "v1".into(),
        kind: "ConfigMap".into(),
    };
    let resource = GroupVersionResource {
        group: String::new(),
        version: "v1".into(),
        resource: "configmaps".into(),
    };
    let user = UserInfo {
        username: "u".into(),
        uid: String::new(),
        groups: vec![],
        extra: HashMap::new(),
    };
    let admission = Admission {
        state: &state,
        kind: &kind,
        resource: &resource,
        subresource: None,
        namespace: Some("default"),
        user: &user,
        dry_run: false,
        forbid_create: false,
    };
    let cm: ConfigMap = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "cm", "namespace": "default"},
    }))
    .unwrap();
    let (admit0, validate0, quota0) = (
        step("admit", "false"),
        step("validate", "false"),
        quota("false"),
    );

    let cm = admission.admit(Operation::Create, cm, None).await.unwrap();
    assert!(step("admit", "false") > admit0, "admit step not observed");

    let ctx = RequestContext::new(Some("default")).with_user(&user);
    admission
        .validate(&ctx, Operation::Create, Some(&cm), None)
        .await
        .unwrap();
    assert!(
        step("validate", "false") > validate0,
        "validate step not observed"
    );
    assert!(quota("false") > quota0, "ResourceQuota plugin not observed");
}

// --- in-tree plugin controller metrics (#3521) ---------------------------
//
// `WithControllerMetrics` decorates EVERY plugin
// (apiserver/pkg/server/options/admission.go:90, admission/plugins.go:142-144)
// and the chain calls a plugin whenever `Handles(op)` is true
// (admission/chain.go:33-35, :53-55), whatever the resource: a plugin that
// has nothing to do for the object is still observed.

fn plugin_series(name: &str, ty: &str, op: &str, rejected: &str) -> u64 {
    hist_count(
        "apiserver_admission_controller_admission_duration_seconds",
        &[
            ("name", name),
            ("type", ty),
            ("operation", op),
            ("rejected", rejected),
        ],
    )
}

fn cm_admission<'a>(
    state: &'a ApiServerState,
    kind: &'a GroupVersionKind,
    resource: &'a GroupVersionResource,
    user: &'a UserInfo,
) -> Admission<'a> {
    Admission {
        state,
        kind,
        resource,
        subresource: None,
        namespace: Some("default"),
        user,
        dry_run: false,
    }
}

fn cm_kind() -> (GroupVersionKind, GroupVersionResource, UserInfo, ConfigMap) {
    (
        GroupVersionKind {
            group: String::new(),
            version: "v1".into(),
            kind: "ConfigMap".into(),
        },
        GroupVersionResource {
            group: String::new(),
            version: "v1".into(),
            resource: "configmaps".into(),
        },
        UserInfo {
            username: "u".into(),
            uid: String::new(),
            groups: vec![],
            extra: HashMap::new(),
        },
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "cm", "namespace": "default"},
        }))
        .unwrap(),
    )
}

/// plugins.go:69-100 + each plugin's `NewHandler(...)` and interface
/// assertions: which plugin is observed under which step on a CREATE.
const CREATE_ADMIT: &[&str] = &[
    "NodeRestriction",
    "LimitRanger",
    "ServiceAccount",
    "Priority",
    "DefaultTolerationSeconds",
    "RuntimeClass",
    "DefaultStorageClass",
    "StorageObjectInUseProtection",
    "PodTopologyLabels",
];
const CREATE_VALIDATE: &[&str] = &[
    "PodSecurity",
    "Priority",
    "LimitRanger",
    "CertificateSubjectRestriction",
    "ClusterTrustBundleAttest",
];

#[tokio::test]
async fn create_observes_every_handling_in_tree_plugin() {
    let state = state();
    let (kind, resource, user, cm) = cm_kind();
    let admission = cm_admission(&state, &kind, &resource, &user);
    let before_a: Vec<u64> = CREATE_ADMIT
        .iter()
        .map(|p| plugin_series(p, "admit", "CREATE", "false"))
        .collect();
    let before_v: Vec<u64> = CREATE_VALIDATE
        .iter()
        .map(|p| plugin_series(p, "validate", "CREATE", "false"))
        .collect();

    let cm = admission.admit(Operation::Create, cm, None).await.unwrap();
    let ctx = RequestContext::new(Some("default")).with_user(&user);
    admission
        .validate(&ctx, Operation::Create, Some(&cm), None)
        .await
        .unwrap();

    for (p, b) in CREATE_ADMIT.iter().zip(before_a) {
        assert!(
            plugin_series(p, "admit", "CREATE", "false") > b,
            "{p} admit not observed"
        );
    }
    for (p, b) in CREATE_VALIDATE.iter().zip(before_v) {
        assert!(
            plugin_series(p, "validate", "CREATE", "false") > b,
            "{p} validate not observed"
        );
    }
}

/// CertificateApproval, CertificateSigning and NodeDeclaredFeatureValidator
/// register for UPDATE only (approval/admission.go:67, signing/admission.go:70,
/// nodedeclaredfeatures/admission.go:78); ServiceAccount for CREATE and UPDATE
/// (:102); RuntimeClass and DefaultStorageClass for CREATE only (:137, :62).
#[tokio::test]
async fn update_observes_only_the_plugins_that_handle_update() {
    let state = state();
    let (kind, resource, user, cm) = cm_kind();
    let admission = cm_admission(&state, &kind, &resource, &user);
    let upd = [
        "CertificateApproval",
        "CertificateSigning",
        "NodeDeclaredFeatureValidator",
    ];
    let before: Vec<u64> = upd
        .iter()
        .map(|p| plugin_series(p, "validate", "UPDATE", "false"))
        .collect();
    let sa0 = plugin_series("ServiceAccount", "admit", "UPDATE", "false");
    let rc0 = plugin_series("RuntimeClass", "admit", "UPDATE", "false");
    let dsc0 = plugin_series("DefaultStorageClass", "admit", "UPDATE", "false");

    let old = cm.clone();
    let cm = admission
        .admit(Operation::Update, cm, Some(&old))
        .await
        .unwrap();
    let ctx = RequestContext::new(Some("default")).with_user(&user);
    admission
        .validate(&ctx, Operation::Update, Some(&cm), Some(&old))
        .await
        .unwrap();

    for (p, b) in upd.iter().zip(before) {
        assert!(
            plugin_series(p, "validate", "UPDATE", "false") > b,
            "{p} validate not observed"
        );
    }
    assert!(plugin_series("ServiceAccount", "admit", "UPDATE", "false") > sa0);
    assert_eq!(
        plugin_series("RuntimeClass", "admit", "UPDATE", "false"),
        rc0
    );
    assert_eq!(
        plugin_series("DefaultStorageClass", "admit", "UPDATE", "false"),
        dsc0
    );
}

/// DELETE: Priority (C,U,D) validates; NodeRestriction (C,U,D) is an
/// Admit-only plugin, so it is observed under `admit`; LimitRanger (C,U) is not
/// called.
#[tokio::test]
async fn delete_observes_priority_and_node_restriction_only() {
    let state = state();
    let (kind, resource, user, cm) = cm_kind();
    let admission = cm_admission(&state, &kind, &resource, &user);
    let pr0 = plugin_series("Priority", "validate", "DELETE", "false");
    let nr0 = plugin_series("NodeRestriction", "admit", "DELETE", "false");
    let lr0 = plugin_series("LimitRanger", "validate", "DELETE", "false");

    let ctx = RequestContext::new(Some("default")).with_user(&user);
    admission
        .validate::<ConfigMap>(&ctx, Operation::Delete, None, Some(&cm))
        .await
        .unwrap();

    assert!(plugin_series("Priority", "validate", "DELETE", "false") > pr0);
    assert!(plugin_series("NodeRestriction", "admit", "DELETE", "false") > nr0);
    assert_eq!(
        plugin_series("LimitRanger", "validate", "DELETE", "false"),
        lr0
    );
}

/// `rejected` is `err != nil` (metrics.go:97-99): a Pod naming a missing
/// PriorityClass is rejected by Priority's `Admit`.
#[tokio::test]
async fn rejecting_plugin_is_observed_as_rejected() {
    let state = state();
    let (_, _, user, _) = cm_kind();
    let kind = GroupVersionKind {
        group: String::new(),
        version: "v1".into(),
        kind: "Pod".into(),
    };
    let resource = GroupVersionResource {
        group: String::new(),
        version: "v1".into(),
        resource: "pods".into(),
    };
    let admission = cm_admission(&state, &kind, &resource, &user);
    let pod: rusternetes_common::resources::Pod = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p", "namespace": "default"},
        "spec": {"priorityClassName": "does-not-exist",
                 "containers": [{"name": "c", "image": "busybox"}]},
    }))
    .unwrap();
    let bad0 = plugin_series("Priority", "admit", "CREATE", "true");
    let ok0 = plugin_series("LimitRanger", "admit", "CREATE", "false");

    assert!(admission.admit(Operation::Create, pod, None).await.is_err());

    assert!(plugin_series("Priority", "admit", "CREATE", "true") > bad0);
    // LimitRanger precedes Priority in the chain, so it ran and succeeded.
    assert!(plugin_series("LimitRanger", "admit", "CREATE", "false") > ok0);
}
