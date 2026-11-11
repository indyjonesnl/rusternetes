//! `plugins::install` decides which plugins the chain decorates with
//! `WithControllerMetrics` (`NewFromPlugins`, apiserver/pkg/admission/
//! plugins.go:142-144, over `enabledPluginNames()`, server/options/
//! admission.go:136-182). `NodeRestriction` is in `DefaultOffAdmissionPlugins`
//! (pkg/kubeapiserver/options/plugins.go:131-135), so its Admit on a pod DELETE
//! (noderestriction/admission.go:72, :257-270) is observed only once enabled.
//!
//! `install` writes a process-global: run under nextest (one process per
//! test); the test restores the default set before returning.

use std::collections::HashMap;
use std::sync::Arc;

use k3rs_api_server::admission::plugins::{install, AdmissionOptions};
use k3rs_api_server::endpoints::handlers::admission::Admission;
use k3rs_api_server::state::ApiServerState;
use k3rs_common::admission::{GroupVersionKind, GroupVersionResource};
use k3rs_common::auth::{TokenManager, UserInfo};
use k3rs_common::authz::AlwaysAllowAuthorizer;
use k3rs_common::observability::MetricsRegistry;
use k3rs_common::resources::Pod;
use k3rs_storage::{MemoryStorage, StorageBackend};
use serde_json::json;

fn node_restriction_delete_admit() -> u64 {
    prometheus::gather()
        .into_iter()
        .find(|f| f.name() == "apiserver_admission_controller_admission_duration_seconds")
        .and_then(|mf| {
            mf.get_metric()
                .iter()
                .find(|m| {
                    [
                        ("name", "NodeRestriction"),
                        ("type", "admit"),
                        ("operation", "DELETE"),
                        ("rejected", "false"),
                    ]
                    .iter()
                    .all(|(k, v)| {
                        m.get_label()
                            .iter()
                            .any(|l| l.name() == *k && l.value() == *v)
                    })
                })
                .map(|m| m.get_histogram().get_sample_count())
        })
        .unwrap_or(0)
}

#[tokio::test]
async fn node_restriction_delete_is_observed_only_when_enabled() {
    let state = ApiServerState::new(
        Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new()))),
        Arc::new(TokenManager::new(b"secret")),
        Arc::new(AlwaysAllowAuthorizer),
        Arc::new(MetricsRegistry::new()),
        true,
    );
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
    let pod: Pod = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]},
    }))
    .unwrap();

    // Default set: NodeRestriction is off, so the chain never calls it.
    install(&AdmissionOptions::kube_apiserver());
    let before = node_restriction_delete_admit();
    admission.admit_delete(&pod).await.unwrap();
    assert_eq!(node_restriction_delete_admit(), before);

    // --enable-admission-plugins=NodeRestriction: now decorated and called.
    let mut options = AdmissionOptions::kube_apiserver();
    options.enable_plugins = Some(vec!["NodeRestriction".to_string()]);
    install(&options);
    admission.admit_delete(&pod).await.unwrap();
    assert_eq!(node_restriction_delete_admit(), before + 1);

    install(&AdmissionOptions::kube_apiserver());
}
