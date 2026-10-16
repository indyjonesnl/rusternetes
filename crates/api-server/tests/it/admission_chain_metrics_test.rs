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
