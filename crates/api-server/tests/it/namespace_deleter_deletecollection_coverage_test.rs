//! #2348: every resource type the namespace deleter walks must serve the
//! `deletecollection` verb.
//!
//! The deleter issues one collection DELETE per type and treats 404/405 as
//! "verb unsupported" (upstream `deleteCollection`,
//! `pkg/controller/namespace/deletion/namespaced_resources_deleter.go:316-348`),
//! falling back to one DELETE per object. A type that wrongly answers 404/405
//! silently loses the round-trip saving of #2203, so pin that none do. The
//! type list mirrors `NamespaceController::finalize_namespace`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;

const NS: &str = "dc-coverage";

/// Namespaced collection path for every type the
/// namespace deleter walks.
const WALKED: &[&str] = &[
    "/api/v1/namespaces/{ns}/pods",
    "/api/v1/namespaces/{ns}/replicationcontrollers",
    "/apis/apps/v1/namespaces/{ns}/replicasets",
    "/apis/apps/v1/namespaces/{ns}/deployments",
    "/apis/apps/v1/namespaces/{ns}/statefulsets",
    "/apis/apps/v1/namespaces/{ns}/daemonsets",
    "/apis/batch/v1/namespaces/{ns}/jobs",
    "/apis/batch/v1/namespaces/{ns}/cronjobs",
    "/api/v1/namespaces/{ns}/configmaps",
    "/api/v1/namespaces/{ns}/secrets",
    "/api/v1/namespaces/{ns}/serviceaccounts",
    "/api/v1/namespaces/{ns}/services",
    "/api/v1/namespaces/{ns}/endpoints",
    "/apis/discovery.k8s.io/v1/namespaces/{ns}/endpointslices",
    "/apis/networking.k8s.io/v1/namespaces/{ns}/ingresses",
    "/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies",
    "/api/v1/namespaces/{ns}/persistentvolumeclaims",
    "/apis/policy/v1/namespaces/{ns}/poddisruptionbudgets",
    "/api/v1/namespaces/{ns}/resourcequotas",
    "/api/v1/namespaces/{ns}/limitranges",
    "/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/roles",
    "/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings",
    "/api/v1/namespaces/{ns}/events",
    "/apis/events.k8s.io/v1/namespaces/{ns}/events",
    "/apis/autoscaling/v2/namespaces/{ns}/horizontalpodautoscalers",
    "/apis/coordination.k8s.io/v1/namespaces/{ns}/leases",
    "/apis/resource.k8s.io/v1/namespaces/{ns}/resourceclaims",
    "/apis/resource.k8s.io/v1/namespaces/{ns}/resourceclaimtemplates",
    "/apis/apps/v1/namespaces/{ns}/controllerrevisions",
    "/api/v1/namespaces/{ns}/podtemplates",
    "/apis/storage.k8s.io/v1/namespaces/{ns}/csistoragecapacities",
];

#[tokio::test]
async fn every_type_the_namespace_deleter_walks_serves_deletecollection() {
    let api = TestApiServer::new();
    let mut unsupported = Vec::new();
    for tmpl in WALKED {
        let path = tmpl.replace("{ns}", NS);
        let (status, _, _, body) = api.send_full("DELETE", &path, None, None, None).await;
        if status != StatusCode::OK {
            unsupported.push(format!("{path} -> {status} {body}"));
        }
    }
    assert!(
        unsupported.is_empty(),
        "collection DELETE not served:\n{}",
        unsupported.join("\n")
    );
}
