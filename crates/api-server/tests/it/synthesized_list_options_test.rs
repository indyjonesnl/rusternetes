//! #2814: synthesized list endpoints (metrics.k8s.io, componentstatuses,
//! custom/external metrics) must run `ValidateListOptions` like every other
//! list.
//!
//! Upstream: `ListResource`
//! (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/get.go`) decodes
//! `ListOptions` and runs `ValidateListOptions`
//! (`apimachinery/pkg/apis/meta/internalversion/validation/validation.go:28-51`)
//! for every list, rejecting `resourceVersionMatch` without `resourceVersion`
//! with 422 Invalid. No resourceVersion floor: these are not store reads.

use rusternetes_test_support::harness::TestApiServer;

#[tokio::test]
async fn synthesized_lists_reject_invalid_list_options() {
    let api = TestApiServer::new();
    for base in [
        "/apis/metrics.k8s.io/v1beta1/nodes",
        "/apis/metrics.k8s.io/v1beta1/pods",
        "/apis/metrics.k8s.io/v1beta1/namespaces/default/pods",
        "/api/v1/componentstatuses",
        "/apis/custom.metrics.k8s.io/v1beta2/namespaces/default/pods/qps",
        "/apis/external.metrics.k8s.io/v1beta1/namespaces/default/qps",
    ] {
        let uri = format!("{base}?resourceVersionMatch=Exact");
        let (s, b) = api.get(&uri).await;
        assert_eq!(s.as_u16(), 422, "{uri}: {b}");
        assert_eq!(b["reason"], "Invalid", "{uri}: {b}");
        let (s, b) = api.get(base).await;
        assert!(s.is_success(), "{base} without options: {s} {b}");
    }
}
