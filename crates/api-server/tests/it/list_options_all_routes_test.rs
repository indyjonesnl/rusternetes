//! Every store-backed list handler runs `ValidateListOptions` and the
//! resourceVersion floor (#2683, #2224 slice 2).
//!
//! Upstream: `ListResource` decodes `ListOptions` and rejects invalid ones
//! before touching the store:
//! `if errs := validation.ValidateListOptions(&opts, ...); len(errs) > 0 {
//! err := errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind:
//! "ListOptions"}, "", errs) ...}`
//! (staging/src/k8s.io/apiserver/pkg/endpoints/handlers/get.go, ListResource).
//! The rule set is pinned by `TestValidateListOptions`
//! (apimachinery pkg/apis/meta/internalversion/validation/validation_test.go),
//! ported in `handlers/list_options.rs`; this file pins that EVERY list route
//! applies it, not just pods/configmaps/podtemplates.

use rusternetes_test_support::harness::TestApiServer;

const LIST_URLS: &[&str] = &[
    "/api/v1/configmaps",
    "/api/v1/endpoints",
    "/api/v1/events",
    "/api/v1/limitranges",
    "/api/v1/namespaces",
    "/api/v1/namespaces/default/configmaps",
    "/api/v1/namespaces/default/endpoints",
    "/api/v1/namespaces/default/events",
    "/api/v1/namespaces/default/limitranges",
    "/api/v1/namespaces/default/persistentvolumeclaims",
    "/api/v1/namespaces/default/pods",
    "/api/v1/namespaces/default/podtemplates",
    "/api/v1/namespaces/default/replicationcontrollers",
    "/api/v1/namespaces/default/resourcequotas",
    "/api/v1/namespaces/default/secrets",
    "/api/v1/namespaces/default/serviceaccounts",
    "/api/v1/namespaces/default/services",
    "/api/v1/nodes",
    "/api/v1/persistentvolumeclaims",
    "/api/v1/persistentvolumes",
    "/api/v1/pods",
    "/api/v1/podtemplates",
    "/api/v1/replicationcontrollers",
    "/api/v1/resourcequotas",
    "/api/v1/secrets",
    "/api/v1/serviceaccounts",
    "/api/v1/services",
    "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
    "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies",
    "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings",
    "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
    "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
    "/apis/apiregistration.k8s.io/v1/apiservices",
    "/apis/apps/v1/controllerrevisions",
    "/apis/apps/v1/daemonsets",
    "/apis/apps/v1/deployments",
    "/apis/apps/v1/namespaces/default/controllerrevisions",
    "/apis/apps/v1/namespaces/default/daemonsets",
    "/apis/apps/v1/namespaces/default/deployments",
    "/apis/apps/v1/namespaces/default/replicasets",
    "/apis/apps/v1/namespaces/default/statefulsets",
    "/apis/apps/v1/replicasets",
    "/apis/apps/v1/statefulsets",
    "/apis/autoscaling/v1/horizontalpodautoscalers",
    "/apis/autoscaling/v1/namespaces/default/horizontalpodautoscalers",
    "/apis/autoscaling/v2/horizontalpodautoscalers",
    "/apis/autoscaling/v2/namespaces/default/horizontalpodautoscalers",
    "/apis/batch/v1/cronjobs",
    "/apis/batch/v1/jobs",
    "/apis/batch/v1/namespaces/default/cronjobs",
    "/apis/batch/v1/namespaces/default/jobs",
    "/apis/certificates.k8s.io/v1/certificatesigningrequests",
    "/apis/certificates.k8s.io/v1beta1/clustertrustbundles",
    "/apis/certificates.k8s.io/v1beta1/namespaces/default/podcertificaterequests",
    "/apis/certificates.k8s.io/v1beta1/podcertificaterequests",
    "/apis/coordination.k8s.io/v1/leases",
    "/apis/coordination.k8s.io/v1/namespaces/default/leases",
    "/apis/discovery.k8s.io/v1/endpointslices",
    "/apis/discovery.k8s.io/v1/namespaces/default/endpointslices",
    "/apis/events.k8s.io/v1/events",
    "/apis/events.k8s.io/v1/namespaces/default/events",
    "/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas",
    "/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations",
    "/apis/networking.k8s.io/v1/ingressclasses",
    "/apis/networking.k8s.io/v1/ingresses",
    "/apis/networking.k8s.io/v1/ipaddresses",
    "/apis/networking.k8s.io/v1/namespaces/default/ingresses",
    "/apis/networking.k8s.io/v1/namespaces/default/networkpolicies",
    "/apis/networking.k8s.io/v1/networkpolicies",
    "/apis/networking.k8s.io/v1/servicecidrs",
    "/apis/node.k8s.io/v1/runtimeclasses",
    "/apis/policy/v1/namespaces/default/poddisruptionbudgets",
    "/apis/policy/v1/poddisruptionbudgets",
    "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
    "/apis/rbac.authorization.k8s.io/v1/clusterroles",
    "/apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings",
    "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles",
    "/apis/rbac.authorization.k8s.io/v1/rolebindings",
    "/apis/rbac.authorization.k8s.io/v1/roles",
    "/apis/resource.k8s.io/v1/deviceclasses",
    "/apis/resource.k8s.io/v1/namespaces/default/resourceclaims",
    "/apis/resource.k8s.io/v1/namespaces/default/resourceclaimtemplates",
    "/apis/resource.k8s.io/v1/resourceclaims",
    "/apis/resource.k8s.io/v1/resourceclaimtemplates",
    "/apis/resource.k8s.io/v1/resourceslices",
    "/apis/scheduling.k8s.io/v1/priorityclasses",
    "/apis/snapshot.storage.k8s.io/v1/namespaces/default/volumesnapshots",
    "/apis/snapshot.storage.k8s.io/v1/volumesnapshotclasses",
    "/apis/snapshot.storage.k8s.io/v1/volumesnapshotcontents",
    "/apis/snapshot.storage.k8s.io/v1/volumesnapshots",
    "/apis/storage.k8s.io/v1/csidrivers",
    "/apis/storage.k8s.io/v1/csinodes",
    "/apis/storage.k8s.io/v1/csistoragecapacities",
    "/apis/storage.k8s.io/v1/namespaces/default/csistoragecapacities",
    "/apis/storage.k8s.io/v1/storageclasses",
    "/apis/storage.k8s.io/v1/volumeattachments",
    "/apis/storage.k8s.io/v1/volumeattributesclasses",
];

/// `resourceVersionMatch` without `resourceVersion` is `Forbidden` upstream
/// (validation.go: "resourceVersionMatch is forbidden unless resourceVersion
/// is provided"), surfaced as a 422 `Invalid` on `ListOptions`.
#[tokio::test]
async fn every_list_route_validates_list_options() {
    let server = TestApiServer::new();
    let mut bypassing = Vec::new();
    for url in LIST_URLS {
        let (status, body) = server
            .get(&format!("{url}?resourceVersionMatch=Exact"))
            .await;
        if status.as_u16() != 422 || !body.to_string().contains("resourceVersionMatch") {
            bypassing.push(format!("{url} -> {status}"));
        }
    }
    assert!(
        bypassing.is_empty(),
        "list routes that skip ValidateListOptions:\n{}",
        bypassing.join("\n")
    );
}
