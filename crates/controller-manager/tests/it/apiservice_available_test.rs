//! Integration tests for the APIService availability controller.
//!
//! Mirrors the upstream kube-aggregator remote_available_controller behaviour:
//! given an APIService that points at a backing Service, the controller must
//! set `status.conditions[type=Available]` based on whether that Service has
//! ready EndpointSlices on the requested port.
//!
//! Regression target: conformance e2e `apimachinery/aggregator.go:359`, which
//! fails because the sample-apiserver APIService never reports Available=True.

use k3rs_common::resources::endpointslice::{
    Endpoint, EndpointConditions, EndpointPort as ESEndpointPort, EndpointSlice,
};
use k3rs_controller_manager::controllers::apiservice::APIServiceAvailabilityController;
use k3rs_storage::{build_key, MemoryStorage, Storage};
use serde_json::json;
use std::sync::Arc;

fn apiservice(name: &str, svc_ns: &str, svc_name: &str, port: i64) -> serde_json::Value {
    json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": { "name": name },
        "spec": {
            "service": { "namespace": svc_ns, "name": svc_name, "port": port },
            "group": "wardle.example.com",
            "version": "v1alpha1",
            "groupPriorityMinimum": 100,
            "versionPriority": 100,
        },
    })
}

fn service(name: &str, namespace: &str, port: i64, port_name: Option<&str>) -> serde_json::Value {
    let mut p = json!({ "port": port, "targetPort": port, "protocol": "TCP" });
    if let Some(n) = port_name {
        p["name"] = json!(n);
    }
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": name, "namespace": namespace },
        "spec": { "ports": [p], "selector": { "app": name } },
    })
}

fn ready_slice(svc_ns: &str, svc_name: &str, port: i32, port_name: Option<&str>) -> EndpointSlice {
    let mut slice = EndpointSlice::new(format!("{}-abc", svc_name), "IPv4");
    slice.metadata.namespace = Some(svc_ns.to_string());
    let labels = slice.metadata.labels.get_or_insert_with(Default::default);
    labels.insert(
        "kubernetes.io/service-name".to_string(),
        svc_name.to_string(),
    );
    slice.endpoints.push(Endpoint {
        addresses: vec!["10.244.0.5".to_string()],
        conditions: Some(EndpointConditions {
            ready: Some(true),
            serving: Some(true),
            terminating: Some(false),
        }),
        hostname: None,
        target_ref: None,
        node_name: None,
        zone: None,
        hints: None,
        deprecated_topology: None,
    });
    slice.ports.push(ESEndpointPort {
        name: port_name.map(|s| s.to_string()),
        port: Some(port),
        protocol: "TCP".to_string(),
        app_protocol: None,
    });
    slice
}

async fn put<T>(storage: &Arc<MemoryStorage>, key: &str, value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    storage.create(key, value).await.expect("storage create");
}

async fn read_available(
    storage: &Arc<MemoryStorage>,
    name: &str,
) -> Option<(String, String, String)> {
    let v: serde_json::Value = storage
        .get(&build_key("apiservices", None, name))
        .await
        .expect("apiservice present");
    let conds = v.pointer("/status/conditions")?.as_array()?.clone();
    let c = conds
        .into_iter()
        .find(|c| c.get("type").and_then(|t| t.as_str()) == Some("Available"))?;
    Some((
        c.get("status")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .into(),
        c.get("reason")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .into(),
        c.get("message")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .into(),
    ))
}

#[tokio::test]
async fn ready_endpointslice_makes_apiservice_available() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = APIServiceAvailabilityController::new(storage.clone());

    let ns = "sample";
    let svc = "sample-apiserver-service";
    let api_name = "v1alpha1.wardle.example.com";

    put(
        &storage,
        &build_key("apiservices", None, api_name),
        &apiservice(api_name, ns, svc, 443),
    )
    .await;
    put(
        &storage,
        &build_key("services", Some(ns), svc),
        &service(svc, ns, 443, None),
    )
    .await;
    let slice = ready_slice(ns, svc, 443, None);
    put(
        &storage,
        &build_key("endpointslices", Some(ns), &slice.metadata.name),
        &slice,
    )
    .await;

    controller.reconcile(api_name).await.expect("reconcile ok");

    let (status, reason, _msg) = read_available(&storage, api_name)
        .await
        .expect("Available condition must be present after reconcile");
    assert_eq!(
        status, "True",
        "Available must be True when the backing service has a ready endpoint; got reason={}",
        reason
    );
}

#[tokio::test]
async fn ready_endpointslice_named_port_makes_apiservice_available() {
    // The sample-apiserver deploys with a named port ("https") — make sure the
    // controller correlates the Service port name with the EndpointSlice port
    // name rather than requiring identical numeric ports on both sides.
    let storage = Arc::new(MemoryStorage::new());
    let controller = APIServiceAvailabilityController::new(storage.clone());

    let ns = "wardle";
    let svc = "sample-apiserver-service";
    let api_name = "v1alpha1.wardle.example.com";

    put(
        &storage,
        &build_key("apiservices", None, api_name),
        &apiservice(api_name, ns, svc, 443),
    )
    .await;
    put(
        &storage,
        &build_key("services", Some(ns), svc),
        &service(svc, ns, 443, Some("https")),
    )
    .await;
    // EndpointSlice port carries the target port (8443) but the same port name
    // ("https"), mirroring what the EndpointSlice controller produces.
    let slice = ready_slice(ns, svc, 8443, Some("https"));
    put(
        &storage,
        &build_key("endpointslices", Some(ns), &slice.metadata.name),
        &slice,
    )
    .await;

    controller.reconcile(api_name).await.expect("reconcile ok");

    let (status, reason, _msg) = read_available(&storage, api_name)
        .await
        .expect("Available condition must be present after reconcile");
    assert_eq!(
        status, "True",
        "Available must be True for named-port match; got reason={}",
        reason
    );
}

#[tokio::test]
async fn local_apiservice_is_always_available() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = APIServiceAvailabilityController::new(storage.clone());

    let api_name = "v1.apps";
    let mut local = apiservice(api_name, "", "", 0);
    // Remove the spec.service field — a local APIService has no backing service.
    local["spec"]
        .as_object_mut()
        .unwrap()
        .remove("service")
        .unwrap();

    put(&storage, &build_key("apiservices", None, api_name), &local).await;

    controller.reconcile(api_name).await.expect("reconcile ok");

    let (status, _reason, _msg) = read_available(&storage, api_name)
        .await
        .expect("Available condition must be set for local APIService");
    assert_eq!(status, "True");
}

#[tokio::test]
async fn transitions_from_existing_true_to_false_when_endpoints_missing() {
    // The api-server's create_apiservice handler pre-seeds every new APIService
    // with `Available=True / reason=Passed`, regardless of whether the backing
    // Service actually has endpoints. The controller MUST be willing to flip
    // that condition back to False once it observes no endpoints — otherwise
    // the conformance aggregator test trusts a lie at create time and never
    // sees a real readiness signal.
    let storage = Arc::new(MemoryStorage::new());
    let controller = APIServiceAvailabilityController::new(storage.clone());

    let ns = "wardle";
    let svc = "sample-api";
    let api_name = "v1alpha1.wardle.example.com";

    // Pre-seed with the bogus Available=True condition the api-server hands out
    // at creation time.
    let mut api = apiservice(api_name, ns, svc, 7443);
    api["status"] = json!({
        "conditions": [{
            "type": "Available",
            "status": "True",
            "lastTransitionTime": "1970-01-01T00:00:00Z",
            "reason": "Passed",
            "message": "API service is available",
        }]
    });
    put(&storage, &build_key("apiservices", None, api_name), &api).await;
    // Service exists, but no EndpointSlice / Endpoints yet — pod is still
    // pending. The aggregator must report Available=False.
    put(
        &storage,
        &build_key("services", Some(ns), svc),
        &service(svc, ns, 7443, None),
    )
    .await;

    controller.reconcile(api_name).await.expect("reconcile ok");

    let (status, reason, _msg) = read_available(&storage, api_name)
        .await
        .expect("Available condition must remain present after reconcile");
    assert_eq!(
        status, "False",
        "Available must flip to False when no endpoints exist; got reason={}",
        reason
    );
    assert_eq!(reason, "EndpointsNotFound");
}

#[tokio::test]
async fn transitions_from_false_to_true_when_endpoints_become_ready() {
    // The realistic aggregator-conformance lifecycle: the APIService is
    // created before the sample-apiserver pod is ready. The controller marks
    // it False/EndpointsNotFound. Once endpoints become ready, the next
    // reconcile must flip the condition to True/Passed.
    let storage = Arc::new(MemoryStorage::new());
    let controller = APIServiceAvailabilityController::new(storage.clone());

    let ns = "wardle";
    let svc = "sample-api";
    let api_name = "v1alpha1.wardle.example.com";

    put(
        &storage,
        &build_key("apiservices", None, api_name),
        &apiservice(api_name, ns, svc, 7443),
    )
    .await;
    put(
        &storage,
        &build_key("services", Some(ns), svc),
        &service(svc, ns, 7443, None),
    )
    .await;

    // First reconcile: no endpoints → False
    controller
        .reconcile(api_name)
        .await
        .expect("first reconcile");
    let (status_first, _, _) = read_available(&storage, api_name)
        .await
        .expect("condition present after first reconcile");
    assert_eq!(
        status_first, "False",
        "expected False before endpoints exist"
    );

    // Pod comes up; EndpointSlice now has a ready endpoint.
    let slice = ready_slice(ns, svc, 443, None);
    put(
        &storage,
        &build_key("endpointslices", Some(ns), &slice.metadata.name),
        &slice,
    )
    .await;

    // Second reconcile: ready endpoint → True
    controller
        .reconcile(api_name)
        .await
        .expect("second reconcile");
    let (status_after, reason_after, _) = read_available(&storage, api_name)
        .await
        .expect("condition present after second reconcile");
    assert_eq!(
        status_after, "True",
        "Available must transition to True once endpoints are ready; got reason={}",
        reason_after
    );
    assert_eq!(reason_after, "Passed");
}

#[tokio::test]
async fn preserves_last_transition_time_when_status_unchanged() {
    // K8s convention: lastTransitionTime only advances when `status` flips.
    // A reason/message-only refresh must retain the prior transition time so
    // observers can tell how long the condition has held its current value.
    let storage = Arc::new(MemoryStorage::new());
    let controller = APIServiceAvailabilityController::new(storage.clone());

    let ns = "wardle";
    let svc = "sample-api";
    let api_name = "v1alpha1.wardle.example.com";

    let original_transition = "2020-01-01T00:00:00Z";
    let mut api = apiservice(api_name, ns, svc, 7443);
    api["status"] = json!({
        "conditions": [{
            "type": "Available",
            "status": "False",
            "lastTransitionTime": original_transition,
            "reason": "EndpointsNotFound",
            "message": "cannot find endpointslices for service/old in \"old\"",
        }]
    });
    put(&storage, &build_key("apiservices", None, api_name), &api).await;
    put(
        &storage,
        &build_key("services", Some(ns), svc),
        &service(svc, ns, 7443, None),
    )
    .await;

    controller.reconcile(api_name).await.expect("reconcile ok");

    // Status stayed False so the transition time must not advance, even
    // though the message was refreshed.
    let v: serde_json::Value = storage
        .get(&build_key("apiservices", None, api_name))
        .await
        .expect("apiservice present");
    let conds = v.pointer("/status/conditions").unwrap().as_array().unwrap();
    let c = conds
        .iter()
        .find(|c| c.get("type").and_then(|v| v.as_str()) == Some("Available"))
        .unwrap();
    assert_eq!(c.get("status").and_then(|v| v.as_str()), Some("False"));
    assert_eq!(
        c.get("lastTransitionTime").and_then(|v| v.as_str()),
        Some(original_transition),
        "lastTransitionTime must be preserved when status does not change"
    );
}

#[tokio::test]
async fn refreshes_stale_message_even_when_status_and_reason_match() {
    // Regression: the update-skip fast path only compared `type/status/reason`,
    // so an Available condition whose `message` had gone stale (e.g. it still
    // referenced an old service name) was silently retained. Conformance
    // assertions read `message` directly to diagnose failures, so it must
    // track the live state.
    let storage = Arc::new(MemoryStorage::new());
    let controller = APIServiceAvailabilityController::new(storage.clone());

    let ns = "wardle";
    let svc = "sample-api";
    let api_name = "v1alpha1.wardle.example.com";

    let mut api = apiservice(api_name, ns, svc, 7443);
    // Existing condition: False/EndpointsNotFound, but the message references
    // an *old* service name. The reconcile must rewrite the message.
    api["status"] = json!({
        "conditions": [{
            "type": "Available",
            "status": "False",
            "lastTransitionTime": "1970-01-01T00:00:00Z",
            "reason": "EndpointsNotFound",
            "message": "cannot find endpointslices for service/old-name in \"old-ns\"",
        }]
    });
    put(&storage, &build_key("apiservices", None, api_name), &api).await;
    put(
        &storage,
        &build_key("services", Some(ns), svc),
        &service(svc, ns, 7443, None),
    )
    .await;

    controller.reconcile(api_name).await.expect("reconcile ok");

    let (status, reason, message) = read_available(&storage, api_name)
        .await
        .expect("Available condition must be present");
    assert_eq!(status, "False");
    assert_eq!(reason, "EndpointsNotFound");
    assert!(
        message.contains(svc) && message.contains(ns),
        "stale message must be refreshed to reflect current service/namespace; got {:?}",
        message
    );
}

#[tokio::test]
async fn missing_service_reports_not_available() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = APIServiceAvailabilityController::new(storage.clone());

    let api_name = "v1alpha1.wardle.example.com";
    put(
        &storage,
        &build_key("apiservices", None, api_name),
        &apiservice(api_name, "wardle", "missing", 443),
    )
    .await;

    controller.reconcile(api_name).await.expect("reconcile ok");

    let (status, reason, _msg) = read_available(&storage, api_name)
        .await
        .expect("Available condition must be present");
    assert_eq!(status, "False");
    assert_eq!(reason, "ServiceNotFound");
}

// remote_available_controller.go:277-352: after the service and endpoint
// checks, `attempts` concurrent discovery requests; one success is overall
// success, otherwise the last result is the FailedDiscoveryCheck message.

use k3rs_controller_manager::controllers::apiservice::DiscoveryProbe;
use std::sync::atomic::{AtomicUsize, Ordering};

async fn available_setup(
    storage: &Arc<MemoryStorage>,
    api_name: &str,
) -> (&'static str, &'static str) {
    let (ns, svc) = ("sample", "sample-apiserver-service");
    put(
        storage,
        &build_key("apiservices", None, api_name),
        &apiservice(api_name, ns, svc, 443),
    )
    .await;
    put(
        storage,
        &build_key("services", Some(ns), svc),
        &service(svc, ns, 443, None),
    )
    .await;
    let slice = ready_slice(ns, svc, 443, None);
    put(
        storage,
        &build_key("endpointslices", Some(ns), &slice.metadata.name),
        &slice,
    )
    .await;
    (ns, svc)
}

fn probe_with(
    calls: Arc<AtomicUsize>,
    f: impl Fn(usize) -> Result<(), String> + Send + Sync + 'static,
) -> DiscoveryProbe {
    let f = Arc::new(f);
    Arc::new(move |_apiservice| {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        let f = f.clone();
        Box::pin(async move { f(n) })
    })
}

#[tokio::test]
async fn failed_discovery_check_marks_unavailable_and_requeues() {
    let storage = Arc::new(MemoryStorage::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let controller = APIServiceAvailabilityController::with_discovery_probe(
        storage.clone(),
        probe_with(calls.clone(), |_| Err("bad status from x: 503".to_string())),
    );
    let api_name = "v1alpha1.wardle.example.com";
    available_setup(&storage, api_name).await;

    let err = controller
        .reconcile(api_name)
        .await
        .expect_err("a failed discovery check returns the error so the key is requeued");
    assert_eq!(err.to_string(), "bad status from x: 503");
    assert_eq!(calls.load(Ordering::SeqCst), 5, "five attempts");

    let (status, reason, message) = read_available(&storage, api_name).await.unwrap();
    assert_eq!(status, "False");
    assert_eq!(reason, "FailedDiscoveryCheck");
    assert_eq!(message, "bad status from x: 503");
}

#[tokio::test]
async fn one_successful_discovery_attempt_is_overall_success() {
    let storage = Arc::new(MemoryStorage::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let controller = APIServiceAvailabilityController::with_discovery_probe(
        storage.clone(),
        probe_with(calls, |n| {
            if n == 3 {
                Ok(())
            } else {
                Err("failing or missing response".to_string())
            }
        }),
    );
    let api_name = "v1alpha1.wardle.example.com";
    available_setup(&storage, api_name).await;

    controller.reconcile(api_name).await.expect("reconcile ok");

    let (status, reason, message) = read_available(&storage, api_name).await.unwrap();
    assert_eq!(status, "True");
    assert_eq!(reason, "Passed");
    assert_eq!(message, "all checks passed");
}

#[tokio::test]
async fn discovery_is_not_attempted_when_endpoints_are_missing() {
    let storage = Arc::new(MemoryStorage::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let controller = APIServiceAvailabilityController::with_discovery_probe(
        storage.clone(),
        probe_with(calls.clone(), |_| Ok(())),
    );
    let (ns, svc, api_name) = (
        "sample",
        "sample-apiserver-service",
        "v1alpha1.wardle.example.com",
    );
    put(
        &storage,
        &build_key("apiservices", None, api_name),
        &apiservice(api_name, ns, svc, 443),
    )
    .await;
    put(
        &storage,
        &build_key("services", Some(ns), svc),
        &service(svc, ns, 443, None),
    )
    .await;

    controller.reconcile(api_name).await.expect("reconcile ok");

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let (_, reason, _) = read_available(&storage, api_name).await.unwrap();
    assert_eq!(reason, "EndpointsNotFound");
}

// `if service.Spec.Type == v1.ServiceTypeClusterIP` (:231): the port and
// endpoint checks apply to cluster IP services only.
#[tokio::test]
async fn non_cluster_ip_service_skips_port_and_endpoint_checks() {
    let storage = Arc::new(MemoryStorage::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let controller = APIServiceAvailabilityController::with_discovery_probe(
        storage.clone(),
        probe_with(calls.clone(), |_| Ok(())),
    );
    let (ns, svc, api_name) = ("sample", "external", "v1alpha1.wardle.example.com");
    put(
        &storage,
        &build_key("apiservices", None, api_name),
        &apiservice(api_name, ns, svc, 443),
    )
    .await;
    put(
        &storage,
        &build_key("services", Some(ns), svc),
        &json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": { "name": svc, "namespace": ns },
            "spec": { "type": "ExternalName", "externalName": "backend.example.com" },
        }),
    )
    .await;

    controller.reconcile(api_name).await.expect("reconcile ok");

    assert_eq!(
        calls.load(Ordering::SeqCst),
        5,
        "every attempt is launched; the first success ends the wait"
    );
    let (status, reason, _) = read_available(&storage, api_name).await.unwrap();
    assert_eq!((status.as_str(), reason.as_str()), ("True", "Passed"));
}

// Upstream reads EndpointSlices only (remote_available_controller.go:259-265):
// a legacy Endpoints object alone does not make the APIService available.
#[tokio::test]
async fn legacy_endpoints_without_a_slice_are_not_enough() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = APIServiceAvailabilityController::new(storage.clone());
    let (ns, svc, api_name) = (
        "sample",
        "sample-apiserver-service",
        "v1alpha1.wardle.example.com",
    );
    put(
        &storage,
        &build_key("apiservices", None, api_name),
        &apiservice(api_name, ns, svc, 443),
    )
    .await;
    put(
        &storage,
        &build_key("services", Some(ns), svc),
        &service(svc, ns, 443, None),
    )
    .await;
    put(
        &storage,
        &build_key("endpoints", Some(ns), svc),
        &json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": { "name": svc, "namespace": ns },
            "subsets": [{
                "addresses": [{ "ip": "10.244.0.5" }],
                "ports": [{ "port": 443 }],
            }],
        }),
    )
    .await;

    controller.reconcile(api_name).await.expect("reconcile ok");

    let (status, reason, _) = read_available(&storage, api_name).await.unwrap();
    assert_eq!(
        (status.as_str(), reason.as_str()),
        ("False", "EndpointsNotFound")
    );
}
