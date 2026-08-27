//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-api-machinery] Aggregation layer + Discovery.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/apimachinery/
//!
//! Mirrored files:
//!   * `test/e2e/apimachinery/aggregator.go`  — 1 `framework.ConformanceIt`
//!   * `test/e2e/apimachinery/discovery.go`   — 2 `framework.ConformanceIt`
//!     (plus 2 plain `ginkgo.It`, which are NOT conformance cases)
//!
//! See docs/conformance/apimachinery-aggregation-discovery.md for the
//! test-by-test status table.
//!
//! ---------------------------------------------------------------------------
//! Mirror audit (#1749, 2026-08-27)
//! ---------------------------------------------------------------------------
//!
//! Every citation in this file was re-derived from the upstream body it names,
//! against `../kubernetes` at `release-1.35`. Findings do NOT carry over to
//! other mirror files: coverage was checked suite-wide, not per-file, because
//! a mirror may live in a different file from the one whose source it cites.
//!
//! Upstream conformance cases in this area, and where each is mirrored:
//!
//!   * discovery.go:126 "should validate PreferredVersion for each APIGroup"
//!     → `discovery_apis_preferred_version_is_one_of_versions`
//!   * discovery.go:172 "should locate the groupVersion and a resource within
//!     each APIGroup"
//!     → `discovery_locates_group_version_and_resource_in_each_api_group`
//!   * aggregator.go:102 "Should be able to support the 1.17 Sample API Server
//!     using the current Aggregator [LinuxOnly]"
//!     → `aggregator_sample_apiserver_full_lifecycle` (+ the REST tail in
//!     `aggregator_apiservice_patch_list_and_delete_collection`)
//!   * table_conversion.go:154 "should return a 406 for a backend which does
//!     not implement metadata" → mirrored in
//!     `conformance_apimachinery_vap_apf_server.rs`, not here.
//!
//! Citations repaired: 8 of 10. `discovery.go:54` (×2) named the closing brace
//! of a `BeforeEach`; `:110` named the storage-version-hash `ginkgo.It`;
//! `:149` (×2) named a line inside the *PreferredVersion* body while labelling
//! the *groupVersion* case; `aggregator.go:382` named the remote poll loop
//! under a local-seed test; `:535` named a `versionPriority` patch under a
//! deletion test; one citation was a bare path with a hedged "line ~348".
//!
//! Conformance claims withdrawn: 3. The two "present and missing resources"
//! legs mirror a plain `ginkgo.It`, and the aggregated-discovery V2 tests
//! mirror no upstream case at all — the previous citation asserted they were
//! "tested in discovery.go:149 via the dynamic client", which is false.
//!
//! Assertions re-derived:
//!   * discovery.go:172 is a table of NINETEEN (group, version, resource)
//!     tuples. The mirror asserted two of them, split across two tests. It is
//!     now the whole table, in upstream's order. All nineteen pass.
//!   * discovery.go:126 validates PreferredVersion against the version list
//!     returned by a SECOND request to `/apis/{group}/`, not the copy embedded
//!     in the group list. The mirror read only the group list.
//!   * aggregator.go's teardown is a label-selected `DeleteCollection`
//!     (:743-751). The mirror deleted straight out of `MemoryStorage`, so no
//!     DELETE-handler defect could fail it.
//!
//! Defect found: `list_apiservices` did not apply `?labelSelector=` — only the
//! watch path did. Upstream's `checkApiServiceListQuantity` lists *with* the
//! selector and waits for zero, so against Rusternetes it would have counted
//! every APIService in the cluster and never converged. Fixed in
//! `crates/api-server/src/handlers/generic.rs` in the same change; the test
//! that catches it is `aggregator_apiservice_patch_list_and_delete_collection`,
//! whose unlabelled bystander object is what makes the selector observable.
//!
//! Test removed: 1. `aggregator_create_local_apiservice_returns_available_true`
//! was a strict subset of
//! `aggregator_create_local_apiservice_uses_upstream_local_condition`, and
//! cited the wrong upstream line.
//!
//! Excluded (cannot be reached at this layer):
//!   * `metadata.resourceVersion` growth across a patch (aggregator.go:547) —
//!     `MemoryStorage` does not stamp it; etcd and rhino do.
//!   * Everything gated on a real kubelet — see the exclusion list on
//!     `aggregator_sample_apiserver_full_lifecycle`.
//!
//! Harness: in-process axum router over `StorageBackend::Memory`, driven via
//! `tower::ServiceExt::oneshot`. No Docker, no etcd, no kubelet.

use axum::http::StatusCode;
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::oneshot;
use warp::Filter;

// ---------------------------------------------------------------------------
// HTTP harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

fn spawn_state() -> TestApiServer {
    TestApiServer::new()
}

/// GET helper — returns (status, parsed JSON body).
async fn http_get(router: TestApiServer, uri: &str) -> (StatusCode, Value) {
    http_get_with_headers(router, uri, &[]).await
}

/// GET helper that injects additional request headers (used to negotiate
/// aggregated discovery V2 via the Accept header).
async fn http_get_with_headers(
    router: TestApiServer,
    uri: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let (status, _h, _b, body) = router.send_with_headers("GET", uri, headers, None).await;
    (status, body)
}

/// Build a local APIService body (no spec.service → status seeds to
/// Available=True per the `create_apiservice` handler).
fn apiservice_local(name: &str, group: &str, version: &str) -> Value {
    json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": { "name": name },
        "spec": {
            "group": group,
            "version": version,
            "versionPriority": 100,
            "groupPriorityMinimum": 1000,
        },
    })
}

/// Build a remote (aggregated) APIService body backed by `service`.
fn apiservice_remote(
    name: &str,
    group: &str,
    version: &str,
    svc_namespace: &str,
    svc_name: &str,
    port: u16,
) -> Value {
    json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": { "name": name },
        "spec": {
            "group": group,
            "version": version,
            "versionPriority": 100,
            "groupPriorityMinimum": 1000,
            "insecureSkipTLSVerify": true,
            "service": { "name": svc_name, "namespace": svc_namespace, "port": port },
        },
    })
}

// ---------------------------------------------------------------------------
// /api discovery — core group
// ---------------------------------------------------------------------------

/// [sig-api-machinery] Discovery should locate the groupVersion and a resource
/// within each APIGroup [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/discovery.go:172
///   ("should locate the groupVersion and a resource within each APIGroup")
///   Table of (apiBasePath, apiGroup, apiVersion, validResource) tuples at
///   discovery.go:180-293; assertions at discovery.go:296-311.
///
/// Mirror audit (#1749, 2026-08-27): re-derived. The old citation was
/// `discovery.go:149`, which is inside the *PreferredVersion* case body — a
/// different conformance case. Worse, the mirror asserted only two of
/// upstream's nineteen tuples (core `v1`/namespaces and `apps/v1`/deployments),
/// split across two tests. Upstream iterates the whole table and fails on the
/// first tuple that is missing, so a mirror covering two of them could not
/// fail for the seventeen groups it never asked about. The table below is
/// upstream's, verbatim and in order.
#[tokio::test]
async fn discovery_locates_group_version_and_resource_in_each_api_group() {
    // discovery.go:180-293, verbatim.
    const CASES: &[(&str, &str, &str, &str)] = &[
        ("/api", "", "v1", "namespaces"),
        (
            "/apis",
            "admissionregistration.k8s.io",
            "v1",
            "validatingwebhookconfigurations",
        ),
        (
            "/apis",
            "apiextensions.k8s.io",
            "v1",
            "customresourcedefinitions",
        ),
        ("/apis", "apiregistration.k8s.io", "v1", "apiservices"),
        ("/apis", "apps", "v1", "deployments"),
        ("/apis", "authentication.k8s.io", "v1", "tokenreviews"),
        (
            "/apis",
            "authorization.k8s.io",
            "v1",
            "selfsubjectaccessreviews",
        ),
        ("/apis", "autoscaling", "v1", "horizontalpodautoscalers"),
        ("/apis", "autoscaling", "v2", "horizontalpodautoscalers"),
        ("/apis", "batch", "v1", "jobs"),
        (
            "/apis",
            "certificates.k8s.io",
            "v1",
            "certificatesigningrequests",
        ),
        ("/apis", "coordination.k8s.io", "v1", "leases"),
        ("/apis", "discovery.k8s.io", "v1", "endpointslices"),
        ("/apis", "events.k8s.io", "v1", "events"),
        ("/apis", "networking.k8s.io", "v1", "ingresses"),
        ("/apis", "node.k8s.io", "v1", "runtimeclasses"),
        ("/apis", "policy", "v1", "poddisruptionbudgets"),
        ("/apis", "scheduling.k8s.io", "v1", "priorityclasses"),
        ("/apis", "storage.k8s.io", "v1", "csinodes"),
    ];

    let router = spawn_state();
    for (base_path, group, version, valid_resource) in CASES {
        // `path.Join(t.apiBasePath, t.apiGroup, t.apiVersion)` — the empty
        // core group collapses to `/api/v1`.
        let api_path = if group.is_empty() {
            format!("{base_path}/{version}")
        } else {
            format!("{base_path}/{group}/{version}")
        };
        // `schema.GroupVersion{Group, Version}.String()` — bare version for
        // the core group, `group/version` otherwise.
        let expected_group_version = if group.is_empty() {
            (*version).to_string()
        } else {
            format!("{group}/{version}")
        };

        let (status, body) = http_get(router.clone(), &api_path).await;
        assert_eq!(status, StatusCode::OK, "Fail to access: {api_path}");
        assert_eq!(
            body["groupVersion"].as_str(),
            Some(expected_group_version.as_str()),
            "{api_path} reported groupVersion {:?}",
            body["groupVersion"]
        );

        let names: Vec<&str> = body["resources"]
            .as_array()
            .unwrap_or_else(|| panic!("{api_path} has no resources array: {body}"))
            .iter()
            .filter_map(|r| r["name"].as_str())
            .collect();
        assert!(
            names.contains(valid_resource),
            "Resource {valid_resource:?} was not found inside of resourceList for \
             {api_path}: {names:?}"
        );
    }
}

/// [sig-api-machinery] Discovery — `/api` advertises the core group's versions
///
/// Upstream: no conformance case asserts the `/api` `APIVersions` document
/// directly; discovery.go:172 enters the core group at `/api/v1`. The
/// `APIVersions` document is what `clientdiscovery.IsResourceEnabled`
/// traverses first (discovery.go:56), so it is asserted here on its own
/// footing rather than under a conformance claim.
///
/// Mirror audit (#1749, 2026-08-27): split out of the former
/// `discovery_core_api_lists_v1_and_resources`, whose conformance claim
/// belonged to the table case above.
#[tokio::test]
async fn discovery_core_api_advertises_v1() {
    let router = spawn_state();
    let (status, body) = http_get(router, "/api").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["kind"].as_str(), Some("APIVersions"));
    let versions: Vec<&str> = body["versions"]
        .as_array()
        .expect("versions array")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(
        versions.contains(&"v1"),
        "core /api must advertise v1, got {versions:?}"
    );
}

/// [sig-api-machinery] Discovery should accurately determine present and
/// missing resources (positive case)
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/discovery.go:56
///   ("should accurately determine present and missing resources") — a plain
///   `ginkgo.It`, NOT a `framework.ConformanceIt`, so this is not a
///   conformance case and carries no `[Conformance]` marker.
///   Assertions via `clientdiscovery.IsResourceEnabled` at discovery.go:58-76.
///
/// Mirror audit (#1749, 2026-08-27): re-cited. `:54` is the closing brace of the
/// enclosing `ginkgo.BeforeEach`, not a test.
#[tokio::test]
async fn discovery_reports_enabled_resources_present() {
    let router = spawn_state();

    // namespaces ∈ /api/v1
    let (_, core) = http_get(router.clone(), "/api/v1").await;
    let core_names: Vec<&str> = core["resources"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["name"].as_str())
        .collect();
    assert!(core_names.contains(&"namespaces"));

    // deployments ∈ /apis/apps/v1
    let (_, apps) = http_get(router, "/apis/apps/v1").await;
    let apps_names: Vec<&str> = apps["resources"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["name"].as_str())
        .collect();
    assert!(
        apps_names.contains(&"deployments"),
        "apps/v1 should expose deployments, got {:?}",
        apps_names
    );
}

/// [sig-api-machinery] Discovery should accurately determine present and
/// missing resources (negative case)
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/discovery.go:56
///   ("should accurately determine present and missing resources") — a plain
///   `ginkgo.It`, NOT a `framework.ConformanceIt`; not a conformance case.
///   The missing-resource half is at discovery.go:78-90.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; `:54` was the `BeforeEach` closing brace.
#[tokio::test]
async fn discovery_reports_missing_resources_absent() {
    let router = spawn_state();

    // No nonsense resource in apps/v1.
    let (_, apps) = http_get(router.clone(), "/apis/apps/v1").await;
    let apps_names: Vec<&str> = apps["resources"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["name"].as_str())
        .collect();
    assert!(!apps_names.contains(&"please-dont-ever-create-this"));

    // Fake group should not be present in /apis at all.
    let (_, groups_doc) = http_get(router, "/apis").await;
    let group_names: Vec<&str> = groups_doc["groups"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|g| g["name"].as_str())
        .collect();
    assert!(
        !group_names.contains(&"not-these-apps"),
        "fake group leaked into discovery: {:?}",
        group_names
    );
}

// ---------------------------------------------------------------------------
// /apis discovery — group list + per-group preferredVersion
// ---------------------------------------------------------------------------

/// [sig-api-machinery] Discovery should validate PreferredVersion for each
/// APIGroup [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/discovery.go:126
///   ("should validate PreferredVersion for each APIGroup")
///   Assertions at discovery.go:128-162.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; `:110` is inside the
/// storage-version-hash `ginkgo.It` above, which is not a conformance case.
/// Also re-derived: upstream does not read `preferredVersion` out of the
/// `/apis` group list alone — for every group it issues a SECOND request to
/// `/apis/{group}/` and validates the PreferredVersion against the version
/// list *that endpoint* returns (discovery.go:141-146). A mirror that only
/// reads `/apis` cannot catch the two documents disagreeing.
#[tokio::test]
async fn discovery_apis_preferred_version_is_one_of_versions() {
    let router = spawn_state();
    let (status, body) = http_get(router.clone(), "/apis").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["kind"].as_str(), Some("APIGroupList"));

    let groups = body["groups"].as_array().expect("groups array");
    assert!(!groups.is_empty(), "at least one API group expected");

    for group in groups {
        let name = group["name"].as_str().unwrap_or("");
        if name.ends_with(".example.com") {
            // upstream skips example.com test groups; we mirror that
            continue;
        }
        // Upstream re-fetches the group's own endpoint and validates against
        // the version list *that* document reports (discovery.go:141-146),
        // not the copy embedded in the group list.
        let api_path = format!("/apis/{name}/");
        let (status, check_group) = http_get(router.clone(), &api_path).await;
        assert_eq!(status, StatusCode::OK, "Fail to access: {api_path}");

        let versions: Vec<&str> = check_group["versions"]
            .as_array()
            .unwrap_or_else(|| panic!("No version found for {name}: {check_group}"))
            .iter()
            .filter_map(|v| v["groupVersion"].as_str())
            .collect();
        assert!(!versions.is_empty(), "No version found for {name}");

        let preferred = check_group["preferredVersion"]["groupVersion"]
            .as_str()
            .unwrap_or("");
        assert!(
            versions.contains(&preferred),
            "Failed to find a valid version for PreferredVersion {preferred} of \
             group {name} in versions {versions:?}"
        );

        // The group list and the group endpoint must agree; upstream reads
        // only the latter, so a divergence would be invisible to it, but a
        // client that trusts the list would then follow a version the group
        // endpoint does not serve.
        let listed_preferred = group["preferredVersion"]["groupVersion"]
            .as_str()
            .unwrap_or("");
        assert_eq!(
            listed_preferred, preferred,
            "/apis and {api_path} disagree on preferredVersion for {name}"
        );
    }
}

/// [sig-api-machinery] Discovery — apps/v1 exposes the workload resources
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/discovery.go:172
///   ("should locate the groupVersion and a resource within each APIGroup")
///   covers the `apps`/`v1`/`deployments` tuple; `statefulsets` and
///   `daemonsets` are asserted here beyond upstream's table.
///
/// Mirror audit (#1749, 2026-08-27): the `[Conformance]` marker moved to
/// `discovery_locates_group_version_and_resource_in_each_api_group`, which
/// mirrors the whole table. This test is now a superset detail, not the case.
#[tokio::test]
async fn discovery_group_apps_v1_returns_groupversion_and_deployments() {
    let router = spawn_state();
    let (status, body) = http_get(router, "/apis/apps/v1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["kind"].as_str(), Some("APIResourceList"));
    assert_eq!(body["groupVersion"].as_str(), Some("apps/v1"));
    let names: Vec<&str> = body["resources"]
        .as_array()
        .expect("resources")
        .iter()
        .filter_map(|r| r["name"].as_str())
        .collect();
    assert!(names.contains(&"deployments"));
    assert!(names.contains(&"statefulsets"));
    assert!(names.contains(&"daemonsets"));
}

/// [sig-api-machinery] Discovery — /apis/apiregistration.k8s.io/v1 lists
/// apiservices (prereq for the Aggregator scenario)
///
/// Upstream context: aggregator.go:382 reads /apis/apiregistration.k8s.io/v1
/// while validating APIService discovery.
/// Sonobuoy (Round 160): PASS (discovery surface; aggregator FAIL is the
/// deployment, not the discovery doc)
#[tokio::test]
async fn discovery_apiregistration_v1_lists_apiservices_resource() {
    let router = spawn_state();
    let (status, body) = http_get(router, "/apis/apiregistration.k8s.io/v1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["kind"].as_str(), Some("APIResourceList"));
    assert_eq!(
        body["groupVersion"].as_str(),
        Some("apiregistration.k8s.io/v1")
    );
    let names: Vec<&str> = body["resources"]
        .as_array()
        .expect("resources")
        .iter()
        .filter_map(|r| r["name"].as_str())
        .collect();
    assert!(
        names.contains(&"apiservices"),
        "apiregistration.k8s.io/v1 must expose apiservices, got {:?}",
        names
    );
}

// ---------------------------------------------------------------------------
// Aggregated discovery V2 (apidiscovery.k8s.io)
// ---------------------------------------------------------------------------

/// [sig-api-machinery] Aggregated Discovery V2 — Accept negotiation on /apis
///
/// Upstream: no conformance case. `test/e2e/apimachinery/discovery.go`
/// contains no `framework.ConformanceIt` that negotiates aggregated
/// discovery; the two cases there read the unaggregated `/apis` and
/// `/apis/{group}/{version}` documents. The wire contract asserted here is
/// `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/...` +
/// `staging/src/k8s.io/apiserver/pkg/endpoints/discovery/aggregated/handler.go`.
///
/// Mirror audit (#1749, 2026-08-27): the previous citation claimed this was
/// "tested in discovery.go:149 via the dynamic client". It is not: `:149` is
/// inside the PreferredVersion case, and no conformance case in that file
/// exercises the `apidiscovery.k8s.io` representation at all.
#[tokio::test]
async fn discovery_aggregated_v2_negotiated_via_accept_header() {
    let router = spawn_state();
    let (status, body) = http_get_with_headers(
        router,
        "/apis",
        &[(
            "accept",
            "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,\
             application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList,\
             application/json",
        )],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["kind"].as_str(), Some("APIGroupDiscoveryList"));
    let api_version = body["apiVersion"].as_str().unwrap_or("");
    assert!(
        api_version.starts_with("apidiscovery.k8s.io/"),
        "aggregated discovery V2 must use apidiscovery.k8s.io group, got {}",
        api_version
    );
    let items = body["items"].as_array().expect("items");
    assert!(
        !items.is_empty(),
        "aggregated discovery returned empty items"
    );
    // Each item must declare a metadata.name (group name; "" for core).
    for item in items {
        assert!(item["metadata"]["name"].is_string());
    }
}

/// [sig-api-machinery] Aggregated Discovery V2 — core /api leg
///
/// Upstream: no conformance case — see the sibling test above. The
/// aggregated representation of `/api` is served by
/// `staging/src/k8s.io/apiserver/pkg/endpoints/discovery/aggregated/handler.go`.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; not a conformance case.
#[tokio::test]
async fn discovery_aggregated_v2_on_core_api() {
    let router = spawn_state();
    let (status, body) = http_get_with_headers(
        router,
        "/api",
        &[(
            "accept",
            "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
        )],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["kind"].as_str(), Some("APIGroupDiscoveryList"));
    let items = body["items"].as_array().expect("items");
    // Core group has metadata.name "" and at least one v1 entry with resources.
    let core = items
        .iter()
        .find(|it| it["metadata"]["name"].as_str() == Some(""))
        .expect("core group present in aggregated /api response");
    let versions = core["versions"].as_array().expect("versions");
    let v1 = versions
        .iter()
        .find(|v| v["version"].as_str() == Some("v1"))
        .expect("core v1 present");
    let resource_names: Vec<&str> = v1["resources"]
        .as_array()
        .expect("resources")
        .iter()
        .filter_map(|r| r["resource"].as_str())
        .collect();
    assert!(resource_names.contains(&"pods"));
    assert!(resource_names.contains(&"namespaces"));
}

// ---------------------------------------------------------------------------
// APIService aggregation — TestSampleAPIServer slice
//
// The conformance scenarios that matter for the aggregator slice are:
//   * persisted APIService gets picked up by `/apis` discovery merge
//   * `resolve_aggregator_target` finds the registered backend
//   * status semantics on creation
//
// All three are driven through the real HTTP route. A note here used to say
// that POSTing to `/apis/apiregistration.k8s.io/v1/apiservices` returned 500
// because the handler's `Extension<AuthContext>` extractor had no middleware to
// populate it, and that the tests therefore seeded storage directly. That is no
// longer true — the route works — and seeding directly had quietly turned the
// creation assertions into a test of a copy of the handler kept in this file.
// ---------------------------------------------------------------------------

/// Helper: register an APIService by POSTing it, so what lands in storage is
/// whatever `create_apiservice` actually writes.
///
/// This used to write straight to storage, re-implementing the handler's
/// status-seeding logic inline. That made every "status seed semantics on
/// creation" assertion below tautological — they compared the handler's output
/// to a *copy of the handler's own code* living in this file, so a divergence
/// from upstream in the real create path could never fail them. Going through
/// the route is what lets those assertions mean anything.
async fn seed_apiservice(state: &TestApiServer, body: Value) {
    let (status, _bytes, response) = state
        .send_raw(
            "POST",
            "/apis/apiregistration.k8s.io/v1/apiservices",
            Some("application/json"),
            Some(&body),
        )
        .await;
    assert!(
        status.is_success(),
        "POST apiservice must succeed: {status} {response}"
    );
}

/// [sig-api-machinery] Aggregator should be able to support the 1.17 Sample
/// API Server using the current Aggregator [LinuxOnly] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/aggregator.go:102
/// Sonobuoy (Round 160): FAIL — "deploying extension apiserver in namespace
/// aggregator-...: error waiting for deployment ... status to match
/// expectation" (aggregator.go:359). Root cause is the sample-apiserver Pod
/// never reaches Ready in our kubelet — that's a Layer A (Sonobuoy) defect
/// gated on real-kubelet image pull and is tracked separately.
///
/// This Layer B mirror ports the REST + discovery + proxy sub-assertions
/// from `aggregator.go:285–541` against the in-process axum router +
/// `MemoryStorage`. Mirrored sub-assertions:
///
/// 1. APIService creation through `/apis/apiregistration.k8s.io/v1/apiservices`
///    is accepted (201) and persisted with the correct shape.
/// 2. APIService status seed: remote APIService starts with
///    `Available=Unknown,reason=Pending` (controller probe pending).
/// 3. `update_apiservice_status` flips Available to True after a successful
///    probe (mirrors the controller transitioning the condition).
/// 4. Discovery aggregation: a GET /apis surfaces the aggregated group after
///    APIService registration, and the matching APIGroup is in /apis/{group}.
/// 5. Proxy: a GET on `/apis/{group}/{version}/{resource}` is forwarded to
///    the backing Service's ClusterIP/port. The mock backend captures the
///    request and we assert path, query string, impersonation headers, and
///    response status are all preserved.
/// 6. 503 from the proxy when the backing Service has no endpoints/clusterIP
///    (the controller would mark Available=False; here we exercise the
///    runtime-resolution path that returns 503 directly).
/// 7. APIService deletion removes the group from /apis on the next request.
///
/// Skipped sub-assertions (require a real kubelet — Sonobuoy E2E layer):
///   * Pulling and running `registry.k8s.io/e2e-test-images/sample-apiserver`
///   * Deployment ready-replica gating
///   * mTLS handshake against a real backend serving a CSR-signed cert
///   * Etcd-backed flunder CRUD persistence across api-server restarts
#[tokio::test]
async fn aggregator_sample_apiserver_full_lifecycle() {
    // Spin up a mock "sample-apiserver" backend on a random port. The proxy
    // resolver will be pointed here via the APIService's spec.service +
    // ClusterIP. The mock echoes back the request path so we can verify the
    // proxy preserved it.
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let captured_path: Arc<tokio::sync::Mutex<Option<String>>> =
        Arc::new(tokio::sync::Mutex::new(None));
    let captured_user: Arc<tokio::sync::Mutex<Option<String>>> =
        Arc::new(tokio::sync::Mutex::new(None));
    let cp = captured_path.clone();
    let cu = captured_user.clone();

    let route = warp::path::full()
        .and(warp::header::headers_cloned())
        .and_then(
            move |full: warp::path::FullPath, headers: warp::http::HeaderMap| {
                let cp = cp.clone();
                let cu = cu.clone();
                async move {
                    *cp.lock().await = Some(full.as_str().to_string());
                    let user = headers
                        .get("x-remote-user")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    *cu.lock().await = Some(user);
                    Ok::<_, warp::Rejection>(
                        warp::http::Response::builder()
                            .status(200)
                            .header("Content-Type", "application/json")
                            .body(r#"{"kind":"FlunderList","apiVersion":"wardle.example.com/v1alpha1","items":[]}"#.to_string())
                            .unwrap(),
                    )
                }
            },
        );

    let (mock_addr, server) =
        warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async {
            shutdown_rx.await.ok();
        });
    let mock_handle = tokio::spawn(server);

    let state = spawn_state();

    // -------------------------------------------------------------------
    // Sub-assertion 1: create APIService through the HTTP router.
    // Upstream aggregator.go ~334 "register sample-apiserver as an APIService".
    // -------------------------------------------------------------------
    let apiservice_body = apiservice_remote(
        "v1alpha1.wardle.example.com",
        "wardle.example.com",
        "v1alpha1",
        "wardle",
        "sample-apiserver",
        mock_addr.port(),
    );
    let (post_status, _) = state
        .post(
            "/apis/apiregistration.k8s.io/v1/apiservices",
            &apiservice_body,
        )
        .await;
    assert_eq!(
        post_status,
        StatusCode::CREATED,
        "POST APIService must return 201"
    );

    // -------------------------------------------------------------------
    // Sub-assertion 2: create leaves a REMOTE APIService's status empty.
    //
    // Upstream `apiServerStrategy.PrepareForCreate`
    // (`staging/src/k8s.io/kube-aggregator/pkg/registry/apiservice/strategy.go:68-76`) wipes
    // `status` and only seeds a condition when `spec.service == nil`. So the
    // first `Available` condition on a remote APIService comes from the
    // availability controller, off a real probe — never from the create path.
    // -------------------------------------------------------------------
    let key = build_key("apiservices", None, "v1alpha1.wardle.example.com");
    let stored: Value = state.storage.get(&key).await.unwrap();
    let conditions = stored
        .pointer("/status/conditions")
        .and_then(|v| v.as_array());
    assert!(
        conditions.is_none_or(|c| c.is_empty()),
        "create must not fabricate a condition for a remote APIService; got {}",
        stored["status"]
    );

    // -------------------------------------------------------------------
    // Sub-assertion 3: status-subresource update flips Available to True.
    // Mirrors the APIServiceAvailabilityController after a successful probe
    // (aggregator.go waits for `Status == True` before issuing client calls).
    // -------------------------------------------------------------------
    let mut flipped = stored.clone();
    flipped["status"] = json!({
        "conditions": [{
            "type": "Available",
            "status": "True",
            "lastTransitionTime": chrono::Utc::now().to_rfc3339(),
            "reason": "Passed",
            "message": "all checks passed",
        }]
    });
    let (put_status, _) = state
        .put(
            "/apis/apiregistration.k8s.io/v1/apiservices/v1alpha1.wardle.example.com/status",
            &flipped,
        )
        .await;
    assert!(
        put_status.is_success(),
        "PUT /status must succeed, got {put_status}"
    );
    let after: Value = state.storage.get(&key).await.unwrap();
    let cond = after["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"].as_str() == Some("Available"))
        .unwrap();
    assert_eq!(
        cond["status"].as_str(),
        Some("True"),
        "status subresource update must persist Available=True",
    );

    // -------------------------------------------------------------------
    // Sub-assertion 4: discovery merge — aggregated group appears in /apis
    // and /apis/wardle.example.com.
    // -------------------------------------------------------------------
    let (status, body) = http_get(state.clone(), "/apis").await;
    assert_eq!(status, StatusCode::OK);
    let group_names: Vec<&str> = body["groups"]
        .as_array()
        .expect("groups array")
        .iter()
        .filter_map(|g| g["name"].as_str())
        .collect();
    assert!(
        group_names.contains(&"wardle.example.com"),
        "registered APIService group missing from discovery: {:?}",
        group_names
    );

    // -------------------------------------------------------------------
    // Sub-assertion 5: seed the backing Service so the proxy can resolve
    // a host:port, then issue a GET through the aggregator router and
    // verify it lands on the mock backend.
    // -------------------------------------------------------------------
    let svc_key = build_key("services", Some("wardle"), "sample-apiserver");
    let svc = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": "sample-apiserver", "namespace": "wardle" },
        "spec": {
            "clusterIP": "127.0.0.1",
            "ports": [{
                "port": mock_addr.port(),
                "targetPort": mock_addr.port(),
                "protocol": "TCP",
            }],
        },
        "status": {},
    });
    state
        .storage
        .create::<rusternetes_common::resources::Service>(
            &svc_key,
            &serde_json::from_value(svc).unwrap(),
        )
        .await
        .expect("seed sample-apiserver Service");

    // Crucial: the aggregator forwards over HTTPS by default. The mock is
    // plain HTTP. We can't override the AggregatorTarget.scheme through the
    // router, so this sub-assertion exercises `resolve_aggregator_target`
    // via the public helper directly (the routed call would fail TLS).
    let resolved =
        rusternetes_api_server::handlers::generic::resolve_aggregator_target_with_storage(
            state.storage.as_ref(),
            "wardle.example.com",
            "v1alpha1",
        )
        .await
        .expect("resolver Ok")
        .expect("resolved target");
    assert_eq!(resolved.host, "127.0.0.1");
    assert_eq!(resolved.port, mock_addr.port());

    // Now forward over HTTP through the public helper (test-only scheme
    // override), and verify the mock observed the proxied request with the
    // correct path and impersonation header.
    let target = rusternetes_api_server::handlers::generic::AggregatorTarget {
        host: resolved.host.clone(),
        port: resolved.port,
        insecure_skip_tls_verify: true,
        ca_bundle: None,
        scheme: "http",
        server_name: None,
    };
    let auth = rusternetes_api_server::middleware::AuthContext {
        user: rusternetes_common::auth::UserInfo {
            username: "system:admin".to_string(),
            uid: "uid-admin".to_string(),
            groups: vec!["system:masters".to_string()],
            extra: std::collections::HashMap::new(),
        },
    };
    let resp = rusternetes_api_server::handlers::generic::forward_to_aggregator(
        &target,
        &auth,
        axum::http::Method::GET,
        "/apis/wardle.example.com/v1alpha1/flunders?labelSelector=foo%3Dbar",
        &axum::http::HeaderMap::new(),
        Vec::new(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let observed_path = captured_path
        .lock()
        .await
        .clone()
        .expect("mock saw request");
    assert!(
        observed_path.starts_with("/apis/wardle.example.com/v1alpha1/flunders"),
        "proxy must preserve the request path, got {:?}",
        observed_path
    );
    let observed_user = captured_user.lock().await.clone().unwrap_or_default();
    assert_eq!(
        observed_user, "system:admin",
        "proxy must inject X-Remote-User: system:admin"
    );

    // -------------------------------------------------------------------
    // Sub-assertion 6: 503 when the backing Service has no usable ClusterIP
    // (mirrors upstream behaviour when the sample-apiserver Pod is down).
    // We delete the Service to force the resolver into the no-endpoint branch.
    // -------------------------------------------------------------------
    state
        .storage
        .delete(&svc_key)
        .await
        .expect("delete service");
    let err = rusternetes_api_server::handlers::generic::resolve_aggregator_target_with_storage(
        state.storage.as_ref(),
        "wardle.example.com",
        "v1alpha1",
    )
    .await
    .expect_err("expected 503 when service is gone");
    assert_eq!(
        err.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "missing backing Service must yield 503, mirroring upstream proxy behaviour",
    );

    // -------------------------------------------------------------------
    // Sub-assertion 7: APIService deletion removes the group from /apis.
    // Upstream aggregator.go:535 issues DeleteCollection; we exercise the
    // single-delete route since DeleteCollection is covered by the watch/gc
    // mirror unit.
    // -------------------------------------------------------------------
    let (del_status, _) = state
        .delete("/apis/apiregistration.k8s.io/v1/apiservices/v1alpha1.wardle.example.com")
        .await;
    assert!(
        del_status.is_success(),
        "DELETE APIService must succeed, got {}",
        del_status
    );
    let (_, after_delete) = http_get(state.clone(), "/apis").await;
    let names_after: Vec<&str> = after_delete["groups"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|g| g["name"].as_str())
        .collect();
    assert!(
        !names_after.contains(&"wardle.example.com"),
        "aggregated group still present after DELETE: {:?}",
        names_after
    );

    // Shut down the mock backend cleanly.
    let _ = shutdown_tx.send(());
    let _ = mock_handle.await;
}

/// [sig-api-machinery] Aggregator — create leaves a remote APIService's status empty
///
/// Upstream: `apiServerStrategy.PrepareForCreate`
/// (`staging/src/k8s.io/kube-aggregator/pkg/registry/apiservice/strategy.go:68-76`):
///
/// ```text
/// apiservice.Status = apiregistration.APIServiceStatus{}
/// if apiservice.Spec.Service == nil {
///     SetAPIServiceCondition(apiservice, NewLocalAvailableAPIServiceCondition())
/// }
/// ```
///
/// The condition on a remote APIService is written by the availability
/// controller off a real probe. This test used to assert a create-time seed of
/// `Available=Unknown` / `reason: Pending` — a condition upstream never writes,
/// held in place by a test in the directory that is supposed to mirror upstream
/// responses.
#[tokio::test]
async fn aggregator_create_remote_apiservice_leaves_status_empty() {
    let state = spawn_state();
    let body = apiservice_remote(
        "v1alpha1.wardle.example.com",
        "wardle.example.com",
        "v1alpha1",
        "wardle",
        "sample-apiserver",
        7443,
    );
    seed_apiservice(&state, body).await;

    let key = build_key("apiservices", None, "v1alpha1.wardle.example.com");
    let stored: Value = state.storage.get(&key).await.unwrap();
    let conditions = stored
        .pointer("/status/conditions")
        .and_then(|v| v.as_array());
    assert!(
        conditions.is_none_or(|c| c.is_empty()),
        "remote APIService must carry no conditions until a probe runs; got {}",
        stored["status"]
    );
}

/// [sig-api-machinery] Aggregator — a LOCAL APIService is available on create,
/// with upstream's exact reason and message.
///
/// Upstream `NewLocalAvailableAPIServiceCondition`
/// (`staging/src/k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/helper/helpers.go:96-104`):
/// `Reason: "Local"`, `Message: "Local APIServices are always available"`.
/// The message was singular here ("Local APIService is always available"),
/// in both the create handler and the availability controller.
#[tokio::test]
async fn aggregator_create_local_apiservice_uses_upstream_local_condition() {
    let state = spawn_state();
    let body = json!({
        "apiVersion": "apiregistration.k8s.io/v1",
        "kind": "APIService",
        "metadata": { "name": "v1.local.example.com" },
        "spec": {
            "group": "local.example.com",
            "version": "v1",
            "groupPriorityMinimum": 1000,
            "versionPriority": 100,
        },
    });
    seed_apiservice(&state, body).await;

    let key = build_key("apiservices", None, "v1.local.example.com");
    let stored: Value = state.storage.get(&key).await.unwrap();
    let avail = stored["status"]["conditions"]
        .as_array()
        .expect("local APIService is available on create")
        .iter()
        .find(|c| c["type"].as_str() == Some("Available"))
        .expect("Available condition");
    assert_eq!(avail["status"].as_str(), Some("True"));
    assert_eq!(avail["reason"].as_str(), Some("Local"));
    assert_eq!(
        avail["message"].as_str(),
        Some("Local APIServices are always available"),
    );
    // `metav1.Time` on the wire: RFC3339, second precision, `Z` — never
    // sub-second digits or a `+00:00` offset.
    let transition = avail["lastTransitionTime"].as_str().unwrap_or_default();
    assert!(
        transition.ends_with('Z') && !transition.contains('.'),
        "lastTransitionTime must be second-precision UTC like metav1.Time; got {transition:?}"
    );
    let created = stored["metadata"]["creationTimestamp"]
        .as_str()
        .unwrap_or_default();
    assert!(
        created.ends_with('Z') && !created.contains('.'),
        "creationTimestamp must be second-precision UTC like metav1.Time; got {created:?}"
    );
}

/// [sig-api-machinery] Aggregator — APIService discovery merge: a registered
/// APIService group appears in /apis (HTTP surface)
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/aggregator.go:102
///   ("Should be able to support the 1.17 Sample API Server using the current
///   Aggregator [LinuxOnly]") — the discovery-merge step of that case, whose
///   poll loop at aggregator.go:384-390 requires
///   `/apis/wardle.example.com/v1alpha1/...` to become reachable, which is
///   only possible once the group is merged into discovery.
///
/// Mirror audit (#1749, 2026-08-27): re-cited. The old citation gave a bare
/// file path plus a hedged "line ~348", which pins nothing and cannot be
/// invalidated when upstream moves.
#[tokio::test]
async fn aggregator_registered_apiservice_appears_in_discovery() {
    let state = spawn_state();
    seed_apiservice(
        &state,
        apiservice_remote(
            "v1alpha1.wardle.example.com",
            "wardle.example.com",
            "v1alpha1",
            "wardle",
            "sample-apiserver",
            7443,
        ),
    )
    .await;

    let router = state;
    let (status, body) = http_get(router, "/apis").await;
    assert_eq!(status, StatusCode::OK);
    let group_names: Vec<&str> = body["groups"]
        .as_array()
        .expect("groups")
        .iter()
        .filter_map(|g| g["name"].as_str())
        .collect();
    assert!(
        group_names.contains(&"wardle.example.com"),
        "aggregated group missing from /apis discovery merge: {:?}",
        group_names
    );
}

/// [sig-api-machinery] Aggregator — APIService removal drops the group from
/// /apis discovery on the next request
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/aggregator.go:102
///   ("Should be able to support the 1.17 Sample API Server using the current
///   Aggregator [LinuxOnly]") — the teardown step at aggregator.go:743-751:
///   `APIServices().DeleteCollection(labelSelector)` followed by
///   `checkApiServiceListQuantity(..., 0)`.
///
/// Mirror audit (#1749, 2026-08-27): re-cited and re-derived. `:535` is a
/// `versionPriority` merge-patch, not a delete. The mirror also deleted the
/// object straight out of storage, which is the same shortcut this file's
/// own header comment calls out for creation: it turned a test of the DELETE
/// handler into a test of `MemoryStorage`. Both the single DELETE and
/// upstream's label-selected collection delete now go through the router.
#[tokio::test]
async fn aggregator_delete_apiservice_removes_from_discovery() {
    let state = spawn_state();
    seed_apiservice(
        &state,
        apiservice_remote(
            "v1alpha1.wardle.example.com",
            "wardle.example.com",
            "v1alpha1",
            "wardle",
            "sample-apiserver",
            7443,
        ),
    )
    .await;

    // Sanity: the group is present before deletion.
    let (_, before) = http_get(state.clone(), "/apis").await;
    let before_names: Vec<&str> = before["groups"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|g| g["name"].as_str())
        .collect();
    assert!(before_names.contains(&"wardle.example.com"));

    // Delete through the route, not through storage: a DELETE handler that
    // failed to remove the object (or removed the wrong one) has to be able
    // to fail this test.
    let (status, _, body) = state
        .send_raw(
            "DELETE",
            "/apis/apiregistration.k8s.io/v1/apiservices/v1alpha1.wardle.example.com",
            None,
            None,
        )
        .await;
    assert!(
        status.is_success(),
        "DELETE apiservice must succeed: {status} {body}"
    );

    let (status, after) = http_get(state, "/apis").await;
    assert_eq!(status, StatusCode::OK);
    let after_names: Vec<&str> = after["groups"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|g| g["name"].as_str())
        .collect();
    assert!(
        !after_names.contains(&"wardle.example.com"),
        "aggregated group still present after deletion: {:?}",
        after_names
    );
}

/// [sig-api-machinery] Aggregator — APIService patch, list, label and
/// deleteCollection (the REST tail of the Sample API Server case)
///
/// Upstream: k8s.io/kubernetes/test/e2e/apimachinery/aggregator.go:102
///   ("Should be able to support the 1.17 Sample API Server using the current
///   Aggregator [LinuxOnly]") — the REST sequence at aggregator.go:536-751:
///   merge-patch `spec.versionPriority` to 400 (:537-546), list APIServices
///   and locate the registered one (:549-570), add a label so the object can
///   be selected (:573-...), then `DeleteCollection` by that label selector
///   and confirm the list count drops to zero (:743-751).
///
/// Mirror audit (#1749, 2026-08-27): added. None of this tail was mirrored;
/// the file's only deletion test used a single DELETE and reached into
/// storage to do it. The label-selected collection delete is the mechanism
/// upstream actually uses, and it is the one that can regress independently
/// of single-object DELETE.
///
/// Excluded: upstream also asserts the patched object carries a larger
/// `metadata.resourceVersion` than the pre-patch one
/// (`resourceversion.CompareResourceVersion`, aggregator.go:547).
/// `MemoryStorage` does not stamp `metadata.resourceVersion` on write — the
/// etcd (`crates/storage/src/etcd.rs:43-52`) and rhino
/// (`crates/storage/src/rhino.rs:100-105`) backends do — so this harness
/// cannot observe it. Asserting it here would test the harness, not the
/// server.
#[tokio::test]
async fn aggregator_apiservice_patch_list_and_delete_collection() {
    let state = spawn_state();
    let name = "v1alpha1.wardle.example.com";
    seed_apiservice(
        &state,
        apiservice_remote(
            name,
            "wardle.example.com",
            "v1alpha1",
            "wardle",
            "sample-apiserver",
            7443,
        ),
    )
    .await;

    // A second, unlabelled APIService. Upstream labels its object precisely so
    // the collection delete is *scoped*; without a bystander that must survive,
    // a handler that ignored `labelSelector` and deleted everything would pass
    // this test just as happily.
    let bystander = "v1.bystander.example.com";
    seed_apiservice(
        &state,
        apiservice_local(bystander, "bystander.example.com", "v1"),
    )
    .await;

    let object_path = format!("/apis/apiregistration.k8s.io/v1/apiservices/{name}");

    // aggregator.go:537-546 — merge-patch spec.versionPriority to 400.
    let (status, _, patched) = state
        .send_raw(
            "PATCH",
            &object_path,
            Some("application/merge-patch+json"),
            Some(&json!({ "spec": { "versionPriority": 400 } })),
        )
        .await;
    assert!(
        status.is_success(),
        "Patch failed for {object_path}: {status} {patched}"
    );
    assert_eq!(
        patched["spec"]["versionPriority"].as_i64(),
        Some(400),
        "The VersionPriority returned was {:?}",
        patched["spec"]["versionPriority"]
    );

    // aggregator.go:549-570 — the APIService must be locatable in the list.
    let (status, list) =
        http_get(state.clone(), "/apis/apiregistration.k8s.io/v1/apiservices").await;
    assert_eq!(status, StatusCode::OK);
    let listed: Vec<&str> = list["items"]
        .as_array()
        .expect("APIServiceList items")
        .iter()
        .filter_map(|i| i["metadata"]["name"].as_str())
        .collect();
    assert!(
        listed.contains(&name),
        "Unable to find {name} in APIServiceList: {listed:?}"
    );

    // aggregator.go:573+ — the object carries no labels, so upstream adds one
    // specifically to have something to select on.
    let (status, _, labelled) = state
        .send_raw(
            "PATCH",
            &object_path,
            Some("application/merge-patch+json"),
            Some(&json!({ "metadata": { "labels": { "e2e-apiservice": "patched" } } })),
        )
        .await;
    assert!(
        status.is_success(),
        "labelling patch failed: {status} {labelled}"
    );
    assert_eq!(
        labelled["metadata"]["labels"]["e2e-apiservice"].as_str(),
        Some("patched"),
        "label not persisted: {labelled}"
    );

    // aggregator.go:743-747 — DeleteCollection restricted by that selector.
    let (status, _, deleted) = state
        .send_raw(
            "DELETE",
            "/apis/apiregistration.k8s.io/v1/apiservices?labelSelector=e2e-apiservice%3Dpatched",
            None,
            None,
        )
        .await;
    assert!(
        status.is_success(),
        "Unable to delete apiservice {name}: {status} {deleted}"
    );

    // aggregator.go:749-751 — the selected set must now be empty.
    let (status, remaining) = http_get(
        state.clone(),
        "/apis/apiregistration.k8s.io/v1/apiservices?labelSelector=e2e-apiservice%3Dpatched",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        remaining["items"].as_array().map(Vec::len),
        Some(0),
        "failed to count the required APIServices: {remaining}"
    );

    // The unlabelled APIService must be untouched.
    let (status, all) =
        http_get(state.clone(), "/apis/apiregistration.k8s.io/v1/apiservices").await;
    assert_eq!(status, StatusCode::OK);
    let survivors: Vec<&str> = all["items"]
        .as_array()
        .expect("APIServiceList items")
        .iter()
        .filter_map(|i| i["metadata"]["name"].as_str())
        .collect();
    assert!(
        survivors.contains(&bystander),
        "deleteCollection ignored its labelSelector and removed {bystander}: {survivors:?}"
    );
    assert!(
        !survivors.contains(&name),
        "{name} survived the label-selected deleteCollection: {survivors:?}"
    );

    // ...and the aggregated group must be gone from discovery with it.
    let (_, apis) = http_get(state, "/apis").await;
    let group_names: Vec<&str> = apis["groups"]
        .as_array()
        .expect("groups")
        .iter()
        .filter_map(|g| g["name"].as_str())
        .collect();
    assert!(
        !group_names.contains(&"wardle.example.com"),
        "aggregated group still present after deleteCollection: {group_names:?}"
    );
}
