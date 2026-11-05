//! Tests for `src/openapi_aggregator.rs`, ported from
//! `kube-aggregator/pkg/controllers/openapi/aggregator/{downloader,aggregator,priority}_test.go`
//! (`TestDownloadOpenAPISpec`, `TestAPIServiceSort`, `TestBasicPathsMerged`,
//! `TestAddUpdateAPIService`, `TestAPIServiceOpenAPIServiceMismatch`,
//! `TestAddRemoveAPIService`, `TestUpdateAPIService`,
//! `TestFailingAPIServiceSkippedAggregation`,
//! `TestAPIServiceFailSuccessTransition`, `TestFailingAPIServiceDoesNotBlockAdd`)
//! and the controller's `sync`.
//!
//! Adaptation: upstream hands each APIService its own `http.Handler`; here the
//! `SpecServer` is keyed by the APIService's group and the handlers' documents
//! are mutated in place.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusternetes_api_server::aggregated_discovery::{HandlerRequest, HandlerResponse};
use rusternetes_api_server::aggregator_mux::ProxyHandler;
use rusternetes_api_server::apiservice_registration::{
    APIHandlerManager, AggregatorHandlerManager,
};
use rusternetes_api_server::openapi_aggregator::{
    etag_for, sort_by_priority, AggregationController, Downloader, SpecAggregator, SyncAction,
};
use rusternetes_api_server::openapiv3_aggregator::{
    Downloader as V3Downloader, SpecProxier, SpecServer, UpdateError,
};
use rusternetes_common::resources::{APIService, APIServiceReference};
use serde_json::{json, Value};

/// `handlerTest`: serves `data` with `etag`, 304 on a matching `If-None-Match`.
struct EtagServer {
    etag: &'static str,
    data: &'static str,
    requests: Mutex<Vec<HandlerRequest>>,
}

impl EtagServer {
    fn new(etag: &'static str, data: &'static str) -> Self {
        Self {
            etag,
            data,
            requests: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl SpecServer for EtagServer {
    async fn serve(
        &self,
        _: &ProxyHandler,
        _: &str,
        _: &str,
        req: HandlerRequest,
    ) -> HandlerResponse {
        self.requests.lock().unwrap().push(req.clone());
        let etag = (!self.etag.is_empty()).then(|| self.etag.to_string());
        if req.if_none_match.as_deref() == Some(self.etag) {
            return HandlerResponse {
                status: 304,
                etag,
                ..Default::default()
            };
        }
        HandlerResponse {
            status: 200,
            etag,
            body: self.data.as_bytes().to_vec(),
            ..Default::default()
        }
    }
}

/// `openAPIHandler`.
struct OpenAPIHandler {
    openapi: Mutex<Value>,
    return_err: Mutex<bool>,
}

fn swagger_with_paths(paths: &[&str]) -> Value {
    let p: serde_json::Map<String, Value> =
        paths.iter().map(|p| (p.to_string(), json!({}))).collect();
    json!({ "paths": p })
}

impl OpenAPIHandler {
    fn new(paths: &[&str]) -> Arc<Self> {
        Arc::new(Self {
            openapi: Mutex::new(swagger_with_paths(paths)),
            return_err: Mutex::new(false),
        })
    }

    fn failing(paths: &[&str]) -> Arc<Self> {
        let h = Self::new(paths);
        *h.return_err.lock().unwrap() = true;
        h
    }
}

/// The handlers by the APIService's group.
#[derive(Default)]
struct ByGroup(Mutex<HashMap<String, Arc<OpenAPIHandler>>>);

#[async_trait]
impl SpecServer for ByGroup {
    async fn serve(
        &self,
        _: &ProxyHandler,
        group: &str,
        _: &str,
        req: HandlerRequest,
    ) -> HandlerResponse {
        assert_eq!(req.path, "/openapi/v2");
        let h = self.0.lock().unwrap().get(group).cloned().unwrap();
        if *h.return_err.lock().unwrap() {
            return HandlerResponse {
                status: 500,
                ..Default::default()
            };
        }
        let body = serde_json::to_vec(&*h.openapi.lock().unwrap()).unwrap();
        HandlerResponse {
            status: 200,
            body,
            ..Default::default()
        }
    }
}

fn api_service(name: &str, group: &str, version: &str) -> APIService {
    let mut s = APIService::default();
    s.metadata.name = name.to_string();
    s.spec.group = group.to_string();
    s.spec.version = version.to_string();
    s.spec.service = Some(APIServiceReference {
        namespace: "ns".into(),
        name: "dummy".into(),
        port: None,
    });
    s
}

/// `buildAndRegisterSpecAggregator`: the aggregator spec, one delegate, and a
/// `SpecAggregator` over `server`.
struct Fixture {
    aggregator: SpecAggregator,
    server: Arc<ByGroup>,
    local: Value,
}

impl Fixture {
    fn new() -> Self {
        let server = Arc::new(ByGroup::default());
        let aggregator = SpecAggregator::new(Downloader::new(), server.clone());
        aggregator.add_local_spec(
            "k8s_internal_local_delegation_chain_0000000001",
            swagger_with_paths(&["/apis/foo/v1/"]),
        );
        Self {
            aggregator,
            server,
            local: swagger_with_paths(&["/apis/apiregistration.k8s.io/v1/"]),
        }
    }

    fn handler(&self, group: &str, h: &Arc<OpenAPIHandler>) {
        self.server
            .0
            .lock()
            .unwrap()
            .insert(group.to_string(), h.clone());
    }

    fn add(&self, svc: &APIService) {
        self.aggregator
            .add_update_api_service(svc, Arc::new(ProxyHandler::new()));
    }

    /// `fetchOpenAPI`.
    fn fetch(&self) -> Value {
        self.aggregator
            .merged_spec(Some(&self.local))
            .unwrap()
            .unwrap()
    }
}

fn expect_path(spec: &Value, path: &str) {
    assert!(
        spec["paths"].get(path).is_some(),
        "Expected path {path} to exist in aggregated paths"
    );
}

fn expect_no_path(spec: &Value, path: &str) {
    assert!(
        spec["paths"].get(path).is_none(),
        "Expected path {path} to be omitted in aggregated paths"
    );
}

// --- downloader_test.go ---------------------------------------------------

const DATA: &str = "{\"id\": \"test\"}";

fn assert_downloaded(got: (Option<Value>, String, u16), expected_id: &str, expected_etag: &str) {
    let (spec, etag, _) = got;
    if expected_id.is_empty() {
        assert!(spec.is_none(), "expected Not Modified");
    } else {
        assert_eq!(spec.unwrap()["id"], expected_id);
    }
    assert_eq!(etag, expected_etag);
}

#[tokio::test]
async fn download_openapi_spec() {
    // TestDownloadOpenAPISpec
    let d = Downloader::new();
    let h = ProxyHandler::new();

    // "Test with no eTag"
    let s = EtagServer::new("", DATA);
    assert_downloaded(
        d.download(&s, &h, "g", "v", "").await.unwrap(),
        "test",
        "\"6E8F849B434D4B98A569B9D7718876E9-356ECAB19D7FBE1336BABB1E70F8F3025050DE218BE78256BE81620681CFC9A268508E542B8B55974E17B2184BBFC8FFFAA577E51BE195D32B3CA2547818ABE4\"",
    );
    // `/openapi/v2`, `Accept: application/json`, as the aggregator user.
    {
        let reqs = s.requests.lock().unwrap();
        assert_eq!(reqs[0].path, "/openapi/v2");
        assert_eq!(reqs[0].accept, "application/json");
        assert_eq!(reqs[0].user, "system:aggregator");
    }

    // "Test with eTag"
    let s = EtagServer::new("etag_test", DATA);
    assert_downloaded(
        d.download(&s, &h, "g", "v", "").await.unwrap(),
        "test",
        "etag_test",
    );
    // "Test not modified"
    assert_downloaded(
        d.download(&s, &h, "g", "v", "etag_test").await.unwrap(),
        "",
        "etag_test",
    );
    // "Test different eTags"
    let s = EtagServer::new("etag_test1", DATA);
    assert_downloaded(
        d.download(&s, &h, "g", "v", "etag_test2").await.unwrap(),
        "test",
        "etag_test1",
    );
}

#[tokio::test]
async fn download_does_not_send_a_locally_generated_etag_and_detects_no_change() {
    let d = Downloader::new();
    let h = ProxyHandler::new();
    let s = EtagServer::new("", DATA);
    let local = etag_for(DATA.as_bytes());
    let (spec, etag, status) = d.download(&s, &h, "g", "v", &local).await.unwrap();
    // "if etag == newEtag { return nil, etag, http.StatusNotModified, nil }"
    assert!(spec.is_none());
    assert_eq!((etag.as_str(), status), (local.as_str(), 304));
    assert_eq!(s.requests.lock().unwrap()[0].if_none_match, None);
}

#[tokio::test]
async fn download_maps_the_other_statuses() {
    struct Fixed(u16);
    #[async_trait]
    impl SpecServer for Fixed {
        async fn serve(
            &self,
            _: &ProxyHandler,
            _: &str,
            _: &str,
            _: HandlerRequest,
        ) -> HandlerResponse {
            HandlerResponse {
                status: self.0,
                body: b"nope".to_vec(),
                ..Default::default()
            }
        }
    }
    let d = Downloader::new();
    let h = ProxyHandler::new();
    assert_eq!(
        d.download(&Fixed(404), &h, "g", "v", "").await.unwrap(),
        (None, String::new(), 404)
    );
    assert_eq!(
        d.download(&Fixed(304), &h, "g", "v", "").await.unwrap_err(),
        "http.StatusNotModified is not allowed in absence of etag"
    );
    let err = d.download(&Fixed(403), &h, "g", "v", "").await.unwrap_err();
    assert!(err.starts_with("failed to retrieve openAPI spec, http error: ResponseCode: 403"));
}

// --- priority_test.go -----------------------------------------------------

#[test]
fn api_service_sort() {
    // TestAPIServiceSort
    let mk = |name: &str, group: &str, min: i32, ver: i32, svc: bool| {
        let mut r = APIService::default();
        r.spec.group = group.into();
        r.spec.group_priority_minimum = min;
        r.spec.version_priority = ver;
        if svc {
            r.spec.service = Some(APIServiceReference::default());
        }
        r.metadata.name = name.into();
        r
    };
    let mut list = vec![
        mk("FirstService", "Group1", 10, 5, true),
        mk("SecondService", "Group2", 15, 3, true),
        mk("FirstServiceInternal", "Group1", 16, 3, true),
        mk("ThirdService", "Group3", 15, 3, true),
        mk("local_service_1", "Group4", 15, 1, false),
        mk("local_service_3", "Group5", 15, 2, false),
        mk("local_service_2", "Group6", 15, 3, false),
    ];
    sort_by_priority(&mut list);
    let names: Vec<&str> = list.iter().map(|a| a.metadata.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "local_service_1",
            "local_service_2",
            "local_service_3",
            "FirstService",
            "FirstServiceInternal",
            "SecondService",
            "ThirdService"
        ]
    );
}

// --- aggregator_test.go ---------------------------------------------------

#[test]
fn basic_paths_merged() {
    // TestBasicPathsMerged
    let f = Fixture::new();
    let swagger = f.fetch();
    expect_path(&swagger, "/apis/foo/v1/");
    expect_path(&swagger, "/apis/apiregistration.k8s.io/v1/");
}

#[tokio::test]
async fn add_update_api_service() {
    // TestAddUpdateAPIService
    let f = Fixture::new();
    let svc = api_service("apiservice", "apiservicegroup", "v1");
    let h = OpenAPIHandler::new(&["/apis/apiservicegroup/v1/path1"]);
    f.handler("apiservicegroup", &h);
    f.add(&svc);
    f.aggregator
        .update_api_service_spec("apiservice")
        .await
        .unwrap();

    let swagger = f.fetch();
    expect_path(&swagger, "/apis/apiservicegroup/v1/path1");
    expect_path(&swagger, "/apis/apiregistration.k8s.io/v1/");

    // "Update APIService OpenAPI"
    *h.openapi.lock().unwrap() = swagger_with_paths(&["/apis/apiservicegroup/v1/path2"]);
    f.aggregator
        .update_api_service_spec("apiservice")
        .await
        .unwrap();
    let swagger = f.fetch();
    expect_path(&swagger, "/apis/apiservicegroup/v1/path2");
    expect_no_path(&swagger, "/apis/apiservicegroup/v1/path1");
    expect_path(&swagger, "/apis/apiregistration.k8s.io/v1/");
}

#[tokio::test]
async fn api_service_openapi_service_mismatch() {
    // TestAPIServiceOpenAPIServiceMismatch: an APIService's OpenAPI is
    // filtered to its own group version (kubernetes#123570).
    let f = Fixture::new();
    let svc = api_service("apiservice", "apiservicegroup", "v1");
    let svc2 = api_service("apiservice2", "apiservicegroup2", "v2");
    f.handler(
        "apiservicegroup",
        &OpenAPIHandler::new(&[
            "/apis/apiservicegroup/v1/",
            "/apis/apiservicegroup/v1beta1/",
        ]),
    );
    f.handler(
        "apiservicegroup2",
        &OpenAPIHandler::new(&[
            "/apis/a/",
            "/apis/apiservicegroup2/v1/",
            "/apis/apiservicegroup2/v2/",
        ]),
    );
    f.add(&svc);
    f.aggregator
        .update_api_service_spec("apiservice")
        .await
        .unwrap();
    f.add(&svc2);
    f.aggregator
        .update_api_service_spec("apiservice2")
        .await
        .unwrap();

    let swagger = f.fetch();
    expect_path(&swagger, "/apis/apiservicegroup/v1/");
    expect_path(&swagger, "/apis/apiservicegroup2/v2/");
    expect_path(&swagger, "/apis/apiregistration.k8s.io/v1/");
    expect_no_path(&swagger, "/apis/a/");
    expect_no_path(&swagger, "/apis/apiservicegroup/v1beta1/");
    expect_no_path(&swagger, "/apis/apiservicegroup2/v1/");

    f.aggregator.remove_api_service("apiservice");
    let swagger = f.fetch();
    expect_no_path(&swagger, "/apis/apiservicegroup/v1");
    expect_path(&swagger, "/apis/apiregistration.k8s.io/v1/");
    expect_no_path(&swagger, "/apis/a");
}

#[tokio::test]
async fn add_remove_api_service() {
    // TestAddRemoveAPIService
    let f = Fixture::new();
    let svc = api_service("apiservice", "apiservicegroup", "v1");
    f.handler(
        "apiservicegroup",
        &OpenAPIHandler::new(&["/apis/apiservicegroup/v1/"]),
    );
    f.add(&svc);
    f.aggregator
        .update_api_service_spec("apiservice")
        .await
        .unwrap();
    let swagger = f.fetch();
    expect_path(&swagger, "/apis/apiservicegroup/v1/");
    expect_path(&swagger, "/apis/apiregistration.k8s.io/v1/");

    f.aggregator.remove_api_service("apiservice");
    let swagger = f.fetch();
    expect_no_path(&swagger, "/apis/apiservicegroup/v1/");
    expect_path(&swagger, "/apis/apiregistration.k8s.io/v1/");
}

#[tokio::test]
async fn update_api_service() {
    // TestUpdateAPIService
    let f = Fixture::new();
    let svc = api_service("apiservice", "apiservicegroup", "v1");
    let h = OpenAPIHandler::new(&["/apis/apiservicegroup/v1/"]);
    f.handler("apiservicegroup", &h);
    f.add(&svc);
    f.aggregator
        .update_api_service_spec("apiservice")
        .await
        .unwrap();
    expect_path(&f.fetch(), "/apis/apiservicegroup/v1/");

    // "Updating APIService": the handler now serves an empty document.
    *h.openapi.lock().unwrap() = swagger_with_paths(&[]);
    f.add(&svc);
    f.aggregator
        .update_api_service_spec("apiservice")
        .await
        .unwrap();
    let swagger = f.fetch();
    expect_no_path(&swagger, "/apis/apiservicegroup/v1/");
    expect_path(&swagger, "/apis/apiregistration.k8s.io/v1/");
}

#[tokio::test]
async fn failing_api_service_skipped_aggregation() {
    // TestFailingAPIServiceSkippedAggregation
    let f = Fixture::new();
    let failed = api_service("apiserviceFailed", "failed", "v1");
    let success = api_service("apiserviceSuccess", "success", "v1");
    f.handler("failed", &OpenAPIHandler::failing(&["/apis/failed/v1/"]));
    f.handler("success", &OpenAPIHandler::new(&["/apis/success/v1/"]));
    f.add(&success);
    f.add(&failed);
    f.aggregator
        .update_api_service_spec("apiserviceSuccess")
        .await
        .unwrap();
    assert!(f
        .aggregator
        .update_api_service_spec("apiserviceFailed")
        .await
        .is_err());

    let swagger = f.fetch();
    expect_path(&swagger, "/apis/foo/v1/");
    expect_no_path(&swagger, "/apis/failed/v1/");
    expect_path(&swagger, "/apis/success/v1/");
}

#[tokio::test]
async fn api_service_fail_success_transition() {
    // TestAPIServiceFailSuccessTransition
    let f = Fixture::new();
    let svc = api_service("apiservice", "apiservicegroup", "v1");
    let h = OpenAPIHandler::failing(&["/apis/apiservicegroup/v1/"]);
    f.handler("apiservicegroup", &h);
    f.add(&svc);
    assert!(f
        .aggregator
        .update_api_service_spec("apiservice")
        .await
        .is_err());
    let swagger = f.fetch();
    expect_path(&swagger, "/apis/foo/v1/");
    expect_no_path(&swagger, "/apis/apiservicegroup/v1/");

    // "Transition APIService to not return error"
    *h.return_err.lock().unwrap() = false;
    f.aggregator
        .update_api_service_spec("apiservice")
        .await
        .unwrap();
    let swagger = f.fetch();
    expect_path(&swagger, "/apis/foo/v1/");
    expect_path(&swagger, "/apis/apiservicegroup/v1/");
}

#[tokio::test]
async fn failing_api_service_does_not_block_add() {
    // TestFailingAPIServiceDoesNotBlockAdd: adding never downloads.
    let f = Fixture::new();
    let failed = api_service("apiserviceFailed", "failed", "v1");
    f.handler("failed", &OpenAPIHandler::failing(&["/apis/failed/v1/"]));
    let t = std::time::Instant::now();
    f.add(&failed);
    assert!(t.elapsed() < std::time::Duration::from_secs(2));
    let swagger = f.fetch();
    expect_path(&swagger, "/apis/foo/v1/");
    expect_no_path(&swagger, "/apis/failed/v1/");
}

#[tokio::test]
async fn a_spec_that_failed_after_a_success_keeps_serving_the_first_success() {
    // `cached.LastSuccess`: errors are hidden behind the last success.
    let f = Fixture::new();
    let svc = api_service("apiservice", "apiservicegroup", "v1");
    let h = OpenAPIHandler::new(&["/apis/apiservicegroup/v1/"]);
    f.handler("apiservicegroup", &h);
    f.add(&svc);
    f.aggregator
        .update_api_service_spec("apiservice")
        .await
        .unwrap();
    expect_path(&f.fetch(), "/apis/apiservicegroup/v1/");

    *h.return_err.lock().unwrap() = true;
    assert!(f
        .aggregator
        .update_api_service_spec("apiservice")
        .await
        .is_err());
    expect_path(&f.fetch(), "/apis/apiservicegroup/v1/");
}

#[tokio::test]
async fn a_download_failure_is_wrapped_and_a_404_is_not_the_not_found_sentinel() {
    // cacheableDownloader.Get wraps with `failed to download <name>: ...`, so
    // `err == ErrAPIServiceNotFound` is false for a 404 from the server.
    struct NotFound;
    #[async_trait]
    impl SpecServer for NotFound {
        async fn serve(
            &self,
            _: &ProxyHandler,
            _: &str,
            _: &str,
            _: HandlerRequest,
        ) -> HandlerResponse {
            HandlerResponse {
                status: 404,
                ..Default::default()
            }
        }
    }
    let a = SpecAggregator::new(Downloader::new(), Arc::new(NotFound));
    a.add_update_api_service(
        &api_service("v1.g", "g", "v1"),
        Arc::new(ProxyHandler::new()),
    );
    assert_eq!(
        a.update_api_service_spec("v1.g").await,
        Err(UpdateError::Other(
            "failed to download v1.g: resource not found".into()
        ))
    );
    // ... while an unknown APIService is the sentinel.
    assert_eq!(
        a.update_api_service_spec("nope").await,
        Err(UpdateError::NotFound)
    );
}

#[tokio::test]
async fn the_second_download_is_conditional_on_the_first_etag() {
    let s = Arc::new(EtagServer::new(
        "etag1",
        "{\"paths\":{\"/apis/g/v1/x\":{}}}",
    ));
    let a = SpecAggregator::new(Downloader::new(), s.clone());
    a.add_update_api_service(
        &api_service("v1.g", "g", "v1"),
        Arc::new(ProxyHandler::new()),
    );
    a.update_api_service_spec("v1.g").await.unwrap();
    a.update_api_service_spec("v1.g").await.unwrap();
    {
        let reqs = s.requests.lock().unwrap();
        assert_eq!(reqs[0].if_none_match, None);
        assert_eq!(reqs[1].if_none_match.as_deref(), Some("etag1"));
    }
    let merged = a.merged_spec(None).unwrap().unwrap();
    assert!(merged["paths"].get("/apis/g/v1/x").is_some());
}

#[tokio::test]
async fn local_apiservices_are_not_added() {
    // "if apiService.Spec.Service == nil { return nil }"
    let f = Fixture::new();
    let mut local = api_service("v1.g", "g", "v1");
    local.spec.service = None;
    f.add(&local);
    assert_eq!(
        f.aggregator.update_api_service_spec("v1.g").await,
        Err(UpdateError::NotFound)
    );
}

#[test]
fn nothing_to_merge_is_nil() {
    let a = SpecAggregator::new(Downloader::new(), Arc::new(ByGroup::default()));
    assert_eq!(a.merged_spec(None).unwrap(), None);
}

// --- controller.go ----------------------------------------------------------

fn controller(server: Arc<dyn SpecServer>) -> Arc<AggregationController> {
    AggregationController::new(Arc::new(SpecAggregator::new(Downloader::new(), server)))
}

#[tokio::test]
async fn sync_maps_errors_to_actions() {
    // sync: not found -> syncNothing; other -> syncRequeueRateLimited; ok -> syncRequeue.
    let server = Arc::new(ByGroup::default());
    let c = controller(server.clone());
    assert_eq!(c.sync("v1.missing").await, (SyncAction::Nothing, None));

    let svc = api_service("v1.g", "g", "v1");
    let h = OpenAPIHandler::failing(&["/apis/g/v1/"]);
    server.0.lock().unwrap().insert("g".into(), h.clone());
    c.spec_aggregator
        .add_update_api_service(&svc, Arc::new(ProxyHandler::new()));
    let (action, err) = c.sync("v1.g").await;
    assert_eq!(action, SyncAction::RequeueRateLimited);
    assert!(err.is_some());

    *h.return_err.lock().unwrap() = false;
    assert_eq!(c.sync("v1.g").await, (SyncAction::Requeue, None));
}

#[tokio::test]
async fn add_api_service_downloads_the_spec_in_a_second() {
    // AddAPIService -> queue.AddAfter(name, time.Second) -> worker -> sync.
    let server = Arc::new(ByGroup::default());
    server
        .0
        .lock()
        .unwrap()
        .insert("g".into(), OpenAPIHandler::new(&["/apis/g/v1/x"]));
    let c = controller(server);
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let run = tokio::spawn(c.clone().run(stop_rx));
    c.add_api_service(
        Arc::new(ProxyHandler::new()),
        &api_service("v1.g", "g", "v1"),
    );
    assert!(c.spec_aggregator.merged_spec(None).unwrap().is_none());
    let mut got = false;
    for _ in 0..300 {
        if c.spec_aggregator.merged_spec(None).unwrap().is_some() {
            got = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(got, "spec not downloaded");
    stop_tx.send_replace(true);
    run.await.unwrap();
}

#[tokio::test]
async fn controller_ignores_local_apiservices() {
    let c = controller(Arc::new(ByGroup::default()));
    let mut local = api_service("v1.g", "g", "v1");
    local.spec.service = None;
    c.add_api_service(Arc::new(ProxyHandler::new()), &local);
    c.update_api_service(&local);
    assert_eq!(
        c.spec_aggregator.update_api_service_spec("v1.g").await,
        Err(UpdateError::NotFound)
    );
}

#[tokio::test]
async fn handler_manager_forwards_add_update_and_remove() {
    // apiserver.go AddAPIService / RemoveAPIService call the v2 controller.
    let server = Arc::new(ByGroup::default());
    server
        .0
        .lock()
        .unwrap()
        .insert("g".into(), OpenAPIHandler::new(&["/apis/g/v1/x"]));
    let c = controller(server);
    let mgr = AggregatorHandlerManager::new(None).with_openapi_controller(c.clone());
    let svc = api_service("v1.g", "g", "v1");

    mgr.add_api_service(&svc).unwrap();
    // the update branch (proxyHandler exists) refreshes the spec
    mgr.add_api_service(&svc).unwrap();
    let mut got = false;
    for _ in 0..300 {
        if c.spec_aggregator.merged_spec(None).unwrap().is_some() {
            got = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(got, "UpdateAPIService did not refresh the spec");

    mgr.remove_api_service("v1.g");
    assert!(c.spec_aggregator.merged_spec(None).unwrap().is_none());
    assert!(mgr.openapi_controller().is_some());
    assert!(AggregatorHandlerManager::new(None)
        .openapi_controller()
        .is_none());
}

// --- the v3 controller's v2 fallback (openapiv3 aggregator.go) -------------

/// `testV2APIService`: no `/openapi/v3`; `/openapi/v2` serves a spec.
struct LegacyServer {
    v2: &'static str,
    etag: &'static str,
}

#[async_trait]
impl SpecServer for LegacyServer {
    async fn serve(
        &self,
        _: &ProxyHandler,
        _: &str,
        _: &str,
        req: HandlerRequest,
    ) -> HandlerResponse {
        if req.path != "/openapi/v2" {
            return HandlerResponse {
                status: 404,
                ..Default::default()
            };
        }
        if !self.etag.is_empty() && req.if_none_match.as_deref() == Some(self.etag) {
            return HandlerResponse {
                status: 304,
                etag: Some(self.etag.into()),
                ..Default::default()
            };
        }
        HandlerResponse {
            status: 200,
            etag: (!self.etag.is_empty()).then(|| self.etag.to_string()),
            body: self.v2.as_bytes().to_vec(),
            ..Default::default()
        }
    }
}

#[tokio::test]
async fn v3_legacy_api_service_is_converted_from_its_v2_spec() {
    // TestV2APIService: "A legacy APIService will not publish OpenAPI V3.
    // Ensure that we can still aggregate its V2 spec and convert it to V3."
    let server = Arc::new(LegacyServer {
        v2: r#"{"swagger":"2.0","info":{"title":"Kubernetes","version":"unversioned"}}"#,
        etag: "",
    });
    let p = Arc::new(SpecProxier::new(V3Downloader::new(), server));
    let svc = api_service("v1.group.example.com", "group.example.com", "v1");
    p.add_update_api_service(Arc::new(ProxyHandler::new()), &svc);
    p.update_api_service_spec("v1.group.example.com")
        .await
        .unwrap();

    let root = p.get_open_api_v3_root();
    let gv = &root.paths["apis/group.example.com/v1"];
    assert!(gv
        .server_relative_url
        .starts_with("/openapi/v3/apis/group.example.com/v1?hash="));
    assert_eq!(
        p.converted_group_version("apis/group.example.com/v1")
            .unwrap(),
        json!({"openapi":"3.0.0","info":{"title":"Kubernetes","version":"unversioned"},"components":{}})
    );

    // "Ensure that OpenAPI v3 for legacy APIService is removed."
    p.remove_api_service_spec("v1.group.example.com");
    assert!(p.get_open_api_v3_root().paths.is_empty());
    assert!(p
        .converted_group_version("apis/group.example.com/v1")
        .is_none());
}

#[tokio::test]
async fn v3_legacy_api_service_is_not_asked_for_v3_again_and_sends_its_etag() {
    let server = Arc::new(LegacyServer {
        v2: r#"{"swagger":"2.0","info":{"title":"t","version":"v"}}"#,
        etag: "\"abc\"",
    });
    let p = SpecProxier::new(V3Downloader::new(), server);
    let svc = api_service("v1.g", "g", "v1");
    p.add_update_api_service(Arc::new(ProxyHandler::new()), &svc);
    p.update_api_service_spec("v1.g").await.unwrap();
    // The second refresh is a conditional /openapi/v2 download: 304 keeps the
    // converted document.
    p.update_api_service_spec("v1.g").await.unwrap();
    assert!(p.converted_group_version("apis/g/v1").is_some());
}

#[tokio::test]
async fn v3_legacy_api_service_without_v2_either_is_a_no_op() {
    // `if httpStatus == http.StatusOK { ... }` -- a 404 publishes nothing.
    struct Gone;
    #[async_trait]
    impl SpecServer for Gone {
        async fn serve(
            &self,
            _: &ProxyHandler,
            _: &str,
            _: &str,
            _: HandlerRequest,
        ) -> HandlerResponse {
            HandlerResponse {
                status: 404,
                ..Default::default()
            }
        }
    }
    let p = SpecProxier::new(V3Downloader::new(), Arc::new(Gone));
    p.add_update_api_service(
        Arc::new(ProxyHandler::new()),
        &api_service("v1.g", "g", "v1"),
    );
    p.update_api_service_spec("v1.g").await.unwrap();
    assert!(p.get_open_api_v3_root().paths.is_empty());
}

#[tokio::test]
async fn the_built_in_document_keeps_definitions_that_no_path_uses() {
    // The built-in document is the merge destination: its definitions are not
    // derived from its paths (module docs of `openapi_aggregator`).
    let f = Fixture::new();
    let svc = api_service("apiservice", "apiservicegroup", "v1");
    f.handler(
        "apiservicegroup",
        &OpenAPIHandler::new(&["/apis/apiservicegroup/v1/x"]),
    );
    f.add(&svc);
    f.aggregator
        .update_api_service_spec("apiservice")
        .await
        .unwrap();
    let local =
        json!({"swagger": "2.0", "paths": {}, "definitions": {"io.k8s.Pod": {"type": "object"}}});
    let merged = f.aggregator.merged_spec(Some(&local)).unwrap().unwrap();
    assert!(merged["definitions"].get("io.k8s.Pod").is_some());
    assert_eq!(merged["swagger"], "2.0");
    expect_path(&merged, "/apis/apiservicegroup/v1/x");
}
