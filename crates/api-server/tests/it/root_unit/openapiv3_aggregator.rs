//! Tests for `src/openapiv3_aggregator.rs`, ported from
//! `kube-aggregator/pkg/controllers/openapiv3/aggregator/{downloader,aggregator}_test.go`
//! (`TestDownloadOpenAPISpec`, `TestV3APIService`; `TestV2APIService` is in
//! `openapi_aggregator.rs` next to the v2 controller's tests).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use k3rs_api_server::aggregated_discovery::{HandlerRequest, HandlerResponse};
use k3rs_api_server::aggregator_mux::ProxyHandler;
use k3rs_api_server::apiservice_registration::{APIHandlerManager, AggregatorHandlerManager};
use k3rs_api_server::openapiv3_aggregator::{
    AggregationController, Downloader, SpecProxier, SpecServer, SyncAction, UpdateError,
    AGGREGATOR_USER,
};
use k3rs_common::resources::{APIService, APIServiceReference};

const ETAG: &str = "6E8F849B434D4B98A569B9D7718876E9-356ECAB19D7FBE";

/// `testV3APIService` / `handlerTest`: one group-version, root + document.
struct V3Server {
    gv: &'static str,
    requests: Mutex<Vec<HandlerRequest>>,
}

impl V3Server {
    fn new(gv: &'static str) -> Arc<Self> {
        Arc::new(Self {
            gv,
            requests: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl SpecServer for V3Server {
    async fn serve(
        &self,
        _handler: &ProxyHandler,
        _group: &str,
        _version: &str,
        req: HandlerRequest,
    ) -> HandlerResponse {
        self.requests.lock().unwrap().push(req.clone());
        if req.path == "/openapi/v3" {
            let body = serde_json::json!({"paths": {
                self.gv: {"serverRelativeURL": format!("/openapi/v3/{}?hash={ETAG}", self.gv)}
            }});
            return HandlerResponse {
                status: 200,
                body: body.to_string().into_bytes(),
                ..Default::default()
            };
        }
        HandlerResponse {
            status: 404,
            ..Default::default()
        }
    }
}

/// `testV2APIService`: no `/openapi/v3`.
struct V2OnlyServer(AtomicUsize);

#[async_trait]
impl SpecServer for V2OnlyServer {
    async fn serve(
        &self,
        _: &ProxyHandler,
        _: &str,
        _: &str,
        _: HandlerRequest,
    ) -> HandlerResponse {
        self.0.fetch_add(1, Ordering::SeqCst);
        HandlerResponse {
            status: 404,
            ..Default::default()
        }
    }
}

/// Answers every request with a fixed status and body.
struct FixedServer(u16, &'static str);

#[async_trait]
impl SpecServer for FixedServer {
    async fn serve(
        &self,
        _: &ProxyHandler,
        _: &str,
        _: &str,
        _: HandlerRequest,
    ) -> HandlerResponse {
        HandlerResponse {
            status: self.0,
            body: self.1.as_bytes().to_vec(),
            ..Default::default()
        }
    }
}

fn api_service(group: &str, version: &str, with_service: bool) -> APIService {
    let mut s = APIService::default();
    s.metadata.name = format!("{version}.{group}");
    s.spec.group = group.to_string();
    s.spec.version = version.to_string();
    if with_service {
        s.spec.service = Some(APIServiceReference {
            namespace: "ns".into(),
            name: "backend".into(),
            port: None,
        });
    }
    s
}

fn proxier(server: Arc<dyn SpecServer>) -> Arc<SpecProxier> {
    Arc::new(SpecProxier::new(Downloader::new(), server))
}

#[tokio::test]
async fn download_openapi_v3_root_lists_the_group_version() {
    // TestDownloadOpenAPISpec
    let server = V3Server::new("apis/group/version");
    let (groups, status) = Downloader::new()
        .openapi_v3_root(server.as_ref(), &ProxyHandler::new(), "group", "version")
        .await
        .unwrap();
    assert_eq!(status, 200);
    let groups = groups.unwrap();
    assert_eq!(groups.paths.len(), 1);
    assert!(!groups.paths["apis/group/version"]
        .server_relative_url
        .is_empty());
}

#[tokio::test]
async fn download_runs_as_the_aggregator_user_against_openapi_v3() {
    // handlerWithUser(handler, &user.DefaultInfo{Name: aggregatorUser}); GET /openapi/v3
    let server = V3Server::new("apis/group/version");
    Downloader::new()
        .openapi_v3_root(server.as_ref(), &ProxyHandler::new(), "group", "version")
        .await
        .unwrap();
    let reqs = server.requests.lock().unwrap();
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].path, "/openapi/v3");
    assert_eq!(reqs[0].user, AGGREGATOR_USER);
    assert_eq!(AGGREGATOR_USER, "system:aggregator");
    assert!(reqs[0].groups.is_empty());
}

#[tokio::test]
async fn download_maps_status_codes_like_openapi_v3_root() {
    let d = Downloader::new();
    let h = ProxyHandler::new();
    // 404: (nil, 404, nil)
    let (g, status) = d
        .openapi_v3_root(&FixedServer(404, ""), &h, "g", "v")
        .await
        .unwrap();
    assert!(g.is_none());
    assert_eq!(status, 404);
    // anything else: the "could not get list" error
    let err = d
        .openapi_v3_root(&FixedServer(503, "x"), &h, "g", "v")
        .await
        .unwrap_err();
    assert_eq!(
        err,
        "Error, could not get list of group versions for APIService"
    );
    // 200 with a body that is not the discovery document: the decode error
    assert!(d
        .openapi_v3_root(&FixedServer(200, "not json"), &h, "g", "v")
        .await
        .is_err());
}

#[tokio::test]
async fn v3_api_service_is_aggregated_and_proxied_by_group_version() {
    // TestV3APIService
    let p = proxier(V3Server::new("apis/group.example.com/v1"));
    let svc = api_service("group.example.com", "v1", true);
    p.add_update_api_service(Arc::new(ProxyHandler::new()), &svc);
    p.update_api_service_spec("v1.group.example.com")
        .await
        .unwrap();

    let root = p.get_open_api_v3_root();
    let gv = &root.paths["apis/group.example.com/v1"];
    assert_eq!(
        gv.server_relative_url,
        format!("/openapi/v3/apis/group.example.com/v1?hash={ETAG}")
    );
    // handleGroupVersion: the APIService listing the path serves it
    let (_, group, version) = p
        .group_version_handler("apis/group.example.com/v1")
        .unwrap();
    assert_eq!(
        (group.as_str(), version.as_str()),
        ("group.example.com", "v1")
    );
    // ... and a path nobody lists is the 404 branch
    assert!(p.group_version_handler("apis/other/v1").is_none());
    assert_eq!(p.get_api_service_names(), vec!["v1.group.example.com"]);
}

#[tokio::test]
async fn removing_an_api_service_removes_its_paths() {
    let p = proxier(V3Server::new("apis/group.example.com/v1"));
    let svc = api_service("group.example.com", "v1", true);
    p.add_update_api_service(Arc::new(ProxyHandler::new()), &svc);
    p.update_api_service_spec(&svc.metadata.name).await.unwrap();
    assert_eq!(p.get_open_api_v3_root().paths.len(), 1);

    p.remove_api_service_spec(&svc.metadata.name);
    assert!(p.get_open_api_v3_root().paths.is_empty());
    assert!(p.get_api_service_names().is_empty());
    // "If it does not exist, no error is returned."
    p.remove_api_service_spec(&svc.metadata.name);
}

#[tokio::test]
async fn update_of_an_unknown_api_service_is_err_api_service_not_found() {
    let p = proxier(V3Server::new("apis/g/v"));
    assert_eq!(
        p.update_api_service_spec("v1.nope").await,
        Err(UpdateError::NotFound)
    );
    assert_eq!(UpdateError::NotFound.to_string(), "resource not found");
}

#[tokio::test]
async fn an_api_service_without_openapi_v3_is_marked_legacy_and_not_asked_again() {
    // TestV2APIService's "legacy" half: 404 -> isLegacyAPIService, then only
    // `/openapi/v2` is asked for. This server has no v2 document either, so it
    // contributes no v3 path.
    let server = Arc::new(V2OnlyServer(AtomicUsize::new(0)));
    let p = proxier(server.clone());
    let svc = api_service("group.example.com", "v1", true);
    p.add_update_api_service(Arc::new(ProxyHandler::new()), &svc);
    p.update_api_service_spec(&svc.metadata.name).await.unwrap();
    p.update_api_service_spec(&svc.metadata.name).await.unwrap();
    assert!(p.get_open_api_v3_root().paths.is_empty());
    // v3 once (404), then v2 on both refreshes: "sticky isLegacy".
    assert_eq!(server.0.load(Ordering::SeqCst), 3, "sticky isLegacy");
    assert_eq!(p.get_api_service_names().len(), 1);
}

#[tokio::test]
async fn a_failed_refresh_keeps_the_previous_discovery() {
    // `s.apiServiceInfo[name].discovery = gv` only runs on success.
    let p = proxier(V3Server::new("apis/group.example.com/v1"));
    let svc = api_service("group.example.com", "v1", true);
    p.add_update_api_service(Arc::new(ProxyHandler::new()), &svc);
    p.update_api_service_spec(&svc.metadata.name).await.unwrap();

    let failing = proxier(Arc::new(FixedServer(503, "")));
    failing.add_update_api_service(Arc::new(ProxyHandler::new()), &svc);
    assert!(matches!(
        failing.update_api_service_spec(&svc.metadata.name).await,
        Err(UpdateError::Other(_))
    ));
    assert!(failing.get_open_api_v3_root().paths.is_empty());
    assert_eq!(p.get_open_api_v3_root().paths.len(), 1);
}

#[tokio::test]
async fn add_update_reuses_the_existing_record() {
    // "If the APIService is being updated, use the existing struct."
    let p = proxier(V3Server::new("apis/group.example.com/v1"));
    let svc = api_service("group.example.com", "v1", true);
    p.add_update_api_service(Arc::new(ProxyHandler::new()), &svc);
    p.update_api_service_spec(&svc.metadata.name).await.unwrap();
    p.add_update_api_service(Arc::new(ProxyHandler::new()), &svc);
    assert_eq!(p.get_open_api_v3_root().paths.len(), 1);
}

#[tokio::test]
async fn sync_maps_errors_to_actions() {
    // controller.go sync: not found -> syncNothing; other -> rate-limited requeue.
    let c = AggregationController::new(proxier(Arc::new(FixedServer(503, ""))));
    let (action, err) = c.sync("v1.missing").await;
    assert_eq!((action, err), (SyncAction::Nothing, None));

    let svc = api_service("g", "v1", true);
    c.spec_proxier
        .add_update_api_service(Arc::new(ProxyHandler::new()), &svc);
    let (action, err) = c.sync(&svc.metadata.name).await;
    assert_eq!(action, SyncAction::RequeueRateLimited);
    assert!(err.is_some());

    let ok = AggregationController::new(proxier(V3Server::new("apis/g/v1")));
    ok.spec_proxier
        .add_update_api_service(Arc::new(ProxyHandler::new()), &svc);
    assert_eq!(
        ok.sync(&svc.metadata.name).await,
        (SyncAction::Requeue, None)
    );
}

#[tokio::test]
async fn add_api_service_ignores_local_apiservices() {
    // "if apiService.Spec.Service == nil { return }"
    let c = AggregationController::new(proxier(V3Server::new("apis/g/v1")));
    let local = api_service("g", "v1", false);
    c.add_api_service(Arc::new(ProxyHandler::new()), &local);
    c.update_api_service(Arc::new(ProxyHandler::new()), &local);
    assert!(c.spec_proxier.get_api_service_names().is_empty());
}

#[tokio::test]
async fn add_api_service_downloads_the_spec_in_a_second() {
    // AddAPIService -> queue.AddAfter(name, time.Second) -> worker -> sync.
    let c = AggregationController::new(proxier(V3Server::new("apis/g/v1")));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let run = tokio::spawn(c.clone().run(stop_rx));
    c.add_api_service(Arc::new(ProxyHandler::new()), &api_service("g", "v1", true));
    assert!(c.spec_proxier.get_open_api_v3_root().paths.is_empty());
    let mut got = false;
    for _ in 0..300 {
        if !c.spec_proxier.get_open_api_v3_root().paths.is_empty() {
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
async fn handler_manager_forwards_add_update_and_remove() {
    // apiserver.go AddAPIService / RemoveAPIService call the controller.
    let c = AggregationController::new(proxier(V3Server::new("apis/g/v1")));
    let mgr = AggregatorHandlerManager::new(None).with_openapi_v3_controller(c.clone());
    let svc = api_service("g", "v1", true);

    mgr.add_api_service(&svc).unwrap();
    assert_eq!(c.spec_proxier.get_api_service_names(), vec!["v1.g"]);
    // the update branch (proxyHandler exists)
    mgr.add_api_service(&svc).unwrap();
    assert_eq!(c.spec_proxier.get_api_service_names(), vec!["v1.g"]);

    mgr.remove_api_service("v1.g");
    assert!(c.spec_proxier.get_api_service_names().is_empty());
    assert!(mgr.openapi_v3_controller().is_some());
    assert!(AggregatorHandlerManager::new(None)
        .openapi_v3_controller()
        .is_none());
}
