//! Unit tests moved from `src/apiservice_registration.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use futures::StreamExt;
    pub use rusternetes_api_server::aggregated_discovery::{
        is_aggregated_discovery_content_type, legacy_path, parse_discovery_list,
        parse_legacy_resource_list, serve_with_timeout, AggregatorHandler, HandlerRequest,
        ProxyDiscoveryHandler, ACCEPT_V2, ACCEPT_V2_BETA1, AGGREGATOR_GROUP, AGGREGATOR_USER,
    };
    pub use rusternetes_api_server::aggregator_mux::{
        install_handler_manager, serve_api_group, ApiGroupResponse, ProxyHandler,
    };
    pub use rusternetes_api_server::autoregister::{APIServiceClient, StorageAPIServiceClient};
    pub use rusternetes_api_server::crdregistration::WorkQueue;
    pub use rusternetes_api_server::post_start_hooks::{spawn_hook_with, PostStartHooks};
    pub use rusternetes_api_server::registry::apiextensions::customresourcedefinition::ControllerRateLimiter;
    pub use rusternetes_common::resources::APIService;
    pub use rusternetes_storage::{build_prefix, Storage, WatchEvent};
    pub use std::collections::{BTreeMap, BTreeSet, HashMap};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::{Duration, Instant};
    pub use tokio::sync::watch;
}

mod tests {
    use super::__parent::*;
    use async_trait::async_trait;
    use rusternetes_api_server::aggregated_discovery::HandlerResponse;
    use rusternetes_api_server::apiservice_registration::*;
    use rusternetes_api_server::autoregister::ClientError;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn svc(name: &str, with_service: bool) -> APIService {
        let (group, version) = api_service_name_to_group_version(name);
        let mut s = APIService::default();
        s.metadata.name = name.to_string();
        s.spec.group = group;
        s.spec.version = version;
        if with_service {
            s.spec.service = Some(rusternetes_common::resources::APIServiceReference {
                namespace: "ns".into(),
                name: "backend".into(),
                port: None,
            });
        }
        s
    }

    #[derive(Default)]
    struct FakeLister {
        present: Mutex<BTreeMap<String, APIService>>,
        failing: AtomicBool,
    }
    #[async_trait]
    impl APIServiceClient for FakeLister {
        async fn get(&self, name: &str) -> Result<Option<APIService>, ClientError> {
            if self.failing.load(Ordering::SeqCst) {
                return Err(ClientError::Other("down".into()));
            }
            Ok(self.present.lock().unwrap().get(name).cloned())
        }
        async fn list_names(&self) -> Result<Vec<String>, ClientError> {
            if self.failing.load(Ordering::SeqCst) {
                return Err(ClientError::Other("down".into()));
            }
            Ok(self.present.lock().unwrap().keys().cloned().collect())
        }
        async fn create(&self, _: &APIService) -> Result<(), ClientError> {
            unreachable!()
        }
        async fn update(&self, _: &APIService) -> Result<(), ClientError> {
            unreachable!()
        }
        async fn delete(&self, _: &str, _: &str) -> Result<(), ClientError> {
            unreachable!()
        }
    }

    fn lister_with(names: &[&str]) -> Arc<FakeLister> {
        let l = Arc::new(FakeLister::default());
        for n in names {
            l.present
                .lock()
                .unwrap()
                .insert(n.to_string(), svc(n, true));
        }
        l
    }

    fn controller_over(
        lister: Arc<FakeLister>,
    ) -> (
        Arc<APIServiceRegistrationController>,
        Arc<AggregatorHandlerManager>,
        Arc<DiscoveryManager>,
        Arc<MergedDiscovery>,
    ) {
        let merged = Arc::new(MergedDiscovery::default());
        let dm = DiscoveryManager::new(merged.clone(), FnHandler::status(503));
        let mgr = Arc::new(AggregatorHandlerManager::new(Some(dm.clone())));
        let c = Arc::new(APIServiceRegistrationController::new(lister, mgr.clone()));
        (c, mgr, dm, merged)
    }

    /// `sync`: a present APIService is added, an absent one removed.
    #[tokio::test]
    async fn sync_adds_present_and_removes_absent() {
        let lister = lister_with(&["v1.apps.example.com"]);
        let (c, mgr, _, _) = controller_over(lister.clone());
        c.sync("v1.apps.example.com").await.unwrap();
        assert_eq!(
            mgr.registered(),
            BTreeSet::from(["v1.apps.example.com".to_string()])
        );
        lister.present.lock().unwrap().clear();
        c.sync("v1.apps.example.com").await.unwrap();
        assert!(mgr.registered().is_empty());
        assert!(mgr.handled_group_versions().is_empty());
    }

    /// `AddAPIService` mounts a `proxyHandler` at `/apis/<group>/<version>`
    /// and, for the first version of a group, the `apiGroupHandler`;
    /// re-adding updates the existing handler in place; `RemoveAPIService`
    /// unmounts the version, and the group handler with its last version.
    #[tokio::test]
    async fn add_and_remove_mount_and_unmount_handlers() {
        use rusternetes_api_server::aggregator_mux::{ApiGroupResponse, ProxyDecision};
        let (_, mgr, _, _) = controller_over(lister_with(&[]));
        assert!(mgr.proxy_handler("g.example.com", "v1").is_none());
        assert!(mgr.serve_api_group("g.example.com").is_none());

        let mut v1 = svc("v1.g.example.com", true);
        mgr.add_api_service(&v1).unwrap();
        let h = mgr.proxy_handler("g.example.com", "v1").unwrap();
        // Not Available yet: handler_proxy.go answers 503.
        assert_eq!(h.decide(), ProxyDecision::ServiceUnavailable);
        mgr.add_api_service(&svc("v2.g.example.com", true)).unwrap();
        let ApiGroupResponse::Group(g) = mgr.serve_api_group("g.example.com").unwrap() else {
            panic!("expected group");
        };
        assert_eq!(g["versions"].as_array().unwrap().len(), 2);

        // An update reaches the SAME handler (updateAPIService), not a new one.
        v1.status.conditions.push(
            rusternetes_common::resources::apiregistration::APIServiceCondition {
                type_: "Available".into(),
                status: "True".into(),
                ..Default::default()
            },
        );
        mgr.add_api_service(&v1).unwrap();
        assert!(Arc::ptr_eq(
            &h,
            &mgr.proxy_handler("g.example.com", "v1").unwrap()
        ));
        assert!(matches!(h.decide(), ProxyDecision::Proxy(_)));

        mgr.remove_api_service("v1.g.example.com");
        assert!(mgr.proxy_handler("g.example.com", "v1").is_none());
        assert!(mgr.serve_api_group("g.example.com").is_some());
        mgr.remove_api_service("v2.g.example.com");
        assert!(mgr.proxy_handler("g.example.com", "v2").is_none());
        assert!(mgr.serve_api_group("g.example.com").is_none());
    }

    /// `Run`: nothing is signalled until the initial sync succeeds; it retries.
    #[tokio::test(start_paused = true)]
    async fn run_signals_only_after_every_apiservice_is_installed() {
        let lister = lister_with(&["v1.a.example.com", "v1beta1.a.example.com"]);
        lister.failing.store(true, Ordering::SeqCst);
        let (c, mgr, _, _) = controller_over(lister.clone());
        let (_stop_tx, stop_rx) = watch::channel(false);
        let (synced_tx, synced_rx) = watch::channel(false);
        tokio::spawn(c.run(stop_rx, synced_tx));
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!*synced_rx.borrow(), "must not signal while listing fails");
        assert!(mgr.registered().is_empty());
        lister.failing.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(*synced_rx.borrow());
        assert_eq!(mgr.registered().len(), 2);
        assert_eq!(
            mgr.handled_group_versions()["a.example.com"],
            BTreeSet::from(["v1".to_string(), "v1beta1".to_string()])
        );
    }

    /// An informer event after the initial sync is processed by the worker.
    #[tokio::test]
    async fn worker_applies_informer_events() {
        let lister = lister_with(&[]);
        let (c, mgr, _, _) = controller_over(lister.clone());
        let (_stop_tx, stop_rx) = watch::channel(false);
        let (synced_tx, mut synced_rx) = watch::channel(false);
        tokio::spawn(c.clone().run(stop_rx, synced_tx));
        synced_rx.wait_for(|s| *s).await.unwrap();
        let s = svc("v1.late.example.com", true);
        lister
            .present
            .lock()
            .unwrap()
            .insert(s.metadata.name.clone(), s.clone());
        c.on_add(&s);
        for _ in 0..200 {
            if !mgr.registered().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(mgr.registered().len(), 1);
        lister.present.lock().unwrap().clear();
        c.on_delete(&s);
        for _ in 0..200 {
            if mgr.registered().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(mgr.registered().is_empty());
    }

    /// `TestInitialRunHasAllAPIServices`: when discovery is ready, every
    /// APIService is present, and one that could not sync is `Stale`.
    #[tokio::test]
    async fn discovery_initial_run_has_all_apiservices_stale() {
        let merged = Arc::new(MergedDiscovery::default());
        let dm = DiscoveryManager::new(merged.clone(), FnHandler::status(503));
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let (_stop_tx, stop_rx) = watch::channel(false);
        let (tx, mut rx) = watch::channel(false);
        tokio::spawn(dm.clone().run(stop_rx, Some(tx)));
        rx.wait_for(|s| *s).await.unwrap();
        let groups = merged.groups();
        assert_eq!(groups.len(), 1);
        let v = &groups["stable.example.com"]["v1"];
        assert_eq!(v.freshness, DiscoveryFreshness::Stale);
        // apiregistration.k8s.io sorts first.
        assert_eq!(
            merged.priority("apiregistration.k8s.io", "v1"),
            Some((API_REGISTRATION_GROUP_PRIORITY, 0))
        );
    }

    fn v2_doc(groups: &[(&str, &[&str])]) -> serde_json::Value {
        serde_json::json!({
            "kind": "APIGroupDiscoveryList",
            "apiVersion": "apidiscovery.k8s.io/v2",
            "items": groups.iter().map(|(g, vs)| serde_json::json!({
                "metadata": {"name": g},
                "versions": vs.iter().map(|v| serde_json::json!({
                    "version": v,
                    "freshness": "Current",
                    "resources": [{
                        "resource": "foos",
                        "responseKind": {"group": g, "version": v, "kind": "Foo"},
                        "scope": "Cluster",
                        "verbs": ["get"],
                    }],
                })).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        })
    }

    fn ok(content_type: &str, body: &serde_json::Value, etag: Option<&str>) -> HandlerResponse {
        HandlerResponse {
            status: 200,
            content_type: Some(content_type.to_string()),
            etag: etag.map(str::to_string),
            body: body.to_string().into_bytes(),
        }
    }

    /// A closure-backed [`AggregatorHandler`] that records every request.
    struct FnHandler {
        f: Box<dyn Fn(&HandlerRequest) -> HandlerResponse + Send + Sync>,
        delay: Duration,
        seen: Mutex<Vec<HandlerRequest>>,
    }
    impl FnHandler {
        fn new(
            f: impl Fn(&HandlerRequest) -> HandlerResponse + Send + Sync + 'static,
        ) -> Arc<Self> {
            Self::slow(Duration::ZERO, f)
        }
        fn slow(
            delay: Duration,
            f: impl Fn(&HandlerRequest) -> HandlerResponse + Send + Sync + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                f: Box::new(f),
                delay,
                seen: Mutex::new(Vec::new()),
            })
        }
        fn status(code: u16) -> Arc<Self> {
            Self::new(move |_| HandlerResponse {
                status: code,
                ..Default::default()
            })
        }
        fn paths(&self) -> Vec<String> {
            self.seen
                .lock()
                .unwrap()
                .iter()
                .map(|r| r.path.clone())
                .collect()
        }
    }
    #[async_trait]
    impl AggregatorHandler for FnHandler {
        async fn serve(&self, _: &str, _: &str, req: HandlerRequest) -> HandlerResponse {
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            let r = (self.f)(&req);
            self.seen.lock().unwrap().push(req);
            r
        }
    }

    const V2_CT: &str = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList";
    const V2BETA1_CT: &str =
        "application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList";

    async fn run_to_idle(dm: &Arc<DiscoveryManager>) -> watch::Sender<bool> {
        let (stop_tx, stop_rx) = watch::channel(false);
        tokio::spawn(dm.clone().run(stop_rx, None));
        dm.wait_for_queue_complete().await;
        stop_tx
    }

    /// `TestWaitForSync`: `Run` blocks on the slow backend before signalling,
    /// and a reachable one is `Current` with its resources.
    #[tokio::test]
    async fn discovery_run_waits_for_slow_backend() {
        let merged = Arc::new(MergedDiscovery::default());
        let dm = DiscoveryManager::new(
            merged.clone(),
            FnHandler::slow(Duration::from_millis(300), |_| {
                ok(V2_CT, &v2_doc(&[("stable.example.com", &["v1"])]), None)
            }),
        );
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let (_stop_tx, stop_rx) = watch::channel(false);
        let (tx, rx) = watch::channel(false);
        let mut rx2 = rx.clone();
        tokio::spawn(dm.clone().run(stop_rx, Some(tx)));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!*rx.borrow(), "signalled before the backend answered");
        rx2.wait_for(|s| *s).await.unwrap();
        let v = &merged.groups()["stable.example.com"]["v1"];
        assert_eq!(v.freshness, DiscoveryFreshness::Current);
        assert_eq!(v.resources.len(), 1);
    }

    /// `TestRemoveAPIService`: removed APIServices leave no group behind.
    #[tokio::test]
    async fn discovery_remove_api_service_drops_group() {
        let merged = Arc::new(MergedDiscovery::default());
        let dm = DiscoveryManager::new(merged.clone(), FnHandler::status(503));
        let a = svc("v1.stable.example.com", true);
        let b = svc("v2.stable.example.com", true);
        dm.add_api_service(&a);
        dm.add_api_service(&b);
        let (_stop_tx, stop_rx) = watch::channel(false);
        tokio::spawn(dm.clone().run(stop_rx, None));
        dm.wait_for_queue_complete().await;
        assert_eq!(merged.groups()["stable.example.com"].len(), 2);
        dm.remove_api_service("v1.stable.example.com");
        dm.remove_api_service("v2.stable.example.com");
        dm.wait_for_queue_complete().await;
        assert!(merged.groups().is_empty(), "{:?}", merged.groups());
    }

    /// A local APIService (no `spec.service`) is not tracked by the manager.
    #[test]
    fn local_apiservice_is_not_tracked_by_discovery() {
        let dm =
            DiscoveryManager::new(Arc::new(MergedDiscovery::default()), FnHandler::status(503));
        dm.add_api_service(&svc("v1.apps", false));
        assert!(dm.api_services.lock().unwrap().is_empty());
    }

    /// apiserver.go:381-419: the discovery hook blocks on
    /// `apiServiceRegistrationControllerInitiated`; neither hook finishes until
    /// the registration controller has installed every APIService, then both do.
    #[tokio::test(start_paused = true)]
    async fn hooks_finish_only_after_registration_is_initiated() {
        let lister = lister_with(&["v1.a.example.com"]);
        lister.failing.store(true, Ordering::SeqCst);
        let (c, mgr, dm, merged) = controller_over(lister.clone());
        let reg = PostStartHooks::new();
        let (_stop_tx, stop_rx) = watch::channel(false);
        register_apiservice_hooks_with(&reg, c, dm, stop_rx);
        assert_eq!(
            reg.checks()
                .iter()
                .map(|c| c.0.clone())
                .collect::<Vec<_>>()
                .len(),
            2
        );
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(
            reg.check(APISERVICE_REGISTRATION_HOOK),
            Some(Err("not finished"))
        );
        assert_eq!(
            reg.check(APISERVICE_DISCOVERY_HOOK),
            Some(Err("not finished"))
        );
        assert!(merged.groups().is_empty());

        lister.failing.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(reg.check(APISERVICE_REGISTRATION_HOOK), Some(Ok(())));
        assert_eq!(reg.check(APISERVICE_DISCOVERY_HOOK), Some(Ok(())));
        assert_eq!(mgr.registered().len(), 1);
        // The discovery hook returns only once every APIService is in the
        // merged document.
        assert!(merged.groups().contains_key("a.example.com"));
    }

    /// `TestBasic`: one backend serving several groups feeds every APIService
    /// that points at it; each GV gets only its own entry, `Current`.
    #[tokio::test]
    async fn discovery_basic_merges_groups_from_backend() {
        let merged = Arc::new(MergedDiscovery::default());
        let h = FnHandler::new(|r| {
            assert_eq!(r.path, "/apis");
            assert_eq!(r.user, AGGREGATOR_USER);
            assert_eq!(r.groups, vec![AGGREGATOR_GROUP.to_string()]);
            assert_eq!(r.accept, format!("{ACCEPT_V2},{ACCEPT_V2_BETA1}"));
            ok(
                V2_CT,
                &v2_doc(&[("a.example.com", &["v1", "v2"]), ("b.example.com", &["v1"])]),
                Some("\"e1\""),
            )
        });
        let dm = DiscoveryManager::new(merged.clone(), h.clone());
        for n in ["v1.a.example.com", "v2.a.example.com", "v1.b.example.com"] {
            dm.add_api_service(&svc(n, true));
        }
        let _stop = run_to_idle(&dm).await;
        let groups = merged.groups();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups["a.example.com"].len(), 2);
        for vs in groups.values() {
            for v in vs.values() {
                assert_eq!(v.freshness, DiscoveryFreshness::Current);
                assert_eq!(v.resources.len(), 1);
            }
        }
    }

    /// `TestServiceGC`: re-pointing an APIService at another service purges the
    /// cache entry of the old one.
    #[tokio::test]
    async fn discovery_service_gc_purges_unused_cache_entries() {
        let h = FnHandler::new(|_| ok(V2_CT, &v2_doc(&[("stable.example.com", &["v1"])]), None));
        let dm = DiscoveryManager::new(Arc::new(MergedDiscovery::default()), h);
        let _stop = run_to_idle(&dm).await;
        dm.add_api_service(&svc("v1.stable.example.com", true));
        dm.wait_for_queue_complete().await;
        assert_eq!(dm.cached_results.lock().unwrap().len(), 1);
        let mut changed = svc("v1.stable.example.com", true);
        changed.spec.service.as_mut().unwrap().name = "backend-changed".into();
        dm.add_api_service(&changed);
        dm.wait_for_queue_complete().await;
        let cache = dm.cached_results.lock().unwrap();
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.keys().next().unwrap().name, "backend-changed");
    }

    /// `TestV2Beta1Skew`: a backend that only serves v2beta1 is still read,
    /// and nil `responseKind`s are defaulted.
    #[tokio::test]
    async fn discovery_accepts_v2beta1_backend() {
        let merged = Arc::new(MergedDiscovery::default());
        let h = FnHandler::new(|_| {
            let mut d = v2_doc(&[("stable.example.com", &["v1"])]);
            d["apiVersion"] = "apidiscovery.k8s.io/v2beta1".into();
            d["items"][0]["versions"][0]["resources"][0]["responseKind"] = serde_json::Value::Null;
            ok(V2BETA1_CT, &d, None)
        });
        let dm = DiscoveryManager::new(merged.clone(), h);
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let _stop = run_to_idle(&dm).await;
        let v = &merged.groups()["stable.example.com"]["v1"];
        assert_eq!(v.freshness, DiscoveryFreshness::Current);
        assert_eq!(v.resources[0]["responseKind"], serde_json::json!({}));
    }

    /// `TestDirty`: the handler is pinged for a newly added APIService.
    #[tokio::test]
    async fn discovery_pings_handler_for_dirty_apiservice() {
        let h = FnHandler::new(|_| ok(V2_CT, &v2_doc(&[("stable.example.com", &["v1"])]), None));
        let dm = DiscoveryManager::new(Arc::new(MergedDiscovery::default()), h.clone());
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let _stop = run_to_idle(&dm).await;
        assert_eq!(h.paths(), vec!["/apis".to_string()]);
    }

    fn legacy_handler(root: HandlerResponse) -> Arc<FnHandler> {
        FnHandler::new(move |r| match r.path.as_str() {
            "/apis" => root.clone(),
            "/apis/stable.example.com/v1" => HandlerResponse {
                status: 200,
                content_type: Some("application/json".into()),
                etag: None,
                body: serde_json::json!({
                    "kind": "APIResourceList",
                    "groupVersion": "stable.example.com/v1",
                    "resources": [{
                        "name": "foos", "singularName": "foo", "namespaced": false,
                        "group": "stable.example.com", "version": "v1", "kind": "Foo",
                        "verbs": ["get", "list"], "categories": ["all"],
                    }],
                })
                .to_string()
                .into_bytes(),
            },
            p => panic!("unexpected request sent to {p}"),
        })
    }

    /// `TestLegacyFallback`: every kind of unacceptable root response falls
    /// back to `/apis/{group}/{version}`; the result is `Current`, converted,
    /// and NOT cached (`TestLegacyFallbackNoCache`).
    #[tokio::test]
    async fn discovery_legacy_fallback_for_unacceptable_root() {
        let roots = [
            HandlerResponse {
                status: 406,
                ..Default::default()
            },
            HandlerResponse {
                status: 404,
                ..Default::default()
            },
            HandlerResponse {
                status: 200,
                content_type: Some("application/json".into()),
                ..Default::default()
            },
            HandlerResponse {
                status: 200,
                content_type: Some(
                    "application/json;g=apidiscovery.k8s.io;v=v1alpha1;as=APIGroupDiscoveryList"
                        .into(),
                ),
                ..Default::default()
            },
        ];
        for root in roots {
            let merged = Arc::new(MergedDiscovery::default());
            let dm = DiscoveryManager::new(merged.clone(), legacy_handler(root));
            dm.add_api_service(&svc("v1.stable.example.com", true));
            let _stop = run_to_idle(&dm).await;
            let v = &merged.groups()["stable.example.com"]["v1"];
            assert_eq!(v.freshness, DiscoveryFreshness::Current);
            assert_eq!(v.resources.len(), 1);
            assert_eq!(v.resources[0]["resource"], "foos");
            assert_eq!(v.resources[0]["scope"], "Cluster");
            assert_eq!(v.resources[0]["singularResource"], "foo");
            assert_eq!(v.resources[0]["categories"], serde_json::json!(["all"]));
            assert_eq!(v.resources[0]["responseKind"]["kind"], "Foo");
            assert!(dm.cached_results.lock().unwrap().is_empty());
        }
    }

    /// A legacy failure marks the GV `Stale`.
    #[tokio::test]
    async fn discovery_legacy_failure_is_stale() {
        let merged = Arc::new(MergedDiscovery::default());
        let h = FnHandler::new(|_| HandlerResponse {
            status: 404,
            ..Default::default()
        });
        let dm = DiscoveryManager::new(merged.clone(), h.clone());
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let _stop = run_to_idle(&dm).await;
        let v = &merged.groups()["stable.example.com"]["v1"];
        assert_eq!(v.freshness, DiscoveryFreshness::Stale);
        assert!(v.resources.is_empty());
        assert_eq!(
            h.paths()[..2],
            [
                "/apis".to_string(),
                "/apis/stable.example.com/v1".to_string()
            ]
        );
    }

    /// `TestAPIServiceStale`: a 503 backend is listed `Stale` with no resources.
    #[tokio::test]
    async fn discovery_503_backend_is_stale_and_empty() {
        let merged = Arc::new(MergedDiscovery::default());
        let h = FnHandler::status(503);
        let dm = DiscoveryManager::new(merged.clone(), h.clone());
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let _stop = run_to_idle(&dm).await;
        let v = &merged.groups()["stable.example.com"]["v1"];
        assert_eq!(v.freshness, DiscoveryFreshness::Stale);
        assert!(v.resources.is_empty());
        // 503 is a hard error: no legacy fallback request.
        // (Run's initial sync and the dirty queue each try once.)
        assert!(h.paths().iter().all(|p| p == "/apis"), "{:?}", h.paths());
    }

    /// `http.TimeoutHandler(..., 5s)`: a backend that never answers is `Stale`.
    #[tokio::test(start_paused = true)]
    async fn discovery_timeout_is_stale() {
        let merged = Arc::new(MergedDiscovery::default());
        let h = FnHandler::slow(Duration::from_secs(60), |_| {
            ok(V2_CT, &v2_doc(&[("stable.example.com", &["v1"])]), None)
        });
        let dm = DiscoveryManager::new(merged.clone(), h);
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let (_stop_tx, stop_rx) = watch::channel(false);
        let (tx, mut rx) = watch::channel(false);
        tokio::spawn(dm.clone().run(stop_rx, Some(tx)));
        rx.wait_for(|s| *s).await.unwrap();
        assert_eq!(
            merged.groups()["stable.example.com"]["v1"].freshness,
            DiscoveryFreshness::Stale
        );
    }

    /// `TestNotModified`: once an etag is cached, a re-sync after the APIService
    /// is marked dirty sends `If-None-Match`; a 304 keeps the cached resources.
    #[tokio::test]
    async fn discovery_not_modified_keeps_cached_document() {
        let merged = Arc::new(MergedDiscovery::default());
        let h = FnHandler::new(|r| match &r.if_none_match {
            Some(e) if e == "\"e1\"" => HandlerResponse {
                status: 304,
                ..Default::default()
            },
            Some(e) => panic!("unexpected etag {e}"),
            None => ok(
                V2_CT,
                &v2_doc(&[("stable.example.com", &["v1"])]),
                Some("\"e1\""),
            ),
        });
        let dm = DiscoveryManager::new(merged.clone(), h.clone());
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let _stop = run_to_idle(&dm).await;
        assert_eq!(h.seen.lock().unwrap().len(), 1);
        // Mark dirty again (as the minute poll / an APIService update does).
        dm.add_api_service(&svc("v1.stable.example.com", true));
        dm.wait_for_queue_complete().await;
        {
            let seen = h.seen.lock().unwrap();
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[1].if_none_match.as_deref(), Some("\"e1\""));
        }
        let v = &merged.groups()["stable.example.com"]["v1"];
        assert_eq!(v.freshness, DiscoveryFreshness::Current);
        assert_eq!(v.resources.len(), 1);
    }

    /// The cache rule: a result fetched after the APIService was marked dirty is
    /// reused, so syncing again does not contact the backend.
    #[tokio::test]
    async fn discovery_fresh_cache_is_not_refetched() {
        let h = FnHandler::new(|_| {
            ok(
                V2_CT,
                &v2_doc(&[("a.example.com", &["v1"]), ("b.example.com", &["v1"])]),
                None,
            )
        });
        let dm = DiscoveryManager::new(Arc::new(MergedDiscovery::default()), h.clone());
        dm.add_api_service(&svc("v1.a.example.com", true));
        let _stop = run_to_idle(&dm).await;
        let n = h.seen.lock().unwrap().len();
        dm.sync_api_service("v1.a.example.com").await.unwrap();
        assert_eq!(h.seen.lock().unwrap().len(), n);
    }
}
