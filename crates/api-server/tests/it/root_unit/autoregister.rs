//! Unit tests moved from `src/autoregister.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use futures::StreamExt;
    pub use rusternetes_api_server::crdregistration::WorkQueue;
    pub use rusternetes_api_server::registry::apiextensions::customresourcedefinition::ControllerRateLimiter;
    pub use rusternetes_common::resources::APIService;
    pub use rusternetes_storage::{build_key, build_prefix, Storage, WatchEvent};
    pub use std::collections::{BTreeSet, HashMap, HashSet};
    pub use std::sync::Arc;
    pub use std::sync::Mutex;
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::autoregister::*;
    use std::sync::Mutex;

    #[derive(Debug, PartialEq, Clone)]
    enum Action {
        Create(APIService),
        Update(APIService),
        Delete(String),
    }

    #[derive(Default)]
    struct Fake {
        present: Mutex<HashMap<String, APIService>>,
        actions: Mutex<Vec<Action>>,
    }

    #[async_trait]
    impl APIServiceClient for Fake {
        async fn get(&self, name: &str) -> Result<Option<APIService>, ClientError> {
            Ok(self.present.lock().unwrap().get(name).cloned())
        }
        async fn list_names(&self) -> Result<Vec<String>, ClientError> {
            Ok(self.present.lock().unwrap().keys().cloned().collect())
        }
        async fn create(&self, a: &APIService) -> Result<(), ClientError> {
            self.actions.lock().unwrap().push(Action::Create(a.clone()));
            Ok(())
        }
        async fn update(&self, a: &APIService) -> Result<(), ClientError> {
            self.actions.lock().unwrap().push(Action::Update(a.clone()));
            Ok(())
        }
        async fn delete(&self, name: &str, _uid: &str) -> Result<(), ClientError> {
            self.actions
                .lock()
                .unwrap()
                .push(Action::Delete(name.to_string()));
            Ok(())
        }
    }

    fn svc(name: &str, label: Option<&str>, group: &str) -> APIService {
        let mut s = APIService::default();
        s.metadata.name = name.to_string();
        s.spec.group = group.to_string();
        if let Some(l) = label {
            s.metadata.labels = Some(HashMap::from([(
                AUTO_REGISTER_MANAGED_LABEL.to_string(),
                l.to_string(),
            )]));
        }
        s
    }
    fn plain(name: &str) -> APIService {
        svc(name, None, "")
    }
    fn managed(name: &str) -> APIService {
        svc(name, Some("true"), "")
    }
    fn on_start(name: &str) -> APIService {
        svc(name, Some("onstart"), "")
    }
    fn modified(name: &str) -> APIService {
        svc(name, Some("true"), "something")
    }

    // apiServicesToRegister/makeAPIService: only listed paths with a known
    // priority are registered, and always on-start.
    #[test]
    fn api_services_to_register_registers_prioritised_paths_on_start() {
        let ctl = AutoRegisterController::new();
        let paths: Vec<String> = [
            "/api",
            "/api/v1",
            "/apis/rbac.authorization.k8s.io/v1",
            "/apis/example.com/v1",
            "/apis/rbac.authorization.k8s.io",
            "/healthz",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let out = api_services_to_register(&paths, &ctl);
        let names: Vec<&str> = out.iter().map(|s| s.metadata.name.as_str()).collect();
        assert_eq!(names, vec!["v1.", "v1.rbac.authorization.k8s.io"]);
        let core = ctl.get_api_service_to_sync("v1.").unwrap();
        assert_eq!(automanaged_type(Some(&core)), "onstart");
        assert_eq!(core.spec.group_priority_minimum, 18000);
        assert_eq!(core.spec.version_priority, 1);
        assert!(ctl.get_api_service_to_sync("v1.example.com").is_none());
    }

    // cmd/kube-apiserver/app/aggregator.go:31 merges over the generic table.
    #[test]
    fn kube_apiserver_priorities_overlay_the_generic_ones() {
        let apps = make_api_service("apps", "v1").unwrap();
        assert_eq!(apps.spec.group_priority_minimum, 17800);
        let r = make_api_service("resource.k8s.io", "v1alpha3").unwrap();
        assert_eq!(
            (r.spec.group_priority_minimum, r.spec.version_priority),
            (16200, 1)
        );
    }

    fn available(mut s: APIService, status: &str) -> APIService {
        s.status
            .conditions
            .push(rusternetes_common::resources::APIServiceCondition {
                type_: "Available".to_string(),
                status: status.to_string(),
                ..Default::default()
            });
        s
    }

    // makeAPIServiceAvailableHealthCheck: pending until each is seen Available.
    #[test]
    fn completion_check_pending_until_all_available() {
        let a = make_api_service("apps", "v1").unwrap();
        let b = make_api_service("", "v1").unwrap();
        let chk = APIServiceAvailableCheck::new(&[a.clone(), b.clone()]);
        assert_eq!(
            chk.check().unwrap_err(),
            "missing APIService: [v1. v1.apps]"
        );
        chk.handle_api_service_change(&available(a.clone(), "False"));
        assert!(chk.check().is_err(), "Available=False does not count");
        chk.handle_api_service_change(&available(plain("zzz"), "True"));
        assert!(chk.check().is_err(), "unrelated APIService ignored");
        chk.handle_api_service_change(&available(a, "True"));
        assert_eq!(chk.check().unwrap_err(), "missing APIService: [v1.]");
        chk.handle_api_service_change(&available(b, "True"));
        assert!(chk.check().is_ok());
    }

    // The hook creates every delegate path's APIService as `onstart`-managed.
    #[tokio::test]
    async fn autoregistration_hook_seeds_listed_apiservices() {
        let storage = std::sync::Arc::new(rusternetes_storage::MemoryStorage::new());
        let paths = vec!["/api/v1".to_string(), "/apis/apps/v1".to_string()];
        let h = spawn_autoregistration(storage.clone(), paths);
        let client = StorageAPIServiceClient(storage.clone());
        let mut got = None;
        for _ in 0..50 {
            if let Ok(Some(a)) = client.get("v1.apps").await {
                got = Some(a);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let a = got.expect("v1.apps seeded");
        assert_eq!(automanaged_type(Some(&a)), "onstart");
        assert!(client.get("v1.").await.unwrap().is_some());
        h.abort();
    }

    #[derive(Clone, Copy)]
    enum Expect {
        Nothing,
        Create(&'static str),
        Update,
        Delete,
    }

    struct Case {
        name: &'static str,
        present: Vec<APIService>,
        sync: Vec<APIService>,
        sync_on_start: Vec<APIService>,
        del_sync: Vec<&'static str>,
        already_synced: Vec<&'static str>,
        at_start: Vec<&'static str>,
        expect: Expect,
    }

    fn case(name: &'static str, expect: Expect) -> Case {
        Case {
            name,
            present: vec![],
            sync: vec![],
            sync_on_start: vec![],
            del_sync: vec![],
            already_synced: vec![],
            at_start: vec![],
            expect,
        }
    }

    // TestSync (autoregister_controller_test.go:114-263). Upstream's
    // `updateAPIServices` replaces the indexer entry, so the later object wins;
    // the cases here give the final lister state directly.
    #[tokio::test]
    async fn test_sync() {
        let mut cases = vec![];
        let mut c = case(
            "adding an API service which isn't auto-managed does nothing",
            Expect::Nothing,
        );
        c.present = vec![plain("foo")];
        cases.push(c);
        let mut c = case(
            "adding one to auto-register should create",
            Expect::Create("true"),
        );
        c.sync = vec![plain("foo")];
        cases.push(c);
        let mut c = case("duplicate AddAPIServiceToSync don't panic", Expect::Nothing);
        c.present = vec![managed("foo")];
        c.sync = vec![managed("foo"), managed("foo")];
        cases.push(c);
        let mut c = case(
            "duplicate RemoveAPIServiceToSync don't panic",
            Expect::Delete,
        );
        c.present = vec![managed("foo")];
        c.del_sync = vec!["foo", "foo"];
        cases.push(c);
        let mut c = case(
            "removing auto-managed then RemoveAPIService should not touch APIService",
            Expect::Nothing,
        );
        c.present = vec![plain("foo")];
        c.del_sync = vec!["foo"];
        cases.push(c);
        let mut c = case(
            "create managed apiservice without a matching request",
            Expect::Delete,
        );
        c.present = vec![managed("foo")];
        cases.push(c);
        let mut c = case("modifying it should result in stomping", Expect::Update);
        c.present = vec![modified("foo")];
        c.sync = vec![managed("foo")];
        cases.push(c);
        let mut c = case(
            "adding one to auto-register on start should create",
            Expect::Create("onstart"),
        );
        c.sync_on_start = vec![plain("foo")];
        cases.push(c);
        let mut c = case(
            "adding one to auto-register on start already synced should do nothing",
            Expect::Nothing,
        );
        c.sync_on_start = vec![plain("foo")];
        c.already_synced = vec!["foo"];
        cases.push(c);
        let mut c = case(
            "managed onstart apiservice present at start without a matching request should delete",
            Expect::Delete,
        );
        c.present = vec![on_start("foo")];
        c.at_start = vec!["foo"];
        cases.push(c);
        let mut c = case(
            "managed onstart apiservice present at start without a matching request already synced once should no-op",
            Expect::Nothing,
        );
        c.present = vec![on_start("foo")];
        c.at_start = vec!["foo"];
        c.already_synced = vec!["foo"];
        cases.push(c);
        let mut c = case(
            "managed onstart apiservice not present at start without a matching request should no-op",
            Expect::Nothing,
        );
        c.present = vec![on_start("foo")];
        cases.push(c);
        let mut c = case(
            "modifying onstart it should result in stomping",
            Expect::Update,
        );
        c.present = vec![modified("foo")];
        c.sync_on_start = vec![on_start("foo")];
        cases.push(c);
        let mut c = case(
            "modifying onstart already synced should no-op",
            Expect::Nothing,
        );
        c.present = vec![modified("foo")];
        c.sync_on_start = vec![on_start("foo")];
        c.already_synced = vec!["foo"];
        cases.push(c);

        for t in cases {
            let client = Fake::default();
            for s in &t.present {
                client
                    .present
                    .lock()
                    .unwrap()
                    .insert(s.metadata.name.clone(), s.clone());
            }
            let ctl = AutoRegisterController::new();
            for n in &t.already_synced {
                ctl.mark_synced(n);
            }
            ctl.record_present_at_start(t.at_start.iter().map(|s| s.to_string()));
            for s in &t.sync {
                ctl.add_api_service_to_sync(s);
            }
            for s in &t.sync_on_start {
                ctl.add_api_service_to_sync_on_start(s);
            }
            for n in &t.del_sync {
                ctl.remove_api_service_to_sync(n);
            }
            ctl.check_api_service(&client, "foo").await.unwrap();
            let actions = client.actions.lock().unwrap().clone();
            match (t.expect, actions.as_slice()) {
                (Expect::Nothing, []) => {}
                (Expect::Create(label), [Action::Create(a)]) => {
                    assert_eq!(a.metadata.name, "foo", "{}", t.name);
                    assert_eq!(automanaged_type(Some(a)), label, "{}: bad label", t.name);
                }
                (Expect::Update, [Action::Update(a)]) => {
                    assert_eq!(a.metadata.name, "foo", "{}", t.name);
                    assert_eq!(automanaged_type(Some(a)), "true", "{}", t.name);
                    assert_eq!(a.spec.group, "", "{}: spec not stomped", t.name);
                }
                (Expect::Delete, [Action::Delete(n)]) => assert_eq!(n, "foo", "{}", t.name),
                (_, other) => panic!("{}: unexpected actions {other:?}", t.name),
            }
        }
    }

    async fn wait_for(cond: impl Fn() -> bool) {
        for _ in 0..200 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached");
    }

    /// `Add/RemoveAPIServiceToSync` enqueue the name and a worker runs
    /// `checkAPIService` for it: no polling tick involved.
    #[tokio::test]
    async fn worker_syncs_queued_name() {
        let ctl = Arc::new(AutoRegisterController::new());
        let fake = Arc::new(Fake::default());
        ctl.add_api_service_to_sync(&plain("foo"));
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let client: Arc<dyn APIServiceClient> = fake.clone();
        let h = tokio::spawn(ctl.clone().run(client, 2, stop_rx));
        wait_for(|| !fake.actions.lock().unwrap().is_empty()).await;
        assert_eq!(
            fake.actions.lock().unwrap().clone(),
            vec![Action::Create(managed("foo"))]
        );
        // an informer event for a queued name wakes a worker as well
        ctl.add_api_service_to_sync(&plain("bar"));
        wait_for(|| fake.actions.lock().unwrap().len() == 2).await;
        stop_tx.send(true).unwrap();
        h.await.unwrap();
    }

    /// A client whose first `create` fails.
    struct FailFirstCreate {
        inner: Fake,
        failures: Mutex<u32>,
        attempts: Mutex<u32>,
    }

    #[async_trait]
    impl APIServiceClient for FailFirstCreate {
        async fn get(&self, name: &str) -> Result<Option<APIService>, ClientError> {
            self.inner.get(name).await
        }
        async fn list_names(&self) -> Result<Vec<String>, ClientError> {
            self.inner.list_names().await
        }
        async fn create(&self, a: &APIService) -> Result<(), ClientError> {
            *self.attempts.lock().unwrap() += 1;
            let fail = {
                let mut f = self.failures.lock().unwrap();
                let fail = *f > 0;
                if fail {
                    *f -= 1;
                }
                fail
            };
            if fail {
                return Err(ClientError::Other("boom".into()));
            }
            self.inner.create(a).await
        }
        async fn update(&self, a: &APIService) -> Result<(), ClientError> {
            self.inner.update(a).await
        }
        async fn delete(&self, name: &str, uid: &str) -> Result<(), ClientError> {
            self.inner.delete(name, uid).await
        }
    }

    /// An error requeues the key rate-limited (`AddRateLimited`) until the
    /// sync succeeds, then the failure count is forgotten.
    #[tokio::test]
    async fn failed_sync_is_requeued_rate_limited() {
        let ctl = Arc::new(AutoRegisterController::new());
        let client = Arc::new(FailFirstCreate {
            inner: Fake::default(),
            failures: Mutex::new(2),
            attempts: Mutex::new(0),
        });
        ctl.add_api_service_to_sync(&plain("foo"));
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let dyn_client: Arc<dyn APIServiceClient> = client.clone();
        let h = tokio::spawn(ctl.clone().run(dyn_client, 1, stop_rx));
        wait_for(|| !client.inner.actions.lock().unwrap().is_empty()).await;
        assert_eq!(*client.attempts.lock().unwrap(), 3);
        wait_for(|| ctl.rate_limiter.lock().unwrap().num_requeues("foo") == 0).await;
        stop_tx.send(true).unwrap();
        h.await.unwrap();
    }

    /// The APIService watch feeds the workqueue and the completion check.
    #[tokio::test]
    async fn watch_events_drive_queue_and_check() {
        use rusternetes_storage::MemoryStorage;
        let storage = Arc::new(MemoryStorage::new());
        let ctl = Arc::new(AutoRegisterController::new());
        let mut pending = plain("v1.example.com");
        pending.metadata.name = "v1.example.com".into();
        let check = Arc::new(APIServiceAvailableCheck::new(&[pending.clone()]));
        assert!(check.check().is_err());
        spawn_api_service_event_handlers(storage.clone(), ctl.clone(), check.clone()).await;
        let mut available = pending.clone();
        available.status.conditions = vec![serde_json::from_value(serde_json::json!({
            "type": "Available", "status": "True"
        }))
        .unwrap()];
        storage
            .create(
                &build_key("apiservices", None, "v1.example.com"),
                &available,
            )
            .await
            .unwrap();
        wait_for(|| check.check().is_ok()).await;
        assert!(!ctl.queue.is_idle());
    }
}
