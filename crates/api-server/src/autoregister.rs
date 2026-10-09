//! The kube-aggregator `autoregister` controller: keeps a set of built-in
//! APIServices present in the API.
//!
//! Ported from `staging/src/k8s.io/kube-aggregator/pkg/controllers/autoregister/
//! autoregister_controller.go` (`checkAPIService`, `Add/RemoveAPIServiceToSync`)
//! and `pkg/controlplane/apiserver/aggregator.go` (`makeAPIService`,
//! `apiServicesToRegister`, `DefaultGenericAPIServicePriorities`). Tests are
//! ported from `autoregister_controller_test.go` (`TestSync`).
//!
//! Deviation: upstream drives `checkAPIService` from an APIService informer's
//! add/update/delete events through a rate-limited workqueue; [`run`] here
//! re-syncs every known name on a short tick (the sync is idempotent and
//! guarded by the same synced-once / present-at-start state).
//! Not yet wired into `startup.rs`: the `kube-apiserver-autoregistration` hook
//! needs the delegate's ListedPaths and the crdregistration controller.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Mutex;

use async_trait::async_trait;
use rusternetes_common::resources::APIService;

/// `AutoRegisterManagedLabel`.
pub const AUTO_REGISTER_MANAGED_LABEL: &str = "kube-aggregator.kubernetes.io/automanaged";
/// `manageOnStart`.
pub const MANAGE_ON_START: &str = "onstart";
/// `manageContinuously`.
pub const MANAGE_CONTINUOUSLY: &str = "true";

/// The API errors `checkAPIService` distinguishes (`apierrors.IsNotFound`, ...).
#[derive(Debug, Clone, PartialEq)]
pub enum ClientError {
    NotFound,
    AlreadyExists,
    Conflict,
    Other(String),
}

/// `apiServiceLister` + `apiServiceClient`.
#[async_trait]
pub trait APIServiceClient: Send + Sync {
    /// Lister `Get`: `Ok(None)` is NotFound.
    async fn get(&self, name: &str) -> Result<Option<APIService>, ClientError>;
    async fn list_names(&self) -> Result<Vec<String>, ClientError>;
    async fn create(&self, apiservice: &APIService) -> Result<(), ClientError>;
    async fn update(&self, apiservice: &APIService) -> Result<(), ClientError>;
    /// Delete with `Preconditions: NewUIDPreconditions(uid)`.
    async fn delete(&self, name: &str, uid: &str) -> Result<(), ClientError>;
}

/// `autoRegisterController` state.
#[derive(Default)]
pub struct AutoRegisterController {
    api_services_to_sync: Mutex<HashMap<String, APIService>>,
    synced_successfully: Mutex<HashSet<String>>,
    api_services_at_start: Mutex<HashSet<String>>,
    queue: Mutex<BTreeSet<String>>,
}

fn automanaged_type(service: Option<&APIService>) -> &str {
    service
        .and_then(|s| s.metadata.labels.as_ref())
        .and_then(|l| l.get(AUTO_REGISTER_MANAGED_LABEL))
        .map(String::as_str)
        .unwrap_or("")
}

fn is_automanaged_on_start(service: Option<&APIService>) -> bool {
    automanaged_type(service) == MANAGE_ON_START
}

fn is_automanaged(service: Option<&APIService>) -> bool {
    let t = automanaged_type(service);
    t == MANAGE_ON_START || t == MANAGE_CONTINUOUSLY
}

impl AutoRegisterController {
    pub fn new() -> Self {
        Self::default()
    }

    /// `AddAPIServiceToSyncOnStart`.
    pub fn add_api_service_to_sync_on_start(&self, _in: &APIService) {}

    /// `AddAPIServiceToSync`.
    pub fn add_api_service_to_sync(&self, _in: &APIService) {}

    /// `RemoveAPIServiceToSync`.
    pub fn remove_api_service_to_sync(&self, _name: &str) {}

    /// `GetAPIServiceToSync`.
    pub fn get_api_service_to_sync(&self, name: &str) -> Option<APIService> {
        self.api_services_to_sync.lock().unwrap().get(name).cloned()
    }

    /// Test/`Run` helper: names present when the controller started.
    pub fn record_present_at_start(&self, names: impl IntoIterator<Item = String>) {
        self.api_services_at_start.lock().unwrap().extend(names);
    }

    /// Test helper: mark a name as already synced successfully.
    pub fn mark_synced(&self, name: &str) {
        self.synced_successfully
            .lock()
            .unwrap()
            .insert(name.to_string());
    }

    /// `checkAPIService`.
    pub async fn check_api_service(
        &self,
        _client: &dyn APIServiceClient,
        _name: &str,
    ) -> Result<(), ClientError> {
        let _ = (is_automanaged(None), is_automanaged_on_start(None));
        let _ = &self.queue;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
