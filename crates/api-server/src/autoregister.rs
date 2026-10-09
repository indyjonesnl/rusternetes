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
use rusternetes_storage::{build_key, build_prefix, Storage};

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
    pub fn add_api_service_to_sync_on_start(&self, in_: &APIService) {
        self.add_api_service_to_sync_typed(in_, MANAGE_ON_START);
    }

    /// `AddAPIServiceToSync`.
    pub fn add_api_service_to_sync(&self, in_: &APIService) {
        self.add_api_service_to_sync_typed(in_, MANAGE_CONTINUOUSLY);
    }

    /// `addAPIServiceToSync`: label the copy and enqueue its name.
    fn add_api_service_to_sync_typed(&self, in_: &APIService, sync_type: &str) {
        let mut api_service = in_.clone();
        api_service
            .metadata
            .labels
            .get_or_insert_with(HashMap::new)
            .insert(
                AUTO_REGISTER_MANAGED_LABEL.to_string(),
                sync_type.to_string(),
            );
        let name = api_service.metadata.name.clone();
        self.api_services_to_sync
            .lock()
            .unwrap()
            .insert(name.clone(), api_service);
        self.queue.lock().unwrap().insert(name);
    }

    /// `RemoveAPIServiceToSync`.
    pub fn remove_api_service_to_sync(&self, name: &str) {
        self.api_services_to_sync.lock().unwrap().remove(name);
        self.queue.lock().unwrap().insert(name.to_string());
    }

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

    /// `checkAPIService` (autoregister_controller.go:~207-288): the decision
    /// table of current vs desired, with the synced-once and present-at-start
    /// enforcement.
    pub async fn check_api_service(
        &self,
        client: &dyn APIServiceClient,
        name: &str,
    ) -> Result<(), ClientError> {
        let desired = self.get_api_service_to_sync(name);
        let curr = client.get(name).await;

        // if we've never synced this service successfully, record a successful sync.
        let has_synced = self.synced_successfully.lock().unwrap().contains(name);
        let result = self
            .decide_and_apply(client, name, desired.as_ref(), curr, has_synced)
            .await;
        if !has_synced && result.is_ok() {
            self.synced_successfully
                .lock()
                .unwrap()
                .insert(name.to_string());
        }
        result
    }

    async fn decide_and_apply(
        &self,
        client: &dyn APIServiceClient,
        name: &str,
        desired: Option<&APIService>,
        curr: Result<Option<APIService>, ClientError>,
        has_synced: bool,
    ) -> Result<(), ClientError> {
        // we had a real error, just return it (1A,1B,1C)
        let curr = curr?;
        // we don't have an entry and we don't want one (2A)
        if curr.is_none() && desired.is_none() {
            return Ok(());
        }
        // the local object only wants to sync on start and has already synced
        // (2B,5B,6B "once" enforcement)
        if is_automanaged_on_start(desired) && has_synced {
            return Ok(());
        }
        // we don't have an entry and we do want one (2B,2C)
        let Some(curr) = curr else {
            return match client.create(desired.expect("checked above")).await {
                // created in the meantime, we'll get called again
                Err(ClientError::AlreadyExists) => Ok(()),
                other => other,
            };
        };
        // we aren't trying to manage this APIService (3A,3B,3C)
        if !is_automanaged(Some(&curr)) {
            return Ok(());
        }
        // the remote object only wants to sync on start, but was added after
        // we started (4A,4B,4C)
        if is_automanaged_on_start(Some(&curr))
            && !self.api_services_at_start.lock().unwrap().contains(name)
        {
            return Ok(());
        }
        // the remote object only wants to sync on start and has already
        // synced (5A,5B,5C "once" enforcement)
        if is_automanaged_on_start(Some(&curr)) && has_synced {
            return Ok(());
        }
        let Some(desired) = desired else {
            // we have a spurious APIService that we're managing, delete it (5A,6A)
            return match client.delete(&curr.metadata.name, &curr.metadata.uid).await {
                // deleted or changed in the meantime, we'll get called again
                Err(ClientError::NotFound | ClientError::Conflict) => Ok(()),
                other => other,
            };
        };
        // if the specs already match, nothing for us to do
        if curr.spec == desired.spec {
            return Ok(());
        }
        // we have an entry and we have a desired, now we deconflict. Only a
        // few fields matter. (5B,5C,6B,6C)
        let mut api_service = curr;
        api_service.spec = desired.spec.clone();
        match client.update(&api_service).await {
            Err(ClientError::NotFound | ClientError::Conflict) => Ok(()),
            other => other,
        }
    }

    /// `Run` (minus the informer): record what exists at start, then sync
    /// every known name until `stop` fires. See the module deviation note.
    pub async fn run(
        &self,
        client: &dyn APIServiceClient,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) {
        if let Ok(names) = client.list_names().await {
            self.record_present_at_start(names);
        }
        loop {
            let mut names: BTreeSet<String> = std::mem::take(&mut *self.queue.lock().unwrap());
            names.extend(self.api_services_to_sync.lock().unwrap().keys().cloned());
            if let Ok(listed) = client.list_names().await {
                names.extend(listed);
            }
            for name in names {
                if let Err(e) = self.check_api_service(client, &name).await {
                    tracing::warn!("autoregister: {name} failed with : {e:?}");
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                _ = stop.changed() => return,
            }
        }
    }
}

/// An `APIServiceClient` over the API server's own storage (upstream uses the
/// loopback client; creates stamp the system fields as `FillObjectMetaSystemFields`).
pub struct StorageAPIServiceClient<S: Storage>(pub std::sync::Arc<S>);

fn map_err(e: rusternetes_common::Error) -> ClientError {
    match e {
        rusternetes_common::Error::NotFound(_) => ClientError::NotFound,
        rusternetes_common::Error::AlreadyExists(_) => ClientError::AlreadyExists,
        rusternetes_common::Error::Conflict(_) => ClientError::Conflict,
        other => ClientError::Other(other.to_string()),
    }
}

#[async_trait]
impl<S: Storage + 'static> APIServiceClient for StorageAPIServiceClient<S> {
    async fn get(&self, name: &str) -> Result<Option<APIService>, ClientError> {
        match self
            .0
            .get::<APIService>(&build_key("apiservices", None, name))
            .await
        {
            Ok(a) => Ok(Some(a)),
            Err(rusternetes_common::Error::NotFound(_)) => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }
    async fn list_names(&self) -> Result<Vec<String>, ClientError> {
        let all = self
            .0
            .list::<APIService>(&build_prefix("apiservices", None))
            .await
            .map_err(map_err)?;
        Ok(all.into_iter().map(|a| a.metadata.name).collect())
    }
    async fn create(&self, apiservice: &APIService) -> Result<(), ClientError> {
        let mut a = apiservice.clone();
        if a.api_version.is_empty() {
            a.api_version = "apiregistration.k8s.io/v1".to_string();
        }
        if a.kind.is_empty() {
            a.kind = "APIService".to_string();
        }
        crate::registry::rest::fill_object_meta_system_fields(&mut a.metadata);
        self.0
            .create(&build_key("apiservices", None, &a.metadata.name), &a)
            .await
            .map(|_| ())
            .map_err(map_err)
    }
    async fn update(&self, apiservice: &APIService) -> Result<(), ClientError> {
        self.0
            .update(
                &build_key("apiservices", None, &apiservice.metadata.name),
                apiservice,
            )
            .await
            .map(|_| ())
            .map_err(map_err)
    }
    async fn delete(&self, name: &str, uid: &str) -> Result<(), ClientError> {
        let key = build_key("apiservices", None, name);
        let current = self.0.get::<APIService>(&key).await.map_err(map_err)?;
        // `Preconditions: NewUIDPreconditions(uid)`
        if current.metadata.uid != uid {
            return Err(ClientError::Conflict);
        }
        self.0.delete(&key).await.map_err(map_err)
    }
}

/// `APIServicePriority`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct APIServicePriority {
    pub group: i32,
    pub version: i32,
}

/// `DefaultGenericAPIServicePriorities` (aggregator.go:317-352), as
/// `(group, version, group priority, version priority)`.
pub const DEFAULT_GENERIC_API_SERVICE_PRIORITIES: &[(&str, &str, i32, i32)] = &[
    ("", "v1", 18000, 1),
    ("events.k8s.io", "v1", 17750, 15),
    ("events.k8s.io", "v1beta1", 17750, 5),
    ("authentication.k8s.io", "v1", 17700, 15),
    ("authentication.k8s.io", "v1beta1", 17700, 9),
    ("authentication.k8s.io", "v1alpha1", 17700, 1),
    ("authorization.k8s.io", "v1", 17600, 15),
    ("certificates.k8s.io", "v1", 17300, 15),
    ("certificates.k8s.io", "v1beta1", 17300, 9),
    ("certificates.k8s.io", "v1alpha1", 17300, 1),
    ("rbac.authorization.k8s.io", "v1", 17000, 15),
    ("apiextensions.k8s.io", "v1", 16700, 15),
    ("admissionregistration.k8s.io", "v1", 16700, 15),
    ("admissionregistration.k8s.io", "v1beta1", 16700, 12),
    ("admissionregistration.k8s.io", "v1alpha1", 16700, 9),
    ("coordination.k8s.io", "v1", 16500, 15),
    ("coordination.k8s.io", "v1beta1", 16500, 13),
    ("coordination.k8s.io", "v1alpha2", 16500, 12),
    ("discovery.k8s.io", "v1", 16200, 15),
    ("discovery.k8s.io", "v1beta1", 16200, 12),
    ("flowcontrol.apiserver.k8s.io", "v1", 16100, 21),
    ("flowcontrol.apiserver.k8s.io", "v1beta3", 16100, 18),
    ("flowcontrol.apiserver.k8s.io", "v1beta2", 16100, 15),
    ("flowcontrol.apiserver.k8s.io", "v1beta1", 16100, 12),
    ("flowcontrol.apiserver.k8s.io", "v1alpha1", 16100, 9),
    ("internal.apiserver.k8s.io", "v1alpha1", 16000, 9),
    ("resource.k8s.io", "v1alpha3", 15900, 9),
    ("storagemigration.k8s.io", "v1beta1", 15800, 9),
];

/// `makeAPIService` (aggregator.go:~262-281): `None` for a group-version
/// without a priority, so a CRD's group-version is never pinned in the list.
pub fn make_api_service(group: &str, version: &str) -> Option<APIService> {
    let (_, _, group_priority, version_priority) = DEFAULT_GENERIC_API_SERVICE_PRIORITIES
        .iter()
        .find(|(g, v, _, _)| *g == group && *v == version)?;
    let mut s = APIService::default();
    s.metadata.name = format!("{version}.{group}");
    s.spec.group = group.to_string();
    s.spec.version = version.to_string();
    s.spec.group_priority_minimum = *group_priority;
    s.spec.version_priority = *version_priority;
    Some(s)
}

/// `apiServicesToRegister` (aggregator.go:354-387): registers on start one
/// APIService per delegate path `/api/v1` or `/apis/<group>/<version>`.
pub fn api_services_to_register(
    listed_paths: &[String],
    registration: &AutoRegisterController,
) -> Vec<APIService> {
    let mut api_services = vec![];
    for curr in listed_paths {
        if curr == "/api/v1" {
            if let Some(s) = make_api_service("", "v1") {
                registration.add_api_service_to_sync_on_start(&s);
                api_services.push(s);
            }
            continue;
        }
        if !curr.starts_with("/apis/") {
            continue;
        }
        let tokens: Vec<&str> = curr.split('/').collect();
        if tokens.len() != 4 {
            continue;
        }
        let Some(s) = make_api_service(tokens[2], tokens[3]) else {
            continue;
        };
        registration.add_api_service_to_sync_on_start(&s);
        api_services.push(s);
    }
    api_services
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
