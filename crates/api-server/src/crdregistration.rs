//! The crdregistration controller: registers CRD group-versions with the
//! auto APIService registration controller so they stay in sync.
//!
//! Ported from `pkg/controlplane/controller/crdregistration/
//! crdregistration_controller.go`; tests from `crdregistration_controller_test.go`.
//!
//! Deviations: the CRD informer is a storage watch ([`spawn_crd_event_handlers`])
//! feeding the same Add/Update/Delete handlers, and the lister reads storage
//! directly (no local cache), so `handleVersionUpdate` always sees current CRDs.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use rusternetes_common::resources::{APIService, CustomResourceDefinition};
use rusternetes_storage::{build_prefix, Storage, WatchEvent};
use tokio::sync::{watch, Notify};

use crate::registry::apiextensions::customresourcedefinition::ControllerRateLimiter;

/// `AutoAPIServiceRegistration`.
pub trait AutoAPIServiceRegistration: Send + Sync {
    /// `AddAPIServiceToSync`.
    fn add_api_service_to_sync(&self, in_: &APIService);
    /// `RemoveAPIServiceToSync`.
    fn remove_api_service_to_sync(&self, name: &str);
}

/// `crdlisters.CustomResourceDefinitionLister`.
#[async_trait]
pub trait CrdLister: Send + Sync {
    async fn list(&self) -> Result<Vec<CustomResourceDefinition>, String>;
}

/// A `schema.GroupVersion` work-queue key.
type GroupVersion = (String, String);

/// The dedup half of `workqueue.TypedRateLimitingInterface` (`Add`, `Get`,
/// `Done`): a key added while it is being processed is re-queued on `Done`, so
/// one key is never worked by two workers at once
/// (client-go `util/workqueue/queue.go`).
#[derive(Default)]
struct WorkQueue {
    state: Mutex<QueueState>,
    notify: Notify,
}

#[derive(Default)]
struct QueueState {
    queue: VecDeque<GroupVersion>,
    dirty: HashSet<GroupVersion>,
    processing: HashSet<GroupVersion>,
    shutting_down: bool,
}

impl WorkQueue {
    fn add(&self, item: GroupVersion) {
        let mut st = self.state.lock().unwrap();
        if st.shutting_down || st.dirty.contains(&item) {
            return;
        }
        st.dirty.insert(item.clone());
        if st.processing.contains(&item) {
            return;
        }
        st.queue.push_back(item);
        drop(st);
        self.notify.notify_one();
    }

    /// `Get`: blocks for work; `None` once shut down.
    async fn get(&self) -> Option<GroupVersion> {
        loop {
            let notified = self.notify.notified();
            {
                let mut st = self.state.lock().unwrap();
                if st.shutting_down {
                    return None;
                }
                if let Some(item) = st.queue.pop_front() {
                    st.processing.insert(item.clone());
                    st.dirty.remove(&item);
                    return Some(item);
                }
            }
            notified.await;
        }
    }

    fn done(&self, item: &GroupVersion) {
        let mut st = self.state.lock().unwrap();
        st.processing.remove(item);
        if st.dirty.contains(item) {
            st.queue.push_back(item.clone());
            drop(st);
            self.notify.notify_one();
        }
    }

    fn shut_down(&self) {
        self.state.lock().unwrap().shutting_down = true;
        self.notify.notify_waiters();
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.state.lock().unwrap().queue.len()
    }
}

/// `crdRegistrationController`.
pub struct CrdRegistrationController<L: CrdLister> {
    crd_lister: L,
    api_service_registration: Arc<dyn AutoAPIServiceRegistration>,
    synced_initial_set: watch::Sender<bool>,
    /// Keyed by a groupVersion; de-dups and carries the rate-limited requeues.
    queue: WorkQueue,
    rate_limiter: Mutex<ControllerRateLimiter>,
}

impl<L: CrdLister + 'static> CrdRegistrationController<L> {
    /// `NewCRDRegistrationController` (the informer event handlers are
    /// [`Self::on_add`] / [`Self::on_update`] / [`Self::on_delete`]).
    pub fn new(
        crd_lister: L,
        api_service_registration: Arc<dyn AutoAPIServiceRegistration>,
    ) -> Self {
        Self {
            crd_lister,
            api_service_registration,
            synced_initial_set: watch::channel(false).0,
            queue: WorkQueue::default(),
            rate_limiter: Mutex::new(ControllerRateLimiter::new()),
        }
    }

    /// `AddFunc`.
    pub fn on_add(&self, crd: &CustomResourceDefinition) {
        self.enqueue_crd(crd);
    }

    /// `UpdateFunc`: "Enqueue both old and new object to make sure we remove
    /// and add appropriate API services."
    pub fn on_update(&self, old: &CustomResourceDefinition, new: &CustomResourceDefinition) {
        self.enqueue_crd(old);
        self.enqueue_crd(new);
    }

    /// `DeleteFunc` (the `DeletedFinalStateUnknown` tombstone is unrepresentable
    /// here: a storage watch delivers the previous object).
    pub fn on_delete(&self, crd: &CustomResourceDefinition) {
        self.enqueue_crd(crd);
    }

    /// `Run`: sync the caches, process each listed CRD once, release
    /// `WaitForInitialSync`, then run `workers` workers until `stop` fires.
    pub async fn run(self: Arc<Self>, workers: usize, mut stop: watch::Receiver<bool>) {
        tracing::info!("Starting crd-autoregister controller");
        // `WaitForNamedCacheSync("crd-autoregister", stopCh, c.crdSynced)`
        let crds = loop {
            match self.crd_lister.list().await {
                Ok(crds) => break crds,
                Err(e) => tracing::debug!("crd-autoregister: waiting for cache sync: {e}"),
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                _ = stop.changed() => return,
            }
        };
        // process each item in the list once
        for crd in &crds {
            for version in &crd.spec.versions {
                if let Err(e) = self
                    .handle_version_update(&crd.spec.group, &version.name)
                    .await
                {
                    tracing::error!("crd-autoregister: {e}");
                }
            }
        }
        self.synced_initial_set.send_replace(true);

        let mut tasks = Vec::new();
        for _ in 0..workers {
            let c = self.clone();
            tasks.push(tokio::spawn(async move {
                while c.process_next_work_item().await {}
            }));
        }
        // wait until we're told to stop
        while !*stop.borrow() {
            if stop.changed().await.is_err() {
                break;
            }
        }
        self.queue.shut_down();
        for t in tasks {
            let _ = t.await;
        }
        tracing::info!("Shutting down crd-autoregister controller");
    }

    /// `WaitForInitialSync`.
    pub async fn wait_for_initial_sync(&self) {
        let mut rx = self.synced_initial_set.subscribe();
        let _ = rx.wait_for(|synced| *synced).await;
    }

    /// `processNextWorkItem`: false when it's time to quit.
    async fn process_next_work_item(self: &Arc<Self>) -> bool {
        let Some(key) = self.queue.get().await else {
            return false;
        };
        let result = self.handle_version_update(&key.0, &key.1).await;
        self.queue.done(&key);
        let name = format!("{}/{}", key.0, key.1);
        match result {
            Ok(()) => self.rate_limiter.lock().unwrap().forget(&name),
            Err(e) => {
                tracing::error!("crd-autoregister: {}/{} failed with : {e}", key.0, key.1);
                // `AddRateLimited`
                let delay = self
                    .rate_limiter
                    .lock()
                    .unwrap()
                    .when_at(&name, tokio::time::Instant::now());
                let c = self.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    c.queue.add(key);
                });
            }
        }
        true
    }

    /// `enqueueCRD`.
    fn enqueue_crd(&self, crd: &CustomResourceDefinition) {
        for version in &crd.spec.versions {
            self.queue
                .add((crd.spec.group.clone(), version.name.clone()));
        }
    }

    /// `handleVersionUpdate`.
    pub async fn handle_version_update(&self, group: &str, version: &str) -> Result<(), String> {
        let api_service_name = format!("{version}.{group}");

        // check all CRDs.  There shouldn't that many, but if we have problems
        // later we can index them
        let crds = self.crd_lister.list().await?;
        for crd in &crds {
            if crd.spec.group != group {
                continue;
            }
            for v in &crd.spec.versions {
                if v.name != version || !v.served {
                    continue;
                }
                let mut s = APIService::default();
                s.metadata.name = api_service_name;
                s.spec.group = group.to_string();
                s.spec.version = version.to_string();
                // CRDs should have relatively low priority
                s.spec.group_priority_minimum = 1000;
                // CRDs will be sorted by kube-like versions like any other
                // APIService with the same VersionPriority
                s.spec.version_priority = 100;
                self.api_service_registration.add_api_service_to_sync(&s);
                return Ok(());
            }
        }

        self.api_service_registration
            .remove_api_service_to_sync(&api_service_name);
        Ok(())
    }
}

/// The CRD lister over the API server's storage.
pub struct StorageCrdLister<S: Storage>(pub Arc<S>);

#[async_trait]
impl<S: Storage + 'static> CrdLister for StorageCrdLister<S> {
    async fn list(&self) -> Result<Vec<CustomResourceDefinition>, String> {
        self.0
            .list::<CustomResourceDefinition>(&build_prefix("customresourcedefinitions", None))
            .await
            .map_err(|e| e.to_string())
    }
}

/// The CRD informer's event handlers, fed by a storage watch: Added ->
/// `AddFunc`, Modified -> `UpdateFunc` (old from the local cache), Deleted ->
/// `DeleteFunc`. Start it BEFORE `run` so no event falls between the initial
/// list and the watch.
pub async fn spawn_crd_event_handlers<S: Storage + 'static, L: CrdLister + 'static>(
    storage: Arc<S>,
    controller: Arc<CrdRegistrationController<L>>,
) {
    let mut stream = match storage
        .watch(&build_prefix("customresourcedefinitions", None))
        .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("crd-autoregister: cannot watch CRDs: {e}");
            return;
        }
    };
    tokio::spawn(async move {
        let mut cache: HashMap<String, CustomResourceDefinition> = HashMap::new();
        while let Some(ev) = stream.next().await {
            let Ok(ev) = ev else { continue };
            match ev {
                WatchEvent::Added(_, v) | WatchEvent::Modified(_, v) => {
                    let Ok(new) = serde_json::from_str::<CustomResourceDefinition>(&v) else {
                        continue;
                    };
                    match cache.insert(new.metadata.name.clone(), new.clone()) {
                        Some(old) => controller.on_update(&old, &new),
                        None => controller.on_add(&new),
                    }
                }
                WatchEvent::Deleted(_, v) => {
                    let Ok(old) = serde_json::from_str::<CustomResourceDefinition>(&v) else {
                        continue;
                    };
                    cache.remove(&old.metadata.name);
                    controller.on_delete(&old);
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeLister(Vec<CustomResourceDefinition>);
    #[async_trait]
    impl CrdLister for FakeLister {
        async fn list(&self) -> Result<Vec<CustomResourceDefinition>, String> {
            Ok(self.0.clone())
        }
    }

    #[derive(Default)]
    struct FakeAPIServiceRegistration {
        added: Mutex<Vec<APIService>>,
        removed: Mutex<Vec<String>>,
    }
    impl AutoAPIServiceRegistration for FakeAPIServiceRegistration {
        fn add_api_service_to_sync(&self, in_: &APIService) {
            self.added.lock().unwrap().push(in_.clone());
        }
        fn remove_api_service_to_sync(&self, name: &str) {
            self.removed.lock().unwrap().push(name.to_string());
        }
    }

    fn crd(group: &str, versions: &[(&str, bool)]) -> CustomResourceDefinition {
        serde_json::from_value(serde_json::json!({
            "spec": {
                "group": group,
                "versions": versions.iter()
                    .map(|(n, s)| serde_json::json!({"name": n, "served": s, "storage": true}))
                    .collect::<Vec<_>>(),
            }
        }))
        .unwrap()
    }

    fn api_service(group: &str, version: &str) -> APIService {
        let mut s = APIService::default();
        s.metadata.name = format!("{version}.{group}");
        s.spec.group = group.to_string();
        s.spec.version = version.to_string();
        s.spec.group_priority_minimum = 1000;
        s.spec.version_priority = 100;
        s
    }

    /// `TestHandleVersionUpdate` (crdregistration_controller_test.go:30-95):
    /// "simple add crd" and "simple remove crd".
    #[tokio::test]
    async fn test_handle_version_update() {
        struct Case {
            name: &'static str,
            starting: Vec<CustomResourceDefinition>,
            version: (&'static str, &'static str),
            added: Vec<APIService>,
            removed: Vec<String>,
        }
        let cases = vec![
            Case {
                name: "simple add crd",
                starting: vec![crd("group.com", &[("v1", true)])],
                version: ("group.com", "v1"),
                added: vec![api_service("group.com", "v1")],
                removed: vec![],
            },
            Case {
                name: "simple remove crd",
                starting: vec![crd("group.com", &[("v1", true)])],
                version: ("group.com", "v2"),
                added: vec![],
                removed: vec!["v2.group.com".into()],
            },
            // Not in upstream's table: the `!version.Served` arm of
            // handleVersionUpdate (:218) falls through to the remove.
            Case {
                name: "unserved version is removed",
                starting: vec![crd("group.com", &[("v1", false)])],
                version: ("group.com", "v1"),
                added: vec![],
                removed: vec!["v1.group.com".into()],
            },
        ];
        for c in cases {
            let reg = Arc::new(FakeAPIServiceRegistration::default());
            let ctl = CrdRegistrationController::new(FakeLister(c.starting), reg.clone());
            ctl.handle_version_update(c.version.0, c.version.1)
                .await
                .unwrap();
            assert_eq!(*reg.added.lock().unwrap(), c.added, "{}", c.name);
            assert_eq!(*reg.removed.lock().unwrap(), c.removed, "{}", c.name);
        }
    }

    /// `Run` (:104-141): the initial list is processed once per version before
    /// `WaitForInitialSync` releases.
    #[tokio::test]
    async fn run_processes_initial_list_then_releases_wait_for_initial_sync() {
        let reg = Arc::new(FakeAPIServiceRegistration::default());
        let ctl = Arc::new(CrdRegistrationController::new(
            FakeLister(vec![crd("group.com", &[("v1", true), ("v2", true)])]),
            reg.clone(),
        ));
        let (stop_tx, stop_rx) = watch::channel(false);
        let run = tokio::spawn(ctl.clone().run(5, stop_rx));
        tokio::time::timeout(Duration::from_secs(5), ctl.wait_for_initial_sync())
            .await
            .expect("initial sync must complete");
        let mut names: Vec<String> = reg
            .added
            .lock()
            .unwrap()
            .iter()
            .map(|a| a.metadata.name.clone())
            .collect();
        names.sort();
        assert_eq!(names, vec!["v1.group.com", "v2.group.com"]);
        stop_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .unwrap()
            .unwrap();
    }

    /// `UpdateFunc`/`enqueueCRD`: old and new are both enqueued, duplicates
    /// collapse, and a worker drains the queue into the registration.
    #[tokio::test]
    async fn update_enqueues_old_and_new_and_workers_drain() {
        let reg = Arc::new(FakeAPIServiceRegistration::default());
        let ctl = Arc::new(CrdRegistrationController::new(
            FakeLister(vec![crd("group.com", &[("v2", true)])]),
            reg.clone(),
        ));
        ctl.on_update(
            &crd("group.com", &[("v1", true)]),
            &crd("group.com", &[("v1", true), ("v2", true)]),
        );
        assert_eq!(ctl.queue.len(), 2, "v1 deduped, v2 added");
        assert!(ctl.process_next_work_item().await);
        assert!(ctl.process_next_work_item().await);
        assert_eq!(*reg.removed.lock().unwrap(), vec!["v1.group.com"]);
        assert_eq!(reg.added.lock().unwrap()[0].metadata.name, "v2.group.com");
    }

    /// A key added while processing is re-queued on `Done`, never run twice
    /// concurrently (client-go `queue.go` dirty/processing).
    #[tokio::test]
    async fn queue_requeues_a_key_added_while_processing() {
        let q = WorkQueue::default();
        let k = ("g".to_string(), "v1".to_string());
        q.add(k.clone());
        assert_eq!(q.get().await, Some(k.clone()));
        q.add(k.clone());
        assert_eq!(q.len(), 0);
        q.done(&k);
        assert_eq!(q.len(), 1);
    }
}
