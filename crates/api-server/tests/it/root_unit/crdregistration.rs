//! Unit tests moved from `src/crdregistration.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use futures::StreamExt;
    pub use rusternetes_api_server::registry::apiextensions::customresourcedefinition::ControllerRateLimiter;
    pub use rusternetes_common::resources::{APIService, CustomResourceDefinition};
    pub use rusternetes_storage::{build_prefix, Storage, WatchEvent};
    pub use std::collections::{HashMap, HashSet, VecDeque};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::Duration;
    pub use tokio::sync::{watch, Notify};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::crdregistration::*;

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
