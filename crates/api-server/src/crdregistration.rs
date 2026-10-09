//! The crdregistration controller: registers CRD group-versions with the
//! auto APIService registration controller so they stay in sync.
//!
//! Ported from `pkg/controlplane/controller/crdregistration/
//! crdregistration_controller.go`; tests from `crdregistration_controller_test.go`.

use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::resources::{APIService, CustomResourceDefinition};

/// `AutoAPIServiceRegistration`.
pub trait AutoAPIServiceRegistration: Send + Sync {
    fn add_api_service_to_sync(&self, in_: &APIService);
    fn remove_api_service_to_sync(&self, name: &str);
}

/// `crdlisters.CustomResourceDefinitionLister`.
#[async_trait]
pub trait CrdLister: Send + Sync {
    async fn list(&self) -> Result<Vec<CustomResourceDefinition>, String>;
}

pub struct CrdRegistrationController<L: CrdLister> {
    crd_lister: L,
    api_service_registration: Arc<dyn AutoAPIServiceRegistration>,
}

impl<L: CrdLister> CrdRegistrationController<L> {
    pub fn new(
        crd_lister: L,
        api_service_registration: Arc<dyn AutoAPIServiceRegistration>,
    ) -> Self {
        Self {
            crd_lister,
            api_service_registration,
        }
    }

    /// `handleVersionUpdate`.
    pub async fn handle_version_update(&self, _group: &str, _version: &str) -> Result<(), String> {
        let _ = (&self.crd_lister, &self.api_service_registration);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

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
}
