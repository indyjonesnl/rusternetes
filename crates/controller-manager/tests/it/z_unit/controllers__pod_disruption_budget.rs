//! Unit tests moved from `src/controllers/pod_disruption_budget.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Duration as ChronoDuration, Utc};
    pub use futures::StreamExt;
    pub use rusternetes_common::pdbhelper::{
        conditions_are_up_to_date, set_status_condition, update_disruption_allowed_condition,
        DISRUPTION_ALLOWED_CONDITION, SYNC_FAILED_REASON,
    };
    pub use rusternetes_common::podutil::is_pod_ready;
    pub use rusternetes_common::resources::service_account::ObjectReference;
    pub use rusternetes_common::resources::{
        CustomResource, CustomResourceDefinition, EventSource, EventType, IntOrString, Pod,
        PodDisruptionBudget, PodDisruptionBudgetCondition, PodDisruptionBudgetStatus,
    };
    pub use rusternetes_common::types::{LabelSelector, OwnerReference, Phase, Selector};
    pub use rusternetes_common::validation::metav1::{is_qualified_name, is_valid_label_value};
    pub use rusternetes_common::Error;
    pub use rusternetes_storage::{
        build_key, build_prefix, extract_key, EventRecorder, Storage, WatchEvent, WorkQueue,
    };
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use std::time::Duration as StdDuration;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::{IntOrString, PodDisruptionBudgetSpec};
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_controller_manager::controllers::pod_disruption_budget::*;
    use rusternetes_storage::MemoryStorage;
    /// A storage double that behaves like a real api-server: `update` (full-object
    /// PUT) DISCARDS `.status`; only the status subresource persists it. Upstream
    /// strips status on the main resource for any type with a status subresource,
    /// and `crates/storage/src/api_storage.rs` says so explicitly — a controller
    /// writing status through `update` "will not see it stick in API mode".
    ///
    /// Against a vanilla kube-apiserver that made `status.observedGeneration` never
    /// advance, so upstream's `waitForPdbToBeProcessed` polled until it timed out
    /// and every DisruptionController [Conformance] spec burned ~10 minutes — four
    /// of them consumed the whole vanilla-swap controller-manager budget (#1712).
    ///
    /// MemoryStorage cannot show this: its default `update_status` funnels through
    /// `update`, so both paths persist status there.
    struct StatusStrippingStorage {
        inner: MemoryStorage,
        /// Play the `/eviction` handler inside the status write: bump the
        /// stored object after the controller read it and before the guarded
        /// write lands.
        overtake_status_write: bool,
    }

    #[async_trait::async_trait]
    impl Storage for StatusStrippingStorage {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.create(key, value).await
        }

        async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.get(key).await
        }

        /// Full-object PUT: keep whatever status is already stored, ignore the
        /// caller's — exactly what an api-server with a status subresource does.
        async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            let mut doc = serde_json::to_value(value).unwrap();
            let stored: serde_json::Value =
                self.inner.get(key).await.unwrap_or(serde_json::Value::Null);
            if let Some(obj) = doc.as_object_mut() {
                match stored.get("status") {
                    Some(prev) if !prev.is_null() => {
                        obj.insert("status".to_string(), prev.clone());
                    }
                    _ => {
                        obj.remove("status");
                    }
                }
            }
            let kept: T = serde_json::from_value(doc).unwrap();
            self.inner.update(key, &kept).await
        }

        /// The status subresource: this is the ONLY path that persists `.status`.
        /// Must be overridden — the trait's default `update_status` does a
        /// read-modify-write through `update`, which above strips status, so
        /// inheriting it would make even a correct controller look broken.
        async fn update_status<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            let incoming = serde_json::to_value(value).unwrap();
            let mut stored: serde_json::Value = self.inner.get(key).await?;
            if let Some(obj) = stored.as_object_mut() {
                if let Some(status) = incoming.get("status") {
                    obj.insert("status".to_string(), status.clone());
                }
            }
            let merged: T = serde_json::from_value(stored).unwrap();
            self.inner.update(key, &merged).await
        }

        /// The status subresource with the resourceVersion precondition an
        /// api-server applies; the compare is `MemoryStorage`'s own.
        async fn update_status_cas<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            if self.overtake_status_write {
                let mut stored: serde_json::Value = self.inner.get(key).await?;
                stored["status"]["disruptionsAllowed"] = serde_json::json!(0);
                self.inner.update(key, &stored).await?;
            }
            self.inner.update_status_cas(key, value).await
        }

        async fn update_raw(
            &self,
            key: &str,
            value: &serde_json::Value,
        ) -> rusternetes_common::Result<()> {
            self.inner.update_raw(key, value).await
        }

        async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
            self.inner.delete(key).await
        }

        async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.list(prefix).await
        }

        async fn watch(
            &self,
            prefix: &str,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch(prefix).await
        }

        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }

        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.inner.current_revision().await
        }

        async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
            self.inner.is_revision_compacted(revision).await
        }
    }

    fn pdb_fixture() -> PodDisruptionBudget {
        PodDisruptionBudget {
            type_meta: TypeMeta {
                api_version: "policy/v1".to_string(),
                kind: "PodDisruptionBudget".to_string(),
            },
            metadata: ObjectMeta {
                name: "pdb-1".to_string(),
                namespace: Some("default".to_string()),
                generation: Some(1),
                ..Default::default()
            },
            spec: PodDisruptionBudgetSpec {
                min_available: Some(IntOrString::Int(1)),
                max_unavailable: None,
                selector: Some(LabelSelector::default()),
                unhealthy_pod_eviction_policy: None,
            },
            status: None,
        }
    }

    /// #2151: the status write is a compare-and-set, not check-then-write. The
    /// eviction lands AFTER the controller's read and INSIDE the status write,
    /// a window a get-compare-then-`update_status` cannot see; the write must
    /// conflict and leave the eviction's `disruptionsAllowed: 0` in place.
    #[tokio::test]
    async fn stale_status_write_conflicts_instead_of_overwriting() {
        let storage = Arc::new(StatusStrippingStorage {
            inner: MemoryStorage::new(),
            overtake_status_write: true,
        });
        let mut pdb = pdb_fixture();
        pdb.status = Some(PodDisruptionBudgetStatus {
            current_healthy: 3,
            desired_healthy: 1,
            disruptions_allowed: 2,
            expected_pods: 3,
            observed_generation: Some(1),
            conditions: None,
            disrupted_pods: None,
        });
        let key = build_key("poddisruptionbudgets", Some("default"), "pdb-1");
        storage.create(&key, &pdb).await.unwrap();
        let read: PodDisruptionBudget = storage.get(&key).await.unwrap();

        let controller = PodDisruptionBudgetController::new(storage.clone());
        let mut computed = read.clone();
        computed.status.as_mut().unwrap().disruptions_allowed = 2;
        let err = controller.write_pdb_status(&computed).await.unwrap_err();
        assert!(matches!(err, Error::Conflict(_)), "{err:?}");

        let stored: PodDisruptionBudget = storage.get(&key).await.unwrap();
        assert_eq!(
            stored.status.unwrap().disruptions_allowed,
            0,
            "the stale write must not overwrite the eviction's decrement"
        );
    }

    /// THE regression: reconcile must persist status through the status
    /// subresource, so observedGeneration advances even when a full-object PUT
    /// drops status.
    #[tokio::test]
    async fn pdb_status_survives_an_apiserver_that_strips_status_on_put() {
        let storage = Arc::new(StatusStrippingStorage {
            inner: MemoryStorage::new(),
            overtake_status_write: false,
        });
        let pdb = pdb_fixture();
        let key = build_key("poddisruptionbudgets", Some("default"), "pdb-1");
        storage.create(&key, &pdb).await.unwrap();

        let controller = PodDisruptionBudgetController::new(storage.clone());
        controller.sync("default/pdb-1").await.unwrap();

        let stored: PodDisruptionBudget = storage.get(&key).await.unwrap();
        let status = stored
            .status
            .expect("reconcile must persist PDB status via the status subresource");
        assert_eq!(
            status.observed_generation,
            Some(1),
            "observedGeneration must reach metadata.generation, else upstream's \
             waitForPdbToBeProcessed polls until it times out"
        );
    }

    /// `status.disruptionsAllowed` must never go negative.
    ///
    /// With no matching pods and `minAvailable: 2`, `currentHealthy -
    /// desiredHealthy` is -2. Upstream clamps that to 0
    /// (pkg/controller/disruption/disruption.go:1008-1011):
    ///
    /// ```go
    /// disruptionsAllowed := currentHealthy - desiredHealthy
    /// if expectedCount <= 0 || disruptionsAllowed <= 0 {
    ///     disruptionsAllowed = 0
    /// }
    /// ```
    ///
    /// and the API type declares the field `+kubebuilder:validation:Minimum=0`,
    /// so an api-server that validates it rejects the whole status write:
    ///
    /// ```text
    /// Failed to reconcile poddisruptionbudgets/pdb-check/foo: Bad request:
    ///   Error from server (Invalid): PodDisruptionBudget.policy "foo" is invalid:
    ///   status.disruptionsAllowed: Invalid value: -2: must be greater than or equal to 0
    /// ```
    ///
    /// The rejected write took observedGeneration with it, so upstream's
    /// `waitForPdbToBeProcessed` (test/e2e/apps/disruption.go:760) polled for
    /// its full 10-minute budget and FOUR DisruptionController [Conformance]
    /// specs failed on it.
    #[tokio::test]
    async fn disruptions_allowed_is_never_negative() {
        let storage = Arc::new(StatusStrippingStorage {
            inner: MemoryStorage::new(),
            overtake_status_write: false,
        });
        // minAvailable: 2, and not a single pod matches the selector.
        let mut pdb = pdb_fixture();
        pdb.spec.min_available = Some(IntOrString::Int(2));
        let key = build_key("poddisruptionbudgets", Some("default"), "pdb-1");
        storage.create(&key, &pdb).await.unwrap();

        let controller = PodDisruptionBudgetController::new(storage.clone());
        controller.sync("default/pdb-1").await.unwrap();

        let status = storage
            .get::<PodDisruptionBudget>(&key)
            .await
            .unwrap()
            .status
            .expect("reconcile must persist status");
        assert_eq!(
            status.disruptions_allowed, 0,
            "disruptionsAllowed must clamp at 0, not report {}",
            status.disruptions_allowed
        );
        assert_eq!(
            status.observed_generation,
            Some(1),
            "the clamped write must still advance observedGeneration"
        );
    }

    /// The clamp is not a blanket zero: a PDB with slack still reports it.
    #[tokio::test]
    async fn disruptions_allowed_reports_real_slack() {
        let storage = Arc::new(StatusStrippingStorage {
            inner: MemoryStorage::new(),
            overtake_status_write: false,
        });
        let mut pdb = pdb_fixture();
        pdb.spec.min_available = Some(IntOrString::Int(0));
        let key = build_key("poddisruptionbudgets", Some("default"), "pdb-1");
        storage.create(&key, &pdb).await.unwrap();

        let controller = PodDisruptionBudgetController::new(storage.clone());
        controller.sync("default/pdb-1").await.unwrap();

        let status = storage
            .get::<PodDisruptionBudget>(&key)
            .await
            .unwrap()
            .status
            .expect("reconcile must persist status");
        assert!(
            status.disruptions_allowed >= 0,
            "never negative: {}",
            status.disruptions_allowed
        );
    }

    #[test]
    fn test_resolve_json_path_simple() {
        let doc = serde_json::json!({"spec": {"replicas": 7, "nested": {"x": 1}}});
        assert_eq!(
            resolve_json_path(&doc, ".spec.replicas").and_then(|v| v.as_i64()),
            Some(7)
        );
        // Leading-dot stripping is just sugar — both should work identically.
        assert_eq!(
            resolve_json_path(&doc, "spec.replicas").and_then(|v| v.as_i64()),
            Some(7)
        );
        // Multi-level descent.
        assert_eq!(
            resolve_json_path(&doc, ".spec.nested.x").and_then(|v| v.as_i64()),
            Some(1)
        );
        // Missing path returns None (caller falls back to pod count).
        assert!(resolve_json_path(&doc, ".spec.missing").is_none());
        // Empty segment is malformed input; must not panic / silently match.
        assert!(resolve_json_path(&doc, ".spec..replicas").is_none());
    }

    #[test]
    fn test_parse_group_version() {
        // schema.ParseGroupVersion: "" and "/" are the zero value; one slash
        // splits; more is an error; core resources have no group.
        assert_eq!(
            parse_group_version("apps/v1").unwrap(),
            ("apps".to_string(), "v1".to_string())
        );
        assert_eq!(
            parse_group_version("v1").unwrap(),
            (String::new(), "v1".to_string())
        );
        assert_eq!(
            parse_group_version("/v1").unwrap(),
            (String::new(), "v1".to_string())
        );
        assert_eq!(
            parse_group_version("").unwrap(),
            (String::new(), String::new())
        );
        assert!(parse_group_version("a/b/c").is_err());
    }

    #[test]
    fn test_controller_ref_from_json_returns_only_controller() {
        // Pure ownerRef without `controller: true` MUST be ignored — upstream's
        // GetControllerOf is strict about this. Only the explicit controller
        // ref is the one we walk up from.
        let obj = serde_json::json!({
            "metadata": {
                "ownerReferences": [
                    {"apiVersion": "v1", "kind": "Pod", "name": "side", "uid": "1"},
                    {
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "main",
                        "uid": "2",
                        "controller": true
                    }
                ]
            }
        });
        let r = controller_ref_from_json(&obj).expect("controller present");
        assert_eq!(r.kind, "Deployment");
        assert_eq!(r.uid, "2");

        // No controller flag set anywhere → None.
        let obj2 = serde_json::json!({
            "metadata": {
                "ownerReferences": [
                    {"apiVersion": "v1", "kind": "Pod", "name": "side", "uid": "1"}
                ]
            }
        });
        assert!(controller_ref_from_json(&obj2).is_none());

        // No metadata at all → None.
        let obj3 = serde_json::json!({});
        assert!(controller_ref_from_json(&obj3).is_none());
    }
}
