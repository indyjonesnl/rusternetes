//! Unit tests moved from `src/registry/core/service/portallocator/repair.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::core::service::allocator::storage::RangeRegistry;
    pub use rusternetes_api_server::registry::core::service::portallocator::{
        PortAllocator, PortError, PortRange,
    };
    pub use rusternetes_common::resources::{EventSource, EventType, ObjectReference, Service};
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::{EventRecorder, Storage};
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{error, warn};
}

mod tests {
    //! Ports of portallocator/controller/repair_test.go.
    use super::__parent::*;
    use async_trait::async_trait;
    use rusternetes_api_server::registry::core::service::portallocator::repair::*;
    use rusternetes_common::resources::rangeallocation::RangeAllocation;
    use rusternetes_storage::MemoryStorage;
    use serde_json::json;
    use std::sync::Mutex;

    /// `mockRangeRegistry` (repair_test.go:34-54).
    #[derive(Default)]
    struct MockRangeRegistry {
        item: RangeAllocation,
        update_err: Option<String>,
        updated: Mutex<Option<RangeAllocation>>,
    }

    #[async_trait]
    impl RangeRegistry for MockRangeRegistry {
        async fn get(&self) -> Result<RangeAllocation> {
            Ok(self.item.clone())
        }
        async fn create_or_update(&self, alloc: RangeAllocation) -> Result<()> {
            *self.updated.lock().unwrap() = Some(alloc);
            match &self.update_err {
                Some(e) => Err(Error::Internal(e.clone())),
                None => Ok(()),
            }
        }
    }

    impl MockRangeRegistry {
        fn updated(&self) -> RangeAllocation {
            self.updated.lock().unwrap().clone().expect("updated")
        }
    }

    async fn add_service(storage: &MemoryStorage, ns: &str, name: &str, spec: serde_json::Value) {
        let svc: Service = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": name, "namespace": ns},
            "spec": spec,
        }))
        .unwrap();
        storage
            .create(&format!("/registry/services/{ns}/{name}"), &svc)
            .await
            .unwrap();
    }

    fn pr() -> PortRange {
        PortRange::parse("100-200").unwrap()
    }

    #[tokio::test]
    async fn test_repair() {
        let storage = Arc::new(MemoryStorage::new());
        let registry = Arc::new(MockRangeRegistry {
            item: RangeAllocation {
                range: "100-200".to_string(),
                ..Default::default()
            },
            ..Default::default()
        });
        let mut r = Repair::new(storage.clone(), pr(), registry.clone());
        r.run_once().await.unwrap();
        assert_eq!(registry.updated().range, pr().to_string());

        let registry = Arc::new(MockRangeRegistry {
            item: RangeAllocation {
                range: "100-200".to_string(),
                ..Default::default()
            },
            update_err: Some("test error".to_string()),
            ..Default::default()
        });
        let mut r = Repair::new(storage, pr(), registry);
        let err = r.run_once().await.unwrap_err();
        assert!(err.to_string().contains(": test error"), "{err}");
    }

    #[tokio::test]
    async fn test_repair_leak() {
        let previous = PortAllocator::new_in_memory(pr());
        previous.allocate(111).await.unwrap();
        let mut dst = RangeAllocation::default();
        previous.snapshot(&mut dst).await.unwrap();
        dst.metadata.resource_version = Some("1".to_string());

        let registry = Arc::new(MockRangeRegistry {
            item: dst,
            ..Default::default()
        });
        let mut r = Repair::new(Arc::new(MemoryStorage::new()), pr(), registry.clone());
        // Run through the "leak detection holdoff" loops.
        for _ in 0..(NUM_REPAIRS_BEFORE_LEAK_CLEANUP - 1) {
            r.run_once().await.unwrap();
            let after = PortAllocator::from_snapshot(&registry.updated())
                .await
                .unwrap();
            assert!(after.has(111).await, "expected the leaked port to remain");
        }
        // One more pass actually removes the leak.
        r.run_once().await.unwrap();
        let after = PortAllocator::from_snapshot(&registry.updated())
            .await
            .unwrap();
        assert!(
            !after.has(111).await,
            "expected the leaked port to be freed"
        );
    }

    #[tokio::test]
    async fn test_repair_with_existing() {
        let previous = PortAllocator::new_in_memory(pr());
        let mut dst = RangeAllocation::default();
        previous.snapshot(&mut dst).await.unwrap();
        dst.metadata.resource_version = Some("1".to_string());

        let storage = Arc::new(MemoryStorage::new());
        add_service(
            &storage,
            "one",
            "one",
            json!({"ports": [{"port": 1, "nodePort": 111}]}),
        )
        .await;
        add_service(
            &storage,
            "two",
            "two",
            json!({"ports": [{"port": 1, "nodePort": 122}, {"port": 2, "nodePort": 133}]}),
        )
        .await;
        // Outside the range: dropped.
        add_service(
            &storage,
            "three",
            "three",
            json!({"ports": [{"port": 1, "nodePort": 201}]}),
        )
        .await;
        // Empty: ignored.
        add_service(&storage, "four", "four", json!({"ports": [{"port": 1}]})).await;
        // Duplicate: dropped.
        add_service(
            &storage,
            "five",
            "five",
            json!({"ports": [{"port": 1, "nodePort": 111}]}),
        )
        .await;
        add_service(&storage, "six", "six", json!({"healthCheckNodePort": 144})).await;

        let registry = Arc::new(MockRangeRegistry {
            item: dst,
            ..Default::default()
        });
        let mut r = Repair::new(storage, pr(), registry.clone());
        r.run_once().await.unwrap();
        let after = PortAllocator::from_snapshot(&registry.updated())
            .await
            .unwrap();
        for p in [111, 122, 133, 144] {
            assert!(after.has(p).await, "{p} should be allocated");
        }
        assert_eq!(after.free().await, 97);
    }

    #[test]
    fn test_collect_service_node_ports() {
        let cases: &[(&str, serde_json::Value, &[usize])] = &[
            (
                "no duplicated nodePorts",
                json!({"ports": [
                    {"port": 1, "nodePort": 111, "protocol": "TCP"},
                    {"port": 2, "nodePort": 112, "protocol": "UDP"},
                    {"port": 3, "nodePort": 113, "protocol": "UDP"}]}),
                &[111, 112, 113],
            ),
            (
                "duplicated nodePort with TCP protocol",
                json!({"ports": [
                    {"port": 1, "nodePort": 111, "protocol": "TCP"},
                    {"port": 2, "nodePort": 111, "protocol": "TCP"},
                    {"port": 3, "nodePort": 112, "protocol": "UDP"}]}),
                &[111, 111, 112],
            ),
            (
                "duplicated nodePort with UDP protocol",
                json!({"ports": [
                    {"port": 1, "nodePort": 111, "protocol": "UDP"},
                    {"port": 2, "nodePort": 111, "protocol": "UDP"},
                    {"port": 3, "nodePort": 112, "protocol": "TCP"}]}),
                &[111, 111, 112],
            ),
            (
                "duplicated nodePort with different protocol",
                json!({"ports": [
                    {"port": 1, "nodePort": 111, "protocol": "TCP"},
                    {"port": 2, "nodePort": 112, "protocol": "TCP"},
                    {"port": 3, "nodePort": 111, "protocol": "UDP"}]}),
                &[111, 112],
            ),
            (
                "no duplicated port(with health check port)",
                json!({"ports": [
                    {"port": 1, "nodePort": 111, "protocol": "TCP"},
                    {"port": 2, "nodePort": 112, "protocol": "UDP"}],
                    "healthCheckNodePort": 113}),
                &[111, 112, 113],
            ),
            (
                "nodePort has different protocol with duplicated health check port",
                json!({"ports": [
                    {"port": 1, "nodePort": 111, "protocol": "UDP"},
                    {"port": 2, "nodePort": 112, "protocol": "TCP"}],
                    "healthCheckNodePort": 111}),
                &[111, 112],
            ),
            (
                "nodePort has same protocol as duplicated health check port",
                json!({"ports": [
                    {"port": 1, "nodePort": 111, "protocol": "UDP"},
                    {"port": 2, "nodePort": 112, "protocol": "TCP"}],
                    "healthCheckNodePort": 112}),
                &[111, 112, 112],
            ),
        ];
        for (name, spec, want) in cases {
            let svc: Service = serde_json::from_value(json!({
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": "s"}, "spec": spec,
            }))
            .unwrap();
            let mut got = collect_service_node_ports(&svc);
            got.sort();
            let mut want = want.to_vec();
            want.sort();
            assert_eq!(got, want, "{name}");
        }
    }
}
