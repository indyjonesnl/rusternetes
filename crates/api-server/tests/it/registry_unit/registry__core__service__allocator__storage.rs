//! Unit tests moved from `src/registry/core/service/allocator/storage.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::registry::core::service::allocator::{
        AllocationBitmap, Interface,
    };
    pub use rusternetes_common::resources::rangeallocation::RangeAllocation;
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::Storage;
    pub use std::sync::Arc;
    pub use tokio::sync::Mutex;
}

mod tests {
    //! Ports of allocator/storage/storage_test.go.
    use super::__parent::*;
    use rusternetes_api_server::registry::core::service::allocator::storage::*;
    use rusternetes_storage::MemoryStorage;

    const KEY: &str = "/registry/ranges/serviceips";

    fn new_storage(storage: Arc<MemoryStorage>) -> Etcd<MemoryStorage> {
        Etcd::new(
            AllocationBitmap::new(100, "rangeSpecValue"),
            storage,
            KEY,
            "serviceipallocations",
        )
    }

    async fn seed(storage: &MemoryStorage) {
        let ra = RangeAllocation {
            range: "rangeSpecValue".to_string(),
            ..Default::default()
        };
        storage.create(KEY, &ra).await.unwrap();
    }

    #[tokio::test]
    async fn test_empty() {
        let etcd = new_storage(Arc::new(MemoryStorage::new()));
        let err = etcd.allocate(1).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot allocate resources of type serviceipallocations at this time"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn test_store() {
        let storage = Arc::new(MemoryStorage::new());
        let etcd = new_storage(storage.clone());
        seed(&storage).await;

        assert!(etcd.allocate(2).await.unwrap());
        assert!(!etcd.backing(|b| b.allocate(2).unwrap()).await);
        assert!(!etcd.allocate(2).await.unwrap());

        let stored: RangeAllocation = storage.get(KEY).await.unwrap();
        assert_eq!(stored.range, "rangeSpecValue");
        let mut other = AllocationBitmap::new(100, "rangeSpecValue");
        other.restore("rangeSpecValue", &stored.data).unwrap();
        assert!(other.has(2), "could not restore allocated item");

        // A second allocator over the same storage sees the allocation.
        let other_storage = new_storage(storage);
        assert!(!other_storage.allocate(2).await.unwrap());
    }

    /// `TestAllocatedStorageButReleasedLocally`: storage wins over a stale
    /// local bitmap.
    #[tokio::test]
    async fn allocated_storage_but_released_locally() {
        let storage = Arc::new(MemoryStorage::new());
        let etcd = new_storage(storage.clone());
        seed(&storage).await;
        assert!(etcd.allocate(2).await.unwrap());
        etcd.backing(|b| b.release(2)).await;
        // `last` is the resourceVersion read before the write that recorded
        // the allocation, so the next update restores from storage.
        assert!(!etcd.allocate(2).await.unwrap());
    }

    /// `TestAllocatedLocallyButReleasedStorage`.
    #[tokio::test]
    async fn allocated_locally_but_released_storage() {
        let storage = Arc::new(MemoryStorage::new());
        let etcd = new_storage(storage.clone());
        seed(&storage).await;
        assert!(etcd.backing(|b| b.allocate(2).unwrap()).await);
        assert!(etcd.allocate(2).await.unwrap());
    }

    /// `Etcd.CreateOrUpdate` (storage.go:189-222).
    #[tokio::test]
    async fn create_or_update_initializes_once() {
        let storage = Arc::new(MemoryStorage::new());
        let etcd = new_storage(storage.clone());
        let snapshot = RangeAllocation {
            range: "rangeSpecValue".to_string(),
            ..Default::default()
        };
        etcd.create_or_update(snapshot.clone()).await.unwrap();
        assert!(etcd.allocate(3).await.unwrap());

        let err = etcd.create_or_update(snapshot).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("another caller has already initialized the resource"),
            "{err}"
        );

        let mut stale = etcd.get().await.unwrap();
        stale.metadata.resource_version = Some("1".to_string());
        let err = etcd.create_or_update(stale).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("the provided resource version does not match"),
            "{err}"
        );
    }
}
