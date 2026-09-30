//! Port of `pkg/registry/core/service/allocator/storage/storage.go`: an
//! allocator whose bitmap is persisted as a `RangeAllocation` after every
//! change, so every api-server shares one allocation state.

use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::resources::rangeallocation::RangeAllocation;
use rusternetes_common::{Error, Result};
use rusternetes_storage::Storage;
use tokio::sync::Mutex;

use super::{AllocationBitmap, Interface};

/// `errorUnableToAllocate` (storage.go:39-41).
const UNABLE_TO_ALLOCATE: &str = "unable to allocate";

/// `rangeallocation.RangeRegistry` (pkg/registry/core/rangeallocation/
/// registry.go:24-30): read and overwrite the persisted snapshot.
#[async_trait]
pub trait RangeRegistry: Send + Sync {
    /// The stored snapshot; an empty `resourceVersion` if there is none.
    async fn get(&self) -> Result<RangeAllocation>;
    /// Replace the stored snapshot, which must still be the one
    /// `snapshot.metadata.resourceVersion` names (or absent, for an empty
    /// one).
    async fn create_or_update(&self, snapshot: RangeAllocation) -> Result<()>;
}

struct State {
    alloc: AllocationBitmap,
    /// The resourceVersion `alloc` was last synced with.
    last: String,
}

/// `storage.Etcd` (storage.go:43-58).
pub struct Etcd<S: Storage> {
    state: Mutex<State>,
    storage: Arc<S>,
    key: String,
    /// The resource named in errors, e.g. `servicenodeportallocations`.
    resource: String,
}

impl<S: Storage> Etcd<S> {
    /// `NewEtcd` (storage.go:64-81). `key` is the full storage key, e.g.
    /// `/registry/ranges/servicenodeports`.
    pub fn new(
        alloc: AllocationBitmap,
        storage: Arc<S>,
        key: impl Into<String>,
        resource: impl Into<String>,
    ) -> Self {
        Self {
            state: Mutex::new(State {
                alloc,
                last: String::new(),
            }),
            storage,
            key: key.into(),
            resource: resource.into(),
        }
    }

    /// The stored snapshot, or `None` when there is none yet.
    async fn stored(&self) -> Result<Option<RangeAllocation>> {
        match self.storage.get::<RangeAllocation>(&self.key).await {
            Ok(r) => Ok(Some(r)),
            Err(Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// `Etcd.tryUpdate` (storage.go:150-177): a guaranteed update that syncs
    /// the bitmap with storage, applies `f` and persists the result.
    async fn try_update(
        &self,
        state: &mut State,
        f: &mut (dyn FnMut(&mut AllocationBitmap) -> Result<()> + Send),
    ) -> Result<()> {
        loop {
            let existing = self.stored().await?;
            let Some(mut existing) = existing.filter(|e| e.metadata.resource_version.is_some())
            else {
                return Err(Error::Internal(format!(
                    "cannot allocate resources of type {} at this time",
                    self.resource
                )));
            };
            let rv = existing
                .metadata
                .resource_version
                .clone()
                .unwrap_or_default();
            if rv != state.last {
                state.alloc.restore(&existing.range, &existing.data)?;
            }
            f(&mut state.alloc)?;
            state.last = rv;
            let (range_spec, data) = state.alloc.snapshot();
            existing.range = range_spec;
            existing.data = data;
            match self.storage.update(&self.key, &existing).await {
                Ok(_) => return Ok(()),
                // Retry against the latest snapshot, as GuaranteedUpdate does.
                Err(Error::Conflict(_)) => continue,
                Err(e) => return Err(e),
            }
        }
    }
}

#[async_trait]
impl<S: Storage + 'static> Interface for Etcd<S> {
    /// `Etcd.Allocate` (storage.go:83-104).
    async fn allocate(&self, offset: usize) -> Result<bool> {
        let mut state = self.state.lock().await;
        let result = self
            .try_update(&mut state, &mut |alloc| match alloc.allocate(offset)? {
                true => Ok(()),
                false => Err(Error::Internal(UNABLE_TO_ALLOCATE.to_string())),
            })
            .await;
        match result {
            Ok(()) => Ok(true),
            Err(Error::Internal(msg)) if msg == UNABLE_TO_ALLOCATE => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// `Etcd.AllocateNext` (storage.go:106-131).
    async fn allocate_next(&self) -> Result<Option<usize>> {
        let mut state = self.state.lock().await;
        let mut offset = None;
        let result = self
            .try_update(&mut state, &mut |alloc| {
                offset = alloc.allocate_next();
                match offset {
                    Some(_) => Ok(()),
                    None => Err(Error::Internal(UNABLE_TO_ALLOCATE.to_string())),
                }
            })
            .await;
        match result {
            Ok(()) => Ok(offset),
            Err(Error::Internal(msg)) if msg == UNABLE_TO_ALLOCATE => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// `Etcd.Release` (storage.go:133-141).
    async fn release(&self, offset: usize) -> Result<()> {
        let mut state = self.state.lock().await;
        self.try_update(&mut state, &mut |alloc| {
            alloc.release(offset);
            Ok(())
        })
        .await
    }

    /// `Etcd.ForEach` (storage.go:143-147): the in-memory view.
    async fn allocated(&self) -> Vec<usize> {
        self.state.lock().await.alloc.allocated()
    }

    /// `Etcd.Has` (storage.go:224-230).
    async fn has(&self, offset: usize) -> bool {
        self.state.lock().await.alloc.has(offset)
    }

    /// `Etcd.Free` (storage.go:232-238).
    async fn free(&self) -> usize {
        self.state.lock().await.alloc.free()
    }
}

#[async_trait]
impl<S: Storage + 'static> RangeRegistry for Etcd<S> {
    /// `Etcd.Get` (storage.go:179-187).
    async fn get(&self) -> Result<RangeAllocation> {
        Ok(self.stored().await?.unwrap_or_default())
    }

    /// `Etcd.CreateOrUpdate` (storage.go:189-222).
    async fn create_or_update(&self, snapshot: RangeAllocation) -> Result<()> {
        let mut state = self.state.lock().await;
        let conflict = |reason: &str| {
            Error::Conflict(format!(
                "Operation cannot be fulfilled on {}: {reason}",
                self.resource
            ))
        };
        let want = snapshot
            .metadata
            .resource_version
            .clone()
            .unwrap_or_default();
        match self.stored().await? {
            Some(existing) => {
                let have = existing
                    .metadata
                    .resource_version
                    .clone()
                    .unwrap_or_default();
                if !want.is_empty() && !have.is_empty() {
                    if want != have {
                        return Err(conflict("the provided resource version does not match"));
                    }
                } else if !have.is_empty() {
                    return Err(conflict(
                        "another caller has already initialized the resource",
                    ));
                }
                self.storage.update(&self.key, &snapshot).await?;
            }
            None => {
                let mut fresh = snapshot.clone();
                fresh.metadata.resource_version = None;
                fresh.type_meta.kind = "RangeAllocation".to_string();
                fresh.type_meta.api_version = "v1".to_string();
                self.storage.create(&self.key, &fresh).await?;
            }
        }
        state.alloc.restore(&snapshot.range, &snapshot.data)?;
        state.last = want;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Ports of allocator/storage/storage_test.go.
    use super::*;
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

    impl<S: Storage> Etcd<S> {
        /// The in-memory bitmap, which upstream's tests hold as `backing`.
        async fn backing<R>(&self, f: impl FnOnce(&mut AllocationBitmap) -> R) -> R {
            f(&mut self.state.lock().await.alloc)
        }
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
