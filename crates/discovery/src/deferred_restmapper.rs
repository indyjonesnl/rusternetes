//! `DeferredDiscoveryRESTMapper` over a `memCacheClient`.
//!
//! Ported from Kubernetes release-1.35:
//! - `staging/src/k8s.io/client-go/restmapper/discovery.go`:
//!   `DeferredDiscoveryRESTMapper` :145-330 (`getDelegate` :194-211, `Reset`
//!   :213-222, and the invalidate-and-retry-on-miss guard in `KindFor` :225,
//!   `KindsFor` :240, `ResourceFor` :255, `ResourcesFor` :270, `RESTMapping`
//!   :285, `RESTMappings` :301 and `ResourceSingularizer` :316).
//! - `staging/src/k8s.io/client-go/discovery/cached/memory/memcache.go`:
//!   `memCacheClient` (`Fresh` :209, `Invalidate` :220, `refreshLocked` :235).
//! - `cmd/kube-controller-manager/app/controllermanager.go:515-519`: one
//!   mapper per process, `go wait.Until(restMapper.Reset, 30*time.Second,
//!   ctx.Done())` ([`RESET_PERIOD`]).
//!
//! Rust-only: the discovery "client" is an async [`DiscoveryClient`] (the
//! served groups are computed from storage), so the lookups are `async`.
//! The recursion upstream (`d.KindFor(resource)` after `d.Reset()`) is a loop
//! with the same exit conditions. `memCacheClient`'s aggregated-discovery and
//! per-group-version transient-error handling have no counterpart: the
//! served groups are computed in one call.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::restmapper::{
    new_discovery_rest_mapper, ApiGroupResources, Gk, Gvk, Gvr, MapperError, PriorityRestMapper,
    RestMapping,
};

/// The `wait.Until` period of the periodic `Reset`
/// (controllermanager.go:518).
pub const RESET_PERIOD: Duration = Duration::from_secs(30);

/// `discovery.CachedDiscoveryInterface`'s `Fresh` / `Invalidate`, plus the
/// `restmapper.GetAPIGroupResources` result the mapper is built from.
#[async_trait]
pub trait CachedDiscovery: Send + Sync {
    /// `Fresh`: whether the cache is populated (memcache.go:209-217).
    fn fresh(&self) -> bool;
    /// `Invalidate` (memcache.go:220-232).
    fn invalidate(&self);
    /// `GetAPIGroupResources(d.cl)` (discovery.go:140-).
    async fn group_resources(&self) -> Result<Vec<ApiGroupResources>, MapperError>;
}

/// The uncached `discovery.DiscoveryInterface` a [`MemCacheClient`] wraps.
#[async_trait]
pub trait DiscoveryClient: Send + Sync {
    async fn group_resources(&self) -> Result<Vec<ApiGroupResources>, MapperError>;
}

#[derive(Default)]
struct CacheState {
    /// `cacheValid`.
    valid: bool,
    /// Bumped by every `Invalidate`: a refresh that raced one must not
    /// repopulate the cache (upstream holds `d.lock` across `Invalidate` and
    /// `refreshLocked`, memcache.go:220-232, 235).
    generation: u64,
    /// `groupList` + `groupToServerResources`.
    groups: Option<Arc<Vec<ApiGroupResources>>>,
}

/// `memCacheClient`.
pub struct MemCacheClient<C: DiscoveryClient> {
    delegate: Arc<C>,
    state: Mutex<CacheState>,
}

impl<C: DiscoveryClient> MemCacheClient<C> {
    /// `NewMemCacheClient`.
    pub fn new(delegate: Arc<C>) -> Self {
        Self {
            delegate,
            state: Mutex::new(CacheState::default()),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, CacheState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl<C: DiscoveryClient> CachedDiscovery for MemCacheClient<C> {
    /// "Whether the cache is populated at all" (memcache.go:213-215).
    fn fresh(&self) -> bool {
        self.state().valid
    }

    fn invalidate(&self) {
        let mut s = self.state();
        s.valid = false;
        s.groups = None;
        s.generation += 1;
    }

    /// `if !d.cacheValid { d.refreshLocked() }` (memcache.go:100-108): the
    /// cache is populated on first use after an invalidation.
    async fn group_resources(&self) -> Result<Vec<ApiGroupResources>, MapperError> {
        let generation = {
            let s = self.state();
            if s.valid {
                if let Some(g) = &s.groups {
                    return Ok(g.as_ref().clone());
                }
            }
            s.generation
        };
        // refreshLocked: an error leaves the cache invalid (memcache.go:
        // 270-274, `return err`).
        let groups = Arc::new(self.delegate.group_resources().await?);
        let mut s = self.state();
        if s.generation == generation {
            s.groups = Some(groups.clone());
            s.valid = true;
        }
        Ok(groups.as_ref().clone())
    }
}

/// `DeferredDiscoveryRESTMapper`.
pub struct DeferredRestMapper<D: CachedDiscovery> {
    /// `initMu` + `delegate`.
    delegate: tokio::sync::Mutex<Option<Arc<PriorityRestMapper>>>,
    cl: Arc<D>,
}

impl<D: CachedDiscovery> DeferredRestMapper<D> {
    /// `NewDeferredDiscoveryRESTMapper`.
    pub fn new(cl: Arc<D>) -> Self {
        Self {
            delegate: tokio::sync::Mutex::new(None),
            cl,
        }
    }

    /// `getDelegate` (discovery.go:194-211).
    async fn get_delegate(&self) -> Result<Arc<PriorityRestMapper>, MapperError> {
        let mut delegate = self.delegate.lock().await;
        if let Some(d) = delegate.as_ref() {
            return Ok(d.clone());
        }
        let group_resources = self.cl.group_resources().await?;
        let d = Arc::new(new_discovery_rest_mapper(&group_resources));
        *delegate = Some(d.clone());
        Ok(d)
    }

    /// `Reset` (discovery.go:213-222): "resets the internally cached
    /// Discovery information and will cause the next mapping request to
    /// re-discover".
    pub async fn reset(&self) {
        let mut delegate = self.delegate.lock().await;
        self.cl.invalidate();
        *delegate = None;
    }

    /// The guard every lookup shares: run `lookup` on the delegate and, when
    /// `missed` and the client is not fresh, `Reset` and run it again
    /// (`if err != nil && !d.cl.Fresh() { d.Reset(); ... }`).
    async fn lookup<T>(
        &self,
        lookup: impl Fn(&PriorityRestMapper) -> Result<T, MapperError>,
        missed: impl Fn(&Result<T, MapperError>) -> bool,
    ) -> Result<T, MapperError> {
        loop {
            let del = self.get_delegate().await?;
            let r = lookup(&del);
            if missed(&r) && !self.cl.fresh() {
                self.reset().await;
                continue;
            }
            return r;
        }
    }

    /// `KindFor` (discovery.go:225-237).
    pub async fn kind_for(&self, resource: &Gvr) -> Result<Gvk, MapperError> {
        self.lookup(|m| m.kind_for(resource), |r| r.is_err()).await
    }

    /// `KindsFor` (discovery.go:240-252): retries on `len(gvks) == 0`.
    pub async fn kinds_for(&self, resource: &Gvr) -> Result<Vec<Gvk>, MapperError> {
        self.lookup(
            |m| m.kinds_for(resource),
            |r| r.as_ref().map_or(true, |v| v.is_empty()),
        )
        .await
    }

    /// `ResourceFor` (discovery.go:255-267).
    pub async fn resource_for(&self, input: &Gvr) -> Result<Gvr, MapperError> {
        self.lookup(|m| m.resource_for(input), |r| r.is_err()).await
    }

    /// `ResourcesFor` (discovery.go:270-282): retries on `len(gvrs) == 0`.
    pub async fn resources_for(&self, input: &Gvr) -> Result<Vec<Gvr>, MapperError> {
        self.lookup(
            |m| m.resources_for(input),
            |r| r.as_ref().map_or(true, |v| v.is_empty()),
        )
        .await
    }

    /// `RESTMapping` (discovery.go:285-298).
    pub async fn rest_mapping(
        &self,
        gk: &Gk,
        versions: &[&str],
    ) -> Result<RestMapping, MapperError> {
        self.lookup(|m| m.rest_mapping(gk, versions), |r| r.is_err())
            .await
    }

    /// `RESTMappings` (discovery.go:301-313): retries on `len(ms) == 0`.
    pub async fn rest_mappings(
        &self,
        gk: &Gk,
        versions: &[&str],
    ) -> Result<Vec<RestMapping>, MapperError> {
        self.lookup(
            |m| m.rest_mappings(gk, versions),
            |r| r.as_ref().map_or(true, |v| v.is_empty()),
        )
        .await
    }

    /// `ResourceSingularizer` (discovery.go:316-327).
    pub async fn resource_singularizer(&self, resource: &str) -> Result<String, MapperError> {
        self.lookup(|m| m.resource_singularizer(resource), |r| r.is_err())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::restmapper::ApiResource;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn group_a() -> ApiGroupResources {
        ApiGroupResources {
            name: "a".into(),
            versions: vec!["v1".into()],
            preferred_version: "v1".into(),
            versioned_resources: [(
                "v1".to_string(),
                vec![ApiResource {
                    name: "foo".into(),
                    singular_name: String::new(),
                    namespaced: true,
                    kind: "Foo".into(),
                }],
            )]
            .into(),
        }
    }

    /// `fakeCachedDiscoveryInterface` (discovery_test.go:429-445): serves
    /// nothing until it is invalidated, then serves group `a`.
    #[derive(Default)]
    struct FakeCachedDiscovery {
        invalidate_calls: AtomicUsize,
        fresh: AtomicBool,
        enabled_group_a: AtomicBool,
    }

    #[async_trait]
    impl CachedDiscovery for FakeCachedDiscovery {
        fn fresh(&self) -> bool {
            self.fresh.load(Ordering::SeqCst)
        }
        fn invalidate(&self) {
            self.invalidate_calls.fetch_add(1, Ordering::SeqCst);
            self.fresh.store(true, Ordering::SeqCst);
            self.enabled_group_a.store(true, Ordering::SeqCst);
        }
        async fn group_resources(&self) -> Result<Vec<ApiGroupResources>, MapperError> {
            if self.enabled_group_a.load(Ordering::SeqCst) {
                Ok(vec![group_a()])
            } else {
                Ok(Vec::new())
            }
        }
    }

    fn calls(c: &FakeCachedDiscovery) -> usize {
        c.invalidate_calls.load(Ordering::SeqCst)
    }

    /// `TestDeferredDiscoveryRESTMapper_CacheMiss` (discovery_test.go:245-287).
    #[tokio::test]
    async fn deferred_mapper_cache_miss_invalidates_once_while_fresh() {
        let cdc = Arc::new(FakeCachedDiscovery::default());
        let m = DeferredRestMapper::new(cdc.clone());
        assert!(!cdc.fresh(), "should NOT be fresh after instantiation");
        assert_eq!(0, calls(&cdc), "should not have called Invalidate()");

        let foo = Gvr::new("a", "v1", "foo");
        let gvk = m.kind_for(&foo).await.unwrap();
        assert!(cdc.fresh(), "should be fresh after a cache-miss");
        assert_eq!(1, calls(&cdc), "should have called Invalidate() once");
        assert_eq!("Foo", gvk.kind);

        m.kind_for(&foo).await.unwrap();
        assert_eq!(1, calls(&cdc), "should NOT have called Invalidate() again");

        let bar = Gvr::new("a", "v1", "bar");
        assert!(m.kind_for(&bar).await.is_err());
        assert_eq!(
            1,
            calls(&cdc),
            "should NOT have called Invalidate() again after another cache-miss, but with fresh==true"
        );

        cdc.fresh.store(false, Ordering::SeqCst);
        assert!(m.kind_for(&bar).await.is_err());
        assert_eq!(
            2,
            calls(&cdc),
            "should HAVE called Invalidate() again after another cache-miss, but with fresh==false"
        );
    }

    /// The same guard on every lookup `DeferredDiscoveryRESTMapper` wraps
    /// (discovery.go:240-327); the list-returning ones retry on an empty
    /// result (`len(gvks) == 0`) rather than on an error.
    #[tokio::test]
    async fn deferred_mapper_every_lookup_retries_on_a_miss() {
        let foo = Gvr::new("a", "v1", "foo");
        let gk = Gk::new("a", "Foo");
        let new = || {
            let cdc = Arc::new(FakeCachedDiscovery::default());
            (cdc.clone(), DeferredRestMapper::new(cdc))
        };
        let (c, m) = new();
        assert_eq!(m.kinds_for(&foo).await.unwrap().len(), 1);
        assert_eq!(1, calls(&c), "kinds_for");
        let (c, m) = new();
        assert_eq!(m.resource_for(&foo).await.unwrap().resource, "foo");
        assert_eq!(1, calls(&c), "resource_for");
        let (c, m) = new();
        assert_eq!(m.resources_for(&foo).await.unwrap().len(), 1);
        assert_eq!(1, calls(&c), "resources_for");
        let (c, m) = new();
        assert_eq!(
            m.rest_mapping(&gk, &[]).await.unwrap().resource.resource,
            "foo"
        );
        assert_eq!(1, calls(&c), "rest_mapping");
        let (c, m) = new();
        assert_eq!(m.rest_mappings(&gk, &[]).await.unwrap().len(), 1);
        assert_eq!(1, calls(&c), "rest_mappings");
        let (c, m) = new();
        // DefaultRESTMapper.ResourceSingularizer never errors on an unknown
        // resource (restmapper.go:152-180), so it never invalidates.
        m.resource_singularizer("foo").await.unwrap();
        assert_eq!(0, calls(&c), "resource_singularizer");
    }

    /// `Reset` (discovery.go:213-222): invalidates the client and drops the
    /// delegate, so the next mapping re-discovers.
    #[tokio::test]
    async fn reset_invalidates_the_client_and_rediscovers() {
        let cdc = Arc::new(FakeCachedDiscovery::default());
        let m = DeferredRestMapper::new(cdc.clone());
        m.reset().await;
        assert_eq!(1, calls(&cdc));
        m.kind_for(&Gvr::new("a", "v1", "foo")).await.unwrap();
        assert_eq!(1, calls(&cdc), "a hit on the fresh delegate");
        m.reset().await;
        assert_eq!(2, calls(&cdc));
    }

    struct Counting(AtomicUsize);
    #[async_trait]
    impl DiscoveryClient for Counting {
        async fn group_resources(&self) -> Result<Vec<ApiGroupResources>, MapperError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(crate::restmapper::served_group_resources(&[]))
        }
    }

    /// getDelegate (discovery.go:194-211) builds the delegate once; memcache
    /// (:235) lists the server once until invalidated.
    #[tokio::test]
    async fn delegate_and_discovery_are_cached_between_lookups() {
        let client = Arc::new(Counting(AtomicUsize::new(0)));
        let m = DeferredRestMapper::new(Arc::new(MemCacheClient::new(client.clone())));
        let pods = Gvr::new("", "v1", "pods");
        for _ in 0..3 {
            m.resource_for(&pods).await.unwrap();
        }
        assert_eq!(1, client.0.load(Ordering::SeqCst));
        m.reset().await;
        m.resource_for(&pods).await.unwrap();
        assert_eq!(2, client.0.load(Ordering::SeqCst));
    }

    struct Growing(AtomicBool);
    #[async_trait]
    impl DiscoveryClient for Growing {
        async fn group_resources(&self) -> Result<Vec<ApiGroupResources>, MapperError> {
            let mut g = crate::restmapper::served_group_resources(&[]);
            if self.0.load(Ordering::SeqCst) {
                g.push(group_a());
            }
            Ok(g)
        }
    }

    /// A resource that appears after the cache is populated is not seen by a
    /// miss on a fresh cache (`Fresh` is `cacheValid`, memcache.go:209-217),
    /// only after the periodic `Reset`.
    #[tokio::test]
    async fn new_resources_appear_after_reset_not_on_a_fresh_miss() {
        let client = Arc::new(Growing(AtomicBool::new(false)));
        let m = DeferredRestMapper::new(Arc::new(MemCacheClient::new(client.clone())));
        let foo = Gvr::new("a", "v1", "foo");
        m.resource_for(&Gvr::new("", "v1", "pods")).await.unwrap();
        client.0.store(true, Ordering::SeqCst);
        assert!(m.resource_for(&foo).await.is_err());
        m.reset().await;
        assert_eq!("foo", m.resource_for(&foo).await.unwrap().resource);
    }

    /// controllermanager.go:518.
    #[test]
    fn reset_period_is_thirty_seconds() {
        assert_eq!(RESET_PERIOD, Duration::from_secs(30));
    }
}
