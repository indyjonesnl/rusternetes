//! Port of `pkg/registry/core/service/ipallocator/cidrallocator.go`: the
//! `MetaAllocator`, one [`Allocator`] per ServiceCIDR prefix of a family.
//!
//! Upstream keeps the allocator set in sync from a ServiceCIDR informer
//! (`syncAllocators` on every add/update, `deleteServiceCIDR` on delete).
//! Rusternetes has no informer in the api-server, so `sync_allocators` runs
//! against a fresh ServiceCIDR list before each operation. The same rules
//! decide which prefixes exist and which are ready.

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use ipnet::IpNet;
use rusternetes_common::resources::{Service, ServiceCIDR};
use rusternetes_storage::Storage;
use tokio::sync::Mutex;

use super::{family_of, list_managed, prefix_contains_ip, Allocator, IpError, IpResult};

/// `item` (cidrallocator.go:80-83).
struct Item<S: Storage> {
    allocator: Arc<Allocator<S>>,
    /// The ServiceCIDRs using this allocator.
    service_cidrs: HashSet<String>,
}

/// `MetaAllocator` (cidrallocator.go:58-78), without the dual-write bitmap:
/// `DisableAllocatorDualWrite` is GA and locked on in 1.35
/// (pkg/features/kube_features.go:1279).
pub struct MetaAllocator<S: Storage> {
    storage: Arc<S>,
    allocators: Mutex<HashMap<String, Item<S>>>,
    ip_family: &'static str,
}

/// `isReady` (cidrallocator.go:520-533): the Ready condition, true when
/// absent.
fn is_ready(sc: &ServiceCIDR) -> bool {
    sc.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|c| c.iter().find(|c| c.condition_type == "Ready"))
        .map(|c| c.status == "True")
        .unwrap_or(true)
}

impl<S: Storage> MetaAllocator<S> {
    /// `NewMetaAllocator` (cidrallocator.go:88-102).
    pub fn new(storage: Arc<S>, is_ipv6: bool) -> Self {
        Self {
            storage,
            allocators: Mutex::new(HashMap::new()),
            ip_family: if is_ipv6 { "IPv6" } else { "IPv4" },
        }
    }

    /// `syncAllocators` (cidrallocator.go:240-297) plus the removal half of
    /// `deleteServiceCIDR` (:147-178): one allocator per prefix of this
    /// family, kept while any ServiceCIDR still names it.
    async fn sync_allocators(&self) -> rusternetes_common::Result<()> {
        let service_cidrs: Vec<ServiceCIDR> = self.storage.list("/registry/servicecidrs/").await?;
        let mut allocators = self.allocators.lock().await;

        let mut present: HashMap<String, HashSet<String>> = HashMap::new();
        for sc in &service_cidrs {
            let Some(spec) = &sc.spec else { continue };
            for cidr in &spec.cidrs {
                let Ok(prefix) = cidr.parse::<IpNet>() else {
                    tracing::info!("error parsing cidr {cidr}");
                    continue;
                };
                if family_of(&prefix.addr()) != self.ip_family {
                    continue;
                }
                present
                    .entry(cidr.clone())
                    .or_default()
                    .insert(sc.metadata.name.clone());
                let ready = is_ready(sc) && sc.metadata.deletion_timestamp.is_none();

                if let Some(item) = allocators.get_mut(cidr) {
                    item.service_cidrs.insert(sc.metadata.name.clone());
                    if ready {
                        item.allocator.ready.store(true, Ordering::SeqCst);
                    } else if item.service_cidrs.len() == 1 {
                        item.allocator.ready.store(false, Ordering::SeqCst);
                    }
                    continue;
                }

                let allocator = match Allocator::new(prefix, self.storage.clone()) {
                    Ok(a) => a,
                    Err(e) => {
                        tracing::info!(
                            "error creating new IPAllocator for Service CIDR {cidr}: {e}"
                        );
                        continue;
                    }
                };
                allocator.ready.store(ready, Ordering::SeqCst);
                allocators.insert(
                    cidr.clone(),
                    Item {
                        allocator: Arc::new(allocator),
                        service_cidrs: HashSet::from([sc.metadata.name.clone()]),
                    },
                );
                tracing::info!("created ClusterIP allocator for Service CIDR {cidr}");
            }
        }

        // `deleteServiceCIDR`: drop a ServiceCIDR that is gone from the
        // allocators it used, and an allocator no ServiceCIDR uses.
        allocators.retain(|cidr, item| {
            let names = present.get(cidr);
            item.service_cidrs
                .retain(|n| names.is_some_and(|names| names.contains(n)));
            let keep = !item.service_cidrs.is_empty();
            if !keep {
                tracing::info!("deleted ClusterIP allocator for Service CIDR {cidr}");
            }
            keep
        });
        Ok(())
    }

    /// `getAllocator` (cidrallocator.go:299-322).
    async fn get_allocator(&self, ip: IpAddr, ready: bool) -> IpResult<Arc<Allocator<S>>> {
        self.sync_allocators().await.map_err(IpError::Other)?;
        let allocators = self.allocators.lock().await;
        for (cidr, item) in allocators.iter() {
            let Ok(prefix) = cidr.parse::<IpNet>() else {
                continue;
            };
            if prefix_contains_ip(&prefix, &ip)
                && (!ready || item.allocator.ready.load(Ordering::SeqCst))
            {
                return Ok(item.allocator.clone());
            }
        }
        tracing::debug!("Could not get allocator for IP {ip}");
        Err(IpError::MismatchedNetwork)
    }

    /// `AllocateService` (cidrallocator.go:324-341).
    pub async fn allocate_service(
        &self,
        service: Option<&Service>,
        ip: IpAddr,
        dry_run: bool,
    ) -> IpResult<()> {
        let allocator = self.get_allocator(ip, true).await?;
        allocator.allocate_service(service, ip, dry_run).await
    }

    /// `AllocateNextService` (cidrallocator.go:348-384): the first
    /// allocator of this family that is neither full nor unready.
    pub async fn allocate_next_service(
        &self,
        service: Option<&Service>,
        dry_run: bool,
    ) -> IpResult<IpAddr> {
        self.sync_allocators().await.map_err(IpError::Other)?;
        let allocators: Vec<Arc<Allocator<S>>> = self
            .allocators
            .lock()
            .await
            .values()
            .map(|i| i.allocator.clone())
            .collect();
        if dry_run {
            // `DryRun` (cidrallocator.go:509-518) hands back the first
            // allocator's dry-run shim.
            return match allocators.first() {
                Some(a) => a.allocate_next_service(service, true).await,
                None => Err(IpError::NotReady),
            };
        }
        for allocator in allocators {
            if allocator.ip_family() != self.ip_family {
                continue;
            }
            match allocator.allocate_next_service(service, false).await {
                Ok(ip) => return Ok(ip),
                Err(IpError::Full) | Err(IpError::NotReady) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(IpError::Full)
    }

    /// `Release` (cidrallocator.go:390-404): unready allocators still
    /// release.
    pub async fn release(&self, ip: IpAddr, dry_run: bool) -> IpResult<()> {
        let allocator = self.get_allocator(ip, false).await?;
        allocator.release(ip, dry_run).await
    }

    /// `Has` (cidrallocator.go:427-434).
    #[allow(dead_code)] // upstream API; exercised by the tests
    pub async fn has(&self, ip: IpAddr) -> bool {
        match self.get_allocator(ip, true).await {
            Ok(a) => a.has(ip).await,
            Err(_) => false,
        }
    }

    /// `Used` (cidrallocator.go:446-456).
    #[allow(dead_code)] // upstream API; exercised by the tests
    pub async fn used(&self) -> usize {
        list_managed(&*self.storage, self.ip_family).await.len()
    }

    /// `Free` (cidrallocator.go:459-484): the sizes of the prefixes no
    /// other prefix contains, less what is used.
    #[allow(dead_code)] // upstream API; exercised by the tests
    pub async fn free(&self) -> u64 {
        let _ = self.sync_allocators().await;
        let allocators = self.allocators.lock().await;
        let prefixes: Vec<IpNet> = allocators.keys().filter_map(|c| c.parse().ok()).collect();
        let mut size: u64 = 0;
        for prefix in &prefixes {
            let contained = prefixes.iter().any(|p| {
                p != prefix && p.contains(prefix) && prefix.prefix_len() >= p.prefix_len()
            });
            if contained {
                continue;
            }
            if let Some(item) = allocators.get(&prefix.to_string()) {
                size = size.saturating_add(item.allocator.size());
            }
        }
        drop(allocators);
        size.saturating_sub(self.used().await as u64)
    }
}
