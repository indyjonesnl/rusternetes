//! Port of `pkg/registry/core/service/portallocator/controller/repair.go`:
//! a loop that rebuilds the NodePort allocation from the stored Services,
//! repairs ports missing from it, and frees ports leaked for three passes in
//! a row.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use rusternetes_common::resources::{EventSource, EventType, ObjectReference, Service};
use rusternetes_common::{Error, Result};
use rusternetes_storage::{EventRecorder, Storage};
use tracing::{error, warn};

use super::{PortAllocator, PortError, PortRange};
use crate::registry::core::service::allocator::storage::RangeRegistry;

/// `repairLoopInterval` (pkg/controlplane/instance.go:122), upstream's
/// `IPRepairInterval` for both repair loops.
pub const REPAIR_INTERVAL: Duration = Duration::from_secs(3 * 60);

/// `numRepairsBeforeLeakCleanup` (repair.go:53-55).
pub const NUM_REPAIRS_BEFORE_LEAK_CLEANUP: u32 = 3;

/// The recorder's component (repair.go:62).
const COMPONENT: &str = "portallocator-repair-controller";

/// `Repair` (repair.go:40-50).
pub struct Repair<S: Storage + 'static> {
    storage: Arc<S>,
    port_range: PortRange,
    alloc: Arc<dyn RangeRegistry>,
    leaks: HashMap<usize, u32>,
    recorder: EventRecorder<S>,
}

impl<S: Storage + 'static> Repair<S> {
    /// `NewRepair` (repair.go:57-73).
    pub fn new(storage: Arc<S>, port_range: PortRange, alloc: Arc<dyn RangeRegistry>) -> Self {
        Self {
            recorder: EventRecorder::new(storage.clone()),
            storage,
            port_range,
            alloc,
            leaks: HashMap::new(),
        }
    }

    /// `runOnce` (repair.go:89-98): `doRunOnce` under
    /// `retry.RetryOnConflict(retry.DefaultBackoff, ...)` — 4 steps from
    /// 10ms, factor 5 (client-go/util/retry/util.go:38-43).
    pub async fn run_once(&mut self) -> Result<()> {
        let mut delay = Duration::from_millis(10);
        let mut steps = 4;
        loop {
            match self.do_run_once().await {
                Err(Error::Conflict(_)) if steps > 1 => {
                    steps -= 1;
                    tokio::time::sleep(delay).await;
                    delay *= 5;
                }
                other => return other,
            }
        }
    }

    async fn event(&self, svc: &Service, reason: &str, message: String) {
        let involved = ObjectReference {
            kind: Some("Service".to_string()),
            namespace: svc.metadata.namespace.clone(),
            name: Some(svc.metadata.name.clone()),
            uid: Some(svc.metadata.uid.clone()),
            api_version: Some("v1".to_string()),
            resource_version: svc.metadata.resource_version.clone(),
            field_path: None,
        };
        let source = EventSource {
            component: COMPONENT.to_string(),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(&involved, &source, EventType::Warning, reason, &message)
            .await
        {
            warn!("{COMPONENT}: could not record {reason} event: {e}");
        }
    }

    /// `doRunOnce` (repair.go:100-229).
    async fn do_run_once(&mut self) -> Result<()> {
        // Wait up to 30s for storage (repair.go:111-121).
        let mut snapshot = None;
        let mut last_err = None;
        for attempt in 0..=30 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            match self.alloc.get().await {
                Ok(s) => {
                    snapshot = Some(s);
                    break;
                }
                Err(e) => {
                    error!("unable to refresh the port allocations: {e}");
                    last_err = Some(e);
                }
            }
        }
        let Some(mut snapshot) = snapshot else {
            return Err(Error::Internal(format!(
                "unable to refresh the port allocations: {}",
                last_err.map(|e| e.to_string()).unwrap_or_default()
            )));
        };
        // Not yet initialised.
        if snapshot.range.is_empty() {
            snapshot.range = self.port_range.to_string();
        }
        let stored = PortAllocator::from_snapshot(&snapshot).await.map_err(|e| {
            Error::Internal(format!("unable to rebuild allocator from snapshot: {e}"))
        })?;

        let services: Vec<Service> = self
            .storage
            .list("/registry/services/")
            .await
            .map_err(|e| Error::Internal(format!("unable to refresh the port block: {e}")))?;

        let rebuilt = PortAllocator::new_in_memory(self.port_range);
        // Check every Service's ports, and rebuild the state as it should be.
        for svc in &services {
            for port in collect_service_node_ports(svc) {
                let (name, ns) = (
                    &svc.metadata.name,
                    svc.metadata.namespace.as_deref().unwrap_or(""),
                );
                match rebuilt.allocate(port).await {
                    Ok(()) => {
                        if stored.has(port).await {
                            // Remove it from the old set, so leaks show.
                            let _ = stored.release(port).await;
                        } else {
                            self.event(
                                svc,
                                "PortNotAllocated",
                                format!("Port {port} is not allocated; repairing"),
                            )
                            .await;
                            error!("the node port {port} for service {name}/{ns} is not allocated; repairing");
                        }
                        // It is used, so it cannot be leaked.
                        self.leaks.remove(&port);
                    }
                    Err(PortError::Allocated) => {
                        self.event(svc, "PortAlreadyAllocated", format!("Port {port} was assigned to multiple services; please recreate service")).await;
                        error!("the node port {port} for service {name}/{ns} was assigned to multiple services; please recreate");
                    }
                    Err(PortError::NotInRange { .. }) => {
                        let pr = self.port_range;
                        self.event(svc, "PortOutOfRange", format!("Port {port} is not within the port range {pr}; please recreate service")).await;
                        error!("the port {port} for service {name}/{ns} is not within the port range {pr}; please recreate");
                    }
                    Err(PortError::Full) => {
                        let pr = self.port_range;
                        self.event(svc, "PortRangeFull", format!("Port range {pr} is full; you must widen the port range in order to create new services")).await;
                        return Err(Error::Internal(format!(
                            "the port range {pr} is full; you must widen the port range in order to create new services"
                        )));
                    }
                    Err(e) => {
                        self.event(
                            svc,
                            "UnknownError",
                            format!("Unable to allocate port {port} due to an unknown error"),
                        )
                        .await;
                        return Err(Error::Internal(format!(
                            "unable to allocate port {port} for service {name}/{ns} due to an unknown error, exiting: {e}"
                        )));
                    }
                }
            }
        }

        // Ports left in the old set appear to have leaked (repair.go:194-216).
        for port in stored.allocated().await {
            let count = match self.leaks.get(&port) {
                None => {
                    // Flag it for clean up after any races are gone.
                    error!("the node port {port} may have leaked: flagging for later clean up");
                    Some(NUM_REPAIRS_BEFORE_LEAK_CLEANUP - 1)
                }
                Some(&c) if c > 0 => Some(c),
                Some(_) => None,
            };
            match count {
                Some(count) => {
                    // Pretend it is still in use until the count expires.
                    self.leaks.insert(port, count - 1);
                    if let Err(e) = rebuilt.allocate(port).await {
                        error!(
                            "the node port {port} may have leaked, but can not be allocated: {e}"
                        );
                    }
                }
                None => {
                    // Left out of the rebuilt set: free for reuse.
                    error!("the node port {port} appears to have leaked: cleaning up");
                }
            }
        }

        // Blast the rebuilt state into storage.
        rebuilt.snapshot(&mut snapshot).await.map_err(|e| {
            Error::Internal(format!(
                "unable to snapshot the updated port allocations: {e}"
            ))
        })?;
        match self.alloc.create_or_update(snapshot).await {
            Ok(()) => Ok(()),
            Err(e @ Error::Conflict(_)) => Err(e),
            Err(e) => Err(Error::Internal(format!(
                "unable to persist the updated port allocations: {e}"
            ))),
        }
    }
}

/// `collectServiceNodePorts` (repair.go:231-268): a nodePort repeated with
/// the same protocol counts twice (so the duplicate is caught), one shared
/// across protocols once, and the health-check port unless a non-TCP port
/// already uses it.
pub fn collect_service_node_ports(service: &Service) -> Vec<usize> {
    let mut ports = Vec::new();
    let mut seen: HashMap<usize, HashSet<String>> = HashMap::new();
    for port in &service.spec.ports {
        let Some(node_port) = port.node_port.filter(|p| *p != 0).map(usize::from) else {
            continue;
        };
        let protocols = seen.entry(node_port).or_default();
        if protocols.is_empty() {
            protocols.insert(port.protocol.clone());
            ports.push(node_port);
        } else if protocols.contains(&port.protocol) {
            ports.push(node_port);
        } else {
            protocols.insert(port.protocol.clone());
        }
    }
    if let Some(health) = service
        .spec
        .health_check_node_port
        .filter(|p| *p != 0)
        .map(|p| p as usize)
    {
        if seen.get(&health).is_none_or(|s| s.contains("TCP")) {
            ports.push(health);
        }
    }
    ports
}
