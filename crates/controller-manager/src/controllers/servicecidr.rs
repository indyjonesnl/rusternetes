//! ServiceCIDR controller.
//!
//! Direct port of upstream `pkg/controller/servicecidrs/servicecidrs_controller.go`
//! (the `service-cidr-controller`). It owns three things upstream, and all three
//! are ported here:
//!
//! 1. **A protection finalizer.** Every live ServiceCIDR carries
//!    `networking.k8s.io/service-cidr-finalizer`
//!    (`servicecidrs_controller.go:61`, `addServiceCIDRFinalizerIfNeeded` at
//!    `:418`), so a `DELETE` marks the object terminating instead of removing a
//!    range out from under live Services.
//! 2. **The `Ready` condition.** `Ready=True` / message
//!    "Kubernetes Service CIDR is ready" / no reason on a healthy range
//!    (`:341-346`), and `Ready=False` with reason `Terminating`
//!    (`:313-320`) while IPAddresses still reference a range being deleted.
//! 3. **`canDeleteCIDR` + a deletion grace period.** The finalizer is only
//!    dropped once no IPAddress would be orphaned (`:357-416`) *and* the
//!    deletion has been visible for `deletionGracePeriod` (`:63-66`, 10s), so
//!    the allocators observe the deletion before an IP is handed out of a range
//!    that is going away.
//!
//! Divergence from upstream, and why: upstream removes the finalizer via a
//! PATCH and lets the apiserver do the actual removal once the last finalizer
//! is gone. Controllers in this workspace write straight to storage, with no
//! apiserver in the loop, so the finalizer removal and the delete happen
//! together here (same shape as `replicationcontroller.rs`'s orphan path).

use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use ipnet::IpNet;
use rusternetes_common::resources::{
    IPAddress, ServiceCIDR, ServiceCIDRCondition, ServiceCIDRStatus,
};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

/// Upstream `ServiceCIDRProtectionFinalizer`
/// (`pkg/controller/servicecidrs/servicecidrs_controller.go:61`).
pub const SERVICE_CIDR_PROTECTION_FINALIZER: &str = "networking.k8s.io/service-cidr-finalizer";

/// Upstream `deletionGracePeriod` (`servicecidrs_controller.go:63-66`): how long
/// to wait after the deletionTimestamp before dropping the finalizer, so the
/// deletion has propagated to the allocators and no new IP is handed out of a
/// range that is going away.
pub const DELETION_GRACE_PERIOD: Duration = Duration::from_secs(10);

/// Upstream `networkingapiv1.ServiceCIDRConditionReady`
/// (`staging/src/k8s.io/api/networking/v1/types.go:738`).
pub const CONDITION_READY: &str = "Ready";

/// Upstream `networkingapiv1.ServiceCIDRReasonTerminating`
/// (`staging/src/k8s.io/api/networking/v1/types.go:741`).
pub const REASON_TERMINATING: &str = "Terminating";

/// Upstream `servicecidrs_controller.go:344`. Applied with **no reason** — the
/// controller builds the condition without one, and ServiceCIDR status is not
/// condition-validated (`ValidateServiceCIDRStatusUpdate`,
/// `pkg/apis/networking/validation/validation.go:883-886`).
pub const READY_MESSAGE: &str = "Kubernetes Service CIDR is ready";

/// Upstream `servicecidrs_controller.go:317`, verbatim.
pub const TERMINATING_MESSAGE: &str = "There are still IPAddresses referencing the ServiceCIDR, please remove them or create a new ServiceCIDR";

/// Upstream `networkingapiv1.LabelIPAddressFamily`
/// (`staging/src/k8s.io/api/networking/v1/well_known_labels.go:26`).
pub const LABEL_IP_ADDRESS_FAMILY: &str = "ipaddress.kubernetes.io/ip-family";

/// Upstream `networkingapiv1.LabelManagedBy`
/// (`staging/src/k8s.io/api/networking/v1/well_known_labels.go:32`).
pub const LABEL_MANAGED_BY: &str = "ipaddress.kubernetes.io/managed-by";

/// Upstream `ipallocator.ControllerName`
/// (`pkg/registry/core/service/ipallocator/ipallocator.go:45`). Only IPAddresses
/// bearing this `managed-by` value can block a ServiceCIDR deletion — an
/// IPAddress managed by something else is not the service allocator's to
/// protect (`containingServiceCIDRs`, `servicecidrs_controller.go:234-238`).
pub const IP_ALLOCATOR_CONTROLLER_NAME: &str = "ipallocator.k8s.io";

/// Upstream `PrefixContainsIP` (`pkg/api/servicecidr/servicecidr.go:96-110`).
///
/// Stricter than a plain `contains`: the network address is never contained,
/// and for IPv4 neither is the broadcast address, because a ServiceCIDR does
/// not allocate those — so a Service holding one of them does not belong to
/// that range.
pub fn prefix_contains_ip(prefix: &IpNet, ip: &IpAddr) -> bool {
    if prefix.network() == *ip {
        return false;
    }
    if ip.is_ipv4() && prefix.broadcast() == *ip {
        return false;
    }
    prefix.contains(ip)
}

/// Upstream `ContainsPrefix` (`pkg/api/servicecidr/servicecidr.go:48-66`): the
/// ServiceCIDRs holding a prefix equal to or larger than `prefix`.
fn contains_prefix<'a>(all: &'a [ServiceCIDR], prefix: &IpNet) -> Vec<&'a ServiceCIDR> {
    all.iter()
        .filter(|sc| {
            spec_cidrs(sc)
                .iter()
                .filter_map(|c| c.parse::<IpNet>().ok())
                // `p.Overlaps(prefix) && p.Bits() <= prefix.Bits()` upstream —
                // i.e. p contains prefix, which is what `IpNet::contains` means.
                .any(|p| p.contains(prefix))
        })
        .collect()
}

/// Upstream `ContainsAddress` (`pkg/api/servicecidr/servicecidr.go:75-92`).
fn contains_address<'a>(all: &'a [ServiceCIDR], addr: &IpAddr) -> Vec<&'a ServiceCIDR> {
    all.iter()
        .filter(|sc| {
            spec_cidrs(sc)
                .iter()
                .filter_map(|c| c.parse::<IpNet>().ok())
                .any(|p| prefix_contains_ip(&p, addr))
        })
        .collect()
}

fn spec_cidrs(sc: &ServiceCIDR) -> &[String] {
    sc.spec.as_ref().map(|s| s.cidrs.as_slice()).unwrap_or(&[])
}

/// Upstream `convertToV1IPFamily` (`servicecidrs_controller.go:502-511`) applied
/// to the CIDR's family, which is the `ipaddress.kubernetes.io/ip-family` label
/// value the allocator stamps on its IPAddresses.
fn cidr_ip_family(cidr: &str) -> Option<&'static str> {
    match cidr.parse::<IpNet>().ok()? {
        IpNet::V4(_) => Some("IPv4"),
        IpNet::V6(_) => Some("IPv6"),
    }
}

fn label<'a>(meta: &'a rusternetes_common::types::ObjectMeta, key: &str) -> Option<&'a str> {
    meta.labels.as_ref()?.get(key).map(String::as_str)
}

/// Upstream `canDeleteCIDR` (`servicecidrs_controller.go:357-416`): may this
/// ServiceCIDR be removed without orphaning an IPAddress?
pub fn can_delete_cidr(cidr: &ServiceCIDR, all: &[ServiceCIDR], ips: &[IPAddress]) -> bool {
    // Is there another ServiceCIDR that contains this one? If so, every IP in
    // this range stays covered after the delete and it is safe to remove.
    let mut has_parent = true;
    for c in spec_cidrs(cidr) {
        if let Ok(prefix) = c.parse::<IpNet>() {
            let parents = contains_prefix(all, &prefix);
            if parents.is_empty()
                || (parents.len() == 1 && parents[0].metadata.name == cidr.metadata.name)
            {
                has_parent = false;
            }
        }
    }
    if has_parent {
        debug!(
            "Deleting ServiceCIDR {} contained in other ServiceCIDR",
            cidr.metadata.name
        );
        return true;
    }

    // No parent range: any IPAddress whose *only* covering ServiceCIDR is this
    // one would be orphaned, so the deletion is blocked.
    for c in spec_cidrs(cidr) {
        let Some(family) = cidr_ip_family(c) else {
            continue;
        };
        for ip in ips {
            // Only IPs managed by the kube-apiserver's service allocator count.
            if label(&ip.metadata, LABEL_IP_ADDRESS_FAMILY) != Some(family)
                || label(&ip.metadata, LABEL_MANAGED_BY) != Some(IP_ALLOCATOR_CONTROLLER_NAME)
            {
                continue;
            }
            let Ok(addr) = ip.metadata.name.parse::<IpAddr>() else {
                // The IPAddress object validates its name is a valid IP.
                debug!(
                    "[SHOULD NOT HAPPEN] unexpected error parsing IPAddress {}",
                    ip.metadata.name
                );
                continue;
            };
            let covering = contains_address(all, &addr);
            if covering.len() == 1 && covering[0].metadata.name == cidr.metadata.name {
                info!(
                    "Deleting ServiceCIDR {} blocked by IP address {}",
                    cidr.metadata.name, addr
                );
                return false;
            }
        }
    }

    debug!(
        "Deleting ServiceCIDR {} no longer have orphan IPs",
        cidr.metadata.name
    );
    true
}

/// Upstream `service-cidr-controller`.
pub struct ServiceCIDRController<S: Storage> {
    storage: Arc<S>,
    interval: Duration,
    deletion_grace_period: Duration,
}

impl<S: Storage + 'static> ServiceCIDRController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            interval: Duration::from_secs(30),
            deletion_grace_period: DELETION_GRACE_PERIOD,
        }
    }

    /// Shorten the grace period. Only for tests — production must keep
    /// upstream's 10s so the allocators observe the deletion first. (`main.rs`
    /// compiles the controller modules into the binary too, where a test-only
    /// builder is legitimately unused.)
    #[allow(dead_code)]
    pub fn with_deletion_grace_period(mut self, period: Duration) -> Self {
        self.deletion_grace_period = period;
        self
    }

    /// Watch-driven run loop with a periodic resync fallback, matching the
    /// other controllers in this crate. Upstream is informer + workqueue over
    /// both ServiceCIDRs and IPAddresses; both are watched here for the same
    /// reason — an IPAddress appearing or going away can block or unblock a
    /// pending ServiceCIDR deletion (`addIPAddress` / `deleteIPAddress`,
    /// `servicecidrs_controller.go:184-212`).
    ///
    /// The resync also drives the deletion grace period: a ServiceCIDR still
    /// inside its grace window is left alone and picked up on a later pass,
    /// which is this loop's equivalent of upstream's `queue.AddAfter`.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting ServiceCIDR controller");

        loop {
            if let Err(e) = self.reconcile_all().await {
                error!("ServiceCIDR reconcile failed: {}", e);
            }

            let cidr_prefix = build_prefix("servicecidrs", None);
            let ip_prefix = build_prefix("ipaddresses", None);
            let (mut cidr_watch, mut ip_watch) = match tokio::try_join!(
                self.storage.watch(&cidr_prefix),
                self.storage.watch(&ip_prefix)
            ) {
                Ok(w) => w,
                Err(e) => {
                    warn!(
                        "ServiceCIDR watch failed: {e}; retrying in {:?}",
                        self.interval
                    );
                    time::sleep(self.interval).await;
                    continue;
                }
            };

            // The grace period is shorter than the resync, so poll at least
            // that often while a deletion is pending.
            let tick = self.interval.min(self.deletion_grace_period);
            let mut resync = time::interval(tick);
            resync.tick().await; // drop the immediate first tick

            loop {
                let reconcile = tokio::select! {
                    ev = cidr_watch.next() => match ev {
                        Some(Ok(_)) => true,
                        // Stream ended/errored -> reconnect via the outer loop.
                        _ => break,
                    },
                    ev = ip_watch.next() => match ev {
                        Some(Ok(_)) => true,
                        _ => break,
                    },
                    _ = resync.tick() => true,
                };
                if reconcile {
                    if let Err(e) = self.reconcile_all().await {
                        error!("ServiceCIDR reconcile failed: {}", e);
                    }
                }
            }
        }
    }

    pub async fn reconcile_all(&self) -> Result<()> {
        let cidrs: Vec<ServiceCIDR> = self
            .storage
            .list(&build_prefix("servicecidrs", None))
            .await?;
        if cidrs.is_empty() {
            return Ok(());
        }
        let ips: Vec<IPAddress> = self
            .storage
            .list(&build_prefix("ipaddresses", None))
            .await
            .unwrap_or_default();

        for cidr in &cidrs {
            if let Err(e) = self.reconcile_one(cidr, &cidrs, &ips).await {
                error!("ServiceCIDR {} reconcile failed: {}", cidr.metadata.name, e);
            }
        }
        Ok(())
    }

    /// Upstream `sync` (`servicecidrs_controller.go:285-354`).
    pub async fn reconcile_one(
        &self,
        cidr: &ServiceCIDR,
        all: &[ServiceCIDR],
        ips: &[IPAddress],
    ) -> Result<()> {
        // Deleting ...
        if let Some(deleted_at) = cidr.metadata.deletion_timestamp {
            if !can_delete_cidr(cidr, all, ips) {
                // Say why it cannot go: re-evaluated whenever a ServiceCIDR or
                // IPAddress event may lift the block.
                return self
                    .update_condition_if_needed(
                        cidr,
                        "False",
                        REASON_TERMINATING,
                        TERMINATING_MESSAGE,
                    )
                    .await;
            }

            // Safe to remove — but only after the deletion has been visible
            // long enough for the allocators to stop handing out IPs from it.
            let elapsed = Utc::now().signed_duration_since(deleted_at);
            let grace = chrono::Duration::from_std(self.deletion_grace_period)
                .unwrap_or_else(|_| chrono::Duration::seconds(10));
            if elapsed < grace {
                debug!(
                    "ServiceCIDR {} still within the deletion grace period",
                    cidr.metadata.name
                );
                return Ok(());
            }
            return self.remove_finalizer_if_needed(cidr).await;
        }

        // Created or Updated: the ServiceCIDR must have a finalizer.
        self.add_finalizer_if_needed(cidr).await?;
        self.update_condition_if_needed(cidr, "True", "", READY_MESSAGE)
            .await
    }

    /// Upstream `addServiceCIDRFinalizerIfNeeded` (`:418-441`).
    async fn add_finalizer_if_needed(&self, cidr: &ServiceCIDR) -> Result<()> {
        if has_protection_finalizer(cidr) {
            return Ok(());
        }
        let key = build_key("servicecidrs", None, &cidr.metadata.name);
        let mut current: ServiceCIDR = self.storage.get(&key).await?;
        if has_protection_finalizer(&current) {
            return Ok(());
        }
        current
            .metadata
            .finalizers
            .get_or_insert_with(Vec::new)
            .push(SERVICE_CIDR_PROTECTION_FINALIZER.to_string());
        self.storage.update(&key, &current).await?;
        debug!(
            "Added protection finalizer to ServiceCIDR {}",
            cidr.metadata.name
        );
        Ok(())
    }

    /// Upstream `removeServiceCIDRFinalizerIfNeeded` (`:443-469`).
    ///
    /// Upstream PATCHes the finalizer away and the apiserver reaps the object
    /// once the last one is gone. There is no apiserver between this controller
    /// and storage, so the reap happens here.
    async fn remove_finalizer_if_needed(&self, cidr: &ServiceCIDR) -> Result<()> {
        let key = build_key("servicecidrs", None, &cidr.metadata.name);
        let mut current: ServiceCIDR = match self.storage.get(&key).await {
            Ok(c) => c,
            // Already gone — nothing to do (upstream tolerates NotFound too).
            Err(_) => return Ok(()),
        };
        if !has_protection_finalizer(&current) {
            return Ok(());
        }
        if let Some(finalizers) = current.metadata.finalizers.as_mut() {
            finalizers.retain(|f| f != SERVICE_CIDR_PROTECTION_FINALIZER);
        }
        let no_finalizers_left = current
            .metadata
            .finalizers
            .as_ref()
            .is_none_or(|f| f.is_empty());
        if no_finalizers_left && current.metadata.deletion_timestamp.is_some() {
            self.storage.delete(&key).await?;
            info!("Deleted terminating ServiceCIDR {}", cidr.metadata.name);
        } else {
            self.storage.update(&key, &current).await?;
            debug!(
                "Removed protection finalizer from ServiceCIDR {}",
                cidr.metadata.name
            );
        }
        Ok(())
    }

    /// Upstream `updateConditionIfNeeded` (`:472-497`): a no-op when status,
    /// reason and message all already match, so `lastTransitionTime` only moves
    /// on a real transition.
    async fn update_condition_if_needed(
        &self,
        cidr: &ServiceCIDR,
        status: &str,
        reason: &str,
        message: &str,
    ) -> Result<()> {
        if condition_matches(cidr, status, reason, message) {
            return Ok(());
        }
        let key = build_key("servicecidrs", None, &cidr.metadata.name);
        let mut current: ServiceCIDR = self.storage.get(&key).await?;
        if condition_matches(&current, status, reason, message) {
            return Ok(());
        }
        let new = ServiceCIDRCondition {
            condition_type: CONDITION_READY.to_string(),
            status: status.to_string(),
            observed_generation: current.metadata.generation,
            last_transition_time: Some(Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()),
            reason: reason.to_string(),
            message: message.to_string(),
        };
        let conditions = current
            .status
            .get_or_insert(ServiceCIDRStatus { conditions: None })
            .conditions
            .get_or_insert_with(Vec::new);
        match conditions
            .iter_mut()
            .find(|c| c.condition_type == CONDITION_READY)
        {
            Some(existing) => *existing = new,
            None => conditions.push(new),
        }
        self.storage.update_status(&key, &current).await?;
        debug!(
            "Updated ServiceCIDR {} Ready condition to {}",
            cidr.metadata.name, status
        );
        Ok(())
    }
}

fn has_protection_finalizer(cidr: &ServiceCIDR) -> bool {
    cidr.metadata
        .finalizers
        .as_ref()
        .is_some_and(|f| f.iter().any(|x| x == SERVICE_CIDR_PROTECTION_FINALIZER))
}

fn condition_matches(cidr: &ServiceCIDR, status: &str, reason: &str, message: &str) -> bool {
    cidr.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|c| c.iter().find(|c| c.condition_type == CONDITION_READY))
        .is_some_and(|c| c.status == status && c.reason == reason && c.message == message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_and_broadcast_addresses_are_not_contained() {
        let prefix: IpNet = "10.96.0.0/24".parse().unwrap();
        assert!(!prefix_contains_ip(&prefix, &"10.96.0.0".parse().unwrap()));
        assert!(!prefix_contains_ip(
            &prefix,
            &"10.96.0.255".parse().unwrap()
        ));
        assert!(prefix_contains_ip(&prefix, &"10.96.0.1".parse().unwrap()));
        assert!(!prefix_contains_ip(&prefix, &"10.97.0.1".parse().unwrap()));
    }

    #[test]
    fn ipv6_broadcast_rule_does_not_apply() {
        let prefix: IpNet = "2001:db8::/64".parse().unwrap();
        // Network address still excluded ...
        assert!(!prefix_contains_ip(&prefix, &"2001:db8::".parse().unwrap()));
        // ... but the all-ones address is a normal IPv6 host address.
        assert!(prefix_contains_ip(
            &prefix,
            &"2001:db8::ffff:ffff:ffff:ffff".parse().unwrap()
        ));
    }

    #[test]
    fn ip_family_of_cidr() {
        assert_eq!(cidr_ip_family("10.96.0.0/12"), Some("IPv4"));
        assert_eq!(cidr_ip_family("2001:db8::/64"), Some("IPv6"));
        assert_eq!(cidr_ip_family("not-a-cidr"), None);
    }
}
