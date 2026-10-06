//! Service storage — port of `pkg/registry/core/service/storage/storage.go`:
//! the Store's `Decorator` (`defaultOnRead`), `BeginCreate`, `BeginUpdate`
//! and `AfterDelete` hooks, which keep ClusterIPs and node ports allocated
//! in step with the stored Services.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::deletion::DeleteOptions;
use rusternetes_common::resources::{
    Endpoints, IPFamily, IPFamilyPolicy, Service, ServiceExternalTrafficPolicy, ServiceType,
};
use rusternetes_common::validation::service::parse_ip_sloppy;
use rusternetes_common::{Error, Result};
use rusternetes_storage::StorageBackend;

use super::alloc::{self, ClusterIpAllocators, ClusterIpTxn, DEFAULT_SERVICE_IP_FAMILY};
use super::portallocator::operation::PortAllocationOperation;
use super::portallocator::PortAllocator;
use super::strategy::{StatusStrategy, Strategy};
use crate::registry::generic::store::{
    AfterDelete, BeginCreate, BeginUpdate, CreateOptions, Finish, UpdateOptions,
};
use crate::registry::generic::Store;
use crate::registry::rest::{GroupResource, RequestContext};

/// The v1 defaulting a decoded Service goes through: `SetDefaults_Service`
/// (pkg/apis/core/v1/defaults.go:106-162).
pub fn convert_to_internal(svc: &mut Service) {
    crate::handlers::defaults::apply_service_defaults(svc);
}

fn cluster_ip(svc: &Service) -> &str {
    svc.spec.cluster_ip.as_deref().unwrap_or("")
}

fn cluster_ips(svc: &Service) -> &[String] {
    svc.spec.cluster_ips.as_deref().unwrap_or(&[])
}

fn is_type(svc: &Service, t: ServiceType) -> bool {
    svc.spec.service_type.as_ref() == Some(&t)
}

/// `needsClusterIP` (storage.go:668-673).
fn needs_cluster_ip(svc: &Service) -> bool {
    !is_type(svc, ServiceType::ExternalName)
}

/// `needsNodePort` (storage.go:675-685).
fn needs_node_port(svc: &Service) -> bool {
    is_type(svc, ServiceType::NodePort)
        || (is_type(svc, ServiceType::LoadBalancer)
            && svc.spec.allocate_load_balancer_node_ports.unwrap_or(true))
}

/// `needsHCNodePort` (storage.go:687-695).
fn needs_hc_node_port(svc: &Service) -> bool {
    is_type(svc, ServiceType::LoadBalancer)
        && matches!(
            svc.spec.external_traffic_policy,
            Some(ServiceExternalTrafficPolicy::Local)
        )
}

/// `normalizeClusterIPs` (storage.go:521-587): keep `clusterIP` and
/// `clusterIPs` in step. `old` is `None` on create.
pub fn normalize_cluster_ips(new: &mut Service, old: Option<&Service>) {
    let Some(old) = old else {
        // An old client set only the singular field.
        if !cluster_ip(new).is_empty() && cluster_ips(new).is_empty() {
            new.spec.cluster_ips = Some(vec![cluster_ip(new).to_string()]);
        }
        return;
    };

    // An old client patching another field dropped clusterIPs.
    if !cluster_ips(old).is_empty()
        && cluster_ips(new).is_empty()
        && cluster_ip(old) == cluster_ip(new)
    {
        new.spec.cluster_ips = old.spec.cluster_ips.clone();
    }

    if cluster_ip(old) != cluster_ip(new) {
        let same = alloc::same_cluster_ips(old, new);
        if !cluster_ip(old).is_empty() && cluster_ip(new).is_empty() {
            // A client clearing it: clear the plural on their behalf.
            if same {
                new.spec.cluster_ips = None;
            }
        } else if same {
            new.spec.cluster_ips = Some(vec![cluster_ip(new).to_string()]);
        }
    }
}

/// `patchAllocatedValues` (storage.go:589-666): values allocated on the
/// client's behalf survive a resubmission that omits them.
pub fn patch_allocated_values(new: &mut Service, old: &Service) {
    if needs_cluster_ip(old) && needs_cluster_ip(new) {
        if cluster_ip(new).is_empty() {
            new.spec.cluster_ip = old.spec.cluster_ip.clone();
        }
        if cluster_ips(new).is_empty() && !cluster_ips(old).is_empty() {
            new.spec.cluster_ips = old.spec.cluster_ips.clone();
        }
    }

    if needs_node_port(old) && needs_node_port(new) {
        let used_by = |svc: &Service| -> HashSet<u16> {
            svc.spec
                .ports
                .iter()
                .filter_map(|p| p.node_port.filter(|n| *n != 0))
                .collect()
        };
        // The ports in old that are also in new cannot be patched in.
        let used: HashSet<u16> = used_by(old).intersection(&used_by(new)).copied().collect();
        // Node ports by port name.
        let np: HashMap<&str, u16> = old
            .spec
            .ports
            .iter()
            .map(|p| (p.name.as_deref().unwrap_or(""), p.node_port.unwrap_or(0)))
            .collect();
        for p in &mut new.spec.ports {
            if p.node_port.unwrap_or(0) == 0 {
                let old_val = np
                    .get(p.name.as_deref().unwrap_or(""))
                    .copied()
                    .unwrap_or(0);
                if !used.contains(&old_val) {
                    p.node_port = (old_val != 0).then_some(old_val);
                }
            }
        }
    }

    if needs_hc_node_port(old)
        && needs_hc_node_port(new)
        && new.spec.health_check_node_port.unwrap_or(0) == 0
    {
        new.spec.health_check_node_port = old.spec.health_check_node_port;
    }
}

fn other_family(fam: &IPFamily) -> IPFamily {
    match fam {
        IPFamily::IPv4 => IPFamily::IPv6,
        IPFamily::IPv6 => IPFamily::IPv4,
    }
}

/// `defaultOnReadIPFamilies` (storage.go:281-330).
fn default_on_read_ip_families(svc: &mut Service) {
    if !needs_cluster_ip(svc) {
        return;
    }
    if svc.spec.ip_families.as_ref().is_some_and(|f| !f.is_empty()) {
        return;
    }
    let primary = DEFAULT_SERVICE_IP_FAMILY;
    if cluster_ip(svc) == "None" {
        if svc.spec.selector.as_ref().is_none_or(|s| s.is_empty()) {
            // Headless + selectorless.
            svc.spec.ip_family_policy = Some(IPFamilyPolicy::RequireDualStack);
            let other = other_family(&primary);
            svc.spec.ip_families = Some(vec![primary, other]);
        } else {
            svc.spec.ip_family_policy = Some(IPFamilyPolicy::SingleStack);
            svc.spec.ip_families = Some(vec![primary]);
        }
    } else {
        // Headful: families from clusterIPs.
        let fams: Vec<IPFamily> = cluster_ips(svc)
            .iter()
            .map(|ip| {
                // `netutil.IsIPv6String`.
                if parse_ip_sloppy(ip).is_some_and(|a| a.is_ipv6()) {
                    IPFamily::IPv6
                } else {
                    IPFamily::IPv4
                }
            })
            .collect();
        match fams.len() {
            1 => svc.spec.ip_family_policy = Some(IPFamilyPolicy::SingleStack),
            2 => svc.spec.ip_family_policy = Some(IPFamilyPolicy::RequireDualStack),
            _ => {}
        }
        svc.spec.ip_families = Some(fams);
    }
}

/// `defaultOnReadService` (storage.go:255-274): the `Decorator`.
pub fn default_on_read(svc: &mut Service) {
    // Services written before ClusterIP became plural.
    normalize_cluster_ips(svc, None);
    default_on_read_ip_families(svc);
    // `defaultOnReadInternalTrafficPolicy`.
    if is_type(svc, ServiceType::ExternalName) {
        svc.spec.internal_traffic_policy = None;
    }
}

/// `metaTransaction` over a ClusterIP and a node-port transaction
/// (alloc.go:65-100): commit on success, revert otherwise.
struct AllocFinish {
    ips: ClusterIpTxn<StorageBackend>,
    ports: PortAllocationOperation,
}

#[async_trait]
impl Finish for AllocFinish {
    async fn finish(self: Box<Self>, success: bool) {
        let outcome: Result<()> = if success {
            Ok(())
        } else {
            Err(Error::Internal(String::new()))
        };
        alloc::settle_all(self.ips, self.ports, &outcome).await;
    }
}

/// The `REST` of storage.go:60-68: the allocators and the Endpoints storage
/// the hooks use.
pub struct ServiceRest {
    ips: Arc<ClusterIpAllocators<StorageBackend>>,
    ports: Arc<PortAllocator>,
    endpoints: Store<Endpoints, StorageBackend>,
}

impl ServiceRest {
    /// `allocateCreate` (alloc.go:65-100).
    async fn allocate_create(&self, svc: &mut Service, dry_run: bool) -> Result<AllocFinish> {
        alloc::init_ip_family_fields(svc, None, &self.ips.families())?;
        let ips = alloc::txn_alloc_cluster_ips(&self.ips, svc, dry_run).await?;
        let ports = match alloc::txn_alloc_node_ports(&self.ports, svc, dry_run).await {
            Ok(op) => op,
            Err(e) => {
                ips.revert().await;
                return Err(e);
            }
        };
        Ok(AllocFinish { ips, ports })
    }

    /// `allocateUpdate` (alloc.go:590-626).
    async fn allocate_update(
        &self,
        svc: &mut Service,
        old: &Service,
        dry_run: bool,
    ) -> Result<AllocFinish> {
        alloc::init_ip_family_fields(svc, Some(old), &self.ips.families())?;
        let ips = alloc::txn_update_cluster_ips(&self.ips, svc, old, dry_run).await?;
        let ports = match alloc::txn_update_node_ports(&self.ports, svc, old, dry_run).await {
            Ok(op) => op,
            Err(e) => {
                ips.revert().await;
                return Err(e);
            }
        };
        Ok(AllocFinish { ips, ports })
    }
}

#[async_trait]
impl BeginCreate<Service> for ServiceRest {
    /// `beginCreate` (storage.go:360-388).
    async fn begin_create(
        &self,
        _ctx: &RequestContext,
        svc: &mut Service,
        options: &CreateOptions,
    ) -> Result<Box<dyn Finish>> {
        normalize_cluster_ips(svc, None);
        Ok(Box::new(self.allocate_create(svc, options.dry_run).await?))
    }
}

#[async_trait]
impl BeginUpdate<Service> for ServiceRest {
    /// `beginUpdate` (storage.go:390-422).
    async fn begin_update(
        &self,
        _ctx: &RequestContext,
        svc: &mut Service,
        old: &mut Service,
        options: &UpdateOptions,
    ) -> Result<Box<dyn Finish>> {
        // The Decorator is not called on the stored object in the update
        // path.
        default_on_read(old);
        patch_allocated_values(svc, old);
        normalize_cluster_ips(svc, Some(old));
        Ok(Box::new(
            self.allocate_update(svc, old, options.dry_run).await?,
        ))
    }
}

#[async_trait]
impl AfterDelete<Service> for ServiceRest {
    /// `afterDelete` (storage.go:332-358): drop the Endpoints of the same
    /// name, then release what the Service held.
    async fn after_delete(&self, obj: &Service, options: &DeleteOptions) {
        let mut svc = obj.clone();
        default_on_read(&mut svc);
        if options.dry_run.as_ref().is_some_and(|d| !d.is_empty()) {
            return;
        }
        let ctx = RequestContext::new(svc.metadata.namespace.as_deref());
        match self
            .endpoints
            .delete(&ctx, &svc.metadata.name, None, DeleteOptions::default())
            .await
        {
            Ok(_) | Err(Error::NotFound(_)) => {}
            Err(e) => tracing::error!(
                "delete service endpoints {}/{} failed: {}",
                svc.metadata.name,
                svc.metadata.namespace.as_deref().unwrap_or(""),
                e
            ),
        }
        // `releaseAllocatedResources` (alloc.go:886-908).
        alloc::release_cluster_ips(&self.ips, &svc).await;
        alloc::release_node_ports(&self.ports, &svc).await;
    }
}

/// `NewREST` (storage.go:79-133): the Service store and its `/status`
/// store. Both run `afterDelete`, since a status patch can remove the last
/// finalizer.
pub fn new_stores(
    storage: Arc<StorageBackend>,
    ips: Arc<ClusterIpAllocators<StorageBackend>>,
    ports: Arc<PortAllocator>,
) -> (
    Store<Service, StorageBackend>,
    Store<Service, StorageBackend>,
) {
    let rest = Arc::new(ServiceRest {
        ips,
        ports,
        endpoints: crate::registry::core::endpoint::new_store(storage.clone()),
    });
    let mut store = Store::new(
        storage,
        GroupResource::new("", "services"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    store.return_deleted_object = true;

    let mut status = store.with_update_strategy(Arc::new(StatusStrategy));
    status.after_delete = Some(rest.clone());

    store.decorator = Some(Arc::new(default_on_read));
    store.after_delete = Some(rest.clone());
    store.begin_create = Some(rest.clone());
    store.begin_update = Some(rest);
    (store, status)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn svc(cluster_ip: &str, cluster_ips: Option<&[&str]>) -> Service {
        let mut s: Service = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "s"}, "spec": {}
        }))
        .unwrap();
        s.spec.cluster_ip = Some(cluster_ip.to_string());
        s.spec.cluster_ips = cluster_ips.map(|ips| ips.iter().map(|ip| ip.to_string()).collect());
        s
    }

    /// `TestNormalizeClusterIPs` (storage/storage_test.go:287-425).
    #[test]
    fn normalize_cluster_ips_matches_upstream() {
        type Case<'a> = (&'a str, Option<Service>, Service, &'a str, &'a [&'a str]);
        let v4 = &["10.0.0.10"][..];
        let cases: Vec<Case> = vec![
            ("new - only clusterip used", None, svc("10.0.0.10", None), "10.0.0.10", v4),
            ("new - only clusterips used", None, svc("", Some(v4)), "", v4),
            ("new - both used", None, svc("10.0.0.10", Some(v4)), "10.0.0.10", v4),
            (
                "update - no change",
                Some(svc("10.0.0.10", Some(v4))),
                svc("10.0.0.10", Some(v4)),
                "10.0.0.10",
                v4,
            ),
            (
                "update - malformed change",
                Some(svc("10.0.0.10", Some(v4))),
                svc("10.0.0.11", Some(&["10.0.0.11"])),
                "10.0.0.11",
                &["10.0.0.11"],
            ),
            (
                "update - malformed change on secondary ip",
                Some(svc("10.0.0.10", Some(&["10.0.0.10", "2000::1"]))),
                svc("10.0.0.11", Some(&["10.0.0.11", "3000::1"])),
                "10.0.0.11",
                &["10.0.0.11", "3000::1"],
            ),
            (
                "update - upgrade",
                Some(svc("10.0.0.10", Some(v4))),
                svc("10.0.0.10", Some(&["10.0.0.10", "2000::1"])),
                "10.0.0.10",
                &["10.0.0.10", "2000::1"],
            ),
            (
                "update - downgrade",
                Some(svc("10.0.0.10", Some(&["10.0.0.10", "2000::1"]))),
                svc("10.0.0.10", Some(v4)),
                "10.0.0.10",
                v4,
            ),
            (
                "update - user cleared cluster IP",
                Some(svc("10.0.0.10", Some(v4))),
                svc("", Some(v4)),
                "",
                &[],
            ),
            (
                "update - user cleared clusterIPs",
                Some(svc("10.0.0.10", Some(v4))),
                svc("10.0.0.10", None),
                "10.0.0.10",
                v4,
            ),
            (
                "update - user cleared both",
                Some(svc("10.0.0.10", Some(v4))),
                svc("", None),
                "",
                &[],
            ),
            (
                "update - user cleared ClusterIP but changed clusterIPs",
                Some(svc("10.0.0.10", Some(v4))),
                svc("", Some(&["10.0.0.11"])),
                "",
                &["10.0.0.11"],
            ),
            (
                "update - user cleared ClusterIPs but changed ClusterIP",
                Some(svc("10.0.0.10", Some(&["10.0.0.10", "2000::1"]))),
                svc("10.0.0.11", None),
                "10.0.0.11",
                &[],
            ),
            (
                "update - user changed from None to ClusterIP",
                Some(svc("None", Some(&["None"]))),
                svc("10.0.0.10", Some(&["None"])),
                "10.0.0.10",
                v4,
            ),
            (
                "update - user changed from ClusterIP to None",
                Some(svc("10.0.0.10", Some(v4))),
                svc("None", Some(v4)),
                "None",
                &["None"],
            ),
            (
                "update - user changed from ClusterIP to None and changed ClusterIPs in a dual stack",
                Some(svc("10.0.0.10", Some(&["10.0.0.10", "2000::1"]))),
                svc("None", Some(&["10.0.0.11", "2000::1"])),
                "None",
                &["10.0.0.11", "2000::1"],
            ),
        ];
        for (name, old, mut new, want_ip, want_ips) in cases {
            normalize_cluster_ips(&mut new, old.as_ref());
            assert_eq!(cluster_ip(&new), want_ip, "{name}");
            assert_eq!(cluster_ips(&new), want_ips, "{name}");
        }
    }

    fn lb_local(ips: &[&str], node_port: Option<u16>, hcnp: Option<i32>) -> Service {
        let mut s: Service = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "foo"},
            "spec": {"type": "LoadBalancer", "externalTrafficPolicy": "Local",
                     "ports": [{"name": "p", "port": 80, "protocol": "TCP"}]}
        }))
        .unwrap();
        if let Some(first) = ips.first() {
            s.spec.cluster_ip = Some(first.to_string());
            s.spec.cluster_ips = Some(ips.iter().map(|ip| ip.to_string()).collect());
        }
        s.spec.ports[0].node_port = node_port;
        s.spec.health_check_node_port = hcnp;
        s
    }

    /// `TestPatchAllocatedValues` "all_patched" (storage/storage_test.go:436-446).
    #[test]
    fn patch_allocated_values_fills_what_the_client_omitted() {
        let before = lb_local(&["10.0.0.93", "2000::76"], Some(30093), Some(31234));
        let mut update = lb_local(&[], None, None);
        patch_allocated_values(&mut update, &before);
        assert_eq!(update.spec.cluster_ip, before.spec.cluster_ip);
        assert_eq!(update.spec.cluster_ips, before.spec.cluster_ips);
        assert_eq!(update.spec.ports[0].node_port, Some(30093));
        assert_eq!(update.spec.health_check_node_port, Some(31234));
    }

    /// `defaultOnReadIPFamilies`: a headful Service stored without families
    /// reads with them inferred from its ClusterIPs.
    #[test]
    fn default_on_read_infers_families() {
        let mut s = svc("10.0.0.10", None);
        default_on_read(&mut s);
        assert_eq!(cluster_ips(&s), &["10.0.0.10"]);
        assert_eq!(s.spec.ip_families, Some(vec![IPFamily::IPv4]));
        assert_eq!(s.spec.ip_family_policy, Some(IPFamilyPolicy::SingleStack));
    }
}
