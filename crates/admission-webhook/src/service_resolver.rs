//! The aggregator's `ServiceResolver`: turns an APIService's
//! `{namespace, name, port}` into the `host:port` the proxy dials.
//!
//! Ported from:
//! - `staging/src/k8s.io/kube-aggregator/pkg/apiserver/resolvers.go`
//!   (`NewEndpointServiceResolver`, `NewClusterIPServiceResolver`,
//!   `NewLoopbackServiceResolver`),
//! - `staging/src/k8s.io/apiserver/pkg/util/proxy/proxy.go`
//!   (`ResolveEndpoint`, `ResolveCluster`, `findServicePort`),
//! - `cmd/kube-apiserver/app/server.go` `buildServiceResolver`: the
//!   `--enable-aggregator-routing` flag picks the endpoint resolver, otherwise
//!   the ClusterIP resolver; either is wrapped in the loopback resolver.
//!
//! Go-runtime differences: upstream reads Services and EndpointSlices from
//! shared-informer listers; here they are read from storage on each call. The
//! returned `url.URL` is reduced to the `(host, port)` the proxy needs, since
//! the scheme is always `https`.

use rand::seq::SliceRandom;
use rand::Rng;
use rusternetes_common::resources::{EndpointSlice, IPFamily, Service, ServiceType};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::sync::OnceLock;

/// `label "kubernetes.io/service-name"` (`discoveryv1.LabelServiceName`).
const LABEL_SERVICE_NAME: &str = "kubernetes.io/service-name";

/// The two resolvers `buildServiceResolver` chooses between.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Routing {
    /// `NewClusterIPServiceResolver` (the default).
    #[default]
    ClusterIp,
    /// `NewEndpointServiceResolver` (`--enable-aggregator-routing`).
    Endpoint,
}

/// `ServiceResolver`: the chosen routing wrapped in the loopback resolver.
#[derive(Debug, Clone, Default)]
pub struct ServiceResolver {
    pub routing: Routing,
    /// `NewLoopbackServiceResolver(delegate, host)`: where
    /// `default/kubernetes:443` resolves to.
    pub loopback: Option<(String, u16)>,
}

impl ServiceResolver {
    /// `buildServiceResolver(enabledAggregatorRouting, hostname, ..)`.
    pub fn new(enable_aggregator_routing: bool, loopback: Option<(String, u16)>) -> Self {
        Self {
            routing: if enable_aggregator_routing {
                Routing::Endpoint
            } else {
                Routing::ClusterIp
            },
            loopback,
        }
    }

    /// `ResolveEndpoint(namespace, name, port)`.
    pub async fn resolve_endpoint<S: Storage + Send + Sync>(
        &self,
        storage: &S,
        namespace: &str,
        name: &str,
        port: i32,
    ) -> Result<(String, u16), String> {
        // loopbackResolver.ResolveEndpoint
        if namespace == "default" && name == "kubernetes" && port == 443 {
            if let Some(host) = &self.loopback {
                return Ok(host.clone());
            }
        }
        match self.routing {
            Routing::Endpoint => resolve_endpoint(storage, namespace, name, port).await,
            Routing::ClusterIp => resolve_cluster(storage, namespace, name, port).await,
        }
    }
}

static SERVICE_RESOLVER: OnceLock<ServiceResolver> = OnceLock::new();

/// Installs the process-wide resolver (`serviceResolver` handed to the
/// aggregator config). First call wins.
pub fn set_service_resolver(resolver: ServiceResolver) {
    let _ = SERVICE_RESOLVER.set(resolver);
}

/// The installed resolver; the ClusterIP resolver when none was installed
/// (`EnableAggregatorRouting` defaults to false).
pub fn service_resolver() -> ServiceResolver {
    SERVICE_RESOLVER.get().cloned().unwrap_or_default()
}

/// `findServicePort`: the Service port whose `port` equals `port`.
fn find_service_port(
    svc: &Service,
    port: i32,
) -> Result<&rusternetes_common::resources::ServicePort, String> {
    svc.spec
        .ports
        .iter()
        .find(|p| i32::from(p.port) == port)
        .ok_or_else(|| {
            format!(
                "no service port {port} found for service \"{}\"",
                svc.metadata.name
            )
        })
}

async fn get_service<S: Storage + Send + Sync>(
    storage: &S,
    namespace: &str,
    name: &str,
) -> Result<Service, String> {
    storage
        .get::<Service>(&build_key("services", Some(namespace), name))
        .await
        .map_err(|_| format!("service \"{name}\" not found"))
}

/// `ServiceType` with the API default (an unset type is `ClusterIP`).
fn service_type(svc: &Service) -> ServiceType {
    svc.spec
        .service_type
        .clone()
        .unwrap_or(ServiceType::ClusterIP)
}

fn type_name(t: &ServiceType) -> String {
    match t {
        ServiceType::ClusterIP => "ClusterIP".into(),
        ServiceType::NodePort => "NodePort".into(),
        ServiceType::LoadBalancer => "LoadBalancer".into(),
        ServiceType::ExternalName => "ExternalName".into(),
        ServiceType::Unknown(s) => s.clone(),
    }
}

/// `proxy.ResolveEndpoint`: picks a Ready endpoint of a slice that carries the
/// Service port (matched by name), preferring the primary IP family.
pub async fn resolve_endpoint<S: Storage + Send + Sync>(
    storage: &S,
    namespace: &str,
    id: &str,
    port: i32,
) -> Result<(String, u16), String> {
    let svc = get_service(storage, namespace, id).await?;
    let ty = service_type(&svc);
    if !matches!(
        ty,
        ServiceType::ClusterIP | ServiceType::LoadBalancer | ServiceType::NodePort
    ) {
        return Err(format!("unsupported service type \"{}\"", type_name(&ty)));
    }
    let svc_port = find_service_port(&svc, port)?;

    // `endpointSlices.GetEndpointSlices(namespace, svc.Name)`
    let all: Vec<EndpointSlice> = storage
        .list(&build_prefix("endpointslices", Some(namespace)))
        .await
        .unwrap_or_default();
    let mut slices: Vec<EndpointSlice> = all
        .into_iter()
        .filter(|s| {
            s.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(LABEL_SERVICE_NAME))
                .is_some_and(|v| *v == svc.metadata.name)
        })
        .collect();
    if slices.is_empty() {
        return Err(format!(
            "no endpoints available for service \"{}\"",
            svc.metadata.name
        ));
    }
    let families = svc.spec.ip_families.clone().unwrap_or_default();
    let mut rng = rand::rng();
    if slices.len() > 1 && !families.is_empty() {
        // Random order, but every slice of the primary IP family first.
        let preferred = match &families[0] {
            IPFamily::IPv4 => "IPv4".to_string(),
            IPFamily::IPv6 => "IPv6".to_string(),
            IPFamily::Unknown(s) => s.clone(),
        };
        let mut order: Vec<usize> = (0..slices.len()).collect();
        order.shuffle(&mut rng);
        let mut keyed: Vec<(usize, EndpointSlice)> = order.into_iter().zip(slices).collect();
        keyed.sort_by(|(ri, a), (rj, b)| {
            if a.address_type != b.address_type {
                return if a.address_type == preferred {
                    std::cmp::Ordering::Less
                } else if b.address_type == preferred {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                };
            }
            ri.cmp(rj)
        });
        slices = keyed.into_iter().map(|(_, s)| s).collect();
    }

    let want = svc_port.name.clone().unwrap_or_default();
    // Find a slice that has the port.
    for slice in &slices {
        for p in &slice.ports {
            if p.name.as_deref() != Some(want.as_str()) {
                continue;
            }
            let Some(slice_port) = p.port else { continue };
            if slice.endpoints.is_empty() {
                continue;
            }
            // Starting from a random index, find a Ready endpoint.
            let n = slice.endpoints.len();
            let offset = rng.random_range(0..n);
            for epi in 0..n {
                let ep = &slice.endpoints[(epi + offset) % n];
                if ep.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true) {
                    // Addresses is an array but only Addresses[0] is used.
                    let ip = ep
                        .addresses
                        .first()
                        .ok_or_else(|| format!("endpoint of service \"{id}\" has no addresses"))?;
                    return Ok((ip.clone(), slice_port as u16));
                }
            }
        }
    }
    Err(format!("no endpoints available for service \"{id}\""))
}

/// `proxy.ResolveCluster`: the Service's ClusterIP (or ExternalName).
pub async fn resolve_cluster<S: Storage + Send + Sync>(
    storage: &S,
    namespace: &str,
    id: &str,
    port: i32,
) -> Result<(String, u16), String> {
    let svc = get_service(storage, namespace, id).await?;
    let ty = service_type(&svc);
    match ty {
        ServiceType::ClusterIP if svc.spec.cluster_ip.as_deref() == Some("None") => {
            Err("cannot route to service with ClusterIP \"None\"".to_string())
        }
        // use IP from a clusterIP for these service types
        ServiceType::ClusterIP | ServiceType::LoadBalancer | ServiceType::NodePort => {
            let svc_port = find_service_port(&svc, port)?;
            Ok((
                svc.spec.cluster_ip.clone().unwrap_or_default(),
                svc_port.port,
            ))
        }
        ServiceType::ExternalName => Ok((
            svc.spec.external_name.clone().unwrap_or_default(),
            port as u16,
        )),
        ServiceType::Unknown(s) => Err(format!("unsupported service type \"{s}\"")),
    }
}
