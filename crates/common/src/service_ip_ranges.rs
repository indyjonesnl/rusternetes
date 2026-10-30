//! `--service-cluster-ip-range`: the comma-separated list of one or two
//! ranges, one per IP family, ClusterIPs are allocated from.
//!
//! Ports `getServiceIPAndRanges` (cmd/kube-apiserver/app/options/
//! completion.go:94-134) and `validateClusterIPFlags` (cmd/kube-apiserver/
//! app/options/validation.go:36-84). The `maxCIDRBits` size cap upstream
//! applies only while `MultiCIDRServiceAllocator` or
//! `DisableAllocatorDualWrite` is off; both are GA and locked on in 1.35, so
//! it does not apply.

use crate::resources::IPFamily;
use ipnet::IpNet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// This project's default. Upstream's `kubeoptions.DefaultServiceIPCIDR`
/// (pkg/kubeapiserver/options/options.go:30) is 10.0.0.0/24; the
/// Kubernetes-distribution convention 10.96.0.0/12 is kept deliberately.
pub const DEFAULT_SERVICE_CLUSTER_IP_RANGE: &str = "10.96.0.0/12";

/// The validated ranges, primary first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceIpRanges {
    cidrs: Vec<IpNet>,
}

impl Default for ServiceIpRanges {
    fn default() -> Self {
        Self::parse(DEFAULT_SERVICE_CLUSTER_IP_RANGE).expect("the default range parses")
    }
}

fn family(net: &IpNet) -> IPFamily {
    match net {
        IpNet::V4(_) => IPFamily::IPv4,
        IpNet::V6(_) => IPFamily::IPv6,
    }
}

impl ServiceIpRanges {
    /// Parse and validate the flag value. An empty value is the default
    /// range (`getServiceIPAndRanges`, completion.go:105-112).
    pub fn parse(flag: &str) -> Result<Self, String> {
        let entries: Vec<&str> = if flag.is_empty() {
            vec![DEFAULT_SERVICE_CLUSTER_IP_RANGE]
        } else {
            flag.split(',').collect()
        };
        // validation.go:46-49.
        if entries.len() > 2 {
            return Err("--service-cluster-ip-range must not contain more than two entries".into());
        }
        let mut cidrs = Vec::new();
        for (i, entry) in entries.iter().enumerate() {
            // `ParseCIDRSloppy` keeps the network address (`IPNet`).
            let net: IpNet = entry.trim().parse().map_err(|_| {
                if i == 0 {
                    "service-cluster-ip-range[0] is not a valid cidr".to_string()
                } else {
                    "service-cluster-ip-range[1] is not an ip net".to_string()
                }
            })?;
            cidrs.push(net.trunc());
        }
        // validation.go:67-76.
        if cidrs.len() == 2 && family(&cidrs[0]) == family(&cidrs[1]) {
            return Err(
                "--service-cluster-ip-range[0] and --service-cluster-ip-range[1] must be of different IP family"
                    .into(),
            );
        }
        // `cp.ServiceIPRange` (pkg/controlplane/apiserver/options/
        // options.go:373-376) runs on the primary range only.
        let host_bits = match cidrs[0] {
            IpNet::V4(n) => 32 - n.prefix_len(),
            IpNet::V6(n) => 128 - n.prefix_len(),
        };
        if host_bits < 3 {
            return Err("the service cluster IP range must be at least 8 IP addresses".into());
        }
        Ok(Self { cidrs })
    }

    /// The `kubernetes` Service address: the first usable address of the
    /// primary range (`cp.ServiceIPRange`, options.go:378-382,
    /// `GetIndexedIP(&serviceClusterIPRange, 1)`).
    pub fn api_server_service_ip(&self) -> IpAddr {
        match self.cidrs[0] {
            IpNet::V4(n) => IpAddr::V4(Ipv4Addr::from(u32::from(n.network()).wrapping_add(1))),
            IpNet::V6(n) => IpAddr::V6(Ipv6Addr::from(u128::from(n.network()).wrapping_add(1))),
        }
    }

    /// The ranges as ServiceCIDR `spec.cidrs`, primary first.
    pub fn cidrs(&self) -> Vec<String> {
        self.cidrs.iter().map(|c| c.to_string()).collect()
    }

    /// The configured families, primary first
    /// (`serviceIPAllocatorsByFamily`'s keys).
    pub fn families(&self) -> Vec<IPFamily> {
        self.cidrs.iter().map(family).collect()
    }
}
