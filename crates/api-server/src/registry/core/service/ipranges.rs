//! `--service-cluster-ip-range`: the comma-separated list of one or two
//! ranges, one per IP family, ClusterIPs are allocated from.
//!
//! Ports `getServiceIPAndRanges` (cmd/kube-apiserver/app/options/
//! completion.go:94-134) and `validateClusterIPFlags` (cmd/kube-apiserver/
//! app/options/validation.go:36-84). The `maxCIDRBits` size cap upstream
//! applies only while `MultiCIDRServiceAllocator` or
//! `DisableAllocatorDualWrite` is off; both are GA and locked on in 1.35, so
//! it does not apply.

use ipnet::IpNet;
use rusternetes_common::resources::IPFamily;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_the_default_single_stack_range() {
        let r = ServiceIpRanges::parse("").unwrap();
        assert_eq!(r.cidrs(), vec!["10.96.0.0/12"]);
        assert_eq!(r.families(), vec![IPFamily::IPv4]);
        assert_eq!(r, ServiceIpRanges::default());
    }

    #[test]
    fn a_dual_stack_pair_keeps_its_order() {
        let r = ServiceIpRanges::parse("10.96.0.0/12,2001:db8:1::/112").unwrap();
        assert_eq!(r.cidrs(), vec!["10.96.0.0/12", "2001:db8:1::/112"]);
        assert_eq!(r.families(), vec![IPFamily::IPv4, IPFamily::IPv6]);
    }

    #[test]
    fn rejects_what_upstream_rejects() {
        let err = |s: &str| ServiceIpRanges::parse(s).unwrap_err();
        assert!(err("10.96.0.0/12,fd00::/112,10.0.0.0/16").contains("more than two entries"));
        assert!(err("nope").contains("[0] is not a valid cidr"));
        assert!(err("10.96.0.0/12,nope").contains("[1] is not an ip net"));
        assert!(err("10.96.0.0/12,10.0.0.0/16").contains("must be of different IP family"));
        assert!(err("10.96.0.0/30").contains("at least 8 IP addresses"));
    }

    /// `TestGetServiceIPAndRanges` (cmd/kube-apiserver/app/options/
    /// completion_test.go:23-67): (flag, apiServerServiceIP, primary, secondary).
    #[test]
    fn upstream_get_service_ip_and_ranges_table() {
        let r = ServiceIpRanges::parse("192.0.2.1/24").unwrap();
        assert_eq!(r.api_server_service_ip().to_string(), "192.0.2.1");
        assert_eq!(r.cidrs(), vec!["192.0.2.0/24"]);
        // (Upstream's IPv4+IPv4 row is omitted: validation.go:67-76 rejects it
        // and the table does not exercise validation.)
        let r = ServiceIpRanges::parse("192.0.2.1/24,2001:db2:1:3:4::1/112").unwrap();
        assert_eq!(r.api_server_service_ip().to_string(), "192.0.2.1");
        assert_eq!(r.cidrs(), vec!["192.0.2.0/24", "2001:db2:1:3:4::/112"]);
        // IPv6-primary dual-stack.
        let r = ServiceIpRanges::parse("2001:db2:1:3:4::1/112,192.0.2.1/24").unwrap();
        assert_eq!(r.api_server_service_ip().to_string(), "2001:db2:1:3:4::1");
        assert_eq!(r.cidrs(), vec!["2001:db2:1:3:4::/112", "192.0.2.0/24"]);
        assert_eq!(r.families(), vec![IPFamily::IPv6, IPFamily::IPv4]);
        assert_eq!(
            ServiceIpRanges::parse("fd00::/112").unwrap().families()[0],
            IPFamily::IPv6
        );
        for bad in [
            "192.0.2.1/30,192.168.128.0/17",
            "192.0.2.1/33,192.168.128.0/17",
            "192.0.2.1/24,192.168.128.0/33",
            "2001:db2:1:3:4::1/129,192.0.2.1/24",
            "192.0.2.1/24,2001:db2:1:3:4::1/129",
            "192.0.2.1,192.168.128.0/17",
            "192.0.2.1/24,192.168.128.1",
            "2001:db2:1:3:4::1,192.0.2.1/24",
            "192.0.2.1/24,2001:db2:1:3:4::1",
            "bad.ip.range,192.168.0.2/24",
            "192.168.0.2/24,bad.ip.range",
        ] {
            assert!(ServiceIpRanges::parse(bad).is_err(), "{bad}");
        }
    }
}
