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

/// Upstream's default (`cp.DefaultServiceIPCIDR`, pkg/controlplane/
/// instance.go), and the range the `kubernetes` Service address
/// (`10.96.0.1`) belongs to.
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
        // Deviation (tracked in the follow-up issue): the `kubernetes`
        // Service address is still the fixed first address of the default
        // range, so the primary range cannot differ from it yet.
        let default: IpNet = DEFAULT_SERVICE_CLUSTER_IP_RANGE.parse().expect("constant");
        if cidrs[0] != default {
            return Err(format!(
                "--service-cluster-ip-range[0] must be {DEFAULT_SERVICE_CLUSTER_IP_RANGE}: \
                 the kubernetes Service address is not yet derived from the range"
            ));
        }
        Ok(Self { cidrs })
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
        assert!(err("fd00::/112").contains("must be 10.96.0.0/12"));
    }
}
