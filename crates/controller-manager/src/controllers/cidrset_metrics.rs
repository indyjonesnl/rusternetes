//! Prometheus series for the node-IPAM `CidrSet`.
//!
//! Port of `pkg/controller/nodeipam/ipam/cidrset/metrics.go` (release-1.35):
//! subsystem `node_ipam_controller`, five series keyed by the `clusterCIDR`
//! label. Upstream registers them in `legacyregistry` once (`registerMetrics
//! sync.Once`); here a process-global [`Registry`] plays that role and is
//! exposed on `/metrics` by the controller-manager binary.
//!
//! Histogram buckets are `metrics.ExponentialBuckets(1, 5, 5)` (metrics.go).
//! Upstream's quirks are kept verbatim: the gauge is named `cirdset_max_cidrs`
//! (sic) and the histogram's help text is the copy-pasted "Number of endpoints
//! added on each Service sync".

use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
};
use std::sync::LazyLock;

const SUBSYSTEM: &str = "node_ipam_controller";
const LABEL: &[&str] = &["clusterCIDR"];

fn opts(name: &str, help: &str) -> Opts {
    Opts::new(name, help).subsystem(SUBSYSTEM)
}

struct Series {
    registry: Registry,
    allocations: IntCounterVec,
    releases: IntCounterVec,
    max_cidrs: GaugeVec,
    usage: GaugeVec,
    tries: HistogramVec,
}

static SERIES: LazyLock<Series> = LazyLock::new(|| {
    let registry = Registry::new();
    let allocations = IntCounterVec::new(
        opts(
            "cidrset_cidrs_allocations_total",
            "Counter measuring total number of CIDR allocations.",
        ),
        LABEL,
    )
    .expect("valid opts");
    let releases = IntCounterVec::new(
        opts(
            "cidrset_cidrs_releases_total",
            "Counter measuring total number of CIDR releases.",
        ),
        LABEL,
    )
    .expect("valid opts");
    // A gauge, "as in theory, a limit can increase or decrease" (metrics.go).
    let max_cidrs = GaugeVec::new(
        opts(
            "cirdset_max_cidrs",
            "Maximum number of CIDRs that can be allocated.",
        ),
        LABEL,
    )
    .expect("valid opts");
    let usage = GaugeVec::new(
        opts(
            "cidrset_usage_cidrs",
            "Gauge measuring percentage of allocated CIDRs.",
        ),
        LABEL,
    )
    .expect("valid opts");
    let tries = HistogramVec::new(
        HistogramOpts::new(
            "cidrset_allocation_tries_per_request",
            "Number of endpoints added on each Service sync",
        )
        .subsystem(SUBSYSTEM)
        // metrics.ExponentialBuckets(1, 5, 5)
        .buckets(prometheus::exponential_buckets(1.0, 5.0, 5).expect("valid buckets")),
        LABEL,
    )
    .expect("valid opts");
    registry
        .register(Box::new(allocations.clone()))
        .expect("register once");
    registry
        .register(Box::new(releases.clone()))
        .expect("register once");
    registry
        .register(Box::new(max_cidrs.clone()))
        .expect("register once");
    registry
        .register(Box::new(usage.clone()))
        .expect("register once");
    registry
        .register(Box::new(tries.clone()))
        .expect("register once");
    Series {
        registry,
        allocations,
        releases,
        max_cidrs,
        usage,
        tries,
    }
});

/// `cidrSetMaxCidrs.WithLabelValues(label).Set(max)` (`cidr_set.go:98`).
pub fn set_max_cidrs(label: &str, max: u64) {
    SERIES.max_cidrs.with_label_values(&[label]).set(max as f64);
}

/// `cidrSetAllocations.WithLabelValues(label).Inc()` (`cidr_set.go:172,259`).
pub fn inc_allocations(label: &str) {
    SERIES.allocations.with_label_values(&[label]).inc();
}

/// `cidrSetReleases.WithLabelValues(label).Inc()` (`cidr_set.go:236`).
pub fn inc_releases(label: &str) {
    SERIES.releases.with_label_values(&[label]).inc();
}

/// `cidrSetAllocationTriesPerRequest.WithLabelValues(label).Observe(tries)`
/// (`cidr_set.go:173`).
pub fn observe_tries(label: &str, tries: u64) {
    SERIES
        .tries
        .with_label_values(&[label])
        .observe(tries as f64);
}

/// `cidrSetUsage.WithLabelValues(label).Set(allocated / max)`
/// (`cidr_set.go:174,240,263`).
pub fn set_usage(label: &str, allocated: u64, max: u64) {
    SERIES
        .usage
        .with_label_values(&[label])
        .set(allocated as f64 / max as f64);
}

/// The registry in Prometheus text exposition format (`/metrics` body).
pub fn gather() -> String {
    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&SERIES.registry.gather(), &mut buf)
        .expect("text encode");
    String::from_utf8(buf).expect("utf8")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::node_ipam::CidrSet;
    use ipnet::IpNet;

    fn line(label: &str, name: &str) -> Option<f64> {
        let want = format!("node_ipam_controller_{name}{{clusterCIDR=\"{label}\"");
        gather().lines().find_map(|l| {
            l.starts_with(&want)
                .then(|| l.rsplit(' ').next().unwrap().parse().unwrap())
        })
    }

    // TestCidrSetMetrics: the same walk as the in-struct accessor test, read
    // back through the exposition (testutil.Get*MetricValue upstream).
    #[test]
    fn exposition_tracks_cidrset_metrics() {
        let label = "10.20.0.0/16";
        let a = CidrSet::new(label.parse::<IpNet>().unwrap(), 24).unwrap();
        assert_eq!(line(label, "cirdset_max_cidrs"), Some(256.0));
        for _ in 0..256 {
            a.allocate_next().unwrap();
        }
        assert_eq!(line(label, "cidrset_cidrs_allocations_total"), Some(256.0));
        assert_eq!(line(label, "cidrset_usage_cidrs"), Some(1.0));
        a.release(label.parse().unwrap()).unwrap();
        assert_eq!(line(label, "cidrset_cidrs_releases_total"), Some(256.0));
        assert_eq!(line(label, "cidrset_usage_cidrs"), Some(0.0));
        a.occupy(label.parse().unwrap()).unwrap();
        assert_eq!(line(label, "cidrset_cidrs_allocations_total"), Some(512.0));
        assert_eq!(line(label, "cidrset_usage_cidrs"), Some(1.0));
    }

    // TestCidrSetMetricsHistogram: occupy half, then AllocateNext walks 128.
    #[test]
    fn exposition_histogram_sum_and_buckets() {
        let label = "10.21.0.0/16";
        let a = CidrSet::new(label.parse::<IpNet>().unwrap(), 24).unwrap();
        a.occupy("10.21.0.0/17".parse().unwrap()).unwrap();
        a.allocate_next().unwrap();
        assert_eq!(
            line(label, "cidrset_allocation_tries_per_request_sum"),
            Some(128.0)
        );
        assert_eq!(
            line(label, "cidrset_allocation_tries_per_request_count"),
            Some(1.0)
        );
        // ExponentialBuckets(1, 5, 5) = 1, 5, 25, 125, 625
        let out = gather();
        assert!(out.contains(&format!(
            "cidrset_allocation_tries_per_request_bucket{{clusterCIDR=\"{label}\",le=\"125\"}} 0"
        )));
        assert!(out.contains(&format!(
            "cidrset_allocation_tries_per_request_bucket{{clusterCIDR=\"{label}\",le=\"625\"}} 1"
        )));
    }

    // TestCidrSetMetricsDual: IPv4 and IPv6 sets keep separate label series.
    #[test]
    fn exposition_labels_are_per_cluster_cidr() {
        let v4 = "10.22.0.0/16";
        let v6 = "beef:2222::/32";
        let a4 = CidrSet::new(v4.parse::<IpNet>().unwrap(), 24).unwrap();
        let a6 = CidrSet::new(v6.parse::<IpNet>().unwrap(), 48).unwrap();
        a4.allocate_next().unwrap();
        assert_eq!(line(v4, "cidrset_cidrs_allocations_total"), Some(1.0));
        assert_eq!(line(v6, "cidrset_cidrs_allocations_total"), None);
        assert_eq!(line(v6, "cirdset_max_cidrs"), Some(65536.0));
        a6.allocate_next().unwrap();
        assert_eq!(line(v6, "cidrset_cidrs_allocations_total"), Some(1.0));
    }
}
