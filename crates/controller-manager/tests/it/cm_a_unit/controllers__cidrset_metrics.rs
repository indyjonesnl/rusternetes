//! Unit tests moved from `src/controllers/cidrset_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prometheus::{
        Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
    };
    pub use std::sync::LazyLock;
}

mod tests {
    use super::__parent::*;
    use ipnet::IpNet;
    use rusternetes_controller_manager::controllers::cidrset_metrics::*;
    use rusternetes_controller_manager::controllers::node_ipam::CidrSet;

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
