//! Unit tests moved from `src/controllers/node_ipam.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use futures::StreamExt;
    pub use ipnet::IpNet;
    pub use rusternetes_common::resources::{EventSource, EventType, Node, ObjectReference};
    pub use rusternetes_common::Error;
    pub use rusternetes_controller_manager::controllers::cidrset_metrics;
    pub use rusternetes_storage::{
        build_key, build_prefix, extract_key, EventRecorder, Storage, WatchEvent, WorkQueue,
    };
    pub use std::collections::HashMap;
    pub use std::fmt;
    pub use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::Duration;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::Event;
    use rusternetes_controller_manager::controllers::node_ipam::*;
    use rusternetes_storage::memory::MemoryStorage;
    use rusternetes_storage::{build_key, build_prefix};
    use std::collections::HashSet;

    /// The patch body is upstream's `nodeForCIDRMergePatch`
    /// (`cidr.go:29-38`), and applying it leaves unrelated spec/metadata alone.
    #[tokio::test]
    async fn patch_node_cidrs_sends_only_the_cidr_delta() {
        assert_eq!(
            node_cidr_merge_patch(&["10.0.0.0/24".to_string()]),
            serde_json::json!({"spec": {"podCIDR": "10.0.0.0/24", "podCIDRs": ["10.0.0.0/24"]}})
        );
        let storage = Arc::new(MemoryStorage::new());
        let key = build_key("nodes", None, "n1");
        let _: serde_json::Value = storage
            .create(
                &key,
                &serde_json::json!({"apiVersion": "v1", "kind": "Node",
                    "metadata": {"name": "n1", "labels": {"x": "y"}},
                    "spec": {"unschedulable": true}}),
            )
            .await
            .unwrap();
        let patched: Node = storage
            .patch_strategic_merge(&key, &node_cidr_merge_patch(&["10.0.0.0/24".to_string()]))
            .await
            .unwrap();
        let spec = patched.spec.unwrap();
        assert_eq!(spec.pod_cidr.as_deref(), Some("10.0.0.0/24"));
        assert_eq!(spec.unschedulable, Some(true));
        assert_eq!(patched.metadata.labels.unwrap()["x"], "y");
    }

    fn ipn(s: &str) -> IpNet {
        s.parse::<IpNet>().unwrap().trunc()
    }

    fn set(cluster: &str, mask: u8) -> CidrSet {
        CidrSet::new(ipn(cluster), mask).unwrap()
    }

    // ---- CidrSet: ported from ipam/cidrset/cidr_set_test.go ----

    #[test]
    fn config_rejects_node_mask_shorter_than_cluster() {
        assert!(NodeIpamConfig::new("10.244.0.0/16", 8).is_err());
        assert!(NodeIpamConfig::new("10.244.0.0/16", 33).is_err());
        assert!(NodeIpamConfig::new("not-a-cidr", 24).is_err());
        assert!(NodeIpamConfig::new("10.244.0.0/16", 24).is_ok());
    }

    // TestCIDRSetFullyAllocated
    #[test]
    fn cidr_set_fully_allocated() {
        let a = set("127.123.234.0/30", 30);
        assert_eq!(a.allocate_next().unwrap(), ipn("127.123.234.0/30"));
        assert_eq!(a.allocate_next(), Err(CidrSetError::NoCidrsRemaining));
        a.release(ipn("127.123.234.0/30")).unwrap();
        assert_eq!(a.allocate_next().unwrap(), ipn("127.123.234.0/30"));
        assert!(a.allocate_next().is_err());
    }

    // TestIndexToCIDRBlock (IPv4 rows)
    #[test]
    fn cidr_set_index_to_cidr_block() {
        let a = set("127.123.3.0/16", 24);
        assert_eq!(a.index_to_cidr_block(0), ipn("127.123.0.0/24"));
        assert_eq!(a.index_to_cidr_block(15), ipn("127.123.15.0/24"));
        assert_eq!(a.index_to_cidr_block(255), ipn("127.123.255.0/24"));
        let b = set("10.0.0.0/8", 26);
        assert_eq!(b.index_to_cidr_block(1), ipn("10.0.0.64/26"));
    }

    // TestCIDRSet_RandomishAllocation
    #[test]
    fn cidr_set_randomish_allocation() {
        let a = set("127.123.234.0/16", 24);
        let first: Vec<_> = (0..256).map(|_| a.allocate_next().unwrap()).collect();
        assert!(a.allocate_next().is_err());
        for c in &first {
            a.release(*c).unwrap();
        }
        let again: Vec<_> = (0..256).map(|_| a.allocate_next().unwrap()).collect();
        assert!(a.allocate_next().is_err());
        assert_eq!(first, again);
    }

    // TestCIDRSet_AllocationOccupied
    #[test]
    fn cidr_set_allocation_occupied() {
        let a = set("127.123.234.0/16", 24);
        let cidrs: Vec<_> = (0..256).map(|_| a.allocate_next().unwrap()).collect();
        for c in &cidrs {
            a.release(*c).unwrap();
        }
        for c in &cidrs[128..] {
            a.occupy(*c).unwrap();
        }
        a.occupy(cidrs[128]).unwrap();
        let mut got: Vec<_> = (0..128).map(|_| a.allocate_next().unwrap()).collect();
        assert!(a.allocate_next().is_err());
        got.extend_from_slice(&cidrs[128..]);
        assert_eq!(got, cidrs);
    }

    // TestDoubleOccupyRelease
    #[test]
    fn cidr_set_double_occupy_release() {
        let a = set("10.42.0.0/16", 24);
        let ops: [(&str, bool, u64); 7] = [
            ("10.42.5.0/24", true, 1),
            ("10.42.9.0/24", true, 2),
            ("10.42.8.0/22", true, 5),
            ("10.42.9.0/24", true, 5),
            ("10.42.9.0/24", false, 4),
            ("10.42.9.0/24", false, 4),
            ("10.42.4.0/22", false, 3),
        ];
        for (cidr, occupy, want) in ops {
            if occupy {
                a.occupy(ipn(cidr)).unwrap();
            } else {
                a.release(ipn(cidr)).unwrap();
            }
            assert_eq!(a.allocated(), want, "after {cidr} occupy={occupy}");
        }
        for i in 0..(256 - 3) {
            a.allocate_next()
                .unwrap_or_else(|_| panic!("failed after {i}"));
        }
        assert!(a.allocate_next().is_err());
    }

    // TestGetBitforCIDR / TestOccupy (IPv4 rows)
    #[test]
    fn cidr_set_occupy_ranges_and_out_of_range() {
        let a = set("127.0.0.0/8", 16);
        // Whole cluster, and a shorter prefix that contains it, occupy every bit.
        a.occupy(ipn("127.0.0.0/8")).unwrap();
        assert_eq!(a.allocated(), 256);
        let b = set("127.0.0.0/8", 16);
        b.occupy(ipn("127.0.0.0/2")).unwrap();
        assert_eq!(b.allocated(), 256);
        // A /16 at index 123.
        let c = set("127.0.0.0/8", 16);
        c.occupy(ipn("127.123.0.0/16")).unwrap();
        assert_eq!(c.allocated(), 1);
        assert_eq!(c.get_indices(ipn("127.123.0.0/16")).unwrap(), (123, 123));
        // A /12 spans 16 /16s.
        assert_eq!(c.get_indices(ipn("127.16.0.0/12")).unwrap(), (16, 31));
        // Out of the cluster range.
        assert!(c.occupy(ipn("128.0.0.0/16")).is_err());
        assert!(c.release(ipn("10.0.0.0/16")).is_err());
    }

    // Test_getMaxCIDRs
    #[test]
    fn cidr_set_max_cidrs() {
        assert_eq!(set("10.0.0.0/16", 24).max_cidrs(), 256);
        assert!(CidrSet::new(ipn("10.0.0.0/16"), 8).is_err());
        assert!(CidrSet::new(ipn("10.0.0.0/16"), 33).is_err());
    }

    // TestIndexToCIDRBlock (IPv4 /32 and every IPv6 row)
    #[test]
    fn cidr_set_index_to_cidr_block_v6() {
        // (cluster, mask, index, want) verbatim from cidr_set_test.go:86-196.
        let rows = [
            ("192.168.5.219/28", 32, 5, "192.168.5.213/32"),
            ("2001:0db8:1234:3::/48", 64, 0, "2001:db8:1234::/64"),
            ("2001:0db8:1234::/48", 64, 15, "2001:db8:1234:f::/64"),
            (
                "2001:0db8:85a3::8a2e:0370:7334/50",
                63,
                6425,
                "2001:db8:85a3:3232::/63",
            ),
            ("2001:0db8::/32", 48, 0, "2001:db8::/48"),
            ("2001:0db8::/32", 48, 15, "2001:db8:f::/48"),
            (
                "2001:0db8:85a3::8a2e:0370:7334/32",
                48,
                6425,
                "2001:db8:1919::/48",
            ),
            ("2001:0db8:1234:ff00::/56", 72, 0, "2001:db8:1234:ff00::/72"),
            (
                "2001:0db8:1234:ff00::/56",
                72,
                15,
                "2001:db8:1234:ff00:f00::/72",
            ),
            (
                "2001:0db8:1234:ff00::0370:7334/56",
                72,
                6425,
                "2001:db8:1234:ff19:1900::/72",
            ),
            (
                "2001:0db8:1234:0:1234::/80",
                96,
                0,
                "2001:db8:1234:0:1234::/96",
            ),
            (
                "2001:0db8:1234:0:1234::/80",
                96,
                15,
                "2001:db8:1234:0:1234:f::/96",
            ),
            (
                "2001:0db8:1234:ff00::0370:7334/80",
                96,
                6425,
                "2001:db8:1234:ff00:0:1919::/96",
            ),
        ];
        for (cluster, mask, index, want) in rows {
            let a = set(cluster, mask);
            assert_eq!(
                a.index_to_cidr_block(index).to_string(),
                want,
                "{cluster} /{mask} #{index}"
            );
        }
    }

    // TestCIDRSetv6
    #[test]
    fn cidr_set_v6_allocation_and_subnet_too_big() {
        let a = set("127.0.0.0/8", 32);
        assert_eq!(a.allocate_next().unwrap(), ipn("127.0.0.0/32"));
        assert_eq!(a.allocate_next().unwrap(), ipn("127.0.0.1/32"));
        // "Max cluster subnet size with IPv6": 49 - 32 > clusterSubnetMaxDiff (16).
        let err = CidrSet::new(ipn("beef:1234::/32"), 49).err().unwrap();
        assert_eq!(
            err.to_string(),
            "New CIDR set failed; the node CIDR size is too big"
        );
        let b = set("2001:beef:1234:369b::/60", 64);
        assert_eq!(b.allocate_next().unwrap(), ipn("2001:beef:1234:3690::/64"));
        assert_eq!(b.allocate_next().unwrap(), ipn("2001:beef:1234:3691::/64"));
    }

    // TestGetBitforCIDR / TestOccupy (IPv6 rows)
    #[test]
    fn cidr_set_v6_indices_and_occupy() {
        let a = set("be00::/8", 16);
        assert_eq!(a.get_indices(ipn("be00::/16")).unwrap(), (0, 0));
        let b = set("2001:beef:1200::/40", 48);
        b.occupy(ipn("2001:beef:1200::/40")).unwrap();
        assert_eq!(b.allocated(), 256);
        let c = set("2001:beef:1200::/40", 48);
        c.occupy(ipn("2001:beef:1234::/34")).unwrap();
        assert_eq!(c.allocated(), 256);
        // A v4 CIDR is out of range of a v6 cluster, and vice versa.
        assert!(c.occupy(ipn("10.0.0.0/16")).is_err());
        assert!(set("10.0.0.0/8", 16).occupy(ipn("be00::/16")).is_err());
        // Outside the cluster.
        assert!(c.release(ipn("2001:beef:3400::/48")).is_err());
    }

    // Test_getMaxCIDRs (IPv6 row)
    #[test]
    fn cidr_set_max_cidrs_v6() {
        assert_eq!(set("2001:db8::/48", 64).max_cidrs(), 65536);
    }

    fn expect_metrics(a: &CidrSet, usage: f64, allocs: u64, releases: u64, tries: f64, max: u64) {
        let m = a.metrics();
        assert_eq!(
            (
                m.usage,
                m.allocations,
                m.releases,
                m.allocation_tries_sum,
                m.max_cidrs
            ),
            (usage, allocs, releases, tries, max)
        );
    }

    // TestCidrSetMetrics
    #[test]
    fn cidr_set_metrics() {
        let a = set("10.0.0.0/16", 24);
        assert_eq!(a.metrics().label, "10.0.0.0/16");
        expect_metrics(&a, 0.0, 0, 0, 0.0, 256);
        for i in 1..=256u64 {
            a.allocate_next().unwrap();
            expect_metrics(&a, i as f64 / 256.0, i, 0, 0.0, 256);
        }
        a.release(ipn("10.0.0.0/16")).unwrap();
        expect_metrics(&a, 0.0, 256, 256, 0.0, 256);
        a.occupy(ipn("10.0.0.0/16")).unwrap();
        expect_metrics(&a, 1.0, 512, 256, 0.0, 256);
    }

    // TestCidrSetMetricsHistogram
    #[test]
    fn cidr_set_metrics_histogram() {
        let a = set("10.0.0.0/16", 24);
        a.occupy(ipn("10.0.0.0/17")).unwrap();
        expect_metrics(&a, 0.5, 128, 0, 0.0, 256);
        // Occupy does not move nextCandidate, so AllocateNext walks 128 bits.
        a.allocate_next().unwrap();
        expect_metrics(&a, 129.0 / 256.0, 129, 0, 128.0, 256);
    }

    // TestCidrSetMetricsDual
    #[test]
    fn cidr_set_metrics_dual() {
        let a = set("10.0.0.0/16", 24);
        let b = set("2001:db8::/48", 64);
        expect_metrics(&a, 0.0, 0, 0, 0.0, 256);
        expect_metrics(&b, 0.0, 0, 0, 0.0, 65536);
        a.occupy(ipn("10.0.0.0/16")).unwrap();
        expect_metrics(&a, 1.0, 256, 0, 0.0, 256);
        b.occupy(ipn("2001:db8::/48")).unwrap();
        expect_metrics(&b, 1.0, 65536, 0, 0.0, 65536);
        a.release(ipn("10.0.0.0/16")).unwrap();
        expect_metrics(&a, 0.0, 256, 256, 0.0, 256);
        b.release(ipn("2001:db8::/48")).unwrap();
        expect_metrics(&b, 0.0, 65536, 65536, 0.0, 65536);
    }

    // ---- RangeAllocator: ported from ipam/range_allocator_test.go ----

    fn node_with_cidr(name: &str, cidr: Option<&str>) -> Node {
        let mut n = Node::new(name);
        if let Some(c) = cidr {
            n.spec = Some(rusternetes_common::resources::NodeSpec {
                pod_cidr: Some(c.to_string()),
                pod_cidrs: Some(vec![c.to_string()]),
                provider_id: None,
                unschedulable: None,
                taints: None,
            });
        }
        n
    }

    async fn put(storage: &Arc<MemoryStorage>, n: &Node) {
        storage
            .create(&build_key("nodes", None, &n.metadata.name), n)
            .await
            .unwrap();
    }

    async fn cidr_of(storage: &Arc<MemoryStorage>, name: &str) -> Option<String> {
        let n: Node = storage.get(&build_key("nodes", None, name)).await.unwrap();
        n.spec.and_then(|s| s.pod_cidr)
    }

    fn allocator(
        storage: &Arc<MemoryStorage>,
        cluster: &str,
        mask: u8,
        nodes: &[Node],
    ) -> Result<Arc<RangeAllocator<MemoryStorage>>, String> {
        let cfg = NodeIpamConfig::new(cluster, mask).unwrap();
        RangeAllocator::new(storage.clone(), cfg, nodes).map(Arc::new)
    }

    // TestOccupyPreExistingCIDR
    #[tokio::test]
    async fn new_occupies_preexisting_cidrs() {
        let storage = Arc::new(MemoryStorage::new());
        // A non-network-aligned stored value still occupies its /24.
        let nodes = [
            node_with_cidr("a", Some("10.244.0.1/24")),
            node_with_cidr("b", None),
        ];
        let ra = allocator(&storage, "10.244.0.0/16", 24, &nodes).unwrap();
        assert_eq!(
            ra.cidr_sets[0].allocate_next().unwrap(),
            ipn("10.244.1.0/24")
        );
    }

    #[tokio::test]
    async fn new_fails_on_garbage_or_out_of_range_cidr() {
        let storage = Arc::new(MemoryStorage::new());
        let bad = [node_with_cidr("a", Some("not-a-cidr"))];
        assert!(allocator(&storage, "10.244.0.0/16", 24, &bad).is_err());
        let out = [node_with_cidr("a", Some("192.168.0.0/24"))];
        assert!(allocator(&storage, "10.244.0.0/16", 24, &out).is_err());
    }

    // TestAllocateOrOccupyCIDRSuccess
    #[tokio::test]
    async fn allocate_or_occupy_assigns_lowest_free_and_keeps_existing() {
        let storage = Arc::new(MemoryStorage::new());
        let n1 = node_with_cidr("node-1", None);
        let n2 = node_with_cidr("node-2", None);
        put(&storage, &n1).await;
        put(&storage, &n2).await;
        let ra = allocator(&storage, "10.244.0.0/16", 24, &[]).unwrap();

        ra.allocate_or_occupy_cidr(&n1).await.unwrap();
        ra.allocate_or_occupy_cidr(&n2).await.unwrap();
        assert_eq!(
            cidr_of(&storage, "node-1").await.as_deref(),
            Some("10.244.0.0/24")
        );
        assert_eq!(
            cidr_of(&storage, "node-2").await.as_deref(),
            Some("10.244.1.0/24")
        );
        let stored: Node = storage
            .get(&build_key("nodes", None, "node-1"))
            .await
            .unwrap();
        assert_eq!(
            stored.spec.unwrap().pod_cidrs,
            Some(vec!["10.244.0.0/24".to_string()])
        );

        // Re-running on a node that has a CIDR occupies, never reallocates.
        let stored: Node = storage
            .get(&build_key("nodes", None, "node-1"))
            .await
            .unwrap();
        ra.allocate_or_occupy_cidr(&stored).await.unwrap();
        assert_eq!(
            cidr_of(&storage, "node-1").await.as_deref(),
            Some("10.244.0.0/24")
        );
    }

    // TestAllocateOrOccupyCIDRFailure: exhausted range, no update, event.
    #[tokio::test]
    async fn exhausted_range_errors_without_update_and_records_event() {
        let storage = Arc::new(MemoryStorage::new());
        let n = node_with_cidr("node0", None);
        put(&storage, &n).await;
        let ra = allocator(&storage, "127.123.234.0/28", 30, &[]).unwrap();
        for _ in 0..4 {
            ra.cidr_sets[0].allocate_next().unwrap();
        }
        assert!(ra.allocate_or_occupy_cidr(&n).await.is_err());
        assert_eq!(cidr_of(&storage, "node0").await, None);
        let events: Vec<Event> = storage.list(&build_prefix("events", None)).await.unwrap();
        assert!(
            events.iter().any(|e| e.reason == "CIDRNotAvailable"),
            "got {events:?}"
        );
    }

    // TestReleaseCIDRSuccess
    #[tokio::test]
    async fn release_cidr_returns_it_to_the_pool() {
        let storage = Arc::new(MemoryStorage::new());
        let n = node_with_cidr("node0", Some("10.10.0.0/24"));
        let ra = allocator(&storage, "10.10.0.0/16", 24, std::slice::from_ref(&n)).unwrap();
        ra.release_cidr(&n).unwrap();
        assert_eq!(
            ra.cidr_sets[0].allocate_next().unwrap(),
            ipn("10.10.0.0/24")
        );
        // A node without CIDRs releases nothing.
        ra.release_cidr(&node_with_cidr("x", None)).unwrap();
    }

    // TestNodeDeletionReleaseCIDR (syncNode rows)
    #[tokio::test]
    async fn sync_node_releases_nothing_for_live_or_terminating_nodes() {
        let storage = Arc::new(MemoryStorage::new());
        let mut n = node_with_cidr("node0", Some("10.10.0.0/24"));
        put(&storage, &n).await;
        let ra = allocator(&storage, "10.10.0.0/16", 24, std::slice::from_ref(&n)).unwrap();
        ra.sync_node("nodes/node0").await.unwrap();
        assert_eq!(
            ra.cidr_sets[0].allocate_next().unwrap(),
            ipn("10.10.1.0/24")
        );

        n.metadata.deletion_timestamp = Some(chrono::Utc::now());
        storage
            .update(&build_key("nodes", None, "node0"), &n)
            .await
            .unwrap();
        ra.sync_node("nodes/node0").await.unwrap();
        assert_eq!(
            ra.cidr_sets[0].allocate_next().unwrap(),
            ipn("10.10.2.0/24")
        );
        // A key for a node that no longer exists is a no-op, not an error.
        ra.sync_node("nodes/gone").await.unwrap();
    }

    // updateCIDRsAllocation: node vanished between allocate and write.
    // pkg/controller/nodeipam/ipam/range_allocator.go:395-399 returns the
    // lister error WITHOUT releasing allocatedCIDRs (leaked, by design).
    #[tokio::test]
    async fn allocation_for_vanished_node_leaks_and_errors_like_upstream() {
        let storage = Arc::new(MemoryStorage::new());
        let ra = allocator(&storage, "10.10.0.0/16", 24, &[]).unwrap();
        assert!(ra
            .allocate_or_occupy_cidr(&node_with_cidr("ghost", None))
            .await
            .is_err());
        assert_eq!(ra.cidr_sets[0].allocated(), 1);
    }

    // updateCIDRsAllocation: "Node already has a CIDR allocated. Releasing the new one".
    #[tokio::test]
    async fn allocation_losing_race_releases_the_new_cidr() {
        let storage = Arc::new(MemoryStorage::new());
        let stored = node_with_cidr("node0", Some("10.10.7.0/24"));
        put(&storage, &stored).await;
        let ra = allocator(&storage, "10.10.0.0/16", 24, &[]).unwrap();
        // The caller's snapshot is stale: it still shows no CIDR.
        ra.allocate_or_occupy_cidr(&node_with_cidr("node0", None))
            .await
            .unwrap();
        assert_eq!(
            cidr_of(&storage, "node0").await.as_deref(),
            Some("10.10.7.0/24")
        );
        assert_eq!(ra.cidr_sets[0].allocated(), 0);
    }

    // filterOutServiceRange
    #[tokio::test]
    async fn service_cidr_is_filtered_out() {
        let storage = Arc::new(MemoryStorage::new());
        let cfg = NodeIpamConfig::new("10.0.0.0/16", 24)
            .unwrap()
            .with_service_cidr(ipn("10.0.0.0/23"));
        let ra = RangeAllocator::new(storage, cfg, &[]).unwrap();
        assert_eq!(ra.cidr_sets[0].allocate_next().unwrap(), ipn("10.0.2.0/24"));
    }

    // The issue #1887 race: assignment must not serialise behind one worker.
    #[tokio::test]
    async fn thirty_concurrent_syncs_get_distinct_cidrs() {
        let storage = Arc::new(MemoryStorage::new());
        for i in 0..30 {
            put(&storage, &node_with_cidr(&format!("n{i}"), None)).await;
        }
        let ra = allocator(&storage, "10.244.0.0/16", 24, &[]).unwrap();
        let mut set = tokio::task::JoinSet::new();
        for i in 0..30 {
            let ra = ra.clone();
            set.spawn(async move { ra.sync_node(&format!("nodes/n{i}")).await });
        }
        while let Some(r) = set.join_next().await {
            r.unwrap().unwrap();
        }
        let mut seen = HashSet::new();
        for i in 0..30 {
            let c = cidr_of(&storage, &format!("n{i}")).await.expect("assigned");
            assert!(seen.insert(c), "duplicate CIDR");
        }
    }

    async fn wait_for<F, Fut>(mut f: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        for _ in 0..100 {
            if f().await {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("condition not reached in 5s");
    }

    #[test]
    fn from_flags_parses_service_range() {
        let m = masks(24, 0, 0);
        let cfg = NodeIpamConfig::from_flags("10.0.0.0/16", m, Some("10.0.0.0/23")).unwrap();
        assert_eq!(cfg.service_cidrs, [ipn("10.0.0.0/23")]);
        assert!(NodeIpamConfig::from_flags("10.0.0.0/16", m, Some("nope")).is_err());
        assert!(NodeIpamConfig::from_flags("10.0.0.0/16", m, None)
            .unwrap()
            .service_cidrs
            .is_empty());
    }

    #[test]
    fn worker_count_matches_upstream() {
        // cidrUpdateWorkers = 30, cidrUpdateRetries = 3
        // (pkg/controller/nodeipam/ipam/cidr_allocator.go).
        assert_eq!(CIDR_UPDATE_WORKERS, 30);
        assert_eq!(CIDR_UPDATE_RETRIES, 3);
    }

    // End to end: startup occupy, watch-driven allocate, release on delete.
    #[tokio::test]
    async fn run_occupies_existing_allocates_new_and_releases_deleted() {
        let storage = Arc::new(MemoryStorage::new());
        put(&storage, &node_with_cidr("old", Some("10.244.0.0/24"))).await;
        // A /23 holds exactly two /24s, so the range is full until "old" goes.
        let cfg = NodeIpamConfig::new("10.244.0.0/23", 24).unwrap();
        let s = storage.clone();
        tokio::spawn(async move { run_node_ipam(s, cfg).await });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        put(&storage, &node_with_cidr("new", None)).await;
        let s = storage.clone();
        wait_for(|| {
            let s = s.clone();
            async move { cidr_of(&s, "new").await.is_some() }
        })
        .await;
        // "old" was occupied at startup, so "new" cannot have 10.244.0.0/24.
        assert_eq!(
            cidr_of(&storage, "new").await.as_deref(),
            Some("10.244.1.0/24")
        );

        // Deleting "old" frees 10.244.0.0/24 for the next node.
        storage
            .delete(&build_key("nodes", None, "old"))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        put(&storage, &node_with_cidr("third", None)).await;
        let s = storage.clone();
        wait_for(|| {
            let s = s.clone();
            async move { cidr_of(&s, "third").await.is_some() }
        })
        .await;
        assert_eq!(
            cidr_of(&storage, "third").await.as_deref(),
            Some("10.244.0.0/24")
        );
    }

    // ---- dual-stack: multi-set RangeAllocator (#2409) ----

    fn node_with_cidrs(name: &str, cidrs: &[&str]) -> Node {
        let mut n = Node::new(name);
        if !cidrs.is_empty() {
            n.spec = Some(rusternetes_common::resources::NodeSpec {
                pod_cidr: Some(cidrs[0].to_string()),
                pod_cidrs: Some(cidrs.iter().map(|c| c.to_string()).collect()),
                provider_id: None,
                unschedulable: None,
                taints: None,
            });
        }
        n
    }

    async fn cidrs_of(storage: &Arc<MemoryStorage>, name: &str) -> Vec<String> {
        let n: Node = storage.get(&build_key("nodes", None, name)).await.unwrap();
        n.spec.and_then(|s| s.pod_cidrs).unwrap_or_default()
    }

    /// Builds a config from explicit per-cluster-CIDR masks, as
    /// `CIDRAllocatorParams{ClusterCIDRs, NodeCIDRMaskSizes}`.
    fn dual_cfg(clusters: &[&str], masks: &[u8]) -> NodeIpamConfig {
        NodeIpamConfig {
            cluster_cidrs: clusters.iter().map(|c| ipn(c)).collect(),
            node_masks: masks.to_vec(),
            service_cidrs: Vec::new(),
        }
    }

    // TestOccupyPreExistingCIDR (range_allocator_test.go:45-290), every row.
    #[test]
    fn occupy_pre_existing_cidr_rows() {
        // (description, existing podCIDRs, cluster cidrs, masks, ctrlCreateFail)
        type Row<'a> = (&'a str, &'a [&'a str], &'a [&'a str], &'a [u8], bool);
        let rows: &[Row<'_>] = &[
            (
                "single stack no node allocation",
                &[],
                &["10.10.0.0/16"],
                &[24],
                false,
            ),
            (
                "dual stack no node allocation",
                &[],
                &["10.10.0.0/16", "ace:cab:deca::/8"],
                &[24, 24],
                false,
            ),
            (
                "single stack correct node allocation",
                &["10.10.0.1/24"],
                &["10.10.0.0/16"],
                &[24],
                false,
            ),
            (
                "dual stack both allocated correctly",
                &["10.10.0.1/24", "a00::/86"],
                &["10.10.0.0/16", "ace:cab:deca::/8"],
                &[24, 24],
                false,
            ),
            (
                "fail, single stack incorrect node allocation",
                &["172.10.0.1/24"],
                &["10.10.0.0/16"],
                &[24],
                true,
            ),
            (
                "fail, dualstack node allocating from non existing cidr",
                &["10.10.0.1/24", "a00::/86"],
                &["10.10.0.0/16"],
                &[24],
                true,
            ),
            (
                "fail, dualstack node allocating bad v4",
                &["172.10.0.1/24", "a00::/86"],
                &["10.10.0.0/16", "ace:cab:deca::/8"],
                &[24, 24],
                true,
            ),
            (
                "fail, dualstack node allocating bad v6",
                &["10.10.0.1/24", "cdd::/86"],
                &["10.10.0.0/16", "ace:cab:deca::/8"],
                &[24, 24],
                true,
            ),
        ];
        for (desc, pod_cidrs, clusters, masks, fail) in rows {
            let storage = Arc::new(MemoryStorage::new());
            let node = node_with_cidrs("node0", pod_cidrs);
            let r = RangeAllocator::new(storage, dual_cfg(clusters, masks), &[node]);
            assert_eq!(r.is_err(), *fail, "{desc}: {:?}", r.err());
        }
    }

    // TestAllocateOrOccupyCIDRSuccess "Dualstack CIDRs v4,v6" / "v6,v4": one
    // podCIDR per cluster CIDR, in cluster-CIDR order.
    #[tokio::test]
    async fn dual_stack_allocates_one_cidr_per_family_in_config_order() {
        for (clusters, masks, want) in [
            (
                ["127.123.234.0/8", "ace:cab:deca::/84"],
                [24u8, 98],
                ["127.0.0.0/24", "ace:cab:deca::/98"],
            ),
            (
                ["ace:cab:deca::/84", "127.123.234.0/8"],
                [98u8, 24],
                ["ace:cab:deca::/98", "127.0.0.0/24"],
            ),
        ] {
            let storage = Arc::new(MemoryStorage::new());
            put(&storage, &node_with_cidrs("node0", &[])).await;
            let ra =
                RangeAllocator::new(storage.clone(), dual_cfg(&clusters, &masks), &[]).unwrap();
            ra.allocate_or_occupy_cidr(&node_with_cidrs("node0", &[]))
                .await
                .unwrap();
            assert_eq!(cidrs_of(&storage, "node0").await, want);
            // spec.podCIDR is the primary (first) one.
            assert_eq!(cidr_of(&storage, "node0").await.as_deref(), Some(want[0]));
        }
    }

    // `filterOutServiceRange` loops over every cluster CIDR; a service range
    // only touches the set of the family it overlaps, and the secondary
    // service CIDR is filtered the same way (`SecondaryServiceCIDR`).
    #[tokio::test]
    async fn dual_stack_service_cidrs_filter_only_overlapping_family() {
        let storage = Arc::new(MemoryStorage::new());
        let mut cfg = dual_cfg(&["10.0.0.0/16", "fd00::/48"], &[24, 64]);
        cfg.service_cidrs = vec![ipn("10.0.0.0/23"), ipn("fd00::/62")];
        let ra = RangeAllocator::new(storage, cfg, &[]).unwrap();
        assert_eq!(ra.cidr_sets.len(), 2);
        assert_eq!(ra.cidr_sets[0].allocate_next().unwrap(), ipn("10.0.2.0/24"));
        assert_eq!(
            ra.cidr_sets[1].allocate_next().unwrap(),
            ipn("fd00:0:0:4::/64")
        );
    }

    // TestReleaseCIDRSuccess dual-stack: both CIDRs go back to their own sets.
    #[tokio::test]
    async fn dual_stack_release_returns_each_cidr_to_its_set() {
        let storage = Arc::new(MemoryStorage::new());
        let n = node_with_cidrs("node0", &["10.10.0.0/24", "fd00::/64"]);
        let ra = RangeAllocator::new(
            storage,
            dual_cfg(&["10.10.0.0/16", "fd00::/48"], &[24, 64]),
            std::slice::from_ref(&n),
        )
        .unwrap();
        assert_eq!(ra.cidr_sets[0].allocated(), 1);
        assert_eq!(ra.cidr_sets[1].allocated(), 1);
        ra.release_cidr(&n).unwrap();
        assert_eq!(ra.cidr_sets[0].allocated(), 0);
        assert_eq!(ra.cidr_sets[1].allocated(), 0);
        // `idx >= len(r.cidrSets)`: a CIDR index with no set cannot be released.
        let single = RangeAllocator::new(
            Arc::new(MemoryStorage::new()),
            dual_cfg(&["10.10.0.0/16"], &[24]),
            &[],
        )
        .unwrap();
        assert!(single.release_cidr(&n).is_err());
    }

    // updateCIDRsAllocation "node has cidrs, release the reserved": every
    // allocated CIDR is released, one per set.
    #[tokio::test]
    async fn dual_stack_losing_race_releases_every_new_cidr() {
        let storage = Arc::new(MemoryStorage::new());
        put(
            &storage,
            &node_with_cidrs("node0", &["10.10.7.0/24", "fd00:0:0:7::/64"]),
        )
        .await;
        let ra = RangeAllocator::new(
            storage.clone(),
            dual_cfg(&["10.10.0.0/16", "fd00::/48"], &[24, 64]),
            &[],
        )
        .unwrap();
        ra.allocate_or_occupy_cidr(&node_with_cidrs("node0", &[]))
            .await
            .unwrap();
        assert_eq!(ra.cidr_sets[0].allocated(), 0);
        assert_eq!(ra.cidr_sets[1].allocated(), 0);
        assert_eq!(
            cidrs_of(&storage, "node0").await,
            ["10.10.7.0/24", "fd00:0:0:7::/64"]
        );
    }

    // range_allocator.go:316-339 AllocateOrOccupyCIDR returns the set-1 error
    // without releasing set 0's CIDR (leaked, again on every retry).
    #[tokio::test]
    async fn dual_stack_partial_allocation_failure_leaks_earlier_sets_like_upstream() {
        let storage = Arc::new(MemoryStorage::new());
        let n = node_with_cidrs("node0", &[]);
        put(&storage, &n).await;
        // v6 /126 with /127 nodes -> 2 CIDRs; exhaust them.
        let ra = RangeAllocator::new(
            storage.clone(),
            dual_cfg(&["10.10.0.0/16", "fd00::/126"], &[24, 127]),
            &[],
        )
        .unwrap();
        ra.cidr_sets[1].allocate_next().unwrap();
        ra.cidr_sets[1].allocate_next().unwrap();
        assert!(ra.allocate_or_occupy_cidr(&n).await.is_err());
        assert_eq!(ra.cidr_sets[0].allocated(), 1);
        assert!(ra.allocate_or_occupy_cidr(&n).await.is_err());
        assert_eq!(ra.cidr_sets[0].allocated(), 2);
        assert!(cidrs_of(&storage, "node0").await.is_empty());
        let events: Vec<Event> = storage.list(&build_prefix("events", None)).await.unwrap();
        assert!(events.iter().any(|e| e.reason == "CIDRNotAvailable"));
    }

    // ---- --cluster-cidr list / --node-cidr-mask-size-ipv4/-ipv6 ----
    // cmd/kube-controller-manager/app/core.go validateCIDRs + setNodeCIDRMaskSizes

    fn masks(general: u8, ipv4: u8, ipv6: u8) -> NodeCidrMaskSizes {
        NodeCidrMaskSizes {
            general,
            ipv4,
            ipv6,
        }
    }

    #[test]
    fn from_flags_dual_stack_uses_per_family_masks_and_defaults() {
        let cfg =
            NodeIpamConfig::from_flags("10.244.0.0/16,fd00::/48", masks(0, 0, 0), None).unwrap();
        assert_eq!(cfg.cluster_cidrs, [ipn("10.244.0.0/16"), ipn("fd00::/48")]);
        // defaultNodeMaskCIDRIPv4 = 24, defaultNodeMaskCIDRIPv6 = 64
        assert_eq!(cfg.node_masks, [24, 64]);

        // sortedSizes follows cluster-CIDR order, not v4-first.
        let cfg =
            NodeIpamConfig::from_flags("fd00::/48, 10.244.0.0/16", masks(0, 26, 56), None).unwrap();
        assert_eq!(cfg.cluster_cidrs, [ipn("fd00::/48"), ipn("10.244.0.0/16")]);
        assert_eq!(cfg.node_masks, [56, 26]);
    }

    #[test]
    fn from_flags_single_stack_mask_rules() {
        // Single-stack IPv6 defaults to /64.
        let cfg = NodeIpamConfig::from_flags("fd00::/48", masks(0, 0, 0), None).unwrap();
        assert_eq!(cfg.node_masks, [64]);
        // --node-cidr-mask-size is the reference for a single family.
        let cfg = NodeIpamConfig::from_flags("fd00::/48", masks(60, 0, 0), None).unwrap();
        assert_eq!(cfg.node_masks, [60]);
        // Family flags are accepted only for their own family.
        let cfg = NodeIpamConfig::from_flags("10.0.0.0/16", masks(0, 26, 0), None).unwrap();
        assert_eq!(cfg.node_masks, [26]);
        assert!(NodeIpamConfig::from_flags("10.0.0.0/16", masks(0, 0, 64), None).is_err());
        assert!(NodeIpamConfig::from_flags("fd00::/48", masks(0, 24, 0), None).is_err());
        // Mixing the general flag with a family flag is an error.
        assert!(NodeIpamConfig::from_flags("10.0.0.0/16", masks(24, 24, 0), None).is_err());
    }

    #[test]
    fn from_flags_dual_stack_rejects_general_mask_and_bad_lists() {
        let e =
            NodeIpamConfig::from_flags("10.0.0.0/16,fd00::/48", masks(24, 0, 0), None).unwrap_err();
        assert!(
            e.contains("--node-cidr-mask-size is not allowed with dual-stack"),
            "{e}"
        );
        // two cidrs of the same family
        let e = NodeIpamConfig::from_flags("10.0.0.0/16,10.1.0.0/16", masks(0, 0, 0), None)
            .unwrap_err();
        assert!(e.contains("not configured as dual stack"), "{e}");
        // more than two
        let e =
            NodeIpamConfig::from_flags("10.0.0.0/16,fd00::/48,10.1.0.0/16", masks(0, 0, 0), None)
                .unwrap_err();
        assert!(e.contains("more than max allowed of 2"), "{e}");
        assert!(NodeIpamConfig::from_flags("10.0.0.0/16,nope", masks(0, 0, 0), None).is_err());
        // A mask that does not fit its own cluster CIDR is still rejected.
        assert!(NodeIpamConfig::from_flags("10.0.0.0/16,fd00::/48", masks(0, 8, 0), None).is_err());
    }

    #[test]
    fn from_flags_service_cidr_list() {
        let cfg = NodeIpamConfig::from_flags(
            "10.0.0.0/16,fd00::/48",
            masks(0, 0, 0),
            Some("10.0.0.0/23,fd00::/62"),
        )
        .unwrap();
        assert_eq!(cfg.service_cidrs, [ipn("10.0.0.0/23"), ipn("fd00::/62")]);
        // `--service-cluster-ip-range can not contain more than two entries`
        assert!(NodeIpamConfig::from_flags(
            "10.0.0.0/16",
            masks(0, 0, 0),
            Some("10.0.0.0/23,fd00::/62,10.9.0.0/24")
        )
        .is_err());
        // serviceCIDR and secondaryServiceCIDR must be from different families.
        let e = NodeIpamConfig::from_flags(
            "10.0.0.0/16",
            masks(0, 0, 0),
            Some("10.0.0.0/23,10.9.0.0/24"),
        )
        .unwrap_err();
        assert!(e.contains("not dualstack"), "{e}");
    }
}
