//! Unit tests moved from `src/registry/core/service/portallocator/mod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::core::service::allocator::{
        AllocationBitmap, AllocatorWithOffsetFactory, InMemory, Interface,
    };
    pub use rusternetes_common::resources::rangeallocation::RangeAllocation;
    pub use rusternetes_common::Error;
    pub use std::fmt;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::service::portallocator::*;

    fn pr(s: &str) -> PortRange {
        PortRange::parse(s).unwrap()
    }

    /// `TestPortRange` (apimachinery/pkg/util/net/port_range_test.go:25-80).
    #[test]
    fn port_range_parses_like_upstream() {
        let cases: [(&str, bool, &str, i64, i64); 19] = [
            ("100-200", true, "100-200", 200, 201),
            (" 100-200 ", true, "100-200", 200, 201),
            ("0-0", true, "0-0", 0, 1),
            ("", true, "", -1, 0),
            ("100", true, "100-100", 100, 101),
            ("100 - 200", false, "", -1, -1),
            ("-100", false, "", -1, -1),
            ("100-", false, "", -1, -1),
            ("200-100", false, "", -1, -1),
            ("60000-70000", false, "", -1, -1),
            ("70000-80000", false, "", -1, -1),
            ("70000+80000", false, "", -1, -1),
            ("1+0", true, "1-1", 1, 2),
            ("0+0", true, "0-0", 0, 1),
            ("1+-1", false, "", -1, -1),
            ("1-+1", false, "", -1, -1),
            ("100+200", true, "100-300", 300, 301),
            ("1+65535", false, "", -1, -1),
            ("0+65535", true, "0-65535", 65535, 65536),
        ];
        for (input, success, expected, included, excluded) in cases {
            let parsed = PortRange::parse(input);
            assert_eq!(parsed.is_ok(), success, "{input:?}: {parsed:?}");
            let Ok(range) = parsed else { continue };
            assert_eq!(range.to_string(), expected, "{input:?}");
            if included >= 0 {
                assert!(
                    range.contains(included as usize),
                    "{input:?} includes {included}"
                );
            }
            if excluded >= 0 {
                assert!(
                    !range.contains(excluded as usize),
                    "{input:?} excludes {excluded}"
                );
            }
        }
        assert_eq!(DEFAULT_SERVICE_NODE_PORT_RANGE.to_string(), "30000-32767");
    }

    /// `TestAllocate` (portallocator/allocator_test.go:29-119).
    #[tokio::test]
    async fn allocate_every_port_then_full() {
        let r = PortAllocator::new_in_memory(pr("10000-10199"));
        assert_eq!(r.free().await, 200);
        let mut found = std::collections::BTreeSet::new();
        for _ in 0..200 {
            let p = r.allocate_next().await.unwrap();
            assert!(found.insert(p), "{p} allocated twice");
            assert!((10000..10200).contains(&p));
        }
        assert!(matches!(r.allocate_next().await, Err(PortError::Full)));

        let released = 10005;
        r.release(released).await.unwrap();
        assert_eq!(r.free().await, 1);
        assert_eq!(r.allocate_next().await.unwrap(), released);

        r.release(released).await.unwrap();
        let err = r.allocate(1).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "provided port is not in the valid range. The range of valid ports is 10000-10199"
        );
        assert!(matches!(r.allocate(10001).await, Err(PortError::Allocated)));
        assert!(matches!(
            r.allocate(20000).await,
            Err(PortError::NotInRange { .. })
        ));
        r.allocate(released).await.unwrap();
        assert!(r.has(released).await);
        // Releasing outside the range is a no-op.
        r.release(1).await.unwrap();
    }

    /// `TestSnapshot` and `TestNewFromSnapshot` (allocator_test.go:212-314).
    #[tokio::test]
    async fn snapshot_restores_into_the_same_range_only() {
        let r = PortAllocator::new_in_memory(pr("10000-10200"));
        let ports = [10000, 10010, 10011, 10199];
        for p in ports {
            r.allocate(p).await.unwrap();
        }
        let mut dst = RangeAllocation::default();
        r.snapshot(&mut dst).await.unwrap();
        assert_eq!(dst.range, "10000-10200");

        let other = PortAllocator::new_in_memory(pr("10000-10200"));
        assert!(matches!(
            other.restore(pr("10000-10100"), &dst.data).await,
            Err(PortError::MismatchedNetwork)
        ));
        other.restore(pr("10000-10200"), &dst.data).await.unwrap();
        assert_eq!(other.allocated().await, ports);
        let from = PortAllocator::from_snapshot(&dst).await.unwrap();
        assert_eq!(from.allocated().await, ports);
    }

    /// `Test_calculateRangeOffset` (allocator_test.go:316-403).
    #[test]
    fn range_offset_matches_upstream() {
        for (size, want) in [
            (2768, 86),
            (10, 0),
            (16, 0),
            (128, 16),
            (2048, 64),
            (4096, 128),
            (8192, 128),
        ] {
            let pr = PortRange { base: 30000, size };
            assert_eq!(calculate_range_offset(pr), want, "{size}");
        }
    }

    /// `TestAllocateReserved` (allocator_test.go:121-169): dynamic
    /// allocation fills the upper block first, leaving the static band.
    #[tokio::test]
    async fn allocate_next_keeps_the_static_band_for_last() {
        let range = pr("30000-30128");
        let r = PortAllocator::new_in_memory(range);
        let dynamic_offset = calculate_range_offset(range);
        for _ in 0..(range.size - dynamic_offset) {
            r.allocate_next().await.unwrap();
        }
        for i in dynamic_offset..range.size {
            assert!(
                r.has(range.base + i).await,
                "{} not allocated",
                range.base + i
            );
        }
        assert_eq!(r.free().await, dynamic_offset);
        for i in 0..dynamic_offset {
            r.allocate(range.base + i).await.unwrap();
        }
        assert_eq!(r.free().await, 0);
        r.release(30053).await.unwrap();
        r.allocate_next().await.unwrap();
        assert_eq!(r.free().await, 0);
    }
}
