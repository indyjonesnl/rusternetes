//! Unit tests moved from `src/registry/core/service/allocator/mod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rand::Rng;
    pub use rusternetes_common::{Error, Result};
    pub use std::sync::Mutex;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::service::allocator::*;

    /// `TestAllocate` (bitmap_test.go:26-62).
    #[test]
    fn allocate_fills_then_frees() {
        let max = 10;
        let mut m = AllocationBitmap::new(max, "test");
        let mut found = std::collections::BTreeSet::new();
        for _ in 0..max {
            let p = m.allocate_next().expect("free item");
            assert!(found.insert(p), "{p} allocated twice");
        }
        assert_eq!(m.free(), 0);
        assert!(m.allocate_next().is_none());
        let released = 3;
        m.release(released);
        assert_eq!(m.free(), 1);
        assert_eq!(m.allocate_next(), Some(released));
    }

    /// `TestAllocateMax` / `TestAllocateError` (bitmap_test.go): out of range
    /// is an error, a taken item is `false`.
    #[test]
    fn allocate_rejects_out_of_range_and_taken() {
        let mut m = AllocationBitmap::new(10, "test");
        assert!(m.allocate(10).is_err());
        assert!(m.allocate(3).unwrap());
        assert!(!m.allocate(3).unwrap());
    }

    /// `TestAllocateMaxOffset` / `TestAllocateOffset` (bitmap_test.go): with
    /// an offset, dynamic allocation exhausts `[offset, max)` before
    /// touching the static band.
    #[test]
    fn with_offset_fills_the_upper_block_first() {
        let (max, offset) = (16, 4);
        let mut m = AllocationBitmap::with_offset(max, "test", offset);
        for _ in 0..(max - offset) {
            let p = m.allocate_next().unwrap();
            assert!(p >= offset, "{p} is in the static band");
        }
        for _ in 0..offset {
            let p = m.allocate_next().unwrap();
            assert!(p < offset, "{p} was expected in the static band");
        }
        assert!(m.allocate_next().is_none());
    }

    /// `TestForEach` (bitmap_test.go).
    #[test]
    fn allocated_lists_every_set_bit() {
        let mut m = AllocationBitmap::new(200, "test");
        for p in [0, 7, 63, 64, 130, 199] {
            m.allocate(p).unwrap();
        }
        assert_eq!(m.allocated(), [0, 7, 63, 64, 130, 199]);
    }

    /// `TestSnapshotAndRestore` (bitmap_test.go): and the bytes are Go's
    /// `big.Int.Bytes()`.
    #[test]
    fn snapshot_restore_round_trips_in_big_int_bytes() {
        let mut m = AllocationBitmap::new(200, "test");
        m.allocate(0).unwrap();
        m.allocate(9).unwrap();
        let (spec, data) = m.snapshot();
        assert_eq!(spec, "test");
        // bits 0 and 9 = 0x0201.
        assert_eq!(data, [0x02, 0x01]);

        let mut other = AllocationBitmap::new(200, "test");
        other.restore(&spec, &data).unwrap();
        assert!(other.has(0) && other.has(9) && !other.has(1));
        assert_eq!(other.free(), 198);
        assert!(other.restore("other", &data).is_err());

        let mut wide = AllocationBitmap::new(200, "test");
        wide.allocate(150).unwrap();
        let (_, data) = wide.snapshot();
        let mut back = AllocationBitmap::new(200, "test");
        back.restore("test", &data).unwrap();
        assert_eq!(back.allocated(), [150]);
        assert!(AllocationBitmap::new(1, "x").snapshot().1.is_empty());
    }
}
