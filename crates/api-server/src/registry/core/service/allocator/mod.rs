//! Port of `pkg/registry/core/service/allocator`: a bitmap over a contiguous
//! range of items (`bitmap.go`), the interfaces over it (`interfaces.go`),
//! and its storage-backed form (`storage/storage.go`, in [`storage`]).

pub mod storage;

use std::sync::Mutex;

use async_trait::async_trait;
use rand::Rng;
use rusternetes_common::{Error, Result};

/// `allocator.Interface` (interfaces.go:19-31): allocation of items out of a
/// range. Async because the storage-backed allocator persists every change.
#[async_trait]
pub trait Interface: Send + Sync {
    /// Reserve `offset`: `Ok(false)` if it is already taken.
    async fn allocate(&self, offset: usize) -> Result<bool>;
    /// Reserve any free item: `Ok(None)` if there are none left.
    async fn allocate_next(&self) -> Result<Option<usize>>;
    /// Return `offset` to the pool; releasing a free item is a no-op.
    async fn release(&self, offset: usize) -> Result<()>;
    /// Every allocated offset, ascending.
    async fn allocated(&self) -> Vec<usize>;
    async fn has(&self, offset: usize) -> bool;
    /// The number of free items.
    #[allow(dead_code)] // upstream API; exercised by the tests
    async fn free(&self) -> usize;
    /// `Snapshottable.Snapshot` (interfaces.go:35-40), for the allocators
    /// that are snapshottable.
    async fn snapshot(&self) -> Result<(String, Vec<u8>)> {
        Err(Error::Internal("not a snapshottable allocator".to_string()))
    }
    /// `Snapshottable.Restore`.
    async fn restore(&self, _range_spec: &str, _data: &[u8]) -> Result<()> {
        Err(Error::Internal("not a snapshottable allocator".to_string()))
    }
}

/// A shared allocator: upstream hands the same `*Etcd` to the port allocator
/// and, as its `RangeRegistry`, to the repair loop (storage_core.go:484-492).
#[async_trait]
impl<T: Interface + ?Sized> Interface for std::sync::Arc<T> {
    async fn allocate(&self, offset: usize) -> Result<bool> {
        (**self).allocate(offset).await
    }
    async fn allocate_next(&self) -> Result<Option<usize>> {
        (**self).allocate_next().await
    }
    async fn release(&self, offset: usize) -> Result<()> {
        (**self).release(offset).await
    }
    async fn allocated(&self) -> Vec<usize> {
        (**self).allocated().await
    }
    async fn has(&self, offset: usize) -> bool {
        (**self).has(offset).await
    }
    async fn free(&self) -> usize {
        (**self).free().await
    }
    async fn snapshot(&self) -> Result<(String, Vec<u8>)> {
        (**self).snapshot().await
    }
    async fn restore(&self, range_spec: &str, data: &[u8]) -> Result<()> {
        (**self).restore(range_spec, data).await
    }
}

/// `AllocatorWithOffsetFactory` (interfaces.go:44): builds the backing store
/// of a range allocator from its size, range spec and static-band offset.
pub type AllocatorWithOffsetFactory =
    Box<dyn FnOnce(usize, String, usize) -> Result<Box<dyn Interface>> + Send>;

/// `AllocationBitmap` (bitmap.go:28-47): each item has an offset, and a set
/// bit marks it taken. `count` is always the number of set bits.
#[derive(Debug, Clone)]
pub struct AllocationBitmap {
    /// `randomScanStrategyWithOffset.offset` (bitmap.go:236-240).
    offset: usize,
    max: usize,
    range_spec: String,
    count: usize,
    /// The bit array, least significant word first — the words of Go's
    /// `big.Int`.
    words: Vec<u64>,
}

impl AllocationBitmap {
    /// `NewAllocationMap` (bitmap.go:55-58): the random-scan strategy.
    #[allow(dead_code)] // upstream API; exercised by the tests
    pub fn new(max: usize, range_spec: impl Into<String>) -> Self {
        Self::with_offset(max, range_spec, 0)
    }

    /// `NewAllocationMapWithOffset` (bitmap.go:60-78): random scan that
    /// prefers `[offset, max)` and only falls back to `[0, offset)` — the
    /// band left for static allocations — once that is full.
    pub fn with_offset(max: usize, range_spec: impl Into<String>, offset: usize) -> Self {
        Self {
            offset,
            max,
            range_spec: range_spec.into(),
            count: 0,
            words: Vec::new(),
        }
    }

    fn bit(&self, at: usize) -> bool {
        self.words
            .get(at / 64)
            .is_some_and(|w| w & (1 << (at % 64)) != 0)
    }

    fn set_bit(&mut self, at: usize, on: bool) {
        let word = at / 64;
        if on {
            if self.words.len() <= word {
                self.words.resize(word + 1, 0);
            }
            self.words[word] |= 1 << (at % 64);
        } else if let Some(w) = self.words.get_mut(word) {
            *w &= !(1 << (at % 64));
            // Keep the words normalised as `big.Int` does.
            while self.words.last() == Some(&0) {
                self.words.pop();
            }
        }
    }

    /// `AllocationBitmap.Allocate` (bitmap.go:80-96).
    pub fn allocate(&mut self, offset: usize) -> Result<bool> {
        if offset >= self.max {
            return Err(Error::Internal(format!(
                "offset {offset} out of range [0,{}]",
                self.max
            )));
        }
        if self.bit(offset) {
            return Ok(false);
        }
        self.set_bit(offset, true);
        self.count += 1;
        Ok(true)
    }

    /// `AllocationBitmap.AllocateNext` (bitmap.go:98-111).
    pub fn allocate_next(&mut self) -> Option<usize> {
        let next = self.random_scan_with_offset()?;
        self.count += 1;
        self.set_bit(next, true);
        Some(next)
    }

    /// `randomScanStrategyWithOffset.AllocateBit` (bitmap.go:242-262).
    fn random_scan_with_offset(&self) -> Option<usize> {
        if self.count >= self.max {
            return None;
        }
        let mut rng = rand::rng();
        // The upper subrange, prioritised for random allocation.
        let subrange_max = self.max - self.offset;
        let start = rng.random_range(0..subrange_max);
        for i in 0..subrange_max {
            let at = self.offset + ((start + i) % subrange_max);
            if !self.bit(at) {
                return Some(at);
            }
        }
        if self.offset == 0 {
            return None;
        }
        // Subrange full: try the first block before giving up.
        let start = rng.random_range(0..self.offset);
        for i in 0..self.offset {
            let at = (start + i) % self.offset;
            if !self.bit(at) {
                return Some(at);
            }
        }
        None
    }

    /// `AllocationBitmap.Release` (bitmap.go:113-127): releasing a free item
    /// is a no-op.
    pub fn release(&mut self, offset: usize) {
        if !self.bit(offset) {
            return;
        }
        self.set_bit(offset, false);
        self.count -= 1;
    }

    /// `AllocationBitmap.ForEach` (bitmap.go:136-155): every set bit,
    /// ascending.
    pub fn allocated(&self) -> Vec<usize> {
        let mut out = Vec::with_capacity(self.count);
        for (i, word) in self.words.iter().enumerate() {
            let mut w = *word;
            while w != 0 {
                let bit = w.trailing_zeros() as usize;
                out.push(i * 64 + bit);
                w &= w - 1;
            }
        }
        out
    }

    /// `AllocationBitmap.Has` (bitmap.go:157-164).
    pub fn has(&self, offset: usize) -> bool {
        self.bit(offset)
    }

    /// `AllocationBitmap.Free` (bitmap.go:166-171).
    #[allow(dead_code)] // upstream API; exercised by the tests
    pub fn free(&self) -> usize {
        self.max - self.count
    }

    /// `AllocationBitmap.Snapshot` (bitmap.go:173-179): the range spec and
    /// `big.Int.Bytes()` of the bit array — big-endian with no leading zero
    /// bytes, so a snapshot is interchangeable with upstream's.
    pub fn snapshot(&self) -> (String, Vec<u8>) {
        let mut bytes: Vec<u8> = self
            .words
            .iter()
            .rev()
            .flat_map(|w| w.to_be_bytes())
            .collect();
        let leading = bytes.iter().take_while(|b| **b == 0).count();
        bytes.drain(..leading);
        (self.range_spec.clone(), bytes)
    }

    /// `AllocationBitmap.Restore` (bitmap.go:181-194): `big.Int.SetBytes`.
    pub fn restore(&mut self, range_spec: &str, data: &[u8]) -> Result<()> {
        if self.range_spec != range_spec {
            return Err(Error::Internal(
                "the provided range does not match the current range".to_string(),
            ));
        }
        let mut words = Vec::with_capacity(data.len().div_ceil(8));
        for chunk in data.rchunks(8) {
            let mut buf = [0u8; 8];
            buf[8 - chunk.len()..].copy_from_slice(chunk);
            words.push(u64::from_be_bytes(buf));
        }
        while words.last() == Some(&0) {
            words.pop();
        }
        // `countBits` (utils.go:24-31).
        self.count = words.iter().map(|w| w.count_ones() as usize).sum();
        self.words = words;
        Ok(())
    }
}

/// An in-memory [`AllocationBitmap`] behind its lock (bitmap.go:39-41): what
/// `NewAllocationMapWithOffset` hands a range allocator.
#[derive(Debug)]
pub struct InMemory(Mutex<AllocationBitmap>);

impl InMemory {
    pub fn new(bitmap: AllocationBitmap) -> Self {
        Self(Mutex::new(bitmap))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, AllocationBitmap> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }
}

#[async_trait]
impl Interface for InMemory {
    async fn allocate(&self, offset: usize) -> Result<bool> {
        self.lock().allocate(offset)
    }
    async fn allocate_next(&self) -> Result<Option<usize>> {
        Ok(self.lock().allocate_next())
    }
    async fn release(&self, offset: usize) -> Result<()> {
        self.lock().release(offset);
        Ok(())
    }
    async fn allocated(&self) -> Vec<usize> {
        self.lock().allocated()
    }
    async fn has(&self, offset: usize) -> bool {
        self.lock().has(offset)
    }
    async fn free(&self) -> usize {
        self.lock().free()
    }
    async fn snapshot(&self) -> Result<(String, Vec<u8>)> {
        Ok(self.lock().snapshot())
    }
    async fn restore(&self, range_spec: &str, data: &[u8]) -> Result<()> {
        self.lock().restore(range_spec, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
