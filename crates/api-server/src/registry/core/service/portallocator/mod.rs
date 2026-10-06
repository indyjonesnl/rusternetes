//! Port of `pkg/registry/core/service/portallocator`: allocation of service
//! NodePorts out of `--service-node-port-range` (`allocator.go`), the
//! per-request transaction over it (`operation.go`, in [`operation`]) and the
//! repair loop that rebuilds it from the stored Services
//! (`controller/repair.go`, in [`repair`]).

pub mod operation;
pub mod repair;

use std::fmt;

use rusternetes_common::resources::rangeallocation::RangeAllocation;
use rusternetes_common::Error;

use super::allocator::{AllocationBitmap, AllocatorWithOffsetFactory, InMemory, Interface};

/// `utilnet.PortRange` (apimachinery/pkg/util/net/port_range.go:25-28).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortRange {
    pub base: usize,
    pub size: usize,
}

/// `DefaultServiceNodePortRange` (pkg/kubeapiserver/options/options.go:27).
pub const DEFAULT_SERVICE_NODE_PORT_RANGE: PortRange = PortRange {
    base: 30000,
    size: 2768,
};

impl PortRange {
    /// `PortRange.Contains` (port_range.go:31-33).
    pub fn contains(&self, p: usize) -> bool {
        p >= self.base && (p - self.base) < self.size
    }

    /// `ParsePortRange` / `PortRange.Set` (port_range.go:42-126): `min-max`,
    /// `min+offset` or a single port.
    pub fn parse(value: &str) -> Result<Self, String> {
        let value = value.trim();
        if value.is_empty() {
            return Ok(Self { base: 0, size: 0 });
        }
        let atoi = |s: &str| {
            s.parse::<usize>()
                .map_err(|e| format!("strconv.Atoi: parsing {s:?}: {e}"))
        };
        let (low, high) = match (value.find('-'), value.find('+')) {
            (None, None) => {
                let port = atoi(value)?;
                (port, port)
            }
            (Some(h), None) => (atoi(&value[..h])?, atoi(&value[h + 1..])?),
            (None, Some(p)) => {
                let low = atoi(&value[..p])?;
                (low, low + atoi(&value[p + 1..])?)
            }
            _ => return Err(format!("unable to parse port range: {value}")),
        };
        if low > 65535 || high > 65535 {
            return Err(format!(
                "the port range cannot be greater than 65535: {value}"
            ));
        }
        if high < low {
            return Err(format!("end port cannot be less than start port: {value}"));
        }
        Ok(Self {
            base: low,
            size: 1 + high - low,
        })
    }
}

/// `PortRange.Set` as a flag value (port_range.go:78-126), so clap can parse
/// `--service-node-port-range` the way pflag's `fs.Var` does.
impl std::str::FromStr for PortRange {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        Self::parse(s)
    }
}

/// `PortRange.String` (port_range.go:35-40).
impl fmt::Display for PortRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.size == 0 {
            return Ok(());
        }
        write!(f, "{}-{}", self.base, self.base + self.size - 1)
    }
}

/// The errors of `allocator.go:39-52`, plus a storage failure.
#[derive(Debug)]
pub enum PortError {
    /// `ErrFull`.
    Full,
    /// `ErrAllocated`.
    Allocated,
    /// `ErrNotInRange`.
    NotInRange {
        valid_ports: String,
    },
    /// `ErrMismatchedNetwork`.
    MismatchedNetwork,
    Other(Error),
}

impl fmt::Display for PortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortError::Full => f.write_str("range is full"),
            PortError::Allocated => f.write_str("provided port is already allocated"),
            PortError::NotInRange { valid_ports } => write!(
                f,
                "provided port is not in the valid range. The range of valid ports is {valid_ports}"
            ),
            PortError::MismatchedNetwork => {
                f.write_str("the provided port range does not match the current port range")
            }
            PortError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl From<Error> for PortError {
    fn from(e: Error) -> Self {
        PortError::Other(e)
    }
}

pub type PortResult<T> = std::result::Result<T, PortError>;

/// `PortAllocator` (allocator.go:54-60).
pub struct PortAllocator {
    port_range: PortRange,
    alloc: Box<dyn Interface>,
}

impl PortAllocator {
    /// `New` (allocator.go:65-83): the backing store comes from `factory`.
    pub fn new(
        pr: PortRange,
        factory: AllocatorWithOffsetFactory,
    ) -> rusternetes_common::Result<Self> {
        let alloc = factory(pr.size, pr.to_string(), calculate_range_offset(pr))?;
        Ok(Self {
            port_range: pr,
            alloc,
        })
    }

    /// `NewInMemory` (allocator.go:85-90).
    pub fn new_in_memory(pr: PortRange) -> Self {
        Self::new(
            pr,
            Box::new(|max, spec, offset| {
                Ok(Box::new(InMemory::new(AllocationBitmap::with_offset(
                    max, spec, offset,
                ))))
            }),
        )
        .expect("an in-memory allocator cannot fail to build")
    }

    /// `NewFromSnapshot` (allocator.go:92-106).
    pub async fn from_snapshot(snap: &RangeAllocation) -> PortResult<Self> {
        let pr = PortRange::parse(&snap.range).map_err(|e| PortError::Other(Error::Internal(e)))?;
        let r = Self::new_in_memory(pr);
        r.restore(pr, &snap.data).await?;
        Ok(r)
    }

    pub fn port_range(&self) -> PortRange {
        self.port_range
    }

    /// `PortAllocator.Free` (allocator.go:108-111).
    #[allow(dead_code)] // upstream API; exercised by the tests
    pub async fn free(&self) -> usize {
        self.alloc.free().await
    }

    /// `PortAllocator.Allocate` (allocator.go:118-147).
    pub async fn allocate(&self, port: usize) -> PortResult<()> {
        let Some(offset) = self.contains(port) else {
            return Err(PortError::NotInRange {
                valid_ports: self.port_range.to_string(),
            });
        };
        if !self.alloc.allocate(offset).await? {
            return Err(PortError::Allocated);
        }
        Ok(())
    }

    /// `PortAllocator.AllocateNext` (allocator.go:149-167).
    pub async fn allocate_next(&self) -> PortResult<usize> {
        match self.alloc.allocate_next().await? {
            Some(offset) => Ok(self.port_range.base + offset),
            None => Err(PortError::Full),
        }
    }

    /// `PortAllocator.ForEach` (allocator.go:169-174): every allocated port.
    pub async fn allocated(&self) -> Vec<usize> {
        self.alloc
            .allocated()
            .await
            .into_iter()
            .map(|offset| self.port_range.base + offset)
            .collect()
    }

    /// `PortAllocator.Release` (allocator.go:176-193): a port outside the
    /// range is ignored.
    pub async fn release(&self, port: usize) -> PortResult<()> {
        let Some(offset) = self.contains(port) else {
            tracing::warn!("port is not in the range when release it. port: {port}");
            return Ok(());
        };
        Ok(self.alloc.release(offset).await?)
    }

    /// `PortAllocator.Has` (allocator.go:195-204).
    pub async fn has(&self, port: usize) -> bool {
        match self.contains(port) {
            Some(offset) => self.alloc.has(offset).await,
            None => false,
        }
    }

    /// `PortAllocator.Snapshot` (allocator.go:206-216).
    pub async fn snapshot(&self, dst: &mut RangeAllocation) -> PortResult<()> {
        let (range, data) = self.alloc.snapshot().await?;
        dst.range = range;
        dst.data = data;
        Ok(())
    }

    /// `PortAllocator.Restore` (allocator.go:218-229).
    pub async fn restore(&self, pr: PortRange, data: &[u8]) -> PortResult<()> {
        if pr != self.port_range {
            return Err(PortError::MismatchedNetwork);
        }
        Ok(self.alloc.restore(&pr.to_string(), data).await?)
    }

    /// `PortAllocator.contains` (allocator.go:231-240): the offset of `port`.
    fn contains(&self, port: usize) -> Option<usize> {
        self.port_range
            .contains(port)
            .then(|| port - self.port_range.base)
    }
}

/// `calculateRangeOffset` (allocator.go:253-285): the band at the bottom of
/// the range kept for static allocations — `min(max(16, size/32), 128)`, or
/// none for a range of 16 ports or fewer.
pub fn calculate_range_offset(pr: PortRange) -> usize {
    const MIN: usize = 16;
    const MAX: usize = 128;
    const STEP: usize = 32;
    if pr.size <= MIN {
        return 0;
    }
    (pr.size / STEP).clamp(MIN, MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

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
