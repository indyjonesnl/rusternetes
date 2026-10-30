//! Port of `pkg/registry/core/service/portallocator/operation.go`: a port
//! allocation "transaction". Leaking a port is better than handing it out
//! twice, so allocations happen at once and releases are deferred: a commit
//! performs the deferred releases, a rollback releases what was allocated.

use std::sync::Arc;

use super::{PortAllocator, PortError, PortResult};

/// `PortAllocationOperation` (operation.go:33-39).
pub struct PortAllocationOperation {
    pa: Arc<PortAllocator>,
    allocated: Vec<usize>,
    release_deferred: Vec<usize>,
    should_rollback: bool,
    dry_run: bool,
}

impl PortAllocationOperation {
    /// `StartOperation` (operation.go:41-51): with `dry_run`, nothing is
    /// ever allocated or released.
    pub fn start(pa: Arc<PortAllocator>, dry_run: bool) -> Self {
        Self {
            pa,
            allocated: Vec::new(),
            release_deferred: Vec::new(),
            should_rollback: true,
            dry_run,
        }
    }

    /// `Finish` (operation.go:53-58): roll back unless committed.
    pub async fn finish(&mut self) {
        if self.should_rollback {
            self.rollback().await;
        }
    }

    /// `Rollback` (operation.go:60-79): best-effort release of what this
    /// operation allocated.
    pub async fn rollback(&mut self) -> Vec<PortError> {
        if self.dry_run {
            return Vec::new();
        }
        let mut errors = Vec::new();
        for port in &self.allocated {
            if let Err(e) = self.pa.release(*port).await {
                errors.push(e);
            }
        }
        errors
    }

    /// `Commit` (operation.go:81-106): best-effort deferred releases. Even
    /// on error nothing is rolled back; a repair pass fixes it.
    pub async fn commit(&mut self) -> Vec<PortError> {
        if self.dry_run {
            return Vec::new();
        }
        let mut errors = Vec::new();
        for port in &self.release_deferred {
            if let Err(e) = self.pa.release(*port).await {
                errors.push(e);
            }
        }
        self.should_rollback = false;
        errors
    }

    /// `Allocate` (operation.go:108-127).
    pub async fn allocate(&mut self, port: usize) -> PortResult<()> {
        if self.dry_run {
            if self.pa.has(port).await || self.allocated.contains(&port) {
                return Err(PortError::Allocated);
            }
            self.allocated.push(port);
            return Ok(());
        }
        self.pa.allocate(port).await?;
        self.allocated.push(port);
        Ok(())
    }

    /// `AllocateNext` (operation.go:129-165). A dry run hands out a dummy
    /// port above the largest one it has allocated; upstream's search loop
    /// (`port < 100`) never runs, so neither does this one.
    pub async fn allocate_next(&mut self) -> PortResult<usize> {
        if self.dry_run {
            let last_port = if self.allocated.is_empty() {
                32768
            } else {
                *self.allocated.iter().max().expect("non-empty")
            };
            self.allocated.push(last_port + 1);
            return Ok(last_port + 1);
        }
        let port = self.pa.allocate_next().await?;
        self.allocated.push(port);
        Ok(port)
    }

    /// `ReleaseDeferred` (operation.go:167-170): release `port` on commit.
    pub fn release_deferred(&mut self, port: usize) {
        self.release_deferred.push(port);
    }
}
