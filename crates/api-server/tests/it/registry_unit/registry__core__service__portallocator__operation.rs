//! Unit tests moved from `src/registry/core/service/portallocator/operation.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::core::service::portallocator::{
        PortAllocator, PortError, PortResult,
    };
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::service::portallocator::operation::*;
    use rusternetes_api_server::registry::core::service::portallocator::PortRange;

    async fn allocator() -> Arc<PortAllocator> {
        let r = PortAllocator::new_in_memory(PortRange::parse("10000-10200").unwrap());
        for port in [10000, 10010, 10020] {
            r.allocate(port).await.unwrap();
        }
        Arc::new(r)
    }

    /// `TestDryRunAllocate` (operation_test.go:26-69).
    #[tokio::test]
    async fn dry_run_allocate_checks_but_takes_nothing() {
        let r = allocator().await;
        let free_at_start = r.free().await;
        let mut op = PortAllocationOperation::start(r.clone(), true);
        assert!(matches!(
            op.allocate(10000).await,
            Err(PortError::Allocated)
        ));
        op.allocate(10030).await.unwrap();
        assert!(matches!(
            op.allocate(10030).await,
            Err(PortError::Allocated)
        ));
        op.allocate(10040).await.unwrap();
        assert_eq!(r.free().await, free_at_start);
    }

    /// `TestDryRunAllocateNext` (operation_test.go:71-126).
    #[tokio::test]
    async fn dry_run_allocate_next_hands_out_a_dummy() {
        let r = allocator().await;
        let free_at_start = r.free().await;
        let mut op = PortAllocationOperation::start(r.clone(), true);
        let port = op.allocate_next().await.unwrap();
        assert_ne!(port, 0);
        assert!(matches!(op.allocate(port).await, Err(PortError::Allocated)));

        let mut op = PortAllocationOperation::start(r.clone(), true);
        op.allocate(12345).await.unwrap();
        let port = op.allocate_next().await.unwrap();
        assert_ne!(port, 0);
        assert_ne!(port, 12345);
        assert_eq!(r.free().await, free_at_start);
    }

    /// A finish without a commit rolls the allocations back; a commit keeps
    /// them and performs the deferred releases.
    #[tokio::test]
    async fn finish_rolls_back_unless_committed() {
        let r = allocator().await;
        let mut op = PortAllocationOperation::start(r.clone(), false);
        op.allocate(10050).await.unwrap();
        op.release_deferred(10000);
        op.finish().await;
        assert!(!r.has(10050).await);
        assert!(r.has(10000).await);

        let mut op = PortAllocationOperation::start(r.clone(), false);
        op.allocate(10050).await.unwrap();
        op.release_deferred(10000);
        op.commit().await;
        op.finish().await;
        assert!(r.has(10050).await);
        assert!(!r.has(10000).await);
    }
}
