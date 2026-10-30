//! Unit tests moved from `src/controllers/limitrange.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_common::resources::LimitRange;
    pub use rusternetes_storage::{build_prefix, extract_key, Storage, WorkQueue};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{debug, error, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::limitrange::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_reconcile_all_is_noop_on_empty_storage() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = LimitRangeController::new(storage);
        // Stub MUST return Ok even with nothing in storage.
        controller.reconcile_all().await.unwrap();
    }
}
