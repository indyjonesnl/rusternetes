//! Unit tests moved from `src/controllers/priorityclass.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_common::resources::{Pod, PriorityClass};
    pub use rusternetes_storage::{build_key, build_prefix, Storage};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::info;
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::priorityclass::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_reconcile_all_is_noop_when_empty() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PriorityClassController::new(storage);
        // Must succeed without doing anything when there is no data.
        controller.reconcile_all().await.unwrap();
    }
}
