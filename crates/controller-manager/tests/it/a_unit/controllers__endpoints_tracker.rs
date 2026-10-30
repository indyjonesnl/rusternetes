//! Unit tests moved from `src/controllers/endpoints_tracker.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::HashMap;
    pub use std::sync::RwLock;
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::endpoints_tracker::*;

    /// Port of `TestStaleEndpointsTracker` (`endpoints_tracker_test.go:30-60`).
    #[test]
    fn stale_endpoints_tracker() {
        let tracker = StaleEndpointsTracker::new();
        assert!(
            !tracker.is_stale("default", "foo", "1"),
            "is_stale should return false before the endpoint is staled"
        );
        tracker.stale("default", "foo", "1");
        assert!(
            tracker.is_stale("default", "foo", "1"),
            "is_stale should return true after the endpoint is staled"
        );
        assert!(
            !tracker.is_stale("default", "foo", "2"),
            "is_stale should return false after the endpoint is updated"
        );
        tracker.delete("default", "foo");
        assert!(tracker.is_empty());
    }
}
