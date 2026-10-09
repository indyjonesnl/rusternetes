//! Port of `staleEndpointsTracker`
//! (`pkg/controller/endpoint/endpoints_tracker.go:28-70`).
//!
//! Remembers, per Endpoints, the resource version the controller has already
//! replaced, so a later sync that is handed the same (out-of-date) version
//! back can be rejected instead of recomputed from stale data.

use std::collections::HashMap;
use std::sync::RwLock;

/// `types.NamespacedName`.
type NamespacedName = (String, String);

/// `staleEndpointsTracker` (`:28-34`).
#[derive(Default)]
pub struct StaleEndpointsTracker {
    /// `staleResourceVersionByEndpoints`, behind `lock`.
    stale_resource_version_by_endpoints: RwLock<HashMap<NamespacedName, String>>,
}

impl StaleEndpointsTracker {
    /// `newStaleEndpointsTracker` (`:36-40`).
    pub fn new() -> Self {
        Self::default()
    }

    /// `Stale` (`:42-47`): record `resource_version` as stale for the Endpoints.
    pub fn stale(&self, namespace: &str, name: &str, resource_version: &str) {
        self.stale_resource_version_by_endpoints
            .write()
            .unwrap()
            .insert(
                (namespace.to_string(), name.to_string()),
                resource_version.to_string(),
            );
    }

    /// `IsStale` (`:49-60`).
    pub fn is_stale(&self, namespace: &str, name: &str, resource_version: &str) -> bool {
        self.stale_resource_version_by_endpoints
            .read()
            .unwrap()
            .get(&(namespace.to_string(), name.to_string()))
            .is_some_and(|stale| stale == resource_version)
    }

    /// `Delete` (`:62-67`).
    pub fn delete(&self, namespace: &str, name: &str) {
        self.stale_resource_version_by_endpoints
            .write()
            .unwrap()
            .remove(&(namespace.to_string(), name.to_string()));
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.stale_resource_version_by_endpoints
            .read()
            .unwrap()
            .is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
