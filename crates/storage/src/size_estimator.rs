//! Per-key object size cache behind `Storage::stats`.
//!
//! Port of `storage/etcd3/stats.go` `resourceSizeEstimator`
//! (k8s.io/apiserver, release-1.35): the average object size is estimated
//! from the last observed size of individual keys, so `Stats` itself needs
//! only a keys-only read (`getKeys`, `Stats` :79-94) and never re-reads or
//! re-encodes the objects. A per-key revision keeps the newer state when
//! updates arrive out of order (`updateKey` :171-182); keys whose delete was
//! never observed are dropped against the live key list (`cleanKeys`
//! :127-147).
//!
//! Deviation: upstream keeps one estimator per resource prefix; this backend
//! shares one map across prefixes, so `cleanKeys` is scoped to the prefix
//! being measured.

use crate::ResourceStats;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// `sizeRevision` (stats.go:74-77).
#[derive(Clone, Copy)]
struct SizeRevision {
    size_bytes: i64,
    revision: i64,
}

#[derive(Default)]
pub struct SizeEstimator {
    keys: Mutex<HashMap<String, SizeRevision>>,
}

impl SizeEstimator {
    /// `updateKey` (stats.go:171-182): only a newer revision replaces a size.
    pub fn update_key(&self, key: &str, size_bytes: usize, revision: i64) {
        let mut keys = self.keys.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cur) = keys.get(key) {
            if cur.revision >= revision {
                return;
            }
        }
        keys.insert(
            key.to_string(),
            SizeRevision {
                size_bytes: size_bytes as i64,
                revision,
            },
        );
    }

    /// `DeleteKey` (stats.go:184-195).
    pub fn delete_key(&self, key: &str, revision: i64) {
        let mut keys = self.keys.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(cur) = keys.get(key) {
            if cur.revision >= revision {
                return;
            }
        }
        keys.remove(key);
    }

    /// `Stats` (stats.go:79-94) given the keys-only read of `prefix`:
    /// `ObjectCount` is the key count; the size is the mean over the cached
    /// keys that are still live (0 when none is cached).
    pub fn stats(&self, prefix: &str, live_keys: &[String]) -> ResourceStats {
        let live: HashSet<&str> = live_keys.iter().map(String::as_str).collect();
        let mut keys = self.keys.lock().unwrap_or_else(|e| e.into_inner());
        // cleanKeys, scoped to this prefix.
        keys.retain(|k, _| !k.starts_with(prefix) || live.contains(k.as_str()));
        let (total, cached) = keys
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .fold((0i64, 0i64), |(t, n), (_, v)| (t + v.size_bytes, n + 1));
        ResourceStats {
            object_count: live_keys.len() as i64,
            estimated_average_object_size_bytes: if cached == 0 { 0 } else { total / cached },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(ks: &[&str]) -> Vec<String> {
        ks.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn count_is_the_live_keys_and_size_the_mean_of_the_cached_ones() {
        let e = SizeEstimator::default();
        e.update_key("/registry/pods/ns/a", 100, 1);
        e.update_key("/registry/pods/ns/b", 300, 2);
        let st = e.stats(
            "/registry/pods/",
            &keys(&[
                "/registry/pods/ns/a",
                "/registry/pods/ns/b",
                "/registry/pods/ns/c",
            ]),
        );
        assert_eq!(st.object_count, 3, "uncached live keys still count");
        assert_eq!(
            st.estimated_average_object_size_bytes, 200,
            "mean over cached"
        );
    }

    #[test]
    fn nothing_cached_means_a_zero_size() {
        let e = SizeEstimator::default();
        let st = e.stats("/registry/pods/", &keys(&["/registry/pods/ns/a"]));
        assert_eq!(st.object_count, 1);
        assert_eq!(st.estimated_average_object_size_bytes, 0);
    }

    #[test]
    fn a_key_that_is_no_longer_live_is_cleaned() {
        let e = SizeEstimator::default();
        e.update_key("/registry/pods/ns/a", 100, 1);
        e.update_key("/registry/pods/ns/gone", 900, 2);
        let st = e.stats("/registry/pods/", &keys(&["/registry/pods/ns/a"]));
        assert_eq!(st.estimated_average_object_size_bytes, 100);
    }

    #[test]
    fn cleaning_one_prefix_leaves_other_prefixes_alone() {
        let e = SizeEstimator::default();
        e.update_key("/registry/services/ns/s", 50, 1);
        e.stats("/registry/pods/", &[]);
        let st = e.stats("/registry/services/", &keys(&["/registry/services/ns/s"]));
        assert_eq!(st.estimated_average_object_size_bytes, 50);
    }

    #[test]
    fn an_older_revision_never_overwrites_a_newer_size() {
        let e = SizeEstimator::default();
        e.update_key("/registry/pods/ns/a", 500, 5);
        e.update_key("/registry/pods/ns/a", 100, 3);
        let st = e.stats("/registry/pods/", &keys(&["/registry/pods/ns/a"]));
        assert_eq!(st.estimated_average_object_size_bytes, 500);
    }

    #[test]
    fn a_stale_delete_does_not_evict_a_newer_key() {
        let e = SizeEstimator::default();
        e.update_key("/registry/pods/ns/a", 500, 5);
        e.delete_key("/registry/pods/ns/a", 4);
        let st = e.stats("/registry/pods/", &keys(&["/registry/pods/ns/a"]));
        assert_eq!(st.estimated_average_object_size_bytes, 500);
        e.delete_key("/registry/pods/ns/a", 6);
        let st = e.stats("/registry/pods/", &keys(&["/registry/pods/ns/a"]));
        assert_eq!(st.estimated_average_object_size_bytes, 0);
    }
}
