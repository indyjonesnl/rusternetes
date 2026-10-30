//! Unit tests moved from `src/size_estimator.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_storage::ResourceStats;
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::Mutex;
}

mod tests {
    use super::__parent::*;
    use rusternetes_storage::size_estimator::*;

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
