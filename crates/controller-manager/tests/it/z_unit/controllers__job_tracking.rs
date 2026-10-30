//! Unit tests moved from `src/controllers/job_tracking.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::workloads::UncountedTerminatedPods;
    pub use rusternetes_common::resources::Pod;
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::Mutex;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_controller_manager::controllers::job_tracking::*;

    fn pod_with(finalizers: Option<Vec<String>>) -> Pod {
        Pod {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta {
                name: "p".to_string(),
                finalizers,
                ..Default::default()
            },
            spec: None,
            status: None,
        }
    }

    fn uids(list: &[&str]) -> HashSet<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn detects_the_tracking_finalizer() {
        assert!(!has_job_tracking_finalizer(&pod_with(None)));
        assert!(!has_job_tracking_finalizer(&pod_with(Some(vec![
            "other/fin".to_string()
        ]))));
        assert!(has_job_tracking_finalizer(&pod_with(Some(vec![
            JOB_TRACKING_FINALIZER.to_string()
        ]))));
    }

    #[test]
    fn removes_only_the_tracking_finalizer() {
        let mut pod = pod_with(Some(vec![
            "keep/me".to_string(),
            JOB_TRACKING_FINALIZER.to_string(),
        ]));
        assert!(remove_tracking_finalizer(&mut pod));
        assert_eq!(
            pod.metadata.finalizers,
            Some(vec!["keep/me".to_string()]),
            "unrelated finalizers must survive"
        );
        assert!(
            !remove_tracking_finalizer(&mut pod),
            "removing twice is a no-op"
        );
    }

    #[test]
    fn clears_the_list_when_the_tracking_finalizer_was_the_only_one() {
        let mut pod = pod_with(Some(vec![JOB_TRACKING_FINALIZER.to_string()]));
        assert!(remove_tracking_finalizer(&mut pod));
        assert_eq!(
            pod.metadata.finalizers, None,
            "an empty finalizer list serializes as absent, not []"
        );
    }

    /// The core of exactly-once: a UID still holding its finalizer stays
    /// parked; one whose finalizer is gone is folded into the counter.
    #[test]
    fn folds_only_uids_whose_finalizer_is_gone() {
        let mut uncounted = UncountedTerminatedPods {
            succeeded: Some(vec!["s-gone".to_string(), "s-held".to_string()]),
            failed: Some(vec!["f-gone".to_string()]),
        };
        let mut succeeded = Some(1);
        let mut failed = Some(2);

        let changed = clean_uncounted_pods_without_finalizers(
            &mut succeeded,
            &mut failed,
            &mut uncounted,
            &uids(&["s-held"]),
        );

        assert!(changed);
        assert_eq!(succeeded, Some(2), "s-gone folded into the counter");
        assert_eq!(failed, Some(3), "f-gone folded into the counter");
        assert_eq!(uncounted.succeeded, Some(vec!["s-held".to_string()]));
        assert_eq!(uncounted.failed, Some(vec![]));
    }

    #[test]
    fn folding_is_idempotent_and_never_decreases() {
        let mut uncounted = UncountedTerminatedPods {
            succeeded: Some(vec!["a".to_string()]),
            failed: None,
        };
        let mut succeeded = Some(0);
        let mut failed = Some(0);

        assert!(clean_uncounted_pods_without_finalizers(
            &mut succeeded,
            &mut failed,
            &mut uncounted,
            &uids(&[]),
        ));
        assert_eq!(succeeded, Some(1));

        // A second pass has nothing left to fold, so the counter holds.
        assert!(!clean_uncounted_pods_without_finalizers(
            &mut succeeded,
            &mut failed,
            &mut uncounted,
            &uids(&[]),
        ));
        assert_eq!(succeeded, Some(1), "a pod is counted exactly once");
    }

    #[test]
    fn expectations_park_and_release_uids() {
        let exp = FinalizerExpectations::new();
        assert!(exp.expected("ns/j").is_empty());

        exp.expect_removed("ns/j", ["u1".to_string(), "u2".to_string()]);
        assert_eq!(exp.expected("ns/j"), uids(&["u1", "u2"]));

        exp.removal_observed("ns/j", "u1");
        assert_eq!(exp.expected("ns/j"), uids(&["u2"]));

        exp.removal_observed("ns/j", "u2");
        assert!(
            exp.expected("ns/j").is_empty(),
            "the entry is dropped once drained"
        );
    }

    #[test]
    fn expectations_are_scoped_per_job() {
        let exp = FinalizerExpectations::new();
        exp.expect_removed("ns/a", ["u1".to_string()]);
        exp.expect_removed("ns/b", ["u2".to_string()]);

        exp.forget("ns/a");
        assert!(exp.expected("ns/a").is_empty());
        assert_eq!(
            exp.expected("ns/b"),
            uids(&["u2"]),
            "forgetting one job must not touch another"
        );
    }
}
