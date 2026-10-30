//! Unit tests moved from `src/controllers/expectations.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::Mutex;
    pub use std::time::{Duration, Instant};
    pub use tracing::debug;
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::expectations::*;

    const KEY: &str = "default/simpletest-rc";

    /// A controller that has recorded nothing may always sync. Upstream returns
    /// true for the "never recorded expectations" branch
    /// (`controller_utils.go:210-216`).
    #[test]
    fn no_record_means_satisfied() {
        let exp = ControllerExpectations::new();
        assert!(exp.satisfied(KEY));
        assert_eq!(exp.get_expectations(KEY), None);
    }

    /// Outstanding creates hold the next sync back — this is the whole point.
    #[test]
    fn outstanding_creates_block_until_observed() {
        let exp = ControllerExpectations::new();
        exp.expect_creations(KEY, 3);
        assert!(!exp.satisfied(KEY), "3 creates outstanding");
        assert_eq!(exp.get_expectations(KEY), Some((3, 0)));

        exp.creation_observed(KEY);
        exp.creation_observed(KEY);
        assert!(!exp.satisfied(KEY), "1 create still outstanding");

        exp.creation_observed(KEY);
        assert!(exp.satisfied(KEY), "all creates observed");
    }

    /// Deletes work the same way and are tracked separately from creates.
    #[test]
    fn outstanding_deletes_block_until_observed() {
        let exp = ControllerExpectations::new();
        exp.expect_deletions(KEY, 2);
        assert_eq!(exp.get_expectations(KEY), Some((0, 2)));
        assert!(!exp.satisfied(KEY));

        exp.deletion_observed(KEY);
        assert!(!exp.satisfied(KEY));
        exp.deletion_observed(KEY);
        assert!(exp.satisfied(KEY));
    }

    /// Counters go negative rather than clamping at zero, and fulfilment is
    /// `<= 0` (upstream `Fulfilled`, `controller_utils.go:288-292`).
    ///
    /// This matters: observing a controllee the controller did not create — an
    /// adopted pod, a duplicate watch event — must not leave the record
    /// permanently unsatisfiable. Clamping at zero would be the intuitive
    /// choice and would be wrong in the other direction only; going negative is
    /// what upstream does and is strictly the safer bias, since it can only
    /// make the next sync more eager.
    #[test]
    fn observations_beyond_the_expectation_go_negative_and_stay_satisfied() {
        let exp = ControllerExpectations::new();
        exp.expect_creations(KEY, 1);
        exp.creation_observed(KEY);
        exp.creation_observed(KEY);
        exp.creation_observed(KEY);

        assert_eq!(exp.get_expectations(KEY), Some((-2, 0)));
        assert!(exp.satisfied(KEY));
    }

    /// An expectation that is never fulfilled must not wedge the controller
    /// forever. Upstream treats an expired record as satisfied
    /// (`controller_utils.go:200-202`) so a dropped watch event costs a delay,
    /// not a permanently stuck object.
    #[test]
    fn an_unfulfilled_expectation_expires_and_stops_blocking() {
        let exp = ControllerExpectations::with_timeout(Duration::from_millis(20));
        exp.expect_creations(KEY, 5);
        assert!(!exp.satisfied(KEY), "blocks while fresh");

        std::thread::sleep(Duration::from_millis(40));

        assert!(exp.satisfied(KEY), "expired records stop blocking");
        // Still recorded — expiry is not deletion.
        assert_eq!(exp.get_expectations(KEY), Some((5, 0)));
    }

    /// Setting expectations replaces the record and restarts its clock, rather
    /// than accumulating (upstream `SetExpectations` calls `Add`, replacing).
    #[test]
    fn setting_expectations_replaces_rather_than_accumulates() {
        let exp = ControllerExpectations::new();
        exp.expect_creations(KEY, 5);
        exp.expect_creations(KEY, 2);
        assert_eq!(exp.get_expectations(KEY), Some((2, 0)));
    }

    /// Deleting the controller clears its record, so a controller later created
    /// with the same name starts clean. Upstream's `TestExpectationsOnRecreate`
    /// covers exactly this; without it a recreated object inherits a stale
    /// unfulfilled record and refuses to sync until the TTL expires.
    #[test]
    fn deleting_a_controller_clears_expectations_for_a_recreated_one() {
        let exp = ControllerExpectations::new();
        exp.expect_creations(KEY, 4);
        assert!(!exp.satisfied(KEY));

        exp.delete_expectations(KEY);

        assert_eq!(exp.get_expectations(KEY), None);
        assert!(
            exp.satisfied(KEY),
            "a recreated controller must not inherit a stale record"
        );
    }

    /// Observations against a controller with no record are a no-op, not a
    /// panic and not an implicit record. Upstream's
    /// `TestDeleteControllerAndExpectations` exercises this: a concurrent
    /// pod-add lands after the controller was deleted and must have no effect.
    #[test]
    fn observing_after_deletion_has_no_effect() {
        let exp = ControllerExpectations::new();
        exp.expect_creations(KEY, 1);
        exp.delete_expectations(KEY);

        exp.creation_observed(KEY);
        exp.deletion_observed(KEY);

        assert_eq!(exp.get_expectations(KEY), None);
        assert!(exp.satisfied(KEY));
    }

    /// Upstream `UIDTrackingControllerExpectations` (`controller_utils.go:343`):
    /// a pod seen with a deletionTimestamp and then deleted counts once, and
    /// an unexpected pod's deletion does not lower the count.
    #[test]
    fn keyed_deletions_are_counted_once_per_pod() {
        let exp = ControllerExpectations::new();
        exp.expect_deletions_of(KEY, &["ns/a".to_string(), "ns/b".to_string()]);
        assert_eq!(exp.get_expectations(KEY), Some((0, 2)));

        exp.deletion_observed_of(KEY, "ns/a");
        exp.deletion_observed_of(KEY, "ns/a"); // the later DELETED event
        exp.deletion_observed_of(KEY, "ns/other");
        assert_eq!(exp.get_expectations(KEY), Some((0, 1)));
        assert!(!exp.satisfied(KEY));

        exp.deletion_observed_of(KEY, "ns/b");
        assert!(exp.satisfied(KEY));
    }

    /// A created pod lowers the creation count once, however many times it is
    /// observed (event and fresh listing both see it).
    #[test]
    fn creation_observed_of_counts_each_pod_once() {
        let exp = ControllerExpectations::new();
        exp.expect_creations(KEY, 2);
        exp.record_created(KEY, "ns/a");
        exp.record_created(KEY, "ns/b");
        exp.creation_observed_of(KEY, "ns/a");
        exp.creation_observed_of(KEY, "ns/a");
        exp.creation_observed_of(KEY, "ns/other");
        assert_eq!(exp.get_expectations(KEY), Some((1, 0)));
        assert_eq!(exp.pending_creations(KEY), vec!["ns/b".to_string()]);
        exp.creation_observed_of(KEY, "ns/b");
        assert!(exp.satisfied(KEY));
    }

    /// Expectations are per-controller; one object's outstanding creates must
    /// not gate another's.
    #[test]
    fn expectations_are_keyed_per_controller() {
        let exp = ControllerExpectations::new();
        exp.expect_creations("ns-a/rc-1", 1);

        assert!(!exp.satisfied("ns-a/rc-1"));
        assert!(exp.satisfied("ns-a/rc-2"));
        assert!(exp.satisfied("ns-b/rc-1"));
    }
}
