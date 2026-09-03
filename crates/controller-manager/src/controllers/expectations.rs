//! Controller expectations — the brake that makes batched controllee creation
//! safe.
//!
//! Ported from upstream's `ControllerExpectations`
//! (`pkg/controller/controller_utils.go:150-300`). A controller records how
//! many creates or deletes it is about to issue, and then does nothing further
//! for that object until it has *observed* those controllees arrive (or the
//! record expires). Without it, a watch-driven controller re-enters its sync
//! while its own creates are still in flight, sees a list that does not contain
//! them yet, and issues them again.
//!
//! Upstream is explicit that this is what makes slow-start batching safe:
//! `syncReplicaSet` reads `SatisfiedExpectations` and only then calls
//! `manageReplicas` (`pkg/controller/replicaset/replica_set.go:728, 756`).
//!
//! One deliberate deviation: upstream hard-codes `clock.RealClock{}` and
//! carries a `TODO: Support injection of clock`
//! (`controller_utils.go:222-227`). [`ControllerExpectations::with_timeout`]
//! takes the timeout so the expiry rule can be tested in milliseconds rather
//! than by waiting five minutes.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tracing::debug;

/// How long an unfulfilled expectation stands before a sync proceeds anyway.
///
/// Upstream's `ExpectationsTimeout` (`controller_utils.go:72`). It is a
/// liveness guard: if the watch event that would fulfil an expectation is
/// never delivered, the controller must not wedge forever.
pub const EXPECTATIONS_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Outstanding creates and deletes for one controller.
///
/// Mirrors upstream's `ControlleeExpectations`
/// (`controller_utils.go:272-278`). The counters are signed and may go
/// negative — upstream lowers them without a floor and defines fulfilment as
/// `add <= 0 && del <= 0` (`Fulfilled`, `:288-292`), so an extra observation
/// (a pod the controller did not create, say) can only ever make a sync more
/// eager, never less.
#[derive(Debug, Clone)]
struct ControlleeExpectations {
    add: i64,
    del: i64,
    timestamp: Instant,
}

impl ControlleeExpectations {
    /// Upstream `Fulfilled` (`controller_utils.go:288-292`).
    fn fulfilled(&self) -> bool {
        self.add <= 0 && self.del <= 0
    }

    /// Upstream `isExpired` (`controller_utils.go:225-227`).
    fn is_expired(&self, timeout: Duration) -> bool {
        self.timestamp.elapsed() > timeout
    }
}

/// A cache of what each controller expects to observe before syncing again.
#[derive(Debug)]
pub struct ControllerExpectations {
    entries: Mutex<HashMap<String, ControlleeExpectations>>,
    timeout: Duration,
}

impl Default for ControllerExpectations {
    fn default() -> Self {
        Self::new()
    }
}

impl ControllerExpectations {
    pub fn new() -> Self {
        Self::with_timeout(EXPECTATIONS_TIMEOUT)
    }

    /// Construct with a custom expiry, for tests. See the module comment on why
    /// this deviates from upstream.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            timeout,
        }
    }

    /// Whether the required creates/deletes for `key` have been observed.
    ///
    /// Upstream `SatisfiedExpectations` (`controller_utils.go:195-219`):
    /// fulfilled, expired, or never recorded all mean "go ahead". Only a live,
    /// unfulfilled record holds the sync back.
    ///
    /// **Read this before listing controllees.** Upstream's
    /// `TestRSSyncExpectations` pins the ordering, and says why: if the list is
    /// taken first and expectations checked second, a controllee arriving in
    /// between makes the record look fulfilled while the list still lacks the
    /// object — and the controller creates a duplicate.
    pub fn satisfied(&self, key: &str) -> bool {
        let entries = self.entries.lock().unwrap();
        match entries.get(key) {
            Some(exp) if exp.fulfilled() => true,
            Some(exp) if exp.is_expired(self.timeout) => {
                debug!("Controller expectations expired for {key}, forcing sync");
                true
            }
            Some(_) => false,
            // Never recorded, or already cleared: nothing to wait for.
            None => true,
        }
    }

    /// Replace `key`'s expectations and restart its clock.
    ///
    /// Upstream `SetExpectations` (`controller_utils.go:230-234`) — note it
    /// *forgets* any existing record rather than accumulating.
    pub fn set_expectations(&self, key: &str, add: i64, del: i64) {
        let mut entries = self.entries.lock().unwrap();
        entries.insert(
            key.to_string(),
            ControlleeExpectations {
                add,
                del,
                timestamp: Instant::now(),
            },
        );
    }

    /// Upstream `ExpectCreations` (`controller_utils.go:236-238`).
    pub fn expect_creations(&self, key: &str, adds: i64) {
        self.set_expectations(key, adds, 0);
    }

    /// Upstream `ExpectDeletions` (`controller_utils.go:240-242`).
    pub fn expect_deletions(&self, key: &str, dels: i64) {
        self.set_expectations(key, 0, dels);
    }

    /// Upstream `LowerExpectations` (`controller_utils.go:245-251`). A missing
    /// record is a no-op, exactly as upstream's `if exists` guard makes it.
    pub fn lower_expectations(&self, key: &str, add: i64, del: i64) {
        let mut entries = self.entries.lock().unwrap();
        if let Some(exp) = entries.get_mut(key) {
            exp.add -= add;
            exp.del -= del;
        }
    }

    /// Upstream `CreationObserved` (`controller_utils.go:262-265`).
    pub fn creation_observed(&self, key: &str) {
        self.lower_expectations(key, 1, 0);
    }

    /// Upstream `DeletionObserved` (`controller_utils.go:267-270`).
    pub fn deletion_observed(&self, key: &str) {
        self.lower_expectations(key, 0, 1);
    }

    /// Upstream `DeleteExpectations` (`controller_utils.go:181-188`).
    ///
    /// Called when the controller object itself goes away, so a later
    /// controller created with the same name does not inherit a stale record —
    /// the case upstream's `TestExpectationsOnRecreate` covers.
    pub fn delete_expectations(&self, key: &str) {
        self.entries.lock().unwrap().remove(key);
    }

    /// The outstanding `(add, del)` counts, or `None` if nothing is recorded.
    /// Upstream `GetExpectations` (`controller_utils.go:172-179`), which is part
    /// of its public interface; here only the tests inspect the counts, so it
    /// is test-only rather than shipped dead.
    #[cfg(test)]
    pub fn get_expectations(&self, key: &str) -> Option<(i64, i64)> {
        self.entries
            .lock()
            .unwrap()
            .get(key)
            .map(|exp| (exp.add, exp.del))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
