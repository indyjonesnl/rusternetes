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

use std::collections::{HashMap, HashSet};
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
    /// Pod keys a controller is waiting to see deleted — upstream's
    /// `UIDTrackingControllerExpectations.uidStore`
    /// (`controller_utils.go:343-409`).
    uids: Mutex<HashMap<String, HashSet<String>>>,
    /// Rusternetes-only: pod keys a controller created and has not yet seen
    /// arrive. The creation-side twin of `uids`, so a watch `Added` event and
    /// a fresh listing each lower the creation count at most once per pod
    /// (upstream's `addPod` lowers it unconditionally per event).
    created: Mutex<HashMap<String, HashSet<String>>>,
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
            uids: Mutex::new(HashMap::new()),
            created: Mutex::new(HashMap::new()),
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
        self.uids.lock().unwrap().remove(key);
        self.created.lock().unwrap().remove(key);
    }

    /// Rusternetes-only: remember that `pod_key` was created for `key` and its
    /// arrival is still awaited. Call after the matching `expect_creations`.
    pub fn record_created(&self, key: &str, pod_key: &str) {
        self.created
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_default()
            .insert(pod_key.to_string());
    }

    /// Rusternetes-only: lower the creation count for `key`, but only if
    /// `pod_key` is still awaited, so an `Added` event and a fresh listing of
    /// the same pod never both count it.
    pub fn creation_observed_of(&self, key: &str, pod_key: &str) {
        let hit = self
            .created
            .lock()
            .unwrap()
            .get_mut(key)
            .is_some_and(|set| set.remove(pod_key));
        if hit {
            self.creation_observed(key);
        }
    }

    /// Rusternetes-only: created pod keys for `key` not yet observed.
    pub fn pending_creations(&self, key: &str) -> Vec<String> {
        self.created
            .lock()
            .unwrap()
            .get(key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Rusternetes-only: forget the awaited-creation set for `key` (call when
    /// a fresh `expect_creations` replaces the record).
    pub fn clear_created(&self, key: &str) {
        self.created.lock().unwrap().remove(key);
    }

    /// Upstream `UIDTrackingControllerExpectations.ExpectDeletions`
    /// (`controller_utils.go:361-377`): expect exactly these controllee keys
    /// to be seen deleted, so each is counted once whether it is observed as a
    /// deletionTimestamp update or as the final delete.
    pub fn expect_deletions_of(&self, key: &str, deleted_keys: &[String]) {
        let set: HashSet<String> = deleted_keys.iter().cloned().collect();
        let n = set.len() as i64;
        self.uids.lock().unwrap().insert(key.to_string(), set);
        self.expect_deletions(key, n);
    }

    /// Upstream `UIDTrackingControllerExpectations.DeletionObserved`
    /// (`controller_utils.go:380-390`): lowers the count only if `deleted_key`
    /// is still expected, so the MODIFIED (deletionTimestamp) and DELETED
    /// events of one pod are not double counted.
    pub fn deletion_observed_of(&self, key: &str, deleted_key: &str) {
        let hit = {
            let mut uids = self.uids.lock().unwrap();
            uids.get_mut(key).is_some_and(|set| set.remove(deleted_key))
        };
        if hit {
            self.deletion_observed(key);
        }
    }

    /// Controllee keys still expected to be seen deleted for `key`.
    ///
    /// Rusternetes-only (no upstream equivalent): lets a controller that can
    /// list its controllees fresh from storage settle an expectation from
    /// that listing when a pod is already gone or terminating, instead of
    /// waiting for the watch event (see the DaemonSet controller).
    pub fn pending_deletions(&self, key: &str) -> Vec<String> {
        self.uids
            .lock()
            .unwrap()
            .get(key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// The outstanding `(add, del)` counts, or `None` if nothing is recorded.
    /// Upstream `GetExpectations` (`controller_utils.go:172-179`), which is part
    /// of its public interface; here only the tests inspect the counts, so it
    /// is test-only rather than shipped dead.
    pub fn get_expectations(&self, key: &str) -> Option<(i64, i64)> {
        self.entries
            .lock()
            .unwrap()
            .get(key)
            .map(|exp| (exp.add, exp.del))
    }
}
