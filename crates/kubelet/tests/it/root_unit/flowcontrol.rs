//! Ports of `flowcontrol.Backoff` semantics
//! (`staging/src/k8s.io/client-go/util/flowcontrol/backoff_test.go`).
use rusternetes_kubelet::flowcontrol::Backoff;
use std::time::{Duration, Instant};

const S: Duration = Duration::from_secs(1);

#[test]
fn next_doubles_and_caps_at_max() {
    let b = Backoff::new(S, 8 * S);
    let t = Instant::now();
    for want in [1, 2, 4, 8, 8, 8] {
        b.next("k", t);
        assert_eq!(b.get("k"), want * S);
    }
}

#[test]
fn is_in_back_off_since_update_window() {
    let b = Backoff::new(10 * S, 300 * S);
    let t = Instant::now();
    assert!(!b.is_in_back_off_since_update("k", t), "no entry");
    b.next("k", t);
    assert!(b.is_in_back_off_since_update("k", t + 9 * S));
    assert!(!b.is_in_back_off_since_update("k", t + 10 * S));
    // Keys are independent.
    assert!(!b.is_in_back_off_since_update("other", t));
}

#[test]
fn expired_entry_restarts_at_the_default() {
    let b = Backoff::new(S, 4 * S);
    let t = Instant::now();
    for _ in 0..4 {
        b.next("k", t);
    }
    assert_eq!(b.get("k"), 4 * S);
    // Stable for more than 2*max: treated as new.
    assert!(!b.is_in_back_off_since_update("k", t + 9 * S));
    b.next("k", t + 9 * S);
    assert_eq!(b.get("k"), S);
}

#[test]
fn gc_drops_only_expired_entries() {
    let b = Backoff::new(S, 4 * S);
    let t = Instant::now();
    b.next("old", t);
    b.next("new", t + 8 * S);
    b.gc(t + 9 * S);
    assert_eq!(b.len(), 1);
    assert_eq!(b.get("old"), Duration::ZERO);
    assert_eq!(b.get("new"), S);
}

#[test]
fn reset_forgets_the_key() {
    let b = Backoff::new(S, 4 * S);
    b.next("k", Instant::now());
    b.reset("k");
    assert!(b.is_empty());
}
