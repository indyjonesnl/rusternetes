//! Ports of `nestedpendingoperations_test.go`.

use super::*;
use std::time::Duration;
use tokio::sync::oneshot;

const V: &str = "volume-name";

/// `generateWaitFunc` / `generateWaitWithErrorFunc`: block until signalled,
/// then succeed / fail.
fn wait_func(
    ok: bool,
) -> (
    oneshot::Sender<()>,
    impl Future<Output = Result<(), String>> + Send,
) {
    let (tx, rx) = oneshot::channel();
    (tx, async move {
        let _ = rx.await;
        if ok {
            Ok(())
        } else {
            Err("generateWaitWithErrorFunc error".to_string())
        }
    })
}

async fn noop() -> Result<(), String> {
    Ok(())
}

async fn err_func() -> Result<(), String> {
    Err("errorFunc".to_string())
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// `Test_NestedPendingOperations_Positive_SingleOp`
#[tokio::test]
async fn positive_single_op() {
    let grm = NestedPendingOperations::new(false);
    grm.run(V, EMPTY, EMPTY, "", noop()).unwrap();
    grm.wait().await;
}

/// `..._Positive_TwoOps`, `..._TwoSubOps`: different volumes / pods in parallel.
#[tokio::test]
async fn positive_two_ops_and_sub_ops() {
    let grm = NestedPendingOperations::new(false);
    let (t1, o1) = wait_func(true);
    let (t2, o2) = wait_func(true);
    grm.run("volume-1", EMPTY, EMPTY, "", o1).unwrap();
    grm.run("volume-2", EMPTY, EMPTY, "", o2).unwrap();
    let (t3, o3) = wait_func(true);
    let (t4, o4) = wait_func(true);
    grm.run("volume-3", "pod-1", EMPTY, "", o3).unwrap();
    grm.run("volume-3", "pod-2", EMPTY, "", o4).unwrap();
    for t in [t1, t2, t3, t4] {
        t.send(()).unwrap();
    }
    grm.wait().await;
}

/// `..._SecondOpAfterFirstCompletes` (with and without backoff).
#[tokio::test]
async fn positive_second_op_after_first_completes() {
    for backoff in [false, true] {
        let grm = NestedPendingOperations::new(backoff);
        let (t, o) = wait_func(true);
        grm.run(V, EMPTY, EMPTY, "", o).unwrap();
        t.send(()).unwrap();
        grm.wait().await;
        grm.run(V, EMPTY, EMPTY, "", noop()).unwrap();
        grm.wait().await;
    }
}

/// `..._SecondOpAfterFirstPanics` (with and without backoff): a panic is a
/// failed operation, not a stuck pending one.
#[tokio::test]
async fn second_op_after_first_panics() {
    let grm = NestedPendingOperations::new(false);
    grm.run(V, EMPTY, EMPTY, "", async { panic!("boom") })
        .unwrap();
    grm.wait().await;
    grm.run(V, EMPTY, EMPTY, "", noop()).unwrap();
    grm.wait().await;

    let grm = NestedPendingOperations::new(true);
    grm.run(V, EMPTY, EMPTY, "x", async { panic!("boom") })
        .unwrap();
    settle().await;
    assert!(!grm.is_operation_pending(V, EMPTY, EMPTY));
    let e = grm.run(V, EMPTY, EMPTY, "x", noop()).unwrap_err();
    assert!(e.is_exponential_backoff());
}

/// `..._Negative_SecondOpBeforeFirstCompletes`, `..._SecondSubOpBeforeFirstCompletes`,
/// `..._WithExpBackoff`.
#[tokio::test]
async fn negative_second_op_before_first_completes() {
    for (backoff, pod) in [(false, EMPTY), (false, "pod"), (true, EMPTY)] {
        let grm = NestedPendingOperations::new(backoff);
        let (t, o) = wait_func(true);
        grm.run(V, pod, EMPTY, "", o).unwrap();
        let e = grm.run(V, pod, EMPTY, "", noop()).unwrap_err();
        assert!(e.is_already_exists(), "{e}");
        assert!(grm.is_operation_pending(V, pod, EMPTY));
        assert!(!grm.is_operation_safe_to_retry(V, pod, EMPTY, ""));
        t.send(()).unwrap();
        grm.wait().await;
    }
}

/// `..._SecondThirdOpWithDifferentNames`: the same operation name is refused
/// during backoff; a different name resets the backoff and proceeds.
#[tokio::test]
async fn second_third_op_with_different_names() {
    let grm = NestedPendingOperations::new(true);
    grm.run(V, EMPTY, EMPTY, "mount_volume", err_func())
        .unwrap();
    settle().await;
    let e = grm
        .run(V, EMPTY, EMPTY, "mount_volume", err_func())
        .unwrap_err();
    assert!(e.is_exponential_backoff(), "{e}");
    assert!(!grm.is_operation_pending(V, EMPTY, EMPTY));
    assert!(!grm.is_operation_safe_to_retry(V, EMPTY, EMPTY, "mount_volume"));
    assert!(grm.is_operation_safe_to_retry(V, EMPTY, EMPTY, "unmount_volume"));
    grm.run(V, EMPTY, EMPTY, "unmount_volume", noop()).unwrap();
    grm.wait().await;
}

/// The backoff elapses (500ms first), then the same operation may retry.
#[tokio::test]
async fn retry_allowed_after_backoff() {
    let grm = NestedPendingOperations::new(true);
    grm.run(V, EMPTY, EMPTY, "op", err_func()).unwrap();
    settle().await;
    assert!(grm.run(V, EMPTY, EMPTY, "op", noop()).is_err());
    tokio::time::sleep(Duration::from_millis(550)).await;
    assert!(grm.is_operation_safe_to_retry(V, EMPTY, EMPTY, "op"));
    grm.run(V, EMPTY, EMPTY, "op", noop()).unwrap();
    grm.wait().await;
}

/// Without backoff a failure forgets the entry.
#[tokio::test]
async fn failure_without_backoff_is_forgotten() {
    let grm = NestedPendingOperations::new(false);
    grm.run(V, EMPTY, EMPTY, "op", err_func()).unwrap();
    grm.wait().await;
    grm.run(V, EMPTY, EMPTY, "op", noop()).unwrap();
    grm.wait().await;
}

/// `..._WaitEmpty`
#[tokio::test]
async fn wait_empty() {
    NestedPendingOperations::new(false).wait().await;
    NestedPendingOperations::new(true).wait().await;
}

/// `..._Positive_Wait`: wait returns only once the operation is done.
#[tokio::test]
async fn wait_blocks_until_done() {
    let grm = NestedPendingOperations::new(true);
    let (t, o) = wait_func(true);
    grm.run(V, EMPTY, EMPTY, "", o).unwrap();
    let g = grm.clone();
    let waiter = tokio::spawn(async move { g.wait().await });
    settle().await;
    assert!(!waiter.is_finished());
    t.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .unwrap()
        .unwrap();
}

/// `..._OperationExists_PendingFirst`: a pending sibling pod blocks an
/// empty-pod-name (whole volume) operation even though others failed.
#[tokio::test]
async fn operation_exists_pending_first() {
    let grm = NestedPendingOperations::new(true);
    let (t1, o1) = wait_func(false);
    let (t2, o2) = wait_func(false);
    let (t3, o3) = wait_func(true);
    grm.run(V, "pod1", EMPTY, "umount", o1).unwrap();
    grm.run(V, "pod2", EMPTY, "umount", o2).unwrap();
    grm.run(V, "pod3", EMPTY, "umount", o3).unwrap();
    t1.send(()).unwrap();
    t2.send(()).unwrap();
    settle().await;
    let e = grm.run(V, EMPTY, EMPTY, "mount", noop()).unwrap_err();
    assert!(e.is_already_exists(), "{e}");
    t3.send(()).unwrap();
}

/// `..._OperationExists_ExactMatchFirstNoPending`: with nothing pending, the
/// exact match is the entry a retry replaces, and its success removes it.
#[tokio::test]
async fn operation_exists_exact_match_first_no_pending() {
    let grm = NestedPendingOperations::new(true);
    for pod in ["pod1", "pod2", "pod3"] {
        grm.run(V, pod, EMPTY, "umount", err_func()).unwrap();
    }
    settle().await;
    // An empty-pod-name op matches all three; none is pending, the first is
    // in backoff under a different name, so the backoff resets and it runs.
    let (t4, o4) = wait_func(false);
    grm.run(V, EMPTY, EMPTY, "mount", o4).unwrap();
    t4.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(550)).await;
    // pod2's retry matches its own entry exactly and succeeds.
    grm.run(V, "pod2", EMPTY, "umount", noop()).unwrap();
    settle().await;
    let inner = grm.inner.lock().unwrap();
    assert!(
        !inner.operations.iter().any(|op| op.key.pod_name == "pod2"),
        "pod2's successful retry must remove its entry"
    );
}

/// An empty volume name never matches anything.
#[tokio::test]
async fn empty_volume_name_runs_concurrently() {
    let grm = NestedPendingOperations::new(true);
    let (t, o) = wait_func(true);
    grm.run(EMPTY, EMPTY, EMPTY, "", o).unwrap();
    grm.run(EMPTY, EMPTY, EMPTY, "", noop()).unwrap();
    assert!(!grm.is_operation_pending(EMPTY, EMPTY, EMPTY));
    t.send(()).unwrap();
}

/// `run_inline` shares the bookkeeping, and dropping it mid-operation parks
/// the entry in backoff instead of leaving it pending forever.
#[tokio::test]
async fn run_inline_bookkeeping_and_cancellation() {
    let grm = NestedPendingOperations::new(true);
    let r = grm.run_inline(V, EMPTY, EMPTY, "op", noop()).await.unwrap();
    assert!(r.is_ok());
    assert!(!grm.is_operation_pending(V, EMPTY, EMPTY));

    let fut = grm.run_inline(V, EMPTY, EMPTY, "op", std::future::pending());
    let _ = tokio::time::timeout(Duration::from_millis(20), fut).await;
    assert!(!grm.is_operation_pending(V, EMPTY, EMPTY));
    let e = grm
        .run_inline(V, EMPTY, EMPTY, "op", noop())
        .await
        .unwrap_err();
    assert!(e.is_exponential_backoff());
}
