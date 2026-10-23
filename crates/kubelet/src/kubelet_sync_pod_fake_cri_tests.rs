//! `Kubelet::sync_pod` concurrency against the in-process fake CRI
//! (issues #3884, #1115): the `pod_sync_locks` skip-and-retry path, driven
//! end-to-end through a real `Kubelet` rather than the bare `SyncLocks`.
//!
//! Upstream parity. The Go kubelet serialises per pod with `podWorkers`
//! (`pkg/kubelet/pod_workers.go`: `UpdatePod` / `podWorkerLoop`), coalescing
//! updates so a pod is never synced by two goroutines at once, while distinct
//! pods run in parallel. The cases below port the observable contracts of
//! `TestUpdatePodParallel` (`pod_workers_test.go:526`: distinct pods proceed
//! independently, each processed at least once) and the per-UID serialisation
//! of `TestUpdatePod`/`podWorkerLoop`. The constructor mirrors
//! `newTestKubelet` (`kubelet_test.go:174`): a whole kubelet wired to fake
//! runtime + storage, with no real container runtime.
//!
//! Deliberate deviation (documented on `Kubelet::pod_sync_locks`): rusternetes
//! keys a name-based skip-and-retry lock instead of upstream's per-UID worker
//! goroutines, so a second concurrent sync is *skipped* (returns `Ok`) and
//! retried by a later reconcile, rather than queued.

use super::Kubelet;
use rusternetes_common::resources::Pod;
use rusternetes_cri::fake::FakeCri;
use rusternetes_storage::{Storage, StorageBackend};
use std::sync::Arc;

/// Test constructor for a [`Kubelet`] on a fake CRI socket + memory storage
/// (upstream: `newTestKubelet`, `kubelet_test.go:174`). Callers must be
/// `#[serial_test::serial]`: the CRI endpoint is read from the process
/// environment by `new_with_eviction`, like the other constructor tests.
struct TestKubelet {
    kubelet: Arc<Kubelet>,
    fake: FakeCri,
    storage: Arc<StorageBackend>,
    _dir: tempfile::TempDir,
    _server: tokio::task::JoinHandle<()>,
}

async fn new_test_kubelet() -> TestKubelet {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("fake-cri.sock");
    let fake = FakeCri::new();
    let server = fake.serve(&sock);
    std::env::set_var(
        "CONTAINER_RUNTIME_ENDPOINT",
        format!("unix://{}", sock.display()),
    );
    let storage = Arc::new(StorageBackend::new_memory());
    let kubelet = Kubelet::new(
        "node-test".into(),
        storage.clone(),
        10,
        dir.path().join("vols").display().to_string(),
        "10.96.0.10".into(),
        "cluster.local".into(),
        "bridge".into(),
        String::new(),
    )
    .await
    .unwrap();
    TestKubelet {
        kubelet: Arc::new(kubelet),
        fake,
        storage,
        _dir: dir,
        _server: server,
    }
}

fn pod(ns: &str, name: &str, uid: &str) -> Pod {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": ns, "uid": uid},
        "spec": {
            "nodeName": "node-test",
            "containers": [{"name": "app", "image": "busybox"}]
        }
    }))
    .unwrap()
}

async fn store(t: &TestKubelet, p: &Pod) {
    let key = format!(
        "/registry/pods/{}/{}",
        p.metadata.namespace.as_deref().unwrap(),
        p.metadata.name
    );
    t.storage.create(&key, p).await.unwrap();
}

fn count(fake: &FakeCri, rpc: &str) -> usize {
    fake.called().iter().filter(|c| *c == rpc).count()
}

/// `sync_pod`'s future is large; in a debug build it overflows the default
/// 2 MiB test-thread stack. Run each case on a big-stack thread with its own
/// current-thread runtime (which also makes `join!` interleaving
/// deterministic: the second call is polled as soon as the first yields).
fn run<Fut: std::future::Future<Output = ()>>(f: impl FnOnce() -> Fut + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(256 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(f())
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
#[serial_test::serial]
fn lone_sync_pod_creates_one_sandbox() {
    // Baseline: the harness really drives sync_pod to the CRI.
    run(|| async {
        let t = new_test_kubelet().await;
        let p = pod("default", "web-0", "uid-1");
        store(&t, &p).await;
        t.kubelet.sync_pod(&p).await.unwrap();
        assert_eq!(count(&t.fake, "RunPodSandbox"), 1, "{:?}", t.fake.called());
    });
}

#[test]
#[serial_test::serial]
fn concurrent_same_name_sync_pod_skips_the_loser() {
    // Two syncs of the same pod race: the lock holder drives the CRI, the
    // other is skipped (Ok, no CRI traffic) instead of issuing a duplicate
    // RunPodSandbox/CreateContainer (the 409 "name already in use" class,
    // #1014/#1112). Upstream: podWorkers never runs one pod's sync twice at
    // once (`podWorkerLoop`, pod_workers.go).
    run(|| async {
        let t = new_test_kubelet().await;
        let p = pod("default", "web-0", "uid-1");
        store(&t, &p).await;
        let (a, b) = tokio::join!(t.kubelet.sync_pod(&p), t.kubelet.sync_pod(&p));
        a.unwrap();
        b.unwrap();
        assert_eq!(count(&t.fake, "RunPodSandbox"), 1, "{:?}", t.fake.called());
        assert_eq!(
            count(&t.fake, "CreateContainer"),
            1,
            "{:?}",
            t.fake.called()
        );
        assert_eq!(t.fake.sandboxes().len(), 1);
    });
}

#[test]
#[serial_test::serial]
fn skipped_sync_is_retried_after_the_holder_finishes() {
    // Skip-and-retry: once the guard drops, the next reconcile syncs again
    // and does not duplicate what the first sync created.
    run(|| async {
        let t = new_test_kubelet().await;
        let p = pod("default", "web-0", "uid-1");
        store(&t, &p).await;
        let (a, b) = tokio::join!(t.kubelet.sync_pod(&p), t.kubelet.sync_pod(&p));
        a.unwrap();
        b.unwrap();
        // The guard is released on every path, so the key is free again.
        assert!(
            t.kubelet
                .pod_sync_locks
                .try_acquire("default/web-0")
                .is_some(),
            "sync lock must be released after the concurrent syncs"
        );
        t.kubelet.sync_pod(&p).await.unwrap();
        assert_eq!(count(&t.fake, "RunPodSandbox"), 1, "{:?}", t.fake.called());
        assert_eq!(
            count(&t.fake, "CreateContainer"),
            1,
            "{:?}",
            t.fake.called()
        );
    });
}

#[test]
#[serial_test::serial]
fn distinct_pods_sync_independently() {
    // Upstream TestUpdatePodParallel (pod_workers_test.go:526): distinct pods
    // are never serialised against each other.
    run(|| async {
        let t = new_test_kubelet().await;
        let a = pod("default", "web-0", "uid-a");
        let b = pod("default", "web-1", "uid-b");
        let c = pod("other", "web-0", "uid-c"); // same name, other namespace
        for p in [&a, &b, &c] {
            store(&t, p).await;
        }
        let (ra, rb, rc) = tokio::join!(
            t.kubelet.sync_pod(&a),
            t.kubelet.sync_pod(&b),
            t.kubelet.sync_pod(&c)
        );
        ra.unwrap();
        rb.unwrap();
        rc.unwrap();
        assert_eq!(count(&t.fake, "RunPodSandbox"), 3, "{:?}", t.fake.called());
    });
}

#[test]
#[serial_test::serial]
fn same_name_new_uid_is_serialised_against_the_old_incarnation() {
    // #1112: a StatefulSet replacement (same name, new UID) must not create
    // containers while the old incarnation's sync is running. The lock is
    // name-keyed, so the second incarnation is skipped, not interleaved.
    run(|| async {
        let t = new_test_kubelet().await;
        let old = pod("default", "web-0", "uid-old");
        let new = pod("default", "web-0", "uid-new");
        store(&t, &old).await;
        let (a, b) = tokio::join!(t.kubelet.sync_pod(&old), t.kubelet.sync_pod(&new));
        a.unwrap();
        b.unwrap();
        assert_eq!(count(&t.fake, "RunPodSandbox"), 1, "{:?}", t.fake.called());
        let sandboxes = t.fake.sandboxes();
        assert_eq!(sandboxes.len(), 1);
    });
}

#[test]
#[serial_test::serial]
fn concurrent_same_name_sync_writes_one_consistent_running_status() {
    // Status race (#1115): with the loser skipped, the stored pod carries the
    // single winner's status (phase Running, the sandbox's IP) rather than a clobbered/duplicated one.
    run(|| async {
        let t = new_test_kubelet().await;
        let p = pod("default", "web-0", "uid-1");
        store(&t, &p).await;
        let (a, b) = tokio::join!(t.kubelet.sync_pod(&p), t.kubelet.sync_pod(&p));
        a.unwrap();
        b.unwrap();
        let stored: Pod = t.storage.get("/registry/pods/default/web-0").await.unwrap();
        let status = stored.status.expect("kubelet wrote a status");
        assert_eq!(
            status.phase,
            Some(rusternetes_common::types::Phase::Running)
        );
        assert_eq!(status.pod_ip.as_deref(), Some("192.168.192.168"));
    });
}
