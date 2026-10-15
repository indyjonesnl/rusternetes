//! Mirror of the one `Watchers` conformance case that had no mirror at all
//! (#1770).
//!
//! Upstream (`release-1.35`): `test/e2e/apimachinery/watch.go:334` — "should
//! receive events on concurrent watches in same order" (Testname
//! `watch-consistency`).
//!
//! The upstream body opens a new watch at the resourceVersion of the last
//! event the *first* watch delivered, 100 times over, while a background
//! goroutine mutates configmaps every 5ms (`produceConfigMapEvents`,
//! :465-522). After each new watch is added, every watch must deliver its next
//! event with the same resourceVersion — so watches started at different
//! points all see one total order.
//!
//! Each watch is a `watchtools.NewRetryWatcher`
//! (`staging/src/k8s.io/client-go/tools/watch/retrywatcher.go`), which the
//! mirror ports as `retry_watcher`: watch with `allowWatchBookmarks=true` from
//! the last seen resourceVersion, forward ADDED/MODIFIED/DELETED, swallow
//! BOOKMARK but advance the resourceVersion (:196-224), and re-open from that
//! resourceVersion when the stream closes. Everything goes over HTTP to a
//! served `TestApiServer`, the way `client-go` reaches a real api-server.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use rusternetes_client::http::ApiClient;
use rusternetes_client::watch::{watch_stream, WatchEvent};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch as stop_signal};

/// `waitForNextConfigMapEvent` (:443-457) budget.
const NEXT_EVENT_TIMEOUT: Duration = Duration::from_secs(10);

struct Served {
    api: TestApiServer,
    client: Arc<ApiClient>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve() -> Served {
    let api = TestApiServer::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test api-server");
    let address = listener.local_addr().expect("test api-server address");
    let server = tokio::spawn({
        let router = api.router.clone();
        async move {
            let _ = axum::serve(listener, router).await;
        }
    });
    // `f.ClientSet`: one clientset, one limiter, `ClientQPS: 20,
    // ClientBurst: 50` (test/e2e/framework/framework.go:182-183, :224-225).
    let client = Arc::new(
        ApiClient::new(&format!("http://{address}"), true, None)
            .expect("build api-server client")
            .with_rate_limit(20.0, 50.0),
    );
    Served {
        api,
        client,
        server,
    }
}

/// `watchtools.NewRetryWatcher(resourceVersion, listWatcher)`: a watch that
/// survives the server closing its stream by re-opening from the last
/// resourceVersion it observed. Returns the receiving end of its result
/// channel; an `Err` is a terminal error (upstream forwards a 410 and stops,
/// :246-248).
fn retry_watcher(
    client: Arc<ApiClient>,
    path: String,
    initial_rv: String,
) -> mpsc::UnboundedReceiver<Result<(String, Value), String>> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut last_rv = initial_rv;
        loop {
            let watch_path = format!("{path}?allowWatchBookmarks=true");
            let stream = match watch_stream::<Value>(&client, &watch_path, Some(&last_rv)).await {
                Ok(s) => s,
                Err(_) => {
                    // doReceive retries a failed establish (:119-141).
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    if tx.is_closed() {
                        return;
                    }
                    continue;
                }
            };
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let (kind, obj) = match item {
                    Ok(WatchEvent::Added(o)) => ("ADDED", o),
                    Ok(WatchEvent::Modified(o)) => ("MODIFIED", o),
                    Ok(WatchEvent::Deleted(o)) => ("DELETED", o),
                    Ok(WatchEvent::Bookmark(o)) => ("BOOKMARK", o),
                    Err(e) => {
                        let _ = tx.send(Err(format!("watch from rv {last_rv}: {e}")));
                        return;
                    }
                };
                let Some(rv) = obj["metadata"]["resourceVersion"]
                    .as_str()
                    .filter(|rv| !rv.is_empty())
                    .map(str::to_string)
                else {
                    // :208-216 — an event without a resourceVersion aborts.
                    let _ = tx.send(Err(format!("{kind} event without resourceVersion: {obj}")));
                    return;
                };
                if kind != "BOOKMARK" && tx.send(Ok((kind.to_string(), obj))).is_err() {
                    return;
                }
                last_rv = rv;
            }
            if tx.is_closed() {
                return;
            }
        }
    });
    rx
}

/// `waitForNextConfigMapEvent` (:443-457): the next event, which must carry a
/// ConfigMap; returns that ConfigMap's resourceVersion.
async fn wait_for_next_config_map_event(
    rx: &mut mpsc::UnboundedReceiver<Result<(String, Value), String>>,
) -> String {
    let event = tokio::time::timeout(NEXT_EVENT_TIMEOUT, rx.recv())
        .await
        .expect("timed out waiting for watch event")
        .expect("Watch closed unexpectedly")
        .unwrap_or_else(|e| panic!("watch failed: {e}"));
    let (kind, obj) = event;
    assert_eq!(
        obj["kind"], "ConfigMap",
        "expected config map, got {kind} {obj}"
    );
    obj["metadata"]["resourceVersion"]
        .as_str()
        .expect("configmap resourceVersion")
        .to_string()
}

/// `produceConfigMapEvents` (:465-522): every `min_wait` pick create / update /
/// delete at random (create when none exist) until `stop` fires. Upstream's
/// `rand.Intn` becomes a fixed-seed xorshift so a failure reproduces.
///
/// Two pieces of upstream pacing are kept, because without them the producer
/// can outrun the watch cache's replay ring on a loaded runner, and the next
/// watch the spec opens then gets a 410:
/// - Writes go through `f.ClientSet`, whose rate limiter is
///   `ClientQPS: 20, ClientBurst: 50` (`test/e2e/framework/framework.go:182-183`,
///   applied at :224-225). The 5ms ticker is the floor and the limiter is
///   the real pace.
/// - `time.NewTicker` drops ticks for a slow receiver instead of firing the
///   missed ones back to back, which is `MissedTickBehavior::Skip`. tokio's
///   default (`Burst`) would replay every missed tick at once after a stall.
async fn produce_config_map_events(
    client: Arc<ApiClient>,
    ns: String,
    mut stop: stop_signal::Receiver<bool>,
    min_wait: Duration,
) {
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut rand = move |n: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % n as u64) as usize
    };
    let name = |i: usize| format!("cm-{i}");
    let collection = format!("/api/v1/namespaces/{ns}/configmaps");
    let mut existing: Vec<usize> = Vec::new();
    // time.NewTicker: first tick after one interval, slow receivers drop ticks.
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + min_wait, min_wait);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut i = 0usize;
    let mut updates = 0usize;
    loop {
        ticker.tick().await;
        let mut op = rand(3);
        if existing.is_empty() {
            op = 0;
        }
        match op {
            0 => {
                let _: Value = client
                    .post(
                        &collection,
                        &json!({
                            "apiVersion": "v1", "kind": "ConfigMap",
                            "metadata": { "name": name(i) }
                        }),
                    )
                    .await
                    .unwrap_or_else(|e| panic!("Failed to create configmap {}: {e}", name(i)));
                existing.push(i);
                i += 1;
            }
            1 => {
                let idx = rand(existing.len());
                let n = name(existing[idx]);
                // An Update with no resourceVersion: ConfigMap allows an
                // unconditional update (`AllowUnconditionalUpdate`).
                let _: Value = client
                    .put(
                        &format!("{collection}/{n}"),
                        &json!({
                            "apiVersion": "v1", "kind": "ConfigMap",
                            "metadata": {
                                "name": n,
                                "labels": { "mutated": updates.to_string() }
                            }
                        }),
                    )
                    .await
                    .unwrap_or_else(|e| panic!("Failed to update configmap {n}: {e}"));
                updates += 1;
            }
            _ => {
                let idx = rand(existing.len());
                let n = name(existing[idx]);
                let s = client
                    .delete_with_options(&format!("{collection}/{n}"), &[], None)
                    .await
                    .unwrap_or_else(|e| panic!("Failed to delete configmap {n}: {e}"));
                assert_eq!(s.as_u16(), 200, "Failed to delete configmap {n}: {s}");
                existing.remove(idx);
            }
        }
        if *stop.borrow_and_update() {
            return;
        }
    }
}

/// watch.go:334 — should receive events on concurrent watches in same order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_watches_receive_events_in_same_order() {
    let served = serve().await;
    let ns = "watch-5551";
    let collection = format!("/api/v1/namespaces/{ns}/configmaps");
    // f.Namespace: the framework creates the test namespace up front.
    let (s, b) = served
        .api
        .post(
            "/api/v1/namespaces",
            &json!({ "apiVersion": "v1", "kind": "Namespace", "metadata": { "name": ns } }),
        )
        .await;
    assert_eq!(s.as_u16(), 201, "create namespace: {b}");

    let iterations = 100;

    // "getting a starting resourceVersion" (:340-343).
    let (s, list) = served.api.get(&collection).await;
    assert_eq!(s.as_u16(), 200, "list configmaps: {list}");
    let mut resource_version = list["metadata"]["resourceVersion"]
        .as_str()
        .expect("list resourceVersion")
        .to_string();

    // "starting a background goroutine to produce watch events" (:345-352).
    let (stop_tx, stop_rx) = stop_signal::channel(false);
    let producer = tokio::spawn(produce_config_map_events(
        served.client.clone(),
        ns.to_string(),
        stop_rx,
        Duration::from_millis(5),
    ));

    // "creating watches starting from each resource version of the events
    // produced and verifying they all receive resource versions in the same
    // order" (:360-372).
    let mut wcs = Vec::new();
    for _ in 0..iterations {
        wcs.push(retry_watcher(
            served.client.clone(),
            collection.clone(),
            resource_version.clone(),
        ));
        resource_version = wait_for_next_config_map_event(&mut wcs[0]).await;
        for (n, wc) in wcs.iter_mut().enumerate().skip(1) {
            let rv = wait_for_next_config_map_event(wc).await;
            assert_eq!(
                resource_version, rv,
                "resource version mismatch on watch {n}, expected {resource_version} but got {rv}"
            );
        }
    }
    // close(stopc); wc.Stop(); <-donec (:374-378).
    stop_tx.send(true).expect("stop producer");
    drop(wcs);
    producer.await.expect("producer finished cleanly");
}
