use futures::stream::BoxStream;
use rusternetes_client::reflector::{ListWatch, Reflector, StoreEvent, WatchItem};
use rusternetes_client::watch::WatchEvent;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq)]
struct Obj {
    name: String,
    v: u64,
}

/// One scripted watch session: a sequence of (event, observed rv) the mock
/// yields one at a time, mirroring the real streaming `ListWatch::watch`.
type WatchSession = Vec<(WatchEvent<Obj>, Option<String>)>;

struct MockLw {
    // each watch() call pops the next scripted session and streams it
    batches: Mutex<Vec<WatchSession>>,
    list_result: (Vec<Obj>, String),
    watch_calls: Mutex<Vec<Option<String>>>, // recorded resourceVersions
}

#[async_trait::async_trait]
impl ListWatch<Obj> for MockLw {
    async fn list(&self) -> anyhow::Result<(Vec<Obj>, String)> {
        Ok(self.list_result.clone())
    }
    async fn watch<'a>(
        &'a self,
        rv: Option<String>,
    ) -> anyhow::Result<BoxStream<'a, WatchItem<Obj>>> {
        self.watch_calls.lock().unwrap().push(rv);
        let session = self.batches.lock().unwrap().remove(0);
        let items: Vec<WatchItem<Obj>> = session.into_iter().map(Ok).collect();
        Ok(Box::pin(futures::stream::iter(items)))
    }
}

fn key(o: &Obj) -> String {
    o.name.clone()
}

/// Mock whose `list` reads a shared, test-mutable source and whose `watch`
/// ALWAYS fails to establish (mirrors the real reqwest "error sending request"
/// seen when a CPU-starved co-located client can't sustain the watch to the
/// api-server). Records how many times `list` is called.
struct FailingWatchLw {
    source: Arc<Mutex<(Vec<Obj>, String)>>,
    list_calls: Arc<Mutex<usize>>,
}

#[async_trait::async_trait]
impl ListWatch<Obj> for FailingWatchLw {
    async fn list(&self) -> anyhow::Result<(Vec<Obj>, String)> {
        *self.list_calls.lock().unwrap() += 1;
        Ok(self.source.lock().unwrap().clone())
    }
    async fn watch<'a>(
        &'a self,
        _rv: Option<String>,
    ) -> anyhow::Result<BoxStream<'a, WatchItem<Obj>>> {
        // Watch never establishes — the reflector must recover by re-listing.
        anyhow::bail!("Failed to send GET request: error sending request")
    }
}

/// When the watch keeps failing to establish, the reflector's `run` loop must
/// keep RE-LISTING so objects created after the initial list still land in the
/// store. Regression test for the M2d 4-node scheduler stall: the reflector
/// used to keep `last_rv` on a (non-Expired) watch error, so `sync_once`
/// skipped the list and only re-watched — freezing the store at the startup
/// list, and the API-mode scheduler (which reads the reflector store) never
/// saw newly-created pods. Upstream client-go re-lists whenever ListAndWatch
/// returns an error.
#[tokio::test(start_paused = true)]
async fn relists_when_watch_keeps_failing_so_new_objects_surface() {
    let source = Arc::new(Mutex::new((
        vec![Obj {
            name: "a".into(),
            v: 1,
        }],
        "10".into(),
    )));
    let list_calls = Arc::new(Mutex::new(0usize));
    let lw = Arc::new(FailingWatchLw {
        source: source.clone(),
        list_calls: list_calls.clone(),
    });
    let r = Arc::new(Reflector::new(lw, key));

    let r_run = Arc::clone(&r);
    let handle = tokio::spawn(async move { r_run.run().await });

    // Let the initial list + first (failing) watch happen.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(
        r.store().get("a").is_some(),
        "initial list must populate the store"
    );

    // A new object appears AFTER the initial list (as the DNS/PHP pods did,
    // created after the scheduler started). Only a re-list can surface it,
    // because the watch never delivers events.
    {
        let mut src = source.lock().unwrap();
        src.0.push(Obj {
            name: "b".into(),
            v: 1,
        });
        src.1 = "11".into();
    }

    // Give the run loop time to cycle through its backoff and re-list.
    tokio::time::sleep(std::time::Duration::from_secs(90)).await;

    assert!(
        r.store().get("b").is_some(),
        "reflector must re-list after a failing watch so post-list objects appear \
         (regression: store frozen at initial list -> scheduler never sees new pods)"
    );
    assert!(
        *list_calls.lock().unwrap() > 1,
        "watch kept failing, so the reflector must have re-listed more than once \
         (got {} list calls)",
        *list_calls.lock().unwrap()
    );

    handle.abort();
}

#[tokio::test]
async fn initial_list_populates_store_and_watch_resumes_from_list_rv() {
    let lw = Arc::new(MockLw {
        batches: Mutex::new(vec![vec![]]),
        list_result: (
            vec![Obj {
                name: "a".into(),
                v: 1,
            }],
            "10".into(),
        ),
        watch_calls: Mutex::new(vec![]),
    });
    let r = Reflector::new(lw.clone(), key);
    r.sync_once().await.unwrap(); // one list + one (empty) watch session
    assert_eq!(r.store().get("a").unwrap().v, 1);
    // watch must have been started from the list's resourceVersion
    assert_eq!(
        lw.watch_calls.lock().unwrap().as_slice(),
        &[Some("10".to_string())]
    );
}

#[tokio::test]
async fn watch_events_mutate_store_and_emit() {
    let lw = Arc::new(MockLw {
        batches: Mutex::new(vec![vec![
            (
                WatchEvent::Added(Obj {
                    name: "b".into(),
                    v: 1,
                }),
                Some("2".into()),
            ),
            (
                WatchEvent::Modified(Obj {
                    name: "b".into(),
                    v: 2,
                }),
                Some("3".into()),
            ),
            (
                WatchEvent::Deleted(Obj {
                    name: "b".into(),
                    v: 2,
                }),
                Some("4".into()),
            ),
        ]]),
        list_result: (vec![], "1".into()),
        watch_calls: Mutex::new(vec![]),
    });
    let r = Reflector::new(lw, key);
    let mut events = r.subscribe();
    r.sync_once().await.unwrap();
    assert!(r.store().get("b").is_none()); // added, modified, then deleted
    assert!(matches!(events.try_recv().unwrap(), StoreEvent::Added(_)));
    assert!(matches!(
        events.try_recv().unwrap(),
        StoreEvent::Modified(_)
    ));
    assert!(matches!(events.try_recv().unwrap(), StoreEvent::Deleted(_)));
}

#[tokio::test]
async fn bookmark_advances_rv_without_store_change() {
    let lw = Arc::new(MockLw {
        batches: Mutex::new(vec![
            // first watch session: only a bookmark carrying rv 20
            vec![(
                WatchEvent::Bookmark(Obj {
                    name: "ignored".into(),
                    v: 0,
                }),
                Some("20".to_string()),
            )],
            // second watch session: nothing
            vec![],
        ]),
        list_result: (
            vec![Obj {
                name: "a".into(),
                v: 1,
            }],
            "10".into(),
        ),
        watch_calls: Mutex::new(vec![]),
    });
    let r = Reflector::new(lw.clone(), key);
    let mut events = r.subscribe();
    r.sync_once().await.unwrap();
    r.sync_once().await.unwrap();
    // second watch resumed from the bookmark-advanced rv
    assert_eq!(
        lw.watch_calls.lock().unwrap().as_slice(),
        &[Some("10".to_string()), Some("20".to_string())]
    );
    // bookmark neither mutates the store nor emits a StoreEvent
    assert_eq!(r.store().get("a").unwrap().v, 1);
    assert!(r.store().get("ignored").is_none());
    assert!(events.try_recv().is_err());
}
