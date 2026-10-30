//! Unit tests moved from `src/audit_webhook.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::authorization_webhook::WebhookClientConfig;
    pub use rusternetes_common::audit::{AuditBackend, AuditEvent};
    pub use rusternetes_common::wait::Backoff;
    pub use serde::Serialize;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::sync::{mpsc, watch};
    pub use tokio::task::JoinSet;
    pub use tokio::time::Instant;
    pub use tracing::{error, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::audit_webhook::*;
    use rusternetes_common::audit::{AuditLevel, AuditStage, UserInfo};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    fn event() -> AuditEvent {
        AuditEvent {
            api_version: "audit.k8s.io/v1".into(),
            kind: "Event".into(),
            level: AuditLevel::Metadata,
            audit_id: uuid::Uuid::new_v4().to_string(),
            stage: AuditStage::ResponseComplete,
            request_uri: "/api".into(),
            verb: "get".into(),
            user: UserInfo {
                username: "u".into(),
                uid: String::new(),
                groups: vec![],
                extra: None,
            },
            user_agent: None,
            source_ips: vec![],
            object_ref: None,
            response_status: None,
            response_object: None,
            request_received_timestamp: chrono::Utc::now(),
            stage_timestamp: chrono::Utc::now(),
            annotations: None,
        }
    }

    fn tempdir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("audit-webhook-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn test_config() -> BatchConfig {
        // testBatchConfig (buffered_test.go:46-54)
        BatchConfig {
            buffer_size: 100,
            max_batch_size: 10,
            max_batch_wait: Duration::from_secs(30),
            throttle_enable: false,
            throttle_qps: 0.0,
            throttle_burst: 0,
            async_delegate: true,
        }
    }

    #[derive(Default)]
    struct Recorder {
        batches: Mutex<Vec<usize>>,
        calls: AtomicUsize,
        gate: Option<Arc<tokio::sync::Notify>>,
    }

    #[async_trait]
    impl BatchBackend for Recorder {
        async fn process_events(&self, events: Vec<AuditEvent>) -> bool {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(g) = &self.gate {
                g.notified().await;
            }
            self.batches.lock().unwrap().push(events.len());
            true
        }
        fn name(&self) -> String {
            "fake".into()
        }
    }

    /// A worker over a fresh buffer, to drive `collect_events` directly
    /// (the tests of buffered_test.go call it on the bufferedBackend).
    fn worker(cfg: &BatchConfig) -> (mpsc::Sender<AuditEvent>, Worker) {
        let (tx, rx) = mpsc::channel(cfg.buffer_size);
        (
            tx,
            Worker {
                delegate: Arc::new(Recorder::default()),
                rx,
                max_batch_size: cfg.max_batch_size,
                max_batch_wait: cfg.max_batch_wait,
                async_delegate: cfg.async_delegate,
                throttle: None,
                inflight: JoinSet::new(),
            },
        )
    }

    /// TestBatchedBackendCollectEvents (buffered_test.go:56-102).
    #[tokio::test(start_paused = true)]
    async fn batched_collect_events() {
        let cfg = test_config();
        let (tx, mut w) = worker(&cfg);
        let (stop_tx, mut stop) = watch::channel(false);

        // Max batch size encountered.
        for _ in 0..cfg.max_batch_size + 1 {
            tx.try_send(event()).unwrap();
        }
        assert_eq!(w.collect_events(true, &mut stop).await.len(), 10);

        // A partial batch waits until the timer expires.
        tx.try_send(event()).unwrap();
        assert_eq!(w.collect_events(true, &mut stop).await.len(), 2);

        // Collected events are delivered when the stop signal is given.
        for _ in 0..3 {
            tx.try_send(event()).unwrap();
        }
        let collect = w.collect_events(true, &mut stop);
        tokio::pin!(collect);
        tokio::select! {
            _ = &mut collect => panic!("returned before the timer or stop"),
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
        stop_tx.send(true).unwrap();
        assert_eq!(collect.await.len(), 3);
    }

    /// TestUnbatchedBackendCollectEvents (buffered_test.go:104-131).
    #[tokio::test(start_paused = true)]
    async fn unbatched_collect_events() {
        let mut cfg = test_config();
        cfg.max_batch_size = 1;
        let (tx, mut w) = worker(&cfg);
        let (stop_tx, mut stop) = watch::channel(false);
        for _ in 0..3 {
            tx.try_send(event()).unwrap();
        }
        for _ in 0..3 {
            assert_eq!(w.collect_events(false, &mut stop).await.len(), 1);
        }
        // Collection halts when the stop signal is given.
        stop_tx.send(true).unwrap();
        assert!(w.collect_events(false, &mut stop).await.is_empty());
    }

    /// TestBufferedBackendProcessEventsAfterStop (buffered_test.go:133-146).
    #[tokio::test]
    async fn process_events_after_stop() {
        let b = BufferedBackend::new(Arc::new(Recorder::default()), test_config());
        b.shutdown().await;
        // Still answers true, drops the event.
        assert!(b.log(event()).await.is_ok());
    }

    /// TestBufferedBackendProcessEventsBufferFull (buffered_test.go:148-158).
    #[tokio::test]
    async fn process_events_buffer_full() {
        let (tx, w) = worker(&{
            let mut c = test_config();
            c.buffer_size = 1;
            c
        });
        tx.try_send(event()).unwrap();
        assert!(tx.try_send(event()).is_err());
        drop(w);
    }

    /// A full buffer drops the event and `log` still answers Ok
    /// (`ProcessEvents` returns true).
    #[tokio::test]
    async fn log_with_full_buffer_is_ok() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let rec = Arc::new(Recorder {
            gate: Some(gate.clone()),
            ..Default::default()
        });
        let mut cfg = test_config();
        cfg.buffer_size = 1;
        cfg.max_batch_size = 1;
        cfg.async_delegate = false;
        let b = BufferedBackend::new(rec.clone(), cfg);
        for _ in 0..5 {
            assert!(b.log(event()).await.is_ok());
        }
        gate.notify_waiters();
        gate.notify_one();
        b.shutdown().await;
    }

    /// TestBufferedBackendShutdownWaitsForDelegatedCalls
    /// (buffered_test.go:160-200).
    #[tokio::test]
    async fn shutdown_waits_for_delegated_calls() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let rec = Arc::new(Recorder {
            gate: Some(gate.clone()),
            ..Default::default()
        });
        let cfg = test_config();
        let b = Arc::new(BufferedBackend::new(rec.clone(), cfg.clone()));
        for _ in 0..cfg.max_batch_size {
            b.log(event()).await.unwrap();
        }
        // Wait for the delegated call to start.
        while rec.calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let b2 = b.clone();
        let shutdown = tokio::spawn(async move { b2.shutdown().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !shutdown.is_finished(),
            "Shutdown exited before delegated call ended"
        );
        gate.notify_one();
        shutdown.await.unwrap();
        assert_eq!(*rec.batches.lock().unwrap(), vec![10]);
    }

    /// TestDelegateProcessEvents (buffered_test.go:202-222): full batches
    /// reach the delegate, async or not.
    #[tokio::test]
    async fn delegate_process_events() {
        for async_delegate in [true, false] {
            let rec = Arc::new(Recorder::default());
            let mut cfg = test_config();
            cfg.async_delegate = async_delegate;
            let b = BufferedBackend::new(rec.clone(), cfg.clone());
            for _ in 0..5 * cfg.max_batch_size {
                b.log(event()).await.unwrap();
            }
            b.shutdown().await;
            let batches = rec.batches.lock().unwrap().clone();
            assert_eq!(batches.iter().sum::<usize>(), 50);
            assert!(batches.iter().all(|n| *n <= cfg.max_batch_size));
        }
    }

    /// Events left in the buffer at shutdown are still sent
    /// (buffered.go:98-117).
    #[tokio::test]
    async fn shutdown_flushes_partial_batch() {
        let rec = Arc::new(Recorder::default());
        let b = BufferedBackend::new(rec.clone(), test_config());
        for _ in 0..3 {
            b.log(event()).await.unwrap();
        }
        b.shutdown().await;
        assert_eq!(rec.batches.lock().unwrap().iter().sum::<usize>(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn token_bucket_throttles() {
        let mut t = TokenBucket::new(10.0, 2);
        let start = Instant::now();
        for _ in 0..4 {
            t.accept().await;
        }
        // 2 from the burst, then 2 at 10 per second.
        let waited = start.elapsed();
        assert!(waited >= Duration::from_millis(190), "{waited:?}");
        assert!(waited < Duration::from_millis(400), "{waited:?}");
    }

    #[test]
    fn batch_options_validate() {
        let mut o = BatchOptions {
            mode: "batch".into(),
            batch_config: BatchConfig::default_webhook(),
        };
        assert!(o.validate("webhook").is_ok());
        o.mode = "nope".into();
        assert_eq!(
            o.validate("webhook").unwrap_err(),
            "invalid audit webhook mode nope, allowed modes are \"batch,blocking,blocking-strict\""
        );
        o.mode = "batch".into();
        o.batch_config.buffer_size = 0;
        assert!(o.validate("webhook").unwrap_err().contains("buffer size 0"));
        o.batch_config = BatchConfig::default_webhook();
        o.batch_config.max_batch_size = 0;
        assert!(o
            .validate("webhook")
            .unwrap_err()
            .contains("max batch size 0"));
        o.batch_config = BatchConfig::default_webhook();
        o.batch_config.throttle_qps = 0.0;
        assert!(o.validate("webhook").unwrap_err().contains("throttle QPS"));
        o.batch_config.throttle_qps = 1.0;
        o.batch_config.throttle_burst = 0;
        assert!(o
            .validate("webhook")
            .unwrap_err()
            .contains("throttle burst"));
        // Unused options are not validated outside batch mode.
        o.mode = "blocking".into();
        assert!(o.validate("webhook").is_ok());
    }

    // ---- the webhook itself (webhook_test.go TestWebhook) ----

    async fn serve(
        status: u16,
        fail_first: usize,
    ) -> (String, Arc<Mutex<Vec<serde_json::Value>>>, Arc<AtomicUsize>) {
        use axum::{extract::State, routing::post, Json, Router};
        #[derive(Clone)]
        struct S {
            got: Arc<Mutex<Vec<serde_json::Value>>>,
            n: Arc<AtomicUsize>,
            status: u16,
            fail_first: usize,
        }
        async fn h(State(s): State<S>, Json(v): Json<serde_json::Value>) -> axum::http::StatusCode {
            let n = s.n.fetch_add(1, Ordering::SeqCst);
            if n < s.fail_first {
                return axum::http::StatusCode::INTERNAL_SERVER_ERROR;
            }
            s.got.lock().unwrap().push(v);
            axum::http::StatusCode::from_u16(s.status).unwrap()
        }
        let s = S {
            got: Default::default(),
            n: Default::default(),
            status,
            fail_first,
        };
        let (got, n) = (s.got.clone(), s.n.clone());
        let app = Router::new().route("/", post(h)).with_state(s);
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        (format!("http://{addr}/"), got, n)
    }

    /// `newWebhook` (webhook_test.go:93-119): a kubeconfig with one cluster.
    fn kubeconfig(dir: &std::path::Path, server: &str) -> std::path::PathBuf {
        let p = dir.join("kubeconfig");
        std::fs::write(
            &p,
            format!(
                "apiVersion: v1\nkind: Config\nclusters:\n- name: \"\"\n  cluster:\n    server: {server}\n    insecure-skip-tls-verify: true\n"
            ),
        )
        .unwrap();
        p
    }

    fn fast_backoff() -> Backoff {
        Backoff {
            duration: Duration::from_millis(10),
            factor: 1.5,
            jitter: 0.2,
            steps: 5,
        }
    }

    #[tokio::test]
    async fn webhook_sends_event_list() {
        let (url, got, _) = serve(200, 0).await;
        let dir = tempdir();
        let b = WebhookBackend::new(&kubeconfig(&dir, &url), AUDIT_GROUP_VERSION, fast_backoff())
            .unwrap();
        b.try_process_events(&[event(), event()]).await.unwrap();
        let got = got.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0]["kind"], "EventList");
        assert_eq!(got[0]["apiVersion"], "audit.k8s.io/v1");
        assert_eq!(got[0]["items"].as_array().unwrap().len(), 2);
        assert_eq!(got[0]["items"][0]["kind"], "Event");
    }

    /// `retryOnError`: every failure is retried until it works.
    #[tokio::test]
    async fn webhook_retries_every_error() {
        let (url, got, n) = serve(200, 3).await;
        let dir = tempdir();
        let b = WebhookBackend::new(&kubeconfig(&dir, &url), AUDIT_GROUP_VERSION, fast_backoff())
            .unwrap();
        assert!(b.process_events(vec![event()]).await);
        assert_eq!(n.load(Ordering::SeqCst), 4);
        assert_eq!(got.lock().unwrap().len(), 1);
    }

    /// A 4xx is retried too (`retryOnError` is true for any error) and the
    /// batch is lost after `Steps` attempts.
    #[tokio::test]
    async fn webhook_gives_up_after_steps() {
        let (url, _, n) = serve(403, 0).await;
        let dir = tempdir();
        let b = WebhookBackend::new(&kubeconfig(&dir, &url), AUDIT_GROUP_VERSION, fast_backoff())
            .unwrap();
        assert!(!b.process_events(vec![event()]).await);
        assert_eq!(n.load(Ordering::SeqCst), 5);
        assert!(b.log(event()).await.is_err());
    }

    #[test]
    fn webhook_rejects_unregistered_version() {
        let dir = tempdir();
        let kc = kubeconfig(&dir, "http://127.0.0.1:1/");
        let e = WebhookBackend::new(&kc, "audit.k8s.io/v1beta1", fast_backoff())
            .err()
            .unwrap();
        assert!(e
            .to_string()
            .contains("requires enabling extension resource"));
    }

    /// Batch mode end to end: events buffered, delivered on shutdown.
    #[tokio::test]
    async fn options_build_batch_backend() {
        let (url, got, _) = serve(200, 0).await;
        let dir = tempdir();
        let kc = kubeconfig(&dir, &url);
        let mut cfg = BatchConfig::default_webhook();
        cfg.throttle_enable = false;
        let o = WebhookOptions {
            config_file: Some(kc.to_string_lossy().into()),
            initial_backoff: Duration::from_millis(10),
            group_version: AUDIT_GROUP_VERSION.into(),
            batch: BatchOptions {
                mode: MODE_BATCH.into(),
                batch_config: cfg,
            },
        };
        assert!(o.validate().is_empty());
        let (b, buffered) = o.new_backend().unwrap();
        for _ in 0..3 {
            b.log(event()).await.unwrap();
        }
        // The 30s max wait has not passed: nothing is sent yet.
        assert!(got.lock().unwrap().is_empty());
        buffered.unwrap().shutdown().await;
        let got = got.lock().unwrap();
        // Shutdown races the worker's collect loop (`select` over the buffer
        // and the stop channel, buffered.go `collectEvents`), so the three
        // events may leave as one batch or as a partial batch followed by the
        // drain; either way all three arrive.
        let delivered: usize = got
            .iter()
            .map(|b| b["items"].as_array().unwrap().len())
            .sum();
        assert_eq!(delivered, 3);
    }

    #[test]
    fn options_validate_group_version_and_disabled() {
        let mut o = WebhookOptions {
            config_file: None,
            initial_backoff: DEFAULT_INITIAL_BACKOFF_DELAY,
            group_version: "bogus".into(),
            batch: BatchOptions {
                mode: MODE_BATCH.into(),
                batch_config: BatchConfig::default_webhook(),
            },
        };
        // Not enabled: nothing is validated.
        assert!(o.validate().is_empty());
        o.config_file = Some("/x".into());
        assert_eq!(
            o.validate(),
            vec!["invalid group version, allowed versions are [\"audit.k8s.io/v1\"]".to_string()]
        );
    }

    #[tokio::test]
    async fn blocking_modes() {
        let (url, _, _) = serve(500, 0).await;
        let dir = tempdir();
        let kc = kubeconfig(&dir, &url);
        let mk = |mode: &str| {
            let o = WebhookOptions {
                config_file: Some(kc.to_string_lossy().into()),
                initial_backoff: Duration::from_millis(1),
                group_version: AUDIT_GROUP_VERSION.into(),
                batch: BatchOptions {
                    mode: mode.into(),
                    batch_config: BatchConfig::default_webhook(),
                },
            };
            o.new_backend().unwrap().0
        };
        // blocking: a failing webhook never fails the request.
        assert!(mk(MODE_BLOCKING).log(event()).await.is_ok());
        // blocking-strict: it does.
        assert!(mk(MODE_BLOCKING_STRICT).log(event()).await.is_err());
    }

    #[tokio::test]
    async fn union_reports_any_failure_but_feeds_all() {
        struct Fail;
        #[async_trait]
        impl AuditBackend for Fail {
            async fn log(&self, _: AuditEvent) -> Result<(), String> {
                Err("boom".into())
            }
            async fn flush(&self) -> Result<(), String> {
                Ok(())
            }
        }
        struct Count(AtomicUsize);
        #[async_trait]
        impl AuditBackend for Count {
            async fn log(&self, _: AuditEvent) -> Result<(), String> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn flush(&self) -> Result<(), String> {
                Ok(())
            }
        }
        let c = Arc::new(Count(AtomicUsize::new(0)));
        let u = UnionBackend(vec![Arc::new(Fail), c.clone()]);
        assert!(u.log(event()).await.is_err());
        assert_eq!(c.0.load(Ordering::SeqCst), 1);
    }
}
