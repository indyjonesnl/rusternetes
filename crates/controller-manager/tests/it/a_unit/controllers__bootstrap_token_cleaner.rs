//! Unit tests moved from `src/controllers/bootstrap_token_cleaner.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use chrono::{DateTime, Utc};
    pub use k3rs_common::resources::Secret;
    pub use k3rs_storage::{build_key, build_prefix, Storage};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{error, info};
}

mod tests {
    use super::__parent::*;
    use chrono::Duration as D;
    use k3rs_controller_manager::controllers::bootstrap_token_cleaner::*;
    use k3rs_storage::memory::MemoryStorage;
    use std::collections::HashMap;

    /// `newTokenSecret` (common_test.go).
    fn new_token_secret(id: &str, secret: &str) -> Secret {
        let mut s = Secret::new(format!("bootstrap-token-{id}"), "kube-system");
        s.metadata.uid = format!("uid{id}");
        s.secret_type = Some(SECRET_TYPE_BOOTSTRAP_TOKEN.into());
        let mut d = HashMap::new();
        d.insert("token-id".to_string(), id.as_bytes().to_vec());
        d.insert("token-secret".to_string(), secret.as_bytes().to_vec());
        s.data = Some(d);
        s
    }

    /// `addSecretExpiration`.
    fn add_expiration(s: &mut Secret, exp: &str) {
        s.data
            .as_mut()
            .unwrap()
            .insert("expiration".into(), exp.as_bytes().to_vec());
    }

    /// `timeString(offset)`: RFC3339 of now+offset.
    fn time_string(now: DateTime<Utc>, off: D) -> String {
        (now + off).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }

    async fn setup(s: &Secret) -> (TokenCleaner<MemoryStorage>, Arc<MemoryStorage>, String) {
        let st = Arc::new(MemoryStorage::new());
        let key = build_key("secrets", Some("kube-system"), &s.metadata.name);
        st.create(&key, s).await.unwrap();
        (TokenCleaner::new(st.clone()), st, key)
    }

    fn q() -> k3rs_storage::WorkQueue {
        k3rs_storage::WorkQueue::new()
    }

    async fn exists(st: &MemoryStorage, key: &str) -> bool {
        st.get::<Secret>(key).await.is_ok()
    }

    // TestCleanerNoExpiration
    #[tokio::test]
    async fn cleaner_no_expiration() {
        let s = new_token_secret("tokenID", "tokenSecret");
        let (c, st, key) = setup(&s).await;
        assert_eq!(
            c.eval_secret(&q(), &s, Utc::now()).await,
            EvalOutcome::Nothing
        );
        assert!(exists(&st, &key).await);
    }

    // TestCleanerExpired
    #[tokio::test]
    async fn cleaner_expired() {
        let now = Utc::now();
        let mut s = new_token_secret("tokenID", "tokenSecret");
        add_expiration(&mut s, &time_string(now, D::hours(-1)));
        let (c, st, key) = setup(&s).await;
        assert_eq!(c.eval_secret(&q(), &s, now).await, EvalOutcome::Deleted);
        assert!(!exists(&st, &key).await);
    }

    // TestCleanerNotExpired
    #[tokio::test]
    async fn cleaner_not_expired() {
        let now = Utc::now();
        let mut s = new_token_secret("tokenID", "tokenSecret");
        add_expiration(&mut s, &time_string(now, D::hours(1)));
        let (c, st, key) = setup(&s).await;
        assert!(matches!(
            c.eval_secret(&q(), &s, now).await,
            EvalOutcome::RequeueAfter(_)
        ));
        assert!(exists(&st, &key).await);
    }

    // TestCleanerExpiredAt: not deleted now, requeued, deleted once due.
    #[tokio::test]
    async fn cleaner_expired_at() {
        let now = Utc::now();
        let mut s = new_token_secret("tokenID", "tokenSecret");
        add_expiration(&mut s, &time_string(now, D::seconds(2)));
        let (c, st, key) = setup(&s).await;
        let EvalOutcome::RequeueAfter(ttl) = c.eval_secret(&q(), &s, now).await else {
            panic!("expected requeue");
        };
        assert!(ttl <= Duration::from_secs(2) && ttl > Duration::ZERO);
        assert!(exists(&st, &key).await);
        let later = now + D::seconds(3);
        assert_eq!(c.eval_secret(&q(), &s, later).await, EvalOutcome::Deleted);
        assert!(!exists(&st, &key).await);
    }

    // GetExpiration: unparseable expiration is treated as expired.
    #[tokio::test]
    async fn unparseable_expiration_is_expired() {
        let mut s = new_token_secret("tokenID", "tokenSecret");
        add_expiration(&mut s, "not-a-time");
        let (c, st, key) = setup(&s).await;
        assert_eq!(
            c.eval_secret(&q(), &s, Utc::now()).await,
            EvalOutcome::Deleted
        );
        assert!(!exists(&st, &key).await);
    }

    // Delete's UID precondition: a recreated secret with another UID survives.
    #[tokio::test]
    async fn uid_precondition_conflict_keeps_secret() {
        let now = Utc::now();
        let mut stale = new_token_secret("tokenID", "tokenSecret");
        add_expiration(&mut stale, &time_string(now, D::hours(-1)));
        let mut live = stale.clone();
        live.metadata.uid = "other-uid".into();
        let (c, st, key) = setup(&live).await;
        c.eval_secret(&q(), &stale, now).await;
        assert!(exists(&st, &key).await);
    }

    // Informer FilterFunc + `enqueueSecrets`: only bootstrap-token secrets in
    // kube-system are queued, by `<namespace>/<name>`.
    #[tokio::test]
    async fn enqueue_secrets_filters_type_and_namespace() {
        let now = Utc::now();
        let st = Arc::new(MemoryStorage::new());
        let exp = time_string(now, D::hours(-1));

        let mut tok = new_token_secret("aaaaaa", "x");
        add_expiration(&mut tok, &exp);
        let mut opaque = new_token_secret("bbbbbb", "x");
        add_expiration(&mut opaque, &exp);
        opaque.secret_type = Some("Opaque".into());
        let mut other_ns = new_token_secret("cccccc", "x");
        add_expiration(&mut other_ns, &exp);
        other_ns.metadata.namespace = Some("default".into());

        for s in [&tok, &opaque, &other_ns] {
            let ns = s.metadata.namespace.clone().unwrap();
            st.create(&build_key("secrets", Some(&ns), &s.metadata.name), s)
                .await
                .unwrap();
        }
        let queue = q();
        TokenCleaner::new(st.clone()).enqueue_secrets(&queue).await;
        assert_eq!(queue.len().await, 1);
        assert_eq!(
            queue.get().await.unwrap(),
            format!("kube-system/{}", tok.metadata.name)
        );
    }

    // `processNextWorkItem` -> `syncFunc` -> `evalSecret`: an expired token is
    // deleted and the key is Forgotten.
    #[tokio::test]
    async fn worker_deletes_an_expired_token() {
        let now = Utc::now();
        let mut s = new_token_secret("tokenID", "tokenSecret");
        add_expiration(&mut s, &time_string(now, D::hours(-1)));
        let (c, st, key) = setup(&s).await;
        let queue = q();
        c.enqueue_secrets(&queue).await;
        assert!(c.process_next_work_item(&queue).await);
        assert!(!exists(&st, &key).await);
        assert_eq!(
            queue
                .num_requeues(&format!("kube-system/{}", s.metadata.name))
                .await,
            0
        );
    }

    // `queue.AddAfter(key, ttl)` (tokencleaner.go:219): a not-yet-expired token
    // is re-queued after its remaining lifetime, not deleted.
    #[tokio::test]
    async fn eval_secret_requeues_a_future_expiry_with_add_after() {
        let now = Utc::now();
        let mut s = new_token_secret("tokenID", "tokenSecret");
        add_expiration(&mut s, &time_string(now, D::seconds(1)));
        let (c, st, key) = setup(&s).await;
        let queue = q();
        c.sync_func(&queue, &format!("kube-system/{}", s.metadata.name), now)
            .await
            .unwrap();
        assert!(exists(&st, &key).await);
        // Delayed: not ready now ...
        assert_eq!(queue.len().await, 0);
        // ... but ready once the ttl has elapsed.
        let got = tokio::time::timeout(Duration::from_secs(5), queue.get())
            .await
            .expect("AddAfter never fired");
        assert_eq!(got.unwrap(), format!("kube-system/{}", s.metadata.name));
    }

    // `syncFunc`: a deleted Secret is not an error.
    #[tokio::test]
    async fn sync_func_ignores_a_deleted_secret() {
        let st = Arc::new(MemoryStorage::new());
        let c = TokenCleaner::new(st);
        c.sync_func(&q(), "kube-system/bootstrap-token-gone", Utc::now())
            .await
            .unwrap();
    }

    // `cache.SplitMetaNamespaceKey` failure is a sync error: rate-limited retry.
    #[tokio::test]
    async fn bad_key_is_rate_limited_not_forgotten() {
        let st = Arc::new(MemoryStorage::new());
        let c = TokenCleaner::new(st);
        let queue = k3rs_storage::WorkQueue::named(QUEUE_NAME);
        assert!(c.sync_func(&queue, "no-slash", Utc::now()).await.is_err());
        queue.add("no-slash".into()).await;
        assert!(c.process_next_work_item(&queue).await);
        assert_eq!(queue.num_requeues("no-slash").await, 1);
    }

    #[tokio::test]
    async fn worker_stops_when_the_queue_shuts_down() {
        let c = TokenCleaner::new(Arc::new(MemoryStorage::new()));
        let queue = q();
        queue.shutdown().await;
        assert!(!c.process_next_work_item(&queue).await);
        assert_eq!(QUEUE_NAME, "token_cleaner");
    }
}
