//! Unit tests moved from `src/server_auth.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use axum::body::Body;
    pub use axum::extract::{Request, State};
    pub use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
    pub use axum::middleware::Next;
    pub use axum::response::{IntoResponse, Response};
    pub use base64::Engine;
    pub use futures::future::{BoxFuture, FutureExt, Shared};
    pub use hmac::{Hmac, Mac};
    pub use rusternetes_client::api_status;
    pub use rusternetes_client::http::ApiClient;
    pub use rusternetes_common::auth::UserInfo;
    pub use rusternetes_common::authz::{AlwaysAllowAuthorizer, Authorizer, Decision, Opinion};
    pub use rusternetes_common::lru_expire_cache::LruExpireCache;
    pub use rusternetes_common::resources::{
        NonResourceAttributes, ResourceAttributes, SubjectAccessReview, SubjectAccessReviewSpec,
        TokenReview, TokenReviewSpec,
    };
    pub use rusternetes_common::wait::Backoff;
    pub use rusternetes_kubelet::config::KubeletConfiguration;
    pub use rusternetes_kubelet::server::{get_request_attributes, RequestAttributes};
    pub use rusternetes_middleware::{user_from_client_cert_der, PeerCertificates};
    pub use rustls::pki_types::{CertificateDer, UnixTime};
    pub use rustls::server::danger::ClientCertVerifier;
    pub use sha2::Sha256;
    pub use std::collections::{BTreeMap, HashMap};
    pub use std::fmt;
    pub use std::sync::{Arc, Mutex, RwLock};
    pub use std::time::{Duration, Instant};
}

mod tests {
    #[test]
    fn default_should_retry_covers_delay_and_http2_lost() {
        let st = |code: u16, reason: &str, secs: Option<i32>| {
            anyhow::Error::new(api_status::ApiStatusError {
                code,
                reason: reason.into(),
                message: "m".into(),
                retry_after_seconds: secs,
            })
        };
        // SuggestsClientDelay: Retry-After on an otherwise non-retriable code.
        assert!(default_should_retry(&st(
            503,
            "ServiceUnavailable",
            Some(2)
        )));
        assert!(default_should_retry(&st(418, "", Some(1))));
        assert!(default_should_retry(&st(418, "ServerTimeout", Some(0))));
        assert!(!default_should_retry(&st(418, "", Some(0))));
        assert!(!default_should_retry(&st(403, "Forbidden", None)));
        // IsHTTP2ConnectionLost.
        assert!(default_should_retry(&anyhow::anyhow!(
            "Post: http2: client connection lost"
        )));
        assert!(!default_should_retry(&anyhow::anyhow!("boom")));
    }

    use super::__parent::*;
    use axum::routing::get;
    use axum::Router;
    use rusternetes_common::resources::{SubjectAccessReviewStatus, TokenReviewStatus};
    use rusternetes_kubelet::server_auth::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;

    // ---- fakes -----------------------------------------------------------

    /// A TokenReview backend that authenticates `good` as `alice` (groups
    /// `dev`) and reports an `error` for `broken`.
    #[derive(Default)]
    struct FakeTokenReview {
        calls: AtomicUsize,
        /// Fail this many leading calls with a 500 (`IsInternalError`).
        transient_failures: AtomicUsize,
    }

    #[async_trait]
    impl TokenReviewClient for FakeTokenReview {
        async fn create(&self, review: &TokenReview) -> ReviewResponse<TokenReview> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self
                .transient_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return (
                    500,
                    Err(anyhow::Error::new(api_status::ApiStatusError {
                        code: 500,
                        reason: "InternalError".into(),
                        message: "boom".into(),
                        retry_after_seconds: None,
                    })),
                );
            }
            let status = match review.spec.token.as_str() {
                "good" => TokenReviewStatus {
                    authenticated: Some(true),
                    user: Some(rusternetes_common::resources::authentication::UserInfo {
                        username: Some("alice".into()),
                        uid: Some("u1".into()),
                        groups: Some(vec!["dev".into()]),
                        extra: None,
                    }),
                    ..Default::default()
                },
                "broken" => TokenReviewStatus {
                    authenticated: Some(false),
                    error: Some("token is broken".into()),
                    ..Default::default()
                },
                _ => TokenReviewStatus {
                    authenticated: Some(false),
                    ..Default::default()
                },
            };
            let mut out = review.clone();
            out.status = Some(status);
            (200, Ok(out))
        }
    }

    /// A SubjectAccessReview backend that records the specs it was asked and
    /// answers by `subresource`: `log` and `stats` are allowed, `metrics` is
    /// explicitly denied, `checkpoint` errors, anything else gets no opinion.
    #[derive(Default)]
    struct FakeSar {
        calls: AtomicUsize,
        specs: Mutex<Vec<SubjectAccessReviewSpec>>,
    }

    #[async_trait]
    impl SubjectAccessReviewClient for FakeSar {
        async fn create(
            &self,
            review: &SubjectAccessReview,
        ) -> ReviewResponse<SubjectAccessReview> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.specs.lock().unwrap().push(review.spec.clone());
            let sub = review
                .spec
                .resource_attributes
                .as_ref()
                .and_then(|r| r.subresource.clone())
                .unwrap_or_default();
            if sub == "checkpoint" {
                return (
                    403,
                    Err(anyhow::Error::new(api_status::ApiStatusError {
                        code: 403,
                        reason: "Forbidden".into(),
                        message: "sar forbidden".into(),
                        retry_after_seconds: None,
                    })),
                );
            }
            let status = SubjectAccessReviewStatus {
                allowed: sub == "log" || sub == "stats",
                denied: (sub == "metrics").then_some(true),
                reason: Some(format!("fake:{sub}")),
                ..Default::default()
            };
            let mut out = review.clone();
            out.status = Some(status);
            (200, Ok(out))
        }
    }

    fn authn(webhook: bool, anonymous: bool) -> AuthnConfig {
        AuthnConfig {
            client_ca_file: String::new(),
            webhook_enabled: webhook,
            webhook_cache_ttl: Duration::from_secs(120),
            anonymous_enabled: anonymous,
        }
    }

    fn authz_webhook() -> AuthzConfig {
        AuthzConfig {
            mode: AuthzMode::Webhook,
            cache_authorized_ttl: Duration::from_secs(300),
            cache_unauthorized_ttl: Duration::from_secs(30),
        }
    }

    fn app(auth: KubeletAuth) -> Router {
        let routes = Router::new()
            .route("/pods", get(|| async { "pods" }))
            .route("/stats/summary", get(|| async { "stats" }))
            .route("/metrics", get(|| async { "metrics" }))
            .route("/logs/x", get(|| async { "logs" }))
            .route("/checkpoint/a/b/c", get(|| async { "cp" }));
        install_auth_filter(routes, Some(Arc::new(auth)))
    }

    fn auth_with(
        tr: Arc<FakeTokenReview>,
        sar: Arc<FakeSar>,
        authn_cfg: &AuthnConfig,
        authz_cfg: &AuthzConfig,
    ) -> KubeletAuth {
        let (authenticator, _) = build_authn(Some(tr), authn_cfg).unwrap();
        let authorizer = build_authz(Some(sar), authz_cfg).unwrap();
        KubeletAuth {
            authenticator,
            node_name: "node-1".into(),
            authorizer,
        }
    }

    async fn call(app: &Router, path: &str, bearer: Option<&str>) -> (u16, String) {
        let mut req = axum::http::Request::builder().uri(path);
        if let Some(t) = bearer {
            req = req.header("Authorization", format!("Bearer {t}"));
        }
        let resp = app
            .clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status().as_u16();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    // ---- the filter (server.go InstallAuthFilter) --------------------------

    // Before this change nothing wrapped the routes: an unauthenticated
    // request to the kubelet API was served.
    #[tokio::test]
    async fn unauthenticated_request_is_401() {
        let app = app(auth_with(
            Arc::default(),
            Arc::default(),
            &authn(true, false),
            &authz_webhook(),
        ));
        assert_eq!(
            call(&app, "/pods", None).await,
            (401, "Unauthorized".into())
        );
        // an unauthenticated bearer token is an error (bearertoken: "invalid bearer token")
        assert_eq!(
            call(&app, "/pods", Some("nope")).await,
            (401, "Unauthorized".into())
        );
        assert_eq!(
            call(&app, "/pods", Some("broken")).await,
            (401, "Unauthorized".into())
        );
    }

    #[tokio::test]
    async fn authenticated_and_authorized_request_is_served() {
        let sar = Arc::new(FakeSar::default());
        let app = app(auth_with(
            Arc::default(),
            sar.clone(),
            &authn(true, false),
            &authz_webhook(),
        ));
        assert_eq!(
            call(&app, "/stats/summary", Some("good")).await,
            (200, "stats".into())
        );
        let specs = sar.specs.lock().unwrap();
        let spec = &specs[0];
        assert_eq!(spec.user.as_deref(), Some("alice"));
        assert_eq!(spec.uid.as_deref(), Some("u1"));
        assert_eq!(
            spec.groups.as_deref(),
            Some(&["dev".to_string(), "system:authenticated".to_string()][..])
        );
        let ra = spec.resource_attributes.as_ref().unwrap();
        assert_eq!(ra.resource.as_deref(), Some("nodes"));
        assert_eq!(ra.subresource.as_deref(), Some("stats"));
        assert_eq!(ra.name.as_deref(), Some("node-1"));
        assert_eq!(ra.verb.as_deref(), Some("get"));
        assert_eq!(ra.version.as_deref(), Some("v1"));
    }

    // server.go:371-377: the Forbidden text.
    #[tokio::test]
    async fn authenticated_but_unauthorized_request_is_403() {
        let app = app(auth_with(
            Arc::default(),
            Arc::default(),
            &authn(true, false),
            &authz_webhook(),
        ));
        // `metrics` is an explicit deny; `proxy` (/pods) gets no opinion.
        assert_eq!(
            call(&app, "/metrics", Some("good")).await,
            (
                403,
                "Forbidden (user=alice, verb=get, resource=nodes, subresource(s)=[metrics])\n"
                    .into()
            )
        );
        assert_eq!(call(&app, "/pods", Some("good")).await.0, 403);
    }

    // server.go:359-364: an authorizer error is a 500, not a 403.
    #[tokio::test]
    async fn authorization_error_is_500() {
        let app = app(auth_with(
            Arc::default(),
            Arc::default(),
            &authn(true, false),
            &authz_webhook(),
        ));
        assert_eq!(
            call(&app, "/checkpoint/a/b/c", Some("good")).await,
            (
                500,
                "Authorization error (user=alice, verb=get, resource=nodes, subresource=checkpoint)"
                    .into()
            )
        );
    }

    // delegating.go:121-127: anonymous runs behind `NewFailOnError`.
    #[tokio::test]
    async fn anonymous_is_only_for_requests_without_credentials() {
        let sar = Arc::new(FakeSar::default());
        let app = app(auth_with(
            Arc::default(),
            sar.clone(),
            &authn(true, true),
            &authz_webhook(),
        ));
        // no credentials -> system:anonymous, allowed for `log`
        assert_eq!(call(&app, "/logs/x", None).await, (200, "logs".into()));
        {
            let specs = sar.specs.lock().unwrap();
            assert_eq!(specs[0].user.as_deref(), Some("system:anonymous"));
            assert_eq!(
                specs[0].groups.as_deref(),
                Some(&["system:unauthenticated".to_string()][..])
            );
        }
        // a rejected token must NOT fall back to anonymous
        assert_eq!(call(&app, "/logs/x", Some("nope")).await.0, 401);
    }

    // BuildAuthz AlwaysAllow + anonymous only (the legacy flag defaults).
    #[tokio::test]
    async fn legacy_defaults_serve_everything() {
        let (authn_cfg, authz_cfg) = resolve_auth_config(None, &AuthFlags::default());
        let (authenticator, _) = build_authn(None, &authn_cfg).unwrap();
        let app = app(KubeletAuth {
            authenticator,
            node_name: "node-1".into(),
            authorizer: build_authz(None, &authz_cfg).unwrap(),
        });
        assert_eq!(call(&app, "/pods", None).await, (200, "pods".into()));
    }

    // ---- caching (cached_token_authenticator.go / webhook.go) --------------

    // The consumer of `authentication.webhook.cacheTTL`.
    #[tokio::test]
    async fn token_review_results_are_cached_for_the_ttl() {
        let tr = Arc::new(FakeTokenReview::default());
        let (a, _) = build_authn(Some(tr.clone()), &authn(true, false)).unwrap();
        let mut h = HeaderMap::new();
        for _ in 0..3 {
            h.insert("authorization", "Bearer good".parse().unwrap());
            let mut r = AuthnRequest {
                headers: &mut h,
                peer_certs: None,
            };
            let u = a.authenticate_request(&mut r).await.unwrap().unwrap();
            assert_eq!(u.username, "alice");
            // bearertoken deletes the header once authenticated
            assert!(h.get("authorization").is_none());
        }
        assert_eq!(tr.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn zero_cache_ttl_does_not_cache() {
        let tr = Arc::new(FakeTokenReview::default());
        let mut cfg = authn(true, false);
        cfg.webhook_cache_ttl = Duration::ZERO;
        let (a, _) = build_authn(Some(tr.clone()), &cfg).unwrap();
        for _ in 0..2 {
            let mut h = HeaderMap::new();
            h.insert("authorization", "Bearer good".parse().unwrap());
            let mut r = AuthnRequest {
                headers: &mut h,
                peer_certs: None,
            };
            a.authenticate_request(&mut r).await.unwrap().unwrap();
        }
        assert_eq!(tr.calls.load(Ordering::SeqCst), 2);
    }

    // cached_token_authenticator_test.go TestSharedLookup: concurrent lookups
    // of one token share a single call.
    #[tokio::test]
    async fn concurrent_lookups_share_one_call() {
        struct Slow(AtomicUsize);
        #[async_trait]
        impl TokenAuthenticator for Slow {
            async fn authenticate_token(&self, _t: &str) -> AuthnResult {
                self.0.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(Some(UserInfo {
                    username: "bob".into(),
                    uid: String::new(),
                    groups: vec![],
                    extra: HashMap::new(),
                }))
            }
        }
        let slow = Arc::new(Slow(AtomicUsize::new(0)));
        let c = CachedTokenAuthenticator::new(
            slow.clone(),
            false,
            Duration::ZERO, // not cached: only the in-flight sharing applies
            Duration::ZERO,
        );
        let rs = futures::future::join_all((0..8).map(|_| c.authenticate_token("t"))).await;
        assert!(rs.iter().all(|r| r.as_ref().unwrap().is_some()));
        assert_eq!(slow.0.load(Ordering::SeqCst), 1);
    }

    // webhook.go:262-285: allow and deny answers are cached (5m / 30s).
    #[tokio::test]
    async fn sar_responses_are_cached() {
        let sar = Arc::new(FakeSar::default());
        let app = app(auth_with(
            Arc::default(),
            sar.clone(),
            &authn(true, false),
            &authz_webhook(),
        ));
        for _ in 0..3 {
            assert_eq!(call(&app, "/stats/summary", Some("good")).await.0, 200);
            assert_eq!(call(&app, "/metrics", Some("good")).await.0, 403);
        }
        // one SAR per distinct spec, not per request
        assert_eq!(sar.calls.load(Ordering::SeqCst), 2);
    }

    // webhook.go:235 `WithExponentialBackoff`/`DefaultShouldRetry`: a 500 is
    // retried, anything else is not.
    #[tokio::test(start_paused = true)]
    async fn transient_token_review_errors_are_retried() {
        let tr = Arc::new(FakeTokenReview::default());
        tr.transient_failures.store(2, Ordering::SeqCst);
        let w = WebhookTokenAuthenticator::new(
            tr.clone(),
            Backoff::default_auth_webhook(),
            Arc::new(NoopReviewMetrics),
        );
        let u = w.authenticate_token("good").await.unwrap().unwrap();
        assert_eq!(u.username, "alice");
        assert_eq!(tr.calls.load(Ordering::SeqCst), 3);

        // exhausted: 5 attempts (`Steps: 5`), the webhook error is returned
        let tr = Arc::new(FakeTokenReview::default());
        tr.transient_failures.store(100, Ordering::SeqCst);
        let w = WebhookTokenAuthenticator::new(
            tr.clone(),
            Backoff::default_auth_webhook(),
            Arc::new(NoopReviewMetrics),
        );
        assert!(w.authenticate_token("good").await.is_err());
        assert_eq!(tr.calls.load(Ordering::SeqCst), 5);
    }

    // webhook.go:~190: `status.error` is the authentication error.
    #[tokio::test]
    async fn token_review_status_error_is_an_error() {
        let w = WebhookTokenAuthenticator::new(
            Arc::new(FakeTokenReview::default()),
            Backoff::default_auth_webhook(),
            Arc::new(NoopReviewMetrics),
        );
        assert_eq!(
            w.authenticate_token("broken").await,
            Err(AuthnError("token is broken".into()))
        );
        assert_eq!(w.authenticate_token("who").await, Ok(None));
    }

    // ---- request metrics (plugin/pkg/authenticator/token/webhook/metrics_test.go
    // TestAuthenticatorMetrics; plugin/pkg/authorizer/webhook/webhook_v1_test.go
    // TestV1WebhookMetrics-style recording) --------------------------------

    /// `fakeAuthenticatorMetrics`: the last code recorded by each method.
    #[derive(Default)]
    struct FakeReviewMetrics {
        total_code: Mutex<String>,
        latency_code: Mutex<String>,
    }

    impl ReviewMetrics for FakeReviewMetrics {
        fn record_request_total(&self, code: &str) {
            *self.total_code.lock().unwrap() = code.to_string();
        }
        fn record_request_latency(&self, code: &str, _latency_secs: f64) {
            *self.latency_code.lock().unwrap() = code.to_string();
        }
    }

    /// A TokenReview backend answering with a fixed outcome: `Some(code)` is a
    /// response with that status (2xx succeeds, otherwise an `ApiStatusError`),
    /// `None` is a request that never got a response (the "incorrect client
    /// certificate" scenario: the webhook is not called, an error is recorded).
    struct FixedTokenReview(Option<u16>);

    #[async_trait]
    impl TokenReviewClient for FixedTokenReview {
        async fn create(&self, review: &TokenReview) -> ReviewResponse<TokenReview> {
            match self.0 {
                None => (
                    0,
                    Err(anyhow::anyhow!(
                        "x509: certificate signed by unknown authority"
                    )),
                ),
                Some(code) if code < 300 => {
                    let mut out = review.clone();
                    out.status = Some(TokenReviewStatus {
                        authenticated: Some(true),
                        ..Default::default()
                    });
                    (code, Ok(out))
                }
                Some(code) => (
                    code,
                    Err(anyhow::Error::new(api_status::ApiStatusError {
                        code,
                        reason: "InternalError".into(),
                        message: "boom".into(),
                        retry_after_seconds: None,
                    })),
                ),
            }
        }
    }

    // TestAuthenticatorMetrics: the status code of the response is recorded
    // for both the total and the latency; no response records `<error>`.
    #[tokio::test(start_paused = true)]
    async fn authenticator_metrics_record_status_code() {
        for (name, client, expected, want_err) in [
            ("happy path", FixedTokenReview(Some(200)), "200", false),
            (
                "an internal error returned from the webhook",
                FixedTokenReview(Some(500)),
                "500",
                true,
            ),
            (
                "incorrect client certificate used, the webhook not called, an error is recorded",
                FixedTokenReview(None),
                "<error>",
                true,
            ),
        ] {
            let metrics = Arc::new(FakeReviewMetrics::default());
            let w = WebhookTokenAuthenticator::new(
                Arc::new(client),
                Backoff::default_auth_webhook(),
                metrics.clone(),
            );
            let got = w.authenticate_token("t0k3n").await;
            assert_eq!(got.is_err(), want_err, "{name}");
            assert_eq!(*metrics.total_code.lock().unwrap(), expected, "{name}");
            assert_eq!(*metrics.latency_code.lock().unwrap(), expected, "{name}");
        }
    }

    // webhook.go:256-266 for the delegating authorizer: a 403 from the
    // SubjectAccessReview endpoint is recorded under its code, a response
    // with 200 under "200".
    #[tokio::test]
    async fn authorizer_metrics_record_status_code() {
        let metrics = Arc::new(FakeReviewMetrics::default());
        let sar = Arc::new(FakeSar::default());
        let a = DelegatingAuthorizer::new(
            sar.clone(),
            Duration::from_secs(60),
            Duration::from_secs(60),
            Backoff::default_auth_webhook(),
            metrics.clone(),
        );
        let attrs = |sub: &str| {
            let user = UserInfo {
                username: "alice".into(),
                uid: String::new(),
                groups: vec![],
                extra: Default::default(),
            };
            let mut a = rusternetes_common::authz::RequestAttributes::new(user, "get", "nodes");
            a.api_version = "v1".into();
            a.subresource = Some(sub.into());
            a.name = Some("n1".into());
            a
        };
        assert!(a.authorize_opinion(&attrs("log")).await.is_ok());
        assert_eq!(*metrics.total_code.lock().unwrap(), "200");
        assert_eq!(*metrics.latency_code.lock().unwrap(), "200");
        assert!(a.authorize_opinion(&attrs("checkpoint")).await.is_err());
        assert_eq!(*metrics.total_code.lock().unwrap(), "403");
        assert_eq!(*metrics.latency_code.lock().unwrap(), "403");
    }

    // authenticatorfactory/metrics.go + authorizerfactory/metrics.go: the
    // delegated metrics are served from the default registry under their
    // upstream names.
    #[test]
    fn delegated_metrics_are_registered_under_upstream_names() {
        let authn = delegating_authenticator_metrics();
        authn.record_request_total("200");
        authn.record_request_latency("200", 0.3);
        let authz = new_delegating_authorizer_metrics();
        authz.record_request_total("<error>");
        authz.record_request_latency("<error>", 0.3);
        let names: Vec<String> = prometheus::gather()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        for n in [
            "apiserver_delegated_authn_request_total",
            "apiserver_delegated_authn_request_duration_seconds",
            "apiserver_delegated_authz_request_total",
            "apiserver_delegated_authz_request_duration_seconds",
        ] {
            assert!(names.iter().any(|x| x == n), "{n} missing from {names:?}");
        }
    }

    // ---- BuildAuthn / BuildAuthz (cmd/kubelet/app/auth.go) -----------------

    #[test]
    fn build_authn_errors() {
        let err = |r: anyhow::Result<_>| r.err().map(|e| e.to_string());
        assert_eq!(
            err(build_authn(None, &authn(true, false))),
            Some("no client provided, cannot use webhook authentication".into())
        );
        assert_eq!(
            err(build_authn(None, &authn(false, false))),
            Some("no authentication method configured".into())
        );
        assert!(build_authn(None, &authn(false, true)).is_ok());
    }

    #[test]
    fn build_authz_modes() {
        let cfg = |mode: AuthzMode| AuthzConfig {
            mode,
            ..authz_webhook()
        };
        let err = |r: anyhow::Result<_>| r.err().map(|e| e.to_string());
        assert!(build_authz(None, &cfg(AuthzMode::AlwaysAllow)).is_ok());
        assert_eq!(
            err(build_authz(None, &cfg(AuthzMode::Webhook))),
            Some("no client provided, cannot use webhook authorization".into())
        );
        assert_eq!(
            err(build_authz(None, &cfg(AuthzMode::parse("")))),
            Some("no authorization mode specified".into())
        );
        assert_eq!(
            err(build_authz(None, &cfg(AuthzMode::parse("RBAC")))),
            Some("unknown authorization mode RBAC".into())
        );
    }

    // options.go:211-221 (no --config) vs v1beta1/defaults.go:92-109 (--config).
    #[test]
    fn resolve_auth_config_defaults_and_precedence() {
        let (n, z) = resolve_auth_config(None, &AuthFlags::default());
        assert!(n.anonymous_enabled && !n.webhook_enabled);
        assert_eq!(z.mode, AuthzMode::AlwaysAllow);
        assert_eq!(n.webhook_cache_ttl, Duration::from_secs(120));
        assert_eq!(z.cache_authorized_ttl, Duration::from_secs(300));
        assert_eq!(z.cache_unauthorized_ttl, Duration::from_secs(30));

        let file: KubeletConfiguration = serde_yaml::from_str(
            "apiVersion: kubelet.config.k8s.io/v1beta1\nkind: KubeletConfiguration\n\
             authentication:\n  x509:\n    clientCAFile: /ca.pem\n  webhook:\n    cacheTTL: 10s\n\
             authorization:\n  webhook:\n    cacheAuthorizedTTL: 1m\n    cacheUnauthorizedTTL: 5s\n",
        )
        .unwrap();
        let (n, z) = resolve_auth_config(Some(&file), &AuthFlags::default());
        assert!(!n.anonymous_enabled && n.webhook_enabled);
        assert_eq!(z.mode, AuthzMode::Webhook);
        assert_eq!(n.client_ca_file, "/ca.pem");
        assert_eq!(n.webhook_cache_ttl, Duration::from_secs(10));
        assert_eq!(z.cache_authorized_ttl, Duration::from_secs(60));
        assert_eq!(z.cache_unauthorized_ttl, Duration::from_secs(5));

        // a flag that was passed wins over the file
        let (n, z) = resolve_auth_config(
            Some(&file),
            &AuthFlags {
                anonymous_auth: Some(true),
                authentication_token_webhook: Some(false),
                authorization_mode: Some("AlwaysAllow".into()),
                client_ca_file: Some("/other.pem".into()),
                authentication_token_webhook_cache_ttl: Some(Duration::from_secs(7)),
                ..Default::default()
            },
        );
        assert!(n.anonymous_enabled && !n.webhook_enabled);
        assert_eq!(z.mode, AuthzMode::AlwaysAllow);
        assert_eq!(n.client_ca_file, "/other.pem");
        assert_eq!(n.webhook_cache_ttl, Duration::from_secs(7));
    }

    // ---- request authenticators (bearertoken / websocket / union / group) ---

    struct Tok;
    #[async_trait]
    impl TokenAuthenticator for Tok {
        async fn authenticate_token(&self, t: &str) -> AuthnResult {
            Ok((t == "tok").then(|| UserInfo {
                username: "u".into(),
                uid: String::new(),
                groups: vec![],
                extra: HashMap::new(),
            }))
        }
    }

    // bearertoken_test.go
    #[tokio::test]
    async fn bearer_token_header_parsing() {
        let a = BearerTokenAuthenticator::new(Arc::new(Tok));
        let run = |v: Option<&'static str>| {
            let a = &a;
            async move {
                let mut h = HeaderMap::new();
                if let Some(v) = v {
                    h.insert("authorization", v.parse().unwrap());
                }
                a.authenticate_request(&mut AuthnRequest {
                    headers: &mut h,
                    peer_certs: None,
                })
                .await
            }
        };
        assert_eq!(run(None).await, Ok(None));
        assert_eq!(run(Some("Basic abc")).await, Ok(None));
        assert_eq!(run(Some("Bearer")).await, Ok(None));
        assert_eq!(run(Some("Bearer  tok")).await, Ok(None)); // extra space
        assert_eq!(
            run(Some("Bearer bad")).await,
            Err(AuthnError("invalid bearer token".into()))
        );
        assert_eq!(
            run(Some("bearer tok")).await.unwrap().unwrap().username,
            "u"
        );
        assert_eq!(
            run(Some("BEARER tok x")).await.unwrap().unwrap().username,
            "u"
        );
    }

    // websocket/protocol_test.go
    #[tokio::test]
    async fn websocket_protocol_bearer_token() {
        let a = WebsocketProtocolAuthenticator::new(Arc::new(Tok));
        let enc = |t: &str| {
            format!(
                "{BEARER_PROTOCOL_PREFIX}{}",
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(t)
            )
        };
        let run = |proto: Option<String>, ws: bool| {
            let a = &a;
            async move {
                let mut h = HeaderMap::new();
                if ws {
                    h.insert("upgrade", "websocket".parse().unwrap());
                    h.insert("connection", "keep-alive, Upgrade".parse().unwrap());
                }
                if let Some(p) = proto {
                    h.insert("sec-websocket-protocol", p.parse().unwrap());
                }
                let r = a
                    .authenticate_request(&mut AuthnRequest {
                        headers: &mut h,
                        peer_certs: None,
                    })
                    .await;
                (r, h)
            }
        };
        // not a websocket request
        assert_eq!(
            run(Some(format!("{}, v4.channel.k8s.io", enc("tok"))), false)
                .await
                .0,
            Ok(None)
        );
        let (r, h) = run(Some(format!("{}, v4.channel.k8s.io", enc("tok"))), true).await;
        assert_eq!(r.unwrap().unwrap().username, "u");
        // the token protocol is removed from the header
        assert_eq!(
            h.get("sec-websocket-protocol").unwrap(),
            "v4.channel.k8s.io"
        );
        assert_eq!(
            run(Some(enc("tok")), true).await.0,
            Err(AuthnError("missing additional subprotocol".into()))
        );
        assert_eq!(
            run(Some(format!("{}, {}, x", enc("tok"), enc("tok"))), true)
                .await
                .0,
            Err(AuthnError(
                "multiple base64.bearer.authorization tokens specified".into()
            ))
        );
        assert_eq!(
            run(Some(format!("{}!!, x", BEARER_PROTOCOL_PREFIX)), true)
                .await
                .0,
            Err(AuthnError(
                "invalid base64.bearer.authorization token encoding".into()
            ))
        );
        assert_eq!(
            run(Some(format!("{}, x", enc("bad"))), true).await.0,
            Err(AuthnError("invalid bearer token".into()))
        );
        assert_eq!(run(Some("x".into()), true).await.0, Ok(None));
    }

    // authenticated_group_adder_test.go
    #[tokio::test]
    async fn authenticated_group_adder() {
        struct Fixed(&'static str, Vec<&'static str>);
        #[async_trait]
        impl RequestAuthenticator for Fixed {
            async fn authenticate_request(&self, _r: &mut AuthnRequest<'_>) -> AuthnResult {
                Ok(Some(UserInfo {
                    username: self.0.into(),
                    uid: String::new(),
                    groups: self.1.iter().map(|s| s.to_string()).collect(),
                    extra: HashMap::new(),
                }))
            }
        }
        for (name, groups, want) in [
            ("bob", vec![], vec!["system:authenticated"]),
            ("bob", vec!["g"], vec!["g", "system:authenticated"]),
            ("system:anonymous", vec![], vec![]),
            (
                "bob",
                vec!["system:unauthenticated"],
                vec!["system:unauthenticated"],
            ),
            (
                "bob",
                vec!["system:authenticated"],
                vec!["system:authenticated"],
            ),
        ] {
            let a = AuthenticatedGroupAdder {
                authenticator: Arc::new(Fixed(name, groups)),
            };
            let mut h = HeaderMap::new();
            let u = a
                .authenticate_request(&mut AuthnRequest {
                    headers: &mut h,
                    peer_certs: None,
                })
                .await
                .unwrap()
                .unwrap();
            assert_eq!(u.groups, want, "{name}");
        }
    }

    // union_test.go
    #[tokio::test]
    async fn union_error_aggregation() {
        struct E(Option<&'static str>);
        #[async_trait]
        impl RequestAuthenticator for E {
            async fn authenticate_request(&self, _r: &mut AuthnRequest<'_>) -> AuthnResult {
                match self.0 {
                    Some(m) => Err(AuthnError(m.into())),
                    None => Ok(None),
                }
            }
        }
        let run = |u: Arc<dyn RequestAuthenticator>| async move {
            let mut h = HeaderMap::new();
            u.authenticate_request(&mut AuthnRequest {
                headers: &mut h,
                peer_certs: None,
            })
            .await
        };
        let hs = |v: Vec<Option<&'static str>>| -> Vec<Arc<dyn RequestAuthenticator>> {
            v.into_iter().map(|m| Arc::new(E(m)) as _).collect()
        };
        assert_eq!(
            run(UnionAuthenticator::union(hs(vec![None, None]))).await,
            Ok(None)
        );
        assert_eq!(
            run(UnionAuthenticator::union(hs(vec![Some("a"), None]))).await,
            Err(AuthnError("a".into()))
        );
        assert_eq!(
            run(UnionAuthenticator::union(hs(vec![Some("a"), Some("b")]))).await,
            Err(AuthnError("[a, b]".into()))
        );
        // FailOnError stops at the first error
        assert_eq!(
            run(UnionAuthenticator::fail_on_error(hs(vec![
                Some("a"),
                Some("b")
            ])))
            .await,
            Err(AuthnError("a".into()))
        );
    }

    // ---- x509 --------------------------------------------------------------

    struct Pki {
        ca_pem: String,
        ca: rcgen::Certificate,
        ca_key: rcgen::KeyPair,
    }

    fn pki() -> Pki {
        rusternetes_common::tls::TlsConfig::generate_self_signed("x", vec!["x".into()]).ok();
        let mut p = rcgen::CertificateParams::default();
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        p.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
        p.distinguished_name
            .push(rcgen::DnType::CommonName, "test-ca");
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca = p.self_signed(&ca_key).unwrap();
        Pki {
            ca_pem: ca.pem(),
            ca,
            ca_key,
        }
    }

    /// (leaf DER, leaf PEM, key PEM) of a client cert for `cn`/`org`.
    fn client_cert(pki: &Pki, cn: &str, org: &str) -> (Vec<u8>, String, String) {
        let mut p = rcgen::CertificateParams::default();
        p.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        p.distinguished_name.push(rcgen::DnType::CommonName, cn);
        p.distinguished_name
            .push(rcgen::DnType::OrganizationName, org);
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = p.signed_by(&key, &pki.ca, &pki.ca_key).unwrap();
        (cert.der().to_vec(), cert.pem(), key.serialize_pem())
    }

    fn x509(pki: &Pki) -> (X509Authenticator, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca.pem");
        std::fs::write(&path, &pki.ca_pem).unwrap();
        let ca = ClientCaContent::from_file(path.to_str().unwrap()).unwrap();
        (X509Authenticator::new(ca), dir)
    }

    // x509_test.go: CN -> user, O -> groups; an unknown CA is an error.
    #[tokio::test]
    async fn x509_authenticator() {
        let pki = pki();
        let (a, _dir) = x509(&pki);
        let (der, _, _) = client_cert(&pki, "system:node:n1", "system:nodes");
        let mut h = HeaderMap::new();
        let peer = PeerCertificates(Arc::new(vec![der]));
        let u = a
            .authenticate_request(&mut AuthnRequest {
                headers: &mut h,
                peer_certs: Some(&peer),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(u.username, "system:node:n1");
        assert!(u.groups.contains(&"system:nodes".to_string()));

        // no certificate: no opinion
        assert_eq!(
            a.authenticate_request(&mut AuthnRequest {
                headers: &mut h,
                peer_certs: None,
            })
            .await,
            Ok(None)
        );
        // a certificate from another CA: an error
        let other = self::pki();
        let (bad, _, _) = client_cert(&other, "mallory", "x");
        let peer = PeerCertificates(Arc::new(vec![bad]));
        assert!(a
            .authenticate_request(&mut AuthnRequest {
                headers: &mut h,
                peer_certs: Some(&peer),
            })
            .await
            .is_err());
    }

    // An end-to-end handshake: `ClientAuth = RequestClientCert`, the acceptor's
    // `PeerCertificates`, the x509 authenticator and the filter together.
    #[tokio::test]
    async fn x509_over_a_real_tls_connection() {
        let pki = pki();
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, &pki.ca_pem).unwrap();
        let cfg = AuthnConfig {
            client_ca_file: ca_path.to_str().unwrap().into(),
            ..authn(false, true)
        };
        let (authenticator, _) = build_authn(None, &cfg).unwrap();
        let app = install_auth_filter(
            Router::new().route(
                "/pods",
                get(|req: Request<Body>| async move {
                    req.headers()
                        .get("x-seen")
                        .map(|_| "x")
                        .unwrap_or("ok")
                        .to_string()
                }),
            ),
            Some(Arc::new(KubeletAuth {
                authenticator,
                node_name: "node-1".into(),
                authorizer: Arc::new(AlwaysAllowAuthorizer),
            })),
        );
        let tls = rusternetes_kubelet::serving_tls::kubelet_serving_tls_with_client_ca(
            "node-1",
            Some(ca_path.to_str().unwrap()),
        )
        .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        tokio::spawn(
            axum_server::from_tcp_rustls(listener, tls.clone())
                .acceptor(rusternetes_kubelet::serving_tls::PeerCertAcceptor::new(tls))
                .serve(app.into_make_service()),
        );

        let (_, cert_pem, key_pem) = client_cert(&pki, "alice", "devs");
        let anon = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let with_cert = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .identity(
                reqwest::Identity::from_pem(format!("{cert_pem}{key_pem}").as_bytes()).unwrap(),
            )
            .build()
            .unwrap();
        let url = format!("https://127.0.0.1:{}/pods", addr.port());
        // both are served: anonymous is enabled, and a valid cert authenticates
        assert_eq!(anon.get(&url).send().await.unwrap().status(), 200);
        assert_eq!(with_cert.get(&url).send().await.unwrap().status(), 200);

        // a certificate from an unknown CA is a 401 (not anonymous), as
        // `RequestClientCert` leaves verification to the authenticator
        let other = self::pki();
        let (_, c, k) = client_cert(&other, "mallory", "x");
        let bad = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .identity(reqwest::Identity::from_pem(format!("{c}{k}").as_bytes()).unwrap())
            .build()
            .unwrap();
        assert_eq!(bad.get(&url).send().await.unwrap().status(), 401);
    }

    // ---- DynamicFileCAContent (dynamic_cafile_content_test.go) ------------

    struct CountingListener(std::sync::atomic::AtomicUsize);
    impl Listener for CountingListener {
        fn enqueue(&self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    async fn verifies(ca: &Arc<ClientCaContent>, pki: &Pki) -> bool {
        let (der, _, _) = client_cert(pki, "u", "g");
        let leaf = CertificateDer::from(der);
        ca.verifier()
            .verify_client_cert(&leaf, &[], UnixTime::now())
            .is_ok()
    }

    async fn eventually<F: std::future::Future<Output = bool>>(mut f: impl FnMut() -> F) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if f().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("condition not reached in time");
    }

    // NewDynamicCAContentFromFile: a missing, empty or certificate-less file
    // fails construction.
    #[test]
    fn client_ca_content_rejects_bad_files() {
        let dir = tempfile::tempdir().unwrap();
        assert!(ClientCaContent::from_file("").is_err());
        assert!(ClientCaContent::from_file(dir.path().join("none").to_str().unwrap()).is_err());
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, b"").unwrap();
        assert!(ClientCaContent::from_file(empty.to_str().unwrap()).is_err());
        let junk = dir.path().join("junk.pem");
        std::fs::write(&junk, b"not a cert").unwrap();
        assert!(ClientCaContent::from_file(junk.to_str().unwrap()).is_err());
    }

    // loadCABundle: unchanged content does not notify listeners; changed
    // content swaps the bundle and notifies; a bad write keeps the old one.
    #[test]
    fn client_ca_content_load_detects_change() {
        let (a, b) = (pki(), pki());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca.pem");
        std::fs::write(&path, &a.ca_pem).unwrap();
        let ca = ClientCaContent::from_file(path.to_str().unwrap()).unwrap();
        let l = Arc::new(CountingListener(Default::default()));
        ca.add_listener(l.clone());

        ca.run_once().unwrap();
        assert_eq!(l.0.load(std::sync::atomic::Ordering::SeqCst), 0);

        std::fs::write(&path, &b.ca_pem).unwrap();
        ca.run_once().unwrap();
        assert_eq!(l.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(ca.current_ca_bundle_content(), b.ca_pem.as_bytes());

        std::fs::write(&path, b"garbage").unwrap();
        assert!(ca.run_once().is_err());
        assert_eq!(ca.current_ca_bundle_content(), b.ca_pem.as_bytes());
        std::fs::write(&path, b"").unwrap();
        assert!(ca.run_once().is_err());
        assert_eq!(ca.current_ca_bundle_content(), b.ca_pem.as_bytes());
    }

    // Run: an in-place write and an atomic rename-over (kubelet configmap/secret
    // volume style) are both picked up by the file watch, with no 1-minute wait.
    #[tokio::test]
    async fn client_ca_content_reloads_on_file_change() {
        let (a, b, c) = (pki(), pki(), pki());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca.pem");
        std::fs::write(&path, &a.ca_pem).unwrap();
        let ca = ClientCaContent::from_file(path.to_str().unwrap()).unwrap();
        assert!(verifies(&ca, &a).await);
        assert!(!verifies(&ca, &b).await);

        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(ca.clone().run(stop_rx));
        // let the watch register
        tokio::time::sleep(Duration::from_millis(200)).await;

        std::fs::write(&path, &b.ca_pem).unwrap();
        eventually(|| verifies(&ca, &b)).await;
        assert!(!verifies(&ca, &a).await);

        // rename over the watched file: the watch is re-added on the new inode
        let tmp = dir.path().join("ca.pem.tmp");
        std::fs::write(&tmp, &c.ca_pem).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        eventually(|| verifies(&ca, &c)).await;

        // and a second replacement still lands
        std::fs::write(&tmp, &a.ca_pem).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        eventually(|| verifies(&ca, &a)).await;

        stop_tx.send(true).unwrap();
        task.await.unwrap();
    }
}
