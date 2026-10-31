//! Unit tests moved from `src/secure_serving/authn.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use axum::http::{HeaderMap, HeaderName, HeaderValue};
    pub use futures::future::{BoxFuture, FutureExt, Shared};
    pub use hmac::{Hmac, Mac};
    pub use rusternetes_common::auth::UserInfo;
    pub use rusternetes_middleware::secure_serving::webhook::{
        default_should_retry, with_exponential_backoff, Backoff, ReviewClient,
    };
    pub use sha2::{Digest, Sha256};
    pub use std::collections::HashMap;
    pub use std::sync::{Arc, Mutex, RwLock};
    pub use std::time::Duration;
    pub use tokio::time::Instant;
}

mod tests {
    use super::__parent::*;
    use rusternetes_middleware::secure_serving::authn::*;
    use rusternetes_middleware::secure_serving::webhook::ReviewError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    fn user(name: &str) -> UserInfo {
        UserInfo {
            username: name.to_string(),
            uid: String::new(),
            groups: vec![],
            extra: HashMap::new(),
        }
    }

    /// `authenticator.TokenFunc`.
    struct TokenFn<F>(F);
    #[async_trait]
    impl<F> TokenAuthenticator for TokenFn<F>
    where
        F: Fn(&str) -> AuthResult + Send + Sync,
    {
        async fn authenticate_token(&self, token: &str) -> AuthResult {
            (self.0)(token)
        }
    }

    /// `authenticator.RequestFunc`.
    struct ReqFn<F>(F);
    #[async_trait]
    impl<F> RequestAuthenticator for ReqFn<F>
    where
        F: Fn(&mut AuthRequest<'_>) -> AuthResult + Send + Sync,
    {
        async fn authenticate_request(&self, req: &mut AuthRequest<'_>) -> AuthResult {
            (self.0)(req)
        }
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                axum::http::HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    async fn run(a: &dyn RequestAuthenticator, h: &mut HeaderMap) -> AuthResult {
        a.authenticate_request(&mut AuthRequest {
            headers: h,
            path: "/metrics",
            peer_certs: &[],
        })
        .await
    }

    fn ok_user(name: &'static str) -> Arc<dyn TokenAuthenticator> {
        Arc::new(TokenFn(move |_t: &str| {
            Ok(Some(Response { user: user(name) }))
        }))
    }

    // ---- bearertoken_test.go

    #[tokio::test]
    async fn bearer_token_authenticates() {
        let auth = BearerTokenAuthenticator::new(Arc::new(TokenFn(|t: &str| {
            assert_eq!(t, "token");
            Ok(Some(Response { user: user("user") }))
        })));
        let mut h = headers(&[("authorization", "Bearer token")]);
        let r = run(&auth, &mut h).await.unwrap();
        assert_eq!(r.unwrap().user.username, "user");
        // bearertoken.go: "remove the bearer token so that no one is ever tempted to use it"
        assert!(h.get("authorization").is_none());
    }

    // TestAuthenticateRequestIncludingValueAfterToken
    #[tokio::test]
    async fn bearer_token_ignores_values_after_the_token() {
        for v in ["Bearer token a", "Bearer token a b c", "Bearer token   a"] {
            let auth = BearerTokenAuthenticator::new(Arc::new(TokenFn(|t: &str| {
                assert_eq!(t, "token");
                Ok(Some(Response { user: user("user") }))
            })));
            let mut h = headers(&[("authorization", v)]);
            assert!(run(&auth, &mut h).await.unwrap().is_some(), "{v}");
        }
    }

    // TestAuthenticateRequestTokenInvalid: !ok without an error becomes `invalid bearer token`.
    #[tokio::test]
    async fn bearer_token_invalid_gets_the_default_error() {
        let auth = BearerTokenAuthenticator::new(Arc::new(TokenFn(|_t: &str| Ok(None))));
        let mut h = headers(&[("authorization", "Bearer token")]);
        assert_eq!(
            run(&auth, &mut h).await.unwrap_err(),
            "invalid bearer token"
        );
        // not removed on failure
        assert!(h.get("authorization").is_some());
    }

    // TestAuthenticateRequestTokenInvalidCustomError / TokenError
    #[tokio::test]
    async fn bearer_token_keeps_a_custom_error() {
        let auth =
            BearerTokenAuthenticator::new(Arc::new(TokenFn(|_t: &str| Err("custom".to_string()))));
        let mut h = headers(&[("authorization", "Bearer token")]);
        assert_eq!(run(&auth, &mut h).await.unwrap_err(), "custom");
    }

    // TestAuthenticateRequestBadValue
    #[tokio::test]
    async fn bearer_token_bad_values_are_not_authenticated() {
        for v in [
            None,
            Some("Bearer"),
            Some("bear token"),
            Some("Bearer: token"),
        ] {
            let auth = BearerTokenAuthenticator::new(Arc::new(TokenFn(|_t: &str| {
                panic!("authentication should not have been called")
            })));
            let mut h = match v {
                Some(v) => headers(&[("authorization", v)]),
                None => HeaderMap::new(),
            };
            assert!(run(&auth, &mut h).await.unwrap().is_none(), "{v:?}");
        }
    }

    // bearertoken.go: the scheme is matched case-insensitively.
    #[tokio::test]
    async fn bearer_scheme_is_case_insensitive() {
        let auth = BearerTokenAuthenticator::new(ok_user("u"));
        let mut h = headers(&[("authorization", "bEaReR token")]);
        assert!(run(&auth, &mut h).await.unwrap().is_some());
    }

    // ---- websocket protocol_test.go

    fn ws(protocols: &str) -> HeaderMap {
        headers(&[
            ("connection", "upgrade"),
            ("upgrade", "websocket"),
            ("sec-websocket-protocol", protocols),
        ])
    }

    #[tokio::test]
    async fn websocket_token_protocol_authenticates_and_is_removed() {
        let auth = WebsocketProtocolAuthenticator::new(Arc::new(TokenFn(|t: &str| {
            assert_eq!(t, "token");
            Ok(Some(Response { user: user("user") }))
        })));
        let mut h = ws("base64url.bearer.authorization.k8s.io.dG9rZW4,dummy");
        assert!(run(&auth, &mut h).await.unwrap().is_some());
        assert_eq!(h.get("sec-websocket-protocol").unwrap(), "dummy");
    }

    // TestAuthenticateRequestMultipleConnectionHeaders
    #[tokio::test]
    async fn websocket_accepts_upgrade_among_several_connection_headers() {
        let auth = WebsocketProtocolAuthenticator::new(ok_user("user"));
        let mut h = headers(&[
            ("connection", "not"),
            ("connection", "upgrade"),
            ("upgrade", "websocket"),
            (
                "sec-websocket-protocol",
                "base64url.bearer.authorization.k8s.io.dG9rZW4,dummy",
            ),
        ]);
        assert!(run(&auth, &mut h).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn websocket_rejects_malformed_protocols() {
        let auth = WebsocketProtocolAuthenticator::new(ok_user("user"));
        let cases = [
            (
                "base64url.bearer.authorization.k8s.io.dG9rZW4,base64url.bearer.authorization.k8s.io.dG9rZW4",
                "multiple base64.bearer.authorization tokens specified",
            ),
            (
                "base64url.bearer.authorization.k8s.io.!!!,dummy",
                "invalid base64.bearer.authorization token encoding",
            ),
            (
                "base64url.bearer.authorization.k8s.io.dG9rZW4",
                "missing additional subprotocol",
            ),
        ];
        for (protocols, want) in cases {
            let mut h = ws(protocols);
            assert_eq!(run(&auth, &mut h).await.unwrap_err(), want, "{protocols}");
        }
        // not a websocket request: ignored
        let mut h = headers(&[(
            "sec-websocket-protocol",
            "base64url.bearer.authorization.k8s.io.dG9rZW4,dummy",
        )]);
        assert!(run(&auth, &mut h).await.unwrap().is_none());
        // no token protocol: not authenticated, no error
        let mut h = ws("dummy");
        assert!(run(&auth, &mut h).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn websocket_invalid_token_gets_the_default_error() {
        let auth = WebsocketProtocolAuthenticator::new(Arc::new(TokenFn(|_t: &str| Ok(None))));
        let mut h = ws("base64url.bearer.authorization.k8s.io.dG9rZW4,dummy");
        assert_eq!(
            run(&auth, &mut h).await.unwrap_err(),
            "invalid bearer token"
        );
    }

    // ---- anonymous_test.go

    #[tokio::test]
    async fn anonymous_authenticates_everything_unrestricted() {
        let a = AnonymousAuthenticator {
            allowed_paths: vec![],
        };
        let r = run(&a, &mut HeaderMap::new()).await.unwrap().unwrap();
        assert_eq!(r.user.username, ANONYMOUS_USER);
        assert_eq!(r.user.groups, vec![ALL_UNAUTHENTICATED.to_string()]);
    }

    #[tokio::test]
    async fn anonymous_restricted_to_condition_paths() {
        let a = AnonymousAuthenticator {
            allowed_paths: vec!["/healthz".to_string()],
        };
        let mut h = HeaderMap::new();
        let mut req = AuthRequest {
            headers: &mut h,
            path: "/healthz",
            peer_certs: &[],
        };
        assert!(a.authenticate_request(&mut req).await.unwrap().is_some());
        req.path = "/metrics";
        assert!(a.authenticate_request(&mut req).await.unwrap().is_none());
    }

    // ---- union.go

    #[tokio::test]
    async fn union_returns_the_first_success() {
        let u = UnionAuthenticator::new(vec![
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Ok(None))),
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| {
                Ok(Some(Response { user: user("b") }))
            })),
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| panic!("not reached"))),
        ]);
        let r = run(&*u, &mut HeaderMap::new()).await.unwrap().unwrap();
        assert_eq!(r.user.username, "b");
    }

    // union.go:55-62: errors are collected and the chain continues...
    #[tokio::test]
    async fn union_continues_past_errors_and_aggregates_them() {
        let u = UnionAuthenticator::new(vec![
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Err("e1".to_string()))),
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Err("e2".to_string()))),
        ]);
        let err = run(&*u, &mut HeaderMap::new()).await.unwrap_err();
        assert_eq!(err, "[e1, e2]");

        let u = UnionAuthenticator::new(vec![
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Err("e1".to_string()))),
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| {
                Ok(Some(Response { user: user("b") }))
            })),
        ]);
        assert!(run(&*u, &mut HeaderMap::new()).await.unwrap().is_some());
    }

    // ...except for NewFailOnError, whose first error short-circuits.
    #[tokio::test]
    async fn union_fail_on_error_stops_at_the_first_error() {
        let u = UnionAuthenticator::new_fail_on_error(vec![
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Err("e1".to_string()))),
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| {
                Ok(Some(Response { user: user("b") }))
            })),
        ]);
        assert_eq!(run(&*u, &mut HeaderMap::new()).await.unwrap_err(), "e1");
    }

    // utilerrors.NewAggregate(nil) is nil: nothing authenticated, no error.
    #[tokio::test]
    async fn union_with_no_opinion_and_no_errors_is_not_authenticated() {
        let u = UnionAuthenticator::new(vec![
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Ok(None))),
            Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Ok(None))),
        ]);
        assert!(run(&*u, &mut HeaderMap::new()).await.unwrap().is_none());
    }

    // ---- authenticated_group_adder.go

    #[tokio::test]
    async fn group_adder_adds_system_authenticated() {
        let mut u = user("bob");
        u.groups = vec!["g".into()];
        u.uid = "7".into();
        let inner = ReqFn(move |_r: &mut AuthRequest<'_>| Ok(Some(Response { user: u.clone() })));
        let a = AuthenticatedGroupAdder::new(Arc::new(inner));
        let r = run(&a, &mut HeaderMap::new()).await.unwrap().unwrap();
        assert_eq!(
            r.user.groups,
            vec!["g".to_string(), ALL_AUTHENTICATED.to_string()]
        );
        assert_eq!(r.user.uid, "7");
    }

    #[tokio::test]
    async fn group_adder_leaves_anonymous_and_already_grouped_users_alone() {
        for (name, groups) in [
            (ANONYMOUS_USER, vec![]),
            ("bob", vec![ALL_AUTHENTICATED.to_string()]),
            ("bob", vec![ALL_UNAUTHENTICATED.to_string()]),
        ] {
            let mut u = user(name);
            u.groups = groups.clone();
            let inner =
                ReqFn(move |_r: &mut AuthRequest<'_>| Ok(Some(Response { user: u.clone() })));
            let a = AuthenticatedGroupAdder::new(Arc::new(inner));
            let r = run(&a, &mut HeaderMap::new()).await.unwrap().unwrap();
            assert_eq!(r.user.groups, groups, "{name}");
        }
    }

    #[tokio::test]
    async fn group_adder_passes_failures_through() {
        let a = AuthenticatedGroupAdder::new(Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| {
            Err("boom".to_string())
        })));
        assert_eq!(run(&a, &mut HeaderMap::new()).await.unwrap_err(), "boom");
        let a = AuthenticatedGroupAdder::new(Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| Ok(None))));
        assert!(run(&a, &mut HeaderMap::new()).await.unwrap().is_none());
    }

    // ---- requestheader_test.go / authentication_test.go TestAuthenticateRequestClearHeaders

    fn rh(
        name: &[&str],
        uid: &[&str],
        group: &[&str],
        extra: &[&str],
    ) -> RequestHeaderAuthenticator {
        let v = |s: &[&str]| s.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        RequestHeaderAuthenticator::new(Arc::new(RwLock::new(RequestHeaderSettings {
            username_headers: v(name),
            uid_headers: v(uid),
            group_headers: v(group),
            extra_header_prefixes: v(extra),
            allowed_client_names: vec![],
        })))
    }

    #[tokio::test]
    async fn request_header_builds_the_user_and_clears_the_headers() {
        let a = rh(
            &["X-Remote-User"],
            &["X-Remote-Uid"],
            &["X-Remote-Group"],
            &["X-Remote-Extra-"],
        );
        let mut h = headers(&[
            ("x-remote-user", "Bob"),
            ("x-remote-uid", "bobs-uid"),
            ("x-remote-group", "one"),
            ("x-remote-group", "two"),
            ("x-remote-extra-Key1", "alfa"),
            ("x-remote-extra-key1", "bravo"),
            ("x-remote-extra-a%2Fb", "c"),
            ("x-unrelated", "keep"),
        ]);
        let r = run(&a, &mut h).await.unwrap().unwrap();
        assert_eq!(r.user.username, "Bob");
        assert_eq!(r.user.uid, "bobs-uid");
        assert_eq!(r.user.groups, vec!["one".to_string(), "two".to_string()]);
        assert_eq!(
            r.user.extra.get("key1").unwrap(),
            &vec!["alfa".to_string(), "bravo".to_string()]
        );
        // `unescapeExtraKey`
        assert_eq!(r.user.extra.get("a/b").unwrap(), &vec!["c".to_string()]);
        let remaining: Vec<_> = h.keys().map(|k| k.as_str().to_string()).collect();
        assert_eq!(remaining, vec!["x-unrelated".to_string()]);
    }

    #[tokio::test]
    async fn request_header_without_a_user_is_not_authenticated_and_clears_nothing() {
        let a = rh(&["X-Remote-User"], &[], &["X-Remote-Group"], &[]);
        let mut h = headers(&[("x-remote-group", "one")]);
        assert!(run(&a, &mut h).await.unwrap().is_none());
        assert!(h.get("x-remote-group").is_some());
    }

    // "user first match": the first non-empty header wins.
    #[tokio::test]
    async fn request_header_first_non_empty_username_header_wins() {
        let a = rh(&["X-Remote-User", "A-Second"], &[], &[], &[]);
        let mut h = headers(&[("x-remote-user", ""), ("a-second", "Second")]);
        let r = run(&a, &mut h).await.unwrap().unwrap();
        assert_eq!(r.user.username, "Second");
    }

    // "custom config but request contains standard headers": clearing only
    // touches the configured headers (the filter clears the standard ones).
    #[test]
    fn clear_authentication_headers_is_case_insensitive_on_prefixes() {
        let mut h = headers(&[
            ("x-remote-user", "Bob"),
            ("X-Remote-extra-1-key1", "a"),
            ("x-group-remote", "keep"),
        ]);
        clear_authentication_headers(
            &mut h,
            &["x-REMOTE-user".to_string()],
            &[],
            &[],
            &["X-Remote-Extra-1-".to_string()],
        );
        let remaining: Vec<_> = h.keys().map(|k| k.as_str().to_string()).collect();
        assert_eq!(remaining, vec!["x-group-remote".to_string()]);
    }

    // ---- x509_test.go

    struct Pki {
        ca_pem: String,
        ca: rcgen::Certificate,
        ca_key: rcgen::KeyPair,
    }

    fn pki(name: &str) -> Pki {
        let mut p = rcgen::CertificateParams::default();
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        p.distinguished_name = rcgen::DistinguishedName::new();
        p.distinguished_name.push(rcgen::DnType::CommonName, name);
        p.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::DigitalSignature,
        ];
        let key = rcgen::KeyPair::generate().unwrap();
        let ca = p.self_signed(&key).unwrap();
        Pki {
            ca_pem: ca.pem(),
            ca,
            ca_key: key,
        }
    }

    fn client_cert(pki: &Pki, cn: &str, orgs: &[&str], eku_client: bool) -> Vec<u8> {
        let mut p = rcgen::CertificateParams::default();
        p.distinguished_name = rcgen::DistinguishedName::new();
        if !cn.is_empty() {
            p.distinguished_name.push(rcgen::DnType::CommonName, cn);
        }
        for o in orgs {
            p.distinguished_name
                .push(rcgen::DnType::OrganizationName, *o);
        }
        p.extended_key_usages = vec![if eku_client {
            rcgen::ExtendedKeyUsagePurpose::ClientAuth
        } else {
            rcgen::ExtendedKeyUsagePurpose::ServerAuth
        }];
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = p.signed_by(&key, &pki.ca, &pki.ca_key).unwrap();
        cert.der().to_vec()
    }

    fn ca_content(pki: &Pki) -> Arc<CaContent> {
        let c = CaContent::new("test");
        assert!(c.set_pem(pki.ca_pem.as_bytes()).unwrap());
        c
    }

    async fn run_certs(a: &dyn RequestAuthenticator, chain: &[Vec<u8>]) -> AuthResult {
        let mut h = HeaderMap::new();
        a.authenticate_request(&mut AuthRequest {
            headers: &mut h,
            path: "/metrics",
            peer_certs: chain,
        })
        .await
    }

    // x509.go:279-302 CommonNameUserConversion: CN -> name, O -> groups,
    // credential id is the SHA-256 of the DER.
    #[tokio::test]
    async fn x509_maps_cn_and_organizations() {
        let p = pki("ca");
        let der = client_cert(&p, "system:kube-scheduler", &["g1"], true);
        let a = X509Authenticator::new_dynamic(ca_content(&p));
        let r = run_certs(&a, std::slice::from_ref(&der))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.user.username, "system:kube-scheduler");
        assert_eq!(r.user.groups, vec!["g1".to_string()]); // rcgen keeps one value per DN type
        use sha2::{Digest, Sha256};
        let fp: String = Sha256::digest(&der)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(
            r.user.extra.get(CREDENTIAL_ID_KEY).unwrap(),
            &vec![format!("X509SHA256={fp}")]
        );
    }

    // x509.go:281-283: an empty CommonName does not authenticate (and is no error).
    #[tokio::test]
    async fn x509_empty_common_name_is_not_authenticated() {
        let p = pki("ca");
        let der = client_cert(&p, "", &["o"], true);
        let a = X509Authenticator::new_dynamic(ca_content(&p));
        assert!(run_certs(&a, &[der]).await.unwrap().is_none());
    }

    // x509.go:231-233, 237-240
    #[tokio::test]
    async fn x509_without_certs_or_without_a_bundle_is_not_authenticated() {
        let p = pki("ca");
        let der = client_cert(&p, "u", &[], true);
        let a = X509Authenticator::new_dynamic(ca_content(&p));
        assert!(run_certs(&a, &[]).await.unwrap().is_none());
        let empty = X509Authenticator::new_dynamic(CaContent::new("empty"));
        assert!(run_certs(&empty, &[der]).await.unwrap().is_none());
    }

    // x509.go:248-250: a chain that does not verify is an error.
    #[tokio::test]
    async fn x509_untrusted_or_wrong_usage_certs_are_errors() {
        let trusted = pki("ca");
        let other = pki("other");
        let a = X509Authenticator::new_dynamic(ca_content(&trusted));
        let der = client_cert(&other, "u", &[], true);
        assert!(run_certs(&a, &[der]).await.is_err());
        // DefaultVerifyOptions requires ExtKeyUsageClientAuth
        let der = client_cert(&trusted, "u", &[], false);
        assert!(run_certs(&a, &[der]).await.is_err());
    }

    // x509.go:257-269 verifySubject (request-header allowed names), wrapping the
    // inner authenticator.
    #[tokio::test]
    async fn x509_verifier_enforces_allowed_common_names_then_delegates() {
        let p = pki("ca");
        let inner: Arc<dyn RequestAuthenticator> = Arc::new(ReqFn(|_r: &mut AuthRequest<'_>| {
            Ok(Some(Response {
                user: user("from-header"),
            }))
        }));
        let allowed = Arc::new(|| vec!["front-proxy".to_string()]);
        let a = X509Authenticator::new_verifier(ca_content(&p), inner, allowed);
        let ok = client_cert(&p, "front-proxy", &[], true);
        assert_eq!(
            run_certs(&a, &[ok]).await.unwrap().unwrap().user.username,
            "from-header"
        );
        let bad = client_cert(&p, "someone-else", &[], true);
        assert_eq!(
            run_certs(&a, &[bad]).await.unwrap_err(),
            "x509: subject with cn=someone-else is not in the allowed list"
        );
    }

    // the dynamic part: swapping the bundle changes the verdict.
    #[tokio::test]
    async fn ca_content_reload_changes_verification() {
        let a_pki = pki("a");
        let b_pki = pki("b");
        let content = ca_content(&a_pki);
        let a = X509Authenticator::new_dynamic(content.clone());
        let der = client_cert(&b_pki, "u", &[], true);
        assert!(run_certs(&a, std::slice::from_ref(&der)).await.is_err());
        assert!(content.set_pem(b_pki.ca_pem.as_bytes()).unwrap());
        assert!(run_certs(&a, &[der]).await.unwrap().is_some());
        // same bytes again: hasCAChanged is false
        assert!(!content.set_pem(b_pki.ca_pem.as_bytes()).unwrap());
    }

    #[test]
    fn ca_content_rejects_an_empty_or_garbage_bundle() {
        let c = CaContent::new("x");
        assert!(c.set_pem(b"").is_err());
        assert!(c.set_pem(b"not pem").is_err());
        assert!(c.current_pem().is_none());
    }

    #[test]
    fn ca_content_from_file_errors_when_unreadable() {
        assert!(CaContent::from_file("x", "/nonexistent/ca.crt").is_err());
        let dir = tempfile::tempdir().unwrap();
        let p = pki("ca");
        let path = dir.path().join("ca.crt");
        std::fs::write(&path, &p.ca_pem).unwrap();
        let c = CaContent::from_file("x", path.to_str().unwrap()).unwrap();
        assert_eq!(c.current_pem().unwrap(), p.ca_pem.as_bytes());
    }

    // ---- cache: cached_token_authenticator_test.go TestCachedTokenAuthenticator

    struct Counting {
        calls: Mutex<Vec<String>>,
        ok: Mutex<bool>,
        err: Mutex<Option<String>>,
    }
    impl Counting {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(vec![]),
                ok: Mutex::new(false),
                err: Mutex::new(None),
            })
        }
        fn take(&self) -> Vec<String> {
            std::mem::take(&mut *self.calls.lock().unwrap())
        }
    }
    #[async_trait]
    impl TokenAuthenticator for Counting {
        async fn authenticate_token(&self, token: &str) -> AuthResult {
            self.calls.lock().unwrap().push(token.to_string());
            if let Some(e) = self.err.lock().unwrap().clone() {
                return Err(e);
            }
            if *self.ok.lock().unwrap() {
                Ok(Some(Response {
                    user: user(&token.replace("token", "")),
                }))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cache_caches_successes_for_the_ttl_but_not_failures_with_zero_failure_ttl() {
        let inner = Counting::new();
        let a = CachedTokenAuthenticator::new(
            inner.clone(),
            true,
            Duration::from_secs(60),
            Duration::ZERO,
        );

        // failures are not cached (failureTTL 0)
        for _ in 0..2 {
            for t in ["bad1", "bad2"] {
                let _ = a.authenticate_token(t).await;
            }
        }
        assert_eq!(inner.take(), vec!["bad1", "bad2", "bad1", "bad2"]);

        *inner.ok.lock().unwrap() = true;
        for t in ["usertoken1", "usertoken2"] {
            assert!(a.authenticate_token(t).await.unwrap().is_some());
        }
        assert_eq!(inner.take(), vec!["usertoken1", "usertoken2"]);

        // backend now fails, cache still answers
        *inner.ok.lock().unwrap() = false;
        for (t, want) in [("usertoken1", "user1"), ("usertoken2", "user2")] {
            assert_eq!(
                a.authenticate_token(t)
                    .await
                    .unwrap()
                    .unwrap()
                    .user
                    .username,
                want
            );
        }
        assert!(inner.take().is_empty());

        tokio::time::advance(Duration::from_secs(120)).await;
        let _ = a.authenticate_token("usertoken1").await;
        assert_eq!(inner.take(), vec!["usertoken1"]);
    }

    // `case !record.ok && a.failureTTL > 0`
    #[tokio::test(start_paused = true)]
    async fn cache_caches_failures_for_the_failure_ttl() {
        let inner = Counting::new();
        let a = CachedTokenAuthenticator::new(
            inner.clone(),
            false,
            Duration::from_secs(60),
            Duration::from_secs(10),
        );
        assert!(a.authenticate_token("bad").await.unwrap().is_none());
        assert!(a.authenticate_token("bad").await.unwrap().is_none());
        assert_eq!(inner.take(), vec!["bad"]);
        tokio::time::advance(Duration::from_secs(11)).await;
        assert!(a.authenticate_token("bad").await.unwrap().is_none());
        assert_eq!(inner.take(), vec!["bad"]);
    }

    // `if !a.cacheErrs && record.err != nil { return }`: errors are not cached.
    #[tokio::test(start_paused = true)]
    async fn cache_does_not_cache_errors_unless_asked() {
        let inner = Counting::new();
        *inner.err.lock().unwrap() = Some("boom".into());
        let a = CachedTokenAuthenticator::new(
            inner.clone(),
            false,
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        assert_eq!(a.authenticate_token("t").await.unwrap_err(), "boom");
        assert_eq!(a.authenticate_token("t").await.unwrap_err(), "boom");
        assert_eq!(inner.take(), vec!["t", "t"]);
    }

    // `A ttl of 0 bypasses the cache.`
    #[tokio::test(start_paused = true)]
    async fn cache_with_zero_ttl_bypasses_the_cache() {
        let inner = Counting::new();
        *inner.ok.lock().unwrap() = true;
        let a = CachedTokenAuthenticator::new(inner.clone(), false, Duration::ZERO, Duration::ZERO);
        a.authenticate_token("u1").await.unwrap();
        a.authenticate_token("u1").await.unwrap();
        assert_eq!(inner.take(), vec!["u1", "u1"]);
    }

    // TestSharedLookup "actually shared": concurrent callers share one lookup.
    #[tokio::test]
    async fn cache_shares_concurrent_lookups_of_the_same_token() {
        struct Slow(AtomicUsize);
        #[async_trait]
        impl TokenAuthenticator for Slow {
            async fn authenticate_token(&self, _t: &str) -> AuthResult {
                self.0.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(Some(Response {
                    user: user("chewbacca"),
                }))
            }
        }
        let slow = Arc::new(Slow(AtomicUsize::new(0)));
        let a = Arc::new(CachedTokenAuthenticator::new(
            slow.clone(),
            true,
            Duration::from_secs(60),
            Duration::ZERO,
        ));
        let mut tasks = vec![];
        for _ in 0..10 {
            let a = a.clone();
            tasks.push(tokio::spawn(async move { a.authenticate_token("").await }));
        }
        for t in tasks {
            assert_eq!(
                t.await.unwrap().unwrap().unwrap().user.username,
                "chewbacca"
            );
        }
        assert!(
            slow.0.load(Ordering::SeqCst) <= 3,
            "{}",
            slow.0.load(Ordering::SeqCst)
        );
    }

    // TestSharedLookup "lookup panics": errAuthnCrash.
    #[tokio::test]
    async fn cache_turns_a_panicking_lookup_into_an_internal_error() {
        let a = CachedTokenAuthenticator::new(
            Arc::new(TokenFn(|_t: &str| -> AuthResult { panic!("uh oh") })),
            true,
            Duration::from_secs(60),
            Duration::ZERO,
        );
        assert_eq!(
            a.authenticate_token("").await.unwrap_err(),
            "Internal error occurred: authentication failed unexpectedly"
        );
    }

    // ---- webhook_v1_test.go TestV1WebhookTokenAuthenticator

    struct FakeReview {
        responses: Mutex<Vec<Result<serde_json::Value, ReviewError>>>,
        requests: Mutex<Vec<(String, serde_json::Value)>>,
    }
    impl FakeReview {
        fn new(responses: Vec<Result<serde_json::Value, ReviewError>>) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new(responses),
                requests: Mutex::new(vec![]),
            })
        }
    }
    #[async_trait]
    impl ReviewClient for FakeReview {
        async fn create(
            &self,
            path: &str,
            review: &serde_json::Value,
        ) -> Result<serde_json::Value, ReviewError> {
            self.requests
                .lock()
                .unwrap()
                .push((path.to_string(), review.clone()));
            self.responses.lock().unwrap().remove(0)
        }
    }

    fn fast_backoff() -> Backoff {
        Backoff {
            duration: Duration::from_millis(1),
            factor: 1.0,
            jitter: 0.0,
            steps: 3,
        }
    }

    fn status(v: serde_json::Value) -> Result<serde_json::Value, ReviewError> {
        Ok(
            serde_json::json!({"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status": v}),
        )
    }

    #[tokio::test]
    async fn webhook_posts_a_tokenreview_and_maps_the_user() {
        let c = FakeReview::new(vec![status(serde_json::json!({
            "authenticated": true,
            "user": {"username":"somebody","uid":"u1","groups":["a","b"],"extra":{"k":["v"]}}
        }))]);
        let a = WebhookTokenAuthenticator::new(c.clone(), fast_backoff(), Duration::from_secs(10));
        let r = a.authenticate_token("tok").await.unwrap().unwrap();
        assert_eq!(r.user.username, "somebody");
        assert_eq!(r.user.uid, "u1");
        assert_eq!(r.user.groups, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(r.user.extra.get("k").unwrap(), &vec!["v".to_string()]);
        let reqs = c.requests.lock().unwrap();
        assert_eq!(reqs[0].0, "/apis/authentication.k8s.io/v1/tokenreviews");
        assert_eq!(reqs[0].1["spec"]["token"], "tok");
    }

    #[tokio::test]
    async fn webhook_unauthenticated_is_not_ok_and_status_error_becomes_the_error() {
        let c = FakeReview::new(vec![status(serde_json::json!({"authenticated": false}))]);
        let a = WebhookTokenAuthenticator::new(c, fast_backoff(), Duration::from_secs(10));
        assert!(a.authenticate_token("t").await.unwrap().is_none());

        let c = FakeReview::new(vec![status(
            serde_json::json!({"authenticated": false, "error": "no good"}),
        )]);
        let a = WebhookTokenAuthenticator::new(c, fast_backoff(), Duration::from_secs(10));
        assert_eq!(a.authenticate_token("t").await.unwrap_err(), "no good");
    }

    // TestV1WebhookCacheAndRetry shape: a 500 is retried, a 403 is not.
    #[tokio::test]
    async fn webhook_retries_transient_errors_only() {
        let e500 = ReviewError {
            status_code: 500,
            reason: "InternalError".into(),
            message: "oops".into(),
            connection_reset: false,
            http2_connection_lost: false,
            suggests_client_delay: false,
        };
        let c = FakeReview::new(vec![
            Err(e500.clone()),
            status(serde_json::json!({"authenticated": true, "user": {"username":"u"}})),
        ]);
        let a = WebhookTokenAuthenticator::new(c.clone(), fast_backoff(), Duration::from_secs(10));
        assert!(a.authenticate_token("t").await.unwrap().is_some());
        assert_eq!(c.requests.lock().unwrap().len(), 2);

        let mut e403 = e500;
        e403.status_code = 403;
        e403.reason = "Forbidden".into();
        e403.message = "denied".into();
        let c = FakeReview::new(vec![Err(e403)]);
        let a = WebhookTokenAuthenticator::new(c.clone(), fast_backoff(), Duration::from_secs(10));
        assert_eq!(a.authenticate_token("t").await.unwrap_err(), "denied");
        assert_eq!(c.requests.lock().unwrap().len(), 1);
    }

    // ---- delegating.go New()

    fn cfg() -> DelegatingAuthenticatorConfig {
        DelegatingAuthenticatorConfig {
            anonymous: Some(AnonymousAuthConfig {
                enabled: true,
                condition_paths: vec![],
            }),
            token_access_review_client: None,
            token_access_review_timeout: Duration::from_secs(10),
            webhook_retry_backoff: Some(fast_backoff()),
            cache_ttl: Duration::from_secs(10),
            client_certificate_ca: None,
            request_header_config: None,
        }
    }

    // delegating.go:121-124: nothing but anonymous
    #[tokio::test]
    async fn factory_with_only_anonymous_returns_the_anonymous_authenticator() {
        let a = cfg().new().unwrap();
        let r = run(&*a, &mut HeaderMap::new()).await.unwrap().unwrap();
        assert_eq!(r.user.username, ANONYMOUS_USER);
    }

    // delegating.go:125
    #[test]
    fn factory_without_any_method_fails() {
        let mut c = cfg();
        c.anonymous = None;
        assert_eq!(
            c.new().err().unwrap(),
            "no authentication method configured"
        );
        let mut c = cfg();
        c.anonymous = Some(AnonymousAuthConfig::default());
        assert!(c.new().is_err());
    }

    // delegating.go:101-102
    #[test]
    fn factory_with_a_token_client_needs_a_retry_backoff() {
        let mut c = cfg();
        c.token_access_review_client = Some(FakeReview::new(vec![]));
        c.webhook_retry_backoff = None;
        assert_eq!(
            c.new().err().unwrap(),
            "retry backoff parameters for delegating authentication webhook has not been specified"
        );
    }

    // delegating.go:127-130: authenticated users get system:authenticated, and
    // anonymous is the fallback behind a FailOnError union.
    #[tokio::test]
    async fn factory_token_chain_adds_group_and_falls_back_to_anonymous() {
        let mut c = cfg();
        c.token_access_review_client = Some(FakeReview::new(vec![
            status(serde_json::json!({"authenticated": true, "user": {"username":"sa"}})),
            status(serde_json::json!({"authenticated": false})),
        ]));
        let a = c.new().unwrap();
        let mut h = headers(&[("authorization", "Bearer good")]);
        let r = run(&*a, &mut h).await.unwrap().unwrap();
        assert_eq!(r.user.username, "sa");
        assert_eq!(r.user.groups, vec![ALL_AUTHENTICATED.to_string()]);

        // no credentials at all: anonymous
        let r = run(&*a, &mut HeaderMap::new()).await.unwrap().unwrap();
        assert_eq!(r.user.username, ANONYMOUS_USER);
        // a rejected token is an error (`invalid bearer token`), NOT anonymous
        let mut h = headers(&[("authorization", "Bearer bad")]);
        assert_eq!(run(&*a, &mut h).await.unwrap_err(), "invalid bearer token");
    }

    // delegating.go:76-93: client cert chain is authenticated through the CA provider.
    #[tokio::test]
    async fn factory_client_cert_chain() {
        let p = pki("ca");
        let mut c = cfg();
        c.client_certificate_ca = Some(ca_content(&p));
        let a = c.new().unwrap();
        let der = client_cert(&p, "admin", &["system:masters"], true);
        let r = run_certs(&*a, &[der]).await.unwrap().unwrap();
        assert_eq!(r.user.username, "admin");
        assert_eq!(
            r.user.groups,
            vec!["system:masters".to_string(), ALL_AUTHENTICATED.to_string()]
        );
    }

    // delegating.go:66-80: front-proxy first; headers are only trusted from a
    // client cert that verifies against the request-header CA.
    #[tokio::test]
    async fn factory_request_header_requires_a_verified_front_proxy_cert() {
        let p = pki("front-proxy-ca");
        let mut c = cfg();
        let settings = Arc::new(RwLock::new(RequestHeaderSettings {
            username_headers: vec!["x-remote-user".into()],
            group_headers: vec!["x-remote-group".into()],
            extra_header_prefixes: vec!["x-remote-extra-".into()],
            ..Default::default()
        }));
        c.request_header_config = Some(RequestHeaderConfig {
            ca: ca_content(&p),
            settings,
        });
        let a = c.new().unwrap();
        let der = client_cert(&p, "front-proxy-client", &[], true);
        let mut h = headers(&[("x-remote-user", "alice"), ("x-remote-group", "devs")]);
        let r = a
            .authenticate_request(&mut AuthRequest {
                headers: &mut h,
                path: "/metrics",
                peer_certs: &[der],
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.user.username, "alice");
        // without a cert the headers are NOT trusted: anonymous
        let mut h = headers(&[("x-remote-user", "alice")]);
        let r = run(&*a, &mut h).await.unwrap().unwrap();
        assert_eq!(r.user.username, ANONYMOUS_USER);
    }
}
