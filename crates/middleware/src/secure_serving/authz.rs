//! Delegating request authorization: the authorizer chain
//! `DelegatingAuthorizationOptions.toAuthorizer` builds, and its parts.
//!
//! Upstream sources (release-1.35, all under `staging/src/k8s.io/apiserver/`):
//! * `pkg/server/options/authorization.go:177-209` (`toAuthorizer`)
//! * `pkg/authorization/path/path.go:30-80` (`path.NewAuthorizer`)
//! * `pkg/authorization/authorizerfactory/builtin.go:68-96` (privileged groups)
//!   and `delegating.go:46-60` (`DelegatingAuthorizerConfig.New`)
//! * `plugin/pkg/authorizer/webhook/webhook.go:171-319,426-436,535-547`
//!   (`WebhookAuthorizer.Authorize`, `shouldCache`)
//! * `pkg/authorization/union/union.go` (reused: [`UnionAuthorizer`])
//!
//! Not ported (unreachable from the delegating authorizer): `matchConditions`
//! (`NewFromInterface` passes none) and the authorizer metrics.

use super::webhook::{default_should_retry, with_exponential_backoff, Backoff, ReviewClient};
use async_trait::async_trait;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, Opinion, RequestAttributes};
use rusternetes_common::error::Result;
use std::sync::Arc;
use std::time::Duration;

/// `maxControlledAttrCacheSize` (webhook.go:56).
pub const MAX_CONTROLLED_ATTR_CACHE_SIZE: usize = 10000;

/// `path.NewAuthorizer`: accepts the given paths; each is either a fully
/// matching path or ends in `*` for a prefix match; a leading `/` is optional.
/// Resource requests get no opinion.
pub fn new_path_authorizer(_always_allow_paths: &[String]) -> std::result::Result<Arc<dyn Authorizer>, String> {
    unimplemented!("new_path_authorizer")
}

/// `webhook.shouldCache` (webhook.go:537-547).
pub fn should_cache(_attr: &RequestAttributes) -> bool {
    unimplemented!("should_cache")
}

/// The `SubjectAccessReviewSpec` `WebhookAuthorizer.Authorize` builds
/// (webhook.go:171-190, `resourceAttributesFrom`).
pub fn subject_access_review_spec(_attr: &RequestAttributes) -> serde_json::Value {
    unimplemented!("subject_access_review_spec")
}

/// `webhook.WebhookAuthorizer` as built by `DelegatingAuthorizerConfig.New`
/// (`decisionOnError` = `DecisionNoOpinion`).
pub struct SubjectAccessReviewAuthorizer {
    _client: Arc<dyn ReviewClient>,
    _authorized_ttl: Duration,
    _unauthorized_ttl: Duration,
    _retry_backoff: Backoff,
}

impl SubjectAccessReviewAuthorizer {
    /// `webhook.NewFromInterface` with an 8192-entry response cache.
    pub fn new(
        client: Arc<dyn ReviewClient>,
        authorized_ttl: Duration,
        unauthorized_ttl: Duration,
        retry_backoff: Backoff,
    ) -> Self {
        Self {
            _client: client,
            _authorized_ttl: authorized_ttl,
            _unauthorized_ttl: unauthorized_ttl,
            _retry_backoff: retry_backoff,
        }
    }
}

#[async_trait]
impl Authorizer for SubjectAccessReviewAuthorizer {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        // The deny/no-opinion distinction lives in `authorize_opinion`; this
        // is the two-valued projection `UnionAuthorizer` never calls.
        match self.authorize_opinion(attrs).await? {
            Opinion::Allow => Ok(Decision::Allow),
            Opinion::Deny(r) => Ok(Decision::Deny(r)),
            Opinion::NoOpinion { reason, .. } => Ok(Decision::Deny(reason)),
        }
    }

    async fn authorize_opinion(&self, _attrs: &RequestAttributes) -> Result<Opinion> {
        unimplemented!("SubjectAccessReviewAuthorizer::authorize_opinion")
    }

    async fn get_user_rules(
        &self,
        _user: &UserInfo,
        _namespace: &str,
    ) -> Result<(
        Vec<rusternetes_common::resources::ResourceRule>,
        Vec<rusternetes_common::resources::NonResourceRule>,
    )> {
        Ok((vec![], vec![]))
    }
}

/// `authorizerfactory.DelegatingAuthorizerConfig`.
pub struct DelegatingAuthorizerConfig {
    pub subject_access_review_client: Arc<dyn ReviewClient>,
    pub allow_cache_ttl: Duration,
    pub deny_cache_ttl: Duration,
    pub webhook_retry_backoff: Option<Backoff>,
}

impl DelegatingAuthorizerConfig {
    /// `DelegatingAuthorizerConfig.New` (delegating.go:46-60).
    pub fn new(&self) -> std::result::Result<Arc<dyn Authorizer>, String> {
        unimplemented!("DelegatingAuthorizerConfig::new")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secure_serving::webhook::ReviewError;
    use rusternetes_common::authz::UnionAuthorizer;
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn user(name: &str, groups: &[&str]) -> UserInfo {
        UserInfo {
            username: name.to_string(),
            uid: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            extra: HashMap::new(),
        }
    }

    fn non_resource(u: UserInfo, verb: &str, path: &str) -> RequestAttributes {
        RequestAttributes::new_non_resource(u, verb, path)
    }

    // ---- path_test.go TestNewAuthorizer

    #[tokio::test]
    async fn path_authorizer_matches_upstream_table() {
        struct Case {
            name: &'static str,
            excluded: Vec<&'static str>,
            allowed: Vec<&'static str>,
            no_opinion: Vec<&'static str>,
            want_err: bool,
        }
        let cases = vec![
            Case { name: "inner star", excluded: vec!["/foo*bar"], allowed: vec![], no_opinion: vec![], want_err: true },
            Case { name: "double star", excluded: vec!["/foo**"], allowed: vec![], no_opinion: vec![], want_err: true },
            Case { name: "empty", excluded: vec![], allowed: vec![], no_opinion: vec!["/"], want_err: false },
            Case { name: "slash", excluded: vec!["/"], allowed: vec!["/"], no_opinion: vec!["/foo", "//"], want_err: false },
            Case { name: "foo", excluded: vec!["/foo"], allowed: vec!["/foo", "foo"], no_opinion: vec!["/", "", "/bar", "/foo/", "/fooooo", "//foo"], want_err: false },
            Case { name: "foo slash", excluded: vec!["/foo/"], allowed: vec!["/foo/"], no_opinion: vec!["/", "", "/bar", "/foo", "/fooooo"], want_err: false },
            Case { name: "foo slash star", excluded: vec!["/foo/*"], allowed: vec!["/foo/", "/foo/bar/bla"], no_opinion: vec!["/", "", "/foo", "/bar", "/fooooo"], want_err: false },
            Case { name: "foo bar", excluded: vec!["/foo", "/bar"], allowed: vec!["/foo", "/bar"], no_opinion: vec!["/", "", "/foo/", "/bar/", "/fooooo"], want_err: false },
            Case { name: "foo star", excluded: vec!["/foo*"], allowed: vec!["/foo", "/foooo"], no_opinion: vec!["/", "", "/fo", "/bar"], want_err: false },
            Case { name: "star", excluded: vec!["/*"], allowed: vec!["/", "", "/foo", "/foooo"], no_opinion: vec![], want_err: false },
        ];
        for c in cases {
            let paths: Vec<String> = c.excluded.iter().map(|s| s.to_string()).collect();
            let a = match new_path_authorizer(&paths) {
                Ok(a) => {
                    assert!(!c.want_err, "{}: expected error", c.name);
                    a
                }
                Err(_) => {
                    assert!(c.want_err, "{}: unexpected error", c.name);
                    continue;
                }
            };
            for p in &c.allowed {
                let got = a
                    .authorize_opinion(&non_resource(user("u", &[]), "get", p))
                    .await
                    .unwrap();
                assert_eq!(got, Opinion::Allow, "{} {p:?}", c.name);
            }
            for p in &c.no_opinion {
                let got = a
                    .authorize_opinion(&non_resource(user("u", &[]), "get", p))
                    .await
                    .unwrap();
                assert!(matches!(got, Opinion::NoOpinion { .. }), "{} {p:?}: {got:?}", c.name);
            }
        }
    }

    // path.go:48-50: resource requests get no opinion even on an allowed path.
    #[tokio::test]
    async fn path_authorizer_ignores_resource_requests() {
        let a = new_path_authorizer(&["/*".to_string()]).unwrap();
        let attrs = RequestAttributes::new(user("u", &[]), "get", "pods");
        assert!(matches!(
            a.authorize_opinion(&attrs).await.unwrap(),
            Opinion::NoOpinion { .. }
        ));
    }

    #[test]
    fn path_authorizer_error_text() {
        assert_eq!(
            new_path_authorizer(&["/foo*bar".to_string()]).err().unwrap(),
            "only trailing * allowed in \"foo*bar\""
        );
    }

    // ---- SAR spec: webhook.go:171-190

    #[test]
    fn spec_for_a_non_resource_request() {
        let mut u = user("jane", &["a", "b"]);
        u.uid = "u1".into();
        u.extra.insert("k".into(), vec!["v".into()]);
        let spec = subject_access_review_spec(&non_resource(u, "get", "/metrics"));
        assert_eq!(
            spec,
            serde_json::json!({
                "user": "jane",
                "uid": "u1",
                "groups": ["a", "b"],
                "extra": {"k": ["v"]},
                "nonResourceAttributes": {"path": "/metrics", "verb": "get"}
            })
        );
    }

    #[test]
    fn spec_for_a_resource_request() {
        let attrs = RequestAttributes::new(user("jane", &[]), "get", "pods")
            .with_namespace("ns")
            .with_api_group("g")
            .with_api_version("v1")
            .with_name("n")
            .with_subresource("status");
        let spec = subject_access_review_spec(&attrs);
        assert_eq!(
            spec,
            serde_json::json!({
                "user": "jane",
                "resourceAttributes": {
                    "namespace": "ns", "verb": "get", "group": "g", "version": "v1",
                    "resource": "pods", "subresource": "status", "name": "n"
                }
            })
        );
    }

    // ---- shouldCache

    #[test]
    fn should_cache_skips_oversized_attributes() {
        let small = non_resource(user("u", &[]), "get", "/metrics");
        assert!(should_cache(&small));
        let big = non_resource(user("u", &[]), "get", &"p".repeat(10_000));
        assert!(!should_cache(&big));
    }

    // ---- TestV1WebhookCache: server errors retry, 4xx do not, results are cached

    struct Backend {
        calls: Mutex<usize>,
        allow: Mutex<bool>,
        status: Mutex<u16>,
        denied: Mutex<bool>,
    }
    impl Backend {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(0),
                allow: Mutex::new(false),
                status: Mutex::new(200),
                denied: Mutex::new(false),
            })
        }
        fn reset(&self, allow: bool, status: u16) {
            *self.calls.lock().unwrap() = 0;
            *self.allow.lock().unwrap() = allow;
            *self.status.lock().unwrap() = status;
        }
        fn calls(&self) -> usize {
            *self.calls.lock().unwrap()
        }
    }
    #[async_trait]
    impl ReviewClient for Backend {
        async fn create(
            &self,
            path: &str,
            review: &serde_json::Value,
        ) -> std::result::Result<serde_json::Value, ReviewError> {
            assert_eq!(path, "/apis/authorization.k8s.io/v1/subjectaccessreviews");
            assert_eq!(review["kind"], "SubjectAccessReview");
            assert_eq!(review["apiVersion"], "authorization.k8s.io/v1");
            *self.calls.lock().unwrap() += 1;
            let status = *self.status.lock().unwrap();
            if status != 200 {
                return Err(ReviewError {
                    status_code: status,
                    reason: match status {
                        500 => "InternalError",
                        429 => "TooManyRequests",
                        404 => "NotFound",
                        403 => "Forbidden",
                        401 => "Unauthorized",
                        _ => "",
                    }
                    .into(),
                    message: format!("status {status}"),
                    connection_reset: false,
                    suggests_client_delay: false,
                });
            }
            Ok(serde_json::json!({"status": {
                "allowed": *self.allow.lock().unwrap(),
                "denied": *self.denied.lock().unwrap(),
                "reason": "because"
            }}))
        }
    }

    fn fast(steps: i32) -> Backoff {
        Backoff {
            duration: Duration::from_millis(1),
            factor: 1.0,
            jitter: 0.0,
            steps,
        }
    }

    #[tokio::test]
    async fn sar_cache_and_retry_follow_the_upstream_table() {
        let backend = Backend::new();
        let a = SubjectAccessReviewAuthorizer::new(
            backend.clone(),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            fast(5),
        );
        let alice = non_resource(user("alice", &[]), "get", "/metrics");
        let bob = non_resource(user("bob", &[]), "get", "/metrics");
        let ridiculous_bob = non_resource(user("bob", &[]), "get", &"v".repeat(11_000));
        let ridiculous_alice = non_resource(user("alice", &[]), "get", &"v".repeat(11_000));

        // (name, attr, allow, status, expected_err, expected_authorized, expected_calls)
        let cases: Vec<(&str, &RequestAttributes, bool, u16, bool, bool, usize)> = vec![
            ("server errors retry", &alice, false, 500, true, false, 5),
            ("429s retry", &alice, false, 429, true, false, 5),
            ("404 doesnt retry", &alice, false, 404, true, false, 1),
            ("403 doesnt retry", &alice, false, 403, true, false, 1),
            ("401 doesnt retry", &alice, false, 401, true, false, 1),
            ("alice successful request", &alice, true, 200, false, true, 1),
            ("alice cached request", &alice, false, 500, false, true, 0),
            ("bob failed request", &bob, false, 500, true, false, 5),
            ("bob unauthorized request", &bob, false, 200, false, false, 1),
            ("bob unauthorized cached request", &bob, false, 500, false, false, 0),
            ("ridiculous unauthorized request", &ridiculous_bob, false, 200, false, false, 1),
            ("ridiculous unauthorized request again", &ridiculous_bob, false, 200, false, false, 1),
            ("ridiculous authorized request", &ridiculous_alice, true, 200, false, true, 1),
            ("ridiculous authorized request again", &ridiculous_alice, true, 200, false, true, 1),
        ];
        for (name, attr, allow, status, want_err, want_authorized, want_calls) in cases {
            backend.reset(allow, status);
            let got = a.authorize_opinion(attr).await;
            let (authorized, err) = match got {
                Ok(Opinion::Allow) => (true, false),
                Ok(Opinion::NoOpinion { error, .. }) => (false, error.is_some()),
                Ok(Opinion::Deny(_)) => (false, false),
                Err(_) => (false, true),
            };
            assert_eq!(err, want_err, "{name}: error");
            assert_eq!(authorized, want_authorized, "{name}: authorized");
            assert_eq!(backend.calls(), want_calls, "{name}: calls");
        }
    }

    // webhook.go:296-306: denied => explicit deny, allowed+denied => deny + error,
    // neither => no opinion, always with the reason.
    #[tokio::test]
    async fn sar_status_maps_to_opinions() {
        let backend = Backend::new();
        let a = SubjectAccessReviewAuthorizer::new(backend.clone(), Duration::ZERO, Duration::ZERO, fast(1));
        let attr = non_resource(user("u", &[]), "get", "/x");

        backend.reset(true, 200);
        assert_eq!(a.authorize_opinion(&attr).await.unwrap(), Opinion::Allow);

        backend.reset(false, 200);
        assert_eq!(
            a.authorize_opinion(&attr).await.unwrap(),
            Opinion::NoOpinion { reason: "because".into(), error: None }
        );

        *backend.denied.lock().unwrap() = true;
        assert_eq!(
            a.authorize_opinion(&attr).await.unwrap(),
            Opinion::Deny("because".into())
        );

        backend.reset(true, 200);
        assert!(a.authorize_opinion(&attr).await.is_err());
    }

    // a ttl of 0 does not cache (cache.Add with 0 expiry is immediately stale).
    #[tokio::test]
    async fn sar_zero_ttl_does_not_cache() {
        let backend = Backend::new();
        let a = SubjectAccessReviewAuthorizer::new(backend.clone(), Duration::ZERO, Duration::ZERO, fast(1));
        let attr = non_resource(user("u", &[]), "get", "/x");
        backend.reset(true, 200);
        a.authorize_opinion(&attr).await.unwrap();
        a.authorize_opinion(&attr).await.unwrap();
        assert_eq!(backend.calls(), 2);
    }

    // ---- DelegatingAuthorizerConfig.New

    #[test]
    fn delegating_config_requires_a_retry_backoff() {
        let c = DelegatingAuthorizerConfig {
            subject_access_review_client: Backend::new(),
            allow_cache_ttl: Duration::from_secs(10),
            deny_cache_ttl: Duration::from_secs(10),
            webhook_retry_backoff: None,
        };
        assert_eq!(
            c.new().err().unwrap(),
            "retry backoff parameters for delegating authorization webhook has not been specified"
        );
    }

    // toAuthorizer ordering end to end through the union: privileged groups,
    // then always-allow paths, then SubjectAccessReview.
    #[tokio::test]
    async fn union_of_privileged_paths_and_sar() {
        use rusternetes_common::authz::PrivilegedGroupAuthorizer;
        let backend = Backend::new();
        let sar = DelegatingAuthorizerConfig {
            subject_access_review_client: backend.clone(),
            allow_cache_ttl: Duration::from_secs(10),
            deny_cache_ttl: Duration::from_secs(10),
            webhook_retry_backoff: Some(fast(1)),
        }
        .new()
        .unwrap();
        let u = UnionAuthorizer::new(vec![
            Arc::new(PrivilegedGroupAuthorizer::new(&["system:masters"])),
            new_path_authorizer(&["/healthz".into(), "/readyz".into(), "/livez".into()]).unwrap(),
            sar,
        ]);
        backend.reset(false, 200);
        // masters never reach the backend
        let r = u
            .authorize(&non_resource(user("admin", &["system:masters"]), "get", "/metrics"))
            .await
            .unwrap();
        assert_eq!(r, Decision::Allow);
        // an always-allow path never reaches the backend
        let r = u
            .authorize(&non_resource(user("system:anonymous", &[]), "get", "/healthz"))
            .await
            .unwrap();
        assert_eq!(r, Decision::Allow);
        assert_eq!(backend.calls(), 0);
        // everything else asks the API server
        let r = u
            .authorize(&non_resource(user("system:anonymous", &[]), "get", "/metrics"))
            .await
            .unwrap();
        assert!(matches!(r, Decision::Deny(_)));
        assert_eq!(backend.calls(), 1);
        backend.reset(true, 200);
        let r = u
            .authorize(&non_resource(user("monitor", &[]), "get", "/statusz"))
            .await
            .unwrap();
        assert_eq!(r, Decision::Allow);
    }
}
