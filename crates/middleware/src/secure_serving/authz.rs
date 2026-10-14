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
//! (`NewFromInterface` passes none), the authorizer metrics, and the
//! field/label selector attributes (every request is a non-resource request,
//! see `filters::authorizer_attributes`).

use super::webhook::{default_should_retry, with_exponential_backoff, Backoff, ReviewClient};
use async_trait::async_trait;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, Opinion, RequestAttributes};
use rusternetes_common::error::{Error, Result};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

/// `unionAuthzHandler` (`pkg/authorization/union/union.go:44-69`): the first
/// `Allow` or `Deny` wins; no-opinion reasons that are not empty are joined
/// with `\n`, and the errors that came with them are aggregated.
///
/// Used instead of `rusternetes_common::authz::UnionAuthorizer`, which joins
/// every reason (empty ones too) with `"; "` and so renders the
/// always-empty reasons of the path and privileged-group authorizers as
/// `": ; "` in the 403 message.
pub struct UnionAuthz {
    authorizers: Vec<Arc<dyn Authorizer>>,
}

impl UnionAuthz {
    pub fn new(authorizers: Vec<Arc<dyn Authorizer>>) -> Self {
        Self { authorizers }
    }
}

#[async_trait]
impl Authorizer for UnionAuthz {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        let mut errlist: Vec<String> = vec![];
        let mut reasons: Vec<String> = vec![];
        for a in &self.authorizers {
            match a.authorize_opinion(attrs).await? {
                Opinion::Allow => return Ok(Decision::Allow),
                Opinion::Deny(r) => return Ok(Decision::Deny(r)),
                Opinion::NoOpinion { reason, error } => {
                    errlist.extend(error);
                    if !reason.is_empty() {
                        reasons.push(reason);
                    }
                }
            }
        }
        match super::authn::aggregate(&errlist) {
            Some(e) => Err(Error::Authorization(e)),
            None => Ok(Decision::Deny(reasons.join("\n"))),
        }
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

/// `maxControlledAttrCacheSize` (webhook.go:56).
pub const MAX_CONTROLLED_ATTR_CACHE_SIZE: usize = 10000;

/// `cache.NewLRUExpireCache(8192)` (`NewFromInterface`).
const RESPONSE_CACHE_SIZE: usize = 8192;

struct PathAuthorizer {
    paths: Vec<String>,
    prefixes: Vec<String>,
}

#[async_trait]
impl Authorizer for PathAuthorizer {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        match self.authorize_opinion(attrs).await? {
            Opinion::Allow => Ok(Decision::Allow),
            Opinion::Deny(r) => Ok(Decision::Deny(r)),
            Opinion::NoOpinion { reason, .. } => Ok(Decision::Deny(reason)),
        }
    }

    async fn authorize_opinion(&self, a: &RequestAttributes) -> Result<Opinion> {
        let no_opinion = Opinion::NoOpinion {
            reason: String::new(),
            error: None,
        };
        if !a.is_non_resource_request {
            return Ok(no_opinion);
        }
        let pth = a.path.as_deref().unwrap_or("");
        let pth = pth.strip_prefix('/').unwrap_or(pth);
        if self.paths.iter().any(|p| p == pth) {
            return Ok(Opinion::Allow);
        }
        if self.prefixes.iter().any(|p| pth.starts_with(p.as_str())) {
            return Ok(Opinion::Allow);
        }
        Ok(no_opinion)
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

/// `path.NewAuthorizer`: accepts the given paths; each is either a fully
/// matching path or ends in `*` for a prefix match; a leading `/` is optional.
/// Resource requests get no opinion.
pub fn new_path_authorizer(
    always_allow_paths: &[String],
) -> std::result::Result<Arc<dyn Authorizer>, String> {
    let mut prefixes = vec![];
    let mut paths: Vec<String> = vec![];
    for p in always_allow_paths {
        let p = p.strip_prefix('/').unwrap_or(p);
        if p.is_empty() {
            // matches "/"
            paths.push(String::new());
            continue;
        }
        if p[..p.len() - 1].contains('*') {
            return Err(format!("only trailing * allowed in {p:?}"));
        }
        if let Some(prefix) = p.strip_suffix('*') {
            prefixes.push(prefix.to_string());
        } else {
            paths.push(p.to_string());
        }
    }
    Ok(Arc::new(PathAuthorizer { paths, prefixes }))
}

/// `webhook.shouldCache` (webhook.go:537-547).
pub fn should_cache(attr: &RequestAttributes) -> bool {
    let size = attr.namespace.as_deref().unwrap_or("").len()
        + attr.verb.len()
        + attr.api_group.len()
        + attr.api_version.len()
        + attr.resource.len()
        + attr.subresource.as_deref().unwrap_or("").len()
        + attr.name.as_deref().unwrap_or("").len()
        + attr.path.as_deref().unwrap_or("").len();
    size < MAX_CONTROLLED_ATTR_CACHE_SIZE
}

/// The `SubjectAccessReviewSpec` `WebhookAuthorizer.Authorize` builds
/// (webhook.go:171-190, `resourceAttributesFrom`). Empty fields are omitted
/// (`omitempty`).
pub fn subject_access_review_spec(attr: &RequestAttributes) -> Value {
    let mut spec = Map::new();
    let u = &attr.user;
    let mut put = |k: &str, v: &str| {
        if !v.is_empty() {
            spec.insert(k.to_string(), json!(v));
        }
    };
    put("user", &u.username);
    put("uid", &u.uid);
    if !u.groups.is_empty() {
        spec.insert("groups".into(), json!(u.groups));
    }
    if !u.extra.is_empty() {
        spec.insert("extra".into(), json!(u.extra));
    }
    if !attr.is_non_resource_request {
        let mut ra = Map::new();
        let mut put = |k: &str, v: &str| {
            if !v.is_empty() {
                ra.insert(k.to_string(), json!(v));
            }
        };
        put("namespace", attr.namespace.as_deref().unwrap_or(""));
        put("verb", &attr.verb);
        put("group", &attr.api_group);
        put("version", &attr.api_version);
        put("resource", &attr.resource);
        put("subresource", attr.subresource.as_deref().unwrap_or(""));
        put("name", attr.name.as_deref().unwrap_or(""));
        spec.insert("resourceAttributes".into(), Value::Object(ra));
    } else {
        spec.insert(
            "nonResourceAttributes".into(),
            json!({"path": attr.path.as_deref().unwrap_or(""), "verb": attr.verb}),
        );
    }
    Value::Object(spec)
}

/// `webhook.WebhookAuthorizer` as built by `DelegatingAuthorizerConfig.New`
/// (`decisionOnError` = `DecisionNoOpinion`).
pub struct SubjectAccessReviewAuthorizer {
    client: Arc<dyn ReviewClient>,
    authorized_ttl: Duration,
    unauthorized_ttl: Duration,
    retry_backoff: Backoff,
    /// `responseCache`: spec JSON -> (expiry, `SubjectAccessReviewStatus`).
    cache: Mutex<HashMap<String, (Instant, Value)>>,
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
            client,
            authorized_ttl,
            unauthorized_ttl,
            retry_backoff,
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn cache_get(&self, key: &str) -> Option<Value> {
        let mut c = self.cache.lock().unwrap();
        match c.get(key) {
            Some((exp, v)) if *exp > Instant::now() => Some(v.clone()),
            Some(_) => {
                c.remove(key);
                None
            }
            None => None,
        }
    }

    fn cache_add(&self, key: String, status: Value, ttl: Duration) {
        let mut c = self.cache.lock().unwrap();
        if c.len() >= RESPONSE_CACHE_SIZE {
            let now = Instant::now();
            c.retain(|_, (exp, _)| *exp > now);
            if c.len() >= RESPONSE_CACHE_SIZE {
                // evict the entry closest to expiry
                if let Some(k) = c
                    .iter()
                    .min_by_key(|(_, (e, _))| *e)
                    .map(|(k, _)| k.clone())
                {
                    c.remove(&k);
                }
            }
        }
        c.insert(key, (Instant::now() + ttl, status));
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

    /// `WebhookAuthorizer.Authorize` (webhook.go:~ 188-319).
    async fn authorize_opinion(&self, attrs: &RequestAttributes) -> Result<Opinion> {
        let spec = subject_access_review_spec(attrs);
        let key = spec.to_string();
        let status = match self.cache_get(&key) {
            Some(s) => s,
            None => {
                let review = json!({
                    "apiVersion": "authorization.k8s.io/v1",
                    "kind": "SubjectAccessReview",
                    "spec": spec,
                });
                let result: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
                let outcome = with_exponential_backoff(
                    self.retry_backoff.clone(),
                    || {
                        let client = self.client.clone();
                        let review = review.clone();
                        let result = result.clone();
                        async move {
                            let r = client
                                .create(
                                    "/apis/authorization.k8s.io/v1/subjectaccessreviews",
                                    &review,
                                )
                                .await?;
                            *result.lock().unwrap() = Some(r);
                            Ok(())
                        }
                    },
                    default_should_retry,
                )
                .await;
                if let Err(e) = outcome {
                    tracing::error!("Failed to make webhook authorizer request: {e}");
                    // decisionOnError == DecisionNoOpinion
                    return Ok(Opinion::NoOpinion {
                        reason: String::new(),
                        error: Some(e),
                    });
                }
                let status = result
                    .lock()
                    .unwrap()
                    .take()
                    .map(|r| r["status"].clone())
                    .unwrap_or(Value::Null);
                if should_cache(attrs) {
                    let ttl = if status["allowed"].as_bool().unwrap_or(false) {
                        self.authorized_ttl
                    } else {
                        self.unauthorized_ttl
                    };
                    self.cache_add(key, status.clone(), ttl);
                }
                status
            }
        };
        let allowed = status["allowed"].as_bool().unwrap_or(false);
        let denied = status["denied"].as_bool().unwrap_or(false);
        let reason = status["reason"].as_str().unwrap_or("").to_string();
        match (denied, allowed) {
            (true, true) => Err(Error::Authorization(
                "webhook subject access review returned both allow and deny response".into(),
            )),
            (true, false) => Ok(Opinion::Deny(reason)),
            (false, true) => Ok(Opinion::Allow),
            (false, false) => Ok(Opinion::NoOpinion {
                reason,
                error: None,
            }),
        }
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
    #[allow(clippy::new_ret_no_self)] // `DelegatingAuthorizerConfig.New`
    pub fn new(&self) -> std::result::Result<Arc<dyn Authorizer>, String> {
        let Some(backoff) = &self.webhook_retry_backoff else {
            return Err(
                "retry backoff parameters for delegating authorization webhook has not been specified"
                    .into(),
            );
        };
        Ok(Arc::new(SubjectAccessReviewAuthorizer::new(
            self.subject_access_review_client.clone(),
            self.allow_cache_ttl,
            self.deny_cache_ttl,
            backoff.clone(),
        )))
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
            Case {
                name: "inner star",
                excluded: vec!["/foo*bar"],
                allowed: vec![],
                no_opinion: vec![],
                want_err: true,
            },
            Case {
                name: "double star",
                excluded: vec!["/foo**"],
                allowed: vec![],
                no_opinion: vec![],
                want_err: true,
            },
            Case {
                name: "empty",
                excluded: vec![],
                allowed: vec![],
                no_opinion: vec!["/"],
                want_err: false,
            },
            Case {
                name: "slash",
                excluded: vec!["/"],
                allowed: vec!["/"],
                no_opinion: vec!["/foo", "//"],
                want_err: false,
            },
            Case {
                name: "foo",
                excluded: vec!["/foo"],
                allowed: vec!["/foo", "foo"],
                no_opinion: vec!["/", "", "/bar", "/foo/", "/fooooo", "//foo"],
                want_err: false,
            },
            Case {
                name: "foo slash",
                excluded: vec!["/foo/"],
                allowed: vec!["/foo/"],
                no_opinion: vec!["/", "", "/bar", "/foo", "/fooooo"],
                want_err: false,
            },
            Case {
                name: "foo slash star",
                excluded: vec!["/foo/*"],
                allowed: vec!["/foo/", "/foo/bar/bla"],
                no_opinion: vec!["/", "", "/foo", "/bar", "/fooooo"],
                want_err: false,
            },
            Case {
                name: "foo bar",
                excluded: vec!["/foo", "/bar"],
                allowed: vec!["/foo", "/bar"],
                no_opinion: vec!["/", "", "/foo/", "/bar/", "/fooooo"],
                want_err: false,
            },
            Case {
                name: "foo star",
                excluded: vec!["/foo*"],
                allowed: vec!["/foo", "/foooo"],
                no_opinion: vec!["/", "", "/fo", "/bar"],
                want_err: false,
            },
            Case {
                name: "star",
                excluded: vec!["/*"],
                allowed: vec!["/", "", "/foo", "/foooo"],
                no_opinion: vec![],
                want_err: false,
            },
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
                assert!(
                    matches!(got, Opinion::NoOpinion { .. }),
                    "{} {p:?}: {got:?}",
                    c.name
                );
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
            new_path_authorizer(&["/foo*bar".to_string()])
                .err()
                .unwrap(),
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
            (
                "alice successful request",
                &alice,
                true,
                200,
                false,
                true,
                1,
            ),
            ("alice cached request", &alice, false, 500, false, true, 0),
            ("bob failed request", &bob, false, 500, true, false, 5),
            (
                "bob unauthorized request",
                &bob,
                false,
                200,
                false,
                false,
                1,
            ),
            (
                "bob unauthorized cached request",
                &bob,
                false,
                500,
                false,
                false,
                0,
            ),
            (
                "ridiculous unauthorized request",
                &ridiculous_bob,
                false,
                200,
                false,
                false,
                1,
            ),
            (
                "ridiculous unauthorized request again",
                &ridiculous_bob,
                false,
                200,
                false,
                false,
                1,
            ),
            (
                "ridiculous authorized request",
                &ridiculous_alice,
                true,
                200,
                false,
                true,
                1,
            ),
            (
                "ridiculous authorized request again",
                &ridiculous_alice,
                true,
                200,
                false,
                true,
                1,
            ),
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
        let a = SubjectAccessReviewAuthorizer::new(
            backend.clone(),
            Duration::ZERO,
            Duration::ZERO,
            fast(1),
        );
        let attr = non_resource(user("u", &[]), "get", "/x");

        backend.reset(true, 200);
        assert_eq!(a.authorize_opinion(&attr).await.unwrap(), Opinion::Allow);

        backend.reset(false, 200);
        assert_eq!(
            a.authorize_opinion(&attr).await.unwrap(),
            Opinion::NoOpinion {
                reason: "because".into(),
                error: None
            }
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
        let a = SubjectAccessReviewAuthorizer::new(
            backend.clone(),
            Duration::ZERO,
            Duration::ZERO,
            fast(1),
        );
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
            .authorize(&non_resource(
                user("admin", &["system:masters"]),
                "get",
                "/metrics",
            ))
            .await
            .unwrap();
        assert_eq!(r, Decision::Allow);
        // an always-allow path never reaches the backend
        let r = u
            .authorize(&non_resource(
                user("system:anonymous", &[]),
                "get",
                "/healthz",
            ))
            .await
            .unwrap();
        assert_eq!(r, Decision::Allow);
        assert_eq!(backend.calls(), 0);
        // everything else asks the API server
        let r = u
            .authorize(&non_resource(
                user("system:anonymous", &[]),
                "get",
                "/metrics",
            ))
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

    struct Fixed(Opinion);
    #[async_trait]
    impl Authorizer for Fixed {
        async fn authorize(&self, _a: &RequestAttributes) -> Result<Decision> {
            unreachable!()
        }
        async fn authorize_opinion(&self, _a: &RequestAttributes) -> Result<Opinion> {
            Ok(self.0.clone())
        }
        async fn get_user_rules(
            &self,
            _u: &UserInfo,
            _ns: &str,
        ) -> Result<(
            Vec<rusternetes_common::resources::ResourceRule>,
            Vec<rusternetes_common::resources::NonResourceRule>,
        )> {
            Ok((vec![], vec![]))
        }
    }

    fn no_opinion(reason: &str, error: Option<&str>) -> Arc<dyn Authorizer> {
        Arc::new(Fixed(Opinion::NoOpinion {
            reason: reason.into(),
            error: error.map(String::from),
        }))
    }

    // union.go:44-69: empty reasons are dropped, the rest joined with "\n".
    #[tokio::test]
    async fn union_joins_non_empty_reasons_with_newlines() {
        let attr = non_resource(user("u", &[]), "get", "/x");
        let u = UnionAuthz::new(vec![no_opinion("", None), no_opinion("", None)]);
        assert_eq!(
            u.authorize(&attr).await.unwrap(),
            Decision::Deny(String::new())
        );
        let u = UnionAuthz::new(vec![
            no_opinion("a", None),
            no_opinion("", None),
            no_opinion("b", None),
        ]);
        assert_eq!(
            u.authorize(&attr).await.unwrap(),
            Decision::Deny("a\nb".into())
        );
    }

    // an Allow or Deny stops the chain; errors of earlier no-opinions are dropped then
    #[tokio::test]
    async fn union_stops_at_the_first_decision() {
        let attr = non_resource(user("u", &[]), "get", "/x");
        let u = UnionAuthz::new(vec![
            no_opinion("", Some("boom")),
            Arc::new(Fixed(Opinion::Allow)),
            no_opinion("never", None),
        ]);
        assert_eq!(u.authorize(&attr).await.unwrap(), Decision::Allow);
        let u = UnionAuthz::new(vec![
            Arc::new(Fixed(Opinion::Deny("no".into()))),
            Arc::new(Fixed(Opinion::Allow)),
        ]);
        assert_eq!(
            u.authorize(&attr).await.unwrap(),
            Decision::Deny("no".into())
        );
    }

    // no decision plus errors is an error: the filter renders it as a 500
    #[tokio::test]
    async fn union_aggregates_errors_when_nobody_decides() {
        let attr = non_resource(user("u", &[]), "get", "/x");
        let u = UnionAuthz::new(vec![no_opinion("", Some("e1")), no_opinion("", Some("e2"))]);
        assert_eq!(
            u.authorize(&attr).await.unwrap_err().to_string(),
            Error::Authorization("[e1, e2]".into()).to_string()
        );
    }
}
