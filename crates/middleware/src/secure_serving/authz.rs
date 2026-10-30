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
//! * `pkg/authorization/union/union.go` (reused: `rusternetes_common::authz::UnionAuthorizer`)
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
            Opinion::Allow(_) => Ok(Decision::Allow),
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
            return Ok(Opinion::Allow(String::new()));
        }
        if self.prefixes.iter().any(|p| pth.starts_with(p.as_str())) {
            return Ok(Opinion::Allow(String::new()));
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
            Opinion::Allow(_) => Ok(Decision::Allow),
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
            (false, true) => Ok(Opinion::Allow(reason)),
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
