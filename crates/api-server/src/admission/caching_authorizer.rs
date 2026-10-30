//! `NewCachingAuthorizer` (#3228, rest of #3196).
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/admission/plugin/authorizer/
//! caching_authorizer.go` (`NewCachingAuthorizer`, `cachingAuthorizer.
//! Authorize`) and its test (`caching_authorizer_test.go`
//! `TestCachingAuthorizer`).
//!
//! Deviation (Rust expression, not mechanism): `rusternetes_common::Error` is
//! not `Clone`, so a cached error is replayed as `Error::Authorization` with
//! the original error's message rather than as the same error value.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{
    AuthorizationOutcome, Authorizer, Decision, Opinion, RequestAttributes,
};
use rusternetes_common::resources::{NonResourceRule, ResourceRule};
use rusternetes_common::{Error, Result};

type Cached<T> = std::result::Result<T, String>;

/// `cachingAuthorizer`: caches decisions for the duration of the authorizer's
/// use, intended for one request in the admission chain.
pub struct CachingAuthorizer {
    authorizer: Arc<dyn Authorizer>,
    outcomes: Mutex<HashMap<String, Cached<AuthorizationOutcome>>>,
}

/// `NewCachingAuthorizer`.
pub fn new_caching_authorizer(inner: Arc<dyn Authorizer>) -> CachingAuthorizer {
    CachingAuthorizer {
        authorizer: inner,
        outcomes: Mutex::new(HashMap::new()),
    }
}

/// The cache key: `Authorize`'s `SerializableAttributes` JSON. Groups and
/// extra values are sorted so "differently-ordered groups or extras" share a
/// key; a field selector parsing error counts only when requirements are
/// present ("in the error case, we won't honor this field selector"), and a
/// label selector parsing error is never part of the key.
fn cache_key(a: &RequestAttributes) -> String {
    let mut groups = a.user.groups.clone();
    groups.sort();
    let extra: BTreeMap<&String, Vec<&String>> = a
        .user
        .extra
        .iter()
        .map(|(k, vs)| {
            let mut vs: Vec<&String> = vs.iter().collect();
            vs.sort();
            (k, vs)
        })
        .collect();
    let field_selector = (!a.field_selector_requirements.is_empty()).then_some((
        &a.field_selector_requirements,
        &a.field_selector_parsing_err,
    ));
    let label_selector =
        (!a.label_selector_requirements.is_empty()).then_some(&a.label_selector_requirements);
    format!(
        "{:?}",
        (
            (&a.user.username, &a.user.uid, groups, extra),
            &a.verb,
            &a.namespace,
            &a.api_group,
            &a.api_version,
            &a.resource,
            &a.subresource,
            &a.name,
            &a.path,
            a.is_non_resource_request,
            field_selector,
            label_selector,
        )
    )
}

fn replay<T: Clone>(cached: &Cached<T>) -> Result<T> {
    cached.clone().map_err(Error::Authorization)
}

fn remember<T: Clone>(result: &Result<T>) -> Cached<T> {
    match result {
        Ok(v) => Ok(v.clone()),
        Err(e) => Err(e.to_string()),
    }
}

#[async_trait]
impl Authorizer for CachingAuthorizer {
    /// `cachingAuthorizer.Authorize`: not safe for concurrent use upstream;
    /// the lock is never held across the delegate call.
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        self.authorize_outcome(attrs).await?.into_decision()
    }

    async fn authorize_opinion(&self, attrs: &RequestAttributes) -> Result<Opinion> {
        Ok(self.authorize_outcome(attrs).await?.into_opinion())
    }

    // caching_authorizer.go:31-39,142-147 caches the entire tuple once.
    async fn authorize_outcome(&self, attrs: &RequestAttributes) -> Result<AuthorizationOutcome> {
        let key = cache_key(attrs);
        if let Some(cached) = self.outcomes.lock().unwrap().get(&key) {
            return replay(cached);
        }
        let result = self.authorizer.authorize_outcome(attrs).await;
        self.outcomes.lock().unwrap().insert(key, remember(&result));
        result
    }

    async fn get_user_rules(
        &self,
        user: &UserInfo,
        namespace: &str,
    ) -> Result<(Vec<ResourceRule>, Vec<NonResourceRule>)> {
        self.authorizer.get_user_rules(user, namespace).await
    }
}
