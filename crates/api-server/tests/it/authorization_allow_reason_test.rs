//! authorization.go:64-81, metrics_test.go:218-228: an actual authorization
//! outcome may allow and carry an error; the filter must still annotate allow.
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::get,
    Router,
};
use rusternetes_api_server::{
    authorization_filter::with_authorization, request_scope::request_info_middleware,
};
use rusternetes_common::{
    auth::UserInfo,
    authz::{AuthorizationOutcome, Authorizer, Decision, Opinion, RequestAttributes},
    latency_trackers::{with_latency_trackers, LatencyTrackers},
    resources::{NonResourceRule, ResourceRule},
    Result,
};
use rusternetes_middleware::AuthContext;
use std::sync::Arc;
use tower::ServiceExt;

struct AllowWithError;
#[async_trait]
impl Authorizer for AllowWithError {
    async fn authorize(&self, _: &RequestAttributes) -> Result<Decision> {
        unreachable!("filter must preserve the complete outcome")
    }
    async fn authorize_outcome(&self, _: &RequestAttributes) -> Result<AuthorizationOutcome> {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        Ok(AuthorizationOutcome {
            opinion: Opinion::Allow("RBAC: partial evaluation allowed".into()),
            error: Some("evaluation failed elsewhere".into()),
        })
    }
    async fn get_user_rules(
        &self,
        _: &UserInfo,
        _: &str,
    ) -> Result<(Vec<ResourceRule>, Vec<NonResourceRule>)> {
        Ok((vec![], vec![]))
    }
}

#[tokio::test]
async fn allow_with_error_survives_union_and_instrumentation_and_tracks_latency() {
    use rusternetes_common::authz::UnionAuthorizer;
    let instrumented = rusternetes_api_server::authorization_metrics::instrumented_authorizer(
        "RBAC",
        "partial",
        Arc::new(AllowWithError),
    );
    let authorizer: Arc<dyn Authorizer> = Arc::new(UnionAuthorizer::new(vec![instrumented]));
    let attrs = RequestAttributes::new(UserInfo::anonymous(), "get", "pods");
    let outcome = authorizer.authorize_outcome(&attrs).await.unwrap();
    assert_eq!(
        outcome.opinion,
        Opinion::Allow("RBAC: partial evaluation allowed".into())
    );
    assert_eq!(
        outcome.error.as_deref(),
        Some("evaluation failed elsewhere")
    );
    let app = Router::new()
        .route("/api/v1/pods", get(|| async { "allowed" }))
        .layer(axum::middleware::from_fn_with_state(
            authorizer,
            with_authorization,
        ))
        .layer(axum::middleware::from_fn(request_info_middleware));
    let mut request = Request::builder()
        .uri("/api/v1/pods")
        .body(Body::empty())
        .unwrap();
    request.extensions_mut().insert(AuthContext {
        user: UserInfo::anonymous(),
    });
    let trackers = Arc::new(LatencyTrackers::new());
    let response = with_latency_trackers(Arc::clone(&trackers), app.oneshot(request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(trackers.authorization.get_latency() >= std::time::Duration::from_millis(2));
}

// ABAC's successful reason is deliberately empty, per abac.go:229-235.
#[tokio::test]
async fn abac_reasons_remain_empty_on_allow_and_exact_on_no_match() {
    use rusternetes_api_server::abac::{Policy, PolicyList, PolicySpec};
    let policies = PolicyList(vec![Policy {
        spec: PolicySpec {
            user: "alice".into(),
            namespace: "*".into(),
            resource: "*".into(),
            api_group: "*".into(),
            ..PolicySpec::default()
        },
    }]);
    let attrs = RequestAttributes::new(
        UserInfo {
            username: "alice".into(),
            uid: String::new(),
            groups: vec![],
            extra: Default::default(),
        },
        "get",
        "pods",
    );
    assert_eq!(
        policies.authorize_opinion(&attrs).await.unwrap(),
        Opinion::Allow(String::new())
    );
    let other = RequestAttributes::new(UserInfo::anonymous(), "get", "pods");
    assert_eq!(
        policies.authorize_opinion(&other).await.unwrap(),
        Opinion::NoOpinion {
            reason: "No policy matched.".into(),
            error: None
        }
    );
}

// caching_authorizer.go:31-39,79-81 caches the entire tuple once, shared by
// adapters for the legacy APIs, rather than separately reauthorizing.
#[tokio::test]
async fn cached_outcome_is_shared_with_legacy_adapters() {
    let cache = rusternetes_api_server::admission::caching_authorizer::new_caching_authorizer(
        Arc::new(AllowWithError),
    );
    let attrs = RequestAttributes::new(UserInfo::anonymous(), "get", "pods");
    let outcome = cache.authorize_outcome(&attrs).await.unwrap();
    assert_eq!(cache.authorize(&attrs).await.unwrap(), Decision::Allow);
    assert_eq!(
        cache.authorize_opinion(&attrs).await.unwrap(),
        outcome.opinion
    );
    assert_eq!(cache.authorize_outcome(&attrs).await.unwrap(), outcome);
}

// metrics/metrics.go:77-94 returns the delegate's complete tuple, including
// when called via legacy adapters in this Rust implementation.
#[tokio::test]
async fn instrumented_legacy_adapters_share_the_outcome_path() {
    let authorizer = rusternetes_api_server::authorization_metrics::instrumented_authorizer(
        "RBAC", "legacy-outcome", Arc::new(AllowWithError),
    );
    let attrs = RequestAttributes::new(UserInfo::anonymous(), "get", "pods");
    assert_eq!(authorizer.authorize(&attrs).await.unwrap(), Decision::Allow);
    assert_eq!(authorizer.authorize_opinion(&attrs).await.unwrap(),
        Opinion::Allow("RBAC: partial evaluation allowed".into()));
}

// pkg/kubeapiserver/authorizer/reload.go:78-79 forwards the full tuple.
#[tokio::test]
async fn reloadable_legacy_adapters_share_the_outcome_path() {
    use rusternetes_api_server::{
        authorization_config::AuthorizationConfiguration,
        authorization_reload::{ChainBuilder, ReloadableAuthorizer},
    };
    struct Builder;
    impl ChainBuilder for Builder {
        fn new_for_config(&self, _: &AuthorizationConfiguration) -> anyhow::Result<Arc<dyn Authorizer>> {
            unreachable!("this test never reloads")
        }
    }
    let authorizer = ReloadableAuthorizer::new("", "test".into(),
        AuthorizationConfiguration::default(), vec![], Arc::new(AllowWithError), Arc::new(Builder));
    let attrs = RequestAttributes::new(UserInfo::anonymous(), "get", "pods");
    assert_eq!(authorizer.authorize(&attrs).await.unwrap(), Decision::Allow);
    assert_eq!(authorizer.authorize_opinion(&attrs).await.unwrap(),
        Opinion::Allow("RBAC: partial evaluation allowed".into()));
    assert!(authorizer.authorize_outcome(&attrs).await.unwrap().error.is_some());
}
