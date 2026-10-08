use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Bytes,
    extract::{Path, State},
    Extension, Json,
};
use rusternetes_common::dump::DumpingJson;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::{
        SelfSubjectReview, SelfSubjectReviewStatus, TokenRequest, TokenRequestStatus, TokenReview,
        TokenReviewStatus, UserInfo,
    },
    Result,
};
use rusternetes_storage::Storage;
use std::sync::Arc;
use tracing::{info, warn};

/// Create a TokenReview (authentication.k8s.io/v1)
/// TokenReview attempts to authenticate a token to a known user.
pub async fn create_token_review(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    DumpingJson(mut token_review): DumpingJson<TokenReview>,
) -> Result<Json<TokenReview>> {
    info!("Creating token review");

    // Check authorization - creating a TokenReview requires impersonation privileges
    let attrs = RequestAttributes::new(auth_ctx.user.clone(), "create", "tokenreviews")
        .with_api_group("authentication.k8s.io");

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // A TokenReview with no token is a 400 from the registry, not a 422 from a
    // validator: `TokenREST.Create` checks it before touching the
    // authenticator (`pkg/registry/authentication/tokenreview/storage.go:79-81`).
    if token_review.spec.token.is_empty() {
        return Err(rusternetes_common::Error::BadRequest(
            "token is required for TokenReview in authentication".to_string(),
        ));
    }

    // Authenticate the provided token using the available authentication mechanisms
    // Try service account token first
    // Upstream puts `spec.audiences` in the request context so the token's
    // audience is matched against them (tokenreview/storage.go:83-85), then the
    // JWT authenticator verifies signature + issuer + audience and the
    // validator checks the bound objects (jwt.go:334-411, claims.go:144-263).
    let requested_audiences = token_review.spec.audiences.clone().unwrap_or_default();
    let authn = match state
        .token_manager
        .authenticate_token(&token_review.spec.token, Some(&requested_audiences))
    {
        Ok((claims, matched)) => {
            match rusternetes_middleware::validate_service_account_token(
                &state.storage,
                &token_review.spec.token,
                &claims,
            )
            .await
            {
                Ok(()) => Ok((claims, matched)),
                Err(e) => Err(e),
            }
        }
        Err(e) => Err(e.to_string()),
    };
    let status = if let Ok((claims, matched_audiences)) = authn {
        // Valid service account token
        TokenReviewStatus {
            authenticated: Some(true),
            user: Some(UserInfo {
                username: Some(claims.sub.clone()),
                uid: Some(claims.uid),
                groups: Some(vec![
                    "system:serviceaccounts".to_string(),
                    format!("system:serviceaccounts:{}", claims.namespace),
                    "system:authenticated".to_string(),
                ]),
                extra: Some({
                    let mut extra = std::collections::HashMap::new();
                    // K8s expects credential-id with JTI prefix for SA tokens
                    let jti = format!("JTI={}", uuid::Uuid::new_v4());
                    extra.insert(
                        "authentication.kubernetes.io/credential-id".to_string(),
                        vec![jti],
                    );
                    // Include pod binding info if present in claims
                    if let Some(ref pod_name) = claims.pod_name {
                        extra.insert(
                            "authentication.kubernetes.io/pod-name".to_string(),
                            vec![pod_name.clone()],
                        );
                    }
                    if let Some(ref pod_uid) = claims.pod_uid {
                        extra.insert(
                            "authentication.kubernetes.io/pod-uid".to_string(),
                            vec![pod_uid.clone()],
                        );
                    }
                    if let Some(ref node_name) = claims.node_name {
                        extra.insert(
                            "authentication.kubernetes.io/node-name".to_string(),
                            vec![node_name.clone()],
                        );
                    }
                    if let Some(ref node_uid) = claims.node_uid {
                        extra.insert(
                            "authentication.kubernetes.io/node-uid".to_string(),
                            vec![node_uid.clone()],
                        );
                    }
                    extra
                }),
            }),
            // With --api-audiences configured the status carries the matched
            // audiences like upstream; otherwise keep echoing the request.
            audiences: if state.token_manager.api_audiences().is_empty() {
                token_review.spec.audiences.clone()
            } else {
                Some(matched_audiences)
            },
            error: None,
        }
    } else {
        // Token validation failed - could be bootstrap token, OIDC, or invalid
        // For now, mark as unauthenticated
        // Note: Full implementation would try other auth mechanisms here
        // (bootstrap tokens, OIDC, webhook auth, etc.)
        TokenReviewStatus {
            authenticated: Some(false),
            user: None,
            audiences: None,
            error: Some(
                "Token authentication failed - not a valid service account token".to_string(),
            ),
        }
    };

    token_review.status = Some(status);
    Ok(Json(token_review))
}

/// Create a TokenRequest (authentication.k8s.io/v1)
/// TokenRequest requests a token for a given service account.
pub async fn create_token_request(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, service_account_name)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<TokenRequest>> {
    // Parse the body manually — the K8s client may send protobuf or JSON with extra
    // fields that Axum's Json<T> extractor rejects with a 422, which is not a proper
    // Kubernetes Status error.  Manual parsing gives us control over the error response.
    let mut token_request: TokenRequest = serde_json::from_slice(&body).map_err(|e| {
        warn!(
            "Failed to decode TokenRequest body ({} bytes): {}",
            body.len(),
            e
        );
        rusternetes_common::Error::InvalidResource(format!("failed to decode: {}", e))
    })?;
    info!(
        "Creating token request for service account {}/{}",
        namespace, service_account_name
    );

    // Check authorization - requires permission to create token requests for the service account
    let attrs = RequestAttributes::new(auth_ctx.user.clone(), "create", "serviceaccounts/token")
        .with_api_group("")
        .with_namespace(&namespace)
        .with_name(&service_account_name);

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Set metadata from path params so response includes them
    token_request.metadata.name = service_account_name.clone();
    token_request.metadata.namespace = Some(namespace.clone());

    // Verify the service account exists
    let sa_key =
        rusternetes_storage::build_key("serviceaccounts", Some(&namespace), &service_account_name);
    let sa: rusternetes_common::resources::ServiceAccount = state.storage.get(&sa_key).await?;

    // Calculate expiration time
    let expiration_seconds = token_request.spec.expiration_seconds.unwrap_or(3600);

    // TokenRequest.spec.expirationSeconds bounds (upstream ValidateTokenRequest):
    // >= 10 minutes and <= 2^32 seconds.
    if expiration_seconds < 600 {
        return Err(rusternetes_common::Error::new_invalid(
            "authentication.k8s.io",
            "TokenRequest",
            "",
            vec![rusternetes_common::validation::field::Error::invalid(
                &rusternetes_common::validation::field::Path::new("spec")
                    .child("expirationSeconds"),
                expiration_seconds,
                "may not specify a duration less than 10 minutes",
            )],
        ));
    }
    if expiration_seconds > (1_i64 << 32) {
        return Err(rusternetes_common::Error::new_invalid(
            "authentication.k8s.io",
            "TokenRequest",
            "",
            vec![rusternetes_common::validation::field::Error::invalid(
                &rusternetes_common::validation::field::Path::new("spec")
                    .child("expirationSeconds"),
                expiration_seconds,
                "may not specify a duration larger than 2^32 seconds",
            )],
        ));
    }

    let now = chrono::Utc::now();
    let expiration_timestamp = now
        .checked_add_signed(chrono::Duration::seconds(expiration_seconds))
        .ok_or_else(|| {
            rusternetes_common::Error::Internal("Failed to calculate expiration time".to_string())
        })?;

    // Generate a proper JWT service account token using direct seconds for precision
    let mut claims = rusternetes_common::auth::ServiceAccountClaims {
        sub: format!(
            "system:serviceaccount:{}:{}",
            namespace, service_account_name
        ),
        namespace: namespace.clone(),
        uid: sa.metadata.uid.clone(),
        iat: now.timestamp(),
        exp: expiration_timestamp.timestamp(),
        iss: "https://kubernetes.default.svc.cluster.local".to_string(),
        aud: vec!["rusternetes".to_string()],
        kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
            namespace: namespace.clone(),
            svcacct: rusternetes_common::auth::KubeRef {
                name: service_account_name.clone(),
                uid: sa.metadata.uid.clone(),
            },
            pod: None,
            node: None,
            secret: None,
        }),
        pod_name: None,
        pod_uid: None,
        node_name: None,
        node_uid: None,
    };

    // Set audience from the request (TokenRequestSpec.audiences is Vec<String>, not Option)
    if !token_request.spec.audiences.is_empty() {
        claims.aud = token_request.spec.audiences.clone();
    } else if !state.token_manager.api_audiences().is_empty() {
        // Default unset spec audiences to the API server audiences
        // (pkg/registry/core/serviceaccount/storage/token.go:116-119).
        claims.aud = state.token_manager.api_audiences().to_vec();
    }

    // Bound object reference. Port of `TokenREST.Create`
    // (pkg/registry/core/serviceaccount/storage/token.go:171-224) and
    // `token.Claims` (pkg/serviceaccount/claims.go:84-123): the referent must
    // exist and its UID must match, and the claims record it so that deleting
    // the object invalidates the token (see `validate_service_account_claims`).
    if let Some(ref bound_ref) = token_request.spec.bound_object_ref {
        let api_version = bound_ref.api_version.clone().unwrap_or_default();
        let kind = bound_ref.kind.clone().unwrap_or_default();
        let ref_name = bound_ref.name.clone().unwrap_or_default();
        let ref_uid = bound_ref.uid.clone().unwrap_or_default();
        let core_group = !api_version.contains('/');
        let uid: String;
        match (core_group, kind.as_str()) {
            (true, "Pod") => {
                let pod_key = rusternetes_storage::build_key("pods", Some(&namespace), &ref_name);
                let pod: rusternetes_common::resources::Pod = state.storage.get(&pod_key).await?;
                let pod_sa = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.service_account_name.clone())
                    .unwrap_or_else(|| "default".to_string());
                if pod_sa != service_account_name {
                    return Err(rusternetes_common::Error::BadRequest(format!(
                        "cannot bind token for serviceaccount {:?} to pod running with different serviceaccount name.",
                        service_account_name
                    )));
                }
                uid = pod.metadata.uid.clone();
                claims.pod_name = Some(ref_name.clone());
                claims.pod_uid = Some(uid.clone());
                let mut node_ref = None;
                if let Some(node_name) = pod.spec.as_ref().and_then(|s| s.node_name.clone()) {
                    // A missing Node still records its name (token.go:196-202).
                    let node_key =
                        rusternetes_storage::build_key("nodes", None::<&str>, &node_name);
                    let node_uid = state
                        .storage
                        .get::<rusternetes_common::resources::Node>(&node_key)
                        .await
                        .map(|n| n.metadata.uid)
                        .unwrap_or_default();
                    claims.node_name = Some(node_name.clone());
                    if !node_uid.is_empty() {
                        claims.node_uid = Some(node_uid.clone());
                    }
                    node_ref = Some(rusternetes_common::auth::KubeRef {
                        name: node_name,
                        uid: node_uid,
                    });
                }
                if let Some(k) = claims.kubernetes.as_mut() {
                    k.pod = Some(rusternetes_common::auth::KubeRef {
                        name: ref_name.clone(),
                        uid: uid.clone(),
                    });
                    k.node = node_ref;
                }
            }
            (true, "Node") => {
                let node_key = rusternetes_storage::build_key("nodes", None::<&str>, &ref_name);
                let node: rusternetes_common::resources::Node =
                    state.storage.get(&node_key).await?;
                uid = node.metadata.uid.clone();
                claims.node_name = Some(ref_name.clone());
                claims.node_uid = Some(uid.clone());
                if let Some(k) = claims.kubernetes.as_mut() {
                    k.node = Some(rusternetes_common::auth::KubeRef {
                        name: ref_name.clone(),
                        uid: uid.clone(),
                    });
                }
            }
            (true, "Secret") => {
                let key = rusternetes_storage::build_key("secrets", Some(&namespace), &ref_name);
                let secret: rusternetes_common::resources::Secret = state.storage.get(&key).await?;
                uid = secret.metadata.uid.clone();
                if let Some(k) = claims.kubernetes.as_mut() {
                    k.secret = Some(rusternetes_common::auth::KubeRef {
                        name: ref_name.clone(),
                        uid: uid.clone(),
                    });
                }
            }
            _ => {
                return Err(rusternetes_common::Error::BadRequest(format!(
                    "cannot bind token to object of type {}, Kind={}",
                    api_version, kind
                )));
            }
        }
        if !ref_uid.is_empty() && uid != ref_uid {
            return Err(rusternetes_common::Error::Conflict(format!(
                "the UID in the bound object reference ({}) does not match the UID in record. The object might have been deleted and then recreated",
                ref_uid
            )));
        }
    }

    let token = state.token_manager.generate_token(claims)?;

    // Ensure apiVersion and kind are set
    token_request.api_version = "authentication.k8s.io/v1".to_string();
    token_request.kind = "TokenRequest".to_string();
    token_request.metadata.ensure_creation_timestamp();

    token_request.status = Some(TokenRequestStatus {
        token,
        // `TokenRequestStatus.expirationTimestamp` is a `metav1.Time` upstream,
        // so it goes on the wire as RFC3339 at second precision with a `Z`
        // suffix. Rusternetes carries it as a `String` with no normalising
        // serializer, so the format has to be chosen here — chrono's plain
        // `to_rfc3339()` would emit `…:00.123456789+00:00`.
        expiration_timestamp: expiration_timestamp
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    });

    Ok(Json(token_request))
}

/// Create a SelfSubjectReview (authentication.k8s.io/v1)
/// SelfSubjectReview contains the user information that the kube-apiserver has about the user making this request.
pub async fn create_self_subject_review(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    DumpingJson(mut self_subject_review): DumpingJson<SelfSubjectReview>,
) -> Result<Json<SelfSubjectReview>> {
    info!("Creating self subject review for user: {:?}", auth_ctx.user);

    // Check authorization - creating a SelfSubjectReview is always allowed
    let attrs = RequestAttributes::new(auth_ctx.user.clone(), "create", "selfsubjectreviews")
        .with_api_group("authentication.k8s.io");

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Return the current user's information
    self_subject_review.status = Some(SelfSubjectReviewStatus {
        user_info: Some(UserInfo {
            username: Some(auth_ctx.user.username.clone()),
            uid: Some(auth_ctx.user.uid.clone()),
            groups: Some(auth_ctx.user.groups.clone()),
            extra: Some(auth_ctx.user.extra.clone()),
        }),
    });

    Ok(Json(self_subject_review))
}

#[cfg(test)]
#[cfg(feature = "integration-tests")] // Disable incomplete tests
mod tests {
    use super::*;
    use crate::state::MockAuth;
    use rusternetes_common::resources::TokenReviewSpec;

    #[tokio::test]
    async fn test_token_review_authenticated() {
        // This test would verify that a valid token returns authenticated=true
        // Implementation would depend on the actual auth system
    }

    #[tokio::test]
    async fn test_token_review_unauthenticated() {
        // This test would verify that an invalid token returns authenticated=false
    }

    #[tokio::test]
    async fn test_self_subject_review() {
        // This test would verify that SelfSubjectReview returns the current user's info
    }
}
