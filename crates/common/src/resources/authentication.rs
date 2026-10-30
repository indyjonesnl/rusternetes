use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::types::ObjectMeta;

// ============================================================================
// TokenReview (authentication.k8s.io/v1)
// ============================================================================

/// TokenReview attempts to authenticate a token to a known user.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenReview {
    #[serde(default = "default_api_version_token_review")]
    pub api_version: String,
    #[serde(default = "default_kind_token_review")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: TokenReviewSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<TokenReviewStatus>,
}

fn default_api_version_token_review() -> String {
    "authentication.k8s.io/v1".to_string()
}

fn default_kind_token_review() -> String {
    "TokenReview".to_string()
}

/// TokenReviewSpec is a description of the token authentication request.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TokenReviewSpec {
    /// Audiences is a list of the identifiers that the resource server presented
    /// with the token identifies as. Audience-aware token authenticators will
    /// verify that the token was intended for at least one of the audiences in
    /// this list. If no audiences are provided, the audience will default to the
    /// audience of the Kubernetes apiserver.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audiences: Option<Vec<String>>,

    /// Token is the opaque bearer token.
    ///
    /// Upstream answers an absent token from the registry rather than from a
    /// validator: `TokenREST.Create` returns
    /// `NewBadRequest("token is required for TokenReview in authentication")`
    /// (`pkg/registry/authentication/tokenreview/storage.go:79-81`), so the
    /// field decodes and `create_token_review` reports that message.
    #[serde(default)]
    pub token: String,
}

/// TokenReviewStatus is the result of the token authentication request.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TokenReviewStatus {
    /// Audiences are audience identifiers chosen by the authenticator that are
    /// compatible with both the TokenReview and token. An identifier is any
    /// identifier in the intersection of the TokenReviewSpec audiences and the
    /// token's audiences. A client of the TokenReview API that sets the
    /// spec.audiences field should validate that a compatible audience identifier
    /// is returned in the status.audiences field to ensure that the TokenReview
    /// server is audience aware.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audiences: Option<Vec<String>>,

    /// Authenticated indicates that the token was associated with a known user.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authenticated: Option<bool>,

    /// Error indicates that the token couldn't be checked.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// User is the UserInfo associated with the provided token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<UserInfo>,
}

/// UserInfo holds the information about the user needed to implement the user.Info interface.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    /// Any additional information provided by the authenticator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra: Option<HashMap<String, Vec<String>>>,

    /// The names of groups this user is a part of.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<String>>,

    /// A unique value that identifies this user across time. If this user is
    /// deleted and another user by the same name is added, they will have
    /// different UIDs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,

    /// The name that uniquely identifies this user among all active users.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

// ============================================================================
// TokenRequest (authentication.k8s.io/v1)
// ============================================================================

/// TokenRequest requests a token for a given service account.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenRequest {
    #[serde(default = "default_api_version_token_request")]
    pub api_version: String,
    #[serde(default = "default_kind_token_request")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: TokenRequestSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<TokenRequestStatus>,
}

fn default_api_version_token_request() -> String {
    "authentication.k8s.io/v1".to_string()
}

fn default_kind_token_request() -> String {
    "TokenRequest".to_string()
}

/// TokenRequestSpec contains client provided parameters of a token request.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenRequestSpec {
    /// Audiences are the intendend audiences of the token. A recipient of a
    /// token must identify himself with an identifier in the list of audiences
    /// of the token, and otherwise should reject the token.
    #[serde(default, deserialize_with = "crate::deserialize_null_default")]
    pub audiences: Vec<String>,

    /// BoundObjectRef is a reference to an object that the token will be bound to.
    /// The token will only be valid for as long as the bound object exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_object_ref: Option<BoundObjectReference>,

    /// ExpirationSeconds is the requested duration of validity of the request. The
    /// token issuer may return a token with a different validity duration so a
    /// client needs to check the 'expiration' field in a response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_seconds: Option<i64>,
}

/// BoundObjectReference is a reference to an object that a token is bound to.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BoundObjectReference {
    /// API version of the referent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,

    /// Kind of the referent. Valid kinds are 'Pod' and 'Secret'.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,

    /// Name of the referent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// UID of the referent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}

/// TokenRequestStatus is the result of a token request.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TokenRequestStatus {
    /// ExpirationTimestamp is the time of expiration of the returned token.
    /// Go's metav1.Time serializes as null for the zero value, so we must
    /// accept null gracefully during deserialization.
    #[serde(default, deserialize_with = "crate::deserialize_null_default")]
    pub expiration_timestamp: String,

    /// Token is the opaque bearer token.
    #[serde(default, deserialize_with = "crate::deserialize_null_default")]
    pub token: String,
}

// ============================================================================
// SelfSubjectReview (authentication.k8s.io/v1)
// ============================================================================

/// SelfSubjectReview contains the user information that the kube-apiserver has about the user making this request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelfSubjectReview {
    #[serde(default = "default_api_version_self_subject_review")]
    pub api_version: String,
    #[serde(default = "default_kind_self_subject_review")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<SelfSubjectReviewStatus>,
}

fn default_api_version_self_subject_review() -> String {
    "authentication.k8s.io/v1".to_string()
}

fn default_kind_self_subject_review() -> String {
    "SelfSubjectReview".to_string()
}

/// SelfSubjectReviewStatus is filled by the kube-apiserver and sent back to a user.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SelfSubjectReviewStatus {
    /// User attributes of the user making this request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_info: Option<UserInfo>,
}
