//! Shared port of `k8s.io/cluster-bootstrap/util/secrets` and the
//! `bootstrapapi` constants (`staging/src/k8s.io/cluster-bootstrap/token/api/types.go`,
//! release-1.35), used by both the bootstrap signer and the token cleaner as
//! upstream shares one `bootstrapsecretutil` package between them.

use chrono::{DateTime, Utc};
use rusternetes_common::resources::Secret;
use std::time::Duration;

/// `bootstrapapi.SecretTypeBootstrapToken` (types.go:33).
pub const SECRET_TYPE_BOOTSTRAP_TOKEN: &str = "bootstrap.kubernetes.io/token";
/// `bootstrapapi.BootstrapTokenSecretPrefix`.
pub const BOOTSTRAP_TOKEN_SECRET_PREFIX: &str = "bootstrap-token-";
/// `bootstrapapi.BootstrapTokenIDKey`.
pub const BOOTSTRAP_TOKEN_ID_KEY: &str = "token-id";
/// `bootstrapapi.BootstrapTokenSecretKey`.
pub const BOOTSTRAP_TOKEN_SECRET_KEY: &str = "token-secret";
/// `bootstrapapi.BootstrapTokenExpirationKey` (types.go:48).
pub const BOOTSTRAP_TOKEN_EXPIRATION_KEY: &str = "expiration";
/// `bootstrapapi.BootstrapTokenUsageSigningKey`.
pub const BOOTSTRAP_TOKEN_USAGE_SIGNING_KEY: &str = "usage-bootstrap-signing";

/// `bootstrapsecretutil.ParseName`: `^bootstrap-token-([a-z0-9]{6})$`.
pub fn parse_name(name: &str) -> Option<&str> {
    let id = name.strip_prefix(BOOTSTRAP_TOKEN_SECRET_PREFIX)?;
    (id.len() == 6
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()))
    .then_some(id)
}

/// `bootstrapsecretutil.GetData`: empty string when absent.
pub fn get_data(secret: &Secret, key: &str) -> String {
    secret
        .data
        .as_ref()
        .and_then(|d| d.get(key))
        .map(|v| String::from_utf8_lossy(v).into_owned())
        .unwrap_or_default()
}

/// `bootstrapsecretutil.GetExpiration` (secrets.go:59-78):
/// `(timeRemaining, isExpired)`. No expiration is not expired, an
/// unparseable one is expired.
pub fn get_expiration(secret: &Secret, now: DateTime<Utc>) -> (Duration, bool) {
    let expiration = get_data(secret, BOOTSTRAP_TOKEN_EXPIRATION_KEY);
    if expiration.is_empty() {
        return (Duration::ZERO, false);
    }
    let Ok(exp) = DateTime::parse_from_rfc3339(&expiration) else {
        return (Duration::ZERO, true);
    };
    match (exp.with_timezone(&Utc) - now).to_std() {
        Ok(d) if !d.is_zero() => (d, false),
        _ => (Duration::ZERO, true),
    }
}

/// `bootstrapsecretutil.HasExpired`.
pub fn has_expired(secret: &Secret, now: DateTime<Utc>) -> bool {
    get_expiration(secret, now).1
}
