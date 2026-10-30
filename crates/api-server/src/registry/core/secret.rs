//! Secret strategy and storage — port of `pkg/registry/core/secret/strategy.go`
//! and `pkg/registry/core/secret/storage/storage.go`, plus the decode-time
//! defaulting and conversion (`pkg/apis/core/v1/{defaults,conversion}.go`)
//! the Secret endpoints apply to every object they build.

use std::sync::Arc;

use rusternetes_common::resources::Secret;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::secret::{validate_secret, validate_secret_update};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `SecretTypeTLS`, `TLSCertKey` and `TLSPrivateKeyKey`
/// (pkg/apis/core/types.go).
pub const SECRET_TYPE_TLS: &str = "kubernetes.io/tls";
pub const TLS_CERT_KEY: &str = "tls.crt";
pub const TLS_PRIVATE_KEY_KEY: &str = "tls.key";

/// A decoded v1 Secret as the strategy sees it: `SetDefaults_Secret`
/// (pkg/apis/core/v1/defaults.go:265-269) defaults an empty type to
/// `Opaque`, then `Convert_v1_Secret_To_core_Secret` (conversion.go:407-423)
/// writes `stringData` over `data` — the internal type has no `stringData`.
pub fn convert_to_internal(secret: &mut Secret) {
    if secret.secret_type.as_deref().unwrap_or("").is_empty() {
        secret.secret_type = Some("Opaque".to_string());
    }
    secret.normalize();
}

/// `secret.Strategy` (strategy.go:37-45).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

/// `warningsForSecret` (strategy.go:131-141): a TLS secret whose key does not
/// match its certificate is accepted, with `tls.X509KeyPair`'s error as a
/// warning.
fn warnings_for_secret(secret: &Secret) -> Vec<String> {
    if secret.secret_type.as_deref() != Some(SECRET_TYPE_TLS) {
        return Vec::new();
    }
    let value = |key: &str| {
        secret
            .data
            .as_ref()
            .and_then(|d| d.get(key))
            .map(Vec::as_slice)
            .unwrap_or_default()
    };
    match rusternetes_common::tls::x509_key_pair(value(TLS_CERT_KEY), value(TLS_PRIVATE_KEY_KEY)) {
        Ok(()) => Vec::new(),
        Err(e) => vec![e],
    }
}

impl RestCreateStrategy<Secret> for Strategy {
    /// `dropDisabledFields(secret, nil)` (strategy.go:55-58): Secret has no
    /// feature-gated fields.
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut Secret) {}

    fn validate(&self, _ctx: &RequestContext, obj: &Secret) -> ErrorList {
        validate_secret(obj)
    }

    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &Secret) -> Vec<String> {
        warnings_for_secret(obj)
    }
}

impl RestUpdateStrategy<Secret> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:77-87: an update without a type keeps the old one.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Secret, old: &Secret) {
        if obj.secret_type.as_deref().unwrap_or("").is_empty() {
            obj.secret_type = old.secret_type.clone();
        }
    }

    fn validate_update(&self, _ctx: &RequestContext, obj: &Secret, old: &Secret) -> ErrorList {
        validate_secret_update(old, obj)
    }

    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &Secret,
        _old: &Secret,
    ) -> Vec<String> {
        warnings_for_secret(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// Secret uses the default delete strategy: no graceful deletion and no
/// default garbage-collection policy.
impl RestDeleteStrategy<Secret> for Strategy {}

/// `NewREST` (storage/storage.go:35-58): a plain `genericregistry.Store`
/// driven by [`Strategy`].
pub fn new_store(storage: Arc<StorageBackend>) -> Store<Secret, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("", "secrets"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}
