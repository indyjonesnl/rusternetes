//! Unit tests moved from `src/registry/core/secret.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::resources::Secret;
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::secret::{validate_secret, validate_secret_update};
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::secret::*;
    use std::collections::HashMap;

    fn secret(type_: Option<&str>) -> Secret {
        let mut s = Secret::new("s", "default");
        s.secret_type = type_.map(str::to_string);
        s
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        let ctx = RequestContext::new(Some("default"));
        assert!(Strategy.default_garbage_collection_policy(&ctx).is_none());
        assert!(Strategy.graceful().is_none());
    }

    /// `SetDefaults_Secret`, then the conversion's "StringData overwrites
    /// Data".
    #[test]
    fn convert_to_internal_defaults_type_and_folds_string_data() {
        let mut s = secret(Some(""));
        s.data = Some(HashMap::from([
            ("a".to_string(), b"old".to_vec()),
            ("b".to_string(), b"kept".to_vec()),
        ]));
        s.string_data = Some(HashMap::from([("a".to_string(), "new".to_string())]));
        convert_to_internal(&mut s);
        assert_eq!(s.secret_type.as_deref(), Some("Opaque"));
        assert!(s.string_data.is_none());
        let data = s.data.unwrap();
        assert_eq!(data["a"], b"new");
        assert_eq!(data["b"], b"kept");

        let mut typed = secret(Some("kubernetes.io/basic-auth"));
        convert_to_internal(&mut typed);
        assert_eq!(
            typed.secret_type.as_deref(),
            Some("kubernetes.io/basic-auth")
        );
    }

    /// strategy.go:81-83.
    #[test]
    fn prepare_for_update_keeps_the_old_type_when_none_is_given() {
        let ctx = RequestContext::new(Some("default"));
        let old = secret(Some("kubernetes.io/basic-auth"));
        let mut new = secret(None);
        Strategy.prepare_for_update(&ctx, &mut new, &old);
        assert_eq!(new.secret_type.as_deref(), Some("kubernetes.io/basic-auth"));

        let mut explicit = secret(Some("Opaque"));
        Strategy.prepare_for_update(&ctx, &mut explicit, &old);
        assert_eq!(explicit.secret_type.as_deref(), Some("Opaque"));
    }

    /// `warningsForSecret` warns on a TLS secret with a mismatched pair and
    /// only on TLS secrets.
    #[test]
    fn tls_secrets_with_a_bad_pair_warn() {
        let ctx = RequestContext::new(Some("default"));
        let mut tls = secret(Some(SECRET_TYPE_TLS));
        tls.data = Some(HashMap::from([
            (TLS_CERT_KEY.to_string(), b"not pem".to_vec()),
            (TLS_PRIVATE_KEY_KEY.to_string(), b"not pem".to_vec()),
        ]));
        assert_eq!(
            Strategy.warnings_on_create(&ctx, &tls),
            vec!["tls: failed to find any PEM data in certificate input".to_string()]
        );
        assert_eq!(Strategy.warnings_on_update(&ctx, &tls, &tls).len(), 1);

        let mut opaque = tls.clone();
        opaque.secret_type = Some("Opaque".to_string());
        assert!(Strategy.warnings_on_create(&ctx, &opaque).is_empty());
    }

    #[test]
    fn tls_secrets_with_a_matching_pair_do_not_warn() {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["example.com".to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let mut tls = secret(Some(SECRET_TYPE_TLS));
        tls.data = Some(HashMap::from([
            (TLS_CERT_KEY.to_string(), cert.pem().into_bytes()),
            (
                TLS_PRIVATE_KEY_KEY.to_string(),
                key.serialize_pem().into_bytes(),
            ),
        ]));
        let ctx = RequestContext::new(Some("default"));
        assert!(Strategy.warnings_on_create(&ctx, &tls).is_empty());
    }
}
