//! Unit tests moved from `src/lib.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum_server::tls_rustls::RustlsConfig;
    pub use rusternetes_admission_webhook as admission_webhook;
    pub use rusternetes_admission_webhook::cel_evaluators as cel;
    pub use rusternetes_api_server::state::ApiServerState;
    pub use rusternetes_common::auth::TokenManager;
    pub use rusternetes_common::observability::MetricsRegistry;
    pub use rusternetes_common::tls::TlsConfig;
    pub use rusternetes_middleware as middleware;
    pub use rusternetes_middleware::cbor;
    pub use rusternetes_protobuf as protobuf;
    pub use rusternetes_storage::StorageBackend;
    pub use std::path::PathBuf;
    pub use std::sync::Arc;
    pub use tracing::{info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::*;

    #[test]
    #[serial_test::serial]
    fn prepare_tls_for_self_signed_exposes_served_cert_as_ca() {
        let old_ca_cert_path = std::env::var_os("CA_CERT_PATH");
        let old_home = std::env::var_os("HOME");
        std::env::set_var("CA_CERT_PATH", "/tmp/rusternetes-test-missing-ca-cert.pem");
        std::env::set_var("HOME", "/tmp/rusternetes-test-missing-home");

        let config = ApiServerConfig {
            tls: true,
            tls_self_signed: true,
            tls_san: "localhost,127.0.0.1".to_string(),
            ..Default::default()
        };

        let prepared = prepare_tls_for_config(&config)
            .expect("self-signed TLS should prepare")
            .expect("TLS is enabled");
        let ca_cert_pem = prepared
            .ca_cert_pem()
            .expect("self-signed TLS should expose a CA PEM")
            .to_string();
        let tls_config = prepared.into_tls_config();

        assert_eq!(tls_config.cert_pem.as_deref(), Some(ca_cert_pem.as_str()));

        match old_ca_cert_path {
            Some(value) => std::env::set_var("CA_CERT_PATH", value),
            None => std::env::remove_var("CA_CERT_PATH"),
        }
        match old_home {
            Some(value) => std::env::set_var("HOME", value),
            None => std::env::remove_var("HOME"),
        }
    }
}

mod service_account_args_tests {
    use super::__parent::*;
    use clap::Parser;
    use rusternetes_api_server::*;

    #[derive(Parser, Debug)]
    struct Wrap {
        #[command(flatten)]
        sa: ServiceAccountArgs,
    }

    #[test]
    fn flags_map_to_options_2713() {
        let w = Wrap::try_parse_from([
            "x",
            "--service-account-key-file=/k1",
            "--service-account-key-file=/k2",
            "--service-account-signing-key-file=/s",
            "--service-account-issuer=https://i",
            "--api-audiences=a,b",
            "--service-account-max-token-expiration=24h",
        ])
        .unwrap();
        let o = w.sa.to_options();
        assert_eq!(o.key_files, ["/k1", "/k2"]);
        assert_eq!(o.signing_key_file.as_deref(), Some("/s"));
        assert_eq!(o.issuers, ["https://i"]);
        assert_eq!(o.api_audiences, ["a", "b"]);
        assert_eq!(
            o.max_expiration,
            Some(std::time::Duration::from_secs(86400))
        );
    }

    /// `--service-account-extend-token-expiration` defaults to true
    /// (authentication.go:227) and takes `=false` (a pflag bool).
    #[test]
    fn extend_token_expiration_flag_defaults_true() {
        let o = |a: &[&str]| Wrap::try_parse_from(a).unwrap().sa.to_options();
        assert!(o(&["x"]).extend_expiration);
        assert!(ServiceAccountArgs::default().to_options().extend_expiration);
        assert!(!o(&["x", "--service-account-extend-token-expiration=false"]).extend_expiration);
    }

    /// `--service-account-lookup` defaults to true (authentication.go:226) and
    /// takes `=false` (a pflag bool).
    #[test]
    fn lookup_flag_defaults_true() {
        assert!(Wrap::try_parse_from(["x"]).unwrap().sa.to_options().lookup);
        assert!(ServiceAccountArgs::default().to_options().lookup);
        let w = Wrap::try_parse_from(["x", "--service-account-lookup=false"]).unwrap();
        assert!(!w.sa.to_options().lookup);
        let w = Wrap::try_parse_from(["x", "--service-account-lookup"]).unwrap();
        assert!(w.sa.to_options().lookup);
    }

    #[test]
    fn bad_duration_rejected() {
        assert!(Wrap::try_parse_from(["x", "--service-account-max-token-expiration=-1h"]).is_err());
        assert!(Wrap::try_parse_from(["x", "--service-account-max-token-expiration=abc"]).is_err());
    }
}
