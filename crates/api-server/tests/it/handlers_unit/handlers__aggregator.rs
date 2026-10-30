//! Unit tests moved from `src/handlers/aggregator.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Body,
        http::{HeaderMap, HeaderValue, Method, StatusCode},
        response::Response,
    };
    pub use rusternetes_api_server::{middleware::AuthContext, state::ApiServerState};
    pub use rusternetes_storage::{build_prefix, Storage};
    pub use serde_json::Value;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::{debug, warn};
}

mod proxy_header_tests {
    use rusternetes_api_server::handlers::aggregator::header_key_escape;

    // Regression: SA identities carry extra keys with `/`, which is illegal in
    // an HTTP header name. Without escaping, the aggregator proxy fails to build
    // every request ("builder error" -> 503) and the Aggregator conformance test
    // can never reach the sample-apiserver.
    #[test]
    fn escapes_slash_in_extra_key() {
        assert_eq!(
            header_key_escape("authentication.kubernetes.io/pod-name"),
            "authentication.kubernetes.io%2Fpod-name"
        );
    }

    #[test]
    fn leaves_legal_token_chars_untouched() {
        // tchar set minus `%` (which is force-escaped).
        assert_eq!(header_key_escape("Ab9-._~|"), "Ab9-._~|");
    }

    #[test]
    fn force_escapes_percent() {
        assert_eq!(header_key_escape("a%b"), "a%25b");
    }

    #[test]
    fn escapes_other_illegal_bytes() {
        assert_eq!(header_key_escape("a/b c"), "a%2Fb%20c");
    }
}

mod proxy_client_identity_tests {
    use rusternetes_api_server::handlers::aggregator::load_proxy_client_identity_from;

    // Regression: the aggregation proxy MUST be able to load the kubeadm
    // front-proxy client credential (upstream --proxy-client-cert-file).
    // Without presenting it, the extension apiserver's requestheader
    // authenticator treats every proxied request as system:anonymous → 403 →
    // the Aggregator conformance test polls to timeout.
    #[test]
    fn loads_identity_from_pem_pair() {
        let cert = rcgen::generate_simple_self_signed(vec!["front-proxy-client".to_string()])
            .expect("generate test cert");
        let dir = std::env::temp_dir().join(format!("rn-fpc-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert_path = dir.join("front-proxy-client.crt");
        let key_path = dir.join("front-proxy-client.key");
        std::fs::write(&cert_path, cert.cert.pem()).unwrap();
        std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();

        let id = load_proxy_client_identity_from(
            cert_path.to_str().unwrap(),
            key_path.to_str().unwrap(),
        );
        assert!(id.is_some(), "cert+key PEM pair must load as an identity");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Absent files (rusternetes' own cluster) → None, proxy stays client-auth-less.
    #[test]
    fn absent_files_yield_none() {
        assert!(load_proxy_client_identity_from(
            "/nonexistent/front-proxy-client.crt",
            "/nonexistent/front-proxy-client.key"
        )
        .is_none());
    }
}
