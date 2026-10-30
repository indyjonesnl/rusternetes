//! Unit tests moved from `src/handlers/proxy.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Body,
        extract::{OriginalUri, Path, State},
        http::{HeaderMap, Method, StatusCode, Uri},
        response::Response,
        Extension,
    };
    pub use rusternetes_api_server::{middleware::AuthContext, state::ApiServerState};
    pub use rusternetes_common::{
        authz::{Decision, RequestAttributes},
        Result,
    };
    pub use rusternetes_storage::Storage;
    pub use std::sync::Arc;
    pub use tracing::{debug, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::proxy::*;

    #[test]
    fn test_hop_by_hop_headers() {
        assert!(is_hop_by_hop_header("Connection"));
        assert!(is_hop_by_hop_header("Keep-Alive"));
        assert!(is_hop_by_hop_header("transfer-encoding"));
        assert!(!is_hop_by_hop_header("Content-Type"));
        assert!(!is_hop_by_hop_header("Authorization"));
    }

    #[test]
    fn test_split_scheme_name_port_plain_name() {
        assert_eq!(split_scheme_name_port("myname"), Some(("", "myname", "")));
    }

    #[test]
    fn test_split_scheme_name_port_name_and_port() {
        assert_eq!(
            split_scheme_name_port("myname:8080"),
            Some(("", "myname", "8080"))
        );
    }

    #[test]
    fn test_split_scheme_name_port_scheme_name_port() {
        assert_eq!(
            split_scheme_name_port("http:myname:8080"),
            Some(("http", "myname", "8080"))
        );
        assert_eq!(
            split_scheme_name_port("https:myname:443"),
            Some(("https", "myname", "443"))
        );
    }

    #[test]
    fn test_split_scheme_name_port_invalid() {
        // Empty name
        assert_eq!(split_scheme_name_port(""), None);
        // Empty name with port
        assert_eq!(split_scheme_name_port(":8080"), None);
        // Invalid scheme
        assert_eq!(split_scheme_name_port("ftp:myname:21"), None);
        // Empty name with valid scheme
        assert_eq!(split_scheme_name_port("http::8080"), None);
    }

    #[test]
    fn test_http_method_to_verb_get_and_head() {
        // GET/HEAD map to "get"; any other method has no verb
        // (requestinfo.go:177-190).
        assert_eq!(http_method_to_verb(&Method::GET), "get");
        assert_eq!(http_method_to_verb(&Method::HEAD), "get");
        assert_eq!(http_method_to_verb(&Method::OPTIONS), "");
    }

    #[test]
    fn test_http_method_to_verb_mutations() {
        assert_eq!(http_method_to_verb(&Method::POST), "create");
        assert_eq!(http_method_to_verb(&Method::PUT), "update");
        assert_eq!(http_method_to_verb(&Method::PATCH), "patch");
        assert_eq!(http_method_to_verb(&Method::DELETE), "delete");
    }

    #[test]
    fn test_extract_proxy_suffix_root_no_slash() {
        // /proxy with no trailing components — suffix is empty.
        let suffix = extract_proxy_suffix("/api/v1/namespaces/ns/pods/p/proxy", "pods");
        assert_eq!(suffix, "");
    }

    #[test]
    fn test_extract_proxy_suffix_root_trailing_slash() {
        // /proxy/ — suffix is empty after the trailing slash strip.
        // (The forwarder appends `/{suffix}` so /proxy and /proxy/ both
        // produce a URL ending in `/` which is what backends expect.)
        let suffix = extract_proxy_suffix("/api/v1/namespaces/ns/pods/p/proxy/", "pods");
        assert_eq!(suffix, "");
    }

    #[test]
    fn test_extract_proxy_suffix_path_no_trailing_slash() {
        let suffix = extract_proxy_suffix("/api/v1/namespaces/ns/pods/p/proxy/foo/bar", "pods");
        assert_eq!(suffix, "foo/bar");
    }

    #[test]
    fn test_extract_proxy_suffix_preserves_trailing_slash() {
        // Critical for conformance: /foo/ must arrive at the backend as /foo/.
        let suffix = extract_proxy_suffix("/api/v1/namespaces/ns/pods/p/proxy/foo/", "pods");
        assert_eq!(suffix, "foo/");
    }

    #[test]
    fn test_extract_proxy_suffix_services() {
        let suffix = extract_proxy_suffix(
            "/api/v1/namespaces/ns/services/svc:http/proxy/x/",
            "services",
        );
        assert_eq!(suffix, "x/");
    }

    #[test]
    fn test_extract_proxy_suffix_with_port_in_name() {
        // The pod/service name may include a port like `mypod:8080`.
        let suffix =
            extract_proxy_suffix("/api/v1/namespaces/ns/pods/mypod:8080/proxy/health", "pods");
        assert_eq!(suffix, "health");
    }

    #[test]
    fn test_extract_proxy_suffix_node() {
        let suffix = extract_proxy_suffix("/api/v1/nodes/node-1/proxy/stats/summary", "nodes");
        assert_eq!(suffix, "stats/summary");
    }

    /// Regression: node-proxy URLs that embed the kubelet port in the
    /// resource id must still surface the path suffix unchanged. The
    /// upstream e2e framework constructs URLs of the form
    /// `/api/v1/nodes/<name>:<port>/proxy/<path>`; `extract_proxy_suffix`
    /// is invoked with the verbatim path so the `:<port>` segment must
    /// not bleed into the returned suffix.
    /// K8s ref: test/e2e/framework/node/wait.go uses `node.Name + ":<port>"`.
    #[test]
    fn test_extract_proxy_suffix_node_name_with_port() {
        let suffix = extract_proxy_suffix("/api/v1/nodes/node-1:10250/proxy/pods", "nodes");
        assert_eq!(suffix, "pods");
    }

    /// Regression: when the port segment of a `<name>:<port>` resource id
    /// is non-numeric, `proxy_node` must fall back to the node-advertised
    /// kubelet port rather than crashing or proxying to port 0. We assert
    /// the parse step here so the fallback chain in `proxy_node` is
    /// driven by the same `port.parse::<u16>().ok()` contract used by
    /// `proxy_service` / `proxy_pod`.
    /// K8s ref: pkg/registry/core/node/strategy.go ResourceLocation —
    /// portReq carries the raw string; numeric validation is implicit
    /// in the downstream URL build.
    #[test]
    fn test_split_scheme_name_port_invalid_port_string() {
        let parsed = split_scheme_name_port("node-1:invalid-port");
        assert_eq!(parsed, Some(("", "node-1", "invalid-port")));
        // ...and the port-numeric guard the handler uses to gate the
        // override falls back to None.
        let override_port: Option<u16> = parsed.and_then(|(_, _, p)| p.parse::<u16>().ok());
        assert!(override_port.is_none());
    }

    #[test]
    fn test_extract_proxy_suffix_unrelated_path() {
        // No proxy segment — return empty.
        let suffix = extract_proxy_suffix("/api/v1/namespaces/ns/pods/p", "pods");
        assert_eq!(suffix, "");
    }

    #[test]
    fn test_extract_proxy_suffix_encoded_chars_preserved() {
        // Path encoding such as %20 must pass through untouched.
        let suffix =
            extract_proxy_suffix("/api/v1/namespaces/ns/pods/p/proxy/with%20space", "pods");
        assert_eq!(suffix, "with%20space");
    }
}
