//! Unit tests moved from `src/handlers/pod_subresources.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Body,
        extract::{ws::WebSocketUpgrade, Path, Query, Request, State},
        http::{header, StatusCode, Uri},
        response::{IntoResponse, Response},
        Extension,
    };
    pub use rusternetes_api_server::{
        handlers::node_conn::{node_conn, NodeConn},
        middleware::AuthContext,
        state::ApiServerState,
        streaming,
    };
    pub use rusternetes_common::{
        authz::{Decision, RequestAttributes},
        Error, Result,
    };
    pub use rusternetes_storage::Storage;
    pub use serde::Deserialize;
    pub use std::sync::Arc;
    pub use tracing::{debug, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::pod_subresources::*;
    #[test]
    fn portforward_kubelet_url_matches_upstream_shape() {
        let conn = NodeConn {
            host: "10.0.0.5".into(),
            port: 10250,
            scheme: "https",
        };
        // strategy.go:823-828 - capital-F path, `port=` comma-joined.
        let q = port_forward_stream_query("ports=80&ports=8080,9090").unwrap();
        assert_eq!(q, "port=80,8080,9090");
        assert_eq!(
            build_kubelet_portforward_url(&conn, "ns1", "pod1", &q).to_string(),
            "https://10.0.0.5:10250/portForward/ns1/pod1?port=80,8080,9090"
        );
        // kubectl's SPDY client sends no query: no param (strategy.go:716).
        let q = port_forward_stream_query("").unwrap();
        assert_eq!(
            build_kubelet_portforward_url(&conn, "ns1", "pod1", &q).to_string(),
            "https://10.0.0.5:10250/portForward/ns1/pod1"
        );
        assert!(port_forward_stream_query("ports=abc").is_err());
    }

    #[test]
    fn exec_kubelet_url_matches_upstream_shape() {
        let u = build_kubelet_stream_url(
            &NodeConn {
                host: "10.0.0.5".into(),
                port: 10250,
                scheme: "http",
            },
            "exec",
            "ns1",
            "pod1",
            "c1",
            "command=id&tty=false&stdin=false&stdout=true&stderr=true",
        );
        assert_eq!(
            u.to_string(),
            "http://10.0.0.5:10250/exec/ns1/pod1/c1?command=id&tty=false&stdin=false&stdout=true&stderr=true"
        );
    }

    #[test]
    fn attach_kubelet_url_matches_upstream_shape() {
        let u = build_kubelet_stream_url(
            &NodeConn {
                host: "192.168.1.10".into(),
                port: 10250,
                scheme: "http",
            },
            "attach",
            "default",
            "my-pod",
            "main",
            "stdin=true&stdout=true",
        );
        assert_eq!(
            u.to_string(),
            "http://192.168.1.10:10250/attach/default/my-pod/main?stdin=true&stdout=true"
        );
    }

    #[test]
    fn kubelet_url_no_query() {
        let u = build_kubelet_stream_url(
            &NodeConn {
                host: "10.0.0.1".into(),
                port: 10250,
                scheme: "http",
            },
            "exec",
            "kube-system",
            "coredns",
            "coredns",
            "",
        );
        assert_eq!(
            u.to_string(),
            "http://10.0.0.1:10250/exec/kube-system/coredns/coredns"
        );
    }

    #[test]
    fn logs_kubelet_url_uses_containerlogs_path() {
        let u = build_kubelet_stream_url(
            &NodeConn {
                host: "10.0.0.6".into(),
                port: 10250,
                scheme: "http",
            },
            "containerLogs",
            "ns1",
            "pod1",
            "c1",
            "tailLines=5&follow=false",
        );
        assert_eq!(
            u.to_string(),
            "http://10.0.0.6:10250/containerLogs/ns1/pod1/c1?tailLines=5&follow=false"
        );
    }
}

mod exec_query_bool_tests {
    use rusternetes_api_server::handlers::pod_subresources::parse_exec_query;

    /// Upstream decodes stdin/stdout/stderr/tty with
    /// `runtime.Convert_Slice_string_To_bool`
    /// (apimachinery/pkg/runtime/conversion.go:79-95): only absence, "0" and a
    /// case-insensitive "false" are false; every other value is true. The old
    /// `value == "true" || value == "1"` comparison dropped "t", "T", "yes"
    /// and "", so a client asking for a TTY quietly did not get one.
    #[test]
    fn stream_flags_follow_upstream_query_conversion() {
        for v in ["true", "1", "t", "T", "TRUE", "yes", ""] {
            let q = parse_exec_query(&format!("stdin={v}&stdout={v}&stderr={v}&tty={v}"));
            assert!(q.stdin, "stdin={v:?} must be true");
            assert!(q.stdout, "stdout={v:?} must be true");
            assert!(q.stderr, "stderr={v:?} must be true");
            assert!(q.tty, "tty={v:?} must be true");
        }
        for v in ["0", "false", "False", "FALSE"] {
            let q = parse_exec_query(&format!("stdin={v}&stdout={v}&stderr={v}&tty={v}"));
            assert!(!q.stdin, "stdin={v:?} must be false");
            assert!(!q.tty, "tty={v:?} must be false");
        }
    }

    #[test]
    fn absent_stream_flags_default_to_false() {
        let q = parse_exec_query("command=sh&container=c");
        assert!(!q.stdin && !q.stdout && !q.stderr && !q.tty);
        assert_eq!(q.container.as_deref(), Some("c"));
        assert_eq!(q.command, vec!["sh".to_string()]);
    }
}

mod connect_admission_step_metrics_tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::pod_subresources::*;
    use rusternetes_common::auth::TokenManager;
    use rusternetes_common::authz::AlwaysAllowAuthorizer;
    use rusternetes_common::observability::MetricsRegistry;
    use rusternetes_storage::{MemoryStorage, StorageBackend};

    fn step(ty: &str) -> u64 {
        prometheus::gather()
            .into_iter()
            .find(|f| f.name() == "apiserver_admission_step_admission_duration_seconds")
            .and_then(|mf| {
                mf.get_metric()
                    .iter()
                    .find(|m| {
                        [
                            ("type", ty),
                            ("operation", "CONNECT"),
                            ("rejected", "false"),
                        ]
                        .iter()
                        .all(|(k, v)| {
                            m.get_label()
                                .iter()
                                .any(|l| l.name() == *k && l.value() == *v)
                        })
                    })
                    .map(|m| m.get_histogram().get_sample_count())
            })
            .unwrap_or(0)
    }

    /// `WithStepMetrics` wraps the whole chain
    /// (server/options/admission.go:179) and ConnectResource admits through
    /// it (endpoints/handlers/rest.go:199-216), so a connect request is one
    /// `admit` and one `validate` observation (#3523).
    #[tokio::test]
    async fn connect_admission_observes_admit_and_validate_steps() {
        let state = ApiServerState::new(
            Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new()))),
            Arc::new(TokenManager::new(b"secret")),
            Arc::new(AlwaysAllowAuthorizer),
            Arc::new(MetricsRegistry::new()),
            true,
        );
        let user = rusternetes_common::admission::UserInfo {
            username: "u".into(),
            uid: String::new(),
            groups: vec![],
        };
        let (admit0, validate0) = (step("admit"), step("validate"));
        run_connect_admission(
            &state,
            "PodExecOptions",
            "pods/exec",
            "default",
            "p",
            serde_json::json!({"kind": "PodExecOptions"}),
            &user,
        )
        .await
        .unwrap();
        assert!(step("admit") > admit0, "admit step not observed");
        assert!(step("validate") > validate0, "validate step not observed");
    }
}
