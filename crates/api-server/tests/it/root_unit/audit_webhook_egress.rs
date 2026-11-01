//! The audit webhook dials through the egress selector's ControlPlane
//! dialer (`options/audit.go:300-312` `newWebhookBackend` ->
//! `audit.NewBackend`'s `webhook.NewGenericWebhook(..., egressDialer)`;
//! `util/webhook/webhook.go:157`).
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::sync::{Arc, Mutex};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::audit_webhook::*;
    use rusternetes_api_server::egress_selector::{
        set_egress_selector, Conn, Dialer, DialerCreator, EgressSelector, EgressType,
        MetricsOptions, Proxier, ProxyServerConnector,
    };
    use rusternetes_common::audit::{AuditEvent, AuditLevel, AuditStage, UserInfo};
    use rusternetes_common::wait::Backoff;
    use std::collections::HashMap;
    use std::time::Duration;

    struct Tunnel {
        target: String,
        seen: Arc<Mutex<Vec<String>>>,
    }
    struct TunnelProxier {
        target: String,
        seen: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait::async_trait]
    impl ProxyServerConnector for Tunnel {
        async fn connect(&self) -> Result<Box<dyn Proxier>, String> {
            Ok(Box::new(TunnelProxier {
                target: self.target.clone(),
                seen: self.seen.clone(),
            }))
        }
    }
    #[async_trait::async_trait]
    impl Proxier for TunnelProxier {
        async fn proxy(&self, addr: &str) -> Result<Conn, String> {
            self.seen.lock().unwrap().push(addr.to_string());
            let c = tokio::net::TcpStream::connect(&self.target)
                .await
                .map_err(|e| e.to_string())?;
            Ok(Box::new(c))
        }
    }

    fn event() -> AuditEvent {
        AuditEvent {
            api_version: "audit.k8s.io/v1".into(),
            kind: "Event".into(),
            level: AuditLevel::Metadata,
            audit_id: uuid::Uuid::new_v4().to_string(),
            stage: AuditStage::ResponseComplete,
            request_uri: "/api".into(),
            verb: "get".into(),
            user: UserInfo {
                username: "u".into(),
                uid: String::new(),
                groups: vec![],
                extra: None,
            },
            user_agent: None,
            source_ips: vec![],
            object_ref: None,
            response_status: None,
            response_object: None,
            request_object: None,
            impersonated_user: None,
            request_received_timestamp: chrono::Utc::now(),
            stage_timestamp: chrono::Utc::now(),
            annotations: Default::default(),
        }
    }

    #[tokio::test]
    async fn webhook_dials_through_control_plane_egress() {
        use axum::{extract::State, routing::post, Json, Router};
        let got: Arc<Mutex<Vec<serde_json::Value>>> = Default::default();
        async fn h(
            State(g): State<Arc<Mutex<Vec<serde_json::Value>>>>,
            Json(v): Json<serde_json::Value>,
        ) {
            g.lock().unwrap().push(v);
        }
        let app = Router::new().route("/", post(h)).with_state(got.clone());
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = l.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });

        let seen = Arc::new(Mutex::new(Vec::new()));
        let dialer = DialerCreator {
            connector: Some(Arc::new(Tunnel {
                target,
                seen: seen.clone(),
            })),
            direct: false,
            options: MetricsOptions {
                transport: "tcp".into(),
                protocol: "HTTPConnect".into(),
            },
        }
        .create_dialer();
        let mut m = HashMap::new();
        m.insert(EgressType::ControlPlane, dialer);
        set_egress_selector(Some(EgressSelector::with_map(m)));

        // The host does not resolve: only the egress dialer can reach it.
        let dir = std::env::temp_dir().join(format!("audit-egress-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let kc = dir.join("kubeconfig");
        std::fs::write(
            &kc,
            "apiVersion: v1\nkind: Config\nclusters:\n- name: \"\"\n  cluster:\n    server: http://audit-sink.invalid:8080/\n    insecure-skip-tls-verify: true\n",
        )
        .unwrap();
        let b = WebhookBackend::new(
            &kc,
            AUDIT_GROUP_VERSION,
            Backoff {
                duration: Duration::from_millis(10),
                factor: 1.5,
                jitter: 0.2,
                steps: 2,
            },
        )
        .unwrap();
        b.try_process_events(&[event()]).await.unwrap();

        assert_eq!(*seen.lock().unwrap(), vec!["audit-sink.invalid:8080"]);
        let got = got.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0]["kind"], "EventList");
    }
}
