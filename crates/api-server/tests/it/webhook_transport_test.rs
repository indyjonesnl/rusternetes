//! The webhook client's target and dial
//! (staging/src/k8s.io/apiserver/pkg/util/webhook/client.go
//! `hookClientConfig`, authentication.go
//! `NewDefaultAuthenticationInfoResolverWrapper`).

use rusternetes_admission_webhook::transport::{
    Dial, ServiceRef, WebhookHttpRequest, WebhookTarget, WebhookTransport,
};
use rusternetes_admission_webhook::AdmissionWebhookClient;
use rusternetes_api_server::webhook_transport::EgressWebhookTransport;
use rusternetes_common::resources::{ServiceReference, WebhookClientConfig};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn service_config(port: Option<i32>, path: Option<&str>) -> WebhookClientConfig {
    WebhookClientConfig {
        url: None,
        service: Some(ServiceReference {
            namespace: "ns".into(),
            name: "hook".into(),
            path: path.map(str::to_string),
            port,
        }),
        ca_bundle: None,
    }
}

#[test]
fn service_port_defaults_to_443_when_unset_or_zero() {
    // client.go:176-180: "Default to port 443 if no service port is specified".
    let client = AdmissionWebhookClient::new();
    for port in [None, Some(0)] {
        let t = client
            .build_webhook_target(&service_config(port, Some("/v")))
            .unwrap();
        assert_eq!(t.url, "https://hook.ns.svc:443/v");
        assert_eq!(
            t.service,
            Some(ServiceRef {
                namespace: "ns".into(),
                name: "hook".into(),
                port: 443
            })
        );
    }
}

#[test]
fn service_is_checked_before_url() {
    // client.go:171 `if cc.Service != nil` precedes the URL branch.
    let mut cfg = service_config(Some(8443), None);
    cfg.url = Some("https://example.com/x".into());
    let t = AdmissionWebhookClient::new()
        .build_webhook_target(&cfg)
        .unwrap();
    assert_eq!(t.url, "https://hook.ns.svc:8443/");
    assert!(t.service.is_some());
}

#[test]
fn url_config_has_no_service() {
    let cfg = WebhookClientConfig {
        url: Some("https://example.com/x".into()),
        service: None,
        ca_bundle: None,
    };
    let t = AdmissionWebhookClient::new()
        .build_webhook_target(&cfg)
        .unwrap();
    assert_eq!(t, WebhookTarget::from_url("https://example.com/x".into()));
}

async fn put_service(storage: &MemoryStorage, cluster_ip: &str) {
    let svc = serde_json::json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": { "namespace": "ns", "name": "hook" },
        "spec": { "clusterIP": cluster_ip, "ports": [{ "port": 443 }] },
    });
    storage
        .create::<rusternetes_common::resources::Service>(
            &build_key("services", Some("ns"), "hook"),
            &serde_json::from_value(svc).unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn service_dial_is_what_the_resolver_returns() {
    // client.go:203-212: `addr == host` becomes `ResolveEndpoint`'s host.
    let storage = Arc::new(MemoryStorage::new());
    put_service(&storage, "10.96.0.7").await;
    let client = AdmissionWebhookClient::new();
    let target = client
        .build_webhook_target(&service_config(None, None))
        .unwrap();
    let target = AdmissionWebhookClient::resolve_webhook_target(target, &storage).await;
    assert_eq!(target.dial, Dial::Resolved("10.96.0.7:443".into()));
}

#[tokio::test]
async fn unresolvable_service_is_a_dial_error_not_a_fallback() {
    // The resolver's error is returned by `cfg.Dial`; the call fails, there
    // is no fall back to some other address.
    let storage = Arc::new(MemoryStorage::new());
    let client = AdmissionWebhookClient::new();
    let target = client
        .build_webhook_target(&service_config(None, None))
        .unwrap();
    let target = AdmissionWebhookClient::resolve_webhook_target(target, &storage).await;
    assert!(matches!(target.dial, Dial::Failed(_)), "{:?}", target.dial);

    let err = EgressWebhookTransport::new(None)
        .post_json(WebhookHttpRequest {
            target: &target,
            body: b"{}".to_vec(),
            timeout: Duration::from_secs(5),
            ca_bundle: None,
        })
        .await
        .unwrap_err();
    assert!(err
        .message
        .starts_with("Post \"https://hook.ns.svc:443/\": "));
}

#[tokio::test]
async fn url_webhook_posts_json_over_the_default_dial() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = s.read(&mut buf).await.unwrap();
        let req = String::from_utf8_lossy(&buf[..n]).to_lowercase();
        assert!(req.starts_with("post /hook?timeout=3s http/1.1"), "{req}");
        assert!(req.contains("content-type: application/json"), "{req}");
        let body = r#"{"ok":true}"#;
        s.write_all(
            format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    });
    let target = WebhookTarget::from_url(format!("http://{addr}/hook?timeout=3s"));
    let resp = EgressWebhookTransport::new(None)
        .post_json(WebhookHttpRequest {
            target: &target,
            body: br#"{"a":1}"#.to_vec(),
            timeout: Duration::from_secs(5),
            ca_bundle: None,
        })
        .await
        .unwrap();
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, br#"{"ok":true}"#);
}

#[tokio::test]
async fn service_webhook_dials_the_resolved_address() {
    // The URL host stays `<name>.<ns>.svc` (the TLS server name), the TCP
    // connection goes to the resolved address.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut hello = [0u8; 5];
        s.read_exact(&mut hello).await.unwrap();
        hello
    });
    let client = AdmissionWebhookClient::new();
    let mut target = client
        .build_webhook_target(&service_config(Some(8443), Some("/v")))
        .unwrap();
    target.dial = Dial::Resolved(addr.to_string());
    let err = EgressWebhookTransport::new(None)
        .post_json(WebhookHttpRequest {
            target: &target,
            body: b"{}".to_vec(),
            timeout: Duration::from_secs(5),
            ca_bundle: None,
        })
        .await
        .unwrap_err();
    // The peer is not a TLS server, so the exchange fails; what matters is
    // that it was the resolved address that got the ClientHello.
    assert!(!err.message.is_empty());
    let hello = accepted.await.unwrap();
    assert_eq!(hello[0], 0x16, "a TLS handshake record");
}

#[tokio::test]
async fn ca_bundle_is_the_only_trust_root() {
    // `rootCertPool(caData)`: a bundle with no certificate is an error, not
    // "accept anything".
    let target = WebhookTarget::from_url("https://127.0.0.1:1/".into());
    let err = EgressWebhookTransport::new(None)
        .post_json(WebhookHttpRequest {
            target: &target,
            body: b"{}".to_vec(),
            timeout: Duration::from_secs(5),
            ca_bundle: Some(b"not a pem"),
        })
        .await
        .unwrap_err();
    assert!(
        err.message.contains("no valid certificates"),
        "{}",
        err.message
    );
}
