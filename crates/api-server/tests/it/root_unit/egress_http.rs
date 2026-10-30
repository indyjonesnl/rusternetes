//! Unit tests moved from `src/egress_http.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use bytes::Bytes;
    pub use http_body_util::{BodyExt, Full};
    pub use hyper::Uri;
    pub use hyper_util::client::legacy::connect::{Connected, Connection};
    pub use hyper_util::rt::{TokioExecutor, TokioIo};
    pub use rusternetes_api_server::egress_selector::{Conn, Dialer};
    pub use std::future::Future;
    pub use std::pin::Pin;
    pub use std::sync::Arc;
    pub use std::task::{Context, Poll};
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::egress_http::*;
    use rusternetes_api_server::egress_selector::{
        DialerCreator, MetricsOptions, Proxier, ProxyServerConnector,
    };
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A proxy server connector whose proxier records the requested address
    /// and tunnels to `target` (a local listener), like a konnectivity agent
    /// reaching the real destination.
    struct Tunnel {
        target: String,
        seen: Arc<Mutex<Vec<String>>>,
        fail: bool,
    }
    struct TunnelProxier {
        target: String,
        seen: Arc<Mutex<Vec<String>>>,
        fail: bool,
    }
    #[async_trait::async_trait]
    impl ProxyServerConnector for Tunnel {
        async fn connect(&self) -> Result<Box<dyn Proxier>, String> {
            Ok(Box::new(TunnelProxier {
                target: self.target.clone(),
                seen: self.seen.clone(),
                fail: self.fail,
            }))
        }
    }
    #[async_trait::async_trait]
    impl Proxier for TunnelProxier {
        async fn proxy(&self, addr: &str) -> Result<Conn, String> {
            self.seen.lock().unwrap().push(addr.to_string());
            if self.fail {
                return Err("agent unreachable".into());
            }
            let c = tokio::net::TcpStream::connect(&self.target)
                .await
                .map_err(|e| e.to_string())?;
            Ok(Box::new(c))
        }
    }

    fn tunnel_dialer(target: String, fail: bool) -> (Dialer, Arc<Mutex<Vec<String>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let d = DialerCreator {
            connector: Some(Arc::new(Tunnel {
                target,
                seen: seen.clone(),
                fail,
            })),
            direct: false,
            options: MetricsOptions {
                transport: "tcp".into(),
                protocol: "HTTPConnect".into(),
            },
        }
        .create_dialer();
        (d, seen)
    }

    /// Serve one request on `s`: returns the head and body it carried, and
    /// answers `200 {"ok":true}` after `delay`.
    async fn serve_one<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
        mut s: S,
        delay: Duration,
    ) -> (String, Vec<u8>) {
        let mut buf = Vec::new();
        let mut b = [0u8; 1];
        while !buf.ends_with(b"\r\n\r\n") {
            s.read_exact(&mut b).await.unwrap();
            buf.push(b[0]);
        }
        let head = String::from_utf8(buf).unwrap();
        let len = head
            .lines()
            .find_map(|l| {
                l.to_lowercase()
                    .strip_prefix("content-length:")
                    .map(|v| v.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let mut body = vec![0u8; len];
        s.read_exact(&mut body).await.unwrap();
        tokio::time::sleep(delay).await;
        let out = br#"{"ok":true}"#;
        let _ = s
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: application/json\r\nretry-after: 3\r\nconnection: close\r\n\r\n",
                    out.len()
                )
                .as_bytes(),
            )
            .await;
        let _ = s.write_all(out).await;
        let _ = s.flush().await;
        (head, body)
    }

    fn client(dialer: Dialer, timeout: Duration) -> EgressHttpClient {
        let tls = tls_client_config(None, false, None).unwrap();
        EgressHttpClient::new(dialer, tls, timeout)
    }

    #[tokio::test]
    async fn http_post_is_dialed_through_the_egress_dialer() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (dialer, seen) = tunnel_dialer(l.local_addr().unwrap().to_string(), false);
        let server = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            serve_one(s, Duration::ZERO).await
        });
        // The hostname does not resolve locally: only the proxy can reach it.
        let resp = client(dialer, Duration::from_secs(5))
            .post_json(
                "http://webhook.cluster.invalid:8080/authz",
                &[("authorization", "Bearer tok".to_string())],
                br#"{"a":1}"#.to_vec(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(&resp.body[..], br#"{"ok":true}"#);
        assert_eq!(resp.headers.get("retry-after").unwrap(), "3");
        assert_eq!(*seen.lock().unwrap(), vec!["webhook.cluster.invalid:8080"]);
        let (head, body) = server.await.unwrap();
        assert!(head.starts_with("POST /authz HTTP/1.1\r\n"), "{head}");
        assert!(head
            .to_lowercase()
            .contains("authorization: bearer tok\r\n"));
        assert!(head
            .to_lowercase()
            .contains("content-type: application/json\r\n"));
        assert_eq!(body, br#"{"a":1}"#);
    }

    fn self_signed(name: &str) -> (rcgen::Certificate, rcgen::KeyPair) {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec![name.to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        (cert, key)
    }

    async fn tls_server(
        name: &str,
    ) -> (String, String, tokio::task::JoinHandle<(String, Vec<u8>)>) {
        let (cert, key) = self_signed(name);
        let cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
        )
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(cfg));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = l.local_addr().unwrap().to_string();
        let h = tokio::spawn(async move {
            let (tcp, _) = l.accept().await.unwrap();
            let s = acceptor.accept(tcp).await.unwrap();
            serve_one(s, Duration::ZERO).await
        });
        (target, cert.pem(), h)
    }

    #[tokio::test]
    async fn https_is_verified_over_the_dialed_connection() {
        let (target, ca, server) = tls_server("webhook.example").await;
        let (dialer, seen) = tunnel_dialer(target, false);
        let c = EgressHttpClient::new(
            dialer,
            tls_client_config(Some(ca.as_bytes()), false, None).unwrap(),
            Duration::from_secs(5),
        );
        let resp = c
            .post_json("https://webhook.example/x", &[], b"{}".to_vec())
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        // The default port of https, as `host:port` for the proxy.
        assert_eq!(*seen.lock().unwrap(), vec!["webhook.example:443"]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn https_with_an_untrusted_certificate_fails_unless_insecure() {
        let (target, _ca, server) = tls_server("webhook.example").await;
        let (dialer, _) = tunnel_dialer(target.clone(), false);
        let (_, other_ca) = (0, self_signed("other").0.pem());
        let c = EgressHttpClient::new(
            dialer,
            tls_client_config(Some(other_ca.as_bytes()), false, None).unwrap(),
            Duration::from_secs(5),
        );
        let err = c
            .post_json("https://webhook.example/x", &[], b"{}".to_vec())
            .await
            .unwrap_err();
        assert!(!err.timeout, "{err}");
        server.abort();

        let (target, _ca, server) = tls_server("webhook.example").await;
        let (dialer, _) = tunnel_dialer(target, false);
        let c = EgressHttpClient::new(
            dialer,
            tls_client_config(None, true, None).unwrap(),
            Duration::from_secs(5),
        );
        let resp = c
            .post_json("https://webhook.example/x", &[], b"{}".to_vec())
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn dial_failure_surfaces_as_an_error() {
        let (dialer, seen) = tunnel_dialer("127.0.0.1:1".into(), true);
        let err = client(dialer, Duration::from_secs(5))
            .post_json("http://10.0.0.7:9000/", &[], b"{}".to_vec())
            .await
            .unwrap_err();
        assert!(err.message.contains("agent unreachable"), "{err}");
        assert!(!err.timeout);
        assert_eq!(*seen.lock().unwrap(), vec!["10.0.0.7:9000"]);
    }

    #[tokio::test]
    async fn slow_response_times_out() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (dialer, _) = tunnel_dialer(l.local_addr().unwrap().to_string(), false);
        let server = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            serve_one(s, Duration::from_secs(5)).await
        });
        let err = client(dialer, Duration::from_millis(200))
            .post_json("http://slow.invalid/", &[], b"{}".to_vec())
            .await
            .unwrap_err();
        assert!(err.timeout, "{err}");
        server.abort();
    }

    #[test]
    fn tls_config_rejects_a_ca_without_certificates() {
        assert!(tls_client_config(Some(b"not pem"), false, None).is_err());
        assert!(tls_client_config(None, false, Some(b"not pem")).is_err());
    }
}
