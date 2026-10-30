//! Unit tests moved from `src/egress_selector.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::validation::field::{
        BadValue, Error as FieldError, ErrorList, Path,
    };
    pub use std::collections::BTreeSet;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::egress_selector::*;
    use rusternetes_common::validation::field::ErrorType;
    use std::collections::HashMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn tmp(name: &str, contents: &str) -> String {
        let p = std::env::temp_dir().join(format!(
            "egress-selector-{}-{}-{name}",
            std::process::id(),
            contents.len()
        ));
        std::fs::write(&p, contents).unwrap();
        p.to_string_lossy().into_owned()
    }

    fn sel(name: &str, protocol: &str, transport: Option<Transport>) -> EgressSelection {
        EgressSelection {
            name: name.into(),
            connection: Connection {
                proxy_protocol: protocol.into(),
                transport,
            },
        }
    }

    fn cfg(selections: Vec<EgressSelection>) -> EgressSelectorConfiguration {
        EgressSelectorConfiguration {
            egress_selections: Some(selections),
        }
    }

    fn tls(dir: &str) -> TlsConfig {
        TlsConfig {
            ca_bundle: format!("{dir}/ca.crt"),
            client_key: format!("{dir}/client.key"),
            client_cert: format!("{dir}/client.crt"),
        }
    }

    fn tcp(url: &str, tls: Option<TlsConfig>) -> Transport {
        Transport {
            tcp: Some(TcpTransport {
                url: url.into(),
                tls_config: tls,
            }),
            uds: None,
        }
    }

    fn uds(name: &str) -> Transport {
        Transport {
            tcp: None,
            uds: Some(UdsTransport {
                uds_name: name.into(),
            }),
        }
    }

    // ---- config_test.go TestReadEgressSelectorConfiguration -------------

    const V1BETA1: &str = r#"
apiVersion: apiserver.k8s.io/v1beta1
kind: EgressSelectorConfiguration
egressSelections:
- name: "cluster"
  connection:
    proxyProtocol: "HTTPConnect"
    transport:
      tcp:
        url: "https://127.0.0.1:8131"
        tlsConfig:
          caBundle: "/etc/srv/kubernetes/pki/konnectivity-server/ca.crt"
          clientKey: "/etc/srv/kubernetes/pki/konnectivity-server/client.key"
          clientCert: "/etc/srv/kubernetes/pki/konnectivity-server/client.crt"
- name: "controlplane"
  connection:
    proxyProtocol: "HTTPConnect"
    transport:
      tcp:
        url: "https://127.0.0.1:8132"
        tlsConfig:
          caBundle: "/etc/srv/kubernetes/pki/konnectivity-server-master/ca.crt"
          clientKey: "/etc/srv/kubernetes/pki/konnectivity-server-master/client.key"
          clientCert: "/etc/srv/kubernetes/pki/konnectivity-server-master/client.crt"
- name: "etcd"
  connection:
    proxyProtocol: "Direct"
"#;

    fn expected_v1beta1() -> EgressSelectorConfiguration {
        cfg(vec![
            sel(
                "cluster",
                "HTTPConnect",
                Some(tcp(
                    "https://127.0.0.1:8131",
                    Some(tls("/etc/srv/kubernetes/pki/konnectivity-server")),
                )),
            ),
            sel(
                "controlplane",
                "HTTPConnect",
                Some(tcp(
                    "https://127.0.0.1:8132",
                    Some(tls("/etc/srv/kubernetes/pki/konnectivity-server-master")),
                )),
            ),
            sel("etcd", "Direct", None),
        ])
    }

    #[test]
    fn read_empty_path_is_nil_config() {
        assert_eq!(read_egress_selector_configuration(""), Ok(None));
    }

    #[test]
    fn read_empty_file_errors() {
        let p = tmp("empty", "");
        let e = read_egress_selector_configuration(&p).unwrap_err();
        assert_eq!(e, "Object 'Kind' is missing in 'null'");
    }

    #[test]
    fn read_absent_file_errors() {
        let e = read_egress_selector_configuration("/nonexistent/egress.yaml").unwrap_err();
        assert_eq!(
            e,
            "unable to read egress selector configuration from \"/nonexistent/egress.yaml\" [open /nonexistent/egress.yaml: no such file or directory]"
        );
    }

    #[test]
    fn read_unknown_field_is_strict_decoding_error() {
        let p = tmp(
            "unknown",
            r#"
apiVersion: apiserver.k8s.io/v1beta1
kind: EgressSelectorConfiguration
egressSelections:
- name: "etcd"
  connection:
    proxyProtocol: "Direct"
  foo:
    bar: "baz"
"#,
        );
        let e = read_egress_selector_configuration(&p).unwrap_err();
        assert_eq!(
            e,
            "strict decoding error: unknown field \"egressSelections[0].foo\""
        );
    }

    #[test]
    fn read_duplicate_field_is_strict_decoding_error() {
        let p = tmp(
            "dup",
            r#"
apiVersion: apiserver.k8s.io/v1beta1
kind: EgressSelectorConfiguration
egressSelections:
- name: "etcd"
  connection:
    proxyProtocol: "Direct"
  connection:
    proxyProtocol: "Indirect"
"#,
        );
        let e = read_egress_selector_configuration(&p).unwrap_err();
        assert_eq!(
            e,
            "strict decoding error: error converting YAML to JSON: yaml: unmarshal errors:\n  line 8: key \"connection\" already set in map"
        );
    }

    #[test]
    fn read_v1beta1() {
        let p = tmp("v1beta1", V1BETA1);
        assert_eq!(
            read_egress_selector_configuration(&p).unwrap(),
            Some(expected_v1beta1())
        );
    }

    #[test]
    fn read_v1alpha1_same_shape() {
        let p = tmp(
            "v1alpha1",
            &V1BETA1.replace("apiserver.k8s.io/v1beta1", "apiserver.k8s.io/v1alpha1"),
        );
        assert_eq!(
            read_egress_selector_configuration(&p).unwrap(),
            Some(expected_v1beta1())
        );
    }

    #[test]
    fn read_master_is_converted_to_controlplane() {
        let p = tmp(
            "master",
            &V1BETA1.replace("name: \"controlplane\"", "name: \"master\""),
        );
        assert_eq!(
            read_egress_selector_configuration(&p).unwrap(),
            Some(expected_v1beta1())
        );
        let p = tmp(
            "master-a1",
            &V1BETA1
                .replace("name: \"controlplane\"", "name: \"master\"")
                .replace("apiserver.k8s.io/v1beta1", "apiserver.k8s.io/v1alpha1"),
        );
        assert_eq!(
            read_egress_selector_configuration(&p).unwrap(),
            Some(expected_v1beta1())
        );
    }

    #[test]
    fn read_wrong_kind_errors() {
        let p = tmp(
            "wrong",
            "apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: proxy-agent\n",
        );
        let e = read_egress_selector_configuration(&p).unwrap_err();
        assert_eq!(
            e,
            "no kind \"DaemonSet\" is registered for version \"apps/v1\" in scheme \"k8s.io/apiserver/pkg/server/egressselector/config.go:33\""
        );
    }

    #[test]
    fn read_v1_group_has_no_egress_selector_kind() {
        // The apiserver.config.k8s.io/v1 group does not register the kind
        // (v1/register.go), only the legacy apiserver.k8s.io group does.
        let p = tmp(
            "v1",
            &V1BETA1.replace("apiserver.k8s.io/v1beta1", "apiserver.config.k8s.io/v1"),
        );
        let e = read_egress_selector_configuration(&p).unwrap_err();
        assert!(e.starts_with("no kind \"EgressSelectorConfiguration\" is registered for version \"apiserver.config.k8s.io/v1\""), "{e}");
    }

    // ---- config_test.go TestValidateEgressSelectorConfiguration ---------

    fn errs_of(c: &EgressSelectorConfiguration) -> Vec<String> {
        validate_egress_selector_configuration(Some(c))
            .iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn validate_nil_is_valid() {
        assert!(validate_egress_selector_configuration(None).is_empty());
    }

    #[test]
    fn validate_direct_valid() {
        assert!(errs_of(&cfg(vec![sel("controlplane", "Direct", None)])).is_empty());
    }

    #[test]
    fn validate_direct_invalid_transport() {
        assert_eq!(
            errs_of(&cfg(vec![sel(
                "controlplane",
                "Direct",
                Some(Transport::default())
            )])),
            vec![
                "service.connection.transport: Invalid value: \"direct\": config must be absent for direct connect"
            ]
        );
    }

    #[test]
    fn validate_httpconnect_no_https() {
        assert!(errs_of(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp("http://127.0.0.1:8131", None))
        )]))
        .is_empty());
    }

    #[test]
    fn validate_http_with_tls_config_invalid() {
        let e = errs_of(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp("http://127.0.0.1:8131", Some(TlsConfig::default()))),
        )]));
        assert_eq!(
            e,
            vec!["service.connection.tlsConfig: Invalid value: \"nil\": config must not be present when using HTTP"]
        );
    }

    #[test]
    fn validate_httpconnect_https_no_cert_error() {
        assert_eq!(
            errs_of(&cfg(vec![sel(
                "cluster",
                "HTTPConnect",
                Some(tcp("https://127.0.0.1:8131", None))
            )])),
            vec!["service.connection.tlsConfig: Required value: TLSConfig must be present when using HTTPS"]
        );
    }

    #[test]
    fn validate_tls_files() {
        let e = errs_of(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp(
                "https://127.0.0.1:8131",
                Some(TlsConfig {
                    ca_bundle: "/nonexistent/ca".into(),
                    client_key: String::new(),
                    client_cert: "/nonexistent/cert".into(),
                }),
            )),
        )]));
        assert_eq!(
            e,
            vec![
                "service.connection.tlsConfig.caBundle: Invalid value: \"/nonexistent/ca\": TLS config ca bundle does not exist",
                "service.connection.tlsConfig.clientCert: Invalid value: \"/nonexistent/cert\": TLS client cert does not exist",
                "service.connection.tlsConfig.clientKey: Invalid value: \"nil\": Using TLS requires requires clientKey",
            ]
        );
        let e = errs_of(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp(
                "https://127.0.0.1:8131",
                Some(TlsConfig {
                    client_key: "/nonexistent/key".into(),
                    ..Default::default()
                }),
            )),
        )]));
        assert_eq!(
            e,
            vec![
                "service.connection.tlsConfig.clientCert: Invalid value: \"nil\": Using TLS requires clientCert",
                "service.connection.tlsConfig.clientKey: Invalid value: \"/nonexistent/key\": TLS client key does not exist",
            ]
        );
    }

    #[test]
    fn validate_tls_existing_files_ok() {
        let ca = tmp("ca", "x");
        let key = tmp("key", "xx");
        let crt = tmp("crt", "xxx");
        assert!(errs_of(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp(
                "https://127.0.0.1:8131",
                Some(TlsConfig {
                    ca_bundle: ca,
                    client_key: key,
                    client_cert: crt
                })
            ))
        )]))
        .is_empty());
    }

    #[test]
    fn validate_unsupported_url_scheme() {
        assert_eq!(
            errs_of(&cfg(vec![sel(
                "cluster",
                "HTTPConnect",
                Some(tcp("ftp://x", None))
            )])),
            vec!["service.connection.url: Invalid value: \"ftp://x\": supported connection protocols are http:// and https://"]
        );
    }

    #[test]
    fn validate_httpconnect_tcp_uds_both_set() {
        let t = Transport {
            tcp: Some(TcpTransport {
                url: "http://127.0.0.1:8131".into(),
                tls_config: None,
            }),
            uds: Some(UdsTransport {
                uds_name: "/s.socket".into(),
            }),
        };
        assert_eq!(
            errs_of(&cfg(vec![sel("cluster", "HTTPConnect", Some(t))])),
            vec![
                "service.connection.tcp: Invalid value: {\"URL\":\"http://127.0.0.1:8131\",\"TLSConfig\":null}: TCP and UDS cannot both be set"
            ]
        );
    }

    #[test]
    fn validate_httpconnect_neither_tcp_nor_uds() {
        assert_eq!(
            errs_of(&cfg(vec![sel(
                "cluster",
                "HTTPConnect",
                Some(Transport::default())
            )])),
            vec!["service.connection.tcp: Required value: One of TCP or UDS must be set"]
        );
    }

    #[test]
    fn validate_httpconnect_and_grpc_require_transport() {
        assert_eq!(
            errs_of(&cfg(vec![sel("cluster", "HTTPConnect", None)])),
            vec!["service.connection.transport: Required value: transport must be set for HTTPConnect"]
        );
        assert_eq!(
            errs_of(&cfg(vec![sel("cluster", "GRPC", None)])),
            vec!["service.connection.transport: Required value: transport must be set for GRPC"]
        );
    }

    #[test]
    fn validate_httpconnect_uds() {
        assert!(errs_of(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(uds("/s.socket"))
        )]))
        .is_empty());
        assert_eq!(
            errs_of(&cfg(vec![sel("cluster", "HTTPConnect", Some(uds("")))])),
            vec!["service.connection.udsName: Invalid value: \"nil\": must be present for UDS connections"]
        );
    }

    #[test]
    fn validate_grpc() {
        assert!(errs_of(&cfg(vec![sel("cluster", "GRPC", Some(uds("/s.socket")))])).is_empty());
        // grpc-https-invalid: TCP transport is not supported with GRPC.
        assert_eq!(
            errs_of(&cfg(vec![sel(
                "cluster",
                "GRPC",
                Some(tcp("http://127.0.0.1:8131", Some(TlsConfig::default())))
            )])),
            vec!["service.connection.uds: Required value: UDS must be set with GRPC"]
        );
    }

    #[test]
    fn validate_unknown_protocol() {
        assert_eq!(
            errs_of(&cfg(vec![sel("cluster", "Indirect", None)])),
            vec!["service.connection.protocol: Unsupported value: \"Indirect\": supported values: \"Direct\", \"HTTPConnect\", \"GRPC\""]
        );
    }

    #[test]
    fn validate_invalid_name() {
        assert_eq!(
            errs_of(&cfg(vec![sel("invalid", "Direct", None)])),
            vec!["service.connection: Unsupported value: \"invalid\": supported values: \"cluster\", \"controlplane\", \"etcd\""]
        );
    }

    #[test]
    fn validate_names_are_case_insensitive_and_duplicates_rejected() {
        assert!(errs_of(&cfg(vec![sel("ControlPlane", "Direct", None)])).is_empty());
        let e = validate_egress_selector_configuration(Some(&cfg(vec![
            sel("controlplane", "Direct", None),
            sel("ControlPlane", "Direct", None),
        ])));
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].error_type, ErrorType::Duplicate);
        assert_eq!(
            e[0].to_string(),
            "service.connection[1]: Duplicate value: \"controlplane\""
        );
    }

    // ---- egress_selector_test.go TestEgressSelector ---------------------

    #[test]
    fn selector_direct_for_all_types() {
        let s = EgressSelector::new(Some(&cfg(vec![
            sel("cluster", "Direct", None),
            sel("controlplane", "Direct", None),
            sel("etcd", "Direct", None),
        ])))
        .unwrap()
        .unwrap();
        for t in [
            EgressType::Cluster,
            EgressType::ControlPlane,
            EgressType::Etcd,
        ] {
            assert!(matches!(s.lookup(t), Some(Dialer::Direct)));
        }
    }

    #[test]
    fn selector_nil_config_is_no_selector() {
        assert!(EgressSelector::new(None).unwrap().is_none());
        assert!(
            EgressSelector::new(Some(&EgressSelectorConfiguration::default()))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn selector_empty_selections_gives_selector_with_no_dialers() {
        let s = EgressSelector::new(Some(&cfg(vec![]))).unwrap().unwrap();
        assert!(s.lookup(EgressType::Cluster).is_none());
    }

    #[test]
    fn selector_unrecognized_name() {
        let e = EgressSelector::new(Some(&cfg(vec![sel("bogus", "Direct", None)])))
            .err()
            .unwrap();
        assert_eq!(e, "unrecognized service name bogus");
    }

    #[test]
    fn selector_unconfigured_type_has_no_dialer() {
        let s = EgressSelector::new(Some(&cfg(vec![sel("etcd", "Direct", None)])))
            .unwrap()
            .unwrap();
        assert!(s.lookup(EgressType::Cluster).is_none());
        assert!(s.lookup(EgressType::Etcd).is_some());
    }

    #[test]
    fn selector_unknown_protocol_errors() {
        let e = EgressSelector::new(Some(&cfg(vec![sel("etcd", "Bogus", None)])))
            .err()
            .unwrap();
        assert_eq!(
            e,
            "failed to create dialer for egressSelection \"etcd\": unrecognized service connection protocol \"Bogus\""
        );
    }

    #[test]
    fn selector_with_map() {
        let mut m = HashMap::new();
        m.insert(EgressType::Etcd, Dialer::Direct);
        let s = EgressSelector::with_map(m);
        assert!(s.lookup(EgressType::Etcd).is_some());
        assert!(s.lookup(EgressType::Cluster).is_none());
    }

    #[test]
    fn egress_type_names() {
        assert_eq!(EgressType::ControlPlane.to_string(), "controlplane");
        assert_eq!(EgressType::Etcd.to_string(), "etcd");
        assert_eq!(EgressType::Cluster.to_string(), "cluster");
    }

    // ---- egress_selector_test.go TestMetrics ----------------------------

    struct FakeConnector {
        connector_err: bool,
        proxier_err: bool,
    }
    struct FakeProxier {
        err: bool,
    }
    #[async_trait::async_trait]
    impl ProxyServerConnector for FakeConnector {
        async fn connect(&self) -> Result<Box<dyn Proxier>, String> {
            if self.connector_err {
                return Err("fake error".into());
            }
            Ok(Box::new(FakeProxier {
                err: self.proxier_err,
            }))
        }
    }
    #[async_trait::async_trait]
    impl Proxier for FakeProxier {
        async fn proxy(&self, _addr: &str) -> Result<Conn, String> {
            if self.err {
                return Err("fake error".into());
            }
            let (a, _b) = tokio::io::duplex(8);
            Ok(Box::new(a))
        }
    }

    fn fake_dialer(connector_err: bool, proxier_err: bool, tag: &str) -> Dialer {
        DialerCreator {
            connector: Some(Arc::new(FakeConnector {
                connector_err,
                proxier_err,
            })),
            direct: false,
            options: MetricsOptions {
                transport: tag.into(),
                protocol: "fake_protocol".into(),
            },
        }
        .create_dialer()
    }

    fn counter(name: &str, labels: &[(&str, &str)]) -> f64 {
        for mf in prometheus::gather() {
            if mf.name() != name {
                continue;
            }
            for m in mf.get_metric() {
                let ok = labels.iter().all(|(k, v)| {
                    m.get_label()
                        .iter()
                        .any(|l| l.name() == *k && l.value() == *v)
                });
                if ok {
                    let n = m.get_histogram().get_sample_count();
                    return if n > 0 {
                        n as f64
                    } else {
                        m.get_counter().value()
                    };
                }
            }
        }
        0.0
    }

    #[tokio::test]
    async fn metrics_connect_failure() {
        let d = fake_dialer(true, true, "t_connect");
        assert!(d.dial("", "").await.is_err());
        let l = [("protocol", "fake_protocol"), ("transport", "t_connect")];
        assert_eq!(counter("apiserver_egress_dialer_dial_start_total", &l), 1.0);
        assert_eq!(
            counter(
                "apiserver_egress_dialer_dial_failure_count",
                &[l[0], l[1], ("stage", "connect")]
            ),
            1.0
        );
    }

    #[tokio::test]
    async fn metrics_proxy_failure() {
        let d = fake_dialer(false, true, "t_proxy");
        assert!(d.dial("", "").await.is_err());
        assert_eq!(
            counter(
                "apiserver_egress_dialer_dial_failure_count",
                &[
                    ("protocol", "fake_protocol"),
                    ("transport", "t_proxy"),
                    ("stage", "proxy")
                ]
            ),
            1.0
        );
    }

    #[tokio::test]
    async fn metrics_success_observes_latency() {
        let d = fake_dialer(false, false, "t_ok");
        assert!(d.dial("", "").await.is_ok());
        assert_eq!(
            counter(
                "apiserver_egress_dialer_dial_duration_seconds",
                &[("protocol", "fake_protocol"), ("transport", "t_ok")]
            ),
            1.0
        );
    }

    // ---- dialing ---------------------------------------------------------

    #[tokio::test]
    async fn direct_dialer_connects() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let accept = tokio::spawn(async move { l.accept().await.unwrap().0 });
        let mut c = Dialer::Direct.dial("tcp", &addr).await.unwrap();
        let mut s = accept.await.unwrap();
        c.write_all(b"hi").await.unwrap();
        let mut b = [0u8; 2];
        s.read_exact(&mut b).await.unwrap();
        assert_eq!(&b, b"hi");
    }

    /// A proxy that answers one CONNECT with `status_line` and then echoes.
    async fn fake_connect_proxy(sock: &str, status_line: &'static str, extra: &'static [u8]) {
        let l = tokio::net::UnixListener::bind(sock).unwrap();
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut b = [0u8; 1];
            while !buf.ends_with(b"\r\n\r\n") {
                s.read_exact(&mut b).await.unwrap();
                buf.push(b[0]);
            }
            let mut resp = format!("{status_line}\r\n\r\n").into_bytes();
            resp.extend_from_slice(extra);
            s.write_all(&resp).await.unwrap();
            let mut e = [0u8; 4];
            if s.read_exact(&mut e).await.is_ok() {
                let _ = s.write_all(&e).await;
            }
        });
    }

    fn uds_dialer(sock: &str) -> Dialer {
        EgressSelector::new(Some(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(uds(sock)),
        )])))
        .unwrap()
        .unwrap()
        .lookup(EgressType::Cluster)
        .unwrap()
    }

    fn sock_path(n: &str) -> String {
        let p = std::env::temp_dir().join(format!("egress-{}-{n}.sock", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p.to_string_lossy().into_owned()
    }

    #[tokio::test]
    async fn http_connect_over_uds_tunnels() {
        let sock = sock_path("ok");
        fake_connect_proxy(&sock, "HTTP/1.1 200 Connection established", b"").await;
        let mut c = uds_dialer(&sock)
            .dial("tcp", "10.0.0.1:10250")
            .await
            .unwrap();
        c.write_all(b"ping").await.unwrap();
        let mut e = [0u8; 4];
        c.read_exact(&mut e).await.unwrap();
        assert_eq!(&e, b"ping");
    }

    #[tokio::test]
    async fn http_connect_non_200_is_proxy_error() {
        let sock = sock_path("503");
        fake_connect_proxy(&sock, "HTTP/1.1 503 Service Unavailable", b"").await;
        let e = uds_dialer(&sock)
            .dial("tcp", "10.0.0.1:10250")
            .await
            .err()
            .unwrap();
        assert_eq!(
            e,
            format!(
                "proxy error from {sock} while dialing 10.0.0.1:10250, code 503: 503 Service Unavailable"
            )
        );
    }

    #[tokio::test]
    async fn http_connect_unexpected_buffered_data_is_error() {
        let sock = sock_path("buf");
        fake_connect_proxy(&sock, "HTTP/1.1 200 OK", b"junk").await;
        let e = uds_dialer(&sock)
            .dial("tcp", "10.0.0.1:10250")
            .await
            .err()
            .unwrap();
        assert_eq!(
            e,
            format!("unexpected 4 bytes of buffered data from CONNECT proxy \"{sock}\"")
        );
    }

    #[tokio::test]
    async fn http_connect_uds_missing_socket_is_connect_error() {
        let sock = sock_path("missing");
        assert!(uds_dialer(&sock).dial("tcp", "1.2.3.4:5").await.is_err());
    }

    #[test]
    #[should_panic(expected = "nil pointer dereference")]
    fn http_scheme_without_tls_config_panics_like_upstream() {
        // egress_selector.go:329 calls getTLSConfig(c.Transport.TCP.TLSConfig)
        // unconditionally while validation (config.go:162) allows http:// with
        // no tlsConfig: a nil dereference in Go, replicated here.
        let _ = EgressSelector::new(Some(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp("http://127.0.0.1:8131", None)),
        )])));
    }

    #[test]
    fn bad_key_pair_is_error() {
        let ca = tmp("kp-ca", "x");
        let e = EgressSelector::new(Some(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp(
                "https://127.0.0.1:8131",
                Some(TlsConfig {
                    ca_bundle: ca,
                    client_key: "/nonexistent/k".into(),
                    client_cert: "/nonexistent/c".into(),
                }),
            )),
        )])))
        .err()
        .unwrap();
        assert!(
            e.starts_with(
                "failed to create dialer for egressSelection \"cluster\": failed to read key pair /nonexistent/c & /nonexistent/k, got "
            ),
            "{e}"
        );
    }

    // ---- options/egress_selector.go -------------------------------------

    #[test]
    fn validate_options_checks_file_exists() {
        assert!(validate_options("").is_empty());
        assert_eq!(
            validate_options("/nonexistent/egress.yaml"),
            vec!["egress-selector-config-file /nonexistent/egress.yaml does not exist"]
        );
        assert!(validate_options(&tmp("opt", "x")).is_empty());
    }

    #[test]
    fn apply_to_no_file_is_noop() {
        assert!(apply_to("").unwrap().is_none());
    }

    #[test]
    fn apply_to_builds_selector_from_file() {
        let p = tmp(
            "apply",
            "apiVersion: apiserver.k8s.io/v1beta1\nkind: EgressSelectorConfiguration\negressSelections:\n- name: etcd\n  connection:\n    proxyProtocol: Direct\n",
        );
        let s = apply_to(&p).unwrap().unwrap();
        assert!(matches!(s.lookup(EgressType::Etcd), Some(Dialer::Direct)));
    }

    #[test]
    fn apply_to_reports_read_and_validation_errors() {
        let e = apply_to("/nonexistent/egress.yaml").err().unwrap();
        assert!(
            e.starts_with("failed to read egress selector config: unable to read"),
            "{e}"
        );
        let p = tmp(
            "apply-bad",
            "apiVersion: apiserver.k8s.io/v1beta1\nkind: EgressSelectorConfiguration\negressSelections:\n- name: bogus\n  connection:\n    proxyProtocol: Direct\n",
        );
        assert_eq!(
            apply_to(&p).err().unwrap(),
            "failed to validate egress selector configuration: service.connection: Unsupported value: \"bogus\": supported values: \"cluster\", \"controlplane\", \"etcd\""
        );
    }

    #[test]
    fn flag_is_parsed() {
        use clap::Parser;
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            e: EgressSelectorArgs,
        }
        let c = Cli::parse_from(["x", "--egress-selector-config-file", "/etc/egress.yaml"]);
        assert_eq!(c.e.config_file, "/etc/egress.yaml");
        assert_eq!(Cli::parse_from(["x"]).e.config_file, "");
    }

    // ---- HTTP CONNECT over TLS (tcp transport) --------------------------

    #[tokio::test]
    async fn http_connect_over_tls_tcp_tunnels() {
        use std::sync::Arc;
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        let cert = params.self_signed(&key).unwrap();
        let dir = std::env::temp_dir().join(format!("egress-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (crt, k) = (dir.join("client.crt"), dir.join("client.key"));
        std::fs::write(&crt, cert.pem()).unwrap();
        std::fs::write(&k, key.serialize_pem()).unwrap();

        let server_cfg = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.der().clone()],
                rustls::pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
            )
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (tcp, _) = l.accept().await.unwrap();
            let mut s = acceptor.accept(tcp).await.unwrap();
            let mut buf = Vec::new();
            let mut b = [0u8; 1];
            while !buf.ends_with(b"\r\n\r\n") {
                s.read_exact(&mut b).await.unwrap();
                buf.push(b[0]);
            }
            assert_eq!(
                String::from_utf8(buf).unwrap(),
                "CONNECT 10.0.0.1:10250 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
            );
            s.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .unwrap();
            let mut e = [0u8; 4];
            s.read_exact(&mut e).await.unwrap();
            s.write_all(&e).await.unwrap();
            s.flush().await.unwrap();
        });

        let d = EgressSelector::new(Some(&cfg(vec![sel(
            "cluster",
            "HTTPConnect",
            Some(tcp(
                &format!("https://127.0.0.1:{port}"),
                Some(TlsConfig {
                    ca_bundle: crt.to_string_lossy().into_owned(),
                    client_key: k.to_string_lossy().into_owned(),
                    client_cert: crt.to_string_lossy().into_owned(),
                }),
            )),
        )])))
        .unwrap()
        .unwrap()
        .lookup(EgressType::Cluster)
        .unwrap();
        let mut c = d.dial("tcp", "10.0.0.1:10250").await.unwrap();
        c.write_all(b"ping").await.unwrap();
        let mut e = [0u8; 4];
        c.read_exact(&mut e).await.unwrap();
        assert_eq!(&e, b"ping");
    }

    #[test]
    fn proxy_address_is_url_host() {
        assert_eq!(
            get_proxy_address("https://127.0.0.1:8131").unwrap(),
            "127.0.0.1:8131"
        );
        assert_eq!(
            get_proxy_address("https://u@proxy.example/x?y").unwrap(),
            "proxy.example"
        );
        assert!(get_proxy_address("://bad").is_err());
    }
}
