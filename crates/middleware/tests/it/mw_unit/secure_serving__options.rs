//! Unit tests moved from `src/secure_serving/options.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_common::authz::{Authorizer, PrivilegedGroupAuthorizer};
    pub use rusternetes_middleware::secure_serving::authn::{
        AnonymousAuthConfig, CaContent, DelegatingAuthenticatorConfig, RequestAuthenticator,
        RequestHeaderConfig, RequestHeaderSettings,
    };
    pub use rusternetes_middleware::secure_serving::authz::{
        new_path_authorizer, DelegatingAuthorizerConfig,
    };
    pub use rusternetes_middleware::secure_serving::serve::{
        tls_cipher_suites, tls_version, SecureServingInfo, ServingCert,
    };
    pub use rusternetes_middleware::secure_serving::webhook::{
        ApiClientReviewClient, Backoff, ReviewClient, ReviewError,
    };
    pub use std::net::IpAddr;
    pub use std::sync::{Arc, RwLock};
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use clap::Parser;
    use rusternetes_middleware::secure_serving::options::*;

    #[derive(Parser, Debug)]
    struct Cli {
        #[command(flatten)]
        serving: SecureServingFlags,
        #[command(flatten)]
        authn: AuthenticationFlags,
        #[command(flatten)]
        authz: AuthorizationFlags,
    }

    fn parse(args: &[&str]) -> Cli {
        let mut v = vec!["bin"];
        v.extend_from_slice(args);
        Cli::try_parse_from(v).unwrap()
    }

    // ---- serving.go NewSecureServingOptions / Validate

    #[test]
    fn serving_defaults() {
        let o = SecureServingOptions::new();
        assert_eq!(o.bind_address, "0.0.0.0".parse::<IpAddr>().unwrap());
        assert_eq!(o.bind_port, 443);
        assert_eq!(o.server_cert.pair_name, "apiserver");
        assert_eq!(
            o.server_cert.cert_directory,
            "apiserver.local.config/certificates"
        );
    }

    #[test]
    fn serving_validate_port_range() {
        let mut o = SecureServingOptions::new();
        // serving.go:137 parses as `(Required && BindPort < 1) || BindPort > 65535`,
        // so an oversized port gets the "cannot be turned off" wording even when
        // the port is optional. Replicated as is.
        o.bind_port = 70000;
        assert_eq!(
            o.validate(),
            vec!["--secure-port 70000 must be between 1 and 65535, inclusive. It cannot be turned off with 0"]
        );
        o.bind_port = -1;
        assert_eq!(
            o.validate(),
            vec!["--secure-port -1 must be between 0 and 65535, inclusive. 0 for turning off secure port"]
        );
        o.bind_port = 0;
        assert!(o.validate().is_empty());
        // serving.go:137: Required && BindPort < 1 || BindPort > 65535
        o.required = true;
        assert_eq!(
            o.validate(),
            vec!["--secure-port 0 must be between 1 and 65535, inclusive. It cannot be turned off with 0"]
        );
        o.bind_port = 10257;
        assert!(o.validate().is_empty());
    }

    #[test]
    fn serving_validate_cert_and_generated_cert_conflict() {
        let mut o = SecureServingOptions::new();
        o.bind_port = 10257;
        o.server_cert.cert_key.cert_file = "a".into();
        o.server_cert.generated_cert = Some((vec![], vec![]));
        assert_eq!(
            o.validate(),
            vec!["cert/key file and in-memory certificate cannot both be set"]
        );
    }

    // ---- flags

    #[test]
    fn serving_flags_override_only_what_is_given() {
        let cli = parse(&[
            "--bind-address=127.0.0.1",
            "--secure-port=1234",
            "--tls-cipher-suites=TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "--tls-min-version=VersionTLS13",
            "--cert-dir=/tmp/x",
            "--disable-http2-serving",
        ]);
        let mut o = SecureServingOptions::new();
        o.bind_port = 10257;
        cli.serving.apply_to_options(&mut o);
        assert_eq!(o.bind_address, "127.0.0.1".parse::<IpAddr>().unwrap());
        assert_eq!(o.bind_port, 1234);
        assert_eq!(o.cipher_suites.len(), 2);
        assert_eq!(o.min_tls_version, "VersionTLS13");
        assert_eq!(o.server_cert.cert_directory, "/tmp/x");
        assert!(o.disable_http2_serving);

        let mut o = SecureServingOptions::new();
        o.bind_port = 10257;
        parse(&[]).serving.apply_to_options(&mut o);
        assert_eq!(o.bind_port, 10257);
        assert!(!o.disable_http2_serving);
    }

    // pflag bool: `--x=false` is accepted.
    #[test]
    fn bool_flags_accept_explicit_false() {
        let cli = parse(&[
            "--authentication-tolerate-lookup-failure=false",
            "--disable-http2-serving=false",
        ]);
        let mut a = DelegatingAuthenticationOptions::new();
        a.tolerate_in_cluster_lookup_failure = true;
        cli.authn.apply_to_options(&mut a);
        assert!(!a.tolerate_in_cluster_lookup_failure);
        assert_eq!(cli.serving.disable_http2_serving, Some(false));
    }

    // ---- ApplyTo of serving

    #[test]
    fn apply_to_with_port_zero_and_no_listener_is_none() {
        let mut o = SecureServingOptions::new();
        o.bind_port = 0;
        assert!(o.apply_to().unwrap().is_none());
    }

    #[test]
    fn apply_to_binds_and_records_the_port() {
        let mut o = SecureServingOptions::new();
        o.bind_address = "127.0.0.1".parse().unwrap();
        o.bind_port = 0;
        o.listener = Some(std::net::TcpListener::bind("127.0.0.1:0").unwrap());
        let info = o.apply_to().unwrap().unwrap();
        let port = info.local_addr().unwrap().port();
        assert_eq!(o.bind_port, port as i32);
        assert_eq!(o.bind_address, "127.0.0.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn apply_to_rejects_unknown_cipher_and_version() {
        let mut o = SecureServingOptions::new();
        o.bind_address = "127.0.0.1".parse().unwrap();
        o.bind_port = 0;
        o.listener = Some(std::net::TcpListener::bind("127.0.0.1:0").unwrap());
        o.cipher_suites = vec!["TLS_NOPE".into()];
        assert_eq!(
            o.apply_to().err().unwrap(),
            "Cipher suite TLS_NOPE not supported or doesn't exist"
        );
        let mut o = SecureServingOptions::new();
        o.listener = Some(std::net::TcpListener::bind("127.0.0.1:0").unwrap());
        o.min_tls_version = "VersionTLS99".into();
        assert_eq!(
            o.apply_to().err().unwrap(),
            "unknown tls version \"VersionTLS99\""
        );
    }

    // ---- MaybeDefaultWithSelfSignedCerts (serving.go:340-391)

    fn serving_on_loopback() -> SecureServingOptions {
        let mut o = SecureServingOptions::new();
        o.bind_port = 10257;
        o.server_cert.cert_directory = String::new();
        o.server_cert.pair_name = "kube-controller-manager".into();
        o
    }

    #[test]
    fn self_signed_in_memory_when_no_cert_directory() {
        let mut o = serving_on_loopback();
        o.maybe_default_with_self_signed_certs("localhost", &[], &["127.0.0.1".parse().unwrap()])
            .unwrap();
        let (cert, key) = o.server_cert.generated_cert.clone().unwrap();
        assert!(String::from_utf8(cert)
            .unwrap()
            .contains("BEGIN CERTIFICATE"));
        assert!(String::from_utf8(key).unwrap().contains("RSA PRIVATE KEY"));
        // nothing named on disk
        assert!(o.server_cert.cert_key.cert_file.is_empty());
    }

    #[test]
    fn self_signed_is_written_to_the_cert_directory_and_reused() {
        let dir = tempfile::tempdir().unwrap();
        let mut o = serving_on_loopback();
        o.server_cert.cert_directory = dir.path().to_str().unwrap().into();
        o.maybe_default_with_self_signed_certs("localhost", &[], &[])
            .unwrap();
        let cert = dir.path().join("kube-controller-manager.crt");
        let key = dir.path().join("kube-controller-manager.key");
        assert!(cert.exists() && key.exists());
        assert_eq!(o.server_cert.cert_key.cert_file, cert.to_str().unwrap());
        assert!(o.server_cert.generated_cert.is_none());
        let first = std::fs::read(&cert).unwrap();
        // second run: `canReadCertAndKey` -> reuse, no regeneration
        let mut o2 = serving_on_loopback();
        o2.server_cert.cert_directory = dir.path().to_str().unwrap().into();
        o2.maybe_default_with_self_signed_certs("localhost", &[], &[])
            .unwrap();
        assert_eq!(std::fs::read(&cert).unwrap(), first);
    }

    #[test]
    fn self_signed_is_skipped_when_cert_files_are_given_or_serving_is_off() {
        let mut o = serving_on_loopback();
        o.server_cert.cert_key.cert_file = "a.crt".into();
        o.maybe_default_with_self_signed_certs("localhost", &[], &[])
            .unwrap();
        assert!(o.server_cert.generated_cert.is_none());
        let mut o = serving_on_loopback();
        o.bind_port = 0;
        o.maybe_default_with_self_signed_certs("localhost", &[], &[])
            .unwrap();
        assert!(o.server_cert.generated_cert.is_none());
    }

    #[test]
    fn self_signed_requires_a_pair_name_with_a_cert_directory() {
        let mut o = serving_on_loopback();
        o.server_cert.cert_directory = "/tmp/somewhere".into();
        o.server_cert.pair_name = String::new();
        assert_eq!(
            o.maybe_default_with_self_signed_certs("localhost", &[], &[])
                .unwrap_err(),
            "PairName is required if CertDirectory is set"
        );
    }

    // serving.go:365-369: a specific bind address becomes a SAN, `localhost` only for unspecified.
    #[test]
    fn self_signed_sans_follow_the_bind_address() {
        use x509_parser::prelude::*;
        let san = |bind: &str| {
            let mut o = serving_on_loopback();
            o.bind_address = bind.parse().unwrap();
            o.maybe_default_with_self_signed_certs("host", &[], &[])
                .unwrap();
            let (cert, _) = o.server_cert.generated_cert.clone().unwrap();
            let der = rustls_pemfile::certs(&mut &cert[..])
                .next()
                .unwrap()
                .unwrap()
                .to_vec();
            let (_, c) = parse_x509_certificate(&der).unwrap();
            let mut dns = vec![];
            let mut ips = vec![];
            for e in c.extensions() {
                if let ParsedExtension::SubjectAlternativeName(s) = e.parsed_extension() {
                    for n in &s.general_names {
                        match n {
                            GeneralName::DNSName(d) => dns.push(d.to_string()),
                            GeneralName::IPAddress(b) => ips.push(b.to_vec()),
                            _ => {}
                        }
                    }
                }
            }
            (dns, ips)
        };
        let (dns, ips) = san("0.0.0.0");
        assert!(dns.contains(&"localhost".to_string()));
        assert!(ips.is_empty());
        let (dns, ips) = san("10.1.2.3");
        assert!(!dns.contains(&"localhost".to_string()));
        assert_eq!(ips, vec![vec![10, 1, 2, 3]]);
    }

    // ---- authentication.go defaults and validation

    #[test]
    fn authentication_defaults() {
        let o = DelegatingAuthenticationOptions::new();
        assert_eq!(o.cache_ttl, Duration::from_secs(10));
        assert_eq!(o.request_header.username_headers, vec!["x-remote-user"]);
        assert!(o.request_header.uid_headers.is_empty());
        assert_eq!(o.request_header.group_headers, vec!["x-remote-group"]);
        assert_eq!(
            o.request_header.extra_header_prefixes,
            vec!["x-remote-extra-"]
        );
        assert_eq!(
            o.webhook_retry_backoff,
            Some(Backoff::default_auth_webhook())
        );
        assert_eq!(o.token_request_timeout, Duration::from_secs(10));
        assert!(o.anonymous.as_ref().unwrap().enabled);
    }

    #[test]
    fn authentication_validate_retry_steps() {
        let mut o = DelegatingAuthenticationOptions::new();
        assert!(o.validate().is_empty());
        o.webhook_retry_backoff.as_mut().unwrap().steps = 0;
        assert_eq!(
            o.validate(),
            vec!["number of webhook retry attempts must be greater than 1, but is: 0"]
        );
    }

    #[test]
    fn request_header_validate() {
        let mut o = RequestHeaderAuthenticationOptions {
            username_headers: vec!["  ".into()],
            ..Default::default()
        };
        assert_eq!(
            o.validate(false),
            vec!["empty value in \"requestheader-username-headers\""]
        );
        o.username_headers = vec![];
        o.uid_headers = vec!["X-Remote-Uid".into()];
        assert_eq!(
            o.validate(false),
            vec!["--requestheader-uid-headers requires the \"RemoteRequestHeaderUID\" feature to be enabled"]
        );
        // gate on, but the standard header missing: error
        o.uid_headers = vec!["x-uid".into()];
        assert_eq!(
            o.validate(true),
            vec!["--requestheader-uid-headers is set without specifying the standard X-Remote-Uid header - API aggregation will not work"]
        );
        o.uid_headers = vec!["x-remote-uid".into()];
        assert!(o.validate(true).is_empty());
    }

    // authentication_test.go TestToAuthenticationRequestHeaderConfig
    #[test]
    fn to_authentication_request_header_config() {
        let o = RequestHeaderAuthenticationOptions::default();
        assert!(o
            .to_authentication_request_header_config()
            .unwrap()
            .is_none());

        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path().join("ca.crt");
        let key = rcgen::KeyPair::generate().unwrap();
        let mut p = rcgen::CertificateParams::default();
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        std::fs::write(&ca, p.self_signed(&key).unwrap().pem()).unwrap();
        let o = RequestHeaderAuthenticationOptions {
            client_ca_file: ca.to_str().unwrap().into(),
            username_headers: vec!["x-remote-user".into()],
            uid_headers: vec!["x-remote-uid".into()],
            group_headers: vec!["x-remote-group".into()],
            extra_header_prefixes: vec!["x-remote-extra-".into()],
            allowed_names: vec!["kube-aggregator".into()],
        };
        let cfg = o
            .to_authentication_request_header_config()
            .unwrap()
            .unwrap();
        let s = cfg.settings.read().unwrap().clone();
        assert_eq!(s.username_headers, vec!["x-remote-user"]);
        assert_eq!(s.uid_headers, vec!["x-remote-uid"]);
        assert_eq!(s.group_headers, vec!["x-remote-group"]);
        assert_eq!(s.extra_header_prefixes, vec!["x-remote-extra-"]);
        assert_eq!(s.allowed_client_names, vec!["kube-aggregator"]);
        assert!(cfg.ca.current_pem().is_some());

        // an unreadable CA file is an error
        let o = RequestHeaderAuthenticationOptions {
            client_ca_file: "/nonexistent".into(),
            ..Default::default()
        };
        assert!(o.to_authentication_request_header_config().is_err());
    }

    #[test]
    fn authentication_flags_apply() {
        let cli = parse(&[
            "--authentication-kubeconfig=/k",
            "--authentication-token-webhook-cache-ttl=30s",
            "--client-ca-file=/ca",
            "--requestheader-username-headers=a,b",
            "--requestheader-client-ca-file=/rca",
            "--requestheader-allowed-names=x",
            "--authentication-skip-lookup",
        ]);
        let mut o = DelegatingAuthenticationOptions::new();
        cli.authn.apply_to_options(&mut o);
        assert_eq!(o.remote_kubeconfig_file, "/k");
        assert_eq!(o.cache_ttl, Duration::from_secs(30));
        assert_eq!(o.client_ca, "/ca");
        assert_eq!(o.request_header.username_headers, vec!["a", "b"]);
        // untouched defaults survive
        assert_eq!(o.request_header.group_headers, vec!["x-remote-group"]);
        assert_eq!(o.request_header.client_ca_file, "/rca");
        assert_eq!(o.request_header.allowed_names, vec!["x"]);
        assert!(o.skip_in_cluster_lookup);
    }

    #[test]
    fn go_duration_flag_values() {
        assert_eq!(parse_go_duration_arg("1m30s"), Ok(Duration::from_secs(90)));
        assert_eq!(
            parse_go_duration_arg("250ms"),
            Ok(Duration::from_millis(250))
        );
        assert!(parse_go_duration_arg("ten").is_err());
    }

    // ---- authorization.go

    #[test]
    fn authorization_defaults() {
        let o = DelegatingAuthorizationOptions::new();
        assert_eq!(o.allow_cache_ttl, Duration::from_secs(10));
        assert_eq!(o.deny_cache_ttl, Duration::from_secs(10));
        assert_eq!(o.client_timeout, Duration::from_secs(10));
        assert_eq!(o.always_allow_paths, vec!["/healthz", "/readyz", "/livez"]);
        assert_eq!(o.always_allow_groups, vec!["system:masters"]);
        assert_eq!(
            o.webhook_retry_backoff,
            Some(Backoff::default_auth_webhook())
        );
    }

    #[test]
    fn authorization_flags_apply() {
        let cli = parse(&[
            "--authorization-kubeconfig=/k",
            "--authorization-webhook-cache-authorized-ttl=1m",
            "--authorization-webhook-cache-unauthorized-ttl=5s",
            "--authorization-always-allow-paths=/a,/b*",
        ]);
        let mut o = DelegatingAuthorizationOptions::new();
        cli.authz.apply_to_options(&mut o);
        assert_eq!(o.remote_kubeconfig_file, "/k");
        assert_eq!(o.allow_cache_ttl, Duration::from_secs(60));
        assert_eq!(o.deny_cache_ttl, Duration::from_secs(5));
        assert_eq!(o.always_allow_paths, vec!["/a", "/b*"]);
        // an empty value clears the list (pflag StringSlice)
        let cli = parse(&["--authorization-always-allow-paths="]);
        let mut o = DelegatingAuthorizationOptions::new();
        cli.authz.apply_to_options(&mut o);
        assert!(o.always_allow_paths.is_empty());
    }

    #[test]
    fn authorization_validate_retry_steps() {
        let mut o = DelegatingAuthorizationOptions::new();
        assert!(o.validate().is_empty());
        o.webhook_retry_backoff.as_mut().unwrap().steps = -1;
        assert_eq!(o.validate().len(), 1);
    }

    // authorization.go:177-209 toAuthorizer with no client: only the static
    // authorizers; everything else is not allowed.
    #[tokio::test]
    async fn to_authorizer_without_a_client_has_only_masters_and_paths() {
        use rusternetes_common::auth::UserInfo;
        use rusternetes_common::authz::{Decision, RequestAttributes};
        let o = DelegatingAuthorizationOptions::new();
        let a = o.to_authorizer(None).unwrap();
        let u = |name: &str, groups: &[&str]| UserInfo {
            username: name.into(),
            uid: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            extra: Default::default(),
        };
        let get =
            |user: UserInfo, path: &str| RequestAttributes::new_non_resource(user, "get", path);
        assert_eq!(
            a.authorize(&get(u("a", &["system:masters"]), "/metrics"))
                .await
                .unwrap(),
            Decision::Allow
        );
        assert_eq!(
            a.authorize(&get(u("a", &[]), "/healthz")).await.unwrap(),
            Decision::Allow
        );
        assert!(matches!(
            a.authorize(&get(u("a", &[]), "/metrics")).await.unwrap(),
            Decision::Deny(_)
        ));
    }

    // ---- getClient

    fn write_kubeconfig(dir: &std::path::Path) -> String {
        let p = dir.join("kubeconfig");
        std::fs::write(
            &p,
            "apiVersion: v1\nkind: Config\nclusters:\n- cluster:\n    server: http://localhost:56789\n  name: cluster\ncontexts:\n- context:\n    cluster: cluster\n    user: u\n  name: cluster\ncurrent-context: cluster\nusers:\n- name: u\n  user: {}\n",
        )
        .unwrap();
        p.to_str().unwrap().to_string()
    }

    #[test]
    fn get_client_from_a_kubeconfig() {
        let dir = tempfile::tempdir().unwrap();
        let k = write_kubeconfig(dir.path());
        assert!(get_client(&k, false, "authentication").unwrap().is_some());
        assert!(get_client("/nonexistent", true, "authentication").is_err());
    }

    // authentication.go:473-479: no kubeconfig and not in a cluster: nil when
    // optional, an error otherwise.
    #[test]
    fn get_client_without_a_kubeconfig_outside_a_cluster() {
        if std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
            || std::env::var("KUBERNETES_SERVICE_HOST_OVERRIDE").is_ok()
        {
            return; // running inside a cluster
        }
        assert!(get_client("", true, "authentication").unwrap().is_none());
        let err = get_client("", false, "authentication").err().unwrap();
        assert!(
            err.starts_with("failed to get delegated authentication kubeconfig: "),
            "{err}"
        );
    }

    // ---- authentication_test.go TestApplyToFallback

    async fn apply_authn(
        o: &DelegatingAuthenticationOptions,
    ) -> Result<AuthenticationInfo, String> {
        o.apply_to(None).await
    }

    // "optional kubeconfig": in-cluster client building fails, no kubeconfig
    // required => an authenticator that treats every token as anonymous.
    #[tokio::test]
    async fn apply_to_fallback_optional_kubeconfig() {
        if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
            return;
        }
        let mut o = DelegatingAuthenticationOptions::new();
        o.remote_kubeconfig_file_optional = true;
        let info = apply_authn(&o).await.unwrap();
        let a = info.authenticator.unwrap();
        let mut h = axum::http::HeaderMap::new();
        h.insert("authorization", "Bearer token".parse().unwrap());
        let r = a
            .authenticate_request(
                &mut rusternetes_middleware::secure_serving::authn::AuthRequest {
                    headers: &mut h,
                    path: "/",
                    peer_certs: &[],
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.user.username, "system:anonymous");
    }

    // "default": in-cluster client building fails and no kubeconfig => error.
    #[tokio::test]
    async fn apply_to_fallback_default_errors() {
        if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
            return;
        }
        let o = DelegatingAuthenticationOptions::new();
        assert!(apply_authn(&o).await.is_err());
    }

    // "valid client, failed cluster info lookup": the lookup of the
    // request-header configuration fails => error ...
    #[tokio::test]
    async fn apply_to_fallback_failed_lookup_is_fatal_unless_tolerated() {
        let dir = tempfile::tempdir().unwrap();
        let mut o = DelegatingAuthenticationOptions::new();
        o.remote_kubeconfig_file = write_kubeconfig(dir.path());
        assert!(apply_authn(&o).await.is_err());

        // ... "valid client, tolerate cluster info lookup failure"
        o.tolerate_in_cluster_lookup_failure = true;
        let info = apply_authn(&o).await.unwrap();
        assert!(info.authenticator.is_some());
    }

    // "valid client, skip cluster info lookup": a token validator exists
    // (a token from an unreachable server errors), anonymous is the fallback.
    #[tokio::test]
    async fn apply_to_fallback_skip_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let mut o = DelegatingAuthenticationOptions::new();
        o.remote_kubeconfig_file = write_kubeconfig(dir.path());
        o.skip_in_cluster_lookup = true;
        // keep the unreachable-server retries short
        o.webhook_retry_backoff = Some(Backoff {
            duration: Duration::from_millis(1),
            factor: 1.0,
            jitter: 0.0,
            steps: 1,
        });
        let info = apply_authn(&o).await.unwrap();
        let a = info.authenticator.unwrap();
        let mut h = axum::http::HeaderMap::new();
        h.insert("authorization", "Bearer token".parse().unwrap());
        let r = a
            .authenticate_request(
                &mut rusternetes_middleware::secure_serving::authn::AuthRequest {
                    headers: &mut h,
                    path: "/",
                    peer_certs: &[],
                },
            )
            .await;
        assert!(
            r.is_err(),
            "token errors against an unreachable server: {r:?}"
        );
        // no credentials: anonymous
        let r = a
            .authenticate_request(
                &mut rusternetes_middleware::secure_serving::authn::AuthRequest {
                    headers: &mut axum::http::HeaderMap::new(),
                    path: "/",
                    peer_certs: &[],
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.user.username, "system:anonymous");
    }

    // an unreadable --client-ca-file is fatal (`unable to load client CA provider`)
    #[tokio::test]
    async fn apply_to_with_a_missing_client_ca_file_fails() {
        let mut o = DelegatingAuthenticationOptions::new();
        o.remote_kubeconfig_file_optional = true;
        o.client_ca = "/nonexistent/ca.crt".into();
        let err = apply_authn(&o).await.err().unwrap();
        assert!(
            err.starts_with("unable to load client CA provider: "),
            "{err}"
        );
    }
}
