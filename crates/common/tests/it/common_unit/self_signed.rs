//! Unit tests moved from `src/self_signed.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::net::IpAddr;
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::self_signed::*;
    use x509_parser::prelude::*;

    fn certs(pem_bytes: &[u8]) -> Vec<Vec<u8>> {
        ::pem::parse_many(pem_bytes)
            .unwrap()
            .into_iter()
            .filter(|p| p.tag() == "CERTIFICATE")
            .map(|p| p.into_contents())
            .collect()
    }

    fn san_names(der: &[u8]) -> (Vec<String>, Vec<IpAddr>) {
        let (_, c) = parse_x509_certificate(der).unwrap();
        let mut dns = vec![];
        let mut ips = vec![];
        for ext in c.extensions() {
            if let ParsedExtension::SubjectAlternativeName(san) = ext.parsed_extension() {
                for n in &san.general_names {
                    match n {
                        GeneralName::DNSName(d) => dns.push(d.to_string()),
                        GeneralName::IPAddress(b) => ips.push(match b.len() {
                            4 => IpAddr::from([b[0], b[1], b[2], b[3]]),
                            _ => IpAddr::from(<[u8; 16]>::try_from(*b).unwrap()),
                        }),
                        _ => {}
                    }
                }
            }
        }
        (dns, ips)
    }

    // cert.go:158 `baseName` + 219 / 186: subjects are `host@<unix>` and
    // `host-ca@<unix>`; the chain is "cert, followed by ca" (cert.go:240-247).
    #[test]
    fn chain_is_leaf_then_ca_with_upstream_subjects() {
        let (cert, _key) =
            generate_self_signed_cert_key_with_fixtures("localhost", &[], &[], "").unwrap();
        let chain = certs(&cert);
        assert_eq!(chain.len(), 2);
        let (_, leaf) = parse_x509_certificate(&chain[0]).unwrap();
        let (_, ca) = parse_x509_certificate(&chain[1]).unwrap();
        let leaf_cn = leaf
            .subject()
            .iter_common_name()
            .next()
            .unwrap()
            .as_str()
            .unwrap();
        let ca_cn = ca
            .subject()
            .iter_common_name()
            .next()
            .unwrap()
            .as_str()
            .unwrap();
        assert!(leaf_cn.starts_with("localhost@"), "{leaf_cn}");
        assert!(ca_cn.starts_with("localhost-ca@"), "{ca_cn}");
        assert_eq!(leaf.issuer().to_string(), ca.subject().to_string());
        assert!(ca.is_ca());
        assert!(!leaf.is_ca());
    }

    // cert.go:229-236: host goes to DNS or IP SANs, alternates are appended.
    #[test]
    fn sans_carry_host_and_alternates() {
        let (cert, _) = generate_self_signed_cert_key_with_fixtures(
            "localhost",
            &["127.0.0.1".parse().unwrap()],
            &["alt.example".to_string()],
            "",
        )
        .unwrap();
        let (dns, ips) = san_names(&certs(&cert)[0]);
        assert_eq!(
            dns,
            vec!["localhost".to_string(), "alt.example".to_string()]
        );
        assert_eq!(ips, vec!["127.0.0.1".parse::<IpAddr>().unwrap()]);

        let (cert, _) =
            generate_self_signed_cert_key_with_fixtures("10.0.0.1", &[], &[], "").unwrap();
        let (dns, ips) = san_names(&certs(&cert)[0]);
        assert!(dns.is_empty());
        assert_eq!(ips, vec!["10.0.0.1".parse::<IpAddr>().unwrap()]);
    }

    // cert.go:156 (valid an hour earlier) and 152-154 (365 days default).
    #[test]
    fn validity_is_an_hour_back_for_a_year() {
        let (cert, _) =
            generate_self_signed_cert_key_with_fixtures("localhost", &[], &[], "").unwrap();
        let chain = certs(&cert);
        let (_, leaf) = parse_x509_certificate(&chain[0]).unwrap();
        let now = chrono::Utc::now().timestamp();
        let nb = leaf.validity().not_before.timestamp();
        let na = leaf.validity().not_after.timestamp();
        assert!((now - 3600 - nb).abs() < 120, "not_before {nb} vs {now}");
        assert_eq!(na - nb, 365 * 24 * 3600);
    }

    // cert.go:251: the key is PKCS#1, `RSA PRIVATE KEY`; and it matches the
    // serving certificate.
    #[test]
    fn key_is_pkcs1_rsa_and_matches_the_certificate() {
        let (cert, key) =
            generate_self_signed_cert_key_with_fixtures("localhost", &[], &[], "").unwrap();
        let blocks = ::pem::parse_many(&key).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].tag(), "RSA PRIVATE KEY");
        assert_eq!(rusternetes_common::tls::x509_key_pair(&cert, &key), Ok(()));
    }

    // cert.go:161-171, 254-263: a fixture directory caches the pair and the
    // second call returns the same bytes; fixtures live 100 years.
    #[test]
    fn fixture_directory_is_written_then_reused() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().to_str().unwrap();
        let a = generate_self_signed_cert_key_with_fixtures("localhost", &[], &[], d).unwrap();
        let b = generate_self_signed_cert_key_with_fixtures("localhost", &[], &[], d).unwrap();
        assert_eq!(a, b);
        assert!(dir.path().join("localhost__.crt").exists());
        assert!(dir.path().join("localhost__.key").exists());
        let chain = certs(&a.0);
        let (_, leaf) = parse_x509_certificate(&chain[0]).unwrap();
        let span = leaf.validity().not_after.timestamp() - leaf.validity().not_before.timestamp();
        assert_eq!(span, 100 * 24 * 3600 * 365);
    }

    // cert.go:162-169: readable cert with unreadable key is an error.
    #[test]
    fn fixture_with_cert_but_no_key_errors() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("localhost__.crt"), b"x").unwrap();
        let err = generate_self_signed_cert_key_with_fixtures(
            "localhost",
            &[],
            &[],
            dir.path().to_str().unwrap(),
        )
        .unwrap_err();
        assert!(err.starts_with("cert "), "{err}");
        assert!(err.contains("can be read, but key "), "{err}");
    }

    // cert.go:158: `host_<ip>-<ip>_<dns>-<dns>`.
    #[test]
    fn fixture_names_join_alternates() {
        let dir = tempfile::tempdir().unwrap();
        generate_self_signed_cert_key_with_fixtures(
            "h",
            &["1.2.3.4".parse().unwrap(), "5.6.7.8".parse().unwrap()],
            &["a".to_string(), "b".to_string()],
            dir.path().to_str().unwrap(),
        )
        .unwrap();
        assert!(dir.path().join("h_1.2.3.4-5.6.7.8_a-b.crt").exists());
    }

    // io.go:28-45.
    #[test]
    fn can_read_cert_and_key_matrix() {
        let dir = tempfile::tempdir().unwrap();
        let c = dir.path().join("a.crt");
        let k = dir.path().join("a.key");
        let (cs, ks) = (c.to_str().unwrap(), k.to_str().unwrap());
        assert_eq!(can_read_cert_and_key(cs, ks), Ok(false));
        std::fs::write(&c, b"c").unwrap();
        assert_eq!(
            can_read_cert_and_key(cs, ks),
            Err(format!(
                "error reading {ks}, certificate and key must be supplied as a pair"
            ))
        );
        std::fs::remove_file(&c).unwrap();
        std::fs::write(&k, b"k").unwrap();
        assert_eq!(
            can_read_cert_and_key(cs, ks),
            Err(format!(
                "error reading {cs}, certificate and key must be supplied as a pair"
            ))
        );
        std::fs::write(&c, b"c").unwrap();
        assert_eq!(can_read_cert_and_key(cs, ks), Ok(true));
    }
}
