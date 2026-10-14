//! Port of the self-signed serving certificate helpers used by
//! `SecureServingOptions.MaybeDefaultWithSelfSignedCerts`
//! (`staging/src/k8s.io/apiserver/pkg/server/options/serving.go:340-391`).
//!
//! Upstream sources (release-1.35):
//! * `staging/src/k8s.io/client-go/util/cert/cert.go:136-259`
//!   (`GenerateSelfSignedCertKeyWithFixtures` / `GenerateSelfSignedCertKeyWithOptions`)
//! * `staging/src/k8s.io/client-go/util/cert/io.go:28-58` (`CanReadCertAndKey`)

use std::net::IpAddr;
use std::time::Duration;

/// `SelfSignedCertKeyOptions` (cert.go).
#[derive(Debug, Clone, Default)]
pub struct SelfSignedCertKeyOptions {
    pub host: String,
    pub alternate_ips: Vec<IpAddr>,
    pub alternate_dns: Vec<String>,
    /// Empty means no fixtures.
    pub fixture_directory: String,
    /// Zero means 365 days (cert.go:152-154).
    pub max_age: Duration,
}

/// `GenerateSelfSignedCertKeyWithFixtures`: returns `(cert PEM, key PEM)`. The
/// cert PEM is the serving certificate followed by its CA.
pub fn generate_self_signed_cert_key_with_fixtures(
    host: &str,
    alternate_ips: &[IpAddr],
    alternate_dns: &[String],
    fixture_directory: &str,
) -> Result<(Vec<u8>, Vec<u8>), String> {
    generate_self_signed_cert_key_with_options(&SelfSignedCertKeyOptions {
        host: host.to_string(),
        alternate_ips: alternate_ips.to_vec(),
        alternate_dns: alternate_dns.to_vec(),
        fixture_directory: fixture_directory.to_string(),
        max_age: Duration::ZERO,
    })
}

/// `GenerateSelfSignedCertKeyWithOptions` (cert.go:146-259): an RSA-2048 CA and
/// an RSA-2048 serving certificate signed by it.
///
/// Fixtures (cert.go:161-171, 254-263): when `fixture_directory` is set and the
/// pair exists it is returned as is; otherwise it is generated with a
/// 100-year lifetime and written there.
pub fn generate_self_signed_cert_key_with_options(
    opts: &SelfSignedCertKeyOptions,
) -> Result<(Vec<u8>, Vec<u8>), String> {
    use rcgen::{
        BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose,
        Ia5String, IsCa, KeyPair, KeyUsagePurpose, SanType, SerialNumber,
    };
    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::pkcs8::EncodePrivateKey;
    use std::str::FromStr;

    let mut max_age = opts.max_age;
    if max_age.is_zero() {
        max_age = Duration::from_secs(365 * 24 * 3600);
    }
    // valid an hour earlier to avoid flakes due to clock skew (cert.go:156)
    let now = time::OffsetDateTime::now_utc()
        .replace_nanosecond(0)
        .map_err(|e| e.to_string())?;
    let valid_from = now - time::Duration::hours(1);

    let ips: Vec<String> = opts.alternate_ips.iter().map(|ip| ip.to_string()).collect();
    let base_name = format!(
        "{}_{}_{}",
        opts.host,
        ips.join("-"),
        opts.alternate_dns.join("-")
    );
    let cert_fixture =
        std::path::Path::new(&opts.fixture_directory).join(format!("{base_name}.crt"));
    let key_fixture =
        std::path::Path::new(&opts.fixture_directory).join(format!("{base_name}.key"));
    if !opts.fixture_directory.is_empty() {
        if let Ok(cert) = std::fs::read(&cert_fixture) {
            return match std::fs::read(&key_fixture) {
                Ok(key) => Ok((cert, key)),
                Err(e) => Err(format!(
                    "cert {} can be read, but key {} cannot: {}",
                    cert_fixture.display(),
                    key_fixture.display(),
                    e
                )),
            };
        }
        max_age = Duration::from_secs(100 * 24 * 3600 * 365); // 100 years fixtures
    }
    let valid_to = valid_from + time::Duration::seconds(max_age.as_secs() as i64);

    let new_rsa = || -> Result<(rsa::RsaPrivateKey, KeyPair), String> {
        let key =
            rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).map_err(|e| e.to_string())?;
        let der = key.to_pkcs8_der().map_err(|e| e.to_string())?;
        let pair = KeyPair::try_from(der.as_bytes()).map_err(|e| e.to_string())?;
        Ok((key, pair))
    };
    // `cryptorand.Int(MaxInt64-1) + 1`: uniform in [1, MaxInt64).
    let serial = || {
        let mut b = [0u8; 8];
        let _ = getrandom_fill(&mut b);
        SerialNumber::from(1 + u64::from_be_bytes(b) % (i64::MAX as u64 - 1))
    };

    let (_, ca_pair) = new_rsa()?;
    let mut ca_params = CertificateParams::default();
    ca_params.serial_number = Some(serial());
    let mut dn = DistinguishedName::new();
    dn.push(
        DnType::CommonName,
        format!("{}-ca@{}", opts.host, now.unix_timestamp()),
    );
    ca_params.distinguished_name = dn;
    ca_params.not_before = valid_from;
    ca_params.not_after = valid_to;
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyEncipherment,
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
    ];
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_pair).map_err(|e| e.to_string())?;

    let (leaf_key, leaf_pair) = new_rsa()?;
    let mut params = CertificateParams::default();
    params.serial_number = Some(serial());
    let mut dn = DistinguishedName::new();
    dn.push(
        DnType::CommonName,
        format!("{}@{}", opts.host, now.unix_timestamp()),
    );
    params.distinguished_name = dn;
    params.not_before = valid_from;
    params.not_after = valid_to;
    params.key_usages = vec![
        KeyUsagePurpose::KeyEncipherment,
        KeyUsagePurpose::DigitalSignature,
    ];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    params.is_ca = IsCa::ExplicitNoCa;
    // `if ip := ParseIPSloppy(host); ip != nil {IPAddresses} else {DNSNames}`
    params.subject_alt_names = Vec::new();
    match opts.host.parse::<IpAddr>() {
        Ok(ip) => params.subject_alt_names.push(SanType::IpAddress(ip)),
        Err(_) => params.subject_alt_names.push(SanType::DnsName(
            Ia5String::from_str(&opts.host).map_err(|e| e.to_string())?,
        )),
    }
    for ip in &opts.alternate_ips {
        params.subject_alt_names.push(SanType::IpAddress(*ip));
    }
    for dns in &opts.alternate_dns {
        params.subject_alt_names.push(SanType::DnsName(
            Ia5String::from_str(dns).map_err(|e| e.to_string())?,
        ));
    }
    let leaf = params
        .signed_by(&leaf_pair, &ca_cert, &ca_pair)
        .map_err(|e| e.to_string())?;

    // Generate cert, followed by ca (cert.go:240-247)
    let cert_pem = format!("{}{}", leaf.pem(), ca_cert.pem()).into_bytes();
    let key_pem = leaf_key
        .to_pkcs1_pem(rsa::pkcs8::LineEnding::LF)
        .map_err(|e| e.to_string())?
        .as_bytes()
        .to_vec();

    if !opts.fixture_directory.is_empty() {
        std::fs::write(&cert_fixture, &cert_pem).map_err(|e| {
            format!(
                "failed to write cert fixture to {}: {}",
                cert_fixture.display(),
                e
            )
        })?;
        write_private(&key_fixture, &key_pem).map_err(|e| {
            format!(
                "failed to write key fixture to {}: {}",
                cert_fixture.display(),
                e
            )
        })?;
    }
    Ok((cert_pem, key_pem))
}

fn getrandom_fill(buf: &mut [u8]) -> Result<(), String> {
    use rsa::rand_core::RngCore;
    rsa::rand_core::OsRng
        .try_fill_bytes(buf)
        .map_err(|e| e.to_string())
}

/// `os.WriteFile(path, data, 0600)`.
fn write_private(path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(data)
}

/// `CanReadCertAndKey` (io.go:28-45).
pub fn can_read_cert_and_key(cert_path: &str, key_path: &str) -> Result<bool, String> {
    let cert_readable = std::fs::File::open(cert_path).is_ok();
    let key_readable = std::fs::File::open(key_path).is_ok();
    if !cert_readable && !key_readable {
        return Ok(false);
    }
    if !cert_readable {
        return Err(format!(
            "error reading {cert_path}, certificate and key must be supplied as a pair"
        ));
    }
    if !key_readable {
        return Err(format!(
            "error reading {key_path}, certificate and key must be supplied as a pair"
        ));
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(crate::tls::x509_key_pair(&cert, &key), Ok(()));
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
