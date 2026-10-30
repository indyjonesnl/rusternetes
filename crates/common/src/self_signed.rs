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
