//! Service-account token key handling.
//!
//! Ported from:
//! - `staging/src/k8s.io/client-go/util/keyutil/key.go` (`PublicKeysFromFile`,
//!   `ParsePublicKeysPEM`, `ParsePrivateKeyPEM`)
//! - `pkg/serviceaccount/jwt.go` (`keyIDFromPublicKey`, `StaticPublicKeysGetter`,
//!   `signerFromRSAPrivateKey`, `signerFromECDSAPrivateKey`)
//!
//! Supported key types are RSA and ECDSA P-256 / P-384 (ES512 / P-521 is not
//! supported by the JWT backend and such keys are skipped like any other
//! unsupported PEM block).

use crate::error::{Error, Result};
use base64::Engine;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey};
use sha2::{Digest, Sha256};

const OID_RSA: &str = "1.2.840.113549.1.1.1";
const OID_EC: &str = "1.2.840.10045.2.1";
const OID_P256: &str = "1.2.840.10045.3.1.7";
const OID_P384: &str = "1.3.132.0.34";

/// One public key a service-account token may be verified with
/// (`pkg/serviceaccount/jwt.go` `PublicKey`).
#[derive(Clone)]
pub struct VerificationKey {
    /// `keyIDFromPublicKey`: base64url(sha256(PKIX DER)).
    pub key_id: String,
    /// Signature algorithms a token verified with this key may carry.
    pub algorithms: Vec<Algorithm>,
    pub decoding_key: DecodingKey,
}

impl std::fmt::Debug for VerificationKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerificationKey")
            .field("key_id", &self.key_id)
            .field("algorithms", &self.algorithms)
            .finish()
    }
}

/// A private key usable for signing (`signerFromRSAPrivateKey` /
/// `signerFromECDSAPrivateKey`).
#[derive(Clone)]
pub struct SigningKey {
    pub key_id: String,
    pub algorithm: Algorithm,
    pub encoding_key: EncodingKey,
    /// The matching verification key.
    pub public: VerificationKey,
}

/// `keyIDFromPublicKey` (`pkg/serviceaccount/jwt.go:99-112`) over a PKIX DER.
pub fn key_id_from_spki_der(spki_der: &[u8]) -> String {
    let hash = Sha256::digest(spki_der);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash)
}

fn pem_encode(label: &str, der: &[u8]) -> String {
    pem::encode(&pem::Pem::new(label, der.to_vec()))
}

/// Build a verification key from a PKIX SubjectPublicKeyInfo DER. Returns
/// `None` for key types we cannot verify with.
fn verification_key_from_spki(spki_der: &[u8]) -> Option<VerificationKey> {
    use x509_parser::prelude::FromDer;
    let (_, spki) = x509_parser::x509::SubjectPublicKeyInfo::from_der(spki_der).ok()?;
    let oid = spki.algorithm.algorithm.to_id_string();
    let pem = pem_encode("PUBLIC KEY", spki_der);
    let (algorithms, decoding_key) = match oid.as_str() {
        OID_RSA => (
            vec![Algorithm::RS256, Algorithm::RS384, Algorithm::RS512],
            DecodingKey::from_rsa_pem(pem.as_bytes()).ok()?,
        ),
        OID_EC => {
            let curve = spki
                .algorithm
                .parameters
                .as_ref()
                .and_then(|p| p.as_oid().ok())
                .map(|o| o.to_id_string())?;
            let alg = match curve.as_str() {
                OID_P256 => Algorithm::ES256,
                OID_P384 => Algorithm::ES384,
                _ => return None,
            };
            (vec![alg], DecodingKey::from_ec_pem(pem.as_bytes()).ok()?)
        }
        _ => return None,
    };
    Some(VerificationKey {
        key_id: key_id_from_spki_der(spki_der),
        algorithms,
        decoding_key,
    })
}

/// The SPKI DER of a PEM block holding a private key, plus the private key
/// re-encoded for the JWT backend.
struct ParsedPrivate {
    spki_der: Vec<u8>,
    algorithm: Algorithm,
    encoding_key: EncodingKey,
}

fn parse_private_block(der: &[u8]) -> Option<ParsedPrivate> {
    use rsa::pkcs1::{DecodeRsaPrivateKey, EncodeRsaPrivateKey};
    use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};

    // parseRSAPrivateKey: PKCS#1 then PKCS#8.
    let rsa_key = rsa::RsaPrivateKey::from_pkcs1_der(der)
        .or_else(|_| rsa::RsaPrivateKey::from_pkcs8_der(der))
        .ok();
    if let Some(key) = rsa_key {
        let spki = key.to_public_key().to_public_key_der().ok()?;
        let pkcs1 = key.to_pkcs1_der().ok()?;
        let pem = pem_encode("RSA PRIVATE KEY", pkcs1.as_bytes());
        return Some(ParsedPrivate {
            spki_der: spki.as_bytes().to_vec(),
            algorithm: Algorithm::RS256,
            encoding_key: EncodingKey::from_rsa_pem(pem.as_bytes()).ok()?,
        });
    }

    // parseECPrivateKey: SEC1 then PKCS#8, P-256 then P-384.
    {
        use p256::pkcs8::EncodePrivateKey;
        let k = p256::SecretKey::from_sec1_der(der)
            .or_else(|_| p256::SecretKey::from_pkcs8_der(der))
            .ok();
        if let Some(k) = k {
            let spki = k.public_key().to_public_key_der().ok()?;
            let pkcs8 = k.to_pkcs8_der().ok()?;
            let pem = pem_encode("PRIVATE KEY", pkcs8.as_bytes());
            return Some(ParsedPrivate {
                spki_der: spki.as_bytes().to_vec(),
                algorithm: Algorithm::ES256,
                encoding_key: EncodingKey::from_ec_pem(pem.as_bytes()).ok()?,
            });
        }
    }
    {
        use p384::pkcs8::EncodePrivateKey;
        let k = p384::SecretKey::from_sec1_der(der)
            .or_else(|_| p384::SecretKey::from_pkcs8_der(der))
            .ok();
        if let Some(k) = k {
            let spki = k.public_key().to_public_key_der().ok()?;
            let pkcs8 = k.to_pkcs8_der().ok()?;
            let pem = pem_encode("PRIVATE KEY", pkcs8.as_bytes());
            return Some(ParsedPrivate {
                spki_der: spki.as_bytes().to_vec(),
                algorithm: Algorithm::ES384,
                encoding_key: EncodingKey::from_ec_pem(pem.as_bytes()).ok()?,
            });
        }
    }
    None
}

/// SPKI DER from a public key or certificate block (`parseRSAPublicKey` /
/// `parseECPublicKey`: PKIX, falling back to an x509 certificate's key).
fn spki_from_public_block(der: &[u8]) -> Option<Vec<u8>> {
    use x509_parser::prelude::FromDer;
    if x509_parser::x509::SubjectPublicKeyInfo::from_der(der).is_ok() {
        return Some(der.to_vec());
    }
    let (_, cert) = x509_parser::certificate::X509Certificate::from_der(der).ok()?;
    Some(cert.tbs_certificate.subject_pki.raw.to_vec())
}

/// `ParsePublicKeysPEM` (`keyutil/key.go:196-232`): every RSA/ECDSA private or
/// public key (or certificate) in `data`, as public keys. Non-key PEM blocks
/// are tolerated; no key at all is an error.
pub fn parse_public_keys_pem(data: &[u8]) -> Result<Vec<VerificationKey>> {
    let mut keys = Vec::new();
    for block in pem::parse_many(data).unwrap_or_default() {
        let der = block.contents();
        if let Some(private) = parse_private_block(der) {
            if let Some(k) = verification_key_from_spki(&private.spki_der) {
                keys.push(k);
            }
            continue;
        }
        if let Some(k) = spki_from_public_block(der).and_then(|s| verification_key_from_spki(&s)) {
            keys.push(k);
        }
    }
    if keys.is_empty() {
        return Err(Error::InvalidResource(
            "data does not contain any valid RSA or ECDSA public keys".to_string(),
        ));
    }
    Ok(keys)
}

/// `PublicKeysFromFile` (`keyutil/key.go:137-147`).
pub fn public_keys_from_file(path: &str) -> Result<Vec<VerificationKey>> {
    let data = std::fs::read(path)
        .map_err(|e| Error::Internal(format!("error reading public key file {path}: {e}")))?;
    parse_public_keys_pem(&data)
        .map_err(|e| Error::Internal(format!("error reading public key file {path}: {e}")))
}

/// `ParsePrivateKeyPEM` + signer construction: the first private key in
/// `data` (`--service-account-signing-key-file`).
pub fn signing_key_from_pem(data: &[u8]) -> Result<SigningKey> {
    for block in pem::parse_many(data).unwrap_or_default() {
        if let Some(p) = parse_private_block(block.contents()) {
            let public = verification_key_from_spki(&p.spki_der).ok_or_else(|| {
                Error::Internal("failed to derive public key for signing key".to_string())
            })?;
            return Ok(SigningKey {
                key_id: public.key_id.clone(),
                algorithm: p.algorithm,
                encoding_key: p.encoding_key,
                public,
            });
        }
    }
    Err(Error::InvalidResource(
        "data does not contain a valid RSA or ECDSA private key".to_string(),
    ))
}
