//! Validation for `certificates.k8s.io` PodCertificateRequest, ported from
//! upstream `pkg/apis/certificates/validation/validation.go`:
//! `ValidatePodCertificateRequestCreate` (:592-715),
//! `ValidatePodCertificateRequestUpdate` (:733-744) and
//! `ValidatePodCertificateRequestStatusUpdate` (:748-934), plus the helpers
//! `pcrIsIssued/Denied/Failed`, `validateSemanticEquality`, `timeNear`
//! (:936-989) and `apivalidation.ValidateUserAnnotations`
//! (`pkg/apis/core/validation/validation.go:146-156`).
//!
//! Deliberate deviations (the Rust crypto stack differs from Go's):
//! * RSA-PSS verification: Go calls `rsa.VerifyPSS(.., nil)` (salt length
//!   auto-detected). The `rsa` crate needs the salt length up front, so the
//!   two lengths a signer produces in practice are tried: the maximum
//!   (`PSSSaltLengthAuto` on sign) and the digest length
//!   (`PSSSaltLengthEqualsHash`).
//! * `mail.ParseAddress` on SAN e-mail addresses is approximated by a
//!   structural check (single `@`, non-empty local/domain, no whitespace or
//!   angle brackets).

use crate::resources::podcertificaterequest::{
    PodCertificateRequest, PodCertificateRequestStatus, CONDITION_TYPE_DENIED,
    CONDITION_TYPE_FAILED, CONDITION_TYPE_ISSUED, KUBERNETES_MAX_MAX_EXPIRATION_SECONDS,
    MAX_CERTIFICATE_CHAIN_SIZE, MAX_MAX_EXPIRATION_SECONDS, MAX_PKIX_PUBLIC_KEY_SIZE,
    MAX_PROOF_OF_POSSESSION_SIZE, MIN_MAX_EXPIRATION_SECONDS,
};
use crate::validation::certificatesigningrequest::validate_signer_name;
use crate::validation::field::{BadValue, Error, ErrorList, Path};
use crate::validation::metav1::{is_qualified_name, validate_conditions};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_annotations_size, validate_immutable_field,
    validate_object_meta, validate_object_meta_update, TOTAL_ANNOTATION_SIZE_LIMIT_B,
};
use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};
use x509_parser::x509::SubjectPublicKeyInfo;

/// `IsKubernetesSignerName` (`pkg/apis/core/validation/names.go:29-32`).
pub fn is_kubernetes_signer_name(signer_name: &str) -> bool {
    let host = signer_name.split('/').next().unwrap_or("");
    host == "kubernetes.io" || host.ends_with(".kubernetes.io")
}

/// `ValidateUserAnnotations` (`pkg/apis/core/validation/validation.go:146-156`).
/// Every key must be a domain-prefixed key (`IsDomainPrefixedKey`,
/// `apimachinery/pkg/util/validation/validation.go:124-143`), compared
/// case-insensitively; the total size is capped like annotations.
pub fn validate_user_annotations(
    user_annotations: &HashMap<String, String>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = ErrorList::new();
    for k in user_annotations.keys() {
        let key = k.to_lowercase();
        if key.is_empty() {
            errs.push(Error::required(fld_path, ""));
            continue;
        }
        let msgs = is_qualified_name(&key);
        if !msgs.is_empty() {
            for m in msgs {
                errs.push(Error::invalid(fld_path, key.clone(), m));
            }
            continue;
        }
        if key.split('/').count() != 2 {
            errs.push(Error::invalid(
                fld_path,
                key.clone(),
                "must be a domain-prefixed key (such as \"acme.io/foo\")",
            ));
        }
    }
    if validate_annotations_size(user_annotations).is_some() {
        errs.push(Error::too_long(fld_path, TOTAL_ANNOTATION_SIZE_LIMIT_B));
    }
    errs
}

fn pkix_path() -> Path {
    Path::new("spec").child("pkixPublicKey")
}
fn pop_path() -> Path {
    Path::new("spec").child("proofOfPossession")
}
fn cert_chain_path() -> Path {
    Path::new("status").child("certificateChain")
}
fn not_before_path() -> Path {
    Path::new("status").child("notBefore")
}
fn not_after_path() -> Path {
    Path::new("status").child("notAfter")
}
fn begin_refresh_path() -> Path {
    Path::new("status").child("beginRefreshAt")
}

/// `[]byte` values render as a base64 string in Go's `ErrorBody` JSON.
fn b64(bytes: &[u8]) -> BadValue {
    BadValue::String(base64::engine::general_purpose::STANDARD.encode(bytes))
}

fn fmt_time(t: &DateTime<Utc>) -> BadValue {
    BadValue::String(t.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
}

/// `ValidatePodCertificateRequestCreate` (validation.go:592-715).
pub fn validate_pod_certificate_request_create(req: &PodCertificateRequest) -> ErrorList {
    let mut errs = ErrorList::new();
    let spec = &req.spec;
    let spec_path = Path::new("spec");

    errs.extend(validate_object_meta(
        &req.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    ));
    errs.extend(validate_signer_name(
        &spec_path.child("signerName"),
        &spec.signer_name,
    ));
    if let Some(ann) = &spec.unverified_user_annotations {
        errs.extend(validate_user_annotations(
            ann,
            &spec_path.child("unverifiedUserAnnotations"),
        ));
    }

    for msg in name_is_dns_subdomain(&spec.pod_name, false) {
        errs.push(Error::invalid(
            &spec_path.child("podName"),
            spec.pod_name.clone(),
            msg,
        ));
    }
    validate_uid(&mut errs, "podUID", &spec.pod_uid);
    for msg in name_is_dns_subdomain(&spec.service_account_name, false) {
        errs.push(Error::invalid(
            &spec_path.child("serviceAccountName"),
            spec.service_account_name.clone(),
            msg,
        ));
    }
    validate_uid(&mut errs, "serviceAccountUID", &spec.service_account_uid);
    for msg in name_is_dns_subdomain(&spec.node_name, false) {
        errs.push(Error::invalid(
            &spec_path.child("nodeName"),
            spec.node_name.clone(),
            msg,
        ));
    }
    validate_uid(&mut errs, "nodeUID", &spec.node_uid);

    let max_path = spec_path.child("maxExpirationSeconds");
    let Some(max_exp) = spec.max_expiration_seconds else {
        errs.push(Error::required(&max_path, "must be set"));
        return errs;
    };
    let upper = if is_kubernetes_signer_name(&spec.signer_name) {
        // Kubernetes signers are restricted to max 24 hour certs.
        KUBERNETES_MAX_MAX_EXPIRATION_SECONDS
    } else {
        // All other signers are restricted to max 91 day certs.
        MAX_MAX_EXPIRATION_SECONDS
    };
    if !(MIN_MAX_EXPIRATION_SECONDS <= max_exp && max_exp <= upper) {
        errs.push(Error::invalid(
            &max_path,
            max_exp,
            format!("must be in the range [{MIN_MAX_EXPIRATION_SECONDS}, {upper}]"),
        ));
    }

    if spec.pkix_public_key.len() > MAX_PKIX_PUBLIC_KEY_SIZE {
        errs.push(Error::too_long(&pkix_path(), MAX_PKIX_PUBLIC_KEY_SIZE));
        return errs;
    }
    if spec.proof_of_possession.len() > MAX_PROOF_OF_POSSESSION_SIZE {
        errs.push(Error::too_long(&pop_path(), MAX_PROOF_OF_POSSESSION_SIZE));
        return errs;
    }

    match parse_pkix_public_key(&spec.pkix_public_key) {
        Err(PkixError::Invalid) => errs.push(Error::invalid(
            &pkix_path(),
            b64(&spec.pkix_public_key),
            "must be a valid PKIX-serialized public key",
        )),
        Err(PkixError::UnknownType) => errs.push(Error::invalid(
            &pkix_path(),
            b64(&spec.pkix_public_key),
            "unknown public key type; supported types are Ed25519, ECDSA, and RSA",
        )),
        Ok(key) => {
            let digest = Sha256::digest(spec.pod_uid.as_bytes());
            let sig = &spec.proof_of_possession;
            let pop_err = || {
                Error::invalid(
                    &pop_path(),
                    BadValue::Omit,
                    "could not verify proof-of-possession signature",
                )
            };
            match key {
                PublicKey::Ed25519(pk) => {
                    // ed25519 has no key configuration to check.
                    use ed25519_dalek::Verifier;
                    let ok = ed25519_dalek::Signature::from_slice(sig)
                        .ok()
                        .map(|s| pk.verify(spec.pod_uid.as_bytes(), &s).is_ok())
                        .unwrap_or(false);
                    if !ok {
                        errs.push(pop_err());
                    }
                }
                PublicKey::Ecdsa(verify) => {
                    // ecdsa.VerifyASN1 over sha256(podUID), whatever the curve.
                    if !verify(&digest, sig) {
                        errs.push(pop_err());
                    }
                }
                PublicKey::Rsa(pk) => {
                    use rsa::traits::PublicKeyParts;
                    let bits = pk.size() * 8;
                    if bits != 3072 && bits != 4096 {
                        errs.push(Error::invalid(
                            &pkix_path(),
                            format!("{bits}-bit modulus"),
                            "RSA keys must have modulus size 3072 or 4096",
                        ));
                    } else if !verify_rsa_pss(&pk, &digest, sig) {
                        errs.push(pop_err());
                    }
                }
            }
        }
    }
    errs
}

fn validate_uid(errs: &mut ErrorList, field: &str, value: &str) {
    let p = Path::new("spec").child(field);
    if value.is_empty() {
        errs.push(Error::invalid(&p, value.to_string(), "must not be empty"));
    }
    if value.len() > 128 {
        errs.push(Error::too_long(&p, 128));
    }
}

enum PkixError {
    Invalid,
    UnknownType,
}

type EcdsaVerifier = Box<dyn Fn(&[u8], &[u8]) -> bool>;

enum PublicKey {
    Ed25519(ed25519_dalek::VerifyingKey),
    /// Verifies an ASN.1/DER signature over a prehashed digest.
    Ecdsa(EcdsaVerifier),
    Rsa(rsa::RsaPublicKey),
}

const OID_RSA: &str = "1.2.840.113549.1.1.1";
const OID_EC: &str = "1.2.840.10045.2.1";
const OID_ED25519: &str = "1.3.101.112";
const OID_X25519: &str = "1.3.101.110";
const OID_P256: &str = "1.2.840.10045.3.1.7";
const OID_P384: &str = "1.3.132.0.34";
const OID_P521: &str = "1.3.132.0.35";

/// `x509.ParsePKIXPublicKey` restricted to the types the validator switches
/// on. X25519 parses in Go but lands in the `default:` arm.
fn parse_pkix_public_key(der: &[u8]) -> Result<PublicKey, PkixError> {
    use p256::pkcs8::DecodePublicKey;
    let (rest, spki) = SubjectPublicKeyInfo::from_der(der).map_err(|_| PkixError::Invalid)?;
    if !rest.is_empty() {
        return Err(PkixError::Invalid);
    }
    let alg = spki.algorithm.algorithm.to_id_string();
    match alg.as_str() {
        OID_ED25519 => ed25519_dalek::VerifyingKey::from_public_key_der(der)
            .map(PublicKey::Ed25519)
            .map_err(|_| PkixError::Invalid),
        OID_X25519 => Err(PkixError::UnknownType),
        OID_RSA => rsa::RsaPublicKey::from_public_key_der(der)
            .map(PublicKey::Rsa)
            .map_err(|_| PkixError::Invalid),
        OID_EC => {
            let curve = spki
                .algorithm
                .parameters
                .as_ref()
                .and_then(|p| p.as_oid().ok())
                .map(|o| o.to_id_string())
                .ok_or(PkixError::Invalid)?;
            // Go rejects curves other than P-224/256/384/521 at parse time.
            use p256::ecdsa::signature::hazmat::PrehashVerifier;
            match curve.as_str() {
                OID_P256 => {
                    let vk = p256::ecdsa::VerifyingKey::from_public_key_der(der)
                        .map_err(|_| PkixError::Invalid)?;
                    Ok(PublicKey::Ecdsa(Box::new(move |digest, sig| {
                        p256::ecdsa::Signature::from_der(sig)
                            .map(|s| vk.verify_prehash(&left_pad(digest, 32), &s).is_ok())
                            .unwrap_or(false)
                    })))
                }
                OID_P384 => {
                    let vk = p384::ecdsa::VerifyingKey::from_public_key_der(der)
                        .map_err(|_| PkixError::Invalid)?;
                    Ok(PublicKey::Ecdsa(Box::new(move |digest, sig| {
                        p384::ecdsa::Signature::from_der(sig)
                            .map(|s| vk.verify_prehash(&left_pad(digest, 48), &s).is_ok())
                            .unwrap_or(false)
                    })))
                }
                OID_P521 => {
                    // p521's VerifyingKey has no DecodePublicKey impl; go via
                    // the curve's PublicKey and its SEC1 encoding.
                    use p521::elliptic_curve::sec1::ToEncodedPoint;
                    let pk = p521::PublicKey::from_public_key_der(der)
                        .map_err(|_| PkixError::Invalid)?;
                    let vk = p521::ecdsa::VerifyingKey::from_sec1_bytes(
                        pk.to_encoded_point(false).as_bytes(),
                    )
                    .map_err(|_| PkixError::Invalid)?;
                    Ok(PublicKey::Ecdsa(Box::new(move |digest, sig| {
                        p521::ecdsa::Signature::from_der(sig)
                            .map(|s| vk.verify_prehash(&left_pad(digest, 66), &s).is_ok())
                            .unwrap_or(false)
                    })))
                }
                _ => Err(PkixError::Invalid),
            }
        }
        _ => Err(PkixError::Invalid),
    }
}

/// Go's `ecdsa.VerifyASN1` reads the digest as a big-endian integer (hashToNat),
/// so a SHA-256 digest is valid for any curve. RustCrypto's `verify_prehash`
/// rejects a digest shorter than half the field size (P-521: 33 bytes), so the
/// digest is left-padded with zeros to the field size — the same integer.
fn left_pad(digest: &[u8], len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len.saturating_sub(digest.len())];
    out.extend_from_slice(digest);
    out
}

/// `rsa.VerifyPSS(pub, crypto.SHA256, digest, sig, nil)`. See the module doc
/// for the salt-length deviation.
fn verify_rsa_pss(pk: &rsa::RsaPublicKey, digest: &[u8], sig: &[u8]) -> bool {
    use rsa::traits::PublicKeyParts;
    let em_len = (pk.n().bits() - 1).div_ceil(8);
    let max_salt = em_len.saturating_sub(digest.len() + 2);
    [max_salt, digest.len()].into_iter().any(|salt_len| {
        let vk = rsa::pss::VerifyingKey::<Sha256>::new_with_salt_len(pk.clone(), salt_len);
        let Ok(sig) = rsa::pss::Signature::try_from(sig) else {
            return false;
        };
        rsa::signature::hazmat::PrehashVerifier::verify_prehash(&vk, digest, &sig).is_ok()
    })
}

/// `ValidatePodCertificateRequestUpdate` (validation.go:733-744): all spec
/// fields are immutable; status goes through the status verb.
pub fn validate_pod_certificate_request_update(
    new_req: &PodCertificateRequest,
    old_req: &PodCertificateRequest,
) -> ErrorList {
    let mut errs = ErrorList::new();
    errs.extend(validate_object_meta_update(
        &new_req.metadata,
        &old_req.metadata,
        &Path::new("metadata"),
    ));
    errs.extend(validate_immutable_field(
        &new_req.spec,
        &old_req.spec,
        &Path::new("spec"),
    ));
    errs
}

fn has_true_condition(status: &PodCertificateRequestStatus, ty: &str) -> bool {
    status
        .conditions
        .iter()
        .any(|c| c.condition_type == ty && c.status == "True")
}

/// `pcrIsIssued` (validation.go:936).
pub fn pcr_is_issued(req: &PodCertificateRequest) -> bool {
    has_true_condition(&req.status, CONDITION_TYPE_ISSUED)
}
/// `pcrIsDenied` (validation.go:945).
pub fn pcr_is_denied(req: &PodCertificateRequest) -> bool {
    has_true_condition(&req.status, CONDITION_TYPE_DENIED)
}
/// `pcrIsFailed` (validation.go:954).
pub fn pcr_is_failed(req: &PodCertificateRequest) -> bool {
    has_true_condition(&req.status, CONDITION_TYPE_FAILED)
}

/// `validateSemanticEquality` (validation.go:967-973).
fn validate_semantic_equality(
    old_val: &PodCertificateRequestStatus,
    new_val: &PodCertificateRequestStatus,
    fld_path: &Path,
    detail: &str,
) -> ErrorList {
    let a = serde_json::to_value(old_val).unwrap_or_default();
    let b = serde_json::to_value(new_val).unwrap_or_default();
    if a != b {
        return vec![Error::invalid(fld_path, BadValue::Omit, detail)];
    }
    ErrorList::new()
}

/// `timeNear` (validation.go:975-977).
fn time_near(a: DateTime<Utc>, b: DateTime<Utc>, skew: Duration) -> bool {
    a > b - skew && a < b + skew
}

/// Go's `pem.Decode` loop: yields `(type, der)` for each well-formed block,
/// stopping at the first thing that is not one.
fn pem_blocks(chain: &str) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut rest = chain;
    while let Some(start) = rest.find("-----BEGIN ") {
        let after = &rest[start + "-----BEGIN ".len()..];
        let Some(ty_end) = after.find("-----") else {
            break;
        };
        let ty = &after[..ty_end];
        let body_and_rest = &after[ty_end + 5..];
        let end_marker = format!("-----END {ty}-----");
        let Some(end) = body_and_rest.find(&end_marker) else {
            break;
        };
        let body: String = body_and_rest[..end]
            .lines()
            .filter(|l| !l.contains(':'))
            .flat_map(|l| l.split_whitespace())
            .collect();
        let Ok(der) = base64::engine::general_purpose::STANDARD.decode(body) else {
            break;
        };
        out.push((ty.to_string(), der));
        rest = &body_and_rest[end + end_marker.len()..];
    }
    out
}

/// Structural stand-in for Go's `mail.ParseAddress` on a bare address.
fn is_valid_email(addr: &str) -> bool {
    let mut parts = addr.splitn(2, '@');
    let (local, domain) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    !local.is_empty()
        && !domain.is_empty()
        && !domain.contains('@')
        && !addr
            .chars()
            .any(|c| c.is_whitespace() || "<>(),;:\\\"".contains(c))
}

/// Go's `PublicKey.Equal` over two SPKIs.
fn same_public_key(want_der: &[u8], leaf: &SubjectPublicKeyInfo<'_>) -> bool {
    let Ok((_, want)) = SubjectPublicKeyInfo::from_der(want_der) else {
        return false;
    };
    let oid = |s: &SubjectPublicKeyInfo<'_>| s.algorithm.algorithm.to_id_string();
    if oid(&want) != oid(leaf) || want.subject_public_key.data != leaf.subject_public_key.data {
        return false;
    }
    // EC keys are only the same key on the same curve; RSA params are a NULL
    // that Go accepts present or absent, so they are not compared.
    if oid(&want) == OID_EC {
        let curve = |s: &SubjectPublicKeyInfo<'_>| {
            s.algorithm
                .parameters
                .as_ref()
                .and_then(|p| p.as_oid().ok())
                .map(|o| o.to_id_string())
        };
        return curve(&want) == curve(leaf);
    }
    true
}

/// `ValidatePodCertificateRequestStatusUpdate` (validation.go:748-934).
/// `now` is the injected `clock.PassiveClock` reading.
pub fn validate_pod_certificate_request_status_update(
    new_req: &PodCertificateRequest,
    old_req: &PodCertificateRequest,
    now: DateTime<Utc>,
) -> ErrorList {
    let mut errs = ErrorList::new();

    // Metadata is *mostly* immutable (the strategy has already run
    // ResetObjectMetaForStatus on newReq.ObjectMeta).
    errs.extend(validate_object_meta_update(
        &new_req.metadata,
        &old_req.metadata,
        &Path::new("metadata"),
    ));
    if !errs.is_empty() {
        return errs;
    }

    // Don't validate spec. Strategy has stomped it.

    // At most one of the known conditions, and it must have status "True".
    let cond_path = Path::new("status").child("conditions");
    let mut num_known = 0;
    for (i, cond) in new_req.status.conditions.iter().enumerate() {
        match cond.condition_type.as_str() {
            CONDITION_TYPE_ISSUED | CONDITION_TYPE_DENIED | CONDITION_TYPE_FAILED => {
                num_known += 1;
                if num_known > 1 {
                    errs.push(Error::invalid(
                        &cond_path.index(i).child("type"),
                        cond.condition_type.clone(),
                        "There may be at most one condition with type \"Issued\", \"Denied\", or \"Failed\"",
                    ));
                }
                if cond.status != "True" {
                    errs.push(Error::not_supported(
                        &cond_path.index(i).child("status"),
                        cond.status.clone(),
                        &["True"],
                    ));
                }
            }
            _ => errs.push(Error::not_supported(
                &cond_path.index(i).child("type"),
                cond.condition_type.clone(),
                &[
                    CONDITION_TYPE_ISSUED,
                    CONDITION_TYPE_DENIED,
                    CONDITION_TYPE_FAILED,
                ],
            )),
        }
    }
    errs.extend(validate_conditions(&new_req.status.conditions, &cond_path));

    // Bail if something seems wrong with the conditions.
    if !errs.is_empty() {
        return errs;
    }

    let status_path = Path::new("status");

    // Terminal old object: the whole status is immutable.
    if pcr_is_issued(old_req) || pcr_is_denied(old_req) || pcr_is_failed(old_req) {
        errs.extend(validate_semantic_equality(
            &new_req.status,
            &old_req.status,
            &status_path,
            "immutable after PodCertificateRequest is issued, denied, or failed",
        ));
        return errs;
    }

    // Transitioning to Denied/Failed: no other status fields may change.
    if pcr_is_denied(new_req) || pcr_is_failed(new_req) {
        let want = PodCertificateRequestStatus {
            conditions: new_req.status.conditions.clone(),
            ..Default::default()
        };
        errs.extend(validate_semantic_equality(
            &new_req.status,
            &want,
            &status_path,
            "non-condition status fields must be empty when denying or failing the PodCertificateRequest",
        ));
        return errs;
    }

    // Transitioning to Issued.
    if pcr_is_issued(new_req) {
        validate_issued(&mut errs, new_req, old_req, now);
        return errs;
    }

    // Not transitioning to any terminal state: status is immutable.
    errs.extend(validate_semantic_equality(
        &new_req.status,
        &old_req.status,
        &status_path,
        "status is immutable unless transitioning to \"Issued\", \"Denied\", or \"Failed\"",
    ));
    errs
}

/// The `if pcrIsIssued(newReq) { ... }` arm (validation.go:810-926).
fn validate_issued(
    errs: &mut ErrorList,
    new_req: &PodCertificateRequest,
    old_req: &PodCertificateRequest,
    now: DateTime<Utc>,
) {
    let status = &new_req.status;
    let chain_path = cert_chain_path();
    let chain_val = || BadValue::String(status.certificate_chain.clone());

    if status.certificate_chain.len() > MAX_CERTIFICATE_CHAIN_SIZE {
        errs.push(Error::too_long(&chain_path, MAX_CERTIFICATE_CHAIN_SIZE));
        return;
    }

    let blocks = pem_blocks(&status.certificate_chain);
    let Some((leaf_ty, leaf_der)) = blocks.first() else {
        errs.push(Error::invalid(
            &chain_path,
            chain_val(),
            "issued certificate chain must contain at least one certificate",
        ));
        return;
    };
    if leaf_ty != "CERTIFICATE" {
        errs.push(Error::invalid(
            &chain_path,
            chain_val(),
            "issued certificate chain must consist entirely of CERTIFICATE PEM blocks",
        ));
        return;
    }
    let Ok((_, leaf)) = X509Certificate::from_der(leaf_der) else {
        errs.push(Error::invalid(
            &chain_path,
            chain_val(),
            "leaf certificate does not parse as valid X.509",
        ));
        return;
    };

    if let Ok(Some(san)) = leaf.subject_alternative_name() {
        for name in &san.value.general_names {
            match name {
                GeneralName::DNSName(dns) => {
                    if dns.is_empty() {
                        errs.push(Error::invalid(
                            &chain_path,
                            dns.to_string(),
                            "leaf certificate should not contain empty DNSName",
                        ));
                    }
                    if dns.contains("..") {
                        errs.push(Error::invalid(
                            &chain_path,
                            dns.to_string(),
                            "leaf certificate's DNSName should not contain '..'",
                        ));
                    }
                    if dns.starts_with('.') || dns.ends_with('.') {
                        errs.push(Error::invalid(
                            &chain_path,
                            dns.to_string(),
                            "leaf certificate's DNSName should not start or end with '.'",
                        ));
                    }
                }
                GeneralName::RFC822Name(email) if !is_valid_email(email) => {
                    errs.push(Error::invalid(
                        &chain_path,
                        email.to_string(),
                        "leaf certificate should not contain invalid EmailAddress",
                    ));
                }
                _ => {}
            }
        }
    }

    // Was the certificate issued to the public key in the spec?
    match parse_pkix_public_key(&old_req.spec.pkix_public_key) {
        Err(_) => {
            errs.push(Error::invalid(
                &pkix_path(),
                b64(&old_req.spec.pkix_public_key),
                "must be a valid PKIX-serialized public key",
            ));
            return;
        }
        Ok(_) => {
            if !same_public_key(&old_req.spec.pkix_public_key, leaf.public_key()) {
                errs.push(Error::invalid(
                    &chain_path,
                    chain_val(),
                    "leaf certificate was not issued to the requested public key",
                ));
                return;
            }
        }
    }

    // All timestamps must be set.
    if status.not_before.is_none() {
        errs.push(Error::required(
            &not_before_path(),
            "must be present and consistent with the issued certificate",
        ));
    }
    if status.not_after.is_none() {
        errs.push(Error::required(
            &not_after_path(),
            "must be present and consistent with the issued certificate",
        ));
    }
    if status.begin_refresh_at.is_none() {
        errs.push(Error::required(
            &begin_refresh_path(),
            "must be present and in the range [notbefore+10min, notafter-10min]",
        ));
    }
    let (Some(not_before), Some(not_after), Some(begin_refresh)) =
        (status.not_before, status.not_after, status.begin_refresh_at)
    else {
        return;
    };
    if !errs.is_empty() {
        return;
    }

    let cert_not_before =
        DateTime::<Utc>::from_timestamp(leaf.validity().not_before.timestamp(), 0)
            .unwrap_or_default();
    let cert_not_after = DateTime::<Utc>::from_timestamp(leaf.validity().not_after.timestamp(), 0)
        .unwrap_or_default();

    // NotBefore consistent with the cert, and within 5 minutes of now.
    if not_before != cert_not_before {
        errs.push(Error::invalid(
            &not_before_path(),
            fmt_time(&not_before),
            "must be set to the NotBefore time encoded in the leaf certificate",
        ));
        return;
    }
    if !time_near(not_before, now, Duration::minutes(5)) {
        errs.push(Error::invalid(
            &not_before_path(),
            fmt_time(&not_before),
            "must be set to within 5 minutes of kube-apiserver's current time",
        ));
        return;
    }

    // NotAfter consistent with the cert.
    if not_after != cert_not_after {
        errs.push(Error::invalid(
            &not_after_path(),
            fmt_time(&not_after),
            "must be set to the NotAfter time encoded in the leaf certificate",
        ));
        return;
    }

    // Leaf lifetime against the minimum and maximum.
    let lifetime = cert_not_after - cert_not_before;
    let lifetime_nanos = lifetime.num_seconds().saturating_mul(1_000_000_000);
    if lifetime < Duration::hours(1) {
        errs.push(Error::invalid(
            &chain_path,
            lifetime_nanos,
            "leaf certificate lifetime must be >= 1 hour",
        ));
        return;
    }
    if let Some(max_exp) = new_req.spec.max_expiration_seconds {
        if lifetime > Duration::seconds(max_exp as i64) {
            errs.push(Error::invalid(
                &chain_path,
                lifetime_nanos,
                format!(
                    "leaf certificate lifetime must be <= spec.maxExpirationSeconds ({max_exp})"
                ),
            ));
            return;
        }
    }

    // BeginRefreshAt within limits.
    if begin_refresh < not_before + Duration::minutes(10) {
        errs.push(Error::invalid(
            &begin_refresh_path(),
            fmt_time(&begin_refresh),
            "must be at least 10 minutes after status.notBefore",
        ));
        return;
    }
    if begin_refresh > not_after - Duration::minutes(10) {
        errs.push(Error::invalid(
            &begin_refresh_path(),
            fmt_time(&begin_refresh),
            "must be at least 10 minutes before status.notAfter",
        ));
        return;
    }

    // The remainder of the chain must at least be valid certificates.
    for (ty, der) in blocks.iter().skip(1) {
        if ty != "CERTIFICATE" {
            errs.push(Error::invalid(
                &chain_path,
                chain_val(),
                "issued certificate chain must consist entirely of CERTFICATE PEM blocks",
            ));
            return;
        }
        if X509Certificate::from_der(der).is_err() {
            errs.push(Error::invalid(
                &chain_path,
                chain_val(),
                "intermediate certificate does not parse as valid X.509",
            ));
            return;
        }
    }
}
