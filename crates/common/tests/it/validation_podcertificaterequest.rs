//! PodCertificateRequest validation parity tests. Cases mirror upstream
//! `pkg/apis/certificates/validation/validation_test.go`
//! (`TestValidatePodCertificateRequestCreate`, ...`Update`, ...`StatusUpdate`).

use base64::Engine;
use chrono::{DateTime, TimeZone, Utc};
use ed25519_dalek::pkcs8::EncodePublicKey;
use p256::ecdsa::signature::hazmat::PrehashSigner;
use rcgen::{CertificateParams, KeyPair, SubjectPublicKeyInfo, PKCS_ECDSA_P256_SHA256};
use rusternetes_common::resources::podcertificaterequest::*;
use rusternetes_common::types::Condition;
use rusternetes_common::validation::podcertificaterequest::*;
use sha2::{Digest, Sha256};

const RSA3072_PUB: &str = "MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAtaPQUBH2vZp6eIn3LYa4+WxDGb2fziig6Gkt4/fR/LqGupuHIfjxR7p+nhB6dFH6D3aetTF/UaYOsQWXZ/BkYQSqepHtbPQVousNoXdxri3R7QwuDVHKYEQ7/nJiDQ6Qe/n0Jx4qKLqbZw9zxwdhYRTS5j0k/zEBzk8a4R31cT9Mi1EJMCOSOtn2jHTthf0BThItUspv+GwRZffMu4GedOg7C/asaHnay+1J6STxAWeWRX10p5tRzFgrhT+rrkvrm6Bdbrb4F3hWXvOWySpXJBxztdEz76AmPGZLTMRcdiTaxRaga8zLGx2sI2v0HErZIi+636rMYldjV87wB10NE3rL1zycgg2sChQb8YqAcsBK8n186p0pfzLdBB79e9oilimeAZyBsh/L7W2Kwiz1KmHnES9YaKTbQbX2hUoMJd+03ZLeNIs1ESGY3xKxmg/N6OgjxEqbsAxV1WIF+SME7tMyVKxnqFsPHLFqAu2oL4CUTQTmEv1fCMENfgmkDwUBAgMBAAE=";
const RSA2048_PUB: &str = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3bP3k2L5gkj8QCOLr3GZgF/XqyNN24nZ3yADl3Fk5UU4355aYtw87YSXpqNPoycyO6RHnlGGIQ047uvOayy/Fmb41VKnfZW1zIYasiLEPPIzelB7whp0ZfZt0R4tdiVYd0RFx83UljwzglPh4f5GaEj4hAlkv1Hr/ofkBX/8hJoKrbTdStF7qzhmX0qcXWU9g78r/dbN/ZNJutUc9N8t0zc98sEDU3PmfQ1DXGFWLKIxWcRcJgAUUUv6+an/YZAuoKdskxIGPfbwhfxwMHZg0mssZL8hEi4KpqG/UkptOBE2zCTDhMaQzQ4Hh+ysG7JzP/5PfRBb80XRRW1a+ZIc8QIDAQAB";
/// RSA-PSS/SHA-256 over sha256("pod-uid-1"), salt = maximum (Go PSSSaltLengthAuto).
const RSA3072_SIG_MAX_SALT: &str = "F23DW40IhA2RgsfLvjkXC9kwUXMk91yDHdPqTzJy230FVcRSepfDrIyTg6jbTF2ELXDNvQlVniP4Sdv/oeyVPkL44M/xwGE4TaZ3IVK08vMBKQ/yg4CbqBurrgvAekQjELsbq7fWZrz6NfGMYUaORuVlF5zMTtxtAKD9uuCbxWqxIIm5RMQShuuUmuH8/O7eyFD29Yjm4cO9D7/S1FbbSwRx2lEEiqz7ezSHCC5SbEJoArvNoB3qxz2nNhx8UH5RQwyjR5ue2dNceW2QhE9NHhoqa+8pXtUM6929IEFiRuqBtjmwHY5mlzUIvhj4bHc4DJdtz/cPMhDEgH7elp52IHw8JC5rSJloiDxjMR756L4pGTSG0JD8naHZ7hUPjVtJ8jDOeK7nzMvnteLCrVunkpDTpMzxwOfPHtalSvHKtrRMEieiWM9dBsP2ZmlhLR4gqMX99pzFO3TK5/DvUq29gRxvBuqd5ux8KahX7dtMQoXfwBhZhUF0BzQuPG6zVk9I";
/// Same, salt = digest length (Go PSSSaltLengthEqualsHash).
const RSA3072_SIG_HASH_SALT: &str = "SApof5GCv4Ws4A53S/FakT6X/rmI1vRZsxW6jHl5ByngISmk8eMjRI/sJmYH5Vr1mmAyN1XPt45OMG0dewGClHy+Q6J/1zNG6tZ7Z1Kq/nWK9rvEFB/9HpVqd+JovFpFKU7BZeXuor3SBybMAFqJUT85EURDmXseRuGWngYZGj6c9/l2HIfpK3pJtstnTwASV8hRHInrzNLDsQzEHPeuydBlP+NV/jGDE6RDWjTr//q9Uv7jvEkGXlx8Jxqe97YkIgnzL5LqGlg1mtjNmXQ0vVAgJCL7fN9x5/yKHJ8gaOUw2D5IIWnFvkzF9HLZ/5R/77he7kaIXx+6/HHMoxgIhtapaIk7BHKsXyi38i1zF+99sSr3PG0a72nguQvT7KrTF2gv+fRA0eMfdVKKGjFhuQdnzBv41FcEx37miQjimsaC0qv/H+Zjfrg2aq1CZjIpXxjwUvdqY7BQcKQ/MBGAeOwjnGqekWAQ5zYKmUCOCEbWnOPvlUycyGbCRFtUTvOf";

const POD_UID: &str = "pod-uid-1";

fn b64d(s: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD.decode(s).unwrap()
}

/// sha256(uid), left-padded to `len` (same integer; RustCrypto's prehash API
/// wants at least half the field size, Go's ecdsa.SignASN1 does not).
fn digest(uid: &str, len: usize) -> Vec<u8> {
    let d = Sha256::digest(uid.as_bytes());
    let mut out = vec![0u8; len - d.len()];
    out.extend_from_slice(&d);
    out
}

/// (spki DER, PoP signature over `uid`)
fn ed25519_key(uid: &str) -> (Vec<u8>, Vec<u8>) {
    use ed25519_dalek::Signer;
    let sk = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]);
    let spki = sk.verifying_key().to_public_key_der().unwrap().to_vec();
    (spki, sk.sign(uid.as_bytes()).to_bytes().to_vec())
}

fn p256_key(uid: &str) -> (Vec<u8>, Vec<u8>) {
    use p256::pkcs8::EncodePublicKey;
    let sk = p256::ecdsa::SigningKey::from_slice(&[1u8; 32]).unwrap();
    let spki = sk.verifying_key().to_public_key_der().unwrap().to_vec();
    let sig: p256::ecdsa::Signature = sk.sign_prehash(&digest(uid, 32)).unwrap();
    (spki, sig.to_der().as_bytes().to_vec())
}

fn p384_key(uid: &str) -> (Vec<u8>, Vec<u8>) {
    use p384::pkcs8::EncodePublicKey;
    let sk = p384::ecdsa::SigningKey::from_slice(&[1u8; 48]).unwrap();
    let spki = sk.verifying_key().to_public_key_der().unwrap().to_vec();
    let sig: p384::ecdsa::Signature = sk.sign_prehash(&digest(uid, 48)).unwrap();
    (spki, sig.to_der().as_bytes().to_vec())
}

fn p521_key(uid: &str) -> (Vec<u8>, Vec<u8>) {
    use p521::pkcs8::EncodePublicKey;
    let sk = p521::ecdsa::SigningKey::from_slice(&[1u8; 66]).unwrap();
    let spki = p521::SecretKey::from_slice(&[1u8; 66])
        .unwrap()
        .public_key()
        .to_public_key_der()
        .unwrap()
        .to_vec();
    let sig: p521::ecdsa::Signature = sk.sign_prehash(&digest(uid, 66)).unwrap();
    (spki, sig.to_der().as_bytes().to_vec())
}

fn pcr(pkix: Vec<u8>, pop: Vec<u8>) -> PodCertificateRequest {
    PodCertificateRequest {
        metadata: serde_json::from_value(serde_json::json!({
            "name": "foo", "namespace": "ns", "resourceVersion": "1", "uid": "u"
        }))
        .unwrap(),
        spec: PodCertificateRequestSpec {
            signer_name: "foo.com/signer".into(),
            pod_name: "pod-1".into(),
            pod_uid: POD_UID.into(),
            service_account_name: "sa".into(),
            service_account_uid: "sa-uid".into(),
            node_name: "node-1".into(),
            node_uid: "node-uid".into(),
            max_expiration_seconds: Some(86400),
            pkix_public_key: pkix,
            proof_of_possession: pop,
            unverified_user_annotations: None,
        },
        ..Default::default()
    }
}

fn valid() -> PodCertificateRequest {
    let (k, s) = ed25519_key(POD_UID);
    pcr(k, s)
}

fn msgs(errs: &[rusternetes_common::validation::field::Error]) -> Vec<String> {
    errs.iter().map(|e| e.to_string()).collect()
}

fn assert_has(errs: &[rusternetes_common::validation::field::Error], needle: &str) {
    let m = msgs(errs);
    assert!(
        m.iter().any(|e| e.contains(needle)),
        "want an error containing {needle:?}, got {m:?}"
    );
}

// ---------------------------------------------------------------- create

#[test]
fn create_valid_for_every_key_type() {
    for (name, (k, s)) in [
        ("ed25519", ed25519_key(POD_UID)),
        ("p256", p256_key(POD_UID)),
        ("p384", p384_key(POD_UID)),
        ("p521", p521_key(POD_UID)),
        (
            "rsa3072-max-salt",
            (b64d(RSA3072_PUB), b64d(RSA3072_SIG_MAX_SALT)),
        ),
        (
            "rsa3072-hash-salt",
            (b64d(RSA3072_PUB), b64d(RSA3072_SIG_HASH_SALT)),
        ),
    ] {
        let errs = validate_pod_certificate_request_create(&pcr(k, s));
        assert!(errs.is_empty(), "{name}: {:?}", msgs(&errs));
    }
}

#[test]
fn create_bad_proof_of_possession_for_every_key_type() {
    for (name, (k, s)) in [
        ("ed25519", ed25519_key("some-other-uid")),
        ("p256", p256_key("some-other-uid")),
        ("p384", p384_key("some-other-uid")),
        ("p521", p521_key("some-other-uid")),
        ("rsa3072", (b64d(RSA3072_PUB), vec![0u8; 384])),
    ] {
        let errs = validate_pod_certificate_request_create(&pcr(k, s));
        assert_eq!(errs.len(), 1, "{name}: {:?}", msgs(&errs));
        assert_has(&errs, "spec.proofOfPossession");
        assert_has(&errs, "could not verify proof-of-possession signature");
    }
}

#[test]
fn create_rsa_2048_is_rejected() {
    let errs = validate_pod_certificate_request_create(&pcr(b64d(RSA2048_PUB), vec![0; 256]));
    assert_has(&errs, "spec.pkixPublicKey");
    assert_has(&errs, "RSA keys must have modulus size 3072 or 4096");
}

#[test]
fn create_garbage_public_key_is_rejected() {
    let errs = validate_pod_certificate_request_create(&pcr(vec![1, 2, 3], vec![]));
    assert_has(&errs, "must be a valid PKIX-serialized public key");
}

#[test]
fn create_x25519_key_is_an_unknown_type() {
    // SPKI: SEQ{ SEQ{ OID 1.3.101.110 }, BITSTRING(32 bytes) }
    let mut spki = vec![
        0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x6e, 0x03, 0x21, 0x00,
    ];
    spki.extend([9u8; 32]);
    let errs = validate_pod_certificate_request_create(&pcr(spki, vec![]));
    assert_has(
        &errs,
        "unknown public key type; supported types are Ed25519, ECDSA, and RSA",
    );
}

#[test]
fn create_max_expiration_seconds_required() {
    let mut r = valid();
    r.spec.max_expiration_seconds = None;
    let errs = validate_pod_certificate_request_create(&r);
    assert_has(
        &errs,
        "spec.maxExpirationSeconds: Required value: must be set",
    );
}

#[test]
fn create_max_expiration_range_depends_on_signer() {
    // Kubernetes signers: [3600, 86400].
    let mut r = valid();
    r.spec.signer_name = "kubernetes.io/foo".into();
    r.spec.max_expiration_seconds = Some(86401);
    assert_has(
        &validate_pod_certificate_request_create(&r),
        "must be in the range [3600, 86400]",
    );
    r.spec.max_expiration_seconds = Some(3599);
    assert_has(
        &validate_pod_certificate_request_create(&r),
        "must be in the range [3600, 86400]",
    );
    // Other signers: [3600, 91 days].
    let mut r = valid();
    r.spec.max_expiration_seconds = Some(91 * 24 * 3600);
    assert!(validate_pod_certificate_request_create(&r).is_empty());
    r.spec.max_expiration_seconds = Some(91 * 24 * 3600 + 1);
    assert_has(
        &validate_pod_certificate_request_create(&r),
        "must be in the range [3600, 7862400]",
    );
}

#[test]
fn create_required_identity_fields() {
    let mut r = valid();
    r.spec.pod_uid.clear();
    r.spec.service_account_uid.clear();
    r.spec.node_uid.clear();
    let errs = validate_pod_certificate_request_create(&r);
    assert_has(&errs, "spec.podUID: Invalid value: \"\": must not be empty");
    assert_has(&errs, "spec.serviceAccountUID");
    assert_has(&errs, "spec.nodeUID");

    let mut r = valid();
    r.spec.pod_name = "Not_Valid".into();
    r.spec.service_account_name = "Not_Valid".into();
    r.spec.node_name = "Not_Valid".into();
    let errs = validate_pod_certificate_request_create(&r);
    assert_has(&errs, "spec.podName");
    assert_has(&errs, "spec.serviceAccountName");
    assert_has(&errs, "spec.nodeName");

    let mut r = valid();
    r.spec.pod_uid = "x".repeat(129);
    assert_has(
        &validate_pod_certificate_request_create(&r),
        "spec.podUID: Too long",
    );
}

#[test]
fn create_invalid_signer_name() {
    let mut r = valid();
    r.spec.signer_name = "no-slash".into();
    assert_has(
        &validate_pod_certificate_request_create(&r),
        "spec.signerName",
    );
}

#[test]
fn create_unverified_user_annotations_need_domain_prefixed_keys() {
    let mut r = valid();
    r.spec.unverified_user_annotations =
        Some([("acme.io/foo".to_string(), "v".to_string())].into());
    assert!(validate_pod_certificate_request_create(&r).is_empty());
    r.spec.unverified_user_annotations = Some([("nodomain".to_string(), "v".to_string())].into());
    assert_has(
        &validate_pod_certificate_request_create(&r),
        "spec.unverifiedUserAnnotations",
    );
}

#[test]
fn create_oversized_key_and_pop() {
    let r = pcr(vec![0; MAX_PKIX_PUBLIC_KEY_SIZE + 1], vec![]);
    assert_has(
        &validate_pod_certificate_request_create(&r),
        "spec.pkixPublicKey: Too long",
    );
    let (k, _) = ed25519_key(POD_UID);
    let r = pcr(k, vec![0; MAX_PROOF_OF_POSSESSION_SIZE + 1]);
    assert_has(
        &validate_pod_certificate_request_create(&r),
        "spec.proofOfPossession: Too long",
    );
}

// ---------------------------------------------------------------- update

#[test]
fn update_spec_is_immutable_metadata_is_not() {
    let old = valid();
    let mut new = old.clone();
    new.metadata.labels = Some([("a".to_string(), "b".to_string())].into());
    assert!(validate_pod_certificate_request_update(&new, &old).is_empty());

    new.spec.node_name = "other".into();
    let errs = validate_pod_certificate_request_update(&new, &old);
    assert_has(&errs, "spec: Invalid value");
    assert_has(&errs, "field is immutable");
}

// ---------------------------------------------------------- status update

fn t(h: u32, m: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 1, h, m, s).unwrap()
}

fn now() -> DateTime<Utc> {
    t(0, 1, 0)
}

fn cond(ty: &str, status: &str) -> Condition {
    Condition {
        condition_type: ty.to_string(),
        status: status.to_string(),
        observed_generation: None,
        last_transition_time: Some(t(0, 0, 0)),
        reason: Some("Because".into()),
        message: None,
    }
}

fn with_conds(mut r: PodCertificateRequest, c: Vec<Condition>) -> PodCertificateRequest {
    r.status.conditions = c;
    r
}

#[test]
fn status_denied_and_failed_transitions() {
    for ty in [CONDITION_TYPE_DENIED, CONDITION_TYPE_FAILED] {
        let old = valid();
        let new = with_conds(old.clone(), vec![cond(ty, "True")]);
        assert!(
            validate_pod_certificate_request_status_update(&new, &old, now()).is_empty(),
            "{ty}"
        );
        // Other status fields must stay empty.
        let mut bad = new.clone();
        bad.status.certificate_chain = "x".into();
        assert_has(
            &validate_pod_certificate_request_status_update(&bad, &old, now()),
            "non-condition status fields must be empty when denying or failing",
        );
    }
}

#[test]
fn status_condition_rules() {
    let old = valid();
    let new = with_conds(old.clone(), vec![cond("Bogus", "True")]);
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "status.conditions[0].type: Unsupported value: \"Bogus\"",
    );
    let new = with_conds(old.clone(), vec![cond(CONDITION_TYPE_DENIED, "False")]);
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "status.conditions[0].status: Unsupported value: \"False\"",
    );
    let new = with_conds(
        old.clone(),
        vec![
            cond(CONDITION_TYPE_DENIED, "True"),
            cond(CONDITION_TYPE_FAILED, "True"),
        ],
    );
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "There may be at most one condition with type \"Issued\", \"Denied\", or \"Failed\"",
    );
}

#[test]
fn status_terminal_old_object_is_immutable() {
    let old = with_conds(valid(), vec![cond(CONDITION_TYPE_DENIED, "True")]);
    let mut new = old.clone();
    new.status.certificate_chain = "x".into();
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "immutable after PodCertificateRequest is issued, denied, or failed",
    );
    assert!(validate_pod_certificate_request_status_update(&old, &old, now()).is_empty());
}

#[test]
fn status_non_terminal_change_is_rejected() {
    let old = valid();
    let mut new = old.clone();
    new.status.certificate_chain = "x".into();
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "status is immutable unless transitioning to \"Issued\", \"Denied\", or \"Failed\"",
    );
}

/// Issues a leaf for `spki` valid [nb, na) signed by a throwaway CA. `extra`
/// customises the leaf params.
fn issue(
    spki: &[u8],
    nb: time::OffsetDateTime,
    na: time::OffsetDateTime,
    extra: impl FnOnce(&mut CertificateParams),
) -> String {
    let ca_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    let ca = CertificateParams::new(vec![])
        .unwrap()
        .self_signed(&ca_key)
        .unwrap();
    let mut params = CertificateParams::new(vec!["leaf.example.com".to_string()]).unwrap();
    params.not_before = nb;
    params.not_after = na;
    extra(&mut params);
    let leaf = params
        .signed_by(&SubjectPublicKeyInfo::from_der(spki).unwrap(), &ca, &ca_key)
        .unwrap();
    format!("{}{}", leaf.pem(), ca.pem())
}

fn ymdh(d: u8, h: u8) -> time::OffsetDateTime {
    time::OffsetDateTime::new_utc(
        time::Date::from_calendar_date(2030, time::Month::January, d).unwrap(),
        time::Time::from_hms(h, 0, 0).unwrap(),
    )
}

fn issued(
    spki: &[u8],
    nb: time::OffsetDateTime,
    na: time::OffsetDateTime,
) -> PodCertificateRequest {
    let (k, s) = ed25519_key(POD_UID);
    let old = pcr(k, s);
    let mut new = old.clone();
    new.status.conditions = vec![cond(CONDITION_TYPE_ISSUED, "True")];
    new.status.certificate_chain = issue(spki, nb, na, |_| {});
    new.status.not_before = DateTime::from_timestamp(nb.unix_timestamp(), 0);
    new.status.not_after = DateTime::from_timestamp(na.unix_timestamp(), 0);
    new.status.begin_refresh_at = new
        .status
        .not_before
        .map(|t| t + chrono::Duration::hours(2));
    new
}

fn spki_of(r: &PodCertificateRequest) -> Vec<u8> {
    r.spec.pkix_public_key.clone()
}

#[test]
fn status_issued_happy_path() {
    let old = valid();
    let new = issued(&spki_of(&old), ymdh(1, 0), ymdh(2, 0));
    let errs = validate_pod_certificate_request_status_update(&new, &old, now());
    assert!(errs.is_empty(), "{:?}", msgs(&errs));
}

#[test]
fn status_issued_chain_problems() {
    let old = valid();
    let mut new = issued(&spki_of(&old), ymdh(1, 0), ymdh(2, 0));
    new.status.certificate_chain = String::new();
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "issued certificate chain must contain at least one certificate",
    );
    let mut new = issued(&spki_of(&old), ymdh(1, 0), ymdh(2, 0));
    new.status.certificate_chain =
        "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n".into();
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "issued certificate chain must consist entirely of CERTIFICATE PEM blocks",
    );
    let mut new = issued(&spki_of(&old), ymdh(1, 0), ymdh(2, 0));
    new.status.certificate_chain =
        "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n".into();
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "leaf certificate does not parse as valid X.509",
    );
}

#[test]
fn status_issued_to_a_different_key_is_rejected() {
    let old = valid();
    let (other, _) = p256_key(POD_UID);
    let new = issued(&other, ymdh(1, 0), ymdh(2, 0));
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "leaf certificate was not issued to the requested public key",
    );
}

#[test]
fn status_issued_timestamps() {
    let old = valid();
    let base = issued(&spki_of(&old), ymdh(1, 0), ymdh(2, 0));

    let mut new = base.clone();
    new.status.not_before = None;
    new.status.not_after = None;
    new.status.begin_refresh_at = None;
    let errs = validate_pod_certificate_request_status_update(&new, &old, now());
    assert_has(&errs, "status.notBefore: Required value");
    assert_has(&errs, "status.notAfter: Required value");
    assert_has(&errs, "status.beginRefreshAt: Required value");

    let mut new = base.clone();
    new.status.not_before = Some(t(0, 0, 1));
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "must be set to the NotBefore time encoded in the leaf certificate",
    );
    let mut new = base.clone();
    new.status.not_after = Some(t(5, 0, 0));
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "must be set to the NotAfter time encoded in the leaf certificate",
    );
    // Clock far from notBefore.
    assert_has(
        &validate_pod_certificate_request_status_update(&base, &old, t(1, 0, 0)),
        "must be set to within 5 minutes of kube-apiserver's current time",
    );
    // beginRefreshAt too early / too late.
    let mut new = base.clone();
    new.status.begin_refresh_at = Some(t(0, 5, 0));
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "must be at least 10 minutes after status.notBefore",
    );
    let mut new = base.clone();
    new.status.begin_refresh_at = Some(Utc.with_ymd_and_hms(2030, 1, 1, 23, 55, 0).unwrap());
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "must be at least 10 minutes before status.notAfter",
    );
}

#[test]
fn status_issued_lifetime_limits() {
    let old = valid();
    // 24h lifetime vs maxExpirationSeconds 3600.
    let mut new = issued(&spki_of(&old), ymdh(1, 0), ymdh(2, 0));
    new.spec.max_expiration_seconds = Some(3600);
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "leaf certificate lifetime must be <= spec.maxExpirationSeconds (3600)",
    );
    // 30 minute lifetime.
    let nb = ymdh(1, 0);
    let na = nb + time::Duration::minutes(30);
    let mut new = issued(&spki_of(&old), nb, na);
    new.status.begin_refresh_at = Some(t(0, 11, 0));
    assert_has(
        &validate_pod_certificate_request_status_update(&new, &old, now()),
        "leaf certificate lifetime must be >= 1 hour",
    );
}
