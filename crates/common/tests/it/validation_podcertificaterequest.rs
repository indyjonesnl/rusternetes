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

const P224_PUB: &str = "ME4wEAYHKoZIzj0CAQYFK4EEACEDOgAEykIWJVkEkcGIKH1bTGXRxtB/R8MYVP673BeGggffCcavvTWyYou0J0fwkvqTFCSbWGlV1NWImiI=";

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

// -------------------------------------- Go-parity: PSS salt / mail.ParseAddress

const RSA3072_PUB_GO: &str = "MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEA0j1CcC7KwDnOhN0vnHLaWgQJQOCJnwNnqWve9db3fgrg7vTil7RfF0kuAxIRUl0DsTgbHWsrRh92YO9eLoIboeomfK9SHgsDKIhCrizapAzKeBizcSPT0rgnUyIfKlomJ6K/wX8wp76rYOgJ7MpOcvHFb+W9y6d0fpVuMkEnSfUdWPynH2AND3btUeyr3P0Welwl8jQAJrdbSrjsU6+Af38Kr5zjEHz0CcbmES7Ro0dddSKzJHyl3/hKQQA0S4e4jFiNAWxZsHUUqwKZ9d5skhnwTmCs8ph01HSz7YY1TfY2bRquQV2HAudvWHNRRP71YQy66ORnj3dtR/vj3cpW3Rqn20PyruEv1Upqvwc5MMCa/SXpjwJtRSPz5K70jmStwFv4EqXAykOmssAotbH2R3gqslCPOfPd2oBe4bdrBLo0n7ad6oSDrcJ2H7ahvh20+1Y6QFyOdf02jnvKC7kWTZq23WiR4NjBFfXJ0XigtLWCgATerGVSNS9keeFQCgc3AgMBAAE=";
/// Generated with Go `rsa.SignPSS(.., &PSSOptions{SaltLength: n})` over
/// sha256("pod-uid-1"); `rsa.VerifyPSS(.., nil)` auto-detects each of them.
const RSA3072_GO_SIG_SALT_0: &str = "CrIiaT20FqQRxz7OsALSmL9ZepyB7KIWb99j7OBXJ/ZblBr4JFJ71Fm9w159RYcOCfG3gZAJz2Qz5n/1CIYaor8uEYn+H/+iaeYlFpzu5V1UWCtFPGx6a6wCk5684EONpRQcQUmwPJ+B8P7VPkVTf3SyWNcF8R64tGstZ8RSXd4ue/Ph7xB/pWdZNzXFDLiYsQ6sL+54AMAlREn9J+DV5ceuDM/oHFQ9Uu6Ql0aYOohi7fwY981LXVJC64M9MuCJ+6lK8YDbQP7DDKMkvV6xTwF76I4qph2ht1CgnZdHPOq+MxgNe0nEnDI14X2cm0MgnUV4UBmivMcjSVdmbgWR0zbgVFdXRN57nt3AB7RL+68sw+kifkpVOMNSUGJBpE54myRdgqIARpFKN8s1Q1wZIMTDeK1N29npTjGfcsZ79YbCUjN+T4JdjQQwQNb0ToyPK8eZvN2VevIciUs5FWjY3Cg+79tDetzdYQCr0pNq8RKehGUNIrp1LSrU6sVx2hYH";
const RSA3072_GO_SIG_SALT_20: &str = "N5iwSUjO0avaFo63yZtA60ZD5r+lJFRN+q4F9l2KETa9glzK9A7Ki5uC5OlELXevQZBJhJOc2STw/xlhrpioFJk8JCye8fl3aTXqwY/+ahkna98W5kAVDoi3P1yrHEVOXheND+wDdMgS1I/HPkRVV+HWnaJQf0gCGTQQuREbWPoFQhSAQVAiZ5IaHgdpiqA3bM8a1B+qwBhhrZOFA/M6eEtT7jcION8sJovEzpdJBhRLd6N0bkxFTAkPFL6jTLx9y/EG9iqKEyofGS9BW7WZUm4jeVsv9Nvg92p+BK0db8VJjzrx6xVmElOvy6NoyrKKC1yVGJSMTcN+clnfCEMaLqLnzY6HfcbVzI0iazrzY6Zp19OVCBa42pSPZp6eNktjdIH8/HYoze0h9MYKw9hd6Hj23VmEIk53nd0G0A3lh+LhSq3C48L2Gi5N6vBft53Hm1QjbKihV6tIQkMxB63cGsdf8IN0G11idNIyUsI4dke6V/2Z8CBHqgCkQA6oNlXv";
const RSA3072_GO_SIG_SALT_48: &str = "Z3ClY3oATI7ShUc1gg+49Hszh0CtxCzXGpfATC2weV1fUQMZiF8IAUOJ+9QJwKi2upzsb2jSPD7zsHi2CDcgsSI13guhlJhsLRenvPIOM3h+8poRmF8jooUw+HhTkY4GeNax0WWM9W6kYyodbYEWK5JroGwcNS5GFaw26rMjplySY0rEHyNoWA8BgNGwTU7M4MLGrIp4bBZrCLIUVuZFm/BGnDgVJR1HB8+Zp66hgDbpIYuyhRwS3n9gLCvVg+/v66d40n/iwmlu/ObBoQ/xP7md7QmHD7TKmt4X9rK6KA4iprWeAr+vsF8OvaokI8oxr0Oxx5ThdNOJeMpmG1FkPLLUIojLt7zK/csurdpaBUn6gdQYMHQz5CINAXXSuQKzpKANuw7fsk/xs7F2BqO1lZzEBGedCKmhWBDrjMZDnHw0uhycMLAoBztH0tRoVTBc211BbTIQZffBxJU8wI4/lhs9uyQ17dvWNQDvv+fgm93FVTaSfM8yMXQ93tH08RAJ";

/// `rsa.VerifyPSS(.., nil)` auto-detects the salt length
/// (crypto/internal/fips140/rsa/pkcs1v22.go: `pssSaltLengthAutodetect`).
#[test]
fn create_rsa_pss_any_salt_length_is_accepted() {
    for (name, sig) in [
        ("salt0", RSA3072_GO_SIG_SALT_0),
        ("salt20", RSA3072_GO_SIG_SALT_20),
        ("salt48", RSA3072_GO_SIG_SALT_48),
    ] {
        let errs = validate_pod_certificate_request_create(&pcr(b64d(RSA3072_PUB_GO), b64d(sig)));
        assert!(errs.is_empty(), "{name}: {:?}", msgs(&errs));
    }
}

#[test]
fn create_rsa_pss_corrupted_signature_is_rejected() {
    for sig in [RSA3072_GO_SIG_SALT_0, RSA3072_GO_SIG_SALT_20] {
        let mut sig = b64d(sig);
        sig[100] ^= 1;
        let errs = validate_pod_certificate_request_create(&pcr(b64d(RSA3072_PUB_GO), sig));
        assert_has(&errs, "could not verify proof-of-possession signature");
    }
    // Wrong length is ErrVerification before any maths.
    let mut sig = b64d(RSA3072_GO_SIG_SALT_20);
    sig.pop();
    let errs = validate_pod_certificate_request_create(&pcr(b64d(RSA3072_PUB_GO), sig));
    assert_has(&errs, "could not verify proof-of-possession signature");
}

fn status_errs_with_email(email: &str) -> Vec<String> {
    let old = valid();
    let mut new = issued(&spki_of(&old), ymdh(1, 0), ymdh(2, 0));
    let email = email.to_string();
    new.status.certificate_chain = issue(&spki_of(&old), ymdh(1, 0), ymdh(2, 0), |p| {
        p.subject_alt_names
            .push(rcgen::SanType::Rfc822Name(email.try_into().unwrap()));
    });
    msgs(&validate_pod_certificate_request_status_update(
        &new,
        &old,
        now(),
    ))
}

/// Oracle: output of Go 1.25 `mail.ParseAddress(s)` (err == nil) for each
/// input, run against /usr/local/go/src/net/mail/message.go.
#[test]
fn status_issued_email_matches_go_mail_parse_address() {
    const MSG: &str = "leaf certificate should not contain invalid EmailAddress";
    let table: &[(&str, bool)] = &[
        ("joe@example.com", true),
        ("Joe Q <joe@example.com>", true),
        ("<joe@example.com>", true),
        ("\"joe smith\"@example.com", true),
        ("joe@example.com (Joe)", true),
        ("(c) joe@example.com", false),
        ("joe@[1.2.3.4]", true),
        ("joe@[::1]", true),
        ("j.o.e@example.com", true),
        ("=?utf-8?q?Joe?= <joe@example.com>", true),
        ("joe@example", true),
        ("email@@address", false),
        ("joe", false),
        ("joe@", false),
        ("@example.com", false),
        ("joe@[bogus]", false),
        ("joe@[1.2.3.4", false),
        ("a..b@example.com", false),
        (".a@example.com", false),
        ("a.@example.com", false),
        ("Joe Q joe@example.com", false),
        ("joe@example.com extra", false),
        ("\"\"@example.com", false),
        ("joe@example.com (unclosed", false),
        ("<joe@example.com", false),
        ("a@b, c@d", false),
        ("=?x-unknown?q?Joe?= (=?x-unknown?q?x?=) a@b", false),
        ("=?x-unknown?q?Joe?= <a@b>", false),
        ("Joe =?x-unknown?q?Joe?= <a@b>", true),
        ("joe@example.com (=?x-unknown?q?x?=)", false),
        ("joe@example.com (=?utf-8?q?x?=)", true),
        ("g: a@b;", true),
        ("g:;", false),
        ("g: a@b, c@d;", false),
        ("a@b (c (nested) d)", true),
        ("joe@[01.2.3.4]", false),
        ("joe@[1.2.3.4] (x", false),
        ("\"a\\\"b\"@x.y", true),
        ("Joe <joe@example.com> x", false),
        ("joe @ example.com", false),
        (" joe@example.com ", true),
        ("=?utf-8?b?Sm9l?= <a@b>", true),
        ("=?utf-8?b?Sm9?= <a@b>", true),
        ("joe@exa mple.com", false),
        ("a@b.", false),
        ("a@.b", false),
        ("a@b..c", false),
        ("<>", false),
        ("<a@b>c@d", false),
    ];
    for (addr, ok) in table {
        let m = status_errs_with_email(addr);
        let bad = m.iter().any(|e| e.contains(MSG));
        assert_eq!(!bad, *ok, "{addr:?}: Go accepts={ok}, errors: {m:?}");
    }
}

// ------------------------------------------- exact upstream error lists

const RFC1123_SUBDOMAIN: &str = "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')";

/// `TestValidatePodCertificateRequestCreate` compares the WHOLE error list
/// (`cmp.Diff(gotErrors, tc.wantErrors)`), so each case below pins the exact
/// type, path, value and detail of every error, not a substring.
#[test]
fn create_exact_error_lists_match_upstream_table() {
    type Mutate = fn(&mut PodCertificateRequest);
    let cases: Vec<(&str, Mutate, Vec<String>)> = vec![
        ("valid Ed25519 PCR", |_| {}, vec![]),
        (
            // Go parses P-224 (x509.ParsePKIXPublicKey) and then rejects the
            // curve; it is not a malformed key.
            "invalid ECDSA P224 PCR",
            |r| {
                r.spec.pkix_public_key = b64d(P224_PUB);
                r.spec.proof_of_possession = vec![];
            },
            vec!["spec.pkixPublicKey: Invalid value: \"curve P-224\": elliptic public keys must use curve P256 or P384".into()],
        ),
        (
            "bad signer name",
            |r| r.spec.signer_name = "not-valid-signername".into(),
            vec!["spec.signerName: Invalid value: \"not-valid-signername\": must be a fully qualified domain and path of the form 'example.com/signer-name'".into()],
        ),
        (
            "bad pod name",
            |r| r.spec.pod_name = "pod-1-bad!!!!!".into(),
            vec![format!("spec.podName: Invalid value: \"pod-1-bad!!!!!\": {RFC1123_SUBDOMAIN}")],
        ),
        (
            "bad pod uid",
            |r| {
                // Upstream signs the empty UID (pubPKIXEmpty, proofEmpty), so
                // the proof of possession verifies and only the UID errs.
                let (k, s) = ed25519_key("");
                r.spec.pod_uid = String::new();
                r.spec.pkix_public_key = k;
                r.spec.proof_of_possession = s;
            },
            vec!["spec.podUID: Invalid value: \"\": must not be empty".into()],
        ),
        (
            "bad service account name",
            |r| r.spec.service_account_name = "sa-1-bad!!!!!".into(),
            vec![format!("spec.serviceAccountName: Invalid value: \"sa-1-bad!!!!!\": {RFC1123_SUBDOMAIN}")],
        ),
        (
            "bad service account uid",
            |r| r.spec.service_account_uid = String::new(),
            vec!["spec.serviceAccountUID: Invalid value: \"\": must not be empty".into()],
        ),
        (
            "bad node name",
            |r| r.spec.node_name = "node-1-bad!!!!!".into(),
            vec![format!("spec.nodeName: Invalid value: \"node-1-bad!!!!!\": {RFC1123_SUBDOMAIN}")],
        ),
        (
            "bad node uid",
            |r| r.spec.node_uid = String::new(),
            vec!["spec.nodeUID: Invalid value: \"\": must not be empty".into()],
        ),
        (
            "maxExpirationSeconds missing",
            |r| r.spec.max_expiration_seconds = None,
            vec!["spec.maxExpirationSeconds: Required value: must be set".into()],
        ),
        (
            "maxExpirationSeconds too large",
            |r| r.spec.max_expiration_seconds = Some(91 * 86400 + 1),
            vec!["spec.maxExpirationSeconds: Invalid value: 7862401: must be in the range [3600, 7862400]".into()],
        ),
        (
            "maxExpirationSeconds too large (Kubernetes signer)",
            |r| {
                r.spec.signer_name = "kubernetes.io/foo".into();
                r.spec.max_expiration_seconds = Some(86401);
            },
            vec!["spec.maxExpirationSeconds: Invalid value: 86401: must be in the range [3600, 86400]".into()],
        ),
        (
            "maxExpirationSeconds too small",
            |r| r.spec.max_expiration_seconds = Some(3600 - 1),
            vec!["spec.maxExpirationSeconds: Invalid value: 3599: must be in the range [3600, 7862400]".into()],
        ),
        (
            "bad user annotations key name",
            |r| {
                r.spec.unverified_user_annotations =
                    Some([("test/domain/foo".to_string(), "bar".to_string())].into())
            },
            vec!["spec.unverifiedUserAnnotations: Invalid value: \"test/domain/foo\": a valid label key must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]') with an optional DNS subdomain prefix and '/' (e.g. 'example.com/MyName')".into()],
        ),
        (
            "bad user annotations key prefix too long",
            |r| {
                r.spec.unverified_user_annotations =
                    Some([(format!("{}/foo", "a".repeat(254)), "bar".to_string())].into())
            },
            vec![format!(
                "spec.unverifiedUserAnnotations: Invalid value: \"{}/foo\": prefix part must be no more than 253 bytes",
                "a".repeat(254)
            )],
        ),
    ];
    for (name, mutate, want) in cases {
        let mut r = valid();
        mutate(&mut r);
        let got = msgs(&validate_pod_certificate_request_create(&r));
        assert_eq!(got, want, "{name}");
    }
}

// ------------------------- exact upstream status-update error lists

/// `TestValidatePodCertificateRequestStatusUpdate` compares the WHOLE error
/// list. These are the cases that need no issued certificate (conditions
/// only), pinned to the exact type, path, value and detail.
#[test]
fn status_update_exact_error_lists_match_upstream_table() {
    const AT_MOST_ONE: &str =
        "There may be at most one condition with type \"Issued\", \"Denied\", or \"Failed\"";
    const IMMUTABLE: &str = "immutable after PodCertificateRequest is issued, denied, or failed";
    let denied = || vec![cond(CONDITION_TYPE_DENIED, "True")];
    let failed = || vec![cond(CONDITION_TYPE_FAILED, "True")];
    type Case = (&'static str, Vec<Condition>, Vec<Condition>, Vec<String>);
    let cases: Vec<Case> = vec![
        ("changing nothing is allowed", vec![], vec![], vec![]),
        (
            "adding unknown condition types is not allowed",
            vec![],
            vec![cond("Unknown", "True")],
            vec!["status.conditions[0].type: Unsupported value: \"Unknown\": supported values: \"Issued\", \"Denied\", \"Failed\"".into()],
        ),
        (
            "Issued must have status True",
            vec![],
            vec![cond(CONDITION_TYPE_ISSUED, "False")],
            vec!["status.conditions[0].status: Unsupported value: \"False\": supported values: \"True\"".into()],
        ),
        (
            "Denied must have status True",
            vec![],
            vec![cond(CONDITION_TYPE_DENIED, "False")],
            vec!["status.conditions[0].status: Unsupported value: \"False\": supported values: \"True\"".into()],
        ),
        (
            "Failed must have status True",
            vec![],
            vec![cond(CONDITION_TYPE_FAILED, "False")],
            vec!["status.conditions[0].status: Unsupported value: \"False\": supported values: \"True\"".into()],
        ),
        ("transitioning to Denied is allowed", vec![], denied(), vec![]),
        ("transitioning to Failed is allowed", vec![], failed(), vec![]),
        (
            "a request cannot be both Denied and Failed",
            vec![],
            vec![cond(CONDITION_TYPE_DENIED, "True"), cond(CONDITION_TYPE_FAILED, "True")],
            vec![format!("status.conditions[1].type: Invalid value: \"Failed\": {AT_MOST_ONE}")],
        ),
        (
            "a request cannot change from Denied to Failed",
            denied(),
            failed(),
            vec![format!("status: Invalid value: {IMMUTABLE}")],
        ),
        (
            "a request cannot change from Failed to Denied",
            failed(),
            denied(),
            vec![format!("status: Invalid value: {IMMUTABLE}")],
        ),
        (
            "a request cannot change from Denied to pending",
            denied(),
            vec![],
            vec![format!("status: Invalid value: {IMMUTABLE}")],
        ),
        (
            "a request cannot change from Failed to pending",
            failed(),
            vec![],
            vec![format!("status: Invalid value: {IMMUTABLE}")],
        ),
    ];
    for (name, old_conds, new_conds, want) in cases {
        let old = with_conds(valid(), old_conds);
        let new = with_conds(valid(), new_conds);
        let got = msgs(&validate_pod_certificate_request_status_update(
            &new,
            &old,
            now(),
        ));
        assert_eq!(got, want, "{name}");
    }
}
