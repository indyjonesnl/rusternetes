//! Service-account tokens issued by an external control plane (#1575).
//!
//! Mirrors the cases of upstream `pkg/serviceaccount/jwt_test.go`
//! (`TestTokenGenerateAndValidate`, `TestMaxExpirationSeconds`-adjacent issuer /
//! key / audience cases, `TestTokenAuds`-style audience intersection) against
//! `TokenManager`, configured with the four kube-apiserver flags
//! `--service-account-key-file`, `--service-account-signing-key-file`,
//! `--service-account-issuer` and `--api-audiences`.

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rusternetes_common::auth::{ServiceAccountClaims, ServiceAccountOptions, TokenManager};
use rusternetes_common::sa_keys;
use serde_json::{json, Value};

const ISSUER: &str = "https://kubernetes.default.svc";

fn fixture(name: &str) -> String {
    format!("{}/tests/fixtures/sa/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn read(name: &str) -> Vec<u8> {
    std::fs::read(fixture(name)).unwrap()
}

fn kid_of(file: &str) -> String {
    sa_keys::public_keys_from_file(&fixture(file)).unwrap()[0]
        .key_id
        .clone()
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// An upstream-shaped bound token payload (`pkg/serviceaccount/claims.go`):
/// the namespace and SA uid live only in `kubernetes.io`, and a single
/// audience is a bare string (go-jose marshals it that way).
fn upstream_claims(iss: &str, aud: Value) -> Value {
    json!({
        "iss": iss,
        "sub": "system:serviceaccount:ns1:sa1",
        "aud": aud,
        "iat": now(),
        "nbf": now(),
        "exp": now() + 3600,
        "kubernetes.io": {
            "namespace": "ns1",
            "serviceaccount": {"name": "sa1", "uid": "sa-uid-1"},
            "pod": {"name": "pod1", "uid": "pod-uid-1"},
        }
    })
}

fn sign(alg: Algorithm, key: EncodingKey, kid: Option<String>, claims: &Value) -> String {
    let mut h = Header::new(alg);
    h.kid = kid;
    encode(&h, claims, &key).unwrap()
}

fn rsa_key() -> EncodingKey {
    EncodingKey::from_rsa_pem(&read("rsa-pkcs1.key")).unwrap()
}

fn opts(key_files: &[&str]) -> ServiceAccountOptions {
    ServiceAccountOptions {
        key_files: key_files.iter().map(|f| fixture(f)).collect(),
        signing_key_file: Some(fixture("rsa-pkcs1.key")),
        issuers: vec![ISSUER.to_string()],
        api_audiences: vec![],
        ..Default::default()
    }
}

fn manager(o: &ServiceAccountOptions) -> TokenManager {
    TokenManager::new(b"unused")
        .with_service_account_options(o)
        .unwrap()
}

#[test]
fn external_rsa_token_validates_and_fills_nested_claims() {
    let tm = manager(&opts(&["rsa.pub"]));
    let token = sign(
        Algorithm::RS256,
        rsa_key(),
        Some(kid_of("rsa.pub")),
        &upstream_claims(ISSUER, json!(ISSUER)),
    );
    let (claims, auds) = tm.authenticate_token(&token, None).unwrap();
    // namespace / uid only exist inside `kubernetes.io` in upstream tokens
    assert_eq!(claims.namespace, "ns1");
    assert_eq!(claims.uid, "sa-uid-1");
    assert_eq!(claims.aud, vec![ISSUER.to_string()]);
    assert_eq!(claims.pod_name.as_deref(), Some("pod1"));
    assert_eq!(auds, vec![ISSUER.to_string()]);
}

#[test]
fn token_without_kid_is_tried_against_every_key() {
    let tm = manager(&opts(&["rsa2.pub", "rsa.pub"]));
    let token = sign(
        Algorithm::RS256,
        rsa_key(),
        None,
        &upstream_claims(ISSUER, json!([ISSUER])),
    );
    assert!(tm.authenticate_token(&token, None).is_ok());
}

#[test]
fn token_with_unknown_kid_has_no_keys() {
    let tm = manager(&opts(&["rsa2.pub"]));
    let token = sign(
        Algorithm::RS256,
        rsa_key(),
        Some(kid_of("rsa.pub")),
        &upstream_claims(ISSUER, json!(ISSUER)),
    );
    let err = tm.authenticate_token(&token, None).unwrap_err().to_string();
    assert!(err.contains("no keys found"), "{err}");
}

#[test]
fn token_signed_by_another_key_is_rejected() {
    let tm = manager(&opts(&["rsa2.pub"]));
    let token = sign(
        Algorithm::RS256,
        rsa_key(),
        None,
        &upstream_claims(ISSUER, json!(ISSUER)),
    );
    assert!(tm.authenticate_token(&token, None).is_err());
}

#[test]
fn token_with_foreign_issuer_is_rejected() {
    let tm = manager(&opts(&["rsa.pub"]));
    let token = sign(
        Algorithm::RS256,
        rsa_key(),
        None,
        &upstream_claims("https://other.example", json!(ISSUER)),
    );
    let err = tm.authenticate_token(&token, None).unwrap_err().to_string();
    assert!(err.contains("issuer"), "{err}");
}

#[test]
fn every_configured_issuer_is_accepted() {
    let mut o = opts(&["rsa.pub"]);
    o.issuers.push("https://old-issuer.example".to_string());
    let tm = manager(&o);
    let token = sign(
        Algorithm::RS256,
        rsa_key(),
        None,
        &upstream_claims("https://old-issuer.example", json!(ISSUER)),
    );
    assert!(tm.authenticate_token(&token, None).is_ok());
}

#[test]
fn expired_token_is_rejected() {
    let tm = manager(&opts(&["rsa.pub"]));
    let mut c = upstream_claims(ISSUER, json!(ISSUER));
    c["exp"] = json!(now() - 3600);
    let token = sign(Algorithm::RS256, rsa_key(), None, &c);
    assert!(tm.authenticate_token(&token, None).is_err());
}

#[test]
fn not_yet_valid_token_is_rejected() {
    let tm = manager(&opts(&["rsa.pub"]));
    let mut c = upstream_claims(ISSUER, json!(ISSUER));
    c["nbf"] = json!(now() + 3600);
    let token = sign(Algorithm::RS256, rsa_key(), None, &c);
    assert!(tm.authenticate_token(&token, None).is_err());
}

#[test]
fn verification_key_may_be_a_certificate_or_a_private_key() {
    // rsa2.crt carries rsa2's key; rsa-pkcs8.key is a PKCS#8 private key
    for key_file in ["rsa2.crt", "rsa-pkcs8.key"] {
        let mut o = opts(&[key_file]);
        o.signing_key_file = Some(fixture("rsa2.key"));
        let tm = manager(&o);
        let signer = if key_file == "rsa2.crt" {
            EncodingKey::from_rsa_pem(&read("rsa2.key")).unwrap()
        } else {
            rsa_key()
        };
        let token = sign(
            Algorithm::RS256,
            signer,
            None,
            &upstream_claims(ISSUER, json!(ISSUER)),
        );
        assert!(tm.authenticate_token(&token, None).is_ok(), "{key_file}");
    }
}

#[test]
fn ecdsa_token_validates() {
    let mut o = opts(&["ec.pub"]);
    o.signing_key_file = Some(fixture("ec-sec1.key"));
    let tm = manager(&o);
    // ec-sec1.key is SEC1; the generator re-encodes it, so round-trip our own
    // token through an independent verifier built from the public key only.
    let mut claims = ServiceAccountClaims::new("sa1".into(), "ns1".into(), "uid".into(), 1);
    claims.aud = vec![ISSUER.to_string()];
    let token = tm.generate_token(claims).unwrap();
    let header = jsonwebtoken::decode_header(&token).unwrap();
    assert_eq!(header.alg, Algorithm::ES256);
    assert_eq!(header.kid.as_deref(), Some(kid_of("ec.pub").as_str()));
    assert!(tm.authenticate_token(&token, None).is_ok());
}

#[test]
fn generated_token_carries_configured_issuer_and_kid() {
    let tm = manager(&opts(&["rsa.pub"]));
    let claims = ServiceAccountClaims::new("sa1".into(), "ns1".into(), "uid".into(), 1);
    let token = tm.generate_token(claims).unwrap();
    let header = jsonwebtoken::decode_header(&token).unwrap();
    assert_eq!(header.alg, Algorithm::RS256);
    assert_eq!(header.kid.as_deref(), Some(kid_of("rsa.pub").as_str()));
    let validated = tm
        .authenticate_token(&token, Some(&[ISSUER.to_string()]))
        .unwrap_err();
    // The default claims audience ("rusternetes") is not an API audience.
    assert!(validated.to_string().contains("audiences"), "{validated}");

    let mut claims = ServiceAccountClaims::new("sa1".into(), "ns1".into(), "uid".into(), 1);
    claims.aud = vec![ISSUER.to_string()];
    let token = tm.generate_token(claims).unwrap();
    let (c, _) = tm.authenticate_token(&token, None).unwrap();
    assert_eq!(c.iss, ISSUER);
}

#[test]
fn api_audiences_default_to_the_issuers() {
    let tm = manager(&opts(&["rsa.pub"]));
    assert_eq!(tm.api_audiences(), [ISSUER.to_string()]);
    assert_eq!(tm.issuer(), Some(ISSUER));
}

#[test]
fn token_audience_must_intersect_api_audiences() {
    let mut o = opts(&["rsa.pub"]);
    o.api_audiences = vec!["api".to_string()];
    let tm = manager(&o);

    let ok = sign(
        Algorithm::RS256,
        rsa_key(),
        None,
        &upstream_claims(ISSUER, json!(["api", "other"])),
    );
    let (_, auds) = tm.authenticate_token(&ok, None).unwrap();
    assert_eq!(auds, vec!["api".to_string()]);

    let bad = sign(
        Algorithm::RS256,
        rsa_key(),
        None,
        &upstream_claims(ISSUER, json!("other")),
    );
    let err = tm.authenticate_token(&bad, None).unwrap_err().to_string();
    assert!(err.contains("is invalid for the target audiences"), "{err}");
}

#[test]
fn requested_audiences_narrow_the_match() {
    let mut o = opts(&["rsa.pub"]);
    o.api_audiences = vec!["api".to_string()];
    let tm = manager(&o);
    let token = sign(
        Algorithm::RS256,
        rsa_key(),
        None,
        &upstream_claims(ISSUER, json!(["api", "custom"])),
    );
    let (_, auds) = tm
        .authenticate_token(&token, Some(&["custom".to_string()]))
        .unwrap();
    assert_eq!(auds, vec!["custom".to_string()]);
    assert!(tm
        .authenticate_token(&token, Some(&["nope".to_string()]))
        .is_err());
}

#[test]
fn token_without_audience_is_valid_for_the_api_audiences_only() {
    // jwt.go:383-388: a token with no `aud` is treated as having the
    // API server's audiences.
    let mut o = opts(&["rsa.pub"]);
    o.api_audiences = vec!["api".to_string()];
    let tm = manager(&o);
    let mut c = upstream_claims(ISSUER, json!(null));
    c.as_object_mut().unwrap().remove("aud");
    let token = sign(Algorithm::RS256, rsa_key(), None, &c);
    let (_, auds) = tm.authenticate_token(&token, None).unwrap();
    assert_eq!(auds, vec!["api".to_string()]);
    assert!(tm
        .authenticate_token(&token, Some(&["other".to_string()]))
        .is_err());
}

#[test]
fn unconfigured_manager_keeps_the_legacy_behaviour() {
    let tm = TokenManager::new(b"secret");
    let claims = ServiceAccountClaims::new("sa1".into(), "ns1".into(), "uid".into(), 1);
    let token = tm.generate_token(claims).unwrap();
    let (c, _) = tm.authenticate_token(&token, None).unwrap();
    assert_eq!(c.iss, "https://kubernetes.default.svc.cluster.local");
    assert!(tm.api_audiences().is_empty());
}

#[test]
fn options_validation_matches_upstream() {
    // validation.go:44-48: signing key, issuer and audiences go together
    let mut o = opts(&["rsa.pub"]);
    o.signing_key_file = None;
    let err = o.validate().unwrap_err().to_string();
    assert!(err.contains("should be specified together"), "{err}");

    // authentication.go:280: issuer is required
    let o = ServiceAccountOptions {
        key_files: vec![fixture("rsa.pub")],
        ..Default::default()
    };
    let err = o.validate().unwrap_err().to_string();
    assert!(
        err.contains("service-account-issuer is a required flag"),
        "{err}"
    );

    // authentication.go:283: a key file is required
    let mut o = opts(&["rsa.pub"]);
    o.key_files.clear();
    assert!(o.validate().unwrap_err().to_string().contains("key-file"));

    // authentication.go:261-273
    let mut o = opts(&["rsa.pub"]);
    o.issuers = vec![ISSUER.to_string(), ISSUER.to_string()];
    assert!(o
        .validate()
        .unwrap_err()
        .to_string()
        .contains("already specified"));
    let mut o = opts(&["rsa.pub"]);
    o.issuers = vec!["".to_string()];
    assert!(o
        .validate()
        .unwrap_err()
        .to_string()
        .contains("empty string"));

    assert!(ServiceAccountOptions::default().validate().is_ok());
}

#[test]
fn key_file_parsing_matches_keyutil() {
    // several keys in one file, plus a non-key block that is tolerated
    let mut pem = read("rsa.pub");
    pem.extend_from_slice(&read("ec.pub"));
    pem.extend_from_slice(b"-----BEGIN SOMETHING ELSE-----\nAAAA\n-----END SOMETHING ELSE-----\n");
    let keys = sa_keys::parse_public_keys_pem(&pem).unwrap();
    assert_eq!(keys.len(), 2);
    // key id = base64url(sha256(PKIX DER)): same for the private and public file
    assert_eq!(keys[0].key_id, kid_of("rsa-pkcs1.key"));

    assert!(sa_keys::parse_public_keys_pem(b"not pem").is_err());
    assert!(sa_keys::public_keys_from_file(&fixture("missing.pem")).is_err());
}

/// `--service-account-max-token-expiration`
/// (`pkg/controlplane/apiserver/options/options.go:296-302`,
/// `completeServiceAccountOptions`): zero is unset, otherwise it must lie in
/// `[1h, 2^32 s]`.
#[test]
fn max_token_expiration_bounds_2714() {
    let ok = |secs: u64| {
        let mut o = opts(&["rsa.pub"]);
        o.max_expiration = Some(std::time::Duration::from_secs(secs));
        o.validate_max_expiration()
    };
    assert!(ok(0).is_ok(), "zero means unset");
    assert!(ok(3600).is_ok());
    assert!(ok(1 << 32).is_ok());
    for bad in [1, 3599, (1 << 32) + 1] {
        let err = ok(bad).unwrap_err().to_string();
        assert!(
            err.contains(
                "the service-account-max-token-expiration must be between 1 hour and 2^32 seconds"
            ),
            "{bad}: {err}"
        );
    }
    // The bound applies even when no other SA flag is given.
    let o = ServiceAccountOptions {
        max_expiration: Some(std::time::Duration::from_secs(60)),
        ..Default::default()
    };
    assert!(TokenManager::new(b"s")
        .with_service_account_options(&o)
        .is_err());
}

/// `TokenREST.Create` (`pkg/registry/core/serviceaccount/storage/token.go:222-226`):
/// a request longer than the max is shortened to it; unset (`0`) never clamps.
#[test]
fn max_token_expiration_clamps_requests_2714() {
    let unset = TokenManager::new(b"s");
    assert_eq!(unset.clamp_expiration_seconds(86_400), 86_400);

    let o = ServiceAccountOptions {
        max_expiration: Some(std::time::Duration::from_secs(7200)),
        ..Default::default()
    };
    let tm = TokenManager::new(b"s")
        .with_service_account_options(&o)
        .unwrap();
    assert_eq!(tm.max_token_expiration_seconds(), 7200);
    assert_eq!(tm.clamp_expiration_seconds(86_400), 7200);
    assert_eq!(tm.clamp_expiration_seconds(3600), 3600);
}
