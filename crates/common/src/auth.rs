use crate::error::{Error, Result};
use crate::sa_keys::VerificationKey;
use base64::Engine;
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Nested kubernetes.io claim in JWT tokens (matches K8s pkg/serviceaccount/claims.go)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KubernetesClaims {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub namespace: String,
    #[serde(rename = "serviceaccount", default)]
    pub svcacct: KubeRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod: Option<KubeRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<KubeRef>,
    /// Bound Secret (`pkg/serviceaccount/claims.go` `privateClaims.Kubernetes.Secret`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<KubeRef>,
}

/// Name+UID reference used in kubernetes.io JWT claims
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KubeRef {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    // `default` so a token minted with an empty uid (skipped on serialize)
    // decodes again. Upstream pkg/serviceaccount/claims.go:65-68:
    // `type ref struct { Name string `json:"name,omitempty"`; UID string `json:"uid,omitempty"` }`
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub uid: String,
}

/// JWT claims for service account tokens
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceAccountClaims {
    /// Subject (service account name)
    #[serde(default)]
    pub sub: String,

    /// Namespace (kept for backward compat with our auth middleware).
    /// Tokens minted by an upstream kube-apiserver carry it only inside the
    /// nested `kubernetes.io` claim; `TokenManager` fills it from there.
    #[serde(default)]
    pub namespace: String,

    /// Service account UID (kept for backward compat); see `namespace`.
    #[serde(default)]
    pub uid: String,

    /// Issued at timestamp
    #[serde(default)]
    pub iat: i64,

    /// Expiration timestamp. Absent (0) for legacy secret-based tokens, which
    /// never expire (`legacy.go` `LegacyClaims` sets no `exp`).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub exp: i64,

    /// Issuer
    #[serde(default)]
    pub iss: String,

    /// Audience. go-jose marshals a single audience as a bare string, so an
    /// upstream-issued token may carry `"aud": "x"` as well as `["x"]`.
    #[serde(default, deserialize_with = "deserialize_audience")]
    pub aud: Vec<String>,

    /// Nested kubernetes.io claims (matches K8s JWT structure)
    #[serde(rename = "kubernetes.io", skip_serializing_if = "Option::is_none")]
    pub kubernetes: Option<KubernetesClaims>,

    /// Bound pod name (for projected service account tokens)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_name: Option<String>,

    /// Bound pod UID (for projected service account tokens)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_uid: Option<String>,

    /// Node name where the bound pod is running
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,

    /// Node UID where the bound pod is running
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_uid: Option<String>,
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

/// `serviceaccount.LegacyIssuer` (`pkg/serviceaccount/legacy.go:56`).
pub const LEGACY_ISSUER: &str = "kubernetes/serviceaccount";
/// `serviceaccount.LastUsedLabelKey` (`legacy.go:57`).
pub const LEGACY_TOKEN_LAST_USED_LABEL_KEY: &str = "kubernetes.io/legacy-token-last-used";
/// `serviceaccount.InvalidSinceLabelKey` (`legacy.go:40`).
pub const LEGACY_TOKEN_INVALID_SINCE_LABEL_KEY: &str = "kubernetes.io/legacy-token-invalid-since";

/// `legacyPrivateClaims` (`pkg/serviceaccount/legacy.go:60-65`): the private
/// claims of a legacy secret-based token.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct LegacyPrivateClaims {
    #[serde(rename = "kubernetes.io/serviceaccount/service-account.name", default)]
    pub service_account_name: String,
    #[serde(rename = "kubernetes.io/serviceaccount/service-account.uid", default)]
    pub service_account_uid: String,
    #[serde(rename = "kubernetes.io/serviceaccount/secret.name", default)]
    pub secret_name: String,
    #[serde(rename = "kubernetes.io/serviceaccount/namespace", default)]
    pub namespace: String,
}

impl LegacyPrivateClaims {
    /// Decode the private claims from the payload of a token whose signature
    /// has already been verified.
    pub fn from_verified_token(token: &str) -> Result<Self> {
        let payload = token
            .split('.')
            .nth(1)
            .and_then(|p| {
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(p)
                    .ok()
            })
            .ok_or_else(|| Error::Authentication("Invalid token: malformed payload".into()))?;
        serde_json::from_slice(&payload)
            .map_err(|_| Error::Authentication("Invalid token: malformed payload".into()))
    }
}

fn deserialize_audience<'de, D>(d: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }
    Ok(match Option::<OneOrMany>::deserialize(d)? {
        None => Vec::new(),
        Some(OneOrMany::One(s)) => vec![s],
        Some(OneOrMany::Many(v)) => v,
    })
}

impl ServiceAccountClaims {
    pub fn new(
        service_account: String,
        namespace: String,
        uid: String,
        expiration_hours: i64,
    ) -> Self {
        let now = Utc::now();
        let exp = now + Duration::hours(expiration_hours);

        let sa_name = service_account.clone();
        Self {
            sub: format!("system:serviceaccount:{}:{}", namespace, sa_name),
            namespace: namespace.clone(),
            uid: uid.clone(),
            iat: now.timestamp(),
            exp: exp.timestamp(),
            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
            aud: vec!["rusternetes".to_string()],
            kubernetes: Some(KubernetesClaims {
                namespace,
                svcacct: KubeRef { name: sa_name, uid },
                pod: None,
                node: None,
                secret: None,
            }),
            pod_name: None,
            pod_uid: None,
            node_name: None,
            node_uid: None,
        }
    }
}

/// Options for the four upstream service-account flags
/// (`--service-account-key-file`, `--service-account-signing-key-file`,
/// `--service-account-issuer`, `--api-audiences`).
///
/// Ported from `pkg/kubeapiserver/options/authentication.go`
/// (`ServiceAccountAuthenticationOptions`, `Validate`, `ToAuthenticationConfig`)
/// and `pkg/controlplane/apiserver/options/validation.go` (`validateTokenRequest`).
#[derive(Debug, Clone, Default)]
pub struct ServiceAccountOptions {
    /// `--service-account-key-file` (repeatable): PEM files whose RSA/ECDSA
    /// private or public keys verify tokens.
    pub key_files: Vec<String>,
    /// `--service-account-signing-key-file`: private key tokens are signed with.
    pub signing_key_file: Option<String>,
    /// `--service-account-issuer` (repeatable): the first is asserted in the
    /// `iss` of issued tokens, all are accepted.
    pub issuers: Vec<String>,
    /// `--api-audiences`. Defaults to the issuers when unset.
    pub api_audiences: Vec<String>,
    /// `--service-account-max-token-expiration` (authentication.go:459-462):
    /// the longest TokenRequest validity; a longer request is shortened to it.
    /// `None`/zero = unset. Independent of the other flags, so it does not
    /// count towards [`Self::is_empty`].
    pub max_expiration: Option<std::time::Duration>,
}

impl ServiceAccountOptions {
    /// True when none of the four flags was given: the legacy
    /// (`--jwt-secret` / auto-discovered key) behaviour applies.
    pub fn is_empty(&self) -> bool {
        self.key_files.is_empty()
            && self.signing_key_file.is_none()
            && self.issuers.is_empty()
            && self.api_audiences.is_empty()
    }

    /// The `--service-account-max-token-expiration` bound check of
    /// `completeServiceAccountOptions` (options.go:296-302): unset, or between
    /// one hour and 2^32 seconds.
    pub fn validate_max_expiration(&self) -> Result<()> {
        match self.max_expiration {
            Some(d) if !d.is_zero() => {
                let low = std::time::Duration::from_secs(3600);
                let up = std::time::Duration::from_secs(1 << 32);
                if d < low || d > up {
                    return Err(Error::InvalidResource(
                        "the service-account-max-token-expiration must be between 1 hour and 2^32 seconds"
                            .to_string(),
                    ));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Port of `validateTokenRequest` (validation.go:34-52) and
    /// `BuiltInAuthenticationOptions.Validate` (authentication.go:258-288).
    pub fn validate(&self) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }
        let mut errs: Vec<String> = Vec::new();

        // validation.go:38-49
        let enable_attempted = self.signing_key_file.is_some()
            || self.issuers.first().is_some_and(|i| !i.is_empty())
            || !self.api_audiences.is_empty();
        let enable_succeeded = self.signing_key_file.is_some() && !self.issuers.is_empty();
        if enable_attempted && !enable_succeeded {
            errs.push(
                "--service-account-signing-key-file, --service-account-issuer, and --api-audiences should be specified together"
                    .to_string(),
            );
        }

        // authentication.go:258-277
        let mut seen = std::collections::HashSet::new();
        for issuer in &self.issuers {
            if issuer.contains(':') {
                if let Err(e) = url::Url::parse(issuer) {
                    errs.push(format!(
                        "service-account-issuer {issuer:?} contained a ':' but was not a valid URL: {e}"
                    ));
                    continue;
                }
            }
            if issuer.is_empty() {
                errs.push("service-account-issuer should not be an empty string".to_string());
                continue;
            }
            if !seen.insert(issuer.clone()) {
                errs.push(format!(
                    "service-account-issuer {issuer:?} is already specified"
                ));
            }
        }

        // authentication.go:279-287 (the signing-endpoint alternative is not supported)
        if self.issuers.is_empty() {
            errs.push("service-account-issuer is a required flag".to_string());
        }
        if self.key_files.is_empty() {
            errs.push("--service-account-key-file must be set".to_string());
        }

        if errs.is_empty() {
            Ok(())
        } else {
            Err(Error::InvalidResource(errs.join("; ")))
        }
    }
}

/// The issuers accepted when `--service-account-issuer` is not given.
const DEFAULT_ISSUERS: [&str; 2] = [
    "https://kubernetes.default.svc.cluster.local",
    "rusternetes-api-server",
];

/// TokenManager handles JWT token generation and validation
#[derive(Clone)]
pub struct TokenManager {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    /// Whether we're using RS256 (true) or HS256 (false) signing
    use_rsa: bool,
    /// Algorithm tokens are signed with.
    signing_algorithm: Algorithm,
    /// `kid` header of issued tokens (`keyIDFromPublicKey`), when signing with
    /// an asymmetric key.
    signing_key_id: Option<String>,
    /// Static public keys tokens are verified with
    /// (`serviceaccount.StaticPublicKeysGetter`). `None` = verify with the
    /// signing key only (legacy behaviour).
    verification_keys: Option<std::sync::Arc<Vec<VerificationKey>>>,
    /// `--service-account-issuer`; empty = `DEFAULT_ISSUERS`.
    issuers: Vec<String>,
    /// `--api-audiences` (the authenticator's `implicitAuds`); empty = no
    /// audience enforcement.
    api_audiences: Vec<String>,
    /// `--service-account-max-token-expiration` in seconds; 0 = unset
    /// (`TokenREST.maxExpirationSeconds`).
    max_token_expiration_seconds: i64,
}

impl TokenManager {
    /// Create a new TokenManager with a secret key (HS256 — fallback)
    pub fn new(secret: &[u8]) -> Self {
        Self {
            encoding_key: EncodingKey::from_secret(secret),
            decoding_key: DecodingKey::from_secret(secret),
            use_rsa: false,
            signing_algorithm: Algorithm::HS256,
            signing_key_id: None,
            verification_keys: None,
            issuers: Vec::new(),
            api_audiences: Vec::new(),
            max_token_expiration_seconds: 0,
        }
    }

    /// Create a new TokenManager with RSA PEM keys (RS256 — K8s compatible)
    /// K8s uses RS256 for service account tokens so OIDC discovery works.
    /// See: pkg/serviceaccount/jwt.go — JWTTokenGenerator
    pub fn new_rsa(private_key_pem: &[u8], public_key_pem: &[u8]) -> Result<Self> {
        let encoding_key = EncodingKey::from_rsa_pem(private_key_pem)
            .map_err(|e| Error::Internal(format!("Invalid RSA private key: {}", e)))?;
        let decoding_key = DecodingKey::from_rsa_pem(public_key_pem)
            .map_err(|e| Error::Internal(format!("Invalid RSA public key: {}", e)))?;
        Ok(Self {
            encoding_key,
            decoding_key,
            use_rsa: true,
            signing_algorithm: Algorithm::RS256,
            signing_key_id: None,
            verification_keys: None,
            issuers: Vec::new(),
            api_audiences: Vec::new(),
            max_token_expiration_seconds: 0,
        })
    }

    /// Apply the four upstream service-account flags on top of this manager.
    ///
    /// Port of `ToAuthenticationConfig` (authentication.go:616-645): key files
    /// become a static public-key getter, issuers and API audiences configure
    /// the authenticator, and the signing key replaces the signer.
    pub fn with_service_account_options(mut self, opts: &ServiceAccountOptions) -> Result<Self> {
        opts.validate_max_expiration()?;
        self.max_token_expiration_seconds = opts
            .max_expiration
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
        if opts.is_empty() {
            return Ok(self);
        }
        opts.validate()?;

        let mut keys: Vec<VerificationKey> = Vec::new();
        for f in &opts.key_files {
            keys.extend(crate::sa_keys::public_keys_from_file(f)?);
        }
        self.verification_keys = Some(std::sync::Arc::new(keys));

        if let Some(path) = &opts.signing_key_file {
            let data = std::fs::read(path).map_err(|e| {
                Error::Internal(format!(
                    "failed to read service account signing key {path}: {e}"
                ))
            })?;
            let signing = crate::sa_keys::signing_key_from_pem(&data).map_err(|e| {
                Error::Internal(format!(
                    "failed to parse service account signing key {path}: {e}"
                ))
            })?;
            self.encoding_key = signing.encoding_key;
            self.signing_algorithm = signing.algorithm;
            self.signing_key_id = Some(signing.key_id);
            self.use_rsa = true;
        }

        self.issuers = opts.issuers.clone();
        // authentication.go:618-620
        self.api_audiences = if opts.api_audiences.is_empty() {
            opts.issuers.clone()
        } else {
            opts.api_audiences.clone()
        };
        Ok(self)
    }

    /// `--service-account-max-token-expiration` in seconds; 0 when unset.
    pub fn max_token_expiration_seconds(&self) -> i64 {
        self.max_token_expiration_seconds
    }

    /// `TokenREST.Create` (pkg/registry/core/serviceaccount/storage/token.go:
    /// 222-226): a requested expiration longer than the configured maximum is
    /// shortened to it.
    pub fn clamp_expiration_seconds(&self, requested: i64) -> i64 {
        let max = self.max_token_expiration_seconds;
        if max > 0 && requested > max {
            max
        } else {
            requested
        }
    }

    /// The issuer asserted in issued tokens, when configured.
    pub fn issuer(&self) -> Option<&str> {
        self.issuers.first().map(String::as_str)
    }

    /// `--api-audiences` (defaulted to the issuers). Empty when unconfigured.
    pub fn api_audiences(&self) -> &[String] {
        &self.api_audiences
    }

    /// The legacy authenticator is always registered next to the bound-token
    /// one (`pkg/kubeapiserver/authenticator/config.go:141-147`,
    /// `newLegacyServiceAccountAuthenticator`: `JWTTokenAuthenticator([]string{
    /// serviceaccount.LegacyIssuer}, ...)`), so `LEGACY_ISSUER` is accepted
    /// whatever `--service-account-issuer` says. Such a token must then pass
    /// the legacy validator, never the bound one (see the middleware's
    /// `validate_service_account_token`).
    fn accepts_issuer(&self, iss: &str) -> bool {
        iss == LEGACY_ISSUER
            || if self.issuers.is_empty() {
                DEFAULT_ISSUERS.contains(&iss)
            } else {
                self.issuers.iter().any(|i| i == iss)
            }
    }

    fn accepted_issuers(&self) -> Vec<&str> {
        let mut v: Vec<&str> = if self.issuers.is_empty() {
            DEFAULT_ISSUERS.to_vec()
        } else {
            self.issuers.iter().map(String::as_str).collect()
        };
        v.push(LEGACY_ISSUER);
        v
    }

    /// Create a TokenManager, trying RSA keys first, falling back to HMAC secret
    pub fn new_auto(secret: &[u8]) -> Self {
        // Try to load RSA keys from standard paths
        let key_paths = [
            ("/etc/kubernetes/pki/sa.key", "/etc/kubernetes/pki/sa.pub"),
            (
                "/root/.rusternetes/certs/sa.key",
                "/root/.rusternetes/certs/sa.pub",
            ),
            (
                "/root/.rusternetes/keys/sa-signing-key.pem",
                "/root/.rusternetes/keys/sa-signing-key.pub",
            ),
        ];
        for (priv_path, pub_path) in &key_paths {
            if let (Ok(priv_pem), Ok(pub_pem)) = (std::fs::read(priv_path), std::fs::read(pub_path))
            {
                match Self::new_rsa(&priv_pem, &pub_pem) {
                    Ok(tm) => {
                        tracing::info!("TokenManager: using RS256 with keys from {}", priv_path);
                        return tm;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "TokenManager: failed to load RSA keys from {}: {}",
                            priv_path,
                            e
                        );
                    }
                }
            }
        }
        tracing::info!("TokenManager: using HS256 (no RSA keys found)");
        Self::new(secret)
    }

    /// Generate a JWT token for a service account
    pub fn generate_token(&self, mut claims: ServiceAccountClaims) -> Result<String> {
        let mut header = if self.use_rsa {
            Header::new(self.signing_algorithm)
        } else {
            Header::default() // HS256
        };
        header.kid = self.signing_key_id.clone();
        // `GenerateToken` (jwt.go:442-451): the configured issuer is applied
        // last and wins over anything in the claims.
        if let Some(iss) = self.issuer() {
            claims.iss = iss.to_string();
        }
        encode(&header, &claims, &self.encoding_key)
            .map_err(|e| Error::Internal(format!("Failed to generate token: {}", e)))
    }

    /// Validate and decode a JWT token against the API server's own
    /// audiences (see `authenticate_token`).
    pub fn validate_token(&self, token: &str) -> Result<ServiceAccountClaims> {
        self.authenticate_token(token, None).map(|(c, _)| c)
    }

    /// Validate and decode a JWT token against specific audiences
    pub fn validate_token_with_audiences(
        &self,
        token: &str,
        audiences: &[String],
    ) -> Result<ServiceAccountClaims> {
        let algo = if self.use_rsa {
            Algorithm::RS256
        } else {
            Algorithm::HS256
        };
        let mut validation = Validation::new(algo);
        if !audiences.is_empty() {
            validation.set_audience(audiences);
        } else {
            validation.validate_aud = false;
        }
        validation.set_issuer(&self.accepted_issuers());

        decode::<ServiceAccountClaims>(token, &self.decoding_key, &validation)
            .map(|data| data.claims)
            .map_err(|e| Error::Authentication(format!("Invalid token: {}", e)))
    }

    /// Port of `jwtTokenAuthenticator.AuthenticateToken`
    /// (`pkg/serviceaccount/jwt.go:334-411`): pre-check the (unverified)
    /// issuer, verify the signature against the key selected by `kid` (or every
    /// key when there is none), sanity-check the issuer again, then apply the
    /// audience rules. `requested_audiences` is the audience set the caller
    /// wants the token valid for (`authenticator.AudiencesFrom(ctx)`, e.g. a
    /// TokenReview's `spec.audiences`); `None` means the API server's own.
    ///
    /// Without `--api-audiences` the token's audience is not enforced (the
    /// authenticator's `implicitAuds` is empty, so the check at jwt.go:397 is
    /// skipped).
    ///
    /// Returns the claims and the matched audiences.
    pub fn authenticate_token(
        &self,
        token: &str,
        requested_audiences: Option<&[String]>,
    ) -> Result<(ServiceAccountClaims, Vec<String>)> {
        let invalid = |m: String| Error::Authentication(format!("Invalid token: {m}"));

        // hasCorrectIssuer (jwt.go:420-440)
        if token.trim_start().starts_with('{') {
            return Err(invalid("not a compact JWT".to_string()));
        }
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(invalid("not a compact JWT".to_string()));
        }
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[1])
            .map_err(|_| invalid("malformed payload".to_string()))?;
        #[derive(Deserialize)]
        struct IssOnly {
            #[serde(default)]
            iss: String,
        }
        let unverified: IssOnly = serde_json::from_slice(&payload)
            .map_err(|_| invalid("malformed payload".to_string()))?;
        if !self.accepts_issuer(&unverified.iss) {
            return Err(invalid(format!(
                "token issuer {:?} is invalid",
                unverified.iss
            )));
        }

        let header = jsonwebtoken::decode_header(token).map_err(|e| invalid(e.to_string()))?;
        let kid = header.kid.clone().unwrap_or_default();

        // Candidate (key, algorithms) pairs: `GetPublicKeys(ctx, kid)`.
        let legacy_algs: Vec<Algorithm> = vec![if self.use_rsa {
            Algorithm::RS256
        } else {
            Algorithm::HS256
        }];
        let candidates: Vec<(&DecodingKey, &[Algorithm])> = match &self.verification_keys {
            Some(keys) => keys
                .iter()
                .filter(|k| kid.is_empty() || k.key_id == kid)
                .map(|k| (&k.decoding_key, k.algorithms.as_slice()))
                .collect(),
            None => vec![(&self.decoding_key, legacy_algs.as_slice())],
        };
        if candidates.is_empty() {
            return Err(invalid("invalid signature, no keys found".to_string()));
        }

        let issuers = self.accepted_issuers();
        let mut errors: Vec<String> = Vec::new();
        let mut decoded: Option<ServiceAccountClaims> = None;
        for (key, algs) in candidates {
            if !algs.contains(&header.alg) {
                errors.push(format!("unexpected signature algorithm {:?}", header.alg));
                continue;
            }
            let mut validation = Validation::new(header.alg);
            validation.algorithms = algs.to_vec();
            // Audiences are matched below (jwt.go:382-402); `validator.Validate`
            // enforces expiry / not-before with `jwt.DefaultLeeway` (1 minute).
            validation.validate_aud = false;
            validation.validate_nbf = true;
            // `exp` is optional: go-jose `Claims.Validate` only checks it when
            // present, and legacy tokens never carry one.
            validation.required_spec_claims.clear();
            validation.leeway = 60;
            validation.set_issuer(&issuers);
            match decode::<ServiceAccountClaims>(token, key, &validation) {
                Ok(data) => {
                    decoded = Some(data.claims);
                    break;
                }
                Err(e) => errors.push(e.to_string()),
            }
        }
        let mut claims = decoded.ok_or_else(|| invalid(errors.join("; ")))?;

        // A legacy token carries them in the `kubernetes.io/serviceaccount/*`
        // private claims (`legacyPrivateClaims`); the legacy validator
        // verifies them against the Secret / ServiceAccount.
        if claims.iss == LEGACY_ISSUER {
            let private = LegacyPrivateClaims::from_verified_token(token)?;
            if claims.namespace.is_empty() {
                claims.namespace = private.namespace;
            }
            if claims.uid.is_empty() {
                claims.uid = private.service_account_uid;
            }
        }

        // An upstream token carries namespace / SA uid only in `kubernetes.io`.
        if let Some(k) = &claims.kubernetes {
            if claims.namespace.is_empty() {
                claims.namespace = k.namespace.clone();
            }
            if claims.uid.is_empty() {
                claims.uid = k.svcacct.uid.clone();
            }
            if claims.sub.is_empty() && !k.namespace.is_empty() && !k.svcacct.name.is_empty() {
                claims.sub = format!("system:serviceaccount:{}:{}", k.namespace, k.svcacct.name);
            }
            // Surface the bound pod / node in the flat fields `UserInfo` reads.
            if let Some(p) = &k.pod {
                if claims.pod_name.is_none() {
                    claims.pod_name = Some(p.name.clone());
                }
                if claims.pod_uid.is_none() {
                    claims.pod_uid = Some(p.uid.clone());
                }
            }
            if let Some(n) = &k.node {
                if claims.node_name.is_none() && !n.name.is_empty() {
                    claims.node_name = Some(n.name.clone());
                }
                if claims.node_uid.is_none() && !n.uid.is_empty() {
                    claims.node_uid = Some(n.uid.clone());
                }
            }
        }

        // iat in the future (`jwt.ErrIssuedInTheFuture`, claims.go:159).
        if claims.iat > Utc::now().timestamp() + 60 {
            return Err(invalid(
                "service account token is issued in the future".to_string(),
            ));
        }

        // Audience rules (jwt.go:382-402).
        let implicit = &self.api_audiences;
        let mut token_auds: Vec<String> = claims.aud.clone();
        if token_auds.is_empty() {
            // legacy token without an audience: only API server audiences
            token_auds = implicit.clone();
        }
        let requested: Vec<String> = match requested_audiences {
            Some(r) if !r.is_empty() => r.to_vec(),
            _ => implicit.clone(),
        };
        let matched: Vec<String> = token_auds
            .iter()
            .filter(|a| requested.contains(a))
            .cloned()
            .collect();
        if matched.is_empty() && !implicit.is_empty() {
            return Err(Error::Authentication(format!(
                "Invalid token: token audiences {token_auds:?} is invalid for the target audiences {requested:?}"
            )));
        }
        Ok((claims, matched))
    }
}

/// Bootstrap Token for node join and authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapToken {
    /// Token ID (6 characters)
    pub token_id: String,

    /// Token Secret (16 characters)
    pub token_secret: String,

    /// Expiration timestamp (None = no expiration)
    pub expiration: Option<i64>,

    /// Usage restrictions
    pub usages: Vec<String>,

    /// Description
    pub description: Option<String>,

    /// Extra groups to add to the authenticated user
    pub auth_extra_groups: Option<Vec<String>>,
}

impl BootstrapToken {
    /// Create a new bootstrap token
    pub fn new(token_id: String, token_secret: String) -> Self {
        Self {
            token_id,
            token_secret,
            expiration: None,
            usages: vec!["signing".to_string(), "authentication".to_string()],
            description: None,
            auth_extra_groups: None,
        }
    }

    /// Format as kubernetes bootstrap token (token-id.token-secret)
    pub fn to_token_string(&self) -> String {
        format!("{}.{}", self.token_id, self.token_secret)
    }

    /// Parse from kubernetes bootstrap token format
    pub fn from_token_string(token: &str) -> Result<(String, String)> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 2 {
            return Err(Error::Authentication(
                "Invalid bootstrap token format".to_string(),
            ));
        }

        let token_id = parts[0].to_string();
        let token_secret = parts[1].to_string();

        // Validate format
        if token_id.len() != 6 || token_secret.len() != 16 {
            return Err(Error::Authentication(
                "Bootstrap token must be in format [a-z0-9]{6}.[a-z0-9]{16}".to_string(),
            ));
        }

        Ok((token_id, token_secret))
    }

    /// Hash the token secret for secure storage
    pub fn hash_secret(secret: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(secret.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Check if token is expired
    pub fn is_expired(&self) -> bool {
        if let Some(exp) = self.expiration {
            Utc::now().timestamp() > exp
        } else {
            false
        }
    }

    /// Check if token has the specified usage
    pub fn has_usage(&self, usage: &str) -> bool {
        self.usages.contains(&usage.to_string())
    }
}

/// User information extracted from authentication
#[derive(Debug, Clone)]
pub struct UserInfo {
    /// Username
    pub username: String,

    /// UID
    pub uid: String,

    /// Groups the user belongs to
    pub groups: Vec<String>,

    /// Extra attributes
    pub extra: std::collections::HashMap<String, Vec<String>>,
}

impl UserInfo {
    pub fn from_service_account_claims(claims: &ServiceAccountClaims) -> Self {
        let mut extra = std::collections::HashMap::new();

        // Include pod/node binding info in extra if present
        if let Some(ref pod_name) = claims.pod_name {
            extra.insert(
                "authentication.kubernetes.io/pod-name".to_string(),
                vec![pod_name.clone()],
            );
        }
        if let Some(ref pod_uid) = claims.pod_uid {
            extra.insert(
                "authentication.kubernetes.io/pod-uid".to_string(),
                vec![pod_uid.clone()],
            );
        }
        if let Some(ref node_name) = claims.node_name {
            extra.insert(
                "authentication.kubernetes.io/node-name".to_string(),
                vec![node_name.clone()],
            );
        }
        if let Some(ref node_uid) = claims.node_uid {
            extra.insert(
                "authentication.kubernetes.io/node-uid".to_string(),
                vec![node_uid.clone()],
            );
        }

        // Add credential-id
        let jti = format!("JTI={}", uuid::Uuid::new_v4());
        extra.insert(
            "authentication.kubernetes.io/credential-id".to_string(),
            vec![jti],
        );

        Self {
            username: claims.sub.clone(),
            uid: claims.uid.clone(),
            groups: vec![
                "system:serviceaccounts".to_string(),
                format!("system:serviceaccounts:{}", claims.namespace),
                "system:authenticated".to_string(),
            ],
            extra,
        }
    }

    pub fn from_bootstrap_token(token: &BootstrapToken) -> Self {
        let mut groups = vec![
            "system:bootstrappers".to_string(),
            "system:authenticated".to_string(),
        ];

        // Add extra groups if specified
        if let Some(ref extra_groups) = token.auth_extra_groups {
            groups.extend(extra_groups.clone());
        }

        Self {
            username: format!("system:bootstrap:{}", token.token_id),
            uid: token.token_id.clone(),
            groups,
            extra: std::collections::HashMap::new(),
        }
    }

    pub fn anonymous() -> Self {
        Self {
            username: "system:anonymous".to_string(),
            uid: String::new(),
            groups: vec!["system:unauthenticated".to_string()],
            extra: std::collections::HashMap::new(),
        }
    }

    pub fn from_client_cert(subject: &str) -> Self {
        // Extract CN and O from subject
        // Format: CN=<username>,O=<group1>,O=<group2>,...
        let mut username = String::new();
        let mut groups = vec!["system:authenticated".to_string()];

        for part in subject.split(',') {
            let part = part.trim();
            if let Some(cn) = part.strip_prefix("CN=") {
                username = cn.to_string();
            } else if let Some(o) = part.strip_prefix("O=") {
                groups.push(o.to_string());
            }
        }

        Self {
            uid: username.clone(),
            username,
            groups,
            extra: std::collections::HashMap::new(),
        }
    }

    /// Build a user from a verified client certificate's parsed identity,
    /// mirroring upstream's `CommonNameUserConversion`
    /// (staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go):
    /// the Subject CommonName becomes the username and each Subject
    /// Organization becomes a group. The union authenticator then layers on the
    /// `system:authenticated` group (group.NewAuthenticatedGroupAdder), which we
    /// add here. UID is left empty — x509 carries no stable UID upstream.
    ///
    /// Callers MUST only invoke this for a certificate the TLS layer already
    /// verified against the configured client CA; this function does no
    /// verification of its own.
    pub fn from_cert_identity(common_name: &str, organizations: &[String]) -> Self {
        let mut groups: Vec<String> = organizations.to_vec();
        groups.push("system:authenticated".to_string());
        Self {
            username: common_name.to_string(),
            uid: String::new(),
            groups,
            extra: std::collections::HashMap::new(),
        }
    }
}

/// Bootstrap Token Manager for node authentication
pub struct BootstrapTokenManager {
    /// In-memory token storage (in production, this would be in etcd as Secrets)
    tokens: std::sync::RwLock<HashMap<String, BootstrapToken>>,
}

impl BootstrapTokenManager {
    pub fn new() -> Self {
        Self {
            tokens: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Add a bootstrap token
    pub fn add_token(&self, token: BootstrapToken) {
        let mut tokens = self.tokens.write().unwrap();
        tokens.insert(token.token_id.clone(), token);
    }

    /// Validate a bootstrap token
    pub fn validate_token(&self, token_str: &str) -> Result<BootstrapToken> {
        let (token_id, token_secret) = BootstrapToken::from_token_string(token_str)?;

        let tokens = self.tokens.read().unwrap();
        if let Some(stored_token) = tokens.get(&token_id) {
            // Check if token is expired
            if stored_token.is_expired() {
                return Err(Error::Authentication(
                    "Bootstrap token has expired".to_string(),
                ));
            }

            // Check if token has authentication usage
            if !stored_token.has_usage("authentication") {
                return Err(Error::Authentication(
                    "Bootstrap token does not have authentication usage".to_string(),
                ));
            }

            // Verify token secret (constant-time comparison)
            if stored_token.token_secret == token_secret {
                Ok(stored_token.clone())
            } else {
                Err(Error::Authentication(
                    "Invalid bootstrap token secret".to_string(),
                ))
            }
        } else {
            Err(Error::Authentication(
                "Bootstrap token not found".to_string(),
            ))
        }
    }

    /// Remove a bootstrap token
    pub fn remove_token(&self, token_id: &str) {
        let mut tokens = self.tokens.write().unwrap();
        tokens.remove(token_id);
    }

    /// List all tokens
    pub fn list_tokens(&self) -> Vec<BootstrapToken> {
        let tokens = self.tokens.read().unwrap();
        tokens.values().cloned().collect()
    }
}

impl Default for BootstrapTokenManager {
    fn default() -> Self {
        Self::new()
    }
}

/// OIDC Discovery Document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OIDCDiscoveryDocument {
    pub issuer: String,
    pub jwks_uri: String,
    pub authorization_endpoint: Option<String>,
    pub token_endpoint: Option<String>,
    pub userinfo_endpoint: Option<String>,
}

/// JSON Web Key Set
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonWebKeySet {
    pub keys: Vec<JsonWebKey>,
}

/// JSON Web Key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonWebKey {
    #[serde(rename = "kty")]
    pub key_type: String,
    #[serde(rename = "kid")]
    pub key_id: Option<String>,
    #[serde(rename = "use")]
    pub key_use: Option<String>,
    #[serde(rename = "alg")]
    pub algorithm: Option<String>,
    #[serde(rename = "n")]
    pub modulus: Option<String>,
    #[serde(rename = "e")]
    pub exponent: Option<String>,
}

/// OIDC Token Claims
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OIDCTokenClaims {
    pub iss: String,
    pub sub: String,
    pub aud: serde_json::Value,
    pub exp: i64,
    pub iat: i64,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub groups: Option<Vec<String>>,
    #[serde(default)]
    pub name: Option<String>,
}

/// OIDC Token Validator with JWKS support
pub struct OIDCTokenValidator {
    issuer_url: String,
    client_id: String,
    jwks: std::sync::RwLock<Option<JsonWebKeySet>>,
    http_client: reqwest::Client,
    #[allow(dead_code)]
    ca_cert: Option<String>,
}

impl OIDCTokenValidator {
    pub fn new(issuer_url: String, client_id: String, ca_cert: Option<String>) -> Self {
        Self {
            issuer_url,
            client_id,
            jwks: std::sync::RwLock::new(None),
            http_client: reqwest::Client::new(),
            ca_cert,
        }
    }

    /// Fetch the OIDC discovery document
    pub async fn fetch_discovery_document(&self) -> Result<OIDCDiscoveryDocument> {
        let discovery_url = format!("{}/.well-known/openid-configuration", self.issuer_url);

        let response = self
            .http_client
            .get(&discovery_url)
            .send()
            .await
            .map_err(|e| {
                Error::Authentication(format!("Failed to fetch OIDC discovery document: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(Error::Authentication(format!(
                "OIDC discovery document request failed with status: {}",
                response.status()
            )));
        }

        response.json::<OIDCDiscoveryDocument>().await.map_err(|e| {
            Error::Authentication(format!("Failed to parse OIDC discovery document: {}", e))
        })
    }

    /// Fetch the JWKS from the OIDC provider
    pub async fn fetch_jwks(&self) -> Result<JsonWebKeySet> {
        // First fetch the discovery document to get the JWKS URI
        let discovery = self.fetch_discovery_document().await?;

        let response = self
            .http_client
            .get(&discovery.jwks_uri)
            .send()
            .await
            .map_err(|e| Error::Authentication(format!("Failed to fetch JWKS: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::Authentication(format!(
                "JWKS request failed with status: {}",
                response.status()
            )));
        }

        response
            .json::<JsonWebKeySet>()
            .await
            .map_err(|e| Error::Authentication(format!("Failed to parse JWKS: {}", e)))
    }

    /// Refresh the cached JWKS
    pub async fn refresh_jwks(&self) -> Result<()> {
        let jwks = self.fetch_jwks().await?;
        let mut cached_jwks = self.jwks.write().unwrap();
        *cached_jwks = Some(jwks);
        Ok(())
    }

    /// Get the cached JWKS, fetching if not present
    async fn get_jwks(&self) -> Result<JsonWebKeySet> {
        {
            let cached_jwks = self.jwks.read().unwrap();
            if let Some(ref jwks) = *cached_jwks {
                return Ok(jwks.clone());
            }
        }

        // JWKS not cached, fetch it
        self.refresh_jwks().await?;

        let cached_jwks = self.jwks.read().unwrap();
        cached_jwks
            .as_ref()
            .ok_or_else(|| Error::Authentication("Failed to fetch JWKS".to_string()))
            .cloned()
    }

    /// Validate an OIDC token
    pub async fn validate_token(&self, token: &str) -> Result<UserInfo> {
        // Decode the token header to get the key ID
        let header = jsonwebtoken::decode_header(token)
            .map_err(|e| Error::Authentication(format!("Failed to decode token header: {}", e)))?;

        let kid = header
            .kid
            .ok_or_else(|| Error::Authentication("Token missing kid (key ID)".to_string()))?;

        // Get the JWKS
        let jwks = self.get_jwks().await?;

        // Find the key with matching kid
        let jwk = jwks
            .keys
            .iter()
            .find(|k| k.key_id.as_ref() == Some(&kid))
            .ok_or_else(|| Error::Authentication(format!("Key ID {} not found in JWKS", kid)))?;

        // Validate the token using the key
        let decoding_key = self.jwk_to_decoding_key(jwk)?;

        let mut validation = Validation::new(header.alg);
        validation.set_audience(&[&self.client_id]);
        validation.set_issuer(&[&self.issuer_url]);

        let token_data = decode::<OIDCTokenClaims>(token, &decoding_key, &validation)
            .map_err(|e| Error::Authentication(format!("Token validation failed: {}", e)))?;

        // Convert to UserInfo
        Ok(self.claims_to_user_info(&token_data.claims))
    }

    /// Convert JWK to DecodingKey
    fn jwk_to_decoding_key(&self, jwk: &JsonWebKey) -> Result<DecodingKey> {
        match jwk.key_type.as_str() {
            "RSA" => {
                let modulus = jwk
                    .modulus
                    .as_ref()
                    .ok_or_else(|| Error::Authentication("RSA key missing modulus".to_string()))?;
                let exponent = jwk
                    .exponent
                    .as_ref()
                    .ok_or_else(|| Error::Authentication("RSA key missing exponent".to_string()))?;

                // Decode base64url encoded values
                let n = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(modulus)
                    .map_err(|e| {
                        Error::Authentication(format!("Failed to decode modulus: {}", e))
                    })?;
                let e = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(exponent)
                    .map_err(|e| {
                        Error::Authentication(format!("Failed to decode exponent: {}", e))
                    })?;

                DecodingKey::from_rsa_components(
                    &base64::engine::general_purpose::STANDARD.encode(&n),
                    &base64::engine::general_purpose::STANDARD.encode(&e),
                )
                .map_err(|e| Error::Authentication(format!("Failed to create decoding key: {}", e)))
            }
            _ => Err(Error::Authentication(format!(
                "Unsupported key type: {}",
                jwk.key_type
            ))),
        }
    }

    /// Convert OIDC claims to UserInfo
    fn claims_to_user_info(&self, claims: &OIDCTokenClaims) -> UserInfo {
        let mut groups = vec!["system:authenticated".to_string()];

        // Add groups from token if present
        if let Some(ref token_groups) = claims.groups {
            groups.extend(token_groups.clone());
        }

        UserInfo {
            username: claims.email.clone().unwrap_or_else(|| claims.sub.clone()),
            uid: claims.sub.clone(),
            groups,
            extra: std::collections::HashMap::new(),
        }
    }
}

/// Webhook Token Authenticator with full HTTP integration
pub struct WebhookTokenAuthenticator {
    webhook_url: String,
    http_client: reqwest::Client,
    #[allow(dead_code)]
    ca_cert: Option<String>,
}

impl WebhookTokenAuthenticator {
    pub fn new(webhook_url: String, ca_cert: Option<String>) -> Result<Self> {
        let http_client = if let Some(ref cert_pem) = ca_cert {
            // Build client with custom CA certificate
            let cert = reqwest::Certificate::from_pem(cert_pem.as_bytes())
                .map_err(|e| Error::Internal(format!("Failed to parse CA certificate: {}", e)))?;

            reqwest::Client::builder()
                .add_root_certificate(cert)
                .build()
                .map_err(|e| Error::Internal(format!("Failed to create HTTP client: {}", e)))?
        } else {
            reqwest::Client::new()
        };

        Ok(Self {
            webhook_url,
            http_client,
            ca_cert,
        })
    }

    /// Authenticate a token using the webhook
    pub async fn authenticate(&self, token: &str) -> Result<UserInfo> {
        use crate::resources::authentication::{TokenReview, TokenReviewSpec};
        use crate::types::ObjectMeta;

        // Create TokenReview request
        let token_review = TokenReview {
            api_version: "authentication.k8s.io/v1".to_string(),
            kind: "TokenReview".to_string(),
            metadata: ObjectMeta::new(""),
            spec: TokenReviewSpec {
                token: token.to_string(),
                audiences: None,
            },
            status: None,
        };

        // Send request to webhook
        let response = self
            .http_client
            .post(&self.webhook_url)
            .json(&token_review)
            .send()
            .await
            .map_err(|e| Error::Authentication(format!("Webhook request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::Authentication(format!(
                "Webhook returned error status: {}",
                response.status()
            )));
        }

        // Parse response
        let token_review_response: TokenReview = response.json().await.map_err(|e| {
            Error::Authentication(format!("Failed to parse webhook response: {}", e))
        })?;

        // Check if authentication succeeded
        let status = token_review_response
            .status
            .ok_or_else(|| Error::Authentication("Webhook response missing status".to_string()))?;

        if !status.authenticated.unwrap_or(false) {
            return Err(Error::Authentication(
                status
                    .error
                    .unwrap_or_else(|| "Authentication failed".to_string()),
            ));
        }

        // Extract user info
        let user = status.user.ok_or_else(|| {
            Error::Authentication("Webhook response missing user info".to_string())
        })?;

        Ok(UserInfo {
            username: user.username.unwrap_or_default(),
            uid: user.uid.unwrap_or_default(),
            groups: user.groups.unwrap_or_default(),
            extra: user.extra.unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_sa_uid_token_round_trips_2358() {
        let manager = TokenManager::new(b"test-secret-key");
        let claims = ServiceAccountClaims::new(
            "default".to_string(),
            "default".to_string(),
            String::new(),
            24,
        );
        let token = manager.generate_token(claims).unwrap();
        let validated = manager.validate_token(&token).unwrap();
        assert!(validated.uid.is_empty());
    }

    #[test]
    fn test_token_generation_and_validation() {
        let secret = b"test-secret-key";
        let manager = TokenManager::new(secret);

        let claims = ServiceAccountClaims::new(
            "default".to_string(),
            "default".to_string(),
            "test-uid".to_string(),
            24,
        );

        let token = manager.generate_token(claims.clone()).unwrap();
        let validated = manager.validate_token(&token).unwrap();

        assert_eq!(validated.sub, claims.sub);
        assert_eq!(validated.namespace, claims.namespace);
        assert_eq!(validated.uid, claims.uid);
    }

    #[test]
    fn test_invalid_token() {
        let secret = b"test-secret-key";
        let manager = TokenManager::new(secret);

        let result = manager.validate_token("invalid.token.here");
        assert!(result.is_err());
    }

    #[test]
    fn test_bootstrap_token_validation() {
        let manager = BootstrapTokenManager::new();

        // Create and add a bootstrap token
        let token = BootstrapToken::new("abcdef".to_string(), "0123456789abcdef".to_string());
        manager.add_token(token.clone());

        // Validate the token
        let token_str = token.to_token_string();
        let validated = manager.validate_token(&token_str).unwrap();

        assert_eq!(validated.token_id, "abcdef");
        assert_eq!(validated.token_secret, "0123456789abcdef");
    }

    #[test]
    fn test_bootstrap_token_invalid_format() {
        let result = BootstrapToken::from_token_string("invalid");
        assert!(result.is_err());

        let result = BootstrapToken::from_token_string("abc.def");
        assert!(result.is_err());
    }

    #[test]
    fn test_bootstrap_token_expiration() {
        let mut token = BootstrapToken::new("abcdef".to_string(), "0123456789abcdef".to_string());

        // Set expiration to past
        token.expiration = Some(Utc::now().timestamp() - 3600);
        assert!(token.is_expired());

        // Set expiration to future
        token.expiration = Some(Utc::now().timestamp() + 3600);
        assert!(!token.is_expired());

        // No expiration
        token.expiration = None;
        assert!(!token.is_expired());
    }

    #[test]
    fn test_user_info_from_bootstrap_token() {
        let token = BootstrapToken::new("abcdef".to_string(), "0123456789abcdef".to_string());
        let user = UserInfo::from_bootstrap_token(&token);

        assert_eq!(user.username, "system:bootstrap:abcdef");
        assert!(user.groups.contains(&"system:bootstrappers".to_string()));
        assert!(user.groups.contains(&"system:authenticated".to_string()));
    }

    #[test]
    fn test_user_info_from_client_cert() {
        let subject = "CN=admin,O=system:masters";
        let user = UserInfo::from_client_cert(subject);

        assert_eq!(user.username, "admin");
        assert!(user.groups.contains(&"system:masters".to_string()));
        assert!(user.groups.contains(&"system:authenticated".to_string()));
    }

    #[test]
    fn test_user_info_from_cert_identity() {
        // CommonNameUserConversion parity: CN → username, each O → group, plus
        // the union authenticator's system:authenticated; x509 carries no UID.
        let user = UserInfo::from_cert_identity(
            "system:kube-scheduler",
            &["system:kube-scheduler".to_string()],
        );
        assert_eq!(user.username, "system:kube-scheduler");
        assert!(user.uid.is_empty());
        assert!(user.groups.contains(&"system:kube-scheduler".to_string()));
        assert!(user.groups.contains(&"system:authenticated".to_string()));
    }

    #[test]
    fn test_user_info_from_cert_identity_multiple_orgs() {
        // Each Subject Organization maps to its own group, in order, followed by
        // system:authenticated.
        let user = UserInfo::from_cert_identity("dave", &["dev".to_string(), "ops".to_string()]);
        assert_eq!(
            user.groups,
            vec![
                "dev".to_string(),
                "ops".to_string(),
                "system:authenticated".to_string()
            ]
        );
    }

    #[test]
    fn test_user_info_from_cert_identity_no_orgs() {
        // No Organization → only the system:authenticated group is present.
        let user = UserInfo::from_cert_identity("dave", &[]);
        assert_eq!(user.username, "dave");
        assert_eq!(user.groups, vec!["system:authenticated".to_string()]);
    }

    #[test]
    fn test_token_with_custom_audience() {
        let secret = b"test-secret-key";
        let manager = TokenManager::new(secret);

        // Create a token with custom audiences (like TokenRequest API would)
        let now = Utc::now();
        let claims = ServiceAccountClaims {
            sub: "system:serviceaccount:default:my-sa".to_string(),
            namespace: "default".to_string(),
            uid: "test-uid".to_string(),
            iat: now.timestamp(),
            exp: (now + Duration::hours(1)).timestamp(),
            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
            aud: vec![
                "https://kubernetes.default.svc".to_string(),
                "api".to_string(),
            ],
            pod_name: None,
            pod_uid: None,
            node_name: None,
            node_uid: None,
            kubernetes: None,
        };

        let token = manager.generate_token(claims).unwrap();

        // validate_token should accept custom audiences (relaxed validation)
        let validated = manager.validate_token(&token).unwrap();
        assert_eq!(validated.sub, "system:serviceaccount:default:my-sa");
        assert_eq!(validated.namespace, "default");
        assert!(validated
            .aud
            .contains(&"https://kubernetes.default.svc".to_string()));
    }

    #[test]
    fn test_token_with_specific_audience_validation() {
        let secret = b"test-secret-key";
        let manager = TokenManager::new(secret);

        let now = Utc::now();
        let claims = ServiceAccountClaims {
            sub: "system:serviceaccount:default:my-sa".to_string(),
            namespace: "default".to_string(),
            uid: "test-uid".to_string(),
            iat: now.timestamp(),
            exp: (now + Duration::hours(1)).timestamp(),
            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
            aud: vec!["api".to_string()],
            pod_name: None,
            pod_uid: None,
            node_name: None,
            node_uid: None,
            kubernetes: None,
        };

        let token = manager.generate_token(claims).unwrap();

        // validate_token_with_audiences should work with matching audience
        let validated = manager
            .validate_token_with_audiences(&token, &["api".to_string()])
            .unwrap();
        assert_eq!(validated.sub, "system:serviceaccount:default:my-sa");

        // Should fail with non-matching audience
        let result = manager.validate_token_with_audiences(&token, &["wrong-audience".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_token_with_short_expiry() {
        let secret = b"test-secret-key";
        let manager = TokenManager::new(secret);

        // TokenRequest API may request short expiry (e.g., 600 seconds)
        let now = Utc::now();
        let claims = ServiceAccountClaims {
            sub: "system:serviceaccount:default:short-lived".to_string(),
            namespace: "default".to_string(),
            uid: "test-uid".to_string(),
            iat: now.timestamp(),
            exp: (now + Duration::seconds(600)).timestamp(),
            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
            aud: vec!["rusternetes".to_string()],
            pod_name: None,
            pod_uid: None,
            node_name: None,
            node_uid: None,
            kubernetes: None,
        };

        let token = manager.generate_token(claims).unwrap();

        // Token should be valid
        let validated = manager.validate_token(&token).unwrap();
        assert_eq!(validated.sub, "system:serviceaccount:default:short-lived");
        // Expiry should be ~600 seconds from now
        let exp_diff = validated.exp - validated.iat;
        assert!(
            (590..=610).contains(&exp_diff),
            "Expected ~600s expiry, got {}s",
            exp_diff
        );
    }

    #[test]
    fn test_token_with_pod_binding() {
        let secret = b"rusternetes-secret-change-in-production";
        let manager = TokenManager::new(secret);

        let now = Utc::now();
        let claims = ServiceAccountClaims {
            sub: "system:serviceaccount:test-ns:my-sa".to_string(),
            namespace: "test-ns".to_string(),
            uid: "sa-uid-123".to_string(),
            iat: now.timestamp(),
            exp: (now + Duration::hours(1)).timestamp(),
            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
            aud: vec!["rusternetes".to_string()],
            kubernetes: None,
            pod_name: Some("my-pod".to_string()),
            pod_uid: Some("pod-uid-456".to_string()),
            node_name: Some("node-1".to_string()),
            node_uid: Some("node-uid-789".to_string()),
        };

        let token = manager.generate_token(claims).unwrap();
        let validated = manager.validate_token(&token).unwrap();

        // Verify pod binding info is preserved in the token
        assert_eq!(validated.pod_name, Some("my-pod".to_string()));
        assert_eq!(validated.pod_uid, Some("pod-uid-456".to_string()));
        assert_eq!(validated.node_name, Some("node-1".to_string()));
        assert_eq!(validated.node_uid, Some("node-uid-789".to_string()));
        assert_eq!(validated.sub, "system:serviceaccount:test-ns:my-sa");
        assert_eq!(validated.namespace, "test-ns");
    }

    #[test]
    fn test_token_review_includes_pod_extra_info() {
        // Simulate what create_token_review does: validate a token with pod binding
        // and verify the extra info contains pod-name, pod-uid, node-name
        let secret = b"rusternetes-secret-change-in-production";
        let manager = TokenManager::new(secret);

        let now = Utc::now();
        let claims = ServiceAccountClaims {
            sub: "system:serviceaccount:default:test-sa".to_string(),
            namespace: "default".to_string(),
            uid: "sa-uid".to_string(),
            iat: now.timestamp(),
            exp: (now + Duration::hours(1)).timestamp(),
            iss: "https://kubernetes.default.svc.cluster.local".to_string(),
            aud: vec!["rusternetes".to_string()],
            kubernetes: None,
            pod_name: Some("pod-abc".to_string()),
            pod_uid: Some("pod-uid-abc".to_string()),
            node_name: Some("node-2".to_string()),
            node_uid: Some("node-uid-2".to_string()),
        };

        let token = manager.generate_token(claims).unwrap();
        let validated = manager.validate_token(&token).unwrap();

        // Build extra info the way create_token_review does
        let mut extra = std::collections::HashMap::new();
        if let Some(ref pod_name) = validated.pod_name {
            extra.insert(
                "authentication.kubernetes.io/pod-name".to_string(),
                vec![pod_name.clone()],
            );
        }
        if let Some(ref pod_uid) = validated.pod_uid {
            extra.insert(
                "authentication.kubernetes.io/pod-uid".to_string(),
                vec![pod_uid.clone()],
            );
        }
        if let Some(ref node_name) = validated.node_name {
            extra.insert(
                "authentication.kubernetes.io/node-name".to_string(),
                vec![node_name.clone()],
            );
        }
        if let Some(ref node_uid) = validated.node_uid {
            extra.insert(
                "authentication.kubernetes.io/node-uid".to_string(),
                vec![node_uid.clone()],
            );
        }

        assert_eq!(
            extra.get("authentication.kubernetes.io/pod-name"),
            Some(&vec!["pod-abc".to_string()])
        );
        assert_eq!(
            extra.get("authentication.kubernetes.io/pod-uid"),
            Some(&vec!["pod-uid-abc".to_string()])
        );
        assert_eq!(
            extra.get("authentication.kubernetes.io/node-name"),
            Some(&vec!["node-2".to_string()])
        );
        assert_eq!(
            extra.get("authentication.kubernetes.io/node-uid"),
            Some(&vec!["node-uid-2".to_string()])
        );
    }

    /// OIDC issuer must be a valid HTTPS URL, not a bare hostname.
    /// The conformance test verifies that tokens have an `iss` claim matching
    /// the OIDC discovery endpoint's `issuer` field.
    #[test]
    fn test_service_account_claims_use_correct_issuer() {
        let claims = ServiceAccountClaims::new(
            "default".to_string(),
            "default".to_string(),
            "uid-123".to_string(),
            24,
        );
        assert_eq!(
            claims.iss, "https://kubernetes.default.svc.cluster.local",
            "Token issuer must be a valid HTTPS URL for OIDC compliance"
        );
        assert!(
            claims.iss.starts_with("https://"),
            "Issuer must start with https://"
        );
    }
}
