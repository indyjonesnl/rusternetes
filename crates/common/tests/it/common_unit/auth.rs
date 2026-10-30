//! Unit tests moved from `src/auth.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use base64::Engine;
    pub use chrono::{Duration, Utc};
    pub use jsonwebtoken::{
        decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
    };
    pub use rusternetes_common::error::{Error, Result};
    pub use rusternetes_common::sa_keys::VerificationKey;
    pub use serde::{Deserialize, Serialize};
    pub use sha2::{Digest, Sha256};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::auth::*;

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
