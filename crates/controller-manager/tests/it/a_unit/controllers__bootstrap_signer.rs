//! Unit tests moved from `src/controllers/bootstrap_signer.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    pub use base64::Engine;
    pub use chrono::{DateTime, Utc};
    pub use hmac::{Hmac, Mac};
    pub use rusternetes_common::resources::{ConfigMap, Secret};
    pub use rusternetes_storage::{build_key, build_prefix, Storage};
    pub use sha2::Sha256;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{debug, error, info};
}

mod tests {
    use super::__parent::*;
    use chrono::Duration as D;
    use rusternetes_controller_manager::controllers::bootstrap_signer::*;
    use rusternetes_storage::memory::MemoryStorage;

    const TEST_TOKEN_ID: &str = "abc123";
    const SIG: &str =
        "eyJhbGciOiJIUzI1NiIsImtpZCI6ImFiYzEyMyJ9..QSxpUG7Q542CirTI2ECPSZjvBOJURUW5a7XqFpNI958";

    fn cm_key() -> String {
        build_key("configmaps", Some("kube-public"), "cluster-info")
    }

    /// `newConfigMap` (bootstrapsigner_test.go).
    fn new_config_map(token_id: &str, signature: &str) -> ConfigMap {
        let mut cm = ConfigMap::new("cluster-info", "kube-public");
        let mut d = HashMap::new();
        d.insert("kubeconfig".to_string(), "payload".to_string());
        if !token_id.is_empty() {
            d.insert(format!("jws-kubeconfig-{token_id}"), signature.to_string());
        }
        cm.data = Some(d);
        cm
    }

    /// `newTokenSecret` (common_test.go).
    fn new_token_secret(id: &str, secret: &str) -> Secret {
        let mut s = Secret::new(format!("bootstrap-token-{id}"), "kube-system");
        s.metadata.uid = format!("uid{id}");
        s.secret_type = Some(SECRET_TYPE_BOOTSTRAP_TOKEN.into());
        let mut d = HashMap::new();
        d.insert("token-id".to_string(), id.as_bytes().to_vec());
        d.insert("token-secret".to_string(), secret.as_bytes().to_vec());
        s.data = Some(d);
        s
    }

    /// `addSecretSigningUsage`.
    fn add_signing_usage(s: &mut Secret, v: &str) {
        s.data
            .as_mut()
            .unwrap()
            .insert("usage-bootstrap-signing".into(), v.as_bytes().to_vec());
    }

    async fn setup(
        cm: Option<ConfigMap>,
        secrets: &[Secret],
    ) -> (Signer<MemoryStorage>, Arc<MemoryStorage>) {
        let st = Arc::new(MemoryStorage::new());
        if let Some(cm) = cm {
            st.create(&cm_key(), &cm).await.unwrap();
        }
        for s in secrets {
            let key = build_key("secrets", Some("kube-system"), &s.metadata.name);
            st.create(&key, s).await.unwrap();
        }
        (Signer::new(st.clone()), st)
    }

    async fn stored_data(st: &MemoryStorage) -> HashMap<String, String> {
        st.get::<ConfigMap>(&cm_key()).await.unwrap().data.unwrap()
    }

    // TestNoConfigMap
    #[tokio::test]
    async fn no_config_map() {
        let (signer, st) = setup(None, &[]).await;
        assert!(!signer.sign_config_map(Utc::now()).await);
        assert!(st.get::<ConfigMap>(&cm_key()).await.is_err());
    }

    // TestSimpleSign
    #[tokio::test]
    async fn simple_sign() {
        let mut s = new_token_secret(TEST_TOKEN_ID, "tokenSecret");
        add_signing_usage(&mut s, "true");
        let (signer, st) = setup(Some(new_config_map("", "")), &[s]).await;
        assert!(signer.sign_config_map(Utc::now()).await);
        assert_eq!(
            stored_data(&st).await,
            new_config_map(TEST_TOKEN_ID, SIG).data.unwrap()
        );
    }

    // TestNoSignNeeded
    #[tokio::test]
    async fn no_sign_needed() {
        let mut s = new_token_secret(TEST_TOKEN_ID, "tokenSecret");
        add_signing_usage(&mut s, "true");
        let (signer, _st) = setup(Some(new_config_map(TEST_TOKEN_ID, SIG)), &[s]).await;
        assert!(!signer.sign_config_map(Utc::now()).await);
    }

    // TestUpdateSignature
    #[tokio::test]
    async fn update_signature() {
        let mut s = new_token_secret(TEST_TOKEN_ID, "tokenSecret");
        add_signing_usage(&mut s, "true");
        let (signer, st) = setup(Some(new_config_map(TEST_TOKEN_ID, "old signature")), &[s]).await;
        assert!(signer.sign_config_map(Utc::now()).await);
        assert_eq!(
            stored_data(&st).await,
            new_config_map(TEST_TOKEN_ID, SIG).data.unwrap()
        );
    }

    // TestRemoveSignature
    #[tokio::test]
    async fn remove_signature() {
        let (signer, st) = setup(Some(new_config_map(TEST_TOKEN_ID, "old signature")), &[]).await;
        assert!(signer.sign_config_map(Utc::now()).await);
        assert_eq!(stored_data(&st).await, new_config_map("", "").data.unwrap());
    }

    // validateSecretForSigning (util.go) gates: each makes the token unusable.
    #[tokio::test]
    async fn secrets_not_valid_for_signing_are_skipped() {
        let now = Utc::now();
        let mut no_usage = new_token_secret("aaaaaa", "x");
        add_signing_usage(&mut no_usage, "false");
        let mut expired = new_token_secret("bbbbbb", "x");
        add_signing_usage(&mut expired, "true");
        expired.data.as_mut().unwrap().insert(
            "expiration".into(),
            (now - D::hours(1))
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                .into_bytes(),
        );
        let mut name_mismatch = new_token_secret("cccccc", "x");
        add_signing_usage(&mut name_mismatch, "true");
        name_mismatch
            .data
            .as_mut()
            .unwrap()
            .insert("token-id".into(), b"dddddd".to_vec());
        let mut wrong_type = new_token_secret("eeeeee", "x");
        add_signing_usage(&mut wrong_type, "true");
        wrong_type.secret_type = Some("Opaque".into());
        let mut ok = new_token_secret("ffffff", "x");
        add_signing_usage(&mut ok, "true");
        let (signer, st) = setup(
            Some(new_config_map("", "")),
            &[no_usage, expired, name_mismatch, wrong_type, ok],
        )
        .await;
        assert!(signer.sign_config_map(now).await);
        let data = stored_data(&st).await;
        let mut keys: Vec<_> = data.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, vec!["jws-kubeconfig-ffffff", "kubeconfig"]);
    }

    // No `kubeconfig` key: nothing is signed.
    #[tokio::test]
    async fn no_kubeconfig_key_is_noop() {
        let mut cm = new_config_map("", "");
        cm.data.as_mut().unwrap().clear();
        let mut s = new_token_secret(TEST_TOKEN_ID, "tokenSecret");
        add_signing_usage(&mut s, "true");
        let (signer, _st) = setup(Some(cm), &[s]).await;
        assert!(!signer.sign_config_map(Utc::now()).await);
    }

    // The expected signature in bootstrapsigner_test.go, computed directly.
    #[test]
    fn detached_signature_vector() {
        assert_eq!(
            compute_detached_signature("payload", TEST_TOKEN_ID, "tokenSecret").unwrap(),
            SIG
        );
    }
}
