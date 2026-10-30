//! Unit tests moved from `src/controllers/serviceaccount.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use futures::StreamExt;
    pub use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    pub use rusternetes_common::resources::{Namespace, Secret, ServiceAccount};
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use serde::Serialize;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::serviceaccount::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_serviceaccount_controller_creation() {
        let storage = Arc::new(MemoryStorage::new());
        let _controller = ServiceAccountController::new(storage);
    }

    #[test]
    fn test_token_generation() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ServiceAccountController::new(storage);
        let token = controller
            .generate_token("default", "default", "test-uid-123", "tok")
            .unwrap();
        // Without a signing key, should generate a simple token
        assert!(token.contains("default"));
        assert!(token.contains("test-uid-123"));
    }

    const TEST_RSA_PRIVATE_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----\nMIIEogIBAAKCAQEAqB2YGMQG0Td9BCuf4kL3OS0Ze1DV6kQCou6tfUrFgsURkNCS\nfhDbojJ7AXvYfPyaZ6gZHD2BYCL7w9Q4umhP8jnrJvdOIJ71r+oY8MauKsk0Zeoo\nP/Sm2eyQ+91uMWd0nEo0o3D9Q0UlAdotCDjV7WE0onUmU/lTyiamcGsNk2WLOT7B\n2iVXZmzoSOPw2R/nt3bsNV21Pkb/YQj237PTqqT9wcsDlpG8NdJUox1E1+QTxPZU\nFYZDxL15jsoVABqKfb+ktKfnKjD2iMblE60lK/WetPNMuJcxF3qaQIl6J49Az+ha\nBCRUbbBwVwRsPTKCWG3LSqBxbwW7MbvvXvyIowIDAQABAoIBACsXGssOQ6kQjeyp\nudtmyrNPCf7/ozTepcZZYwKATcvM80mpDENf0svqIHkq4zx2CqWTAoyofybDEMEK\n/ldZMVSm380nClF2LQcf+7CLXEz/MX0F3bc24CVva2IDSaFEITGGG6Pg7Cl36Zpl\n77Dx0HN9vN3/JQnVGFLyQSsDZYFn1T2RddfX43t0HrZofcJZdeKtx0TdwY+Gy7bi\ni7HMIWD1eQk1MNuzGGisZ69R2ZT5iW+UwjrUDweBe056GioPMPV3nY/RZ2QRfhUm\nOWjWgPT9xWZpMQE1jmDGpMMOolhw/osrFG6UoT8900E/5nM7iIfwTS3IZIuxkJxs\n1kbyS6ECgYEA1CdUE1CKLKt6XeC14tBFVifTT65vo22YZ0y8VYKSecoP2T4Vscf2\nAsfZ0YBsLZqtm2XjZSRTibJG0gBQVEgLxT3ZSOXO9ase3Co9EGbrTib+eHZLw4Cl\nBnPS8MrI2TK9xqrAbiYVea3Gb63+Ek08Pesfk1KbNrbK4CNwSNWoxnkCgYEAytxK\nw12ohwCw+d0VB0FYQ4fXZb65DwWeucOtHt/cY0dolMqxO/hOLpj3IpwxADbbHa1I\nPtYJRsYK9PnVo8K8Rs8tPS81vGiX99NsgCsoh5DyuU6d6c2QFrsb26ov29OyX2lv\nvOc3XWNcao+tsKpoQ/KFKB432fHlYri/P/HVcPsCgYAfdW18J7c1hH/yp72Q0n1V\nlzY4XI9lVn0A5FoQ+/moYZQUDKa+4/3Qz7222SoxYPxZTLR5bPeONYdW4IEI3l4Q\nc2li6+DSgPtkfkbrxbcisZmOV0xIwyy1VjtzRT6fJm0Jpow+SRtqHaCNMum34QgL\nzm+yMs+dP2G59sdRpY0PUQKBgAKf9xewDo4wpBmXkr4VSl8VUuQuI5beK7+bmJHd\ns6xVMDU8qi5seBaCRDBedQPbsdogc97cRiJ0TY/965XC30zLQXqZMcjOUakTQ0Ql\nStD2Py3GpqRv1H12zlV5TkU56AT0CE4Zb831iyyVz1mJ2u+GI9LxESfwyVcNrOvW\n5TwhAoGAQjU0xBQzdO5CN6awzK0UIBYD+T9kWYfuvo3U/HM2Rvkqwkoy/joTJQW6\nVYLNOHpsxvFtfDXq1VH61L618sKgsiMzK6ccSXS6K7w1nLxtooDwFkNLFKScg/lG\nLb5SNwMRw0msssuqZ6s3jXac1UcJCNZC6P7OLAgJoNw16QyLnlU=\n-----END RSA PRIVATE KEY-----\n";
    const TEST_RSA_PUBLIC_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqB2YGMQG0Td9BCuf4kL3\nOS0Ze1DV6kQCou6tfUrFgsURkNCSfhDbojJ7AXvYfPyaZ6gZHD2BYCL7w9Q4umhP\n8jnrJvdOIJ71r+oY8MauKsk0ZeooP/Sm2eyQ+91uMWd0nEo0o3D9Q0UlAdotCDjV\n7WE0onUmU/lTyiamcGsNk2WLOT7B2iVXZmzoSOPw2R/nt3bsNV21Pkb/YQj237PT\nqqT9wcsDlpG8NdJUox1E1+QTxPZUFYZDxL15jsoVABqKfb+ktKfnKjD2iMblE60l\nK/WetPNMuJcxF3qaQIl6J49Az+haBCRUbbBwVwRsPTKCWG3LSqBxbwW7MbvvXvyI\nowIDAQAB\n-----END PUBLIC KEY-----\n";

    /// Upstream `serviceaccount.LegacyClaims` + `GenerateToken`
    /// (pkg/serviceaccount/legacy.go:42-51, jwt.go:443-452): a Secret token is
    /// signed with iss `kubernetes/serviceaccount`, the subject and the four
    /// `kubernetes.io/serviceaccount/*` private claims, and nothing else
    /// (no exp, iat, nbf, aud, nested `kubernetes.io`).
    #[test]
    fn secret_token_carries_legacy_claims() {
        let storage = Arc::new(MemoryStorage::new());
        let mut controller = ServiceAccountController::new(storage);
        controller.signing_key =
            Some(EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_PEM.as_bytes()).unwrap());
        let token = controller
            .generate_token("ns", "sa", "sa-uid", "tok")
            .unwrap();

        let mut v = jsonwebtoken::Validation::new(Algorithm::RS256);
        v.required_spec_claims.clear();
        v.validate_exp = false;
        v.validate_aud = false;
        let data = jsonwebtoken::decode::<serde_json::Value>(
            &token,
            &jsonwebtoken::DecodingKey::from_rsa_pem(TEST_RSA_PUBLIC_PEM.as_bytes()).unwrap(),
            &v,
        )
        .unwrap();
        assert_eq!(
            data.claims,
            serde_json::json!({
                "iss": "kubernetes/serviceaccount",
                "sub": "system:serviceaccount:ns:sa",
                "kubernetes.io/serviceaccount/namespace": "ns",
                "kubernetes.io/serviceaccount/service-account.name": "sa",
                "kubernetes.io/serviceaccount/service-account.uid": "sa-uid",
                "kubernetes.io/serviceaccount/secret.name": "tok",
            })
        );
    }

    /// Upstream `namespaceAdded` / `namespaceUpdated`
    /// (pkg/controller/serviceaccount/serviceaccounts_controller.go) enqueue the
    /// namespace on its informer event, so the default ServiceAccount appears
    /// within a sync, not on the next periodic resync. A namespace created
    /// after the controller started must get its default SA well before the
    /// 30s resync (#3029).
    #[tokio::test]
    async fn namespace_add_event_creates_default_serviceaccount_promptly() {
        use rusternetes_common::resources::Namespace;
        let storage = Arc::new(MemoryStorage::new());
        let controller = Arc::new(ServiceAccountController::new(storage.clone()));
        let handle = tokio::spawn(controller.run());
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let ns: Namespace = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Namespace",
            "metadata": {"name": "late-ns"}
        }))
        .unwrap();
        storage
            .create(&build_key("namespaces", None, "late-ns"), &ns)
            .await
            .unwrap();

        let sa_key = build_key("serviceaccounts", Some("late-ns"), "default");
        let mut found = false;
        for _ in 0..50 {
            if storage.get::<ServiceAccount>(&sa_key).await.is_ok() {
                found = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        handle.abort();
        assert!(found, "default SA not created within 5s of namespace add");
    }

    /// #3071: a namespace created AFTER the list revision but BEFORE the
    /// watches open (the list-then-watch gap) must still surface as a namespace
    /// event. Reflector `ListAndWatch` watches from the list's resourceVersion;
    /// a revision-less watch only tails future events and would miss it until
    /// the 30s resync.
    #[tokio::test]
    async fn namespace_created_in_list_watch_gap_is_replayed() {
        use futures::StreamExt;
        use rusternetes_common::resources::Namespace;
        let storage = Arc::new(MemoryStorage::new());
        let controller = ServiceAccountController::new(storage.clone());

        let list_rev = storage.current_revision().await.unwrap();
        // The list happens here; the namespace then lands in the gap.
        let ns: Namespace = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Namespace",
            "metadata": {"name": "gap-ns"}
        }))
        .unwrap();
        storage
            .create(&build_key("namespaces", None, "gap-ns"), &ns)
            .await
            .unwrap();

        let (_sa, _secrets, mut ns_watch) = controller.open_watches(list_rev).await.unwrap();
        let ev = tokio::time::timeout(std::time::Duration::from_secs(2), ns_watch.next())
            .await
            .expect("namespace created in the gap was never delivered")
            .unwrap()
            .unwrap();
        assert_eq!(
            namespace_event_key(&ev).as_deref(),
            Some("serviceaccounts/gap-ns/default")
        );
    }
}
