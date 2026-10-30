//! Unit tests moved from `src/encryption.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use aes_gcm::{
        aead::{Aead, KeyInit, OsRng},
        Aes256Gcm, Nonce,
    };
    pub use anyhow::{anyhow, Result};
    pub use base64::{engine::general_purpose, Engine as _};
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::{debug, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::encryption::*;

    #[test]
    fn test_aes_gcm_encrypt_decrypt() {
        let key = AesGcmProvider::generate_key();
        let provider = AesGcmProvider::new(&key, "test-key".to_string()).unwrap();

        let plaintext = b"Hello, World!";
        let ciphertext = provider.encrypt(plaintext).unwrap();

        assert_ne!(plaintext.to_vec(), ciphertext);

        let decrypted = provider.decrypt(&ciphertext).unwrap();
        assert_eq!(plaintext.to_vec(), decrypted);
    }

    #[test]
    fn test_aes_gcm_from_base64() {
        let key = AesGcmProvider::generate_key();
        let encoded = general_purpose::STANDARD.encode(key);

        let provider = AesGcmProvider::from_base64(&encoded, "test-key".to_string()).unwrap();

        let plaintext = b"Test data";
        let ciphertext = provider.encrypt(plaintext).unwrap();
        let decrypted = provider.decrypt(&ciphertext).unwrap();

        assert_eq!(plaintext.to_vec(), decrypted);
    }

    #[test]
    fn test_identity_provider() {
        let provider = IdentityProvider;

        let plaintext = b"Hello, World!";
        let ciphertext = provider.encrypt(plaintext).unwrap();

        assert_eq!(plaintext.to_vec(), ciphertext);

        let decrypted = provider.decrypt(&ciphertext).unwrap();
        assert_eq!(plaintext.to_vec(), decrypted);
    }

    #[test]
    fn test_encryption_transformer() {
        let mut transformer = EncryptionTransformer::new();

        let key = AesGcmProvider::generate_key();
        let provider = Arc::new(AesGcmProvider::new(&key, "test-key".to_string()).unwrap());

        transformer.add_provider("test-key".to_string(), provider);
        transformer.set_resource_provider("secrets".to_string(), "test-key".to_string());

        let plaintext = b"Secret data";

        // Encrypt
        let ciphertext = transformer
            .encrypt_for_resource("secrets", plaintext)
            .unwrap();
        assert_ne!(plaintext.to_vec(), ciphertext);

        // Decrypt
        let decrypted = transformer
            .decrypt_for_resource("secrets", &ciphertext)
            .unwrap();
        assert_eq!(plaintext.to_vec(), decrypted);

        // Non-encrypted resource
        let other_data = b"Other data";
        let result = transformer
            .encrypt_for_resource("pods", other_data)
            .unwrap();
        assert_eq!(other_data.to_vec(), result);
    }

    #[test]
    fn test_should_encrypt() {
        let mut transformer = EncryptionTransformer::new();
        transformer.set_resource_provider("secrets".to_string(), "aes".to_string());

        assert!(transformer.should_encrypt("secrets"));
        assert!(!transformer.should_encrypt("pods"));
    }
}
