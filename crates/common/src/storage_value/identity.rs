//! Port of `k8s.io/apiserver/pkg/storage/value/encrypt/identity/identity.go`.

use super::{Context, Error, Read, Transformer, Write};

/// `encryptedPrefix`: prefix `k8s:enc:` is reserved for encrypted data on disk.
const ENCRYPTED_PREFIX: &[u8] = b"k8s:enc:";

/// `errEncryptedData`.
const ERR_ENCRYPTED_DATA: &str = "identity transformer tried to read encrypted data";

/// `identityTransformer` performs no transformation on provided data, but
/// validates that the data is not encrypted data during `TransformFromStorage`.
struct IdentityTransformer;

/// `NewEncryptCheckTransformer` returns an identityTransformer which returns an
/// error on attempts to read encrypted data.
pub fn new_encrypt_check_transformer() -> Box<dyn Transformer> {
    Box::new(IdentityTransformer)
}

impl Read for IdentityTransformer {
    /// `TransformFromStorage` returns the input bytes if the data is not encrypted.
    fn transform_from_storage(
        &self,
        data: &[u8],
        _data_ctx: &dyn Context,
    ) -> Result<(Vec<u8>, bool), Error> {
        // identityTransformer has to return an error if the data is encoded using another transformer.
        // JSON data starts with '{'. Protobuf data has a prefix 'k8s[\x00-\xFF]'.
        // Prefix 'k8s:enc:' is reserved for encrypted data on disk.
        if data.starts_with(ENCRYPTED_PREFIX) {
            return Err(Error::new(ERR_ENCRYPTED_DATA));
        }
        Ok((data.to_vec(), false))
    }
}

impl Write for IdentityTransformer {
    fn transform_to_storage(&self, data: &[u8], _data_ctx: &dyn Context) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }
}
