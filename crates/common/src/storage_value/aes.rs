//! Port of the AES-GCM half of
//! `k8s.io/apiserver/pkg/storage/value/encrypt/aes/aes.go`: transforms values
//! for storage at rest using AES-GCM.
//!
//! Not ported here (tracked as deferred in the PR):
//! - `NewCBCTransformer` (same file): needs a CBC mode crate (`cbc`), which is
//!   not in the dependency tree.
//! - `aes_extended_nonce.go` / `cache.go` (HKDF extended-nonce GCM): needs
//!   `hkdf` as a direct dependency.
//! - `value.RecordDataKeyGeneration` in `GenerateKey` (metrics).

use std::sync::atomic::{AtomicU64, Ordering};

use aes_gcm::aead::consts::U12;
use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, OsRng, Payload};
use aes_gcm::aes::{Aes128, Aes192, Aes256};
use aes_gcm::{AesGcm, KeyInit, Nonce};

use super::{Context, Error, Read, Transformer, Write};

/// `commonSize` is the length of various security sensitive byte slices such
/// as encryption keys. Do not change this value. It would be a backward
/// incompatible change.
const COMMON_SIZE: usize = 32;

const KEY_SIZE_COUNTER_NONCE_GCM: usize = COMMON_SIZE;

/// The GCM nonce size. "all data in etcd will be broken if this ever changes"
/// (`newGCM`).
pub const GCM_NONCE_SIZE: usize = 12;

/// GCM tag size (`aead.Overhead()`).
const GCM_OVERHEAD: usize = 16;

type Aes128Gcm12 = AesGcm<Aes128, U12>;
type Aes192Gcm12 = AesGcm<Aes192, U12>;
type Aes256Gcm12 = AesGcm<Aes256, U12>;

/// `cipher.Block` for AES, i.e. the result of Go's `aes.NewCipher(key)`.
#[derive(Clone)]
pub struct Block {
    key: Vec<u8>,
}

/// `aes.NewCipher`: the key must be 16, 24 or 32 bytes (AES-128/192/256).
pub fn new_cipher(key: &[u8]) -> Result<Block, Error> {
    match key.len() {
        16 | 24 | 32 => Ok(Block { key: key.to_vec() }),
        n => Err(Error::new(format!("crypto/aes: invalid key size {n}"))),
    }
}

/// `cipher.AEAD` for the GCM mode with a 12 byte nonce.
enum GcmAead {
    Aes128(Box<Aes128Gcm12>),
    Aes192(Box<Aes192Gcm12>),
    Aes256(Box<Aes256Gcm12>),
}

impl GcmAead {
    fn seal(&self, nonce: &[u8], plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, Error> {
        let nonce = Nonce::<U12>::from_slice(nonce);
        let payload = Payload {
            msg: plaintext,
            aad,
        };
        let r = match self {
            GcmAead::Aes128(a) => a.encrypt(nonce, payload),
            GcmAead::Aes192(a) => a.encrypt(nonce, payload),
            GcmAead::Aes256(a) => a.encrypt(nonce, payload),
        };
        r.map_err(|_| Error::new("cipher: message too large"))
    }

    fn open(&self, nonce: &[u8], ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>, Error> {
        let nonce = Nonce::<U12>::from_slice(nonce);
        let payload = Payload {
            msg: ciphertext,
            aad,
        };
        let r = match self {
            GcmAead::Aes128(a) => a.decrypt(nonce, payload),
            GcmAead::Aes192(a) => a.decrypt(nonce, payload),
            GcmAead::Aes256(a) => a.decrypt(nonce, payload),
        };
        r.map_err(|_| Error::new("cipher: message authentication failed"))
    }
}

/// `newGCM`.
fn new_gcm(block: &Block) -> Result<GcmAead, Error> {
    let key = block.key.as_slice();
    let bad = |_| Error::new(format!("crypto/aes: invalid key size {}", key.len()));
    Ok(match key.len() {
        16 => GcmAead::Aes128(Box::new(Aes128Gcm12::new_from_slice(key).map_err(bad)?)),
        24 => GcmAead::Aes192(Box::new(Aes192Gcm12::new_from_slice(key).map_err(bad)?)),
        _ => GcmAead::Aes256(Box::new(Aes256Gcm12::new_from_slice(key).map_err(bad)?)),
    })
}

type NonceFunc = Box<dyn Fn(&mut [u8]) -> Result<(), Error> + Send + Sync>;

struct Gcm {
    aead: GcmAead,
    nonce_func: NonceFunc,
}

/// `randomNonce`.
fn random_nonce(b: &mut [u8]) -> Result<(), Error> {
    OsRng
        .try_fill_bytes(b)
        .map_err(|e| Error::new(e.to_string()))
}

/// `nonceGenerator`. Even at one million encryptions per second, this counter
/// is enough for half a million years.
pub struct NonceGenerator {
    pub nonce: AtomicU64,
    pub zero: u64,
    /// `fatal`: called when the counter rolls over. Unrecoverable upstream.
    pub fatal: Box<dyn Fn(&str) + Send + Sync>,
}

impl NonceGenerator {
    /// `next`: writes the next counter value little-endian into `b` (8 bytes).
    fn next(&self, b: &mut [u8]) {
        let incrementing_nonce = self.nonce.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
        if incrementing_nonce <= self.zero {
            // this should never happen, and is unrecoverable if it does
            (self.fatal)("aes-gcm detected nonce overflow - cryptographic wear out has occurred");
        }
        b[..8].copy_from_slice(&incrementing_nonce.to_le_bytes());
    }
}

/// `die`: upstream calls `klog.FatalDepth`, which logs and exits the process
/// with status 255.
fn die(msg: &str) {
    tracing::error!("{msg}");
    std::process::exit(255);
}

/// `GenerateKey` generates a random key using system randomness.
pub fn generate_key(length: usize) -> Result<Vec<u8>, Error> {
    let mut key = vec![0u8; length];
    random_nonce(&mut key)?;
    Ok(key)
}

/// `NewGCMTransformerWithUniqueKeyUnsafe` is the same as
/// [`new_gcm_transformer`] but is unsafe for general use because it makes
/// assumptions about the key underlying the block cipher. Specifically, it
/// uses a 96-bit nonce where the first 32 bits are random data and the
/// remaining 64 bits are a monotonically incrementing atomic counter. This
/// means that the key must be randomly generated on process startup and must
/// never be used for encryption outside the lifetime of the process. Unlike
/// [`new_gcm_transformer`], this function is immune to the birthday attack and
/// thus the key can be used for 2^64-1 writes without rotation. To prevent the
/// chance of the block cipher being accidentally re-used, it is not taken in
/// as input; a new random key is generated and returned on every invocation.
/// If the key is stored and retrieved at a later point, it can be passed to
/// `new_gcm_transformer(new_cipher(key))` to construct a transformer capable
/// of decrypting values encrypted by this transformer (that transformer must
/// not be used for encryption).
pub fn new_gcm_transformer_with_unique_key_unsafe() -> Result<(Box<dyn Transformer>, Vec<u8>), Error>
{
    let key = generate_key(KEY_SIZE_COUNTER_NONCE_GCM)?;
    let block = new_cipher(&key)?;

    let nonce_gen = NonceGenerator {
        // we start the nonce counter at one billion so that we are
        // guaranteed to detect rollover across different go routines
        zero: 1_000_000_000,
        nonce: AtomicU64::new(1_000_000_000),
        fatal: Box::new(die),
    };

    let transformer = new_gcm_transformer_with_unique_key_unsafe_gen(&block, nonce_gen)?;
    Ok((transformer, key))
}

/// `newGCMTransformerWithUniqueKeyUnsafe` (exposed for the upstream overflow
/// test, which injects its own generator).
pub fn new_gcm_transformer_with_unique_key_unsafe_gen(
    block: &Block,
    nonce_gen: NonceGenerator,
) -> Result<Box<dyn Transformer>, Error> {
    let aead = new_gcm(block)?;

    let nonce_func: NonceFunc = Box::new(move |b: &mut [u8]| {
        // we only need 8 bytes to store our 64 bit incrementing nonce
        // instead of leaving the unused bytes as zeros, set those to random bits
        // this mostly protects us from weird edge cases like a VM restore that rewinds our atomic counter
        let rand_nonce_size = b.len() - 8;

        random_nonce(&mut b[..rand_nonce_size])?;

        nonce_gen.next(&mut b[rand_nonce_size..]);

        Ok(())
    });

    Ok(Box::new(Gcm { aead, nonce_func }))
}

/// `NewGCMTransformer` takes the given block cipher and performs encryption
/// and decryption on the given data. It implements AEAD encryption of the
/// provided values given a cipher.Block algorithm. The authenticated data
/// provided as part of the [`Context`] must match when the same value is set
/// to and loaded from storage. In order to ensure that values cannot be copied
/// by an attacker from a location under their control, use characteristics of
/// the storage location (such as the etcd key) as part of the authenticated
/// data.
///
/// Because this mode requires a generated IV and IV reuse is a known weakness
/// of AES-GCM, keys must be rotated before a birthday attack becomes
/// feasible. NIST SP 800-38D recommends using the same key with random 96-bit
/// nonces (the default nonce length) no more than 2^32 times, and therefore
/// transformers using this implementation *must* ensure they allow for
/// frequent key rotation.
pub fn new_gcm_transformer(block: &Block) -> Result<Box<dyn Transformer>, Error> {
    let aead = new_gcm(block)?;
    Ok(Box::new(Gcm {
        aead,
        nonce_func: Box::new(random_nonce),
    }))
}

impl Read for Gcm {
    fn transform_from_storage(
        &self,
        data: &[u8],
        data_ctx: &dyn Context,
    ) -> Result<(Vec<u8>, bool), Error> {
        if data.len() < GCM_NONCE_SIZE {
            return Err(Error::new(
                "the stored data was shorter than the required size",
            ));
        }
        let result = self.aead.open(
            &data[..GCM_NONCE_SIZE],
            &data[GCM_NONCE_SIZE..],
            data_ctx.authenticated_data(),
        )?;
        Ok((result, false))
    }
}

impl Write for Gcm {
    fn transform_to_storage(&self, data: &[u8], data_ctx: &dyn Context) -> Result<Vec<u8>, Error> {
        let mut nonce = [0u8; GCM_NONCE_SIZE];
        (self.nonce_func)(&mut nonce)
            .map_err(|e| Error::new(format!("failed to write nonce for AES-GCM: {e}")))?;

        let cipher_text = self
            .aead
            .seal(&nonce, data, data_ctx.authenticated_data())?;
        let mut result = Vec::with_capacity(GCM_NONCE_SIZE + GCM_OVERHEAD + data.len());
        result.extend_from_slice(&nonce);
        result.extend_from_slice(&cipher_text);
        Ok(result)
    }
}
