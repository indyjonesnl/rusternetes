//! Ports of the upstream tests for `k8s.io/apiserver/pkg/storage/value`:
//! - `transformer_test.go` (`TestPrefixFrom`, `TestPrefixTo`; the metric and
//!   log-format tests depend on unported metrics/request-info, deferred)
//! - `encrypt/aes/aes_test.go` (`TestGCMDataStable`, `TestGCMUnsafeNonceOverflow`,
//!   `TestGCMUnsafeCompatibility`, `TestGCMLegacyDataCompatibility`,
//!   `TestGCMUnsafeNonceGen`, `TestGCMNonce`, `TestGCMKeyRotation`; the CBC and
//!   extended-nonce variants are deferred with their implementations)
//! - `encrypt/identity` behaviour.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use rusternetes_common::storage_value::aes::{
    new_cipher, new_gcm_transformer, new_gcm_transformer_with_unique_key_unsafe,
    new_gcm_transformer_with_unique_key_unsafe_gen, NonceGenerator, GCM_NONCE_SIZE,
};
use rusternetes_common::storage_value::identity::new_encrypt_check_transformer;
use rusternetes_common::storage_value::{
    new_prefix_transformers, Context, DefaultContext, Error, PrefixTransformer, Read, Transformer,
    Write,
};

/// `testTransformer` from transformer_test.go.
#[derive(Default)]
struct TestTransformer {
    from: Vec<u8>,
    to: Vec<u8>,
    err: Option<Error>,
    stale: bool,
    received_from: Arc<Mutex<Vec<u8>>>,
    received_to: Arc<Mutex<Vec<u8>>>,
}

impl Read for TestTransformer {
    fn transform_from_storage(
        &self,
        data: &[u8],
        _ctx: &dyn Context,
    ) -> Result<(Vec<u8>, bool), Error> {
        *self.received_from.lock().unwrap() = data.to_vec();
        match &self.err {
            Some(e) => Err(e.clone()),
            None => Ok((self.from.clone(), self.stale)),
        }
    }
}

impl Write for TestTransformer {
    fn transform_to_storage(&self, data: &[u8], _ctx: &dyn Context) -> Result<Vec<u8>, Error> {
        *self.received_to.lock().unwrap() = data.to_vec();
        match &self.err {
            Some(e) => Err(e.clone()),
            None => Ok(self.to.clone()),
        }
    }
}

fn dctx(s: &str) -> DefaultContext {
    DefaultContext::new(s.as_bytes())
}

#[test]
fn test_prefix_from() {
    let test_err = Error::new("test error");
    let transform_err = Error::new("transform error");
    let mk = |prefix: &str, t: TestTransformer| {
        let recv = t.received_from.clone();
        (
            PrefixTransformer {
                prefix: prefix.as_bytes().to_vec(),
                transformer: Box::new(t),
            },
            recv,
        )
    };
    let (t0, r0) = mk(
        "first:",
        TestTransformer {
            from: b"value1".to_vec(),
            ..Default::default()
        },
    );
    let (t1, r1) = mk(
        "second:",
        TestTransformer {
            from: b"value2".to_vec(),
            ..Default::default()
        },
    );
    let (t2, r2) = mk(
        "fails:",
        TestTransformer {
            err: Some(transform_err.clone()),
            ..Default::default()
        },
    );
    let (t3, r3) = mk(
        "stale:",
        TestTransformer {
            from: b"value3".to_vec(),
            stale: true,
            ..Default::default()
        },
    );
    let recv = [r0, r1, r2, r3];
    let p = new_prefix_transformers(Some(test_err.clone()), vec![t0, t1, t2, t3]);

    type Case<'a> = (&'a [u8], Option<&'a [u8]>, bool, Option<Error>, i32);
    let cases: Vec<Case> = vec![
        (b"first:value", Some(b"value1"), false, None, 0),
        (b"second:value", Some(b"value2"), true, None, 1),
        (b"third:value", None, false, Some(test_err.clone()), -1),
        (b"fails:value", None, false, Some(transform_err.clone()), 2),
        (b"stale:value", Some(b"value3"), true, None, 3),
    ];
    for (i, (input, expect, stale, err, matched)) in cases.into_iter().enumerate() {
        let got = p.transform_from_storage(input, &DefaultContext::default());
        match (got, err) {
            (Ok((out, s)), None) => {
                assert_eq!(Some(out.as_slice()), expect, "case {i}");
                assert_eq!(s, stale, "case {i}");
            }
            (Err(e), Some(want)) => assert_eq!(e, want, "case {i}"),
            (other, want) => panic!("case {i}: unexpected {other:?} want err {want:?}"),
        }
        if matched != -1 {
            assert_eq!(
                recv[matched as usize].lock().unwrap().as_slice(),
                b"value",
                "case {i}: unexpected value received by transformer"
            );
        }
    }
}

#[test]
fn test_prefix_to() {
    let test_err = Error::new("test error");
    let transform_err = Error::new("test error");
    type ToCase<'a> = (&'a str, TestTransformer, Option<&'a [u8]>, Option<Error>);
    let cases: Vec<ToCase> = vec![
        (
            "first:",
            TestTransformer {
                to: b"value1".to_vec(),
                ..Default::default()
            },
            Some(b"first:value1"),
            None,
        ),
        (
            "second:",
            TestTransformer {
                to: b"value2".to_vec(),
                ..Default::default()
            },
            Some(b"second:value2"),
            None,
        ),
        (
            "fails:",
            TestTransformer {
                err: Some(transform_err.clone()),
                ..Default::default()
            },
            None,
            Some(transform_err.clone()),
        ),
    ];
    for (i, (prefix, t, expect, err)) in cases.into_iter().enumerate() {
        let recv = t.received_to.clone();
        let p = new_prefix_transformers(
            Some(test_err.clone()),
            vec![PrefixTransformer {
                prefix: prefix.as_bytes().to_vec(),
                transformer: Box::new(t),
            }],
        );
        let got = p.transform_to_storage(b"value", &DefaultContext::default());
        match (got, err) {
            (Ok(out), None) => assert_eq!(Some(out.as_slice()), expect, "case {i}"),
            (Err(e), Some(want)) => assert_eq!(e, want, "case {i}"),
            (other, want) => panic!("case {i}: unexpected {other:?} want {want:?}"),
        }
        assert_eq!(recv.lock().unwrap().as_slice(), b"value", "case {i}");
    }
}

#[test]
fn test_prefix_default_error() {
    let p = new_prefix_transformers(None, vec![]);
    let e = p
        .transform_from_storage(b"x", &DefaultContext::default())
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        "the provided value does not match any of the supported transformers"
    );
}

/// Upstream's identity-prefix comment: an identity transformer ahead of an
/// encryption transformer must not stop the later transformer being tried.
#[test]
fn test_prefix_identity_error_does_not_short_circuit() {
    let key = b"abcdefghijklmnopabcdefghijklmnop";
    let gcm = new_gcm_transformer(&new_cipher(key).unwrap()).unwrap();
    let ctx = dctx("authenticated_data");
    let enc = new_prefix_transformers(
        None,
        vec![PrefixTransformer {
            prefix: b"k8s:enc:aesgcm:v1:key1:".to_vec(),
            transformer: gcm,
        }],
    );
    let stored = enc.transform_to_storage(b"secret", &ctx).unwrap();
    assert!(stored.starts_with(b"k8s:enc:aesgcm:v1:key1:"));

    let gcm2 = new_gcm_transformer(&new_cipher(key).unwrap()).unwrap();
    let migrating = new_prefix_transformers(
        None,
        vec![
            PrefixTransformer {
                prefix: vec![],
                transformer: new_encrypt_check_transformer(),
            },
            PrefixTransformer {
                prefix: b"k8s:enc:aesgcm:v1:key1:".to_vec(),
                transformer: gcm2,
            },
        ],
    );
    let (out, stale) = migrating.transform_from_storage(&stored, &ctx).unwrap();
    assert_eq!(out, b"secret");
    // matched the second transformer => stale
    assert!(stale);
}

#[test]
fn test_identity_transformer() {
    let t = new_encrypt_check_transformer();
    let ctx = DefaultContext::default();
    assert_eq!(
        t.transform_to_storage(b"{\"a\":1}", &ctx).unwrap(),
        b"{\"a\":1}"
    );
    assert_eq!(
        t.transform_from_storage(b"{\"a\":1}", &ctx).unwrap(),
        (b"{\"a\":1}".to_vec(), false)
    );
    let e = t
        .transform_from_storage(b"k8s:enc:aescbc:v1:k:xyz", &ctx)
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        "identity transformer tried to read encrypted data"
    );
}

#[test]
fn test_gcm_data_stable() {
    // IMPORTANT: if this must change, all previously encrypted data is broken.
    assert_eq!(GCM_NONCE_SIZE, 12);
    let block = new_cipher(b"0123456789abcdef").unwrap();
    let t = new_gcm_transformer(&block).unwrap();
    let out = t.transform_to_storage(b"hello", &dctx("ctx")).unwrap();
    // nonce(12) + plaintext + tag(16)
    assert_eq!(out.len(), 12 + 5 + 16);
    let (_, key) = new_gcm_transformer_with_unique_key_unsafe().unwrap();
    assert_eq!(key.len(), 32);
}

#[test]
fn test_new_cipher_key_sizes() {
    for n in [16usize, 24, 32] {
        let block = new_cipher(&vec![7u8; n]).unwrap();
        let t = new_gcm_transformer(&block).unwrap();
        let c = dctx("a");
        let out = t.transform_to_storage(b"v", &c).unwrap();
        assert_eq!(t.transform_from_storage(&out, &c).unwrap().0, b"v");
    }
    let e = new_cipher(b"short").err().unwrap();
    assert_eq!(e.to_string(), "crypto/aes: invalid key size 5");
}

#[test]
fn test_gcm_short_data_and_tamper() {
    let t = new_gcm_transformer(&new_cipher(b"0123456789abcdef").unwrap()).unwrap();
    let e = t.transform_from_storage(b"tiny", &dctx("a")).unwrap_err();
    assert_eq!(
        e.to_string(),
        "the stored data was shorter than the required size"
    );
    let mut out = t.transform_to_storage(b"value", &dctx("a")).unwrap();
    let last = out.len() - 1;
    out[last] ^= 1;
    let e = t.transform_from_storage(&out, &dctx("a")).unwrap_err();
    assert_eq!(e.to_string(), "cipher: message authentication failed");
}

#[test]
fn test_gcm_unsafe_nonce_overflow() {
    let fatal_msgs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
    let sink = fatal_msgs.clone();
    let nonce_gen = NonceGenerator {
        nonce: AtomicU64::new(0),
        zero: 0,
        fatal: Box::new(move |m| sink.lock().unwrap().push(m.to_string())),
    };
    // The generator is moved into the transformer; share the counter by
    // reading it back through a second handle is not possible, so observe it
    // through the encoded nonce instead (little-endian counter in bytes 4..12).
    let block = new_cipher(b"abcdefghijklmnop").unwrap();
    let t = new_gcm_transformer_with_unique_key_unsafe_gen(&block, nonce_gen).unwrap();
    let ctx = dctx("authenticated_data");
    let counter = |out: &[u8]| u64::from_le_bytes(out[4..12].try_into().unwrap());

    assert_eq!(
        counter(&t.transform_to_storage(b"firstvalue", &ctx).unwrap()),
        1
    );
    assert_eq!(
        counter(&t.transform_to_storage(b"firstvalue", &ctx).unwrap()),
        2
    );
    assert!(fatal_msgs.lock().unwrap().is_empty());
}

#[test]
fn test_gcm_unsafe_nonce_overflow_wraps() {
    let fatal_msgs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
    let sink = fatal_msgs.clone();
    let nonce_gen = NonceGenerator {
        // pretend lots of encryptions occurred: math.MaxUint64 - 1
        nonce: AtomicU64::new(u64::MAX - 1),
        zero: 0,
        fatal: Box::new(move |m| sink.lock().unwrap().push(m.to_string())),
    };
    let block = new_cipher(b"abcdefghijklmnop").unwrap();
    let t = new_gcm_transformer_with_unique_key_unsafe_gen(&block, nonce_gen).unwrap();
    let ctx = dctx("authenticated_data");
    let counter = |out: &[u8]| u64::from_le_bytes(out[4..12].try_into().unwrap());

    assert_eq!(
        counter(&t.transform_to_storage(b"firstvalue", &ctx).unwrap()),
        u64::MAX
    );
    assert!(
        fatal_msgs.lock().unwrap().is_empty(),
        "fatal should not have been called yet"
    );
    t.transform_to_storage(b"firstvalue", &ctx).unwrap();
    let msgs = fatal_msgs.lock().unwrap();
    assert_eq!(msgs.len(), 1, "fatal should have been once");
    assert_eq!(
        msgs[0],
        "aes-gcm detected nonce overflow - cryptographic wear out has occurred"
    );
}

/// TestGCMUnsafeCompatibility: encryptions from the unique-key transformer can
/// be decrypted via the plain GCM transformer.
#[test]
fn test_gcm_unsafe_compatibility() {
    let (encrypt, key) = new_gcm_transformer_with_unique_key_unsafe().unwrap();
    let decrypt = new_gcm_transformer(&new_cipher(&key).unwrap()).unwrap();
    let ctx = dctx("authenticated_data");
    let plaintext = b"firstvalue";
    let ciphertext = encrypt.transform_to_storage(plaintext, &ctx).unwrap();
    assert_ne!(plaintext.as_slice(), ciphertext.as_slice());
    let (again, _) = decrypt.transform_from_storage(&ciphertext, &ctx).unwrap();
    assert_eq!(plaintext.as_slice(), again.as_slice());
}

/// TestGCMLegacyDataCompatibility: recorded output from NewGCMTransformer at
/// upstream commit 3b1fc60d8010dd8b53e97ba80e4710dbb430beee.
#[test]
fn test_gcm_legacy_data_compatibility() {
    let decrypt = new_gcm_transformer(&new_cipher(b"snorlax_awesomes").unwrap()).unwrap();
    const LEGACY: [u8; 47] = [
        0x9f, 0x27, 0xc8, 0xfc, 0xea, 0x8a, 0x58, 0xc4, 0x67, 0xd8, 0xe4, 0x37, 0xdb, 0xf2, 0xd8,
        0x59, 0x55, 0xf9, 0xb4, 0xbd, 0x91, 0x2f, 0x4e, 0xf9, 0x67, 0xd7, 0x88, 0xa0, 0xcb, 0x07,
        0x79, 0x7d, 0xac, 0x0a, 0x3f, 0x0a, 0x08, 0x45, 0x60, 0x5c, 0xa8, 0x5a, 0xc8, 0x56, 0x2b,
        0x4a, 0xe1,
    ];
    let (again, _) = decrypt
        .transform_from_storage(&LEGACY, &dctx("bamboo"))
        .unwrap();
    assert_eq!(again, b"pandas are the best");
}

/// TestGCMUnsafeNonceGen: counter uniqueness under concurrency.
#[test]
fn test_gcm_unsafe_nonce_gen() {
    let nonce_gen = NonceGenerator {
        nonce: AtomicU64::new(0),
        zero: 0,
        fatal: Box::new(|m| panic!("{m}")),
    };
    let block = new_cipher(b"abcdefghijklmnop").unwrap();
    let t: Arc<dyn Transformer> =
        Arc::from(new_gcm_transformer_with_unique_key_unsafe_gen(&block, nonce_gen).unwrap());
    const COUNT: usize = 1_000;
    let counters = Arc::new(Mutex::new(vec![0u64; COUNT]));
    let mut handles = vec![];
    for i in 0..COUNT {
        let t = t.clone();
        let counters = counters.clone();
        handles.push(std::thread::spawn(move || {
            let ctx = dctx("authenticated_data");
            let plaintext = vec![(i % 8) as u8; COUNT];
            let out = t.transform_to_storage(&plaintext, &ctx).unwrap();
            let nonce = &out[..12];
            assert_ne!(&nonce[..4], &[0u8; 4], "got all zeros for random nonce");
            let c = u64::from_le_bytes(nonce[4..].try_into().unwrap());
            counters.lock().unwrap()[(c - 1) as usize] += 1;
            let (again, _) = t.transform_from_storage(&out, &ctx).unwrap();
            assert_eq!(plaintext, again);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert!(counters.lock().unwrap().iter().all(|&c| c == 1));
}

type TransformerFunc = fn(&[u8]) -> Box<dyn Transformer>;

fn gcm_plain(key: &[u8]) -> Box<dyn Transformer> {
    new_gcm_transformer(&new_cipher(key).unwrap()).unwrap()
}

fn gcm_unsafe(key: &[u8]) -> Box<dyn Transformer> {
    let nonce_gen = NonceGenerator {
        nonce: AtomicU64::new(0),
        zero: 0,
        fatal: Box::new(|m| panic!("{m}")),
    };
    new_gcm_transformer_with_unique_key_unsafe_gen(&new_cipher(key).unwrap(), nonce_gen).unwrap()
}

#[test]
fn test_gcm_nonce() {
    let key = b"abcdefghijklmnopabcdefghijklmnop";
    for (name, f, unsafe_counter) in [
        ("gcm", gcm_plain as TransformerFunc, false),
        ("gcm unsafe", gcm_unsafe as TransformerFunc, true),
    ] {
        let t = f(key);
        let ctx = dctx("authenticated_data");
        const COUNT: usize = 1_000;
        for i in 0..COUNT {
            let out = t
                .transform_to_storage(&vec![(i % 8) as u8; COUNT], &ctx)
                .unwrap();
            let nonce = &out[..12];
            assert_ne!(
                &nonce[..4],
                &[0u8; 4],
                "{name}: all zeros for first four bytes"
            );
            if unsafe_counter {
                let counter = u64::from_le_bytes(nonce[4..].try_into().unwrap());
                assert_eq!(counter, (i + 1) as u64, "{name}: counter nonce is invalid");
            } else {
                assert_ne!(nonce, &[0u8; 12], "{name}: got all zeros for nonce");
            }
        }
    }
}

#[test]
fn test_gcm_key_rotation() {
    for (name, f) in [
        ("gcm", gcm_plain as TransformerFunc),
        ("gcm unsafe", gcm_unsafe as TransformerFunc),
    ] {
        let key1 = b"abcdefghijklmnopabcdefghijklmnop";
        let key2 = b"0123456789abcdef0123456789abcdef";
        let test_err = Error::new("test error");
        let ctx = dctx("authenticated_data");
        let pt = |prefix: &str, key: &[u8]| PrefixTransformer {
            prefix: prefix.as_bytes().to_vec(),
            transformer: f(key),
        };

        let p = new_prefix_transformers(
            Some(test_err.clone()),
            vec![pt("first:", key1), pt("second:", key2)],
        );
        let out = p.transform_to_storage(b"firstvalue", &ctx).unwrap();
        assert!(out.starts_with(b"first:"), "{name}: unexpected prefix");
        let (from, stale) = p.transform_from_storage(&out, &ctx).unwrap();
        assert!(!stale && from == b"firstvalue", "{name}");

        // verify changing the context fails storage
        assert!(
            p.transform_from_storage(&out, &dctx("incorrect_context"))
                .is_err(),
            "{name}: expected unauthenticated data"
        );

        // reverse the order, use the second key
        let p = new_prefix_transformers(
            Some(test_err),
            vec![pt("second:", key2), pt("first:", key1)],
        );
        let (from, stale) = p.transform_from_storage(&out, &ctx).unwrap();
        assert!(stale && from == b"firstvalue", "{name}");
    }
}

#[test]
fn test_prefix_aggregates_multiple_errors() {
    // Two transformers sharing a prefix, both failing: errors.Reduce(NewAggregate)
    // yields "[a, b]" for distinct messages and the bare message when equal.
    let mk = |msg: &str| PrefixTransformer {
        prefix: b"k8s:enc:aescbc:v1:".to_vec(),
        transformer: Box::new(TestTransformer {
            err: Some(Error::new(msg)),
            ..Default::default()
        }),
    };
    let p = new_prefix_transformers(None, vec![mk("a"), mk("b")]);
    let e = p
        .transform_from_storage(b"k8s:enc:aescbc:v1:x", &DefaultContext::default())
        .unwrap_err();
    assert_eq!(e.to_string(), "[a, b]");
    let p = new_prefix_transformers(None, vec![mk("a"), mk("a")]);
    let e = p
        .transform_from_storage(b"k8s:enc:aescbc:v1:x", &DefaultContext::default())
        .unwrap_err();
    assert_eq!(e.to_string(), "a");
}
