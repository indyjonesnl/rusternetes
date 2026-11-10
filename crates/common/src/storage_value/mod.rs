//! Port of `k8s.io/apiserver/pkg/storage/value` (`transformer.go`).
//!
//! Methods for transforming values before they are written to, or after they
//! are read from, the underlying store (encryption at rest and friends).
//!
//! Sub-packages mirror upstream's `storage/value/encrypt/*`:
//! - [`identity`] -> `encrypt/identity/identity.go`
//! - [`aes`]      -> `encrypt/aes/aes.go` (the AES-GCM half)
//!
//! Deliberate differences from Go (Rust has no ambient `context.Context`):
//! the `ctx` parameter of every method is dropped. In upstream it only carries
//! the `RequestInfo` used to label `apiserver_storage_transformation_*`
//! metrics and the level-6 error log; neither is ported yet (see the PR for the
//! tracked deferrals), so the error log here has no request attributes.

pub mod aes;
pub mod identity;

use std::fmt;

/// Error produced by a [`Transformer`]. Upstream returns plain Go `error`
/// values; only their message is observable, so a message-carrying type is the
/// equivalent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl Error {
    pub fn new(message: impl Into<String>) -> Self {
        Error(message.into())
    }

    pub fn message(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

/// `value.Context`: additional information that a storage transformation may
/// need to verify the data at rest.
pub trait Context {
    /// `AuthenticatedData` should return an array of bytes that describes the
    /// current value. If the value changes, the transformer may report the
    /// value as unreadable or tampered. This may be empty if no such
    /// description exists or is needed. For additional verification, set this
    /// to data that strongly identifies the value, such as the key and
    /// creation version of the stored data.
    fn authenticated_data(&self) -> &[u8];
}

/// `value.DefaultContext`: a simple implementation of [`Context`] for a slice
/// of bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DefaultContext(pub Vec<u8>);

impl DefaultContext {
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        DefaultContext(data.into())
    }
}

impl Context for DefaultContext {
    /// `AuthenticatedData` returns itself.
    fn authenticated_data(&self) -> &[u8] {
        &self.0
    }
}

/// `value.Read`.
pub trait Read: Send + Sync {
    /// `TransformFromStorage` may transform the provided data from its
    /// underlying storage representation or return an error. The returned bool
    /// is `stale`: true if the object on disk is stale and a write to etcd
    /// should be issued, even if the contents of the object have not changed.
    fn transform_from_storage(
        &self,
        data: &[u8],
        data_ctx: &dyn Context,
    ) -> Result<(Vec<u8>, bool), Error>;
}

/// `value.Write`.
pub trait Write: Send + Sync {
    /// `TransformToStorage` may transform the provided data into the
    /// appropriate form in storage or return an error.
    fn transform_to_storage(&self, data: &[u8], data_ctx: &dyn Context) -> Result<Vec<u8>, Error>;
}

/// `value.Transformer` allows a value to be transformed before being read from
/// or written to the underlying store. The methods must be able to undo the
/// transformation caused by the other.
pub trait Transformer: Read + Write {}

impl<T: Read + Write + ?Sized> Transformer for T {}

/// `value.PrefixTransformer` holds a transformer and the prefix that the
/// transformation is located under.
pub struct PrefixTransformer {
    pub prefix: Vec<u8>,
    pub transformer: Box<dyn Transformer>,
}

struct PrefixTransformers {
    transformers: Vec<PrefixTransformer>,
    err: Error,
}

/// `value.NewPrefixTransformers` supports the [`Transformer`] interface by
/// checking the incoming data against the provided prefixes in order. The
/// first matching prefix will be used to transform the value (the prefix is
/// stripped before the Transformer interface is invoked). The first provided
/// transformer will be used when writing to the store.
///
/// `err` is returned when no prefix matches; `None` selects upstream's default
/// message.
pub fn new_prefix_transformers(
    err: Option<Error>,
    transformers: Vec<PrefixTransformer>,
) -> Box<dyn Transformer> {
    let err = err.unwrap_or_else(|| {
        Error::new("the provided value does not match any of the supported transformers")
    });
    Box::new(PrefixTransformers { transformers, err })
}

/// `errors.Reduce(errors.NewAggregate(errs))` from
/// `k8s.io/apimachinery/pkg/util/errors`: nothing for an empty list, the error
/// itself for one, otherwise an aggregate whose message is `[a, b]` over the
/// de-duplicated messages (a single distinct message is not bracketed).
fn reduce_aggregate(errs: Vec<Error>) -> Option<Error> {
    match errs.len() {
        0 => None,
        1 => errs.into_iter().next(),
        _ => {
            let mut seen: Vec<&str> = Vec::new();
            for e in &errs {
                if !seen.contains(&e.message()) {
                    seen.push(e.message());
                }
            }
            let joined = seen.join(", ");
            Some(Error::new(if seen.len() == 1 {
                joined
            } else {
                format!("[{joined}]")
            }))
        }
    }
}

impl Read for PrefixTransformers {
    /// `TransformFromStorage` finds the first transformer with a prefix
    /// matching the provided data and returns the result of transforming the
    /// value. It will always mark any transformation as stale that is not
    /// using the first transformer.
    fn transform_from_storage(
        &self,
        data: &[u8],
        data_ctx: &dyn Context,
    ) -> Result<(Vec<u8>, bool), Error> {
        let mut errs: Vec<Error> = Vec::new();
        for (i, transformer) in self.transformers.iter().enumerate() {
            if data.starts_with(&transformer.prefix) {
                let result = transformer
                    .transformer
                    .transform_from_storage(&data[transformer.prefix.len()..], data_ctx);
                // To migrate away from encryption, user can specify an identity transformer higher up
                // (in the config file) than the encryption transformer. In that scenario, the identity transformer needs to
                // identify (during reads from disk) whether the data being read is encrypted or not. If the data is encrypted,
                // it shall throw an error, but that error should not prevent the next subsequent transformer from being tried.
                if transformer.prefix.is_empty() && result.is_err() {
                    continue;
                }
                // It is valid to have overlapping prefixes when the same encryption provider
                // is specified multiple times but with different keys (the first provider is
                // being rotated to and some later provider is being rotated away from).
                // The transformers for both aescbc configs share the prefix k8s:enc:aescbc:v1:
                // but a failure in the first one should not prevent a later match from being attempted.
                // Thus we never short-circuit on a prefix match that results in an error.
                match result {
                    Err(err) => {
                        errs.push(err);
                        continue;
                    }
                    Ok((out, stale)) => return Ok((out, stale || i != 0)),
                }
            }
        }
        if let Some(err) = reduce_aggregate(errs) {
            log_transform_err(&err, "failed to decrypt data");
            return Err(err);
        }
        Err(self.err.clone())
    }
}

impl Write for PrefixTransformers {
    /// `TransformToStorage` uses the first transformer and adds its prefix to
    /// the data.
    fn transform_to_storage(&self, data: &[u8], data_ctx: &dyn Context) -> Result<Vec<u8>, Error> {
        let transformer = &self.transformers[0];
        let result = match transformer.transformer.transform_to_storage(data, data_ctx) {
            Ok(r) => r,
            Err(err) => {
                log_transform_err(&err, "failed to encrypt data");
                return Err(err);
            }
        };
        let mut prefixed = Vec::with_capacity(result.len() + transformer.prefix.len());
        prefixed.extend_from_slice(&transformer.prefix);
        prefixed.extend_from_slice(&result);
        Ok(prefixed)
    }
}

/// `logTransformErr`. Upstream logs the request attributes at klog level 6 and
/// otherwise `klog.ErrorS`; request info is not available here (see module
/// docs).
fn log_transform_err(err: &Error, message: &str) {
    tracing::error!(err = %err, "{message}");
}
