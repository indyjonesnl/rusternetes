//! TLS acceptor that surfaces the verified client-certificate chain to handlers.
//!
//! The implementation is shared with the kubelet and lives in
//! `k3rs_middleware::secure_serving::serve`; this module keeps the
//! api-server's path and adds the glue from its reloading
//! [`CaContentProvider`]s to the acceptor's `x509.VerifyOptionsFn` closures.

use std::sync::Arc;

use crate::dynamic_certificates::CaContentProvider;
pub use k3rs_middleware::secure_serving::serve::{PeerCertAcceptor, VerifierFn};

/// `x509.NewDynamic(verifyOptionsFn, ..)`'s function over a provider: the
/// verifier for the provider's current bundle, read per connection
/// (x509.go:229-255).
pub fn verifier_fn(provider: Arc<dyn CaContentProvider>) -> VerifierFn {
    Arc::new(move || provider.verifier())
}
