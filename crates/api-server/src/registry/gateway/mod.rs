//! Strategies and stores for `gateway.networking.k8s.io`.
//!
//! ReferenceGrant is not a Kubernetes API: it is a CRD of
//! kubernetes-sigs/gateway-api, so `../kubernetes` has no `pkg/registry` for
//! it. Upstream serves it through the apiextensions-apiserver's generic
//! custom-resource store, whose strategy
//! (`staging/src/k8s.io/apiextensions-apiserver/pkg/registry/customresource/strategy.go`)
//! is what the strategy here ports, for the subresources the CRD declares
//! (none). The schema rules are in
//! `rusternetes_common::validation::referencegrant`.

pub mod referencegrant;

/// `gateway.networking.k8s.io`.
pub const GROUP: &str = "gateway.networking.k8s.io";
