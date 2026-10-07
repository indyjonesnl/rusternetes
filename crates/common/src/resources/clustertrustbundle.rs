//! `certificates.k8s.io/v1beta1` ClusterTrustBundle, ported from
//! `staging/src/k8s.io/api/certificates/v1beta1/types.go:275-356`.
use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};

/// ClusterTrustBundle is a cluster-scoped container for X.509 trust anchors
/// (root certificates). It can optionally be associated with a signer, in
/// which case it holds one valid set of trust anchors for that signer.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterTrustBundle {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// `Spec ClusterTrustBundleSpec json:"spec"` (types.go:301): not a pointer.
    #[serde(default)]
    pub spec: ClusterTrustBundleSpec,
}

/// ClusterTrustBundleSpec contains the signer and trust anchors
/// (types.go:304-340).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterTrustBundleSpec {
    /// signerName indicates the associated signer, if any. List/watch
    /// requests can filter on it with a `spec.signerName=NAME` field
    /// selector (types.go:322-326).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub signer_name: String,
    /// PEM bundle of PEM-wrapped, DER-formatted X.509 CA certificates.
    #[serde(default)]
    pub trust_bundle: String,
}

impl ClusterTrustBundle {
    pub const API_VERSION: &'static str = "certificates.k8s.io/v1beta1";

    pub fn new(name: impl Into<String>, signer_name: &str, trust_bundle: &str) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "ClusterTrustBundle".to_string(),
                api_version: Self::API_VERSION.to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec: ClusterTrustBundleSpec {
                signer_name: signer_name.to_string(),
                trust_bundle: trust_bundle.to_string(),
            },
        }
    }
}
