//! `RangeAllocation` (`staging/src/k8s.io/api/core/v1/types.go:8325-8340`):
//! the persisted snapshot of a range allocator — the service NodePort
//! bitmap. It is not served by the API; the api-server keeps it in storage
//! under `/registry/ranges/`.

use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RangeAllocation {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// The range `data` covers, e.g. `30000-32767`.
    #[serde(default)]
    pub range: String,
    /// The allocation bit array, base64 in JSON as Go encodes `[]byte`.
    #[serde(
        default,
        serialize_with = "crate::serde_helpers::bytes_b64::serialize",
        deserialize_with = "crate::serde_helpers::bytes_b64::deserialize"
    )]
    pub data: Vec<u8>,
}
