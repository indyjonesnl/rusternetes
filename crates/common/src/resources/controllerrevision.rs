use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// ControllerRevision represents an immutable snapshot of state data
/// Used by controllers like StatefulSet and DaemonSet to track rollback history
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControllerRevision {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    // ObjectMeta is `omitempty` in upstream Kubernetes and the apiserver fills in
    // defaults, so tolerate a body that omits it rather than rejecting with a 422.
    // `#[serde(default)]` also guards against the serde-flatten quirk where a
    // failure in a sibling field gets misreported as `missing field metadata`.
    #[serde(default)]
    pub metadata: ObjectMeta,

    /// Data is the serialized representation of the state
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,

    /// Revision indicates the revision of the state represented by Data
    #[serde(default)]
    pub revision: i64,
}

impl ControllerRevision {
    pub fn new(name: impl Into<String>, namespace: impl Into<String>, revision: i64) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "ControllerRevision".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            data: None,
            revision,
        }
    }

    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }
}
