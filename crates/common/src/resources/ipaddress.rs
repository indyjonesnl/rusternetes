use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};

/// IPAddress represents a single IP of a single IP Family. The object is designed to be used
/// by APIs that operate on IP addresses. New in Kubernetes v1.35.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IPAddress {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<IPAddressSpec>,
}

impl IPAddress {
    pub fn new(name: impl Into<String>, parent_ref: ParentReference) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "IPAddress".to_string(),
                api_version: "networking.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec: Some(IPAddressSpec {
                parent_ref: Some(parent_ref),
            }),
        }
    }
}

/// IPAddressSpec describe the attributes in an IP Address
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IPAddressSpec {
    /// ParentRef references the resource that an IPAddress is attached to. An IPAddress must
    /// reference a parent object.
    ///
    /// Upstream's field is a pointer (`ParentRef *ParentReference`,
    /// `staging/src/k8s.io/api/networking/v1/types.go:669`) and an absent one is
    /// `field.Required(fldPath.Child("parentRef"))`
    /// (`pkg/apis/networking/validation/validation.go:772`), so `Option` is the
    /// faithful shape: defaulting to an empty `ParentReference` would answer
    /// with `resource`/`name` required instead of naming `parentRef` (#1939).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_ref: Option<ParentReference>,
}

/// ParentReference describes a reference to a parent object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParentReference {
    /// Group is the group of the object being referenced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,

    /// Resource is the resource of the object being referenced.
    #[serde(default)]
    pub resource: String,

    /// Namespace is the namespace of the object being referenced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Name is the name of the object being referenced.
    #[serde(default)]
    pub name: String,

    /// UID is the UID of the object being referenced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}
