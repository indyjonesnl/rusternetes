use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};

/// IngressClass represents the class of an Ingress, referenced by the Ingress Spec.
/// The ingressclass.kubernetes.io/is-default-class annotation can be used to indicate
/// that an IngressClass should be considered default.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngressClass {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    #[serde(default)]
    pub metadata: ObjectMeta,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<IngressClassSpec>,
}

impl IngressClass {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "IngressClass".to_string(),
                api_version: "networking.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec: None,
        }
    }

    pub fn with_spec(mut self, spec: IngressClassSpec) -> Self {
        self.spec = Some(spec);
        self
    }

    pub fn with_controller(mut self, controller: impl Into<String>) -> Self {
        let controller_str = controller.into();
        if let Some(ref mut spec) = self.spec {
            spec.controller = controller_str;
        } else {
            self.spec = Some(IngressClassSpec {
                controller: controller_str,
                parameters: None,
            });
        }
        self
    }
}

/// IngressClassSpec provides information about the class of an Ingress
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngressClassSpec {
    /// Controller refers to the name of the controller that should handle this class.
    /// This allows for different "flavors" of Ingress within a cluster.
    /// Must be a valid domain-prefixed path (max 250 characters).
    /// Examples: "acme.io/ingress-controller", "example.com/ingress-controller"
    ///
    /// Absent decodes to `""`, which `ValidateIngressClassSpec` reports as
    /// `Required` (`pkg/apis/networking/validation/validation.go`).
    #[serde(default)]
    pub controller: String,

    /// Parameters is a link to a custom resource containing additional configuration
    /// for the controller. This is optional and can be used to customize the
    /// behavior of the Ingress controller.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<IngressClassParametersReference>,
}

/// IngressClassParametersReference identifies an API object containing
/// controller-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngressClassParametersReference {
    /// APIGroup is the group for the resource being referenced.
    /// If APIGroup is not specified, the specified Kind must be in the core API group.
    /// For any other third-party types, APIGroup is required.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_group: Option<String>,

    /// Kind is the type of resource being referenced
    #[serde(default)]
    pub kind: String,

    /// Name is the name of resource being referenced
    #[serde(default)]
    pub name: String,

    /// Namespace is the namespace of the resource being referenced.
    /// This field is required when scope is set to "Namespace" and must be
    /// unset when scope is set to "Cluster".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Scope represents if this refers to a cluster or namespace scoped resource.
    /// This may be set to "Cluster" (default) or "Namespace".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}
