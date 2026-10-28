//! The Node authorizer.
//!
//! Ported from `plugin/pkg/auth/authorizer/node/node_authorizer.go`
//! (`NodeAuthorizer.Authorize` and its `authorize*` helpers, `hasPathFrom`)
//! on the graph in `node_graph.rs`, with `pkg/auth/nodeidentifier/default.go`
//! as the identity rule.
//!
//! `NodeAuthorizer` authorizes requests from kubelets, with the following logic:
//!  1. If a request is not from a node (`NodeIdentity()` returns isNode=false), reject
//!  2. If a specific node cannot be identified (nodeName=""), reject
//!  3. If a request is for a secret, configmap, persistent volume, resource
//!     claim, or persistent volume claim, reject unless the verb is get, and
//!     the requested object is related to the requesting node:
//!     node <- configmap; node <- pod; node <- pod <- secret;
//!     node <- pod <- configmap; node <- pod <- pvc; node <- pod <- pvc <- pv;
//!     node <- pod <- pvc <- pv <- secret; node <- pod <- ResourceClaim;
//!     node <- pcr
//!  4. If a request is for a resourceslice, then authorize access if there is
//!     an edge from the existing slice object to the node, which is the case
//!     if the existing object has the node in its NodeName field. For create,
//!     the access gets granted because the noderestriction admission plugin
//!     checks that the NodeName is set to the node.
//!  5. For other resources, authorize all nodes uniformly using statically
//!     defined rules

use super::node_graph::{NodeGraph, VertexType};
use super::reasons::quoted;
use super::{node_rules_allow, Authorizer, Decision, Opinion, RequestAttributes};
use crate::auth::UserInfo;
use crate::error::Result;
use crate::feature_gates::{self, Feature};
use crate::field_selector::FieldOperator;
use crate::resources::ResourceRule;
use async_trait::async_trait;
use std::sync::Arc;

/// `api.NamespaceNodeLease` (pkg/apis/core/types.go).
const NAMESPACE_NODE_LEASE: &str = "kube-node-lease";

/// The feature gates `NodeAuthorizer.features` reads
/// (`r.features.Enabled(...)`, node_authorizer.go:131-153, 238-241).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeAuthorizerFeatures {
    /// `AuthorizeNodeWithSelectors` (1.34 GA, locked on).
    pub authorize_node_with_selectors: bool,
    /// `KubeletServiceAccountTokenForCredentialProviders` (1.34 Beta, on).
    pub kubelet_service_account_token_for_credential_providers: bool,
    /// `PodCertificateRequest` (1.35 Beta, off by default).
    pub pod_certificate_request: bool,
}

impl NodeAuthorizerFeatures {
    /// The process-wide gates. `KubeletServiceAccountTokenForCredentialProviders`
    /// is not modelled in `feature_gates.rs`; it is Beta and on by default in
    /// the targeted release (pkg/features/versioned_kube_features.go: 1.34
    /// Beta, `Default: true`).
    pub fn current() -> Self {
        Self {
            authorize_node_with_selectors: feature_gates::enabled(
                Feature::AuthorizeNodeWithSelectors,
            ),
            kubelet_service_account_token_for_credential_providers: true,
            pod_certificate_request: feature_gates::enabled(Feature::PodCertificateRequest),
        }
    }
}

/// What `Authorize` returned, before the `Opinion` encoding.
enum Verdict {
    Allow,
    /// `DecisionNoOpinion` with a reason.
    NoOpinion(String),
}

fn no_opinion(reason: impl Into<String>) -> Verdict {
    Verdict::NoOpinion(reason.into())
}

/// `NodeAuthorizer` (node_authorizer.go:61-76).
pub struct NodeAuthorizer {
    graph: Arc<NodeGraph>,
    /// "allows overriding for testing"; `None` reads the live gates.
    features: Option<NodeAuthorizerFeatures>,
}

impl NodeAuthorizer {
    /// `NewAuthorizer` (node_authorizer.go:81-88). The static `nodeRules` are
    /// [`super::NODE_RULES`] (`bootstrappolicy.NodeRules()`).
    pub fn new(graph: Arc<NodeGraph>) -> Self {
        Self {
            graph,
            features: None,
        }
    }

    /// Pin the feature gates instead of reading the process-wide ones.
    pub fn with_features(mut self, features: NodeAuthorizerFeatures) -> Self {
        self.features = Some(features);
        self
    }

    fn features(&self) -> NodeAuthorizerFeatures {
        self.features
            .unwrap_or_else(NodeAuthorizerFeatures::current)
    }

    /// `DefaultNodeIdentifier.NodeIdentity`
    /// (pkg/auth/nodeidentifier/default.go:42-68): `(nodeName, isNode)`. Both
    /// the `system:node:` username prefix and the `system:nodes` group are
    /// required; an empty name is a node that cannot be identified.
    fn node_identity(user: &UserInfo) -> (String, bool) {
        let Some(name) = user.username.strip_prefix("system:node:") else {
            return (String::new(), false);
        };
        if !user.groups.iter().any(|g| g == "system:nodes") {
            return (String::new(), false);
        }
        (name.to_string(), true)
    }

    /// `Authorize` (node_authorizer.go:109-168).
    fn evaluate(&self, attrs: &RequestAttributes) -> Option<Verdict> {
        let (node_name, is_node) = Self::node_identity(&attrs.user);
        if !is_node {
            // reject requests from non-nodes
            return None;
        }
        if node_name.is_empty() {
            // reject requests from unidentifiable nodes
            return Some(no_opinion(format!(
                "unknown node for user {}",
                quoted(&attrs.user.username)
            )));
        }

        // subdivide access to specific resources
        if !attrs.is_non_resource_request {
            let group = attrs.api_group.as_str();
            let resource = attrs.resource.as_str();
            let features = self.features();
            match (group, resource) {
                ("", "secrets") => {
                    return Some(self.authorize_read_namespaced_object(
                        &node_name,
                        VertexType::Secret,
                        attrs,
                    ))
                }
                ("", "configmaps") => {
                    return Some(self.authorize_read_namespaced_object(
                        &node_name,
                        VertexType::ConfigMap,
                        attrs,
                    ))
                }
                ("", "persistentvolumeclaims") => {
                    if sub(attrs) == "status" {
                        return Some(self.authorize_status_update(
                            &node_name,
                            VertexType::Pvc,
                            attrs,
                        ));
                    }
                    return Some(self.authorize_get(&node_name, VertexType::Pvc, attrs));
                }
                ("", "persistentvolumes") => {
                    return Some(self.authorize_get(&node_name, VertexType::Pv, attrs))
                }
                ("resource.k8s.io", "resourceclaims") => {
                    return Some(self.authorize_get(&node_name, VertexType::ResourceClaim, attrs))
                }
                ("storage.k8s.io", "volumeattachments") => {
                    return Some(self.authorize_get(&node_name, VertexType::Va, attrs))
                }
                ("", "serviceaccounts") => {
                    return Some(self.authorize_service_account(&node_name, attrs))
                }
                ("coordination.k8s.io", "leases") => {
                    return Some(self.authorize_lease(&node_name, attrs))
                }
                ("storage.k8s.io", "csinodes") => {
                    return Some(self.authorize_csi_node(&node_name, attrs))
                }
                ("resource.k8s.io", "resourceslices") => {
                    return Some(self.authorize_resource_slice(&node_name, attrs))
                }
                ("", "nodes") => {
                    if features.authorize_node_with_selectors {
                        return Some(self.authorize_node(&node_name, attrs));
                    }
                }
                ("", "pods") => {
                    if features.authorize_node_with_selectors {
                        return Some(self.authorize_pod(&node_name, attrs));
                    }
                }
                ("certificates.k8s.io", "podcertificaterequests") => {
                    if features.pod_certificate_request && features.authorize_node_with_selectors {
                        return Some(self.authorize_pod_certificate_request(&node_name, attrs));
                    }
                    return Some(no_opinion(""));
                }
                _ => {}
            }
        }

        // Access to other resources is not subdivided, so just evaluate against the statically defined node rules
        if node_rules_allow(attrs) {
            return Some(Verdict::Allow);
        }
        Some(no_opinion(""))
    }

    /// `authorizeStatusUpdate`: get/update/patch requests to status
    /// subresources of the specified type if they are related to the node.
    fn authorize_status_update(
        &self,
        node_name: &str,
        starting_type: VertexType,
        attrs: &RequestAttributes,
    ) -> Verdict {
        match attrs.verb.as_str() {
            "update" | "patch" => {}
            _ => return no_opinion("can only get/update/patch this type"),
        }
        if sub(attrs) != "status" {
            return no_opinion("can only update status subresource");
        }
        self.authorize(node_name, starting_type, attrs)
    }

    /// `authorizeGet`: "get" requests to objects of the specified type if
    /// they are related to the specified node.
    fn authorize_get(
        &self,
        node_name: &str,
        starting_type: VertexType,
        attrs: &RequestAttributes,
    ) -> Verdict {
        if attrs.verb != "get" {
            return no_opinion("can only get individual resources of this type");
        }
        if !sub(attrs).is_empty() {
            return no_opinion("cannot get subresource");
        }
        self.authorize(node_name, starting_type, attrs)
    }

    /// `authorizeReadNamespacedObject`: "get", "list" and "watch" requests to
    /// single objects of a specified type if they are related to the node.
    fn authorize_read_namespaced_object(
        &self,
        node_name: &str,
        starting_type: VertexType,
        attrs: &RequestAttributes,
    ) -> Verdict {
        match attrs.verb.as_str() {
            "get" | "list" | "watch" => {}
            _ => return no_opinion("can only read resources of this type"),
        }
        if !sub(attrs).is_empty() {
            return no_opinion("cannot read subresource");
        }
        if ns(attrs).is_empty() {
            return no_opinion("can only read namespaced object of this type");
        }
        self.authorize(node_name, starting_type, attrs)
    }

    /// `authorize`.
    fn authorize(
        &self,
        node_name: &str,
        starting_type: VertexType,
        attrs: &RequestAttributes,
    ) -> Verdict {
        if name(attrs).is_empty() {
            return no_opinion("No Object name found");
        }
        match self
            .graph
            .has_path_from(node_name, starting_type, ns(attrs), name(attrs))
        {
            Ok(true) => Verdict::Allow,
            // Both an error from hasPathFrom and a missing path are denied
            // with the same reason (node_authorizer.go:206-215).
            Ok(false) | Err(_) => no_opinion(format!(
                "no relationship found between node '{node_name}' and this object"
            )),
        }
    }

    /// `authorizeServiceAccount`: "get" of serviceaccounts when
    /// `KubeletServiceAccountTokenForCredentialProviders` or
    /// `PodCertificateRequest` is enabled; "create" of the `token`
    /// subresource of pods running on a node.
    fn authorize_service_account(&self, node_name: &str, attrs: &RequestAttributes) -> Verdict {
        if attrs.verb == "get" && sub(attrs).is_empty() {
            let features = self.features();
            if !(features.kubelet_service_account_token_for_credential_providers
                || features.pod_certificate_request)
            {
                return no_opinion("not allowed to get service accounts");
            }
            return self.authorize_read_namespaced_object(
                node_name,
                VertexType::ServiceAccount,
                attrs,
            );
        }
        self.authorize_create_token(node_name, VertexType::ServiceAccount, attrs)
    }

    /// `authorizeCreateToken`.
    fn authorize_create_token(
        &self,
        node_name: &str,
        starting_type: VertexType,
        attrs: &RequestAttributes,
    ) -> Verdict {
        if attrs.verb != "create" || name(attrs).is_empty() {
            return no_opinion("can only create tokens for individual service accounts");
        }
        if sub(attrs) != "token" {
            return no_opinion("can only create token subresource of serviceaccount");
        }
        match self
            .graph
            .has_path_from(node_name, starting_type, ns(attrs), name(attrs))
        {
            Ok(true) => Verdict::Allow,
            Ok(false) | Err(_) => no_opinion(format!(
                "no relationship found between node '{node_name}' and this object"
            )),
        }
    }

    /// `authorizeLease`: node requests to coordination.k8s.io/leases.
    fn authorize_lease(&self, node_name: &str, attrs: &RequestAttributes) -> Verdict {
        // allowed verbs: get, create, update, patch, delete
        let verb = attrs.verb.as_str();
        match verb {
            "get" | "create" | "update" | "patch" | "delete" => {}
            _ => return no_opinion("can only get, create, update, patch, or delete a node lease"),
        }

        // the request must be against the system namespace reserved for node leases
        if ns(attrs) != NAMESPACE_NODE_LEASE {
            return no_opinion(format!(
                "can only access leases in the {} system namespace",
                quoted(NAMESPACE_NODE_LEASE)
            ));
        }

        // the request must come from a node with the same name as the lease
        // note we skip this check for create, since the authorizer doesn't know the name on create
        // the noderestriction admission plugin is capable of performing this check at create time
        if verb != "create" && name(attrs) != node_name {
            return no_opinion(
                "can only access node lease with the same name as the requesting node",
            );
        }

        Verdict::Allow
    }

    /// `authorizeCSINode`: node requests to storage.k8s.io/csinodes.
    fn authorize_csi_node(&self, node_name: &str, attrs: &RequestAttributes) -> Verdict {
        // allowed verbs: get, create, update, patch, delete
        let verb = attrs.verb.as_str();
        match verb {
            "get" | "create" | "update" | "patch" | "delete" => {}
            _ => return no_opinion("can only get, create, update, patch, or delete a CSINode"),
        }

        if !sub(attrs).is_empty() {
            return no_opinion("cannot authorize CSINode subresources");
        }

        // the request must come from a node with the same name as the CSINode
        // note we skip this check for create, since the authorizer doesn't know the name on create
        // the noderestriction admission plugin is capable of performing this check at create time
        if verb != "create" && name(attrs) != node_name {
            return no_opinion("can only access CSINode with the same name as the requesting node");
        }

        Verdict::Allow
    }

    /// `authorizeResourceSlice`: node requests to resource.k8s.io/resourceslices.
    fn authorize_resource_slice(&self, node_name: &str, attrs: &RequestAttributes) -> Verdict {
        if !sub(attrs).is_empty() {
            return no_opinion("cannot authorize ResourceSlice subresources");
        }

        // allowed verbs: get, create, update, patch, delete, watch, list, deletecollection
        match attrs.verb.as_str() {
            // The request must come from a node with the same name as the ResourceSlice.NodeName field.
            //
            // For create, the noderestriction admission plugin is performing this check.
            // Here we don't have access to the content of the new object.
            "create" => Verdict::Allow,
            // Checking the existing object must have established that access
            // is allowed by recording a graph edge.
            "get" | "update" | "patch" | "delete" => {
                self.authorize(node_name, VertexType::Slice, attrs)
            }
            "watch" | "list" | "deletecollection" => {
                if self.features().authorize_node_with_selectors {
                    // only allow a scoped fieldSelector
                    if has_node_name_selector(attrs, "spec.nodeName", node_name) {
                        return Verdict::Allow;
                    }
                    // deny otherwise
                    no_opinion(
                        "can only list/watch/deletecollection resourceslices with nodeName field selector",
                    )
                } else {
                    // Allow broad list/watch access if AuthorizeNodeWithSelectors is not enabled.
                    //
                    // The NodeRestriction admission plugin (plugin/pkg/admission/noderestriction)
                    // ensures that the node is not deleting some ResourceSlice belonging to
                    // some other node.
                    Verdict::Allow
                }
            }
            _ => no_opinion(
                "only the following verbs are allowed for a ResourceSlice: get, watch, list, create, update, patch, delete, deletecollection",
            ),
        }
    }

    /// `authorizePodCertificateRequest`.
    fn authorize_pod_certificate_request(
        &self,
        node_name: &str,
        attrs: &RequestAttributes,
    ) -> Verdict {
        if !sub(attrs).is_empty() {
            return no_opinion(
                "nodes may not access the status subresource of PodCertificateRequests",
            );
        }

        match attrs.verb.as_str() {
            // Creates are further restricted by the noderestriction admission plugin.
            "create" => return Verdict::Allow,
            "get" => return self.authorize(node_name, VertexType::Pcr, attrs),
            "list" | "watch" => {
                // Allow requests that have a fieldselector restricting to this node.
                if has_node_name_selector(attrs, "spec.nodeName", node_name) {
                    return Verdict::Allow;
                }
                // deny otherwise
                return no_opinion(
                    "can only list/watch podcertificaterequests with nodeName field selector",
                );
            }
            _ => {}
        }

        no_opinion(format!(
            "nodes may not {} podcertificaterequests",
            attrs.verb
        ))
    }

    /// `authorizeNode`: node requests to Node API objects.
    fn authorize_node(&self, node_name: &str, attrs: &RequestAttributes) -> Verdict {
        match sub(attrs) {
            "" => match attrs.verb.as_str() {
                // Use the NodeRestriction admission plugin to limit a node to creating/updating its own API object.
                "create" | "update" | "patch" => return Verdict::Allow,
                "get" | "list" | "watch" => {
                    // Compare the name directly, rather than using the graph,
                    // so kubelets can attempt a read of their Node API object prior to creation.
                    let requested = name(attrs);
                    if requested == node_name {
                        return Verdict::Allow;
                    }
                    if requested.is_empty() {
                        return no_opinion(format!(
                            "node '{node_name}' cannot read all nodes, only its own Node object"
                        ));
                    }
                    return no_opinion(format!(
                        "node '{node_name}' cannot read '{requested}', only its own Node object"
                    ));
                }
                _ => {}
            },
            "status" => {
                if matches!(attrs.verb.as_str(), "update" | "patch") {
                    // Use the NodeRestriction admission plugin to limit a node to updating its own Node status.
                    return Verdict::Allow;
                }
            }
            _ => {}
        }
        no_opinion("")
    }

    /// `authorizePod`: node requests to Pod API objects.
    fn authorize_pod(&self, node_name: &str, attrs: &RequestAttributes) -> Verdict {
        match sub(attrs) {
            "" => match attrs.verb.as_str() {
                "get" => return self.authorize_get(node_name, VertexType::Pod, attrs),
                "list" | "watch" => {
                    // allow a scoped fieldSelector
                    if has_node_name_selector(attrs, "spec.nodeName", node_name) {
                        return Verdict::Allow;
                    }
                    // allow a read of a single pod known to be related to the node
                    if !name(attrs).is_empty() {
                        return self.authorize(node_name, VertexType::Pod, attrs);
                    }
                    // deny otherwise
                    return no_opinion(
                        "can only list/watch pods with spec.nodeName field selector",
                    );
                }
                // Needed for the node to create/delete mirror pods.
                // Use the NodeRestriction admission plugin to limit a node to creating/deleting mirror pods bound to itself.
                "create" | "delete" => return Verdict::Allow,
                _ => {}
            },
            "status" => {
                // Needed for the node to report status of pods it is running.
                // Use the NodeRestriction admission plugin to limit a node to updating status of pods bound to itself.
                if matches!(attrs.verb.as_str(), "update" | "patch") {
                    return Verdict::Allow;
                }
            }
            // Needed for the node to evict pods it is running.
            // Use the NodeRestriction admission plugin to limit a node to evicting pods bound to itself.
            "eviction" if attrs.verb == "create" => return Verdict::Allow,
            _ => {}
        }
        no_opinion("")
    }
}

fn sub(attrs: &RequestAttributes) -> &str {
    attrs.subresource.as_deref().unwrap_or("")
}

fn ns(attrs: &RequestAttributes) -> &str {
    attrs.namespace.as_deref().unwrap_or("")
}

fn name(attrs: &RequestAttributes) -> &str {
    attrs.name.as_deref().unwrap_or("")
}

/// `req.Field == field && req.Operator == selection.Equals && req.Value == nodeName`
/// over `attrs.GetFieldSelector()` requirements.
fn has_node_name_selector(attrs: &RequestAttributes, field: &str, node_name: &str) -> bool {
    attrs.field_selector_requirements.iter().any(|req| {
        req.field() == field && req.operator() == FieldOperator::Equals && req.value() == node_name
    })
}

#[async_trait]
impl Authorizer for NodeAuthorizer {
    /// The binary form: a `NoOpinion` is a `Deny` carrying the reason. A bare
    /// miss keeps the message of the previous stateless authorizer.
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        Ok(match self.evaluate(attrs) {
            Some(Verdict::Allow) => Decision::Allow,
            Some(Verdict::NoOpinion(reason)) if !reason.is_empty() => Decision::Deny(reason),
            Some(Verdict::NoOpinion(_)) => Decision::Deny(format!(
                "Node {} is not authorized to {} {} in namespace {}",
                Self::node_identity(&attrs.user).0,
                attrs.verb,
                attrs.resource,
                ns(attrs)
            )),
            None => Decision::Deny("Not a node user".to_string()),
        })
    }

    /// node_authorizer.go:109-168: the `NoOpinion` reasons are upstream's.
    async fn authorize_opinion(&self, attrs: &RequestAttributes) -> Result<Opinion> {
        Ok(match self.evaluate(attrs) {
            Some(Verdict::Allow) => Opinion::Allow(String::new()),
            Some(Verdict::NoOpinion(reason)) => Opinion::NoOpinion {
                reason,
                error: None,
            },
            None => Opinion::NoOpinion {
                reason: String::new(),
                error: None,
            },
        })
    }

    async fn get_user_rules(
        &self,
        user: &UserInfo,
        _namespace: &str,
    ) -> Result<(Vec<ResourceRule>, Vec<crate::resources::NonResourceRule>)> {
        // Upstream's RulesFor reports "node authorizer does not support user
        // rule resolution" for nodes; the representative rules below predate
        // the graph.
        if !Self::node_identity(user).1 {
            return Ok((vec![], vec![]));
        }
        let rule = |verbs: &[&str], resources: &[&str]| ResourceRule {
            verbs: verbs.iter().map(|s| s.to_string()).collect(),
            api_groups: Some(vec!["".to_string()]),
            resources: Some(resources.iter().map(|s| s.to_string()).collect()),
            resource_names: None,
        };
        Ok((
            vec![
                rule(
                    &["get", "list", "watch"],
                    &[
                        "services",
                        "endpoints",
                        "nodes",
                        "pods",
                        "secrets",
                        "configmaps",
                        "persistentvolumeclaims",
                        "persistentvolumes",
                    ],
                ),
                rule(&["create"], &["events"]),
                rule(&["update", "patch"], &["nodes", "pods"]),
            ],
            vec![],
        ))
    }
}

#[cfg(test)]
#[path = "node_authorizer_tests.rs"]
mod tests;
