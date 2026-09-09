//! Port of upstream's **NodeRestriction** admission plugin, for pods.
//!
//! Upstream splits the mirror-pod rules across two components, and both halves
//! are needed for a kubelet to manage the mirror pod of a static pod:
//!
//! * the node's RBAC rules grant `create` and `delete` on `pods` outright
//!   (`plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go:215-217`):
//!
//!   ```go
//!   // Needed for the node to create/delete mirror pods.
//!   // Use the NodeRestriction admission plugin to limit a node to creating/deleting mirror pods bound to itself.
//!   rbacv1helpers.NewRule("create", "delete").Groups(legacyGroup).Resources("pods").RuleOrDie(),
//!   ```
//!
//! * and this plugin narrows them to the node's own mirror pods
//!   (`plugin/pkg/admission/noderestriction/admission.go:252-275`).
//!
//! Rusternetes had neither: the authorizer denied node pod writes outright,
//! with a comment saying the grant would be unsafe until this plugin existed
//! (#1721). A real kubelet against our api-server then looped on
//!
//! ```text
//! Failed deleting a mirror pod: Node "…-control-plane" is not authorized to
//! delete pods in namespace kube-system
//! ```
//!
//! so static-pod lifecycle was broken (#1906). This module is the missing half;
//! the authorizer grant is restored alongside it.

use rusternetes_common::resources::Pod;
use rusternetes_common::types::ObjectMeta;
use rusternetes_common::{Error, Result};
use rusternetes_middleware::AuthContext;

/// `api.MirrorPodAnnotationKey` (`pkg/apis/core/annotation_key_constants.go:27`).
pub const MIRROR_POD_ANNOTATION_KEY: &str = "kubernetes.io/config.mirror";

const NODE_USERNAME_PREFIX: &str = "system:node:";
const NODES_GROUP: &str = "system:nodes";

/// `DefaultNodeIdentifier.NodeIdentity` (`pkg/auth/nodeidentifier/default.go:42-60`):
/// the username must carry the `system:node:` prefix **and** the user must be in
/// the `system:nodes` group. A username alone is not an identity — anyone able
/// to mint a token with that name would otherwise inherit the node's rules.
pub fn node_identity(auth: &AuthContext) -> Option<String> {
    let name = auth.user.username.strip_prefix(NODE_USERNAME_PREFIX)?;
    if !auth.user.groups.iter().any(|g| g == NODES_GROUP) {
        return None;
    }
    if name.is_empty() {
        return None;
    }
    Some(name.to_string())
}

fn spec_node_name(pod: &Pod) -> &str {
    pod.spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .unwrap_or("")
}

/// `admitPod`, `admission.Create` branch — `admitPodCreate`
/// (`admission.go:277-340`). A non-node request is not this plugin's business
/// and passes through.
pub async fn admit_pod_create<S>(storage: &S, auth: &AuthContext, pod: &Pod) -> Result<()>
where
    S: rusternetes_storage::Storage,
{
    let Some(node_name) = node_identity(auth) else {
        return Ok(());
    };

    // only allow nodes to create mirror pods
    if !pod
        .metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.contains_key(MIRROR_POD_ANNOTATION_KEY))
    {
        return Err(Error::Forbidden(format!(
            "pod does not have {MIRROR_POD_ANNOTATION_KEY:?} annotation, node {node_name:?} can only create mirror pods"
        )));
    }

    // only allow nodes to create a pod bound to itself
    if spec_node_name(pod) != node_name {
        return Err(Error::Forbidden(format!(
            "node {node_name:?} can only create pods with spec.nodeName set to itself"
        )));
    }

    check_owner_references(storage, &node_name, &pod.metadata).await?;

    // don't allow a node to create a pod that references any other API objects
    if let Some(resource) = api_object_reference(pod) {
        return Err(Error::Forbidden(format!(
            "node {node_name:?} can not create pods that reference {resource:?}"
        )));
    }

    Ok(())
}

/// The owner-reference half of `admitPodCreate` (`admission.go:293-324`): zero
/// or one reference, and the one must be this node, as controller, without
/// `blockOwnerDeletion`, with a matching uid.
async fn check_owner_references<S>(storage: &S, node_name: &str, meta: &ObjectMeta) -> Result<()>
where
    S: rusternetes_storage::Storage,
{
    let owners = meta.owner_references.as_deref().unwrap_or(&[]);
    if owners.len() > 1 {
        return Err(Error::Forbidden(format!(
            "node {node_name:?} can only create pods with a single owner reference set to itself"
        )));
    }
    let Some(owner) = owners.first() else {
        return Err(Error::Forbidden(format!(
            "node {node_name:?} can only create pods with an owner reference set to itself"
        )));
    };

    if owner.api_version != "v1" || owner.kind != "Node" || owner.name != node_name {
        return Err(Error::Forbidden(format!(
            "node {node_name:?} can only create pods with an owner reference set to itself"
        )));
    }
    if owner.controller != Some(true) {
        return Err(Error::Forbidden(format!(
            "node {node_name:?} can only create pods with a controller owner reference set to itself"
        )));
    }
    if owner.block_owner_deletion == Some(true) {
        return Err(Error::Forbidden(format!(
            "node {node_name} must not set blockOwnerDeletion on an owner reference"
        )));
    }

    // Verify the node UID.
    let node_key = rusternetes_storage::build_key("nodes", None, node_name);
    let node: rusternetes_common::resources::Node = storage.get(&node_key).await?;
    if owner.uid != node.metadata.uid {
        return Err(Error::Forbidden(format!(
            "node {} UID mismatch: expected {} got {}",
            node_name, owner.uid, node.metadata.uid
        )));
    }
    Ok(())
}

/// `admitPod`, `admission.Delete` branch (`admission.go:257-270`):
///
/// ```go
/// // only allow a node to delete a pod bound to itself
/// if existingPod.Spec.NodeName != nodeName {
///     return admission.NewForbidden(a, fmt.Errorf("node %q can only delete pods with spec.nodeName set to itself", nodeName))
/// }
/// ```
///
/// Note it is the *stored* pod that decides, and that mirror-ness is not part
/// of the delete rule — a node may delete any pod bound to it.
pub fn admit_pod_delete(auth: &AuthContext, existing: &Pod) -> Result<()> {
    let Some(node_name) = node_identity(auth) else {
        return Ok(());
    };
    if spec_node_name(existing) != node_name {
        return Err(Error::Forbidden(format!(
            "node {node_name:?} can only delete pods with spec.nodeName set to itself"
        )));
    }
    Ok(())
}

/// `podutil.HasAPIObjectReference` (`pkg/api/pod/util.go:1696-1766`), returning
/// the resource name upstream reports, or `None` when the pod references
/// nothing from the API.
///
/// Upstream also errors on an unknown volume type, which cannot be expressed
/// here: an unknown volume does not decode into our `Volume` at all.
fn api_object_reference(pod: &Pod) -> Option<&'static str> {
    let spec = pod.spec.as_ref()?;

    if spec
        .service_account_name
        .as_deref()
        .is_some_and(|s| !s.is_empty())
    {
        return Some("serviceaccounts");
    }
    if spec
        .image_pull_secrets
        .as_ref()
        .is_some_and(|s| !s.is_empty())
    {
        return Some("secrets");
    }

    // VisitPodSecretNames / VisitPodConfigmapNames over every container class.
    let containers = spec
        .containers
        .iter()
        .chain(spec.init_containers.iter().flatten());
    for c in containers {
        for e in c.env.iter().flatten() {
            let Some(src) = e.value_from.as_ref() else {
                continue;
            };
            if src.secret_key_ref.is_some() {
                return Some("secrets");
            }
            if src.config_map_key_ref.is_some() {
                return Some("configmaps");
            }
        }
        for e in c.env_from.iter().flatten() {
            if e.secret_ref.is_some() {
                return Some("secrets");
            }
            if e.config_map_ref.is_some() {
                return Some("configmaps");
            }
        }
    }

    if spec.resource_claims.as_ref().is_some_and(|c| !c.is_empty()) {
        return Some("resourceclaims");
    }

    for v in spec.volumes.iter().flatten() {
        if v.config_map.is_some() {
            return Some("configmaps (via configmap volumes)");
        }
        if v.secret.is_some() {
            return Some("secrets (via secret volumes)");
        }
        if v.csi.is_some() {
            return Some("csidrivers (via CSI volumes)");
        }
        if v.persistent_volume_claim.is_some() {
            return Some("persistentvolumeclaims");
        }
        if v.ephemeral.is_some() {
            return Some("persistentvolumeclaims (via ephemeral volumes)");
        }
        if let Some(projected) = v.projected.as_ref() {
            for s in projected.sources.iter().flatten() {
                if s.config_map.is_some() {
                    return Some("configmaps (via projected volumes)");
                }
                if s.secret.is_some() {
                    return Some("secrets (via projected volumes)");
                }
                if s.service_account_token.is_some() {
                    return Some("serviceaccounts (via projected volumes)");
                }
                if s.cluster_trust_bundle.is_some() {
                    return Some("clustertrustbundles");
                }
                if s.pod_certificate.is_some() {
                    return Some("podcertificates");
                }
            }
        }
    }

    None
}
