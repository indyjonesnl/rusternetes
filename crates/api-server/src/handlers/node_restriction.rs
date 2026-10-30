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

use rusternetes_common::authz::{Authorizer, Decision, RequestAttributes};
use rusternetes_common::csi_translation::CsiTranslator;
use rusternetes_common::resources::csi::CSIDriver;
use rusternetes_common::resources::pod::Volume;
use rusternetes_common::resources::podcertificaterequest::PodCertificateRequest;
use rusternetes_common::resources::{
    Node, PersistentVolume, PersistentVolumeClaim, Pod, ServiceAccount, TokenRequest,
};
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

/// `admitPodCertificateRequest` (`plugin/pkg/admission/noderestriction/
/// admission.go:820-895`), dispatched from `Admit` at `admission.go:228-229`.
///
/// Upstream's first check, `!p.podCertificateRequestsEnabled` -> "PodCertificateRequest
/// feature gate is disabled" (`:821-823`), has no counterpart: the resource is
/// only served when the gate is on (`handlers/podcertificaterequest.rs`), so
/// it is unreachable for a request that got this far.
///
/// Upstream's plain (non-Forbidden) errors mark failures that informer lag can
/// cause, so that a retry may succeed (`:845-847`, `:852-853`, `:858-860`,
/// `:865-866`, `:882-885`, `:890-891`); they map to `Error::Internal` here.
pub async fn admit_pod_certificate_request<S>(
    storage: &S,
    auth: &AuthContext,
    create: bool,
    subresource: Option<&str>,
    namespace: &str,
    req: &PodCertificateRequest,
) -> Result<()>
where
    S: rusternetes_storage::Storage,
{
    let Some(node_name) = node_identity(auth) else {
        return Ok(());
    };

    if !create {
        return Err(Error::Forbidden("unexpected operation".to_string()));
    }
    if let Some(sub) = subresource.filter(|s| !s.is_empty()) {
        return Err(Error::Forbidden(format!("unexpected subresource {sub}")));
    }

    let spec = &req.spec;
    let pod_ref = format!("{namespace}/{}", spec.pod_name);

    // Cross check the node name and node UID with the node that made the request.
    if spec.node_name != node_name {
        return Err(Error::Forbidden(format!(
            "PodCertificateRequest.Spec.NodeName={:?}, which is not the requesting node {node_name:?}",
            spec.node_name
        )));
    }
    let node: Node = match storage
        .get(&rusternetes_storage::build_key(
            "nodes",
            None,
            &spec.node_name,
        ))
        .await
    {
        Ok(n) => n,
        Err(Error::NotFound(_)) => {
            return Err(Error::Internal(format!(
            "while retrieving node {:?} named in the PodCertificateRequest: node {:?} not found",
            spec.node_name, spec.node_name
        )))
        }
        Err(e) => {
            return Err(Error::Forbidden(format!(
                "while retrieving node {:?} named in the PodCertificateRequest: {e}",
                spec.node_name
            )))
        }
    };
    if node.metadata.uid != spec.node_uid {
        return Err(Error::Internal(format!(
            "PodCertificateRequest for pod {pod_ref:?} names node UID {:?}, inconsistent with the running node ({:?})",
            spec.node_uid, node.metadata.uid
        )));
    }

    // Cross-check that the pod is a real pod, running on the node.
    let pod: Pod = match storage
        .get(&rusternetes_storage::build_key(
            "pods",
            Some(namespace),
            &spec.pod_name,
        ))
        .await
    {
        Ok(p) => p,
        Err(Error::NotFound(_)) => {
            return Err(Error::Internal(format!(
                "while retrieving pod {pod_ref:?} named in the PodCertificateRequest: pod {:?} not found",
                spec.pod_name
            )))
        }
        Err(e) => {
            return Err(Error::Forbidden(format!(
                "while retrieving pod {pod_ref:?} named in the PodCertificateRequest: {e}"
            )))
        }
    };
    if spec.pod_uid != pod.metadata.uid {
        return Err(Error::Internal(format!(
            "PodCertificateRequest for pod {pod_ref:?} contains pod UID ({:?}) which differs from running pod {:?}",
            spec.pod_uid, pod.metadata.uid
        )));
    }
    if spec_node_name(&pod) != spec.node_name {
        return Err(Error::Forbidden(format!(
            "pod {pod_ref:?} is not running on node {:?} named in the PodCertificateRequest",
            spec.node_name
        )));
    }

    // Mirror pods don't get pod certificates.
    if pod
        .metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.contains_key(MIRROR_POD_ANNOTATION_KEY))
    {
        return Err(Error::Forbidden(format!("pod {pod_ref:?} is a mirror pod")));
    }

    let pod_sa = pod
        .spec
        .as_ref()
        .and_then(|s| s.service_account_name.as_deref())
        .unwrap_or("");
    if spec.service_account_name != pod_sa {
        // We can outright forbid because this cannot be caused by informer lag (the UIDs match)
        return Err(Error::Forbidden(format!(
            "PodCertificateRequest for pod {pod_ref:?} contains serviceAccountName ({:?}) that differs from running pod ({pod_sa:?})",
            spec.service_account_name
        )));
    }
    let sa_ref = format!("{namespace}/{}", spec.service_account_name);
    let sa: ServiceAccount = match storage
        .get(&rusternetes_storage::build_key(
            "serviceaccounts",
            Some(namespace),
            &spec.service_account_name,
        ))
        .await
    {
        Ok(sa) => sa,
        Err(Error::NotFound(_)) => {
            return Err(Error::Internal(format!(
                "while retrieving service account {sa_ref:?} named in the PodCertificateRequest: serviceaccount {:?} not found",
                spec.service_account_name
            )))
        }
        Err(e) => {
            return Err(Error::Forbidden(format!(
                "while retrieving service account {sa_ref:?} named in the PodCertificateRequest: {e}"
            )))
        }
    };
    let sa_uid = sa.metadata.uid.as_str();
    if spec.service_account_uid != sa_uid {
        return Err(Error::Internal(format!(
            "PodCertificateRequest for pod {pod_ref:?} names service account UID {:?}, which differs from the running service account ({sa_uid:?})",
            spec.service_account_uid
        )));
    }

    Ok(())
}

/// `admitServiceAccount` (`plugin/pkg/admission/noderestriction/admission.go:623-674`),
/// dispatched from `Admit` at `admission.go:224-225`: a node may only create a
/// `serviceaccounts/token` bound to a pod scheduled on itself. Every other
/// operation or subresource on serviceaccounts is not restricted here.
///
/// Upstream's lister errors map as: NotFound is returned as-is (`:650-652`),
/// anything else is Forbidden (`:653-655`).
///
/// `audience_restriction` is the `ServiceAccountNodeAudienceRestriction` gate
/// (`:111`, pkg/features/kube_features.go:1773-1776, Beta, on since 1.33).
pub async fn admit_service_account_token<S>(
    storage: &S,
    authz: &dyn Authorizer,
    auth: &AuthContext,
    namespace: &str,
    sa_name: &str,
    tr: &TokenRequest,
    audience_restriction: bool,
) -> Result<()>
where
    S: rusternetes_storage::Storage,
{
    let Some(node_name) = node_identity(auth) else {
        return Ok(());
    };

    // TokenRequests from a node must have a pod binding. That pod must be
    // scheduled on the node.
    let bound = tr.spec.bound_object_ref.as_ref();
    let get = |f: fn(
        &rusternetes_common::resources::authentication::BoundObjectReference,
    ) -> &Option<String>| {
        bound
            .and_then(|b| f(b).as_deref())
            .unwrap_or("")
            .to_string()
    };
    let api_version = get(|b| &b.api_version);
    let kind = get(|b| &b.kind);
    let ref_name = get(|b| &b.name);
    let ref_uid = get(|b| &b.uid);
    if bound.is_none() || api_version != "v1" || kind != "Pod" || ref_name.is_empty() {
        return Err(Error::Forbidden(
            "node requested token not bound to a pod".to_string(),
        ));
    }
    if ref_uid.is_empty() {
        return Err(Error::Forbidden(
            "node requested token with a pod binding without a uid".to_string(),
        ));
    }
    let pod: Pod = storage
        .get(&rusternetes_storage::build_key(
            "pods",
            Some(namespace),
            &ref_name,
        ))
        .await
        .map_err(|e| match e {
            Error::NotFound(m) => Error::NotFound(m),
            other => Error::Forbidden(other.to_string()),
        })?;
    if ref_uid != pod.metadata.uid {
        return Err(Error::Forbidden(format!(
            "the UID in the bound object reference ({ref_uid}) does not match the UID in record. The object might have been deleted and then recreated"
        )));
    }
    if spec_node_name(&pod) != node_name {
        return Err(Error::Forbidden(
            "node requested token bound to a pod scheduled on a different node".to_string(),
        ));
    }

    if audience_restriction {
        validate_node_service_account_audience(storage, authz, auth, namespace, sa_name, tr, &pod)
            .await
            .map_err(Error::Forbidden)?;
    }
    Ok(())
}

/// `validateNodeServiceAccountAudience` (`admission.go:676-719`).
async fn validate_node_service_account_audience<S>(
    storage: &S,
    authz: &dyn Authorizer,
    auth: &AuthContext,
    namespace: &str,
    sa_name: &str,
    tr: &TokenRequest,
    pod: &Pod,
) -> std::result::Result<(), String>
where
    S: rusternetes_storage::Storage,
{
    let requested_audience = match tr.spec.audiences.len() {
        0 => "",
        1 => tr.spec.audiences[0].as_str(),
        _ => return Err("node may only request 0 or 1 audiences".to_string()),
    };

    let found = pod_references_audience(storage, pod, requested_audience)
        .await
        .map_err(|e| format!("error validating audience {requested_audience:?}: {e}"))?;
    if found {
        return Ok(());
    }

    let user = auth.user.username.as_str();
    let attrs = RequestAttributes::new(
        auth.user.clone(),
        "request-serviceaccounts-token-audience",
        requested_audience,
    )
    .with_namespace(namespace)
    .with_api_group("")
    .with_api_version("v1")
    .with_name(sa_name);

    // An authorizer like RBAC can hit evaluation errors and still allow, so the
    // decision is checked before the error (`admission.go:704-711`).
    match authz.authorize(&attrs).await {
        Ok(Decision::Allow) => Ok(()),
        Ok(Decision::Deny(_)) => Err(format!(
            "audience {requested_audience:?} not found in pod spec volume, {user} is not authorized to request tokens for this audience"
        )),
        Err(e) => Err(format!(
            "audience {requested_audience:?} not found in pod spec volume, error authorizing {user} to request tokens for this audience: {e}"
        )),
    }
}

/// `podReferencesAudience` (`admission.go:721-777`). Errors are aggregated like
/// `utilerrors.NewAggregate`: one error prints bare, several as `[a, b]`.
async fn pod_references_audience<S>(
    storage: &S,
    pod: &Pod,
    audience: &str,
) -> std::result::Result<bool, String>
where
    S: rusternetes_storage::Storage,
{
    let translator = CsiTranslator::new();
    let namespace = pod.metadata.namespace.as_deref().unwrap_or("");
    let mut errs: Vec<String> = Vec::new();

    let volumes: &[Volume] = pod
        .spec
        .as_ref()
        .and_then(|s| s.volumes.as_deref())
        .unwrap_or(&[]);
    for v in volumes {
        if let Some(projected) = v.projected.as_ref() {
            for src in projected.sources.iter().flatten() {
                if let Some(t) = src.service_account_token.as_ref() {
                    if t.audience.as_deref().unwrap_or("") == audience {
                        return Ok(true);
                    }
                }
            }
        }

        // also allow audiences for CSI token requests
        // - pod --> ephemeral --> pvc --> pv --> csi --> driver --> tokenrequest with audience
        // - pod --> pvc --> pv --> csi --> driver --> tokenrequest with audience
        // - pod --> csi --> driver --> tokenrequest with audience
        let driver: std::result::Result<String, String> = if v
            .ephemeral
            .as_ref()
            .is_some_and(|e| e.volume_claim_template.is_some())
        {
            // `ephemeral.VolumeClaimName` (`component-helpers/storage/ephemeral`):
            // `<pod name>-<volume name>`.
            let claim = format!("{}-{}", pod.metadata.name, v.name);
            get_csi_from_pvc(storage, &translator, namespace, &claim).await
        } else if let Some(pvc) = v.persistent_volume_claim.as_ref() {
            get_csi_from_pvc(storage, &translator, namespace, &pvc.claim_name).await
        } else if let Some(csi) = v.csi.as_ref() {
            Ok(csi.driver.clone())
        } else if translator.is_inline_migratable(v) {
            translator
                .translate_in_tree_inline_volume_to_csi(v, namespace)
                .map(|pv| pv.spec.csi.map(|c| c.driver).unwrap_or_default())
        } else {
            Ok(String::new())
        };

        let driver = match driver {
            Ok(d) => d,
            Err(e) => {
                errs.push(e);
                continue;
            }
        };
        if !driver.is_empty() {
            match csi_driver_has_audience(storage, &driver, audience).await {
                Ok(true) => return Ok(true),
                Ok(false) => {}
                Err(e) => errs.push(e),
            }
        }
    }

    match errs.len() {
        0 => Ok(false),
        1 => Err(errs.remove(0)),
        _ => Err(format!("[{}]", errs.join(", "))),
    }
}

/// `getCSIFromPVC` (`admission.go:779-804`): the PVC -> PV -> CSI driver chain,
/// translating an in-tree PV first.
async fn get_csi_from_pvc<S>(
    storage: &S,
    translator: &CsiTranslator,
    namespace: &str,
    claim_name: &str,
) -> std::result::Result<String, String>
where
    S: rusternetes_storage::Storage,
{
    let pvc: PersistentVolumeClaim = storage
        .get(&rusternetes_storage::build_key(
            "persistentvolumeclaims",
            Some(namespace),
            claim_name,
        ))
        .await
        .map_err(|e| e.to_string())?;
    let pv_name = pvc.spec.volume_name.clone().unwrap_or_default();
    let pv: PersistentVolume = storage
        .get(&rusternetes_storage::build_key(
            "persistentvolumes",
            None,
            &pv_name,
        ))
        .await
        .map_err(|e| e.to_string())?;
    if let Some(csi) = pv.spec.csi.as_ref() {
        return Ok(csi.driver.clone());
    }
    if translator.is_pv_migratable(&pv) {
        let translated = translator.translate_in_tree_pv_to_csi(&pv)?;
        if let Some(csi) = translated.spec.csi.as_ref() {
            return Ok(csi.driver.clone());
        }
    }
    Ok(String::new())
}

/// `csiDriverHasAudience` (`admission.go:806-818`).
async fn csi_driver_has_audience<S>(
    storage: &S,
    driver_name: &str,
    audience: &str,
) -> std::result::Result<bool, String>
where
    S: rusternetes_storage::Storage,
{
    let driver: CSIDriver = storage
        .get(&rusternetes_storage::build_key(
            "csidrivers",
            None,
            driver_name,
        ))
        .await
        .map_err(|e| e.to_string())?;
    Ok(driver
        .spec
        .token_requests
        .iter()
        .flatten()
        .any(|t| t.audience == audience))
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
