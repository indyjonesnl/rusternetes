//! DynamicResources queueing hints.
//!
//! Ported from upstream (release-1.35)
//! `pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go`:
//! `EventsToRegister` (`:211-244`, wired in `hints.rs`),
//! `isSchedulableAfterClaimChange` (`:261-323`),
//! `isSchedulableAfterPodChange` (`:325-348`) and
//! `foreachPodResourceClaim` (`:366-400`), with
//! `resourceclaim.Name` / `resourceclaim.IsForPod`
//! (`staging/src/k8s.io/dynamic-resource-allocation/resourceclaim/resourceclaim.go:65-93`);
//! tests: `dynamicresources_test.go` `Test_isSchedulableAfterClaimChange`
//! (`:2997`) and `Test_isSchedulableAfterPodChange` (`:3154`).
//!
//! Deviation: upstream reads claims through `draManager.ResourceClaims()`
//! (an informer-backed assume cache). Here [`ResourceClaimCache`] is filled by
//! the ResourceClaim event feed (`resource_events.rs`) before it dispatches
//! the event, the same ordering an informer guarantees. The DeviceClass and
//! ResourceSlice events carry no hint upstream either.

use super::events::*;
use rusternetes_common::resources::{Pod, ResourceClaim};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::debug;

/// The `draManager.ResourceClaims()` lister: claims by `namespace/name`.
#[derive(Clone, Default)]
pub struct ResourceClaimCache(Arc<RwLock<HashMap<String, ResourceClaim>>>);

impl ResourceClaimCache {
    fn key(namespace: &str, name: &str) -> String {
        format!("{namespace}/{name}")
    }

    pub fn get(&self, namespace: &str, name: &str) -> Option<ResourceClaim> {
        self.0
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&Self::key(namespace, name))
            .cloned()
    }

    pub fn insert(&self, claim: ResourceClaim) {
        let key = Self::key(
            claim.metadata.namespace.as_deref().unwrap_or(""),
            &claim.metadata.name,
        );
        self.0
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, claim);
    }

    pub fn remove(&self, claim: &ResourceClaim) {
        let key = Self::key(
            claim.metadata.namespace.as_deref().unwrap_or(""),
            &claim.metadata.name,
        );
        self.0
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&key);
    }
}

fn pod_key(pod: &Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.as_deref().unwrap_or(""),
        pod.metadata.name
    )
}

/// `foreachPodResourceClaim` (`dynamicresources.go:366-400`): every claim the
/// pod references must exist, not be deleting, and (for template claims) be
/// owned by the pod. Errors only mean "not schedulable".
fn foreach_pod_resource_claim(
    cache: &ResourceClaimCache,
    pod: &Pod,
    mut cb: impl FnMut(&ResourceClaim),
) -> Result<(), String> {
    let ns = pod.metadata.namespace.as_deref().unwrap_or("");
    let Some(pod_claims) = pod.spec.as_ref().and_then(|s| s.resource_claims.as_ref()) else {
        return Ok(());
    };
    for resource in pod_claims {
        // `resourceclaim.Name`
        let (claim_name, must_check_owner) = if let Some(n) = &resource.resource_claim_name {
            (Some(n.clone()), false)
        } else if resource.resource_claim_template_name.is_some() {
            let status = pod
                .status
                .as_ref()
                .and_then(|s| s.resource_claim_statuses.as_ref())
                .and_then(|l| l.iter().find(|s| s.name == resource.name));
            match status {
                Some(s) => (s.resource_claim_name.clone(), true),
                None => {
                    return Err(format!(
                        "pod \"{}/{}\": ResourceClaim not found",
                        ns, pod.metadata.name
                    ))
                }
            }
        } else {
            return Err(format!(
                "pod \"{}/{}\", spec.resourceClaim \"{}\": this Kubernetes version does not support the API",
                ns, pod.metadata.name, resource.name
            ));
        };
        // The claim name might be nil if no underlying resource claim was
        // generated for the referenced claim; skip it (upstream :374-378).
        let Some(claim_name) = claim_name else {
            continue;
        };
        let claim = cache
            .get(ns, &claim_name)
            .ok_or_else(|| format!("resourceclaim.resource.k8s.io \"{claim_name}\" not found"))?;
        if claim.metadata.deletion_timestamp.is_some() {
            return Err(format!(
                "resourceclaim \"{}\" is being deleted",
                claim.metadata.name
            ));
        }
        if must_check_owner {
            // `resourceclaim.IsForPod`: `metav1.IsControlledBy(claim, pod)`.
            let controlled = claim.metadata.namespace.as_deref().unwrap_or("") == ns
                && claim
                    .metadata
                    .owner_references
                    .iter()
                    .flatten()
                    .any(|r| r.controller == Some(true) && r.uid == pod.metadata.uid);
            if !controlled {
                return Err(format!(
                    "ResourceClaim {}/{} was not created for pod {}/{} (pod is not owner)",
                    ns, claim.metadata.name, ns, pod.metadata.name
                ));
            }
        }
        cb(&claim);
    }
    Ok(())
}

/// `isSchedulableAfterClaimChange` (`dynamicresources.go:261-323`).
pub fn is_schedulable_after_claim_change(
    cache: &ResourceClaimCache,
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (original, modified) = as_typed::<ResourceClaim>(old_obj, new_obj)
        .map_err(|e| format!("unexpected object in isSchedulableAfterClaimChange: {e}"))?;
    // Upstream is never invoked for deletes (Add|Update only) and would
    // dereference nil; skip instead.
    let Some(modified) = modified else {
        return Ok(QueueingHint::QueueSkip);
    };

    let mut uses_claim = false;
    if let Err(reason) = foreach_pod_resource_claim(cache, pod, |claim| {
        if claim.metadata.uid == modified.metadata.uid {
            uses_claim = true;
        }
    }) {
        debug!(pod = %pod_key(pod), claim = %modified.metadata.name, %reason, "pod is not schedulable after resource claim change");
        return Ok(QueueingHint::QueueSkip);
    }

    let allocated = |c: &ResourceClaim| c.status.as_ref().is_some_and(|s| s.allocation.is_some());
    if original.is_some_and(allocated) && !allocated(modified) {
        // A claim with structured parameters was deallocated. This might have
        // made resources available for other pods.
        debug!(pod = %pod_key(pod), claim = %modified.metadata.name, "claim with structured parameters got deallocated");
        return Ok(QueueingHint::Queue);
    }

    if !uses_claim {
        debug!(pod = %pod_key(pod), claim = %modified.metadata.name, "unrelated claim got modified");
        return Ok(QueueingHint::QueueSkip);
    }

    let Some(original) = original else {
        debug!(pod = %pod_key(pod), claim = %modified.metadata.name, "claim for pod got created");
        return Ok(QueueingHint::Queue);
    };

    // Modifications may or may not be relevant. If the entire status is as
    // before, then something else must have changed and we don't care (the
    // resource driver adds the finalizer). A nil status equals the zero value.
    let status = |c: &ResourceClaim| c.status.clone().unwrap_or_default();
    if status(original) == status(modified) {
        debug!(pod = %pod_key(pod), claim = %modified.metadata.name, "claim for pod got modified where the pod doesn't care");
        return Ok(QueueingHint::QueueSkip);
    }

    debug!(pod = %pod_key(pod), claim = %modified.metadata.name, "status of claim for pod got updated");
    Ok(QueueingHint::Queue)
}

/// `isSchedulableAfterPodChange` (`dynamicresources.go:325-348`): the update
/// must be for the pod itself and complete the claims it is waiting for.
pub fn is_schedulable_after_pod_change(
    cache: &ResourceClaimCache,
    pod: &Pod,
    _old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (_, modified) = as_typed::<Pod>(None, new_obj)
        .map_err(|e| format!("unexpected object in isSchedulableAfterClaimChange: {e}"))?;
    let modified = modified
        .ok_or_else(|| "unexpected object in isSchedulableAfterClaimChange: nil pod".to_string())?;
    if pod.metadata.uid != modified.metadata.uid {
        debug!(pod = %pod_key(pod), modified_pod = %pod_key(modified), "pod is not schedulable after change in other pod");
        return Ok(QueueingHint::QueueSkip);
    }
    if let Err(reason) = foreach_pod_resource_claim(cache, modified, |_| {}) {
        debug!(pod = %pod_key(pod), %reason, "pod is not schedulable after being updated");
        return Ok(QueueingHint::QueueSkip);
    }
    debug!(pod = %pod_key(pod), "pod got updated and is schedulable");
    Ok(QueueingHint::Queue)
}
