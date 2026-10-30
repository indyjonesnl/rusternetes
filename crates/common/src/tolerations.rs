//! Default-toleration admission helper shared by the api-server (HTTP pod-create
//! admission) and the controller-manager (controllers that create pods by
//! writing directly to storage, bypassing the HTTP admission chain).
//!
//! K8s ref: `plugin/pkg/admission/defaulttolerationseconds/admission.go`
//! (release-1.35). That plugin appends, to every pod that does not already
//! tolerate them, a `node.kubernetes.io/not-ready:NoExecute` toleration and a
//! `node.kubernetes.io/unreachable:NoExecute` toleration, each with
//! `tolerationSeconds: 300` (the `defaultNotReadyTolerationSeconds` /
//! `defaultUnreachableTolerationSeconds` defaults at admission.go:44-45).
//!
//! Without this, rusternetes pods carry NO toleration for the NotReady taint
//! the node controller adds on a NotReady node, so a single transient node
//! flap under CI load lets the kubelet's NoExecute sweep evict (and ultimately
//! delete) every pod on the node instantly — including Succeeded pods whose
//! status conformance tests still need to observe. See #442.

use crate::resources::{PodSpec, Toleration};

/// Taint key added to a node that is not ready (`v1.TaintNodeNotReady`).
pub const TAINT_NODE_NOT_READY: &str = "node.kubernetes.io/not-ready";
/// Taint key added to a node that is unreachable (`v1.TaintNodeUnreachable`).
pub const TAINT_NODE_UNREACHABLE: &str = "node.kubernetes.io/unreachable";
/// Default toleration grace period, in seconds, for both default tolerations.
pub const DEFAULT_TOLERATION_SECONDS: i64 = 300;

const EFFECT_NO_EXECUTE: &str = "NoExecute";
const OP_EXISTS: &str = "Exists";

/// Mirror upstream `Plugin.Admit`'s "already tolerates" test
/// (admission.go:144-153): a pod already tolerates `taint_key:NoExecute` if any
/// existing toleration has (key == taint_key OR an empty key) AND (effect
/// NoExecute OR an empty effect). The operator/value are intentionally NOT part
/// of this gate — matching upstream, which only inspects key+effect here.
fn already_tolerates(tolerations: &[Toleration], taint_key: &str) -> bool {
    tolerations.iter().any(|t| {
        let key_match = t
            .key
            .as_deref()
            .is_none_or(|k| k.is_empty() || k == taint_key);
        let effect_match = t
            .effect
            .as_deref()
            .is_none_or(|e| e.is_empty() || e == EFFECT_NO_EXECUTE);
        key_match && effect_match
    })
}

fn default_toleration(key: &str) -> Toleration {
    Toleration {
        key: Some(key.to_string()),
        operator: Some(OP_EXISTS.to_string()),
        value: None,
        effect: Some(EFFECT_NO_EXECUTE.to_string()),
        toleration_seconds: Some(DEFAULT_TOLERATION_SECONDS),
    }
}

/// Append the default NotReady / Unreachable NoExecute tolerations
/// (`tolerationSeconds: 300`) to a pod spec, exactly as upstream's
/// `DefaultTolerationSeconds` admission plugin does.
///
/// Idempotent: a toleration that already covers the relevant taint (per
/// [`already_tolerates`], the same key+effect gate upstream uses) is left
/// untouched and the default is not appended. Returns the number of tolerations
/// added (0, 1, or 2).
pub fn add_default_tolerations(spec: &mut PodSpec) -> usize {
    let existing = spec.tolerations.get_or_insert_with(Vec::new);
    let mut added = 0;

    if !already_tolerates(existing, TAINT_NODE_NOT_READY) {
        existing.push(default_toleration(TAINT_NODE_NOT_READY));
        added += 1;
    }
    if !already_tolerates(existing, TAINT_NODE_UNREACHABLE) {
        existing.push(default_toleration(TAINT_NODE_UNREACHABLE));
        added += 1;
    }
    added
}
