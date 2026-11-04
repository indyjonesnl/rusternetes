//! The `RuntimeClass` admission plugin.
//!
//! Port of `plugin/pkg/admission/runtimeclass/admission.go` (release-1.35):
//! `Admit` (`prepareObjects`, `setOverhead`, `setScheduling`) and `Validate`
//! (`validateOverhead`). The plugin is registered for CREATE only
//! (`NewRuntimeClass`, :135-139) and ignores subresources and everything but
//! core `pods` (`shouldIgnore`, :239-246). Both halves live here as pure
//! functions over the already looked-up class; the lookup (`prepareObjects`,
//! :142-164) and the `NewForbidden` wrapping are done by the chain in
//! `endpoints/handlers/admission.rs`.

use std::collections::HashMap;

use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::{Pod, RuntimeClass};

use crate::admission::pod_plugins::merge_tolerations;

/// `prepareObjects`' not-found message (admission.go:158), for `NewForbidden`.
pub fn not_found(name: &str) -> String {
    format!("pod rejected: RuntimeClass {name:?} not found")
}

/// `apiequality.Semantic.DeepEqual` of two `ResourceList`s: nil and empty
/// are equal and quantities compare by value, not by spelling.
fn overhead_equal(a: &HashMap<String, String>, b: &HashMap<String, String>) -> bool {
    a.len() == b.len()
        && a.iter().all(|(k, av)| {
            b.get(k)
                .is_some_and(|bv| match (Quantity::parse(av), Quantity::parse(bv)) {
                    (Ok(x), Ok(y)) => x.value_eq(&y),
                    _ => av == bv,
                })
        })
}

fn pod_fixed(rc: &RuntimeClass) -> Option<HashMap<String, String>> {
    rc.overhead
        .as_ref()
        .map(|o| o.pod_fixed.clone().unwrap_or_default())
}

/// `setOverhead` (admission.go:166-185). `Err` is the `NewForbidden` cause.
pub fn set_overhead(pod: &mut Pod, rc: Option<&RuntimeClass>) -> Result<(), String> {
    let Some(fixed) = rc.and_then(pod_fixed) else {
        return Ok(());
    };
    let Some(spec) = pod.spec.as_mut() else {
        return Ok(());
    };
    // Reject a pod whose Overhead is already set and differs.
    if let Some(existing) = spec.overhead.as_ref().filter(|o| !o.is_empty()) {
        if !overhead_equal(&fixed, existing) {
            return Err(
                "pod rejected: Pod's Overhead doesn't match RuntimeClass's defined Overhead".into(),
            );
        }
    }
    spec.overhead = if fixed.is_empty() { None } else { Some(fixed) };
    Ok(())
}

/// `setScheduling` (admission.go:187-217): merge the class's nodeSelector
/// (a conflicting key is rejected) and tolerations (`MergeTolerations`,
/// pkg/util/tolerations/tolerations.go:53-80) into the pod.
pub fn set_scheduling(pod: &mut Pod, rc: Option<&RuntimeClass>) -> Result<(), String> {
    let Some(scheduling) = rc.and_then(|r| r.scheduling.as_ref()) else {
        return Ok(());
    };
    let Some(spec) = pod.spec.as_mut() else {
        return Ok(());
    };
    let runtime_selector = scheduling.node_selector.clone().unwrap_or_default();
    let new_selector = match spec.node_selector.take() {
        None => (!runtime_selector.is_empty()).then_some(runtime_selector),
        Some(mut selector) => {
            // Go ranges a map; sort so the reported conflict is stable.
            let mut keys: Vec<&String> = runtime_selector.keys().collect();
            keys.sort();
            for key in keys {
                let rc_value = &runtime_selector[key];
                if let Some(pod_value) = selector.get(key) {
                    if pod_value != rc_value {
                        let msg = format!(
                            "conflict: runtimeClass.scheduling.nodeSelector[{key}] = {rc_value}; pod.spec.nodeSelector[{key}] = {pod_value}"
                        );
                        spec.node_selector = Some(selector);
                        return Err(msg);
                    }
                }
                selector.insert(key.clone(), rc_value.clone());
            }
            Some(selector)
        }
    };
    let merged = merge_tolerations(
        spec.tolerations.as_deref().unwrap_or_default(),
        scheduling.tolerations.as_deref().unwrap_or_default(),
    );
    spec.node_selector = new_selector;
    spec.tolerations = (!merged.is_empty()).then_some(merged);
    Ok(())
}

/// `validateOverhead` (admission.go:219-237).
pub fn validate_overhead(pod: &Pod, rc: Option<&RuntimeClass>) -> Result<(), String> {
    let overhead = pod.spec.as_ref().and_then(|s| s.overhead.as_ref());
    match rc.and_then(pod_fixed) {
        Some(fixed) => {
            if !overhead_equal(&fixed, overhead.unwrap_or(&HashMap::new())) {
                return Err(
                    "pod rejected: Pod's Overhead doesn't match RuntimeClass's defined Overhead"
                        .into(),
                );
            }
        }
        None => {
            if overhead.is_some() {
                return Err(
                    "pod rejected: Pod Overhead set without corresponding RuntimeClass defined Overhead"
                        .into(),
                );
            }
        }
    }
    Ok(())
}
