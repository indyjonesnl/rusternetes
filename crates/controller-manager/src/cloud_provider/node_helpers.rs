//! Port of `staging/src/k8s.io/cloud-provider/node/helpers/taints.go`
//! (release-1.35): `AddOrUpdateTaintOnNode`, `RemoveTaintOffNode`,
//! `PatchNodeTaints` and their private helpers.
//!
//! Differences forced by the Rust/`Storage` setting:
//!
//! * `PatchNodeTaints` builds a strategic-merge patch whose `resourceVersion`
//!   makes the API server reject a stale write with a Conflict. Here the
//!   fresh node (carrying its `resourceVersion`) is written back with only
//!   `.spec.taints` replaced, which `Storage::update` rejects with
//!   `Error::Conflict` on a stale `resourceVersion` - the same observable
//!   contract, and the same `RetryOnConflict` loop wraps it.
//! * The `firstTry` read with `ResourceVersion: "0"` (API-server cache) has no
//!   equivalent: there is no cache layer between this code and `Storage`, so
//!   every read is the fresh read the retry path would have used.

use rusternetes_common::resources::{Node, NodeSpec, Taint};
use rusternetes_common::Error;
use rusternetes_storage::{build_key, Storage};
use std::time::Duration;

/// `updateTaintBackoff` (taints.go:43-47): `Steps: 5, Duration: 100ms,
/// Jitter: 1.0` (no Factor, so the base delay is constant).
const UPDATE_TAINT_BACKOFF_STEPS: u32 = 5;
const UPDATE_TAINT_BACKOFF_DURATION: Duration = Duration::from_millis(100);

/// `clientretry.RetryOnConflict(updateTaintBackoff, fn)`: run `f` up to
/// `Steps` times, sleeping `Duration * (1 + rand[0,1))` between attempts, and
/// retry only while it fails with a Conflict.
async fn retry_on_conflict<F, Fut>(mut f: F) -> Result<(), Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), Error>>,
{
    let mut last = Ok(());
    for step in 0..UPDATE_TAINT_BACKOFF_STEPS {
        match f().await {
            Err(Error::Conflict(m)) => last = Err(Error::Conflict(m)),
            other => return other,
        }
        if step + 1 < UPDATE_TAINT_BACKOFF_STEPS {
            let jitter = 1.0 + rand::random::<f64>();
            tokio::time::sleep(UPDATE_TAINT_BACKOFF_DURATION.mul_f64(jitter)).await;
        }
    }
    last
}

/// `v1.Taint.MatchTaint`: same key and effect.
fn match_taint(a: &Taint, b: &Taint) -> bool {
    a.key == b.key && a.effect == b.effect
}

/// `equality.Semantic.DeepEqual` on two taints (an empty value equals an
/// absent one, as Go's `string` zero value does).
fn taint_equal(a: &Taint, b: &Taint) -> bool {
    a.key == b.key
        && a.effect == b.effect
        && a.value.clone().unwrap_or_default() == b.value.clone().unwrap_or_default()
        && a.time_added == b.time_added
}

fn node_taints(node: &Node) -> Vec<Taint> {
    node.spec
        .as_ref()
        .and_then(|s| s.taints.clone())
        .unwrap_or_default()
}

/// `taintExists` (taints.go:206-213).
pub fn taint_exists(taints: &[Taint], taint_to_find: &Taint) -> bool {
    taints.iter().any(|t| match_taint(t, taint_to_find))
}

/// `addOrUpdateTaint` (taints.go:121-146): returns the updated copy of the
/// node and whether anything changed.
fn add_or_update_taint(node: &Node, taint: &Taint) -> (Node, bool) {
    let mut new_node = node.clone();
    let mut new_taints: Vec<Taint> = Vec::new();
    let mut updated = false;
    for existing in node_taints(node) {
        if match_taint(taint, &existing) {
            if taint_equal(taint, &existing) {
                return (new_node, false);
            }
            new_taints.push(taint.clone());
            updated = true;
            continue;
        }
        new_taints.push(existing);
    }
    if !updated {
        new_taints.push(taint.clone());
    }
    set_taints(&mut new_node, new_taints);
    (new_node, true)
}

/// `deleteTaint` (taints.go:234-245): drop every taint with the same key and
/// effect.
pub fn delete_taint(taints: &[Taint], taint_to_delete: &Taint) -> (Vec<Taint>, bool) {
    let mut new_taints = Vec::new();
    let mut deleted = false;
    for t in taints {
        if match_taint(taint_to_delete, t) {
            deleted = true;
            continue;
        }
        new_taints.push(t.clone());
    }
    (new_taints, deleted)
}

/// `removeTaint` (taints.go:217-231).
pub fn remove_taint(node: &Node, taint: &Taint) -> (Node, bool) {
    let mut new_node = node.clone();
    let taints = node_taints(node);
    if taints.is_empty() || !taint_exists(&taints, taint) {
        return (new_node, false);
    }
    let (new_taints, _) = delete_taint(&taints, taint);
    set_taints(&mut new_node, new_taints);
    (new_node, true)
}

/// An empty `.spec.taints` is omitted on the wire (`omitempty`).
fn set_taints(node: &mut Node, taints: Vec<Taint>) {
    let taints = if taints.is_empty() {
        None
    } else {
        Some(taints)
    };
    match node.spec.as_mut() {
        Some(spec) => spec.taints = taints,
        None => {
            node.spec = Some(NodeSpec {
                pod_cidr: None,
                pod_cidrs: None,
                provider_id: None,
                unschedulable: None,
                taints,
            })
        }
    }
}

/// `PatchNodeTaints` (taints.go:91-117): write `new_node`'s taints onto the
/// stored node, guarded by `old_node`'s `resourceVersion`.
pub async fn patch_node_taints<S: Storage>(
    storage: &S,
    node_name: &str,
    old_node: &Node,
    new_node: &Node,
) -> Result<(), Error> {
    let mut clone = old_node.clone();
    set_taints(&mut clone, node_taints(new_node));
    storage
        .update(&build_key("nodes", None, node_name), &clone)
        .await?;
    Ok(())
}

/// `AddOrUpdateTaintOnNode` (taints.go:51-88): add taints to the node. If a
/// taint was added it writes the node, otherwise there is no write.
pub async fn add_or_update_taint_on_node<S: Storage>(
    storage: &S,
    node_name: &str,
    taints: &[&Taint],
) -> Result<(), Error> {
    if taints.is_empty() {
        return Ok(());
    }
    retry_on_conflict(|| async {
        let old_node: Node = storage.get(&build_key("nodes", None, node_name)).await?;
        let mut new_node = old_node.clone();
        let mut updated = false;
        for taint in taints {
            let (cur, ok) = add_or_update_taint(&new_node, taint);
            updated = updated || ok;
            new_node = cur;
        }
        if !updated {
            return Ok(());
        }
        patch_node_taints(storage, node_name, &old_node, &new_node).await
    })
    .await
}

/// `RemoveTaintOffNode` (taints.go:152-203): clean up taints temporarily
/// added to a node; does not fail if the target taint does not exist. If
/// passed a node it checks first whether there is anything to do and, if the
/// taint is not present, issues no API calls.
pub async fn remove_taint_off_node<S: Storage>(
    storage: &S,
    node_name: &str,
    node: Option<&Node>,
    taints: &[&Taint],
) -> Result<(), Error> {
    if taints.is_empty() {
        return Ok(());
    }
    // Short circuit for limiting amount of API calls.
    if let Some(node) = node {
        let have = node_taints(node);
        if !taints.iter().any(|t| taint_exists(&have, t)) {
            return Ok(());
        }
    }
    retry_on_conflict(|| async {
        let old_node: Node = storage.get(&build_key("nodes", None, node_name)).await?;
        let mut new_node = old_node.clone();
        let mut updated = false;
        for taint in taints {
            let (cur, ok) = remove_taint(&new_node, taint);
            updated = updated || ok;
            new_node = cur;
        }
        if !updated {
            return Ok(());
        }
        patch_node_taints(storage, node_name, &old_node, &new_node).await
    })
    .await
}
