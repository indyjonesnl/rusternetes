//! Node status report gating: change detection and the report-frequency
//! gate. Ports `tryUpdateNodeStatus` / `nodeStatusHasChanged` /
//! `isUpdateStatusPeriodExpired` / `calculateDelay`
//! (pkg/kubelet/kubelet_node_status.go:493-541, :745-790) and the 4% loop
//! jitter (`wait.JitterUntil(kl.syncNodeStatus, kl.nodeStatusUpdateFrequency,
//! 0.04, true, wait.NeverStop)`, pkg/kubelet/kubelet.go:1852).
//!
//! The node is kept Ready between full reports by the node-heartbeat Lease
//! (see the lease task in `Kubelet::run`), exactly as upstream: status is
//! computed every `nodeStatusUpdateFrequency` but only written when it
//! changed or `nodeStatusReportFrequency` elapsed.

use crate::image_gc::Image;
use rusternetes_common::resources::{ContainerImage, Node, NodeCondition, NodeStatus};
use serde_json::Value;
use std::time::{Duration, Instant};

/// `wait.JitterUntil` factor used for the status loop (kubelet.go:1852).
pub const NODE_STATUS_LOOP_JITTER_FACTOR: f64 = 0.04;

/// A uniform sample in [0, 1).
pub fn rand_unit() -> f64 {
    let b = uuid::Uuid::new_v4().as_u128();
    ((b >> 75) as u64 as f64) / ((1u64 << 53) as f64)
}

/// `wait.Jitter(d, factor)`: `d + rand[0,1) * factor * d`
/// (k8s.io/apimachinery/pkg/util/wait/backoff.go).
pub fn jitter(d: Duration, factor: f64, r: f64) -> Duration {
    d + d.mul_f64(r * factor)
}

/// `calculateDelay` (:539-541): `freq * (-0.5 + rand)`, in seconds, signed.
pub fn calculate_delay_secs(report_frequency: Duration, r: f64) -> f64 {
    report_frequency.as_secs_f64() * (-0.5 + r)
}

/// `isUpdateStatusPeriodExpired` (:535-537). A kubelet that never reported
/// (`last_report == None`, the zero `lastStatusReportTime`) is always expired.
pub fn is_update_status_period_expired(
    last_report: Option<Instant>,
    now: Instant,
    report_frequency: Duration,
    delay_secs: f64,
) -> bool {
    match last_report {
        None => true,
        Some(last) => {
            now.saturating_duration_since(last).as_secs_f64()
                >= report_frequency.as_secs_f64() + delay_secs
        }
    }
}

/// Drop nulls and empty arrays/objects: `apiequality.Semantic.DeepEqual`
/// treats nil and empty slices/maps as equal.
fn normalize(v: Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.into_iter()
                .map(|(k, v)| (k, normalize(v)))
                .filter(|(_, v)| !is_empty(v))
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.into_iter().map(normalize).collect()),
        other => other,
    }
}

fn is_empty(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Array(a) => a.is_empty(),
        Value::Object(m) => m.is_empty(),
        _ => false,
    }
}

/// `nodeConditionsHaveChanged` (:769-800): ignores `lastHeartbeatTime` and order.
pub fn node_conditions_have_changed(original: &[NodeCondition], current: &[NodeCondition]) -> bool {
    if original.len() != current.len() {
        return true;
    }
    let prep = |c: &[NodeCondition]| {
        let mut v: Vec<NodeCondition> = c.to_vec();
        v.sort_by(|a, b| a.condition_type.cmp(&b.condition_type));
        for c in &mut v {
            c.last_heartbeat_time = None;
        }
        serde_json::to_value(&v).unwrap_or(Value::Null)
    };
    prep(original) != prep(current)
}

/// `nodeStatusHasChanged` (:745-764).
pub fn node_status_has_changed(
    original: Option<&NodeStatus>,
    current: Option<&NodeStatus>,
) -> bool {
    let (o, c) = match (original, current) {
        (None, None) => return false,
        (Some(o), Some(c)) => (o, c),
        _ => return true,
    };
    if node_conditions_have_changed(
        o.conditions.as_deref().unwrap_or(&[]),
        c.conditions.as_deref().unwrap_or(&[]),
    ) {
        return true;
    }
    let strip = |s: &NodeStatus| {
        let mut s = s.clone();
        s.conditions = None;
        normalize(serde_json::to_value(&s).unwrap_or(Value::Null))
    };
    strip(o) != strip(c)
}

/// `MaxNamesPerImageInNodeStatus` (pkg/kubelet/nodestatus/setters.go:53).
pub const MAX_NAMES_PER_IMAGE_IN_NODE_STATUS: usize = 5;

/// `nodestatus.Images` (pkg/kubelet/nodestatus/setters.go:385). `image_list` is
/// `Kubelet.imageManager.GetImageList`'s result, already sorted by size
/// descending, so only truncation is needed. On error the node's image list is
/// reset to empty and the error is returned.
pub fn set_images(
    node: &mut Node,
    node_status_max_images: i32,
    image_list: anyhow::Result<Vec<Image>>,
) -> Result<(), String> {
    let Some(status) = node.status.as_mut() else {
        return Ok(());
    };
    let mut container_images = match image_list {
        Ok(l) => l,
        Err(err) => {
            status.images = None;
            return Err(format!("error getting image list: {err}"));
        }
    };
    // we expect the list to be sorted, so we just need to truncate
    if node_status_max_images > -1 && (node_status_max_images as usize) < container_images.len() {
        container_images.truncate(node_status_max_images as usize);
    }
    let images: Vec<ContainerImage> = container_images
        .into_iter()
        .map(|image| {
            let mut names: Vec<String> = image.repo_digests;
            names.extend(image.repo_tags);
            // Report up to MaxNamesPerImageInNodeStatus names per image.
            names.truncate(MAX_NAMES_PER_IMAGE_IN_NODE_STATUS);
            ContainerImage {
                names: if names.is_empty() { None } else { Some(names) },
                size_bytes: Some(image.size),
            }
        })
        .collect();
    status.images = if images.is_empty() {
        None
    } else {
        Some(images)
    };
    Ok(())
}

// ---------------------------------------------------------------------------
// PatchNodeStatus: two-way strategic-merge diff
// ---------------------------------------------------------------------------

/// Patch metadata of the `v1.Node` fields that carry `patchStrategy:"merge"`
/// and therefore diff as keyed/merged lists; every other list is replaced
/// whole (strategicpatch `handleSliceDiff` default arm,
/// staging/src/k8s.io/apimachinery/pkg/util/strategicpatch/patch.go:336).
/// `Some(key)` is a list of maps keyed by `patchMergeKey`; `None` is a merged
/// list of scalars. The set is exactly the Node's merge-strategy lists:
/// `metadata.finalizers`, `metadata.ownerReferences` (uid),
/// `status.conditions` (type) and `status.addresses` (type, see the
/// manual-addresses handling in [`prepare_patch_for_node_status`]).
fn merge_list_key(path: &str) -> Option<Option<&'static str>> {
    match path {
        "metadata.finalizers" => Some(None),
        "metadata.ownerReferences" => Some(Some("uid")),
        "status.conditions" | "status.addresses" => Some(Some("type")),
        _ => None,
    }
}

fn sv(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// `diffMaps` (patch.go:168-234) with `SetElementOrder: true`.
fn diff_maps(
    original: &serde_json::Map<String, Value>,
    modified: &serde_json::Map<String, Value>,
    path: &str,
) -> Result<serde_json::Map<String, Value>, String> {
    let mut patch = serde_json::Map::new();
    for (key, mv) in modified {
        let Some(ov) = original.get(key) else {
            patch.insert(key.clone(), mv.clone());
            continue;
        };
        let child = if path.is_empty() {
            key.clone()
        } else {
            format!("{path}.{key}")
        };
        match (ov, mv) {
            (Value::Object(o), Value::Object(m)) => {
                let sub = diff_maps(o, m, &child)?;
                if !sub.is_empty() {
                    patch.insert(key.clone(), Value::Object(sub));
                }
            }
            (Value::Array(o), Value::Array(m)) => match merge_list_key(&child) {
                Some(merge_key) => {
                    let (add, del, order) = diff_lists(o, m, merge_key)?;
                    if !add.is_empty() {
                        patch.insert(key.clone(), Value::Array(add));
                    }
                    if !del.is_empty() {
                        patch.insert(format!("$deleteFromPrimitiveList/{key}"), Value::Array(del));
                    }
                    if !order.is_empty() {
                        patch.insert(format!("$setElementOrder/{key}"), Value::Array(order));
                    }
                }
                None => {
                    if ov != mv {
                        patch.insert(key.clone(), mv.clone());
                    }
                }
            },
            _ => {
                // Differing types or scalars: replacePatchFieldIfNotEqual.
                if ov != mv {
                    patch.insert(key.clone(), mv.clone());
                }
            }
        }
    }
    // updatePatchIfMissing (:368-379): clear keys absent from modified.
    for key in original.keys() {
        if !modified.contains_key(key) {
            patch.insert(key.clone(), Value::Null);
        }
    }
    Ok(patch)
}

type ListDiff = (Vec<Value>, Vec<Value>, Vec<Value>);

/// `diffLists` (patch.go:548-615): (patchList, deleteList, setOrderList).
fn diff_lists(
    original: &[Value],
    modified: &[Value],
    merge_key: Option<&str>,
) -> Result<ListDiff, String> {
    if original.is_empty() {
        return Ok((modified.to_vec(), vec![], vec![]));
    }
    let Some(mk) = merge_key else {
        // diffListsOfScalars (:632-690)
        let mut add: Vec<Value> = modified
            .iter()
            .filter(|v| !original.contains(v))
            .cloned()
            .collect();
        let mut del: Vec<Value> = original
            .iter()
            .filter(|v| !modified.contains(v))
            .cloned()
            .collect();
        add.dedup();
        del.dedup();
        let order = if !del.is_empty() || original != modified {
            modified.to_vec()
        } else {
            vec![]
        };
        return Ok((add, del, order));
    };
    let key_of = |v: &Value| -> Result<String, String> {
        v.get(mk)
            .map(sv)
            .ok_or_else(|| format!("map: {v} does not contain declared merge key: {mk}"))
    };
    for v in original.iter().chain(modified.iter()) {
        key_of(v)?;
    }
    // diffListsOfMaps (:700-770): walk both lists sorted by merge key.
    let mut o: Vec<&Value> = original.iter().collect();
    let mut m: Vec<&Value> = modified.iter().collect();
    o.sort_by_key(|v| key_of(v).unwrap_or_default());
    m.sort_by_key(|v| key_of(v).unwrap_or_default());
    let (mut oi, mut mi) = (0, 0);
    let mut patch: Vec<Value> = vec![];
    let mut deletions: Vec<Value> = vec![];
    while oi < o.len() || mi < m.len() {
        let ok = o.get(oi).map(|v| key_of(v).unwrap_or_default());
        let mkv = m.get(mi).map(|v| key_of(v).unwrap_or_default());
        match (ok, mkv) {
            (Some(a), Some(b)) if a == b => {
                let (Value::Object(ao), Value::Object(bo)) = (o[oi], m[mi]) else {
                    return Err("list element is not a map".into());
                };
                let mut d = diff_maps(ao, bo, "")?;
                if !d.is_empty() {
                    d.insert(mk.to_string(), bo[mk].clone());
                    patch.push(Value::Object(d));
                }
                oi += 1;
                mi += 1;
            }
            (Some(a), Some(b)) if a > b => {
                patch.push(m[mi].clone());
                mi += 1;
            }
            (None, Some(_)) => {
                patch.push(m[mi].clone());
                mi += 1;
            }
            _ => {
                // CreateDeleteDirective: {mergeKey: v, "$patch": "delete"}
                let mut d = serde_json::Map::new();
                d.insert(mk.to_string(), o[oi][mk].clone());
                d.insert("$patch".into(), Value::String("delete".into()));
                deletions.push(Value::Object(d));
                oi += 1;
            }
        }
    }
    // normalizeSliceOrder: patch items follow `modified` order.
    let pos = |v: &Value| {
        let k = v.get(mk).map(sv).unwrap_or_default();
        modified
            .iter()
            .position(|x| x.get(mk).map(sv).unwrap_or_default() == k)
            .unwrap_or(usize::MAX)
    };
    patch.sort_by_key(pos);
    let order_same = original.len() == modified.len()
        && original
            .iter()
            .zip(modified)
            .all(|(a, b)| a.get(mk).map(sv) == b.get(mk).map(sv));
    patch.extend(deletions);
    let order = if !patch.is_empty() || !order_same {
        modified
            .iter()
            .map(|v| {
                let mut d = serde_json::Map::new();
                d.insert(mk.to_string(), v[mk].clone());
                Value::Object(d)
            })
            .collect()
    } else {
        vec![]
    };
    Ok((patch, vec![], order))
}

/// `preparePatchBytesforNodeStatus`
/// (staging/src/k8s.io/component-helpers/node/util/status.go:46-82): the
/// strategic-merge patch that turns `old` into `new`, restricted to
/// `metadata`/`status` (spec is reset to the old one so only those can be
/// patched, :57-61). `status.addresses` is wrongly annotated
/// `patchStrategy=merge`, so when it changed it is excluded from the diff and
/// sent as an explicit replace list (`fixupPatchForNodeStatusAddresses`,
/// :84-130).
///
/// Deviation: upstream drives the diff from the Go struct's patch-meta tags
/// via `CreateTwoWayMergePatch`; there is no Rust reflection equivalent, so
/// the four merge-strategy lists of `v1.Node` are tabulated in
/// [`merge_list_key`]. `$retainKeys` never applies to a Node.
pub fn prepare_patch_for_node_status(old: &Node, new: &Node) -> Result<Value, String> {
    let old_addrs = old
        .status
        .as_ref()
        .and_then(|s| s.addresses.clone())
        .unwrap_or_default();
    let new_addrs = new
        .status
        .as_ref()
        .and_then(|s| s.addresses.clone())
        .unwrap_or_default();
    let to_v = |a: &Vec<rusternetes_common::resources::NodeAddress>| {
        serde_json::to_value(a).map_err(|e| e.to_string())
    };
    let manually_patch_addresses = !old_addrs.is_empty() && to_v(&old_addrs)? != to_v(&new_addrs)?;

    let mut diff_node = new.clone();
    diff_node.spec = old.spec.clone();
    if manually_patch_addresses {
        if let Some(st) = diff_node.status.as_mut() {
            st.addresses = old.status.as_ref().and_then(|s| s.addresses.clone());
        }
    }
    let old_data = serde_json::to_value(old).map_err(|e| e.to_string())?;
    let new_data = serde_json::to_value(&diff_node).map_err(|e| e.to_string())?;
    let (Value::Object(o), Value::Object(n)) = (old_data, new_data) else {
        return Err("node did not serialize to an object".into());
    };
    let mut patch = diff_maps(&o, &n, "")?;
    if manually_patch_addresses {
        let mut addrs = to_v(&new_addrs)?.as_array().cloned().unwrap_or_default();
        addrs.push(serde_json::json!({"$patch": "replace"}));
        let status = patch
            .entry("status")
            .or_insert_with(|| Value::Object(Default::default()));
        match status {
            Value::Object(m) => {
                m.insert("addresses".into(), Value::Array(addrs));
            }
            _ => return Err("unexpected data in patch".into()),
        }
    }
    Ok(Value::Object(patch))
}
