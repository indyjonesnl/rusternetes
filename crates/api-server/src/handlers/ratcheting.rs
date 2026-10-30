//! CRD validation ratcheting (KEP-4008).
//!
//! When an UPDATE arrives, the new object may fail validation against a CRD
//! schema that has tightened constraints since the resource was created.
//! Ratcheting suppresses those failures for sub-trees that are *unchanged*
//! relative to the prior persisted value, provided each sub-tree along the
//! path is *correlatable* between old and new (object property name, or
//! list-type=map composite key).
//!
//! Upstream references:
//! - `staging/src/k8s.io/apiserver/pkg/cel/common/equality.go`
//!   (`CorrelatedObject`)
//! - `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/validation/ratcheting.go`
//!   (`RatchetingSchemaValidator`)
//! - `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/validation.go`
//!   (CEL ratcheting and transition-rule semantics)
//!
//! The algorithm is intentionally simple: for every validation issue
//! produced at a path under the candidate value, walk the schema + the old
//! value following the same path, correlating at each step. If every step
//! correlates AND the resulting sub-trees compare equal, the issue is
//! ratcheted (dropped). If any step is uncorrelatable (atomic/set arrays,
//! missing key, etc.) the issue is preserved.
//!
//! Transition rules (CEL rules whose source references `oldSelf`) are
//! *never* ratcheted — they are inherently update-only constraints.

use rusternetes_common::resources::crd::{
    JSONSchemaProps, JSONSchemaPropsOrArray, JSONSchemaPropsOrBool,
};
use rusternetes_common::schema_validation::PathSeg;
use serde_json::Value;

/// Resolve a path against the schema/old/new triple, returning the correlated
/// old and new values at the end of the path (or `None` if any segment along
/// the way is uncorrelatable).
///
/// Correlation rules (mirroring upstream `common.CorrelatedObject`):
/// - **object property**: the segment is `Key(name)`; old/new must both be
///   objects and both must contain `name`.
/// - **additionalProperties**: same shape as a named property.
/// - **list-type=map**: the segment is `Index(i)`; the new item at `i` is
///   keyed by its `x-kubernetes-list-map-keys`; the old list is searched
///   for the same composite key. Indexes do NOT correlate by position.
/// - **list-type=atomic|set|unset**: not correlatable, return `None`.
pub fn correlate<'a>(
    schema: &JSONSchemaProps,
    old: &'a Value,
    new: &'a Value,
    path: &[PathSeg],
) -> Option<(&'a Value, &'a Value)> {
    let mut cur_schema = schema;
    let mut cur_old = old;
    let mut cur_new = new;

    for seg in path {
        match seg {
            PathSeg::Key(name) => {
                let old_map = cur_old.as_object()?;
                let new_map = cur_new.as_object()?;
                let old_val = old_map.get(name)?;
                let new_val = new_map.get(name)?;

                // Locate the schema for this property — fall back to
                // additionalProperties if present.
                let prop_schema = cur_schema
                    .properties
                    .as_ref()
                    .and_then(|p| p.get(name))
                    .or(match cur_schema.additional_properties.as_deref() {
                        Some(JSONSchemaPropsOrBool::Schema(s)) => Some(s),
                        _ => None,
                    })?;
                cur_schema = prop_schema;
                cur_old = old_val;
                cur_new = new_val;
            }
            PathSeg::Index(i) => {
                let new_arr = cur_new.as_array()?;
                let item_new = new_arr.get(*i)?;

                // Only list-type=map is correlatable index-wise.
                let list_type = cur_schema.x_kubernetes_list_type.as_deref().unwrap_or("");
                if list_type != "map" {
                    return None;
                }
                let keys = cur_schema.x_kubernetes_list_map_keys.as_ref()?;
                if keys.is_empty() {
                    return None;
                }
                let composite_new = composite_key(item_new, keys)?;

                // Find the old item with the same composite key.
                let old_arr = cur_old.as_array()?;
                let mut matched_old: Option<&Value> = None;
                for o in old_arr.iter() {
                    if let Some(ck_old) = composite_key(o, keys) {
                        if ck_old == composite_new {
                            matched_old = Some(o);
                            break;
                        }
                    }
                }
                let item_old = matched_old?;

                // Advance into the item schema.
                let item_schema = match cur_schema.items.as_deref()? {
                    JSONSchemaPropsOrArray::Schema(s) => s,
                    // Tuple-style `items: [..]` is not used in K8s CRDs.
                    JSONSchemaPropsOrArray::Schemas(_) => return None,
                };
                cur_schema = item_schema;
                cur_old = item_old;
                cur_new = item_new;
            }
        }
    }

    Some((cur_old, cur_new))
}

/// Composite key for a list-type=map item. Returns `None` if any required
/// key is missing or not a scalar.
fn composite_key(item: &Value, keys: &[String]) -> Option<String> {
    let obj = item.as_object()?;
    let mut out = String::new();
    for k in keys {
        let v = obj.get(k)?;
        out.push('\x00');
        match v {
            Value::Bool(b) => out.push_str(&format!("b:{b}")),
            Value::Number(n) => out.push_str(&format!("n:{n}")),
            Value::String(s) => out.push_str(&format!("s:{s}")),
            _ => return None,
        }
    }
    Some(out)
}

/// Return true if the issue at `path` (against `schema_root`) is ratcheted —
/// i.e. the old and new values can be correlated all the way down AND they
/// compare equal at the terminal node.
pub fn is_ratcheted(
    schema_root: &JSONSchemaProps,
    old_root: &Value,
    new_root: &Value,
    path: &[PathSeg],
) -> bool {
    match correlate(schema_root, old_root, new_root, path) {
        Some((old_v, new_v)) => old_v == new_v,
        None => false,
    }
}
