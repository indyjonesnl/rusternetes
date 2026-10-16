//! PATCH operations implementation for Kubernetes API compatibility
//!
//! Supports three patch types:
//! 1. Strategic Merge Patch (Kubernetes-specific)
//! 2. JSON Merge Patch (RFC 7386)
//! 3. JSON Patch (RFC 6902)
//!
//! Lives in `common` (below both api-server and storage) so direct storage
//! backends can apply a patch themselves. Upstream layering: the applier is
//! apimachinery `pkg/util/strategicpatch/patch.go` (`StrategicMergePatch`,
//! :812), a library below apiserver; the apiserver patch handler
//! (`smpPatcher.applyPatchToCurrentObject`,
//! apiserver/pkg/endpoints/handlers/patch.go:446) only calls it. Moved
//! verbatim from `rusternetes-api-server`; behaviour unchanged.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;

mod mergepatch;
mod meta;
#[cfg(test)]
mod smp_error_tests;
#[cfg(test)]
mod smp_schema_tests;

use mergepatch::{err_bad_arg_type, err_bad_patch_type, err_no_merge_key, go_fmt_v, GO_SLICE};
pub use meta::PatchSchema;

/// Patch types supported by the API
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, PartialEq)]
pub enum PatchType {
    /// Strategic merge patch (Kubernetes-specific merge semantics)
    StrategicMergePatch,
    /// JSON merge patch (RFC 7386)
    JsonMergePatch,
    /// JSON patch (RFC 6902) - array of operations
    JsonPatch,
}

impl PatchType {
    /// Parse patch type from Content-Type header
    pub fn from_content_type(content_type: &str) -> Result<Self, PatchError> {
        match content_type {
            "application/strategic-merge-patch+json" => Ok(PatchType::StrategicMergePatch),
            "application/merge-patch+json" => Ok(PatchType::JsonMergePatch),
            "application/json-patch+json" => Ok(PatchType::JsonPatch),
            _ => Err(PatchError::UnsupportedContentType(content_type.to_string())),
        }
    }

    /// Get Content-Type header value for this patch type
    pub fn content_type(&self) -> &'static str {
        match self {
            PatchType::StrategicMergePatch => "application/strategic-merge-patch+json",
            PatchType::JsonMergePatch => "application/merge-patch+json",
            PatchType::JsonPatch => "application/json-patch+json",
        }
    }
}

/// Errors that can occur during patch operations
#[derive(Debug, thiserror::Error)]
pub enum PatchError {
    #[error("Unsupported content type: {0}")]
    UnsupportedContentType(String),

    #[error("Invalid patch document: {0}")]
    InvalidPatch(String),

    /// The JSON Patch document does not decode into operations —
    /// `jsonpatch.DecodePatch` failing, which `applyJSPatch` returns as
    /// `errors.NewBadRequest(err.Error())`
    /// (apiserver/pkg/endpoints/handlers/patch.go:404-407).
    #[error("Invalid patch document: {0}")]
    DecodeFailed(String),

    #[error("Patch operation failed: {0}")]
    OperationFailed(String),

    /// More than `maxJSONPatchOperations` operations: `applyJSPatch`'s
    /// `errors.NewRequestEntityTooLargeError(...)` (patch.go:407-411). Carries
    /// the message it wraps.
    #[error("{0}")]
    RequestEntityTooLarge(String),

    /// An error upstream's patch code returns as `errors.NewBadRequest(
    /// err.Error())`: the strategic-merge sentinels
    /// `ErrBadPatchFormatForRetainKeys` & co. (`interpretStrategicMergePatchError`,
    /// patch.go:797-801) and a merge patch's `jsonpatch.ErrBadJSONPatch`
    /// (patch.go:428-430).
    #[error("{0}")]
    BadRequest(String),

    /// An error upstream returns as a plain `error` (`fmt.Errorf`,
    /// `mergepatch.ErrNoMergeKey`, `ErrBadArgType`, ...): the default arm of
    /// `interpretStrategicMergePatchError` (patch.go:805-806) passes it on, and
    /// `ErrorToAPIStatus` renders it as a 500 with no reason.
    #[error("{0}")]
    Uninterpreted(String),

    /// `mergepatch.ErrNoListOfLists`: `interpretStrategicMergePatchError`
    /// answers it `NewGenericServerResponse(422, ...)` (apiserver
    /// patch.go:802-803).
    #[error("{0}")]
    UnprocessableEntity(String),

    /// JSON-Patch targeted a path that does not exist on the object.
    ///
    /// Carries the resolved dotted field path (e.g. `spec.doesNotExist`) so
    /// the caller can emit a `Status.details.causes[]` entry with reason
    /// `FieldValueNotFound` and that exact field.
    #[error("Path not found: {0}")]
    PathNotFound(String),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Resource version conflict")]
    ResourceVersionConflict,
}

/// Translate a JSON Pointer (RFC 6901) to upstream dotted field path —
/// `/spec/doesNotExist` becomes `spec.doesNotExist`,
/// `/spec/containers/0/name` becomes `spec.containers[0].name`. Numeric
/// segments are emitted as `[i]` so the result mirrors the breadcrumbs
/// upstream `field.Path` produces.
pub fn json_pointer_to_field_path(ptr: &str) -> String {
    if ptr.is_empty() || ptr == "/" {
        return String::new();
    }
    let parts: Vec<&str> = ptr.trim_start_matches('/').split('/').collect();
    let mut out = String::new();
    for part in parts {
        let unescaped = part.replace("~1", "/").replace("~0", "~");
        if unescaped.parse::<usize>().is_ok() {
            out.push('[');
            out.push_str(&unescaped);
            out.push(']');
        } else {
            if !out.is_empty() {
                out.push('.');
            }
            out.push_str(&unescaped);
        }
    }
    out
}

/// JSON Patch operation (RFC 6902)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonPatchOperation {
    pub op: JsonPatchOp,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
}

/// JSON Patch operation types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum JsonPatchOp {
    Add,
    Remove,
    Replace,
    Move,
    Copy,
    Test,
}

/// Apply a patch to a resource
pub fn apply_patch(
    original: &Value,
    patch: &Value,
    patch_type: PatchType,
) -> Result<Value, PatchError> {
    // Without the served kind, the object's own `apiVersion`/`kind` names it.
    let schema = PatchSchema::for_object(original);
    apply_patch_with_schema(original, patch, patch_type, schema.as_ref())
}

/// [`apply_patch`] against `schema`, the served kind's patch metadata
/// (`NewPatchMetaFromStruct(versionedObj)`, apiserver `patch.go`
/// `strategicPatchObject`). `None` has no struct to look up: a strategic
/// merge then infers each list's merge key from the patch items.
pub fn apply_patch_with_schema(
    original: &Value,
    patch: &Value,
    patch_type: PatchType,
    schema: Option<&PatchSchema>,
) -> Result<Value, PatchError> {
    match patch_type {
        PatchType::JsonMergePatch => {
            // `jsonpatch.MergePatch` (json-patch v4 merge.go:127-133, :144-149):
            // a patch that is `null` or neither an object nor an array is
            // `ErrBadJSONPatch`, which patch.go:428-430 makes a BadRequest.
            if !(patch.is_object() || patch.is_array()) {
                return Err(PatchError::BadRequest("Invalid JSON Patch".to_string()));
            }
            apply_merge_patch(original, patch)
        }
        PatchType::JsonPatch => apply_json_patch(original, patch),
        PatchType::StrategicMergePatch => smp_merge_map(original, patch, schema),
    }
}

/// Apply JSON Merge Patch (RFC 7386)
///
/// Rules:
/// - If patch is not an object, replace original with patch
/// - If patch is an object:
///   - For each key in patch:
///     - If value is null, delete key from original
///     - If value is an object and original[key] is an object, recursively merge
///     - Otherwise, replace original[key] with patch[key]
fn apply_merge_patch(original: &Value, patch: &Value) -> Result<Value, PatchError> {
    if !patch.is_object() {
        // Non-object patch replaces the entire value
        return Ok(patch.clone());
    }

    let mut result = if original.is_object() {
        original.clone()
    } else {
        json!({})
    };

    let result_obj = result
        .as_object_mut()
        .ok_or_else(|| PatchError::InvalidPatch("merge patch base is not an object".to_string()))?;
    let patch_obj = patch
        .as_object()
        .ok_or_else(|| PatchError::InvalidPatch("merge patch is not an object".to_string()))?;

    for (key, value) in patch_obj {
        if value.is_null() {
            // Null value deletes the key
            result_obj.remove(key);
        } else if value.is_object() && result_obj.get(key).is_some_and(|v| v.is_object()) {
            // Both are objects - recursively merge
            let merged = apply_merge_patch(&result_obj[key], value)?;
            result_obj.insert(key.clone(), merged);
        } else {
            // Replace or add the value
            result_obj.insert(key.clone(), value.clone());
        }
    }

    Ok(result)
}

/// `maxJSONPatchOperations` (apiserver/pkg/endpoints/handlers/patch.go:64).
pub const MAX_JSON_PATCH_OPERATIONS: usize = 10000;

/// Apply JSON Patch (RFC 6902)
fn apply_json_patch(original: &Value, patch: &Value) -> Result<Value, PatchError> {
    let operations: Vec<JsonPatchOperation> = serde_json::from_value(patch.clone())
        .map_err(|e| PatchError::DecodeFailed(e.to_string()))?;
    // patch.go:407-411, after `jsonpatch.DecodePatch` and before `Apply`.
    if operations.len() > MAX_JSON_PATCH_OPERATIONS {
        return Err(PatchError::RequestEntityTooLarge(format!(
            "The allowed maximum operations in a JSON patch is {}, got {}",
            MAX_JSON_PATCH_OPERATIONS,
            operations.len()
        )));
    }

    let mut current = original.clone();

    for op in operations {
        current = apply_json_patch_operation(&current, &op)?;
    }

    Ok(current)
}

/// Apply a single JSON Patch operation
fn apply_json_patch_operation(value: &Value, op: &JsonPatchOperation) -> Result<Value, PatchError> {
    let require_value = |what: &str| -> Result<&Value, PatchError> {
        op.value
            .as_ref()
            .ok_or_else(|| PatchError::InvalidPatch(format!("'value' required for {}", what)))
    };
    match op.op {
        JsonPatchOp::Add => add_operation(value, &op.path, require_value("add")?),
        JsonPatchOp::Remove => remove_operation(value, &op.path),
        JsonPatchOp::Replace => replace_operation(value, &op.path, require_value("replace")?),
        JsonPatchOp::Move => {
            let from = op
                .from
                .as_ref()
                .ok_or_else(|| PatchError::InvalidPatch("'from' required for move".to_string()))?;
            move_operation(value, from, &op.path)
        }
        JsonPatchOp::Copy => {
            let from = op
                .from
                .as_ref()
                .ok_or_else(|| PatchError::InvalidPatch("'from' required for copy".to_string()))?;
            copy_operation(value, from, &op.path)
        }
        JsonPatchOp::Test => {
            test_operation(value, &op.path, require_value("test")?)?;
            Ok(value.clone())
        }
    }
}

/// Add operation - adds a value at the specified path
fn add_operation(value: &Value, path: &str, new_value: &Value) -> Result<Value, PatchError> {
    let mut result = value.clone();
    let (parent_path, key) = split_path(path)?;

    if parent_path.is_empty() {
        // Adding to root
        return Ok(new_value.clone());
    }

    let parent = get_mut_value(&mut result, parent_path)?;

    if let Some(obj) = parent.as_object_mut() {
        obj.insert(key, new_value.clone());
    } else if let Some(arr) = parent.as_array_mut() {
        let index = if key == "-" {
            arr.len()
        } else {
            key.parse::<usize>()
                .map_err(|_| PatchError::InvalidPatch(format!("Invalid array index: {}", key)))?
        };
        arr.insert(index, new_value.clone());
    } else {
        return Err(PatchError::OperationFailed(format!(
            "Cannot add to non-object/array at path: {}",
            parent_path
        )));
    }

    Ok(result)
}

/// Remove operation - removes a value at the specified path.
///
/// Per RFC 6902 §4.2 the target location MUST exist; we emit
/// [`PatchError::PathNotFound`] (carrying the upstream-style dotted field
/// path) when the key/index is missing so the api-server can map it to a
/// `FieldValueNotFound` cause.
fn remove_operation(value: &Value, path: &str) -> Result<Value, PatchError> {
    let mut result = value.clone();
    let (parent_path, key) = split_path(path)?;

    let parent = get_mut_value(&mut result, parent_path)?;

    if let Some(obj) = parent.as_object_mut() {
        if obj.remove(&key).is_none() {
            return Err(PatchError::PathNotFound(json_pointer_to_field_path(path)));
        }
    } else if let Some(arr) = parent.as_array_mut() {
        let index: usize = key
            .parse()
            .map_err(|_| PatchError::InvalidPatch(format!("Invalid array index: {}", key)))?;
        if index >= arr.len() {
            return Err(PatchError::PathNotFound(json_pointer_to_field_path(path)));
        }
        arr.remove(index);
    } else {
        return Err(PatchError::OperationFailed(format!(
            "Cannot remove from non-object/array at path: {}",
            parent_path
        )));
    }

    Ok(result)
}

/// Replace operation - replaces a value at the specified path.
///
/// Per RFC 6902 §4.3 the target location MUST exist; we emit
/// [`PatchError::PathNotFound`] (carrying the upstream-style dotted field
/// path) when the key/index is missing — equivalent to remove + add where
/// the remove must succeed.
fn replace_operation(value: &Value, path: &str, new_value: &Value) -> Result<Value, PatchError> {
    let mut result = value.clone();

    if path.is_empty() || path == "/" {
        return Ok(new_value.clone());
    }

    let (parent_path, key) = split_path(path)?;
    let parent = get_mut_value(&mut result, parent_path)?;

    if let Some(obj) = parent.as_object_mut() {
        if !obj.contains_key(&key) {
            return Err(PatchError::PathNotFound(json_pointer_to_field_path(path)));
        }
        obj.insert(key, new_value.clone());
    } else if let Some(arr) = parent.as_array_mut() {
        let index: usize = key
            .parse()
            .map_err(|_| PatchError::InvalidPatch(format!("Invalid array index: {}", key)))?;
        if index >= arr.len() {
            return Err(PatchError::PathNotFound(json_pointer_to_field_path(path)));
        }
        arr[index] = new_value.clone();
    } else {
        return Err(PatchError::OperationFailed(format!(
            "Cannot replace in non-object/array at path: {}",
            parent_path
        )));
    }

    Ok(result)
}

/// Move operation - moves a value from one path to another. RFC 6902 §4.4
/// forbids `from` being a proper prefix of `path` (a location cannot be
/// moved into one of its own children).
fn move_operation(value: &Value, from: &str, to: &str) -> Result<Value, PatchError> {
    if is_proper_prefix(from, to) {
        return Err(PatchError::InvalidPatch(format!(
            "move: 'from' location ({}) must not be a proper prefix of 'path' ({}) per RFC 6902 §4.4",
            from, to
        )));
    }
    // Get the value at 'from'
    let moved_value = get_value(value, from)?;
    // Remove from 'from' location
    let mut result = remove_operation(value, from)?;
    // Add to 'to' location
    result = add_operation(&result, to, &moved_value)?;
    Ok(result)
}

/// `from` is a proper prefix of `to` when `to == from` or `to` begins with
/// `from` followed by a `/` (so `/a/b` is a prefix of `/a/b/c` but not of
/// `/a/bb`). Comparison is on the raw escaped pointer per RFC 6901.
fn is_proper_prefix(from: &str, to: &str) -> bool {
    if from == to {
        return true;
    }
    to.strip_prefix(from)
        .is_some_and(|rest| rest.starts_with('/'))
}

/// Copy operation - copies a value from one path to another
fn copy_operation(value: &Value, from: &str, to: &str) -> Result<Value, PatchError> {
    let copied_value = get_value(value, from)?;
    add_operation(value, to, &copied_value)
}

/// Test operation - tests that a value at the specified path equals the given value
fn test_operation(value: &Value, path: &str, test_value: &Value) -> Result<(), PatchError> {
    let current_value = get_value(value, path)?;
    if current_value != *test_value {
        return Err(PatchError::OperationFailed(format!(
            "Test failed at path: {}",
            path
        )));
    }
    Ok(())
}

/// Apply Strategic Merge Patch (Kubernetes-specific)
///
/// Implements strategic merge with directive markers:
/// - `$patch`: Specifies merge strategy ("replace", "merge", "delete")
/// - `$retainKeys`: List of keys to retain when using replace strategy
/// - `$deleteFromPrimitiveList/<field>`: Values to delete from primitive arrays
/// - Arrays with items that have a 'name' field are merged by name
/// - Other arrays replace the original (unless directives specify otherwise)
/// - Objects are recursively merged
/// - Null values delete keys
fn apply_strategic_merge_patch(original: &Value, patch: &Value) -> Result<Value, PatchError> {
    smp_merge_map(original, patch, None)
}

/// `mergeMap` (strategicpatch/patch.go:1316) with `MergeParallelList` and
/// `IgnoreUnmatchedNulls`, as `StrategicMergeMapPatchUsingLookupPatchMeta`
/// runs it (:875-881). `schema` is `None` only where no kind is known.
fn smp_merge_map(
    original: &Value,
    patch: &Value,
    schema: Option<&PatchSchema>,
) -> Result<Value, PatchError> {
    if !patch.is_object() {
        return Ok(patch.clone());
    }

    let mut result = if original.is_object() {
        original.clone()
    } else {
        json!({})
    };

    let result_obj = result.as_object_mut().ok_or_else(|| {
        PatchError::InvalidPatch("strategic merge base is not an object".to_string())
    })?;
    let patch_obj = patch.as_object().ok_or_else(|| {
        PatchError::InvalidPatch("strategic merge patch is not an object".to_string())
    })?;

    // `$patch` directive: `handleDirectiveInMergeMap` (patch.go:906-923)
    // knows only `replace` and `delete`; any other value (even `merge`) is
    // `ErrBadPatchType` over the whole patch map.
    let patch_strategy = match patch_obj.get("$patch") {
        None => "merge",
        Some(Value::String(s)) if s == "replace" || s == "delete" => s.as_str(),
        Some(other) => return Err(PatchError::Uninterpreted(err_bad_patch_type(other, patch))),
    };

    // Check for $retainKeys directive
    let retain_keys: Option<Vec<String>> = patch_obj
        .get("$retainKeys")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        });

    match patch_strategy {
        "replace" => {
            // Replace strategy - replace entire object but retain specified keys
            if let Some(keys_to_retain) = retain_keys {
                let mut new_obj = serde_json::Map::new();
                // First, copy retained keys from original
                for key in &keys_to_retain {
                    if let Some(value) = result_obj.get(key) {
                        new_obj.insert(key.clone(), value.clone());
                    }
                }
                // Then apply patch values (excluding directives)
                for (key, value) in patch_obj {
                    if !key.starts_with('$') {
                        new_obj.insert(key.clone(), value.clone());
                    }
                }
                return Ok(Value::Object(new_obj));
            } else {
                // Full replacement (excluding directives)
                let mut new_obj = serde_json::Map::new();
                for (key, value) in patch_obj {
                    if !key.starts_with('$') {
                        new_obj.insert(key.clone(), value.clone());
                    }
                }
                return Ok(Value::Object(new_obj));
            }
        }
        "delete" => {
            // Upstream parity: `$patch: delete` on a map clears the
            // original. If the patch carries sibling (non-directive)
            // keys, treat the directive as "start from an empty map and
            // merge the siblings" so combined delete + add bodies work
            // (e.g. `{$patch: delete, new: value}`).
            let mut new_obj = serde_json::Map::new();
            let mut had_siblings = false;
            for (key, value) in patch_obj {
                if key.starts_with('$') {
                    continue;
                }
                had_siblings = true;
                if !value.is_null() {
                    new_obj.insert(key.clone(), value.clone());
                }
            }
            if had_siblings {
                return Ok(Value::Object(new_obj));
            }
            return Ok(Value::Null);
        }
        _ => {
            // Default merge strategy

            // `applyRetainKeysDirective` (patch.go:1047-1100): validate the
            // patch against `$retainKeys`, then clear original keys that
            // are not listed. Runs before `mergePatchIntoOriginal`
            // (patch.go:1333-1342).
            if let Some(retain) = patch_obj.get("$retainKeys") {
                apply_retain_keys_directive(result_obj, patch_obj, retain)?;
            }

            // Upstream `mergePatchIntoOriginal` (patch.go:1110-1230): a
            // `$setElementOrder/<field>` directive merges the `<field>` list
            // together with its order list, then drops both from the patch.
            let consumed = merge_set_element_order_lists(result_obj, patch_obj, schema)?;

            for (key, patch_value) in patch_obj {
                // Skip directive keys
                if key.starts_with('$') {
                    continue;
                }
                if consumed.contains(key) {
                    continue;
                }

                // With the kind's schema, a key present in both the live
                // object and the patch, as two maps or two lists, is
                // merged as `mergeMap` (patch.go:1394-1424) does: look up
                // the field's patch metadata, then `mergeMapHandler` /
                // `mergeSliceHandler` (:1451-1488) pick merge or replace.
                if let (Some(schema), false) = (schema, patch_value.is_null()) {
                    let handled = match (result_obj.get(key), patch_value) {
                        (Some(Value::Object(_)), Value::Object(_)) => {
                            let (sub, meta) = schema.lookup_struct(key).map_err(lookup_error)?;
                            let (_, strategy) = extract_retain_keys_strategy(&meta.strategies)?;
                            Some(if strategy == "replace" {
                                patch_value.clone()
                            } else {
                                smp_merge_map(&result_obj[key], patch_value, Some(&sub))?
                            })
                        }
                        (Some(Value::Array(orig)), Value::Array(patch_arr)) => {
                            let (sub, meta) = schema.lookup_slice(key).map_err(lookup_error)?;
                            let (_, strategy) = extract_retain_keys_strategy(&meta.strategies)?;
                            Some(if strategy == "merge" {
                                Value::Array(merge_slice(
                                    orig,
                                    patch_arr,
                                    Some(&sub),
                                    &meta.merge_key,
                                )?)
                            } else {
                                patch_value.clone()
                            })
                        }
                        _ => None,
                    };
                    if let Some(merged) = handled {
                        result_obj.insert(key.clone(), merged);
                        continue;
                    }
                }

                if patch_value.is_null() {
                    // Null deletes the key
                    result_obj.remove(key);
                } else if patch_value.is_array()
                    && result_obj.get(key).is_some_and(|v| v.is_array())
                    && patch_obj.contains_key(&format!("{DELETE_FROM_PRIMITIVE_LIST}/{key}"))
                    && is_scalar_list(patch_value.as_array().unwrap())
                    && is_scalar_list(result_obj[key].as_array().unwrap())
                {
                    // A parallel `$deleteFromPrimitiveList/<key>` marks a
                    // merge-strategy list of primitives: `mergeSlice`
                    // (patch.go:1510-1540) appends + deduplicates + orders.
                    let merged = merge_scalar_lists(
                        result_obj[key].as_array().unwrap(),
                        patch_value.as_array().unwrap(),
                    );
                    result_obj.insert(key.clone(), Value::Array(merged));
                } else if let (Some(Value::Array(orig_arr)), Value::Array(patch_arr)) =
                    (result_obj.get(key), patch_value)
                {
                    // Strategic merge for arrays
                    let merged_array = strategic_merge_arrays_for_field(key, orig_arr, patch_arr)?;
                    result_obj.insert(key.clone(), Value::Array(merged_array));
                } else if patch_value.is_object()
                    && result_obj.get(key).is_some_and(|v| v.is_object())
                {
                    // Recursively merge objects
                    let merged = smp_merge_map(&result_obj[key], patch_value, None)?;
                    result_obj.insert(key.clone(), merged);
                } else {
                    // Not in the live object, or of another type: the patch
                    // value goes in with its directives removed
                    // (`removeDirectives`, patch.go:1423-1428, :1445-1450);
                    // a value that is itself a directive map is dropped.
                    match remove_directives_deep(patch_value) {
                        Some(v) => {
                            result_obj.insert(key.clone(), v);
                        }
                        None => {
                            result_obj.remove(key);
                        }
                    }
                }
            }

            // `$deleteFromPrimitiveList/<field>` (patch.go:1349-1420,
            // `preprocessDeletionListForMerging` + `deleteFromSlice`): remove
            // the listed values from the original `<field>` list. Absent
            // originals are left alone (patch_test.go:681-730). The value
            // then runs through the rest of `mergeMap` (patch.go:1366-1426)
            // with `isDeleteList` set: `null` deletes the field, a value of
            // a different Go type than the live field is skipped, two maps
            // merge, and two equal-typed scalars replace.
            for (key, patch_value) in patch_obj {
                let Some(field) = key.strip_prefix(&format!("{DELETE_FROM_PRIMITIVE_LIST}/"))
                else {
                    continue;
                };
                if patch_value.is_null() {
                    result_obj.remove(field);
                    continue;
                }
                let Some(current) = result_obj.get_mut(field) else {
                    continue;
                };
                if mergepatch::go_type_name(current) != mergepatch::go_type_name(patch_value) {
                    continue;
                }
                match (current, patch_value) {
                    (Value::Array(current), Value::Array(to_delete)) => {
                        current.retain(|v| !to_delete.contains(v));
                    }
                    (current @ Value::Object(_), Value::Object(_)) => {
                        let sub = match schema {
                            Some(s) => Some(s.lookup_struct(field).map_err(lookup_error)?.0),
                            None => None,
                        };
                        *current = smp_merge_map(current, patch_value, sub.as_ref())?;
                    }
                    (current, _) => *current = patch_value.clone(),
                }
            }
        }
    }

    Ok(result)
}

/// Port of `applyRetainKeysDirective` (patch.go:1070-1099) for the
/// `MergeParallelList` (apply-a-patch-to-an-object) case: a non-list
/// directive, or a non-null patch key absent from the list, is
/// `ErrBadPatchFormatForRetainKeys`; original keys not listed are cleared.
fn apply_retain_keys_directive(
    original: &mut serde_json::Map<String, Value>,
    patch: &serde_json::Map<String, Value>,
    retain: &Value,
) -> Result<(), PatchError> {
    let bad = || PatchError::BadRequest("invalid patch format of retainKeys".to_string());
    let list = retain.as_array().ok_or_else(bad)?;
    let keys: std::collections::HashSet<&str> = list.iter().filter_map(|v| v.as_str()).collect();
    for (k, v) in patch {
        // `$retainKeys` itself is deleted from the patch upstream before
        // this loop; `$patch` never reaches here (handled as a directive).
        if v.is_null()
            || k == "$retainKeys"
            || k == "$patch"
            || k.starts_with(&format!("{DELETE_FROM_PRIMITIVE_LIST}/"))
            || k.starts_with(&format!("{SET_ELEMENT_ORDER}/"))
        {
            continue;
        }
        if !keys.contains(k.as_str()) {
            return Err(bad());
        }
    }
    original.retain(|k, _| keys.contains(k.as_str()));
    Ok(())
}

const DELETE_FROM_PRIMITIVE_LIST: &str = "$deleteFromPrimitiveList";
const SET_ELEMENT_ORDER: &str = "$setElementOrder";

fn is_scalar_list(list: &[Value]) -> bool {
    list.iter().all(|v| !v.is_object() && !v.is_array())
}

/// Identity of a list item for ordering: the value itself for primitives
/// (`index` default branch, patch.go:486), the merge key for maps
/// (patch.go:477). `None` for a map without its merge key.
type ItemId<'a> = &'a dyn Fn(&Value) -> Option<String>;

fn scalar_id(v: &Value) -> Option<String> {
    Some(v.to_string())
}

fn index_of(list: &[Value], v: &Value, id: ItemId) -> Option<usize> {
    let target = id(v)?;
    list.iter().position(|x| id(x).as_ref() == Some(&target))
}

/// `normalizeSliceOrder` (patch.go:521) + `normalizeElementOrder`
/// (patch.go:404) + `mergeSortedSlice` (patch.go:427), after
/// `partitionPrimitivesByPresentInList` / `partitionMapsByPresentInList`
/// (patch.go:1236, 1254).
///
/// `merge_key` is `Some` for a list of maps: `partitionMapsByPresentInList`
/// (patch.go:1262-1265) fails on the first merged map lacking it with
/// `ErrNoMergeKey`.
fn normalize_order(
    merged: &[Value],
    patch_order: &[Value],
    server_order: &[Value],
    id: ItemId,
    merge_key: Option<&str>,
) -> Result<Vec<Value>, PatchError> {
    if let Some(key) = merge_key {
        if let Some(bad) = merged.iter().find(|v| v.is_object() && id(v).is_none()) {
            return Err(PatchError::Uninterpreted(err_no_merge_key(bad, key)));
        }
    }
    let (mut patch_items, mut server_only): (Vec<Value>, Vec<Value>) = merged
        .iter()
        .cloned()
        .partition(|v| index_of(patch_order, v, id).is_some());
    patch_items.sort_by_key(|v| index_of(patch_order, v, id));
    server_only.sort_by_key(|v| index_of(server_order, v, id));
    let (left, right) = (server_only, patch_items);
    let less =
        |l: &Value, r: &Value| match (index_of(server_order, l, id), index_of(server_order, r, id))
        {
            (Some(li), Some(ri)) => li < ri,
            _ => false,
        };
    let mut out = Vec::with_capacity(left.len() + right.len());
    let (mut i, mut j) = (0, 0);
    while i < left.len() || j < right.len() {
        if i >= left.len() {
            out.push(right[j].clone());
            j += 1;
        } else if j >= right.len() || less(&left[i], &right[j]) {
            out.push(left[i].clone());
            i += 1;
        } else {
            out.push(right[j].clone());
            j += 1;
        }
    }
    Ok(out)
}

/// Ported from `deduplicateScalars` (patch.go:1846) followed by
/// `normalizeElementOrder` (patch.go:404-416): union of two scalar lists,
/// patch-listed items ordered by `patch_order`, server-only items by the
/// original order, then interleaved by `mergeSortedSlice`.
fn merge_scalar_lists(original: &[Value], patch: &[Value]) -> Vec<Value> {
    let mut merged: Vec<Value> = Vec::new();
    for v in original.iter().chain(patch.iter()) {
        if !merged.contains(v) {
            merged.push(v.clone());
        }
    }
    normalize_order(&merged, patch, original, &scalar_id, None).expect("scalars always have an id")
}

/// `validatePatchWithSetOrderList` (patch.go:977-1021). For lists of maps
/// `$patch: delete` items are dropped first (patch.go:992) and items that
/// carry a directive are skipped (patch.go:1003). `merge_key` is `Some` for
/// a list of maps; `mergeKeyValueEqual` (patch.go:947-954) reports the
/// patch item, then the order item, lacking it as `ErrNoMergeKey`.
fn validate_patch_with_set_order_list(
    patch_list: &[Value],
    order: &[Value],
    id: ItemId,
    merge_key: Option<&str>,
) -> Result<(), PatchError> {
    let is_map_list = merge_key.is_some();
    if patch_list.is_empty() || order.is_empty() {
        return Ok(());
    }
    let non_delete: Vec<&Value> = patch_list
        .iter()
        .filter(|v| !(is_map_list && is_delete_directive(v)))
        .collect();
    let (mut p, mut o) = (0, 0);
    while p < non_delete.len() && o < order.len() {
        if is_map_list
            && non_delete[p]
                .as_object()
                .is_some_and(|m| m.contains_key("$patch"))
        {
            p += 1;
            continue;
        }
        let (a, b) = (id(non_delete[p]), id(&order[o]));
        if let Some(key) = merge_key {
            if a.is_none() {
                return Err(PatchError::Uninterpreted(err_no_merge_key(
                    non_delete[p],
                    key,
                )));
            }
            if b.is_none() {
                return Err(PatchError::Uninterpreted(err_no_merge_key(&order[o], key)));
            }
        }
        if a == b {
            p += 1;
        }
        o += 1;
    }
    if p < non_delete.len() && o >= order.len() {
        // patch.go:1019 renders both lists with `%v`.
        return Err(PatchError::Uninterpreted(format!(
            "The order in patch list:\n{}\n doesn't match {SET_ELEMENT_ORDER} list:\n{}\n",
            go_fmt_v(&Value::Array(patch_list.to_vec())),
            go_fmt_v(&Value::Array(order.to_vec())),
        )));
    }
    Ok(())
}

/// `removeDirectives` (patch.go:1282): drop objects carrying `$patch`.
fn remove_directives(list: &[Value]) -> Vec<Value> {
    list.iter()
        .filter(|v| !v.as_object().is_some_and(|m| m.contains_key("$patch")))
        .cloned()
        .collect()
}

/// Port of `mergePatchIntoOriginal` (patch.go:1110-1230): merge `<field>`
/// with its `$setElementOrder/<field>` list and enforce the order
/// (precedence: `$setElementOrder` > patch list > live list). Returns the
/// fields it consumed so the caller skips them.
///
/// Upstream finds the merge key through `schema.LookupPatchMetadataForSlice`
/// (patch.go:1165); with a `schema` that is what runs. Without one (no
/// kind known) the key of a list of maps is derived from the order list
/// itself via `detect_merge_key_strategy` (the same heuristic
/// `strategic_merge_arrays` uses); an order list that yields no key leaves
/// the field to the ordinary merge.
fn merge_set_element_order_lists(
    result_obj: &mut serde_json::Map<String, Value>,
    patch_obj: &serde_json::Map<String, Value>,
    schema: Option<&PatchSchema>,
) -> Result<std::collections::HashSet<String>, PatchError> {
    let mut consumed = std::collections::HashSet::new();
    let prefix = format!("{SET_ELEMENT_ORDER}/");
    for (key, order_v) in patch_obj {
        let Some(field) = key.strip_prefix(&prefix) else {
            continue;
        };
        // patch.go:1141-1164: the order list, then the live field, then the
        // patch field must be a `[]interface{}` (a present `null` included),
        // else `ErrBadArgType`.
        let bad_arg = |v: &Value| PatchError::Uninterpreted(err_bad_arg_type(GO_SLICE, v));
        let order = order_v.as_array().ok_or_else(|| bad_arg(order_v))?;
        let original = match result_obj.get(field) {
            Some(Value::Array(a)) => Some(a.clone()),
            None => None,
            Some(other) => return Err(bad_arg(other)),
        };
        let patch_list = match patch_obj.get(field) {
            Some(Value::Array(a)) => Some(a.clone()),
            None => None,
            Some(other) => return Err(bad_arg(other)),
        };
        if let Some(schema) = schema {
            // patch.go:1165-1226: the field's own metadata drives the merge
            // key and strategy.
            let (sub, meta) = schema.lookup_slice(field).map_err(lookup_error)?;
            let (_, strategy) = extract_retain_keys_strategy(&meta.strategies)?;
            let key = meta.merge_key.as_str();
            let id = |v: &Value| -> Option<String> {
                if key.is_empty() {
                    scalar_id(v)
                } else {
                    v.as_object().and_then(|m| m.get(key)).map(Value::to_string)
                }
            };
            let merge_key = (!key.is_empty()).then_some(key);
            validate_patch_with_set_order_list(
                patch_list.as_deref().unwrap_or(&[]),
                order,
                &id,
                merge_key,
            )?;
            let merged = match (&original, &patch_list) {
                (None, None) => continue,
                (Some(o), None) => o.clone(),
                (None, Some(p)) => match remove_directives_deep(&Value::Array(p.clone())) {
                    Some(Value::Array(a)) => a,
                    _ => continue,
                },
                (Some(o), Some(p)) if strategy == "merge" => merge_slice(o, p, Some(&sub), key)?,
                (Some(_), Some(p)) => p.clone(),
            };
            let server_order = original.unwrap_or_default();
            slice_elements_are_maps(&[&server_order, patch_list.as_deref().unwrap_or(&[])])?;
            let ordered = normalize_order(&merged, order, &server_order, &id, merge_key)?;
            result_obj.insert(field.to_string(), Value::Array(ordered));
            consumed.insert(field.to_string());
            continue;
        }
        let all_scalar = original.as_deref().is_none_or(is_scalar_list)
            && patch_list.as_deref().is_none_or(is_scalar_list)
            && is_scalar_list(order);
        let strategy = if all_scalar {
            None
        } else {
            let all_maps = |l: &[Value]| l.iter().all(|v| v.is_object());
            if !(original.as_deref().is_none_or(all_maps)
                && patch_list.as_deref().is_none_or(all_maps)
                && all_maps(order))
            {
                continue;
            }
            match detect_merge_key_strategy(order) {
                Some(s) => Some(s),
                None => continue,
            }
        };
        let id = move |v: &Value| match strategy {
            None => scalar_id(v),
            Some(s) => merge_key_with(v, s),
        };
        let merge_key = strategy.map(MergeKeyStrategy::field);
        let is_map_list = merge_key.is_some();
        validate_patch_with_set_order_list(
            patch_list.as_deref().unwrap_or(&[]),
            order,
            &id,
            merge_key,
        )?;
        let merged = match (&original, &patch_list) {
            (None, None) => continue,
            (Some(o), None) => o.clone(),
            (None, Some(p)) => {
                if is_map_list {
                    remove_directives(p)
                } else {
                    p.clone()
                }
            }
            (Some(o), Some(p)) if is_map_list => strategic_merge_arrays(o, p)?,
            (Some(o), Some(p)) => {
                let mut m: Vec<Value> = Vec::new();
                for v in o.iter().chain(p.iter()) {
                    if !m.contains(v) {
                        m.push(v.clone());
                    }
                }
                m
            }
        };
        let server_order = original.unwrap_or_default();
        let ordered = normalize_order(&merged, order, &server_order, &id, merge_key)?;
        result_obj.insert(field.to_string(), Value::Array(ordered));
        consumed.insert(field.to_string());
    }
    Ok(consumed)
}

/// `removeDirectives` (patch.go:1282-1311): a map carrying `$patch` is
/// dropped (`None`); maps and lists are cleaned recursively.
fn remove_directives_deep(v: &Value) -> Option<Value> {
    match v {
        Value::Object(m) => {
            if m.contains_key("$patch") {
                return None;
            }
            Some(Value::Object(
                m.iter()
                    .filter_map(|(k, v)| remove_directives_deep(v).map(|v| (k.clone(), v)))
                    .collect(),
            ))
        }
        Value::Array(a) => Some(Value::Array(
            a.iter().filter_map(remove_directives_deep).collect(),
        )),
        other => Some(other.clone()),
    }
}

/// A failed `LookupPatchMetadataFor*` (meta.go): a plain `error`, so the
/// apiserver answers it as a reason-less 500 (patch.go:805-806).
fn lookup_error(msg: String) -> PatchError {
    PatchError::Uninterpreted(msg)
}

/// `extractRetainKeysPatchStrategy` (patch.go:1778-1805): split a field's
/// strategies into "has `retainKeys`" and the other strategy.
fn extract_retain_keys_strategy(strategies: &[String]) -> Result<(bool, String), PatchError> {
    match strategies {
        [] => Ok((false, String::new())),
        [one] if one == "retainKeys" => Ok((true, String::new())),
        [one] => Ok((false, one.clone())),
        [a, b] if a == "retainKeys" => Ok((true, b.clone())),
        [a, b] if b == "retainKeys" => Ok((true, a.clone())),
        _ => Err(PatchError::Uninterpreted(format!(
            "unexpected patch strategy: [{}]",
            strategies.join(" ")
        ))),
    }
}

/// `sliceElementType` (patch.go:1868-1895) reduced to what `mergeSlice`
/// needs: whether the elements are maps. All elements must share one Go
/// type, and a list of lists is `ErrNoListOfLists`.
fn slice_elements_are_maps(slices: &[&[Value]]) -> Result<bool, PatchError> {
    let mut prev: Option<Option<&'static str>> = None;
    for s in slices {
        for v in *s {
            let cur = mergepatch::go_type_name(v);
            match prev {
                None => {
                    prev = Some(cur);
                    if v.is_array() {
                        return Err(PatchError::UnprocessableEntity(
                            "lists of lists are not supported".to_string(),
                        ));
                    }
                }
                Some(p) if p != cur => {
                    let all =
                        Value::Array(slices.iter().map(|s| Value::Array(s.to_vec())).collect());
                    return Err(PatchError::Uninterpreted(format!(
                        "list element types are not identical: {}",
                        go_fmt_v(&all)
                    )));
                }
                Some(_) => {}
            }
        }
    }
    match prev {
        None => Err(PatchError::Uninterpreted(
            "no elements in any of the given slices".to_string(),
        )),
        Some(t) => Ok(t == Some("map[string]interface {}")),
    }
}

/// `findMapInSliceBasedOnKeyValue` (patch.go:1692-1708): the first element
/// whose `key` equals `value`; a non-map element is an error.
fn find_map_in_slice(
    list: &[Value],
    key: &str,
    value: &Value,
) -> Result<Option<usize>, PatchError> {
    for (i, v) in list.iter().enumerate() {
        let Some(m) = v.as_object() else {
            return Err(PatchError::Uninterpreted(format!(
                "value for key {i} is not a map"
            )));
        };
        if m.get(key) == Some(value) {
            return Ok(Some(i));
        }
    }
    Ok(None)
}

/// `mergeSlice` (patch.go:1500-1565): merge `patch` into `original` for a
/// field whose strategy is `merge`. `schema` is the element's schema and
/// `merge_key` the field's `patchMergeKey` (empty for a list of scalars).
fn merge_slice(
    original: &[Value],
    patch: &[Value],
    schema: Option<&PatchSchema>,
    merge_key: &str,
) -> Result<Vec<Value>, PatchError> {
    if original.is_empty() && patch.is_empty() {
        return Ok(original.to_vec());
    }
    let is_map = slice_elements_are_maps(&[original, patch])?;
    if !is_map {
        // Union, then `normalizeElementOrder` over the patch order and the
        // live order (`deduplicateScalars` + :1556-1563).
        return Ok(merge_scalar_lists(original, patch));
    }
    if merge_key.is_empty() {
        return Err(PatchError::Uninterpreted(format!(
            "cannot merge lists without merge key for {}",
            schema.map(PatchSchema::name).unwrap_or_default()
        )));
    }
    let (original, patch) = merge_slice_with_special_elements(original, patch, merge_key)?;
    let merged = merge_slice_without_special_elements(original.clone(), &patch, merge_key, schema)?;
    let id = |v: &Value| {
        v.as_object()
            .and_then(|m| m.get(merge_key))
            .map(Value::to_string)
    };
    normalize_order(&merged, &patch, &original, &id, Some(merge_key))
}

/// `mergeSliceWithSpecialElements` (patch.go:1567-1607): apply the `$patch`
/// elements of `patch` and return the live list after any deletes plus the
/// patch without them (`replace` makes the patch items the live list and the
/// patch empty, patch.go:1583-1585).
fn merge_slice_with_special_elements(
    original: &[Value],
    patch: &[Value],
    merge_key: &str,
) -> Result<(Vec<Value>, Vec<Value>), PatchError> {
    let mut original = original.to_vec();
    let mut plain = Vec::new();
    let mut replace = false;
    for v in patch {
        let m = v.as_object().expect("elements are maps");
        let Some(directive) = m.get("$patch") else {
            plain.push(v.clone());
            continue;
        };
        match directive.as_str() {
            Some("delete") => {
                let Some(value) = m.get(merge_key) else {
                    return Err(PatchError::Uninterpreted(err_no_merge_key(v, merge_key)));
                };
                // `deleteMatchingEntries` (:1609-1625).
                while let Some(i) = find_map_in_slice(&original, merge_key, value)? {
                    original.remove(i);
                }
            }
            Some("replace") => replace = true,
            Some("merge") => {
                return Err(PatchError::Uninterpreted(
                    "merging lists cannot yet be specified in the patch".to_string(),
                ));
            }
            _ => {
                return Err(PatchError::Uninterpreted(err_bad_patch_type(directive, v)));
            }
        }
    }
    if replace {
        return Ok((plain, Vec::new()));
    }
    Ok((original, plain))
}

/// `mergeSliceWithoutSpecialElements` (patch.go:1627-1660): merge each patch
/// map into the live map with the same merge key, or append it as given.
fn merge_slice_without_special_elements(
    mut original: Vec<Value>,
    patch: &[Value],
    merge_key: &str,
    schema: Option<&PatchSchema>,
) -> Result<Vec<Value>, PatchError> {
    for v in patch {
        let m = v.as_object().expect("elements are maps");
        let Some(value) = m.get(merge_key) else {
            return Err(PatchError::Uninterpreted(err_no_merge_key(v, merge_key)));
        };
        match find_map_in_slice(&original, merge_key, value)? {
            Some(i) => original[i] = smp_merge_map(&original[i], v, schema)?,
            None => original.push(v.clone()),
        }
    }
    Ok(original)
}

/// Strategy used to compute the merge key for every item in an array
/// being strategic-merged. The whole array uses a single strategy so
/// items in `original` and `patch` always derive comparable keys, even
/// when one side has extra optional fields (e.g. an original port that
/// carries a `name` but the patch port doesn't).
#[derive(Debug, Clone, Copy)]
enum MergeKeyStrategy {
    /// `name` field — applies to containers, volumes, envFrom, env, etc.
    Name,
    /// `(containerPort, protocol)` composite key — applies to
    /// `containers[*].ports`. Defaults `protocol` to `"TCP"`.
    ContainerPort,
    /// `uid` field — applies to `metadata.ownerReferences` (upstream
    /// `patchMergeKey:"uid"`). The garbage collector orphans dependents with a
    /// strategic-merge `$patch: delete` keyed by the owner's uid; without this
    /// key the array falls back to wholesale replacement and the `$patch`
    /// directive object leaks into the stored list, breaking OwnerReference
    /// decode ("missing field `apiVersion`") and blocking orphan deletion.
    Uid,
    /// `type` field — upstream `patchMergeKey:"type"` on every `conditions`
    /// list (`PodStatus.conditions`, `NodeStatus.conditions`, ...). A sparse
    /// status patch that names one condition (the readiness-gate pattern,
    /// test/e2e/common/node/pods.go:779) must add to the list, not replace it.
    Type,
}

/// Pick the merge-key strategy for an array based on the patch items.
/// Returns `None` for primitive lists or arrays where no patch item
/// exposes a recognized key, so the caller can fall back to wholesale
/// replacement.
fn detect_merge_key_strategy(patch: &[Value]) -> Option<MergeKeyStrategy> {
    if patch.is_empty() {
        return None;
    }
    let all_objects = patch.iter().all(|v| v.is_object());
    if !all_objects {
        return None;
    }
    if patch
        .iter()
        .all(|v| v.as_object().is_some_and(|o| o.contains_key("name")))
    {
        return Some(MergeKeyStrategy::Name);
    }
    if patch.iter().all(|v| {
        v.as_object()
            .is_some_and(|o| o.contains_key("containerPort"))
    }) {
        return Some(MergeKeyStrategy::ContainerPort);
    }
    // `uid`-keyed lists (ownerReferences). Checked last so a list that happens
    // to carry both `name` and `uid` still keys by `name`.
    if patch
        .iter()
        .all(|v| v.as_object().is_some_and(|o| o.contains_key("uid")))
    {
        return Some(MergeKeyStrategy::Uid);
    }
    None
}

impl MergeKeyStrategy {
    /// The `patchMergeKey` struct tag this strategy stands for — the key
    /// `ErrNoMergeKey` names.
    fn field(self) -> &'static str {
        match self {
            MergeKeyStrategy::Name => "name",
            MergeKeyStrategy::ContainerPort => "containerPort",
            MergeKeyStrategy::Uid => "uid",
            MergeKeyStrategy::Type => "type",
        }
    }
}

fn merge_key_with(item: &Value, strategy: MergeKeyStrategy) -> Option<String> {
    let obj = item.as_object()?;
    match strategy {
        MergeKeyStrategy::Name => obj
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| format!("name:{s}")),
        MergeKeyStrategy::ContainerPort => {
            let port = obj.get("containerPort")?;
            let protocol = obj
                .get("protocol")
                .and_then(|v| v.as_str())
                .unwrap_or("TCP");
            Some(format!("port:{port}:{protocol}"))
        }
        MergeKeyStrategy::Uid => obj
            .get("uid")
            .and_then(|v| v.as_str())
            .map(|s| format!("uid:{s}")),
        MergeKeyStrategy::Type => obj
            .get("type")
            .and_then(|v| v.as_str())
            .map(|s| format!("type:{s}")),
    }
}

fn is_delete_directive(item: &Value) -> bool {
    item.as_object()
        .and_then(|o| o.get("$patch"))
        .and_then(|v| v.as_str())
        == Some("delete")
}

/// Strategic merge for arrays.
///
/// A single [`MergeKeyStrategy`] is picked for the whole array so that
/// originals and patch items always derive comparable keys. Arrays
/// where no patch item exposes a recognized key are replaced wholesale
/// (matching upstream's behavior for primitive lists and lists of
/// un-keyed structs).
///
/// `$patch: delete` on a list entry drops the matched item from the
/// resulting array, matching upstream
/// `apimachinery/pkg/util/strategicpatch/patch.go::mergePatchIntoOriginal`.
fn strategic_merge_arrays(original: &[Value], patch: &[Value]) -> Result<Vec<Value>, PatchError> {
    strategic_merge_arrays_with(original, patch, detect_merge_key_strategy(patch))
}

/// [`strategic_merge_arrays`] for the list under object key `field`: the
/// `conditions` lists are keyed by `type` (upstream struct tag
/// `patchStrategy:"merge" patchMergeKey:"type"`), which the field-agnostic
/// detection cannot see.
fn strategic_merge_arrays_for_field(
    field: &str,
    original: &[Value],
    patch: &[Value],
) -> Result<Vec<Value>, PatchError> {
    let strategy = if field == "conditions"
        && !patch.is_empty()
        && patch
            .iter()
            .all(|v| v.as_object().is_some_and(|o| o.contains_key("type")))
    {
        Some(MergeKeyStrategy::Type)
    } else {
        detect_merge_key_strategy(patch)
    };
    strategic_merge_arrays_with(original, patch, strategy)
}

fn strategic_merge_arrays_with(
    original: &[Value],
    patch: &[Value],
    strategy: Option<MergeKeyStrategy>,
) -> Result<Vec<Value>, PatchError> {
    // `mergeSliceWithSpecialElements` (patch.go:1548-1587) walks the
    // directive elements of a merge-keyed list of maps in order: `delete`
    // needs the merge key (`ErrNoMergeKey`), `merge` is refused, and anything
    // but `replace` is `ErrBadPatchType` over the element.
    if let Some(s) = strategy {
        for item in patch {
            let Some(obj) = item.as_object() else {
                continue;
            };
            let Some(directive) = obj.get("$patch") else {
                continue;
            };
            match directive.as_str() {
                Some("delete") if !obj.contains_key(s.field()) => {
                    return Err(PatchError::Uninterpreted(err_no_merge_key(item, s.field())));
                }
                Some("delete") | Some("replace") => {}
                Some("merge") => {
                    return Err(PatchError::Uninterpreted(
                        "merging lists cannot yet be specified in the patch".to_string(),
                    ));
                }
                _ => {
                    return Err(PatchError::Uninterpreted(err_bad_patch_type(
                        directive, item,
                    )));
                }
            }
        }
    }
    // A `$patch: replace` element makes the patch items (minus every
    // `$patch` element) the whole list, whatever the original held.
    if patch.iter().any(|v| {
        v.as_object()
            .and_then(|o| o.get("$patch"))
            .and_then(|d| d.as_str())
            == Some("replace")
    }) {
        return Ok(remove_directives(patch));
    }
    let strategy = match strategy {
        Some(s) => s,
        None => {
            // Primitive list or un-keyed objects — replace the array.
            return Ok(patch.to_vec());
        }
    };
    let patch_keys: Vec<Option<String>> =
        patch.iter().map(|i| merge_key_with(i, strategy)).collect();
    if patch_keys.iter().any(|k| k.is_none()) {
        // The chosen strategy can't key every patch item — bail to
        // replacement rather than silently dropping items.
        return Ok(patch.to_vec());
    }

    // Index originals by merge key. Originals that cannot be keyed
    // under the chosen strategy are preserved untouched at the tail of
    // the result (matching upstream behavior for "named patch over
    // unnamed original").
    let mut result: HashMap<String, Value> = HashMap::new();
    let mut original_unkeyed: Vec<Value> = Vec::new();
    for item in original {
        match merge_key_with(item, strategy) {
            Some(k) => {
                result.insert(k, item.clone());
            }
            None => original_unkeyed.push(item.clone()),
        }
    }

    // Apply patch items in order. `$patch: delete` drops the matched
    // entry from the result map; other items are merged into the
    // existing entry or inserted fresh.
    for (item, key_opt) in patch.iter().zip(patch_keys.iter()) {
        let key = key_opt.clone().expect("is_keyed guarantees Some");
        if is_delete_directive(item) {
            result.remove(&key);
            continue;
        }
        if let Some(existing) = result.get(&key) {
            let merged = apply_strategic_merge_patch(existing, item)?;
            result.insert(key, merged);
        } else {
            // Fresh insert — strip top-level $-directives so they don't
            // leak into the rendered list entry.
            let mut clean = item.clone();
            if let Some(o) = clean.as_object_mut() {
                o.retain(|k, _| !k.starts_with('$'));
            }
            result.insert(key, clean);
        }
    }

    // K8s SMP order: items in patch order first (skipping deletes),
    // then server-only items in original order.
    // See: apimachinery/pkg/util/strategicpatch/patch.go normalizeElementOrder
    let mut final_array = Vec::new();
    for (item, key_opt) in patch.iter().zip(patch_keys.iter()) {
        if is_delete_directive(item) {
            continue;
        }
        let key = key_opt.as_ref().expect("is_keyed guarantees Some");
        if let Some(v) = result.remove(key) {
            final_array.push(v);
        }
    }
    for item in original {
        if let Some(k) = merge_key_with(item, strategy) {
            if let Some(v) = result.remove(&k) {
                final_array.push(v);
            }
        }
    }
    // Preserve un-keyable originals at the tail so a named patch over
    // an unnamed original doesn't silently drop the original entry.
    final_array.extend(original_unkeyed);

    Ok(final_array)
}

/// Get a value at the specified JSON pointer path
fn get_value(value: &Value, path: &str) -> Result<Value, PatchError> {
    if path.is_empty() || path == "/" {
        return Ok(value.clone());
    }

    let parts = parse_path(path)?;
    let mut current = value;

    for part in parts {
        if let Some(obj) = current.as_object() {
            current = obj
                .get(&part)
                .ok_or_else(|| PatchError::OperationFailed(format!("Path not found: {}", path)))?;
        } else if let Some(arr) = current.as_array() {
            let index: usize = part
                .parse()
                .map_err(|_| PatchError::InvalidPatch(format!("Invalid array index: {}", part)))?;
            current = arr.get(index).ok_or_else(|| {
                PatchError::OperationFailed(format!("Array index out of bounds: {}", index))
            })?;
        } else {
            return Err(PatchError::OperationFailed(format!(
                "Cannot traverse non-object/array at path: {}",
                path
            )));
        }
    }

    Ok(current.clone())
}

/// Get a mutable reference to a value at the specified path
fn get_mut_value<'a>(value: &'a mut Value, path: &str) -> Result<&'a mut Value, PatchError> {
    if path.is_empty() || path == "/" {
        return Ok(value);
    }

    let parts = parse_path(path)?;
    let mut current = value;

    for part in parts {
        if current.is_object() {
            current = current
                .as_object_mut()
                .unwrap()
                .entry(part.clone())
                .or_insert(json!({}));
        } else if current.is_array() {
            let index: usize = part
                .parse()
                .map_err(|_| PatchError::InvalidPatch(format!("Invalid array index: {}", part)))?;
            current = current
                .as_array_mut()
                .unwrap()
                .get_mut(index)
                .ok_or_else(|| {
                    PatchError::OperationFailed(format!("Array index out of bounds: {}", index))
                })?;
        } else {
            return Err(PatchError::OperationFailed(format!(
                "Cannot traverse non-object/array in path: {}",
                path
            )));
        }
    }

    Ok(current)
}

/// Parse a JSON pointer path into parts
fn parse_path(path: &str) -> Result<Vec<String>, PatchError> {
    if !path.starts_with('/') {
        return Err(PatchError::InvalidPatch(format!(
            "Path must start with '/': {}",
            path
        )));
    }

    Ok(path[1..]
        .split('/')
        .map(|part| {
            // Unescape ~1 -> / and ~0 -> ~
            part.replace("~1", "/").replace("~0", "~")
        })
        .collect())
}

/// Split a path into parent path and final key. The returned key is
/// unescaped per RFC 6901 §3 (`~1` → `/`, `~0` → `~`); the parent path
/// is returned raw because `get_mut_value` re-splits and unescapes it.
fn split_path(path: &str) -> Result<(&str, String), PatchError> {
    if !path.starts_with('/') {
        return Err(PatchError::InvalidPatch(format!(
            "Path must start with '/': {}",
            path
        )));
    }
    if path == "/" {
        return Err(PatchError::InvalidPatch(
            "Cannot split root path".to_string(),
        ));
    }

    let last_slash = path.rfind('/').expect("path starts with '/'");
    let parent = if last_slash == 0 {
        "/"
    } else {
        &path[..last_slash]
    };
    let key = unescape_token(&path[last_slash + 1..]);

    Ok((parent, key))
}

/// Unescape a single JSON Pointer reference token (RFC 6901 §3): `~1`
/// decodes to `/`, `~0` decodes to `~`. Order matters — `~1` must be
/// decoded first so that `~01` round-trips to `~1` rather than `/`.
fn unescape_token(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_merge_patch_simple() {
        let original = json!({
            "name": "test",
            "value": 1
        });

        let patch = json!({
            "value": 2,
            "new_field": "hello"
        });

        let result = apply_merge_patch(&original, &patch).unwrap();
        assert_eq!(result["name"], "test");
        assert_eq!(result["value"], 2);
        assert_eq!(result["new_field"], "hello");
    }

    #[test]
    fn test_json_merge_patch_delete() {
        let original = json!({
            "name": "test",
            "value": 1
        });

        let patch = json!({
            "value": null
        });

        let result = apply_merge_patch(&original, &patch).unwrap();
        assert_eq!(result["name"], "test");
        assert!(result.get("value").is_none());
    }

    #[test]
    fn test_json_patch_add() {
        let original = json!({
            "name": "test"
        });

        let patch = json!([
            {"op": "add", "path": "/value", "value": 42}
        ]);

        let result = apply_json_patch(&original, &patch).unwrap();
        assert_eq!(result["value"], 42);
    }

    #[test]
    fn test_json_patch_remove() {
        let original = json!({
            "name": "test",
            "value": 42
        });

        let patch = json!([
            {"op": "remove", "path": "/value"}
        ]);

        let result = apply_json_patch(&original, &patch).unwrap();
        assert!(result.get("value").is_none());
    }

    #[test]
    fn test_json_patch_replace() {
        let original = json!({
            "name": "test",
            "value": 42
        });

        let patch = json!([
            {"op": "replace", "path": "/value", "value": 100}
        ]);

        let result = apply_json_patch(&original, &patch).unwrap();
        assert_eq!(result["value"], 100);
    }

    #[test]
    fn test_strategic_merge_patch_simple() {
        let original = json!({
            "metadata": {
                "name": "test",
                "labels": {
                    "app": "nginx"
                }
            },
            "spec": {
                "replicas": 1
            }
        });

        let patch = json!({
            "metadata": {
                "labels": {
                    "version": "1.0"
                }
            },
            "spec": {
                "replicas": 3
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(result["metadata"]["name"], "test");
        assert_eq!(result["metadata"]["labels"]["app"], "nginx");
        assert_eq!(result["metadata"]["labels"]["version"], "1.0");
        assert_eq!(result["spec"]["replicas"], 3);
    }

    #[test]
    fn test_strategic_merge_arrays_by_name() {
        let original = json!([
            {"name": "container1", "image": "nginx:1.0"},
            {"name": "container2", "image": "redis:5"}
        ]);

        let patch = json!([
            {"name": "container1", "image": "nginx:1.1"},
            {"name": "container3", "image": "postgres:12"}
        ]);

        let result =
            strategic_merge_arrays(original.as_array().unwrap(), patch.as_array().unwrap())
                .unwrap();

        assert_eq!(result.len(), 3);
        // K8s SMP order: patch items first, then server-only items
        assert_eq!(result[0]["image"], "nginx:1.1"); // Patch item 1 (updated)
        assert_eq!(result[1]["name"], "container3"); // Patch item 2 (added)
        assert_eq!(result[2]["image"], "redis:5"); // Server-only (preserved)
    }

    #[test]
    fn test_strategic_merge_arrays_patch_items_first() {
        // Reproduces StatefulSet patch scenario: patch adds container with different name
        // K8s SMP puts patch items before server-only items
        let original = json!([
            {"name": "webserver", "image": "agnhost:2.55", "args": ["test-webserver"]}
        ]);
        let patch = json!([
            {"name": "test-ss", "image": "pause:3.10.1"}
        ]);

        let result =
            strategic_merge_arrays(original.as_array().unwrap(), patch.as_array().unwrap())
                .unwrap();

        assert_eq!(result.len(), 2);
        // Patch item comes first
        assert_eq!(result[0]["name"], "test-ss");
        assert_eq!(result[0]["image"], "pause:3.10.1");
        // Server-only item comes second
        assert_eq!(result[1]["name"], "webserver");
        assert_eq!(result[1]["image"], "agnhost:2.55");
    }

    #[test]
    fn test_patch_type_from_content_type() {
        assert_eq!(
            PatchType::from_content_type("application/strategic-merge-patch+json").unwrap(),
            PatchType::StrategicMergePatch
        );
        assert_eq!(
            PatchType::from_content_type("application/merge-patch+json").unwrap(),
            PatchType::JsonMergePatch
        );
        assert_eq!(
            PatchType::from_content_type("application/json-patch+json").unwrap(),
            PatchType::JsonPatch
        );
    }

    #[test]
    fn test_strategic_merge_patch_directive() {
        let original = json!({
            "metadata": {
                "name": "test",
                "labels": {
                    "app": "nginx",
                    "version": "1.0"
                }
            },
            "spec": {
                "replicas": 1
            }
        });

        let patch = json!({
            "metadata": {
                "labels": {
                    "$patch": "replace",
                    "app": "apache"
                }
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        // With $patch: replace, the labels should be replaced entirely
        assert_eq!(result["metadata"]["labels"]["app"], "apache");
        assert!(result["metadata"]["labels"].get("version").is_none());
    }

    #[test]
    fn test_strategic_merge_patch_retain_keys() {
        let original = json!({
            "metadata": {
                "name": "test",
                "uid": "abc-123",
                "labels": {
                    "app": "nginx",
                    "version": "1.0"
                }
            }
        });

        let patch = json!({
            "metadata": {
                "$patch": "replace",
                "$retainKeys": ["name", "uid"],
                "labels": {
                    "app": "apache"
                }
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        // Should retain name and uid, but replace labels
        assert_eq!(result["metadata"]["name"], "test");
        assert_eq!(result["metadata"]["uid"], "abc-123");
        assert_eq!(result["metadata"]["labels"]["app"], "apache");
    }

    #[test]
    fn test_strategic_merge_delete_from_primitive_list() {
        let original = json!({
            "spec": {
                "finalizers": ["kubernetes.io/pv-protection", "example.com/my-finalizer"]
            }
        });

        let patch = json!({
            "spec": {
                "$deleteFromPrimitiveList/finalizers": ["example.com/my-finalizer"]
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        let finalizers = result["spec"]["finalizers"].as_array().unwrap();
        assert_eq!(finalizers.len(), 1);
        assert_eq!(finalizers[0], "kubernetes.io/pv-protection");
    }

    #[test]
    fn test_smp_delete_from_primitive_list_parallel_key() {
        // Wire form used by kube-controller-manager (servicecidrs controller).
        let original = json!({"metadata": {"finalizers": ["a", "b", "c"]}});
        let patch = json!({"metadata": {"$deleteFromPrimitiveList/finalizers": ["b"]}});
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(r, json!({"metadata": {"finalizers": ["a", "c"]}}));
    }

    #[test]
    fn test_smp_delete_from_primitive_list_nonexistent_item_and_list() {
        // patch_test.go:681-731
        let r = apply_strategic_merge_patch(
            &json!({"l": [1, 2]}),
            &json!({"$deleteFromPrimitiveList/l": [3]}),
        )
        .unwrap();
        assert_eq!(r, json!({"l": [1, 2]}));
        let r = apply_strategic_merge_patch(
            &json!({"foo": ["bar"]}),
            &json!({"$deleteFromPrimitiveList/l": [3]}),
        )
        .unwrap();
        assert_eq!(r, json!({"foo": ["bar"]}));
        let r = apply_strategic_merge_patch(
            &json!({"l": null}),
            &json!({"$deleteFromPrimitiveList/l": [3]}),
        )
        .unwrap();
        assert_eq!(r, json!({"l": null}));
    }

    #[test]
    fn test_smp_delete_all_duplicates_with_set_element_order() {
        // patch_test.go:1360-1395 "delete all duplicate items in lists of scalars"
        let original = json!({"l": [1, 2, 3, 3, 4]});
        let patch = json!({
            "$setElementOrder/l": [1, 2],
            "$deleteFromPrimitiveList/l": [3]
        });
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(r, json!({"l": [1, 2, 4]}));
    }

    #[test]
    fn test_smp_add_and_delete_with_set_element_order() {
        // patch_test.go:1397-1440 "add and delete items in lists of scalars"
        let original = json!({"l": [1, 2, 3]});
        let patch = json!({
            "$setElementOrder/l": [1, 2, 4],
            "$deleteFromPrimitiveList/l": [3],
            "l": [4]
        });
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(r, json!({"l": [1, 2, 4]}));
    }

    #[test]
    fn test_smp_set_element_order_reorders_primitives_keeping_server_only() {
        let original = json!({"l": ["a", "b", "x", "c"]});
        let patch = json!({"$setElementOrder/l": ["c", "a", "b"], "l": ["c"]});
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        // patch items follow the order list; server-only "x" is kept.
        let l = r["l"].as_array().unwrap();
        let pos = |s: &str| l.iter().position(|v| v == s).unwrap();
        assert!(pos("c") < pos("a") && pos("a") < pos("b"));
        assert_eq!(l.len(), 4);
        assert!(r.get("$setElementOrder/l").is_none());
    }

    #[test]
    fn test_smp_set_element_order_lists_of_maps_merge_key_name() {
        // patch_test.go:1455-1522 "merge lists of maps" (ThreeWay + Result).
        let original = json!({"mergingList": [
            {"name": "1", "other": "a"},
            {"name": "2", "value": "2", "other": "b"}
        ]});
        let patch = json!({
            "$setElementOrder/mergingList": [
                {"name": "4"}, {"name": "1"}, {"name": "2"}, {"name": "3"}
            ],
            "mergingList": [
                {"name": "4", "value": "4"},
                {"name": "3", "value": "3"}
            ]
        });
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(
            r,
            json!({"mergingList": [
                {"name": "4", "value": "4"},
                {"name": "1", "other": "a"},
                {"name": "2", "value": "2", "other": "b"},
                {"name": "3", "value": "3"}
            ]})
        );
    }

    #[test]
    fn test_smp_set_element_order_lists_of_maps_keeps_server_only() {
        // Server-only item "x" is kept; patch items follow the order list.
        let original = json!({"containers": [
            {"name": "a"}, {"name": "x"}, {"name": "b"}
        ]});
        let patch = json!({
            "$setElementOrder/containers": [{"name": "b"}, {"name": "a"}],
            "containers": [{"name": "b", "image": "i"}]
        });
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        let names: Vec<&str> = r["containers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap())
            .collect();
        assert_eq!(names.len(), 3);
        let pos = |n: &str| names.iter().position(|x| *x == n).unwrap();
        assert!(pos("b") < pos("a"));
        assert_eq!(r["containers"][pos("b")]["image"], "i");
        assert!(r.get("$setElementOrder/containers").is_none());
    }

    #[test]
    fn test_smp_set_element_order_lists_of_maps_delete_item() {
        // patch_test.go:1275-1315: `$patch: delete` item listed in the order.
        let original = json!({"mergingList": [{"name": "hello2"}]});
        let patch = json!({
            "$setElementOrder/mergingList": [{"name": "hello"}, {"name": "doesntexist"}],
            "mergingList": [{"name": "hello"}, {"$patch": "delete", "name": "doesntexist"}]
        });
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(
            r,
            json!({"mergingList": [{"name": "hello"}, {"name": "hello2"}]})
        );
    }

    #[test]
    fn test_smp_replace_directive_deletes_all_items_from_merging_list() {
        // patch_test.go:370-377 "delete all items from merging list":
        // mergeSliceWithSpecialElements (patch.go:1548-1587) returns the
        // patch minus its `$patch: replace` element.
        let original = json!({"mergingList": [{"name": "1"}, {"name": "2"}]});
        let patch = json!({"mergingList": [{"$patch": "replace"}]});
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(r, json!({"mergingList": []}));
    }

    #[test]
    fn test_smp_replace_directive_replaces_merging_list_with_patch_items() {
        // Same function: the items beside `$patch: replace` become the list
        // (what fixupPatchForNodeStatusAddresses emits for status.addresses,
        // component-helpers/node/util/status.go:84-130).
        let original = json!({"status": {"addresses": [
            {"type": "InternalIP", "address": "9.9.9.9"}]}});
        let patch = json!({"status": {"addresses": [
            {"type": "InternalIP", "address": "10.1.1.1"},
            {"type": "Hostname", "address": "n"},
            {"$patch": "replace"}]}});
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(
            r,
            json!({"status": {"addresses": [
                {"type": "InternalIP", "address": "10.1.1.1"},
                {"type": "Hostname", "address": "n"}]}})
        );
    }

    #[test]
    fn test_smp_set_element_order_lists_of_maps_mismatch_errors() {
        // validatePatchWithSetOrderList (patch.go:1018).
        let r = apply_strategic_merge_patch(
            &json!({"l": [{"name": "1"}]}),
            &json!({
                "$setElementOrder/l": [{"name": "1"}, {"name": "2"}],
                "l": [{"name": "3"}]
            }),
        );
        assert!(r.is_err());
    }

    #[test]
    fn test_smp_retain_keys_clears_and_accepts_listed() {
        // patch_test.go:4412-4440 discriminated union.
        let original = json!({"retainKeysMap": {"name": "type1", "value": "foo"}});
        let patch = json!({"retainKeysMap": {
            "$retainKeys": ["name", "other"], "name": "type2", "other": "bar"
        }});
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(
            r,
            json!({"retainKeysMap": {"name": "type2", "other": "bar"}})
        );
    }

    #[test]
    fn test_smp_retain_keys_rejects_patch_key_not_listed() {
        // applyRetainKeysDirective (patch.go:1081-1090).
        let err = apply_strategic_merge_patch(
            &json!({"m": {"a": 1}}),
            &json!({"m": {"$retainKeys": ["a"], "b": 2}}),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("invalid patch format of retainKeys"),
            "{err}"
        );
        // null values are exempt (patch.go:1082).
        let r = apply_strategic_merge_patch(
            &json!({"m": {"a": 1, "b": 2}}),
            &json!({"m": {"$retainKeys": ["a"], "b": null}}),
        )
        .unwrap();
        assert_eq!(r, json!({"m": {"a": 1}}));
    }

    #[test]
    fn test_smp_retain_keys_not_a_list_errors() {
        // patch.go:1070-1073.
        let err = apply_strategic_merge_patch(
            &json!({"m": {"a": 1}}),
            &json!({"m": {"$retainKeys": "a", "a": 2}}),
        )
        .unwrap_err();
        assert!(err.to_string().contains("retainKeys"), "{err}");
    }

    #[test]
    fn test_smp_array_element_delete_shim_removed() {
        // Not an upstream wire form (only the parallel `$deleteFromPrimitiveList/<f>`
        // key is); must not delete.
        let r = apply_strategic_merge_patch(
            &json!({"f": ["a", "b"]}),
            &json!({"f": [{"$deleteFromPrimitiveList": ["a"]}]}),
        )
        .unwrap();
        assert_ne!(r, json!({"f": ["b"]}));
    }

    #[test]
    fn test_smp_set_element_order_mismatch_errors() {
        let r = apply_strategic_merge_patch(
            &json!({"l": [1, 2]}),
            &json!({"$setElementOrder/l": [1, 2], "l": [3]}),
        );
        assert!(r.is_err());
    }

    #[test]
    fn test_strategic_merge_delete_directive() {
        let original = json!({
            "metadata": {
                "name": "test",
                "annotations": {
                    "foo": "bar"
                }
            }
        });

        let patch = json!({
            "metadata": {
                "annotations": {
                    "$patch": "delete"
                }
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert!(result["metadata"]["annotations"].is_null());
    }

    // Regression: the garbage collector orphans a dependent by strategic-merge
    // `$patch: delete` on ownerReferences, keyed by the owner's `uid`. Without a
    // uid merge-key the array was replaced wholesale, leaking the `$patch`
    // directive object into the list and breaking OwnerReference decode
    // ("missing field `apiVersion`") — blocking GC orphan deletion.
    #[test]
    fn test_strategic_merge_owner_reference_delete_by_uid() {
        let original = json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "rs",
                "uid": "rs-uid",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "d",
                    "uid": "owner-uid",
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            }
        });
        // Exactly what pkg/controller/garbagecollector sends.
        let patch = json!({
            "metadata": {
                "ownerReferences": [{"$patch": "delete", "uid": "owner-uid"}],
                "uid": "rs-uid"
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        // Top-level TypeMeta must survive.
        assert_eq!(result["apiVersion"], "apps/v1");
        assert_eq!(result["kind"], "ReplicaSet");
        // The ownerReference must be gone and no `$patch` directive must leak.
        let owners = result["metadata"]["ownerReferences"].as_array().unwrap();
        assert!(
            owners.is_empty(),
            "ownerReferences must be empty after $patch:delete, got {owners:?}"
        );
    }

    // A second owner must be preserved when only one is deleted by uid.
    #[test]
    fn test_strategic_merge_owner_reference_delete_keeps_others() {
        let original = json!({
            "metadata": {"ownerReferences": [
                {"apiVersion":"apps/v1","kind":"Deployment","name":"d","uid":"a","controller":true},
                {"apiVersion":"apps/v1","kind":"Deployment","name":"e","uid":"b"}
            ]}
        });
        let patch = json!({"metadata": {"ownerReferences": [{"$patch":"delete","uid":"a"}]}});
        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        let owners = result["metadata"]["ownerReferences"].as_array().unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0]["uid"], "b");
    }

    #[test]
    fn test_json_patch_add_missing_value_returns_err() {
        let original = json!({"a": 1});
        let patch = json!([{ "op": "add", "path": "/b" }]);
        let result = apply_patch(&original, &patch, PatchType::JsonPatch);
        match result {
            Err(PatchError::InvalidPatch(msg)) => assert!(msg.contains("add")),
            other => panic!(
                "expected InvalidPatch for add without value, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_json_patch_replace_missing_value_returns_err() {
        let original = json!({"a": 1});
        let patch = json!([{ "op": "replace", "path": "/a" }]);
        let result = apply_patch(&original, &patch, PatchType::JsonPatch);
        match result {
            Err(PatchError::InvalidPatch(msg)) => assert!(msg.contains("replace")),
            other => panic!(
                "expected InvalidPatch for replace without value, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_json_patch_test_missing_value_returns_err() {
        let original = json!({"a": 1});
        let patch = json!([{ "op": "test", "path": "/a" }]);
        let result = apply_patch(&original, &patch, PatchType::JsonPatch);
        match result {
            Err(PatchError::InvalidPatch(msg)) => assert!(msg.contains("test")),
            other => panic!(
                "expected InvalidPatch for test without value, got {:?}",
                other
            ),
        }
    }
}

/// Recursively merge two JSON objects. For nested objects, entries from
/// `patch` are merged INTO `target` without replacing existing entries.
/// Null values in patch remove the key from target.
/// This is used for strategic merge patch on status subresources where
/// maps like capacity/allocatable must be merged, not replaced.
pub fn deep_merge_objects(target: &mut serde_json::Value, patch: &serde_json::Value) {
    if let (Some(target_obj), Some(patch_obj)) = (target.as_object_mut(), patch.as_object()) {
        for (k, v) in patch_obj {
            if v.is_null() {
                target_obj.remove(k);
            } else if v.is_object() {
                let existing = target_obj
                    .entry(k.clone())
                    .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                deep_merge_objects(existing, v);
            } else if v.is_array() && k == "conditions" {
                // `conditions` arrays carry a strategic merge key of `type`
                // (patchMergeKey). A sparse status patch — e.g. setting a single
                // readinessGate condition — must update/add that condition by
                // type without dropping the others. A plain insert would replace
                // the whole array and lose previously-patched conditions.
                let existing = target_obj
                    .entry(k.clone())
                    .or_insert_with(|| serde_json::Value::Array(Vec::new()));
                merge_conditions_by_type(existing, v);
            } else {
                target_obj.insert(k.clone(), v.clone());
            }
        }
    }
}

/// Merge a `conditions` patch array into the target array by the `type` key:
/// patch entries update the matching existing condition (deep-merged) or are
/// appended; existing conditions absent from the patch are preserved.
fn merge_conditions_by_type(target: &mut serde_json::Value, patch: &serde_json::Value) {
    let Some(patch_arr) = patch.as_array() else {
        return;
    };
    let target_arr = match target.as_array_mut() {
        Some(a) => a,
        None => {
            *target = patch.clone();
            return;
        }
    };
    for pc in patch_arr {
        let ptype = pc.get("type").and_then(|t| t.as_str());
        match ptype.and_then(|pt| {
            target_arr
                .iter_mut()
                .find(|c| c.get("type").and_then(|t| t.as_str()) == Some(pt))
        }) {
            Some(existing) => deep_merge_objects(existing, pc),
            None => target_arr.push(pc.clone()),
        }
    }
}

#[test]
fn test_deep_merge_preserves_existing_map_entries() {
    let mut target = serde_json::json!({
        "capacity": {
            "cpu": "4",
            "memory": "8Gi",
            "pods": "110"
        },
        "conditions": [{"type": "Ready"}]
    });
    let patch = serde_json::json!({
        "capacity": {
            "scheduling.k8s.io/foo": "5"
        }
    });
    deep_merge_objects(&mut target, &patch);

    let cap = target.get("capacity").unwrap().as_object().unwrap();
    assert_eq!(cap.get("cpu").unwrap(), "4", "existing cpu preserved");
    assert_eq!(
        cap.get("memory").unwrap(),
        "8Gi",
        "existing memory preserved"
    );
    assert_eq!(
        cap.get("scheduling.k8s.io/foo").unwrap(),
        "5",
        "new extended resource added"
    );
    assert!(target.get("conditions").is_some(), "conditions preserved");
}
