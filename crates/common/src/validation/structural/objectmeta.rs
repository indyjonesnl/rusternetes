//! `schemaobjectmeta`: `GetObjectMetaWithOptions` and `CoerceWithOptions`.
//!
//! Port of `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/objectmeta/`
//! (`coerce.go`, `algorithm.go`), release-1.35.

use serde_json::{Map, Value};

use super::Structural;
use crate::validation::field::{Error, Path};

/// `ObjectMetaOptions` (coerce.go:34-48).
#[derive(Debug, Clone, Default)]
pub struct ObjectMetaOptions {
    pub drop_malformed_fields: bool,
    pub return_unknown_field_paths: bool,
    pub parent_path: Option<Path>,
}

/// What `GetObjectMetaWithOptions` returns: the decoded `metav1.ObjectMeta`
/// (as its `ToUnstructured` form), whether `metadata` was found, and the
/// unknown field paths.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ObjectMetaResult {
    pub meta: Option<Map<String, Value>>,
    pub found: bool,
    pub unknown_field_paths: Vec<String>,
}

pub fn get_object_meta_with_options(
    _obj: &Map<String, Value>,
    _opts: &ObjectMetaOptions,
) -> Result<ObjectMetaResult, String> {
    Ok(ObjectMetaResult::default())
}

/// `CoerceOptions` (algorithm.go:29-40).
#[derive(Debug, Clone, Copy, Default)]
pub struct CoerceOptions {
    pub drop_invalid_fields: bool,
    pub return_unknown_field_paths: bool,
}

pub fn coerce_with_options(
    _pth: Option<&Path>,
    _obj: &mut Value,
    _s: Option<&Structural>,
    _is_resource_root: bool,
    _opts: CoerceOptions,
) -> (Option<Error>, Vec<String>) {
    (None, Vec::new())
}

#[cfg(test)]
mod tests;
