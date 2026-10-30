//! `listtype`: `ValidateListSetsAndMaps`.
//!
//! Port of `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/listtype/validation.go`
//! (release-1.35): arrays with `x-kubernetes-list-type` `set` must have unique
//! elements and arrays with `map` must have unique keys
//! (`x-kubernetes-list-map-keys`).
//!
//! The consumers are the custom resource strategies
//! (`registry/customresource/strategy.go:214, :300`,
//! `status_strategy.go:118-119`), wired in
//! `registry::apiextensions::customresource`.

use std::collections::HashMap;

use serde_json::{Map, Value};

use super::Structural;
use crate::validation::field::{Error, Path};

/// `ValidateListSetsAndMaps` (validation.go:28-47). `fld_path` is Go's
/// `*field.Path`, nil for the object root (`Path::default()`).
pub fn validate_list_sets_and_maps(
    fld_path: &Path,
    s: Option<&Structural>,
    obj: Option<&Map<String, Value>>,
) -> Vec<Error> {
    let (Some(s), Some(obj)) = (s, obj) else {
        return Vec::new();
    };

    let mut errs = Vec::new();

    if let Some(sub) = s
        .additional_properties
        .as_ref()
        .and_then(|ap| ap.structural.as_ref())
    {
        for (k, v) in obj {
            errs.extend(validation_list_set_and_maps(&fld_path.key(k), sub, v));
        }
    }
    for (k, v) in obj {
        if let Some(sub) = s.properties.get(k) {
            errs.extend(validation_list_set_and_maps(&fld_path.child(k), sub, v));
        }
    }

    errs
}

/// `validationListSetAndMaps` (validation.go:49-58).
fn validation_list_set_and_maps(fld_path: &Path, s: &Structural, obj: &Value) -> Vec<Error> {
    match obj {
        Value::Array(a) => validate_list_sets_and_maps_array(fld_path, s, a),
        Value::Object(m) => validate_list_sets_and_maps(fld_path, Some(s), Some(m)),
        _ => Vec::new(),
    }
}

/// `validateListSetsAndMapsArray` (validation.go:60-88).
fn validate_list_sets_and_maps_array(fld_path: &Path, s: &Structural, obj: &[Value]) -> Vec<Error> {
    let mut errs = Vec::new();

    match s.extensions.x_list_type.as_deref() {
        Some("set") => {
            let keys: Vec<ListKey> = obj.iter().map(ListKey::of).collect();
            for i in validate_list_set(&keys) {
                errs.push(Error::duplicate(&fld_path.index(i), obj[i].clone()));
            }
        }
        Some("map") => errs.extend(validate_list_map(fld_path, s, obj)),
        _ => {}
    }

    if let Some(items) = s.items.as_deref() {
        for (i, v) in obj.iter().enumerate() {
            errs.extend(validation_list_set_and_maps(&fld_path.index(i), items, v));
        }
    }

    errs
}

/// An element compared by Go map-key / marshalled-JSON equality
/// (validation.go:90-128). `Unspecified` is Go's `unspecifiedKeyValue{}`,
/// "handled like scalars with correct comparison by Golang".
#[derive(Clone, PartialEq, Eq, Hash)]
enum ListKey {
    Unspecified,
    /// A scalar or `null`, compared by its JSON rendering (`1` and `1.0`
    /// differ as Go's `int64`/`float64` map keys do).
    Scalar(String),
    /// A compound value, compared by its marshalled JSON (`json.Marshal`
    /// sorts map keys; so does `serde_json::Map`).
    Compound(String),
}

impl ListKey {
    fn of(v: &Value) -> Self {
        match v {
            Value::Object(_) | Value::Array(_) => ListKey::Compound(v.to_string()),
            _ => ListKey::Scalar(v.to_string()),
        }
    }
}

/// `validateListSet` (validation.go:97-128): the indices of the second
/// appearance of each non-unique item (a third appearance is not reported
/// again).
fn validate_list_set(keys: &[ListKey]) -> Vec<usize> {
    if keys.len() <= 1 {
        return Vec::new();
    }
    let mut seen: HashMap<&ListKey, usize> = HashMap::with_capacity(keys.len());
    let mut non_unique = Vec::new();
    for (i, k) in keys.iter().enumerate() {
        let times = seen.entry(k).or_insert(0);
        *times += 1;
        if *times == 2 {
            non_unique.push(i);
        }
    }
    non_unique
}

/// `validateListMap` (validation.go:130-211).
fn validate_list_map(fld_path: &Path, s: &Structural, obj: &[Value]) -> Vec<Error> {
    // only allow nil and objects
    for (i, x) in obj.iter().enumerate() {
        if !x.is_null() && !x.is_object() {
            return vec![Error::invalid(
                &fld_path.index(i),
                x.clone(),
                "must be an object for an array of list-type map",
            )];
        }
    }

    if obj.len() <= 1 {
        return Vec::new();
    }

    let map_keys = &s.extensions.x_list_map_keys;

    // optimize simple case of one key
    if map_keys.len() == 1 {
        let key_field = &map_keys[0];
        // nil object or undefined key means an unspecified key.
        let keys: Vec<Option<&Value>> = obj
            .iter()
            .map(|x| x.as_object().and_then(|m| m.get(key_field)))
            .collect();
        let set_keys: Vec<ListKey> = keys
            .iter()
            .map(|k| k.map_or(ListKey::Unspecified, ListKey::of))
            .collect();

        return validate_list_set(&set_keys)
            .into_iter()
            .map(|i| {
                let value = match keys[i] {
                    None => Value::Object(Map::new()),
                    Some(k) => {
                        let mut m = Map::new();
                        m.insert(key_field.clone(), k.clone());
                        Value::Object(m)
                    }
                };
                Error::duplicate(&fld_path.index(i), value)
            })
            .collect();
    }

    // multiple key fields
    let keys: Vec<Value> = obj
        .iter()
        .map(|x| {
            let mut key = Map::new();
            if let Some(m) = x.as_object() {
                for key_field in map_keys {
                    if let Some(k) = m.get(key_field) {
                        key.insert(key_field.clone(), k.clone());
                    }
                }
            }
            Value::Object(key)
        })
        .collect();
    let set_keys: Vec<ListKey> = keys.iter().map(ListKey::of).collect();

    validate_list_set(&set_keys)
        .into_iter()
        .map(|i| Error::duplicate(&fld_path.index(i), keys[i].clone()))
        .collect()
}
