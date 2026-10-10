//! RFC 6902 patch application with the semantics and error strings of
//! `gopkg.in/evanphx/json-patch.v4` (`patch.go`), the library the
//! MutatingAdmissionPolicy `jsonPatch` patcher applies its operations with
//! (`mutating/patch/json_patch.go:31,92`).
//!
//! Ported rather than replaced by the in-tree `rusternetes_common::patch`
//! applier because the two disagree where `json_patch_test.go` pins the
//! behaviour: `replace` of an unset *map* key adds it
//! (`patch.go:393-395,599-604`: `partialDoc.get` returns `nil, nil`), a failed
//! `test` is the sentinel `ErrTestFailed` the patcher turns into "no change"
//! (`json_patch.go:94-97`), negative array indices count from the end
//! (`SupportNegativeIndices`, `patch.go:22`), and `add` of an out-of-range
//! array index is an error.
//!
//! Not modelled: `AccumulatedCopySizeLimit` (default 0 = unlimited,
//! `patch.go:25`; the MAP patcher does not set it).

#![allow(dead_code)]

use serde_json::Value;

/// `jsonpatch.Operation`: the fields `Apply` reads. `value: None` is an
/// absent `"value"` key (`Operation.value()` returning nil, `patch.go:299`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Operation {
    pub op: String,
    pub path: Option<String>,
    pub from: Option<String>,
    pub value: Option<Value>,
}

/// Why `Patch.Apply` failed.
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyError {
    /// `errors.Is(err, jsonpatch.ErrTestFailed)`.
    TestFailed,
    /// Any other error, with the library's message.
    Other(String),
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApplyError::TestFailed => f.write_str("test failed"),
            ApplyError::Other(m) => f.write_str(m),
        }
    }
}

const ERR_MISSING: &str = "missing value";

fn other<T>(msg: String) -> Result<T, ApplyError> {
    Err(ApplyError::Other(msg))
}

/// `decodePatchKey` (`patch.go:848`): `~1` then `~0`.
fn decode_key(k: &str) -> String {
    k.replace("~1", "/").replace("~0", "~")
}

/// `strconv.Atoi`, with its error text.
fn atoi(key: &str) -> Result<i64, String> {
    key.parse::<i64>()
        .map_err(|_| format!("strconv.Atoi: parsing \"{key}\": invalid syntax"))
}

fn invalid_index(idx: i64) -> String {
    format!("Unable to access invalid index: {idx}: invalid index referenced")
}

/// A node `findObject` can descend into.
enum Container<'a> {
    Doc(&'a mut serde_json::Map<String, Value>),
    Ary(&'a mut Vec<Value>),
}

fn container(v: &mut Value) -> Option<Container<'_>> {
    match v {
        Value::Object(m) => Some(Container::Doc(m)),
        Value::Array(a) => Some(Container::Ary(a)),
        _ => None,
    }
}

/// Resolve a possibly negative index against `len` as `partialArray.get`
/// does (`patch.go:468-490`).
fn array_index(key: &str, len: usize) -> Result<usize, String> {
    let mut idx = atoi(key)?;
    if idx < 0 {
        if idx < -(len as i64) {
            return Err(invalid_index(idx));
        }
        idx += len as i64;
    }
    if idx >= len as i64 {
        return Err(invalid_index(idx));
    }
    Ok(idx as usize)
}

impl Container<'_> {
    /// `container.get`. `Ok(None)` is a nil node (a missing map key).
    fn get(&self, key: &str) -> Result<Option<Value>, String> {
        match self {
            Container::Doc(m) => Ok(m.get(key).cloned()),
            Container::Ary(a) => array_index(key, a.len()).map(|i| Some(a[i].clone())),
        }
    }

    /// `container.set`, used by `replace` after a successful `get`.
    fn set(&mut self, key: &str, val: Value) -> Result<(), String> {
        match self {
            Container::Doc(m) => {
                m.insert(key.to_string(), val);
                Ok(())
            }
            Container::Ary(a) => {
                let mut idx = atoi(key)?;
                if idx < 0 {
                    if idx < -(a.len() as i64) {
                        return Err(invalid_index(idx));
                    }
                    idx += a.len() as i64;
                }
                a[idx as usize] = val;
                Ok(())
            }
        }
    }

    /// `container.add` (`patch.go:388,429`).
    fn add(&mut self, key: &str, val: Value) -> Result<(), String> {
        match self {
            Container::Doc(m) => {
                m.insert(key.to_string(), val);
                Ok(())
            }
            Container::Ary(a) => {
                if key == "-" {
                    a.push(val);
                    return Ok(());
                }
                let mut idx = atoi(key)
                    .map_err(|e| format!("value was not a proper array index: '{key}': {e}"))?;
                let sz = a.len() as i64 + 1;
                if idx >= sz {
                    return Err(invalid_index(idx));
                }
                if idx < 0 {
                    if idx < -sz {
                        return Err(invalid_index(idx));
                    }
                    idx += sz;
                }
                a.insert(idx as usize, val);
                Ok(())
            }
        }
    }

    /// `container.remove` (`patch.go:397,492`).
    fn remove(&mut self, key: &str) -> Result<Value, String> {
        match self {
            Container::Doc(m) => m
                .remove(key)
                .ok_or_else(|| format!("Unable to remove nonexistent key: {key}: {ERR_MISSING}")),
            Container::Ary(a) => {
                let mut idx = atoi(key)?;
                if idx >= a.len() as i64 {
                    return Err(invalid_index(idx));
                }
                if idx < 0 {
                    if idx < -(a.len() as i64) {
                        return Err(invalid_index(idx));
                    }
                    idx += a.len() as i64;
                }
                Ok(a.remove(idx as usize))
            }
        }
    }
}

/// `findObject` (`patch.go:342-381`): the container holding `path`'s last
/// token, and that token decoded. `None` is a nil container.
fn find_object<'a>(doc: &'a mut Value, path: &str) -> Option<(Container<'a>, String)> {
    let split: Vec<&str> = path.split('/').collect();
    if split.len() < 2 {
        return None;
    }
    let key = decode_key(split[split.len() - 1]);
    let mut cur = doc;
    for part in &split[1..split.len() - 1] {
        let part = decode_key(part);
        let next = match container(cur)? {
            Container::Doc(m) => m.get_mut(&part)?,
            Container::Ary(a) => {
                let i = array_index(&part, a.len()).ok()?;
                &mut a[i]
            }
        };
        // `next.raw == nil` / an unparsable node ends the walk.
        if !matches!(next, Value::Object(_) | Value::Array(_)) {
            return None;
        }
        cur = next;
    }
    Some((container(cur)?, key))
}

fn op_path(op: &Operation, what: &str) -> Result<String, ApplyError> {
    op.path.clone().ok_or_else(|| {
        ApplyError::Other(format!(
            "{what} operation failed to decode path: {ERR_MISSING}"
        ))
    })
}

fn op_from(op: &Operation, what: &str) -> Result<String, ApplyError> {
    op.from.clone().ok_or_else(|| {
        ApplyError::Other(format!(
            "{what} operation failed to decode from: operation, missing from field: {ERR_MISSING}"
        ))
    })
}

/// `op.value()` as a node: an absent value is a nil node, which marshals as
/// `null` once stored.
fn node(op: &Operation) -> Value {
    op.value.clone().unwrap_or(Value::Null)
}

/// `Patch.Apply` (`patch.go:779-835`). The document must be an object or an
/// array.
pub fn apply(doc: &Value, ops: &[Operation]) -> Result<Value, ApplyError> {
    if !matches!(doc, Value::Object(_) | Value::Array(_)) {
        return other("json: cannot unmarshal into a patch document".to_string());
    }
    let mut doc = doc.clone();
    for op in ops {
        match op.op.as_str() {
            "add" => add(&mut doc, op)?,
            "remove" => remove(&mut doc, op)?,
            "replace" => replace(&mut doc, op)?,
            "move" => mv(&mut doc, op)?,
            "test" => test(&mut doc, op)?,
            "copy" => copy(&mut doc, op)?,
            // `Operation.Kind` is "unknown" for a missing/undecodable op.
            "" => return other("Unexpected kind: unknown".to_string()),
            k => return other(format!("Unexpected kind: {k}")),
        }
    }
    Ok(doc)
}

fn add(doc: &mut Value, op: &Operation) -> Result<(), ApplyError> {
    // The library discards the decode error (patch.go:527).
    let path = op_path(op, "add")?;
    let Some((mut con, key)) = find_object(doc, &path) else {
        return other(format!(
            "add operation does not apply: doc is missing path: \"{path}\": {ERR_MISSING}"
        ));
    };
    con.add(&key, node(op))
        .map_err(|e| ApplyError::Other(format!("error in add for path: '{path}': {e}")))
}

fn remove(doc: &mut Value, op: &Operation) -> Result<(), ApplyError> {
    let path = op_path(op, "remove")?;
    let Some((mut con, key)) = find_object(doc, &path) else {
        return other(format!(
            "remove operation does not apply: doc is missing path: \"{path}\": {ERR_MISSING}"
        ));
    };
    con.remove(&key)
        .map(|_| ())
        .map_err(|e| ApplyError::Other(format!("error in remove for path: '{path}': {e}")))
}

fn replace(doc: &mut Value, op: &Operation) -> Result<(), ApplyError> {
    let path = op_path(op, "replace")?;
    if path.is_empty() {
        let val = node(op);
        if !matches!(val, Value::Object(_) | Value::Array(_)) {
            return other("replace operation value must be object or array: %!w(<nil>)".into());
        }
        *doc = val;
        return Ok(());
    }
    let Some((mut con, key)) = find_object(doc, &path) else {
        return other(format!(
            "replace operation does not apply: doc is missing path: {path}: {ERR_MISSING}"
        ));
    };
    if con.get(&key).is_err() {
        return other(format!(
            "replace operation does not apply: doc is missing key: {path}: {ERR_MISSING}"
        ));
    }
    con.set(&key, node(op))
        .map_err(|e| ApplyError::Other(format!("error in remove for path: '{path}': {e}")))
}

fn mv(doc: &mut Value, op: &Operation) -> Result<(), ApplyError> {
    let from = op_from(op, "move")?;
    let val = {
        let Some((mut con, key)) = find_object(doc, &from) else {
            return other(format!(
                "move operation does not apply: doc is missing from path: {from}: {ERR_MISSING}"
            ));
        };
        // `get` then `remove`: a missing map key is a nil node here.
        let v = con
            .get(&key)
            .map_err(|e| ApplyError::Other(format!("error in move for path: '{key}': {e}")))?;
        con.remove(&key)
            .map_err(|e| ApplyError::Other(format!("error in move for path: '{key}': {e}")))?;
        v.unwrap_or(Value::Null)
    };
    let path = op_path(op, "move")?;
    let Some((mut con, key)) = find_object(doc, &path) else {
        return other(format!(
            "move operation does not apply: doc is missing destination path: {path}: {ERR_MISSING}"
        ));
    };
    con.add(&key, val)
        .map_err(|e| ApplyError::Other(format!("error in move for path: '{path}': {e}")))
}

fn copy(doc: &mut Value, op: &Operation) -> Result<(), ApplyError> {
    let from = op_from(op, "copy")?;
    let val = {
        let Some((con, key)) = find_object(doc, &from) else {
            return other(format!(
                "copy operation does not apply: doc is missing from path: {from}: {ERR_MISSING}"
            ));
        };
        con.get(&key)
            .map_err(|e| ApplyError::Other(format!("error in copy for from: '{from}': {e}")))?
            .unwrap_or(Value::Null)
    };
    let path = op_path(op, "copy")?;
    let Some((mut con, key)) = find_object(doc, &path) else {
        return other(format!(
            "copy operation does not apply: doc is missing destination path: {path}: {ERR_MISSING}"
        ));
    };
    con.add(&key, val)
        .map_err(|e| ApplyError::Other(format!("error while adding value during copy: {e}")))
}

fn test(doc: &mut Value, op: &Operation) -> Result<(), ApplyError> {
    let path = op_path(op, "test")?;
    if path.is_empty() {
        return if *doc == node(op) {
            Ok(())
        } else {
            Err(ApplyError::TestFailed)
        };
    }
    let Some((con, key)) = find_object(doc, &path) else {
        return other(format!(
            "test operation does not apply: is missing path: {path}: {ERR_MISSING}"
        ));
    };
    let val = con
        .get(&key)
        .map_err(|e| ApplyError::Other(format!("error in test for path: '{path}': {e}")))?;
    match (val, &op.value) {
        // patch.go:689-696: a nil node only matches a nil or `null` value.
        (None, None) | (None, Some(Value::Null)) => Ok(()),
        (None, Some(_)) | (Some(_), None) => Err(ApplyError::TestFailed),
        (Some(v), Some(want)) if v == *want => Ok(()),
        _ => Err(ApplyError::TestFailed),
    }
}

#[cfg(test)]
mod tests;
