//! The kube-openapi spec-merging helpers the aggregator's OpenAPI v2
//! controller builds on, over the Swagger document as a [`serde_json::Value`]
//! (the Go `spec.Swagger`).
//!
//! Ported from `k8s.io/kube-openapi/pkg/aggregator/{aggregator,walker}.go`
//! (`FilterSpecByPathsWithoutSideEffects`, `MergeSpecsIgnorePathConflict
//! RenamingDefinitionsAndParameters`, `mergeSpecs`, `renameDefinitions`,
//! `renameParameters`, `deepEqualDefinitionsModuloGVKs`, `mergedGVKs`,
//! `walkOnAllReferences`), `pkg/util/trie.go` (`Trie.HasPrefix`) and
//! `pkg/schemamutation/walker.go` (`Walker`, `ReplaceReferences`).
//!
//! Deviations: the Go code mutates typed structs that share data with their
//! inputs (copy-on-write); here an input that must change is cloned and the
//! clone mutated. A schema compares by JSON value rather than by Go struct
//! (so `"description": ""` and an absent description differ).

use std::collections::{BTreeMap, HashSet};

use serde_json::{Map, Value};

/// `gvkKey`.
pub const GVK_KEY: &str = "x-kubernetes-group-version-kind";
/// `definitionPrefix`.
pub const DEFINITION_PREFIX: &str = "#/definitions/";
/// `parameterPrefix`.
pub const PARAMETER_PREFIX: &str = "#/parameters/";

const OPERATIONS: [&str; 7] = ["delete", "get", "head", "options", "patch", "post", "put"];

fn ref_of(v: &Value) -> Option<&str> {
    v.get("$ref")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// The path items of a Swagger `paths` object (`spec.Paths.Paths`: the keys
/// that start with `/`).
fn path_items(paths: &Value) -> impl Iterator<Item = (&String, &Value)> {
    paths
        .as_object()
        .into_iter()
        .flat_map(|m| m.iter())
        .filter(|(k, _)| k.starts_with('/'))
}

fn paths_of(sp: &Value) -> Option<&Value> {
    sp.get("paths").filter(|p| p.is_object())
}

// ---------------------------------------------------------------------------
// walker.go: readonlyReferenceWalker
// ---------------------------------------------------------------------------

struct RefWalker<'a, 'f> {
    root: &'a Value,
    visited: HashSet<String>,
    walk_ref: &'f mut dyn FnMut(&str),
}

impl<'a, 'f> RefWalker<'a, 'f> {
    /// `walker.walkRefCallback`: the caller's callback, then following a
    /// parameter or definition reference.
    fn on_ref(&mut self, r: &str) {
        (self.walk_ref)(r);
        let root = self.root;
        if let Some(name) = r.strip_prefix(PARAMETER_PREFIX) {
            if let Some(param) = root.get("parameters").and_then(|p| p.get(name)) {
                self.walk_param(param);
            }
        } else if let Some(name) = r.strip_prefix(DEFINITION_PREFIX) {
            if let Some(def) = root.get("definitions").and_then(|d| d.get(name)) {
                if self.visited.insert(r.to_string()) {
                    self.walk_schema(def);
                }
            }
        }
    }

    fn walk_ref_of(&mut self, v: &Value) {
        if let Some(r) = ref_of(v) {
            self.on_ref(r);
        }
    }

    fn walk_schema(&mut self, schema: &'a Value) {
        let Some(obj) = schema.as_object() else {
            return;
        };
        self.walk_ref_of(schema);
        for key in ["definitions", "properties", "patternProperties"] {
            if let Some(m) = obj.get(key).and_then(Value::as_object) {
                for v in m.values() {
                    self.walk_schema(v);
                }
            }
        }
        for key in ["allOf", "anyOf", "oneOf"] {
            if let Some(a) = obj.get(key).and_then(Value::as_array) {
                for v in a {
                    self.walk_schema(v);
                }
            }
        }
        if let Some(n) = obj.get("not") {
            self.walk_schema(n);
        }
        for key in ["additionalProperties", "additionalItems"] {
            if let Some(v) = obj.get(key).filter(|v| v.is_object()) {
                self.walk_schema(v);
            }
        }
        match obj.get("items") {
            Some(v) if v.is_object() => self.walk_schema(v),
            Some(Value::Array(a)) => {
                for v in a {
                    self.walk_schema(v);
                }
            }
            _ => {}
        }
    }

    fn walk_params(&mut self, params: Option<&'a Value>) {
        for p in params.and_then(Value::as_array).into_iter().flatten() {
            self.walk_param(p);
        }
    }

    fn walk_param(&mut self, param: &'a Value) {
        self.walk_ref_of(param);
        if let Some(s) = param.get("schema") {
            self.walk_schema(s);
        }
        if let Some(items) = param.get("items") {
            self.walk_ref_of(items);
        }
    }

    fn walk_response(&mut self, resp: &'a Value) {
        if !resp.is_object() {
            return;
        }
        self.walk_ref_of(resp);
        if let Some(s) = resp.get("schema") {
            self.walk_schema(s);
        }
    }

    fn walk_operation(&mut self, op: &'a Value) {
        if !op.is_object() {
            return;
        }
        self.walk_params(op.get("parameters"));
        let Some(responses) = op.get("responses").and_then(Value::as_object) else {
            return;
        };
        if let Some(d) = responses.get("default") {
            self.walk_response(d);
        }
        for (k, r) in responses {
            if k.parse::<i64>().is_ok() {
                self.walk_response(r);
            }
        }
    }

    fn start(&mut self) {
        let root = self.root;
        let Some(paths) = paths_of(root) else {
            return;
        };
        for (_, item) in path_items(paths) {
            self.walk_params(item.get("parameters"));
            for op in OPERATIONS {
                if let Some(o) = item.get(op) {
                    self.walk_operation(o);
                }
            }
        }
    }
}

/// `walkOnAllReferences`: calls `walk_ref` on every reference reachable from
/// the paths, following references into definitions and parameters.
pub fn walk_on_all_references(walk_ref: &mut dyn FnMut(&str), root: &Value) {
    let mut w = RefWalker {
        root,
        visited: HashSet::new(),
        walk_ref,
    };
    w.start();
}

/// `usedDefinitionForSpec`.
fn used_definition_for_spec(root: &Value) -> HashSet<String> {
    let mut used = HashSet::new();
    walk_on_all_references(
        &mut |r| {
            if let Some(name) = r.strip_prefix(DEFINITION_PREFIX) {
                used.insert(name.to_string());
            }
        },
        root,
    );
    used
}

// ---------------------------------------------------------------------------
// schemamutation.Walker (mutating in place)
// ---------------------------------------------------------------------------

/// `schemamutation.Walker`: `schema_callback` runs on every schema before it
/// is descended into; `ref_callback` returns the replacement for a reference.
pub type RefCallback<'f> = &'f mut dyn FnMut(&str) -> Option<String>;

pub struct Walker<'f> {
    pub schema_callback: Option<&'f mut dyn FnMut(&mut Value)>,
    pub ref_callback: Option<RefCallback<'f>>,
}

impl Walker<'_> {
    fn on_ref(&mut self, owner: &mut Value) {
        let Some(cb) = self.ref_callback.as_mut() else {
            return;
        };
        let Some(r) = ref_of(owner) else {
            return;
        };
        if let Some(new) = cb(r) {
            owner["$ref"] = Value::String(new);
        }
    }

    /// `WalkSchema`.
    pub fn walk_schema(&mut self, schema: &mut Value) {
        if !schema.is_object() {
            return;
        }
        if let Some(cb) = self.schema_callback.as_mut() {
            cb(schema);
        }
        self.on_ref(schema);
        let obj = schema.as_object_mut().unwrap();
        for key in ["definitions", "properties", "patternProperties"] {
            if let Some(m) = obj.get_mut(key).and_then(Value::as_object_mut) {
                for v in m.values_mut() {
                    self.walk_schema(v);
                }
            }
        }
        for key in ["allOf", "anyOf", "oneOf"] {
            if let Some(a) = obj.get_mut(key).and_then(Value::as_array_mut) {
                for v in a {
                    self.walk_schema(v);
                }
            }
        }
        if let Some(n) = obj.get_mut("not") {
            self.walk_schema(n);
        }
        for key in ["additionalProperties", "additionalItems"] {
            if let Some(v) = obj.get_mut(key).filter(|v| v.is_object()) {
                self.walk_schema(v);
            }
        }
        match obj.get_mut("items") {
            Some(v) if v.is_object() => self.walk_schema(v),
            Some(Value::Array(a)) => {
                for v in a {
                    self.walk_schema(v);
                }
            }
            _ => {}
        }
    }

    fn walk_parameter(&mut self, param: &mut Value) {
        if !param.is_object() {
            return;
        }
        self.on_ref(param);
        if let Some(s) = param.get_mut("schema") {
            self.walk_schema(s);
        }
        if let Some(items) = param.get_mut("items") {
            self.on_ref(items);
        }
    }

    fn walk_parameters(&mut self, params: Option<&mut Value>) {
        for p in params.and_then(Value::as_array_mut).into_iter().flatten() {
            self.walk_parameter(p);
        }
    }

    fn walk_response(&mut self, resp: &mut Value) {
        if !resp.is_object() {
            return;
        }
        self.on_ref(resp);
        if let Some(s) = resp.get_mut("schema") {
            self.walk_schema(s);
        }
    }

    fn walk_operation(&mut self, op: &mut Value) {
        if !op.is_object() {
            return;
        }
        self.walk_parameters(op.get_mut("parameters"));
        let Some(responses) = op.get_mut("responses").and_then(Value::as_object_mut) else {
            return;
        };
        if let Some(d) = responses.get_mut("default") {
            self.walk_response(d);
        }
        for (k, r) in responses.iter_mut() {
            if k.parse::<i64>().is_ok() {
                self.walk_response(r);
            }
        }
    }

    /// `WalkRoot`.
    pub fn walk_root(&mut self, swagger: &mut Value) {
        if let Some(m) = swagger.get_mut("parameters").and_then(Value::as_object_mut) {
            for v in m.values_mut() {
                self.walk_parameter(v);
            }
        }
        if let Some(m) = swagger.get_mut("responses").and_then(Value::as_object_mut) {
            for v in m.values_mut() {
                self.walk_response(v);
            }
        }
        if let Some(m) = swagger
            .get_mut("definitions")
            .and_then(Value::as_object_mut)
        {
            for v in m.values_mut() {
                self.walk_schema(v);
            }
        }
        if let Some(paths) = swagger.get_mut("paths").and_then(Value::as_object_mut) {
            for (k, item) in paths.iter_mut() {
                if !k.starts_with('/') {
                    continue;
                }
                self.walk_parameters(item.get_mut("parameters"));
                for op in ["get", "head", "delete", "options", "patch", "post", "put"] {
                    if let Some(o) = item.get_mut(op) {
                        self.walk_operation(o);
                    }
                }
            }
        }
    }
}

/// `ReplaceReferences`.
pub fn replace_references(walk_ref: &mut dyn FnMut(&str) -> Option<String>, sp: &mut Value) {
    let mut w = Walker {
        schema_callback: None,
        ref_callback: Some(walk_ref),
    };
    w.walk_root(sp);
}

// ---------------------------------------------------------------------------
// aggregator.go
// ---------------------------------------------------------------------------

/// `FilterSpecByPathsWithoutSideEffects`: removes the paths without one of the
/// `keep_path_prefixes` and the definitions only those paths used. Does not
/// modify the input.
pub fn filter_spec_by_paths_without_side_effects(
    sp: &Value,
    keep_path_prefixes: &[String],
) -> Value {
    let Some(paths) = paths_of(sp) else {
        return sp.clone();
    };
    // "Walk all references to find all used definitions. This function want
    // to only deal with unused definitions resulted from filtering paths.
    // Thus a definition will be removed only if it has been used before but
    // it is unused because of a path prune."
    let initial_used = used_definition_for_spec(sp);

    // `util.NewTrie(keepPathPrefixes)`, `prefixes.HasPrefix(path)`.
    let mut kept = Map::new();
    for (k, v) in paths.as_object().unwrap() {
        let keep = k.starts_with("x-")
            || (k.starts_with('/') && keep_path_prefixes.iter().any(|p| k.starts_with(p.as_str())));
        if keep {
            kept.insert(k.clone(), v.clone());
        }
    }
    let mut ret = sp.clone();
    ret["paths"] = Value::Object(kept);

    let used = used_definition_for_spec(&ret);
    let mut defs = Map::new();
    if let Some(d) = sp.get("definitions").and_then(Value::as_object) {
        for (k, v) in d {
            if used.contains(k) || !initial_used.contains(k) {
                defs.insert(k.clone(), v.clone());
            }
        }
    }
    let obj = ret.as_object_mut().unwrap();
    if defs.is_empty() {
        obj.remove("definitions");
    } else {
        obj.insert("definitions".to_string(), Value::Object(defs));
    }
    ret
}

/// `renameDefinitions`: `None` when no definition is renamed (the input is
/// used as is).
fn rename_definitions(s: &Value, renames: &BTreeMap<String, String>) -> Option<Value> {
    let defs = s.get("definitions").and_then(Value::as_object);
    let mut ref_renames = BTreeMap::new();
    let mut found_one = false;
    for (k, v) in renames {
        ref_renames.insert(
            format!("{DEFINITION_PREFIX}{k}"),
            format!("{DEFINITION_PREFIX}{v}"),
        );
        if defs.is_some_and(|d| d.contains_key(k)) {
            found_one = true;
        }
    }
    if !found_one {
        return None;
    }
    let mut ret = s.clone();
    replace_references(&mut |r| ref_renames.get(r).cloned(), &mut ret);
    if let Some(d) = ret.get_mut("definitions").and_then(Value::as_object_mut) {
        let old = std::mem::take(d);
        for (k, v) in old {
            let k = renames.get(&k).cloned().unwrap_or(k);
            d.insert(k, v);
        }
    }
    Some(ret)
}

/// `renameParameters`.
fn rename_parameters(s: &Value, renames: &BTreeMap<String, String>) -> Option<Value> {
    let params = s.get("parameters").and_then(Value::as_object);
    let mut ref_renames = BTreeMap::new();
    let mut found_one = false;
    for (k, v) in renames {
        ref_renames.insert(
            format!("{PARAMETER_PREFIX}{k}"),
            format!("{PARAMETER_PREFIX}{v}"),
        );
        if params.is_some_and(|d| d.contains_key(k)) {
            found_one = true;
        }
    }
    if !found_one {
        return None;
    }
    let mut ret = s.clone();
    replace_references(&mut |r| ref_renames.get(r).cloned(), &mut ret);
    if let Some(d) = ret.get_mut("parameters").and_then(Value::as_object_mut) {
        let old = std::mem::take(d);
        for (k, v) in old {
            let k = renames.get(&k).cloned().unwrap_or(k);
            d.insert(k, v);
        }
    }
    Some(ret)
}

/// `MergeSpecsIgnorePathConflictRenamingDefinitionsAndParameters`: the same as
/// `MergeSpecs` except it ignores any path conflicts by keeping the paths of
/// `dest`, and renames definition and parameter conflicts.
pub fn merge_specs_ignore_path_conflict_renaming_definitions_and_parameters(
    dest: &mut Value,
    source: &Value,
) -> Result<(), String> {
    merge_specs(dest, source, true, true, true)
}

/// `mergeSpecs`: merges `source` into `dest` while resolving conflicts. The
/// source is not mutated.
pub fn merge_specs(
    dest: &mut Value,
    source: &Value,
    rename_model_conflicts: bool,
    rename_parameter_conflicts: bool,
    ignore_path_conflicts: bool,
) -> Result<(), String> {
    // "Paths may be empty, due to ACL constraints. When a source spec does
    // not have any path, that means none of the definitions are used thus we
    // should not do anything"
    if paths_of(source).is_none() {
        return Ok(());
    }
    if paths_of(dest).is_none() {
        dest["paths"] = Value::Object(Map::new());
    }
    let mut source_filtered: Option<Value> = None;
    if ignore_path_conflicts {
        let mut keep_paths = Vec::new();
        let mut has_conflicting_path = false;
        let dest_paths = paths_of(dest).unwrap();
        for (k, _) in path_items(paths_of(source).unwrap()) {
            if path_items(dest_paths).any(|(d, _)| d == k) {
                has_conflicting_path = true;
            } else {
                keep_paths.push(k.clone());
            }
        }
        if keep_paths.is_empty() {
            // "There is nothing to merge. All paths are conflicting."
            return Ok(());
        }
        if has_conflicting_path {
            source_filtered = Some(filter_spec_by_paths_without_side_effects(
                source,
                &keep_paths,
            ));
        }
    }
    let mut source: &Value = source_filtered.as_ref().unwrap_or(source);

    // "Check for model conflicts and rename to make definitions
    // conflict-free (modulo different GVKs)"
    let empty = Map::new();
    let dest_defs = dest
        .get("definitions")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let source_defs = source
        .get("definitions")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let mut used_names: HashSet<String> = dest_defs.keys().cloned().collect();
    let mut renames: BTreeMap<String, String> = BTreeMap::new();
    'definitions: for (k, v) in source_defs {
        let existing = dest_defs.get(k);
        if existing.is_none_or(|e| deep_equal_definitions_modulo_gvks(e, v)) {
            // "skip for now, we copy them after the rename loop"
            continue;
        }
        if !rename_model_conflicts {
            return Err(format!("model name conflict in merging OpenAPI spec: {k}"));
        }
        // "Reuse previously renamed model if one exists"
        let mut new_name;
        let mut i = 1;
        loop {
            i += 1;
            new_name = format!("{k}_v{i}");
            match dest_defs.get(&new_name) {
                Some(e) => {
                    if deep_equal_definitions_modulo_gvks(e, v) {
                        renames.insert(k.clone(), new_name);
                        continue 'definitions;
                    }
                }
                None => break,
            }
        }
        let mut found_in_source = source_defs.contains_key(&new_name);
        while used_names.contains(&new_name) || found_in_source {
            i += 1;
            new_name = format!("{k}_v{i}");
            found_in_source = source_defs.contains_key(&new_name);
        }
        renames.insert(k.clone(), new_name.clone());
        used_names.insert(new_name);
    }
    let renamed_defs = rename_definitions(source, &renames);
    if let Some(r) = &renamed_defs {
        source = r;
    }

    // "Check for parameter conflicts and rename to make parameters
    // conflict-free"
    let dest_params = dest
        .get("parameters")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let source_params = source
        .get("parameters")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let mut used_names: HashSet<String> = dest_params.keys().cloned().collect();
    let mut renames: BTreeMap<String, String> = BTreeMap::new();
    'parameters: for (k, p) in source_params {
        let existing = dest_params.get(k);
        if existing.is_none_or(|e| e == p) {
            continue;
        }
        if !rename_parameter_conflicts {
            return Err(format!(
                "parameter name conflict in merging OpenAPI spec: {k}"
            ));
        }
        let mut new_name;
        let mut i = 1;
        loop {
            i += 1;
            new_name = format!("{k}_v{i}");
            match dest_params.get(&new_name) {
                Some(e) => {
                    if e == p {
                        renames.insert(k.clone(), new_name);
                        continue 'parameters;
                    }
                }
                None => break,
            }
        }
        let mut found_in_source = source_params.contains_key(&new_name);
        while used_names.contains(&new_name) || found_in_source {
            i += 1;
            new_name = format!("{k}_v{i}");
            found_in_source = source_params.contains_key(&new_name);
        }
        renames.insert(k.clone(), new_name.clone());
        used_names.insert(new_name);
    }
    let renamed_params = rename_parameters(source, &renames);
    if let Some(r) = &renamed_params {
        source = r;
    }

    // "Now without conflict (modulo different GVKs), copy definitions to dest"
    if let Some(defs) = source.get("definitions").and_then(Value::as_object) {
        for (k, v) in defs {
            let dest_obj = dest.as_object_mut().unwrap();
            let dest_defs = dest_obj
                .entry("definitions")
                .or_insert_with(|| Value::Object(Map::new()))
                .as_object_mut()
                .unwrap();
            match dest_defs.get_mut(k) {
                None => {
                    dest_defs.insert(k.clone(), v.clone());
                }
                Some(existing) => {
                    let (merged, changed) = merged_gvks(existing, v)?;
                    if changed {
                        if let (Some(m), Some(e)) = (merged, existing.as_object_mut()) {
                            e.insert(GVK_KEY.to_string(), m);
                        }
                    }
                }
            }
        }
    }

    // "Now without conflict, copy parameters to dest"
    if let Some(params) = source.get("parameters").and_then(Value::as_object) {
        for (k, v) in params {
            let dest_obj = dest.as_object_mut().unwrap();
            let dest_params = dest_obj
                .entry("parameters")
                .or_insert_with(|| Value::Object(Map::new()))
                .as_object_mut()
                .unwrap();
            dest_params.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }

    // "Check for path conflicts"
    for (k, v) in path_items(paths_of(source).unwrap()) {
        let dest_paths = dest["paths"].as_object_mut().unwrap();
        if dest_paths.contains_key(k) {
            return Err(format!("unable to merge: duplicated path {k}"));
        }
        dest_paths.insert(k.clone(), v.clone());
    }
    Ok(())
}

/// `deepEqualDefinitionsModuloGVKs`: `s1 == s2` ignoring the
/// `x-kubernetes-group-version-kind` extension.
fn deep_equal_definitions_modulo_gvks(s1: &Value, s2: &Value) -> bool {
    match (s1.as_object(), s2.as_object()) {
        (Some(a), Some(b)) => {
            let len = |m: &Map<String, Value>| m.len() - usize::from(m.contains_key(GVK_KEY));
            len(a) == len(b)
                && a.iter()
                    .filter(|(k, _)| k.as_str() != GVK_KEY)
                    .all(|(k, v)| b.get(k) == Some(v))
        }
        _ => s1 == s2,
    }
}

/// `mergedGVKs`: merges the `x-kubernetes-group-version-kind` slices and
/// returns the result (`None` when `s2` has none and `s1` none either), and
/// whether `s1`'s slice was changed at all.
fn merged_gvks(s1: &Value, s2: &Value) -> Result<(Option<Value>, bool), String> {
    let gvk1 = s1.get(GVK_KEY);
    let gvk2 = s2.get(GVK_KEY);
    let Some(gvk1) = gvk1 else {
        return Ok((gvk2.cloned(), gvk2.is_some()));
    };
    let Some(gvk2) = gvk2 else {
        return Ok((Some(gvk1.clone()), false));
    };
    let Some(slice1) = gvk1.as_array() else {
        return Err(format!("expected slice of GroupVersionKinds, got: {gvk1}"));
    };
    let Some(slice2) = gvk2.as_array() else {
        return Err(format!("expected slice of GroupVersionKinds, got: {gvk2}"));
    };
    let key_of = |x: &Value| -> Result<String, String> {
        let gvk = x.as_object().ok_or_else(|| {
            format!(
                r#"expected {{"group": <group>, "kind": <kind>, "version": <version>}}, got: {x:?}"#
            )
        })?;
        let f = |n: &str| {
            gvk.get(n)
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| "%!s(<nil>)".to_string())
        };
        Ok(format!("{}/{}.{}", f("group"), f("version"), f("kind")))
    };
    let mut ret: Vec<Value> = slice1.clone();
    let mut keys: Vec<String> = Vec::new();
    let mut seen = HashSet::new();
    for x in slice1 {
        let k = key_of(x)?;
        keys.push(k.clone());
        seen.insert(k);
    }
    let mut changed = false;
    for x in slice2 {
        let k = key_of(x)?;
        if seen.contains(&k) {
            continue;
        }
        ret.push(x.clone());
        keys.push(k);
        changed = true;
    }
    if changed {
        // `sort.Sort(byKeys{ret, keys})`
        let mut zipped: Vec<(String, Value)> = keys.into_iter().zip(ret).collect();
        zipped.sort_by(|a, b| a.0.cmp(&b.0));
        ret = zipped.into_iter().map(|(_, v)| v).collect();
    }
    Ok((Some(Value::Array(ret)), changed))
}
