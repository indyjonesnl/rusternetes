//! `DefinitionsSchemaResolver` and `PopulateRefs` of
//! `k8s.io/apiserver/pkg/cel/openapi/resolver` (release-1.35,
//! `definitions.go`, `refs.go`) over the OpenAPI v2 `definitions` map.
//!
//! Upstream builds the resolver from the generated `GetOpenAPIDefinitions`,
//! a flat map of `$ref`-linked definitions whose
//! `x-kubernetes-group-version-kind` extension ties a definition to its GVK
//! (`extensionsToGVKs`). The v2 `definitions` of `swagger.json` are that same
//! map, so the resolver takes them as data and does no I/O: the embedded
//! document is the caller's (`api-server`'s `upstream_v2_definitions`).

use crate::resources::crd::{JSONSchemaProps, JSONSchemaPropsOrArray, JSONSchemaPropsOrBool};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

/// `schema.GroupVersionKind`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Gvk {
    pub group: String,
    pub version: String,
    pub kind: String,
}

impl Gvk {
    pub fn new(group: &str, version: &str, kind: &str) -> Self {
        Self {
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
        }
    }
}

/// `resolver.ErrSchemaNotFound` and internal failures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolveError {
    /// `ErrSchemaNotFound`.
    NotFound(String),
    /// Anything else (upstream: "internal error").
    Internal(String),
}

const REF_PREFIX: &str = "#/definitions/";

/// `NewDefinitionsSchemaResolver`.
pub struct DefinitionsSchemaResolver<'a> {
    defs: &'a Map<String, Value>,
    gvk_to_ref: HashMap<Gvk, String>,
}

impl<'a> DefinitionsSchemaResolver<'a> {
    pub fn new(defs: &'a Map<String, Value>) -> Self {
        let mut gvk_to_ref = HashMap::new();
        for (name, def) in defs {
            for gvk in extensions_to_gvks(def) {
                gvk_to_ref.insert(gvk, name.clone());
            }
        }
        Self { defs, gvk_to_ref }
    }

    /// `ResolveSchema`: the schema of `gvk` with every `$ref` populated.
    pub fn resolve_schema(&self, gvk: &Gvk) -> Result<JSONSchemaProps, ResolveError> {
        let Some(root) = self.gvk_to_ref.get(gvk) else {
            return Err(ResolveError::NotFound(format!(
                "cannot resolve {}/{}, Kind={}",
                gvk.group, gvk.version, gvk.kind
            )));
        };
        populate_refs(&|r| self.schema_of(r), &format!("{REF_PREFIX}{root}"))
    }

    /// The by-ref lookup `DefinitionsSchemaResolver.ResolveSchema` hands to
    /// `PopulateRefs`: a deep copy of the definition.
    fn schema_of(&self, r: &str) -> Option<JSONSchemaProps> {
        let name = r.strip_prefix(REF_PREFIX).unwrap_or(r);
        serde_json::from_value(self.defs.get(name)?.clone()).ok()
    }
}

/// `extensionsToGVKs`: the `x-kubernetes-group-version-kind` list of a
/// definition, or empty if it is malformed.
fn extensions_to_gvks(def: &Value) -> Vec<Gvk> {
    let Some(list) = def
        .get("x-kubernetes-group-version-kind")
        .and_then(Value::as_array)
    else {
        return vec![];
    };
    let mut out = Vec::with_capacity(list.len());
    for item in list {
        let field = |k: &str| item.get(k).and_then(Value::as_str);
        match (field("group"), field("version"), field("kind")) {
            (Some(g), Some(v), Some(k)) => out.push(Gvk::new(g, v, k)),
            _ => return vec![],
        }
    }
    out
}

/// `refOf`: the `$ref` of a schema, possibly wrapped in `allOf` to keep its
/// description (kubernetes/kubernetes#106387).
fn ref_of(schema: &JSONSchemaProps) -> Option<String> {
    if let Some(r) = schema.ref_path.as_ref().filter(|r| !r.is_empty()) {
        return Some(r.clone());
    }
    schema.all_of.iter().flatten().find_map(ref_of)
}

/// `PopulateRefs`: replaces every `$ref` below the schema of `root_ref` with
/// the schema it refers to. `schema_of` returns a copy of the schema for a
/// ref string.
pub fn populate_refs(
    schema_of: &dyn Fn(&str) -> Option<JSONSchemaProps>,
    root_ref: &str,
) -> Result<JSONSchemaProps, ResolveError> {
    let mut visited = HashSet::new();
    let Some(root) = schema_of(root_ref) else {
        return Err(ResolveError::NotFound(format!(
            "internal error: cannot resolve Ref for root schema {root_ref:?}"
        )));
    };
    visited.insert(root_ref.to_string());
    populate(schema_of, &mut visited, root)
}

fn populate(
    schema_of: &dyn Fn(&str) -> Option<JSONSchemaProps>,
    visited: &mut HashSet<String>,
    schema: JSONSchemaProps,
) -> Result<JSONSchemaProps, ResolveError> {
    let mut result = schema.clone();
    let mut inserted_ref = None;
    if let Some(r) = ref_of(&schema) {
        if visited.contains(&r) {
            // For a circular ref, return an empty object as placeholder.
            return Ok(JSONSchemaProps {
                type_: Some("object".into()),
                ..Default::default()
            });
        }
        visited.insert(r.clone());
        inserted_ref = Some(r.clone());
        // Replace the whole schema with the referred one.
        match schema_of(&r) {
            Some(resolved) => result = resolved,
            None => {
                visited.remove(&r);
                return Err(ResolveError::NotFound(format!(
                    "internal error: cannot resolve Ref {r:?}"
                )));
            }
        }
    }
    let outcome = populate_children(schema_of, visited, &mut result);
    // Restore the visited state at the end of the recursion.
    if let Some(r) = inserted_ref {
        visited.remove(&r);
    }
    outcome.map(|()| result)
}

fn populate_children(
    schema_of: &dyn Fn(&str) -> Option<JSONSchemaProps>,
    visited: &mut HashSet<String>,
    result: &mut JSONSchemaProps,
) -> Result<(), ResolveError> {
    // An object: its properties and additionalProperties.
    if let Some(props) = result.properties.take() {
        let mut populated = HashMap::with_capacity(props.len());
        for (name, prop) in props {
            populated.insert(name, populate(schema_of, visited, prop)?);
        }
        result.properties = Some(populated);
    }
    if let Some(JSONSchemaPropsOrBool::Schema(extra)) = result.additional_properties.as_deref_mut()
    {
        *extra = populate(schema_of, visited, std::mem::take(extra))?;
    }
    // A list: its items.
    if let Some(JSONSchemaPropsOrArray::Schema(items)) = result.items.as_deref_mut() {
        *items = populate(schema_of, visited, std::mem::take(items))?;
    }
    Ok(())
}
