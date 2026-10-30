//! Structural merge primitives for Server-Side Apply (SSA).
//!
//! This module implements the smallest viable subset of K8s SSA: a JSON-Pointer
//! based ownership tracker for string-map fields (`data`, `binaryData`,
//! `metadata.labels`, `metadata.annotations`, `stringData`) plus a small set
//! of top-level atomic scalar leaves (e.g. `Secret.type`).
//!
//! # Schema-driven design
//!
//! Each supported resource registers a [`ResourceLeafSchema`] enumerating
//! which string-map groups exist (via [`LeafGroup`]) and which atomic scalar
//! leaves the applier may touch. The SSA driver
//! ([`super::apply_via_schema`]) iterates the schema rather than hard-coding
//! ConfigMap-specific groups. Today ConfigMap and Secret are registered;
//! other resources go through [`super::apply_legacy`], which applies via the
//! field manager (`rusternetes_common::fieldmanager`).
//!
//! The full upstream merge algorithm in
//! `staging/src/k8s.io/apimachinery/pkg/util/managedfields/internal/typeconverter.go`
//! is fully schema-driven off the OpenAPI spec. We deliberately ship a
//! curated per-resource leaf schema instead: the OpenAPI converter is a much
//! bigger lift, and the resources we care about for the first round of SSA
//! conformance (ConfigMap, Secret) only need granular string-map merges plus
//! a couple of atomic scalars.
//!
//! # Ownership representation
//!
//! Each manager owns a set of JSON-Pointers. For ConfigMap that means paths of
//! the form:
//!
//! ```text
//! /data/<key>
//! /binaryData/<key>
//! /metadata/labels/<key>
//! /metadata/annotations/<key>
//! ```
//!
//! Encoded as a `fieldsV1` v1-style tree the same paths look like:
//!
//! ```json
//! {
//!   "f:data": { "f:<key>": {} },
//!   "f:metadata": {
//!     "f:labels":      { "f:<key>": {} },
//!     "f:annotations": { "f:<key>": {} }
//!   }
//! }
//! ```
//!
//! We expose both forms: the flat `OwnedPaths` set for internal merge logic,
//! and a `to_fields_v1` / `from_fields_v1` codec for persistence in
//! `metadata.managedFields[*].fieldsV1`.

use serde_json::{json, Map, Value};
use std::collections::BTreeSet;

/// A flat set of JSON-Pointers owned by a single field manager.
///
/// JSON-Pointers are stored without the leading `/` for compactness:
/// `data/foo`, `metadata/labels/app`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnedPaths(pub BTreeSet<String>);

impl OwnedPaths {
    /// Create an empty path set.
    pub fn new() -> Self {
        Self(BTreeSet::new())
    }

    /// Insert a JSON-Pointer (without leading `/`).
    pub fn insert(&mut self, path: impl Into<String>) {
        self.0.insert(path.into());
    }

    /// Whether the manager owns the given path.
    pub fn contains(&self, path: &str) -> bool {
        self.0.contains(path)
    }

    /// Iterate owned paths in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.0.iter()
    }

    /// Number of owned paths.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// True if no paths are owned.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Encode this path set as the `fieldsV1` v1 tree used by upstream
    /// Kubernetes for `metadata.managedFields[*].fieldsV1`.
    pub fn to_fields_v1(&self) -> Value {
        let mut root = Map::new();
        for path in &self.0 {
            let segments = split_pointer_segments(path);
            insert_segment(&mut root, &segments);
        }
        Value::Object(root)
    }

    /// Decode a `fieldsV1` v1 tree into a flat owned-paths set.
    ///
    /// Leaves are recognised by an empty object `{}` at the bottom of the
    /// `f:` prefix chain — upstream calls these `PathElement{FieldName}`
    /// leaves. Any non-`f:` keys (e.g. `v:`, `i:`, `k:`) are ignored at the
    /// scaffold level — those are required for list/set/map-of-struct
    /// semantics we don't yet support.
    pub fn from_fields_v1(value: &Value) -> Self {
        let mut out = OwnedPaths::new();
        if let Some(obj) = value.as_object() {
            collect_paths(obj, &mut Vec::new(), &mut out);
        }
        out
    }
}

/// Split an owned JSON-Pointer (no leading `/`) into the segments used to
/// build the `fieldsV1` `f:`-prefixed tree.
///
/// A naive `path.split('/')` is wrong for string-map groups: a label or
/// annotation KEY may itself contain `/` (e.g. `controller.cert-manager.io/fao`)
/// or `.`. In Kubernetes `fieldsV1`, the map key is a single `f:<key>` segment —
/// the slash is part of the key, not a path separator. Splitting it would emit
/// `f:controller.cert-manager.io > f:fao` instead of the correct
/// `f:controller.cert-manager.io/fao`, and an applier (e.g. cert-manager) that
/// compares its managed fieldset would never converge — it re-applies forever.
///
/// So for the known map-group prefixes the trailing key is kept atomic; every
/// other pointer (atomic leaves like `type`, `data/<key>` where keys can't
/// contain `/`) splits normally.
fn split_pointer_segments(path: &str) -> Vec<&str> {
    // Longest-first so `metadata/labels` wins before any shorter match.
    const MAP_GROUP_PREFIXES: &[&str] = &[
        "metadata/labels",
        "metadata/annotations",
        "binaryData",
        "stringData",
        "data",
    ];
    for prefix in MAP_GROUP_PREFIXES {
        if let Some(key) = path
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_prefix('/'))
        {
            // Empty key shouldn't occur, but guard against a trailing slash.
            if !key.is_empty() {
                let mut segments: Vec<&str> = prefix.split('/').collect();
                segments.push(key);
                return segments;
            }
        }
    }
    path.split('/').collect()
}

fn insert_segment(node: &mut Map<String, Value>, segments: &[&str]) {
    if segments.is_empty() {
        return;
    }
    let key = format!("f:{}", segments[0]);
    let rest = &segments[1..];
    let entry = node.entry(key).or_insert_with(|| Value::Object(Map::new()));
    if rest.is_empty() {
        // Leaf — upstream encodes leaves as empty objects.
        *entry = Value::Object(Map::new());
    } else if let Value::Object(child) = entry {
        insert_segment(child, rest);
    } else {
        // The previous entry was a leaf but we are extending it — promote
        // it back to an object. This can only happen if the caller stored
        // both `a/b` and `a/b/c`; upstream never does that for ConfigMap.
        let mut child = Map::new();
        insert_segment(&mut child, rest);
        *entry = Value::Object(child);
    }
}

fn collect_paths(node: &Map<String, Value>, stack: &mut Vec<String>, out: &mut OwnedPaths) {
    if node.is_empty() && !stack.is_empty() {
        out.insert(stack.join("/"));
        return;
    }
    for (key, child) in node {
        // Only `f:` (FieldName) keys describe ownership at the scaffold
        // level. Skip `v:`, `i:`, `k:` (value/index/key set discriminators)
        // — those require the schema-driven Forge.
        let Some(field) = key.strip_prefix("f:") else {
            continue;
        };
        stack.push(field.to_string());
        if let Some(child_obj) = child.as_object() {
            if child_obj.is_empty() {
                out.insert(stack.join("/"));
            } else {
                collect_paths(child_obj, stack, out);
            }
        } else {
            // Non-object child — treat as a leaf.
            out.insert(stack.join("/"));
        }
        stack.pop();
    }
}

/// A single conflict between the applying manager and an existing owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathConflict {
    /// The dotted JSON-Pointer (without leading `/`) of the disputed leaf.
    pub path: String,
    /// The manager that currently owns the leaf.
    pub current_manager: String,
    /// The manager trying to apply.
    pub applying_manager: String,
}

/// The string-map leaf groups SSA recognises.
///
/// Each group is a flat string→Value map. Server-Side Apply on these is
/// always a granular merge: keys present in `desired` are claimed by the
/// applying manager; keys absent from `desired` but previously owned by
/// the applying manager are removed; keys owned by some other manager are
/// left untouched (or trigger a conflict if `desired` tries to set them).
///
/// `StringData` is Secret-specific — it's a write-only sibling of `data`
/// whose entries are base64-encoded and merged into `data` at storage time
/// (see [`rusternetes_common::resources::Secret::normalize`]). SSA still
/// tracks ownership of the raw `/stringData/<key>` paths so a manager that
/// applied via `stringData` can later release those entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafGroup {
    Data,
    BinaryData,
    StringData,
    Labels,
    Annotations,
}

impl LeafGroup {
    /// JSON-Pointer prefix (no leading `/`) for this group.
    pub fn pointer_prefix(self) -> &'static str {
        match self {
            LeafGroup::Data => "data",
            LeafGroup::BinaryData => "binaryData",
            LeafGroup::StringData => "stringData",
            LeafGroup::Labels => "metadata/labels",
            LeafGroup::Annotations => "metadata/annotations",
        }
    }

    /// The JSON object path (sequence of object keys, no leading `/`) where
    /// this group's inner map lives. `Data` lives at `["data"]`, `Labels`
    /// lives at `["metadata", "labels"]`, etc.
    fn path(self) -> &'static [&'static str] {
        match self {
            LeafGroup::Data => &["data"],
            LeafGroup::BinaryData => &["binaryData"],
            LeafGroup::StringData => &["stringData"],
            LeafGroup::Labels => &["metadata", "labels"],
            LeafGroup::Annotations => &["metadata", "annotations"],
        }
    }

    /// Walk the resource and return the inner string→Value map for this
    /// group, or an empty map if the parent path doesn't exist.
    pub fn extract_map(self, resource: &Value) -> Map<String, Value> {
        let mut cursor = resource;
        for segment in self.path() {
            match cursor.get(segment) {
                Some(next) => cursor = next,
                None => return Map::new(),
            }
        }
        cursor.as_object().cloned().unwrap_or_default()
    }

    /// Replace the inner map for this group on `resource`. Empty maps are
    /// removed (parity with upstream: an empty `data` field is serialised
    /// out, not stored as `data: {}`).
    pub fn set_map(self, resource: &mut Value, map: Map<String, Value>) {
        let Some(root) = resource.as_object_mut() else {
            return;
        };
        let segments = self.path();
        // Walk-or-create down to the parent of the leaf key.
        let (leaf_key, parents) = segments
            .split_last()
            .expect("LeafGroup::path() is never empty");
        let mut cursor = root;
        for segment in parents {
            let entry = cursor
                .entry((*segment).to_string())
                .or_insert_with(|| json!({}));
            match entry.as_object_mut() {
                Some(obj) => cursor = obj,
                None => return,
            }
        }
        if map.is_empty() {
            cursor.remove(*leaf_key);
        } else {
            cursor.insert((*leaf_key).to_string(), Value::Object(map));
        }
    }
}

/// Declarative SSA merge surface for one resource kind.
///
/// Each registered resource lists which string-map groups exist on it (the
/// common cases — `data`, `metadata.labels`, etc.) and which top-level
/// scalar leaves the applier may touch (e.g. `Secret.type`). The SSA driver
/// iterates this schema rather than hard-coding per-kind logic, so adding a
/// new resource is purely a matter of declaring its schema.
///
/// This is the rusternetes equivalent of the OpenAPI-driven type converter
/// in `staging/src/k8s.io/apimachinery/pkg/util/managedfields/internal/typeconverter.go`
/// — except we curate the schema by hand per resource we want to support,
/// which keeps the implementation small while still being general enough
/// to add new resources without touching the merge core.
#[derive(Debug, Clone)]
pub struct ResourceLeafSchema {
    /// Kind name (e.g. `"ConfigMap"`, `"Secret"`). Used for diagnostics only.
    pub kind: &'static str,
    /// Plural resource name as it appears in API paths (e.g. `"configmaps"`,
    /// `"secrets"`). Surfaced via the public field so downstream tooling
    /// (audit, conformance reporting) can correlate a schema with its
    /// REST path without re-deriving it.
    pub apply_resource: &'static str,
    /// String-map groups merged with granular ownership.
    pub string_map_groups: &'static [LeafGroup],
    /// Top-level scalar leaves an Apply request may touch directly. Each
    /// entry is a top-level JSON object key (no nested paths supported
    /// yet — Secret only needs `type`).
    pub atomic_leaves: &'static [&'static str],
    /// Whether the resource honours `immutable: true` post-create (true
    /// for both ConfigMap and Secret). The post-merge enforcement lives
    /// in the resource's update validation (the ConfigMap and Secret
    /// strategies) rather than in the merge core; this flag is
    /// exposed so a future generic immutability fence can consult the
    /// schema instead of being hard-coded per resource.
    pub immutable_supported: bool,
}

/// SSA schema for `ConfigMap`. Four string-map groups, no atomic leaves
/// outside `metadata.*`.
pub const CONFIGMAP_SCHEMA: ResourceLeafSchema = ResourceLeafSchema {
    kind: "ConfigMap",
    apply_resource: "configmaps",
    string_map_groups: &[
        LeafGroup::Data,
        LeafGroup::BinaryData,
        LeafGroup::Labels,
        LeafGroup::Annotations,
    ],
    atomic_leaves: &[],
    immutable_supported: true,
};

/// SSA schema for `Secret`. Two payload string-map groups (`data` plus the
/// write-only `stringData` sibling), the usual metadata maps, and a single
/// atomic scalar — `type` — which upstream
/// (`pkg/registry/core/secret/strategy.go::ValidateUpdate`) treats as
/// immutable post-create via `ValidateImmutableField`.
pub const SECRET_SCHEMA: ResourceLeafSchema = ResourceLeafSchema {
    kind: "Secret",
    apply_resource: "secrets",
    string_map_groups: &[
        LeafGroup::Data,
        LeafGroup::StringData,
        LeafGroup::Labels,
        LeafGroup::Annotations,
    ],
    atomic_leaves: &["type"],
    immutable_supported: true,
};

/// Per-group structural merge result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupMergeOutcome {
    /// JSON-Pointers (no leading `/`) the applying manager now owns.
    pub claimed: OwnedPaths,
    /// JSON-Pointers (no leading `/`) the applying manager has released
    /// (i.e. were previously owned by the manager but absent from `desired`).
    pub released: OwnedPaths,
    /// Conflicts encountered when `force=false`.
    pub conflicts: Vec<PathConflict>,
}

/// Merge a single string-map leaf group, returning the merged map plus
/// claimed / released / conflict bookkeeping.
///
/// # Semantics
///
/// 1. For every key present in `desired_map`:
///    - If no other manager owns the path, take the value from `desired_map`
///      and mark the path as claimed by `applying_manager`.
///    - If another manager owns the path and the value is unchanged, leave
///      it alone and re-claim it (shared ownership is allowed when values
///      match).
///    - If another manager owns the path and the value differs, record a
///      `PathConflict`. When `force=true` overwrite anyway and transfer
///      ownership; when `force=false` keep the current value (the caller
///      will surface the conflict as a 409).
/// 2. For every key in `current_map` not present in `desired_map`:
///    - If `applying_manager` is the sole previous owner, drop the key and
///      mark the path as released.
///    - Otherwise leave it alone.
///
/// `other_owners` maps JSON-Pointer → manager-name for every leaf currently
/// owned by a *different* manager than `applying_manager`. Paths that the
/// applying manager already owned are simply absent from `other_owners`.
pub fn merge_string_map_group(
    group: LeafGroup,
    current_map: &Map<String, Value>,
    desired_map: &Map<String, Value>,
    applying_manager: &str,
    other_owners: &std::collections::BTreeMap<String, String>,
    previously_owned_by_applier: &OwnedPaths,
    force: bool,
) -> (Map<String, Value>, GroupMergeOutcome) {
    let prefix = group.pointer_prefix();
    let mut merged = current_map.clone();
    let mut outcome = GroupMergeOutcome::default();

    // Pass 1 — keys in desired.
    for (key, desired_value) in desired_map {
        let path = format!("{}/{}", prefix, key);
        let current_value = current_map.get(key);
        let other_owner = other_owners.get(&path);

        match other_owner {
            Some(owner) if current_value != Some(desired_value) => {
                // Conflict: another manager owns a leaf and applier wants to
                // change it.
                outcome.conflicts.push(PathConflict {
                    path: path.clone(),
                    current_manager: owner.clone(),
                    applying_manager: applying_manager.to_string(),
                });
                if force {
                    merged.insert(key.clone(), desired_value.clone());
                    outcome.claimed.insert(path);
                }
            }
            _ => {
                // Either no other owner, or other owner with identical value.
                merged.insert(key.clone(), desired_value.clone());
                outcome.claimed.insert(path);
            }
        }
    }

    // Pass 2 — keys in current but not in desired.
    for key in current_map.keys() {
        if desired_map.contains_key(key) {
            continue;
        }
        let path = format!("{}/{}", prefix, key);
        // Drop only if the applying manager previously owned this leaf AND
        // no other manager currently owns it.
        let applier_owned = previously_owned_by_applier.contains(&path);
        let foreign_owned = other_owners.contains_key(&path);
        if applier_owned && !foreign_owned {
            merged.remove(key);
            outcome.released.insert(path);
        }
    }

    (merged, outcome)
}
