//! A field set and the comparison of two objects, over `serde_json::Value`.
//!
//! Port of the parts of `sigs.k8s.io/structured-merge-diff/v6` the Update path
//! needs: `fieldpath.Set` (`fieldpath/set.go`), its `FieldsV1` encoding
//! (`fieldpath/serialize.go`, and `managedfields/internal/fields.go`
//! `FieldsToSet` / `SetToFields`), `typed.TypedValue.ToFieldSet` and
//! `TypedValue.Compare` (`typed/typed.go`, `typed/compare.go`).
//!
//! DEVIATION (tracked in #2701's follow-up): upstream's walkers are driven by
//! the OpenAPI schema (`typeconverter.go`), which says whether a field is a
//! struct, a granular map, a set, an associative list keyed by which fields,
//! or atomic. This tree has no per-type schema, so [`FieldKind`] is inferred
//! from the field name from a small table of the well-known shapes; anything
//! else is a struct (objects) or an atomic leaf (lists).

use std::collections::BTreeMap;

use serde_json::{Map, Value};

/// `fieldpath.Set`, as a trie: a path is a member when `member` is set on the
/// node it ends at. A node may be a member and have children (`"."` in
/// `FieldsV1`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Set {
    member: bool,
    children: BTreeMap<String, Set>,
}

impl Set {
    pub fn new() -> Self {
        Self::default()
    }

    /// `Set.Empty`.
    pub fn is_empty(&self) -> bool {
        !self.member && self.children.is_empty()
    }

    /// `Set.Insert`.
    pub fn insert(&mut self, path: &[String]) {
        let mut node = self;
        for element in path {
            node = node.children.entry(element.clone()).or_default();
        }
        node.member = true;
    }

    /// `Set.Has`.
    pub fn has(&self, path: &[String]) -> bool {
        let mut node = self;
        for element in path {
            match node.children.get(element) {
                Some(child) => node = child,
                None => return false,
            }
        }
        node.member
    }

    /// `Set.Union`.
    pub fn union(&self, other: &Set) -> Set {
        let mut out = self.clone();
        out.member |= other.member;
        for (k, v) in &other.children {
            let merged = match out.children.get(k) {
                Some(mine) => mine.union(v),
                None => v.clone(),
            };
            out.children.insert(k.clone(), merged);
        }
        out
    }

    /// `Set.Difference`: the paths of `self` that are not members of `other`
    /// (a path's children are not removed with it).
    pub fn difference(&self, other: &Set) -> Set {
        let mut out = Set {
            member: self.member && !other.member,
            children: BTreeMap::new(),
        };
        for (k, v) in &self.children {
            let child = match other.children.get(k) {
                Some(o) => v.difference(o),
                None => v.clone(),
            };
            if !child.is_empty() {
                out.children.insert(k.clone(), child);
            }
        }
        out
    }

    /// `Set.RecursiveDifference`: also removes everything under a member of
    /// `other`.
    pub fn recursive_difference(&self, other: &Set) -> Set {
        let mut out = Set {
            member: self.member && !other.member,
            children: BTreeMap::new(),
        };
        for (k, v) in &self.children {
            let child = match other.children.get(k) {
                Some(o) if o.member => Set::new(),
                Some(o) => v.recursive_difference(o),
                None => v.clone(),
            };
            if !child.is_empty() {
                out.children.insert(k.clone(), child);
            }
        }
        out
    }

    /// `Set.Intersection`.
    pub fn intersection(&self, other: &Set) -> Set {
        let mut out = Set {
            member: self.member && other.member,
            children: BTreeMap::new(),
        };
        for (k, v) in &self.children {
            if let Some(o) = other.children.get(k) {
                let child = v.intersection(o);
                if !child.is_empty() {
                    out.children.insert(k.clone(), child);
                }
            }
        }
        out
    }

    /// `Set.Iterate`: every member path, in order.
    pub fn members(&self) -> Vec<Vec<String>> {
        fn walk(node: &Set, path: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
            if node.member {
                out.push(path.clone());
            }
            for (k, child) in &node.children {
                path.push(k.clone());
                walk(child, path, out);
                path.pop();
            }
        }
        let mut out = Vec::new();
        walk(self, &mut Vec::new(), &mut out);
        out
    }

    /// `SetToFields` (fields.go): the `FieldsV1` object.
    pub fn to_fields_v1(&self) -> Value {
        let mut map = Map::new();
        if self.member && !self.children.is_empty() {
            map.insert(".".to_string(), Value::Object(Map::new()));
        }
        for (k, v) in &self.children {
            map.insert(k.clone(), v.to_fields_v1());
        }
        Value::Object(map)
    }

    /// `FieldsToSet` (fields.go): `None` when a key is not a path element.
    pub fn from_fields_v1(fields: &Value) -> Option<Set> {
        let obj = fields.as_object()?;
        let mut set = Set::new();
        if obj.is_empty() {
            // An empty node is a leaf of its parent; the root is empty.
            set.member = true;
            return Some(set);
        }
        for (k, v) in obj {
            if k == "." {
                set.member = true;
                continue;
            }
            if !(k.starts_with("f:")
                || k.starts_with("k:")
                || k.starts_with("v:")
                || k.starts_with("i:"))
            {
                return None;
            }
            set.children.insert(k.clone(), Set::from_fields_v1(v)?);
        }
        Some(set)
    }

    /// Decode a whole managed set: the root of a `FieldsV1` is never a member.
    pub fn from_root_fields_v1(fields: &Value) -> Option<Set> {
        let mut set = Set::from_fields_v1(fields)?;
        set.member = false;
        Some(set)
    }
}

/// What the (absent) schema would say about a field.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum FieldKind {
    Struct,
    /// A granular map (`labels`): the map itself is a member.
    Map,
    /// An associative list keyed by these fields.
    KeyedList(&'static [&'static str]),
    /// A `listType=set` of scalars.
    ScalarSet,
}

pub(super) fn field_kind(name: &str) -> FieldKind {
    match name {
        "labels" | "annotations" | "data" | "binaryData" | "stringData" | "nodeSelector"
        | "matchLabels" | "limits" | "requests" | "capacity" | "allocatable" => FieldKind::Map,
        "containers"
        | "initContainers"
        | "ephemeralContainers"
        | "volumes"
        | "env"
        | "imagePullSecrets" => FieldKind::KeyedList(&["name"]),
        "volumeMounts" => FieldKind::KeyedList(&["mountPath"]),
        "volumeDevices" => FieldKind::KeyedList(&["devicePath"]),
        "conditions" => FieldKind::KeyedList(&["type"]),
        "ownerReferences" => FieldKind::KeyedList(&["uid"]),
        "hostAliases" => FieldKind::KeyedList(&["ip"]),
        "finalizers" => FieldKind::ScalarSet,
        _ => FieldKind::Struct,
    }
}

/// `typed.TypedValue.ToFieldSet` plus the leaf values `Compare` needs.
#[derive(Debug, Default)]
pub struct Fields {
    pub set: Set,
    leaves: BTreeMap<Vec<String>, Value>,
}

/// The `Comparison` of `typed/compare.go`.
#[derive(Debug, Default)]
pub struct Comparison {
    pub removed: Set,
    pub modified: Set,
    pub added: Set,
}

impl Comparison {
    /// `Comparison.FilterFields` with an `ExcludeSetFilter`
    /// (`fieldpath/set.go` `excludeFilter`): drop everything under `exclude`.
    pub fn exclude(&self, exclude: &Set) -> Comparison {
        Comparison {
            removed: self.removed.recursive_difference(exclude),
            modified: self.modified.recursive_difference(exclude),
            added: self.added.recursive_difference(exclude),
        }
    }
}

impl Fields {
    /// `ToFieldSet` of a whole object. `null`s are absent (a typed Go object
    /// has no such value).
    pub fn of(obj: &Value) -> Fields {
        let mut fields = Fields::default();
        if let Some(map) = obj.as_object() {
            let mut path = Vec::new();
            for (k, v) in map {
                fields.field(&mut path, k, v);
            }
        }
        fields
    }

    fn leaf(&mut self, path: &[String], v: &Value) {
        self.set.insert(path);
        self.leaves.insert(path.to_vec(), v.clone());
    }

    fn field(&mut self, path: &mut Vec<String>, name: &str, v: &Value) {
        if v.is_null() {
            return;
        }
        path.push(format!("f:{name}"));
        match v {
            Value::Object(m) if m.is_empty() => self.leaf(path, v),
            Value::Object(m) => {
                if field_kind(name) == FieldKind::Map {
                    self.set.insert(path);
                }
                for (k, child) in m {
                    self.field(path, k, child);
                }
            }
            Value::Array(items) => self.list(path, field_kind(name), items, v),
            _ => self.leaf(path, v),
        }
        path.pop();
    }

    fn list(&mut self, path: &mut Vec<String>, kind: FieldKind, items: &[Value], whole: &Value) {
        match kind {
            FieldKind::KeyedList(keys) => {
                let elements: Option<Vec<String>> =
                    items.iter().map(|item| key_element(keys, item)).collect();
                match elements {
                    Some(elements) if !items.is_empty() => {
                        for (element, item) in elements.into_iter().zip(items) {
                            path.push(element);
                            self.set.insert(path);
                            if let Some(m) = item.as_object() {
                                for (k, child) in m {
                                    self.field(path, k, child);
                                }
                            }
                            path.pop();
                        }
                    }
                    _ => self.leaf(path, whole),
                }
            }
            FieldKind::ScalarSet if items.iter().all(|i| !i.is_object() && !i.is_array()) => {
                for item in items {
                    path.push(format!("v:{item}"));
                    self.leaf(path, item);
                    path.pop();
                }
            }
            _ => self.leaf(path, whole),
        }
    }

    /// `TypedValue.Compare`: `self` is the old object.
    pub fn compare(&self, new: &Fields) -> Comparison {
        let mut modified = Set::new();
        for (path, old_value) in &self.leaves {
            if let Some(new_value) = new.leaves.get(path) {
                if old_value != new_value {
                    modified.insert(path);
                }
            }
        }
        Comparison {
            removed: self.set.difference(&new.set),
            added: new.set.difference(&self.set),
            modified,
        }
    }
}

/// `k:{"name":"x"}`: the item's key fields as a sorted JSON object, `None`
/// when the item lacks one (the list is then not associative).
pub(super) fn key_element(keys: &[&str], item: &Value) -> Option<String> {
    let obj = item.as_object()?;
    let mut sorted = BTreeMap::new();
    for key in keys {
        sorted.insert(*key, obj.get(*key).filter(|v| !v.is_null())?);
    }
    Some(format!("k:{}", serde_json::to_string(&sorted).ok()?))
}

/// A path of plain field names, as `fieldpath.MakePathOrDie`.
pub fn field_path(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| format!("f:{n}")).collect()
}
