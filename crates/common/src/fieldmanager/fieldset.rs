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

use super::schema::{ListKind, Type};

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

    /// `Set.FilterIncludeMatches` (fieldpath/set.go:323-342) and
    /// `SetNodeMap.FilterIncludeMatches` (:704-728): only the field paths
    /// `pattern` matches.
    ///
    /// Upstream keeps a `Set`'s members and its children apart; here an
    /// element is one node carrying both. An element survives when a matcher
    /// at its level matches it (a wildcard takes precedence), keeping its
    /// member flag whatever that matcher's child says, and its children are
    /// filtered by that matcher's child; a node with neither left is dropped.
    pub fn filter_include_matches(&self, pattern: &SetMatcher) -> Set {
        if pattern.wildcard {
            return self.clone();
        }
        let mut out = Set::new();
        for (element, node) in &self.children {
            let matcher = pattern
                .members
                .iter()
                .find(|(pm, _)| *pm == PathElementMatcher::Any)
                .or_else(|| pattern.members.iter().find(|(pm, _)| pm.matches(element)));
            let Some((_, child_matcher)) = matcher else {
                continue;
            };
            let below = Set {
                member: false,
                children: node.children.clone(),
            }
            .filter_include_matches(child_matcher);
            let kept = Set {
                member: node.member,
                children: below.children,
            };
            if !kept.is_empty() {
                out.children.insert(element.clone(), kept);
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
pub enum FieldKind {
    Struct,
    /// A granular map (`labels`): the map itself is a member.
    Map,
    /// An associative list keyed by these fields.
    KeyedList(&'static [&'static str]),
    /// A `listType=set` of scalars.
    ScalarSet,
    /// `+structType=atomic` / `+mapType=atomic`: the whole value is one leaf
    /// (`metav1.LabelSelector`, `Service.spec.selector`, ...).
    Atomic,
}

pub fn field_kind(name: &str) -> FieldKind {
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
        "selector" | "podSelector" | "namespaceSelector" | "labelSelector" => FieldKind::Atomic,
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

/// `fieldpath.PathElementMatcher` (set.go:290-299): any element, or the one
/// element (`"f:name"`, `"k:{...}"`, `"v:..."`, `"i:N"`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathElementMatcher {
    /// `MatchAnyPathElement()`.
    Any,
    Element(String),
}

impl PathElementMatcher {
    /// A plain field name, `PathElementMatcher{PathElement{FieldName}}`.
    pub fn field(name: &str) -> Self {
        Self::Element(format!("f:{name}"))
    }

    /// A list index, `PathElementMatcher{PathElement{Index}}`.
    pub fn index(index: usize) -> Self {
        Self::Element(format!("i:{index}"))
    }

    /// A scalar or set-type list element, `PathElement{Value}`.
    pub fn value(value: &Value) -> Self {
        Self::Element(format!("v:{value}"))
    }

    /// A list-map key, `PathElement{Key}`: the key fields as a sorted JSON
    /// object. `None` when there is no key field (upstream: "associative list
    /// key type path elements must have at least one key").
    pub fn key(keys: &[(&str, Value)]) -> Option<Self> {
        if keys.is_empty() {
            return None;
        }
        let sorted: BTreeMap<&str, &Value> = keys.iter().map(|(k, v)| (*k, v)).collect();
        Some(Self::Element(format!(
            "k:{}",
            serde_json::to_string(&sorted).ok()?
        )))
    }

    fn matches(&self, element: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Element(e) => e == element,
        }
    }
}

/// `fieldpath.SetMatcher` (set.go:229-239): a `Set` with wildcards.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SetMatcher {
    wildcard: bool,
    members: Vec<(PathElementMatcher, SetMatcher)>,
}

impl SetMatcher {
    /// `MatchAnySet()`.
    pub fn any() -> Self {
        Self {
            wildcard: true,
            members: Vec::new(),
        }
    }

    /// `PrefixMatcher(parts...)` (set.go:171-209): every field path prefixed
    /// by `parts`.
    pub fn prefix(parts: &[PathElementMatcher]) -> Self {
        let mut current = Self::any();
        for part in parts.iter().rev() {
            current = Self {
                wildcard: false,
                members: vec![(part.clone(), current)],
            };
        }
        current
    }

    /// `SetMatcher.Merge` (set.go:255-273): matches what either matches.
    pub fn merge(&self, other: &SetMatcher) -> SetMatcher {
        if self.wildcard || other.wildcard {
            return Self::any();
        }
        let mut members = self.members.clone();
        for (path, child) in &other.members {
            match members.iter_mut().find(|(p, _)| p == path) {
                Some((_, existing)) => *existing = existing.merge(child),
                None => members.push((path.clone(), child.clone())),
            }
        }
        Self {
            wildcard: false,
            members,
        }
    }
}

/// `fieldpath.Filter` (set.go:767-780): what a strategy tells the field
/// manager it resets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    /// `NewExcludeSetFilter`: removes the paths in the set and everything under
    /// them.
    Exclude(Set),
    /// `NewIncludeMatcherFilter`: removes everything the matcher does not
    /// match.
    IncludeMatcher(SetMatcher),
}

impl Filter {
    /// `NewIncludeMatcherFilter(matchers...)` (set.go:803-814): no matcher
    /// includes everything.
    pub fn include_matcher(matchers: &[SetMatcher]) -> Filter {
        let mut iter = matchers.iter();
        let Some(first) = iter.next() else {
            return Filter::IncludeMatcher(SetMatcher::any());
        };
        Filter::IncludeMatcher(iter.fold(first.clone(), |acc, m| acc.merge(m)))
    }

    /// `Filter.Filter`: a filtered copy of the set.
    pub fn filter(&self, set: &Set) -> Set {
        match self {
            Filter::Exclude(exclude) => set.recursive_difference(exclude),
            Filter::IncludeMatcher(matcher) => set.filter_include_matches(matcher),
        }
    }
}

impl Comparison {
    /// `Comparison.FilterFields` (typed/compare.go:75-83).
    pub fn filter_fields(&self, filter: &Filter) -> Comparison {
        Comparison {
            removed: filter.filter(&self.removed),
            modified: filter.filter(&self.modified),
            added: filter.filter(&self.added),
        }
    }
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

    /// `ToFieldSet` of a whole object, driven by the schema `ty` of its kind
    /// (`typed.TypedValue.ToFieldSet` over `typeconverter.ObjectToTyped`).
    pub fn of_typed(obj: &Value, ty: &Type) -> Fields {
        let mut fields = Fields::default();
        if let Some(map) = obj.as_object() {
            let mut path = Vec::new();
            fields.struct_children(&mut path, map, ty);
        }
        fields
    }

    /// The members of an object: each field with the type the schema gives it.
    fn struct_children(&mut self, path: &mut Vec<String>, map: &Map<String, Value>, ty: &Type) {
        for (k, child) in map {
            self.typed_field(path, k, child, ty.field(k));
        }
    }

    /// One field: a typed walk where the schema has a type for it, the
    /// name-driven walk where it has none.
    fn typed_field(&mut self, path: &mut Vec<String>, name: &str, v: &Value, ty: Option<&Type>) {
        match ty {
            None | Some(Type::Legacy) => self.field(path, name, v),
            Some(t) => {
                if v.is_null() {
                    return;
                }
                path.push(format!("f:{name}"));
                self.typed(path, v, t);
                path.pop();
            }
        }
    }

    /// `v`, at `path`, as a value of type `ty` (the `toFieldSetWalker`'s
    /// `doScalar` / `doList` / `doMap`, typed/toset.go).
    fn typed(&mut self, path: &mut Vec<String>, v: &Value, ty: &Type) {
        if v.is_null() {
            return;
        }
        match (ty, v) {
            (Type::Scalar | Type::Legacy, _) => self.leaf(path, v),
            (_, Value::Object(m)) if m.is_empty() => self.leaf(path, v),
            (Type::Struct(s), Value::Object(m)) => {
                if s.atomic {
                    self.leaf(path, v);
                } else {
                    self.struct_children(path, m, ty);
                }
            }
            (Type::Map { elem, atomic }, Value::Object(m)) => {
                if *atomic {
                    self.leaf(path, v);
                } else {
                    self.set.insert(path);
                    for (k, child) in m {
                        self.typed_field(path, k, child, Some(elem));
                    }
                }
            }
            (Type::Deduced, Value::Object(m)) => self.struct_children(path, m, ty),
            (Type::List { elem, kind }, Value::Array(items)) => match kind {
                ListKind::Map(keys) if !items.is_empty() => {
                    let elements: Option<Vec<String>> = items
                        .iter()
                        .map(|item| typed_key_element(keys, item))
                        .collect();
                    match elements {
                        Some(elements) => {
                            for (element, item) in elements.into_iter().zip(items) {
                                path.push(element);
                                self.set.insert(path);
                                if let Some(m) = item.as_object() {
                                    self.struct_children(path, m, elem);
                                }
                                path.pop();
                            }
                        }
                        None => self.leaf(path, v),
                    }
                }
                ListKind::Set if items.iter().all(|i| !i.is_object() && !i.is_array()) => {
                    for item in items {
                        path.push(format!("v:{item}"));
                        self.leaf(path, item);
                        path.pop();
                    }
                }
                _ => self.leaf(path, v),
            },
            // A value that does not have the shape of its type is one leaf.
            _ => self.leaf(path, v),
        }
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
            Value::Object(_) if field_kind(name) == FieldKind::Atomic => self.leaf(path, v),
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
pub fn key_element(keys: &[&str], item: &Value) -> Option<String> {
    let obj = item.as_object()?;
    let mut sorted = BTreeMap::new();
    for key in keys {
        sorted.insert(*key, obj.get(*key).filter(|v| !v.is_null())?);
    }
    Some(format!("k:{}", serde_json::to_string(&sorted).ok()?))
}

/// `k:{...}` for an item of a `x-kubernetes-list-type: map` list: every key
/// field, from the item or else from the schema default, as a sorted JSON
/// object (`typed.listItemToPathElement`, typed/helpers.go). `None` when a key
/// has neither.
pub fn typed_key_element(keys: &[(String, Option<Value>)], item: &Value) -> Option<String> {
    let obj = item.as_object()?;
    let mut sorted = BTreeMap::new();
    for (key, default) in keys {
        let value = obj.get(key).filter(|v| !v.is_null()).or(default.as_ref())?;
        sorted.insert(key.as_str(), value);
    }
    Some(format!("k:{}", serde_json::to_string(&sorted).ok()?))
}

/// A path of plain field names, as `fieldpath.MakePathOrDie`.
pub fn field_path(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| format!("f:{n}")).collect()
}
