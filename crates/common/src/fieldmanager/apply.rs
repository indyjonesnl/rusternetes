//! The field manager's Apply path (#2736).
//!
//! Port of:
//!
//! - `sigs.k8s.io/structured-merge-diff/v6/merge/update.go` -- `Updater.Apply`
//!   (:209-254), `Updater.prune` (:256-282), `addBackOwnedItemsForVersion`
//!   (:313-337), `addBackDanglingItems` (:345-372) and the conflict half of
//!   `Updater.update` (:73-164, shared with the Update path).
//! - `internal/structuredmerge.go` -- `structuredMergeManager.Apply`
//!   (:107-176).
//! - `internal/managedfieldsupdater.go` -- `Apply` (:78-92): the entry is
//!   timestamped only when the object changed.
//! - `internal/buildmanagerinfo.go` -- `Apply` (:55-61) builds the `Apply`
//!   identifier; `internal/stripmeta.go` -- `Apply` (:66-74) strips the
//!   meta fields from the applier's entry; `internal/skipnonapplied.go` --
//!   `Apply` (:76-92) gives an object with no managers a `before-first-apply`
//!   Update entry; `internal/conflict.go` -- `NewConflictError`.
//!
//! DEVIATIONS, deliberate: (1) the sets come from the kind's schema where the
//! caller supplies one (#2734), else [`fieldset`]'s name-driven walker; (2) the
//! per-manager `versionConverter` is [`super::versionconverter`] when the
//! caller supplies a converter, the identity otherwise (#2736 part 1);
//! (3) for a kind with no converter `prune` is the net effect of `prune` +
//! `addBackOwnedItems` + `addBackDanglingItems` -- the fields the applier
//! owned last time, and no one owns now, are removed -- rather than three
//! passes over typed values; with a converter it is
//! [`FieldManager::prune_versioned`] (no `addBackDanglingItems`);
//! (4) `reconcileManagedFieldsWithSchemaChanges` needs the schema and is
//! skipped; (5) the live object's `managedFields` that do not decode (written
//! by the former engine) are treated as absent instead of failing the apply.

use chrono::{SubsecRound, Utc};
use serde_json::{Map, Value};

use super::fieldset::{field_kind, key_element, typed_key_element, FieldKind, Set};
use super::schema::{ListKind, Type};
use super::versionconverter::UpdateFailure;
use super::{
    build_manager_identifier, remove_managed_fields, strip_set, FieldManager, Managed,
    VersionedSet, OPERATION_APPLY,
};

/// `skipNonAppliedManager.beforeApplyManagerName` (skipnonapplied.go:47).
const BEFORE_FIRST_APPLY: &str = "before-first-apply";

/// The system-owned metadata is the live object's, whatever the applied
/// configuration says (the registry strategy's `PrepareForUpdate` /
/// `BeforeCreate` own these on a real server). Shared by the api-server's
/// apply and the direct storage backends' apply (#3292).
pub fn retain_live_system_metadata(applied: &mut Value, live: Option<&Value>) {
    if let (Some(meta), Some(live)) = (
        applied.get_mut("metadata").and_then(Value::as_object_mut),
        live.and_then(|c| c.get("metadata"))
            .and_then(Value::as_object),
    ) {
        for key in [
            "uid",
            "resourceVersion",
            "generation",
            "creationTimestamp",
            "deletionTimestamp",
            "deletionGracePeriodSeconds",
        ] {
            match live.get(key) {
                Some(v) => {
                    meta.insert(key.to_string(), v.clone());
                }
                None => {
                    meta.remove(key);
                }
            }
        }
    }
}

/// One `merge.Conflict`: a path owned by another manager.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyConflict {
    /// The owner's manager name (`printManager`).
    pub manager: String,
    /// `Path.String()`, e.g. `.spec.holderIdentity`.
    pub path: String,
}

impl FieldManager {
    /// `FieldManager.Apply` (fieldmanager.go:183-209) over JSON. `live` is
    /// `None` when the apply creates the object; `config` is the applied
    /// configuration. Returns the merged object with its new `managedFields`,
    /// or the conflicts when `force` is false.
    pub fn apply_value(
        &self,
        live: Option<&Value>,
        config: &Value,
        manager: &str,
        force: bool,
    ) -> Result<Value, UpdateFailure> {
        let mut live_json = live.cloned().unwrap_or_else(|| Value::Object(Map::new()));
        let managed = live
            .and_then(|l| l.pointer("/metadata/managedFields"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .and_then(|e: Vec<_>| Managed::decode(&e).ok())
            .unwrap_or_default();
        remove_managed_fields(&mut live_json);
        // structuredMergeManager.Apply (structuredmerge.go:123-126): the live
        // object is converted to the version being applied.
        if live.is_some() && self.converter.is_some() {
            live_json = self
                .convert_to(&live_json, &self.api_version)
                .map_err(|e| {
                    UpdateFailure::Convert(format!(
                        "failed to convert live object to proper version: {e}"
                    ))
                })?;
        }
        let mut config = config.clone();
        remove_managed_fields(&mut config);

        let (mut object, managed) =
            self.apply_managed(&live_json, live.is_some(), &config, managed, manager, force)?;
        if let Some(meta) = object.as_object_mut().and_then(|o| {
            o.entry("metadata")
                .or_insert_with(|| Value::Object(Map::new()))
                .as_object_mut()
        }) {
            match managed.encode() {
                Some(entries) => {
                    meta.insert(
                        "managedFields".to_string(),
                        serde_json::to_value(entries).unwrap_or(Value::Null),
                    );
                }
                None => {
                    meta.remove("managedFields");
                }
            }
        }
        Ok(object)
    }

    /// The managers below `FieldManager.Apply`, outermost first.
    fn apply_managed(
        &self,
        live: &Value,
        live_exists: bool,
        config: &Value,
        mut managed: Managed,
        manager: &str,
        force: bool,
    ) -> Result<(Value, Managed), UpdateFailure> {
        // skipNonAppliedManager.Apply: an object with no managers first gets
        // an Update entry for the fields it already has.
        if managed.fields.is_empty() && live_exists {
            managed = self
                .update_managed(
                    &Value::Object(Map::new()),
                    live,
                    managed,
                    BEFORE_FIRST_APPLY,
                    true,
                )
                .map_err(UpdateFailure::Convert)?;
        }

        // buildManagerInfoManager.Apply.
        let manager_name = if manager.is_empty() {
            "unknown"
        } else {
            manager
        };
        let id = build_manager_identifier(
            manager_name,
            OPERATION_APPLY,
            &self.api_version,
            &self.subresource,
        );

        // Updater.Apply (update.go:209-254).
        let mut merged = self.merge(live, config);
        let last = managed
            .fields
            .get(&id)
            .map(|vs| (vs.set.clone(), vs.api_version.clone()));
        let mut set = self.fields_of(config).set;
        let exclude = self.reset_set();
        if !exclude.is_empty() {
            set = set.recursive_difference(&exclude);
        }
        managed.fields.insert(
            id.clone(),
            VersionedSet {
                set,
                api_version: self.api_version.clone(),
                applied: true,
            },
        );
        if let Some((last, last_version)) = last.filter(|(s, _)| !s.is_empty()) {
            if self.converter.is_some() {
                merged = self.prune_versioned(&merged, &managed, &last, &last_version)?;
            } else {
                // prune: what the applier owned last time, and no manager
                // owns now (its own new set included), goes.
                let mut owned = Set::new();
                for vs in managed.fields.values() {
                    owned = owned.union(&vs.set);
                }
                for path in last.difference(&owned).members() {
                    remove_path(&mut merged, &path);
                }
            }
        }

        // Updater.update (update.go:73-164), conflict half.
        self.update_versions(live, &merged, &mut managed, Some(&id), force)?;

        // stripMetaManager.Apply.
        if let Some(vs) = managed.fields.get_mut(&id) {
            vs.set = vs.set.difference(&strip_set());
        }
        if managed.fields.get(&id).is_some_and(|vs| vs.set.is_empty()) {
            managed.fields.remove(&id);
        }

        // managedFieldsUpdater.Apply: the time moves only when the object
        // changed (`newObject == nil` on a no-op, update.go:246-248).
        if &merged != live {
            managed.times.insert(id, Some(Utc::now().trunc_subsecs(0)));
            Ok((merged, managed))
        } else {
            Ok((live.clone(), managed))
        }
    }
}

impl FieldManager {
    /// `TypedValue.Merge` over the kind's schema, or the name table without one.
    pub(crate) fn merge(&self, live: &Value, config: &Value) -> Value {
        match &self.schema {
            Some(t) => merge_typed(live, config, t),
            None => merge_values(live, config, ""),
        }
    }
}

/// The merge of two values of one field: typed when the schema has a type for
/// it, by name otherwise.
fn merge_field(live: &Value, config: &Value, name: &str, ty: Option<&Type>) -> Value {
    match ty {
        None | Some(Type::Legacy) => merge_values(live, config, name),
        Some(t) => merge_typed(live, config, t),
    }
}

/// `TypedValue.Merge` (typed/merge.go) driven by the schema: structs and
/// granular maps merge field by field, `x-kubernetes-list-type: map` lists by
/// item key, sets by union; scalars, atomic structs and atomic lists are
/// replaced by the config.
pub fn merge_typed(live: &Value, config: &Value, ty: &Type) -> Value {
    match (live, config, ty) {
        (Value::Object(_), Value::Object(_), Type::Struct(s)) if s.atomic => config.clone(),
        (Value::Object(_), Value::Object(_), Type::Map { atomic: true, .. }) => config.clone(),
        (
            Value::Object(l),
            Value::Object(c),
            Type::Struct(_) | Type::Map { .. } | Type::Deduced,
        ) => {
            let mut out = l.clone();
            for (k, cv) in c {
                if cv.is_null() {
                    out.remove(k);
                    continue;
                }
                let merged = match l.get(k) {
                    Some(lv) => merge_field(lv, cv, k, ty.field(k)),
                    None => cv.clone(),
                };
                out.insert(k.clone(), merged);
            }
            Value::Object(out)
        }
        (Value::Array(l), Value::Array(c), Type::List { elem, kind }) => match kind {
            ListKind::Map(keys) => merge_keyed(
                l,
                c,
                |i| typed_key_element(keys, i),
                |a, b| merge_typed(a, b, elem),
            )
            .unwrap_or_else(|| config.clone()),
            ListKind::Set if c.iter().chain(l).all(|i| !i.is_object() && !i.is_array()) => {
                union_scalars(l, c)
            }
            _ => config.clone(),
        },
        // Legacy: no schema for this node.
        (_, _, Type::Legacy) => merge_values(live, config, ""),
        _ => config.clone(),
    }
}

fn union_scalars(l: &[Value], c: &[Value]) -> Value {
    let mut out = l.to_vec();
    for item in c {
        if !out.contains(item) {
            out.push(item.clone());
        }
    }
    Value::Array(out)
}

/// Merge two associative lists by item key; `None` when an item has no key
/// (the list is then replaced). A live list with duplicate keys is
/// consolidated into one merged entry (smd_test.go: "apply configuration
/// modify existing env variable").
fn merge_keyed(
    l: &[Value],
    c: &[Value],
    key: impl Fn(&Value) -> Option<String>,
    merge_item: impl Fn(&Value, &Value) -> Value,
) -> Option<Value> {
    let lk: Vec<String> = l.iter().map(&key).collect::<Option<_>>()?;
    let ck: Vec<String> = c.iter().map(&key).collect::<Option<_>>()?;
    let mut out: Vec<Value> = Vec::with_capacity(l.len());
    let mut merged_keys: Vec<&String> = Vec::new();
    for (item, k) in l.iter().zip(&lk) {
        match ck.iter().position(|x| x == k) {
            Some(_) if merged_keys.contains(&k) => {}
            Some(i) => {
                merged_keys.push(k);
                out.push(merge_item(item, &c[i]));
            }
            None => out.push(item.clone()),
        }
    }
    for (item, k) in c.iter().zip(&ck) {
        if !lk.contains(k) {
            out.push(item.clone());
        }
    }
    Some(Value::Array(out))
}

/// `TypedValue.Merge` (typed/merge.go): maps merge key by key, associative
/// lists by item key, sets by union; scalars and atomic lists are replaced by
/// the config. Name-driven: the shapes come from [`field_kind`].
pub fn merge_values(live: &Value, config: &Value, name: &str) -> Value {
    match (live, config) {
        (Value::Object(_), Value::Object(_)) if field_kind(name) == FieldKind::Atomic => {
            config.clone()
        }
        (Value::Object(l), Value::Object(c)) => {
            let mut out = l.clone();
            for (k, cv) in c {
                if cv.is_null() {
                    out.remove(k);
                    continue;
                }
                let merged = match l.get(k) {
                    Some(lv) => merge_values(lv, cv, k),
                    None => cv.clone(),
                };
                out.insert(k.clone(), merged);
            }
            Value::Object(out)
        }
        (Value::Array(l), Value::Array(c)) => match field_kind(name) {
            FieldKind::KeyedList(keys) => merge_keyed(
                l,
                c,
                |i| key_element(keys, i),
                |a, b| merge_values(a, b, ""),
            )
            .unwrap_or_else(|| config.clone()),
            FieldKind::ScalarSet if c.iter().chain(l).all(|i| !i.is_object() && !i.is_array()) => {
                union_scalars(l, c)
            }
            _ => config.clone(),
        },
        _ => config.clone(),
    }
}

/// `RemoveItems` for one path element chain: drop the field, list item or
/// set value the path names.
pub fn remove_path(value: &mut Value, path: &[String]) {
    let Some((first, rest)) = path.split_first() else {
        return;
    };
    if let Some(name) = first.strip_prefix("f:") {
        let Some(map) = value.as_object_mut() else {
            return;
        };
        if rest.is_empty() {
            map.remove(name);
        } else if let Some(child) = map.get_mut(name) {
            remove_path(child, rest);
        }
    } else if let Some(key) = first.strip_prefix("k:") {
        let Some(want) = serde_json::from_str::<Map<String, Value>>(key).ok() else {
            return;
        };
        let Some(items) = value.as_array_mut() else {
            return;
        };
        let matches = |item: &Value| want.iter().all(|(k, v)| item.get(k) == Some(v));
        if rest.is_empty() {
            items.retain(|i| !matches(i));
        } else if let Some(item) = items.iter_mut().find(|i| matches(i)) {
            remove_path(item, rest);
        }
    } else if let Some(v) = first.strip_prefix("v:") {
        let Some(want) = serde_json::from_str::<Value>(v).ok() else {
            return;
        };
        if let (true, Some(items)) = (rest.is_empty(), value.as_array_mut()) {
            items.retain(|i| *i != want);
        }
    }
}

/// `Path.String()` (fieldpath/path.go): `.spec.containers[name="c"]`.
pub(super) fn path_string(path: &[String]) -> String {
    let mut out = String::new();
    for element in path {
        if let Some(name) = element.strip_prefix("f:") {
            out.push('.');
            out.push_str(name);
        } else if let Some(key) = element.strip_prefix("k:") {
            let pairs = serde_json::from_str::<Map<String, Value>>(key).unwrap_or_default();
            let inner: Vec<String> = pairs.iter().map(|(k, v)| format!("{k}={v}")).collect();
            out.push_str(&format!("[{}]", inner.join(",")));
        } else if let Some(v) = element.strip_prefix("v:") {
            out.push_str(&format!("[={v}]"));
        } else if let Some(i) = element.strip_prefix("i:") {
            out.push_str(&format!("[{i}]"));
        }
    }
    out
}
