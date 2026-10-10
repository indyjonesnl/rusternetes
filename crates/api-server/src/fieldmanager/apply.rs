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
//! DEVIATIONS, deliberate: (1) the sets come from [`fieldset`]'s name-driven
//! walker, not the OpenAPI schema (#2734); (2) one version per kind is served,
//! so the per-manager `versionConverter` is the identity (#2736 part 1);
//! (3) `prune` is the net effect of `prune` + `addBackOwnedItems` +
//! `addBackDanglingItems` -- the fields the applier owned last time, and no
//! one owns now, are removed -- rather than three passes over typed values;
//! (4) `reconcileManagedFieldsWithSchemaChanges` needs the schema and is
//! skipped; (5) the live object's `managedFields` that do not decode (written
//! by the former engine) are treated as absent instead of failing the apply.

use chrono::{SubsecRound, Utc};
use serde_json::{Map, Value};

use super::fieldset::{field_kind, key_element, FieldKind, Fields, Set};
use super::{
    build_manager_identifier, remove_managed_fields, strip_set, FieldManager, Managed,
    ManagerIdentity, VersionedSet, OPERATION_APPLY,
};

/// `skipNonAppliedManager.beforeApplyManagerName` (skipnonapplied.go:47).
const BEFORE_FIRST_APPLY: &str = "before-first-apply";

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
    ) -> Result<Value, Vec<ApplyConflict>> {
        let mut live_json = live.cloned().unwrap_or_else(|| Value::Object(Map::new()));
        let managed = live
            .and_then(|l| l.pointer("/metadata/managedFields"))
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .and_then(|e: Vec<_>| Managed::decode(&e).ok())
            .unwrap_or_default();
        remove_managed_fields(&mut live_json);
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
    ) -> Result<(Value, Managed), Vec<ApplyConflict>> {
        // skipNonAppliedManager.Apply: an object with no managers first gets
        // an Update entry for the fields it already has.
        if managed.fields.is_empty() && live_exists {
            managed = self.update_managed(
                &Value::Object(Map::new()),
                live,
                managed,
                BEFORE_FIRST_APPLY,
                true,
            );
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
        let mut merged = merge_values(live, config, "");
        let last_set = managed.fields.get(&id).map(|vs| vs.set.clone());
        let mut set = Fields::of(config).set;
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
        if let Some(last) = last_set.filter(|s| !s.is_empty()) {
            // prune: what the applier owned last time, and no manager owns
            // now (its own new set included), goes.
            let mut owned = Set::new();
            for vs in managed.fields.values() {
                owned = owned.union(&vs.set);
            }
            for path in last.difference(&owned).members() {
                remove_path(&mut merged, &path);
            }
        }

        // Updater.update (update.go:73-164), conflict half.
        let mut comparison = Fields::of(live).compare(&Fields::of(&merged));
        if !exclude.is_empty() {
            comparison = comparison.exclude(&exclude);
        }
        let changed = comparison.modified.union(&comparison.added);
        let mut conflicts = Vec::new();
        for (other, vs) in &managed.fields {
            if *other == id {
                continue;
            }
            let conflict_set = vs.set.intersection(&changed);
            if conflict_set.is_empty() {
                continue;
            }
            let owner: ManagerIdentity = serde_json::from_str(other).unwrap_or_default();
            for path in conflict_set.members() {
                conflicts.push(ApplyConflict {
                    manager: owner.manager.clone(),
                    path: path_string(&path),
                });
            }
        }
        if !force && !conflicts.is_empty() {
            conflicts.sort_by(|a, b| a.manager.cmp(&b.manager));
            return Err(conflicts);
        }
        for (other, vs) in managed.fields.iter_mut() {
            if *other == id {
                continue;
            }
            let conflict_set = vs.set.intersection(&changed);
            vs.set = vs
                .set
                .difference(&conflict_set)
                .difference(&comparison.removed);
        }
        managed.fields.retain(|_, vs| !vs.set.is_empty());

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

/// `TypedValue.Merge` (typed/merge.go): maps merge key by key, associative
/// lists by item key, sets by union; scalars and atomic lists are replaced by
/// the config.
fn merge_values(live: &Value, config: &Value, name: &str) -> Value {
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
            FieldKind::KeyedList(keys) => {
                let lk: Option<Vec<String>> = l.iter().map(|i| key_element(keys, i)).collect();
                let ck: Option<Vec<String>> = c.iter().map(|i| key_element(keys, i)).collect();
                match (lk, ck) {
                    (Some(lk), Some(ck)) => {
                        let mut out: Vec<Value> = Vec::with_capacity(l.len());
                        for (item, key) in l.iter().zip(&lk) {
                            match ck.iter().position(|k| k == key) {
                                Some(i) => out.push(merge_values(item, &c[i], "")),
                                None => out.push(item.clone()),
                            }
                        }
                        for (item, key) in c.iter().zip(&ck) {
                            if !lk.contains(key) {
                                out.push(item.clone());
                            }
                        }
                        Value::Array(out)
                    }
                    _ => config.clone(),
                }
            }
            FieldKind::ScalarSet if c.iter().chain(l).all(|i| !i.is_object() && !i.is_array()) => {
                let mut out = l.clone();
                for item in c {
                    if !out.contains(item) {
                        out.push(item.clone());
                    }
                }
                Value::Array(out)
            }
            _ => config.clone(),
        },
        _ => config.clone(),
    }
}

/// `RemoveItems` for one path element chain: drop the field, list item or
/// set value the path names.
fn remove_path(value: &mut Value, path: &[String]) {
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
fn path_string(path: &[String]) -> String {
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
