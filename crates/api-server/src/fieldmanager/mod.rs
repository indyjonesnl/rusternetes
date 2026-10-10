//! The field manager's Update path: the `managedFields` entry that a create,
//! an update or a non-apply patch records for the requester (#2701).
//!
//! Port of `staging/src/k8s.io/apimachinery/pkg/util/managedfields/`:
//!
//! - `internal/fieldmanager.go` -- `FieldManager.Update` (:94-137),
//!   `UpdateNoErrors` (:140-160), `decodeLiveOrNew` (:63-92),
//!   `isResetManagedFields`, and the layering `NewDefaultFieldManager`
//!   (:46-61) builds: version check, last-applied, skip-non-applied (with
//!   `DefaultTrackOnCreateProbability` = 1), cap-managers
//!   (`DefaultMaxUpdateManagers` = 10), build-manager-info, the
//!   managed-fields updater, strip-meta, and the structured merge.
//! - `internal/managedfields.go` -- `DecodeManagedFields`,
//!   `encodeManagedFields`, `sortEncodedManagedFields`,
//!   `BuildManagerIdentifier`.
//! - `internal/managedfieldsupdater.go`, `capmanagers.go`,
//!   `buildmanagerinfo.go`, `stripmeta.go`, `skipnonapplied.go`.
//! - `sigs.k8s.io/structured-merge-diff/v6/merge/update.go` -- `Updater.Update`
//!   and `Updater.update`.
//!
//! and of the call sites in `apiserver/pkg/endpoints/handlers`:
//! `create.go:199`, `update.go:160-167`, `patch.go:372` and `:466`, each with
//! `managerOrUserAgent` (`create.go:259-282`).
//!
//! Server-side apply is [`apply`]; this is the other half, for every write
//! that is not an apply.
//!
//! DEVIATIONS, deliberate: (1) the field sets come from [`fieldset`]'s
//! name-driven walker, not the OpenAPI schema; (2) one version per kind is
//! served here, so upstream's per-manager `versionConverter` is the identity.

pub mod apply;
pub mod fieldset;

use std::collections::BTreeMap;

use chrono::{DateTime, SubsecRound, Utc};
use rusternetes_common::types::ManagedFieldsEntry;
use rusternetes_common::validation::metav1::FIELD_MANAGER_MAX_LENGTH;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use self::fieldset::{field_path, Fields, Set};
use crate::registry::rest::Object;
use crate::ssa::ResetFields;

/// `DefaultMaxUpdateManagers` (fieldmanager.go:32-36).
pub const DEFAULT_MAX_UPDATE_MANAGERS: usize = 10;

/// The name a manager's fields are collected under while one operation runs
/// (managedfieldsupdater.go `self := "current-operation"`).
const CURRENT_OPERATION: &str = "current-operation";

/// `capManagersManager.oldUpdatesManagerName` (capmanagers.go).
const ANCIENT_CHANGES: &str = "ancient-changes";

const OPERATION_APPLY: &str = "Apply";
const OPERATION_UPDATE: &str = "Update";

tokio::task_local! {
    /// `req.UserAgent()` of the request being served, for [`manager_or_user_agent`].
    static USER_AGENT: String;
}

/// Serve the rest of the request with its `User-Agent` known to the field
/// manager (upstream hands `req.UserAgent()` to each handler).
pub async fn user_agent_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let agent = req
        .headers()
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    USER_AGENT.scope(agent, next.run(req)).await
}

/// `managerOrUserAgent` (create.go:259-264): the `fieldManager` parameter, or
/// the user agent's product name.
pub fn manager_or_user_agent(field_manager: Option<&str>) -> String {
    match field_manager {
        Some(m) if !m.is_empty() => m.to_string(),
        _ => prefix_from_user_agent(&USER_AGENT.try_with(Clone::clone).unwrap_or_default()),
    }
}

/// `prefixFromUserAgent` (create.go:266-282): the characters before the first
/// `/`, without unprintable ones, cut to `FieldManagerMaxLength` bytes.
pub fn prefix_from_user_agent(user_agent: &str) -> String {
    let product = user_agent.split('/').next().unwrap_or_default();
    let mut out = String::new();
    for c in product.chars() {
        if c.is_control() {
            continue;
        }
        if out.len() + c.len_utf8() > FIELD_MANAGER_MAX_LENGTH {
            break;
        }
        out.push(c);
    }
    out
}

/// The identity of a manager entry, `BuildManagerIdentifier`
/// (managedfields.go:134-159): the entry without its time and fields, as JSON.
#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ManagerIdentity {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    manager: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    operation: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    api_version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    subresource: String,
}

fn build_manager_identifier(
    manager: &str,
    operation: &str,
    api_version: &str,
    subresource: &str,
) -> String {
    let identity = ManagerIdentity {
        manager: manager.to_string(),
        operation: operation.to_string(),
        // For appliers the version is not part of the identity.
        api_version: if operation == OPERATION_APPLY {
            String::new()
        } else {
            api_version.to_string()
        },
        subresource: subresource.to_string(),
    };
    serde_json::to_string(&identity).unwrap_or_default()
}

/// `fieldpath.VersionedSet`.
#[derive(Debug, Clone)]
struct VersionedSet {
    set: Set,
    api_version: String,
    applied: bool,
}

/// `internal.Managed`: the sets by manager identifier, and their times.
#[derive(Debug, Default, Clone)]
pub struct Managed {
    fields: BTreeMap<String, VersionedSet>,
    times: BTreeMap<String, Option<DateTime<Utc>>>,
}

impl Managed {
    /// `DecodeManagedFields` (managedfields.go:94-132).
    fn decode(entries: &[ManagedFieldsEntry]) -> Result<Managed, String> {
        let mut managed = Managed::default();
        for (i, entry) in entries.iter().enumerate() {
            let operation = entry.operation.as_deref().unwrap_or_default();
            if operation != OPERATION_APPLY && operation != OPERATION_UPDATE {
                return Err("operation must be `Apply` or `Update`".into());
            }
            let api_version = entry.api_version.as_deref().unwrap_or_default();
            if api_version.is_empty() {
                return Err("apiVersion must not be empty".into());
            }
            match entry.fields_type.as_deref() {
                Some("FieldsV1") => {}
                None | Some("") => {
                    return Err(format!("missing fieldsType in managed fields entry {i}"))
                }
                Some(other) => {
                    return Err(format!(
                        "invalid fieldsType {other:?} in managed fields entry {i}"
                    ))
                }
            }
            let id = build_manager_identifier(
                entry.manager.as_deref().unwrap_or_default(),
                operation,
                api_version,
                entry.subresource.as_deref().unwrap_or_default(),
            );
            let set = match &entry.fields_v1 {
                None => Set::new(),
                Some(v) => Set::from_root_fields_v1(v)
                    .ok_or_else(|| "error decoding set: invalid fieldsV1".to_string())?,
            };
            managed.fields.insert(
                id.clone(),
                VersionedSet {
                    set,
                    api_version: api_version.to_string(),
                    applied: operation == OPERATION_APPLY,
                },
            );
            managed.times.insert(id, entry.time);
        }
        Ok(managed)
    }

    /// `encodeManagedFields` + `sortEncodedManagedFields`
    /// (managedfields.go:162-231).
    fn encode(&self) -> Option<Vec<ManagedFieldsEntry>> {
        if self.fields.is_empty() {
            return None;
        }
        let mut entries: Vec<ManagedFieldsEntry> = self
            .fields
            .iter()
            .map(|(id, vs)| {
                let identity: ManagerIdentity = serde_json::from_str(id).unwrap_or_default();
                ManagedFieldsEntry {
                    manager: Some(identity.manager).filter(|m| !m.is_empty()),
                    operation: Some(if vs.applied {
                        OPERATION_APPLY.to_string()
                    } else {
                        OPERATION_UPDATE.to_string()
                    }),
                    api_version: Some(vs.api_version.clone()),
                    time: self.times.get(id).copied().flatten(),
                    fields_type: Some("FieldsV1".to_string()),
                    fields_v1: Some(vs.set.to_fields_v1()),
                    subresource: Some(identity.subresource).filter(|s| !s.is_empty()),
                }
            })
            .collect();
        entries.sort_by(|p, q| {
            let seconds = |e: &ManagedFieldsEntry| e.time.map(|t| t.timestamp()).unwrap_or(0);
            p.operation
                .cmp(&q.operation)
                .then(seconds(p).cmp(&seconds(q)))
                .then(p.manager.cmp(&q.manager))
                .then(p.api_version.cmp(&q.api_version))
                .then(p.subresource.cmp(&q.subresource))
        });
        Some(entries)
    }
}

/// `isResetManagedFields` (fieldmanager.go:164-176): the request asks for the
/// entries to be dropped, with `[]` or a single empty item.
fn is_reset_managed_fields(entries: &Option<Vec<ManagedFieldsEntry>>) -> bool {
    match entries {
        None => false,
        Some(e) if e.is_empty() => true,
        Some(e) if e.len() == 1 => {
            let x = &e[0];
            x.manager.is_none()
                && x.operation.is_none()
                && x.api_version.is_none()
                && x.time.is_none()
                && x.fields_type.is_none()
                && x.fields_v1.is_none()
                && x.subresource.is_none()
        }
        Some(_) => false,
    }
}

/// `FieldManager` (fieldmanager.go:38-44) for one served resource.
pub struct FieldManager {
    /// `kind.GroupVersion().String()`.
    api_version: String,
    /// `""` for the resource itself.
    subresource: String,
    reset_fields: ResetFields,
}

impl FieldManager {
    pub fn new(api_version: &str, subresource: Option<&str>, reset_fields: ResetFields) -> Self {
        Self {
            api_version: api_version.to_string(),
            subresource: subresource.unwrap_or_default().to_string(),
            reset_fields,
        }
    }

    /// `UpdateNoErrors` (fieldmanager.go:140-160): `live` is `None` for a
    /// create. On failure the object comes back with no `managedFields`.
    pub fn update_no_errors<T: Object>(&self, live: Option<&T>, new: T, manager: &str) -> T {
        match self.update(live, new.clone(), manager) {
            Ok(obj) => obj,
            Err(e) => {
                tracing::error!("[SHOULD NOT HAPPEN] failed to update managedFields: {e}");
                let mut new = new;
                new.metadata_mut().managed_fields = None;
                new
            }
        }
    }

    /// `FieldManager.Update` (fieldmanager.go:94-137).
    pub fn update<T: Object>(
        &self,
        live: Option<&T>,
        mut new: T,
        manager: &str,
    ) -> Result<T, String> {
        // decodeLiveOrNew (fieldmanager.go:63-92). A subresource ignores the
        // managedFields of the request object: "in case the request tries to
        // manually set managedFields via a subresource".
        let live_managed = || {
            live.and_then(|l| l.metadata().managed_fields.as_ref())
                .map(|e| Managed::decode(e).unwrap_or_default())
                .unwrap_or_default()
        };
        let managed = if !self.subresource.is_empty() {
            live_managed()
        } else if is_reset_managed_fields(&new.metadata().managed_fields) {
            Managed::default()
        } else {
            match new
                .metadata()
                .managed_fields
                .as_ref()
                .map(|e| Managed::decode(e))
            {
                Some(Ok(m)) if !m.fields.is_empty() => m,
                _ => live_managed(),
            }
        };

        // RemoveObjectManagedFields(newObj).
        new.metadata_mut().managed_fields = None;

        let mut new_json = serde_json::to_value(&new).map_err(|e| e.to_string())?;
        let mut live_json = match live {
            Some(l) => serde_json::to_value(l).map_err(|e| e.to_string())?,
            None => Value::Object(Default::default()),
        };
        remove_managed_fields(&mut new_json);
        remove_managed_fields(&mut live_json);
        let live_is_new = live.is_none_or(|l| l.metadata().uid.is_empty());

        let managed = self.update_managed(&live_json, &new_json, managed, manager, live_is_new);
        new.metadata_mut().managed_fields = managed.encode();
        Ok(new)
    }

    /// The managers below `FieldManager`, outermost first.
    fn update_managed(
        &self,
        live: &Value,
        new: &Value,
        mut managed: Managed,
        manager: &str,
        live_is_new: bool,
    ) -> Managed {
        // skipNonAppliedManager.Update (skipnonapplied.go): with no entries,
        // an update is not tracked; a create is, with
        // DefaultTrackOnCreateProbability = 1.
        if managed.fields.is_empty() && !live_is_new {
            return managed;
        }

        // buildManagerInfoManager.Update (buildmanagerinfo.go).
        let manager_name = if manager.is_empty() {
            "unknown"
        } else {
            manager
        };
        let id = build_manager_identifier(
            manager_name,
            OPERATION_UPDATE,
            &self.api_version,
            &self.subresource,
        );

        // managedFieldsUpdater.Update (managedfieldsupdater.go:36-56), over
        // stripMetaManager.Update and structuredMergeManager.Update, which is
        // `merge.Updater.Update` (update.go:167-204) with `CURRENT_OPERATION`
        // as the manager.
        let exclude = self.reset_set();
        let mut comparison = Fields::of(live).compare(&Fields::of(new));
        if !exclude.is_empty() {
            comparison = comparison.exclude(&exclude);
        }
        let changed = comparison.modified.union(&comparison.added);
        // update.go:112-139: other managers lose what this operation changed
        // (force is true for an update) and what it removed.
        for vs in managed.fields.values_mut() {
            let conflicts = vs.set.intersection(&changed);
            vs.set = vs
                .set
                .difference(&conflicts)
                .difference(&comparison.removed);
        }
        managed.fields.retain(|_, vs| !vs.set.is_empty());

        let mut current = changed;
        if !exclude.is_empty() {
            current = current.recursive_difference(&exclude);
        }
        // stripMetaManager.stripFields (stripmeta.go:44-56, 87-101).
        current = current.difference(&strip_set());

        // If the operation took any field, the object changed: stamp the entry
        // and merge with the manager's earlier updates (managedfieldsupdater.go
        // :45-55).
        if !current.is_empty() {
            let merged = match managed.fields.get(&id) {
                Some(previous) => current.union(&previous.set),
                None => current,
            };
            managed.fields.insert(
                id.clone(),
                VersionedSet {
                    set: merged,
                    api_version: self.api_version.clone(),
                    applied: false,
                },
            );
            managed.times.insert(id, Some(Utc::now().trunc_subsecs(0)));
        }

        // capManagersManager.Update.
        cap_update_managers(&mut managed, DEFAULT_MAX_UPDATE_MANAGERS);
        managed
    }

    /// The strategy's reset fields for this version, as a set.
    fn reset_set(&self) -> Set {
        let mut set = Set::new();
        for path in self.reset_fields.for_version(&self.api_version) {
            let names: Vec<&str> = path.iter().map(String::as_str).collect();
            set.insert(&field_path(&names));
        }
        set
    }
}

/// `stripMetaManager.stripSet` (stripmeta.go:36-52).
fn strip_set() -> Set {
    let mut set = Set::new();
    for path in [
        &["apiVersion"][..],
        &["kind"],
        &["metadata"],
        &["metadata", "name"],
        &["metadata", "namespace"],
        &["metadata", "creationTimestamp"],
        &["metadata", "selfLink"],
        &["metadata", "uid"],
        &["metadata", "clusterName"],
        &["metadata", "generation"],
        &["metadata", "managedFields"],
        &["metadata", "resourceVersion"],
    ] {
        set.insert(&field_path(path));
    }
    set
}

fn remove_managed_fields(obj: &mut Value) {
    if let Some(meta) = obj.get_mut("metadata").and_then(Value::as_object_mut) {
        meta.remove("managedFields");
    }
}

/// `capManagersManager.capUpdateManagers` (capmanagers.go:77-142): when more
/// than `max` Update entries exist, the oldest are merged into one
/// `ancient-changes` entry per API version.
fn cap_update_managers(managed: &mut Managed, max: usize) {
    let mut updaters: Vec<String> = managed
        .fields
        .iter()
        .filter(|(_, vs)| !vs.applied)
        .map(|(id, _)| id.clone())
        .collect();
    if updaters.len() <= max {
        return;
    }
    let seconds = |managed: &Managed, id: &str| {
        managed
            .times
            .get(id)
            .copied()
            .flatten()
            .map(|t| t.timestamp())
            .unwrap_or(0)
    };
    updaters.sort_by(|i, j| seconds(managed, i).cmp(&seconds(managed, j)).then(i.cmp(j)));

    let mut version_to_first: BTreeMap<String, String> = BTreeMap::new();
    let mut length = updaters.len();
    for manager in &updaters {
        if length <= max {
            break;
        }
        let Some(vs) = managed.fields.get(manager).cloned() else {
            continue;
        };
        let time = managed.times.get(manager).copied().flatten();
        let version = vs.api_version.clone();
        let bucket = build_manager_identifier(ANCIENT_CHANGES, OPERATION_UPDATE, &version, "");
        match version_to_first.get(&version) {
            Some(first) => {
                if !managed.fields.contains_key(&bucket) {
                    if let Some(s) = managed.fields.remove(first) {
                        managed.fields.insert(bucket.clone(), s);
                    }
                }
                let existing = managed
                    .fields
                    .get(&bucket)
                    .map(|b| b.set.clone())
                    .unwrap_or_default();
                managed.fields.insert(
                    bucket.clone(),
                    VersionedSet {
                        set: vs.set.union(&existing),
                        api_version: vs.api_version.clone(),
                        applied: vs.applied,
                    },
                );
                managed.fields.remove(manager);
                length -= 1;
                // The time of the update merged in is the more recent one.
                managed.times.insert(bucket, time);
            }
            None => {
                version_to_first.insert(version, manager.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests;
