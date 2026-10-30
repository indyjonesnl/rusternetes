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
//! DEVIATIONS, deliberate: (1) the field sets come from the kind's OpenAPI /
//! structural schema ([`schema`], #2734) where the caller supplies one
//! ([`FieldManager::with_schema`]), and from [`fieldset`]'s name-driven table
//! for a kind with none; (2) the per-manager `versionConverter`
//! ([`versionconverter`], #2736) is used when the caller supplies a
//! [`Converter`] ([`FieldManager::with_converter`]); a kind served at one
//! version has none and every manager's set is compared at the served
//! version, as upstream's identity conversion does.

pub mod apply;
pub mod fieldset;
pub mod schema;
pub mod versionconverter;

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::types::ManagedFieldsEntry;
use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use self::fieldset::{field_path, Fields, Set};
use self::versionconverter::{Converter, UpdateFailure};

/// `map[fieldpath.APIVersion]*fieldpath.Set`: what a strategy resets on write,
/// per `group/version` (`rest.ResetFieldsStrategy.GetResetFields`,
/// apiserver/pkg/registry/rest/rest.go:385-389). Each set is a list of field
/// paths (`fieldpath.MakePathOrDie("status")` is `["status"]`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResetFields(BTreeMap<String, Vec<Vec<String>>>);

impl ResetFields {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add the set reset in `api_version` (`"networking.k8s.io/v1"`).
    pub fn with(mut self, api_version: &str, paths: &[&[&str]]) -> Self {
        self.0.insert(
            api_version.to_string(),
            paths
                .iter()
                .map(|p| p.iter().map(|s| s.to_string()).collect())
                .collect(),
        );
        self
    }

    /// `GetResetFields()[version]`: empty when the version has no entry.
    pub fn for_version(&self, api_version: &str) -> &[Vec<String>] {
        self.0.get(api_version).map(Vec::as_slice).unwrap_or(&[])
    }
}

/// `DefaultMaxUpdateManagers` (fieldmanager.go:32-36).
pub const DEFAULT_MAX_UPDATE_MANAGERS: usize = 10;

/// `capManagersManager.oldUpdatesManagerName` (capmanagers.go).
const ANCIENT_CHANGES: &str = "ancient-changes";

pub const OPERATION_APPLY: &str = "Apply";
pub const OPERATION_UPDATE: &str = "Update";

/// The identity of a manager entry, `BuildManagerIdentifier`
/// (managedfields.go:134-159): the entry without its time and fields, as JSON.
#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ManagerIdentity {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    manager: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    operation: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    api_version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    subresource: String,
}

pub fn build_manager_identifier(
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
pub struct VersionedSet {
    pub set: Set,
    pub api_version: String,
    pub applied: bool,
}

/// `internal.Managed`: the sets by manager identifier, and their times.
#[derive(Debug, Default, Clone)]
pub struct Managed {
    pub fields: BTreeMap<String, VersionedSet>,
    pub times: BTreeMap<String, Option<DateTime<Utc>>>,
}

impl Managed {
    /// `DecodeManagedFields` (managedfields.go:94-132).
    pub fn decode(entries: &[ManagedFieldsEntry]) -> Result<Managed, String> {
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
    pub fn encode(&self) -> Option<Vec<ManagedFieldsEntry>> {
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
pub fn is_reset_managed_fields(entries: &Option<Vec<ManagedFieldsEntry>>) -> bool {
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
    /// The schema of the kind (`typeconverter`), when this tree has one.
    schema: Option<Arc<schema::Type>>,
    /// `merge.Updater.Converter`: converts the objects to a manager's version.
    converter: Option<Arc<dyn Converter>>,
}

impl FieldManager {
    /// The subresource this manager serves (`""` for the resource itself).
    pub fn subresource(&self) -> &str {
        &self.subresource
    }

    pub fn new(api_version: &str, subresource: Option<&str>, reset_fields: ResetFields) -> Self {
        Self {
            api_version: api_version.to_string(),
            subresource: subresource.unwrap_or_default().to_string(),
            reset_fields,
            schema: None,
            converter: None,
        }
    }

    /// Drive the field sets and the merge by `schema`
    /// (`NewDefaultFieldManager`'s `typeConverter` argument). Without one the
    /// name-driven table answers.
    pub fn with_schema(mut self, schema: Option<Arc<schema::Type>>) -> Self {
        self.schema = schema;
        self
    }

    /// The `Converter` of `merge.Updater` (`newVersionConverter` /
    /// `newCRDVersionConverter`), for a kind served at several versions.
    pub fn with_converter(mut self, converter: Option<Arc<dyn Converter>>) -> Self {
        self.converter = converter;
        self
    }

    /// `typeconverter.ObjectToTyped(obj).ToFieldSet()`.
    pub(crate) fn fields_of(&self, obj: &Value) -> Fields {
        match &self.schema {
            Some(t) => Fields::of_typed(obj, t),
            None => Fields::of(obj),
        }
    }

    /// The managers below `FieldManager`, outermost first. Fails when a
    /// manager's version cannot be converted (`Updater.update`'s
    /// `failed to convert old object`), which `UpdateNoErrors` absorbs.
    pub fn update_managed(
        &self,
        live: &Value,
        new: &Value,
        mut managed: Managed,
        manager: &str,
        live_is_new: bool,
    ) -> Result<Managed, String> {
        // skipNonAppliedManager.Update (skipnonapplied.go): with no entries,
        // an update is not tracked; a create is, with
        // DefaultTrackOnCreateProbability = 1.
        if managed.fields.is_empty() && !live_is_new {
            return Ok(managed);
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
        // `merge.Updater.Update` (update.go:167-204) with the name "current-operation"
        // as the manager, so no entry is skipped.
        let exclude = self.reset_set();
        // update.go:112-139: other managers lose what this operation changed
        // (force is true for an update) and what it removed, each compared at
        // its own version.
        let comparison = self
            .update_versions(live, new, &mut managed, None, true)
            .map_err(|e| match e {
                UpdateFailure::Convert(m) => m,
                UpdateFailure::Conflicts(_) => "unexpected conflicts on update".to_string(),
            })?;
        let changed = comparison.modified.union(&comparison.added);

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
        Ok(managed)
    }

    /// The strategy's reset fields for this version, as a set.
    fn reset_set(&self) -> Set {
        self.reset_set_for(&self.api_version)
    }

    /// `GetResetFields()[version]` as a set.
    pub(crate) fn reset_set_for(&self, version: &str) -> Set {
        let mut set = Set::new();
        for path in self.reset_fields.for_version(version) {
            let names: Vec<&str> = path.iter().map(String::as_str).collect();
            set.insert(&field_path(&names));
        }
        set
    }
}

/// `stripMetaManager.stripSet` (stripmeta.go:36-52).
pub fn strip_set() -> Set {
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

pub fn remove_managed_fields(obj: &mut Value) {
    if let Some(meta) = obj.get_mut("metadata").and_then(Value::as_object_mut) {
        meta.remove("managedFields");
    }
}

/// `capManagersManager.capUpdateManagers` (capmanagers.go:77-142): when more
/// than `max` Update entries exist, the oldest are merged into one
/// `ancient-changes` entry per API version.
pub fn cap_update_managers(managed: &mut Managed, max: usize) {
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
