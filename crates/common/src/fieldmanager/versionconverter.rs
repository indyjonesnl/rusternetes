//! Per-manager version conversion (#2736 part 1).
//!
//! Port of:
//!
//! - `staging/src/k8s.io/apimachinery/pkg/util/managedfields/internal/
//!   versionconverter.go` -- `versionConverter` (`newVersionConverter`,
//!   `newCRDVersionConverter`, `Convert`, `IsMissingVersionError`) and
//!   `noCorrespondingTypeErr`.
//! - `sigs.k8s.io/structured-merge-diff/v6/merge/update.go` -- the `Converter`
//!   interface (:28-31), the per-manager loop of `Updater.update` (:73-164),
//!   `Updater.prune` (:256-282), `addBackOwnedItems` (:284-311) and
//!   `addBackOwnedItemsForVersion` (:313-337).
//! - `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/conversion/
//!   nop_converter.go` -- `nopConverter`, the CRD `conversion.strategy: None`
//!   converter that only rewrites `apiVersion`.
//!
//! A manager's `FieldsV1` is recorded at the `apiVersion` its entry carries.
//! When a write arrives at another version, the old and the new object are
//! converted to the manager's version before they are compared, so the field
//! set the manager owns is compared against the paths of its own version
//! (`update.go:101-139`). A version the converter does not know drops the
//! manager's entry (`IsMissingVersionError`, :107-111, :117-121).
//!
//! DEVIATIONS, deliberate: (1) the schema that drives the field sets is the
//! one the [`super::FieldManager`] was built with, for every version;
//! upstream's `typeConverter` picks the schema by the converted object's
//! GVK, so a CRD whose versions have different schemas is walked with the
//! served version's. (2) `addBackDanglingItems` (update.go:345-372) is not
//! ported: the cross-version `prune` below removes what the applier owned and
//! adds back what is owned, but does not restore an item kept alive only by
//! fields that were never owned.

use std::sync::Arc;

use serde_json::{Map, Value};

use super::apply::remove_path;
use super::fieldset::Set;

/// What a conversion fails with: `runtime.IsNotRegisteredError` /
/// `noCorrespondingTypeErr` (the version is unknown) or anything else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConvertError {
    /// `IsMissingVersionError` is true: the version has no corresponding type.
    MissingVersion(String),
    Other(String),
}

impl std::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // `noCorrespondingTypeErr.Error()` (versionconverter.go:97-99).
            ConvertError::MissingVersion(gvk) => write!(f, "no corresponding type for {gvk}"),
            ConvertError::Other(e) => f.write_str(e),
        }
    }
}

/// `merge.Converter` (update.go:28-31).
pub trait Converter: Send + Sync + std::fmt::Debug {
    /// Convert `object` to `version` (`"group/version"` or `"version"`).
    fn convert(&self, object: &Value, version: &str) -> Result<Value, ConvertError>;

    /// `IsMissingVersionError`.
    fn is_missing_version_error(&self, err: &ConvertError) -> bool {
        matches!(err, ConvertError::MissingVersion(_))
    }
}

/// The part of `runtime.ObjectConvertor` the version converter uses:
/// `ConvertToVersion(in, gv)`.
pub trait ObjectConvertor: Send + Sync {
    fn convert_to_version(
        &self,
        object: &Value,
        group_version: &str,
    ) -> Result<Value, ConvertError>;
}

/// `schema.ParseGroupVersion` (apimachinery/pkg/runtime/schema/group_version.go).
pub fn parse_group_version(gv: &str) -> Result<(String, String), ConvertError> {
    if gv.is_empty() || gv == "/" {
        return Ok((String::new(), String::new()));
    }
    match gv.matches('/').count() {
        0 => Ok((String::new(), gv.to_string())),
        1 => {
            let (g, v) = gv.split_once('/').unwrap_or_default();
            Ok((g.to_string(), v.to_string()))
        }
        _ => Err(ConvertError::Other(format!(
            "unexpected GroupVersion string: {gv}"
        ))),
    }
}

fn group_version_string(group: &str, version: &str) -> String {
    if group.is_empty() {
        version.to_string()
    } else {
        format!("{group}/{version}")
    }
}

/// `runtime.APIVersionInternal`.
pub const API_VERSION_INTERNAL: &str = "__internal";

type HubGetter = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// `versionConverter` (versionconverter.go:30-35): converts through the hub
/// version, as `ConvertToVersion` does for the scheme.
pub struct VersionConverter {
    object_convertor: Arc<dyn ObjectConvertor>,
    hub_getter: HubGetter,
}

impl std::fmt::Debug for VersionConverter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionConverter").finish_non_exhaustive()
    }
}

impl VersionConverter {
    /// `newVersionConverter` (:38-47): the hub is the internal version of the
    /// converted-from object's group.
    pub fn new(object_convertor: Arc<dyn ObjectConvertor>, hub_version: &str) -> Self {
        let hub_version = hub_version.to_string();
        Self {
            object_convertor,
            hub_getter: Arc::new(move |from: &str| {
                let group = parse_group_version(from).map(|g| g.0).unwrap_or_default();
                group_version_string(&group, &hub_version)
            }),
        }
    }

    /// `newCRDVersionConverter` (:50-58): one hub for every version.
    pub fn new_crd(object_convertor: Arc<dyn ObjectConvertor>, hub: &str) -> Self {
        let hub = hub.to_string();
        Self {
            object_convertor,
            hub_getter: Arc::new(move |_| hub.clone()),
        }
    }
}

impl Converter for VersionConverter {
    /// `versionConverter.Convert` (versionconverter.go:61-87).
    fn convert(&self, object: &Value, version: &str) -> Result<Value, ConvertError> {
        // Parse the target groupVersion.
        let target = parse_group_version(version)?;
        // If attempting to convert to the same version as we already have,
        // just return it.
        let from_version = object
            .get("apiVersion")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if parse_group_version(from_version)? == target {
            return Ok(object.clone());
        }
        // Convert to internal, then into the target version.
        let hub = (self.hub_getter)(from_version);
        let internal = self.object_convertor.convert_to_version(object, &hub)?;
        self.object_convertor.convert_to_version(&internal, version)
    }
}

/// `nopConverter.Convert` (nop_converter.go:32-40), as an
/// [`ObjectConvertor`]: only `apiVersion` is rewritten.
#[derive(Debug, Default, Clone, Copy)]
pub struct NopObjectConvertor;

impl ObjectConvertor for NopObjectConvertor {
    fn convert_to_version(
        &self,
        object: &Value,
        group_version: &str,
    ) -> Result<Value, ConvertError> {
        let mut out = object.clone();
        if let Some(map) = out.as_object_mut() {
            map.insert(
                "apiVersion".to_string(),
                Value::String(group_version.to_string()),
            );
        }
        Ok(out)
    }
}

/// `Value` at `path` (`f:`, `k:` and `v:` elements), if it exists.
fn value_at<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    let Some((first, rest)) = path.split_first() else {
        return Some(value);
    };
    if let Some(name) = first.strip_prefix("f:") {
        return value_at(value.as_object()?.get(name)?, rest);
    }
    if let Some(key) = first.strip_prefix("k:") {
        let want = serde_json::from_str::<Map<String, Value>>(key).ok()?;
        let item = value
            .as_array()?
            .iter()
            .find(|i| want.iter().all(|(k, v)| i.get(k) == Some(v)))?;
        return value_at(item, rest);
    }
    if let Some(v) = first.strip_prefix("v:") {
        let want = serde_json::from_str::<Value>(v).ok()?;
        let item = value.as_array()?.iter().find(|i| **i == want)?;
        return value_at(item, rest);
    }
    None
}

/// Write into `out` the part of `merged` that `path` names: the value of a
/// leaf, an empty container for a struct, the key fields of a list item
/// (`TypedValue.ExtractItems`).
fn extract_path(out: &mut Value, merged: &Value, path: &[String]) {
    let Some((first, rest)) = path.split_first() else {
        return;
    };
    let Some(source) = value_at(merged, std::slice::from_ref(first)) else {
        return;
    };
    if let Some(name) = first.strip_prefix("f:") {
        if !out.is_object() {
            *out = Value::Object(Map::new());
        }
        let map = out.as_object_mut().expect("just made an object");
        if rest.is_empty() {
            let value = match source {
                Value::Object(_) => map
                    .get(name)
                    .cloned()
                    .unwrap_or_else(|| Value::Object(Map::new())),
                other => other.clone(),
            };
            map.insert(name.to_string(), value);
        } else {
            let child = map.entry(name.to_string()).or_insert(Value::Null);
            extract_path(child, source, rest);
        }
    } else if let Some(key) = first.strip_prefix("k:") {
        let Ok(want) = serde_json::from_str::<Map<String, Value>>(key) else {
            return;
        };
        if !out.is_array() {
            *out = Value::Array(Vec::new());
        }
        let items = out.as_array_mut().expect("just made an array");
        let matches = |i: &Value| want.iter().all(|(k, v)| i.get(k) == Some(v));
        let at = match items.iter().position(matches) {
            Some(at) => at,
            None => {
                items.push(Value::Object(want.clone()));
                items.len() - 1
            }
        };
        extract_path(&mut items[at], source, rest);
    } else if first.starts_with("v:") {
        if !out.is_array() {
            *out = Value::Array(Vec::new());
        }
        let items = out.as_array_mut().expect("just made an array");
        if !items.contains(source) {
            items.push(source.clone());
        }
    }
}

use super::apply::ApplyConflict;
use super::{FieldManager, Managed, ManagerIdentity};

/// What `Updater.update` / `Updater.prune` fail with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateFailure {
    /// `merge.Conflicts`: another manager owns what the operation changed.
    Conflicts(Vec<ApplyConflict>),
    /// `failed to convert old object: ...` and the like.
    Convert(String),
}

impl FieldManager {
    /// `Convert` through the configured converter. An object that carries no
    /// `apiVersion` is the served version's, as upstream's objects always
    /// carry the GVK their codec set.
    pub(crate) fn convert_to(&self, object: &Value, version: &str) -> Result<Value, ConvertError> {
        let Some(converter) = &self.converter else {
            return Ok(object.clone());
        };
        let has_version = object
            .get("apiVersion")
            .and_then(Value::as_str)
            .is_some_and(|v| !v.is_empty());
        if has_version {
            return converter.convert(object, version);
        }
        let mut stamped = object.clone();
        if let Some(map) = stamped.as_object_mut() {
            map.insert(
                "apiVersion".to_string(),
                Value::String(self.api_version.clone()),
            );
        }
        converter.convert(&stamped, version)
    }

    fn is_missing_version(&self, err: &ConvertError) -> bool {
        self.converter
            .as_ref()
            .is_some_and(|c| c.is_missing_version_error(err))
    }

    /// `Updater.update` (update.go:73-164): compare `old` and `new` at the
    /// operation's version and, for each manager recorded at another version,
    /// at that version; remove from every manager (but `skip`) what the
    /// operation changed or removed. Without `force` a change to what another
    /// manager owns is a conflict and nothing is modified. Returns the
    /// comparison at the operation's version.
    pub(crate) fn update_versions(
        &self,
        old: &Value,
        new: &Value,
        managed: &mut Managed,
        skip: Option<&str>,
        force: bool,
    ) -> Result<super::fieldset::Comparison, UpdateFailure> {
        let compare_at = |old: &Value, new: &Value, version: &str| {
            let comparison = self.fields_of(old).compare(&self.fields_of(new));
            let exclude = self.reset_set_for(version);
            if exclude.is_empty() {
                comparison
            } else {
                comparison.exclude(&exclude)
            }
        };
        let current = compare_at(old, new, &self.api_version);
        let mut versions: std::collections::BTreeMap<String, super::fieldset::Comparison> =
            std::collections::BTreeMap::new();

        let mut conflicts: Vec<(String, Set)> = Vec::new();
        let mut removed: Vec<(String, Set)> = Vec::new();
        let mut missing: Vec<String> = Vec::new();

        let ids: Vec<String> = managed.fields.keys().cloned().collect();
        for id in ids {
            if skip == Some(id.as_str()) {
                continue;
            }
            let vs = &managed.fields[&id];
            let version = vs.api_version.clone();
            let compare = if version == self.api_version {
                &current
            } else {
                if !versions.contains_key(&version) {
                    let converted = self
                        .convert_to(old, &version)
                        .map_err(|e| (e, "old"))
                        .and_then(|o| {
                            self.convert_to(new, &version)
                                .map(|n| (o, n))
                                .map_err(|e| (e, "new"))
                        });
                    match converted {
                        Ok((o, n)) => {
                            versions.insert(version.clone(), compare_at(&o, &n, &version));
                        }
                        Err((e, which)) => {
                            if self.is_missing_version(&e) {
                                missing.push(id);
                                continue;
                            }
                            return Err(UpdateFailure::Convert(format!(
                                "failed to convert {which} object: {e}"
                            )));
                        }
                    }
                }
                &versions[&version]
            };
            let changed = compare.modified.union(&compare.added);
            let conflict_set = managed.fields[&id].set.intersection(&changed);
            if !conflict_set.is_empty() {
                conflicts.push((id.clone(), conflict_set));
            }
            if !compare.removed.is_empty() {
                removed.push((id, compare.removed.clone()));
            }
        }

        if !force && !conflicts.is_empty() {
            let mut out = Vec::new();
            for (id, set) in &conflicts {
                let owner: ManagerIdentity = serde_json::from_str(id).unwrap_or_default();
                for path in set.members() {
                    out.push(ApplyConflict {
                        manager: owner.manager.clone(),
                        path: super::apply::path_string(&path),
                    });
                }
            }
            out.sort_by(|a, b| a.manager.cmp(&b.manager));
            return Err(UpdateFailure::Conflicts(out));
        }

        for id in missing {
            managed.fields.remove(&id);
            managed.times.remove(&id);
        }
        for (id, set) in conflicts.iter().chain(removed.iter()) {
            if let Some(vs) = managed.fields.get_mut(id) {
                vs.set = vs.set.difference(set);
            }
        }
        managed.fields.retain(|_, vs| !vs.set.is_empty());
        Ok(current)
    }

    /// `Updater.prune` (update.go:256-282): remove what the applier owned last
    /// time (`last`, at `last_version`) from `merged`, unless some manager
    /// owns it now (`addBackOwnedItems`). The work happens at the version of
    /// the applier's last entry, and the result is converted back to the
    /// applying version.
    pub(crate) fn prune_versioned(
        &self,
        merged: &Value,
        managed: &Managed,
        last: &Set,
        last_version: &str,
    ) -> Result<Value, UpdateFailure> {
        let failed = |what: &str, e: ConvertError| {
            UpdateFailure::Convert(format!("failed to convert merged object {what}: {e}"))
        };
        let converted = match self.convert_to(merged, last_version) {
            Ok(v) => v,
            Err(e) if self.is_missing_version(&e) => return Ok(merged.clone()),
            Err(e) => return Err(failed("to last applied version", e)),
        };
        let mut pruned = converted.clone();
        for path in last.members() {
            remove_path(&mut pruned, &path);
        }

        // addBackOwnedItems (:284-311): the sets by version, the pruned
        // version first "to avoid conversion failure caused by pruned fields
        // which are required for conversion".
        let mut at_version: std::collections::BTreeMap<String, Set> =
            std::collections::BTreeMap::new();
        for vs in managed.fields.values() {
            let entry = at_version.entry(vs.api_version.clone()).or_default();
            *entry = entry.union(&vs.set);
        }
        let mut order: Vec<String> = Vec::new();
        if at_version.contains_key(last_version) {
            order.push(last_version.to_string());
        }
        order.extend(at_version.keys().filter(|v| *v != last_version).cloned());

        let mut merged_v = converted;
        let mut pruned_v = pruned;
        for version in order {
            // addBackOwnedItemsForVersion (:313-337).
            let (m, p) = match self
                .convert_to(&merged_v, &version)
                .and_then(|m| self.convert_to(&pruned_v, &version).map(|p| (m, p)))
            {
                Ok(pair) => pair,
                Err(e) if self.is_missing_version(&e) => continue,
                Err(e) => {
                    return Err(failed(&format!("at version {version}"), e));
                }
            };
            let owned = at_version[&version].intersection(&self.fields_of(&m).set);
            let mut extracted = Value::Null;
            for path in owned.members() {
                extract_path(&mut extracted, &m, &path);
            }
            pruned_v = if extracted.is_null() {
                p
            } else {
                self.merge(&p, &extracted)
            };
            merged_v = m;
        }
        self.convert_to(&pruned_v, &self.api_version)
            .map_err(|e| failed("back to the applying version", e))
    }
}
