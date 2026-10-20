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
fn parse_group_version(gv: &str) -> Result<(String, String), ConvertError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fieldmanager::fieldset::field_path;
    use crate::fieldmanager::{build_manager_identifier, ResetFields, VersionedSet};
    use serde_json::json;

    /// Two versions of one kind that spell the replica count differently:
    /// `example.io/v1` `.spec.replicas`, `example.io/v2` `.spec.size`. The
    /// hub (`example.io/__internal`) spells it as v2 does. Mirrors the fake
    /// object convertor of `versionconverter_test.go`.
    struct Renamer;

    impl ObjectConvertor for Renamer {
        fn convert_to_version(&self, obj: &Value, gv: &str) -> Result<Value, ConvertError> {
            if gv == "example.io/v0" {
                return Err(ConvertError::MissingVersion("example.io/v0".into()));
            }
            let mut out = obj.clone();
            if let Some(spec) = out.get_mut("spec").and_then(Value::as_object_mut) {
                let count = spec.remove("replicas").or_else(|| spec.remove("size"));
                if let Some(c) = count {
                    let name = if gv == "example.io/v1" {
                        "replicas"
                    } else {
                        "size"
                    };
                    spec.insert(name.into(), c);
                }
            }
            out["apiVersion"] = json!(gv);
            Ok(out)
        }
    }

    fn converter() -> Arc<dyn Converter> {
        Arc::new(VersionConverter::new(
            Arc::new(Renamer),
            API_VERSION_INTERNAL,
        ))
    }

    fn fm(version: &str) -> FieldManager {
        FieldManager::new(version, None, ResetFields::new()).with_converter(Some(converter()))
    }

    fn obj(version: &str, field: &str, value: i64) -> Value {
        json!({"apiVersion": version, "kind": "Foo", "spec": {field: value}})
    }

    fn entry(manager: &str, op: &str, version: &str, path: &[&str]) -> (String, VersionedSet) {
        let mut set = Set::new();
        set.insert(&field_path(path));
        (
            build_manager_identifier(manager, op, version, ""),
            VersionedSet {
                set,
                api_version: version.to_string(),
                applied: op == "Apply",
            },
        )
    }

    fn managed_of(entries: Vec<(String, VersionedSet)>) -> Managed {
        let mut managed = Managed::default();
        for (id, vs) in entries {
            managed.times.insert(id.clone(), None);
            managed.fields.insert(id, vs);
        }
        managed
    }

    fn id(manager: &str, op: &str, version: &str) -> String {
        build_manager_identifier(manager, op, version, "")
    }

    /// versionconverter_test.go `TestVersionConverter`.
    #[test]
    fn convert_goes_through_the_hub_and_skips_the_same_version() {
        let c = converter();
        let v1 = obj("example.io/v1", "replicas", 3);
        assert_eq!(c.convert(&v1, "example.io/v1").unwrap(), v1);
        assert_eq!(
            c.convert(&v1, "example.io/v2").unwrap(),
            obj("example.io/v2", "size", 3)
        );
    }

    /// `newCRDVersionConverter`: one hub whatever the version.
    #[test]
    fn crd_converter_uses_a_single_hub() {
        let c = VersionConverter::new_crd(Arc::new(NopObjectConvertor), "example.io/v1");
        let out = c
            .convert(&obj("example.io/v2", "size", 1), "example.io/v1")
            .unwrap();
        assert_eq!(out["apiVersion"], "example.io/v1");
        assert_eq!(out["spec"]["size"], 1);
    }

    #[test]
    fn parse_group_version_rejects_two_slashes() {
        assert!(parse_group_version("a/b/c").is_err());
        assert_eq!(
            parse_group_version("v1").unwrap(),
            (String::new(), "v1".into())
        );
    }

    /// update.go:98-139: an update at v2 changes `.spec.size`, which is the
    /// v1 manager's `.spec.replicas` once the objects are converted to v1.
    #[test]
    fn an_update_takes_the_fields_of_a_manager_at_another_version() {
        let managed = managed_of(vec![entry(
            "a",
            "Update",
            "example.io/v1",
            &["spec", "replicas"],
        )]);
        let out = fm("example.io/v2")
            .update_managed(
                &obj("example.io/v2", "size", 1),
                &obj("example.io/v2", "size", 2),
                managed,
                "b",
                false,
            )
            .unwrap();
        assert!(
            !out.fields.contains_key(&id("a", "Update", "example.io/v1")),
            "a's only field was taken, so its entry goes"
        );
        let b = &out.fields[&id("b", "Update", "example.io/v2")];
        assert!(b.set.has(&field_path(&["spec", "size"])));
    }

    /// Without a converter the sets are compared as-is, so the same update
    /// leaves the v1 entry alone: the behaviour #2736 replaces.
    #[test]
    fn without_a_converter_other_versions_are_compared_as_is() {
        let managed = managed_of(vec![entry(
            "a",
            "Update",
            "example.io/v1",
            &["spec", "replicas"],
        )]);
        let out = FieldManager::new("example.io/v2", None, ResetFields::new())
            .update_managed(
                &obj("example.io/v2", "size", 1),
                &obj("example.io/v2", "size", 2),
                managed,
                "b",
                false,
            )
            .unwrap();
        assert!(out.fields.contains_key(&id("a", "Update", "example.io/v1")));
    }

    /// update.go:107-111: a version with no corresponding type drops the
    /// manager.
    #[test]
    fn a_manager_at_a_missing_version_is_dropped() {
        let managed = managed_of(vec![
            entry("gone", "Update", "example.io/v0", &["spec", "replicas"]),
            entry("kept", "Update", "example.io/v1", &["spec", "other"]),
        ]);
        let out = fm("example.io/v2")
            .update_managed(
                &obj("example.io/v2", "size", 1),
                &obj("example.io/v2", "size", 2),
                managed,
                "b",
                false,
            )
            .unwrap();
        assert!(!out
            .fields
            .contains_key(&id("gone", "Update", "example.io/v0")));
        assert!(out
            .fields
            .contains_key(&id("kept", "Update", "example.io/v1")));
    }

    /// A conversion that is not a missing-version error fails the update
    /// (`failed to convert old object`).
    #[test]
    fn another_conversion_error_fails_the_update() {
        struct Broken;
        impl ObjectConvertor for Broken {
            fn convert_to_version(&self, _: &Value, _: &str) -> Result<Value, ConvertError> {
                Err(ConvertError::Other("boom".into()))
            }
        }
        let fm = FieldManager::new("example.io/v2", None, ResetFields::new()).with_converter(Some(
            Arc::new(VersionConverter::new(
                Arc::new(Broken),
                API_VERSION_INTERNAL,
            )),
        ));
        let managed = managed_of(vec![entry(
            "a",
            "Update",
            "example.io/v1",
            &["spec", "replicas"],
        )]);
        let err = fm
            .update_managed(
                &obj("example.io/v2", "size", 1),
                &obj("example.io/v2", "size", 2),
                managed,
                "b",
                false,
            )
            .unwrap_err();
        assert_eq!(err, "failed to convert old object: boom");
    }

    /// update.go:123-139 on the Apply path: b applies v2 `.spec.size`, a owns
    /// the v1 spelling of it.
    #[test]
    fn apply_conflicts_with_a_manager_at_another_version() {
        let live = fm("example.io/v1")
            .apply_value(None, &obj("example.io/v1", "replicas", 1), "a", false)
            .unwrap();
        let err = fm("example.io/v2")
            .apply_value(Some(&live), &obj("example.io/v2", "size", 5), "b", false)
            .unwrap_err();
        match err {
            UpdateFailure::Conflicts(c) => {
                assert_eq!(c.len(), 1);
                assert_eq!(c[0].manager, "a");
                assert_eq!(c[0].path, ".spec.replicas");
            }
            other => panic!("{other:?}"),
        }
        // Forced, a loses the field.
        let forced = fm("example.io/v2")
            .apply_value(Some(&live), &obj("example.io/v2", "size", 5), "b", true)
            .unwrap();
        let entries = forced["metadata"]["managedFields"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["manager"], "b");
    }

    /// update.go:256-282: the applier owned `.spec.replicas` at v1 and no
    /// longer applies it at v2, so the pruned field is removed.
    #[test]
    fn prune_works_at_the_version_of_the_last_apply() {
        let live = fm("example.io/v1")
            .apply_value(None, &obj("example.io/v1", "replicas", 1), "a", false)
            .unwrap();
        let config = json!({"apiVersion": "example.io/v2", "kind": "Foo",
            "metadata": {"labels": {"x": "y"}}});
        let out = fm("example.io/v2")
            .apply_value(Some(&live), &config, "a", false)
            .unwrap();
        assert!(out["spec"].get("size").is_none(), "{out}");
        assert!(out["spec"].get("replicas").is_none(), "{out}");
        assert_eq!(out["metadata"]["labels"]["x"], "y");
    }

    /// `addBackOwnedItems`: what another manager owns survives the prune,
    /// even when it owns it at another version.
    #[test]
    fn prune_keeps_what_another_version_owns() {
        let live = fm("example.io/v1")
            .apply_value(None, &obj("example.io/v1", "replicas", 1), "a", false)
            .unwrap();
        let entries: Vec<crate::types::ManagedFieldsEntry> =
            serde_json::from_value(live["metadata"]["managedFields"].clone()).unwrap();
        let mut managed = Managed::decode(&entries).unwrap();
        let (bid, vs) = entry("b", "Update", "example.io/v2", &["spec", "size"]);
        managed.times.insert(bid.clone(), None);
        managed.fields.insert(bid, vs);
        let mut co = live.clone();
        co["metadata"]["managedFields"] = serde_json::to_value(managed.encode().unwrap()).unwrap();

        let config = json!({"apiVersion": "example.io/v2", "kind": "Foo",
            "metadata": {"labels": {"x": "y"}}});
        let out = fm("example.io/v2")
            .apply_value(Some(&co), &config, "a", false)
            .unwrap();
        assert_eq!(out["spec"]["size"], 1, "{out}");
    }
}
