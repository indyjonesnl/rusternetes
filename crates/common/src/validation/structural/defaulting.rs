//! `structuraldefaulting.Default` (`algorithm.go:25-72`), `ValidateDefaults`,
//! `PruneDefaults` and the `requirePrunedDefaults` ratchet.
//!
//! Port of, under
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/` (release-1.35):
//!
//! - `apiserver/schema/defaulting/algorithm.go` (`Default`),
//! - `apiserver/schema/defaulting/validation.go` (`ValidateDefaults`,
//!   `validate`),
//! - `apiserver/schema/defaulting/prune.go` (`PruneDefaults`),
//! - `apiserver/schema/defaulting/surroundingobject.go`
//!   (`SurroundingObjectFunc`),
//! - `apiserver/schema/objectmeta/validation.go` (`Validate`, the embedded
//!   `ObjectMeta`/`TypeMeta` check run on a default),
//! - `apis/apiextensions/validation/validation.go` (`requirePrunedDefaults`,
//!   `schemaHasUnprunedDefaults`, `:1779-1810`).
//!
//! The caller is `validateCustomResourceDefinitionValidation`
//! (`validation.go:963`), which runs `ValidateDefaults` once
//! `ValidateStructural` found nothing.
//!
//! Deliberately not ported here (tracked in #3928 follow-ups):
//!
//! - the `cel.NewValidator(...).Validate` step on a default
//!   (`validation.go:97-110`, `:134-148`) with its `RuntimeCELCostBudget`
//!   threading and the `CRDValidationRatcheting` nil-old-object pass — the CEL
//!   evaluator lives in the api-server crate, not in `common`;
//! - `apiservervalidation.ValidateCustomResource` is served by this crate's
//!   [`SchemaValidator::collect_issues`], so the *detail text* of a schema
//!   failure on a default is this crate's, not go-openapi's. The error type
//!   and path (`default`, `default.a.b[0]`) follow
//!   `kubeOpenAPIResultToFieldErrors` (`apiserver/validation/validation.go:165-`).
//!
//! Go iterates `Properties` (a map) in random order; the keys are visited
//! sorted here, which is one of the orders Go may produce.

use serde_json::{Map, Value};

use super::objectmeta::{coerce_without_paths, get_object_meta};
use super::prune::prune_with_options;
use super::{new_structural, validate_structural, Structural};
use crate::resources::crd::{
    CustomResourceDefinitionSpec, JSONSchemaProps, JSONSchemaPropsOrArray, JSONSchemaPropsOrBool,
};
use crate::schema_validation::{PathSeg, SchemaValidator};
use crate::types::ObjectMeta;
use crate::validation::field::{BadValue, Error, ErrorList, Path};
use crate::validation::metav1::is_dns1035_label;
use crate::validation::objectmeta::{name_is_path_segment, validate_object_meta};

// ---------------------------------------------------------------------------
// surroundingobject.go
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Step {
    Child(String),
    Index,
}

/// `SurroundingObjectFunc` (surroundingobject.go:35): builds an object with
/// `x` at a leaf determined by a series of `Child(k)`/`Index()` steps, and the
/// accessor that extracts `x` again. The steps run root to leaf.
#[derive(Debug, Clone)]
struct Surrounding {
    steps: Vec<Step>,
    /// `WithTypeMeta(validation/v1 Validation)` (:63-83).
    with_type_meta: bool,
}

impl Surrounding {
    /// `NewRootObjectFunc().WithTypeMeta(...)` as used by `ValidateDefaults`
    /// (validation.go:40, :73).
    fn root_with_type_meta() -> Self {
        Self {
            steps: Vec::new(),
            with_type_meta: true,
        }
    }

    /// `NewRootObjectFunc()` as used by `PruneDefaults` (prune.go:30).
    fn root() -> Self {
        Self {
            steps: Vec::new(),
            with_type_meta: false,
        }
    }

    /// `f.Child(k)` (:86-106).
    fn child(&self, k: &str) -> Self {
        let mut s = self.clone();
        s.steps.push(Step::Child(k.to_string()));
        s
    }

    /// `f.Index()` (:109-130).
    fn index(&self) -> Self {
        let mut s = self.clone();
        s.steps.push(Step::Index);
        s
    }

    /// `f(focus)`: the surrounding object.
    fn build(&self, focus: Value) -> Result<Value, String> {
        let mut v = focus;
        for step in self.steps.iter().rev() {
            v = match step {
                Step::Child(k) => Value::Object(Map::from_iter([(k.clone(), v)])),
                Step::Index => Value::Array(vec![v]),
            };
        }
        // NewRootObjectFunc (:53-61).
        let Value::Object(mut obj) = v else {
            return Err("object root default value must be of object type".to_string());
        };
        if self.with_type_meta {
            obj.entry("kind".to_string())
                .or_insert_with(|| Value::String("Validation".to_string()));
            obj.entry("apiVersion".to_string())
                .or_insert_with(|| Value::String("validation/v1".to_string()));
        }
        Ok(Value::Object(obj))
    }

    /// The accessor returned alongside `f(focus)`: `None` when the path does
    /// not exist (`found == false`).
    fn access(&self, root: &Value) -> Result<Option<Value>, String> {
        let mut cur = root;
        for step in &self.steps {
            match step {
                Step::Child(k) => {
                    let Value::Object(m) = cur else {
                        return Err(format!(".{k} must be of object type"));
                    };
                    match m.get(k) {
                        Some(v) => cur = v,
                        None => return Ok(None),
                    }
                }
                Step::Index => {
                    let Value::Array(a) = cur else {
                        return Err("[] must be of array type".to_string());
                    };
                    match a.first() {
                        Some(v) => cur = v,
                        None => return Ok(None),
                    }
                }
            }
        }
        Ok(Some(cur.clone()))
    }
}

// ---------------------------------------------------------------------------
// objectmeta/validation.go: Validate
// ---------------------------------------------------------------------------

/// `schemaobjectmeta.Validate` (objectmeta/validation.go:30-73): validates
/// embedded ObjectMeta and TypeMeta, and those at the root if
/// `is_resource_root`.
fn object_meta_validate(
    pth: Option<&Path>,
    obj: &Value,
    s: &Structural,
    is_resource_root: bool,
) -> ErrorList {
    if is_resource_root && !s.extensions.x_embedded_resource {
        let mut clone = s.clone();
        clone.extensions.x_embedded_resource = true;
        return object_meta_validate_rec(pth, obj, Some(&clone));
    }
    object_meta_validate_rec(pth, obj, Some(s))
}

fn opt_child(p: Option<&Path>, name: &str) -> Path {
    match p {
        Some(p) => p.child(name),
        None => Path::new(name),
    }
}

fn opt_key(p: Option<&Path>, k: &str) -> Path {
    match p {
        Some(p) => p.key(k),
        None => Path::default().key(k),
    }
}

fn opt_index(p: Option<&Path>, i: usize) -> Path {
    match p {
        Some(p) => p.index(i),
        None => Path::default().index(i),
    }
}

fn object_meta_validate_rec(pth: Option<&Path>, x: &Value, s: Option<&Structural>) -> ErrorList {
    let Some(s) = s else {
        return Vec::new();
    };
    let mut errs = Vec::new();
    match x {
        Value::Object(map) => {
            if s.extensions.x_embedded_resource {
                errs.extend(validate_embedded_resource(pth, map));
            }
            for (k, v) in map {
                if let Some(prop) = s.properties.get(k) {
                    errs.extend(object_meta_validate_rec(
                        Some(&opt_child(pth, k)),
                        v,
                        Some(prop),
                    ));
                } else if let Some(ap) = s.additional_properties.as_deref() {
                    errs.extend(object_meta_validate_rec(
                        Some(&opt_key(pth, k)),
                        v,
                        ap.structural.as_ref(),
                    ));
                }
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                errs.extend(object_meta_validate_rec(
                    Some(&opt_index(pth, i)),
                    v,
                    s.items.as_deref(),
                ));
            }
        }
        _ => {}
    }
    errs
}

/// `schema.ParseGroupVersion` (apimachinery/pkg/runtime/schema/group_version.go).
fn parse_group_version(gv: &str) -> Result<(), String> {
    if gv.is_empty() || gv == "/" {
        return Ok(());
    }
    if gv.matches('/').count() > 1 {
        return Err(format!("unexpected GroupVersion string: {gv}"));
    }
    Ok(())
}

/// `validateEmbeddedResource` (objectmeta/validation.go:75-123).
fn validate_embedded_resource(pth: Option<&Path>, x: &Map<String, Value>) -> ErrorList {
    let mut errs = Vec::new();
    // require apiVersion and kind, but not metadata
    if !x.contains_key("apiVersion") {
        errs.push(Error::required(&opt_child(pth, "apiVersion"), ""));
    }
    if !x.contains_key("kind") {
        errs.push(Error::required(&opt_child(pth, "kind"), ""));
    }
    for (k, v) in x {
        match k.as_str() {
            "apiVersion" => {
                let p = opt_child(pth, "apiVersion");
                match v {
                    Value::String(a) if a.is_empty() => {
                        errs.push(Error::invalid(&p, a.clone(), "must not be empty"))
                    }
                    Value::String(a) => {
                        if let Err(e) = parse_group_version(a) {
                            errs.push(Error::invalid(&p, a.clone(), e));
                        }
                    }
                    other => errs.push(Error::invalid(
                        &p,
                        BadValue::Json(other.clone()),
                        "must be a string",
                    )),
                }
            }
            "kind" => {
                let p = opt_child(pth, "kind");
                match v {
                    Value::String(kind) if kind.is_empty() => {
                        errs.push(Error::invalid(&p, kind.clone(), "must not be empty"))
                    }
                    Value::String(kind) => {
                        let msgs = is_dns1035_label(&kind.to_lowercase());
                        if !msgs.is_empty() {
                            errs.push(Error::invalid(
                                &p,
                                kind.clone(),
                                format!(
                                    "may have mixed case, but should otherwise match: {}",
                                    msgs.join(",")
                                ),
                            ));
                        }
                    }
                    other => errs.push(Error::invalid(
                        &p,
                        BadValue::Json(other.clone()),
                        "must be a string",
                    )),
                }
            }
            "metadata" => {
                let p = opt_child(pth, "metadata");
                // GetObjectMeta returns the decoded ObjectMeta in its
                // ToUnstructured form, which the typed `ObjectMeta` reads.
                let meta = get_object_meta(x, false).and_then(|r| {
                    serde_json::from_value::<ObjectMeta>(Value::Object(r.meta.unwrap_or_default()))
                        .map_err(|e| e.to_string())
                });
                match meta {
                    Err(e) => errs.push(Error::invalid(&p, BadValue::Json(v.clone()), e)),
                    Ok(mut meta) => {
                        if meta.name.is_empty() {
                            // we have to set something to avoid an error
                            meta.name = "fakename".to_string();
                        }
                        let namespaced = meta.namespace.as_deref().is_some_and(|n| !n.is_empty());
                        errs.extend(validate_object_meta(
                            &meta,
                            namespaced,
                            name_is_path_segment,
                            &p,
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    errs
}

/// `utilerrors.Aggregate.Error()` for a list of field errors.
fn aggregate_message(errs: &[Error]) -> String {
    match errs {
        [one] => one.to_string(),
        many => format!(
            "[{}]",
            many.iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

// ---------------------------------------------------------------------------
// apiservervalidation.ValidateCustomResource
// ---------------------------------------------------------------------------

/// The schema (a `JSONSchemaProps`) at `segs` below `props`.
fn props_at<'a>(props: &'a JSONSchemaProps, segs: &[PathSeg]) -> Option<&'a JSONSchemaProps> {
    let mut cur = props;
    for seg in segs {
        cur = match seg {
            PathSeg::Key(k) => match cur.properties.as_ref().and_then(|p| p.get(k)) {
                Some(p) => p,
                None => match cur.additional_properties.as_deref() {
                    Some(JSONSchemaPropsOrBool::Schema(s)) => s,
                    _ => return None,
                },
            },
            PathSeg::Index(i) => match cur.items.as_deref() {
                Some(JSONSchemaPropsOrArray::Schema(s)) => s,
                Some(JSONSchemaPropsOrArray::Schemas(l)) => l.get(*i)?,
                None => return None,
            },
        };
    }
    Some(cur)
}

fn value_at<'a>(v: &'a Value, segs: &[PathSeg]) -> Option<&'a Value> {
    let mut cur = v;
    for seg in segs {
        cur = match seg {
            PathSeg::Key(k) => cur.get(k)?,
            PathSeg::Index(i) => cur.get(*i)?,
        };
    }
    Some(cur)
}

/// `ValidateCustomResource` (apiserver/validation/validation.go:152-163) with
/// `kubeOpenAPIResultToFieldErrors`: `Required` for a missing property,
/// `NotSupported` for an enum miss, `Invalid` otherwise.
fn validate_custom_resource(fld_path: &Path, value: &Value, props: &JSONSchemaProps) -> ErrorList {
    let mut errs = Vec::new();
    for issue in SchemaValidator::collect_issues(props, value) {
        let name = PathSeg::render_path(&issue.path);
        let err_path = if name.is_empty() {
            fld_path.clone()
        } else {
            fld_path.child(name.clone())
        };
        let detail = issue
            .message
            .strip_prefix(&format!("{name}: "))
            .unwrap_or(&issue.message)
            .to_string();
        let bad = value_at(value, &issue.path).cloned().unwrap_or(Value::Null);
        if detail.starts_with("Required value") {
            errs.push(Error::required(&err_path, ""));
        } else if detail.starts_with("Unsupported value") {
            let allowed: Vec<String> = props_at(props, &issue.path)
                .and_then(|p| p.enum_.as_ref())
                .map(|e| {
                    e.iter()
                        .map(|v| match v {
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            errs.push(Error::not_supported(
                &err_path,
                BadValue::Json(bad),
                &allowed,
            ));
        } else {
            errs.push(Error::invalid(&err_path, BadValue::Json(bad), detail));
        }
    }
    errs
}

// ---------------------------------------------------------------------------
// validation.go: ValidateDefaults
// ---------------------------------------------------------------------------

/// `ValidateDefaults` (validation.go:37-55): checks that default values
/// validate and are properly pruned. `props` is the `JSONSchemaProps` `s` was
/// built from (go: `s.ToKubeOpenAPI()`, the schema a default is validated
/// against). The `Err` is upstream's "this should never happen" error.
pub fn validate_defaults(
    pth: &Path,
    s: &Structural,
    props: &JSONSchemaProps,
    is_resource_root: bool,
    require_pruned_defaults: bool,
) -> Result<ErrorList, String> {
    let mut s = s.clone();
    if is_resource_root {
        s.extensions.x_embedded_resource = true;
    }
    validate(
        pth,
        &s,
        Some(props),
        &s,
        &Surrounding::root_with_type_meta(),
        false,
        require_pruned_defaults,
    )
}

/// `validate` (validation.go:61-196), minus the CEL steps (see the module
/// doc). `inside_meta` is true if `s` specifies TypeMeta or ObjectMeta.
fn validate(
    pth: &Path,
    s: &Structural,
    props: Option<&JSONSchemaProps>,
    root_schema: &Structural,
    f: &Surrounding,
    inside_meta: bool,
    require_pruned_defaults: bool,
) -> Result<ErrorList, String> {
    let mut all_errs: ErrorList = Vec::new();
    let mut inside_meta = inside_meta;
    let mut f = f.clone();
    let mut root_schema = root_schema;
    if s.extensions.x_embedded_resource {
        inside_meta = false;
        f = Surrounding::root_with_type_meta();
        root_schema = s;
    }

    // `s.Default.Object != nil`: a JSON null default is a nil object.
    let default = s.generic.default.as_ref().filter(|d| !d.is_null());
    if let Some(default) = default {
        let default_path = pth.child("default");
        if inside_meta {
            let mut obj = f
                .build(default.clone())
                .map_err(|e| format!("failed to validate default value inside metadata: {e}"))?;
            // check ObjectMeta/TypeMeta and everything else
            if let Some(err) = coerce_without_paths(None, &mut obj, Some(root_schema), true, false)
            {
                all_errs.push(Error::invalid(
                    &default_path,
                    BadValue::Json(default.clone()),
                    format!("must result in valid metadata: {err}"),
                ));
            } else {
                let errs = object_meta_validate(None, &obj, root_schema, true);
                if !errs.is_empty() {
                    all_errs.push(Error::invalid(
                        &default_path,
                        BadValue::Json(default.clone()),
                        format!(
                            "must result in valid metadata: {}",
                            aggregate_message(&errs)
                        ),
                    ));
                } else if let Some(props) = props {
                    all_errs.extend(validate_custom_resource(&default_path, default, props));
                }
            }
        } else {
            // check whether default is pruned
            if require_pruned_defaults {
                let mut pruned = default.clone();
                prune_with_options(
                    &mut pruned,
                    Some(s),
                    s.extensions.x_embedded_resource,
                    false,
                );
                if &pruned != default {
                    all_errs.push(Error::invalid(
                        &default_path,
                        BadValue::Json(default.clone()),
                        "must not have unknown fields",
                    ));
                }
            }

            // check ObjectMeta/TypeMeta and everything else. Coerce mutates
            // the default in place and the checks after it see the result.
            let mut d = default.clone();
            if let Some(err) = coerce_without_paths(
                Some(&default_path),
                &mut d,
                Some(s),
                s.extensions.x_embedded_resource,
                false,
            ) {
                all_errs.push(err);
            } else {
                let errs = object_meta_validate(
                    Some(&default_path),
                    &d,
                    s,
                    s.extensions.x_embedded_resource,
                );
                if !errs.is_empty() {
                    all_errs.extend(errs);
                } else if let Some(props) = props {
                    let errs = validate_custom_resource(&default_path, &d, props);
                    all_errs.extend(errs);
                }
            }
        }
    }

    // do not follow additionalProperties because defaults are forbidden there

    if let Some(items) = s.items.as_deref() {
        let item_props = match props.and_then(|p| p.items.as_deref()) {
            Some(JSONSchemaPropsOrArray::Schema(i)) => Some(i),
            _ => None,
        };
        all_errs.extend(validate(
            &pth.child("items"),
            items,
            item_props,
            root_schema,
            &f.index(),
            inside_meta,
            require_pruned_defaults,
        )?);
    }

    for (k, sub) in &s.properties {
        let mut sub_inside_meta = inside_meta;
        if s.extensions.x_embedded_resource && (k == "metadata" || k == "apiVersion" || k == "kind")
        {
            sub_inside_meta = true;
        }
        let sub_props = props
            .and_then(|p| p.properties.as_ref())
            .and_then(|m| m.get(k));
        all_errs.extend(validate(
            &pth.child("properties").key(k),
            sub,
            sub_props,
            root_schema,
            &f.child(k),
            sub_inside_meta,
            require_pruned_defaults,
        )?);
    }

    Ok(all_errs)
}

// ---------------------------------------------------------------------------
// prune.go: PruneDefaults
// ---------------------------------------------------------------------------

/// `PruneDefaults` (prune.go:29-34): prunes default values according to the
/// schema and the ObjectMeta definition of the running server. It mutates the
/// passed schema.
pub fn prune_defaults(s: &mut Structural) -> Result<(), String> {
    let root = s.clone();
    prune_defaults_rec(s, &root, &Surrounding::root()).map(|_| ())
}

/// `pruner.pruneDefaults` (prune.go:41-90). `root` is `pruner.rootSchema`.
fn prune_defaults_rec(
    s: &mut Structural,
    root: &Structural,
    f: &Surrounding,
) -> Result<bool, String> {
    let mut changed = false;

    if let Some(default) = s.generic.default.clone().filter(|d| !d.is_null()) {
        let orig = default.clone();
        let fail = |e: String| format!("failed to prune default value: {e}");
        let mut obj = f.build(default).map_err(fail)?;
        if let Some(err) = coerce_without_paths(None, &mut obj, Some(root), true, true) {
            return Err(fail(err.to_string()));
        }
        prune_with_options(&mut obj, Some(root), true, false);
        s.generic.default = f.access(&obj).map_err(fail)?;
        changed = changed || s.generic.default.as_ref() != Some(&orig);
    }

    if let Some(ap) = s.additional_properties.as_deref_mut() {
        if let Some(inner) = ap.structural.as_mut() {
            changed = prune_defaults_rec(inner, root, &f.child("*"))? || changed;
        }
    }
    if let Some(items) = s.items.as_deref_mut() {
        changed = prune_defaults_rec(items, root, &f.index())? || changed;
    }
    for (k, sub) in s.properties.iter_mut() {
        changed = prune_defaults_rec(sub, root, &f.child(k))? || changed;
    }

    Ok(changed)
}

// ---------------------------------------------------------------------------
// validation.go: requirePrunedDefaults
// ---------------------------------------------------------------------------

/// `schemaHasUnprunedDefaults` (validation.go:1795-1810).
fn schema_has_unpruned_defaults(schema: &JSONSchemaProps) -> bool {
    if !crate::validation::crd::schema_has(schema, &|s| s.default.is_some()) {
        return false;
    }
    let Ok(ss) = new_structural(schema) else {
        return false;
    };
    if !validate_structural(&Path::new(""), &ss).is_empty() {
        return false;
    }
    let mut pruned = ss.clone();
    if prune_defaults(&mut pruned).is_err() {
        return false;
    }
    ss != pruned
}

/// `requirePrunedDefaults` (validation.go:1779-1793): false if there are any
/// unpruned defaults in `old_spec`, true otherwise. (v1 has no top-level
/// `spec.validation`, so only the per-version schemas are checked.)
pub fn require_pruned_defaults(old_spec: &CustomResourceDefinitionSpec) -> bool {
    !old_spec.versions.iter().any(|v| {
        v.schema
            .as_ref()
            .and_then(|s| s.open_apiv3_schema.as_ref())
            .is_some_and(schema_has_unpruned_defaults)
    })
}

// ---------------------------------------------------------------------------
// algorithm.go
// ---------------------------------------------------------------------------

/// `Default` (:30-72): defaults `x` depending on the default values in `s`.
pub fn default(x: &mut Value, s: Option<&Structural>) {
    let Some(s) = s else {
        return;
    };

    match x {
        Value::Object(map) => {
            for (k, prop) in &s.properties {
                let Some(def) = default_value(prop) else {
                    continue;
                };
                if map
                    .get(k)
                    .is_none_or(|v| is_non_nullable_null(v, Some(prop)))
                {
                    map.insert(k.clone(), def.clone());
                }
            }
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys {
                let Some(v) = map.get_mut(&k) else { continue };
                if let Some(prop) = s.properties.get(&k) {
                    default(v, Some(prop));
                } else if let Some(ap) = s.additional_properties.as_deref() {
                    let ap_s = ap.structural.as_ref();
                    if is_non_nullable_null(v, ap_s) {
                        // `DeepCopyJSONValue(nil)` is nil: a missing default leaves the null.
                        if let Some(def) = ap_s.and_then(default_value) {
                            *v = def.clone();
                        }
                    }
                    default(v, ap_s);
                }
            }
        }
        Value::Array(items) => {
            let item_s = s.items.as_deref();
            for item in items {
                if is_non_nullable_null(item, item_s) {
                    if let Some(def) = item_s.and_then(default_value) {
                        *item = def.clone();
                    }
                }
                default(item, item_s);
            }
        }
        // scalars, do nothing
        _ => {}
    }
}

/// `prop.Default.Object` when it is non-nil. A JSON `null` default is the
/// nil `JSON{Object: nil}` upstream.
fn default_value(s: &Structural) -> Option<&Value> {
    s.generic.default.as_ref().filter(|v| !v.is_null())
}

/// `isNonNullableNull` (:25-28): returns true if the item is nil AND it is
/// not nullable.
fn is_non_nullable_null(x: &Value, s: Option<&Structural>) -> bool {
    x.is_null() && s.is_some_and(|s| !s.generic.nullable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::structural::{Generic, StructuralOrBool};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn d(default: Value) -> Structural {
        Structural {
            generic: Generic {
                default: Some(default),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn gen(default: Option<Value>, nullable: bool) -> Structural {
        Structural {
            generic: Generic {
                default,
                nullable,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn props(v: Vec<(&str, Structural)>) -> BTreeMap<String, Structural> {
        v.into_iter().map(|(k, s)| (k.to_string(), s)).collect()
    }

    fn items(s: Structural) -> Option<Box<Structural>> {
        Some(Box::new(s))
    }

    fn ap(s: Structural) -> Option<Box<StructuralOrBool>> {
        Some(Box::new(StructuralOrBool {
            structural: Some(s),
            bool_: false,
        }))
    }

    fn run(mut x: Value, s: Option<Structural>, expected: Value) {
        default(&mut x, s.as_ref());
        assert_eq!(x, expected);
    }

    /// algorithm_test.go `TestDefault` :30-247.
    #[test]
    fn upstream_cases() {
        // "empty"
        run(json!(null), None, json!(null));
        // "scalar"
        run(json!(4), Some(d(json!("foo"))), json!(4));
        // "scalar array"
        run(
            json!([1, 2]),
            Some(Structural {
                items: items(d(json!("foo"))),
                ..Default::default()
            }),
            json!([1, 2]),
        );
        // "object array"
        run(
            json!([{"a":1},{"b":1},{"c":1}]),
            Some(Structural {
                items: items(Structural {
                    properties: props(vec![
                        ("a", d(json!("A"))),
                        ("b", d(json!("B"))),
                        ("c", d(json!("C"))),
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            json!([{"a":1,"b":"B","c":"C"},{"a":"A","b":1,"c":"C"},{"a":"A","b":"B","c":1}]),
        );
        // "object array object"
        run(
            json!({"array":[{"a":1},{"b":2}],"object":{"a":1},"additionalProperties":{"x":{"a":1},"y":{"b":2}}}),
            Some(Structural {
                properties: props(vec![
                    (
                        "array",
                        Structural {
                            items: items(Structural {
                                properties: props(vec![("a", d(json!("A"))), ("b", d(json!("B")))]),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    (
                        "object",
                        Structural {
                            properties: props(vec![("a", d(json!("N"))), ("b", d(json!("O")))]),
                            ..Default::default()
                        },
                    ),
                    (
                        "additionalProperties",
                        Structural {
                            additional_properties: ap(Structural {
                                properties: props(vec![
                                    ("a", d(json!("alpha"))),
                                    ("b", d(json!("beta"))),
                                ]),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    ("foo", d(json!("bar"))),
                ]),
                ..Default::default()
            }),
            json!({"array":[{"a":1,"b":"B"},{"a":"A","b":2}],"object":{"a":1,"b":"O"},"additionalProperties":{"x":{"a":1,"b":"beta"},"y":{"a":"alpha","b":2}},"foo":"bar"}),
        );
        // "empty and null"
        run(
            json!([{},{"a":1},{"a":0},{"a":0.0},{"a":""},{"a":null},{"a":[]},{"a":{}}]),
            Some(Structural {
                items: items(Structural {
                    properties: props(vec![("a", d(json!("A")))]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            json!([{"a":"A"},{"a":1},{"a":0},{"a":0.0},{"a":""},{"a":"A"},{"a":[]},{"a":{}}]),
        );
        // "null in nullable list"
        run(
            json!([null]),
            Some(Structural {
                generic: Generic {
                    nullable: true,
                    ..Default::default()
                },
                items: items(Structural {
                    properties: props(vec![("a", d(json!("A")))]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            json!([null]),
        );
        // "null in non-nullable list"
        run(
            json!([null]),
            Some(Structural {
                items: items(d(json!("A"))),
                ..Default::default()
            }),
            json!(["A"]),
        );
        // "null in nullable object"
        run(
            json!({"a": null}),
            Some(Structural {
                properties: props(vec![("a", gen(Some(json!("A")), true))]),
                ..Default::default()
            }),
            json!({"a": null}),
        );
        // "null in non-nullable object"
        run(
            json!({"a": null}),
            Some(Structural {
                properties: props(vec![("a", gen(Some(json!("A")), false))]),
                ..Default::default()
            }),
            json!({"a": "A"}),
        );
        // "null in nullable object with additionalProperties"
        run(
            json!({"a": null}),
            Some(Structural {
                additional_properties: ap(gen(Some(json!("A")), true)),
                ..Default::default()
            }),
            json!({"a": null}),
        );
        // "null in non-nullable object with additionalProperties"
        run(
            json!({"a": null}),
            Some(Structural {
                additional_properties: ap(gen(Some(json!("A")), false)),
                ..Default::default()
            }),
            json!({"a": "A"}),
        );
        // "null unknown field"
        run(
            json!({"a": null}),
            Some(Structural {
                additional_properties: Some(Box::new(StructuralOrBool {
                    structural: None,
                    bool_: true,
                })),
                ..Default::default()
            }),
            json!({"a": null}),
        );
    }
}
