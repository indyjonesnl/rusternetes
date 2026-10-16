//! Port of the schema side of strategic merge patch: apimachinery
//! `pkg/util/strategicpatch/meta.go` (`LookupPatchMeta`, `PatchMeta`,
//! `PatchMetaFromStruct`) over
//! `third_party/forked/golang/json/fields.go:LookupPatchMetadataForStruct`.
//!
//! The apiserver's `smpPatcher` builds `NewPatchMetaFromStruct(versionedObj)`
//! (apiserver `endpoints/handlers/patch.go`), so list strategy and merge key
//! come from the Go struct tags `patchStrategy` / `patchMergeKey`. We have no
//! Go structs; the upstream swagger spec is generated from the same tags
//! (`x-kubernetes-patch-strategy`, `x-kubernetes-patch-merge-key`), and
//! `scripts/gen-patch-meta.py` flattens it into `patch_meta.json`: one entry
//! per definition (struct), one per field, carrying the field's type and its
//! tags.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

/// `strategicpatch.PatchMeta`: the tags of one field.
#[derive(Debug, Clone, Default)]
pub(crate) struct PatchMeta {
    /// `GetPatchStrategies`: `strings.Split(tag, ",")`, so an untagged field
    /// is `[""]` (fields.go:64).
    pub(crate) strategies: Vec<String>,
    /// `GetPatchMergeKey`.
    pub(crate) merge_key: String,
}

/// The Go type of a field, as far as the lookups distinguish it.
#[derive(Debug, Clone)]
enum Ty {
    /// A struct (definition name).
    Struct(String),
    /// `[]T`.
    Slice(Box<Ty>),
    /// `map[string]T`.
    Map(Box<Ty>),
    /// Anything else; carries the `reflect.Kind` string.
    Prim(String),
}

impl Ty {
    fn parse(s: &str) -> Ty {
        match s.split_once(':') {
            Some(("S", d)) => Ty::Struct(d.to_string()),
            Some(("L", e)) => Ty::Slice(Box::new(Ty::parse(e))),
            Some(("M", e)) => Ty::Map(Box::new(Ty::parse(e))),
            Some((_, k)) => Ty::Prim(k.to_string()),
            None => Ty::Prim(s.to_string()),
        }
    }

    /// `reflect.Type.Kind().String()`.
    fn kind(&self) -> String {
        match self {
            Ty::Struct(_) => "struct".to_string(),
            Ty::Slice(_) => "slice".to_string(),
            Ty::Map(_) => "map".to_string(),
            Ty::Prim(k) => k.clone(),
        }
    }
}

#[derive(Debug)]
struct Field {
    name: String,
    ty: Ty,
    meta: PatchMeta,
}

/// The struct definitions the lookups walk.
#[derive(Debug, Default)]
pub(crate) struct Defs {
    defs: HashMap<String, Vec<Field>>,
    gvk: HashMap<String, String>,
}

impl Defs {
    /// Parse the `patch_meta.json` layout (see `scripts/gen-patch-meta.py`).
    pub(crate) fn from_json(json: &str) -> Defs {
        let v: Value = serde_json::from_str(json).expect("patch_meta.json is valid JSON");
        let mut out = Defs::default();
        if let Some(gvk) = v.get("gvk").and_then(Value::as_object) {
            for (k, d) in gvk {
                if let Some(d) = d.as_str() {
                    out.gvk.insert(k.clone(), d.to_string());
                }
            }
        }
        if let Some(defs) = v.get("defs").and_then(Value::as_object) {
            for (name, fields) in defs {
                let mut list = Vec::new();
                for (f, spec) in fields.as_object().into_iter().flatten() {
                    let s = |i: usize| spec.get(i).and_then(Value::as_str).unwrap_or("");
                    list.push(Field {
                        name: f.clone(),
                        ty: Ty::parse(s(0)),
                        meta: PatchMeta {
                            strategies: s(1).split(',').map(str::to_string).collect(),
                            merge_key: s(2).to_string(),
                        },
                    });
                }
                out.defs.insert(name.clone(), list);
            }
        }
        out
    }

    fn embedded() -> &'static Arc<Defs> {
        static DEFS: OnceLock<Arc<Defs>> = OnceLock::new();
        DEFS.get_or_init(|| Arc::new(Defs::from_json(include_str!("patch_meta.json"))))
    }
}

/// `LookupPatchMeta`: the schema of one value, walked key by key.
#[derive(Debug, Clone)]
pub struct PatchSchema {
    defs: Arc<Defs>,
    ty: Ty,
}

impl PatchSchema {
    /// The schema of the served kind, `NewPatchMetaFromStruct(&Kind{})`.
    /// `None` for a kind without a built-in definition (nothing to look up).
    pub fn for_kind(group: &str, version: &str, kind: &str) -> Option<PatchSchema> {
        let defs = Defs::embedded();
        let def = defs.gvk.get(&format!("{group}/{version}/{kind}"))?;
        Some(PatchSchema {
            defs: defs.clone(),
            ty: Ty::Struct(def.clone()),
        })
    }

    /// The schema named by the object's own `apiVersion`/`kind`.
    pub fn for_object(obj: &Value) -> Option<PatchSchema> {
        let api_version = obj.get("apiVersion")?.as_str()?;
        let kind = obj.get("kind")?.as_str()?;
        let (group, version) = api_version.split_once('/').unwrap_or(("", api_version));
        PatchSchema::for_kind(group, version, kind)
    }

    #[cfg(test)]
    pub(crate) fn for_test(json: &str, root: &str) -> PatchSchema {
        PatchSchema {
            defs: Arc::new(Defs::from_json(json)),
            ty: Ty::Struct(root.to_string()),
        }
    }

    /// `LookupPatchMetadataForStruct` (meta.go:78-88 over fields.go:29-70).
    pub(crate) fn lookup_struct(&self, key: &str) -> Result<(PatchSchema, PatchMeta), String> {
        let (ty, meta) = self.lookup_field(key)?;
        Ok((
            PatchSchema {
                defs: self.defs.clone(),
                ty,
            },
            meta,
        ))
    }

    /// `LookupPatchMetadataForSlice` (meta.go:90-125).
    pub(crate) fn lookup_slice(&self, key: &str) -> Result<(PatchSchema, PatchMeta), String> {
        let (ty, meta) = self.lookup_field(key)?;
        match ty {
            Ty::Slice(elem) => {
                if matches!(*elem, Ty::Slice(_)) {
                    return Err("unexpected slice of slice".to_string());
                }
                Ok((
                    PatchSchema {
                        defs: self.defs.clone(),
                        ty: *elem,
                    },
                    meta,
                ))
            }
            other => Err(format!(
                "expected slice or array type, but got: {}",
                other.kind()
            )),
        }
    }

    /// `PatchMetaFromStruct.Name` (meta.go:127-129): the Go kind.
    pub(crate) fn name(&self) -> String {
        self.ty.kind()
    }

    fn lookup_field(&self, key: &str) -> Result<(Ty, PatchMeta), String> {
        let Ty::Struct(def) = &self.ty else {
            return Err(format!(
                "merging an object in json but data type is not struct, instead is: {}",
                self.ty.kind()
            ));
        };
        let fields = self.defs.defs.get(def).map(Vec::as_slice).unwrap_or(&[]);
        // fields.go:44-53: an exact name wins, else the first
        // case-insensitive match.
        let found = fields.iter().find(|f| f.name == key).or_else(|| {
            let k = key.to_lowercase();
            fields.iter().find(|f| f.name.to_lowercase() == k)
        });
        match found {
            Some(f) => Ok((f.ty.clone(), f.meta.clone())),
            None => Err(format!(
                "unable to find api field in struct {} for the json field {:?}",
                def.rsplit('.').next().unwrap_or(def),
                key
            )),
        }
    }
}
