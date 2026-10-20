//! The `typeConverter` of the built-in kinds (#2734).
//!
//! Upstream builds one `TypeConverter` from the OpenAPI models of every served
//! type (`managedfields.NewTypeConverter`, typeconverter.go:62-81, fed by
//! `kube-apiserver`'s `openapi.GetOpenAPIDefinitions`). This tree vendors the
//! release-1.35 OpenAPI v3 `components.schemas` for `apps/v1` and `batch/v1`
//! (`handlers::openapi::upstream_v3_schemas`), which also carry the `core/v1`
//! types their pod templates reference and `ObjectMeta`. A kind with no
//! vendored definition gets `None`, and the field manager falls back to its
//! name-driven table.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use rusternetes_common::fieldmanager::schema::Type;

/// `Pod`'s root is not vendored, but its `metadata` and `spec` types are;
/// `status` (and anything else) stays name-driven.
const POD_FIELDS: &[(&str, &str)] = &[
    (
        "metadata",
        "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
    ),
    ("spec", "io.k8s.api.core.v1.PodSpec"),
];

/// The schema of `kind` in `api_version` (`group/version`, or `version` for
/// the core group), built once.
pub fn builtin(api_version: &str, kind: &str) -> Option<Arc<Type>> {
    type Cache = Mutex<HashMap<(String, String), Option<Arc<Type>>>>;
    static CACHE: OnceLock<Cache> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    let key = (api_version.to_string(), kind.to_string());
    if let Some(hit) = cache.lock().ok()?.get(&key) {
        return hit.clone();
    }
    let built = build(api_version, kind).map(Arc::new);
    cache.lock().ok()?.insert(key, built.clone());
    built
}

fn build(api_version: &str, kind: &str) -> Option<Type> {
    use crate::handlers::openapi::upstream_v3_schemas;
    match api_version {
        "v1" if kind == "Pod" => {
            // The core types are vendored inside the apps/v1 document.
            let schemas = upstream_v3_schemas("apps", "v1")?;
            Type::root_of(schemas, POD_FIELDS)
        }
        "apps/v1" | "batch/v1" => {
            let (group, version) = api_version.split_once('/')?;
            let schemas = upstream_v3_schemas(group, version)?;
            Type::from_openapi(schemas, &format!("io.k8s.api.{group}.{version}.{kind}"))
        }
        _ => None,
    }
}
