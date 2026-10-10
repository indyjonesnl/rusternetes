//! The server-side collaborators the MutatingAdmissionPolicy source and plugin
//! need at startup (#3013, part of #2886): a `meta.RESTMapper` over the
//! served resources, a storage-backed `NamespaceObjects`, registry-backed
//! `TypeConverters`, and the `HookSource` adapter over a `PolicySource`.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/`:
//! - `generic/plugin.go` `ValidateInitialization` (:100-170): the plugin is
//!   handed the REST mapper (`SetRESTMapper`), the namespace informer
//!   (`SetExternalKubeInformerFactory`) and the object-interfaces
//!   (`SetObjectInterfaces`), then `source.Run` is started.
//! - `mutating/dispatcher.go` `dispatchOne` (:223-269): a kind with no type
//!   converter is a 503 `Resource kind ... not found`.

#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
use rusternetes_common::resources::Namespace;
use rusternetes_common::Error;
use rusternetes_storage::Storage;
use serde_json::Value;

use super::policy_dispatch::ParamScope;
use super::policy_mutating::{NamespaceObjects, TypeConverters};
use super::policy_source::{RestMapper, RestMapping};

/// `meta.RESTMapper` over the resources this server serves.
pub struct DiscoveryRestMapper;

impl RestMapper for DiscoveryRestMapper {
    /// `RESTMapping(GroupKind, version)`: a kind that is not served is
    /// upstream's `NoKindMatchError`.
    fn rest_mapping(&self, group: &str, kind: &str, version: &str) -> Result<RestMapping, String> {
        let (resource, namespaced) =
            rusternetes_discovery::resolve_kind_to_resource_and_scope(group, version, kind)
                .ok_or_else(|| {
                    format!(
                        "no matches for kind \"{kind}\" in version \"{}\"",
                        if group.is_empty() {
                            version.to_string()
                        } else {
                            format!("{group}/{version}")
                        }
                    )
                })?;
        Ok(RestMapping {
            resource: GroupVersionResource {
                group: group.to_string(),
                version: version.to_string(),
                resource,
            },
            scope: if namespaced {
                ParamScope::Namespace
            } else {
                ParamScope::Root
            },
        })
    }
}

/// `matcher.GetNamespace` over storage.
pub struct StorageNamespaceObjects<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage> StorageNamespaceObjects<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<S: Storage> NamespaceObjects for StorageNamespaceObjects<S> {
    async fn get_namespace(&self, name: &str) -> Result<Value, Error> {
        let ns: Namespace = self
            .storage
            .get(&rusternetes_storage::build_key("namespaces", None, name))
            .await?;
        serde_json::to_value(ns).map_err(|e| Error::Internal(e.to_string()))
    }
}

/// `TypeConverterManager.GetTypeConverter` for every served kind.
pub struct RegistryTypeConverters;

impl TypeConverters for RegistryTypeConverters {
    fn has_type_converter(&self, kind: &GroupVersionKind) -> bool {
        rusternetes_discovery::resolve_kind_to_resource(&kind.group, &kind.version, &kind.kind)
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::Namespace;
    use rusternetes_storage::MemoryStorage;

    fn gvk(g: &str, v: &str, k: &str) -> GroupVersionKind {
        GroupVersionKind {
            group: g.into(),
            version: v.into(),
            kind: k.into(),
        }
    }

    #[test]
    fn rest_mapper_resolves_namespaced_and_cluster_kinds() {
        let m = DiscoveryRestMapper;
        let cm = m.rest_mapping("", "ConfigMap", "v1").unwrap();
        assert_eq!(cm.resource.resource, "configmaps");
        assert_eq!(cm.resource.version, "v1");
        assert_eq!(cm.scope, ParamScope::Namespace);
        let ns = m.rest_mapping("", "Namespace", "v1").unwrap();
        assert_eq!(ns.resource.resource, "namespaces");
        assert_eq!(ns.scope, ParamScope::Root);
        let d = m.rest_mapping("apps", "Deployment", "v1").unwrap();
        assert_eq!(d.resource.group, "apps");
        assert_eq!(d.resource.resource, "deployments");
    }

    #[test]
    fn rest_mapper_unknown_kind_is_an_error() {
        // NoKindMatchError.
        assert!(DiscoveryRestMapper
            .rest_mapping("example.com", "Widget", "v1")
            .is_err());
        assert!(DiscoveryRestMapper.rest_mapping("", "Nope", "v1").is_err());
    }

    #[tokio::test]
    async fn namespace_objects_returns_the_full_namespace() {
        let storage = Arc::new(MemoryStorage::new());
        let mut ns = Namespace::new("team-a");
        ns.metadata.labels = Some([("env".to_string(), "dev".to_string())].into());
        storage
            .create(
                &rusternetes_storage::build_key("namespaces", None, "team-a"),
                &ns,
            )
            .await
            .unwrap();
        let got = StorageNamespaceObjects::new(storage)
            .get_namespace("team-a")
            .await
            .unwrap();
        assert_eq!(got["metadata"]["name"], "team-a");
        assert_eq!(got["metadata"]["labels"]["env"], "dev");
    }

    #[tokio::test]
    async fn namespace_objects_missing_is_not_found() {
        let storage = Arc::new(MemoryStorage::new());
        let err = StorageNamespaceObjects::new(storage)
            .get_namespace("absent")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err:?}");
    }

    #[test]
    fn type_converters_cover_served_kinds_only() {
        let c = RegistryTypeConverters;
        assert!(c.has_type_converter(&gvk("", "v1", "ConfigMap")));
        assert!(c.has_type_converter(&gvk("apps", "v1", "Deployment")));
        assert!(!c.has_type_converter(&gvk("example.com", "v1", "Widget")));
    }
}
