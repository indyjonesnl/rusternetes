//! The `EquivalentResourceMapper` and `NamespaceLister` the policy `Matcher`
//! needs (#2909, part of #2886).
//!
//! Ported from:
//! - `staging/src/k8s.io/apimachinery/pkg/runtime/mapper.go`
//!   (`equivalentResourceRegistry`)
//! - `staging/src/k8s.io/apiserver/pkg/endpoints/installer.go:1127`
//!   (`RegisterKindFor`, fed from the served resources)
//!
//! Tests are ported from `runtime/mapper_test.go` (`TestResourceMapper`).

// Not called from the request path yet (the MutatingAdmissionPolicy plugin is
// the rest of #2731); the bin target compiles `admission` separately.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
use rusternetes_common::resources::Namespace;
use rusternetes_common::Result;
use rusternetes_storage::Storage;

use super::policy_matching::{EquivalentResourceMapper, NamespaceLister};

type KeyFunc = Box<dyn Fn(&str, &str) -> String + Send + Sync>;
type Gr = (String, String);
type Gvr = (String, String, String);

#[derive(Default)]
struct Inner {
    resources: HashMap<String, HashMap<String, Vec<GroupVersionResource>>>,
    kinds: HashMap<Gvr, HashMap<String, GroupVersionKind>>,
    keys: HashMap<Gr, String>,
}

/// `runtime.EquivalentResourceRegistry`.
pub struct EquivalentResourceRegistry {
    key_func: Option<KeyFunc>,
    inner: RwLock<Inner>,
}

impl EquivalentResourceRegistry {
    /// `NewEquivalentResourceRegistry`: all versions of a GroupResource are
    /// equivalent.
    pub fn new() -> Self {
        Self {
            key_func: None,
            inner: RwLock::new(Inner::default()),
        }
    }

    /// `NewEquivalentResourceRegistryWithIdentity`; `key_func(group,
    /// resource)` returning `""` falls back to `group/resource`.
    pub fn with_identity(key_func: impl Fn(&str, &str) -> String + Send + Sync + 'static) -> Self {
        Self {
            key_func: Some(Box::new(key_func)),
            inner: RwLock::new(Inner::default()),
        }
    }

    /// `RegisterKindFor` (mapper.go): upstream appends without de-duplicating.
    pub fn register_kind_for(
        &self,
        resource: GroupVersionResource,
        subresource: &str,
        kind: GroupVersionKind,
    ) {
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        inner
            .kinds
            .entry((
                resource.group.clone(),
                resource.version.clone(),
                resource.resource.clone(),
            ))
            .or_default()
            .insert(subresource.to_string(), kind);

        // get the shared key of the parent resource
        let gr = (resource.group.clone(), resource.resource.clone());
        let mut key = self
            .key_func
            .as_ref()
            .map(|f| f(&gr.0, &gr.1))
            .unwrap_or_default();
        if key.is_empty() {
            // schema.GroupResource.String()
            key = if gr.0.is_empty() {
                gr.1.clone()
            } else {
                format!("{}.{}", gr.1, gr.0)
            };
        }
        inner.keys.insert(gr, key.clone());
        inner
            .resources
            .entry(key)
            .or_default()
            .entry(subresource.to_string())
            .or_default()
            .push(resource);
    }

    /// A registry holding every built-in resource this server serves, as
    /// installer.go:1127 registers them (`RegisterKindFor` per resource and
    /// subresource), with the default identity: all served versions of a
    /// GroupResource are equivalent. Upstream's identity is the storage
    /// prefix (server/config.go:734-745); only the `extensions` group shares a
    /// prefix across groups upstream, and it is not served here.
    pub fn from_discovery() -> Self {
        let reg = Self::new();
        for k in rusternetes_discovery::registered_kinds() {
            reg.register_kind_for(
                GroupVersionResource {
                    group: k.group,
                    version: k.version,
                    resource: k.resource,
                },
                &k.subresource,
                GroupVersionKind {
                    group: k.kind_group,
                    version: k.kind_version,
                    kind: k.kind,
                },
            );
        }
        reg
    }
}

impl Default for EquivalentResourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl EquivalentResourceMapper for EquivalentResourceRegistry {
    fn equivalent_resources_for(
        &self,
        resource: &GroupVersionResource,
        subresource: &str,
    ) -> Vec<GroupVersionResource> {
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        inner
            .keys
            .get(&(resource.group.clone(), resource.resource.clone()))
            .and_then(|key| inner.resources.get(key))
            .and_then(|subs| subs.get(subresource))
            .cloned()
            .unwrap_or_default()
    }

    fn kind_for(&self, resource: &GroupVersionResource, subresource: &str) -> GroupVersionKind {
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        inner
            .kinds
            .get(&(
                resource.group.clone(),
                resource.version.clone(),
                resource.resource.clone(),
            ))
            .and_then(|subs| subs.get(subresource))
            .cloned()
            .unwrap_or(GroupVersionKind {
                group: String::new(),
                version: String::new(),
                kind: String::new(),
            })
    }
}

/// `NamespaceLister` over storage (the namespace informer's lister upstream).
pub struct StorageNamespaceLister<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage> StorageNamespaceLister<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<S: Storage> NamespaceLister for StorageNamespaceLister<S> {
    /// `NamespaceLister.Get`: a missing namespace is `NotFound`.
    async fn namespace_labels(&self, name: &str) -> Result<HashMap<String, String>> {
        let ns: Namespace = self
            .storage
            .get(&rusternetes_storage::build_key("namespaces", None, name))
            .await?;
        Ok(ns.metadata.labels.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::Error;
    use rusternetes_storage::MemoryStorage;

    fn gvr(g: &str, v: &str, r: &str) -> GroupVersionResource {
        GroupVersionResource {
            group: g.into(),
            version: v.into(),
            resource: r.into(),
        }
    }
    fn gvk(g: &str, v: &str, k: &str) -> GroupVersionKind {
        GroupVersionKind {
            group: g.into(),
            version: v.into(),
            kind: k.into(),
        }
    }

    fn kinds_to_register() -> Vec<(GroupVersionResource, &'static str, GroupVersionKind)> {
        vec![
            (gvr("", "v1", "pods"), "", gvk("", "v1", "Pod")),
            (gvr("", "v1", "pods"), "status", gvk("", "v1", "Pod")),
            (
                gvr("apps", "v1", "deployments"),
                "",
                gvk("apps", "v1", "Deployment"),
            ),
            (
                gvr("apps", "v1beta1", "deployments"),
                "",
                gvk("apps", "v1beta1", "Deployment"),
            ),
            (
                gvr("apps", "v1alpha1", "deployments"),
                "",
                gvk("apps", "v1alpha1", "Deployment"),
            ),
            (
                gvr("extensions", "v1beta1", "deployments"),
                "",
                gvk("extensions", "v1beta1", "Deployment"),
            ),
            (
                gvr("apps", "v1", "deployments"),
                "scale",
                gvk("", "", "Scale"),
            ),
            (
                gvr("apps", "v1beta1", "deployments"),
                "scale",
                gvk("", "", "Scale"),
            ),
            (
                gvr("extensions", "v1beta1", "deployments"),
                "scale",
                gvk("", "", "Scale"),
            ),
            (
                gvr("apps", "v1", "deployments"),
                "status",
                gvk("apps", "v1", "Deployment"),
            ),
            (
                gvr("apps", "v1beta1", "deployments"),
                "status",
                gvk("apps", "v1beta1", "Deployment"),
            ),
            (
                gvr("extensions", "v1beta1", "deployments"),
                "status",
                gvk("extensions", "v1beta1", "Deployment"),
            ),
        ]
    }

    fn d(g: &str, v: &str) -> GroupVersionResource {
        gvr(g, v, "deployments")
    }

    /// mapper_test.go `TestResourceMapper`.
    #[test]
    fn resource_mapper() {
        type Id = Option<fn(&str, &str) -> String>;
        type Case = (
            &'static str,
            Id,
            Vec<GroupVersionResource>,
            Vec<GroupVersionResource>,
            Vec<GroupVersionResource>,
        );
        let cases: Vec<Case> = vec![
            (
                "no identityfunc",
                None,
                vec![d("apps", "v1"), d("apps", "v1beta1"), d("apps", "v1alpha1")],
                vec![d("apps", "v1"), d("apps", "v1beta1")],
                vec![d("apps", "v1"), d("apps", "v1beta1")],
            ),
            (
                "empty identityfunc",
                Some(|_, _| String::new()),
                vec![d("apps", "v1"), d("apps", "v1beta1"), d("apps", "v1alpha1")],
                vec![d("apps", "v1"), d("apps", "v1beta1")],
                vec![d("apps", "v1"), d("apps", "v1beta1")],
            ),
            (
                "common identityfunc",
                Some(|_, _| "x".to_string()),
                vec![
                    gvr("", "v1", "pods"),
                    d("apps", "v1"),
                    d("apps", "v1beta1"),
                    d("apps", "v1alpha1"),
                    d("extensions", "v1beta1"),
                ],
                vec![
                    d("apps", "v1"),
                    d("apps", "v1beta1"),
                    d("extensions", "v1beta1"),
                ],
                vec![
                    gvr("", "v1", "pods"),
                    d("apps", "v1"),
                    d("apps", "v1beta1"),
                    d("extensions", "v1beta1"),
                ],
            ),
            (
                "colocated deployments",
                Some(|_, r| {
                    if r == "deployments" {
                        "deployments".into()
                    } else {
                        String::new()
                    }
                }),
                vec![
                    d("apps", "v1"),
                    d("apps", "v1beta1"),
                    d("apps", "v1alpha1"),
                    d("extensions", "v1beta1"),
                ],
                vec![
                    d("apps", "v1"),
                    d("apps", "v1beta1"),
                    d("extensions", "v1beta1"),
                ],
                vec![
                    d("apps", "v1"),
                    d("apps", "v1beta1"),
                    d("extensions", "v1beta1"),
                ],
            ),
        ];
        for (name, identity, main, scale, status) in cases {
            let reg = match identity {
                Some(f) => EquivalentResourceRegistry::with_identity(f),
                None => EquivalentResourceRegistry::new(),
            };
            for (r, sub, k) in kinds_to_register() {
                reg.register_kind_for(r, sub, k);
            }
            for (r, sub, k) in kinds_to_register() {
                assert_eq!(reg.kind_for(&r, sub), k, "{name}: KindFor {r:?}/{sub}");
            }
            let v1 = d("apps", "v1");
            assert_eq!(reg.equivalent_resources_for(&v1, ""), main, "{name}");
            assert_eq!(reg.equivalent_resources_for(&v1, "scale"), scale, "{name}");
            assert_eq!(
                reg.equivalent_resources_for(&v1, "status"),
                status,
                "{name}"
            );
        }
    }

    #[test]
    fn unregistered_is_empty_and_unknown_kind() {
        let reg = EquivalentResourceRegistry::new();
        let r = gvr("apps", "v1", "nope");
        assert!(reg.equivalent_resources_for(&r, "").is_empty());
        assert_eq!(reg.kind_for(&r, ""), gvk("", "", ""));
    }

    #[test]
    fn from_discovery_covers_served_resources() {
        let reg = EquivalentResourceRegistry::from_discovery();
        assert_eq!(
            reg.kind_for(&d("apps", "v1"), ""),
            gvk("apps", "v1", "Deployment")
        );
        assert_eq!(
            reg.kind_for(&gvr("", "v1", "pods"), "status"),
            gvk("", "v1", "Pod")
        );
        // Both served versions of HPAs are equivalent.
        let eq =
            reg.equivalent_resources_for(&gvr("autoscaling", "v2", "horizontalpodautoscalers"), "");
        assert!(eq.contains(&gvr("autoscaling", "v1", "horizontalpodautoscalers")));
        assert!(eq.contains(&gvr("autoscaling", "v2", "horizontalpodautoscalers")));
    }

    #[tokio::test]
    async fn namespace_lister_reads_labels_and_reports_missing() {
        let storage = Arc::new(MemoryStorage::new());
        let mut ns = Namespace::new("team-a");
        ns.metadata.labels = Some(HashMap::from([("env".to_string(), "prod".to_string())]));
        storage
            .create(
                &rusternetes_storage::build_key("namespaces", None, "team-a"),
                &ns,
            )
            .await
            .unwrap();
        let lister = StorageNamespaceLister::new(storage);
        assert_eq!(
            lister
                .namespace_labels("team-a")
                .await
                .unwrap()
                .get("env")
                .map(String::as_str),
            Some("prod")
        );
        assert!(matches!(
            lister.namespace_labels("missing").await,
            Err(Error::NotFound(_))
        ));
    }
}
