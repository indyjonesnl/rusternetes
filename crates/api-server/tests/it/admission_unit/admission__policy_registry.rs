//! Unit tests moved from `src/admission/policy_registry.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::admission::policy_matching::{
        EquivalentResourceMapper, NamespaceLister,
    };
    pub use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
    pub use rusternetes_common::resources::Namespace;
    pub use rusternetes_common::Result;
    pub use rusternetes_storage::Storage;
    pub use std::collections::HashMap;
    pub use std::sync::{Arc, RwLock};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::policy_registry::*;
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
