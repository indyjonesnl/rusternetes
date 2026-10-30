//! Unit tests moved from `src/admission/owner_references_permission_enforcement.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::handlers::custom_metrics::ServedRestMapper;
    pub use rusternetes_common::auth::UserInfo;
    pub use rusternetes_common::authz::{Authorizer, Opinion, RequestAttributes};
    pub use rusternetes_common::types::{ObjectMeta, OwnerReference};
    pub use rusternetes_discovery::restmapper::{Gk, MapperError, RestMapping, Scope};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
}

#[allow(clippy::cloned_ref_to_slice_refs)]
mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::owner_references_permission_enforcement::*;
    use rusternetes_common::authz::Decision;
    use rusternetes_discovery::restmapper::{
        new_discovery_rest_mapper, ApiGroupResources, ApiResource, PriorityRestMapper,
    };

    /// `fakeAuthorizer` (gc_admission_test.go:43-101).
    struct FakeAuthorizer;

    #[async_trait]
    impl Authorizer for FakeAuthorizer {
        async fn authorize(&self, a: &RequestAttributes) -> rusternetes_common::Result<Decision> {
            let no_opinion = || Ok(Decision::Deny(String::new()));
            let sub = a.subresource.as_deref().unwrap_or_default();
            let user = a.user.username.as_str();
            let all_denied = user == "non-deleter";
            let resource_denied = match user {
                "non-pod-deleter" => Some("pods"),
                "non-rc-deleter" => Some("replicationcontrollers"),
                "non-node-deleter" => Some("nodes"),
                _ => None,
            };
            if all_denied && a.verb == "delete" {
                return no_opinion();
            }
            if all_denied && a.verb == "update" && sub == "finalizers" {
                return no_opinion();
            }
            if let Some(res) = resource_denied {
                if a.verb == "delete" && a.resource == res {
                    return no_opinion();
                }
                if a.verb == "update" && a.resource == res && sub == "finalizers" {
                    return no_opinion();
                }
            }
            if (all_denied || resource_denied.is_some()) && a.api_group == "*" && a.resource == "*"
            {
                // this user does not have full rights
                return no_opinion();
            }
            Ok(Decision::Allow)
        }

        async fn get_user_rules(
            &self,
            _: &UserInfo,
            _: &str,
        ) -> rusternetes_common::Result<(
            Vec<rusternetes_common::resources::ResourceRule>,
            Vec<rusternetes_common::resources::NonResourceRule>,
        )> {
            Ok((Vec::new(), Vec::new()))
        }
    }

    /// The `fakediscovery` resource lists of `newGCPermissionsEnforcement`.
    fn fake_mapper() -> PriorityRestMapper {
        let res = |name: &str, singular: &str, namespaced: bool, kind: &str| ApiResource {
            name: name.to_string(),
            singular_name: singular.to_string(),
            namespaced,
            kind: kind.to_string(),
        };
        let group = |name: &str, version: &str, resources: Vec<ApiResource>| ApiGroupResources {
            name: name.to_string(),
            versions: vec![version.to_string()],
            preferred_version: version.to_string(),
            versioned_resources: [(version.to_string(), resources)].into(),
        };
        new_discovery_rest_mapper(&[
            group(
                "",
                "v1",
                vec![
                    res("nodes", "node", false, "Node"),
                    res("pods", "pod", true, "Pod"),
                    res(
                        "replicationcontrollers",
                        "replicationcontroller",
                        true,
                        "ReplicationController",
                    ),
                ],
            ),
            group(
                "apps",
                "v1",
                vec![res("daemonsets", "daemonset", true, "DaemonSet")],
            ),
        ])
    }

    struct Fake(PriorityRestMapper);

    #[async_trait]
    impl OwnerRestMapper for Fake {
        async fn rest_mappings(
            &self,
            gk: &Gk,
            versions: &[&str],
        ) -> Result<Vec<RestMapping>, MapperError> {
            self.0.rest_mappings(gk, versions)
        }
    }

    /// `neverReturningRESTMapper`: the test fails if it is consulted.
    struct NeverReturning;

    #[async_trait]
    impl OwnerRestMapper for NeverReturning {
        async fn rest_mappings(&self, _: &Gk, _: &[&str]) -> Result<Vec<RestMapping>, MapperError> {
            panic!("test failed");
        }
    }

    fn plugin(mapper: Arc<dyn OwnerRestMapper>) -> OwnerReferencesPermissionEnforcement {
        OwnerReferencesPermissionEnforcement::new(Arc::new(FakeAuthorizer), mapper)
    }

    fn user(name: &str) -> UserInfo {
        UserInfo {
            username: name.to_string(),
            uid: String::new(),
            groups: Vec::new(),
            extra: Default::default(),
        }
    }

    fn meta(refs: &[OwnerReference]) -> ObjectMeta {
        let mut m = ObjectMeta::new("foo");
        m.owner_references = Some(refs.to_vec());
        m
    }

    async fn run(
        p: &OwnerReferencesPermissionEnforcement,
        username: &str,
        resource: &str,
        subresource: Option<&str>,
        old: Option<&ObjectMeta>,
        new: &ObjectMeta,
    ) -> Result<(), String> {
        let u = user(username);
        p.validate(
            &Attributes {
                user: &u,
                group: "",
                version: "v1",
                resource,
                subresource,
                namespace: Some("default"),
                name: "foo",
                create: old.is_none(),
            },
            new,
            old,
        )
        .await
    }

    fn named(name: &str) -> OwnerReference {
        OwnerReference::new("", "", name, "")
    }

    /// `TestGCAdmission`.
    #[tokio::test]
    async fn gc_admission() {
        let p = plugin(Arc::new(Fake(fake_mapper())));
        let none = meta(&[]);
        let one = meta(&[named("first")]);
        let two = meta(&[named("first"), named("second")]);
        // (name, user, resource, subresource, old, new, expect_cant_set)
        #[allow(clippy::type_complexity)]
        let cases: Vec<(
            &str,
            &str,
            &str,
            Option<&str>,
            Option<&ObjectMeta>,
            &ObjectMeta,
            bool,
        )> = vec![
            (
                "super-user, create, no objectref change",
                "super",
                "pods",
                None,
                None,
                &none,
                false,
            ),
            (
                "super-user, create, objectref change",
                "super",
                "pods",
                None,
                None,
                &one,
                false,
            ),
            (
                "non-deleter, create, no objectref change",
                "non-deleter",
                "pods",
                None,
                None,
                &none,
                false,
            ),
            (
                "non-deleter, create, objectref change",
                "non-deleter",
                "pods",
                None,
                None,
                &one,
                false,
            ),
            (
                "non-pod-deleter, create, no objectref change",
                "non-pod-deleter",
                "pods",
                None,
                None,
                &none,
                false,
            ),
            (
                "non-pod-deleter, create, objectref change",
                "non-pod-deleter",
                "pods",
                None,
                None,
                &one,
                false,
            ),
            (
                "non-pod-deleter, create, objectref change, but not a pod",
                "non-pod-deleter",
                "not-pods",
                None,
                None,
                &one,
                false,
            ),
            (
                "super-user, update, no objectref change",
                "super",
                "pods",
                None,
                Some(&none),
                &none,
                false,
            ),
            (
                "super-user, update, no objectref change two",
                "super",
                "pods",
                None,
                Some(&one),
                &one,
                false,
            ),
            (
                "super-user, update, objectref change",
                "super",
                "pods",
                None,
                Some(&none),
                &one,
                false,
            ),
            (
                "non-deleter, update, no objectref change",
                "non-deleter",
                "pods",
                None,
                Some(&none),
                &none,
                false,
            ),
            (
                "non-deleter, update, no objectref change two",
                "non-deleter",
                "pods",
                None,
                Some(&one),
                &one,
                false,
            ),
            (
                "non-deleter, update, objectref change",
                "non-deleter",
                "pods",
                None,
                Some(&none),
                &one,
                true,
            ),
            (
                "non-deleter, update, objectref change two",
                "non-deleter",
                "pods",
                None,
                Some(&one),
                &two,
                true,
            ),
            (
                "non-pod-deleter, update, no objectref change",
                "non-pod-deleter",
                "pods",
                None,
                Some(&none),
                &none,
                false,
            ),
            (
                "non-pod-deleter, update status, objectref change",
                "non-pod-deleter",
                "pods",
                Some("status"),
                Some(&none),
                &one,
                false,
            ),
            (
                "non-pod-deleter, update, objectref change",
                "non-pod-deleter",
                "pods",
                None,
                Some(&none),
                &one,
                true,
            ),
            (
                "non-pod-deleter, update, objectref change, but not a pod",
                "non-pod-deleter",
                "not-pods",
                None,
                Some(&none),
                &one,
                false,
            ),
        ];
        for (name, username, resource, sub, old, new, cant_set) in cases {
            let r = run(&p, username, resource, sub, old, new).await;
            if cant_set {
                assert!(
                    r.as_ref().is_err_and(
                        |e| e.contains("cannot set an ownerRef on a resource you can't delete")
                    ),
                    "{name}: {r:?}"
                );
            } else {
                assert_eq!(r, Ok(()), "{name}");
            }
        }
    }

    fn rc_ref(name: &str, block: Option<bool>) -> OwnerReference {
        let mut r = OwnerReference::new("v1", "ReplicationController", name, "");
        r.block_owner_deletion = block;
        r
    }

    fn node_ref(name: &str, block: Option<bool>) -> OwnerReference {
        let mut r = OwnerReference::new("v1", "Node", name, "");
        r.block_owner_deletion = block;
        r
    }

    fn ds_ref(block: bool) -> OwnerReference {
        let mut r = OwnerReference::new("apps/v1", "DaemonSet", "ds1", "");
        r.block_owner_deletion = Some(block);
        r
    }

    /// `TestBlockOwnerDeletionAdmission`.
    #[tokio::test]
    async fn block_owner_deletion_admission() {
        let t = Some(true);
        let f = Some(false);
        let block_rc1 = rc_ref("rc1", t);
        let block_rc2 = rc_ref("rc2", t);
        let not_block_rc1 = rc_ref("rc1", f);
        let not_block_rc2 = rc_ref("rc2", f);
        let nil_block_rc1 = rc_ref("rc1", None);
        let nil_block_rc2 = rc_ref("rc2", None);
        let block_ds1 = ds_ref(true);
        let not_block_ds1 = ds_ref(false);
        let block_node = node_ref("node1", t);
        let not_block_node = node_ref("node", f);
        let nil_block_node = node_ref("node", None);

        struct Case {
            name: &'static str,
            user: &'static str,
            old: Option<ObjectMeta>,
            new: ObjectMeta,
            hanging_mapper: bool,
            err: bool,
        }
        let c = |name, user, old: Option<&[OwnerReference]>, new: &[OwnerReference], err| Case {
            name,
            user,
            old: old.map(meta),
            new: meta(new),
            hanging_mapper: false,
            err,
        };
        let mut cases = vec![
            // cases for create
            c("super-user, create, no ownerReferences", "super", None, &[], false),
            c("super-user, create, all ownerReferences have blockOwnerDeletion=false", "super", None, &[not_block_rc1.clone(), not_block_rc2.clone()], false),
            c("super-user, create, some ownerReferences have blockOwnerDeletion=true", "super", None, &[block_rc1.clone(), block_rc2.clone(), block_node.clone()], false),
            c("non-rc-deleter, create, no ownerReferences", "non-rc-deleter", None, &[], false),
            c("non-rc-deleter, create, all ownerReferences have blockOwnerDeletion=false or nil", "non-rc-deleter", None, &[not_block_rc1.clone(), nil_block_rc2.clone()], false),
            c("non-node-deleter, create, all ownerReferences have blockOwnerDeletion=false", "non-node-deleter", None, &[not_block_node.clone()], false),
            c("non-rc-deleter, create, some ownerReferences have blockOwnerDeletion=true", "non-rc-deleter", None, &[block_rc1.clone(), not_block_rc2.clone()], true),
            c("non-rc-deleter, create, some ownerReferences have blockOwnerDeletion=true, but are pointing to daemonset", "non-rc-deleter", None, &[block_ds1.clone()], false),
            c("non-node-deleter, create, some ownerReferences have blockOwnerDeletion=true", "non-node-deleter", None, &[block_node.clone()], true),
            // cases are for update
            c("super-user, update, no ownerReferences change blockOwnerDeletion", "super", Some(&[nil_block_rc1.clone(), nil_block_node.clone()]), &[not_block_rc1.clone(), not_block_node.clone()], false),
            c("super-user, update, some ownerReferences change to blockOwnerDeletion=true", "super", Some(&[not_block_rc1.clone(), not_block_node.clone()]), &[block_rc1.clone(), block_node.clone()], false),
            c("super-user, update, add new ownerReferences with blockOwnerDeletion=true", "super", Some(&[]), &[block_rc1.clone(), block_node.clone()], false),
            c("non-rc-deleter, update, no ownerReferences change blockOwnerDeletion", "non-rc-deleter", Some(&[nil_block_rc1.clone()]), &[not_block_rc1.clone()], false),
            c("non-rc-deleter, update, some ownerReferences change from blockOwnerDeletion=false to true", "non-rc-deleter", Some(&[not_block_rc1.clone()]), &[block_rc1.clone()], true),
            c("non-rc-deleter, update, some ownerReferences change from blockOwnerDeletion=nil to true", "non-rc-deleter", Some(&[nil_block_rc1.clone()]), &[block_rc1.clone()], true),
            c("non-node-deleter, update, some ownerReferences change from blockOwnerDeletion=nil to true", "non-node-deleter", Some(&[nil_block_node.clone()]), &[block_node.clone()], true),
            c("non-rc-deleter, update, some ownerReferences change from blockOwnerDeletion=true to false", "non-rc-deleter", Some(&[block_rc1.clone()]), &[not_block_rc1.clone()], false),
            c("non-node-deleter, update, some ownerReferences change from blockOwnerDeletion=true to false", "non-node-deleter", Some(&[block_node.clone()]), &[not_block_node.clone()], false),
            c("non-rc-deleter, update, some ownerReferences change blockOwnerDeletion, but all such references are to daemonset", "non-rc-deleter", Some(&[not_block_ds1.clone()]), &[block_ds1.clone()], false),
            c("non-rc-deleter, update, add new ownerReferences with blockOwnerDeletion=nil or false", "non-rc-deleter", Some(&[]), &[not_block_rc1.clone(), nil_block_rc2.clone()], false),
            c("non-rc-deleter, update, add new ownerReferences with blockOwnerDeletion=true", "non-rc-deleter", Some(&[]), &[block_rc1.clone()], true),
            c("non-rc-deleter, update, add new ownerReferences with blockOwnerDeletion=true, but the references are to daemonset", "non-rc-deleter", Some(&[]), &[block_ds1.clone()], false),
            c("non-node-deleter, update, add ownerReferences with blockOwnerDeletion=true", "non-node-deleter", Some(&[]), &[block_node.clone()], true),
        ];
        let mut hanging = c(
            "super-user, create, some ownerReferences have blockOwnerDeletion=true, hangingRESTMapper",
            "super",
            None,
            &[block_rc1.clone(), block_rc2.clone(), block_node.clone()],
            false,
        );
        hanging.hanging_mapper = true;
        cases.push(hanging);

        for case in cases {
            let mapper: Arc<dyn OwnerRestMapper> = if case.hanging_mapper {
                Arc::new(NeverReturning)
            } else {
                Arc::new(Fake(fake_mapper()))
            };
            let p = plugin(mapper);
            let r = run(&p, case.user, "pods", None, case.old.as_ref(), &case.new).await;
            if case.err {
                assert!(
                    r.as_ref().is_err_and(|e| e.contains(
                        "cannot set blockOwnerDeletion if an ownerReference refers to a resource you can't set finalizers on"
                    )),
                    "{}: {r:?}",
                    case.name
                );
            } else {
                assert_eq!(r, Ok(()), "{}", case.name);
            }
        }
    }

    #[tokio::test]
    async fn unmappable_owner_kind_is_forbidden() {
        let p = plugin(Arc::new(Fake(fake_mapper())));
        let mut r = OwnerReference::new("example.com/v1", "Widget", "w", "");
        r.block_owner_deletion = Some(true);
        let e = run(&p, "non-rc-deleter", "pods", None, None, &meta(&[r]))
            .await
            .unwrap_err();
        assert!(
            e.starts_with("cannot set blockOwnerDeletion in this case because cannot find RESTMapping for APIVersion example.com/v1 Kind Widget: "),
            "{e}"
        );
    }

    #[test]
    fn group_version_parsing() {
        assert_eq!(parse_group_version("v1"), Ok((String::new(), "v1".into())));
        assert_eq!(
            parse_group_version("apps/v1"),
            Ok(("apps".into(), "v1".into()))
        );
        assert!(parse_group_version("a/b/c").is_err());
    }
}
