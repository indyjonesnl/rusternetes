//! Unit tests moved from `src/registry/rbac/escalation_check.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::rest::RequestContext;
    pub use rusternetes_common::authz::{Authorizer, Decision, RequestAttributes};
    pub use rusternetes_common::resources::RoleRef;
    pub use serde::Serialize;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::rbac::escalation_check::*;
    use rusternetes_common::resources::Role;
    use rusternetes_common::types::OwnerReference;

    fn role() -> Role {
        Role::new("r", "ns")
    }

    /// TestIsOnlyMutatingGCFields (helpers_test.go:43).
    #[test]
    fn only_gc_fields_may_differ() {
        let base = role();
        assert!(is_only_mutating_gc_fields(&base, &base.clone()));

        let mut owner = base.clone();
        owner.metadata.owner_references = Some(vec![OwnerReference {
            api_version: "v1".into(),
            kind: "Pod".into(),
            name: "foo".into(),
            uid: "u".into(),
            controller: None,
            block_owner_deletion: None,
        }]);
        assert!(is_only_mutating_gc_fields(&owner, &base));

        let mut both = owner.clone();
        both.metadata.finalizers = Some(vec!["final".into()]);
        assert!(is_only_mutating_gc_fields(&both, &base));

        let mut annotated = both.clone();
        annotated.metadata.annotations = Some([("foo".to_string(), "bar".to_string())].into());
        assert!(!is_only_mutating_gc_fields(&annotated, &base));

        let mut other = both;
        other.rules = vec![rusternetes_common::resources::PolicyRule {
            verbs: vec!["get".into()],
            api_groups: None,
            resources: None,
            resource_names: None,
            non_resource_urls: None,
        }];
        assert!(!is_only_mutating_gc_fields(&other, &base));
    }

    #[test]
    fn superusers_are_system_masters_members() {
        let mut ctx = RequestContext::new(None);
        assert!(!escalation_allowed(&ctx));
        let user = |groups: &[&str]| rusternetes_common::auth::UserInfo {
            username: "u".into(),
            uid: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            extra: Default::default(),
        };
        ctx = ctx.with_user(&user(&["devs"]));
        assert!(!escalation_allowed(&ctx));
        let ctx = RequestContext::new(None).with_user(&user(&["devs", "system:masters"]));
        assert!(escalation_allowed(&ctx));
    }
}
