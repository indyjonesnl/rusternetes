//! Unit tests moved from `src/validation/rbac.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::rbac::{
        ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, RoleRef, Subject,
    };
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        is_dns1123_subdomain, validate_label_selector, LabelSelectorValidationOptions,
    };
}

mod update_tests {
    use super::__parent::*;
    use rusternetes_common::validation::rbac::*;

    fn rb(role_ref_name: &str) -> RoleBinding {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "rb", "namespace": "default"},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": role_ref_name},
            "subjects": [{"kind": "User", "name": "alice", "apiGroup": "rbac.authorization.k8s.io"}]
        }))
        .unwrap()
    }

    fn crb(role_ref_name: &str) -> ClusterRoleBinding {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "crb"},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role_ref_name},
            "subjects": [{"kind": "User", "name": "alice", "apiGroup": "rbac.authorization.k8s.io"}]
        }))
        .unwrap()
    }

    #[test]
    fn unchanged_role_ref_passes() {
        assert!(validate_role_binding_update(&rb("admin"), &rb("admin")).is_empty());
        assert!(validate_cluster_role_binding_update(&crb("admin"), &crb("admin")).is_empty());
    }

    #[test]
    fn changing_role_ref_rejected() {
        let e = validate_role_binding_update(&rb("editor"), &rb("admin"));
        assert!(
            e.iter().any(|x| x.detail == "cannot change roleRef"),
            "{e:?}"
        );
        let ce = validate_cluster_role_binding_update(&crb("editor"), &crb("admin"));
        assert!(
            ce.iter().any(|x| x.detail == "cannot change roleRef"),
            "{ce:?}"
        );
    }
}
