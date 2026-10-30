//! Unit tests moved from `src/resources/rbac.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::rbac::*;

    /// Upstream `rbacv1.{ClusterRoleBinding,RoleBinding}.Subjects` are
    /// `json:"subjects,omitempty"` — a binding may have zero subjects (e.g.
    /// kubeadm's `system:node` ClusterRoleBinding). A missing `subjects` key
    /// MUST decode to an empty list, not error; otherwise a single such object
    /// aborts a whole `kubectl apply -f <list>` (blocked the api-server swap
    /// substrate restore, #1659).
    #[test]
    fn binding_without_subjects_decodes_to_empty() {
        let crb: ClusterRoleBinding = serde_json::from_value(serde_json::json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": {"name": "system:node"},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "system:node"}
        }))
        .expect("ClusterRoleBinding without subjects must decode");
        assert!(crb.subjects.is_empty());

        let rb: RoleBinding = serde_json::from_value(serde_json::json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {"name": "x", "namespace": "default"},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "x"}
        }))
        .expect("RoleBinding without subjects must decode");
        assert!(rb.subjects.is_empty());
    }
}
