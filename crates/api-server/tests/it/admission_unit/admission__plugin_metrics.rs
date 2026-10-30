//! Unit tests moved from `src/admission/plugin_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::admission::Operation;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::plugin_metrics::*;

    #[test]
    fn handlers_match_upstream_new_handler() {
        // NodeRestriction: Create, Update, Delete; Admit only.
        for op in [Operation::Create, Operation::Update, Operation::Delete] {
            assert!(observed("NodeRestriction", &op, "admit"));
            assert!(!observed("NodeRestriction", &op, "validate"));
        }
        // LimitRanger: Create, Update, both steps; never Delete.
        assert!(observed("LimitRanger", &Operation::Update, "validate"));
        assert!(!observed("LimitRanger", &Operation::Delete, "validate"));
        // Priority: Create, Update, Delete, both steps.
        assert!(observed("Priority", &Operation::Delete, "validate"));
        // RuntimeClass and DefaultStorageClass: Create only.
        assert!(observed("RuntimeClass", &Operation::Create, "admit"));
        assert!(!observed("RuntimeClass", &Operation::Update, "admit"));
        assert!(!observed(
            "DefaultStorageClass",
            &Operation::Update,
            "admit"
        ));
        // The certificate approval/signing plugins and the node-declared
        // feature validator: Update, Validate only.
        for p in [
            "CertificateApproval",
            "CertificateSigning",
            "NodeDeclaredFeatureValidator",
        ] {
            assert!(observed(p, &Operation::Update, "validate"));
            assert!(!observed(p, &Operation::Update, "admit"));
            assert!(!observed(p, &Operation::Create, "validate"));
        }
        // Connect is handled by no in-tree plugin.
        assert!(!observed("LimitRanger", &Operation::Connect, "admit"));
        // Unknown plugins are not observed.
        assert!(!observed("NoSuchPlugin", &Operation::Create, "admit"));
    }
}
