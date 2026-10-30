//! Unit tests moved from `src/preconditions.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::{Error, Result};
}

mod tests {
    use super::__parent::*;
    use rusternetes_storage::preconditions::*;
    use serde_json::json;

    #[test]
    fn empty_preconditions_always_pass() {
        let obj = json!({"metadata": {"uid": "a", "resourceVersion": "3"}});
        assert!(Preconditions::default().check("k", &obj).is_ok());
    }

    #[test]
    fn uid_mismatch_is_conflict_with_upstream_wording() {
        let obj = json!({"metadata": {"uid": "other"}});
        let err = Preconditions::new_uid("myUID")
            .check("k", &obj)
            .unwrap_err();
        assert!(matches!(&err, Error::Conflict(m) if m ==
            "Precondition failed: UID in precondition: myUID, UID in object meta: other"));
    }

    #[test]
    fn resource_version_mismatch_is_conflict() {
        let obj = json!({"metadata": {"resourceVersion": "7"}});
        assert!(Preconditions::new_resource_version("7")
            .check("k", &obj)
            .is_ok());
        assert!(matches!(
            Preconditions::new_resource_version("6").check("k", &obj),
            Err(Error::Conflict(_))
        ));
    }

    #[test]
    fn delete_options_body_carries_only_the_set_preconditions() {
        let body = Preconditions::new_uid("u1").to_delete_options();
        assert_eq!(body["kind"], "DeleteOptions");
        assert_eq!(body["preconditions"], json!({"uid": "u1"}));
        let both = Preconditions {
            uid: Some("u".into()),
            resource_version: Some("9".into()),
        };
        assert_eq!(
            both.to_delete_options()["preconditions"],
            json!({"uid": "u", "resourceVersion": "9"})
        );
    }
}
