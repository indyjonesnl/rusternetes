//! Storage-level operation preconditions.
//!
//! Ported from `storage.Preconditions` and `Preconditions.Check`
//! (`staging/src/k8s.io/apiserver/pkg/storage/interfaces.go:122-157`,
//! release-1.35). A precondition names the UID and/or resourceVersion the
//! object MUST currently have for the write to proceed; the store checks it
//! against the object it is about to replace, inside the same compare-and-swap
//! that performs the write (`etcd3/store.go` `conditionalDelete`).

use rusternetes_common::{Error, Result};

/// `storage.Preconditions`: both fields optional, each checked only when set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Preconditions {
    /// Specifies the target UID.
    pub uid: Option<String>,
    /// Specifies the target ResourceVersion.
    pub resource_version: Option<String>,
}

impl Preconditions {
    /// `NewUIDPreconditions`.
    pub fn new_uid(uid: impl Into<String>) -> Self {
        Self {
            uid: Some(uid.into()),
            resource_version: None,
        }
    }

    /// A ResourceVersion-only precondition
    /// (`metav1.DeleteOptions{Preconditions: {ResourceVersion}}`).
    pub fn new_resource_version(rv: impl Into<String>) -> Self {
        Self {
            uid: None,
            resource_version: Some(rv.into()),
        }
    }

    /// `Preconditions.Check(key, obj)`. Upstream returns `NewInvalidObjError`
    /// (`ErrCodeInvalidObj`), which `InterpretDeleteError`
    /// (`storage/errors/storage.go:103-104`) maps to `NewConflict` (409), so
    /// the failure is `Error::Conflict` here, with upstream's message wording.
    pub fn check(&self, _key: &str, obj: &serde_json::Value) -> Result<()> {
        let meta = obj.get("metadata");
        let str_of = |f: &str| {
            meta.and_then(|m| m.get(f))
                .and_then(|v| v.as_str())
                .unwrap_or("")
        };
        if let Some(uid) = &self.uid {
            if uid != str_of("uid") {
                return Err(Error::Conflict(format!(
                    "Precondition failed: UID in precondition: {}, UID in object meta: {}",
                    uid,
                    str_of("uid")
                )));
            }
        }
        if let Some(rv) = &self.resource_version {
            if rv != str_of("resourceVersion") {
                return Err(Error::Conflict(format!(
                    "Precondition failed: ResourceVersion in precondition: {}, ResourceVersion in object meta: {}",
                    rv,
                    str_of("resourceVersion")
                )));
            }
        }
        Ok(())
    }

    /// The `metav1.DeleteOptions` body carrying these preconditions, for
    /// clients that delete through an api-server.
    pub fn to_delete_options(&self) -> serde_json::Value {
        let mut p = serde_json::Map::new();
        if let Some(uid) = &self.uid {
            p.insert("uid".into(), uid.clone().into());
        }
        if let Some(rv) = &self.resource_version {
            p.insert("resourceVersion".into(), rv.clone().into());
        }
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "preconditions": p,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
