//! Unit tests moved from `src/validation/storageversion.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::storageversion::{
        ServerStorageVersion, StorageVersion, StorageVersionCondition, StorageVersionStatus,
    };
    pub use rusternetes_common::validation::field::{BadValue, Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        is_dns1035_label, is_dns1123_subdomain, is_qualified_name,
    };
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta,
    };
    pub use std::collections::{HashMap, HashSet};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::storageversion::*;

    fn agg(errs: &ErrorList) -> String {
        errs.iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn v(s: &[&str]) -> Vec<String> {
        s.iter().map(|x| x.to_string()).collect()
    }

    fn ssv(id: &str, enc: &str, dec: &[&str], served: &[&str]) -> ServerStorageVersion {
        ServerStorageVersion {
            api_server_id: id.into(),
            encoding_version: enc.into(),
            decodable_versions: v(dec),
            served_versions: v(served),
        }
    }

    /// validation_test.go `TestValidateServerStorageVersion`.
    #[test]
    fn server_storage_version() {
        let group_err = "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')";
        let ver_err = "a DNS-1035 label must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?')";
        let api_err = "an apiVersion is a DNS-1035 label, which must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?') with an optional DNS subdomain prefix and '/' (e.g. 'example.com/MyVersion')";
        let cases: Vec<(ServerStorageVersion, String)> = vec![
            (
                ssv("-fea", "v1alpha1", &["v1alpha1", "v1"], &["v1alpha1", "v1"]),
                "apiServerID: Invalid value".into(),
            ),
            (
                ssv("fea", "v1alpha1", &["v1beta1", "v1"], &["v1beta1", "v1"]),
                "decodableVersions must include encodingVersion".into(),
            ),
            (
                ssv(
                    "fea",
                    "v1alpha1",
                    &["v1alpha1", "v1", "-fea"],
                    &["v1alpha1", "v1", "-fea"],
                ),
                "decodableVersions[2]: Invalid value".into(),
            ),
            (
                ssv("fea", "v1alpha1", &["v1alpha1", "v1"], &["v1alpha1", "v1"]),
                "".into(),
            ),
            (
                ssv(
                    "fea",
                    "mygroup.com/v2",
                    &["v1alpha1", "v1", "mygroup.com/v2"],
                    &["v1alpha1", "v1", "mygroup.com/v2"],
                ),
                "".into(),
            ),
            (
                ssv("fea", "v1alpha1", &["v1alpha1", "v1"], &["/v3"]),
                "servedVersions[0]: Invalid value: \"/v3\": group part: must be non-empty".into(),
            ),
            (
                ssv(
                    "fea",
                    "mygroup.com/v2",
                    &["mygroup.com/v2", "/v3"],
                    &["mygroup.com/v2", "/v3"],
                ),
                "decodableVersions[1]: Invalid value: \"/v3\": group part: must be non-empty"
                    .into(),
            ),
            (
                ssv(
                    "fea",
                    "mygroup.com/v2",
                    &["mygroup.com/v2", "/v3"],
                    &["mygroup.com/"],
                ),
                "servedVersions[0]: Invalid value: \"mygroup.com/\": version part: must be non-empty"
                    .into(),
            ),
            (
                ssv(
                    "fea",
                    "mygroup.com/v2",
                    &["mygroup.com/v2", "mygroup.com/"],
                    &["mygroup.com/v2", "mygroup.com/"],
                ),
                "decodableVersions[1]: Invalid value: \"mygroup.com/\": version part: must be non-empty"
                    .into(),
            ),
            (
                ssv(
                    "fea",
                    "/v3",
                    &["mygroup.com/v2", "/v3"],
                    &["mygroup.com/v2", "/v3"],
                ),
                "encodingVersion: Invalid value: \"/v3\": group part: must be non-empty".into(),
            ),
            (
                ssv(
                    "fea",
                    "v1",
                    &["v1", "mygroup_com/v2"],
                    &["v1", "mygroup_com/v2"],
                ),
                format!("decodableVersions[1]: Invalid value: \"mygroup_com/v2\": group part: {group_err}"),
            ),
            (
                ssv(
                    "fea",
                    "v1",
                    &["v1", "mygroup.com/v2"],
                    &["v1", "mygroup_com/v2"],
                ),
                format!("servedVersions[1]: Invalid value: \"mygroup_com/v2\": group part: {group_err}"),
            ),
            (
                ssv(
                    "fea",
                    "v1",
                    &["v1", "mygroup.com/v2_"],
                    &["v1", "mygroup.com/v2_"],
                ),
                format!("decodableVersions[1]: Invalid value: \"mygroup.com/v2_\": version part: {ver_err}"),
            ),
            (
                ssv(
                    "fea",
                    "v1",
                    &["v1", "mygroup.com/v2"],
                    &["v1", "mygroup.com/v2_"],
                ),
                format!("servedVersions[1]: Invalid value: \"mygroup.com/v2_\": version part: {ver_err}"),
            ),
            (
                ssv(
                    "fea",
                    "v1",
                    &["v1", "mygroup.com/v2/myresource"],
                    &["v1", "mygroup.com/v2/myresource"],
                ),
                format!("decodableVersions[1]: Invalid value: \"mygroup.com/v2/myresource\": {api_err}"),
            ),
            (
                ssv(
                    "fea",
                    "v1",
                    &["v1", "mygroup.com/v2"],
                    &["v1", "mygroup.com/v2/myresource"],
                ),
                format!("servedVersions[1]: Invalid value: \"mygroup.com/v2/myresource\": {api_err}"),
            ),
            (
                ssv("fea", "v1alpha1", &["v1alpha1", "v1"], &["v2"]),
                "servedVersions[0]: Invalid value: \"v2\": individual served version : v2 must be included in decodableVersions : [v1alpha1 v1]".into(),
            ),
        ];
        for (s, want) in cases {
            let got = agg(&validate_server_storage_version(&s, &Path::new("")));
            if want.is_empty() {
                assert_eq!(got, "", "{s:?}");
            } else {
                assert!(got.contains(&want), "want {want:?} in {got:?}");
            }
        }
    }

    /// `TestValidateStorageVersionStatus`.
    #[test]
    fn status() {
        let ok = StorageVersionStatus {
            storage_versions: vec![
                ssv("1", "v1alpha1", &["v1alpha1", "v1"], &[]),
                ssv("2", "v1alpha1", &["v1alpha1", "v1"], &[]),
            ],
            common_encoding_version: Some("v1alpha1".into()),
            conditions: vec![],
        };
        assert_eq!(agg(&validate_status(&ok, &Path::new(""))), "");
        let dup = StorageVersionStatus {
            storage_versions: vec![
                ssv("1", "v1alpha1", &["v1alpha1", "v1"], &[]),
                ssv("1", "v1beta1", &["v1alpha1", "v1"], &[]),
            ],
            common_encoding_version: Some("v1alpha1".into()),
            conditions: vec![],
        };
        let got = agg(&validate_status(&dup, &Path::new("")));
        assert!(
            got.contains("storageVersions[1].apiServerID: Duplicate value: \"1\""),
            "{got}"
        );
    }

    /// `TestValidateCommonVersion`.
    #[test]
    fn common_version_cases() {
        let st = |svs: Vec<ServerStorageVersion>, c: Option<&str>| StorageVersionStatus {
            storage_versions: svs,
            common_encoding_version: c.map(String::from),
            conditions: vec![],
        };
        let s = |id: &str, e: &str| ssv(id, e, &[], &[]);
        let nil_msg = "should be nil if servers do not agree on the same encoding version, or if there is no server reporting the supported versions yet";
        let cases: Vec<(StorageVersionStatus, &str)> = vec![
            (st(vec![], Some("v1alpha1")), nil_msg),
            (
                st(vec![s("1", "v1alpha1"), s("2", "v1")], Some("v1alpha1")),
                nil_msg,
            ),
            (
                st(vec![s("1", "v1alpha1"), s("2", "v1alpha1")], None),
                "Invalid value: null: the common encoding version is v1alpha1",
            ),
            (
                st(vec![s("1", "v1alpha1"), s("2", "v1alpha1")], Some("v1")),
                "Invalid value: \"v1\": the actual common encoding version is v1alpha1",
            ),
            (
                st(
                    vec![s("1", "v1alpha1"), s("2", "v1alpha1")],
                    Some("v1alpha1"),
                ),
                "",
            ),
            (st(vec![s("1", "v1alpha1")], Some("v1alpha1")), ""),
        ];
        for (status, want) in cases {
            let got = validate_common_version(&status, &Path::new(""))
                .map(|e| e.to_string())
                .unwrap_or_default();
            if want.is_empty() {
                assert_eq!(got, "");
            } else {
                assert!(got.contains(want), "want {want:?} in {got:?}");
            }
        }
    }

    fn cond(t: &str, s: &str, reason: &str, msg: &str) -> StorageVersionCondition {
        StorageVersionCondition {
            condition_type: t.into(),
            status: s.into(),
            reason: reason.into(),
            message: msg.into(),
            ..Default::default()
        }
    }

    /// `TestValidateStorageVersionCondition`.
    #[test]
    fn conditions() {
        let cases: Vec<(Vec<StorageVersionCondition>, &str)> = vec![
            (
                vec![cond("-fea", "True", "unknown", "unknown")],
                "type: Invalid value",
            ),
            (
                vec![cond("fea", "-True", "unknown", "unknown")],
                "status: Invalid value",
            ),
            (vec![cond("fea", "True", "", "unknown")], "Required value"),
            (vec![cond("fea", "True", "unknown", "")], "Required value"),
            (
                vec![
                    cond("fea", "True", "unknown", "unknown"),
                    cond("fea", "True", "unknown", "unknown"),
                ],
                "\"fea\": the type of the condition is not unique, it also appears in conditions[0]",
            ),
            (vec![cond("fea", "True", "unknown", "unknown")], ""),
        ];
        for (c, want) in cases {
            let got = agg(&validate_conditions(&c, &Path::new("")));
            if want.is_empty() {
                assert_eq!(got, "");
            } else {
                assert!(got.contains(want), "want {want:?} in {got:?}");
            }
        }
    }

    /// `TestValidateStorageVersionName`.
    #[test]
    fn name() {
        let x = |n: usize| "x".repeat(n);
        let cases: Vec<(String, &str)> = vec![
            ("".into(), "name must be in the form of <group>.<resource>"),
            (
                "pods".into(),
                "name must be in the form of <group>.<resource>",
            ),
            ("core.pods".into(), ""),
            ("authentication.k8s.io.tokenreviews".into(), ""),
            (format!("{}.tokenreviews", x(253)), ""),
            (
                format!("{}.tokenreviews", x(254)),
                "the group segment must be no more than 253 characters",
            ),
            (format!("authentication.k8s.io.{}", x(63)), ""),
            (
                format!("authentication.k8s.io.{}", x(64)),
                "the resource segment must be no more than 63 characters",
            ),
        ];
        for (name, want) in cases {
            let errs = validate_storage_version_name(&name, false);
            if want.is_empty() {
                assert!(errs.is_empty(), "{name}: {errs:?}");
            } else {
                // metav1.rs `max_len_error` says "bytes"; upstream 1.35
                // `MaxLenError` says "characters" (a divergence in the shared
                // helper, outside this port), so compare modulo the unit.
                assert!(
                    errs.iter()
                        .any(|m| m.replace(" bytes", " characters") == want),
                    "{name}: {errs:?}"
                );
            }
        }
    }

    /// Update validation checks nothing (validation.go:57-60); status update
    /// validates only the status.
    #[test]
    fn update_validators() {
        let sv = StorageVersion::default();
        assert!(validate_storage_version_update(&sv, &sv).is_empty());
        let mut bad = StorageVersion::default();
        bad.status.common_encoding_version = Some("v1".into());
        assert_eq!(validate_storage_version_status_update(&bad, &sv).len(), 1);
    }
}
