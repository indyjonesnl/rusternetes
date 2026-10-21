//! StorageVersion validation — port of upstream
//! `pkg/apis/apiserverinternal/validation/validation.go` (release-1.35):
//! `ValidateStorageVersion` :32-37, `ValidateStorageVersionName` :40-54,
//! `ValidateStorageVersionUpdate` :57-60 (no checks: the spec is empty),
//! `ValidateStorageVersionStatusUpdate` :63-67, `validateStorageVersionStatus`
//! :69-87, `validateServerStorageVersion` :89-134, `validateCommonVersion`
//! :148-163, `validateStorageVersionCondition` :165-188 and `isValidAPIVersion`
//! :190-224.

use crate::resources::storageversion::{
    ServerStorageVersion, StorageVersion, StorageVersionCondition, StorageVersionStatus,
};
use crate::validation::field::{BadValue, Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1035_label, is_dns1123_subdomain, is_qualified_name};
use crate::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use std::collections::{HashMap, HashSet};

const DNS1035_LABEL_FMT: &str = "[a-z]([-a-z0-9]*[a-z0-9])?";
const DNS1035_LABEL_ERR_MSG: &str = "a DNS-1035 label, which must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character";

/// `ValidateStorageVersion` (validation.go:32-37).
pub fn validate_storage_version(sv: &StorageVersion) -> ErrorList {
    let mut errs = validate_object_meta(
        &sv.metadata,
        false,
        validate_storage_version_name,
        &Path::new("metadata"),
    );
    errs.extend(validate_status(&sv.status, &Path::new("status")));
    errs
}

/// `ValidateStorageVersionName` (validation.go:40-54): a `ValidateNameFunc`.
pub fn validate_storage_version_name(name: &str, _prefix: bool) -> Vec<String> {
    let mut errs = Vec::new();
    match name.rfind('.') {
        None => errs.push("name must be in the form of <group>.<resource>".to_string()),
        Some(idx) => {
            for msg in is_dns1123_subdomain(&name[..idx]) {
                errs.push(format!("the group segment {msg}"));
            }
            for msg in is_dns1035_label(&name[idx + 1..]) {
                errs.push(format!("the resource segment {msg}"));
            }
        }
    }
    errs
}

/// `ValidateStorageVersionUpdate` (validation.go:57-60): "no error since
/// StorageVersionSpec is an empty spec".
pub fn validate_storage_version_update(_sv: &StorageVersion, _old: &StorageVersion) -> ErrorList {
    ErrorList::new()
}

/// `ValidateStorageVersionStatusUpdate` (validation.go:63-67).
pub fn validate_storage_version_status_update(
    sv: &StorageVersion,
    _old: &StorageVersion,
) -> ErrorList {
    validate_status(&sv.status, &Path::new("status"))
}

fn validate_status(ss: &StorageVersionStatus, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    let mut seen: HashSet<&str> = HashSet::new();
    for (i, ssv) in ss.storage_versions.iter().enumerate() {
        let p = fld_path.child("storageVersions").index(i);
        if !seen.insert(ssv.api_server_id.as_str()) {
            errs.push(Error::duplicate(
                &p.child("apiServerID"),
                ssv.api_server_id.clone(),
            ));
        }
        errs.extend(validate_server_storage_version(ssv, &p));
    }
    if let Some(e) = validate_common_version(ss, fld_path) {
        errs.push(e);
    }
    errs.extend(validate_conditions(&ss.conditions, fld_path));
    errs
}

fn validate_server_storage_version(ssv: &ServerStorageVersion, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    for msg in name_is_dns_subdomain(&ssv.api_server_id, false) {
        errs.push(Error::invalid(
            &fld_path.child("apiServerID"),
            ssv.api_server_id.clone(),
            msg,
        ));
    }
    let enc = is_valid_api_version(&ssv.encoding_version);
    if !enc.is_empty() {
        errs.push(Error::invalid(
            &fld_path.child("encodingVersion"),
            ssv.encoding_version.clone(),
            enc.join(","),
        ));
    }

    let mut found = false;
    for (i, dv) in ssv.decodable_versions.iter().enumerate() {
        let e = is_valid_api_version(dv);
        if !e.is_empty() {
            errs.push(Error::invalid(
                &fld_path.child("decodableVersions").index(i),
                dv.clone(),
                e.join(","),
            ));
        }
        if *dv == ssv.encoding_version {
            found = true;
        }
    }
    if !found {
        errs.push(Error::invalid(
            &fld_path.child("decodableVersions"),
            ssv.decodable_versions.clone(),
            format!(
                "decodableVersions must include encodingVersion {}",
                ssv.encoding_version
            ),
        ));
    }

    for (i, sv) in ssv.served_versions.iter().enumerate() {
        let e = is_valid_api_version(sv);
        if !e.is_empty() {
            errs.push(Error::invalid(
                &fld_path.child("servedVersions").index(i),
                sv.clone(),
                e.join(","),
            ));
        }
        if !ssv.decodable_versions.contains(sv) {
            errs.push(Error::invalid(
                &fld_path.child("servedVersions").index(i),
                sv.clone(),
                format!(
                    "individual served version : {sv} must be included in decodableVersions : [{}]",
                    ssv.decodable_versions.join(" ")
                ),
            ));
        }
    }
    errs
}

/// `commonVersion` (validation.go:136-146).
fn common_version(ssv: &[ServerStorageVersion]) -> Option<&str> {
    let first = ssv.first()?.encoding_version.as_str();
    ssv[1..]
        .iter()
        .all(|v| v.encoding_version == first)
        .then_some(first)
}

fn validate_common_version(svs: &StorageVersionStatus, fld_path: &Path) -> Option<Error> {
    let actual = common_version(&svs.storage_versions);
    let p = fld_path.child("commonEncodingVersion");
    match (actual, svs.common_encoding_version.as_deref()) {
        (None, None) => None,
        (None, Some(c)) => Some(Error::invalid(
            &p,
            c.to_string(),
            "should be nil if servers do not agree on the same encoding version, or if there is no server reporting the supported versions yet",
        )),
        (Some(a), None) => Some(Error::invalid(
            &p,
            BadValue::marshal(&Option::<String>::None),
            format!("the common encoding version is {a}"),
        )),
        (Some(a), Some(c)) if a != c => Some(Error::invalid(
            &p,
            c.to_string(),
            format!("the actual common encoding version is {a}"),
        )),
        _ => None,
    }
}

/// `validateStorageVersionCondition` (validation.go:165-188). Upstream indexes
/// the conditions off `status`, not `status.conditions`; ported as is.
fn validate_conditions(conditions: &[StorageVersionCondition], fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    // The condition type or status is not checked against the predefined
    // ones because more may be added later.
    let mut seen_type: HashMap<&str, usize> = HashMap::new();
    for (i, c) in conditions.iter().enumerate() {
        let p = fld_path.index(i);
        if let Some(ii) = seen_type.get(c.condition_type.as_str()) {
            errs.push(Error::invalid(
                &p.child("type"),
                c.condition_type.clone(),
                format!(
                    "the type of the condition is not unique, it also appears in conditions[{ii}]"
                ),
            ));
        }
        seen_type.insert(c.condition_type.as_str(), i);
        errs.extend(validate_qualified_name(&c.condition_type, &p.child("type")));
        errs.extend(validate_qualified_name(&c.status, &p.child("status")));
        if c.reason.is_empty() {
            errs.push(Error::required(&p.child("reason"), ""));
        }
        if c.message.is_empty() {
            errs.push(Error::required(&p.child("message"), ""));
        }
    }
    errs
}

/// `apivalidation.ValidateQualifiedName`.
fn validate_qualified_name(value: &str, fld_path: &Path) -> ErrorList {
    is_qualified_name(value)
        .into_iter()
        .map(|msg| Error::invalid(fld_path, value, msg))
        .collect()
}

/// `isValidAPIVersion` (validation.go:190-224).
fn is_valid_api_version(api_version: &str) -> Vec<String> {
    let mut errs = Vec::new();
    let parts: Vec<&str> = api_version.split('/').collect();
    let version = match parts.len() {
        1 => parts[0],
        2 => {
            let (group, version) = (parts[0], parts[1]);
            if group.is_empty() {
                errs.push("group part: must be non-empty".to_string());
            } else {
                for m in is_dns1123_subdomain(group) {
                    errs.push(format!("group part: {m}"));
                }
            }
            version
        }
        _ => {
            errs.push(format!(
                "an apiVersion is {} with an optional DNS subdomain prefix and '/' (e.g. 'example.com/MyVersion')",
                crate::validation::metav1::regex_error(
                    DNS1035_LABEL_ERR_MSG,
                    DNS1035_LABEL_FMT,
                    &["my-name", "abc-123"]
                )
            ));
            return errs;
        }
    };
    if version.is_empty() {
        errs.push("version part: must be non-empty".to_string());
    } else {
        for m in is_dns1035_label(version) {
            errs.push(format!("version part: {m}"));
        }
    }
    errs
}

#[cfg(test)]
mod tests {
    use super::*;

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
