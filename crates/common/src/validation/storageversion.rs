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

pub fn validate_status(ss: &StorageVersionStatus, fld_path: &Path) -> ErrorList {
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

pub fn validate_server_storage_version(ssv: &ServerStorageVersion, fld_path: &Path) -> ErrorList {
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

pub fn validate_common_version(svs: &StorageVersionStatus, fld_path: &Path) -> Option<Error> {
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
pub fn validate_conditions(conditions: &[StorageVersionCondition], fld_path: &Path) -> ErrorList {
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
