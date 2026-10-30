//! ConfigMap validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateConfigMap` and
//! `ValidateConfigMapUpdate` (release-1.35).
//!
//! Scope: object metadata (`ValidateObjectMeta` with `ValidateConfigMapName`
//! == `NameIsDNSSubdomain`, and `ValidateObjectMetaUpdate` on update), key
//! validity (`IsConfigMapKey`) for `data` and `binaryData`, the cross-bag
//! duplicate-key check, the `MaxSecretSize` (1 MiB) total-size cap, and the
//! update-time immutability enforcement (`immutable: true` freezes
//! `immutable`, `data`, and `binaryData`).

use crate::resources::ConfigMap;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};

/// Upstream `core.MaxSecretSize` (`pkg/apis/core/types.go`): the combined byte
/// length of a ConfigMap's `data` + `binaryData` (and a Secret's `data`) values
/// may not exceed 1 MiB.
pub const MAX_SECRET_SIZE: usize = 1024 * 1024;

/// Port of upstream `IsConfigMapKey`
/// (`staging/src/k8s.io/apimachinery/pkg/util/validation/validation.go`):
/// ≤253 chars (`DNS1123SubdomainMaxLength`), matching `[-._a-zA-Z0-9]+`, and
/// not a chdir-prefix key (`.`, `..`, or anything starting with `..`). Returns
/// the upstream-style messages in upstream order. Shared with Secret data-key
/// validation (the same `IsConfigMapKey` rule applies there).
pub fn config_map_key_errors(key: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if key.len() > 253 {
        errs.push("must be no more than 253 characters".to_string());
    }
    if key.is_empty()
        || !key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'))
    {
        errs.push(
            "a valid config key must consist of alphanumeric characters, '-', '_' or '.'"
                .to_string(),
        );
    }
    // Upstream `hasChDirPrefix`: distinct messages for `.`, `..`, and a `..`
    // prefix. The bare `.`/`..` cases and the leading-`..` case are mutually
    // exclusive (`starts_with("..")` only fires for strings longer than two
    // dots once `..` itself is handled).
    if key == "." {
        errs.push("must not be '.'".to_string());
    } else if key == ".." {
        errs.push("must not be '..'".to_string());
    } else if key.starts_with("..") {
        errs.push("must not start with '..'".to_string());
    }
    errs
}

/// Validate a `ConfigMap`. Port of upstream `ValidateConfigMap`
/// (`pkg/apis/core/validation/validation.go:7758-7787`).
pub fn validate_config_map(cfg: &ConfigMap) -> ErrorList {
    // `ValidateObjectMeta(&cfg.ObjectMeta, true, ValidateConfigMapName, ...)`.
    // ValidateConfigMapName == apimachineryvalidation.NameIsDNSSubdomain.
    let mut errs = validate_object_meta(
        &cfg.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    let mut total_size: usize = 0;

    if let Some(data) = &cfg.data {
        let data_path = Path::new("data");
        for (key, value) in data {
            for msg in config_map_key_errors(key) {
                errs.push(Error::invalid(&data_path.key(key), key.clone(), msg));
            }
            if cfg
                .binary_data
                .as_ref()
                .is_some_and(|b| b.contains_key(key))
            {
                errs.push(Error::invalid(
                    &data_path.key(key),
                    key.clone(),
                    "duplicate of key present in binaryData",
                ));
            }
            total_size += value.len();
        }
    }
    if let Some(binary) = &cfg.binary_data {
        let bin_path = Path::new("binaryData");
        for (key, value) in binary {
            for msg in config_map_key_errors(key) {
                errs.push(Error::invalid(&bin_path.key(key), key.clone(), msg));
            }
            total_size += value.len();
        }
    }

    // Upstream emits `field.TooLong(field.NewPath(""), "", MaxSecretSize)` —
    // the empty path indicates the error refers to the whole object.
    if total_size > MAX_SECRET_SIZE {
        errs.push(Error::too_long(&Path::new(""), MAX_SECRET_SIZE));
    }

    errs
}

/// Validate a `ConfigMap` update. Port of upstream `ValidateConfigMapUpdate`
/// (`pkg/apis/core/validation/validation.go:7790-7808`): the object-meta
/// update rules; when the **old** object is immutable (`immutable: true`), the
/// `immutable` flag, `data`, and `binaryData` may not change; then the full
/// `ValidateConfigMap` checks run against the new object.
pub fn validate_config_map_update(old: &ConfigMap, new: &ConfigMap) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));

    if old.immutable == Some(true) {
        if new.immutable != Some(true) {
            errs.push(Error::forbidden(
                &Path::new("immutable"),
                "field is immutable when `immutable` is set",
            ));
        }
        if new.data != old.data {
            errs.push(Error::forbidden(
                &Path::new("data"),
                "field is immutable when `immutable` is set",
            ));
        }
        if new.binary_data != old.binary_data {
            errs.push(Error::forbidden(
                &Path::new("binaryData"),
                "field is immutable when `immutable` is set",
            ));
        }
    }

    errs.extend(validate_config_map(new));
    errs
}
