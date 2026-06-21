//! ConfigMap validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidateConfigMap` (release-1.35).
//!
//! Scope: key validity (`IsConfigMapKey`) for `data` and `binaryData`, and the
//! cross-bag duplicate-key check. The 1 MiB total-size cap is left as a
//! follow-up.

use crate::resources::ConfigMap;
use crate::validation::field::{Error, ErrorList, Path};

/// Port of upstream `IsConfigMapKey`: ≤253 chars, matching `[-._a-zA-Z0-9]+`,
/// and not `.`/`..`. Returns the upstream-style messages.
fn config_map_key_errors(key: &str) -> Vec<String> {
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
    if key == "." || key == ".." {
        errs.push("must not be '.' or '..'".to_string());
    }
    errs
}

/// Validate a `ConfigMap`. Mirrors the core of upstream `ValidateConfigMap`.
pub fn validate_config_map(cfg: &ConfigMap) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if let Some(data) = &cfg.data {
        let data_path = Path::new("data");
        for key in data.keys() {
            for msg in config_map_key_errors(key) {
                errs.push(Error::invalid(&data_path.child(key), key.clone(), msg));
            }
            if cfg
                .binary_data
                .as_ref()
                .is_some_and(|b| b.contains_key(key))
            {
                errs.push(Error::invalid(
                    &data_path.child(key),
                    key.clone(),
                    "duplicate of key present in binaryData",
                ));
            }
        }
    }
    if let Some(binary) = &cfg.binary_data {
        let bin_path = Path::new("binaryData");
        for key in binary.keys() {
            for msg in config_map_key_errors(key) {
                errs.push(Error::invalid(&bin_path.child(key), key.clone(), msg));
            }
        }
    }

    errs
}
