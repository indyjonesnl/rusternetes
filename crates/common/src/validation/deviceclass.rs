//! DeviceClass (resource.k8s.io / DRA) validation — port of upstream
//! Kubernetes `pkg/apis/resource/validation/validation.go::ValidateDeviceClass`
//! (release-1.35).
//!
//! Scope: the CEL-free structural checks of `spec` — the `selectors` set
//! (≤32, each must carry a `cel` selector whose `expression` is non-empty and
//! ≤10Ki) and the `config` set (≤32). Parse-checking the CEL expressions; compiling against the
//! DRA CEL environment is tracked in #1442. ObjectMeta is validated separately.

use crate::cel_versions::{self, EnvSet};
use crate::resources::{DeviceClass, DeviceClassSpec};
use crate::validation::field::{ErrorList, Path};
use crate::validation::resourceclaim::{too_many_max_items, validate_selector_slice_in};

const DEVICE_CONFIG_MAX_SIZE: usize = 32;

/// Validate a `DeviceClass` on create. Mirrors the structural part of upstream
/// `ValidateDeviceClass` (minus CEL compilation — see #1442).
pub fn validate_device_class(class: &DeviceClass) -> ErrorList {
    validate_device_class_spec(&class.spec, &Path::new("spec"))
}

/// `validateDeviceClassSpec(spec, oldSpec, fldPath)` as called from
/// `ValidateDeviceClassUpdate` (`validation.go:545-567`): the selectors are
/// "stored" (compiled in `StoredExpressions`) iff exactly as before; any
/// change, including a reorder, compiles them as new expressions.
pub fn validate_device_class_update(class: &DeviceClass, old: &DeviceClass) -> ErrorList {
    validate_device_class_update_in(
        class,
        old,
        &EnvSet::must_base_env_set(cel_versions::default_compatibility_version()),
    )
}

pub fn validate_device_class_update_in(
    class: &DeviceClass,
    old: &DeviceClass,
    env_set: &EnvSet,
) -> ErrorList {
    let stored = class.spec.selectors == old.spec.selectors;
    validate_device_class_spec_in(&class.spec, &Path::new("spec"), stored, env_set)
}

fn validate_device_class_spec(spec: &DeviceClassSpec, fld_path: &Path) -> ErrorList {
    validate_device_class_spec_in(
        spec,
        fld_path,
        false,
        &EnvSet::must_base_env_set(cel_versions::default_compatibility_version()),
    )
}

fn validate_device_class_spec_in(
    spec: &DeviceClassSpec,
    fld_path: &Path,
    stored: bool,
    env_set: &EnvSet,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // Upstream reaches the identical `validateSelectorSlice` from both a
    // DeviceClass and a claim's requests (`validation.go:298`), so this calls
    // the one port of it rather than keeping a second copy.
    errs.extend(validate_selector_slice_in(
        &spec.selectors,
        &fld_path.child("selectors"),
        stored,
        env_set,
    ));

    if spec.config.len() > DEVICE_CONFIG_MAX_SIZE {
        errs.push(too_many_max_items(
            &fld_path.child("config"),
            spec.config.len(),
            DEVICE_CONFIG_MAX_SIZE,
        ));
    }

    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::resourceclaim::{
        CEL_SELECTOR_EXPRESSION_MAX_LENGTH, DEVICE_SELECTORS_MAX_SIZE,
    };

    fn dc(spec: serde_json::Value) -> DeviceClass {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "class-1"},
            "spec": spec,
        }))
        .unwrap()
    }

    fn errs(spec: serde_json::Value) -> Vec<String> {
        validate_device_class(&dc(spec))
            .into_iter()
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn valid_selector_passes() {
        assert!(errs(serde_json::json!({
            "selectors": [{"cel": {"expression": "device.attributes['x'] == 1"}}]
        }))
        .is_empty());
        // empty spec also passes
        assert!(errs(serde_json::json!({})).is_empty());
    }

    #[test]
    fn selector_requires_cel() {
        let e = errs(serde_json::json!({"selectors": [{}]}));
        assert!(e.iter().any(|m| m.contains("cel")), "{e:?}");
    }

    /// `validateCELSelector` (validation.go:316): an expression that does not
    /// compile is `field.Invalid(.., "compilation failed: ...")`.
    #[test]
    fn uncompilable_expression_rejected() {
        let e = errs(serde_json::json!({
            "selectors": [{"cel": {"expression": "device.driver =="}}]
        }));
        assert!(
            e.iter()
                .any(|m| m.contains("selectors[0].cel.expression")
                    && m.contains("compilation failed")),
            "{e:?}"
        );
    }

    #[test]
    fn dra_environment_expressions_compile() {
        for x in [
            "device.driver == 'dra.example.com'",
            "device.attributes['dra.example.com'].model == 'a'",
            "device.capacity['dra.example.com'].memory.compareTo(quantity('1Gi')) >= 0",
        ] {
            let e = errs(serde_json::json!({"selectors": [{"cel": {"expression": x}}]}));
            assert!(e.is_empty(), "{x}: {e:?}");
        }
    }

    #[test]
    fn empty_expression_rejected() {
        let e = errs(serde_json::json!({"selectors": [{"cel": {"expression": ""}}]}));
        assert!(e.iter().any(|m| m.contains("expression")), "{e:?}");
    }

    #[test]
    fn too_long_expression_rejected() {
        let big = "a".repeat(CEL_SELECTOR_EXPRESSION_MAX_LENGTH + 1);
        let e = errs(serde_json::json!({"selectors": [{"cel": {"expression": big}}]}));
        assert!(
            e.iter()
                .any(|m| m.contains("expression") && m.contains("bytes")),
            "{e:?}"
        );
    }

    #[test]
    fn too_many_selectors_rejected() {
        let sels: Vec<_> = (0..DEVICE_SELECTORS_MAX_SIZE + 1)
            .map(|_| serde_json::json!({"cel": {"expression": "true"}}))
            .collect();
        let e = errs(serde_json::json!({"selectors": sels}));
        assert!(
            e.iter()
                .any(|m| m.contains("selectors") && m.contains("at most")),
            "{e:?}"
        );
    }

    /// `validateSlice` tags the size breach `.WithOrigin("maxItems")`
    /// (`pkg/apis/resource/validation/validation.go:1181`; asserted by
    /// `validation_deviceclass_test.go:222,316`).
    #[test]
    fn too_many_selectors_and_config_carry_max_items_origin() {
        let selectors: Vec<_> = (0..33)
            .map(|_| serde_json::json!({"cel": {"expression": "true"}}))
            .collect();
        let config: Vec<_> = (0..33)
            .map(|_| serde_json::json!({"opaque": {"driver": "d.example.com", "parameters": {}}}))
            .collect();
        let mut got: Vec<(String, String)> = validate_device_class(&dc(
            serde_json::json!({"selectors": selectors, "config": config}),
        ))
        .into_iter()
        .filter(|e| e.error_type == crate::validation::field::ErrorType::TooMany)
        .map(|e| (e.field, e.origin))
        .collect();
        got.sort();
        let m = |f: &str| (f.to_string(), "maxItems".to_string());
        assert_eq!(got, vec![m("spec.config"), m("spec.selectors")]);
    }

    fn class_with(expr: &str) -> DeviceClass {
        dc(serde_json::json!({"selectors": [{"cel": {"expression": expr}}]}))
    }

    fn env_1_32() -> EnvSet {
        EnvSet::must_base_env_set(crate::feature_gates::Version::major_minor(1, 32))
    }

    /// `validateCELSelector` (validation.go:316-322) + `validateDeviceClassSpec`
    /// (:571-577): an unchanged selector list compiles in `StoredExpressions`
    /// (every library), a changed one in `NewExpressions`, which carries only
    /// the libraries introduced by the compatibility version (semver: 1.33).
    #[test]
    fn selector_library_gated_by_new_vs_stored_environment() {
        let expr = "device.attributes['d'].v.isSemver()";
        let new = class_with(expr);
        let msgs = |errs: ErrorList| errs.into_iter().map(|e| e.to_string()).collect::<Vec<_>>();

        let created = msgs(validate_device_class_update_in(
            &new,
            &class_with("true"),
            &env_1_32(),
        ));
        assert!(
            created
                .iter()
                .any(|m| m.contains("undeclared reference to 'isSemver'")),
            "{created:?}"
        );
        let stored = msgs(validate_device_class_update_in(&new, &new, &env_1_32()));
        assert!(stored.is_empty(), "{stored:?}");
        let mut two = new.clone();
        two.spec
            .selectors
            .push(class_with("true").spec.selectors[0].clone());
        let mut swapped = two.clone();
        swapped.spec.selectors.reverse();
        assert!(!msgs(validate_device_class_update_in(&swapped, &two, &env_1_32())).is_empty());
        assert!(validate_device_class(&new).is_empty());
    }
}
