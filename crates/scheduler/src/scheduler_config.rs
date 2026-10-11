//! `VolumeBindingArgs` scheduler plugin configuration: defaulting, validation
//! and the shape -> scorer-shape conversion.
//!
//! Ported from:
//! - `pkg/scheduler/apis/config/v1/defaults.go:192-208` (`SetDefaults_VolumeBindingArgs`)
//! - `pkg/scheduler/apis/config/validation/validation_pluginargs.go:176-211`
//!   (`validateFunctionShape`) and `:266-290` (`ValidateVolumeBindingArgs*`)
//! - `pkg/scheduler/framework/plugins/volumebinding/volume_binding.go:642-653`
//!   (shape scaled by `MaxNodeScore / MaxCustomPriorityScore`).

use crate::volume_binding::FunctionShapePoint;
use rusternetes_common::validation::field::{ErrorList, Path};
use serde::{Deserialize, Serialize};

/// `config.UtilizationShapePoint` (pkg/scheduler/apis/config/types.go).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UtilizationShapePoint {
    pub utilization: i32,
    pub score: i32,
}

/// `config.VolumeBindingArgs` / `configv1.VolumeBindingArgs`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeBindingArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind_timeout_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape: Option<Vec<UtilizationShapePoint>>,
}

impl VolumeBindingArgs {
    /// `SetDefaults_VolumeBindingArgs` (v1/defaults.go:192-208).
    pub fn set_defaults(&mut self, storage_capacity_scoring: bool) {
        let _ = storage_capacity_scoring;
        todo!()
    }

    /// `ValidateVolumeBindingArgsWithOptions` (validation_pluginargs.go:274-290).
    pub fn validate(&self, path: &Path, allow_storage_capacity_scoring: bool) -> ErrorList {
        let _ = (path, allow_storage_capacity_scoring);
        todo!()
    }

    /// volume_binding.go:642-653: the scorer shape, each score scaled by
    /// `MaxNodeScore / MaxCustomPriorityScore`.
    pub fn scorer_shape(&self) -> Vec<FunctionShapePoint> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::validation::field::ErrorType;

    fn pt(utilization: i32, score: i32) -> UtilizationShapePoint {
        UtilizationShapePoint { utilization, score }
    }

    fn args(timeout: i64, shape: Option<Vec<UtilizationShapePoint>>) -> VolumeBindingArgs {
        VolumeBindingArgs {
            bind_timeout_seconds: Some(timeout),
            shape,
        }
    }

    fn summary(errs: &ErrorList) -> Vec<(ErrorType, String)> {
        errs.iter()
            .map(|e| (e.error_type, e.field.clone()))
            .collect()
    }

    // validation_pluginargs_test.go TestValidateVolumeBindingArgs
    #[test]
    fn zero_and_positive_timeout_valid() {
        for t in [0, 10] {
            assert!(args(t, None).validate(&Path::default(), false).is_empty());
        }
    }

    #[test]
    fn negative_timeout_invalid() {
        let errs = args(-10, None).validate(&Path::default(), false);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Invalid, "bindTimeoutSeconds".to_string())]
        );
        assert_eq!(
            errs[0].detail,
            "invalid BindTimeoutSeconds, should not be a negative value"
        );
    }

    #[test]
    fn feature_off_shape_nil_ok() {
        assert!(args(10, None).validate(&Path::default(), false).is_empty());
    }

    #[test]
    fn feature_off_shape_set_is_invalid() {
        let errs = args(10, Some(vec![pt(1, 1), pt(3, 3)])).validate(&Path::default(), false);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Invalid, "shape".to_string())]
        );
        assert_eq!(
            errs[0].detail,
            "unexpected field `shape`, remove it or turn on the feature gate StorageCapacityScoring"
        );
    }

    #[test]
    fn feature_on_empty_shape_required() {
        let errs = args(10, Some(vec![])).validate(&Path::default(), true);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Required, "shape".to_string())]
        );
        // A nil shape with the gate on is also "at least one point".
        let errs = args(10, None).validate(&Path::default(), true);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Required, "shape".to_string())]
        );
    }

    #[test]
    fn feature_on_unsorted_points() {
        let errs = args(10, Some(vec![pt(3, 3), pt(1, 1)])).validate(&Path::default(), true);
        assert_eq!(
            summary(&errs),
            vec![(ErrorType::Invalid, "shape[1].utilization".to_string())]
        );
        assert_eq!(errs[0].detail, "values must be sorted in increasing order");
    }

    #[test]
    fn feature_on_out_of_range_points() {
        let errs = args(
            10,
            Some(vec![pt(-1, 1), pt(10, -1), pt(20, 11), pt(101, 1)]),
        )
        .validate(&Path::default(), true);
        assert_eq!(
            summary(&errs),
            vec![
                (ErrorType::Invalid, "shape[0].utilization".to_string()),
                (ErrorType::Invalid, "shape[1].score".to_string()),
                (ErrorType::Invalid, "shape[2].score".to_string()),
                (ErrorType::Invalid, "shape[3].utilization".to_string()),
            ]
        );
        assert_eq!(errs[0].detail, "not in valid range [0, 100]");
        assert_eq!(errs[1].detail, "not in valid range [0, 10]");
    }

    #[test]
    fn nested_path_prefix() {
        let p = Path::new("profiles").index(0).child("args");
        let errs = args(-1, None).validate(&p, false);
        assert_eq!(errs[0].field, "profiles[0].args.bindTimeoutSeconds");
    }

    // v1/defaults_test.go TestPluginArgsDefaults, VolumeBindingArgs cases.
    #[test]
    fn defaults_gate_off() {
        let mut a = VolumeBindingArgs::default();
        a.set_defaults(false);
        assert_eq!(a, args(600, None));
    }

    #[test]
    fn defaults_gate_on() {
        let mut a = VolumeBindingArgs::default();
        a.set_defaults(true);
        assert_eq!(a, args(600, Some(vec![pt(0, 10), pt(100, 0)])));
    }

    #[test]
    fn defaults_keep_explicit_values() {
        let mut a = args(0, Some(vec![pt(50, 5)]));
        a.set_defaults(true);
        assert_eq!(a, args(0, Some(vec![pt(50, 5)])));
    }

    #[test]
    fn scorer_shape_scales_to_max_node_score() {
        let a = args(10, Some(vec![pt(0, 1), pt(100, 10)]));
        let got: Vec<_> = a
            .scorer_shape()
            .iter()
            .map(|p| (p.utilization, p.score))
            .collect();
        assert_eq!(got, vec![(0, 10), (100, 100)]);
    }

    #[test]
    fn default_args_scorer_shape_equals_default_shape() {
        let mut a = VolumeBindingArgs::default();
        a.set_defaults(true);
        let got: Vec<_> = a
            .scorer_shape()
            .iter()
            .map(|p| (p.utilization, p.score))
            .collect();
        let want: Vec<_> = crate::volume_binding::default_shape()
            .iter()
            .map(|p| (p.utilization, p.score))
            .collect();
        assert_eq!(got, want);
    }
}
