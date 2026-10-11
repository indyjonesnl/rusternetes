//! `VolumeBindingArgs` scheduler plugin configuration: defaulting, validation
//! and the shape -> scorer-shape conversion.
//!
//! Ported from:
//! - `pkg/scheduler/apis/config/v1/defaults.go:192-208` (`SetDefaults_VolumeBindingArgs`)
//! - `pkg/scheduler/apis/config/validation/validation_pluginargs.go:176-211`
//!   (`validateFunctionShape`) and `:266-290` (`ValidateVolumeBindingArgs*`)
//! - `pkg/scheduler/framework/plugins/volumebinding/volume_binding.go:642-653`
//!   (shape scaled by `MaxNodeScore / MaxCustomPriorityScore`).

use crate::volume_binding::{FunctionShapePoint, MAX_CUSTOM_PRIORITY_SCORE, MAX_NODE_SCORE};
use rusternetes_common::validation::field::{self, ErrorList, Path};
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

/// `DefaultBindTimeoutSeconds` (v1/defaults.go:193-194).
const DEFAULT_BIND_TIMEOUT_SECONDS: i64 = 600;

impl VolumeBindingArgs {
    /// `SetDefaults_VolumeBindingArgs` (v1/defaults.go:192-208).
    pub fn set_defaults(&mut self, storage_capacity_scoring: bool) {
        if self.bind_timeout_seconds.is_none() {
            self.bind_timeout_seconds = Some(DEFAULT_BIND_TIMEOUT_SECONDS);
        }
        if self.shape.as_ref().is_none_or(|s| s.is_empty()) && storage_capacity_scoring {
            self.shape = Some(vec![
                UtilizationShapePoint {
                    utilization: 0,
                    score: MAX_CUSTOM_PRIORITY_SCORE as i32,
                },
                UtilizationShapePoint {
                    utilization: 100,
                    score: 0,
                },
            ]);
        }
    }

    /// `ValidateVolumeBindingArgsWithOptions` (validation_pluginargs.go:274-290).
    pub fn validate(&self, path: &Path, allow_storage_capacity_scoring: bool) -> ErrorList {
        let mut errs = ErrorList::new();
        let timeout = self.bind_timeout_seconds.unwrap_or(0);
        if timeout < 0 {
            errs.push(field::Error::invalid(
                &path.child("bindTimeoutSeconds"),
                timeout,
                "invalid BindTimeoutSeconds, should not be a negative value",
            ));
        }
        if allow_storage_capacity_scoring {
            errs.extend(validate_function_shape(
                self.shape.as_deref().unwrap_or(&[]),
                &path.child("shape"),
            ));
        } else if let Some(shape) = &self.shape {
            // When the feature is off, return an error if the config is not nil.
            // This prevents unexpected configuration from taking effect when the
            // feature turns on in the future.
            errs.push(field::Error::invalid(
                &path.child("shape"),
                field::BadValue::marshal(shape),
                "unexpected field `shape`, remove it or turn on the feature gate StorageCapacityScoring",
            ));
        }
        errs
    }

    /// volume_binding.go:642-653: the scorer shape, each score scaled by
    /// `MaxNodeScore / MaxCustomPriorityScore`.
    pub fn scorer_shape(&self) -> Vec<FunctionShapePoint> {
        self.shape
            .iter()
            .flatten()
            .map(|p| FunctionShapePoint {
                utilization: p.utilization as i64,
                score: p.score as i64 * (MAX_NODE_SCORE / MAX_CUSTOM_PRIORITY_SCORE),
            })
            .collect()
    }
}

/// The slice of `KubeSchedulerConfiguration` the scheduler reads
/// (`profiles[].pluginConfig[]`, pkg/scheduler/apis/config/types.go).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KubeSchedulerConfiguration {
    #[serde(default)]
    profiles: Vec<Profile>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Profile {
    #[serde(default)]
    plugin_config: Vec<PluginConfig>,
}

#[derive(Debug, Deserialize)]
struct PluginConfig {
    name: String,
    #[serde(default)]
    args: Option<serde_yaml::Value>,
}

/// Extract, default and validate the `VolumeBinding` plugin args from a
/// `KubeSchedulerConfiguration` document (YAML or JSON). `Ok(defaults)` when the
/// document has no `VolumeBinding` pluginConfig. `Err` carries the aggregated
/// validation errors, mirroring `ValidateVolumeBindingArgs` run per profile
/// (validation.go ValidateKubeSchedulerConfiguration; only the first profile is
/// consulted because Rusternetes runs a single `default-scheduler` profile).
pub fn volume_binding_args_from_config(
    doc: &str,
    storage_capacity_scoring: bool,
) -> Result<VolumeBindingArgs, String> {
    let cfg: KubeSchedulerConfiguration =
        serde_yaml::from_str(doc).map_err(|e| format!("invalid scheduler config: {e}"))?;
    let mut args = VolumeBindingArgs::default();
    let mut path = Path::new("profiles").index(0);
    if let Some(profile) = cfg.profiles.first() {
        for (i, pc) in profile.plugin_config.iter().enumerate() {
            if pc.name == "VolumeBinding" {
                path = path.child("pluginConfig").index(i).child("args");
                if let Some(v) = &pc.args {
                    args = serde_yaml::from_value(v.clone())
                        .map_err(|e| format!("{path}: invalid VolumeBindingArgs: {e}"))?;
                }
                break;
            }
        }
    }
    args.set_defaults(storage_capacity_scoring);
    let errs = args.validate(&path, storage_capacity_scoring);
    if errs.is_empty() {
        Ok(args)
    } else {
        Err(errs
            .iter()
            .map(|e| format!("{}: {}", e.field, e.error_body()))
            .collect::<Vec<_>>()
            .join(", "))
    }
}

/// `validateFunctionShape` (validation_pluginargs.go:176-211).
fn validate_function_shape(shape: &[UtilizationShapePoint], path: &Path) -> ErrorList {
    const MIN_UTILIZATION: i32 = 0;
    const MAX_UTILIZATION: i32 = 100;
    const MIN_SCORE: i32 = 0;
    const MAX_SCORE: i32 = MAX_CUSTOM_PRIORITY_SCORE as i32;

    let mut errs = ErrorList::new();
    if shape.is_empty() {
        errs.push(field::Error::required(
            path,
            "at least one point must be specified",
        ));
        return errs;
    }
    for i in 1..shape.len() {
        if shape[i - 1].utilization >= shape[i].utilization {
            errs.push(field::Error::invalid(
                &path.index(i).child("utilization"),
                shape[i].utilization,
                "values must be sorted in increasing order",
            ));
            break;
        }
    }
    for (i, point) in shape.iter().enumerate() {
        if point.utilization < MIN_UTILIZATION || point.utilization > MAX_UTILIZATION {
            errs.push(field::Error::invalid(
                &path.index(i).child("utilization"),
                point.utilization,
                format!("not in valid range [{MIN_UTILIZATION}, {MAX_UTILIZATION}]"),
            ));
        }
        if point.score < MIN_SCORE || point.score > MAX_SCORE {
            errs.push(field::Error::invalid(
                &path.index(i).child("score"),
                point.score,
                format!("not in valid range [{MIN_SCORE}, {MAX_SCORE}]"),
            ));
        }
    }
    errs
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

    #[test]
    fn config_doc_custom_shape_gate_on() {
        let doc = r#"
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: default-scheduler
  pluginConfig:
  - name: VolumeBinding
    args:
      bindTimeoutSeconds: 30
      shape:
      - utilization: 0
        score: 0
      - utilization: 100
        score: 10
"#;
        let a = volume_binding_args_from_config(doc, true).unwrap();
        assert_eq!(a, args(30, Some(vec![pt(0, 0), pt(100, 10)])));
    }

    #[test]
    fn config_doc_shape_with_gate_off_rejected() {
        let doc = "profiles:\n- pluginConfig:\n  - name: VolumeBinding\n    args:\n      shape:\n      - {utilization: 0, score: 1}\n";
        let err = volume_binding_args_from_config(doc, false).unwrap_err();
        assert!(
            err.contains("profiles[0].pluginConfig[0].args.shape"),
            "{err}"
        );
        assert!(err.contains("StorageCapacityScoring"), "{err}");
    }

    #[test]
    fn config_doc_without_volume_binding_gets_defaults() {
        let a = volume_binding_args_from_config("profiles: []\n", true).unwrap();
        assert_eq!(a, args(600, Some(vec![pt(0, 10), pt(100, 0)])));
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

    // #3202: file loader shared by the scheduler binary and the all-in-one
    // binary (--scheduler-config).
    #[test]
    fn load_from_file_reads_defaults_and_validates() {
        let dir = std::env::temp_dir().join(format!("sched-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ok = dir.join("ok.yaml");
        std::fs::write(
            &ok,
            "profiles:\n- pluginConfig:\n  - name: VolumeBinding\n    args:\n      bindTimeoutSeconds: 42\n",
        )
        .unwrap();
        let a = load_volume_binding_args(ok.to_str().unwrap()).unwrap();
        assert_eq!(a.bind_timeout_seconds, Some(42));
        let bad = dir.join("bad.yaml");
        std::fs::write(
            &bad,
            "profiles:\n- pluginConfig:\n  - name: VolumeBinding\n    args:\n      bindTimeoutSeconds: -1\n",
        )
        .unwrap();
        let err = load_volume_binding_args(bad.to_str().unwrap()).unwrap_err();
        assert!(err.contains("bindTimeoutSeconds"), "{err}");
        let err = load_volume_binding_args(dir.join("missing.yaml").to_str().unwrap()).unwrap_err();
        assert!(err.contains("missing.yaml"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
