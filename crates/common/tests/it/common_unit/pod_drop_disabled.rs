//! Unit tests moved from `src/pod_drop_disabled.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::feature_gates::{enabled, Feature};
    pub use rusternetes_common::resources::pod::{Container, EnvVar, Lifecycle};
    pub use rusternetes_common::resources::{ContainerStatus, Pod, PodSpec, PodStatus};
}

mod tests {
    //! `TestDropFileKeyRefInUse` and `TestPodFileKeyRefInUse`
    //! (pkg/api/pod/util_test.go:5400-5920).
    use super::__parent::*;
    use rusternetes_common::feature_gates::with_feature;
    use rusternetes_common::pod_drop_disabled::*;
    use rusternetes_common::resources::pod::{EnvVarSource, EphemeralContainer, FileKeySelector};

    fn file_env(name: &str) -> EnvVar {
        EnvVar {
            name: name.into(),
            value: None,
            value_from: Some(EnvVarSource {
                config_map_key_ref: None,
                secret_key_ref: None,
                field_ref: None,
                resource_field_ref: None,
                file_key_ref: Some(FileKeySelector {
                    volume_name: "v".into(),
                    path: "/p".into(),
                    key: Some("k".into()),
                    optional: None,
                }),
            }),
        }
    }

    fn plain_env(name: &str) -> EnvVar {
        EnvVar {
            name: name.into(),
            value: Some("x".into()),
            value_from: None,
        }
    }

    fn container(env: Option<Vec<EnvVar>>) -> Container {
        Container {
            name: "c".into(),
            env,
            ..Default::default()
        }
    }

    fn spec_with(env: Vec<EnvVar>) -> PodSpec {
        PodSpec {
            containers: vec![container(Some(env))],
            ..Default::default()
        }
    }

    fn has_ref(spec: &PodSpec) -> bool {
        pod_file_key_ref_in_use(Some(spec))
    }

    #[test]
    fn gate_off_and_no_old_spec_drops() {
        let _g = with_feature(Feature::EnvFiles, false);
        let mut spec = spec_with(vec![plain_env("A"), file_env("B")]);
        drop_file_key_ref_in_use(&mut spec, None);
        assert!(!has_ref(&spec));
        // only the ref goes: the env var and its (now empty) source stay
        let env = spec.containers[0].env.as_ref().unwrap();
        assert_eq!(env.len(), 2);
        assert!(env[1].value_from.is_some());
        assert_eq!(env[0].value.as_deref(), Some("x"));
    }

    #[test]
    fn gate_off_and_old_spec_without_a_ref_drops() {
        let _g = with_feature(Feature::EnvFiles, false);
        let old = spec_with(vec![plain_env("A")]);
        let mut spec = spec_with(vec![file_env("B")]);
        drop_file_key_ref_in_use(&mut spec, Some(&old));
        assert!(!has_ref(&spec));
    }

    #[test]
    fn gate_off_and_old_spec_with_a_ref_keeps() {
        let _g = with_feature(Feature::EnvFiles, false);
        let old = spec_with(vec![file_env("A")]);
        let mut spec = spec_with(vec![file_env("B")]);
        drop_file_key_ref_in_use(&mut spec, Some(&old));
        assert!(has_ref(&spec));
    }

    #[test]
    fn gate_on_keeps() {
        let _g = with_feature(Feature::EnvFiles, true);
        let mut spec = spec_with(vec![file_env("B")]);
        drop_file_key_ref_in_use(&mut spec, None);
        assert!(has_ref(&spec));
    }

    #[test]
    fn drops_in_every_container_kind_and_tolerates_nil_env() {
        let _g = with_feature(Feature::EnvFiles, false);
        let mut spec = PodSpec {
            containers: vec![container(None), container(Some(vec![file_env("A")]))],
            init_containers: Some(vec![container(Some(vec![file_env("B")]))]),
            ephemeral_containers: Some(vec![EphemeralContainer {
                name: "e".into(),
                env: Some(vec![file_env("C")]),
                ..Default::default()
            }]),
            ..Default::default()
        };
        assert!(has_ref(&spec));
        drop_file_key_ref_in_use(&mut spec, None);
        assert!(!has_ref(&spec));
        assert!(spec.containers[0].env.is_none());
    }

    #[test]
    fn in_use_checks_every_container_kind() {
        assert!(!pod_file_key_ref_in_use(None));
        assert!(!has_ref(&spec_with(vec![plain_env("A")])));
        assert!(!has_ref(&PodSpec {
            containers: vec![container(None)],
            ..Default::default()
        }));
        assert!(has_ref(&PodSpec {
            init_containers: Some(vec![container(Some(vec![file_env("A")]))]),
            ..Default::default()
        }));
        assert!(has_ref(&PodSpec {
            ephemeral_containers: Some(vec![EphemeralContainer {
                env: Some(vec![file_env("A")]),
                ..Default::default()
            }]),
            ..Default::default()
        }));
    }
}
