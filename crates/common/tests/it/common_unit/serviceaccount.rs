//! Unit tests moved from `src/serviceaccount.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{
        ConfigMapProjection, DownwardAPIProjection, DownwardAPIVolumeFile, KeyToPath,
        ObjectFieldSelector, PodSpec, ProjectedVolumeSource, ServiceAccountTokenProjection, Volume,
        VolumeMount, VolumeProjection,
    };
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::Container;
    use rusternetes_common::serviceaccount::*;

    fn sa_secrets(
        names: &[&str],
    ) -> Vec<rusternetes_common::resources::service_account::LocalObjectReference> {
        names
            .iter()
            .map(
                |n| rusternetes_common::resources::service_account::LocalObjectReference {
                    name: n.to_string(),
                },
            )
            .collect()
    }

    #[test]
    fn propagate_copies_sa_secrets_when_pod_has_none() {
        let mut spec = spec_with_container();
        let secrets = sa_secrets(&["regcred", "other"]);
        let copied = propagate_image_pull_secrets(&mut spec, Some(&secrets));
        assert_eq!(copied, 2);
        let pod_secrets = spec.image_pull_secrets.as_ref().unwrap();
        assert_eq!(pod_secrets.len(), 2);
        assert_eq!(pod_secrets[0].name, "regcred");
        assert_eq!(pod_secrets[1].name, "other");
    }

    #[test]
    fn propagate_copies_sa_secrets_when_pod_list_empty() {
        let mut spec = spec_with_container();
        spec.image_pull_secrets = Some(vec![]);
        let secrets = sa_secrets(&["regcred"]);
        let copied = propagate_image_pull_secrets(&mut spec, Some(&secrets));
        assert_eq!(copied, 1);
        assert_eq!(spec.image_pull_secrets.as_ref().unwrap()[0].name, "regcred");
    }

    #[test]
    fn propagate_pod_list_wins_no_merge() {
        let mut spec = spec_with_container();
        spec.image_pull_secrets = Some(vec![
            rusternetes_common::resources::pod::LocalObjectReference {
                name: "pod-own".to_string(),
            },
        ]);
        let secrets = sa_secrets(&["regcred"]);
        let copied = propagate_image_pull_secrets(&mut spec, Some(&secrets));
        assert_eq!(copied, 0);
        let pod_secrets = spec.image_pull_secrets.as_ref().unwrap();
        assert_eq!(pod_secrets.len(), 1, "no merge: pod list wins outright");
        assert_eq!(pod_secrets[0].name, "pod-own");
    }

    #[test]
    fn propagate_noop_when_sa_has_none() {
        let mut spec = spec_with_container();
        assert_eq!(propagate_image_pull_secrets(&mut spec, None), 0);
        assert!(spec.image_pull_secrets.is_none());
        assert_eq!(propagate_image_pull_secrets(&mut spec, Some(&[])), 0);
        assert!(spec.image_pull_secrets.is_none());
    }

    fn spec_with_container() -> PodSpec {
        PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "busybox".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn ensure_service_account_name_defaults() {
        let mut spec = spec_with_container();
        assert_eq!(ensure_service_account_name(&mut spec), "default");
        assert_eq!(spec.service_account_name.as_deref(), Some("default"));
        spec.service_account_name = Some("custom".to_string());
        assert_eq!(ensure_service_account_name(&mut spec), "custom");
    }

    #[test]
    fn add_volume_injects_three_sources_and_mounts() {
        let mut spec = spec_with_container();
        add_kube_api_access_volume(&mut spec);
        let vol = spec
            .volumes
            .as_ref()
            .unwrap()
            .iter()
            .find(|v| v.name == SA_TOKEN_VOLUME_NAME)
            .expect("volume injected");
        let sources = vol.projected.as_ref().unwrap().sources.as_ref().unwrap();
        assert_eq!(sources.len(), 3, "token + ca.crt + namespace");
        let mount = spec.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.mount_path == SA_TOKEN_MOUNT_PATH)
            .expect("mount injected");
        assert_eq!(mount.read_only, Some(true));
    }

    #[test]
    fn add_volume_is_idempotent() {
        let mut spec = spec_with_container();
        add_kube_api_access_volume(&mut spec);
        add_kube_api_access_volume(&mut spec);
        assert_eq!(
            spec.volumes
                .as_ref()
                .unwrap()
                .iter()
                .filter(|v| v.name == SA_TOKEN_VOLUME_NAME)
                .count(),
            1
        );
        assert_eq!(
            spec.containers[0]
                .volume_mounts
                .as_ref()
                .unwrap()
                .iter()
                .filter(|m| m.mount_path == SA_TOKEN_MOUNT_PATH)
                .count(),
            1
        );
    }
}
