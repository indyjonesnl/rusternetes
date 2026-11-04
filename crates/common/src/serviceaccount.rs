//! ServiceAccount admission helpers shared by the api-server (HTTP pod-create
//! admission) and the controller-manager (controllers that create pods by
//! writing directly to storage, bypassing the HTTP admission chain).
//!
//! K8s ref: `plugin/pkg/admission/serviceaccount/admission.go`. The kubelet's
//! projected `kube-api-access-*` volume carries three sources — the bound SA
//! token, the cluster CA (`kube-root-ca.crt`), and the pod namespace — mounted
//! read-only at `/var/run/secrets/kubernetes.io/serviceaccount`.

use crate::resources::{
    ConfigMapProjection, DownwardAPIProjection, DownwardAPIVolumeFile, KeyToPath,
    ObjectFieldSelector, PodSpec, ProjectedVolumeSource, ServiceAccountTokenProjection, Volume,
    VolumeMount, VolumeProjection,
};

/// Canonical name of the injected projected volume.
pub const SA_TOKEN_VOLUME_NAME: &str = "kube-api-access";
/// Canonical mount path of the SA token volume.
pub const SA_TOKEN_MOUNT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// Ensure `spec.service_account_name` is set, defaulting to `"default"`.
/// Returns the effective service account name.
pub fn ensure_service_account_name(spec: &mut PodSpec) -> String {
    match &spec.service_account_name {
        Some(name) => name.clone(),
        None => {
            spec.service_account_name = Some("default".to_string());
            "default".to_string()
        }
    }
}

/// Build the projected `kube-api-access` SA-token volume (token + ca.crt +
/// namespace), matching upstream `TokenVolumeSource()`.
fn sa_token_volume() -> Volume {
    Volume {
        legacy_sources: Default::default(),
        name: SA_TOKEN_VOLUME_NAME.to_string(),
        empty_dir: None,
        host_path: None,
        config_map: None,
        secret: None,
        persistent_volume_claim: None,
        downward_api: None,
        csi: None,
        ephemeral: None,
        nfs: None,
        iscsi: None,
        projected: Some(ProjectedVolumeSource {
            sources: Some(vec![
                VolumeProjection {
                    service_account_token: Some(ServiceAccountTokenProjection {
                        path: "token".to_string(),
                        expiration_seconds: Some(3607),
                        audience: None,
                    }),
                    config_map: None,
                    secret: None,
                    downward_api: None,
                    cluster_trust_bundle: None,
                    ..Default::default()
                },
                VolumeProjection {
                    service_account_token: None,
                    config_map: Some(ConfigMapProjection {
                        name: Some("kube-root-ca.crt".to_string()),
                        items: Some(vec![KeyToPath {
                            key: "ca.crt".to_string(),
                            path: "ca.crt".to_string(),
                            mode: None,
                        }]),
                        optional: None,
                    }),
                    secret: None,
                    downward_api: None,
                    cluster_trust_bundle: None,
                    ..Default::default()
                },
                VolumeProjection {
                    service_account_token: None,
                    config_map: None,
                    secret: None,
                    downward_api: Some(DownwardAPIProjection {
                        items: Some(vec![DownwardAPIVolumeFile {
                            path: "namespace".to_string(),
                            field_ref: Some(ObjectFieldSelector {
                                api_version: Some("v1".to_string()),
                                field_path: "metadata.namespace".to_string(),
                            }),
                            resource_field_ref: None,
                            mode: None,
                        }]),
                    }),
                    cluster_trust_bundle: None,
                    ..Default::default()
                },
            ]),
            default_mode: Some(0o644),
        }),
        image: None,
    }
}

/// Inject the `kube-api-access` projected volume into `spec` and mount it
/// read-only into every container and init container. Idempotent.
///
/// Port of `Plugin.mountServiceAccountToken`
/// (plugin/pkg/admission/serviceaccount/admission.go:416-480): an existing
/// volume named `kube-api-access-*` is reused (its name is the mount's); a
/// container already mounting `DefaultAPITokenMountPath` is left alone; and
/// the volume is added only when some container needed the mount.
/// Deviation: a fresh volume is named `kube-api-access` rather than
/// `GenerateName("kube-api-access-")`, as other components address it by that
/// name.
///
/// Callers are responsible for the `automountServiceAccountToken` decision —
/// only call this when the token should be mounted.
pub fn add_kube_api_access_volume(spec: &mut PodSpec) {
    let existing = spec
        .volumes
        .iter()
        .flatten()
        .find(|v| {
            v.name == SA_TOKEN_VOLUME_NAME
                || v.name.starts_with(&format!("{SA_TOKEN_VOLUME_NAME}-"))
        })
        .map(|v| v.name.clone());
    let has_token_volume = existing.is_some();
    let volume_name = existing.unwrap_or_else(|| SA_TOKEN_VOLUME_NAME.to_string());

    let mount = VolumeMount {
        name: volume_name,
        mount_path: SA_TOKEN_MOUNT_PATH.to_string(),
        read_only: Some(true),
        sub_path: None,
        sub_path_expr: None,
        mount_propagation: None,
        recursive_read_only: None,
    };

    // Returns whether the mount was added.
    let add_mount = |mounts: &mut Option<Vec<VolumeMount>>| -> bool {
        match mounts {
            Some(list) => {
                if list.iter().any(|m| m.mount_path == SA_TOKEN_MOUNT_PATH) {
                    false
                } else {
                    list.push(mount.clone());
                    true
                }
            }
            None => {
                *mounts = Some(vec![mount.clone()]);
                true
            }
        }
    };

    let mut needs_token_volume = false;
    // Upstream: init containers first, then containers.
    if let Some(init_containers) = &mut spec.init_containers {
        for container in init_containers {
            needs_token_volume |= add_mount(&mut container.volume_mounts);
        }
    }
    for container in &mut spec.containers {
        needs_token_volume |= add_mount(&mut container.volume_mounts);
    }

    if !has_token_volume && needs_token_volume {
        spec.volumes
            .get_or_insert_with(Vec::new)
            .push(sa_token_volume());
    }
}

/// Propagate a ServiceAccount's `imagePullSecrets` onto a pod spec.
///
/// K8s ref: `plugin/pkg/admission/serviceaccount/admission.go` (release-1.35)
/// `Plugin.Admit`: the SA's imagePullSecrets are copied onto the pod ONLY when
/// the pod declares none of its own — there is no merge, a pod-level list wins
/// outright. This applies regardless of `automountServiceAccountToken` (a pod
/// can opt out of token mounting but still inherit pull secrets).
///
/// Takes the resolved SA's secret list so it works for both the api-server
/// admission path and controllers that create pods directly in storage.
/// Returns the number of secrets copied (0 when the pod kept its own list or
/// the SA has none).
pub fn propagate_image_pull_secrets(
    spec: &mut PodSpec,
    sa_image_pull_secrets: Option<&[crate::resources::service_account::LocalObjectReference]>,
) -> usize {
    let pod_has_secrets = spec
        .image_pull_secrets
        .as_ref()
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    if pod_has_secrets {
        return 0;
    }
    let Some(sa_secrets) = sa_image_pull_secrets.filter(|s| !s.is_empty()) else {
        return 0;
    };
    spec.image_pull_secrets = Some(
        sa_secrets
            .iter()
            .map(|r| crate::resources::pod::LocalObjectReference {
                name: r.name.clone(),
            })
            .collect(),
    );
    sa_secrets.len()
}
