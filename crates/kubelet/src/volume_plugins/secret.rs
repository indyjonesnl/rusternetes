use crate::atomic_writer::FileProjection;
use crate::volume_plugins::{Attributes, Mounter, Spec, VolumeHost, VolumePlugin};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use rusternetes_common::resources::{KeyToPath, Pod, Secret, SecretVolumeSource};
use rusternetes_storage::{build_key, Storage, StorageBackend};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::info;

/// Port of `pkg/volume/secret/secret.go`.
pub struct SecretPlugin {
    host: Arc<dyn VolumeHost>,
}

impl SecretPlugin {
    pub fn new(host: Arc<dyn VolumeHost>) -> Self {
        Self { host }
    }
}

#[async_trait]
impl VolumePlugin for SecretPlugin {
    fn name(&self) -> &'static str {
        crate::pod_dirs::plugin::SECRET
    }

    /// `GetVolumeName` (`secret.go:72-79`): the referenced Secret's name.
    ///
    /// `SecretVolumeSource.secret_name` is `Option<String>` here where
    /// upstream's is a plain string whose zero value is `""`;
    /// `unwrap_or_default` reproduces that zero value.
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        let Some(source) = &spec.volume.secret else {
            return Err(anyhow!("Spec does not reference a Secret volume type"));
        };
        Ok(source.secret_name.clone().unwrap_or_default())
    }

    /// `RequiresRemount` (`secret.go:85-87`): `true`.
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        true
    }

    /// `SupportsSELinuxContextMount` (`secret.go:93-95`): `(false, nil)`.
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }

    /// `CanSupport` (`secret.go:81-83`), verbatim:
    ///
    /// ```go
    /// return spec.Volume != nil && spec.Volume.Secret != nil
    /// ```
    ///
    /// secret has no PersistentVolume form — upstream's `CanSupport` has no
    /// `spec.PersistentVolume` arm either — so only the inline volume is
    /// checked, same as emptyDir and configMap.
    fn can_support(&self, spec: &Spec<'_>) -> bool {
        spec.volume.secret.is_some()
    }

    async fn new_mounter(&self, spec: &Spec<'_>, pod: &Pod) -> Result<Box<dyn Mounter>> {
        Ok(Box::new(SecretMounter {
            host: self.host.clone(),
            path: self
                .host
                .get_pod_volume_dir(&pod.metadata.uid, self.name(), &spec.volume.name),
            volume_name: spec.volume.name.clone(),
            namespace: pod
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".to_string()),
            secret: spec.volume.secret.clone().expect("checked by can_support"),
            storage: self.host.get_kube_client().cloned(),
            pod: pod.clone(),
        }))
    }

    /// `NewUnmounter` (`secret.go:112-136`): the wrapper plugins share one unmounter that
    /// delegates `TearDownAt` to emptyDir (`volumeutil.UnmountViaEmptyDir`).
    fn new_unmounter(
        &self,
        vol_name: &str,
        pod_uid: &str,
    ) -> Result<Box<dyn crate::volume_plugins::Unmounter>> {
        Ok(Box::new(
            crate::volume_plugins::util::WrappedEmptyDirUnmounter {
                host: self.host.clone(),
                plugin_name: self.name(),
                vol_name: vol_name.to_string(),
                pod_uid: pod_uid.to_string(),
            },
        ))
    }

    /// `ConstructVolumeSpec` (`secret.go:112-136`): a bare `SecretVolumeSource{SecretName: volName}`
    /// named after the volume.
    fn construct_volume_spec(
        &self,
        vol_name: &str,
        _mount_path: &str,
    ) -> Result<crate::volume_plugins::ReconstructedVolume> {
        crate::volume_plugins::util::reconstructed_volume(
            vol_name,
            serde_json::json!({"secret": {"secretName": vol_name}}),
        )
    }
}

/// `MakePayload` (`pkg/volume/secret/secret.go:210-247`).
///
/// With no `items`, every Secret key becomes a file at `defaultMode`. With
/// `items`, only the listed keys are projected, at `items[].path`, with
/// `items[].mode` or else `defaultMode`. A listed key the Secret lacks is
/// an error unless the volume is optional (secret.go:228-233).
pub(crate) fn make_payload(
    mappings: Option<&[KeyToPath]>,
    secret: &Secret,
    default_mode: u32,
    optional: bool,
) -> Result<BTreeMap<String, FileProjection>> {
    let empty = Default::default();
    let data = secret.data.as_ref().unwrap_or(&empty);
    let mut payload = BTreeMap::new();
    match mappings {
        None | Some([]) => {
            for (name, bytes) in data {
                payload.insert(
                    name.clone(),
                    FileProjection {
                        fs_user: None,
                        data: bytes.clone(),
                        mode: default_mode,
                    },
                );
            }
        }
        Some(items) => {
            for ktp in items {
                let Some(content) = data.get(&ktp.key) else {
                    if optional {
                        continue;
                    }
                    return Err(anyhow!("references non-existent secret key: {}", ktp.key));
                };
                payload.insert(
                    ktp.path.clone(),
                    FileProjection {
                        fs_user: None,
                        data: content.clone(),
                        mode: ktp.mode.map(|m| m as u32).unwrap_or(default_mode),
                    },
                );
            }
        }
    }
    Ok(payload)
}

struct SecretMounter {
    /// `plugin.host` (`secret.go:151`), for the wrapped emptyDir's metadata dir.
    host: Arc<dyn VolumeHost>,
    path: String,
    volume_name: String,
    namespace: String,
    secret: SecretVolumeSource,
    storage: Option<Arc<StorageBackend>>,
    /// `secretVolumeMounter.pod` (`secret.go:155`), read by
    /// `MakeNestedMountpoints`.
    pod: Pod,
}

#[async_trait]
impl Mounter for SecretMounter {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    /// `secretVolume.GetAttributes` (`pkg/volume/secret/secret.go:164-170`).
    fn get_attributes(&self) -> Attributes {
        Attributes {
            read_only: true,
            managed: true,
            selinux_relabel: true,
        }
    }

    async fn set_up_at(&self, dir: &str, args: &crate::volume_plugins::MounterArgs) -> Result<()> {
        // ---- moved verbatim from create_volume's secret branch
        //      (991a503d:crates/kubelet/src/volumes.rs:1062-1325) ----
        let storage = self
            .storage
            .as_ref()
            .context("Storage not available for Secret volumes")?;

        let secret_source = &self.secret;

        let secret_name = secret_source
            .secret_name
            .as_ref()
            .context("Secret volume must specify secret_name")?;

        let is_optional = secret_source.optional.unwrap_or(false);

        let key = build_key("secrets", Some(&self.namespace), secret_name);
        let secret_result: Result<Secret, _> = storage.get(&key).await;

        let volume_dir = dir;

        let secret = match secret_result {
            Ok(s) => Some(s),
            Err(e) => {
                if is_optional {
                    info!(
                        "Optional Secret {} not found in namespace {}, creating empty volume",
                        secret_name, &self.namespace
                    );
                    None
                } else {
                    // Required secret not found — return error so the kubelet
                    // retries on next sync. K8s leaves the pod in Pending with
                    // ContainerCreating and retries until the secret exists.
                    // K8s ref: pkg/kubelet/kubelet_pods.go — makeVolumes
                    return Err(anyhow::anyhow!(
                        "Secret {} not found in namespace {}: {}",
                        secret_name,
                        self.namespace,
                        e
                    ));
                }
            }
        };

        // Upstream's API defaulting always fills `defaultMode` (0644); a
        // volume that reaches us without it takes the same default.
        let secret_default_mode = secret_source.default_mode.unwrap_or(0o644);

        // `SetUpAt` (secret.go:167-172): an optional Secret that is not found
        // is replaced by an empty one, so the volume is still projected.
        let empty = Secret::new(secret_name.clone(), self.namespace.clone());
        let secret_ref = secret.as_ref().unwrap_or(&empty);

        // `MakePayload` (secret.go:210-247).
        let payload = make_payload(
            secret_source.items.as_deref(),
            secret_ref,
            secret_default_mode as u32,
            is_optional,
        )?;

        // No service-account token or CA special case here: upstream's secret
        // plugin projects the Secret verbatim (secret.go:162-208, MakePayload
        // :210-247). SA tokens and the cluster CA reach pods through the
        // projected `kube-api-access` volume (serviceAccountToken source +
        // `kube-root-ca.crt` ConfigMap; plugin/pkg/admission/serviceaccount/
        // admission.go TokenVolumeSource), injected at admission (#2638).

        // `wrapped.SetUpAt(dir, ...)` (secret.go:179-181) runs AFTER `getSecret`
        // and `MakePayload` (:166-177), so a missing Secret or bad item leaves
        // no volume behind (`TestInvalidPathSecret`, secret_test.go:365). The
        // wrapped emptyDir's `setupDir` creates the root at 0777. Its fsGroup
        // ownership runs in the AtomicWriter's `setPerms` (:242-247) below.
        crate::volume_plugins::empty_dir::setup_dir(volume_dir)
            .context("Failed to create Secret volume directory")?;
        // The wrapped emptyDir's `SetUpAt` ends with
        // `volumeutil.SetReady(ed.getMetaDir())` (empty_dir.go:284), which
        // creates `<pod>/plugins/kubernetes.io~empty-dir/wrapped_<vol>`
        // (`TestPlugin`, secret_test.go:340-348).
        let meta_dir = crate::volume_plugins::util::wrapped_empty_dir_meta_dir(
            &self.host,
            &self.volume_name,
            &self.pod.metadata.uid,
        );
        crate::volume_plugins::util::set_ready(&meta_dir);
        // `volumeutil.MakeNestedMountpoints(b.volName, dir, b.pod)`
        // (secret.go:181-183), before the failure-teardown `defer` is armed.
        crate::volume_plugins::util::nested_volumes::make_nested_mountpoints(
            &self.volume_name,
            std::path::Path::new(volume_dir),
            &self.pod,
        )?;

        // `writer.Write(payload, setPerms)` (secret.go:196-204) via the
        // upstream AtomicWriter port: unchanged content is a no-op. The
        // `defer` at secret.go:183-200 runs `unmounter.TearDown()` when the
        // write fails; emptyDir `TearDownAt` removes the volume directory
        // (same as configMap/downwardAPI).
        if let Err(e) = crate::volume_ownership::write_payload_with_ownership(
            std::path::Path::new(volume_dir),
            &payload,
            args.fs_group,
            true,
        ) {
            // emptyDir `TearDownAt` removes the ready dir first (empty_dir.go:497-500).
            let _ = std::fs::remove_dir_all(&meta_dir);
            if let Err(td) = std::fs::remove_dir_all(volume_dir) {
                tracing::error!("Error tearing down volume {}: {}", self.volume_name, td);
            }
            return Err(
                anyhow::Error::new(e).context(format!("failed to project Secret {secret_name}"))
            );
        }

        info!(
            "Created Secret volume {} at {}",
            self.volume_name, volume_dir
        );
        // ---- end moved body ----
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{Pod, Volume};
    use serde_json::json;

    fn plugin() -> SecretPlugin {
        SecretPlugin::new(std::sync::Arc::new(
            crate::volume_plugins::KubeletVolumeHost::new(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    fn inline_secret(name: &str) -> Volume {
        serde_json::from_value(json!({"name": "sec", "secret": {"secretName": name}})).unwrap()
    }

    fn config_map_volume() -> Volume {
        serde_json::from_value(json!({"name": "cfg", "configMap": {"name": "x"}})).unwrap()
    }

    fn test_pod() -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": []}
        }))
        .unwrap()
    }

    #[test]
    fn supports_an_inline_secret() {
        let v = inline_secret("x");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_other_kinds() {
        let v = config_map_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
        };
        assert!(!plugin().can_support(&spec));
    }

    #[test]
    fn plugin_name_is_the_upstream_name() {
        assert_eq!(plugin().name(), crate::pod_dirs::plugin::SECRET);
    }

    /// The mounter's path comes from the pod-directory layout the host
    /// provides, keyed on the plugin name — same contract as every other
    /// plugin (host.rs `pod_volume_dir_matches_pod_dirs`).
    #[tokio::test]
    async fn get_path_uses_the_secret_plugin_directory() {
        let v = inline_secret("x");
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
        };
        let pod = test_pod();
        let m = plugin().new_mounter(&spec, &pod).await.unwrap();
        assert_eq!(
            m.get_path(),
            "/var/lib/rusternetes/pods/uid-1/volumes/kubernetes.io~secret/sec"
        );
    }

    // ---- set_up behaviour, ported from upstream secret_test.go ----

    /// Build a plugin whose host serves `secret` from an in-memory store,
    /// rooted at `base`.
    async fn plugin_with_secret(base: &str, secret: Option<Secret>) -> SecretPlugin {
        let storage = Arc::new(StorageBackend::new_memory());
        if let Some(s) = secret {
            let key = build_key("secrets", Some("default"), &s.metadata.name);
            storage.create(&key, &s).await.unwrap();
        }
        SecretPlugin::new(Arc::new(crate::volume_plugins::KubeletVolumeHost::new(
            base.to_string(),
            Some(storage),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            std::collections::HashMap::new(),
        )))
    }

    fn opaque(name: &str, data: &[(&str, &[u8])]) -> Secret {
        let mut s = Secret::new(name, "default");
        s.data = Some(
            data.iter()
                .map(|(k, v)| (k.to_string(), v.to_vec()))
                .collect(),
        );
        s
    }

    fn volume_with_items(items: serde_json::Value, optional: bool) -> Volume {
        serde_json::from_value(json!({"name": "sec", "secret": {
            "secretName": "s", "items": items, "optional": optional}}))
        .unwrap()
    }

    /// `MakePayload` (secret.go:219-232): a key named in `items` that the
    /// Secret lacks is an error unless the volume is optional.
    /// `TestPluginOptionalKeys` / `TestMakePayload` "no defaultMode" family.
    #[tokio::test]
    async fn required_item_missing_from_secret_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().to_str().unwrap();
        let p = plugin_with_secret(base, Some(opaque("s", &[("a", b"1")]))).await;
        let v = volume_with_items(json!([{"key": "nope", "path": "p"}]), false);
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
        };
        let m = p.new_mounter(&spec, &test_pod()).await.unwrap();
        let err = m.set_up().await.unwrap_err().to_string();
        assert!(
            err.contains("references non-existent secret key: nope"),
            "{err}"
        );
    }

    /// Same, but optional: the missing key is skipped, setup succeeds
    /// (`TestPluginOptionalKeys`).
    #[tokio::test]
    async fn optional_item_missing_from_secret_is_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().to_str().unwrap();
        let p = plugin_with_secret(base, Some(opaque("s", &[("a", b"1")]))).await;
        let v = volume_with_items(
            json!([{"key": "nope", "path": "p"}, {"key": "a", "path": "a"}]),
            true,
        );
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
        };
        let m = p.new_mounter(&spec, &test_pod()).await.unwrap();
        m.set_up().await.unwrap();
        let dir = std::path::PathBuf::from(m.get_path());
        assert_eq!(std::fs::read(dir.join("a")).unwrap(), b"1");
        assert!(!dir.join("p").exists());
    }

    /// `AtomicWriter` layout (atomic_writer.go): user-visible files are
    /// symlinks through `..data`, and re-running SetUp on unchanged content
    /// is a no-op, so the `..data` target must not change.
    #[tokio::test]
    async fn set_up_projects_through_atomic_writer_and_is_idempotent() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().to_str().unwrap();
        let p = plugin_with_secret(base, Some(opaque("s", &[("a", b"1")]))).await;
        let v = volume_with_items(json!([{"key": "a", "path": "a", "mode": 256}]), false);
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
        };
        let m = p.new_mounter(&spec, &test_pod()).await.unwrap();
        m.set_up().await.unwrap();
        let dir = std::path::PathBuf::from(m.get_path());
        let first = std::fs::read_link(dir.join("..data")).expect("..data symlink");
        assert!(std::fs::symlink_metadata(dir.join("a"))
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            std::fs::metadata(dir.join("a"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o400
        );
        m.set_up().await.unwrap();
        assert_eq!(std::fs::read_link(dir.join("..data")).unwrap(), first);
    }

    async fn mounter_at(
        base: &std::path::Path,
        items: serde_json::Value,
        secret: Option<Secret>,
    ) -> Box<dyn Mounter> {
        let p = plugin_with_secret(base.to_str().unwrap(), secret).await;
        let v = volume_with_items(items, false);
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
        };
        p.new_mounter(&spec, &test_pod()).await.unwrap()
    }

    /// `TestInvalidPathSecret` (secret_test.go:365-417): `MakePayload` fails
    /// BEFORE `wrapped.SetUpAt` (secret.go:177-181), so the volume path must
    /// not exist afterwards ("Expected path %s to not exist").
    #[tokio::test]
    async fn invalid_path_secret_does_not_create_the_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_at(
            tmp.path(),
            json!([{"key": "missing", "path": "missing"}]),
            Some(opaque("s", &[("a", b"1")])),
        )
        .await;
        assert!(m.set_up().await.is_err());
        assert!(!std::path::Path::new(&m.get_path()).exists());
    }

    /// `getSecret` fails before `wrapped.SetUpAt` too (secret.go:166-176).
    #[tokio::test]
    async fn missing_required_secret_does_not_create_the_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_at(tmp.path(), json!([]), None).await;
        assert!(m.set_up().await.is_err());
        assert!(!std::path::Path::new(&m.get_path()).exists());
    }

    /// The wrapped emptyDir's `setupDir` (empty_dir.go:447-486) leaves the
    /// volume root at 0777 (same as downwardAPI).
    #[tokio::test]
    async fn volume_root_is_the_wrapped_empty_dir_mode() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_at(
            tmp.path(),
            json!([{"key": "a", "path": "a", "mode": 256}]),
            Some(opaque("s", &[("a", b"1")])),
        )
        .await;
        m.set_up().await.unwrap();
        let mode = std::fs::metadata(m.get_path())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o777);
    }

    /// `TestPlugin` (secret_test.go:340-348): "secret volume should create its
    /// own empty wrapper path" -- the wrapped emptyDir's `SetReady`
    /// (empty_dir.go:284, `volumeutil.SetReady` util.go:93) creates
    /// `<pod>/plugins/kubernetes.io~empty-dir/wrapped_<vol>`.
    #[tokio::test]
    async fn set_up_creates_the_wrapped_empty_dir_metadata_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_at(
            tmp.path(),
            json!([{"key": "a", "path": "a"}]),
            Some(opaque("s", &[("a", b"1")])),
        )
        .await;
        m.set_up().await.unwrap();
        let wrapper = tmp
            .path()
            .join("pods/uid-1/plugins/kubernetes.io~empty-dir/wrapped_sec");
        assert!(wrapper.is_dir(), "missing {}", wrapper.display());
        assert!(wrapper.join("ready").exists());
    }

    /// SetUpAt's `defer` (secret.go:183-200): a failed AtomicWriter runs
    /// `unmounter.TearDown()`, and emptyDir `TearDownAt` removes the volume.
    #[tokio::test]
    async fn failed_write_tears_the_volume_down() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_at(
            tmp.path(),
            json!([{"key": "a", "path": "a"}]),
            Some(opaque("s", &[("a", b"1")])),
        )
        .await;
        let dir = std::path::PathBuf::from(m.get_path());
        std::fs::create_dir_all(dir.join("..data_tmp/x")).unwrap();
        assert!(m.set_up().await.is_err());
        assert!(
            !dir.exists(),
            "volume dir must be removed after a failed write"
        );
    }

    /// `TestPluginReboot` (secret_test.go:419-470): the volume dir already
    /// exists (post-reboot state); SetUp still writes the data.
    #[tokio::test]
    async fn plugin_reboot_sets_up_over_an_existing_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let m = mounter_at(
            tmp.path(),
            json!([{"key": "a", "path": "a"}]),
            Some(opaque("s", &[("a", b"1")])),
        )
        .await;
        let dir = std::path::PathBuf::from(m.get_path());
        std::fs::create_dir_all(&dir).unwrap();
        m.set_up().await.unwrap();
        assert_eq!(std::fs::read(dir.join("a")).unwrap(), b"1");
    }

    /// `MakeNestedMountpoints` (secret.go:181): a volume mounted beneath the
    /// secret volume gets its mountpoint directory created inside it.
    #[tokio::test]
    async fn nested_mountpoints_are_created_inside_the_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let p = plugin_with_secret(
            tmp.path().to_str().unwrap(),
            Some(opaque("s", &[("a", b"1")])),
        )
        .await;
        let v = volume_with_items(json!([]), false);
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
        };
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": [{"name": "c", "image": "i", "volumeMounts": [
                {"name": "sec", "mountPath": "/etc/sec"},
                {"name": "other", "mountPath": "/etc/sec/sub/dir"}
            ]}]}
        }))
        .unwrap();
        let m = p.new_mounter(&spec, &pod).await.unwrap();
        m.set_up().await.unwrap();
        let dir = std::path::PathBuf::from(m.get_path());
        assert!(dir.join("sub/dir").is_dir());
        assert_eq!(std::fs::read(dir.join("a")).unwrap(), b"1");
    }
}
