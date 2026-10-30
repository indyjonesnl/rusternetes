//! Unit tests moved from `src/volume_plugins/projected.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Context, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::{ConfigMap, KeyToPath, Pod, Secret, Volume};
    pub use rusternetes_kubelet::atomic_writer::FileProjection;
    pub use rusternetes_kubelet::volume_plugins::{
        Attributes, Mounter, Spec, VolumeHost, VolumePlugin,
    };
    pub use rusternetes_storage::{build_key, Storage, StorageBackend};
    pub use std::collections::{BTreeMap, HashMap};
    pub use std::sync::Arc;
    pub use tracing::{info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::projected::*;

    /// The args the operation generator builds from the pod
    /// (`operation_generator.go:582-589`).
    fn args_for(pod: &Pod) -> rusternetes_kubelet::volume_plugins::MounterArgs {
        rusternetes_kubelet::volume_plugins::MounterArgs {
            fs_user: rusternetes_kubelet::volume_plugins::util::fs_user_from(pod),
            fs_group: rusternetes_kubelet::volume_plugins::util::fs_group_from(pod),
            ..Default::default()
        }
    }

    // Upstream pkg/kubelet/token/token_manager.go:40 `maxTTL = 24 * time.Hour`,
    // :187 `now.After(iat.Add(maxTTL - jitter))` => refresh by 24h-jitter.
    #[test]
    fn refresh_after_is_capped_at_24h_minus_jitter_2358() {
        // 48h token: 80% (38.4h) exceeds the 24h cap.
        assert_eq!(token_refresh_after_secs(48 * 3600, 4), 24 * 3600 - 4);
        // 1h token: 80% rule wins.
        assert_eq!(token_refresh_after_secs(3600, 0), 2880);
        // Floor kept (pre-existing 60s minimum).
        assert_eq!(token_refresh_after_secs(10, 0), 60);
    }
    use rusternetes_common::resources::Volume;

    fn plugin() -> ProjectedPlugin {
        ProjectedPlugin::new(std::sync::Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                std::collections::HashMap::new(),
            ),
        ))
    }

    fn inline_projected() -> Volume {
        serde_json::from_value(serde_json::json!({"name": "proj", "projected": {}})).unwrap()
    }

    fn config_map_volume() -> Volume {
        serde_json::from_value(serde_json::json!({"name": "cfg", "configMap": {"name": "x"}}))
            .unwrap()
    }

    #[test]
    fn supports_an_inline_projected_volume() {
        let v = inline_projected();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(plugin().can_support(&spec));
    }

    /// A projected volume nests configMap and secret sources, but the
    /// configMap and secret plugins must NOT claim it — otherwise
    /// find_plugin_by_spec sees three matches and errors.
    #[test]
    fn rejects_a_plain_config_map() {
        let v = config_map_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(!plugin().can_support(&spec));
    }

    #[test]
    fn plugin_name_is_the_upstream_name() {
        assert_eq!(
            plugin().name(),
            rusternetes_kubelet::pod_dirs::plugin::PROJECTED
        );
    }

    // ---- behavioural tests through the mounter (projected.go SetUpAt /
    //      collectData) ----

    use rusternetes_common::resources::ConfigMap;
    use std::collections::HashMap;

    fn pod() -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "uid-1"},
            "spec": {"containers": [{"name": "c", "image": "i"}]}
        }))
        .unwrap()
    }

    fn vol(sources: serde_json::Value) -> Volume {
        serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"sources": sources}
        }))
        .unwrap()
    }

    async fn mounter(
        root: &std::path::Path,
        storage: &Arc<StorageBackend>,
        v: &Volume,
    ) -> Box<dyn Mounter> {
        let p = ProjectedPlugin::new(Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                root.to_string_lossy().to_string(),
                Some(storage.clone()),
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                HashMap::new(),
            ),
        ));
        let spec = Spec {
            volume: v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        p.new_mounter(&spec, &pod()).await.unwrap()
    }

    async fn put_cm(storage: &Arc<StorageBackend>, name: &str, k: &str, val: &str) {
        let mut d = HashMap::new();
        d.insert(k.to_string(), val.to_string());
        let cm = ConfigMap::new(name, "ns").with_data(d);
        storage
            .create(&build_key("configmaps", Some("ns"), name), &cm)
            .await
            .unwrap();
    }

    /// `collectData` (`projected.go:241-246`): a non-optional source whose
    /// object cannot be fetched appends to `errlist`, `SetUpAt` returns the
    /// aggregate and writes nothing.
    #[tokio::test]
    async fn missing_required_config_map_fails_set_up() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        let v = vol(serde_json::json!([{"configMap": {"name": "nope"}}]));
        let m = mounter(dir.path(), &st, &v).await;
        let err = m.set_up().await.unwrap_err().to_string();
        assert!(err.contains("nope"), "{err}");
        assert!(!std::path::Path::new(&m.get_path()).join("..data").exists());
    }

    /// `!(errors.IsNotFound(err) && optional)` (`projected.go:243`): an
    /// optional, absent source is an empty payload, not an error.
    #[tokio::test]
    async fn missing_optional_config_map_is_empty_payload() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        let v = vol(serde_json::json!([{"configMap": {"name": "nope", "optional": true}}]));
        let m = mounter(dir.path(), &st, &v).await;
        m.set_up().await.unwrap();
    }

    /// `configmap.MakePayload` (`configmap.go:289-293`): a mapped key that is
    /// absent from a non-optional configMap is an error.
    #[tokio::test]
    async fn missing_item_key_fails_set_up() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        put_cm(&st, "cm", "a", "1").await;
        let v = vol(serde_json::json!([
            {"configMap": {"name": "cm", "items": [{"key": "zzz", "path": "z"}]}}
        ]));
        let m = mounter(dir.path(), &st, &v).await;
        let err = m.set_up().await.unwrap_err().to_string();
        assert!(
            err.contains("configmap references non-existent config key: zzz"),
            "{err}"
        );
    }

    /// `volumeutil.NewAtomicWriter` + `writer.Write` (`projected.go:208-221`):
    /// content lands behind `..data`, and a re-SetUp of an unchanged payload
    /// leaves the on-disk layout untouched.
    #[tokio::test]
    async fn projects_through_atomic_writer_and_resync_is_inert() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        put_cm(&st, "cm", "a", "1").await;
        let v = vol(serde_json::json!([{"configMap": {"name": "cm"}}]));
        let m = mounter(dir.path(), &st, &v).await;
        m.set_up().await.unwrap();
        let root = std::path::PathBuf::from(m.get_path());
        assert!(root.join("..data").exists());
        assert!(std::fs::symlink_metadata(root.join("a"))
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(std::fs::read_to_string(root.join("a")).unwrap(), "1");
        let before = std::fs::read_link(root.join("..data")).unwrap();
        m.set_up().await.unwrap();
        assert_eq!(std::fs::read_link(root.join("..data")).unwrap(), before);
    }

    async fn mounter_for_pod(
        root: &std::path::Path,
        storage: &Arc<StorageBackend>,
        v: &Volume,
        pod: &Pod,
    ) -> Box<dyn Mounter> {
        let p = ProjectedPlugin::new(Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                root.to_string_lossy().to_string(),
                Some(storage.clone()),
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                HashMap::new(),
            ),
        ));
        let spec = Spec {
            volume: v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        p.new_mounter(&spec, pod).await.unwrap()
    }

    /// `TestPlugin` (projected_test.go:1174): "projected volume should create
    /// its own empty wrapper path" -- the wrapped emptyDir's `SetReady`
    /// creates `<pod>/plugins/kubernetes.io~empty-dir/wrapped_<vol>`.
    #[tokio::test]
    async fn set_up_creates_the_wrapped_empty_dir_metadata_dir() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        put_cm(&st, "cm", "a", "1").await;
        let v = vol(serde_json::json!([{"configMap": {"name": "cm"}}]));
        let m = mounter(dir.path(), &st, &v).await;
        m.set_up().await.unwrap();
        let wrapper = dir
            .path()
            .join("pods/uid-1/plugins/kubernetes.io~empty-dir/wrapped_proj");
        assert!(wrapper.is_dir(), "missing {}", wrapper.display());
        assert!(wrapper.join("ready").exists());
    }

    /// The wrapped emptyDir's `setupDir` leaves the volume root at 0777.
    #[tokio::test]
    async fn volume_root_is_the_wrapped_empty_dir_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        put_cm(&st, "cm", "a", "1").await;
        let v = vol(serde_json::json!([{"configMap": {"name": "cm"}}]));
        let m = mounter(dir.path(), &st, &v).await;
        m.set_up().await.unwrap();
        let mode = std::fs::metadata(m.get_path())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o777);
    }

    /// `MakeNestedMountpoints` (`projected.go:205`): a volume mounted beneath
    /// the projected volume gets its mountpoint directory created inside it.
    #[tokio::test]
    async fn nested_mountpoints_are_created_inside_the_volume() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        put_cm(&st, "cm", "a", "1").await;
        let v = vol(serde_json::json!([{"configMap": {"name": "cm"}}]));
        let pod: Pod = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "p", "namespace": "ns", "uid": "uid-1"},
            "spec": {"containers": [{"name": "c", "image": "i", "volumeMounts": [
                {"name": "proj", "mountPath": "/etc/proj"},
                {"name": "other", "mountPath": "/etc/proj/sub/dir"}
            ]}]}
        }))
        .unwrap();
        let m = mounter_for_pod(dir.path(), &st, &v, &pod).await;
        m.set_up().await.unwrap();
        let root = std::path::PathBuf::from(m.get_path());
        assert!(root.join("sub/dir").is_dir());
        assert_eq!(std::fs::read_to_string(root.join("a")).unwrap(), "1");
    }

    /// `TestInvalidPathProjected` (projected_test.go:1235): a bad item path
    /// fails `collectData` BEFORE `wrapped.SetUpAt`, so no volume is created.
    #[tokio::test]
    async fn collect_failure_does_not_create_the_volume() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        let v = vol(serde_json::json!([{"configMap": {"name": "missing"}}]));
        let m = mounter(dir.path(), &st, &v).await;
        assert!(m.set_up().await.is_err());
        assert!(!std::path::Path::new(&m.get_path()).exists());
    }

    /// SetUpAt's `defer` (`projected.go:209-222`): a failed AtomicWriter runs
    /// `unmounter.TearDown()`, and emptyDir `TearDownAt` removes the volume
    /// and its ready dir.
    #[tokio::test]
    async fn failed_write_tears_the_volume_down() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        put_cm(&st, "cm", "a", "1").await;
        let v = vol(serde_json::json!([{"configMap": {"name": "cm"}}]));
        let m = mounter(dir.path(), &st, &v).await;
        let root = std::path::PathBuf::from(m.get_path());
        std::fs::create_dir_all(root.join("..data_tmp/x")).unwrap();
        assert!(m.set_up().await.is_err());
        assert!(
            !root.exists(),
            "volume dir must be removed after failed write"
        );
        assert!(!dir
            .path()
            .join("pods/uid-1/plugins/kubernetes.io~empty-dir/wrapped_proj")
            .exists());
    }

    /// `collectData` merges every source into ONE payload (`projected.go:
    /// 270-272`); a later source overwrites an earlier one at the same path.
    #[tokio::test]
    async fn merges_sources_and_downward_api_errors_fail() {
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        put_cm(&st, "cm", "a", "1").await;
        let v = vol(serde_json::json!([
            {"configMap": {"name": "cm"}},
            {"downwardAPI": {"items": [{"path": "n", "fieldRef": {"fieldPath": "metadata.name"}}]}}
        ]));
        let m = mounter(dir.path(), &st, &v).await;
        m.set_up().await.unwrap();
        let root = std::path::PathBuf::from(m.get_path());
        assert_eq!(std::fs::read_to_string(root.join("a")).unwrap(), "1");
        assert_eq!(std::fs::read_to_string(root.join("n")).unwrap(), "p");

        // `downwardapi.CollectData` (`downwardapi.go:255-258`): a bad
        // fieldPath is an error, not an empty file.
        let dir2 = tempfile::tempdir().unwrap();
        let v2 = vol(serde_json::json!([
            {"downwardAPI": {"items": [{"path": "n", "fieldRef": {"fieldPath": "bogus.path"}}]}}
        ]));
        let m2 = mounter(dir2.path(), &st, &v2).await;
        assert!(m2.set_up().await.is_err());
    }

    /// `secret.MakePayload` (`secret.go:280`) wording, and the optional skip.
    #[test]
    fn secret_payload_missing_key_error_and_optional() {
        let s = Secret::new("s", "ns");
        let items = vec![KeyToPath {
            key: "k".into(),
            path: "p".into(),
            mode: Some(0o400),
        }];
        assert_eq!(
            secret_payload(Some(&items), &s, 0o644, false).unwrap_err(),
            "references non-existent secret key: k"
        );
        assert!(secret_payload(Some(&items), &s, 0o644, true)
            .unwrap()
            .is_empty());
    }

    /// `aggregate.Error()` (`errors.go:70-96`).
    #[test]
    fn aggregate_formats_like_upstream() {
        assert_eq!(aggregate_message(&["a".into()]), "a");
        assert_eq!(aggregate_message(&["a".into(), "b".into()]), "[a, b]");
        assert_eq!(aggregate_message(&["a".into(), "a".into()]), "a");
    }

    // ---- fsUser / fsGroup (projected.go:279-282, :208-214) ----

    fn pod_with_security(pod_sc: serde_json::Value, container_sc: serde_json::Value) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "uid-1"},
            "spec": {
                "securityContext": pod_sc,
                "serviceAccountName": "default",
                "containers": [{"name": "c", "image": "i", "securityContext": container_sc}]
            }
        }))
        .unwrap()
    }

    async fn mounter_for(
        root: &std::path::Path,
        storage: &Arc<StorageBackend>,
        v: &Volume,
        pod: &Pod,
    ) -> Box<dyn Mounter> {
        let p = ProjectedPlugin::new(Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                root.to_string_lossy().to_string(),
                Some(storage.clone()),
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                HashMap::new(),
            ),
        ));
        let spec = Spec {
            volume: v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        p.new_mounter(&spec, pod).await.unwrap()
    }

    /// The (uid, gid) a file created by this process gets — chowning to them
    /// is permitted without root.
    #[cfg(unix)]
    fn own_ids() -> (i64, i64) {
        use std::os::unix::fs::MetadataExt;
        let d = tempfile::tempdir().unwrap();
        let f = d.path().join("probe");
        std::fs::write(&f, b"").unwrap();
        let m = std::fs::metadata(&f).unwrap();
        (m.uid() as i64, m.gid() as i64)
    }

    /// `TestCollectDataWithServiceAccountToken` (`projected_test.go:724-880`)
    /// cases "fsUser != nil", "fsGroup != nil", "fsUser != nil && fsGroup !=
    /// nil" and the default: a service-account token is forced to 0600 when
    /// either is set (`projected.go:279-282`; "When FsGroup is set, we depend
    /// on SetVolumeOwnership to change from 0600 to 0640"), and the final
    /// on-disk mode follows. The e2e twin is `service_accounts.go:368`
    /// ("should set ownership and permission when RunAsUser or FsGroup is
    /// present"): `-rw-------` / `-rw-r-----` / `-rw-r-----` / `-rw-r--r--`.
    #[cfg(unix)]
    #[tokio::test]
    async fn service_account_token_mode_follows_fs_user_and_fs_group() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let (uid, gid) = own_ids();
        let cases: [(&str, serde_json::Value, serde_json::Value, u32); 4] = [
            (
                "runAsUser",
                serde_json::json!({}),
                serde_json::json!({"runAsUser": uid}),
                0o600,
            ),
            (
                "fsGroup",
                serde_json::json!({"fsGroup": gid}),
                serde_json::json!({}),
                0o640,
            ),
            (
                "runAsUser+fsGroup",
                serde_json::json!({"fsGroup": gid}),
                serde_json::json!({"runAsUser": uid}),
                0o640,
            ),
            (
                "neither",
                serde_json::json!({}),
                serde_json::json!({}),
                0o644,
            ),
        ];
        for (name, pod_sc, ctr_sc, want) in cases {
            let dir = tempfile::tempdir().unwrap();
            let st = Arc::new(StorageBackend::new_memory());
            let v = vol(serde_json::json!([
                {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}}
            ]));
            let pod = pod_with_security(pod_sc, ctr_sc);
            let m = mounter_for(dir.path(), &st, &v, &pod).await;
            m.set_up_with(&args_for(&pod)).await.unwrap();
            let tok = std::path::PathBuf::from(m.get_path()).join("token");
            let meta = std::fs::metadata(&tok).unwrap();
            assert_eq!(meta.permissions().mode() & 0o777, want, "{name}");
            assert_eq!(meta.uid() as i64, uid, "{name}");
            assert_eq!(meta.gid() as i64, gid, "{name}");
        }
    }

    /// `setPerms` is only run when the AtomicWriter writes (`atomic_writer.go:
    /// 196-201`): a re-SetUp of an unchanged volume with an fsGroup leaves the
    /// file untouched — ctime included — which is what #2390 asks of resync.
    #[cfg(unix)]
    #[tokio::test]
    async fn fs_group_ownership_is_not_reapplied_to_an_unchanged_volume() {
        use std::os::unix::fs::MetadataExt;
        let gid = own_ids().1;
        let dir = tempfile::tempdir().unwrap();
        let st = Arc::new(StorageBackend::new_memory());
        put_cm(&st, "cm", "a", "1").await;
        let v = vol(serde_json::json!([{"configMap": {"name": "cm"}}]));
        let pod = pod_with_security(serde_json::json!({"fsGroup": gid}), serde_json::json!({}));
        let m = mounter_for(dir.path(), &st, &v, &pod).await;
        m.set_up_with(&args_for(&pod)).await.unwrap();
        let real = std::fs::canonicalize(std::path::PathBuf::from(m.get_path()).join("a")).unwrap();
        let before = std::fs::metadata(&real).unwrap().ctime_nsec();
        let before_s = std::fs::metadata(&real).unwrap().ctime();
        m.set_up_with(&args_for(&pod)).await.unwrap();
        let after = std::fs::metadata(&real).unwrap();
        assert_eq!((before_s, before), (after.ctime(), after.ctime_nsec()));
    }

    // ---- ClusterTrustBundle source (projected.go:320-355) ----

    fn test_root(tag: &str) -> String {
        pem::encode_config(
            &pem::Pem::new("CERTIFICATE", tag.as_bytes().to_vec()),
            pem::EncodeConfig::new().set_line_ending(pem::LineEnding::LF),
        )
    }

    async fn put_ctb(
        st: &Arc<StorageBackend>,
        name: &str,
        signer: Option<&str>,
        labels: serde_json::Value,
        bundle: &str,
    ) {
        let mut spec = serde_json::json!({"trustBundle": bundle});
        if let Some(s) = signer {
            spec["signerName"] = s.into();
        }
        let ctb = serde_json::json!({
            "apiVersion": "certificates.k8s.io/v1beta1", "kind": "ClusterTrustBundle",
            "metadata": {"name": name, "labels": labels}, "spec": spec
        });
        st.create(&build_key("clustertrustbundles", None, name), &ctb)
            .await
            .unwrap();
    }

    async fn collect(
        st: &Arc<StorageBackend>,
        v: &Volume,
        pod: &Pod,
    ) -> Result<BTreeMap<String, FileProjection>> {
        let root = tempfile::tempdir().unwrap();
        let plugin = ProjectedPlugin::new(Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                root.path().to_string_lossy().to_string(),
                Some(st.clone()),
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                HashMap::new(),
            ),
        ));
        let spec = Spec {
            volume: v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        plugin
            .build_mounter(&spec, pod)
            .collect_data(&args_for(pod))
            .await
    }

    fn payload_bundle(p: &BTreeMap<String, FileProjection>, path: &str) -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = pem::parse_many(&p[path].data)
            .unwrap()
            .into_iter()
            .map(|b| b.into_contents())
            .collect();
        v.sort();
        v
    }

    /// `TestCollectDataWithClusterTrustBundle` (`projected_test.go:882-1039`),
    /// the three upstream cases: by name, by signer name + label selector, and
    /// by name with a non-default `defaultMode`.
    #[tokio::test]
    async fn collect_data_cluster_trust_bundle_upstream_cases() {
        let st = Arc::new(StorageBackend::new_memory());
        let root1 = test_root("root1");
        put_ctb(&st, "foo", None, serde_json::json!({}), &root1).await;
        put_ctb(
            &st,
            "foo:example:bar",
            Some("foo.example/bar"),
            serde_json::json!({"key": "value"}),
            &root1,
        )
        .await;
        let want = vec![b"root1".to_vec()];

        // "single ClusterTrustBundle by name"
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"clusterTrustBundle": {"name": "foo", "path": "bundle.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect(&st, &v, &pod()).await.unwrap();
        assert_eq!(payload_bundle(&got, "bundle.pem"), want);
        assert_eq!(got["bundle.pem"].mode, 0o644);
        assert_eq!(got["bundle.pem"].fs_user, None);

        // "single ClusterTrustBundle by signer name"
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"clusterTrustBundle": {
                    "signerName": "foo.example/bar",
                    "labelSelector": {"matchLabels": {"key": "value"}},
                    "path": "bundle.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect(&st, &v, &pod()).await.unwrap();
        assert_eq!(payload_bundle(&got, "bundle.pem"), want);
        assert_eq!(got["bundle.pem"].mode, 0o644);

        // "single ClusterTrustBundle by name, non-default mode"
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 384, "sources": [
                {"clusterTrustBundle": {"name": "foo", "path": "bundle.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect(&st, &v, &pod()).await.unwrap();
        assert_eq!(got["bundle.pem"].mode, 0o600);
    }

    /// `projected.go:344-352`: 0600 + `FsUser` when FsUser/FsGroup is set.
    #[tokio::test]
    async fn collect_data_cluster_trust_bundle_fs_user_and_group() {
        let st = Arc::new(StorageBackend::new_memory());
        put_ctb(&st, "foo", None, serde_json::json!({}), &test_root("r")).await;
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"clusterTrustBundle": {"name": "foo", "path": "bundle.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect(
            &st,
            &v,
            &pod_with_security(serde_json::json!({"fsGroup": 1000}), serde_json::json!({})),
        )
        .await
        .unwrap();
        assert_eq!(got["bundle.pem"].mode, 0o600);
        assert_eq!(got["bundle.pem"].fs_user, None);
        let got = collect(
            &st,
            &v,
            &pod_with_security(
                serde_json::json!({}),
                serde_json::json!({"runAsUser": 1000}),
            ),
        )
        .await
        .unwrap();
        assert_eq!(got["bundle.pem"].mode, 0o600);
        assert_eq!(got["bundle.pem"].fs_user, Some(1000));
    }

    /// `projected.go:312-317` and the `allowEmpty` handling (`:321-324`).
    #[tokio::test]
    async fn collect_data_cluster_trust_bundle_errors_and_optional() {
        let st = Arc::new(StorageBackend::new_memory());
        let neither: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"clusterTrustBundle": {"path": "b.pem"}}
            ]}
        }))
        .unwrap();
        assert_eq!(
            collect(&st, &neither, &pod())
                .await
                .unwrap_err()
                .to_string(),
            "ClusterTrustBundle projection requires either name or signerName to be set"
        );
        let missing: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"clusterTrustBundle": {"name": "nope", "path": "b.pem"}}
            ]}
        }))
        .unwrap();
        assert!(collect(&st, &missing, &pod()).await.is_err());
        let optional: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"clusterTrustBundle": {"name": "nope", "optional": true, "path": "b.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect(&st, &optional, &pod()).await.unwrap();
        assert!(got["b.pem"].data.is_empty());
    }

    /// `FakeKubeletVolumeHost.GetPodCertificateCredentialBundle`
    /// (`pkg/volume/testing/testing.go`): hardcoded `"key\n"`, `"cert\n"`.
    struct FakePodCertificateManager;

    #[async_trait]
    impl rusternetes_kubelet::podcertificate::Manager for FakePodCertificateManager {
        fn track_pod(&self, _pod: &Pod) {}
        fn forget_pod(&self, _pod: &Pod) {}
        async fn get_pod_certificate_credential_bundle(
            &self,
            _namespace: &str,
            _pod_name: &str,
            _pod_uid: &str,
            _volume_name: &str,
            _source_index: usize,
        ) -> Result<(Vec<u8>, Vec<u8>)> {
            Ok((b"key\n".to_vec(), b"cert\n".to_vec()))
        }
        fn metric_report(&self) -> rusternetes_kubelet::podcertificate::MetricReport {
            Default::default()
        }
    }

    async fn collect_pc(
        v: &Volume,
        pod: &Pod,
        with_manager: bool,
    ) -> Result<BTreeMap<String, FileProjection>> {
        let root = tempfile::tempdir().unwrap();
        let mut host = rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
            root.path().to_string_lossy().to_string(),
            Some(Arc::new(StorageBackend::new_memory())),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        );
        if with_manager {
            host = host.with_pod_certificate_manager(Arc::new(FakePodCertificateManager));
        }
        let plugin = ProjectedPlugin::new(Arc::new(host));
        let spec = Spec {
            volume: v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        plugin
            .build_mounter(&spec, pod)
            .collect_data(&args_for(pod))
            .await
    }

    /// `TestCollectDataWithPodCertificate` (`projected_test.go:1041-1142`),
    /// case "credential bundle".
    #[tokio::test]
    async fn collect_data_pod_certificate_credential_bundle() {
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"podCertificate": {"signerName": "example.com/foo", "keyType": "ED25519",
                                    "credentialBundlePath": "credbundle.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect_pc(&v, &pod(), true).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got["credbundle.pem"].data, b"key\ncert\n");
        assert_eq!(got["credbundle.pem"].mode, 0o644);
    }

    /// Same test, case "key and cert bundle".
    #[tokio::test]
    async fn collect_data_pod_certificate_key_and_cert() {
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"podCertificate": {"signerName": "example.com/foo", "keyType": "ED25519",
                                    "keyPath": "key.pem",
                                    "certificateChainPath": "certificates.pem"}}
            ]}
        }))
        .unwrap();
        let got = collect_pc(&v, &pod(), true).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got["key.pem"].data, b"key\n");
        assert_eq!(got["certificates.pem"].data, b"cert\n");
        assert_eq!(got["key.pem"].mode, 0o644);
    }

    /// `projected.go:372-375`: fsGroup set forces mode 0600, same as the
    /// other credential-bearing sources.
    #[tokio::test]
    async fn collect_data_pod_certificate_fs_group_forces_0600() {
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"podCertificate": {"signerName": "example.com/foo", "keyType": "ED25519",
                                    "keyPath": "key.pem"}}
            ]}
        }))
        .unwrap();
        let mut p = pod();
        p.spec.as_mut().unwrap().security_context =
            Some(serde_json::from_value(serde_json::json!({"fsGroup": 1000})).unwrap());
        let got = collect_pc(&v, &p, true).await.unwrap();
        assert_eq!(got["key.pem"].mode, 0o600);
    }

    /// The source index passed to the manager is the position in `sources`
    /// (`for sourceIndex, source := range s.source.Sources`), so it counts the
    /// non-certificate sources before it.
    #[tokio::test]
    async fn collect_data_pod_certificate_passes_source_index() {
        use std::sync::Mutex;
        struct Recorder(Mutex<Vec<usize>>);
        #[async_trait]
        impl rusternetes_kubelet::podcertificate::Manager for Recorder {
            fn track_pod(&self, _pod: &Pod) {}
            fn forget_pod(&self, _pod: &Pod) {}
            async fn get_pod_certificate_credential_bundle(
                &self,
                _n: &str,
                _p: &str,
                _u: &str,
                _v: &str,
                source_index: usize,
            ) -> Result<(Vec<u8>, Vec<u8>)> {
                self.0.lock().unwrap().push(source_index);
                Ok((b"k".to_vec(), b"c".to_vec()))
            }
            fn metric_report(&self) -> rusternetes_kubelet::podcertificate::MetricReport {
                Default::default()
            }
        }
        let rec = Arc::new(Recorder(Mutex::new(vec![])));
        let root = tempfile::tempdir().unwrap();
        let host = rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
            root.path().to_string_lossy().to_string(),
            Some(Arc::new(StorageBackend::new_memory())),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        )
        .with_pod_certificate_manager(rec.clone());
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"downwardAPI": {"items": []}},
                {"podCertificate": {"signerName": "a/b", "keyType": "ED25519", "keyPath": "k"}}
            ]}
        }))
        .unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        ProjectedPlugin::new(Arc::new(host))
            .build_mounter(&spec, &pod())
            .collect_data(&args_for(&pod()))
            .await
            .unwrap();
        assert_eq!(*rec.0.lock().unwrap(), vec![1]);
    }

    /// Without a manager the host behaves as upstream's `NoOpManager`
    /// (`podcertificatemanager.go:994`): "unimplemented".
    #[tokio::test]
    async fn collect_data_pod_certificate_without_manager_errors() {
        let v: Volume = serde_json::from_value(serde_json::json!({
            "name": "proj", "projected": {"defaultMode": 420, "sources": [
                {"podCertificate": {"signerName": "a/b", "keyType": "ED25519", "keyPath": "k"}}
            ]}
        }))
        .unwrap();
        let err = collect_pc(&v, &pod(), false).await.unwrap_err();
        assert_eq!(err.to_string(), "unimplemented");
    }
}
