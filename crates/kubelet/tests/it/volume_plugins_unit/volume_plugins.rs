//! Unit tests moved from `src/volume_plugins/mod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_kubelet::volume_plugins::host::{KubeletVolumeHost, VolumeHost};
    pub use rusternetes_kubelet::volume_plugins::plugin::{
        Attributes, DeviceMounter, DeviceMounterArgs, DeviceUnmounter, Mounter, MounterArgs,
        OwnedSpec, ReconstructedVolume, Spec, Unmounter, VolumePlugin,
    };
    pub use rusternetes_kubelet::volume_plugins::registry::{PluginLookupError, VolumePluginMgr};
}

mod create_volume_tests {
    //! Tests driving `VolumeManager::create_volume` through the service-account
    //! token, CA-cert and configMap branches of the secret, projected and
    //! configMap plugins (#1979).
    //!
    //! Every expected value is a literal, never recomputed through the
    //! implementation's helpers. These were written AFTER the plugin move, so
    //! unlike the secret/downwardAPI/projected characterization tests in
    //! `volumes.rs` they pin current behaviour rather than proving the move
    //! equivalent; where upstream defines the behaviour the assertion follows
    //! upstream (`pkg/volume/configmap/configmap_test.go` `TestCollectData`,
    //! `pkg/volume/projected/projected_test.go`
    //! `TestCollectDataWithServiceAccountToken`, and `requiresRefresh` in
    //! `pkg/kubelet/token/token_manager.go`).

    use rusternetes_common::auth::{ServiceAccountClaims, TokenManager};
    use rusternetes_common::resources::{ConfigMap, Pod, Secret, Volume};
    use rusternetes_kubelet::volumes::VolumeManager;
    use rusternetes_storage::{build_key, Storage, StorageBackend};
    use serde_json::json;
    use std::collections::HashMap;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    const CA: &[u8] = b"-----BEGIN CERTIFICATE-----\nFAKE-CA\n-----END CERTIFICATE-----\n";

    struct Env {
        storage: Arc<StorageBackend>,
        tmp: tempfile::TempDir,
        vm: VolumeManager,
    }

    fn env() -> Env {
        let storage = Arc::new(StorageBackend::new_memory());
        let tmp = tempfile::tempdir().unwrap();
        let vm = VolumeManager::new_with_recording_empty_dir_mounts(
            tmp.path().to_string_lossy().to_string(),
            Some(storage.clone()),
            TokenManager::new_auto(b"test-secret"),
        );
        Env { storage, tmp, vm }
    }

    fn pod() -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": [], "serviceAccountName": "sa1", "nodeName": "node-1"}
        }))
        .unwrap()
    }

    fn volume(v: serde_json::Value) -> Volume {
        serde_json::from_value(v).unwrap()
    }

    fn mode(path: &str) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    async fn seed_sa_and_node(e: &Env) {
        Storage::create(
            e.storage.as_ref(),
            &build_key("serviceaccounts", Some("default"), "sa1"),
            &json!({"metadata": {"name": "sa1", "namespace": "default", "uid": "sa-uid-1"}}),
        )
        .await
        .unwrap();
        // The self-mint fallback resolves the bound pod's node through storage
        // (a TokenRequest's BoundObjectRef carries no node).
        Storage::create(
            e.storage.as_ref(),
            &build_key("pods", Some("default"), "p"),
            &pod(),
        )
        .await
        .unwrap();
        Storage::create(
            e.storage.as_ref(),
            &build_key("nodes", None::<&str>, "node-1"),
            &json!({"metadata": {"name": "node-1", "uid": "node-uid-1"}}),
        )
        .await
        .unwrap();
    }

    async fn seed_secret(e: &Env, name: &str, pairs: &[(&str, &[u8])]) {
        let data: HashMap<String, Vec<u8>> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_vec()))
            .collect();
        Storage::create(
            e.storage.as_ref(),
            &build_key("secrets", Some("default"), name),
            &Secret::new(name, "default").with_data(data),
        )
        .await
        .unwrap();
    }

    fn decode(token: &str) -> ServiceAccountClaims {
        TokenManager::new_auto(b"test-secret")
            .validate_token(token)
            .unwrap_or_else(|err| panic!("token must validate: {err}; token={token}"))
    }

    fn read_claims(file: &str) -> ServiceAccountClaims {
        decode(&std::fs::read_to_string(file).unwrap())
    }

    /// Restores `CA_CERT_PATH` on drop; tests that touch it are `serial`.
    struct CaEnvGuard(Option<std::ffi::OsString>);
    impl CaEnvGuard {
        fn set(value: Option<&str>) -> Self {
            let old = std::env::var_os("CA_CERT_PATH");
            match value {
                Some(v) => std::env::set_var("CA_CERT_PATH", v),
                None => std::env::remove_var("CA_CERT_PATH"),
            }
            Self(old)
        }
    }
    impl Drop for CaEnvGuard {
        fn drop(&mut self) {
            match &self.0 {
                Some(v) => std::env::set_var("CA_CERT_PATH", v),
                None => std::env::remove_var("CA_CERT_PATH"),
            }
        }
    }

    // ---------------------------------------------------------------- configMap

    async fn seed_cm(e: &Env) {
        let mut cm = ConfigMap::new("cfg", "default").with_data(HashMap::from([
            ("app.conf".to_string(), "hello".to_string()),
            ("other".to_string(), "x".to_string()),
        ]));
        cm.binary_data = Some(HashMap::from([("blob".to_string(), vec![0u8, 1, 2, 255])]));
        Storage::create(
            e.storage.as_ref(),
            &build_key("configmaps", Some("default"), "cfg"),
            &cm,
        )
        .await
        .unwrap();
    }

    /// `TestCollectData` "no items": every `data` and `binaryData` key becomes a
    /// file at `defaultMode`.
    #[tokio::test]
    async fn config_map_without_items_projects_data_and_binary_data() {
        let e = env();
        seed_cm(&e).await;
        let v = volume(json!({"name": "c", "configMap": {"name": "cfg", "defaultMode": 0o400}}));
        let path = e.vm.create_volume(&pod(), &v).await.unwrap();

        assert_eq!(
            path,
            format!(
                "{}/pods/uid-1/volumes/kubernetes.io~configmap/c",
                e.tmp.path().to_string_lossy()
            )
        );
        assert_eq!(std::fs::read(format!("{path}/app.conf")).unwrap(), b"hello");
        assert_eq!(std::fs::read(format!("{path}/other")).unwrap(), b"x");
        assert_eq!(
            std::fs::read(format!("{path}/blob")).unwrap(),
            vec![0u8, 1, 2, 255]
        );
        assert_eq!(mode(&format!("{path}/app.conf")), 0o400);
        assert_eq!(mode(&format!("{path}/blob")), 0o400);
    }

    /// `TestCollectData` "items": only listed keys, at their `path`, per-item
    /// `mode` overriding `defaultMode`.
    #[tokio::test]
    async fn config_map_items_select_keys_paths_and_modes() {
        let e = env();
        seed_cm(&e).await;
        let v = volume(json!({"name": "c", "configMap": {
            "name": "cfg", "defaultMode": 0o644,
            "items": [
                {"key": "app.conf", "path": "sub/dir/a.conf", "mode": 0o600},
                {"key": "blob", "path": "b.bin"}
            ]
        }}));
        let path = e.vm.create_volume(&pod(), &v).await.unwrap();

        assert_eq!(
            std::fs::read(format!("{path}/sub/dir/a.conf")).unwrap(),
            b"hello"
        );
        assert_eq!(mode(&format!("{path}/sub/dir/a.conf")), 0o600);
        assert_eq!(
            std::fs::read(format!("{path}/b.bin")).unwrap(),
            vec![0u8, 1, 2, 255]
        );
        assert_eq!(mode(&format!("{path}/b.bin")), 0o644);
        assert!(
            !std::path::Path::new(&format!("{path}/other")).exists(),
            "a key not named in items must not be projected"
        );
    }

    /// `TestCollectData` "key not found": a mapped key the ConfigMap lacks is an
    /// error unless the volume is optional.
    #[tokio::test]
    async fn config_map_missing_mapped_key_errors_unless_optional() {
        let e = env();
        seed_cm(&e).await;
        let items = json!([{"key": "nope", "path": "n"}, {"key": "other", "path": "o"}]);

        let required = volume(json!({"name": "c", "configMap": {"name": "cfg", "items": items}}));
        let err = e.vm.create_volume(&pod(), &required).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("references non-existent config key: nope"),
            "got: {err:#}"
        );

        let optional = volume(json!({"name": "c2", "configMap": {
            "name": "cfg", "optional": true, "items": items}}));
        let path = e.vm.create_volume(&pod(), &optional).await.unwrap();
        assert_eq!(std::fs::read(format!("{path}/o")).unwrap(), b"x");
        assert!(!std::path::Path::new(&format!("{path}/n")).exists());
    }

    /// A required ConfigMap that does not exist fails set-up (the pod stays
    /// ContainerCreating and the kubelet retries).
    #[tokio::test]
    async fn config_map_missing_required_is_an_error() {
        let e = env();
        let v = volume(json!({"name": "c", "configMap": {"name": "ghost"}}));
        let err = e.vm.create_volume(&pod(), &v).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("ConfigMap ghost not found in namespace default"),
            "got: {err:#}"
        );
    }

    /// `TestPluginOptional`: a missing optional ConfigMap yields an empty volume.
    #[tokio::test]
    async fn config_map_missing_optional_yields_an_empty_volume() {
        let e = env();
        let v = volume(json!({"name": "c", "configMap": {"name": "ghost", "optional": true}}));
        let path = e.vm.create_volume(&pod(), &v).await.unwrap();
        assert!(std::path::Path::new(&path).is_dir());
        // Upstream runs the AtomicWriter over an empty payload, so the only
        // entries are `..data` and the timestamped dir it points at, and that dir
        // is empty (`configmap_test.go:506-535`).
        let target = std::fs::read_link(std::path::Path::new(&path).join("..data")).unwrap();
        assert_eq!(
            std::fs::read_dir(std::path::Path::new(&path).join(target))
                .unwrap()
                .count(),
            0
        );
        for entry in std::fs::read_dir(&path).unwrap() {
            let name = entry.unwrap().file_name().to_string_lossy().into_owned();
            assert!(name.starts_with(".."), "unexpected entry {name}");
        }
    }

    // ------------------------------------- secret: no SA token / CA special case

    fn sa_token_secret_volume() -> Volume {
        volume(json!({"name": "kube-api-access-abc", "secret": {"secretName": "sa1-token"}}))
    }

    /// Upstream's secret plugin projects the stored Secret verbatim: no volume or
    /// secret NAME makes it mint a token (`pkg/volume/secret/secret.go:162-208`,
    /// `MakePayload` :210-247 -- no token or CA logic anywhere). A legacy
    /// `kube-api-access*` / `*-token` Secret volume keeps its stored `token`
    /// (#2638; SA tokens arrive via the projected `serviceAccountToken` source).
    #[tokio::test]
    #[serial_test::serial]
    async fn secret_sa_named_volume_projects_stored_token_verbatim() {
        let _g = CaEnvGuard::set(Some("/nonexistent/ca.crt"));
        let e = env();
        seed_sa_and_node(&e).await;
        seed_secret(&e, "sa1-token", &[("token", b"static-token"), ("x", b"y")]).await;

        let path =
            e.vm.create_volume(&pod(), &sa_token_secret_volume())
                .await
                .unwrap();

        assert_eq!(
            std::fs::read(format!("{path}/token")).unwrap(),
            b"static-token"
        );
        assert_eq!(std::fs::read(format!("{path}/x")).unwrap(), b"y");
    }

    /// `items` mapping of the `token` key keeps the stored bytes too.
    #[tokio::test]
    #[serial_test::serial]
    async fn secret_sa_named_volume_items_keep_stored_token() {
        let _g = CaEnvGuard::set(Some("/nonexistent/ca.crt"));
        let e = env();
        seed_sa_and_node(&e).await;
        seed_secret(&e, "sa1-token", &[("token", b"static-token"), ("x", b"y")]).await;
        let v = volume(json!({"name": "kube-api-access-abc", "secret": {
            "secretName": "sa1-token",
            "items": [{"key": "token", "path": "t/jwt"}, {"key": "x", "path": "t/token"}]
        }}));
        let path = e.vm.create_volume(&pod(), &v).await.unwrap();

        assert_eq!(
            std::fs::read(format!("{path}/t/jwt")).unwrap(),
            b"static-token"
        );
        assert_eq!(std::fs::read(format!("{path}/t/token")).unwrap(), b"y");
    }

    /// The kubelet never injects the cluster CA into a Secret volume: it reaches
    /// pods through the `kube-root-ca.crt` ConfigMap projection (#2638), so a
    /// `CA_CERT_PATH` / `_certs/ca.crt` must have no effect on a Secret volume.
    #[tokio::test]
    #[serial_test::serial]
    async fn secret_volume_never_gets_ca_crt_injected() {
        let e = env();
        let ca_file = e.tmp.path().join("env-ca.crt");
        std::fs::write(&ca_file, CA).unwrap();
        std::fs::create_dir_all(e.tmp.path().join("_certs")).unwrap();
        std::fs::write(e.tmp.path().join("_certs/ca.crt"), CA).unwrap();
        let _g = CaEnvGuard::set(Some(ca_file.to_str().unwrap()));
        seed_secret(&e, "sa1-token", &[("token", b"t")]).await;
        let path =
            e.vm.create_volume(&pod(), &sa_token_secret_volume())
                .await
                .unwrap();
        assert!(!std::path::Path::new(&format!("{path}/ca.crt")).exists());
        assert!(std::path::Path::new(&format!("{path}/token")).exists());
    }

    /// A `ca.crt` stored in the Secret is projected like any other key.
    #[tokio::test]
    #[serial_test::serial]
    async fn secret_existing_ca_crt_is_projected_as_stored() {
        let _g = CaEnvGuard::set(Some("/nonexistent/ca.crt"));
        let e = env();
        seed_secret(&e, "sa1-token", &[("token", b"t"), ("ca.crt", b"OWN")]).await;
        let path =
            e.vm.create_volume(&pod(), &sa_token_secret_volume())
                .await
                .unwrap();
        assert_eq!(std::fs::read(format!("{path}/ca.crt")).unwrap(), b"OWN");
    }

    // ---------------------------------------------- projected serviceAccountToken

    fn projected_token_volume(extra: serde_json::Value) -> Volume {
        let mut t = json!({"path": "sub/token"});
        t.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        volume(json!({"name": "p", "projected": {
            "defaultMode": 0o440,
            "sources": [{"serviceAccountToken": t}]
        }}))
    }

    /// `TestCollectDataWithServiceAccountToken`: the token lands at `path` with
    /// `defaultMode`; claims carry the pod/SA/node identity. Default lifetime is
    /// 3600s, default audience (self-mint fallback) is "rusternetes".
    #[tokio::test]
    async fn projected_sa_token_is_minted_with_bound_claims() {
        let e = env();
        seed_sa_and_node(&e).await;
        let path =
            e.vm.create_volume(&pod(), &projected_token_volume(json!({})))
                .await
                .unwrap();

        let file = format!("{path}/sub/token");
        assert_eq!(mode(&file), 0o440);
        let c = read_claims(&file);
        assert_eq!(c.sub, "system:serviceaccount:default:sa1");
        assert_eq!(c.uid, "sa-uid-1");
        assert_eq!(c.aud, vec!["rusternetes".to_string()]);
        assert_eq!(c.exp - c.iat, 3600);
        assert_eq!(c.pod_name.as_deref(), Some("p"));
        assert_eq!(c.pod_uid.as_deref(), Some("uid-1"));
        assert_eq!(c.node_name.as_deref(), Some("node-1"));
        assert_eq!(c.node_uid.as_deref(), Some("node-uid-1"));
        assert_eq!(c.kubernetes.unwrap().svcacct.uid, "sa-uid-1");
    }

    /// `audience` and `expirationSeconds` are honoured; the lifetime is floored
    /// at 600s (the TokenRequest minimum).
    #[tokio::test]
    async fn projected_sa_token_honours_audience_and_floors_expiration() {
        let e = env();
        seed_sa_and_node(&e).await;
        let path =
            e.vm.create_volume(
                &pod(),
                &projected_token_volume(json!({"audience": "vault", "expirationSeconds": 7200})),
            )
            .await
            .unwrap();
        let c = read_claims(&format!("{path}/sub/token"));
        assert_eq!(c.aud, vec!["vault".to_string()]);
        assert_eq!(c.exp - c.iat, 7200);

        let e2 = env();
        seed_sa_and_node(&e2).await;
        let path = e2
            .vm
            .create_volume(
                &pod(),
                &projected_token_volume(json!({"expirationSeconds": 10})),
            )
            .await
            .unwrap();
        let c = read_claims(&format!("{path}/sub/token"));
        assert_eq!(c.exp - c.iat, 600);
    }

    /// A ServiceAccount or Node absent from storage does not fail set-up: a
    /// token is still written (not the placeholder). Its payload is inspected
    /// raw because `KubeRef.uid` is skipped on serialize when empty yet required
    /// on deserialize, so the token does not round-trip (filed separately).
    #[tokio::test]
    async fn projected_sa_token_without_sa_or_node_in_storage_still_writes_a_token() {
        let e = env();
        let path =
            e.vm.create_volume(&pod(), &projected_token_volume(json!({})))
                .await
                .unwrap();
        let token = std::fs::read_to_string(format!("{path}/sub/token")).unwrap();
        assert_eq!(token.split('.').count(), 3, "a JWT, got: {token}");
        assert!(!token.ends_with(".placeholder"));
    }

    /// `Manager.GetServiceAccountToken` (token_manager.go:98-130): a token
    /// younger than its refresh window is served from the in-memory cache, so a
    /// per-sync re-SetUp projects the same bytes (the AtomicWriter stays inert).
    /// The cache and its refresh rules are covered by `token_manager.rs`.
    #[tokio::test]
    async fn projected_sa_token_is_served_from_the_manager_cache() {
        let e = env();
        seed_sa_and_node(&e).await;
        let v = projected_token_volume(json!({"expirationSeconds": 600}));
        let path = e.vm.create_volume(&pod(), &v).await.unwrap();
        let file = format!("{path}/sub/token");
        let first = std::fs::read_to_string(&file).unwrap();
        assert_eq!(read_claims(&file).exp - read_claims(&file).iat, 600);
        // `iat` has one-second resolution: a re-mint inside the same second
        // would be identical, so wait it out.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        e.vm.create_volume(&pod(), &v).await.unwrap();
        assert_eq!(std::fs::read_to_string(&file).unwrap(), first);
    }
}

mod mounter_args_tests {
    //! The pod's `fsGroup` reaches a mounter only through `MounterArgs` (#2900).
    //!
    //! Upstream: `operation_generator.go:582-589` is the single place the pod's
    //! `FsGroup` is turned into `volume.MounterArgs{FsGroup: ..}`; every
    //! plugin's `SetUpAt` then reads `mounterArgs.FsGroup`
    //! (`empty_dir.go` `setupDir` -> `volume.SetVolumeOwnership(.., mounterArgs.FsGroup, ..)`,
    //! `configmap.go:246-252`, `secret.go:232-238`) and never the pod.
    //! `NewMounter` captures no fsGroup.
    //!
    //! Each test builds the mounter from a pod WITH an fsGroup and sets up with
    //! EMPTY args: nothing may be chowned/chmodded (setgid stays off). The mirror
    //! test builds it from a pod WITHOUT fsGroup and passes the group in args.

    use rusternetes_common::resources::{Pod, Volume};
    use rusternetes_kubelet::volume_plugins::plugin::{Mounter, MounterArgs, Spec, VolumePlugin};
    use rusternetes_kubelet::volume_plugins::{
        config_map::ConfigMapPlugin, downward_api::DownwardApiPlugin, empty_dir::EmptyDirPlugin,
        projected::ProjectedPlugin, secret::SecretPlugin, KubeletVolumeHost,
    };
    use rusternetes_storage::{MemoryStorage, StorageBackend};
    use serde_json::json;
    use std::collections::HashMap;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::sync::Arc;

    fn host() -> Arc<KubeletVolumeHost> {
        Arc::new(KubeletVolumeHost::new_for_tests(
            "/var/lib/rusternetes".to_string(),
            Some(Arc::new(StorageBackend::Memory(Arc::new(
                MemoryStorage::new(),
            )))),
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        ))
    }

    /// `VolumeHost.GetNodeName` (`pkg/volume/plugins.go:413`,
    /// `pkg/kubelet/volume_host.go:294`) returns the kubelet's node name.
    #[test]
    fn get_node_name_returns_the_configured_node() {
        use rusternetes_kubelet::volume_plugins::VolumeHost;
        assert_eq!(host().get_node_name(), "");
        let h = KubeletVolumeHost::new_for_tests(
            "/var/lib/rusternetes".to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        )
        .with_node_name("node-7");
        assert_eq!(h.get_node_name(), "node-7");
    }

    fn pod(fs_group: Option<i64>) -> Pod {
        let mut spec = json!({"containers": []});
        if let Some(g) = fs_group {
            spec["securityContext"] = json!({"fsGroup": g});
        }
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": spec
        }))
        .unwrap()
    }

    fn volumes() -> Vec<(&'static str, Volume, Box<dyn VolumePlugin>)> {
        let v = |j| -> Volume { serde_json::from_value(j).unwrap() };
        vec![
            (
                "emptyDir",
                v(json!({"name": "v", "emptyDir": {}})),
                Box::new(EmptyDirPlugin::new(host())),
            ),
            (
                "configMap",
                v(json!({"name": "v", "configMap": {"name": "c", "optional": true}})),
                Box::new(ConfigMapPlugin::new(host())),
            ),
            (
                "secret",
                v(json!({"name": "v", "secret": {"secretName": "s", "optional": true}})),
                Box::new(SecretPlugin::new(host())),
            ),
            (
                "downwardAPI",
                v(
                    json!({"name": "v", "downwardAPI": {"defaultMode": 420, "items": [
                    {"path": "name", "fieldRef": {"fieldPath": "metadata.name"}}]}}),
                ),
                Box::new(DownwardApiPlugin::new(host())),
            ),
            (
                "projected",
                v(
                    json!({"name": "v", "projected": {"sources": [{"downwardAPI": {"items": [
                    {"path": "name", "fieldRef": {"fieldPath": "metadata.name"}}]}}]}}),
                ),
                Box::new(ProjectedPlugin::new(host())),
            ),
        ]
    }

    async fn mounter(plugin: &dyn VolumePlugin, v: &Volume, pod: &Pod) -> Box<dyn Mounter> {
        let spec = Spec {
            volume: v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        plugin.new_mounter(&spec, pod).await.unwrap()
    }

    fn setgid(dir: &std::path::Path) -> bool {
        std::fs::metadata(dir).unwrap().permissions().mode() & 0o2000 != 0
    }

    #[tokio::test]
    async fn pod_fs_group_does_not_leak_past_mounter_args() {
        let mut leaked = Vec::new();
        for (kind, v, plugin) in volumes() {
            let tmp = tempfile::tempdir().unwrap();
            let gid = std::fs::metadata(tmp.path()).unwrap().gid() as i64;
            let dir = tmp.path().join("vol");
            let m = mounter(plugin.as_ref(), &v, &pod(Some(gid))).await;
            m.set_up_at(dir.to_str().unwrap(), &MounterArgs::default())
                .await
                .unwrap();
            if setgid(&dir) {
                leaked.push(kind);
            }
        }
        assert!(
            leaked.is_empty(),
            "pod fsGroup applied without MounterArgs: {leaked:?}"
        );
    }

    #[tokio::test]
    async fn mounter_args_fs_group_is_applied_without_a_pod_fs_group() {
        for (kind, v, plugin) in volumes() {
            let tmp = tempfile::tempdir().unwrap();
            let gid = std::fs::metadata(tmp.path()).unwrap().gid() as i64;
            let dir = tmp.path().join("vol");
            let m = mounter(plugin.as_ref(), &v, &pod(None)).await;
            let args = MounterArgs {
                fs_group: Some(gid),
                ..Default::default()
            };
            m.set_up_at(dir.to_str().unwrap(), &args).await.unwrap();
            assert!(setgid(&dir), "{kind}: MounterArgs.fs_group not applied");
        }
    }
}

mod set_up_at_tests {
    //! `Mounter::SetUpAt(dir, MounterArgs)` (#2781, split from #2687).
    //!
    //! Upstream: `pkg/volume/volume.go:175` (`SetUp(MounterArgs)`), `:184`
    //! (`SetUpAt(dir, MounterArgs)`); every plugin's `SetUp` is
    //! `SetUpAt(GetPath(), args)` (e.g. `pkg/volume/emptydir/empty_dir.go`
    //! `SetUp`); hostPath's `SetUpAt` is `"SetUpAt() does not make sense for host
    //! paths"` (`pkg/volume/hostpath/host_path.go:257-260`).

    use rusternetes_common::resources::{Pod, Volume};
    use rusternetes_kubelet::volume_plugins::empty_dir::EmptyDirPlugin;
    use rusternetes_kubelet::volume_plugins::host_path::HostPathPlugin;
    use rusternetes_kubelet::volume_plugins::plugin::{Mounter, MounterArgs, Spec, VolumePlugin};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn host() -> Arc<rusternetes_kubelet::volume_plugins::KubeletVolumeHost> {
        Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                HashMap::new(),
            ),
        )
    }

    fn pod() -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-1"},
            "spec": {"containers": []}
        }))
        .unwrap()
    }

    async fn mounter_for(plugin: &dyn VolumePlugin, v: &Volume) -> Box<dyn Mounter> {
        let spec = Spec {
            volume: v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        plugin.new_mounter(&spec, &pod()).await.unwrap()
    }

    /// `SetUpAt` sets up the volume at the GIVEN directory, not at `GetPath()`.
    #[tokio::test]
    async fn set_up_at_sets_up_the_given_dir_not_get_path() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("elsewhere");
        let v: Volume = serde_json::from_value(json!({"name": "ed", "emptyDir": {}})).unwrap();
        let m = mounter_for(&EmptyDirPlugin::new(host()), &v).await;
        m.set_up_at(dir.to_str().unwrap(), &MounterArgs::default())
            .await
            .unwrap();
        assert!(dir.is_dir(), "SetUpAt(dir) must create dir");
        assert!(
            !std::path::Path::new(&m.get_path()).exists(),
            "SetUpAt(dir) must not touch GetPath()"
        );
    }

    /// `MounterArgs.FsGroup` reaches the mounter per call (`volume.go:132`), not
    /// from a field captured at construction: the pod here has no fsGroup.
    #[tokio::test]
    async fn set_up_at_applies_the_fs_group_from_mounter_args() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("vol");
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("f");
        std::fs::write(&f, b"x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o400)).unwrap();
        let gid = std::fs::metadata(&dir).unwrap().gid() as i64;
        let v: Volume = serde_json::from_value(json!({"name": "ed", "emptyDir": {}})).unwrap();
        let m = mounter_for(&EmptyDirPlugin::new(host()), &v).await;
        let args = MounterArgs {
            fs_group: Some(gid),
            ..Default::default()
        };
        m.set_up_at(dir.to_str().unwrap(), &args).await.unwrap();
        assert_eq!(
            std::fs::metadata(&f).unwrap().permissions().mode() & 0o7777,
            0o660
        );
    }

    /// `host_path.go:257-260`.
    #[tokio::test]
    async fn host_path_set_up_at_does_not_make_sense() {
        let v: Volume =
            serde_json::from_value(json!({"name": "hp", "hostPath": {"path": "/tmp"}})).unwrap();
        let m = mounter_for(&HostPathPlugin::new(host()), &v).await;
        let err = m
            .set_up_at("/tmp/x", &MounterArgs::default())
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "SetUpAt() does not make sense for host paths"
        );
    }

    /// `SetUp(args)` is `SetUpAt(GetPath(), args)` (`volume.go:175`).
    #[tokio::test]
    async fn set_up_with_args_delegates_to_set_up_at_get_path() {
        use std::sync::Mutex;
        struct Rec(Mutex<Vec<(String, Option<i64>)>>);
        impl rusternetes_kubelet::volume_plugins::plugin::MetricsProvider for Rec {
            fn get_metrics(
                &self,
            ) -> anyhow::Result<rusternetes_kubelet::volume_plugins::plugin::Metrics> {
                Ok(Default::default())
            }
        }

        #[async_trait::async_trait]
        impl Mounter for Rec {
            fn get_path(&self) -> String {
                "/the/path".into()
            }
            async fn set_up_at(&self, dir: &str, args: &MounterArgs) -> anyhow::Result<()> {
                self.0.lock().unwrap().push((dir.into(), args.fs_group));
                Ok(())
            }
            fn get_attributes(&self) -> rusternetes_kubelet::volume_plugins::plugin::Attributes {
                Default::default()
            }
        }
        let r = Rec(Mutex::new(vec![]));
        let args = MounterArgs {
            fs_group: Some(7),
            ..Default::default()
        };
        r.set_up_with(&args).await.unwrap();
        r.set_up().await.unwrap();
        assert_eq!(
            *r.0.lock().unwrap(),
            vec![("/the/path".into(), Some(7)), ("/the/path".into(), None)]
        );
    }
}
