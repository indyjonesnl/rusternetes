//! Unit tests moved from `src/volume_plugins/csi.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Context, Result};
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::csi::FSGroupPolicy;
    pub use rusternetes_common::resources::{
        CSIDriver, PersistentVolume, PersistentVolumeAccessMode, Pod, Secret, VolumeAttachment,
    };
    pub use rusternetes_kubelet::volume_plugins::csi_client::{CsiDriverClient, CsiError};
    pub use rusternetes_kubelet::volume_plugins::plugin::{
        DeviceMountableVolumePlugin, DeviceMounter, DeviceMounterArgs, DeviceUnmounter,
    };
    pub use rusternetes_kubelet::volume_plugins::{
        Attributes, Mounter, ReconstructedVolume, Spec, Unmounter, VolumeHost, VolumePlugin,
    };
    pub use rusternetes_storage::{build_key, Storage, StorageBackend};
    pub use sha2::{Digest, Sha256};
    pub use std::collections::HashMap;
    pub use std::path::{Path, PathBuf};
    pub use std::sync::Arc;
    pub use tracing::{debug, error, info};
}

mod tests {
    //! Tests for the CSI volume plugin, ported in shape from
    //! `pkg/volume/csi/csi_mounter_test.go` (`TestMounterGetPath`,
    //! `TestMounterSetUp`, `TestMounterSetupJsonFileHandling`,
    //! `TestMounterSetUpSimple`, `TestMounterSetUpWithInline`,
    //! `TestUnmounterTeardown*`) and `csi_attacher_test.go` (`TestAttacherMountDevice`).
    //!
    //! Upstream drives a fake in-process `csiClient`; here the driver is a real
    //! gRPC `csi.v1.Node` server on a unix socket (`csi_client::fake`) registered in
    //! the driver store, so the whole request path is exercised.

    use super::__parent::*;
    use crate::csi_fake::{serve, FakeDriver};
    use rusternetes_common::resources::{Pod, Secret, Volume};
    use rusternetes_kubelet::volume_plugins::csi::*;
    use rusternetes_kubelet::volume_plugins::csi_client::proto::node_service_capability::rpc::Type as Cap;
    use rusternetes_kubelet::volume_plugins::csi_client::proto::volume_capability::AccessType;
    use rusternetes_kubelet::volume_plugins::csi_client::CsiError;
    use rusternetes_kubelet::volume_plugins::csi_drivers_store::{csi_drivers, Driver};
    use rusternetes_kubelet::volume_plugins::plugin::DeviceMounterArgs;
    use rusternetes_storage::{build_key, MemoryStorage, Storage, StorageBackend};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn host(
        base: &str,
        storage: Option<Arc<StorageBackend>>,
    ) -> Arc<rusternetes_kubelet::volume_plugins::KubeletVolumeHost> {
        Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                base.to_string(),
                storage,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                HashMap::new(),
            ),
        )
    }

    fn plugin() -> CsiPlugin {
        CsiPlugin::new(host("/var/lib/rusternetes", None))
    }

    fn pod() -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "ns1", "uid": "uid-1"},
            "spec": {"containers": [], "nodeName": "node-1", "serviceAccountName": "sa-1"}
        }))
        .unwrap()
    }

    fn claim_volume() -> Volume {
        serde_json::from_value(json!({"name": "data", "persistentVolumeClaim": {"claimName": "c"}}))
            .unwrap()
    }

    fn pv(
        driver: &str,
        extra_csi: serde_json::Value,
    ) -> rusternetes_common::resources::PersistentVolume {
        let mut csi = json!({"driver": driver, "volumeHandle": "vol-1"});
        for (k, v) in extra_csi.as_object().unwrap() {
            csi[k] = v.clone();
        }
        serde_json::from_value(json!({
            "metadata": {"name": "pv1"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteMany"],
                "mountOptions": ["foo=bar"],
                "csi": csi
            }
        }))
        .unwrap()
    }

    /// Everything a mount test needs: a tempdir kubelet root, an in-memory API, a
    /// fake driver registered under a test-unique name.
    struct Fx {
        _dir: tempfile::TempDir,
        root: String,
        driver: String,
        fake: FakeDriver,
        storage: Arc<StorageBackend>,
        plugin: CsiPlugin,
        _srv: tokio::task::JoinHandle<()>,
    }

    async fn fx(name: &str, caps: &[Cap], csi_driver: Option<serde_json::Value>) -> Fx {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("kubelet").to_string_lossy().to_string();
        let sock = dir.path().join("csi.sock");
        let driver = format!("{name}.csi.example.com");
        let fake = FakeDriver::with_capabilities(caps);
        let srv = serve(fake.clone(), &sock);
        csi_drivers().set(
            &driver,
            Driver {
                endpoint: sock.to_string_lossy().to_string(),
                highest_supported_version: "1.0.0".into(),
            },
        );
        let storage = Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new())));
        if let Some(spec) = csi_driver {
            let obj = json!({"apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
                "metadata": {"name": driver}, "spec": spec});
            storage
                .create(&build_key("csidrivers", None, &driver), &obj)
                .await
                .unwrap();
        }
        let plugin = CsiPlugin::new(host(&root, Some(storage.clone())));
        Fx {
            _dir: dir,
            root,
            driver,
            fake,
            storage,
            plugin,
            _srv: srv,
        }
    }

    async fn put_secret(storage: &Arc<StorageBackend>, ns: &str, name: &str, k: &str, v: &str) {
        let s = Secret::new(name, ns)
            .with_data(HashMap::from([(k.to_string(), v.as_bytes().to_vec())]));
        storage
            .create(&build_key("secrets", Some(ns), name), &s)
            .await
            .unwrap();
    }

    /// Run the `MountVolume` operation (`GenerateMountVolumeFunc`): `MountDevice`
    /// then `SetUp`, through a registry holding the CSI plugin.
    async fn mount_volume(f: &Fx, spec: &Spec<'_>, pod: &Pod) -> String {
        let mgr = rusternetes_kubelet::volume_plugins::VolumePluginMgr::new(vec![Box::new(
            CsiPlugin::new(host(&f.root, Some(f.storage.clone()))),
        )]);
        rusternetes_kubelet::volume_plugins::util::operation_generator::mount_volume(
            &mgr, spec, pod, "",
        )
        .await
        .unwrap()
    }

    // ---- DeviceMounter (#2529) ------------------------------------------------

    /// `NewDeviceMounter` -> `GetDeviceMountPath` -> `MountDevice` stages the
    /// volume WITHOUT publishing it (`csi_attacher.go:264-411`): publishing is the
    /// mounter's `SetUp`.
    #[tokio::test]
    async fn device_mounter_stages_without_publishing() {
        let f = fx(
            "devmount",
            &[Cap::StageUnstageVolume],
            Some(json!({"attachRequired": false})),
        )
        .await;
        put_secret(&f.storage, "sec-ns", "stage-secret", "k", "v").await;
        let p = pv(
            &f.driver,
            json!({"fsType": "xfs",
                   "nodeStageSecretRef": {"name": "stage-secret", "namespace": "sec-ns"}}),
        );
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let dm = f.plugin.new_device_mounter().unwrap();
        let path = dm.get_device_mount_path(&spec).unwrap();
        assert!(path.ends_with("/globalmount"), "{path}");
        dm.mount_device(&spec, "", &path, &DeviceMounterArgs::default())
            .await
            .unwrap();

        let calls = f.fake.calls.lock().unwrap();
        assert_eq!(calls.stage.len(), 1);
        assert_eq!(calls.stage[0].staging_target_path, path);
        assert_eq!(calls.stage[0].secrets["k"], "v");
        assert!(calls.publish.is_empty(), "MountDevice must not publish");
        // `UnmountDevice` finds the driver and handle here.
        assert!(std::path::Path::new(&path)
            .parent()
            .unwrap()
            .join("vol_data.json")
            .exists());
    }

    /// `csiMountMgr.GetAttributes` (`csi_mounter.go:418-424`):
    /// `{ReadOnly: c.readOnly, Managed: !c.readOnly, SELinuxRelabel: c.needSELinuxRelabel}`.
    /// The readOnly of a PV-backed mounter is `Spec.ReadOnly`
    /// (`getReadOnlyFromSpec`, `csi_plugin.go:499`), i.e. the claim's `readOnly`.
    #[tokio::test]
    async fn mounter_attributes_follow_spec_read_only() {
        use rusternetes_kubelet::volume_plugins::plugin::Attributes;
        let f = fx("attrs", &[], Some(json!({"attachRequired": false}))).await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        for read_only in [true, false] {
            let spec = Spec {
                volume: &v,
                persistent_volume: Some(&p),
                read_only,
                migrated: false,
                inline_volume_spec_for_csi_migration: false,
            };
            let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
            assert_eq!(
                m.get_attributes(),
                Attributes {
                    read_only,
                    managed: !read_only,
                    selinux_relabel: false
                }
            );
        }
    }

    /// `SetUp` no longer stages: the device mount is its own operation, run first
    /// by `MountVolume` (`operation_generator.go:530-552`).
    #[tokio::test]
    async fn set_up_alone_does_not_stage() {
        let f = fx(
            "nostage",
            &[Cap::StageUnstageVolume],
            Some(json!({"attachRequired": false})),
        )
        .await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();
        let calls = f.fake.calls.lock().unwrap();
        assert!(calls.stage.is_empty());
        assert_eq!(calls.publish.len(), 1);
        assert!(calls.publish[0]
            .staging_target_path
            .ends_with("/globalmount"));
    }

    /// The CSI spec orders NodeStageVolume before NodePublishVolume; the
    /// `MountVolume` operation guarantees it.
    #[tokio::test]
    async fn mount_volume_stages_before_it_publishes() {
        let f = fx(
            "order",
            &[Cap::StageUnstageVolume],
            Some(json!({"attachRequired": false})),
        )
        .await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let path = mount_volume(&f, &spec, &pod()).await;
        assert!(path.ends_with("/mount"), "{path}");
        let calls = f.fake.calls.lock().unwrap();
        assert_eq!((calls.stage.len(), calls.publish.len()), (1, 1));
        assert_eq!(calls.order, vec!["stage", "publish"]);
    }

    /// A failed `MountDevice` stops the operation: nothing is published.
    #[tokio::test]
    async fn mount_volume_does_not_publish_when_staging_fails() {
        let f = fx(
            "stagefail",
            &[Cap::StageUnstageVolume],
            Some(json!({"attachRequired": false})),
        )
        .await;
        *f.fake.stage_error.lock().unwrap() = Some(tonic::Code::InvalidArgument);
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let mgr = rusternetes_kubelet::volume_plugins::VolumePluginMgr::new(vec![Box::new(
            CsiPlugin::new(host(&f.root, Some(f.storage.clone()))),
        )]);
        let err = rusternetes_kubelet::volume_plugins::util::operation_generator::mount_volume(
            &mgr,
            &spec,
            &pod(),
            "",
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("fake stage failure"), "{err}");
        assert!(f.fake.calls.lock().unwrap().publish.is_empty());
    }

    /// `GenerateMountVolumeFunc` with its `actualStateOfWorld`: `MountVolume`
    /// reports the device and the pod mount to it.
    struct World {
        mgr: rusternetes_kubelet::volume_plugins::VolumePluginMgr,
        asw: rusternetes_kubelet::volume_manager::cache::ActualStateOfWorld,
    }

    fn world(f: &Fx) -> World {
        let mgr = rusternetes_kubelet::volume_plugins::VolumePluginMgr::new(vec![Box::new(
            CsiPlugin::new(host(&f.root, Some(f.storage.clone()))),
        )]);
        let asw = rusternetes_kubelet::volume_manager::cache::ActualStateOfWorld::new(
            "node-1",
            Arc::new(rusternetes_kubelet::volume_plugins::VolumePluginMgr::new(
                vec![Box::new(CsiPlugin::new(host(
                    &f.root,
                    Some(f.storage.clone()),
                )))],
            )),
        );
        World { mgr, asw }
    }

    /// `volume_name` is the PV's unique name; the volume is marked attached first
    /// (`MarkVolumeAsAttached`), as the caller of the mount operation does.
    async fn mount_in_world(
        w: &World,
        spec: &rusternetes_kubelet::volume_plugins::OwnedSpec,
        pod: &Pod,
    ) -> anyhow::Result<String> {
        use rusternetes_kubelet::volume_manager::cache::desired_state_of_world::get_unique_volume_name_for_pod;
        let pod_name = rusternetes_kubelet::volume_plugins::util::get_unique_pod_name(pod);
        let plugin = w.mgr.find_plugin_by_spec(&spec.as_spec()).unwrap();
        let (volume_name, attachable, device_mountable) =
            get_unique_volume_name_for_pod(&w.mgr, &pod_name, plugin, &spec.as_spec()).unwrap();
        let vtm = rusternetes_kubelet::volume_plugins::util::operation_executor::VolumeToMount {
            volume_name: volume_name.clone(),
            pod_name,
            volume_spec: Arc::new(spec.clone()),
            outer_volume_spec_names: vec![spec.volume.name.clone()],
            pod: Arc::new(pod.clone()),
            plugin_is_attachable: attachable,
            plugin_is_device_mountable: device_mountable,
            volume_gid_value: String::new(),
            device_path: String::new(),
            reported_in_use: true,
            desired_size_limit: None,
            mount_request_time: std::time::SystemTime::now(),
            desired_persistent_volume_size: None,
            selinux_label: String::new(),
        };
        if !w.asw.volume_exists(&volume_name) {
            w.asw
                .mark_volume_as_attached(Some(&volume_name), vtm.volume_spec.clone(), "node-1", "")
                .unwrap();
        }
        let state = rusternetes_kubelet::volume_plugins::util::operation_generator::MountState {
            volume_to_mount: &vtm,
            actual_state_of_world: &w.asw,
        };
        rusternetes_kubelet::volume_plugins::util::operation_generator::mount_volume_with_attributes(
            &w.mgr,
            &spec.as_spec(),
            pod,
            "",
            None,
            Some(&state),
        )
        .await
        .map(|(path, _)| path)
    }

    fn owned_pv_spec(f: &Fx) -> rusternetes_kubelet::volume_plugins::OwnedSpec {
        rusternetes_kubelet::volume_plugins::OwnedSpec {
            volume: claim_volume(),
            persistent_volume: Some(pv(&f.driver, json!({}))),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        }
    }

    /// `MarkDeviceAsMounted` after `MountDevice`, and no second `MountDevice` for
    /// a device already `DeviceGloballyMounted` (`operation_generator.go:530`,
    /// `:545-566`): a second pod sharing the volume only publishes.
    #[tokio::test]
    async fn mount_volume_marks_the_device_mounted_and_skips_a_second_stage() {
        use rusternetes_kubelet::volume_plugins::util::operation_executor::{
            DeviceMountState, VolumeMountState,
        };
        let f = fx(
            "worldstage",
            &[Cap::StageUnstageVolume],
            Some(json!({"attachRequired": false})),
        )
        .await;
        let w = world(&f);
        let spec = owned_pv_spec(&f);
        mount_in_world(&w, &spec, &pod()).await.unwrap();

        let attached = w.asw.get_attached_volumes();
        assert_eq!(attached.len(), 1);
        assert_eq!(
            attached[0].device_mount_state,
            DeviceMountState::DeviceGloballyMounted
        );
        let name = attached[0].volume_name.clone();
        assert_eq!(
            w.asw.get_volume_mount_state(
                &name,
                &rusternetes_kubelet::volume_plugins::util::types::UniquePodName("uid-1".into())
            ),
            VolumeMountState::VolumeMounted
        );

        let mut other = pod();
        other.metadata.uid = "uid-2".into();
        other.metadata.name = "p2".into();
        mount_in_world(&w, &spec, &other).await.unwrap();
        let calls = f.fake.calls.lock().unwrap();
        assert_eq!(calls.stage.len(), 1, "the device is staged once");
        assert_eq!(calls.publish.len(), 2, "each pod is published");
        drop(calls);
        assert_eq!(w.asw.get_mounted_volumes().len(), 2);
    }

    /// `markDeviceErrorState` / `markVolumeErrorState` (`:674-728`): a
    /// non-final `MountDevice` failure leaves the device `DeviceMountUncertain`,
    /// a final one leaves it `DeviceNotMounted`.
    #[tokio::test]
    async fn a_failed_stage_marks_the_device_uncertain_only_for_a_non_final_error() {
        use rusternetes_kubelet::volume_plugins::util::operation_executor::DeviceMountState;
        let f = fx(
            "worlduncertain",
            &[Cap::StageUnstageVolume],
            Some(json!({"attachRequired": false})),
        )
        .await;
        let w = world(&f);
        let spec = owned_pv_spec(&f);

        *f.fake.stage_error.lock().unwrap() = Some(tonic::Code::InvalidArgument);
        mount_in_world(&w, &spec, &pod()).await.unwrap_err();
        let attached = w.asw.get_attached_volumes();
        assert_eq!(
            attached[0].device_mount_state,
            DeviceMountState::DeviceNotMounted
        );

        *f.fake.stage_error.lock().unwrap() = Some(tonic::Code::DeadlineExceeded);
        mount_in_world(&w, &spec, &pod()).await.unwrap_err();
        let attached = w.asw.get_attached_volumes();
        assert_eq!(
            attached[0].device_mount_state,
            DeviceMountState::DeviceMountUncertain
        );

        // A final error on an uncertain device resolves it to not mounted.
        *f.fake.stage_error.lock().unwrap() = Some(tonic::Code::InvalidArgument);
        mount_in_world(&w, &spec, &pod()).await.unwrap_err();
        let attached = w.asw.get_attached_volumes();
        assert_eq!(
            attached[0].device_mount_state,
            DeviceMountState::DeviceNotMounted
        );

        // Staging succeeds: globally mounted.
        *f.fake.stage_error.lock().unwrap() = None;
        mount_in_world(&w, &spec, &pod()).await.unwrap();
        let attached = w.asw.get_attached_volumes();
        assert_eq!(
            attached[0].device_mount_state,
            DeviceMountState::DeviceGloballyMounted
        );
    }

    /// `VOLUME_MOUNT_GROUP` (`csi_attacher.go:381-385`): the pod's fsGroup reaches
    /// NodeStageVolume only when the driver advertises the capability.
    #[tokio::test]
    async fn mount_device_delegates_fs_group_to_a_volume_mount_group_driver() {
        let f = fx(
            "stagevmg",
            &[Cap::StageUnstageVolume, Cap::VolumeMountGroup],
            Some(json!({"attachRequired": false})),
        )
        .await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let dm = f.plugin.new_device_mounter().unwrap();
        let path = dm.get_device_mount_path(&spec).unwrap();
        let args = DeviceMounterArgs {
            fs_group: Some(4242),
            node_name: "node-1".into(),
            ..Default::default()
        };
        dm.mount_device(&spec, "", &path, &args).await.unwrap();
        let calls = f.fake.calls.lock().unwrap();
        match calls.stage[0]
            .volume_capability
            .as_ref()
            .unwrap()
            .access_type
            .as_ref()
            .unwrap()
        {
            AccessType::Mount(m) => assert_eq!(m.volume_mount_group, "4242"),
            other => panic!("expected mount, got {other:?}"),
        }
    }

    /// Without `STAGE_UNSTAGE_VOLUME` `MountDevice` only records the metadata
    /// (`csi_attacher.go:357-361`).
    #[tokio::test]
    async fn mount_device_without_stage_capability_is_a_noop_rpc() {
        let f = fx("nocap", &[], Some(json!({"attachRequired": false}))).await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let dm = f.plugin.new_device_mounter().unwrap();
        let path = dm.get_device_mount_path(&spec).unwrap();
        dm.mount_device(&spec, "", &path, &DeviceMounterArgs::default())
            .await
            .unwrap();
        assert!(f.fake.calls.lock().unwrap().stage.is_empty());
        assert!(std::path::Path::new(&path).is_dir());
    }

    /// An unregistered driver is a transient failure (kubernetes#120268).
    #[tokio::test]
    async fn mount_device_with_an_unregistered_driver_is_transient() {
        let f = fx("devunreg", &[], None).await;
        let p = pv("nobody.csi.example.com", json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let dm = f.plugin.new_device_mounter().unwrap();
        let path = dm.get_device_mount_path(&spec).unwrap();
        let err = dm
            .mount_device(&spec, "", &path, &DeviceMounterArgs::default())
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<CsiError>(),
            Some(CsiError::Transient(_))
        ));
    }

    /// #3262: an inline volume whose driver is not registered is a transient
    /// failure, exactly as for a PersistentVolume: `csi_mounter.go:106-109`
    /// ("mounter.SetUpAt failed to get CSI client") applies to every lifecycle
    /// mode. No placeholder directory may be created.
    #[tokio::test]
    async fn set_up_of_an_unregistered_inline_driver_is_transient() {
        let f = fx("inlineunreg", &[], None).await;
        let v: Volume = serde_json::from_value(
            json!({"name": "eph", "csi": {"driver": "nobody-inline.csi.example.com"}}),
        )
        .unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        let err = m.set_up().await.unwrap_err();
        assert!(matches!(
            err.downcast_ref::<CsiError>(),
            Some(CsiError::Transient(_))
        ));
        assert!(err
            .to_string()
            .contains("mounter.SetUpAt failed to get CSI client"));
    }

    /// `CanDeviceMount` (`csi_plugin.go:702-715`): persistent yes, ephemeral no.
    #[test]
    fn can_device_mount_is_persistent_only() {
        let plugin = plugin();
        let p = pv("d.csi.example.com", json!({}));
        let claim = claim_volume();
        assert!(plugin.can_device_mount(&Spec {
            volume: &claim,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        }));
        let inline: Volume =
            serde_json::from_value(json!({"name": "v", "csi": {"driver": "d.csi.example.com"}}))
                .unwrap();
        assert!(!plugin.can_device_mount(&Spec {
            volume: &inline,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        }));
    }

    // ---- can_support / names -------------------------------------------------

    #[test]
    fn supports_an_inline_csi_volume() {
        let v: Volume = serde_json::from_value(
            json!({"name": "csi-inline", "csi": {"driver": "example.com/csi"}}),
        )
        .unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_other_kinds() {
        let v: Volume = serde_json::from_value(json!({"name": "scratch", "emptyDir": {}})).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(!plugin().can_support(&spec));
    }

    /// The PV arm of `CanSupport` (`csi_plugin.go:447-455`): a claim-backed volume
    /// whose bound PV has a CSI source is this plugin's.
    #[test]
    fn supports_a_persistent_volume_with_a_csi_source() {
        let v = claim_volume();
        let p = pv("example.com/csi", json!({}));
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(plugin().can_support(&spec));
    }

    #[test]
    fn rejects_a_persistent_volume_without_a_csi_source() {
        let v = claim_volume();
        let p: rusternetes_common::resources::PersistentVolume = serde_json::from_value(json!({
            "metadata": {"name": "pv1"},
            "spec": {"hostPath": {"path": "/mnt/data"}}
        }))
        .unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(!plugin().can_support(&spec));
    }

    #[test]
    fn plugin_name_is_the_upstream_name() {
        assert_eq!(plugin().name(), rusternetes_kubelet::pod_dirs::plugin::CSI);
    }

    /// `GetVolumeName` for a PV spec: `<driver>^<volumeHandle>`.
    #[test]
    fn get_volume_name_for_a_pv() {
        let v = claim_volume();
        let p = pv("example.com/csi", json!({}));
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert_eq!(
            plugin().get_volume_name(&spec).unwrap(),
            "example.com/csi^vol-1"
        );
    }

    // ---- GetPath -------------------------------------------------------------

    /// `TestMounterGetPath` (`csi_mounter_test.go:84`): the mount path is
    /// `pods/<uid>/volumes/kubernetes.io~csi/<PV name>/mount` — the PV's name, not
    /// the pod volume's, because `Spec.Name()` is the PV's name for a PV-resolved
    /// spec (`plugins.go:444-453`).
    #[tokio::test]
    async fn mounter_get_path_uses_the_pv_name_and_mount_suffix() {
        let v = claim_volume();
        let p = pv("example.com/csi", json!({}));
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = plugin().new_mounter(&spec, &pod()).await.unwrap();
        assert_eq!(
            m.get_path(),
            "/var/lib/rusternetes/pods/uid-1/volumes/kubernetes.io~csi/pv1/mount"
        );
    }

    // ---- SetUp: persistent ---------------------------------------------------

    /// `TestMounterSetUp` / `TestMounterSetUpSimple` "setup with persistent
    /// source": NodePublishVolume carries volume id, target path, volume context,
    /// node-publish secrets, fs type, mount flags and the access mode; the volume
    /// info file is persisted for teardown (`csi_mounter.go:284-300`).
    #[tokio::test]
    async fn set_up_publishes_a_persistent_volume() {
        let f = fx("pub", &[], Some(json!({"attachRequired": false}))).await;
        put_secret(&f.storage, "sec-ns", "pub-secret", "token", "s3cret").await;
        let p = pv(
            &f.driver,
            json!({
                "fsType": "ext4",
                "volumeAttributes": {"a": "b"},
                "nodePublishSecretRef": {"name": "pub-secret", "namespace": "sec-ns"}
            }),
        );
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();

        let calls = f.fake.calls.lock().unwrap();
        assert_eq!(calls.publish.len(), 1);
        let req = &calls.publish[0];
        assert_eq!(req.volume_id, "vol-1");
        assert_eq!(req.target_path, m.get_path());
        assert_eq!(req.volume_context["a"], "b");
        assert_eq!(req.secrets["token"], "s3cret");
        assert!(req.staging_target_path.is_empty(), "no STAGE_UNSTAGE");
        assert!(!req.readonly);
        match req
            .volume_capability
            .as_ref()
            .unwrap()
            .access_type
            .as_ref()
            .unwrap()
        {
            AccessType::Mount(mnt) => {
                assert_eq!(mnt.fs_type, "ext4");
                assert_eq!(mnt.mount_flags, vec!["foo=bar"]);
            }
            other => panic!("expected mount, got {other:?}"),
        }
        drop(calls);

        // volume info persisted next to the mount dir
        let data_file = std::path::Path::new(&m.get_path())
            .parent()
            .unwrap()
            .join("vol_data.json");
        let data: HashMap<String, String> =
            serde_json::from_str(&std::fs::read_to_string(data_file).unwrap()).unwrap();
        assert_eq!(data["specVolID"], "pv1");
        assert_eq!(data["volumeHandle"], "vol-1");
        assert_eq!(data["driverName"], f.driver);
        assert_eq!(data["nodeName"], "node-1");
        assert_eq!(data["volumeLifecycleMode"], "Persistent");
        assert!(data["attachmentID"].starts_with("csi-"));
    }

    /// `Spec.ReadOnly` (the claim's `readOnly`) reaches NodePublishVolume (
    /// `csi_plugin.go:499`, `:513`).
    #[tokio::test]
    async fn set_up_passes_the_claims_read_only_flag() {
        let f = fx("ro", &[], Some(json!({"attachRequired": false}))).await;
        let p = pv(&f.driver, json!({}));
        let v: Volume = serde_json::from_value(
            json!({"name": "data", "persistentVolumeClaim": {"claimName": "c", "readOnly": true}}),
        )
        .unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: true,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();
        assert!(f.fake.calls.lock().unwrap().publish[0].readonly);
    }

    /// With STAGE_UNSTAGE_VOLUME the volume is staged first (`MountDevice`,
    /// `csi_attacher.go:264-411`), then published with the staging path
    /// (`csi_mounter.go:164-178`). Stage gets the node-STAGE secret.
    #[tokio::test]
    async fn set_up_stages_then_publishes_when_the_driver_supports_it() {
        let f = fx(
            "stage",
            &[Cap::StageUnstageVolume],
            Some(json!({"attachRequired": false})),
        )
        .await;
        put_secret(&f.storage, "sec-ns", "stage-secret", "k", "stage-v").await;
        let p = pv(
            &f.driver,
            json!({
                "fsType": "xfs",
                "nodeStageSecretRef": {"name": "stage-secret", "namespace": "sec-ns"}
            }),
        );
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        mount_volume(&f, &spec, &pod()).await;

        let calls = f.fake.calls.lock().unwrap();
        assert_eq!(calls.stage.len(), 1);
        let stage = &calls.stage[0];
        assert_eq!(stage.volume_id, "vol-1");
        assert_eq!(stage.secrets["k"], "stage-v");
        // <root>/plugins/kubernetes.io/csi/<driver>/<sha256(handle)>/globalmount
        let want_prefix = format!("{}/plugins/kubernetes.io/csi/{}/", f.root, f.driver);
        assert!(
            stage.staging_target_path.starts_with(&want_prefix)
                && stage.staging_target_path.ends_with("/globalmount"),
            "{}",
            stage.staging_target_path
        );
        assert!(std::path::Path::new(&stage.staging_target_path).is_dir());
        assert_eq!(calls.publish.len(), 1);
        assert_eq!(
            calls.publish[0].staging_target_path,
            stage.staging_target_path
        );
    }

    /// Pod information is injected into the volume context when the CSIDriver sets
    /// `podInfoOnMount` (`getPodInfoAttrs`, `csi_util.go:208`).
    #[tokio::test]
    async fn set_up_injects_pod_info_when_the_driver_asks_for_it() {
        let f = fx(
            "podinfo",
            &[],
            Some(json!({"attachRequired": false, "podInfoOnMount": true})),
        )
        .await;
        let p = pv(&f.driver, json!({"volumeAttributes": {"a": "b"}}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();
        let calls = f.fake.calls.lock().unwrap();
        let vc = &calls.publish[0].volume_context;
        assert_eq!(vc["a"], "b");
        assert_eq!(vc["csi.storage.k8s.io/pod.name"], "p");
        assert_eq!(vc["csi.storage.k8s.io/pod.namespace"], "ns1");
        assert_eq!(vc["csi.storage.k8s.io/pod.uid"], "uid-1");
        assert_eq!(vc["csi.storage.k8s.io/serviceAccount.name"], "sa-1");
        assert_eq!(vc["csi.storage.k8s.io/ephemeral"], "false");
    }

    /// Without `podInfoOnMount` the context is left alone.
    #[tokio::test]
    async fn set_up_omits_pod_info_by_default() {
        let f = fx("nopodinfo", &[], Some(json!({"attachRequired": false}))).await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();
        assert!(f.fake.calls.lock().unwrap().publish[0]
            .volume_context
            .is_empty());
    }

    /// `TestPodServiceAccountTokenAttrs` (`csi_mounter_test.go:1286`): a CSIDriver
    /// with `tokenRequests` gets `csi.storage.k8s.io/serviceAccount.tokens` in the
    /// volume context, a JSON map audience -> `{token, expirationTimestamp}`
    /// (`podServiceAccountTokenAttrs`, `csi_mounter.go:358-416`).
    #[tokio::test]
    async fn set_up_injects_service_account_tokens_into_the_volume_context() {
        let f = fx(
            "satoken",
            &[],
            Some(json!({"attachRequired": false,
                "tokenRequests": [{"audience": "gcp", "expirationSeconds": 3600}, {"audience": ""}]})),
        )
        .await;
        f.storage
            .create(
                &build_key("serviceaccounts", Some("ns1"), "sa-1"),
                &json!({"apiVersion": "v1", "kind": "ServiceAccount",
                    "metadata": {"name": "sa-1", "namespace": "ns1", "uid": "sa-uid-1"}}),
            )
            .await
            .unwrap();
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();
        let calls = f.fake.calls.lock().unwrap();
        let req = &calls.publish[0];
        let raw = req
            .volume_context
            .get("csi.storage.k8s.io/serviceAccount.tokens")
            .expect("tokens attribute missing from volume_context");
        let tokens: serde_json::Value = serde_json::from_str(raw).unwrap();
        // One entry per tokenRequest, keyed by the requested audience ("" kept).
        assert_eq!(tokens.as_object().unwrap().len(), 2, "{raw}");
        let gcp = tokens["gcp"]["token"].as_str().expect("gcp token");
        assert!(tokens["gcp"]["expirationTimestamp"].as_str().is_some());
        assert!(tokens[""]["token"].as_str().is_some());
        // The token is for the pod's service account, bound to the requested audience.
        let claims = rusternetes_common::auth::TokenManager::new_auto(b"test-secret")
            .validate_token_with_audiences(gcp, &["gcp".to_string()])
            .unwrap();
        assert_eq!(claims.sub, "system:serviceaccount:ns1:sa-1");
        // Not in the secrets unless the driver asks for it.
        assert!(!req
            .secrets
            .contains_key("csi.storage.k8s.io/serviceAccount.tokens"));
    }

    /// `serviceAccountTokenGetter` is the kubelet token manager
    /// (`csi_plugin.go:309`, `volume_host.go:252`): a re-SetUp (what
    /// `RequiresRepublish` drives) is served from its cache, not re-minted.
    #[tokio::test]
    async fn republish_reuses_the_cached_service_account_token() {
        let f = fx(
            "satokencache",
            &[],
            Some(json!({"attachRequired": false,
                "tokenRequests": [{"audience": "gcp", "expirationSeconds": 3600}]})),
        )
        .await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();
        // JWT iat/exp have one-second resolution: a re-mint after this sleep differs.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        m.set_up().await.unwrap();
        let calls = f.fake.calls.lock().unwrap();
        let token = |i: usize| {
            let raw = &calls.publish[i].volume_context["csi.storage.k8s.io/serviceAccount.tokens"];
            let v: serde_json::Value = serde_json::from_str(raw).unwrap();
            v["gcp"]["token"].as_str().unwrap().to_string()
        };
        assert!(calls.publish.len() >= 2);
        assert_eq!(token(0), token(1));
    }

    /// `serviceAccountTokenInSecrets`: the tokens move from `volume_context` to
    /// `node_publish_secrets` (`csi_mounter.go:243-249`), so they stay out of the
    /// PV-visible context.
    #[tokio::test]
    async fn set_up_puts_service_account_tokens_in_secrets_when_asked() {
        let f = fx(
            "satokensecrets",
            &[],
            Some(
                json!({"attachRequired": false, "serviceAccountTokenInSecrets": true,
                "tokenRequests": [{"audience": "gcp"}]}),
            ),
        )
        .await;
        put_secret(&f.storage, "sec-ns", "pub-secret", "token", "s3cret").await;
        let p = pv(
            &f.driver,
            json!({"nodePublishSecretRef": {"name": "pub-secret", "namespace": "sec-ns"}}),
        );
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();
        let calls = f.fake.calls.lock().unwrap();
        let req = &calls.publish[0];
        assert!(!req
            .volume_context
            .contains_key("csi.storage.k8s.io/serviceAccount.tokens"));
        assert_eq!(req.secrets["token"], "s3cret", "merged, not replaced");
        assert!(req.secrets["csi.storage.k8s.io/serviceAccount.tokens"].contains("\"gcp\""));
    }

    /// A CSIDriver without `tokenRequests` adds nothing.
    #[tokio::test]
    async fn set_up_without_token_requests_adds_no_token_attribute() {
        let f = fx("notokens", &[], Some(json!({"attachRequired": false}))).await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();
        assert!(f.fake.calls.lock().unwrap().publish[0]
            .volume_context
            .is_empty());
    }

    /// The publish context comes from the node's VolumeAttachment
    /// (`getPublishContext`, `csi_plugin.go:906-928`) when attach is required.
    #[tokio::test]
    async fn set_up_reads_the_publish_context_from_the_volume_attachment() {
        let f = fx("va", &[], None).await; // no CSIDriver object: attach NOT skipped
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();

        // No VolumeAttachment yet: a transient failure, nothing published.
        let err = m.set_up().await.unwrap_err();
        assert!(
            matches!(err.downcast_ref::<CsiError>(), Some(CsiError::Transient(_))),
            "{err:#}"
        );
        assert!(err.to_string().contains("failed to fetch publishContext"));
        assert!(f.fake.calls.lock().unwrap().publish.is_empty());

        let attach_id = get_attachment_name("vol-1", &f.driver, "node-1");
        let va = json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
            "metadata": {"name": attach_id},
            "spec": {"attacher": f.driver, "nodeName": "node-1", "source": {"persistentVolumeName": "pv1"}},
            "status": {"attached": true, "attachmentMetadata": {"devicePath": "/dev/x"}}
        });
        f.storage
            .create(&build_key("volumeattachments", None, &attach_id), &va)
            .await
            .unwrap();
        m.set_up().await.unwrap();
        assert_eq!(
            f.fake.calls.lock().unwrap().publish[0].publish_context["devicePath"],
            "/dev/x"
        );
    }

    /// `TestMounterSetUpSimple` "setup with unknown CSI driver": no registered
    /// driver is a TRANSIENT failure (`csi_mounter.go:105-109`) and nothing is
    /// silently mounted.
    #[tokio::test]
    async fn set_up_with_an_unregistered_driver_is_transient() {
        let f = fx("unreg", &[], Some(json!({"attachRequired": false}))).await;
        let p = pv("never-registered.csi.example.com", json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        let err = m.set_up().await.unwrap_err();
        assert!(
            matches!(err.downcast_ref::<CsiError>(), Some(CsiError::Transient(_))),
            "{err:#}"
        );
        assert!(
            err.to_string().contains("failed to get CSI client"),
            "{err:#}"
        );
        assert!(!std::path::Path::new(&m.get_path()).exists());
    }

    /// `TestMounterSetupJsonFileHandling`: a FINAL publish error removes the
    /// volume info file and mount dir; a non-final (uncertain) one keeps them so
    /// teardown can still find the volume (`csi_mounter.go:300-310`).
    #[tokio::test]
    async fn final_publish_error_cleans_up_but_uncertain_keeps_the_data_file() {
        for (code, keeps) in [
            (tonic::Code::InvalidArgument, false),
            (tonic::Code::Aborted, true),
            (tonic::Code::DeadlineExceeded, true),
        ] {
            let f = fx("jsonfile", &[], Some(json!({"attachRequired": false}))).await;
            *f.fake.publish_error.lock().unwrap() = Some(code);
            let p = pv(&f.driver, json!({}));
            let v = claim_volume();
            let spec = Spec {
                volume: &v,
                persistent_volume: Some(&p),
                read_only: false,
                migrated: false,
                inline_volume_spec_for_csi_migration: false,
            };
            let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
            let err = m.set_up().await.unwrap_err();
            let parent = std::path::PathBuf::from(m.get_path())
                .parent()
                .unwrap()
                .to_path_buf();
            assert_eq!(
                parent.join("vol_data.json").exists(),
                keeps,
                "{code:?}: {err:#}"
            );
            assert_eq!(
                matches!(
                    err.downcast_ref::<CsiError>(),
                    Some(CsiError::UncertainProgress(_))
                ),
                keeps,
                "{code:?}"
            );
        }
    }

    /// A PV resolved from a claim whose driver does not list `Persistent` is
    /// rejected (`supportsVolumeLifecycleMode`, `csi_mounter.go:531-560`).
    #[tokio::test]
    async fn set_up_rejects_a_driver_that_does_not_support_the_mode() {
        let f = fx(
            "modes",
            &[],
            Some(json!({"attachRequired": false, "volumeLifecycleModes": ["Ephemeral"]})),
        )
        .await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        let err = m.set_up().await.unwrap_err();
        assert!(
            err.to_string()
                .contains("volume mode \"Persistent\" not supported by driver"),
            "{err:#}"
        );
        assert!(f.fake.calls.lock().unwrap().publish.is_empty());
    }

    // ---- SetUp: inline (ephemeral) -------------------------------------------

    /// `TestMounterSetUpWithInline`: an inline volume on a registered driver with
    /// an Ephemeral-capable CSIDriver is published under a generated handle
    /// `csi-<sha256(podUID + volumeName)>` (`makeVolumeHandle`, `csi_mounter.go:612`).
    #[tokio::test]
    async fn set_up_publishes_an_inline_volume_on_a_registered_driver() {
        let f = fx(
            "inline",
            &[],
            Some(json!({"volumeLifecycleModes": ["Ephemeral"]})),
        )
        .await;
        put_secret(&f.storage, "ns1", "inline-secret", "k", "v").await;
        let v: Volume = serde_json::from_value(json!({
            "name": "eph",
            "csi": {
                "driver": f.driver, "fsType": "ext4",
                "volumeAttributes": {"x": "y"},
                "nodePublishSecretRef": {"name": "inline-secret"}
            }
        }))
        .unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        assert_eq!(
            m.get_path(),
            format!("{}/pods/uid-1/volumes/kubernetes.io~csi/eph/mount", f.root)
        );
        m.set_up().await.unwrap();
        let calls = f.fake.calls.lock().unwrap();
        let req = &calls.publish[0];
        assert!(req.volume_id.starts_with("csi-") && req.volume_id.len() == 4 + 64);
        assert_eq!(req.volume_context["x"], "y");
        assert_eq!(
            req.secrets["k"], "v",
            "inline secret ref uses the pod's namespace"
        );
        assert!(calls.stage.is_empty(), "inline volumes are never staged");
    }

    // ---- TearDown --------------------------------------------------------------

    /// `TestUnmounterTeardown` (`csi_mounter_test.go:1172`): the unmounter reloads
    /// the volume info file, calls NodeUnpublishVolume with the saved handle and the
    /// mount path, then removes the mount dir, the info file and the volume dir.
    #[tokio::test]
    async fn tear_down_unpublishes_and_cleans_up() {
        let f = fx("teardown", &[], Some(json!({"attachRequired": false}))).await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();
        let mount = std::path::PathBuf::from(m.get_path());
        assert!(mount.is_dir());

        let um = f.plugin.new_unmounter("pv1", "uid-1").unwrap();
        assert_eq!(um.get_path(), m.get_path());
        um.tear_down().await.unwrap();

        let calls = f.fake.calls.lock().unwrap();
        assert_eq!(calls.unpublish.len(), 1);
        assert_eq!(calls.unpublish[0].volume_id, "vol-1");
        assert_eq!(calls.unpublish[0].target_path, m.get_path());
        assert!(!mount.exists());
        assert!(!mount.parent().unwrap().exists(), "volume dir removed");
    }

    /// `TestUnmounterTeardownNoClientError` (`csi_mounter_test.go:1228`): a driver
    /// that is no longer registered is a transient failure, not a silent success.
    #[tokio::test]
    async fn tear_down_with_an_unregistered_driver_is_transient() {
        let f = fx(
            "teardown-noclient",
            &[],
            Some(json!({"attachRequired": false})),
        )
        .await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();
        let um = f.plugin.new_unmounter("pv1", "uid-1").unwrap();

        csi_drivers().delete(&f.driver);
        let err = um.tear_down().await.unwrap_err();
        assert!(
            matches!(err.downcast_ref::<CsiError>(), Some(CsiError::Transient(_))),
            "{err:#}"
        );
        assert!(f.fake.calls.lock().unwrap().unpublish.is_empty());
    }

    /// `NewUnmounter` fails when no volume info file exists
    /// (`csi_plugin.go:560-563`).
    #[test]
    fn new_unmounter_without_a_data_file_fails() {
        let err = plugin().new_unmounter("missing", "uid-x").err().unwrap();
        assert!(
            err.to_string()
                .contains("unmounter failed to load volume data file"),
            "{err:#}"
        );
    }

    /// An orphaned pod's published CSI volume is NodeUnpublished by the kubelet's
    /// cleanup: upstream's reconciler `unmountVolumes` -> `UnmountVolume` ->
    /// `NewUnmounter` -> `TearDownAt` (`reconciler.go`, `operation_generator.go`
    /// `GenerateUnmountVolumeFunc`, `csi_plugin.go:540-567`, `csi_mounter.go:432-466`).
    /// A live pod's volume must be left alone.
    #[tokio::test]
    async fn orphaned_pod_csi_volume_is_unpublished() {
        let f = fx("orphan", &[], Some(json!({"attachRequired": false}))).await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();
        let mount = std::path::PathBuf::from(m.get_path());
        let vm = rusternetes_kubelet::volumes::VolumeManager::new_with_recording_empty_dir_mounts(
            f.root.clone(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        );

        // Live pod: untouched.
        let live: std::collections::HashSet<String> = ["uid-1".to_string()].into();
        vm.unmount_orphaned_volumes(&live, &std::collections::HashSet::new())
            .await;
        assert!(f.fake.calls.lock().unwrap().unpublish.is_empty());
        assert!(mount.is_dir());

        // Orphaned pod: NodeUnpublishVolume with the saved handle + mount path.
        vm.unmount_orphaned_volumes(
            &std::collections::HashSet::new(),
            &std::collections::HashSet::new(),
        )
        .await;
        let calls = f.fake.calls.lock().unwrap();
        assert_eq!(calls.unpublish.len(), 1);
        assert_eq!(calls.unpublish[0].volume_id, "vol-1");
        assert_eq!(calls.unpublish[0].target_path, m.get_path());
        assert!(!mount.exists());
    }

    // ---- ConstructVolumeSpec / UnmountDevice (#2423, #2424) ------------------

    fn volume_manager(root: &str) -> rusternetes_kubelet::volumes::VolumeManager {
        rusternetes_kubelet::volumes::VolumeManager::new_with_recording_empty_dir_mounts(
            root.to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
        )
    }

    /// `TestPluginConstructVolumeSpec` (`csi_plugin_test.go:319-434`): the spec
    /// rebuilt from `vol_data.json` carries the persisted PV name, driver and
    /// handle.
    #[tokio::test]
    async fn construct_volume_spec_rebuilds_a_persistent_spec() {
        let f = fx("construct", &[], Some(json!({"attachRequired": false}))).await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();
        let volume_dir = std::path::PathBuf::from(m.get_path())
            .parent()
            .unwrap()
            .to_path_buf();

        let rec = f
            .plugin
            .construct_volume_spec("pv1", volume_dir.to_str().unwrap())
            .unwrap();
        let rebuilt = rec.persistent_volume.expect("a PV spec");
        let csi = rebuilt.spec.csi.as_ref().expect("a CSI source");
        assert_eq!(csi.driver, f.driver);
        assert_eq!(csi.volume_handle.as_deref(), Some("vol-1"));
        assert_eq!(rebuilt.metadata.name, "pv1");
        assert_eq!(rec.volume.as_ref().unwrap().name, "pv1");
    }

    /// `TestPluginConstructVolumeSpecWithInline` (`csi_plugin_test.go:436`): an
    /// ephemeral volume is rebuilt as a `CSIVolumeSource`, not a PV.
    #[tokio::test]
    async fn construct_volume_spec_rebuilds_an_inline_spec() {
        let f = fx("construct-inline", &[], None).await;
        let dir = std::path::Path::new(&f.root).join("vol");
        std::fs::create_dir_all(&dir).unwrap();
        let data = HashMap::from([
            ("specVolID".to_string(), "inl".to_string()),
            ("driverName".to_string(), f.driver.clone()),
            ("volumeHandle".to_string(), "csi-abc".to_string()),
            ("volumeLifecycleMode".to_string(), "Ephemeral".to_string()),
        ]);
        save_volume_data(&dir, &data).unwrap();
        let rec = f
            .plugin
            .construct_volume_spec("vol", dir.to_str().unwrap())
            .unwrap();
        assert!(rec.persistent_volume.is_none());
        assert_eq!(rec.volume.as_ref().unwrap().name, "inl");
        assert_eq!(
            rec.volume.as_ref().unwrap().csi.as_ref().unwrap().driver,
            f.driver
        );
    }

    #[tokio::test]
    async fn construct_volume_spec_fails_without_a_data_file() {
        let f = fx("construct-none", &[], None).await;
        assert!(f.plugin.construct_volume_spec("x", &f.root).is_err());
    }

    /// Lay out a staged device (`<g>/globalmount` + `<g>/vol_data.json`) by hand.
    fn staged_dir_only(f: &Fx) -> std::path::PathBuf {
        let device = std::path::Path::new(&f.root).join("g/globalmount");
        std::fs::create_dir_all(&device).unwrap();
        save_volume_data(
            device.parent().unwrap(),
            &HashMap::from([
                ("driverName".to_string(), f.driver.clone()),
                ("volumeHandle".to_string(), "vol-1".to_string()),
            ]),
        )
        .unwrap();
        device
    }

    /// `TestAttacherUnmountDevice`, "success, json file exists"
    /// (`csi_attacher_test.go:1490`): `NodeUnstageVolume(volID, deviceMountPath)`,
    /// then the global dir and json file are removed.
    #[tokio::test]
    async fn unmount_device_unstages_and_removes_the_global_dir() {
        let f = fx(
            "unmountdev",
            &[Cap::StageUnstageVolume],
            Some(json!({"attachRequired": false})),
        )
        .await;
        let device = staged_dir_only(&f);
        f.plugin.unmount_device(&device).await.unwrap();
        let calls = f.fake.calls.lock().unwrap();
        assert_eq!(calls.unstage.len(), 1);
        assert_eq!(calls.unstage[0].volume_id, "vol-1");
        assert_eq!(
            calls.unstage[0].staging_target_path,
            device.to_string_lossy()
        );
        assert!(!device.exists());
        assert!(!device.parent().unwrap().exists());
    }

    /// "stage_unstage not set ... unmount device is skipped" (`:1497`): no RPC,
    /// but the global dir and json file are still removed.
    #[tokio::test]
    async fn unmount_device_without_stage_capability_only_cleans_up() {
        let f = fx("unmountdev-nocap", &[], None).await;
        let device = staged_dir_only(&f);
        f.plugin.unmount_device(&device).await.unwrap();
        assert!(f.fake.calls.lock().unwrap().unstage.is_empty());
        assert!(!device.parent().unwrap().exists());
    }

    /// `NewDeviceUnmounter` (`csi_plugin.go:717-719`) hands back the attacher, whose
    /// `UnmountDevice` is the same `NodeUnstageVolume` path.
    #[tokio::test]
    async fn new_device_unmounter_unstages_through_the_attacher() {
        let f = fx(
            "devunmounter",
            &[Cap::StageUnstageVolume],
            Some(json!({"attachRequired": false})),
        )
        .await;
        let device = staged_dir_only(&f);
        let unmounter = DeviceMountableVolumePlugin::new_device_unmounter(&f.plugin).unwrap();
        unmounter
            .unmount_device(&device.to_string_lossy())
            .await
            .unwrap();
        assert_eq!(f.fake.calls.lock().unwrap().unstage.len(), 1);
        assert!(!device.parent().unwrap().exists());
    }

    /// "success: json file doesn't exist, unmount device is skipped" (`:1503`).
    #[tokio::test]
    async fn unmount_device_skips_when_the_data_file_is_missing() {
        let f = fx("unmountdev-nofile", &[Cap::StageUnstageVolume], None).await;
        let device = std::path::Path::new(&f.root).join("g/globalmount");
        std::fs::create_dir_all(&device).unwrap();
        f.plugin.unmount_device(&device).await.unwrap();
        assert!(f.fake.calls.lock().unwrap().unstage.is_empty());
    }

    /// "fail: invalid json" (`:1511`).
    #[tokio::test]
    async fn unmount_device_fails_on_invalid_json() {
        let f = fx("unmountdev-badjson", &[Cap::StageUnstageVolume], None).await;
        let device = std::path::Path::new(&f.root).join("g/globalmount");
        std::fs::create_dir_all(&device).unwrap();
        std::fs::write(
            device.parent().unwrap().join("vol_data.json"),
            "{\"driverName\"}}",
        )
        .unwrap();
        assert!(f.plugin.unmount_device(&device).await.is_err());
    }

    /// "fail with transient error, json file exists but client not found" (`:1519`).
    #[tokio::test]
    async fn unmount_device_with_an_unregistered_driver_is_transient() {
        let f = fx("unmountdev-nodriver", &[Cap::StageUnstageVolume], None).await;
        let device = std::path::Path::new(&f.root).join("g/globalmount");
        std::fs::create_dir_all(&device).unwrap();
        std::fs::write(
            device.parent().unwrap().join("vol_data.json"),
            r#"{"driverName":"unknown.example.com","volumeHandle":"h"}"#,
        )
        .unwrap();
        let e = f.plugin.unmount_device(&device).await.unwrap_err();
        assert!(
            matches!(e.downcast_ref::<CsiError>(), Some(CsiError::Transient(_))),
            "{e:#}"
        );
    }

    /// Pod teardown mechanism: a volume is unpublished first, and the shared staged
    /// device is unstaged only once NO pod still holds it (`unmountDetachDevices`,
    /// `reconciler_common.go:273-315`: `GetUnmountedVolumes` + `!DSW.VolumeExists`).
    #[tokio::test]
    async fn staged_device_is_unstaged_only_after_the_last_pod_unpublishes() {
        let f = fx(
            "refcount",
            &[Cap::StageUnstageVolume],
            Some(json!({"attachRequired": false})),
        )
        .await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let mut pod_b = pod();
        pod_b.metadata.uid = "uid-2".to_string();
        mount_volume(&f, &spec, &pod()).await;
        mount_volume(&f, &spec, &pod_b).await;
        let vm = volume_manager(&f.root);

        // Pod 2 is live, pod 1 gone: unpublish pod 1 only; the device stays staged.
        let none = std::collections::HashSet::new();
        let live2: std::collections::HashSet<String> = ["uid-2".to_string()].into();
        vm.unmount_orphaned_volumes(&live2, &none).await;
        vm.unmount_unused_csi_devices(&[]).await;
        assert_eq!(f.fake.calls.lock().unwrap().unpublish.len(), 1);
        assert!(
            f.fake.calls.lock().unwrap().unstage.is_empty(),
            "pod 2 still holds the device"
        );

        // Both gone: unpublish pod 2, then unstage exactly once.
        vm.unmount_orphaned_volumes(&none, &none).await;
        vm.unmount_unused_csi_devices(&[]).await;
        let calls = f.fake.calls.lock().unwrap();
        assert_eq!(calls.unpublish.len(), 2);
        assert_eq!(calls.unstage.len(), 1);
    }

    /// A live pod that still wants the volume (desired state of the world) keeps
    /// its device staged even before it has published (`!DSW.VolumeExists`).
    #[tokio::test]
    async fn desired_volume_keeps_its_device_staged() {
        let f = fx(
            "desired",
            &[Cap::StageUnstageVolume],
            Some(json!({"attachRequired": false})),
        )
        .await;
        let device = staged_dir_only(&f);
        let vm = volume_manager(&f.root);
        // The plugin dir is under the kubelet root; move the hand-made layout there.
        // `GetDeviceMountPath` (`csi_attacher.go:598-622`) derives the directory
        // from the driver and the sha256 of the handle, so the layout must match.
        let target = make_device_mount_path(&plugin_dir(&f.root), &f.driver, "vol-1")
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::rename(device.parent().unwrap(), &target).unwrap();

        let desired = std::collections::HashSet::from([(f.driver.clone(), "vol-1".to_string())]);
        vm.unmount_csi_devices_not_in(&desired).await;
        assert!(f.fake.calls.lock().unwrap().unstage.is_empty());
        assert!(target.exists());

        // Once nothing wants it, it is unstaged.
        vm.unmount_csi_devices_not_in(&std::collections::HashSet::new())
            .await;
        assert_eq!(f.fake.calls.lock().unwrap().unstage.len(), 1);
        assert!(!target.exists());
    }

    /// `GenerateUnmountDeviceFunc`'s refs gate (`operation_generator.go:846-856`)
    /// as the sweep applies it: the plugin the sweep asks (`FindDeviceMountable
    /// PluginByName` over the manager's registry) answers `GetDeviceMountRefs`
    /// from its host's mounter, so a reference to the device mount path elsewhere
    /// blocks `NodeUnstageVolume` until it is gone, and the mounter's mountinfo is
    /// injectable (`FakeMounter.MountPoints` stand-in).
    #[tokio::test]
    async fn staged_device_with_an_outside_reference_is_not_unstaged() {
        let f = fx(
            "outsideref",
            &[Cap::StageUnstageVolume],
            Some(json!({"attachRequired": false})),
        )
        .await;
        let device = staged_dir_only(&f);
        // `GetDeviceMountPath` (`csi_attacher.go:598-622`) derives the directory
        // from the driver and the sha256 of the handle, so the layout must match.
        let target = make_device_mount_path(&plugin_dir(&f.root), &f.driver, "vol-1")
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::rename(device.parent().unwrap(), &target).unwrap();
        let global = std::fs::canonicalize(target.join("globalmount")).unwrap();

        let mountinfo = std::path::Path::new(&f.root).join("mountinfo");
        let write_info = |outside: bool| {
            let mut info = format!(
                "10 1 8:1 / / rw - ext4 /dev/sda1 rw\n20 10 8:1 /r {} rw - ext4 /dev/sda1 rw\n",
                global.display()
            );
            if outside {
                info.push_str("21 10 8:1 /r /mnt/other rw - ext4 /dev/sda1 rw\n");
            }
            std::fs::write(&mountinfo, info).unwrap();
        };

        let host = Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                f.root.clone(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                HashMap::new(),
            )
            .with_mounter(
                rusternetes_kubelet::volume_plugins::util::mount_ops::Mounter::new(
                    "", false, false,
                )
                .with_mountinfo_path(mountinfo.to_str().unwrap()),
            ),
        );
        let mut vm = volume_manager(&f.root);
        vm.plugin_mgr = Arc::new(rusternetes_kubelet::volume_plugins::VolumePluginMgr::new(
            vec![Box::new(CsiPlugin::new(host))],
        ));
        let none = std::collections::HashSet::new();

        write_info(true);
        vm.unmount_csi_devices_not_in(&none).await;
        assert!(
            f.fake.calls.lock().unwrap().unstage.is_empty(),
            "an outside reference must block NodeUnstageVolume"
        );
        assert!(target.exists());

        write_info(false);
        // The failed `UnmountDevice` is in exponential backoff
        // (`nestedpendingoperations`, 500ms after the first error), so an
        // immediate retry is refused even though the reference is gone.
        vm.unmount_csi_devices_not_in(&none).await;
        assert!(
            f.fake.calls.lock().unwrap().unstage.is_empty(),
            "a retry inside the backoff window must not run UnmountDevice"
        );
        tokio::time::sleep(std::time::Duration::from_millis(550)).await;
        vm.unmount_csi_devices_not_in(&none).await;
        assert_eq!(f.fake.calls.lock().unwrap().unstage.len(), 1);
        assert!(!target.exists());
    }

    /// #2424: a terminated pod that is still present in the API has its volumes
    /// unmounted too (`findAndRemoveDeletedPods` + `ShouldPodRuntimeBeRemoved`,
    /// `pod_workers.go:698`); a running live pod's are left alone.
    #[tokio::test]
    async fn terminated_live_pod_csi_volume_is_unpublished() {
        let f = fx("terminated", &[], Some(json!({"attachRequired": false}))).await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        m.set_up().await.unwrap();
        let vm = volume_manager(&f.root);
        let live: std::collections::HashSet<String> = ["uid-1".to_string()].into();
        vm.unmount_orphaned_volumes(&live, &std::collections::HashSet::new())
            .await;
        assert!(f.fake.calls.lock().unwrap().unpublish.is_empty());
        vm.unmount_orphaned_volumes(&live, &live).await;
        assert_eq!(f.fake.calls.lock().unwrap().unpublish.len(), 1);
    }

    // ---- fsGroup (`csi_mounter.go:126-129`, `:250-260`, `:333-352`, `:469-527`) ----

    mod fs_group {
        use super::*;
        use rusternetes_common::resources::PersistentVolumeAccessMode as Am;
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        fn fs_group_pod(gid: i64, policy: Option<&str>) -> Pod {
            let mut sc = json!({"fsGroup": gid});
            if let Some(p) = policy {
                sc["fsGroupChangePolicy"] = json!(p);
            }
            serde_json::from_value(json!({
                "metadata": {"name": "p", "namespace": "ns1", "uid": "uid-1"},
                "spec": {"containers": [], "nodeName": "node-1", "securityContext": sc}
            }))
            .unwrap()
        }

        /// The test process's own gid: chowning to it needs no privilege.
        fn own_gid(f: &Fx) -> i64 {
            std::fs::metadata(f._dir.path()).unwrap().gid() as i64
        }

        fn mode(p: &std::path::Path) -> u32 {
            std::fs::metadata(p).unwrap().permissions().mode() & 0o7777
        }

        /// Mount a PV with a 0600 file already in the mount dir (what the driver's
        /// NodePublish would have populated) and return that file.
        async fn mount(
            f: &Fx,
            csi: serde_json::Value,
            modes: Vec<Am>,
            policy: Option<&str>,
            root_mode: Option<u32>,
        ) -> std::path::PathBuf {
            let mut p = pv(&f.driver, csi);
            p.spec.access_modes = modes;
            let v = claim_volume();
            let spec = Spec {
                volume: &v,
                persistent_volume: Some(&p),
                read_only: false,
                migrated: false,
                inline_volume_spec_for_csi_migration: false,
            };
            let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
            let dir = std::path::PathBuf::from(m.get_path());
            std::fs::create_dir_all(&dir).unwrap();
            if let Some(rm) = root_mode {
                std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(rm)).unwrap();
            }
            let file = dir.join("f");
            std::fs::write(&file, b"x").unwrap();
            std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
            // The pod's fsGroup reaches the mounter only as `MounterArgs`
            // (`operation_generator.go:582-589`).
            let fsg = fs_group_pod(own_gid(f), policy);
            m.set_up_with(&rusternetes_kubelet::volume_plugins::MounterArgs {
                fs_group: rusternetes_kubelet::volume_plugins::util::fs_group_from(&fsg),
                fs_group_change_policy: policy.map(str::to_string),
                ..Default::default()
            })
            .await
            .unwrap();
            file
        }

        fn driver_spec() -> Option<serde_json::Value> {
            Some(json!({"attachRequired": false}))
        }

        /// Default policy, an RWO PV with an fsType: the kubelet applies fsGroup
        /// with `mode | rwMask` and setgid on the root, and does not ask the driver.
        #[tokio::test]
        async fn kubelet_applies_fs_group_for_an_rwo_volume_with_fs_type() {
            let f = fx("fsg-apply", &[], driver_spec()).await;
            let file = mount(
                &f,
                json!({"fsType": "ext4"}),
                vec![Am::ReadWriteOnce],
                None,
                None,
            )
            .await;
            assert_eq!(mode(&file), 0o660);
            assert_eq!(mode(file.parent().unwrap()) & 0o2000, 0o2000);
            let calls = f.fake.calls.lock().unwrap();
            match calls.publish[0]
                .volume_capability
                .as_ref()
                .unwrap()
                .access_type
                .as_ref()
                .unwrap()
            {
                AccessType::Mount(m) => assert_eq!(m.volume_mount_group, ""),
                other => panic!("expected mount, got {other:?}"),
            }
        }

        /// `ReadWriteOnceWithFSType` (the default) skips a ReadWriteMany volume
        /// and a volume with no fsType (`csi_mounter.go:478-497`).
        #[tokio::test]
        async fn default_policy_skips_rwx_and_missing_fs_type() {
            let f = fx("fsg-skip", &[], driver_spec()).await;
            let file = mount(
                &f,
                json!({"fsType": "ext4"}),
                vec![Am::ReadWriteMany],
                None,
                None,
            )
            .await;
            assert_eq!(mode(&file), 0o600, "RWX is skipped");
            let f = fx("fsg-skip2", &[], driver_spec()).await;
            let file = mount(&f, json!({}), vec![Am::ReadWriteOnce], None, None).await;
            assert_eq!(mode(&file), 0o600, "no fsType is skipped");
        }

        /// `fsGroupPolicy: File` applies regardless of fsType / access mode
        /// (`csi_mounter.go:474-476`).
        #[tokio::test]
        async fn file_policy_applies_without_fs_type_or_rwo() {
            let f = fx(
                "fsg-file",
                &[],
                Some(json!({"attachRequired": false, "fsGroupPolicy": "File"})),
            )
            .await;
            let file = mount(&f, json!({}), vec![Am::ReadWriteMany], None, None).await;
            assert_eq!(mode(&file), 0o660);
        }

        /// `fsGroupPolicy: None` never applies (`csi_mounter.go:470`).
        #[tokio::test]
        async fn none_policy_never_applies() {
            let f = fx(
                "fsg-none",
                &[],
                Some(json!({"attachRequired": false, "fsGroupPolicy": "None"})),
            )
            .await;
            let file = mount(
                &f,
                json!({"fsType": "ext4"}),
                vec![Am::ReadWriteOnce],
                None,
                None,
            )
            .await;
            assert_eq!(mode(&file), 0o600);
        }

        /// A driver with VOLUME_MOUNT_GROUP gets the fsGroup in NodePublish and the
        /// kubelet leaves the tree alone (`csi_mounter.go:257-260`, `:333`).
        #[tokio::test]
        async fn volume_mount_group_driver_receives_fs_group_and_kubelet_skips() {
            let f = fx("fsg-vmg", &[Cap::VolumeMountGroup], driver_spec()).await;
            let file = mount(
                &f,
                json!({"fsType": "ext4"}),
                vec![Am::ReadWriteOnce],
                None,
                None,
            )
            .await;
            assert_eq!(mode(&file), 0o600, "the driver owns it, kubelet must not");
            let calls = f.fake.calls.lock().unwrap();
            match calls.publish[0]
                .volume_capability
                .as_ref()
                .unwrap()
                .access_type
                .as_ref()
                .unwrap()
            {
                AccessType::Mount(m) => assert_eq!(m.volume_mount_group, own_gid(&f).to_string()),
                other => panic!("expected mount, got {other:?}"),
            }
        }

        /// `fsGroupChangePolicy` reaches the ownership walk: `OnRootMismatch` with
        /// a conforming root leaves the children alone (`csi_mounter.go:339`).
        #[tokio::test]
        async fn fs_group_change_policy_is_passed_to_the_ownership_change() {
            let f = fx("fsg-orm", &[], driver_spec()).await;
            let file = mount(
                &f,
                json!({"fsType": "ext4"}),
                vec![Am::ReadWriteOnce],
                Some("OnRootMismatch"),
                Some(0o2770),
            )
            .await;
            assert_eq!(mode(&file), 0o600);
        }

        /// An empty `fsGroupPolicy` string is an error, as upstream
        /// (`csi_mounter.go:523-525`): transient, and nothing is published.
        #[tokio::test]
        async fn empty_fs_group_policy_is_a_transient_error() {
            let f = fx(
                "fsg-empty",
                &[],
                Some(json!({"attachRequired": false, "fsGroupPolicy": ""})),
            )
            .await;
            let p = pv(&f.driver, json!({"fsType": "ext4"}));
            let v = claim_volume();
            let spec = Spec {
                volume: &v,
                persistent_volume: Some(&p),
                read_only: false,
                migrated: false,
                inline_volume_spec_for_csi_migration: false,
            };
            let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
            let e = m.set_up().await.unwrap_err();
            assert!(e.to_string().contains("fsGroup policy"), "{e}");
            assert!(f.fake.calls.lock().unwrap().publish.is_empty());
        }
    }

    // ---- VerifyExhaustedResource (`csi_plugin_test.go:1470-1547`, `csi_plugin.go:192-232`) ----

    mod exhausted {
        use super::*;
        use rusternetes_common::resources::{CSIDriver, VolumeAttachment};
        use rusternetes_kubelet::volume_plugins::csi_client::proto::NodeGetInfoResponse;
        use rusternetes_kubelet::volume_plugins::csi_drivers_store::DriversStore;
        use rusternetes_kubelet::volume_plugins::nodeinfomanager::NodeInfoInstaller;
        use std::sync::Mutex;

        const EXHAUSTED: i32 = 8;
        const NOT_FOUND: i32 = 5;

        fn attachment(code: Option<i32>) -> VolumeAttachment {
            let status = match code {
                Some(c) => {
                    json!({"attached": false, "attachError": {"message": "e", "errorCode": c}})
                }
                None => json!({"attached": false, "attachError": {"message": "e"}}),
            };
            serde_json::from_value(json!({
                "metadata": {"name": "x"},
                "spec": {"attacher": "d", "nodeName": "n", "source": {}},
                "status": status
            }))
            .unwrap()
        }

        /// `TestIsResourceExhaustError` (`csi_plugin_test.go:1470-1547`).
        #[test]
        fn is_resource_exhaust_error_table() {
            assert!(!is_resource_exhaust_error(None), "nil attachment");
            let mut no_err = attachment(None);
            no_err.status.as_mut().unwrap().attach_error = None;
            assert!(!is_resource_exhaust_error(Some(&no_err)), "nil AttachError");
            assert!(
                !is_resource_exhaust_error(Some(&attachment(None))),
                "nil ErrorCode"
            );
            assert!(!is_resource_exhaust_error(Some(&attachment(Some(
                NOT_FOUND
            )))));
            assert!(is_resource_exhaust_error(Some(&attachment(Some(
                EXHAUSTED
            )))));
            let mut no_status = attachment(None);
            no_status.status = None;
            assert!(!is_resource_exhaust_error(Some(&no_status)), "nil status");
        }

        #[derive(Default)]
        struct Rec(Mutex<Vec<String>>);

        #[async_trait::async_trait]
        impl NodeInfoInstaller for Rec {
            async fn install_csi_driver(
                &self,
                _: &str,
                _: &str,
                _: i64,
                _: &HashMap<String, String>,
            ) -> Result<(), String> {
                Ok(())
            }
            async fn update_csi_driver(
                &self,
                n: &str,
                _: &str,
                _: i64,
                _: &HashMap<String, String>,
            ) -> Result<(), String> {
                self.0.lock().unwrap().push(n.into());
                Ok(())
            }
            async fn uninstall_csi_driver(&self, _: &str) -> Result<(), String> {
                Ok(())
            }
        }

        const DRIVER: &str = "exhausted.csi.example.com";
        const NODE: &str = "node-1";

        struct Fx {
            plugin: CsiPlugin,
            drivers: &'static DriversStore,
            rec: Rec,
            _dir: tempfile::TempDir,
        }

        /// `csi_driver`: `Some(period)` creates a CSIDriver with that
        /// `nodeAllocatableUpdatePeriodSeconds`, `None` no object at all.
        /// `code`: `Some(code)` creates the VolumeAttachment with that error code.
        async fn fx(csi_driver: Option<Option<i64>>, code: Option<Option<i32>>) -> Fx {
            let dir = tempfile::tempdir().unwrap();
            let sock = dir.path().join("csi.sock");
            let fake = FakeDriver::default();
            *fake.node_info.lock().unwrap() = Some(Ok(NodeGetInfoResponse {
                node_id: "n1".into(),
                max_volumes_per_node: 7,
                accessible_topology: None,
            }));
            std::mem::forget(serve(fake, &sock));
            let drivers: &'static DriversStore = Box::leak(Box::new(DriversStore::new()));
            drivers.set(
                DRIVER,
                Driver {
                    endpoint: sock.to_string_lossy().to_string(),
                    highest_supported_version: "1.0.0".into(),
                },
            );
            let st = Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new())));
            if let Some(period) = csi_driver {
                let mut d: CSIDriver = serde_json::from_value(json!({
                    "metadata": {"name": DRIVER}, "spec": {}
                }))
                .unwrap();
                d.spec.node_allocatable_update_period_seconds = period;
                st.create(&build_key("csidrivers", None, DRIVER), &d)
                    .await
                    .unwrap();
            }
            if let Some(code) = code {
                let name = get_attachment_name("vol-1", DRIVER, NODE);
                let mut a = attachment(code);
                a.metadata.name = name.clone();
                st.create(&build_key("volumeattachments", None, &name), &a)
                    .await
                    .unwrap();
            }
            Fx {
                plugin: CsiPlugin::new(host("/var/lib/rusternetes", Some(st))),
                drivers,
                rec: Rec::default(),
                _dir: dir,
            }
        }

        async fn verify(f: &Fx, spec_pv: &rusternetes_common::resources::PersistentVolume) -> bool {
            let vol = claim_volume();
            let spec = Spec {
                volume: &vol,
                persistent_volume: Some(spec_pv),
                read_only: false,
                migrated: false,
                inline_volume_spec_for_csi_migration: false,
            };
            f.plugin
                .verify_exhausted_resource(&spec, NODE, f.drivers, &f.rec)
                .await
        }

        #[tokio::test]
        async fn resource_exhausted_attachment_refreshes_csinode_and_returns_true() {
            let f = fx(Some(Some(10)), Some(Some(EXHAUSTED))).await;
            assert!(verify(&f, &pv(DRIVER, json!({}))).await);
            assert_eq!(*f.rec.0.lock().unwrap(), vec![DRIVER], "updateCSIDriver");
        }

        #[tokio::test]
        async fn other_attach_error_returns_false_without_refresh() {
            let f = fx(Some(Some(10)), Some(Some(NOT_FOUND))).await;
            assert!(!verify(&f, &pv(DRIVER, json!({}))).await);
            assert!(f.rec.0.lock().unwrap().is_empty());
        }

        /// `period == 0` (`csi_plugin.go:204-206`): no VolumeAttachment is consulted.
        #[tokio::test]
        async fn unset_update_period_returns_false() {
            let f = fx(Some(None), Some(Some(EXHAUSTED))).await;
            assert!(!verify(&f, &pv(DRIVER, json!({}))).await);
            assert!(f.rec.0.lock().unwrap().is_empty());
        }

        #[tokio::test]
        async fn missing_csidriver_returns_false() {
            let f = fx(None, Some(Some(EXHAUSTED))).await;
            assert!(!verify(&f, &pv(DRIVER, json!({}))).await);
        }

        #[tokio::test]
        async fn missing_attachment_returns_false() {
            let f = fx(Some(Some(10)), None).await;
            assert!(!verify(&f, &pv(DRIVER, json!({}))).await);
        }

        /// `spec.PersistentVolume == nil` (`csi_plugin.go:193-196`).
        #[tokio::test]
        async fn spec_without_pv_returns_false() {
            let f = fx(Some(Some(10)), Some(Some(EXHAUSTED))).await;
            let vol = claim_volume();
            let spec = Spec {
                volume: &vol,
                persistent_volume: None,
                read_only: false,
                migrated: false,
                inline_volume_spec_for_csi_migration: false,
            };
            assert!(
                !f.plugin
                    .verify_exhausted_resource(&spec, NODE, f.drivers, &f.rec)
                    .await
            );
        }
    }

    // ---- RequiresRemount / SupportsSELinuxContextMount lister reads -----------
    //
    // Upstream has no unit test for these two (grep RequiresRemount over
    // pkg/volume/csi/*_test.go is empty); the cases follow the code at
    // `csi_plugin.go:457-472` and `:637-660`.

    fn lister_host(
        drivers: &[serde_json::Value],
        synced: bool,
    ) -> Arc<rusternetes_kubelet::volume_plugins::KubeletVolumeHost> {
        let lister = rusternetes_kubelet::volume_plugins::csi_driver_lister::CsiDriverLister::new();
        for d in drivers {
            lister.insert(serde_json::from_value(d.clone()).unwrap());
        }
        if synced {
            lister.mark_synced();
        }
        Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                HashMap::new(),
            )
            .with_csi_driver_lister(lister),
        )
    }

    fn csi_driver_obj(name: &str, spec: serde_json::Value) -> serde_json::Value {
        json!({"apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
               "metadata": {"name": name}, "spec": spec})
    }

    #[test]
    fn requires_remount_follows_csidriver_requires_republish() {
        let h = lister_host(
            &[
                csi_driver_obj("rep.csi.example.com", json!({"requiresRepublish": true})),
                csi_driver_obj("norep.csi.example.com", json!({"requiresRepublish": false})),
            ],
            true,
        );
        let p = CsiPlugin::new(h);
        let v = claim_volume();
        let yes = pv("rep.csi.example.com", json!({}));
        let no = pv("norep.csi.example.com", json!({}));
        let unknown = pv("missing.csi.example.com", json!({}));
        let spec = |pv| Spec {
            volume: &v,
            persistent_volume: Some(pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(p.requires_remount(&spec(&yes)));
        assert!(!p.requires_remount(&spec(&no)));
        // getCSIDriver NotFound -> "Failed to mark ... as republish required" -> false.
        assert!(!p.requires_remount(&spec(&unknown)));
    }

    #[test]
    fn requires_remount_is_false_without_a_lister() {
        // `if p.csiDriverLister == nil { return false }` (csi_plugin.go:458).
        let p = plugin();
        let v = claim_volume();
        let pv = pv("rep.csi.example.com", json!({}));
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(!p.requires_remount(&spec));
    }

    #[test]
    fn supports_selinux_context_mount_follows_csidriver() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::SELinuxMountReadWriteOncePod,
            true,
        );
        let h = lister_host(
            &[
                csi_driver_obj("se.csi.example.com", json!({"seLinuxMount": true})),
                csi_driver_obj("nose.csi.example.com", json!({"seLinuxMount": false})),
            ],
            true,
        );
        let p = CsiPlugin::new(h);
        let v = claim_volume();
        let yes = pv("se.csi.example.com", json!({}));
        let no = pv("nose.csi.example.com", json!({}));
        let unknown = pv("missing.csi.example.com", json!({}));
        let spec = |pv| Spec {
            volume: &v,
            persistent_volume: Some(pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(p.supports_selinux_context_mount(&spec(&yes)).unwrap());
        assert!(!p.supports_selinux_context_mount(&spec(&no)).unwrap());
        // apierrors.IsNotFound -> (false, nil).
        assert!(!p.supports_selinux_context_mount(&spec(&unknown)).unwrap());
    }

    #[test]
    fn supports_selinux_context_mount_errors_when_lister_missing() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::SELinuxMountReadWriteOncePod,
            true,
        );
        let v = claim_volume();
        let pv = pv("se.csi.example.com", json!({}));
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        // getCSIDriver: "CSIDriver lister does not exist". (An unsynced cache
        // blocks in WaitForCacheSync rather than erroring; see the `waits_for`
        // tests below.)
        assert!(plugin().supports_selinux_context_mount(&spec).is_err());
    }

    // ---- CanAttach (csi_plugin.go:675-700, skipAttach :858-871) ----------------
    //
    // Cases follow `TestPluginCanAttach` (`csi_plugin_test.go:861-911`) plus the
    // `skipAttach` arms upstream's code takes (CSIDriver absent / attachRequired
    // unset / lister unsynced).

    fn can_attach_of(
        p: &CsiPlugin,
        vol: &Volume,
        pv: Option<&rusternetes_common::resources::PersistentVolume>,
    ) -> Result<bool> {
        p.can_attach(&Spec {
            volume: vol,
            persistent_volume: pv,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        })
    }

    #[test]
    fn can_attach_ephemeral_inline_is_false() {
        // "non-attachable inline": ephemeral mode short-circuits (`:681-684`).
        let p = CsiPlugin::new(lister_host(
            &[csi_driver_obj(
                "inline.csi.example.com",
                json!({"attachRequired": true}),
            )],
            true,
        ));
        let inline: Volume = serde_json::from_value(
            json!({"name": "v", "csi": {"driver": "inline.csi.example.com"}}),
        )
        .unwrap();
        assert!(!can_attach_of(&p, &inline, None).unwrap());
    }

    #[test]
    fn can_attach_pv_follows_csidriver_attach_required() {
        let p = CsiPlugin::new(lister_host(
            &[
                csi_driver_obj("attach.csi.example.com", json!({"attachRequired": true})),
                csi_driver_obj("noattach.csi.example.com", json!({"attachRequired": false})),
                csi_driver_obj("unset.csi.example.com", json!({})),
            ],
            true,
        ));
        let v = claim_volume();
        // "attachable PV"
        assert!(can_attach_of(&p, &v, Some(&pv("attach.csi.example.com", json!({})))).unwrap());
        // skipAttach: AttachRequired == false.
        assert!(!can_attach_of(&p, &v, Some(&pv("noattach.csi.example.com", json!({})))).unwrap());
        // AttachRequired nil does not skip.
        assert!(can_attach_of(&p, &v, Some(&pv("unset.csi.example.com", json!({})))).unwrap());
        // "Don't skip attach if CSIDriver does not exist" (`:861-864`).
        assert!(can_attach_of(&p, &v, Some(&pv("missing.csi.example.com", json!({})))).unwrap());
    }

    #[test]
    fn can_attach_incomplete_spec_is_an_error() {
        // "incomplete spec": getVolumeLifecycleMode -> getSourceFromSpec fails
        // with "volume source not found in volume.Spec" (`csi_util.go:161`).
        let p = CsiPlugin::new(lister_host(&[], true));
        let v = claim_volume();
        let err = can_attach_of(&p, &v, None).unwrap_err();
        assert!(
            err.to_string()
                .contains("volume source not found in volume.Spec"),
            "{err}"
        );
    }

    #[test]
    fn registry_finds_csi_as_attachable_only_when_can_attach() {
        // `FindAttachablePluginBySpec` (`plugins.go:805-818`).
        let h = lister_host(
            &[csi_driver_obj(
                "noattach.csi.example.com",
                json!({"attachRequired": false}),
            )],
            true,
        );
        let mgr = rusternetes_kubelet::volume_plugins::VolumePluginMgr::new(vec![Box::new(
            CsiPlugin::new(h),
        )]);
        let v = claim_volume();
        let yes = pv("other.csi.example.com", json!({}));
        let no = pv("noattach.csi.example.com", json!({}));
        let spec = |pv| Spec {
            volume: &v,
            persistent_volume: Some(pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(mgr.find_attachable_plugin_by_spec(&spec(&yes)).is_some());
        assert!(mgr.find_attachable_plugin_by_spec(&spec(&no)).is_none());
    }

    // ---- GetDeviceMountRefs (csi_plugin.go:721-727) ----------------------------
    //
    // `m := p.host.GetMounter(); return m.GetMountRefs(deviceMountPath)`. Upstream
    // has no csi_plugin_test.go case for it; the delegation is the whole method.

    #[test]
    fn get_device_mount_refs_delegates_to_the_host_mounters_get_mount_refs() {
        use rusternetes_kubelet::volume_plugins::plugin::DeviceMountableVolumePlugin;
        let p = plugin();
        // A path that does not exist has no refs (`GetMountRefs`: `!pathExists`).
        assert!(p
            .get_device_mount_refs("/definitely/not/here/globalmount")
            .unwrap()
            .is_empty());
        for path in [
            "/",
            "/proc",
            "/dev",
            "/sys",
            "/tmp",
            "/etc/hosts",
            "/etc/resolv.conf",
        ] {
            let want = rusternetes_kubelet::volume_plugins::util::mount_utils::get_mount_refs(path)
                .map_err(|e| e.to_string());
            let got = p.get_device_mount_refs(path).map_err(|e| e.to_string());
            assert_eq!(got, want, "{path}");
        }
    }

    // ---- getCSIDriver waits for the CSIDriverLister cache (csi_plugin.go:873-890)
    //
    // Upstream's `getCSIDriver` calls `kletHost.WaitForCacheSync()`
    // (`volume_host.go:175-188`), which blocks in `cache.WaitForCacheSync(
    // wait.NeverStop, csiDriversSynced)` until the informer syncs; it does not
    // error while the cache is still filling.

    #[test]
    fn get_csi_driver_waits_for_cache_sync_instead_of_erroring() {
        let lister = rusternetes_kubelet::volume_plugins::csi_driver_lister::CsiDriverLister::new();
        lister.insert(
            serde_json::from_value(csi_driver_obj(
                "late.csi.example.com",
                json!({"attachRequired": false}),
            ))
            .unwrap(),
        );
        let host = Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                HashMap::new(),
            )
            .with_csi_driver_lister(lister.clone()),
        );
        let p = CsiPlugin::new(host);
        let l = lister.clone();
        let t = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            l.mark_synced();
        });
        let d = p
            .get_csi_driver("late.csi.example.com")
            .expect("must wait for the sync, not error")
            .expect("driver present once synced");
        assert_eq!(d.metadata.name, "late.csi.example.com");
        assert!(lister.has_synced());
        t.join().unwrap();
    }

    #[test]
    fn can_attach_waits_for_cache_sync() {
        // skipAttach -> getCSIDriver blocks until synced, then reads attachRequired.
        let lister = rusternetes_kubelet::volume_plugins::csi_driver_lister::CsiDriverLister::new();
        lister.insert(
            serde_json::from_value(csi_driver_obj(
                "noattach.csi.example.com",
                json!({"attachRequired": false}),
            ))
            .unwrap(),
        );
        let host = Arc::new(
            rusternetes_kubelet::volume_plugins::KubeletVolumeHost::new_for_tests(
                "/var/lib/rusternetes".to_string(),
                None,
                rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
                HashMap::new(),
            )
            .with_csi_driver_lister(lister.clone()),
        );
        let p = CsiPlugin::new(host);
        let l = lister.clone();
        let t = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            l.mark_synced();
        });
        let v = claim_volume();
        let r = can_attach_of(&p, &v, Some(&pv("noattach.csi.example.com", json!({}))));
        assert!(!r.unwrap());
        t.join().unwrap();
    }

    // ---- SetUp: SELinux mount context (#2443) -----------------------------------

    /// Publish mount flags + persisted `seLinuxMountContext` of one `SetUp` with
    /// `MounterArgs.SELinuxLabel = label`.
    async fn selinux_set_up(
        name: &str,
        csi_driver: Option<serde_json::Value>,
        gate: bool,
        label: &str,
    ) -> (Vec<String>, Option<String>) {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::SELinuxMountReadWriteOncePod,
            gate,
        );
        let f = fx(name, &[], csi_driver).await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let m = f.plugin.new_mounter(&spec, &pod()).await.unwrap();
        let args = rusternetes_kubelet::volume_plugins::MounterArgs {
            selinux_label: label.to_string(),
            ..Default::default()
        };
        m.set_up_with(&args).await.unwrap();
        let calls = f.fake.calls.lock().unwrap();
        let flags = match calls.publish[0]
            .volume_capability
            .as_ref()
            .unwrap()
            .access_type
            .as_ref()
            .unwrap()
        {
            AccessType::Mount(mnt) => mnt.mount_flags.clone(),
            other => panic!("expected mount, got {other:?}"),
        };
        let vol_dir = std::path::Path::new(&m.get_path())
            .parent()
            .unwrap()
            .to_path_buf();
        let data: HashMap<String, String> =
            serde_json::from_slice(&std::fs::read(vol_dir.join("vol_data.json")).unwrap()).unwrap();
        (flags, data.get("seLinuxMountContext").cloned())
    }

    /// `TestMounterSetUpSimple` "should include SELinux mount options, if
    /// feature-gate is enabled and driver supports it" (`csi_mounter_test.go:182`),
    /// and `csi_mounter.go:287` persists `seLinuxMountContext`.
    #[tokio::test]
    #[serial_test::serial]
    async fn set_up_adds_selinux_mount_option_when_driver_supports_it() {
        let (flags, saved) = selinux_set_up(
            "se-yes",
            Some(json!({"attachRequired": false, "seLinuxMount": true})),
            true,
            "s0,c0",
        )
        .await;
        assert!(
            flags.contains(&"context=\"s0,c0\"".to_string()),
            "{flags:?}"
        );
        assert_eq!(saved.as_deref(), Some("s0,c0"));
    }

    /// "...feature gate is enabled but driver does not support it".
    #[tokio::test]
    #[serial_test::serial]
    async fn set_up_omits_selinux_mount_option_when_driver_does_not_support_it() {
        let (flags, saved) = selinux_set_up(
            "se-no",
            Some(json!({"attachRequired": false, "seLinuxMount": false})),
            true,
            "s0,c0",
        )
        .await;
        assert!(
            !flags.iter().any(|f| f.starts_with("context=")),
            "{flags:?}"
        );
        assert_eq!(saved, None);
    }

    /// "...driver supports it, but Pod does not have it": never `-o context=""`.
    #[tokio::test]
    #[serial_test::serial]
    async fn set_up_omits_selinux_mount_option_without_a_pod_label() {
        let (flags, saved) = selinux_set_up(
            "se-nolabel",
            Some(json!({"attachRequired": false, "seLinuxMount": true})),
            true,
            "",
        )
        .await;
        assert!(
            !flags.iter().any(|f| f.starts_with("context=")),
            "{flags:?}"
        );
        assert_eq!(saved, None);
    }

    /// Gate off: `AddSELinuxMountOption` and the persisted context are skipped.
    #[tokio::test]
    #[serial_test::serial]
    async fn set_up_omits_selinux_mount_option_when_gate_is_off() {
        let (flags, saved) = selinux_set_up(
            "se-gateoff",
            Some(json!({"attachRequired": false, "seLinuxMount": true})),
            false,
            "s0,c0",
        )
        .await;
        assert!(
            !flags.iter().any(|f| f.starts_with("context=")),
            "{flags:?}"
        );
        assert_eq!(saved, None);
    }

    // ---- ConstructVolumeSpec / MountDevice: SELinux mount context (#3882) ------

    const SELINUX_CTX: &str = "system_u:object_r:container_file_t:s0:c314,c894";

    /// `TestPluginConstructVolumeSpec` SELinux rows (`csi_plugin_test.go:349-366`):
    /// the persisted `seLinuxMountContext` comes back only with the gate on.
    #[tokio::test]
    #[serial_test::serial]
    async fn construct_volume_spec_returns_the_selinux_context_only_with_the_gate() {
        for (gate, expected) in [(true, SELINUX_CTX), (false, "")] {
            let _g = rusternetes_common::feature_gates::with_feature(
                rusternetes_common::feature_gates::Feature::SELinuxMountReadWriteOncePod,
                gate,
            );
            let f = fx("construct-selinux", &[], None).await;
            let dir = std::path::Path::new(&f.root).join(format!("vol-{gate}"));
            std::fs::create_dir_all(&dir).unwrap();
            let data = HashMap::from([
                ("specVolID".to_string(), "spec3".to_string()),
                ("driverName".to_string(), f.driver.clone()),
                ("volumeHandle".to_string(), "handle3".to_string()),
                ("seLinuxMountContext".to_string(), SELINUX_CTX.to_string()),
            ]);
            save_volume_data(&dir, &data).unwrap();
            let rec = f
                .plugin
                .construct_volume_spec("spec3", dir.to_str().unwrap())
                .unwrap();
            assert_eq!(rec.selinux_mount_context, expected, "gate={gate}");
        }
    }

    /// `MountDevice` SELinux (`csi_attacher.go:331-350`): returns the stage mount
    /// flags and whether the device `vol_data.json` persisted the context.
    async fn selinux_mount_device(
        name: &str,
        driver_spec: serde_json::Value,
        label: &str,
    ) -> (Vec<String>, Option<String>) {
        let f = fx(name, &[Cap::StageUnstageVolume], Some(driver_spec)).await;
        let p = pv(&f.driver, json!({}));
        let v = claim_volume();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&p),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        let dm = f.plugin.new_device_mounter().unwrap();
        let path = dm.get_device_mount_path(&spec).unwrap();
        let args = DeviceMounterArgs {
            selinux_label: label.into(),
            node_name: "node-1".into(),
            ..Default::default()
        };
        dm.mount_device(&spec, "", &path, &args).await.unwrap();
        let flags = {
            let calls = f.fake.calls.lock().unwrap();
            match calls.stage[0]
                .volume_capability
                .as_ref()
                .unwrap()
                .access_type
                .as_ref()
                .unwrap()
            {
                AccessType::Mount(m) => m.mount_flags.clone(),
                other => panic!("expected mount, got {other:?}"),
            }
        };
        let data = load_volume_data(std::path::Path::new(&path).parent().unwrap()).unwrap();
        (flags, data.get("seLinuxMountContext").cloned())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn mount_device_stages_with_the_selinux_context_option() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::SELinuxMountReadWriteOncePod,
            true,
        );
        // Driver supports `-o context=` and the pod has a label.
        let (flags, ctx) = selinux_mount_device(
            "stage-se-yes",
            json!({"attachRequired": false, "seLinuxMount": true}),
            SELINUX_CTX,
        )
        .await;
        assert_eq!(
            flags,
            vec!["foo=bar".to_string(), format!("context=\"{SELINUX_CTX}\"")]
        );
        assert_eq!(ctx.as_deref(), Some(SELINUX_CTX));

        // Driver does not support it.
        let (flags, ctx) = selinux_mount_device(
            "stage-se-no",
            json!({"attachRequired": false, "seLinuxMount": false}),
            SELINUX_CTX,
        )
        .await;
        assert_eq!(flags, vec!["foo=bar".to_string()]);
        assert_eq!(ctx, None);

        // No pod label.
        let (flags, ctx) = selinux_mount_device(
            "stage-se-nolabel",
            json!({"attachRequired": false, "seLinuxMount": true}),
            "",
        )
        .await;
        assert_eq!(flags, vec!["foo=bar".to_string()]);
        assert_eq!(ctx, None);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn mount_device_ignores_the_selinux_label_with_the_gate_off() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::SELinuxMountReadWriteOncePod,
            false,
        );
        let (flags, ctx) = selinux_mount_device(
            "stage-se-gateoff",
            json!({"attachRequired": false, "seLinuxMount": true}),
            SELINUX_CTX,
        )
        .await;
        assert_eq!(flags, vec!["foo=bar".to_string()]);
        assert_eq!(ctx, None);
    }
}
