//! Unit tests moved from `src/volume_plugins/util/operation_generator.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Error};
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_common::resources::volume::{PersistentVolume, PersistentVolumeClaim};
    pub use rusternetes_common::resources::EventType;
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_kubelet::events::{
        FILE_SYSTEM_RESIZE_FAILED, FILE_SYSTEM_RESIZE_SUCCESS, VOLUME_RESIZE_FAILED,
    };
    pub use rusternetes_kubelet::volume_manager::cache::actual_state_of_world::ActualStateOfWorld;
    pub use rusternetes_kubelet::volume_manager::cache::desired_state_of_world::{
        desired_size_limit, effective_selinux_mount_label, get_selinux_label,
    };
    pub use rusternetes_kubelet::volume_plugins::plugin::{
        DeviceMountableVolumePlugin, DeviceMounter, DeviceMounterArgs, DeviceUnmounter,
        NodeExpandableVolumePlugin, NodeResizeOptions, Spec,
    };
    pub use rusternetes_kubelet::volume_plugins::registry::VolumePluginMgr;
    pub use rusternetes_kubelet::volume_plugins::util::hostutil::HostUtils;
    pub use rusternetes_kubelet::volume_plugins::util::node_expander::{
        pvc_object_reference, record_event, NodeExpander, NodeResizeOperationOpts,
    };
    pub use rusternetes_kubelet::volume_plugins::util::operation_executor::{
        AttachedVolume, VolumeToMount,
    };
    pub use rusternetes_kubelet::volume_plugins::util::resize_util::{self, RESOURCE_STORAGE};
    pub use rusternetes_kubelet::volume_plugins::util::types::{
        is_failed_precondition_error, is_operation_not_supported_error, OperationContext,
    };
    pub use rusternetes_kubelet::volume_plugins::util::{check_device_mount_refs, fs_group_from};
    pub use rusternetes_storage::{build_key, EventRecorder, Storage};
    pub use std::collections::HashMap;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::{PersistentVolume, Volume};
    use rusternetes_kubelet::volume_plugins::csi::CsiPlugin;
    use rusternetes_kubelet::volume_plugins::util::operation_generator::*;
    use rusternetes_kubelet::volume_plugins::KubeletVolumeHost;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn mgr() -> VolumePluginMgr {
        let host = Arc::new(KubeletVolumeHost::new_for_tests(
            "/var/lib/rusternetes".to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        ));
        VolumePluginMgr::new(vec![Box::new(CsiPlugin::new(host))])
    }

    fn pv() -> PersistentVolume {
        serde_json::from_value(json!({
            "metadata": {"name": "pv1"},
            "spec": {"csi": {"driver": "d", "volumeHandle": "h"}}
        }))
        .unwrap()
    }

    /// A CSI PersistentVolume is expandable on the node.
    #[test]
    fn csi_persistent_volume_supports_node_expansion() {
        let m = mgr();
        let v: Volume = serde_json::from_value(
            json!({"name": "v", "persistentVolumeClaim": {"claimName": "c"}}),
        )
        .unwrap();
        let pv = pv();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(check_if_supports_node_expansion(&m, &spec).is_some());
    }

    /// `checkIfSupportsNodeExpansion`'s leading guard
    /// (`operation_generator.go:1997-2001`): a migrated inline volume is not
    /// resizable even though its translated spec carries a CSI PV.
    #[test]
    fn migrated_inline_volume_does_not_support_node_expansion() {
        let m = mgr();
        let v: Volume = serde_json::from_value(
            json!({"name": "v", "persistentVolumeClaim": {"claimName": "c"}}),
        )
        .unwrap();
        let pv = pv();
        let mut spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: false,
            migrated: true,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(check_if_supports_node_expansion(&m, &spec).is_some());
        spec.inline_volume_spec_for_csi_migration = true;
        assert!(check_if_supports_node_expansion(&m, &spec).is_none());
    }

    /// An inline CSI volume has no PersistentVolume, so it is not resized
    /// (`VolumeSpec.PersistentVolume != nil`).
    #[test]
    fn inline_csi_volume_does_not_support_node_expansion() {
        let m = mgr();
        let v: Volume =
            serde_json::from_value(json!({"name": "v", "csi": {"driver": "d"}})).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(check_if_supports_node_expansion(&m, &spec).is_none());
    }

    /// No plugin matches: the lookup error is swallowed, not propagated.
    #[test]
    fn unmatched_spec_does_not_support_node_expansion() {
        let m = mgr();
        let v: Volume = serde_json::from_value(json!({"name": "v", "emptyDir": {}})).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        assert!(check_if_supports_node_expansion(&m, &spec).is_none());
    }

    // ---- node expansion flow (`nodeExpandVolume`, `expandVolumeDuringMount`) ----

    use crate::volume_plugins_unit::volume_plugins__util__node_expander::tests::{
        q, test_asow, test_pv, test_pvc, test_vmt, FakeExpandPlugin,
    };
    use rusternetes_common::resources::volume::PersistentVolumeClaim;
    use rusternetes_storage::StorageBackend;
    use std::sync::atomic::Ordering;

    struct Fixture {
        storage: StorageBackend,
        recorder: EventRecorder<StorageBackend>,
        mgr: VolumePluginMgr,
        plugin: FakeExpandPlugin,
    }

    impl Fixture {
        async fn new(pvc: &PersistentVolumeClaim, plugin: FakeExpandPlugin) -> Self {
            let storage = StorageBackend::new_memory();
            storage
                .create(
                    &build_key("persistentvolumeclaims", Some("ns"), "test-vol0"),
                    pvc,
                )
                .await
                .unwrap();
            Self {
                storage,
                recorder: EventRecorder::new(Arc::new(StorageBackend::new_memory())),
                mgr: VolumePluginMgr::new(vec![Box::new(plugin.sharing())]),
                plugin,
            }
        }

        fn og(&self) -> OperationGenerator<'_, StorageBackend> {
            OperationGenerator {
                volume_plugin_mgr: &self.mgr,
                kube_client: &self.storage,
                recorder: &self.recorder,
            }
        }

        async fn pvc(&self) -> PersistentVolumeClaim {
            self.storage
                .get(&build_key(
                    "persistentvolumeclaims",
                    Some("ns"),
                    "test-vol0",
                ))
                .await
                .unwrap()
        }

        fn called(&self) -> bool {
            self.plugin.called.load(Ordering::SeqCst)
        }
    }

    fn opts<'a>(spec: &'a Spec<'a>, old: &str, new: &str) -> NodeResizeOptions<'a> {
        NodeResizeOptions {
            volume_spec: spec,
            device_path: String::new(),
            device_mount_path: String::new(),
            device_stage_path: String::new(),
            new_size: q(new),
            old_size: q(old),
        }
    }

    /// `nodeExpandVolume`: `NewSize <= OldSize` is a no-op that reports the
    /// old size (`operation_generator.go:2023`, `:2062`).
    #[tokio::test]
    async fn node_expand_volume_without_growth_is_a_noop() {
        let pvc = test_pvc("2G", "2G", "2G", "", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();
        let (finished, size, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "2G", "2G"))
            .await;
        assert!(finished && err.is_none());
        assert_eq!(size.cmp_value(&q("2G")), std::cmp::Ordering::Equal);
        assert!(!f.called());
    }

    /// With a resize controller that populates `allocatedResources`, the
    /// recovery `nodeExpander` runs and the PVC capacity is raised
    /// (`:2048-2055`).
    #[tokio::test]
    async fn node_expand_volume_uses_recovery_expander_and_updates_pvc() {
        let pvc = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();
        let (finished, size, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "1G", "2G"))
            .await;
        assert!(finished && err.is_none(), "{err:?}");
        assert_eq!(size.cmp_value(&q("2G")), std::cmp::Ordering::Equal);
        assert!(f.called());
        let status = f.pvc().await.status.unwrap();
        assert_eq!(status.capacity.unwrap()["storage"], "2G");
        assert!(status.allocated_resource_statuses.is_none());
    }

    /// An older resize controller sets neither `allocatedResources` nor a
    /// resize status, so `checkForRecoveryFromExpansion` is false and the
    /// legacy path runs: it expands, then `MarkFSResizeFinished`
    /// (`:2056-2059`, `:2093-2135`).
    #[tokio::test]
    async fn node_expand_volume_falls_back_to_legacy_path_for_old_controller() {
        let pvc = test_pvc("2G", "1G", "", "", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        assert!(!f
            .og()
            .check_for_recovery_from_expansion(&pvc, &test_vmt(&test_pv("2G"), "2G")));
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();
        let (finished, size, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "1G", "2G"))
            .await;
        assert!(finished && err.is_none(), "{err:?}");
        assert_eq!(size.cmp_value(&q("2G")), std::cmp::Ordering::Equal);
        assert!(f.called());
        assert_eq!(
            f.pvc().await.status.unwrap().capacity.unwrap()["storage"],
            "2G"
        );
    }

    /// Legacy path: a `FailedPrecondition` marks the in-use error and does not
    /// block the mount (`:2112-2120`); any other error is returned.
    #[tokio::test]
    async fn legacy_failed_precondition_marks_in_use_and_finishes() {
        use rusternetes_kubelet::volume_plugins::util::types::VolumeOperationError;
        fn fp() -> anyhow::Error {
            VolumeOperationError::FailedPrecondition("in use".into()).into()
        }
        fn other() -> anyhow::Error {
            anyhow::anyhow!("boom")
        }
        let pvc = test_pvc("2G", "1G", "", "", false, false);
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();

        let f = Fixture::new(&pvc, FakeExpandPlugin::failing(fp)).await;
        let (finished, _, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "1G", "2G"))
            .await;
        assert!(finished && err.is_none());
        // PVC untouched: the resize did not happen.
        assert_eq!(
            f.pvc().await.status.unwrap().capacity.unwrap()["storage"],
            "1G"
        );

        let f = Fixture::new(&pvc, FakeExpandPlugin::failing(other)).await;
        let (finished, _, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "1G", "2G"))
            .await;
        assert!(!finished && err.is_some());
    }

    /// A read-only mount cannot be resized: finished, plugin not called
    /// (`:2032-2038`).
    #[tokio::test]
    async fn node_expand_volume_skips_read_only_mounts() {
        let pvc = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        let pv = test_pv("2G");
        let mut vmt = test_vmt(&pv, "2G");
        let mut owned = vmt.volume_spec.as_spec().to_owned_spec();
        // `NewSpecFromPersistentVolume(pv, true)`
        // (`desired_state_of_world_populator.go:588`).
        owned.read_only = true;
        vmt.volume_spec = Arc::new(owned);
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();
        let (finished, size, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "1G", "2G"))
            .await;
        assert!(finished && err.is_none());
        assert_eq!(size.cmp_value(&q("1G")), std::cmp::Ordering::Equal);
        assert!(!f.called());
    }

    /// A PVC that cannot be fetched is an error, not a silent skip (`:2026-2030`).
    #[tokio::test]
    async fn node_expand_volume_errors_when_pvc_is_missing() {
        let pvc = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        f.storage
            .delete(&build_key(
                "persistentvolumeclaims",
                Some("ns"),
                "test-vol0",
            ))
            .await
            .unwrap();
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();
        let (finished, _, err) = f
            .og()
            .node_expand_volume(&vmt, &asow, opts(&spec, "1G", "2G"))
            .await;
        assert!(!finished);
        assert!(err
            .unwrap()
            .to_string()
            .starts_with("mountVolume.NodeExpandVolume get PVC failed"));
    }

    /// `expandVolumeDuringMount`: nothing to do once `pvc.status.capacity`
    /// has reached `pv.spec.capacity` (`:1961`, `:1993`); otherwise it expands.
    #[tokio::test]
    async fn expand_volume_during_mount_expands_only_when_pvc_is_behind() {
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let spec = vmt.volume_spec.as_spec();

        let current = test_pvc("2G", "2G", "", "", false, false);
        let f = Fixture::new(&current, FakeExpandPlugin::ok()).await;
        let (finished, err) = f
            .og()
            .expand_volume_during_mount(&vmt, &asow, opts(&spec, "0", "0"))
            .await;
        assert!(finished && err.is_none());
        assert!(!f.called());

        let behind = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&behind, FakeExpandPlugin::ok()).await;
        let (finished, err) = f
            .og()
            .expand_volume_during_mount(&vmt, &asow, opts(&spec, "0", "0"))
            .await;
        assert!(finished && err.is_none(), "{err:?}");
        assert!(f.called());
        assert_eq!(
            f.pvc().await.status.unwrap().capacity.unwrap()["storage"],
            "2G"
        );
    }

    // ---- `GenerateExpandInUseVolumeFunc` (`operation_generator.go:1815-1947`) ----

    /// An actual state of world that knows the fixture's plugin, with the
    /// volume attached.
    fn attached_asow(f: &Fixture, vmt: &VolumeToMount) -> ActualStateOfWorld {
        let asow = ActualStateOfWorld::new(
            "node-1",
            Arc::new(VolumePluginMgr::new(vec![Box::new(f.plugin.sharing())])),
        );
        asow.mark_volume_as_attached(
            Some(&vmt.volume_name),
            Arc::clone(&vmt.volume_spec),
            "",
            "/dev/fake",
        )
        .unwrap();
        asow
    }

    /// The closure refuses an unset current or desired size
    /// (`:1832-1836`) before touching any plugin.
    #[tokio::test]
    async fn expand_in_use_requires_current_and_new_size() {
        let pvc = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        let pv = test_pv("2G");
        let asow = test_asow();
        let mut vmt = test_vmt(&pv, "2G");
        let og = f.og();
        let op = og
            .generate_expand_in_use_volume_func(&vmt, &asow, q("0"))
            .unwrap();
        assert_eq!(op.operation_name, "volume_fs_resize");
        let ctx = op.run().await;
        let err = ctx.detailed_err.expect("zero current size is an error");
        assert!(err
            .to_string()
            .contains("current or new size of the volume is not set"));
        assert!(!f.called());

        vmt.desired_persistent_volume_size = None;
        let op = og
            .generate_expand_in_use_volume_func(&vmt, &asow, q("1G"))
            .unwrap();
        assert!(op.run().await.event_err.is_some());
        assert!(!f.called());
    }

    /// Online expansion: `nodeExpandVolume` runs, then `MarkVolumeAsResized`
    /// records the new size in the actual state of world (`:1925-1947`).
    #[tokio::test]
    async fn expand_in_use_expands_and_marks_volume_resized() {
        let pvc = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = attached_asow(&f, &vmt);
        let og = f.og();
        let op = og
            .generate_expand_in_use_volume_func(&vmt, &asow, q("1G"))
            .unwrap();
        let ctx = op.run().await;
        assert!(
            ctx.event_err.is_none() && ctx.detailed_err.is_none(),
            "{:?}",
            ctx.detailed_err
        );
        assert!(f.called());
        assert_eq!(
            f.pvc().await.status.unwrap().capacity.unwrap()["storage"],
            "2G"
        );
    }

    /// A volume the ASW does not track cannot be marked resized: the
    /// operation reports `MarkVolumeAsResized failed` for the caller to retry
    /// (`:1938-1944`).
    #[tokio::test]
    async fn expand_in_use_reports_unmarkable_volume() {
        let pvc = test_pvc("2G", "1G", "2G", "NodeResizePending", false, false);
        let f = Fixture::new(&pvc, FakeExpandPlugin::ok()).await;
        let pv = test_pv("2G");
        let asow = test_asow();
        let vmt = test_vmt(&pv, "2G");
        let og = f.og();
        let ctx = og
            .generate_expand_in_use_volume_func(&vmt, &asow, q("1G"))
            .unwrap()
            .run()
            .await;
        assert!(ctx
            .detailed_err
            .unwrap()
            .to_string()
            .contains("NodeExpandVolume.MarkVolumeAsResized failed"));
    }

    /// A plugin failure surfaces as `NodeExpandVolume.NodeExpandVolume failed`
    /// and the event recorder func emits a `VolumeResizeFailed` warning on
    /// the pod (`:1909-1913`).
    #[tokio::test]
    async fn expand_in_use_failure_is_reported_and_recorded() {
        let pvc = test_pvc("2G", "1G", "", "", false, false);
        let plugin = FakeExpandPlugin::failing(|| anyhow!("boom"));
        let f = Fixture::new(&pvc, plugin).await;
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = attached_asow(&f, &vmt);
        let og = f.og();
        let op = og
            .generate_expand_in_use_volume_func(&vmt, &asow, q("1G"))
            .unwrap();
        let ctx = op.run().await;
        let event_err = ctx.event_err.expect("plugin failure is an error");
        assert!(event_err
            .to_string()
            .contains("NodeExpandVolume.NodeExpandVolume failed"));
        op.record_event_on_error(Some(&event_err)).await;
        op.record_event_on_error(None).await;
    }

    /// `FindPluginBySpec` failing aborts generation (`:1818-1821`).
    #[test]
    fn expand_in_use_generation_fails_without_a_plugin() {
        let storage = StorageBackend::new_memory();
        let recorder = EventRecorder::new(Arc::new(StorageBackend::new_memory()));
        let empty = VolumePluginMgr::new(vec![]);
        let og = OperationGenerator {
            volume_plugin_mgr: &empty,
            kube_client: &storage,
            recorder: &recorder,
        };
        let pv = test_pv("2G");
        let vmt = test_vmt(&pv, "2G");
        let asow = test_asow();
        let err = og
            .generate_expand_in_use_volume_func(&vmt, &asow, q("1G"))
            .err()
            .expect("no plugin");
        assert!(err
            .to_string()
            .contains("NodeExpandVolume.FindPluginBySpec failed"));
    }

    // ---- `MounterArgs` population (`operation_generator.go:582-589`, #2900) ----

    use async_trait::async_trait;
    use rusternetes_common::feature_gates::{with_feature, Feature};
    use rusternetes_kubelet::volume_plugins::plugin::{Mounter, Unmounter, VolumePlugin};
    use rusternetes_kubelet::volume_plugins::util::selinux::FakeSELinuxLabelTranslator;

    fn pod_json(v: serde_json::Value) -> Pod {
        serde_json::from_value(v).unwrap()
    }

    fn empty_dir_mgr() -> VolumePluginMgr {
        let host = Arc::new(KubeletVolumeHost::new_for_tests(
            "/var/lib/rusternetes".to_string(),
            None,
            rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
            HashMap::new(),
        ));
        VolumePluginMgr::new(vec![Box::new(
            rusternetes_kubelet::volume_plugins::empty_dir::EmptyDirPlugin::new(host),
        )])
    }

    fn args_of(
        pod: &Pod,
        vol: serde_json::Value,
    ) -> rusternetes_kubelet::volume_plugins::MounterArgs {
        let v: Volume = serde_json::from_value(vol).unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        mounter_args_for(&empty_dir_mgr(), &spec, pod, &FakeSELinuxLabelTranslator).unwrap()
    }

    fn pod_with_limit(limit: Option<&str>) -> Pod {
        let mut c = json!({"name": "c", "image": "i"});
        if let Some(l) = limit {
            c["resources"] = json!({"limits": {"ephemeral-storage": l}});
        }
        pod_json(json!({
            "metadata": {"name": "p", "namespace": "ns", "uid": "u"},
            "spec": {"containers": [c]}
        }))
    }

    /// `AddPodToVolume` (`desired_state_of_world.go:297-322`): the emptyDir
    /// `sizeLimit` is the desired size limit.
    #[test]
    fn empty_dir_size_limit_is_the_desired_size() {
        let a = args_of(
            &pod_with_limit(None),
            json!({"name": "v", "emptyDir": {"sizeLimit": "1Gi"}}),
        );
        assert_eq!(a.desired_size, Some(1 << 30));
    }

    /// The smaller of the pod's ephemeral-storage limit and the sizeLimit wins.
    #[test]
    fn pod_ephemeral_storage_limit_wins_when_smaller() {
        let a = args_of(
            &pod_with_limit(Some("500Mi")),
            json!({"name": "v", "emptyDir": {"sizeLimit": "1Gi"}}),
        );
        assert_eq!(a.desired_size, Some(500 << 20));
        let a = args_of(
            &pod_with_limit(Some("2Gi")),
            json!({"name": "v", "emptyDir": {"sizeLimit": "1Gi"}}),
        );
        assert_eq!(a.desired_size, Some(1 << 30));
    }

    /// Not a local ephemeral volume: no desired size limit (`:315-316`).
    #[test]
    fn non_local_ephemeral_volume_has_no_desired_size() {
        let a = args_of(
            &pod_with_limit(Some("500Mi")),
            json!({"name": "v", "emptyDir": {"medium": "Memory"}}),
        );
        assert_eq!(a.desired_size, None);
    }

    struct SelinuxPlugin(bool);

    #[async_trait]
    impl VolumePlugin for SelinuxPlugin {
        fn name(&self) -> &'static str {
            "fake-selinux"
        }
        fn get_volume_name(&self, spec: &Spec<'_>) -> anyhow::Result<String> {
            Ok(spec.name().to_string())
        }
        fn can_support(&self, _spec: &Spec<'_>) -> bool {
            true
        }
        fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
            false
        }
        fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> anyhow::Result<bool> {
            Ok(self.0)
        }
        async fn new_mounter(&self, _: &Spec<'_>, _: &Pod) -> anyhow::Result<Box<dyn Mounter>> {
            unimplemented!()
        }
        fn new_unmounter(&self, _: &str, _: &str) -> anyhow::Result<Box<dyn Unmounter>> {
            unimplemented!()
        }
        fn construct_volume_spec(
            &self,
            _: &str,
            _: &str,
        ) -> anyhow::Result<rusternetes_kubelet::volume_plugins::ReconstructedVolume> {
            unimplemented!()
        }
    }

    fn selinux_args(
        plugin_supports: bool,
        access_mode: &str,
    ) -> rusternetes_kubelet::volume_plugins::MounterArgs {
        let _g = with_feature(Feature::SELinuxMountReadWriteOncePod, true);
        let mgr = VolumePluginMgr::new(vec![Box::new(SelinuxPlugin(plugin_supports))]);
        let pod = pod_json(json!({
            "metadata": {"name": "p", "namespace": "ns", "uid": "u"},
            "spec": {
                "containers": [{
                    "name": "c", "image": "i",
                    "volumeMounts": [{"name": "v", "mountPath": "/v"}],
                    "securityContext": {"seLinuxOptions": {
                        "user": "system_u", "role": "object_r",
                        "type": "container_t", "level": "s0:c1,c2"}}
                }],
                "volumes": [{"name": "v", "persistentVolumeClaim": {"claimName": "c"}}]
            }
        }));
        let v = pod.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0].clone();
        let pv: PersistentVolume = serde_json::from_value(json!({
            "metadata": {"name": "pv"}, "spec": {"accessModes": [access_mode]}
        }))
        .unwrap();
        let spec = Spec {
            volume: &v,
            persistent_volume: Some(&pv),
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        mounter_args_for(&mgr, &spec, &pod, &FakeSELinuxLabelTranslator).unwrap()
    }

    /// `AddPodToVolume` -> `getSELinuxLabel` -> `GetMountSELinuxLabel`: an RWOP
    /// volume on a plugin that supports the context mount carries the
    /// containers' file label into `MounterArgs.SELinuxLabel`.
    #[test]
    #[serial_test::serial]
    fn rwop_volume_gets_the_container_selinux_label() {
        let a = selinux_args(true, "ReadWriteOncePod");
        assert_eq!(
            a.selinux_label,
            "system_u:object_r:container_file_t:s0:c1,c2"
        );
    }

    /// A plugin without SELinux context-mount support gets no label.
    #[test]
    #[serial_test::serial]
    fn plugin_without_selinux_mount_gets_no_label() {
        assert_eq!(selinux_args(false, "ReadWriteOncePod").selinux_label, "");
    }
}
