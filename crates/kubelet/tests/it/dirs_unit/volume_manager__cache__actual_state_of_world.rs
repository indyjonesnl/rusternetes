//! Unit tests moved from `src/volume_manager/cache/actual_state_of_world.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::feature_gates::{self, Feature};
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_kubelet::volume_plugins::plugin::{BlockVolumeMapper, Mounter, OwnedSpec};
    pub use rusternetes_kubelet::volume_plugins::registry::VolumePluginMgr;
    pub use rusternetes_kubelet::volume_plugins::util::get_unique_volume_name_from_spec;
    pub use rusternetes_kubelet::volume_plugins::util::operation_executor::{
        self, DeviceMountState, MarkVolumeOpts, VolumeMountState,
    };
    pub use rusternetes_kubelet::volume_plugins::util::types::{UniquePodName, UniqueVolumeName};
    pub use std::collections::{HashMap, HashSet};
    pub use std::ops::{Deref, DerefMut};
    pub use std::sync::{Arc, RwLock};
    pub use tracing::{debug, error};
}

mod tests {
    //! Ports of `pkg/kubelet/volumemanager/cache/actual_state_of_world_test.go`.
    //! Each test keeps the name of the Go test it came from; tests with no Go
    //! counterpart say so.
    //!
    //! **Two substitutions run through every test here**, the same two the
    //! `DesiredStateOfWorld` port made.
    //!
    //! 1. *GCE PersistentDisk -> NFS.* Upstream's fixtures use
    //!    `v1.GCEPersistentDiskVolumeSource{PDName: "fake-deviceN"}` purely as
    //!    "a volume source whose identity the fake plugin can read". This
    //!    project's `Volume` has no `gcePersistentDisk` field, so `nfs` plays
    //!    that role and `nfs.path` carries the `fake-deviceN` identity.
    //!    Nothing about the volume kind matters to `ActualStateOfWorld`.
    //!
    //! 2. *Mounters and mappers are built directly, not through the plugin.*
    //!    Upstream calls `plugin.NewMounter(...)` / `plugin.NewBlockVolumeMapper(...)`
    //!    to obtain the opaque values it stashes in `MarkVolumeOpts`. Our
    //!    `VolumePlugin::new_mounter` is `async` and no plugin implements
    //!    `BlockVolumeMapper` yet, so the tests construct [`FakeMounter`] and
    //!    [`FakeBlockVolumeMapper`] directly. The cache only ever stores and
    //!    returns these values, so where they came from is immaterial.

    use super::__parent::*;
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use rusternetes_common::feature_gates::with_feature;
    use rusternetes_common::quantity::Format;
    use rusternetes_common::resources::Pod;
    use rusternetes_kubelet::volume_manager::cache::actual_state_of_world::*;
    use rusternetes_kubelet::volume_plugins::plugin::{
        Mounter, NodeExpandableVolumePlugin, NodeResizeOptions, Spec, VolumePlugin,
    };
    use rusternetes_kubelet::volume_plugins::util::get_unique_pod_name;

    /// Port of `volumetesting.FakeVolumePlugin`
    /// (`pkg/volume/testing/testing.go`), trimmed to what the
    /// `ActualStateOfWorld` tests exercise. Same shape as the
    /// `DesiredStateOfWorld` tests' fake.
    struct FakeVolumePlugin {
        plugin_name: &'static str,
        /// Stands for registering the plugin as a
        /// `FakeAttachableVolumePlugin`.
        attachable: bool,
        /// Stands for registering the plugin as a
        /// `FakeDeviceMountableVolumePlugin`.
        device_mountable: bool,
        /// `FakeVolumePlugin.SupportsRemount` (`testing.go:287-289`).
        supports_remount: bool,
        /// Stands for registering the plugin as a
        /// `NodeExpandableVolumePlugin` whose `RequiresFSResize` is true.
        requires_fs_resize: bool,
    }

    #[async_trait]
    impl VolumePlugin for FakeVolumePlugin {
        fn name(&self) -> &'static str {
            self.plugin_name
        }

        /// `FakeVolumePlugin.GetVolumeName` (`testing.go:258-277`), with NFS in
        /// GCE PD's place.
        fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
            let mut volume_name = String::new();
            if let Some(nfs) = &spec.volume.nfs {
                volume_name = nfs.path.clone();
            }
            if volume_name.is_empty() {
                volume_name = spec.name().to_string();
            }
            Ok(volume_name)
        }

        fn can_support(&self, _spec: &Spec<'_>) -> bool {
            true
        }

        fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
            self.supports_remount
        }

        fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
            Ok(false)
        }

        fn can_attach(&self, _spec: &Spec<'_>) -> Result<bool> {
            Ok(self.attachable)
        }

        fn can_device_mount(&self, _spec: &Spec<'_>) -> bool {
            self.device_mountable
        }

        fn as_node_expandable_plugin(&self) -> Option<&dyn NodeExpandableVolumePlugin> {
            self.requires_fs_resize.then_some(self)
        }

        async fn new_mounter(&self, _spec: &Spec<'_>, _pod: &Pod) -> Result<Box<dyn Mounter>> {
            Err(anyhow!("ASW tests build mounters directly"))
        }

        fn new_unmounter(
            &self,
            _vol_name: &str,
            _pod_uid: &str,
        ) -> Result<Box<dyn rusternetes_kubelet::volume_plugins::Unmounter>> {
            unimplemented!("test plugin never unmounts")
        }

        fn construct_volume_spec(
            &self,
            _vol_name: &str,
            _mount_path: &str,
        ) -> Result<rusternetes_kubelet::volume_plugins::ReconstructedVolume> {
            unimplemented!("test plugin never reconstructs")
        }
    }

    #[async_trait]
    impl NodeExpandableVolumePlugin for FakeVolumePlugin {
        fn requires_fs_resize(&self) -> bool {
            true
        }

        async fn node_expand(&self, _opts: NodeResizeOptions<'_>) -> Result<bool> {
            Ok(true)
        }
    }

    /// Stands in for whatever `plugin.NewMounter` returns upstream. The cache
    /// never calls a method on it; `id` only exists so a test can tell two
    /// mounters apart.
    struct FakeMounter {
        id: &'static str,
    }

    impl rusternetes_kubelet::volume_plugins::plugin::MetricsProvider for FakeMounter {
        fn get_metrics(
            &self,
        ) -> anyhow::Result<rusternetes_kubelet::volume_plugins::plugin::Metrics> {
            Ok(Default::default())
        }
    }

    #[async_trait]
    impl Mounter for FakeMounter {
        fn get_path(&self) -> String {
            self.id.to_string()
        }

        fn get_attributes(&self) -> rusternetes_kubelet::volume_plugins::plugin::Attributes {
            Default::default()
        }

        async fn set_up_at(
            &self,
            _dir: &str,
            _args: &rusternetes_kubelet::volume_plugins::MounterArgs,
        ) -> Result<()> {
            Ok(())
        }
    }

    /// Stands in for whatever `plugin.NewBlockVolumeMapper` returns upstream.
    struct FakeBlockVolumeMapper;

    impl rusternetes_kubelet::volume_plugins::plugin::MetricsProvider for FakeBlockVolumeMapper {
        fn get_metrics(&self) -> Result<rusternetes_kubelet::volume_plugins::plugin::Metrics> {
            Ok(Default::default())
        }
    }

    impl BlockVolumeMapper for FakeBlockVolumeMapper {
        fn supports_metrics(&self) -> bool {
            false
        }

        fn get_global_map_path(&self, _spec: &Spec<'_>) -> Result<String> {
            Ok("fake/global/map/path".to_string())
        }

        fn get_pod_device_map_path(&self) -> (String, String) {
            ("fake/pod/device/map/path".to_string(), "fake".to_string())
        }
    }

    /// Port of `volumetesting.GetTestKubeletVolumePluginMgr`
    /// (`pkg/volume/testing/testing.go:1671-1680`): one `FakeVolumePlugin`
    /// named `fake-plugin` that supports every spec and is both attachable and
    /// device-mountable.
    fn get_test_kubelet_volume_plugin_mgr() -> Arc<VolumePluginMgr> {
        Arc::new(VolumePluginMgr::new(vec![Box::new(FakeVolumePlugin {
            plugin_name: "fake-plugin",
            attachable: true,
            device_mountable: true,
            supports_remount: false,
            requires_fs_resize: false,
        })]))
    }

    fn new_asw(mgr: Arc<VolumePluginMgr>) -> ActualStateOfWorld {
        ActualStateOfWorld::new("mynode", mgr)
    }

    /// `getTestPod` (`actual_state_of_world_test.go:704-726`), with `nfs` in
    /// `gcePersistentDisk`'s place.
    fn get_test_pod(pod_name: &str, pod_uid: &str, outer_volume_name: &str, pd_name: &str) -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": { "name": pod_name, "uid": pod_uid },
            "spec": {
                "containers": [],
                "volumes": [
                    { "name": outer_volume_name, "nfs": { "server": "fake", "path": pd_name } }
                ]
            }
        }))
        .expect("pod fixture")
    }

    /// `&volume.Spec{Volume: &pod.Spec.Volumes[index]}`.
    fn spec_of(pod: &Pod, index: usize) -> Arc<OwnedSpec> {
        Arc::new(OwnedSpec {
            volume: pod
                .spec
                .as_ref()
                .expect("spec")
                .volumes
                .as_ref()
                .expect("volumes")[index]
                .clone(),
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        })
    }

    /// `util.GetUniqueVolumeNameFromSpec(plugin, volumeSpec)` against the fake
    /// plugin, which is what `addVolume` generates for a `None` volume name.
    fn generated_volume_name(spec: &OwnedSpec) -> UniqueVolumeName {
        UniqueVolumeName(format!(
            "fake-plugin/{}",
            spec.volume.nfs.as_ref().expect("nfs fixture").path
        ))
    }

    /// The `operationexecutor.MarkVolumeOpts` literal every test builds.
    fn mark_volume_opts(
        pod: &Pod,
        volume_name: &UniqueVolumeName,
        volume_spec: Arc<OwnedSpec>,
    ) -> MarkVolumeOpts {
        MarkVolumeOpts {
            pod_name: get_unique_pod_name(pod),
            pod_uid: pod.metadata.uid.clone(),
            volume_name: volume_name.clone(),
            mounter: Some(Arc::new(FakeMounter { id: "fake-mounter" })),
            block_volume_mapper: Some(Arc::new(FakeBlockVolumeMapper)),
            volume_gid_volume: String::new(),
            volume_spec,
            // Go's zero value; upstream's literals leave the field out.
            volume_mount_state: VolumeMountState::Unspecified,
            selinux_mount_context: String::new(),
        }
    }

    // -----------------------------------------------------------------
    // The `verify*` helpers (`actual_state_of_world_test.go:1082-1374`).
    // -----------------------------------------------------------------

    fn verify_volume_exists_in_globally_mounted_volumes(
        expected_volume_name: &UniqueVolumeName,
        asw: &ActualStateOfWorld,
    ) {
        assert!(
            asw.get_globally_mounted_volumes()
                .iter()
                .any(|volume| volume.volume_name == *expected_volume_name),
            "could not find volume {expected_volume_name} in the list of GloballyMountedVolumes"
        );
    }

    fn verify_volume_exists_in_globally_mounted_volumes_with_selinux(
        expected_volume_name: &UniqueVolumeName,
        expected_selinux_context: &str,
        asw: &ActualStateOfWorld,
    ) {
        let globally_mounted_volumes = asw.get_globally_mounted_volumes();
        let volume = globally_mounted_volumes
            .iter()
            .find(|volume| volume.volume_name == *expected_volume_name)
            .unwrap_or_else(|| {
                panic!(
                    "could not find volume {expected_volume_name} in the list of \
                     GloballyMountedVolumes"
                )
            });
        assert_eq!(
            volume.selinux_mount_context, expected_selinux_context,
            "volume {expected_volume_name} has wrong SELinux context"
        );
    }

    fn verify_volume_doesnt_exist_in_globally_mounted_volumes(
        volume_to_check: &UniqueVolumeName,
        asw: &ActualStateOfWorld,
    ) {
        assert!(
            !asw.get_globally_mounted_volumes()
                .iter()
                .any(|volume| volume.volume_name == *volume_to_check),
            "found volume {volume_to_check} in the list of GloballyMountedVolumes; \
             expected it not to exist"
        );
    }

    fn verify_volume_exists_asw(
        expected_volume_name: &UniqueVolumeName,
        should_exist: bool,
        asw: &ActualStateOfWorld,
    ) {
        assert_eq!(
            asw.volume_exists(expected_volume_name),
            should_exist,
            "volume_exists({expected_volume_name}) response incorrect"
        );
    }

    fn verify_volume_exists_asw_with_selinux(
        expected_volume_name: &UniqueVolumeName,
        expected_selinux_context: &str,
        asw: &ActualStateOfWorld,
    ) {
        let volumes = asw.get_mounted_volumes();
        let volume = volumes
            .iter()
            .find(|volume| volume.volume_name == *expected_volume_name)
            .unwrap_or_else(|| panic!("volume {expected_volume_name} not found in ASW"));
        assert_eq!(
            volume.selinux_mount_context, expected_selinux_context,
            "volume {expected_volume_name} has wrong SELinux context"
        );
    }

    fn verify_volume_exists_in_unmounted_volumes(
        expected_volume_name: &UniqueVolumeName,
        asw: &ActualStateOfWorld,
    ) {
        assert!(
            asw.get_unmounted_volumes()
                .iter()
                .any(|volume| volume.volume_name == *expected_volume_name),
            "could not find volume {expected_volume_name} in the list of UnmountedVolumes"
        );
    }

    fn verify_volume_doesnt_exist_in_unmounted_volumes(
        volume_to_check: &UniqueVolumeName,
        asw: &ActualStateOfWorld,
    ) {
        assert!(
            !asw.get_unmounted_volumes()
                .iter()
                .any(|volume| volume.volume_name == *volume_to_check),
            "found volume {volume_to_check} in the list of UnmountedVolumes; \
             expected it not to exist"
        );
    }

    fn verify_pod_exists_in_volume_asw(
        expected_pod_name: &UniquePodName,
        expected_volume_name: &UniqueVolumeName,
        expected_device_path: &str,
        asw: &ActualStateOfWorld,
    ) {
        verify_pod_exists_in_volume_asw_with_selinux(
            expected_pod_name,
            expected_volume_name,
            expected_device_path,
            "",
            asw,
        );
    }

    fn verify_pod_exists_in_volume_asw_with_selinux(
        expected_pod_name: &UniquePodName,
        expected_volume_name: &UniqueVolumeName,
        expected_device_path: &str,
        expected_selinux_label: &str,
        asw: &ActualStateOfWorld,
    ) {
        let (pod_exists_in_volume, device_path, err) = asw.pod_exists_in_volume(
            expected_pod_name,
            expected_volume_name,
            None,
            expected_selinux_label,
        );
        assert!(err.is_none(), "pod_exists_in_volume failed: {err:?}");
        assert!(pod_exists_in_volume, "pod_exists_in_volume result invalid");
        assert_eq!(device_path, expected_device_path, "invalid device path");
    }

    fn verify_volume_mounted_elsewhere(
        expected_pod_name: &UniquePodName,
        expected_volume_name: &UniqueVolumeName,
        expected_mounted_elsewhere: bool,
        asw: &ActualStateOfWorld,
    ) {
        assert_eq!(
            asw.is_volume_mounted_elsewhere(expected_volume_name, expected_pod_name),
            expected_mounted_elsewhere,
            "is_volume_mounted_elsewhere assertion failure"
        );
    }

    fn verify_pod_doesnt_exist_in_volume_asw(
        pod_to_check: &UniquePodName,
        volume_to_check: &UniqueVolumeName,
        expect_volume_to_exist: bool,
        asw: &ActualStateOfWorld,
    ) {
        let (pod_exists_in_volume, device_path, err) =
            asw.pod_exists_in_volume(pod_to_check, volume_to_check, None, "");
        if !expect_volume_to_exist {
            assert!(
                err.is_some(),
                "pod_exists_in_volume did not return an error; \
                 expected one indicating the volume does not exist"
            );
        } else {
            assert!(err.is_none(), "pod_exists_in_volume failed: {err:?}");
        }
        assert!(!pod_exists_in_volume, "pod_exists_in_volume result invalid");
        assert_eq!(device_path, "", "invalid device path");
    }

    fn verify_pod_exists_in_volume_selinux_mismatch(
        pod_to_check: &UniquePodName,
        volume_to_check: &UniqueVolumeName,
        unexpected_selinux_label: &str,
        asw: &ActualStateOfWorld,
    ) {
        let (pod_exists_in_volume, _, err) = asw.pod_exists_in_volume(
            pod_to_check,
            volume_to_check,
            None,
            unexpected_selinux_label,
        );
        assert!(
            !pod_exists_in_volume,
            "expected pod {pod_to_check} not to exist, but it does"
        );
        assert!(
            is_selinux_mount_mismatch_error(err.as_ref()),
            "expected pod_exists_in_volume to return SELinuxMountMismatch, got {err:?}"
        );
    }

    fn verify_volume_exists_with_spec_name_in_volume_asw(
        expected_pod_name: &UniquePodName,
        expected_volume_name: &str,
        asw: &ActualStateOfWorld,
    ) {
        assert!(
            asw.volume_exists_with_spec_name(expected_pod_name, expected_volume_name),
            "volume_exists_with_spec_name result invalid; expected true"
        );
    }

    fn verify_volume_doesnt_exist_with_spec_name_in_volume_asw(
        pod_to_check: &UniquePodName,
        volume_to_check: &str,
        asw: &ActualStateOfWorld,
    ) {
        assert!(
            !asw.volume_exists_with_spec_name(pod_to_check, volume_to_check),
            "volume_exists_with_spec_name result invalid; expected false"
        );
    }

    fn verify_volume_spec_name_in_volume_asw(
        pod_to_check: &UniquePodName,
        volume_specs: &[Arc<OwnedSpec>],
        asw: &ActualStateOfWorld,
    ) {
        for (i, volume) in asw
            .get_mounted_volumes_for_pod(pod_to_check)
            .iter()
            .enumerate()
        {
            assert_eq!(
                volume.inner_volume_spec_name,
                volume_specs[i].name(),
                "volume spec name does not match"
            );
        }
    }

    fn verify_volume_found_in_reconstruction(
        pod_to_check: &UniquePodName,
        volume_to_check: &UniqueVolumeName,
        asw: &ActualStateOfWorld,
    ) {
        assert!(
            asw.is_volume_reconstructed(volume_to_check, pod_to_check),
            "is_volume_reconstructed result invalid; expected true"
        );
    }

    /// `verifyVolumeAttachability` (`actual_state_of_world_test.go:1351-1374`).
    /// Note its own comment: "ASW does not have any special difference between
    /// False and Uncertain. Uncertain only allows to be changed to True /
    /// False." — which is why both expectations assert the same thing.
    fn verify_volume_attachability(
        volume_to_check: &UniqueVolumeName,
        asw: &ActualStateOfWorld,
        expected: VolumeAttachability,
    ) {
        let attachable = asw
            .get_attached_volumes()
            .iter()
            .find(|volume| volume.volume_name == *volume_to_check)
            .is_some_and(|volume| volume.plugin_is_attachable);

        match expected {
            VolumeAttachability::True => assert!(
                attachable,
                "ASW reports {volume_to_check} as not-attachable, when True was expected"
            ),
            VolumeAttachability::False | VolumeAttachability::Uncertain => assert!(
                !attachable,
                "ASW reports {volume_to_check} as attachable, when {expected:?} was expected"
            ),
        }
    }

    // -----------------------------------------------------------------
    // The tests.
    // -----------------------------------------------------------------

    /// Calls `MarkVolumeAsAttached` once to add a volume. Verifies the newly
    /// added volume exists in `GetUnmountedVolumes` and does not exist in
    /// `GetGloballyMountedVolumes`.
    /// (`actual_state_of_world_test.go:44-83`)
    #[test]
    fn test_mark_volume_as_attached_positive_new_volume() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let device_path = "fake/device/path";
        let generated_volume_name = generated_volume_name(&volume_spec);

        asw.mark_volume_as_attached(None, volume_spec, "", device_path)
            .expect("mark_volume_as_attached failed");

        verify_volume_exists_asw(&generated_volume_name, true, &asw);
        verify_volume_exists_in_unmounted_volumes(&generated_volume_name, &asw);
        verify_volume_doesnt_exist_in_globally_mounted_volumes(&generated_volume_name, &asw);
    }

    /// The supplied volume name is used to register the volume rather than the
    /// generated one.
    /// (`actual_state_of_world_test.go:89-129`)
    #[test]
    fn test_mark_volume_as_attached_supplied_volume_name_positive_new_volume() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let device_path = "fake/device/path";
        let volume_name = UniqueVolumeName("this-would-never-be-a-volume-name".to_string());

        asw.mark_volume_as_attached(Some(&volume_name), volume_spec, "", device_path)
            .expect("mark_volume_as_attached failed");

        verify_volume_exists_asw(&volume_name, true, &asw);
        verify_volume_exists_in_unmounted_volumes(&volume_name, &asw);
        verify_volume_doesnt_exist_in_globally_mounted_volumes(&volume_name, &asw);
    }

    /// Calls `MarkVolumeAsAttached` twice for the same volume and verifies the
    /// second call does not fail.
    /// (`actual_state_of_world_test.go:132-179`)
    #[test]
    fn test_mark_volume_as_attached_positive_existing_volume() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let device_path = "fake/device/path";
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let generated_volume_name = generated_volume_name(&volume_spec);
        asw.mark_volume_as_attached(None, volume_spec.clone(), "", device_path)
            .expect("mark_volume_as_attached failed");

        asw.mark_volume_as_attached(None, volume_spec, "", device_path)
            .expect("mark_volume_as_attached failed");

        verify_volume_exists_asw(&generated_volume_name, true, &asw);
        verify_volume_exists_in_unmounted_volumes(&generated_volume_name, &asw);
        verify_volume_doesnt_exist_in_globally_mounted_volumes(&generated_volume_name, &asw);
    }

    /// Populates the data struct with a volume, calls `AddPodToVolume` to add a
    /// pod to it, and verifies the volume/pod combo exists.
    /// (`actual_state_of_world_test.go:184-252`)
    #[test]
    fn test_add_pod_to_volume_positive_existing_volume_new_node() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let device_path = "fake/device/path";
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let generated_volume_name = generated_volume_name(&volume_spec);
        asw.mark_volume_as_attached(None, volume_spec.clone(), "", device_path)
            .expect("mark_volume_as_attached failed");
        let pod_name = get_unique_pod_name(&pod);

        asw.add_pod_to_volume(mark_volume_opts(
            &pod,
            &generated_volume_name,
            volume_spec.clone(),
        ))
        .expect("add_pod_to_volume failed");

        verify_volume_exists_asw(&generated_volume_name, true, &asw);
        verify_volume_doesnt_exist_in_unmounted_volumes(&generated_volume_name, &asw);
        verify_volume_doesnt_exist_in_globally_mounted_volumes(&generated_volume_name, &asw);
        verify_pod_exists_in_volume_asw(&pod_name, &generated_volume_name, device_path, &asw);
        verify_volume_exists_with_spec_name_in_volume_asw(&pod_name, volume_spec.name(), &asw);
        verify_volume_mounted_elsewhere(&pod_name, &generated_volume_name, false, &asw);
    }

    /// Calls `AddPodToVolume` twice with the same pod and verifies the second
    /// call does not fail.
    /// (`actual_state_of_world_test.go:257-332`)
    #[test]
    fn test_add_pod_to_volume_positive_existing_volume_existing_node() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let device_path = "fake/device/path";
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let generated_volume_name = generated_volume_name(&volume_spec);
        asw.mark_volume_as_attached(None, volume_spec.clone(), "", device_path)
            .expect("mark_volume_as_attached failed");
        let pod_name = get_unique_pod_name(&pod);
        let opts = mark_volume_opts(&pod, &generated_volume_name, volume_spec.clone());
        asw.add_pod_to_volume(opts.clone())
            .expect("add_pod_to_volume failed");

        asw.add_pod_to_volume(opts)
            .expect("add_pod_to_volume failed");

        verify_volume_exists_asw(&generated_volume_name, true, &asw);
        verify_volume_doesnt_exist_in_unmounted_volumes(&generated_volume_name, &asw);
        verify_volume_doesnt_exist_in_globally_mounted_volumes(&generated_volume_name, &asw);
        verify_pod_exists_in_volume_asw(&pod_name, &generated_volume_name, device_path, &asw);
        verify_volume_exists_with_spec_name_in_volume_asw(&pod_name, volume_spec.name(), &asw);
        verify_volume_mounted_elsewhere(&pod_name, &generated_volume_name, false, &asw);
    }

    /// Two pods sharing one attachable volume: both resolve to the same unique
    /// volume name and each sees the other as "mounted elsewhere".
    /// (`actual_state_of_world_test.go:337-465`)
    #[test]
    fn test_add_two_pods_to_volume_positive() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let device_path = "fake/device/path";
        let pod1 = get_test_pod("pod1", "pod1uid", "volume-name-1", "fake-device1");
        let pod2 = get_test_pod("pod2", "pod2uid", "volume-name-2", "fake-device1");
        let volume_spec1 = spec_of(&pod1, 0);
        let volume_spec2 = spec_of(&pod2, 0);
        let generated_volume_name1 = generated_volume_name(&volume_spec1);
        let generated_volume_name2 = generated_volume_name(&volume_spec2);
        assert_eq!(
            generated_volume_name1, generated_volume_name2,
            "unique volume names should be the same"
        );

        asw.mark_volume_as_attached(
            Some(&generated_volume_name1),
            volume_spec1.clone(),
            "",
            device_path,
        )
        .expect("mark_volume_as_attached failed");

        let pod_name1 = get_unique_pod_name(&pod1);
        asw.add_pod_to_volume(mark_volume_opts(
            &pod1,
            &generated_volume_name1,
            volume_spec1.clone(),
        ))
        .expect("add_pod_to_volume failed");

        let pod_name2 = get_unique_pod_name(&pod2);
        asw.add_pod_to_volume(mark_volume_opts(
            &pod2,
            &generated_volume_name1,
            volume_spec2.clone(),
        ))
        .expect("add_pod_to_volume failed");

        verify_volume_exists_asw(&generated_volume_name1, true, &asw);
        verify_volume_doesnt_exist_in_unmounted_volumes(&generated_volume_name1, &asw);
        verify_volume_doesnt_exist_in_globally_mounted_volumes(&generated_volume_name1, &asw);
        verify_pod_exists_in_volume_asw(&pod_name1, &generated_volume_name1, device_path, &asw);
        verify_volume_exists_with_spec_name_in_volume_asw(&pod_name1, volume_spec1.name(), &asw);
        verify_pod_exists_in_volume_asw(&pod_name2, &generated_volume_name2, device_path, &asw);
        verify_volume_exists_with_spec_name_in_volume_asw(&pod_name2, volume_spec2.name(), &asw);
        verify_volume_spec_name_in_volume_asw(&pod_name1, &[volume_spec1], &asw);
        verify_volume_spec_name_in_volume_asw(&pod_name2, &[volume_spec2], &asw);
        // Upstream's `""` VolumeMountState is what makes these true — see
        // `VolumeMountState::Unspecified`.
        verify_volume_mounted_elsewhere(&pod_name1, &generated_volume_name1, true, &asw);
        verify_volume_mounted_elsewhere(&pod_name2, &generated_volume_name2, true, &asw);
    }

    /// Volumes recorded as read from disk during reconstruction are handled
    /// correctly by the ASW.
    /// (`actual_state_of_world_test.go:468-636`)
    #[test]
    fn test_actual_state_of_world_found_during_reconstruction() {
        type Callback = fn(&ActualStateOfWorld, &MarkVolumeOpts);

        let cases: &[(&str, Callback, Callback)] = &[
            (
                "marking volume mounted should remove volume from found during reconstruction",
                |asw, opts| {
                    let mut opts = opts.clone();
                    opts.volume_mount_state = VolumeMountState::VolumeMounted;
                    asw.mark_volume_as_mounted(opts)
                        .expect("mark_volume_as_mounted failed");
                },
                |asw, opts| {
                    assert!(
                        !asw.is_volume_reconstructed(&opts.volume_name, &opts.pod_name),
                        "found unexpected volume in reconstructed volume list"
                    );
                },
            ),
            (
                "removing volume from pod should remove volume from found during reconstruction",
                |asw, opts| {
                    asw.mark_volume_as_unmounted(&opts.pod_name, &opts.volume_name)
                        .expect("mark_volume_as_unmounted failed");
                },
                |asw, opts| {
                    assert!(
                        !asw.is_volume_reconstructed(&opts.volume_name, &opts.pod_name),
                        "found unexpected volume in reconstructed volume list"
                    );
                },
            ),
            (
                "removing volume entirely from ASOW should remove volume from found during \
                 reconstruction",
                |asw, opts| {
                    asw.mark_volume_as_unmounted(&opts.pod_name, &opts.volume_name)
                        .expect("mark_volume_as_unmounted failed");
                    asw.mark_volume_as_detached(&opts.volume_name, "");
                },
                |asw, opts| {
                    assert!(
                        !asw.is_volume_reconstructed(&opts.volume_name, &opts.pod_name),
                        "found unexpected volume in reconstructed volume list"
                    );
                    assert!(
                        !asw.state
                            .read()
                            .expect("lock")
                            .found_during_reconstruction
                            .contains_key(&opts.volume_name),
                        "found unexpected volume in reconstructed map"
                    );
                },
            ),
            (
                "uncertain attachability is resolved to attachable",
                |asw, opts| asw.update_reconstructed_volume_attachability(&opts.volume_name, true),
                |asw, opts| {
                    verify_volume_attachability(&opts.volume_name, asw, VolumeAttachability::True);
                },
            ),
            (
                "uncertain attachability is resolved to non-attachable",
                |asw, opts| asw.update_reconstructed_volume_attachability(&opts.volume_name, false),
                |asw, opts| {
                    verify_volume_attachability(&opts.volume_name, asw, VolumeAttachability::False);
                },
            ),
            (
                "certain (false) attachability cannot be changed",
                |asw, opts| {
                    asw.update_reconstructed_volume_attachability(&opts.volume_name, false);
                    // This call should be a NOOP:
                    asw.update_reconstructed_volume_attachability(&opts.volume_name, true);
                },
                |asw, opts| {
                    verify_volume_attachability(&opts.volume_name, asw, VolumeAttachability::False);
                },
            ),
            (
                "certain (true) attachability cannot be changed",
                |asw, opts| {
                    asw.update_reconstructed_volume_attachability(&opts.volume_name, true);
                    // This call should be a NOOP:
                    asw.update_reconstructed_volume_attachability(&opts.volume_name, false);
                },
                |asw, opts| {
                    verify_volume_attachability(&opts.volume_name, asw, VolumeAttachability::True);
                },
            ),
        ];

        for (name, op_callback, verify_callback) in cases {
            let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
            let device_path = "fake/device/path";

            let pod1 = get_test_pod("pod1", "pod1uid", "volume-name-1", "fake-device1");
            let volume_spec1 = spec_of(&pod1, 0);
            let generated_volume_name1 = generated_volume_name(&volume_spec1);
            asw.add_attach_uncertain_reconstructed_volume(
                Some(&generated_volume_name1),
                volume_spec1.clone(),
                "",
                device_path,
            )
            .unwrap_or_else(|err| panic!("for test {name}: {err}"));
            let pod_name1 = get_unique_pod_name(&pod1);

            let mut opts = mark_volume_opts(&pod1, &generated_volume_name1, volume_spec1.clone());
            opts.volume_mount_state = VolumeMountState::VolumeMountUncertain;
            asw.check_and_mark_volume_as_uncertain_via_reconstruction(opts.clone())
                .unwrap_or_else(|err| panic!("for test {name}: {err}"));

            // make sure state is as we expect it to be
            verify_volume_exists_asw(&generated_volume_name1, true, &asw);
            verify_volume_doesnt_exist_in_unmounted_volumes(&generated_volume_name1, &asw);
            verify_volume_doesnt_exist_in_globally_mounted_volumes(&generated_volume_name1, &asw);
            verify_volume_exists_with_spec_name_in_volume_asw(
                &pod_name1,
                volume_spec1.name(),
                &asw,
            );
            verify_volume_spec_name_in_volume_asw(&pod_name1, &[volume_spec1], &asw);
            verify_volume_found_in_reconstruction(&pod_name1, &generated_volume_name1, &asw);
            verify_volume_attachability(
                &generated_volume_name1,
                &asw,
                VolumeAttachability::Uncertain,
            );

            op_callback(&asw, &opts);
            verify_callback(&asw, &opts);
        }
    }

    /// `MarkVolumeAsDetached` on a volume mounted by pod(s) is skipped — and
    /// the caller is told nothing, because the interface has no error return.
    /// (`actual_state_of_world_test.go:637-702`)
    #[test]
    fn test_mark_volume_as_detached_negative_pod_in_volume() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let device_path = "fake/device/path";
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        asw.mark_volume_as_attached(None, volume_spec.clone(), "", device_path)
            .expect("mark_volume_as_attached failed");
        let generated_volume_name = generated_volume_name(&volume_spec);
        let pod_name = get_unique_pod_name(&pod);
        asw.add_pod_to_volume(mark_volume_opts(&pod, &generated_volume_name, volume_spec))
            .expect("add_pod_to_volume failed");

        asw.mark_volume_as_detached(&generated_volume_name, "");

        verify_pod_exists_in_volume_asw(&pod_name, &generated_volume_name, device_path, &asw);
    }

    /// Calls `AddPodToVolume` on an empty data struct; the call must fail with
    /// "volume does not exist". This is where the ASW deliberately differs from
    /// the DSW, whose `add_pod_to_volume` creates the volume implicitly.
    /// (`actual_state_of_world_test.go:726-819`)
    #[test]
    fn test_add_pod_to_volume_negative_volume_doesnt_exist() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let volume_name = generated_volume_name(&volume_spec);
        let pod_name = get_unique_pod_name(&pod);

        let err = asw
            .add_pod_to_volume(mark_volume_opts(&pod, &volume_name, volume_spec.clone()))
            .expect_err("add_pod_to_volume did not fail");
        assert_eq!(
            err,
            ActualStateOfWorldError::VolumeNotInAttachedVolumes {
                volume_name: volume_name.to_string(),
            }
        );

        verify_volume_exists_asw(&volume_name, false, &asw);
        verify_volume_doesnt_exist_in_unmounted_volumes(&volume_name, &asw);
        verify_volume_doesnt_exist_in_globally_mounted_volumes(&volume_name, &asw);
        verify_pod_doesnt_exist_in_volume_asw(&pod_name, &volume_name, false, &asw);
        verify_volume_doesnt_exist_with_spec_name_in_volume_asw(
            &pod_name,
            volume_spec.name(),
            &asw,
        );
        verify_volume_mounted_elsewhere(&pod_name, &volume_name, false, &asw);
    }

    /// `MarkDeviceAsMounted` marks the volume as globally mounted; it stays in
    /// `GetUnmountedVolumes` because no pod mounted it.
    /// (`actual_state_of_world_test.go:816-869`)
    #[test]
    fn test_mark_device_as_mounted_positive_new_volume() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let device_path = "fake/device/path";
        let device_mount_path = "fake/device/mount/path";
        let generated_volume_name = generated_volume_name(&volume_spec);
        asw.mark_volume_as_attached(None, volume_spec, "", device_path)
            .expect("mark_volume_as_attached failed");

        asw.mark_device_as_mounted(&generated_volume_name, device_path, device_mount_path, "")
            .expect("mark_device_as_mounted failed");

        verify_volume_exists_asw(&generated_volume_name, true, &asw);
        verify_volume_exists_in_unmounted_volumes(&generated_volume_name, &asw);
        verify_volume_exists_in_globally_mounted_volumes(&generated_volume_name, &asw);
    }

    /// `AddPodToVolume` with an SELinux context, which is stored on the volume
    /// too so a later `PodExistsInVolume` with a different label fails fast.
    /// (`actual_state_of_world_test.go:870-946`)
    #[test]
    #[serial_test::serial]
    fn test_add_pod_to_volume_positive_selinux() {
        let _gate = with_feature(Feature::SELinuxMountReadWriteOncePod, true);
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let device_path = "fake/device/path";
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let generated_volume_name = generated_volume_name(&volume_spec);
        asw.mark_volume_as_attached(None, volume_spec.clone(), "", device_path)
            .expect("mark_volume_as_attached failed");
        let pod_name = get_unique_pod_name(&pod);
        let label = "system_u:object_r:container_file_t:s0:c0,c1";

        let mut opts = mark_volume_opts(&pod, &generated_volume_name, volume_spec.clone());
        opts.selinux_mount_context = label.to_string();
        opts.volume_mount_state = VolumeMountState::VolumeMounted;
        asw.add_pod_to_volume(opts)
            .expect("add_pod_to_volume failed");

        verify_volume_exists_asw_with_selinux(&generated_volume_name, label, &asw);
        verify_volume_doesnt_exist_in_unmounted_volumes(&generated_volume_name, &asw);
        verify_volume_doesnt_exist_in_globally_mounted_volumes(&generated_volume_name, &asw);
        verify_pod_exists_in_volume_asw_with_selinux(
            &pod_name,
            &generated_volume_name,
            device_path,
            label,
            &asw,
        );
        verify_pod_exists_in_volume_selinux_mismatch(
            &pod_name,
            &generated_volume_name,
            "", // wrong SELinux label
            &asw,
        );
        verify_volume_exists_with_spec_name_in_volume_asw(&pod_name, volume_spec.name(), &asw);
        verify_volume_mounted_elsewhere(&pod_name, &generated_volume_name, false, &asw);
    }

    /// `MarkDeviceAsMounted` with an SELinux context.
    /// (`actual_state_of_world_test.go:947-996`)
    #[test]
    #[serial_test::serial]
    fn test_mark_device_as_mounted_positive_selinux() {
        let _gate = with_feature(Feature::SELinuxMountReadWriteOncePod, true);
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let device_path = "fake/device/path";
        let device_mount_path = "fake/device/mount/path";
        let label = "system_u:system_r:container_t:s0:c0,c1";
        let generated_volume_name = generated_volume_name(&volume_spec);
        asw.mark_volume_as_attached(None, volume_spec, "", device_path)
            .expect("mark_volume_as_attached failed");

        asw.mark_device_as_mounted(
            &generated_volume_name,
            device_path,
            device_mount_path,
            label,
        )
        .expect("mark_device_as_mounted failed");

        verify_volume_exists_asw(&generated_volume_name, true, &asw);
        verify_volume_exists_in_unmounted_volumes(&generated_volume_name, &asw);
        verify_volume_exists_in_globally_mounted_volumes_with_selinux(
            &generated_volume_name,
            label,
            &asw,
        );
    }

    /// The invariant the whole port turns on: a `VolumeMountUncertain` entry
    /// reads as **not existing** AND **not removed** at the same time. Neither
    /// predicate is the negation of the other.
    /// (`actual_state_of_world_test.go:998-1080`)
    #[test]
    fn test_uncertain_volume_mounts() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let device_path = "fake/device/path";
        let pod1 = get_test_pod("pod1", "pod1uid", "volume-name-1", "fake-device1");
        let volume_spec1 = spec_of(&pod1, 0);
        let generated_volume_name1 = generated_volume_name(&volume_spec1);
        asw.mark_volume_as_attached(
            Some(&generated_volume_name1),
            volume_spec1.clone(),
            "",
            device_path,
        )
        .expect("mark_volume_as_attached failed");
        let pod_name1 = get_unique_pod_name(&pod1);

        let mut opts = mark_volume_opts(&pod1, &generated_volume_name1, volume_spec1.clone());
        opts.block_volume_mapper = None;
        opts.volume_mount_state = VolumeMountState::VolumeMountUncertain;
        asw.add_pod_to_volume(opts)
            .expect("add_pod_to_volume failed");

        assert!(
            !asw.get_mounted_volumes_for_pod(&pod_name1)
                .iter()
                .any(|volume| volume.inner_volume_spec_name == volume_spec1.name()),
            "expected volume {} to be not found in get_mounted_volumes_for_pod",
            volume_spec1.name()
        );

        assert!(
            asw.get_possibly_mounted_volumes_for_pod(&pod_name1)
                .iter()
                .any(|volume| volume.inner_volume_spec_name == volume_spec1.name()),
            "expected volume {} to be found in get_possibly_mounted_volumes_for_pod",
            volume_spec1.name()
        );

        let (vol_exists, _, _) =
            asw.pod_exists_in_volume(&pod_name1, &generated_volume_name1, None, "");
        assert!(
            !vol_exists,
            "expected volume {generated_volume_name1} to not exist in asw"
        );
        assert!(
            !asw.pod_removed_from_volume(&pod_name1, &generated_volume_name1),
            "expected volume {generated_volume_name1} not to be removed in asw"
        );
    }

    // -----------------------------------------------------------------
    // Methods with no upstream test. Each of these is exercised only
    // indirectly (or not at all) by `actual_state_of_world_test.go`; the
    // behaviour asserted is read off upstream's body, cited per test.
    // -----------------------------------------------------------------

    /// No upstream test exists for `AddPodToVolume`'s partial field refresh
    /// (`actual_state_of_world.go:738-772`). A second call on an existing
    /// *certain* entry overwrites `remountRequired`, `volumeMountStateForPod`
    /// and a non-nil `mounter`, and leaves `volumeSpec`, `volumeGIDValue` and
    /// `podUID` alone; the same call on an *uncertain* entry rebuilds all of
    /// them.
    #[test]
    fn test_add_pod_to_volume_partial_refresh_no_upstream_test() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let volume_name = generated_volume_name(&volume_spec);
        asw.mark_volume_as_attached(None, volume_spec.clone(), "", "fake/device/path")
            .expect("mark_volume_as_attached failed");
        let pod_name = get_unique_pod_name(&pod);

        // First call: a new, certain entry.
        let mut opts = mark_volume_opts(&pod, &volume_name, volume_spec.clone());
        opts.volume_gid_volume = "1000".to_string();
        opts.volume_mount_state = VolumeMountState::VolumeMounted;
        asw.add_pod_to_volume(opts)
            .expect("add_pod_to_volume failed");

        // Second call on the certain entry: the GID is NOT refreshed, the
        // mounter IS, and a `None` mounter leaves the old one in place.
        let mut opts = mark_volume_opts(&pod, &volume_name, volume_spec.clone());
        opts.volume_gid_volume = "2000".to_string();
        opts.mounter = Some(Arc::new(FakeMounter { id: "second" }));
        opts.volume_mount_state = VolumeMountState::VolumeMounted;
        asw.add_pod_to_volume(opts)
            .expect("add_pod_to_volume failed");

        let mounted = asw
            .get_mounted_volume_for_pod(&pod_name, &volume_name)
            .expect("volume should be mounted");
        assert_eq!(
            mounted.volume_gid_value, "1000",
            "volume_gid_value must not be refreshed on a certain entry"
        );
        assert_eq!(
            mounted.mounter.as_ref().expect("mounter").get_path(),
            "second",
            "a non-None mounter must always be refreshed"
        );

        let mut opts = mark_volume_opts(&pod, &volume_name, volume_spec.clone());
        opts.volume_gid_volume = "3000".to_string();
        opts.mounter = None;
        opts.volume_mount_state = VolumeMountState::VolumeMounted;
        asw.add_pod_to_volume(opts)
            .expect("add_pod_to_volume failed");
        let mounted = asw
            .get_mounted_volume_for_pod(&pod_name, &volume_name)
            .expect("volume should be mounted");
        assert_eq!(
            mounted.mounter.as_ref().expect("mounter").get_path(),
            "second",
            "a None mounter must not clear the stored one"
        );

        // Now make the entry uncertain and repeat: every field is rebuilt.
        let mut opts = mark_volume_opts(&pod, &volume_name, volume_spec.clone());
        opts.volume_mount_state = VolumeMountState::VolumeMountUncertain;
        asw.mark_volume_mount_as_uncertain(opts)
            .expect("mark_volume_mount_as_uncertain failed");
        let mut opts = mark_volume_opts(&pod, &volume_name, volume_spec);
        opts.volume_gid_volume = "4000".to_string();
        opts.volume_mount_state = VolumeMountState::VolumeMounted;
        asw.add_pod_to_volume(opts)
            .expect("add_pod_to_volume failed");
        let mounted = asw
            .get_mounted_volume_for_pod(&pod_name, &volume_name)
            .expect("volume should be mounted");
        assert_eq!(
            mounted.volume_gid_value, "4000",
            "volume_gid_value must be refreshed on an uncertain entry"
        );
    }

    /// No upstream test exists for `DeleteVolume`
    /// (`actual_state_of_world.go:900-919`) on its own: absent is a no-op,
    /// occupied is an error, empty removes.
    #[test]
    fn test_delete_volume_no_upstream_test() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let volume_name = generated_volume_name(&volume_spec);

        asw.delete_volume(&volume_name)
            .expect("deleting an absent volume is a no-op");

        asw.mark_volume_as_attached(None, volume_spec.clone(), "", "fake/device/path")
            .expect("mark_volume_as_attached failed");
        let pod_name = get_unique_pod_name(&pod);
        asw.add_pod_to_volume(mark_volume_opts(&pod, &volume_name, volume_spec))
            .expect("add_pod_to_volume failed");

        assert_eq!(
            asw.delete_volume(&volume_name)
                .expect_err("a volume with mounted pods must not be deleted"),
            ActualStateOfWorldError::VolumeStillHasMountedPods {
                volume_name: volume_name.to_string(),
                mounted_pods: 1,
            }
        );

        asw.delete_pod_from_volume(&pod_name, &volume_name)
            .expect("delete_pod_from_volume failed");
        asw.delete_volume(&volume_name)
            .expect("delete_volume failed");
        verify_volume_exists_asw(&volume_name, false, &asw);
    }

    /// No upstream test exists for the expansion axis:
    /// `MarkVolumeAsResized` (`:787-798`), `InitializeClaimSize` (`:850-861`),
    /// `GetClaimSize` (`:863-872`), `MarkForInUseExpansionError` (`:622-631`)
    /// and the `volumeNeedsExpansion` branch of `PodExistsInVolume`
    /// (`:969-994`).
    #[test]
    fn test_volume_expansion_no_upstream_test() {
        let mgr = Arc::new(VolumePluginMgr::new(vec![Box::new(FakeVolumePlugin {
            plugin_name: "fake-plugin",
            attachable: true,
            device_mountable: true,
            supports_remount: false,
            requires_fs_resize: true,
        })]));
        let asw = new_asw(mgr);
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let volume_name = generated_volume_name(&volume_spec);
        asw.mark_volume_as_attached(None, volume_spec.clone(), "", "fake/device/path")
            .expect("mark_volume_as_attached failed");
        let pod_name = get_unique_pod_name(&pod);
        let mut opts = mark_volume_opts(&pod, &volume_name, volume_spec);
        opts.volume_mount_state = VolumeMountState::VolumeMounted;
        asw.add_pod_to_volume(opts)
            .expect("add_pod_to_volume failed");

        // Unknown size: `None` is upstream's zero Quantity.
        assert_eq!(asw.get_claim_size(&volume_name), None);

        let one_gi = Quantity::parse("1Gi").expect("quantity");
        let two_gi = Quantity::parse("2Gi").expect("quantity");
        asw.initialize_claim_size(&volume_name, one_gi);
        assert_eq!(asw.get_claim_size(&volume_name), Some(one_gi));
        // Only zero sizes are initialised; a second call is a no-op.
        asw.initialize_claim_size(&volume_name, two_gi);
        assert_eq!(asw.get_claim_size(&volume_name), Some(one_gi));

        // Desired > actual and the plugin requires an FS resize.
        let (exists, _, err) = asw.pod_exists_in_volume(&pod_name, &volume_name, Some(two_gi), "");
        assert!(exists, "the pod exists; the volume merely needs a resize");
        assert!(
            is_fs_resize_required_error(err.as_ref()),
            "expected FsResizeRequired, got {err:?}"
        );
        assert_eq!(
            err,
            Some(ActualStateOfWorldError::FsResizeRequired {
                current_size: Some(one_gi),
                volume_name: volume_name.to_string(),
                pod_name: pod_name.to_string(),
            })
        );

        // An in-use expansion error suppresses the resize request entirely.
        asw.mark_for_in_use_expansion_error(&volume_name);
        let (_, _, err) = asw.pod_exists_in_volume(&pod_name, &volume_name, Some(two_gi), "");
        assert!(err.is_none(), "expected no error, got {err:?}");

        // A completed resize records the new size.
        assert!(asw.mark_volume_as_resized(&volume_name, two_gi));
        assert_eq!(asw.get_claim_size(&volume_name), Some(two_gi));
        assert!(
            !asw.mark_volume_as_resized(
                &UniqueVolumeName("fake-plugin/nonexistent".to_string()),
                two_gi
            ),
            "resizing an unknown volume must report false"
        );
    }

    /// No upstream test exists for the final-expansion-error set:
    /// `MarkVolumeExpansionFailedWithFinalError` (`:410-415`),
    /// `RemoveVolumeFromFailedWithFinalErrors` (`:417-422`) and
    /// `CheckVolumeInFailedExpansionWithFinalErrors` (`:424-429`). The set is
    /// independent of `attachedVolumes` — the volume need not exist.
    #[test]
    fn test_final_expansion_errors_no_upstream_test() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let volume_name = UniqueVolumeName("fake-plugin/fake-device1".to_string());

        assert!(!asw.check_volume_in_failed_expansion_with_final_errors(&volume_name));
        asw.mark_volume_expansion_failed_with_final_error(&volume_name);
        assert!(asw.check_volume_in_failed_expansion_with_final_errors(&volume_name));
        asw.remove_volume_from_failed_with_final_errors(&volume_name);
        assert!(!asw.check_volume_in_failed_expansion_with_final_errors(&volume_name));
    }

    /// No upstream test exists for `MarkRemountRequired`
    /// (`actual_state_of_world.go:800-821`) or for the
    /// `remountRequiredError` arm of `PodExistsInVolume` it feeds.
    #[test]
    fn test_mark_remount_required_no_upstream_test() {
        let mgr = Arc::new(VolumePluginMgr::new(vec![Box::new(FakeVolumePlugin {
            plugin_name: "fake-plugin",
            attachable: true,
            device_mountable: true,
            supports_remount: true,
            requires_fs_resize: false,
        })]));
        let asw = new_asw(mgr);
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let volume_name = generated_volume_name(&volume_spec);
        asw.mark_volume_as_attached(None, volume_spec.clone(), "", "fake/device/path")
            .expect("mark_volume_as_attached failed");
        let pod_name = get_unique_pod_name(&pod);
        let mut opts = mark_volume_opts(&pod, &volume_name, volume_spec);
        opts.volume_mount_state = VolumeMountState::VolumeMounted;
        asw.add_pod_to_volume(opts.clone())
            .expect("add_pod_to_volume failed");

        asw.mark_remount_required(&pod_name);

        let (exists, _, err) = asw.pod_exists_in_volume(&pod_name, &volume_name, None, "");
        assert!(exists, "a remount-required volume still exists");
        assert!(
            is_remount_required_error(err.as_ref()),
            "expected RemountRequired, got {err:?}"
        );

        // A re-add clears it (`:762`).
        asw.add_pod_to_volume(opts)
            .expect("add_pod_to_volume failed");
        let (_, _, err) = asw.pod_exists_in_volume(&pod_name, &volume_name, None, "");
        assert!(err.is_none(), "expected no error, got {err:?}");
    }

    /// No upstream test exists for the device axis on its own:
    /// `GetDeviceMountState` (`:610-620`), `MarkDeviceAsUncertain` (`:555-558`),
    /// `MarkDeviceAsUnmounted` (`:565-568`),
    /// `CheckAndMarkDeviceUncertainViaReconstruction` (`:512-530`),
    /// `IsVolumeDeviceReconstructed` (`:449-454`),
    /// `UpdateReconstructedDevicePath` (`:570-586`) and
    /// `AttachedVolume::DeviceMayBeMounted` (`:217-220`).
    #[test]
    fn test_device_mount_state_no_upstream_test() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let volume_name = generated_volume_name(&volume_spec);

        // An unknown volume reads as not mounted and cannot be marked.
        assert_eq!(
            asw.get_device_mount_state(&volume_name),
            DeviceMountState::DeviceNotMounted
        );
        assert!(!asw.check_and_mark_device_uncertain_via_reconstruction(&volume_name, "fake/path"));
        assert!(!asw.is_volume_device_reconstructed(&volume_name));
        assert!(asw.get_attached_volume(&volume_name).is_none());
        assert_eq!(
            asw.mark_device_as_unmounted(&volume_name)
                .expect_err("an unknown volume cannot be marked"),
            ActualStateOfWorldError::VolumeNotInAttachedVolumes {
                volume_name: volume_name.to_string(),
            }
        );

        asw.mark_volume_as_attached(None, volume_spec, "", "fake/device/path")
            .expect("mark_volume_as_attached failed");

        assert!(asw.check_and_mark_device_uncertain_via_reconstruction(&volume_name, "fake/mount"));
        assert_eq!(
            asw.get_device_mount_state(&volume_name),
            DeviceMountState::DeviceMountUncertain
        );
        // Not a second time — the state is no longer DeviceNotMounted.
        assert!(!asw.check_and_mark_device_uncertain_via_reconstruction(&volume_name, "other"));
        assert!(
            asw.get_attached_volume(&volume_name)
                .expect("attached")
                .device_may_be_mounted(),
            "an uncertain device may be mounted"
        );

        // `UpdateReconstructedDevicePath` applies only while uncertain.
        asw.update_reconstructed_device_path(&volume_name, "corrected/device/path");
        assert_eq!(
            asw.get_attached_volume(&volume_name)
                .expect("attached")
                .device_path,
            "corrected/device/path"
        );

        asw.mark_device_as_mounted(&volume_name, "", "fake/mount", "")
            .expect("mark_device_as_mounted failed");
        asw.update_reconstructed_device_path(&volume_name, "ignored");
        assert_eq!(
            asw.get_attached_volume(&volume_name)
                .expect("attached")
                .device_path,
            "corrected/device/path",
            "a certain device path must not be overwritten"
        );

        asw.mark_device_as_uncertain(&volume_name, "", "fake/mount", "")
            .expect("mark_device_as_uncertain failed");
        assert_eq!(
            asw.get_device_mount_state(&volume_name),
            DeviceMountState::DeviceMountUncertain
        );

        asw.mark_device_as_unmounted(&volume_name)
            .expect("mark_device_as_unmounted failed");
        assert_eq!(
            asw.get_device_mount_state(&volume_name),
            DeviceMountState::DeviceNotMounted
        );
        let attached = asw.get_attached_volume(&volume_name).expect("attached");
        assert!(!attached.device_may_be_mounted());
        assert_eq!(
            attached.device_path, "corrected/device/path",
            "an empty device_path argument must not clear the stored one"
        );
        assert_eq!(attached.device_mount_path, "");
    }

    /// No upstream test exists for `GetVolumeMountState` (`:633-647`),
    /// `GetAllMountedVolumes` (`:1059-1075`), `PodHasMountedVolumes`
    /// (`:955-967`) or `GetMountedVolumeForPod` (`:1095-1107`) across the
    /// three mount states.
    #[test]
    fn test_mount_state_getters_no_upstream_test() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let volume_name = generated_volume_name(&volume_spec);
        let pod_name = get_unique_pod_name(&pod);

        // Absent volume and absent pod entry both read as VolumeNotMounted.
        assert_eq!(
            asw.get_volume_mount_state(&volume_name, &pod_name),
            VolumeMountState::VolumeNotMounted
        );
        assert!(!asw.pod_has_mounted_volumes(&pod_name));
        assert!(asw
            .get_mounted_volume_for_pod(&pod_name, &volume_name)
            .is_none());

        asw.mark_volume_as_attached(None, volume_spec.clone(), "", "fake/device/path")
            .expect("mark_volume_as_attached failed");
        assert_eq!(
            asw.get_volume_mount_state(&volume_name, &pod_name),
            VolumeMountState::VolumeNotMounted
        );

        let mut opts = mark_volume_opts(&pod, &volume_name, volume_spec.clone());
        opts.volume_mount_state = VolumeMountState::VolumeMountUncertain;
        asw.add_pod_to_volume(opts)
            .expect("add_pod_to_volume failed");
        assert_eq!(
            asw.get_volume_mount_state(&volume_name, &pod_name),
            VolumeMountState::VolumeMountUncertain
        );
        assert!(
            !asw.pod_has_mounted_volumes(&pod_name),
            "uncertain is not mounted"
        );
        assert_eq!(asw.get_mounted_volumes().len(), 0);
        assert_eq!(asw.get_all_mounted_volumes().len(), 1);
        assert!(asw
            .get_mounted_volume_for_pod(&pod_name, &volume_name)
            .is_none());

        let mut opts = mark_volume_opts(&pod, &volume_name, volume_spec);
        opts.volume_mount_state = VolumeMountState::VolumeMounted;
        asw.add_pod_to_volume(opts)
            .expect("add_pod_to_volume failed");
        assert_eq!(
            asw.get_volume_mount_state(&volume_name, &pod_name),
            VolumeMountState::VolumeMounted
        );
        assert!(asw.pod_has_mounted_volumes(&pod_name));
        assert_eq!(asw.get_mounted_volumes().len(), 1);
        assert_eq!(asw.get_all_mounted_volumes().len(), 1);
        assert!(asw
            .get_mounted_volume_for_pod(&pod_name, &volume_name)
            .is_some());
        assert!(!asw.pod_removed_from_volume(&pod_name, &volume_name));
    }

    /// No upstream test exists for the three attacher-updater no-ops —
    /// `MarkVolumeAsUncertain` (`:400-403`), `AddVolumeToReportAsAttached`
    /// (`:536-538`) and `RemoveVolumeFromReportAsAttached` (`:540-543`) — nor
    /// for `IsVolumeNotAttachedError` (`:237-241`). They exist to satisfy the
    /// operation executor's interface on the kubelet side, where the
    /// attach/detach controller owns `Node.Status.VolumesAttached`.
    #[test]
    fn test_attacher_updater_noops_no_upstream_test() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let volume_name = generated_volume_name(&volume_spec);
        let pod_name = get_unique_pod_name(&pod);

        asw.mark_volume_as_uncertain(&volume_name, volume_spec, "")
            .expect("mark_volume_as_uncertain never errors");
        asw.add_volume_to_report_as_attached(&volume_name, "");
        asw.remove_volume_from_report_as_attached(&volume_name, "")
            .expect("remove_volume_from_report_as_attached never errors");
        // None of them recorded anything.
        verify_volume_exists_asw(&volume_name, false, &asw);

        let (_, _, err) = asw.pod_exists_in_volume(&pod_name, &volume_name, None, "");
        assert!(is_volume_not_attached_error(err.as_ref()));
        assert!(!is_volume_not_attached_error(None));
    }

    /// No upstream test exists for `CheckAndMarkVolumeAsUncertainViaReconstruction`
    /// (`:456-510`) refusing to overwrite an existing entry, nor for the
    /// `attachedVolumes`-miss arm.
    #[test]
    fn test_check_and_mark_volume_as_uncertain_refuses_overwrite_no_upstream_test() {
        let asw = new_asw(get_test_kubelet_volume_plugin_mgr());
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let volume_name = generated_volume_name(&volume_spec);
        let mut opts = mark_volume_opts(&pod, &volume_name, volume_spec.clone());
        opts.volume_mount_state = VolumeMountState::VolumeMountUncertain;

        // Volume not attached at all.
        assert!(!asw
            .check_and_mark_volume_as_uncertain_via_reconstruction(opts.clone())
            .expect("never errors"));

        asw.mark_volume_as_attached(None, volume_spec.clone(), "", "fake/device/path")
            .expect("mark_volume_as_attached failed");
        assert!(asw
            .check_and_mark_volume_as_uncertain_via_reconstruction(opts.clone())
            .expect("never errors"));
        // Already uncertain — refuse.
        assert!(!asw
            .check_and_mark_volume_as_uncertain_via_reconstruction(opts.clone())
            .expect("never errors"));

        // Already mounted — refuse.
        let mut mounted = mark_volume_opts(&pod, &volume_name, volume_spec);
        mounted.volume_mount_state = VolumeMountState::VolumeMounted;
        asw.add_pod_to_volume(mounted)
            .expect("add_pod_to_volume failed");
        assert!(!asw
            .check_and_mark_volume_as_uncertain_via_reconstruction(opts)
            .expect("never errors"));
    }

    /// No upstream test exists for `newAttachedVolume`'s field mapping
    /// (`:1186-1210`) — in particular that `node_name` is the cache's own node
    /// and `plugin_is_attachable` collapses `False`/`Uncertain` alike.
    #[test]
    fn test_new_attached_volume_field_mapping_no_upstream_test() {
        let mgr = Arc::new(VolumePluginMgr::new(vec![Box::new(FakeVolumePlugin {
            plugin_name: "fake-plugin",
            attachable: false,
            device_mountable: false,
            supports_remount: false,
            requires_fs_resize: false,
        })]));
        let asw = new_asw(mgr);
        let pod = get_test_pod("pod1", "pod1uid", "volume-name", "fake-device1");
        let volume_spec = spec_of(&pod, 0);
        let volume_name = generated_volume_name(&volume_spec);
        asw.mark_volume_as_attached(None, volume_spec, "", "fake/device/path")
            .expect("mark_volume_as_attached failed");

        let attached = asw.get_attached_volume(&volume_name).expect("attached");
        assert_eq!(attached.volume_name, volume_name);
        assert_eq!(attached.node_name, "mynode");
        assert_eq!(attached.plugin_name, "fake-plugin");
        assert_eq!(attached.device_path, "fake/device/path");
        assert_eq!(attached.device_mount_path, "");
        assert!(
            !attached.plugin_is_attachable,
            "a non-attachable plugin reads as false"
        );
        assert_eq!(
            attached.device_mount_state,
            DeviceMountState::DeviceNotMounted
        );
        assert_eq!(asw.get_attached_volumes().len(), 1);
    }

    /// No upstream test exists for `is_zero_quantity`, the helper that stands
    /// in for Go's `resource.Quantity.IsZero()` on a value that may be absent.
    #[test]
    fn test_is_zero_quantity_no_upstream_test() {
        assert!(is_zero_quantity(None));
        assert!(is_zero_quantity(Some(Quantity::from_value(
            0,
            Format::BinarySI
        ))));
        assert!(!is_zero_quantity(Some(
            Quantity::parse("1Gi").expect("quantity")
        )));
    }
}
