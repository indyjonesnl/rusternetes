//! Unit tests moved from `src/volume_manager/cache/desired_state_of_world.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::feature_gates::{self, Feature};
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_common::quota::pod_limits;
    pub use rusternetes_common::resources::pod::SELinuxOptions;
    pub use rusternetes_common::resources::volume::PersistentVolumeAccessMode;
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_kubelet::volume_manager::cache::desired_state_of_world_selinux_metrics::{
        register_selinux_metrics, GaugeVec, SELINUX_CONTAINER_CONTEXT_ERRORS,
        SELINUX_CONTAINER_CONTEXT_WARNINGS, SELINUX_POD_CONTEXT_MISMATCH_ERRORS,
        SELINUX_POD_CONTEXT_MISMATCH_WARNINGS, SELINUX_VOLUMES_ADMITTED,
        SELINUX_VOLUME_CONTEXT_MISMATCH_ERRORS, SELINUX_VOLUME_CONTEXT_MISMATCH_WARNINGS,
    };
    pub use rusternetes_kubelet::volume_plugins::plugin::{OwnedSpec, Spec, VolumePlugin};
    pub use rusternetes_kubelet::volume_plugins::registry::VolumePluginMgr;
    pub use rusternetes_kubelet::volume_plugins::util::operation_executor;
    pub use rusternetes_kubelet::volume_plugins::util::selinux::{
        get_mount_selinux_label, volume_supports_selinux_mount, SELinuxLabelError,
        SELinuxLabelTranslator,
    };
    pub use rusternetes_kubelet::volume_plugins::util::types::{UniquePodName, UniqueVolumeName};
    pub use rusternetes_kubelet::volume_plugins::util::{
        contains_access_mode, get_unique_volume_name_from_spec,
        get_unique_volume_name_from_spec_with_pod, is_attachable_volume,
        is_device_mountable_volume, is_local_ephemeral_volume,
    };
    pub use std::collections::{BTreeSet, HashMap, HashSet};
    pub use std::ops::{Deref, DerefMut};
    pub use std::sync::{Arc, RwLock};
    pub use std::time::SystemTime;
    pub use tracing::debug;
}

mod tests {
    //! Ports of `pkg/kubelet/volumemanager/cache/desired_state_of_world_test.go`.
    //! Each test keeps the name of the Go test it came from.
    //!
    //! **Two substitutions run through every test here.**
    //!
    //! 1. *GCE PersistentDisk -> NFS.* Upstream's fixtures use
    //!    `v1.GCEPersistentDiskVolumeSource{PDName: "fake-device1"}` purely as
    //!    "a volume source whose identity the fake plugin can read". This
    //!    project's `Volume` has no `gcePersistentDisk` field, so `nfs` plays
    //!    that role and `nfs.path` carries the `fake-deviceN` identity.
    //!    Nothing about the volume kind matters to `DesiredStateOfWorld`.
    //!
    //! 2. *PV-only specs.* The SELinux tests build
    //!    `&volume.Spec{PersistentVolume: ...}` with a nil `Volume`, which
    //!    [`Spec`] cannot express (see [`Spec::name`]). Those tests pass the
    //!    pod's own PVC volume entry alongside the PV, so `spec.name()` is the
    //!    pod-side name (`"volume-name"`) rather than upstream's PV name
    //!    (`"basicPV"`). Only the generated string differs; every assertion is
    //!    on behaviour that does not read it.

    use super::__parent::*;
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use rusternetes_common::feature_gates::with_feature;
    use rusternetes_common::resources::{PersistentVolume, Volume};
    use rusternetes_kubelet::volume_manager::cache::desired_state_of_world::*;
    use rusternetes_kubelet::volume_plugins::plugin::Mounter;
    use rusternetes_kubelet::volume_plugins::util::get_unique_pod_name;
    use rusternetes_kubelet::volume_plugins::util::selinux::FakeSELinuxLabelTranslator;
    use serde_json::json;
    use std::collections::HashSet;

    /// Port of `volumetesting.FakeVolumePlugin`
    /// (`pkg/volume/testing/testing.go:~250-300`), trimmed to what the
    /// `DesiredStateOfWorld` tests exercise, with
    /// `FakeBasicVolumePlugin.CanSupport` (`testing.go:547-552`) folded in as
    /// `can_support_prefix`.
    struct FakeVolumePlugin {
        plugin_name: &'static str,
        /// `None` is upstream's nil `CanSupportFn`, i.e. support everything
        /// (`testing.go:279-285`). `Some(prefix)` is
        /// `FakeBasicVolumePlugin.CanSupport`: `strings.HasPrefix(spec.Name(), prefix)`.
        can_support_prefix: Option<&'static str>,
        /// `FakeVolumePlugin.SupportsRemount` (`testing.go:287-289`).
        supports_remount: bool,
        /// `FakeVolumePlugin.SupportsSELinux` (`testing.go:295-297`).
        supports_selinux: bool,
        /// Stands for registering the plugin as a
        /// `FakeAttachableVolumePlugin` (`testing.go:609-...`).
        attachable: bool,
        /// Stands for registering the plugin as a
        /// `FakeDeviceMountableVolumePlugin`.
        device_mountable: bool,
    }

    impl FakeVolumePlugin {
        fn basic(plugin_name: &'static str) -> Self {
            Self {
                plugin_name,
                can_support_prefix: Some(plugin_name),
                supports_remount: false,
                supports_selinux: false,
                attachable: false,
                device_mountable: false,
            }
        }
    }

    #[async_trait]
    impl VolumePlugin for FakeVolumePlugin {
        fn name(&self) -> &'static str {
            self.plugin_name
        }

        /// `FakeVolumePlugin.GetVolumeName` (`testing.go:258-277`), with NFS in
        /// GCE PD's place and the `if volumeName == "" { volumeName = spec.Name() }`
        /// fallback that makes the empty-`VolumeSource` fixtures work.
        fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
            let mut volume_name = String::new();
            if let Some(nfs) = &spec.volume.nfs {
                volume_name = nfs.path.clone();
            } else if let Some(nfs) = spec.persistent_volume.and_then(|pv| pv.spec.nfs.as_ref()) {
                volume_name = nfs.path.clone();
            } else if let Some(csi) = &spec.volume.csi {
                volume_name = csi.driver.clone();
            }
            if volume_name.is_empty() {
                volume_name = spec.name().to_string();
            }
            Ok(volume_name)
        }

        fn can_support(&self, spec: &Spec<'_>) -> bool {
            match self.can_support_prefix {
                Some(prefix) => spec.name().starts_with(prefix),
                None => true,
            }
        }

        fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
            self.supports_remount
        }

        fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
            Ok(self.supports_selinux)
        }

        fn can_attach(&self, _spec: &Spec<'_>) -> Result<bool> {
            Ok(self.attachable)
        }

        fn can_device_mount(&self, _spec: &Spec<'_>) -> bool {
            self.device_mountable
        }

        async fn new_mounter(&self, _spec: &Spec<'_>, _pod: &Pod) -> Result<Box<dyn Mounter>> {
            Err(anyhow!("DSW tests never mount"))
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

    /// Port of `volumetesting.GetTestKubeletVolumePluginMgr`
    /// (`pkg/volume/testing/testing.go:1671-1680`): one `FakeVolumePlugin`
    /// named `fake-plugin` that supports every spec and is both attachable and
    /// device-mountable, so every volume gets the plugin-scoped unique name.
    fn get_test_kubelet_volume_plugin_mgr() -> Arc<VolumePluginMgr> {
        Arc::new(VolumePluginMgr::new(vec![Box::new(FakeVolumePlugin {
            plugin_name: "fake-plugin",
            can_support_prefix: None,
            supports_remount: false,
            supports_selinux: false,
            attachable: true,
            device_mountable: true,
        })]))
    }

    fn new_dsw(mgr: Arc<VolumePluginMgr>) -> DesiredStateOfWorld {
        DesiredStateOfWorld::new(mgr, Arc::new(FakeSELinuxLabelTranslator))
    }

    fn pod(value: serde_json::Value) -> Arc<Pod> {
        Arc::new(serde_json::from_value(value).expect("pod fixture"))
    }

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

    /// `nfs` stands in for upstream's `gcePersistentDisk`; `path` carries the
    /// `fake-deviceN` identity the fake plugin reads.
    fn nfs_volume(name: &str, device: &str) -> serde_json::Value {
        json!({ "name": name, "nfs": { "server": "fake-server", "path": device } })
    }

    // ---- verify helpers, ports of `desired_state_of_world_test.go:1429-1576` ----

    fn verify_volume_exists_dsw(
        expected_volume_name: &UniqueVolumeName,
        expected_selinux_context: &str,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            dsw.volume_exists(expected_volume_name, expected_selinux_context),
            "VolumeExists({expected_volume_name}) failed. Expected: <true> Actual: <false>"
        );
    }

    fn verify_volume_doesnt_exist(
        expected_volume_name: &UniqueVolumeName,
        expected_selinux_context: &str,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            !dsw.volume_exists(expected_volume_name, expected_selinux_context),
            "VolumeExists({expected_volume_name}) returned incorrect value. \
             Expected: <false> Actual: <true>"
        );
    }

    fn verify_volume_exists_in_volumes_to_mount(
        expected_volume_name: &UniqueVolumeName,
        expected_outer_names: &[&str],
        expect_reported_in_use: bool,
        dsw: &DesiredStateOfWorld,
    ) {
        let volumes_to_mount = dsw.get_volumes_to_mount();
        for volume in &volumes_to_mount {
            if &volume.volume_name == expected_volume_name {
                assert_eq!(
                    volume.reported_in_use, expect_reported_in_use,
                    "Found volume {expected_volume_name} in the list of VolumesToMount, \
                     but ReportedInUse incorrect"
                );
                let mut names = volume.outer_volume_spec_names.clone();
                names.sort();
                assert_eq!(
                    names, expected_outer_names,
                    "Expected outer volume spec names to be {expected_outer_names:?}"
                );
                return;
            }
        }
        panic!(
            "Could not find volume {expected_volume_name} in the list of desired state of \
             world volumes to mount"
        );
    }

    fn verify_volume_doesnt_exist_in_volumes_to_mount(
        volume_to_check: &UniqueVolumeName,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            !dsw.get_volumes_to_mount()
                .iter()
                .any(|v| &v.volume_name == volume_to_check),
            "Found volume {volume_to_check} in the list of desired state of world volumes \
             to mount. Expected it not to exist."
        );
    }

    fn verify_pod_exists_in_volume_dsw(
        expected_pod_name: &UniquePodName,
        expected_volume_name: &UniqueVolumeName,
        expected_selinux_context: &str,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            dsw.pod_exists_in_volume(
                expected_pod_name,
                expected_volume_name,
                expected_selinux_context
            ),
            "DSW PodExistsInVolume returned incorrect value. Expected: <true> Actual: <false>"
        );
    }

    fn verify_pod_doesnt_exist_in_volume_dsw(
        expected_pod_name: &UniquePodName,
        expected_volume_name: &UniqueVolumeName,
        expected_selinux_context: &str,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            !dsw.pod_exists_in_volume(
                expected_pod_name,
                expected_volume_name,
                expected_selinux_context
            ),
            "DSW PodExistsInVolume returned incorrect value. Expected: <false> Actual: <true>"
        );
    }

    fn verify_volume_exists_with_spec_name_in_volume_dsw(
        expected_pod_name: &UniquePodName,
        expected_volume_spec_name: &str,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            dsw.volume_exists_with_spec_name(expected_pod_name, expected_volume_spec_name),
            "DSW VolumeExistsWithSpecName returned incorrect value. \
             Expected: <true> Actual: <false>"
        );
    }

    fn verify_volume_doesnt_exist_with_spec_name_in_volume_dsw(
        expected_pod_name: &UniquePodName,
        expected_volume_spec_name: &str,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            !dsw.volume_exists_with_spec_name(expected_pod_name, expected_volume_spec_name),
            "DSW VolumeExistsWithSpecName returned incorrect value. \
             Expected: <false> Actual: <true>"
        );
    }

    /// Port of `verifyDesiredSizeLimitInVolumeDsw` (`:1550-1576`).
    fn verify_desired_size_limit_in_volume_dsw(
        expected_pod_name: &UniquePodName,
        expected_desired_size_map: &[(&str, i128)],
        dsw: &DesiredStateOfWorld,
    ) {
        let volumes_to_mount = dsw.get_volumes_to_mount();
        for (volume_name, expected_desired_size) in expected_desired_size_map {
            assert!(
                dsw.volume_exists_with_spec_name(expected_pod_name, volume_name),
                "DSW VolumeExistsWithSpecName returned incorrect value for {volume_name}"
            );
            for v in &volumes_to_mount {
                if v.volume_spec.name() == *volume_name && &v.pod_name == expected_pod_name {
                    let actual = v.desired_size_limit.map(|q| q.value());
                    assert_eq!(
                        actual,
                        Some(*expected_desired_size),
                        "Found volume {volume_name} in the list of VolumesToMount, but \
                         DesiredSizeLimit incorrect"
                    );
                }
            }
        }
    }

    // ---------------------------------------------------------------- tests

    /// <- `Test_AddPodToVolume_Positive_NewPodNewVolume` (`:42`).
    #[test]
    fn test_add_pod_to_volume_positive_new_pod_new_volume() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let pod = pod(json!({
            "metadata": { "name": "pod3", "uid": "pod3uid" },
            "spec": { "volumes": [nfs_volume("volume-name", "fake-device1")] },
        }));

        let volume_spec = spec_of(&pod, 0);
        let pod_name = get_unique_pod_name(&pod);

        let generated_volume_name = dsw
            .add_pod_to_volume(
                &pod_name,
                Arc::clone(&pod),
                Arc::clone(&volume_spec),
                volume_spec.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed");

        verify_volume_exists_dsw(&generated_volume_name, "", &dsw);
        verify_volume_exists_in_volumes_to_mount(
            &generated_volume_name,
            &["volume-name"],
            false,
            &dsw,
        );
        verify_pod_exists_in_volume_dsw(&pod_name, &generated_volume_name, "", &dsw);
        verify_volume_exists_with_spec_name_in_volume_dsw(&pod_name, volume_spec.name(), &dsw);
    }

    /// <- `Test_AddPodToVolume_Positive_ExistingPodExistingVolume` (`:89`).
    ///
    /// Pins that a re-add does not create a duplicate entry and does not change
    /// the generated unique name.
    #[test]
    fn test_add_pod_to_volume_positive_existing_pod_existing_volume() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let pod = pod(json!({
            "metadata": { "name": "pod4", "uid": "pod4uid" },
            "spec": { "volumes": [nfs_volume("volume-name", "fake-device1")] },
        }));

        let volume_spec = spec_of(&pod, 0);
        let pod_name = get_unique_pod_name(&pod);

        let add = || {
            dsw.add_pod_to_volume(
                &pod_name,
                Arc::clone(&pod),
                Arc::clone(&volume_spec),
                volume_spec.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed")
        };
        let generated_volume_name = add();
        let generated_volume_name2 = add();

        assert_eq!(
            generated_volume_name, generated_volume_name2,
            "AddPodToVolume should generate same names"
        );
        verify_volume_exists_dsw(&generated_volume_name, "", &dsw);
        verify_volume_exists_in_volumes_to_mount(
            &generated_volume_name,
            &["volume-name"],
            false,
            &dsw,
        );
        verify_pod_exists_in_volume_dsw(&pod_name, &generated_volume_name, "", &dsw);
        verify_volume_exists_with_spec_name_in_volume_dsw(&pod_name, volume_spec.name(), &dsw);
    }

    /// <- `Test_AddPodToVolume_Positive_NamesForDifferentPodsAndDifferentVolumes` (`:143`).
    ///
    /// The attachable / device-mountable / neither branch of the unique-name
    /// construction: the first two make two pods share one cache entry, the
    /// third gives each pod its own.
    #[test]
    fn test_add_pod_to_volume_positive_names_for_different_pods_and_different_volumes() {
        let mgr = Arc::new(VolumePluginMgr::new(vec![
            Box::new(FakeVolumePlugin::basic("basic")),
            Box::new(FakeVolumePlugin {
                device_mountable: true,
                ..FakeVolumePlugin::basic("device-mountable")
            }),
            Box::new(FakeVolumePlugin {
                attachable: true,
                device_mountable: true,
                ..FakeVolumePlugin::basic("attachable")
            }),
        ]));
        let dsw = new_dsw(mgr);

        // (volume name, same unique name across the two pods?)
        let testcases = [
            ("basic", false),
            ("device-mountable", true),
            ("attachable", true),
        ];

        for (volume_name, same) in testcases {
            let pod1 = pod(json!({
                "metadata": { "name": "pod1", "uid": "pod1uid" },
                "spec": { "volumes": [{ "name": volume_name }] },
            }));
            let pod2 = pod(json!({
                "metadata": { "name": "pod2", "uid": "pod2uid" },
                "spec": { "volumes": [{ "name": volume_name }] },
            }));
            let spec1 = spec_of(&pod1, 0);
            let spec2 = spec_of(&pod2, 0);

            let name1 = dsw
                .add_pod_to_volume(
                    &get_unique_pod_name(&pod1),
                    Arc::clone(&pod1),
                    Arc::clone(&spec1),
                    spec1.name(),
                    "",
                    &[],
                )
                .unwrap_or_else(|e| panic!("test {volume_name:?}: AddPodToVolume failed: {e}"));
            let name2 = dsw
                .add_pod_to_volume(
                    &get_unique_pod_name(&pod2),
                    Arc::clone(&pod2),
                    Arc::clone(&spec2),
                    spec2.name(),
                    "",
                    &[],
                )
                .unwrap_or_else(|e| panic!("test {volume_name:?}: AddPodToVolume failed: {e}"));

            if same {
                assert_eq!(
                    name1, name2,
                    "test {volume_name:?}: AddPodToVolume should generate same names"
                );
            } else {
                assert_ne!(
                    name1, name2,
                    "test {volume_name:?}: AddPodToVolume should generate different names"
                );
            }
        }
    }

    /// <- `Test_AddPodToVolume_Positive_MultiOuterNames` (`:304`).
    ///
    /// Two `podSpec.volumes` entries of ONE pod resolving to the same unique
    /// volume: both outer names must survive against the single entry. A port
    /// that stores one `outer_volume_spec_name` string fails here.
    #[test]
    fn test_add_pod_to_volume_positive_multi_outer_names() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let pod = pod(json!({
            "metadata": { "name": "pod5", "uid": "pod5uid" },
            "spec": { "volumes": [
                nfs_volume("volume-name-1", "fake-device1"),
                nfs_volume("volume-name-2", "fake-device1"),
            ] },
        }));

        let spec1 = spec_of(&pod, 0);
        let spec2 = spec_of(&pod, 1);
        let pod_name = get_unique_pod_name(&pod);

        dsw.add_pod_to_volume(
            &pod_name,
            Arc::clone(&pod),
            Arc::clone(&spec1),
            spec1.name(),
            "",
            &[],
        )
        .expect("AddPodToVolume failed");
        let generated_volume_name = dsw
            .add_pod_to_volume(
                &pod_name,
                Arc::clone(&pod),
                Arc::clone(&spec2),
                spec2.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed");

        verify_volume_exists_in_volumes_to_mount(
            &generated_volume_name,
            &["volume-name-1", "volume-name-2"],
            false,
            &dsw,
        );
    }

    /// <- `Test_DeletePodFromVolume_Positive_PodExistsVolumeExists` (`:360`).
    ///
    /// Pins the cascading delete: removing the last pod removes the volume.
    #[test]
    fn test_delete_pod_from_volume_positive_pod_exists_volume_exists() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let pod = pod(json!({
            "metadata": { "name": "pod3", "uid": "pod3uid" },
            "spec": { "volumes": [nfs_volume("volume-name", "fake-device1")] },
        }));

        let volume_spec = spec_of(&pod, 0);
        let pod_name = get_unique_pod_name(&pod);
        let generated_volume_name = dsw
            .add_pod_to_volume(
                &pod_name,
                Arc::clone(&pod),
                Arc::clone(&volume_spec),
                volume_spec.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed");
        verify_volume_exists_dsw(&generated_volume_name, "", &dsw);
        verify_volume_exists_in_volumes_to_mount(
            &generated_volume_name,
            &["volume-name"],
            false,
            &dsw,
        );
        verify_pod_exists_in_volume_dsw(&pod_name, &generated_volume_name, "", &dsw);

        dsw.delete_pod_from_volume(&pod_name, &generated_volume_name);

        verify_volume_doesnt_exist(&generated_volume_name, "", &dsw);
        verify_volume_doesnt_exist_in_volumes_to_mount(&generated_volume_name, &dsw);
        verify_pod_doesnt_exist_in_volume_dsw(&pod_name, &generated_volume_name, "", &dsw);
        verify_volume_doesnt_exist_with_spec_name_in_volume_dsw(
            &pod_name,
            volume_spec.name(),
            &dsw,
        );
    }

    /// <- `Test_MarkVolumesReportedInUse_Positive_NewPodNewVolume` (`:414`).
    ///
    /// Pins the full-resync semantics: a volume left out of the list is reset
    /// to `false`, not left alone.
    #[test]
    fn test_mark_volumes_reported_in_use_positive_new_pod_new_volume() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());

        let mut pods = Vec::new();
        let mut specs = Vec::new();
        let mut pod_names = Vec::new();
        let mut generated = Vec::new();
        for i in 1..=3 {
            let p = pod(json!({
                "metadata": { "name": format!("pod{i}"), "uid": format!("pod{i}uid") },
                "spec": { "volumes": [
                    nfs_volume(&format!("volume{i}-name"), &format!("fake-device{i}")),
                ] },
            }));
            let spec = spec_of(&p, 0);
            let pod_name = get_unique_pod_name(&p);
            let name = dsw
                .add_pod_to_volume(
                    &pod_name,
                    Arc::clone(&p),
                    Arc::clone(&spec),
                    spec.name(),
                    "",
                    &[],
                )
                .expect("AddPodToVolume failed");
            pods.push(p);
            specs.push(spec);
            pod_names.push(pod_name);
            generated.push(name);
        }

        let assert_only = |reported_index: usize, dsw: &DesiredStateOfWorld| {
            for i in 0..3 {
                verify_volume_exists_dsw(&generated[i], "", dsw);
                verify_volume_exists_in_volumes_to_mount(
                    &generated[i],
                    &[&format!("volume{}-name", i + 1)],
                    i == reported_index,
                    dsw,
                );
                verify_pod_exists_in_volume_dsw(&pod_names[i], &generated[i], "", dsw);
            }
        };

        dsw.mark_volumes_reported_in_use(&[generated[1].clone()]);
        assert_only(1, &dsw);

        dsw.mark_volumes_reported_in_use(&[generated[2].clone()]);
        assert_only(2, &dsw);
    }

    /// <- `Test_AddPodToVolume_WithEmptyDirSizeLimit` (`:542`).
    ///
    /// The desired size limit is the pod's ephemeral-storage limit, lowered by
    /// the emptyDir's own `sizeLimit` only when that is smaller (or when the
    /// pod has no limit at all).
    #[test]
    fn test_add_pod_to_volume_with_empty_dir_size_limit() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        const GI: i128 = 1024 * 1024 * 1024;

        let pod1 = pod(json!({
            "metadata": { "name": "pod1", "uid": "pod1uid" },
            "spec": {
                "containers": [
                    { "name": "c1", "resources": { "limits": { "ephemeral-storage": "1Gi" } } },
                    { "name": "c2", "resources": { "limits": { "ephemeral-storage": "1Gi" } } },
                ],
                "volumes": [
                    { "name": "emptyDir1", "emptyDir": { "sizeLimit": "1Gi" } },
                    { "name": "emptyDir2", "emptyDir": { "sizeLimit": "2Gi" } },
                    { "name": "emptyDir3", "emptyDir": { "sizeLimit": "3Gi" } },
                    { "name": "emptyDir4", "emptyDir": {} },
                ],
            },
        }));
        let pod1_name = get_unique_pod_name(&pod1);
        let pod1_expected = [
            ("emptyDir1", GI),
            ("emptyDir2", 2 * GI),
            ("emptyDir3", 2 * GI),
            ("emptyDir4", 2 * GI),
        ];

        let pod2 = pod(json!({
            "metadata": { "name": "pod2", "uid": "pod2uid" },
            "spec": {
                "volumes": [
                    { "name": "emptyDir5", "emptyDir": { "sizeLimit": "1Gi" } },
                    { "name": "emptyDir6", "emptyDir": { "sizeLimit": "2Gi" } },
                    { "name": "emptyDir7", "emptyDir": { "sizeLimit": "3Gi" } },
                    { "name": "emptyDir8", "emptyDir": {} },
                ],
            },
        }));
        let pod2_name = get_unique_pod_name(&pod2);
        let pod2_expected = [
            ("emptyDir5", GI),
            ("emptyDir6", 2 * GI),
            ("emptyDir7", 3 * GI),
            ("emptyDir8", 0),
        ];

        for (p, name) in [(&pod1, &pod1_name), (&pod2, &pod2_name)] {
            let count = p.spec.as_ref().unwrap().volumes.as_ref().unwrap().len();
            for i in 0..count {
                let spec = spec_of(p, i);
                dsw.add_pod_to_volume(name, Arc::clone(p), Arc::clone(&spec), spec.name(), "", &[])
                    .expect("AddPodToVolume failed");
            }
        }

        verify_desired_size_limit_in_volume_dsw(&pod1_name, &pod1_expected, &dsw);
        verify_desired_size_limit_in_volume_dsw(&pod2_name, &pod2_expected, &dsw);
    }

    /// <- `TestGetVolumeNamesForPod` (`:1333`).
    ///
    /// The returned map is keyed by OUTER name, so one unique volume reached
    /// through two of a pod's `volumes` entries appears twice.
    #[test]
    fn test_get_volume_names_for_pod() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());

        let pod1 = pod(json!({
            "metadata": { "name": "pod1", "uid": "pod1-uid" },
            "spec": { "volumes": [
                nfs_volume("volume1-name", "fake-device1"),
                nfs_volume("volume1-dup", "fake-device1"),
                nfs_volume("volume3-name", "fake-device3"),
            ] },
        }));
        let pod2 = pod(json!({
            "metadata": { "name": "pod2", "uid": "pod2-uid" },
            "spec": { "volumes": [nfs_volume("volume2-name", "fake-device2")] },
        }));

        let add_volume = |p: &Arc<Pod>, index: usize| {
            let spec = spec_of(p, index);
            dsw.add_pod_to_volume(
                &get_unique_pod_name(p),
                Arc::clone(p),
                Arc::clone(&spec),
                spec.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed");
        };
        add_volume(&pod1, 0);
        add_volume(&pod1, 1);
        add_volume(&pod1, 2);
        add_volume(&pod2, 0);

        let expect = |p: &Arc<Pod>, expected: &[(&str, &str)]| {
            let actual = dsw.get_volume_names_for_pod(&get_unique_pod_name(p));
            let expected: HashMap<String, UniqueVolumeName> = expected
                .iter()
                .map(|(k, v)| ((*k).to_string(), UniqueVolumeName((*v).to_string())))
                .collect();
            assert_eq!(
                actual, expected,
                "GetVolumeNamesForPod returned incorrect value"
            );
        };
        expect(
            &pod1,
            &[
                ("volume1-name", "fake-plugin/fake-device1"),
                ("volume1-dup", "fake-plugin/fake-device1"),
                ("volume3-name", "fake-plugin/fake-device3"),
            ],
        );
        expect(&pod2, &[("volume2-name", "fake-plugin/fake-device2")]);
    }

    // ------------------------------------------------------------- SELinux

    /// The gates a Go SELinux test case names, resolved to the value the Go
    /// test binary actually runs with.
    ///
    /// `featuregatetesting.SetFeatureGateDuringTest` only forces the NAMED
    /// gates to `true`; the rest keep their upstream defaults, which for
    /// v1.35 are `SELinuxMountReadWriteOncePod = true`,
    /// `SELinuxChangePolicy = true`, `SELinuxMount = false`
    /// (`pkg/features/kube_features.go:1733-1747`). So the only gate the
    /// `featureGates` column actually varies is `SELinuxMount`; the other two
    /// are on in every case either way. Set here explicitly so the test does
    /// not depend on whatever a sibling test left in the process-wide registry.
    fn selinux_gates(selinux_mount: bool) -> (impl Drop, impl Drop, impl Drop) {
        (
            with_feature(Feature::SELinuxMountReadWriteOncePod, true),
            with_feature(Feature::SELinuxChangePolicy, true),
            with_feature(Feature::SELinuxMount, selinux_mount),
        )
    }

    const COMPLETE_SELINUX_LABEL: &str = "system_u:object_r:container_file_t:s0:c1,c2";

    fn complete_selinux_opts() -> SELinuxOptions {
        serde_json::from_value(json!({
            "user": "system_u", "role": "object_r", "type": "container_t", "level": "s0:c1,c2",
        }))
        .unwrap()
    }

    fn incomplete_selinux_opts() -> SELinuxOptions {
        serde_json::from_value(json!({ "level": "s0:c1,c2" })).unwrap()
    }

    fn conflicting_selinux_opts() -> SELinuxOptions {
        serde_json::from_value(json!({
            "user": "system_u", "role": "object_r", "type": "container_t", "level": "s0:c98,c99",
        }))
        .unwrap()
    }

    /// The pod + PV pair the SELinux tests share. See the module doc for why
    /// the inline PVC volume is carried alongside the PV.
    fn selinux_fixture(
        pod_name: &str,
        pod_uid: &str,
        change_policy: Option<&str>,
        access_mode: &str,
    ) -> (Arc<Pod>, Arc<OwnedSpec>) {
        let p = pod(json!({
            "metadata": { "name": pod_name, "uid": pod_uid },
            "spec": {
                "securityContext": { "seLinuxChangePolicy": change_policy },
                "volumes": [{
                    "name": "volume-name",
                    "persistentVolumeClaim": { "claimName": "myClaim" },
                }],
            },
        }));
        let volume: Volume = p.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0].clone();
        let pv: PersistentVolume = serde_json::from_value(json!({
            "metadata": { "name": "basicPV" },
            "spec": { "accessModes": [access_mode] },
        }))
        .unwrap();
        (
            p,
            Arc::new(OwnedSpec {
                volume,
                persistent_volume: Some(pv),
                read_only: false,
                migrated: false,
                inline_volume_spec_for_csi_migration: false,
            }),
        )
    }

    fn selinux_plugin_mgr(supports_selinux: bool) -> Arc<VolumePluginMgr> {
        Arc::new(VolumePluginMgr::new(vec![Box::new(FakeVolumePlugin {
            plugin_name: "basic",
            // Upstream uses `FakeBasicVolumePlugin`, whose CanSupport is a
            // prefix match on `spec.Name()` == "basicPV". Our spec name is the
            // pod-side "volume-name" (module doc, substitution 2), and this is
            // the only registered plugin, so "support everything" is the same
            // registry behaviour.
            can_support_prefix: None,
            supports_remount: false,
            supports_selinux,
            attachable: false,
            device_mountable: true,
        })]))
    }

    /// <- `Test_AddPodToVolume_SELinuxSinglePod` (`:680`).
    #[test]
    #[serial_test::serial]
    fn test_add_pod_to_volume_selinux_single_pod() {
        // (name, selinux_mount gate, plugin supports SELinux, access mode,
        //  pod SELinuxOptions, change policy, expect error, expected label)
        type Case = (
            &'static str,
            bool,
            bool,
            &'static str,
            Option<SELinuxOptions>,
            Option<&'static str>,
            bool,
            &'static str,
        );
        let tests: Vec<Case> = vec![
            (
                "RWOP: ReadWriteOncePod with plugin that supports SELinux mount",
                false,
                true,
                "ReadWriteOncePod",
                Some(complete_selinux_opts()),
                None,
                false,
                COMPLETE_SELINUX_LABEL,
            ),
            // kubelet fills the missing SELinuxOptions fields
            (
                "RWOP: ReadWriteOncePod incomplete SELinuxOptions",
                false,
                true,
                "ReadWriteOncePod",
                Some(incomplete_selinux_opts()),
                None,
                false,
                COMPLETE_SELINUX_LABEL,
            ),
            (
                "RWOP: ReadWriteOncePod no SELinuxOptions",
                false,
                true,
                "ReadWriteOncePod",
                None,
                None,
                false,
                "",
            ),
            // The plugin does not support SELinux
            (
                "RWOP: ReadWriteOncePod with plugin that does not support SELinux mount",
                false,
                false,
                "ReadWriteOncePod",
                Some(complete_selinux_opts()),
                None,
                false,
                "",
            ),
            (
                "RWOP: ReadWriteMany with plugin that supports SELinux mount",
                false,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                None,
                false,
                "",
            ),
            (
                "RWOP+ChangePolicy: ReadWriteMany with the default policy",
                false,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                None,
                false,
                "",
            ),
            (
                "RWOP+ChangePolicy: ReadWriteMany with Recursive policy",
                false,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                Some("Recursive"),
                false,
                "",
            ),
            // still not supported for RWX
            (
                "RWOP+ChangePolicy: ReadWriteMany with MountOption policy",
                false,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                Some("MountOption"),
                false,
                "",
            ),
            // "Recursive" is applied to RWOP volumes too
            (
                "RWOP+ChangePolicy: ReadWriteOncePod with Recursive policy",
                false,
                true,
                "ReadWriteOncePod",
                Some(complete_selinux_opts()),
                Some("Recursive"),
                false,
                "",
            ),
            // the policy is ignored, but mounting with SELinux is the default
            (
                "RWOP+ChangePolicy: ReadWriteOncePod with MountOption policy",
                false,
                true,
                "ReadWriteOncePod",
                Some(complete_selinux_opts()),
                Some("MountOption"),
                false,
                COMPLETE_SELINUX_LABEL,
            ),
            (
                "RWOP+ChangePolicy+Mount: ReadWriteMany with the default policy",
                true,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                None,
                false,
                COMPLETE_SELINUX_LABEL,
            ),
            (
                "RWOP+ChangePolicy+Mount: ReadWriteMany with Recursive policy",
                true,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                Some("Recursive"),
                false,
                "",
            ),
            (
                "RWOP+ChangePolicy+Mount: ReadWriteMany with MountOption policy",
                true,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                Some("MountOption"),
                false,
                COMPLETE_SELINUX_LABEL,
            ),
            // "Recursive" is applied to RWOP volumes too
            (
                "RWOP+ChangePolicy+Mount: ReadWriteOncePod with Recursive policy",
                true,
                true,
                "ReadWriteOncePod",
                Some(complete_selinux_opts()),
                Some("Recursive"),
                false,
                "",
            ),
            (
                "RWOP+ChangePolicy+Mount: ReadWriteOncePod with MountOption policy",
                true,
                true,
                "ReadWriteOncePod",
                Some(complete_selinux_opts()),
                Some("MountOption"),
                false,
                COMPLETE_SELINUX_LABEL,
            ),
        ];

        for (
            name,
            selinux_mount,
            plugin_supports,
            access_mode,
            opts,
            policy,
            expect_error,
            expected_label,
        ) in tests
        {
            let _gates = selinux_gates(selinux_mount);
            let dsw = new_dsw(selinux_plugin_mgr(plugin_supports));
            let (p, volume_spec) = selinux_fixture("pod1", "pod1uid", policy, access_mode);
            let pod_name = get_unique_pod_name(&p);
            let contexts = [opts];

            let result = dsw.add_pod_to_volume(
                &pod_name,
                Arc::clone(&p),
                Arc::clone(&volume_spec),
                "volume-name",
                "",
                &contexts,
            );

            if expect_error {
                assert!(result.is_err(), "{name}: expected an error, got Ok");
                continue;
            }
            let generated_volume_name =
                result.unwrap_or_else(|e| panic!("{name}: AddPodToVolume failed: {e}"));

            verify_volume_exists_dsw(&generated_volume_name, expected_label, &dsw);
            verify_volume_exists_in_volumes_to_mount(
                &generated_volume_name,
                &["volume-name"],
                false,
                &dsw,
            );
            verify_pod_exists_in_volume_dsw(
                &pod_name,
                &generated_volume_name,
                expected_label,
                &dsw,
            );
            verify_volume_exists_with_spec_name_in_volume_dsw(&pod_name, volume_spec.name(), &dsw);
        }
    }

    /// <- `Test_AddPodToVolume_SELinux_MultiplePods` (`:961`).
    ///
    /// Pins that the conflicting-label check fires only when a SECOND pod
    /// joins an existing volume (`desired_state_of_world.go:371-390`), and that
    /// the first pod's entry survives the rejection.
    #[test]
    #[serial_test::serial]
    fn test_add_pod_to_volume_selinux_multiple_pods() {
        // (name, selinux_mount gate, access mode, first opts, second opts,
        //  first policy, second policy, expect error, expected label)
        type Case = (
            &'static str,
            bool,
            &'static str,
            Option<SELinuxOptions>,
            Option<SELinuxOptions>,
            Option<&'static str>,
            Option<&'static str>,
            bool,
            &'static str,
        );
        let c = complete_selinux_opts;
        let x = conflicting_selinux_opts;
        let tests: Vec<Case> = vec![
            ("RWOP: ReadWriteOncePod with the same SELinux options", false, "ReadWriteOncePod", Some(c()), Some(c()), None, None, false, COMPLETE_SELINUX_LABEL),
            ("RWOP: ReadWriteOncePod with conflicting SELinux options", false, "ReadWriteOncePod", Some(c()), Some(x()), None, None, true, COMPLETE_SELINUX_LABEL),
            // RWX does not support SELinux mount (yet)
            ("RWOP: ReadWriteMany with the same SELinux options", false, "ReadWriteMany", Some(c()), Some(c()), None, None, false, ""),
            ("RWOP+ChangePolicy: ReadWriteOncePod with the same SELinux options", false, "ReadWriteOncePod", Some(c()), Some(c()), None, None, false, COMPLETE_SELINUX_LABEL),
            // Recursive is applied to RWOP volumes
            ("RWOP+ChangePolicy: ReadWriteOncePod with the same SELinux options and same Recursive policy", false, "ReadWriteOncePod", Some(c()), Some(c()), Some("Recursive"), Some("Recursive"), false, ""),
            // Conflicting policies with RWOP are an error
            ("RWOP+ChangePolicy: ReadWriteOncePod with the same SELinux options and conflicting policies", false, "ReadWriteOncePod", Some(c()), Some(c()), Some("Recursive"), None, true, ""),
            ("RWOP+ChangePolicy: ReadWriteMany with the same SELinux options with Recursive policy", false, "ReadWriteMany", Some(c()), Some(c()), Some("Recursive"), Some("Recursive"), false, ""),
            ("RWOP+ChangePolicy: ReadWriteMany with the same SELinux options with MountOption policy", false, "ReadWriteMany", Some(c()), Some(c()), Some("MountOption"), Some("MountOption"), false, ""),
            ("RWOP+ChangePolicy: ReadWriteMany with the same SELinux options with conflicting policies", false, "ReadWriteMany", Some(c()), Some(c()), Some("MountOption"), Some("Recursive"), false, ""),
            // Conflicting SELinux options are allowed with recursive policy
            ("RWOP+ChangePolicy: ReadWriteMany with conflicting SELinux options and Recursive policy", false, "ReadWriteMany", Some(c()), Some(x()), Some("Recursive"), Some("Recursive"), false, ""),
            ("RWOP+ChangePolicy: ReadWriteMany with conflicting SELinux options and MountOption policy", false, "ReadWriteMany", Some(c()), Some(x()), Some("MountOption"), Some("MountOption"), false, ""),
            ("RWOP+ChangePolicy+Mount: ReadWriteOncePod with the same SELinux options", true, "ReadWriteOncePod", Some(c()), Some(c()), None, None, false, COMPLETE_SELINUX_LABEL),
            ("RWOP+ChangePolicy+Mount: ReadWriteOncePod with the same SELinux options and same Recursive policy", true, "ReadWriteOncePod", Some(c()), Some(c()), Some("Recursive"), Some("Recursive"), false, ""),
            ("RWOP+ChangePolicy+Mount: ReadWriteOncePod with the same SELinux options and conflicting policies", true, "ReadWriteOncePod", Some(c()), Some(c()), Some("Recursive"), None, true, ""),
            ("RWOP+ChangePolicy+Mount: ReadWriteMany with the same SELinux options with Recursive policy", true, "ReadWriteMany", Some(c()), Some(c()), Some("Recursive"), Some("Recursive"), false, ""),
            ("RWOP+ChangePolicy+Mount: ReadWriteMany with the same SELinux options with MountOption policy", true, "ReadWriteMany", Some(c()), Some(c()), Some("MountOption"), Some("MountOption"), false, COMPLETE_SELINUX_LABEL),
            // nil should default to MountOption
            ("RWOP+ChangePolicy+Mount: ReadWriteMany with the same SELinux options with default and MountOption policy", true, "ReadWriteMany", Some(c()), Some(c()), None, Some("MountOption"), false, COMPLETE_SELINUX_LABEL),
            // MountOption policy is applied to the first volume
            ("RWOP+ChangePolicy+Mount: ReadWriteMany with the same SELinux options with conflicting policies", true, "ReadWriteMany", Some(c()), Some(c()), Some("MountOption"), Some("Recursive"), true, COMPLETE_SELINUX_LABEL),
            ("RWOP+ChangePolicy+Mount: ReadWriteMany with conflicting SELinux options and Recursive policy", true, "ReadWriteMany", Some(c()), Some(x()), Some("Recursive"), Some("Recursive"), false, ""),
            // The SELinux label of the first pod is used
            ("RWOP+ChangePolicy+Mount: ReadWriteMany with conflicting SELinux options and MountOption policy", true, "ReadWriteMany", Some(c()), Some(x()), Some("MountOption"), Some("MountOption"), true, COMPLETE_SELINUX_LABEL),
        ];

        for (
            name,
            selinux_mount,
            access_mode,
            first_opts,
            second_opts,
            first_policy,
            second_policy,
            expect_error,
            expected_label,
        ) in tests
        {
            let _gates = selinux_gates(selinux_mount);
            let dsw = new_dsw(selinux_plugin_mgr(true));

            let (pod1, volume_spec) = selinux_fixture("pod1", "pod1uid", first_policy, access_mode);
            let pod1_name = get_unique_pod_name(&pod1);
            let generated_volume_name = dsw
                .add_pod_to_volume(
                    &pod1_name,
                    Arc::clone(&pod1),
                    Arc::clone(&volume_spec),
                    "volume-name",
                    "",
                    &[first_opts],
                )
                .unwrap_or_else(|e| panic!("{name}: AddPodToVolume failed: {e}"));

            verify_volume_exists_dsw(&generated_volume_name, expected_label, &dsw);
            verify_volume_exists_in_volumes_to_mount(
                &generated_volume_name,
                &["volume-name"],
                false,
                &dsw,
            );
            verify_pod_exists_in_volume_dsw(
                &pod1_name,
                &generated_volume_name,
                expected_label,
                &dsw,
            );
            verify_volume_exists_with_spec_name_in_volume_dsw(&pod1_name, volume_spec.name(), &dsw);

            // A different pod with the same volume.
            let (pod2, _) = selinux_fixture("pod2", "pod2uid", second_policy, access_mode);
            let pod2_name = get_unique_pod_name(&pod2);
            let result = dsw.add_pod_to_volume(
                &pod2_name,
                Arc::clone(&pod2),
                Arc::clone(&volume_spec),
                "volume-name",
                "",
                &[second_opts],
            );

            if expect_error {
                assert!(result.is_err(), "{name}: expected an error, got Ok");
                // Verify the original SELinux context is still in DSW.
                verify_pod_exists_in_volume_dsw(
                    &pod1_name,
                    &generated_volume_name,
                    expected_label,
                    &dsw,
                );
                continue;
            }
            let generated_volume_name2 =
                result.unwrap_or_else(|e| panic!("{name}: second AddPodToVolume failed: {e}"));
            assert_eq!(
                generated_volume_name2, generated_volume_name,
                "{name}: expected the same generated volume name"
            );
            verify_pod_exists_in_volume_dsw(
                &pod2_name,
                &generated_volume_name,
                expected_label,
                &dsw,
            );
        }
    }

    // --------------------------------------------- no upstream test exists

    /// `desired_state_of_world_test.go` has no coverage for the pod-errors
    /// methods; this pins them against `desired_state_of_world.go:618-651` as
    /// read. Not a port.
    #[test]
    fn pod_errors_dedupe_are_popped_destructively_and_are_listed() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let pod_name = UniquePodName("poduid".to_string());

        assert!(dsw.get_pods_with_errors().is_empty());
        assert!(dsw.pop_pod_errors(&pod_name).is_empty());

        dsw.add_error_to_pod(&pod_name, "b");
        dsw.add_error_to_pod(&pod_name, "a");
        dsw.add_error_to_pod(&pod_name, "b"); // stored only once
        assert_eq!(dsw.get_pods_with_errors(), vec![pod_name.clone()]);

        // `sets.List` sorts.
        assert_eq!(dsw.pop_pod_errors(&pod_name), vec!["a", "b"]);
        // ...and clears.
        assert!(dsw.pop_pod_errors(&pod_name).is_empty());
        assert!(dsw.get_pods_with_errors().is_empty());
    }

    /// The `<=` bound at `:623` lets the set reach `MAX_POD_ERRORS + 1`. Pinned
    /// so a later "tidy-up" to `<` is caught as the divergence it would be.
    #[test]
    fn pod_errors_cap_admits_one_more_than_max_pod_errors() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let pod_name = UniquePodName("poduid".to_string());
        for i in 0..50 {
            dsw.add_error_to_pod(&pod_name, &format!("err{i:02}"));
        }
        assert_eq!(dsw.pop_pod_errors(&pod_name).len(), MAX_POD_ERRORS + 1);
    }

    /// `DeletePodFromVolume` is a no-op for an unknown volume and for a known
    /// volume that does not hold the pod — but it drops the pod's errors
    /// either way, because that happens before the volume lookup (`:463`).
    #[test]
    fn delete_pod_from_volume_is_a_no_op_but_still_drops_pod_errors() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let p = pod(json!({
            "metadata": { "name": "pod1", "uid": "pod1uid" },
            "spec": { "volumes": [nfs_volume("volume-name", "fake-device1")] },
        }));
        let volume_spec = spec_of(&p, 0);
        let pod_name = get_unique_pod_name(&p);
        let volume_name = dsw
            .add_pod_to_volume(
                &pod_name,
                Arc::clone(&p),
                Arc::clone(&volume_spec),
                volume_spec.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed");

        let other_pod = UniquePodName("otheruid".to_string());
        dsw.add_error_to_pod(&other_pod, "boom");

        // Unknown volume: the volume and its pod survive.
        dsw.delete_pod_from_volume(&other_pod, &UniqueVolumeName("nope".to_string()));
        verify_volume_exists_dsw(&volume_name, "", &dsw);
        // ...but the pod's errors are gone.
        assert!(dsw.get_pods_with_errors().is_empty());

        // Known volume, pod not under it: still a no-op.
        dsw.delete_pod_from_volume(&other_pod, &volume_name);
        verify_pod_exists_in_volume_dsw(&pod_name, &volume_name, "", &dsw);
        assert_eq!(dsw.get_pods(), HashSet::from([pod_name]));
    }

    /// `MarkVolumeAttachability` and `UpdatePersistentVolumeSize` are both
    /// untested upstream. Both mutate in place and both no-op on an unknown
    /// volume (`:653-662`, `:483-494`).
    #[test]
    fn mark_volume_attachability_and_update_persistent_volume_size() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let p = pod(json!({
            "metadata": { "name": "pod1", "uid": "pod1uid" },
            "spec": { "volumes": [nfs_volume("volume-name", "fake-device1")] },
        }));
        let volume_spec = spec_of(&p, 0);
        let pod_name = get_unique_pod_name(&p);
        let volume_name = dsw
            .add_pod_to_volume(
                &pod_name,
                Arc::clone(&p),
                Arc::clone(&volume_spec),
                volume_spec.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed");

        // The fake plugin is attachable, so it starts true.
        assert!(dsw.get_volumes_to_mount()[0].plugin_is_attachable);
        dsw.mark_volume_attachability(&volume_name, false);
        assert!(!dsw.get_volumes_to_mount()[0].plugin_is_attachable);

        // No PV, so no size is recorded until one is pushed in.
        assert_eq!(
            dsw.get_volumes_to_mount()[0].desired_persistent_volume_size,
            None
        );
        dsw.update_persistent_volume_size(&volume_name, Quantity::parse("5Gi").unwrap());
        assert_eq!(
            dsw.get_volumes_to_mount()[0].desired_persistent_volume_size,
            Some(Quantity::parse("5Gi").unwrap())
        );

        // Unknown volume: both are silent no-ops.
        let missing = UniqueVolumeName("nope".to_string());
        dsw.mark_volume_attachability(&missing, true);
        dsw.update_persistent_volume_size(&missing, Quantity::parse("1Gi").unwrap());
        assert!(!dsw.get_volumes_to_mount()[0].plugin_is_attachable);
    }
}
