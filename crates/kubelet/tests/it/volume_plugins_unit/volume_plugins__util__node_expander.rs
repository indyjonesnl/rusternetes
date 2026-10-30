//! Unit tests moved from `src/volume_plugins/util/node_expander.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Error};
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_common::resources::volume::{
        PersistentVolume, PersistentVolumeAccessMode, PersistentVolumeClaim,
    };
    pub use rusternetes_common::resources::{EventSource, EventType, ObjectReference};
    pub use rusternetes_kubelet::events::{FILE_SYSTEM_RESIZE_SUCCESS, KUBELET_COMPONENT};
    pub use rusternetes_kubelet::volume_manager::cache::actual_state_of_world::ActualStateOfWorld;
    pub use rusternetes_kubelet::volume_plugins::plugin::{
        NodeExpandableVolumePlugin, NodeResizeOptions,
    };
    pub use rusternetes_kubelet::volume_plugins::util::operation_executor::VolumeToMount;
    pub use rusternetes_kubelet::volume_plugins::util::resize_util::{
        self, CLAIM_NODE_RESIZE_IN_PROGRESS, CLAIM_NODE_RESIZE_PENDING, RESOURCE_STORAGE,
    };
    pub use rusternetes_kubelet::volume_plugins::util::types::{
        is_failed_precondition_error, is_infeasible_error, is_operation_finished_error,
        is_operation_not_supported_error, NODE_EXPANSION_NOT_REQUIRED,
    };
    pub use rusternetes_storage::{EventRecorder, Storage};
    pub use tracing::{debug, error, info};
}

pub(crate) mod tests {
    //! Port of `TestNodeExpander` (`node_expander_test.go:57-287`). Upstream's
    //! fake plugin keys its behaviour off the volume name
    //! (`volumetesting.InfeasibleNodeExpansion`, `OtherFinalNodeExpansionError`,
    //! `FailWithUnSupportedVolumeName`); here the fake plugin is told the
    //! error to return directly.
    use super::__parent::*;
    use async_trait::async_trait;
    use rusternetes_common::resources::{Pod, Volume};
    use rusternetes_kubelet::volume_manager::cache::actual_state_of_world::ActualStateOfWorld;
    use rusternetes_kubelet::volume_plugins::plugin::{Mounter, OwnedSpec, Spec, VolumePlugin};
    use rusternetes_kubelet::volume_plugins::registry::VolumePluginMgr;
    use rusternetes_kubelet::volume_plugins::util::node_expander::*;
    use rusternetes_kubelet::volume_plugins::util::types::{
        UniquePodName, UniqueVolumeName, VolumeOperationError,
    };
    use rusternetes_storage::StorageBackend;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::SystemTime;

    /// `volumetesting.FakeVolumePlugin` as a `NodeExpandableVolumePlugin`.
    pub(crate) struct FakeExpandPlugin {
        pub called: Arc<AtomicBool>,
        pub error: Option<fn() -> Error>,
    }

    impl FakeExpandPlugin {
        /// A second plugin that reports into the same `called` flag, so a test
        /// can hand one to the plugin manager and still observe it.
        pub fn sharing(&self) -> Self {
            Self {
                called: Arc::clone(&self.called),
                error: self.error,
            }
        }
        pub fn ok() -> Self {
            Self {
                called: Arc::new(AtomicBool::new(false)),
                error: None,
            }
        }
        pub fn failing(error: fn() -> Error) -> Self {
            Self {
                called: Arc::new(AtomicBool::new(false)),
                error: Some(error),
            }
        }
    }

    #[async_trait]
    impl VolumePlugin for FakeExpandPlugin {
        fn name(&self) -> &'static str {
            "fake-plugin"
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
            Ok(false)
        }
        fn as_node_expandable_plugin(&self) -> Option<&dyn NodeExpandableVolumePlugin> {
            Some(self)
        }
        async fn new_mounter(&self, _s: &Spec<'_>, _p: &Pod) -> anyhow::Result<Box<dyn Mounter>> {
            Ok(Box::new(FakeMounter))
        }
        fn new_unmounter(
            &self,
            _n: &str,
            _u: &str,
        ) -> anyhow::Result<Box<dyn rusternetes_kubelet::volume_plugins::plugin::Unmounter>>
        {
            Err(anyhow!("not used"))
        }
        fn construct_volume_spec(
            &self,
            _n: &str,
            _p: &str,
        ) -> anyhow::Result<rusternetes_kubelet::volume_plugins::plugin::ReconstructedVolume>
        {
            Err(anyhow!("not used"))
        }
    }

    /// `FakeVolume`'s mounter: only `GetPath` is read by node expansion.
    struct FakeMounter;

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
            "/mnt/fake".to_string()
        }
        async fn set_up_at(
            &self,
            _dir: &str,
            _args: &rusternetes_kubelet::volume_plugins::MounterArgs,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        fn get_attributes(&self) -> rusternetes_kubelet::volume_plugins::plugin::Attributes {
            Default::default()
        }
    }

    #[async_trait]
    impl NodeExpandableVolumePlugin for FakeExpandPlugin {
        fn requires_fs_resize(&self) -> bool {
            true
        }
        async fn node_expand(&self, _o: NodeResizeOptions<'_>) -> anyhow::Result<bool> {
            self.called.store(true, Ordering::SeqCst);
            match self.error {
                Some(e) => Err(e()),
                None => Ok(true),
            }
        }
    }

    pub(crate) fn q(s: &str) -> Quantity {
        Quantity::parse(s).unwrap()
    }

    /// Port of `getTestPVC` (`operation_generator_test.go`): `status_cap` /
    /// `allocated` empty means unset; `resize_status` likewise.
    pub(crate) fn test_pvc(
        spec_cap: &str,
        status_cap: &str,
        allocated: &str,
        resize_status: &str,
        rwx: bool,
        annotation: bool,
    ) -> PersistentVolumeClaim {
        let mut status = json!({});
        if !status_cap.is_empty() {
            status["capacity"] = json!({"storage": status_cap});
        }
        if !allocated.is_empty() {
            status["allocatedResources"] = json!({"storage": allocated});
        }
        if !resize_status.is_empty() {
            status["allocatedResourceStatuses"] = json!({"storage": resize_status});
        }
        let mut meta = json!({"name": "test-vol0", "namespace": "ns", "uid": "pvc-uid"});
        if annotation {
            meta["annotations"] = json!({ NODE_EXPANSION_NOT_REQUIRED: "true" });
        }
        serde_json::from_value(json!({
            "metadata": meta,
            "spec": {
                "accessModes": [if rwx { "ReadWriteMany" } else { "ReadWriteOnce" }],
                "resources": {"requests": {"storage": spec_cap}}
            },
            "status": status
        }))
        .unwrap()
    }

    pub(crate) fn test_pv(cap: &str) -> PersistentVolume {
        serde_json::from_value(json!({
            "metadata": {"name": "test-vol0"},
            "spec": {
                "capacity": {"storage": cap},
                "volumeMode": "Filesystem",
                "csi": {"driver": "d", "volumeHandle": "h"},
                "claimRef": {"namespace": "ns", "name": "test-vol0"}
            }
        }))
        .unwrap()
    }

    pub(crate) fn test_vmt(pv: &PersistentVolume, desired: &str) -> VolumeToMount {
        let volume: Volume = serde_json::from_value(
            json!({"name": "test-vol0", "persistentVolumeClaim": {"claimName": "test-vol0"}}),
        )
        .unwrap();
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "test-pod", "namespace": "ns", "uid": "pod-uid"},
            "spec": {"nodeName": "node-1", "containers": []}
        }))
        .unwrap();
        VolumeToMount {
            volume_name: UniqueVolumeName(pv.metadata.name.clone()),
            pod_name: UniquePodName("pod-uid".to_string()),
            volume_spec: Arc::new(OwnedSpec {
                volume,
                persistent_volume: Some(pv.clone()),
                read_only: false,
                migrated: false,
                inline_volume_spec_for_csi_migration: false,
            }),
            outer_volume_spec_names: vec![],
            pod: Arc::new(pod),
            plugin_is_attachable: false,
            plugin_is_device_mountable: false,
            volume_gid_value: String::new(),
            device_path: String::new(),
            reported_in_use: true,
            desired_size_limit: None,
            mount_request_time: SystemTime::now(),
            desired_persistent_volume_size: Some(q(desired)),
            selinux_label: String::new(),
        }
    }

    pub(crate) fn test_asow() -> ActualStateOfWorld {
        ActualStateOfWorld::new("node-1", Arc::new(VolumePluginMgr::new(vec![])))
    }

    struct Case {
        name: &'static str,
        pvc: PersistentVolumeClaim,
        plugin: FakeExpandPlugin,
        expect_error: bool,
        expected_resize_status: &'static str,
        expect_resize_call: bool,
        expect_final_errors: bool,
        expected_return_value: bool,
        expected_status_size: &'static str,
    }

    fn infeasible() -> Error {
        VolumeOperationError::Infeasible("infeasible".into()).into()
    }
    fn other_final() -> Error {
        anyhow!("other final error")
    }
    fn unsupported() -> Error {
        VolumeOperationError::OperationNotSupported("unsupported".into()).into()
    }
    fn failed_precondition() -> Error {
        VolumeOperationError::FailedPrecondition("in use".into()).into()
    }

    /// The table of `TestNodeExpander` that the `RecoverVolumeExpansionFailure`
    /// gate does not change (GA, locked on in 1.35, so the `=false` rows
    /// upstream are not reachable here).
    #[tokio::test]
    async fn node_expander_table() {
        let cases = vec![
            Case {
                name: "pv.spec.cap > pvc.status.cap, resizeStatus=node_expansion_failed",
                pvc: test_pvc("2G", "1G", "", "NodeResizeInfeasible", false, false),
                plugin: FakeExpandPlugin::ok(),
                expect_error: false,
                expected_resize_status: "NodeResizeInfeasible",
                expect_resize_call: false,
                expect_final_errors: false,
                expected_return_value: false,
                expected_status_size: "1G",
            },
            Case {
                name: "resizeStatus=node_expansion_pending",
                pvc: test_pvc("2G", "1G", "2G", "NodeResizePending", false, false),
                plugin: FakeExpandPlugin::ok(),
                expect_error: false,
                expected_resize_status: "",
                expect_resize_call: true,
                expect_final_errors: false,
                expected_return_value: true,
                expected_status_size: "2G",
            },
            Case {
                name: "resizeStatus=node_expansion_pending, resize_op=infeasible",
                pvc: test_pvc("2G", "1G", "2G", "NodeResizePending", false, false),
                plugin: FakeExpandPlugin::failing(infeasible),
                expect_error: true,
                expected_resize_status: "NodeResizeInfeasible",
                expect_resize_call: true,
                expect_final_errors: true,
                expected_return_value: false,
                expected_status_size: "1G",
            },
            Case {
                name: "resizeStatus=node_expansion_pending, resize_op=failing",
                pvc: test_pvc("2G", "1G", "2G", "NodeResizePending", false, false),
                plugin: FakeExpandPlugin::failing(other_final),
                expect_error: true,
                expected_resize_status: "NodeResizeInProgress",
                expect_resize_call: true,
                expect_final_errors: true,
                expected_return_value: false,
                expected_status_size: "1G",
            },
            Case {
                name: "RWO volumes, pv.spec.cap = pvc.status.cap, resizeStatus=''",
                pvc: test_pvc("2G", "2G", "2G", "", false, false),
                plugin: FakeExpandPlugin::ok(),
                expect_error: false,
                expected_resize_status: "",
                expect_resize_call: false,
                expect_final_errors: false,
                expected_return_value: true,
                expected_status_size: "2G",
            },
            Case {
                name: "RWX volumes, pv.spec.cap = pvc.status.cap, resizeStatus=''",
                pvc: test_pvc("2G", "2G", "2G", "", true, false),
                plugin: FakeExpandPlugin::ok(),
                expect_error: false,
                expected_resize_status: "",
                expect_resize_call: true,
                expect_final_errors: false,
                expected_return_value: true,
                expected_status_size: "2G",
            },
            Case {
                name: "RWX, pv.spec.cap = pvc.status.cap, resize_op=unsupported",
                pvc: test_pvc("2G", "2G", "2G", "", true, false),
                plugin: FakeExpandPlugin::failing(unsupported),
                expect_error: false,
                expected_resize_status: "",
                // upstream's `resizeCalledOnPlugin` is false on this path (the
                // testStatus is overwritten); the fake is still invoked.
                expect_resize_call: true,
                expect_final_errors: false,
                expected_return_value: true,
                expected_status_size: "2G",
            },
            Case {
                name: "RWX volumes, node-expansion-not-required",
                pvc: test_pvc("2G", "2G", "2G", "", true, true),
                plugin: FakeExpandPlugin::ok(),
                expect_error: false,
                expected_resize_status: "",
                expect_resize_call: false,
                expect_final_errors: false,
                expected_return_value: true,
                expected_status_size: "2G",
            },
            Case {
                name: "FailedPrecondition is swallowed and marks in-use error",
                pvc: test_pvc("2G", "1G", "2G", "NodeResizePending", false, false),
                plugin: FakeExpandPlugin::failing(failed_precondition),
                expect_error: false,
                expected_resize_status: "NodeResizeInProgress",
                expect_resize_call: true,
                expect_final_errors: true,
                expected_return_value: false,
                expected_status_size: "1G",
            },
        ];

        for case in cases {
            let storage = StorageBackend::new_memory();
            let recorder = EventRecorder::new(Arc::new(StorageBackend::new_memory()));
            let key =
                rusternetes_storage::build_key("persistentvolumeclaims", Some("ns"), "test-vol0");
            let pvc = storage.create(&key, &case.pvc).await.unwrap();
            let pv = test_pv("2G");
            let vmt = test_vmt(&pv, "2G");
            let asow = test_asow();
            let spec = vmt.volume_spec.as_spec();
            let status_cap =
                storage_quantity(pvc.status.as_ref().and_then(|s| s.capacity.as_ref()));
            let op = NodeResizeOperationOpts {
                vmt: &vmt,
                pvc,
                pv: &pv,
                plugin_resize_opts: NodeResizeOptions {
                    volume_spec: &spec,
                    device_path: String::new(),
                    device_mount_path: String::new(),
                    device_stage_path: String::new(),
                    new_size: q("2G"),
                    old_size: status_cap,
                },
                volume_plugin: &case.plugin,
                actual_state_of_world: &asow,
            };
            let mut expander = NodeExpander::new(op, &storage, &recorder);
            let out = expander.expand_on_plugin().await;

            let pvc = expander.pvc();
            let s = pvc.status.as_ref().unwrap();
            let resize_status = s
                .allocated_resource_statuses
                .as_ref()
                .and_then(|m| m.get("storage").cloned())
                .unwrap_or_default();
            let size = storage_quantity(s.capacity.as_ref());

            assert_eq!(out.err.is_some(), case.expect_error, "{}: error", case.name);
            assert_eq!(
                out.resize_finished, case.expected_return_value,
                "{}: return value",
                case.name
            );
            assert_eq!(
                case.plugin.called.load(Ordering::SeqCst),
                case.expect_resize_call,
                "{}: resize call",
                case.name
            );
            assert_eq!(
                resize_status, case.expected_resize_status,
                "{}: status",
                case.name
            );
            assert_eq!(
                asow.check_volume_in_failed_expansion_with_final_errors(&vmt.volume_name),
                case.expect_final_errors,
                "{}: final errors",
                case.name
            );
            assert_eq!(
                size.cmp_value(&q(case.expected_status_size)),
                std::cmp::Ordering::Equal,
                "{}: size {size}",
                case.name
            );
        }
    }
}
