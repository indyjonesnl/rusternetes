//! Port of `pkg/volume/util/operationexecutor/node_expander.go` — the
//! `NodeExpander` that runs node-side volume expansion with recovery from
//! expansion failure (`RecoverVolumeExpansionFailure`, KEP-1790).
//!
//! Nothing in the running kubelet calls this yet: its caller is
//! `operationGenerator.nodeExpandVolume` /
//! `expandVolumeDuringMount` (see [`super::operation_generator`]), which the
//! volume-manager reconciler (#1970) invokes.
//!
//! **Deviation: no `testStatus`.** Upstream records
//! `testResponseData{resizeCalledOnPlugin, assumeResizeFinished}` on the
//! struct only so `TestNodeExpander` can assert on it. Here the tests observe
//! the same two facts directly: whether the fake plugin was called, and the
//! returned `finished` flag (`assumeResizeFinished` is true on every path
//! except the early `NodeExpandVolume is not allowed` and in-progress-mark
//! failure, where it is `false`/unset and `finished` is `false` too).

use crate::events::{FILE_SYSTEM_RESIZE_SUCCESS, KUBELET_COMPONENT};
use crate::volume_manager::cache::actual_state_of_world::ActualStateOfWorld;
use crate::volume_plugins::plugin::{NodeExpandableVolumePlugin, NodeResizeOptions};
use crate::volume_plugins::util::operation_executor::VolumeToMount;
use crate::volume_plugins::util::resize_util::{
    self, CLAIM_NODE_RESIZE_IN_PROGRESS, CLAIM_NODE_RESIZE_PENDING, RESOURCE_STORAGE,
};
use crate::volume_plugins::util::types::{
    is_failed_precondition_error, is_infeasible_error, is_operation_finished_error,
    is_operation_not_supported_error, NODE_EXPANSION_NOT_REQUIRED,
};
use anyhow::{anyhow, Error};
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::resources::volume::{
    PersistentVolume, PersistentVolumeAccessMode, PersistentVolumeClaim,
};
use rusternetes_common::resources::{EventSource, EventType, ObjectReference};
use rusternetes_storage::{EventRecorder, Storage};
use tracing::{debug, error, info};

/// Port of `nodeResizeOperationOpts` (`operation_generator.go:152-159`).
pub struct NodeResizeOperationOpts<'a> {
    pub vmt: &'a VolumeToMount,
    pub pvc: PersistentVolumeClaim,
    pub pv: &'a PersistentVolume,
    pub plugin_resize_opts: NodeResizeOptions<'a>,
    pub volume_plugin: &'a dyn NodeExpandableVolumePlugin,
    pub actual_state_of_world: &'a ActualStateOfWorld,
}

/// The `(bool, resource.Quantity, error)` triple `expandOnPlugin` returns.
///
/// Kept as a triple, not a `Result`, because upstream returns a non-nil error
/// together with a meaningful `resizeFinished` on one path
/// (`node_expander.go:223-227`: finished, new size, *and* the status-update
/// error), and `expandVolumeDuringMount` forwards both
/// (`operation_generator.go:1986-1987`).
pub struct ExpandOutcome {
    pub resize_finished: bool,
    pub size: Quantity,
    pub err: Option<Error>,
}

/// Port of `NodeExpander` (`node_expander.go:34-54`).
pub struct NodeExpander<'a, S: Storage + ?Sized> {
    opts: NodeResizeOperationOpts<'a>,
    client: &'a S,
    recorder: &'a EventRecorder<S>,

    // computed via precheck
    pvc_status_cap: Quantity,
    resize_status: String,

    /// Indicates that if volume expansion failed on the node, then current
    /// expansion should be marked as infeasible so as controller can
    /// reconcile the resizing operation by using new user requested size.
    mark_expansion_infeasible_on_failure: bool,

    /// If true, although we are calling NodeExpandVolume on the kubelet the
    /// PVC has already been updated — possibly because expansion already
    /// succeeded on a different node. This can happen when a RWX PVC is
    /// expanded.
    pvc_already_updated: bool,
}

fn zero_quantity() -> Quantity {
    Quantity::from_value(0, Format::DecimalSI)
}

/// `pvc.Status.Capacity[v1.ResourceStorage]`: a missing entry is the zero
/// `resource.Quantity`.
fn storage_quantity(map: Option<&std::collections::HashMap<String, String>>) -> Quantity {
    map.and_then(|m| m.get(RESOURCE_STORAGE))
        .and_then(|v| Quantity::parse(v.trim()).ok())
        .unwrap_or_else(zero_quantity)
}

/// Record an event on `involved` as the kubelet. `Eventf` has no return value
/// upstream, so a recorder failure is dropped.
pub(crate) async fn record_event<S: Storage + ?Sized>(
    recorder: &EventRecorder<S>,
    involved: &ObjectReference,
    node_name: Option<&str>,
    event_type: EventType,
    reason: &str,
    message: &str,
) {
    let source = EventSource {
        component: KUBELET_COMPONENT.to_string(),
        host: node_name.filter(|n| !n.is_empty()).map(str::to_string),
    };
    let _ = recorder
        .event(involved, &source, event_type, reason, message)
        .await;
}

/// `involvedObject` for a PVC event.
pub(crate) fn pvc_object_reference(pvc: &PersistentVolumeClaim) -> ObjectReference {
    ObjectReference {
        kind: Some("PersistentVolumeClaim".to_string()),
        namespace: pvc.metadata.namespace.clone(),
        name: Some(pvc.metadata.name.clone()),
        uid: Some(pvc.metadata.uid.clone()),
        api_version: Some("v1".to_string()),
        resource_version: pvc.metadata.resource_version.clone(),
        field_path: None,
    }
}

impl<'a, S: Storage + ?Sized> NodeExpander<'a, S> {
    /// Port of `newNodeExpander` (`node_expander.go:56-62`).
    pub fn new(
        resize_op: NodeResizeOperationOpts<'a>,
        client: &'a S,
        recorder: &'a EventRecorder<S>,
    ) -> Self {
        Self {
            opts: resize_op,
            client,
            recorder,
            pvc_status_cap: zero_quantity(),
            resize_status: String::new(),
            mark_expansion_infeasible_on_failure: false,
            pvc_already_updated: false,
        }
    }

    /// The PVC as the expander last saw it (`nodeExpander.pvc`), which the
    /// status writers replace as they succeed.
    pub fn pvc(&self) -> &PersistentVolumeClaim {
        &self.opts.pvc
    }

    /// Port of `runPreCheck` (`node_expander.go:78-130`): sanity checks before
    /// expansion can be performed on the PVC. Returns true only if node
    /// expansion is allowed to proceed.
    fn run_pre_check(&mut self) -> bool {
        let status = self.opts.pvc.status.as_ref();
        self.pvc_status_cap = storage_quantity(status.and_then(|s| s.capacity.as_ref()));

        if let Some(current_status) = status
            .and_then(|s| s.allocated_resource_statuses.as_ref())
            .and_then(|m| m.get(RESOURCE_STORAGE))
        {
            self.resize_status = current_status.clone();
        }

        let pvc_spec_cap = storage_quantity(self.opts.pvc.spec.resources.requests.as_ref());
        let new_size = self.opts.plugin_resize_opts.new_size;

        // usually when are performing node expansion, we expect pv size and
        // pvc spec size to be the same, but if user has edited pvc since then
        // and volume expansion failed with final error, then we should let
        // controller reconcile this state, by marking entire node expansion
        // as infeasible.
        if pvc_spec_cap.cmp_value(&new_size) != std::cmp::Ordering::Equal
            && self
                .opts
                .actual_state_of_world
                .check_volume_in_failed_expansion_with_final_errors(&self.opts.vmt.volume_name)
        {
            self.mark_expansion_infeasible_on_failure = true;
        }

        if self.pvc_status_cap.cmp_value(&new_size) != std::cmp::Ordering::Less
            && self.resize_status.is_empty()
        {
            self.pvc_already_updated = true;
        }

        // if the volume is already expanded, but volume is of type RWX and
        // pvc doesn't have annotation indicating that node expansion is not
        // required then we should allow node expansion to proceed, even if
        // the volume is already expanded.
        //
        // This special cases is needed because, in case of RWX volumes, the
        // volume expansion should be performed on all nodes, even if the
        // volume is already expanded.
        if self.pvc_already_updated
            && self
                .opts
                .pvc
                .spec
                .access_modes
                .contains(&PersistentVolumeAccessMode::ReadWriteMany)
            && !self
                .opts
                .pvc
                .metadata
                .annotations
                .as_ref()
                .is_some_and(|a| a.contains_key(NODE_EXPANSION_NOT_REQUIRED))
        {
            return true;
        }

        // recovery features will only work for newer version of resize controller
        if self.resize_status.is_empty() {
            return false;
        }

        // if resizestatus is nil or NodeExpansionInProgress or
        // NodeExpansionPending then we should allow volume expansion on the
        // node to proceed.
        self.resize_status == CLAIM_NODE_RESIZE_PENDING
            || self.resize_status == CLAIM_NODE_RESIZE_IN_PROGRESS
    }

    /// Port of `expandOnPlugin` (`node_expander.go:132-228`).
    pub async fn expand_on_plugin(&mut self) -> ExpandOutcome {
        let new_size = self.opts.plugin_resize_opts.new_size;
        let old_size = self.opts.plugin_resize_opts.old_size;
        let outcome = |resize_finished: bool, size: Quantity, err: Option<Error>| ExpandOutcome {
            resize_finished,
            size,
            err,
        };

        let allow_expansion = self.run_pre_check();
        if !allow_expansion {
            if self.pvc_already_updated {
                // if pvc is already updated, then we could be here because
                // size stored in ASOW is smaller and controller did full
                // expansion and hence no node expansion is needed. This will
                // stop reconciler from retrying expansion on the node.
                return outcome(true, new_size, None);
            }

            debug!(
                volume = %self.opts.vmt.volume_name,
                resize_status = %self.resize_status,
                "NodeExpandVolume is not allowed to proceed for volume"
            );
            return outcome(false, old_size, None);
        }

        let node_name = self
            .opts
            .vmt
            .pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.clone())
            .unwrap_or_default();

        if !self.pvc_already_updated {
            match resize_util::mark_node_expansion_in_progress(&self.opts.pvc, self.client).await {
                Ok(pvc) => self.opts.pvc = pvc,
                Err(err) => {
                    let msg = self.opts.vmt.generate_error_detailed(
                        "MountVolume.NodeExpandVolume failed to mark node expansion in progress",
                        &err,
                    );
                    error!("{msg}");
                    return outcome(false, old_size, Some(err));
                }
            }
        }

        let resize_err = self
            .opts
            .volume_plugin
            .node_expand(self.opts.plugin_resize_opts.clone())
            .await
            .err();
        if let Some(resize_err) = resize_err {
            // In order to support node volume expansion for RWX volumes on
            // different nodes, we bypass the check for
            // VolumeExpansionPendingOnNode state during the pre-check and then
            // directly call the NodeExpandVolume method on the plugin.
            //
            // However, it does not make sense where the csi driver does not
            // support node expansion. We should not treat this as a failure.
            // It is a workaround for this issue:
            // https://github.com/kubernetes/kubernetes/issues/131381.
            //
            // For other access modes, we should not hit this state, because we
            // will wait for VolumeExpansionPendingOnNode before trying to
            // expand volume in kubelet. See run_pre_check() above.
            //
            // If volume is already expanded, then we should not retry
            // expansion on the node if driver returns
            // OperationNotSupportedError.
            if is_operation_not_supported_error(&resize_err) && self.pvc_already_updated {
                debug!(
                    pod = %self.opts.vmt.pod.metadata.name,
                    "{}",
                    self.opts.vmt.generate_msg_detailed(
                        "MountVolume.NodeExpandVolume failed",
                        "NodeExpandVolume not supported"
                    )
                );
                return outcome(true, new_size, None);
            }

            if is_operation_finished_error(&resize_err) {
                self.opts
                    .actual_state_of_world
                    .mark_volume_expansion_failed_with_final_error(&self.opts.vmt.volume_name);
                let marked = if is_infeasible_error(&resize_err)
                    || self.mark_expansion_infeasible_on_failure
                {
                    resize_util::mark_node_expansion_infeasible(
                        &self.opts.pvc,
                        self.client,
                        &resize_err,
                    )
                    .await
                } else {
                    resize_util::mark_node_expansion_failed_condition(
                        &self.opts.pvc,
                        self.client,
                        &resize_err,
                    )
                    .await
                };
                match marked {
                    Ok(pvc) => self.opts.pvc = pvc,
                    // Upstream's `ne.pvc` is the unchanged input on error.
                    Err(mark_err) => error!(
                        "{}",
                        self.opts.vmt.generate_error_detailed(
                            "MountMount.NodeExpandVolume failed to mark node expansion as failed",
                            &mark_err
                        )
                    ),
                }
            }

            // if driver returned FailedPrecondition error that means volume
            // expansion should not be retried on this node but expansion
            // operation should not block mounting
            if is_failed_precondition_error(&resize_err) {
                self.opts
                    .actual_state_of_world
                    .mark_for_in_use_expansion_error(&self.opts.vmt.volume_name);
                error!(
                    "{}",
                    self.opts.vmt.generate_error_detailed(
                        "MountVolume.NodeExapndVolume failed",
                        &resize_err
                    )
                );
                return outcome(false, old_size, None);
            }
            return outcome(false, old_size, Some(resize_err));
        }

        let (simple_msg, detailed_msg) = self
            .opts
            .vmt
            .generate_msg("MountVolume.NodeExpandVolume succeeded", &node_name);
        record_event(
            self.recorder,
            &crate::events::pod_object_reference(&self.opts.vmt.pod),
            Some(&node_name),
            EventType::Normal,
            FILE_SYSTEM_RESIZE_SUCCESS,
            &simple_msg,
        )
        .await;
        record_event(
            self.recorder,
            &pvc_object_reference(&self.opts.pvc),
            Some(&node_name),
            EventType::Normal,
            FILE_SYSTEM_RESIZE_SUCCESS,
            &simple_msg,
        )
        .await;
        info!(pod = %self.opts.vmt.pod.metadata.name, "{detailed_msg}");

        // no need to update PVC object if we already updated it
        if self.pvc_already_updated {
            return outcome(true, new_size, None);
        }

        // File system resize succeeded, now update the PVC's Capacity to
        // match the PV's
        match resize_util::mark_node_expansion_finished_with_recovery(
            &self.opts.pvc,
            new_size,
            self.client,
        )
        .await
        {
            Ok(pvc) => {
                self.opts.pvc = pvc;
                outcome(true, new_size, None)
            }
            Err(err) => outcome(
                true,
                new_size,
                Some(anyhow!(
                    "mountVolume.NodeExpandVolume update pvc status failed: {err}"
                )),
            ),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    //! Port of `TestNodeExpander` (`node_expander_test.go:57-287`). Upstream's
    //! fake plugin keys its behaviour off the volume name
    //! (`volumetesting.InfeasibleNodeExpansion`, `OtherFinalNodeExpansionError`,
    //! `FailWithUnSupportedVolumeName`); here the fake plugin is told the
    //! error to return directly.
    use super::*;
    use crate::volume_manager::cache::actual_state_of_world::ActualStateOfWorld;
    use crate::volume_plugins::plugin::{Mounter, OwnedSpec, Spec, VolumePlugin};
    use crate::volume_plugins::registry::VolumePluginMgr;
    use crate::volume_plugins::util::types::{
        UniquePodName, UniqueVolumeName, VolumeOperationError,
    };
    use async_trait::async_trait;
    use rusternetes_common::resources::{Pod, Volume};
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
            Err(anyhow!("not used"))
        }
        fn new_unmounter(
            &self,
            _n: &str,
            _u: &str,
        ) -> anyhow::Result<Box<dyn crate::volume_plugins::plugin::Unmounter>> {
            Err(anyhow!("not used"))
        }
        fn construct_volume_spec(
            &self,
            _n: &str,
            _p: &str,
        ) -> anyhow::Result<crate::volume_plugins::plugin::ReconstructedVolume> {
            Err(anyhow!("not used"))
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
