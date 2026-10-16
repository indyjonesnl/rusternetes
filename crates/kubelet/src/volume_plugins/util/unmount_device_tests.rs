//! Tests for `operationGenerator.GenerateUnmountDeviceFunc`
//! (`pkg/volume/util/operationexecutor/operation_generator.go:807-905`) and
//! `isDeviceOpened` (`:2179-2198`).
//!
//! Upstream has no direct unit test of the function: `operation_executor_test.go`
//! drives it only through `fakegenerator.go`. The cases here are therefore one
//! per branch of the closure, with `volumetesting.FakeVolumePlugin` replaced by
//! a fake [`DeviceMountableVolumePlugin`] and `hostutil.FakeHostUtil`
//! (`fake_hostutil.go:51-66`) by [`FakeHostUtil`].

use super::hostutil::HostUtils;
use super::operation_executor::AttachedVolume;
use super::operation_generator::generate_unmount_device_func;
use super::types::{OperationContext, UniqueVolumeName};
use crate::volume_manager::cache::actual_state_of_world::ActualStateOfWorld;
use crate::volume_plugins::plugin::{
    DeviceMountableVolumePlugin, DeviceMounter, DeviceMounterArgs, DeviceUnmounter, Mounter,
    OwnedSpec, ReconstructedVolume, Spec, Unmounter, VolumePlugin,
};
use crate::volume_plugins::registry::VolumePluginMgr;
use crate::volume_plugins::util::operation_executor::DeviceMountState;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusternetes_common::resources::Pod;
use std::io;
use std::sync::{Arc, Mutex};

const PLUGIN: &str = "fake-plugin";
const VOLUME: &str = "vol-1";
const DEVICE_PATH: &str = "/dev/fake";
const MOUNT_PATH: &str = "/var/lib/kubelet/plugins/kubernetes.io/fake/globalmount";

/// What the fake plugin was asked to do, and what it should answer.
#[derive(Default)]
struct Behaviour {
    /// `GetDeviceMountPath`: `Ok(path)` or `Err(message)`.
    mount_path: Option<Result<String, String>>,
    /// `GetDeviceMountRefs`.
    refs: Option<Result<Vec<String>, String>>,
    /// `UnmountDevice` fails with this message.
    unmount_error: Option<String>,
    fail_new_unmounter: bool,
    fail_new_mounter: bool,
    /// Every `UnmountDevice(deviceMountPath)` call, in order.
    unmounted: Vec<String>,
    /// Every `GetDeviceMountRefs(deviceMountPath)` call.
    refs_asked: Vec<String>,
}

#[derive(Clone, Default)]
struct Fake(Arc<Mutex<Behaviour>>);

struct FakePlugin {
    fake: Fake,
    /// `false` registers a plugin that is not a `DeviceMountableVolumePlugin`.
    device_mountable: bool,
}

#[async_trait]
impl VolumePlugin for FakePlugin {
    fn name(&self) -> &'static str {
        PLUGIN
    }
    fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
        Ok(spec.name().to_string())
    }
    fn can_support(&self, _spec: &Spec<'_>) -> bool {
        true
    }
    fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
        false
    }
    fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
        Ok(false)
    }
    fn as_device_mountable_plugin(&self) -> Option<&dyn DeviceMountableVolumePlugin> {
        self.device_mountable.then_some(self as _)
    }
    async fn new_mounter(&self, _s: &Spec<'_>, _p: &Pod) -> Result<Box<dyn Mounter>> {
        Err(anyhow!("not used"))
    }
    fn new_unmounter(&self, _n: &str, _u: &str) -> Result<Box<dyn Unmounter>> {
        Err(anyhow!("not used"))
    }
    fn construct_volume_spec(&self, _n: &str, _p: &str) -> Result<ReconstructedVolume> {
        Err(anyhow!("not used"))
    }
}

impl DeviceMountableVolumePlugin for FakePlugin {
    fn new_device_mounter(&self) -> Result<Box<dyn DeviceMounter>> {
        if self.fake.0.lock().unwrap().fail_new_mounter {
            return Err(anyhow!("new mounter boom"));
        }
        Ok(Box::new(self.fake.clone()))
    }
    fn new_device_unmounter(&self) -> Result<Box<dyn DeviceUnmounter>> {
        if self.fake.0.lock().unwrap().fail_new_unmounter {
            return Err(anyhow!("new unmounter boom"));
        }
        Ok(Box::new(self.fake.clone()))
    }
    fn get_device_mount_refs(&self, device_mount_path: &str) -> Result<Vec<String>> {
        let mut b = self.fake.0.lock().unwrap();
        b.refs_asked.push(device_mount_path.to_string());
        match b.refs.clone().unwrap_or(Ok(vec![])) {
            Ok(r) => Ok(r),
            Err(e) => Err(anyhow!(e)),
        }
    }
}

#[async_trait]
impl DeviceMounter for Fake {
    fn get_device_mount_path(&self, _spec: &Spec<'_>) -> Result<String> {
        match self
            .0
            .lock()
            .unwrap()
            .mount_path
            .clone()
            .unwrap_or_else(|| Ok(MOUNT_PATH.to_string()))
        {
            Ok(p) => Ok(p),
            Err(e) => Err(anyhow!(e)),
        }
    }
    async fn mount_device(
        &self,
        _spec: &Spec<'_>,
        _device_path: &str,
        _device_mount_path: &str,
        _args: &DeviceMounterArgs,
    ) -> Result<()> {
        Err(anyhow!("not used"))
    }
}

#[async_trait]
impl DeviceUnmounter for Fake {
    async fn unmount_device(&self, device_mount_path: &str) -> Result<()> {
        let mut b = self.0.lock().unwrap();
        b.unmounted.push(device_mount_path.to_string());
        match b.unmount_error.clone() {
            Some(e) => Err(anyhow!(e)),
            None => Ok(()),
        }
    }
}

/// `hostutil.FakeHostUtil` with the answers made settable.
struct FakeHostUtil {
    path_is_device: io::Result<bool>,
    device_opened: io::Result<bool>,
}

impl FakeHostUtil {
    /// `FakeHostUtil.PathIsDevice` always returns true; no mount holds the
    /// device (`fake_hostutil.go:51-66`).
    fn idle() -> Self {
        Self {
            path_is_device: Ok(true),
            device_opened: Ok(false),
        }
    }
}

fn copy(r: &io::Result<bool>) -> io::Result<bool> {
    match r {
        Ok(b) => Ok(*b),
        Err(e) => Err(io::Error::other(e.to_string())),
    }
}

impl HostUtils for FakeHostUtil {
    fn device_opened(&self, _pathname: &str) -> io::Result<bool> {
        copy(&self.device_opened)
    }
    fn path_is_device(&self, _pathname: &str) -> io::Result<bool> {
        copy(&self.path_is_device)
    }
}

struct Fixture {
    fake: Fake,
    mgr: Arc<VolumePluginMgr>,
    asw: ActualStateOfWorld,
    volume: AttachedVolume,
}

fn spec() -> Arc<OwnedSpec> {
    Arc::new(OwnedSpec {
        volume: serde_json::from_value(serde_json::json!({"name": VOLUME, "emptyDir": {}}))
            .unwrap(),
        persistent_volume: None,
        read_only: false,
        migrated: false,
        inline_volume_spec_for_csi_migration: false,
    })
}

/// A volume the ASW knows as globally mounted at [`MOUNT_PATH`].
fn fixture_with(device_mountable: bool, plugin_name: &str) -> Fixture {
    let fake = Fake::default();
    let mgr = Arc::new(VolumePluginMgr::new(vec![Box::new(FakePlugin {
        fake: fake.clone(),
        device_mountable,
    })]));
    let asw = ActualStateOfWorld::new("node-1", Arc::clone(&mgr));
    let name = UniqueVolumeName(format!("{PLUGIN}/{VOLUME}"));
    asw.mark_volume_as_attached(Some(&name), spec(), "node-1", DEVICE_PATH)
        .unwrap();
    asw.mark_device_as_mounted(&name, DEVICE_PATH, MOUNT_PATH, "")
        .unwrap();
    let volume = AttachedVolume {
        volume_name: name,
        volume_spec: spec(),
        node_name: "node-1".to_string(),
        plugin_is_attachable: false,
        device_path: DEVICE_PATH.to_string(),
        device_mount_path: "/recorded/in/asw".to_string(),
        plugin_name: plugin_name.to_string(),
        selinux_mount_context: String::new(),
    };
    Fixture {
        fake,
        mgr,
        asw,
        volume,
    }
}

fn fixture() -> Fixture {
    fixture_with(true, PLUGIN)
}

impl Fixture {
    async fn run(&self, host: &FakeHostUtil) -> OperationContext {
        generate_unmount_device_func(&self.mgr, &self.volume, &self.asw, host)
            .expect("generate")
            .run()
            .await
    }

    fn state(&self) -> DeviceMountState {
        self.asw.get_device_mount_state(&self.volume.volume_name)
    }

    fn unmounted(&self) -> Vec<String> {
        self.fake.0.lock().unwrap().unmounted.clone()
    }
}

/// The detailed message `AttachedVolume.GenerateMsgDetailed` builds
/// (`operation_executor.go:576-583`, `generateVolumeMsgDetailed` `:315-317`).
fn detailed(prefix: &str, suffix: &str) -> String {
    format!(
        "{prefix} for volume {VOLUME:?} (UniqueName: \"{PLUGIN}/{VOLUME}\") on node \"node-1\" {suffix}"
    )
}

/// The simple message `AttachedVolume.GenerateMsg` builds for events
/// (`generateVolumeMsg`, `operation_executor.go:320-323`).
fn simple(prefix: &str, suffix: &str) -> String {
    format!("{prefix} for volume {VOLUME:?} {suffix}")
}

fn event(ctx: &OperationContext) -> Option<String> {
    ctx.event_err.as_ref().map(|e| e.to_string())
}

fn logged(ctx: &OperationContext) -> Option<String> {
    ctx.detailed_err.as_ref().map(|e| e.to_string())
}

// ---- generation-time failures (`:812-828`) ---------------------------------

#[test]
fn unknown_plugin_name_fails_generation() {
    let f = fixture_with(true, "no-such-plugin");
    let err = generate_unmount_device_func(&f.mgr, &f.volume, &f.asw, &FakeHostUtil::idle())
        .err()
        .expect("error");
    assert_eq!(
        err.to_string(),
        detailed(
            "UnmountDevice.FindDeviceMountablePluginByName failed",
            ": no volume plugin matched"
        )
    );
}

/// `FindDeviceMountablePluginByName` returns `(nil, nil)` for a plugin that is
/// not a `DeviceMountableVolumePlugin` (`plugins.go:857-860`), which the
/// generator reports through the same `GenerateErrorDetailed` with a nil error.
#[test]
fn plugin_that_is_not_device_mountable_fails_generation() {
    let f = fixture_with(false, PLUGIN);
    let err = generate_unmount_device_func(&f.mgr, &f.volume, &f.asw, &FakeHostUtil::idle())
        .err()
        .expect("error");
    assert_eq!(
        err.to_string(),
        detailed("UnmountDevice.FindDeviceMountablePluginByName failed", "")
    );
}

#[test]
fn new_device_unmounter_failure_fails_generation() {
    let f = fixture();
    f.fake.0.lock().unwrap().fail_new_unmounter = true;
    let err = generate_unmount_device_func(&f.mgr, &f.volume, &f.asw, &FakeHostUtil::idle())
        .err()
        .expect("error");
    assert_eq!(
        err.to_string(),
        detailed(
            "UnmountDevice.NewDeviceUnmounter failed",
            ": new unmounter boom"
        )
    );
}

#[test]
fn new_device_mounter_failure_fails_generation() {
    let f = fixture();
    f.fake.0.lock().unwrap().fail_new_mounter = true;
    let err = generate_unmount_device_func(&f.mgr, &f.volume, &f.asw, &FakeHostUtil::idle())
        .err()
        .expect("error");
    assert_eq!(
        err.to_string(),
        detailed(
            "UnmountDevice.NewDeviceMounter failed",
            ": new mounter boom"
        )
    );
}

#[test]
fn operation_is_named_unmount_device() {
    let f = fixture();
    let host = FakeHostUtil::idle();
    let op = generate_unmount_device_func(&f.mgr, &f.volume, &f.asw, &host).expect("generate");
    assert_eq!(op.operation_name, "unmount_device");
}

// ---- the closure (`:830-896`) ----------------------------------------------

/// The whole happy path: refs checked on the path the mounter computed,
/// `UnmountDevice` on it, `MarkDeviceAsUnmounted` (`:895`).
#[tokio::test]
async fn unmounts_the_device_and_marks_it_unmounted() {
    let f = fixture();
    let ctx = f.run(&FakeHostUtil::idle()).await;
    assert_eq!((event(&ctx), logged(&ctx)), (None, None));
    assert_eq!(f.unmounted(), vec![MOUNT_PATH.to_string()]);
    assert_eq!(
        f.fake.0.lock().unwrap().refs_asked,
        vec![MOUNT_PATH.to_string()]
    );
    assert_eq!(f.state(), DeviceMountState::DeviceNotMounted);
    assert!(!ctx.migrated);
}

/// `GetDeviceMountPath` failing with "does not exist" is not fatal: the path
/// recorded on the volume is used instead (`:824-836`).
#[tokio::test]
async fn missing_device_mount_path_falls_back_to_the_recorded_one() {
    let f = fixture();
    f.fake.0.lock().unwrap().mount_path = Some(Err("the volume does not exist".into()));
    let ctx = f.run(&FakeHostUtil::idle()).await;
    assert_eq!((event(&ctx), logged(&ctx)), (None, None));
    assert_eq!(f.unmounted(), vec!["/recorded/in/asw".to_string()]);
    assert_eq!(f.state(), DeviceMountState::DeviceNotMounted);
}

/// Any other `GetDeviceMountPath` failure is returned (`:826-830`).
#[tokio::test]
async fn other_device_mount_path_failure_is_returned() {
    let f = fixture();
    f.fake.0.lock().unwrap().mount_path = Some(Err("boom".into()));
    let ctx = f.run(&FakeHostUtil::idle()).await;
    assert_eq!(
        event(&ctx),
        Some(simple("GetDeviceMountPath failed", ": boom"))
    );
    assert_eq!(
        logged(&ctx),
        Some(detailed("GetDeviceMountPath failed", ": boom"))
    );
    assert!(f.unmounted().is_empty());
    assert_eq!(f.state(), DeviceMountState::DeviceGloballyMounted);
}

/// A failing `GetDeviceMountRefs` blocks the unmount (`:846-856`).
#[tokio::test]
async fn refs_lookup_failure_blocks_the_unmount() {
    let f = fixture();
    f.fake.0.lock().unwrap().refs = Some(Err("mountinfo unreadable".into()));
    let ctx = f.run(&FakeHostUtil::idle()).await;
    assert_eq!(
        event(&ctx),
        Some(simple(
            "GetDeviceMountRefs check failed",
            ": mountinfo unreadable"
        ))
    );
    assert!(f.unmounted().is_empty());
    assert_eq!(f.state(), DeviceMountState::DeviceGloballyMounted);
}

/// An outside reference to the device mount path blocks the unmount
/// (`HasMountRefs`, `:846-856`); the kubelet's own plugin-dir binds do not.
#[tokio::test]
async fn outside_reference_blocks_the_unmount() {
    let f = fixture();
    f.fake.0.lock().unwrap().refs = Some(Ok(vec!["/mnt/other".into()]));
    let ctx = f.run(&FakeHostUtil::idle()).await;
    assert_eq!(
        event(&ctx),
        Some(simple(
            "GetDeviceMountRefs check failed",
            &format!(
                ": the device mount path {MOUNT_PATH:?} is still mounted by other references [/mnt/other]"
            )
        ))
    );
    assert!(f.unmounted().is_empty());

    let f = fixture();
    f.fake.0.lock().unwrap().refs = Some(Ok(vec![format!("/some/root{MOUNT_PATH}")]));
    let ctx = f.run(&FakeHostUtil::idle()).await;
    assert_eq!((event(&ctx), logged(&ctx)), (None, None));
    assert_eq!(f.unmounted().len(), 1);
}

/// `UnmountDevice` failing marks the device uncertain so `MountDevice` is
/// called for new pods, and returns the error (`:859-870`).
#[tokio::test]
async fn unmount_failure_marks_the_device_uncertain() {
    let f = fixture();
    f.fake.0.lock().unwrap().unmount_error = Some("unstage boom".into());
    let ctx = f.run(&FakeHostUtil::idle()).await;
    assert_eq!(
        event(&ctx),
        Some(simple("UnmountDevice failed", ": unstage boom"))
    );
    assert_eq!(
        logged(&ctx),
        Some(detailed("UnmountDevice failed", ": unstage boom"))
    );
    assert_eq!(f.state(), DeviceMountState::DeviceMountUncertain);
}

/// A failing `MarkDeviceAsUncertain` is only logged ("There is nothing else
/// we can do", `:862-865`); the `UnmountDevice` error is still returned.
#[tokio::test]
async fn failing_mark_uncertain_does_not_replace_the_unmount_error() {
    let mut f = fixture();
    f.fake.0.lock().unwrap().unmount_error = Some("unstage boom".into());
    // The ASW has never heard of this volume.
    f.asw = ActualStateOfWorld::new("node-1", Arc::clone(&f.mgr));
    let ctx = f.run(&FakeHostUtil::idle()).await;
    assert_eq!(
        event(&ctx),
        Some(simple("UnmountDevice failed", ": unstage boom"))
    );
}

/// The device is still open elsewhere after a successful `UnmountDevice`:
/// uncertain, retry (`:874-893`).
#[tokio::test]
async fn device_still_open_marks_uncertain_and_retries() {
    let f = fixture();
    let host = FakeHostUtil {
        path_is_device: Ok(true),
        device_opened: Ok(true),
    };
    let ctx = f.run(&host).await;
    assert_eq!(
        event(&ctx),
        Some(simple(
            "UnmountDevice failed",
            ": the device is in use when it was no longer expected to be in use"
        ))
    );
    assert_eq!(f.unmounted().len(), 1);
    assert_eq!(f.state(), DeviceMountState::DeviceMountUncertain);
}

/// `isDeviceOpened`: `PathIsDevice` false, or an error mentioning "does not
/// exist", skips the check (`:2183-2188`).
#[tokio::test]
async fn not_a_device_or_missing_path_skips_the_opened_check() {
    for path_is_device in [
        Ok(false),
        Err(io::Error::other("path \"/x\" does not exist")),
    ] {
        let f = fixture();
        let host = FakeHostUtil {
            path_is_device,
            // Would fail the operation if it were consulted.
            device_opened: Ok(true),
        };
        let ctx = f.run(&host).await;
        assert_eq!((event(&ctx), logged(&ctx)), (None, None));
        assert_eq!(f.state(), DeviceMountState::DeviceNotMounted);
    }
}

/// Any other `PathIsDevice` error is returned as a detailed-only error, with
/// no event error and no state change (`:2189-2191`, `:877-880`).
#[tokio::test]
async fn path_is_device_error_is_a_detailed_only_failure() {
    let f = fixture();
    let host = FakeHostUtil {
        path_is_device: Err(io::Error::other("permission denied")),
        device_opened: Ok(false),
    };
    let ctx = f.run(&host).await;
    assert_eq!(event(&ctx), None);
    assert_eq!(
        logged(&ctx),
        Some(detailed("PathIsDevice failed", ": permission denied"))
    );
    assert_eq!(f.state(), DeviceMountState::DeviceGloballyMounted);
}

#[tokio::test]
async fn device_opened_error_is_a_detailed_only_failure() {
    let f = fixture();
    let host = FakeHostUtil {
        path_is_device: Ok(true),
        device_opened: Err(io::Error::other("ebadf")),
    };
    let ctx = f.run(&host).await;
    assert_eq!(event(&ctx), None);
    assert_eq!(
        logged(&ctx),
        Some(detailed("DeviceOpened failed", ": ebadf"))
    );
    assert_eq!(f.state(), DeviceMountState::DeviceGloballyMounted);
}

/// `MarkDeviceAsUnmounted` failing (volume absent from the ASW) is an error
/// the caller retries (`:895-901`).
#[tokio::test]
async fn failing_mark_unmounted_is_returned() {
    let mut f = fixture();
    f.asw = ActualStateOfWorld::new("node-1", Arc::clone(&f.mgr));
    let ctx = f.run(&FakeHostUtil::idle()).await;
    let msg = event(&ctx).expect("event error");
    assert!(
        msg.starts_with(&simple("MarkDeviceAsUnmounted failed", ": ")),
        "{msg}"
    );
    assert_eq!(f.unmounted().len(), 1);
}
