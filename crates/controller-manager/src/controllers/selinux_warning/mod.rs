//! SELinux warning controller.
//!
//! Port of `pkg/controller/volume/selinuxwarning/selinux_warning_controller.go`
//! and `volumehost.go` (release-1.35), wired as upstream's
//! `newSELinuxWarningControllerDescriptor`
//! (`cmd/kube-controller-manager/app/core.go:1107-1150`): disabled by default
//! and requiring the `SELinuxChangePolicy` feature gate.
//!
//! The controller emits a warning event and a metric when two pods *might*
//! use the same volume with different SELinux labels or change policies. It
//! is optional, does nothing on a cluster that has SELinux disabled, and does
//! not modify any API object except for the events. It watches *all* pods and
//! reports issues on all their volumes, regardless of whether the pods run,
//! are Pending forever, or can never land on the same node.
//!
//! Layers (each a port of the same-named upstream package):
//! - `translator`: `translator/selinux_translator.go`, `internal/parse`;
//! - `cache`: `cache/conflict.go`, `cache/volumecache.go`;
//! - `metrics`: `metrics.go`;
//! - this file: the controller, the controller-side `volume.VolumePluginMgr`
//!   (`FindPluginBySpec`, `GetVolumeName`, `SupportsSELinuxContextMount` of
//!   the plugins `ProbePersistentVolumePlugins` registers: hostpath, nfs, fc,
//!   iscsi, local, csi), `attachdetach/util.CreateVolumeSpec`,
//!   `volume/util.GetPodVolumeNames` and `volume/util.GetMountSELinuxLabel`.
//!
//! Deviations (no informer cache in this codebase, the controllers consume
//! raw storage watches, see [`super::ephemeral_volume`]):
//! - the pod/PVC/PV/CSIDriver "listers" are storage reads;
//! - a watch event carries no old object, so pod and PVC/PV updates are not
//!   diffed (`updatePod`, `updatePVC`, `updatePV`): every event enqueues. The
//!   cache short-circuits an unchanged pod (`AddVolume`), so a redundant sync
//!   emits nothing. CSIDriver updates are diffed against the last
//!   `seLinuxMount` this controller saw, as upstream diffs old against new;
//! - `GetMountSELinuxLabel` is ported here again (the kubelet's copy in
//!   `crates/kubelet` is tied to the kubelet's volume plugin manager);
//! - flexvolume plugins from the dynamic prober are not probed.

pub mod cache;
pub mod metrics;
pub mod translator;

use super::ephemeral_volume::{volume_claim_name, volume_is_for_pod};
use async_trait::async_trait;
use cache::{
    Conflict, ObjectName, VolumeCache, VolumeLabelCache, SELINUX_CHANGE_POLICY_MOUNT_OPTION,
    SELINUX_CHANGE_POLICY_RECURSIVE,
};
use futures::StreamExt;
use rusternetes_common::csi_migration::{translate_in_tree_spec_to_csi, PluginManager, VolumeSpec};
use rusternetes_common::csi_translation::CsiTranslator;
use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::resources::csi::CSIDriver;
use rusternetes_common::resources::pod::{PodSecurityContext, SELinuxOptions, Volume};
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::PersistentVolumeClaimPhase;
use rusternetes_common::resources::{
    EventSource, EventType, PersistentVolume, PersistentVolumeClaim, Pod,
};
use rusternetes_common::types::Phase;
use rusternetes_common::Error;
use rusternetes_storage::{
    build_key, build_prefix, extract_key, EventRecorder, Storage, WatchEvent, WorkQueue,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, info, warn};
use translator::ControllerSELinuxTranslator;

/// `csi.CSIPluginName` (`pkg/volume/csi/csi_plugin.go:54`).
pub const CSI_PLUGIN_NAME: &str = "kubernetes.io/csi";
/// `csi.volNameSep` (`csi_plugin.go:57`).
const CSI_VOL_NAME_SEP: &str = "^";

/// `volume.Spec.Name()` (`pkg/volume/plugins.go`).
pub fn spec_name(spec: &VolumeSpec) -> String {
    if let Some(v) = &spec.volume {
        v.name.clone()
    } else if let Some(pv) = &spec.persistent_volume {
        pv.metadata.name.clone()
    } else {
        String::new()
    }
}

/// `csi.GetCSIDriverName` (`pkg/volume/csi/csi_util.go:182`).
pub fn csi_driver_name(spec: &VolumeSpec) -> Result<String, String> {
    if let Some(v) = spec.volume.as_ref().and_then(|v| v.csi.as_ref()) {
        return Ok(v.driver.clone());
    }
    if let Some(pv) = spec
        .persistent_volume
        .as_ref()
        .and_then(|pv| pv.spec.csi.as_ref())
    {
        return Ok(pv.driver.clone());
    }
    Err("volume source not found in volume.Spec".to_string())
}

// ---------------------------------------------------------------------------
// Volume plugins
// ---------------------------------------------------------------------------

/// The part of `volume.VolumePlugin` this controller calls.
#[async_trait]
pub trait VolumePlugin: Send + Sync {
    /// `GetPluginName`.
    fn plugin_name(&self) -> &str;
    /// `CanSupport`.
    fn can_support(&self, spec: &VolumeSpec) -> bool;
    /// `GetVolumeName`.
    fn get_volume_name(&self, spec: &VolumeSpec) -> Result<String, String>;
    /// `SupportsSELinuxContextMount`.
    async fn supports_selinux_context_mount(&self, spec: &VolumeSpec) -> Result<bool, String>;
}

/// `FindPluginBySpec` (`pkg/volume/plugins.go`): exactly one plugin must
/// match.
pub fn find_plugin_by_spec<'a>(
    plugins: &'a [Arc<dyn VolumePlugin>],
    spec: &VolumeSpec,
) -> Result<&'a Arc<dyn VolumePlugin>, String> {
    let matched: Vec<&Arc<dyn VolumePlugin>> =
        plugins.iter().filter(|p| p.can_support(spec)).collect();
    match matched.as_slice() {
        [] => Err("no volume plugin matched".to_string()),
        [one] => Ok(one),
        many => Err(format!(
            "multiple volume plugins matched: {}",
            many.iter()
                .map(|p| p.plugin_name())
                .collect::<Vec<_>>()
                .join(",")
        )),
    }
}

/// Go's `%v` of a `[]string`: `[a b]`.
fn go_slice(items: &[String]) -> String {
    format!("[{}]", items.join(" "))
}

/// `hostPathPlugin` (`pkg/volume/hostpath/host_path.go`): name = path, no
/// SELinux mount.
struct HostPathPlugin;

#[async_trait]
impl VolumePlugin for HostPathPlugin {
    fn plugin_name(&self) -> &str {
        "kubernetes.io/host-path"
    }
    fn can_support(&self, spec: &VolumeSpec) -> bool {
        spec.persistent_volume
            .as_ref()
            .is_some_and(|pv| pv.spec.host_path.is_some())
            || spec.volume.as_ref().is_some_and(|v| v.host_path.is_some())
    }
    fn get_volume_name(&self, spec: &VolumeSpec) -> Result<String, String> {
        if let Some(h) = spec.volume.as_ref().and_then(|v| v.host_path.as_ref()) {
            return Ok(h.path.clone());
        }
        if let Some(h) = spec
            .persistent_volume
            .as_ref()
            .and_then(|pv| pv.spec.host_path.as_ref())
        {
            return Ok(h.path.clone());
        }
        Err("spec does not reference an HostPath volume type".to_string())
    }
    async fn supports_selinux_context_mount(&self, _: &VolumeSpec) -> Result<bool, String> {
        Ok(false)
    }
}

/// `nfsPlugin` (`pkg/volume/nfs/nfs.go`): name = `server/path`.
struct NfsPlugin;

#[async_trait]
impl VolumePlugin for NfsPlugin {
    fn plugin_name(&self) -> &str {
        "kubernetes.io/nfs"
    }
    fn can_support(&self, spec: &VolumeSpec) -> bool {
        spec.persistent_volume
            .as_ref()
            .is_some_and(|pv| pv.spec.nfs.is_some())
            || spec.volume.as_ref().is_some_and(|v| v.nfs.is_some())
    }
    fn get_volume_name(&self, spec: &VolumeSpec) -> Result<String, String> {
        let src = spec
            .volume
            .as_ref()
            .and_then(|v| v.nfs.as_ref())
            .or_else(|| {
                spec.persistent_volume
                    .as_ref()
                    .and_then(|pv| pv.spec.nfs.as_ref())
            })
            .ok_or_else(|| "spec does not reference a NFS volume type".to_string())?;
        Ok(format!("{}/{}", src.server, src.path))
    }
    async fn supports_selinux_context_mount(&self, _: &VolumeSpec) -> Result<bool, String> {
        Ok(false)
    }
}

/// `fcPlugin` (`pkg/volume/fc/fc.go`): supports SELinux mount.
struct FcPlugin;

impl FcPlugin {
    /// `getVolumeSource`.
    fn source(spec: &VolumeSpec) -> Option<rusternetes_common::resources::volume::FCVolumeSource> {
        if let Some(fc) = spec
            .volume
            .as_ref()
            .and_then(|v| v.legacy_sources.fc.clone())
        {
            return Some(fc);
        }
        spec.persistent_volume
            .as_ref()
            .and_then(|pv| pv.spec.fc.clone())
    }
}

#[async_trait]
impl VolumePlugin for FcPlugin {
    fn plugin_name(&self) -> &str {
        "kubernetes.io/fc"
    }
    fn can_support(&self, spec: &VolumeSpec) -> bool {
        spec.volume
            .as_ref()
            .is_some_and(|v| v.legacy_sources.fc.is_some())
            || spec
                .persistent_volume
                .as_ref()
                .is_some_and(|pv| pv.spec.fc.is_some())
    }
    fn get_volume_name(&self, spec: &VolumeSpec) -> Result<String, String> {
        let src = Self::source(spec)
            .ok_or_else(|| "spec does not reference a FibreChannel volume type".to_string())?;
        // API server validates these parameters beforehand but attach/detach
        // controller creates volumespec without validation (fc.go:74-77).
        if let (Some(wwns), Some(lun)) = (src.target_wwns.as_ref(), src.lun) {
            if !wwns.is_empty() {
                return Ok(format!("{}:{}", go_slice(wwns), lun));
            }
        }
        if let Some(wwids) = src.wwids.as_ref().filter(|w| !w.is_empty()) {
            return Ok(go_slice(wwids));
        }
        // Upstream `return "", err` with a nil err; the empty name then fails
        // `GetUniqueVolumeNameFromSpec`.
        Ok(String::new())
    }
    async fn supports_selinux_context_mount(&self, _: &VolumeSpec) -> Result<bool, String> {
        Ok(true)
    }
}

/// `iscsiPlugin` (`pkg/volume/iscsi/iscsi.go`): supports SELinux mount.
struct IscsiPlugin;

#[async_trait]
impl VolumePlugin for IscsiPlugin {
    fn plugin_name(&self) -> &str {
        "kubernetes.io/iscsi"
    }
    fn can_support(&self, spec: &VolumeSpec) -> bool {
        spec.volume.as_ref().is_some_and(|v| v.iscsi.is_some())
            || spec
                .persistent_volume
                .as_ref()
                .is_some_and(|pv| pv.spec.iscsi.is_some())
    }
    fn get_volume_name(&self, spec: &VolumeSpec) -> Result<String, String> {
        // getISCSITargetInfo
        let src = spec
            .volume
            .as_ref()
            .and_then(|v| v.iscsi.as_ref())
            .or_else(|| {
                spec.persistent_volume
                    .as_ref()
                    .and_then(|pv| pv.spec.iscsi.as_ref())
            })
            .ok_or_else(|| "Spec does not reference an ISCSI volume type".to_string())?;
        Ok(format!("{}:{}:{}", src.target_portal, src.iqn, src.lun))
    }
    async fn supports_selinux_context_mount(&self, _: &VolumeSpec) -> Result<bool, String> {
        Ok(true)
    }
}

/// `localVolumePlugin` (`pkg/volume/local/local.go`): PV only, name = PV name.
struct LocalPlugin;

#[async_trait]
impl VolumePlugin for LocalPlugin {
    fn plugin_name(&self) -> &str {
        "kubernetes.io/local-volume"
    }
    fn can_support(&self, spec: &VolumeSpec) -> bool {
        spec.persistent_volume
            .as_ref()
            .is_some_and(|pv| pv.spec.local.is_some())
    }
    fn get_volume_name(&self, spec: &VolumeSpec) -> Result<String, String> {
        // This volume is only supported as a PersistentVolumeSource, so the
        // PV name is unique.
        Ok(spec_name(spec))
    }
    async fn supports_selinux_context_mount(&self, _: &VolumeSpec) -> Result<bool, String> {
        Ok(false)
    }
}

/// `csiPlugin` (`pkg/volume/csi/csi_plugin.go`).
struct CsiPlugin<S: Storage> {
    storage: Arc<S>,
}

#[async_trait]
impl<S: Storage + 'static> VolumePlugin for CsiPlugin<S> {
    fn plugin_name(&self) -> &str {
        CSI_PLUGIN_NAME
    }
    fn can_support(&self, spec: &VolumeSpec) -> bool {
        spec.persistent_volume
            .as_ref()
            .is_some_and(|pv| pv.spec.csi.is_some())
            || spec.volume.as_ref().is_some_and(|v| v.csi.is_some())
    }
    /// `GetVolumeName` (`csi_plugin.go:437`): `driverName^volumeHandle` of the
    /// PV source; an inline `CSIVolumeSource` is an error
    /// (`getPVSourceFromSpec`).
    fn get_volume_name(&self, spec: &VolumeSpec) -> Result<String, String> {
        let prefix = "plugin.GetVolumeName failed to extract volume source from spec: ";
        if spec.volume.as_ref().is_some_and(|v| v.csi.is_some()) {
            return Err(format!(
                "{prefix}unexpected api.CSIVolumeSource found in volume.Spec"
            ));
        }
        let csi = spec
            .persistent_volume
            .as_ref()
            .and_then(|pv| pv.spec.csi.as_ref())
            .ok_or_else(|| format!("{prefix}volume source not found in volume.Spec"))?;
        Ok(format!(
            "{}{CSI_VOL_NAME_SEP}{}",
            csi.driver,
            csi.volume_handle.as_deref().unwrap_or("")
        ))
    }
    /// `SupportsSELinuxContextMount` (`csi_plugin.go:637`).
    async fn supports_selinux_context_mount(&self, spec: &VolumeSpec) -> Result<bool, String> {
        if !feature_gates::enabled(Feature::SELinuxMountReadWriteOncePod) {
            return Ok(false);
        }
        let driver = csi_driver_name(spec)?;
        match self
            .storage
            .get::<CSIDriver>(&build_key("csidrivers", None, &driver))
            .await
        {
            Ok(d) => Ok(d.spec.se_linux_mount.unwrap_or(false)),
            Err(Error::NotFound(_)) => Ok(false),
            Err(e) => Err(e.to_string()),
        }
    }
}

/// `ProbePersistentVolumePlugins` (`cmd/kube-controller-manager/app/plugins.go`):
/// the plugins the controller's `VolumePluginMgr` knows.
pub fn probe_persistent_volume_plugins<S: Storage + 'static>(
    storage: Arc<S>,
) -> Vec<Arc<dyn VolumePlugin>> {
    vec![
        Arc::new(HostPathPlugin),
        Arc::new(NfsPlugin),
        Arc::new(FcPlugin),
        Arc::new(IscsiPlugin),
        Arc::new(LocalPlugin),
        Arc::new(CsiPlugin { storage }),
    ]
}

// ---------------------------------------------------------------------------
// volume/util helpers
// ---------------------------------------------------------------------------

/// The two error kinds `GetMountSELinuxLabel` produces
/// (`pkg/volume/util/selinux.go:158-221`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SELinuxLabelError {
    /// `MultipleSELinuxLabelsError`.
    MultipleLabels(Vec<String>),
    /// Anything else (`SELinuxLabelTranslationError`, plugin errors).
    Other(String),
}

impl std::fmt::Display for SELinuxLabelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MultipleLabels(l) => {
                write!(f, "multiple SELinux labels found: {}", l.join(","))
            }
            Self::Other(s) => f.write_str(s),
        }
    }
}

/// `SELinuxLabelInfo` (`pkg/volume/util/selinux.go:234`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SELinuxLabelInfo {
    pub selinux_mount_label: String,
    pub selinux_process_label: String,
    pub plugin_supports_selinux_context_mount: bool,
}

/// `GetPodVolumeNames(pod, collectSELinuxOptions=true)`
/// (`pkg/volume/util/util.go`): the names of the volumes the pod's
/// containers mount, and for each the effective SELinux options of every
/// container that mounts it (`DetermineEffectiveSecurityContext`: the
/// container's options override the pod's; containers without any options
/// are not recorded). Init, regular and ephemeral containers are visited.
pub fn get_pod_volume_names(pod: &Pod) -> (BTreeSet<String>, HashMap<String, Vec<SELinuxOptions>>) {
    let mut mounts = BTreeSet::new();
    let mut labels: HashMap<String, Vec<SELinuxOptions>> = HashMap::new();
    let Some(spec) = pod.spec.as_ref() else {
        return (mounts, labels);
    };
    let pod_opts = spec
        .security_context
        .as_ref()
        .and_then(|sc| sc.se_linux_options.as_ref());

    let mut visit =
        |volume_mounts: Option<&Vec<rusternetes_common::resources::pod::VolumeMount>>,
         container_opts: Option<&SELinuxOptions>| {
            let effective = container_opts.or(pod_opts);
            for m in volume_mounts.into_iter().flatten() {
                mounts.insert(m.name.clone());
                if let Some(o) = effective {
                    labels.entry(m.name.clone()).or_default().push(o.clone());
                }
            }
        };
    for c in spec.init_containers.iter().flatten() {
        visit(
            c.volume_mounts.as_ref(),
            c.security_context
                .as_ref()
                .and_then(|sc| sc.se_linux_options.as_ref()),
        );
    }
    for c in &spec.containers {
        visit(
            c.volume_mounts.as_ref(),
            c.security_context
                .as_ref()
                .and_then(|sc| sc.se_linux_options.as_ref()),
        );
    }
    for c in spec.ephemeral_containers.iter().flatten() {
        visit(
            c.volume_mounts.as_ref(),
            c.security_context
                .as_ref()
                .and_then(|sc| sc.se_linux_options.as_ref()),
        );
    }
    (mounts, labels)
}

/// `GetMountSELinuxLabel` (`pkg/volume/util/selinux.go:246-313`).
///
/// Like upstream, the caller reads `plugin_supports_selinux_context_mount`
/// only when there is no error.
pub async fn get_mount_selinux_label(
    spec: &VolumeSpec,
    effective_container_labels: &[SELinuxOptions],
    pod_security_context: Option<&PodSecurityContext>,
    plugins: &[Arc<dyn VolumePlugin>],
    translator: &ControllerSELinuxTranslator,
) -> Result<SELinuxLabelInfo, SELinuxLabelError> {
    let mut info = SELinuxLabelInfo::default();
    if !feature_gates::enabled(Feature::SELinuxMountReadWriteOncePod) {
        return Ok(info);
    }
    if !translator.selinux_enabled() {
        return Ok(info);
    }

    // `SupportsSELinuxContextMount`: the plugin-lookup error is discarded.
    let supports = match find_plugin_by_spec(plugins, spec) {
        Ok(p) => p
            .supports_selinux_context_mount(spec)
            .await
            .map_err(SELinuxLabelError::Other)?,
        Err(_) => false,
    };
    info.plugin_supports_selinux_context_mount = supports;

    // Collect all SELinux options from all containers that use this volume.
    // A set squashes duplicates.
    let labels: BTreeSet<String> = effective_container_labels
        .iter()
        .map(|o| translator.selinux_options_to_file_label(Some(o)))
        .collect();

    // Ensure that all containers use the same SELinux label.
    if labels.len() > 1 {
        return Err(SELinuxLabelError::MultipleLabels(
            labels.into_iter().collect(),
        ));
    }
    let Some(lbl) = labels.into_iter().next() else {
        return Ok(info);
    };

    info.selinux_process_label = lbl.clone();
    info.selinux_mount_label = lbl;

    if feature_gates::enabled(Feature::SELinuxChangePolicy)
        && pod_security_context
            .and_then(|sc| sc.se_linux_change_policy.as_deref())
            .is_some_and(|p| p == SELINUX_CHANGE_POLICY_RECURSIVE)
    {
        // The pod has opted into recursive SELinux label changes. Do not
        // mount with -o context.
        info.selinux_mount_label = String::new();
    }
    if !supports {
        // The volume plugin does not support SELinux mount. Do not mount with
        // -o context.
        info.selinux_mount_label = String::new();
    }
    Ok(info)
}

/// `errorutils.NewAggregate(errs).Error()`.
fn aggregate(errs: &[String]) -> String {
    match errs {
        [one] => one.clone(),
        many => format!("[{}]", many.join(", ")),
    }
}

// ---------------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------------

/// `eventBroadcaster.NewRecorder(..., EventSource{Component: "selinux_warning"})`.
const EVENT_COMPONENT: &str = "selinux_warning";

/// Safety-net full resync. Upstream relies on the shared informer's resync
/// period; a re-list also happens whenever the watch is re-established.
const RESYNC: Duration = Duration::from_secs(600);

/// `ControllerDescriptor.requiredFeatureGates` of the controller
/// (`core.go:1112-1114`): `SELinuxChangePolicy`. Upstream skips a controller
/// whose gates are off and logs why; so does this.
pub fn feature_gates_enabled() -> bool {
    if feature_gates::enabled(Feature::SELinuxChangePolicy) {
        return true;
    }
    warn!(
        "Skipping the selinux-warning-controller: the SELinuxChangePolicy feature gate is disabled"
    );
    false
}

/// `Controller`.
pub struct SELinuxWarningController<S: Storage> {
    storage: Arc<S>,
    recorder: EventRecorder<S>,
    plugins: Vec<Arc<dyn VolumePlugin>>,
    /// `cmpm`.
    plugin_manager: PluginManager,
    /// `csiTranslator`.
    csi_translator: CsiTranslator,
    /// `seLinuxTranslator`.
    se_linux_translator: ControllerSELinuxTranslator,
    /// `labelCache`.
    label_cache: Arc<dyn VolumeCache>,
    /// The `spec.seLinuxMount` last seen per CSIDriver, to tell what an
    /// update changed (upstream compares the informer's old and new object).
    known_csi_drivers: Mutex<HashMap<String, bool>>,
}

impl<S: Storage + 'static> SELinuxWarningController<S> {
    /// `NewController`.
    pub fn new(storage: Arc<S>) -> Self {
        let label_cache: Arc<dyn VolumeCache> =
            Arc::new(VolumeLabelCache::new(ControllerSELinuxTranslator));
        metrics::register_metrics(label_cache.clone());
        Self {
            recorder: EventRecorder::new(Arc::clone(&storage)),
            plugins: probe_persistent_volume_plugins(Arc::clone(&storage)),
            storage,
            plugin_manager: PluginManager::new(),
            csi_translator: CsiTranslator::new(),
            se_linux_translator: ControllerSELinuxTranslator,
            label_cache,
            known_csi_drivers: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    fn with_plugins(mut self, plugins: Vec<Arc<dyn VolumePlugin>>) -> Self {
        self.plugins = plugins;
        self
    }

    #[cfg(test)]
    fn with_label_cache(mut self, cache: Arc<dyn VolumeCache>) -> Self {
        self.label_cache = cache;
        self
    }

    fn pod_queue_key(key: &ObjectName) -> String {
        format!("pods/{}/{}", key.namespace, key.name)
    }

    async fn enqueue_pod(&self, queue: &WorkQueue, key: &ObjectName) {
        queue.add(Self::pod_queue_key(key)).await;
    }

    /// `enqueueAllPodsForPVC`: the pods referencing the claim, found through
    /// `common.PodPVCIndexFunc` upstream (`claimName`, or the generated claim
    /// name of a generic ephemeral volume).
    async fn enqueue_all_pods_for_pvc(&self, queue: &WorkQueue, namespace: &str, name: &str) {
        let prefix = build_prefix("pods", Some(namespace));
        let pods = match self.storage.list::<Pod>(&prefix).await {
            Ok(p) => p,
            Err(e) => {
                error!("SELinux warning: listing pods from storage: {e}");
                return;
            }
        };
        for pod in pods {
            let uses = pod
                .spec
                .as_ref()
                .into_iter()
                .flat_map(|s| s.volumes.iter().flatten())
                .any(|v| {
                    v.persistent_volume_claim
                        .as_ref()
                        .is_some_and(|p| p.claim_name == name)
                        || (v.ephemeral.is_some() && volume_claim_name(&pod, v) == name)
                });
            if uses {
                self.enqueue_pod(queue, &Self::object_name(&pod)).await;
            }
        }
    }

    fn object_name(pod: &Pod) -> ObjectName {
        ObjectName::new(
            pod.metadata.namespace.clone().unwrap_or_default(),
            pod.metadata.name.clone(),
        )
    }

    /// `enqueueAllPodsForCSIDriver`.
    async fn enqueue_all_pods_for_csi_driver(&self, queue: &WorkQueue, driver: &str) {
        for key in self.label_cache.get_pods_for_csi_driver(driver) {
            self.enqueue_pod(queue, &key).await;
        }
    }

    /// `addCSIDriver` / `updateCSIDriver` / `deleteCSIDriver`.
    async fn on_csi_driver_event(&self, queue: &WorkQueue, ev: &WatchEvent) {
        let key = extract_key(ev);
        let name = key.rsplit('/').next().unwrap_or("").to_string();
        let new_mount = |v: &str| {
            serde_json::from_str::<CSIDriver>(v)
                .ok()
                .map(|d| d.spec.se_linux_mount.unwrap_or(false))
        };
        match ev {
            WatchEvent::Added(_, v) | WatchEvent::Modified(_, v) => {
                let Some(now) = new_mount(v) else { return };
                let old = self
                    .known_csi_drivers
                    .lock()
                    .expect("csi driver map poisoned")
                    .insert(name.clone(), now);
                // add: SELinuxMount may have changed, so re-queue all pods that
                // use this CSIDriver. update: only when it did change.
                if old.is_none() || old != Some(now) {
                    self.enqueue_all_pods_for_csi_driver(queue, &name).await;
                }
            }
            WatchEvent::Deleted(_, v) => {
                self.known_csi_drivers
                    .lock()
                    .expect("csi driver map poisoned")
                    .remove(&name);
                // A deleted driver that announced SELinuxMount falls back to
                // `SELinuxMount: false`, which changes the effective policy.
                if new_mount(v) == Some(true) {
                    self.enqueue_all_pods_for_csi_driver(queue, &name).await;
                }
            }
        }
    }

    /// The initial "add" of every existing object.
    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self.storage.list::<Pod>(&build_prefix("pods", None)).await {
            Ok(pods) => {
                for pod in pods {
                    self.enqueue_pod(queue, &Self::object_name(&pod)).await;
                }
            }
            Err(e) => error!("SELinux warning: failed to list pods: {e}"),
        }
        match self
            .storage
            .list::<CSIDriver>(&build_prefix("csidrivers", None))
            .await
        {
            Ok(drivers) => {
                let mut known = self.known_csi_drivers.lock().expect("poisoned");
                for d in drivers {
                    known.insert(
                        d.metadata.name.clone(),
                        d.spec.se_linux_mount.unwrap_or(false),
                    );
                }
            }
            Err(e) => error!("SELinux warning: failed to list CSI drivers: {e}"),
        }
    }

    /// `processNextWorkItem` loop, one worker (`Run(ctx, 1)`).
    async fn run_worker(self: Arc<Self>, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            if parts.len() == 3 {
                match self.sync(&ObjectName::new(parts[1], parts[2])).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        error!("SELinux warning: {key} failed with: {e}");
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                }
            } else {
                queue.forget(&key).await;
            }
            queue.done(&key).await;
        }
    }

    /// `Run`: watch pods, PVCs, PVs and CSIDrivers and keep the conflict
    /// cache current.
    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        info!("Starting SELinux warning controller");
        let queue = WorkQueue::new();
        tokio::spawn(self.clone().run_worker(queue.clone()));

        loop {
            self.enqueue_all(&queue).await;
            let watches = (
                self.storage.watch(&build_prefix("pods", None)).await,
                self.storage
                    .watch(&build_prefix("persistentvolumeclaims", None))
                    .await,
                self.storage
                    .watch(&build_prefix("persistentvolumes", None))
                    .await,
                self.storage.watch(&build_prefix("csidrivers", None)).await,
            );
            let (mut pods, mut pvcs, mut pvs, mut drivers) = match watches {
                (Ok(a), Ok(b), Ok(c), Ok(d)) => (a, b, c, d),
                _ => {
                    warn!("SELinux warning: watch failed, retrying");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let mut resync = tokio::time::interval(RESYNC);
            resync.tick().await;
            loop {
                tokio::select! {
                    ev = pods.next() => match ev {
                        // enqueuePod (add, delete) / updatePod
                        Some(Ok(ev)) => {
                            // "pods/<ns>/<name>"
                            let k = extract_key(&ev);
                            let parts: Vec<&str> = k.splitn(3, '/').collect();
                            if let [_, ns, name] = parts.as_slice() {
                                self.enqueue_pod(&queue, &ObjectName::new(*ns, *name)).await;
                            }
                        }
                        _ => break,
                    },
                    ev = pvcs.next() => match ev {
                        Some(Ok(WatchEvent::Added(_, v) | WatchEvent::Modified(_, v))) => {
                            // addPVC / updatePVC: ignore unbound PVCs.
                            if let Ok(pvc) = serde_json::from_str::<PersistentVolumeClaim>(&v) {
                                if pvc.spec.volume_name.as_deref().is_some_and(|n| !n.is_empty()) {
                                    let ns = pvc.metadata.namespace.clone().unwrap_or_default();
                                    self.enqueue_all_pods_for_pvc(&queue, &ns, &pvc.metadata.name).await;
                                }
                            }
                        }
                        Some(Ok(WatchEvent::Deleted(..))) => {}
                        _ => break,
                    },
                    ev = pvs.next() => match ev {
                        Some(Ok(WatchEvent::Added(_, v) | WatchEvent::Modified(_, v))) => {
                            // addPV / updatePV: only PVs with a complete binding.
                            if let Ok(pv) = serde_json::from_str::<PersistentVolume>(&v) {
                                if let Some(cr) = pv.spec.claim_ref.as_ref() {
                                    if cr.uid.as_deref().is_some_and(|u| !u.is_empty()) {
                                        self.enqueue_all_pods_for_pvc(
                                            &queue,
                                            cr.namespace.as_deref().unwrap_or(""),
                                            cr.name.as_deref().unwrap_or(""),
                                        ).await;
                                    }
                                }
                            }
                        }
                        Some(Ok(WatchEvent::Deleted(..))) => {}
                        _ => break,
                    },
                    ev = drivers.next() => match ev {
                        Some(Ok(ev)) => self.on_csi_driver_event(&queue, &ev).await,
                        _ => break,
                    },
                    _ = resync.tick() => self.enqueue_all(&queue).await,
                }
            }
        }
    }

    /// `sync`: invoked for each pod which might need to be processed. An
    /// error requeues the pod with backoff.
    pub async fn sync(&self, pod_ref: &ObjectName) -> Result<(), String> {
        let key = build_key("pods", Some(&pod_ref.namespace), &pod_ref.name);
        let pod = match self.storage.get::<Pod>(&key).await {
            Ok(p) => p,
            // The pod must have been deleted.
            Err(Error::NotFound(_)) => return self.sync_pod_delete(pod_ref),
            Err(e) => {
                debug!("Error getting pod {pod_ref}: {e}");
                return Err(e.to_string());
            }
        };
        let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
        if matches!(phase, Some(Phase::Failed) | Some(Phase::Succeeded)) {
            // The pod has reached its final state and kubelet is unmounting
            // its volumes. Remove them from the cache.
            return self.sync_pod_delete(pod_ref);
        }
        self.sync_pod(&pod).await
    }

    /// `syncPodDelete`.
    fn sync_pod_delete(&self, pod_key: &ObjectName) -> Result<(), String> {
        debug!("Deleting pod {pod_key}");
        self.label_cache.delete_pod(pod_key);
        Ok(())
    }

    /// `syncPod`.
    async fn sync_pod(&self, pod: &Pod) -> Result<(), String> {
        let mut errs: Vec<String> = Vec::new();
        let mut volume_specs: HashMap<String, VolumeSpec> = HashMap::new();
        let pod_name = format!(
            "{}/{}",
            pod.metadata.namespace.as_deref().unwrap_or(""),
            pod.metadata.name
        );
        let spec = pod.spec.as_ref();

        // Pre-compute volumes.
        for vol in spec.into_iter().flat_map(|s| s.volumes.iter().flatten()) {
            match self.create_volume_spec(vol, pod).await {
                Ok(s) => {
                    volume_specs.insert(vol.name.clone(), s);
                }
                // This can happen frequently when PVC or PV do not exist yet.
                // Report it, but continue further.
                Err(e) => errs.push(e),
            }
        }

        let (mounts, se_linux_labels) = get_pod_volume_names(pod);
        let pod_sc = spec.and_then(|s| s.security_context.as_ref());
        for mount in &mounts {
            let opts = se_linux_labels.get(mount).map(Vec::as_slice).unwrap_or(&[]);
            let Some(vspec) = volume_specs.get(mount) else {
                // A volume that failed CreateVolumeSpec above; the error is
                // reported.
                debug!("skipping not found volume {mount} of pod {pod_name}");
                continue;
            };
            let info = match get_mount_selinux_label(
                vspec,
                opts,
                pod_sc,
                &self.plugins,
                &self.se_linux_translator,
            )
            .await
            {
                Ok(i) => i,
                Err(e) => {
                    if matches!(e, SELinuxLabelError::MultipleLabels(_)) {
                        self.record_pod_event(
                            pod,
                            EventType::Warning,
                            "MultipleSELinuxLabels",
                            &format!(
                                "Volume {mount:?} is mounted twice with different SELinux labels inside this pod"
                            ),
                        )
                        .await;
                    }
                    debug!("failed to get SELinux label for {pod_name} volume {mount}: {e}");
                    errs.push(e.to_string());
                    continue;
                }
            };

            // Use the same label as kubelet will use for mount -o context. If
            // the Pod has opted in to Recursive policy, it will be empty
            // string here and no conflicts will be reported for it.
            if let Err(e) = self
                .sync_volume(
                    pod,
                    vspec,
                    &info.selinux_mount_label,
                    info.plugin_supports_selinux_context_mount,
                )
                .await
            {
                errs.push(e);
            }
        }

        if errs.is_empty() {
            Ok(())
        } else {
            Err(aggregate(&errs))
        }
    }

    /// `syncVolume`.
    async fn sync_volume(
        &self,
        pod: &Pod,
        spec: &VolumeSpec,
        selinux_label: &str,
        plugin_supports_selinux_context_mount: bool,
    ) -> Result<(), String> {
        let plugin = match find_plugin_by_spec(&self.plugins, spec) {
            Ok(p) => p,
            Err(e) => {
                // The controller does not have all volume plugins, only those
                // that affect SELinux.
                debug!("Skipping volume {} of unknown plugin: {e}", spec_name(spec));
                return Ok(());
            }
        };

        // GetUniqueVolumeNameFromSpec
        let volume_name = plugin.get_volume_name(spec).map_err(|e| {
            format!(
                "failed to get unique volume name for volume {:?}: failed to GetVolumeName from volumePlugin for volumeSpec {:?} err={e}",
                spec_name(spec),
                spec_name(spec)
            )
        })?;
        if volume_name.is_empty() {
            return Err(format!(
                "failed to get unique volume name for volume {:?}: failed to GetVolumeName from volumePlugin for volumeSpec {:?} err=<nil>",
                spec_name(spec),
                spec_name(spec)
            ));
        }
        let unique_volume_name = format!("{}/{}", plugin.plugin_name(), volume_name);

        let mut change_policy = SELINUX_CHANGE_POLICY_MOUNT_OPTION.to_string();
        if let Some(p) = pod
            .spec
            .as_ref()
            .and_then(|s| s.security_context.as_ref())
            .and_then(|sc| sc.se_linux_change_policy.as_ref())
        {
            change_policy = p.clone();
            debug!("Using Pod SELinux change policy {change_policy}");
        }
        if !plugin_supports_selinux_context_mount
            && change_policy != SELINUX_CHANGE_POLICY_RECURSIVE
        {
            debug!(
                "Volume {} does not support SELinux context mount, setting changePolicy to Recursive",
                spec_name(spec)
            );
            change_policy = SELINUX_CHANGE_POLICY_RECURSIVE.to_string();
        }
        if selinux_label.is_empty() && change_policy != SELINUX_CHANGE_POLICY_RECURSIVE {
            debug!("Pod has empty SELinux label, setting changePolicy to Recursive");
            change_policy = SELINUX_CHANGE_POLICY_RECURSIVE.to_string();
        }

        // This is likely not a CSI volume when this fails.
        let csi_driver = csi_driver_name(spec).unwrap_or_default();
        debug!(
            "Syncing pod volume {} label {selinux_label:?} uniqueVolumeName {unique_volume_name} changePolicy {change_policy} csiDriver {csi_driver}",
            spec_name(spec)
        );

        let conflicts = self.label_cache.add_volume(
            &unique_volume_name,
            &Self::object_name(pod),
            selinux_label,
            &change_policy,
            &csi_driver,
        );
        self.report_conflict_events(&conflicts).await;
        Ok(())
    }

    /// `reportConflictEvents`.
    async fn report_conflict_events(&self, conflicts: &[Conflict]) {
        for conflict in conflicts {
            let key = build_key("pods", Some(&conflict.pod.namespace), &conflict.pod.name);
            let pod = match self.storage.get::<Pod>(&key).await {
                Ok(p) => p,
                Err(e) => {
                    debug!("failed to get first pod {} for event: {e}", conflict.pod);
                    // It does not make sense to report a conflict that has
                    // been resolved by deleting one of the pods.
                    return;
                }
            };
            self.record_pod_event(
                &pod,
                EventType::Normal,
                &conflict.event_reason,
                &conflict.event_message(),
            )
            .await;
        }
    }

    async fn record_pod_event(&self, pod: &Pod, event_type: EventType, reason: &str, msg: &str) {
        let involved = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: pod.metadata.namespace.clone(),
            name: Some(pod.metadata.name.clone()),
            uid: Some(pod.metadata.uid.clone()),
            api_version: Some("v1".to_string()),
            ..Default::default()
        };
        let source = EventSource {
            component: EVENT_COMPONENT.to_string(),
            host: None,
        };
        if let Err(e) = self
            .recorder
            .event(&involved, &source, event_type, reason, msg)
            .await
        {
            warn!("failed to record {reason} event: {e}");
        }
    }

    /// `attachdetach/util.CreateVolumeSpec` (`util.go:84`):
    /// `createInTreeVolumeSpec` + `translateInTreeSpecToCSIIfNeeded`.
    async fn create_volume_spec(&self, vol: &Volume, pod: &Pod) -> Result<VolumeSpec, String> {
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("");
        let (spec, claim_name) = self.create_in_tree_volume_spec(vol, pod).await?;
        // translateInTreeSpecToCSIIfNeeded
        let migratable = self.plugin_manager.is_migratable(Some(&spec))?;
        if !migratable {
            // Jump out of translation fast.
            return Ok(spec);
        }
        translate_in_tree_spec_to_csi(&spec, namespace, &self.csi_translator).map_err(|e| {
            format!(
                "error performing CSI migration checks and translation for PVC {namespace:?}/{claim_name:?}: {e}"
            )
        })
    }

    /// `createInTreeVolumeSpec` (`util.go:122`), with `getPVCFromCache` and
    /// `getPVSpecFromCache`.
    async fn create_in_tree_volume_spec(
        &self,
        vol: &Volume,
        pod: &Pod,
    ) -> Result<(VolumeSpec, String), String> {
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("");
        let mut claim_name = String::new();
        let mut read_only = false;
        if let Some(src) = vol.persistent_volume_claim.as_ref() {
            claim_name = src.claim_name.clone();
            read_only = src.read_only.unwrap_or(false);
        }
        let is_ephemeral = vol.ephemeral.is_some();
        if is_ephemeral {
            claim_name = volume_claim_name(pod, vol);
        }
        if claim_name.is_empty() {
            // In-line volume.
            return Ok((
                VolumeSpec {
                    volume: Some(vol.clone()),
                    ..Default::default()
                },
                String::new(),
            ));
        }
        let wrap = |e: String| format!("error processing PVC {namespace:?}/{claim_name:?}: {e}");

        // If the volume is a PVC, fetch the real PV behind the claim.
        let pvc: PersistentVolumeClaim = self
            .storage
            .get(&build_key(
                "persistentvolumeclaims",
                Some(namespace),
                &claim_name,
            ))
            .await
            .map_err(|e| {
                wrap(format!(
                    "failed to find PVC {namespace}/{claim_name} in storage: {e}"
                ))
            })?;
        let phase = pvc
            .status
            .as_ref()
            .map(|s| s.phase.clone())
            .unwrap_or_default();
        let pv_name = pvc.spec.volume_name.clone().unwrap_or_default();
        if phase != PersistentVolumeClaimPhase::Bound || pv_name.is_empty() {
            return Err(wrap(format!(
                "PVC {namespace}/{claim_name} has non-bound phase ({phase:?}) or empty pvc.Spec.VolumeName ({pv_name:?})"
            )));
        }
        if is_ephemeral {
            // `ephemeral.VolumeIsForPod`; the error is returned unwrapped.
            volume_is_for_pod(pod, &pvc).map_err(|e| e.to_string())?;
        }

        let pv: PersistentVolume = self
            .storage
            .get(&build_key("persistentvolumes", None, &pv_name))
            .await
            .map_err(|e| wrap(format!("failed to find PV {pv_name:?} in storage: {e}")))?;
        let Some(claim_ref) = pv.spec.claim_ref.as_ref() else {
            return Err(wrap(format!(
                "found PV object {pv_name:?} but it has a nil pv.Spec.ClaimRef indicating it is not yet bound to the claim"
            )));
        };
        let claim_uid = claim_ref.uid.clone().unwrap_or_default();
        if claim_uid != pvc.metadata.uid {
            return Err(wrap(format!(
                "found PV object {pv_name:?} but its pv.Spec.ClaimRef.UID ({claim_uid:?}) does not point to claim.UID ({:?})",
                pvc.metadata.uid
            )));
        }
        Ok((
            VolumeSpec {
                persistent_volume: Some(pv),
                read_only,
                ..Default::default()
            },
            claim_name,
        ))
    }
}

#[cfg(test)]
mod tests;
