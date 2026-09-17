//! Port of `pkg/volume/util/selinux.go` — translating a container's
//! `v1.SELinuxOptions` into the file label a volume must be mounted with.

use crate::volume_plugins::plugin::Spec;
use crate::volume_plugins::registry::VolumePluginMgr;
use crate::volume_plugins::util::contains_access_mode;
use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::resources::pod::{PodSecurityContext, SELinuxOptions};
use rusternetes_common::resources::volume::PersistentVolumeAccessMode;
use std::collections::BTreeSet;

/// Port of `SELinuxLabelTranslator` (`pkg/volume/util/selinux.go:34-44`).
pub trait SELinuxLabelTranslator: Send + Sync {
    /// `SELinuxOptionsToFileLabel` (`selinux.go:39`). Returns `""` and no error
    /// on platforms without SELinux.
    fn selinux_options_to_file_label(
        &self,
        opts: Option<&SELinuxOptions>,
    ) -> Result<String, SELinuxLabelError>;

    /// `SELinuxEnabled` (`selinux.go:43`).
    fn selinux_enabled(&self) -> bool;
}

/// Port of `translator` / `NewSELinuxLabelTranslator`
/// (`pkg/volume/util/selinux.go:46-56`, `:104-106`).
///
/// **Deliberate deviation, flagged.** Upstream's real translator calls
/// `github.com/opencontainers/selinux`: `SELinuxEnabled` is `selinux.GetEnabled()`
/// and the label is built by `label.InitLabels`. Rusternetes has no SELinux
/// binding and no container-runtime label allocation to release afterwards, so
/// this reports SELinux as disabled — which is exactly upstream's own
/// behaviour on a platform that does not have SELinux enabled, the case its
/// doc comment calls out ("It returns "" and no error on platforms that do not
/// have SELinux enabled or don't support SELinux at all"). The consequence is
/// that every label is `""`, which is what every caller in this crate already
/// passes; it is not a silent behaviour change, but it *is* a gap to close
/// when SELinux mounting is implemented.
pub struct Translator;

impl SELinuxLabelTranslator for Translator {
    fn selinux_options_to_file_label(
        &self,
        _opts: Option<&SELinuxOptions>,
    ) -> Result<String, SELinuxLabelError> {
        Ok(String::new())
    }

    fn selinux_enabled(&self) -> bool {
        false
    }
}

/// Port of `fakeTranslator` / `NewFakeSELinuxLabelTranslator`
/// (`pkg/volume/util/selinux.go:108-160`).
///
/// Upstream keeps this in non-test code so the volume-manager tests can
/// exercise the SELinux paths on a machine without SELinux; the same is true
/// here, so it is not `#[cfg(test)]`.
pub struct FakeSELinuxLabelTranslator;

impl SELinuxLabelTranslator for FakeSELinuxLabelTranslator {
    /// `fakeTranslator.SELinuxOptionsToFileLabel` (`selinux.go:120-152`).
    /// Fills empty fields from "system defaults" taken from Fedora Linux, and
    /// translates the *process* type `container_t` to the *file* type
    /// `container_file_t`.
    fn selinux_options_to_file_label(
        &self,
        opts: Option<&SELinuxOptions>,
    ) -> Result<String, SELinuxLabelError> {
        let Some(opts) = opts else {
            return Ok(String::new());
        };

        let user = match opts.user.as_deref() {
            Some("") | None => "system_u",
            Some(u) => u,
        };
        let role = match opts.role.as_deref() {
            Some("") | None => "object_r",
            Some(r) => r,
        };
        let file_type = match opts.type_.as_deref() {
            Some("") | None | Some("container_t") => "container_file_t",
            Some(t) => t,
        };
        let level = match opts.level.as_deref() {
            Some("") | None => "s0:c998,c999",
            Some(l) => l,
        };

        Ok(format!("{user}:{role}:{file_type}:{level}"))
    }

    /// `fakeTranslator.SELinuxEnabled` (`selinux.go:154-156`): always `true`.
    fn selinux_enabled(&self) -> bool {
        true
    }
}

/// The two error kinds `GetMountSELinuxLabel` can produce, which its caller
/// distinguishes with `IsSELinuxLabelTranslationError` (`selinux.go:166-169`)
/// and `IsMultipleSELinuxLabelsError` (`selinux.go:227-230`) to bump different
/// metrics.
///
/// Go's `errors.As` type-switch becomes an enum match; `Other` stands for
/// upstream's fall-through arm (`desired_state_of_world.go:433`), which today
/// can only carry the error from `SupportsSELinuxContextMount`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SELinuxLabelError {
    /// `SELinuxLabelTranslationError` (`selinux.go:158-164`). Its `Error()` is
    /// the wrapped message verbatim, with no prefix.
    #[error("{0}")]
    Translation(String),

    /// `MultipleSELinuxLabelsError` (`selinux.go:210-221`):
    /// `fmt.Sprintf("multiple SELinux labels found: %s", strings.Join(labels, ","))`.
    #[error("multiple SELinux labels found: {}", .0.join(","))]
    MultipleLabels(Vec<String>),

    /// Any other error, e.g. from `SupportsSELinuxContextMount`.
    #[error("{0}")]
    Other(String),
}

/// Port of `SELinuxLabelInfo` (`pkg/volume/util/selinux.go:234-244`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SELinuxLabelInfo {
    /// The label the volume should be mounted with, or `""` when the plugin
    /// does not support SELinux mount or the pod opted out.
    pub selinux_mount_label: String,
    /// The label the container runtime will use for the pod, regardless of the
    /// above.
    pub selinux_process_label: String,
    /// Whether the volume plugin supports SELinux mount.
    pub plugin_supports_selinux_context_mount: bool,
}

/// Port of `SupportsSELinuxContextMount` (`pkg/volume/util/selinux.go:176-183`).
///
/// Upstream discards the plugin-lookup error (`plugin, _ :=`) and returns
/// `(false, nil)` when nothing matched; that is ported as the `ok()` below.
pub fn supports_selinux_context_mount(
    volume_spec: &Spec<'_>,
    volume_plugin_mgr: &VolumePluginMgr,
) -> Result<bool, SELinuxLabelError> {
    match volume_plugin_mgr.find_plugin_by_spec(volume_spec).ok() {
        Some(plugin) => plugin
            .supports_selinux_context_mount(volume_spec)
            .map_err(|e| SELinuxLabelError::Other(e.to_string())),
        None => Ok(false),
    }
}

/// Port of `VolumeSupportsSELinuxMount` (`pkg/volume/util/selinux.go:185-208`).
///
/// Answers "can this volume's *access mode* carry `-o context=`", which is a
/// different question from "does the plugin support it". `AddPodToVolume` uses
/// it twice: to decide whether to clear the effective mount label, and to
/// decide whether an SELinux mismatch is an error or only a warning.
pub fn volume_supports_selinux_mount(volume_spec: &Spec<'_>) -> bool {
    if !feature_gates::enabled(Feature::SELinuxMountReadWriteOncePod) {
        return false;
    }
    let Some(pv) = volume_spec.persistent_volume else {
        return false;
    };
    if feature_gates::enabled(Feature::SELinuxMount) {
        return true;
    }

    // Only SELinuxMountReadWriteOncePod feature is enabled
    if pv.spec.access_modes.len() != 1 {
        // RWOP volumes must be the only access mode of the volume
        return false;
    }
    if !contains_access_mode(
        &pv.spec.access_modes,
        &PersistentVolumeAccessMode::ReadWriteOncePod,
    ) {
        // Not a RWOP volume
        return false;
    }
    // RWOP volume
    true
}

/// Port of `GetMountSELinuxLabel` (`pkg/volume/util/selinux.go:246-313`).
///
/// **Go multi-return.** Upstream returns `(SELinuxLabelInfo, error)` and its
/// caller reads `labelInfo.PluginSupportsSELinuxContextMount` *even when the
/// error is non-nil* (`desired_state_of_world.go:422, :433`). A
/// `Result<SELinuxLabelInfo, _>` would drop that, so the tuple shape is kept.
///
/// It does not evaluate the volume access mode; the caller does, because it
/// may need to bump different metrics per feature gate / access mode / label.
pub fn get_mount_selinux_label(
    volume_spec: &Spec<'_>,
    effective_selinux_container_labels: &[Option<SELinuxOptions>],
    pod_security_context: Option<&PodSecurityContext>,
    volume_plugin_mgr: &VolumePluginMgr,
    selinux_translator: &dyn SELinuxLabelTranslator,
) -> (SELinuxLabelInfo, Option<SELinuxLabelError>) {
    let mut info = SELinuxLabelInfo::default();
    if !feature_gates::enabled(Feature::SELinuxMountReadWriteOncePod) {
        return (info, None);
    }

    if !selinux_translator.selinux_enabled() {
        return (info, None);
    }

    let plugin_supports_selinux_context_mount =
        match supports_selinux_context_mount(volume_spec, volume_plugin_mgr) {
            Ok(supported) => supported,
            Err(err) => return (info, Some(err)),
        };

    info.plugin_supports_selinux_context_mount = plugin_supports_selinux_context_mount;

    // Collect all SELinux options from all containers that use this volume.
    // A set will squash any duplicities.
    let mut labels: BTreeSet<String> = BTreeSet::new();
    for container_label in effective_selinux_container_labels {
        let lbl = match selinux_translator.selinux_options_to_file_label(container_label.as_ref()) {
            Ok(lbl) => lbl,
            Err(err) => {
                // Upstream wraps with `failed to construct SELinux label from
                // context %q: %w` (`selinux.go:280`). `%q` of a `*v1.SELinuxOptions`
                // prints the Go struct literal; `{container_label:?}` is the
                // closest faithful rendering available.
                return (
                    info,
                    Some(SELinuxLabelError::Translation(format!(
                        "failed to construct SELinux label from context {container_label:?}: {err}"
                    ))),
                );
            }
        };
        labels.insert(lbl);
    }

    // Ensure that all containers use the same SELinux label.
    if labels.len() > 1 {
        // This volume is used with more than one SELinux label in the pod.
        return (
            info,
            Some(SELinuxLabelError::MultipleLabels(
                labels.into_iter().collect(),
            )),
        );
    }
    let Some(lbl) = labels.into_iter().next() else {
        return (info, None);
    };

    info.selinux_process_label = lbl.clone();
    info.selinux_mount_label = lbl;

    if feature_gates::enabled(Feature::SELinuxChangePolicy)
        && pod_security_context
            .and_then(|sc| sc.se_linux_change_policy.as_deref())
            .is_some_and(|p| p == SELINUX_CHANGE_POLICY_RECURSIVE)
    {
        // The pod has opted into recursive SELinux label changes. Do not mount with -o context.
        info.selinux_mount_label = String::new();
    }

    if !plugin_supports_selinux_context_mount {
        // The volume plugin does not support SELinux mount. Do not mount with -o context.
        info.selinux_mount_label = String::new();
    }

    (info, None)
}

/// `v1.SELinuxChangePolicyRecursive`
/// (`staging/src/k8s.io/api/core/v1/types.go`). `PodSecurityContext
/// .seLinuxChangePolicy` is an `Option<String>` in this project rather than a
/// named type, so the constant is compared as a string.
pub const SELINUX_CHANGE_POLICY_RECURSIVE: &str = "Recursive";
