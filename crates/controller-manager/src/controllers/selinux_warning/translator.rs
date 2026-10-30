//! Port of `pkg/controller/volume/selinuxwarning/translator/selinux_translator.go`
//! and `.../internal/parse/selinux_label.go` (release-1.35).

use rusternetes_common::resources::pod::SELinuxOptions;

/// `parse.ParseSELinuxLabel` (`internal/parse/selinux_label.go`): split a
/// label into `[user, role, type, level]` with `strings.SplitN(label, ":", 4)`.
/// Missing trailing parts are `""`; the level keeps any further colons.
pub fn parse_selinux_label(label: &str) -> [String; 4] {
    let mut parts: [String; 4] = Default::default();
    if label.is_empty() {
        return parts;
    }
    for (slot, part) in parts.iter_mut().zip(label.splitn(4, ':')) {
        *slot = part.to_string();
    }
    parts
}

/// `ControllerSELinuxTranslator`: the `SELinuxLabelTranslator` usable in
/// kube-controller-manager. A real translator fills empty parts of
/// `SELinuxOptions` from `/etc/selinux/*`; the controller-manager often runs
/// in a container and cannot read the node's defaults (and may run on a
/// different distro than the nodes), so only the fields present in the
/// provided options are used.
#[derive(Debug, Clone, Copy, Default)]
pub struct ControllerSELinuxTranslator;

impl ControllerSELinuxTranslator {
    /// `SELinuxEnabled`: the controller must have been explicitly enabled, so
    /// all nodes are expected to have SELinux enabled.
    pub fn selinux_enabled(&self) -> bool {
        true
    }

    /// `SELinuxOptionsToFileLabel`: concatenate the fields, do not default
    /// the missing ones. Empty options behave like nil.
    pub fn selinux_options_to_file_label(&self, opts: Option<&SELinuxOptions>) -> String {
        let Some(opts) = opts else {
            return String::new();
        };
        let label = [&opts.user, &opts.role, &opts.type_, &opts.level]
            .iter()
            .map(|p| p.as_deref().unwrap_or(""))
            .collect::<Vec<_>>()
            .join(":");
        if label == ":::" {
            return String::new();
        }
        label
    }

    /// `Conflicts`: true if two labels produced by
    /// [`Self::selinux_options_to_file_label`] conflict. The first three
    /// components do not conflict when either side is empty (the node may
    /// default them); the level is always compared.
    #[cfg_attr(not(test), allow(dead_code))] // the cache calls `conflicts_parsed`; only tests call this
    pub fn conflicts(&self, label_a: &str, label_b: &str) -> bool {
        self.conflicts_parsed(&parse_selinux_label(label_a), &parse_selinux_label(label_b))
    }

    /// `ConflictsParsed`: [`Self::conflicts`] on pre-split labels.
    pub fn conflicts_parsed(&self, parts_a: &[String; 4], parts_b: &[String; 4]) -> bool {
        for i in 0..4 {
            if parts_a[i] == parts_b[i] {
                continue;
            }
            if i == 3 {
                // The last component must always match.
                return true;
            }
            // i < 3, empty parts are incomparable.
            if parts_a[i].is_empty() || parts_b[i].is_empty() {
                continue;
            }
            // Parts are not equal and neither of them is "" -> conflict.
            return true;
        }
        false
    }
}
