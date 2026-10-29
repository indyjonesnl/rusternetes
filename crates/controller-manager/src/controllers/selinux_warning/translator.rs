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

#[cfg(test)]
mod tests {
    //! `selinux_translator_test.go` `TestSELinuxOptionsToFileLabel`,
    //! `TestLabelsConflict`; `internal/parse/selinux_label_test.go`
    //! `TestParseSELinuxLabel`.
    use super::*;

    fn opts(user: &str, role: &str, ty: &str, level: &str) -> SELinuxOptions {
        let s = |v: &str| (!v.is_empty()).then(|| v.to_string());
        SELinuxOptions {
            user: s(user),
            role: s(role),
            type_: s(ty),
            level: s(level),
        }
    }

    #[test]
    fn selinux_options_to_file_label() {
        let cases: Vec<(&str, Option<SELinuxOptions>, &str)> = vec![
            ("nil options", None, ""),
            (
                "all fields set",
                Some(opts("system_u", "system_r", "container_t", "s0:c0,c1")),
                "system_u:system_r:container_t:s0:c0,c1",
            ),
            (
                "some fields set",
                Some(opts("", "", "container_t", "s0:c0,c1")),
                "::container_t:s0:c0,c1",
            ),
            (
                "only level set",
                Some(opts("", "", "", "s0:c0,c1")),
                ":::s0:c0,c1",
            ),
            ("no fields set", Some(opts("", "", "", "")), ""),
        ];
        for (name, o, expected) in cases {
            let got = ControllerSELinuxTranslator.selinux_options_to_file_label(o.as_ref());
            assert_eq!(got, expected, "{name}");
        }
    }

    #[test]
    fn labels_conflict() {
        let cases = [
            ("empty strings don't conflict", "", "", false),
            (
                "empty strings don't conflict with anything except the level",
                "",
                "system_u:system_r:container_t",
                false,
            ),
            (
                "empty string conflicts with level",
                "",
                "system_u:system_r:container_t:s0:c1,c2",
                true,
            ),
            (
                "empty parts don't conflict with anything",
                ":::",
                "system_u:system_r:container_t",
                false,
            ),
            (
                "different lengths don't conflict if the common parts are the same",
                "system_u:system_r:container_t:",
                "system_u:system_r",
                false,
            ),
            (
                "different lengths conflict if the common parts differ",
                "system_u:system_r:conflict_t:",
                "system_u:system_r:container_t",
                true,
            ),
            (
                "empty parts with conflict",
                "::conflict_t",
                "::container_t",
                true,
            ),
            (
                "non-conflicting empty parts",
                "system_u::container_t",
                ":system_r::",
                false,
            ),
            (
                "empty level conflicts with non-empty level",
                ":::s0:c1,c2",
                "",
                true,
            ),
        ];
        for (name, a, b, conflict) in cases {
            assert_eq!(
                ControllerSELinuxTranslator.conflicts(a, b),
                conflict,
                "{name}: Conflicts({a:?}, {b:?})"
            );
        }
    }

    #[test]
    fn parse_selinux_label_cases() {
        let cases: [(&str, &str, [&str; 4]); 12] = [
            ("empty label", "", ["", "", "", ""]),
            (
                "complete label with all components",
                "system_u:system_r:container_t:s0:c0,c1",
                ["system_u", "system_r", "container_t", "s0:c0,c1"],
            ),
            (
                "label with user, role, and type only",
                "system_u:system_r:container_t",
                ["system_u", "system_r", "container_t", ""],
            ),
            (
                "label with user and role only",
                "system_u:system_r",
                ["system_u", "system_r", "", ""],
            ),
            ("label with user only", "system_u", ["system_u", "", "", ""]),
            (
                "label missing user but with role and type",
                ":system_r:container_t",
                ["", "system_r", "container_t", ""],
            ),
            (
                "label missing user and role but with type",
                "::container_t",
                ["", "", "container_t", ""],
            ),
            (
                "label missing user and role but with type and level",
                "::container_t:s0",
                ["", "", "container_t", "s0"],
            ),
            (
                "label with all empty components except level",
                ":::s0:c0,c1",
                ["", "", "", "s0:c0,c1"],
            ),
            (
                "label with special characters in components",
                "user_with_underscore:role-with-dash:type.with.dots:s0:c0.c1",
                [
                    "user_with_underscore",
                    "role-with-dash",
                    "type.with.dots",
                    "s0:c0.c1",
                ],
            ),
            (
                "label with extra colons in level component",
                "user:role:type:s0:c0,c1:extra",
                ["user", "role", "type", "s0:c0,c1:extra"],
            ),
            ("multiple colons only", ":::", ["", "", "", ""]),
        ];
        for (name, label, expected) in cases {
            let got = parse_selinux_label(label);
            let want: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
            assert_eq!(got.to_vec(), want, "{name}: {label:?}");
        }
        // "five colons"
        assert_eq!(
            parse_selinux_label(":::::").to_vec(),
            vec!["", "", "", "::"]
        );
    }
}
