//! Unit tests moved from `src/controllers/selinux_warning/translator.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::pod::SELinuxOptions;
}

mod tests {
    //! `selinux_translator_test.go` `TestSELinuxOptionsToFileLabel`,
    //! `TestLabelsConflict`; `internal/parse/selinux_label_test.go`
    //! `TestParseSELinuxLabel`.
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::selinux_warning::translator::*;

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
