//! Unit tests moved from `src/volume_plugins/util/selinux.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::feature_gates::{self, Feature};
    pub use rusternetes_common::resources::pod::{PodSecurityContext, SELinuxOptions};
    pub use rusternetes_common::resources::volume::PersistentVolumeAccessMode;
    pub use rusternetes_kubelet::go_selinux_label::Labeler;
    pub use rusternetes_kubelet::volume_plugins::plugin::Spec;
    pub use rusternetes_kubelet::volume_plugins::registry::VolumePluginMgr;
    pub use rusternetes_kubelet::volume_plugins::util::contains_access_mode;
    pub use std::collections::BTreeSet;
}

mod real_translator_tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::selinux::*;

    #[test]
    fn real_translator_reports_the_platform_selinux_state() {
        // Upstream `translator.SELinuxEnabled` is `selinux.GetEnabled()`
        // (selinux.go:96-98), not a constant.
        assert_eq!(
            Translator.selinux_enabled(),
            rusternetes_kubelet::go_selinux::get_enabled()
        );
    }
}

mod translator_tests {
    use super::__parent::*;
    use rusternetes_kubelet::go_selinux_label::LxcContexts;
    use rusternetes_kubelet::volume_plugins::util::selinux::*;

    fn labeler() -> Labeler {
        Labeler::new(LxcContexts::parse(
            "process = \"system_u:system_r:container_t:s0\"\n\
             file = \"system_u:object_r:container_file_t:s0\"\n",
        ))
    }

    fn so(user: &str, role: &str, ty: &str, level: &str) -> SELinuxOptions {
        let f = |s: &str| (!s.is_empty()).then(|| s.to_string());
        SELinuxOptions {
            user: f(user),
            role: f(role),
            type_: f(ty),
            level: f(level),
        }
    }

    #[test]
    fn level_only_fills_rest_from_system_defaults() {
        // Ported shape of fakeTranslator's contract: empty fields come from
        // policy defaults, process type container_t becomes file type
        // container_file_t.
        let got =
            selinux_options_to_file_label_with(&labeler(), true, Some(&so("", "", "", "s0:c1,c2")))
                .unwrap();
        assert_eq!(got, "system_u:object_r:container_file_t:s0:c1,c2");
    }

    #[test]
    fn user_and_type_options_apply_type_to_process_only() {
        let got = selinux_options_to_file_label_with(
            &labeler(),
            true,
            Some(&so("u_u", "r_r", "spc_t", "s0:c3,c4")),
        )
        .unwrap();
        // user + level carry to the file label; role and type do not.
        assert_eq!(got, "u_u:object_r:container_file_t:s0:c3,c4");
    }

    #[test]
    fn nil_or_empty_options_and_disabled_selinux_give_empty() {
        let l = labeler();
        assert_eq!(
            selinux_options_to_file_label_with(&l, true, None).unwrap(),
            ""
        );
        assert_eq!(
            selinux_options_to_file_label_with(&l, true, Some(&so("", "", "", ""))).unwrap(),
            ""
        );
        assert_eq!(
            selinux_options_to_file_label_with(&l, false, Some(&so("", "", "", "s0:c1,c2")))
                .unwrap(),
            ""
        );
    }

    #[test]
    fn does_not_leak_the_process_mcs_reservation() {
        let l = labeler();
        selinux_options_to_file_label_with(&l, true, Some(&so("", "", "", "s0:c1,c2"))).unwrap();
        assert!(!l.is_reserved("s0:c1,c2"));
    }
}

mod add_selinux_mount_option_tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::selinux::*;

    /// `selinux.go:233-235`: the comma-bearing example from upstream's comment
    /// is quoted as one option; the gate off leaves the options alone.
    #[test]
    #[serial_test::serial]
    fn quotes_the_label_when_the_gate_is_on_and_skips_it_when_off() {
        let ctx = "system_u:object_r:container_file_t:s0:c15,c25";
        let on = feature_gates::with_feature(Feature::SELinuxMountReadWriteOncePod, true);
        assert_eq!(
            add_selinux_mount_option(vec!["dirsync".into(), "noatime".into()], ctx),
            vec![
                "dirsync".to_string(),
                "noatime".into(),
                format!("context=\"{ctx}\"")
            ]
        );
        drop(on);
        let _off = feature_gates::with_feature(Feature::SELinuxMountReadWriteOncePod, false);
        assert_eq!(
            add_selinux_mount_option(vec!["a".into()], ctx),
            vec!["a".to_string()]
        );
    }
}
