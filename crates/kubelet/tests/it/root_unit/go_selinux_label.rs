//! Unit tests moved from `src/go_selinux_label.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::{BTreeMap, HashSet};
    pub use std::sync::{Mutex, OnceLock};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::go_selinux_label::*;

    const LXC: &str = "process = \"system_u:system_r:container_t:s0\"\n\
        content = \"system_u:object_r:virt_var_lib_t:s0\"\n\
        file = \"system_u:object_r:container_file_t:s0\"\n\
        ro_file=\"system_u:object_r:container_ro_file_t:s0\"\n\
        # comment\n\n; other\n";

    fn labeler() -> Labeler {
        Labeler::new(LxcContexts::parse(LXC))
    }

    fn seq(v: Vec<u32>) -> impl FnMut() -> u32 {
        let mut i = 0;
        move || {
            let x = v[i % v.len()];
            i += 1;
            x
        }
    }

    fn opts(o: &[&str]) -> Vec<String> {
        o.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn context_roundtrip_and_invalid() {
        let c = Context::new("system_u:object_r:container_file_t:s0:c1,c2").unwrap();
        assert_eq!(c.field("level"), "s0:c1,c2");
        assert_eq!(c.get(), "system_u:object_r:container_file_t:s0:c1,c2");
        assert_eq!(Context::new("a:b").unwrap_err(), ERR_INVALID_LABEL);
        assert_eq!(Context::new("a:b:c").unwrap().get(), "a:b:c");
    }

    #[test]
    fn lxc_contexts_parse_trims_and_unquotes() {
        let l = LxcContexts::parse(LXC);
        assert_eq!(l.label("file"), "system_u:object_r:container_file_t:s0");
        assert_eq!(
            l.label("ro_file"),
            "system_u:object_r:container_ro_file_t:s0"
        );
        assert_eq!(l.label("nope"), "");
    }

    #[test]
    fn read_config_picks_target() {
        let c = "# x\nSELINUX=enforcing\nSELINUXTYPE=\"targeted\"\n";
        assert_eq!(read_config(c, "SELINUXTYPE"), "targeted");
        assert_eq!(read_config(c, "SELINUX"), "enforcing");
        assert_eq!(read_config(c, "X"), "");
    }

    #[test]
    fn priv_mount_label_is_reserved_max_categories() {
        let l = labeler();
        assert_eq!(
            l.priv_container_mount_label,
            "system_u:object_r:container_file_t:s0:c1022,c1023"
        );
        assert!(l.is_reserved("s0:c1022,c1023"));
    }

    #[test]
    fn disabled_returns_empty() {
        let l = labeler();
        assert_eq!(
            l.init_labels_with(false, &opts(&["user:u"]), || 1).unwrap(),
            (String::new(), String::new())
        );
    }

    #[test]
    fn allocates_unique_mcs_and_skips_collisions() {
        let l = labeler();
        // c1==c2 retried; (5,3) swapped to c3,c5; second call collides then moves on.
        let (p, m) = l
            .init_labels_with(true, &[], seq(vec![4, 4, 5, 3]))
            .unwrap();
        assert_eq!(p, "system_u:system_r:container_t:s0:c3,c5");
        assert_eq!(m, "system_u:object_r:container_file_t:s0:c3,c5");
        assert!(l.is_reserved("s0:c3,c5"));
        let (p2, _) = l
            .init_labels_with(true, &[], seq(vec![3, 5, 7, 8]))
            .unwrap();
        assert_eq!(p2, "system_u:system_r:container_t:s0:c7,c8");
    }

    #[test]
    fn level_option_overrides_both_and_reserves_process_label() {
        let l = labeler();
        let (p, m) = l
            .init_labels_with(true, &opts(&["level:s0:c10,c20"]), seq(vec![1, 2]))
            .unwrap();
        assert_eq!(p, "system_u:system_r:container_t:s0:c10,c20");
        assert_eq!(m, "system_u:object_r:container_file_t:s0:c10,c20");
        // the random level was released, the explicit one reserved
        assert!(!l.is_reserved("s0:c1,c2"));
        assert!(l.is_reserved("s0:c10,c20"));
    }

    #[test]
    fn type_applies_to_process_only_and_filetype_to_mount_only() {
        let l = labeler();
        let (p, m) = l
            .init_labels_with(
                true,
                &opts(&["type:spc_t", "filetype:my_file_t", "user:u_u", "role:r_r"]),
                seq(vec![1, 2]),
            )
            .unwrap();
        assert_eq!(p, "u_u:r_r:spc_t:s0:c1,c2");
        // user is copied to the mount label, role is not.
        assert_eq!(m, "u_u:object_r:my_file_t:s0:c1,c2");
    }

    #[test]
    fn disable_returns_priv_mount_label_and_releases() {
        let l = labeler();
        let (p, m) = l
            .init_labels_with(true, &opts(&["disable"]), seq(vec![1, 2]))
            .unwrap();
        assert_eq!(p, "");
        assert_eq!(m, "system_u:object_r:container_file_t:s0:c1022,c1023");
        assert!(!l.is_reserved("s0:c1,c2"));
    }

    #[test]
    fn bad_option_errors_and_releases_mount_label() {
        let l = labeler();
        let e = l
            .init_labels_with(true, &opts(&["bogus:x"]), seq(vec![1, 2]))
            .unwrap_err();
        assert_eq!(
            e,
            "bad label option \"bogus\", valid options 'disable, user, role, level, type, filetype'"
        );
        assert!(!l.is_reserved("s0:c1,c2"));
        let e = l
            .init_labels_with(true, &opts(&["nocolon"]), seq(vec![1, 2]))
            .unwrap_err();
        assert!(e.starts_with("bad label option \"nocolon\""));
    }

    #[test]
    fn missing_policy_yields_empty_process_label() {
        let l = Labeler::new(LxcContexts::default());
        assert_eq!(
            l.init_labels_with(true, &opts(&["user:u"]), || 1).unwrap(),
            (String::new(), String::new())
        );
    }
}
