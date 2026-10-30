//! Unit tests moved from `src/profile.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::pod::NodeAffinity;
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_scheduler::framework_runtime::{
        default_plugins, merge_plugins, new_framework, ExtensionPoint, Framework, Plugins,
        ProfileSpec, Registry,
    };
    pub use rusternetes_scheduler::scheduler_config::VolumeBindingArgs;
    pub use rusternetes_scheduler::volume_binding::{self, FunctionShapePoint};
    pub use std::collections::HashMap;
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::PodSpec;
    use rusternetes_scheduler::profile::*;
    use serde_json::json;

    /// profile_test.go `fakeRegistry`: queue sort plugins `QueueSort` /
    /// `AnotherSort` and the `Bind` plugin, beside the in-tree ones.
    fn fake_registry() -> Registry {
        let mut r = Registry::in_tree();
        r.register("QueueSort", &[ExtensionPoint::QueueSort])
            .unwrap();
        r.register("AnotherSort", &[ExtensionPoint::QueueSort])
            .unwrap();
        r
    }

    fn fake_map(cfgs: Vec<Profile>) -> Result<Map, String> {
        Map::new_map_with_registry(cfgs, &fake_registry())
    }

    fn p(name: &str, qs: &str, args: Option<serde_json::Value>) -> Profile {
        use rusternetes_scheduler::framework_runtime::{Plugin, PluginSet};
        let one = |n: &str| PluginSet {
            enabled: vec![Plugin::new(n)],
            disabled: Vec::new(),
        };
        Profile {
            scheduler_name: name.to_string(),
            plugins: Some(Plugins {
                queue_sort: one(qs),
                bind: one("DefaultBinder"),
                ..Default::default()
            }),
            queue_sort_args: args,
            ..Profile::named(name)
        }
    }

    // profile_test.go TestNewMap "valid".
    #[test]
    fn new_map_valid() {
        let m = fake_map(vec![
            p("profile-1", "QueueSort", None),
            p("profile-2", "QueueSort", None),
        ])
        .unwrap();
        assert_eq!(m.len(), 2);
        assert!(m.handles_scheduler_name("profile-1"));
        assert!(m.handles_scheduler_name("profile-2"));
        assert!(!m.handles_scheduler_name("profile-3"));
    }

    // "different queue sort".
    #[test]
    fn new_map_different_queue_sort() {
        let err = fake_map(vec![
            p("profile-1", "QueueSort", None),
            p("profile-2", "AnotherSort", None),
        ])
        .unwrap_err();
        assert_eq!(
            err,
            "different queue sort plugins for profile \"profile-2\": \"AnotherSort\", first: \"QueueSort\""
        );
    }

    // "different queue sort args".
    #[test]
    fn new_map_different_queue_sort_args() {
        let err = fake_map(vec![
            p("profile-1", "QueueSort", Some(json!({"foo": "bar1"}))),
            p("profile-2", "QueueSort", Some(json!({"foo": "bar2"}))),
        ])
        .unwrap_err();
        assert_eq!(
            err,
            "different queue sort plugin args for profile \"profile-2\""
        );
    }

    // "duplicate scheduler name".
    #[test]
    fn new_map_duplicate_scheduler_name() {
        let err = fake_map(vec![
            p("profile-1", "QueueSort", None),
            p("profile-1", "QueueSort", None),
        ])
        .unwrap_err();
        assert_eq!(err, "duplicate profile with scheduler name \"profile-1\"");
    }

    // "scheduler name is needed".
    #[test]
    fn new_map_scheduler_name_needed() {
        let err = fake_map(vec![p("", "QueueSort", None)]).unwrap_err();
        assert_eq!(err, "scheduler name is needed");
    }

    // "plugins required for profile".
    #[test]
    fn new_map_plugins_required() {
        let mut prof = p("profile-1", "QueueSort", None);
        prof.plugins = None;
        let err = fake_map(vec![prof]).unwrap_err();
        assert_eq!(
            err,
            "plugins required for profile with scheduler name \"profile-1\""
        );
    }

    // profile_test.go TestNewMap "invalid framework configuration".
    #[test]
    fn new_map_invalid_framework_configuration() {
        use rusternetes_scheduler::framework_runtime::{Plugin, PluginSet};
        let mut prof = p("invalid-profile", "QueueSort", None);
        prof.plugins = Some(Plugins {
            queue_sort: PluginSet {
                enabled: vec![Plugin::new("QueueSort")],
                disabled: Vec::new(),
            },
            ..Default::default()
        });
        let err = fake_map(vec![prof]).unwrap_err();
        assert_eq!(
            err,
            "creating profile for scheduler name invalid-profile: at least one bind plugin is needed for profile with scheduler name \"invalid-profile\""
        );
    }

    // Each profile keeps its own resolved plugin set and score weights.
    #[test]
    fn each_profile_keeps_its_own_framework() {
        use rusternetes_scheduler::framework_runtime::{Plugin, PluginSet};
        let mut b = Profile::named("b");
        let mut plugins = b.plugins.take().unwrap();
        plugins.filter.disabled = vec![Plugin::new("TaintToleration")];
        plugins.score = PluginSet {
            enabled: vec![Plugin::weighted("NodeAffinity", 9)],
            disabled: Vec::new(),
        };
        b.plugins = Some(plugins);
        let m = Map::new_map(vec![Profile::named("a"), b]).unwrap();
        let (a, b) = (m.get("a").unwrap(), m.get("b").unwrap());
        assert!(a.framework.has(ExtensionPoint::Filter, "TaintToleration"));
        assert!(!b.framework.has(ExtensionPoint::Filter, "TaintToleration"));
        assert_eq!(a.framework.score_weight("NodeAffinity"), 2);
        assert_eq!(b.framework.score_weight("NodeAffinity"), 9);
    }

    fn pod_named(sn: Option<&str>) -> Pod {
        Pod::new(
            "p",
            PodSpec {
                scheduler_name: sn.map(str::to_string),
                ..Default::default()
            },
        )
    }

    // frameworkForPod routing and error text.
    #[test]
    fn framework_for_pod_routes_by_scheduler_name() {
        let m = fake_map(vec![
            p("profile-1", "PrioritySort", None),
            p("profile-2", "PrioritySort", None),
        ])
        .unwrap();
        assert_eq!(
            m.framework_for_pod(&pod_named(Some("profile-2")))
                .unwrap()
                .scheduler_name,
            "profile-2"
        );
        assert_eq!(
            m.framework_for_pod(&pod_named(Some("nope"))).unwrap_err(),
            "profile not found for scheduler name \"nope\""
        );
        // Unset name is the API server's default-scheduler default; no such
        // profile here.
        assert_eq!(
            m.framework_for_pod(&pod_named(None)).unwrap_err(),
            "profile not found for scheduler name \"default-scheduler\""
        );
    }
}
