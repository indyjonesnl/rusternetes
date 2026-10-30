//! Unit tests moved from `src/framework_runtime.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::feature_gates::{self, Feature};
    pub use serde::Deserialize;
    pub use std::collections::{HashMap, HashSet};
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::framework_runtime::*;

    const TEST_PLUGIN: &str = "test-plugin";
    const QUEUE_SORT: &str = "no-op-queue-sort-plugin";
    const BIND: &str = "no-op-bind-plugin";
    const SCORE1: &str = "score-plugin-1";
    const SCORE2: &str = "score-plugin-2";
    const SCORE_WITH_NORMALIZE1: &str = "score-with-normalize-plugin-1";
    const NOT_SCORE: &str = "plugin-not-implementing-score";

    /// framework_test.go `registry` fixture: TestPlugin implements every
    /// extension point (:99-110 + the no-op queue sort/bind plugins).
    fn registry() -> Registry {
        use ExtensionPoint::*;
        let mut r = Registry::default();
        r.register(
            TEST_PLUGIN,
            &[
                QueueSort, PreFilter, Filter, PostFilter, PreScore, Score, Reserve, Permit,
                PreBind, Bind, PostBind,
            ],
        )
        .unwrap();
        r.register(QUEUE_SORT, &[QueueSort]).unwrap();
        r.register(BIND, &[Bind]).unwrap();
        r.register(SCORE1, &[PreScore, Score]).unwrap();
        r.register(SCORE2, &[PreScore, Score]).unwrap();
        r.register(SCORE_WITH_NORMALIZE1, &[Score]).unwrap();
        r.register(NOT_SCORE, &[Filter]).unwrap();
        r
    }

    fn set(names: &[(&str, i32)]) -> PluginSet {
        PluginSet {
            enabled: names
                .iter()
                .map(|(n, w)| {
                    if *w == 0 {
                        Plugin::new(n)
                    } else {
                        Plugin::weighted(n, *w)
                    }
                })
                .collect(),
            disabled: Vec::new(),
        }
    }

    fn names_of(f: &Framework, ep: ExtensionPoint) -> Vec<&str> {
        f.plugins_at(ep).iter().map(String::as_str).collect()
    }

    fn build(p: &Plugins) -> Result<Framework, String> {
        new_framework(
            &registry(),
            &ProfileSpec {
                scheduler_name: "p",
                plugins: Some(p),
                plugin_config_names: &[],
                ..Default::default()
            },
        )
    }

    /// `newFrameworkWithQueueSortAndBind` (framework_test.go): the queue sort
    /// and bind plugins are filled in when the config lacks them.
    fn build_with_qs_bind(mut p: Plugins) -> Result<Framework, String> {
        if p.queue_sort.enabled.is_empty() {
            p.queue_sort = set(&[(QUEUE_SORT, 0)]);
        }
        if p.bind.enabled.is_empty() {
            p.bind = set(&[(BIND, 0)]);
        }
        build(&p)
    }

    // TestInitFrameworkWithScorePlugins
    #[test]
    fn init_framework_with_score_plugins() {
        let score = |names: &[&str]| Plugins {
            score: set(&names.iter().map(|n| (*n, 1)).collect::<Vec<_>>()),
            ..Default::default()
        };
        assert!(build_with_qs_bind(score(&["notExist"])).is_err());
        assert!(build_with_qs_bind(score(&[NOT_SCORE])).is_err());
        assert!(build_with_qs_bind(Plugins::default()).is_ok());
        assert!(build_with_qs_bind(score(&[])).is_ok());
        assert!(build_with_qs_bind(score(&[SCORE1])).is_ok());
        assert!(build_with_qs_bind(score(&[SCORE_WITH_NORMALIZE1])).is_ok());
        let err = build_with_qs_bind(score(&["notExist"])).unwrap_err();
        assert_eq!(err, "ScorePlugin \"notExist\" does not exist");
        let err = build_with_qs_bind(score(&[NOT_SCORE])).unwrap_err();
        assert_eq!(
            err,
            "plugin \"plugin-not-implementing-score\" does not extend ScorePlugin plugin"
        );
    }

    // TestNewFrameworkErrors
    #[test]
    fn new_framework_errors() {
        let p = Plugins {
            pre_filter: set(&[(TEST_PLUGIN, 1), (TEST_PLUGIN, 1)]),
            ..Default::default()
        };
        let err = build_with_qs_bind(p).unwrap_err();
        assert!(err.contains("already registered"), "{err}");
        let p = Plugins {
            pre_filter: set(&[(TEST_PLUGIN, 1)]),
            ..Default::default()
        };
        let err = new_framework(
            &registry(),
            &ProfileSpec {
                scheduler_name: "p",
                plugins: Some(&p),
                plugin_config_names: &[TEST_PLUGIN.to_string(), TEST_PLUGIN.to_string()],
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.contains("repeated config for plugin"), "{err}");
    }

    fn mp(names: &[(&str, i32)]) -> Plugins {
        Plugins {
            multi_point: set(names),
            ..Default::default()
        }
    }

    fn expect(f: &Framework, ep: ExtensionPoint, want: &[&str]) {
        assert_eq!(names_of(f, ep), want, "{ep:?}");
    }

    // TestNewFrameworkMultiPointExpansion "plugin expansion"
    #[test]
    fn multi_point_plugin_expansion() {
        use ExtensionPoint::*;
        let f = build(&mp(&[(TEST_PLUGIN, 5)])).unwrap();
        for ep in [
            QueueSort, PreFilter, Filter, PostFilter, PreScore, Score, Reserve, Permit, PreBind,
            Bind, PostBind,
        ] {
            expect(&f, ep, &[TEST_PLUGIN]);
        }
        assert_eq!(f.score_weight(TEST_PLUGIN), 5);
        let listed = f.list_plugins();
        assert_eq!(listed.score.enabled, vec![Plugin::weighted(TEST_PLUGIN, 5)]);
        assert_eq!(listed.filter.enabled, vec![Plugin::new(TEST_PLUGIN)]);
    }

    // "disable MultiPoint plugin at some extension points"
    #[test]
    fn multi_point_disabled_at_some_extension_points() {
        use ExtensionPoint::*;
        let mut p = mp(&[(TEST_PLUGIN, 0)]);
        p.pre_score.disabled = vec![Plugin::new(TEST_PLUGIN)];
        p.score.disabled = vec![Plugin::new(TEST_PLUGIN)];
        let f = build(&p).unwrap();
        expect(&f, PreScore, &[]);
        expect(&f, Score, &[]);
        expect(&f, Filter, &[TEST_PLUGIN]);
        expect(&f, Bind, &[TEST_PLUGIN]);
    }

    // "Multiple MultiPoint plugins"
    #[test]
    fn multiple_multi_point_plugins() {
        use ExtensionPoint::*;
        let f = build(&mp(&[(TEST_PLUGIN, 0), (SCORE1, 0)])).unwrap();
        expect(&f, PreScore, &[TEST_PLUGIN, SCORE1]);
        expect(&f, Score, &[TEST_PLUGIN, SCORE1]);
        assert_eq!(f.score_weight(TEST_PLUGIN), 1);
        assert_eq!(f.score_weight(SCORE1), 1);
        expect(&f, Filter, &[TEST_PLUGIN]);
    }

    // "disable MultiPoint extension"
    #[test]
    fn disable_multi_point_extension() {
        use ExtensionPoint::*;
        let mut p = mp(&[(TEST_PLUGIN, 0), (SCORE1, 0)]);
        p.pre_score.disabled = vec![Plugin::new("*")];
        let f = build(&p).unwrap();
        expect(&f, PreScore, &[]);
        expect(&f, Score, &[TEST_PLUGIN, SCORE1]);
    }

    // "Reorder MultiPoint plugins (specified extension takes precedence)"
    #[test]
    fn reorder_specified_extension_takes_precedence() {
        use ExtensionPoint::*;
        let mut p = mp(&[(SCORE_WITH_NORMALIZE1, 0), (TEST_PLUGIN, 0), (SCORE1, 0)]);
        p.score = set(&[(SCORE1, 0), (TEST_PLUGIN, 0)]);
        let f = build(&p).unwrap();
        expect(&f, PreScore, &[TEST_PLUGIN, SCORE1]);
        expect(&f, Score, &[SCORE1, TEST_PLUGIN, SCORE_WITH_NORMALIZE1]);
    }

    // "Reorder MultiPoint plugins (specified extension only takes precedence
    // when it exists in MultiPoint)"
    #[test]
    fn reorder_only_when_in_multi_point() {
        use ExtensionPoint::*;
        let mut p = mp(&[(TEST_PLUGIN, 0), (SCORE1, 0)]);
        p.score = set(&[(SCORE_WITH_NORMALIZE1, 0), (SCORE1, 0), (TEST_PLUGIN, 0)]);
        let f = build(&p).unwrap();
        expect(&f, Score, &[SCORE1, TEST_PLUGIN, SCORE_WITH_NORMALIZE1]);
    }

    // "Override MultiPoint plugins weights"
    #[test]
    fn override_multi_point_weights() {
        use ExtensionPoint::*;
        let mut p = mp(&[(TEST_PLUGIN, 0), (SCORE1, 0)]);
        p.score = set(&[(SCORE1, 5), (TEST_PLUGIN, 3)]);
        let f = build(&p).unwrap();
        expect(&f, Score, &[SCORE1, TEST_PLUGIN]);
        assert_eq!(f.score_weight(SCORE1), 5);
        assert_eq!(f.score_weight(TEST_PLUGIN), 3);
    }

    // "disable and enable MultiPoint plugins with '*'" (the merged shape: the
    // disabled list is carried, the multipoint list holds only the enabled
    // plugins).
    #[test]
    fn multi_point_enable_with_star_disabled() {
        use ExtensionPoint::*;
        let mut p = mp(&[(QUEUE_SORT, 0), (BIND, 0), (SCORE1, 0)]);
        p.multi_point.disabled = vec![Plugin::new("*")];
        let f = build(&p).unwrap();
        expect(&f, QueueSort, &[QUEUE_SORT]);
        expect(&f, PreScore, &[SCORE1]);
        expect(&f, Score, &[SCORE1]);
        expect(&f, Bind, &[BIND]);
        expect(&f, Filter, &[]);
    }

    // "Expect 'already registered' error"
    #[test]
    fn multi_point_double_registration_errors() {
        let err = build(&mp(&[(TEST_PLUGIN, 0), (TEST_PLUGIN, 0)])).unwrap_err();
        assert!(err.contains("already registered"), "{err}");
    }

    // "Override MultiPoint plugins weights and avoid a plugin being loaded
    // multiple times"
    #[test]
    fn override_weights_avoid_double_load() {
        use ExtensionPoint::*;
        let mut p = mp(&[(TEST_PLUGIN, 0), (SCORE1, 0)]);
        p.score = set(&[(SCORE1, 5), (SCORE2, 5), (TEST_PLUGIN, 3)]);
        let f = build(&p).unwrap();
        expect(&f, PreScore, &[TEST_PLUGIN, SCORE1]);
        expect(&f, Score, &[SCORE1, TEST_PLUGIN, SCORE2]);
        assert_eq!(f.score_weight(SCORE2), 5);
    }

    // profile_test.go TestNewMap "invalid framework configuration" shape.
    #[test]
    fn missing_bind_and_queue_sort_errors() {
        let p = Plugins {
            queue_sort: set(&[(QUEUE_SORT, 0)]),
            ..Default::default()
        };
        let err = build(&p).unwrap_err();
        assert_eq!(
            err,
            "at least one bind plugin is needed for profile with scheduler name \"p\""
        );
        let err = build(&Plugins::default()).unwrap_err();
        assert_eq!(
            err,
            "only one queue sort plugin required for profile with scheduler name \"p\", but got 0"
        );
    }

    #[test]
    fn nil_plugins_builds_an_empty_framework() {
        let f = new_framework(
            &registry(),
            &ProfileSpec {
                scheduler_name: "p",
                plugins: None,
                plugin_config_names: &[],
                ..Default::default()
            },
        )
        .unwrap();
        assert!(!f.has_filter_plugins());
        assert_eq!(f.profile_name, "p");
    }

    // default_plugins_test.go TestApplyFeatureGates / TestMergePlugins shapes.
    #[test]
    fn default_plugins_resolve_through_the_in_tree_registry() {
        use ExtensionPoint::*;
        let d = merge_plugins(default_plugins(), None);
        let f = new_framework(
            &Registry::in_tree(),
            &ProfileSpec {
                scheduler_name: "default-scheduler",
                plugins: Some(&d),
                plugin_config_names: &[],
                ..Default::default()
            },
        )
        .unwrap();
        expect(&f, QueueSort, &["PrioritySort"]);
        expect(&f, Bind, &["DefaultBinder"]);
        assert_eq!(f.score_weight("TaintToleration"), 3);
        assert_eq!(f.score_weight("NodeAffinity"), 2);
        assert_eq!(f.score_weight("VolumeBinding"), 1);
        assert!(f.has(Filter, "NodeUnschedulable"));
        assert!(f.has(PostFilter, "DefaultPreemption"));
        assert!(f.has(Reserve, "VolumeBinding"));
    }

    #[test]
    fn merge_replaces_default_in_place_and_honours_disabled() {
        let custom = Plugins {
            multi_point: PluginSet {
                enabled: vec![Plugin::weighted(names::NODE_AFFINITY, 7)],
                disabled: vec![Plugin::new(names::NODE_PORTS)],
            },
            ..Default::default()
        };
        let merged = merge_plugins(default_plugins(), Some(&custom));
        let enabled: Vec<&str> = merged
            .multi_point
            .enabled
            .iter()
            .map(|p| p.name.as_str())
            .collect();
        assert!(!enabled.contains(&"NodePorts"));
        let pos = enabled.iter().position(|n| *n == "NodeAffinity").unwrap();
        assert_eq!(merged.multi_point.enabled[pos].weight, Some(7));
        // Order preserved: NodeAffinity stays right after TaintToleration.
        assert_eq!(enabled[pos - 1], "TaintToleration");
        assert_eq!(merged.multi_point.disabled, vec![Plugin::new("NodePorts")]);
        // "*" drops every default.
        let star = Plugins {
            multi_point: PluginSet {
                enabled: vec![Plugin::new("X")],
                disabled: vec![Plugin::new("*")],
            },
            ..Default::default()
        };
        let merged = merge_plugins(default_plugins(), Some(&star));
        assert_eq!(merged.multi_point.enabled, vec![Plugin::new("X")]);
    }

    // registry_test.go TestMerge / TestRegister / TestUnregister.
    fn reg(entries: &[(&str, &[ExtensionPoint])]) -> Registry {
        let mut r = Registry::default();
        for (n, p) in entries {
            r.register(n, p).unwrap();
        }
        r
    }

    fn same(a: &Registry, b: &Registry) -> bool {
        a.plugins == b.plugins
    }

    #[test]
    fn registry_merge() {
        use ExtensionPoint::*;
        let mut p = reg(&[("pluginFactory1", &[Filter])]);
        p.merge(&reg(&[("pluginFactory2", &[Score])])).unwrap();
        assert!(same(
            &p,
            &reg(&[("pluginFactory1", &[Filter]), ("pluginFactory2", &[Score])])
        ));

        let mut p = reg(&[("pluginFactory1", &[Filter])]);
        let err = p.merge(&reg(&[("pluginFactory1", &[Score])])).unwrap_err();
        assert_eq!(err, "a plugin named pluginFactory1 already exists");
        assert!(same(&p, &reg(&[("pluginFactory1", &[Filter])])));
    }

    #[test]
    fn registry_register() {
        use ExtensionPoint::*;
        let mut r = Registry::default();
        r.register("pluginFactory1", &[Filter]).unwrap();
        assert!(same(&r, &reg(&[("pluginFactory1", &[Filter])])));
        let err = r.register("pluginFactory1", &[Score]).unwrap_err();
        assert_eq!(err, "a plugin named pluginFactory1 already exists");
        assert!(same(&r, &reg(&[("pluginFactory1", &[Filter])])));
    }

    #[test]
    fn registry_unregister() {
        use ExtensionPoint::*;
        let mut r = reg(&[("pluginFactory1", &[Filter]), ("pluginFactory2", &[Score])]);
        r.unregister("pluginFactory1").unwrap();
        assert!(same(&r, &reg(&[("pluginFactory2", &[Score])])));
        let mut empty = Registry::default();
        let err = empty.unregister("pluginFactory1").unwrap_err();
        assert_eq!(err, "no plugin named pluginFactory1 exists");
        assert!(same(&empty, &Registry::default()));
    }
}
