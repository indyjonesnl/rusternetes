//! Scheduling profiles, indexed by scheduler name.
//!
//! Port of `pkg/scheduler/profile/profile.go` (release-1.35): `Map`, `NewMap`,
//! `HandlesSchedulerName`, `cfgValidator.validate`, and of
//! `Scheduler.frameworkForPod` (`pkg/scheduler/schedule_one.go:390-396`).
//!
//! Rusternetes has no per-profile plugin objects: the filter/score pipeline is
//! compiled in. A [`Profile`] therefore carries the per-profile state the
//! runtime actually consumes (`VolumeBindingArgs`, the event recorder's
//! reporting controller = `schedulerName`), the resolved [`Framework`]
//! (`frameworkruntime.NewFramework`: which plugins run at each extension
//! point, with what score weight) and the queue-sort identity that
//! `cfgValidator` compares across profiles.

use crate::framework_runtime::{
    default_plugins, merge_plugins, new_framework, ExtensionPoint, Framework, Plugins, ProfileSpec,
    Registry,
};
use crate::scheduler_config::VolumeBindingArgs;
use crate::volume_binding::{self, FunctionShapePoint};
use rusternetes_common::resources::pod::NodeAffinity;
use rusternetes_common::resources::Pod;
use std::collections::HashMap;
use std::time::Duration;

/// `v1.DefaultSchedulerName`.
pub const DEFAULT_SCHEDULER_NAME: &str = "default-scheduler";
/// The default `QueueSort` plugin (`names.PrioritySort`).
pub const DEFAULT_QUEUE_SORT: &str = "PrioritySort";

/// One entry of `config.KubeSchedulerProfile`, resolved (defaults applied).
#[derive(Debug, Clone)]
pub struct Profile {
    /// `SchedulerName` == `framework.ProfileName()`.
    pub scheduler_name: String,
    /// `cfg.Plugins` after defaulting (`mergePlugins` over the default plugin
    /// set); `None` fails `cfgValidator` ("plugins required for profile ...").
    pub plugins: Option<Plugins>,
    /// `cfg.PluginConfig[].Name`, for `NewFramework`'s duplicate-config check.
    pub plugin_config_names: Vec<String>,
    /// `f.ListPlugins().QueueSort.Enabled[0].Name`, filled in by
    /// [`Map::new_map`] from the built framework.
    pub queue_sort: String,
    /// The `PluginConfig` args of the queue sort plugin, if any.
    pub queue_sort_args: Option<serde_json::Value>,
    /// Resolved `VolumeBinding` bind timeout (`VolumeBindingArgs.BindTimeoutSeconds`).
    pub volume_bind_timeout: Duration,
    /// Resolved `VolumeBinding` scorer shape (`VolumeBindingArgs.Shape`).
    pub volume_shape: Vec<FunctionShapePoint>,
    /// `KubeSchedulerProfile.PercentageOfNodesToScore`: overrides the global
    /// value when set (`numFeasibleNodesToFind`, schedule_one.go:717-723).
    pub percentage_of_nodes_to_score: Option<i32>,
    /// `NodeAffinityArgs.AddedAffinity` (node_affinity.go:310-333
    /// `addedNodeSelector` / `addedPrefSchedTerms`).
    pub added_affinity: Option<NodeAffinity>,
    /// `DefaultPreemptionArgs.MinCandidateNodesPercentage`
    /// (default_preemption.go:174-183 `calculateNumCandidates`).
    pub min_candidate_nodes_percentage: i32,
    /// `DefaultPreemptionArgs.MinCandidateNodesAbsolute`.
    pub min_candidate_nodes_absolute: i32,
    /// The built framework for this profile (plugins per extension point +
    /// score weights). [`Map::new_map`] rebuilds it from `plugins`.
    pub framework: Framework,
}

impl Profile {
    /// A defaulted profile named `scheduler_name`: the default plugin set
    /// (`getDefaultPlugins`) resolved through the in-tree registry.
    pub fn named(scheduler_name: &str) -> Self {
        let mut p = Self {
            scheduler_name: scheduler_name.to_string(),
            plugins: Some(merge_plugins(default_plugins(), None)),
            plugin_config_names: Vec::new(),
            queue_sort: DEFAULT_QUEUE_SORT.to_string(),
            queue_sort_args: None,
            volume_bind_timeout: crate::volume_bind::DEFAULT_BIND_TIMEOUT,
            volume_shape: volume_binding::default_shape(),
            percentage_of_nodes_to_score: None,
            added_affinity: None,
            // SetDefaults_DefaultPreemptionArgs (v1/defaults.go:177-184).
            min_candidate_nodes_percentage: 10,
            min_candidate_nodes_absolute: 100,
            framework: Framework::default(),
        };
        p.framework = p
            .build_framework(&Registry::in_tree())
            .expect("the default plugin set is a valid framework");
        p
    }

    /// `newProfile` -> `frameworkruntime.NewFramework` for this profile.
    fn build_framework(&self, registry: &Registry) -> Result<Framework, String> {
        new_framework(
            registry,
            &ProfileSpec {
                scheduler_name: &self.scheduler_name,
                plugins: self.plugins.as_ref(),
                plugin_config_names: &self.plugin_config_names,
                percentage_of_nodes_to_score: self.percentage_of_nodes_to_score,
            },
        )
    }

    /// Apply validated `VolumeBindingArgs` to this profile.
    pub fn with_volume_binding_args(mut self, args: &VolumeBindingArgs) -> Self {
        if let Some(secs) = args.bind_timeout_seconds {
            self.volume_bind_timeout = Duration::from_secs(secs.max(0) as u64);
        }
        let shape = args.scorer_shape();
        if !shape.is_empty() {
            self.volume_shape = shape;
        }
        self
    }
}

/// `profile.Map`: profiles indexed by scheduler name.
#[derive(Debug, Clone, Default)]
pub struct Map {
    profiles: HashMap<String, Profile>,
    /// The global `KubeSchedulerConfiguration.PercentageOfNodesToScore`
    /// (`Scheduler.percentageOfNodesToScore`, scheduler.go:142;
    /// `DefaultPercentageOfNodesToScore` = 0, types.go:217). Carried beside
    /// the profiles because both come from the one configuration.
    pub percentage_of_nodes_to_score: i32,
}

impl Map {
    /// `NewMap` over the in-tree registry.
    pub fn new_map(cfgs: Vec<Profile>) -> Result<Map, String> {
        Self::new_map_with_registry(cfgs, &Registry::in_tree())
    }

    /// `NewMap` (profile.go:49-65) with `cfgValidator.validate` (:98-129).
    pub fn new_map_with_registry(cfgs: Vec<Profile>, registry: &Registry) -> Result<Map, String> {
        let mut m = Map::default();
        // cfgValidator{m: m}: the first profile's queue sort (profile.go:98-129).
        let mut queue_sort: Option<(String, Option<serde_json::Value>)> = None;
        for mut cfg in cfgs {
            // newProfile (profile.go:38-43), error wrapped as profile.go:57.
            cfg.framework = cfg.build_framework(registry).map_err(|e| {
                format!(
                    "creating profile for scheduler name {}: {e}",
                    cfg.scheduler_name
                )
            })?;
            if cfg.scheduler_name.is_empty() {
                return Err("scheduler name is needed".to_string());
            }
            if cfg.plugins.is_none() {
                return Err(format!(
                    "plugins required for profile with scheduler name {:?}",
                    cfg.scheduler_name
                ));
            }
            if m.profiles.contains_key(&cfg.scheduler_name) {
                return Err(format!(
                    "duplicate profile with scheduler name {:?}",
                    cfg.scheduler_name
                ));
            }
            // `f.ListPlugins().QueueSort.Enabled[0].Name`; NewFramework
            // guarantees exactly one queue sort plugin once Plugins != nil.
            cfg.queue_sort = cfg
                .framework
                .plugins_at(ExtensionPoint::QueueSort)
                .first()
                .cloned()
                .unwrap_or_default();
            match &queue_sort {
                None => queue_sort = Some((cfg.queue_sort.clone(), cfg.queue_sort_args.clone())),
                Some((first, first_args)) => {
                    if *first != cfg.queue_sort {
                        return Err(format!(
                            "different queue sort plugins for profile {:?}: {:?}, first: {:?}",
                            cfg.scheduler_name, cfg.queue_sort, first
                        ));
                    }
                    if *first_args != cfg.queue_sort_args {
                        return Err(format!(
                            "different queue sort plugin args for profile {:?}",
                            cfg.scheduler_name
                        ));
                    }
                }
            }
            m.profiles.insert(cfg.scheduler_name.clone(), cfg);
        }
        Ok(m)
    }

    /// `HandlesSchedulerName` (profile.go:67-70).
    pub fn handles_scheduler_name(&self, name: &str) -> bool {
        self.profiles.contains_key(name)
    }

    pub fn get(&self, name: &str) -> Option<&Profile> {
        self.profiles.get(name)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut Profile> {
        self.profiles.get_mut(name)
    }

    pub fn len(&self) -> usize {
        self.profiles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }

    /// Scheduler names, sorted (Go map iteration is unordered; sorted for logs).
    pub fn names(&self) -> Vec<&str> {
        let mut v: Vec<&str> = self.profiles.keys().map(String::as_str).collect();
        v.sort_unstable();
        v
    }

    /// `Scheduler.frameworkForPod` (schedule_one.go:390-396).
    pub fn framework_for_pod(&self, pod: &Pod) -> Result<&Profile, String> {
        let name = pod_scheduler_name(pod);
        self.profiles
            .get(name)
            .ok_or_else(|| format!("profile not found for scheduler name {name:?}"))
    }
}

/// `pod.Spec.SchedulerName`. The API server defaults an unset name to
/// `default-scheduler` (`SetDefaults_PodSpec`); objects read straight from
/// storage may still carry `None`, so apply the same default here.
pub fn pod_scheduler_name(pod: &Pod) -> &str {
    pod.spec
        .as_ref()
        .and_then(|s| s.scheduler_name.as_deref())
        .unwrap_or(DEFAULT_SCHEDULER_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::PodSpec;
    use serde_json::json;

    /// profile_test.go `fakeRegistry`: queue sort plugins `QueueSort` /
    /// `AnotherSort` and the `Bind` plugin, beside the in-tree ones.
    fn fake_registry() -> Registry {
        let mut r = Registry::in_tree();
        r.register("QueueSort", &[ExtensionPoint::QueueSort]);
        r.register("AnotherSort", &[ExtensionPoint::QueueSort]);
        r
    }

    fn fake_map(cfgs: Vec<Profile>) -> Result<Map, String> {
        Map::new_map_with_registry(cfgs, &fake_registry())
    }

    fn p(name: &str, qs: &str, args: Option<serde_json::Value>) -> Profile {
        use crate::framework_runtime::{Plugin, PluginSet};
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
        use crate::framework_runtime::{Plugin, PluginSet};
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
        use crate::framework_runtime::{Plugin, PluginSet};
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
