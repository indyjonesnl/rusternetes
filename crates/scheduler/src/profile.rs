//! Scheduling profiles, indexed by scheduler name.
//!
//! Port of `pkg/scheduler/profile/profile.go` (release-1.35): `Map`, `NewMap`,
//! `HandlesSchedulerName`, `cfgValidator.validate`, and of
//! `Scheduler.frameworkForPod` (`pkg/scheduler/schedule_one.go:390-396`).
//!
//! Rusternetes has no per-profile `framework.Framework` object: the plugin set
//! is compiled in. A [`Profile`] therefore carries the per-profile state the
//! runtime actually consumes (`VolumeBindingArgs`, the event recorder's
//! reporting controller = `schedulerName`) plus the queue-sort identity that
//! `cfgValidator` compares across profiles.

use crate::scheduler_config::VolumeBindingArgs;
use crate::volume_binding::{self, FunctionShapePoint};
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
    /// `cfg.Plugins != nil` (validate: "plugins required for profile ...").
    /// Config defaulting always fills it in.
    pub has_plugins: bool,
    /// `f.ListPlugins().QueueSort.Enabled[0].Name`.
    pub queue_sort: String,
    /// The `PluginConfig` args of the queue sort plugin, if any.
    pub queue_sort_args: Option<serde_json::Value>,
    /// Resolved `VolumeBinding` bind timeout (`VolumeBindingArgs.BindTimeoutSeconds`).
    pub volume_bind_timeout: Duration,
    /// Resolved `VolumeBinding` scorer shape (`VolumeBindingArgs.Shape`).
    pub volume_shape: Vec<FunctionShapePoint>,
}

impl Profile {
    /// A defaulted profile named `scheduler_name`.
    pub fn named(scheduler_name: &str) -> Self {
        Self {
            scheduler_name: scheduler_name.to_string(),
            has_plugins: true,
            queue_sort: DEFAULT_QUEUE_SORT.to_string(),
            queue_sort_args: None,
            volume_bind_timeout: crate::volume_bind::DEFAULT_BIND_TIMEOUT,
            volume_shape: volume_binding::default_shape(),
        }
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
}

impl Map {
    /// `NewMap` (profile.go:45-58) with `cfgValidator.validate` (:93-123).
    pub fn new_map(cfgs: Vec<Profile>) -> Result<Map, String> {
        let mut m = Map::default();
        // cfgValidator{m: m}: the first profile's queue sort (:93-123).
        let mut queue_sort: Option<(String, Option<serde_json::Value>)> = None;
        for cfg in cfgs {
            if cfg.scheduler_name.is_empty() {
                return Err("scheduler name is needed".to_string());
            }
            if !cfg.has_plugins {
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

    fn p(name: &str, qs: &str, args: Option<serde_json::Value>) -> Profile {
        Profile {
            scheduler_name: name.to_string(),
            queue_sort: qs.to_string(),
            queue_sort_args: args,
            ..Profile::named(name)
        }
    }

    // profile_test.go TestNewMap "valid".
    #[test]
    fn new_map_valid() {
        let m = Map::new_map(vec![
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
        let err = Map::new_map(vec![
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
        let err = Map::new_map(vec![
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
        let err = Map::new_map(vec![
            p("profile-1", "QueueSort", None),
            p("profile-1", "QueueSort", None),
        ])
        .unwrap_err();
        assert_eq!(err, "duplicate profile with scheduler name \"profile-1\"");
    }

    // "scheduler name is needed".
    #[test]
    fn new_map_scheduler_name_needed() {
        let err = Map::new_map(vec![p("", "QueueSort", None)]).unwrap_err();
        assert_eq!(err, "scheduler name is needed");
    }

    // "plugins required for profile".
    #[test]
    fn new_map_plugins_required() {
        let mut prof = p("profile-1", "QueueSort", None);
        prof.has_plugins = false;
        let err = Map::new_map(vec![prof]).unwrap_err();
        assert_eq!(
            err,
            "plugins required for profile with scheduler name \"profile-1\""
        );
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
        let m = Map::new_map(vec![
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
