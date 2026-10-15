//! Per-profile plugin resolution: `config.Plugins`, the default plugin set and
//! its merge with a profile's own `plugins`, and the plugin-list part of
//! `frameworkruntime.NewFramework`.
//!
//! Ported from (release-1.35):
//! - `pkg/scheduler/apis/config/v1/default_plugins.go` (`getDefaultPlugins`,
//!   `applyFeatureGates`, `mergePlugins`, `mergePluginSet`);
//! - `pkg/scheduler/framework/runtime/framework.go` (`NewFramework`,
//!   `getExtensionPoints`, `updatePluginList`, `expandMultiPointPlugins`,
//!   `getScoreWeights`, `pluginsNeeded`, `ListPlugins`);
//! - `pkg/scheduler/framework/plugins/registry.go` (`NewInTreeRegistry`) and
//!   the `var _ fwk.XPlugin = &Name{}` assertions of each in-tree plugin for
//!   which extension points it implements.
//!
//! Rusternetes has no per-plugin objects (the filter/score pipeline in
//! `scheduler.rs` is compiled in), so a plugin's "implementation" here is the
//! set of extension points it implements; the resolved [`Framework`] is what
//! the pipeline consults to decide which stages run for a profile.

use rusternetes_common::feature_gates::{self, Feature};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

/// `names.*` (pkg/scheduler/framework/plugins/names/names.go).
pub mod names {
    pub const PRIORITY_SORT: &str = "PrioritySort";
    pub const DEFAULT_BINDER: &str = "DefaultBinder";
    pub const DEFAULT_PREEMPTION: &str = "DefaultPreemption";
    pub const DYNAMIC_RESOURCES: &str = "DynamicResources";
    pub const GANG_SCHEDULING: &str = "GangScheduling";
    pub const IMAGE_LOCALITY: &str = "ImageLocality";
    pub const INTER_POD_AFFINITY: &str = "InterPodAffinity";
    pub const NODE_AFFINITY: &str = "NodeAffinity";
    pub const NODE_DECLARED_FEATURES: &str = "NodeDeclaredFeatures";
    pub const NODE_NAME: &str = "NodeName";
    pub const NODE_PORTS: &str = "NodePorts";
    pub const NODE_RESOURCES_BALANCED_ALLOCATION: &str = "NodeResourcesBalancedAllocation";
    pub const NODE_RESOURCES_FIT: &str = "NodeResourcesFit";
    pub const NODE_UNSCHEDULABLE: &str = "NodeUnschedulable";
    pub const NODE_VOLUME_LIMITS: &str = "NodeVolumeLimits";
    pub const POD_TOPOLOGY_SPREAD: &str = "PodTopologySpread";
    pub const SCHEDULING_GATES: &str = "SchedulingGates";
    pub const TAINT_TOLERATION: &str = "TaintToleration";
    pub const VOLUME_BINDING: &str = "VolumeBinding";
    pub const VOLUME_RESTRICTIONS: &str = "VolumeRestrictions";
    pub const VOLUME_ZONE: &str = "VolumeZone";
}

/// `config.Plugin` (pkg/scheduler/apis/config/types.go). The internal type has
/// no json tags, so a marshalled bad value uses the Go field names. The v1
/// `Weight *int32` converts a nil to 0 (`Convert_Pointer_int32_To_int32`).
#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
pub struct Plugin {
    pub name: String,
    #[serde(default)]
    pub weight: Option<i32>,
}

impl Plugin {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            weight: None,
        }
    }

    pub fn weighted(name: &str, weight: i32) -> Self {
        Self {
            name: name.to_string(),
            weight: Some(weight),
        }
    }
}

/// `config.PluginSet`. `apiequality.Semantic.DeepEqual` treats nil and empty
/// slices as equal, which `Vec` equality already does.
#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
pub struct PluginSet {
    #[serde(default)]
    pub enabled: Vec<Plugin>,
    #[serde(default)]
    pub disabled: Vec<Plugin>,
}

impl PluginSet {
    /// `json.Marshal` of the internal `config.PluginSet` (no json tags; a nil
    /// slice marshals as `null`).
    pub fn go_json(&self) -> String {
        fn list(v: &[Plugin]) -> String {
            if v.is_empty() {
                return "null".to_string();
            }
            let items: Vec<String> = v
                .iter()
                .map(|p| {
                    let name = serde_json::Value::String(p.name.clone());
                    format!("{{\"Name\":{name},\"Weight\":{}}}", p.weight.unwrap_or(0))
                })
                .collect();
            format!("[{}]", items.join(","))
        }
        // Struct field order, which serde_json's map would sort away.
        format!(
            "{{\"Enabled\":{},\"Disabled\":{}}}",
            list(&self.enabled),
            list(&self.disabled)
        )
    }
}

/// The extension points of `config.Plugins`, in `getExtensionPoints` order
/// (framework.go:118-133). `multiPoint` is not one of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExtensionPoint {
    PreFilter,
    Filter,
    PostFilter,
    Reserve,
    PreScore,
    Score,
    PreBind,
    Bind,
    PostBind,
    Permit,
    PreEnqueue,
    QueueSort,
}

impl ExtensionPoint {
    /// `getExtensionPoints` order.
    pub const ALL: [ExtensionPoint; 12] = [
        ExtensionPoint::PreFilter,
        ExtensionPoint::Filter,
        ExtensionPoint::PostFilter,
        ExtensionPoint::Reserve,
        ExtensionPoint::PreScore,
        ExtensionPoint::Score,
        ExtensionPoint::PreBind,
        ExtensionPoint::Bind,
        ExtensionPoint::PostBind,
        ExtensionPoint::Permit,
        ExtensionPoint::PreEnqueue,
        ExtensionPoint::QueueSort,
    ];

    /// The Go interface's type name (`reflect.Type.Name()`), used in
    /// `NewFramework` error text.
    pub fn interface_name(self) -> &'static str {
        match self {
            ExtensionPoint::PreFilter => "PreFilterPlugin",
            ExtensionPoint::Filter => "FilterPlugin",
            ExtensionPoint::PostFilter => "PostFilterPlugin",
            ExtensionPoint::Reserve => "ReservePlugin",
            ExtensionPoint::PreScore => "PreScorePlugin",
            ExtensionPoint::Score => "ScorePlugin",
            ExtensionPoint::PreBind => "PreBindPlugin",
            ExtensionPoint::Bind => "BindPlugin",
            ExtensionPoint::PostBind => "PostBindPlugin",
            ExtensionPoint::Permit => "PermitPlugin",
            ExtensionPoint::PreEnqueue => "PreEnqueuePlugin",
            ExtensionPoint::QueueSort => "QueueSortPlugin",
        }
    }
}

/// `config.Plugins`: a `PluginSet` per extension point plus `MultiPoint`.
#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Plugins {
    #[serde(default)]
    pub multi_point: PluginSet,
    #[serde(default)]
    pub pre_enqueue: PluginSet,
    #[serde(default)]
    pub queue_sort: PluginSet,
    #[serde(default)]
    pub pre_filter: PluginSet,
    #[serde(default)]
    pub filter: PluginSet,
    #[serde(default)]
    pub post_filter: PluginSet,
    #[serde(default)]
    pub pre_score: PluginSet,
    #[serde(default)]
    pub score: PluginSet,
    #[serde(default)]
    pub reserve: PluginSet,
    #[serde(default)]
    pub permit: PluginSet,
    #[serde(default)]
    pub pre_bind: PluginSet,
    #[serde(default)]
    pub bind: PluginSet,
    #[serde(default)]
    pub post_bind: PluginSet,
}

impl Plugins {
    pub fn set(&self, ep: ExtensionPoint) -> &PluginSet {
        match ep {
            ExtensionPoint::PreFilter => &self.pre_filter,
            ExtensionPoint::Filter => &self.filter,
            ExtensionPoint::PostFilter => &self.post_filter,
            ExtensionPoint::Reserve => &self.reserve,
            ExtensionPoint::PreScore => &self.pre_score,
            ExtensionPoint::Score => &self.score,
            ExtensionPoint::PreBind => &self.pre_bind,
            ExtensionPoint::Bind => &self.bind,
            ExtensionPoint::PostBind => &self.post_bind,
            ExtensionPoint::Permit => &self.permit,
            ExtensionPoint::PreEnqueue => &self.pre_enqueue,
            ExtensionPoint::QueueSort => &self.queue_sort,
        }
    }

    pub fn set_mut(&mut self, ep: ExtensionPoint) -> &mut PluginSet {
        match ep {
            ExtensionPoint::PreFilter => &mut self.pre_filter,
            ExtensionPoint::Filter => &mut self.filter,
            ExtensionPoint::PostFilter => &mut self.post_filter,
            ExtensionPoint::Reserve => &mut self.reserve,
            ExtensionPoint::PreScore => &mut self.pre_score,
            ExtensionPoint::Score => &mut self.score,
            ExtensionPoint::PreBind => &mut self.pre_bind,
            ExtensionPoint::Bind => &mut self.bind,
            ExtensionPoint::PostBind => &mut self.post_bind,
            ExtensionPoint::Permit => &mut self.permit,
            ExtensionPoint::PreEnqueue => &mut self.pre_enqueue,
            ExtensionPoint::QueueSort => &mut self.queue_sort,
        }
    }
}

// ---------------------------------------------------------------------------
// Defaults (apis/config/v1/default_plugins.go)
// ---------------------------------------------------------------------------

/// `getDefaultPlugins` (default_plugins.go:30-54) with `applyFeatureGates`
/// (:56-67).
pub fn default_plugins() -> Plugins {
    use names::*;
    let mut plugins = Plugins {
        multi_point: PluginSet {
            enabled: vec![
                Plugin::new(SCHEDULING_GATES),
                Plugin::new(PRIORITY_SORT),
                Plugin::new(NODE_UNSCHEDULABLE),
                Plugin::new(NODE_NAME),
                Plugin::weighted(TAINT_TOLERATION, 3),
                Plugin::weighted(NODE_AFFINITY, 2),
                Plugin::new(NODE_PORTS),
                Plugin::weighted(NODE_RESOURCES_FIT, 1),
                Plugin::new(VOLUME_RESTRICTIONS),
                Plugin::new(NODE_VOLUME_LIMITS),
                Plugin::new(VOLUME_BINDING),
                Plugin::new(VOLUME_ZONE),
                Plugin::weighted(POD_TOPOLOGY_SPREAD, 2),
                Plugin::weighted(INTER_POD_AFFINITY, 2),
                Plugin::new(DEFAULT_PREEMPTION),
                Plugin::weighted(NODE_RESOURCES_BALANCED_ALLOCATION, 1),
                Plugin::weighted(IMAGE_LOCALITY, 1),
                Plugin::new(DEFAULT_BINDER),
            ],
            disabled: Vec::new(),
        },
        ..Default::default()
    };
    apply_feature_gates(&mut plugins);
    plugins
}

fn apply_feature_gates(config: &mut Plugins) {
    if feature_gates::enabled(Feature::NodeDeclaredFeatures) {
        config
            .multi_point
            .enabled
            .push(Plugin::new(names::NODE_DECLARED_FEATURES));
    }
    if feature_gates::enabled(Feature::DynamicResourceAllocation) {
        // `applyDynamicResources`: before DefaultPreemption, so an idle
        // ResourceClaim is deallocated before a Pod is evicted.
        if let Some(i) = config
            .multi_point
            .enabled
            .iter()
            .position(|p| p.name == names::DEFAULT_PREEMPTION)
        {
            config
                .multi_point
                .enabled
                .insert(i, Plugin::weighted(names::DYNAMIC_RESOURCES, 2));
        }
    }
    if feature_gates::enabled(Feature::GangScheduling) {
        config
            .multi_point
            .enabled
            .push(Plugin::new(names::GANG_SCHEDULING));
    }
}

/// `mergePlugins` (default_plugins.go:94-112): merge the custom set into the
/// given default one, handling disabled sets.
pub fn merge_plugins(mut default_plugins: Plugins, custom: Option<&Plugins>) -> Plugins {
    let Some(custom) = custom else {
        return default_plugins;
    };
    default_plugins.multi_point =
        merge_plugin_set(&default_plugins.multi_point, &custom.multi_point);
    for ep in ExtensionPoint::ALL {
        let merged = merge_plugin_set(default_plugins.set(ep), custom.set(ep));
        *default_plugins.set_mut(ep) = merged;
    }
    default_plugins
}

/// `mergePluginSet` (default_plugins.go:119-173).
fn merge_plugin_set(default_set: &PluginSet, custom_set: &PluginSet) -> PluginSet {
    let mut disabled_plugins: HashSet<&str> = HashSet::new();
    let mut disabled: Vec<Plugin> = Vec::new();
    // A user disabling default plugins (or all, with "*") for an extension
    // point is tracked so MultiPoint expansion knows to skip inserting
    // unspecified default plugins there.
    for p in &custom_set.disabled {
        disabled.push(Plugin::new(&p.name));
        disabled_plugins.insert(p.name.as_str());
    }
    // With MultiPoint a default set may itself carry disabled plugins.
    for p in &default_set.disabled {
        disabled.push(Plugin::new(&p.name));
        disabled_plugins.insert(p.name.as_str());
    }
    // enabledCustomPlugins: name -> (index, plugin); the last duplicate wins,
    // as Go's map assignment.
    let mut enabled_custom: HashMap<&str, (usize, &Plugin)> = HashMap::new();
    for (index, p) in custom_set.enabled.iter().enumerate() {
        enabled_custom.insert(p.name.as_str(), (index, p));
    }
    let mut replaced: HashSet<usize> = HashSet::new();
    let mut enabled: Vec<Plugin> = Vec::new();
    if !disabled_plugins.contains("*") {
        for default_enabled in &default_set.enabled {
            if disabled_plugins.contains(default_enabled.name.as_str()) {
                continue;
            }
            // The default plugin is explicitly re-configured: update it in
            // place to preserve order.
            if let Some((index, custom)) = enabled_custom.get(default_enabled.name.as_str()) {
                replaced.insert(*index);
                enabled.push((*custom).clone());
                continue;
            }
            enabled.push(default_enabled.clone());
        }
    }
    // Append all the custom plugins which haven't replaced any default
    // plugin. Duplicated custom plugins are appended too; framework
    // construction detects and rejects them.
    for (index, p) in custom_set.enabled.iter().enumerate() {
        if !replaced.contains(&index) {
            enabled.push(p.clone());
        }
    }
    PluginSet { enabled, disabled }
}

// ---------------------------------------------------------------------------
// Registry (plugins/registry.go NewInTreeRegistry)
// ---------------------------------------------------------------------------

/// `runtime.Registry`: plugin name -> the extension points the plugin
/// implements (what Go checks with `reflect.Type.Implements`).
#[derive(Debug, Clone, Default)]
pub struct Registry {
    plugins: HashMap<String, HashSet<ExtensionPoint>>,
}

impl Registry {
    /// `NewInTreeRegistry`, with each plugin's extension points taken from its
    /// `var _ fwk.XPlugin = &Name{}` assertions.
    pub fn in_tree() -> Self {
        use names::*;
        use ExtensionPoint::*;
        let mut r = Registry::default();
        for (name, points) in [
            (
                DYNAMIC_RESOURCES,
                &[
                    PreEnqueue, PreFilter, Filter, PostFilter, Score, Reserve, PreBind,
                ][..],
            ),
            (IMAGE_LOCALITY, &[Score][..]),
            (TAINT_TOLERATION, &[Filter, PreScore, Score][..]),
            (NODE_NAME, &[Filter][..]),
            (NODE_PORTS, &[PreFilter, Filter][..]),
            (NODE_AFFINITY, &[PreFilter, Filter, PreScore, Score][..]),
            (NODE_DECLARED_FEATURES, &[PreFilter, Filter][..]),
            (
                POD_TOPOLOGY_SPREAD,
                &[PreFilter, Filter, PreScore, Score][..],
            ),
            (NODE_UNSCHEDULABLE, &[Filter][..]),
            (
                NODE_RESOURCES_FIT,
                &[PreFilter, Filter, PreScore, Score][..],
            ),
            (NODE_RESOURCES_BALANCED_ALLOCATION, &[PreScore, Score][..]),
            (
                VOLUME_BINDING,
                &[PreFilter, Filter, Reserve, PreBind, PreScore, Score][..],
            ),
            (VOLUME_RESTRICTIONS, &[PreFilter, Filter][..]),
            (VOLUME_ZONE, &[PreFilter, Filter][..]),
            (NODE_VOLUME_LIMITS, &[PreFilter, Filter][..]),
            (
                INTER_POD_AFFINITY,
                &[PreFilter, Filter, PreScore, Score][..],
            ),
            (PRIORITY_SORT, &[QueueSort][..]),
            (DEFAULT_BINDER, &[Bind][..]),
            (DEFAULT_PREEMPTION, &[PostFilter, PreEnqueue][..]),
            (SCHEDULING_GATES, &[PreEnqueue][..]),
            (GANG_SCHEDULING, &[PreEnqueue, Reserve, Permit][..]),
        ] {
            r.register(name, points);
        }
        r
    }

    /// Register a plugin (`Registry.Register`, also used for out-of-tree
    /// plugins / test fakes).
    pub fn register(&mut self, name: &str, points: &[ExtensionPoint]) {
        self.plugins
            .insert(name.to_string(), points.iter().copied().collect());
    }

    fn get(&self, name: &str) -> Option<&HashSet<ExtensionPoint>> {
        self.plugins.get(name)
    }
}

// ---------------------------------------------------------------------------
// NewFramework
// ---------------------------------------------------------------------------

/// `fwk.MaxNodeScore` / `fwk.MaxTotalScore` (getScoreWeights overflow guard).
const MAX_NODE_SCORE: i64 = 100;
const MAX_TOTAL_SCORE: i64 = i64::MAX;

/// The slice of a `config.KubeSchedulerProfile` `NewFramework` reads.
#[derive(Debug, Clone, Default)]
pub struct ProfileSpec<'a> {
    pub scheduler_name: &'a str,
    pub plugins: Option<&'a Plugins>,
    /// `profile.PluginConfig[].Name`, for the duplicate-config check.
    pub plugin_config_names: &'a [String],
}

/// A resolved profile framework: per-extension-point plugin lists (after
/// MultiPoint expansion) and the score weights.
#[derive(Debug, Clone, Default)]
pub struct Framework {
    pub profile_name: String,
    lists: HashMap<ExtensionPoint, Vec<String>>,
    score_plugin_weight: HashMap<String, i32>,
}

impl Framework {
    /// Plugins configured at `ep`, in execution order.
    pub fn plugins_at(&self, ep: ExtensionPoint) -> &[String] {
        self.lists.get(&ep).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Whether `name` runs at `ep`.
    pub fn has(&self, ep: ExtensionPoint, name: &str) -> bool {
        self.plugins_at(ep).iter().any(|n| n == name)
    }

    /// `f.scorePluginWeight[name]` (0 when `name` is not a score plugin).
    pub fn score_weight(&self, name: &str) -> i32 {
        self.score_plugin_weight.get(name).copied().unwrap_or(0)
    }

    /// `HasFilterPlugins`.
    pub fn has_filter_plugins(&self) -> bool {
        !self.plugins_at(ExtensionPoint::Filter).is_empty()
    }

    /// `HasPostFilterPlugins`.
    pub fn has_post_filter_plugins(&self) -> bool {
        !self.plugins_at(ExtensionPoint::PostFilter).is_empty()
    }

    /// `HasScorePlugins`.
    pub fn has_score_plugins(&self) -> bool {
        !self.plugins_at(ExtensionPoint::Score).is_empty()
    }

    /// `ListPlugins` (framework.go:1820-1841): the enabled plugins per
    /// extension point; weights only on Score plugins.
    pub fn list_plugins(&self) -> Plugins {
        let mut m = Plugins::default();
        for ep in ExtensionPoint::ALL {
            let cfgs: Vec<Plugin> = self
                .plugins_at(ep)
                .iter()
                .map(|name| Plugin {
                    name: name.clone(),
                    weight: (ep == ExtensionPoint::Score).then(|| self.score_weight(name)),
                })
                .collect();
            if !cfgs.is_empty() {
                m.set_mut(ep).enabled = cfgs;
            }
        }
        m
    }
}

/// `NewFramework` (framework.go:306-452), minus the parts with no Rusternetes
/// counterpart (plugin factories/handles, metrics, parallelizer).
pub fn new_framework(r: &Registry, profile: &ProfileSpec<'_>) -> Result<Framework, String> {
    let mut f = Framework {
        profile_name: profile.scheduler_name.to_string(),
        ..Default::default()
    };
    let Some(plugins) = profile.plugins else {
        return Ok(f);
    };

    // pluginsNeeded (framework.go:1878-1898)
    let mut needed: HashSet<&str> = HashSet::new();
    for ep in ExtensionPoint::ALL {
        for p in &plugins.set(ep).enabled {
            needed.insert(p.name.as_str());
        }
    }
    for p in &plugins.multi_point.enabled {
        needed.insert(p.name.as_str());
    }

    let mut seen_config: HashSet<&str> = HashSet::new();
    for name in profile.plugin_config_names {
        if !seen_config.insert(name.as_str()) {
            return Err(format!("repeated config for plugin {name}"));
        }
    }

    // pluginsMap: only needed plugins that the registry knows.
    let plugins_map: HashMap<&str, &HashSet<ExtensionPoint>> = needed
        .iter()
        .filter_map(|n| r.get(n).map(|pts| (*n, pts)))
        .collect();

    // initialize plugins per individual extension points (updatePluginList)
    for ep in ExtensionPoint::ALL {
        let iface = ep.interface_name();
        let mut set: HashSet<&str> = HashSet::new();
        let mut list: Vec<String> = Vec::new();
        for p in &plugins.set(ep).enabled {
            let Some(pts) = plugins_map.get(p.name.as_str()) else {
                return Err(format!("{iface} {:?} does not exist", p.name));
            };
            if !pts.contains(&ep) {
                return Err(format!(
                    "plugin {:?} does not extend {iface} plugin",
                    p.name
                ));
            }
            if !set.insert(p.name.as_str()) {
                return Err(format!(
                    "plugin {:?} already registered as {iface:?}",
                    p.name
                ));
            }
            list.push(p.name.clone());
        }
        f.lists.insert(ep, list);
    }

    // initialize multiPoint plugins to their expanded extension points
    if !plugins.multi_point.enabled.is_empty() {
        expand_multi_point_plugins(&mut f, plugins, &plugins_map)?;
    }

    let queue_sort = f.plugins_at(ExtensionPoint::QueueSort).len();
    if queue_sort != 1 {
        return Err(format!(
            "only one queue sort plugin required for profile with scheduler name {:?}, but got {queue_sort}",
            profile.scheduler_name
        ));
    }
    if f.plugins_at(ExtensionPoint::Bind).is_empty() {
        return Err(format!(
            "at least one bind plugin is needed for profile with scheduler name {:?}",
            profile.scheduler_name
        ));
    }

    let mut score_candidates: Vec<&Plugin> = plugins.score.enabled.iter().collect();
    score_candidates.extend(plugins.multi_point.enabled.iter());
    get_score_weights(&mut f, &score_candidates, &plugins_map)?;

    // Verifying the score weights again since Plugin.Name() could return a
    // different value from the one used in the configuration.
    for name in f.plugins_at(ExtensionPoint::Score) {
        if f.score_weight(name) == 0 {
            return Err(format!(
                "score plugin {name:?} is not configured with weight"
            ));
        }
    }
    Ok(f)
}

/// `getScoreWeights` (framework.go:514-543): between MultiPoint-Score plugin
/// weights and individual Score plugin weights there must be no overflow of
/// MaxTotalScore.
fn get_score_weights(
    f: &mut Framework,
    plugins: &[&Plugin],
    plugins_map: &HashMap<&str, &HashSet<ExtensionPoint>>,
) -> Result<(), String> {
    let mut total_priority: i64 = 0;
    for e in plugins {
        let Some(pts) = plugins_map.get(e.name.as_str()) else {
            continue;
        };
        if !pts.contains(&ExtensionPoint::Score) {
            continue;
        }
        // MultiPoint plugins are appended after the Score ones, so the
        // individual Score weight takes precedence.
        if f.score_plugin_weight.contains_key(&e.name) {
            continue;
        }
        // A weight of zero is not permitted; plugins can be disabled
        // explicitly when configured.
        let mut w = e.weight.unwrap_or(0);
        if w == 0 {
            w = 1;
        }
        f.score_plugin_weight.insert(e.name.clone(), w);
        // Checks totalPriority against MaxTotalScore to avoid overflow.
        if i64::from(w) * MAX_NODE_SCORE > MAX_TOTAL_SCORE - total_priority {
            return Err("total score of Score plugins could overflow".to_string());
        }
        total_priority += i64::from(w) * MAX_NODE_SCORE;
    }
    Ok(())
}

/// `expandMultiPointPlugins` (framework.go:576-661).
fn expand_multi_point_plugins(
    f: &mut Framework,
    plugins: &Plugins,
    plugins_map: &HashMap<&str, &HashSet<ExtensionPoint>>,
) -> Result<(), String> {
    for ep in ExtensionPoint::ALL {
        let iface = ep.interface_name();
        let set = plugins.set(ep);
        // enabledSet: plugins already registered via normal extension points,
        // to check double registration (insertion ordered).
        let mut enabled_set: Vec<&str> = Vec::new();
        for p in &set.enabled {
            if !enabled_set.contains(&p.name.as_str()) {
                enabled_set.push(p.name.as_str());
            }
        }
        let disabled_set: HashSet<&str> = set.disabled.iter().map(|p| p.name.as_str()).collect();
        if disabled_set.contains("*") {
            // Skipped MultiPoint expansion: all plugins are disabled for this
            // extension point.
            continue;
        }

        // Plugins enabled via multipoint are tracked separately from those
        // enabled by specific extensions, to distinguish double-registration
        // from explicit overrides.
        let mut multi_point_enabled: Vec<&str> = Vec::new();
        let mut override_plugins: HashSet<&str> = HashSet::new();
        for ep_plugin in &plugins.multi_point.enabled {
            let Some(pts) = plugins_map.get(ep_plugin.name.as_str()) else {
                return Err(format!("{iface} {:?} does not exist", ep_plugin.name));
            };
            // Skip plugins that don't implement the type being expanded.
            if !pts.contains(&ep) {
                continue;
            }
            // A MultiPoint plugin can still be disabled for specific points.
            if disabled_set.contains(ep_plugin.name.as_str()) {
                continue;
            }
            // Already enabled by the specific extension point: the user intent
            // is to override the default plugin or make some other explicit
            // setting, so the MultiPoint value is discarded
            // (kubernetes/kubernetes#99582).
            if enabled_set.contains(&ep_plugin.name.as_str()) {
                override_plugins.insert(ep_plugin.name.as_str());
                continue;
            }
            // Already registered via MultiPoint: a double registration.
            if multi_point_enabled.contains(&ep_plugin.name.as_str()) {
                return Err(format!(
                    "plugin {:?} already registered as {iface:?}",
                    ep_plugin.name
                ));
            }
            multi_point_enabled.push(ep_plugin.name.as_str());
        }

        // Reorder plugins. Expected order:
        // - part 1: overridePlugins, in the order of the regular extension point;
        // - part 2: multiPointEnabled (in multipoint, not in the regular point);
        // - part 3: the other plugins of the regular extension point.
        let mut new_plugins: Vec<String> = Vec::new();
        let mut remaining: Vec<&str> = Vec::new();
        for name in &enabled_set {
            if override_plugins.contains(name) {
                new_plugins.push((*name).to_string());
            } else {
                remaining.push(name);
            }
        }
        for name in &multi_point_enabled {
            new_plugins.push((*name).to_string());
        }
        for name in remaining {
            new_plugins.push(name.to_string());
        }
        f.lists.insert(ep, new_plugins);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        );
        r.register(QUEUE_SORT, &[QueueSort]);
        r.register(BIND, &[Bind]);
        r.register(SCORE1, &[PreScore, Score]);
        r.register(SCORE2, &[PreScore, Score]);
        r.register(SCORE_WITH_NORMALIZE1, &[Score]);
        r.register(NOT_SCORE, &[Filter]);
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
}
