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

use crate::fwk::{
    CycleState, FilterPlugin, Handle, NodeInfo, NodeScore, PodSignature, PreBindPlugin,
    PreFilterPlugin, PreFilterResult, PreScorePlugin, ReservePlugin, ScorePlugin, SignPlugin,
    Status,
};
use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::resources::pod::NodeAffinity;
use rusternetes_common::resources::{Node, Pod};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
    pub plugins: HashMap<String, HashSet<ExtensionPoint>>,
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
            r.register(name, points)
                .expect("in-tree plugin names are unique");
        }
        r
    }

    /// `Registry.Register` (framework/runtime/registry.go): add a plugin, or
    /// fail if one with the same name exists (the existing entry is kept).
    pub fn register(&mut self, name: &str, points: &[ExtensionPoint]) -> Result<(), String> {
        if self.plugins.contains_key(name) {
            return Err(format!("a plugin named {name} already exists"));
        }
        self.plugins
            .insert(name.to_string(), points.iter().copied().collect());
        Ok(())
    }

    /// `Registry.Unregister`: remove a plugin, or fail if none has that name.
    pub fn unregister(&mut self, name: &str) -> Result<(), String> {
        if self.plugins.remove(name).is_none() {
            return Err(format!("no plugin named {name} exists"));
        }
        Ok(())
    }

    /// `Registry.Merge`: register every plugin of `other`, stopping at the
    /// first duplicate. As in Go, plugins merged before the error stay in `self`
    /// (map iteration order there is random; ours is too).
    /// `scheduler.New` uses it to add `WithFrameworkOutOfTreeRegistry` to
    /// `NewInTreeRegistry` (scheduler.go).
    pub fn merge(&mut self, other: &Registry) -> Result<(), String> {
        for (name, points) in &other.plugins {
            if self.plugins.contains_key(name) {
                return Err(format!("a plugin named {name} already exists"));
            }
            self.plugins.insert(name.clone(), points.clone());
        }
        Ok(())
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
    /// `profile.PercentageOfNodesToScore` (framework.go:354).
    pub percentage_of_nodes_to_score: Option<i32>,
    /// `NodeAffinityArgs.AddedAffinity`, handed to the NodeAffinity plugin
    /// (node_affinity.go:310-333).
    pub added_affinity: Option<&'a NodeAffinity>,
    /// `NodeResourcesFitArgs.ScoringStrategy` as a scorer, handed to the
    /// NodeResourcesFit score plugin.
    pub fit_scorer: Option<&'a crate::score::ResourceAllocationScorer>,
    /// `NodeResourcesBalancedAllocationArgs` as a scorer.
    pub balanced_scorer: Option<&'a crate::score::ResourceAllocationScorer>,
    /// `InterPodAffinityArgs`, handed to the InterPodAffinity score plugin.
    pub inter_pod_affinity: crate::inter_pod_affinity_score::Args,
    /// `PodTopologySpreadArgs`, handed to the PodTopologySpread score plugin.
    pub pod_topology_spread: Option<&'a crate::pod_topology_spread_score::Args>,
    /// `VolumeBindingArgs.Shape`, handed to the VolumeBinding score plugin.
    pub volume_shape: Option<&'a [crate::volume_binding::FunctionShapePoint]>,
}

/// A resolved profile framework: per-extension-point plugin lists (after
/// MultiPoint expansion) and the score weights.
#[derive(Debug, Clone, Default)]
pub struct Framework {
    /// `f.filterPlugins` (framework.go:68): the Filter plugin objects, in
    /// execution order. Only plugins that are objects (see
    /// [`crate::in_tree::new_filter_plugin`]) appear here; the others still
    /// run inline in the scheduling cycle while they migrate (#3455).
    filter_plugin_objects: FilterPluginObjects,
    /// `f.preFilterPlugins` (framework.go:66): the PreFilter plugin objects,
    /// in execution order; the rest still run inline (#3455).
    pre_filter_plugin_objects: PreFilterPluginObjects,
    /// `f.preScorePlugins` / `f.scorePlugins` (framework.go:67-68): the
    /// PreScore and Score plugin objects, in execution order; the rest still
    /// score inline (#4231).
    pre_score_plugin_objects: PreScorePluginObjects,
    score_plugin_objects: ScorePluginObjects,
    /// `f.reservePlugins` / `f.preBindPlugins` (framework.go:70-71): the
    /// Reserve and PreBind plugin objects, in execution order.
    reserve_plugin_objects: ReservePluginObjects,
    pre_bind_plugin_objects: PreBindPluginObjects,
    /// `f.batchablePlugins` (framework.go:75): the signers of the
    /// PreFilter/Filter/PreScore/Score plugins, once per plugin name.
    batchable_plugins: SignPluginObjects,
    /// `f.enableSignatures` (framework.go:104).
    enable_signatures: bool,
    pub profile_name: String,
    /// `f.percentageOfNodesToScore` (framework.go:354); set even when the
    /// profile has no plugins.
    pub percentage_of_nodes_to_score: Option<i32>,
    lists: HashMap<ExtensionPoint, Vec<String>>,
    score_plugin_weight: HashMap<String, i32>,
    /// `f.enqueueExtensions` (framework.go:63): the enabled plugins whose
    /// type is PreEnqueue/PreFilter/Filter/Reserve/Permit
    /// (`shouldHaveEnqueueExtensions`, framework.go:663-671), in
    /// `fillEnqueueExtensions` order.
    enqueue_extensions: Vec<String>,
}

/// The Filter plugin objects of a [`Framework`].
#[derive(Clone, Default)]
pub struct FilterPluginObjects(Vec<Arc<dyn FilterPlugin>>);

impl std::fmt::Debug for FilterPluginObjects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|p| p.name()))
            .finish()
    }
}

/// The PreScore plugin objects of a [`Framework`].
#[derive(Clone, Default)]
pub struct PreScorePluginObjects(Vec<Arc<dyn PreScorePlugin>>);

impl std::fmt::Debug for PreScorePluginObjects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|p| p.name()))
            .finish()
    }
}

/// The Reserve plugin objects of a [`Framework`].
#[derive(Clone, Default)]
pub struct ReservePluginObjects(Vec<Arc<dyn ReservePlugin>>);

impl std::fmt::Debug for ReservePluginObjects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|p| p.name()))
            .finish()
    }
}

/// The PreBind plugin objects of a [`Framework`].
#[derive(Clone, Default)]
pub struct PreBindPluginObjects(Vec<Arc<dyn PreBindPlugin>>);

impl std::fmt::Debug for PreBindPluginObjects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|p| p.name()))
            .finish()
    }
}

/// The Score plugin objects of a [`Framework`].
#[derive(Clone, Default)]
pub struct ScorePluginObjects(Vec<Arc<dyn ScorePlugin>>);

impl std::fmt::Debug for ScorePluginObjects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|p| p.name()))
            .finish()
    }
}

/// The sign plugin objects of a [`Framework`].
#[derive(Clone, Default)]
pub struct SignPluginObjects(Vec<Arc<dyn SignPlugin>>);

impl std::fmt::Debug for SignPluginObjects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|p| p.name()))
            .finish()
    }
}

/// The PreFilter plugin objects of a [`Framework`].
#[derive(Clone, Default)]
pub struct PreFilterPluginObjects(Vec<Arc<dyn PreFilterPlugin>>);

impl std::fmt::Debug for PreFilterPluginObjects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|p| p.name()))
            .finish()
    }
}

impl Framework {
    /// `RunPreScorePlugins` (framework.go:1230-1262) over the PreScore plugin
    /// objects: a Skip records the plugin in the cycle state's skip-score
    /// set; any other non-success is wrapped as
    /// `running PreScore plugin "<name>": <err>`.
    pub async fn run_pre_score_plugins(
        &self,
        state: &mut CycleState,
        pod: &Pod,
        nodes: &[&Node],
        all_pods: &[Pod],
        all_nodes: &[Node],
        handle: &dyn Handle,
    ) -> Status {
        let mut skip: HashSet<String> = HashSet::new();
        for pl in &self.pre_score_plugin_objects.0 {
            let s = pl
                .pre_score(state, pod, nodes, all_pods, all_nodes, handle)
                .await;
            if s.is_skip() {
                skip.insert(pl.name().to_string());
                continue;
            }
            if !s.is_success() {
                state.set_skip_score_plugins(skip);
                return Status::from_error(format!(
                    "running PreScore plugin {:?}: {}",
                    pl.name(),
                    s.as_error().unwrap_or_default()
                ));
            }
        }
        state.set_skip_score_plugins(skip);
        Status::success()
    }

    /// `RunReservePluginsReserve` (framework.go:1598-1628): stops at the first
    /// plugin that does not succeed; the caller then runs
    /// [`Framework::run_reserve_plugins_unreserve`]. A rejection is returned
    /// as is (tagged with the plugin), any other failure is wrapped as
    /// `running Reserve plugin "<name>": <err>`.
    pub async fn run_reserve_plugins_reserve(
        &self,
        state: &CycleState,
        pod: &Pod,
        node_name: &str,
        handle: &dyn Handle,
    ) -> Status {
        for pl in &self.reserve_plugin_objects.0 {
            let mut s = pl.reserve(state, pod, node_name, handle).await;
            if s.is_success() {
                continue;
            }
            if s.is_rejected() {
                s.set_plugin(pl.name());
                return s;
            }
            return Status::from_error(format!(
                "running Reserve plugin {:?}: {}",
                pl.name(),
                s.as_error().unwrap_or_default()
            ));
        }
        Status::success()
    }

    /// `RunReservePluginsUnreserve` (framework.go:1642-1664): every plugin's
    /// `Unreserve`, in the reverse order of `Reserve`.
    pub async fn run_reserve_plugins_unreserve(
        &self,
        state: &CycleState,
        pod: &Pod,
        node_name: &str,
        handle: &dyn Handle,
    ) {
        for pl in self.reserve_plugin_objects.0.iter().rev() {
            pl.unreserve(state, pod, node_name, handle).await;
        }
    }

    /// `RunPreBindPreFlights` (framework.go:1464-1501): Success when one or
    /// more plugins will work, Skip when all skip, Error otherwise; the
    /// skipping plugins are recorded in the cycle state.
    pub fn run_pre_bind_pre_flights(
        &self,
        state: &mut CycleState,
        pod: &Pod,
        node_name: &str,
    ) -> Status {
        let mut skip: HashSet<String> = HashSet::new();
        let mut returning = Status::new(crate::fwk::Code::Skip, Vec::new());
        for pl in &self.pre_bind_plugin_objects.0 {
            let s = pl.pre_bind_pre_flight(state, pod, node_name);
            if s.code() == crate::fwk::Code::Error {
                return Status::from_error(format!(
                    "running PreBindPreFlight {:?}: {}",
                    pl.name(),
                    s.as_error().unwrap_or_default()
                ));
            } else if s.is_success() {
                returning = Status::success();
            } else if s.is_skip() {
                skip.insert(pl.name().to_string());
            } else {
                return Status::from_error(format!(
                    "PreBindPreFlight {} returned {:?}, which is unsupported. It is supposed to return Success, Skip, or Error status",
                    pl.name(),
                    s.code()
                ));
            }
        }
        state.set_skip_pre_bind_plugins(skip);
        returning
    }

    /// `RunPreBindPlugins` (framework.go:1413-1447): plugins skipped by their
    /// PreBindPreFlight do not run; a rejection is returned as is, any other
    /// failure is wrapped as `running PreBind plugin "<name>": <err>`.
    pub async fn run_pre_bind_plugins(
        &self,
        state: &CycleState,
        pod: &Pod,
        node_name: &str,
        handle: &dyn Handle,
    ) -> Status {
        for pl in &self.pre_bind_plugin_objects.0 {
            if state.skip_pre_bind_plugins().contains(pl.name()) {
                continue;
            }
            let mut s = pl.pre_bind(state, pod, node_name, handle).await;
            if s.is_success() {
                continue;
            }
            if s.is_rejected() {
                s.set_plugin(pl.name());
                return s;
            }
            return Status::from_error(format!(
                "running PreBind plugin {:?}: {}",
                pl.name(),
                s.as_error().unwrap_or_default()
            ));
        }
        Status::success()
    }

    /// `RunScorePlugins` (framework.go:1273-1382) over the Score plugin
    /// objects, up to the weighted sum (which [`crate::score::total_scores`]
    /// does, with the `[0, 100]` check): each plugin not skipped by PreScore
    /// scores every node, then normalises, in place. Errors are
    /// `plugin "<name>" failed with: <err>`.
    pub fn run_score_plugins(
        &self,
        state: &CycleState,
        pod: &Pod,
        nodes: &[&Node],
        all_pods: &[Pod],
        all_nodes: &[Node],
    ) -> Result<Vec<crate::score::PluginScores>, String> {
        let mut out = Vec::new();
        for pl in &self.score_plugin_objects.0 {
            if state.skip_score_plugins().contains(pl.name()) {
                continue;
            }
            let mut scores = Vec::with_capacity(nodes.len());
            for node in nodes {
                let info = NodeInfo::new(node, all_pods).with_all_nodes(all_nodes);
                let (score, status) = pl.score(state, pod, &info);
                if !status.is_success() {
                    return Err(format!(
                        "plugin {:?} failed with: {}",
                        pl.name(),
                        status.as_error().unwrap_or_default()
                    ));
                }
                scores.push(NodeScore {
                    name: node.metadata.name.clone(),
                    score,
                });
            }
            if let Some(status) = pl.normalize_score(state, pod, &mut scores) {
                if !status.is_success() {
                    return Err(format!(
                        "plugin {:?} failed with: {}",
                        pl.name(),
                        status.as_error().unwrap_or_default()
                    ));
                }
            }
            out.push(crate::score::PluginScores {
                name: pl.name(),
                weight: i64::from(self.score_weight(pl.name())),
                scores: scores.into_iter().map(|s| s.score).collect(),
            });
        }
        Ok(out)
    }

    /// `computeBatchablePlugins` (framework.go:757-803): signatures are only
    /// enabled when every PreFilter, Filter, PreScore and Score plugin can
    /// sign. (Rusternetes has no extenders to disable them.)
    fn compute_batchable_plugins(&mut self) {
        self.enable_signatures = true;
        self.batchable_plugins = SignPluginObjects::default();
        let mut seen: HashSet<String> = HashSet::new();
        for ep in [
            ExtensionPoint::PreFilter,
            ExtensionPoint::Filter,
            ExtensionPoint::PreScore,
            ExtensionPoint::Score,
        ] {
            for name in self.plugins_at(ep).to_vec() {
                if !seen.insert(name.clone()) {
                    continue;
                }
                match crate::in_tree::new_sign_plugin(&name) {
                    Some(pl) => self.batchable_plugins.0.push(pl),
                    None => self.enable_signatures = false,
                }
            }
        }
    }

    /// Whether [`Framework::sign_pod`] can produce signatures for this profile.
    pub fn signatures_enabled(&self) -> bool {
        self.enable_signatures
    }

    /// `SignPod` (framework.go:805-854): a signature for the pod. Any two
    /// pods with the same signature get the same feasibility and scoring for
    /// the same set of nodes in the same state. `None` when a plugin cannot
    /// sign the pod (or signatures are disabled), which turns off the
    /// optimizations for this pod.
    pub fn sign_pod(&self, pod: &Pod) -> Option<PodSignature> {
        if !self.enable_signatures {
            return None;
        }
        let mut sig: std::collections::BTreeMap<String, serde_json::Value> = Default::default();
        sig.insert(
            crate::signers::SCHEDULER_NAME_SIGNER_NAME.to_string(),
            serde_json::json!(pod
                .spec
                .as_ref()
                .and_then(|s| s.scheduler_name.as_deref())
                .unwrap_or("")),
        );
        for plugin in &self.batchable_plugins.0 {
            let (fragments, status) = plugin.sign_pod(pod);
            if !status.is_success() {
                if status.code() == crate::fwk::Code::Error {
                    tracing::error!(
                        plugin = plugin.name(),
                        "SignPod failed for plugin: {}",
                        status.message()
                    );
                }
                tracing::trace!(
                    plugin = plugin.name(),
                    "SignPod can't sign pod due to plugin"
                );
                return None;
            }
            for f in fragments {
                sig.insert(f.key, f.value);
            }
        }
        serde_json::to_vec(&sig).ok()
    }

    /// `RunPreFilterPlugins` (framework.go:862-923) over the PreFilter plugin
    /// objects. Returns the merged [`PreFilterResult`] (`None` is all nodes),
    /// the status, and the names of the plugins that restricted the nodes
    /// (`pluginsWithNodes`).
    ///
    /// A plugin returning Skip has its Filter skipped for the cycle
    /// (`SetSkipFilterPlugins`). UnschedulableAndUnresolvable ends the run;
    /// Unschedulable lets the remaining plugins run (kubernetes#119770) and is
    /// returned at the end; any other non-success is wrapped as
    /// `running PreFilter plugin "<name>": <err>`. A result that leaves no
    /// node is UnschedulableAndUnresolvable.
    pub async fn run_pre_filter_plugins(
        &self,
        state: &mut CycleState,
        pod: &Pod,
        nodes: &[Node],
        all_pods: &[Pod],
        handle: &dyn Handle,
    ) -> (Option<PreFilterResult>, Status, HashSet<String>) {
        let mut skip_plugins: HashSet<String> = HashSet::new();
        let out = self
            .pre_filter_plugins_inner(state, pod, nodes, all_pods, handle, &mut skip_plugins)
            .await;
        // Upstream sets this in a defer, so it applies on every return path.
        state.set_skip_filter_plugins(skip_plugins);
        out
    }

    async fn pre_filter_plugins_inner(
        &self,
        state: &mut CycleState,
        pod: &Pod,
        nodes: &[Node],
        all_pods: &[Pod],
        handle: &dyn Handle,
        skip_plugins: &mut HashSet<String>,
    ) -> (Option<PreFilterResult>, Status, HashSet<String>) {
        let mut result: Option<PreFilterResult> = None;
        let mut plugins_with_nodes: HashSet<String> = HashSet::new();
        let mut return_status = Status::success();
        for pl in &self.pre_filter_plugin_objects.0 {
            let (r, mut s) = pl.pre_filter(state, pod, nodes, all_pods, handle).await;
            if s.is_skip() {
                skip_plugins.insert(pl.name().to_string());
                continue;
            }
            if !s.is_success() {
                s.set_plugin(pl.name());
                if s.code() == crate::fwk::Code::UnschedulableAndUnresolvable {
                    // Preemption shouldn't happen in this cycle, so no need
                    // to execute all PreFilter.
                    return (None, s, HashSet::new());
                }
                if s.code() == crate::fwk::Code::Unschedulable {
                    // Preemption happens later in this cycle, so all
                    // PreFilter plugins must run (kubernetes#119770).
                    return_status = s;
                    continue;
                }
                let mut wrapped = Status::from_error(format!(
                    "running PreFilter plugin {:?}: {}",
                    pl.name(),
                    s.as_error().unwrap_or_default()
                ));
                wrapped.set_plugin(pl.name());
                return (None, wrapped, HashSet::new());
            }
            if !PreFilterResult::all_nodes(&r) {
                plugins_with_nodes.insert(pl.name().to_string());
            }
            result = PreFilterResult::merge(&result, &r);
            if !PreFilterResult::all_nodes(&result)
                && result
                    .as_ref()
                    .and_then(|r| r.node_names.as_ref())
                    .is_some_and(HashSet::is_empty)
            {
                let mut names: Vec<&str> = plugins_with_nodes.iter().map(String::as_str).collect();
                names.sort();
                let msg = if names.len() == 1 {
                    format!("node(s) didn't satisfy plugin {}", names[0])
                } else {
                    format!(
                        "node(s) didn't satisfy plugin(s) [{}] simultaneously",
                        names.join(" ")
                    )
                };
                // Nodes filtered out by a PreFilterResult are considered
                // UnschedulableAndUnresolvable.
                return (
                    result,
                    Status::new(crate::fwk::Code::UnschedulableAndUnresolvable, vec![msg]),
                    plugins_with_nodes,
                );
            }
        }
        (result, return_status, plugins_with_nodes)
    }

    /// `RunFilterPlugins` (framework.go:1033-1066) over the Filter plugin
    /// objects: the first plugin that does not succeed ends the run. A status
    /// other than Success/Unschedulable is wrapped as
    /// `running "<plugin>" filter plugin: <err>`, and the plugin name is set
    /// on the returned status.
    pub async fn run_filter_plugins(
        &self,
        state: &CycleState,
        pod: &Pod,
        node_info: &NodeInfo<'_>,
    ) -> Status {
        for pl in &self.filter_plugin_objects.0 {
            if state.skip_filter_plugins().contains(pl.name()) {
                continue;
            }
            let mut status = pl.filter(state, pod, node_info).await;
            if !status.is_success() {
                if !status.is_rejected() {
                    // Filter plugins are not supposed to return any status
                    // other than Success or Unschedulable.
                    status = Status::from_error(format!(
                        "running {:?} filter plugin: {}",
                        pl.name(),
                        status.as_error().unwrap_or_default()
                    ));
                }
                status.set_plugin(pl.name());
                return status;
            }
        }
        Status::success()
    }

    /// `RunFilterPluginsWithNominatedPods` (framework.go:1145-1190): the
    /// filters run twice when the node has nominated pods of greater or equal
    /// priority. First with those pods added to the node (so the pod yields to
    /// them), and, if that passes, again without them (some filters, such as
    /// inter-pod affinity, may not pass without the nominated pods).
    /// `nominated` is `addGENominatedPods`'s selection.
    pub async fn run_filter_plugins_with_nominated_pods(
        &self,
        state: &CycleState,
        pod: &Pod,
        node: &Node,
        all_pods: &[Pod],
        all_nodes: &[Node],
        nominated: Vec<&Pod>,
    ) -> Status {
        let pods_added = !nominated.is_empty();
        let info = NodeInfo::new(node, all_pods)
            .with_all_nodes(all_nodes)
            .with_added_pods(nominated);
        let status = self.run_filter_plugins(state, pod, &info).await;
        if !status.is_success() && !status.is_rejected() {
            return status;
        }
        if !pods_added || !status.is_success() {
            return status;
        }
        self.run_filter_plugins(
            state,
            pod,
            &NodeInfo::new(node, all_pods).with_all_nodes(all_nodes),
        )
        .await
    }

    /// `EnqueueExtensions` (framework.go:736-738): plugin names whose
    /// `EventsToRegister` feed the scheduling queue.
    pub fn enqueue_extension_plugins(&self) -> &[String] {
        &self.enqueue_extensions
    }

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
        percentage_of_nodes_to_score: profile.percentage_of_nodes_to_score,
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

    // fillEnqueueExtensions (framework.go:673-689), for every needed plugin
    // the registry knows (the `for name, factory := range r` loop at :390-397).
    // Go ranges a map; sort for a stable order.
    {
        let mut names: Vec<&str> = plugins_map.keys().copied().collect();
        names.sort_unstable();
        for name in names {
            let pts = plugins_map[name];
            // shouldHaveEnqueueExtensions (framework.go:663-671)
            if [
                ExtensionPoint::PreEnqueue,
                ExtensionPoint::PreFilter,
                ExtensionPoint::Filter,
                ExtensionPoint::Reserve,
                ExtensionPoint::Permit,
            ]
            .iter()
            .any(|ep| pts.contains(ep))
            {
                f.enqueue_extensions.push(name.to_string());
            }
        }
    }

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
    // Instantiate the Filter plugins that are objects, in list order.
    f.filter_plugin_objects = FilterPluginObjects(
        f.plugins_at(ExtensionPoint::Filter)
            .iter()
            .filter_map(|name| crate::in_tree::new_filter_plugin(name, &plugin_args(profile)))
            .collect(),
    );
    // The PreFilter plugins that are objects, in list order.
    f.pre_filter_plugin_objects = PreFilterPluginObjects(
        f.plugins_at(ExtensionPoint::PreFilter)
            .iter()
            .filter_map(|name| crate::in_tree::new_pre_filter_plugin(name, &plugin_args(profile)))
            .collect(),
    );
    f.pre_score_plugin_objects = PreScorePluginObjects(
        f.plugins_at(ExtensionPoint::PreScore)
            .iter()
            .filter_map(|name| crate::in_tree::new_pre_score_plugin(name, &plugin_args(profile)))
            .collect(),
    );
    f.score_plugin_objects = ScorePluginObjects(
        f.plugins_at(ExtensionPoint::Score)
            .iter()
            .filter_map(|name| crate::in_tree::new_score_plugin(name, &plugin_args(profile)))
            .collect(),
    );
    f.reserve_plugin_objects = ReservePluginObjects(
        f.plugins_at(ExtensionPoint::Reserve)
            .iter()
            .filter_map(|name| crate::in_tree::new_reserve_plugin(name))
            .collect(),
    );
    f.pre_bind_plugin_objects = PreBindPluginObjects(
        f.plugins_at(ExtensionPoint::PreBind)
            .iter()
            .filter_map(|name| crate::in_tree::new_pre_bind_plugin(name))
            .collect(),
    );
    f.compute_batchable_plugins();
    Ok(f)
}

/// The plugin factories' arguments from the profile.
fn plugin_args<'a>(profile: &ProfileSpec<'a>) -> crate::in_tree::FilterPluginArgs<'a> {
    crate::in_tree::FilterPluginArgs {
        added_affinity: profile.added_affinity,
        fit_scorer: profile.fit_scorer,
        balanced_scorer: profile.balanced_scorer,
        inter_pod_affinity: profile.inter_pod_affinity,
        pod_topology_spread: profile.pod_topology_spread,
        volume_shape: profile.volume_shape,
    }
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
