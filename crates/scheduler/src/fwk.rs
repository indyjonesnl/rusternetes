//! The scheduling framework interface, shaped after
//! `staging/src/k8s.io/kube-scheduler/framework` (`fwk`), release-1.35:
//! [`Code`] / [`Status`] (`interface.go:60-236`), [`CycleState`]
//! (`cycle_state.go`), [`NodeInfo`] (`types.go`) and the plugin traits.
//!
//! This is the interface plugin objects are dispatched through
//! (`runtime/framework.go`). The older `framework` module holds legacy traits
//! that do not follow upstream's shapes and are not used by the scheduling
//! cycle.
//!
//! Deviations: [`NodeInfo`] is a view over the cycle's pod list rather than an
//! incrementally maintained snapshot, and [`CycleState`] has no
//! skip-score/pre-score sets or metrics flag yet; they arrive with the
//! plugins that need them. [`CycleState`] is written through `&mut` (only
//! PreFilter and later write; the per-node Filter pass reads shared), where
//! upstream's is internally locked.

use crate::node_volume_limits::NodeVolumeLimitsSnapshot;
use crate::volume_binding::VolumeSnapshot;
use async_trait::async_trait;
use rusternetes_common::resources::{Node, Pod};
use std::any::Any;
use std::collections::{HashMap, HashSet};

/// `fwk.Code` (`interface.go:60-95`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Code {
    /// Plugin ran correctly and found the pod schedulable.
    Success,
    /// An internal plugin error or an unexpected input.
    Error,
    /// The pod cannot be scheduled by the plugin (e.g. a node fails a Filter).
    Unschedulable,
    /// Like `Unschedulable`, but preemption would not help.
    UnschedulableAndUnresolvable,
    /// A Permit plugin wants the pod to wait.
    Wait,
    /// The plugin has nothing to do for this pod at this extension point.
    Skip,
    /// The plugin needs an event before it can decide.
    Pending,
}

impl Code {
    /// `Code.String()` (`interface.go:97-103`).
    pub fn as_str(self) -> &'static str {
        match self {
            Code::Success => "Success",
            Code::Error => "Error",
            Code::Unschedulable => "Unschedulable",
            Code::UnschedulableAndUnresolvable => "UnschedulableAndUnresolvable",
            Code::Wait => "Wait",
            Code::Skip => "Skip",
            Code::Pending => "Pending",
        }
    }
}

/// `fwk.Status` (`interface.go:105-236`). Go's nil `*Status` is Success;
/// here that is [`Status::success`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    code: Code,
    reasons: Vec<String>,
    err: Option<String>,
    plugin: String,
}

impl Status {
    /// A nil `*Status`.
    pub fn success() -> Self {
        Self::new(Code::Success, Vec::new())
    }

    /// `NewStatus(code, reasons...)`.
    pub fn new(code: Code, reasons: Vec<String>) -> Self {
        Self {
            code,
            reasons,
            err: None,
            plugin: String::new(),
        }
    }

    /// `AsStatus(err)`: an `Error` status carrying `err`.
    pub fn from_error(err: impl Into<String>) -> Self {
        Self {
            code: Code::Error,
            reasons: Vec::new(),
            err: Some(err.into()),
            plugin: String::new(),
        }
    }

    pub fn code(&self) -> Code {
        self.code
    }

    /// `Reasons()`: the error text first, then the reasons.
    pub fn reasons(&self) -> Vec<String> {
        match &self.err {
            Some(e) => std::iter::once(e.clone())
                .chain(self.reasons.iter().cloned())
                .collect(),
            None => self.reasons.clone(),
        }
    }

    /// `Message()`: the reasons joined with ", ".
    pub fn message(&self) -> String {
        self.reasons().join(", ")
    }

    pub fn plugin(&self) -> &str {
        &self.plugin
    }

    /// `SetPlugin`.
    pub fn set_plugin(&mut self, plugin: &str) {
        self.plugin = plugin.to_string();
    }

    pub fn is_success(&self) -> bool {
        self.code == Code::Success
    }

    pub fn is_wait(&self) -> bool {
        self.code == Code::Wait
    }

    pub fn is_skip(&self) -> bool {
        self.code == Code::Skip
    }

    /// `IsRejected`: Unschedulable, UnschedulableAndUnresolvable or Pending.
    pub fn is_rejected(&self) -> bool {
        matches!(
            self.code,
            Code::Unschedulable | Code::UnschedulableAndUnresolvable | Code::Pending
        )
    }

    /// `AsError`: `None` for Success/Wait/Skip, otherwise the error text.
    pub fn as_error(&self) -> Option<String> {
        if self.is_success() || self.is_wait() || self.is_skip() {
            return None;
        }
        Some(self.err.clone().unwrap_or_else(|| self.message()))
    }
}

/// `fwk.CycleState`: per-scheduling-cycle plugin data (`cycle_state.go`).
#[derive(Default)]
pub struct CycleState {
    storage: HashMap<String, Box<dyn Any + Send + Sync>>,
    skip_filter_plugins: HashSet<String>,
}

impl CycleState {
    pub fn new() -> Self {
        Self::default()
    }

    /// `Write`.
    pub fn write<T: Any + Send + Sync>(&mut self, key: &str, value: T) {
        self.storage.insert(key.to_string(), Box::new(value));
    }

    /// `Read`: `None` is upstream's `ErrNotFound`.
    pub fn read<T: Any + Send + Sync>(&self, key: &str) -> Option<&T> {
        self.storage.get(key).and_then(|v| v.downcast_ref::<T>())
    }

    /// `SetSkipFilterPlugins`: plugins whose Filter is skipped this cycle
    /// (their PreFilter returned Skip).
    pub fn set_skip_filter_plugins(&mut self, plugins: HashSet<String>) {
        self.skip_filter_plugins = plugins;
    }

    /// `GetSkipFilterPlugins`.
    pub fn skip_filter_plugins(&self) -> &HashSet<String> {
        &self.skip_filter_plugins
    }
}

/// `fwk.PreFilterResult` (`interface.go:332-360`): the nodes that should be
/// considered downstream. Go's nil `*PreFilterResult` and a nil `NodeNames`
/// both mean "all nodes"; here an `Option<PreFilterResult>` of `None` or a
/// `node_names` of `None` do.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreFilterResult {
    pub node_names: Option<HashSet<String>>,
}

impl PreFilterResult {
    /// `AllNodes`, for a possibly-nil result.
    pub fn all_nodes(this: &Option<PreFilterResult>) -> bool {
        this.as_ref().is_none_or(|r| r.node_names.is_none())
    }

    /// `Merge`, for possibly-nil results: nil only if both are all-nodes,
    /// otherwise the intersection of the restricted sets.
    pub fn merge(
        this: &Option<PreFilterResult>,
        other: &Option<PreFilterResult>,
    ) -> Option<PreFilterResult> {
        if Self::all_nodes(this) && Self::all_nodes(other) {
            return None;
        }
        let names = |r: &Option<PreFilterResult>| r.as_ref().and_then(|r| r.node_names.clone());
        let node_names = match (names(this), names(other)) {
            (Some(a), Some(b)) => a.intersection(&b).cloned().collect(),
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (None, None) => unreachable!("both all-nodes handled above"),
        };
        Some(PreFilterResult {
            node_names: Some(node_names),
        })
    }
}

/// `fwk.NodeInfo` for the Filter extension point: the node and the pods bound
/// to it, plus any pods "added" to it for a what-if pass (`nodeInfo.AddPod`,
/// used for nominated pods by `addGENominatedPods`, framework.go).
pub struct NodeInfo<'a> {
    node: &'a Node,
    all_pods: &'a [Pod],
    all_nodes: &'a [Node],
    added: Vec<&'a Pod>,
}

impl<'a> NodeInfo<'a> {
    pub fn new(node: &'a Node, all_pods: &'a [Pod]) -> Self {
        Self {
            node,
            all_pods,
            all_nodes: &[],
            added: Vec::new(),
        }
    }

    /// The cycle's node snapshot, for plugins that look across nodes
    /// (`SnapshotSharedLister().NodeInfos()` upstream).
    pub fn with_all_nodes(mut self, nodes: &'a [Node]) -> Self {
        self.all_nodes = nodes;
        self
    }

    /// Every node of the cycle's snapshot (empty unless set with
    /// [`NodeInfo::with_all_nodes`]).
    pub fn all_nodes(&self) -> &'a [Node] {
        self.all_nodes
    }

    /// Every pod of the cycle's snapshot, bound to any node.
    pub fn all_pods(&self) -> &'a [Pod] {
        self.all_pods
    }

    /// A node info with `pods` added, as `nodeInfo.AddPod` for each.
    pub fn with_added_pods(mut self, pods: Vec<&'a Pod>) -> Self {
        self.added = pods;
        self
    }

    /// `Node()`.
    pub fn node(&self) -> &'a Node {
        self.node
    }

    /// `GetPods()`: the pods bound to this node, then the added ones.
    pub fn pods(&self) -> impl Iterator<Item = &'a Pod> + '_ {
        let name = self.node.metadata.name.as_str();
        self.all_pods
            .iter()
            .filter(move |p| p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(name))
            .chain(self.added.iter().copied())
    }
}

/// `fwk.Plugin`.
pub trait Plugin: Send + Sync {
    /// Name used in the plugin registry and configurations.
    fn name(&self) -> &'static str;
}

/// `fwk.FilterPlugin`: a node that fails is not a candidate for the pod.
#[async_trait]
pub trait FilterPlugin: Plugin {
    async fn filter(&self, state: &CycleState, pod: &Pod, node_info: &NodeInfo<'_>) -> Status;
}

/// `fwk.PreFilterPlugin`: runs once per scheduling cycle before the per-node
/// Filter pass; it may precompute state into the [`CycleState`], return
/// `Skip` (its Filter is then not run), or narrow the candidate nodes.
#[async_trait]
pub trait PreFilterPlugin: Plugin {
    async fn pre_filter(
        &self,
        state: &mut CycleState,
        pod: &Pod,
        nodes: &[Node],
        handle: &dyn Handle,
    ) -> (Option<PreFilterResult>, Status);
}

/// The slice of `framework.Handle` the plugin objects use: access to cluster
/// objects (upstream's informer-backed listers). Upstream hands a plugin its
/// `Handle` at construction; here the scheduler's data plane is generic over
/// its storage and per-scheduler rather than per-profile, so it is passed to
/// each PreFilter call instead. Every method defaults to "no cluster access".
#[async_trait]
pub trait Handle: Send + Sync {
    /// The cluster-wide listers NodeVolumeLimits reads (`csi.go` `pvLister`,
    /// `pvcLister`, `scLister`, `vaLister`, `csiDriverLister`, CSINode lister).
    async fn node_volume_limits_snapshot(&self) -> Result<NodeVolumeLimitsSnapshot, String> {
        Err("no cluster access".to_string())
    }

    /// The listers VolumeBinding reads (PVC, PV, StorageClass, CSIDriver,
    /// CSIStorageCapacity, CSINode) for namespace `ns`; it also feeds the
    /// binder's assume caches, as the informers do upstream.
    async fn volume_binding_snapshot(&self, _ns: &str) -> Result<VolumeSnapshot, String> {
        Err("no cluster access".to_string())
    }
}

/// A [`Handle`] with no cluster access, for plugins that need none.
pub struct NoHandle;

impl Handle for NoHandle {}
