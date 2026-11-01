//! Per-cycle scheduling diagnosis and the `FailedScheduling` message built
//! from it.
//!
//! Ported from upstream `pkg/scheduler/framework/types.go`:
//! `Diagnosis` (PreFilterMsg + per-node statuses) and `FitError.Error()`
//! (`:807-847`), with `NoNodeAvailableMsg = "0/%v nodes are available"`
//! (`:784`), and `NodeToStatus` (`interface.go:35-110`) reduced to the status
//! codes preemption reads. The FailedScheduling message uses the reason
//! histogram only.

#[derive(Debug, Default, Clone)]
pub struct Diagnosis {
    /// Message from a PreFilter plugin that returned unschedulable.
    pub pre_filter_msg: String,
    /// Filter rejection reasons, one entry per (node, reason).
    pub node_reasons: Vec<String>,
    /// `Diagnosis.UnschedulablePlugins` (`types.go:765-766`): plugins that
    /// returned Unschedulable or UnschedulableAndUnresolvable. Copied onto
    /// the `QueuedPodInfo` in `handleSchedulingFailure` so the scheduling
    /// queue only re-evaluates the pod on events those plugins registered.
    pub unschedulable_plugins: std::collections::HashSet<String>,
    /// A plugin returned `Error` (not a rejection): `findNodesThatPassFilters`
    /// (schedule_one.go:664-668) cancels the search and `schedulePod` returns
    /// this error instead of a `FitError`, so the cycle is a scheduling
    /// *error* (no PostFilter, error backoff), not "unschedulable".
    pub scheduling_error: Option<String>,
    /// `Diagnosis.NodeToStatus` (`types.go:762`): the code of each node's
    /// rejection, by node name.
    pub node_codes: std::collections::HashMap<String, crate::fwk::Code>,
    /// `NodeToStatus.absentNodesStatus` (interface.go:42-45, :73-85): the code
    /// of the nodes without an entry. `None` is the default,
    /// UnschedulableAndUnresolvable; a PreFilter rejection sets it to its own
    /// code so every node is handled alike in preemption.
    pub absent_nodes_code: Option<crate::fwk::Code>,
}

impl Diagnosis {
    pub fn add_node_reasons<I: IntoIterator<Item = String>>(&mut self, reasons: I) {
        self.node_reasons.extend(reasons);
    }

    /// `Diagnosis.AddPluginStatus` (`types.go:787-801`) for a rejected
    /// (Unschedulable / UnschedulableAndUnresolvable) status from `plugin`.
    /// Rusternetes has no Pending status, so `PendingPlugins` is not modelled.
    pub fn add_plugin_status(&mut self, plugin: &str) {
        if plugin.is_empty() {
            return;
        }
        self.unschedulable_plugins.insert(plugin.to_string());
    }

    /// `NodeToStatus.Set`.
    pub fn set_node_code(&mut self, node: &str, code: crate::fwk::Code) {
        self.node_codes.insert(node.to_string(), code);
    }

    /// `NodeToStatus.Get(..).Code()`.
    pub fn node_code(&self, node: &str) -> crate::fwk::Code {
        self.node_codes.get(node).copied().unwrap_or_else(|| {
            self.absent_nodes_code
                .unwrap_or(crate::fwk::Code::UnschedulableAndUnresolvable)
        })
    }

    pub fn is_empty(&self) -> bool {
        self.pre_filter_msg.is_empty() && self.node_reasons.is_empty()
    }
}

/// Port of `FitError.Error()`.
pub fn fit_error_message(num_all_nodes: usize, d: &Diagnosis) -> String {
    let mut msg = format!("0/{} nodes are available:", num_all_nodes);
    if !d.pre_filter_msg.is_empty() {
        // PreFilter returned unschedulable; the framework stamps the same
        // status on every node, so node reasons would only duplicate it.
        msg.push_str(&format!(" {}.", d.pre_filter_msg));
        return msg;
    }
    let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for r in &d.node_reasons {
        *counts.entry(r.as_str()).or_insert(0) += 1;
    }
    let mut parts: Vec<String> = counts.iter().map(|(k, v)| format!("{} {}", v, k)).collect();
    parts.sort();
    if parts.is_empty() {
        // upstream: no histogram -> message ends after the colon-less prefix
        return format!("0/{} nodes are available.", num_all_nodes);
    }
    msg.push_str(&format!(" {}.", parts.join(", ")));
    msg
}
