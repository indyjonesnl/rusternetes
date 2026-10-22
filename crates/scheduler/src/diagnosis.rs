//! Per-cycle scheduling diagnosis and the `FailedScheduling` message built
//! from it.
//!
//! Ported from upstream `pkg/scheduler/framework/types.go`:
//! `Diagnosis` (PreFilterMsg + per-node statuses) and `FitError.Error()`
//! (`:807-847`), with `NoNodeAvailableMsg = "0/%v nodes are available"`
//! (`:784`). Only the reason histogram is modelled; Rusternetes has no
//! PostFilter message or NodeToStatus absent-node default.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefilter_msg_only() {
        let d = Diagnosis {
            pre_filter_msg: "persistentvolumeclaim \"x\" not found".into(),
            ..Default::default()
        };
        assert_eq!(
            fit_error_message(3, &d),
            "0/3 nodes are available: persistentvolumeclaim \"x\" not found."
        );
    }

    #[test]
    fn filter_reasons_histogram_sorted() {
        let mut d = Diagnosis::default();
        d.add_node_reasons(["node(s) didn't match PersistentVolume's node affinity".to_string()]);
        d.add_node_reasons(["node(s) didn't match PersistentVolume's node affinity".to_string()]);
        d.add_node_reasons(["node(s) had untolerated taint".to_string()]);
        assert_eq!(
            fit_error_message(3, &d),
            "0/3 nodes are available: 1 node(s) had untolerated taint, 2 node(s) didn't match PersistentVolume's node affinity."
        );
    }

    #[test]
    fn prefilter_suppresses_node_reasons() {
        let d = Diagnosis {
            pre_filter_msg: "boom".into(),
            node_reasons: vec!["dup".into()],
            ..Default::default()
        };
        assert_eq!(fit_error_message(2, &d), "0/2 nodes are available: boom.");
    }

    #[test]
    fn empty_diagnosis_is_bare() {
        assert_eq!(
            fit_error_message(2, &Diagnosis::default()),
            "0/2 nodes are available."
        );
    }
}
