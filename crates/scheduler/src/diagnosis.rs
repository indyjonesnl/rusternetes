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
}

impl Diagnosis {
    pub fn add_node_reasons<I: IntoIterator<Item = String>>(&mut self, reasons: I) {
        self.node_reasons.extend(reasons);
    }

    pub fn is_empty(&self) -> bool {
        self.pre_filter_msg.is_empty() && self.node_reasons.is_empty()
    }
}

/// Port of `FitError.Error()`.
pub fn fit_error_message(num_all_nodes: usize, d: &Diagnosis) -> String {
    let _ = (num_all_nodes, d);
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefilter_msg_only() {
        let d = Diagnosis {
            pre_filter_msg: "persistentvolumeclaim \"x\" not found".into(),
            node_reasons: vec![],
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
