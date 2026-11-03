//! Unit tests moved from `src/diagnosis.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::diagnosis::*;

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

    #[test]
    fn node_codes_default_to_the_absent_nodes_status() {
        use rusternetes_scheduler::fwk::Code;
        // `NewDefaultNodeToStatus` (interface.go:42-44).
        let mut d = Diagnosis::default();
        assert_eq!(d.node_code("n"), Code::UnschedulableAndUnresolvable);
        d.set_node_code("n", Code::Unschedulable);
        assert_eq!(d.node_code("n"), Code::Unschedulable);
        assert_eq!(d.node_code("m"), Code::UnschedulableAndUnresolvable);
        // `SetAbsentNodesStatus` after a PreFilter rejection.
        d.absent_nodes_code = Some(Code::Unschedulable);
        assert_eq!(d.node_code("m"), Code::Unschedulable);
    }

    /// `Diagnosis.AddPluginStatus` (types.go:787-801): a rejection is an
    /// unschedulable plugin, a Pending status is both unschedulable (it is
    /// rejected) and pending; no plugin name, or a non-rejection, records
    /// nothing.
    #[test]
    fn add_plugin_status_records_pending_plugins() {
        use rusternetes_scheduler::fwk::{Code, Status};
        let status = |code: Code, plugin: &str| {
            let mut s = Status::new(code, vec!["why".to_string()]);
            s.set_plugin(plugin);
            s
        };
        let mut d = Diagnosis::default();
        d.add_plugin_status(&status(Code::Unschedulable, "A"));
        d.add_plugin_status(&status(Code::UnschedulableAndUnresolvable, "B"));
        d.add_plugin_status(&status(Code::Pending, "C"));
        d.add_plugin_status(&status(Code::Pending, ""));
        d.add_plugin_status(&status(Code::Success, "D"));
        d.add_plugin_status(&status(Code::Error, "E"));
        let set = |v: &[&str]| -> std::collections::HashSet<String> {
            v.iter().map(|s| s.to_string()).collect()
        };
        assert_eq!(d.unschedulable_plugins, set(&["A", "B", "C"]));
        assert_eq!(d.pending_plugins, set(&["C"]));
    }
}
