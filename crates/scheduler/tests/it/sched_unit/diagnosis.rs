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
}
