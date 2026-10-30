//! Unit tests moved from `src/volume_plugins/util/types.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::fmt;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::util::types::*;

    /// `types.go:179-187`: uncertain progress and transient failures are not
    /// finished; everything else, including a plain error, is.
    #[test]
    fn operation_finished_error_classification() {
        let uncertain: anyhow::Error = VolumeOperationError::UncertainProgress("u".into()).into();
        let transient: anyhow::Error =
            VolumeOperationError::TransientOperationFailure("t".into()).into();
        let infeasible: anyhow::Error = VolumeOperationError::Infeasible("i".into()).into();
        assert!(!is_operation_finished_error(&uncertain));
        assert!(!is_operation_finished_error(&transient));
        assert!(is_operation_finished_error(&infeasible));
        assert!(is_operation_finished_error(&anyhow::anyhow!("plain")));
        assert!(is_uncertain_progress_error(&uncertain));
        assert!(!is_uncertain_progress_error(&transient));
        assert_eq!(uncertain.to_string(), "u");
    }
}
