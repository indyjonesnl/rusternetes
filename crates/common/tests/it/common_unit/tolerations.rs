//! Unit tests moved from `src/tolerations.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{PodSpec, Toleration};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::Container;
    use rusternetes_common::tolerations::*;

    fn spec_with_container() -> PodSpec {
        PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "busybox".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn tol(
        key: Option<&str>,
        effect: Option<&str>,
        op: Option<&str>,
        secs: Option<i64>,
    ) -> Toleration {
        Toleration {
            key: key.map(str::to_string),
            operator: op.map(str::to_string),
            value: None,
            effect: effect.map(str::to_string),
            toleration_seconds: secs,
        }
    }

    #[test]
    fn appends_both_defaults_when_none_present() {
        let mut spec = spec_with_container();
        assert_eq!(add_default_tolerations(&mut spec), 2);
        let tols = spec.tolerations.as_ref().unwrap();
        assert_eq!(tols.len(), 2);
        let not_ready = tols
            .iter()
            .find(|t| t.key.as_deref() == Some(TAINT_NODE_NOT_READY))
            .unwrap();
        assert_eq!(not_ready.operator.as_deref(), Some("Exists"));
        assert_eq!(not_ready.effect.as_deref(), Some("NoExecute"));
        assert_eq!(not_ready.toleration_seconds, Some(300));
        assert!(tols
            .iter()
            .any(|t| t.key.as_deref() == Some(TAINT_NODE_UNREACHABLE)));
    }

    #[test]
    fn skips_default_when_pod_already_tolerates_not_ready() {
        let mut spec = spec_with_container();
        // Pod already tolerates not-ready:NoExecute with its own (forever) toleration.
        spec.tolerations = Some(vec![tol(
            Some(TAINT_NODE_NOT_READY),
            Some("NoExecute"),
            Some("Exists"),
            None,
        )]);
        // Only the unreachable default should be appended.
        assert_eq!(add_default_tolerations(&mut spec), 1);
        let tols = spec.tolerations.as_ref().unwrap();
        assert_eq!(tols.len(), 2);
        // The pre-existing not-ready toleration keeps its tolerationSeconds:None.
        let not_ready = tols
            .iter()
            .find(|t| t.key.as_deref() == Some(TAINT_NODE_NOT_READY))
            .unwrap();
        assert_eq!(
            not_ready.toleration_seconds, None,
            "existing toleration untouched"
        );
    }

    #[test]
    fn empty_key_exists_toleration_covers_both() {
        let mut spec = spec_with_container();
        // Empty-key NoExecute toleration matches all taint keys (upstream rule 3).
        spec.tolerations = Some(vec![tol(None, Some("NoExecute"), Some("Exists"), None)]);
        assert_eq!(add_default_tolerations(&mut spec), 0);
        assert_eq!(spec.tolerations.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn empty_effect_toleration_covers_matching_key() {
        let mut spec = spec_with_container();
        // Empty effect matches all effects → covers NoExecute for not-ready.
        spec.tolerations = Some(vec![tol(
            Some(TAINT_NODE_NOT_READY),
            None,
            Some("Exists"),
            None,
        )]);
        // unreachable default still appended.
        assert_eq!(add_default_tolerations(&mut spec), 1);
    }

    #[test]
    fn noschedule_toleration_does_not_cover_noexecute() {
        let mut spec = spec_with_container();
        // A NoSchedule toleration for not-ready does NOT cover the NoExecute taint.
        spec.tolerations = Some(vec![tol(
            Some(TAINT_NODE_NOT_READY),
            Some("NoSchedule"),
            Some("Exists"),
            None,
        )]);
        assert_eq!(add_default_tolerations(&mut spec), 2);
    }

    #[test]
    fn idempotent_second_call_adds_nothing() {
        let mut spec = spec_with_container();
        add_default_tolerations(&mut spec);
        assert_eq!(add_default_tolerations(&mut spec), 0);
        assert_eq!(spec.tolerations.as_ref().unwrap().len(), 2);
    }
}
