//! Unit tests moved from `src/registry/core/pod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::registry::generic::{Store, UpdateOptions};
    pub use rusternetes_api_server::registry::rest::{
        reset_object_meta_for_status, DefaultUpdatedObjectInfo, GroupResource,
        NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
        RestGracefulDeleteStrategy, RestUpdateStrategy, TransformFunc, UpdatedObjectInfo,
    };
    pub use rusternetes_api_server::ssa::ResetFields;
    pub use rusternetes_common::equality::semantic_equal;
    pub use rusternetes_common::podutil::update_pod_condition;
    pub use rusternetes_common::resources::pod::PodSpec;
    pub use rusternetes_common::resources::pod::{
        AppArmorProfile, PodAffinityTerm, SecurityContext,
    };
    pub use rusternetes_common::resources::{Binding, Container, Pod, PodCondition, PodStatus};
    pub use rusternetes_common::types::Phase;
    pub use rusternetes_common::types::{LabelSelector, LabelSelectorRequirement};
    pub use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{get_warnings_for_ip, is_dns1123_label};
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta,
    };
    pub use rusternetes_common::Status;
    pub use rusternetes_storage::StorageBackend;
    pub use std::collections::{HashMap, VecDeque};
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::core::pod::*;
    use rusternetes_common::deletion::DeleteOptions;

    fn pod(extra: serde_json::Value) -> Pod {
        let mut body = serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "busybox"}]}
        });
        for (k, v) in extra.as_object().unwrap() {
            body["spec"][k] = v.clone();
        }
        serde_json::from_value(body).unwrap()
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    fn grace(pod: &Pod, requested: Option<i64>) -> Option<i64> {
        let mut options = DeleteOptions {
            grace_period_seconds: requested,
            ..Default::default()
        };
        assert!(Strategy.check_graceful_delete(&ctx(), pod, &mut options));
        options.grace_period_seconds
    }

    /// `dropNonResizeUpdates` (strategy.go:376-380): pod-level resources of
    /// the new pod survive only with InPlacePodLevelResourcesVerticalScaling.
    #[test]
    #[serial_test::serial]
    fn drop_non_resize_updates_keeps_pod_level_resources_only_with_the_gate() {
        let old = pod(serde_json::json!({"resources": {"requests": {"cpu": "1"}}}));
        let new = pod(serde_json::json!({"resources": {"requests": {"cpu": "2"}}}));
        let cpu = |p: &Pod| {
            p.spec
                .as_ref()
                .unwrap()
                .resources
                .as_ref()
                .unwrap()
                .requests
                .as_ref()
                .unwrap()["cpu"]
                .clone()
        };
        let mut n = new.clone();
        drop_non_resize_updates(&mut n, &old);
        assert_eq!(cpu(&n), "1");
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::InPlacePodLevelResourcesVerticalScaling,
            true,
        );
        let mut n = new.clone();
        drop_non_resize_updates(&mut n, &old);
        assert_eq!(cpu(&n), "2");
    }

    fn annotated(ann: serde_json::Value) -> Pod {
        let mut p = with_meta(serde_json::json!({}), serde_json::json!({}), ann);
        p.metadata.resource_version = Some("1".to_string());
        p
    }

    /// `validatePodMetadataAndSpec` (validation.go:4505) runs
    /// `ValidatePodSpecificAnnotations` on create (#2351).
    #[test]
    fn create_validates_pod_specific_annotations() {
        let key = "controller.kubernetes.io/pod-deletion-cost";
        let errs = Strategy.validate(&ctx(), &annotated(serde_json::json!({ key: "+10" })));
        assert!(
            errs.iter()
                .any(|e| e.error_body().contains("must be a 32bit integer")),
            "{errs:?}"
        );
        let errs = Strategy.validate(
            &ctx(),
            &annotated(serde_json::json!({ "kubernetes.io/config.mirror": "h" })),
        );
        assert!(
            errs.iter()
                .any(|e| e.error_body().contains("must set spec.nodeName")),
            "{errs:?}"
        );
        assert!(Strategy
            .validate(&ctx(), &annotated(serde_json::json!({ key: "10" })))
            .is_empty());
    }

    /// Update: an invalid deletion cost is rejected only when the old one was
    /// valid (util.go:488-492); mirror/AppArmor annotation updates are forbidden
    /// (`ValidatePodSpecificAnnotationUpdates`, validation.go:5699).
    #[test]
    fn update_validates_pod_specific_annotations() {
        let key = "controller.kubernetes.io/pod-deletion-cost";
        let good = annotated(serde_json::json!({ key: "5" }));
        let bad = annotated(serde_json::json!({ key: "+5" }));
        let errs = Strategy.validate_update(&ctx(), &bad, &good);
        assert!(
            errs.iter()
                .any(|e| e.error_body().contains("must be a 32bit integer")),
            "{errs:?}"
        );
        // already-invalid old object: leniency
        let e = Strategy.validate_update(&ctx(), &bad, &bad);
        assert!(e.is_empty(), "{e:?}");
        let plain = annotated(serde_json::json!({}));
        let apparmor = annotated(
            serde_json::json!({ "container.apparmor.security.beta.kubernetes.io/c": "unconfined" }),
        );
        let errs = Strategy.validate_update(&ctx(), &apparmor, &plain);
        assert!(
            errs.iter()
                .any(|e| e.error_body().contains("may not add AppArmor annotations")),
            "{errs:?}"
        );
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert!(Strategy.graceful().is_some());
        assert!(!EphemeralContainersStrategy.allow_create_on_update());
        assert!(!ResizeStrategy.allow_create_on_update());
    }

    /// `TestCheckGracefulDelete` (pkg/registry/core/pod/strategy_test.go).
    #[test]
    fn check_graceful_delete_follows_upstream() {
        // unscheduled: immediately
        assert_eq!(grace(&pod(serde_json::json!({})), Some(30)), Some(0));
        // scheduled: the request wins, else the pod's own period, else 0
        let scheduled =
            pod(serde_json::json!({"nodeName": "n", "terminationGracePeriodSeconds": 45}));
        assert_eq!(grace(&scheduled, Some(10)), Some(10));
        assert_eq!(grace(&scheduled, None), Some(45));
        assert_eq!(
            grace(&pod(serde_json::json!({"nodeName": "n"})), None),
            Some(0)
        );
        // negative: 1
        assert_eq!(grace(&scheduled, Some(-5)), Some(1));
        // already terminated: immediately
        for phase in ["Failed", "Succeeded"] {
            let mut done = scheduled.clone();
            done.status = Some(PodStatus {
                phase: Some(serde_json::from_value(serde_json::json!(phase)).unwrap()),
                ..Default::default()
            });
            assert_eq!(grace(&done, Some(30)), Some(0), "{phase}");
        }
    }

    #[test]
    fn create_sets_pending_status_generation_and_qos() {
        let mut p = pod(serde_json::json!({}));
        p.metadata.generation = Some(9);
        p.status = Some(PodStatus {
            phase: Some(Phase::Running),
            pod_ip: Some("1.2.3.4".into()),
            ..Default::default()
        });
        Strategy.prepare_for_create(&ctx(), &mut p);
        let status = p.status.as_ref().unwrap();
        assert_eq!(status.phase, Some(Phase::Pending));
        assert_eq!(status.qos_class.as_deref(), Some("BestEffort"));
        assert!(status.pod_ip.is_none());
        assert_eq!(p.metadata.generation, Some(1));
    }

    #[test]
    fn a_pod_name_that_is_not_a_dns_label_warns() {
        let mut p = pod(serde_json::json!({}));
        p.metadata.name = "a.b".into();
        assert_eq!(Strategy.warnings_on_create(&ctx(), &p).len(), 1);
        p.metadata.name = "ab".into();
        assert!(Strategy.warnings_on_create(&ctx(), &p).is_empty());
    }

    /// `podStatusStrategy.WarningsOnUpdate` (strategy.go:273-283): an IP that
    /// is valid but not in canonical form draws a warning naming its path.
    #[test]
    fn status_update_warns_about_non_canonical_ips() {
        let old = pod(serde_json::json!({}));
        let mut new = old.clone();
        new.status = Some(PodStatus {
            pod_i_ps: Some(vec![rusternetes_common::resources::pod::PodIP {
                ip: "010.0.0.1".into(),
            }]),
            host_i_ps: Some(vec![rusternetes_common::resources::pod::HostIP {
                ip: "10.0.0.1".into(),
            }]),
            ..Default::default()
        });
        let warnings = StatusStrategy.warnings_on_update(&ctx(), &new, &old);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].starts_with("status.podIPs[0].ip:"),
            "{warnings:?}"
        );
    }

    /// `dropNonResizeUpdates` (strategy.go:386-435): only the containers'
    /// `resources` and `resizePolicy` survive from the new pod.
    #[test]
    fn resize_keeps_only_resources_and_resize_policy() {
        let old = pod(serde_json::json!({}));
        let mut new = pod(serde_json::json!({
            "containers": [{"name": "c", "image": "other",
                "resources": {"requests": {"cpu": "1"}}}],
            "activeDeadlineSeconds": 5
        }));
        ResizeStrategy.prepare_for_update(&ctx(), &mut new, &old);
        let spec = new.spec.as_ref().unwrap();
        assert_eq!(spec.containers[0].image, "busybox");
        assert!(spec.active_deadline_seconds.is_none());
        assert!(spec.containers[0].resources.is_some());
    }

    // ---- #2162: mutatePodAffinity / mutateTopologySpreadConstraints /
    // applyAppArmorVersionSkew, ported from strategy_test.go
    // (Test_mutateTopologySpreadConstraints:1804, Test_mutatePodAffinity:1442,
    // TestApplyAppArmorVersionSkew:2318).

    fn created(mut p: Pod, ctx: &RequestContext) -> Pod {
        Strategy.prepare_for_create(ctx, &mut p);
        p
    }

    fn with_meta(
        extra: serde_json::Value,
        labels: serde_json::Value,
        ann: serde_json::Value,
    ) -> Pod {
        let mut p = pod(extra);
        p.metadata.labels = serde_json::from_value(labels).unwrap();
        p.metadata.annotations = serde_json::from_value(ann).unwrap();
        p
    }

    fn exprs(
        sel: &Option<rusternetes_common::types::LabelSelector>,
    ) -> Vec<(String, String, Vec<String>)> {
        sel.as_ref()
            .unwrap()
            .match_expressions
            .iter()
            .flatten()
            .map(|r| (r.key.clone(), r.operator.clone(), r.values.clone().unwrap()))
            .collect()
    }

    fn tsc_pod(constraint: serde_json::Value) -> Pod {
        with_meta(
            serde_json::json!({"topologySpreadConstraints": [constraint]}),
            serde_json::json!({"country": "Japan", "city": "Tokyo"}),
            serde_json::json!(null),
        )
    }

    fn tsc(p: Pod) -> rusternetes_common::resources::pod::TopologySpreadConstraint {
        p.spec
            .unwrap()
            .topology_spread_constraints
            .unwrap()
            .remove(0)
    }

    #[test]
    #[serial_test::serial]
    fn match_label_keys_merged_into_topology_spread_selector_with_in() {
        let p = created(
            tsc_pod(
                serde_json::json!({"maxSkew": 1, "topologyKey": "kubernetes.io/hostname",
                "whenUnsatisfiable": "DoNotSchedule", "labelSelector": {},
                "matchLabelKeys": ["country", "city"]}),
            ),
            &ctx(),
        );
        let c = tsc(p);
        assert_eq!(
            exprs(&c.label_selector),
            vec![
                ("country".into(), "In".into(), vec!["Japan".into()]),
                ("city".into(), "In".into(), vec!["Tokyo".into()]),
            ]
        );
        assert_eq!(c.match_label_keys.unwrap(), vec!["country", "city"]);
    }

    #[test]
    #[serial_test::serial]
    fn topology_spread_keys_not_in_pod_labels_are_ignored() {
        let p = created(
            tsc_pod(
                serde_json::json!({"maxSkew": 1, "topologyKey": "kubernetes.io/hostname",
                "whenUnsatisfiable": "DoNotSchedule", "labelSelector": {},
                "matchLabelKeys": ["country", "not-found"]}),
            ),
            &ctx(),
        );
        assert_eq!(
            exprs(&tsc(p).label_selector),
            vec![("country".into(), "In".into(), vec!["Japan".into()])]
        );
    }

    #[test]
    #[serial_test::serial]
    fn topology_spread_keys_ignored_when_selector_is_nil() {
        let p = created(
            tsc_pod(
                serde_json::json!({"maxSkew": 1, "topologyKey": "kubernetes.io/hostname",
                "whenUnsatisfiable": "DoNotSchedule", "matchLabelKeys": ["country"]}),
            ),
            &ctx(),
        );
        assert!(tsc(p).label_selector.is_none());
    }

    #[test]
    #[serial_test::serial]
    fn topology_spread_keys_not_merged_without_selector_merge_gate() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let _g = with_feature(
            Feature::MatchLabelKeysInPodTopologySpreadSelectorMerge,
            false,
        );
        let p = created(
            tsc_pod(
                serde_json::json!({"maxSkew": 1, "topologyKey": "kubernetes.io/hostname",
                "whenUnsatisfiable": "DoNotSchedule", "labelSelector": {},
                "matchLabelKeys": ["country", "city"]}),
            ),
            &ctx(),
        );
        assert!(exprs(&tsc(p).label_selector).is_empty());
    }

    #[test]
    #[serial_test::serial]
    fn pod_affinity_label_keys_merge_into_all_four_term_lists() {
        let term = serde_json::json!({"topologyKey": "zone", "labelSelector": {},
            "matchLabelKeys": ["country", "absent"], "mismatchLabelKeys": ["city"]});
        let nil = serde_json::json!({"topologyKey": "zone", "matchLabelKeys": ["country"]});
        let weighted =
            |t: &serde_json::Value| serde_json::json!({"weight": 1, "podAffinityTerm": t});
        let p = created(
            with_meta(
                serde_json::json!({"affinity": {
                    "podAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": [term, nil],
                        "preferredDuringSchedulingIgnoredDuringExecution": [weighted(&term)]},
                    "podAntiAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": [term],
                        "preferredDuringSchedulingIgnoredDuringExecution": [weighted(&term)]}}}),
                serde_json::json!({"country": "Japan", "city": "Tokyo"}),
                serde_json::json!(null),
            ),
            &ctx(),
        );
        let want = vec![
            (
                "country".to_string(),
                "In".to_string(),
                vec!["Japan".to_string()],
            ),
            (
                "city".to_string(),
                "NotIn".to_string(),
                vec!["Tokyo".to_string()],
            ),
        ];
        let a = p.spec.unwrap().affinity.unwrap();
        let (pa, anti) = (a.pod_affinity.unwrap(), a.pod_anti_affinity.unwrap());
        let req = pa
            .required_during_scheduling_ignored_during_execution
            .unwrap();
        assert_eq!(exprs(&req[0].label_selector), want);
        assert!(req[1].label_selector.is_none(), "nil selector stays nil");
        let pref = pa
            .preferred_during_scheduling_ignored_during_execution
            .unwrap();
        assert_eq!(exprs(&pref[0].pod_affinity_term.label_selector), want);
        let req = anti
            .required_during_scheduling_ignored_during_execution
            .unwrap();
        assert_eq!(exprs(&req[0].label_selector), want);
        let pref = anti
            .preferred_during_scheduling_ignored_during_execution
            .unwrap();
        assert_eq!(exprs(&pref[0].pod_affinity_term.label_selector), want);
    }

    const AA: &str = "container.apparmor.security.beta.kubernetes.io/";

    fn aa_pod(spec: serde_json::Value, ann: serde_json::Value) -> Pod {
        with_meta(spec, serde_json::json!(null), ann)
    }

    fn aa_type(c: &rusternetes_common::resources::Container) -> Option<(String, Option<String>)> {
        c.security_context
            .as_ref()?
            .app_armor_profile
            .as_ref()
            .map(|p| (p.type_.clone(), p.localhost_profile.clone()))
    }

    const WARN: &str = r#"deprecated since v1.30; use the "appArmorProfile" field instead"#;

    #[test]
    fn apparmor_annotation_unconfined_syncs_to_field_with_warning() {
        let c = ctx();
        let key = format!("{AA}ctr");
        let p = created(
            aa_pod(
                serde_json::json!({"containers": [{"name": "ctr", "image": "i"}]}),
                serde_json::json!({ key.clone(): "unconfined" }),
            ),
            &c,
        );
        let s = p.spec.unwrap();
        assert_eq!(aa_type(&s.containers[0]), Some(("Unconfined".into(), None)));
        assert!(s.security_context.is_none());
        let w = c.warnings();
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(
            w[0].starts_with(&format!("metadata.annotations[{key}]: ")) && w[0].contains(WARN),
            "{w:?}"
        );
    }

    #[test]
    fn apparmor_annotations_on_init_and_localhost_containers() {
        let c = ctx();
        let p = created(
            aa_pod(
                serde_json::json!({
                    "securityContext": {"appArmorProfile": {"type": "RuntimeDefault"}},
                    "initContainers": [{"name": "init", "image": "i"}],
                    "containers": [{"name": "a", "image": "i"}, {"name": "b", "image": "i"},
                                   {"name": "c", "image": "i"}]}),
                serde_json::json!({
                    format!("{AA}init"): "unconfined",
                    format!("{AA}a"): "localhost/test",
                    format!("{AA}c"): "runtime/default"}),
            ),
            &c,
        );
        let s = p.spec.unwrap();
        assert_eq!(
            aa_type(&s.init_containers.unwrap()[0]),
            Some(("Unconfined".into(), None))
        );
        assert_eq!(
            aa_type(&s.containers[0]),
            Some(("Localhost".into(), Some("test".into())))
        );
        assert!(s.containers[1].security_context.is_none());
        // Equal to the pod-level profile: not copied, and no warning for it.
        assert!(s.containers[2].security_context.is_none());
        assert!(!c.warnings().is_empty());
    }

    #[test]
    fn apparmor_annotation_loses_to_existing_container_field() {
        let c = ctx();
        let p = created(
            aa_pod(
                serde_json::json!({"containers": [{"name": "ctr", "image": "i",
                    "securityContext": {"appArmorProfile": {"type": "RuntimeDefault"}}}]}),
                serde_json::json!({format!("{AA}ctr"): "localhost/test"}),
            ),
            &c,
        );
        assert_eq!(
            aa_type(&p.spec.unwrap().containers[0]),
            Some(("RuntimeDefault".into(), None))
        );
        assert!(c.warnings().is_empty());
    }

    #[test]
    fn apparmor_annotation_matching_pod_field_is_not_synced_nor_warned() {
        let c = ctx();
        let p = created(
            aa_pod(
                serde_json::json!({
                    "securityContext": {"appArmorProfile": {"type": "RuntimeDefault"}},
                    "containers": [{"name": "ctr", "image": "i"}]}),
                serde_json::json!({format!("{AA}ctr"): "runtime/default"}),
            ),
            &c,
        );
        assert!(p.spec.unwrap().containers[0].security_context.is_none());
        assert!(c.warnings().is_empty());
    }

    #[test]
    fn apparmor_annotation_overrides_pod_field() {
        let c = ctx();
        let p = created(
            aa_pod(
                serde_json::json!({
                    "securityContext": {"appArmorProfile": {"type": "RuntimeDefault"}},
                    "containers": [{"name": "ctr", "image": "i"}]}),
                serde_json::json!({format!("{AA}ctr"): "unconfined"}),
            ),
            &c,
        );
        assert_eq!(
            aa_type(&p.spec.unwrap().containers[0]),
            Some(("Unconfined".into(), None))
        );
        assert_eq!(c.warnings().len(), 1);
    }

    #[test]
    fn apparmor_annotation_for_unknown_container_is_ignored() {
        let c = ctx();
        let p = created(
            aa_pod(
                serde_json::json!({"containers": [{"name": "ctr", "image": "i"}]}),
                serde_json::json!({format!("{AA}foo-bar"): "unconfined"}),
            ),
            &c,
        );
        assert!(p.spec.unwrap().containers[0].security_context.is_none());
        assert!(c.warnings().is_empty());
    }

    #[test]
    fn apparmor_invalid_annotation_warns_and_copies_nothing() {
        let c = ctx();
        let p = created(
            aa_pod(
                serde_json::json!({"containers": [{"name": "ctr", "image": "i"}]}),
                serde_json::json!({format!("{AA}ctr"): format!("localhost/{}", "a".repeat(4096))}),
            ),
            &c,
        );
        assert!(p.spec.unwrap().containers[0].security_context.is_none());
        assert_eq!(c.warnings().len(), 1);
    }

    #[test]
    fn apparmor_annotations_ignored_on_windows() {
        let c = ctx();
        let p = created(
            aa_pod(
                serde_json::json!({"os": {"name": "windows"},
                    "containers": [{"name": "ctr", "image": "i"}]}),
                serde_json::json!({format!("{AA}ctr"): "runtime/default"}),
            ),
            &c,
        );
        assert!(p.spec.unwrap().containers[0].security_context.is_none());
        assert!(c.warnings().is_empty());
    }
}
