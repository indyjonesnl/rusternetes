//! Unit tests moved from `src/registry/apiextensions/customresourcedefinition.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::registry::apiextensions::controllers::{
        self, condition, crd_has_finalizer, crd_remove_finalizer, set_crd_condition, ESTABLISHED,
        TERMINATING,
    };
    pub use rusternetes_api_server::registry::apiextensions::finalizer_queue;
    pub use rusternetes_api_server::registry::generic::{
        CreateOptions, Deleted, Store, UpdateOptions,
    };
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestStorage, RestUpdateStrategy, UpdatedObjectInfo, ValidateObject,
        ValidateObjectUpdate,
    };
    pub use rusternetes_api_server::ssa::ResetFields;
    pub use rusternetes_common::deletion::{DeleteOptions, Preconditions};
    pub use rusternetes_common::resources::{
        CustomResourceDefinition, JSONSchemaProps, JSONSchemaPropsOrArray, JSONSchemaPropsOrBool,
        JSONSchemaPropsOrStringArray,
    };
    pub use rusternetes_common::validation::crd::{
        is_crd_condition_true, validate_custom_resource_definition,
        validate_custom_resource_definition_update,
        validate_update_custom_resource_definition_status, CUSTOM_RESOURCE_CLEANUP_FINALIZER,
    };
    pub use rusternetes_common::validation::field::{ErrorList, Path};
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::{build_prefix, Storage, StorageBackend};
    pub use std::sync::Arc;
    pub use tracing::warn;
}

mod tests {
    /// The hook polls until the informer reports synced (apiserver.go:263-271).
    #[tokio::test(start_paused = true)]
    async fn crd_informer_synced_polls_until_synced() {
        let calls = std::sync::atomic::AtomicUsize::new(0);
        rusternetes_api_server::registry::apiextensions::customresourcedefinition::wait_for_crd_informer_synced(|| {
            let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move { n >= 3 }
        })
        .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 4);
    }
    use super::__parent::*;
    use rusternetes_api_server::registry::apiextensions::customresourcedefinition::*;

    /// The drain wait polls at once, then every interval, until a list is
    /// empty (crd_finalizer.go:253-267).
    #[tokio::test(start_paused = true)]
    async fn poll_until_gone_polls_until_the_list_is_empty() {
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let r = poll_until_gone(INSTANCE_DRAIN_INTERVAL, INSTANCE_DRAIN_TIMEOUT, || {
            let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move { Ok(3_usize.saturating_sub(n)) }
        })
        .await;
        assert_eq!(r, Ok(()));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 4);
    }

    /// Instances that never go away end the wait at the timeout
    /// (`PollUntilContextTimeout` -> `context deadline exceeded`).
    #[tokio::test(start_paused = true)]
    async fn poll_until_gone_times_out_while_instances_remain() {
        let r = poll_until_gone(INSTANCE_DRAIN_INTERVAL, INSTANCE_DRAIN_TIMEOUT, || async {
            Ok(1)
        })
        .await;
        assert_eq!(r, Err("context deadline exceeded".to_string()));
    }

    /// A list error ends the wait at once (crd_finalizer.go:255-257).
    #[tokio::test(start_paused = true)]
    async fn poll_until_gone_stops_on_a_list_error() {
        let r = poll_until_gone(INSTANCE_DRAIN_INTERVAL, INSTANCE_DRAIN_TIMEOUT, || async {
            Err(Error::Storage("boom".into()))
        })
        .await;
        assert!(r.unwrap_err().contains("boom"));
    }

    /// `go finalizingController.Run(5, ...)` (apiserver.go:249): five workers
    /// drain the queue at once, so one slow CRD does not hold up the rest.
    #[tokio::test]
    async fn crd_finalizer_runs_five_workers_at_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        assert_eq!(CRD_FINALIZER_WORKERS, 5);
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let names: Vec<String> = (0..12).map(|i| format!("crd{i}")).collect();
        let (r, p) = (running.clone(), peak.clone());
        let out = run_workers(names, CRD_FINALIZER_WORKERS, move |_| {
            let (r, p) = (r.clone(), p.clone());
            async move {
                let now = r.fetch_add(1, Ordering::SeqCst) + 1;
                p.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                r.fetch_sub(1, Ordering::SeqCst);
            }
        })
        .await;
        assert_eq!(out.len(), 12);
        assert_eq!(peak.load(Ordering::SeqCst), 5);
    }

    fn named(name: &str) -> CustomResourceDefinition {
        let mut c = crd(serde_json::json!({}));
        c.metadata.name = name.to_string();
        c
    }

    /// The informer's store: first sight is an add, then updates carry the
    /// cached `oldObj`; a relist deletes what is no longer listed
    /// (delta_fifo.go `Replace`).
    #[test]
    fn informer_cache_yields_add_update_and_relist_delete() {
        let mut cache = std::collections::HashMap::new();
        assert!(matches!(
            apply_crd(&mut cache, named("a")),
            CrdEvent::Add(_)
        ));
        assert!(matches!(
            apply_crd(&mut cache, named("a")),
            CrdEvent::Update(_, _)
        ));
        apply_crd(&mut cache, named("b"));
        let events = relist_events(&mut cache, vec![named("b"), named("c")]);
        let kinds: Vec<(&str, String)> = events
            .iter()
            .map(|e| match e {
                CrdEvent::Add(c) => ("add", c.metadata.name.clone()),
                CrdEvent::Update(_, c) => ("update", c.metadata.name.clone()),
                CrdEvent::Delete(c) => ("delete", c.metadata.name.clone()),
            })
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("delete", "a".to_string()),
                ("update", "b".to_string()),
                ("add", "c".to_string())
            ]
        );
    }

    /// A CRD written straight to storage (not through the request path) is
    /// still established: the post-start hook's informer delivers the event
    /// to workers the hook started (apiserver.go:221-252).
    #[tokio::test]
    async fn hook_started_informer_establishes_a_crd_written_to_storage() {
        let storage = Arc::new(StorageBackend::new_memory());
        let controllers: Arc<
            rusternetes_api_server::registry::apiextensions::controller_queue::CrdControllers,
        > = Default::default();
        let _h = start_controllers_loop(storage.clone(), controllers.clone());
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let c = named("widgets.example.com");
        storage
            .create(
                "/registry/customresourcedefinitions/widgets.example.com",
                &c,
            )
            .await
            .unwrap();
        let rest = CrdRest::with_controllers(storage.clone(), controllers);
        for _ in 0..100 {
            if let Some(c) = rest.get_crd("widgets.example.com").await.unwrap() {
                if rusternetes_api_server::registry::apiextensions::controllers::find_crd_condition(
                    &c,
                    "Established",
                )
                .is_some_and(|x| x.status == "True")
                {
                    return;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("CRD written to storage was never established");
    }

    fn crd(extra: serde_json::Value) -> CustomResourceDefinition {
        let mut body = serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "widgets.example.com", "generation": 4},
            "spec": {
                "group": "example.com",
                "scope": "Namespaced",
                "names": {"plural": "widgets", "kind": "Widget"},
                "versions": [{"name": "v1", "served": true, "storage": true}]
            },
            "status": {"storedVersions": ["v0"]}
        });
        for (k, v) in extra.as_object().unwrap() {
            body[k] = v.clone();
        }
        let mut crd: CustomResourceDefinition = serde_json::from_value(body).unwrap();
        convert_to_internal(&mut crd);
        crd
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    fn crd_with_rule(rule: &str) -> CustomResourceDefinition {
        crd(serde_json::json!({"spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "widgets", "kind": "Widget"},
            "versions": [{"name": "v1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object",
                    "x-kubernetes-validations": [{"rule": rule}]}}}]
        }}))
    }

    /// `ValidateCustomResourceDefinitionUpdate` passes
    /// `findPreexistingExpressions(&oldObj.Spec)` (validation.go:241), so a
    /// rule the old CRD already stored compiles in `StoredExpressions`
    /// (validation.go:158-163) even below the library's introduced version.
    #[test]
    fn update_compiles_preexisting_rules_in_the_stored_environment() {
        use rusternetes_common::cel_versions::EnvSet;
        use rusternetes_common::feature_gates::Version;
        let rule = "semver('1.2.3').major() > 0";
        let env_set = EnvSet::must_base_env_set(Version::major_minor(1, 32));
        let new = crd_with_rule(rule);
        // Create (no old object): a new expression, semver is undeclared.
        assert!(!schema_rule_errors_in(&new, None, &env_set).is_empty());
        // Update of a CRD already carrying the rule: stored environment.
        let old = crd_with_rule(rule);
        assert!(schema_rule_errors_in(&new, Some(&old), &env_set).is_empty());
        // A rule not in the old CRD is still new.
        let other = crd_with_rule("semver('1.2.3').minor() > 0");
        assert!(!schema_rule_errors_in(&other, Some(&old), &env_set).is_empty());
    }

    fn crd_with_schema(schema: serde_json::Value) -> CustomResourceDefinition {
        crd(serde_json::json!({"spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "widgets", "kind": "Widget"},
            "versions": [{"name": "v1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": schema}}]
        }}))
    }

    const SEMVER_RULE: &str = "semver('1.2.3').major() > 0";
    const P: &str = "spec.versions[0].schema.openAPIV3Schema";

    fn env_1_32() -> rusternetes_common::cel_versions::EnvSet {
        rusternetes_common::cel_versions::EnvSet::must_base_env_set(
            rusternetes_common::feature_gates::Version::major_minor(1, 32),
        )
    }

    fn fields(errs: &ErrorList) -> Vec<(String, String)> {
        let mut v: Vec<_> = errs
            .iter()
            .map(|e| (format!("{:?}", e.error_type), e.field.clone()))
            .collect();
        v.sort();
        v
    }

    /// `ValidateCustomResourceDefinitionOpenAPISchema` recurses into
    /// `additionalProperties` (validation.go:1065), `properties` (:1084) and
    /// `items` (:1128, :1132) with a non-nil `celContext`, so a rule below any
    /// of them is compiled and reported at its own path (:1292).
    #[test]
    fn rules_under_items_and_additional_properties_are_compiled() {
        let rules = serde_json::json!([{"rule": SEMVER_RULE}]);
        let c = crd_with_schema(serde_json::json!({"type": "object", "properties": {
            "array": {"type": "array", "items": {"type": "string",
                "x-kubernetes-validations": rules}},
            "map": {"type": "object", "additionalProperties": {"type": "string",
                "x-kubernetes-validations": rules}},
            "tuple": {"type": "array", "items": [{"type": "string"},
                {"type": "string", "x-kubernetes-validations": rules}]}
        }}));
        assert_eq!(
            fields(&schema_rule_errors_in(&c, None, &env_1_32())),
            vec![
                (
                    "Invalid".to_string(),
                    format!("{P}.properties[array].items.x-kubernetes-validations[0].rule")
                ),
                (
                    "Invalid".to_string(),
                    format!(
                        "{P}.properties[map].additionalProperties.x-kubernetes-validations[0].rule"
                    )
                ),
                (
                    "Invalid".to_string(),
                    format!("{P}.properties[tuple].items[1].x-kubernetes-validations[0].rule")
                ),
            ]
        );
    }

    /// validation.go:1309-1312: `optionalOldSelf` without `oldSelf` in the
    /// rule is `Invalid` at `x-kubernetes-validations[i].optionalOldSelf`.
    #[test]
    fn optional_old_self_requires_old_self_in_rule() {
        let c = crd_with_schema(serde_json::json!({"type": "object",
            "x-kubernetes-validations": [
                {"rule": "self.size() >= 0", "optionalOldSelf": true},
                {"rule": "self.size() >= 0", "optionalOldSelf": false},
                {"rule": "oldSelf.hasValue() || self.size() >= 0", "optionalOldSelf": true},
                {"rule": "self.size() >= 0"}]}));
        let errs = schema_rule_errors_in(&c, None, &env_1_32());
        assert_eq!(
            fields(&errs),
            vec![
                (
                    "Invalid".to_string(),
                    format!("{P}.x-kubernetes-validations[0].optionalOldSelf")
                ),
                (
                    "Invalid".to_string(),
                    format!("{P}.x-kubernetes-validations[1].optionalOldSelf")
                ),
            ]
        );
        assert!(errs.iter().all(|e| e
            .detail
            .contains("may not be set if oldSelf is not used in rule")));
    }

    /// `allOf`/`anyOf`/`oneOf`/`not`/`definitions`/`dependencies` recurse with
    /// a nil `celContext` (validation.go:1088-1141): their rules are NOT
    /// compiled upstream either.
    #[test]
    fn rules_under_combinators_are_not_compiled_as_upstream() {
        let rules = serde_json::json!([{"rule": SEMVER_RULE}]);
        let c = crd_with_schema(serde_json::json!({"type": "object", "properties": {
            "f": {"type": "string", "allOf": [{"x-kubernetes-validations": rules}],
                "not": {"x-kubernetes-validations": rules}}}}));
        assert!(schema_rule_errors_in(&c, None, &env_1_32()).is_empty());
    }

    fn crd_with_msg(rule: &str, msg_expr: &str) -> CustomResourceDefinition {
        let v = serde_json::json!([{"rule": rule, "messageExpression": msg_expr}]);
        crd_with_schema(serde_json::json!({"type": "object", "properties": {
            "x": {"type": "string", "x-kubernetes-validations": v},
            "obj": {"type": "object", "x-kubernetes-validations": v,
                "properties": {"a": {"type": "string", "x-kubernetes-validations": v}}},
            "array": {"type": "array", "x-kubernetes-validations": v,
                "items": {"type": "string", "x-kubernetes-validations": v}},
            "map": {"type": "object", "x-kubernetes-validations": v,
                "additionalProperties": {"type": "string", "x-kubernetes-validations": v}}
        }}))
    }

    /// `TestValidateCustomResourceDefinitionValidationRuleCompatibility`
    /// (validation_test.go:8168-8290): `preexistingExpressions.
    /// MessageExpressionEnv` (validation.go:165-169) compiles an unchanged
    /// messageExpression in the stored environment and a changed one in the
    /// new-expressions environment.
    #[test]
    fn message_expression_compiles_in_stored_env_only_if_unchanged() {
        let me = "'v: %s'.format([semver('1.2.3').major()])";
        let rule = "true";
        let old = crd_with_msg(rule, me);
        assert!(schema_rule_errors_in(&crd_with_msg(rule, me), Some(&old), &env_1_32()).is_empty());
        assert!(!schema_rule_errors_in(&crd_with_msg(rule, me), None, &env_1_32()).is_empty());
        let changed = "'w: %s'.format([semver('1.2.3').major()])";
        let errs = schema_rule_errors_in(&crd_with_msg(rule, changed), Some(&old), &env_1_32());
        let me_path = |p: &str| {
            (
                "Invalid".to_string(),
                format!("{P}.{p}.x-kubernetes-validations[0].messageExpression"),
            )
        };
        let mut want = vec![
            me_path("properties[x]"),
            me_path("properties[obj]"),
            me_path("properties[obj].properties[a]"),
            me_path("properties[array]"),
            me_path("properties[array].items"),
            me_path("properties[map]"),
            me_path("properties[map].additionalProperties"),
        ];
        want.sort();
        assert_eq!(fields(&errs), want);
    }

    fn one_rule(rule: &str, msg_expr: &str) -> CustomResourceDefinition {
        crd_with_schema(serde_json::json!({"type": "object", "properties": {
            "f": {"type": "string", "x-kubernetes-validations":
                [{"rule": rule, "messageExpression": msg_expr}]}}}))
    }

    /// validation_test.go:10991-11010 (syntax error) and :11125-11144
    /// (whitespace only: validation.go:1243-1245, a schema error).
    #[test]
    fn message_expression_syntax_and_whitespace_errors() {
        let env = env_1_32();
        let errs = schema_rule_errors_in(&one_rule("self == 'a'", "self + \" "), None, &env);
        assert_eq!(
            fields(&errs),
            vec![(
                "Invalid".to_string(),
                format!("{P}.properties[f].x-kubernetes-validations[0].messageExpression")
            )]
        );
        assert!(errs[0]
            .detail
            .starts_with("messageExpression compilation failed: "));
        let errs = schema_rule_errors_in(&one_rule("self == 'a'", "     "), None, &env);
        assert_eq!(
            fields(&errs),
            vec![(
                "Required".to_string(),
                format!("{P}.properties[f].x-kubernetes-validations[0].messageExpression")
            )]
        );
        assert_eq!(
            errs[0].detail,
            "messageExpression must be non-empty if specified"
        );
    }

    /// `compileRule` (compilation.go:236-250) returns before compiling the
    /// messageExpression when the rule itself fails.
    #[test]
    fn rule_error_short_circuits_message_expression() {
        let errs = schema_rule_errors_in(&one_rule(SEMVER_RULE, "bad("), None, &env_1_32());
        assert_eq!(
            fields(&errs),
            vec![(
                "Invalid".to_string(),
                format!("{P}.properties[f].x-kubernetes-validations[0].rule")
            )]
        );
    }

    fn crd_with_props(props: serde_json::Value) -> CustomResourceDefinition {
        crd(serde_json::json!({"spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "widgets", "kind": "Widget"},
            "versions": [{"name": "v1", "served": true, "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object", "properties": props}}}]
        }}))
    }

    fn sorted(mut w: Vec<String>) -> Vec<String> {
        w.sort();
        w
    }

    /// `TestWarningsOnCreate` (strategy_test.go:1428-1584).
    #[test]
    fn warnings_on_create_name_unrecognized_formats() {
        let ok =
            crd_with_props(serde_json::json!({"f": {"type": "string", "format": "date-time"}}));
        assert!(Strategy.warnings_on_create(&ctx(), &ok).is_empty());

        let bad =
            crd_with_props(serde_json::json!({"f": {"type": "string", "format": "invalidformat"}}));
        assert_eq!(
            Strategy.warnings_on_create(&ctx(), &bad),
            vec![r#"unrecognized format "invalidformat""#.to_string()]
        );

        let nested = crd_with_props(serde_json::json!({"nested": {"type": "object",
            "properties": {"e": {"type": "string", "format": "invalidformat"}}}}));
        assert_eq!(
            Strategy.warnings_on_create(&ctx(), &nested),
            vec![r#"unrecognized format "invalidformat""#.to_string()]
        );

        let many = crd_with_props(serde_json::json!({
            "field1": {"type": "string", "format": "unknownformat1"},
            "field2": {"type": "string", "format": "unknownformat2"},
            "nested": {"type": "object",
                "properties": {"field3": {"type": "string", "format": "unknownformat3"}}}}));
        assert_eq!(
            sorted(Strategy.warnings_on_create(&ctx(), &many)),
            vec![
                r#"unrecognized format "unknownformat1""#.to_string(),
                r#"unrecognized format "unknownformat2""#.to_string(),
                r#"unrecognized format "unknownformat3""#.to_string(),
            ]
        );
    }

    /// `GetUnrecognizedFormats` (formats.go:104-119) only judges `type: string`
    /// schemas, and the k8s-short-name/k8s-long-name formats are recognised
    /// at 1.34+ (formats.go:36-52).
    #[test]
    fn warnings_only_judge_string_schemas_and_know_k8s_names() {
        let c = crd_with_props(serde_json::json!({
            "n": {"type": "integer", "format": "whatever"},
            "s": {"type": "string", "format": "k8s-short-name"},
            "l": {"type": "string", "format": "k8s-long-name"},
            "u": {"type": "string", "format": "uuid4"},
            "a": {"type": "array", "items": {"type": "string", "format": "nope"}}}));
        assert_eq!(
            Strategy.warnings_on_create(&ctx(), &c),
            vec![r#"unrecognized format "nope""#.to_string()]
        );
    }

    /// `TestWarningsOnUpdate`: only newly introduced formats warn (ratcheting,
    /// strategy.go:178-194).
    #[test]
    fn warnings_on_update_ratchet() {
        let old = crd_with_props(serde_json::json!({"a": {"type": "string", "format": "oldbad"}}));
        let new = crd_with_props(serde_json::json!({
            "a": {"type": "string", "format": "oldbad"},
            "b": {"type": "string", "format": "newbad"}}));
        assert_eq!(
            Strategy.warnings_on_update(&ctx(), &new, &old),
            vec![r#"unrecognized format "newbad""#.to_string()]
        );
        assert!(Strategy.warnings_on_update(&ctx(), &old, &old).is_empty());
        // statusStrategy.WarningsOnUpdate (strategy.go:292-294) is nil.
        assert!(StatusStrategy
            .warnings_on_update(&ctx(), &new, &old)
            .is_empty());
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(!Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(!Strategy.allow_unconditional_update());
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(!StatusStrategy.allow_unconditional_update());
    }

    /// `PrepareForCreate` (strategy.go:74-89), exercised by
    /// `TestPrepareForCreate`-style inputs: whatever status came in, the
    /// stored versions are the storage version only.
    #[test]
    fn prepare_for_create_resets_status_and_generation() {
        let mut c = crd(serde_json::json!({}));
        Strategy.prepare_for_create(&ctx(), &mut c);
        assert_eq!(c.metadata.generation, Some(1));
        let status = c.status.as_ref().unwrap();
        assert_eq!(status.stored_versions, Some(vec!["v1".to_string()]));
        assert!(status.conditions.is_none() && status.accepted_names.is_none());
    }

    /// `PrepareForUpdate` (strategy.go:91-118): old status wins, a spec
    /// change bumps the generation, a new storage version is appended after
    /// the versions already stored.
    #[test]
    fn prepare_for_update_keeps_status_and_tracks_spec_changes() {
        let old = crd(serde_json::json!({}));
        let mut same = old.clone();
        same.status = None;
        Strategy.prepare_for_update(&ctx(), &mut same, &old);
        assert_eq!(same.metadata.generation, Some(4));
        assert_eq!(
            same.status.as_ref().unwrap().stored_versions,
            Some(vec!["v0".to_string(), "v1".to_string()])
        );

        let mut changed = old.clone();
        changed.spec.names.short_names = Some(vec!["wd".into()]);
        Strategy.prepare_for_update(&ctx(), &mut changed, &old);
        assert_eq!(changed.metadata.generation, Some(5));
    }

    #[test]
    fn status_update_keeps_the_spec_but_not_the_metadata() {
        let old = crd(serde_json::json!({}));
        let mut new = old.clone();
        new.spec.names.short_names = Some(vec!["wd".into()]);
        new.metadata.finalizers = Some(vec![]);
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec, old.spec);
        assert_eq!(new.metadata.finalizers, Some(vec![]));
    }

    #[test]
    fn start_deletion_adds_the_finalizer_and_condition_once() {
        let mut c = crd(serde_json::json!({}));
        start_deletion(&mut c);
        start_deletion(&mut c);
        assert!(c.metadata.deletion_timestamp.is_some());
        assert_eq!(
            c.metadata.finalizers,
            Some(vec![CUSTOM_RESOURCE_CLEANUP_FINALIZER.to_string()])
        );
        let t = controllers::find_crd_condition(&c, TERMINATING).unwrap();
        assert_eq!(t.reason.as_deref(), Some("InstanceDeletionPending"));
        assert_eq!(c.status.unwrap().conditions.unwrap().len(), 1);
    }

    // `TestItemExponentialFailureRateLimiter` /
    // `TestItemExponentialFailureRateLimiterOverFlow` / `TestMaxOfRateLimiter`
    // / `TestBucketRateLimiter`
    // (client-go/util/workqueue/default_rate_limiters_test.go).
    #[test]
    fn item_exponential_limiter_doubles_per_item_and_forgets() {
        use std::time::Duration;
        let mut l = ItemExponentialFailureRateLimiter::new(
            Duration::from_millis(1),
            Duration::from_secs(1),
        );
        assert_eq!(l.when("one"), Duration::from_millis(1));
        assert_eq!(l.when("one"), Duration::from_millis(2));
        assert_eq!(l.when("one"), Duration::from_millis(4));
        assert_eq!(l.when("one"), Duration::from_millis(8));
        assert_eq!(l.when("one"), Duration::from_millis(16));
        assert_eq!(l.num_requeues("one"), 5);
        assert_eq!(l.when("two"), Duration::from_millis(1));
        assert_eq!(l.when("two"), Duration::from_millis(2));
        assert_eq!(l.num_requeues("two"), 2);
        l.forget("one");
        assert_eq!(l.num_requeues("one"), 0);
        assert_eq!(l.when("one"), Duration::from_millis(1));
    }

    #[test]
    fn item_exponential_limiter_caps_and_does_not_overflow() {
        use std::time::Duration;
        let mut l = ItemExponentialFailureRateLimiter::new(
            Duration::from_millis(1),
            Duration::from_secs(1),
        );
        for _ in 0..5 {
            l.when("one");
        }
        assert_eq!(l.when("one"), Duration::from_millis(32));
        for _ in 0..1000 {
            assert!(l.when("one") <= Duration::from_secs(1));
        }
        assert_eq!(l.when("one"), Duration::from_secs(1));
        let day = Duration::from_secs(60 * 60 * 24);
        let mut big = ItemExponentialFailureRateLimiter::new(day, day * 1000);
        for _ in 0..2 {
            big.when("one");
        }
        for _ in 0..1000 {
            assert!(big.when("one") <= day * 1000);
        }
    }

    #[test]
    fn bucket_limiter_spends_the_burst_then_paces_at_qps() {
        use std::time::Duration;
        let now = tokio::time::Instant::now();
        let mut b = BucketRateLimiter::new(10.0, 100);
        for _ in 0..100 {
            assert_eq!(b.when_at(now), Duration::ZERO);
        }
        // Burst spent: each further reservation queues 1/qps behind the last.
        assert_eq!(b.when_at(now), Duration::from_millis(100));
        assert_eq!(b.when_at(now), Duration::from_millis(200));
        // 200ms later 2 tokens have refilled, paying the debt exactly.
        let later = now + Duration::from_millis(200);
        assert_eq!(b.when_at(later), Duration::from_millis(100));
    }

    #[test]
    fn default_controller_limiter_is_the_max_of_item_and_bucket() {
        use std::time::Duration;
        let now = tokio::time::Instant::now();
        let mut l = ControllerRateLimiter::new();
        // Within the burst the per-item exponential limiter dominates.
        assert_eq!(l.when_at("a", now), Duration::from_millis(5));
        assert_eq!(l.when_at("a", now), Duration::from_millis(10));
        assert_eq!(l.when_at("b", now), Duration::from_millis(5));
        // Exhaust the bucket: the overall limiter now dominates fresh items.
        for i in 0..200 {
            l.when_at(&format!("k{i}"), now);
        }
        assert!(l.when_at("fresh", now) > Duration::from_secs(5));
        l.forget("a");
        assert_eq!(l.num_requeues("a"), 0);
    }

    fn items(v: &[(&str, &str)]) -> Vec<(Option<String>, String)> {
        v.iter()
            .map(|(ns, n)| (Some(ns.to_string()), n.to_string()))
            .collect()
    }

    /// "don't retry deleting the same namespace" (crd_finalizer.go:216-219):
    /// one `DeleteCollection` per namespace among the listed instances.
    #[tokio::test]
    async fn one_delete_collection_per_namespace() {
        let calls = std::sync::Mutex::new(Vec::new());
        let r = issue_delete_collections(
            &items(&[("a", "x"), ("a", "y"), ("b", "z"), ("a", "w")]),
            |ctx| {
                calls.lock().unwrap().push(ctx.namespace.clone());
                async { Ok(()) }
            },
        )
        .await;
        assert_eq!(r, Ok(()));
        assert_eq!(
            *calls.lock().unwrap(),
            vec![Some("a".to_string()), Some("b".to_string())]
        );
    }

    /// Failures do not stop the loop (`continue`, :222-225) and are returned as
    /// `utilerrors.NewAggregate(deleteErrors)` (:227): one prints bare,
    /// several as `[a, b]` with repeated messages once (errors.go:70-96).
    #[tokio::test]
    async fn delete_collection_errors_are_aggregated() {
        let calls = std::sync::Mutex::new(0);
        let r = issue_delete_collections(
            &items(&[("a", "x"), ("b", "y"), ("c", "z"), ("d", "w")]),
            |ctx| {
                *calls.lock().unwrap() += 1;
                let ns = ctx.namespace.clone().unwrap();
                async move {
                    match ns.as_str() {
                        "a" | "d" => Err(Error::Internal("boom".into())),
                        "b" => Err(Error::Internal("bang".into())),
                        _ => Ok(()),
                    }
                }
            },
        )
        .await;
        assert_eq!(*calls.lock().unwrap(), 4);
        assert_eq!(
            r,
            Err(format!(
                "[{}, {}]",
                Error::Internal("boom".into()),
                Error::Internal("bang".into())
            ))
        );
        let one = issue_delete_collections(&items(&[("a", "x")]), |_| async {
            Err(Error::Internal("boom".into()))
        })
        .await;
        assert_eq!(one, Err(Error::Internal("boom".into()).to_string()));
        // Two identical messages collapse to one, printed bare.
        let same = issue_delete_collections(&items(&[("a", "x"), ("b", "y")]), |_| async {
            Err(Error::Internal("boom".into()))
        })
        .await;
        assert_eq!(same, Err(Error::Internal("boom".into()).to_string()));
    }

    fn root_rules(rules: serde_json::Value, props: serde_json::Value) -> CustomResourceDefinition {
        crd_with_schema(serde_json::json!({"type": "object",
            "x-kubernetes-validations": rules, "properties": props}))
    }

    fn errs_at(c: &CustomResourceDefinition) -> Vec<(String, String)> {
        fields(&schema_rule_errors_in(c, None, &env_1_32()))
    }

    fn at(kind: &str, path: &str) -> (String, String) {
        (kind.to_string(), format!("{P}.{path}"))
    }

    /// validation_test.go:9951-9988 ("invalid and required messages") via
    /// validation.go:1236-1241.
    #[test]
    fn schema_message_checks() {
        let c = crd_with_schema(serde_json::json!({"type": "object", "properties": {
            "value": {"type": "integer", "x-kubernetes-validations": [
                {"rule": "self >= 0 &&\nself <= 100"},
                {"rule": "self == 50", "message": "value requirements:\nmust be >= 0\nmust be <= 100 "},
                {"rule": "self == 50", "message": " "},
                {"rule": "self >= 0 &&\nself <= 100", "message": "ok"}]}}}));
        assert_eq!(
            errs_at(&c),
            vec![
                at(
                    "Invalid",
                    "properties[value].x-kubernetes-validations[1].message"
                ),
                at(
                    "Invalid",
                    "properties[value].x-kubernetes-validations[2].message"
                ),
                at(
                    "Required",
                    "properties[value].x-kubernetes-validations[0].message"
                ),
            ]
        );
    }

    /// validation.go:1234-1235: an empty rule is a Required schema error.
    #[test]
    fn schema_empty_rule_is_required() {
        let c = root_rules(
            serde_json::json!([{"rule": "  "}, {"message": "m"}]),
            serde_json::json!({}),
        );
        let errs = schema_rule_errors_in(&c, None, &env_1_32());
        assert_eq!(
            fields(&errs),
            vec![
                at("Required", "x-kubernetes-validations[0].rule"),
                at("Required", "x-kubernetes-validations[1].rule"),
            ]
        );
        assert_eq!(errs[0].detail, "rule is not specified");
    }

    /// validation_test.go:4208-4270: validation.go:1246-1248.
    #[test]
    fn schema_reason_must_be_supported() {
        let c = crd_with_schema(serde_json::json!({"type": "object",
            "x-kubernetes-validations": [{"rule": "self.a > 0", "reason": "InternalError", "fieldPath": ".a"}],
            "properties": {"a": {"type": "number", "x-kubernetes-validations": [
                {"rule": "true", "reason": "FieldValueRequired"},
                {"rule": "true", "reason": "FieldValueInvalid"},
                {"rule": "true", "reason": "FieldValueDuplicate"},
                {"rule": "true", "reason": "FieldValueForbidden"}]}}}));
        let errs = schema_rule_errors_in(&c, None, &env_1_32());
        assert_eq!(
            fields(&errs),
            vec![at("NotSupported", "x-kubernetes-validations[0].reason")]
        );
    }

    fn field_path_crd(paths: &[&str], props: serde_json::Value) -> CustomResourceDefinition {
        let rules: Vec<_> = paths
            .iter()
            .map(|p| serde_json::json!({"rule": "true", "fieldPath": p}))
            .collect();
        root_rules(serde_json::Value::Array(rules), props)
    }

    /// validation_test.go:4271-4358 (`pathValid`, validation.go:1249-1259,
    /// 1324-1331): `ValidFieldPath` with array notation allowed.
    #[test]
    fn schema_field_path_valid_and_invalid() {
        let c = field_path_crd(
            &[
                ".foo['b.c']['c\\a']",
                "['a.c']",
                ".a.c",
                ".list[0]",
                "   ",
                ".",
                "..",
            ],
            serde_json::json!({
                "a.c": {"type": "number"},
                "foo": {"type": "object", "properties": {"b.c": {"type": "object",
                    "properties": {"c\u{7}": {"type": "number"}}}}},
                "list": {"type": "array", "items": {"type": "object",
                    "properties": {"a": {"type": "number"}}}}}),
        );
        assert_eq!(
            errs_at(&c),
            vec![
                at("Invalid", "x-kubernetes-validations[2].fieldPath"),
                at("Invalid", "x-kubernetes-validations[3].fieldPath"),
                at("Invalid", "x-kubernetes-validations[4].fieldPath"),
                at("Invalid", "x-kubernetes-validations[4].fieldPath"),
                at("Invalid", "x-kubernetes-validations[5].fieldPath"),
                at("Invalid", "x-kubernetes-validations[6].fieldPath"),
            ]
        );
    }

    /// validation_test.go:4359-4444.
    #[test]
    fn schema_field_path_against_nested_properties() {
        let c = field_path_crd(
            &[".list[0].b", ".list[0.b", ".list0].b", ".a.c", ".a.b.d"],
            serde_json::json!({
                "a": {"type": "object", "properties": {"b": {"type": "object",
                    "properties": {"c": {"type": "number"}}}}},
                "list": {"type": "array", "items": {"type": "object",
                    "properties": {"a": {"type": "number"}}}}}),
        );
        assert_eq!(errs_at(&c).len(), 5, "{:?}", errs_at(&c));
    }

    /// validation.go:1253-1255 (line breaks in fieldPath).
    #[test]
    fn schema_field_path_line_breaks() {
        let c = field_path_crd(&[".a\n"], serde_json::json!({"a": {"type": "number"}}));
        let errs = schema_rule_errors_in(&c, None, &env_1_32());
        assert!(
            errs.iter()
                .any(|e| e.detail == "must not contain line breaks"),
            "{errs:?}"
        );
    }

    /// Schema errors skip CEL compilation (validation.go:1264-1268): the
    /// semver rule beside a bad reason is not reported.
    #[test]
    fn schema_errors_skip_cel_compile() {
        let c = root_rules(
            serde_json::json!([{"rule": SEMVER_RULE, "reason": "Bogus"}]),
            serde_json::json!({}),
        );
        assert_eq!(
            errs_at(&c),
            vec![at("NotSupported", "x-kubernetes-validations[0].reason")]
        );
    }

    const EXPENSIVE: &str = "self.all(a, self.all(b, self.all(c, true)))";

    fn crd_two_versions(v1_extra: &str, v2_rule: &str) -> CustomResourceDefinition {
        let schema = |rule: &str, extra: &str| {
            serde_json::json!({"openAPIV3Schema": {"type": "array",
                "items": {"type": "string"}, "description": extra,
                "x-kubernetes-validations": [{"rule": rule}]}})
        };
        crd(serde_json::json!({"spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"plural": "widgets", "kind": "Widget"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": schema(EXPENSIVE, v1_extra)},
                {"name": "v2", "served": true, "storage": false, "schema": schema(v2_rule, "")}]
        }}))
    }

    /// validation.go:197-226, 1281: a version whose schema is unchanged skips
    /// the per-expression cost check on update (and only that version).
    #[test]
    fn update_suppresses_cost_for_unchanged_schema_only() {
        let env = env_1_32();
        let old = crd_two_versions("", "true");
        // Create: budget enforced.
        assert!(!schema_rule_errors_in(&old, None, &env).is_empty());
        // Update, nothing changed: suppressed.
        assert!(schema_rule_errors_in(&old, Some(&old), &env).is_empty());
        // v1 schema changed: enforced again, only for v1.
        let changed = crd_two_versions("changed", "true");
        let errs = schema_rule_errors_in(&changed, Some(&old), &env);
        assert_eq!(
            fields(&errs),
            vec![(
                "Forbidden".to_string(),
                "spec.versions[0].schema.openAPIV3Schema.x-kubernetes-validations[0].rule"
                    .to_string()
            )]
        );
        // v2 changed to an expensive rule: enforced for v2 only.
        let changed2 = crd_two_versions("", EXPENSIVE);
        let errs = schema_rule_errors_in(&changed2, Some(&old), &env);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].field.starts_with("spec.versions[1]"), "{errs:?}");
    }
}
