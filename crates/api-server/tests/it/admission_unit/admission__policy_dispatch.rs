//! Unit tests moved from `src/admission/policy_dispatch.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::admission::policy_matching::{
        selector_of, Attributes, Matcher,
    };
    pub use rusternetes_api_server::admission::policy_mutating::VersionedAttributes;
    pub use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
    pub use rusternetes_common::resources::validating_admission_policy::{
        FailurePolicy, MatchResources, ParamKind, ParamRef, ParameterNotFoundAction,
    };
    pub use rusternetes_common::types::{Selector, Status, StatusCause, StatusDetails};
    pub use rusternetes_common::Error;
    pub use serde_json::Value;
    pub use std::collections::HashSet;
    pub use std::sync::Arc;
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::policy_dispatch::*;
    use rusternetes_common::admission::Operation;
    use rusternetes_common::resources::validating_admission_policy::{
        MatchPolicyType, NamedRuleWithOperations, OperationType, RuleWithOperations,
    };
    use rusternetes_common::resources::LabelSelector;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn key(n: &str) -> PolicyKey {
        PolicyKey {
            policy: NamespacedName {
                name: format!("p{n}"),
                ..Default::default()
            },
            binding: NamespacedName {
                name: format!("b{n}"),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    // ---- reinvocationcontext_test.go ------------------------------------

    #[test]
    fn full_reinvocation() {
        let (key1, key2, key3) = (key("1"), key("2"), key("3"));
        let v1 = json!({"data": {"v": "1"}});
        let v2 = json!({"data": {"v": "2"}});
        let mut rc = PolicyReinvokeContext::default();

        // key1 is invoked and it updates the configmap
        rc.set_last_policy_invocation_output(Some(&v1));
        rc.require_reinvoking_previously_invoked_plugins();
        rc.add_reinvocable_policy_to_previously_invoked(key1.clone());
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v2)));

        // key2 is invoked and it updates the configmap
        rc.set_last_policy_invocation_output(Some(&v2));
        rc.require_reinvoking_previously_invoked_plugins();
        rc.add_reinvocable_policy_to_previously_invoked(key2.clone());
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v1)));

        // key3 is invoked but it does not change anything
        rc.add_reinvocable_policy_to_previously_invoked(key3.clone());
        assert!(!rc.is_output_changed_since_last_policy_invocation(Some(&v2)));

        // key1 is reinvoked
        assert!(rc.should_reinvoke(&key1));
        rc.add_reinvocable_policy_to_previously_invoked(key1);
        rc.set_last_policy_invocation_output(Some(&v1));
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v2)));
        rc.require_reinvoking_previously_invoked_plugins();

        // key2 is reinvoked
        assert!(rc.should_reinvoke(&key2));
        rc.add_reinvocable_policy_to_previously_invoked(key2);
        rc.set_last_policy_invocation_output(Some(&v2));
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v1)));
        rc.require_reinvoking_previously_invoked_plugins();

        // key3 is reinvoked, because the reinvocations changed the resource
        assert!(rc.should_reinvoke(&key3));
    }

    #[test]
    fn partial_reinvocation() {
        let (key1, key2, key3) = (key("1"), key("2"), key("3"));
        let v1 = json!({"data": {"v": "1"}});
        let v2 = json!({"data": {"v": "2"}});
        let mut rc = PolicyReinvokeContext::default();

        rc.set_last_policy_invocation_output(Some(&v1));
        rc.require_reinvoking_previously_invoked_plugins();
        rc.add_reinvocable_policy_to_previously_invoked(key1.clone());
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v2)));

        rc.set_last_policy_invocation_output(Some(&v2));
        rc.require_reinvoking_previously_invoked_plugins();
        rc.add_reinvocable_policy_to_previously_invoked(key2.clone());
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v1)));

        rc.add_reinvocable_policy_to_previously_invoked(key3.clone());
        assert!(!rc.is_output_changed_since_last_policy_invocation(Some(&v2)));

        // key1 is reinvoked but does not change anything
        assert!(rc.should_reinvoke(&key1));
        // key2 and key3 are not reinvoked: nothing changed since
        assert!(!rc.should_reinvoke(&key2));
        assert!(!rc.should_reinvoke(&key3));
    }

    #[test]
    fn no_reinvocation() {
        let (key1, key2, key3) = (key("1"), key("2"), key("3"));
        let v1 = json!({"data": {"v": "1"}});
        let mut rc = PolicyReinvokeContext::default();

        for k in [&key1, &key2, &key3] {
            rc.add_reinvocable_policy_to_previously_invoked(k.clone());
            rc.set_last_policy_invocation_output(Some(&v1));
            assert!(!rc.is_output_changed_since_last_policy_invocation(Some(&v1)));
        }
        assert!(!rc.should_reinvoke(&key1));
        assert!(!rc.should_reinvoke(&key2));
        assert!(!rc.should_reinvoke(&key3));
    }

    #[test]
    fn nil_output_is_a_change_from_an_object() {
        // SetLastPolicyInvocationOutput(nil) stores nil (reinvocationcontext.go:54-57)
        let mut rc = PolicyReinvokeContext::default();
        assert!(!rc.is_output_changed_since_last_policy_invocation(None));
        let v = json!({"a": 1});
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v)));
        rc.set_last_policy_invocation_output(Some(&v));
        assert!(rc.is_output_changed_since_last_policy_invocation(None));
        rc.set_last_policy_invocation_output(None);
        assert!(!rc.is_output_changed_since_last_policy_invocation(None));
    }

    #[test]
    fn requiring_reinvocation_drains_the_invoked_set() {
        let k = key("1");
        let mut rc = PolicyReinvokeContext::default();
        rc.add_reinvocable_policy_to_previously_invoked(k.clone());
        rc.require_reinvoking_previously_invoked_plugins();
        assert!(rc.should_reinvoke(&k));
        // a second call has nothing new to add, and keeps what is due
        rc.require_reinvoking_previously_invoked_plugins();
        assert!(rc.should_reinvoke(&k));
        assert!(rc.previously_invoked_reinvocable_policies.is_empty());
    }

    // ---- CollectParams --------------------------------------------------

    #[derive(Default)]
    struct Store {
        /// (namespace, name) -> object
        objects: Vec<(Option<String>, Value)>,
        synced: bool,
        get_error: Option<fn() -> Error>,
    }
    #[async_trait]
    impl ParamStore for Store {
        async fn get(&self, ns: Option<&str>, name: &str) -> Result<Value, Error> {
            if let Some(f) = self.get_error {
                return Err(f());
            }
            self.objects
                .iter()
                .find(|(n, o)| n.as_deref() == ns && o["metadata"]["name"] == name)
                .map(|(_, o)| o.clone())
                .ok_or_else(|| Error::NotFound(format!("{name} not found")))
        }
        async fn list(&self, ns: Option<&str>, selector: &Selector) -> Result<Vec<Value>, Error> {
            Ok(self
                .objects
                .iter()
                .filter(|(n, _)| ns.is_none() || n.as_deref() == ns)
                .filter(|(_, o)| {
                    let labels: HashMap<String, String> = o["metadata"]["labels"]
                        .as_object()
                        .map(|m| {
                            m.iter()
                                .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
                                .collect()
                        })
                        .unwrap_or_default();
                    selector.matches(Some(&labels))
                })
                .map(|(_, o)| o.clone())
                .collect())
        }
        fn has_synced(&self) -> bool {
            self.synced
        }
    }

    fn cm(name: &str, labels: Value) -> Value {
        json!({"metadata": {"name": name, "labels": labels}})
    }
    fn kind() -> ParamKind {
        ParamKind {
            api_version: Some("v1".into()),
            kind: "ConfigMap".into(),
        }
    }
    fn pref(name: Option<&str>) -> ParamRef {
        ParamRef {
            name: name.map(str::to_string),
            namespace: None,
            selector: None,
            parameter_not_found_action: None,
        }
    }
    fn synced(objects: Vec<(Option<String>, Value)>) -> Store {
        Store {
            objects,
            synced: true,
            get_error: None,
        }
    }

    #[tokio::test]
    async fn params_without_a_param_kind_or_ref_are_a_single_nil() {
        let r = pref(Some("x"));
        // paramKind unset: any paramRef is ignored
        assert_eq!(
            collect_params(None, None, ParamScope::Root, Some(&r), "ns").await,
            Ok(vec![None])
        );
        // paramKind set, binding has no paramRef
        let s = synced(vec![]);
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, None, "ns").await,
            Ok(vec![None])
        );
    }

    #[tokio::test]
    async fn params_by_name_namespaced_default_to_the_request_namespace() {
        let obj = cm("p", json!({}));
        let s = synced(vec![(Some("ns1".into()), obj.clone())]);
        let r = pref(Some("p"));
        assert_eq!(
            collect_params(
                Some(&kind()),
                Some(&s),
                ParamScope::Namespace,
                Some(&r),
                "ns1"
            )
            .await,
            Ok(vec![Some(obj.clone())])
        );
        // paramRef.namespace wins over the request's
        let mut r2 = pref(Some("p"));
        r2.namespace = Some("ns1".into());
        assert_eq!(
            collect_params(
                Some(&kind()),
                Some(&s),
                ParamScope::Namespace,
                Some(&r2),
                "other"
            )
            .await,
            Ok(vec![Some(obj)])
        );
    }

    #[tokio::test]
    async fn namespaced_param_ref_cannot_serve_a_cluster_scoped_request() {
        let s = synced(vec![]);
        let r = pref(Some("p"));
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Namespace, Some(&r), "").await,
            Err("cannot use namespaced paramRef in policy binding that matches cluster-scoped resources".into())
        );
    }

    #[tokio::test]
    async fn namespace_on_a_cluster_scoped_param_kind_is_rejected() {
        let s = synced(vec![]);
        let mut r = pref(Some("p"));
        r.namespace = Some("ns".into());
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "ns").await,
            Err("paramRef.namespace must not be provided for a cluster-scoped `paramKind`".into())
        );
    }

    #[tokio::test]
    async fn unknown_or_unsynced_param_kind_is_a_config_error() {
        let r = pref(Some("p"));
        assert_eq!(
            collect_params(Some(&kind()), None, ParamScope::Root, Some(&r), "").await,
            Err("paramKind kind `&ParamKind{APIVersion:v1,Kind:ConfigMap,}` not known".into())
        );
        let s = Store::default();
        tokio::time::pause();
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Err("paramKind kind `&ParamKind{APIVersion:v1,Kind:ConfigMap,}` not yet synced to use for admission".into())
        );
    }

    #[tokio::test]
    async fn name_and_selector_are_mutually_exclusive() {
        let s = synced(vec![]);
        let mut r = pref(Some("p"));
        r.selector = Some(LabelSelector::default());
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Err("paramRef.name and paramRef.selector are mutually exclusive".into())
        );
        // neither
        let r = pref(None);
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Err("one of name or selector must be provided".into())
        );
    }

    #[tokio::test]
    async fn selector_lists_every_match() {
        let a = cm("a", json!({"k": "v"}));
        let b = cm("b", json!({"k": "w"}));
        let c = cm("c", json!({"k": "v"}));
        let s = synced(vec![
            (None, a.clone()),
            (None, b.clone()),
            (None, c.clone()),
        ]);
        let mut r = pref(None);
        r.selector = Some(LabelSelector {
            match_labels: Some([("k".to_string(), "v".to_string())].into()),
            match_expressions: None,
        });
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Ok(vec![Some(a), Some(c)])
        );
    }

    #[tokio::test]
    async fn missing_param_follows_parameter_not_found_action() {
        let s = synced(vec![]);
        let mut r = pref(Some("absent"));
        // unset and Allow: no params, no error
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Ok(vec![])
        );
        r.parameter_not_found_action = Some(ParameterNotFoundAction::Allow);
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Ok(vec![])
        );
        r.parameter_not_found_action = Some(ParameterNotFoundAction::Deny);
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Err("no params found for policy binding with `Deny` parameterNotFoundAction".into())
        );
    }

    #[tokio::test]
    async fn store_errors_other_than_not_found_are_config_errors() {
        let mut s = synced(vec![]);
        s.get_error = Some(|| Error::Internal("boom".into()));
        let r = pref(Some("p"));
        let got = collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await;
        assert!(got.unwrap_err().contains("boom"));
    }

    // ---- Dispatch -------------------------------------------------------

    use rusternetes_api_server::admission::policy_matching::{
        EquivalentResourceMapper, NamespaceLister,
    };

    struct NoMapper;
    impl EquivalentResourceMapper for NoMapper {
        fn equivalent_resources_for(
            &self,
            r: &GroupVersionResource,
            _: &str,
        ) -> Vec<GroupVersionResource> {
            vec![r.clone()]
        }
        fn kind_for(&self, _: &GroupVersionResource, _: &str) -> GroupVersionKind {
            gvk("", "", "")
        }
    }
    struct NoNamespaces;
    #[async_trait]
    impl NamespaceLister for NoNamespaces {
        async fn namespace_labels(
            &self,
            name: &str,
        ) -> rusternetes_common::Result<HashMap<String, String>> {
            Err(Error::NotFound(format!("namespace {name} not found")))
        }
    }

    fn gvk(g: &str, v: &str, k: &str) -> GroupVersionKind {
        GroupVersionKind {
            group: g.into(),
            version: v.into(),
            kind: k.into(),
        }
    }
    fn pod_attr() -> Attributes {
        Attributes {
            kind: gvk("", "v1", "Pod"),
            resource: GroupVersionResource {
                group: "".into(),
                version: "v1".into(),
                resource: "pods".into(),
            },
            subresource: "".into(),
            namespace: "".into(),
            name: "p".into(),
            operation: Operation::Create,
            object: Some(json!({"metadata": {"name": "p"}})),
            old_object: None,
            user_info: None,
            dry_run: false,
            operation_options: None,
        }
    }
    fn match_pods() -> MatchResources {
        MatchResources {
            namespace_selector: Some(LabelSelector::default()),
            object_selector: Some(LabelSelector::default()),
            resource_rules: Some(vec![NamedRuleWithOperations {
                resource_names: None,
                rule: RuleWithOperations {
                    operations: Some(vec![OperationType::All]),
                    api_groups: Some(vec!["*".into()]),
                    api_versions: Some(vec!["*".into()]),
                    resources: Some(vec!["pods".into()]),
                    scope: None,
                },
            }]),
            exclude_resource_rules: None,
            match_policy: Some(MatchPolicyType::Exact),
        }
    }

    struct Pol {
        name: &'static str,
        constraints: Option<MatchResources>,
        kind: Option<ParamKind>,
        failure: Option<FailurePolicy>,
    }
    impl PolicyAccessor for Pol {
        fn name(&self) -> &str {
            self.name
        }
        fn namespace(&self) -> &str {
            ""
        }
        fn param_kind(&self) -> Option<&ParamKind> {
            self.kind.as_ref()
        }
        fn match_constraints(&self) -> Option<&MatchResources> {
            self.constraints.as_ref()
        }
        fn failure_policy(&self) -> Option<&FailurePolicy> {
            self.failure.as_ref()
        }
    }
    struct Bind {
        name: &'static str,
        resources: Option<MatchResources>,
        param_ref: Option<ParamRef>,
    }
    impl BindingAccessor for Bind {
        fn name(&self) -> &str {
            self.name
        }
        fn namespace(&self) -> &str {
            ""
        }
        fn policy_name(&self) -> NamespacedName {
            NamespacedName::default()
        }
        fn param_ref(&self) -> Option<&ParamRef> {
            self.param_ref.as_ref()
        }
        fn match_resources(&self) -> Option<&MatchResources> {
            self.resources.as_ref()
        }
    }
    fn pol(name: &'static str) -> Pol {
        Pol {
            name,
            constraints: Some(match_pods()),
            kind: None,
            failure: None,
        }
    }
    fn bind(name: &'static str) -> Bind {
        Bind {
            name,
            resources: None,
            param_ref: None,
        }
    }
    fn hook(p: Pol, bindings: Vec<Bind>) -> PolicyHook<Pol, Bind, ()> {
        PolicyHook {
            policy: p,
            bindings,
            param_store: None,
            param_scope: ParamScope::Root,
            evaluator: (),
            configuration_error: None,
        }
    }

    /// Records what it was called with and returns canned errors.
    #[derive(Default)]
    struct Delegate {
        seen: Mutex<Vec<(String, String, Option<Value>)>>,
        errors: Mutex<Vec<PolicyError>>,
        status_error: Option<fn() -> Error>,
        /// Whether the Pod kind was already in the accessor on entry.
        warm_on_entry: Mutex<Vec<bool>>,
    }
    #[async_trait]
    impl DispatchDelegate<Pol, Bind, ()> for Delegate {
        async fn dispatch(
            &self,
            _: &mut Attributes,
            versioned: &mut VersionedAttributeAccessor<'_>,
            invocations: &[PolicyInvocation<'_, Pol, Bind, ()>],
        ) -> Result<Vec<PolicyError>, Error> {
            self.warm_on_entry
                .lock()
                .unwrap()
                .push(versioned.cached(&gvk("", "v1", "Pod")).is_some());
            for i in invocations {
                self.seen.lock().unwrap().push((
                    i.policy.name.to_string(),
                    i.binding.name.to_string(),
                    i.param.clone(),
                ));
            }
            if let Some(f) = self.status_error {
                return Err(f());
            }
            Ok(self.errors.lock().unwrap().clone())
        }
    }

    async fn run(hooks: &[PolicyHook<Pol, Bind, ()>], delegate: &Delegate) -> Result<(), Error> {
        let m = Matcher {
            namespaces: &NoNamespaces,
            mapper: &NoMapper,
        };
        dispatch(&m, &mut pod_attr(), hooks, delegate).await
    }
    fn status_of(e: Error) -> Status {
        match e {
            Error::Status(s) => *s,
            other => panic!("not a status: {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_matching_policy_is_not_dispatched() {
        let mut p = pol("p");
        let mut mr = match_pods();
        mr.resource_rules.as_mut().unwrap()[0].rule.resources = Some(vec!["deployments".into()]);
        p.constraints = Some(mr);
        let d = Delegate::default();
        assert!(run(&[hook(p, vec![bind("b")])], &d).await.is_ok());
        assert!(d.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn matching_pairs_reach_the_delegate_once_per_binding() {
        let d = Delegate::default();
        run(&[hook(pol("p"), vec![bind("b1"), bind("b2")])], &d)
            .await
            .unwrap();
        assert_eq!(
            *d.seen.lock().unwrap(),
            vec![
                ("p".into(), "b1".into(), None),
                ("p".into(), "b2".into(), None)
            ]
        );
    }

    #[tokio::test]
    async fn non_matching_binding_is_skipped() {
        let mut b = bind("skip");
        let mut mr = match_pods();
        mr.resource_rules.as_mut().unwrap()[0].rule.resources = Some(vec!["nodes".into()]);
        b.resources = Some(mr);
        let d = Delegate::default();
        run(&[hook(pol("p"), vec![b, bind("keep")])], &d)
            .await
            .unwrap();
        assert_eq!(d.seen.lock().unwrap().len(), 1);
        assert_eq!(d.seen.lock().unwrap()[0].1, "keep");
    }

    #[tokio::test]
    async fn a_policy_without_match_constraints_is_denied_as_a_config_error() {
        let mut p = pol("p");
        p.constraints = None;
        let d = Delegate::default();
        let s = status_of(run(&[hook(p, vec![bind("b")])], &d).await.unwrap_err());
        assert_eq!(s.reason.as_deref(), Some("Forbidden"));
        assert_eq!(
            s.message.as_deref(),
            Some("policy \"p\" denied request: failed to configure policy: policy contained no match constraints, a required field")
        );
        assert!(d.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failure_policy_ignore_swallows_config_errors() {
        let mut p = pol("p");
        p.constraints = None;
        p.failure = Some(FailurePolicy::Ignore);
        assert!(run(&[hook(p, vec![bind("b")])], &Delegate::default())
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn hook_configuration_error_is_a_policy_error() {
        let mut h = hook(pol("p"), vec![bind("b")]);
        h.configuration_error = Some("compile failed".into());
        let d = Delegate::default();
        let s = status_of(run(&[h], &d).await.unwrap_err());
        assert_eq!(
            s.message.as_deref(),
            Some("policy \"p\" denied request: failed to configure policy: compile failed")
        );
        assert!(d.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn param_errors_name_the_binding_and_skip_it() {
        let mut p = pol("p");
        p.kind = Some(kind());
        let mut b = bind("b");
        b.param_ref = Some(pref(Some("x")));
        let d = Delegate::default();
        // no param store: "not known"
        let s = status_of(run(&[hook(p, vec![b])], &d).await.unwrap_err());
        assert_eq!(
            s.message.as_deref(),
            Some("policy 'p' with binding 'b' denied request: failed to configure binding: paramKind kind `&ParamKind{APIVersion:v1,Kind:ConfigMap,}` not known")
        );
        assert!(d.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn resolved_params_each_become_an_invocation() {
        let a = cm("a", json!({"k": "v"}));
        let c = cm("c", json!({"k": "v"}));
        let mut p = pol("p");
        p.kind = Some(kind());
        let mut b = bind("b");
        let mut r = pref(None);
        r.selector = Some(LabelSelector {
            match_labels: Some([("k".to_string(), "v".to_string())].into()),
            match_expressions: None,
        });
        b.param_ref = Some(r);
        let mut h = hook(p, vec![b]);
        h.param_store = Some(Arc::new(synced(vec![(None, a.clone()), (None, c.clone())])));
        let d = Delegate::default();
        run(&[h], &d).await.unwrap();
        let seen = d.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].2, Some(a));
        assert_eq!(seen[1].2, Some(c));
    }

    #[tokio::test]
    async fn delegate_status_error_fails_the_request_as_is() {
        let d = Delegate {
            status_error: Some(|| Error::BadRequest("delegate says no".into())),
            ..Default::default()
        };
        let e = run(&[hook(pol("p"), vec![bind("b")])], &d)
            .await
            .unwrap_err();
        assert!(matches!(e, Error::BadRequest(m) if m == "delegate says no"));
    }

    #[tokio::test]
    async fn delegate_errors_are_filtered_by_failure_policy_and_denial_lists_each() {
        let ignored = Pol {
            failure: Some(FailurePolicy::Ignore),
            ..pol("ignored")
        };
        let failing = pol("failing");
        let d = Delegate::default();
        *d.errors.lock().unwrap() = vec![
            PolicyError::new(&ignored, Some(&bind("b")), "dropped", None),
            PolicyError::new(&failing, Some(&bind("b1")), "first", Some("Invalid")),
            PolicyError::new(&failing, Some(&bind("b2")), "second", None),
        ];
        let s = status_of(
            run(
                &[
                    hook(ignored, vec![bind("b")]),
                    hook(failing, vec![bind("b1")]),
                ],
                &d,
            )
            .await
            .unwrap_err(),
        );
        // the first denial supplies the message and reason; code stays 403
        assert_eq!(
            s.message.as_deref(),
            Some("policy 'failing' with binding 'b1' denied request: first")
        );
        assert_eq!(s.reason.as_deref(), Some("Invalid"));
        assert_eq!(s.code, Some(403));
        let causes = s.details.unwrap().causes.unwrap();
        assert_eq!(causes.len(), 2);
        assert_eq!(
            causes[1].message.as_deref(),
            Some("policy 'failing' with binding 'b2' denied request: second")
        );
    }

    #[test]
    fn policy_error_text() {
        let p = pol("p");
        assert_eq!(
            PolicyError::new(&p, Some(&bind("b")), "m", None).error(),
            "policy 'p' with binding 'b' denied request: m"
        );
        assert_eq!(
            PolicyError::new(&p, None, "m", None).error(),
            "policy \"p\" denied request: m"
        );
    }

    // ---- VersionedAttributes pre-warm (policy_dispatcher.go:152-160) -----

    /// Counts conversions and fails on request.
    struct Converter {
        calls: Mutex<Vec<GroupVersionKind>>,
        fail: bool,
    }
    impl VersionConverter for Converter {
        fn new_versioned_attributes(
            &self,
            attr: &Attributes,
            gvk: &GroupVersionKind,
        ) -> Result<VersionedAttributes, String> {
            self.calls.lock().unwrap().push(gvk.clone());
            if self.fail {
                return Err("no conversion to v1 Pod".into());
            }
            IdentityConverter.new_versioned_attributes(attr, gvk)
        }
    }
    async fn run_converting(
        hooks: &[PolicyHook<Pol, Bind, ()>],
        delegate: &Delegate,
        converter: &Converter,
    ) -> Result<(), Error> {
        let m = Matcher {
            namespaces: &NoNamespaces,
            mapper: &NoMapper,
        };
        dispatch_with_converter(&m, &mut pod_attr(), hooks, delegate, converter).await
    }

    /// The versioned attributes of a matched kind are built by `Dispatch`
    /// before the delegate runs, once however many bindings match.
    #[tokio::test]
    async fn matched_kind_is_warmed_once_before_the_delegate() {
        let c = Converter {
            calls: Mutex::new(vec![]),
            fail: false,
        };
        let d = Delegate::default();
        run_converting(&[hook(pol("p"), vec![bind("b1"), bind("b2")])], &d, &c)
            .await
            .unwrap();
        assert_eq!(*d.warm_on_entry.lock().unwrap(), vec![true]);
        assert_eq!(*c.calls.lock().unwrap(), vec![gvk("", "v1", "Pod")]);
    }

    /// A failed conversion is a policy-level (no binding) config error and the
    /// binding is skipped: the delegate never sees it.
    #[tokio::test]
    async fn failed_warm_is_a_policy_config_error_and_skips_the_binding() {
        let c = Converter {
            calls: Mutex::new(vec![]),
            fail: true,
        };
        let d = Delegate::default();
        let s = status_of(
            run_converting(&[hook(pol("p"), vec![bind("b")])], &d, &c)
                .await
                .unwrap_err(),
        );
        assert_eq!(
            s.message.as_deref(),
            Some(
                "policy \"p\" denied request: failed to configure policy: no conversion to v1 Pod"
            )
        );
        assert!(d.seen.lock().unwrap().is_empty());
        assert!(d.warm_on_entry.lock().unwrap().is_empty());
    }

    /// failurePolicy Ignore swallows the warm-up error like any other.
    #[tokio::test]
    async fn failed_warm_honours_failure_policy_ignore() {
        let c = Converter {
            calls: Mutex::new(vec![]),
            fail: true,
        };
        let mut p = pol("p");
        p.failure = Some(FailurePolicy::Ignore);
        let d = Delegate::default();
        assert!(run_converting(&[hook(p, vec![bind("b")])], &d, &c)
            .await
            .is_ok());
    }

    /// Only a binding that matches warms: no conversion for a miss.
    #[tokio::test]
    async fn non_matching_binding_does_not_warm() {
        let c = Converter {
            calls: Mutex::new(vec![]),
            fail: false,
        };
        let mut b = bind("b");
        let mut mr = match_pods();
        mr.resource_rules.as_mut().unwrap()[0].rule.resources = Some(vec!["deployments".into()]);
        b.resources = Some(mr);
        let d = Delegate::default();
        run_converting(&[hook(pol("p"), vec![b])], &d, &c)
            .await
            .unwrap();
        assert!(c.calls.lock().unwrap().is_empty());
    }
}
