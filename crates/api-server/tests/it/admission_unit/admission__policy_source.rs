//! Unit tests moved from `src/admission/policy_source.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use futures::StreamExt;
    pub use rusternetes_api_server::admission::policy_dispatch::{
        BindingAccessor, NamespacedName, ParamScope, ParamStore, PolicyAccessor, PolicyHook,
    };
    pub use rusternetes_common::admission::GroupVersionResource;
    pub use rusternetes_common::resources::validating_admission_policy::{
        FailurePolicy, MatchResources, ParamKind, ParamRef,
    };
    pub use rusternetes_common::resources::{
        MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding,
    };
    pub use rusternetes_common::types::Selector;
    pub use rusternetes_common::Error;
    pub use rusternetes_storage::{build_prefix, Storage, WatchEvent};
    pub use serde::de::DeserializeOwned;
    pub use serde_json::Value;
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::atomic::{AtomicBool, Ordering};
    pub use std::sync::{Arc, Mutex, RwLock};
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::policy_source::*;
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::MemoryStorage;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;

    // policy_source_test.go FakePolicy / FakeBinding.
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct FakePolicy {
        metadata: ObjectMeta,
        #[serde(default)]
        param_kind: Option<ParamKind>,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct FakeBinding {
        metadata: ObjectMeta,
        #[serde(default)]
        policy_name: String,
    }

    impl PolicyAccessor for FakePolicy {
        fn name(&self) -> &str {
            &self.metadata.name
        }
        fn namespace(&self) -> &str {
            self.metadata.namespace.as_deref().unwrap_or("")
        }
        fn param_kind(&self) -> Option<&ParamKind> {
            self.param_kind.as_ref()
        }
        fn match_constraints(&self) -> Option<&MatchResources> {
            None
        }
        fn failure_policy(&self) -> Option<&FailurePolicy> {
            None
        }
    }
    impl HasResourceVersion for FakePolicy {
        fn resource_version(&self) -> &str {
            self.metadata.resource_version.as_deref().unwrap_or("")
        }
    }
    impl BindingAccessor for FakeBinding {
        fn name(&self) -> &str {
            &self.metadata.name
        }
        fn namespace(&self) -> &str {
            ""
        }
        fn policy_name(&self) -> NamespacedName {
            NamespacedName {
                name: self.policy_name.clone(),
                namespace: String::new(),
            }
        }
        fn param_ref(&self) -> Option<&ParamRef> {
            None
        }
        fn match_resources(&self) -> Option<&MatchResources> {
            None
        }
    }

    /// Maps only `policy.example.com/v1 Known`; everything else is an unknown
    /// kind, as upstream's fake RESTMapper does for an unregistered CRD.
    struct FakeMapper;
    impl RestMapper for FakeMapper {
        fn rest_mapping(&self, g: &str, k: &str, v: &str) -> Result<RestMapping, String> {
            if (g, k, v) == ("policy.example.com", "Known", "v1") {
                Ok(RestMapping {
                    resource: GroupVersionResource {
                        group: g.into(),
                        version: v.into(),
                        resource: "knowns".into(),
                    },
                    scope: ParamScope::Root,
                })
            } else {
                Err("no match".into())
            }
        }
    }

    #[derive(Default)]
    struct CountingFactory {
        started: Arc<AtomicUsize>,
        stopped: Arc<AtomicUsize>,
    }
    struct NoStore;
    #[async_trait]
    impl ParamStore for NoStore {
        async fn get(&self, _: Option<&str>, _: &str) -> Result<Value, Error> {
            Err(Error::NotFound("x".into()))
        }
        async fn list(&self, _: Option<&str>, _: &Selector) -> Result<Vec<Value>, Error> {
            Ok(vec![])
        }
        fn has_synced(&self) -> bool {
            true
        }
    }
    impl ParamInformerFactory for CountingFactory {
        fn start(&self, _: &RestMapping) -> ParamInformer {
            self.started.fetch_add(1, Ordering::SeqCst);
            let stopped = self.stopped.clone();
            ParamInformer::new(Arc::new(NoStore), move || {
                stopped.fetch_add(1, Ordering::SeqCst);
            })
        }
    }

    struct Ctx {
        storage: Arc<MemoryStorage>,
        source: Arc<PolicySource<FakePolicy, FakeBinding, usize>>,
        compiles: Arc<AtomicUsize>,
        factory_started: Arc<AtomicUsize>,
        factory_stopped: Arc<AtomicUsize>,
        tasks: Vec<tokio::task::JoinHandle<()>>,
    }

    impl Drop for Ctx {
        fn drop(&mut self) {
            for t in &self.tasks {
                t.abort();
            }
        }
    }

    fn put(kind: &str, name: &str, extra: Value) -> (String, Value) {
        let mut v = json!({"metadata": {"name": name}});
        v.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        (format!("/registry/{kind}/{name}"), v)
    }

    /// `NewPolicyTestContext` + `Start`: informers running over a storage seeded
    /// with `initial`, the source running with a 10ms refresh, and `Start`
    /// returning once the source has synced.
    async fn start(initial: Vec<(String, Value)>) -> Ctx {
        let storage = Arc::new(MemoryStorage::new());
        for (k, v) in initial {
            storage.create(&k, &v).await.unwrap();
        }
        let policies = StorageInformer::<FakePolicy>::new("fakepolicies");
        let bindings = StorageInformer::<FakeBinding>::new("fakebindings");
        let compiles = Arc::new(AtomicUsize::new(0));
        let c = compiles.clone();
        let factory = CountingFactory::default();
        let (started, stopped) = (factory.started.clone(), factory.stopped.clone());
        let source = PolicySource::new(
            policies.clone(),
            bindings.clone(),
            move |_p: &FakePolicy| c.fetch_add(1, Ordering::SeqCst) + 1,
            Arc::new(factory),
            Arc::new(FakeMapper),
        )
        .with_refresh_interval(Duration::from_millis(10));
        let tasks = vec![
            tokio::spawn(policies.run(storage.clone())),
            tokio::spawn(bindings.run(storage.clone())),
            tokio::spawn({
                let s = source.clone();
                async move {
                    let _ = s.run().await;
                }
            }),
        ];
        let ctx = Ctx {
            storage,
            source,
            compiles,
            factory_started: started,
            factory_stopped: stopped,
            tasks,
        };
        wait_for(|| ctx.source.has_synced()).await;
        ctx
    }

    async fn wait_for(cond: impl Fn() -> bool) {
        for _ in 0..500 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached within 5s");
    }

    impl Ctx {
        /// `UpdateAndWait`'s write half: create or replace each object. Callers
        /// then wait for the condition they expect.
        async fn write(&self, objs: Vec<(String, Value)>) {
            for (k, v) in objs {
                if self.storage.update_raw(&k, &v).await.is_err() {
                    self.storage.create(&k, &v).await.unwrap();
                }
            }
        }
        fn hook_names(&self) -> Vec<String> {
            self.source
                .hooks()
                .map(|h| h.iter().map(|h| h.policy.metadata.name.clone()).collect())
                .unwrap_or_default()
        }
        async fn wait_hooks(&self, n: usize) {
            wait_for(|| self.source.hooks().is_some_and(|h| h.len() == n)).await;
        }
    }

    fn pol(name: &str) -> (String, Value) {
        put("fakepolicies", name, json!({}))
    }
    fn bind(name: &str, policy: &str) -> (String, Value) {
        put("fakebindings", name, json!({"policyName": policy}))
    }

    // policy_source_test.go TestPolicySourceHasSyncedEmpty
    #[tokio::test]
    async fn has_synced_empty() {
        let ctx = start(vec![]).await;
        assert!(ctx.source.has_synced());
        assert_eq!(ctx.source.hooks().unwrap().len(), 0);
    }

    // policy_source_test.go TestPolicySourceHasSyncedInitialList
    #[tokio::test]
    async fn has_synced_initial_list() {
        let ctx = start(vec![pol("policy1"), bind("binding1", "policy1")]).await;
        assert_eq!(
            ctx.source.hooks().unwrap().len(),
            1,
            "should have one policy"
        );

        ctx.write(vec![pol("policy2"), bind("binding2", "policy2")])
            .await;
        ctx.wait_hooks(2).await;

        let mut p2 = pol("policy2");
        p2.1["paramKind"] = json!({"apiVersion": "policy.example.com/v1", "kind": "FakeParam"});
        ctx.write(vec![pol("policy3"), bind("binding3", "policy3"), p2])
            .await;
        ctx.wait_hooks(3).await;
        // policy2's paramKind is unknown to the mapper: still a hook, with a
        // configuration error.
        wait_for(|| {
            ctx.source.hooks().is_some_and(|h| {
                h.iter()
                    .any(|h| h.policy.metadata.name == "policy2" && h.configuration_error.is_some())
            })
        })
        .await;
    }

    // policy_source_test.go TestPolicySourceBindsToPolicies
    #[tokio::test]
    async fn binds_to_policies() {
        let ctx = start(vec![pol("policy1"), bind("binding1", "policy1")]).await;
        let hooks = ctx.source.hooks().unwrap();
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].bindings.len(), 1);
        assert_eq!(hooks[0].bindings[0].metadata.name, "binding1");

        // Change the binding to another policy (policies without bindings are
        // ignored, so it removes the first).
        ctx.write(vec![pol("policy2"), bind("binding1", "policy2")])
            .await;
        wait_for(|| ctx.hook_names() == vec!["policy2".to_string()]).await;
        let hooks = ctx.source.hooks().unwrap();
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].bindings.len(), 1);
        assert_eq!(hooks[0].bindings[0].metadata.name, "binding1");
    }

    // policy_source.go:312-320: a binding whose policy does not exist is
    // skipped; the policy arriving later makes the hook appear.
    #[tokio::test]
    async fn binding_without_policy_is_skipped_until_policy_exists() {
        let ctx = start(vec![bind("b", "late")]).await;
        assert_eq!(ctx.source.hooks().unwrap().len(), 0);
        ctx.write(vec![pol("late")]).await;
        ctx.wait_hooks(1).await;
    }

    // policy_source.go:490-498: the evaluator is recompiled only when the
    // policy's resource version changes.
    #[tokio::test]
    async fn evaluator_is_cached_by_resource_version() {
        let ctx = start(vec![pol("p"), bind("b1", "p")]).await;
        assert_eq!(ctx.compiles.load(Ordering::SeqCst), 1);

        // A binding change refreshes the hooks but must not recompile `p`.
        ctx.write(vec![bind("b2", "p")]).await;
        wait_for(|| ctx.source.hooks().is_some_and(|h| h[0].bindings.len() == 2)).await;
        assert_eq!(ctx.compiles.load(Ordering::SeqCst), 1);

        // A policy update bumps its resource version: recompiled.
        let mut p = pol("p");
        p.1["paramKind"] = json!({"apiVersion": "v1", "kind": "Whatever"});
        ctx.write(vec![p]).await;
        wait_for(|| ctx.compiles.load(Ordering::SeqCst) == 2).await;
    }

    // policy_source.go:332-348 + 389-451: a known paramKind starts one
    // informer (shared by policies using it) and reports its scope.
    #[tokio::test]
    async fn param_informer_started_once_with_scope() {
        let known = json!({"apiVersion": "policy.example.com/v1", "kind": "Known"});
        let mut p1 = pol("p1");
        p1.1["paramKind"] = known.clone();
        let mut p2 = pol("p2");
        p2.1["paramKind"] = known;
        let ctx = start(vec![p1, p2, bind("b1", "p1"), bind("b2", "p2")]).await;
        let hooks = ctx.source.hooks().unwrap();
        assert_eq!(hooks.len(), 2);
        for h in hooks.iter() {
            assert!(h.param_store.is_some());
            assert_eq!(h.param_scope, ParamScope::Root);
            assert!(h.configuration_error.is_none());
        }
        assert_eq!(ctx.factory_started.load(Ordering::SeqCst), 1);
    }

    // policy_source.go:374-380: an informer nobody uses any more is stopped.
    #[tokio::test]
    async fn orphaned_param_informer_is_stopped() {
        let mut p = pol("p");
        p.1["paramKind"] = json!({"apiVersion": "policy.example.com/v1", "kind": "Known"});
        let ctx = start(vec![p, bind("b", "p")]).await;
        assert_eq!(ctx.factory_started.load(Ordering::SeqCst), 1);
        assert_eq!(ctx.factory_stopped.load(Ordering::SeqCst), 0);

        // Drop the paramKind.
        ctx.write(vec![pol("p")]).await;
        wait_for(|| ctx.factory_stopped.load(Ordering::SeqCst) == 1).await;
    }

    // policy_source.go:332-337: a paramKind with an unparseable apiVersion
    // drops the policy for this pass (no hook), unlike an unmapped kind.
    #[tokio::test]
    async fn unparseable_param_kind_api_version_yields_no_hook() {
        let mut p = pol("p");
        p.1["paramKind"] = json!({"apiVersion": "a/b/c", "kind": "K"});
        let ctx = start(vec![p, bind("b", "p")]).await;
        assert_eq!(ctx.source.hooks().unwrap().len(), 0);
    }

    // policy_source.go:148-150
    #[tokio::test]
    async fn run_twice_is_an_error() {
        let ctx = start(vec![]).await;
        assert_eq!(
            ctx.source.clone().run().await,
            Err("policy source already running".to_string())
        );
    }

    #[test]
    fn parse_group_version_cases() {
        assert_eq!(parse_group_version("v1"), Ok((String::new(), "v1".into())));
        assert_eq!(
            parse_group_version("apps/v1"),
            Ok(("apps".into(), "v1".into()))
        );
        assert!(parse_group_version("a/b/c").is_err());
    }

    #[test]
    fn mutating_admission_policy_binding_policy_name_is_cluster_scoped() {
        use rusternetes_common::resources::mutating_admission_policy::MutatingAdmissionPolicyBindingSpec;
        let mut b = MutatingAdmissionPolicyBinding::new("b");
        b.spec = Some(MutatingAdmissionPolicyBindingSpec {
            policy_name: Some("p".into()),
            param_ref: None,
            match_resources: None,
        });
        assert_eq!(
            b.policy_name(),
            NamespacedName {
                name: "p".into(),
                namespace: String::new()
            }
        );
    }
}
