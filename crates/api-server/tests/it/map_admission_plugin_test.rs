//! #2910: the MutatingAdmissionPolicy plugin runs at the head of
//! `Admission::admit` (`pkg/kubeapiserver/options/plugins.go:106-110`), and
//! only while `Feature::MutatingAdmissionPolicy` is on
//! (`mutating/plugin.go` `InspectFeatureGates`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_api_server::admission::policy_dispatch::ParamScope;
use rusternetes_api_server::admission::policy_dispatch::PolicyHook;
use rusternetes_api_server::admission::policy_matching::{
    EquivalentResourceMapper, NamespaceLister,
};
use rusternetes_api_server::admission::policy_mutating::{
    MutatingEvaluator, NamespaceObjects, ObjectDefaulter, PatchError, PatchRequest, Patcher,
    TypeConverters,
};
use rusternetes_api_server::admission::policy_plugin::{HookSource, MutatingPolicyPlugin};
use rusternetes_api_server::endpoints::handlers::admission::Admission;
use rusternetes_api_server::state::ApiServerState;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource, Operation};
use rusternetes_common::auth::{TokenManager, UserInfo};
use rusternetes_common::authz::AlwaysAllowAuthorizer;
use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_common::resources::mutating_admission_policy::{
    MutatingAdmissionPolicyBindingSpec, MutatingAdmissionPolicySpec, Mutation, PatchType,
    ReinvocationPolicyType,
};
use rusternetes_common::resources::validating_admission_policy::{
    MatchPolicyType, NamedRuleWithOperations, OperationType, RuleWithOperations,
};
use rusternetes_common::resources::{
    ConfigMap, LabelSelector, MatchResources, MutatingAdmissionPolicy,
    MutatingAdmissionPolicyBinding,
};
use rusternetes_storage::{MemoryStorage, StorageBackend};
use serde_json::{json, Value};
use serial_test::serial;

struct Source(
    Arc<
        Vec<PolicyHook<MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding, MutatingEvaluator>>,
    >,
);
impl HookSource for Source {
    fn has_synced(&self) -> bool {
        true
    }
    fn hooks(
        &self,
    ) -> Option<
        Arc<
            Vec<
                PolicyHook<
                    MutatingAdmissionPolicy,
                    MutatingAdmissionPolicyBinding,
                    MutatingEvaluator,
                >,
            >,
        >,
    > {
        Some(self.0.clone())
    }
}

struct Fakes;
#[async_trait]
impl NamespaceLister for Fakes {
    async fn namespace_labels(
        &self,
        _: &str,
    ) -> rusternetes_common::Result<HashMap<String, String>> {
        Ok(HashMap::new())
    }
}
#[async_trait]
impl NamespaceObjects for Fakes {
    async fn get_namespace(&self, name: &str) -> rusternetes_common::Result<Value> {
        Ok(json!({"kind": "Namespace", "metadata": {"name": name}}))
    }
}
impl EquivalentResourceMapper for Fakes {
    fn equivalent_resources_for(
        &self,
        r: &GroupVersionResource,
        _: &str,
    ) -> Vec<GroupVersionResource> {
        vec![r.clone()]
    }
    fn kind_for(&self, _: &GroupVersionResource, _: &str) -> GroupVersionKind {
        GroupVersionKind {
            group: String::new(),
            version: String::new(),
            kind: String::new(),
        }
    }
}
impl TypeConverters for Fakes {
    fn has_type_converter(&self, _: &GroupVersionKind) -> bool {
        true
    }
}
impl ObjectDefaulter for Fakes {
    fn default_object(&self, _: &mut Value) {}
}

struct SetLabel;
#[async_trait]
impl Patcher for SetLabel {
    async fn patch(&self, r: &PatchRequest<'_>) -> Result<Value, PatchError> {
        let mut o = r.versioned_attributes.versioned_object.clone().unwrap();
        o["metadata"]["labels"] = json!({"mutated": "yes"});
        Ok(o)
    }
}

type Hook = PolicyHook<MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding, MutatingEvaluator>;

fn plugin() -> Arc<MutatingPolicyPlugin> {
    plugin_with(vec![hook("p", None, Arc::new(SetLabel))])
}

fn hook(
    name: &str,
    reinvocation: Option<ReinvocationPolicyType>,
    patcher: Arc<dyn Patcher>,
) -> Hook {
    let mut p = MutatingAdmissionPolicy::new(name);
    p.spec = Some(MutatingAdmissionPolicySpec {
        match_constraints: Some(MatchResources {
            namespace_selector: Some(LabelSelector::default()),
            object_selector: Some(LabelSelector::default()),
            resource_rules: Some(vec![NamedRuleWithOperations {
                resource_names: None,
                rule: RuleWithOperations {
                    operations: Some(vec![OperationType::All]),
                    api_groups: Some(vec!["".into()]),
                    api_versions: Some(vec!["*".into()]),
                    resources: Some(vec!["configmaps".into()]),
                    scope: None,
                },
            }]),
            exclude_resource_rules: None,
            match_policy: Some(MatchPolicyType::Exact),
        }),
        mutations: Some(vec![Mutation {
            patch_type: Some(PatchType::ApplyConfiguration),
            apply_configuration: None,
            json_patch: None,
        }]),
        reinvocation_policy: reinvocation,
        ..Default::default()
    });
    let mut b = MutatingAdmissionPolicyBinding::new(&format!("{name}-b"));
    b.spec = Some(MutatingAdmissionPolicyBindingSpec {
        policy_name: Some(name.into()),
        ..Default::default()
    });
    PolicyHook {
        policy: p,
        bindings: vec![b],
        param_store: None,
        param_scope: ParamScope::Root,
        evaluator: MutatingEvaluator {
            matcher: None,
            mutators: vec![Some(patcher)],
        },
        configuration_error: None,
    }
}

fn plugin_with(hooks: Vec<Hook>) -> Arc<MutatingPolicyPlugin> {
    Arc::new(MutatingPolicyPlugin {
        source: Arc::new(Source(Arc::new(hooks))),
        namespaces: Arc::new(Fakes),
        mapper: Arc::new(Fakes),
        namespace_objects: Arc::new(Fakes),
        type_converters: Arc::new(Fakes),
        defaulter: Arc::new(Fakes),
    })
}

fn state(install: bool) -> ApiServerState {
    state_with(install.then(plugin))
}

fn state_with(plugin: Option<Arc<MutatingPolicyPlugin>>) -> ApiServerState {
    let backend = Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new())));
    state_on(backend, plugin)
}

fn state_on(
    backend: Arc<StorageBackend>,
    plugin: Option<Arc<MutatingPolicyPlugin>>,
) -> ApiServerState {
    let s = ApiServerState::new(
        backend,
        Arc::new(TokenManager::new(b"secret")),
        Arc::new(AlwaysAllowAuthorizer),
        Arc::new(MetricsRegistry::new()),
        true,
    );
    match plugin {
        Some(p) => s.with_mutating_admission_policy(p),
        None => s,
    }
}

async fn admit(state: &ApiServerState, op: Operation) -> ConfigMap {
    let kind = GroupVersionKind {
        group: String::new(),
        version: "v1".into(),
        kind: "ConfigMap".into(),
    };
    let resource = GroupVersionResource {
        group: String::new(),
        version: "v1".into(),
        resource: "configmaps".into(),
    };
    let user = UserInfo {
        username: "u".into(),
        uid: "".into(),
        groups: vec![],
        extra: HashMap::new(),
    };
    let admission = Admission {
        state,
        kind: &kind,
        resource: &resource,
        subresource: None,
        namespace: Some("default"),
        user: &user,
        dry_run: false,
    };
    let cm: ConfigMap = serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "cm", "namespace": "default"},
    }))
    .unwrap();
    admission.admit(op, cm, None).await.unwrap()
}

fn labels(cm: &ConfigMap) -> Option<HashMap<String, String>> {
    cm.metadata.labels.clone()
}

#[tokio::test]
#[serial]
async fn gate_on_runs_the_policy_at_the_head_of_admit() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
    let cm = admit(&state(true), Operation::Create).await;
    assert_eq!(
        labels(&cm),
        Some(HashMap::from([("mutated".to_string(), "yes".to_string())]))
    );
}

#[tokio::test]
#[serial]
async fn gate_off_leaves_admit_untouched() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, false);
    let cm = admit(&state(true), Operation::Create).await;
    assert_eq!(labels(&cm), None);
}

#[tokio::test]
#[serial]
async fn gate_on_without_an_installed_plugin_is_a_no_op() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
    let cm = admit(&state(false), Operation::Create).await;
    assert_eq!(labels(&cm), None);
}

/// Adds `key=value` to the labels and counts its invocations.
struct Counting {
    key: &'static str,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl Patcher for Counting {
    async fn patch(&self, r: &PatchRequest<'_>) -> Result<Value, PatchError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut o = r.versioned_attributes.versioned_object.clone().unwrap();
        o["metadata"]["labels"][self.key] = json!("yes");
        Ok(o)
    }
}

fn counting(
    name: &str,
    policy: Option<ReinvocationPolicyType>,
    key: &'static str,
) -> (Hook, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let patcher = Arc::new(Counting {
        key,
        calls: calls.clone(),
    });
    (hook(name, policy, patcher), calls)
}

/// reinvocation.go `reinvoker.Admit`: a later mutation sets
/// `ShouldReinvoke`, the chain runs a second time, and a policy with
/// `reinvocationPolicy: IfNeeded` is run again.
#[tokio::test]
#[serial]
async fn if_needed_policy_is_reinvoked_after_a_later_mutation() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
    let (first, first_calls) = counting("first", Some(ReinvocationPolicyType::IfNeeded), "a");
    let (second, second_calls) = counting("second", Some(ReinvocationPolicyType::Never), "b");
    let state = state_with(Some(plugin_with(vec![first, second])));
    let cm = admit(&state, Operation::Create).await;
    assert_eq!(first_calls.load(Ordering::SeqCst), 2);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        labels(&cm),
        Some(HashMap::from([
            ("a".to_string(), "yes".to_string()),
            ("b".to_string(), "yes".to_string())
        ]))
    );
}

/// `reinvocationPolicy: Never` is not remembered, so it is not rerun.
#[tokio::test]
#[serial]
async fn never_policy_is_not_reinvoked() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
    let (first, first_calls) = counting("first", Some(ReinvocationPolicyType::Never), "a");
    let (second, _) = counting("second", Some(ReinvocationPolicyType::Never), "b");
    let state = state_with(Some(plugin_with(vec![first, second])));
    admit(&state, Operation::Create).await;
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
}

// ---- #3041: mutating webhooks join the shared ReinvocationContext --------
// webhook/mutating/dispatcher.go:106-121 (`webhookReinvokeContext`).

/// Mock mutating webhook: sets label `w=call-N` and counts its calls.
async fn mock_webhook() -> (String, Arc<AtomicUsize>) {
    use base64::Engine;
    use rusternetes_common::admission::AdmissionReview;
    use warp::Filter;
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let route = warp::post()
        .and(warp::body::json())
        .map(move |review: AdmissionReview| {
            let n = c.fetch_add(1, Ordering::SeqCst) + 1;
            let uid = review.request.map(|r| r.uid).unwrap_or_default();
            let patch =
                json!([{"op": "add", "path": "/metadata/labels/w", "value": format!("call-{n}")}]);
            warp::reply::json(&json!({
                "apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview",
                "response": {
                    "uid": uid, "allowed": true, "patchType": "JSONPatch",
                    "patch": base64::engine::general_purpose::STANDARD.encode(patch.to_string()),
                }
            }))
        });
    let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
    tokio::spawn(server);
    (format!("http://{addr}"), calls)
}

async fn install_webhook(backend: &Arc<StorageBackend>, url: String) {
    use rusternetes_common::resources::{
        MutatingWebhook, MutatingWebhookConfiguration, OperationType as WhOp, ReinvocationPolicy,
        Rule, RuleWithOperations as WhRule, SideEffectClass, WebhookClientConfig,
    };
    use rusternetes_storage::{build_key, Storage};
    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".into(),
        kind: "MutatingWebhookConfiguration".into(),
        metadata: rusternetes_common::types::ObjectMeta::new("w-cfg"),
        webhooks: Some(vec![MutatingWebhook {
            name: "w.example.com".into(),
            client_config: WebhookClientConfig {
                url: Some(url),
                service: None,
                ca_bundle: None,
            },
            rules: vec![WhRule {
                operations: vec![WhOp::All],
                rule: Rule {
                    api_groups: vec!["".into()],
                    api_versions: vec!["v1".into()],
                    resources: vec!["configmaps".into()],
                    scope: None,
                },
            }],
            failure_policy: None,
            match_policy: None,
            namespace_selector: None,
            object_selector: None,
            side_effects: SideEffectClass::None,
            timeout_seconds: None,
            admission_review_versions: vec!["v1".into()],
            reinvocation_policy: Some(ReinvocationPolicy::IfNeeded),
            match_conditions: None,
        }]),
    };
    backend
        .create(
            &build_key("mutatingwebhookconfigurations", None, "w-cfg"),
            &cfg,
        )
        .await
        .unwrap();
}

/// Sets label `n` to its own call count: every rerun changes the object.
struct Stamp(Arc<AtomicUsize>);
#[async_trait]
impl Patcher for Stamp {
    async fn patch(&self, r: &PatchRequest<'_>) -> Result<Value, PatchError> {
        let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        let mut o = r.versioned_attributes.versioned_object.clone().unwrap();
        o["metadata"]["labels"]["n"] = json!(n.to_string());
        Ok(o)
    }
}

/// dispatcher.go:115-119: on the second pass a policy rerun that changed the
/// object since the webhook's last output requires the IfNeeded webhook to be
/// reinvoked.
#[tokio::test]
#[serial]
async fn if_needed_webhook_is_reinvoked_after_a_policy_changed_the_object() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
    let backend = Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new())));
    let (url, w_calls) = mock_webhook().await;
    install_webhook(&backend, url).await;
    let p_calls = Arc::new(AtomicUsize::new(0));
    let policy = hook(
        "p",
        Some(ReinvocationPolicyType::IfNeeded),
        Arc::new(Stamp(p_calls.clone())),
    );
    let state = state_on(backend, Some(plugin_with(vec![policy])));
    let cm = admit(&state, Operation::Create).await;
    assert_eq!(p_calls.load(Ordering::SeqCst), 2);
    assert_eq!(w_calls.load(Ordering::SeqCst), 2);
    let l = labels(&cm).unwrap();
    assert_eq!(l.get("w").map(String::as_str), Some("call-2"));
    assert_eq!(l.get("n").map(String::as_str), Some("2"));
}

/// dispatcher.go:115: nothing changed the object since the webhook's last
/// output (the policy rerun is idempotent), so the webhook is not rerun.
#[tokio::test]
#[serial]
async fn if_needed_webhook_is_not_reinvoked_when_nothing_changed_after_it() {
    let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
    let backend = Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new())));
    let (url, w_calls) = mock_webhook().await;
    install_webhook(&backend, url).await;
    let (policy, p_calls) = counting("p", Some(ReinvocationPolicyType::IfNeeded), "a");
    let state = state_on(backend, Some(plugin_with(vec![policy])));
    admit(&state, Operation::Create).await;
    assert_eq!(p_calls.load(Ordering::SeqCst), 2);
    assert_eq!(w_calls.load(Ordering::SeqCst), 1);
}
