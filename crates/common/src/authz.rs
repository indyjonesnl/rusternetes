use crate::auth::UserInfo;
use crate::error::Result;
use crate::field_selector::{FieldRequirement, FieldSelector};
use crate::label_selector::{LabelRequirement, LabelSelector};
use crate::resources::rbac::{
    ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, Subject,
};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::sync::Arc;

pub mod node_authorizer;
pub mod node_graph;
pub mod reasons;
pub use node_authorizer::{NodeAuthorizer, NodeAuthorizerFeatures};
use reasons::{aggregate_errors, applicable_subject, describe_subject, quoted};

/// Subject ↔ user matcher mirroring upstream
/// `pkg/registry/rbac/validation/rule.go` `appliesTo`:
///   * `User`           — `subject.name == user.username`
///   * `Group`          — `user.groups.contains(subject.name)`
///   * `ServiceAccount` — `user.username == "system:serviceaccount:<ns>:<name>"`,
///     where `<ns>` is `subject.namespace` and `<name>` is `subject.name`.
///
/// Defensive default for unknown `kind`: plain-name equality so existing
/// integration tests that synthesize subjects without a `kind` keep working.
fn subject_matches(subject: &Subject, user: &UserInfo) -> bool {
    match subject.kind.as_str() {
        "User" => subject.name == user.username,
        "Group" => user.groups.contains(&subject.name),
        "ServiceAccount" => {
            let ns = subject.namespace.as_deref().unwrap_or("");
            if ns.is_empty() {
                return false;
            }
            let expected = format!("system:serviceaccount:{}:{}", ns, subject.name);
            user.username == expected
        }
        _ => subject.name == user.username || user.groups.contains(&subject.name),
    }
}

/// Minimal storage trait for authorization (to avoid circular dependency)
#[async_trait]
pub trait AuthzStorage: Send + Sync {
    async fn get<T>(&self, key: &str, namespace: Option<&str>) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync;

    async fn list<T>(&self, namespace: Option<&str>) -> Result<Vec<T>>
    where
        T: serde::Serialize + DeserializeOwned + Send + Sync;
}

/// Request attributes for authorization
#[derive(Debug, Clone)]
pub struct RequestAttributes {
    /// The user making the request
    pub user: UserInfo,

    /// The verb being requested (get, list, create, update, delete, watch, etc.)
    pub verb: String,

    /// The namespace of the resource (None for cluster-scoped resources)
    pub namespace: Option<String>,

    /// The API group of the resource
    pub api_group: String,

    /// The API version of the resource (upstream `Attributes.GetAPIVersion`,
    /// `authorizer/interfaces.go`). Empty when the caller did not set it.
    pub api_version: String,

    /// The resource type (pods, services, deployments, etc.)
    pub resource: String,

    /// The specific resource name (None for list operations)
    pub name: Option<String>,

    /// The subresource being accessed (status, scale, etc.)
    pub subresource: Option<String>,

    /// Non-resource URL path (for non-resource requests like /healthz, /metrics)
    pub path: Option<String>,

    /// Whether this is a non-resource request
    pub is_non_resource_request: bool,

    /// `AttributesRecord.FieldSelectorRequirements`
    /// (apiserver/pkg/authorization/authorizer/interfaces.go): the parsed
    /// `fieldSelector` of a list/watch/deletecollection request.
    pub field_selector_requirements: Vec<FieldRequirement>,

    /// `AttributesRecord.FieldSelectorParsingErr`.
    pub field_selector_parsing_err: Option<String>,

    /// `AttributesRecord.LabelSelectorRequirements`.
    pub label_selector_requirements: Vec<LabelRequirement>,

    /// `AttributesRecord.LabelSelectorParsingErr`.
    pub label_selector_parsing_err: Option<String>,
}

impl RequestAttributes {
    pub fn new(user: UserInfo, verb: impl Into<String>, resource: impl Into<String>) -> Self {
        Self {
            user,
            verb: verb.into(),
            namespace: None,
            api_group: String::new(),
            api_version: String::new(),
            resource: resource.into(),
            name: None,
            subresource: None,
            path: None,
            is_non_resource_request: false,
            field_selector_requirements: Vec::new(),
            field_selector_parsing_err: None,
            label_selector_requirements: Vec::new(),
            label_selector_parsing_err: None,
        }
    }

    pub fn new_non_resource(
        user: UserInfo,
        verb: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            user,
            verb: verb.into(),
            namespace: None,
            api_group: String::new(),
            api_version: String::new(),
            resource: String::new(),
            name: None,
            subresource: None,
            path: Some(path.into()),
            is_non_resource_request: true,
            field_selector_requirements: Vec::new(),
            field_selector_parsing_err: None,
            label_selector_requirements: Vec::new(),
            label_selector_parsing_err: None,
        }
    }

    /// The `AuthorizeWithSelectors` block of `WithAuthorization`'s
    /// `GetAuthorizerAttributes` (apiserver/pkg/endpoints/filters/authorization.go:126-150).
    /// `field_selector` / `label_selector` are `RequestInfo.FieldSelector` /
    /// `.LabelSelector`. The gate is GA and `LockToDefault: true` since 1.34
    /// (apiserver/pkg/features/kube_features.go:353), so it is always on.
    pub fn set_selectors(&mut self, field_selector: &str, label_selector: &str) {
        // parsing here makes it easy to keep the attributes value-only
        // (authorization.go:128-129).
        if !field_selector.is_empty() {
            match FieldSelector::parse(field_selector) {
                Err(e) => {
                    self.field_selector_requirements = Vec::new();
                    self.field_selector_parsing_err = Some(e.to_string());
                }
                Ok(sel) => {
                    if !sel.requirements().is_empty() {
                        self.field_selector_requirements = sel.requirements().to_vec();
                        self.field_selector_parsing_err = None;
                    }
                }
            }
        }
        if !label_selector.is_empty() {
            match LabelSelector::parse(label_selector) {
                Err(e) => {
                    self.label_selector_requirements = Vec::new();
                    self.label_selector_parsing_err = Some(e.to_string());
                }
                Ok(sel) => {
                    if !sel.requirements().is_empty() {
                        self.label_selector_requirements = sel.requirements().to_vec();
                        self.label_selector_parsing_err = None;
                    }
                }
            }
        }
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn with_api_group(mut self, api_group: impl Into<String>) -> Self {
        self.api_group = api_group.into();
        self
    }

    pub fn with_api_version(mut self, api_version: impl Into<String>) -> Self {
        self.api_version = api_version.into();
        self
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_subresource(mut self, subresource: impl Into<String>) -> Self {
        self.subresource = Some(subresource.into());
        self
    }
}

/// Authorization decision
#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    Allow,
    Deny(String),
}

/// The three-valued answer of upstream's `authorizer.Decision`
/// (`staging/src/k8s.io/apiserver/pkg/authorization/authorizer/interfaces.go`:
/// `DecisionDeny` / `DecisionAllow` / `DecisionNoOpinion`). [`Decision`] has no
/// `NoOpinion` (an authorizer's `Deny` means "fall through" to the next one in
/// the [`UnionAuthorizer`]), so an authorizer that must be able to stop the
/// chain with an *explicit* deny, such as the authorization webhook
/// (`status.denied`, or `failurePolicy: Deny`), overrides
/// [`Authorizer::authorize_opinion`].
#[derive(Debug, Clone, PartialEq)]
pub enum Opinion {
    /// `DecisionAllow` with the authorizer's reason (e.g. RBAC's `RBAC:
    /// allowed by ...`), which `WithAuthorization` records as the
    /// `authorization.k8s.io/reason` audit annotation.
    Allow(String),
    /// An explicit deny: the union stops here (union.go returns on `Deny`).
    Deny(String),
    /// No decision; the union asks the next authorizer. `error` is the error
    /// upstream returns alongside `DecisionNoOpinion`; the union collects those
    /// and returns them only if no later authorizer decides.
    NoOpinion {
        reason: String,
        error: Option<String>,
    },
}

/// Upstream authorizer.Authorize returns decision, reason and error
/// independently (authorizer/interfaces.go:80-85). Keep the existing Opinion API
/// compatible while allowing a terminal decision to carry an evaluation error.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthorizationOutcome {
    pub opinion: Opinion,
    pub error: Option<String>,
}

impl AuthorizationOutcome {
    pub fn from_opinion(mut opinion: Opinion) -> Self {
        let error = match &mut opinion {
            Opinion::NoOpinion { error, .. } => error.take(),
            _ => None,
        };
        Self { opinion, error }
    }

    /// Adapt to the legacy opinion API, which has no error slot for a terminal
    /// decision. Consumers needing that error use authorize_outcome instead.
    pub fn into_opinion(mut self) -> Opinion {
        if let Opinion::NoOpinion { error, .. } = &mut self.opinion {
            *error = self.error.or(error.take());
        }
        self.opinion
    }

    /// Legacy binary consumers still allow an affirmative decision before
    /// considering its error (filters/authorization.go:79-87).
    pub fn into_decision(self) -> Result<Decision> {
        match self.opinion {
            Opinion::Allow(_) => Ok(Decision::Allow),
            Opinion::Deny(reason) => match self.error {
                Some(error) => Err(crate::Error::Authorization(error)),
                None => Ok(Decision::Deny(reason)),
            },
            Opinion::NoOpinion { reason, error } => match self.error.or(error) {
                Some(error) => Err(crate::Error::Authorization(error)),
                None => Ok(Decision::Deny(reason)),
            },
        }
    }
}

/// Authorizer trait for authorization implementations.
#[async_trait]
pub trait Authorizer: Send + Sync {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision>;

    /// Three-valued form of [`Authorizer::authorize`]. The default maps
    /// `Deny` to `NoOpinion`, the established meaning of `Deny` here.
    async fn authorize_opinion(&self, attrs: &RequestAttributes) -> Result<Opinion> {
        Ok(match self.authorize(attrs).await? {
            Decision::Allow => Opinion::Allow(String::new()),
            Decision::Deny(reason) => Opinion::NoOpinion {
                reason,
                error: None,
            },
        })
    }

    /// Full upstream decision/reason/error tuple. The default normalizes the
    /// existing NoOpinion error into the independent outcome error slot.
    async fn authorize_outcome(&self, attrs: &RequestAttributes) -> Result<AuthorizationOutcome> {
        self.authorize_opinion(attrs)
            .await
            .map(AuthorizationOutcome::from_opinion)
    }

    /// Get all rules that apply to a user in a given namespace
    async fn get_user_rules(
        &self,
        user: &UserInfo,
        namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )>;
}

/// RBAC Authorizer that uses Role and RoleBinding resources
pub struct RBACAuthorizer<S: AuthzStorage> {
    storage: Arc<S>,
}

impl<S: AuthzStorage> RBACAuthorizer<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// VisitRulesFor (validation/rule.go:179-235) visits cluster bindings
    /// first, then namespaced bindings; errors do not hide a later valid rule.
    async fn check_rbac(&self, attrs: &RequestAttributes) -> Result<Opinion> {
        let mut errors = Vec::new();
        match self.storage.list::<ClusterRoleBinding>(None).await {
            Err(error) => errors.push(error.to_string()),
            Ok(bindings) => {
                for binding in bindings {
                    let Some(subject) = applicable_subject(&binding.subjects, &attrs.user, "")
                    else {
                        continue;
                    };
                    match self.reference_rules(&binding.role_ref, "").await {
                        Err(error) => errors.push(error.to_string()),
                        Ok(rules) if self.check_policy_rules(&rules, attrs) => {
                            // authorizingVisitor.visit (rbac.go:63-80), with the
                            // clusterRoleBindingDescriber (rule.go:156-162).
                            return Ok(Opinion::Allow(format!(
                                "RBAC: allowed by ClusterRoleBinding {} of {} {} to {}",
                                quoted(&binding.metadata.name),
                                binding.role_ref.kind,
                                quoted(&binding.role_ref.name),
                                describe_subject(subject, "")
                            )));
                        }
                        Ok(_) => {}
                    }
                }
            }
        }
        if let Some(namespace) = attrs
            .namespace
            .as_deref()
            .filter(|namespace| !namespace.is_empty())
        {
            match self.storage.list::<RoleBinding>(Some(namespace)).await {
                Err(error) => errors.push(error.to_string()),
                Ok(bindings) => {
                    for binding in bindings {
                        let Some(subject) =
                            applicable_subject(&binding.subjects, &attrs.user, namespace)
                        else {
                            continue;
                        };
                        match self.reference_rules(&binding.role_ref, namespace).await {
                            Err(error) => errors.push(error.to_string()),
                            Ok(rules) if self.check_policy_rules(&rules, attrs) => {
                                // roleBindingDescriber (validation/rule.go:170-176).
                                let binding_namespace =
                                    binding.metadata.namespace.as_deref().unwrap_or("");
                                return Ok(Opinion::Allow(format!(
                                    "RBAC: allowed by RoleBinding {} of {} {} to {}",
                                    quoted(&format!(
                                        "{}/{binding_namespace}",
                                        binding.metadata.name
                                    )),
                                    binding.role_ref.kind,
                                    quoted(&binding.role_ref.name),
                                    describe_subject(subject, binding_namespace)
                                )));
                            }
                            Ok(_) => {}
                        }
                    }
                }
            }
        }
        // rbac.go:122-126: evaluation failures are a NoOpinion reason, never
        // a returned error, and a no-match without errors has an empty reason.
        let reason = aggregate_errors(&errors)
            .map(|error| format!("RBAC: {error}"))
            .unwrap_or_default();
        Ok(Opinion::NoOpinion {
            reason,
            error: None,
        })
    }

    /// GetRoleReferenceRules (validation/rule.go:240-258).
    async fn reference_rules(
        &self,
        reference: &crate::resources::rbac::RoleRef,
        namespace: &str,
    ) -> std::result::Result<Vec<PolicyRule>, String> {
        match reference.kind.as_str() {
            "Role" => self
                .storage
                .get::<Role>(&reference.name, Some(namespace))
                .await
                .map(|role| role.rules)
                .map_err(|error| error.to_string()),
            "ClusterRole" => self
                .storage
                .get::<ClusterRole>(&reference.name, None)
                .await
                .map(|role| role.rules)
                .map_err(|error| error.to_string()),
            _ => Err(format!(
                "unsupported role reference kind: {}",
                quoted(&reference.kind)
            )),
        }
    }

    /// Check if policy rules allow the requested action
    fn check_policy_rules(&self, rules: &[PolicyRule], attrs: &RequestAttributes) -> bool {
        rules.iter().any(|rule| self.rule_allows(rule, attrs))
    }

    /// Check if a single policy rule allows the requested action
    pub fn rule_allows(&self, rule: &PolicyRule, attrs: &RequestAttributes) -> bool {
        // Check verb
        if !rule.verbs.contains(&attrs.verb) && !rule.verbs.contains(&"*".to_string()) {
            return false;
        }

        // Handle non-resource requests
        if attrs.is_non_resource_request {
            if let Some(ref non_resource_urls) = rule.non_resource_urls {
                if let Some(ref path) = attrs.path {
                    // Check if the path matches any of the non-resource URLs
                    return non_resource_urls
                        .iter()
                        .any(|url| url == "*" || url == path || path.starts_with(url));
                }
            }
            return false;
        }

        // Check API group
        if let Some(ref api_groups) = rule.api_groups {
            if !api_groups.contains(&attrs.api_group) && !api_groups.contains(&"*".to_string()) {
                return false;
            }
        }

        // Check resource + subresource, mirroring upstream
        // `pkg/apis/rbac/v1` `resourceMatches`: `*` matches all; an exact rule
        // resource matches the COMBINED `resource/subresource` (or the bare
        // resource when there is no subresource); and `*/subresource` matches
        // that subresource on any resource. A rule on the bare resource does
        // NOT grant its subresources — so e.g. `system:kube-scheduler`'s
        // `create` on `pods/binding` requires the rule to name `pods/binding`,
        // and a request for resource=pods subresource=binding must be tested as
        // `pods/binding`, not `pods` (otherwise the scheduler's Bind is denied
        // and no pod is ever scheduled against a swapped api-server — #1667).
        if let Some(ref resources) = rule.resources {
            let sub = attrs.subresource.as_deref().filter(|s| !s.is_empty());
            let combined = match sub {
                Some(s) => format!("{}/{}", attrs.resource, s),
                None => attrs.resource.clone(),
            };
            let matched = resources.iter().any(|r| {
                r == "*" || *r == combined || matches!(sub, Some(s) if *r == format!("*/{s}"))
            });
            if !matched {
                return false;
            }
        }

        // Check resource name if specified
        if let Some(ref name) = attrs.name {
            if let Some(ref resource_names) = rule.resource_names {
                if !resource_names.contains(name) && !resource_names.contains(&"*".to_string()) {
                    return false;
                }
            }
        }

        true
    }
}

#[async_trait]
impl<S: AuthzStorage> Authorizer for RBACAuthorizer<S> {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        AuthorizationOutcome::from_opinion(self.check_rbac(attrs).await?).into_decision()
    }

    async fn authorize_opinion(&self, attrs: &RequestAttributes) -> Result<Opinion> {
        // RBAC has no username-only system:admin bypass; privileged groups are
        // a separate authorizer (builtin.go:82-96), before RBAC in the union.
        self.check_rbac(attrs).await
    }

    async fn get_user_rules(
        &self,
        user: &UserInfo,
        namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )> {
        let mut resource_rules = Vec::new();
        let mut non_resource_rules = Vec::new();

        // Get all role bindings that apply to this user in the namespace
        let role_bindings = self.storage.list::<RoleBinding>(Some(namespace)).await?;
        let cluster_role_bindings = self.storage.list::<ClusterRoleBinding>(None).await?;

        // Process namespace-scoped role bindings
        for binding in role_bindings {
            // Check if this binding applies to the user
            let applies = binding
                .subjects
                .iter()
                .any(|subject| subject_matches(subject, user));

            if !applies {
                continue;
            }

            // Get the role/cluster role referenced by this binding
            if binding.role_ref.kind == "Role" {
                if let Ok(role) = self
                    .storage
                    .get::<Role>(&binding.role_ref.name, Some(namespace))
                    .await
                {
                    // Convert PolicyRules to ResourceRules
                    for rule in &role.rules {
                        resource_rules.push(crate::resources::ResourceRule {
                            verbs: rule.verbs.clone(),
                            api_groups: rule.api_groups.clone(),
                            resources: rule.resources.clone(),
                            resource_names: rule.resource_names.clone(),
                        });
                    }
                }
            } else if binding.role_ref.kind == "ClusterRole" {
                if let Ok(cluster_role) = self
                    .storage
                    .get::<ClusterRole>(&binding.role_ref.name, None)
                    .await
                {
                    for rule in &cluster_role.rules {
                        resource_rules.push(crate::resources::ResourceRule {
                            verbs: rule.verbs.clone(),
                            api_groups: rule.api_groups.clone(),
                            resources: rule.resources.clone(),
                            resource_names: rule.resource_names.clone(),
                        });

                        // Also collect non-resource rules
                        if let Some(ref urls) = rule.non_resource_urls {
                            non_resource_rules.push(crate::resources::NonResourceRule {
                                verbs: rule.verbs.clone(),
                                non_resource_urls: Some(urls.clone()),
                            });
                        }
                    }
                }
            }
        }

        // Process cluster-scoped role bindings
        for binding in cluster_role_bindings {
            let applies = binding
                .subjects
                .iter()
                .any(|subject| subject_matches(subject, user));

            if !applies {
                continue;
            }

            if let Ok(cluster_role) = self
                .storage
                .get::<ClusterRole>(&binding.role_ref.name, None)
                .await
            {
                for rule in &cluster_role.rules {
                    resource_rules.push(crate::resources::ResourceRule {
                        verbs: rule.verbs.clone(),
                        api_groups: rule.api_groups.clone(),
                        resources: rule.resources.clone(),
                        resource_names: rule.resource_names.clone(),
                    });

                    if let Some(ref urls) = rule.non_resource_urls {
                        non_resource_rules.push(crate::resources::NonResourceRule {
                            verbs: rule.verbs.clone(),
                            non_resource_urls: Some(urls.clone()),
                        });
                    }
                }
            }
        }

        Ok((resource_rules, non_resource_rules))
    }
}

/// Always allow authorizer (for testing)
pub struct AlwaysAllowAuthorizer;

#[async_trait]
impl Authorizer for AlwaysAllowAuthorizer {
    async fn authorize(&self, _attrs: &RequestAttributes) -> Result<Decision> {
        Ok(Decision::Allow)
    }

    // authorizerfactory/builtin.go:32-33.
    async fn authorize_opinion(&self, _attrs: &RequestAttributes) -> Result<Opinion> {
        Ok(Opinion::Allow(String::new()))
    }

    async fn get_user_rules(
        &self,
        _user: &UserInfo,
        _namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )> {
        // Return wildcard rules for testing
        Ok((
            vec![crate::resources::ResourceRule {
                verbs: vec!["*".to_string()],
                api_groups: Some(vec!["*".to_string()]),
                resources: Some(vec!["*".to_string()]),
                resource_names: None,
            }],
            vec![crate::resources::NonResourceRule {
                verbs: vec!["*".to_string()],
                non_resource_urls: Some(vec!["*".to_string()]),
            }],
        ))
    }
}

/// Always deny authorizer (for testing)
pub struct AlwaysDenyAuthorizer;

#[async_trait]
impl Authorizer for AlwaysDenyAuthorizer {
    async fn authorize(&self, _attrs: &RequestAttributes) -> Result<Decision> {
        Ok(Decision::Deny("Everything is forbidden.".to_string()))
    }

    // authorizerfactory/builtin.go:60-61.
    async fn authorize_opinion(&self, _attrs: &RequestAttributes) -> Result<Opinion> {
        Ok(Opinion::NoOpinion {
            reason: "Everything is forbidden.".into(),
            error: None,
        })
    }

    async fn get_user_rules(
        &self,
        _user: &UserInfo,
        _namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )> {
        // Return empty rules for testing
        Ok((vec![], vec![]))
    }
}

/// Node Authorizer - Authorizes kubelet requests to access node-related resources
///
/// Implements node authorization according to Kubernetes node authorizer specification.
/// Nodes can only access resources that are bound to them or required for their operation.
/// Static node rules, ported from upstream
/// `plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go` `NodeRules()`.
///
/// Upstream's Node authorizer subdivides access for the resources it can tie to a
/// node through its graph (secrets, configmaps, PVCs, leases, …) and then falls
/// through to these statically defined rules for everything else
/// (`plugin/pkg/auth/authorizer/node/node_authorizer.go`: "Access to other
/// resources is not subdivided, so just evaluate against the statically defined
/// node rules"). Pods are in that fall-through unless the
/// `AuthorizeNodeWithSelectors` feature gate is on, which is why a kubelet's
/// cluster-scoped `LIST pods?fieldSelector=spec.nodeName=…` is allowed.
///
/// Without these, a vanilla kubelet against this api-server never learns about its
/// own pods and starts nothing:
///
/// ```text
/// Failed to watch: failed to list *v1.Pod: Node <name> is not authorized to list
/// pods in namespace ; User does not have permission to perform this action
/// ```
///
/// Entries are `(apiGroup, resource[/subresource], verbs)`.
const NODE_RULES: &[(&str, &str, &[&str])] = &[
    ("authentication.k8s.io", "tokenreviews", &["create"]),
    ("authorization.k8s.io", "subjectaccessreviews", &["create"]),
    (
        "authorization.k8s.io",
        "localsubjectaccessreviews",
        &["create"],
    ),
    ("", "services", &["get", "list", "watch"]),
    (
        "",
        "nodes",
        &["create", "get", "list", "watch", "update", "patch"],
    ),
    ("", "nodes/status", &["update", "patch"]),
    ("", "events", &["create", "update", "patch"]),
    ("events.k8s.io", "events", &["create", "update", "patch"]),
    // "Needed for the node to create/delete mirror pods. Use the NodeRestriction
    // admission plugin to limit a node to creating/deleting mirror pods bound to
    // itself." (policy.go:215-217). The grant is deliberately unconditional here,
    // as it is upstream: the narrowing lives in the admission plugin, ported in
    // api-server `handlers::node_restriction` — a node may only create a mirror
    // pod owned by and bound to itself, and only delete a pod bound to itself
    // (#1721, #1906). Read access is separate (policy.go:213).
    //
    // "create" on pods/eviction (policy.go:223) is still omitted: the eviction
    // path has no NodeRestriction equivalent yet, so granting it would let a node
    // evict any pod.
    ("", "pods", &["get", "list", "watch", "create", "delete"]),
    ("", "pods/status", &["update", "patch"]),
    ("", "secrets", &["get", "list", "watch"]),
    ("", "configmaps", &["get", "list", "watch"]),
    ("", "persistentvolumeclaims", &["get"]),
    (
        "",
        "persistentvolumeclaims/status",
        &["get", "update", "patch"],
    ),
    ("", "persistentvolumes", &["get"]),
    ("", "endpoints", &["get"]),
    ("", "serviceaccounts/token", &["create"]),
    (
        "certificates.k8s.io",
        "certificatesigningrequests",
        &["create", "get", "list", "watch"],
    ),
    (
        "coordination.k8s.io",
        "leases",
        &["get", "create", "update", "patch", "delete"],
    ),
    ("storage.k8s.io", "volumeattachments", &["get"]),
    ("storage.k8s.io", "csidrivers", &["get", "list", "watch"]),
    (
        "storage.k8s.io",
        "csinodes",
        &["get", "create", "update", "patch", "delete"],
    ),
    ("node.k8s.io", "runtimeclasses", &["get", "list", "watch"]),
];

/// True when the request matches one of the static [`NODE_RULES`].
pub fn node_rules_allow(attrs: &RequestAttributes) -> bool {
    let target = match attrs.subresource.as_deref() {
        Some(sub) if !sub.is_empty() => format!("{}/{}", attrs.resource, sub),
        _ => attrs.resource.clone(),
    };
    NODE_RULES.iter().any(|(group, resource, verbs)| {
        *group == attrs.api_group.as_str()
            && *resource == target.as_str()
            && verbs.contains(&attrs.verb.as_str())
    })
}

/// Upstream `authorization/union/union.go:45-68`: return the first terminal
/// Allow/Deny tuple; otherwise aggregate abstention reasons and errors.
/// Legacy binary authorizers map Deny to NoOpinion through the trait adapter.
pub struct UnionAuthorizer {
    authorizers: Vec<Arc<dyn Authorizer>>,
}

impl UnionAuthorizer {
    pub fn new(authorizers: Vec<Arc<dyn Authorizer>>) -> Self {
        Self { authorizers }
    }
}

#[async_trait]
impl Authorizer for UnionAuthorizer {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        self.authorize_outcome(attrs).await?.into_decision()
    }

    async fn authorize_opinion(&self, attrs: &RequestAttributes) -> Result<Opinion> {
        Ok(self.authorize_outcome(attrs).await?.into_opinion())
    }

    async fn authorize_outcome(&self, attrs: &RequestAttributes) -> Result<AuthorizationOutcome> {
        // union.go:45-68: aggregate only abstentions. A terminal result owns
        // its reason AND error, even when previous delegates failed.
        let mut reasons = Vec::new();
        let mut errors = Vec::new();
        for authorizer in &self.authorizers {
            let outcome = match authorizer.authorize_outcome(attrs).await {
                Ok(outcome) => outcome,
                Err(error) => AuthorizationOutcome {
                    opinion: Opinion::NoOpinion {
                        reason: String::new(),
                        error: None,
                    },
                    error: Some(error.to_string()),
                },
            };
            match outcome.opinion {
                Opinion::NoOpinion { reason, error } => {
                    if !reason.is_empty() {
                        reasons.push(reason);
                    }
                    errors.extend(outcome.error.or(error));
                }
                _ => return Ok(outcome),
            }
        }
        Ok(AuthorizationOutcome {
            opinion: Opinion::NoOpinion {
                reason: reasons.join("\n"),
                error: None,
            },
            error: aggregate_errors(&errors),
        })
    }

    async fn get_user_rules(
        &self,
        user: &UserInfo,
        namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )> {
        let mut resource_rules = Vec::new();
        let mut non_resource_rules = Vec::new();
        for authorizer in &self.authorizers {
            if let Ok((rr, nrr)) = authorizer.get_user_rules(user, namespace).await {
                resource_rules.extend(rr);
                non_resource_rules.extend(nrr);
            }
        }
        Ok((resource_rules, non_resource_rules))
    }
}

/// `user.SystemPrivilegedGroup`
/// (`staging/src/k8s.io/apiserver/pkg/authentication/user/user.go:71`).
pub const SYSTEM_PRIVILEGED_GROUP: &str = "system:masters";

/// Port of `privilegedGroupAuthorizer`
/// (`staging/src/k8s.io/apiserver/pkg/authorization/authorizerfactory/builtin.go:68-96`):
/// allows any request whose user is in one of the configured groups. Upstream
/// returns `DecisionNoOpinion` otherwise; our [`Decision`] has no `NoOpinion`,
/// and [`UnionAuthorizer`] already treats `Deny` as "fall through", so `Deny`
/// is the equivalent. Like upstream it is not a rule resolver (it is not
/// appended to `ruleResolvers` in `reload.go`), so it reports no rules.
pub struct PrivilegedGroupAuthorizer {
    groups: Vec<String>,
}

impl PrivilegedGroupAuthorizer {
    /// `NewPrivilegedGroups`.
    pub fn new(groups: &[&str]) -> Self {
        Self {
            groups: groups.iter().map(|g| g.to_string()).collect(),
        }
    }
}

#[async_trait]
impl Authorizer for PrivilegedGroupAuthorizer {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        if attrs
            .user
            .groups
            .iter()
            .any(|g| self.groups.iter().any(|p| p == g))
        {
            return Ok(Decision::Allow);
        }
        Ok(Decision::Deny(String::new()))
    }

    // authorizerfactory/builtin.go:82-96.
    async fn authorize_opinion(&self, attrs: &RequestAttributes) -> Result<Opinion> {
        Ok(match self.authorize(attrs).await? {
            Decision::Allow => Opinion::Allow(String::new()),
            Decision::Deny(_) => Opinion::NoOpinion {
                reason: String::new(),
                error: None,
            },
        })
    }

    async fn get_user_rules(
        &self,
        _user: &UserInfo,
        _namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )> {
        Ok((vec![], vec![]))
    }
}

/// `newForConfig` (`pkg/kubeapiserver/authorizer/reload.go:97-99`): the
/// `system:masters` superuser authorizer is always first in the chain, ahead of
/// the configured modes (Node, RBAC, ...). This is in addition to the
/// `cluster-admin` -> `system:masters` binding the bootstrap seeds, exactly as
/// upstream has both.
pub fn superuser_then(rest: Vec<Arc<dyn Authorizer>>) -> UnionAuthorizer {
    let mut authorizers: Vec<Arc<dyn Authorizer>> =
        vec![Arc::new(PrivilegedGroupAuthorizer::new(&[
            SYSTEM_PRIVILEGED_GROUP,
        ]))];
    authorizers.extend(rest);
    UnionAuthorizer::new(authorizers)
}

/// Webhook Authorizer with full HTTP integration
pub struct WebhookAuthorizer {
    webhook_url: String,
    http_client: reqwest::Client,
}

impl WebhookAuthorizer {
    pub fn new(webhook_url: String, ca_cert: Option<String>) -> crate::error::Result<Self> {
        let http_client = if let Some(ref cert_pem) = ca_cert {
            // Build client with custom CA certificate
            let cert = reqwest::Certificate::from_pem(cert_pem.as_bytes()).map_err(|e| {
                crate::error::Error::Internal(format!("Failed to parse CA certificate: {}", e))
            })?;

            reqwest::Client::builder()
                .add_root_certificate(cert)
                .build()
                .map_err(|e| {
                    crate::error::Error::Internal(format!("Failed to create HTTP client: {}", e))
                })?
        } else {
            reqwest::Client::new()
        };

        Ok(Self {
            webhook_url,
            http_client,
        })
    }
}

#[async_trait]
impl Authorizer for WebhookAuthorizer {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        use crate::resources::authorization::{
            NonResourceAttributes, ResourceAttributes as SARResourceAttributes,
            SubjectAccessReview, SubjectAccessReviewSpec,
        };
        use crate::types::ObjectMeta;

        // Create SubjectAccessReview request
        let spec = if attrs.is_non_resource_request {
            SubjectAccessReviewSpec {
                resource_attributes: None,
                non_resource_attributes: Some(NonResourceAttributes {
                    path: attrs.path.clone(),
                    verb: Some(attrs.verb.clone()),
                }),
                user: Some(attrs.user.username.clone()),
                groups: Some(attrs.user.groups.clone()),
                extra: Some(attrs.user.extra.clone()),
                uid: Some(attrs.user.uid.clone()),
            }
        } else {
            SubjectAccessReviewSpec {
                resource_attributes: Some(SARResourceAttributes {
                    namespace: attrs.namespace.clone(),
                    verb: Some(attrs.verb.clone()),
                    group: Some(attrs.api_group.clone()),
                    version: None,
                    resource: Some(attrs.resource.clone()),
                    subresource: attrs.subresource.clone(),
                    name: attrs.name.clone(),
                    field_selector: None,
                    label_selector: None,
                }),
                non_resource_attributes: None,
                user: Some(attrs.user.username.clone()),
                groups: Some(attrs.user.groups.clone()),
                extra: Some(attrs.user.extra.clone()),
                uid: Some(attrs.user.uid.clone()),
            }
        };

        let sar = SubjectAccessReview {
            api_version: "authorization.k8s.io/v1".to_string(),
            kind: "SubjectAccessReview".to_string(),
            metadata: ObjectMeta::new(""),
            spec,
            status: None,
        };

        // Send request to webhook
        let response = self
            .http_client
            .post(&self.webhook_url)
            .json(&sar)
            .send()
            .await
            .map_err(|e| {
                crate::error::Error::Authorization(format!("Webhook request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Ok(Decision::Deny(format!(
                "Webhook returned error status: {}",
                response.status()
            )));
        }

        // Parse response
        let sar_response: SubjectAccessReview = response.json().await.map_err(|e| {
            crate::error::Error::Authorization(format!("Failed to parse webhook response: {}", e))
        })?;

        // Check if authorization succeeded
        let status = sar_response.status.ok_or_else(|| {
            crate::error::Error::Authorization("Webhook response missing status".to_string())
        })?;

        if status.allowed {
            Ok(Decision::Allow)
        } else {
            Ok(Decision::Deny(
                status
                    .reason
                    .unwrap_or_else(|| "Authorization denied".to_string()),
            ))
        }
    }

    async fn get_user_rules(
        &self,
        _user: &UserInfo,
        _namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )> {
        // Webhook authorizers don't support listing user rules
        // This would require a different API call or mechanism
        Ok((vec![], vec![]))
    }
}
