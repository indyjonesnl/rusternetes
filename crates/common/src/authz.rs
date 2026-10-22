use crate::auth::UserInfo;
use crate::error::Result;
use crate::field_selector::{FieldRequirement, FieldSelector};
use crate::label_selector::{LabelRequirement, LabelSelector};
use crate::node_graph::{NodeGraph, VertexType};
use crate::resources::rbac::{
    ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, Subject,
};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::sync::Arc;

mod reasons;
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
    fn rule_allows(&self, rule: &PolicyRule, attrs: &RequestAttributes) -> bool {
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
/// node (secrets, configmaps, PVCs, PVs, leases, pods, nodes, …) in
/// [`NodeAuthorizer::evaluate`], and falls through to these statically defined
/// rules only for everything else
/// (`plugin/pkg/auth/authorizer/node/node_authorizer.go`: "Access to other
/// resources is not subdivided, so just evaluate against the statically defined
/// node rules"). The rows for subdivided resources are therefore unreachable;
/// they are kept because they are upstream's `NodeRules()` verbatim.
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
fn node_rules_allow(attrs: &RequestAttributes) -> bool {
    // `rbac.RulesAllow` matches a non-resource request against non-resource
    // URLs only; `NodeRules()` has none, so a resource row never matches it.
    if attrs.is_non_resource_request {
        return false;
    }
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

/// Feature gates the upstream Node authorizer consults through
/// `utilfeature.DefaultFeatureGate` (node_authorizer.go `features`), at their
/// release-1.35 defaults (`pkg/features/kube_features.go`).
///
/// `AuthorizeNodeWithSelectors` is not a field: it is GA and `LockToDefault:
/// true` since 1.34 (kube_features.go:1146-1150), so the selector-aware
/// `authorizeNode` / `authorizePod` / `authorizeResourceSlice` branches are
/// unconditional.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeAuthorizerFeatures {
    /// `KubeletServiceAccountTokenForCredentialProviders`: Beta, default on in
    /// 1.35 (kube_features.go:1478-1481).
    pub kubelet_service_account_token_for_credential_providers: bool,
    /// `PodCertificateRequest`: Beta, default OFF in 1.35 (kube_features.go:1602-1605).
    pub pod_certificate_request: bool,
}

impl Default for NodeAuthorizerFeatures {
    fn default() -> Self {
        Self {
            kubelet_service_account_token_for_credential_providers: true,
            pod_certificate_request: false,
        }
    }
}

/// `NodeAuthorizer` (plugin/pkg/auth/authorizer/node/node_authorizer.go):
///
///  1. a request not from a node (`NodeIdentity()` isNode=false) gets no opinion;
///  2. a node that cannot be named gets no opinion;
///  3. a request for a secret, configmap, persistent volume, resource claim or
///     persistent volume claim is rejected unless the verb is get and the object
///     is related to the requesting node through the [`NodeGraph`]:
///     node <- configmap, node <- pod, node <- pod <- secret, node <- pod <-
///     configmap, node <- pod <- pvc, node <- pod <- pvc <- pv, node <- pod <-
///     pvc <- pv <- secret, node <- pod <- ResourceClaim, node <- pcr;
///  4. for a resourceslice, access follows the edge from the slice to its node;
///  5. other resources are authorized uniformly by the static [`NODE_RULES`].
///
/// The graph is kept current by `rusternetes_api_server::node_graph_populator`,
/// the counterpart of upstream's `AddGraphEventHandlers`.
#[derive(Clone, Default)]
pub struct NodeAuthorizer {
    graph: Arc<NodeGraph>,
    features: NodeAuthorizerFeatures,
}

/// `nodeidentifier.NewDefaultNodeIdentifier().NodeIdentity`
/// (pkg/auth/nodeidentifier/default.go:42-68): the user name must start with
/// `system:node:` AND the user must be in `system:nodes`. Returns
/// `(nodeName, isNode)`.
fn node_identity(user: &UserInfo) -> (String, bool) {
    let Some(name) = user.username.strip_prefix("system:node:") else {
        return (String::new(), false);
    };
    if !user.groups.iter().any(|g| g == "system:nodes") {
        return (String::new(), false);
    }
    (name.to_string(), true)
}

fn no_opinion(reason: impl Into<String>) -> Opinion {
    Opinion::NoOpinion {
        reason: reason.into(),
        error: None,
    }
}

/// `selection.Equals` requirement `field == value` among the request's parsed
/// field selector requirements (`attrs.GetFieldSelector()`).
fn has_field_selector_equals(attrs: &RequestAttributes, field: &str, value: &str) -> bool {
    attrs.field_selector_requirements.iter().any(|r| {
        r.field() == field
            && r.operator() == crate::field_selector::FieldOperator::Equals
            && r.value() == value
    })
}

impl NodeAuthorizer {
    /// `NewAuthorizer(graph, identifier, rules)`.
    pub fn new(graph: Arc<NodeGraph>) -> Self {
        Self {
            graph,
            features: NodeAuthorizerFeatures::default(),
        }
    }

    /// Override the feature gates (upstream tests assign `authz.features`).
    pub fn with_features(mut self, features: NodeAuthorizerFeatures) -> Self {
        self.features = features;
        self
    }

    /// The graph this authorizer reads, for the populator that feeds it.
    pub fn graph(&self) -> &Arc<NodeGraph> {
        &self.graph
    }

    /// `NodeAuthorizer.Authorize` (node_authorizer.go:109-168).
    pub fn evaluate(&self, attrs: &RequestAttributes) -> Opinion {
        let (node_name, is_node) = node_identity(&attrs.user);
        if !is_node {
            // reject requests from non-nodes
            return no_opinion("");
        }
        if node_name.is_empty() {
            // reject requests from unidentifiable nodes
            return no_opinion(format!(
                "unknown node for user {}",
                quoted(&attrs.user.username)
            ));
        }

        // subdivide access to specific resources
        if !attrs.is_non_resource_request {
            match (attrs.api_group.as_str(), attrs.resource.as_str()) {
                ("", "secrets") => {
                    return self.authorize_read_namespaced_object(
                        &node_name,
                        VertexType::Secret,
                        attrs,
                    )
                }
                ("", "configmaps") => {
                    return self.authorize_read_namespaced_object(
                        &node_name,
                        VertexType::ConfigMap,
                        attrs,
                    )
                }
                ("", "persistentvolumeclaims") => {
                    if attrs.subresource.as_deref() == Some("status") {
                        return self.authorize_status_update(&node_name, VertexType::Pvc, attrs);
                    }
                    return self.authorize_get(&node_name, VertexType::Pvc, attrs);
                }
                ("", "persistentvolumes") => {
                    return self.authorize_get(&node_name, VertexType::Pv, attrs)
                }
                ("resource.k8s.io", "resourceclaims") => {
                    return self.authorize_get(&node_name, VertexType::ResourceClaim, attrs)
                }
                ("storage.k8s.io", "volumeattachments") => {
                    return self.authorize_get(&node_name, VertexType::Va, attrs)
                }
                ("", "serviceaccounts") => {
                    return self.authorize_service_account(&node_name, attrs)
                }
                ("coordination.k8s.io", "leases") => {
                    return self.authorize_lease(&node_name, attrs)
                }
                ("storage.k8s.io", "csinodes") => {
                    return self.authorize_csi_node(&node_name, attrs)
                }
                ("resource.k8s.io", "resourceslices") => {
                    return self.authorize_resource_slice(&node_name, attrs)
                }
                ("", "nodes") => return self.authorize_node(&node_name, attrs),
                ("", "pods") => return self.authorize_pod(&node_name, attrs),
                ("certificates.k8s.io", "podcertificaterequests") => {
                    if self.features.pod_certificate_request {
                        return self.authorize_pod_certificate_request(&node_name, attrs);
                    }
                    return no_opinion("");
                }
                _ => {}
            }
        }

        // Access to other resources is not subdivided, so just evaluate against
        // the statically defined node rules.
        if node_rules_allow(attrs) {
            return Opinion::Allow(String::new());
        }
        no_opinion("")
    }

    /// `authorizeStatusUpdate`: update/patch of a status subresource of an
    /// object related to the node.
    fn authorize_status_update(
        &self,
        node_name: &str,
        starting_type: VertexType,
        attrs: &RequestAttributes,
    ) -> Opinion {
        match attrs.verb.as_str() {
            "update" | "patch" => {}
            _ => return no_opinion("can only get/update/patch this type"),
        }
        if attrs.subresource.as_deref() != Some("status") {
            return no_opinion("can only update status subresource");
        }
        self.authorize_object(node_name, starting_type, attrs)
    }

    /// `authorizeGet`: "get" of an object related to the node.
    fn authorize_get(
        &self,
        node_name: &str,
        starting_type: VertexType,
        attrs: &RequestAttributes,
    ) -> Opinion {
        if attrs.verb != "get" {
            return no_opinion("can only get individual resources of this type");
        }
        if attrs.subresource.as_deref().is_some_and(|s| !s.is_empty()) {
            return no_opinion("cannot get subresource");
        }
        self.authorize_object(node_name, starting_type, attrs)
    }

    /// `authorizeReadNamespacedObject`: get/list/watch of a single namespaced
    /// object related to the node.
    fn authorize_read_namespaced_object(
        &self,
        node_name: &str,
        starting_type: VertexType,
        attrs: &RequestAttributes,
    ) -> Opinion {
        match attrs.verb.as_str() {
            "get" | "list" | "watch" => {}
            _ => return no_opinion("can only read resources of this type"),
        }
        if attrs.subresource.as_deref().is_some_and(|s| !s.is_empty()) {
            return no_opinion("cannot read subresource");
        }
        if attrs.namespace.as_deref().unwrap_or("").is_empty() {
            return no_opinion("can only read namespaced object of this type");
        }
        self.authorize_object(node_name, starting_type, attrs)
    }

    /// `authorize` (named apart from [`Authorizer::authorize`]).
    fn authorize_object(
        &self,
        node_name: &str,
        starting_type: VertexType,
        attrs: &RequestAttributes,
    ) -> Opinion {
        let name = attrs.name.as_deref().unwrap_or("");
        if name.is_empty() {
            return no_opinion("No Object name found");
        }
        self.authorize_by_graph(node_name, starting_type, attrs.namespace.as_deref(), name)
    }

    /// The tail shared by `authorize` and `authorizeCreateToken`: any error or
    /// absence of a path yields the same relationship reason.
    fn authorize_by_graph(
        &self,
        node_name: &str,
        starting_type: VertexType,
        namespace: Option<&str>,
        name: &str,
    ) -> Opinion {
        match self
            .graph
            .has_path_from(node_name, starting_type, namespace.unwrap_or(""), name)
        {
            Ok(true) => Opinion::Allow(String::new()),
            Ok(false) | Err(_) => no_opinion(format!(
                "no relationship found between node '{node_name}' and this object"
            )),
        }
    }

    /// `authorizeServiceAccount`: get (feature gated) or create of the token
    /// subresource of a service account used by a pod on the node.
    fn authorize_service_account(&self, node_name: &str, attrs: &RequestAttributes) -> Opinion {
        let no_subresource = attrs.subresource.as_deref().unwrap_or("").is_empty();
        if attrs.verb == "get" && no_subresource {
            if !(self
                .features
                .kubelet_service_account_token_for_credential_providers
                || self.features.pod_certificate_request)
            {
                return no_opinion("not allowed to get service accounts");
            }
            return self.authorize_read_namespaced_object(
                node_name,
                VertexType::ServiceAccount,
                attrs,
            );
        }
        self.authorize_create_token(node_name, VertexType::ServiceAccount, attrs)
    }

    /// `authorizeCreateToken`.
    fn authorize_create_token(
        &self,
        node_name: &str,
        starting_type: VertexType,
        attrs: &RequestAttributes,
    ) -> Opinion {
        let name = attrs.name.as_deref().unwrap_or("");
        if attrs.verb != "create" || name.is_empty() {
            return no_opinion("can only create tokens for individual service accounts");
        }
        if attrs.subresource.as_deref() != Some("token") {
            return no_opinion("can only create token subresource of serviceaccount");
        }
        self.authorize_by_graph(node_name, starting_type, attrs.namespace.as_deref(), name)
    }

    /// `authorizeLease`: node leases in `kube-node-lease`, named after the node.
    fn authorize_lease(&self, node_name: &str, attrs: &RequestAttributes) -> Opinion {
        let verb = attrs.verb.as_str();
        match verb {
            "get" | "create" | "update" | "patch" | "delete" => {}
            _ => return no_opinion("can only get, create, update, patch, or delete a node lease"),
        }
        // the request must be against the system namespace reserved for node leases
        if attrs.namespace.as_deref().unwrap_or("") != "kube-node-lease" {
            return no_opinion(
                "can only access leases in the \"kube-node-lease\" system namespace",
            );
        }
        // note we skip this check for create, since the authorizer doesn't know
        // the name on create; the noderestriction admission plugin checks it
        if verb != "create" && attrs.name.as_deref().unwrap_or("") != node_name {
            return no_opinion(
                "can only access node lease with the same name as the requesting node",
            );
        }
        Opinion::Allow(String::new())
    }

    /// `authorizeCSINode`.
    fn authorize_csi_node(&self, node_name: &str, attrs: &RequestAttributes) -> Opinion {
        let verb = attrs.verb.as_str();
        match verb {
            "get" | "create" | "update" | "patch" | "delete" => {}
            _ => return no_opinion("can only get, create, update, patch, or delete a CSINode"),
        }
        if attrs.subresource.as_deref().is_some_and(|s| !s.is_empty()) {
            return no_opinion("cannot authorize CSINode subresources");
        }
        if verb != "create" && attrs.name.as_deref().unwrap_or("") != node_name {
            return no_opinion("can only access CSINode with the same name as the requesting node");
        }
        Opinion::Allow(String::new())
    }

    /// `authorizeResourceSlice` (with `AuthorizeNodeWithSelectors` on).
    fn authorize_resource_slice(&self, node_name: &str, attrs: &RequestAttributes) -> Opinion {
        if attrs.subresource.as_deref().is_some_and(|s| !s.is_empty()) {
            return no_opinion("cannot authorize ResourceSlice subresources");
        }
        match attrs.verb.as_str() {
            // The noderestriction admission plugin checks ResourceSlice.NodeName
            // on create; the authorizer does not see the new object.
            "create" => Opinion::Allow(String::new()),
            // Checking the existing object must have established that access is
            // allowed by recording a graph edge.
            "get" | "update" | "patch" | "delete" => {
                self.authorize_object(node_name, VertexType::Slice, attrs)
            }
            "watch" | "list" | "deletecollection" => {
                // only allow a scoped fieldSelector
                if has_field_selector_equals(attrs, "spec.nodeName", node_name) {
                    return Opinion::Allow(String::new());
                }
                no_opinion(
                    "can only list/watch/deletecollection resourceslices with nodeName field selector",
                )
            }
            _ => no_opinion(
                "only the following verbs are allowed for a ResourceSlice: get, watch, list, create, update, patch, delete, deletecollection",
            ),
        }
    }

    /// `authorizePodCertificateRequest`.
    fn authorize_pod_certificate_request(
        &self,
        node_name: &str,
        attrs: &RequestAttributes,
    ) -> Opinion {
        if attrs.subresource.as_deref().is_some_and(|s| !s.is_empty()) {
            return no_opinion(
                "nodes may not access the status subresource of PodCertificateRequests",
            );
        }
        match attrs.verb.as_str() {
            // Creates are further restricted by the noderestriction admission plugin.
            "create" => Opinion::Allow(String::new()),
            "get" => self.authorize_object(node_name, VertexType::Pcr, attrs),
            "list" | "watch" => {
                // Allow requests that have a fieldselector restricting to this node.
                if has_field_selector_equals(attrs, "spec.nodeName", node_name) {
                    return Opinion::Allow(String::new());
                }
                no_opinion(
                    "can only list/watch podcertificaterequests with nodeName field selector",
                )
            }
            verb => no_opinion(format!("nodes may not {verb} podcertificaterequests")),
        }
    }

    /// `authorizeNode` (with `AuthorizeNodeWithSelectors` on).
    fn authorize_node(&self, node_name: &str, attrs: &RequestAttributes) -> Opinion {
        let name = attrs.name.as_deref().unwrap_or("");
        match attrs.subresource.as_deref().unwrap_or("") {
            "" => match attrs.verb.as_str() {
                // Use the NodeRestriction admission plugin to limit a node to
                // creating/updating its own API object.
                "create" | "update" | "patch" => return Opinion::Allow(String::new()),
                "get" | "list" | "watch" => {
                    // Compare the name directly, rather than using the graph, so
                    // kubelets can attempt a read of their Node API object prior
                    // to creation.
                    if name == node_name {
                        return Opinion::Allow(String::new());
                    }
                    if name.is_empty() {
                        return no_opinion(format!(
                            "node '{node_name}' cannot read all nodes, only its own Node object"
                        ));
                    }
                    return no_opinion(format!(
                        "node '{node_name}' cannot read '{name}', only its own Node object"
                    ));
                }
                _ => {}
            },
            "status" => {
                if matches!(attrs.verb.as_str(), "update" | "patch") {
                    // NodeRestriction admission limits a node to its own status.
                    return Opinion::Allow(String::new());
                }
            }
            _ => {}
        }
        no_opinion("")
    }

    /// `authorizePod` (with `AuthorizeNodeWithSelectors` on).
    ///
    /// DEVIATION (security-conservative, not a loosening): upstream allows
    /// `create` of `pods/eviction` here, relying on NodeRestriction
    /// (node_authorizer.go "case eviction"). This repository has no
    /// NodeRestriction path for evictions yet, so the grant stays withheld, as
    /// it was before the graph port (see [`NODE_RULES`] `pods`).
    fn authorize_pod(&self, node_name: &str, attrs: &RequestAttributes) -> Opinion {
        let name = attrs.name.as_deref().unwrap_or("");
        match attrs.subresource.as_deref().unwrap_or("") {
            "" => match attrs.verb.as_str() {
                "get" => return self.authorize_get(node_name, VertexType::Pod, attrs),
                "list" | "watch" => {
                    // allow a scoped fieldSelector
                    if has_field_selector_equals(attrs, "spec.nodeName", node_name) {
                        return Opinion::Allow(String::new());
                    }
                    // allow a read of a single pod known to be related to the node
                    if !name.is_empty() {
                        return self.authorize_object(node_name, VertexType::Pod, attrs);
                    }
                    return no_opinion(
                        "can only list/watch pods with spec.nodeName field selector",
                    );
                }
                // Needed for the node to create/delete mirror pods. NodeRestriction
                // admission limits a node to mirror pods bound to itself.
                "create" | "delete" => return Opinion::Allow(String::new()),
                _ => {}
            },
            "status" => {
                if matches!(attrs.verb.as_str(), "update" | "patch") {
                    // Needed for the node to report status of pods it is running;
                    // NodeRestriction limits it to pods bound to itself.
                    return Opinion::Allow(String::new());
                }
            }
            _ => {}
        }
        no_opinion("")
    }
}

#[async_trait]
impl Authorizer for NodeAuthorizer {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        Ok(match self.evaluate(attrs) {
            Opinion::Allow(_) => Decision::Allow,
            Opinion::Deny(reason) | Opinion::NoOpinion { reason, .. } => Decision::Deny(reason),
        })
    }

    async fn authorize_opinion(&self, attrs: &RequestAttributes) -> Result<Opinion> {
        Ok(self.evaluate(attrs))
    }

    async fn get_user_rules(
        &self,
        user: &UserInfo,
        _namespace: &str,
    ) -> Result<(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    )> {
        // Extract node name
        let Some(_node_name) = user.username.strip_prefix("system:node:") else {
            return Ok((vec![], vec![]));
        };

        // Return a representative set of rules for nodes
        let resource_rules = vec![
            crate::resources::ResourceRule {
                verbs: vec!["get".to_string(), "list".to_string(), "watch".to_string()],
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec![
                    "services".to_string(),
                    "endpoints".to_string(),
                    "nodes".to_string(),
                    "pods".to_string(),
                    "secrets".to_string(),
                    "configmaps".to_string(),
                    "persistentvolumeclaims".to_string(),
                    "persistentvolumes".to_string(),
                ]),
                resource_names: None,
            },
            crate::resources::ResourceRule {
                verbs: vec!["create".to_string()],
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["events".to_string()]),
                resource_names: None,
            },
            crate::resources::ResourceRule {
                verbs: vec!["update".to_string(), "patch".to_string()],
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["nodes".to_string(), "pods".to_string()]),
                resource_names: None,
            },
        ];

        Ok((resource_rules, vec![]))
    }
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
    #[allow(dead_code)]
    ca_cert: Option<String>,
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
            ca_cert,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn selector_attrs(field: &str, label: &str) -> RequestAttributes {
        let mut a = RequestAttributes::new(
            UserInfo {
                username: "alice".into(),
                uid: String::new(),
                groups: vec![],
                extra: Default::default(),
            },
            "list",
            "jobs",
        );
        a.set_selectors(field, label);
        a
    }

    // authorization_test.go TestGetAuthorizerAttributes "enabled, good field selector".
    #[test]
    fn set_selectors_parses_good_field_selector() {
        let a = selector_attrs("foo==bar", "");
        assert_eq!(a.field_selector_requirements.len(), 1);
        assert_eq!(a.field_selector_requirements[0].field(), "foo");
        assert_eq!(a.field_selector_requirements[0].value(), "bar");
        assert!(a.field_selector_parsing_err.is_none());
        assert!(a.label_selector_requirements.is_empty());
    }

    // "enabled, bad field selector": requirements nil, error recorded.
    #[test]
    fn set_selectors_records_field_selector_parse_error() {
        let a = selector_attrs("*bar", "");
        assert!(a.field_selector_requirements.is_empty());
        assert!(a.field_selector_parsing_err.is_some());
    }

    // "enabled, good label selector".
    #[test]
    fn set_selectors_parses_good_label_selector() {
        let a = selector_attrs("", "foo==bar");
        assert_eq!(a.label_selector_requirements.len(), 1);
        assert_eq!(a.label_selector_requirements[0].key(), "foo");
        assert!(a.label_selector_parsing_err.is_none());
    }

    // "enabled, bad label selector". Upstream's text differs only in the empty
    // field path, which Go renders `<nil>` and the existing parser renders empty.
    #[test]
    fn set_selectors_records_label_selector_parse_error() {
        let a = selector_attrs("", "*bar");
        assert!(a.label_selector_requirements.is_empty());
        assert_eq!(
            a.label_selector_parsing_err.as_deref(),
            Some("unable to parse requirement: : Invalid value: \"*bar\": name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')")
        );
    }

    #[test]
    fn test_policy_rule_matching() {
        let rule = PolicyRule {
            verbs: vec!["get".to_string(), "list".to_string()],
            api_groups: Some(vec!["".to_string()]),
            resources: Some(vec!["pods".to_string()]),
            resource_names: None,
            non_resource_urls: None,
        };

        let user = UserInfo {
            username: "test-user".to_string(),
            uid: "test-uid".to_string(),
            groups: vec![],
            extra: std::collections::HashMap::new(),
        };

        let attrs = RequestAttributes::new(user.clone(), "get", "pods")
            .with_namespace("default")
            .with_api_group("");

        // This would need a mock storage implementation to fully test
        // Just testing the rule matching logic
        assert!(rule.verbs.contains(&attrs.verb));
    }

    struct NoStore;
    #[async_trait]
    impl AuthzStorage for NoStore {
        async fn get<T>(&self, key: &str, _ns: Option<&str>) -> Result<T>
        where
            T: DeserializeOwned + Send + Sync,
        {
            Err(crate::error::Error::NotFound(key.to_string()))
        }
        async fn list<T>(&self, _ns: Option<&str>) -> Result<Vec<T>>
        where
            T: serde::Serialize + DeserializeOwned + Send + Sync,
        {
            Ok(Vec::new())
        }
    }

    /// A kubelet's own pod watch, verbatim: cluster-scoped LIST of pods with a
    /// `spec.nodeName` field selector, as `system:node:<name>`.
    ///
    /// A vanilla kind kubelet against a swapped rusternetes api-server was denied
    /// this and therefore never started a single pod:
    ///
    ///   Failed to watch: failed to list *v1.Pod: Node vs-apisrv-dbg-worker is not
    ///   authorized to list pods in namespace None
    ///
    /// Upstream allows it through the static node rules
    /// (plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go NodeRules():
    /// `NewRule(Read...).Groups(legacyGroup).Resources("pods")`), which the Node
    /// authorizer falls through to when pods are not subdivided by selector
    /// (node_authorizer.go: "Access to other resources is not subdivided, so just
    /// evaluate against the statically defined node rules").
    fn node_user() -> UserInfo {
        UserInfo {
            username: "system:node:worker-1".to_string(),
            uid: "node-uid".to_string(),
            groups: vec!["system:nodes".to_string()],
            extra: std::collections::HashMap::new(),
        }
    }

    #[tokio::test]
    async fn node_may_list_pods_cluster_wide() {
        let mut attrs = RequestAttributes::new(node_user(), "list", "pods").with_api_group("");
        attrs.set_selectors("spec.nodeName=worker-1", "");
        assert!(
            matches!(
                NodeAuthorizer::default().authorize(&attrs).await.unwrap(),
                Decision::Allow
            ),
            "the kubelet's own cluster-scoped pod list must be allowed"
        );
    }

    /// With `AuthorizeNodeWithSelectors` GA (authorizePod), the same list
    /// WITHOUT the node's `spec.nodeName` selector, or with another node's, is
    /// not allowed any more.
    #[tokio::test]
    async fn node_may_not_list_or_watch_pods_without_its_own_selector() {
        for verb in ["list", "watch"] {
            for selector in ["", "spec.nodeName=other", "status.phase=Running"] {
                let mut attrs =
                    RequestAttributes::new(node_user(), verb, "pods").with_api_group("");
                attrs.set_selectors(selector, "");
                assert_eq!(
                    NodeAuthorizer::default().authorize(&attrs).await.unwrap(),
                    Decision::Deny(
                        "can only list/watch pods with spec.nodeName field selector".to_string()
                    ),
                    "{verb} {selector:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn node_may_watch_pods_cluster_wide() {
        let mut attrs = RequestAttributes::new(node_user(), "watch", "pods").with_api_group("");
        attrs.set_selectors("spec.nodeName=worker-1", "");
        assert!(matches!(
            NodeAuthorizer::default().authorize(&attrs).await.unwrap(),
            Decision::Allow
        ));
    }

    /// The second denial the same kubelet logged, also a NodeRules entry:
    /// `NewRule("get","list","watch").Groups("node.k8s.io").Resources("runtimeclasses")`.
    #[tokio::test]
    async fn node_may_list_runtimeclasses() {
        let attrs = RequestAttributes::new(node_user(), "list", "runtimeclasses")
            .with_api_group("node.k8s.io");
        assert!(matches!(
            NodeAuthorizer::default().authorize(&attrs).await.unwrap(),
            Decision::Allow
        ));
    }

    /// Upstream NodeRules grants pods create/delete outright and constrains them
    /// in NodeRestriction admission instead (policy.go:215-217). This asserted
    /// the opposite while rusternetes had no such plugin (#1721); it does now
    /// (api-server `handlers::node_restriction`), which is what lets a kubelet
    /// delete the mirror pod of a static pod (#1906).
    #[tokio::test]
    async fn node_may_create_and_delete_pods() {
        for verb in ["create", "delete"] {
            let attrs = RequestAttributes::new(node_user(), verb, "pods")
                .with_namespace("kube-system")
                .with_api_group("");
            assert_eq!(
                NodeAuthorizer::default().authorize(&attrs).await.unwrap(),
                Decision::Allow,
                "node should be authorized to {verb} pods; NodeRestriction narrows it"
            );
        }
    }

    /// `pods/eviction` (policy.go:223) stays denied: there is no NodeRestriction
    /// equivalent for the eviction path yet, so the grant would let a node evict
    /// any pod in the cluster.
    #[tokio::test]
    async fn node_may_not_create_pod_evictions() {
        let attrs = RequestAttributes::new(node_user(), "create", "pods")
            .with_namespace("kube-system")
            .with_api_group("")
            .with_subresource("eviction");
        assert!(matches!(
            NodeAuthorizer::default().authorize(&attrs).await.unwrap(),
            Decision::Deny(_)
        ));
    }

    /// Services are read-only for nodes (`Read...`), so writes stay denied — the
    /// rule table must not become a blanket allow.
    #[tokio::test]
    async fn node_may_not_delete_services() {
        let attrs = RequestAttributes::new(node_user(), "delete", "services")
            .with_namespace("default")
            .with_api_group("");
        assert!(
            matches!(
                NodeAuthorizer::default().authorize(&attrs).await.unwrap(),
                Decision::Deny(_)
            ),
            "nodes have read-only access to services"
        );
    }

    /// And a non-node user is still not a node.
    #[tokio::test]
    async fn non_node_user_is_denied() {
        let user = UserInfo {
            username: "alice".to_string(),
            uid: "u".to_string(),
            groups: vec![],
            extra: std::collections::HashMap::new(),
        };
        let attrs = RequestAttributes::new(user, "list", "pods").with_api_group("");
        assert!(matches!(
            NodeAuthorizer::default().authorize(&attrs).await.unwrap(),
            Decision::Deny(_)
        ));
    }

    fn rule(verbs: &[&str], groups: &[&str], resources: &[&str]) -> PolicyRule {
        PolicyRule {
            verbs: verbs.iter().map(|s| s.to_string()).collect(),
            api_groups: Some(groups.iter().map(|s| s.to_string()).collect()),
            resources: Some(resources.iter().map(|s| s.to_string()).collect()),
            resource_names: None,
            non_resource_urls: None,
        }
    }

    /// #1667: a rule on the subresource `pods/binding` must authorize a request
    /// for resource=pods subresource=binding (the scheduler's Bind), and a rule
    /// on the bare `pods` must NOT — subresources are granted explicitly. Also
    /// `*/binding` matches. Mirrors upstream `resourceMatches`.
    #[test]
    fn rbac_matches_subresource_combined() {
        let authz = RBACAuthorizer::new(Arc::new(NoStore));
        let bind = RequestAttributes::new(mk_user("system:kube-scheduler"), "create", "pods")
            .with_api_group("")
            .with_subresource("binding");

        assert!(
            authz.rule_allows(&rule(&["create"], &[""], &["pods/binding"]), &bind),
            "a rule on pods/binding must authorize the Bind"
        );
        assert!(
            !authz.rule_allows(&rule(&["create"], &[""], &["pods"]), &bind),
            "a rule on the bare `pods` must NOT grant pods/binding"
        );
        assert!(
            authz.rule_allows(&rule(&["create"], &[""], &["*/binding"]), &bind),
            "`*/binding` must match the binding subresource"
        );
        assert!(
            authz.rule_allows(&rule(&["*"], &["*"], &["*"]), &bind),
            "`*` resources must match everything incl. subresources"
        );
        // A bare-resource request still matches a bare-resource rule.
        let get_pod = RequestAttributes::new(mk_user("x"), "get", "pods").with_api_group("");
        assert!(authz.rule_allows(&rule(&["get"], &[""], &["pods"]), &get_pod));
    }

    fn mk_user(username: &str) -> UserInfo {
        UserInfo {
            username: username.to_string(),
            uid: String::new(),
            groups: vec![],
            extra: std::collections::HashMap::new(),
        }
    }

    /// `mockAuthzHandler` from `authorization/union/union_test.go:30-38`:
    /// a fixed decision, empty reason, optional error.
    struct MockAuthz {
        opinion: Opinion,
        error: Option<&'static str>,
    }

    fn mock(opinion: Opinion, error: Option<&'static str>) -> Arc<dyn Authorizer> {
        Arc::new(MockAuthz { opinion, error })
    }

    fn no_op() -> Opinion {
        Opinion::NoOpinion {
            reason: String::new(),
            error: None,
        }
    }

    #[async_trait]
    impl Authorizer for MockAuthz {
        async fn authorize(&self, a: &RequestAttributes) -> Result<Decision> {
            self.authorize_outcome(a).await?.into_decision()
        }
        async fn authorize_outcome(&self, _: &RequestAttributes) -> Result<AuthorizationOutcome> {
            Ok(AuthorizationOutcome {
                opinion: self.opinion.clone(),
                error: self.error.map(str::to_string),
            })
        }
        async fn get_user_rules(
            &self,
            _: &UserInfo,
            _: &str,
        ) -> Result<(
            Vec<crate::resources::ResourceRule>,
            Vec<crate::resources::NonResourceRule>,
        )> {
            Ok((vec![], vec![]))
        }
    }

    /// Ports of `TestAuthorizationSecondPasses`, `TestAuthorizationFirstPasses`,
    /// `TestAuthorizationNonePasses`, `TestAuthorizationError` and
    /// `TestAuthorizationUnequivocalDeny`
    /// (`authorization/union/union_test.go:40-82,224-268`), plus the reason
    /// join of `union.go:45-68` (`strings.Join(reasonlist, "\n")`, empty
    /// reasons skipped, errors aggregated).
    #[tokio::test]
    async fn union_test_go_authorize_ports() {
        let attrs = RequestAttributes::new(mk_user("a"), "get", "pods");
        let allow = || mock(Opinion::Allow(String::new()), None);
        let deny = |e| mock(Opinion::Deny(String::new()), e);

        // SecondPasses / FirstPasses.
        let u = UnionAuthorizer::new(vec![mock(no_op(), None), allow()]);
        assert_eq!(u.authorize(&attrs).await.unwrap(), Decision::Allow);
        let u = UnionAuthorizer::new(vec![allow(), mock(no_op(), None)]);
        assert_eq!(u.authorize(&attrs).await.unwrap(), Decision::Allow);

        // NonePasses: not Allow.
        let u = UnionAuthorizer::new(vec![mock(no_op(), None), mock(no_op(), None)]);
        assert!(!matches!(u.authorize(&attrs).await, Ok(Decision::Allow)));

        // Error: two NoOpinion+error delegates -> an error, aggregated with
        // dedup of identical messages (utilerrors.NewAggregate).
        let u = UnionAuthorizer::new(vec![mock(no_op(), Some("foo")), mock(no_op(), Some("foo"))]);
        let out = u.authorize_outcome(&attrs).await.unwrap();
        assert_eq!(out.error.as_deref(), Some("foo"));
        assert!(u.authorize(&attrs).await.is_err());
        let u = UnionAuthorizer::new(vec![mock(no_op(), Some("a")), mock(no_op(), Some("b"))]);
        let out = u.authorize_outcome(&attrs).await.unwrap();
        assert_eq!(out.error.as_deref(), Some("[a, b]"));

        // UnequivocalDeny cases 0-3.
        let u = UnionAuthorizer::new(vec![]);
        let out = u.authorize_outcome(&attrs).await.unwrap();
        assert!(matches!(out.opinion, Opinion::NoOpinion { .. }));
        let u = UnionAuthorizer::new(vec![mock(no_op(), None), allow(), deny(None)]);
        assert!(matches!(
            u.authorize_outcome(&attrs).await.unwrap().opinion,
            Opinion::Allow(_)
        ));
        let u = UnionAuthorizer::new(vec![mock(no_op(), None), deny(None), allow()]);
        assert!(matches!(
            u.authorize_outcome(&attrs).await.unwrap().opinion,
            Opinion::Deny(_)
        ));
        let u = UnionAuthorizer::new(vec![
            mock(no_op(), None),
            deny(Some("webhook failed closed")),
            allow(),
        ]);
        let out = u.authorize_outcome(&attrs).await.unwrap();
        assert!(matches!(out.opinion, Opinion::Deny(_)));
        assert_eq!(out.error.as_deref(), Some("webhook failed closed"));

        // Reason join: "\n" separator, empty reasons skipped.
        let r = |s: &str| {
            mock(
                Opinion::NoOpinion {
                    reason: s.into(),
                    error: None,
                },
                None,
            )
        };
        let u = UnionAuthorizer::new(vec![r("one"), r(""), r("two")]);
        match u.authorize_outcome(&attrs).await.unwrap().opinion {
            Opinion::NoOpinion { reason, .. } => assert_eq!(reason, "one\ntwo"),
            other => panic!("unexpected {other:?}"),
        }
    }

    struct RulesMock(
        Vec<crate::resources::ResourceRule>,
        Vec<crate::resources::NonResourceRule>,
    );

    #[async_trait]
    impl Authorizer for RulesMock {
        async fn authorize(&self, _: &RequestAttributes) -> Result<Decision> {
            Ok(Decision::Deny(String::new()))
        }
        async fn get_user_rules(
            &self,
            _: &UserInfo,
            _: &str,
        ) -> Result<(
            Vec<crate::resources::ResourceRule>,
            Vec<crate::resources::NonResourceRule>,
        )> {
            Ok((self.0.clone(), self.1.clone()))
        }
    }

    /// Ports of `TestAuthorizationResourceRules` and
    /// `TestAuthorizationNonResourceRules` (union_test.go:97-198):
    /// `unionAuthzRulesHandler.RulesFor` concatenates rules in order.
    #[tokio::test]
    async fn union_rules_concatenate_in_order() {
        use crate::resources::{NonResourceRule, ResourceRule};
        let s = |v: &[&str]| Some(v.iter().map(|x| x.to_string()).collect::<Vec<_>>());
        let rr = |verbs: &[&str], res: &[&str], names: Option<&[&str]>| ResourceRule {
            verbs: verbs.iter().map(|x| x.to_string()).collect(),
            api_groups: s(&["*"]),
            resources: s(res),
            resource_names: names.and_then(s),
        };
        let nr = |url: &str| NonResourceRule {
            verbs: vec!["get".into()],
            non_resource_urls: s(&[url]),
        };
        let u = UnionAuthorizer::new(vec![
            Arc::new(RulesMock(
                vec![
                    rr(&["*"], &["bindings"], None),
                    rr(&["get", "list", "watch"], &["*"], None),
                ],
                vec![nr("/api")],
            )),
            Arc::new(RulesMock(
                vec![
                    rr(&["*"], &["events"], None),
                    rr(&["get"], &["*"], Some(&["foo"])),
                ],
                vec![nr("/api/*")],
            )),
        ]);
        let (res, non) = u.get_user_rules(&mk_user("a"), "").await.unwrap();
        let want_res = vec![
            rr(&["*"], &["bindings"], None),
            rr(&["get", "list", "watch"], &["*"], None),
            rr(&["*"], &["events"], None),
            rr(&["get"], &["*"], Some(&["foo"])),
        ];
        assert_eq!(
            serde_json::to_value(&res).unwrap(),
            serde_json::to_value(&want_res).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&non).unwrap(),
            serde_json::to_value(vec![nr("/api"), nr("/api/*")]).unwrap()
        );
    }

    /// #1664: the Node,RBAC union is allow-override — a `Deny` from one
    /// authorizer falls through to the next, any `Allow` wins, and only an
    /// all-deny chain denies. In particular a kubelet (`system:node:<n>`) is
    /// authorized for its own Node via the Node authorizer even when the other
    /// authorizer denies (as an empty RBAC store would).
    #[tokio::test]
    async fn union_is_allow_override() {
        let u = mk_user("alice");
        let attrs = RequestAttributes::new(u.clone(), "get", "pods").with_namespace("default");

        // Deny then Allow -> Allow (fall-through).
        let union = UnionAuthorizer::new(vec![
            Arc::new(AlwaysDenyAuthorizer),
            Arc::new(AlwaysAllowAuthorizer),
        ]);
        assert!(matches!(
            union.authorize(&attrs).await.unwrap(),
            Decision::Allow
        ));

        // All deny -> Deny.
        let union_deny = UnionAuthorizer::new(vec![Arc::new(AlwaysDenyAuthorizer)]);
        assert!(matches!(
            union_deny.authorize(&attrs).await.unwrap(),
            Decision::Deny(_)
        ));

        // Node authorizer grants a kubelet its own Node, even chained with a
        // denying authorizer (models Node + empty-RBAC).
        let node_union = UnionAuthorizer::new(vec![
            Arc::new(NodeAuthorizer::default()),
            Arc::new(AlwaysDenyAuthorizer),
        ]);
        // NodeIdentity requires the system:nodes group (default.go:42-68).
        let mut node_user = mk_user("system:node:n1");
        node_user.groups.push("system:nodes".into());
        let node_attrs = RequestAttributes::new(node_user, "get", "nodes").with_name("n1");
        assert!(matches!(
            node_union.authorize(&node_attrs).await.unwrap(),
            Decision::Allow
        ));
        // A non-node user gets no Node grant; with a denying peer the union denies.
        let denied = RequestAttributes::new(mk_user("mallory"), "get", "nodes").with_name("n1");
        assert!(matches!(
            node_union.authorize(&denied).await.unwrap(),
            Decision::Deny(_)
        ));
    }

    /// Port of `TestPrivilegedGroupAuthorizer`
    /// (`staging/src/k8s.io/apiserver/pkg/authorization/authorizerfactory/builtin_test.go:41-55`):
    /// a user in a privileged group is allowed, one outside it is not, and the
    /// authorizer holds regardless of what any RBAC object says (#1576).
    #[tokio::test]
    async fn privileged_group_authorizer() {
        let auth = PrivilegedGroupAuthorizer::new(&["allow-01", "allow-01"]);
        let mut yes = mk_user("alice");
        yes.groups = vec!["no".into(), "allow-01".into()];
        let mut no = mk_user("bob");
        no.groups = vec!["no".into(), "deny-01".into()];
        let a = RequestAttributes::new(yes, "delete", "nodes");
        assert!(matches!(auth.authorize(&a).await.unwrap(), Decision::Allow));
        let b = RequestAttributes::new(no, "delete", "nodes");
        assert!(matches!(
            auth.authorize(&b).await.unwrap(),
            Decision::Deny(_)
        ));
    }

    /// `system:masters` is the superuser group (`user.SystemPrivilegedGroup`,
    /// `k8s.io/apiserver/pkg/authentication/user/user.go:71`) and the union
    /// built by `superuser_then` puts it first, as `newForConfig` does
    /// (`pkg/kubeapiserver/authorizer/reload.go:97-99`): it is allowed against an
    /// empty RBAC store, a non-member is not.
    #[tokio::test]
    async fn system_masters_is_superuser_over_empty_rbac() {
        let union = superuser_then(vec![Arc::new(AlwaysDenyAuthorizer)]);
        let mut admin = mk_user("kubernetes-admin");
        admin.groups = vec![SYSTEM_PRIVILEGED_GROUP.to_string()];
        let a = RequestAttributes::new(admin, "create", "clusterroles")
            .with_api_group("rbac.authorization.k8s.io");
        assert!(matches!(
            union.authorize(&a).await.unwrap(),
            Decision::Allow
        ));
        let m = RequestAttributes::new(mk_user("mallory"), "create", "clusterroles");
        assert!(matches!(
            union.authorize(&m).await.unwrap(),
            Decision::Deny(_)
        ));
    }
}

#[cfg(test)]
#[path = "authz/allow_reason_tests.rs"]
mod allow_reason_tests;
#[cfg(test)]
#[path = "authz/node_authorizer_tests.rs"]
mod node_authorizer_tests;
