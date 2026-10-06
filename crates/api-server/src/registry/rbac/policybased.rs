//! Port of the `policybased` storages —
//! `pkg/registry/rbac/{role,rolebinding,clusterrole,clusterrolebinding}/policybased/storage.go`
//! — which wrap each RBAC REST storage so that a request cannot grant
//! permissions the requester does not hold.
//!
//! Upstream wraps `Create` and `Update` of the standard storage. `Create` is
//! the Store's `BeginCreate` hook here. `Update` wraps the `UpdatedObjectInfo`
//! with `rest.WrapUpdatedObjectInfo` (rest/update.go:241-269), so the check
//! runs on the object as the client sent it, BEFORE `Store.Update` copies the
//! stored `resourceVersion` into an unconditional update
//! (`AllowUnconditionalUpdate` is true for all four kinds). That ordering
//! matters: `IsOnlyMutatingGCFields` (pkg/registry/rbac/helpers.go:29-50)
//! compares `resourceVersion` too, so an unconditional PUT that changes only
//! finalizers/ownerReferences is not GC-only and is escalation-checked. Here
//! the wrapper is the Store's `update_transformers`
//! (`TransformFunc`s applied before the resourceVersion handling).

use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::authz::Authorizer;
use rusternetes_common::resources::{
    ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, RoleRef,
};
use rusternetes_common::{Error, Result};
use rusternetes_storage::StorageBackend;

use super::aggregation::materialise_aggregated_rules;
use super::escalation_check::{
    binding_authorized, escalation_allowed, is_only_mutating_gc_fields, role_escalation_authorized,
};
use super::rule::{confirm_no_escalation, AuthorizationRuleResolver};
use crate::registry::generic::store::{
    BeginCreate, BeginUpdate, CreateOptions, Finish, UpdateOptions,
};
use crate::registry::rest::{GroupResource, RequestContext, TransformFunc};

/// A transformer reached without an object: `rest.WrapUpdatedObjectInfo`'s
/// inner info always produces one.
fn missing_object() -> Error {
    Error::Internal("no object was supplied to the update transformer".to_string())
}

/// A `FinishFunc` that has nothing to commit or revert.
pub struct Noop;

#[async_trait]
impl Finish for Noop {
    async fn finish(self: Box<Self>, _success: bool) {}
}

/// `errors.NewForbidden(groupResource, name, err)`
/// (apimachinery/pkg/api/errors/errors.go:248-262).
pub fn new_forbidden(qualified_resource: &GroupResource, name: &str, err: &str) -> Error {
    Error::Forbidden(if name.is_empty() {
        format!("{qualified_resource} is forbidden: {err}")
    } else {
        format!("{qualified_resource} \"{name}\" is forbidden: {err}")
    })
}

/// `rbac.Resource(resource)`: the group-qualified resource used in errors.
fn rbac_resource(resource: &str) -> GroupResource {
    GroupResource::new("rbac.authorization.k8s.io", resource)
}

/// `fullAuthority` (clusterrole/policybased/storage.go:63-66): `*` on `*.*`
/// and on every non-resource URL.
fn full_authority() -> Vec<PolicyRule> {
    vec![
        PolicyRule {
            verbs: vec!["*".into()],
            api_groups: Some(vec!["*".into()]),
            resources: Some(vec!["*".into()]),
            resource_names: None,
            non_resource_urls: None,
        },
        PolicyRule {
            verbs: vec!["*".into()],
            api_groups: None,
            resources: None,
            resource_names: None,
            non_resource_urls: Some(vec!["*".into()]),
        },
    ]
}

/// The pieces every policybased storage holds (`authorizer`, `ruleResolver`).
#[derive(Clone)]
pub struct PolicyBased {
    pub authorizer: Arc<dyn Authorizer>,
    pub resolver: Arc<dyn AuthorizationRuleResolver>,
}

impl PolicyBased {
    /// The rule check of the Role and ClusterRole storages:
    /// `EscalationAllowed || RoleEscalationAuthorized`, else
    /// `ConfirmNoEscalationInternal` on the rules.
    async fn confirm_rules(
        &self,
        ctx: &RequestContext,
        qualified_resource: &GroupResource,
        name: &str,
        rules: &[PolicyRule],
    ) -> Result<()> {
        confirm_no_escalation(
            ctx.user.as_ref(),
            ctx.namespace.as_deref(),
            &*self.resolver,
            rules,
        )
        .await
        .map_err(|msg| new_forbidden(qualified_resource, name, &msg))
    }

    async fn escalation_bypassed(&self, ctx: &RequestContext, resource: &str) -> bool {
        escalation_allowed(ctx)
            || role_escalation_authorized(ctx, &*self.authorizer, resource).await
    }

    /// The binding check of the RoleBinding and ClusterRoleBinding storages
    /// (after the `EscalationAllowed` and namespace steps): `BindingAuthorized`,
    /// else resolve the referenced role and `ConfirmNoEscalation` on its rules.
    async fn confirm_binding(
        &self,
        ctx: &RequestContext,
        qualified_resource: &GroupResource,
        name: &str,
        role_ref: &RoleRef,
        binding_namespace: &str,
    ) -> Result<()> {
        if binding_authorized(ctx, role_ref, binding_namespace, &*self.authorizer).await {
            return Ok(());
        }
        let rules = self
            .resolver
            .get_role_reference_rules(role_ref, binding_namespace)
            .await?;
        confirm_no_escalation(
            ctx.user.as_ref(),
            ctx.namespace.as_deref(),
            &*self.resolver,
            &rules,
        )
        .await
        .map_err(|msg| new_forbidden(qualified_resource, name, &msg))
    }
}

// ---------------------------------------------------------------------------
// Role (role/policybased/storage.go)
// ---------------------------------------------------------------------------

pub struct RolePolicyBased(pub PolicyBased);

#[async_trait]
impl BeginCreate<Role> for RolePolicyBased {
    /// `Storage.Create` (:63-74).
    async fn begin_create(
        &self,
        ctx: &RequestContext,
        obj: &mut Role,
        _options: &CreateOptions,
    ) -> Result<Box<dyn Finish>> {
        if !self.0.escalation_bypassed(ctx, "roles").await {
            self.0
                .confirm_rules(ctx, &rbac_resource("roles"), &obj.metadata.name, &obj.rules)
                .await?;
        }
        Ok(Box::new(Noop))
    }
}

#[async_trait]
impl TransformFunc<Role> for RolePolicyBased {
    /// `Storage.Update` (:76-99): the `WrapUpdatedObjectInfo` transformer.
    async fn transform(
        &self,
        ctx: &RequestContext,
        new: Option<Role>,
        old: Option<&Role>,
    ) -> Result<Role> {
        let obj = new.ok_or_else(missing_object)?;
        let Some(old) = old else { return Ok(obj) };
        if !self.0.escalation_bypassed(ctx, "roles").await
            // If we're only mutating fields needed for the GC to eventually
            // delete this obj, return.
            && !is_only_mutating_gc_fields(&obj, old)
        {
            self.0
                .confirm_rules(ctx, &rbac_resource("roles"), &obj.metadata.name, &obj.rules)
                .await?;
        }
        Ok(obj)
    }
}

// ---------------------------------------------------------------------------
// RoleBinding (rolebinding/policybased/storage.go)
// ---------------------------------------------------------------------------

pub struct RoleBindingPolicyBased(pub PolicyBased);

impl RoleBindingPolicyBased {
    const RESOURCE: &'static str = "rolebindings";

    /// The namespace of the request, populated from the URL
    /// (`genericapirequest.NamespaceFrom`). The namespace in the object can be
    /// empty until `BeforeCreate` / `BeforeUpdate` fills it in from the
    /// context.
    fn request_namespace(ctx: &RequestContext) -> Result<String> {
        ctx.namespace
            .clone()
            .ok_or_else(|| Error::BadRequest("namespace is required".to_string()))
    }
}

#[async_trait]
impl BeginCreate<RoleBinding> for RoleBindingPolicyBased {
    /// `Storage.Create` (:62-96).
    async fn begin_create(
        &self,
        ctx: &RequestContext,
        obj: &mut RoleBinding,
        _options: &CreateOptions,
    ) -> Result<Box<dyn Finish>> {
        if escalation_allowed(ctx) {
            return Ok(Box::new(Noop));
        }
        let namespace = Self::request_namespace(ctx)?;
        self.0
            .confirm_binding(
                ctx,
                &rbac_resource(Self::RESOURCE),
                &obj.metadata.name,
                &obj.role_ref,
                &namespace,
            )
            .await?;
        Ok(Box::new(Noop))
    }
}

#[async_trait]
impl TransformFunc<RoleBinding> for RoleBindingPolicyBased {
    /// `Storage.Update` (:98-140): the `WrapUpdatedObjectInfo` transformer.
    async fn transform(
        &self,
        ctx: &RequestContext,
        new: Option<RoleBinding>,
        old: Option<&RoleBinding>,
    ) -> Result<RoleBinding> {
        let obj = new.ok_or_else(missing_object)?;
        let Some(old) = old else { return Ok(obj) };
        if escalation_allowed(ctx) {
            return Ok(obj);
        }
        let namespace = Self::request_namespace(ctx)?;
        // If we're only mutating fields needed for the GC to eventually delete
        // this obj, return.
        if is_only_mutating_gc_fields(&obj, old) {
            return Ok(obj);
        }
        self.0
            .confirm_binding(
                ctx,
                &rbac_resource(Self::RESOURCE),
                &obj.metadata.name,
                &obj.role_ref,
                &namespace,
            )
            .await?;
        Ok(obj)
    }
}

// ---------------------------------------------------------------------------
// ClusterRoleBinding (clusterrolebinding/policybased/storage.go)
// ---------------------------------------------------------------------------

pub struct ClusterRoleBindingPolicyBased(pub PolicyBased);

impl ClusterRoleBindingPolicyBased {
    const RESOURCE: &'static str = "clusterrolebindings";
}

#[async_trait]
impl BeginCreate<ClusterRoleBinding> for ClusterRoleBindingPolicyBased {
    /// `Storage.Create` (:61-90).
    async fn begin_create(
        &self,
        ctx: &RequestContext,
        obj: &mut ClusterRoleBinding,
        _options: &CreateOptions,
    ) -> Result<Box<dyn Finish>> {
        if escalation_allowed(ctx) {
            return Ok(Box::new(Noop));
        }
        self.0
            .confirm_binding(
                ctx,
                &rbac_resource(Self::RESOURCE),
                &obj.metadata.name,
                &obj.role_ref,
                "",
            )
            .await?;
        Ok(Box::new(Noop))
    }
}

#[async_trait]
impl TransformFunc<ClusterRoleBinding> for ClusterRoleBindingPolicyBased {
    /// `Storage.Update` (:94-126): the `WrapUpdatedObjectInfo` transformer.
    async fn transform(
        &self,
        ctx: &RequestContext,
        new: Option<ClusterRoleBinding>,
        old: Option<&ClusterRoleBinding>,
    ) -> Result<ClusterRoleBinding> {
        let obj = new.ok_or_else(missing_object)?;
        let Some(old) = old else { return Ok(obj) };
        if escalation_allowed(ctx) || is_only_mutating_gc_fields(&obj, old) {
            return Ok(obj);
        }
        self.0
            .confirm_binding(
                ctx,
                &rbac_resource(Self::RESOURCE),
                &obj.metadata.name,
                &obj.role_ref,
                "",
            )
            .await?;
        Ok(obj)
    }
}

// ---------------------------------------------------------------------------
// ClusterRole (clusterrole/policybased/storage.go)
// ---------------------------------------------------------------------------

/// ClusterRole's storage also materialises `aggregationRule` (see
/// [`super::aggregation`]), which needs storage.
pub struct ClusterRolePolicyBased {
    pub base: PolicyBased,
    pub storage: Arc<StorageBackend>,
}

impl ClusterRolePolicyBased {
    const RESOURCE: &'static str = "clusterroles";

    /// `hasAggregationRule` (:129-131).
    fn has_aggregation_rule(role: &ClusterRole) -> bool {
        role.aggregation_rule
            .as_ref()
            .and_then(|a| a.cluster_role_selectors.as_ref())
            .is_some_and(|s| !s.is_empty())
    }

    /// Setting or changing an aggregation rule needs `*` on `*.*`, because it
    /// can gather anything and prevent tightening.
    async fn confirm_aggregation_rule(
        &self,
        ctx: &RequestContext,
        role: &ClusterRole,
    ) -> Result<()> {
        confirm_no_escalation(
            ctx.user.as_ref(),
            ctx.namespace.as_deref(),
            &*self.base.resolver,
            &full_authority(),
        )
        .await
        .map_err(|_| {
            new_forbidden(
                &rbac_resource(Self::RESOURCE),
                &role.metadata.name,
                "must have cluster-admin privileges to use the aggregationRule",
            )
        })
    }

    /// `Storage.Create` (:68-86).
    async fn check_create(&self, ctx: &RequestContext, obj: &ClusterRole) -> Result<()> {
        if self.base.escalation_bypassed(ctx, Self::RESOURCE).await {
            return Ok(());
        }
        self.base
            .confirm_rules(
                ctx,
                &rbac_resource(Self::RESOURCE),
                &obj.metadata.name,
                &obj.rules,
            )
            .await?;
        // To set the aggregation rule, since it can gather anything, requires
        // * on *.*.
        if Self::has_aggregation_rule(obj) {
            self.confirm_aggregation_rule(ctx, obj).await?;
        }
        Ok(())
    }

    /// `Storage.Update` (:88-119).
    async fn check_update(
        &self,
        ctx: &RequestContext,
        obj: &ClusterRole,
        old: &ClusterRole,
    ) -> Result<()> {
        if self.base.escalation_bypassed(ctx, Self::RESOURCE).await
            // If we're only mutating fields needed for the GC to eventually
            // delete this obj, return.
            || is_only_mutating_gc_fields(obj, old)
        {
            return Ok(());
        }
        self.base
            .confirm_rules(
                ctx,
                &rbac_resource(Self::RESOURCE),
                &obj.metadata.name,
                &obj.rules,
            )
            .await?;
        // To change the aggregation rule, since it can gather anything and
        // prevent tightening, requires * on *.*.
        if Self::has_aggregation_rule(obj) || Self::has_aggregation_rule(old) {
            self.confirm_aggregation_rule(ctx, obj).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl BeginCreate<ClusterRole> for ClusterRolePolicyBased {
    async fn begin_create(
        &self,
        ctx: &RequestContext,
        obj: &mut ClusterRole,
        _options: &CreateOptions,
    ) -> Result<Box<dyn Finish>> {
        self.check_create(ctx, obj).await?;
        materialise_aggregated_rules(&*self.storage, obj).await;
        Ok(Box::new(Noop))
    }
}

#[async_trait]
impl TransformFunc<ClusterRole> for ClusterRolePolicyBased {
    /// `Storage.Update`: the `WrapUpdatedObjectInfo` transformer.
    async fn transform(
        &self,
        ctx: &RequestContext,
        new: Option<ClusterRole>,
        old: Option<&ClusterRole>,
    ) -> Result<ClusterRole> {
        let obj = new.ok_or_else(missing_object)?;
        let Some(old) = old else { return Ok(obj) };
        self.check_update(ctx, &obj, old).await?;
        Ok(obj)
    }
}

#[async_trait]
impl BeginUpdate<ClusterRole> for ClusterRolePolicyBased {
    /// Rusternetes-only: materialises an aggregated ClusterRole's rules at
    /// write time (upstream does it in the `clusterroleaggregation`
    /// controller). The policybased check itself is the transformer above.
    async fn begin_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ClusterRole,
        _old: &mut ClusterRole,
        _options: &UpdateOptions,
    ) -> Result<Box<dyn Finish>> {
        materialise_aggregated_rules(&*self.storage, obj).await;
        Ok(Box::new(Noop))
    }
}
