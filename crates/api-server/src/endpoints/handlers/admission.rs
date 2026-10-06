//! The admission chain as the handlers and the Store see it.
//!
//! Upstream's chain is ordered by `pkg/kubeapiserver/options/plugins.go:106-110`:
//! MutatingAdmissionPolicy, MutatingAdmissionWebhook, ValidatingAdmissionPolicy,
//! ValidatingAdmissionWebhook, ResourceQuota. The mutating half runs in the
//! handler (create.go:202-206) or as a `TransformFunc` inside `Store.Update`
//! (update.go:170-189, patch.go:631-651); the validating half is handed to the
//! Store as `rest.AdmissionToValidateObjectFunc` /
//! `AdmissionToValidateObjectUpdateFunc` / `AdmissionToValidateObjectDeleteFunc`
//! (registry/rest/rest.go) and runs on the fully formed object.
//!
//! Of those plugins Rusternetes implements the two webhook plugins,
//! ValidatingAdmissionPolicy and ResourceQuota
//! ([`crate::admission::resourcequota`]). MutatingAdmissionPolicy is not
//! implemented.
//!
//! The in-tree plugins ahead of them run first, in `AllOrderedPlugins` order
//! (plugins.go:69-100), for the resources on this path that they handle:
//! LimitRanger and DefaultStorageClass, for PersistentVolumeClaims, and
//! Priority, for PriorityClasses. For Pods, [`Admission::admit_pod`] runs
//! NodeRestriction, LimitRanger, ServiceAccount, Priority,
//! DefaultTolerationSeconds and RuntimeClass, and
//! [`Admission::validate_pod`] runs PodSecurity and the pod ResourceQuota.

use async_trait::async_trait;
use rusternetes_common::admission::{
    self, AdmissionResponse, GroupVersionKind, GroupVersionResource, Operation,
};
use rusternetes_common::auth::UserInfo;
use rusternetes_common::resources::{
    CertificateSigningRequest, PersistentVolumeClaim, Pod, PriorityClass,
};
use rusternetes_common::{Error, Result};
use rusternetes_storage::Storage;

use super::rest::{authorize, RequestScope};
use crate::admission::resourcequota;
use crate::registry::rest::{
    GroupResource, Object, RequestContext, TransformFunc, ValidateObject, ValidateObjectUpdate,
};
use crate::state::ApiServerState;

/// One request's view of the admission chain: upstream's `admission.Interface`
/// plus the static parts of `admission.NewAttributesRecord`.
pub struct Admission<'a> {
    pub state: &'a ApiServerState,
    pub kind: &'a GroupVersionKind,
    pub resource: &'a GroupVersionResource,
    pub subresource: Option<&'a str>,
    pub namespace: Option<&'a str>,
    pub user: &'a UserInfo,
    pub dry_run: bool,
}

fn denied(reason: &str) -> Error {
    Error::Forbidden(format!("admission webhook denied the request: {reason}"))
}

fn to_value<T: Object>(obj: &T) -> Result<serde_json::Value> {
    serde_json::to_value(obj).map_err(|e| Error::Internal(e.to_string()))
}

/// The object as another type — the typed view an in-tree plugin works on,
/// as upstream's plugins type-assert `a.GetObject()`.
fn recast<A: serde::Serialize, B: serde::de::DeserializeOwned>(obj: &A) -> Result<B> {
    serde_json::to_value(obj)
        .and_then(serde_json::from_value)
        .map_err(|e| Error::Internal(e.to_string()))
}

impl Admission<'_> {
    /// The resource the plugins match against: `<resource>/<subresource>` on
    /// a subresource, as the webhook rules spell it (rules.go:95-115).
    fn request_resource(&self) -> GroupVersionResource {
        let mut gvr = self.resource.clone();
        if let Some(subresource) = self.subresource {
            gvr.resource = format!("{}/{subresource}", gvr.resource);
        }
        gvr
    }

    fn user_info(&self) -> admission::UserInfo {
        admission::UserInfo {
            username: self.user.username.clone(),
            uid: self.user.uid.clone(),
            groups: self.user.groups.clone(),
        }
    }

    /// Whether the request is for the `resize` subresource of a Pod.
    fn is_pod_resize(&self) -> bool {
        self.resource.group.is_empty()
            && self.resource.resource == "pods"
            && self.subresource == Some("resize")
    }

    /// Whether the request is for the core-group `resource` itself, not a
    /// subresource of it.
    fn is_core(&self, resource: &str) -> bool {
        self.resource.group.is_empty()
            && self.resource.resource == resource
            && self.subresource.is_none()
    }

    /// `admission.NewForbidden` (apiserver/pkg/admission/errors.go):
    /// `<resource> "<name>" is forbidden: <err>`, the resource being
    /// `a.GetResource().GroupResource()`.
    fn forbidden(&self, name: &str, err: impl std::fmt::Display) -> Error {
        Error::Forbidden(format!(
            "{} \"{name}\" is forbidden: {err}",
            GroupResource::new(&self.resource.group, &self.resource.resource)
        ))
    }

    /// The in-tree mutating plugins. `DefaultStorageClass`
    /// (plugin/pkg/admission/storage/storageclass/setdefault/admission.go)
    /// handles only CREATE of a PersistentVolumeClaim. LimitRanger's `Admit`
    /// mutates only pods.
    async fn admit_in_tree<T: Object>(&self, op: &Operation, obj: T) -> Result<T> {
        if self.is_core("pods") {
            let pod: Pod = recast(&obj)?;
            return recast(&self.admit_pod(op, pod).await?);
        }
        if *op != Operation::Create || !self.is_core("persistentvolumeclaims") {
            return Ok(obj);
        }
        let mut pvc: PersistentVolumeClaim = recast(&obj)?;
        crate::admission::set_default_storage_class(&self.state.storage, &mut pvc)
            .await
            .map_err(|e| self.forbidden(&pvc.metadata.name, e))?;
        recast(&pvc)
    }

    /// The in-tree validating plugins. `LimitRanger.Validate`
    /// (plugin/pkg/admission/limitranger/admission.go:116-156) checks a
    /// PersistentVolumeClaim's requests on CREATE and UPDATE, except when the
    /// old object is being deleted.
    async fn validate_in_tree<T: Object>(
        &self,
        ctx: &RequestContext,
        op: &Operation,
        obj: Option<&T>,
        old: Option<&T>,
    ) -> Result<()> {
        if let (Operation::Update, Some(obj), Some(old)) = (op, obj, old) {
            self.validate_csr_signer(obj, old).await?;
        }
        if self.is_core("pods") || self.is_pod_resize() {
            let obj: Option<Pod> = obj.map(recast).transpose()?;
            let old: Option<Pod> = old.map(recast).transpose()?;
            return self.validate_pod(ctx, op, obj.as_ref(), old.as_ref()).await;
        }
        // `Priority` sits ahead of `LimitRanger` in `AllOrderedPlugins`
        // (plugins.go:69-100).
        self.validate_priority(op, obj).await?;
        let (Some(obj), Some(namespace)) = (obj, self.namespace) else {
            return Ok(());
        };
        if !matches!(op, Operation::Create | Operation::Update)
            || !self.is_core("persistentvolumeclaims")
            || old.is_some_and(|o| o.metadata().deletion_timestamp.is_some())
        {
            return Ok(());
        }
        let pvc: PersistentVolumeClaim = recast(obj)?;
        match crate::admission::limit_ranger_validate_pvc(&self.state.storage, namespace, &pvc)
            .await
        {
            Ok(None) => Ok(()),
            Ok(Some(err)) => Err(self.forbidden(&pvc.metadata.name, err)),
            Err(e) => Err(self.forbidden(&pvc.metadata.name, e)),
        }
    }

    /// The `certificates/approval` and `certificates/signing` plugins, for an
    /// UPDATE of a CertificateSigningRequest's `approval` or `status`
    /// ([`crate::admission::certificates`]).
    async fn validate_csr_signer<T: Object>(&self, obj: &T, old: &T) -> Result<()> {
        if self.resource.group != "certificates.k8s.io"
            || self.resource.resource != "certificatesigningrequests"
        {
            return Ok(());
        }
        let new: CertificateSigningRequest = recast(obj)?;
        let old: CertificateSigningRequest = recast(old)?;
        match crate::admission::certificates::validate_update(
            self.state,
            self.user,
            self.subresource,
            &new,
            &old,
        )
        .await
        {
            Some(err) => Err(self.forbidden(&old.metadata.name, err)),
            None => Ok(()),
        }
    }

    /// `Priority.Validate` (plugin/pkg/admission/priority/admission.go:116-133)
    /// for PriorityClasses: `validatePriorityClass` (:156-176) lets at most
    /// one class be `globalDefault`.
    async fn validate_priority<T: Object>(&self, op: &Operation, obj: Option<&T>) -> Result<()> {
        let Some(obj) = obj else {
            return Ok(());
        };
        if self.subresource.is_some()
            || self.resource.group != "scheduling.k8s.io"
            || self.resource.resource != "priorityclasses"
            || !matches!(op, Operation::Create | Operation::Update)
        {
            return Ok(());
        }
        let pc: PriorityClass = recast(obj)?;
        if pc.global_default != Some(true) {
            return Ok(());
        }
        let Some(dpc) = crate::admission::get_default_priority_class(&self.state.storage).await?
        else {
            return Ok(());
        };
        if *op == Operation::Create || dpc.metadata.name != pc.metadata.name {
            return Err(self.forbidden(
                &pc.metadata.name,
                format!(
                    "PriorityClass {} is already marked as default. Only one default can exist",
                    dpc.metadata.name
                ),
            ));
        }
        Ok(())
    }

    /// The in-tree mutating plugins for a Pod, in `AllOrderedPlugins` order
    /// (plugins.go:69-100): LimitRanger, ServiceAccount, NodeRestriction,
    /// Priority, DefaultTolerationSeconds, RuntimeClass. NodeRestriction runs
    /// first here, as it did before the pod handlers moved onto this chain:
    /// it only narrows a node's requests and reads the pod as sent.
    async fn admit_pod(&self, op: &Operation, mut pod: Pod) -> Result<Pod> {
        let name = pod.metadata.name.clone();
        if *op == Operation::Update {
            // DefaultTolerationSeconds registers for Create AND Update
            // (plugin/pkg/admission/defaulttolerationseconds/admission.go:86).
            if let Some(spec) = pod.spec.as_mut() {
                rusternetes_common::tolerations::add_default_tolerations(spec);
            }
            return Ok(pod);
        }
        if *op != Operation::Create {
            return Ok(pod);
        }
        let Some(namespace) = self.namespace else {
            return Ok(pod);
        };
        let storage = &self.state.storage;

        crate::handlers::node_restriction::admit_pod_create(
            &**storage,
            &rusternetes_middleware::AuthContext {
                user: self.user.clone(),
            },
            &pod,
        )
        .await?;

        // LimitRanger.Admit (plugin/pkg/admission/limitranger/admission.go).
        let limit_ranges: Vec<rusternetes_common::resources::LimitRange> = storage
            .list(&rusternetes_storage::build_prefix(
                "limitranges",
                Some(namespace),
            ))
            .await
            .unwrap_or_default();
        match crate::admission::apply_limit_range_with(&mut pod, &limit_ranges) {
            Ok(true) => {}
            Ok(false) => {
                return Err(self.forbidden(&name, "Pod violates LimitRange constraints"));
            }
            Err(e) => {
                tracing::warn!("Error checking LimitRange for pod {namespace}/{name}: {e}");
            }
        }

        // ServiceAccount.Admit: a failure to inject does not fail the pod.
        if let Err(e) =
            crate::admission::inject_service_account_token(storage, namespace, &mut pod).await
        {
            tracing::warn!("Error injecting service account token for pod {namespace}/{name}: {e}");
        }

        self.admit_pod_priority(&mut pod).await?;

        // DefaultTolerationSeconds: the NotReady/Unreachable NoExecute
        // tolerations (tolerationSeconds: 300), unless the pod has them.
        if let Some(spec) = pod.spec.as_mut() {
            rusternetes_common::tolerations::add_default_tolerations(spec);
        }

        // RuntimeClass.Admit (plugin/pkg/admission/runtimeclass/admission.go):
        // the class must exist, and its overhead is set on the pod.
        let runtime_class = pod
            .spec
            .as_ref()
            .and_then(|s| s.runtime_class_name.clone())
            .filter(|n| !n.is_empty());
        if let Some(rc_name) = runtime_class {
            let rc_key = rusternetes_storage::build_key("runtimeclasses", None, &rc_name);
            let Ok(rc) = storage.get::<serde_json::Value>(&rc_key).await else {
                return Err(self.forbidden(
                    &name,
                    format!("pod {name} references non-existent RuntimeClass \"{rc_name}\""),
                ));
            };
            let overhead: std::collections::HashMap<String, String> = rc
                .pointer("/overhead/podFixed")
                .and_then(|o| o.as_object())
                .map(|o| {
                    o.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();
            if !overhead.is_empty() {
                if let Some(spec) = pod.spec.as_mut() {
                    spec.overhead = Some(overhead);
                }
            }
        }

        // PodTopologyLabels.admitPod (plugin/pkg/admission/podtopologylabels/
        // admission.go:123-148; after RuntimeClass in plugins.go:98): a pod
        // created with `spec.nodeName` gets the node's topology labels,
        // overwriting its own.
        if rusternetes_common::feature_gates::enabled(
            rusternetes_common::feature_gates::Feature::PodTopologyLabelsAdmission,
        ) {
            let node_name = pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.clone())
                .filter(|n| !n.is_empty());
            if let Some(node_name) = node_name {
                let labels =
                    crate::handlers::pod::topology_labels_for_node_name(&**storage, &node_name)
                        .await?;
                if !labels.is_empty() {
                    pod.metadata
                        .labels
                        .get_or_insert_with(Default::default)
                        .extend(labels);
                }
            }
        }
        Ok(pod)
    }

    /// `Priority.Admit` for a Pod (plugin/pkg/admission/priority/admission.go:
    /// 162-201): `spec.priority` is the one the PriorityClass names, and a
    /// request that sets a different one is refused.
    async fn admit_pod_priority(&self, pod: &mut Pod) -> Result<()> {
        let name = pod.metadata.name.clone();
        let storage = &self.state.storage;
        let Some(spec) = pod.spec.as_mut() else {
            return Ok(());
        };
        let mut priority: i32 = 0;
        let mut preemption_policy: Option<String> = None;
        match spec.priority_class_name.clone() {
            Some(pc_name) if !pc_name.is_empty() => {
                let pc_key = format!("/registry/priorityclasses/{pc_name}");
                match storage.get::<serde_json::Value>(&pc_key).await {
                    Ok(pc) => {
                        priority = pc.get("value").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                        preemption_policy = pc
                            .get("preemptionPolicy")
                            .and_then(|v| v.as_str())
                            .map(str::to_string);
                    }
                    Err(Error::NotFound(_)) => {
                        return Err(self.forbidden(
                            &name,
                            format!("no PriorityClass with name {pc_name} was found"),
                        ));
                    }
                    Err(_) => {}
                }
            }
            Some(_) => {}
            None => {
                // No priorityClassName: the globalDefault class, if any.
                // getDefaultPriorityClass (admission.go:268-285): the lowest
                // value wins if a race left several defaults.
                if let Some(pc) = crate::admission::get_default_priority_class(storage).await? {
                    priority = pc.value;
                    preemption_policy = pc.preemption_policy;
                    spec.priority_class_name = Some(pc.metadata.name);
                }
            }
        }
        if let Some(existing) = spec.priority {
            if existing != priority {
                return Err(self.forbidden(
                    &name,
                    format!(
                        "the integer value of priority ({existing}) must not be provided in pod spec; priority admission controller computed {priority} from the given PriorityClass name"
                    ),
                ));
            }
        }
        spec.priority = Some(priority);
        if spec.preemption_policy.is_none() {
            spec.preemption_policy = preemption_policy;
        }
        Ok(())
    }

    /// The in-tree validating plugins for a Pod: NodeRestriction on DELETE
    /// (admission.go:257-270), PodSecurity on CREATE, and the pod
    /// ResourceQuota evaluator.
    async fn validate_pod(
        &self,
        ctx: &RequestContext,
        op: &Operation,
        obj: Option<&Pod>,
        old: Option<&Pod>,
    ) -> Result<()> {
        let name = obj
            .or(old)
            .map(|p| p.metadata.name.clone())
            .unwrap_or_default();
        let storage = &self.state.storage;
        match op {
            Operation::Delete => {
                if let Some(old) = old {
                    crate::handlers::node_restriction::admit_pod_delete(
                        &rusternetes_middleware::AuthContext {
                            user: self.user.clone(),
                        },
                        old,
                    )?;
                }
            }
            Operation::Create => {
                if let (Some(pod), Some(namespace)) = (obj, self.namespace) {
                    crate::admission::PodSecurityAdmission::new()
                        .admit(storage, namespace, pod)
                        .await?;
                }
            }
            // KEP-5328 + KEP-1287: a Guaranteed-QoS CPU resize against a node
            // that has not declared `GuaranteedQoSPodCPUResize` is refused
            // (the NodeDeclaredFeatureValidator plugin, off with its gate).
            Operation::Update if self.subresource == Some("resize") => {
                if let (Some(pod), Some(old)) = (obj, old) {
                    crate::handlers::pod_subresources::check_node_declared_features_for_resize(
                        storage.as_ref(),
                        old,
                        pod,
                    )
                    .await?;
                }
            }
            _ => {}
        }

        // The pod evaluator (pkg/quota/v1/evaluator/core/pods.go:179-199)
        // handles CREATE, and an UPDATE only when the pod moves between
        // quota scopes. The namespace lock is held until the pod is stored.
        let (Some(pod), Some(namespace)) = (obj, self.namespace) else {
            return Ok(());
        };
        // `Constraints` runs before any usage arithmetic
        // (`resourcequota/controller.go:464-474`): a container omitting a
        // quota'd cpu/memory is refused `failed quota: <name>: must specify ...`.
        let constraints = |res: anyhow::Result<Option<String>>| -> Result<()> {
            match res {
                Ok(None) => Ok(()),
                Ok(Some(msg)) => Err(self.forbidden(&name, msg)),
                Err(e) => Err(Error::Internal(format!(
                    "error checking ResourceQuota: {e}"
                ))),
            }
        };
        match (op, old) {
            (Operation::Create, _) => {
                ctx.hold(crate::admission::lock_namespace_quota(namespace).await);
                constraints(
                    crate::admission::check_pod_quota_constraints(storage, namespace, pod).await,
                )?;
                match crate::admission::check_resource_quota(storage, namespace, pod).await {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(self.forbidden(&name, "exceeded quota")),
                    Err(e) => Err(Error::Internal(format!(
                        "error checking ResourceQuota: {e}"
                    ))),
                }
            }
            (Operation::Update, Some(old))
                if self.subresource == Some("resize") || pod_quota_scope_changed(old, pod) =>
            {
                ctx.hold(crate::admission::lock_namespace_quota(namespace).await);
                constraints(
                    crate::admission::check_pod_quota_constraints(storage, namespace, pod).await,
                )?;
                match crate::admission::check_resource_quota_with_old(
                    storage,
                    namespace,
                    pod,
                    Some(old),
                )
                .await
                {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(self.forbidden(&name, "exceeded quota")),
                    Err(e) => {
                        tracing::warn!("Error checking ResourceQuota on pod update: {e}");
                        Ok(())
                    }
                }
            }
            _ => Ok(()),
        }
    }

    /// The mutating plugins: `MutationInterface.Admit`.
    pub async fn admit<T: Object>(&self, op: Operation, obj: T, old: Option<&T>) -> Result<T> {
        let obj = self.admit_in_tree(&op, obj).await?;
        let name = obj.metadata().name.clone();
        let (response, mutated) = self
            .state
            .webhook_manager
            .run_mutating_webhooks_with_dryrun(
                &op,
                self.kind,
                &self.request_resource(),
                self.namespace,
                &name,
                Some(to_value(&obj)?),
                old.map(to_value).transpose()?,
                &self.user_info(),
                self.dry_run,
            )
            .await?;
        if let AdmissionResponse::Deny(reason) = &response {
            return Err(denied(reason));
        }
        match mutated {
            Some(value) => serde_json::from_value(value).map_err(|e| {
                Error::Internal(format!(
                    "failed to decode the object mutated by admission: {e}"
                ))
            }),
            None => Ok(obj),
        }
    }

    /// The validating plugins: `ValidationInterface.Validate`. `obj` is `None`
    /// on DELETE, `old` is `None` on CREATE.
    pub async fn validate<T: Object>(
        &self,
        ctx: &RequestContext,
        op: Operation,
        obj: Option<&T>,
        old: Option<&T>,
    ) -> Result<()> {
        self.validate_in_tree(ctx, &op, obj, old).await?;
        let name = obj
            .or(old)
            .map(|o| o.metadata().name.clone())
            .unwrap_or_default();
        let obj = obj.map(to_value).transpose()?;
        let old = old.map(to_value).transpose()?;

        self.state
            .webhook_manager
            .run_validating_admission_policies_ext(
                &op,
                self.kind,
                obj.as_ref(),
                old.as_ref(),
                Some(&self.request_resource().resource),
                self.namespace,
            )
            .await?;

        if let AdmissionResponse::Deny(reason) = self
            .state
            .webhook_manager
            .run_validating_webhooks_with_dryrun(
                &op,
                self.kind,
                &self.request_resource(),
                self.namespace,
                &name,
                obj.clone(),
                old.clone(),
                &self.user_info(),
                self.dry_run,
            )
            .await?
        {
            return Err(denied(&reason));
        }

        self.validate_quota(op, &name, obj.as_ref(), old.as_ref())
            .await
    }

    /// `QuotaAdmission.Validate` (apiserver/pkg/admission/plugin/
    /// resourcequota/admission.go:158-165), last in the chain.
    async fn validate_quota(
        &self,
        op: Operation,
        name: &str,
        obj: Option<&serde_json::Value>,
        old: Option<&serde_json::Value>,
    ) -> Result<()> {
        let (Some(namespace), Some(obj)) = (self.namespace, obj) else {
            return Ok(());
        };
        if resourcequota::is_namespace_creation(&op, &self.kind.group, &self.kind.kind) {
            return Ok(());
        }
        let gr = GroupResource::new(&self.resource.group, &self.resource.resource);
        let Some(evaluator) = resourcequota::evaluator::evaluator_for(&gr) else {
            return Ok(());
        };
        let attrs = resourcequota::Attributes {
            operation: op,
            namespace,
            subresource: self.subresource,
            object: obj,
            old_object: old,
            dry_run: self.dry_run,
        };
        resourcequota::evaluate(&*self.state.storage, &*evaluator, &attrs)
            .await
            .map_err(|e| resourcequota::to_api_error(e, &gr, name))
    }
}

/// Whether a pod's ResourceQuota *scope* changed across an update, which is
/// the only thing that makes an update worth re-evaluating against quota.
///
/// Ported from upstream's `podEvaluator.Handles`
/// (pkg/quota/v1/evaluator/core/pods.go:179-199): quota is evaluated on
/// CREATE, on the `resize` subresource, and on a plain UPDATE only when the
/// terminating scope flips (`IsTerminating`, :417-422: a non-negative
/// `activeDeadlineSeconds`). Everything else is already counted.
///
/// The gate is load-bearing here: our quota check recounts the namespace live,
/// a paginated pod LIST, and running it on every update made
/// `[sig-node] Pods Extended (pod generation) ... issue 500 podspec updates`
/// take 2754 seconds before failing with `ResourceExhausted: h2 protocol
/// error` against the storage backend.
fn pod_quota_scope_changed(old: &Pod, new: &Pod) -> bool {
    fn is_terminating(pod: &Pod) -> bool {
        pod.spec
            .as_ref()
            .and_then(|s| s.active_deadline_seconds)
            .is_some_and(|d| d >= 0)
    }
    is_terminating(old) != is_terminating(new)
}

/// `rest.AdmissionToValidateObjectFunc` for CREATE. With `authorize_create`
/// it is also `withAuthorization` (update.go:258-283): an update or patch
/// that turns into a create must be allowed to `create`.
pub struct CreateValidation<'a> {
    pub admission: &'a Admission<'a>,
    pub authorize_create: bool,
}

#[async_trait]
impl<T: Object> ValidateObject<T> for CreateValidation<'_> {
    async fn validate(&self, ctx: &RequestContext, obj: &T) -> Result<()> {
        if self.authorize_create {
            let a = self.admission;
            authorize(
                a.state,
                a.user,
                "create",
                a.resource,
                a.subresource,
                a.namespace,
                Some(&obj.metadata().name),
            )
            .await?;
        }
        self.admission
            .validate(ctx, Operation::Create, Some(obj), None)
            .await
    }
}

/// `rest.AdmissionToValidateObjectUpdateFunc`.
pub struct UpdateValidation<'a> {
    pub admission: &'a Admission<'a>,
}

#[async_trait]
impl<T: Object> ValidateObjectUpdate<T> for UpdateValidation<'_> {
    async fn validate(&self, ctx: &RequestContext, obj: &T, old: &T) -> Result<()> {
        self.admission
            .validate(ctx, Operation::Update, Some(obj), Some(old))
            .await
    }
}

/// `rest.AdmissionToValidateObjectDeleteFunc`: the object is nil and the
/// stored object is the old one.
pub struct DeleteValidation<'a> {
    pub admission: &'a Admission<'a>,
}

#[async_trait]
impl<T: Object> ValidateObject<T> for DeleteValidation<'_> {
    async fn validate(&self, ctx: &RequestContext, obj: &T) -> Result<()> {
        self.admission
            .validate::<T>(ctx, Operation::Delete, None, Some(obj))
            .await
    }
}

/// The mutating-admission `TransformFunc` of `UpdateResource`
/// (update.go:170-189) and the patcher's `applyAdmission` (patch.go:631-651):
/// CREATE when there is no live object (`hasUID(old)` is false), UPDATE
/// otherwise.
pub struct MutatingAdmission<'a, T: Object> {
    pub admission: &'a Admission<'a>,
    /// The mutating-webhook dispatcher decodes a webhook's patched object,
    /// so it is defaulted and converted as any request body is.
    pub scope: &'a RequestScope<T>,
}

#[async_trait]
impl<T: Object> TransformFunc<T> for MutatingAdmission<'_, T> {
    async fn transform(&self, ctx: &RequestContext, new: Option<T>, old: Option<&T>) -> Result<T> {
        let new = new.ok_or_else(|| {
            Error::Internal("mutating admission ran before an object was built".to_string())
        })?;
        let mut obj = match old.filter(|o| !o.metadata().uid.is_empty()) {
            None => self.admission.admit(Operation::Create, new, None).await?,
            Some(old) => {
                self.admission
                    .admit(Operation::Update, new, Some(old))
                    .await?
            }
        };
        self.scope.convert(&mut obj);
        // Dedup owner references again after mutating admission:
        // update.go:185-189 and patch.go:691-697 (`patch` only dedups here).
        super::rest::dedup_owner_references_and_add_warning(&mut obj, ctx, true);
        Ok(obj)
    }
}
