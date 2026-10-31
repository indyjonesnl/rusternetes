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
//! ([`crate::admission::resourcequota`]). MutatingAdmissionPolicy is wired
//! ahead of the mutating webhooks behind `Feature::MutatingAdmissionPolicy`
//! ([`crate::admission::policy_plugin`]); the server installs it at startup
//! when the gate is on ([`crate::admission::policy_install::new_mutating_policy_plugin`]),
//! and it does nothing otherwise.
//!
//! The in-tree plugins ahead of them run first, in `AllOrderedPlugins` order
//! (plugins.go:70-101), for the resources on this path that they handle:
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
use rusternetes_common::resources::podcertificaterequest::PodCertificateRequest;
use rusternetes_common::resources::{
    CertificateSigningRequest, ClusterTrustBundle, PersistentVolumeClaim, Pod, PriorityClass,
};
use rusternetes_common::{Error, Result};
use rusternetes_storage::Storage;

use super::rest::{authorize, RequestScope};
use crate::admission::caching_authorizer::new_caching_authorizer;
use crate::admission::plugins::enabled as plugin_enabled;
use crate::admission::{builtin_plugins, resourcequota, storage_object_in_use_protection};
use crate::registry::metadata::HasMetadata;
use crate::registry::rest::{
    GroupResource, Object, RequestContext, TransformFunc, ValidateObject, ValidateObjectUpdate,
};
use crate::state::ApiServerState;
use rusternetes_admission_webhook::cel_evaluators::VapAuthorizer;
use std::sync::Arc;

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
    /// `forbidCreateAdmission` (customresource_handler.go:1523-1554) wraps the
    /// chain: `Handles(Create)` is true and both `Admit` and `Validate` refuse
    /// an `admission.Create`.
    pub forbid_create: bool,
}

fn denied(reason: &str) -> Error {
    Error::Forbidden(format!("admission webhook denied the request: {reason}"))
}

/// The object the admission chain works on, type-erased. The chain runs for
/// every resource type, so keeping it generic over `T: Object` monomorphised
/// ~5k lines of plugin code once per type; the plugins only ever look at the
/// object's metadata or a JSON view of it, which is all this exposes. The
/// public entry points stay generic and are thin wrappers over it.
pub trait AdmissionObject: Send + Sync {
    fn meta(&self) -> &rusternetes_common::types::ObjectMeta;
    fn meta_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta;
    fn to_json(&self) -> serde_json::Result<serde_json::Value>;
    /// Replace the object with `value` decoded as its own type; on error the
    /// object is left as it was.
    fn replace_from_value(&mut self, value: serde_json::Value) -> serde_json::Result<()>;
}

impl<T: Object> AdmissionObject for T {
    fn meta(&self) -> &rusternetes_common::types::ObjectMeta {
        HasMetadata::metadata(self)
    }
    fn meta_mut(&mut self) -> &mut rusternetes_common::types::ObjectMeta {
        HasMetadata::metadata_mut(self)
    }
    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }
    fn replace_from_value(&mut self, value: serde_json::Value) -> serde_json::Result<()> {
        *self = serde_json::from_value(value)?;
        Ok(())
    }
}

fn to_value(obj: &dyn AdmissionObject) -> Result<serde_json::Value> {
    obj.to_json().map_err(|e| Error::Internal(e.to_string()))
}

/// The object as another type — the typed view an in-tree plugin works on,
/// as upstream's plugins type-assert `a.GetObject()`.
fn recast_obj<B: serde::de::DeserializeOwned>(obj: &dyn AdmissionObject) -> Result<B> {
    obj.to_json()
        .and_then(serde_json::from_value)
        .map_err(|e| Error::Internal(e.to_string()))
}

/// Write the typed view `a` back into `obj`.
fn assign<A: serde::Serialize>(obj: &mut dyn AdmissionObject, a: &A) -> Result<()> {
    serde_json::to_value(a)
        .and_then(|v| obj.replace_from_value(v))
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

    /// PodSecurity evaluates every pod subresource not in
    /// `ignoredPodSubresources` (admission.go:316), `ephemeralcontainers`
    /// being the one that adds containers.
    fn is_pod_ephemeralcontainers(&self) -> bool {
        self.resource.group.is_empty()
            && self.resource.resource == "pods"
            && self.subresource == Some("ephemeralcontainers")
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

    /// `forbidCreateAdmission.Admit`/`Validate`:
    /// `apierrors.NewForbidden(a.GetResource().GroupResource(), a.GetName(),
    /// errors.New("create not allowed while custom resource definition is terminating"))`
    /// (customresource_handler.go:1541-1543).
    fn forbid_create_error(&self, name: &str) -> Error {
        self.forbidden(
            name,
            "create not allowed while custom resource definition is terminating",
        )
    }

    /// One in-tree plugin as `NewFromPlugins` runs it
    /// (apiserver/pkg/admission/plugins.go:142-144): skipped when it is not in
    /// the enabled set, or when the chain would not call it for this
    /// operation and step (`chain.go:33-35`, `:49-51`); otherwise `work` is
    /// decorated by `WithControllerMetrics` (metrics.go:63-66, :89-113), so the
    /// plugin is observed whatever the resource, `rejected` being `err != nil`.
    /// See [`crate::admission::plugin_metrics`].
    async fn plugin(
        &self,
        name: &'static str,
        op: &Operation,
        step_type: &str,
        work: impl std::future::Future<Output = Result<()>>,
    ) -> Result<()> {
        if !plugin_enabled(name) || !crate::admission::plugin_metrics::observed(name, op, step_type)
        {
            return Ok(());
        }
        // Boxed: this chain runs 30 plugin bodies in one async fn, and held
        // inline their futures sum into one state machine (each plugin's
        // storage/recast frames), overflowing the stack of an unoptimised
        // build. Upstream's decorator is a plain call frame per plugin
        // (metrics.go:89-113), never part of the chain's own frame.
        rusternetes_admission_webhook::admission_metrics::with_controller_metrics(
            name,
            op,
            step_type,
            Box::pin(work),
        )
        .await
    }

    /// The in-tree mutating plugins, each run through [`Self::plugin`]. For a
    /// Pod they are NodeRestriction, LimitRanger, ServiceAccount, Priority,
    /// DefaultTolerationSeconds, RuntimeClass and PodTopologyLabels;
    /// `DefaultStorageClass` (plugin/pkg/admission/storage/storageclass/
    /// setdefault/admission.go) handles only CREATE of a PersistentVolumeClaim
    /// and NodeRestriction's `Admit` a node's request on a
    /// PodCertificateRequest. NodeRestriction runs first, as it did before the
    /// pod handlers moved onto this chain: it only narrows a node's requests
    /// and reads the pod as sent.
    async fn admit_in_tree(
        &self,
        op: &Operation,
        obj: &mut dyn AdmissionObject,
        old: Option<&dyn AdmissionObject>,
    ) -> Result<()> {
        use rusternetes_admission_webhook::admission_metrics::STEP_ADMIT as ADMIT;
        let mut pod: Option<Pod> = if self.is_core("pods") {
            Some(recast_obj(&*obj)?)
        } else {
            None
        };
        let storage = &self.state.storage;
        let namespace = self.namespace;
        let create = *op == Operation::Create;

        // NamespaceAutoProvision.Admit (autoprovision/admission.go:55-95) is
        // second in `AllOrderedPlugins`, ahead of every other in-tree plugin;
        // `NewProvision` handles Create only.
        self.plugin("NamespaceAutoProvision", op, ADMIT, async {
            crate::admission::namespace_plugins::auto_provision_admit(
                &self.state.storage,
                self.kind,
                &GroupResource::new(&self.resource.group, &self.resource.resource),
                self.namespace,
                &builtin_plugins::attr_name(obj.meta()),
                self.dry_run,
            )
            .await
        })
        .await?;

        // NodeRestriction.Admit (admission.go:228-229): a node's request on
        // any operation or subresource of a PodCertificateRequest, and its
        // pod create.
        self.plugin("NodeRestriction", op, ADMIT, async {
            if let Some(pod) = pod.as_ref() {
                if create && namespace.is_some() {
                    crate::handlers::node_restriction::admit_pod_create(
                        &**storage,
                        &rusternetes_middleware::AuthContext {
                            user: self.user.clone(),
                        },
                        pod,
                    )
                    .await?;
                }
            } else if self.resource.group == "certificates.k8s.io"
                && self.resource.resource == "podcertificaterequests"
            {
                let pcr: rusternetes_common::resources::podcertificaterequest::PodCertificateRequest =
                    recast_obj(&*obj)?;
                crate::handlers::node_restriction::admit_pod_certificate_request(
                    &**storage,
                    &rusternetes_middleware::AuthContext {
                        user: self.user.clone(),
                    },
                    create,
                    self.subresource,
                    namespace.unwrap_or(""),
                    &pcr,
                )
                .await
                .map_err(|e| match e {
                    Error::Forbidden(m) => self.forbidden(&pcr.metadata.name, m),
                    other => other,
                })?;
            }
            Ok(())
        })
        .await?;

        // TaintNodesByCondition.Admit (nodetaint/admission.go:45, :61-77):
        // `NewHandler(Create)`, nodes without a subresource only. It follows
        // NodeRestriction in `AllOrderedPlugins`.
        self.plugin("TaintNodesByCondition", op, ADMIT, async {
            if self.is_core("nodes") {
                let mut node: rusternetes_common::resources::Node = recast_obj(&*obj)?;
                builtin_plugins::node_taint_admit(&mut node);
                assign(&mut *obj, &node)?;
            }
            Ok(())
        })
        .await?;

        // LimitRanger.Admit (plugin/pkg/admission/limitranger/admission.go).
        self.plugin("LimitRanger", op, ADMIT, async {
            if let (Some(pod), Some(namespace), true) = (pod.as_mut(), namespace, create) {
                let name = pod.metadata.name.clone();
                let limit_ranges: Vec<rusternetes_common::resources::LimitRange> = storage
                    .list(&rusternetes_storage::build_prefix(
                        "limitranges",
                        Some(namespace),
                    ))
                    .await
                    .unwrap_or_default();
                match crate::admission::apply_limit_range_with(pod, &limit_ranges) {
                    Ok(true) => {}
                    Ok(false) => {
                        return Err(self.forbidden(&name, "Pod violates LimitRange constraints"));
                    }
                    Err(e) => {
                        tracing::warn!("Error checking LimitRange for pod {namespace}/{name}: {e}");
                    }
                }
            }
            Ok(())
        })
        .await?;

        // ServiceAccount.Admit: a failure to inject does not fail the pod.
        self.plugin("ServiceAccount", op, ADMIT, async {
            if let (Some(pod), Some(namespace), true) = (pod.as_mut(), namespace, create) {
                let name = pod.metadata.name.clone();
                if let Err(e) =
                    crate::admission::inject_service_account_token(storage, namespace, pod).await
                {
                    tracing::warn!(
                        "Error injecting service account token for pod {namespace}/{name}: {e}"
                    );
                }
            }
            Ok(())
        })
        .await?;

        // PodNodeSelector.Admit (podnodeselector/admission.go): CREATE only; it
        // precedes Priority in `AllOrderedPlugins`.
        self.plugin("PodNodeSelector", op, ADMIT, async {
            if let (Some(pod), Some(namespace), true) = (pod.as_mut(), namespace, create) {
                crate::admission::pod_plugins::pod_node_selector_admit(storage, namespace, pod)
                    .await?;
            }
            Ok(())
        })
        .await?;

        self.plugin("Priority", op, ADMIT, async {
            if let (Some(pod), Some(_), true) = (pod.as_mut(), namespace, create) {
                self.admit_pod_priority(pod).await?;
            }
            Ok(())
        })
        .await?;

        // DefaultTolerationSeconds registers for Create AND Update
        // (plugin/pkg/admission/defaulttolerationseconds/admission.go:86): the
        // NotReady/Unreachable NoExecute tolerations (tolerationSeconds: 300),
        // unless the pod has them.
        self.plugin("DefaultTolerationSeconds", op, ADMIT, async {
            if let Some(pod) = pod.as_mut() {
                if *op == Operation::Update || (create && namespace.is_some()) {
                    if let Some(spec) = pod.spec.as_mut() {
                        rusternetes_common::tolerations::add_default_tolerations(spec);
                    }
                }
            }
            Ok(())
        })
        .await?;

        // PodTolerationRestriction.Admit registers for Create and Update
        // (podtolerationrestriction/admission.go:Admit); it follows
        // DefaultTolerationSeconds in `AllOrderedPlugins`: the namespace/cluster
        // default tolerations. An Update never targets a subresource.
        self.plugin("PodTolerationRestriction", op, ADMIT, async {
            if let (Some(pod), Some(namespace)) = (pod.as_mut(), namespace) {
                if create || self.subresource.is_none() {
                    crate::admission::pod_plugins::toleration_restriction_admit(
                        storage, namespace, create, pod,
                    )
                    .await?;
                }
            }
            Ok(())
        })
        .await?;

        // ExtendedResourceToleration.Admit registers for Create and Update
        // (extendedresourcetoleration/admission.go:44); it follows
        // PodTolerationRestriction in `AllOrderedPlugins`. An Update never
        // targets a subresource.
        self.plugin("ExtendedResourceToleration", op, ADMIT, async {
            if let Some(pod) = pod.as_mut() {
                if create || self.subresource.is_none() {
                    crate::admission::pod_plugins::extended_resource_toleration_admit(pod);
                }
            }
            Ok(())
        })
        .await?;

        // RuntimeClass.Admit (plugin/pkg/admission/runtimeclass/admission.go):
        // the class must exist, and its overhead is set on the pod.
        self.plugin("RuntimeClass", op, ADMIT, async {
            let (Some(pod), Some(_), true) = (pod.as_mut(), namespace, create) else {
                return Ok(());
            };
            let name = pod.metadata.name.clone();
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
            Ok(())
        })
        .await?;

        self.plugin("DefaultStorageClass", op, ADMIT, async {
            if self.is_core("persistentvolumeclaims") {
                let mut pvc: PersistentVolumeClaim = recast_obj(&*obj)?;
                crate::admission::set_default_storage_class(storage, &mut pvc)
                    .await
                    .map_err(|e| self.forbidden(&pvc.metadata.name, e))?;
                assign(&mut *obj, &pvc)?;
            }
            Ok(())
        })
        .await?;

        // `StorageObjectInUseProtection` follows `DefaultStorageClass` in
        // `AllOrderedPlugins` (plugins.go:70-101); it acts on the resource
        // itself, never a subresource (admission.go `admitPV`/`admitPVC`).
        self.plugin("StorageObjectInUseProtection", op, ADMIT, async {
            if self.subresource.is_none() {
                if let Some(finalizer) = storage_object_in_use_protection::finalizer_for(
                    &self.resource.group,
                    &self.resource.resource,
                    rusternetes_common::feature_gates::gate_enabled("VolumeAttributesClass")
                        .unwrap_or(false),
                ) {
                    storage_object_in_use_protection::add_protection_finalizer(
                        obj.meta_mut(),
                        finalizer,
                    );
                }
            }
            Ok(())
        })
        .await?;

        // PodTopologyLabels.admitPod (plugin/pkg/admission/podtopologylabels/
        // admission.go:123-148; after RuntimeClass in plugins.go:93, listed at :100): a pod
        // created with `spec.nodeName` gets the node's topology labels,
        // overwriting its own.
        self.plugin("PodTopologyLabels", op, ADMIT, async {
            let (Some(pod), Some(_), true) = (pod.as_mut(), namespace, create) else {
                return Ok(());
            };
            if !rusternetes_common::feature_gates::enabled(
                rusternetes_common::feature_gates::Feature::PodTopologyLabelsAdmission,
            ) {
                return Ok(());
            }
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
            Ok(())
        })
        .await?;

        // AlwaysPullImages.Admit (alwayspullimages/admission.go:53-76):
        // Create and Update, skipping `shouldIgnore` (:161-171).
        self.plugin("AlwaysPullImages", op, ADMIT, async {
            if let Some(pod) = pod.as_mut() {
                let old: Option<Pod> = old.map(recast_obj).transpose()?;
                if !builtin_plugins::is_update_with_no_new_images(pod, old.as_ref()) {
                    builtin_plugins::always_pull_images_admit(pod);
                }
            }
            Ok(())
        })
        .await?;

        // DefaultIngressClass.Admit (defaultingressclass/admission.go:84-129):
        // CREATE of an `ingresses` in networking.k8s.io, no subresource.
        self.plugin("DefaultIngressClass", op, ADMIT, async {
            if self.resource.group == "networking.k8s.io"
                && self.resource.resource == "ingresses"
                && self.subresource.is_none()
            {
                let mut ingress: rusternetes_common::resources::Ingress = recast_obj(&*obj)?;
                builtin_plugins::default_ingress_class(storage, &mut ingress).await?;
                assign(&mut *obj, &ingress)?;
            }
            Ok(())
        })
        .await?;

        if let Some(pod) = pod {
            assign(&mut *obj, &pod)?;
        }
        Ok(())
    }

    /// The in-tree validating plugins, each run through [`Self::plugin`].
    /// `LimitRanger.Validate` (plugin/pkg/admission/limitranger/admission.go:
    /// 116-156) checks a PersistentVolumeClaim's requests on CREATE and
    /// UPDATE, except when the old object is being deleted.
    async fn validate_in_tree(
        &self,
        ctx: &RequestContext,
        op: &Operation,
        obj: Option<&dyn AdmissionObject>,
        old: Option<&dyn AdmissionObject>,
    ) -> Result<()> {
        use rusternetes_admission_webhook::admission_metrics::STEP_VALIDATE as VALIDATE;
        // NamespaceExists.Validate (exists/admission.go:55-86), registered for
        // Create, Update and Delete; it follows NamespaceLifecycle in
        // `AllOrderedPlugins` (plugins.go:69-72), ahead of the other
        // validating plugins.
        self.plugin("NamespaceExists", op, VALIDATE, async {
            crate::admission::namespace_plugins::exists_validate(
                &self.state.storage,
                self.kind,
                self.namespace,
            )
            .await
        })
        .await?;
        // OwnerReferencesPermissionEnforcement: `NewHandler(Create, Update)` and
        // `var _ admission.ValidationInterface`
        // (plugin/pkg/admission/gc/gc_admission.go:51, :71).
        self.plugin(
            "OwnerReferencesPermissionEnforcement",
            op,
            VALIDATE,
            async {
                if let Some(obj) = obj {
                    self.validate_owner_references(op, obj, old).await?;
                }
                Ok(())
            },
        )
        .await?;
        // `approval` is CertificateApproval's, `status` CertificateSigning's.
        let approval = self.subresource == Some("approval");
        self.plugin("CertificateApproval", op, VALIDATE, async {
            if let (true, Some(obj), Some(old)) = (approval, obj, old) {
                self.validate_csr_signer(obj, old).await?;
            }
            Ok(())
        })
        .await?;
        self.plugin("CertificateSigning", op, VALIDATE, async {
            if let (false, Some(obj), Some(old)) = (approval, obj, old) {
                self.validate_csr_signer(obj, old).await?;
            }
            Ok(())
        })
        .await?;
        if let (Operation::Update, Some(obj), Some(old)) = (op, obj, old) {
            self.validate_pcr_signer(obj, old).await?;
        }
        self.plugin("CertificateSubjectRestriction", op, VALIDATE, async {
            if let Some(obj) = obj {
                self.validate_csr_subject(obj)?;
            }
            Ok(())
        })
        .await?;
        self.plugin("ClusterTrustBundleAttest", op, VALIDATE, async {
            if let Some(obj) = obj {
                self.validate_ctb_attest(obj, old).await?;
            }
            Ok(())
        })
        .await?;
        // ImagePolicyWebhook.Validate: `NewHandler(Create, Update)` and
        // `var _ admission.ValidationInterface` (plugin/pkg/admission/
        // imagepolicy/admission.go:91, :284); ahead of PodSecurity as in
        // `AllOrderedPlugins`.
        self.plugin("ImagePolicyWebhook", op, VALIDATE, async {
            if let (true, Some(obj), Some(namespace)) = (
                self.resource.group.is_empty() && self.resource.resource == "pods",
                obj,
                self.namespace,
            ) {
                let pod: Pod = recast_obj(obj)?;
                self.validate_image_policy(&pod, namespace).await?;
            }
            Ok(())
        })
        .await?;
        self.plugin("PodSecurity", op, VALIDATE, async {
            self.validate_pod_security(ctx, op, obj, old).await
        })
        .await?;
        self.validate_builtin_plugins(op, obj, old).await?;
        // EventRateLimit: `NewHandler(Create, Update)` and
        // `var _ admission.ValidationInterface`
        // (plugin/pkg/admission/eventratelimit/admission.go:64, :78).
        self.plugin("EventRateLimit", op, VALIDATE, async {
            self.validate_event_rate_limit(op, obj)
        })
        .await?;
        // KEP-5328 + KEP-1287: a Guaranteed-QoS CPU resize against a node
        // that has not declared `GuaranteedQoSPodCPUResize` is refused (the
        // NodeDeclaredFeatureValidator plugin, off with its gate).
        self.plugin("NodeDeclaredFeatureValidator", op, VALIDATE, async {
            if self.subresource == Some("resize") && self.is_pod_resize() {
                let pod: Option<Pod> = obj.map(recast_obj).transpose()?;
                let old: Option<Pod> = old.map(recast_obj).transpose()?;
                if let (Some(pod), Some(old), Some(_)) =
                    (pod.as_ref(), old.as_ref(), self.namespace)
                {
                    crate::handlers::pod_subresources::check_node_declared_features_for_resize(
                        self.state.storage.as_ref(),
                        old,
                        pod,
                    )
                    .await?;
                }
            }
            Ok(())
        })
        .await?;
        // PodNodeSelector.Validate (Create) and PodTolerationRestriction.Validate
        // (Create, Update): pods proper, never a subresource.
        self.plugin("PodNodeSelector", op, VALIDATE, async {
            if let (Some(obj), true) = (obj, self.is_core("pods") && self.subresource.is_none()) {
                let pod: Pod = recast_obj(obj)?;
                crate::admission::pod_plugins::pod_node_selector_validate(
                    &self.state.storage,
                    self.namespace.unwrap_or(""),
                    &pod,
                )
                .await?;
            }
            Ok(())
        })
        .await?;
        self.plugin("PodTolerationRestriction", op, VALIDATE, async {
            if let (Some(obj), true) = (obj, self.is_core("pods") && self.subresource.is_none()) {
                let pod: Pod = recast_obj(obj)?;
                crate::admission::pod_plugins::toleration_restriction_validate(
                    &self.state.storage,
                    self.namespace.unwrap_or(""),
                    &pod,
                )
                .await?;
            }
            Ok(())
        })
        .await?;
        // `Priority` sits ahead of `LimitRanger` in `AllOrderedPlugins`
        // (plugins.go:70-101).
        self.plugin("Priority", op, VALIDATE, async {
            self.validate_priority(op, obj).await
        })
        .await?;
        self.plugin("LimitRanger", op, VALIDATE, async {
            let (Some(obj), Some(namespace)) = (obj, self.namespace) else {
                return Ok(());
            };
            if !self.is_core("persistentvolumeclaims")
                || old.is_some_and(|o| o.meta().deletion_timestamp.is_some())
            {
                return Ok(());
            }
            let pvc: PersistentVolumeClaim = recast_obj(obj)?;
            match crate::admission::limit_ranger_validate_pvc(&self.state.storage, namespace, &pvc)
                .await
            {
                Ok(None) => Ok(()),
                Ok(Some(err)) => Err(self.forbidden(&pvc.metadata.name, err)),
                Err(e) => Err(self.forbidden(&pvc.metadata.name, e)),
            }
        })
        .await
    }

    /// The validating half of the small in-tree plugins of
    /// [`crate::admission::builtin_plugins`], in `AllOrderedPlugins` order
    /// (plugins.go:69-111): LimitPodHardAntiAffinityTopology,
    /// AlwaysPullImages, PersistentVolumeClaimResize,
    /// DenyServiceExternalIPs. Each registers for Create and Update (the
    /// resize plugin for Update only) and ignores subresources.
    async fn validate_builtin_plugins(
        &self,
        op: &Operation,
        obj: Option<&dyn AdmissionObject>,
        old: Option<&dyn AdmissionObject>,
    ) -> Result<()> {
        use rusternetes_admission_webhook::admission_metrics::STEP_VALIDATE as VALIDATE;
        let Some(obj) = obj else {
            return Ok(());
        };
        self.plugin("LimitPodHardAntiAffinityTopology", op, VALIDATE, async {
            if self.is_core("pods") {
                let pod: Pod = recast_obj(obj)?;
                builtin_plugins::limit_pod_hard_anti_affinity_topology(&pod)?;
            }
            Ok(())
        })
        .await?;
        self.plugin("AlwaysPullImages", op, VALIDATE, async {
            if self.is_core("pods") {
                let pod: Pod = recast_obj(obj)?;
                let old: Option<Pod> = old.map(recast_obj).transpose()?;
                if !builtin_plugins::is_update_with_no_new_images(&pod, old.as_ref()) {
                    builtin_plugins::always_pull_images_validate("pods", &pod)?;
                }
            }
            Ok(())
        })
        .await?;
        self.plugin("PersistentVolumeClaimResize", op, VALIDATE, async {
            if self.is_core("persistentvolumeclaims") {
                if let Some(old) = old {
                    let pvc: PersistentVolumeClaim = recast_obj(obj)?;
                    let old: PersistentVolumeClaim = recast_obj(old)?;
                    builtin_plugins::pvc_resize_validate(&self.state.storage, &pvc, &old).await?;
                }
            }
            Ok(())
        })
        .await?;
        self.plugin("DenyServiceExternalIPs", op, VALIDATE, async {
            if self.is_core("services") {
                let svc: rusternetes_common::resources::Service = recast_obj(obj)?;
                let old: Option<rusternetes_common::resources::Service> =
                    old.map(recast_obj).transpose()?;
                builtin_plugins::deny_service_external_ips(&svc, old.as_ref())?;
            }
            Ok(())
        })
        .await
    }

    /// The `EventRateLimit` plugin (eventratelimit/admission.go:91-120): for
    /// CREATE and UPDATE of a core Event, when it is configured. The enabled
    /// and operation checks are made by [`Self::plugin`].
    fn validate_event_rate_limit(
        &self,
        _op: &Operation,
        obj: Option<&dyn AdmissionObject>,
    ) -> Result<()> {
        use crate::admission::event_rate_limit as erl;
        let Some(plugin) = erl::installed() else {
            return Ok(());
        };
        let event: Option<rusternetes_common::resources::Event> = if self.is_core("events") {
            obj.map(recast_obj).transpose()?
        } else {
            None
        };
        plugin.validate(&erl::Attributes {
            group: &self.kind.group,
            kind: &self.kind.kind,
            namespace: self.namespace.unwrap_or_default(),
            user: &self.user.username,
            dry_run: self.dry_run,
            object: event.as_ref(),
        })
    }

    /// `OwnerReferencesPermissionEnforcement.Validate`
    /// ([`crate::admission::owner_references_permission_enforcement`]).
    async fn validate_owner_references(
        &self,
        op: &Operation,
        obj: &dyn AdmissionObject,
        old: Option<&dyn AdmissionObject>,
    ) -> Result<()> {
        use crate::admission::owner_references_permission_enforcement::{
            Attributes, OwnerReferencesPermissionEnforcement,
        };
        let plugin = OwnerReferencesPermissionEnforcement::new(
            self.state.authorizer.clone(),
            self.state.custom_metrics_mapper.clone(),
        );
        let attributes = Attributes {
            user: self.user,
            group: &self.resource.group,
            version: &self.resource.version,
            resource: &self.resource.resource,
            subresource: self.subresource,
            namespace: self.namespace,
            name: &obj.meta().name,
            create: *op == Operation::Create,
        };
        plugin
            .validate(&attributes, obj.meta(), old.map(|o| o.meta()))
            .await
            .map_err(|e| self.forbidden(&obj.meta().name, e))
    }

    /// The `certificates/approval` and `certificates/signing` plugins, for an
    /// UPDATE of a CertificateSigningRequest's `approval` or `status`
    /// ([`crate::admission::certificates`]).
    async fn validate_csr_signer(
        &self,
        obj: &dyn AdmissionObject,
        old: &dyn AdmissionObject,
    ) -> Result<()> {
        if self.resource.group != "certificates.k8s.io"
            || self.resource.resource != "certificatesigningrequests"
        {
            return Ok(());
        }
        let new: CertificateSigningRequest = recast_obj(obj)?;
        let old: CertificateSigningRequest = recast_obj(old)?;
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

    /// The `certificates/ctbattest` plugin (`ClusterTrustBundleAttest`), for
    /// CREATE and UPDATE of a ClusterTrustBundle
    /// ([`crate::admission::certificates::validate_cluster_trust_bundle_attest`]).
    async fn validate_ctb_attest(
        &self,
        obj: &dyn AdmissionObject,
        old: Option<&dyn AdmissionObject>,
    ) -> Result<()> {
        if self.resource.group != "certificates.k8s.io"
            || self.resource.resource != "clustertrustbundles"
            || self.subresource.is_some()
        {
            return Ok(());
        }
        let new: ClusterTrustBundle = recast_obj(obj)?;
        let old: Option<ClusterTrustBundle> = old.map(recast_obj).transpose()?;
        match crate::admission::certificates::validate_cluster_trust_bundle_attest(
            self.state,
            self.user,
            &new,
            old.as_ref(),
        )
        .await
        {
            Some(err) => Err(self.forbidden(&new.metadata.name, err)),
            None => Ok(()),
        }
    }

    /// The `"sign"` authorization of a PodCertificateRequest `/status` update
    /// ([`crate::admission::certificates::validate_pod_certificate_request_sign`]).
    async fn validate_pcr_signer(
        &self,
        obj: &dyn AdmissionObject,
        old: &dyn AdmissionObject,
    ) -> Result<()> {
        if self.resource.group != "certificates.k8s.io"
            || self.resource.resource != "podcertificaterequests"
            || self.subresource != Some("status")
        {
            return Ok(());
        }
        let new: PodCertificateRequest = recast_obj(obj)?;
        let old: PodCertificateRequest = recast_obj(old)?;
        match crate::admission::certificates::validate_pod_certificate_request_sign(
            self.state, self.user, &new, &old,
        )
        .await
        {
            Some(err) => Err(self.forbidden(&old.metadata.name, err)),
            None => Ok(()),
        }
    }

    /// The `certificates/subjectrestriction` plugin, for CREATE of a
    /// CertificateSigningRequest (subjectrestriction/admission.go:64-93).
    fn validate_csr_subject(&self, obj: &dyn AdmissionObject) -> Result<()> {
        if self.resource.group != "certificates.k8s.io"
            || self.resource.resource != "certificatesigningrequests"
            || self.subresource.is_some()
        {
            return Ok(());
        }
        let csr: CertificateSigningRequest = recast_obj(obj)?;
        match crate::admission::certificates::subject_restriction_error(&csr) {
            Some(err) => Err(self.forbidden(&csr.metadata.name, err)),
            None => Ok(()),
        }
    }

    /// `Priority.Validate` (plugin/pkg/admission/priority/admission.go:116-133)
    /// for PriorityClasses:`validatePriorityClass` (:156-176) lets at most
    /// one class be `globalDefault`.
    async fn validate_priority(
        &self,
        op: &Operation,
        obj: Option<&dyn AdmissionObject>,
    ) -> Result<()> {
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
        let pc: PriorityClass = recast_obj(obj)?;
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

    /// `ImagePolicyWebhook.Validate`
    /// (plugin/pkg/admission/imagepolicy/admission.go:126-179). The backend's
    /// audit annotations are recorded on the request's audit event whether or
    /// not it is denied.
    async fn validate_image_policy(&self, pod: &Pod, namespace: &str) -> Result<()> {
        let Some(plugin) = crate::admission::image_policy::installed() else {
            return Ok(());
        };
        let decision = plugin.validate(pod, namespace, self.subresource).await;
        if !decision.audit_annotations.is_empty() {
            crate::audit::add_audit_annotations(&decision.audit_annotations);
        }
        decision.into_result()
    }

    /// The PodSecurity plugin's `Validate` (podsecurity/admission.go), one call
    /// per request: `ValidateNamespace` (pod-security-admission/admission/
    /// admission.go:229-327), `ValidatePodController` (:393-453) and
    /// `ValidatePod` (:370-388). The pod ResourceQuota evaluator runs last, on
    /// the generic path (`validate_quota`).
    async fn validate_pod_security(
        &self,
        ctx: &RequestContext,
        op: &Operation,
        obj: Option<&dyn AdmissionObject>,
        old: Option<&dyn AdmissionObject>,
    ) -> Result<()> {
        // `ValidateNamespace`: label validation, and the existing pods
        // checked against a tightened enforce level.
        if let (Operation::Create | Operation::Update, Some(obj), true) =
            (op, obj, self.is_core("namespaces"))
        {
            let ns: rusternetes_common::resources::Namespace = recast_obj(obj)?;
            let old_ns: Option<rusternetes_common::resources::Namespace> =
                if *op == Operation::Update {
                    old.map(recast_obj).transpose()?
                } else {
                    None
                };
            for w in crate::admission::PodSecurityAdmission::new()
                .validate_namespace(&self.state.storage, &ns, old_ns.as_ref())
                .await?
            {
                ctx.add_warning(w);
            }
        }
        // `ValidatePodController`: the warn/audit evaluation of a controller's
        // pod template. The plugin runs it for resources with `HasPodSpec`
        // (podsecurity/admission.go:195).
        if let (Operation::Create | Operation::Update, Some(obj), Some(namespace)) =
            (op, obj, self.namespace)
        {
            let r = &self.resource;
            if !(r.group.is_empty() && r.resource == "pods")
                && crate::admission::pod_security_controller::has_pod_spec(&r.group, &r.resource)
            {
                let value = obj
                    .to_json()
                    .map_err(|e| Error::Internal(format!("failed to encode object: {e}")))?;
                for w in crate::admission::PodSecurityAdmission::new()
                    .validate_pod_controller(
                        &self.state.storage,
                        namespace,
                        self.subresource,
                        &r.group,
                        &r.resource,
                        &value,
                        &self.user.username,
                    )
                    .await
                    .warnings
                {
                    ctx.add_warning(w);
                }
            }
        }
        // `ValidatePod`, on CREATE and on UPDATE.
        if self.is_core("pods") || self.is_pod_resize() || self.is_pod_ephemeralcontainers() {
            let pod: Option<Pod> = obj.map(recast_obj).transpose()?;
            let old: Option<Pod> = old.map(recast_obj).transpose()?;
            let storage = &self.state.storage;
            let target = match (op, pod.as_ref(), old.as_ref(), self.namespace) {
                (Operation::Create, Some(pod), _, Some(namespace)) => Some((pod, None, namespace)),
                (Operation::Update, Some(pod), Some(old), Some(namespace)) => {
                    Some((pod, Some(old), namespace))
                }
                _ => None,
            };
            if let Some((pod, old, namespace)) = target {
                if crate::admission::PodSecurityAdmission::should_evaluate(
                    self.subresource,
                    old,
                    pod,
                ) {
                    // Warn mode: the request is admitted with warnings
                    // (AdmissionResponse.Warnings -> warning.AddWarning).
                    let outcome = crate::admission::PodSecurityAdmission::new()
                        .admit_outcome(storage, namespace, pod, &self.user.username)
                        .await?;
                    crate::admission::record_pod_security_audit(&outcome);
                    for w in outcome.warnings {
                        ctx.add_warning(w);
                    }
                }
            }
        }
        Ok(())
    }

    /// `mutatingadmissionpolicy.Plugin.Admit` (policy/mutating/plugin.go).
    /// A no-op unless `Feature::MutatingAdmissionPolicy` is on and a policy
    /// source is installed in the state: nothing changes (not even a
    /// round-trip through JSON) otherwise.
    ///
    /// `reinvocation` is the request's `a.GetReinvocationContext()`, shared
    /// by both passes of [`Self::admit`].
    async fn admit_mutating_policy(
        &self,
        op: &Operation,
        obj: &mut dyn AdmissionObject,
        old: Option<&dyn AdmissionObject>,
        reinvocation: &std::sync::Arc<crate::admission::policy_mutating::ReinvocationContext>,
    ) -> Result<()> {
        use crate::admission::policy_plugin::MutatingPolicyPlugin;

        if !plugin_enabled("MutatingAdmissionPolicy")
            || !MutatingPolicyPlugin::handles(op)
            || !MutatingPolicyPlugin::enabled()
        {
            return Ok(());
        }
        let Some(plugin) = self.state.mutating_admission_policy.as_ref() else {
            return Ok(());
        };
        let before = to_value(&*obj)?;
        let mut attr = crate::admission::policy_matching::Attributes {
            kind: self.kind.clone(),
            resource: self.resource.clone(),
            subresource: self.subresource.unwrap_or("").to_string(),
            namespace: self.namespace.unwrap_or("").to_string(),
            name: obj.meta().name.clone(),
            operation: op.clone(),
            object: Some(before.clone()),
            old_object: old.map(to_value).transpose()?,
            user_info: Some(self.user.clone()),
            dry_run: self.dry_run,
            operation_options: None,
        };
        // `WithControllerMetrics(plugin, "MutatingAdmissionPolicy")`
        // (metrics.go:63-66, :89-100).
        rusternetes_admission_webhook::admission_metrics::with_controller_metrics(
            "MutatingAdmissionPolicy",
            op,
            rusternetes_admission_webhook::admission_metrics::STEP_ADMIT,
            plugin.admit(&mut attr, reinvocation.clone()),
        )
        .await?;
        match attr.object {
            Some(mut after) if after != before => {
                // policy/mutating/dispatcher.go:274: the patched object is
                // defaulted (a custom resource by `unstructuredDefaulter`,
                // customresource_handler.go:1240-1249), where the patch is
                // applied rather than by the final conversion.
                if !self.resource.group.is_empty() {
                    let key = rusternetes_storage::build_key(
                        "customresourcedefinitions",
                        None,
                        &format!("{}.{}", self.resource.resource, self.resource.group),
                    );
                    if let Ok(crd) = self
                        .state
                        .storage
                        .get::<rusternetes_common::resources::CustomResourceDefinition>(&key)
                        .await
                    {
                        rusternetes_common::resources::crd::default_custom_resource_value(
                            &crd,
                            &self.resource.version,
                            &mut after,
                        );
                    }
                }
                obj.replace_from_value(after).map_err(|e| {
                    Error::Internal(format!(
                        "failed to decode the object mutated by admission: {e}"
                    ))
                })
            }
            _ => Ok(()),
        }
    }

    /// The mutating plugins behind the reinvoker: `reinvoker.Admit`
    /// (staging/src/k8s.io/apiserver/pkg/admission/reinvocation.go:34-50).
    /// The chain runs once; when a plugin asked for reinvocation
    /// (`a.GetReinvocationContext().ShouldReinvoke()`) it runs a second time
    /// after `SetIsReinvoke`, and each plugin decides from its own
    /// reinvocation context what to rerun.
    ///
    /// The mutating webhooks keep their `webhookReinvokeContext` in the
    /// shared context (webhook/mutating/dispatcher.go:106-121), so an
    /// `IfNeeded` webhook is rerun on the second pass when a policy or
    /// in-tree plugin changed the object after it (#3041).
    pub async fn admit<T: Object>(&self, op: Operation, obj: T, old: Option<&T>) -> Result<T> {
        let mut obj = obj;
        self.admit_dyn(op, &mut obj, old.map(|o| o as &dyn AdmissionObject))
            .await?;
        Ok(obj)
    }

    async fn admit_dyn(
        &self,
        op: Operation,
        obj: &mut dyn AdmissionObject,
        old: Option<&dyn AdmissionObject>,
    ) -> Result<()> {
        // `c.AdmissionControl = admissionmetrics.WithStepMetrics(admissionChain)`
        // (apiserver/pkg/server/options/admission.go:179): the whole admit
        // phase, both reinvoker passes included, is one `step` observation.
        let step_op = op.clone();
        rusternetes_admission_webhook::admission_metrics::with_step_metrics(
            &step_op,
            rusternetes_admission_webhook::admission_metrics::STEP_ADMIT,
            self.admit_reinvoking(op, obj, old),
        )
        .await
    }

    async fn admit_reinvoking(
        &self,
        op: Operation,
        obj: &mut dyn AdmissionObject,
        old: Option<&dyn AdmissionObject>,
    ) -> Result<()> {
        if self.forbid_create && op == Operation::Create {
            return Err(self.forbid_create_error(&obj.meta().name));
        }
        use crate::admission::policy_mutating::ReinvocationContext;
        let reinvocation = std::sync::Arc::new(ReinvocationContext::new(false));
        self.admit_chain(&op, &mut *obj, old, &reinvocation).await?;
        // AlwaysDeny is last in `AllOrderedPlugins` (plugins.go:69-111),
        // after the webhooks.
        if plugin_enabled("AlwaysDeny") {
            return Err(self.always_deny(obj.meta()));
        }
        if !reinvocation.should_reinvoke() {
            return Ok(());
        }
        reinvocation.set_is_reinvoke();
        self.admit_chain(&op, obj, old, &reinvocation).await
    }

    /// One pass of the mutating chain (`mutator.Admit`).
    async fn admit_chain(
        &self,
        op: &Operation,
        obj: &mut dyn AdmissionObject,
        old: Option<&dyn AdmissionObject>,
        reinvocation: &std::sync::Arc<crate::admission::policy_mutating::ReinvocationContext>,
    ) -> Result<()> {
        // MutatingAdmissionPolicy is first (plugins.go:106-110), ahead of
        // the webhooks; the in-tree plugins in `AllOrderedPlugins` precede
        // all four, and they are kept ahead of it here as before.
        self.admit_in_tree(op, &mut *obj, old).await?;
        self.admit_mutating_policy(op, &mut *obj, old, reinvocation)
            .await?;
        if !plugin_enabled("MutatingAdmissionWebhook") {
            return Ok(());
        }
        let name = obj.meta().name.clone();
        let before_webhooks = to_value(&*obj)?;
        // dispatcher.go:106-113: this plugin's context lives in the shared
        // reinvocation context, so it survives into the second pass.
        let mut webhook_ctx = reinvocation.take_webhook_context();
        let result = self
            .state
            .webhook_manager
            .run_mutating_webhooks_pass(
                op,
                self.kind,
                &self.request_resource(),
                self.namespace,
                &name,
                Some(before_webhooks.clone()),
                old.map(to_value).transpose()?,
                &self.user_info(),
                self.dry_run,
                reinvocation.is_reinvoke(),
                &mut webhook_ctx,
            )
            .await;
        let (response, mutated, changed) = match result {
            Ok(r) => r,
            Err(e) => {
                reinvocation.restore_webhook_context(webhook_ctx);
                return Err(e);
            }
        };
        if changed {
            // dispatcher.go:197: `reinvokeCtx.SetShouldReinvoke()`.
            reinvocation.set_should_reinvoke();
        }
        if let AdmissionResponse::Deny(reason) = &response {
            reinvocation.restore_webhook_context(webhook_ctx);
            return Err(denied(reason));
        }
        let decoded = match mutated {
            Some(value) => obj.replace_from_value(value).map_err(|e| {
                Error::Internal(format!(
                    "failed to decode the object mutated by admission: {e}"
                ))
            }),
            None => Ok(()),
        };
        // The next pass compares the typed object (what the plugins hand on),
        // not the raw webhook output, against the last webhook output: a
        // serde round trip that normalises a field is not a mutation.
        if decoded.is_ok() {
            webhook_ctx.set_last_webhook_invocation_output(Some(to_value(&*obj)?));
        }
        reinvocation.restore_webhook_context(webhook_ctx);
        decoded
    }

    /// The mutating chain for a DELETE, which `AdmissionToValidateObjectDeleteFunc`
    /// runs before the validating one (registry/rest/delete.go:177-218):
    /// `Admit` with a nil object and the stored one as `old`, when the chain
    /// `Handles(DELETE)`. The same `WithStepMetrics` as any admit phase wraps
    /// it (server/options/admission.go:179).
    ///
    /// Nothing is mutated: the in-tree plugins that handle DELETE
    /// (NodeRestriction, Priority, NamespaceLifecycle, NamespaceExists) only
    /// reject or do nothing, mutating webhooks see no object to patch
    /// (webhook/mutating/dispatcher.go:346), and the mutating admission policy
    /// plugin does not handle DELETE (`policy_plugin.rs`).
    pub async fn admit_delete<T: Object>(&self, old: &T) -> Result<()> {
        self.admit_delete_dyn(old).await
    }

    async fn admit_delete_dyn(&self, old: &dyn AdmissionObject) -> Result<()> {
        use rusternetes_admission_webhook::admission_metrics::{with_step_metrics, STEP_ADMIT};
        with_step_metrics(&Operation::Delete, STEP_ADMIT, self.admit_delete_chain(old)).await
    }

    async fn admit_delete_chain(&self, old: &dyn AdmissionObject) -> Result<()> {
        use rusternetes_admission_webhook::admission_metrics::STEP_ADMIT as ADMIT;
        let op = Operation::Delete;
        // NodeRestriction.Admit on a pod DELETE (admission.go:257-270); it
        // is an Admit-only plugin.
        self.plugin("NodeRestriction", &op, ADMIT, async {
            if self.is_core("pods") {
                let pod: Pod = recast_obj(old)?;
                crate::handlers::node_restriction::admit_pod_delete(
                    &rusternetes_middleware::AuthContext {
                        user: self.user.clone(),
                    },
                    &pod,
                )?;
            }
            Ok(())
        })
        .await?;
        // Priority.Admit handles DELETE but acts only on a Pod CREATE
        // (priority/admission.go:64-66, :112).
        self.plugin("Priority", &op, ADMIT, async { Ok(()) })
            .await?;
        if !plugin_enabled("MutatingAdmissionWebhook") {
            return self.admit_delete_deny(old.meta());
        }
        let name = old.meta().name.clone();
        let mut webhook_ctx = rusternetes_admission_webhook::WebhookReinvokeContext::default();
        let (response, _, _) = self
            .state
            .webhook_manager
            .run_mutating_webhooks_pass(
                &op,
                self.kind,
                &self.request_resource(),
                self.namespace,
                &name,
                None,
                Some(to_value(old)?),
                &self.user_info(),
                self.dry_run,
                false,
                &mut webhook_ctx,
            )
            .await?;
        if let AdmissionResponse::Deny(reason) = &response {
            return Err(denied(reason));
        }
        self.admit_delete_deny(old.meta())
    }

    /// AlwaysDeny is last in `AllOrderedPlugins`, after the webhooks.
    fn admit_delete_deny(&self, meta: &rusternetes_common::types::ObjectMeta) -> Result<()> {
        if plugin_enabled("AlwaysDeny") {
            return Err(self.always_deny(meta));
        }
        Ok(())
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
        self.validate_dyn(
            ctx,
            op,
            obj.map(|o| o as &dyn AdmissionObject),
            old.map(|o| o as &dyn AdmissionObject),
        )
        .await
    }

    async fn validate_dyn(
        &self,
        ctx: &RequestContext,
        op: Operation,
        obj: Option<&dyn AdmissionObject>,
        old: Option<&dyn AdmissionObject>,
    ) -> Result<()> {
        // `WithStepMetrics` (admission.go:179), the validate phase.
        let step_op = op.clone();
        rusternetes_admission_webhook::admission_metrics::with_step_metrics(
            &step_op,
            rusternetes_admission_webhook::admission_metrics::STEP_VALIDATE,
            self.validate_chain(ctx, op, obj, old),
        )
        .await
    }

    async fn validate_chain(
        &self,
        ctx: &RequestContext,
        op: Operation,
        obj: Option<&dyn AdmissionObject>,
        old: Option<&dyn AdmissionObject>,
    ) -> Result<()> {
        if self.forbid_create && op == Operation::Create {
            let name = obj.map(|o| o.meta().name.as_str()).unwrap_or_default();
            return Err(self.forbid_create_error(name));
        }
        self.validate_in_tree(ctx, &op, obj, old).await?;
        let name = obj
            .or(old)
            .map(|o| o.meta().name.clone())
            .unwrap_or_default();
        let deny = plugin_enabled("AlwaysDeny").then(|| {
            let default = rusternetes_common::types::ObjectMeta::default();
            self.always_deny(obj.or(old).map(|o| o.meta()).unwrap_or(&default))
        });
        let obj = obj.map(to_value).transpose()?;
        let old = old.map(to_value).transpose()?;

        if plugin_enabled("ValidatingAdmissionPolicy") {
            self.state
                .webhook_manager
                .run_validating_admission_policies_authz(
                    &op,
                    self.kind,
                    obj.as_ref(),
                    old.as_ref(),
                    Some(&self.request_resource().resource),
                    self.namespace,
                    // The policy plugin's `SetAuthorizer` (WantsAuthorizer) value,
                    // wrapped per dispatch (validating/dispatcher.go:117).
                    Some(&VapAuthorizer {
                        user: self.user.clone(),
                        authorizer: Arc::new(new_caching_authorizer(
                            self.state.unfiltered_authorizer.clone(),
                        )),
                        subresource: self.subresource.unwrap_or_default().to_string(),
                    }),
                )
                .await?;
        }

        if plugin_enabled("ValidatingAdmissionWebhook") {
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
        }

        if plugin_enabled("ResourceQuota") {
            self.validate_quota(op.clone(), &name, obj.as_ref(), old.as_ref())
                .await?;
        }
        // AlwaysDeny is last in `AllOrderedPlugins` (plugins.go:69-111) and
        // handles every operation (deny/admission.go:57-60).
        deny.map_or(Ok(()), Err)
    }

    /// `alwaysDeny`'s `NewForbidden` (deny/admission.go:46-54).
    fn always_deny(&self, meta: &rusternetes_common::types::ObjectMeta) -> Error {
        builtin_plugins::always_deny(
            &GroupResource::new(&self.resource.group, &self.resource.resource).to_string(),
            &builtin_plugins::attr_name(meta),
        )
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
        // The plugin is decorated by `WithControllerMetrics`
        // (metrics.go:63-66); `NewHandler(Create, Update)`
        // (resourcequota/admission.go:97) keeps other operations out of the
        // chain, so they are not observed.
        if !matches!(op, Operation::Create | Operation::Update) {
            return self.evaluate_quota(op, name, obj, old).await;
        }
        let observed_op = op.clone();
        rusternetes_admission_webhook::admission_metrics::with_controller_metrics(
            "ResourceQuota",
            &observed_op,
            rusternetes_admission_webhook::admission_metrics::STEP_VALIDATE,
            self.evaluate_quota(op, name, obj, old),
        )
        .await
    }

    async fn evaluate_quota(
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
        self.admission.admit_delete(obj).await?;
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
    /// The mutating-webhook dispatcher defaults a webhook's patched object
    /// where it is applied; here it is converted to the storage version, which
    /// coerces it without defaulting again.
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
        self.scope.convert_admitted(&mut obj)?;
        // Dedup owner references again after mutating admission:
        // update.go:185-189 and patch.go:691-697 (`patch` only dedups here).
        super::rest::dedup_owner_references_and_add_warning(&mut obj, ctx, true);
        Ok(obj)
    }
}
